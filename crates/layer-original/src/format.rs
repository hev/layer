//! Bounded original format validation, without pixel decoding. QuickXor follows
//! Microsoft's 160-bit, 11-bit rotation and little-endian length finalization:
//! https://learn.microsoft.com/en-us/onedrive/developer/code-snippets/quickxorhash
use base64::Engine;
use flate2::{Decompress, FlushDecompress, Status};

#[derive(Default)]
pub struct QuickXor {
    bytes: [u8; 20],
    length: u64,
    shift: usize,
}
impl QuickXor {
    pub fn update(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            let index = self.shift / 8;
            let offset = self.shift % 8;
            self.bytes[index] ^= byte << offset;
            if offset != 0 {
                self.bytes[(index + 1) % 20] ^= byte >> (8 - offset);
            }
            self.shift = (self.shift + 11) % 160;
        }
        self.length += bytes.len() as u64;
    }
    pub fn finish(mut self) -> String {
        for (i, byte) in self.length.to_le_bytes().iter().enumerate() {
            self.bytes[12 + i] ^= byte;
        }
        base64::engine::general_purpose::STANDARD.encode(self.bytes)
    }
}

#[derive(Default)]
pub struct Inspector {
    header: Vec<u8>,
    png: Option<Png>,
    error: bool,
    quick_xor: QuickXor,
    nonzero: bool,
    seen: bool,
}
pub struct Format {
    pub mime: Option<&'static str>,
    pub quick_xor: String,
    pub corrupt_png: bool,
    pub all_zero: bool,
    pub empty: bool,
    pub error_response: bool,
}
impl Inspector {
    pub fn update(&mut self, bytes: &[u8]) {
        self.quick_xor.update(bytes);
        self.seen |= !bytes.is_empty();
        self.nonzero |= bytes.iter().any(|b| *b != 0);
        // Keep the PDF sniffing window fixed, including for arbitrary chunks.
        let n = bytes.len().min(1029usize.saturating_sub(self.header.len()));
        self.header.extend_from_slice(&bytes[..n]);
        if self.header.len() >= 8 && self.header.starts_with(b"\x89PNG\r\n\x1a\n") {
            if self.png.is_none() && !self.error {
                let mut png = Png::default();
                if png.update(&self.header[8..]).is_err() {
                    self.error = true;
                }
                self.png = Some(png);
                // Header includes all bytes up to n in this call. Earlier calls
                // had fewer than eight bytes, so were not parsed yet.
                if !self.error && self.png.as_mut().unwrap().update(&bytes[n..]).is_err() {
                    self.error = true;
                }
            } else if !self.error && self.png.as_mut().unwrap().update(bytes).is_err() {
                self.error = true;
            }
        }
    }
    pub fn finish(self) -> Format {
        let mime = if self.png.is_some() {
            (!self.error && self.png.as_ref().is_some_and(|p| p.done)).then_some("image/png")
        } else if crate::has_pdf_header(&self.header) {
            Some("application/pdf")
        } else {
            None
        };
        let corrupt_png = self
            .png
            .as_ref()
            .is_some_and(|p| !p.unsupported && (self.error || !p.done));
        let error_response = is_error_response(&self.header);
        Format {
            error_response,
            all_zero: self.seen && !self.nonzero,
            empty: !self.seen,
            // A serialized wrapper's nested PDF magic is not a plain PDF.
            // This exclusion does not validate or assign a type to the wrapper.
            mime: if error_response || self.header.starts_with(b"rtfd\0\0\0\0") {
                None
            } else {
                mime
            },
            corrupt_png,
            quick_xor: self.quick_xor.finish(),
        }
    }
}

/// Sniff only; never normalize retained bytes. The header is capped at1029B.
/// BOMs/ASCII whitespace cannot disguise an upstream HTML/JSON response.
fn is_error_response(header: &[u8]) -> bool {
    let header = header.trim_ascii_start();
    let text = if header.starts_with(&[0xff, 0xfe, 0, 0]) || header.starts_with(&[0, 0, 0xfe, 0xff])
    {
        let little = header[0] == 0xff;
        header[4..]
            .as_chunks::<4>()
            .0
            .iter()
            .map(|p| {
                let bytes = [p[0], p[1], p[2], p[3]];
                char::from_u32(if little {
                    u32::from_le_bytes(bytes)
                } else {
                    u32::from_be_bytes(bytes)
                })
                .unwrap_or('\u{fffd}')
            })
            .collect::<String>()
    } else if header.starts_with(&[0xff, 0xfe]) || header.starts_with(&[0xfe, 0xff]) {
        let little = header[0] == 0xff;
        let units = header[2..]
            .as_chunks::<2>()
            .0
            .iter()
            .map(|p| {
                if little {
                    u16::from_le_bytes([p[0], p[1]])
                } else {
                    u16::from_be_bytes([p[0], p[1]])
                }
            })
            .collect::<Vec<_>>();
        String::from_utf16_lossy(&units)
    } else {
        String::from_utf8_lossy(header).into_owned()
    };
    let text = text
        .trim_start_matches(|c: char| c == '\u{feff}' || c.is_ascii_whitespace())
        .to_ascii_lowercase();
    text.starts_with("<!doctype html")
        || text.starts_with("<html")
        || text.starts_with('{')
        || text.starts_with('[')
}

struct Png {
    // framing: chunk header (8), data (remaining), CRC (4).
    framing: Vec<u8>,
    kind: [u8; 4],
    remaining: u32,
    crc: crc32fast::Hasher,
    data: Vec<u8>,
    in_chunk: bool,
    ihdr: bool,
    palette: bool,
    color_type: u8,
    idat: bool,
    idat_closed: bool,
    done: bool,
    unsupported: bool,
    inflater: Decompress,
    zlib_done: bool,
    rows: Vec<(u64, u64)>, // (row bytes including filter, number of rows), <=7 passes
    pass: usize,
    row: u64,
    offset: u64,
    inflated: u64,
}
impl Default for Png {
    fn default() -> Self {
        Self {
            framing: Vec::with_capacity(8),
            kind: [0; 4],
            remaining: 0,
            crc: crc32fast::Hasher::new(),
            data: Vec::with_capacity(13),
            in_chunk: false,
            ihdr: false,
            palette: false,
            color_type: 0,
            idat: false,
            idat_closed: false,
            done: false,
            unsupported: false,
            inflater: Decompress::new(true),
            zlib_done: false,
            rows: Vec::new(),
            pass: 0,
            row: 0,
            offset: 0,
            inflated: 0,
        }
    }
}
impl Png {
    fn update(&mut self, mut input: &[u8]) -> Result<(), ()> {
        while !input.is_empty() {
            if self.done {
                return Err(());
            }
            if !self.in_chunk {
                let n = (8 - self.framing.len()).min(input.len());
                self.framing.extend_from_slice(&input[..n]);
                input = &input[n..];
                if self.framing.len() < 8 {
                    continue;
                }
                self.remaining = u32::from_be_bytes(self.framing[..4].try_into().unwrap());
                self.kind.copy_from_slice(&self.framing[4..]);
                if self.remaining > 0x7fff_ffff
                    || !self.kind.iter().all(u8::is_ascii_alphabetic)
                    || self.kind[2].is_ascii_lowercase()
                {
                    return Err(());
                }
                if !self.ihdr && self.kind != *b"IHDR" {
                    return Err(());
                }
                match &self.kind {
                    b"IHDR" if !self.ihdr && self.remaining == 13 => {}
                    b"PLTE"
                        if self.ihdr
                            && !self.palette
                            && !self.idat
                            && self.remaining > 0
                            && self.remaining <= 768
                            && self.remaining.is_multiple_of(3)
                            && matches!(self.color_type, 2 | 6) => {}
                    b"IDAT" if self.ihdr && !self.idat_closed => {
                        self.idat = true;
                    }
                    b"IEND" if self.idat && self.remaining == 0 => {}
                    // APNG is a distinct unsupported format, never raw-success.
                    b"acTL" | b"fcTL" | b"fdAT" => {
                        self.unsupported = true;
                        return Err(());
                    }
                    b"IHDR" | b"PLTE" | b"IDAT" | b"IEND" => return Err(()),
                    _ if self.kind[0].is_ascii_uppercase() => {
                        self.unsupported = true;
                        return Err(());
                    }
                    _ => {}
                }
                if self.idat && self.kind != *b"IDAT" {
                    self.idat_closed = true;
                }
                self.crc = crc32fast::Hasher::new();
                self.crc.update(&self.kind);
                self.framing.clear();
                self.data.clear();
                self.in_chunk = true;
            } else if self.remaining > 0 {
                let n = (self.remaining as usize).min(input.len());
                let bytes = &input[..n];
                self.crc.update(bytes);
                if self.kind == *b"IHDR" {
                    self.data.extend_from_slice(bytes);
                }
                if self.kind == *b"IDAT" {
                    self.inflate(bytes)?;
                }
                self.remaining -= n as u32;
                input = &input[n..];
            } else {
                let n = (4 - self.framing.len()).min(input.len());
                self.framing.extend_from_slice(&input[..n]);
                input = &input[n..];
                if self.framing.len() != 4 {
                    continue;
                }
                if self.crc.clone().finalize()
                    != u32::from_be_bytes(self.framing[..].try_into().unwrap())
                {
                    return Err(());
                }
                match &self.kind {
                    b"IHDR" => self.header()?,
                    b"PLTE" => self.palette = true,
                    b"IEND" => {
                        if !self.zlib_done || self.pass != self.rows.len() || self.offset != 0 {
                            return Err(());
                        }
                        self.done = true;
                    }
                    _ => {}
                }
                self.framing.clear();
                self.in_chunk = false;
            }
        }
        Ok(())
    }
    fn header(&mut self) -> Result<(), ()> {
        let d = &self.data;
        let width = u32::from_be_bytes(d[..4].try_into().unwrap()) as u64;
        let height = u32::from_be_bytes(d[4..8].try_into().unwrap()) as u64;
        let depth = d[8] as u64;
        let channels = match d[9] {
            0 if [1, 2, 4, 8, 16].contains(&d[8]) => 1,
            2 if [8, 16].contains(&d[8]) => 3,
            // Indexed palette sample validation needs pixel reconstruction;
            // leave this form unsupported rather than overclaim validity.
            3 => {
                self.unsupported = true;
                return Err(());
            }
            4 if [8, 16].contains(&d[8]) => 2,
            6 if [8, 16].contains(&d[8]) => 4,
            _ => return Err(()),
        };
        if width == 0
            || height == 0
            || width > 0x7fff_ffff
            || height > 0x7fff_ffff
            || d[10] != 0
            || d[11] != 0
            || d[12] > 1
        {
            return Err(());
        }
        let passes: &[(u64, u64, u64, u64)] = if d[12] == 0 {
            &[(0, 0, 1, 1)]
        } else {
            &[
                (0, 0, 8, 8),
                (4, 0, 8, 8),
                (0, 4, 4, 8),
                (2, 0, 4, 4),
                (0, 2, 2, 4),
                (1, 0, 2, 2),
                (0, 1, 1, 2),
            ]
        };
        let mut total = 0u64;
        for &(x, y, dx, dy) in passes {
            if width <= x || height <= y {
                continue;
            }
            let w = (width - x).div_ceil(dx);
            let h = (height - y).div_ceil(dy);
            let row = (w * depth * channels).div_ceil(8) + 1;
            total = match row
                .checked_mul(h)
                .and_then(|bytes| total.checked_add(bytes))
            {
                Some(total) => total,
                None => {
                    self.unsupported = true;
                    return Err(());
                }
            };
            if total > 1024 * 1024 * 1024 {
                self.unsupported = true;
                return Err(());
            }
            self.rows.push((row, h));
        }
        self.color_type = d[9];
        self.ihdr = true;
        Ok(())
    }
    fn inflate(&mut self, mut input: &[u8]) -> Result<(), ()> {
        if self.zlib_done && !input.is_empty() {
            return Err(());
        }
        loop {
            let mut output = [0u8; 32 * 1024];
            let before_in = self.inflater.total_in();
            let before_out = self.inflater.total_out();
            let status = self
                .inflater
                .decompress(input, &mut output, FlushDecompress::None)
                .map_err(|_| ())?;
            let used = (self.inflater.total_in() - before_in) as usize;
            let produced = (self.inflater.total_out() - before_out) as usize;
            input = &input[used..];
            for &byte in &output[..produced] {
                if self.pass >= self.rows.len() || (self.offset == 0 && byte > 4) {
                    return Err(());
                }
                self.inflated += 1;
                if self.inflated > 1024 * 1024 * 1024 {
                    self.unsupported = true;
                    return Err(());
                }
                self.offset += 1;
                if self.offset == self.rows[self.pass].0 {
                    self.offset = 0;
                    self.row += 1;
                    if self.row == self.rows[self.pass].1 {
                        self.row = 0;
                        self.pass += 1;
                    }
                }
            }
            if status == Status::StreamEnd {
                if !input.is_empty() {
                    return Err(());
                }
                self.zlib_done = true;
                return Ok(());
            }
            if input.is_empty() && produced < output.len() {
                return Ok(());
            }
            if used == 0 && produced == 0 {
                return Err(());
            }
        }
    }
}

pub fn inspect(bytes: &[u8]) -> Format {
    let mut inspector = Inspector::default();
    inspector.update(bytes);
    inspector.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    const PNG: &[u8] = include_bytes!("../tests/fixtures/rgba.png");
    #[test]
    fn wrapped_pdf_magic_is_unclassified_without_claiming_wrapper_validity() {
        let bytes = b"rtfd\0\0\0\0unassessed wrapper containing %PDF-1.4 nested bytes";
        assert!(crate::has_pdf_header(bytes));
        let format = inspect(bytes);
        assert_eq!(format.mime, None);
        assert!(!format.corrupt_png);
        assert!(!format.error_response);
    }
    #[test]
    fn all_zero_requires_entire_nonempty_stream() {
        let zeros = vec![0; 151687];
        for chunk in [1, 65536] {
            let mut inspector = Inspector::default();
            for part in zeros.chunks(chunk) {
                inspector.update(part);
            }
            assert!(inspector.finish().all_zero);
        }
        assert!(!inspect(&[]).all_zero);
        assert!(inspect(&[]).empty);
        let mut empty = Inspector::default();
        empty.update(&[]);
        assert!(empty.finish().empty);
        let mut nonzero = zeros;
        *nonzero.last_mut().unwrap() = 1;
        assert!(!inspect(&nonzero).all_zero);
        assert!(!inspect(&nonzero).empty);
        assert_eq!(inspect(&nonzero).mime, None);
    }
    #[test]
    fn bom_error_responses_never_become_pdf_or_opaque_even_with_embedded_pdf_magic() {
        for text in [
            " \r\n<html>error %PDF-1.4</html>",
            "\t{\"error\":\"%PDF-1.4\"}",
            " [\"%PDF-1.4\"]",
        ] {
            let mut variants = vec![text.as_bytes().to_vec()];
            variants.push([b"\xef\xbb\xbf".as_slice(), text.as_bytes()].concat());
            variants.push([b" \xef\xbb\xbf\xef\xbb\xbf".as_slice(), text.as_bytes()].concat());
            for little in [true, false] {
                let mut bytes = if little {
                    vec![0xff, 0xfe]
                } else {
                    vec![0xfe, 0xff]
                };
                for u in text.encode_utf16() {
                    bytes.extend_from_slice(&if little {
                        u.to_le_bytes()
                    } else {
                        u.to_be_bytes()
                    });
                }
                variants.push(bytes);
                let mut bytes = if little {
                    vec![0xff, 0xfe, 0, 0]
                } else {
                    vec![0, 0, 0xfe, 0xff]
                };
                for c in text.chars() {
                    bytes.extend_from_slice(&if little {
                        (c as u32).to_le_bytes()
                    } else {
                        (c as u32).to_be_bytes()
                    });
                }
                variants.push(bytes);
            }
            for bytes in variants {
                for chunk in [1, 3, 64 * 1024] {
                    let mut inspector = Inspector::default();
                    for part in bytes.chunks(chunk) {
                        inspector.update(part);
                    }
                    let format = inspector.finish();
                    assert!(format.error_response, "{bytes:?}");
                    assert_eq!(format.mime, None);
                    let mut xor = QuickXor::default();
                    xor.update(&bytes);
                    assert_eq!(format.quick_xor, xor.finish());
                }
            }
        }
    }
    #[test]
    fn error_response_with_embedded_pdf_signature_is_not_a_pdf() {
        let format = inspect(b"<html>upstream error %PDF-1.4 not a document</html>");
        assert!(format.error_response);
        assert_eq!(format.mime, None);
    }
    #[test]
    fn quick_xor_reference_vectors_and_chunk_boundaries() {
        let bytes: Vec<u8> = (0..=255).collect();
        for size in [1, 3, 37, 160, 256] {
            let mut hash = QuickXor::default();
            for part in bytes.chunks(size) {
                hash.update(part);
            }
            assert_eq!(hash.finish(), "QkGEfSisZcA7k+FCh71r2dbCayY=");
        }
        for (input, expected) in [
            ("", "AAAAAAAAAAAAAAAAAAAAAAAAAAA="),
            ("a", "YQAAAAAAAAAAAAAAAQAAAAAAAAA="),
            ("abc", "YRDDGAAAAAAAAAAAAwAAAAAAAAA="),
            ("hello world", "aCgDG9jwBhDc4Q1yawMZAAAAAAA="),
        ] {
            let mut hash = QuickXor::default();
            for byte in input.as_bytes() {
                hash.update(&[*byte]);
            }
            assert_eq!(hash.finish(), expected);
        }
    }
    #[test]
    fn png_crc_zlib_scanlines_and_exact_container_are_required() {
        for chunk_size in [1, 7, 8, 13, 32, 4096] {
            let mut inspector = Inspector::default();
            for chunk in PNG.chunks(chunk_size) {
                inspector.update(chunk);
            }
            assert_eq!(inspector.finish().mime, Some("image/png"));
        }
        for end in 0..PNG.len() {
            assert_ne!(inspect(&PNG[..end]).mime, Some("image/png"));
        }
        for at in 0..PNG.len() {
            let mut bad = PNG.to_vec();
            bad[at] ^= 1;
            assert_ne!(inspect(&bad).mime, Some("image/png"), "byte {at}");
        }
        let mut trailing = PNG.to_vec();
        trailing.push(0);
        assert_eq!(inspect(&trailing).mime, None);
        assert_eq!(inspect(b"<html>error</html>").mime, None);
    }
    fn chunk(kind: &[u8; 4], bytes: &[u8]) -> Vec<u8> {
        let mut out = (bytes.len() as u32).to_be_bytes().to_vec();
        out.extend_from_slice(kind);
        out.extend_from_slice(bytes);
        let mut crc = crc32fast::Hasher::new();
        crc.update(kind);
        crc.update(bytes);
        out.extend_from_slice(&crc.finalize().to_be_bytes());
        out
    }
    fn encoded(scanlines: &[u8]) -> Vec<u8> {
        use std::io::Write;
        let mut zlib = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        zlib.write_all(scanlines).unwrap();
        zlib.finish().unwrap()
    }
    fn png_with_data(data: &[u8]) -> Vec<u8> {
        let mut out = PNG[..33].to_vec();
        out.extend(chunk(b"IDAT", data));
        out.extend(chunk(b"IEND", &[]));
        out
    }
    #[test]
    fn crc_correct_but_invalid_deflate_scanlines_or_dimensions_fail() {
        for data in [
            encoded(&[0; 17]),
            encoded(&[0; 19]),
            encoded(&[5; 18]),
            vec![1, 2, 3],
        ] {
            assert_eq!(inspect(&png_with_data(&data)).mime, None);
        }
        let mut checksum_bad = encoded(&[0; 18]);
        *checksum_bad.last_mut().unwrap() ^= 1;
        // Recompute the PNG CRC around a bad zlib checksum.
        assert!(inspect(&png_with_data(&checksum_bad)).corrupt_png);
        let mut zlib = encoded(&[0; 18]);
        zlib.push(0);
        assert_eq!(inspect(&png_with_data(&zlib)).mime, None);
        let mut bad_header = PNG[16..29].to_vec();
        bad_header[..4].copy_from_slice(&0x7fff_ffffu32.to_be_bytes());
        let mut huge = PNG[..8].to_vec();
        huge.extend(chunk(b"IHDR", &bad_header));
        huge.extend_from_slice(&PNG[33..]);
        assert_eq!(inspect(&huge).mime, None);
        assert!(
            !inspect(&huge).corrupt_png,
            "validation bound is unsupported, not corrupt"
        );
        bad_header[4..8].copy_from_slice(&0x7fff_ffffu32.to_be_bytes());
        let mut overflow = PNG[..8].to_vec();
        overflow.extend(chunk(b"IHDR", &bad_header));
        overflow.extend_from_slice(&PNG[33..]);
        assert_eq!(inspect(&overflow).mime, None);
        assert!(!inspect(&overflow).corrupt_png);
    }
    #[test]
    fn split_idat_and_adam7_scanline_framing() {
        let compressed = encoded(&[0; 18]);
        let mut split = PNG[..33].to_vec();
        for byte in &compressed {
            split.extend(chunk(b"IDAT", &[*byte]));
        }
        split.extend(chunk(b"IEND", &[]));
        assert_eq!(inspect(&split).mime, Some("image/png"));
        let mut ihdr = PNG[16..29].to_vec();
        ihdr[12] = 1;
        // 2x2 RGBA Adam7: pass1 1x1, pass6 1x1, pass7 2x1.
        let mut interlaced = PNG[..8].to_vec();
        interlaced.extend(chunk(b"IHDR", &ihdr));
        interlaced.extend(chunk(b"IDAT", &encoded(&[0; 19])));
        interlaced.extend(chunk(b"IEND", &[]));
        assert_eq!(inspect(&interlaced).mime, Some("image/png"));
    }
}
