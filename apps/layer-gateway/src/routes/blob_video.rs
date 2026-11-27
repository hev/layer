//! Container checks for staged videos. This inspects container metadata without
//! decoding untrusted media or invoking an external player.

use super::blob_uploads::VideoType;

const MAX_SECONDS: u64 = 60;

pub(super) fn valid_video(bytes: &[u8], kind: &VideoType) -> bool {
    match kind {
        VideoType::Mp4 | VideoType::Quicktime => bmff(bytes, kind).is_some(),
        VideoType::Webm => webm(bytes).is_some(),
    }
}

fn bmff(bytes: &[u8], kind: &VideoType) -> Option<()> {
    let mut boxes = Boxes(bytes);
    let (tag, ftyp) = boxes.next()??;
    let (brands, remainder) = ftyp.as_chunks::<4>();
    let [major, _, compatible @ ..] = brands else {
        return None;
    };
    if tag != *b"ftyp" || !remainder.is_empty() {
        return None;
    }
    let quicktime = major == b"qt  ";
    let mp4 = std::iter::once(major).chain(compatible).any(|brand| {
        matches!(
            brand,
            b"isom"
                | b"iso2"
                | b"iso3"
                | b"iso4"
                | b"iso5"
                | b"iso6"
                | b"iso7"
                | b"iso8"
                | b"iso9"
                | b"mp41"
                | b"mp42"
                | b"avc1"
                | b"M4V "
                | b"hvc1"
                | b"av01"
        )
    });
    if (*kind == VideoType::Quicktime && !quicktime)
        || (*kind == VideoType::Mp4 && (quicktime || !mp4))
    {
        return None;
    }

    let (mut movie, mut media) = (None, false);
    for item in boxes {
        let (tag, data) = item?;
        match &tag {
            b"moov" if movie.is_none() => movie = Some(data),
            b"moov" | b"moof" => return None,
            b"mdat" => media |= !data.is_empty(),
            _ => {}
        }
    }
    let (mut duration, mut track) = (false, false);
    for item in Boxes(movie?) {
        let (tag, data) = item?;
        match &tag {
            b"mvhd" if !duration => {
                let (timescale, ticks) = match data.first()? {
                    0 => (uint(data.get(12..16)?), uint(data.get(16..20)?)),
                    1 => (uint(data.get(20..24)?), uint(data.get(24..32)?)),
                    _ => return None,
                };
                if ticks == 0 || ticks > timescale * MAX_SECONDS {
                    return None;
                }
                duration = true;
            }
            b"mvhd" | b"mvex" => return None,
            b"trak" => {
                for child in Boxes(data) {
                    let (tag, data) = child?;
                    if tag == *b"mdia" {
                        for child in Boxes(data) {
                            let (tag, data) = child?;
                            track |= tag == *b"hdlr" && data.get(8..12) == Some(b"vide");
                        }
                    }
                }
            }
            _ => {}
        }
    }
    (media && duration && track).then_some(())
}

struct Boxes<'a>(&'a [u8]);

impl<'a> Iterator for Boxes<'a> {
    type Item = Option<([u8; 4], &'a [u8])>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_empty() {
            return None;
        }
        let bytes = std::mem::take(&mut self.0);
        let (size, tag, header): (_, [u8; 4], _) = match bytes.get(..8) {
            Some(fixed) => {
                let size = uint(&fixed[..4]);
                let tag = fixed[4..8].try_into().ok()?;
                match size {
                    0 => (bytes.len() as u64, tag, 8),
                    1 => match bytes.get(8..16) {
                        Some(ext) => (uint(ext), tag, 16),
                        None => return Some(None),
                    },
                    _ => (size, tag, 8),
                }
            }
            None => return Some(None),
        };
        if size < header || size > bytes.len() as u64 {
            return Some(None);
        }
        self.0 = &bytes[size as usize..];
        Some(Some((tag, &bytes[header as usize..size as usize])))
    }
}

fn webm(bytes: &[u8]) -> Option<()> {
    let mut top = Elements(bytes);
    let (header, data) = top.next()??;
    if header != 0x1a45dfa3 {
        return None;
    }
    let mut webm_type = false;
    for item in Elements(data) {
        let (tag, data) = item?;
        if tag == 0x4282 {
            if webm_type || data != b"webm" {
                return None;
            }
            webm_type = true;
        }
    }
    let (segment, data) = top.next()??;
    if !webm_type || segment != 0x18538067 || top.next().is_some() {
        return None;
    }
    let (mut duration, mut video, mut cluster) = (false, false, false);
    for item in Elements(data) {
        let (tag, data) = item?;
        match tag {
            0x1549a966 if !duration => {
                let mut scale = 1_000_000;
                let mut ticks = None;
                for item in Elements(data) {
                    let (tag, data) = item?;
                    match (tag, data.len()) {
                        (0x2ad7b1, 1..=8) => scale = uint(data),
                        (0x4489, 4) if ticks.is_none() => {
                            ticks = Some(f32::from_bits(uint(data) as u32) as f64);
                        }
                        (0x4489, 8) if ticks.is_none() => {
                            ticks = Some(f64::from_bits(uint(data)));
                        }
                        (0x2ad7b1 | 0x4489, _) => return None,
                        _ => {}
                    }
                }
                let seconds = ticks? * scale as f64 / 1e9;
                if !(seconds > 0.0 && seconds <= MAX_SECONDS as f64) {
                    return None;
                }
                duration = true;
            }
            0x1549a966 => return None,
            0x1654ae6b => {
                for item in Elements(data) {
                    let (tag, data) = item?;
                    if tag == 0xae {
                        for item in Elements(data) {
                            let (tag, data) = item?;
                            video |= tag == 0x83 && data == [1];
                        }
                    }
                }
            }
            0x1f43b675 => cluster |= !data.is_empty(),
            _ => {}
        }
    }
    (duration && video && cluster).then_some(())
}

struct Elements<'a>(&'a [u8]);

impl<'a> Iterator for Elements<'a> {
    type Item = Option<(u64, &'a [u8])>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_empty() {
            return None;
        }
        let bytes = std::mem::take(&mut self.0);
        let parse = || {
            let (tag, _, tag_width) = vint(bytes, 4)?;
            let (encoded, size, size_width) = vint(&bytes[tag_width..], 8)?;
            let payload = &bytes[tag_width + size_width..];
            let unknown = encoded == (1 << (size_width * 7 + 1)) - 1;
            let size = if unknown && matches!(tag, 0x18538067 | 0x1f43b675) {
                payload.len()
            } else if unknown {
                return None;
            } else {
                usize::try_from(size).ok()?
            };
            Some((tag, payload.get(..size)?, payload.get(size..)?))
        };
        Some(parse().map(|(tag, data, rest)| {
            self.0 = rest;
            (tag, data)
        }))
    }
}

fn vint(bytes: &[u8], max_width: usize) -> Option<(u64, u64, usize)> {
    let width = bytes.first()?.leading_zeros() as usize + 1;
    if width > max_width {
        return None;
    }
    let encoded = uint(bytes.get(..width)?);
    Some((encoded, encoded ^ (1 << (width * 7)), width))
}

fn uint(bytes: &[u8]) -> u64 {
    bytes
        .iter()
        .fold(0, |value, &byte| (value << 8) | u64::from(byte))
}
