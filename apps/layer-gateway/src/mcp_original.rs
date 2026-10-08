//! On-demand original-file fetch for MCP. The bytes live in the namespace's
//! private, content-addressed blob storage (written by `layer-extract`'s
//! `retainOriginals`). Nothing here accepts a bucket, key or URL from the
//! caller: the blob address is derived from the authorized document's own
//! row and the namespace the caller was granted, and the bytes are verified
//! against the hash the row was indexed with before they leave the gateway.
use std::time::Duration;

use base64::Engine;
use serde::Deserialize;
use serde_json::Value;

use crate::error::AppError;
use crate::AppState;

pub const DEFAULT_MAX_BYTES: u64 = 700_000;
pub const DEFAULT_MAX_PAGES: usize = 3;
pub const DEFAULT_MAX_WIDTH: u32 = 1100;
/// Hosts cap one tool result at about 1 MB; base64 of everything returned
/// stays under this.
const RESULT_BUDGET: usize = 950_000;
const RENDER_TIMEOUT: Duration = Duration::from_secs(20);

/// `originals` on an MCP namespace. Presence enables the fetch tool.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OriginalsConfig {
    /// Largest original returned whole as an embedded resource.
    pub max_bytes: Option<u64>,
    /// Page images per call.
    pub max_pages: Option<usize>,
    /// Page image width in pixels.
    pub max_width: Option<u32>,
}

impl OriginalsConfig {
    pub fn max_bytes(&self) -> u64 {
        self.max_bytes.unwrap_or(DEFAULT_MAX_BYTES)
    }
    pub fn max_pages(&self) -> usize {
        self.max_pages.unwrap_or(DEFAULT_MAX_PAGES).clamp(1, 8)
    }
    pub fn max_width(&self) -> u32 {
        self.max_width.unwrap_or(DEFAULT_MAX_WIDTH).clamp(200, 2000)
    }
    pub fn validate(&self) -> Result<(), AppError> {
        if self
            .max_bytes
            .is_some_and(|n| n == 0 || n > 8 * 1024 * 1024)
            || self.max_pages.is_some_and(|n| !(1..=8).contains(&n))
            || self.max_width.is_some_and(|n| !(200..=2000).contains(&n))
        {
            return Err(AppError::Validation(
                "MCP originals needs maxBytes 1..8388608, maxPages 1..8, maxWidth 200..2000".into(),
            ));
        }
        Ok(())
    }
}

/// What the authorized row says about its original.
#[derive(Debug, PartialEq, Eq)]
pub struct Bound {
    pub sha256: String,
    pub declared_bytes: Option<u64>,
    pub mime: String,
    pub version: Option<String>,
    identity: layer_original::Identity,
}

/// Bind the row's stored reference to this namespace and this document. The
/// reference must be exactly the one derived from the row's own indexed hash
/// (`layer-original`, the same code that wrote it), so a row edited to point
/// at another namespace's blob, at different bytes, a bucket or a URL is
/// refused rather than followed.
pub fn bind(namespace: &str, attrs: &Value) -> Result<Bound, AppError> {
    use layer_original::{Identity, Refusal};
    let identity = Identity::bound(namespace, attrs).map_err(|refusal| match refusal {
        Refusal::NotRetained
            if attrs.get("original_skipped").and_then(Value::as_str) == Some("oversize") =>
        {
            AppError::NotFound(
                "the original is over the retention size limit and was skipped \
                 (skipped-oversize); use its source link"
                    .into(),
            )
        }
        Refusal::NotRetained => AppError::NotFound(
            "no original is retained for this document; use its source link".into(),
        ),
        Refusal::NoHash => {
            AppError::Conflict("the document has no valid original hash to verify against".into())
        }
        Refusal::NotBound | Refusal::Mismatch(_) => AppError::Forbidden(
            "the stored original reference does not belong to this document and namespace".into(),
        ),
    })?;
    Ok(Bound {
        sha256: identity.sha256.clone(),
        declared_bytes: identity.bytes,
        mime: identity.mime.clone(),
        version: identity.version.clone(),
        identity,
    })
}

/// Refuse an oversize original before reading it, from the row's own size.
pub fn check_declared_size(bound: &Bound, limit: u64) -> Result<(), AppError> {
    match bound.declared_bytes {
        Some(n) if n > limit => Err(oversize(n, limit)),
        _ => Ok(()),
    }
}

fn oversize(size: u64, limit: u64) -> AppError {
    AppError::PayloadTooLarge(format!(
        "the original is {size} bytes, over this server's {limit}-byte limit; \
         request specific `pages` as images instead, or use the source link"
    ))
}

/// Verify what the store returned is the document the row was indexed from.
pub fn verify(bound: &Bound, bytes: &[u8], limit: u64) -> Result<(), AppError> {
    if let Err(layer_original::Refusal::Mismatch(why)) = bound.identity.verify(bytes) {
        return Err(AppError::Conflict(format!(
            "version mismatch: the retained original {why}; re-sync the source before relying on it"
        )));
    }
    if bytes.len() as u64 > limit {
        return Err(oversize(bytes.len() as u64, limit));
    }
    Ok(())
}

/// Read the bytes from the namespace's private blob storage, never from a
/// caller-supplied location. `hard_limit` bounds what is held in memory.
pub async fn load(
    state: &AppState,
    namespace: &str,
    bound: &Bound,
    hard_limit: u64,
) -> Result<Vec<u8>, AppError> {
    check_declared_size(bound, hard_limit)?;
    let bytes = crate::routes::blobs::read_durable_blob(state, namespace, &bound.sha256)
        .await?
        .ok_or_else(|| {
            AppError::NotFound(
                "the retained original is missing from storage (deleted or never retained); \
                 use the source link"
                    .into(),
            )
        })?;
    verify(bound, &bytes, hard_limit.max(bytes.len() as u64))?;
    Ok(bytes)
}

pub fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// Parse `pages`: distinct, ascending 1-based page numbers.
pub fn parse_pages(value: Option<Value>, max_pages: usize) -> Result<Option<Vec<u32>>, AppError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let invalid =
        || AppError::Validation(format!("pages must be 1..{max_pages} positive integers"));
    let list = value.as_array().ok_or_else(invalid)?;
    if list.is_empty() || list.len() > max_pages {
        return Err(invalid());
    }
    let mut pages = list
        .iter()
        .map(|p| {
            p.as_u64()
                .filter(|n| *n >= 1 && *n <= 100_000)
                .map(|n| n as u32)
        })
        .collect::<Option<Vec<_>>>()
        .ok_or_else(invalid)?;
    pages.sort_unstable();
    pages.dedup();
    Ok(Some(pages))
}

pub struct Rendered {
    pub page: u32,
    pub png: Vec<u8>,
}

pub struct RenderOutcome {
    pub images: Vec<Rendered>,
    pub page_count: u32,
    /// Requested pages left out because the result would exceed the host limit.
    pub omitted: Vec<u32>,
}

/// Rasterize the requested pages. Bounded in width, bytes and time.
pub async fn render_pages(
    pdf: Vec<u8>,
    pages: Vec<u32>,
    max_width: u32,
) -> Result<RenderOutcome, AppError> {
    let task = tokio::task::spawn_blocking(move || render_blocking(pdf, &pages, max_width));
    match tokio::time::timeout(RENDER_TIMEOUT, task).await {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err(AppError::Upstream("page rendering failed".into())),
        Err(_) => Err(AppError::GatewayTimeout(
            "rendering the pages took too long; request fewer pages".into(),
        )),
    }
}

fn render_blocking(pdf: Vec<u8>, pages: &[u32], max_width: u32) -> Result<RenderOutcome, AppError> {
    use hayro::hayro_interpret::InterpreterSettings;
    use hayro::hayro_syntax::Pdf;
    use hayro::{render, PixmapSettings, RenderCache, RenderSettings};

    let cannot = || AppError::Conflict("the original PDF cannot be rendered".into());
    let document = Pdf::new(pdf).map_err(|_| cannot())?;
    let all = document.pages();
    let page_count = all.len() as u32;
    if let Some(bad) = pages.iter().find(|p| **p > page_count) {
        return Err(AppError::Validation(format!(
            "page {bad} is out of range; the document has {page_count} pages"
        )));
    }
    let cache = RenderCache::new();
    let settings = InterpreterSettings::default();
    let mut images = Vec::new();
    let mut omitted = Vec::new();
    let mut used = 0usize;
    for number in pages {
        let page = &all[*number as usize - 1];
        let (width, _) = page.render_dimensions();
        let mut scale = (max_width as f32 / width.max(1.0)).min(4.0);
        let mut png = None;
        // Shrink until one page fits in half the budget, so a dense scan
        // cannot silently crowd out the others.
        for _ in 0..4 {
            let pixmap = render(
                page,
                &cache,
                &settings,
                &RenderSettings::default(),
                &PixmapSettings {
                    x_scale: scale,
                    y_scale: scale,
                    bg_color: hayro::vello_cpu::color::palette::css::WHITE,
                },
            );
            let encoded = pixmap.into_png().map_err(|_| cannot())?;
            let fits = encoded.len() * 4 / 3 + 4 <= RESULT_BUDGET / 2;
            png = Some(encoded);
            if fits {
                break;
            }
            scale *= 0.7;
        }
        let png = png.ok_or_else(cannot)?;
        let size = png.len() * 4 / 3 + 4;
        if used + size > RESULT_BUDGET {
            omitted.push(*number);
            continue;
        }
        used += size;
        images.push(Rendered { page: *number, png });
    }
    Ok(RenderOutcome {
        images,
        page_count,
        omitted,
    })
}

/// A one-page PDF with a red square and a blue bar and no text at all: the
/// information exists only as pixels, as a scanned or drawn page would.
#[doc(hidden)]
pub fn synthetic_pdf() -> Vec<u8> {
    let content = "1 0 0 rg 50 100 100 100 re f 0 0 1 rg 50 250 200 30 re f";
    let objects = [
        "<< /Type /Catalog /Pages 2 0 R >>".to_string(),
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>".to_string(),
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 300] /Contents 4 0 R /Resources << >> >>"
            .to_string(),
        format!(
            "<< /Length {} >>\nstream\n{content}\nendstream",
            content.len()
        ),
    ];
    let mut out = b"%PDF-1.4\n".to_vec();
    let mut offsets = Vec::new();
    for (i, body) in objects.iter().enumerate() {
        offsets.push(out.len());
        out.extend(format!("{} 0 obj\n{body}\nendobj\n", i + 1).bytes());
    }
    let xref = out.len();
    out.extend(format!("xref\n0 {}\n0000000000 65535 f \n", objects.len() + 1).bytes());
    for offset in offsets {
        out.extend(format!("{offset:010} 00000 n \n").bytes());
    }
    out.extend(
        format!(
            "trailer\n<< /Size {} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n",
            objects.len() + 1
        )
        .bytes(),
    );
    out
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use serde_json::json;

    fn row(ns: &str, pdf: &[u8]) -> Value {
        let sha = layer_original::sha256_hex(pdf);
        json!({
            "original_blob": format!("blob://{ns}/{sha}"),
            "original_sha256": sha,
            "original_bytes": pdf.len(),
            "original_mime": "application/pdf",
            "webUrl": "https://example.sharepoint.com/doc.pdf",
        })
    }

    #[test]
    fn binds_only_to_this_namespace_and_the_indexed_hash() {
        let pdf = synthetic_pdf();
        let attrs = row("docs", &pdf);
        let bound = bind("docs", &attrs).unwrap();
        assert_eq!(bound.sha256, layer_original::sha256_hex(&pdf));
        // Cross-namespace: the same row read through another namespace.
        assert!(matches!(bind("other", &attrs), Err(AppError::Forbidden(_))));
        // Tampered reference: points at another namespace or other bytes.
        let mut tampered = attrs.clone();
        tampered["original_blob"] =
            json!(format!("blob://other/{}", layer_original::sha256_hex(&pdf)));
        assert!(matches!(
            bind("docs", &tampered),
            Err(AppError::Forbidden(_))
        ));
        let mut swapped = attrs.clone();
        swapped["original_blob"] = json!(format!(
            "blob://docs/{}",
            layer_original::sha256_hex(b"other")
        ));
        assert!(matches!(
            bind("docs", &swapped),
            Err(AppError::Forbidden(_))
        ));
        // Arbitrary locations are never followed.
        let mut url = attrs.clone();
        url["original_blob"] = json!("s3://bucket/key");
        assert!(matches!(bind("docs", &url), Err(AppError::Forbidden(_))));
        // Missing reference and missing hash.
        assert!(matches!(
            bind("docs", &json!({})),
            Err(AppError::NotFound(_))
        ));
        let mut no_hash = attrs;
        no_hash.as_object_mut().unwrap().remove("original_sha256");
        assert!(matches!(bind("docs", &no_hash), Err(AppError::Conflict(_))));
    }

    #[test]
    fn verify_rejects_wrong_bytes_size_type_and_oversize() {
        let pdf = synthetic_pdf();
        let bound = bind("docs", &row("docs", &pdf)).unwrap();
        verify(&bound, &pdf, 1 << 20).unwrap();
        let mut other = pdf.clone();
        other.push(b'\n');
        assert!(matches!(
            verify(&bound, &other, 1 << 20),
            Err(AppError::Conflict(m)) if m.contains("version mismatch")
        ));
        assert!(matches!(
            verify(&bound, &pdf, 10),
            Err(AppError::PayloadTooLarge(_))
        ));
        assert!(matches!(
            check_declared_size(&bound, 10),
            Err(AppError::PayloadTooLarge(_))
        ));
        let not_pdf = b"hello".to_vec();
        let mut attrs = row("docs", &not_pdf);
        let bound = bind("docs", &attrs).unwrap();
        assert!(matches!(
            verify(&bound, &not_pdf, 1 << 20),
            Err(AppError::Conflict(_))
        ));
        attrs["original_bytes"] = json!(99);
        let bound = bind("docs", &attrs).unwrap();
        assert!(matches!(
            verify(&bound, &not_pdf, 1 << 20),
            Err(AppError::Conflict(_))
        ));
    }

    #[test]
    fn a_skipped_oversize_document_says_so_instead_of_pretending_nothing_was_tried() {
        let skipped = json!({"original_skipped": "oversize", "original_skipped_bytes": 30_000_000});
        match bind("docs", &skipped) {
            Err(AppError::NotFound(message)) => assert!(message.contains("skipped-oversize")),
            other => panic!("{other:?}"),
        }
        match bind("docs", &json!({})) {
            Err(AppError::NotFound(message)) => assert!(!message.contains("skipped")),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn pages_are_validated_and_bounded() {
        assert_eq!(parse_pages(None, 3).unwrap(), None);
        assert_eq!(
            parse_pages(Some(json!([3, 1, 3])), 3).unwrap(),
            Some(vec![1, 3])
        );
        for bad in [
            json!([]),
            json!([0]),
            json!([1, 2, 3, 4]),
            json!("1"),
            json!([1.5]),
        ] {
            assert!(parse_pages(Some(bad), 3).is_err());
        }
    }

    #[tokio::test]
    async fn renders_visual_only_content_to_png_pixels() {
        let outcome = render_pages(synthetic_pdf(), vec![1], 300).await.unwrap();
        assert_eq!(outcome.page_count, 1);
        let image = image::load_from_memory(&outcome.images[0].png)
            .unwrap()
            .to_rgba8();
        assert_eq!(image.width(), 300);
        // PDF origin is bottom-left: the red square spans y 100..200 of 300.
        let red = image.get_pixel(100, 300 - 150);
        let blue = image.get_pixel(150, 300 - 265);
        let white = image.get_pixel(280, 5);
        assert_eq!([red[0], red[1], red[2]], [255, 0, 0]);
        assert_eq!([blue[0], blue[1], blue[2]], [0, 0, 255]);
        assert_eq!([white[0], white[1], white[2]], [255, 255, 255]);
        assert!(matches!(
            render_pages(synthetic_pdf(), vec![2], 300).await,
            Err(AppError::Validation(_))
        ));
        assert!(render_pages(b"%PDF-garbage".to_vec(), vec![1], 300)
            .await
            .is_err());
    }
}
