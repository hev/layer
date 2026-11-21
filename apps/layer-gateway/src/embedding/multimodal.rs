//! Multimodal embedding inputs, model profiles and the embedder client for
//! `google/embeddinggemma-2` (text, image and video in one 768d space).
//!
//! The existing [`EmbeddingProvider`] text and image paths stay unchanged.
//! This module adds:
//!
//! - [`EmbedInput`], a typed input that is text, an image, or a video, with
//!   the media either inline bytes or a reference resolved by a bounded
//!   [`MediaResolver`].
//! - [`MultimodalModelProfile`] and [`EMBEDDINGGEMMA_2`]: dimensions, MRL
//!   truncation sizes, modalities, byte caps and the video duration cap.
//! - [`validate_input`]: byte sniffing (the declared MIME type is never
//!   trusted), SVG refusal, size and duration caps, and the modality check
//!   that fails closed when media reaches a text-only model.
//! - [`MultimodalEmbedderClient`]: the HTTP client for the
//!   `apps/layer-multimodal-embedder` wire (`GET /health`, `POST /v1/embed`)
//!   with explicit timeouts, a request payload cap computed before the body
//!   is built, and a streamed response read that stops at its cap.
//!
//! Every rejection is a [`MultimodalError`] whose [`MultimodalError::status`]
//! is `415` for an unsupported modality or media type and `422` for a
//! corrupt, oversized or overlong input.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::http::StatusCode;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{
    is_clip_model, EmbeddingBatch, EmbeddingError, EmbeddingProvider, EmbeddingPurpose,
    EmbeddingRequest,
};

/// Model id of EmbeddingGemma 2. EmbeddingGemma 1
/// (`google/embeddinggemma-300m`) is text-only and is not this model.
pub const EMBEDDINGGEMMA_2_MODEL: &str = "google/embeddinggemma-2";

pub const MIB: u64 = 1024 * 1024;
/// Largest image accepted, in bytes.
pub const MAX_IMAGE_BYTES: u64 = 10 * MIB;
/// Largest video accepted, in bytes (whole-clip v1).
pub const MAX_VIDEO_BYTES: u64 = 100 * MIB;
/// Longest video accepted, in milliseconds (whole-clip v1).
pub const MAX_VIDEO_DURATION_MS: u64 = 60_000;
/// Longest text input accepted, in bytes. EmbeddingGemma 2 reads 8K tokens;
/// this bounds the bytes before tokenization.
pub const MAX_TEXT_BYTES: u64 = 64 * 1024;

/// What an embedding input is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputModality {
    Text,
    Image,
    Video,
}

impl InputModality {
    pub fn label(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Image => "image",
            Self::Video => "video",
        }
    }
}

/// Media types the gateway accepts, identified from the bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MediaType {
    #[serde(rename = "image/png")]
    Png,
    #[serde(rename = "image/jpeg")]
    Jpeg,
    #[serde(rename = "image/gif")]
    Gif,
    #[serde(rename = "image/webp")]
    Webp,
    #[serde(rename = "video/mp4")]
    Mp4,
    #[serde(rename = "video/webm")]
    Webm,
    #[serde(rename = "video/quicktime")]
    QuickTime,
}

impl MediaType {
    pub const ALL: [MediaType; 7] = [
        Self::Png,
        Self::Jpeg,
        Self::Gif,
        Self::Webp,
        Self::Mp4,
        Self::Webm,
        Self::QuickTime,
    ];

    pub fn mime(self) -> &'static str {
        match self {
            Self::Png => "image/png",
            Self::Jpeg => "image/jpeg",
            Self::Gif => "image/gif",
            Self::Webp => "image/webp",
            Self::Mp4 => "video/mp4",
            Self::Webm => "video/webm",
            Self::QuickTime => "video/quicktime",
        }
    }

    pub fn modality(self) -> InputModality {
        match self {
            Self::Png | Self::Jpeg | Self::Gif | Self::Webp => InputModality::Image,
            Self::Mp4 | Self::Webm | Self::QuickTime => InputModality::Video,
        }
    }

    /// Parse a declared MIME type (parameters and case ignored). `None` for
    /// anything outside the accepted set, SVG included.
    pub fn from_mime(mime: &str) -> Option<Self> {
        let essence = mime
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        Self::ALL.into_iter().find(|kind| kind.mime() == essence)
    }
}

/// Inline media or a reference the caller's [`MediaResolver`] can fetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MediaSource {
    Bytes(Vec<u8>),
    Ref(String),
}

/// One embedding input. Text-only callers keep using
/// [`EmbeddingProvider::embed`]; this type is for callers that mix
/// modalities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmbedInput {
    Text(String),
    Image {
        /// Declared MIME type. Checked against the sniffed type when set.
        mime: Option<String>,
        bytes_or_ref: MediaSource,
    },
    Video {
        mime: Option<String>,
        bytes_or_ref: MediaSource,
        /// Declared clip length. The container header is authoritative when
        /// it carries one; the longer of the two is enforced.
        duration_ms: Option<u64>,
    },
}

impl EmbedInput {
    pub fn modality(&self) -> InputModality {
        match self {
            Self::Text(_) => InputModality::Text,
            Self::Image { .. } => InputModality::Image,
            Self::Video { .. } => InputModality::Video,
        }
    }

    /// Replace a [`MediaSource::Ref`] with bytes from `resolver`, bounded by
    /// the profile's byte cap for the input's modality.
    pub async fn resolve(
        self,
        resolver: &dyn MediaResolver,
        profile: &MultimodalModelProfile,
    ) -> Result<Self, MultimodalError> {
        let modality = self.modality();
        let max_bytes = profile.max_bytes(modality);
        let fetch = |reference: String| async move {
            let bytes = resolver.fetch(&reference, max_bytes).await?;
            if bytes.len() as u64 > max_bytes {
                return Err(MultimodalError::MediaTooLarge {
                    modality,
                    bytes: bytes.len() as u64,
                    max_bytes,
                });
            }
            Ok(bytes)
        };
        Ok(match self {
            Self::Image {
                mime,
                bytes_or_ref: MediaSource::Ref(reference),
            } => Self::Image {
                mime,
                bytes_or_ref: MediaSource::Bytes(fetch(reference).await?),
            },
            Self::Video {
                mime,
                bytes_or_ref: MediaSource::Ref(reference),
                duration_ms,
            } => Self::Video {
                mime,
                bytes_or_ref: MediaSource::Bytes(fetch(reference).await?),
                duration_ms,
            },
            other => other,
        })
    }
}

/// Fetches referenced media. Implementations must stop reading after
/// `max_bytes` (returning [`MultimodalError::MediaTooLarge`]) rather than
/// buffer an unbounded object.
#[async_trait]
pub trait MediaResolver: Send + Sync {
    async fn fetch(&self, reference: &str, max_bytes: u64) -> Result<Vec<u8>, MultimodalError>;
}

/// A validated media input, shared cheaply across federated search legs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedMedia {
    pub media_type: MediaType,
    pub bytes: Arc<[u8]>,
    /// Effective duration for video; `None` for images.
    pub duration_ms: Option<u64>,
}

impl ValidatedMedia {
    pub fn modality(&self) -> InputModality {
        self.media_type.modality()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidatedInput {
    Text(String),
    Media(ValidatedMedia),
}

impl ValidatedInput {
    pub fn modality(&self) -> InputModality {
        match self {
            Self::Text(_) => InputModality::Text,
            Self::Media(media) => media.modality(),
        }
    }
}

/// What a model accepts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultimodalModelProfile {
    pub id: &'static str,
    pub default_dimensions: u64,
    /// Matryoshka sizes a vector may be truncated to (then renormalized).
    pub mrl_dimensions: &'static [u64],
    pub modalities: &'static [InputModality],
    pub accepted_media: &'static [MediaType],
    pub max_text_bytes: u64,
    pub max_image_bytes: u64,
    pub max_video_bytes: u64,
    pub max_video_duration_ms: u64,
    pub max_context_tokens: u64,
}

pub const EMBEDDINGGEMMA_2: MultimodalModelProfile = MultimodalModelProfile {
    id: EMBEDDINGGEMMA_2_MODEL,
    default_dimensions: 768,
    mrl_dimensions: &[128, 256, 512, 768],
    modalities: &[
        InputModality::Text,
        InputModality::Image,
        InputModality::Video,
    ],
    accepted_media: &MediaType::ALL,
    max_text_bytes: MAX_TEXT_BYTES,
    max_image_bytes: MAX_IMAGE_BYTES,
    max_video_bytes: MAX_VIDEO_BYTES,
    max_video_duration_ms: MAX_VIDEO_DURATION_MS,
    max_context_tokens: 8192,
};

impl MultimodalModelProfile {
    pub fn supports(&self, modality: InputModality) -> bool {
        self.modalities.contains(&modality)
    }

    pub fn max_bytes(&self, modality: InputModality) -> u64 {
        match modality {
            InputModality::Text => self.max_text_bytes,
            InputModality::Image => self.max_image_bytes,
            InputModality::Video => self.max_video_bytes,
        }
    }

    /// The output size for a request: the default when unset, otherwise one
    /// of the MRL sizes.
    pub fn resolve_dimensions(&self, requested: Option<u64>) -> Result<u64, MultimodalError> {
        match requested {
            None => Ok(self.default_dimensions),
            Some(dims) if self.mrl_dimensions.contains(&dims) => Ok(dims),
            Some(dims) => Err(MultimodalError::UnsupportedDimensions {
                model: self.id.to_string(),
                dimensions: dims,
                supported: self.mrl_dimensions.to_vec(),
            }),
        }
    }
}

/// The multimodal profile of a known model, if it has one.
pub fn multimodal_profile(model: &str) -> Option<&'static MultimodalModelProfile> {
    (model == EMBEDDINGGEMMA_2_MODEL).then_some(&EMBEDDINGGEMMA_2)
}

/// The modalities a model id accepts. Unknown models are text-only, so new
/// media never reaches a model that was not declared to read it.
pub fn model_modalities(model: &str) -> &'static [InputModality] {
    if let Some(profile) = multimodal_profile(model) {
        profile.modalities
    } else if is_clip_model(model) {
        &[InputModality::Text, InputModality::Image]
    } else {
        &[InputModality::Text]
    }
}

/// Every way an input or an embedder call can be refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MultimodalError {
    #[error("model `{model}` does not accept {} inputs; it accepts {}", .modality.label(), labels(.supported))]
    ModalityUnsupported {
        model: String,
        modality: InputModality,
        supported: Vec<InputModality>,
    },
    #[error("SVG is refused: embed a raster image (PNG, JPEG, GIF or WebP) instead")]
    SvgRefused,
    #[error("unsupported media type `{0}`; accepted: image/png, image/jpeg, image/gif, image/webp, video/mp4, video/webm, video/quicktime")]
    UnsupportedMediaType(String),
    #[error("{} input is not a valid PNG, JPEG, GIF, WebP, MP4, WebM or QuickTime file: {reason}", .modality.label())]
    CorruptMedia {
        modality: InputModality,
        reason: String,
    },
    #[error("declared media type `{declared}` does not match the bytes, which are `{}`", .sniffed.mime())]
    MediaTypeMismatch {
        declared: String,
        sniffed: MediaType,
    },
    #[error("{} input is {bytes} bytes; the limit is {max_bytes} bytes", .modality.label())]
    MediaTooLarge {
        modality: InputModality,
        bytes: u64,
        max_bytes: u64,
    },
    #[error("video is {duration_ms} ms long; the limit is {max_duration_ms} ms")]
    VideoTooLong {
        duration_ms: u64,
        max_duration_ms: u64,
    },
    #[error("video duration could not be read from the container and was not declared; send `duration_ms`")]
    VideoDurationUnknown,
    #[error("text input is empty")]
    EmptyText,
    #[error("media reference `{0}` must be resolved before embedding")]
    UnresolvedReference(String),
    #[error(
        "model `{model}` does not produce {dimensions}-dimension vectors; supported: {supported:?}"
    )]
    UnsupportedDimensions {
        model: String,
        dimensions: u64,
        supported: Vec<u64>,
    },
    #[error("request is invalid: {0}")]
    InvalidRequest(String),
    #[error("multimodal embedder is unavailable: {0}")]
    Unavailable(String),
    #[error("multimodal embedder timed out: {0}")]
    Timeout(String),
    #[error("multimodal embedder failed: {0}")]
    Upstream(String),
}

fn labels(modalities: &[InputModality]) -> String {
    modalities
        .iter()
        .map(|modality| modality.label())
        .collect::<Vec<_>>()
        .join(", ")
}

impl MultimodalError {
    /// The public HTTP status: `415` for a modality or media type the model
    /// cannot read, `422` for a readable but invalid input, `503`, `504` and
    /// `502` for embedder failures.
    pub fn status(&self) -> StatusCode {
        match self {
            Self::ModalityUnsupported { .. } | Self::SvgRefused | Self::UnsupportedMediaType(_) => {
                StatusCode::UNSUPPORTED_MEDIA_TYPE
            }
            Self::CorruptMedia { .. }
            | Self::MediaTypeMismatch { .. }
            | Self::MediaTooLarge { .. }
            | Self::VideoTooLong { .. }
            | Self::VideoDurationUnknown
            | Self::EmptyText
            | Self::UnresolvedReference(_)
            | Self::UnsupportedDimensions { .. }
            | Self::InvalidRequest(_) => StatusCode::UNPROCESSABLE_ENTITY,
            Self::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::Timeout(_) => StatusCode::GATEWAY_TIMEOUT,
            Self::Upstream(_) => StatusCode::BAD_GATEWAY,
        }
    }

    /// Stable machine-readable code, shared with the embedder service.
    pub fn code(&self) -> &'static str {
        match self {
            Self::ModalityUnsupported { .. } => "modality_unsupported",
            Self::SvgRefused => "svg_refused",
            Self::UnsupportedMediaType(_) => "unsupported_media_type",
            Self::CorruptMedia { .. } => "corrupt_media",
            Self::MediaTypeMismatch { .. } => "media_type_mismatch",
            Self::MediaTooLarge { .. } => "media_too_large",
            Self::VideoTooLong { .. } => "video_too_long",
            Self::VideoDurationUnknown => "video_duration_unknown",
            Self::EmptyText => "empty_text",
            Self::UnresolvedReference(_) => "unresolved_reference",
            Self::UnsupportedDimensions { .. } => "unsupported_dimensions",
            Self::InvalidRequest(_) => "invalid_request",
            Self::Unavailable(_) => "unavailable",
            Self::Timeout(_) => "timeout",
            Self::Upstream(_) => "upstream",
        }
    }
}

impl From<MultimodalError> for EmbeddingError {
    fn from(error: MultimodalError) -> Self {
        match error {
            MultimodalError::Unavailable(message) => Self::Unavailable(message),
            MultimodalError::Timeout(message) => Self::Timeout(message),
            MultimodalError::Upstream(message) => Self::Upstream(message),
            other => Self::Validation(other.to_string()),
        }
    }
}

impl axum::response::IntoResponse for MultimodalError {
    fn into_response(self) -> axum::response::Response {
        let body = json!({ "error": { "code": self.code(), "message": self.to_string() } });
        (self.status(), axum::Json(body)).into_response()
    }
}

// --- Sniffing ---

/// What the bytes are, independent of any declared MIME type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sniffed {
    pub media_type: MediaType,
    /// Duration read from the container header (MP4/QuickTime `mvhd`, WebM
    /// `Info/Duration`), when present.
    pub duration_ms: Option<u64>,
}

/// Identify media from its leading bytes and validate the container header.
pub fn sniff_media(bytes: &[u8]) -> Result<Sniffed, MultimodalError> {
    if looks_like_svg(bytes) {
        return Err(MultimodalError::SvgRefused);
    }
    let corrupt = |modality, reason: &str| MultimodalError::CorruptMedia {
        modality,
        reason: reason.to_string(),
    };
    let image = |media_type| {
        Ok(Sniffed {
            media_type,
            duration_ms: None,
        })
    };
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        if bytes.len() < 24 || &bytes[12..16] != b"IHDR" {
            return Err(corrupt(
                InputModality::Image,
                "PNG is missing its IHDR header",
            ));
        }
        let width = u32::from_be_bytes(bytes[16..20].try_into().expect("4 bytes"));
        let height = u32::from_be_bytes(bytes[20..24].try_into().expect("4 bytes"));
        if width == 0 || height == 0 {
            return Err(corrupt(
                InputModality::Image,
                "PNG has zero width or height",
            ));
        }
        return image(MediaType::Png);
    }
    if bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
        if bytes.len() < 4 {
            return Err(corrupt(InputModality::Image, "JPEG is truncated"));
        }
        return image(MediaType::Jpeg);
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        if bytes.len() < 13 {
            return Err(corrupt(
                InputModality::Image,
                "GIF is missing its screen descriptor",
            ));
        }
        return image(MediaType::Gif);
    }
    if bytes.starts_with(b"RIFF") && bytes.len() >= 12 && &bytes[8..12] == b"WEBP" {
        if bytes.len() < 16 || !matches!(&bytes[12..16], b"VP8 " | b"VP8L" | b"VP8X") {
            return Err(corrupt(
                InputModality::Image,
                "WebP is missing its VP8 chunk header",
            ));
        }
        return image(MediaType::Webp);
    }
    if bytes.len() >= 8 && &bytes[4..8] == b"ftyp" {
        return sniff_iso_bmff(bytes);
    }
    if bytes.starts_with(&[0x1A, 0x45, 0xDF, 0xA3]) {
        return sniff_webm(bytes);
    }
    Err(MultimodalError::UnsupportedMediaType(
        "unrecognized bytes".to_string(),
    ))
}

fn looks_like_svg(bytes: &[u8]) -> bool {
    let head = &bytes[..bytes.len().min(1024)];
    let head = head.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(head);
    let start = head
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(head.len());
    let head = &head[start..];
    if !head.starts_with(b"<") {
        return false;
    }
    let lower = head.to_ascii_lowercase();
    lower.windows(4).any(|window| window == b"<svg")
}

fn read_u32(bytes: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_be_bytes(bytes.get(at..at + 4)?.try_into().ok()?))
}

fn read_u64(bytes: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_be_bytes(bytes.get(at..at + 8)?.try_into().ok()?))
}

/// One ISO-BMFF box header: `(header_len, total_len)`. `None` when the box
/// does not fit in `bytes` or declares a size smaller than its header.
fn bmff_box(bytes: &[u8], at: usize) -> Option<(usize, usize, [u8; 4])> {
    let size = read_u32(bytes, at)? as u64;
    let kind: [u8; 4] = bytes.get(at + 4..at + 8)?.try_into().ok()?;
    let (header, total) = match size {
        0 => (8, (bytes.len() - at) as u64),
        1 => (16, read_u64(bytes, at + 8)?),
        size => (8, size),
    };
    if total < header as u64 || at as u64 + total > bytes.len() as u64 {
        return None;
    }
    Some((header, total as usize, kind))
}

fn sniff_iso_bmff(bytes: &[u8]) -> Result<Sniffed, MultimodalError> {
    let corrupt = |reason: &str| MultimodalError::CorruptMedia {
        modality: InputModality::Video,
        reason: reason.to_string(),
    };
    let (_, ftyp_len, _) = bmff_box(bytes, 0).ok_or_else(|| corrupt("ftyp box is truncated"))?;
    if ftyp_len < 16 {
        return Err(corrupt("ftyp box is too short"));
    }
    let brand = &bytes[8..12];
    let media_type = if brand == b"qt  " {
        MediaType::QuickTime
    } else if brand.starts_with(b"3g") {
        return Err(MultimodalError::UnsupportedMediaType(format!(
            "video/3gpp (brand {})",
            String::from_utf8_lossy(brand)
        )));
    } else {
        MediaType::Mp4
    };
    // Walk top-level boxes (bounded by the input) to `moov`, then its
    // children to `mvhd`.
    let mut at = ftyp_len;
    let mut duration_ms = None;
    let mut boxes = 0;
    while at < bytes.len() && boxes < 1024 {
        boxes += 1;
        let Some((header, total, kind)) = bmff_box(bytes, at) else {
            // A truncated trailing box (e.g. a streamed `mdat`) ends the
            // walk; the duration may simply be unknown.
            break;
        };
        if &kind == b"moov" {
            duration_ms = mvhd_duration(&bytes[at + header..at + total]);
            if duration_ms.is_none() {
                return Err(corrupt("moov box has no readable mvhd header"));
            }
            break;
        }
        at += total;
    }
    Ok(Sniffed {
        media_type,
        duration_ms,
    })
}

fn mvhd_duration(moov: &[u8]) -> Option<u64> {
    let mut at = 0;
    while at < moov.len() {
        let (header, total, kind) = bmff_box(moov, at)?;
        if &kind == b"mvhd" {
            let body = &moov[at + header..at + total];
            let version = *body.first()?;
            let (timescale, duration) = if version == 1 {
                (read_u32(body, 20)? as u64, read_u64(body, 24)?)
            } else {
                (read_u32(body, 12)? as u64, read_u32(body, 16)? as u64)
            };
            if timescale == 0 {
                return None;
            }
            return Some(duration.saturating_mul(1000) / timescale);
        }
        at += total;
    }
    None
}

/// An EBML variable-length integer at `at`: `(value, len)`. When `marker` is
/// true the length marker bit is kept (element ids); otherwise it is masked
/// (sizes).
fn ebml_vint(bytes: &[u8], at: usize, marker: bool) -> Option<(u64, usize)> {
    let first = *bytes.get(at)?;
    if first == 0 {
        return None;
    }
    let len = first.leading_zeros() as usize + 1;
    if len > 8 {
        return None;
    }
    let mut value = if marker {
        first as u64
    } else {
        (first & (0xFF >> len)) as u64
    };
    for index in 1..len {
        value = (value << 8) | *bytes.get(at + index)? as u64;
    }
    Some((value, len))
}

/// The EBML element at `at`: `(id, data_start, data_end)`. An unknown size
/// (all ones) extends to the end of `bytes`.
fn ebml_element(bytes: &[u8], at: usize) -> Option<(u64, usize, usize)> {
    let (id, id_len) = ebml_vint(bytes, at, true)?;
    let (size, size_len) = ebml_vint(bytes, at + id_len, false)?;
    let start = at + id_len + size_len;
    let unknown = size == (1_u64 << (7 * size_len)) - 1;
    let end = if unknown {
        bytes.len()
    } else {
        start.checked_add(usize::try_from(size).ok()?)?
    };
    Some((id, start, end.min(bytes.len())))
}

const EBML_HEADER: u64 = 0x1A45_DFA3;
const EBML_DOC_TYPE: u64 = 0x4282;
const MKV_SEGMENT: u64 = 0x1853_8067;
const MKV_INFO: u64 = 0x1549_A966;
const MKV_TIMECODE_SCALE: u64 = 0x2A_D7B1;
const MKV_DURATION: u64 = 0x4489;

fn sniff_webm(bytes: &[u8]) -> Result<Sniffed, MultimodalError> {
    let corrupt = |reason: &str| MultimodalError::CorruptMedia {
        modality: InputModality::Video,
        reason: reason.to_string(),
    };
    let (id, start, end) =
        ebml_element(bytes, 0).ok_or_else(|| corrupt("EBML header is truncated"))?;
    if id != EBML_HEADER || end > bytes.len() {
        return Err(corrupt("EBML header is invalid"));
    }
    let mut doc_type = None;
    let mut at = start;
    while at < end {
        let (child, data, child_end) =
            ebml_element(bytes, at).ok_or_else(|| corrupt("EBML header is truncated"))?;
        if child == EBML_DOC_TYPE {
            doc_type = Some(
                String::from_utf8_lossy(&bytes[data..child_end])
                    .trim_end_matches('\0')
                    .to_string(),
            );
        }
        at = child_end;
    }
    match doc_type.as_deref() {
        Some("webm") => {}
        Some(other) => {
            return Err(MultimodalError::UnsupportedMediaType(format!(
                "EBML doc type `{other}` (only WebM is accepted)"
            )))
        }
        None => return Err(corrupt("EBML header has no DocType")),
    }
    Ok(Sniffed {
        media_type: MediaType::Webm,
        duration_ms: webm_duration(bytes, end),
    })
}

fn webm_duration(bytes: &[u8], mut at: usize) -> Option<u64> {
    let (id, start, end) = ebml_element(bytes, at)?;
    if id != MKV_SEGMENT {
        return None;
    }
    at = start;
    let mut children = 0;
    while at < end && children < 256 {
        children += 1;
        let (child, data, child_end) = ebml_element(bytes, at)?;
        if child == MKV_INFO {
            let mut scale = 1_000_000_u64;
            let mut duration = None;
            let mut inner = data;
            while inner < child_end {
                let (field, value, value_end) = ebml_element(bytes, inner)?;
                let raw = &bytes[value..value_end];
                match field {
                    MKV_TIMECODE_SCALE => {
                        scale = raw
                            .iter()
                            .fold(0_u64, |acc, byte| (acc << 8) | *byte as u64)
                    }
                    MKV_DURATION => {
                        duration = match raw.len() {
                            4 => Some(f32::from_be_bytes(raw.try_into().ok()?) as f64),
                            8 => Some(f64::from_be_bytes(raw.try_into().ok()?)),
                            _ => None,
                        }
                    }
                    _ => {}
                }
                inner = value_end;
            }
            let duration = duration?;
            if !duration.is_finite() || duration < 0.0 {
                return None;
            }
            return Some((duration * scale as f64 / 1_000_000.0).ceil() as u64);
        }
        at = child_end;
    }
    None
}

// --- Validation ---

/// Validate one input against `profile`: modality, then bytes, then caps.
/// Media must already be resolved to bytes.
pub fn validate_input(
    profile: &MultimodalModelProfile,
    input: &EmbedInput,
) -> Result<ValidatedInput, MultimodalError> {
    validate_for_modalities(profile.id, profile.modalities, Some(profile), input)
}

/// Validate one input for any model id. Media sent to a model that is not
/// declared to read it fails with `415` before its bytes are inspected.
pub fn validate_input_for_model(
    model: &str,
    input: &EmbedInput,
) -> Result<ValidatedInput, MultimodalError> {
    validate_for_modalities(
        model,
        model_modalities(model),
        multimodal_profile(model),
        input,
    )
}

fn validate_for_modalities(
    model: &str,
    modalities: &[InputModality],
    profile: Option<&MultimodalModelProfile>,
    input: &EmbedInput,
) -> Result<ValidatedInput, MultimodalError> {
    let modality = input.modality();
    if !modalities.contains(&modality) {
        return Err(MultimodalError::ModalityUnsupported {
            model: model.to_string(),
            modality,
            supported: modalities.to_vec(),
        });
    }
    let profile = profile.unwrap_or(&EMBEDDINGGEMMA_2);
    let (mime, source, declared_duration) = match input {
        EmbedInput::Text(text) => {
            if text.trim().is_empty() {
                return Err(MultimodalError::EmptyText);
            }
            if text.len() as u64 > profile.max_text_bytes {
                return Err(MultimodalError::MediaTooLarge {
                    modality,
                    bytes: text.len() as u64,
                    max_bytes: profile.max_text_bytes,
                });
            }
            return Ok(ValidatedInput::Text(text.clone()));
        }
        EmbedInput::Image { mime, bytes_or_ref } => (mime, bytes_or_ref, None),
        EmbedInput::Video {
            mime,
            bytes_or_ref,
            duration_ms,
        } => (mime, bytes_or_ref, *duration_ms),
    };
    let bytes = match source {
        MediaSource::Bytes(bytes) => bytes,
        MediaSource::Ref(reference) => {
            return Err(MultimodalError::UnresolvedReference(reference.clone()))
        }
    };
    let declared = match mime.as_deref() {
        Some(mime) if mime.to_ascii_lowercase().contains("svg") => {
            return Err(MultimodalError::SvgRefused)
        }
        Some(mime) => Some(
            MediaType::from_mime(mime)
                .ok_or_else(|| MultimodalError::UnsupportedMediaType(mime.to_string()))?,
        ),
        None => None,
    };
    let max_bytes = profile.max_bytes(modality);
    if bytes.len() as u64 > max_bytes {
        return Err(MultimodalError::MediaTooLarge {
            modality,
            bytes: bytes.len() as u64,
            max_bytes,
        });
    }
    let sniffed = match sniff_media(bytes) {
        Ok(sniffed) => sniffed,
        // Declared as a supported type but unreadable: a corrupt file, not
        // an unsupported one.
        Err(MultimodalError::UnsupportedMediaType(_)) if declared.is_some() => {
            return Err(MultimodalError::CorruptMedia {
                modality,
                reason: format!(
                    "bytes do not start with a {} header",
                    declared.expect("checked").mime()
                ),
            })
        }
        Err(error) => return Err(error),
    };
    if let Some(declared) = declared {
        if declared != sniffed.media_type {
            return Err(MultimodalError::MediaTypeMismatch {
                declared: declared.mime().to_string(),
                sniffed: sniffed.media_type,
            });
        }
    }
    if sniffed.media_type.modality() != modality {
        return Err(MultimodalError::MediaTypeMismatch {
            declared: format!("{} input", modality.label()),
            sniffed: sniffed.media_type,
        });
    }
    if !profile.accepted_media.contains(&sniffed.media_type) {
        return Err(MultimodalError::UnsupportedMediaType(
            sniffed.media_type.mime().to_string(),
        ));
    }
    let duration_ms = if modality == InputModality::Video {
        let duration = match (sniffed.duration_ms, declared_duration) {
            (Some(header), Some(declared)) => header.max(declared),
            (Some(duration), None) | (None, Some(duration)) => duration,
            (None, None) => return Err(MultimodalError::VideoDurationUnknown),
        };
        if duration > profile.max_video_duration_ms {
            return Err(MultimodalError::VideoTooLong {
                duration_ms: duration,
                max_duration_ms: profile.max_video_duration_ms,
            });
        }
        Some(duration)
    } else {
        None
    };
    Ok(ValidatedInput::Media(ValidatedMedia {
        media_type: sniffed.media_type,
        bytes: Arc::from(bytes.as_slice()),
        duration_ms,
    }))
}

// --- MRL ---

/// Truncate a Matryoshka vector to `dimensions` and L2-renormalize it.
pub fn truncate_mrl(
    profile: &MultimodalModelProfile,
    vector: &[f64],
    dimensions: u64,
) -> Result<Vec<f64>, MultimodalError> {
    let dims = profile.resolve_dimensions(Some(dimensions))? as usize;
    if vector.len() < dims {
        return Err(MultimodalError::InvalidRequest(format!(
            "cannot truncate a {}-dimension vector to {dims}",
            vector.len()
        )));
    }
    let mut truncated = vector[..dims].to_vec();
    let norm = truncated
        .iter()
        .map(|value| value * value)
        .sum::<f64>()
        .sqrt();
    if !norm.is_finite() || norm == 0.0 {
        return Err(MultimodalError::Upstream(
            "vector has zero or non-finite norm".to_string(),
        ));
    }
    for value in &mut truncated {
        *value /= norm;
    }
    Ok(truncated)
}

// --- Client ---

/// Bounds for [`MultimodalEmbedderClient`].
#[derive(Debug, Clone)]
pub struct MultimodalClientOptions {
    pub connect_timeout: Duration,
    pub health_timeout: Duration,
    /// Whole-call budget for one `/v1/embed` request, response read included.
    pub request_timeout: Duration,
    /// Largest serialized request body. Checked from the input sizes before
    /// any body is built.
    pub max_request_bytes: u64,
    pub max_response_bytes: usize,
    pub max_batch_inputs: usize,
}

impl Default for MultimodalClientOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(1),
            health_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            // One base64-encoded 100 MiB video plus envelope.
            max_request_bytes: 140 * MIB,
            max_response_bytes: 4 * MIB as usize,
            max_batch_inputs: 32,
        }
    }
}

const MAX_CLIENT_TIMEOUT: Duration = Duration::from_secs(300);
const UNIT_NORM_TOLERANCE: f64 = 1e-3;
const ENVELOPE_BYTES: u64 = 1024;
const PER_INPUT_ENVELOPE_BYTES: u64 = 128;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct EmbedderHealth {
    pub status: String,
    pub model: String,
    pub backend: String,
    pub modalities: Vec<InputModality>,
    pub dimensions: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MultimodalEmbeddings {
    pub model: String,
    pub dimensions: u64,
    pub vectors: Vec<Vec<f64>>,
}

#[derive(Debug, Deserialize)]
struct EmbedResponse {
    model: String,
    dimensions: u64,
    embeddings: Vec<Vec<f64>>,
}

#[derive(Debug, Deserialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    code: String,
    #[serde(default)]
    message: String,
}

/// HTTP client for `apps/layer-multimodal-embedder`.
pub struct MultimodalEmbedderClient {
    origin: String,
    client: reqwest::Client,
    options: MultimodalClientOptions,
    profile: &'static MultimodalModelProfile,
}

impl std::fmt::Debug for MultimodalEmbedderClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultimodalEmbedderClient")
            .field("origin", &self.origin)
            .field("model", &self.profile.id)
            .finish_non_exhaustive()
    }
}

impl MultimodalEmbedderClient {
    /// A client for an operator-controlled origin such as
    /// `http://multimodal-embedder:8090`. Paths, credentials, queries and
    /// zero or unbounded timeouts are refused; redirects and proxies are off.
    pub fn new(
        url: &str,
        profile: &'static MultimodalModelProfile,
        options: MultimodalClientOptions,
    ) -> Result<Self, String> {
        let parsed = reqwest::Url::parse(url.trim())
            .map_err(|error| format!("multimodal embedder URL `{url}` is invalid: {error}"))?;
        if !matches!(parsed.scheme(), "http" | "https")
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || !matches!(parsed.path(), "" | "/")
        {
            return Err(format!(
                "multimodal embedder URL `{url}` must be an http(s) origin without credentials, path, query or fragment"
            ));
        }
        for (name, timeout) in [
            ("connect", options.connect_timeout),
            ("health", options.health_timeout),
            ("request", options.request_timeout),
        ] {
            if timeout.is_zero() || timeout > MAX_CLIENT_TIMEOUT {
                return Err(format!(
                    "multimodal embedder {name} timeout must be between 1ms and {}s",
                    MAX_CLIENT_TIMEOUT.as_secs()
                ));
            }
        }
        if options.max_batch_inputs == 0
            || options.max_response_bytes == 0
            || options.max_request_bytes <= ENVELOPE_BYTES
        {
            return Err("multimodal embedder payload caps must be non-zero".to_string());
        }
        let client = reqwest::Client::builder()
            .connect_timeout(options.connect_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .user_agent(concat!("hevlayer-gateway/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|error| format!("could not construct multimodal embedder client: {error}"))?;
        let origin = parsed.as_str().trim_end_matches('/').to_string();
        Ok(Self {
            origin,
            client,
            options,
            profile,
        })
    }

    pub fn profile(&self) -> &'static MultimodalModelProfile {
        self.profile
    }

    pub async fn health(&self) -> Result<EmbedderHealth, MultimodalError> {
        let timeout = self.options.health_timeout;
        let call = async {
            let response = self
                .client
                .get(format!("{}/health", self.origin))
                .timeout(timeout)
                .send()
                .await
                .map_err(transport_error)?;
            let status = response.status().as_u16();
            let body = read_bounded(response, self.options.max_response_bytes).await?;
            if status != 200 {
                return Err(map_error(status, &body));
            }
            serde_json::from_slice::<EmbedderHealth>(&body).map_err(|error| {
                MultimodalError::Upstream(format!("invalid health document: {error}"))
            })
        };
        tokio::time::timeout(timeout, call)
            .await
            .map_err(|_| MultimodalError::Timeout(format!("health exceeded {timeout:?}")))?
    }

    /// Validate and embed `inputs` in one call. Validation runs before any
    /// network I/O, so a refused input never reaches the embedder.
    pub async fn embed(
        &self,
        inputs: &[EmbedInput],
        dimensions: Option<u64>,
        purpose: EmbeddingPurpose,
    ) -> Result<MultimodalEmbeddings, MultimodalError> {
        let validated = inputs
            .iter()
            .map(|input| validate_input(self.profile, input))
            .collect::<Result<Vec<_>, _>>()?;
        self.embed_validated(&validated, dimensions, purpose).await
    }

    pub async fn embed_validated(
        &self,
        inputs: &[ValidatedInput],
        dimensions: Option<u64>,
        purpose: EmbeddingPurpose,
    ) -> Result<MultimodalEmbeddings, MultimodalError> {
        let dimensions = self.profile.resolve_dimensions(dimensions)?;
        if inputs.is_empty() {
            return Ok(MultimodalEmbeddings {
                model: self.profile.id.to_string(),
                dimensions,
                vectors: Vec::new(),
            });
        }
        if inputs.len() > self.options.max_batch_inputs {
            return Err(MultimodalError::InvalidRequest(format!(
                "{} inputs exceed the batch limit of {}",
                inputs.len(),
                self.options.max_batch_inputs
            )));
        }
        let estimated = ENVELOPE_BYTES
            + inputs
                .iter()
                .map(|input| {
                    PER_INPUT_ENVELOPE_BYTES
                        + match input {
                            // JSON escaping can at most sextuple a byte.
                            ValidatedInput::Text(text) => text.len() as u64 * 6,
                            ValidatedInput::Media(media) => {
                                (media.bytes.len() as u64).div_ceil(3) * 4
                            }
                        }
                })
                .sum::<u64>();
        if estimated > self.options.max_request_bytes {
            return Err(MultimodalError::InvalidRequest(format!(
                "request would be about {estimated} bytes; the embedder payload cap is {} bytes",
                self.options.max_request_bytes
            )));
        }
        let wire_inputs: Vec<Value> = inputs.iter().map(wire_input).collect();
        let body = json!({
            "model": self.profile.id,
            "dimensions": dimensions,
            "purpose": purpose.label(),
            "inputs": wire_inputs,
        });
        let timeout = self.options.request_timeout;
        let call = async {
            let response = self
                .client
                .post(format!("{}/v1/embed", self.origin))
                .timeout(timeout)
                .json(&body)
                .send()
                .await
                .map_err(transport_error)?;
            let status = response.status().as_u16();
            let bytes = read_bounded(response, self.options.max_response_bytes).await?;
            if status != 200 {
                return Err(map_error(status, &bytes));
            }
            serde_json::from_slice::<EmbedResponse>(&bytes).map_err(|error| {
                MultimodalError::Upstream(format!("invalid embed response: {error}"))
            })
        };
        let response = tokio::time::timeout(timeout, call)
            .await
            .map_err(|_| MultimodalError::Timeout(format!("embed exceeded {timeout:?}")))??;
        self.verify(response, inputs.len(), dimensions)
    }

    fn verify(
        &self,
        response: EmbedResponse,
        count: usize,
        dimensions: u64,
    ) -> Result<MultimodalEmbeddings, MultimodalError> {
        if response.model != self.profile.id {
            return Err(MultimodalError::Upstream(format!(
                "embedder answered with model `{}`, expected `{}`",
                response.model, self.profile.id
            )));
        }
        if response.dimensions != dimensions || response.embeddings.len() != count {
            return Err(MultimodalError::Upstream(format!(
                "embedder returned {} vectors of {} dimensions, expected {count} of {dimensions}",
                response.embeddings.len(),
                response.dimensions
            )));
        }
        for (index, vector) in response.embeddings.iter().enumerate() {
            if vector.len() as u64 != dimensions || vector.iter().any(|value| !value.is_finite()) {
                return Err(MultimodalError::Upstream(format!(
                    "embedding {index} has the wrong length or a non-finite value"
                )));
            }
            let norm = vector.iter().map(|value| value * value).sum::<f64>().sqrt();
            if (norm - 1.0).abs() > UNIT_NORM_TOLERANCE {
                return Err(MultimodalError::Upstream(format!(
                    "embedding {index} is not unit-normalized (norm {norm})"
                )));
            }
        }
        Ok(MultimodalEmbeddings {
            model: response.model,
            dimensions,
            vectors: response.embeddings,
        })
    }
}

fn wire_input(input: &ValidatedInput) -> Value {
    match input {
        ValidatedInput::Text(text) => json!({ "type": "text", "text": text }),
        ValidatedInput::Media(media) => {
            let mut value = json!({
                "type": media.modality().label(),
                "mime": media.media_type.mime(),
                "data_b64": BASE64_STANDARD.encode(&media.bytes),
            });
            if let Some(duration_ms) = media.duration_ms {
                value["duration_ms"] = json!(duration_ms);
            }
            value
        }
    }
}

fn transport_error(error: reqwest::Error) -> MultimodalError {
    if error.is_timeout() {
        MultimodalError::Timeout(error.to_string())
    } else if error.is_connect() {
        MultimodalError::Unavailable(error.to_string())
    } else {
        MultimodalError::Upstream(error.to_string())
    }
}

async fn read_bounded(
    response: reqwest::Response,
    max_bytes: usize,
) -> Result<Vec<u8>, MultimodalError> {
    let too_large =
        || MultimodalError::Upstream(format!("embedder response exceeds {max_bytes} bytes"));
    if response
        .content_length()
        .is_some_and(|length| length > max_bytes as u64)
    {
        return Err(too_large());
    }
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(transport_error)?;
        if body.len() + chunk.len() > max_bytes {
            return Err(too_large());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// Map an embedder error response back to the same refusal the gateway
/// would have produced, so a check that only the service could make (for
/// example a decoded duration) keeps its `415`/`422` status.
fn map_error(status: u16, body: &[u8]) -> MultimodalError {
    let parsed = serde_json::from_slice::<ErrorEnvelope>(body).ok();
    let message = parsed
        .as_ref()
        .map(|envelope| format!("{}: {}", envelope.error.code, envelope.error.message))
        .unwrap_or_else(|| format!("HTTP {status}"));
    match status {
        415 => {
            if parsed
                .as_ref()
                .is_some_and(|envelope| envelope.error.code == "svg_refused")
            {
                MultimodalError::SvgRefused
            } else {
                MultimodalError::UnsupportedMediaType(message)
            }
        }
        400 | 413 | 422 => MultimodalError::InvalidRequest(message),
        503 => MultimodalError::Unavailable(message),
        504 => MultimodalError::Timeout(message),
        _ => MultimodalError::Upstream(message),
    }
}

#[async_trait]
impl EmbeddingProvider for MultimodalEmbedderClient {
    async fn embed(
        &self,
        request: &EmbeddingRequest<'_>,
        texts: &[String],
    ) -> Result<EmbeddingBatch, EmbeddingError> {
        self.provider_call(
            request,
            texts.iter().cloned().map(EmbedInput::Text).collect(),
        )
        .await
    }

    async fn embed_images(
        &self,
        request: &EmbeddingRequest<'_>,
        images: &[Vec<u8>],
    ) -> Result<EmbeddingBatch, EmbeddingError> {
        self.provider_call(
            request,
            images
                .iter()
                .map(|bytes| EmbedInput::Image {
                    mime: None,
                    bytes_or_ref: MediaSource::Bytes(bytes.clone()),
                })
                .collect(),
        )
        .await
    }
}

impl MultimodalEmbedderClient {
    async fn provider_call(
        &self,
        request: &EmbeddingRequest<'_>,
        inputs: Vec<EmbedInput>,
    ) -> Result<EmbeddingBatch, EmbeddingError> {
        // This wire has no artifact fingerprint, so a profile pinned to one
        // cannot be verified and is refused rather than silently served.
        if request.model != self.profile.id
            || request.revision.is_some()
            || request.artifact.is_some()
        {
            return Err(EmbeddingError::Validation(format!(
                "multimodal embedder serves `{}` without revision pins, not `{}`",
                self.profile.id,
                request.provider_model()
            )));
        }
        let mut vectors = Vec::with_capacity(inputs.len());
        for chunk in inputs.chunks(self.options.max_batch_inputs) {
            let output =
                MultimodalEmbedderClient::embed(self, chunk, request.dims, request.purpose).await?;
            vectors.extend(output.vectors);
        }
        Ok(EmbeddingBatch {
            vectors,
            performance: json!({}),
            billing: None,
        })
    }
}

#[cfg(test)]
pub(crate) mod fixtures {
    //! Minimal valid container headers for tests.

    pub fn png() -> Vec<u8> {
        let mut bytes = b"\x89PNG\r\n\x1a\n\x00\x00\x00\x0dIHDR".to_vec();
        bytes.extend_from_slice(&1_u32.to_be_bytes());
        bytes.extend_from_slice(&1_u32.to_be_bytes());
        bytes.extend_from_slice(&[8, 6, 0, 0, 0, 0, 0, 0, 0]);
        bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn png() -> Vec<u8> {
        fixtures::png()
    }

    fn jpeg() -> Vec<u8> {
        vec![0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, b'J', b'F', b'I', b'F']
    }

    fn gif() -> Vec<u8> {
        let mut bytes = b"GIF89a".to_vec();
        bytes.extend_from_slice(&[1, 0, 1, 0, 0, 0, 0]);
        bytes
    }

    fn webp() -> Vec<u8> {
        let mut bytes = b"RIFF".to_vec();
        bytes.extend_from_slice(&26_u32.to_le_bytes());
        bytes.extend_from_slice(b"WEBPVP8 ");
        bytes.extend_from_slice(&[0; 14]);
        bytes
    }

    fn bmff_box(kind: &[u8; 4], body: &[u8]) -> Vec<u8> {
        let mut bytes = ((body.len() + 8) as u32).to_be_bytes().to_vec();
        bytes.extend_from_slice(kind);
        bytes.extend_from_slice(body);
        bytes
    }

    pub(super) fn iso_video(brand: &[u8; 4], duration_ms: u32) -> Vec<u8> {
        let mut ftyp = brand.to_vec();
        ftyp.extend_from_slice(&[0, 0, 2, 0]);
        ftyp.extend_from_slice(brand);
        let mut mvhd = vec![0; 4]; // version 0, flags
        mvhd.extend_from_slice(&[0; 8]); // creation, modification
        mvhd.extend_from_slice(&1000_u32.to_be_bytes()); // timescale
        mvhd.extend_from_slice(&duration_ms.to_be_bytes());
        mvhd.extend_from_slice(&[0; 80]);
        let mut bytes = bmff_box(b"ftyp", &ftyp);
        bytes.extend(bmff_box(b"mdat", &[0; 32]));
        bytes.extend(bmff_box(b"moov", &bmff_box(b"mvhd", &mvhd)));
        bytes
    }

    fn ebml(id: &[u8], body: &[u8]) -> Vec<u8> {
        assert!(body.len() < 127);
        let mut bytes = id.to_vec();
        bytes.push(0x80 | body.len() as u8);
        bytes.extend_from_slice(body);
        bytes
    }

    fn webm(duration_ms: f64, doc_type: &str) -> Vec<u8> {
        let mut bytes = ebml(
            &[0x1A, 0x45, 0xDF, 0xA3],
            &ebml(&[0x42, 0x82], doc_type.as_bytes()),
        );
        let mut info = ebml(&[0x2A, 0xD7, 0xB1], &[0x0F, 0x42, 0x40]); // 1_000_000 ns
        info.extend(ebml(&[0x44, 0x89], &duration_ms.to_be_bytes()));
        bytes.extend(ebml(
            &[0x18, 0x53, 0x80, 0x67],
            &ebml(&[0x15, 0x49, 0xA9, 0x66], &info),
        ));
        bytes
    }

    fn image(bytes: Vec<u8>, mime: Option<&str>) -> EmbedInput {
        EmbedInput::Image {
            mime: mime.map(str::to_string),
            bytes_or_ref: MediaSource::Bytes(bytes),
        }
    }

    fn video(bytes: Vec<u8>, mime: Option<&str>, duration_ms: Option<u64>) -> EmbedInput {
        EmbedInput::Video {
            mime: mime.map(str::to_string),
            bytes_or_ref: MediaSource::Bytes(bytes),
            duration_ms,
        }
    }

    fn validate(input: &EmbedInput) -> Result<ValidatedInput, MultimodalError> {
        validate_input(&EMBEDDINGGEMMA_2, input)
    }

    #[test]
    fn embeddinggemma_2_profile_is_registered() {
        let profile = multimodal_profile("google/embeddinggemma-2").unwrap();
        assert_eq!(profile.default_dimensions, 768);
        assert_eq!(profile.mrl_dimensions, &[128, 256, 512, 768]);
        assert_eq!(profile.max_video_bytes, 100 * MIB);
        assert_eq!(profile.max_video_duration_ms, 60_000);
        assert_eq!(profile.max_image_bytes, 10 * MIB);
        assert_eq!(
            model_modalities("google/embeddinggemma-2"),
            &[
                InputModality::Text,
                InputModality::Image,
                InputModality::Video
            ]
        );
        // EmbeddingGemma 1 is text-only and is not this profile.
        assert!(multimodal_profile("google/embeddinggemma-300m").is_none());
        assert_eq!(
            model_modalities("google/embeddinggemma-300m"),
            &[InputModality::Text]
        );
    }

    #[test]
    fn accepts_text_and_every_supported_image() {
        assert_eq!(
            validate(&EmbedInput::Text("a red bicycle".into())).unwrap(),
            ValidatedInput::Text("a red bicycle".into())
        );
        for (bytes, mime, expected) in [
            (png(), "image/png", MediaType::Png),
            (jpeg(), "image/jpeg", MediaType::Jpeg),
            (gif(), "image/gif", MediaType::Gif),
            (webp(), "image/webp", MediaType::Webp),
        ] {
            for declared in [Some(mime), None] {
                let ValidatedInput::Media(media) = validate(&image(bytes.clone(), declared))
                    .unwrap_or_else(|error| panic!("{mime}: {error}"))
                else {
                    panic!("expected media");
                };
                assert_eq!(media.media_type, expected);
                assert_eq!(media.duration_ms, None);
            }
        }
    }

    #[test]
    fn accepts_mp4_webm_and_quicktime_within_caps() {
        for (bytes, mime, expected) in [
            (iso_video(b"isom", 12_000), "video/mp4", MediaType::Mp4),
            (
                iso_video(b"qt  ", 60_000),
                "video/quicktime",
                MediaType::QuickTime,
            ),
            (webm(30_000.0, "webm"), "video/webm", MediaType::Webm),
        ] {
            let ValidatedInput::Media(media) = validate(&video(bytes, Some(mime), None))
                .unwrap_or_else(|error| panic!("{mime}: {error}"))
            else {
                panic!("expected media");
            };
            assert_eq!(media.media_type, expected);
            assert!(media.duration_ms.unwrap() <= 60_000);
        }
    }

    #[test]
    fn refuses_svg_by_bytes_and_by_mime() {
        let svg = br#"<?xml version="1.0"?><svg xmlns="http://www.w3.org/2000/svg"/>"#.to_vec();
        let error = validate(&image(svg.clone(), None)).unwrap_err();
        assert_eq!(error, MultimodalError::SvgRefused);
        assert_eq!(error.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_eq!(
            validate(&image(png(), Some("image/svg+xml"))).unwrap_err(),
            MultimodalError::SvgRefused
        );
        assert_eq!(
            validate(&image(svg, Some("image/png"))).unwrap_err(),
            MultimodalError::SvgRefused
        );
    }

    #[test]
    fn refuses_corrupt_headers() {
        let mut truncated_png = png();
        truncated_png.truncate(12);
        for (bytes, mime) in [
            (truncated_png, Some("image/png")),
            (b"GIF89a".to_vec(), None),
            (b"RIFF\0\0\0\0WEBPJUNK".to_vec(), None),
            (b"not an image at all".to_vec(), Some("image/jpeg")),
        ] {
            let error = validate(&image(bytes, mime)).unwrap_err();
            assert!(
                matches!(error, MultimodalError::CorruptMedia { .. }),
                "{error:?}"
            );
            assert_eq!(error.status(), StatusCode::UNPROCESSABLE_ENTITY);
        }
        let mut broken_mp4 = iso_video(b"isom", 1_000);
        broken_mp4[0..4].copy_from_slice(&4096_u32.to_be_bytes());
        let error = validate(&video(broken_mp4, Some("video/mp4"), Some(1_000))).unwrap_err();
        assert!(
            matches!(error, MultimodalError::CorruptMedia { .. }),
            "{error:?}"
        );
    }

    #[test]
    fn refuses_unknown_bytes_and_mime_types_with_415() {
        let error = validate(&image(b"BM\0\0".to_vec(), None)).unwrap_err();
        assert_eq!(error.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        let error = validate(&image(png(), Some("image/tiff"))).unwrap_err();
        assert_eq!(error.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        let error = validate(&video(webm(1_000.0, "matroska"), None, None)).unwrap_err();
        assert_eq!(error.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[test]
    fn refuses_mismatched_declarations() {
        let error = validate(&image(png(), Some("image/jpeg"))).unwrap_err();
        assert!(matches!(error, MultimodalError::MediaTypeMismatch { .. }));
        let error = validate(&image(iso_video(b"isom", 1_000), None)).unwrap_err();
        assert!(matches!(error, MultimodalError::MediaTypeMismatch { .. }));
        assert_eq!(error.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[test]
    fn enforces_image_byte_cap() {
        let mut at_cap = png();
        at_cap.resize(MAX_IMAGE_BYTES as usize, 0);
        assert!(validate(&image(at_cap.clone(), None)).is_ok());
        at_cap.push(0);
        let error = validate(&image(at_cap, None)).unwrap_err();
        assert!(matches!(error, MultimodalError::MediaTooLarge { .. }));
        assert_eq!(error.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[test]
    fn enforces_video_byte_cap() {
        let mut over = iso_video(b"isom", 1_000);
        over.resize(MAX_VIDEO_BYTES as usize + 1, 0);
        let error = validate(&video(over, Some("video/mp4"), None)).unwrap_err();
        assert!(matches!(
            error,
            MultimodalError::MediaTooLarge {
                modality: InputModality::Video,
                ..
            }
        ));
        assert_eq!(error.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[test]
    fn enforces_video_duration_cap_from_header_and_declaration() {
        let error = validate(&video(iso_video(b"isom", 60_001), None, None)).unwrap_err();
        assert_eq!(
            error,
            MultimodalError::VideoTooLong {
                duration_ms: 60_001,
                max_duration_ms: 60_000
            }
        );
        let error = validate(&video(webm(61_000.0, "webm"), None, None)).unwrap_err();
        assert!(matches!(error, MultimodalError::VideoTooLong { .. }));
        // A short header cannot hide a declared long clip.
        let error = validate(&video(iso_video(b"isom", 1_000), None, Some(90_000))).unwrap_err();
        assert!(matches!(error, MultimodalError::VideoTooLong { .. }));
        assert_eq!(error.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[test]
    fn video_without_any_duration_fails_closed() {
        let mut ftyp = b"isom".to_vec();
        ftyp.extend_from_slice(&[0, 0, 2, 0]);
        ftyp.extend_from_slice(b"isom");
        let bytes = bmff_box(b"ftyp", &ftyp);
        assert_eq!(
            validate(&video(bytes.clone(), None, None)).unwrap_err(),
            MultimodalError::VideoDurationUnknown
        );
        let ValidatedInput::Media(media) = validate(&video(bytes, None, Some(5_000))).unwrap()
        else {
            panic!("expected media")
        };
        assert_eq!(media.duration_ms, Some(5_000));
    }

    #[test]
    fn media_to_text_only_model_fails_closed_with_415() {
        for model in [
            "google/embeddinggemma-300m",
            "sentence-transformers/all-MiniLM-L6-v2",
            "openai/text-embedding-3-small",
        ] {
            assert!(validate_input_for_model(model, &EmbedInput::Text("ok".into())).is_ok());
            for input in [
                image(png(), Some("image/png")),
                video(iso_video(b"isom", 1_000), Some("video/mp4"), None),
            ] {
                let error = validate_input_for_model(model, &input).unwrap_err();
                assert!(
                    matches!(error, MultimodalError::ModalityUnsupported { .. }),
                    "{model}: {error:?}"
                );
                assert_eq!(error.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
                assert_eq!(error.code(), "modality_unsupported");
            }
        }
        // CLIP reads images but not video.
        let clip = "openai/clip-vit-base-patch32";
        assert!(validate_input_for_model(clip, &image(png(), None)).is_ok());
        let error = validate_input_for_model(clip, &video(iso_video(b"isom", 1_000), None, None))
            .unwrap_err();
        assert_eq!(error.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[test]
    fn unresolved_references_and_empty_text_are_refused() {
        let input = EmbedInput::Image {
            mime: None,
            bytes_or_ref: MediaSource::Ref("blob://evidence/1".into()),
        };
        assert_eq!(
            validate(&input).unwrap_err(),
            MultimodalError::UnresolvedReference("blob://evidence/1".into())
        );
        assert_eq!(
            validate(&EmbedInput::Text("  ".into())).unwrap_err(),
            MultimodalError::EmptyText
        );
    }

    struct FixedResolver(Vec<u8>);

    #[async_trait]
    impl MediaResolver for FixedResolver {
        async fn fetch(&self, _: &str, _max: u64) -> Result<Vec<u8>, MultimodalError> {
            Ok(self.0.clone())
        }
    }

    #[tokio::test]
    async fn resolver_output_is_bounded_and_validated() {
        let input = EmbedInput::Image {
            mime: None,
            bytes_or_ref: MediaSource::Ref("blob://x".into()),
        };
        let resolved = input
            .clone()
            .resolve(&FixedResolver(png()), &EMBEDDINGGEMMA_2)
            .await
            .unwrap();
        assert!(validate(&resolved).is_ok());
        let error = input
            .resolve(
                &FixedResolver(vec![0; MAX_IMAGE_BYTES as usize + 1]),
                &EMBEDDINGGEMMA_2,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, MultimodalError::MediaTooLarge { .. }));
    }

    #[test]
    fn mrl_truncation_returns_exact_unit_vectors() {
        let full: Vec<f64> = (0..768).map(|index| ((index % 13) as f64) - 6.5).collect();
        for dims in [128_u64, 256, 512, 768] {
            let truncated = truncate_mrl(&EMBEDDINGGEMMA_2, &full, dims).unwrap();
            assert_eq!(truncated.len() as u64, dims);
            let norm = truncated.iter().map(|v| v * v).sum::<f64>().sqrt();
            assert!((norm - 1.0).abs() < 1e-12, "{dims}: {norm}");
        }
        for dims in [0_u64, 64, 384, 1024] {
            let error = truncate_mrl(&EMBEDDINGGEMMA_2, &full, dims).unwrap_err();
            assert!(matches!(
                error,
                MultimodalError::UnsupportedDimensions { .. }
            ));
            assert_eq!(error.status(), StatusCode::UNPROCESSABLE_ENTITY);
        }
        assert_eq!(EMBEDDINGGEMMA_2.resolve_dimensions(None).unwrap(), 768);
    }

    #[test]
    fn client_refuses_unbounded_configuration() {
        let ok = MultimodalClientOptions::default();
        assert!(MultimodalEmbedderClient::new(
            "http://embedder:8090",
            &EMBEDDINGGEMMA_2,
            ok.clone()
        )
        .is_ok());
        assert!(MultimodalEmbedderClient::new(
            "http://embedder:8090/v1",
            &EMBEDDINGGEMMA_2,
            ok.clone()
        )
        .is_err());
        assert!(MultimodalEmbedderClient::new(
            "http://u:p@embedder",
            &EMBEDDINGGEMMA_2,
            ok.clone()
        )
        .is_err());
        let zero = MultimodalClientOptions {
            request_timeout: Duration::ZERO,
            ..ok.clone()
        };
        assert!(MultimodalEmbedderClient::new("http://embedder", &EMBEDDINGGEMMA_2, zero).is_err());
        let forever = MultimodalClientOptions {
            request_timeout: Duration::from_secs(3600),
            ..ok
        };
        assert!(
            MultimodalEmbedderClient::new("http://embedder", &EMBEDDINGGEMMA_2, forever).is_err()
        );
    }
}
