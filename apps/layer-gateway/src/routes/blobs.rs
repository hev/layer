use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE, ETAG};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tracing::warn;
use vectorstore_core::capabilities::BlobStorage;

use crate::error::AppError;
use crate::models::BlobPutResponse;
use crate::AppState;

const MAX_BLOB_BYTES: usize = 10 * 1024 * 1024;
const CACHE_CONTROL_IMMUTABLE: &str = "public, max-age=31536000, immutable";

#[derive(Debug, Deserialize, Default)]
pub struct PutBlobQuery {
    #[serde(default)]
    warm: bool,
}

/// PUT /v1/namespaces/{namespace}/blobs
pub async fn put_blob(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
    Query(query): Query<PutBlobQuery>,
    body: Bytes,
) -> Result<Json<BlobPutResponse>, AppError> {
    validate_namespace(&namespace)?;
    if body.is_empty() {
        return Err(AppError::Validation("blob body is empty".to_string()));
    }
    if body.len() > MAX_BLOB_BYTES {
        return Err(AppError::PayloadTooLarge(format!(
            "blob body exceeds {} bytes",
            MAX_BLOB_BYTES
        )));
    }

    let sha256 = sha256_hex(&body);
    let storage = blob_storage(&state, &namespace);
    if let Some(store) = state
        .turbopuffer
        .as_ref()
        .filter(|_| storage.holds(body.len()))
    {
        store
            .put_blob(&namespace, &sha256, &body)
            .await
            .map_err(|e| AppError::from_turbopuffer(e, "persist blob to store"))?;
    } else if storage.native && !state.s3.is_configured() {
        return Err(AppError::BlobExceedsStoreCap(format!(
            "blob is {} bytes; this namespace's store holds blobs up to {} bytes, and \
             larger blobs need an S3-compatible object store (set S3_BUCKET)",
            body.len(),
            storage.max_value_bytes.unwrap_or_default()
        )));
    } else {
        state
            .s3
            .put_if_not_exists(&blob_s3_key(&namespace, &sha256), body.to_vec())
            .await
            .map_err(|e| AppError::from_s3(e, "persist blob to S3"))?;
    }

    if query.warm && state.blob_cache_enabled {
        if let Err(e) = state
            .aerospike
            .put_raw(&blob_cache_set(&namespace), &sha256, &body)
            .await
        {
            warn!(
                namespace = %namespace,
                sha256 = %sha256,
                error = %e,
                "Aerospike blob warm-on-write failed"
            );
        }
    }

    Ok(Json(BlobPutResponse {
        reference: blob_reference(&namespace, &sha256),
        sha256,
        size: body.len() as u64,
    }))
}

/// GET /v1/namespaces/{namespace}/blobs/{sha256}
pub async fn get_blob(
    State(state): State<Arc<AppState>>,
    Path((namespace, sha256)): Path<(String, String)>,
) -> Result<impl IntoResponse, AppError> {
    validate_namespace(&namespace)?;
    validate_sha256(&sha256)?;
    let sha256 = sha256.to_ascii_lowercase();

    let bytes = if state.blob_cache_enabled {
        match state
            .aerospike
            .get_raw(&blob_cache_set(&namespace), &sha256)
            .await
        {
            Ok(Some(bytes)) => bytes,
            Ok(None) => read_blob_through(&state, &namespace, &sha256).await?,
            Err(e) => {
                warn!(
                    namespace = %namespace,
                    sha256 = %sha256,
                    error = %e,
                    "Aerospike blob read failed; falling back to the durable backend"
                );
                read_blob_through(&state, &namespace, &sha256).await?
            }
        }
    } else {
        read_durable_blob(&state, &namespace, &sha256)
            .await?
            .ok_or_else(|| blob_not_found(&sha256))?
    };

    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static(sniff_content_type(&bytes)),
    );
    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_static(CACHE_CONTROL_IMMUTABLE),
    );
    headers.insert(
        ETAG,
        HeaderValue::from_str(&format!("\"{sha256}\""))
            .map_err(|e| AppError::Upstream(format!("invalid blob etag: {e}")))?,
    );

    Ok((StatusCode::OK, headers, bytes))
}

/// Where a namespace's blobs live: its store when the store holds native
/// bytes (up to the store's value cap), S3 otherwise.
fn blob_storage(state: &AppState, namespace: &str) -> BlobStorage {
    state
        .turbopuffer
        .as_ref()
        .map_or(BlobStorage::NONE, |store| store.blob_storage(namespace))
}

/// Read a blob from its durable backend, skipping the cache. A native store
/// is asked first; S3 still answers for blobs over the store's cap and for
/// blobs written before the store held bytes. S3 cannot branch, so a branch
/// reads an S3 blob it inherited under its ancestors' prefixes (RFC 0124).
pub(crate) async fn read_durable_blob(
    state: &AppState,
    namespace: &str,
    sha256: &str,
) -> Result<Option<Vec<u8>>, AppError> {
    if let Some(bytes) = read_own_durable_blob(state, namespace, sha256).await? {
        return Ok(Some(bytes));
    }
    if !state.s3.is_configured() {
        return Ok(None);
    }
    for ancestor in crate::lineage::read_lineage(state, namespace)
        .await
        .ancestors
    {
        if let Some(bytes) = state
            .s3
            .get(&blob_s3_key(&ancestor, sha256))
            .await
            .map_err(|e| AppError::from_s3(e, "read blob from S3"))?
        {
            return Ok(Some(bytes));
        }
    }
    Ok(None)
}

async fn read_own_durable_blob(
    state: &AppState,
    namespace: &str,
    sha256: &str,
) -> Result<Option<Vec<u8>>, AppError> {
    if let Some(store) = state
        .turbopuffer
        .as_ref()
        .filter(|store| store.blob_storage(namespace).native)
    {
        if let Some(bytes) = store
            .get_blob(namespace, sha256)
            .await
            .map_err(|e| AppError::from_turbopuffer(e, "read blob from store"))?
        {
            return Ok(Some(bytes));
        }
    }
    state
        .s3
        .get(&blob_s3_key(namespace, sha256))
        .await
        .map_err(|e| AppError::from_s3(e, "read blob from S3"))
}

/// Cache miss: read the durable backend and backfill Aerospike best-effort.
async fn read_blob_through(
    state: &Arc<AppState>,
    namespace: &str,
    sha256: &str,
) -> Result<Vec<u8>, AppError> {
    let bytes = read_durable_blob(state, namespace, sha256)
        .await?
        .ok_or_else(|| blob_not_found(sha256))?;

    if let Err(e) = state
        .aerospike
        .put_raw(&blob_cache_set(namespace), sha256, &bytes)
        .await
    {
        warn!(
            namespace = %namespace,
            sha256 = %sha256,
            error = %e,
            "Aerospike blob backfill failed"
        );
    }
    Ok(bytes)
}

fn blob_not_found(sha256: &str) -> AppError {
    AppError::NotFound(format!("blob '{sha256}' not found"))
}

fn validate_namespace(namespace: &str) -> Result<(), AppError> {
    if namespace.trim().is_empty() {
        return Err(AppError::Validation("namespace is required".to_string()));
    }
    Ok(())
}

fn validate_sha256(value: &str) -> Result<(), AppError> {
    if is_valid_sha256(value) {
        return Ok(());
    }
    Err(AppError::Validation(
        "sha256 must be a 64-character hex string".to_string(),
    ))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(crate) fn is_valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

pub(crate) fn blob_s3_key(namespace: &str, sha256: &str) -> String {
    format!("blobs/{namespace}/{sha256}")
}

pub(crate) fn blob_cache_set(namespace: &str) -> String {
    format!("blob_{namespace}")
}

fn blob_reference(namespace: &str, sha256: &str) -> String {
    format!("blob://{namespace}/{sha256}")
}

fn sniff_content_type(bytes: &[u8]) -> &'static str {
    if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        "image/jpeg"
    } else if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        "image/png"
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        "image/gif"
    } else if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") {
        "image/webp"
    } else {
        "application/octet-stream"
    }
}
