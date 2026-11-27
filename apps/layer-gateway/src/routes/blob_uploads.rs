use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::extract::{OriginalUri, Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::Json;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::{Semaphore, SemaphorePermit};
use uuid::Uuid;
use vectorstore_core::s3::{S3Error, STAGED_UPLOAD_NOT_CONFIGURED};

use crate::auth::CallerGrant;
use crate::error::AppError;
use crate::models::BlobPutResponse;
use crate::AppState;

use super::blobs::{blob_reference, blob_s3_key, sha256_hex, validate_sha256, MAX_BLOB_BYTES};

const MAX_STAGED_BYTES: usize = 100 * 1024 * 1024;
const GRANT_TTL: Duration = Duration::from_secs(900);
static BUFFERS: Semaphore = Semaphore::const_new(2);

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadSpec {
    size: usize,
    sha256: String,
    content_type: VideoType,
}

#[derive(Serialize, Deserialize, PartialEq)]
pub(super) enum VideoType {
    #[serde(rename = "video/mp4")]
    Mp4,
    #[serde(rename = "video/quicktime")]
    Quicktime,
    #[serde(rename = "video/webm")]
    Webm,
}

#[derive(Serialize, Deserialize)]
struct Grant {
    spec: UploadSpec,
    expires_at: i64,
}

#[derive(Serialize)]
pub struct UploadCreated {
    upload_id: String,
    upload_url: String,
    method: &'static str,
    headers: BTreeMap<String, String>,
    expires_at: i64,
}

pub async fn create(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
    OriginalUri(uri): OriginalUri,
    caller: Option<axum::Extension<CallerGrant>>,
    Json(mut spec): Json<UploadSpec>,
) -> Result<(StatusCode, HeaderMap, Json<UploadCreated>), AppError> {
    authorize(caller, &namespace, uri.path())?;
    validate_sha256(&spec.sha256)?;
    spec.sha256.make_ascii_lowercase();
    if spec.size <= MAX_BLOB_BYTES {
        return Err(AppError::Validation(
            "staged video must exceed 10 MiB".into(),
        ));
    }
    if spec.size > MAX_STAGED_BYTES {
        return Err(AppError::PayloadTooLarge(
            "staged video exceeds 100 MiB".into(),
        ));
    }
    let upload_id = Uuid::new_v4().simple().to_string();
    let now = SystemTime::now();
    let expires_at = chrono::DateTime::<Utc>::from(now + GRANT_TTL).timestamp();
    let prefix = upload_prefix(&namespace, &upload_id)?;
    let mut digest = [0; 32];
    for (index, octet) in digest.iter_mut().enumerate() {
        *octet = u8::from_str_radix(&spec.sha256[index * 2..index * 2 + 2], 16)
            .map_err(|_| AppError::Validation("invalid SHA-256".into()))?;
    }
    let put = state
        .s3
        .presign_put(&staged_key(&upload_id), spec.size, digest, now, GRANT_TTL)
        .await
        .map_err(storage_error)?;
    let grant = Grant { spec, expires_at };
    let encoded = serde_json::to_vec(&grant).map_err(invalid_manifest)?;
    if !state
        .s3
        .put_if_not_exists(&format!("{prefix}/grant"), encoded)
        .await
        .map_err(storage_error)?
    {
        return Err(AppError::Conflict("upload ID already exists".into()));
    }
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    Ok((
        StatusCode::CREATED,
        headers,
        Json(UploadCreated {
            upload_url: put.url,
            upload_id,
            method: "PUT",
            headers: put.headers,
            expires_at,
        }),
    ))
}

pub async fn complete(
    State(state): State<Arc<AppState>>,
    Path((namespace, upload_id)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
    caller: Option<axum::Extension<CallerGrant>>,
) -> Result<Json<BlobPutResponse>, AppError> {
    authorize(caller, &namespace, uri.path())?;
    let prefix = upload_prefix(&namespace, &upload_id)?;
    let grant = load_grant(&state, &prefix)
        .await?
        .ok_or_else(|| AppError::NotFound("upload grant not found".into()))?;
    grant.check_expiry()?;
    let _permit = buffer_slot()?;
    let bytes = state
        .s3
        .get_limited(&staged_key(&upload_id), grant.spec.size as u64)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| AppError::Conflict("upload has no staged bytes".into()))?;
    grant.spec.validate_bytes(&bytes)?;
    grant.check_expiry()?;
    state
        .s3
        .put_if_not_exists(&blob_s3_key(&namespace, &grant.spec.sha256), bytes)
        .await
        .map_err(storage_error)?;
    // Preserve the create-only slot until expiry. Deleting it here would permit
    // the same S3 URL to create the object again before the signature expires.
    Ok(Json(BlobPutResponse {
        reference: blob_reference(&namespace, &grant.spec.sha256),
        sha256: grant.spec.sha256,
        size: grant.spec.size as u64,
    }))
}

impl UploadSpec {
    fn validate_bytes(&self, bytes: &[u8]) -> Result<(), AppError> {
        if bytes.len() != self.size || sha256_hex(bytes) != self.sha256 {
            return Err(AppError::Validation(
                "upload size or SHA-256 does not match the grant".into(),
            ));
        }
        if !super::blob_video::valid_video(bytes, &self.content_type) {
            return Err(AppError::Validation(
                "invalid video container, type or duration".into(),
            ));
        }
        Ok(())
    }
}

impl Grant {
    fn check_expiry(&self) -> Result<(), AppError> {
        if Utc::now().timestamp() >= self.expires_at {
            return Err(AppError::Forbidden("upload grant expired".into()));
        }
        Ok(())
    }
}

fn authorize(
    caller: Option<axum::Extension<CallerGrant>>,
    namespace: &str,
    path: &str,
) -> Result<(), AppError> {
    if caller.is_none() {
        return Err(AppError::Forbidden(
            "staged uploads require a verified Layer write key".into(),
        ));
    }
    // RBAC checks the URI segment before Axum decodes it. Use the same spelling
    // for authorization and storage, including when a key's grants have globs.
    if path.split('/').nth(3) != Some(namespace) {
        return Err(AppError::Validation(
            "encode the namespace using literal URL-safe characters".into(),
        ));
    }
    Ok(())
}

fn valid_id(id: &str) -> bool {
    id.len() == 32 && id.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn upload_prefix(namespace: &str, id: &str) -> Result<String, AppError> {
    if namespace.is_empty()
        || namespace.len() > 255
        || matches!(namespace, "." | "..")
        || !namespace
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
        || !valid_id(id)
    {
        return Err(AppError::Validation(
            "invalid upload namespace or ID".into(),
        ));
    }
    Ok(format!("blob-uploads/grants/{namespace}/{id}"))
}

fn staged_key(id: &str) -> String {
    format!("blob-uploads/objects/{id}")
}

async fn load_grant(state: &AppState, prefix: &str) -> Result<Option<Grant>, AppError> {
    state
        .s3
        .get_limited(&format!("{prefix}/grant"), 1024)
        .await
        .map_err(storage_error)?
        .map(|bytes| serde_json::from_slice(&bytes).map_err(invalid_manifest))
        .transpose()
}

fn buffer_slot() -> Result<SemaphorePermit<'static>, AppError> {
    BUFFERS
        .try_acquire()
        .map_err(|_| AppError::ServiceUnavailable("staged upload capacity reached; retry".into()))
}

fn invalid_manifest(_: serde_json::Error) -> AppError {
    AppError::Upstream("invalid staged blob manifest".into())
}

fn storage_error(error: S3Error) -> AppError {
    if error.0 == STAGED_UPLOAD_NOT_CONFIGURED {
        return AppError::ServiceUnavailable(error.0);
    }
    if error.0 == "object exceeds byte limit" {
        return AppError::PayloadTooLarge("staged object exceeds the declared size".into());
    }
    AppError::from_s3(error, "access staged blob")
}
