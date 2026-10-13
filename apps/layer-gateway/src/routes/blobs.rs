use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE, ETAG};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::warn;
use vectorstore_core::capabilities::BlobStorage;

use crate::error::AppError;
use crate::models::BlobPutResponse;
use crate::AppState;

const MAX_BLOB_BYTES: usize = 10 * 1024 * 1024;
const CACHE_CONTROL_IMMUTABLE: &str = "public, max-age=31536000, immutable";
/// turbopuffer's largest `top_k`: one move page is one store query.
const MAX_MOVE_PAGE_SIZE: u32 = 10_000;
/// Without a `page_size`, the first page measures blob size and later pages
/// hold about this many decoded bytes: a page is held in gateway memory.
const MOVE_PAGE_BYTE_BUDGET: u64 = 256 * 1024 * 1024;
const MOVE_FIRST_PAGE_SIZE: u32 = 100;
/// How long a replica trusts a finished move before re-reading its state, so
/// a store write on another replica that reopened it is seen.
const MOVED_TTL: Duration = Duration::from_secs(60);
/// S3 writes in flight while a move copies one page.
const MOVE_PUT_CONCURRENCY: usize = 16;

/// Where a namespace keeps blobs its store could hold itself (LYR-313).
/// Every turbopuffer read of a blob set is a query billed at the whole set's
/// size, so S3 is the default for turbopuffer namespaces.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BlobStoragePreference {
    /// S3 for turbopuffer namespaces when S3 is configured; the store for
    /// other stores with native bytes.
    #[default]
    Auto,
    /// S3 whenever it is configured.
    S3,
    /// The store whenever it holds the blob; S3 only above its cap.
    Store,
}

impl BlobStoragePreference {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "auto" => Some(Self::Auto),
            "s3" => Some(Self::S3),
            "store" => Some(Self::Store),
            _ => None,
        }
    }
}

/// The gateway's blob placement policy: `LAYER_BLOB_STORAGE`, per-namespace
/// `Index.spec.blobs.storage` overrides, and the namespaces whose blob sets
/// finished moving to S3.
#[derive(Debug, Default)]
pub struct BlobPlacement {
    default: BlobStoragePreference,
    overrides: RwLock<HashMap<String, BlobStoragePreference>>,
    moved: RwLock<HashMap<String, Instant>>,
}

impl BlobPlacement {
    pub fn new(default: BlobStoragePreference) -> Self {
        Self {
            default,
            ..Self::default()
        }
    }

    /// Replace the Index overrides, keyed by backend namespace or pattern.
    pub fn replace_overrides(&self, overrides: HashMap<String, BlobStoragePreference>) {
        *self
            .overrides
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = overrides;
    }

    pub fn preference(&self, namespace: &str) -> BlobStoragePreference {
        vectorstore_core::namespace_pattern::resolve(
            &self
                .overrides
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            namespace,
        )
        .copied()
        .unwrap_or(self.default)
    }

    fn known_moved(&self, namespace: &str) -> bool {
        self.moved
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(namespace)
            .is_some_and(|at| at.elapsed() < MOVED_TTL)
    }

    fn mark_moved(&self, namespace: &str) {
        self.moved
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(namespace.to_string(), Instant::now());
    }

    /// The namespace's store may hold blobs S3 lacks again.
    pub fn forget_moved(&self, namespace: &str) {
        self.moved
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(namespace);
    }
}

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
        .filter(|_| !s3_first(&state, &namespace) && storage.holds(body.len()))
    {
        store
            .put_blob(&namespace, &sha256, &body)
            .await
            .map_err(|e| AppError::from_turbopuffer(e, "persist blob to store"))?;
        reopen_blob_move(&state, &namespace).await?;
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

/// What the namespace's store can hold natively: up to the store's value
/// cap when it has native bytes, nothing otherwise.
fn blob_storage(state: &AppState, namespace: &str) -> BlobStorage {
    state
        .turbopuffer
        .as_ref()
        .map_or(BlobStorage::NONE, |store| store.blob_storage(namespace))
}

/// Whether a namespace whose store could hold its blobs keeps them in S3
/// instead. Writes then go to S3 and reads ask S3 before the store.
fn s3_first(state: &AppState, namespace: &str) -> bool {
    let Some(store) = state.turbopuffer.as_ref() else {
        return false;
    };
    if !store.blob_storage(namespace).native || !state.s3.is_configured() {
        return false;
    }
    match state.blob_placement.preference(namespace) {
        BlobStoragePreference::S3 => true,
        BlobStoragePreference::Store => false,
        BlobStoragePreference::Auto => {
            store.capabilities_for_namespace(namespace).kind
                == vectorstore_core::turbopuffer::TURBOPUFFER_CAPABILITIES.kind
        }
    }
}

/// Read a blob from its durable backend, skipping the cache. A namespace
/// that keeps blobs in S3 asks S3 first and its store only for blobs not yet
/// moved; otherwise the native store is asked first and S3 still answers for
/// blobs over the store's cap and blobs written before the store held bytes.
/// S3 cannot branch, so a branch
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
    if s3_first(state, namespace) {
        return read_s3_first_blob(state, namespace, sha256).await;
    }
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

/// S3, then the store's blob set for blobs not yet moved. Once the move
/// has finished, an S3 miss is final: no store query.
async fn read_s3_first_blob(
    state: &AppState,
    namespace: &str,
    sha256: &str,
) -> Result<Option<Vec<u8>>, AppError> {
    if let Some(bytes) = state
        .s3
        .get(&blob_s3_key(namespace, sha256))
        .await
        .map_err(|e| AppError::from_s3(e, "read blob from S3"))?
    {
        return Ok(Some(bytes));
    }
    if blob_set_moved(state, namespace).await? {
        return Ok(None);
    }
    let Some(store) = state.turbopuffer.as_ref() else {
        return Ok(None);
    };
    store
        .get_blob(namespace, sha256)
        .await
        .map_err(|e| AppError::from_turbopuffer(e, "read blob from store"))
}

/// A blob written to the store after its namespace finished moving to S3
/// (the namespace switched back to `store`, or a replica had not seen the
/// switch yet) must stay readable: drop the finished state so S3 misses ask
/// the store again, and a rerun of the move copies it.
async fn reopen_blob_move(state: &AppState, namespace: &str) -> Result<(), AppError> {
    state.blob_placement.forget_moved(namespace);
    if !state.s3.is_configured() {
        return Ok(());
    }
    state
        .s3
        .delete_key(&blob_move_state_key(namespace))
        .await
        .map_err(|e| AppError::from_s3(e, "reopen blob move state"))
}

/// Whether the namespace's blob set has finished moving to S3. A replica
/// trusts a finished move for [`MOVED_TTL`] before reading it again.
async fn blob_set_moved(state: &AppState, namespace: &str) -> Result<bool, AppError> {
    if state.blob_placement.known_moved(namespace) {
        return Ok(true);
    }
    let done = read_move_state(state, namespace)
        .await?
        .is_some_and(|move_state| move_state.done);
    if done {
        state.blob_placement.mark_moved(namespace);
    }
    Ok(done)
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct MoveBlobsRequest {
    /// Blobs per store query, at most 10,000. Unset sizes pages to about
    /// [`MOVE_PAGE_BYTE_BUDGET`] from the blobs moved so far.
    page_size: Option<u32>,
    /// Pages this call moves before returning; rerun to continue.
    max_pages: u32,
    /// Start over from the first blob, even after a finished move.
    restart: bool,
}

impl Default for MoveBlobsRequest {
    fn default() -> Self {
        Self {
            page_size: None,
            max_pages: 1,
            restart: false,
        }
    }
}

/// A page size that keeps a page near [`MOVE_PAGE_BYTE_BUDGET`], from the
/// mean size of the blobs scanned so far.
fn budgeted_page_size(totals: &BlobMoveCounts) -> u32 {
    let scanned = totals.moved + totals.already_in_s3 + totals.mismatched;
    if scanned == 0 {
        return MOVE_FIRST_PAGE_SIZE;
    }
    let mean = (totals.scanned_bytes / scanned).max(1);
    (MOVE_PAGE_BYTE_BUDGET / mean).clamp(1, u64::from(MAX_MOVE_PAGE_SIZE)) as u32
}

/// Counters for a move, cumulative in [`BlobMoveState`] and per call in
/// [`MoveBlobsResponse::run`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobMoveCounts {
    /// Store queries: one per page.
    pub pages: u64,
    /// Blobs written to S3.
    pub moved: u64,
    /// Blobs S3 already held, so not rewritten.
    pub already_in_s3: u64,
    /// Rows whose bytes do not hash to their id; not copied.
    pub mismatched: u64,
    pub moved_bytes: u64,
    /// Bytes of every blob scanned, moved or not.
    pub scanned_bytes: u64,
    pub billable_bytes_queried: u64,
    pub billable_bytes_returned: u64,
}

impl BlobMoveCounts {
    fn add(&mut self, other: &Self) {
        self.pages += other.pages;
        self.moved += other.moved;
        self.already_in_s3 += other.already_in_s3;
        self.mismatched += other.mismatched;
        self.moved_bytes += other.moved_bytes;
        self.scanned_bytes += other.scanned_bytes;
        self.billable_bytes_queried += other.billable_bytes_queried;
        self.billable_bytes_returned += other.billable_bytes_returned;
    }
}

/// A move's progress, persisted in S3 after every page so a rerun resumes
/// where the last one stopped.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobMoveState {
    pub namespace: String,
    pub done: bool,
    /// The last sha256 moved; the next page starts after it.
    pub cursor: Option<String>,
    /// The page size of the latest page.
    pub page_size: Option<u32>,
    #[serde(flatten)]
    pub totals: BlobMoveCounts,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Serialize)]
pub struct MoveBlobsResponse {
    #[serde(flatten)]
    pub state: BlobMoveState,
    /// What this call did.
    pub run: BlobMoveCounts,
}

/// POST /v1/namespaces/{namespace}/blobs/move_to_s3
///
/// Copy a namespace's store-held blob set to S3 in paged scans, one store
/// query per page and never one per blob. Idempotent and resumable: S3
/// writes are conditional, and the cursor persists after every page.
pub async fn move_blobs_to_s3(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
    request: Option<Json<MoveBlobsRequest>>,
) -> Result<Json<MoveBlobsResponse>, AppError> {
    validate_namespace(&namespace)?;
    let request = request.map(|Json(request)| request).unwrap_or_default();
    if request
        .page_size
        .is_some_and(|size| !(1..=MAX_MOVE_PAGE_SIZE).contains(&size))
    {
        return Err(AppError::Validation(format!(
            "page_size must be between 1 and {MAX_MOVE_PAGE_SIZE}"
        )));
    }
    if request.max_pages == 0 {
        return Err(AppError::Validation("max_pages must be > 0".to_string()));
    }
    if !state.s3.is_configured() {
        return Err(AppError::object_store_not_configured("moving blobs to S3"));
    }
    let Some(store) = state
        .turbopuffer
        .as_ref()
        .filter(|store| store.blob_storage(&namespace).native)
    else {
        return Err(AppError::Validation(format!(
            "namespace '{namespace}' keeps no blobs in its store; there is nothing to move"
        )));
    };
    if !s3_first(&state, &namespace) {
        return Err(AppError::Conflict(format!(
            "namespace '{namespace}' keeps blobs in its store; set LAYER_BLOB_STORAGE or \
             the Index's spec.blobs.storage to s3 before moving them"
        )));
    }

    let fresh = || BlobMoveState {
        namespace: namespace.clone(),
        ..BlobMoveState::default()
    };
    let mut move_state = if request.restart {
        state.blob_placement.forget_moved(&namespace);
        fresh()
    } else {
        read_move_state(&state, &namespace)
            .await?
            .unwrap_or_else(fresh)
    };
    let mut run = BlobMoveCounts::default();
    while !move_state.done && run.pages < u64::from(request.max_pages) {
        let page_size = request
            .page_size
            .unwrap_or_else(|| budgeted_page_size(&move_state.totals));
        let page = store
            .scan_blobs(&namespace, move_state.cursor.as_deref(), page_size)
            .await
            .map_err(|e| AppError::from_turbopuffer(e, "scan blob set"))?;
        let mut counts = BlobMoveCounts {
            pages: 1,
            billable_bytes_queried: page.billable_bytes_queried,
            billable_bytes_returned: page.billable_bytes_returned,
            ..BlobMoveCounts::default()
        };
        let last = page.blobs.last().map(|(sha256, _)| sha256.clone());
        // Only an empty page ends the move: a short page is not proof the
        // store had nothing more after it.
        let exhausted = page.blobs.is_empty();
        counts.scanned_bytes = page.blobs.iter().map(|(_, b)| b.len() as u64).sum();
        let outcomes: Vec<BlobCopy> = futures::stream::iter(page.blobs)
            .map(|(sha256, bytes)| copy_blob_to_s3(&state, &namespace, sha256, bytes))
            .buffer_unordered(MOVE_PUT_CONCURRENCY)
            .try_collect()
            .await?;
        for outcome in outcomes {
            match outcome {
                BlobCopy::Written(len) => {
                    counts.moved += 1;
                    counts.moved_bytes += len;
                }
                BlobCopy::AlreadyInS3 => counts.already_in_s3 += 1,
                BlobCopy::Mismatched => counts.mismatched += 1,
            }
        }
        move_state.cursor = last.or(move_state.cursor);
        move_state.done = exhausted;
        move_state.page_size = Some(page_size);
        move_state.totals.add(&counts);
        move_state.updated_at = Some(chrono::Utc::now());
        run.add(&counts);
        write_move_state(&state, &move_state).await?;
    }
    if move_state.done {
        state.blob_placement.mark_moved(&namespace);
    }
    Ok(Json(MoveBlobsResponse {
        state: move_state,
        run,
    }))
}

enum BlobCopy {
    Written(u64),
    AlreadyInS3,
    Mismatched,
}

async fn copy_blob_to_s3(
    state: &AppState,
    namespace: &str,
    sha256: String,
    bytes: Vec<u8>,
) -> Result<BlobCopy, AppError> {
    if !sha256.eq_ignore_ascii_case(&sha256_hex(&bytes)) {
        warn!(
            namespace = %namespace,
            sha256 = %sha256,
            "blob set row does not hash to its id; not moving it"
        );
        return Ok(BlobCopy::Mismatched);
    }
    let len = bytes.len() as u64;
    let written = state
        .s3
        .put_if_not_exists(&blob_s3_key(namespace, &sha256), bytes)
        .await
        .map_err(|e| AppError::from_s3(e, "move blob to S3"))?;
    Ok(if written {
        BlobCopy::Written(len)
    } else {
        BlobCopy::AlreadyInS3
    })
}

/// Kept outside `blobs/{namespace}/`, whose keys are all sha256s.
pub(crate) fn blob_move_state_key(namespace: &str) -> String {
    format!("blob-moves/{namespace}.json")
}

async fn read_move_state(
    state: &AppState,
    namespace: &str,
) -> Result<Option<BlobMoveState>, AppError> {
    let Some(raw) = state
        .s3
        .get(&blob_move_state_key(namespace))
        .await
        .map_err(|e| AppError::from_s3(e, "read blob move state"))?
    else {
        return Ok(None);
    };
    serde_json::from_slice(&raw)
        .map(Some)
        .map_err(|e| AppError::Upstream(format!("blob move state is not valid JSON: {e}")))
}

async fn write_move_state(state: &AppState, move_state: &BlobMoveState) -> Result<(), AppError> {
    let body = serde_json::to_vec(move_state)
        .map_err(|e| AppError::Upstream(format!("serialize blob move state: {e}")))?;
    state
        .s3
        .put(&blob_move_state_key(&move_state.namespace), body)
        .await
        .map_err(|e| AppError::from_s3(e, "persist blob move state"))
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
    layer_original::reference(namespace, sha256)
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
