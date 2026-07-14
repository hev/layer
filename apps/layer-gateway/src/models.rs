use std::collections::HashMap;

#[cfg(feature = "pro")]
pub use layer_transform::models::{
    UdfErrorKind, UdfRetrySpec, UdfScheduleSpec, UdfSpec, UdfTrigger, UdfWorkerSpec,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
pub use vectorstore_core::models::{
    DocumentPage, DocumentResponse, FieldValueResult, IncludeAttributes, QueryResult,
};

#[cfg(not(feature = "pro"))]
fn default_udf_triggers() -> Vec<UdfTrigger> {
    vec![UdfTrigger::Discovery]
}

#[cfg(not(feature = "pro"))]
fn default_udf_batch_size_spec() -> u32 {
    32
}

#[cfg(not(feature = "pro"))]
fn default_udf_timeout_seconds() -> u64 {
    30
}

#[cfg(not(feature = "pro"))]
fn default_udf_discovery_interval_seconds() -> u64 {
    60
}

#[cfg(not(feature = "pro"))]
fn default_udf_lease_seconds_spec() -> i64 {
    120
}

#[cfg(not(feature = "pro"))]
fn default_udf_max_in_flight_batches() -> u32 {
    8
}

#[cfg(not(feature = "pro"))]
fn default_udf_max_concurrent_scans() -> u32 {
    4
}

#[cfg(not(feature = "pro"))]
fn default_udf_retry_max_attempts() -> u32 {
    8
}

#[cfg(not(feature = "pro"))]
fn default_udf_initial_backoff_seconds() -> u64 {
    5
}

#[cfg(not(feature = "pro"))]
fn default_udf_max_backoff_seconds() -> u64 {
    300
}

#[cfg(not(feature = "pro"))]
fn default_udf_version() -> String {
    "v1".to_string()
}

#[cfg(not(feature = "pro"))]
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UdfTrigger {
    Discovery,
    Write,
}

#[cfg(not(feature = "pro"))]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UdfWorkerSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(default = "default_udf_batch_size_spec")]
    pub batch_size: u32,
    #[serde(default = "default_udf_timeout_seconds")]
    pub timeout_seconds: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pod_spec: Option<Value>,
}

#[cfg(not(feature = "pro"))]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UdfScheduleSpec {
    #[serde(default = "default_udf_discovery_interval_seconds")]
    pub discovery_interval_seconds: u64,
    #[serde(default = "default_udf_lease_seconds_spec")]
    pub lease_seconds: i64,
    #[serde(default = "default_udf_max_in_flight_batches")]
    pub max_in_flight_batches: u32,
    #[serde(default = "default_udf_max_concurrent_scans")]
    pub max_concurrent_scans: u32,
}

#[cfg(not(feature = "pro"))]
impl Default for UdfScheduleSpec {
    fn default() -> Self {
        Self {
            discovery_interval_seconds: default_udf_discovery_interval_seconds(),
            lease_seconds: default_udf_lease_seconds_spec(),
            max_in_flight_batches: default_udf_max_in_flight_batches(),
            max_concurrent_scans: default_udf_max_concurrent_scans(),
        }
    }
}

#[cfg(not(feature = "pro"))]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UdfRetrySpec {
    #[serde(default = "default_udf_retry_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_udf_initial_backoff_seconds")]
    pub initial_backoff_seconds: u64,
    #[serde(default = "default_udf_max_backoff_seconds")]
    pub max_backoff_seconds: u64,
}

#[cfg(not(feature = "pro"))]
impl Default for UdfRetrySpec {
    fn default() -> Self {
        Self {
            max_attempts: default_udf_retry_max_attempts(),
            initial_backoff_seconds: default_udf_initial_backoff_seconds(),
            max_backoff_seconds: default_udf_max_backoff_seconds(),
        }
    }
}

#[cfg(not(feature = "pro"))]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UdfSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_selector: Option<Value>,
    #[serde(default)]
    pub target_namespaces: Vec<String>,
    #[serde(default)]
    pub inputs: Vec<String>,
    #[serde(default = "default_udf_version")]
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Value>,
    pub worker: UdfWorkerSpec,
    #[serde(default)]
    pub schedule: UdfScheduleSpec,
    #[serde(default)]
    pub retry: UdfRetrySpec,
    #[serde(default = "default_udf_triggers")]
    pub triggers: Vec<UdfTrigger>,
    #[serde(default)]
    pub invalidates: Vec<String>,
}

#[cfg(not(feature = "pro"))]
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UdfErrorKind {
    Transient,
    Permanent,
}

// --- Request models ---

#[derive(Debug, Clone, Deserialize)]
pub struct QueryRequest {
    /// Raw query vector. Mutually exclusive with `nearest_to_id` — the
    /// query route validates exactly one of the two is supplied.
    #[serde(default)]
    pub vector: Option<Vec<f64>>,
    /// Document ids whose stored vectors are averaged into the centroid
    /// query vector. The gateway resolves each via Aerospike, falling back
    /// to Turbopuffer on miss. Mutually exclusive with `vector`; must be
    /// non-empty when supplied.
    #[serde(default)]
    pub nearest_to_id: Option<Vec<String>>,
    #[serde(default = "default_top_k")]
    pub top_k: u32,
    #[serde(default)]
    pub filters: Option<Value>,
    /// Epoch-ms upper bound for a temporal read. Mutually exclusive with
    /// `between`; implemented as `_hevlayer_upserted_at <= as_of`.
    #[serde(default)]
    pub as_of: Option<u64>,
    /// Epoch-ms open/closed temporal window `(lo, hi]`. Mutually exclusive
    /// with `as_of`; implemented as
    /// `lo < _hevlayer_upserted_at <= hi`.
    #[serde(default)]
    pub between: Option<[u64; 2]>,
    #[serde(default)]
    pub include_attributes: Option<IncludeAttributes>,
    /// Opaque pagination cursor from a prior response's `next_cursor`.
    /// Resumes the same ranked query after the cursor's last result.
    #[serde(default)]
    pub cursor: Option<String>,
    /// Portable rank expression tuple. Native upstream bodies without
    /// `top_k` still pass through; this model is for Layer-shaped ranked
    /// queries that need cache/history/consistency behavior.
    #[serde(default)]
    pub rank_by: Option<Value>,
}

/// Opaque cursor for paginating ranked (vector ANN) queries. Encoded as
/// base64(JSON) on the wire so clients treat it as a token; the gateway
/// decodes it into a score-band filter `($dist > dist OR ($dist == dist AND
/// id > id))` for the next page.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueryCursor {
    pub dist: f64,
    pub id: String,
}

impl QueryCursor {
    pub fn encode(&self) -> String {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
        use base64::Engine;
        let bytes = serde_json::to_vec(self).expect("QueryCursor is JSON-encodable");
        B64.encode(bytes)
    }

    pub fn decode(s: &str) -> Result<Self, String> {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
        use base64::Engine;
        let bytes = B64
            .decode(s)
            .map_err(|e| format!("invalid cursor: base64 decode: {e}"))?;
        let cursor: QueryCursor = serde_json::from_slice(&bytes)
            .map_err(|e| format!("invalid cursor: json decode: {e}"))?;
        if !cursor.dist.is_finite() {
            return Err("invalid cursor: dist must be a finite number".to_string());
        }
        Ok(cursor)
    }
}

fn default_top_k() -> u32 {
    10
}

#[derive(Debug, Deserialize)]
pub struct FetchManyRequest {
    pub ids: Vec<String>,
    #[serde(default)]
    pub include_attributes: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateCheckpointRequest {
    pub label: String,
}

// --- Response models ---

#[derive(Debug, Serialize)]
pub struct StatusResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_affected: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_upserted: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_patched: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_deleted: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub billing: Option<Value>,
}

impl Default for StatusResponse {
    fn default() -> Self {
        Self {
            status: "OK".to_string(),
            message: None,
            rows_affected: None,
            rows_upserted: None,
            rows_patched: None,
            rows_deleted: None,
            billing: None,
        }
    }
}

impl StatusResponse {
    pub fn write_result(rows_upserted: u64, rows_patched: u64, rows_deleted: u64) -> Self {
        let rows_affected = rows_upserted + rows_patched + rows_deleted;
        Self {
            status: "OK".to_string(),
            message: Some("write accepted".to_string()),
            rows_affected: Some(rows_affected),
            rows_upserted: (rows_upserted > 0).then_some(rows_upserted),
            rows_patched: (rows_patched > 0).then_some(rows_patched),
            rows_deleted: (rows_deleted > 0).then_some(rows_deleted),
            billing: Some(serde_json::json!({
                "billable_logical_bytes_written": 0
            })),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobPutResponse {
    #[serde(rename = "ref")]
    pub reference: String,
    pub sha256: String,
    pub size: u64,
}

#[derive(Debug, Serialize)]
pub struct FetchManyResponse {
    pub documents: Vec<DocumentResponse>,
    pub missing: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub results: Vec<QueryResult>,
    /// Epoch-ms upper bound used in the query's `_hevlayer_upserted_at` filter.
    /// `None` before the consistency watcher has observed a clean
    /// `unindexed_bytes == 0` snapshot for this namespace.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stable_as_of: Option<u64>,
    /// Opaque cursor to fetch the next page. Present when the response
    /// returned a full `top_k` page — i.e. there *might* be more results.
    /// Absent on the last page. Clients should treat the value as opaque.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub aerospike: AerospikeHealthResponse,
    pub cache_state: Vec<NamespaceCacheStateResponse>,
}

#[derive(Debug, Serialize)]
pub struct AerospikeHealthResponse {
    pub connected: bool,
    pub generation: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct NamespaceCacheStateResponse {
    pub namespace: String,
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warmed_through: Option<u64>,
    pub warm_inflight: bool,
}

// --- Namespace list (GET /v2/namespaces) ---

#[derive(Debug, Clone, Serialize)]
pub struct NamespaceList {
    pub namespaces: Vec<NamespaceListEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NamespaceListEntry {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stable_as_of_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_stable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_summary: Option<NamespaceSchemaSummary>,
    /// Upstream Turbopuffer `index` object, passed through verbatim:
    /// `{ status, unindexed_bytes? }`. `unindexed_bytes` is present only
    /// while `status == "updating"`. Omitted when the upstream metadata
    /// carried no `index` field (cold namespace or metadata error). This is
    /// the raw per-fetch signal used by list rows for `is_stable` and
    /// `stable_as_of_ms`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<Value>,
    pub cache_state: NamespaceCacheState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_write_ms: Option<u64>,
    pub shadow: bool,
    pub labels: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NamespaceSchemaSummary {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_dim: Option<u64>,
    pub fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NamespaceCacheState {
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warmed_through_ms: Option<u64>,
    pub warm_inflight: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Checkpoint {
    pub namespace: String,
    pub label: String,
    pub watermark_ms: u64,
    pub sha: String,
    pub row_count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct CheckpointList {
    pub checkpoints: Vec<Checkpoint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

// --- Ranked scan selectors ---

/// Full-text (BM25) scan selector. Counts rows whose `field` matches the BM25
/// `query`. Exact; always origin scatter/gather. Was the FTS arm of the
/// removed `/result-count` endpoint.
#[derive(Debug, Clone, Deserialize)]
pub struct FtsScan {
    /// BM25-indexed field to rank against.
    pub field: String,
    /// User query string passed to BM25.
    pub query: String,
}

/// Hybrid text scan selector. Counts the union of the same BM25 and fuzzy
/// token legs used by the `HybridText` query route.
#[derive(Debug, Clone, Deserialize)]
pub struct HybridTextScan {
    /// Full-text field to rank and fuzzy-match against.
    pub field: String,
    /// User input passed through the HybridText tokenizer policy.
    pub query: String,
    /// Optional fuzziness policy: "auto", 0, 1, or 2.
    #[serde(default)]
    pub fuzziness: Option<Value>,
}

/// Radius (ANN) scan selector. Counts rows within `radius` of `vector` — a
/// distance-ball scan evaluated by approximate nearest-neighbour search.
/// **Approximate**; always origin scatter/gather. Was the vector arm of the
/// removed `/result-count` endpoint; `max_distance` is renamed `radius` to
/// name the ball it defines.
#[derive(Debug, Clone, Deserialize)]
pub struct AnnScan {
    /// Query vector. Distance metric is whatever the namespace was created
    /// with (cosine_distance by default).
    pub vector: Vec<f64>,
    /// Vector field name; defaults to `"vector"`.
    #[serde(default)]
    pub field: Option<String>,
    /// Required, finite upper bound on distance — the radius of the ball the
    /// scan counts. Without it every row is in the ball and the count is
    /// meaningless. The gateway applies the bound to returned `$dist` rows;
    /// it is not pushed upstream as a filter.
    pub radius: f64,
}

fn default_count_timeout_seconds() -> u32 {
    30
}

// --- Snapshot / warm / scan job models ---

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorKind {
    FieldValues,
    FullDocument,
    Ids,
    /// `mode: values` scan — distinct values of one attribute field with
    /// per-value counts. Same evaluators as `FieldValues` (the snapshot-job
    /// kind), but surfaced as a scan job and never persisted as a snapshot.
    Values,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotSource {
    #[default]
    Auto,
    Stored,
    Cache,
    Origin,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ScanSource {
    #[default]
    Auto,
    Cache,
    Origin,
    /// Values scans only: serve from the latest snapshot's pre-aggregated
    /// facet histogram. Rejected for ID scans at create time.
    Snapshot,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ScanCountSource {
    #[default]
    Auto,
    Snapshot,
    Cache,
    Origin,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ScanMode {
    #[default]
    Ids,
    Count,
    Values,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ScanCountServedBy {
    Snapshot,
    Cache,
    Origin,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorSource {
    #[default]
    Auto,
    Cache,
    Origin,
    /// Serve a `field_values` scan from the latest S3 snapshot's pre-aggregated
    /// histogram. Requires the field to be in the namespace's facet config; the
    /// snapshot writer maintains the histograms (see `snapshots.rs`).
    Snapshot,
}

#[derive(Debug, Deserialize)]
pub struct CreateExecutorRequest {
    pub kind: ExecutorKind,
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub source: Option<ExecutorSource>,
    #[serde(default)]
    pub filters: Option<Value>,
    #[serde(default)]
    pub page_size: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct CreateSnapshotRequest {
    pub field: String,
    #[serde(default)]
    pub source: Option<SnapshotSource>,
    #[serde(default)]
    pub filters: Option<Value>,
    #[serde(default)]
    pub page_size: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct CreateScanRequest {
    #[serde(default)]
    pub source: Option<ScanCountSource>,
    #[serde(default)]
    pub filters: Option<Value>,
    /// Epoch-ms upper bound for a temporal scan. Mutually exclusive with
    /// `between`; implemented as `_hevlayer_upserted_at <= as_of`.
    #[serde(default)]
    pub as_of: Option<u64>,
    /// Epoch-ms open/closed temporal window `(lo, hi]`. Mutually exclusive
    /// with `as_of`; implemented as
    /// `lo < _hevlayer_upserted_at <= hi`.
    #[serde(default)]
    pub between: Option<[u64; 2]>,
    /// Full-text selector. Mutually exclusive with other ranked selectors. When present the
    /// scan runs origin scatter/gather and `filters`, if any, is ANDed on as
    /// an extra constraint.
    #[serde(default)]
    pub fts: Option<FtsScan>,
    /// HybridText selector. Mutually exclusive with other ranked selectors.
    #[serde(default)]
    pub hybrid_text: Option<HybridTextScan>,
    /// Radius (ANN) selector. Mutually exclusive with other ranked selectors.
    #[serde(default)]
    pub ann: Option<AnnScan>,
    #[serde(default)]
    pub mode: ScanMode,
    /// Values mode only: the attribute field to enumerate. Required for
    /// `mode: values`, rejected on other modes.
    #[serde(default)]
    pub field: Option<String>,
    /// Ranked scans only. `false` (default) = one scatter/gather, saturated
    /// shards bound the result; `true` = recurse saturated BM25 shards until
    /// every page is short or the deadline elapses. ANN radius scans do not
    /// push `$dist` filters upstream, so saturated all-in-radius pages remain
    /// bounded.
    #[serde(default)]
    pub exhaustive: bool,
    /// Max concurrent upstream requests used by origin scatter/gather. Values
    /// <= 0 are rejected by the route; over-large values are clamped to the
    /// server cap and active shard count.
    #[serde(default)]
    pub threads: Option<i32>,
    #[serde(default)]
    pub page_size: Option<u32>,
    #[serde(default = "default_count_timeout_seconds")]
    pub timeout_seconds: u32,
}

#[derive(Debug, Deserialize)]
pub struct WarmJobQuery {
    #[serde(default)]
    pub page_size: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct WarmCacheOptions {
    #[serde(default = "default_true")]
    pub turbopuffer: bool,
    #[serde(default = "default_true")]
    pub documents: bool,
    #[serde(default = "default_true")]
    pub snapshots: bool,
    #[serde(default)]
    pub blobs: bool,
    #[serde(default)]
    pub blob_budget_bytes: Option<u64>,
    #[serde(default = "default_warm_page_size")]
    pub page_size: u32,
}

fn default_true() -> bool {
    true
}

fn default_warm_page_size() -> u32 {
    1000
}

#[derive(Debug, Clone, Serialize)]
pub struct ExecutorJob {
    pub id: String,
    pub namespace: String,
    pub kind: ExecutorKind,
    /// The source the caller asked for (`auto`, `cache`, or `origin`).
    pub source: ExecutorSource,
    /// The source auto-mode actually resolved to. Set once the background task
    /// makes the decision; `None` while the scan is queued or if the caller
    /// requested an explicit source.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effective_source: Option<ExecutorSource>,
    pub status: JobStatus,
    pub progress: f64,
    pub documents_scanned: u64,
    /// Values-mode field echo (also set on snapshot jobs).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unique_values: Option<u64>,
    /// Values mode: the per-job distinct-value cap was crossed, so the value
    /// listing is incomplete (retained values keep exact counts).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncated: Option<bool>,
    /// Ranked values scans: a shard saturated its `top_k` cap (or the deadline
    /// elapsed), so per-value counts are `>=` lower bounds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bounded: Option<bool>,
    /// Ranked `ann` values scans carry ANN recall error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approximate: Option<bool>,
    /// Epoch-ms `cache_warmed_through` watermark applied to a cache scan, or
    /// the watermark stamped on the cache after an origin scan completes.
    /// `None` until the consistency watcher has observed a clean namespace.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stable_as_of: Option<u64>,
    /// Effective origin fan-out width. Present only for jobs that actually ran
    /// origin scatter/gather.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threads: Option<u32>,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SnapshotJob {
    pub id: String,
    pub namespace: String,
    pub field: String,
    pub source: SnapshotSource,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effective_source: Option<SnapshotSource>,
    pub status: JobStatus,
    pub progress: f64,
    pub documents_scanned: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stable_as_of: Option<u64>,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SnapshotJobList {
    pub snapshot_jobs: Vec<SnapshotJob>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WarmJob {
    pub id: String,
    pub namespace: String,
    pub status: JobStatus,
    pub progress: f64,
    pub documents_scanned: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stable_as_of: Option<u64>,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct WarmJobList {
    pub warm_jobs: Vec<WarmJob>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScanJob {
    pub id: String,
    pub namespace: String,
    pub mode: ScanMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    pub source: ScanSource,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effective_source: Option<ScanSource>,
    pub status: JobStatus,
    pub progress: f64,
    pub documents_scanned: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unique_values: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncated: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bounded: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approximate: Option<bool>,
    /// Snapshot-served values scans echo the snapshot body's identity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_sha: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub watermark_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threads: Option<u32>,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ScanJobList {
    pub scans: Vec<ScanJob>,
}

#[derive(Debug, Serialize)]
pub struct ScanIdsResponse {
    pub ids: Vec<String>,
    pub total: u64,
}

/// One distinct value of a values scan, in the snapshot body's `{v, n}`
/// vocabulary: `v` is the value, `n` its document count.
#[derive(Debug, Clone, Serialize)]
pub struct ScanValue {
    pub v: String,
    pub n: u64,
}

#[derive(Debug, Serialize)]
pub struct ScanValuesResponse {
    pub values: Vec<ScanValue>,
    /// Total distinct values held by the job (before `limit`/`offset`).
    pub total: u64,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScanCountResponse {
    pub count: u64,
    pub served_by: ScanCountServedBy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_sha: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub watermark_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bounded: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timed_out: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shards_saturated: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shards_total: Option<u64>,
    /// `true` when the count carries ANN recall error (radius scans): the
    /// index's membership of the distance ball is itself fuzzy, independent
    /// of saturation. Absent (≡ exact) on filter and FTS scans.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approximate: Option<bool>,
    /// Effective origin fan-out width. Present only when the count ran origin
    /// scatter/gather; snapshot and cache reads do not fan out.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threads: Option<u32>,
    pub elapsed_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct WarmCacheResponse {
    pub namespace: String,
    pub turbopuffer: WarmStepResponse,
    pub documents: WarmDocumentsResponse,
    pub snapshots: WarmSnapshotsResponse,
    pub blobs: WarmBlobsResponse,
}

#[derive(Debug, Clone, Serialize)]
pub struct WarmStepResponse {
    pub enabled: bool,
    pub status: WarmStepStatus,
}

#[derive(Debug, Clone, Serialize)]
pub struct WarmDocumentsResponse {
    pub enabled: bool,
    pub status: WarmStepStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job: Option<WarmJob>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WarmSnapshotsResponse {
    pub enabled: bool,
    pub status: WarmStepStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub watermark_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WarmBlobsResponse {
    pub enabled: bool,
    pub status: WarmStepStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attributes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub budget_bytes: Option<u64>,
    pub documents_scanned: u64,
    pub refs_seen: u64,
    pub objects: u64,
    pub bytes: u64,
    pub missing: u64,
    pub invalid_refs: u64,
    pub budget_exhausted: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WarmStepStatus {
    Skipped,
    Completed,
    Started,
    NoSnapshot,
}

// --- Search history models ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchHistoryEntry {
    pub timestamp: String,
    pub timestamp_nanos: u64,
    pub namespace: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_query: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stable_as_of: Option<u64>,
    pub query: Value,
    pub top_result_ids: Vec<String>,
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct SearchHistoryListResponse {
    pub entries: Vec<SearchHistoryEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickstreamEvent {
    pub timestamp: String,
    pub timestamp_nanos: u64,
    pub trace_id: String,
    pub namespace: String,
    pub doc_id: String,
    #[serde(default)]
    pub tags: Vec<String>,
    pub source: String,
    pub served_from: String,
}

#[derive(Debug, Serialize)]
pub struct ClickstreamListResponse {
    pub events: Vec<ClickstreamEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

// --- Restore models ---

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestoreSourceType {
    Managed,
    Direct,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreSource {
    #[serde(rename = "type")]
    pub source_type: RestoreSourceType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ref: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestoreIfExists {
    Replace,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreOptions {
    #[serde(default)]
    pub if_exists: Option<RestoreIfExists>,
    #[serde(default)]
    pub verify_after_restore: bool,
}

#[derive(Debug, Deserialize)]
pub struct CreateRestoreRequest {
    pub stream: String,
    pub target_dsn: String,
    pub source: RestoreSource,
    #[serde(default)]
    pub options: Option<RestoreOptions>,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ListRestoresQuery {
    #[serde(default)]
    pub stream: Option<String>,
    #[serde(default)]
    pub status: Option<RestoreStatus>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RestoreStatus {
    Queued,
    Running,
    Verifying,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VerificationStatus {
    NotRun,
    Passed,
    PassedWithUnknowns,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RestoreCounts {
    pub wal_segments: u64,
    pub wal_records: u64,
    pub clog_events: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreVerificationSummary {
    pub wal_contiguous: bool,
    pub missing_disposition_ranges: u64,
    pub unknown_ranges: u64,
    pub count_mismatches: u64,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreRunResponse {
    pub id: String,
    pub stream: String,
    pub status: RestoreStatus,
    pub verification_status: VerificationStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub source: RestoreSource,
    pub target_dsn: String,
    pub counts: RestoreCounts,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification: Option<RestoreVerificationSummary>,
}

// --- Pipeline models ---

#[derive(Debug, Deserialize)]
pub struct CreatePipelineRequest {
    pub id: String,
    pub target_namespace: String,
    #[serde(default = "default_distance_metric")]
    pub distance_metric: String,
}

fn default_distance_metric() -> String {
    "cosine_distance".to_string()
}

#[derive(Debug, Serialize)]
pub struct PipelineResponse {
    pub id: String,
    pub target_namespace: String,
    pub distance_metric: String,
    pub created_at: String,
}

#[derive(Debug, Serialize)]
pub struct PipelineStatusResponse {
    pub pipeline_id: String,
    pub status: String,
    pub counts: HashMap<String, u64>,
    pub failed_reasons: HashMap<String, u64>,
    pub pending_count: u64,
    pub processing_count: u64,
    pub failed_count: u64,
    pub indexed_rate_per_min: f64,
    pub rate_window_seconds: u64,
}

#[derive(Debug, Serialize)]
pub struct ListPipelinesResponse {
    pub pipelines: Vec<PipelineResponse>,
}

fn default_claim_from_stage() -> String {
    "pending".to_string()
}

fn default_claim_to_stage() -> String {
    "embedding".to_string()
}

fn default_claim_limit() -> u32 {
    100
}

fn default_claim_lease_seconds() -> i64 {
    900
}

#[derive(Debug, Deserialize)]
pub struct StageDocumentRequest {
    pub chunks: Vec<Chunk>,
}

#[derive(Debug, Deserialize)]
pub struct ClaimDocumentsRequest {
    #[serde(default = "default_claim_from_stage")]
    pub stage: String,
    #[serde(default = "default_claim_to_stage")]
    pub claim_stage: String,
    #[serde(default = "default_claim_limit")]
    pub limit: u32,
    pub worker_id: String,
    #[serde(default = "default_claim_lease_seconds")]
    pub lease_seconds: i64,
    #[serde(default)]
    pub document_id_prefix: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct HeartbeatDocumentsRequest {
    pub document_ids: Vec<String>,
    #[serde(default = "default_claim_to_stage")]
    pub stage: String,
    pub worker_id: String,
}

#[derive(Debug, Deserialize)]
pub struct SetDocumentsStageRequest {
    pub document_ids: Vec<String>,
    pub stage: String,
    #[serde(default)]
    pub from_stage: Option<String>,
    #[serde(default)]
    pub worker_id: Option<String>,
    #[serde(default)]
    pub create_missing: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Chunk {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct StageDocumentResponse {
    pub pipeline_id: String,
    pub document_id: String,
    pub stage: String,
    pub chunk_count: i32,
    pub chunk_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ClaimDocumentsResponse {
    pub pipeline_id: String,
    pub stage: String,
    pub claim_stage: String,
    pub worker_id: String,
    pub documents: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct DocumentsStageResponse {
    pub pipeline_id: String,
    pub stage: String,
    pub updated: u64,
}

#[derive(Debug, Deserialize)]
pub struct WriteVectorsRequest {
    pub vectors: Vec<VectorDoc>,
}

#[derive(Debug, Deserialize)]
pub struct VectorDoc {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<Vec<f64>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vectors: Option<Vec<Vec<f64>>>,
    #[serde(default)]
    pub attributes: HashMap<String, Value>,
}

#[derive(Debug, Serialize)]
pub struct ChunkResponse {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

// --- UDF models ---

fn default_udf_batch_size() -> u32 {
    32
}

fn default_udf_lease_seconds() -> i64 {
    120
}

fn default_scan_page_size() -> u32 {
    10_000
}

#[derive(Debug, Deserialize)]
pub struct CreateUdfRequest {
    pub id: String,
    pub spec: UdfSpec,
}

#[derive(Debug, Deserialize)]
pub struct UpdateUdfRequest {
    pub spec: UdfSpec,
}

#[derive(Debug, Serialize)]
pub struct UdfResponse {
    pub id: String,
    pub spec: UdfSpec,
    pub paused: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Serialize)]
pub struct ListUdfsResponse {
    pub udfs: Vec<UdfResponse>,
}

#[derive(Debug, Serialize)]
pub struct GetUdfResponse {
    pub udf: UdfResponse,
    pub status: UdfStatusResponse,
}

#[derive(Debug, Serialize)]
pub struct UdfStatusResponse {
    pub udf_id: String,
    pub paused: bool,
    pub active_namespaces: Vec<String>,
    pub discovery: UdfDiscoveryStatusResponse,
    pub counts: HashMap<String, u64>,
    pub pending_count: u64,
    pub processing_count: u64,
    pub failed_count: u64,
    pub indexed_rate_per_min: f64,
    pub rate_window_seconds: u64,
}

#[derive(Debug, Serialize)]
pub struct UdfDiscoveryStatusResponse {
    pub sweeps_completed: u64,
    pub last_completed_at: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UdfDiscoverRequest {
    #[serde(default)]
    pub namespaces: Vec<String>,
    #[serde(default = "default_scan_page_size")]
    pub page_size: u32,
}

#[derive(Debug, Serialize)]
pub struct UdfDiscoverResponse {
    pub udf_id: String,
    pub enqueued: u64,
    pub namespaces: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct UdfClaimRequest {
    pub worker_id: String,
    #[serde(default = "default_udf_batch_size")]
    pub limit: u32,
    #[serde(default = "default_udf_lease_seconds")]
    pub lease_seconds: i64,
}

#[derive(Debug, Serialize)]
pub struct UdfClaimedItem {
    pub namespace: String,
    pub id: String,
    pub input: HashMap<String, Value>,
}

#[derive(Debug, Serialize)]
pub struct UdfClaimResponse {
    pub udf_id: String,
    pub worker_id: String,
    pub items: Vec<UdfClaimedItem>,
}

#[derive(Debug, Deserialize)]
pub struct UdfHeartbeatRequest {
    pub worker_id: String,
    pub items: Vec<UdfItemRef>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct UdfItemRef {
    pub namespace: String,
    pub id: String,
}

#[derive(Debug, Serialize)]
pub struct UdfItemsResponse {
    pub udf_id: String,
    pub updated: u64,
}

#[derive(Debug, Deserialize)]
pub struct UdfCompleteRequest {
    pub worker_id: String,
    pub items: Vec<UdfCompleteItem>,
}

#[derive(Debug, Deserialize)]
pub struct UdfCompleteItem {
    pub namespace: String,
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<Vec<f64>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vectors: Option<Vec<Vec<f64>>>,
    #[serde(default)]
    pub attributes: HashMap<String, Value>,
}

#[derive(Debug, Deserialize)]
pub struct UdfFailRequest {
    pub worker_id: String,
    pub items: Vec<UdfFailItem>,
}

#[derive(Debug, Deserialize)]
pub struct UdfFailItem {
    pub namespace: String,
    pub id: String,
    pub kind: UdfErrorKind,
    #[serde(default)]
    pub message: Option<String>,
}
