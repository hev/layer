pub mod receipts;
pub const TURBOPUFFER_CAPABILITIES: crate::capabilities::Capabilities =
    crate::capabilities::Capabilities {
        kind: "turbopuffer",
        coverage: turbopuffer_coverage,
        // RFC 0117 declarations table: native embed, no schema limits.
        limits: crate::capabilities::SchemaLimits {
            embed: crate::capabilities::Coverage::supported(),
            max_gateway_embed_attributes: None,
            max_full_text_search_fields: None,
            max_vector_fields: None,
        },
        // A blob-set namespace with a native `bytes` attribute.
        blobs: crate::capabilities::BlobStorage::native(Some(TURBOPUFFER_MAX_BLOB_BYTES)),
    };

/// Write-body key carrying a namespace's gateway embedding profiles to a store
/// that keeps them beside its schema (RFC 0118 step E). The store persists the
/// value in the write's own transaction and returns it from
/// [`TurbopufferClient::embedding_profiles`]; it never reaches the schema.
pub const EMBEDDING_PROFILES_KEY: &str = "_hevlayer_embedding_profiles";

/// The largest blob a turbopuffer `bytes` value holds. turbopuffer caps a
/// value at 8 MiB measured on the base64 wire string, so the decoded cap is
/// three quarters of that: 6 MiB.
pub const TURBOPUFFER_MAX_BLOB_BYTES: u64 = 8 * 1024 * 1024 / 4 * 3;
/// The `bytes` attribute holding a blob in its blob-set namespace.
const BLOB_DATA_ATTRIBUTE: &str = "data";
const TURBOPUFFER_MAX_NAMESPACE_LEN: usize = 128;

/// The blob-set namespace holding a namespace's blobs, keyed by sha256.
pub fn blob_set_namespace(namespace: &str) -> String {
    format!("{namespace}__hevlayer_blobs")
}

/// A namespace whose blob-set name would exceed turbopuffer's namespace
/// length keeps its blobs in S3.
fn turbopuffer_blob_storage(namespace: &str) -> crate::capabilities::BlobStorage {
    if blob_set_namespace(namespace).len() <= TURBOPUFFER_MAX_NAMESPACE_LEN {
        TURBOPUFFER_CAPABILITIES.blobs
    } else {
        crate::capabilities::BlobStorage::NONE
    }
}

fn blob_rejection(kind: &str) -> TurbopufferError {
    crate::capabilities::unsupported_by_store(kind, "blobs", Some("the store has no native bytes"))
}

fn decode_blob_value(value: &Value) -> Result<Vec<u8>, TurbopufferError> {
    use base64::Engine as _;
    let encoded = value.as_str().ok_or_else(|| {
        TurbopufferError::Other("blob row has no base64 `data` attribute".to_string())
    })?;
    base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|e| TurbopufferError::Other(format!("blob row `data` is not base64: {e}")))
}

fn encode_blob_value(bytes: &[u8]) -> Value {
    use base64::Engine as _;
    Value::String(base64::engine::general_purpose::STANDARD.encode(bytes))
}

fn turbopuffer_coverage(
    feature: crate::capabilities::WireFeature,
) -> crate::capabilities::Coverage {
    use crate::capabilities::{Coverage, WireFeature::*};
    match feature {
        NamespaceCrud | UpsertRows | UpsertColumns | DeleteIds | Fetch | Dense | DistanceMetric
        | Fts | Hybrid | Projection | ScalarFilters | NotFilters | ArrayFilters | RegexFilters
        | AdvancedFilters | Fuzzy | AdvancedText | MultiVector | MultipleFields | MultiQuery
        | Pagination | OrderedScan | Aggregate | PatchRows | PatchColumns | ConditionalWrites
        | Copy | Branch | Encryption | Export | Warm | Consistency | Snapshots | Udf
        | Passthrough | Embed | NearestToId | Temporal | LegBreakdown | Collapse | Auto
        | Threads | VectorEncoding | Search => Coverage::supported(),
        DeleteByFilter | Facet => Coverage::approximate(
            "Native wire request only; the optional portable adapter primitive is unavailable.",
        ),
        _ => Coverage::unsupported(),
    }
}

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;

use crate::models::{
    id_from_wire, DocumentPage, DocumentResponse, FieldValueResult, IncludeAttributes, QueryResult,
};

tokio::task_local! {
    static REQUEST_UPSTREAM_API_KEY: String;
}

pub async fn scope_upstream_api_key<F>(api_key: String, future: F) -> F::Output
where
    F: std::future::Future,
{
    REQUEST_UPSTREAM_API_KEY.scope(api_key, future).await
}

/// Runs immediately before each physical provider query attempt. Cache hits
/// and empty fetches do not call it. Failed/429/canceled dispatched attempts
/// consume their reservation; callers never refund uncertain upstream work.
pub type QueryPermit = Arc<
    dyn Fn(
            String,
            u32,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<(), TurbopufferError>> + Send>,
        > + Send
        + Sync,
>;
tokio::task_local! { static QUERY_PERMIT: QueryPermit; }
pub async fn scope_query_permit<F: std::future::Future>(
    permit: QueryPermit,
    future: F,
) -> F::Output {
    // Nested consumers must retain Function admission. A successful earlier
    // reservation is never refunded if another permit rejects or is canceled.
    let permit = if let Ok(inherited) = QUERY_PERMIT.try_with(Clone::clone) {
        Arc::new(move |namespace: String, queries| {
            let inherited = inherited.clone();
            let permit = permit.clone();
            Box::pin(async move {
                inherited(namespace.clone(), queries).await?;
                permit(namespace, queries).await
            })
                as std::pin::Pin<
                    Box<dyn std::future::Future<Output = Result<(), TurbopufferError>> + Send>,
                >
        }) as QueryPermit
    } else {
        permit
    };
    QUERY_PERMIT.scope(permit, future).await
}
async fn reserve_provider_query(namespace: &str, queries: u32) -> Result<(), TurbopufferError> {
    let permit = QUERY_PERMIT.try_with(Clone::clone).ok();
    if let Some(permit) = permit {
        permit(namespace.to_owned(), queries).await?;
    }
    Ok(())
}

// Opt-in for financially admitted capture scans. Existing callers retain their
// compatibility path; this scope forbids auxiliary metadata IO inside a page.
tokio::task_local! { static PREPARED_SCAN_METADATA_ONLY: (); }
pub async fn scope_prepared_scan_metadata<F: std::future::Future>(future: F) -> F::Output {
    PREPARED_SCAN_METADATA_ONLY.scope((), future).await
}

/// Observer for billing otherwise discarded by row-only read interfaces.
pub type ReadBillingObserver = std::sync::Arc<dyn Fn(&str, &Value) + Send + Sync>;
tokio::task_local! {
    static READ_BILLING_OBSERVER: ReadBillingObserver;
}
pub async fn scope_read_billing<F: std::future::Future>(
    observer: ReadBillingObserver,
    future: F,
) -> F::Output {
    let inherited = READ_BILLING_OBSERVER.try_with(Clone::clone).ok();
    let composed: ReadBillingObserver = match inherited {
        Some(parent) => Arc::new(move |namespace, billing| {
            parent(namespace, billing);
            observer(namespace, billing);
        }),
        None => observer,
    };
    READ_BILLING_OBSERVER.scope(composed, future).await
}
/// Report upstream billing before projecting a response into another result.
pub fn observe_projected_billing(namespace: &str, response: &Value) {
    if let Some(billing) = response.get("billing") {
        let _ = READ_BILLING_OBSERVER.try_with(|observer| observer(namespace, billing));
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TurbopufferError {
    #[error("Function provider query budget exhausted")]
    QueryBudgetExhausted,
    /// Synthetic/mock HTTP 429. Real HTTP responses use `Response`; the
    /// query path recognizes both through `is_rate_limited`.
    #[error("Turbopuffer rate limited: {0}")]
    RateLimited(String),
    /// Synthetic/mock HTTP 404. Real HTTP responses use `Response`; routes
    /// that special-case absence recognize both through `is_not_found`.
    #[error("Turbopuffer not found: {0}")]
    NotFound(String),
    /// A completed upstream HTTP response. Keep the wire response separate
    /// from transport and gateway-originated failures so transparent routes
    /// can return its status, content type, and body unchanged.
    #[error("Turbopuffer HTTP response: {0:?}")]
    Response(TurbopufferPassthroughResponse),
    #[error("Turbopuffer error: {0}")]
    Other(String),
}

impl TurbopufferError {
    /// Construct from an HTTP status + JSON body when only decoded response
    /// data is available (for example, from a non-Turbopuffer backend).
    pub fn from_status(status: reqwest::StatusCode, body: &str) -> Self {
        Self::Response(TurbopufferPassthroughResponse {
            status: status.as_u16(),
            content_type: Some("application/json".to_string()),
            body: body.as_bytes().to_vec(),
        })
    }

    /// Capture a completed HTTP error response without decoding or rewriting
    /// its body. Body-read failures remain transport failures and may map to
    /// a gateway 502.
    pub async fn from_response(response: reqwest::Response) -> Self {
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        match response.bytes().await {
            Ok(body) => Self::Response(TurbopufferPassthroughResponse {
                status,
                content_type,
                body: body.to_vec(),
            }),
            Err(error) => Self::Other(format!("failed to read Turbopuffer response body: {error}")),
        }
    }

    pub fn is_rate_limited(&self) -> bool {
        matches!(self, Self::RateLimited(_))
            || matches!(self, Self::Response(response) if response.status == 429)
    }

    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound(_))
            || matches!(self, Self::Response(response) if response.status == 404)
    }
}

/// Indexing state from turbopuffer's `/metadata` response.
///
/// Turbopuffer documents `index.status` as either `"up-to-date"` or
/// `"updating"`, with `index.unindexed_bytes` present only when status is
/// `updating`. We model "no signal observed" explicitly as `Unknown` so the
/// query path can distinguish cold-start from confirmed-stable.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IndexStatus {
    Stable,
    Updating,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Default)]
pub struct NamespaceMeta {
    /// Last-observed `index.status`. `Unknown` means the field was missing
    /// from the response — treated as "not stable" by the watcher but as
    /// "no filter needed" by the query path (per the cold-start contract).
    pub index_status: IndexStatus,
    /// Present only when `index_status == Updating` (or pulled from a legacy
    /// top-level field). `None` does NOT imply zero — it means "no signal".
    pub unindexed_bytes: Option<u64>,
    pub approx_row_count: u64,
    pub approx_logical_bytes: Option<u64>,
    /// Optional backend-specific settle key. Backends without a durable LSN can
    /// set this to a count-like value; the consistency watcher advances only
    /// after seeing the same value in two consecutive polls.
    pub count_settle: Option<u64>,
    /// Full turbopuffer response, kept so `/v2/namespaces/{ns}/metadata` can
    /// proxy the upstream body verbatim alongside our enhancement fields.
    pub raw: Value,
}

impl NamespaceMeta {
    /// True only when we have positive evidence the index is caught up —
    /// either `index.status == "up-to-date"`, or no `unindexed_bytes > 0`
    /// signal anywhere in the body and status is not `Updating`. Returns
    /// false for `Unknown`; the watcher uses this to gate watermark advance.
    pub fn is_stable(&self) -> bool {
        match self.index_status {
            IndexStatus::Stable => true,
            IndexStatus::Updating => false,
            IndexStatus::Unknown => false,
        }
    }
}

/// Recursively scan a JSON value for any `unindexed_bytes` field with a
/// non-zero u64 value. Used as a defensive fallback when turbopuffer's
/// response shape moves the field around.
fn any_unindexed_bytes_nonzero(value: &Value) -> bool {
    match value {
        Value::Object(map) => map.iter().any(|(k, v)| {
            (k == "unindexed_bytes" && v.as_u64().is_some_and(|n| n > 0))
                || any_unindexed_bytes_nonzero(v)
        }),
        Value::Array(arr) => arr.iter().any(any_unindexed_bytes_nonzero),
        _ => false,
    }
}

#[async_trait]
pub trait TurbopufferClient: Send + Sync {
    /// Adapters that must see the complete wire request before gateway parsing
    /// (including unknown options and numeric IDs) opt out of portable writes
    /// and parsed single queries. Gateway-owned hybrid orchestration still uses legs.
    /// These adapters encode ranked-query IDs as serialized JSON in the trait's
    /// string ID slot; the gateway restores the wire type after grouping/fusion.
    fn requires_native_wire(&self, _namespace: &str) -> bool {
        false
    }

    /// Local adapters may fail readiness when their required database or
    /// extensions disappear. Remote proxy backends keep their existing policy.
    async fn check_readiness(&self) -> Result<(), TurbopufferError> {
        Ok(())
    }

    fn capabilities(&self) -> crate::capabilities::Capabilities {
        crate::capabilities::UNDECLARED
    }

    /// The capabilities of the store `namespace` resolves to. Routers answer
    /// for that store; a single store answers for itself.
    fn capabilities_for_namespace(&self, _namespace: &str) -> crate::capabilities::Capabilities {
        self.capabilities()
    }

    /// The capabilities of a configured VectorStore by name. `None` when this
    /// client does not know the store. A single store does not know its own
    /// resource name, so only routers answer.
    fn store_capabilities(&self, _store: &str) -> Option<crate::capabilities::Capabilities> {
        None
    }

    /// Where this store keeps blob bytes for `namespace`. Routers answer for
    /// the store the namespace resolves to.
    fn blob_storage(&self, _namespace: &str) -> crate::capabilities::BlobStorage {
        self.capabilities().blobs
    }

    /// Store a blob in the store itself, keyed by its sha256. Idempotent.
    /// Only called when [`Self::blob_storage`] holds the blob.
    async fn put_blob(
        &self,
        _namespace: &str,
        _sha256: &str,
        _bytes: &[u8],
    ) -> Result<(), TurbopufferError> {
        Err(blob_rejection(self.capabilities().kind))
    }

    /// Read a blob the store holds. `None` when the store has no such blob.
    async fn get_blob(
        &self,
        _namespace: &str,
        _sha256: &str,
    ) -> Result<Option<Vec<u8>>, TurbopufferError> {
        Err(blob_rejection(self.capabilities().kind))
    }

    /// Raw Turbopuffer-compatible pass-through for API surfaces where
    /// hevlayer does not add cache/history/consistency behavior.
    async fn passthrough(
        &self,
        method: &str,
        path: &str,
        query: Option<&str>,
        body: Option<Value>,
    ) -> Result<TurbopufferPassthroughResponse, TurbopufferError>;

    /// Delete all backend state for a namespace.
    async fn delete_namespace(
        &self,
        namespace: &str,
    ) -> Result<TurbopufferPassthroughResponse, TurbopufferError> {
        self.passthrough("DELETE", &format!("/v2/namespaces/{namespace}"), None, None)
            .await
    }

    /// Hint turbopuffer to prepare this namespace for low-latency requests.
    /// Mirrors `GET /v1/namespaces/{namespace}/hint_cache_warm`.
    async fn hint_cache_warm(&self, namespace: &str) -> Result<(), TurbopufferError>;

    async fn upsert(
        &self,
        namespace: &str,
        docs: &[UpsertDoc],
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError>;

    /// Column-level merge. Calls turbopuffer `patch_rows`: only the supplied
    /// attribute keys are written; everything else on the existing row stays.
    /// Vectors cannot be patched upstream.
    async fn patch(
        &self,
        namespace: &str,
        docs: &[PatchDoc],
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError>;

    /// Column-shaped merge helper for high-throughput writeback paths. The
    /// `id` array and every attribute array must be the same length; values are
    /// paired positionally by Turbopuffer.
    async fn patch_columns(
        &self,
        namespace: &str,
        columns: &PatchColumns,
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError>;

    async fn delete(
        &self,
        namespace: &str,
        ids: &[String],
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError>;

    async fn delete_by_filter(
        &self,
        _namespace: &str,
        _filters: &Value,
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        self.capabilities()
            .unimplemented(crate::capabilities::WireFeature::DeleteByFilter)
    }

    async fn import_arrow(
        &self,
        _namespace: &str,
        _content_type: &str,
        _body: Vec<u8>,
    ) -> Result<TurbopufferPassthroughResponse, TurbopufferError> {
        self.capabilities()
            .unimplemented(crate::capabilities::WireFeature::ImportArrow)
    }

    async fn query(
        &self,
        namespace: &str,
        vector: &[f64],
        top_k: u32,
        filters: Option<&Value>,
        include_attributes: Option<&IncludeAttributes>,
    ) -> Result<TurbopufferQueryOutcome, TurbopufferError>;

    /// Generic ranked query. `rank_by` is a turbopuffer-shaped tuple:
    /// `["vector", "ANN", [...]]` for vector ANN, or `["text_field", "BM25",
    /// "query string"]` for FTS. Backs the `fts` and `ann` scan selectors so the
    /// same primitive powers both shapes; vector-only callers should keep
    /// using `query`.
    async fn ranked_query(
        &self,
        namespace: &str,
        rank_by: &Value,
        top_k: u32,
        filters: Option<&Value>,
        include_attributes: Option<&IncludeAttributes>,
    ) -> Result<TurbopufferQueryOutcome, TurbopufferError>;

    /// Native upstream multi-query primitive. Callers pass already-rewritten
    /// Turbopuffer query legs and receive the upstream multi-query response
    /// body (`{results: [{rows: ...}]}`) as JSON. With `rerank_by` set
    /// (e.g. `["RRF", {"rank_constant": 60}]`) upstream fuses the legs into
    /// one ranked list and the response carries the fused rows instead of
    /// per-leg results.
    async fn multi_ranked_query(
        &self,
        namespace: &str,
        legs: &[Value],
        rerank_by: Option<&Value>,
    ) -> Result<Value, TurbopufferError>;

    async fn fetch(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<DocumentResponse>, TurbopufferError>;

    async fn fetch_many(
        &self,
        namespace: &str,
        ids: &[String],
    ) -> Result<HashMap<String, DocumentResponse>, TurbopufferError>;

    /// Complete parent membership, with full attributes. Never return a
    /// truncated group: callers fan output back to every sibling.
    async fn fetch_siblings(
        &self,
        _namespace: &str,
        _parent: &str,
    ) -> Result<Vec<DocumentResponse>, TurbopufferError> {
        Err(TurbopufferError::Other(
            "parent lookup is unavailable for this store".into(),
        ))
    }

    /// Pull a document's embedding vector from Turbopuffer. Used as the
    /// pull-through fallback when search-by-id misses the Aerospike cache.
    /// Returns `None` if the doc has no row upstream or no vector column.
    async fn fetch_vector(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<Vec<f64>>, TurbopufferError>;

    async fn scan_page(
        &self,
        namespace: &str,
        cursor: Option<&str>,
        page_size: u32,
        filters: Option<&Value>,
        include_attributes: Option<&[String]>,
    ) -> Result<DocumentPage, TurbopufferError>;

    async fn facet(
        &self,
        _namespace: &str,
        _filters: Option<&Value>,
        _field: &str,
        _top: usize,
    ) -> Result<Vec<FieldValueResult>, TurbopufferError> {
        self.capabilities()
            .unimplemented(crate::capabilities::WireFeature::Facet)
    }

    /// Exhaustive strong pages for an owner-guarded complete capture. Unsupported
    /// adapters must refuse, never silently use eventual scan_page.
    async fn scan_page_strong(
        &self,
        _namespace: &str,
        _cursor: Option<&str>,
        _page_size: u32,
        _filters: Option<&Value>,
        _include_attributes: Option<&[String]>,
    ) -> Result<DocumentPage, TurbopufferError> {
        Err(TurbopufferError::Other(
            "strong capture scan unsupported by store".into(),
        ))
    }

    async fn head_namespace(&self, namespace: &str) -> Result<NamespaceMeta, TurbopufferError>;

    /// Durable identity that changes for every committed data mutation,
    /// including external writes and deletes. Schema is checked separately. Timestamp/count hints do not meet
    /// this contract. None means safe unchanged-scan suppression is unavailable.
    async fn reconcile_change_token(
        &self,
        _namespace: &str,
    ) -> Result<Option<String>, TurbopufferError> {
        Ok(None)
    }

    /// Gateway embedding profiles the store holds for `namespace`, as last
    /// written under [`EMBEDDING_PROFILES_KEY`] (RFC 0118 step E). `None` when
    /// the store keeps none or the namespace does not exist.
    async fn embedding_profiles(
        &self,
        _namespace: &str,
    ) -> Result<Option<Value>, TurbopufferError> {
        Ok(None)
    }

    /// Pin durable cleanup to its original store even after Index routing changes.
    /// Single-store clients already represent that store; routers override this.
    async fn head_namespace_in_store(
        &self,
        namespace: &str,
        _store: &str,
    ) -> Result<NamespaceMeta, TurbopufferError> {
        self.head_namespace(namespace).await
    }

    async fn delete_namespace_in_store(
        &self,
        namespace: &str,
        _store: &str,
    ) -> Result<TurbopufferPassthroughResponse, TurbopufferError> {
        self.delete_namespace(namespace).await
    }
}

#[derive(Debug, Clone)]
pub struct UpsertDoc {
    pub id: String,
    pub vector: Option<Vec<f64>>,
    pub vectors: Option<Vec<Vec<f64>>>,
    pub attributes: HashMap<String, Value>,
}

#[derive(Debug, Clone, Default)]
pub struct TurbopufferWriteOutcome {
    pub billing: Option<Value>,
}

#[derive(Debug, Clone, Default)]
pub struct TurbopufferQueryOutcome {
    pub rows: Vec<QueryResult>,
    pub billing: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct PatchDoc {
    pub id: String,
    pub attributes: HashMap<String, Value>,
}

#[derive(Debug, Clone)]
pub struct PatchColumns {
    pub ids: Vec<String>,
    pub columns: HashMap<String, Vec<Value>>,
}

impl PatchColumns {
    pub fn new(
        ids: Vec<String>,
        columns: HashMap<String, Vec<Value>>,
    ) -> Result<Self, TurbopufferError> {
        if ids.is_empty() {
            return Err(TurbopufferError::Other(
                "patch_columns requires at least one id".to_string(),
            ));
        }
        for (name, values) in &columns {
            if name == "id" {
                return Err(TurbopufferError::Other(
                    "patch_columns attribute map must not include id".to_string(),
                ));
            }
            if values.len() != ids.len() {
                return Err(TurbopufferError::Other(format!(
                    "patch_columns column '{}' has {} values for {} ids",
                    name,
                    values.len(),
                    ids.len()
                )));
            }
        }
        Ok(Self { ids, columns })
    }

    pub fn from_docs(docs: &[PatchDoc]) -> Result<Self, TurbopufferError> {
        let ids: Vec<String> = docs.iter().map(|doc| doc.id.clone()).collect();
        if ids.is_empty() {
            return Err(TurbopufferError::Other(
                "patch_columns requires at least one id".to_string(),
            ));
        }

        let mut columns: HashMap<String, Vec<Value>> = HashMap::new();
        for doc in docs {
            for (name, value) in &doc.attributes {
                columns.entry(name.clone()).or_default().push(value.clone());
            }
        }

        for (name, values) in &columns {
            if values.len() != ids.len() {
                return Err(TurbopufferError::Other(format!(
                    "patch_columns_from_docs requires every row to include '{}'",
                    name
                )));
            }
        }

        Self::new(ids, columns)
    }
}

#[derive(Clone)]
pub struct TurbopufferPassthroughResponse {
    pub status: u16,
    pub content_type: Option<String>,
    pub body: Vec<u8>,
}

impl std::fmt::Debug for TurbopufferPassthroughResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TurbopufferPassthroughResponse")
            .field("status", &self.status)
            .field("content_type", &self.content_type)
            .field("body", &String::from_utf8_lossy(&self.body))
            .finish()
    }
}

pub struct RoutingTurbopufferClient {
    default_store: String,
    clients: HashMap<String, Arc<dyn TurbopufferClient>>,
    namespace_store_refs: Arc<RwLock<HashMap<String, String>>>,
}

impl RoutingTurbopufferClient {
    pub fn new(
        default_store: String,
        clients: HashMap<String, Arc<dyn TurbopufferClient>>,
        namespace_store_refs: Arc<RwLock<HashMap<String, String>>>,
    ) -> Self {
        Self {
            default_store,
            clients,
            namespace_store_refs,
        }
    }

    fn client_for_namespace(
        &self,
        namespace: Option<&str>,
    ) -> Result<Arc<dyn TurbopufferClient>, TurbopufferError> {
        let store_name = namespace
            .and_then(|namespace| {
                crate::namespace_pattern::resolve(
                    &self
                        .namespace_store_refs
                        .read()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()),
                    namespace,
                )
                .cloned()
            })
            .unwrap_or_else(|| self.default_store.clone());

        self.clients.get(&store_name).cloned().ok_or_else(|| {
            TurbopufferError::Other(format!(
                "VectorStore client {store_name:?} is not configured"
            ))
        })
    }
}

#[async_trait]
impl TurbopufferClient for RoutingTurbopufferClient {
    async fn head_namespace_in_store(
        &self,
        namespace: &str,
        store: &str,
    ) -> Result<NamespaceMeta, TurbopufferError> {
        self.clients
            .get(store)
            .ok_or_else(|| {
                TurbopufferError::Other(format!("VectorStore client {store:?} is not configured"))
            })?
            .head_namespace(namespace)
            .await
    }

    async fn delete_namespace_in_store(
        &self,
        namespace: &str,
        store: &str,
    ) -> Result<TurbopufferPassthroughResponse, TurbopufferError> {
        self.clients
            .get(store)
            .ok_or_else(|| {
                TurbopufferError::Other(format!("VectorStore client {store:?} is not configured"))
            })?
            .delete_namespace(namespace)
            .await
    }

    async fn check_readiness(&self) -> Result<(), TurbopufferError> {
        for client in self.clients.values() {
            client.check_readiness().await?;
        }
        Ok(())
    }

    fn requires_native_wire(&self, namespace: &str) -> bool {
        self.client_for_namespace(Some(namespace))
            .is_ok_and(|client| client.requires_native_wire(namespace))
    }

    fn capabilities_for_namespace(&self, namespace: &str) -> crate::capabilities::Capabilities {
        self.client_for_namespace(Some(namespace))
            .map_or(crate::capabilities::UNDECLARED, |client| {
                client.capabilities_for_namespace(namespace)
            })
    }

    fn store_capabilities(&self, store: &str) -> Option<crate::capabilities::Capabilities> {
        self.clients.get(store).map(|client| client.capabilities())
    }

    fn blob_storage(&self, namespace: &str) -> crate::capabilities::BlobStorage {
        self.client_for_namespace(Some(namespace))
            .map_or(crate::capabilities::BlobStorage::NONE, |client| {
                client.blob_storage(namespace)
            })
    }

    async fn put_blob(
        &self,
        namespace: &str,
        sha256: &str,
        bytes: &[u8],
    ) -> Result<(), TurbopufferError> {
        self.client_for_namespace(Some(namespace))?
            .put_blob(namespace, sha256, bytes)
            .await
    }

    async fn get_blob(
        &self,
        namespace: &str,
        sha256: &str,
    ) -> Result<Option<Vec<u8>>, TurbopufferError> {
        self.client_for_namespace(Some(namespace))?
            .get_blob(namespace, sha256)
            .await
    }

    async fn embedding_profiles(&self, namespace: &str) -> Result<Option<Value>, TurbopufferError> {
        self.client_for_namespace(Some(namespace))?
            .embedding_profiles(namespace)
            .await
    }

    async fn passthrough(
        &self,
        method: &str,
        path: &str,
        query: Option<&str>,
        body: Option<Value>,
    ) -> Result<TurbopufferPassthroughResponse, TurbopufferError> {
        let namespace = namespace_from_path(path)
            .map(|name| percent_encoding::percent_decode_str(name).decode_utf8_lossy());
        self.client_for_namespace(namespace.as_deref())?
            .passthrough(method, path, query, body)
            .await
    }

    async fn delete_namespace(
        &self,
        namespace: &str,
    ) -> Result<TurbopufferPassthroughResponse, TurbopufferError> {
        self.client_for_namespace(Some(namespace))?
            .delete_namespace(namespace)
            .await
    }

    async fn hint_cache_warm(&self, namespace: &str) -> Result<(), TurbopufferError> {
        self.client_for_namespace(Some(namespace))?
            .hint_cache_warm(namespace)
            .await
    }

    async fn upsert(
        &self,
        namespace: &str,
        docs: &[UpsertDoc],
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        self.client_for_namespace(Some(namespace))?
            .upsert(namespace, docs)
            .await
    }

    async fn patch(
        &self,
        namespace: &str,
        docs: &[PatchDoc],
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        self.client_for_namespace(Some(namespace))?
            .patch(namespace, docs)
            .await
    }

    async fn patch_columns(
        &self,
        namespace: &str,
        columns: &PatchColumns,
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        self.client_for_namespace(Some(namespace))?
            .patch_columns(namespace, columns)
            .await
    }

    async fn delete(
        &self,
        namespace: &str,
        ids: &[String],
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        self.client_for_namespace(Some(namespace))?
            .delete(namespace, ids)
            .await
    }

    async fn delete_by_filter(
        &self,
        namespace: &str,
        filters: &Value,
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        self.client_for_namespace(Some(namespace))?
            .delete_by_filter(namespace, filters)
            .await
    }

    async fn import_arrow(
        &self,
        namespace: &str,
        content_type: &str,
        body: Vec<u8>,
    ) -> Result<TurbopufferPassthroughResponse, TurbopufferError> {
        self.client_for_namespace(Some(namespace))?
            .import_arrow(namespace, content_type, body)
            .await
    }

    async fn query(
        &self,
        namespace: &str,
        vector: &[f64],
        top_k: u32,
        filters: Option<&Value>,
        include_attributes: Option<&IncludeAttributes>,
    ) -> Result<TurbopufferQueryOutcome, TurbopufferError> {
        self.client_for_namespace(Some(namespace))?
            .query(namespace, vector, top_k, filters, include_attributes)
            .await
    }

    async fn ranked_query(
        &self,
        namespace: &str,
        rank_by: &Value,
        top_k: u32,
        filters: Option<&Value>,
        include_attributes: Option<&IncludeAttributes>,
    ) -> Result<TurbopufferQueryOutcome, TurbopufferError> {
        self.client_for_namespace(Some(namespace))?
            .ranked_query(namespace, rank_by, top_k, filters, include_attributes)
            .await
    }

    async fn multi_ranked_query(
        &self,
        namespace: &str,
        legs: &[Value],
        rerank_by: Option<&Value>,
    ) -> Result<Value, TurbopufferError> {
        self.client_for_namespace(Some(namespace))?
            .multi_ranked_query(namespace, legs, rerank_by)
            .await
    }

    async fn fetch(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<DocumentResponse>, TurbopufferError> {
        self.client_for_namespace(Some(namespace))?
            .fetch(namespace, id)
            .await
    }

    async fn fetch_many(
        &self,
        namespace: &str,
        ids: &[String],
    ) -> Result<HashMap<String, DocumentResponse>, TurbopufferError> {
        self.client_for_namespace(Some(namespace))?
            .fetch_many(namespace, ids)
            .await
    }

    async fn fetch_siblings(
        &self,
        namespace: &str,
        parent: &str,
    ) -> Result<Vec<DocumentResponse>, TurbopufferError> {
        self.client_for_namespace(Some(namespace))?
            .fetch_siblings(namespace, parent)
            .await
    }

    async fn fetch_vector(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<Vec<f64>>, TurbopufferError> {
        self.client_for_namespace(Some(namespace))?
            .fetch_vector(namespace, id)
            .await
    }

    async fn scan_page(
        &self,
        namespace: &str,
        cursor: Option<&str>,
        page_size: u32,
        filters: Option<&Value>,
        include_attributes: Option<&[String]>,
    ) -> Result<DocumentPage, TurbopufferError> {
        self.client_for_namespace(Some(namespace))?
            .scan_page(namespace, cursor, page_size, filters, include_attributes)
            .await
    }

    async fn reconcile_change_token(
        &self,
        namespace: &str,
    ) -> Result<Option<String>, TurbopufferError> {
        self.client_for_namespace(Some(namespace))?
            .reconcile_change_token(namespace)
            .await
    }

    async fn scan_page_strong(
        &self,
        namespace: &str,
        cursor: Option<&str>,
        page_size: u32,
        filters: Option<&Value>,
        include_attributes: Option<&[String]>,
    ) -> Result<DocumentPage, TurbopufferError> {
        self.client_for_namespace(Some(namespace))?
            .scan_page_strong(namespace, cursor, page_size, filters, include_attributes)
            .await
    }

    async fn head_namespace(&self, namespace: &str) -> Result<NamespaceMeta, TurbopufferError> {
        self.client_for_namespace(Some(namespace))?
            .head_namespace(namespace)
            .await
    }
}

fn namespace_from_path(path: &str) -> Option<&str> {
    let mut parts = path.trim_start_matches('/').split('/');
    match (parts.next(), parts.next(), parts.next()) {
        (Some("v1" | "v2"), Some("namespaces"), Some(namespace)) if !namespace.is_empty() => {
            Some(namespace)
        }
        _ => None,
    }
}

// --- Real implementation using reqwest ---

pub struct HttpTurbopufferClient {
    client: reqwest::Client,
    base_url: String,
    api_key: Option<String>,
    canonical_cache: Option<crate::document_cache::CanonicalCache>,
    capture_metadata: bool,
    shared_cache: Option<crate::document_cache::SharedCache>,
    // Non-system attributes produced by native embedding cannot be inferred
    // from a successful write body. Their complete rows require hydration.
    generated_columns: RwLock<HashMap<String, HashMap<String, String>>>,
    id_types: RwLock<HashMap<String, (std::time::Instant, bool)>>,
    scan_vector_dimensions: RwLock<HashMap<String, (std::time::Instant, usize)>>,
}

/// Opaque source provenance: boundary and client cannot be independently supplied.
/// This binds source identity only, not witness/drain approval or a capture receipt.
pub struct HttpCaptureSource {
    client: Arc<HttpTurbopufferClient>,
    boundary: crate::document_cache::CaptureWriterBoundary,
}
impl HttpCaptureSource {
    pub fn client(&self) -> &Arc<HttpTurbopufferClient> {
        &self.client
    }
    pub fn boundary(&self) -> &crate::document_cache::CaptureWriterBoundary {
        &self.boundary
    }
}

impl HttpTurbopufferClient {
    pub fn new(api_key: &str, base_url: &str) -> Self {
        let api_key = api_key.trim();
        let api_key = if api_key.is_empty() {
            None
        } else {
            Some(api_key.to_string())
        };
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(api_key) = api_key.as_deref() {
            headers.insert(
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_str(&format!("Bearer {}", api_key))
                    .expect("invalid API key"),
            );
        }
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .expect("failed to build reqwest client");

        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key,
            canonical_cache: None,
            capture_metadata: false,
            shared_cache: None,
            generated_columns: Default::default(),
            id_types: RwLock::new(HashMap::new()),
            scan_vector_dimensions: Default::default(),
        }
    }

    /// Legacy isolated-fixture cache, without late-provider-commit fencing.
    /// Production composition must not enable this helper. Shared mode clears it.
    pub fn with_document_cache(
        mut self,
        backend: Arc<dyn crate::document_cache::DocumentReadCache>,
        scope: String,
    ) -> Self {
        self.canonical_cache = Some(crate::document_cache::CanonicalCache::new(backend, scope));
        self
    }

    /// Source-only shared mode. Production composition keeps this disabled
    /// until the enforced upstream credential boundary is independently reviewed.
    pub fn with_shared_document_cache(
        mut self,
        mut cache: crate::document_cache::SharedCache,
    ) -> Self {
        cache.bind_provider(&self.base_url, self.api_key.as_deref());
        self.canonical_cache = None;
        self.shared_cache = Some(cache);
        self
    }
    /// Derive provenance from THIS immutable actual endpoint/static-key client.
    /// No public constructor permits a caller-supplied boundary/client pairing.
    /// Missing shared mode or mismatched/unreviewed identity refuses before IO.
    pub fn capture_source(self: &Arc<Self>) -> Result<HttpCaptureSource, TurbopufferError> {
        let cache = self.shared_cache.as_ref().ok_or_else(|| {
            TurbopufferError::Other("capture source unavailable without bound shared cache".into())
        })?;
        let boundary = cache.capture_boundary()?;
        Ok(HttpCaptureSource {
            client: self.clone(),
            boundary,
        })
    }
    fn uncached(&self) -> Self {
        Self {
            client: self.client.clone(),
            base_url: self.base_url.clone(),
            api_key: self.api_key.clone(),
            canonical_cache: None,
            capture_metadata: self.shared_cache.is_some() || self.capture_metadata,
            shared_cache: None,
            generated_columns: Default::default(),
            id_types: Default::default(),
            scan_vector_dimensions: Default::default(),
        }
    }
    async fn shared_session(
        &self,
        namespace: &str,
    ) -> Result<Option<crate::document_cache::CacheSession>, TurbopufferError> {
        match &self.shared_cache {
            Some(cache) if cache.certified() && self.api_key.is_some() => {
                cache.acquire(namespace).await.map(Some)
            }
            _ => Ok(None),
        }
    }
    async fn shared_fetch_many(
        &self,
        namespace: &str,
        ids: &[String],
    ) -> Result<HashMap<String, DocumentResponse>, TurbopufferError> {
        let uncached = self.uncached();
        let Some(session) = self.shared_session(namespace).await.unwrap_or(None) else {
            return uncached.fetch_many(namespace, ids).await;
        };
        session
            .protect(async {
                let hits = session.get_many(ids).await.unwrap_or_default();
                let mut seen = HashSet::new();
                let missing: Vec<_> = ids
                    .iter()
                    .filter(|id| !hits.contains_key(*id) && seen.insert((*id).clone()))
                    .cloned()
                    .collect();
                let mut rows: HashMap<_, _> = hits
                    .into_iter()
                    .filter_map(|(id, row)| {
                        row.map(|attributes| (id.clone(), DocumentResponse { id, attributes }))
                    })
                    .collect();
                if !missing.is_empty() {
                    // Reuse only the namespace identity certified by this
                    // retained original fence. Schema/delete epoch changes
                    // remove it; missing/error identity still reads metadata
                    // strongly through the provider below.
                    let identity = if session.fence.version().readable {
                        session.schema().await.unwrap_or(None)
                    } else {
                        None
                    };
                    if let Some(identity) = identity {
                        uncached
                            .generated_columns
                            .write()
                            .unwrap()
                            .insert(namespace.to_owned(), identity.generated);
                        uncached.id_types.write().unwrap().insert(
                            namespace.to_owned(),
                            (std::time::Instant::now(), identity.integer_ids),
                        );
                    }
                    let fetched = uncached.fetch_many(namespace, &missing).await?;
                    let schema = uncached
                        .generated_columns
                        .read()
                        .unwrap()
                        .get(namespace)
                        .cloned();
                    let integer = uncached
                        .id_types
                        .read()
                        .unwrap()
                        .get(namespace)
                        .map(|(_, integer)| *integer);
                    if let (Some(generated), Some(integer_ids)) = (schema, integer) {
                        let _ = session
                            .put_schema(&crate::document_cache::NamespaceIdentity {
                                integer_ids,
                                generated,
                            })
                            .await;
                    }

                    let certificates = missing
                        .iter()
                        .map(|id| {
                            (
                                id.clone(),
                                fetched.get(id).map(|row| row.attributes.clone()),
                            )
                        })
                        .collect();
                    // Read cache outages are soft; a failed publication is a miss.
                    let _ = session.put_many(&certificates, false).await;
                    rows.extend(fetched);
                }
                Ok(rows)
            })
            .await
    }
    async fn shared_fetch_siblings(
        &self,
        namespace: &str,
        parent: &str,
    ) -> Result<Vec<DocumentResponse>, TurbopufferError> {
        let uncached = self.uncached();
        let Some(session) = self.shared_session(namespace).await.unwrap_or(None) else {
            return uncached.fetch_siblings(namespace, parent).await;
        };
        session
            .protect(async {
                if let Ok(Some(ids)) = session.group(parent).await {
                    if let Ok(mut rows) = session.get_many(&ids).await {
                        if rows.len() == ids.len() && rows.values().all(Option::is_some) {
                            return Ok(ids
                                .into_iter()
                                .map(|id| DocumentResponse {
                                    attributes: rows.remove(&id).unwrap().unwrap(),
                                    id,
                                })
                                .collect());
                        }
                    }
                }
                let rows = uncached.fetch_siblings(namespace, parent).await?;
                let certificates = rows
                    .iter()
                    .map(|row| (row.id.clone(), Some(row.attributes.clone())))
                    .collect();
                if session.put_many(&certificates, false).await.is_ok() {
                    let _ = session
                        .put_group(
                            parent,
                            &rows.iter().map(|row| row.id.clone()).collect::<Vec<_>>(),
                        )
                        .await;
                }
                Ok(rows)
            })
            .await
    }

    async fn send_source_write(
        &self,
        namespace: &str,
        body: &Value,
    ) -> Result<reqwest::Response, TurbopufferError> {
        if self.shared_cache.is_some() {
            let Some(mut session) = self.shared_session(namespace).await? else {
                return Err(TurbopufferError::Other(
                    "shared-cache mutation requires a certified static writer boundary".into(),
                ));
            };
            let identity = session.schema().await?;
            let mut normalized = body.clone();
            if canonical_write_effects(body).is_some() && body.get("schema").is_none() {
                let identity=identity.as_ref().ok_or_else(||TurbopufferError::Other("namespace identity uncertain; strong canonical hydration required before typed mutation".into()))?;
                normalize_source_ids(&mut normalized, identity.integer_ids)?;
            }
            let body = &normalized;
            let effects = canonical_write_effects(body);
            let ids = effects
                .as_ref()
                .map(|effects| effects.keys().cloned().collect::<Vec<_>>());
            let mut before = match &ids {
                Some(ids) => session.get_many(ids).await.unwrap_or_default(),
                None => HashMap::new(),
            };
            let generated = if body.get("schema").is_some() {
                None
            } else {
                identity.map(|identity| identity.generated)
            };
            session
                .fence
                .begin_write(if body.get("schema").is_some() {
                    None
                } else {
                    ids.as_deref()
                })
                .await?;
            let url = format!("{}/v2/namespaces/{}", self.base_url, namespace);
            let response = session
                .protect(async {
                    let response = self
                        .authorize(self.client.post(url).json(body))?
                        .send()
                        .await
                        .map_err(|e| TurbopufferError::Other(e.to_string()))?;
                    let status = response.status();
                    let headers = response.headers().clone();
                    // Completion requires the whole response, not just headers.
                    let bytes = response
                        .bytes()
                        .await
                        .map_err(|e| TurbopufferError::Other(e.to_string()))?;
                    let mut buffered = http::Response::builder()
                        .status(status)
                        .body(bytes)
                        .map_err(|e| TurbopufferError::Other(e.to_string()))?;
                    *buffered.headers_mut() = headers;
                    Ok(reqwest::Response::from(buffered))
                })
                .await?;
            if response.status().is_success() {
                let mut after = HashMap::new();
                for (id, effect) in effects.into_iter().flat_map(|effects| effects.into_iter()) {
                    match effect {
                        CanonicalWrite::Delete => {
                            after.insert(id, None);
                        }
                        CanonicalWrite::Patch(attrs) => {
                            if generated.as_ref().is_some_and(|columns| {
                                !columns.values().any(|source| attrs.contains_key(source))
                            }) {
                                if let Some(old) = before.remove(&id) {
                                    after.insert(
                                        id,
                                        old.map(|mut row| {
                                            row.extend(attrs);
                                            row
                                        }),
                                    );
                                }
                            }
                        }
                        CanonicalWrite::Upsert(attrs) => {
                            if generated.as_ref().is_some_and(|columns| {
                                columns.keys().all(|column| attrs.contains_key(column))
                            }) {
                                after.insert(id, Some(attrs));
                            }
                        }
                    }
                }
                // Publication failure leaves pending durable; it cannot be
                // silently healed by this or a later successful mutation.
                if session.put_many(&after, true).await.is_ok() {
                    // Preserve the known provider outcome and its single billing
                    // observer. Cache/coordinator failures leave pending durable.
                    let _ = session.fence.complete_write().await;
                }
            }
            return Ok(response);
        }
        let mut cached = if self.api_key.is_some() {
            match &self.canonical_cache {
                Some(cache) => cache.lock(namespace).await,
                None => None,
            }
        } else {
            None
        };
        let effects = canonical_write_effects(body);
        let mut generated = self
            .generated_columns
            .read()
            .unwrap()
            .get(namespace)
            .cloned();
        if let Some(schema) = body.get("schema") {
            let columns = generated_columns(schema);
            if !columns.is_empty() {
                generated.get_or_insert_with(HashMap::new).extend(columns);
            }
        }

        let mut before = HashMap::new();
        if let (Some(cache), Some(state)) = (&self.canonical_cache, &mut cached) {
            state.groups.clear();
            if let Some(effects) = &effects {
                let ids: Vec<_> = effects.keys().cloned().collect();
                before = cache
                    .backend
                    .get_many(&state.scope, &ids)
                    .await
                    .unwrap_or_default();
                // Invalidate before dispatch, including when the future is canceled
                // after dispatch and we cannot know whether the store committed.
                if cache.backend.invalidate(&state.scope, &ids).await.is_err() {
                    cache.reset(state);
                    before.clear();
                }
            } else {
                cache.reset(state);
            }
        }
        let url = format!("{}/v2/namespaces/{}", self.base_url, namespace);
        let resp = self
            .authorize(self.client.post(&url).json(body))?
            .send()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;
        if resp.status().is_success() {
            if let Some(generated) = generated.clone() {
                self.generated_columns
                    .write()
                    .unwrap()
                    .insert(namespace.to_owned(), generated);
            }
            if let (Some(cache), Some(state), Some(effects)) =
                (&self.canonical_cache, &mut cached, effects)
            {
                let mut after = HashMap::new();
                for (id, effect) in effects {
                    match effect {
                        CanonicalWrite::Delete => {
                            after.insert(id, None);
                        }
                        CanonicalWrite::Patch(attrs) => {
                            if generated.is_none()
                                || generated.as_ref().is_some_and(|columns| {
                                    columns.values().any(|source| attrs.contains_key(source))
                                })
                            {
                                continue;
                            }
                            if let Some(old) = before.remove(&id) {
                                // patch_rows never creates absent documents.
                                after.insert(
                                    id,
                                    old.map(|mut row| {
                                        row.extend(attrs);
                                        row
                                    }),
                                );
                            }
                        }
                        CanonicalWrite::Upsert(attrs) => {
                            // Native upserts replace all non-vector attributes.
                            if generated.as_ref().is_some_and(|columns| {
                                columns.keys().all(|column| attrs.contains_key(column))
                            }) {
                                after.insert(id, Some(attrs));
                            }
                        }
                    }
                }
                if cache.backend.put_many(&state.scope, &after).await.is_err() {
                    cache.reset(state);
                }
            }
        }
        Ok(resp)
    }

    // ID types are immutable for a namespace's lifetime. Bound the cache and
    // expire entries so namespaces recreated outside this client are rechecked.
    // Request-scoped credentials bypass it: namespace names can overlap across
    // upstream accounts, and a cache hit must not bypass their authorization.
    async fn integer_ids(&self, namespace: &str) -> Result<bool, TurbopufferError> {
        const TTL: Duration = Duration::from_secs(60);
        if self.api_key.is_some() {
            if let Some((at, integer)) = self.id_types.read().unwrap().get(namespace) {
                if at.elapsed() < TTL {
                    return Ok(*integer);
                }
            }
        }
        let meta = self.head_namespace(namespace).await?;
        let integer = match meta.raw["schema"]["id"]["type"].as_str() {
            Some("uint") => true,
            Some("string" | "uuid") => false,
            _ => {
                return Err(TurbopufferError::Other(
                    "namespace metadata has no supported id type".to_string(),
                ));
            }
        };
        if self.api_key.is_some() {
            let mut cache = self.id_types.write().unwrap();
            cache.retain(|_, (at, _)| at.elapsed() < TTL);
            if cache.len() >= 4096 {
                cache.clear();
            }
            cache.insert(namespace.to_string(), (std::time::Instant::now(), integer));
        }
        Ok(integer)
    }

    /// Explicit metadata preparation must execute outside the row-page permit,
    /// under its own financial admission. Cache type from THIS actual response;
    /// never accept a caller's separately asserted namespace ID type.
    pub async fn head_namespace_for_strong_scan(
        &self,
        namespace: &str,
    ) -> Result<NamespaceMeta, TurbopufferError> {
        if self.api_key.is_none() || REQUEST_UPSTREAM_API_KEY.try_with(|_| ()).is_ok() {
            return Err(TurbopufferError::Other(
                "prepared scan requires bound static client".into(),
            ));
        }
        // A failed/cancelled explicit refresh must not retain an old vector
        // dimension as current preparation.
        self.scan_vector_dimensions
            .write()
            .unwrap()
            .remove(namespace);
        let meta = self.head_namespace(namespace).await?;
        let integer = match meta.raw["schema"]["id"]["type"].as_str() {
            Some("uint") => true,
            Some("string" | "uuid") => false,
            _ => {
                return Err(TurbopufferError::Other(
                    "namespace metadata has no supported id type".into(),
                ))
            }
        };
        let mut dimensions = self.scan_vector_dimensions.write().unwrap();
        dimensions.retain(|_, (at, _)| at.elapsed() < Duration::from_secs(60));
        dimensions.remove(namespace);
        if dimensions.len() >= 4096 {
            dimensions.clear();
        }
        if let Ok(dimension) = crate::search_profile::vector_dimensions(&meta.raw) {
            dimensions.insert(namespace.into(), (std::time::Instant::now(), dimension));
        }
        drop(dimensions);
        let mut cache = self.id_types.write().unwrap();
        cache.retain(|_, (at, _)| at.elapsed() < Duration::from_secs(60));
        if cache.len() >= 4096 {
            cache.clear();
        }
        cache.insert(namespace.into(), (std::time::Instant::now(), integer));
        Ok(meta)
    }
    fn prepared_vector_dimension(&self, namespace: &str) -> Result<usize, TurbopufferError> {
        if self.api_key.is_none() || REQUEST_UPSTREAM_API_KEY.try_with(|_| ()).is_ok() {
            return Err(TurbopufferError::Other(
                "vector scan requires bound static client".into(),
            ));
        }
        self.scan_vector_dimensions
            .read()
            .unwrap()
            .get(namespace)
            .filter(|(at, _)| at.elapsed() < Duration::from_secs(60))
            .map(|(_, dimension)| *dimension)
            .ok_or_else(|| {
                TurbopufferError::Other(
                    "vector scan source schema must be independently prepared".into(),
                )
            })
    }
    fn prepared_integer_ids(&self, namespace: &str) -> Result<bool, TurbopufferError> {
        if self.api_key.is_none() || REQUEST_UPSTREAM_API_KEY.try_with(|_| ()).is_ok() {
            return Err(TurbopufferError::Other(
                "prepared scan requires bound static client".into(),
            ));
        }
        self.id_types
            .read()
            .unwrap()
            .get(namespace)
            .filter(|(at, _)| at.elapsed() < Duration::from_secs(60))
            .map(|(_, integer)| *integer)
            .ok_or_else(|| {
                TurbopufferError::Other(
                    "scan metadata must be independently admitted and prepared".into(),
                )
            })
    }
    fn wire_id(id: &str, integer: bool) -> Option<Value> {
        if integer {
            id.parse::<u64>().ok().map(Value::from)
        } else {
            Some(Value::String(id.to_string()))
        }
    }

    fn authorize(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder, TurbopufferError> {
        if self.api_key.is_some() {
            return Ok(request);
        }
        let api_key = REQUEST_UPSTREAM_API_KEY
            .try_with(Clone::clone)
            .map_err(|_| {
                TurbopufferError::Other(
                    "deriveFromStore requires Authorization: Bearer for this request".to_string(),
                )
            })?;
        Ok(request.bearer_auth(api_key))
    }
}

fn normalize_source_ids(body: &mut Value, integer: bool) -> Result<(), TurbopufferError> {
    fn normalize(id: &mut Value, integer: bool) -> Result<(), TurbopufferError> {
        if integer {
            let number = match id {
                Value::Number(number) => number.as_u64(),
                Value::String(text) => text
                    .parse::<u64>()
                    .ok()
                    .filter(|number| number.to_string() == *text),
                _ => None,
            }
            .ok_or_else(|| TurbopufferError::Other("noncanonical uint document id".into()))?;
            *id = Value::from(number);
        } else if !id.is_string() {
            return Err(TurbopufferError::Other(
                "string namespace requires string document ids".into(),
            ));
        }
        Ok(())
    }
    for key in ["upsert_rows", "patch_rows"] {
        if let Some(rows) = body.get_mut(key).and_then(Value::as_array_mut) {
            for row in rows {
                if let Some(id) = row.get_mut("id") {
                    normalize(id, integer)?;
                }
            }
        }
    }
    for key in ["upsert_columns", "patch_columns"] {
        if let Some(ids) = body
            .get_mut(key)
            .and_then(|columns| columns.get_mut("id"))
            .and_then(Value::as_array_mut)
        {
            for id in ids {
                normalize(id, integer)?;
            }
        }
    }
    if let Some(ids) = body.get_mut("deletes").and_then(Value::as_array_mut) {
        for id in ids {
            normalize(id, integer)?;
        }
    }
    Ok(())
}

fn generated_columns(schema: &Value) -> HashMap<String, String> {
    schema
        .as_object()
        .into_iter()
        .flat_map(|schema| schema.iter())
        .filter_map(|(name, definition)| {
            if is_system_column(name) {
                return None;
            }
            definition
                .get("embed")
                .filter(|value| !value.is_null())
                .map(|embed| {
                    (
                        name.clone(),
                        embed
                            .get("source_attribute")
                            .and_then(Value::as_str)
                            .unwrap_or(name)
                            .to_owned(),
                    )
                })
        })
        .collect()
}
enum CanonicalWrite {
    Upsert(HashMap<String, Value>),
    Patch(HashMap<String, Value>),
    Delete,
}
fn canonical_write_effects(body: &Value) -> Option<HashMap<String, CanonicalWrite>> {
    let object = body.as_object()?;
    if object.keys().any(|key| {
        !matches!(
            key.as_str(),
            "upsert_rows"
                | "upsert_columns"
                | "patch_rows"
                | "patch_columns"
                | "deletes"
                | "schema"
                | "distance_metric"
                | "return_affected_ids"
        )
    }) {
        return None;
    }
    let mut effects = HashMap::new();
    for (key, patch) in [("upsert_rows", false), ("patch_rows", true)] {
        if let Some(rows) = object.get(key) {
            for row in rows.as_array()? {
                let row = row.as_object()?;
                let (id, _) = id_from_wire(row.get("id")?)?;
                let attrs = row
                    .iter()
                    .filter(|(key, _)| !is_system_column(key))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                let effect = if patch {
                    CanonicalWrite::Patch(attrs)
                } else {
                    CanonicalWrite::Upsert(attrs)
                };
                if effects.insert(id, effect).is_some() {
                    return None;
                }
            }
        }
    }
    for (key, patch) in [("upsert_columns", false), ("patch_columns", true)] {
        if let Some(columns) = object.get(key) {
            let columns = columns.as_object()?;
            let ids = columns.get("id")?.as_array()?;
            for (i, id) in ids.iter().enumerate() {
                let (id, _) = id_from_wire(id)?;
                let mut attrs = HashMap::new();
                for (key, values) in columns {
                    if !is_system_column(key) {
                        let values = values.as_array()?;
                        if values.len() != ids.len() {
                            return None;
                        }
                        attrs.insert(key.clone(), values[i].clone());
                    }
                }
                let effect = if patch {
                    CanonicalWrite::Patch(attrs)
                } else {
                    CanonicalWrite::Upsert(attrs)
                };
                if effects.insert(id, effect).is_some() {
                    return None;
                }
            }
        }
    }
    if let Some(ids) = object.get("deletes") {
        for id in ids.as_array()? {
            let (id, _) = id_from_wire(id)?;
            if effects.insert(id, CanonicalWrite::Delete).is_some() {
                return None;
            }
        }
    }
    Some(effects)
}

fn is_system_column(key: &str) -> bool {
    matches!(key, "id" | "$dist" | "$score" | "vector")
}

fn billing_from_body(body: &Value) -> Option<Value> {
    body.get("billing")
        .filter(|billing| !billing.is_null())
        .cloned()
}

fn rows_from_query_body(resp_body: &Value) -> Vec<QueryResult> {
    resp_body
        .get("rows")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|row| {
            let (id, numeric_id) = id_from_wire(row.get("id")?)?;
            let dist = row
                .get("$dist")
                .and_then(|v| v.as_f64())
                .or_else(|| row.get("$score").and_then(|v| v.as_f64()));
            let mut attributes = HashMap::new();
            if let Some(obj) = row.as_object() {
                for (k, v) in obj {
                    if !is_system_column(k) {
                        attributes.insert(k.clone(), v.clone());
                    }
                }
            }
            Some(QueryResult {
                id,
                numeric_id,
                dist,
                attributes,
            })
        })
        .collect()
}

impl HttpTurbopufferClient {
    async fn scan_page_at_consistency(
        &self,
        namespace: &str,
        cursor: Option<&str>,
        page_size: u32,
        filters: Option<&Value>,
        include_attributes: Option<&[String]>,
        strong: bool,
    ) -> Result<DocumentPage, TurbopufferError> {
        if strong && !(1..=10_000).contains(&page_size) {
            return Err(TurbopufferError::Other(
                "invalid strong scan page size".into(),
            ));
        }
        self.capabilities()
            .require(crate::capabilities::WireFeature::OrderedScan)?;
        // Reserved payload is opt-in, never an implicit all-attributes expansion.
        // Bind dimension to independently admitted metadata on this actual client;
        // neither a prior row nor a caller's default is schema evidence.
        let vector_dimension = if strong
            && include_attributes.is_some_and(|fields| fields.iter().any(|f| f == "vector"))
        {
            if include_attributes.is_some_and(|fields| fields.len() > 64) {
                return Err(TurbopufferError::Other(
                    "vector projection exceeds 64 fields".into(),
                ));
            }
            Some(self.prepared_vector_dimension(namespace)?)
        } else {
            None
        };
        // Bind every strong response to the namespace's ID type, including
        // the first page. A string cursor alone cannot detect a type switch
        // between pages. This HEAD/type cache is not a content revision proof.
        let integer_ids = if strong && PREPARED_SCAN_METADATA_ONLY.try_with(|_| ()).is_ok() {
            Some(self.prepared_integer_ids(namespace)?)
        } else if strong || cursor.is_some() {
            Some(self.integer_ids(namespace).await?)
        } else {
            None
        };
        let cursor_filter = if let Some(cursor) = cursor {
            let id = Self::wire_id(cursor, integer_ids.unwrap()).ok_or_else(|| {
                TurbopufferError::Other("invalid integer id scan cursor".to_string())
            })?;
            Some(serde_json::json!(["id", "Gt", id]))
        } else {
            None
        };

        let combined_filter = match (cursor_filter, filters) {
            (Some(cf), Some(uf)) => Some(serde_json::json!(["And", [cf, uf.clone()]])),
            (Some(cf), None) => Some(cf),
            (None, Some(uf)) => Some(uf.clone()),
            (None, None) => None,
        };

        let query_top_k = if strong {
            page_size
        } else {
            page_size.saturating_add(1).min(10_000)
        };
        let mut body = serde_json::json!({
            "rank_by": ["id", "asc"],
            "top_k": query_top_k,
            "include_attributes": true,
            "consistency": {"level": if strong { "strong" } else { "eventual" }},
        });
        if let Some(f) = combined_filter {
            body["filters"] = f;
        }
        if let Some(attrs) = include_attributes {
            body["include_attributes"] =
                serde_json::to_value(attrs).map_err(|e| TurbopufferError::Other(e.to_string()))?;
        }

        reserve_provider_query(namespace, 1).await?;
        let url = format!("{}/v2/namespaces/{}/query", self.base_url, namespace);
        let resp_body = receipts::read_json(
            namespace,
            receipts::ReadKind::Scan,
            self.authorize(self.client.post(&url).json(&body))?,
        )
        .await?;

        observe_projected_billing(namespace, &resp_body);

        if strong {
            validate_strong_scan_rows(&resp_body, cursor, query_top_k, integer_ids.unwrap())?;
            if let Some(dimension) = vector_dimension {
                if self.prepared_vector_dimension(namespace)? != dimension {
                    return Err(TurbopufferError::Other(
                        "vector schema changed during scan".into(),
                    ));
                }
                for row in resp_body["rows"].as_array().expect("validated rows") {
                    validate_scan_vector(row.get("vector"), dimension)?;
                }
            }
        }

        let rows = resp_body
            .get("rows")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let mut documents: Vec<DocumentResponse> = rows
            .into_iter()
            .filter_map(|row| {
                let (id, _) = id_from_wire(row.get("id")?)?;
                let mut attributes = HashMap::new();
                if let Some(obj) = row.as_object() {
                    for (k, v) in obj {
                        if !is_system_column(k) || (k == "vector" && vector_dimension.is_some()) {
                            attributes.insert(k.clone(), v.clone());
                        }
                    }
                }
                Some(DocumentResponse { id, attributes })
            })
            .collect();

        // Ask for one extra row when possible. At Turbopuffer's 10k top_k cap,
        // an exact full page schedules one confirming follow-up request.
        let page_size = page_size as usize;
        let next_cursor = if documents.len() > page_size {
            documents.truncate(page_size);
            documents.last().map(|d| d.id.clone())
        } else if query_top_k == page_size as u32 && documents.len() == page_size {
            documents.last().map(|d| d.id.clone())
        } else {
            None
        };

        Ok(DocumentPage {
            documents,
            next_cursor,
        })
    }
}

/// An explicitly projected vector must be a complete, schema-sized numeric
/// payload. Missing/null vectors refuse rather than certifying incomplete search.
fn validate_scan_vector(value: Option<&Value>, dimension: usize) -> Result<(), TurbopufferError> {
    let valid = value.and_then(Value::as_array).is_some_and(|values| {
        dimension > 0
            && values.len() == dimension
            && values
                .iter()
                .all(|v| v.as_f64().is_some_and(f64::is_finite))
    });
    if valid {
        Ok(())
    } else {
        Err(TurbopufferError::Other(
            "invalid source-schema vector payload".into(),
        ))
    }
}

/// A malformed, omitted or non-advancing row cannot certify complete capture.
/// Preserve integer ordering instead of comparing decimal text.
fn validate_strong_scan_rows(
    body: &Value,
    cursor: Option<&str>,
    top_k: u32,
    expected_integer: bool,
) -> Result<(), TurbopufferError> {
    let invalid = || TurbopufferError::Other("invalid/non-advancing strong scan response".into());
    let rows = body
        .get("rows")
        .and_then(Value::as_array)
        .ok_or_else(invalid)?;
    if body.get("error").is_some_and(|v| !v.is_null()) || rows.len() > top_k as usize {
        return Err(invalid());
    }
    let mut previous: Option<(String, bool)> = None;
    for row in rows {
        let (id, integer) = row.get("id").and_then(id_from_wire).ok_or_else(invalid)?;
        if id.is_empty() || !row.is_object() || integer != expected_integer {
            return Err(invalid());
        }
        let prior = previous
            .as_ref()
            .map(|(id, kind)| (id.as_str(), *kind))
            .or_else(|| cursor.map(|id| (id, integer)));
        if let Some((prior_id, prior_integer)) = prior {
            if prior_integer != integer {
                return Err(invalid());
            }
            let advancing = if integer {
                id.parse::<u64>().map_err(|_| invalid())?
                    > prior_id.parse::<u64>().map_err(|_| invalid())?
            } else {
                id.as_str() > prior_id
            };
            if !advancing {
                return Err(invalid());
            }
        }
        previous = Some((id, integer));
    }
    Ok(())
}

#[cfg(test)]
mod strong_scan_contract_tests {
    use super::*;
    #[test]
    fn numeric_and_string_ordering_is_strict_and_malformed_rows_refuse() {
        assert!(validate_strong_scan_rows(
            &serde_json::json!({"rows":[{"id":9},{"id":10}]}),
            Some("8"),
            3,
            true
        )
        .is_ok());
        assert!(validate_strong_scan_rows(
            &serde_json::json!({"rows":[{"id":"a"},{"id":"b"}]}),
            None,
            3,
            false
        )
        .is_ok());
        for body in [
            serde_json::json!({}),
            serde_json::json!({"rows":[{}]}),
            serde_json::json!({"rows":[{"id":"a"},{"id":"a"}]}),
            serde_json::json!({"rows":[{"id":9},{"id":"10"}]}),
            serde_json::json!({"rows":[{"id":-1}]}),
            serde_json::json!({"rows":[{"id":1.5}]}),
            serde_json::json!({"rows":[],"error":"partial result"}),
        ] {
            assert!(validate_strong_scan_rows(&body, None, 3, false).is_err());
        }
        assert!(validate_strong_scan_rows(
            &serde_json::json!({"rows":[{"id":10},{"id":9}]}),
            None,
            3,
            true
        )
        .is_err());
        assert!(validate_strong_scan_rows(
            &serde_json::json!({"rows":[{"id":"b"}]}),
            Some("b"),
            3,
            false
        )
        .is_err());
        assert!(validate_strong_scan_rows(
            &serde_json::json!({"rows":[{"id":"a"},{"id":"b"}]}),
            None,
            1,
            false
        )
        .is_err());
        // Never infer a new ID type from the next row after a string cursor.
        assert!(validate_strong_scan_rows(
            &serde_json::json!({"rows":[{"id":10}]}),
            Some("9"),
            3,
            false
        )
        .is_err());
        assert!(validate_strong_scan_rows(
            &serde_json::json!({"rows":[{"id":"10"}]}),
            Some("9"),
            3,
            true
        )
        .is_err());
        assert!(
            validate_strong_scan_rows(&serde_json::json!({"rows":[]}), Some("b"), 3, false).is_ok()
        );
    }

    #[test]
    fn projected_vectors_require_finite_complete_schema_dimensions() {
        assert!(validate_scan_vector(Some(&serde_json::json!([1.0, -2.0])), 2).is_ok());
        for value in [
            Value::Null,
            serde_json::json!([]),
            serde_json::json!([1]),
            serde_json::json!([1, 2, 3]),
            serde_json::json!([1, "2"]),
            serde_json::json!([1, null]),
            serde_json::json!({"vector":[1, 2]}),
        ] {
            assert!(validate_scan_vector(Some(&value), 2).is_err());
        }
        assert!(validate_scan_vector(None, 2).is_err());
        assert!(validate_scan_vector(Some(&serde_json::json!([])), 0).is_err());
        // serde_json cannot construct a nonfinite Number; overflowing numeric
        // JSON is rejected before validation rather than coerced to a vector.
        assert!(serde_json::from_str::<Value>("[1,1e999]").is_err());
    }

    #[tokio::test]
    async fn strong_vector_projection_preserves_payload_and_meters_invalid_pages() {
        use axum::{
            routing::{get, post},
            Json, Router,
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let app = Router::new()
            .route("/v2/namespaces/ns/metadata", get(|| async {
                Json(serde_json::json!({"schema":{"id":{"type":"string"},"vector":{"type":"[2]f32"}}}))
            }))
            .route("/v2/namespaces/ns/query", post(move |Json(body): Json<Value>| {
                let calls = counted.clone();
                async move {
                    assert_eq!(body["consistency"]["level"], "strong");
                    let index = calls.fetch_add(1, AtomicOrdering::SeqCst);
                    let vector = match index {
                        0..=2 => serde_json::json!([1.25, -2.5]),
                        3 => serde_json::json!([1.0]),
                        _ => serde_json::json!([1.0, "bad"]),
                    };
                    if index == 0 { assert_eq!(body["include_attributes"], serde_json::json!(["vector"])); }
                    if index == 1 { assert_eq!(body["include_attributes"], true); }
                    if index == 2 { assert_eq!(body["include_attributes"], serde_json::json!(["text"])); }
                    Json(serde_json::json!({"rows":[{"id":"a","text":"retained","vector":vector}],
                        "billing":{"billable_logical_bytes_queried":42}}))
                }
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = HttpTurbopufferClient::new("fixture", &format!("http://{address}"));
        let projection = ["vector".into()];
        let oversized = vec!["vector".into(); 65];
        let error = client
            .scan_page_strong("ns", None, 2, None, Some(&oversized))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("exceeds 64 fields"));
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 0);
        assert!(client
            .scan_page_strong("ns", None, 2, None, Some(&projection))
            .await
            .is_err());
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 0);
        client.head_namespace_for_strong_scan("ns").await.unwrap();
        let metered = Arc::new(AtomicUsize::new(0));
        let counted = metered.clone();
        let observer: ReadBillingObserver = Arc::new(move |_, body| {
            assert_eq!(body["billable_logical_bytes_queried"], 42);
            counted.fetch_add(1, AtomicOrdering::SeqCst);
        });
        let page = scope_read_billing(
            observer.clone(),
            client.scan_page_strong("ns", None, 2, None, Some(&projection)),
        )
        .await
        .unwrap();
        assert_eq!(
            page.documents[0].attributes["vector"],
            serde_json::json!([1.25, -2.5])
        );
        assert!(page.next_cursor.is_none());
        for fields in [None, Some(vec!["text".into()])] {
            let page = scope_read_billing(
                observer.clone(),
                client.scan_page_strong("ns", None, 2, None, fields.as_deref()),
            )
            .await
            .unwrap();
            assert!(!page.documents[0].attributes.contains_key("vector"));
        }
        for _ in 0..2 {
            assert!(scope_read_billing(
                observer.clone(),
                client.scan_page_strong("ns", None, 2, None, Some(&projection))
            )
            .await
            .is_err());
        }
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 5);
        assert_eq!(metered.load(AtomicOrdering::SeqCst), 5);
        client.scan_vector_dimensions.write().unwrap().insert(
            "ns".into(),
            (std::time::Instant::now() - Duration::from_secs(61), 2),
        );
        assert!(client
            .scan_page_strong("ns", None, 2, None, Some(&projection))
            .await
            .is_err());
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 5);
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn vector_schema_refresh_during_physical_page_refuses_after_billing() {
        use axum::{
            routing::{get, post},
            Json, Router,
        };
        let dimensions = Arc::new(AtomicUsize::new(2));
        let read_dimensions = dimensions.clone();
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let query_entered = entered.clone();
        let query_release = release.clone();
        let app = Router::new()
            .route("/v2/namespaces/ns/metadata", get(move || {
                let dimensions = read_dimensions.clone();
                async move { Json(serde_json::json!({"schema":{"id":{"type":"string"},
                    "vector":{"type":format!("[{}]f32", dimensions.load(AtomicOrdering::SeqCst))}}})) }
            }))
            .route("/v2/namespaces/ns/query", post(move || {
                let entered = query_entered.clone(); let release = query_release.clone();
                async move {
                    entered.notify_one(); release.notified().await;
                    Json(serde_json::json!({"rows":[{"id":"a","vector":[1.0,2.0]}],
                        "billing":{"billable_logical_bytes_queried":42}}))
                }
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = Arc::new(HttpTurbopufferClient::new(
            "fixture",
            &format!("http://{address}"),
        ));
        client.head_namespace_for_strong_scan("ns").await.unwrap();
        let billed = Arc::new(AtomicUsize::new(0));
        let observed = billed.clone();
        let observer: ReadBillingObserver = Arc::new(move |_, _| {
            observed.fetch_add(1, AtomicOrdering::SeqCst);
        });
        let scanning = client.clone();
        let page = tokio::spawn(async move {
            scope_read_billing(
                observer,
                scanning.scan_page_strong("ns", None, 2, None, Some(&["vector".into()])),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        dimensions.store(3, AtomicOrdering::SeqCst);
        client.head_namespace_for_strong_scan("ns").await.unwrap();
        release.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(5), page)
            .await
            .unwrap()
            .unwrap();
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("vector schema changed"));
        assert_eq!(billed.load(AtomicOrdering::SeqCst), 1);
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn nested_scan_billing_preserves_parent_and_consumer_receipts() {
        let parent = Arc::new(AtomicUsize::new(0));
        let child = Arc::new(AtomicUsize::new(0));
        let p = parent.clone();
        let c = child.clone();
        let outer: ReadBillingObserver = Arc::new(move |_, _| {
            p.fetch_add(1, AtomicOrdering::SeqCst);
        });
        let inner: ReadBillingObserver = Arc::new(move |_, _| {
            c.fetch_add(1, AtomicOrdering::SeqCst);
        });
        scope_read_billing(outer, scope_read_billing(inner, async {
            observe_projected_billing("ns", &serde_json::json!({"billing":{"billable_logical_bytes_queried":1,"billable_logical_bytes_returned":2}}));
        })).await;
        assert_eq!(parent.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(child.load(AtomicOrdering::SeqCst), 1);
    }

    #[tokio::test]
    async fn strong_http_pages_exhaust_and_meter_without_eventual_fallback() {
        use axum::{
            routing::{get, post},
            Json, Router,
        };
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = calls.clone();
        let metadata_calls = Arc::new(AtomicUsize::new(0));
        let head_calls = metadata_calls.clone();
        let app = Router::new().route("/v2/namespaces/sessions/query", post(move |Json(body): Json<Value>| {
            let calls = observed.clone();
            async move {
                assert_eq!(body["consistency"]["level"], "strong");
                assert_eq!(body["top_k"], 2);
                assert_eq!(body["rank_by"], serde_json::json!(["id", "asc"]));
                assert_eq!(body["include_attributes"], serde_json::json!(["session_id", "end"]));
                let index = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let rows = if index == 0 {
                    assert!(body.get("filters").is_none());
                    serde_json::json!([{"id":"a","session_id":"s","end":1},{"id":"b","session_id":"t","end":2}])
                } else {
                    assert_eq!(index, 1);
                    assert_eq!(body["filters"], serde_json::json!(["id","Gt","b"]));
                    serde_json::json!([])
                };
                Json(serde_json::json!({"rows":rows,"billing":{"billable_logical_bytes_queried":42}}))
            }
        }));
        let app = app.route(
            "/v2/namespaces/sessions/metadata",
            get(move || {
                let calls = head_calls.clone();
                async move {
                    calls.fetch_add(1, AtomicOrdering::SeqCst);
                    Json(serde_json::json!({"schema":{"id":{"type":"string"}}}))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = HttpTurbopufferClient::new("fixture", &format!("http://{addr}"));
        // The budgeted scope cannot acquire metadata as a page side effect.
        assert!(scope_prepared_scan_metadata(client.scan_page_strong(
            "sessions",
            None,
            2,
            None,
            Some(&["session_id".into()])
        ))
        .await
        .is_err());
        assert_eq!(metadata_calls.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 0);
        client
            .head_namespace_for_strong_scan("sessions")
            .await
            .unwrap();
        assert_eq!(metadata_calls.load(AtomicOrdering::SeqCst), 1);
        let metered = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = metered.clone();
        let observer: ReadBillingObserver = std::sync::Arc::new(move |namespace, body| {
            assert_eq!(namespace, "sessions");
            assert_eq!(body["billable_logical_bytes_queried"], 42);
            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        });
        // Budget admission runs before physical dispatch, even for this
        // direct strong scanner rather than the ordinary query interface.
        let denied: QueryPermit = std::sync::Arc::new(|namespace, queries| {
            assert_eq!(namespace, "sessions");
            assert_eq!(queries, 1);
            Box::pin(async { Err(TurbopufferError::Other("fixture budget exhausted".into())) })
        });
        let refused = scope_query_permit(
            denied,
            client.scan_page_strong("sessions", None, 2, None, Some(&["session_id".into()])),
        )
        .await;
        assert!(refused.is_err());
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(metered.load(std::sync::atomic::Ordering::SeqCst), 0);
        let page = scope_prepared_scan_metadata(scope_read_billing(
            observer.clone(),
            client.scan_page_strong(
                "sessions",
                None,
                2,
                None,
                Some(&["session_id".into(), "end".into()]),
            ),
        ))
        .await
        .unwrap();
        assert_eq!(page.documents.len(), 2);
        assert_eq!(page.next_cursor.as_deref(), Some("b"));
        let confirmed = scope_prepared_scan_metadata(scope_read_billing(
            observer,
            client.scan_page_strong(
                "sessions",
                page.next_cursor.as_deref(),
                2,
                None,
                Some(&["session_id".into(), "end".into()]),
            ),
        ))
        .await
        .unwrap();
        assert!(confirmed.documents.is_empty());
        assert!(confirmed.next_cursor.is_none());
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(metered.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(metadata_calls.load(AtomicOrdering::SeqCst), 1);
        client.id_types.write().unwrap().clear();
        assert!(scope_prepared_scan_metadata(client.scan_page_strong(
            "sessions",
            None,
            2,
            None,
            Some(&["session_id".into()])
        ))
        .await
        .is_err());
        assert_eq!(metadata_calls.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 2);
        server.abort();
        let _ = server.await;
    }
}

#[async_trait]
impl TurbopufferClient for HttpTurbopufferClient {
    fn capabilities(&self) -> crate::capabilities::Capabilities {
        TURBOPUFFER_CAPABILITIES
    }

    fn blob_storage(&self, namespace: &str) -> crate::capabilities::BlobStorage {
        turbopuffer_blob_storage(namespace)
    }

    async fn put_blob(
        &self,
        namespace: &str,
        sha256: &str,
        bytes: &[u8],
    ) -> Result<(), TurbopufferError> {
        let body = serde_json::json!({
            "upsert_rows": [{"id": sha256, BLOB_DATA_ATTRIBUTE: encode_blob_value(bytes)}],
            "schema": {BLOB_DATA_ATTRIBUTE: {"type": "bytes"}},
        });
        let url = format!(
            "{}/v2/namespaces/{}",
            self.base_url,
            blob_set_namespace(namespace)
        );
        let resp = self
            .authorize(self.client.post(&url).json(&body))?
            .send()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(TurbopufferError::from_response(resp).await);
        }
        if let Ok(body) = resp.json::<Value>().await {
            observe_projected_billing(&blob_set_namespace(namespace), &body);
        }
        Ok(())
    }

    async fn get_blob(
        &self,
        namespace: &str,
        sha256: &str,
    ) -> Result<Option<Vec<u8>>, TurbopufferError> {
        // Strong consistency (the default): a GET right after a PUT sees it.
        let body = serde_json::json!({
            "rank_by": ["id", "asc"],
            "top_k": 1,
            "filters": ["id", "Eq", sha256],
            "include_attributes": [BLOB_DATA_ATTRIBUTE],
        });
        reserve_provider_query(namespace, 1).await?;
        let url = format!(
            "{}/v2/namespaces/{}/query",
            self.base_url,
            blob_set_namespace(namespace)
        );
        let request = self.authorize(self.client.post(&url).json(&body))?;
        let mut receipt =
            receipts::Pending::new(&blob_set_namespace(namespace), receipts::ReadKind::Fetch);
        let resp = receipt.send(request).await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            // No blob-set namespace yet: nothing was ever stored here.
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(TurbopufferError::from_response(resp).await);
        }
        let resp_body: Value = resp
            .json()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;
        receipt.billing(&resp_body);
        receipt.finished_json_success();
        observe_projected_billing(&blob_set_namespace(namespace), &resp_body);
        resp_body
            .get("rows")
            .and_then(Value::as_array)
            .and_then(|rows| {
                rows.iter()
                    .find(|row| row.get("id").and_then(Value::as_str) == Some(sha256))
            })
            .map(|row| decode_blob_value(row.get(BLOB_DATA_ATTRIBUTE).unwrap_or(&Value::Null)))
            .transpose()
    }

    async fn passthrough(
        &self,
        method: &str,
        path: &str,
        query: Option<&str>,
        body: Option<Value>,
    ) -> Result<TurbopufferPassthroughResponse, TurbopufferError> {
        if matches!(method, "POST" | "PATCH" | "DELETE")
            && !path.ends_with("/query")
            && self
                .shared_cache
                .as_ref()
                .is_some_and(|cache| !cache.certified() || self.api_key.is_none())
        {
            return Err(TurbopufferError::Other(
                "unverified shared-cache writer cannot dispatch mutations".into(),
            ));
        }
        if method == "DELETE" {
            if let Some(namespace) = path.strip_prefix("/v2/namespaces/") {
                self.generated_columns.write().unwrap().remove(namespace);
                self.id_types.write().unwrap().remove(namespace);
            }
        }
        let mut url = format!("{}{}", self.base_url, path);
        if let Some(query) = query {
            if !query.is_empty() {
                url.push('?');
                url.push_str(query);
            }
        }

        if method == "POST" {
            if let Some(namespace) = path
                .strip_prefix("/v2/namespaces/")
                .filter(|ns| !ns.contains('/'))
            {
                if query.is_none() {
                    let namespace =
                        percent_encoding::percent_decode_str(namespace).decode_utf8_lossy();
                    let resp = self
                        .send_source_write(&namespace, body.as_ref().unwrap_or(&Value::Null))
                        .await?;
                    let status = resp.status().as_u16();
                    let content_type = resp
                        .headers()
                        .get(reqwest::header::CONTENT_TYPE)
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_string);
                    let body = resp
                        .bytes()
                        .await
                        .map_err(|e| TurbopufferError::Other(e.to_string()))?
                        .to_vec();
                    return Ok(TurbopufferPassthroughResponse {
                        status,
                        content_type,
                        body,
                    });
                }
            }
        }
        let mut shared_mutation = if self
            .shared_cache
            .as_ref()
            .is_some_and(|cache| cache.certified())
            && self.api_key.is_some()
            && matches!(method, "POST" | "PATCH" | "DELETE")
            && !path.ends_with("/query")
        {
            let trimmed = path.trim_start_matches('/');
            if trimmed.split('/').count() != 3 {
                return Err(TurbopufferError::Other(
                    "unsupported shared-cache mutation may affect another namespace".into(),
                ));
            }
            let namespace = namespace_from_path(path).ok_or_else(|| {
                TurbopufferError::Other("unsupported shared-cache mutation path".into())
            })?;
            let namespace = percent_encoding::percent_decode_str(namespace).decode_utf8_lossy();
            let mut session = self.shared_session(&namespace).await?.ok_or_else(|| {
                TurbopufferError::Other("shared mutation fence unavailable".into())
            })?;
            session.fence.begin_write(None).await?;
            Some(session)
        } else {
            None
        };
        // Any other source mutation (delete, import, branch, copy, query flags)
        // conservatively rotates the certified scope before dispatch.
        let _mutation_guard = if matches!(method, "POST" | "PATCH" | "DELETE")
            && !path.ends_with("/query")
        {
            if let (Some(cache), Some(namespace)) =
                (&self.canonical_cache, namespace_from_path(path))
            {
                let namespace = percent_encoding::percent_decode_str(namespace).decode_utf8_lossy();
                self.generated_columns
                    .write()
                    .unwrap()
                    .remove(namespace.as_ref());
                if let Some(mut state) = cache.lock(&namespace).await {
                    cache.reset(&mut state);
                    Some(state)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };
        if method == "POST" && path.ends_with("/query") {
            if let Some(namespace) = namespace_from_path(path) {
                let queries = body
                    .as_ref()
                    .and_then(|v| v.get("queries"))
                    .and_then(Value::as_array)
                    .map_or(1, |legs| legs.len().max(1) as u32);
                reserve_provider_query(namespace, queries).await?;
            }
        }
        let request = match method {
            "GET" => self.client.get(&url),
            "POST" => self.client.post(&url),
            "PATCH" => self.client.patch(&url),
            "DELETE" => self.client.delete(&url),
            other => {
                return Err(TurbopufferError::Other(format!(
                    "unsupported passthrough method {}",
                    other
                )));
            }
        };
        let query_units = body
            .as_ref()
            .and_then(|b| b.get("queries"))
            .and_then(Value::as_array)
            .map_or(1, |q| q.len().max(1).try_into().unwrap_or(u32::MAX));
        let request = if let Some(body) = body {
            request.json(&body)
        } else {
            request
        };

        let dispatch = async {
            let headers_timer =
                crate::delete_timing::start(crate::delete_timing::Phase::UpstreamHeaders);
            let request = self.authorize(request)?;
            let measured_read = path.strip_prefix("/v2/namespaces/").and_then(|p| {
                if method == "POST" {
                    p.strip_suffix("/query")
                        .map(|n| (n, receipts::ReadKind::Query))
                } else if method == "GET" {
                    p.strip_suffix("/metadata")
                        .map(|n| (n, receipts::ReadKind::Metadata))
                } else {
                    None
                }
            });
            let mut receipt = measured_read.map(|(namespace, kind)| {
                receipts::Pending::with_units(namespace, kind, query_units)
            });
            let resp = match &mut receipt {
                Some(receipt) => receipt.send(request).await?,
                None => request
                    .send()
                    .await
                    .map_err(|e| TurbopufferError::Other(e.to_string()))?,
            };
            drop(headers_timer);
            let status = resp.status().as_u16();
            let content_type = resp
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let body_timer = crate::delete_timing::start(crate::delete_timing::Phase::UpstreamBody);
            let body = resp
                .bytes()
                .await
                .map_err(|e| TurbopufferError::Other(e.to_string()))?
                .to_vec();

            if let Some(receipt) = &mut receipt {
                receipt.finished_bytes(status, &body);
            }
            drop(body_timer);
            Ok(TurbopufferPassthroughResponse {
                status,
                content_type,
                body,
            })
        };
        let response = match &shared_mutation {
            Some(session) => session.protect(dispatch).await?,
            None => dispatch.await?,
        };
        if (200..300).contains(&response.status) {
            if let Some(session) = &mut shared_mutation {
                let _ = session.fence.complete_write().await;
            }
        }
        Ok(response)
    }

    async fn hint_cache_warm(&self, namespace: &str) -> Result<(), TurbopufferError> {
        self.capabilities()
            .require(crate::capabilities::WireFeature::Warm)?;
        let url = format!(
            "{}/v1/namespaces/{}/hint_cache_warm",
            self.base_url, namespace
        );
        let resp = self
            .authorize(self.client.get(&url))?
            .send()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(TurbopufferError::from_response(resp).await);
        }
        if let Ok(body) = resp.json::<Value>().await {
            observe_projected_billing(namespace, &body);
        }
        Ok(())
    }

    async fn upsert(
        &self,
        namespace: &str,
        docs: &[UpsertDoc],
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        self.capabilities()
            .require(crate::capabilities::WireFeature::UpsertRows)?;
        // Build row-oriented payload for Turbopuffer v2 API
        let rows: Vec<Value> = docs
            .iter()
            .map(|d| {
                let mut row = serde_json::Map::new();
                row.insert("id".to_string(), Value::String(d.id.clone()));
                if let Some(ref vec) = d.vector {
                    row.insert(
                        "vector".to_string(),
                        serde_json::to_value(vec).unwrap_or(Value::Null),
                    );
                }
                for (k, v) in &d.attributes {
                    row.insert(k.clone(), v.clone());
                }
                Value::Object(row)
            })
            .collect();

        let body = serde_json::json!({
            "upsert_rows": rows,
            "distance_metric": "cosine_distance",
        });

        let resp = self.send_source_write(namespace, &body).await?;

        if !resp.status().is_success() {
            return Err(TurbopufferError::from_response(resp).await);
        }
        let body: Value = resp
            .json()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;
        Ok(TurbopufferWriteOutcome {
            billing: billing_from_body(&body),
        })
    }

    async fn patch(
        &self,
        namespace: &str,
        docs: &[PatchDoc],
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        self.capabilities()
            .require(crate::capabilities::WireFeature::PatchRows)?;
        let rows: Vec<Value> = docs
            .iter()
            .map(|d| {
                let mut row = serde_json::Map::new();
                row.insert("id".to_string(), Value::String(d.id.clone()));
                for (k, v) in &d.attributes {
                    row.insert(k.clone(), v.clone());
                }
                Value::Object(row)
            })
            .collect();

        let body = serde_json::json!({ "patch_rows": rows });

        let resp = self.send_source_write(namespace, &body).await?;

        if !resp.status().is_success() {
            return Err(TurbopufferError::from_response(resp).await);
        }
        let body: Value = resp
            .json()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;
        Ok(TurbopufferWriteOutcome {
            billing: billing_from_body(&body),
        })
    }

    async fn patch_columns(
        &self,
        namespace: &str,
        columns: &PatchColumns,
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        self.capabilities()
            .require(crate::capabilities::WireFeature::PatchColumns)?;
        let mut patch_columns = serde_json::Map::new();
        patch_columns.insert(
            "id".to_string(),
            Value::Array(columns.ids.iter().cloned().map(Value::String).collect()),
        );
        for (name, values) in &columns.columns {
            patch_columns.insert(name.clone(), Value::Array(values.clone()));
        }

        let body = serde_json::json!({ "patch_columns": patch_columns });
        let resp = self.send_source_write(namespace, &body).await?;

        if !resp.status().is_success() {
            return Err(TurbopufferError::from_response(resp).await);
        }
        let body: Value = resp
            .json()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;
        Ok(TurbopufferWriteOutcome {
            billing: billing_from_body(&body),
        })
    }

    async fn delete(
        &self,
        namespace: &str,
        ids: &[String],
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        self.capabilities()
            .require(crate::capabilities::WireFeature::DeleteIds)?;
        let body = serde_json::json!({
            "deletes": ids,
        });

        let resp = self.send_source_write(namespace, &body).await?;

        if !resp.status().is_success() {
            return Err(TurbopufferError::from_response(resp).await);
        }
        let body: Value = resp
            .json()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;
        Ok(TurbopufferWriteOutcome {
            billing: billing_from_body(&body),
        })
    }

    async fn query(
        &self,
        namespace: &str,
        vector: &[f64],
        top_k: u32,
        filters: Option<&Value>,
        include_attributes: Option<&IncludeAttributes>,
    ) -> Result<TurbopufferQueryOutcome, TurbopufferError> {
        self.capabilities()
            .require(crate::capabilities::WireFeature::Dense)?;
        let mut body = serde_json::json!({
            "rank_by": ["vector", "ANN", vector],
            "top_k": top_k,
            "consistency": {"level": "eventual"},
        });
        if let Some(f) = filters {
            body["filters"] = f.clone();
        }
        if let Some(attrs) = include_attributes {
            body["include_attributes"] = attrs.to_turbopuffer_value();
        }

        reserve_provider_query(namespace, 1).await?;
        let url = format!("{}/v2/namespaces/{}/query", self.base_url, namespace);
        let resp_body = receipts::read_json(
            namespace,
            receipts::ReadKind::Query,
            self.authorize(self.client.post(&url).json(&body))?,
        )
        .await?;

        Ok(TurbopufferQueryOutcome {
            rows: rows_from_query_body(&resp_body),
            billing: billing_from_body(&resp_body),
        })
    }

    async fn ranked_query(
        &self,
        namespace: &str,
        rank_by: &Value,
        top_k: u32,
        filters: Option<&Value>,
        include_attributes: Option<&IncludeAttributes>,
    ) -> Result<TurbopufferQueryOutcome, TurbopufferError> {
        if let Some(feature) = crate::capabilities::WireFeature::for_rank(rank_by) {
            self.capabilities().require(feature)?;
        }
        let mut body = serde_json::json!({
            "rank_by": rank_by.clone(),
            "top_k": top_k,
            "consistency": {"level": "eventual"},
        });
        if let Some(f) = filters {
            body["filters"] = f.clone();
        }
        if let Some(attrs) = include_attributes {
            body["include_attributes"] = attrs.to_turbopuffer_value();
        }

        reserve_provider_query(namespace, 1).await?;
        let url = format!("{}/v2/namespaces/{}/query", self.base_url, namespace);
        let resp_body = receipts::read_json(
            namespace,
            receipts::ReadKind::Query,
            self.authorize(self.client.post(&url).json(&body))?,
        )
        .await?;

        Ok(TurbopufferQueryOutcome {
            rows: rows_from_query_body(&resp_body),
            billing: billing_from_body(&resp_body),
        })
    }

    async fn multi_ranked_query(
        &self,
        namespace: &str,
        legs: &[Value],
        rerank_by: Option<&Value>,
    ) -> Result<Value, TurbopufferError> {
        self.capabilities()
            .require(crate::capabilities::WireFeature::MultiQuery)?;
        let mut body = serde_json::json!({
            "queries": legs,
        });
        if let Some(rerank_by) = rerank_by {
            body["rerank_by"] = rerank_by.clone();
        }
        reserve_provider_query(namespace, legs.len().max(1) as u32).await?;
        let url = format!("{}/v2/namespaces/{}/query", self.base_url, namespace);
        receipts::read_json_with_units(
            namespace,
            receipts::ReadKind::Query,
            self.authorize(self.client.post(&url).json(&body))?,
            legs.len().max(1).try_into().unwrap_or(u32::MAX),
        )
        .await
    }

    async fn fetch(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<DocumentResponse>, TurbopufferError> {
        if (self.capture_metadata || self.canonical_cache.is_some() || self.shared_cache.is_some())
            && self.api_key.is_some()
        {
            return Ok(self
                .fetch_many(namespace, &[id.to_owned()])
                .await?
                .remove(id));
        }
        self.capabilities()
            .require(crate::capabilities::WireFeature::Fetch)?;
        let Some(wire_id) = Self::wire_id(id, self.integer_ids(namespace).await?) else {
            // String-only system markers cannot exist in an integer namespace.
            return Ok(None);
        };
        let body = serde_json::json!({
            "rank_by": ["id", "asc"],
            "top_k": 1,
            "filters": ["id", "Eq", wire_id],
            "include_attributes": true,
            "consistency": {"level": "eventual"},
        });

        reserve_provider_query(namespace, 1).await?;
        let url = format!("{}/v2/namespaces/{}/query", self.base_url, namespace);
        let resp_body = receipts::read_json(
            namespace,
            receipts::ReadKind::Fetch,
            self.authorize(self.client.post(&url).json(&body))?,
        )
        .await?;

        observe_projected_billing(namespace, &resp_body);

        let rows = resp_body
            .get("rows")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        Ok(rows.into_iter().find_map(|row| {
            let (row_id, _) = id_from_wire(row.get("id")?)?;
            if row_id != id {
                return None;
            }
            let mut attributes = HashMap::new();
            if let Some(obj) = row.as_object() {
                for (k, v) in obj {
                    if !is_system_column(k) {
                        attributes.insert(k.clone(), v.clone());
                    }
                }
            }
            Some(DocumentResponse {
                id: row_id,
                attributes,
            })
        }))
    }

    async fn fetch_many(
        &self,
        namespace: &str,
        ids: &[String],
    ) -> Result<HashMap<String, DocumentResponse>, TurbopufferError> {
        if self.shared_cache.is_some() {
            return self.shared_fetch_many(namespace, ids).await;
        }
        self.capabilities()
            .require(crate::capabilities::WireFeature::Fetch)?;
        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        let mut cached = if self.api_key.is_some() {
            match &self.canonical_cache {
                Some(cache) => cache.lock(namespace).await,
                None => None,
            }
        } else {
            None
        };
        let mut hits = HashMap::new();
        if let (Some(cache), Some(state)) = (&self.canonical_cache, &mut cached) {
            match cache.backend.get_many(&state.scope, ids).await {
                Ok(rows) => hits = rows,
                Err(_) => cache.reset(state),
            }
        }
        let mut seen = HashSet::new();
        let missing: Vec<_> = ids
            .iter()
            .filter(|id| !hits.contains_key(*id) && seen.insert((*id).clone()))
            .cloned()
            .collect();
        let mut found: HashMap<String, DocumentResponse> = hits
            .into_iter()
            .filter_map(|(id, attrs)| {
                attrs.map(|attributes| (id.clone(), DocumentResponse { id, attributes }))
            })
            .collect();
        if missing.is_empty() {
            return Ok(found);
        }
        let ids = &missing;
        let integer = self.integer_ids(namespace).await?;
        let wire_ids: Vec<Value> = ids
            .iter()
            .filter_map(|id| Self::wire_id(id, integer))
            .collect();
        if wire_ids.is_empty() {
            return Ok(found);
        }
        // Canonical Function inputs and completion fences must observe committed
        // source writes/deletions, just like a native strong ID lookup.
        let body = serde_json::json!({
            "rank_by": ["id", "asc"],
            "top_k": ids.len(),
            "filters": ["id", "In", wire_ids],
            "include_attributes": true,
            "consistency": {"level": "strong"},
        });

        reserve_provider_query(namespace, 1).await?;
        let url = format!("{}/v2/namespaces/{}/query", self.base_url, namespace);
        let resp_body = receipts::read_json(
            namespace,
            receipts::ReadKind::FetchMany,
            self.authorize(self.client.post(&url).json(&body))?,
        )
        .await?;

        observe_projected_billing(namespace, &resp_body);

        let rows = resp_body
            .get("rows")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let mut result = HashMap::new();
        for row in rows {
            if let Some((id, _)) = row.get("id").and_then(id_from_wire) {
                let mut attributes = HashMap::new();
                if let Some(obj) = row.as_object() {
                    for (k, v) in obj {
                        if !is_system_column(k) {
                            attributes.insert(k.clone(), v.clone());
                        }
                    }
                }
                result.insert(id.clone(), DocumentResponse { id, attributes });
            }
        }
        if let (Some(cache), Some(state)) = (&self.canonical_cache, &mut cached) {
            let rows = missing
                .iter()
                .map(|id| (id.clone(), result.get(id).map(|doc| doc.attributes.clone())))
                .collect();
            if cache.backend.put_many(&state.scope, &rows).await.is_err() {
                cache.reset(state);
            }
        }
        found.extend(result);
        Ok(found)
    }

    async fn fetch_siblings(
        &self,
        namespace: &str,
        parent: &str,
    ) -> Result<Vec<DocumentResponse>, TurbopufferError> {
        if self.shared_cache.is_some() {
            return self.shared_fetch_siblings(namespace, parent).await;
        }
        let mut cached = if self.api_key.is_some() {
            match &self.canonical_cache {
                Some(cache) => cache.lock(namespace).await,
                None => None,
            }
        } else {
            None
        };
        if let (Some(cache), Some(state)) = (&self.canonical_cache, &mut cached) {
            if let Some(ids) = state.groups.get(parent) {
                if let Ok(mut rows) = cache.backend.get_many(&state.scope, ids).await {
                    if rows.len() == ids.len() && rows.values().all(Option::is_some) {
                        // Preserve provider ordering (numeric IDs are not
                        // lexicographic) and the same page-two selection.
                        return Ok(ids
                            .iter()
                            .map(|id| DocumentResponse {
                                id: id.clone(),
                                attributes: rows.remove(id).unwrap().unwrap(),
                            })
                            .collect());
                    }
                }
            }
        }
        let body = serde_json::json!({"rank_by":["id","asc"],"top_k":10000,"filters":["_hevlayer_parent_id","Eq",parent],"include_attributes":true,"consistency":{"level":"strong"}});
        reserve_provider_query(namespace, 1).await?;
        let url = format!("{}/v2/namespaces/{}/query", self.base_url, namespace);
        let resp_body = receipts::read_json(
            namespace,
            receipts::ReadKind::Siblings,
            self.authorize(self.client.post(&url).json(&body))?,
        )
        .await?;
        observe_projected_billing(namespace, &resp_body);
        let rows = rows_from_query_body(&resp_body);
        if rows.len() >= 10000 {
            return Err(TurbopufferError::Other(
                "parent lookup exceeds 9999 siblings; refusing a truncated group".into(),
            ));
        }
        let documents: Vec<_> = rows
            .into_iter()
            .map(|row| DocumentResponse {
                id: row.id,
                attributes: row.attributes,
            })
            .collect();
        if let (Some(cache), Some(state)) = (&self.canonical_cache, &mut cached) {
            let rows = documents
                .iter()
                .map(|doc| (doc.id.clone(), Some(doc.attributes.clone())))
                .collect();
            if cache.backend.put_many(&state.scope, &rows).await.is_ok() {
                if state.groups.len() >= 4096
                    || state.groups.values().map(Vec::len).sum::<usize>() + documents.len()
                        > 100_000
                {
                    state.groups.clear();
                }
                state.groups.insert(
                    parent.to_owned(),
                    documents.iter().map(|doc| doc.id.clone()).collect(),
                );
            } else {
                cache.reset(state);
            }
        }
        Ok(documents)
    }

    async fn fetch_vector(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<Vec<f64>>, TurbopufferError> {
        self.capabilities()
            .require(crate::capabilities::WireFeature::NearestToId)?;
        // Ask for the `vector` column explicitly — Turbopuffer omits it from
        // query rows unless requested. This is the *only* place the gateway
        // pulls a vector out of upstream; everywhere else, `is_system_column`
        // drops it before it reaches the caller.
        let Some(wire_id) = Self::wire_id(id, self.integer_ids(namespace).await?) else {
            // String-only system markers cannot exist in an integer namespace.
            return Ok(None);
        };
        let body = serde_json::json!({
            "rank_by": ["id", "asc"],
            "top_k": 1,
            "filters": ["id", "Eq", wire_id],
            "include_attributes": ["vector"],
            "consistency": {"level": "eventual"},
        });

        reserve_provider_query(namespace, 1).await?;
        let url = format!("{}/v2/namespaces/{}/query", self.base_url, namespace);
        let resp_body = receipts::read_json(
            namespace,
            receipts::ReadKind::Vector,
            self.authorize(self.client.post(&url).json(&body))?,
        )
        .await?;

        observe_projected_billing(namespace, &resp_body);

        let rows = resp_body
            .get("rows")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        Ok(rows.into_iter().find_map(|row| {
            if row
                .get("id")
                .and_then(id_from_wire)
                .map(|(row_id, _)| row_id)
                .as_deref()
                != Some(id)
            {
                return None;
            }
            let arr = row.get("vector")?.as_array()?;
            let mut out = Vec::with_capacity(arr.len());
            for item in arr {
                out.push(item.as_f64()?);
            }
            Some(out)
        }))
    }

    async fn scan_page(
        &self,
        namespace: &str,
        cursor: Option<&str>,
        page_size: u32,
        filters: Option<&Value>,
        include_attributes: Option<&[String]>,
    ) -> Result<DocumentPage, TurbopufferError> {
        self.scan_page_at_consistency(
            namespace,
            cursor,
            page_size,
            filters,
            include_attributes,
            false,
        )
        .await
    }

    async fn scan_page_strong(
        &self,
        namespace: &str,
        cursor: Option<&str>,
        page_size: u32,
        filters: Option<&Value>,
        include_attributes: Option<&[String]>,
    ) -> Result<DocumentPage, TurbopufferError> {
        self.scan_page_at_consistency(
            namespace,
            cursor,
            page_size,
            filters,
            include_attributes,
            true,
        )
        .await
    }

    async fn head_namespace(&self, namespace: &str) -> Result<NamespaceMeta, TurbopufferError> {
        self.capabilities()
            .require(crate::capabilities::WireFeature::NamespaceCrud)?;
        let url = format!("{}/v2/namespaces/{}/metadata", self.base_url, namespace);
        // Metadata is a billed physical read too. Retain inherited Function
        // and expense permits before dispatch, including identity hydration.
        reserve_provider_query(namespace, 1).await?;
        let body = receipts::read_json(
            namespace,
            receipts::ReadKind::Metadata,
            self.authorize(self.client.get(&url))?,
        )
        .await?;

        if (self.capture_metadata || self.canonical_cache.is_some() || self.shared_cache.is_some())
            && self.api_key.is_some()
        {
            let mut columns = self.generated_columns.write().unwrap();
            if columns.len() < 4096 || columns.contains_key(namespace) {
                columns.insert(namespace.to_owned(), generated_columns(&body["schema"]));
            }
        }
        observe_projected_billing(namespace, &body);
        Ok(parse_metadata_body(body))
    }
}

/// Parse a turbopuffer `/metadata` body into `NamespaceMeta`. Kept as a free
/// function so tests can drive it without a network round-trip.
///
/// Resolution order for the stability signal:
///   1. `index.status` — `"up-to-date"` → Stable, `"updating"` → Updating.
///   2. If status missing: recursive scan for any `unindexed_bytes > 0`
///      anywhere in the body → Updating.
///   3. Otherwise: `Unknown` (NOT defaulted to Stable; the watcher will
///      refuse to advance the watermark, and the query path treats Unknown
///      as "skip filter, rely on 429 retry").
///
/// `unindexed_bytes` is read from `index.unindexed_bytes`, with legacy
/// top-level keys as fallback for older turbopuffer versions.
pub(crate) fn parse_metadata_body(body: Value) -> NamespaceMeta {
    let index_status = match body
        .get("index")
        .and_then(|i| i.get("status"))
        .and_then(|s| s.as_str())
    {
        Some("up-to-date") => IndexStatus::Stable,
        Some("updating") => IndexStatus::Updating,
        Some(_) | None => {
            if any_unindexed_bytes_nonzero(&body) {
                IndexStatus::Updating
            } else {
                IndexStatus::Unknown
            }
        }
    };

    let unindexed_bytes = body
        .get("index")
        .and_then(|i| i.get("unindexed_bytes"))
        .and_then(|v| v.as_u64())
        .or_else(|| {
            body.get("approx_unindexed_logical_bytes")
                .and_then(|v| v.as_u64())
        })
        .or_else(|| body.get("unindexed_bytes").and_then(|v| v.as_u64()))
        .or_else(|| body.get("unindexed_writes_bytes").and_then(|v| v.as_u64()));

    let approx_row_count = body
        .get("approx_row_count")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let approx_logical_bytes = body.get("approx_logical_bytes").and_then(|v| v.as_u64());

    NamespaceMeta {
        index_status,
        unindexed_bytes,
        approx_row_count,
        approx_logical_bytes,
        count_settle: None,
        raw: body,
    }
}

// --- Mock implementation for testing ---

pub struct MockTurbopufferClient {
    metadata_requests: AtomicUsize,
    fetch_many_requests: AtomicUsize,
    docs: tokio::sync::RwLock<HashMap<String, HashMap<String, DocumentResponse>>>,
    /// Per-namespace per-id vector, populated by `upsert`. `fetch_vector`
    /// reads here. Stored separately from `docs` because `DocumentResponse`
    /// intentionally omits the vector field.
    vectors: tokio::sync::RwLock<HashMap<String, HashMap<String, Vec<f64>>>>,
    /// Stored per-namespace `(status, unindexed_bytes)` overrides. Absent → Unknown.
    status: tokio::sync::RwLock<HashMap<String, (IndexStatus, Option<u64>)>>,
    /// Per-namespace metadata override applied on top of the derived body.
    /// Lets a test pin `approx_logical_bytes`, `schema`, `last_write_at`,
    /// labels, etc. without seeding documents.
    metadata_overrides: tokio::sync::RwLock<HashMap<String, Value>>,
    /// Force the next `query` call for the namespace to fail with 429.
    /// Consumed (set back to false) on first read so tests can verify retry.
    rate_limit_once: tokio::sync::RwLock<HashMap<String, bool>>,
    /// Per-namespace flag: when set, `head_namespace` returns
    /// `TurbopufferError::Other`. Used by tests that exercise the gateway's
    /// per-row `metadata_error` fallback in `/v2/namespaces`.
    head_failure: tokio::sync::RwLock<HashMap<String, String>>,
    patch_failure_once: tokio::sync::RwLock<HashMap<String, bool>>,
    /// Per-namespace flag: when set, `head_namespace` returns
    /// `TurbopufferError::NotFound`, mirroring upstream's 404 for a missing
    /// namespace. Used by tests of the metadata route's 404 mapping.
    head_not_found: tokio::sync::RwLock<std::collections::HashSet<String>>,
    /// Optional status override for namespace delete passthrough calls. Used
    /// by gateway route tests to exercise idempotent 404 and hard upstream
    /// failure handling without mutating the mock store first.
    delete_namespace_status: tokio::sync::RwLock<HashMap<String, u16>>,
    scan_filters: tokio::sync::RwLock<Vec<Option<Value>>>,
    scan_returned_bytes: AtomicUsize,
    strong_scan_calls: AtomicUsize,
    reconcile_token_supported: std::sync::atomic::AtomicBool,
    scan_include_attributes: tokio::sync::RwLock<Vec<Option<Vec<String>>>>,
    ranked_query_filters: tokio::sync::RwLock<Vec<Option<Value>>>,
    /// Every `ranked_query` call as the store received it, in arrival order.
    ranked_query_calls: tokio::sync::RwLock<Vec<Value>>,
    /// Test override for the store declaration and the native-wire flag, so
    /// a route's per-store behaviour is testable without a second adapter.
    capabilities_override: std::sync::RwLock<Option<crate::capabilities::Capabilities>>,
    native_wire_override: std::sync::atomic::AtomicBool,
    missing_include_attributes: tokio::sync::RwLock<HashMap<String, HashSet<String>>>,
    scan_page_delay: tokio::sync::RwLock<Option<Duration>>,
    scan_page_active: AtomicUsize,
    scan_page_max_active: AtomicUsize,
    ranked_query_delay: tokio::sync::RwLock<Option<Duration>>,
    ranked_query_active: AtomicUsize,
    ranked_query_max_active: AtomicUsize,
    warm_hints: tokio::sync::RwLock<HashMap<String, u64>>,
    /// Every body posted to `POST /v2/namespaces/{namespace}` through
    /// `passthrough`, with its path and query, in arrival order.
    write_requests: std::sync::Mutex<Vec<MockWriteRequest>>,
    /// The declaration this mock reports; turbopuffer's unless a test
    /// stands the mock in for another store kind.
    declared: crate::capabilities::Capabilities,
}

/// One namespace write the mock received through `passthrough`.
#[derive(Clone, Debug, PartialEq)]
pub struct MockWriteRequest {
    pub path: String,
    pub query: Option<String>,
    pub body: Option<Value>,
}

struct CounterGuard<'a> {
    active: &'a AtomicUsize,
}

impl Drop for CounterGuard<'_> {
    fn drop(&mut self) {
        self.active.fetch_sub(1, AtomicOrdering::SeqCst);
    }
}

fn enter_counter<'a>(active: &'a AtomicUsize, max_active: &AtomicUsize) -> CounterGuard<'a> {
    let current = active.fetch_add(1, AtomicOrdering::SeqCst) + 1;
    let mut observed = max_active.load(AtomicOrdering::SeqCst);
    while current > observed {
        match max_active.compare_exchange(
            observed,
            current,
            AtomicOrdering::SeqCst,
            AtomicOrdering::SeqCst,
        ) {
            Ok(_) => break,
            Err(next) => observed = next,
        }
    }
    CounterGuard { active }
}

impl Default for MockTurbopufferClient {
    fn default() -> Self {
        Self::new()
    }
}

impl MockTurbopufferClient {
    pub fn new() -> Self {
        Self {
            metadata_requests: AtomicUsize::new(0),
            fetch_many_requests: AtomicUsize::new(0),
            docs: tokio::sync::RwLock::new(HashMap::new()),
            vectors: tokio::sync::RwLock::new(HashMap::new()),
            status: tokio::sync::RwLock::new(HashMap::new()),
            metadata_overrides: tokio::sync::RwLock::new(HashMap::new()),
            rate_limit_once: tokio::sync::RwLock::new(HashMap::new()),
            head_failure: tokio::sync::RwLock::new(HashMap::new()),
            patch_failure_once: tokio::sync::RwLock::new(HashMap::new()),
            head_not_found: tokio::sync::RwLock::new(std::collections::HashSet::new()),
            delete_namespace_status: tokio::sync::RwLock::new(HashMap::new()),
            scan_filters: tokio::sync::RwLock::new(Vec::new()),
            scan_returned_bytes: AtomicUsize::new(0),
            strong_scan_calls: AtomicUsize::new(0),
            reconcile_token_supported: std::sync::atomic::AtomicBool::new(true),
            scan_include_attributes: tokio::sync::RwLock::new(Vec::new()),
            ranked_query_filters: tokio::sync::RwLock::new(Vec::new()),
            ranked_query_calls: tokio::sync::RwLock::new(Vec::new()),
            capabilities_override: std::sync::RwLock::new(None),
            native_wire_override: std::sync::atomic::AtomicBool::new(false),
            missing_include_attributes: tokio::sync::RwLock::new(HashMap::new()),
            scan_page_delay: tokio::sync::RwLock::new(None),
            scan_page_active: AtomicUsize::new(0),
            scan_page_max_active: AtomicUsize::new(0),
            ranked_query_delay: tokio::sync::RwLock::new(None),
            ranked_query_active: AtomicUsize::new(0),
            ranked_query_max_active: AtomicUsize::new(0),
            warm_hints: tokio::sync::RwLock::new(HashMap::new()),
            write_requests: std::sync::Mutex::new(Vec::new()),
            declared: TURBOPUFFER_CAPABILITIES,
        }
    }

    /// Report `capabilities` instead of turbopuffer's, so a gateway test can
    /// exercise a capability gate for another store kind without its backend.
    pub fn with_capabilities(mut self, capabilities: crate::capabilities::Capabilities) -> Self {
        self.declared = capabilities;
        self
    }

    /// The namespace writes received through `passthrough`, oldest first.
    pub fn write_requests(&self) -> Vec<MockWriteRequest> {
        self.write_requests
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Seed a namespace so it shows up in the upstream `/v1/namespaces`
    /// list without having to write any documents to it. The mock's list
    /// is derived from the `docs` map, so namespaces with zero rows would
    /// otherwise only appear after an upsert.
    pub async fn ensure_namespace(&self, namespace: &str) {
        self.docs
            .write()
            .await
            .entry(namespace.to_string())
            .or_default();
    }

    /// Replace the per-namespace metadata body returned by `head_namespace`
    /// (and the `/metadata` passthrough). Caller supplies the full body —
    /// the mock does not merge with derived fields when an override is
    /// present.
    pub async fn set_metadata_override(&self, namespace: &str, body: Value) {
        self.metadata_overrides
            .write()
            .await
            .insert(namespace.to_string(), body);
    }

    pub fn fetch_many_request_count(&self) -> usize {
        self.fetch_many_requests.load(AtomicOrdering::SeqCst)
    }
    pub fn metadata_request_count(&self) -> usize {
        self.metadata_requests.load(AtomicOrdering::SeqCst)
    }
    /// Serialized returned documents, not billable upstream logical bytes.
    pub fn strong_scan_calls(&self) -> usize {
        self.strong_scan_calls.load(AtomicOrdering::SeqCst)
    }

    pub fn scan_returned_bytes(&self) -> usize {
        self.scan_returned_bytes.load(AtomicOrdering::SeqCst)
    }

    pub fn set_reconcile_token_supported(&self, supported: bool) {
        self.reconcile_token_supported
            .store(supported, AtomicOrdering::SeqCst);
    }

    pub async fn scan_filters(&self) -> Vec<Option<Value>> {
        self.scan_filters.read().await.clone()
    }

    pub async fn scan_include_attributes(&self) -> Vec<Option<Vec<String>>> {
        self.scan_include_attributes.read().await.clone()
    }

    pub async fn ranked_query_filters(&self) -> Vec<Option<Value>> {
        self.ranked_query_filters.read().await.clone()
    }

    /// Full request shape of every `ranked_query` call (`namespace`,
    /// `rank_by`, `top_k`, `filters`, `include_attributes`), for tests that
    /// pin the upstream wire.
    pub async fn ranked_query_calls(&self) -> Vec<Value> {
        self.ranked_query_calls.read().await.clone()
    }

    /// Answer `capabilities()` with another store's declaration.
    pub fn set_capabilities(&self, capabilities: crate::capabilities::Capabilities) {
        *self
            .capabilities_override
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(capabilities);
    }

    /// Answer `requires_native_wire` as a native SQL adapter would.
    pub fn set_native_wire(&self, native: bool) {
        self.native_wire_override
            .store(native, AtomicOrdering::SeqCst);
    }

    pub async fn arm_missing_include_attribute(&self, namespace: &str, field: &str) {
        self.missing_include_attributes
            .write()
            .await
            .entry(namespace.to_string())
            .or_default()
            .insert(field.to_string());
    }

    pub async fn set_scan_page_delay(&self, delay: Duration) {
        *self.scan_page_delay.write().await = Some(delay);
    }

    pub fn max_concurrent_scan_pages(&self) -> usize {
        self.scan_page_max_active.load(AtomicOrdering::SeqCst)
    }

    pub async fn set_ranked_query_delay(&self, delay: Duration) {
        *self.ranked_query_delay.write().await = Some(delay);
    }

    pub fn max_concurrent_ranked_queries(&self) -> usize {
        self.ranked_query_max_active.load(AtomicOrdering::SeqCst)
    }

    /// Arm `head_namespace` for this namespace to return an error on every
    /// call until cleared. Used to exercise the gateway's per-row
    /// `metadata_error` fallback in `/v2/namespaces`.
    /// Fail one column patch before mutating the mock store.
    pub async fn arm_patch_failure(&self, namespace: &str) {
        self.patch_failure_once
            .write()
            .await
            .insert(namespace.into(), true);
    }

    pub async fn arm_head_failure(&self, namespace: &str, message: &str) {
        self.head_failure
            .write()
            .await
            .insert(namespace.to_string(), message.to_string());
    }

    /// Arm `head_namespace` for this namespace to return
    /// `TurbopufferError::NotFound`, the way upstream answers a metadata
    /// read for a namespace that does not exist.
    pub async fn arm_head_not_found(&self, namespace: &str) {
        self.head_not_found
            .write()
            .await
            .insert(namespace.to_string());
    }

    /// Set the namespace to `up-to-date` with no `unindexed_bytes` field
    /// (matches turbopuffer's contract that the field is omitted when stable).
    pub async fn set_stable(&self, namespace: &str) {
        self.status
            .write()
            .await
            .insert(namespace.to_string(), (IndexStatus::Stable, None));
    }

    /// Set the namespace to `updating` with the given `unindexed_bytes`.
    pub async fn set_updating(&self, namespace: &str, unindexed_bytes: u64) {
        self.status.write().await.insert(
            namespace.to_string(),
            (IndexStatus::Updating, Some(unindexed_bytes)),
        );
    }

    /// Backwards-compatible shim used by older tests. `bytes == 0` →
    /// stable; `bytes > 0` → updating with that many pending bytes.
    pub async fn set_unindexed_bytes(&self, namespace: &str, bytes: u64) {
        if bytes == 0 {
            self.set_stable(namespace).await;
        } else {
            self.set_updating(namespace, bytes).await;
        }
    }

    /// Arm the namespace to 429 the next `query` call exactly once.
    pub async fn arm_rate_limit(&self, namespace: &str) {
        self.rate_limit_once
            .write()
            .await
            .insert(namespace.to_string(), true);
    }

    pub async fn warm_hint_count(&self, namespace: &str) -> u64 {
        self.warm_hints
            .read()
            .await
            .get(namespace)
            .copied()
            .unwrap_or(0)
    }

    pub async fn set_delete_namespace_status(&self, namespace: &str, status: u16) {
        self.delete_namespace_status
            .write()
            .await
            .insert(namespace.to_string(), status);
    }
}

/// Mimic Turbopuffer's `AttrValueInput` enum (scalars and lists of scalars
/// only). Real Turbopuffer rejects nested-object attribute values with a 422
/// — historically the mock did not, so object-valued writeback regressions slipped past
/// the integration suite. Returns `Err` with the offending attribute name on
/// the first violation.
fn validate_attr_value_input(name: &str, value: &Value) -> Result<(), TurbopufferError> {
    fn is_scalar(value: &Value) -> bool {
        matches!(
            value,
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)
        )
    }
    let ok = match value {
        Value::Array(items) => items.iter().all(is_scalar),
        v => is_scalar(v),
    };
    if !ok {
        return Err(TurbopufferError::Other(format!(
            "attribute '{}' violates AttrValueInput: nested objects are not accepted by Turbopuffer",
            name
        )));
    }
    Ok(())
}

fn object_schema_attribute(body: &Value) -> Option<&str> {
    body.get("schema")
        .and_then(Value::as_object)
        .and_then(|schema| {
            schema.iter().find_map(|(attribute, config)| {
                let attribute_type = config
                    .as_str()
                    .or_else(|| config.get("type").and_then(Value::as_str));
                (attribute_type == Some("object")).then_some(attribute.as_str())
            })
        })
}

#[async_trait]
impl TurbopufferClient for MockTurbopufferClient {
    fn capabilities(&self) -> crate::capabilities::Capabilities {
        self.capabilities_override
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .unwrap_or(self.declared)
    }

    fn requires_native_wire(&self, _namespace: &str) -> bool {
        self.native_wire_override.load(AtomicOrdering::SeqCst)
    }

    fn blob_storage(&self, namespace: &str) -> crate::capabilities::BlobStorage {
        turbopuffer_blob_storage(namespace)
    }

    /// Stores the row the HTTP client writes: base64 `data` in the blob set.
    async fn put_blob(
        &self,
        namespace: &str,
        sha256: &str,
        bytes: &[u8],
    ) -> Result<(), TurbopufferError> {
        self.docs
            .write()
            .await
            .entry(blob_set_namespace(namespace))
            .or_default()
            .insert(
                sha256.to_string(),
                DocumentResponse {
                    id: sha256.to_string(),
                    attributes: HashMap::from([(
                        BLOB_DATA_ATTRIBUTE.to_string(),
                        encode_blob_value(bytes),
                    )]),
                },
            );
        Ok(())
    }

    async fn get_blob(
        &self,
        namespace: &str,
        sha256: &str,
    ) -> Result<Option<Vec<u8>>, TurbopufferError> {
        self.docs
            .read()
            .await
            .get(&blob_set_namespace(namespace))
            .and_then(|docs| docs.get(sha256))
            .map(|doc| {
                decode_blob_value(
                    doc.attributes
                        .get(BLOB_DATA_ATTRIBUTE)
                        .unwrap_or(&Value::Null),
                )
            })
            .transpose()
    }

    async fn passthrough(
        &self,
        method: &str,
        path: &str,
        query: Option<&str>,
        body: Option<Value>,
    ) -> Result<TurbopufferPassthroughResponse, TurbopufferError> {
        let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
        if let ("DELETE", ["v2", "namespaces", namespace]) = (method, parts.as_slice()) {
            if let Some(status) = self.delete_namespace_status.read().await.get(*namespace) {
                let bytes = serde_json::to_vec(&serde_json::json!({
                    "status": "ERROR",
                    "message": format!("mock delete status {status}"),
                }))
                .map_err(|e| TurbopufferError::Other(e.to_string()))?;
                return Ok(TurbopufferPassthroughResponse {
                    status: *status,
                    content_type: Some("application/json".to_string()),
                    body: bytes,
                });
            }
        }
        if let ("POST", ["v2", "namespaces", _]) = (method, parts.as_slice()) {
            if let Some(attribute) = body.as_ref().and_then(object_schema_attribute) {
                let bytes = serde_json::to_vec(&serde_json::json!({
                    "error": format!(
                        "Failed to deserialize the JSON body into the target type: schema.{attribute}: data did not match any variant of untagged enum AttributeSchemaInput"
                    ),
                    "status": "error",
                }))
                .map_err(|e| TurbopufferError::Other(e.to_string()))?;
                return Ok(TurbopufferPassthroughResponse {
                    status: 422,
                    content_type: Some("application/json".to_string()),
                    body: bytes,
                });
            }
        }
        if let ("POST", ["v2", "namespaces", _]) = (method, parts.as_slice()) {
            self.write_requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(MockWriteRequest {
                    path: path.to_string(),
                    query: query.map(str::to_string),
                    body: body.clone(),
                });
        }
        let body = match mock_passthrough(self, method, path, query, body).await {
            Ok(body) => body,
            Err(TurbopufferError::Response(response)) => return Ok(response),
            Err(error) => return Err(error),
        };
        let bytes =
            serde_json::to_vec(&body).map_err(|e| TurbopufferError::Other(e.to_string()))?;
        Ok(TurbopufferPassthroughResponse {
            status: 200,
            content_type: Some("application/json".to_string()),
            body: bytes,
        })
    }

    async fn hint_cache_warm(&self, namespace: &str) -> Result<(), TurbopufferError> {
        let mut hints = self.warm_hints.write().await;
        *hints.entry(namespace.to_string()).or_insert(0) += 1;
        Ok(())
    }

    async fn upsert(
        &self,
        namespace: &str,
        docs: &[UpsertDoc],
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        for doc in docs {
            for (name, value) in &doc.attributes {
                validate_attr_value_input(name, value)?;
            }
        }
        let mut store = self.docs.write().await;
        let ns = store.entry(namespace.to_string()).or_default();
        for doc in docs {
            ns.insert(
                doc.id.clone(),
                DocumentResponse {
                    id: doc.id.clone(),
                    attributes: doc.attributes.clone(),
                },
            );
        }
        drop(store);

        let mut vectors = self.vectors.write().await;
        let ns_vecs = vectors.entry(namespace.to_string()).or_default();
        for doc in docs {
            if let Some(vector) = doc
                .vector
                .as_ref()
                .or_else(|| doc.vectors.as_ref().and_then(|vectors| vectors.first()))
            {
                ns_vecs.insert(doc.id.clone(), vector.clone());
            }
        }
        Ok(TurbopufferWriteOutcome {
            billing: Some(serde_json::json!({
                "billable_logical_bytes_written": 0
            })),
        })
    }

    async fn patch(
        &self,
        namespace: &str,
        docs: &[PatchDoc],
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        for doc in docs {
            for (name, value) in &doc.attributes {
                validate_attr_value_input(name, value)?;
            }
        }
        let mut store = self.docs.write().await;
        let ns = store.entry(namespace.to_string()).or_default();
        for doc in docs {
            // patch_rows is documented to silently ignore non-existent ids.
            if let Some(existing) = ns.get_mut(&doc.id) {
                for (k, v) in &doc.attributes {
                    existing.attributes.insert(k.clone(), v.clone());
                }
            }
        }
        Ok(TurbopufferWriteOutcome {
            billing: Some(serde_json::json!({
                "billable_logical_bytes_written": 0
            })),
        })
    }

    async fn patch_columns(
        &self,
        namespace: &str,
        columns: &PatchColumns,
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        if self
            .patch_failure_once
            .write()
            .await
            .remove(namespace)
            .unwrap_or(false)
        {
            return Err(TurbopufferError::Other(
                "synthetic transient patch failure".into(),
            ));
        }
        let docs: Vec<PatchDoc> = columns
            .ids
            .iter()
            .enumerate()
            .map(|(idx, id)| {
                let attributes = columns
                    .columns
                    .iter()
                    .filter_map(|(name, values)| {
                        values.get(idx).map(|value| (name.clone(), value.clone()))
                    })
                    .collect();
                PatchDoc {
                    id: id.clone(),
                    attributes,
                }
            })
            .collect();
        self.patch(namespace, &docs).await
    }

    async fn delete(
        &self,
        namespace: &str,
        ids: &[String],
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        let mut store = self.docs.write().await;
        if let Some(ns) = store.get_mut(namespace) {
            for id in ids {
                ns.remove(id);
            }
        }
        Ok(TurbopufferWriteOutcome {
            billing: Some(serde_json::json!({
                "billable_logical_bytes_written": 0
            })),
        })
    }

    async fn query(
        &self,
        namespace: &str,
        _vector: &[f64],
        top_k: u32,
        filters: Option<&Value>,
        _include_attributes: Option<&IncludeAttributes>,
    ) -> Result<TurbopufferQueryOutcome, TurbopufferError> {
        // Honor a one-shot 429 arm if present (consumes the flag).
        if self
            .rate_limit_once
            .write()
            .await
            .remove(namespace)
            .unwrap_or(false)
        {
            return Err(TurbopufferError::RateLimited(format!(
                "mock-armed 429 for namespace {}",
                namespace
            )));
        }
        // The mock pretends every doc has the same dist (0.5). Use the
        // ranked-aware filter evaluator so `$dist`/`$score` pseudo-fields in
        // cursor band filters (e.g. `[$dist, Gt, 0.5]`) are honored against
        // that pretend value.
        const MOCK_DIST: f64 = 0.5;
        let store = self.docs.read().await;
        let mut results: Vec<QueryResult> = store
            .get(namespace)
            .map(|ns| {
                ns.values()
                    .filter(|doc| {
                        filters
                            .map(|filter| {
                                mock_matches_ranked_filter(
                                    &doc.id,
                                    &doc.attributes,
                                    MOCK_DIST,
                                    filter,
                                )
                            })
                            .unwrap_or(true)
                    })
                    .map(|doc| QueryResult {
                        id: doc.id.clone(),
                        numeric_id: false,
                        dist: Some(MOCK_DIST),
                        attributes: doc.attributes.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        // Sort by (dist asc, id asc) BEFORE truncating so the mock returns
        // the same "top" subset on every call — matching real turbopuffer
        // (which returns top_k by score) and making cursor pagination tests
        // deterministic.
        results.sort_by(|a, b| {
            a.dist
                .unwrap_or(0.0)
                .partial_cmp(&b.dist.unwrap_or(0.0))
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.id.cmp(&b.id))
        });
        results.truncate(top_k as usize);
        Ok(TurbopufferQueryOutcome {
            rows: results,
            billing: Some(serde_json::json!({})),
        })
    }

    async fn ranked_query(
        &self,
        namespace: &str,
        rank_by: &Value,
        top_k: u32,
        filters: Option<&Value>,
        include_attributes: Option<&IncludeAttributes>,
    ) -> Result<TurbopufferQueryOutcome, TurbopufferError> {
        let _guard = enter_counter(&self.ranked_query_active, &self.ranked_query_max_active);
        if let Some(delay) = *self.ranked_query_delay.read().await {
            tokio::time::sleep(delay).await;
        }
        self.ranked_query_filters
            .write()
            .await
            .push(filters.cloned());
        self.ranked_query_calls
            .write()
            .await
            .push(serde_json::json!({
                "namespace": namespace,
                "rank_by": rank_by,
                "top_k": top_k,
                "filters": filters,
                "include_attributes": include_attributes.map(|include| match include {
                    IncludeAttributes::All(all) => Value::Bool(*all),
                    IncludeAttributes::Fields(fields) => serde_json::json!(fields),
                }),
            }));
        // Honor a one-shot 429 arm if present (consumes the flag), so existing
        // tests that probe retry behavior keep working through this path.
        if self
            .rate_limit_once
            .write()
            .await
            .remove(namespace)
            .unwrap_or(false)
        {
            return Err(TurbopufferError::RateLimited(format!(
                "mock-armed 429 for namespace {}",
                namespace
            )));
        }

        let mode = rank_by
            .as_array()
            .and_then(|arr| arr.get(1))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let needle = rank_by
            .as_array()
            .and_then(|arr| arr.get(2))
            .and_then(|v| v.as_str())
            .map(|s| s.to_lowercase());
        let field = rank_by
            .as_array()
            .and_then(|arr| arr.first())
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let store = self.docs.read().await;
        let mut matches: Vec<QueryResult> = store
            .get(namespace)
            .map(|ns| {
                ns.values()
                    .filter(|doc| {
                        if mode.eq_ignore_ascii_case("BM25") {
                            // Toy BM25: doc matches if its target field contains the needle.
                            let Some(needle) = needle.as_deref() else {
                                return false;
                            };
                            let Some(field_value) = doc.attributes.get(field) else {
                                return false;
                            };
                            let Some(text) = field_value.as_str() else {
                                return false;
                            };
                            text.to_lowercase().contains(needle)
                        } else {
                            true
                        }
                    })
                    .filter(|doc| {
                        filters
                            .map(|filter| {
                                mock_matches_ranked_filter(
                                    &doc.id,
                                    &doc.attributes,
                                    mock_score(&doc.id),
                                    filter,
                                )
                            })
                            .unwrap_or(true)
                    })
                    .map(|doc| QueryResult {
                        id: doc.id.clone(),
                        numeric_id: false,
                        // Deterministic per-id pseudo-score lets pagination tests
                        // assert "shard saturated → recurse with score-band filter".
                        dist: Some(mock_score(&doc.id)),
                        attributes: doc.attributes.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default();

        // BM25 sorts descending by score; ANN sorts ascending by distance.
        // The mock uses one score field for both; flip the order based on mode.
        let descending = mode.eq_ignore_ascii_case("BM25");
        matches.sort_by(|a, b| {
            let (lhs, rhs) = if descending { (b, a) } else { (a, b) };
            lhs.dist
                .unwrap_or(0.0)
                .partial_cmp(&rhs.dist.unwrap_or(0.0))
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.id.cmp(&b.id))
        });
        matches.truncate(top_k as usize);
        Ok(TurbopufferQueryOutcome {
            rows: matches,
            billing: Some(serde_json::json!({})),
        })
    }

    async fn multi_ranked_query(
        &self,
        namespace: &str,
        legs: &[Value],
        rerank_by: Option<&Value>,
    ) -> Result<Value, TurbopufferError> {
        if self
            .rate_limit_once
            .write()
            .await
            .remove(namespace)
            .unwrap_or(false)
        {
            return Err(TurbopufferError::RateLimited(format!(
                "mock-armed 429 for namespace {}",
                namespace
            )));
        }

        let mut results = Vec::with_capacity(legs.len());
        for _ in legs {
            results.push(mock_query_body(self, namespace).await?);
        }

        // Fused mode: apply real RRF over the per-leg row lists so the
        // hybrid-text path is exercised with genuine fusion semantics.
        if let Some(rerank_by) = rerank_by {
            let rank_constant = rerank_by
                .get(1)
                .and_then(|opts| opts.get("rank_constant"))
                .and_then(Value::as_f64)
                .unwrap_or(60.0);
            return Ok(serde_json::json!({
                "rows": rrf_fuse(&results, rank_constant),
                "billing": {},
                "performance": {},
            }));
        }

        Ok(serde_json::json!({
            "results": results,
            "billing": {},
            "performance": {},
        }))
    }

    async fn fetch(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<DocumentResponse>, TurbopufferError> {
        let store = self.docs.read().await;
        Ok(store.get(namespace).and_then(|ns| ns.get(id)).cloned())
    }

    async fn fetch_many(
        &self,
        namespace: &str,
        ids: &[String],
    ) -> Result<HashMap<String, DocumentResponse>, TurbopufferError> {
        self.fetch_many_requests
            .fetch_add(1, AtomicOrdering::SeqCst);
        let store = self.docs.read().await;
        let mut result = HashMap::new();
        if let Some(ns) = store.get(namespace) {
            for id in ids {
                if let Some(doc) = ns.get(id) {
                    result.insert(id.clone(), doc.clone());
                }
            }
        }
        Ok(result)
    }

    async fn fetch_vector(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<Vec<f64>>, TurbopufferError> {
        let vectors = self.vectors.read().await;
        Ok(vectors.get(namespace).and_then(|ns| ns.get(id)).cloned())
    }

    async fn scan_page(
        &self,
        namespace: &str,
        cursor: Option<&str>,
        page_size: u32,
        filters: Option<&Value>,
        include_attributes: Option<&[String]>,
    ) -> Result<DocumentPage, TurbopufferError> {
        let _guard = enter_counter(&self.scan_page_active, &self.scan_page_max_active);
        if let Some(delay) = *self.scan_page_delay.read().await {
            tokio::time::sleep(delay).await;
        }
        self.scan_filters.write().await.push(filters.cloned());
        self.scan_include_attributes
            .write()
            .await
            .push(include_attributes.map(|attrs| attrs.to_vec()));
        if let Some(attrs) = include_attributes {
            let missing = self.missing_include_attributes.read().await;
            if let Some(fields) = missing.get(namespace) {
                if let Some(field) = attrs.iter().find(|field| fields.contains(*field)) {
                    return Err(TurbopufferError::Other(format!(
                        "400 Bad Request: {{\"error\":\"💔 attribute \\\"{}\\\" not found in schema, cannot be part of `include_attributes`. consider passing `include_attributes=True` to return all attribute data instead\",\"status\":\"error\"}}",
                        field
                    )));
                }
            }
        }
        let store = self.docs.read().await;
        let mut all_docs: Vec<DocumentResponse> = store
            .get(namespace)
            .map(|ns| ns.values().cloned().collect())
            .unwrap_or_default();

        // Sort by ID for deterministic pagination
        all_docs.sort_by(|a, b| a.id.cmp(&b.id));

        // Apply cursor filter
        let filtered: Vec<DocumentResponse> = all_docs
            .into_iter()
            .filter(|d| cursor.map(|c| d.id.as_str() > c).unwrap_or(true))
            .filter(|d| {
                filters
                    .map(|filter| mock_matches_filter(&d.id, &d.attributes, filter))
                    .unwrap_or(true)
            })
            .collect();

        let page_size = page_size as usize;
        let has_more = filtered.len() > page_size;
        let documents: Vec<DocumentResponse> = filtered.into_iter().take(page_size).collect();
        let next_cursor = if has_more {
            documents.last().map(|d| d.id.clone())
        } else {
            None
        };

        self.scan_returned_bytes.fetch_add(
            serde_json::to_vec(&documents)
                .expect("mock documents serialize")
                .len(),
            AtomicOrdering::SeqCst,
        );
        Ok(DocumentPage {
            documents,
            next_cursor,
        })
    }

    async fn scan_page_strong(
        &self,
        namespace: &str,
        cursor: Option<&str>,
        page_size: u32,
        filters: Option<&Value>,
        include_attributes: Option<&[String]>,
    ) -> Result<DocumentPage, TurbopufferError> {
        self.strong_scan_calls.fetch_add(1, AtomicOrdering::SeqCst);
        self.scan_page(namespace, cursor, page_size, filters, include_attributes)
            .await
    }

    async fn reconcile_change_token(
        &self,
        namespace: &str,
    ) -> Result<Option<String>, TurbopufferError> {
        if !self.reconcile_token_supported.load(AtomicOrdering::SeqCst) {
            return Ok(None);
        }
        // Test-only content identity; production adapters must provide a real
        // cheap revision, never emulate this by scanning rows.
        let docs = self.docs.read().await;
        let mut rows: Vec<_> = docs
            .get(namespace)
            .into_iter()
            .flat_map(|ns| ns.values())
            .collect();
        rows.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(Some(serde_json::json!(rows).to_string()))
    }

    async fn head_namespace(&self, namespace: &str) -> Result<NamespaceMeta, TurbopufferError> {
        self.metadata_requests.fetch_add(1, AtomicOrdering::SeqCst);
        if self.head_not_found.read().await.contains(namespace) {
            return Err(TurbopufferError::NotFound(format!(
                "404 Not Found: namespace '{}' was not found",
                namespace
            )));
        }
        if let Some(msg) = self.head_failure.read().await.get(namespace).cloned() {
            return Err(TurbopufferError::Other(msg));
        }
        // A blob set exists once a blob is stored in it, as upstream: the
        // branch path asks before branching one alongside its namespace.
        if namespace.ends_with(&blob_set_namespace(""))
            && !self.docs.read().await.contains_key(namespace)
        {
            return Err(TurbopufferError::NotFound(format!(
                "404 Not Found: namespace '{namespace}' was not found"
            )));
        }
        if let Some(raw) = self.metadata_overrides.read().await.get(namespace).cloned() {
            return Ok(parse_metadata_body(raw));
        }
        let (index_status, unindexed_bytes) = self
            .status
            .read()
            .await
            .get(namespace)
            .copied()
            .unwrap_or((IndexStatus::Unknown, None));
        let approx_row_count = self
            .docs
            .read()
            .await
            .get(namespace)
            .map(|ns| ns.len() as u64)
            .unwrap_or(0);
        // Build a `raw` body that mirrors turbopuffer's documented shape so
        // tests of the /metadata proxy route see a realistic structure.
        let mut index_obj = serde_json::Map::new();
        match index_status {
            IndexStatus::Stable => {
                index_obj.insert("status".into(), Value::String("up-to-date".into()));
            }
            IndexStatus::Updating => {
                index_obj.insert("status".into(), Value::String("updating".into()));
                if let Some(b) = unindexed_bytes {
                    index_obj.insert("unindexed_bytes".into(), Value::from(b));
                }
            }
            IndexStatus::Unknown => {}
        }
        let mut raw = serde_json::Map::new();
        raw.insert("approx_row_count".into(), Value::from(approx_row_count));
        raw.insert("approx_logical_bytes".into(), Value::from(0));
        if !index_obj.is_empty() {
            raw.insert("index".into(), Value::Object(index_obj));
        }
        Ok(NamespaceMeta {
            index_status,
            unindexed_bytes,
            approx_row_count,
            approx_logical_bytes: Some(0),
            count_settle: None,
            raw: Value::Object(raw),
        })
    }
}

async fn mock_passthrough(
    client: &MockTurbopufferClient,
    method: &str,
    path: &str,
    query: Option<&str>,
    body: Option<Value>,
) -> Result<Value, TurbopufferError> {
    let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
    match (method, parts.as_slice()) {
        ("GET", ["v1", "namespaces"]) => {
            let mut namespaces: Vec<String> = client.docs.read().await.keys().cloned().collect();
            namespaces.sort();
            let url = reqwest::Url::parse(&format!("http://mock/?{}", query.unwrap_or_default()))
                .map_err(|e| TurbopufferError::Other(e.to_string()))?;
            let params: HashMap<_, _> = url.query_pairs().into_owned().collect();
            if let Some(prefix) = params.get("prefix") {
                namespaces.retain(|name| name.starts_with(prefix));
            }
            let offset = params
                .get("cursor")
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(0);
            let page_size = params
                .get("page_size")
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(1000)
                .max(1);
            let next = (offset.saturating_add(page_size) < namespaces.len())
                .then(|| offset.saturating_add(page_size).to_string());
            Ok(serde_json::json!({
                "namespaces": namespaces.into_iter().skip(offset).take(page_size).map(|id| serde_json::json!({ "id": id })).collect::<Vec<_>>(),
                "next_cursor": next,
            }))
        }
        ("POST", ["v2", "namespaces", namespace]) => {
            let Some(body) = body else {
                return Ok(serde_json::json!({"rows_affected": 0}));
            };
            mock_write_body(client, namespace, &body).await
        }
        ("POST", ["v2", "namespaces", namespace, "query"]) => {
            let body = body.unwrap_or_else(|| serde_json::json!({}));
            if body
                .get("queries")
                .and_then(|value| value.as_array())
                .is_some()
            {
                let queries = body
                    .get("queries")
                    .and_then(|value| value.as_array())
                    .cloned()
                    .unwrap_or_default();
                let mut results = Vec::new();
                for _ in &queries {
                    results.push(mock_query_body(client, namespace).await?);
                }
                return Ok(serde_json::json!({
                    "results": results,
                    "billing": {},
                    "performance": {},
                }));
            }
            if let Some(response) = mock_native_query(client, namespace, &body).await {
                return Ok(response);
            }
            mock_query_body(client, namespace).await
        }
        ("POST", ["v2", "namespaces", _namespace, "explain_query"]) => {
            Ok(serde_json::json!({ "plan_text": "mock query plan" }))
        }
        ("DELETE", ["v2", "namespaces", namespace]) => {
            client.docs.write().await.remove(*namespace);
            Ok(serde_json::json!({ "status": "OK" }))
        }
        ("GET", ["v1", "namespaces", namespace, "metadata"]) => {
            Ok(client.head_namespace(namespace).await?.raw)
        }
        ("PATCH", ["v1", "namespaces", namespace, "metadata"]) => {
            let mut meta = client.head_namespace(namespace).await?.raw;
            if let Some(pinning) = body
                .as_ref()
                .and_then(|value| value.get("pinning"))
                .cloned()
            {
                if let Some(obj) = meta.as_object_mut() {
                    obj.insert("pinning".to_string(), pinning);
                }
            }
            Ok(meta)
        }
        ("GET", ["v1", "namespaces", namespace, "hint_cache_warm"]) => {
            client.hint_cache_warm(namespace).await?;
            Ok(serde_json::json!({
                "status": "ACCEPTED",
                "message": "cache warm hint accepted",
            }))
        }
        ("GET", ["v1", "namespaces", namespace, "schema"]) => {
            Ok(client.head_namespace(namespace).await?.raw)
        }
        ("POST", ["v1", "namespaces", _namespace, "schema"]) => {
            Ok(serde_json::json!({ "schema": body.unwrap_or_else(|| serde_json::json!({})) }))
        }
        ("POST", ["v1", "namespaces", _namespace, "_debug", "recall"]) => Ok(serde_json::json!({
            "avg_ann_count": 10.0,
            "avg_exhaustive_count": 10.0,
            "avg_recall": 1.0,
        })),
        _ => Err(TurbopufferError::Other(format!(
            "mock passthrough unsupported: {} {}",
            method, path
        ))),
    }
}

/// The source namespace a `branch_from_namespace` or `copy_from_namespace`
/// value names: turbopuffer's string form, or `{"source_namespace": …}`.
pub fn branch_source_namespace(value: &Value) -> Option<String> {
    match value {
        Value::String(source) => Some(source.clone()),
        Value::Object(object) => object
            .get("source_namespace")
            .and_then(Value::as_str)
            .map(str::to_string),
        _ => None,
    }
}

fn mock_response(status: u16, message: String) -> TurbopufferError {
    TurbopufferError::Response(TurbopufferPassthroughResponse {
        status,
        content_type: Some("application/json".to_string()),
        body: serde_json::to_vec(&serde_json::json!({"status": "error", "error": message}))
            .expect("static response serializes"),
    })
}

/// Branch or copy as turbopuffer does: the destination must be empty, the
/// source must exist, and the two are independent afterwards.
async fn mock_clone_namespace(
    client: &MockTurbopufferClient,
    source: &str,
    target: &str,
) -> Result<(), TurbopufferError> {
    let mut docs = client.docs.write().await;
    if docs.get(target).is_some_and(|rows| !rows.is_empty()) {
        return Err(mock_response(
            400,
            format!("destination namespace {target} must be empty"),
        ));
    }
    let Some(rows) = docs.get(source).cloned() else {
        return Err(mock_response(404, format!("namespace {source} not found")));
    };
    docs.insert(target.to_string(), rows);
    drop(docs);
    let mut vectors = client.vectors.write().await;
    if let Some(source_vectors) = vectors.get(source).cloned() {
        vectors.insert(target.to_string(), source_vectors);
    }
    Ok(())
}

async fn mock_write_body(
    client: &MockTurbopufferClient,
    namespace: &str,
    body: &Value,
) -> Result<Value, TurbopufferError> {
    let Some(obj) = body.as_object() else {
        return Ok(serde_json::json!({"rows_affected": 0}));
    };

    let mut rows_affected = 0usize;
    let return_affected_ids = obj
        .get("return_affected_ids")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let mut upserted_ids = Vec::new();
    let mut patched_ids = Vec::new();
    let mut deleted_ids = Vec::new();

    if let Some(rows) = obj.get("upsert_rows").and_then(|value| value.as_array()) {
        let mut store = client.docs.write().await;
        let ns = store.entry(namespace.to_string()).or_default();
        for row in rows {
            let Some(row_obj) = row.as_object() else {
                continue;
            };
            let Some(id) = row_obj.get("id").map(mock_id_to_string) else {
                continue;
            };
            for (name, value) in row_obj {
                if !is_system_column(name) {
                    validate_attr_value_input(name, value)?;
                }
            }
            let mut attributes = HashMap::new();
            for (key, value) in row_obj {
                if !is_system_column(key) {
                    attributes.insert(key.clone(), value.clone());
                }
            }
            ns.insert(
                id.clone(),
                DocumentResponse {
                    id: id.clone(),
                    attributes,
                },
            );
            // Keep the mock's vector readback faithful to supported wire
            // upserts, so output patch regressions can prove preservation.
            if let Some(vector) = row_obj
                .get("vector")
                .and_then(Value::as_array)
                .and_then(|values| values.iter().map(Value::as_f64).collect::<Option<Vec<_>>>())
            {
                client
                    .vectors
                    .write()
                    .await
                    .entry(namespace.to_string())
                    .or_default()
                    .insert(id.clone(), vector);
            }
            upserted_ids.push(Value::String(id));
            rows_affected += 1;
        }
    }

    if let Some(columns) = obj
        .get("upsert_columns")
        .and_then(|value| value.as_object())
    {
        let ids = columns
            .get("id")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();
        let mut store = client.docs.write().await;
        let ns = store.entry(namespace.to_string()).or_default();
        for (index, id_value) in ids.iter().enumerate() {
            let id = mock_id_to_string(id_value);
            let mut attributes = HashMap::new();
            for (key, column) in columns {
                if is_system_column(key) {
                    continue;
                }
                if let Some(value) = column.as_array().and_then(|values| values.get(index)) {
                    validate_attr_value_input(key, value)?;
                    attributes.insert(key.clone(), value.clone());
                }
            }
            ns.insert(
                id.clone(),
                DocumentResponse {
                    id: id.clone(),
                    attributes,
                },
            );
            upserted_ids.push(Value::String(id));
            rows_affected += 1;
        }
    }

    if let Some(rows) = obj.get("patch_rows").and_then(|value| value.as_array()) {
        let mut store = client.docs.write().await;
        let ns = store.entry(namespace.to_string()).or_default();
        for row in rows {
            let Some(row_obj) = row.as_object() else {
                continue;
            };
            let Some(id) = row_obj.get("id").map(mock_id_to_string) else {
                continue;
            };
            if let Some(existing) = ns.get_mut(&id) {
                for (key, value) in row_obj {
                    if !is_system_column(key) {
                        validate_attr_value_input(key, value)?;
                        existing.attributes.insert(key.clone(), value.clone());
                    }
                }
                patched_ids.push(Value::String(id));
                rows_affected += 1;
            }
        }
    }

    if let Some(columns) = obj.get("patch_columns").and_then(|value| value.as_object()) {
        let ids = columns
            .get("id")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();
        let mut store = client.docs.write().await;
        let ns = store.entry(namespace.to_string()).or_default();
        for (index, id_value) in ids.iter().enumerate() {
            let id = mock_id_to_string(id_value);
            if let Some(existing) = ns.get_mut(&id) {
                for (key, column) in columns {
                    if is_system_column(key) {
                        continue;
                    }
                    if let Some(value) = column.as_array().and_then(|values| values.get(index)) {
                        validate_attr_value_input(key, value)?;
                        existing.attributes.insert(key.clone(), value.clone());
                    }
                }
                patched_ids.push(Value::String(id));
                rows_affected += 1;
            }
        }
    }

    if let Some(ids) = obj.get("deletes").and_then(|value| value.as_array()) {
        let mut store = client.docs.write().await;
        if let Some(ns) = store.get_mut(namespace) {
            for id in ids {
                let id = mock_id_to_string(id);
                if ns.remove(&id).is_some() {
                    deleted_ids.push(Value::String(id));
                    rows_affected += 1;
                }
            }
        }
    }

    if let Some(source) = ["branch_from_namespace", "copy_from_namespace"]
        .iter()
        .find_map(|key| obj.get(*key))
        .and_then(branch_source_namespace)
    {
        mock_clone_namespace(client, &source, namespace).await?;
    }

    if let Some(filter) = obj.get("delete_by_filter") {
        let mut store = client.docs.write().await;
        if let Some(ns) = store.get_mut(namespace) {
            let ids: Vec<String> = ns
                .iter()
                .filter(|(id, doc)| mock_matches_filter(id, &doc.attributes, filter))
                .map(|(id, _)| id.clone())
                .collect();
            for id in ids {
                ns.remove(&id);
                deleted_ids.push(Value::String(id));
                rows_affected += 1;
            }
        }
    }

    if let Some(patch_by_filter) = obj.get("patch_by_filter").and_then(Value::as_object) {
        if let (Some(filter), Some(patch)) = (
            patch_by_filter.get("filters"),
            patch_by_filter.get("patch").and_then(Value::as_object),
        ) {
            let mut store = client.docs.write().await;
            if let Some(ns) = store.get_mut(namespace) {
                for doc in ns.values_mut() {
                    if !mock_matches_filter(&doc.id, &doc.attributes, filter) {
                        continue;
                    }
                    for (key, value) in patch {
                        if !is_system_column(key) {
                            validate_attr_value_input(key, value)?;
                            doc.attributes.insert(key.clone(), value.clone());
                        }
                    }
                    patched_ids.push(Value::String(doc.id.clone()));
                    rows_affected += 1;
                }
            }
        }
    }

    let mut response = serde_json::json!({
        "status": "OK",
        "message": "mock write accepted",
        "rows_affected": rows_affected,
        "billing": {
            "billable_logical_bytes_written": 0
        }
    });
    // Like Turbopuffer, per-kind counts are present only when non-zero.
    if let Some(obj) = response.as_object_mut() {
        for (key, count) in [
            ("rows_upserted", upserted_ids.len()),
            ("rows_patched", patched_ids.len()),
            ("rows_deleted", deleted_ids.len()),
        ] {
            if count > 0 {
                obj.insert(key.to_string(), Value::from(count));
            }
        }
    }
    if return_affected_ids {
        if let Some(obj) = response.as_object_mut() {
            obj.insert("upserted_ids".to_string(), Value::Array(upserted_ids));
            obj.insert("patched_ids".to_string(), Value::Array(patched_ids));
            obj.insert("deleted_ids".to_string(), Value::Array(deleted_ids));
        }
    }
    Ok(response)
}

/// Mock bytes a native query bills, as upstream's floor does for a small namespace.
pub const MOCK_NATIVE_QUERY_BILLED_BYTES: u64 = 1_280_000_000;

fn mock_filter_matches(filter: &Value, doc: &DocumentResponse) -> bool {
    let Some(parts) = filter.as_array() else {
        return true;
    };
    match (parts.first().and_then(Value::as_str), parts.get(1)) {
        (Some("And"), Some(Value::Array(all))) => all.iter().all(|f| mock_filter_matches(f, doc)),
        (Some("Or"), Some(Value::Array(any))) => any.iter().any(|f| mock_filter_matches(f, doc)),
        (Some(attr), Some(Value::String(op))) => {
            let value = if attr == "id" {
                Some(Value::String(doc.id.clone()))
            } else {
                doc.attributes.get(attr).cloned()
            }
            .unwrap_or(Value::Null);
            let want = parts.get(2).cloned().unwrap_or(Value::Null);
            let order = match (value.as_f64(), want.as_f64()) {
                (Some(a), Some(b)) => a.partial_cmp(&b),
                _ => value.as_str().zip(want.as_str()).map(|(a, b)| a.cmp(b)),
            };
            match op.as_str() {
                "Eq" => value == want,
                "NotEq" => value != want,
                "Gte" => order.is_some_and(|o| o.is_ge()),
                "Gt" => order.is_some_and(|o| o.is_gt()),
                "Lte" => order.is_some_and(|o| o.is_le()),
                "Lt" => order.is_some_and(|o| o.is_lt()),
                _ => true,
            }
        }
        _ => true,
    }
}

/// The native `aggregate_by` / `group_by` and attribute-ordered query forms,
/// evaluated over the mock's documents. `None` leaves every other body to
/// the row-listing mock.
async fn mock_native_query(
    client: &MockTurbopufferClient,
    namespace: &str,
    body: &Value,
) -> Option<Value> {
    let docs = client.docs.read().await;
    let matching: Vec<&DocumentResponse> = docs
        .get(namespace)
        .map(|ns| ns.values().collect())
        .unwrap_or_default();
    let matching: Vec<&DocumentResponse> = matching
        .into_iter()
        .filter(|doc| {
            body.get("filters")
                .is_none_or(|filter| mock_filter_matches(filter, doc))
        })
        .collect();
    let top_k = body.get("top_k").and_then(Value::as_u64).unwrap_or(1200) as usize;
    let billing = |returned: usize| {
        serde_json::json!({
            "billable_logical_bytes_queried": MOCK_NATIVE_QUERY_BILLED_BYTES,
            "billable_logical_bytes_returned": returned as u64,
        })
    };
    if let Some(aggregates) = body.get("aggregate_by").and_then(Value::as_object) {
        let group_by: Vec<&str> = body
            .get("group_by")
            .and_then(Value::as_array)
            .map(|g| g.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        let key = |doc: &DocumentResponse| -> Vec<Value> {
            group_by
                .iter()
                .map(|attr| doc.attributes.get(*attr).cloned().unwrap_or(Value::Null))
                .collect()
        };
        let mut groups: Vec<(Vec<Value>, Vec<&DocumentResponse>)> = Vec::new();
        for doc in matching {
            let k = key(doc);
            match groups.iter_mut().find(|(existing, _)| *existing == k) {
                Some((_, members)) => members.push(doc),
                None => groups.push((k, vec![doc])),
            }
        }
        groups.sort_by_key(|(k, _)| serde_json::to_string(k).unwrap_or_default());
        let computed: Vec<Value> = groups
            .iter()
            .take(top_k)
            .map(|(k, members)| {
                let mut row = serde_json::Map::new();
                for (attr, value) in group_by.iter().zip(k) {
                    row.insert((*attr).to_string(), value.clone());
                }
                for (name, spec) in aggregates {
                    let value = match spec.get(0).and_then(Value::as_str) {
                        Some("Count") => Value::from(members.len() as u64),
                        Some("Sum") => {
                            let attr = spec.get(1).and_then(Value::as_str).unwrap_or_default();
                            let sum: f64 = members
                                .iter()
                                .filter_map(|d| d.attributes.get(attr).and_then(Value::as_f64))
                                .sum();
                            if sum.fract() == 0.0 {
                                Value::from(sum as i64)
                            } else {
                                Value::from(sum)
                            }
                        }
                        _ => Value::Null,
                    };
                    row.insert(name.clone(), value);
                }
                Value::Object(row)
            })
            .collect();
        let mut response = if group_by.is_empty() {
            let totals = computed
                .into_iter()
                .next()
                .unwrap_or_else(|| serde_json::json!({}));
            serde_json::json!({ "aggregations": totals })
        } else {
            serde_json::json!({ "aggregation_groups": computed })
        };
        response["billing"] = billing(64);
        return Some(response);
    }
    let order = body.get("rank_by").and_then(Value::as_array)?;
    let (attr, direction) = (order.first()?.as_str()?, order.get(1)?.as_str()?);
    if attr == "id" || !matches!(direction, "asc" | "desc") {
        return None;
    }
    let mut rows: Vec<&DocumentResponse> = matching;
    rows.sort_by(|a, b| {
        let (a, b) = (
            a.attributes.get(attr).and_then(Value::as_f64),
            b.attributes.get(attr).and_then(Value::as_f64),
        );
        a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal)
    });
    if direction == "desc" {
        rows.reverse();
    }
    let rows: Vec<Value> = rows
        .into_iter()
        .take(top_k)
        .map(|doc| {
            let mut row = serde_json::Map::new();
            row.insert("id".into(), Value::String(doc.id.clone()));
            if let Some(value) = doc.attributes.get(attr) {
                row.insert(attr.to_string(), value.clone());
            }
            Value::Object(row)
        })
        .collect();
    Some(serde_json::json!({ "rows": rows, "billing": billing(32) }))
}

async fn mock_query_body(
    client: &MockTurbopufferClient,
    namespace: &str,
) -> Result<Value, TurbopufferError> {
    let docs = client.docs.read().await;
    let rows = docs
        .get(namespace)
        .map(|ns| {
            let mut rows: Vec<Value> = ns
                .values()
                .map(|doc| {
                    let mut row = serde_json::Map::new();
                    row.insert("id".into(), Value::String(doc.id.clone()));
                    row.insert("$dist".into(), Value::from(mock_score(&doc.id)));
                    for (key, value) in &doc.attributes {
                        row.insert(key.clone(), value.clone());
                    }
                    Value::Object(row)
                })
                .collect();
            rows.sort_by(|a, b| {
                a.get("id")
                    .and_then(|value| value.as_str())
                    .cmp(&b.get("id").and_then(|value| value.as_str()))
            });
            rows
        })
        .unwrap_or_default();

    Ok(serde_json::json!({
        "rows": rows,
        "billing": {},
        "performance": {},
    }))
}

fn mock_id_to_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        other => other.to_string(),
    }
}

/// JSON attribute key used by the gateway to stamp the server-assigned upsert
/// timestamp (epoch ms, u64). Filterable in Turbopuffer.
pub const UPSERTED_AT_ATTR: &str = "_hevlayer_upserted_at";
/// Collision-resistant row revision shared by gateway and Function writers.
pub const WRITE_REVISION_ATTR: &str = "_hevlayer_write_revision";

/// Deterministic per-id pseudo-score used by the mock's `ranked_query` so
/// score-band pagination tests can assert behavior without a real ranker.
/// Reciprocal rank fusion over per-leg mock query bodies: each row scores
/// `Σ 1/(rank_constant + rank)` across the legs that returned it (rank is
/// 1-based within a leg). Returns fused rows sorted by `$score` descending
/// with `id` as tiebreaker, mirroring upstream's fused multi-query response.
fn rrf_fuse(leg_bodies: &[Value], rank_constant: f64) -> Vec<Value> {
    let mut scores: HashMap<String, (f64, Value)> = HashMap::new();
    for body in leg_bodies {
        let rows = body.get("rows").and_then(Value::as_array);
        for (rank, row) in rows.into_iter().flatten().enumerate() {
            let Some(id) = row.get("id").and_then(Value::as_str) else {
                continue;
            };
            let contribution = 1.0 / (rank_constant + (rank as f64) + 1.0);
            let entry = scores
                .entry(id.to_string())
                .or_insert_with(|| (0.0, row.clone()));
            entry.0 += contribution;
        }
    }
    let mut fused: Vec<(String, f64, Value)> = scores
        .into_iter()
        .map(|(id, (score, row))| (id, score, row))
        .collect();
    fused.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    fused
        .into_iter()
        .map(|(_, score, row)| {
            let mut row = row;
            if let Some(obj) = row.as_object_mut() {
                obj.remove("$dist");
                obj.insert("$score".to_string(), Value::from(score));
            }
            row
        })
        .collect()
}

pub(crate) fn mock_score(id: &str) -> f64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    id.hash(&mut hasher);
    let h = hasher.finish();
    // Spread across [0, 1.0) so float comparisons in tests stay legible.
    (h as f64) / (u64::MAX as f64)
}

/// Extension of `mock_matches_filter` that also resolves `$dist` / `$score`
/// pseudo-fields against the supplied score. Lets `ranked_query` honor
/// pagination filters of the form `["$dist", "Gt", last_dist]`.
fn mock_matches_ranked_filter(
    id: &str,
    attrs: &HashMap<String, Value>,
    score: f64,
    filter: &Value,
) -> bool {
    let Some(arr) = filter.as_array() else {
        return false;
    };
    let Some(head) = arr.first().and_then(|v| v.as_str()) else {
        return false;
    };

    match head {
        "And" | "and" => arr
            .get(1)
            .and_then(|v| v.as_array())
            .map(|filters| {
                filters
                    .iter()
                    .all(|f| mock_matches_ranked_filter(id, attrs, score, f))
            })
            .unwrap_or(false),
        "Or" | "or" => arr
            .get(1)
            .and_then(|v| v.as_array())
            .map(|filters| {
                filters
                    .iter()
                    .any(|f| mock_matches_ranked_filter(id, attrs, score, f))
            })
            .unwrap_or(false),
        "Not" | "not" => arr
            .get(1)
            .map(|filter| !mock_matches_ranked_filter(id, attrs, score, filter))
            .unwrap_or(false),
        "$dist" | "$score" => {
            let Some(op) = arr.get(1).and_then(|v| v.as_str()) else {
                return false;
            };
            let Some(expected) = arr.get(2).and_then(|v| v.as_f64()) else {
                return false;
            };
            match op {
                op if op.eq_ignore_ascii_case("Eq") => score == expected,
                op if op.eq_ignore_ascii_case("NotEq") => score != expected,
                op if op.eq_ignore_ascii_case("Gt") => score > expected,
                op if op.eq_ignore_ascii_case("Gte") => score >= expected,
                op if op.eq_ignore_ascii_case("Lt") => score < expected,
                op if op.eq_ignore_ascii_case("Lte") => score <= expected,
                _ => false,
            }
        }
        _ => mock_matches_filter(id, attrs, filter),
    }
}

fn mock_matches_filter(id: &str, attrs: &HashMap<String, Value>, filter: &Value) -> bool {
    let Some(arr) = filter.as_array() else {
        return false;
    };
    let Some(head) = arr.first().and_then(|v| v.as_str()) else {
        return false;
    };

    match head {
        "And" | "and" => arr
            .get(1)
            .and_then(|v| v.as_array())
            .map(|filters| filters.iter().all(|f| mock_matches_filter(id, attrs, f)))
            .unwrap_or(false),
        "Or" | "or" => arr
            .get(1)
            .and_then(|v| v.as_array())
            .map(|filters| filters.iter().any(|f| mock_matches_filter(id, attrs, f)))
            .unwrap_or(false),
        "Not" | "not" => arr
            .get(1)
            .map(|filter| !mock_matches_filter(id, attrs, filter))
            .unwrap_or(false),
        field => {
            let Some(op) = arr.get(1).and_then(|v| v.as_str()) else {
                return false;
            };
            let Some(expected) = arr.get(2) else {
                return false;
            };
            let id_value = Value::String(id.to_string());
            let actual = if field == "id" {
                Some(&id_value)
            } else {
                attrs.get(field)
            };
            if op.eq_ignore_ascii_case("Fuzzy") {
                return mock_fuzzy_match(actual, expected, arr.get(3));
            }
            mock_compare_filter(actual, op, expected)
        }
    }
}

/// Mock `Fuzzy` semantics: true when any whitespace-split word of the field
/// value is within `max_edit_distance` Levenshtein edits of the query token
/// (case-insensitive, punctuation trimmed). Close enough to upstream for
/// tests to exercise real fuzzy-leg filtering.
fn mock_fuzzy_match(actual: Option<&Value>, expected: &Value, opts: Option<&Value>) -> bool {
    let Some(token) = expected.as_str() else {
        return false;
    };
    let Some(text) = actual.and_then(Value::as_str) else {
        return false;
    };
    let token = token.to_lowercase();
    let Some(max_edits) = resolve_max_edits(opts, token.chars().count()) else {
        return false;
    };
    text.to_lowercase()
        .split_whitespace()
        .map(|word| word.trim_matches(|c: char| !c.is_alphanumeric()))
        .any(|word| !word.is_empty() && levenshtein(word, &token) <= max_edits)
}

/// Resolve the edit budget for a query token. Accepts the legacy integer
/// `max_edit_distance` and the current Turbopuffer ladder of
/// `{min_query_chars, distance}` rules: the rule with the largest
/// `min_query_chars` not exceeding the token length wins, and a token shorter
/// than every threshold has no budget (matches exactly), mirroring upstream.
fn resolve_max_edits(opts: Option<&Value>, token_chars: usize) -> Option<usize> {
    let value = opts?.get("max_edit_distance")?;
    if let Some(n) = value.as_u64() {
        return Some(n as usize);
    }
    let mut best: Option<(u64, usize)> = None;
    for rule in value.as_array()? {
        let min_chars = rule.get("min_query_chars").and_then(Value::as_u64)?;
        let distance = rule.get("distance").and_then(Value::as_u64)? as usize;
        if token_chars as u64 >= min_chars && best.is_none_or(|(bm, _)| min_chars >= bm) {
            best = Some((min_chars, distance));
        }
    }
    best.map(|(_, distance)| distance)
}

fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    let mut current = vec![0usize; b.len() + 1];
    for (i, ca) in a.iter().enumerate() {
        current[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let substitution = prev[j] + usize::from(ca != cb);
            current[j + 1] = substitution.min(prev[j + 1] + 1).min(current[j] + 1);
        }
        std::mem::swap(&mut prev, &mut current);
    }
    prev[b.len()]
}

fn mock_compare_filter(actual: Option<&Value>, op: &str, expected: &Value) -> bool {
    match op {
        op if op.eq_ignore_ascii_case("Exists") => actual.is_some(),
        op if op.eq_ignore_ascii_case("NotExists") => actual.is_none(),
        // Upstream treats a missing attribute as null.
        op if op.eq_ignore_ascii_case("Eq") => {
            actual == Some(expected) || (expected.is_null() && actual.is_none())
        }
        op if op.eq_ignore_ascii_case("NotEq") => {
            actual != Some(expected) && !(expected.is_null() && actual.is_none())
        }
        op if op.eq_ignore_ascii_case("In") => expected
            .as_array()
            .map(|values| actual.is_some_and(|actual| values.iter().any(|v| v == actual)))
            .unwrap_or(false),
        op if op.eq_ignore_ascii_case("NotIn") => expected
            .as_array()
            .map(|values| actual.is_some_and(|actual| values.iter().all(|v| v != actual)))
            .unwrap_or(false),
        op if op.eq_ignore_ascii_case("Gt") => compare_json(actual, expected)
            .map(|ordering| ordering == Ordering::Greater)
            .unwrap_or(false),
        op if op.eq_ignore_ascii_case("Gte") => compare_json(actual, expected)
            .map(|ordering| matches!(ordering, Ordering::Greater | Ordering::Equal))
            .unwrap_or(false),
        op if op.eq_ignore_ascii_case("Lt") => compare_json(actual, expected)
            .map(|ordering| ordering == Ordering::Less)
            .unwrap_or(false),
        op if op.eq_ignore_ascii_case("Lte") => compare_json(actual, expected)
            .map(|ordering| matches!(ordering, Ordering::Less | Ordering::Equal))
            .unwrap_or(false),
        _ => false,
    }
}

fn compare_json(actual: Option<&Value>, expected: &Value) -> Option<Ordering> {
    let actual = actual?;
    if let (Some(a), Some(b)) = (actual.as_f64(), expected.as_f64()) {
        return a.partial_cmp(&b);
    }
    if let (Some(a), Some(b)) = (actual.as_str(), expected.as_str()) {
        return Some(a.cmp(b));
    }
    None
}

#[cfg(test)]
mod metadata_parse_tests {
    use super::*;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn metadata_http_dispatch_obeys_nested_permits_and_scope_restoration() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = HttpTurbopufferClient::new("fixture", &format!("http://{addr}"));
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let outer_calls = calls.clone();
        let outer: QueryPermit = Arc::new(move |namespace, count| {
            outer_calls
                .lock()
                .unwrap()
                .push(("function", namespace, count));
            Box::pin(async { Ok(()) })
        });
        let inner_calls = calls.clone();
        let inner: QueryPermit = Arc::new(move |namespace, count| {
            inner_calls
                .lock()
                .unwrap()
                .push(("expense", namespace, count));
            Box::pin(async { Err(TurbopufferError::QueryBudgetExhausted) })
        });
        scope_query_permit(outer, async {
            let denied = scope_query_permit(inner, client.head_namespace("demo")).await;
            assert!(matches!(denied, Err(TurbopufferError::QueryBudgetExhausted)));
            assert!(tokio::time::timeout(std::time::Duration::from_millis(25), listener.accept())
                .await.is_err(), "denied metadata must not dispatch");
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buf = [0; 4096];
                let n = socket.read(&mut buf).await.unwrap();
                assert!(String::from_utf8_lossy(&buf[..n]).starts_with("GET /v2/namespaces/demo/metadata "));
                let body = r#"{"billing":{"billable_bytes_queried":123},"approx_row_count":0}"#;
                socket.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}", body.len(), body).as_bytes()).await.unwrap();
            });
            let billing = Arc::new(std::sync::Mutex::new(Vec::new()));
            let seen = billing.clone();
            scope_read_billing(Arc::new(move |namespace, value| {
                seen.lock().unwrap().push((namespace.to_owned(), value.clone()));
            }), client.head_namespace("demo")).await.unwrap();
            server.await.unwrap();
            assert_eq!(*billing.lock().unwrap(), vec![("demo".into(), json!({"billable_bytes_queried":123}))]);
        }).await;
        assert_eq!(
            *calls.lock().unwrap(),
            vec![
                ("function", "demo".into(), 1),
                ("expense", "demo".into(), 1),
                ("function", "demo".into(), 1),
            ]
        );
    }

    #[tokio::test]
    async fn nested_query_permits_retain_function_admission_and_fail_closed() {
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let first_calls = calls.clone();
        let function: QueryPermit = Arc::new(move |namespace, count| {
            first_calls
                .lock()
                .unwrap()
                .push(("function", namespace, count));
            Box::pin(async { Ok(()) })
        });
        let second_calls = calls.clone();
        let spend: QueryPermit = Arc::new(move |namespace, count| {
            second_calls
                .lock()
                .unwrap()
                .push(("spend", namespace, count));
            Box::pin(async { Err(TurbopufferError::QueryBudgetExhausted) })
        });
        scope_query_permit(function, async {
            let result = scope_query_permit(spend, reserve_provider_query("pages", 1)).await;
            assert!(matches!(
                result,
                Err(TurbopufferError::QueryBudgetExhausted)
            ));
            // Leaving the inner scope restores the original Function permit.
            reserve_provider_query("pages", 2).await.unwrap();
        })
        .await;
        assert_eq!(
            *calls.lock().unwrap(),
            vec![
                ("function", "pages".into(), 1),
                ("spend", "pages".into(), 1),
                ("function", "pages".into(), 2),
            ]
        );
    }

    #[test]
    fn typed_mutation_normalizes_upsert_and_patch_columns() {
        let mut body = json!({
            "upsert_columns": {"id": ["2", "18446744073709551615"]},
            "patch_columns": {"id": ["3"]}
        });
        normalize_source_ids(&mut body, true).unwrap();
        assert_eq!(body["upsert_columns"]["id"], json!([2, u64::MAX]));
        assert_eq!(body["patch_columns"]["id"], json!([3]));
        let mut noncanonical = json!({"upsert_columns": {"id": ["02"]}});
        assert!(normalize_source_ids(&mut noncanonical, true).is_err());
        let mut wrong_type = json!({"upsert_columns": {"id": [2]}});
        assert!(normalize_source_ids(&mut wrong_type, false).is_err());
    }

    #[test]
    fn up_to_date_status_yields_stable_and_no_unindexed_bytes() {
        // Matches the live response from a quiet `amazon-products` namespace.
        let body = json!({
            "index": { "status": "up-to-date" },
            "approx_row_count": 1
        });
        let meta = parse_metadata_body(body);
        assert_eq!(meta.index_status, IndexStatus::Stable);
        assert_eq!(meta.unindexed_bytes, None);
        assert!(meta.is_stable());
    }

    #[test]
    fn live_metadata_count_can_lag_while_index_is_up_to_date() {
        // Synthetic live upstream response immediately after committing two
        // rows, then again 30 seconds later. Stable indexing is not a count
        // freshness guarantee; preserve the reported approximate count.
        let mut body = json!({
            "approx_logical_bytes": 0,
            "approx_row_count": 0,
            "encryption": {"mode": "default"},
            "index": {"status": "up-to-date"},
            "schema": {
                "id": {"type": "string"},
                "text": {"type": "string", "filterable": false,
                         "full_text_search": {"tokenizer": "word_v4", "b": 0.75, "k1": 1.2}}
            }
        });
        let fresh = parse_metadata_body(body.clone());
        assert_eq!(fresh.approx_row_count, 0);
        assert!(fresh.is_stable());
        body["approx_row_count"] = json!(2);
        body["approx_logical_bytes"] = json!(13);
        let settled = parse_metadata_body(body);
        assert_eq!(settled.approx_row_count, 2);
        assert_eq!(settled.approx_logical_bytes, Some(13));
        assert!(settled.is_stable());
    }

    #[test]
    fn updating_status_yields_updating_and_reads_nested_bytes() {
        let body = json!({
            "index": { "status": "updating", "unindexed_bytes": 4096u64 },
            "approx_row_count": 1234
        });
        let meta = parse_metadata_body(body);
        assert_eq!(meta.index_status, IndexStatus::Updating);
        assert_eq!(meta.unindexed_bytes, Some(4096));
        assert!(!meta.is_stable());
    }

    #[test]
    fn missing_index_block_with_no_signal_is_unknown() {
        // Defensive regression: legacy `unwrap_or(0)` parsing treated this
        // as "fully indexed". The new contract is `Unknown`, which the
        // watcher refuses to advance against.
        let meta = parse_metadata_body(json!({"approx_row_count": 0}));
        assert_eq!(meta.index_status, IndexStatus::Unknown);
        assert_eq!(meta.unindexed_bytes, None);
        assert!(!meta.is_stable());
    }

    #[test]
    fn missing_status_with_nonzero_unindexed_bytes_anywhere_is_updating() {
        // Fallback path for unknown-shape responses: if the recursive scan
        // finds any `unindexed_bytes > 0`, we err on the side of "updating".
        let meta = parse_metadata_body(json!({
            "approx_row_count": 7,
            "some_future_block": { "unindexed_bytes": 1 }
        }));
        assert_eq!(meta.index_status, IndexStatus::Updating);
        assert!(!meta.is_stable());
    }

    #[test]
    fn legacy_top_level_unindexed_bytes_is_picked_up() {
        // Pre-`index` API shape from older turbopuffer versions.
        let meta = parse_metadata_body(json!({
            "approx_row_count": 7,
            "unindexed_bytes": 2048u64
        }));
        assert_eq!(meta.unindexed_bytes, Some(2048));
        assert_eq!(meta.index_status, IndexStatus::Updating);
    }

    #[tokio::test]
    async fn routing_client_uses_namespace_store_ref_map() {
        let default = Arc::new(MockTurbopufferClient::new());
        let secondary = Arc::new(MockTurbopufferClient::new());
        let refs = Arc::new(RwLock::new(HashMap::from([(
            "products".to_string(),
            "secondary".to_string(),
        )])));
        let clients: HashMap<String, Arc<dyn TurbopufferClient>> = HashMap::from([
            (
                "default".to_string(),
                default.clone() as Arc<dyn TurbopufferClient>,
            ),
            (
                "secondary".to_string(),
                secondary.clone() as Arc<dyn TurbopufferClient>,
            ),
        ]);
        let routing = RoutingTurbopufferClient::new("default".to_string(), clients, refs);

        routing
            .upsert(
                "products",
                &[UpsertDoc {
                    id: "p1".to_string(),
                    vector: None,
                    vectors: None,
                    attributes: HashMap::from([("title".to_string(), json!("secondary"))]),
                }],
            )
            .await
            .unwrap();

        assert!(default.fetch("products", "p1").await.unwrap().is_none());
        assert!(secondary.fetch("products", "p1").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn routing_client_defaults_unmapped_namespaces_to_default_store() {
        let default = Arc::new(MockTurbopufferClient::new());
        let secondary = Arc::new(MockTurbopufferClient::new());
        let refs = Arc::new(RwLock::new(HashMap::new()));
        let clients: HashMap<String, Arc<dyn TurbopufferClient>> = HashMap::from([
            (
                "default".to_string(),
                default.clone() as Arc<dyn TurbopufferClient>,
            ),
            (
                "secondary".to_string(),
                secondary.clone() as Arc<dyn TurbopufferClient>,
            ),
        ]);
        let routing = RoutingTurbopufferClient::new("default".to_string(), clients, refs);

        routing
            .upsert(
                "products",
                &[UpsertDoc {
                    id: "p1".to_string(),
                    vector: None,
                    vectors: None,
                    attributes: HashMap::new(),
                }],
            )
            .await
            .unwrap();

        assert!(default.fetch("products", "p1").await.unwrap().is_some());
        assert!(secondary.fetch("products", "p1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn keyless_http_client_uses_request_scoped_bearer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0; 4096];
            let n = socket.read(&mut buf).await.unwrap();
            let request = String::from_utf8_lossy(&buf[..n]).to_string();
            let body = r#"{"index":{"status":"up-to-date"},"approx_row_count":0}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            request
        });

        let client = HttpTurbopufferClient::new("", &format!("http://{addr}"));
        scope_upstream_api_key(
            "tpuf_request_token".to_string(),
            client.head_namespace("demo"),
        )
        .await
        .unwrap();

        let request = server.await.unwrap();
        assert!(request.contains("authorization: Bearer tpuf_request_token"));
    }
}
