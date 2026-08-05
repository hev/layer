use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;

use crate::models::{
    DocumentPage, DocumentResponse, FieldValueResult, IncludeAttributes, QueryResult,
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

#[derive(Debug, thiserror::Error)]
pub enum TurbopufferError {
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
        Err(TurbopufferError::Other(
            "UnsupportedByStore: delete_by_filter".to_string(),
        ))
    }

    async fn import_arrow(
        &self,
        _namespace: &str,
        _content_type: &str,
        _body: Vec<u8>,
    ) -> Result<TurbopufferPassthroughResponse, TurbopufferError> {
        Err(TurbopufferError::Other(
            "UnsupportedByStore: import_arrow".to_string(),
        ))
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
        Err(TurbopufferError::Other(
            "UnsupportedByStore: facet".to_string(),
        ))
    }

    async fn head_namespace(&self, namespace: &str) -> Result<NamespaceMeta, TurbopufferError>;
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

#[derive(Debug, Clone)]
pub struct TurbopufferPassthroughResponse {
    pub status: u16,
    pub content_type: Option<String>,
    pub body: Vec<u8>,
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
                self.namespace_store_refs
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .get(namespace)
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
    async fn passthrough(
        &self,
        method: &str,
        path: &str,
        query: Option<&str>,
        body: Option<Value>,
    ) -> Result<TurbopufferPassthroughResponse, TurbopufferError> {
        self.client_for_namespace(namespace_from_path(path))?
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
            let id = row.get("id")?.as_str()?.to_string();
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
                dist,
                attributes,
            })
        })
        .collect()
}

#[async_trait]
impl TurbopufferClient for HttpTurbopufferClient {
    async fn passthrough(
        &self,
        method: &str,
        path: &str,
        query: Option<&str>,
        body: Option<Value>,
    ) -> Result<TurbopufferPassthroughResponse, TurbopufferError> {
        let mut url = format!("{}{}", self.base_url, path);
        if let Some(query) = query {
            if !query.is_empty() {
                url.push('?');
                url.push_str(query);
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
        let request = if let Some(body) = body {
            request.json(&body)
        } else {
            request
        };

        let resp = self
            .authorize(request)?
            .send()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;
        let status = resp.status().as_u16();
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let body = resp
            .bytes()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?
            .to_vec();

        Ok(TurbopufferPassthroughResponse {
            status,
            content_type,
            body,
        })
    }

    async fn hint_cache_warm(&self, namespace: &str) -> Result<(), TurbopufferError> {
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
        Ok(())
    }

    async fn upsert(
        &self,
        namespace: &str,
        docs: &[UpsertDoc],
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
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

        let url = format!("{}/v2/namespaces/{}", self.base_url, namespace);
        let resp = self
            .authorize(self.client.post(&url).json(&body))?
            .send()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;

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

        let url = format!("{}/v2/namespaces/{}", self.base_url, namespace);
        let resp = self
            .authorize(self.client.post(&url).json(&body))?
            .send()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;

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
        let mut patch_columns = serde_json::Map::new();
        patch_columns.insert(
            "id".to_string(),
            Value::Array(columns.ids.iter().cloned().map(Value::String).collect()),
        );
        for (name, values) in &columns.columns {
            patch_columns.insert(name.clone(), Value::Array(values.clone()));
        }

        let body = serde_json::json!({ "patch_columns": patch_columns });
        let url = format!("{}/v2/namespaces/{}", self.base_url, namespace);
        let resp = self
            .authorize(self.client.post(&url).json(&body))?
            .send()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;

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
        let body = serde_json::json!({
            "deletes": ids,
        });

        let url = format!("{}/v2/namespaces/{}", self.base_url, namespace);
        let resp = self
            .authorize(self.client.post(&url).json(&body))?
            .send()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;

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

        let url = format!("{}/v2/namespaces/{}/query", self.base_url, namespace);
        let resp = self
            .authorize(self.client.post(&url).json(&body))?
            .send()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(TurbopufferError::from_response(resp).await);
        }

        let resp_body: Value = resp
            .json()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;

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

        let url = format!("{}/v2/namespaces/{}/query", self.base_url, namespace);
        let resp = self
            .authorize(self.client.post(&url).json(&body))?
            .send()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(TurbopufferError::from_response(resp).await);
        }

        let resp_body: Value = resp
            .json()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;

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
        let mut body = serde_json::json!({
            "queries": legs,
        });
        if let Some(rerank_by) = rerank_by {
            body["rerank_by"] = rerank_by.clone();
        }
        let url = format!("{}/v2/namespaces/{}/query", self.base_url, namespace);
        let resp = self
            .authorize(self.client.post(&url).json(&body))?
            .send()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(TurbopufferError::from_response(resp).await);
        }

        resp.json()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))
    }

    async fn fetch(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<DocumentResponse>, TurbopufferError> {
        let body = serde_json::json!({
            "rank_by": ["id", "asc"],
            "top_k": 1,
            "filters": ["id", "Eq", id],
            "include_attributes": true,
            "consistency": {"level": "eventual"},
        });

        let url = format!("{}/v2/namespaces/{}/query", self.base_url, namespace);
        let resp = self
            .authorize(self.client.post(&url).json(&body))?
            .send()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(TurbopufferError::from_response(resp).await);
        }

        let resp_body: Value = resp
            .json()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;

        let rows = resp_body
            .get("rows")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        Ok(rows.into_iter().find_map(|row| {
            let row_id = row.get("id")?.as_str()?;
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
                id: row_id.to_string(),
                attributes,
            })
        }))
    }

    async fn fetch_many(
        &self,
        namespace: &str,
        ids: &[String],
    ) -> Result<HashMap<String, DocumentResponse>, TurbopufferError> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        let body = serde_json::json!({
            "rank_by": ["id", "asc"],
            "top_k": ids.len(),
            "filters": ["id", "In", ids],
            "include_attributes": true,
            "consistency": {"level": "eventual"},
        });

        let url = format!("{}/v2/namespaces/{}/query", self.base_url, namespace);
        let resp = self
            .authorize(self.client.post(&url).json(&body))?
            .send()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(TurbopufferError::from_response(resp).await);
        }

        let resp_body: Value = resp
            .json()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;

        let rows = resp_body
            .get("rows")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let mut result = HashMap::new();
        for row in rows {
            if let Some(id) = row.get("id").and_then(|v| v.as_str()) {
                let mut attributes = HashMap::new();
                if let Some(obj) = row.as_object() {
                    for (k, v) in obj {
                        if !is_system_column(k) {
                            attributes.insert(k.clone(), v.clone());
                        }
                    }
                }
                result.insert(
                    id.to_string(),
                    DocumentResponse {
                        id: id.to_string(),
                        attributes,
                    },
                );
            }
        }
        Ok(result)
    }

    async fn fetch_vector(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<Vec<f64>>, TurbopufferError> {
        // Ask for the `vector` column explicitly — Turbopuffer omits it from
        // query rows unless requested. This is the *only* place the gateway
        // pulls a vector out of upstream; everywhere else, `is_system_column`
        // drops it before it reaches the caller.
        let body = serde_json::json!({
            "rank_by": ["id", "asc"],
            "top_k": 1,
            "filters": ["id", "Eq", id],
            "include_attributes": ["vector"],
            "consistency": {"level": "eventual"},
        });

        let url = format!("{}/v2/namespaces/{}/query", self.base_url, namespace);
        let resp = self
            .authorize(self.client.post(&url).json(&body))?
            .send()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(TurbopufferError::from_response(resp).await);
        }

        let resp_body: Value = resp
            .json()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;

        let rows = resp_body
            .get("rows")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        Ok(rows.into_iter().find_map(|row| {
            if row.get("id").and_then(|v| v.as_str()) != Some(id) {
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
        // Build filter: Id > cursor AND any user filters
        let cursor_filter = cursor.map(|c| serde_json::json!(["id", "Gt", c]));

        let combined_filter = match (cursor_filter, filters) {
            (Some(cf), Some(uf)) => Some(serde_json::json!(["And", [cf, uf.clone()]])),
            (Some(cf), None) => Some(cf),
            (None, Some(uf)) => Some(uf.clone()),
            (None, None) => None,
        };

        let query_top_k = page_size.saturating_add(1).min(10_000);
        let mut body = serde_json::json!({
            "rank_by": ["id", "asc"],
            "top_k": query_top_k,
            "include_attributes": true,
            "consistency": {"level": "eventual"},
        });
        if let Some(f) = combined_filter {
            body["filters"] = f;
        }
        if let Some(attrs) = include_attributes {
            body["include_attributes"] =
                serde_json::to_value(attrs).map_err(|e| TurbopufferError::Other(e.to_string()))?;
        }

        let url = format!("{}/v2/namespaces/{}/query", self.base_url, namespace);
        let resp = self
            .authorize(self.client.post(&url).json(&body))?
            .send()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(TurbopufferError::from_response(resp).await);
        }

        let resp_body: Value = resp
            .json()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;

        let rows = resp_body
            .get("rows")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let mut documents: Vec<DocumentResponse> = rows
            .into_iter()
            .filter_map(|row| {
                let id = row.get("id")?.as_str()?.to_string();
                let mut attributes = HashMap::new();
                if let Some(obj) = row.as_object() {
                    for (k, v) in obj {
                        if !is_system_column(k) {
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

    async fn head_namespace(&self, namespace: &str) -> Result<NamespaceMeta, TurbopufferError> {
        let url = format!("{}/v2/namespaces/{}/metadata", self.base_url, namespace);
        let resp = self
            .authorize(self.client.get(&url))?
            .send()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(TurbopufferError::from_response(resp).await);
        }

        let body: Value = resp
            .json()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;

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
    /// Per-namespace flag: when set, `head_namespace` returns
    /// `TurbopufferError::NotFound`, mirroring upstream's 404 for a missing
    /// namespace. Used by tests of the metadata route's 404 mapping.
    head_not_found: tokio::sync::RwLock<std::collections::HashSet<String>>,
    /// Optional status override for namespace delete passthrough calls. Used
    /// by gateway route tests to exercise idempotent 404 and hard upstream
    /// failure handling without mutating the mock store first.
    delete_namespace_status: tokio::sync::RwLock<HashMap<String, u16>>,
    scan_filters: tokio::sync::RwLock<Vec<Option<Value>>>,
    scan_include_attributes: tokio::sync::RwLock<Vec<Option<Vec<String>>>>,
    ranked_query_filters: tokio::sync::RwLock<Vec<Option<Value>>>,
    missing_include_attributes: tokio::sync::RwLock<HashMap<String, HashSet<String>>>,
    scan_page_delay: tokio::sync::RwLock<Option<Duration>>,
    scan_page_active: AtomicUsize,
    scan_page_max_active: AtomicUsize,
    ranked_query_delay: tokio::sync::RwLock<Option<Duration>>,
    ranked_query_active: AtomicUsize,
    ranked_query_max_active: AtomicUsize,
    warm_hints: tokio::sync::RwLock<HashMap<String, u64>>,
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
            docs: tokio::sync::RwLock::new(HashMap::new()),
            vectors: tokio::sync::RwLock::new(HashMap::new()),
            status: tokio::sync::RwLock::new(HashMap::new()),
            metadata_overrides: tokio::sync::RwLock::new(HashMap::new()),
            rate_limit_once: tokio::sync::RwLock::new(HashMap::new()),
            head_failure: tokio::sync::RwLock::new(HashMap::new()),
            head_not_found: tokio::sync::RwLock::new(std::collections::HashSet::new()),
            delete_namespace_status: tokio::sync::RwLock::new(HashMap::new()),
            scan_filters: tokio::sync::RwLock::new(Vec::new()),
            scan_include_attributes: tokio::sync::RwLock::new(Vec::new()),
            ranked_query_filters: tokio::sync::RwLock::new(Vec::new()),
            missing_include_attributes: tokio::sync::RwLock::new(HashMap::new()),
            scan_page_delay: tokio::sync::RwLock::new(None),
            scan_page_active: AtomicUsize::new(0),
            scan_page_max_active: AtomicUsize::new(0),
            ranked_query_delay: tokio::sync::RwLock::new(None),
            ranked_query_active: AtomicUsize::new(0),
            ranked_query_max_active: AtomicUsize::new(0),
            warm_hints: tokio::sync::RwLock::new(HashMap::new()),
        }
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

    pub async fn scan_filters(&self) -> Vec<Option<Value>> {
        self.scan_filters.read().await.clone()
    }

    pub async fn scan_include_attributes(&self) -> Vec<Option<Vec<String>>> {
        self.scan_include_attributes.read().await.clone()
    }

    pub async fn ranked_query_filters(&self) -> Vec<Option<Value>> {
        self.ranked_query_filters.read().await.clone()
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
        let body = mock_passthrough(self, method, path, query, body).await?;
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
        _include_attributes: Option<&IncludeAttributes>,
    ) -> Result<TurbopufferQueryOutcome, TurbopufferError> {
        let _guard = enter_counter(&self.ranked_query_active, &self.ranked_query_max_active);
        if let Some(delay) = *self.ranked_query_delay.read().await {
            tokio::time::sleep(delay).await;
        }
        self.ranked_query_filters
            .write()
            .await
            .push(filters.cloned());
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

        Ok(DocumentPage {
            documents,
            next_cursor,
        })
    }

    async fn head_namespace(&self, namespace: &str) -> Result<NamespaceMeta, TurbopufferError> {
        if self.head_not_found.read().await.contains(namespace) {
            return Err(TurbopufferError::NotFound(format!(
                "404 Not Found: namespace '{}' was not found",
                namespace
            )));
        }
        if let Some(msg) = self.head_failure.read().await.get(namespace).cloned() {
            return Err(TurbopufferError::Other(msg));
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
    _query: Option<&str>,
    body: Option<Value>,
) -> Result<Value, TurbopufferError> {
    let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
    match (method, parts.as_slice()) {
        ("GET", ["v1", "namespaces"]) => {
            let mut namespaces: Vec<String> = client.docs.read().await.keys().cloned().collect();
            namespaces.sort();
            Ok(serde_json::json!({
                "namespaces": namespaces.into_iter().map(|id| serde_json::json!({ "id": id })).collect::<Vec<_>>(),
                "next_cursor": null,
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

    if obj.contains_key("branch_from_namespace") || obj.contains_key("copy_from_namespace") {
        client.ensure_namespace(namespace).await;
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
    if return_affected_ids {
        if let Some(obj) = response.as_object_mut() {
            obj.insert("upserted_ids".to_string(), Value::Array(upserted_ids));
            obj.insert("patched_ids".to_string(), Value::Array(patched_ids));
            obj.insert("deleted_ids".to_string(), Value::Array(deleted_ids));
        }
    }
    Ok(response)
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
        op if op.eq_ignore_ascii_case("Eq") => actual == Some(expected),
        op if op.eq_ignore_ascii_case("NotEq") => actual != Some(expected),
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
