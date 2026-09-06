//! In-process, read-only hev search backend for the pro gateway image.

use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use hevsearch_core::cache::NamespaceCache;
use hevsearch_core::{
    decode_list_cursor, CoreMetrics, FacetRequest, FuzzyRequest, ListOrder, NamespaceId,
    NamespaceManager, NamespaceService, QueryRequest, Scheme, StorageRoot,
};
use serde_json::{json, Value};

use crate::models::{
    DocumentPage, DocumentResponse, FieldValueResult, IncludeAttributes, QueryResult,
};
use crate::search::{search_filter_and_fuzzy, turbolisp_to_sql};
use crate::turbopuffer::{
    IndexStatus, NamespaceMeta, PatchColumns, PatchDoc, TurbopufferClient, TurbopufferError,
    TurbopufferPassthroughResponse, TurbopufferQueryOutcome, TurbopufferWriteOutcome, UpsertDoc,
};

const MAX_SCAN_PAGE_SIZE: u32 = 500;

/// A gateway-native hev search client. All mutations are rejected: embedded
/// mode is intentionally a horizontally safe, read-mostly serving path.
pub struct EmbeddedSearchClient {
    service: Arc<NamespaceService>,
    manager: Arc<NamespaceManager>,
}

impl EmbeddedSearchClient {
    /// Build a client whose VectorStore endpoint is a `file://` or `s3://`
    /// dataset root. S3-compatible credentials and endpoints use the same
    /// `HEVSEARCH_S3_*` environment variables as hev search itself.
    pub async fn new(
        store_name: &str,
        storage_uri: &str,
        region: Option<&str>,
    ) -> Result<Self, TurbopufferError> {
        let root = StorageRoot::parse(storage_uri).map_err(engine_error)?;
        let metrics = Arc::new(CoreMetrics::new().map_err(engine_error)?);
        let manager = Arc::new(NamespaceManager::new(
            root.clone(),
            storage_options(root.scheme(), region),
            Arc::clone(&metrics),
        ));
        let cache_path = cache_path(store_name, storage_uri);
        std::fs::create_dir_all(&cache_path).map_err(|error| {
            TurbopufferError::Other(format!("embedded search cache directory: {error}"))
        })?;
        let memory_bytes = env_usize("HEVSEARCH_CACHE_MEMORY_BYTES", 64 * 1024 * 1024)?;
        let nvme_bytes = env_usize("HEVSEARCH_CACHE_NVME_BYTES", 256 * 1024 * 1024)?;
        let cache = Arc::new(
            NamespaceCache::new(memory_bytes, &cache_path, nvme_bytes, Arc::clone(&metrics))
                .await
                .map_err(engine_error)?,
        );
        let service = Arc::new(NamespaceService::new(Arc::clone(&manager), cache, metrics));
        Ok(Self { service, manager })
    }

    fn unsupported(operation: &str) -> TurbopufferError {
        TurbopufferError::Other(format!(
            "UnsupportedByStore: search-embedded is read-only and does not support {operation}"
        ))
    }

    fn namespace(namespace: &str) -> Result<NamespaceId, TurbopufferError> {
        NamespaceId::new(namespace).map_err(engine_error)
    }

    async fn list(
        &self,
        namespace: &str,
        cursor: Option<&str>,
        limit: usize,
        filter: Option<String>,
    ) -> Result<hevsearch_core::ListPage, TurbopufferError> {
        let namespace = Self::namespace(namespace)?;
        let cursor = cursor
            .map(decode_list_cursor)
            .transpose()
            .map_err(engine_error)?;
        self.manager
            .list(&namespace, limit, ListOrder::Asc, cursor, filter)
            .await
            .map_err(engine_error)
    }

    async fn fetch_row(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<hevsearch_core::ListRow>, TurbopufferError> {
        let filter = format!("id = '{}'", id.replace('\'', "''"));
        Ok(self
            .list(namespace, None, 1, Some(filter))
            .await?
            .rows
            .into_iter()
            .find(|row| row.id.to_string() == id))
    }

    async fn run_query(
        &self,
        namespace: &str,
        request: QueryRequest,
        include_attributes: Option<&IncludeAttributes>,
    ) -> Result<TurbopufferQueryOutcome, TurbopufferError> {
        let namespace = Self::namespace(namespace)?;
        let result = self
            .service
            .query(&namespace, &request)
            .await
            .map_err(engine_error)?;
        Ok(TurbopufferQueryOutcome {
            rows: result
                .results
                .into_iter()
                .map(|row| {
                    let mut attributes: HashMap<String, Value> =
                        row.attributes.into_iter().collect();
                    if let Some(text) = row.text {
                        attributes.insert("text".into(), Value::String(text));
                    }
                    QueryResult {
                        id: row.id.to_string(),
                        dist: Some(f64::from(row.score)),
                        attributes: select_attributes(attributes, include_attributes),
                    }
                })
                .collect(),
            billing: None,
        })
    }
}

#[async_trait]
impl TurbopufferClient for EmbeddedSearchClient {
    async fn passthrough(
        &self,
        method: &str,
        path: &str,
        _query: Option<&str>,
        _body: Option<Value>,
    ) -> Result<TurbopufferPassthroughResponse, TurbopufferError> {
        if method == "GET" {
            if let Some(namespace) = path
                .strip_prefix("/v1/namespaces/")
                .and_then(|rest| rest.strip_suffix("/schema"))
            {
                let namespace_id = Self::namespace(namespace)?;
                let info = self
                    .manager
                    .info(&namespace_id)
                    .await
                    .map_err(engine_error)?
                    .ok_or_else(|| {
                        TurbopufferError::NotFound(format!(
                            "search namespace {namespace} not found"
                        ))
                    })?;
                return json_passthrough(200, json!({ "schema": schema_map(&info) }));
            }
        }
        Ok(unsupported_response("turbopuffer passthrough"))
    }

    async fn delete_namespace(
        &self,
        _namespace: &str,
    ) -> Result<TurbopufferPassthroughResponse, TurbopufferError> {
        Ok(unsupported_response("namespace deletion"))
    }

    async fn hint_cache_warm(&self, namespace: &str) -> Result<(), TurbopufferError> {
        let namespace = Self::namespace(namespace)?;
        self.manager.info(&namespace).await.map_err(engine_error)?;
        Ok(())
    }

    async fn upsert(
        &self,
        _namespace: &str,
        _docs: &[UpsertDoc],
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        Err(Self::unsupported("upsert"))
    }
    async fn patch(
        &self,
        _namespace: &str,
        _docs: &[PatchDoc],
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        Err(Self::unsupported("patch"))
    }
    async fn patch_columns(
        &self,
        _namespace: &str,
        _columns: &PatchColumns,
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        Err(Self::unsupported("patch_columns"))
    }
    async fn delete(
        &self,
        _namespace: &str,
        _ids: &[String],
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        Err(Self::unsupported("delete"))
    }
    async fn delete_by_filter(
        &self,
        _namespace: &str,
        _filters: &Value,
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        Err(Self::unsupported("delete_by_filter"))
    }
    async fn import_arrow(
        &self,
        _namespace: &str,
        _content_type: &str,
        _body: Vec<u8>,
    ) -> Result<TurbopufferPassthroughResponse, TurbopufferError> {
        Ok(unsupported_response("import_arrow"))
    }

    async fn query(
        &self,
        namespace: &str,
        vector: &[f64],
        top_k: u32,
        filters: Option<&Value>,
        include_attributes: Option<&IncludeAttributes>,
    ) -> Result<TurbopufferQueryOutcome, TurbopufferError> {
        self.run_query(
            namespace,
            QueryRequest {
                vector: vector.iter().map(|value| *value as f32).collect(),
                vectors: None,
                k: top_k as usize,
                nprobes: None,
                exact: false,
                text: None,
                fuzzy: None,
                filter: filters.map(turbolisp_to_sql).transpose()?,
                include_vector: false,
            },
            include_attributes,
        )
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
        let (filter, fuzzy) = filters
            .map(search_filter_and_fuzzy)
            .transpose()?
            .unwrap_or((None, None));
        let rank = rank_by
            .as_array()
            .ok_or_else(|| Self::unsupported("ranked_query rank_by shape"))?;
        let mut request = QueryRequest {
            vector: Vec::new(),
            vectors: None,
            k: top_k as usize,
            nprobes: None,
            exact: false,
            text: None,
            fuzzy: None,
            filter,
            include_vector: false,
        };
        match (rank.get(1).and_then(Value::as_str), rank.get(2)) {
            (Some(op), Some(Value::Array(values))) if op.eq_ignore_ascii_case("ANN") => {
                if values.first().is_some_and(Value::is_array) {
                    request.vectors = Some(
                        serde_json::from_value(Value::Array(values.clone()))
                            .map_err(|_| Self::unsupported("ranked_query ANN vectors"))?,
                    );
                } else {
                    request.vector = serde_json::from_value(Value::Array(values.clone()))
                        .map_err(|_| Self::unsupported("ranked_query ANN vector"))?;
                }
            }
            (Some(op), Some(Value::String(text)))
                if op.eq_ignore_ascii_case("BM25") || op.eq_ignore_ascii_case("HybridText") =>
            {
                request.text = Some(text.clone());
                request.fuzzy = fuzzy
                    .map(|value| {
                        serde_json::from_value::<FuzzyRequest>(json!({"max_edit_distance": value}))
                    })
                    .transpose()
                    .map_err(|error| TurbopufferError::Other(error.to_string()))?;
            }
            _ => return Err(Self::unsupported("ranked_query rank_by shape")),
        }
        self.run_query(namespace, request, include_attributes).await
    }

    async fn multi_ranked_query(
        &self,
        _namespace: &str,
        _legs: &[Value],
        _rerank_by: Option<&Value>,
    ) -> Result<Value, TurbopufferError> {
        Err(Self::unsupported("multi_ranked_query"))
    }

    async fn fetch(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<DocumentResponse>, TurbopufferError> {
        Ok(self.fetch_row(namespace, id).await?.map(row_to_document))
    }

    async fn fetch_many(
        &self,
        namespace: &str,
        ids: &[String],
    ) -> Result<HashMap<String, DocumentResponse>, TurbopufferError> {
        let mut documents = HashMap::new();
        for id in ids {
            if let Some(document) = self.fetch(namespace, id).await? {
                documents.insert(id.clone(), document);
            }
        }
        Ok(documents)
    }

    async fn fetch_vector(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<Vec<f64>>, TurbopufferError> {
        Ok(self
            .fetch_row(namespace, id)
            .await?
            .map(|row| row.vector.into_iter().map(f64::from).collect()))
    }

    async fn scan_page(
        &self,
        namespace: &str,
        cursor: Option<&str>,
        page_size: u32,
        filters: Option<&Value>,
        include_attributes: Option<&[String]>,
    ) -> Result<DocumentPage, TurbopufferError> {
        let page = self
            .list(
                namespace,
                cursor,
                page_size.min(MAX_SCAN_PAGE_SIZE) as usize,
                filters.map(turbolisp_to_sql).transpose()?,
            )
            .await?;
        Ok(DocumentPage {
            documents: page
                .rows
                .into_iter()
                .map(row_to_document)
                .map(|mut document| {
                    document.attributes = select_fields(document.attributes, include_attributes);
                    document
                })
                .collect(),
            next_cursor: page.next_cursor,
        })
    }

    async fn facet(
        &self,
        namespace: &str,
        filters: Option<&Value>,
        field: &str,
        top: usize,
    ) -> Result<Vec<FieldValueResult>, TurbopufferError> {
        let namespace = Self::namespace(namespace)?;
        let result = self
            .service
            .facet(
                &namespace,
                &FacetRequest {
                    filter: filters.map(turbolisp_to_sql).transpose()?,
                    fields: vec![field.to_string()],
                    top: Some(top),
                },
            )
            .await
            .map_err(engine_error)?;
        Ok(result
            .facets
            .into_iter()
            .find(|facet| facet.field == field)
            .map(|facet| {
                facet
                    .buckets
                    .into_iter()
                    .map(|bucket| FieldValueResult {
                        value: facet_key(&bucket.value),
                        doc_count: bucket.count,
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn head_namespace(&self, namespace: &str) -> Result<NamespaceMeta, TurbopufferError> {
        let namespace_id = Self::namespace(namespace)?;
        let info = self
            .manager
            .info(&namespace_id)
            .await
            .map_err(engine_error)?
            .ok_or_else(|| {
                TurbopufferError::NotFound(format!("search namespace {namespace} not found"))
            })?;
        let raw = json!({
            "row_count": info.row_count,
            "approx_logical_bytes": info.approx_logical_bytes,
            "schema": schema_map(&info),
            "index": { "status": "up-to-date" },
            "search": info,
        });
        Ok(NamespaceMeta {
            index_status: IndexStatus::Stable,
            unindexed_bytes: None,
            approx_row_count: info.row_count as u64,
            approx_logical_bytes: info.approx_logical_bytes,
            count_settle: Some(info.row_count as u64),
            raw,
        })
    }
}

fn storage_options(scheme: Scheme, region: Option<&str>) -> HashMap<String, String> {
    if scheme != Scheme::S3 {
        return HashMap::new();
    }
    let mut options = HashMap::new();
    for (env, key) in [
        ("HEVSEARCH_S3_ENDPOINT", "aws_endpoint"),
        ("HEVSEARCH_S3_ACCESS_KEY", "aws_access_key_id"),
        ("HEVSEARCH_S3_SECRET_KEY", "aws_secret_access_key"),
    ] {
        if let Ok(value) = std::env::var(env) {
            if !value.trim().is_empty() {
                options.insert(key.into(), value);
            }
        }
    }
    if options.contains_key("aws_endpoint") {
        options.insert("allow_http".into(), "true".into());
        options.insert("aws_virtual_hosted_style_request".into(), "false".into());
    }
    options.insert(
        "aws_region".into(),
        region
            .filter(|value| !value.trim().is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| hevsearch_core::resolve_s3_region(|key| std::env::var(key).ok())),
    );
    options
}

fn cache_path(store_name: &str, storage_uri: &str) -> PathBuf {
    let base = std::env::var_os("HEVSEARCH_CACHE_NVME_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("hevlayer-embedded-search-cache"));
    let mut hasher = DefaultHasher::new();
    store_name.hash(&mut hasher);
    storage_uri.hash(&mut hasher);
    base.join(format!("{store_name}-{:016x}", hasher.finish()))
}

fn env_usize(name: &str, default: usize) -> Result<usize, TurbopufferError> {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|error| TurbopufferError::Other(format!("{name}: {error}")))
        })
        .transpose()
        .map(|value| value.unwrap_or(default))
}
fn engine_error(error: impl std::fmt::Display) -> TurbopufferError {
    TurbopufferError::Other(error.to_string())
}
fn unsupported_response(operation: &str) -> TurbopufferPassthroughResponse {
    TurbopufferPassthroughResponse { status: 422, content_type: Some("application/json".into()), body: serde_json::to_vec(&json!({"error":"UnsupportedByStore","message":format!("search-embedded is read-only and does not support {operation}")})).expect("static response serializes") }
}
fn json_passthrough(
    status: u16,
    value: Value,
) -> Result<TurbopufferPassthroughResponse, TurbopufferError> {
    Ok(TurbopufferPassthroughResponse {
        status,
        content_type: Some("application/json".into()),
        body: serde_json::to_vec(&value)
            .map_err(|error| TurbopufferError::Other(error.to_string()))?,
    })
}
fn schema_map(info: &hevsearch_core::NamespaceInfo) -> serde_json::Map<String, Value> {
    info.schema
        .iter()
        .filter(|field| field.name != "_ingested_at" && field.name != "text_tok")
        .map(|field| {
            let schema = if field.name == "vector" || field.name == "vectors" {
                json!({ "type": "[]float", "dimensions": info.vector_dim })
            } else {
                json!({ "type": arrow_type_to_store_type(&field.data_type) })
            };
            (field.name.clone(), schema)
        })
        .collect()
}
fn arrow_type_to_store_type(data_type: &str) -> &'static str {
    let lower = data_type.to_ascii_lowercase();
    if lower.contains("bool") {
        "bool"
    } else if lower.contains("int") || lower.contains("uint") {
        "int"
    } else if lower.contains("float") || lower.contains("double") {
        "float"
    } else if lower.starts_with("list") || lower.starts_with("large_list") {
        "[]string"
    } else {
        "string"
    }
}
fn row_to_document(row: hevsearch_core::ListRow) -> DocumentResponse {
    let mut attributes: HashMap<String, Value> = row.attributes.into_iter().collect();
    if let Some(text) = row.text {
        attributes.insert("text".into(), Value::String(text));
    }
    DocumentResponse {
        id: row.id.to_string(),
        attributes,
    }
}
fn select_attributes(
    attributes: HashMap<String, Value>,
    include: Option<&IncludeAttributes>,
) -> HashMap<String, Value> {
    match include {
        Some(IncludeAttributes::All(false)) => HashMap::new(),
        Some(IncludeAttributes::Fields(fields)) => select_fields(attributes, Some(fields)),
        _ => attributes,
    }
}
fn select_fields(
    attributes: HashMap<String, Value>,
    include: Option<&[String]>,
) -> HashMap<String, Value> {
    let Some(include) = include else {
        return attributes;
    };
    let include: HashSet<&String> = include.iter().collect();
    attributes
        .into_iter()
        .filter(|(key, _)| include.contains(key))
        .collect()
}
fn facet_key(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Null => "null".into(),
        value => serde_json::to_string(value).unwrap_or_else(|_| value.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hevsearch_core::{RowId, UpsertRow};

    fn temp_path(label: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("hevlayer-{label}-{}-{nonce}", std::process::id()))
    }

    #[tokio::test]
    async fn serves_local_lance_reads_and_rejects_writes() {
        let data = temp_path("embedded-data");
        let metrics = Arc::new(CoreMetrics::new().unwrap());
        let manager =
            NamespaceManager::new(StorageRoot::local(&data).unwrap(), HashMap::new(), metrics);
        let namespace = NamespaceId::new("photos").unwrap();
        manager
            .upsert(
                &namespace,
                vec![
                    UpsertRow {
                        id: RowId::String("photo-a".into()),
                        vector: vec![1.0, 0.0],
                        vectors: None,
                        text: Some("red canyon".into()),
                        attributes: serde_json::Map::from_iter([("album".into(), json!("west"))]),
                    },
                    UpsertRow {
                        id: RowId::String("photo-b".into()),
                        vector: vec![0.0, 1.0],
                        vectors: None,
                        text: Some("blue ocean".into()),
                        attributes: serde_json::Map::from_iter([("album".into(), json!("coast"))]),
                    },
                ],
            )
            .await
            .unwrap();

        let client = EmbeddedSearchClient::new("test", &format!("file://{}", data.display()), None)
            .await
            .unwrap();
        let query = client
            .query("photos", &[1.0, 0.0], 1, None, None)
            .await
            .unwrap();
        assert_eq!(query.rows[0].id, "photo-a");
        assert_eq!(query.rows[0].attributes["album"], "west");

        let fetched = client.fetch("photos", "photo-b").await.unwrap().unwrap();
        assert_eq!(fetched.attributes["text"], "blue ocean");
        let page = client
            .scan_page("photos", None, 1, None, Some(&["album".into()]))
            .await
            .unwrap();
        assert_eq!(page.documents.len(), 1);
        assert_eq!(page.documents[0].attributes.len(), 1);
        assert!(page.next_cursor.is_some());
        let meta = client.head_namespace("photos").await.unwrap();
        assert_eq!(meta.approx_row_count, 2);
        assert_eq!(meta.raw["schema"]["vector"]["dimensions"], 2);
        let schema = client
            .passthrough("GET", "/v1/namespaces/photos/schema", None, None)
            .await
            .unwrap();
        assert_eq!(schema.status, 200);
        let schema: Value = serde_json::from_slice(&schema.body).unwrap();
        assert_eq!(schema["schema"]["album"]["type"], "string");
        assert!(
            format!("{}", client.upsert("photos", &[]).await.unwrap_err())
                .contains("UnsupportedByStore")
        );
    }

    #[test]
    fn read_only_response_is_wire_compatible() {
        let response = unsupported_response("upsert");
        assert_eq!(response.status, 422);
        let body: Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["error"], "UnsupportedByStore");
        assert!(body["message"].as_str().unwrap().contains("read-only"));
    }

    #[test]
    fn storage_root_accepts_r2_s3_prefixes() {
        assert_eq!(
            StorageRoot::parse("s3://bucket/prefix").unwrap().scheme(),
            Scheme::S3
        );
    }
}
