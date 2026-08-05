use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::models::{
    DocumentPage, DocumentResponse, FieldValueResult, IncludeAttributes, QueryResult,
};
use crate::turbopuffer::{
    IndexStatus, MockTurbopufferClient, NamespaceMeta, PatchColumns, PatchDoc, TurbopufferClient,
    TurbopufferError, TurbopufferPassthroughResponse, TurbopufferQueryOutcome,
    TurbopufferWriteOutcome, UpsertDoc,
};

#[derive(Clone)]
pub struct HttpSearchClient {
    client: reqwest::Client,
    base_url: String,
}

const SEARCH_SCAN_PAGE_SIZE_MAX: u32 = 500;

pub struct MockSearchClient {
    inner: MockTurbopufferClient,
    delete_filters: tokio::sync::RwLock<Vec<Value>>,
}

impl Default for MockSearchClient {
    fn default() -> Self {
        Self::new()
    }
}

impl MockSearchClient {
    pub fn new() -> Self {
        Self {
            inner: MockTurbopufferClient::new(),
            delete_filters: tokio::sync::RwLock::new(Vec::new()),
        }
    }

    pub async fn delete_filters(&self) -> Vec<Value> {
        self.delete_filters.read().await.clone()
    }
}

#[async_trait]
impl TurbopufferClient for MockSearchClient {
    async fn passthrough(
        &self,
        _method: &str,
        _path: &str,
        _query: Option<&str>,
        _body: Option<Value>,
    ) -> Result<TurbopufferPassthroughResponse, TurbopufferError> {
        Ok(TurbopufferPassthroughResponse {
            status: 422,
            content_type: Some("application/json".to_string()),
            body: br#"{"error":"UnsupportedByStore","message":"mock search does not support turbopuffer passthrough"}"#.to_vec(),
        })
    }

    async fn delete_namespace(
        &self,
        namespace: &str,
    ) -> Result<TurbopufferPassthroughResponse, TurbopufferError> {
        self.inner.delete_namespace(namespace).await
    }

    async fn hint_cache_warm(&self, namespace: &str) -> Result<(), TurbopufferError> {
        self.inner.hint_cache_warm(namespace).await
    }

    async fn upsert(
        &self,
        namespace: &str,
        docs: &[UpsertDoc],
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        self.inner.upsert(namespace, docs).await
    }

    async fn patch(
        &self,
        namespace: &str,
        docs: &[PatchDoc],
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        self.inner.patch(namespace, docs).await
    }

    async fn patch_columns(
        &self,
        namespace: &str,
        columns: &PatchColumns,
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        self.inner.patch_columns(namespace, columns).await
    }

    async fn delete(
        &self,
        namespace: &str,
        ids: &[String],
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        self.inner.delete(namespace, ids).await
    }

    async fn delete_by_filter(
        &self,
        _namespace: &str,
        filters: &Value,
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        self.delete_filters.write().await.push(filters.clone());
        Ok(TurbopufferWriteOutcome {
            billing: Some(json!({})),
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
        self.inner
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
        self.inner
            .ranked_query(namespace, rank_by, top_k, filters, include_attributes)
            .await
    }

    async fn multi_ranked_query(
        &self,
        _namespace: &str,
        _legs: &[Value],
        _rerank_by: Option<&Value>,
    ) -> Result<Value, TurbopufferError> {
        Err(HttpSearchClient::unsupported("multi_ranked_query"))
    }

    async fn fetch(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<DocumentResponse>, TurbopufferError> {
        self.inner.fetch(namespace, id).await
    }

    async fn fetch_many(
        &self,
        namespace: &str,
        ids: &[String],
    ) -> Result<HashMap<String, DocumentResponse>, TurbopufferError> {
        self.inner.fetch_many(namespace, ids).await
    }

    async fn fetch_vector(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<Vec<f64>>, TurbopufferError> {
        self.inner.fetch_vector(namespace, id).await
    }

    async fn scan_page(
        &self,
        namespace: &str,
        cursor: Option<&str>,
        page_size: u32,
        filters: Option<&Value>,
        include_attributes: Option<&[String]>,
    ) -> Result<DocumentPage, TurbopufferError> {
        self.inner
            .scan_page(namespace, cursor, page_size, filters, include_attributes)
            .await
    }

    async fn facet(
        &self,
        namespace: &str,
        filters: Option<&Value>,
        field: &str,
        top: usize,
    ) -> Result<Vec<FieldValueResult>, TurbopufferError> {
        self.inner.facet(namespace, filters, field, top).await
    }

    async fn head_namespace(&self, namespace: &str) -> Result<NamespaceMeta, TurbopufferError> {
        self.inner.head_namespace(namespace).await
    }
}

impl HttpSearchClient {
    pub fn new(api_key: Option<&str>, base_url: &str) -> Self {
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(api_key) = api_key.map(str::trim).filter(|key| !key.is_empty()) {
            headers.insert(
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_str(&format!("Bearer {api_key}"))
                    .expect("invalid search API key"),
            );
        }
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .expect("failed to build reqwest client");
        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    fn unsupported(method: &str) -> TurbopufferError {
        TurbopufferError::Other(format!(
            "UnsupportedByStore: search backend does not support {method}"
        ))
    }

    async fn post_json(&self, path: &str, body: &Value) -> Result<Value, TurbopufferError> {
        let resp = self
            .client
            .post(format!("{}{}", self.base_url, path))
            .json(body)
            .send()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;
        json_response(resp).await
    }

    async fn get_json(&self, path: &str) -> Result<Value, TurbopufferError> {
        let resp = self
            .client
            .get(format!("{}{}", self.base_url, path))
            .send()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;
        json_response(resp).await
    }

    async fn fetch_full_row(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<SearchListRow>, TurbopufferError> {
        let filter = id_eq_filter(id);
        let page = self.list_page(namespace, None, 1, Some(&filter)).await?;
        Ok(page.rows.into_iter().find(|row| row.id.as_string() == id))
    }

    async fn list_page(
        &self,
        namespace: &str,
        cursor: Option<&str>,
        limit: usize,
        filter: Option<&str>,
    ) -> Result<SearchListPage, TurbopufferError> {
        let mut url = reqwest::Url::parse(&format!("{}/ns/{namespace}/list", self.base_url))
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("limit", &limit.to_string());
            query.append_pair("order", "asc");
            if let Some(cursor) = cursor {
                query.append_pair("cursor", cursor);
            }
            if let Some(filter) = filter {
                query.append_pair("filter", filter);
            }
        }
        let resp = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;
        let body = json_response(resp).await?;
        serde_json::from_value(body).map_err(|e| TurbopufferError::Other(e.to_string()))
    }
}

#[async_trait]
impl TurbopufferClient for HttpSearchClient {
    async fn passthrough(
        &self,
        _method: &str,
        _path: &str,
        _query: Option<&str>,
        _body: Option<Value>,
    ) -> Result<TurbopufferPassthroughResponse, TurbopufferError> {
        Ok(TurbopufferPassthroughResponse {
            status: 422,
            content_type: Some("application/json".to_string()),
            body: br#"{"error":"UnsupportedByStore","message":"search backend does not support turbopuffer passthrough"}"#.to_vec(),
        })
    }

    async fn delete_namespace(
        &self,
        namespace: &str,
    ) -> Result<TurbopufferPassthroughResponse, TurbopufferError> {
        let resp = self
            .client
            .delete(format!("{}/ns/{namespace}", self.base_url))
            .send()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;
        raw_response(resp).await
    }

    async fn hint_cache_warm(&self, namespace: &str) -> Result<(), TurbopufferError> {
        self.post_json(
            &format!("/ns/{namespace}/warmup"),
            &json!({ "queries": [] }),
        )
        .await?;
        Ok(())
    }

    async fn upsert(
        &self,
        namespace: &str,
        docs: &[UpsertDoc],
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        let rows = docs
            .iter()
            .map(|doc| {
                let text = doc
                    .attributes
                    .get("text")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
                let vector = doc
                    .vector
                    .clone()
                    .or_else(|| text.as_ref().map(|_| vec![0.0]));
                if vector.is_none() && doc.vectors.is_none() {
                    return Err(TurbopufferError::Other(
                        "UnsupportedByStore: search upsert requires vector, vectors, or text attribute".to_string(),
                    ));
                }
                let vector = vector
                    .or_else(|| doc.vectors.as_ref().and_then(|vectors| vectors.first().cloned()));
                let mut row = serde_json::Map::new();
                row.insert("id".to_string(), Value::String(doc.id.clone()));
                if let Some(vector) = vector {
                    row.insert(
                        "vector".to_string(),
                        serde_json::to_value(vector).unwrap_or(Value::Null),
                    );
                }
                if let Some(vectors) = &doc.vectors {
                    row.insert(
                        "vectors".to_string(),
                        serde_json::to_value(vectors).unwrap_or(Value::Null),
                    );
                }
                if let Some(text) = text {
                    row.insert("text".to_string(), Value::String(text));
                }
                row.insert(
                    "attributes".to_string(),
                    Value::Object(
                        doc.attributes
                            .iter()
                            .filter(|(key, _)| {
                                let key = key.as_str();
                                key != "text" && !key.starts_with("_hevlayer_")
                            })
                            .map(|(key, value)| (key.clone(), value.clone()))
                            .collect(),
                    ),
                );
                Ok(Value::Object(row))
            })
            .collect::<Result<Vec<_>, TurbopufferError>>()?;
        let body = self
            .post_json(&format!("/ns/{namespace}/upsert"), &json!({ "rows": rows }))
            .await?;
        Ok(TurbopufferWriteOutcome {
            billing: billing_from_body(&body),
        })
    }

    async fn import_arrow(
        &self,
        namespace: &str,
        content_type: &str,
        body: Vec<u8>,
    ) -> Result<TurbopufferPassthroughResponse, TurbopufferError> {
        let resp = self
            .client
            .post(format!("{}/ns/{namespace}/import", self.base_url))
            .header(reqwest::header::CONTENT_TYPE, content_type)
            .body(body)
            .send()
            .await
            .map_err(|e| TurbopufferError::Other(e.to_string()))?;
        raw_response(resp).await
    }

    async fn patch(
        &self,
        namespace: &str,
        docs: &[PatchDoc],
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        let mut upserts = Vec::with_capacity(docs.len());
        for doc in docs {
            let mut row = self
                .fetch_full_row(namespace, &doc.id)
                .await?
                .ok_or_else(|| {
                    TurbopufferError::NotFound(format!("search row {} not found", doc.id))
                })?;
            for (key, value) in &doc.attributes {
                row.attributes.insert(key.clone(), value.clone());
            }
            upserts.push(row.into_upsert_doc());
        }
        self.upsert(namespace, &upserts).await
    }

    async fn patch_columns(
        &self,
        namespace: &str,
        columns: &PatchColumns,
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        let docs = (0..columns.ids.len())
            .map(|idx| {
                let attributes = columns
                    .columns
                    .iter()
                    .filter_map(|(name, values)| {
                        values.get(idx).map(|value| (name.clone(), value.clone()))
                    })
                    .collect();
                PatchDoc {
                    id: columns.ids[idx].clone(),
                    attributes,
                }
            })
            .collect::<Vec<_>>();
        self.patch(namespace, &docs).await
    }

    async fn delete(
        &self,
        namespace: &str,
        ids: &[String],
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        let body = self
            .post_json(&format!("/ns/{namespace}/delete"), &json!({ "ids": ids }))
            .await?;
        Ok(TurbopufferWriteOutcome {
            billing: billing_from_body(&body),
        })
    }

    async fn delete_by_filter(
        &self,
        namespace: &str,
        filters: &Value,
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        let filter = turbolisp_to_sql(filters)?;
        let body = self
            .post_json(
                &format!("/ns/{namespace}/delete"),
                &json!({ "filter": filter }),
            )
            .await?;
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
        let filter = filters.map(turbolisp_to_sql).transpose()?;
        let body = json!({
            "vector": vector.iter().map(|v| *v as f32).collect::<Vec<_>>(),
            "k": top_k as usize,
            "filter": filter,
            "include_vector": false,
        });
        let body = self
            .post_json(&format!("/ns/{namespace}/query"), &body)
            .await?;
        Ok(TurbopufferQueryOutcome {
            rows: search_results_to_query_results(&body, include_attributes),
            billing: billing_from_body(&body),
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
        let (filter, fuzzy) = filters
            .map(search_filter_and_fuzzy)
            .transpose()?
            .unwrap_or((None, None));
        let mut body = serde_json::Map::new();
        body.insert("k".to_string(), json!(top_k as usize));
        body.insert("include_vector".to_string(), Value::Bool(false));
        if let Some(filter) = filter {
            body.insert("filter".to_string(), Value::String(filter));
        }
        let rank = rank_by
            .as_array()
            .ok_or_else(|| TurbopufferError::Other("rank_by must be an array".to_string()))?;
        match (rank.get(1).and_then(Value::as_str), rank.get(2)) {
            (Some(op), Some(Value::Array(vector_or_vectors))) if op.eq_ignore_ascii_case("ANN") => {
                if vector_or_vectors
                    .first()
                    .is_some_and(|value| value.is_array())
                {
                    if vector_or_vectors.is_empty() {
                        return Err(Self::unsupported(
                            "ranked_query ANN vectors must be non-empty",
                        ));
                    }
                    let vectors = vector_or_vectors
                        .iter()
                        .map(|vector| {
                            let vector = vector.as_array()?;
                            if vector.is_empty() {
                                return None;
                            }
                            vector
                                .iter()
                                .map(|v| v.as_f64().map(|v| v as f32))
                                .collect::<Option<Vec<_>>>()
                        })
                        .collect::<Option<Vec<_>>>()
                        .ok_or_else(|| {
                            Self::unsupported(
                                "ranked_query ANN vectors must be non-empty numeric vectors",
                            )
                        })?;
                    body.insert("vectors".to_string(), json!(vectors));
                } else {
                    if vector_or_vectors.is_empty() {
                        return Err(Self::unsupported(
                            "ranked_query ANN vector must be non-empty",
                        ));
                    }
                    let vector = vector_or_vectors
                        .iter()
                        .map(|v| v.as_f64().map(|v| v as f32))
                        .collect::<Option<Vec<_>>>()
                        .ok_or_else(|| {
                            Self::unsupported("ranked_query ANN vector must be numeric")
                        })?;
                    body.insert("vector".to_string(), json!(vector));
                }
            }
            (Some(op), Some(Value::String(text)))
                if op.eq_ignore_ascii_case("BM25") || op.eq_ignore_ascii_case("HybridText") =>
            {
                body.insert("text".to_string(), Value::String(text.clone()));
                if let Some(fuzzy) = fuzzy {
                    body.insert("fuzzy".to_string(), json!({ "max_edit_distance": fuzzy }));
                }
            }
            _ => return Err(Self::unsupported("ranked_query rank_by shape")),
        }
        let body = self
            .post_json(&format!("/ns/{namespace}/query"), &Value::Object(body))
            .await?;
        Ok(TurbopufferQueryOutcome {
            rows: search_results_to_query_results(&body, include_attributes),
            billing: billing_from_body(&body),
        })
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
        Ok(self
            .fetch_full_row(namespace, id)
            .await?
            .map(|row| DocumentResponse {
                id: row.id.as_string(),
                attributes: row.attributes_with_text(),
            }))
    }

    async fn fetch_many(
        &self,
        namespace: &str,
        ids: &[String],
    ) -> Result<HashMap<String, DocumentResponse>, TurbopufferError> {
        let mut out = HashMap::new();
        for chunk in ids.chunks(500) {
            let wanted = chunk.iter().cloned().collect::<HashSet<_>>();
            let filter = id_in_filter(chunk);
            let page = self
                .list_page(namespace, None, chunk.len(), Some(&filter))
                .await?;
            for row in page.rows {
                let id = row.id.as_string();
                if wanted.contains(&id) {
                    out.insert(
                        id.clone(),
                        DocumentResponse {
                            id,
                            attributes: row.attributes_with_text(),
                        },
                    );
                }
            }
        }
        Ok(out)
    }

    async fn fetch_vector(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<Vec<f64>>, TurbopufferError> {
        Ok(self
            .fetch_full_row(namespace, id)
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
        let page_size = page_size.min(SEARCH_SCAN_PAGE_SIZE_MAX);
        let filter = filters.map(turbolisp_to_sql).transpose()?;
        let page = self
            .list_page(namespace, cursor, page_size as usize, filter.as_deref())
            .await?;
        let documents = page
            .rows
            .into_iter()
            .map(|row| DocumentResponse {
                id: row.id.as_string(),
                attributes: filter_attributes(row.attributes_with_text(), include_attributes),
            })
            .collect::<Vec<_>>();
        Ok(DocumentPage {
            documents,
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
        let filter = filters.map(turbolisp_to_sql).transpose()?;
        let body = json!({
            "filter": filter,
            "fields": [field],
            "top": top,
        });
        let body = self
            .post_json(&format!("/ns/{namespace}/facet"), &body)
            .await?;
        Ok(body
            .get("facets")
            .and_then(Value::as_array)
            .and_then(|facets| {
                facets
                    .iter()
                    .find(|facet| facet.get("field").and_then(Value::as_str) == Some(field))
            })
            .and_then(|facet| facet.get("buckets").and_then(Value::as_array))
            .map(|buckets| {
                buckets
                    .iter()
                    .filter_map(|bucket| {
                        Some(FieldValueResult {
                            value: facet_value_key(bucket.get("value")?),
                            doc_count: bucket.get("count")?.as_u64()?,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn head_namespace(&self, namespace: &str) -> Result<NamespaceMeta, TurbopufferError> {
        let body = self.get_json(&format!("/ns/{namespace}")).await?;
        let row_count = body.get("row_count").and_then(Value::as_u64).unwrap_or(0);
        Ok(NamespaceMeta {
            index_status: IndexStatus::Stable,
            approx_row_count: row_count,
            approx_logical_bytes: None,
            count_settle: Some(row_count),
            raw: body,
            unindexed_bytes: None,
        })
    }
}

async fn json_response(resp: reqwest::Response) -> Result<Value, TurbopufferError> {
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(TurbopufferError::from_status(status, &text));
    }
    resp.json()
        .await
        .map_err(|e| TurbopufferError::Other(e.to_string()))
}

async fn raw_response(
    resp: reqwest::Response,
) -> Result<TurbopufferPassthroughResponse, TurbopufferError> {
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(TurbopufferError::from_status(status, &text));
    }
    let status = resp.status().as_u16();
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
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

fn billing_from_body(body: &Value) -> Option<Value> {
    body.get("billing")
        .filter(|billing| !billing.is_null())
        .cloned()
}

fn facet_value_key(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => "null".to_string(),
        other => serde_json::to_string(other).unwrap_or_else(|_| other.to_string()),
    }
}

fn search_results_to_query_results(
    body: &Value,
    include_attributes: Option<&IncludeAttributes>,
) -> Vec<QueryResult> {
    body.get("results")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|row| {
            let id = SearchId::from_value(row.get("id")?)?.as_string();
            let dist = row.get("score").and_then(Value::as_f64);
            let attributes: HashMap<String, Value> = row
                .get("attributes")
                .and_then(Value::as_object)
                .map(|attrs| attrs.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                .unwrap_or_default();
            let mut attributes = attributes;
            if let Some(text) = row.get("text").and_then(Value::as_str) {
                attributes.insert("text".to_string(), Value::String(text.to_string()));
            }
            let attributes = filter_attributes(
                attributes,
                include_attribute_fields(include_attributes).as_deref(),
            );
            Some(QueryResult {
                id,
                dist,
                attributes,
            })
        })
        .collect()
}

fn include_attribute_fields(include: Option<&IncludeAttributes>) -> Option<Vec<String>> {
    match include {
        Some(IncludeAttributes::All(false)) => Some(Vec::new()),
        Some(IncludeAttributes::Fields(fields)) => Some(fields.clone()),
        _ => None,
    }
}

fn filter_attributes(
    attributes: HashMap<String, Value>,
    include: Option<&[String]>,
) -> HashMap<String, Value> {
    match include {
        Some(fields) => {
            let fields = fields.iter().collect::<HashSet<_>>();
            attributes
                .into_iter()
                .filter(|(key, _)| fields.contains(key))
                .collect()
        }
        None => attributes,
    }
}

#[derive(Debug, Deserialize)]
struct SearchListPage {
    rows: Vec<SearchListRow>,
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SearchListRow {
    id: SearchId,
    vector: Vec<f32>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    attributes: HashMap<String, Value>,
}

impl SearchListRow {
    fn attributes_with_text(&self) -> HashMap<String, Value> {
        let mut attributes = self.attributes.clone();
        if let Some(text) = &self.text {
            attributes.insert("text".to_string(), Value::String(text.clone()));
        }
        attributes
    }

    fn into_upsert_doc(self) -> UpsertDoc {
        let mut attributes = self.attributes;
        if let Some(text) = self.text {
            attributes.insert("text".to_string(), Value::String(text));
        }
        UpsertDoc {
            id: self.id.as_string(),
            vector: Some(self.vector.into_iter().map(f64::from).collect()),
            vectors: None,
            attributes,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum SearchId {
    U64(u64),
    String(String),
}

impl SearchId {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Number(n) => n.as_u64().map(Self::U64),
            Value::String(s) => Some(Self::String(s.clone())),
            _ => None,
        }
    }

    fn as_string(&self) -> String {
        match self {
            Self::U64(id) => id.to_string(),
            Self::String(id) => id.clone(),
        }
    }
}

pub(crate) fn turbolisp_to_sql(filter: &Value) -> Result<String, TurbopufferError> {
    let arr = filter.as_array().ok_or_else(|| {
        TurbopufferError::Other(
            "UnsupportedByStore: filter must be a Turbopuffer array".to_string(),
        )
    })?;
    if arr.len() != 2 && arr.len() != 3 {
        return Err(TurbopufferError::Other(
            "UnsupportedByStore: filter arrays must have 2 or 3 elements".to_string(),
        ));
    }
    let op = arr.first().and_then(Value::as_str).unwrap_or_default();
    if matches!(op, "And" | "and" | "Or" | "or") {
        let children = arr.get(1).and_then(Value::as_array).ok_or_else(|| {
            TurbopufferError::Other(format!(
                "UnsupportedByStore: {op} filter expects children array"
            ))
        })?;
        let join = if op.eq_ignore_ascii_case("And") {
            " AND "
        } else {
            " OR "
        };
        let parts = children
            .iter()
            .map(turbolisp_to_sql)
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(format!("({})", parts.join(join)));
    }

    let field = arr.first().and_then(Value::as_str).ok_or_else(|| {
        TurbopufferError::Other("UnsupportedByStore: filter field must be a string".to_string())
    })?;
    let op = arr.get(1).and_then(Value::as_str).ok_or_else(|| {
        TurbopufferError::Other("UnsupportedByStore: filter op must be a string".to_string())
    })?;
    let rhs = arr.get(2).ok_or_else(|| {
        TurbopufferError::Other("UnsupportedByStore: comparison filter needs a value".to_string())
    })?;
    let field = quote_ident(field)?;
    match op {
        "Eq" | "eq" if rhs.is_null() => Ok(format!("{field} IS NULL")),
        "Eq" | "eq" => Ok(format!("{field} = {}", sql_literal(rhs)?)),
        "NotEq" | "notEq" | "Ne" | "ne" if rhs.is_null() => Ok(format!("{field} IS NOT NULL")),
        "NotEq" | "notEq" | "Ne" | "ne" => Ok(format!("{field} != {}", sql_literal(rhs)?)),
        "Gt" | "gt" => Ok(format!("{field} > {}", sql_literal(rhs)?)),
        "Gte" | "gte" => Ok(format!("{field} >= {}", sql_literal(rhs)?)),
        "Lt" | "lt" => Ok(format!("{field} < {}", sql_literal(rhs)?)),
        "Lte" | "lte" => Ok(format!("{field} <= {}", sql_literal(rhs)?)),
        "In" | "in" => {
            let values = rhs.as_array().ok_or_else(|| {
                TurbopufferError::Other("UnsupportedByStore: In expects an array".to_string())
            })?;
            let values = values
                .iter()
                .map(sql_literal)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("{field} IN ({})", values.join(", ")))
        }
        "Contains" | "contains" => Ok(format!("array_has({field}, {})", sql_literal(rhs)?)),
        "ContainsAny" | "containsAny" => {
            let values = rhs.as_array().ok_or_else(|| {
                TurbopufferError::Other(
                    "UnsupportedByStore: ContainsAny expects an array".to_string(),
                )
            })?;
            let parts = values
                .iter()
                .map(|value| Ok(format!("array_has({field}, {})", sql_literal(value)?)))
                .collect::<Result<Vec<_>, TurbopufferError>>()?;
            Ok(format!("({})", parts.join(" OR ")))
        }
        other => Err(TurbopufferError::Other(format!(
            "UnsupportedByStore: filter op {other} is not translatable to search SQL"
        ))),
    }
}

fn search_filter_and_fuzzy(
    filter: &Value,
) -> Result<(Option<String>, Option<Value>), TurbopufferError> {
    if let Some(fuzzy) = fuzzy_from_filter(filter)? {
        return Ok((None, Some(fuzzy)));
    }

    let Some(arr) = filter.as_array() else {
        return Ok((Some(turbolisp_to_sql(filter)?), None));
    };
    let Some(op) = arr.first().and_then(Value::as_str) else {
        return Ok((Some(turbolisp_to_sql(filter)?), None));
    };
    if !op.eq_ignore_ascii_case("And") {
        return Ok((Some(turbolisp_to_sql(filter)?), None));
    }

    let children = arr.get(1).and_then(Value::as_array).ok_or_else(|| {
        TurbopufferError::Other("UnsupportedByStore: And filter expects children array".to_string())
    })?;
    let mut sql_parts = Vec::new();
    let mut fuzzy = None;
    for child in children {
        if let Some(child_fuzzy) = fuzzy_from_filter(child)? {
            fuzzy = Some(child_fuzzy);
        } else {
            sql_parts.push(turbolisp_to_sql(child)?);
        }
    }
    let sql = match sql_parts.len() {
        0 => None,
        1 => Some(sql_parts.remove(0)),
        _ => Some(format!("({})", sql_parts.join(" AND "))),
    };
    Ok((sql, fuzzy))
}

fn fuzzy_from_filter(filter: &Value) -> Result<Option<Value>, TurbopufferError> {
    let Some(arr) = filter.as_array() else {
        return Ok(None);
    };
    if arr.len() < 3 {
        return Ok(None);
    }
    let Some(op) = arr.get(1).and_then(Value::as_str) else {
        return Ok(None);
    };
    if !op.eq_ignore_ascii_case("Fuzzy") {
        return Ok(None);
    }
    let opts = arr.get(3).unwrap_or(&Value::Null);
    Ok(Some(search_fuzzy_value(opts)?))
}

fn search_fuzzy_value(opts: &Value) -> Result<Value, TurbopufferError> {
    let Some(max) = opts.get("max_edit_distance") else {
        return Ok(Value::String("auto".to_string()));
    };
    if max.as_str() == Some("auto") || max.as_u64().is_some_and(|n| n <= 2) {
        return Ok(max.clone());
    }
    if let Some(rules) = max.as_array() {
        let distance = rules
            .iter()
            .filter_map(|rule| rule.get("distance").and_then(Value::as_u64))
            .max()
            .unwrap_or(0)
            .min(2);
        return Ok(Value::from(distance));
    }
    Err(TurbopufferError::Other(
        "UnsupportedByStore: Fuzzy max_edit_distance must be auto, 0, 1, 2, or a rule array"
            .to_string(),
    ))
}

fn quote_ident(ident: &str) -> Result<String, TurbopufferError> {
    if ident
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
    {
        Ok(ident.to_string())
    } else {
        Err(TurbopufferError::Other(format!(
            "UnsupportedByStore: filter field {ident:?} is not a portable identifier"
        )))
    }
}

fn sql_literal(value: &Value) -> Result<String, TurbopufferError> {
    match value {
        Value::String(s) => Ok(format!("'{}'", s.replace('\'', "''"))),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Null => Ok("NULL".to_string()),
        _ => Err(TurbopufferError::Other(
            "UnsupportedByStore: filter value must be scalar".to_string(),
        )),
    }
}

fn id_eq_filter(id: &str) -> String {
    format!("id = '{}'", id.replace('\'', "''"))
}

fn id_in_filter(ids: &[String]) -> String {
    let ids = ids
        .iter()
        .map(|id| format!("'{}'", id.replace('\'', "''")))
        .collect::<Vec<_>>();
    format!("id IN ({})", ids.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use axum::extract::{Query, State};
    use axum::http::HeaderMap;
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use serde::Deserialize;
    use tokio::net::TcpListener;

    #[test]
    fn translates_basic_filters_to_sql() {
        assert_eq!(
            turbolisp_to_sql(&json!(["section", "Eq", "warnings"])).unwrap(),
            "section = 'warnings'"
        );
        assert_eq!(
            turbolisp_to_sql(&json!(["events", "Eq", null])).unwrap(),
            "events IS NULL"
        );
        assert_eq!(
            turbolisp_to_sql(&json!(["events", "NotEq", null])).unwrap(),
            "events IS NOT NULL"
        );
        assert_eq!(
            turbolisp_to_sql(&json!([
                "And",
                [["section", "Eq", "warnings"], ["score", "Gte", 2]]
            ]))
            .unwrap(),
            "(section = 'warnings' AND score >= 2)"
        );
        assert_eq!(
            turbolisp_to_sql(&json!(["openfda", "Contains", "rx"])).unwrap(),
            "array_has(openfda, 'rx')"
        );
        assert_eq!(
            turbolisp_to_sql(&json!(["route", "ContainsAny", ["ORAL", "INTRAVENOUS"]])).unwrap(),
            "(array_has(route, 'ORAL') OR array_has(route, 'INTRAVENOUS'))"
        );
    }

    #[test]
    fn rejects_unportable_filters_by_name() {
        let err = turbolisp_to_sql(&json!(["text", "Fuzzy", "typo"])).unwrap_err();
        assert!(format!("{err}").contains("UnsupportedByStore"));
    }

    #[tokio::test]
    async fn upsert_maps_text_only_label_rows_to_search_wire() {
        let captured = Arc::new(Mutex::new(Vec::<Value>::new()));
        let base_url = spawn_capture_server(Arc::clone(&captured)).await;
        let client = HttpSearchClient::new(Some("data-key"), &base_url);

        client
            .upsert(
                "drug-labels",
                &[UpsertDoc {
                    id: "set-a#bw#0".to_string(),
                    vector: None,
                    vectors: None,
                    attributes: HashMap::from([
                        (
                            "text".to_string(),
                            Value::String("boxed warning hepatotoxicity".to_string()),
                        ),
                        (
                            "section".to_string(),
                            Value::String("boxed_warning".to_string()),
                        ),
                        ("route".to_string(), json!(["ORAL", "INTRAVENOUS"])),
                    ]),
                }],
            )
            .await
            .unwrap();

        let bodies = captured.lock().unwrap();
        assert_eq!(bodies.len(), 1);
        let row = &bodies[0]["rows"][0];
        assert_eq!(row["id"], "set-a#bw#0");
        assert_eq!(row["vector"], json!([0.0]));
        assert_eq!(row["text"], "boxed warning hepatotoxicity");
        assert_eq!(row["attributes"]["section"], "boxed_warning");
        assert_eq!(row["attributes"]["route"], json!(["ORAL", "INTRAVENOUS"]));
        assert!(row["attributes"].get("text").is_none());
    }

    #[tokio::test]
    async fn keyless_client_omits_authorization_header() {
        let captured = Arc::new(Mutex::new(Vec::<Value>::new()));
        let base_url = spawn_capture_server(Arc::clone(&captured)).await;
        let client = HttpSearchClient::new(None, &base_url);

        client.hint_cache_warm("drug-labels").await.unwrap();

        let bodies = captured.lock().unwrap();
        assert_eq!(bodies.len(), 1);
        assert_eq!(bodies[0]["authorization"], Value::Null);
    }

    #[tokio::test]
    async fn upsert_maps_multivector_rows_to_search_wire() {
        let captured = Arc::new(Mutex::new(Vec::<Value>::new()));
        let base_url = spawn_capture_server(Arc::clone(&captured)).await;
        let client = HttpSearchClient::new(Some("data-key"), &base_url);

        client
            .upsert(
                "late-interaction",
                &[UpsertDoc {
                    id: "shot-1".to_string(),
                    vector: None,
                    vectors: Some(vec![vec![0.1, 0.2], vec![0.3, 0.4]]),
                    attributes: HashMap::from([(
                        "kind".to_string(),
                        Value::String("shot".to_string()),
                    )]),
                }],
            )
            .await
            .unwrap();

        let bodies = captured.lock().unwrap();
        let row = &bodies[0]["rows"][0];
        assert_eq!(row["id"], "shot-1");
        assert_eq!(row["vector"], json!([0.1, 0.2]));
        assert_eq!(row["vectors"], json!([[0.1, 0.2], [0.3, 0.4]]));
        assert_eq!(row["attributes"]["kind"], "shot");
        assert!(row["attributes"].get("vectors").is_none());
    }

    #[tokio::test]
    async fn ranked_query_maps_hybrid_text_to_search_text_query() {
        let captured = Arc::new(Mutex::new(Vec::<Value>::new()));
        let base_url = spawn_capture_server(Arc::clone(&captured)).await;
        let client = HttpSearchClient::new(Some("data-key"), &base_url);

        let out = client
            .ranked_query(
                "drug-labels",
                &json!(["text", "HybridText", "liver injury"]),
                5,
                Some(&json!([
                    "And",
                    [
                        ["section", "Eq", "boxed_warning"],
                        [
                            "text",
                            "Fuzzy",
                            "injury",
                            {"max_edit_distance": [
                                {"min_query_chars": 3, "distance": 0},
                                {"min_query_chars": 6, "distance": 1}
                            ]}
                        ]
                    ]
                ])),
                Some(&IncludeAttributes::Fields(vec![
                    "text".into(),
                    "section".into(),
                ])),
            )
            .await
            .unwrap();

        let bodies = captured.lock().unwrap();
        assert_eq!(bodies[0]["text"], "liver injury");
        assert_eq!(bodies[0]["k"], 5);
        assert_eq!(bodies[0]["filter"], "section = 'boxed_warning'");
        assert_eq!(bodies[0]["fuzzy"], json!({"max_edit_distance": 1}));
        assert_eq!(out.rows[0].id, "set-a#bw#0");
        assert_eq!(
            out.rows[0].attributes["text"],
            "boxed warning hepatotoxicity"
        );
        assert_eq!(out.rows[0].attributes["section"], "boxed_warning");
        assert!(!out.rows[0].attributes.contains_key("route"));
    }

    #[tokio::test]
    async fn ranked_query_maps_multivector_ann_to_search_vectors() {
        let captured = Arc::new(Mutex::new(Vec::<Value>::new()));
        let base_url = spawn_capture_server(Arc::clone(&captured)).await;
        let client = HttpSearchClient::new(Some("data-key"), &base_url);

        let out = client
            .ranked_query(
                "late-interaction",
                &json!(["vectors", "ANN", [[0.1, 0.2], [0.3, 0.4]]]),
                7,
                Some(&json!(["kind", "Eq", "shot"])),
                None,
            )
            .await
            .unwrap();

        assert_eq!(out.rows[0].id, "set-a#bw#0");
        let bodies = captured.lock().unwrap();
        let body = bodies.last().unwrap();
        assert_eq!(body["k"], 7);
        let vectors = body["vectors"].as_array().unwrap();
        assert_eq!(vectors.len(), 2);
        assert_eq!(vectors[0].as_array().unwrap().len(), 2);
        assert!((vectors[0][0].as_f64().unwrap() - 0.1).abs() < 0.000001);
        assert!((vectors[1][1].as_f64().unwrap() - 0.4).abs() < 0.000001);
        assert!(body.get("vector").is_none());
        assert_eq!(body["filter"], "kind = 'shot'");
    }

    #[tokio::test]
    async fn ranked_query_rejects_malformed_ann_vectors_for_search() {
        let captured = Arc::new(Mutex::new(Vec::<Value>::new()));
        let base_url = spawn_capture_server(Arc::clone(&captured)).await;
        let client = HttpSearchClient::new(Some("data-key"), &base_url);

        for rank_by in [
            json!(["vector", "ANN", []]),
            json!(["vectors", "ANN", [[]]]),
            json!(["vectors", "ANN", [[0.1], []]]),
            json!(["vectors", "ANN", [[0.1], ["bad"]]]),
        ] {
            let err = client
                .ranked_query("late-interaction", &rank_by, 7, None, None)
                .await
                .unwrap_err();

            assert!(
                format!("{err}").contains("UnsupportedByStore"),
                "unexpected error for {rank_by}: {err}"
            );
        }

        assert!(captured.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn facet_posts_label_contains_any_filter_to_search() {
        let captured = Arc::new(Mutex::new(Vec::<Value>::new()));
        let base_url = spawn_capture_server(Arc::clone(&captured)).await;
        let client = HttpSearchClient::new(Some("data-key"), &base_url);

        let buckets = client
            .facet(
                "drug-labels",
                Some(&json!([
                    "And",
                    [
                        ["section", "Eq", "boxed_warning"],
                        ["route", "ContainsAny", ["ORAL", "INTRAVENOUS"]]
                    ]
                ])),
                "route",
                10,
            )
            .await
            .unwrap();

        let bodies = captured.lock().unwrap();
        assert_eq!(bodies[0]["fields"], json!(["route"]));
        assert_eq!(
            bodies[0]["filter"],
            "(section = 'boxed_warning' AND (array_has(route, 'ORAL') OR array_has(route, 'INTRAVENOUS')))"
        );
        assert_eq!(buckets[0].value, "ORAL");
        assert_eq!(buckets[0].doc_count, 2);
    }

    #[tokio::test]
    async fn delete_posts_string_ids_to_search_delete_endpoint() {
        let captured = Arc::new(Mutex::new(Vec::<Value>::new()));
        let base_url = spawn_capture_server(Arc::clone(&captured)).await;
        let client = HttpSearchClient::new(Some("data-key"), &base_url);

        client
            .delete(
                "drug-labels",
                &[
                    "set-a#warnings#0".to_string(),
                    "set-b'special#dosage#1".to_string(),
                ],
            )
            .await
            .unwrap();

        let bodies = captured.lock().unwrap();
        assert_eq!(
            bodies[0]["ids"],
            json!(["set-a#warnings#0", "set-b'special#dosage#1"])
        );
    }

    #[tokio::test]
    async fn delete_by_filter_posts_translated_sql_to_search_delete_endpoint() {
        let captured = Arc::new(Mutex::new(Vec::<Value>::new()));
        let base_url = spawn_capture_server(Arc::clone(&captured)).await;
        let client = HttpSearchClient::new(Some("data-key"), &base_url);

        client
            .delete_by_filter("drug-labels", &json!(["section", "Eq", "boxed_warning"]))
            .await
            .unwrap();

        let bodies = captured.lock().unwrap();
        assert_eq!(bodies[0]["filter"], "section = 'boxed_warning'");
    }

    #[tokio::test]
    async fn scan_page_translates_filter_to_search_list_filter() {
        let captured = Arc::new(Mutex::new(Vec::<Value>::new()));
        let base_url = spawn_capture_server(Arc::clone(&captured)).await;
        let client = HttpSearchClient::new(Some("data-key"), &base_url);

        let page = client
            .scan_page(
                "drug-labels",
                None,
                25,
                Some(&json!(["section", "Eq", "boxed_warning"])),
                Some(&["text".into(), "section".into()]),
            )
            .await
            .unwrap();

        let bodies = captured.lock().unwrap();
        assert_eq!(bodies[0]["list_limit"], "25");
        assert_eq!(bodies[0]["list_order"], "asc");
        assert_eq!(bodies[0]["list_filter"], "section = 'boxed_warning'");
        assert!(bodies[0]["list_cursor"].is_null());
        assert_eq!(page.documents[0].id, "set-a#bw#0");
        assert_eq!(
            page.documents[0].attributes["text"],
            "boxed warning hepatotoxicity"
        );
        assert_eq!(page.documents[0].attributes["section"], "boxed_warning");
        assert!(!page.documents[0].attributes.contains_key("route"));
    }

    #[tokio::test]
    async fn scan_page_clamps_to_search_backend_limit() {
        let captured = Arc::new(Mutex::new(Vec::<Value>::new()));
        let base_url = spawn_capture_server(Arc::clone(&captured)).await;
        let client = HttpSearchClient::new(Some("data-key"), &base_url);

        client
            .scan_page("drug-labels", None, 1000, None, Some(&["section".into()]))
            .await
            .unwrap();

        let bodies = captured.lock().unwrap();
        assert_eq!(bodies[0]["list_limit"], "500");
    }

    #[tokio::test]
    async fn fetch_many_uses_filtered_search_list_by_id() {
        let captured = Arc::new(Mutex::new(Vec::<Value>::new()));
        let base_url = spawn_capture_server(Arc::clone(&captured)).await;
        let client = HttpSearchClient::new(Some("data-key"), &base_url);

        let rows = client
            .fetch_many(
                "drug-labels",
                &[
                    "set-a#bw#0".to_string(),
                    "set-b'special#warnings#1".to_string(),
                ],
            )
            .await
            .unwrap();

        let bodies = captured.lock().unwrap();
        assert_eq!(bodies[0]["list_limit"], "2");
        assert_eq!(
            bodies[0]["list_filter"],
            "id IN ('set-a#bw#0', 'set-b''special#warnings#1')"
        );
        assert!(bodies[0]["list_cursor"].is_null());
        assert_eq!(rows["set-a#bw#0"].attributes["section"], "boxed_warning");
        assert_eq!(
            rows["set-b'special#warnings#1"].attributes["section"],
            "warnings"
        );
    }

    #[tokio::test]
    async fn fetch_vector_uses_filtered_search_list_by_id() {
        let captured = Arc::new(Mutex::new(Vec::<Value>::new()));
        let base_url = spawn_capture_server(Arc::clone(&captured)).await;
        let client = HttpSearchClient::new(Some("data-key"), &base_url);

        let vector = client
            .fetch_vector("drug-labels", "set-b'special#warnings#1")
            .await
            .unwrap()
            .unwrap();

        let bodies = captured.lock().unwrap();
        assert_eq!(bodies[0]["list_limit"], "1");
        assert_eq!(bodies[0]["list_filter"], "id = 'set-b''special#warnings#1'");
        assert_eq!(vector, vec![1.0]);
    }

    #[tokio::test]
    async fn head_namespace_marks_search_stable_with_count_settle_key() {
        let captured = Arc::new(Mutex::new(Vec::<Value>::new()));
        let base_url = spawn_capture_server(Arc::clone(&captured)).await;
        let client = HttpSearchClient::new(Some("data-key"), &base_url);

        let meta = client.head_namespace("drug-labels").await.unwrap();

        assert_eq!(meta.index_status, IndexStatus::Stable);
        assert_eq!(meta.approx_row_count, 7);
        assert_eq!(meta.count_settle, Some(7));
        assert_eq!(meta.raw["namespace"], "drug-labels");
    }

    async fn spawn_capture_server(captured: Arc<Mutex<Vec<Value>>>) -> String {
        #[derive(Deserialize)]
        struct ListQuery {
            limit: Option<String>,
            order: Option<String>,
            cursor: Option<String>,
            filter: Option<String>,
        }

        async fn warmup_handler(
            State(captured): State<Arc<Mutex<Vec<Value>>>>,
            headers: HeaderMap,
        ) -> Json<Value> {
            captured.lock().unwrap().push(json!({
                "authorization": headers
                    .get(axum::http::header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
            }));
            Json(json!({"status": "ok"}))
        }

        async fn upsert_handler(
            State(captured): State<Arc<Mutex<Vec<Value>>>>,
            Json(body): Json<Value>,
        ) -> Json<Value> {
            captured.lock().unwrap().push(body);
            Json(json!({"upserted": 1}))
        }

        async fn query_handler(
            State(captured): State<Arc<Mutex<Vec<Value>>>>,
            Json(body): Json<Value>,
        ) -> Json<Value> {
            captured.lock().unwrap().push(body);
            Json(json!({
                "query_id": "q-test",
                "results": [{
                    "id": "set-a#bw#0",
                    "score": 1.0,
                    "vector": null,
                    "text": "boxed warning hepatotoxicity",
                    "attributes": {
                        "section": "boxed_warning",
                        "route": ["ORAL"]
                    }
                }]
            }))
        }

        async fn facet_handler(
            State(captured): State<Arc<Mutex<Vec<Value>>>>,
            Json(body): Json<Value>,
        ) -> Json<Value> {
            captured.lock().unwrap().push(body);
            Json(json!({
                "facets": [{
                    "field": "route",
                    "buckets": [{"value": "ORAL", "count": 2}],
                    "truncated": false
                }]
            }))
        }

        async fn delete_handler(
            State(captured): State<Arc<Mutex<Vec<Value>>>>,
            Json(body): Json<Value>,
        ) -> Json<Value> {
            captured.lock().unwrap().push(body);
            Json(json!({"deleted": 2}))
        }

        async fn list_handler(
            State(captured): State<Arc<Mutex<Vec<Value>>>>,
            Query(query): Query<ListQuery>,
        ) -> Json<Value> {
            let filter = query.filter.clone().unwrap_or_default();
            captured.lock().unwrap().push(json!({
                "list_limit": query.limit,
                "list_order": query.order,
                "list_cursor": query.cursor,
                "list_filter": query.filter,
            }));
            let mut rows = Vec::new();
            if filter.is_empty()
                || filter.contains("section = 'boxed_warning'")
                || filter.contains("set-a#bw#0")
            {
                rows.push(json!({
                    "id": "set-a#bw#0",
                    "vector": [0.0],
                    "text": "boxed warning hepatotoxicity",
                    "ingested_at_micros": 1,
                    "attributes": {
                        "section": "boxed_warning",
                        "route": ["ORAL"]
                    }
                }));
            }
            if filter.contains("set-b''special#warnings#1") {
                rows.push(json!({
                    "id": "set-b'special#warnings#1",
                    "vector": [1.0],
                    "text": "warnings dosage",
                    "ingested_at_micros": 2,
                    "attributes": {
                        "section": "warnings",
                        "route": ["INTRAVENOUS"]
                    }
                }));
            }
            Json(json!({
                "rows": rows,
                "next_cursor": null
            }))
        }

        async fn namespace_handler() -> Json<Value> {
            Json(json!({
                "namespace": "drug-labels",
                "row_count": 7
            }))
        }

        let app = Router::new()
            .route("/ns/{namespace}", get(namespace_handler))
            .route("/ns/{namespace}/warmup", post(warmup_handler))
            .route("/ns/{namespace}/upsert", post(upsert_handler))
            .route("/ns/{namespace}/query", post(query_handler))
            .route("/ns/{namespace}/facet", post(facet_handler))
            .route("/ns/{namespace}/delete", post(delete_handler))
            .route("/ns/{namespace}/list", get(list_handler))
            .with_state(captured);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }
}
