use std::cmp::Ordering;
use std::sync::Arc;
use std::time::Instant;

use axum::extract::{OriginalUri, Path, State};
use axum::http::{HeaderMap, HeaderValue};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Value};
use tracing::warn;

use crate::clients::turbopuffer::{TurbopufferError, UPSERTED_AT_ATTR};
use crate::error::AppError;
use crate::history::{
    header_to_string, log_search_history, now_timestamp, tags_from_headers, traceparent_for_query,
    TRACEPARENT_HEADER,
};
use crate::metrics::{DIRECT_PIPELINE_ID, STATUS_OK, STATUS_TPUF_ERROR};
use crate::models::{
    IncludeAttributes, QueryCursor, QueryRequest, QueryResult, SearchHistoryEntry,
};
use crate::shards::{active_shard_count, scatter_gather_query};

use crate::AppState;

/// Response header that advertises non-fatal request adjustments.
/// Today the only emitted value is `vector_attribute_dropped`, set when a
/// caller listed `vector` in `include_attributes`: the gateway strips it
/// from the response (it never returns vectors) and flips this header so
/// SDK consumers can surface the misuse.
pub const LAYER_WARNING_HEADER: &str = "x-layer-warning";
pub const VECTOR_ATTRIBUTE_DROPPED_WARNING: &str = "vector_attribute_dropped";
pub const LAYER_STABLE_AS_OF_HEADER: &str = "x-layer-stable-as-of";
pub const LAYER_NEXT_CURSOR_HEADER: &str = "x-layer-next-cursor";

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TemporalFilter {
    pub filter: Value,
    pub upper_bound: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct QueryRunConfig {
    pub watermark: Option<u64>,
    pub inject_filter: bool,
    pub temporal_filter: Option<TemporalFilter>,
    pub shard_count: Option<u64>,
    pub allow_cursor: bool,
}

#[derive(Debug)]
pub(crate) struct QueryLegOutput {
    pub request: QueryRequest,
    pub results: Vec<QueryResult>,
    pub next_cursor: Option<String>,
    pub warn_vector_dropped: bool,
}

/// True when the caller explicitly listed `vector` in `include_attributes`.
/// The boolean form (`include_attributes: true`) silently drops the vector
/// without warning — only the explicit-list form triggers the header.
pub fn include_attributes_requests_vector(include: Option<&IncludeAttributes>) -> bool {
    matches!(include, Some(IncludeAttributes::Fields(fields)) if fields.iter().any(|f| f == "vector"))
}

/// POST /v2/namespaces/{namespace}/query
///
/// Vector similarity search. The gateway forces `consistency=eventual`
/// upstream. When the consistency watcher's latest observation for this
/// namespace is `Updating`, the gateway injects an `_hevlayer_upserted_at <= watermark`
/// predicate alongside the caller's filter so the read never sees
/// partially-indexed data. When the index is `Stable` or `Unknown`, the
/// filter is skipped (the index should be caught up — no need to pay for the
/// extra predicate). If turbopuffer 429s the unfiltered attempt, we retry
/// once with the watermark filter forced on.
///
/// The watermark used in the response is reported as `stable_as_of`
/// regardless of whether the request itself was filtered — it tells the
/// caller "up to this moment, the upstream index is known caught up."
pub async fn query(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, AppError> {
    // Layer-only rank expressions are intercepted ahead of passthrough:
    // `HybridText` (RFC 0022) and `Auto` (RFC 0044) are gateway-expanded
    // spellings inside the upstream `rank_by` vocabulary. Everything else —
    // including native multi-query + `rerank_by` bodies — keeps today's
    // behavior byte-for-byte.
    let layer_operator = crate::routes::hybrid_text::rank_by_operator(&body)
        .filter(|op| *op == "HybridText" || *op == "Auto")
        .map(str::to_string);
    if let Some(op) = layer_operator {
        let map = body
            .as_object()
            .cloned()
            .expect("rank_by_operator only matches JSON objects");
        return match op.as_str() {
            "HybridText" => {
                crate::routes::hybrid_text::hybrid_text_query(state, namespace, headers, map).await
            }
            _ => crate::routes::query_router::auto_query(state, namespace, headers, map).await,
        };
    }
    if crate::routes::hybrid_text::queries_contain_layer_operator(&body) {
        return Err(AppError::Validation(
            "HybridText/Auto are not valid inside a `queries` body; the expansion is one multi-query deep by construction".to_string(),
        ));
    }

    if is_multi_query_request(&body) && !contains_rerank_by(&body) {
        return crate::routes::multi_query::multi_query(state, namespace, headers, body).await;
    }

    if should_passthrough_query(&body) {
        return crate::routes::turbopuffer::passthrough(
            state,
            "POST",
            uri.path(),
            uri.query(),
            Some(body),
        )
        .await;
    }

    let request: QueryRequest = serde_json::from_value(body)
        .map_err(|e| AppError::Validation(format!("invalid query request: {}", e)))?;
    query_hevlayer(state, namespace, headers, request).await
}

fn is_multi_query_request(body: &Value) -> bool {
    body.get("queries").is_some()
}

fn contains_rerank_by(body: &Value) -> bool {
    if body.get("rerank_by").is_some() {
        return true;
    }
    body.get("queries")
        .and_then(Value::as_array)
        .is_some_and(|queries| queries.iter().any(|leg| leg.get("rerank_by").is_some()))
}

async fn query_hevlayer(
    state: Arc<AppState>,
    namespace: String,
    headers: HeaderMap,
    request: QueryRequest,
) -> Result<Response, AppError> {
    let watermark = state.consistency.get(&namespace);
    let inject_filter = state.consistency.should_inject_filter(&namespace);
    let temporal_filter = temporal_filter(request.as_of, request.between)?;
    let (traceparent, trace_id) = traceparent_for_query(&headers);
    let tags = tags_from_headers(&headers).map_err(AppError::Validation)?;
    let shard_count = active_shard_count(&state, &namespace).await;
    let output = run_query_leg(
        &state,
        &namespace,
        request,
        QueryRunConfig {
            watermark,
            inject_filter,
            temporal_filter,
            shard_count,
            allow_cursor: true,
        },
    )
    .await?;

    let (timestamp, timestamp_nanos) = now_timestamp();
    let top_result_ids = output
        .results
        .iter()
        .take(10)
        .map(|result| result.id.clone())
        .collect();
    let entry = SearchHistoryEntry {
        timestamp,
        timestamp_nanos,
        namespace: namespace.clone(),
        trace_id: Some(trace_id),
        raw_query: header_to_string(&headers, "x-hevlayer-search-query"),
        stable_as_of: watermark,
        query: search_history_query_summary(&output.request),
        top_result_ids,
        tags,
    };
    let aerospike = Arc::clone(&state.aerospike);
    let s3 = Arc::clone(&state.s3);
    tokio::spawn(async move {
        log_search_history(aerospike, s3, entry).await;
    });

    let mut response_headers = HeaderMap::new();
    if let Ok(value) = HeaderValue::from_str(&traceparent) {
        response_headers.insert(TRACEPARENT_HEADER, value);
    }
    insert_optional_u64_header(&mut response_headers, LAYER_STABLE_AS_OF_HEADER, watermark);
    if let Some(next_cursor) = output.next_cursor.as_ref() {
        if let Ok(value) = HeaderValue::from_str(next_cursor) {
            response_headers.insert(LAYER_NEXT_CURSOR_HEADER, value);
        }
    }
    if output.warn_vector_dropped {
        response_headers.insert(
            LAYER_WARNING_HEADER,
            HeaderValue::from_static(VECTOR_ATTRIBUTE_DROPPED_WARNING),
        );
    }

    Ok((response_headers, Json(query_response_body(&output.results))).into_response())
}

pub(crate) async fn run_query_leg(
    state: &AppState,
    namespace: &str,
    request: QueryRequest,
    config: QueryRunConfig,
) -> Result<QueryLegOutput, AppError> {
    let total_start = Instant::now();
    let mut tpuf_seconds = 0.0;
    let has_filter = request.filters.is_some() || config.temporal_filter.is_some();

    let query_vector = resolve_query_vector(state, namespace, &request).await?;
    let mut request = request;
    request.vector = Some(query_vector);

    let warn_vector_dropped =
        include_attributes_requests_vector(request.include_attributes.as_ref());

    let cursor = match request.cursor.as_deref() {
        Some(_) if !config.allow_cursor => {
            return Err(AppError::Validation(
                "multi-query legs must not include cursor; pagination is single-query only"
                    .to_string(),
            ));
        }
        Some(s) => Some(QueryCursor::decode(s).map_err(AppError::Validation)?),
        None => None,
    };
    let cursor_filter = cursor.as_ref().map(cursor_band_filter);

    let base_filter = combine_optional(request.filters.as_ref(), cursor_filter.as_ref());
    let first_filter = compose_read_filter(
        base_filter.as_ref(),
        config.temporal_filter.as_ref(),
        config.inject_filter.then_some(config.watermark).flatten(),
    );

    let tpuf_start = Instant::now();
    let first = query_turbopuffer(
        state,
        namespace,
        &request,
        first_filter.as_ref(),
        config.shard_count,
    )
    .await;
    tpuf_seconds += tpuf_start.elapsed().as_secs_f64();

    let results = match first {
        Ok(rows) => rows,
        Err(error) if error.is_rate_limited() && !config.inject_filter => {
            warn!(
                namespace = %namespace,
                %error,
                "turbopuffer 429 on unfiltered query; retrying with watermark filter",
            );
            let retry_filter = compose_read_filter(
                base_filter.as_ref(),
                config.temporal_filter.as_ref(),
                config.watermark,
            );
            let tpuf_start = Instant::now();
            let retry = query_turbopuffer(
                state,
                namespace,
                &request,
                retry_filter.as_ref(),
                config.shard_count,
            )
            .await;
            tpuf_seconds += tpuf_start.elapsed().as_secs_f64();
            match retry {
                Ok(rows) => rows,
                Err(e) => {
                    state.metrics.observe_query(
                        DIRECT_PIPELINE_ID,
                        namespace,
                        STATUS_TPUF_ERROR,
                        total_start.elapsed().as_secs_f64(),
                        tpuf_seconds,
                        has_filter,
                        true,
                    );
                    if AppError::is_store_support_error(&e) {
                        return Err(AppError::from_store_support_error(
                            e,
                            Some("search".to_string()),
                            Some("queryNamespace".to_string()),
                        ));
                    }
                    return Err(AppError::from_turbopuffer(
                        e,
                        "Turbopuffer query failed (retry)",
                    ));
                }
            }
        }
        Err(e) => {
            state.metrics.observe_query(
                DIRECT_PIPELINE_ID,
                namespace,
                STATUS_TPUF_ERROR,
                total_start.elapsed().as_secs_f64(),
                tpuf_seconds,
                has_filter,
                true,
            );
            if AppError::is_store_support_error(&e) {
                return Err(AppError::from_store_support_error(
                    e,
                    Some("search".to_string()),
                    Some("queryNamespace".to_string()),
                ));
            }
            return Err(AppError::from_turbopuffer(e, "Turbopuffer query failed"));
        }
    };

    state.metrics.observe_query(
        DIRECT_PIPELINE_ID,
        namespace,
        STATUS_OK,
        total_start.elapsed().as_secs_f64(),
        tpuf_seconds,
        has_filter,
        true,
    );

    let mut results = results;
    results.sort_by(compare_query_results);
    let top_k = request.top_k as usize;
    if results.len() > top_k {
        results.truncate(top_k);
    }

    let next_cursor = if config.allow_cursor && results.len() == top_k {
        results
            .last()
            .and_then(|r| {
                r.dist.map(|d| QueryCursor {
                    dist: d,
                    id: r.id.clone(),
                })
            })
            .map(|c| c.encode())
    } else {
        None
    };

    Ok(QueryLegOutput {
        request,
        results,
        next_cursor,
        warn_vector_dropped,
    })
}

pub(crate) fn query_response_body(results: &[QueryResult]) -> Value {
    json!({ "rows": query_results_to_rows(results) })
}

pub(crate) fn query_results_to_rows(results: &[QueryResult]) -> Vec<Value> {
    results
        .iter()
        .map(|result| {
            let mut row = serde_json::Map::new();
            row.insert("id".to_string(), Value::String(result.id.clone()));
            if let Some(dist) = result.dist {
                row.insert("$dist".to_string(), Value::from(dist));
            }
            for (key, value) in &result.attributes {
                row.insert(key.clone(), value.clone());
            }
            Value::Object(row)
        })
        .collect()
}

pub(crate) fn insert_optional_u64_header(headers: &mut HeaderMap, name: &str, value: Option<u64>) {
    if let Some(value) = value {
        if let Ok(header_value) = HeaderValue::from_str(&value.to_string()) {
            if let Ok(header_name) = axum::http::header::HeaderName::from_bytes(name.as_bytes()) {
                headers.insert(header_name, header_value);
            }
        }
    }
}

/// Resolve the vector the gateway will forward to Turbopuffer. Validates
/// the spec's exactly-one-of contract on `vector` / `nearest_to_id` (422 on
/// both/neither, or an empty `nearest_to_id`) and runs the search-by-id
/// pull-through for every id, then averages the resolved vectors into a
/// single centroid. 404 if any id has no vector in either layer.
pub(crate) async fn resolve_query_vector(
    state: &AppState,
    namespace: &str,
    request: &QueryRequest,
) -> Result<Vec<f64>, AppError> {
    if request.rank_by.is_some() {
        if request.vector.is_some() || request.nearest_to_id.is_some() {
            return Err(AppError::Validation(
                "`rank_by` is mutually exclusive with `vector` and `nearest_to_id`".to_string(),
            ));
        }
        return Ok(Vec::new());
    }

    match (request.vector.as_ref(), request.nearest_to_id.as_deref()) {
        (Some(_), Some(_)) => Err(AppError::Validation(
            "query request must specify exactly one of `vector` or `nearest_to_id`, not both"
                .to_string(),
        )),
        (None, None) => Err(AppError::Validation(
            "query request must specify either `vector` or `nearest_to_id`".to_string(),
        )),
        (Some(vector), None) => {
            if vector.is_empty() {
                return Err(AppError::Validation(
                    "query `vector` must not be empty".to_string(),
                ));
            }
            Ok(vector.clone())
        }
        (None, Some(ids)) => {
            if ids.is_empty() {
                return Err(AppError::Validation(
                    "query `nearest_to_id` must not be empty".to_string(),
                ));
            }

            // Resolve every seed before pooling so a single miss fails the
            // whole query with a 404 naming the gaps, rather than silently
            // ranking against a partial centroid.
            let mut vectors = Vec::with_capacity(ids.len());
            let mut missing = Vec::new();
            for id in ids {
                match resolve_vector_for_id(state, namespace, id).await? {
                    Some(v) => vectors.push(v),
                    None => missing.push(id.as_str()),
                }
            }
            if !missing.is_empty() {
                return Err(AppError::NotFound(format!(
                    "no stored vector found for nearest_to_id {:?} in namespace '{}'",
                    missing, namespace
                )));
            }

            centroid(&vectors)
        }
    }
}

/// Pull-through resolution for one document's stored vector: Aerospike
/// first, Turbopuffer + best-effort cache backfill on miss. `Ok(None)` means
/// neither layer has a vector for the id (the caller turns that into a 404).
/// Cache errors are non-fatal — we degrade to Turbopuffer like the fetch
/// routes do.
async fn resolve_vector_for_id(
    state: &AppState,
    namespace: &str,
    id: &str,
) -> Result<Option<Vec<f64>>, AppError> {
    match state.aerospike.get_vector(namespace, id).await {
        Ok(Some(v)) => return Ok(Some(v)),
        Ok(None) => {}
        Err(e) => warn!(
            namespace = %namespace,
            id = %id,
            error = %e,
            "Aerospike vector lookup failed; falling over to turbopuffer"
        ),
    }

    let vector = state
        .turbopuffer()
        .fetch_vector(namespace, id)
        .await
        .map_err(|e| AppError::Upstream(format!("Turbopuffer vector fetch failed: {}", e)))?;
    let Some(vector) = vector else {
        return Ok(None);
    };

    // Backfill the cache so the next search-by-id avoids the round trip.
    // Best-effort — a backfill failure does not fail the query.
    if let Err(e) = state.aerospike.put_vector(namespace, id, &vector).await {
        warn!(
            namespace = %namespace,
            id = %id,
            error = %e,
            "Aerospike vector backfill failed (best-effort)"
        );
    }

    Ok(Some(vector))
}

/// Component-wise mean of one or more equal-length vectors — the centroid the
/// gateway ranks against for a `nearest_to_id` query. Each seed contributes
/// equally (weight `1/n`). Returns a Validation error if the inputs disagree
/// on dimensionality; within a namespace they never should, so this only
/// trips on a malformed stored row that would otherwise truncate silently.
fn centroid(vectors: &[Vec<f64>]) -> Result<Vec<f64>, AppError> {
    let dim = vectors[0].len();
    let mut sum = vec![0.0_f64; dim];
    for v in vectors {
        if v.len() != dim {
            return Err(AppError::Validation(format!(
                "nearest_to_id vectors disagree on dimensionality: expected {}, found {}",
                dim,
                v.len()
            )));
        }
        for (acc, x) in sum.iter_mut().zip(v.iter()) {
            *acc += x;
        }
    }
    let n = vectors.len() as f64;
    for acc in sum.iter_mut() {
        *acc /= n;
    }
    Ok(sum)
}

fn should_passthrough_query(body: &Value) -> bool {
    let Some(obj) = body.as_object() else {
        return false;
    };
    (obj.contains_key("rank_by") && !obj.contains_key("top_k"))
        || obj.contains_key("queries")
        || obj.contains_key("aggregate_by")
        || obj.contains_key("group_by")
        || obj.contains_key("limit")
        || obj.contains_key("exclude_attributes")
        || obj.contains_key("distance_metric")
        || obj.contains_key("vector_encoding")
}

async fn query_turbopuffer(
    state: &AppState,
    namespace: &str,
    request: &QueryRequest,
    filters: Option<&Value>,
    shard_count: Option<u64>,
) -> Result<Vec<QueryResult>, TurbopufferError> {
    if let Some(rank_by) = request.rank_by.as_ref() {
        if shard_count.is_some() {
            return Err(TurbopufferError::Other(
                "rank_by queries do not support shard scatter/gather".to_string(),
            ));
        }
        return state
            .turbopuffer()
            .ranked_query(
                namespace,
                rank_by,
                request.top_k,
                filters,
                request.include_attributes.as_ref(),
            )
            .await
            .map(|outcome| outcome.rows);
    }

    // `resolve_query_vector` populates this field before we get here.
    let vector = request
        .vector
        .as_deref()
        .expect("query vector must be resolved before calling query_turbopuffer");

    if let Some(shard_count) = shard_count {
        return scatter_gather_query(
            state.turbopuffer(),
            namespace,
            shard_count,
            vector,
            request.top_k,
            filters,
            request.include_attributes.as_ref(),
        )
        .await;
    }

    state
        .turbopuffer()
        .query(
            namespace,
            vector,
            request.top_k,
            filters,
            request.include_attributes.as_ref(),
        )
        .await
        .map(|outcome| outcome.rows)
}

fn search_history_query_summary(request: &QueryRequest) -> Value {
    json!({
        "vector_len": request.vector.as_ref().map(|v| v.len()).unwrap_or(0),
        "nearest_to_id": request.nearest_to_id.clone(),
        "top_k": request.top_k,
        "filters": request.filters.clone(),
        "as_of": request.as_of,
        "between": request.between,
        "include_attributes": request
            .include_attributes
            .as_ref()
            .map(|include| include.to_turbopuffer_value()),
    })
}

/// Score-band filter for the next page. Strict `$dist > last_dist` with an
/// `id > last_id` tiebreaker, so ties on dist at the boundary don't cause
/// double-counting or dropped results.
fn cursor_band_filter(cursor: &QueryCursor) -> Value {
    json!([
        "Or",
        [
            ["$dist", "Gt", cursor.dist],
            [
                "And",
                [["$dist", "Eq", cursor.dist], ["id", "Gt", &cursor.id]]
            ]
        ]
    ])
}

/// AND two optional filters into one. Used to fold the cursor band into the
/// caller's filter without rebuilding the watermark composition.
pub(crate) fn combine_optional(a: Option<&Value>, b: Option<&Value>) -> Option<Value> {
    match (a, b) {
        (Some(a), Some(b)) => Some(json!(["And", [a.clone(), b.clone()]])),
        (Some(v), None) | (None, Some(v)) => Some(v.clone()),
        (None, None) => None,
    }
}

pub(crate) fn temporal_filter(
    as_of: Option<u64>,
    between: Option<[u64; 2]>,
) -> Result<Option<TemporalFilter>, AppError> {
    match (as_of, between) {
        (Some(_), Some(_)) => Err(AppError::Validation(
            "`as_of` and `between` are mutually exclusive".to_string(),
        )),
        (Some(ts), None) => Ok(Some(TemporalFilter {
            filter: json!([UPSERTED_AT_ATTR, "Lte", ts]),
            upper_bound: ts,
        })),
        (None, Some([lo, hi])) => {
            if lo >= hi {
                return Err(AppError::Validation(
                    "`between` must be [lo, hi] with lo < hi".to_string(),
                ));
            }
            Ok(Some(TemporalFilter {
                filter: json!([
                    "And",
                    [[UPSERTED_AT_ATTR, "Gt", lo], [UPSERTED_AT_ATTR, "Lte", hi]]
                ]),
                upper_bound: hi,
            }))
        }
        (None, None) => Ok(None),
    }
}

pub(crate) fn temporal_filter_from_body(body: &Value) -> Result<Option<TemporalFilter>, AppError> {
    let as_of = match body.get("as_of") {
        Some(Value::Null) | None => None,
        Some(value) => Some(value.as_u64().ok_or_else(|| {
            AppError::Validation("`as_of` must be an epoch-ms integer".to_string())
        })?),
    };
    let between = match body.get("between") {
        Some(Value::Null) | None => None,
        Some(Value::Array(values)) if values.len() == 2 => {
            let lo = values[0].as_u64().ok_or_else(|| {
                AppError::Validation("`between[0]` must be an epoch-ms integer".to_string())
            })?;
            let hi = values[1].as_u64().ok_or_else(|| {
                AppError::Validation("`between[1]` must be an epoch-ms integer".to_string())
            })?;
            Some([lo, hi])
        }
        Some(_) => {
            return Err(AppError::Validation(
                "`between` must be a two-element epoch-ms array".to_string(),
            ))
        }
    };
    temporal_filter(as_of, between)
}

pub(crate) fn compose_read_filter(
    user: Option<&Value>,
    temporal: Option<&TemporalFilter>,
    watermark: Option<u64>,
) -> Option<Value> {
    let temporal = temporal.map(|cut| &cut.filter);
    let scoped = combine_optional(user, temporal);
    combined_filter(scoped.as_ref(), watermark)
}

/// Same ordering the scatter/gather path uses (`shards::compare_query_results`):
/// dist ascending with `id` as a stable tiebreaker. Applied in the single-shot
/// path so cursor pagination sees consistent ordering across shard counts.
pub(crate) fn compare_query_results(a: &QueryResult, b: &QueryResult) -> Ordering {
    match (a.dist, b.dist) {
        (Some(ad), Some(bd)) => ad
            .partial_cmp(&bd)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.id.cmp(&b.id)),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => a.id.cmp(&b.id),
    }
}

/// Combine the caller's filter with the consistency watermark filter.
///
/// Shape matches `scan_page`'s cursor combiner: a 2-element `And` whose second
/// element is a list of children. Returns `None` if neither side contributes.
pub(crate) fn combined_filter(user: Option<&Value>, watermark: Option<u64>) -> Option<Value> {
    let watermark_filter = watermark.map(|ts| json!([UPSERTED_AT_ATTR, "Lte", ts]));
    match (user, watermark_filter) {
        (Some(u), Some(w)) => Some(json!(["And", [u.clone(), w]])),
        (Some(u), None) => Some(u.clone()),
        (None, Some(w)) => Some(w),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_filter_no_watermark_yields_none() {
        assert!(combined_filter(None, None).is_none());
    }

    #[test]
    fn watermark_only_returns_lte_predicate() {
        let got = combined_filter(None, Some(1000)).unwrap();
        assert_eq!(got, json!([UPSERTED_AT_ATTR, "Lte", 1000]));
    }

    #[test]
    fn temporal_as_of_returns_lte_predicate() {
        let got = temporal_filter(Some(1000), None).unwrap().unwrap();
        assert_eq!(got.upper_bound, 1000);
        assert_eq!(got.filter, json!([UPSERTED_AT_ATTR, "Lte", 1000]));
    }

    #[test]
    fn temporal_between_returns_open_closed_window() {
        let got = temporal_filter(None, Some([1000, 2000])).unwrap().unwrap();
        assert_eq!(got.upper_bound, 2000);
        assert_eq!(
            got.filter,
            json!([
                "And",
                [
                    [UPSERTED_AT_ATTR, "Gt", 1000],
                    [UPSERTED_AT_ATTR, "Lte", 2000]
                ]
            ])
        );
    }

    #[test]
    fn temporal_rejects_ambiguous_or_empty_windows() {
        assert!(temporal_filter(Some(1000), Some([1000, 2000])).is_err());
        assert!(temporal_filter(None, Some([2000, 2000])).is_err());
        assert!(temporal_filter(None, Some([3000, 2000])).is_err());
    }

    #[test]
    fn compose_read_filter_conjoins_user_temporal_and_watermark() {
        let user = json!(["color", "Eq", "red"]);
        let temporal = temporal_filter(None, Some([1000, 2000])).unwrap().unwrap();
        let got = compose_read_filter(Some(&user), Some(&temporal), Some(1500)).unwrap();
        assert_eq!(
            got,
            json!([
                "And",
                [
                    [
                        "And",
                        [
                            ["color", "Eq", "red"],
                            [
                                "And",
                                [
                                    [UPSERTED_AT_ATTR, "Gt", 1000],
                                    [UPSERTED_AT_ATTR, "Lte", 2000]
                                ]
                            ]
                        ]
                    ],
                    [UPSERTED_AT_ATTR, "Lte", 1500]
                ]
            ])
        );
    }

    #[test]
    fn user_only_passes_through() {
        let user = json!(["color", "Eq", "red"]);
        assert_eq!(combined_filter(Some(&user), None).unwrap(), user);
    }

    #[test]
    fn cursor_band_is_strict_gt_with_id_tiebreak() {
        let cursor = QueryCursor {
            dist: 0.42,
            id: "doc-123".to_string(),
        };
        assert_eq!(
            cursor_band_filter(&cursor),
            json!([
                "Or",
                [
                    ["$dist", "Gt", 0.42],
                    ["And", [["$dist", "Eq", 0.42], ["id", "Gt", "doc-123"]]]
                ]
            ])
        );
    }

    #[test]
    fn combine_optional_ands_when_both_present() {
        let a = json!(["color", "Eq", "red"]);
        let b = json!(["$dist", "Gt", 0.5]);
        assert_eq!(
            combine_optional(Some(&a), Some(&b)).unwrap(),
            json!(["And", [a, b]])
        );
    }

    #[test]
    fn combine_optional_passes_through_single_side() {
        let a = json!(["color", "Eq", "red"]);
        assert_eq!(combine_optional(Some(&a), None).unwrap(), a);
        assert_eq!(combine_optional(None, Some(&a)).unwrap(), a);
        assert!(combine_optional(None, None).is_none());
    }

    #[test]
    fn cursor_round_trip_preserves_dist_and_id() {
        let cursor = QueryCursor {
            dist: 0.1234,
            id: "doc-with/slashes+and=stuff".to_string(),
        };
        let encoded = cursor.encode();
        let decoded = QueryCursor::decode(&encoded).unwrap();
        assert_eq!(decoded, cursor);
    }

    #[test]
    fn cursor_decode_rejects_garbage() {
        assert!(QueryCursor::decode("not-base64!@#").is_err());
        assert!(QueryCursor::decode("YQ").is_err()); // base64 of "a" → not valid JSON
    }

    #[test]
    fn cursor_decode_rejects_missing_dist() {
        // Well-formed base64 + JSON but `dist` is null → serde rejects it
        // because the field is typed `f64`.
        use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
        use base64::Engine;
        let encoded = B64.encode(b"{\"dist\": null, \"id\": \"x\"}");
        assert!(QueryCursor::decode(&encoded).is_err());
    }

    #[test]
    fn comparator_sorts_by_dist_then_id() {
        let mut rows = [
            QueryResult {
                id: "b".into(),
                dist: Some(0.5),
                attributes: Default::default(),
            },
            QueryResult {
                id: "a".into(),
                dist: Some(0.5),
                attributes: Default::default(),
            },
            QueryResult {
                id: "c".into(),
                dist: Some(0.1),
                attributes: Default::default(),
            },
        ];
        rows.sort_by(compare_query_results);
        let ids: Vec<&str> = rows.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, vec!["c", "a", "b"]);
    }

    #[test]
    fn centroid_of_one_is_identity() {
        let v = vec![1.0, 2.0, 3.0];
        assert_eq!(centroid(std::slice::from_ref(&v)).unwrap(), v);
    }

    #[test]
    fn centroid_averages_component_wise() {
        let got = centroid(&[vec![0.0, 2.0, 4.0], vec![2.0, 0.0, 8.0]]).unwrap();
        assert_eq!(got, vec![1.0, 1.0, 6.0]);
    }

    #[test]
    fn centroid_rejects_mismatched_dims() {
        let err = centroid(&[vec![1.0, 2.0], vec![3.0]]).unwrap_err();
        assert!(matches!(err, AppError::Validation(_)));
    }

    #[test]
    fn ands_user_filter_with_watermark() {
        let user = json!(["color", "Eq", "red"]);
        let got = combined_filter(Some(&user), Some(2500)).unwrap();
        assert_eq!(
            got,
            json!([
                "And",
                [["color", "Eq", "red"], [UPSERTED_AT_ATTR, "Lte", 2500]]
            ])
        );
    }
}
