use std::cmp::Ordering;
use std::sync::Arc;
use std::time::Instant;

use axum::http::{HeaderMap, HeaderValue};
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures::future::try_join_all;
use serde_json::{json, Map, Value};
use tracing::warn;

use crate::clients::turbopuffer::TurbopufferError;
use crate::error::AppError;
use crate::history::{
    header_to_string, log_search_history, now_timestamp, tags_from_headers, traceparent_for_query,
    TRACEPARENT_HEADER,
};
use crate::metrics::{DIRECT_PIPELINE_ID, STATUS_OK, STATUS_TPUF_ERROR};
use crate::models::{IncludeAttributes, QueryRequest, QueryResult, SearchHistoryEntry};
use crate::routes::query::{
    compose_read_filter, include_attributes_requests_vector, insert_optional_u64_header,
    query_results_to_rows, resolve_query_vector, temporal_filter_from_body, TemporalFilter,
    LAYER_STABLE_AS_OF_HEADER, LAYER_WARNING_HEADER, VECTOR_ATTRIBUTE_DROPPED_WARNING,
};
use crate::shards::{active_shard_count, combine_filter as combine_shard_filter, shard_filter};
use crate::AppState;

const MIN_MULTI_QUERY_LEGS: usize = 2;
const MAX_MULTI_QUERY_LEGS: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RankMode {
    Ann,
    Bm25,
}

impl RankMode {
    pub(crate) fn from_rank_by(rank_by: &Value) -> Self {
        if rank_by
            .as_array()
            .and_then(|items| items.get(1))
            .and_then(Value::as_str)
            .is_some_and(|mode| mode.eq_ignore_ascii_case("BM25"))
        {
            Self::Bm25
        } else {
            Self::Ann
        }
    }
}

#[derive(Debug, Clone)]
struct PreparedLeg {
    rank_by: Value,
    top_k: u32,
    base_filter: Option<Value>,
    include_attributes: Option<IncludeAttributes>,
    raw_body: Map<String, Value>,
    mode: RankMode,
    warning_vector_dropped: bool,
    summary: Value,
}

pub async fn multi_query(
    state: Arc<AppState>,
    namespace: String,
    headers: HeaderMap,
    body: Value,
) -> Result<Response, AppError> {
    if body.get("cursor").is_some() {
        state
            .metrics
            .observe_multi_query(&namespace, crate::metrics::STATUS_LAYER_ERROR, 0, 0);
        return Err(AppError::Validation(
            "multi-query does not support top-level cursor; pagination is single-query only"
                .to_string(),
        ));
    }

    let queries = body
        .get("queries")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            AppError::Validation("multi-query body must contain `queries`".to_string())
        })?;
    if !(MIN_MULTI_QUERY_LEGS..=MAX_MULTI_QUERY_LEGS).contains(&queries.len()) {
        state.metrics.observe_multi_query(
            &namespace,
            crate::metrics::STATUS_LAYER_ERROR,
            queries.len(),
            0,
        );
        return Err(AppError::Validation(format!(
            "multi-query requires {}..{} legs; got {}",
            MIN_MULTI_QUERY_LEGS,
            MAX_MULTI_QUERY_LEGS,
            queries.len()
        )));
    }

    let (watermark, inject_filter) = state.query_consistency(&namespace);
    let temporal_filter = temporal_filter_from_body(&body)?;
    let shard_count = active_shard_count(&state, &namespace).await;
    let (traceparent, trace_id) = traceparent_for_query(&headers);
    let tags = tags_from_headers(&headers).map_err(AppError::Validation)?;

    let mut prepared = Vec::with_capacity(queries.len());
    for (idx, leg) in queries.iter().enumerate() {
        prepared.push(
            prepare_leg(&state, &namespace, idx, leg)
                .await
                .map_err(|err| with_leg_index(idx, err))?,
        );
    }

    let any_warning = prepared.iter().any(|leg| leg.warning_vector_dropped);
    let (response_body, upstream_calls) = if let Some(shards) = shard_count {
        run_sharded_multi_query(
            &state,
            &namespace,
            &prepared,
            watermark,
            inject_filter,
            temporal_filter.as_ref(),
            shards,
        )
        .await?
    } else {
        run_upstream_multi_query(
            &state,
            &namespace,
            &prepared,
            watermark,
            inject_filter,
            temporal_filter.as_ref(),
        )
        .await?
    };

    let top_result_ids = top_result_ids_from_multi_response(&response_body);
    let (timestamp, timestamp_nanos) = now_timestamp();
    let entry = SearchHistoryEntry {
        timestamp,
        timestamp_nanos,
        namespace: namespace.clone(),
        trace_id: Some(trace_id),
        raw_query: header_to_string(&headers, "x-hevlayer-search-query"),
        stable_as_of: watermark,
        query: json!({
            "leg_count": prepared.len(),
            "queries": prepared.iter().map(|leg| leg.summary.clone()).collect::<Vec<_>>(),
        }),
        top_result_ids,
        tags,
    };
    let aerospike = Arc::clone(&state.aerospike);
    let s3 = Arc::clone(&state.s3);
    tokio::spawn(async move {
        log_search_history(aerospike, s3, entry).await;
    });

    state
        .metrics
        .observe_multi_query(&namespace, STATUS_OK, prepared.len(), upstream_calls);

    let mut response_headers = HeaderMap::new();
    if let Ok(value) = HeaderValue::from_str(&traceparent) {
        response_headers.insert(TRACEPARENT_HEADER, value);
    }
    insert_optional_u64_header(&mut response_headers, LAYER_STABLE_AS_OF_HEADER, watermark);
    if any_warning {
        response_headers.insert(
            LAYER_WARNING_HEADER,
            HeaderValue::from_static(VECTOR_ATTRIBUTE_DROPPED_WARNING),
        );
    }

    Ok((response_headers, Json(response_body)).into_response())
}

async fn prepare_leg(
    state: &AppState,
    namespace: &str,
    idx: usize,
    leg: &Value,
) -> Result<PreparedLeg, AppError> {
    let mut raw_body = leg.as_object().cloned().ok_or_else(|| {
        AppError::Validation(format!("multi-query leg {idx} must be a JSON object"))
    })?;
    if raw_body.contains_key("cursor") {
        return Err(AppError::Validation(
            "cursor is not supported on multi-query legs; pagination is single-query only"
                .to_string(),
        ));
    }
    if raw_body.contains_key("as_of") || raw_body.contains_key("between") {
        return Err(AppError::Validation(
            "as_of/between are top-level multi-query selectors; per-leg temporal selectors are not supported"
                .to_string(),
        ));
    }

    let has_layer_vector =
        raw_body.contains_key("vector") || raw_body.contains_key("nearest_to_id");
    let rank_by = if has_layer_vector {
        let request: QueryRequest = serde_json::from_value(Value::Object(raw_body.clone()))
            .map_err(|e| AppError::Validation(format!("invalid query leg: {e}")))?;
        let vector = resolve_query_vector(state, namespace, &request).await?;
        raw_body.remove("vector");
        raw_body.remove("nearest_to_id");
        raw_body.insert("rank_by".to_string(), json!(["vector", "ANN", vector]));
        raw_body
            .entry("top_k".to_string())
            .or_insert_with(|| Value::from(request.top_k));
        raw_body
            .get("rank_by")
            .cloned()
            .expect("rank_by inserted for layer vector leg")
    } else {
        raw_body.get("rank_by").cloned().ok_or_else(|| {
            AppError::Validation(
                "multi-query leg must contain `rank_by`, `vector`, or `nearest_to_id`".to_string(),
            )
        })?
    };

    let top_k = top_k_for_leg(&raw_body)?;
    let base_filter = raw_body.get("filters").cloned();
    let (include_attributes, warning_vector_dropped) = sanitize_include_attributes(&mut raw_body)?;
    let mode = RankMode::from_rank_by(&rank_by);

    raw_body.insert(
        "consistency".to_string(),
        json!({
            "level": "eventual",
        }),
    );

    Ok(PreparedLeg {
        rank_by,
        top_k,
        base_filter,
        include_attributes,
        raw_body,
        mode,
        warning_vector_dropped,
        summary: leg_summary(leg),
    })
}

fn sanitize_include_attributes(
    raw_body: &mut Map<String, Value>,
) -> Result<(Option<IncludeAttributes>, bool), AppError> {
    let Some(value) = raw_body.get("include_attributes").cloned() else {
        return Ok((None, false));
    };
    let include: IncludeAttributes = serde_json::from_value(value.clone()).map_err(|e| {
        AppError::Validation(format!(
            "invalid include_attributes on multi-query leg: {e}"
        ))
    })?;
    let warning = include_attributes_requests_vector(Some(&include));
    let sanitized = match include {
        IncludeAttributes::Fields(mut fields) => {
            fields.retain(|field| field != "vector");
            let sanitized = IncludeAttributes::Fields(fields.clone());
            raw_body.insert("include_attributes".to_string(), json!(fields));
            sanitized
        }
        IncludeAttributes::All(value) => {
            raw_body.insert("include_attributes".to_string(), Value::Bool(value));
            IncludeAttributes::All(value)
        }
    };
    Ok((Some(sanitized), warning))
}

fn top_k_for_leg(raw_body: &Map<String, Value>) -> Result<u32, AppError> {
    let raw = raw_body
        .get("top_k")
        .or_else(|| raw_body.get("limit"))
        .and_then(Value::as_u64)
        .unwrap_or(10);
    if raw == 0 || raw > u32::MAX as u64 {
        return Err(AppError::Validation(
            "multi-query leg top_k/limit must be a positive u32".to_string(),
        ));
    }
    Ok(raw as u32)
}

fn render_upstream_leg(
    leg: &PreparedLeg,
    watermark: Option<u64>,
    inject_filter: bool,
    temporal_filter: Option<&TemporalFilter>,
) -> Value {
    let mut body = leg.raw_body.clone();
    match combined_filter_for_leg(
        leg.base_filter.as_ref(),
        watermark,
        inject_filter,
        temporal_filter,
    ) {
        Some(filter) => {
            body.insert("filters".to_string(), filter);
        }
        None => {
            body.remove("filters");
        }
    }
    Value::Object(body)
}

fn combined_filter_for_leg(
    base_filter: Option<&Value>,
    watermark: Option<u64>,
    inject_filter: bool,
    temporal_filter: Option<&TemporalFilter>,
) -> Option<Value> {
    compose_read_filter(
        base_filter,
        temporal_filter,
        inject_filter.then_some(watermark).flatten(),
    )
}

async fn run_upstream_multi_query(
    state: &AppState,
    namespace: &str,
    prepared: &[PreparedLeg],
    watermark: Option<u64>,
    inject_filter: bool,
    temporal_filter: Option<&TemporalFilter>,
) -> Result<(Value, usize), AppError> {
    let legs: Vec<Value> = prepared
        .iter()
        .map(|leg| render_upstream_leg(leg, watermark, inject_filter, temporal_filter))
        .collect();
    let first = state
        .turbopuffer()
        .multi_ranked_query(namespace, &legs, None)
        .await;
    match first {
        Ok(body) => Ok((body, 1)),
        Err(error) if error.is_rate_limited() && !inject_filter => {
            warn!(
                namespace = %namespace,
                %error,
                "turbopuffer 429 on unfiltered multi-query; retrying with watermark filter",
            );
            let retry_legs: Vec<Value> = prepared
                .iter()
                .map(|leg| render_upstream_leg(leg, watermark, true, temporal_filter))
                .collect();
            state
                .turbopuffer()
                .multi_ranked_query(namespace, &retry_legs, None)
                .await
                .map(|body| (body, 2))
                .map_err(|e| {
                    state.metrics.observe_multi_query(
                        namespace,
                        STATUS_TPUF_ERROR,
                        prepared.len(),
                        2,
                    );
                    AppError::from_turbopuffer(e, "Turbopuffer multi-query failed (retry)")
                })
        }
        Err(e) => {
            state
                .metrics
                .observe_multi_query(namespace, STATUS_TPUF_ERROR, prepared.len(), 1);
            Err(AppError::from_turbopuffer(
                e,
                "Turbopuffer multi-query failed",
            ))
        }
    }
}

async fn run_sharded_multi_query(
    state: &AppState,
    namespace: &str,
    prepared: &[PreparedLeg],
    watermark: Option<u64>,
    inject_filter: bool,
    temporal_filter: Option<&TemporalFilter>,
    shard_count: u64,
) -> Result<(Value, usize), AppError> {
    let mut upstream_calls = 0usize;
    let mut results = Vec::with_capacity(prepared.len());
    for leg in prepared {
        let (rows, calls) = run_sharded_leg(
            state,
            namespace,
            leg,
            watermark,
            inject_filter,
            temporal_filter,
            shard_count,
        )
        .await?;
        upstream_calls += calls;
        results.push(json!({ "rows": rows_to_values(&rows, leg.mode) }));
    }
    Ok((
        json!({
            "results": results,
        }),
        upstream_calls,
    ))
}

async fn run_sharded_leg(
    state: &AppState,
    namespace: &str,
    leg: &PreparedLeg,
    watermark: Option<u64>,
    inject_filter: bool,
    temporal_filter: Option<&TemporalFilter>,
    shard_count: u64,
) -> Result<(Vec<QueryResult>, usize), AppError> {
    let total_start = Instant::now();
    let mut tpuf_seconds = 0.0;
    let first_filter = combined_filter_for_leg(
        leg.base_filter.as_ref(),
        watermark,
        inject_filter,
        temporal_filter,
    );
    let tpuf_start = Instant::now();
    let first =
        scatter_gather_ranked_query(state, namespace, leg, first_filter.as_ref(), shard_count)
            .await;
    tpuf_seconds += tpuf_start.elapsed().as_secs_f64();

    let (mut rows, upstream_calls) = match first {
        Ok(rows) => (rows, shard_count as usize),
        Err(error) if error.is_rate_limited() && !inject_filter => {
            warn!(
                namespace = %namespace,
                %error,
                "turbopuffer 429 on unfiltered sharded multi-query leg; retrying with watermark filter",
            );
            let retry_filter =
                combined_filter_for_leg(leg.base_filter.as_ref(), watermark, true, temporal_filter);
            let tpuf_start = Instant::now();
            let retry = scatter_gather_ranked_query(
                state,
                namespace,
                leg,
                retry_filter.as_ref(),
                shard_count,
            )
            .await;
            tpuf_seconds += tpuf_start.elapsed().as_secs_f64();
            match retry {
                Ok(rows) => (rows, (shard_count as usize) * 2),
                Err(e) => {
                    state.metrics.observe_query(
                        DIRECT_PIPELINE_ID,
                        namespace,
                        STATUS_TPUF_ERROR,
                        total_start.elapsed().as_secs_f64(),
                        tpuf_seconds,
                        leg.base_filter.is_some(),
                        true,
                    );
                    return Err(AppError::from_turbopuffer(
                        e,
                        "Turbopuffer multi-query leg failed (retry)",
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
                leg.base_filter.is_some(),
                true,
            );
            return Err(AppError::from_turbopuffer(
                e,
                "Turbopuffer multi-query leg failed",
            ));
        }
    };

    state.metrics.observe_query(
        DIRECT_PIPELINE_ID,
        namespace,
        STATUS_OK,
        total_start.elapsed().as_secs_f64(),
        tpuf_seconds,
        leg.base_filter.is_some(),
        true,
    );

    rows.sort_by(|a, b| compare_ranked_results(a, b, leg.mode));
    rows.truncate(leg.top_k as usize);
    Ok((rows, upstream_calls))
}

async fn scatter_gather_ranked_query(
    state: &AppState,
    namespace: &str,
    leg: &PreparedLeg,
    filters: Option<&Value>,
    shard_count: u64,
) -> Result<Vec<QueryResult>, TurbopufferError> {
    let futures = (0..shard_count).map(|shard| {
        let filter = combine_shard_filter(filters, shard_filter(shard));
        async move {
            state
                .turbopuffer()
                .ranked_query(
                    namespace,
                    &leg.rank_by,
                    leg.top_k,
                    Some(&filter),
                    leg.include_attributes.as_ref(),
                )
                .await
                .map(|outcome| outcome.rows)
        }
    });
    Ok(try_join_all(futures).await?.into_iter().flatten().collect())
}

fn rows_to_values(rows: &[QueryResult], mode: RankMode) -> Vec<Value> {
    let mut values = query_results_to_rows(rows);
    if mode == RankMode::Bm25 {
        for row in &mut values {
            if let Some(obj) = row.as_object_mut() {
                if let Some(score) = obj.remove("$dist") {
                    obj.insert("$score".to_string(), score);
                }
            }
        }
    }
    values
}

pub(crate) fn compare_ranked_results(a: &QueryResult, b: &QueryResult, mode: RankMode) -> Ordering {
    let ordering = match (a.dist, b.dist) {
        (Some(a_score), Some(b_score)) => a_score.partial_cmp(&b_score).unwrap_or(Ordering::Equal),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    };
    let ordering = if mode == RankMode::Bm25 {
        ordering.reverse()
    } else {
        ordering
    };
    ordering.then_with(|| a.id.cmp(&b.id))
}

fn top_result_ids_from_multi_response(body: &Value) -> Vec<String> {
    body.get("results")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .flat_map(|leg| {
            leg.get("rows")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        })
        .filter_map(|row| row.get("id").and_then(Value::as_str).map(str::to_string))
        .take(10)
        .collect()
}

fn leg_summary(leg: &Value) -> Value {
    let Some(obj) = leg.as_object() else {
        return json!({});
    };
    json!({
        "rank_by": obj.get("rank_by").cloned(),
        "vector_len": obj
            .get("vector")
            .and_then(Value::as_array)
            .map(Vec::len),
        "nearest_to_id": obj.get("nearest_to_id").cloned(),
        "top_k": obj.get("top_k").or_else(|| obj.get("limit")).cloned(),
        "filters": obj.get("filters").cloned(),
        "include_attributes": obj.get("include_attributes").cloned(),
    })
}

fn with_leg_index(idx: usize, err: AppError) -> AppError {
    let leg = idx + 1;
    match err {
        AppError::Validation(msg) => AppError::Validation(format!("multi-query leg {leg}: {msg}")),
        AppError::NotFound(msg) => AppError::NotFound(format!("multi-query leg {leg}: {msg}")),
        AppError::Upstream(msg) => AppError::Upstream(format!("multi-query leg {leg}: {msg}")),
        other => other,
    }
}
