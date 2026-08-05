//! Query router (RFC 0044, phase 1): the `Auto` rank expression.
//!
//! The route is chosen from the shape of the input text alone — vector
//! availability never changes which route is best, only whether it can
//! execute in this request. The gateway never embeds: a vectorless query
//! routed `semantic` or `fused` gets a deferral response (the routing
//! decision, `executed: false`, no rows) and the application embeds and
//! re-issues with the route forced.

use std::sync::Arc;

use axum::http::{HeaderMap, HeaderValue};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Map, Value};

use crate::error::AppError;
use crate::history::{
    header_to_string, log_search_history, now_timestamp, tags_from_headers, traceparent_for_query,
    TRACEPARENT_HEADER,
};
use crate::models::{IncludeAttributes, QueryRequest, SearchHistoryEntry};
use crate::routes::hybrid_text::{
    parse_fuzziness, parse_hybrid_request, parse_stopwords, run_hybrid_text, tokenize_query_input,
    FusedCursor, Fuzziness, HybridRequest, HybridTextExpr, LegSpec, StopwordsOption,
};
use crate::routes::query::{
    insert_optional_u64_header, query_results_to_rows, run_query_leg, QueryRunConfig,
    LAYER_NEXT_CURSOR_HEADER, LAYER_STABLE_AS_OF_HEADER,
};
use crate::AppState;

pub(crate) const ROUTING_POLICY_VERSION: &str = "v1";
/// v1 thresholds (placeholders to be settled against the design partner's
/// logged queries — the `policy` echo versions any change).
const HYBRID_TEXT_MAX_TOKENS: usize = 2;
const SEMANTIC_MIN_TOKENS: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Route {
    HybridText,
    Semantic,
    Fused,
}

impl Route {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HybridText => "hybrid_text",
            Self::Semantic => "semantic",
            Self::Fused => "fused",
        }
    }

    fn parse(s: &str) -> Result<Self, AppError> {
        match s {
            "hybrid_text" => Ok(Self::HybridText),
            "semantic" => Ok(Self::Semantic),
            "fused" => Ok(Self::Fused),
            other => Err(AppError::Validation(format!(
                "Auto route must be \"hybrid_text\", \"semantic\", or \"fused\"; got \"{other}\""
            ))),
        }
    }

    pub(crate) fn needs_vector(self) -> bool {
        !matches!(self, Self::HybridText)
    }
}

/// The documented v1 policy: route from token count alone.
pub(crate) fn route_for_tokens(token_count: usize) -> Route {
    if token_count <= HYBRID_TEXT_MAX_TOKENS {
        Route::HybridText
    } else if token_count >= SEMANTIC_MIN_TOKENS {
        Route::Semantic
    } else {
        Route::Fused
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AutoExpr {
    pub field: String,
    pub input: String,
    pub forced_route: Option<Route>,
    pub vector: Option<Vec<f64>>,
    /// Optional fuzziness override forwarded to the `HybridText` expansion
    /// on the `hybrid_text`/`fused` routes. `None` keeps the
    /// documented hybrid default (`auto`); no effect on `semantic`.
    pub fuzziness: Option<Fuzziness>,
    /// Optional stop-word policy override (RFC 0090), forwarded like
    /// `fuzziness`. `None` keeps the default (English list, on).
    pub stopwords: Option<StopwordsOption>,
}

/// Parse `["field", "Auto", "input", {route?, vector?, fuzziness?, stopwords?}?]`.
pub(crate) fn parse_auto_expr(rank_by: &Value) -> Result<AutoExpr, AppError> {
    let tuple = rank_by
        .as_array()
        .filter(|t| t.len() == 3 || t.len() == 4)
        .ok_or_else(|| {
            AppError::Validation(
                "Auto rank_by must be [field, \"Auto\", input, {options}?]".to_string(),
            )
        })?;
    let field = tuple[0]
        .as_str()
        .filter(|f| !f.is_empty())
        .ok_or_else(|| {
            AppError::Validation("Auto field (rank_by[0]) must be a string".to_string())
        })?
        .to_string();
    let input = tuple[2]
        .as_str()
        .ok_or_else(|| {
            AppError::Validation("Auto input (rank_by[2]) must be a string".to_string())
        })?
        .to_string();

    let mut forced_route = None;
    let mut vector = None;
    let mut fuzziness = None;
    let mut stopwords = None;
    if let Some(options) = tuple.get(3) {
        let options = options.as_object().ok_or_else(|| {
            AppError::Validation("Auto options (rank_by[3]) must be an object".to_string())
        })?;
        for (key, value) in options {
            match key.as_str() {
                "route" => match value {
                    Value::Null => {}
                    Value::String(s) if s == "auto" => {}
                    Value::String(s) => forced_route = Some(Route::parse(s)?),
                    _ => {
                        return Err(AppError::Validation(
                            "Auto route must be a string".to_string(),
                        ));
                    }
                },
                "vector" => {
                    let parsed: Vec<f64> = serde_json::from_value(value.clone()).map_err(|e| {
                        AppError::Validation(format!("Auto vector must be a number array: {e}"))
                    })?;
                    if parsed.is_empty() {
                        return Err(AppError::Validation(
                            "Auto vector must not be empty".to_string(),
                        ));
                    }
                    vector = Some(parsed);
                }
                "fuzziness" => fuzziness = Some(parse_fuzziness(value)?),
                "stopwords" => stopwords = Some(parse_stopwords(value)?),
                other => {
                    return Err(AppError::Validation(format!(
                        "unknown Auto option `{other}`"
                    )));
                }
            }
        }
    }

    Ok(AutoExpr {
        field,
        input,
        forced_route,
        vector,
        fuzziness,
        stopwords,
    })
}

pub(crate) fn routing_echo(route: Route, forced: bool, tokens: usize, executed: bool) -> Value {
    json!({
        "route": route.as_str(),
        "policy": if forced { "forced" } else { ROUTING_POLICY_VERSION },
        "tokens": tokens,
        "executed": executed,
    })
}

pub async fn auto_query(
    state: Arc<AppState>,
    namespace: String,
    headers: HeaderMap,
    body: Map<String, Value>,
) -> Result<Response, AppError> {
    let rank_by = body.get("rank_by").cloned().unwrap_or(Value::Null);
    let expr = parse_auto_expr(&rank_by)?;
    let request = parse_hybrid_request(&body)?;

    let token_count = tokenize_query_input(&expr.input).tokens.len();
    if token_count == 0 {
        return Err(AppError::Validation(
            "Auto input yields no tokens under the tokenizer policy".to_string(),
        ));
    }

    let forced = expr.forced_route.is_some();
    let route = expr
        .forced_route
        .unwrap_or_else(|| route_for_tokens(token_count));
    state.telemetry.touch_auto_routing();

    if route.needs_vector() && expr.vector.is_none() {
        if forced {
            // Forcing a route asserts the caller has the vector; only
            // auto-routing defers.
            return Err(AppError::Validation(format!(
                "forced route \"{}\" requires a vector",
                route.as_str()
            )));
        }
        state
            .metrics
            .observe_query_router(&namespace, route.as_str(), false);
        let body = json!({
            "rows": [],
            "routing": routing_echo(route, false, token_count, false),
        });
        return Ok(Json(body).into_response());
    }

    state
        .metrics
        .observe_query_router(&namespace, route.as_str(), true);
    let routing = routing_echo(route, forced, token_count, true);

    let (rows, hybrid_echo, watermark, next_cursor) = match route {
        Route::HybridText => {
            state.telemetry.touch_hybrid_rrf();
            let out = run_hybrid(&state, &namespace, &expr, &request, None).await?;
            (out.0, Some(out.1), out.2, out.3)
        }
        Route::Fused => {
            state.telemetry.touch_hybrid_rrf();
            let vector = expr.vector.clone().expect("fused route checked vector");
            let ann_leg = LegSpec {
                label: "semantic".to_string(),
                rank_by: json!(["vector", "ANN", vector]),
                filter: request.filters.clone(),
            };
            let out = run_hybrid(&state, &namespace, &expr, &request, Some(ann_leg)).await?;
            (out.0, Some(out.1), out.2, out.3)
        }
        Route::Semantic => {
            let vector = expr.vector.clone().expect("semantic route checked vector");
            run_semantic(&state, &namespace, &request, vector).await?
        }
    };

    log_routed_history(
        &state, &namespace, &headers, &rank_by, &request, &routing, &rows, watermark,
    );

    let mut response_headers = HeaderMap::new();
    let (traceparent, _) = traceparent_for_query(&headers);
    if let Ok(value) = HeaderValue::from_str(&traceparent) {
        response_headers.insert(TRACEPARENT_HEADER, value);
    }
    insert_optional_u64_header(&mut response_headers, LAYER_STABLE_AS_OF_HEADER, watermark);
    if let Some(next_cursor) = next_cursor.as_ref() {
        if let Ok(value) = HeaderValue::from_str(next_cursor) {
            response_headers.insert(LAYER_NEXT_CURSOR_HEADER, value);
        }
    }

    let mut body = Map::new();
    body.insert("rows".to_string(), Value::Array(rows));
    body.insert("routing".to_string(), routing);
    body.insert(
        "next_cursor".to_string(),
        next_cursor.map(Value::String).unwrap_or(Value::Null),
    );
    if let Some(hybrid) = hybrid_echo {
        body.insert("hybrid".to_string(), hybrid);
    }
    Ok((response_headers, Json(Value::Object(body))).into_response())
}

/// Run the hybrid expansion for the `hybrid_text` and `fused` routes with
/// the documented hybrid defaults (the `Auto` surface exposes no hybrid
/// tuning knobs; callers who want them use `HybridText` directly).
pub(crate) async fn run_hybrid(
    state: &AppState,
    namespace: &str,
    expr: &AutoExpr,
    request: &HybridRequest,
    extra_leg: Option<LegSpec>,
) -> Result<(Vec<Value>, Value, Option<u64>, Option<String>), AppError> {
    let hybrid_expr = HybridTextExpr {
        field: expr.field.clone(),
        input: expr.input.clone(),
        fuzziness: expr.fuzziness.unwrap_or(Fuzziness::Auto),
        stopwords: expr.stopwords.clone().unwrap_or_default(),
        rank_constant: 60,
        per_leg_limit: None,
        threads: None,
    };
    let out = run_hybrid_text(state, namespace, &hybrid_expr, request, extra_leg).await?;
    Ok((out.rows, out.echo, out.watermark, out.next_cursor))
}

/// The `semantic` route: one ANN query over the supplied vector, through
/// the standard single-query path (watermark cut, 429 retry, ordering).
pub(crate) async fn run_semantic(
    state: &AppState,
    namespace: &str,
    request: &HybridRequest,
    vector: Vec<f64>,
) -> Result<(Vec<Value>, Option<Value>, Option<u64>, Option<String>), AppError> {
    let include_attributes: Option<IncludeAttributes> = match &request.include_attributes {
        None => None,
        Some(value) => Some(
            serde_json::from_value(value.clone())
                .map_err(|e| AppError::Validation(format!("invalid include_attributes: {e}")))?,
        ),
    };
    let offset = request.cursor.as_ref().map_or(0, |cursor| cursor.offset());
    let page_end = offset.checked_add(request.top_k).ok_or_else(|| {
        AppError::Validation("cursor offset exceeds routed pagination depth".to_string())
    })?;
    let fetch_top_k = page_end.saturating_add(1).min(10_000);
    let query = QueryRequest {
        vector: Some(vector),
        nearest_to_id: None,
        top_k: fetch_top_k,
        filters: request.filters.clone(),
        as_of: request.as_of,
        between: request.between,
        include_attributes,
        cursor: None,
        rank_by: None,
    };
    let (watermark, inject_filter) = state.query_consistency(namespace);
    let shard_count = crate::shards::active_shard_count(state, namespace).await;
    let output = run_query_leg(
        state,
        namespace,
        query,
        QueryRunConfig {
            watermark,
            inject_filter,
            temporal_filter: request.temporal_filter.clone(),
            shard_count,
            allow_cursor: false,
        },
    )
    .await?;
    let mut rows = query_results_to_rows(&output.results);
    let has_more = rows.len() > page_end as usize;
    rows = rows
        .into_iter()
        .skip(offset as usize)
        .take(request.top_k as usize)
        .collect();
    let next_cursor = if has_more && page_end < 10_000 {
        Some(FusedCursor::next(page_end).encode())
    } else {
        None
    };
    Ok((rows, None, watermark, next_cursor))
}

#[allow(clippy::too_many_arguments)]
fn log_routed_history(
    state: &AppState,
    namespace: &str,
    headers: &HeaderMap,
    rank_by: &Value,
    request: &HybridRequest,
    routing: &Value,
    rows: &[Value],
    watermark: Option<u64>,
) {
    let (_, trace_id) = traceparent_for_query(headers);
    let tags = tags_from_headers(headers).unwrap_or_default();
    let (timestamp, timestamp_nanos) = now_timestamp();
    let top_result_ids = rows
        .iter()
        .take(10)
        .filter_map(|row| row.get("id").and_then(Value::as_str).map(str::to_string))
        .collect();
    let entry = SearchHistoryEntry {
        timestamp,
        timestamp_nanos,
        namespace: namespace.to_string(),
        trace_id: Some(trace_id),
        raw_query: header_to_string(headers, "x-hevlayer-search-query"),
        stable_as_of: watermark,
        // The Auto expression plus the decision — not the expanded legs — so
        // replay reproduces the routing and the expansion as a unit, and
        // per-route engagement is measurable against the clickstream.
        query: json!({
            "rank_by": rank_by,
            "top_k": request.top_k,
            "filters": request.filters,
            "as_of": request.as_of,
            "between": request.between,
            "include_leg_breakdown": request.include_leg_breakdown,
            "routing": routing,
        }),
        top_result_ids,
        tags,
    };
    let aerospike = Arc::clone(&state.aerospike);
    let s3 = Arc::clone(&state.s3);
    tokio::spawn(async move {
        log_search_history(aerospike, s3, entry).await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_routes_by_token_count() {
        assert_eq!(route_for_tokens(1), Route::HybridText);
        assert_eq!(route_for_tokens(2), Route::HybridText);
        assert_eq!(route_for_tokens(3), Route::Fused);
        assert_eq!(route_for_tokens(7), Route::Fused);
        assert_eq!(route_for_tokens(8), Route::Semantic);
        assert_eq!(route_for_tokens(15), Route::Semantic);
    }

    #[test]
    fn parse_accepts_forced_route_and_vector() {
        let expr = parse_auto_expr(&json!([
            "content",
            "Auto",
            "why do pods lose their connection",
            {"route": "semantic", "vector": [0.1, -0.2]}
        ]))
        .unwrap();
        assert_eq!(expr.forced_route, Some(Route::Semantic));
        assert_eq!(expr.vector, Some(vec![0.1, -0.2]));
    }

    #[test]
    fn parse_treats_route_auto_as_unforced() {
        let expr = parse_auto_expr(&json!(["content", "Auto", "q", {"route": "auto"}])).unwrap();
        assert_eq!(expr.forced_route, None);
    }

    #[test]
    fn parse_rejects_bad_route_vector_and_options() {
        assert!(parse_auto_expr(&json!(["c", "Auto", "q", {"route": "lexical"}])).is_err());
        assert!(parse_auto_expr(&json!(["c", "Auto", "q", {"vector": []}])).is_err());
        assert!(parse_auto_expr(&json!(["c", "Auto", "q", {"vector": ["x"]}])).is_err());
        assert!(parse_auto_expr(&json!(["c", "Auto", "q", {"fuzziness": 3}])).is_err());
        assert!(parse_auto_expr(&json!(["c", "Auto", "q", {"fuzziness": "fuzzy"}])).is_err());
        assert!(parse_auto_expr(&json!(["c", "Auto"])).is_err());
    }

    #[test]
    fn parse_accepts_and_forwards_fuzziness() {
        let expr = parse_auto_expr(&json!(["c", "Auto", "q", {"fuzziness": 0}])).unwrap();
        assert_eq!(expr.fuzziness, Some(Fuzziness::Fixed(0)));
        let expr = parse_auto_expr(&json!(["c", "Auto", "q", {"fuzziness": "auto"}])).unwrap();
        assert_eq!(expr.fuzziness, Some(Fuzziness::Auto));
        let expr = parse_auto_expr(&json!(["c", "Auto", "q"])).unwrap();
        assert_eq!(expr.fuzziness, None);
    }

    #[test]
    fn routing_echo_shapes_policy_field() {
        let auto = routing_echo(Route::Semantic, false, 8, false);
        assert_eq!(auto["policy"], "v1");
        assert_eq!(auto["executed"], false);
        assert_eq!(auto["route"], "semantic");
        let forced = routing_echo(Route::Fused, true, 5, true);
        assert_eq!(forced["policy"], "forced");
    }
}
