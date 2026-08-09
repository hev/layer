//! Query router (RFC 0044, phase 1): the `Auto` rank expression.
//!
//! The route is chosen from the shape of the input text alone — vector
//! availability never changes which route is best, only whether it can
//! execute in this request. An inline `Embed` vector source is resolved only
//! after the route needs it; otherwise a vectorless `semantic` or `fused`
//! query gets a deferral response.

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
    pub vector: Option<AutoVector>,
    /// Optional fuzziness override forwarded to the `HybridText` expansion
    /// on the `hybrid_text`/`fused` routes. `None` keeps the
    /// documented hybrid default (`auto`); no effect on `semantic`.
    pub fuzziness: Option<Fuzziness>,
    /// Optional stop-word policy override (RFC 0090), forwarded like
    /// `fuzziness`. `None` keeps the default (English list, on).
    pub stopwords: Option<StopwordsOption>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum AutoVector {
    Numeric(Vec<f64>),
    Embed { field: String, expression: Value },
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
                    vector = Some(parse_auto_vector(value, &field)?);
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

fn parse_auto_vector(value: &Value, default_field: &str) -> Result<AutoVector, AppError> {
    if value
        .as_array()
        .and_then(|expression| expression.first())
        .and_then(Value::as_str)
        == Some("Embed")
    {
        let expression = value.as_array().expect("checked array");
        if expression.len() != 2 && expression.len() != 3 {
            return Err(AppError::Validation(
                "Auto inline `Embed` must be `[\"Embed\", text]` or `[\"Embed\", text, {field?, model?}]`"
                    .to_string(),
            ));
        }
        if expression.get(1).and_then(Value::as_str).is_none() {
            return Err(AppError::Validation(
                "Auto inline `Embed` text must be a string".to_string(),
            ));
        }

        let mut field = default_field.to_string();
        let mut normalized = expression.clone();
        if let Some(options) = expression.get(2) {
            let options = options.as_object().ok_or_else(|| {
                AppError::Validation("Auto inline `Embed` options must be an object".to_string())
            })?;
            for key in options.keys() {
                if key != "field" && key != "model" {
                    return Err(AppError::Validation(format!(
                        "unknown Auto inline `Embed` option `{key}`"
                    )));
                }
            }
            if let Some(value) = options.get("field") {
                field = value
                    .as_str()
                    .filter(|field| !field.is_empty())
                    .ok_or_else(|| {
                        AppError::Validation(
                            "Auto inline `Embed` field must be a non-empty string".to_string(),
                        )
                    })?
                    .to_string();
            }
            if let Some(value) = options.get("model") {
                if value.as_str().is_none() {
                    return Err(AppError::Validation(
                        "Auto inline `Embed` model must be a string".to_string(),
                    ));
                }
            }
            let mut embed_options = options.clone();
            embed_options.remove("field");
            if embed_options.is_empty() {
                normalized.truncate(2);
            } else {
                normalized[2] = Value::Object(embed_options);
            }
        }
        return Ok(AutoVector::Embed {
            field,
            expression: Value::Array(normalized),
        });
    }

    let parsed: Vec<f64> = serde_json::from_value(value.clone()).map_err(|error| {
        AppError::Validation(format!(
            "Auto vector must be a number array or inline `Embed`: {error}"
        ))
    })?;
    if parsed.is_empty() {
        return Err(AppError::Validation(
            "Auto vector must not be empty".to_string(),
        ));
    }
    Ok(AutoVector::Numeric(parsed))
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

    let mut embedding_performance = json!({});
    let resolved_vector = if route.needs_vector() {
        match expr
            .vector
            .as_ref()
            .expect("vector-needing route checked source")
        {
            AutoVector::Numeric(vector) => Some((vector.clone(), "vector".to_string())),
            AutoVector::Embed { field, expression } => {
                let resolved = crate::routes::embed_wire::resolve_auto_embed(
                    &state,
                    &namespace,
                    field,
                    expression.clone(),
                    state.namespace_uses_search_store(&namespace),
                )
                .await?;
                embedding_performance = resolved.performance;
                Some((resolved.vector, resolved.target))
            }
        }
    } else {
        None
    };

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
            let (vector, target) = resolved_vector
                .clone()
                .expect("fused route checked vector source");
            let ann_leg = LegSpec {
                label: "semantic".to_string(),
                rank_by: json!([target, "ANN", vector]),
                filter: request.filters.clone(),
            };
            let out = run_hybrid(&state, &namespace, &expr, &request, Some(ann_leg)).await?;
            (out.0, Some(out.1), out.2, out.3)
        }
        Route::Semantic => {
            let (vector, target) = resolved_vector.expect("semantic route checked vector source");
            run_semantic(&state, &namespace, &request, vector, target).await?
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
    if embedding_performance
        .as_object()
        .is_some_and(|performance| !performance.is_empty())
    {
        body.insert("performance".to_string(), embedding_performance);
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
    target: String,
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
    let canonical_vector = target == "vector";
    let query = QueryRequest {
        vector: canonical_vector.then_some(vector.clone()),
        nearest_to_id: None,
        top_k: fetch_top_k,
        filters: request.filters.clone(),
        as_of: request.as_of,
        between: request.between,
        include_attributes,
        cursor: None,
        rank_by: (!canonical_vector).then(|| json!([target, "ANN", vector])),
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
        assert_eq!(expr.vector, Some(AutoVector::Numeric(vec![0.1, -0.2])));
    }

    #[test]
    fn parse_accepts_inline_embed_with_independent_field() {
        let expr = parse_auto_expr(&json!([
            "title",
            "Auto",
            "why do plants turn sunlight into useful chemical energy",
            {"vector": ["Embed", "why do plants turn sunlight into useful chemical energy", {"field": "text"}]}
        ]))
        .unwrap();
        assert_eq!(
            expr.vector,
            Some(AutoVector::Embed {
                field: "text".to_string(),
                expression: json!([
                    "Embed",
                    "why do plants turn sunlight into useful chemical energy"
                ]),
            })
        );
        assert_eq!(expr.forced_route, None);
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
