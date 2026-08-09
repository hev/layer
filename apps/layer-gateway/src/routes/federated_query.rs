use std::sync::Arc;

use axum::extract::{Extension, State};
use axum::http::{HeaderMap, HeaderValue};
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures::stream::{self, StreamExt};
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::auth::AuthenticatedApiKey;
use crate::error::AppError;
use crate::models::QueryRequest;
use crate::routes::hybrid_text::{
    parse_hybrid_request, parse_hybrid_text_expr, run_hybrid_text, tokenize_query_input, LegSpec,
};
use crate::routes::query::{
    insert_optional_u64_header, query_results_to_rows, run_query_leg, QueryRunConfig,
    LAYER_STABLE_AS_OF_HEADER,
};
use crate::routes::query_router::{
    parse_auto_expr, route_for_tokens, routing_echo, run_hybrid, run_semantic, AutoVector, Route,
};
use crate::AppState;

const LAYER_PARTIAL_HEADER: &str = "x-layer-partial";

#[derive(Debug, Deserialize)]
struct FederatedEnvelope {
    #[serde(default)]
    namespaces: Option<Vec<String>>,
    #[serde(default)]
    strict: bool,
    #[serde(default)]
    fusion: Option<FusionOptions>,
}

#[derive(Debug, Deserialize)]
struct FusionOptions {
    #[serde(default)]
    per_namespace_limit: Option<u32>,
    #[serde(default)]
    rank_constant: Option<u64>,
}

struct NamespaceOutput {
    namespace: String,
    rows: Vec<Value>,
    stable_as_of: Option<u64>,
    routing: Option<Value>,
    hybrid: Option<Value>,
    merge_method: MergeMethod,
    vector_dim: Option<usize>,
}

struct NamespaceError {
    namespace: String,
    message: String,
    code: Option<String>,
    detail: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MergeMethod {
    Distance,
    RankInterleave,
}

impl MergeMethod {
    fn as_str(self) -> &'static str {
        match self {
            Self::Distance => "distance",
            Self::RankInterleave => "rank-interleave",
        }
    }
}

struct MergeDecision {
    method: MergeMethod,
    downgraded_reason: Option<String>,
}

/// POST /v2/query
///
/// Federates one logical query across an explicit namespace set and fuses
/// results into one list. This is a gateway composition; Turbopuffer has no
/// cross-namespace query primitive.
pub async fn query(
    State(state): State<Arc<AppState>>,
    auth: Option<Extension<AuthenticatedApiKey>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, AppError> {
    let (response, stable_as_of, partial) =
        execute_federated_query(state, auth, headers.clone(), body).await?;
    let mut response_headers = HeaderMap::new();
    insert_optional_u64_header(
        &mut response_headers,
        LAYER_STABLE_AS_OF_HEADER,
        stable_as_of,
    );
    if partial {
        response_headers.insert(LAYER_PARTIAL_HEADER, HeaderValue::from_static("true"));
    }
    if let Some(traceparent) = headers.get(crate::history::TRACEPARENT_HEADER).cloned() {
        response_headers.insert(crate::history::TRACEPARENT_HEADER, traceparent);
    }

    Ok((response_headers, Json(response)).into_response())
}

pub(crate) async fn execute_federated_query(
    state: Arc<AppState>,
    auth: Option<Extension<AuthenticatedApiKey>>,
    _headers: HeaderMap,
    body: Value,
) -> Result<(Value, Option<u64>, bool), AppError> {
    let object = body.as_object().cloned().ok_or_else(|| {
        AppError::Validation("federated query body must be an object".to_string())
    })?;
    state.telemetry.touch_federated_query();
    let envelope: FederatedEnvelope = serde_json::from_value(Value::Object(object.clone()))
        .map_err(|e| AppError::Validation(format!("invalid federated query request: {e}")))?;
    if object.contains_key("cursor") {
        return Err(AppError::Validation(
            "cursor is not supported on federated query; pagination is single-query only"
                .to_string(),
        ));
    }

    let namespaces =
        resolve_namespaces(&state, envelope.namespaces, auth.as_ref().map(|a| &a.0)).await?;
    let top_k = top_k_from_body(&object)?;
    let per_namespace_limit = envelope
        .fusion
        .as_ref()
        .and_then(|fusion| fusion.per_namespace_limit)
        .unwrap_or_else(|| default_per_namespace_limit(top_k));
    if per_namespace_limit == 0 {
        return Err(AppError::Validation(
            "fusion.per_namespace_limit must be an integer > 0".to_string(),
        ));
    }
    let _rank_constant = envelope
        .fusion
        .as_ref()
        .and_then(|fusion| fusion.rank_constant)
        .unwrap_or(60);

    let mut per_namespace_body = object.clone();
    per_namespace_body.remove("namespaces");
    per_namespace_body.remove("strict");
    per_namespace_body.remove("fusion");
    per_namespace_body.insert("top_k".to_string(), Value::from(per_namespace_limit));

    let outputs = stream::iter(namespaces.into_iter().map(|namespace| {
        let state = Arc::clone(&state);
        let body = per_namespace_body.clone();
        async move {
            let result =
                match validate_federated_filter_schema(&state, &namespace, body.get("filters"))
                    .await
                {
                    Ok(()) => run_namespace_query(&state, &namespace, body).await,
                    Err(err) => Err(err),
                };
            (namespace, result)
        }
    }))
    .buffer_unordered(state.federated_query_namespace_threads)
    .collect::<Vec<_>>()
    .await;

    let mut successes = Vec::new();
    let mut errors = Vec::new();
    for (namespace, result) in outputs {
        match result {
            Ok(output) => successes.push(output),
            Err(AppError::Validation(message)) if is_filter_schema_mismatch(&message) => {
                if envelope.strict {
                    return Err(AppError::Validation(message));
                }
                errors.push(filter_schema_mismatch_error(namespace, message));
            }
            Err(err @ AppError::Validation(_)) | Err(err @ AppError::Forbidden(_)) => {
                return Err(err)
            }
            Err(err) if envelope.strict => return Err(err),
            Err(err) => errors.push(NamespaceError {
                namespace,
                message: err.to_string(),
                code: None,
                detail: None,
            }),
        }
    }

    if successes.is_empty() && !errors.is_empty() {
        return Err(AppError::Upstream(
            "all federated query namespaces failed".to_string(),
        ));
    }

    let merge_decision = choose_merge_method(&state, &successes, envelope.strict).await?;
    let merge_method = merge_decision.method;
    let rows = merge_rows(&successes, merge_method, top_k as usize);

    let stable_as_of = successes
        .iter()
        .filter_map(|output| output.stable_as_of)
        .min();
    let routing = first_echo(&successes, |output| output.routing.as_ref());
    let hybrid = first_echo(&successes, |output| output.hybrid.as_ref());

    let namespaces: Vec<Value> = successes
        .iter()
        .map(|output| {
            json!({
                "namespace": output.namespace,
                "stable_as_of": output.stable_as_of,
                "matched": output.rows.len(),
            })
        })
        .collect();

    let mut response = Map::new();
    response.insert("rows".to_string(), Value::Array(rows));
    let mut merge = json!({"method": merge_method.as_str(), "route": "fused"});
    if let Some(reason) = merge_decision.downgraded_reason {
        merge["downgraded_reason"] = Value::String(reason);
    }
    response.insert("merge".to_string(), merge);
    if let Some(routing) = routing {
        response.insert("routing".to_string(), routing);
    }
    if let Some(hybrid) = hybrid {
        response.insert("hybrid".to_string(), hybrid);
    }
    response.insert("namespaces".to_string(), Value::Array(namespaces));
    if !errors.is_empty() {
        response.insert(
            "errors".to_string(),
            Value::Array(
                errors
                    .iter()
                    .map(|err| {
                        let mut object = Map::new();
                        object.insert(
                            "namespace".to_string(),
                            Value::String(err.namespace.clone()),
                        );
                        if let Some(code) = &err.code {
                            object.insert("code".to_string(), Value::String(code.clone()));
                        }
                        object.insert("error".to_string(), Value::String(err.message.clone()));
                        if let Some(detail) = &err.detail {
                            object.insert("detail".to_string(), Value::String(detail.clone()));
                        }
                        Value::Object(object)
                    })
                    .collect(),
            ),
        );
    }

    Ok((Value::Object(response), stable_as_of, !errors.is_empty()))
}

const FILTER_SCHEMA_MISMATCH_CODE: &str = "filter_schema_mismatch";
const FILTER_SCHEMA_MISMATCH_ERROR: &str = "filter schema mismatch";

fn is_filter_schema_mismatch(message: &str) -> bool {
    message.contains(FILTER_SCHEMA_MISMATCH_CODE)
}

fn filter_schema_mismatch_error(namespace: String, message: String) -> NamespaceError {
    let detail = message
        .strip_prefix(FILTER_SCHEMA_MISMATCH_CODE)
        .and_then(|rest| rest.strip_prefix(": "))
        .unwrap_or(&message)
        .to_string();
    NamespaceError {
        namespace,
        message: FILTER_SCHEMA_MISMATCH_ERROR.to_string(),
        code: Some(FILTER_SCHEMA_MISMATCH_CODE.to_string()),
        detail: Some(detail),
    }
}

async fn validate_federated_filter_schema(
    state: &AppState,
    namespace: &str,
    filters: Option<&Value>,
) -> Result<(), AppError> {
    let Some(filters) = filters else {
        return Ok(());
    };
    let schema = federated_filter_schema(state, namespace).await?;
    validate_filter_against_schema(filters, &schema)
}

async fn federated_filter_schema(
    state: &AppState,
    namespace: &str,
) -> Result<Map<String, Value>, AppError> {
    let upstream = state
        .turbopuffer()
        .passthrough(
            "GET",
            &format!("/v1/namespaces/{namespace}/schema"),
            None,
            None,
        )
        .await
        .map_err(|e| AppError::Upstream(format!("Turbopuffer schema fetch failed: {e}")))?;
    if upstream.status >= 400 {
        return Err(AppError::Upstream(format!(
            "Turbopuffer schema fetch returned {}",
            upstream.status
        )));
    }
    let body: Value = serde_json::from_slice(&upstream.body)
        .map_err(|e| AppError::Upstream(format!("Turbopuffer schema parse failed: {e}")))?;
    Ok(extract_filter_schema(&body).cloned().unwrap_or_default())
}

fn extract_filter_schema(body: &Value) -> Option<&Map<String, Value>> {
    body.pointer("/schema/attributes")
        .and_then(Value::as_object)
        .or_else(|| body.get("schema").and_then(Value::as_object))
        .or_else(|| body.get("attributes").and_then(Value::as_object))
}

fn validate_filter_against_schema(
    filter: &Value,
    schema: &Map<String, Value>,
) -> Result<(), AppError> {
    let Some(arr) = filter.as_array() else {
        return Ok(());
    };
    let Some(head) = arr.first().and_then(Value::as_str) else {
        return Ok(());
    };
    match head {
        "And" | "and" | "Or" | "or" => {
            if let Some(filters) = arr.get(1).and_then(Value::as_array) {
                for filter in filters {
                    validate_filter_against_schema(filter, schema)?;
                }
            }
            Ok(())
        }
        "Not" | "not" => {
            if let Some(filter) = arr.get(1) {
                validate_filter_against_schema(filter, schema)?;
            }
            Ok(())
        }
        "id" | "$dist" | "$score" => Ok(()),
        field => {
            let detail = format!(
                "filter attribute {field} is absent, not filterable, or has an incompatible type"
            );
            let Some(spec) = schema.get(field) else {
                return Err(AppError::Validation(format!(
                    "{FILTER_SCHEMA_MISMATCH_CODE}: {detail}"
                )));
            };
            if spec
                .get("filterable")
                .or_else(|| spec.get("filterable_attribute"))
                .and_then(Value::as_bool)
                != Some(true)
            {
                return Err(AppError::Validation(format!(
                    "{FILTER_SCHEMA_MISMATCH_CODE}: {detail}"
                )));
            }
            let Some(expected) = arr.get(2) else {
                return Ok(());
            };
            if !filter_value_matches_schema_type(expected, schema_type(spec)) {
                return Err(AppError::Validation(format!(
                    "{FILTER_SCHEMA_MISMATCH_CODE}: {detail}"
                )));
            }
            Ok(())
        }
    }
}

fn schema_type(spec: &Value) -> Option<&str> {
    spec.get("type")
        .or_else(|| spec.get("data_type"))
        .and_then(Value::as_str)
}

fn filter_value_matches_schema_type(value: &Value, schema_type: Option<&str>) -> bool {
    let Some(schema_type) = schema_type else {
        return true;
    };
    let schema_type = schema_type.trim().to_ascii_lowercase();
    if value.is_null() {
        return true;
    }
    if schema_type.starts_with("[]") || schema_type.starts_with("array") {
        let item_type = schema_type
            .strip_prefix("[]")
            .or_else(|| schema_type.strip_prefix("array<"))
            .unwrap_or(&schema_type)
            .trim_end_matches('>');
        return if let Some(items) = value.as_array() {
            items
                .iter()
                .all(|item| scalar_matches_schema_type(item, item_type))
        } else {
            scalar_matches_schema_type(value, item_type)
        };
    }
    scalar_matches_schema_type(value, &schema_type)
}

fn scalar_matches_schema_type(value: &Value, schema_type: &str) -> bool {
    match schema_type {
        "string" | "str" | "datetime" | "date" => value.is_string(),
        "bool" | "boolean" => value.is_boolean(),
        "int" | "integer" | "uint" | "u64" | "i64" => value.as_i64().is_some(),
        "float" | "f32" | "f64" | "number" | "numeric" => value.is_number(),
        _ => true,
    }
}

async fn resolve_namespaces(
    state: &AppState,
    namespaces: Option<Vec<String>>,
    auth: Option<&AuthenticatedApiKey>,
) -> Result<Vec<String>, AppError> {
    let all_namespaces = namespaces
        .as_ref()
        .map(|namespaces| namespaces.len() == 1 && namespaces[0] == "*")
        .unwrap_or(true);
    if all_namespaces {
        let listed = list_all_upstream_namespaces(state).await?;
        let expanded: Vec<String> = listed
            .into_iter()
            .filter(|namespace| namespace_allowed(auth, namespace))
            .collect();
        return validate_namespace_limit(expanded, state.federated_query_max_namespaces);
    }

    let Some(namespaces) = namespaces else {
        unreachable!("all_namespaces handles None");
    };
    if namespaces.is_empty() {
        return Err(AppError::Validation(
            "`namespaces` must contain at least one namespace".to_string(),
        ));
    }
    if namespaces.iter().any(|namespace| namespace == "*") {
        return Err(AppError::Validation(
            "`namespaces: [\"*\"]` must be used alone".to_string(),
        ));
    }
    for namespace in &namespaces {
        if !namespace_allowed(auth, namespace) {
            return Err(AppError::Forbidden(format!(
                "namespace `{namespace}` is not in the authenticated key grant"
            )));
        }
    }
    validate_namespace_limit(namespaces, state.federated_query_max_namespaces)
}

fn validate_namespace_limit(
    namespaces: Vec<String>,
    max_namespaces: usize,
) -> Result<Vec<String>, AppError> {
    if namespaces.len() > max_namespaces {
        let dropped: Vec<&str> = namespaces[max_namespaces..]
            .iter()
            .map(String::as_str)
            .collect();
        return Err(AppError::Validation(format!(
            "federated query supports at most {max_namespaces} namespaces; excess: {dropped:?}"
        )));
    }
    let mut deduped = Vec::with_capacity(namespaces.len());
    for namespace in namespaces {
        if namespace.is_empty() {
            return Err(AppError::Validation(
                "`namespaces` entries must not be empty".to_string(),
            ));
        }
        if !deduped.iter().any(|seen| seen == &namespace) {
            deduped.push(namespace);
        }
    }
    Ok(deduped)
}

fn namespace_allowed(_auth: Option<&AuthenticatedApiKey>, _namespace: &str) -> bool {
    true
}

async fn list_all_upstream_namespaces(state: &AppState) -> Result<Vec<String>, AppError> {
    let mut cursor = None;
    let mut names = Vec::new();
    loop {
        let mut query = "page_size=1000".to_string();
        if let Some(cursor) = cursor.as_deref() {
            query.push_str("&cursor=");
            query.push_str(&urlencode(cursor));
        }
        let upstream = state
            .turbopuffer()
            .passthrough("GET", "/v1/namespaces", Some(&query), None)
            .await
            .map_err(|e| AppError::Upstream(format!("Turbopuffer namespace list: {e}")))?;
        if upstream.status >= 400 {
            return Err(AppError::Upstream(format!(
                "Turbopuffer namespace list returned {}",
                upstream.status
            )));
        }
        let body: Value = serde_json::from_slice(&upstream.body)
            .map_err(|e| AppError::Upstream(format!("Turbopuffer namespace list parse: {e}")))?;
        if let Some(items) = body.get("namespaces").and_then(Value::as_array) {
            names.extend(items.iter().filter_map(|item| {
                item.as_str()
                    .map(str::to_string)
                    .or_else(|| item.get("id").and_then(Value::as_str).map(str::to_string))
            }));
        }
        cursor = body
            .get("next_cursor")
            .and_then(Value::as_str)
            .filter(|cursor| !cursor.is_empty())
            .map(str::to_string);
        if cursor.is_none() {
            break;
        }
    }
    names.sort();
    names.dedup();
    Ok(names)
}

fn urlencode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(*byte as char)
            }
            _ => out.push_str(&format!("%{:02X}", byte)),
        }
    }
    out
}

fn top_k_from_body(body: &Map<String, Value>) -> Result<u32, AppError> {
    match body.get("top_k") {
        None => Ok(10),
        Some(value) => value
            .as_u64()
            .filter(|n| *n > 0 && *n <= u32::MAX as u64)
            .map(|n| n as u32)
            .ok_or_else(|| AppError::Validation("top_k must be a positive integer".to_string())),
    }
}

fn default_per_namespace_limit(top_k: u32) -> u32 {
    (2 * top_k).clamp(10, 100)
}

async fn run_namespace_query(
    state: &AppState,
    namespace: &str,
    body: Map<String, Value>,
) -> Result<NamespaceOutput, AppError> {
    let rank_operator = body
        .get("rank_by")
        .and_then(|rank_by| rank_by.get(1))
        .and_then(Value::as_str);
    match rank_operator {
        Some("HybridText") => run_namespace_hybrid_text(state, namespace, body).await,
        Some("Auto") => run_namespace_auto(state, namespace, body).await,
        _ => run_namespace_vector(state, namespace, Value::Object(body)).await,
    }
}

async fn run_namespace_hybrid_text(
    state: &AppState,
    namespace: &str,
    body: Map<String, Value>,
) -> Result<NamespaceOutput, AppError> {
    let expr = parse_hybrid_text_expr(body.get("rank_by").unwrap_or(&Value::Null))?;
    let request = parse_hybrid_request(&body)?;
    let out = run_hybrid_text(state, namespace, &expr, &request, None).await?;
    Ok(NamespaceOutput {
        namespace: namespace.to_string(),
        rows: out.rows,
        stable_as_of: out.watermark,
        routing: None,
        hybrid: Some(out.echo),
        merge_method: MergeMethod::RankInterleave,
        vector_dim: None,
    })
}

async fn run_namespace_auto(
    state: &AppState,
    namespace: &str,
    body: Map<String, Value>,
) -> Result<NamespaceOutput, AppError> {
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
    if route.needs_vector() && expr.vector.is_none() {
        if forced {
            return Err(AppError::Validation(format!(
                "forced route \"{}\" requires a vector",
                route.as_str()
            )));
        }
        return Ok(NamespaceOutput {
            namespace: namespace.to_string(),
            rows: Vec::new(),
            stable_as_of: None,
            routing: Some(routing_echo(route, false, token_count, false)),
            hybrid: None,
            merge_method: MergeMethod::RankInterleave,
            vector_dim: None,
        });
    }

    let routing = routing_echo(route, forced, token_count, true);
    let resolved_vector = if route.needs_vector() {
        match expr
            .vector
            .as_ref()
            .expect("vector-needing route checked source")
        {
            AutoVector::Numeric(vector) => Some((vector.clone(), "vector".to_string())),
            AutoVector::Embed { field, expression } => {
                let resolved = crate::routes::embed_wire::resolve_auto_embed(
                    state,
                    namespace,
                    field,
                    expression.clone(),
                    state.namespace_uses_search_store(namespace),
                )
                .await?;
                Some((resolved.vector, resolved.target))
            }
        }
    } else {
        None
    };
    match route {
        Route::HybridText => {
            let (rows, hybrid, watermark, _) =
                run_hybrid(state, namespace, &expr, &request, None).await?;
            Ok(NamespaceOutput {
                namespace: namespace.to_string(),
                rows,
                stable_as_of: watermark,
                routing: Some(routing),
                hybrid: Some(hybrid),
                merge_method: MergeMethod::RankInterleave,
                vector_dim: None,
            })
        }
        Route::Fused => {
            let (vector, target) = resolved_vector
                .clone()
                .expect("fused route checked vector source");
            let ann_leg = LegSpec {
                label: "semantic".to_string(),
                rank_by: json!([target, "ANN", vector]),
                filter: request.filters.clone(),
            };
            let (rows, hybrid, watermark, _) =
                run_hybrid(state, namespace, &expr, &request, Some(ann_leg)).await?;
            Ok(NamespaceOutput {
                namespace: namespace.to_string(),
                rows,
                stable_as_of: watermark,
                routing: Some(routing),
                hybrid: Some(hybrid),
                merge_method: MergeMethod::RankInterleave,
                vector_dim: None,
            })
        }
        Route::Semantic => {
            let (vector, target) = resolved_vector.expect("semantic route checked vector source");
            let vector_dim = vector.len();
            let (rows, _, watermark, _) =
                run_semantic(state, namespace, &request, vector, target).await?;
            Ok(NamespaceOutput {
                namespace: namespace.to_string(),
                rows,
                stable_as_of: watermark,
                routing: Some(routing),
                hybrid: None,
                merge_method: MergeMethod::Distance,
                vector_dim: Some(vector_dim),
            })
        }
    }
}

async fn run_namespace_vector(
    state: &AppState,
    namespace: &str,
    body: Value,
) -> Result<NamespaceOutput, AppError> {
    let request: QueryRequest = serde_json::from_value(body)
        .map_err(|e| AppError::Validation(format!("invalid query request: {e}")))?;
    if request.cursor.is_some() {
        return Err(AppError::Validation(
            "cursor is not supported on federated query; pagination is single-query only"
                .to_string(),
        ));
    }
    let (watermark, inject_filter) = state.query_consistency(namespace);
    let temporal_filter = crate::routes::query::temporal_filter(request.as_of, request.between)?;
    let shard_count = crate::shards::active_shard_count(state, namespace).await;
    let output = run_query_leg(
        state,
        namespace,
        request,
        QueryRunConfig {
            watermark,
            inject_filter,
            temporal_filter,
            shard_count,
            allow_cursor: false,
        },
    )
    .await?;
    Ok(NamespaceOutput {
        namespace: namespace.to_string(),
        rows: query_results_to_rows(&output.results),
        stable_as_of: watermark,
        routing: None,
        hybrid: None,
        merge_method: MergeMethod::Distance,
        vector_dim: output.request.vector.as_ref().map(Vec::len),
    })
}

fn merge_rows(outputs: &[NamespaceOutput], method: MergeMethod, top_k: usize) -> Vec<Value> {
    let mut rows = Vec::new();
    for output in outputs {
        for (idx, row) in output.rows.iter().enumerate() {
            let mut row = row.as_object().cloned().unwrap_or_default();
            row.insert(
                "$namespace".to_string(),
                Value::String(output.namespace.clone()),
            );
            row.insert("$rank".to_string(), Value::from((idx + 1) as u64));
            rows.push(Value::Object(row));
        }
    }

    match method {
        MergeMethod::Distance => rows.sort_by(compare_distance_rows),
        MergeMethod::RankInterleave => rows.sort_by(compare_rank_interleave_rows),
    }
    rows.truncate(top_k);
    rows
}

async fn choose_merge_method(
    state: &AppState,
    outputs: &[NamespaceOutput],
    strict: bool,
) -> Result<MergeDecision, AppError> {
    if !outputs
        .iter()
        .all(|output| output.merge_method == MergeMethod::Distance)
    {
        return Ok(MergeDecision {
            method: MergeMethod::RankInterleave,
            downgraded_reason: None,
        });
    }

    let mut profiles = Vec::with_capacity(outputs.len());
    let mut missing = Vec::new();
    for output in outputs {
        match state.embedding_profile_for(&output.namespace) {
            Some(profile) => {
                let evidence = schema_embedding_evidence(state, &output.namespace).await?;
                if evidence
                    .output_dim
                    .is_some_and(|dim| dim != profile.output_dim)
                {
                    return Err(AppError::Validation(format!(
                        "federated vector query profile contradicts schema for namespace `{}`: profile outputDim {}, schema dim {}",
                        output.namespace,
                        profile.output_dim,
                        evidence.output_dim.unwrap()
                    )));
                }
                if evidence
                    .distance_metric
                    .as_deref()
                    .is_some_and(|metric| metric != profile.distance_metric)
                {
                    return Err(AppError::Validation(format!(
                        "federated vector query profile contradicts schema for namespace `{}`: profile metric {}, schema metric {}",
                        output.namespace,
                        profile.distance_metric,
                        evidence.distance_metric.unwrap()
                    )));
                }
                if output.vector_dim != Some(profile.output_dim as usize) {
                    return Err(AppError::Validation(format!(
                        "federated vector query dimension contradicts spec.embedding for namespace `{}`: profile outputDim {}, query vector dim {}",
                        output.namespace,
                        profile.output_dim,
                        output
                            .vector_dim
                            .map(|dim| dim.to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    )));
                }
                profiles.push((output.namespace.as_str(), profile));
            }
            None => missing.push(output.namespace.as_str()),
        }
    }
    if !missing.is_empty() {
        if strict {
            return Err(AppError::Validation(format!(
                "federated vector query requires spec.embedding on every namespace; missing: {missing:?}"
            )));
        }
        return Ok(MergeDecision {
            method: MergeMethod::RankInterleave,
            downgraded_reason: Some("missing_embedding_profile".to_string()),
        });
    }

    let Some((_, first)) = profiles.first() else {
        return Ok(MergeDecision {
            method: MergeMethod::Distance,
            downgraded_reason: None,
        });
    };
    let mismatched: Vec<&str> = profiles
        .iter()
        .filter_map(|(namespace, profile)| (profile != first).then_some(*namespace))
        .collect();
    if !mismatched.is_empty() {
        if strict {
            return Err(AppError::Validation(format!(
                "federated vector query embedding profiles do not match; mismatched: {mismatched:?}"
            )));
        }
        return Ok(MergeDecision {
            method: MergeMethod::RankInterleave,
            downgraded_reason: Some("embedding_profile_mismatch".to_string()),
        });
    }

    Ok(MergeDecision {
        method: MergeMethod::Distance,
        downgraded_reason: None,
    })
}

#[derive(Default)]
struct SchemaEmbeddingEvidence {
    output_dim: Option<u32>,
    distance_metric: Option<String>,
}

async fn schema_embedding_evidence(
    state: &AppState,
    namespace: &str,
) -> Result<SchemaEmbeddingEvidence, AppError> {
    let upstream = state
        .turbopuffer()
        .passthrough(
            "GET",
            &format!("/v1/namespaces/{namespace}/schema"),
            None,
            None,
        )
        .await
        .map_err(|e| AppError::Upstream(format!("Turbopuffer schema fetch failed: {e}")))?;
    if upstream.status >= 400 {
        return Err(AppError::Upstream(format!(
            "Turbopuffer schema fetch returned {}",
            upstream.status
        )));
    }
    let body: Value = serde_json::from_slice(&upstream.body)
        .map_err(|e| AppError::Upstream(format!("Turbopuffer schema parse failed: {e}")))?;
    Ok(extract_schema_embedding_evidence(&body))
}

fn extract_schema_embedding_evidence(body: &Value) -> SchemaEmbeddingEvidence {
    let schema = body.get("schema").unwrap_or(body);
    let vector = schema
        .get("vector")
        .or_else(|| schema.pointer("/attributes/vector"))
        .or_else(|| schema.pointer("/columns/vector"))
        .unwrap_or(schema);
    SchemaEmbeddingEvidence {
        output_dim: first_u32_at(
            vector,
            &[
                "/dimensions",
                "/dimension",
                "/dim",
                "/num_dimensions",
                "/ann/dimensions",
                "/ann/dim",
            ],
        ),
        distance_metric: first_string_at(
            vector,
            &[
                "/distance_metric",
                "/distanceMetric",
                "/metric",
                "/ann/distance_metric",
                "/ann/distanceMetric",
                "/ann/metric",
            ],
        ),
    }
}

fn first_u32_at(value: &Value, pointers: &[&str]) -> Option<u32> {
    pointers.iter().find_map(|pointer| {
        value
            .pointer(pointer)
            .and_then(Value::as_u64)
            .and_then(|n| u32::try_from(n).ok())
            .filter(|n| *n > 0)
    })
}

fn first_string_at(value: &Value, pointers: &[&str]) -> Option<String> {
    pointers.iter().find_map(|pointer| {
        value
            .pointer(pointer)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    })
}

fn compare_distance_rows(a: &Value, b: &Value) -> std::cmp::Ordering {
    let a_dist = a
        .get("$dist")
        .and_then(Value::as_f64)
        .unwrap_or(f64::INFINITY);
    let b_dist = b
        .get("$dist")
        .and_then(Value::as_f64)
        .unwrap_or(f64::INFINITY);
    a_dist
        .partial_cmp(&b_dist)
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| compare_origin(a, b))
}

fn compare_rank_interleave_rows(a: &Value, b: &Value) -> std::cmp::Ordering {
    let a_rank = a.get("$rank").and_then(Value::as_u64).unwrap_or(u64::MAX);
    let b_rank = b.get("$rank").and_then(Value::as_u64).unwrap_or(u64::MAX);
    a_rank.cmp(&b_rank).then_with(|| compare_origin(a, b))
}

fn compare_origin(a: &Value, b: &Value) -> std::cmp::Ordering {
    let a_ns = a.get("$namespace").and_then(Value::as_str).unwrap_or("");
    let b_ns = b.get("$namespace").and_then(Value::as_str).unwrap_or("");
    let a_id = a.get("id").and_then(Value::as_str).unwrap_or("");
    let b_id = b.get("id").and_then(Value::as_str).unwrap_or("");
    a_ns.cmp(b_ns).then_with(|| a_id.cmp(b_id))
}

fn first_echo<F>(outputs: &[NamespaceOutput], f: F) -> Option<Value>
where
    F: Fn(&NamespaceOutput) -> Option<&Value>,
{
    outputs.iter().find_map(|output| f(output).cloned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rank_interleave_orders_by_namespace_rank_then_origin() {
        let outputs = vec![
            NamespaceOutput {
                namespace: "b".to_string(),
                rows: vec![json!({"id": "b1"}), json!({"id": "b2"})],
                stable_as_of: Some(20),
                routing: None,
                hybrid: None,
                merge_method: MergeMethod::RankInterleave,
                vector_dim: None,
            },
            NamespaceOutput {
                namespace: "a".to_string(),
                rows: vec![json!({"id": "a1"}), json!({"id": "a2"})],
                stable_as_of: Some(10),
                routing: None,
                hybrid: None,
                merge_method: MergeMethod::RankInterleave,
                vector_dim: None,
            },
        ];

        let rows = merge_rows(&outputs, MergeMethod::RankInterleave, 10);
        let ids: Vec<_> = rows.iter().map(|row| row["id"].as_str().unwrap()).collect();
        assert_eq!(ids, vec!["a1", "b1", "a2", "b2"]);
        assert_eq!(rows[0]["$namespace"], "a");
        assert_eq!(rows[0]["$rank"], 1);
    }

    #[test]
    fn distance_merge_orders_by_dist_then_origin() {
        let outputs = vec![
            NamespaceOutput {
                namespace: "b".to_string(),
                rows: vec![json!({"id": "b1", "$dist": 0.2})],
                stable_as_of: None,
                routing: None,
                hybrid: None,
                merge_method: MergeMethod::Distance,
                vector_dim: Some(2),
            },
            NamespaceOutput {
                namespace: "a".to_string(),
                rows: vec![json!({"id": "a1", "$dist": 0.1})],
                stable_as_of: None,
                routing: None,
                hybrid: None,
                merge_method: MergeMethod::Distance,
                vector_dim: Some(2),
            },
        ];

        let rows = merge_rows(&outputs, MergeMethod::Distance, 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"], "a1");
        assert_eq!(rows[0]["$rank"], 1);
    }

    #[test]
    fn namespace_limit_validation_dedupes_and_rejects_empty() {
        let namespaces =
            validate_namespace_limit(vec!["a".to_string(), "b".to_string(), "a".to_string()], 512)
                .unwrap();
        assert_eq!(namespaces, vec!["a".to_string(), "b".to_string()]);
        assert!(validate_namespace_limit(vec!["".to_string()], 512).is_err());
    }

    #[test]
    fn extracts_schema_embedding_evidence_from_common_shapes() {
        let evidence = extract_schema_embedding_evidence(&json!({
            "schema": {
                "vector": {
                    "ann": {
                        "dimensions": 384,
                        "distance_metric": "cosine_distance"
                    }
                }
            }
        }));
        assert_eq!(evidence.output_dim, Some(384));
        assert_eq!(evidence.distance_metric.as_deref(), Some("cosine_distance"));
    }
}
