//! Collapse (LYR-264): one result per group instead of one per row.
//!
//! A ranked query carrying `collapse: {by, expansion, missing}` reads
//! `top_k × expansion` rows through the ordinary query path, groups them in
//! the gateway, and returns the best `top_k` groups. Neither Turbopuffer
//! (`group_by` only exists beside `aggregate_by`) nor the Postgres adapter
//! can rank by group, so there is nothing to push down: every store takes the
//! same widened query and the same grouping.

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{header, HeaderMap, Uri};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Map, Value};

use crate::error::AppError;
use crate::AppState;

pub const PARENT_ID_ATTR: &str = "_hevlayer_parent_id";
const DOCUMENT: &str = "document";
const MAX_READ: u64 = 10_000;
const MAX_BY: usize = 4;

/// Rows fetched per requested group unless the caller tunes `expansion`.
/// Wider windows can expose more groups, at increased store and response cost.
pub const DEFAULT_EXPANSION: u64 = 10;
const DEFAULT_TOP_K: u64 = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MissingMode {
    Own,
    Together,
    Drop,
}

#[derive(Debug, Clone)]
pub(crate) struct CollapseSpec {
    /// The attributes as written; `"document"` is resolved against the schema.
    by: Vec<String>,
    pub(crate) expansion: u64,
    missing: MissingMode,
    /// Values counted as missing besides null, absent and the empty string.
    missing_values: Vec<Value>,
}

/// Take `collapse` out of the body and validate its shape.
pub(crate) fn extract(body: &mut Value) -> Result<Option<CollapseSpec>, AppError> {
    let Some(map) = body.as_object_mut() else {
        return Ok(None);
    };
    let Some(raw) = map.remove("collapse") else {
        return Ok(None);
    };
    let spec = parse(&raw)?;
    if map.contains_key("queries") {
        return Err(bad("`collapse` is not supported on multi-query bodies"));
    }
    if map.contains_key("cursor") {
        return Err(bad("`collapse` does not support `cursor` yet"));
    }
    for key in ["aggregate_by", "group_by"] {
        if map.contains_key(key) {
            return Err(bad(format!("`collapse` cannot combine with `{key}`")));
        }
    }
    let ranked = map.contains_key("vector")
        || map.contains_key("nearest_to_id")
        || map.get("rank_by").is_some_and(|rank| {
            !matches!(
                rank.as_array()
                    .and_then(|r| r.get(1))
                    .and_then(Value::as_str),
                Some("asc" | "desc")
            )
        });
    if !ranked {
        return Err(bad(
            "`collapse` needs a ranked query (`rank_by`, `vector` or `nearest_to_id`), not a browse",
        ));
    }
    Ok(Some(spec))
}

fn bad(message: impl Into<String>) -> AppError {
    AppError::BadRequest(message.into())
}

pub(crate) fn parse(raw: &Value) -> Result<CollapseSpec, AppError> {
    let obj = raw
        .as_object()
        .ok_or_else(|| bad("`collapse` must be an object"))?;
    for key in obj.keys() {
        if !["by", "expansion", "missing"].contains(&key.as_str()) {
            return Err(bad(format!("unknown `collapse` field `{key}`")));
        }
    }
    let by = match obj.get("by") {
        Some(Value::String(one)) => vec![one.clone()],
        Some(Value::Array(many)) => many
            .iter()
            .map(|v| v.as_str().map(str::to_owned))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| bad("`collapse.by` entries must be strings"))?,
        _ => return Err(bad("`collapse.by` is required: an attribute or a list")),
    };
    if by.is_empty() || by.len() > MAX_BY || by.iter().any(String::is_empty) {
        return Err(bad(format!(
            "`collapse.by` takes 1 to {MAX_BY} non-empty attribute names"
        )));
    }
    if by.iter().any(|a| a == "id" || a == "vector") {
        return Err(bad("`collapse.by` cannot be `id` or `vector`"));
    }
    let expansion = match obj.get("expansion") {
        None => DEFAULT_EXPANSION,
        Some(v) => v
            .as_u64()
            .filter(|n| (1..=MAX_READ).contains(n))
            .ok_or_else(|| {
                bad(format!(
                    "`collapse.expansion` must be an integer from 1 to {MAX_READ}"
                ))
            })?,
    };
    let (missing, missing_values) = match obj.get("missing") {
        None => (MissingMode::Own, Vec::new()),
        Some(Value::String(mode)) => (parse_mode(mode)?, Vec::new()),
        Some(Value::Object(config)) => {
            for key in config.keys() {
                if !["mode", "values"].contains(&key.as_str()) {
                    return Err(bad(format!("unknown `collapse.missing` field `{key}`")));
                }
            }
            let mode = match config.get("mode") {
                None => MissingMode::Own,
                Some(Value::String(mode)) => parse_mode(mode)?,
                Some(_) => return Err(bad("`collapse.missing.mode` must be a string")),
            };
            let values =
                match config.get("values") {
                    None => Vec::new(),
                    Some(Value::Array(values))
                        if values
                            .iter()
                            .all(|v| v.is_string() || v.is_number() || v.is_boolean()) =>
                    {
                        values.clone()
                    }
                    Some(_) => return Err(bad(
                        "`collapse.missing.values` must be a list of strings, numbers or booleans",
                    )),
                };
            (mode, values)
        }
        Some(_) => return Err(bad(
            "`collapse.missing` must be \"own\", \"together\", \"drop\" or {\"mode\", \"values\"}",
        )),
    };
    Ok(CollapseSpec {
        by,
        expansion,
        missing,
        missing_values,
    })
}

fn parse_mode(mode: &str) -> Result<MissingMode, AppError> {
    match mode {
        "own" => Ok(MissingMode::Own),
        "together" => Ok(MissingMode::Together),
        "drop" => Ok(MissingMode::Drop),
        other => Err(bad(format!(
            "`collapse.missing` mode `{other}` is not one of own, together, drop"
        ))),
    }
}

/// `(requested name, attribute)` for each `by` entry, `"document"` resolved.
pub(crate) async fn resolve_attributes(
    state: &AppState,
    namespace: &str,
    spec: &CollapseSpec,
) -> Result<Vec<(String, String)>, AppError> {
    let mut resolved = Vec::new();
    let mut schema: Option<Value> = None;
    for name in &spec.by {
        if name != DOCUMENT {
            resolved.push((name.clone(), name.clone()));
            continue;
        }
        if schema.is_none() {
            let meta = state
                .turbopuffer()
                .head_namespace(namespace)
                .await
                .map_err(|e| {
                    if e.is_not_found() {
                        AppError::NotFound(format!("namespace '{namespace}' not found"))
                    } else {
                        AppError::from_turbopuffer(e, "turbopuffer metadata")
                    }
                })?;
            schema = Some(meta.raw.get("schema").cloned().unwrap_or(Value::Null));
        }
        let schema = schema.as_ref().expect("schema loaded above");
        let attribute = match document_attribute(state, namespace, schema).await? {
            Some(attribute) => attribute,
            None if schema.get(PARENT_ID_ATTR).is_some() => PARENT_ID_ATTR.to_string(),
            None => {
                return Err(bad(format!(
                    "`collapse.by: \"document\"` needs `{PARENT_ID_ATTR}` or a schema attribute with `document: true`; namespace '{namespace}' has neither"
                )))
            }
        };
        resolved.push((DOCUMENT.to_string(), attribute));
    }
    Ok(resolved)
}

pub(crate) async fn run(
    state: Arc<AppState>,
    namespace: String,
    uri: Uri,
    headers: HeaderMap,
    mut body: Value,
    spec: CollapseSpec,
) -> Result<Response, AppError> {
    let by = resolve_attributes(&state, &namespace, &spec).await?;
    let map = body
        .as_object_mut()
        .ok_or_else(|| bad("query body must be an object"))?;
    let top_k = match map.get("top_k").or_else(|| map.get("limit")) {
        None => DEFAULT_TOP_K,
        Some(v) => v
            .as_u64()
            .filter(|n| *n >= 1)
            .ok_or_else(|| bad("`top_k` must be a positive integer with `collapse`"))?,
    };
    let read = top_k.saturating_mul(spec.expansion).min(MAX_READ);
    map.remove("limit");
    map.insert("top_k".into(), json!(read));
    let added = ensure_attributes(map, &by);

    let inner = crate::routes::query::query_ungrouped(
        state,
        namespace,
        uri,
        headers,
        Value::Object(std::mem::take(map)),
    )
    .await?;
    let (mut parts, inner_body) = inner.into_parts();
    let bytes = axum::body::to_bytes(inner_body, usize::MAX)
        .await
        .map_err(|e| AppError::Upstream(format!("read query response body: {e}")))?;
    let mut response: Value = match serde_json::from_slice(&bytes) {
        Ok(Value::Object(map)) if map.get("rows").is_some_and(Value::is_array) => {
            Value::Object(map)
        }
        _ => return Ok(Response::from_parts(parts, Body::from(bytes))),
    };
    let rows = response["rows"]
        .take()
        .as_array()
        .cloned()
        .unwrap_or_default();
    let read_rows = rows.len() as u64;
    let groups = group_rows(&rows, &by, &spec, top_k as usize, &added);

    let mut out = response.as_object().cloned().unwrap_or_default();
    let firsts: Vec<Value> = groups
        .iter()
        .filter_map(|g| g["rows"].get(0).cloned())
        .collect();
    out.insert("rows".into(), Value::Array(firsts));
    out.insert("groups".into(), Value::Array(groups));
    out.insert(
        "collapse".into(),
        json!({ "read": read_rows, "exhausted": read_rows < read }),
    );
    parts.headers.remove(header::CONTENT_LENGTH);
    let (status, headers) = (parts.status, parts.headers);
    Ok((status, headers, Json(Value::Object(out))).into_response())
}

/// Make the rows carry the grouping attributes. Returns the ones the caller
/// did not ask for, which are stripped again before the response.
fn ensure_attributes(map: &mut Map<String, Value>, by: &[(String, String)]) -> Vec<String> {
    let mut wanted: Vec<String> = Vec::new();
    for (_, attribute) in by {
        if !wanted.contains(attribute) {
            wanted.push(attribute.clone());
        }
    }
    if let Some(Value::Array(excluded)) = map.get_mut("exclude_attributes") {
        let mut added = Vec::new();
        excluded.retain(|v| match v.as_str() {
            Some(name) if wanted.iter().any(|w| w == name) => {
                added.push(name.to_string());
                false
            }
            _ => true,
        });
        return added;
    }
    match map.get_mut("include_attributes") {
        Some(Value::Bool(true)) => Vec::new(),
        Some(Value::Array(list)) => {
            let mut added = Vec::new();
            for attribute in wanted {
                if !list.iter().any(|v| v.as_str() == Some(attribute.as_str())) {
                    list.push(Value::String(attribute.clone()));
                    added.push(attribute);
                }
            }
            added
        }
        _ => {
            map.insert("include_attributes".into(), json!(wanted));
            wanted
        }
    }
}

fn is_missing(value: Option<&Value>, extra: &[Value]) -> bool {
    match value {
        None | Some(Value::Null) => true,
        Some(Value::String(s)) if s.is_empty() => true,
        Some(v) => extra.iter().any(|m| m == v),
    }
}

/// One group found by [`group_indices`]: its key, the attribute name it was
/// formed on (null for a row with no value) and the members' positions in the
/// ranked input, best first.
pub(crate) struct GroupSlots {
    pub key: Value,
    pub by: Value,
    pub members: Vec<usize>,
}

/// Group ranked rows by position. Shared by `/query` (JSON rows) and
/// `/search` (reranked candidates, which build a row of group attributes).
pub(crate) fn group_indices(
    rows: &[Value],
    by: &[(String, String)],
    spec: &CollapseSpec,
    top_k: usize,
) -> Vec<GroupSlots> {
    let mut groups: Vec<GroupSlots> = Vec::new();
    let mut index: HashMap<String, usize> = HashMap::new();
    for (position, row) in rows.iter().enumerate() {
        // The first listed attribute the row has a value for.
        let found = by.iter().find_map(|(name, attribute)| {
            let value = row.get(attribute);
            (!is_missing(value, &spec.missing_values)).then(|| (name, value.cloned().unwrap()))
        });
        let (slot, key, by_name) = match found {
            Some((name, value)) => (Some(format!("{name}\u{0}{value}")), value, json!(name)),
            None => match spec.missing {
                MissingMode::Drop => continue,
                MissingMode::Together => {
                    (Some("\u{1}missing".to_string()), Value::Null, Value::Null)
                }
                MissingMode::Own => (None, Value::Null, Value::Null),
            },
        };
        match slot.as_ref().and_then(|s| index.get(s)) {
            Some(&at) => groups[at].members.push(position),
            None => {
                if let Some(slot) = slot {
                    index.insert(slot, groups.len());
                }
                groups.push(GroupSlots {
                    key,
                    by: by_name,
                    members: vec![position],
                });
            }
        }
    }
    groups.truncate(top_k);
    groups
}

fn group_rows(
    rows: &[Value],
    by: &[(String, String)],
    spec: &CollapseSpec,
    top_k: usize,
    added: &[String],
) -> Vec<Value> {
    group_indices(rows, by, spec, top_k)
        .into_iter()
        .map(|g| {
            let members: Vec<Value> = g
                .members
                .into_iter()
                .map(|at| {
                    let mut row = rows[at].clone();
                    if let Some(row) = row.as_object_mut() {
                        for attribute in added {
                            row.remove(attribute);
                        }
                    }
                    row
                })
                .collect();
            json!({"key": g.key, "by": g.by, "rows": members})
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Schema `document: true`
// ---------------------------------------------------------------------------

pub(crate) fn marker_key(namespace: &str) -> String {
    format!("collapse/{namespace}/document.json")
}

/// The attribute the namespace declares as its document: `document: true` in
/// the store's schema (Postgres keeps it there), else the marker the gateway
/// holds for stores that reject the key (Turbopuffer).
async fn document_attribute(
    state: &AppState,
    namespace: &str,
    schema: &Value,
) -> Result<Option<String>, AppError> {
    if let Some(found) = schema.as_object().and_then(|attrs| {
        attrs
            .iter()
            .find(|(_, a)| a.get(DOCUMENT) == Some(&Value::Bool(true)))
            .map(|(name, _)| name.clone())
    }) {
        return Ok(Some(found));
    }
    stored_marker(state, namespace).await
}

async fn stored_marker(state: &AppState, namespace: &str) -> Result<Option<String>, AppError> {
    match state.s3.get(&marker_key(namespace)).await {
        Ok(Some(body)) => Ok(serde_json::from_slice::<Value>(&body).ok().and_then(|v| {
            v.get("attribute")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })),
        Ok(None) => Ok(None),
        Err(e) if e.is_not_configured() => Ok(None),
        Err(e) => Err(AppError::Upstream(format!(
            "read collapse document marker: {e}"
        ))),
    }
}

/// On a write to a store that does not know `document`, take it out of the
/// schema and keep it beside the namespace.
pub(crate) async fn capture_document_schema(
    state: &AppState,
    namespace: &str,
    body: &mut Value,
) -> Result<(), AppError> {
    let Some(schema) = body.get_mut("schema").and_then(Value::as_object_mut) else {
        return Ok(());
    };
    let mut marked = Vec::new();
    let mut cleared = false;
    for (name, config) in schema.iter_mut() {
        let Some(config) = config.as_object_mut() else {
            continue;
        };
        match config.remove(DOCUMENT) {
            None => {}
            Some(Value::Bool(true)) => {
                if config
                    .get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|t| t != "string")
                {
                    return Err(bad(format!(
                        "schema attribute `{name}`: `document: true` needs a string attribute"
                    )));
                }
                marked.push(name.clone());
            }
            Some(Value::Bool(false)) => cleared = true,
            Some(_) => {
                return Err(bad(format!(
                    "schema attribute `{name}`: `document` must be a boolean"
                )))
            }
        }
    }
    if marked.len() > 1 {
        return Err(bad("at most one schema attribute can be `document: true`"));
    }
    let key = marker_key(namespace);
    if let Some(attribute) = marked.pop() {
        let bytes = serde_json::to_vec(&json!({ "attribute": attribute })).expect("json");
        state.s3.put(&key, bytes).await.map_err(|e| {
            if e.is_not_configured() {
                bad("schema `document: true` on this store needs object storage, which is not configured")
            } else {
                AppError::Upstream(format!("persist collapse document marker: {e}"))
            }
        })?;
    } else if cleared {
        match state.s3.delete_key(&key).await {
            Ok(()) => {}
            Err(e) if e.is_not_configured() => {}
            Err(e) => {
                return Err(AppError::Upstream(format!(
                    "clear collapse document marker: {e}"
                )))
            }
        }
    }
    Ok(())
}

/// Put the held `document: true` back on a schema read.
pub(crate) async fn annotate_schema(
    state: &AppState,
    namespace: &str,
    schema: &mut Value,
) -> Result<(), AppError> {
    let Some(attributes) = schema.as_object_mut() else {
        return Ok(());
    };
    if let Some(attribute) = stored_marker(state, namespace).await? {
        if let Some(config) = attributes
            .get_mut(&attribute)
            .and_then(Value::as_object_mut)
        {
            config.insert(DOCUMENT.into(), Value::Bool(true));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(raw: Value) -> CollapseSpec {
        parse(&raw).unwrap()
    }

    fn by(names: &[&str]) -> Vec<(String, String)> {
        names
            .iter()
            .map(|n| (n.to_string(), n.to_string()))
            .collect()
    }

    #[test]
    fn defaults_and_validation() {
        let s = spec(json!({"by": "doc"}));
        assert_eq!(s.expansion, DEFAULT_EXPANSION);
        assert_eq!(s.missing, MissingMode::Own);
        for bad in [
            json!({}),
            json!({"by": []}),
            json!({"by": 3}),
            json!({"by": "id"}),
            json!({"by": "a", "expansion": 0}),
            json!({"by": "a", "expansion": 10001}),
            json!({"by": "a", "missing": "nope"}),
            json!({"by": "a", "extra": 1}),
            json!({"by": "a", "missing": {"values": 3}}),
        ] {
            assert!(parse(&bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn groups_in_rank_order_and_cuts_to_top_k() {
        let rows = vec![
            json!({"id": "a1", "doc": "a"}),
            json!({"id": "b1", "doc": "b"}),
            json!({"id": "a2", "doc": "a"}),
            json!({"id": "c1", "doc": "c"}),
        ];
        let out = group_rows(&rows, &by(&["doc"]), &spec(json!({"by": "doc"})), 2, &[]);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["key"], "a");
        assert_eq!(out[0]["rows"].as_array().unwrap().len(), 2);
        assert_eq!(out[1]["key"], "b");
    }

    #[test]
    fn missing_modes_and_values_fall_back_down_the_list() {
        let rows = vec![
            json!({"id": 1, "job": "No job", "folder": "f1"}),
            json!({"id": 2, "job": "", "folder": "f1"}),
            json!({"id": 3, "job": "J", "folder": "f1"}),
            json!({"id": 4}),
            json!({"id": 5}),
        ];
        let by = by(&["job", "folder"]);
        let fallback = spec(json!({"by": ["job", "folder"], "missing": {"values": ["No job"]}}));
        let out = group_rows(&rows, &by, &fallback, 10, &[]);
        // rows 1 and 2 share folder f1; 3 is job J; 4 and 5 stay alone.
        assert_eq!(out.len(), 4);
        assert_eq!(out[0]["by"], "folder");
        assert_eq!(out[0]["rows"].as_array().unwrap().len(), 2);
        let together = spec(json!({"by": ["job", "folder"], "missing": "together"}));
        let out = group_rows(&rows[3..], &by, &together, 10, &[]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["key"], Value::Null);
        let drop = spec(json!({"by": ["job", "folder"], "missing": "drop"}));
        assert!(group_rows(&rows[3..], &by, &drop, 10, &[]).is_empty());
    }

    #[test]
    fn scalar_missing_values_use_json_types_and_preserve_fallback_precedence() {
        let rows = vec![
            json!({"id": 1, "job": 0, "folder": "fallback"}),
            json!({"id": 2, "job": false, "folder": "fallback"}),
            json!({"id": 3, "job": "0", "folder": "fallback"}),
            json!({"id": 4, "job": "fallback", "folder": "fallback"}),
        ];
        let spec = spec(json!({"by": ["job", "folder"], "missing": {"values": [0, false]}}));
        let out = group_rows(&rows, &by(&["job", "folder"]), &spec, 10, &[]);
        assert_eq!(out.len(), 3);
        assert_eq!(out[0]["by"], "folder");
        assert_eq!(out[0]["rows"].as_array().unwrap().len(), 2);
        assert_eq!(out[1]["key"], "0", "string zero is not numeric zero");
        assert_eq!(out[2]["by"], "job", "present job wins over folder");
        assert_eq!(
            out[0]["key"], out[2]["key"],
            "equal values on different attributes stay separate"
        );
    }

    #[test]
    fn grouping_attribute_added_for_the_gateway_is_stripped() {
        let mut map = json!({"include_attributes": ["name"]})
            .as_object()
            .unwrap()
            .clone();
        let added = ensure_attributes(&mut map, &by(&["doc"]));
        assert_eq!(map["include_attributes"], json!(["name", "doc"]));
        let rows = vec![json!({"id": "a", "doc": "d", "name": "n"})];
        let out = group_rows(&rows, &by(&["doc"]), &spec(json!({"by": "doc"})), 5, &added);
        assert_eq!(out[0]["rows"][0], json!({"id": "a", "name": "n"}));
    }
}
