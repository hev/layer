//! `GET /v2/namespaces` — layer-augmented namespace listing.
//!
//! Composes:
//!   * upstream Turbopuffer `/v1/namespaces` (passthrough; returns the
//!     paginated list of namespace names + the upstream cursor),
//!   * per-namespace `/metadata` (bounded concurrency, lifted via
//!     `head_namespace`) for row count, byte size, schema, and
//!     `last_write_at`, `stable_as_of_ms`, and `is_stable`,
//!   * the document cache state already surfaced by `/health`.
//!
//! Per-row metadata failures degrade to a row with `metadata_error` set
//! rather than dropping the namespace — the dashboard can still render a
//! "metadata unavailable" badge instead of losing the row entirely.
//!
//! A short-TTL response cache (keyed by the upstream query string) absorbs
//! dashboard polling so the gateway does not fan out a metadata call per row
//! per refresh.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use axum::extract::{OriginalUri, Path, Query, State};
use axum::Json;
use futures::stream::{FuturesUnordered, StreamExt};
use serde::Deserialize;
use serde_json::Value;
use tracing::warn;

use crate::clients::turbopuffer::{IndexStatus, NamespaceMeta, TurbopufferError};
use crate::consistency::now_ms;
use crate::error::AppError;
use crate::history::search_history_cache_namespace;
use crate::models::{
    NamespaceCacheState, NamespaceList, NamespaceListEntry, NamespaceSchemaSummary, StatusResponse,
};
use crate::snapshots::{latest_snapshot_cache_key, SNAPSHOT_CACHE_SET};
use crate::AppState;

/// Maximum number of per-namespace metadata calls in flight at once. The
/// upstream namespace list page is capped at 1000, so without a bound a
/// single tab open could fan out a thousand parallel metadata requests.
const METADATA_FANOUT_CONCURRENCY: usize = 16;

fn checkpoint_s3_prefix(namespace: &str) -> String {
    format!("checkpoints/{namespace}/")
}

#[derive(Debug, Deserialize)]
pub struct ListNamespacesQuery {
    pub prefix: Option<String>,
    pub cursor: Option<String>,
    pub page_size: Option<u32>,
}

pub async fn list_namespaces(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ListNamespacesQuery>,
) -> Result<Json<NamespaceList>, AppError> {
    let cache_key = cache_key_from(&params);

    // Fast path: a fresh cached response covers the same query exactly. The
    // TTL is intentionally short (default 10s) so the dashboard still feels
    // live without N × per-namespace metadata calls per refresh.
    if !state.namespace_list_cache_ttl.is_zero() {
        if let Some(entry) = state.namespace_list_cache.get(&cache_key) {
            let (cached_at, list) = entry.value();
            if cached_at.elapsed() < state.namespace_list_cache_ttl {
                return Ok(Json(list.clone()));
            }
        }
    }

    let response = fetch_namespace_list(&state, &params).await?;

    if !state.namespace_list_cache_ttl.is_zero() {
        state
            .namespace_list_cache
            .insert(cache_key, (Instant::now(), response.clone()));
    }

    Ok(Json(response))
}

pub async fn delete_namespace(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
    OriginalUri(uri): OriginalUri,
) -> Result<Json<StatusResponse>, AppError> {
    if namespace.trim().is_empty() {
        return Err(AppError::Validation("namespace is required".to_string()));
    }

    let upstream = state
        .turbopuffer()
        .passthrough("DELETE", uri.path(), uri.query(), None)
        .await
        .map_err(|e| AppError::Upstream(format!("Turbopuffer namespace delete failed: {e}")))?;

    if upstream.status >= 400 && upstream.status != 404 {
        return Err(AppError::Upstream(format!(
            "Turbopuffer namespace delete returned {}",
            upstream.status
        )));
    }

    let cleanup = cleanup_namespace_state(&state, &namespace).await;
    if !cleanup.errors.is_empty() {
        return Err(AppError::Upstream(format!(
            "namespace '{}' deleted upstream but local cleanup was incomplete: {}",
            namespace,
            cleanup.errors.join("; ")
        )));
    }

    let response = StatusResponse {
        message: Some(format!(
            "namespace deleted; purged {} S3 object{}",
            cleanup.s3_objects_deleted,
            if cleanup.s3_objects_deleted == 1 {
                ""
            } else {
                "s"
            }
        )),
        ..Default::default()
    };
    Ok(Json(response))
}

#[derive(Debug, Default)]
struct NamespaceCleanupOutcome {
    s3_objects_deleted: u64,
    errors: Vec<String>,
}

async fn cleanup_namespace_state(state: &AppState, namespace: &str) -> NamespaceCleanupOutcome {
    let mut outcome = NamespaceCleanupOutcome::default();

    if let Err(e) = state.aerospike.delete_set(namespace).await {
        outcome
            .errors
            .push(format!("Aerospike document cache purge failed: {e}"));
    }

    let snapshot_cache_key = latest_snapshot_cache_key(namespace);
    if let Err(e) = state
        .aerospike
        .delete(SNAPSHOT_CACHE_SET, &snapshot_cache_key)
        .await
    {
        outcome
            .errors
            .push(format!("Aerospike latest snapshot purge failed: {e}"));
    }

    let history_cache_namespace = search_history_cache_namespace(namespace);
    if let Err(e) = state.aerospike.delete_set(&history_cache_namespace).await {
        outcome
            .errors
            .push(format!("Aerospike search history purge failed: {e}"));
    }

    for prefix in namespace_s3_prefixes(namespace) {
        match delete_s3_prefix(state, &prefix).await {
            Ok(deleted) => outcome.s3_objects_deleted += deleted,
            Err(e) => outcome
                .errors
                .push(format!("S3 purge for prefix '{prefix}' failed: {e}")),
        }
    }

    if let Some(index_deleter) = &state.index_deleter {
        if let Err(e) = index_deleter.delete_index_for_namespace(namespace).await {
            outcome
                .errors
                .push(format!("Index CR garbage collection failed: {e}"));
        }
    }

    purge_in_memory_namespace_state(state, namespace);

    if !outcome.errors.is_empty() {
        warn!(
            namespace = %namespace,
            errors = ?outcome.errors,
            "Namespace hard-delete local cleanup was incomplete"
        );
    }

    outcome
}

fn namespace_s3_prefixes(namespace: &str) -> [String; 5] {
    [
        format!("snapshots/{namespace}/"),
        checkpoint_s3_prefix(namespace),
        format!("search-history/{namespace}/"),
        format!("clickstream/{namespace}/"),
        format!("shards/{namespace}/"),
    ]
}

async fn delete_s3_prefix(state: &AppState, prefix: &str) -> Result<u64, String> {
    let keys = state
        .s3
        .list_keys(prefix)
        .await
        .map_err(|e| e.to_string())?;
    let mut deleted = 0;
    for key in keys {
        state
            .s3
            .delete_key(&key)
            .await
            .map_err(|e| format!("{key}: {e}"))?;
        deleted += 1;
    }
    Ok(deleted)
}

fn purge_in_memory_namespace_state(state: &AppState, namespace: &str) {
    let job_ids: Vec<String> = state
        .jobs
        .iter()
        .filter(|entry| entry.value().response.namespace == namespace)
        .map(|entry| entry.key().clone())
        .collect();
    for job_id in job_ids {
        state.jobs.remove(&job_id);
    }

    state.consistency.forget_namespace(namespace);
    state.cache_warmed_through.remove(namespace);
    state.cache_namespaces.remove(namespace);
    state.warm_inflight.remove(namespace);
    state.reactive_warm_generations.remove(namespace);
    state.last_snapshot_at.remove(namespace);
    state.snapshot_inflight.remove(namespace);
    state.sharded_namespaces.remove(namespace);
    state
        .blended_embedding_states
        .retain(|key, _| !key.starts_with(&format!("{namespace}\0")));
    state.namespace_list_cache.clear();
}

fn cache_key_from(params: &ListNamespacesQuery) -> String {
    format!(
        "prefix={}&cursor={}&page_size={}",
        params.prefix.as_deref().unwrap_or(""),
        params.cursor.as_deref().unwrap_or(""),
        params.page_size.map(|n| n.to_string()).unwrap_or_default(),
    )
}

async fn fetch_namespace_list(
    state: &Arc<AppState>,
    params: &ListNamespacesQuery,
) -> Result<NamespaceList, AppError> {
    let upstream_query = build_upstream_query(params);
    let upstream = state
        .turbopuffer()
        .passthrough("GET", "/v1/namespaces", upstream_query.as_deref(), None)
        .await
        .map_err(|e| AppError::Upstream(format!("Turbopuffer namespace list: {}", e)))?;

    if upstream.status >= 400 {
        return Err(AppError::Upstream(format!(
            "Turbopuffer namespace list returned {}",
            upstream.status
        )));
    }

    let body: Value = serde_json::from_slice(&upstream.body)
        .map_err(|e| AppError::Upstream(format!("Turbopuffer namespace list parse: {}", e)))?;

    let names: Vec<String> = body
        .get("namespaces")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|n| {
                    n.as_str()
                        .map(str::to_string)
                        .or_else(|| n.get("id").and_then(|s| s.as_str()).map(str::to_string))
                })
                .collect()
        })
        .unwrap_or_default();

    let next_cursor = body
        .get("next_cursor")
        .and_then(|v| v.as_str())
        .map(str::to_string);

    let entries = fetch_entries(state, names).await;

    Ok(NamespaceList {
        namespaces: entries,
        next_cursor,
    })
}

fn build_upstream_query(params: &ListNamespacesQuery) -> Option<String> {
    let mut parts: Vec<String> = Vec::new();
    if let Some(prefix) = params.prefix.as_deref().filter(|s| !s.is_empty()) {
        parts.push(format!("prefix={}", urlencode(prefix)));
    }
    if let Some(cursor) = params.cursor.as_deref().filter(|s| !s.is_empty()) {
        parts.push(format!("cursor={}", urlencode(cursor)));
    }
    if let Some(page_size) = params.page_size {
        parts.push(format!("page_size={}", page_size));
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("&"))
    }
}

/// Minimal RFC 3986 query-string encoder so we never have to pull in a
/// dependency just to escape a cursor. Anything that isn't an unreserved
/// character gets percent-encoded.
fn urlencode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(*byte as char);
            }
            _ => {
                out.push_str(&format!("%{:02X}", byte));
            }
        }
    }
    out
}

async fn fetch_entries(state: &Arc<AppState>, names: Vec<String>) -> Vec<NamespaceListEntry> {
    let mut in_flight = FuturesUnordered::new();
    let mut name_iter = names.into_iter();
    let mut results: Vec<(usize, NamespaceListEntry)> = Vec::new();
    let mut next_index: usize = 0;

    while in_flight.len() < METADATA_FANOUT_CONCURRENCY {
        let Some(name) = name_iter.next() else { break };
        let idx = next_index;
        next_index += 1;
        in_flight.push(fetch_one(state.clone(), name, idx));
    }

    while let Some((idx, entry)) = in_flight.next().await {
        results.push((idx, entry));
        if let Some(name) = name_iter.next() {
            let next_idx = next_index;
            next_index += 1;
            in_flight.push(fetch_one(state.clone(), name, next_idx));
        }
    }

    results.sort_by_key(|(idx, _)| *idx);
    results.into_iter().map(|(_, entry)| entry).collect()
}

async fn fetch_one(
    state: Arc<AppState>,
    namespace: String,
    idx: usize,
) -> (usize, NamespaceListEntry) {
    let cache_state = build_cache_state(&state, &namespace).await;
    let shadow = namespace.ends_with("-shadow");

    let metadata_started_ms = now_ms();
    let meta = state.turbopuffer().head_namespace(&namespace).await;
    match meta {
        Ok(meta) => {
            state
                .consistency
                .register_from_metadata(&namespace, meta.index_status);
            let projected = project_metadata(&meta);
            let (stable_as_of_ms, is_stable) = stability_from_meta(&meta, metadata_started_ms);
            (
                idx,
                NamespaceListEntry {
                    name: namespace,
                    row_count: Some(projected.row_count),
                    size_bytes: projected.size_bytes,
                    stable_as_of_ms,
                    is_stable,
                    schema_summary: projected.schema_summary,
                    index: projected.index,
                    cache_state,
                    last_write_ms: projected.last_write_ms,
                    shadow,
                    labels: projected.labels,
                    metadata_error: None,
                },
            )
        }
        Err(err) => (
            idx,
            NamespaceListEntry {
                name: namespace,
                row_count: None,
                size_bytes: None,
                stable_as_of_ms: None,
                is_stable: None,
                schema_summary: None,
                index: None,
                cache_state,
                last_write_ms: None,
                shadow,
                labels: HashMap::new(),
                metadata_error: Some(format_meta_error(&err)),
            },
        ),
    }
}

fn stability_from_meta(meta: &NamespaceMeta, stable_as_of_ms: u64) -> (Option<u64>, Option<bool>) {
    match meta.index_status {
        IndexStatus::Stable => (Some(stable_as_of_ms), Some(true)),
        IndexStatus::Updating => (None, Some(false)),
        IndexStatus::Unknown => (None, None),
    }
}

async fn build_cache_state(state: &Arc<AppState>, namespace: &str) -> NamespaceCacheState {
    let cache_state = state.cache_state_for_namespace(namespace).await;
    let warmed_through_ms = state
        .cache_warmed_through
        .get(namespace)
        .map(|r| *r.value());
    let warm_inflight = state.warm_inflight.contains_key(namespace);
    NamespaceCacheState {
        state: cache_state.as_str().to_string(),
        warmed_through_ms,
        warm_inflight,
    }
}

fn format_meta_error(err: &TurbopufferError) -> String {
    // Keep the error short — it ends up rendered as a UI badge — and avoid
    // leaking long upstream HTML/JSON bodies into the list payload.
    let raw = err.to_string();
    if raw.len() > 200 {
        format!("{}…", &raw[..200])
    } else {
        raw
    }
}

/// Fields projected from the Turbopuffer `/metadata` body for the list row.
/// Bundled into a struct so callers don't pass a five-tuple around.
struct ProjectedMetadata {
    row_count: u64,
    size_bytes: Option<u64>,
    schema_summary: Option<NamespaceSchemaSummary>,
    labels: HashMap<String, String>,
    last_write_ms: Option<u64>,
    /// Upstream `index` object, copied verbatim so unknown sub-fields ride
    /// through unchanged (forward-compatible with future Turbopuffer shapes).
    index: Option<Value>,
}

fn project_metadata(meta: &NamespaceMeta) -> ProjectedMetadata {
    let schema_summary = meta
        .raw
        .get("schema")
        .and_then(|v| v.as_object())
        .map(|schema| {
            let mut fields: Vec<String> = schema.keys().cloned().collect();
            fields.sort();
            let vector_dim = schema.values().find_map(extract_vector_dim);
            NamespaceSchemaSummary { vector_dim, fields }
        });

    ProjectedMetadata {
        row_count: meta.approx_row_count,
        size_bytes: meta
            .raw
            .get("approx_logical_bytes")
            .and_then(|v| v.as_u64()),
        schema_summary,
        labels: extract_labels(&meta.raw),
        last_write_ms: meta
            .raw
            .get("last_write_at")
            .and_then(|v| v.as_str())
            .and_then(parse_rfc3339_to_ms),
        index: meta.raw.get("index").cloned(),
    }
}

fn extract_vector_dim(field: &Value) -> Option<u64> {
    // Turbopuffer's schema shape isn't strictly versioned in the public
    // metadata response — we've seen `dimensions`, `dims`, and a nested
    // `ann.dimensions`. Probe each. Any non-vector field will simply not
    // carry a dimensions key, so this short-circuits cleanly.
    field
        .get("dimensions")
        .and_then(|v| v.as_u64())
        .or_else(|| field.get("dims").and_then(|v| v.as_u64()))
        .or_else(|| {
            field
                .get("ann")
                .and_then(|a| a.get("dimensions"))
                .and_then(|v| v.as_u64())
        })
}

fn extract_labels(raw: &Value) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let candidates = [
        raw.get("labels"),
        raw.get("config").and_then(|c| c.get("labels")),
    ];
    for candidate in candidates.into_iter().flatten() {
        if let Some(obj) = candidate.as_object() {
            for (k, v) in obj {
                if let Some(s) = v.as_str() {
                    out.insert(k.clone(), s.to_string());
                }
            }
            if !out.is_empty() {
                break;
            }
        }
    }
    out
}

fn parse_rfc3339_to_ms(value: &str) -> Option<u64> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .and_then(|dt| {
            let ms = dt.timestamp_millis();
            if ms >= 0 {
                Some(ms as u64)
            } else {
                None
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn cache_key_distinguishes_params() {
        let a = cache_key_from(&ListNamespacesQuery {
            prefix: Some("foo".into()),
            cursor: None,
            page_size: Some(50),
        });
        let b = cache_key_from(&ListNamespacesQuery {
            prefix: Some("bar".into()),
            cursor: None,
            page_size: Some(50),
        });
        assert_ne!(a, b);
    }

    #[test]
    fn upstream_query_skips_empty_values() {
        let q = build_upstream_query(&ListNamespacesQuery {
            prefix: Some(String::new()),
            cursor: None,
            page_size: None,
        });
        assert!(q.is_none());
    }

    #[test]
    fn upstream_query_url_encodes_cursor() {
        // Cursors are opaque base64-ish strings; if a `=` ever leaks
        // through, the resulting query string must remain valid.
        let q = build_upstream_query(&ListNamespacesQuery {
            prefix: None,
            cursor: Some("a/b+c=".into()),
            page_size: Some(100),
        })
        .unwrap();
        assert_eq!(q, "cursor=a%2Fb%2Bc%3D&page_size=100");
    }

    #[test]
    fn project_metadata_extracts_schema_fields_and_vector_dim() {
        let meta = NamespaceMeta {
            index_status: IndexStatus::Stable,
            unindexed_bytes: None,
            approx_row_count: 42,
            approx_logical_bytes: Some(9001),
            count_settle: None,
            raw: json!({
                "approx_row_count": 42,
                "approx_logical_bytes": 9001,
                "schema": {
                    "title": { "type": "string" },
                    "vector": { "type": "f32", "dimensions": 512 }
                },
                "last_write_at": "2026-05-21T00:00:00Z",
                "labels": { "env": "production" },
                "index": { "status": "up-to-date" }
            }),
        };
        let projected = project_metadata(&meta);
        assert_eq!(projected.row_count, 42);
        assert_eq!(projected.size_bytes, Some(9001));
        // `index` rides through verbatim from the upstream body.
        assert_eq!(projected.index, Some(json!({ "status": "up-to-date" })));
        let schema = projected.schema_summary.expect("schema summary present");
        assert_eq!(schema.vector_dim, Some(512));
        assert_eq!(
            schema.fields,
            vec!["title".to_string(), "vector".to_string()]
        );
        assert_eq!(
            projected.labels.get("env").map(String::as_str),
            Some("production")
        );
        assert!(projected.last_write_ms.is_some());
    }

    #[test]
    fn project_metadata_handles_missing_optional_fields() {
        let meta = NamespaceMeta {
            index_status: IndexStatus::Unknown,
            unindexed_bytes: None,
            approx_row_count: 0,
            approx_logical_bytes: None,
            count_settle: None,
            raw: json!({ "approx_row_count": 0 }),
        };
        let projected = project_metadata(&meta);
        assert_eq!(projected.row_count, 0);
        assert_eq!(projected.size_bytes, None);
        assert!(projected.schema_summary.is_none());
        assert!(projected.labels.is_empty());
        assert!(projected.last_write_ms.is_none());
        // No upstream `index` field → omitted, not fabricated.
        assert!(projected.index.is_none());
    }

    #[test]
    fn project_metadata_passes_updating_index_with_unindexed_bytes() {
        let meta = NamespaceMeta {
            index_status: IndexStatus::Updating,
            unindexed_bytes: Some(4_194_304),
            approx_row_count: 7,
            approx_logical_bytes: None,
            count_settle: None,
            raw: json!({
                "approx_row_count": 7,
                "index": { "status": "updating", "unindexed_bytes": 4194304 }
            }),
        };
        let projected = project_metadata(&meta);
        // The whole upstream object passes through, including unindexed_bytes.
        assert_eq!(
            projected.index,
            Some(json!({ "status": "updating", "unindexed_bytes": 4194304 }))
        );
    }
}
