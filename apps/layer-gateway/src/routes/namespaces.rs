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

use crate::auth::{authorize_namespace, can_list_namespace, can_list_store, ApiScope, CallerGrant};
use axum::extract::{Path, Query, State};
use axum::{Extension, Json};
use futures::stream::{FuturesUnordered, StreamExt};
use serde::Deserialize;
use serde_json::Value;
use tracing::{debug, warn};

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

#[derive(Clone, Debug, Deserialize)]
pub struct ListNamespacesQuery {
    pub prefix: Option<String>,
    pub cursor: Option<String>,
    pub page_size: Option<u32>,
}

pub async fn list_namespaces(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ListNamespacesQuery>,
    grant: Option<Extension<CallerGrant>>,
) -> Result<Json<NamespaceList>, AppError> {
    let cache_key = cache_key_from(&params);
    // The shared cache is only safe for unrestricted callers.
    let unrestricted = match grant.as_ref().map(|grant| &grant.0) {
        None | Some(CallerGrant::Declared(_)) => true,
        #[cfg(feature = "pro")]
        Some(CallerGrant::Minted(_)) => false,
    };
    let cache_enabled = unrestricted && !state.namespace_list_cache_ttl.is_zero();

    // Fast path: a fresh cached response covers the same query exactly. The
    // TTL is intentionally short (default 10s) so the dashboard still feels
    // live without N × per-namespace metadata calls per refresh.
    if cache_enabled {
        if let Some(entry) = state.namespace_list_cache.get(&cache_key) {
            let (cached_at, list) = entry.value();
            if cached_at.elapsed() < state.namespace_list_cache_ttl {
                return Ok(Json(list.clone()));
            }
        }
    }

    let response = fetch_namespace_list(&state, &params, grant.as_ref().map(|g| &g.0)).await?;

    if cache_enabled {
        state
            .namespace_list_cache
            .insert(cache_key, (Instant::now(), response.clone()));
    }

    Ok(Json(response))
}

pub async fn delete_namespace(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    use vectorstore_core::delete_timing::Timings;
    let timings = Timings::default();
    let started = Instant::now();
    let result = timings
        .scope(delete_namespace_inner(state, namespace))
        .await;
    let mut response = result.into_response();
    let header = timings.header(started.elapsed());
    tracing::info!(server_timing = %header, "Namespace DELETE foreground completed");
    response.headers_mut().insert(
        "server-timing",
        header.parse().expect("fixed labels and numeric durations"),
    );
    response
}

async fn delete_namespace_inner(
    state: Arc<AppState>,
    namespace: String,
) -> Result<Json<StatusResponse>, AppError> {
    use vectorstore_core::delete_timing::{start, Phase};
    if namespace.trim().is_empty() {
        return Err(AppError::Validation("namespace is required".to_string()));
    }

    // Namespace replacement must serialize with strong Function source reads
    // and completion writes just like row mutations.
    let function_timer = start(Phase::Function);
    let _function_guard = if let Some(trigger) = state.write_trigger.as_ref() {
        Some(
            trigger
                .prepare_replacement(Arc::clone(&state), &namespace)
                .await?,
        )
    } else {
        None
    };
    drop(function_timer);
    crate::run_guarded_write(_function_guard.as_deref(), async {
        let intent_timer = start(Phase::Intent);
        let (intent, store) = state.namespace_purges.prepare(&state, &namespace).await?;
        drop(intent_timer);
        let upstream_timer = start(Phase::Upstream);

        let upstream = state
            .turbopuffer()
            .delete_namespace_in_store(&namespace, &store)
            .await
            .map_err(|e| AppError::Upstream(format!("VectorStore namespace delete failed: {e}")))?;

        drop(upstream_timer);
        if upstream.status >= 400 && upstream.status != 404 {
            return Err(AppError::Upstream(format!(
                "VectorStore namespace delete returned {}",
                upstream.status
            )));
        }

        // Keep in-memory invalidation on the request path, before any local I/O
        // cleanup that may later run in the background.
        let invalidate_timer = start(Phase::Invalidate);
        purge_in_memory_namespace_state(&state, &namespace);
        drop(invalidate_timer);
        let notify_timer = start(Phase::Notify);
        state.namespace_purges.upstream_deleted(&state, &intent);
        drop(notify_timer);
        let response = StatusResponse {
            message: Some(crate::namespace_purge::DELETE_MESSAGE.into()),
            ..Default::default()
        };
        Ok(Json(response))
    })
    .await
}

#[derive(Debug, Default)]
pub(crate) struct NamespaceCleanupOutcome {
    s3_objects_deleted: u64,
    pub(crate) errors: Vec<String>,
}

pub(crate) async fn cleanup_namespace_state(
    state: &AppState,
    namespace: &str,
) -> NamespaceCleanupOutcome {
    let mut outcome = reset_namespace_layer_state(state, namespace).await;

    if let Some(index_deleter) = &state.index_deleter {
        if let Err(e) = index_deleter.delete_index_for_namespace(namespace).await {
            outcome
                .errors
                .push(format!("Index CR garbage collection failed: {e}"));
        }
    }

    if !outcome.errors.is_empty() {
        warn!(
            namespace = %namespace,
            errors = ?outcome.errors,
            "Namespace hard-delete local cleanup was incomplete"
        );
    }

    outcome
}

/// Drop the Layer state held under a namespace's name outside the store:
/// Aerospike cache sets and the S3 prefixes for snapshots, checkpoints,
/// history and shard manifests. Unlike a hard delete it leaves the Index
/// CR alone. A new branch runs this so it never inherits a deleted
/// namespace's residue (RFC 0124).
pub(crate) async fn reset_namespace_layer_state(
    state: &AppState,
    namespace: &str,
) -> NamespaceCleanupOutcome {
    let mut outcome = NamespaceCleanupOutcome::default();

    // Generation 0 means no cache client has ever been part of this process
    // (standalone/open composition, or a pro gateway booted without
    // AEROSPIKE_HOSTS): there is no cache to purge, so skip rather than count
    // the purge as incomplete cleanup. A cache that was connected and dropped
    // keeps its errors — skipping the purge there could resurrect deleted
    // documents.
    if state.aerospike_runtime.generation() == 0 {
        debug!(
            namespace = %namespace,
            "document cache never composed; skipping cache purge on namespace delete"
        );
    } else {
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

        #[cfg(feature = "pro")]
        if let Err(e) = crate::field_stats::purge(state, namespace).await {
            outcome
                .errors
                .push(format!("field stats purge failed: {e}"));
        }

        let history_cache_namespace = search_history_cache_namespace(namespace);
        if let Err(e) = state.aerospike.delete_set(&history_cache_namespace).await {
            outcome
                .errors
                .push(format!("Aerospike search history purge failed: {e}"));
        }
    }

    if state.s3.is_configured() {
        #[allow(unused_mut)]
        let mut keys = vec![
            crate::lineage::lineage_key(namespace),
            crate::routes::collapse::marker_key(namespace),
        ];
        #[cfg(feature = "pro")]
        keys.push(crate::field_stats::s3_key(namespace));
        keys.push(format!("field-stats/{namespace}/reconcile-identity.json"));
        if let Err(e) = state.s3.delete_keys(&keys).await {
            outcome
                .errors
                .push(format!("S3 key purge for {keys:?} failed: {e}"));
        }
    }

    let purges = namespace_s3_prefixes(namespace)
        .into_iter()
        .map(|prefix| async move {
            let result = delete_s3_prefix(state, &prefix).await;
            (prefix, result)
        });
    for (prefix, result) in futures::future::join_all(purges).await {
        match result {
            Ok(deleted) => outcome.s3_objects_deleted += deleted,
            Err(e) => outcome
                .errors
                .push(format!("S3 purge for prefix '{prefix}' failed: {e}")),
        }
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
    if !state.s3.is_configured() {
        return Ok(0);
    }
    let keys = state
        .s3
        .list_keys(prefix)
        .await
        .map_err(|e| e.to_string())?;
    let mut deleted = 0;
    for batch in keys.chunks(1000) {
        state
            .s3
            .delete_keys(batch)
            .await
            .map_err(|e| e.to_string())?;
        deleted += batch.len() as u64;
    }
    Ok(deleted)
}

pub(crate) fn purge_in_memory_namespace_state(state: &AppState, namespace: &str) {
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
    state.reconcile_identity.remove(namespace);
    state.stats_write_epoch.remove(namespace);
    state.field_stats_coverage.remove(namespace);
    state.snapshot_inflight.remove(namespace);
    state.sharded_namespaces.remove(namespace);
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
    grant: Option<&CallerGrant>,
) -> Result<NamespaceList, AppError> {
    let body = authorized_namespace_page(state, params, grant).await?;

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

/// Keep upstream cursors intact and scan through pages with no readable names.
/// Filtering happens before metadata fanout or any caller-visible response.
pub(crate) async fn authorized_namespace_page(
    state: &AppState,
    params: &ListNamespacesQuery,
    grant: Option<&CallerGrant>,
) -> Result<Value, AppError> {
    if !can_list_store(state, grant) {
        return Ok(serde_json::json!({"namespaces": [], "next_cursor": null}));
    }
    let mut params = params.clone();
    let mut result: Option<Value> = None;
    let mut seen = std::collections::HashSet::new();
    if let Some(cursor) = &params.cursor {
        seen.insert(cursor.clone());
    }
    loop {
        let query = build_upstream_query(&params);
        let upstream = state
            .turbopuffer()
            .passthrough("GET", "/v1/namespaces", query.as_deref(), None)
            .await
            .map_err(|e| AppError::from_turbopuffer(e, "namespace list"))?;
        if upstream.status >= 400 {
            return Err(AppError::from_turbopuffer(
                TurbopufferError::Response(upstream),
                "namespace list",
            ));
        }
        let mut body: Value = serde_json::from_slice(&upstream.body)
            .map_err(|e| AppError::Upstream(format!("namespace list parse: {e}")))?;
        let items = body
            .get_mut("namespaces")
            .and_then(Value::as_array_mut)
            .ok_or_else(|| AppError::Upstream("namespace list missing namespaces array".into()))?;
        items.retain(|item| {
            item.as_str()
                .or_else(|| item.get("id").and_then(Value::as_str))
                .is_some_and(|name| {
                    params
                        .prefix
                        .as_deref()
                        .is_none_or(|prefix| name.starts_with(prefix))
                        && can_list_namespace(state, grant, name)
                        // v2 metadata and federated queries follow Index store
                        // routing; discovery must not expose an unreadable target.
                        && authorize_namespace(state, grant, ApiScope::Read, name).is_ok()
                })
        });
        let empty = items.is_empty();
        let cursor = body
            .get("next_cursor")
            .and_then(Value::as_str)
            .filter(|cursor| !cursor.is_empty())
            .map(str::to_owned);
        if cursor.as_ref().is_some_and(|cursor| seen.contains(cursor)) {
            return Err(AppError::Upstream(
                "namespace list cursor did not advance".into(),
            ));
        }
        if !empty {
            if let Some(mut result) = result {
                // Resume immediately before the next readable page. Looking
                // ahead prevents a trailing denied-only page becoming an
                // empty terminal response for a caller with more pages.
                result["next_cursor"] = serde_json::to_value(&params.cursor)
                    .map_err(|e| AppError::Upstream(e.to_string()))?;
                return Ok(result);
            }
            result = Some(body.clone());
        }
        if cursor.is_none() {
            let mut result = result.unwrap_or(body);
            result["next_cursor"] = Value::Null;
            return Ok(result);
        }
        let cursor = cursor.unwrap();
        if !seen.insert(cursor.clone()) {
            return Err(AppError::Upstream(
                "namespace list cursor did not advance".into(),
            ));
        }
        params.cursor = Some(cursor);
    }
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

    let display = state.index_display_for(&namespace).unwrap_or_default();
    let metadata_started_ms = now_ms();
    let meta = state.turbopuffer().head_namespace(&namespace).await;
    if meta.is_err() {
        state.consistency.invalidate_pinning(&namespace);
    }
    match meta {
        Ok(meta) => {
            state.consistency.observe_pinning(&namespace, &meta.raw);
            let projected = project_metadata(&meta);
            let (stable_as_of_ms, is_stable) = stability_from_meta(&meta, metadata_started_ms);
            (
                idx,
                NamespaceListEntry {
                    name: namespace,
                    title: display.title,
                    description: display.description,
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
                title: display.title,
                description: display.description,
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
