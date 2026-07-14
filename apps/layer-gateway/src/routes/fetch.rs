use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue};
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use tracing::warn;

use crate::error::AppError;
use crate::history::{
    log_clickstream_events, now_timestamp, tags_from_headers, trace_id_from_headers,
};
use crate::metrics::{
    estimate_json_bytes, CACHE_ERROR, CACHE_HIT, CACHE_MISS, CACHE_PARTIAL, STATUS_AEROSPIKE_ERROR,
    STATUS_OK,
};
use crate::models::{ClickstreamEvent, DocumentResponse, FetchManyRequest, FetchManyResponse};

use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct FetchQueryParams {
    pub include_attributes: Option<String>,
}

/// Response header that tells callers how the response was served:
///   - `hit`            — served from Aerospike
///   - `miss`           — clean cache miss; served from turbopuffer
///   - `miss-on-error`  — cache itself errored; served from turbopuffer (degraded)
const LAYER_CACHE_HEADER: &str = "x-layer-cache";

fn response_headers(cache_value: &'static str, vector_warning: bool) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert(LAYER_CACHE_HEADER, HeaderValue::from_static(cache_value));
    if vector_warning {
        h.insert(
            crate::routes::query::LAYER_WARNING_HEADER,
            HeaderValue::from_static(crate::routes::query::VECTOR_ATTRIBUTE_DROPPED_WARNING),
        );
    }
    h
}

/// GET /v2/namespaces/{namespace}/documents/{doc_id}
///
/// Single document fetch with pull-through caching:
/// 1. Try Aerospike — return on hit
/// 2. Cache miss (clean or error) → fall through to turbopuffer
/// 3. 404 if upstream has nothing
///
/// On Aerospike error we degrade rather than fail: the cache is supposed to
/// be a read-through accelerator, not a hard dependency of the read path.
pub async fn fetch_document(
    State(state): State<Arc<AppState>>,
    Path((namespace, doc_id)): Path<(String, String)>,
    Query(params): Query<FetchQueryParams>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, AppError> {
    let total_start = Instant::now();
    let cache_set = state.aerospike_set_name(&namespace);
    state.observe_cache_demand(&namespace);
    let include_attrs: Option<Vec<String>> = params
        .include_attributes
        .map(|s| s.split(',').map(|s| s.trim().to_string()).collect());
    let warn_vector_dropped = include_attrs
        .as_deref()
        .is_some_and(|attrs| attrs.iter().any(|a| a == "vector"));
    let trace_id = trace_id_from_headers(&headers);
    let tags = tags_from_headers(&headers).map_err(AppError::Validation)?;

    // 1. Try Aerospike cache
    let cache_start = Instant::now();
    let cache_result = state
        .aerospike
        .get(&namespace, &doc_id, include_attrs.as_deref())
        .await;
    let cache_lookup_seconds = cache_start.elapsed().as_secs_f64();
    let cache_status = match cache_result {
        Ok(Some(attrs)) => {
            state.metrics.observe_cache_lookup(
                &cache_set,
                &namespace,
                CACHE_HIT,
                1,
                cache_lookup_seconds,
                estimate_json_bytes(&attrs),
            );
            state.metrics.observe_fetch(
                "get_doc",
                &namespace,
                CACHE_HIT,
                total_start.elapsed().as_secs_f64(),
            );
            spawn_clickstream_event(
                &state,
                trace_id.as_deref(),
                &namespace,
                &doc_id,
                &tags,
                "single",
                "cache",
            );
            return Ok((
                response_headers("hit", warn_vector_dropped),
                Json(DocumentResponse {
                    id: doc_id,
                    attributes: attrs,
                }),
            ));
        }
        Ok(None) => {
            state.metrics.observe_cache_lookup(
                &cache_set,
                &namespace,
                CACHE_MISS,
                1,
                cache_lookup_seconds,
                None,
            );
            crate::routes::scans::maybe_spawn_reactive_warm(Arc::clone(&state), namespace.clone())
                .await;
            "miss"
        }
        Err(e) => {
            state.metrics.observe_cache_lookup(
                &cache_set,
                &namespace,
                CACHE_ERROR,
                1,
                cache_lookup_seconds,
                None,
            );
            warn!(
                namespace = %namespace,
                id = %doc_id,
                error = %e,
                "Aerospike unavailable; falling over to turbopuffer"
            );
            "miss-on-error"
        }
    };

    // 2. Fetch from Turbopuffer
    let upstream_doc = state
        .turbopuffer()
        .fetch(&namespace, &doc_id)
        .await
        .map_err(|e| AppError::Upstream(format!("Turbopuffer fetch failed: {}", e)))?;

    let doc = upstream_doc.ok_or_else(|| {
        AppError::NotFound(format!(
            "Document '{}' not found in namespace '{}'",
            doc_id, namespace
        ))
    })?;

    let upstream_attrs_for_cache = doc.attributes.clone();

    // 3. Backfill Aerospike cache (best-effort). Skip if the cache is sick —
    // a backfill that hits the same timeout doesn't help the caller and just
    // adds load to a degraded cluster.
    if cache_status == "miss" {
        let backfill_start = Instant::now();
        let backfill = state
            .aerospike
            .put(&namespace, &doc_id, &upstream_attrs_for_cache)
            .await;
        match backfill {
            Ok(()) => {
                state.metrics.observe_cache_backfill(
                    &cache_set,
                    STATUS_OK,
                    backfill_start.elapsed().as_secs_f64(),
                    None,
                );
                if let Some(bytes) = estimate_json_bytes(&upstream_attrs_for_cache) {
                    state
                        .metrics
                        .observe_cache_payload(&cache_set, CACHE_MISS, bytes);
                }
            }
            Err(e) => {
                state.metrics.observe_cache_backfill(
                    &cache_set,
                    STATUS_AEROSPIKE_ERROR,
                    backfill_start.elapsed().as_secs_f64(),
                    None,
                );
                warn!(
                    namespace = %namespace,
                    id = %doc_id,
                    error = %e,
                    "Aerospike cache backfill failed"
                );
            }
        }
    }

    // 4. Filter attributes if requested
    let attributes = match include_attrs {
        Some(ref attrs) => upstream_attrs_for_cache
            .into_iter()
            .filter(|(k, _)| attrs.contains(k))
            .collect(),
        None => upstream_attrs_for_cache,
    };

    state.metrics.observe_fetch(
        "get_doc",
        &namespace,
        CACHE_MISS,
        total_start.elapsed().as_secs_f64(),
    );
    spawn_clickstream_event(
        &state,
        trace_id.as_deref(),
        &namespace,
        &doc_id,
        &tags,
        "single",
        "upstream",
    );

    Ok((
        response_headers(cache_status, warn_vector_dropped),
        Json(DocumentResponse {
            id: doc_id,
            attributes,
        }),
    ))
}

/// POST /v2/namespaces/{namespace}/documents
///
/// Batch document fetch with pull-through caching. Cache header reflects
/// the worst-case outcome across the batch (`hit` only if every id came
/// from cache; `miss-on-error` if the cache call itself failed).
pub async fn fetch_many_documents(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    Json(request): Json<FetchManyRequest>,
) -> Result<impl IntoResponse, AppError> {
    if request.ids.is_empty() {
        return Err(AppError::Validation("ids list cannot be empty".to_string()));
    }

    let total_start = Instant::now();
    let cache_set = state.aerospike_set_name(&namespace);
    state.observe_cache_demand(&namespace);
    state
        .metrics
        .observe_fetch_batch_size("get_batch", request.ids.len());
    let warn_vector_dropped = request
        .include_attributes
        .as_deref()
        .is_some_and(|attrs| attrs.iter().any(|a| a == "vector"));
    let trace_id = trace_id_from_headers(&headers);
    let tags = tags_from_headers(&headers).map_err(AppError::Validation)?;

    // 1. Try Aerospike cache for all IDs. On error, treat the whole batch as
    // missing and degrade to turbopuffer rather than 503'ing the caller.
    let cache_start = Instant::now();
    let cache_result = state
        .aerospike
        .get_many(
            &namespace,
            &request.ids,
            request.include_attributes.as_deref(),
        )
        .await;
    let cache_lookup_seconds = cache_start.elapsed().as_secs_f64();
    let (found, cache_errored) = match cache_result {
        Ok(found) => {
            let hits = found.len() as u64;
            let misses = request.ids.len().saturating_sub(found.len()) as u64;
            if hits > 0 {
                state.metrics.observe_cache_lookup(
                    &cache_set,
                    &namespace,
                    CACHE_HIT,
                    hits,
                    cache_lookup_seconds,
                    None,
                );
            }
            if misses > 0 {
                state.metrics.observe_cache_lookup(
                    &cache_set,
                    &namespace,
                    CACHE_MISS,
                    misses,
                    cache_lookup_seconds,
                    None,
                );
                crate::routes::scans::maybe_spawn_reactive_warm(
                    Arc::clone(&state),
                    namespace.clone(),
                )
                .await;
            }
            (found, false)
        }
        Err(e) => {
            state.metrics.observe_cache_lookup(
                &cache_set,
                &namespace,
                CACHE_ERROR,
                request.ids.len() as u64,
                cache_lookup_seconds,
                None,
            );
            warn!(
                namespace = %namespace,
                error = %e,
                "Aerospike batch get failed; falling over to turbopuffer"
            );
            (HashMap::new(), true)
        }
    };

    // 2. Find missing IDs
    let missing_ids: Vec<String> = request
        .ids
        .iter()
        .filter(|id| !found.contains_key(*id))
        .cloned()
        .collect();

    // 3. Fetch missing from Turbopuffer
    let cache_hit_ids: HashSet<String> = found.keys().cloned().collect();
    let mut all_found = found;
    if !missing_ids.is_empty() {
        let upstream = state
            .turbopuffer()
            .fetch_many(&namespace, &missing_ids)
            .await
            .map_err(|e| AppError::Upstream(format!("Turbopuffer fetch failed: {}", e)))?;

        // Backfill Aerospike cache (best-effort). Skip when the earlier cache
        // call already errored — see fetch_document.
        if !upstream.is_empty() && !cache_errored {
            let cache_docs: HashMap<String, HashMap<String, serde_json::Value>> = upstream
                .iter()
                .map(|(id, doc)| (id.clone(), doc.attributes.clone()))
                .collect();

            let backfill_start = Instant::now();
            let backfill = state.aerospike.put_many(&namespace, &cache_docs).await;
            match backfill {
                Ok(()) => {
                    state.metrics.observe_cache_backfill(
                        &cache_set,
                        STATUS_OK,
                        backfill_start.elapsed().as_secs_f64(),
                        None,
                    );
                    for doc in upstream.values() {
                        if let Some(bytes) = estimate_json_bytes(&doc.attributes) {
                            state
                                .metrics
                                .observe_cache_payload(&cache_set, CACHE_MISS, bytes);
                        }
                    }
                }
                Err(e) => {
                    state.metrics.observe_cache_backfill(
                        &cache_set,
                        STATUS_AEROSPIKE_ERROR,
                        backfill_start.elapsed().as_secs_f64(),
                        None,
                    );
                    warn!(
                        namespace = %namespace,
                        error = %e,
                        "Aerospike cache backfill failed for batch"
                    );
                }
            }
        }

        // Merge upstream results, filtering attributes if needed
        for (id, doc) in upstream {
            let attrs = match &request.include_attributes {
                Some(include) => doc
                    .attributes
                    .into_iter()
                    .filter(|(k, _)| include.contains(k))
                    .collect(),
                None => doc.attributes,
            };
            all_found.insert(id, attrs);
        }
    }

    // 4. Build response preserving request order
    let documents: Vec<DocumentResponse> = request
        .ids
        .iter()
        .filter_map(|id| {
            all_found.get(id).map(|attrs| DocumentResponse {
                id: id.clone(),
                attributes: attrs.clone(),
            })
        })
        .collect();

    let missing: Vec<String> = request
        .ids
        .iter()
        .filter(|id| !all_found.contains_key(*id))
        .cloned()
        .collect();

    let header_value = if cache_errored {
        "miss-on-error"
    } else if missing_ids.is_empty() {
        "hit"
    } else {
        "miss"
    };
    let cache_result = if cache_errored {
        CACHE_MISS
    } else if missing_ids.is_empty() {
        CACHE_HIT
    } else if all_found.is_empty() {
        CACHE_MISS
    } else {
        CACHE_PARTIAL
    };

    state.metrics.observe_fetch(
        "get_batch",
        &namespace,
        cache_result,
        total_start.elapsed().as_secs_f64(),
    );
    if let Some(trace_id) = trace_id.as_deref() {
        let events: Vec<ClickstreamEvent> = documents
            .iter()
            .map(|doc| {
                let served_from = if cache_hit_ids.contains(&doc.id) {
                    "cache"
                } else {
                    "upstream"
                };
                clickstream_event(&namespace, &doc.id, trace_id, &tags, "batch", served_from)
            })
            .collect();
        spawn_clickstream_events(&state, events);
    }

    Ok((
        response_headers(header_value, warn_vector_dropped),
        Json(FetchManyResponse { documents, missing }),
    ))
}

fn spawn_clickstream_event(
    state: &Arc<AppState>,
    trace_id: Option<&str>,
    namespace: &str,
    doc_id: &str,
    tags: &[String],
    source: &str,
    served_from: &str,
) {
    let Some(trace_id) = trace_id else {
        return;
    };
    let event = clickstream_event(namespace, doc_id, trace_id, tags, source, served_from);
    spawn_clickstream_events(state, vec![event]);
}

fn spawn_clickstream_events(state: &Arc<AppState>, events: Vec<ClickstreamEvent>) {
    if events.is_empty() {
        return;
    }
    let s3 = Arc::clone(&state.s3);
    tokio::spawn(async move {
        log_clickstream_events(s3, events).await;
    });
}

fn clickstream_event(
    namespace: &str,
    doc_id: &str,
    trace_id: &str,
    tags: &[String],
    source: &str,
    served_from: &str,
) -> ClickstreamEvent {
    let (timestamp, timestamp_nanos) = now_timestamp();
    ClickstreamEvent {
        timestamp,
        timestamp_nanos,
        trace_id: trace_id.to_string(),
        namespace: namespace.to_string(),
        doc_id: doc_id.to_string(),
        tags: tags.to_vec(),
        source: source.to_string(),
        served_from: served_from.to_string(),
    }
}
