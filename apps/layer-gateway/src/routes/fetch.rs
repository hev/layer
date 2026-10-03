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

/// True when a cached row can answer a fetch that names `include`. A cached
/// row may predate generated columns (for example a hosted `embed_text`
/// embedding), so a row that lacks any requested attribute must not be served:
/// the caller would see a silently empty value. `vector` is exempt because it
/// is never returned from this route and is reported through the warning header.
/// With no explicit list there is nothing to check.
fn cache_covers(attrs: &HashMap<String, serde_json::Value>, include: Option<&[String]>) -> bool {
    include.is_none_or(|wanted| {
        wanted
            .iter()
            .all(|a| a == "vector" || a == "id" || attrs.contains_key(a))
    })
}

/// GET /v2/namespaces/{namespace}/documents/{doc_id}
///
/// Single document fetch with pull-through caching:
/// 1. Try Aerospike — return on hit, but only when the cached row holds every
///    explicitly requested attribute; otherwise treat it as a miss and refresh
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
        Ok(Some(attrs)) if cache_covers(&attrs, include_attrs.as_deref()) => {
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
        Ok(_) => {
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
        .fetch_with_attributes(&namespace, &doc_id, include_attrs.as_deref().unwrap_or(&[]))
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
        Ok(mut found) => {
            found.retain(|_, attrs| cache_covers(attrs, request.include_attributes.as_deref()));
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
            .fetch_many_with_attributes(
                &namespace,
                &missing_ids,
                request.include_attributes.as_deref().unwrap_or(&[]),
            )
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn row(keys: &[&str]) -> HashMap<String, serde_json::Value> {
        keys.iter().map(|k| (k.to_string(), json!(1))).collect()
    }

    /// LYR-238: a write-through cache row written before hosted embedding ran
    /// has no `embed_text`. It must not satisfy a fetch that names it, and a
    /// refresh from origin must.
    #[tokio::test]
    async fn pre_embedding_cache_row_does_not_satisfy_embed_text_fetch() {
        use crate::clients::aerospike::{AerospikeClient, MockAerospikeClient};

        let cache = MockAerospikeClient::default();
        let mut pre: HashMap<String, serde_json::Value> = HashMap::new();
        pre.insert("title".into(), json!("t"));
        cache.put("ns", "d1", &pre).await.unwrap();
        let want = vec!["embed_text".to_string()];

        let warm = cache.get("ns", "d1", Some(&want)).await.unwrap().unwrap();
        assert!(!cache_covers(&warm, Some(&want)), "stale row served");

        let mut origin = pre.clone();
        origin.insert("embed_text".into(), json!([0.1, 0.2]));
        cache.put("ns", "d1", &origin).await.unwrap();
        let refreshed = cache.get("ns", "d1", Some(&want)).await.unwrap().unwrap();
        assert!(cache_covers(&refreshed, Some(&want)));
        assert_eq!(refreshed["embed_text"], json!([0.1, 0.2]));

        let cold = cache.get("ns", "missing", Some(&want)).await.unwrap();
        assert!(cold.is_none());
    }

    // ---- LYR-238 handler-level regression -------------------------------
    //
    // Drives the real `fetch_document` / `fetch_many_documents` handlers over
    // an in-memory origin (`MockTurbopufferClient`) and cache
    // (`MockAerospikeClient`). The hosted embedder is mocked: the origin row
    // carries the `embed_text` vector the store would have generated, and the
    // cache is seeded with the row a write-through cache held *before* that
    // embedding landed. The open gateway binary runs with the document cache
    // disabled, so this is the only place the cache path is exercised here.

    use crate::clients::aerospike::{AerospikeClient, MockAerospikeClient};
    use crate::clients::turbopuffer::{MockTurbopufferClient, TurbopufferClient, UpsertDoc};
    use axum::body::to_bytes;
    use axum::response::IntoResponse;

    const NS: &str = "hosted";

    /// Deterministic stand-in for the hosted embedder.
    fn mock_embed(text: &str) -> Vec<f64> {
        vec![
            text.len() as f64,
            text.bytes().map(f64::from).sum::<f64>() / 1000.0,
        ]
    }

    fn test_state(
        origin: Arc<MockTurbopufferClient>,
        cache: Arc<MockAerospikeClient>,
    ) -> Arc<AppState> {
        use crate::consistency::ConsistencyWatcher;
        use crate::cost::AwsCostConfig;
        use crate::metrics::LayerMetrics;
        use crate::telemetry::TelemetryCounters;
        use std::sync::atomic::AtomicBool;

        let config = crate::config::Config::from_env();
        let origin: Arc<dyn TurbopufferClient> = origin;
        Arc::new(AppState {
            namespace_purges: Arc::new(Default::default()),
            draining: Arc::new(AtomicBool::new(false)),
            drain_marker_path: config.drain_marker_path.clone(),
            metrics: Arc::new(LayerMetrics::new()),
            telemetry: Arc::new(TelemetryCounters::default()),
            turbopuffer: Some(origin),
            embedding_provider: None,
            http_embedding_provider: None,
            lattice_embedding_provider: None,
            local_clip_embedding_provider: None,
            embedding_cache: Default::default(),
            embedding_cache_ttl: std::time::Duration::from_millis(config.embedding_cache_ttl_ms),
            wire_embedding_profiles: Default::default(),
            aerospike: cache,
            aerospike_runtime: Arc::new(crate::clients::aerospike::AerospikeRuntime::new(None)),
            s3: Arc::new(crate::clients::s3::NoopS3Client),
            index_deleter: None,
            jobs: Default::default(),
            restore_runs: Default::default(),
            aerospike_set_prefix: config.aerospike_set_prefix.clone(),
            pipeline_store: None,
            udf_store: None,
            write_trigger: None,
            metrics_backend_url: None,
            aws_cost_config: AwsCostConfig {
                enabled: false,
                region: config.aws_cost_region.clone(),
                tag_key: config.aws_cost_tag_key.clone(),
                tag_value: config.aws_cost_tag_value.clone(),
                site: config.aws_cost_site.clone(),
                cache_ttl_seconds: config.aws_cost_cache_ttl_seconds,
            },
            pipeline_status_cache: Default::default(),
            pipeline_status_cache_ttl: std::time::Duration::from_millis(1),
            pipeline_status_inflight: Default::default(),
            udf_status_cache: Default::default(),
            udf_status_inflight: Default::default(),
            consistency: Arc::new(ConsistencyWatcher::new()),
            cache_warmed_through: Default::default(),
            cache_namespaces: Default::default(),
            warm_inflight: Default::default(),
            reactive_warm_generations: Default::default(),
            facet_fields: Default::default(),
            scan_threads: Default::default(),
            snapshot_min_interval_ms: config.snapshot_min_interval_ms,
            snapshot_interval_ms: Default::default(),
            snapshot_retention: Default::default(),
            blob_reference_attributes: Default::default(),
            blob_cache_enabled: false,
            managed_platform_enabled: false,
            namespace_store_refs: Default::default(),
            embedding_profiles: Default::default(),
            last_snapshot_at: Default::default(),
            snapshot_inflight: Default::default(),
            inbound_auth: crate::auth::InboundAuth::Open,
            minted_key_verifier: None,
            key_store: None,
            keys_namespace: config.keys_namespace.clone(),
            vector_store_namespace: config.vector_store_namespace.clone(),
            turbopuffer_dashboard_base_url: String::new(),
            default_store: "default".to_string(),
            resolved_vectorstores: Default::default(),
            shard_count: config.shard_count,
            federated_query_max_namespaces: config.federated_query_max_namespaces,
            federated_query_namespace_threads: config.federated_query_namespace_threads,
            pinned_federated_query_namespace_threads: config
                .pinned_federated_query_namespace_threads,
            sharded_namespaces: Default::default(),
            init_tasks: Default::default(),
            init_backfill_batch_size: config.init_backfill_batch_size,
            init_backfill_rps: config.init_backfill_rps,
            namespace_list_cache: Default::default(),
            namespace_list_cache_ttl: std::time::Duration::from_millis(1),
            agents: crate::agent::registry_from_json(None).unwrap(),
            agentic_enabled: false,
            agent_provider: Arc::new(crate::agent::DisabledAgentProvider),
            search_kind_stores: Default::default(),
        })
    }

    /// Origin holds `d1`/`d2` with a generated `embed_text`; the cache holds
    /// the same docs as written before embedding ran (no `embed_text`) when
    /// `warm` is set, and nothing when cold.
    async fn fixture(warm: bool) -> (Arc<AppState>, Arc<MockTurbopufferClient>) {
        let origin = Arc::new(MockTurbopufferClient::new());
        let cache = Arc::new(MockAerospikeClient::new());
        let mut docs = Vec::new();
        for (id, body) in [("d1", "alpha"), ("d2", "bravo bravo")] {
            let mut attributes: HashMap<String, serde_json::Value> = HashMap::new();
            attributes.insert("body".into(), json!(body));
            if warm {
                cache.put(NS, id, &attributes).await.unwrap();
            }
            attributes.insert("embed_text".into(), json!(mock_embed(body)));
            docs.push(UpsertDoc {
                id: id.into(),
                vector: None,
                vectors: None,
                attributes,
            });
        }
        origin.upsert(NS, &docs).await.unwrap();
        (test_state(Arc::clone(&origin), cache), origin)
    }

    async fn origin_embed_text(origin: &MockTurbopufferClient, id: &str) -> serde_json::Value {
        let outcome = origin
            .ranked_query(
                NS,
                &json!(["id", "asc"]),
                10,
                Some(&json!(["id", "Eq", id])),
                Some(&vectorstore_core::models::IncludeAttributes::Fields(vec![
                    "embed_text".into(),
                ])),
            )
            .await
            .unwrap();
        outcome.rows[0].attributes["embed_text"].clone()
    }

    async fn body_json(resp: axum::response::Response) -> (HeaderMap, serde_json::Value) {
        let (parts, body) = resp.into_parts();
        let bytes = to_bytes(body, usize::MAX).await.unwrap();
        (parts.headers, serde_json::from_slice(&bytes).unwrap())
    }

    async fn point_fetch(state: &Arc<AppState>, id: &str) -> (HeaderMap, serde_json::Value) {
        let resp = fetch_document(
            State(Arc::clone(state)),
            Path((NS.to_string(), id.to_string())),
            Query(FetchQueryParams {
                include_attributes: Some("embed_text".into()),
            }),
            HeaderMap::new(),
        )
        .await
        .unwrap()
        .into_response();
        body_json(resp).await
    }

    #[tokio::test]
    async fn point_fetch_embed_text_matches_origin_warm_and_cold() {
        for warm in [true, false] {
            let (state, origin) = fixture(warm).await;
            for id in ["d1", "d2"] {
                let (headers, body) = point_fetch(&state, id).await;
                assert_eq!(
                    body["attributes"]["embed_text"],
                    origin_embed_text(&origin, id).await,
                    "warm={warm} id={id}"
                );
                assert_ne!(headers[LAYER_CACHE_HEADER], "hit", "stale row served");
                // The refreshed row now satisfies the same fetch from cache.
                let (headers, body) = point_fetch(&state, id).await;
                assert_eq!(headers[LAYER_CACHE_HEADER], "hit");
                assert_eq!(
                    body["attributes"]["embed_text"],
                    origin_embed_text(&origin, id).await
                );
            }
        }
    }

    #[tokio::test]
    async fn batch_fetch_embed_text_matches_origin_warm_and_cold() {
        for warm in [true, false] {
            let (state, origin) = fixture(warm).await;
            let resp = fetch_many_documents(
                State(Arc::clone(&state)),
                Path(NS.to_string()),
                HeaderMap::new(),
                Json(FetchManyRequest {
                    ids: vec!["d1".into(), "d2".into()],
                    include_attributes: Some(vec!["embed_text".into()]),
                }),
            )
            .await
            .unwrap()
            .into_response();
            let (headers, body) = body_json(resp).await;
            assert_ne!(headers[LAYER_CACHE_HEADER], "hit", "stale rows served");
            for id in ["d1", "d2"] {
                let doc = body["documents"]
                    .as_array()
                    .and_then(|docs| docs.iter().find(|d| d["id"] == id))
                    .unwrap_or_else(|| panic!("{id} missing: {body}"));
                assert_eq!(
                    doc["attributes"]["embed_text"],
                    origin_embed_text(&origin, id).await,
                    "warm={warm} id={id}"
                );
            }
        }
    }

    #[test]
    fn cache_covers_requires_every_requested_attribute() {
        let want = vec!["title".to_string(), "embed_text".to_string()];
        assert!(!cache_covers(&row(&["title"]), Some(&want)));
        assert!(cache_covers(&row(&["title", "embed_text"]), Some(&want)));
        assert!(cache_covers(&row(&["title"]), None));
        let vec_only = vec!["vector".to_string()];
        assert!(cache_covers(&row(&[]), Some(&vec_only)));
    }
}
