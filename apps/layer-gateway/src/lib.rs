pub mod agent;
pub mod auth;
pub mod clients;
pub mod config;
pub mod consistency;
pub mod cost;
pub mod embedding;
pub mod error;
pub mod history;
pub mod index_config;
pub mod index_gc;
pub mod keys;
pub mod metrics;
pub mod models;
pub mod pipeline;
#[cfg(feature = "pro")]
pub mod pipeline_segments;
pub mod routes;
pub mod server;
pub mod shards;
pub mod snapshot_policy;
pub mod snapshots;
pub mod telemetry;
pub mod udf;
pub mod vector_store;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
#[allow(unused_imports)]
use axum::routing::{delete, get, post, put};
use axum::Router;
use dashmap::DashMap;
use serde_json::Value;
use tower_http::trace::TraceLayer;

use agent::{AgentInferenceProvider, AgentRegistry};
use auth::InboundAuth;
use clients::aerospike::{AerospikeClient, AerospikeRuntime};
use clients::s3::S3Client;
use clients::turbopuffer::TurbopufferClient;
use consistency::ConsistencyWatcher;
use index_config::{EmbeddingProfile, Retention};
use index_gc::IndexDeleter;
use metrics::LayerMetrics;
use pipeline::PipelineStore;
use routes::scans::JobState;
use telemetry::TelemetryCounters;
use udf::UdfStore;
use vector_store::ResolvedVectorStore;

pub const DEFAULT_SCAN_THREADS: u32 = 8;
pub const SCAN_THREADS_MAX: u32 = 32;

#[derive(Debug, Clone)]
pub struct RestoreRunState;
pub struct AppState {
    /// Set by SIGTERM handling or by the Kubernetes preStop drain marker before
    /// the pod is removed from Service/ALB endpoints.
    pub draining: Arc<AtomicBool>,
    pub drain_marker_path: PathBuf,
    pub metrics: Arc<LayerMetrics>,
    pub telemetry: Arc<TelemetryCounters>,
    pub turbopuffer: Option<Arc<dyn TurbopufferClient>>,
    pub embedding_provider: Option<Arc<dyn embedding::EmbeddingProvider>>,
    pub embedding_cache: Arc<embedding::EmbeddingCache>,
    pub embedding_cache_ttl: std::time::Duration,
    pub wire_embedding_profiles:
        Arc<DashMap<String, Vec<crate::routes::embed_wire::EmbeddingProfile>>>,
    pub aerospike: Arc<dyn AerospikeClient>,
    pub aerospike_runtime: Arc<AerospikeRuntime>,
    pub s3: Arc<dyn S3Client>,
    pub index_deleter: Option<Arc<dyn IndexDeleter>>,
    pub jobs: Arc<DashMap<String, JobState>>,
    pub restore_runs: Arc<DashMap<String, RestoreRunState>>,
    pub aerospike_set_prefix: String,
    pub pipeline_store: Option<Arc<dyn PipelineStore>>,
    pub udf_store: Option<Arc<dyn UdfStore>>,
    pub write_trigger: Option<Arc<dyn WriteTrigger>>,
    pub metrics_backend_url: Option<String>,
    pub aws_cost_config: cost::AwsCostConfig,
    /// Short-TTL cache for pipeline status responses. Protects PostgreSQL
    /// from repeated full-table counts on large pipelines when a dashboard
    /// or KEDA poller hammers the endpoint.
    pub pipeline_status_cache:
        Arc<DashMap<String, (std::time::Instant, crate::pipeline::PipelineStatus)>>,
    /// TTL for `pipeline_status_cache` entries.
    pub pipeline_status_cache_ttl: std::time::Duration,
    /// Per-pipeline singleflight guard so concurrent status pollers share a
    /// single DB round-trip instead of fanning out into duplicate status reads.
    pub pipeline_status_inflight: Arc<DashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    pub udf_status_cache: Arc<DashMap<String, (std::time::Instant, crate::udf::UdfStatus)>>,
    pub udf_status_inflight: Arc<DashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    pub consistency: Arc<ConsistencyWatcher>,
    /// Per-namespace watermark observed at the end of the last completed full
    /// origin scan. Used as the `_hevlayer_upserted_at` cut for cache scans so they
    /// return a stable snapshot, and to decide whether auto-mode should
    /// schedule a background warm.
    pub cache_warmed_through: Arc<DashMap<String, u64>>,
    /// Logical namespaces that have seen cache-path demand. This is the
    /// bounded namespace set surfaced in /health and cache-state metrics.
    pub cache_namespaces: Arc<DashMap<String, ()>>,
    /// Single-flight guard for auto-mode background warms. An entry's
    /// presence means a warm is currently running for that namespace.
    pub warm_inflight: Arc<DashMap<String, ()>>,
    /// Last Aerospike connection generation that triggered a reactive warm
    /// for each namespace. A reconnect increments the generation, so the next
    /// cache miss per namespace schedules one warm.
    pub reactive_warm_generations: Arc<DashMap<String, u64>>,
    /// Per-namespace facet fields to histogram into a snapshot when the
    /// watermark advances. Replaced atomically after successful Index CR
    /// refreshes.
    pub facet_fields: Arc<RwLock<HashMap<String, Vec<String>>>>,
    /// Per-namespace default origin scan fan-out width loaded from
    /// `Index.spec.scan.threads`. Absent namespace falls back to
    /// `DEFAULT_SCAN_THREADS`; values are clamped before storage.
    pub scan_threads: Arc<RwLock<HashMap<String, u32>>>,
    /// Minimum interval between snapshots per namespace. Cloned from
    /// `Config::snapshot_min_interval_ms`.
    pub snapshot_min_interval_ms: u64,
    /// Per-namespace snapshot interval floors loaded from
    /// `Index.spec.snapshot.interval`. Absent namespace falls back to
    /// `snapshot_min_interval_ms`.
    pub snapshot_interval_ms: Arc<RwLock<HashMap<String, u64>>>,
    /// Per-namespace snapshot retention policy loaded from
    /// `Index.spec.snapshot.retention`. Absent namespace means `Never`.
    pub snapshot_retention: Arc<RwLock<HashMap<String, Retention>>>,
    /// Per-namespace blob reference attribute declarations loaded from
    /// `Index.spec.blobs.referenceAttributes`. Absent namespace means blob
    /// cache warm is not enabled for that namespace.
    pub blob_reference_attributes: Arc<RwLock<HashMap<String, Vec<String>>>>,
    /// Whether blob PUT/GET routes are mounted in this gateway composition.
    ///
    /// Blobs are a pro managed-cache surface. The implementation can fall back
    /// to S3 during cache outages, but standalone/open mode omits the surface.
    pub blob_store_enabled: bool,
    /// Whether managed-platform routes are mounted in this gateway composition.
    ///
    /// These surfaces are Pro in the open-core split: retained history/activity,
    /// checkpoint/restore, cost/fin-ops, and shard-migration lifecycle.
    pub managed_platform_enabled: bool,
    /// Per-namespace VectorStore reference loaded from `Index.spec.backend.storeRef`.
    /// Namespaces absent from this map use the default VectorStore.
    pub namespace_store_refs: Arc<RwLock<HashMap<String, String>>>,
    /// Per-namespace embedding profile loaded from `Index.spec.embedding`
    /// plus `Index.spec.backend.distanceMetric`.
    pub embedding_profiles: Arc<RwLock<HashMap<String, EmbeddingProfile>>>,
    /// Wall-clock (epoch ms) of the last snapshot attempt per namespace.
    /// Bumped on both successful writes and dedup'd skips so a steady-state
    /// namespace doesn't re-scan every poll.
    pub last_snapshot_at: Arc<DashMap<String, u64>>,
    /// Single-flight guard for the snapshot writer. Presence means a
    /// snapshot job is currently running for that namespace.
    pub snapshot_inflight: Arc<DashMap<String, ()>>,
    /// Inbound auth policy resolved from the default VectorStore. Open mode
    /// is explicit; otherwise the middleware accepts one or more bearer keys.
    pub inbound_auth: InboundAuth,
    /// Optional minted-key verifier for request authentication. Open builds
    /// can leave this unset while pro composition installs an implementation.
    pub minted_key_verifier: Option<Arc<dyn auth::MintedKeyVerifier>>,
    /// Watch-fed verify cache for minted `ApiKey` tokens and key management
    /// routes. Absent when the key store is not configured.
    pub key_store: Option<Arc<keys::KeyStore>>,
    /// Kubernetes client for the `/v2/keys` management routes and
    /// `lastSeenAt` bumps. Absent only in tests.
    /// Namespace where `ApiKey` resources live.
    pub keys_namespace: String,
    /// Namespace where VectorStore and Warehouse resources live.
    pub vector_store_namespace: String,
    /// Base URL for Turbopuffer dashboard deep links.
    pub turbopuffer_dashboard_base_url: String,
    /// Name of the default VectorStore; routes whose namespace has no
    /// `Index.spec.backend.storeRef` entry resolve here.
    pub default_store: String,
    /// VectorStores resolved at startup. In standalone mode this is the
    /// read-only source for `/v2/vectorstores`.
    pub resolved_vectorstores: Arc<HashMap<String, ResolvedVectorStore>>,
    /// Configured logical shard count used for `_hevlayer_shard` stamping.
    pub shard_count: u64,
    /// Maximum namespaces accepted by one federated `/v2/query`.
    pub federated_query_max_namespaces: usize,
    /// Maximum namespace legs run concurrently by one federated `/v2/query`.
    pub federated_query_namespace_threads: usize,
    /// Namespaces known to have completed shard migration. Query and scan
    /// fan-out only uses `_hevlayer_shard` after this map is populated from
    /// the durable S3 shard manifest.
    pub sharded_namespaces: Arc<DashMap<String, u64>>,
    /// Per-namespace single-flight guards for embedded namespace init drains.
    pub init_tasks: Arc<DashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Maximum rows patched per embedded namespace init request.
    pub init_backfill_batch_size: u32,
    /// Maximum embedded namespace init scan/patch batches per second.
    pub init_backfill_rps: u32,
    /// Short-TTL cache for `GET /v2/namespaces` responses. Keyed by the
    /// upstream query string (prefix/cursor/page_size). Absorbs dashboard
    /// polling pressure so the gateway does not fan out a per-namespace
    /// metadata call on every refresh.
    pub namespace_list_cache:
        Arc<DashMap<String, (std::time::Instant, crate::models::NamespaceList)>>,
    /// TTL for `namespace_list_cache` entries.
    pub namespace_list_cache_ttl: std::time::Duration,
    /// Resolved Agent resources keyed by name. The request path reads this
    /// in-memory map instead of calling the Kubernetes API.
    pub agents: Arc<AgentRegistry>,
    pub agent_provider: Arc<dyn AgentInferenceProvider>,
    /// Names of VectorStores resolved as `kind: search`. Store kinds are
    /// fixed at startup resolution; the request path consults this to apply
    /// backend-specific behavior (the interim fuzziness clamp).
    pub search_kind_stores: std::collections::HashSet<String>,}

#[async_trait]
pub trait WriteTrigger: Send + Sync {
    async fn enqueue_write_rows(
        &self,
        state: Arc<AppState>,
        namespace: &str,
        rows: Vec<HashMap<String, Value>>,
        partial_rows: bool,
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheState {
    Cold,
    Warming,
    Warm,
}

impl CacheState {
    pub fn as_str(self) -> &'static str {
        match self {
            CacheState::Cold => "cold",
            CacheState::Warming => "warming",
            CacheState::Warm => "warm",
        }
    }
}

impl AppState {
    pub fn start_draining(&self) {
        self.draining.store(true, Ordering::SeqCst);
    }

    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst) || self.drain_marker_path.exists()
    }

    pub fn turbopuffer(&self) -> &dyn TurbopufferClient {
        self.turbopuffer
            .as_ref()
            .expect("Turbopuffer client not configured (default VectorStore unresolved)")
            .as_ref()
    }

    pub fn facet_fields_for(&self, namespace: &str) -> Option<Vec<String>> {
        self.facet_fields
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(namespace)
            .cloned()
            .filter(|fields| !fields.is_empty())
    }

    pub fn has_facet_field(&self, namespace: &str, field: &str) -> bool {
        self.facet_fields_for(namespace)
            .map(|fields| fields.iter().any(|candidate| candidate == field))
            .unwrap_or(false)
    }

    pub fn replace_facet_fields(&self, facet_fields: HashMap<String, Vec<String>>) {
        *self
            .facet_fields
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = facet_fields;
    }

    pub fn scan_threads_for(&self, namespace: &str) -> u32 {
        self.scan_threads
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(namespace)
            .copied()
            .unwrap_or(DEFAULT_SCAN_THREADS)
            .clamp(1, SCAN_THREADS_MAX)
    }

    pub fn replace_scan_threads(&self, scan_threads: HashMap<String, u32>) {
        *self
            .scan_threads
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = scan_threads;
    }

    pub fn snapshot_interval_ms_for(&self, namespace: &str) -> Option<u64> {
        self.snapshot_interval_ms
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(namespace)
            .copied()
    }

    pub fn replace_snapshot_interval_ms(&self, snapshot_interval_ms: HashMap<String, u64>) {
        *self
            .snapshot_interval_ms
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = snapshot_interval_ms;
    }

    pub fn snapshot_retention_for(&self, namespace: &str) -> Retention {
        self.snapshot_retention
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(namespace)
            .cloned()
            .unwrap_or(Retention::Never)
    }

    pub fn replace_snapshot_retention(&self, snapshot_retention: HashMap<String, Retention>) {
        *self
            .snapshot_retention
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = snapshot_retention;
    }

    pub fn blob_reference_attributes_for(&self, namespace: &str) -> Option<Vec<String>> {
        self.blob_reference_attributes
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(namespace)
            .cloned()
            .filter(|fields| !fields.is_empty())
    }

    pub fn replace_blob_reference_attributes(
        &self,
        blob_reference_attributes: HashMap<String, Vec<String>>,
    ) {
        *self
            .blob_reference_attributes
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = blob_reference_attributes;
    }

    pub fn replace_namespace_store_refs(&self, namespace_store_refs: HashMap<String, String>) {
        *self
            .namespace_store_refs
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = namespace_store_refs;
    }

    pub fn embedding_profile_for(&self, namespace: &str) -> Option<EmbeddingProfile> {
        self.embedding_profiles
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(namespace)
            .cloned()
    }

    pub fn replace_embedding_profiles(
        &self,
        embedding_profiles: HashMap<String, EmbeddingProfile>,
    ) {
        *self
            .embedding_profiles
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = embedding_profiles;
    }

    /// The VectorStore a namespace's traffic resolves to — its Index's
    /// `storeRef`, or the default store.
    pub fn store_for_namespace(&self, namespace: &str) -> String {
        let store = self
            .namespace_store_refs
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(namespace)
            .cloned()
            .unwrap_or_else(|| self.default_store.clone());
        if store != self.default_store {
            self.telemetry.touch_multi_store_routing();
        }
        store
    }

    /// Whether a namespace's traffic resolves to a `kind: search` VectorStore.
    pub fn namespace_uses_search_store(&self, namespace: &str) -> bool {
        self.search_kind_stores
            .contains(&self.store_for_namespace(namespace))
    }

    pub fn snapshot_retention_namespaces(&self) -> Vec<(String, Retention)> {
        self.snapshot_retention
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .map(|(namespace, retention)| (namespace.clone(), retention.clone()))
            .collect()
    }

    pub fn facet_field_namespaces(&self) -> Vec<String> {
        self.facet_fields
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter_map(|(namespace, fields)| {
                if fields.is_empty() {
                    None
                } else {
                    Some(namespace.clone())
                }
            })
            .collect()
    }

    pub fn pipeline_store(&self) -> &dyn PipelineStore {
        self.pipeline_store
            .as_ref()
            .expect("Pipeline store not configured (DATABASE_URL not set)")
            .as_ref()
    }

    pub fn udf_store(&self) -> &dyn UdfStore {
        self.udf_store
            .as_ref()
            .expect("UDF store not configured (DATABASE_URL not set)")
            .as_ref()
    }

    pub fn aerospike_set_name(&self, logical_namespace: &str) -> String {
        format!("{}{}", self.aerospike_set_prefix, logical_namespace)
    }

    pub fn observe_cache_demand(&self, namespace: &str) {
        if namespace.trim().is_empty() {
            return;
        }
        self.cache_namespaces.insert(namespace.to_string(), ());
        self.metrics.observe_cache_demand(namespace);
        if !self.aerospike_runtime.is_connected_now() {
            self.metrics.maybe_start_document_cache_cold_start();
        }
    }

    pub fn observe_cache_cold_response(&self, namespace: &str) {
        if namespace.trim().is_empty() {
            return;
        }
        self.cache_namespaces.insert(namespace.to_string(), ());
        self.metrics.observe_cache_cold_response(namespace);
        self.metrics
            .set_cache_state(namespace, CacheState::Cold.as_str());
    }

    pub async fn cache_available(&self) -> bool {
        self.aerospike_runtime.is_connected().await
    }

    pub async fn cache_state_for_namespace(&self, namespace: &str) -> CacheState {
        let state = if self.warm_inflight.contains_key(namespace) {
            CacheState::Warming
        } else if !self.aerospike_runtime.is_connected().await {
            CacheState::Cold
        } else if self.cache_warmed_through.contains_key(namespace) {
            CacheState::Warm
        } else {
            CacheState::Cold
        };
        self.metrics.set_cache_state(namespace, state.as_str());
        state
    }

    pub fn cache_namespace_list(&self) -> Vec<String> {
        let mut namespaces: Vec<String> = self
            .cache_namespaces
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        for entry in self.cache_warmed_through.iter() {
            if !namespaces.iter().any(|ns| ns == entry.key()) {
                namespaces.push(entry.key().clone());
            }
        }
        for entry in self.warm_inflight.iter() {
            if !namespaces.iter().any(|ns| ns == entry.key()) {
                namespaces.push(entry.key().clone());
            }
        }
        namespaces.sort();
        namespaces
    }
}

async fn reject_while_draining(
    State(state): State<Arc<AppState>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if state.is_draining() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [("connection", "close")],
            axum::Json(serde_json::json!({
                "error": "gateway_draining",
                "message": "gateway pod is draining; retry on another endpoint"
            })),
        )
            .into_response();
    }

    next.run(request).await
}

pub fn build_router(state: Arc<AppState>) -> Router {
    let mut public = Router::new()
        .route("/health", get(routes::health::health))
        .route("/ready", get(routes::health::ready))
        .route("/metrics", get(routes::metrics::prometheus_metrics))
        .route(
            "/v2/metrics/catalog",
            get(routes::metrics_catalog::list_catalog),
        )
        .route(
            "/v2/metrics/catalog/{name}",
            get(routes::metrics_catalog::get_catalog_entry),
        );

    #[cfg(feature = "pro")]
    if state.key_store.is_some() {
        // Unauthenticated by construction — the token is the credential.
        public = public.route(
            "/v2/keys/authenticate",
            post(routes::keys::authenticate_key),
        );
    } else {
        public = public.route(
            "/v2/keys/authenticate",
            post(|| async { StatusCode::NOT_FOUND }),
        );
    }
    #[cfg(not(feature = "pro"))]
    {
        public = public.route(
            "/v2/keys/authenticate",
            post(|| async { StatusCode::NOT_FOUND }),
        );
    }

    let router = Router::new()
        .route(
            "/v2/metrics/api/v1/query",
            get(routes::metrics::prometheus_proxy_query)
                .post(routes::metrics::prometheus_proxy_query),
        )
        .route(
            "/v2/metrics/query",
            get(routes::metrics::prometheus_proxy_query)
                .post(routes::metrics::prometheus_proxy_query),
        )
        .route(
            "/v2/metrics/api/v1/query_range",
            get(routes::metrics::prometheus_proxy_query_range)
                .post(routes::metrics::prometheus_proxy_query_range),
        )
        .route(
            "/v2/metrics/api/v1/import/prometheus",
            post(routes::metrics::prometheus_proxy_import),
        )
        .route(
            "/v2/metrics/query_range",
            get(routes::metrics::prometheus_proxy_query_range)
                .post(routes::metrics::prometheus_proxy_query_range),
        );

    #[cfg(feature = "pro")]
    let mut router = router.route("/v2/license", get(routes::license::get_license));
    #[cfg(not(feature = "pro"))]
    #[allow(unused_mut)]
    let mut router = router.route("/v2/license", get(open_gateway_license));

    #[cfg(feature = "pro")]
    if state.pipeline_store.is_some() {
        router = router
            .route(
                "/v2/pipelines",
                post(routes::pipeline::create_pipeline).get(routes::pipeline::list_pipelines),
            )
            .route(
                "/v2/pipelines/{id}",
                delete(routes::pipeline::delete_pipeline),
            )
            .route(
                "/v2/pipelines/{id}/status",
                get(routes::pipeline::get_pipeline_status),
            )
            .route(
                "/v2/pipelines/{id}/claim",
                post(routes::pipeline::claim_documents),
            )
            .route(
                "/v2/pipelines/{id}/documents/heartbeat",
                post(routes::pipeline::heartbeat_documents),
            )
            .route(
                "/v2/pipelines/{id}/documents/stage",
                post(routes::pipeline::set_documents_stage),
            )
            .route(
                "/v2/pipelines/{id}/documents/{doc_id}",
                put(routes::pipeline::stage_document),
            )
            .route(
                "/v2/pipelines/{id}/documents/{doc_id}/chunks",
                get(routes::pipeline::get_chunks),
            )
            .route(
                "/v2/pipelines/{id}/documents/{doc_id}/vectors",
                put(routes::pipeline::write_vectors),
            );
    }

    #[cfg(feature = "pro")]
    if state.udf_store.is_some() {
        router = router
            .route(
                "/v2/udfs",
                post(routes::udf::create_udf).get(routes::udf::list_udfs),
            )
            .route(
                "/v2/udfs/{id}",
                get(routes::udf::get_udf)
                    .put(routes::udf::upsert_udf)
                    .delete(routes::udf::delete_udf),
            )
            .route("/v2/udfs/{id}/status", get(routes::udf::get_udf_status))
            .route("/v2/udfs/{id}/pause", post(routes::udf::pause_udf))
            .route("/v2/udfs/{id}/resume", post(routes::udf::resume_udf))
            .route(
                "/v2/udfs/{id}/reset-failed",
                post(routes::udf::reset_failed_udf),
            )
            .route("/v2/udfs/{id}/discover", post(routes::udf::discover_udf))
            .route("/v2/udfs/{id}/claim", post(routes::udf::claim_udf_items))
            .route(
                "/v2/udfs/{id}/items/heartbeat",
                post(routes::udf::heartbeat_udf_items),
            )
            .route(
                "/v2/udfs/{id}/items/complete",
                post(routes::udf::complete_udf_items),
            )
            .route(
                "/v2/udfs/{id}/items/fail",
                post(routes::udf::fail_udf_items),
            );
    }

    #[cfg(feature = "pro")]
    if !state.agents.is_empty() {
        router = router.route("/v2/agents/{name}/query", post(routes::agents::query_agent));
    }

    #[cfg(feature = "pro")]
    if state.key_store.is_some() && state.kube.is_some() {
        router = router
            .route(
                "/v2/keys",
                post(routes::keys::mint_key).get(routes::keys::list_keys),
            )
            .route(
                "/v2/keys/{key_id}",
                get(routes::keys::get_key).delete(routes::keys::delete_key),
            )
            .route("/v2/keys/{key_id}/revoke", post(routes::keys::revoke_key));
    }

    #[cfg(feature = "pro")]
    if state.kube.is_some() {
        router = router
            .route("/v2/warehouses", get(routes::warehouses::list_warehouses))
            .route(
                "/v2/warehouses/{name}",
                get(routes::warehouses::get_warehouse),
            );
    }

    #[cfg(any())]
    if state.blob_store_enabled {
        router = router
            .route(
                "/v1/namespaces/{namespace}/blobs",
                put(routes::blobs::put_blob),
            )
            .route(
                "/v1/namespaces/{namespace}/blobs/{sha256}",
                get(routes::blobs::get_blob),
            );
    }

    #[cfg(any())]
    if state.managed_platform_enabled {
        router = router
            .route(
                "/v1/control/restores",
                post(routes::restore::create_restore).get(routes::restore::list_restores),
            )
            .route(
                "/v1/control/restores/{restore_id}",
                get(routes::restore::get_restore),
            )
            .route(
                "/v1/control/restores/{restore_id}/verify",
                post(routes::restore::verify_restore),
            )
            .route(
                "/v1/namespaces/{namespace}/search-history",
                get(routes::history::list_search_history_route),
            )
            .route(
                "/v1/namespaces/{namespace}/clickstream",
                get(routes::history::list_clickstream_route),
            )
            .route(
                "/v2/namespaces/{namespace}/search-history",
                get(routes::history::list_search_history_route),
            )
            .route(
                "/v2/namespaces/{namespace}/clickstream",
                get(routes::history::list_clickstream_route),
            )
            .route(
                "/v2/namespaces/{namespace}/shard/migrate",
                post(routes::shards::migrate_namespace_shards),
            )
            .route(
                "/v2/namespaces/{namespace}/checkpoints",
                post(routes::checkpoints::create_checkpoint)
                    .get(routes::checkpoints::list_checkpoints),
            )
            .route(
                "/v2/namespaces/{namespace}/checkpoints/{label}",
                get(routes::checkpoints::get_checkpoint),
            )
            .route(
                "/v2/namespaces/{namespace}/history",
                get(routes::history::list_history),
            )
            .route(
                "/v2/namespaces/{namespace}/snapshots/{sha}",
                get(routes::history::get_snapshot),
            )
            .route(
                "/v2/activity/snapshots",
                get(routes::activity::list_snapshot_events),
            )
            .route("/v2/cost", get(routes::cost::get_cost_snapshot))
            .route(
                "/v2/cost/timeseries",
                get(routes::cost::get_cost_timeseries),
            )
            .route("/v2/cost/rate-card", get(routes::cost::get_cost_rate_card));
    }

    let router = router
        .route(
            "/v2/vectorstores",
            get(routes::vectorstores::list_vectorstores),
        )
        .route(
            "/v2/vectorstores/{name}",
            get(routes::vectorstores::get_vectorstore),
        )
        .route("/v2/namespaces", get(routes::namespaces::list_namespaces))
        .route("/v1/namespaces", get(routes::turbopuffer::passthrough_get))
        .route(
            "/v1/namespaces/{namespace}/hint_cache_warm",
            get(routes::scans::hint_cache_warm),
        )
        .route(
            "/v1/namespaces/{namespace}/metadata",
            get(routes::turbopuffer::passthrough_get).patch(routes::turbopuffer::passthrough_patch),
        )
        .route(
            "/v1/namespaces/{namespace}/schema",
            get(routes::turbopuffer::passthrough_get).post(routes::turbopuffer::passthrough_post),
        )
        .route(
            "/v1/namespaces/{namespace}/query",
            post(routes::turbopuffer::passthrough_query_post),
        )
        .route(
            "/v1/namespaces/{namespace}/_debug/recall",
            post(routes::turbopuffer::passthrough_post),
        )
        .route(
            "/v2/namespaces/{namespace}/import",
            post(routes::upsert::import_arrow),
        )
        .route(
            "/v2/namespaces/{namespace}",
            post(routes::upsert::upsert_or_delete).delete(routes::namespaces::delete_namespace),
        )
        .route(
            "/v2/namespaces/{namespace}/init",
            post(routes::init::init_namespace),
        )
        .route(
            "/v2/namespaces/{namespace}/query",
            post(routes::query::query),
        )
        .route("/v2/query", post(routes::federated_query::query))
        .route(
            "/v2/namespaces/{namespace}/explain_query",
            post(routes::turbopuffer::passthrough_post),
        )
        .route(
            "/v2/namespaces/{namespace}/documents/{doc_id}",
            get(routes::fetch::fetch_document),
        )
        .route(
            "/v2/namespaces/{namespace}/documents",
            post(routes::fetch::fetch_many_documents),
        )
        .route(
            "/v2/namespaces/{namespace}/snapshots",
            post(routes::scans::create_snapshot_job),
        )
        .route(
            "/v2/namespaces/{namespace}/snapshot-policy",
            get(routes::snapshot_policy::get_policy).put(routes::snapshot_policy::put_policy),
        )
        .route(
            "/v2/namespaces/{namespace}/snapshot-jobs",
            get(routes::scans::list_snapshot_jobs),
        )
        .route(
            "/v2/namespaces/{namespace}/snapshot-jobs/{job_id}",
            get(routes::scans::get_snapshot_job),
        )
        .route(
            "/v2/namespaces/{namespace}/warm",
            post(routes::scans::warm_namespace),
        )
        .route(
            "/v2/namespaces/{namespace}/warm-jobs",
            get(routes::scans::list_warm_jobs),
        )
        .route(
            "/v2/namespaces/{namespace}/warm-jobs/{job_id}",
            get(routes::scans::get_warm_job),
        )
        .route(
            "/v2/namespaces/{namespace}/scans",
            post(routes::scans::create_scan).get(routes::scans::list_scans),
        )
        .route(
            "/v2/namespaces/{namespace}/scans/{scan_id}",
            get(routes::scans::get_scan).delete(routes::scans::delete_scan),
        )
        .route(
            "/v2/namespaces/{namespace}/scans/{scan_id}/results",
            get(routes::scans::get_scan_results),
        )
        .route(
            "/v2/namespaces/{namespace}/metadata",
            get(routes::metadata::get_namespace_metadata),
        )
        // Production vector batches are commonly 10k rows. CLIP vectors in JSON
        // are large enough to exceed axum's small default body limit.
        .layer(DefaultBodyLimit::max(512 * 1024 * 1024))
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&state),
            reject_while_draining,
        ))
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&state),
            auth::require_api_key,
        ));

    public
        .merge(router)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

#[cfg(not(feature = "pro"))]
async fn open_gateway_license() -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({
        "valid": false,
        "state": "floor",
        "reason": "open_gateway",
        "gateway": {
            "state": "floor",
            "seconds_to_deadline": 0,
            "grace_seconds_remaining": 0
        }
    }))
}
