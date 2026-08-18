use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use dashmap::DashMap;
use metrics_catalog::{metric, MetricDoc, MetricKind};
use prometheus::{
    CounterVec, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry, TextEncoder,
};
use serde_json::Value;

use crate::clients::aerospike::{AerospikeClient, AerospikeError, AerospikeErrorKind};
use crate::clients::s3::{S3Client, S3Error};
use crate::clients::turbopuffer::{
    NamespaceMeta, PatchColumns, PatchDoc, TurbopufferClient, TurbopufferError,
    TurbopufferQueryOutcome, TurbopufferWriteOutcome, UpsertDoc,
};
use crate::models::{DocumentPage, DocumentResponse, IncludeAttributes};
#[cfg(feature = "pro")]
use crate::pipeline::{
    ClaimDocumentsArgs, FailDocumentArgs, Pipeline, PipelineMetricsSnapshot, PipelineStatus,
    PipelineStore, PipelineStoreError, SetDocumentsStageArgs,
};
#[cfg(feature = "pro")]
use crate::udf::{
    ClaimUdfItemsArgs, UdfFailure, UdfItemKey, UdfMetricsSnapshot, UdfResource, UdfStatus,
    UdfStore, UdfStoreError,
};

pub const STATUS_OK: &str = "ok";
pub const STATUS_TPUF_ERROR: &str = "tpuf_error";
pub const STATUS_LAYER_ERROR: &str = "layer_error";
pub const STATUS_AEROSPIKE_ERROR: &str = "aerospike_error";
pub const STATUS_AEROSPIKE_STOP_WRITES: &str = "aerospike_stop_writes";
pub const STATUS_PG_ERROR: &str = "pg_error";
pub const STATUS_TIMEOUT: &str = "timeout";

pub const CACHE_HIT: &str = "hit";
pub const CACHE_MISS: &str = "miss";
pub const CACHE_PARTIAL: &str = "partial";
pub const CACHE_BYPASS: &str = "bypass";
pub const CACHE_ERROR: &str = "error";

pub const DIRECT_PIPELINE_ID: &str = "direct";

const PIPELINE_LABEL_CAP: usize = 50;
const NAMESPACE_LABEL_CAP: usize = 5000;

fn seconds_buckets() -> Vec<f64> {
    vec![
        0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0, 10.0,
    ]
}

fn upsert_batch_buckets() -> Vec<f64> {
    vec![1.0, 10.0, 100.0, 1_000.0, 10_000.0]
}

fn fetch_batch_buckets() -> Vec<f64> {
    vec![1.0, 10.0, 100.0, 1_000.0]
}

fn scan_document_buckets() -> Vec<f64> {
    vec![1.0, 10.0, 100.0, 1_000.0, 10_000.0, 100_000.0]
}

fn payload_buckets() -> Vec<f64> {
    vec![
        100.0,
        1_000.0,
        10_000.0,
        100_000.0,
        1_000_000.0,
        10_000_000.0,
    ]
}

fn cold_start_buckets() -> Vec<f64> {
    vec![1.0, 2.5, 5.0, 10.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0]
}

#[derive(Default)]
struct LabelLimiter {
    pipelines: DashMap<String, ()>,
    namespaces: DashMap<String, ()>,
    sets: DashMap<String, ()>,
}

impl LabelLimiter {
    fn pipeline(&self, value: &str) -> String {
        self.cap(value, &self.pipelines, PIPELINE_LABEL_CAP)
    }

    fn namespace(&self, value: &str) -> String {
        self.cap(value, &self.namespaces, NAMESPACE_LABEL_CAP)
    }

    fn set(&self, value: &str) -> String {
        self.cap(value, &self.sets, NAMESPACE_LABEL_CAP)
    }

    fn cap(&self, value: &str, seen: &DashMap<String, ()>, cap: usize) -> String {
        if value.is_empty() {
            return String::new();
        }
        if seen.contains_key(value) {
            return value.to_string();
        }
        if seen.len() >= cap {
            return "other".to_string();
        }
        seen.insert(value.to_string(), ());
        value.to_string()
    }
}

pub struct LayerMetrics {
    registry: Registry,
    labels: LabelLimiter,
    store_kind: Mutex<String>,

    query_duration: HistogramVec,
    query_tpuf: HistogramVec,
    query_overhead: HistogramVec,
    upsert_duration: HistogramVec,
    upsert_tpuf: HistogramVec,
    upsert_overhead: HistogramVec,
    upsert_batch_size: HistogramVec,
    head_duration: HistogramVec,
    list_duration: HistogramVec,
    query_shape_total: IntCounterVec,
    multi_query_total: IntCounterVec,
    multi_query_legs: HistogramVec,
    multi_query_upstream_calls: HistogramVec,
    hybrid_text_total: IntCounterVec,
    hybrid_text_tokens: HistogramVec,
    query_router_total: IntCounterVec,
    agent_query_total: IntCounterVec,
    agent_query_duration: HistogramVec,
    agent_turns: HistogramVec,
    agent_tokens_total: IntCounterVec,
    embed_tokens_total: IntCounterVec,
    embed_compute_seconds_total: CounterVec,
    embed_model_hints: DashMap<String, String>,
    embed_serving_hints: DashMap<String, String>,
    tpuf_billable_bytes_written_total: IntCounterVec,
    tpuf_billable_bytes_queried_total: IntCounterVec,
    tpuf_billable_bytes_returned_total: IntCounterVec,
    tpuf_logical_bytes: IntGaugeVec,

    stage_duration: HistogramVec,
    stage_transitions_total: IntCounterVec,
    fetch_duration: HistogramVec,
    fetch_batch_size: HistogramVec,
    aerospike_op_duration: HistogramVec,
    s3_op_duration: HistogramVec,
    pg_query_duration: HistogramVec,

    cache_lookups_total: IntCounterVec,
    cache_lookup_duration: HistogramVec,
    cache_backfills_total: IntCounterVec,
    cache_backfill_duration: HistogramVec,
    cache_payload_bytes: HistogramVec,
    cache_demand_total: IntCounterVec,
    cache_cold_responses_total: IntCounterVec,
    scan_total: IntCounterVec,
    scan_duration_seconds: HistogramVec,
    scan_documents_scanned: HistogramVec,
    snapshot_field_skipped_total: IntCounterVec,
    snapshot_pruned_total: IntCounterVec,
    cache_state: IntGaugeVec,
    document_cache_cold_starts_total: IntCounter,
    document_cache_cold_start_seconds: Histogram,
    document_cache_cold_start_active: IntGauge,
    document_cache_cold_start_started_at: Mutex<Option<Instant>>,

    #[allow(dead_code)]
    pipeline_stage_count: IntGaugeVec,
    #[allow(dead_code)]
    pipeline_indexed_total: IntCounterVec,
    #[allow(dead_code)]
    pipeline_failed_total: IntCounterVec,
    #[allow(dead_code)]
    udf_queue_depth: IntGaugeVec,
    #[allow(dead_code)]
    udf_stage_count: IntGaugeVec,
    #[allow(dead_code)]
    indexed_seen: DashMap<String, u64>,
    #[allow(dead_code)]
    failed_seen: DashMap<String, u64>,

    #[allow(dead_code)]
    pg_pool_connections: IntGaugeVec,
    aerospike_inflight: IntGauge,
    aerospike_connection_state: IntGaugeVec,
    tpuf_inflight: IntGauge,
}

impl Default for LayerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl LayerMetrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let query_duration = histogram(
            &registry,
            "layer_query_duration_seconds",
            "Total wall-clock time for a query through layer.",
            &["pipeline_id", "namespace", "status"],
            seconds_buckets(),
        );
        let query_tpuf = histogram(
            &registry,
            "layer_query_tpuf_seconds",
            "Time spent inside the Turbopuffer query call.",
            &["pipeline_id", "namespace", "status"],
            seconds_buckets(),
        );
        let query_overhead = histogram(
            &registry,
            "layer_query_overhead_seconds",
            "Layer-side query overhead: total minus Turbopuffer time.",
            &["pipeline_id", "namespace", "status"],
            seconds_buckets(),
        );
        let upsert_duration = histogram(
            &registry,
            "layer_upsert_duration_seconds",
            "Total wall-clock time for an upsert through layer.",
            &["pipeline_id", "namespace", "status"],
            seconds_buckets(),
        );
        let upsert_tpuf = histogram(
            &registry,
            "layer_upsert_tpuf_seconds",
            "Time spent inside the Turbopuffer upsert call.",
            &["pipeline_id", "namespace", "status"],
            seconds_buckets(),
        );
        let upsert_overhead = histogram(
            &registry,
            "layer_upsert_overhead_seconds",
            "Layer-side upsert overhead: total minus Turbopuffer time.",
            &["pipeline_id", "namespace", "status"],
            seconds_buckets(),
        );
        let upsert_batch_size = histogram(
            &registry,
            "layer_upsert_batch_size",
            "Documents per upsert request.",
            &["pipeline_id", "namespace"],
            upsert_batch_buckets(),
        );
        let head_duration = histogram(
            &registry,
            "layer_head_duration_seconds",
            "Total wall-clock time for a namespace metadata/head call.",
            &["pipeline_id", "namespace", "status"],
            seconds_buckets(),
        );
        let list_duration = histogram(
            &registry,
            "layer_list_duration_seconds",
            "Total wall-clock time for a namespace list call.",
            &["pipeline_id", "namespace", "status"],
            seconds_buckets(),
        );
        let query_shape_total = counter(
            &registry,
            "layer_query_shape_total",
            "Query-shape distribution.",
            &[
                "pipeline_id",
                "namespace",
                "has_filter",
                "has_rank_by",
                "status",
            ],
        );
        let multi_query_total = counter(
            &registry,
            "hevlayer_multi_query_total",
            "Multi-query requests by namespace and status.",
            &["namespace", "store_kind", "status"],
        );
        let multi_query_legs = histogram(
            &registry,
            "hevlayer_multi_query_legs",
            "Leg count per multi-query request.",
            &["namespace", "store_kind"],
            vec![2.0, 4.0, 8.0, 16.0],
        );
        let multi_query_upstream_calls = histogram(
            &registry,
            "hevlayer_multi_query_upstream_calls",
            "Turbopuffer upstream query calls per multi-query request.",
            &["namespace", "store_kind"],
            vec![1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0],
        );
        let hybrid_text_total = counter(
            &registry,
            "hevlayer_hybrid_text_queries_total",
            "Hybrid text fusion queries by namespace and status.",
            &["namespace", "store_kind", "status"],
        );
        let hybrid_text_tokens = histogram(
            &registry,
            "hevlayer_hybrid_text_tokens",
            "Token count per hybrid text query after the tokenizer policy.",
            &["namespace", "store_kind"],
            vec![1.0, 2.0, 3.0, 4.0, 5.0, 8.0, 10.0, 15.0],
        );
        let query_router_total = counter(
            &registry,
            "hevlayer_query_router_decisions_total",
            "Query router decisions by namespace, chosen route, and whether the route executed (false = vectorless deferral).",
            &["namespace", "store_kind", "route", "executed"],
        );
        let agent_query_total = counter(
            &registry,
            "hevlayer_agent_queries_total",
            "Agentic search requests by agent, status, and whether the deadline was hit.",
            &["agent", "status", "deadline_hit"],
        );
        let agent_query_duration = histogram(
            &registry,
            "hevlayer_agent_query_duration_seconds",
            "Total wall-clock time for an agentic search request.",
            &["agent", "status"],
            seconds_buckets(),
        );
        let agent_turns = histogram(
            &registry,
            "hevlayer_agent_turns",
            "Model turns spent by an agentic search request.",
            &["agent", "status"],
            vec![1.0, 2.0],
        );
        let agent_tokens_total = counter(
            &registry,
            "hevlayer_agent_tokens_total",
            "Model tokens reported by the inference provider for agentic search.",
            &["agent", "turn", "token_type"],
        );
        let embed_tokens_total = counter(
            &registry,
            "hevlayer_embed_tokens_total",
            "Embedding tokens echoed by the serving provider.",
            &["namespace", "store_kind", "model", "serving"],
        );
        let embed_compute_seconds_total = float_counter(
            &registry,
            "hevlayer_embed_compute_seconds_total",
            "Embedding latency echoed by the serving provider, in seconds.",
            &["namespace", "store_kind", "model", "serving"],
        );
        let tpuf_billable_bytes_written_total = counter(
            &registry,
            "hevlayer_tpuf_billable_bytes_written_total",
            "Turbopuffer billable logical bytes written, copied from upstream billing objects.",
            &["namespace", "store_kind"],
        );
        let tpuf_billable_bytes_queried_total = counter(
            &registry,
            "hevlayer_tpuf_billable_bytes_queried_total",
            "Turbopuffer billable logical bytes queried, copied from upstream billing objects.",
            &["namespace", "store_kind"],
        );
        let tpuf_billable_bytes_returned_total = counter(
            &registry,
            "hevlayer_tpuf_billable_bytes_returned_total",
            "Turbopuffer billable logical bytes returned, copied from upstream billing objects.",
            &["namespace", "store_kind"],
        );
        let tpuf_logical_bytes = gauge_vec(
            &registry,
            "hevlayer_tpuf_logical_bytes",
            "Latest Turbopuffer approx_logical_bytes by namespace from metadata responses.",
            &["namespace", "store_kind"],
        );

        let stage_duration = histogram(
            &registry,
            "layer_stage_duration_seconds",
            "Time a document spent in a pipeline stage before transitioning.",
            &["pipeline_id", "from_stage", "to_stage"],
            seconds_buckets(),
        );
        let stage_transitions_total = counter(
            &registry,
            "layer_stage_transitions_total",
            "Pipeline stage transitions.",
            &["pipeline_id", "from_stage", "to_stage", "status"],
        );
        let fetch_duration = histogram(
            &registry,
            "layer_fetch_duration_seconds",
            "Document fetch time through the pull-through cache.",
            &["operation", "namespace", "cache_result"],
            seconds_buckets(),
        );
        let fetch_batch_size = histogram(
            &registry,
            "layer_fetch_batch_size",
            "Documents per batch fetch.",
            &["operation"],
            fetch_batch_buckets(),
        );
        let aerospike_op_duration = histogram(
            &registry,
            "layer_aerospike_op_duration_seconds",
            "Raw Aerospike operation duration.",
            &["operation", "set", "status"],
            seconds_buckets(),
        );
        let s3_op_duration = histogram(
            &registry,
            "layer_s3_op_duration_seconds",
            "Raw S3 operation duration.",
            &["operation", "status"],
            seconds_buckets(),
        );
        let pg_query_duration = histogram(
            &registry,
            "layer_pg_query_duration_seconds",
            "Named PostgreSQL query duration.",
            &["query_name", "status"],
            seconds_buckets(),
        );

        let cache_lookups_total = counter(
            &registry,
            "layer_cache_lookups_total",
            "Per-item cache lookups.",
            &["set", "namespace", "result"],
        );
        let cache_lookup_duration = histogram(
            &registry,
            "layer_cache_lookup_duration_seconds",
            "Cache lookup/check duration.",
            &["set", "result"],
            seconds_buckets(),
        );
        let cache_backfills_total = counter(
            &registry,
            "layer_cache_backfills_total",
            "Best-effort cache backfill writes.",
            &["set", "status"],
        );
        let cache_backfill_duration = histogram(
            &registry,
            "layer_cache_backfill_duration_seconds",
            "Best-effort cache backfill write duration.",
            &["set", "status"],
            seconds_buckets(),
        );
        let cache_payload_bytes = histogram(
            &registry,
            "layer_cache_payload_bytes",
            "Cached payload sizes.",
            &["set", "result"],
            payload_buckets(),
        );
        let cache_demand_total = counter(
            &registry,
            "hevlayer_cache_demand_total",
            "Cache-path requests by logical namespace. Used as the Aerospike scale-up signal.",
            &["namespace", "store_kind"],
        );
        let cache_cold_responses_total = counter(
            &registry,
            "hevlayer_cache_cold_responses_total",
            "Cache-path 503 responses caused by a cold or unavailable cache.",
            &["namespace", "store_kind"],
        );
        let scan_total = counter(
            &registry,
            "hevlayer_scan_total",
            "Scan requests by namespace, mode, selector, serving path, and status.",
            &[
                "namespace",
                "store_kind",
                "mode",
                "selector",
                "served_by",
                "status",
            ],
        );
        let scan_duration_seconds = histogram(
            &registry,
            "hevlayer_scan_duration_seconds",
            "Wall-clock scan duration by namespace, mode, selector, serving path, and status.",
            &[
                "namespace",
                "store_kind",
                "mode",
                "selector",
                "served_by",
                "status",
            ],
            seconds_buckets(),
        );
        let scan_documents_scanned = histogram(
            &registry,
            "hevlayer_scan_documents_scanned",
            "Documents scanned per scan request by namespace, mode, selector, serving path, and status.",
            &[
                "namespace",
                "store_kind",
                "mode",
                "selector",
                "served_by",
                "status",
            ],
            scan_document_buckets(),
        );
        let snapshot_field_skipped_total = counter(
            &registry,
            "hevlayer_snapshot_field_skipped_total",
            "Snapshot facet fields skipped at materialization time.",
            &["namespace", "store_kind", "field", "reason"],
        );
        let snapshot_pruned_total = counter(
            &registry,
            "hevlayer_snapshot_pruned_total",
            "Snapshot bodies pruned by retention policy.",
            &["namespace", "store_kind"],
        );
        let cache_state = gauge_vec(
            &registry,
            "hevlayer_cache_state",
            "Per-namespace cache state. Exactly one state label should be 1 for a seen namespace.",
            &["namespace", "store_kind", "state"],
        );
        let document_cache_cold_starts_total = counter_no_labels(
            &registry,
            "hevlayer_document_cache_cold_starts_total",
            "Demand-triggered document-cache cold starts completed after Aerospike reconnect.",
        );
        let document_cache_cold_start_seconds = histogram_no_labels(
            &registry,
            "hevlayer_document_cache_cold_start_seconds",
            "Seconds from first cache-path demand while Aerospike is unavailable until gateway reconnect.",
            cold_start_buckets(),
        );
        let document_cache_cold_start_active = gauge(
            &registry,
            "hevlayer_document_cache_cold_start_active",
            "Whether a demand-triggered document-cache cold start is currently waiting for Aerospike reconnect.",
        );

        let pipeline_stage_count = gauge_vec(
            &registry,
            "layer_pipeline_stage_count",
            "Current document count per pipeline stage.",
            &["pipeline_id", "stage"],
        );
        let pipeline_indexed_total = counter(
            &registry,
            "layer_pipeline_indexed_total",
            "Monotonic indexed document count by pipeline.",
            &["pipeline_id"],
        );
        let pipeline_failed_total = counter(
            &registry,
            "layer_pipeline_failed_total",
            "Documents that landed in failed by pipeline and reason.",
            &["pipeline_id", "reason"],
        );
        let udf_queue_depth = gauge_vec(
            &registry,
            "layer_udf_queue_depth",
            "Current pending work item count per UDF.",
            &["udf"],
        );
        let udf_stage_count = gauge_vec(
            &registry,
            "layer_udf_stage_count",
            "Current work item count per UDF stage.",
            &["udf", "stage"],
        );

        let pg_pool_connections = gauge_vec(
            &registry,
            "layer_pg_pool_connections",
            "Postgres pool connections by state.",
            &["state"],
        );
        let aerospike_inflight = gauge(
            &registry,
            "layer_aerospike_inflight",
            "Current in-flight Aerospike operations.",
        );
        let aerospike_connection_state = gauge_vec(
            &registry,
            "layer_aerospike_connection_state",
            "Aerospike connection state.",
            &["state"],
        );
        let tpuf_inflight = gauge(
            &registry,
            "layer_tpuf_inflight",
            "Current in-flight Turbopuffer calls.",
        );

        Self {
            registry,
            labels: LabelLimiter::default(),
            store_kind: Mutex::new("turbopuffer".to_string()),
            query_duration,
            query_tpuf,
            query_overhead,
            upsert_duration,
            upsert_tpuf,
            upsert_overhead,
            upsert_batch_size,
            head_duration,
            list_duration,
            query_shape_total,
            multi_query_total,
            multi_query_legs,
            multi_query_upstream_calls,
            hybrid_text_total,
            hybrid_text_tokens,
            query_router_total,
            agent_query_total,
            agent_query_duration,
            agent_turns,
            agent_tokens_total,
            embed_tokens_total,
            embed_compute_seconds_total,
            embed_model_hints: DashMap::new(),
            embed_serving_hints: DashMap::new(),
            tpuf_billable_bytes_written_total,
            tpuf_billable_bytes_queried_total,
            tpuf_billable_bytes_returned_total,
            tpuf_logical_bytes,
            stage_duration,
            stage_transitions_total,
            fetch_duration,
            fetch_batch_size,
            aerospike_op_duration,
            s3_op_duration,
            pg_query_duration,
            cache_lookups_total,
            cache_lookup_duration,
            cache_backfills_total,
            cache_backfill_duration,
            cache_payload_bytes,
            cache_demand_total,
            cache_cold_responses_total,
            scan_total,
            scan_duration_seconds,
            scan_documents_scanned,
            snapshot_field_skipped_total,
            snapshot_pruned_total,
            cache_state,
            document_cache_cold_starts_total,
            document_cache_cold_start_seconds,
            document_cache_cold_start_active,
            document_cache_cold_start_started_at: Mutex::new(None),
            pipeline_stage_count,
            pipeline_indexed_total,
            pipeline_failed_total,
            udf_queue_depth,
            udf_stage_count,
            indexed_seen: DashMap::new(),
            failed_seen: DashMap::new(),
            pg_pool_connections,
            aerospike_inflight,
            aerospike_connection_state,
            tpuf_inflight,
        }
    }

    pub fn encode(&self) -> Result<String, String> {
        let encoder = TextEncoder::new();
        encoder
            .encode_to_string(&self.registry.gather())
            .map_err(|e| format!("metrics encoding error: {e}"))
    }

    pub fn set_store_kind(&self, store_kind: &str) {
        let mut current = self
            .store_kind
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *current = store_kind.to_string();
    }

    fn store_kind(&self) -> String {
        self.store_kind
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn instrument_turbopuffer(
        self: &Arc<Self>,
        inner: Arc<dyn TurbopufferClient>,
    ) -> Arc<dyn TurbopufferClient> {
        Arc::new(MetricsTurbopufferClient {
            inner,
            metrics: Arc::clone(self),
        })
    }

    pub fn instrument_aerospike(
        self: &Arc<Self>,
        inner: Arc<dyn AerospikeClient>,
        set_prefix: impl Into<String>,
    ) -> Arc<dyn AerospikeClient> {
        Arc::new(MetricsAerospikeClient {
            inner,
            metrics: Arc::clone(self),
            set_prefix: set_prefix.into(),
        })
    }

    pub fn instrument_s3(self: &Arc<Self>, inner: Arc<dyn S3Client>) -> Arc<dyn S3Client> {
        Arc::new(MetricsS3Client {
            inner,
            metrics: Arc::clone(self),
        })
    }

    #[cfg(feature = "pro")]
    pub fn instrument_pipeline_store(
        self: &Arc<Self>,
        inner: Arc<dyn PipelineStore>,
    ) -> Arc<dyn PipelineStore> {
        Arc::new(MetricsPipelineStore {
            inner,
            metrics: Arc::clone(self),
            stage_entries: DashMap::new(),
        })
    }

    #[cfg(feature = "pro")]
    pub fn instrument_udf_store(self: &Arc<Self>, inner: Arc<dyn UdfStore>) -> Arc<dyn UdfStore> {
        Arc::new(MetricsUdfStore {
            inner,
            metrics: Arc::clone(self),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn observe_query(
        &self,
        pipeline_id: &str,
        namespace: &str,
        status: &str,
        total_seconds: f64,
        tpuf_seconds: f64,
        has_filter: bool,
        has_rank_by: bool,
    ) {
        let (pipeline_id, namespace) = self.pipeline_namespace_labels(pipeline_id, namespace);
        let labels = [&pipeline_id[..], &namespace[..], status];
        self.query_duration
            .with_label_values(&labels)
            .observe(total_seconds);
        self.query_tpuf
            .with_label_values(&labels)
            .observe(tpuf_seconds);
        self.query_overhead
            .with_label_values(&labels)
            .observe((total_seconds - tpuf_seconds).max(0.0));

        let has_filter = if has_filter { "true" } else { "false" };
        let has_rank_by = if has_rank_by { "true" } else { "false" };
        self.query_shape_total
            .with_label_values(&[
                pipeline_id.as_str(),
                namespace.as_str(),
                has_filter,
                has_rank_by,
                status,
            ])
            .inc();
    }

    pub fn observe_multi_query(
        &self,
        namespace: &str,
        status: &str,
        legs: usize,
        upstream_calls: usize,
    ) {
        let namespace = self.labels.namespace(namespace);
        let store_kind = self.store_kind();
        self.multi_query_total
            .with_label_values(&[namespace.as_str(), store_kind.as_str(), status])
            .inc();
        self.multi_query_legs
            .with_label_values(&[&namespace, &store_kind])
            .observe(legs as f64);
        self.multi_query_upstream_calls
            .with_label_values(&[&namespace, &store_kind])
            .observe(upstream_calls as f64);
    }

    /// `tokens` is observed only when the expansion got far enough to
    /// tokenize (i.e. on success); error paths increment the counter only.
    pub fn observe_hybrid_text_query(&self, namespace: &str, status: &str, tokens: Option<usize>) {
        let namespace = self.labels.namespace(namespace);
        let store_kind = self.store_kind();
        self.hybrid_text_total
            .with_label_values(&[namespace.as_str(), store_kind.as_str(), status])
            .inc();
        if let Some(tokens) = tokens {
            self.hybrid_text_tokens
                .with_label_values(&[&namespace, &store_kind])
                .observe(tokens as f64);
        }
    }

    pub fn observe_query_router(&self, namespace: &str, route: &str, executed: bool) {
        let namespace = self.labels.namespace(namespace);
        let store_kind = self.store_kind();
        let executed = if executed { "true" } else { "false" };
        self.query_router_total
            .with_label_values(&[namespace.as_str(), store_kind.as_str(), route, executed])
            .inc();
    }

    pub fn observe_agent_query(
        &self,
        agent: &str,
        status: &str,
        deadline_hit: bool,
        turns: u64,
        elapsed: std::time::Duration,
    ) {
        let deadline_hit = if deadline_hit { "true" } else { "false" };
        self.agent_query_total
            .with_label_values(&[agent, status, deadline_hit])
            .inc();
        self.agent_query_duration
            .with_label_values(&[agent, status])
            .observe(elapsed.as_secs_f64());
        self.agent_turns
            .with_label_values(&[agent, status])
            .observe(turns as f64);
    }

    pub fn observe_agent_tokens(
        &self,
        agent: &str,
        turn: &str,
        prompt: u64,
        completion: u64,
        total: u64,
    ) {
        self.agent_tokens_total
            .with_label_values(&[agent, turn, "prompt"])
            .inc_by(prompt);
        self.agent_tokens_total
            .with_label_values(&[agent, turn, "completion"])
            .inc_by(completion);
        self.agent_tokens_total
            .with_label_values(&[agent, turn, "total"])
            .inc_by(total);
    }

    pub fn observe_embed_performance(
        &self,
        namespace: &str,
        model: &str,
        serving: &str,
        performance: &Value,
    ) {
        let namespace = self.labels.namespace(namespace);
        let store_kind = self.store_kind();
        let labels = [&namespace, &store_kind, model, serving];
        if let Some(tokens) = billing_u64(performance, "embedding_tokens") {
            self.embed_tokens_total
                .with_label_values(&labels)
                .inc_by(tokens);
        }
        if let Some(milliseconds) = performance
            .get("embedding_ms")
            .and_then(Value::as_f64)
            .filter(|value| value.is_finite() && *value >= 0.0)
        {
            self.embed_compute_seconds_total
                .with_label_values(&labels)
                .inc_by(milliseconds / 1_000.0);
        }
    }

    pub(crate) fn remember_embed_model(
        &self,
        namespace: &str,
        source: &str,
        target: &str,
        model: &str,
    ) {
        for attribute in [source, target] {
            self.embed_model_hints
                .insert(format!("{namespace}\u{1f}{attribute}"), model.to_string());
        }
    }

    pub(crate) fn embed_model_hint(&self, namespace: &str, attribute: &str) -> Option<String> {
        self.embed_model_hints
            .get(&format!("{namespace}\u{1f}{attribute}"))
            .map(|model| model.clone())
    }

    pub(crate) fn remember_embed_serving(
        &self,
        namespace: &str,
        source: &str,
        target: &str,
        serving: &str,
    ) {
        for attribute in [source, target] {
            self.embed_serving_hints
                .insert(format!("{namespace}\u{1f}{attribute}"), serving.to_string());
        }
    }

    fn embed_serving_hint(&self, namespace: &str, attribute: &str) -> Option<String> {
        self.embed_serving_hints
            .get(&format!("{namespace}\u{1f}{attribute}"))
            .map(|serving| serving.clone())
    }

    pub fn observe_tpuf_billing(&self, namespace: &str, billing: &Value) {
        let namespace = self.labels.namespace(namespace);
        let store_kind = self.store_kind();
        if let Some(bytes) = billing_u64(billing, "billable_logical_bytes_written") {
            self.tpuf_billable_bytes_written_total
                .with_label_values(&[&namespace, &store_kind])
                .inc_by(bytes);
        }
        if let Some(bytes) = billing_u64(billing, "billable_logical_bytes_queried") {
            self.tpuf_billable_bytes_queried_total
                .with_label_values(&[&namespace, &store_kind])
                .inc_by(bytes);
        }
        if let Some(bytes) = billing_u64(billing, "billable_logical_bytes_returned") {
            self.tpuf_billable_bytes_returned_total
                .with_label_values(&[&namespace, &store_kind])
                .inc_by(bytes);
        }
    }

    pub fn set_tpuf_logical_bytes(&self, namespace: &str, bytes: u64) {
        let namespace = self.labels.namespace(namespace);
        let store_kind = self.store_kind();
        let bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
        self.tpuf_logical_bytes
            .with_label_values(&[&namespace, &store_kind])
            .set(bytes);
    }

    pub fn observe_upsert(
        &self,
        pipeline_id: &str,
        namespace: &str,
        status: &str,
        total_seconds: f64,
        tpuf_seconds: f64,
        batch_size: Option<usize>,
    ) {
        let (pipeline_id, namespace) = self.pipeline_namespace_labels(pipeline_id, namespace);
        let labels = [&pipeline_id[..], &namespace[..], status];
        self.upsert_duration
            .with_label_values(&labels)
            .observe(total_seconds);
        self.upsert_tpuf
            .with_label_values(&labels)
            .observe(tpuf_seconds);
        self.upsert_overhead
            .with_label_values(&labels)
            .observe((total_seconds - tpuf_seconds).max(0.0));
        if let Some(batch_size) = batch_size {
            self.upsert_batch_size
                .with_label_values(&[&pipeline_id, &namespace])
                .observe(batch_size as f64);
        }
    }

    pub fn observe_head(&self, pipeline_id: &str, namespace: &str, status: &str, seconds: f64) {
        let (pipeline_id, namespace) = self.pipeline_namespace_labels(pipeline_id, namespace);
        self.head_duration
            .with_label_values(&[pipeline_id.as_str(), namespace.as_str(), status])
            .observe(seconds);
    }

    pub fn observe_list(&self, pipeline_id: &str, namespace: &str, status: &str, seconds: f64) {
        let (pipeline_id, namespace) = self.pipeline_namespace_labels(pipeline_id, namespace);
        self.list_duration
            .with_label_values(&[pipeline_id.as_str(), namespace.as_str(), status])
            .observe(seconds);
    }

    pub fn observe_stage_transition(
        &self,
        pipeline_id: &str,
        from_stage: &str,
        to_stage: &str,
        status: &str,
        count: u64,
        duration_seconds: Option<f64>,
    ) {
        let pipeline_id = self.labels.pipeline(pipeline_id);
        self.stage_transitions_total
            .with_label_values(&[pipeline_id.as_str(), from_stage, to_stage, status])
            .inc_by(count);
        if let Some(seconds) = duration_seconds {
            for _ in 0..count {
                self.stage_duration
                    .with_label_values(&[pipeline_id.as_str(), from_stage, to_stage])
                    .observe(seconds);
            }
        }
    }

    pub fn observe_fetch(
        &self,
        operation: &str,
        namespace: &str,
        cache_result: &str,
        seconds: f64,
    ) {
        let namespace = self.labels.namespace(namespace);
        self.fetch_duration
            .with_label_values(&[operation, &namespace, cache_result])
            .observe(seconds);
    }

    pub fn observe_fetch_batch_size(&self, operation: &str, size: usize) {
        self.fetch_batch_size
            .with_label_values(&[operation])
            .observe(size as f64);
    }

    pub fn observe_cache_lookup(
        &self,
        set: &str,
        namespace: &str,
        result: &str,
        count: u64,
        seconds: f64,
        payload_bytes: Option<usize>,
    ) {
        let set = self.labels.set(set);
        let namespace = self.labels.namespace(namespace);
        self.cache_lookups_total
            .with_label_values(&[set.as_str(), namespace.as_str(), result])
            .inc_by(count);
        self.cache_lookup_duration
            .with_label_values(&[set.as_str(), result])
            .observe(seconds);
        if let Some(bytes) = payload_bytes {
            self.cache_payload_bytes
                .with_label_values(&[set.as_str(), result])
                .observe(bytes as f64);
        }
    }

    pub fn observe_cache_backfill(
        &self,
        set: &str,
        status: &str,
        seconds: f64,
        payload_bytes: Option<usize>,
    ) {
        let set = self.labels.set(set);
        self.cache_backfills_total
            .with_label_values(&[set.as_str(), status])
            .inc();
        self.cache_backfill_duration
            .with_label_values(&[set.as_str(), status])
            .observe(seconds);
        if let Some(bytes) = payload_bytes {
            self.cache_payload_bytes
                .with_label_values(&[set.as_str(), status])
                .observe(bytes as f64);
        }
    }

    pub fn observe_cache_payload(&self, set: &str, result: &str, payload_bytes: usize) {
        let set = self.labels.set(set);
        self.cache_payload_bytes
            .with_label_values(&[set.as_str(), result])
            .observe(payload_bytes as f64);
    }

    pub fn observe_cache_demand(&self, namespace: &str) {
        let namespace = self.labels.namespace(namespace);
        let store_kind = self.store_kind();
        self.cache_demand_total
            .with_label_values(&[&namespace, &store_kind])
            .inc();
    }

    pub fn maybe_start_document_cache_cold_start(&self) {
        let mut started_at = self
            .document_cache_cold_start_started_at
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if started_at.is_some() {
            return;
        }

        *started_at = Some(Instant::now());
        self.document_cache_cold_start_active.set(1);
    }

    pub fn observe_document_cache_reconnected(&self) {
        let started_at = self
            .document_cache_cold_start_started_at
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();

        if let Some(started_at) = started_at {
            self.document_cache_cold_start_seconds
                .observe(started_at.elapsed().as_secs_f64());
            self.document_cache_cold_starts_total.inc();
        }
        self.document_cache_cold_start_active.set(0);
    }

    pub fn observe_cache_cold_response(&self, namespace: &str) {
        let namespace = self.labels.namespace(namespace);
        let store_kind = self.store_kind();
        self.cache_cold_responses_total
            .with_label_values(&[&namespace, &store_kind])
            .inc();
    }

    #[allow(clippy::too_many_arguments)]
    pub fn observe_scan(
        &self,
        namespace: &str,
        mode: &str,
        selector: &str,
        served_by: &str,
        status: &str,
        seconds: f64,
        documents_scanned: u64,
    ) {
        let namespace = self.labels.namespace(namespace);
        let store_kind = self.store_kind();
        let labels = [
            &namespace[..],
            &store_kind[..],
            mode,
            selector,
            served_by,
            status,
        ];
        self.scan_total.with_label_values(&labels).inc();
        self.scan_duration_seconds
            .with_label_values(&labels)
            .observe(seconds);
        self.scan_documents_scanned
            .with_label_values(&labels)
            .observe(documents_scanned as f64);
    }

    pub fn observe_snapshot_field_skipped(&self, namespace: &str, field: &str, reason: &str) {
        let namespace = self.labels.namespace(namespace);
        let store_kind = self.store_kind();
        self.snapshot_field_skipped_total
            .with_label_values(&[namespace.as_str(), store_kind.as_str(), field, reason])
            .inc();
    }

    pub fn observe_snapshot_pruned(&self, namespace: &str) {
        let namespace = self.labels.namespace(namespace);
        let store_kind = self.store_kind();
        self.snapshot_pruned_total
            .with_label_values(&[&namespace, &store_kind])
            .inc();
    }

    pub fn set_cache_state(&self, namespace: &str, state: &str) {
        let namespace = self.labels.namespace(namespace);
        let store_kind = self.store_kind();
        for candidate in ["cold", "warming", "warm"] {
            let value = if candidate == state { 1 } else { 0 };
            self.cache_state
                .with_label_values(&[namespace.as_str(), store_kind.as_str(), candidate])
                .set(value);
        }
    }

    pub fn set_aerospike_connection_state(&self, connected: bool) {
        self.aerospike_connection_state
            .with_label_values(&["open"])
            .set(if connected { 1 } else { 0 });
        self.aerospike_connection_state
            .with_label_values(&["failed"])
            .set(if connected { 0 } else { 1 });
    }

    pub fn observe_aerospike_op(&self, operation: &str, set: &str, status: &str, seconds: f64) {
        let set = self.labels.set(set);
        self.aerospike_op_duration
            .with_label_values(&[operation, &set, status])
            .observe(seconds);
        match status {
            STATUS_OK => {
                self.set_aerospike_connection_state(true);
            }
            STATUS_AEROSPIKE_ERROR => {
                self.set_aerospike_connection_state(false);
            }
            _ => {}
        }
    }

    pub fn observe_s3_op(&self, operation: &str, status: &str, seconds: f64) {
        self.s3_op_duration
            .with_label_values(&[operation, status])
            .observe(seconds);
    }

    pub fn observe_pg_query(&self, query_name: &str, status: &str, seconds: f64) {
        self.pg_query_duration
            .with_label_values(&[query_name, status])
            .observe(seconds);
    }

    #[cfg(feature = "pro")]
    pub fn refresh_pipeline_metrics(&self, snapshot: PipelineMetricsSnapshot) {
        self.pipeline_stage_count.reset();
        for row in snapshot.stage_counts {
            let pipeline_id = self.labels.pipeline(&row.pipeline_id);
            self.pipeline_stage_count
                .with_label_values(&[&pipeline_id, &row.stage])
                .set(row.count.max(0));
        }

        for row in snapshot.indexed_totals {
            let pipeline_id = self.labels.pipeline(&row.pipeline_id);
            self.sync_counter(
                &self.pipeline_indexed_total,
                &self.indexed_seen,
                pipeline_id.clone(),
                &[&pipeline_id],
                row.count,
            );
        }

        for row in snapshot.failed_totals {
            let pipeline_id = self.labels.pipeline(&row.pipeline_id);
            let key = format!("{}\u{1f}{}", pipeline_id, row.reason);
            self.sync_counter(
                &self.pipeline_failed_total,
                &self.failed_seen,
                key,
                &[&pipeline_id, &row.reason],
                row.count,
            );
        }

        if let Some(pool) = snapshot.pg_pool {
            self.pg_pool_connections
                .with_label_values(&["idle"])
                .set(pool.idle);
            self.pg_pool_connections
                .with_label_values(&["in_use"])
                .set(pool.in_use);
            self.pg_pool_connections
                .with_label_values(&["waiting"])
                .set(pool.waiting);
        }
    }

    #[cfg(feature = "pro")]
    pub fn refresh_udf_metrics(&self, snapshot: UdfMetricsSnapshot) {
        self.udf_stage_count.reset();
        self.udf_queue_depth.reset();
        for row in snapshot.stage_counts {
            let udf = self.labels.pipeline(&row.udf_id);
            self.udf_stage_count
                .with_label_values(&[&udf, &row.stage])
                .set(row.count.max(0));
        }
        for row in snapshot.queue_depths {
            let udf = self.labels.pipeline(&row.udf_id);
            self.udf_queue_depth
                .with_label_values(&[&udf])
                .set(row.count.max(0));
        }

        if let Some(pool) = snapshot.pg_pool {
            self.pg_pool_connections
                .with_label_values(&["idle"])
                .set(pool.idle);
            self.pg_pool_connections
                .with_label_values(&["in_use"])
                .set(pool.in_use);
            self.pg_pool_connections
                .with_label_values(&["waiting"])
                .set(pool.waiting);
        }
    }

    pub fn inc_tpuf_inflight(&self) {
        self.tpuf_inflight.inc();
    }

    pub fn dec_tpuf_inflight(&self) {
        self.tpuf_inflight.dec();
    }

    pub fn inc_aerospike_inflight(&self) {
        self.aerospike_inflight.inc();
    }

    pub fn dec_aerospike_inflight(&self) {
        self.aerospike_inflight.dec();
    }

    fn pipeline_namespace_labels(&self, pipeline_id: &str, namespace: &str) -> (String, String) {
        (
            self.labels.pipeline(pipeline_id),
            self.labels.namespace(namespace),
        )
    }

    #[allow(dead_code)]
    fn sync_counter(
        &self,
        counter: &IntCounterVec,
        seen: &DashMap<String, u64>,
        key: String,
        labels: &[&str],
        value: u64,
    ) {
        let prev = seen.get(&key).map(|v| *v.value()).unwrap_or(0);
        if value > prev {
            counter.with_label_values(labels).inc_by(value - prev);
        }
        seen.insert(key, value);
    }
}

fn histogram(
    registry: &Registry,
    name: &str,
    help: &str,
    labels: &[&str],
    buckets: Vec<f64>,
) -> HistogramVec {
    let doc = catalog_doc(name, MetricKind::Histogram, labels, help);
    let metric = HistogramVec::new(
        HistogramOpts::new(doc.name, doc.description).buckets(buckets),
        labels,
    )
    .unwrap_or_else(|e| panic!("register metric {name}: {e}"));
    registry
        .register(Box::new(metric.clone()))
        .unwrap_or_else(|e| panic!("register metric {name}: {e}"));
    metric
}

fn counter(registry: &Registry, name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    let doc = catalog_doc(name, MetricKind::Counter, labels, help);
    let metric = IntCounterVec::new(Opts::new(doc.name, doc.description), labels)
        .unwrap_or_else(|e| panic!("register metric {name}: {e}"));
    registry
        .register(Box::new(metric.clone()))
        .unwrap_or_else(|e| panic!("register metric {name}: {e}"));
    metric
}

fn float_counter(registry: &Registry, name: &str, help: &str, labels: &[&str]) -> CounterVec {
    let doc = catalog_doc(name, MetricKind::Counter, labels, help);
    let metric = CounterVec::new(Opts::new(doc.name, doc.description), labels)
        .unwrap_or_else(|e| panic!("register metric {name}: {e}"));
    registry
        .register(Box::new(metric.clone()))
        .unwrap_or_else(|e| panic!("register metric {name}: {e}"));
    metric
}

fn counter_no_labels(registry: &Registry, name: &str, help: &str) -> IntCounter {
    let doc = catalog_doc(name, MetricKind::Counter, &[], help);
    let metric = IntCounter::new(doc.name, doc.description)
        .unwrap_or_else(|e| panic!("register metric {name}: {e}"));
    registry
        .register(Box::new(metric.clone()))
        .unwrap_or_else(|e| panic!("register metric {name}: {e}"));
    metric
}

fn histogram_no_labels(
    registry: &Registry,
    name: &str,
    help: &str,
    buckets: Vec<f64>,
) -> Histogram {
    let doc = catalog_doc(name, MetricKind::Histogram, &[], help);
    let metric =
        Histogram::with_opts(HistogramOpts::new(doc.name, doc.description).buckets(buckets))
            .unwrap_or_else(|e| panic!("register metric {name}: {e}"));
    registry
        .register(Box::new(metric.clone()))
        .unwrap_or_else(|e| panic!("register metric {name}: {e}"));
    metric
}

fn gauge(registry: &Registry, name: &str, help: &str) -> IntGauge {
    let doc = catalog_doc(name, MetricKind::Gauge, &[], help);
    let metric = IntGauge::new(doc.name, doc.description)
        .unwrap_or_else(|e| panic!("register metric {name}: {e}"));
    registry
        .register(Box::new(metric.clone()))
        .unwrap_or_else(|e| panic!("register metric {name}: {e}"));
    metric
}

fn gauge_vec(registry: &Registry, name: &str, help: &str, labels: &[&str]) -> IntGaugeVec {
    let doc = catalog_doc(name, MetricKind::Gauge, labels, help);
    let metric = IntGaugeVec::new(Opts::new(doc.name, doc.description), labels)
        .unwrap_or_else(|e| panic!("register metric {name}: {e}"));
    registry
        .register(Box::new(metric.clone()))
        .unwrap_or_else(|e| panic!("register metric {name}: {e}"));
    metric
}

fn catalog_doc(name: &str, kind: MetricKind, labels: &[&str], help: &str) -> &'static MetricDoc {
    let doc = metric(name).unwrap_or_else(|| panic!("missing MetricDoc for {name}"));
    assert_eq!(doc.kind, kind, "MetricDoc kind mismatch for {name}");
    assert_eq!(doc.labels, labels, "MetricDoc labels mismatch for {name}");
    assert_eq!(
        doc.description, help,
        "MetricDoc description mismatch for {name}"
    );
    doc
}

fn elapsed(start: Instant) -> f64 {
    start.elapsed().as_secs_f64()
}

fn result_status<T, E>(result: &Result<T, E>, err_status: &'static str) -> &'static str {
    if result.is_ok() {
        STATUS_OK
    } else {
        err_status
    }
}

#[cfg(feature = "pro")]
fn pg_status<T>(result: &Result<T, PipelineStoreError>) -> &'static str {
    match result {
        Ok(_) => STATUS_OK,
        Err(PipelineStoreError::Database(_)) => STATUS_PG_ERROR,
        Err(_) => STATUS_LAYER_ERROR,
    }
}

#[cfg(feature = "pro")]
fn udf_pg_status<T>(result: &Result<T, UdfStoreError>) -> &'static str {
    match result {
        Ok(_) => STATUS_OK,
        Err(UdfStoreError::Database(_)) => STATUS_PG_ERROR,
        Err(_) => STATUS_LAYER_ERROR,
    }
}

pub fn app_error_status(error: &crate::error::AppError) -> &'static str {
    let message = error.to_string();
    match error {
        crate::error::AppError::Upstream(_) if message.contains("Turbopuffer") => STATUS_TPUF_ERROR,
        crate::error::AppError::Upstream(_) if message.contains("Aerospike") => {
            STATUS_AEROSPIKE_ERROR
        }
        crate::error::AppError::Upstream(_)
            if message.contains("Database")
                || message.contains("PostgreSQL")
                || message.contains("pipeline") =>
        {
            STATUS_PG_ERROR
        }
        _ => STATUS_LAYER_ERROR,
    }
}

pub fn turbopuffer_status<T>(result: &Result<T, TurbopufferError>) -> &'static str {
    result_status(result, STATUS_TPUF_ERROR)
}

pub fn aerospike_status<T>(result: &Result<T, AerospikeError>) -> &'static str {
    match result {
        Ok(_) => STATUS_OK,
        Err(error) if error.kind() == AerospikeErrorKind::StopWrites => {
            STATUS_AEROSPIKE_STOP_WRITES
        }
        Err(_) => STATUS_AEROSPIKE_ERROR,
    }
}

pub fn s3_status<T>(result: &Result<T, S3Error>) -> &'static str {
    result_status(result, STATUS_LAYER_ERROR)
}

pub fn estimate_json_bytes<T: serde::Serialize>(value: &T) -> Option<usize> {
    serde_json::to_vec(value).ok().map(|v| v.len())
}

fn billing_u64(billing: &Value, key: &str) -> Option<u64> {
    billing.get(key).and_then(|value| {
        value.as_u64().or_else(|| {
            value
                .as_f64()
                .filter(|v| v.is_finite() && *v >= 0.0)
                .map(|v| v.round() as u64)
        })
    })
}

fn namespace_from_tpuf_path(path: &str) -> Option<&str> {
    let mut parts = path.trim_start_matches('/').split('/');
    match (parts.next(), parts.next(), parts.next()) {
        (Some("v1" | "v2"), Some("namespaces"), Some(namespace)) if !namespace.is_empty() => {
            Some(namespace)
        }
        _ => None,
    }
}

struct MetricsTurbopufferClient {
    inner: Arc<dyn TurbopufferClient>,
    metrics: Arc<LayerMetrics>,
}

#[async_trait]
impl TurbopufferClient for MetricsTurbopufferClient {
    async fn passthrough(
        &self,
        method: &str,
        path: &str,
        query: Option<&str>,
        body: Option<Value>,
    ) -> Result<crate::clients::turbopuffer::TurbopufferPassthroughResponse, TurbopufferError> {
        let namespace = namespace_from_tpuf_path(path);
        let embedding = namespace.and_then(|namespace| {
            body.as_ref()
                .and_then(|body| native_embedding_model(&self.metrics, namespace, body))
        });
        self.metrics.inc_tpuf_inflight();
        let result = self.inner.passthrough(method, path, query, body).await;
        self.metrics.dec_tpuf_inflight();
        if let Ok(response) = &result {
            if (200..300).contains(&response.status) {
                if let Some(namespace) = namespace {
                    if let Ok(body) = serde_json::from_slice::<Value>(&response.body) {
                        if let Some(billing) = body.get("billing") {
                            self.metrics.observe_tpuf_billing(namespace, billing);
                        }
                        if let (Some((model, serving)), Some(performance)) =
                            (embedding.as_ref(), body.get("performance"))
                        {
                            self.metrics.observe_embed_performance(
                                namespace,
                                model,
                                serving,
                                performance,
                            );
                        }
                        if let Some(bytes) =
                            body.get("approx_logical_bytes").and_then(Value::as_u64)
                        {
                            self.metrics.set_tpuf_logical_bytes(namespace, bytes);
                        }
                    }
                }
            }
        }
        result
    }

    async fn delete_namespace(
        &self,
        namespace: &str,
    ) -> Result<crate::clients::turbopuffer::TurbopufferPassthroughResponse, TurbopufferError> {
        self.metrics.inc_tpuf_inflight();
        let result = self.inner.delete_namespace(namespace).await;
        self.metrics.dec_tpuf_inflight();
        result
    }

    async fn hint_cache_warm(&self, namespace: &str) -> Result<(), TurbopufferError> {
        self.metrics.inc_tpuf_inflight();
        let result = self.inner.hint_cache_warm(namespace).await;
        self.metrics.dec_tpuf_inflight();
        result
    }

    async fn upsert(
        &self,
        namespace: &str,
        docs: &[UpsertDoc],
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        self.metrics.inc_tpuf_inflight();
        let result = self.inner.upsert(namespace, docs).await;
        self.metrics.dec_tpuf_inflight();
        if let Ok(outcome) = &result {
            if let Some(billing) = outcome.billing.as_ref() {
                self.metrics.observe_tpuf_billing(namespace, billing);
            }
        }
        result
    }

    async fn patch(
        &self,
        namespace: &str,
        docs: &[PatchDoc],
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        self.metrics.inc_tpuf_inflight();
        let result = self.inner.patch(namespace, docs).await;
        self.metrics.dec_tpuf_inflight();
        if let Ok(outcome) = &result {
            if let Some(billing) = outcome.billing.as_ref() {
                self.metrics.observe_tpuf_billing(namespace, billing);
            }
        }
        result
    }

    async fn patch_columns(
        &self,
        namespace: &str,
        columns: &PatchColumns,
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        self.metrics.inc_tpuf_inflight();
        let result = self.inner.patch_columns(namespace, columns).await;
        self.metrics.dec_tpuf_inflight();
        if let Ok(outcome) = &result {
            if let Some(billing) = outcome.billing.as_ref() {
                self.metrics.observe_tpuf_billing(namespace, billing);
            }
        }
        result
    }

    async fn delete(
        &self,
        namespace: &str,
        ids: &[String],
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        self.metrics.inc_tpuf_inflight();
        let result = self.inner.delete(namespace, ids).await;
        self.metrics.dec_tpuf_inflight();
        if let Ok(outcome) = &result {
            if let Some(billing) = outcome.billing.as_ref() {
                self.metrics.observe_tpuf_billing(namespace, billing);
            }
        }
        result
    }

    async fn delete_by_filter(
        &self,
        namespace: &str,
        filters: &Value,
    ) -> Result<TurbopufferWriteOutcome, TurbopufferError> {
        self.metrics.inc_tpuf_inflight();
        let result = self.inner.delete_by_filter(namespace, filters).await;
        self.metrics.dec_tpuf_inflight();
        if let Ok(outcome) = &result {
            if let Some(billing) = outcome.billing.as_ref() {
                self.metrics.observe_tpuf_billing(namespace, billing);
            }
        }
        result
    }

    async fn query(
        &self,
        namespace: &str,
        vector: &[f64],
        top_k: u32,
        filters: Option<&Value>,
        include_attributes: Option<&IncludeAttributes>,
    ) -> Result<TurbopufferQueryOutcome, TurbopufferError> {
        self.metrics.inc_tpuf_inflight();
        let result = self
            .inner
            .query(namespace, vector, top_k, filters, include_attributes)
            .await;
        self.metrics.dec_tpuf_inflight();
        if let Ok(outcome) = &result {
            if let Some(billing) = outcome.billing.as_ref() {
                self.metrics.observe_tpuf_billing(namespace, billing);
            }
        }
        result
    }

    async fn ranked_query(
        &self,
        namespace: &str,
        rank_by: &Value,
        top_k: u32,
        filters: Option<&Value>,
        include_attributes: Option<&IncludeAttributes>,
    ) -> Result<TurbopufferQueryOutcome, TurbopufferError> {
        self.metrics.inc_tpuf_inflight();
        let result = self
            .inner
            .ranked_query(namespace, rank_by, top_k, filters, include_attributes)
            .await;
        self.metrics.dec_tpuf_inflight();
        if let Ok(outcome) = &result {
            if let Some(billing) = outcome.billing.as_ref() {
                self.metrics.observe_tpuf_billing(namespace, billing);
            }
        }
        result
    }

    async fn multi_ranked_query(
        &self,
        namespace: &str,
        legs: &[Value],
        rerank_by: Option<&Value>,
    ) -> Result<Value, TurbopufferError> {
        self.metrics.inc_tpuf_inflight();
        let result = self
            .inner
            .multi_ranked_query(namespace, legs, rerank_by)
            .await;
        self.metrics.dec_tpuf_inflight();
        if let Ok(body) = &result {
            if let Some(billing) = body.get("billing") {
                self.metrics.observe_tpuf_billing(namespace, billing);
            }
        }
        result
    }

    async fn fetch(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<DocumentResponse>, TurbopufferError> {
        self.metrics.inc_tpuf_inflight();
        let result = self.inner.fetch(namespace, id).await;
        self.metrics.dec_tpuf_inflight();
        result
    }

    async fn fetch_many(
        &self,
        namespace: &str,
        ids: &[String],
    ) -> Result<HashMap<String, DocumentResponse>, TurbopufferError> {
        self.metrics.inc_tpuf_inflight();
        let result = self.inner.fetch_many(namespace, ids).await;
        self.metrics.dec_tpuf_inflight();
        result
    }

    async fn fetch_vector(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<Vec<f64>>, TurbopufferError> {
        self.metrics.inc_tpuf_inflight();
        let result = self.inner.fetch_vector(namespace, id).await;
        self.metrics.dec_tpuf_inflight();
        result
    }

    async fn scan_page(
        &self,
        namespace: &str,
        cursor: Option<&str>,
        page_size: u32,
        filters: Option<&Value>,
        include_attributes: Option<&[String]>,
    ) -> Result<DocumentPage, TurbopufferError> {
        self.metrics.inc_tpuf_inflight();
        let result = self
            .inner
            .scan_page(namespace, cursor, page_size, filters, include_attributes)
            .await;
        self.metrics.dec_tpuf_inflight();
        result
    }

    async fn head_namespace(&self, namespace: &str) -> Result<NamespaceMeta, TurbopufferError> {
        self.metrics.inc_tpuf_inflight();
        let result = self.inner.head_namespace(namespace).await;
        self.metrics.dec_tpuf_inflight();
        if let Ok(meta) = &result {
            if let Some(bytes) = meta.approx_logical_bytes {
                self.metrics.set_tpuf_logical_bytes(namespace, bytes);
            }
        }
        result
    }
}

fn native_embedding_model(
    metrics: &LayerMetrics,
    namespace: &str,
    body: &Value,
) -> Option<(String, String)> {
    let mut models = body
        .get("schema")
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|schema| schema.iter())
        .filter_map(|(name, attribute)| {
            let embed = attribute.get("embed")?;
            let model = embed
                .as_str()
                .or_else(|| embed.get("model").and_then(Value::as_str))?;
            Some((
                model.to_string(),
                metrics
                    .embed_serving_hint(namespace, name)
                    .unwrap_or_else(|| "native".to_string()),
            ))
        })
        .collect::<std::collections::BTreeSet<_>>();

    let mut collect_rank_by = |rank_by: &Value| {
        let Some(rank_by) = rank_by.as_array() else {
            return;
        };
        let Some(embed) = rank_by.get(2).and_then(Value::as_array) else {
            return;
        };
        if embed.first().and_then(Value::as_str) != Some("Embed") {
            return;
        }
        let attribute = rank_by.first().and_then(Value::as_str);
        let model = embed
            .get(2)
            .and_then(Value::as_object)
            .and_then(|options| options.get("model"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| {
                attribute.and_then(|attribute| metrics.embed_model_hint(namespace, attribute))
            })
            .unwrap_or_else(|| "schema-inferred".to_string());
        let serving = attribute
            .and_then(|attribute| metrics.embed_serving_hint(namespace, attribute))
            .unwrap_or_else(|| "native".to_string());
        models.insert((model, serving));
    };

    if let Some(rank_by) = body.get("rank_by") {
        collect_rank_by(rank_by);
    }
    if let Some(queries) = body.get("queries").and_then(Value::as_array) {
        for query in queries {
            if let Some(rank_by) = query.get("rank_by") {
                collect_rank_by(rank_by);
            }
        }
    }

    match models.len() {
        0 => None,
        1 => models.into_iter().next(),
        _ => Some(("multiple".to_string(), "multiple".to_string())),
    }
}

struct MetricsAerospikeClient {
    inner: Arc<dyn AerospikeClient>,
    metrics: Arc<LayerMetrics>,
    set_prefix: String,
}

impl MetricsAerospikeClient {
    fn set_name(&self, namespace: &str) -> String {
        format!("{}{}", self.set_prefix, namespace)
    }

    fn finish<T>(
        &self,
        start: Instant,
        operation: &str,
        set: &str,
        result: &Result<T, AerospikeError>,
    ) {
        self.metrics.dec_aerospike_inflight();
        self.metrics
            .observe_aerospike_op(operation, set, aerospike_status(result), elapsed(start));
    }
}

#[async_trait]
impl AerospikeClient for MetricsAerospikeClient {
    async fn put(
        &self,
        namespace: &str,
        id: &str,
        doc: &HashMap<String, Value>,
    ) -> Result<(), AerospikeError> {
        let start = Instant::now();
        let set = self.set_name(namespace);
        self.metrics.inc_aerospike_inflight();
        let result = self.inner.put(namespace, id, doc).await;
        self.finish(start, "put", &set, &result);
        result
    }

    async fn put_many(
        &self,
        namespace: &str,
        docs: &HashMap<String, HashMap<String, Value>>,
    ) -> Result<(), AerospikeError> {
        let start = Instant::now();
        let set = self.set_name(namespace);
        self.metrics.inc_aerospike_inflight();
        let result = self.inner.put_many(namespace, docs).await;
        self.finish(start, "put", &set, &result);
        result
    }

    async fn get(
        &self,
        namespace: &str,
        id: &str,
        include_attributes: Option<&[String]>,
    ) -> Result<Option<HashMap<String, Value>>, AerospikeError> {
        let start = Instant::now();
        let set = self.set_name(namespace);
        self.metrics.inc_aerospike_inflight();
        let result = self.inner.get(namespace, id, include_attributes).await;
        self.finish(start, "get", &set, &result);
        result
    }

    async fn get_many(
        &self,
        namespace: &str,
        ids: &[String],
        include_attributes: Option<&[String]>,
    ) -> Result<HashMap<String, HashMap<String, Value>>, AerospikeError> {
        let start = Instant::now();
        let set = self.set_name(namespace);
        self.metrics.inc_aerospike_inflight();
        let result = self
            .inner
            .get_many(namespace, ids, include_attributes)
            .await;
        self.finish(start, "get", &set, &result);
        result
    }

    async fn put_vector(
        &self,
        namespace: &str,
        id: &str,
        vector: &[f64],
    ) -> Result<(), AerospikeError> {
        let start = Instant::now();
        let set = self.set_name(namespace);
        self.metrics.inc_aerospike_inflight();
        let result = self.inner.put_vector(namespace, id, vector).await;
        self.finish(start, "put_vector", &set, &result);
        result
    }

    async fn get_vector(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<Vec<f64>>, AerospikeError> {
        let start = Instant::now();
        let set = self.set_name(namespace);
        self.metrics.inc_aerospike_inflight();
        let result = self.inner.get_vector(namespace, id).await;
        self.finish(start, "get_vector", &set, &result);
        result
    }

    async fn delete(&self, namespace: &str, id: &str) -> Result<(), AerospikeError> {
        let start = Instant::now();
        let set = self.set_name(namespace);
        self.metrics.inc_aerospike_inflight();
        let result = self.inner.delete(namespace, id).await;
        self.finish(start, "delete", &set, &result);
        result
    }

    async fn scan(
        &self,
        namespace: &str,
        include_attributes: Option<&[String]>,
    ) -> Result<Vec<(String, HashMap<String, Value>)>, AerospikeError> {
        let start = Instant::now();
        let set = self.set_name(namespace);
        self.metrics.inc_aerospike_inflight();
        let result = self.inner.scan(namespace, include_attributes).await;
        self.finish(start, "scan", &set, &result);
        result
    }

    async fn put_raw(&self, namespace: &str, key: &str, data: &[u8]) -> Result<(), AerospikeError> {
        let start = Instant::now();
        let set = self.set_name(namespace);
        self.metrics.inc_aerospike_inflight();
        let result = self.inner.put_raw(namespace, key, data).await;
        self.finish(start, "put", &set, &result);
        result
    }

    async fn get_raw(&self, namespace: &str, key: &str) -> Result<Option<Vec<u8>>, AerospikeError> {
        let start = Instant::now();
        let set = self.set_name(namespace);
        self.metrics.inc_aerospike_inflight();
        let result = self.inner.get_raw(namespace, key).await;
        self.finish(start, "get", &set, &result);
        result
    }

    async fn delete_set(&self, namespace: &str) -> Result<(), AerospikeError> {
        let start = Instant::now();
        let set = self.set_name(namespace);
        self.metrics.inc_aerospike_inflight();
        let result = self.inner.delete_set(namespace).await;
        self.finish(start, "delete", &set, &result);
        result
    }

    async fn count_set(&self, namespace: &str) -> Result<u64, AerospikeError> {
        let start = Instant::now();
        let set = self.set_name(namespace);
        self.metrics.inc_aerospike_inflight();
        let result = self.inner.count_set(namespace).await;
        self.finish(start, "scan", &set, &result);
        result
    }
}

struct MetricsS3Client {
    inner: Arc<dyn S3Client>,
    metrics: Arc<LayerMetrics>,
}

#[async_trait]
impl S3Client for MetricsS3Client {
    async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), S3Error> {
        let start = Instant::now();
        let result = self.inner.put(key, body).await;
        self.metrics
            .observe_s3_op("put", s3_status(&result), elapsed(start));
        result
    }

    async fn put_if_not_exists(&self, key: &str, body: Vec<u8>) -> Result<bool, S3Error> {
        let start = Instant::now();
        let result = self.inner.put_if_not_exists(key, body).await;
        self.metrics
            .observe_s3_op("put", s3_status(&result), elapsed(start));
        result
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, S3Error> {
        let start = Instant::now();
        let result = self.inner.get(key).await;
        self.metrics
            .observe_s3_op("get", s3_status(&result), elapsed(start));
        result
    }

    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, S3Error> {
        let start = Instant::now();
        let result = self.inner.list_keys(prefix).await;
        self.metrics
            .observe_s3_op("list", s3_status(&result), elapsed(start));
        result
    }

    async fn delete_key(&self, key: &str) -> Result<(), S3Error> {
        let start = Instant::now();
        let result = self.inner.delete_key(key).await;
        self.metrics
            .observe_s3_op("delete", s3_status(&result), elapsed(start));
        result
    }

    fn is_configured(&self) -> bool {
        self.inner.is_configured()
    }
}

#[cfg(feature = "pro")]
struct MetricsPipelineStore {
    inner: Arc<dyn PipelineStore>,
    metrics: Arc<LayerMetrics>,
    stage_entries: DashMap<String, (String, Instant)>,
}

#[cfg(feature = "pro")]
impl MetricsPipelineStore {
    fn stage_key(pipeline_id: &str, doc_id: &str) -> String {
        format!("{}\u{1f}{}", pipeline_id, doc_id)
    }

    fn observe_doc_transition(
        &self,
        pipeline_id: &str,
        doc_id: &str,
        from_hint: &str,
        to_stage: &str,
    ) {
        let key = Self::stage_key(pipeline_id, doc_id);
        let now = Instant::now();
        let previous = self.stage_entries.insert(key, (to_stage.to_string(), now));
        let (from_stage, duration_seconds) = match previous {
            Some((stage, entered_at)) => (stage, Some(entered_at.elapsed().as_secs_f64())),
            None => (from_hint.to_string(), None),
        };
        self.metrics.observe_stage_transition(
            pipeline_id,
            &from_stage,
            to_stage,
            STATUS_OK,
            1,
            duration_seconds,
        );
    }

    fn observe_count_transition(
        &self,
        pipeline_id: &str,
        doc_ids: &[String],
        from_stage: &str,
        to_stage: &str,
        updated: u64,
    ) {
        if updated == 0 {
            return;
        }

        if updated as usize == doc_ids.len() {
            for doc_id in doc_ids {
                self.observe_doc_transition(pipeline_id, doc_id, from_stage, to_stage);
            }
            return;
        }

        self.metrics.observe_stage_transition(
            pipeline_id,
            from_stage,
            to_stage,
            STATUS_OK,
            updated,
            None,
        );
    }
}

#[cfg(feature = "pro")]
#[async_trait]
impl PipelineStore for MetricsPipelineStore {
    async fn create_pipeline(
        &self,
        id: &str,
        target_namespace: &str,
        distance_metric: &str,
    ) -> Result<Pipeline, PipelineStoreError> {
        let start = Instant::now();
        let result = self
            .inner
            .create_pipeline(id, target_namespace, distance_metric)
            .await;
        self.metrics
            .observe_pg_query("create_pipeline", pg_status(&result), elapsed(start));
        result
    }

    async fn list_pipelines(&self) -> Result<Vec<Pipeline>, PipelineStoreError> {
        let start = Instant::now();
        let result = self.inner.list_pipelines().await;
        self.metrics
            .observe_pg_query("list_pipelines", pg_status(&result), elapsed(start));
        result
    }

    async fn get_pipeline(&self, id: &str) -> Result<Option<Pipeline>, PipelineStoreError> {
        let start = Instant::now();
        let result = self.inner.get_pipeline(id).await;
        self.metrics
            .observe_pg_query("get_pipeline", pg_status(&result), elapsed(start));
        result
    }

    async fn delete_pipeline(&self, id: &str) -> Result<(), PipelineStoreError> {
        let start = Instant::now();
        let result = self.inner.delete_pipeline(id).await;
        self.metrics
            .observe_pg_query("delete_pipeline", pg_status(&result), elapsed(start));
        if result.is_ok() {
            let prefix = format!("{}\u{1f}", id);
            self.stage_entries
                .retain(|key, _| !key.starts_with(&prefix));
        }
        result
    }

    async fn get_pipeline_status(&self, id: &str) -> Result<PipelineStatus, PipelineStoreError> {
        let start = Instant::now();
        let result = self.inner.get_pipeline_status(id).await;
        self.metrics
            .observe_pg_query("status_counts", pg_status(&result), elapsed(start));
        result
    }

    async fn stage_document(
        &self,
        pipeline_id: &str,
        doc_id: &str,
        chunk_count: i32,
        chunk_ids: &[String],
    ) -> Result<(), PipelineStoreError> {
        let start = Instant::now();
        let result = self
            .inner
            .stage_document(pipeline_id, doc_id, chunk_count, chunk_ids)
            .await;
        self.metrics
            .observe_pg_query("stage_document", pg_status(&result), elapsed(start));
        if result.is_ok() {
            self.observe_doc_transition(pipeline_id, doc_id, "none", "pending");
        }
        result
    }

    async fn complete_document(
        &self,
        pipeline_id: &str,
        doc_id: &str,
    ) -> Result<(), PipelineStoreError> {
        let start = Instant::now();
        let result = self.inner.complete_document(pipeline_id, doc_id).await;
        self.metrics
            .observe_pg_query("complete_document", pg_status(&result), elapsed(start));
        if result.is_ok() {
            self.observe_doc_transition(pipeline_id, doc_id, "unknown", "indexed");
        }
        result
    }

    async fn get_document_chunk_ids(
        &self,
        pipeline_id: &str,
        doc_id: &str,
    ) -> Result<Vec<String>, PipelineStoreError> {
        let start = Instant::now();
        let result = self.inner.get_document_chunk_ids(pipeline_id, doc_id).await;
        self.metrics
            .observe_pg_query("get_document_chunk_ids", pg_status(&result), elapsed(start));
        result
    }

    async fn claim_documents(
        &self,
        args: ClaimDocumentsArgs,
    ) -> Result<Vec<String>, PipelineStoreError> {
        let pipeline_id = args.pipeline_id.clone();
        let from_stage = args.stage.clone();
        let to_stage = args.claim_stage.clone();
        let start = Instant::now();
        let result = self.inner.claim_documents(args).await;
        self.metrics
            .observe_pg_query("claim", pg_status(&result), elapsed(start));
        if let Ok(documents) = &result {
            for doc_id in documents {
                self.observe_doc_transition(&pipeline_id, doc_id, &from_stage, &to_stage);
            }
        }
        result
    }

    async fn heartbeat_documents(
        &self,
        pipeline_id: &str,
        document_ids: &[String],
        stage: &str,
        worker_id: &str,
    ) -> Result<u64, PipelineStoreError> {
        let start = Instant::now();
        let result = self
            .inner
            .heartbeat_documents(pipeline_id, document_ids, stage, worker_id)
            .await;
        self.metrics
            .observe_pg_query("heartbeat_documents", pg_status(&result), elapsed(start));
        result
    }

    async fn set_documents_stage(
        &self,
        args: SetDocumentsStageArgs,
    ) -> Result<u64, PipelineStoreError> {
        let pipeline_id = args.pipeline_id.clone();
        let document_ids = args.document_ids.clone();
        let to_stage = args.stage.clone();
        let from_stage = if args.create_missing {
            "none".to_string()
        } else {
            args.from_stage
                .clone()
                .unwrap_or_else(|| "unknown".to_string())
        };
        let start = Instant::now();
        let result = self.inner.set_documents_stage(args).await;
        self.metrics
            .observe_pg_query("set_documents_stage", pg_status(&result), elapsed(start));
        match result {
            Ok(updated) => {
                self.observe_count_transition(
                    &pipeline_id,
                    &document_ids,
                    &from_stage,
                    &to_stage,
                    updated,
                );
                Ok(updated)
            }
            Err(e) => Err(e),
        }
    }

    async fn fail_document_attempt(
        &self,
        args: FailDocumentArgs,
    ) -> Result<bool, PipelineStoreError> {
        let pipeline_id = args.pipeline_id.clone();
        let document_id = args.document_id.clone();
        let start = Instant::now();
        let result = self.inner.fail_document_attempt(args).await;
        self.metrics
            .observe_pg_query("fail_document_attempt", pg_status(&result), elapsed(start));
        if matches!(result, Ok(true)) {
            self.observe_doc_transition(&pipeline_id, &document_id, "unknown", "failed");
        }
        result
    }

    async fn collect_metrics(&self) -> Result<PipelineMetricsSnapshot, PipelineStoreError> {
        let start = Instant::now();
        let result = self.inner.collect_metrics().await;
        self.metrics
            .observe_pg_query("metrics_collect", pg_status(&result), elapsed(start));
        result
    }
}

#[cfg(feature = "pro")]
struct MetricsUdfStore {
    inner: Arc<dyn UdfStore>,
    metrics: Arc<LayerMetrics>,
}

#[cfg(feature = "pro")]
#[async_trait]
impl UdfStore for MetricsUdfStore {
    async fn create_udf(
        &self,
        id: &str,
        spec: &crate::models::UdfSpec,
    ) -> Result<UdfResource, UdfStoreError> {
        let start = Instant::now();
        let result = self.inner.create_udf(id, spec).await;
        self.metrics
            .observe_pg_query("create_udf", udf_pg_status(&result), elapsed(start));
        result
    }

    async fn upsert_udf(
        &self,
        id: &str,
        spec: &crate::models::UdfSpec,
    ) -> Result<UdfResource, UdfStoreError> {
        let start = Instant::now();
        let result = self.inner.upsert_udf(id, spec).await;
        self.metrics
            .observe_pg_query("upsert_udf", udf_pg_status(&result), elapsed(start));
        result
    }

    async fn list_udfs(&self) -> Result<Vec<UdfResource>, UdfStoreError> {
        let start = Instant::now();
        let result = self.inner.list_udfs().await;
        self.metrics
            .observe_pg_query("list_udfs", udf_pg_status(&result), elapsed(start));
        result
    }

    async fn get_udf(&self, id: &str) -> Result<Option<UdfResource>, UdfStoreError> {
        let start = Instant::now();
        let result = self.inner.get_udf(id).await;
        self.metrics
            .observe_pg_query("get_udf", udf_pg_status(&result), elapsed(start));
        result
    }

    async fn delete_udf(&self, id: &str) -> Result<(), UdfStoreError> {
        let start = Instant::now();
        let result = self.inner.delete_udf(id).await;
        self.metrics
            .observe_pg_query("delete_udf", udf_pg_status(&result), elapsed(start));
        result
    }

    async fn set_paused(&self, id: &str, paused: bool) -> Result<UdfResource, UdfStoreError> {
        let start = Instant::now();
        let result = self.inner.set_paused(id, paused).await;
        self.metrics
            .observe_pg_query("set_udf_paused", udf_pg_status(&result), elapsed(start));
        result
    }

    async fn get_status(&self, id: &str) -> Result<UdfStatus, UdfStoreError> {
        let start = Instant::now();
        let result = self.inner.get_status(id).await;
        self.metrics
            .observe_pg_query("udf_status_counts", udf_pg_status(&result), elapsed(start));
        result
    }

    async fn record_discovery_sweep(
        &self,
        id: &str,
    ) -> Result<crate::udf::UdfDiscoveryStatus, UdfStoreError> {
        let start = Instant::now();
        let result = self.inner.record_discovery_sweep(id).await;
        self.metrics.observe_pg_query(
            "record_udf_discovery_sweep",
            udf_pg_status(&result),
            elapsed(start),
        );
        result
    }

    async fn enqueue_items(
        &self,
        udf_id: &str,
        namespace: &str,
        document_ids: &[String],
    ) -> Result<u64, UdfStoreError> {
        let start = Instant::now();
        let result = self
            .inner
            .enqueue_items(udf_id, namespace, document_ids)
            .await;
        self.metrics
            .observe_pg_query("enqueue_udf_items", udf_pg_status(&result), elapsed(start));
        result
    }

    async fn claim_items(
        &self,
        args: ClaimUdfItemsArgs,
    ) -> Result<Vec<crate::udf::UdfWorkItem>, UdfStoreError> {
        let start = Instant::now();
        let result = self.inner.claim_items(args).await;
        self.metrics
            .observe_pg_query("claim_udf_items", udf_pg_status(&result), elapsed(start));
        result
    }

    async fn heartbeat_items(
        &self,
        udf_id: &str,
        worker_id: &str,
        items: &[UdfItemKey],
    ) -> Result<u64, UdfStoreError> {
        let start = Instant::now();
        let result = self.inner.heartbeat_items(udf_id, worker_id, items).await;
        self.metrics.observe_pg_query(
            "heartbeat_udf_items",
            udf_pg_status(&result),
            elapsed(start),
        );
        result
    }

    async fn complete_items(
        &self,
        udf_id: &str,
        worker_id: &str,
        items: &[UdfItemKey],
    ) -> Result<u64, UdfStoreError> {
        let start = Instant::now();
        let result = self.inner.complete_items(udf_id, worker_id, items).await;
        self.metrics
            .observe_pg_query("complete_udf_items", udf_pg_status(&result), elapsed(start));
        result
    }

    async fn fail_items(
        &self,
        udf_id: &str,
        worker_id: &str,
        failures: &[UdfFailure],
        max_attempts: u32,
    ) -> Result<u64, UdfStoreError> {
        let start = Instant::now();
        let result = self
            .inner
            .fail_items(udf_id, worker_id, failures, max_attempts)
            .await;
        self.metrics
            .observe_pg_query("fail_udf_items", udf_pg_status(&result), elapsed(start));
        result
    }

    async fn reset_failed(&self, udf_id: &str) -> Result<u64, UdfStoreError> {
        let start = Instant::now();
        let result = self.inner.reset_failed(udf_id).await;
        self.metrics
            .observe_pg_query("reset_failed_udf", udf_pg_status(&result), elapsed(start));
        result
    }

    async fn collect_metrics(&self) -> Result<UdfMetricsSnapshot, UdfStoreError> {
        let start = Instant::now();
        let result = self.inner.collect_metrics().await;
        self.metrics.observe_pg_query(
            "udf_metrics_collect",
            udf_pg_status(&result),
            elapsed(start),
        );
        result
    }
}

#[cfg(test)]
mod tests {
    use super::{
        aerospike_status, LayerMetrics, STATUS_AEROSPIKE_ERROR, STATUS_AEROSPIKE_STOP_WRITES,
    };
    use crate::clients::aerospike::AerospikeError;
    use serde_json::json;

    #[test]
    fn registered_metrics_must_have_catalog_docs() {
        let _ = LayerMetrics::new();
    }

    #[test]
    fn aerospike_status_distinguishes_stop_writes() {
        assert_eq!(
            aerospike_status::<()>(&Err(AerospikeError::stop_writes(
                "ServerMemError: stop-writes"
            ))),
            STATUS_AEROSPIKE_STOP_WRITES
        );
        assert_eq!(
            aerospike_status::<()>(&Err(AerospikeError::other("connection reset"))),
            STATUS_AEROSPIKE_ERROR
        );
    }

    #[test]
    fn tpuf_billing_metrics_encode_counters_and_storage_gauge() {
        let metrics = LayerMetrics::new();
        metrics.observe_tpuf_billing(
            "ns",
            &json!({
                "billable_logical_bytes_written": 1000,
                "billable_logical_bytes_queried": 2000,
                "billable_logical_bytes_returned": 3000
            }),
        );
        metrics.set_tpuf_logical_bytes("ns", 4000);

        let encoded = metrics.encode().unwrap();
        assert!(encoded.contains(
            "hevlayer_tpuf_billable_bytes_written_total{namespace=\"ns\",store_kind=\"turbopuffer\"} 1000"
        ));
        assert!(encoded.contains(
            "hevlayer_tpuf_billable_bytes_queried_total{namespace=\"ns\",store_kind=\"turbopuffer\"} 2000"
        ));
        assert!(encoded.contains(
            "hevlayer_tpuf_billable_bytes_returned_total{namespace=\"ns\",store_kind=\"turbopuffer\"} 3000"
        ));
        assert!(encoded.contains(
            "hevlayer_tpuf_logical_bytes{namespace=\"ns\",store_kind=\"turbopuffer\"} 4000"
        ));
    }
}
