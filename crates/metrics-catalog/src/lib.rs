use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricFamily {
    Query,
    Upsert,
    Fetch,
    Cache,
    Pipeline,
    Udf,
    Storage,
    Saturation,
    Cost,
    License,
}

impl MetricFamily {
    pub fn as_str(self) -> &'static str {
        match self {
            MetricFamily::Query => "query",
            MetricFamily::Upsert => "upsert",
            MetricFamily::Fetch => "fetch",
            MetricFamily::Cache => "cache",
            MetricFamily::Pipeline => "pipeline",
            MetricFamily::Udf => "udf",
            MetricFamily::Storage => "storage",
            MetricFamily::Saturation => "saturation",
            MetricFamily::Cost => "cost",
            MetricFamily::License => "license",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "query" => MetricFamily::Query,
            "upsert" => MetricFamily::Upsert,
            "fetch" => MetricFamily::Fetch,
            "cache" => MetricFamily::Cache,
            "pipeline" => MetricFamily::Pipeline,
            "udf" => MetricFamily::Udf,
            "storage" => MetricFamily::Storage,
            "saturation" => MetricFamily::Saturation,
            "cost" => MetricFamily::Cost,
            "license" => MetricFamily::License,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct MetricAlert {
    pub summary: &'static str,
    pub expr: &'static str,
    #[serde(rename = "for")]
    pub for_duration: &'static str,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct MetricDoc {
    pub name: &'static str,
    pub kind: MetricKind,
    pub family: MetricFamily,
    pub labels: &'static [&'static str],
    pub description: &'static str,
    pub example_promql: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alert: Option<MetricAlert>,
}

pub fn catalog() -> &'static [MetricDoc] {
    CATALOG
}

pub fn metric(name: &str) -> Option<&'static MetricDoc> {
    CATALOG.iter().find(|doc| doc.name == name)
}

const CATALOG: &[MetricDoc] = &[
    MetricDoc {
        name: "hevlayer_license_valid",
        kind: MetricKind::Gauge,
        family: MetricFamily::License,
        labels: &["license_tier", "license_sub"],
        description: "Whether the configured hevlayer license verifies and is currently valid.",
        example_promql: "hevlayer_license_valid == 0",
        alert: None,
    },
    MetricDoc {
        name: "hevlayer_license_expiry_seconds",
        kind: MetricKind::Gauge,
        family: MetricFamily::License,
        labels: &["license_tier", "license_sub"],
        description: "Seconds until the configured hevlayer license expires.",
        example_promql: "hevlayer_license_expiry_seconds < 1209600",
        alert: None,
    },
    MetricDoc {
        name: "hevlayer_license_grace_seconds_remaining",
        kind: MetricKind::Gauge,
        family: MetricFamily::License,
        labels: &["surface", "license_tier", "license_sub"],
        description: "Seconds remaining before a licensed surface drops to the floor.",
        example_promql: "hevlayer_license_grace_seconds_remaining{surface=\"gateway\"} > 0",
        alert: None,
    },
    MetricDoc {
        name: "hevlayer_license_degraded",
        kind: MetricKind::Gauge,
        family: MetricFamily::License,
        labels: &["surface", "license_tier", "license_sub"],
        description: "Whether a licensed surface has degraded to the open floor.",
        example_promql: "hevlayer_license_degraded == 1",
        alert: None,
    },
    MetricDoc {
        name: "hevlayer_license_grace_requests_total",
        kind: MetricKind::Counter,
        family: MetricFamily::License,
        labels: &[],
        description: "Requests allowed while the pro gateway license is in grace.",
        example_promql: "sum(rate(hevlayer_license_grace_requests_total[5m]))",
        alert: None,
    },
    MetricDoc {
        name: "layer_query_duration_seconds",
        kind: MetricKind::Histogram,
        family: MetricFamily::Query,
        labels: &["pipeline_id", "namespace", "status"],
        description: "Total wall-clock time for a query through layer.",
        example_promql: "histogram_quantile(0.99, sum by (le, namespace) (rate(layer_query_duration_seconds_bucket[5m])))",
        alert: Some(MetricAlert {
            summary: "p99 query latency is above 1s",
            expr: "histogram_quantile(0.99, sum by (le) (rate(layer_query_duration_seconds_bucket[5m]))) > 1",
            for_duration: "10m",
        }),
    },
    MetricDoc {
        name: "layer_query_tpuf_seconds",
        kind: MetricKind::Histogram,
        family: MetricFamily::Query,
        labels: &["pipeline_id", "namespace", "status"],
        description: "Time spent inside the Turbopuffer query call.",
        example_promql: "histogram_quantile(0.99, sum by (le, namespace) (rate(layer_query_tpuf_seconds_bucket[5m])))",
        alert: None,
    },
    MetricDoc {
        name: "layer_query_overhead_seconds",
        kind: MetricKind::Histogram,
        family: MetricFamily::Query,
        labels: &["pipeline_id", "namespace", "status"],
        description: "Layer-side query overhead: total minus Turbopuffer time.",
        example_promql: "histogram_quantile(0.99, sum by (le, namespace) (rate(layer_query_overhead_seconds_bucket[5m])))",
        alert: None,
    },
    MetricDoc {
        name: "layer_upsert_duration_seconds",
        kind: MetricKind::Histogram,
        family: MetricFamily::Upsert,
        labels: &["pipeline_id", "namespace", "status"],
        description: "Total wall-clock time for an upsert through layer.",
        example_promql: "histogram_quantile(0.99, sum by (le, namespace) (rate(layer_upsert_duration_seconds_bucket[5m])))",
        alert: Some(MetricAlert {
            summary: "p99 upsert latency is above 2s",
            expr: "histogram_quantile(0.99, sum by (le) (rate(layer_upsert_duration_seconds_bucket[5m]))) > 2",
            for_duration: "10m",
        }),
    },
    MetricDoc {
        name: "layer_upsert_tpuf_seconds",
        kind: MetricKind::Histogram,
        family: MetricFamily::Upsert,
        labels: &["pipeline_id", "namespace", "status"],
        description: "Time spent inside the Turbopuffer upsert call.",
        example_promql: "histogram_quantile(0.99, sum by (le, namespace) (rate(layer_upsert_tpuf_seconds_bucket[5m])))",
        alert: None,
    },
    MetricDoc {
        name: "layer_upsert_overhead_seconds",
        kind: MetricKind::Histogram,
        family: MetricFamily::Upsert,
        labels: &["pipeline_id", "namespace", "status"],
        description: "Layer-side upsert overhead: total minus Turbopuffer time.",
        example_promql: "histogram_quantile(0.99, sum by (le, namespace) (rate(layer_upsert_overhead_seconds_bucket[5m])))",
        alert: None,
    },
    MetricDoc {
        name: "layer_upsert_batch_size",
        kind: MetricKind::Histogram,
        family: MetricFamily::Upsert,
        labels: &["pipeline_id", "namespace"],
        description: "Documents per upsert request.",
        example_promql: "sum by (namespace) (rate(layer_upsert_batch_size_sum[5m])) / sum by (namespace) (rate(layer_upsert_batch_size_count[5m]))",
        alert: None,
    },
    MetricDoc {
        name: "layer_head_duration_seconds",
        kind: MetricKind::Histogram,
        family: MetricFamily::Query,
        labels: &["pipeline_id", "namespace", "status"],
        description: "Total wall-clock time for a namespace metadata/head call.",
        example_promql: "histogram_quantile(0.99, sum by (le, namespace) (rate(layer_head_duration_seconds_bucket[5m])))",
        alert: None,
    },
    MetricDoc {
        name: "layer_list_duration_seconds",
        kind: MetricKind::Histogram,
        family: MetricFamily::Query,
        labels: &["pipeline_id", "namespace", "status"],
        description: "Total wall-clock time for a namespace list call.",
        example_promql: "histogram_quantile(0.99, sum by (le, namespace) (rate(layer_list_duration_seconds_bucket[5m])))",
        alert: None,
    },
    MetricDoc {
        name: "layer_query_shape_total",
        kind: MetricKind::Counter,
        family: MetricFamily::Query,
        labels: &[
            "pipeline_id",
            "namespace",
            "has_filter",
            "has_rank_by",
            "status",
        ],
        description: "Query-shape distribution.",
        example_promql: "sum by (namespace, has_filter, has_rank_by) (rate(layer_query_shape_total[5m]))",
        alert: None,
    },
    MetricDoc {
        name: "layer_stage_duration_seconds",
        kind: MetricKind::Histogram,
        family: MetricFamily::Pipeline,
        labels: &["pipeline_id", "from_stage", "to_stage"],
        description: "Time a document spent in a pipeline stage before transitioning.",
        example_promql: "histogram_quantile(0.95, sum by (le, pipeline_id, from_stage) (rate(layer_stage_duration_seconds_bucket[10m])))",
        alert: None,
    },
    MetricDoc {
        name: "layer_stage_transitions_total",
        kind: MetricKind::Counter,
        family: MetricFamily::Pipeline,
        labels: &["pipeline_id", "from_stage", "to_stage", "status"],
        description: "Pipeline stage transitions.",
        example_promql: "sum by (pipeline_id, from_stage, to_stage, status) (rate(layer_stage_transitions_total[5m]))",
        alert: None,
    },
    MetricDoc {
        name: "layer_fetch_duration_seconds",
        kind: MetricKind::Histogram,
        family: MetricFamily::Fetch,
        labels: &["operation", "namespace", "cache_result"],
        description: "Document fetch time through the pull-through cache.",
        example_promql: "histogram_quantile(0.99, sum by (le, namespace, cache_result) (rate(layer_fetch_duration_seconds_bucket[5m])))",
        alert: None,
    },
    MetricDoc {
        name: "layer_fetch_batch_size",
        kind: MetricKind::Histogram,
        family: MetricFamily::Fetch,
        labels: &["operation"],
        description: "Documents per batch fetch.",
        example_promql: "sum by (operation) (rate(layer_fetch_batch_size_sum[5m])) / sum by (operation) (rate(layer_fetch_batch_size_count[5m]))",
        alert: None,
    },
    MetricDoc {
        name: "layer_aerospike_op_duration_seconds",
        kind: MetricKind::Histogram,
        family: MetricFamily::Storage,
        labels: &["operation", "set", "status"],
        description: "Raw Aerospike operation duration.",
        example_promql: "histogram_quantile(0.99, sum by (le, operation, status) (rate(layer_aerospike_op_duration_seconds_bucket[5m])))",
        alert: None,
    },
    MetricDoc {
        name: "layer_s3_op_duration_seconds",
        kind: MetricKind::Histogram,
        family: MetricFamily::Storage,
        labels: &["operation", "status"],
        description: "Raw S3 operation duration.",
        example_promql: "histogram_quantile(0.99, sum by (le, operation, status) (rate(layer_s3_op_duration_seconds_bucket[5m])))",
        alert: None,
    },
    MetricDoc {
        name: "layer_pg_query_duration_seconds",
        kind: MetricKind::Histogram,
        family: MetricFamily::Storage,
        labels: &["query_name", "status"],
        description: "Named PostgreSQL query duration.",
        example_promql: "histogram_quantile(0.99, sum by (le, query_name, status) (rate(layer_pg_query_duration_seconds_bucket[5m])))",
        alert: None,
    },
    MetricDoc {
        name: "layer_cache_lookups_total",
        kind: MetricKind::Counter,
        family: MetricFamily::Cache,
        labels: &["set", "namespace", "result"],
        description: "Per-item cache lookups.",
        example_promql: "sum by (namespace, result) (rate(layer_cache_lookups_total[5m]))",
        alert: None,
    },
    MetricDoc {
        name: "layer_cache_lookup_duration_seconds",
        kind: MetricKind::Histogram,
        family: MetricFamily::Cache,
        labels: &["set", "result"],
        description: "Cache lookup/check duration.",
        example_promql: "histogram_quantile(0.99, sum by (le, set, result) (rate(layer_cache_lookup_duration_seconds_bucket[5m])))",
        alert: None,
    },
    MetricDoc {
        name: "layer_cache_backfills_total",
        kind: MetricKind::Counter,
        family: MetricFamily::Cache,
        labels: &["set", "status"],
        description: "Best-effort cache backfill writes.",
        example_promql: "sum by (set, status) (rate(layer_cache_backfills_total[5m]))",
        alert: None,
    },
    MetricDoc {
        name: "layer_cache_backfill_duration_seconds",
        kind: MetricKind::Histogram,
        family: MetricFamily::Cache,
        labels: &["set", "status"],
        description: "Best-effort cache backfill write duration.",
        example_promql: "histogram_quantile(0.99, sum by (le, set, status) (rate(layer_cache_backfill_duration_seconds_bucket[5m])))",
        alert: None,
    },
    MetricDoc {
        name: "layer_cache_payload_bytes",
        kind: MetricKind::Histogram,
        family: MetricFamily::Cache,
        labels: &["set", "result"],
        description: "Cached payload sizes.",
        example_promql: "histogram_quantile(0.99, sum by (le, set) (rate(layer_cache_payload_bytes_bucket[5m])))",
        alert: None,
    },
    MetricDoc {
        name: "hevlayer_cache_demand_total",
        kind: MetricKind::Counter,
        family: MetricFamily::Cache,
        labels: &["namespace", "store_kind"],
        description: "Cache-path requests by logical namespace. Used as the Aerospike scale-up signal.",
        example_promql: "sum by (namespace, store_kind) (rate(hevlayer_cache_demand_total[1m]))",
        alert: None,
    },
    MetricDoc {
        name: "hevlayer_cache_cold_responses_total",
        kind: MetricKind::Counter,
        family: MetricFamily::Cache,
        labels: &["namespace", "store_kind"],
        description: "Cache-path 503 responses caused by a cold or unavailable cache.",
        example_promql: "sum by (namespace, store_kind) (increase(hevlayer_cache_cold_responses_total[5m]))",
        alert: Some(MetricAlert {
            summary: "cache cold responses observed",
            expr: "sum(increase(hevlayer_cache_cold_responses_total[5m])) > 0",
            for_duration: "5m",
        }),
    },
    MetricDoc {
        name: "hevlayer_multi_query_total",
        kind: MetricKind::Counter,
        family: MetricFamily::Query,
        labels: &["namespace", "store_kind", "status"],
        description: "Multi-query requests by namespace and status.",
        example_promql: "sum by (namespace, store_kind, status) (rate(hevlayer_multi_query_total[5m]))",
        alert: None,
    },
    MetricDoc {
        name: "hevlayer_multi_query_legs",
        kind: MetricKind::Histogram,
        family: MetricFamily::Query,
        labels: &["namespace", "store_kind"],
        description: "Leg count per multi-query request.",
        example_promql: "histogram_quantile(0.99, sum by (le, namespace, store_kind) (rate(hevlayer_multi_query_legs_bucket[5m])))",
        alert: None,
    },
    MetricDoc {
        name: "hevlayer_multi_query_upstream_calls",
        kind: MetricKind::Histogram,
        family: MetricFamily::Query,
        labels: &["namespace", "store_kind"],
        description: "Turbopuffer upstream query calls per multi-query request.",
        example_promql: "histogram_quantile(0.99, sum by (le, namespace, store_kind) (rate(hevlayer_multi_query_upstream_calls_bucket[5m])))",
        alert: None,
    },
    MetricDoc {
        name: "hevlayer_hybrid_text_queries_total",
        kind: MetricKind::Counter,
        family: MetricFamily::Query,
        labels: &["namespace", "store_kind", "status"],
        description: "Hybrid text fusion queries by namespace and status.",
        example_promql: "sum by (namespace, store_kind, status) (rate(hevlayer_hybrid_text_queries_total[5m]))",
        alert: None,
    },
    MetricDoc {
        name: "hevlayer_hybrid_text_tokens",
        kind: MetricKind::Histogram,
        family: MetricFamily::Query,
        labels: &["namespace", "store_kind"],
        description: "Token count per hybrid text query after the tokenizer policy.",
        example_promql: "histogram_quantile(0.99, sum by (le, namespace, store_kind) (rate(hevlayer_hybrid_text_tokens_bucket[5m])))",
        alert: None,
    },
    MetricDoc {
        name: "hevlayer_query_router_decisions_total",
        kind: MetricKind::Counter,
        family: MetricFamily::Query,
        labels: &["namespace", "store_kind", "route", "executed"],
        description: "Query router decisions by namespace, chosen route, and whether the route executed (false = vectorless deferral).",
        example_promql: "sum by (namespace, store_kind, route, executed) (rate(hevlayer_query_router_decisions_total[5m]))",
        alert: None,
    },
    MetricDoc {
        name: "hevlayer_agent_queries_total",
        kind: MetricKind::Counter,
        family: MetricFamily::Query,
        labels: &["agent", "status", "deadline_hit"],
        description: "Agentic search requests by agent, status, and whether the deadline was hit.",
        example_promql: "sum by (agent, status, deadline_hit) (rate(hevlayer_agent_queries_total[5m]))",
        alert: None,
    },
    MetricDoc {
        name: "hevlayer_agent_query_duration_seconds",
        kind: MetricKind::Histogram,
        family: MetricFamily::Query,
        labels: &["agent", "status"],
        description: "Total wall-clock time for an agentic search request.",
        example_promql: "histogram_quantile(0.95, sum by (le, agent, status) (rate(hevlayer_agent_query_duration_seconds_bucket[5m])))",
        alert: None,
    },
    MetricDoc {
        name: "hevlayer_agent_turns",
        kind: MetricKind::Histogram,
        family: MetricFamily::Query,
        labels: &["agent", "status"],
        description: "Model turns spent by an agentic search request.",
        example_promql: "histogram_quantile(0.99, sum by (le, agent, status) (rate(hevlayer_agent_turns_bucket[5m])))",
        alert: None,
    },
    MetricDoc {
        name: "hevlayer_agent_tokens_total",
        kind: MetricKind::Counter,
        family: MetricFamily::Query,
        labels: &["agent", "turn", "token_type"],
        description: "Model tokens reported by the inference provider for agentic search.",
        example_promql: "sum by (agent, turn, token_type) (increase(hevlayer_agent_tokens_total[1h]))",
        alert: None,
    },
    MetricDoc {
        name: "hevlayer_tpuf_billable_bytes_written_total",
        kind: MetricKind::Counter,
        family: MetricFamily::Storage,
        labels: &["namespace", "store_kind"],
        description: "Turbopuffer billable logical bytes written, copied from upstream billing objects.",
        example_promql: "sum(increase(hevlayer_tpuf_billable_bytes_written_total[24h]))",
        alert: None,
    },
    MetricDoc {
        name: "hevlayer_tpuf_billable_bytes_queried_total",
        kind: MetricKind::Counter,
        family: MetricFamily::Query,
        labels: &["namespace", "store_kind"],
        description: "Turbopuffer billable logical bytes queried, copied from upstream billing objects.",
        example_promql: "sum(increase(hevlayer_tpuf_billable_bytes_queried_total[24h]))",
        alert: None,
    },
    MetricDoc {
        name: "hevlayer_tpuf_billable_bytes_returned_total",
        kind: MetricKind::Counter,
        family: MetricFamily::Query,
        labels: &["namespace", "store_kind"],
        description: "Turbopuffer billable logical bytes returned, copied from upstream billing objects.",
        example_promql: "sum(increase(hevlayer_tpuf_billable_bytes_returned_total[24h]))",
        alert: None,
    },
    MetricDoc {
        name: "hevlayer_tpuf_logical_bytes",
        kind: MetricKind::Gauge,
        family: MetricFamily::Storage,
        labels: &["namespace", "store_kind"],
        description: "Latest Turbopuffer approx_logical_bytes by namespace from metadata responses.",
        example_promql: "sum(avg_over_time(hevlayer_tpuf_logical_bytes[24h]))",
        alert: None,
    },
    MetricDoc {
        name: "hevlayer_cost_usd_per_hour",
        kind: MetricKind::Gauge,
        family: MetricFamily::Cost,
        labels: &[
            "provider",
            "service",
            "basis",
            "service_detail",
            "region",
            "site",
            "rate_card_version",
        ],
        description: "Gateway-sampled cost run rate by provider and service. Metered and invoice lines are authoritative; estimates are display-only.",
        example_promql: "sum by (provider, service, basis) (hevlayer_cost_usd_per_hour)",
        alert: None,
    },
    MetricDoc {
        name: "hevlayer_scan_total",
        kind: MetricKind::Counter,
        family: MetricFamily::Cache,
        labels: &[
            "namespace",
            "store_kind",
            "mode",
            "selector",
            "served_by",
            "status",
        ],
        description: "Scan requests by namespace, mode, selector, serving path, and status.",
        example_promql: "sum by (namespace, store_kind, mode, selector, served_by, status) (rate(hevlayer_scan_total[5m]))",
        alert: None,
    },
    MetricDoc {
        name: "hevlayer_scan_duration_seconds",
        kind: MetricKind::Histogram,
        family: MetricFamily::Cache,
        labels: &[
            "namespace",
            "store_kind",
            "mode",
            "selector",
            "served_by",
            "status",
        ],
        description: "Wall-clock scan duration by namespace, mode, selector, serving path, and status.",
        example_promql: "histogram_quantile(0.99, sum by (le, namespace, store_kind, mode, selector, served_by) (rate(hevlayer_scan_duration_seconds_bucket[5m])))",
        alert: None,
    },
    MetricDoc {
        name: "hevlayer_scan_documents_scanned",
        kind: MetricKind::Histogram,
        family: MetricFamily::Cache,
        labels: &[
            "namespace",
            "store_kind",
            "mode",
            "selector",
            "served_by",
            "status",
        ],
        description: "Documents scanned per scan request by namespace, mode, selector, serving path, and status.",
        example_promql: "histogram_quantile(0.99, sum by (le, namespace, store_kind, mode, selector, served_by) (rate(hevlayer_scan_documents_scanned_bucket[5m])))",
        alert: None,
    },
    MetricDoc {
        name: "hevlayer_snapshot_field_skipped_total",
        kind: MetricKind::Counter,
        family: MetricFamily::Cache,
        labels: &["namespace", "store_kind", "field", "reason"],
        description: "Snapshot facet fields skipped at materialization time.",
        example_promql: "sum by (namespace, store_kind, field, reason) (increase(hevlayer_snapshot_field_skipped_total[1h]))",
        alert: Some(MetricAlert {
            summary: "snapshot fields are being skipped",
            expr: "sum(increase(hevlayer_snapshot_field_skipped_total[1h])) > 0",
            for_duration: "15m",
        }),
    },
    MetricDoc {
        name: "hevlayer_snapshot_pruned_total",
        kind: MetricKind::Counter,
        family: MetricFamily::Cache,
        labels: &["namespace", "store_kind"],
        description: "Snapshot bodies pruned by retention policy.",
        example_promql: "sum by (namespace, store_kind) (increase(hevlayer_snapshot_pruned_total[1h]))",
        alert: None,
    },
    MetricDoc {
        name: "hevlayer_cache_state",
        kind: MetricKind::Gauge,
        family: MetricFamily::Cache,
        labels: &["namespace", "store_kind", "state"],
        description: "Per-namespace cache state. Exactly one state label should be 1 for a seen namespace.",
        example_promql: "max by (namespace, store_kind, state) (hevlayer_cache_state)",
        alert: None,
    },
    MetricDoc {
        name: "hevlayer_document_cache_cold_starts_total",
        kind: MetricKind::Counter,
        family: MetricFamily::Cache,
        labels: &[],
        description: "Demand-triggered document-cache cold starts completed after Aerospike reconnect.",
        example_promql: "increase(hevlayer_document_cache_cold_starts_total[1h])",
        alert: None,
    },
    MetricDoc {
        name: "hevlayer_document_cache_cold_start_seconds",
        kind: MetricKind::Histogram,
        family: MetricFamily::Cache,
        labels: &[],
        description: "Seconds from first cache-path demand while Aerospike is unavailable until gateway reconnect.",
        example_promql: "histogram_quantile(0.95, sum by (le) (rate(hevlayer_document_cache_cold_start_seconds_bucket[1h])))",
        alert: Some(MetricAlert {
            summary: "document cache cold start is slow",
            expr: "histogram_quantile(0.95, sum by (le) (rate(hevlayer_document_cache_cold_start_seconds_bucket[1h]))) > 120",
            for_duration: "10m",
        }),
    },
    MetricDoc {
        name: "hevlayer_document_cache_cold_start_active",
        kind: MetricKind::Gauge,
        family: MetricFamily::Cache,
        labels: &[],
        description: "Whether a demand-triggered document-cache cold start is currently waiting for Aerospike reconnect.",
        example_promql: "hevlayer_document_cache_cold_start_active",
        alert: Some(MetricAlert {
            summary: "document cache cold start is active",
            expr: "hevlayer_document_cache_cold_start_active == 1",
            for_duration: "5m",
        }),
    },
    MetricDoc {
        name: "layer_pipeline_stage_count",
        kind: MetricKind::Gauge,
        family: MetricFamily::Pipeline,
        labels: &["pipeline_id", "stage"],
        description: "Current document count per pipeline stage.",
        example_promql: "sum by (pipeline_id, stage) (layer_pipeline_stage_count)",
        alert: None,
    },
    MetricDoc {
        name: "layer_pipeline_indexed_total",
        kind: MetricKind::Counter,
        family: MetricFamily::Pipeline,
        labels: &["pipeline_id"],
        description: "Monotonic indexed document count by pipeline.",
        example_promql: "sum by (pipeline_id) (rate(layer_pipeline_indexed_total[1m]))",
        alert: None,
    },
    MetricDoc {
        name: "layer_pipeline_failed_total",
        kind: MetricKind::Counter,
        family: MetricFamily::Pipeline,
        labels: &["pipeline_id", "reason"],
        description: "Documents that landed in failed by pipeline and reason.",
        example_promql: "sum by (pipeline_id, reason) (increase(layer_pipeline_failed_total[5m]))",
        alert: Some(MetricAlert {
            summary: "pipeline failures observed",
            expr: "sum(increase(layer_pipeline_failed_total[5m])) > 0",
            for_duration: "5m",
        }),
    },
    MetricDoc {
        name: "layer_udf_queue_depth",
        kind: MetricKind::Gauge,
        family: MetricFamily::Udf,
        labels: &["udf"],
        description: "Current pending work item count per UDF.",
        example_promql: "sum by (udf) (layer_udf_queue_depth)",
        alert: None,
    },
    MetricDoc {
        name: "layer_udf_stage_count",
        kind: MetricKind::Gauge,
        family: MetricFamily::Udf,
        labels: &["udf", "stage"],
        description: "Current work item count per UDF stage.",
        example_promql: "sum by (udf, stage) (layer_udf_stage_count)",
        alert: None,
    },
    MetricDoc {
        name: "layer_pg_pool_connections",
        kind: MetricKind::Gauge,
        family: MetricFamily::Saturation,
        labels: &["state"],
        description: "Postgres pool connections by state.",
        example_promql: "sum by (state) (layer_pg_pool_connections)",
        alert: Some(MetricAlert {
            summary: "Postgres pool has waiters",
            expr: "sum(layer_pg_pool_connections{state=\"waiting\"}) > 0",
            for_duration: "5m",
        }),
    },
    MetricDoc {
        name: "layer_aerospike_inflight",
        kind: MetricKind::Gauge,
        family: MetricFamily::Saturation,
        labels: &[],
        description: "Current in-flight Aerospike operations.",
        example_promql: "sum(layer_aerospike_inflight)",
        alert: None,
    },
    MetricDoc {
        name: "layer_aerospike_connection_state",
        kind: MetricKind::Gauge,
        family: MetricFamily::Saturation,
        labels: &["state"],
        description: "Aerospike connection state.",
        example_promql: "max by (state) (layer_aerospike_connection_state)",
        alert: Some(MetricAlert {
            summary: "Aerospike connection is failed",
            expr: "max(layer_aerospike_connection_state{state=\"failed\"}) == 1",
            for_duration: "5m",
        }),
    },
    MetricDoc {
        name: "layer_tpuf_inflight",
        kind: MetricKind::Gauge,
        family: MetricFamily::Saturation,
        labels: &[],
        description: "Current in-flight Turbopuffer calls.",
        example_promql: "sum(layer_tpuf_inflight)",
        alert: None,
    },
];

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{catalog, MetricFamily, MetricKind};

    #[test]
    fn metric_names_are_unique() {
        let mut names = BTreeSet::new();
        for doc in catalog() {
            assert!(names.insert(doc.name), "duplicate metric doc {}", doc.name);
        }
    }

    #[test]
    fn metric_docs_have_promql_and_descriptions() {
        for doc in catalog() {
            assert!(
                !doc.description.trim().is_empty(),
                "{} missing description",
                doc.name
            );
            assert!(
                !doc.example_promql.trim().is_empty(),
                "{} missing example_promql",
                doc.name
            );
            match doc.kind {
                MetricKind::Counter | MetricKind::Gauge | MetricKind::Histogram => {}
            }
            match doc.family {
                MetricFamily::Query
                | MetricFamily::Upsert
                | MetricFamily::Fetch
                | MetricFamily::Cache
                | MetricFamily::Pipeline
                | MetricFamily::Udf
                | MetricFamily::Storage
                | MetricFamily::Saturation
                | MetricFamily::Cost
                | MetricFamily::License => {}
            }
        }
    }

    #[test]
    fn metric_family_parse_round_trip() {
        for doc in catalog() {
            let parsed = MetricFamily::parse(doc.family.as_str()).expect("known family");
            assert_eq!(parsed, doc.family);
        }
    }
}
