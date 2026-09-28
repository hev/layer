use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::http::Method;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::{debug, warn};

/// The heartbeat loop wakes hourly and sends when there is usage to report,
/// so a laptop that sleeps most of the day still reports the same day. An
/// idle gateway sends one liveness heartbeat per `IDLE_HEARTBEAT_TICKS`
/// wakeups, which keeps idle installs at the old daily cadence.
const HEARTBEAT_TICK: Duration = Duration::from_secs(60 * 60);
const IDLE_HEARTBEAT_TICKS: u32 = 24;
const MAX_DISTRIBUTION_LEN: usize = 32;

#[derive(Debug, Default)]
pub struct TelemetryCounters {
    auto_routing: AtomicU64,
    hybrid_rrf: AtomicU64,
    fuzzy_surfacing: AtomicU64,
    facets: AtomicU64,
    scans: AtomicU64,
    federated_query: AtomicU64,
    multi_store_routing: AtomicU64,
    gated_command_hit: AtomicU64,
    queries: AtomicU64,
    writes: AtomicU64,
    rows_upserted: AtomicU64,
    rows_patched: AtomicU64,
    rows_deleted: AtomicU64,
}

/// Wire-level usage since the previous heartbeat. Aggregates only: no
/// namespace names, ids, text or request data.
#[derive(Debug, Clone, Copy, Default, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UsageSnapshot {
    pub queries: u64,
    pub writes: u64,
    pub rows_upserted: u64,
    pub rows_patched: u64,
    pub rows_deleted: u64,
}

impl UsageSnapshot {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

/// Row counts from a Turbopuffer-wire write response.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WriteRows {
    pub upserted: u64,
    pub patched: u64,
    pub deleted: u64,
}

/// Which wire surface a request hit, for usage counting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageRoute {
    Query,
    Write,
}

#[derive(Debug, Clone, Copy, Default, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TelemetryCounterSnapshot {
    pub auto_routing: u64,
    pub hybrid_rrf: u64,
    pub fuzzy_surfacing: u64,
    pub facets: u64,
    pub scans: u64,
    pub federated_query: u64,
    pub multi_store_routing: u64,
    #[serde(rename = "gated_command_hit")]
    pub gated_command_hit: u64,
}

impl TelemetryCounters {
    pub fn touch_auto_routing(&self) {
        self.auto_routing.fetch_add(1, Ordering::Relaxed);
    }

    pub fn touch_hybrid_rrf(&self) {
        self.hybrid_rrf.fetch_add(1, Ordering::Relaxed);
    }

    pub fn touch_fuzzy_surfacing(&self) {
        self.fuzzy_surfacing.fetch_add(1, Ordering::Relaxed);
    }

    pub fn touch_facets(&self) {
        self.facets.fetch_add(1, Ordering::Relaxed);
    }

    pub fn touch_scans(&self) {
        self.scans.fetch_add(1, Ordering::Relaxed);
    }

    pub fn touch_federated_query(&self) {
        self.federated_query.fetch_add(1, Ordering::Relaxed);
    }

    pub fn touch_multi_store_routing(&self) {
        self.multi_store_routing.fetch_add(1, Ordering::Relaxed);
    }

    pub fn touch_gated_command_hit(&self) {
        self.gated_command_hit.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_query(&self) {
        self.queries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_write(&self, rows: WriteRows) {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.rows_upserted
            .fetch_add(rows.upserted, Ordering::Relaxed);
        self.rows_patched.fetch_add(rows.patched, Ordering::Relaxed);
        self.rows_deleted.fetch_add(rows.deleted, Ordering::Relaxed);
    }

    /// Reads and resets the usage counters. Feature touches stay cumulative.
    pub fn take_usage(&self) -> UsageSnapshot {
        UsageSnapshot {
            queries: self.queries.swap(0, Ordering::Relaxed),
            writes: self.writes.swap(0, Ordering::Relaxed),
            rows_upserted: self.rows_upserted.swap(0, Ordering::Relaxed),
            rows_patched: self.rows_patched.swap(0, Ordering::Relaxed),
            rows_deleted: self.rows_deleted.swap(0, Ordering::Relaxed),
        }
    }

    /// Puts back usage from a heartbeat that was not delivered, so it rides
    /// on the next one.
    pub fn restore_usage(&self, usage: UsageSnapshot) {
        self.queries.fetch_add(usage.queries, Ordering::Relaxed);
        self.writes.fetch_add(usage.writes, Ordering::Relaxed);
        self.rows_upserted
            .fetch_add(usage.rows_upserted, Ordering::Relaxed);
        self.rows_patched
            .fetch_add(usage.rows_patched, Ordering::Relaxed);
        self.rows_deleted
            .fetch_add(usage.rows_deleted, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> TelemetryCounterSnapshot {
        TelemetryCounterSnapshot {
            auto_routing: self.auto_routing.load(Ordering::Relaxed),
            hybrid_rrf: self.hybrid_rrf.load(Ordering::Relaxed),
            fuzzy_surfacing: self.fuzzy_surfacing.load(Ordering::Relaxed),
            facets: self.facets.load(Ordering::Relaxed),
            scans: self.scans.load(Ordering::Relaxed),
            federated_query: self.federated_query.load(Ordering::Relaxed),
            multi_store_routing: self.multi_store_routing.load(Ordering::Relaxed),
            gated_command_hit: self.gated_command_hit.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Telemetry {
    endpoint: String,
    instance_id: String,
    version: String,
    backend_kinds: Vec<String>,
    distribution: Option<String>,
    distribution_version: Option<String>,
    counters: Arc<TelemetryCounters>,
    http: reqwest::Client,
}

#[derive(Debug, Deserialize, Serialize)]
struct TelemetryState {
    instance_id: String,
}

impl Telemetry {
    pub fn new(
        endpoint: String,
        state_path: Option<PathBuf>,
        backend_kinds: impl IntoIterator<Item = String>,
        counters: Arc<TelemetryCounters>,
    ) -> Option<Self> {
        let instance_id = load_or_create_instance_id(state_path.as_deref());
        let http = match reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
        {
            Ok(http) => http,
            Err(error) => {
                warn!(error = %error, "Telemetry HTTP client initialization failed");
                return None;
            }
        };

        Some(Self {
            endpoint,
            instance_id,
            version: env!("CARGO_PKG_VERSION").to_string(),
            backend_kinds: normalized_backend_kinds(backend_kinds),
            distribution: None,
            distribution_version: None,
            counters,
            http,
        })
    }

    /// Attributes this install to whatever started it (`LAYER_TELEMETRY_SOURCE`
    /// and `LAYER_TELEMETRY_SOURCE_VERSION`). Values outside the documented
    /// shape are dropped, not sent.
    pub fn with_distribution(mut self, source: Option<String>, version: Option<String>) -> Self {
        self.distribution = source.as_deref().and_then(sanitize_distribution);
        self.distribution_version = version.as_deref().and_then(sanitize_distribution_version);
        self
    }

    pub fn spawn(self) {
        tokio::spawn(async move {
            self.send(self.started_payload()).await;
            let mut interval = tokio::time::interval(HEARTBEAT_TICK);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // The first tick completes immediately; heartbeats start an hour in.
            interval.tick().await;
            let mut idle_ticks = 0u32;
            loop {
                interval.tick().await;
                idle_ticks = idle_ticks.saturating_add(1);
                let usage = self.counters.take_usage();
                if usage.is_empty() && idle_ticks < IDLE_HEARTBEAT_TICKS {
                    continue;
                }
                if self.send(self.heartbeat_payload(usage)).await {
                    idle_ticks = 0;
                } else {
                    self.counters.restore_usage(usage);
                }
            }
        });
    }

    fn base_properties(&self) -> serde_json::Map<String, Value> {
        let mut properties = serde_json::Map::new();
        properties.insert("backendKinds".into(), json!(self.backend_kinds));
        if let Some(distribution) = &self.distribution {
            properties.insert("distribution".into(), json!(distribution));
        }
        if let Some(version) = &self.distribution_version {
            properties.insert("distributionVersion".into(), json!(version));
        }
        properties
    }

    pub fn started_payload(&self) -> Value {
        json!({
            "event": "gateway_started",
            "instanceId": self.instance_id,
            "version": self.version,
            "properties": self.base_properties(),
        })
    }

    pub fn heartbeat_payload(&self, usage: UsageSnapshot) -> Value {
        let mut properties = self.base_properties();
        properties.insert("featureTouches".into(), json!(self.counters.snapshot()));
        properties.insert("usage".into(), json!(usage));
        json!({
            "event": "gateway_heartbeat",
            "instanceId": self.instance_id,
            "version": self.version,
            "properties": properties,
        })
    }

    async fn send(&self, payload: Value) -> bool {
        match self.http.post(&self.endpoint).json(&payload).send().await {
            Ok(response) if response.status().is_success() => true,
            Ok(response) => {
                debug!(
                    status = %response.status(),
                    "Telemetry endpoint returned non-success status"
                );
                false
            }
            Err(error) => {
                debug!(error = %error, "Telemetry send failed");
                false
            }
        }
    }
}

/// Classifies a request onto the Turbopuffer wire surfaces that usage counts
/// cover. Every store is served through these routes, so counting here covers
/// all of them.
pub fn classify_usage_route(method: &Method, path: &str) -> Option<UsageRoute> {
    if method != Method::POST {
        return None;
    }
    let segments: Vec<&str> = path.trim_end_matches('/').split('/').collect();
    match segments.as_slice() {
        ["", "v2", "query"] => Some(UsageRoute::Query),
        ["", "v1" | "v2", "namespaces", namespace, "query"] if !namespace.is_empty() => {
            Some(UsageRoute::Query)
        }
        ["", "v2", "namespaces", namespace] if !namespace.is_empty() => Some(UsageRoute::Write),
        ["", "v2", "namespaces", namespace, "import"] if !namespace.is_empty() => {
            Some(UsageRoute::Write)
        }
        _ => None,
    }
}

/// Reads `rows_upserted`, `rows_patched` and `rows_deleted` from a write
/// response body. Missing fields and non-JSON bodies count as zero rows.
pub fn write_rows_from_response(body: &[u8]) -> WriteRows {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return WriteRows::default();
    };
    let count = |key: &str| {
        value
            .get(key)
            .and_then(|v| v.as_u64().or_else(|| v.as_f64().map(|f| f.max(0.0) as u64)))
            .unwrap_or(0)
    };
    WriteRows {
        upserted: count("rows_upserted"),
        patched: count("rows_patched"),
        deleted: count("rows_deleted"),
    }
}

/// Same rule as the telemetry proxy: lowercase `[a-z0-9-]`, 1-32 chars.
fn sanitize_distribution(value: &str) -> Option<String> {
    let value = value.trim().to_ascii_lowercase();
    (!value.is_empty()
        && value.len() <= MAX_DISTRIBUTION_LEN
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'))
    .then_some(value)
}

/// Same rule as the telemetry proxy: `[0-9A-Za-z.+-]`, 1-32 chars.
fn sanitize_distribution_version(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()
        && value.len() <= MAX_DISTRIBUTION_LEN
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'+' | b'-')))
    .then(|| value.to_string())
}

fn load_or_create_instance_id(state_path: Option<&Path>) -> String {
    if let Some(path) = state_path {
        if let Ok(raw) = std::fs::read_to_string(path) {
            if let Ok(state) = serde_json::from_str::<TelemetryState>(&raw) {
                if !state.instance_id.trim().is_empty() {
                    return state.instance_id;
                }
            }
        }
    }

    let instance_id = uuid::Uuid::new_v4().to_string();
    if let Some(path) = state_path {
        if let Err(error) = persist_instance_id(path, &instance_id) {
            warn!(
                path = %path.display(),
                error = %error,
                "Telemetry instance ID could not be persisted; using an ephemeral ID"
            );
        }
    }
    instance_id
}

fn persist_instance_id(path: &Path, instance_id: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let body = serde_json::to_vec_pretty(&TelemetryState {
        instance_id: instance_id.to_string(),
    })?;
    std::fs::write(path, body)
}

fn normalized_backend_kinds(kinds: impl IntoIterator<Item = String>) -> Vec<String> {
    kinds
        .into_iter()
        .filter(|kind| !kind.trim().is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_snapshot_feature_touches() {
        let counters = TelemetryCounters::default();
        counters.touch_auto_routing();
        counters.touch_hybrid_rrf();
        counters.touch_hybrid_rrf();
        counters.touch_multi_store_routing();
        counters.touch_gated_command_hit();

        assert_eq!(
            counters.snapshot(),
            TelemetryCounterSnapshot {
                auto_routing: 1,
                hybrid_rrf: 2,
                multi_store_routing: 1,
                gated_command_hit: 1,
                ..TelemetryCounterSnapshot::default()
            }
        );
    }

    #[test]
    fn payloads_exclude_request_data() {
        let counters = Arc::new(TelemetryCounters::default());
        counters.touch_scans();
        let telemetry = Telemetry::new(
            "http://127.0.0.1:9".to_string(),
            None,
            vec!["search".to_string(), "turbopuffer".to_string()],
            counters,
        )
        .expect("telemetry client");

        let payload = telemetry.heartbeat_payload(UsageSnapshot::default());
        assert_eq!(payload["event"], "gateway_heartbeat");
        assert_eq!(
            payload["properties"]["backendKinds"],
            json!(["search", "turbopuffer"])
        );
        assert_eq!(payload["properties"]["featureTouches"]["scans"], 1);
        assert_eq!(
            payload["properties"]["featureTouches"]["gated_command_hit"],
            0
        );
        assert!(payload.get("namespace").is_none());
        assert!(payload.get("query").is_none());
        assert!(payload.get("apiKey").is_none());
        assert!(payload["properties"].get("distribution").is_none());
        assert!(payload["properties"].get("distributionVersion").is_none());
    }

    fn telemetry_with(counters: Arc<TelemetryCounters>) -> Telemetry {
        Telemetry::new(
            "http://127.0.0.1:9".to_string(),
            None,
            vec!["pgvector".to_string()],
            counters,
        )
        .expect("telemetry client")
    }

    #[test]
    fn usage_counts_reset_on_take_and_restore_on_failed_send() {
        let counters = TelemetryCounters::default();
        counters.record_query();
        counters.record_query();
        counters.record_write(WriteRows {
            upserted: 10,
            patched: 2,
            deleted: 1,
        });
        counters.record_write(WriteRows::default());
        counters.touch_scans();

        let usage = counters.take_usage();
        assert_eq!(
            usage,
            UsageSnapshot {
                queries: 2,
                writes: 2,
                rows_upserted: 10,
                rows_patched: 2,
                rows_deleted: 1,
            }
        );
        assert!(counters.take_usage().is_empty());
        // Feature touches are cumulative and not reset by taking usage.
        assert_eq!(counters.snapshot().scans, 1);

        counters.restore_usage(usage);
        counters.record_query();
        assert_eq!(
            counters.take_usage(),
            UsageSnapshot {
                queries: 3,
                ..usage
            }
        );
    }

    #[test]
    fn heartbeat_carries_distribution_and_usage() {
        let telemetry = telemetry_with(Arc::new(TelemetryCounters::default()))
            .with_distribution(Some(" Kit ".to_string()), Some("0.3.3".to_string()));
        let usage = UsageSnapshot {
            queries: 4,
            writes: 1,
            rows_upserted: 3,
            rows_patched: 0,
            rows_deleted: 0,
        };
        let payload = telemetry.heartbeat_payload(usage);
        assert_eq!(
            payload["properties"],
            json!({
                "backendKinds": ["pgvector"],
                "distribution": "kit",
                "distributionVersion": "0.3.3",
                "featureTouches": TelemetryCounterSnapshot::default(),
                "usage": {
                    "queries": 4,
                    "writes": 1,
                    "rowsUpserted": 3,
                    "rowsPatched": 0,
                    "rowsDeleted": 0,
                },
            })
        );

        let started = telemetry.started_payload();
        assert_eq!(started["event"], "gateway_started");
        assert_eq!(started["properties"]["distribution"], "kit");
        assert_eq!(started["properties"]["distributionVersion"], "0.3.3");
        assert!(started["properties"].get("usage").is_none());
    }

    #[test]
    fn distribution_values_outside_the_contract_are_dropped() {
        assert_eq!(sanitize_distribution("kit").as_deref(), Some("kit"));
        assert_eq!(
            sanitize_distribution("Hev-Kit2").as_deref(),
            Some("hev-kit2")
        );
        assert_eq!(sanitize_distribution(""), None);
        assert_eq!(sanitize_distribution("kit/../etc"), None);
        assert_eq!(sanitize_distribution("kit_dev"), None);
        assert_eq!(sanitize_distribution(&"k".repeat(33)), None);
        assert_eq!(
            sanitize_distribution_version("0.3.3-rc.1+build5").as_deref(),
            Some("0.3.3-rc.1+build5")
        );
        assert_eq!(sanitize_distribution_version("0.3.3 beta"), None);
        assert_eq!(sanitize_distribution_version(&"1".repeat(33)), None);

        let telemetry = telemetry_with(Arc::new(TelemetryCounters::default()))
            .with_distribution(Some("my kit!".to_string()), Some("v1 two".to_string()));
        let started = telemetry.started_payload();
        assert!(started["properties"].get("distribution").is_none());
        assert!(started["properties"].get("distributionVersion").is_none());
    }

    #[test]
    fn usage_routes_cover_the_wire_query_and_write_surfaces() {
        let post = Method::POST;
        assert_eq!(
            classify_usage_route(&post, "/v2/namespaces/docs/query"),
            Some(UsageRoute::Query)
        );
        assert_eq!(
            classify_usage_route(&post, "/v1/namespaces/docs/query"),
            Some(UsageRoute::Query)
        );
        assert_eq!(
            classify_usage_route(&post, "/v2/query"),
            Some(UsageRoute::Query)
        );
        assert_eq!(
            classify_usage_route(&post, "/v2/namespaces/docs"),
            Some(UsageRoute::Write)
        );
        assert_eq!(
            classify_usage_route(&post, "/v2/namespaces/docs/import"),
            Some(UsageRoute::Write)
        );
        assert_eq!(
            classify_usage_route(&post, "/v2/namespaces/docs/init"),
            None
        );
        assert_eq!(
            classify_usage_route(&post, "/v2/namespaces/docs/explain_query"),
            None
        );
        assert_eq!(
            classify_usage_route(&post, "/v1/namespaces/docs/schema"),
            None
        );
        assert_eq!(
            classify_usage_route(&Method::DELETE, "/v2/namespaces/docs"),
            None
        );
        assert_eq!(
            classify_usage_route(&Method::GET, "/v2/namespaces/docs/query"),
            None
        );
    }

    #[test]
    fn write_rows_come_from_the_wire_response() {
        assert_eq!(
            write_rows_from_response(
                br#"{"status":"OK","rows_affected":6,"rows_upserted":3,"rows_patched":2,"rows_deleted":1}"#
            ),
            WriteRows {
                upserted: 3,
                patched: 2,
                deleted: 1,
            }
        );
        assert_eq!(
            write_rows_from_response(br#"{"rows_affected":0}"#),
            WriteRows::default()
        );
        assert_eq!(write_rows_from_response(b"not json"), WriteRows::default());
    }

    #[test]
    fn state_file_reuses_instance_id() {
        let path = std::env::temp_dir().join(format!(
            "hevlayer-telemetry-test-{}.json",
            uuid::Uuid::new_v4()
        ));
        let first = load_or_create_instance_id(Some(&path));
        let second = load_or_create_instance_id(Some(&path));
        let _ = std::fs::remove_file(path);
        assert_eq!(first, second);
    }
}
