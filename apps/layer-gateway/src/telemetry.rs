use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::{debug, warn};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60 * 60 * 24);

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
            counters,
            http,
        })
    }

    pub fn spawn(self) {
        tokio::spawn(async move {
            self.send(self.started_payload()).await;
            let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
            loop {
                interval.tick().await;
                self.send(self.heartbeat_payload()).await;
            }
        });
    }

    pub fn started_payload(&self) -> Value {
        json!({
            "event": "gateway_started",
            "instanceId": self.instance_id,
            "version": self.version,
            "properties": {
                "backendKinds": self.backend_kinds,
            }
        })
    }

    pub fn heartbeat_payload(&self) -> Value {
        json!({
            "event": "gateway_heartbeat",
            "instanceId": self.instance_id,
            "version": self.version,
            "properties": {
                "backendKinds": self.backend_kinds,
                "featureTouches": self.counters.snapshot(),
            }
        })
    }

    async fn send(&self, payload: Value) {
        match self.http.post(&self.endpoint).json(&payload).send().await {
            Ok(response) if response.status().is_success() => {}
            Ok(response) => {
                debug!(
                    status = %response.status(),
                    "Telemetry endpoint returned non-success status"
                );
            }
            Err(error) => {
                debug!(error = %error, "Telemetry send failed");
            }
        }
    }
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

        let payload = telemetry.heartbeat_payload();
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
