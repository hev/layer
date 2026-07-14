#[cfg(feature = "pro")]
pub use layer_transform::udf::*;

#[cfg(not(feature = "pro"))]
use async_trait::async_trait;
#[cfg(not(feature = "pro"))]
use std::collections::HashMap;

#[cfg(not(feature = "pro"))]
#[derive(Debug, thiserror::Error)]
pub enum UdfStoreError {
    #[error("UDF runtime is not enabled")]
    Disabled,
}

#[cfg(not(feature = "pro"))]
#[derive(Debug, Clone)]
pub struct UdfStatus {
    pub udf_id: String,
    pub paused: bool,
    pub active_namespaces: Vec<String>,
    pub discovery: UdfDiscoveryStatus,
    pub counts: HashMap<String, u64>,
    pub pending_count: u64,
    pub processing_count: u64,
    pub failed_count: u64,
    pub indexed_rate_per_min: f64,
    pub rate_window_seconds: u64,
}

#[cfg(not(feature = "pro"))]
#[derive(Debug, Clone)]
pub struct UdfDiscoveryStatus {
    pub sweeps_completed: u64,
    pub last_completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[cfg(not(feature = "pro"))]
#[async_trait]
pub trait UdfStore: Send + Sync {}
