#[cfg(feature = "pro")]
pub use layer_transform::pipeline::*;

#[cfg(not(feature = "pro"))]
use async_trait::async_trait;
#[cfg(not(feature = "pro"))]
use std::collections::HashMap;

#[cfg(not(feature = "pro"))]
#[derive(Debug, thiserror::Error)]
pub enum PipelineStoreError {
    #[error("pipeline runtime is not enabled")]
    Disabled,
}

#[cfg(not(feature = "pro"))]
#[derive(Debug, Clone)]
pub struct PipelineStatus {
    pub pipeline_id: String,
    pub status: String,
    pub counts: HashMap<String, u64>,
    pub failed_reasons: HashMap<String, u64>,
    pub pending_count: u64,
    pub processing_count: u64,
    pub failed_count: u64,
    pub indexed_rate_per_min: f64,
    pub rate_window_seconds: u64,
}

#[cfg(not(feature = "pro"))]
#[async_trait]
pub trait PipelineStore: Send + Sync {}
