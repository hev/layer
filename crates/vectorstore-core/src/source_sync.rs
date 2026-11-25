//! Durable source sweep health shared by workers and namespace metadata.
use serde::{Deserialize, Serialize};

use crate::s3::{S3Client, S3Error};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceSyncHealth {
    pub last_success_at: Option<String>,
    pub last_attempt_at: String,
    pub last_status: String,
    pub rows_seen: u64,
    pub source_kind: String,
    pub pipeline: String,
}

pub fn key(namespace: &str) -> String {
    use sha2::{Digest, Sha256};
    format!(
        "source-sync/{:x}.json",
        Sha256::digest(namespace.as_bytes())
    )
}

pub async fn read(s3: &dyn S3Client, namespace: &str) -> Result<Option<SourceSyncHealth>, S3Error> {
    s3.get(&key(namespace))
        .await?
        .map(|body| serde_json::from_slice(&body).map_err(|e| S3Error(e.to_string())))
        .transpose()
}

/// Publish only after the sweep and its manifests have completed. A failed
/// persistence operation fails the sweep; it must never claim fresh health.
/// Source workers for one namespace must be serialized by their scheduler.
pub async fn publish(
    s3: &dyn S3Client,
    namespace: &str,
    pipeline: &str,
    source_kind: &str,
    finished_at: String,
    status: &str,
    rows_seen: u64,
) -> Result<(), S3Error> {
    let prior = read(s3, namespace).await?;
    let last_success_at = if status == "succeeded" {
        Some(finished_at.clone())
    } else {
        prior
            .filter(|p| p.pipeline == pipeline && p.source_kind == source_kind)
            .and_then(|p| p.last_success_at)
    };
    let health = SourceSyncHealth {
        last_success_at,
        last_attempt_at: finished_at,
        last_status: status.into(),
        rows_seen,
        source_kind: source_kind.into(),
        pipeline: pipeline.into(),
    };
    s3.put(
        &key(namespace),
        serde_json::to_vec(&health).map_err(|e| S3Error(e.to_string()))?,
    )
    .await
}
