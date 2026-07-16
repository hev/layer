use std::cmp::Ordering;
use std::future::Future;

use futures::stream::{self, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::warn;
use xxhash_rust::xxh64::xxh64;

use crate::clients::s3::S3Client;
use crate::clients::turbopuffer::{TurbopufferClient, TurbopufferError, UpsertDoc};
use crate::models::{IncludeAttributes, QueryResult};
use crate::{AppState, SCAN_THREADS_MAX};

pub const DEFAULT_SHARD_COUNT: u64 = 16;
pub const SHARD_ATTR: &str = "_hevlayer_shard";
pub const SCHEMA_VERSION_ATTR: &str = "_hevlayer_schema_version";
pub const SHARD_COUNT_ATTR: &str = "_hevlayer_shard_count";
pub const NAMESPACE_META_ID: &str = "_hevlayer:namespace_meta";

const XXH64_SEED: u64 = 0;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardManifest {
    pub namespace: String,
    pub shard_count: u64,
    pub completed_at: String,
    pub documents_migrated: u64,
}

pub fn shard_for_id(id: &str, shard_count: u64) -> u64 {
    if shard_count == 0 {
        return 0;
    }
    xxh64(id.as_bytes(), XXH64_SEED) % shard_count
}

pub fn shard_filter(shard: u64) -> Value {
    json!([SHARD_ATTR, "Eq", shard])
}

pub fn combine_filter(user: Option<&Value>, extra: Value) -> Value {
    match user {
        Some(user) => json!(["And", [user.clone(), extra]]),
        None => extra,
    }
}

pub fn namespace_meta_exclusion_filter() -> Value {
    json!(["id", "NotEq", NAMESPACE_META_ID])
}

pub fn shard_drain_filter() -> Value {
    json!([
        "And",
        [
            namespace_meta_exclusion_filter(),
            [SHARD_ATTR, "NotExists", true]
        ]
    ])
}

pub async fn read_namespace_marker(
    tpuf: &dyn TurbopufferClient,
    namespace: &str,
) -> Result<Option<u64>, TurbopufferError> {
    let doc = match tpuf.fetch(namespace, NAMESPACE_META_ID).await {
        Ok(doc) => doc,
        Err(error) if error.is_not_found() => return Ok(None),
        Err(error) => return Err(error),
    };
    let Some(doc) = doc else {
        return Ok(None);
    };
    Ok(doc
        .attributes
        .get(SHARD_COUNT_ATTR)
        .and_then(Value::as_u64)
        .filter(|count| *count > 0))
}

pub async fn write_namespace_marker(
    tpuf: &dyn TurbopufferClient,
    namespace: &str,
    shard_count: u64,
    schema_version: u64,
) -> Result<(), TurbopufferError> {
    let mut attributes = std::collections::HashMap::new();
    attributes.insert(SHARD_COUNT_ATTR.to_string(), Value::from(shard_count));
    attributes.insert(SCHEMA_VERSION_ATTR.to_string(), Value::from(schema_version));
    tpuf.upsert(
        namespace,
        &[UpsertDoc {
            id: NAMESPACE_META_ID.to_string(),
            vector: None,
            vectors: None,
            attributes,
        }],
    )
    .await?;
    Ok(())
}

pub async fn shard_fanout<T, E, F, Fut>(
    state: &AppState,
    namespace: &str,
    threads: u32,
    per_shard: F,
) -> Result<Vec<T>, E>
where
    F: Fn(Option<Value>) -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let Some(shard_count) = active_shard_count(state, namespace).await else {
        return per_shard(None).await.map(|outcome| vec![outcome]);
    };

    let shard_cap = u32::try_from(shard_count).unwrap_or(u32::MAX).max(1);
    let width = threads.min(SCAN_THREADS_MAX).min(shard_cap).max(1);
    let per_shard = &per_shard;
    stream::iter(0..shard_count)
        .map(|shard| per_shard(Some(shard_filter(shard))))
        .buffer_unordered(width as usize)
        .try_collect::<Vec<_>>()
        .await
}

pub fn shard_manifest_key(namespace: &str) -> String {
    format!("shards/{namespace}/manifest.json")
}

pub async fn active_shard_count(state: &AppState, namespace: &str) -> Option<u64> {
    if let Some(entry) = state.sharded_namespaces.get(namespace) {
        return Some(*entry.value());
    }

    if let Some(shard_count) = match read_shard_manifest(state.s3.as_ref(), namespace).await {
        Ok(Some(manifest)) => {
            if manifest.shard_count == 0 {
                warn!(
                    namespace = %namespace,
                    "Ignoring shard manifest with zero shard_count"
                );
                return None;
            }
            state
                .sharded_namespaces
                .insert(namespace.to_string(), manifest.shard_count);
            Some(manifest.shard_count)
        }
        Ok(None) => None,
        Err(e) => {
            warn!(
                namespace = %namespace,
                error = %e,
                "Failed to read shard manifest; falling back to single-namespace query"
            );
            None
        }
    } {
        return Some(shard_count);
    }

    marker_ready_shard_count(state, namespace).await
}

pub async fn marker_ready_shard_count(state: &AppState, namespace: &str) -> Option<u64> {
    let shard_count = match read_namespace_marker(state.turbopuffer(), namespace).await {
        Ok(Some(shard_count)) => shard_count,
        Ok(None) => return None,
        Err(e) => {
            warn!(namespace = %namespace, error = %e, "Failed to read namespace shard marker");
            return None;
        }
    };
    match state
        .turbopuffer()
        .scan_page(namespace, None, 1, Some(&shard_drain_filter()), None)
        .await
    {
        Ok(page) if page.documents.is_empty() => {
            state
                .sharded_namespaces
                .insert(namespace.to_string(), shard_count);
            Some(shard_count)
        }
        Ok(_) => None,
        Err(e) => {
            warn!(namespace = %namespace, error = %e, "Failed to check namespace shard lag");
            None
        }
    }
}

pub async fn read_shard_manifest(
    s3: &dyn S3Client,
    namespace: &str,
) -> Result<Option<ShardManifest>, String> {
    let Some(bytes) = s3
        .get(&shard_manifest_key(namespace))
        .await
        .map_err(|e| e.to_string())?
    else {
        return Ok(None);
    };

    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|e| format!("decode shard manifest: {e}"))
}

pub async fn write_shard_manifest(
    s3: &dyn S3Client,
    manifest: &ShardManifest,
) -> Result<bool, String> {
    let body = serde_json::to_vec(manifest).map_err(|e| e.to_string())?;
    s3.put_if_not_exists(&shard_manifest_key(&manifest.namespace), body)
        .await
        .map_err(|e| e.to_string())
}

pub async fn scatter_gather_query(
    tpuf: &dyn TurbopufferClient,
    namespace: &str,
    shard_count: u64,
    vector: &[f64],
    top_k: u32,
    filters: Option<&Value>,
    include_attributes: Option<&IncludeAttributes>,
) -> Result<Vec<QueryResult>, TurbopufferError> {
    let futures = (0..shard_count).map(|shard| {
        let shard_filter = combine_filter(filters, shard_filter(shard));
        async move {
            tpuf.query(
                namespace,
                vector,
                top_k,
                Some(&shard_filter),
                include_attributes,
            )
            .await
            .map(|outcome| outcome.rows)
        }
    });

    let mut rows: Vec<QueryResult> = futures::future::try_join_all(futures)
        .await?
        .into_iter()
        .flatten()
        .collect();
    rows.sort_by(compare_query_results);
    rows.truncate(top_k as usize);
    Ok(rows)
}

fn compare_query_results(a: &QueryResult, b: &QueryResult) -> Ordering {
    match (a.dist, b.dist) {
        (Some(a_dist), Some(b_dist)) => a_dist
            .partial_cmp(&b_dist)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.id.cmp(&b.id)),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => a.id.cmp(&b.id),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_hash_is_stable() {
        assert_eq!(shard_for_id("doc-1", 16), shard_for_id("doc-1", 16));
        assert!(shard_for_id("doc-1", 16) < 16);
    }

    #[test]
    fn shard_filter_ands_with_user_filter() {
        let user = json!(["color", "Eq", "red"]);
        assert_eq!(
            combine_filter(Some(&user), shard_filter(3)),
            json!(["And", [["color", "Eq", "red"], [SHARD_ATTR, "Eq", 3]]])
        );
    }
}
