//! Per-namespace consistency snapshots.
//!
//! Every time the consistency watcher observes a `Stable` upstream index, it
//! advances the namespace watermark and fires the `SnapshotTrigger` (see
//! `consistency::ConsistencyWatcher::poll_once`). The trigger spawns
//! [`snapshot_namespace`], which paginates the upstream index once,
//! aggregates configured facet fields client-side, content-hashes the result,
//! and writes a self-contained snapshot object to S3 if the content changed.
//! The latest computed body is also mirrored into Aerospike so hot
//! `field_values` scans can read one K/V record instead of listing/getting
//! from S3 or scanning the document cache.
//!
//! ## S3 layout
//!
//! ```text
//! snapshots/{namespace}/{watermark_ms:013}-{sha7}.json
//! ```
//!
//! Zero-padded `watermark_ms` keeps lexical sort identical to numeric sort,
//! so `list_keys` alone gives chronological order without a separate index
//! file (the S3 client does not support append).
//!
//! ## Aerospike layout
//!
//! ```text
//! set: _hevlayer_snapshots
//! key: latest/{namespace}
//! ```
//!
//! This is an ephemeral read-through mirror of the latest snapshot body. S3
//! remains the source of truth for history.
//!
//! ## Dedup
//!
//! Before writing, the most recent existing snapshot for the namespace is
//! consulted. If its SHA matches the new one, the write is skipped —
//! steady-state "nothing changed" advances do not pollute the history view.
//! `last_snapshot_at` is bumped either way so an unchanged namespace does
//! not re-scan until the floor elapses.
//!
//! ## Cardinality guard
//!
//! Per-field distinct values are capped at [`MAX_FACET_VALUES`]. Anything
//! over the cap is recorded in `fields_skipped[]` and omitted from `fields[]`.
//! Snapshot readers can therefore treat every emitted field as complete.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use dashmap::mapref::entry::Entry;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tracing::{debug, info, warn};

use crate::clients::aerospike::AerospikeClient;
use crate::clients::s3::S3Client;
use crate::clients::turbopuffer::{TurbopufferClient, TurbopufferError};
use crate::consistency::{now_ms, SnapshotTrigger};
use crate::index_config::Retention;
use crate::metrics::LayerMetrics;
use crate::shards::shard_fanout;
use crate::AppState;

/// Maximum distinct values per facet field. Beyond this, the field is skipped
/// rather than partially materialized.
pub const MAX_FACET_VALUES: usize = 10_000;

/// Page size for the upstream scan that backs a snapshot.
pub const SNAPSHOT_SCAN_PAGE_SIZE: u32 = 1000;

/// Logical Aerospike set that stores latest snapshot bodies. The configured
/// Aerospike set prefix is still applied by the client wrapper in production.
pub const SNAPSHOT_CACHE_SET: &str = "_hevlayer_snapshots";

/// Record key for the latest snapshot body of a namespace.
pub fn latest_snapshot_cache_key(namespace: &str) -> String {
    format!("latest/{}", namespace)
}

/// Persisted snapshot body. `sha` is the SHA-256 of the canonical JSON of the
/// `fields` array (sorted) and is the only "content identity" in the system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotBody {
    pub namespace: String,
    pub watermark_ms: u64,
    pub sha: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_count: Option<u64>,
    pub fields: Vec<FieldSummary>,
    #[serde(default)]
    pub fields_skipped: Vec<SnapshotFieldSkipped>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldSummary {
    pub name: String,
    pub values: Vec<ValueCount>,
    /// Compatibility only: old S3 bodies may contain `truncated: true`.
    /// New bodies do not serialize this field, and count readers treat a
    /// legacy truncated field as unavailable.
    #[serde(default, rename = "truncated", skip_serializing)]
    pub legacy_truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueCount {
    pub v: String,
    pub n: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotFieldSkipped {
    pub name: String,
    pub reason: SkipReason,
    pub distinct_observed: u64,
    pub cap: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkipReason {
    ExceededCap,
}

/// Per-snapshot index entry surfaced by `GET /history`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub watermark_ms: u64,
    pub sha: String,
}

#[derive(Debug, Clone)]
pub struct SnapshotCacheWarmOutcome {
    pub key: String,
    pub watermark_ms: u64,
    pub sha: String,
}

/// Trigger backed by [`AppState`]. Constructed at startup and passed to the
/// consistency watcher; calls back into the same `AppState` to spawn the
/// snapshot job under its single-flight and floor guards.
pub struct AppStateSnapshotTrigger {
    state: Arc<AppState>,
}

impl AppStateSnapshotTrigger {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

impl SnapshotTrigger for AppStateSnapshotTrigger {
    fn on_stable(&self, namespace: &str, _watermark_ms: u64) {
        let state = Arc::clone(&self.state);
        let namespace = namespace.to_string();
        tokio::spawn(async move {
            snapshot_namespace(state, namespace).await;
        });
    }
}

/// Run one snapshot for `namespace`. Idempotent and self-rate-limiting via
/// `snapshot_inflight` (single-flight) and `last_snapshot_at` (interval floor).
/// Errors are logged, never returned — the call site is fire-and-forget.
pub async fn snapshot_namespace(state: Arc<AppState>, namespace: String) {
    // Skip namespaces with no facet config — nothing to histogram.
    let facet_fields = match state.facet_fields_for(&namespace) {
        Some(fields) => fields,
        _ => return,
    };

    // Watermark must exist; consistency.rs only fires us after an advance,
    // but a stale spawn could race a reset. Bail rather than guess.
    let watermark_ms = match state.consistency.get(&namespace) {
        Some(w) => w,
        None => return,
    };

    // Floor: bail if we snapshot'd this namespace too recently.
    let now = now_ms();
    let floor_ms = state
        .snapshot_interval_ms_for(&namespace)
        .unwrap_or(state.snapshot_min_interval_ms);
    if let Some(last) = state.last_snapshot_at.get(&namespace).map(|r| *r.value()) {
        if now.saturating_sub(last) < floor_ms {
            debug!(
                namespace = %namespace,
                since_last_ms = now.saturating_sub(last),
                floor_ms,
                "snapshot floor not elapsed; skipping"
            );
            return;
        }
    }

    // Single-flight: bail if another snapshot is already running.
    match state.snapshot_inflight.entry(namespace.clone()) {
        Entry::Occupied(_) => return,
        Entry::Vacant(v) => {
            v.insert(());
        }
    }

    let result = run_snapshot(state.as_ref(), &namespace, watermark_ms, &facet_fields).await;

    match result {
        Ok(outcome) => {
            if let Err(e) =
                cache_latest_snapshot(state.aerospike.as_ref(), &namespace, outcome.body_bytes())
                    .await
            {
                warn!(
                    namespace = %namespace,
                    error = %e,
                    "failed to mirror latest snapshot to Aerospike"
                );
            }

            // Bump `last_snapshot_at` whether the body changed or not — a
            // dedup'd snapshot still counts as "we just checked."
            state.last_snapshot_at.insert(namespace.clone(), now);
            match outcome {
                SnapshotOutcome::Wrote { key, sha, .. } => {
                    info!(namespace = %namespace, key = %key, sha = %sha, "snapshot written");
                }
                SnapshotOutcome::Deduped { sha, .. } => {
                    debug!(namespace = %namespace, sha = %sha, "snapshot unchanged; skipped write");
                }
            }
        }
        Err(e) => {
            warn!(namespace = %namespace, error = %e, "snapshot failed");
        }
    }

    state.snapshot_inflight.remove(&namespace);
}

#[derive(Debug)]
enum SnapshotOutcome {
    Wrote {
        key: String,
        sha: String,
        bytes: Vec<u8>,
    },
    Deduped {
        sha: String,
        bytes: Vec<u8>,
    },
}

impl SnapshotOutcome {
    fn body_bytes(&self) -> &[u8] {
        match self {
            SnapshotOutcome::Wrote { bytes, .. } => bytes,
            SnapshotOutcome::Deduped { bytes, .. } => bytes,
        }
    }
}

async fn run_snapshot(
    state: &AppState,
    namespace: &str,
    watermark_ms: u64,
    facet_fields: &[String],
) -> Result<SnapshotOutcome, String> {
    let aggregation = aggregate_facets(state, namespace, facet_fields, SNAPSHOT_SCAN_PAGE_SIZE)
        .await
        .map_err(|e| format!("upstream scan: {}", e))?;

    let (fields, fields_skipped) = build_field_summaries(facet_fields, &aggregation.counts);
    observe_skipped_fields(state.metrics.as_ref(), namespace, &fields_skipped);
    let sha = compute_content_sha(aggregation.row_count, &fields, &fields_skipped);
    let body = SnapshotBody {
        namespace: namespace.to_string(),
        watermark_ms,
        sha: sha.clone(),
        row_count: Some(aggregation.row_count),
        fields,
        fields_skipped,
    };
    let bytes =
        serde_json::to_vec_pretty(&body).map_err(|e| format!("serialize snapshot: {}", e))?;

    if let Some(prev_key) = latest_snapshot_key(state.s3.as_ref(), namespace)
        .await
        .map_err(|e| format!("s3 list: {}", e))?
    {
        if let Some((_, prev_sha7)) = parse_snapshot_key(&prev_key) {
            if sha.starts_with(&prev_sha7) {
                return Ok(SnapshotOutcome::Deduped { sha, bytes });
            }
        }
    }

    let sha7 = &sha[..7];
    let key = format!("snapshots/{}/{:013}-{}.json", namespace, watermark_ms, sha7);

    state
        .s3
        .put_if_not_exists(&key, bytes.clone())
        .await
        .map_err(|e| format!("s3 put: {}", e))?;

    Ok(SnapshotOutcome::Wrote { key, sha, bytes })
}

/// Persist an already-aggregated snapshot body and return the body with its
/// content SHA stamped. Used by on-demand snapshot jobs that materialize one
/// requested field instead of the full configured facet set.
pub async fn persist_snapshot_body(
    s3: &dyn S3Client,
    namespace: &str,
    watermark_ms: u64,
    row_count: u64,
    mut fields: Vec<FieldSummary>,
    mut fields_skipped: Vec<SnapshotFieldSkipped>,
) -> Result<SnapshotBody, String> {
    fields.sort_by(|a, b| a.name.cmp(&b.name));
    for field in &mut fields {
        field.values.sort_by(|a, b| a.v.cmp(&b.v));
    }
    fields_skipped.sort_by(|a, b| a.name.cmp(&b.name));

    let sha = compute_content_sha(row_count, &fields, &fields_skipped);
    let body = SnapshotBody {
        namespace: namespace.to_string(),
        watermark_ms,
        sha: sha.clone(),
        row_count: Some(row_count),
        fields,
        fields_skipped,
    };
    let bytes =
        serde_json::to_vec_pretty(&body).map_err(|e| format!("serialize snapshot: {}", e))?;
    let sha7 = &sha[..7];
    let key = format!("snapshots/{}/{:013}-{}.json", namespace, watermark_ms, sha7);

    s3.put_if_not_exists(&key, bytes)
        .await
        .map_err(|e| format!("s3 put: {}", e))?;

    Ok(body)
}

/// Best-effort mirror of the latest snapshot body into Aerospike. The caller
/// still persists durable history to S3; this only protects hot scan reads from
/// S3 latency and object-store list pressure.
pub async fn cache_latest_snapshot(
    aerospike: &dyn AerospikeClient,
    namespace: &str,
    bytes: &[u8],
) -> Result<(), String> {
    let key = latest_snapshot_cache_key(namespace);
    aerospike
        .put_raw(SNAPSHOT_CACHE_SET, &key, bytes)
        .await
        .map_err(|e| format!("aerospike put_raw: {}", e))
}

/// Read the Aerospike latest-snapshot mirror. `Ok(None)` means the mirror is
/// cold and the caller should fall back to S3.
pub async fn latest_snapshot_bytes_from_cache(
    aerospike: &dyn AerospikeClient,
    namespace: &str,
) -> Result<Option<Vec<u8>>, String> {
    let key = latest_snapshot_cache_key(namespace);
    aerospike
        .get_raw(SNAPSHOT_CACHE_SET, &key)
        .await
        .map_err(|e| format!("aerospike get_raw: {}", e))
}

/// Load the latest snapshot body, preferring the Aerospike mirror and falling
/// back to S3. Returns `Ok(None)` when no snapshot has been written yet.
pub async fn latest_snapshot_body(
    state: &AppState,
    namespace: &str,
) -> Result<Option<SnapshotBody>, String> {
    match latest_snapshot_bytes_from_cache(state.aerospike.as_ref(), namespace).await {
        Ok(Some(bytes)) => match decode_snapshot_body(namespace, &bytes) {
            Ok(body) => return Ok(Some(body)),
            Err(e) => {
                warn!(
                    namespace = %namespace,
                    error = %e,
                    "cached snapshot body is invalid; falling back to S3"
                );
            }
        },
        Ok(None) => {}
        Err(e) => {
            warn!(
                namespace = %namespace,
                error = %e,
                "Aerospike snapshot lookup failed; falling back to S3"
            );
        }
    }

    let Some(key) = latest_snapshot_key(state.s3.as_ref(), namespace)
        .await
        .map_err(|e| format!("s3 list: {}", e))?
    else {
        return Ok(None);
    };

    let bytes = state
        .s3
        .get(&key)
        .await
        .map_err(|e| format!("s3 get: {}", e))?
        .ok_or_else(|| "snapshot key disappeared between list and get".to_string())?;
    let body = decode_snapshot_body(namespace, &bytes)?;
    if let Err(e) = cache_latest_snapshot(state.aerospike.as_ref(), namespace, &bytes).await {
        warn!(
            namespace = %namespace,
            error = %e,
            "failed to backfill Aerospike snapshot mirror from S3"
        );
    }

    Ok(Some(body))
}

fn decode_snapshot_body(namespace: &str, bytes: &[u8]) -> Result<SnapshotBody, String> {
    let body: SnapshotBody =
        serde_json::from_slice(bytes).map_err(|e| format!("decode snapshot body: {}", e))?;
    if body.namespace != namespace {
        return Err(format!(
            "snapshot namespace mismatch: expected '{}', got '{}'",
            namespace, body.namespace
        ));
    }
    Ok(body)
}

/// Load the latest snapshot body from S3 and mirror it into Aerospike.
/// Returns `Ok(None)` when the namespace has no snapshot history yet.
pub async fn cache_latest_snapshot_from_s3(
    s3: &dyn S3Client,
    aerospike: &dyn AerospikeClient,
    namespace: &str,
) -> Result<Option<SnapshotCacheWarmOutcome>, String> {
    let key = match latest_snapshot_key(s3, namespace)
        .await
        .map_err(|e| format!("s3 list: {}", e))?
    {
        Some(key) => key,
        None => return Ok(None),
    };

    let bytes = s3
        .get(&key)
        .await
        .map_err(|e| format!("s3 get: {}", e))?
        .ok_or_else(|| "snapshot key disappeared between list and get".to_string())?;

    let body: SnapshotBody =
        serde_json::from_slice(&bytes).map_err(|e| format!("decode snapshot body: {}", e))?;
    if body.namespace != namespace {
        return Err(format!(
            "snapshot namespace mismatch: expected '{}', got '{}'",
            namespace, body.namespace
        ));
    }

    cache_latest_snapshot(aerospike, namespace, &bytes).await?;

    Ok(Some(SnapshotCacheWarmOutcome {
        key,
        watermark_ms: body.watermark_ms,
        sha: body.sha,
    }))
}

/// Paginate Turbopuffer once, requesting only the configured facet fields,
/// and aggregate distinct value counts per field.
struct FacetAggregation {
    row_count: u64,
    counts: HashMap<String, HashMap<String, u64>>,
}

async fn aggregate_facets(
    state: &AppState,
    namespace: &str,
    facet_fields: &[String],
    page_size: u32,
) -> Result<FacetAggregation, TurbopufferError> {
    let threads = state.scan_threads_for(namespace);
    let partitions = shard_fanout(state, namespace, threads, |filters| async move {
        aggregate_facets_partition(
            state.turbopuffer(),
            namespace,
            facet_fields,
            page_size,
            filters,
        )
        .await
    })
    .await?;

    let mut merged: HashMap<String, HashMap<String, u64>> = facet_fields
        .iter()
        .map(|f| (f.clone(), HashMap::new()))
        .collect();
    let mut row_count = 0;
    for partition in partitions {
        row_count += partition.row_count;
        for (field, counts) in partition.counts {
            let field_counts = merged.entry(field).or_default();
            for (value, count) in counts {
                *field_counts.entry(value).or_insert(0) += count;
            }
        }
    }

    Ok(FacetAggregation {
        row_count,
        counts: merged,
    })
}

async fn aggregate_facets_partition(
    tpuf: &dyn TurbopufferClient,
    namespace: &str,
    facet_fields: &[String],
    page_size: u32,
    filters: Option<Value>,
) -> Result<FacetAggregation, TurbopufferError> {
    let mut counts: HashMap<String, HashMap<String, u64>> = facet_fields
        .iter()
        .map(|f| (f.clone(), HashMap::new()))
        .collect();
    let include: Vec<String> = facet_fields.to_vec();
    let mut cursor: Option<String> = None;
    let mut row_count = 0;

    loop {
        let page = tpuf
            .scan_page(
                namespace,
                cursor.as_deref(),
                page_size,
                filters.as_ref(),
                Some(&include),
            )
            .await?;

        row_count += page.documents.len() as u64;
        for doc in &page.documents {
            for field in facet_fields {
                if let Some(val) = doc.attributes.get(field) {
                    let Some(per_field) = counts.get_mut(field) else {
                        continue;
                    };
                    // []string columns histogram per element, not per serialized
                    // array. Count each element at most once per document so
                    // facet counts remain document counts.
                    match val {
                        Value::Array(elements) => {
                            let mut seen = std::collections::HashSet::with_capacity(elements.len());
                            for element in elements {
                                let key = value_to_key(element);
                                if seen.insert(key.clone()) {
                                    *per_field.entry(key).or_insert(0) += 1;
                                }
                            }
                        }
                        _ => {
                            *per_field.entry(value_to_key(val)).or_insert(0) += 1;
                        }
                    }
                }
            }
        }

        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }

    Ok(FacetAggregation { row_count, counts })
}

pub async fn run_snapshot_retention_loop(state: Arc<AppState>, interval: Duration) {
    loop {
        if let Err(error) = sweep_snapshot_retention_once(&state).await {
            warn!(error = %error, "snapshot retention sweep failed");
        }
        tokio::time::sleep(interval).await;
    }
}

pub async fn sweep_snapshot_retention_once(state: &AppState) -> Result<usize, String> {
    let mut pruned = 0;
    for (namespace, retention) in state.snapshot_retention_namespaces() {
        let Retention::After(duration) = retention else {
            continue;
        };
        pruned += prune_snapshot_namespace(
            state.s3.as_ref(),
            state.metrics.as_ref(),
            &namespace,
            duration,
        )
        .await?;
    }
    Ok(pruned)
}

async fn prune_snapshot_namespace(
    s3: &dyn S3Client,
    metrics: &LayerMetrics,
    namespace: &str,
    retention: Duration,
) -> Result<usize, String> {
    let prefix = format!("snapshots/{}/", namespace);
    let keys = s3
        .list_keys(&prefix)
        .await
        .map_err(|e| format!("s3 list: {}", e))?;
    let entries: Vec<(String, u64)> = keys
        .into_iter()
        .filter_map(|key| parse_snapshot_key(&key).map(|(watermark_ms, _)| (key, watermark_ms)))
        .collect();
    let Some((latest_key, _)) = entries.last() else {
        return Ok(0);
    };
    let latest_key = latest_key.clone();
    let retention_ms = u64::try_from(retention.as_millis()).unwrap_or(u64::MAX);
    let cutoff_ms = now_ms().saturating_sub(retention_ms);
    let mut pruned = 0;

    for (key, watermark_ms) in entries {
        if key == latest_key || watermark_ms >= cutoff_ms {
            continue;
        }
        s3.delete_key(&key)
            .await
            .map_err(|e| format!("s3 delete {key}: {}", e))?;
        metrics.observe_snapshot_pruned(namespace);
        pruned += 1;
    }

    Ok(pruned)
}

/// Convert a JSON attribute value to a stable string key for histogramming.
/// `Value::String` is unwrapped directly so quoted/unquoted aren't conflated.
fn value_to_key(val: &Value) -> String {
    match val {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Build canonical, sorted `FieldSummary` entries from raw counts, applying
/// the cardinality cap. Fields over the cap are omitted from `fields` and
/// recorded in `fields_skipped`.
fn build_field_summaries(
    facet_fields: &[String],
    counts: &HashMap<String, HashMap<String, u64>>,
) -> (Vec<FieldSummary>, Vec<SnapshotFieldSkipped>) {
    // Canonical order: fields by their declared name (lex sort gives the same
    // result no matter how the operator listed them in config).
    let mut field_names: Vec<&String> = facet_fields.iter().collect();
    field_names.sort();

    let mut fields = Vec::new();
    let mut fields_skipped = Vec::new();
    for name in field_names {
        let empty = HashMap::new();
        let per_field = counts.get(name).unwrap_or(&empty);
        if per_field.len() > MAX_FACET_VALUES {
            fields_skipped.push(SnapshotFieldSkipped {
                name: name.clone(),
                reason: SkipReason::ExceededCap,
                distinct_observed: per_field.len() as u64,
                cap: MAX_FACET_VALUES as u64,
            });
            continue;
        }

        let mut pairs: Vec<(String, u64)> =
            per_field.iter().map(|(v, n)| (v.clone(), *n)).collect();
        pairs.sort_by(|a, b| a.0.cmp(&b.0));

        fields.push(FieldSummary {
            name: name.clone(),
            values: pairs
                .into_iter()
                .map(|(v, n)| ValueCount { v, n })
                .collect(),
            legacy_truncated: false,
        });
    }

    (fields, fields_skipped)
}

fn observe_skipped_fields(
    metrics: &LayerMetrics,
    namespace: &str,
    fields_skipped: &[SnapshotFieldSkipped],
) {
    for skipped in fields_skipped {
        metrics.observe_snapshot_field_skipped(namespace, &skipped.name, skipped.reason.as_str());
    }
}

impl SkipReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            SkipReason::ExceededCap => "exceeded_cap",
        }
    }
}

/// SHA-256 over the canonical JSON of
/// `{row_count, fields: [...], fields_skipped: [...]}`.
/// Namespace and watermark are intentionally excluded — two snapshots with
/// identical facet distributions share a SHA even if their watermarks differ.
fn compute_content_sha(
    row_count: u64,
    fields: &[FieldSummary],
    fields_skipped: &[SnapshotFieldSkipped],
) -> String {
    // Build the canonical body. `serde_json::to_vec` over `Vec<FieldSummary>`
    // is deterministic because all collections involved are `Vec`, not
    // `HashMap`, and the caller has already sorted them.
    let canonical = serde_json::json!({
        "row_count": row_count,
        "fields": fields,
        "fields_skipped": fields_skipped
    });
    let bytes = serde_json::to_vec(&canonical).expect("FieldSummary serializes");
    let digest = Sha256::digest(&bytes);
    format!("{:x}", digest)
}

/// Return the S3 key of the most recently written snapshot for `namespace`,
/// if any. Snapshots are keyed `snapshots/{ns}/{wm:013}-{sha7}.json` so a
/// lexical sort matches chronological order.
pub async fn latest_snapshot_key(
    s3: &dyn S3Client,
    namespace: &str,
) -> Result<Option<String>, crate::clients::s3::S3Error> {
    let prefix = format!("snapshots/{}/", namespace);
    let keys = s3.list_keys(&prefix).await?;
    Ok(keys.into_iter().last())
}

/// Parse a `snapshots/{ns}/{wm:013}-{sha7}.json` key into `(watermark_ms, sha7)`.
/// Returns `None` for any key that doesn't match the expected shape.
pub fn parse_snapshot_key(key: &str) -> Option<(u64, String)> {
    let filename = key.rsplit('/').next()?;
    let stem = filename.strip_suffix(".json")?;
    let (wm_str, sha7) = stem.split_once('-')?;
    let watermark_ms: u64 = wm_str.parse().ok()?;
    Some((watermark_ms, sha7.to_string()))
}

/// Parse a `snapshots/{ns}/{wm:013}-{sha7}.json` key into
/// `(namespace, watermark_ms, sha7)`. Returns `None` for any key that doesn't
/// match the expected shape. Used by the cross-namespace activity feed where
/// the namespace is not implicit from the listing prefix.
pub fn parse_snapshot_key_full(key: &str) -> Option<(String, u64, String)> {
    let rest = key.strip_prefix("snapshots/")?;
    let (namespace, filename) = rest.split_once('/')?;
    if namespace.is_empty() || namespace.contains('/') {
        return None;
    }
    let stem = filename.strip_suffix(".json")?;
    let (wm_str, sha7) = stem.split_once('-')?;
    let watermark_ms: u64 = wm_str.parse().ok()?;
    Some((namespace.to_string(), watermark_ms, sha7.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn build_field_summaries_sorts_values_lex() {
        let fields = vec!["category".to_string()];
        let mut raw: HashMap<String, HashMap<String, u64>> = HashMap::new();
        let mut inner = HashMap::new();
        inner.insert("books".into(), 3);
        inner.insert("apparel".into(), 1);
        inner.insert("music".into(), 2);
        raw.insert("category".into(), inner);

        let (summaries, skipped) = build_field_summaries(&fields, &raw);
        assert_eq!(summaries.len(), 1);
        assert!(skipped.is_empty());
        let vs = &summaries[0].values;
        assert_eq!(
            vs.iter().map(|v| v.v.as_str()).collect::<Vec<_>>(),
            vec!["apparel", "books", "music"]
        );
        assert!(!summaries[0].legacy_truncated);
    }

    #[test]
    fn build_field_summaries_skips_fields_over_cap() {
        let fields = vec!["x".to_string()];
        let mut inner = HashMap::new();
        for i in 0..(MAX_FACET_VALUES + 10) {
            inner.insert(format!("v{:04}", i), (i as u64) + 1);
        }
        let mut raw = HashMap::new();
        raw.insert("x".into(), inner);

        let (summaries, skipped) = build_field_summaries(&fields, &raw);
        assert!(summaries.is_empty());
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].name, "x");
        assert_eq!(skipped[0].distinct_observed, (MAX_FACET_VALUES + 10) as u64);
        assert_eq!(skipped[0].cap, MAX_FACET_VALUES as u64);
    }

    #[test]
    fn compute_content_sha_is_stable_across_input_order() {
        let fields = vec!["category".to_string(), "brand".to_string()];
        let mut raw1: HashMap<String, HashMap<String, u64>> = HashMap::new();
        raw1.entry("category".into())
            .or_default()
            .extend([("a".into(), 1u64), ("b".into(), 2u64)]);
        raw1.entry("brand".into())
            .or_default()
            .extend([("acme".into(), 5u64)]);

        // Same data, declared in reverse order.
        let mut raw2: HashMap<String, HashMap<String, u64>> = HashMap::new();
        raw2.entry("brand".into())
            .or_default()
            .extend([("acme".into(), 5u64)]);
        raw2.entry("category".into())
            .or_default()
            .extend([("b".into(), 2u64), ("a".into(), 1u64)]);

        let (s1, skipped1) = build_field_summaries(&fields, &raw1);
        let (s2, skipped2) = build_field_summaries(&fields, &raw2);
        assert_eq!(
            compute_content_sha(3, &s1, &skipped1),
            compute_content_sha(3, &s2, &skipped2)
        );
    }

    #[test]
    fn compute_content_sha_changes_with_data() {
        let fields = vec!["category".to_string()];
        let mut raw1 = HashMap::new();
        raw1.insert("category".to_string(), {
            let mut m = HashMap::new();
            m.insert("a".to_string(), 1u64);
            m
        });
        let mut raw2 = HashMap::new();
        raw2.insert("category".to_string(), {
            let mut m = HashMap::new();
            m.insert("a".to_string(), 2u64); // count changed
            m
        });
        let (s1, skipped1) = build_field_summaries(&fields, &raw1);
        let (s2, skipped2) = build_field_summaries(&fields, &raw2);
        assert_ne!(
            compute_content_sha(1, &s1, &skipped1),
            compute_content_sha(1, &s2, &skipped2)
        );
    }

    #[test]
    fn compute_content_sha_changes_with_row_count() {
        let fields = vec!["category".to_string()];
        let mut raw = HashMap::new();
        raw.insert("category".to_string(), {
            let mut m = HashMap::new();
            m.insert("a".to_string(), 1u64);
            m
        });
        let (summaries, skipped) = build_field_summaries(&fields, &raw);

        assert_ne!(
            compute_content_sha(1, &summaries, &skipped),
            compute_content_sha(2, &summaries, &skipped)
        );
    }

    #[test]
    fn parse_snapshot_key_round_trips() {
        let key = "snapshots/my-ns/0001731612345000-abc1234.json";
        let (wm, sha) = parse_snapshot_key(key).expect("parses");
        assert_eq!(wm, 1_731_612_345_000);
        assert_eq!(sha, "abc1234");
    }

    #[test]
    fn parse_snapshot_key_rejects_garbage() {
        assert!(parse_snapshot_key("snapshots/my-ns/garbage.json").is_none());
        assert!(parse_snapshot_key("snapshots/my-ns/abc.txt").is_none());
        assert!(parse_snapshot_key("snapshots/my-ns/notanumber-abc.json").is_none());
    }

    #[test]
    fn parse_snapshot_key_full_extracts_namespace() {
        let key = "snapshots/my-ns/0001731612345000-abc1234.json";
        let (ns, wm, sha) = parse_snapshot_key_full(key).expect("parses");
        assert_eq!(ns, "my-ns");
        assert_eq!(wm, 1_731_612_345_000);
        assert_eq!(sha, "abc1234");
    }

    #[test]
    fn parse_snapshot_key_full_rejects_non_snapshot_prefixes() {
        assert!(parse_snapshot_key_full("history/my-ns/0001-abc.json").is_none());
        assert!(parse_snapshot_key_full("snapshots//0001-abc1234.json").is_none());
        assert!(parse_snapshot_key_full("snapshots/my-ns/garbage.json").is_none());
        assert!(parse_snapshot_key_full("snapshots/my-ns/extra/0001-abc1234.json").is_none());
    }

    #[test]
    fn value_to_key_unwraps_strings_but_preserves_others() {
        assert_eq!(value_to_key(&json!("books")), "books");
        assert_eq!(value_to_key(&json!(42)), "42");
        assert_eq!(value_to_key(&json!(true)), "true");
    }
}
