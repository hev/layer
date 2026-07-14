use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{OriginalUri, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use dashmap::mapref::entry::Entry;
use futures::stream::{self, StreamExt, TryStreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::clients::turbopuffer::{TurbopufferError, UPSERTED_AT_ATTR};
use crate::error::AppError;
use crate::metrics::{
    CACHE_ERROR, CACHE_HIT, CACHE_MISS, STATUS_AEROSPIKE_ERROR, STATUS_LAYER_ERROR, STATUS_OK,
};
use crate::models::{
    AnnScan, CreateExecutorRequest, CreateScanRequest, CreateSnapshotRequest, DocumentPage,
    ExecutorJob, ExecutorKind, ExecutorSource, FieldValueResult, FtsScan, HybridTextScan,
    IncludeAttributes, JobStatus, QueryResult, ScanCountResponse, ScanCountServedBy,
    ScanCountSource, ScanIdsResponse, ScanJob, ScanJobList, ScanMode, ScanSource, ScanValue,
    ScanValuesResponse, SnapshotJob, SnapshotJobList, SnapshotSource, StatusResponse,
    WarmBlobsResponse, WarmCacheOptions, WarmCacheResponse, WarmDocumentsResponse, WarmJob,
    WarmJobList, WarmJobQuery, WarmSnapshotsResponse, WarmStepResponse, WarmStepStatus,
};
use crate::routes::hybrid_text::{
    build_hybrid_leg_specs, build_surfacing_leg_specs, parse_hybrid_text_expr,
    tokenize_query_input, LegSpec,
};
use crate::routes::query::{
    combine_optional, insert_optional_u64_header, temporal_filter, LAYER_STABLE_AS_OF_HEADER,
};
use crate::shards::{
    active_shard_count, combine_filter as combine_shard_filter, shard_fanout, shard_filter,
};
use crate::snapshots::{
    cache_latest_snapshot, cache_latest_snapshot_from_s3, latest_snapshot_bytes_from_cache,
    latest_snapshot_key, persist_snapshot_body, FieldSummary, SkipReason, SnapshotBody,
    SnapshotFieldSkipped, ValueCount, MAX_FACET_VALUES,
};
use crate::{AppState, SCAN_THREADS_MAX};

fn is_valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

fn blob_s3_key(namespace: &str, sha256: &str) -> String {
    format!("blobs/{namespace}/{sha256}")
}

fn blob_cache_set(namespace: &str) -> String {
    format!("blob-cache:{namespace}")
}

fn scan_count_response(response: ScanCountResponse) -> Response {
    let mut headers = axum::http::HeaderMap::new();
    insert_optional_u64_header(
        &mut headers,
        LAYER_STABLE_AS_OF_HEADER,
        response.watermark_ms,
    );
    (headers, Json(response)).into_response()
}

/// Internal state for a running/completed scan.
#[derive(Debug, Clone)]
pub struct JobState {
    pub response: ExecutorJob,
    pub field_results: Option<Vec<FieldValueResult>>,
    pub id_results: Option<Vec<String>>,
    pub snapshot_field: Option<String>,
    pub snapshot_sha: Option<String>,
}

const MAX_SCAN_COUNT_TIMEOUT_SECONDS: u32 = 300;

/// Per-job distinct-value cap for `mode: values` scans. Two orders above the
/// snapshot writer's `MAX_FACET_VALUES` so fields the writer skips for
/// cardinality stay enumerable on demand. Crossing it truncates the listing
/// deterministically (top values by count, value-asc tiebreak) rather than
/// failing; retained counts stay exact because the cap is applied after the
/// full pass.
const MAX_SCAN_VALUES: usize = 1_000_000;

fn now_iso() -> String {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = d.as_secs();
    let nanos = d.subsec_nanos();
    // Good enough ISO timestamp for ephemeral scan state
    format!(
        "{}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        1970 + secs / 31_536_000,
        (secs % 31_536_000) / 2_592_000 + 1,
        (secs % 2_592_000) / 86_400 + 1,
        (secs % 86_400) / 3600,
        (secs % 3600) / 60,
        secs % 60,
        nanos / 1_000_000,
    )
}

fn enqueue_scan(
    state: Arc<AppState>,
    namespace: String,
    request: CreateExecutorRequest,
    origin_threads: u32,
) -> Result<JobState, AppError> {
    // Validate field-aggregating kinds require a field
    if matches!(
        request.kind,
        ExecutorKind::FieldValues | ExecutorKind::Values
    ) && request.field.is_none()
    {
        return Err(AppError::Validation(
            "field is required for field_values scan".to_string(),
        ));
    }

    let source = request.source.unwrap_or_default();
    let page_size = request.page_size.unwrap_or(1000);
    let scan_id = uuid::Uuid::new_v4().to_string();

    let scan_response = ExecutorJob {
        id: scan_id.clone(),
        namespace: namespace.clone(),
        kind: request.kind.clone(),
        source: source.clone(),
        effective_source: None,
        status: JobStatus::Running,
        progress: 0.0,
        documents_scanned: 0,
        field: request.field.clone(),
        unique_values: None,
        truncated: None,
        bounded: None,
        approximate: None,
        stable_as_of: None,
        threads: None,
        created_at: now_iso(),
        completed_at: None,
        error: None,
    };

    let scan_state = JobState {
        response: scan_response,
        field_results: None,
        id_results: None,
        snapshot_field: None,
        snapshot_sha: None,
    };

    state.jobs.insert(scan_id.clone(), scan_state.clone());

    // Spawn the background scan task
    let state_clone = Arc::clone(&state);
    let kind = request.kind;
    let field = request.field;
    let filters = request.filters;
    tokio::spawn(async move {
        execute_scan(
            state_clone,
            scan_id,
            namespace,
            kind,
            source,
            field,
            filters,
            page_size,
            origin_threads,
        )
        .await;
    });

    Ok(scan_state)
}

#[derive(Debug, Deserialize)]
pub struct ScanResultsQuery {
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    offset: Option<usize>,
}

/// POST /v2/namespaces/{namespace}/snapshots
pub async fn create_snapshot_job(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
    Json(request): Json<CreateSnapshotRequest>,
) -> Result<(StatusCode, Json<SnapshotJob>), AppError> {
    let field = request.field.trim().to_string();
    if field.is_empty() {
        return Err(AppError::Validation("field is required".to_string()));
    }
    if matches!(request.source, Some(SnapshotSource::Cache)) {
        state.observe_cache_demand(&namespace);
        if !state.cache_available().await {
            state.observe_cache_cold_response(&namespace);
            return Err(AppError::CacheCold(format!(
                "cache is cold for namespace '{}'; retry after Aerospike reconnects",
                namespace
            )));
        }
    }
    let page_size = request.page_size.unwrap_or(1000);
    validate_page_size(page_size)?;

    let source = request.source.unwrap_or_default();
    let scan_id = uuid::Uuid::new_v4().to_string();
    let scan_response = ExecutorJob {
        id: scan_id.clone(),
        namespace: namespace.clone(),
        kind: ExecutorKind::FieldValues,
        source: source.clone().into(),
        effective_source: None,
        status: JobStatus::Running,
        progress: 0.0,
        documents_scanned: 0,
        field: None,
        unique_values: None,
        truncated: None,
        bounded: None,
        approximate: None,
        stable_as_of: None,
        threads: None,
        created_at: now_iso(),
        completed_at: None,
        error: None,
    };
    let state_entry = JobState {
        response: scan_response,
        field_results: None,
        id_results: None,
        snapshot_field: Some(field.clone()),
        snapshot_sha: None,
    };
    let job = snapshot_job_from_state(&state_entry)
        .ok_or_else(|| AppError::Validation("invalid snapshot job".to_string()))?;
    state.jobs.insert(scan_id.clone(), state_entry);

    let filters = request.filters;
    tokio::spawn(async move {
        execute_snapshot_job(state, scan_id, namespace, field, source, filters, page_size).await;
    });

    Ok((StatusCode::ACCEPTED, Json(job)))
}

/// GET /v2/namespaces/{namespace}/snapshot-jobs
pub async fn list_snapshot_jobs(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
) -> Result<Json<SnapshotJobList>, AppError> {
    let mut snapshot_jobs: Vec<SnapshotJob> = state
        .jobs
        .iter()
        .filter(|entry| entry.value().response.namespace == namespace)
        .filter_map(|entry| snapshot_job_from_state(entry.value()))
        .collect();
    snapshot_jobs.sort_by(|a, b| a.created_at.cmp(&b.created_at));
    Ok(Json(SnapshotJobList { snapshot_jobs }))
}

/// GET /v2/namespaces/{namespace}/snapshot-jobs/{job_id}
pub async fn get_snapshot_job(
    State(state): State<Arc<AppState>>,
    Path((namespace, job_id)): Path<(String, String)>,
) -> Result<Json<SnapshotJob>, AppError> {
    let job = state
        .jobs
        .get(&job_id)
        .ok_or_else(|| AppError::NotFound(format!("Snapshot job '{}' not found", job_id)))?;
    if job.response.namespace != namespace {
        return Err(AppError::NotFound(format!(
            "Snapshot job '{}' not found",
            job_id
        )));
    }
    snapshot_job_from_state(&job)
        .map(Json)
        .ok_or_else(|| AppError::NotFound(format!("Snapshot job '{}' not found", job_id)))
}

/// POST /v2/namespaces/{namespace}/scans
pub async fn create_scan(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
    Json(mut request): Json<CreateScanRequest>,
) -> Result<Response, AppError> {
    let requested_threads = validate_requested_scan_threads(request.threads)?;
    state.telemetry.touch_scans();
    if matches!(request.mode, ScanMode::Values) {
        state.telemetry.touch_facets();
    }
    let temporal_filter = temporal_filter(request.as_of, request.between)?;
    if let Some(temporal_filter) = temporal_filter.as_ref() {
        request.filters = combine_optional(request.filters.as_ref(), Some(&temporal_filter.filter));
    }
    // A scan carries at most one ranked selector. `filters` rides alongside
    // either as the sole selector or as an extra AND constraint.
    let ranked_selectors = [
        request.fts.is_some(),
        request.hybrid_text.is_some(),
        request.ann.is_some(),
    ]
    .into_iter()
    .filter(|present| *present)
    .count();
    if ranked_selectors > 1 {
        return Err(AppError::Validation(
            "fts, hybrid_text, and ann are mutually exclusive; supply at most one ranked selector"
                .to_string(),
        ));
    }
    if !matches!(request.mode, ScanMode::Values) && request.field.is_some() {
        return Err(AppError::Validation(
            "field is only valid with mode \"values\"".to_string(),
        ));
    }
    match request.mode {
        ScanMode::Ids => {
            if ranked_selectors > 0 {
                // Ranked IDs mode is a defined fast-follow; result count only
                // ever counted, so it is not available at this cutover.
                return Err(AppError::Validation(
                    "ranked scans (fts/hybrid_text/ann) support modes \"count\" and \"values\" only; ids mode for ranked selectors is not yet available".to_string(),
                ));
            }
            create_scan_ids(state, namespace, request, requested_threads).await
        }
        ScanMode::Count => create_scan_count(state, namespace, request, requested_threads).await,
        ScanMode::Values => create_scan_values(state, namespace, request, requested_threads).await,
    }
}

fn validate_requested_scan_threads(threads: Option<i32>) -> Result<Option<u32>, AppError> {
    match threads {
        Some(value) if value < 1 => Err(AppError::Validation("threads must be >= 1".to_string())),
        Some(value) => Ok(Some(value as u32)),
        None => Ok(None),
    }
}

async fn resolve_scan_threads(
    state: &AppState,
    namespace: &str,
    requested_threads: Option<u32>,
) -> u32 {
    let active = active_shard_count(state, namespace).await;
    resolve_scan_threads_with_active(state, namespace, requested_threads, active)
}

pub(crate) fn resolve_scan_threads_with_active(
    state: &AppState,
    namespace: &str,
    requested_threads: Option<u32>,
    active_shards: Option<u64>,
) -> u32 {
    let configured = requested_threads.unwrap_or_else(|| state.scan_threads_for(namespace));
    let shard_cap = active_shards
        .and_then(|count| u32::try_from(count).ok())
        .unwrap_or(u32::MAX)
        .max(1);
    configured.min(SCAN_THREADS_MAX).min(shard_cap).max(1)
}

async fn create_scan_ids(
    state: Arc<AppState>,
    namespace: String,
    request: CreateScanRequest,
    requested_threads: Option<u32>,
) -> Result<Response, AppError> {
    let source = match scan_source_for_ids(request.source.unwrap_or_default()) {
        Ok(source) => source,
        Err(err) => {
            state.metrics.observe_scan(
                &namespace,
                "ids",
                "filter",
                "snapshot",
                STATUS_LAYER_ERROR,
                0.0,
                0,
            );
            return Err(err);
        }
    };
    if matches!(source, ScanSource::Cache) {
        state.observe_cache_demand(&namespace);
        if !state.cache_available().await {
            state.observe_cache_cold_response(&namespace);
            state.metrics.observe_scan(
                &namespace,
                "ids",
                "filter",
                "cache",
                STATUS_AEROSPIKE_ERROR,
                0.0,
                0,
            );
            return Err(AppError::CacheCold(format!(
                "cache is cold for namespace '{}'; retry after Aerospike reconnects",
                namespace
            )));
        }
    }
    let origin_threads = resolve_scan_threads(&state, &namespace, requested_threads).await;
    let job_state = enqueue_scan(
        state,
        namespace,
        CreateExecutorRequest {
            kind: ExecutorKind::Ids,
            field: None,
            source: Some(source.into()),
            filters: request.filters,
            page_size: request.page_size,
        },
        origin_threads,
    )?;
    scan_job_from_state(&job_state)
        .map(|job| (StatusCode::ACCEPTED, Json(job)).into_response())
        .ok_or_else(|| AppError::Validation("invalid scan job".to_string()))
}

/// `mode: values` create path — enumerate one attribute field's distinct
/// values over the selector's match set. Snapshot serving happens inline so a
/// precomputed listing completes during the create call; everything else is a
/// background job like ID mode.
async fn create_scan_values(
    state: Arc<AppState>,
    namespace: String,
    request: CreateScanRequest,
    requested_threads: Option<u32>,
) -> Result<Response, AppError> {
    let field = match request.field.as_deref().map(str::trim) {
        Some(f) if !f.is_empty() => f.to_string(),
        _ => {
            return Err(AppError::Validation(
                "field is required for mode \"values\"".to_string(),
            ))
        }
    };

    if request.fts.is_some() || request.hybrid_text.is_some() || request.ann.is_some() {
        return create_ranked_scan_values(state, namespace, request, field, requested_threads)
            .await;
    }

    let source = request.source.unwrap_or_default();
    let start = Instant::now();

    if matches!(source, ScanCountSource::Snapshot) {
        if request.filters.is_some() {
            return Err(AppError::PreconditionFailed(
                "snapshot-served values scans cannot carry filters; use source=cache or source=origin"
                    .to_string(),
            ));
        }
        if !state.has_facet_field(&namespace, &field) {
            return Err(AppError::PreconditionFailed(format!(
                "field '{}' is not pre-aggregated in snapshots for namespace '{}'; add it to Index.spec.snapshot.facetFields",
                field, namespace
            )));
        }
        return match serve_values_from_snapshot(&state, &namespace, &field, &source).await {
            Ok(job) => {
                state.metrics.observe_scan(
                    &namespace,
                    "values",
                    "filter",
                    "snapshot",
                    STATUS_OK,
                    start.elapsed().as_secs_f64(),
                    0,
                );
                Ok((StatusCode::ACCEPTED, Json(job)).into_response())
            }
            Err(e) => {
                state.metrics.observe_scan(
                    &namespace,
                    "values",
                    "filter",
                    "snapshot",
                    STATUS_LAYER_ERROR,
                    start.elapsed().as_secs_f64(),
                    0,
                );
                Err(AppError::PreconditionFailed(e))
            }
        };
    }

    // Auto-mode tries the precomputed listing first, exactly like count mode,
    // but inline so the 202 body already shows the completed job. Any failure
    // falls through to the live scan policy.
    if matches!(source, ScanCountSource::Auto)
        && request.filters.is_none()
        && state.has_facet_field(&namespace, &field)
    {
        if let Ok(job) = serve_values_from_snapshot(&state, &namespace, &field, &source).await {
            state.metrics.observe_scan(
                &namespace,
                "values",
                "filter",
                "snapshot",
                STATUS_OK,
                start.elapsed().as_secs_f64(),
                0,
            );
            return Ok((StatusCode::ACCEPTED, Json(job)).into_response());
        }
    }

    let executor_source = match source {
        ScanCountSource::Auto => ExecutorSource::Auto,
        ScanCountSource::Cache => ExecutorSource::Cache,
        ScanCountSource::Origin => ExecutorSource::Origin,
        ScanCountSource::Snapshot => unreachable!("snapshot source handled above"),
    };
    if matches!(executor_source, ExecutorSource::Cache) {
        state.observe_cache_demand(&namespace);
        if !state.cache_available().await {
            state.observe_cache_cold_response(&namespace);
            state.metrics.observe_scan(
                &namespace,
                "values",
                "filter",
                "cache",
                STATUS_AEROSPIKE_ERROR,
                0.0,
                0,
            );
            return Err(AppError::CacheCold(format!(
                "cache is cold for namespace '{}'; retry after Aerospike reconnects",
                namespace
            )));
        }
    }
    let origin_threads = resolve_scan_threads(&state, &namespace, requested_threads).await;
    let job_state = enqueue_scan(
        state,
        namespace,
        CreateExecutorRequest {
            kind: ExecutorKind::Values,
            field: Some(field),
            source: Some(executor_source),
            filters: request.filters,
            page_size: request.page_size,
        },
        origin_threads,
    )?;
    scan_job_from_state(&job_state)
        .map(|job| (StatusCode::ACCEPTED, Json(job)).into_response())
        .ok_or_else(|| AppError::Validation("invalid scan job".to_string()))
}

/// Serve a values scan from the latest snapshot's pre-aggregated histogram.
/// The job is inserted already completed, so the `202` body carries the
/// final state and results are readable immediately.
async fn serve_values_from_snapshot(
    state: &AppState,
    namespace: &str,
    field: &str,
    requested: &ScanCountSource,
) -> Result<ScanJob, String> {
    let body = load_snapshot_body(state, namespace, None).await?;
    let (documents_scanned, results) = field_results_from_snapshot_body(&body, field)?;

    let scan_id = uuid::Uuid::new_v4().to_string();
    let created_at = now_iso();
    let response = ExecutorJob {
        id: scan_id.clone(),
        namespace: namespace.to_string(),
        kind: ExecutorKind::Values,
        source: match requested {
            ScanCountSource::Snapshot => ExecutorSource::Snapshot,
            _ => ExecutorSource::Auto,
        },
        effective_source: Some(ExecutorSource::Snapshot),
        status: JobStatus::Completed,
        progress: 1.0,
        documents_scanned,
        field: Some(field.to_string()),
        unique_values: Some(results.len() as u64),
        truncated: Some(false),
        bounded: None,
        approximate: None,
        stable_as_of: Some(body.watermark_ms),
        threads: None,
        created_at: created_at.clone(),
        completed_at: Some(created_at),
        error: None,
    };
    let job_state = JobState {
        response,
        field_results: Some(results),
        id_results: None,
        snapshot_field: None,
        snapshot_sha: Some(body.sha),
    };
    let job = scan_job_from_state(&job_state).ok_or_else(|| "invalid scan job".to_string())?;
    state.jobs.insert(scan_id, job_state);
    info!(
        namespace = %namespace,
        field = %field,
        sha = ?job.snapshot_sha,
        "values scan served from snapshot"
    );
    Ok(job)
}

async fn create_scan_count(
    state: Arc<AppState>,
    namespace: String,
    request: CreateScanRequest,
    requested_threads: Option<u32>,
) -> Result<Response, AppError> {
    if request.timeout_seconds == 0 {
        return Err(AppError::Validation(
            "timeout_seconds must be > 0".to_string(),
        ));
    }
    // Ranked selectors (fts / hybrid_text / ann) run origin scatter/gather; the filter
    // count path below never sees them.
    if request.fts.is_some() || request.hybrid_text.is_some() || request.ann.is_some() {
        return create_ranked_scan_count(state, namespace, request, requested_threads).await;
    }
    let page_size = request.page_size.unwrap_or(1000);
    validate_page_size(page_size)?;

    let source = request.source.unwrap_or_default();
    let start = Instant::now();

    if matches!(source, ScanCountSource::Auto | ScanCountSource::Snapshot) {
        match try_count_from_snapshot(&state, &namespace, request.filters.as_ref()).await {
            Ok(Some(hit)) => {
                state.metrics.observe_scan(
                    &namespace,
                    "count",
                    "filter",
                    "snapshot",
                    STATUS_OK,
                    start.elapsed().as_secs_f64(),
                    0,
                );
                return Ok(scan_count_response(ScanCountResponse {
                    count: hit.count,
                    served_by: ScanCountServedBy::Snapshot,
                    snapshot_sha: Some(hit.sha),
                    watermark_ms: Some(hit.watermark_ms),
                    bounded: None,
                    timed_out: None,
                    shards_saturated: None,
                    shards_total: None,
                    approximate: None,
                    threads: None,
                    elapsed_ms: start.elapsed().as_millis() as u64,
                }));
            }
            Ok(None) if matches!(source, ScanCountSource::Snapshot) => {
                state.metrics.observe_scan(
                    &namespace,
                    "count",
                    "filter",
                    "snapshot",
                    STATUS_LAYER_ERROR,
                    start.elapsed().as_secs_f64(),
                    0,
                );
                return Err(AppError::PreconditionFailed(
                    "latest snapshot does not cover this filter".to_string(),
                ));
            }
            Ok(None) => {}
            Err(e) if matches!(source, ScanCountSource::Snapshot) => {
                state.metrics.observe_scan(
                    &namespace,
                    "count",
                    "filter",
                    "snapshot",
                    STATUS_LAYER_ERROR,
                    start.elapsed().as_secs_f64(),
                    0,
                );
                return Err(AppError::Upstream(format!("snapshot count failed: {}", e)));
            }
            Err(e) => {
                warn!(
                    namespace = %namespace,
                    error = %e,
                    "snapshot count lookup failed in auto-mode; falling back to live scan"
                );
            }
        }
    }

    let scan_start_watermark = state.consistency.get(&namespace);
    let filters_supported = request
        .filters
        .as_ref()
        .map(filter_supported)
        .unwrap_or(true);
    let (served_by, schedule_warm) = match source {
        ScanCountSource::Auto => {
            if !filters_supported {
                (ScanCountServedBy::Origin, false)
            } else {
                let (resolved, warm) = decide_auto(&state, &namespace, scan_start_watermark).await;
                (
                    match resolved {
                        ResolvedSource::Cache => ScanCountServedBy::Cache,
                        ResolvedSource::Origin | ResolvedSource::Snapshot => {
                            ScanCountServedBy::Origin
                        }
                    },
                    warm,
                )
            }
        }
        ScanCountSource::Cache => {
            state.observe_cache_demand(&namespace);
            if !state.cache_available().await {
                state.observe_cache_cold_response(&namespace);
                state.metrics.observe_scan(
                    &namespace,
                    "count",
                    "filter",
                    "cache",
                    STATUS_AEROSPIKE_ERROR,
                    start.elapsed().as_secs_f64(),
                    0,
                );
                return Err(AppError::CacheCold(format!(
                    "cache is cold for namespace '{}'; retry after Aerospike reconnects",
                    namespace
                )));
            }
            (ScanCountServedBy::Cache, false)
        }
        ScanCountSource::Origin => (ScanCountServedBy::Origin, false),
        ScanCountSource::Snapshot => unreachable!("snapshot source handled above"),
    };

    if matches!(source, ScanCountSource::Auto)
        && matches!(
            served_by,
            ScanCountServedBy::Cache | ScanCountServedBy::Origin
        )
    {
        state.observe_cache_demand(&namespace);
    }

    let timeout =
        Duration::from_secs(request.timeout_seconds.min(MAX_SCAN_COUNT_TIMEOUT_SECONDS) as u64);
    let deadline = Instant::now() + timeout;
    let mut live_threads = None;
    let live_result = match served_by {
        ScanCountServedBy::Snapshot => unreachable!("snapshot source returned above"),
        ScanCountServedBy::Cache => {
            execute_cache_filter_count(&state, &namespace, request.filters.as_ref(), deadline)
                .await
                .map_err(|e| AppError::Upstream(format!("cache scan count failed: {}", e)))
        }
        ScanCountServedBy::Origin => {
            let active = active_shard_count(&state, &namespace).await;
            let threads =
                resolve_scan_threads_with_active(&state, &namespace, requested_threads, active);
            live_threads = Some(threads);
            let outcome = execute_origin_filter_count(
                &state,
                &namespace,
                request.filters.as_ref(),
                page_size,
                deadline,
                active,
                threads,
            )
            .await
            .map_err(|e| AppError::Upstream(format!("origin scan count failed: {}", e)));
            if let Ok(ref outcome) = outcome {
                if !outcome.timed_out {
                    if let Some(w) = scan_start_watermark {
                        state.cache_warmed_through.insert(namespace.clone(), w);
                    }
                }
            }
            outcome
        }
    };
    let live = match live_result {
        Ok(live) => live,
        Err(err) => {
            state.metrics.observe_scan(
                &namespace,
                "count",
                "filter",
                count_served_by_label(&served_by),
                scan_error_status(&err),
                start.elapsed().as_secs_f64(),
                0,
            );
            return Err(err);
        }
    };

    if schedule_warm {
        spawn_background_warm(
            Arc::clone(&state),
            namespace.clone(),
            page_size,
            scan_start_watermark,
        );
    }

    state.metrics.observe_scan(
        &namespace,
        "count",
        "filter",
        count_served_by_label(&served_by),
        STATUS_OK,
        start.elapsed().as_secs_f64(),
        live.documents_scanned,
    );
    info!(
        namespace = %namespace,
        selector = "filter",
        source = ?served_by,
        count = live.count,
        threads = ?live_threads,
        "scan count completed"
    );

    Ok(scan_count_response(ScanCountResponse {
        count: live.count,
        served_by,
        snapshot_sha: None,
        watermark_ms: None,
        bounded: Some(live.timed_out),
        timed_out: Some(live.timed_out),
        shards_saturated: Some(live.shards_saturated),
        shards_total: Some(live.shards_total),
        approximate: None,
        threads: live_threads,
        elapsed_ms: start.elapsed().as_millis() as u64,
    }))
}

// --- Ranked scan executor (fts / ann selectors) ---
//
// Turbopuffer ranked queries cap at `top_k = 10_000`. With `_hevlayer_shard`
// fan-out that cap becomes per-shard, and most namespaces are exact in one
// scatter/gather. Saturated shards either contribute their `top_k` as a lower
// bound (`exhaustive: false`) or get recursed into with score-band pagination
// (`exhaustive: true`) until exhausted or the deadline elapses. Lifted from the
// removed `/result-count` route; the scatter/gather is unchanged.

/// Turbopuffer ranked-query cap. Mirrors the hardcoded `10_000` in the
/// turbopuffer client's `scan_page` so the count path uses the same ceiling.
const TPUF_TOP_K_MAX: u32 = 10_000;

/// One of the two ranked selectors, borrowed from the request.
enum RankedSelector<'a> {
    Fts(&'a FtsScan),
    Ann(&'a AnnScan),
}

enum ScanSelector<'a> {
    Ranked(RankedSelector<'a>),
    HybridText(&'a HybridTextScan),
}

impl ScanSelector<'_> {
    fn label(&self) -> &'static str {
        match self {
            ScanSelector::Ranked(selector) => selector.label(),
            ScanSelector::HybridText(_) => "hybrid_text",
        }
    }
}

impl RankedSelector<'_> {
    fn label(&self) -> &'static str {
        match self {
            RankedSelector::Fts(_) => "fts",
            RankedSelector::Ann(_) => "ann",
        }
    }
}

/// Kind of ranking so band pagination knows which way to move (BM25 sorts
/// descending, ANN ascending).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RankMode {
    Bm25,
    Ann,
}

#[derive(Default)]
struct ShardState {
    count: u64,
    last_score: Option<f64>,
    last_id: Option<String>,
    exhausted: bool,
}

/// Per-value histogram accumulated by a ranked values scan. The field's
/// attribute is requested via `include_attributes` and recorded per row.
struct RankedValuesAcc {
    field: String,
    counts: HashMap<String, u64>,
}

fn value_key(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Record one document's value(s) for `field` into the histogram. Value
/// listing modes enumerate array elements (each element counts once per
/// containing document); every other shape counts its canonical key.
fn record_doc_value(counts: &mut HashMap<String, u64>, kind: &ExecutorKind, value: &Value) {
    match (kind, value) {
        (ExecutorKind::FieldValues | ExecutorKind::Values, Value::Array(items)) => {
            let mut seen = std::collections::HashSet::with_capacity(items.len());
            for item in items {
                let key = value_key(item);
                if seen.insert(key.clone()) {
                    *counts.entry(key).or_insert(0) += 1;
                }
            }
        }
        _ => {
            *counts.entry(value_key(value)).or_insert(0) += 1;
        }
    }
}

/// Sort `(n desc, v asc)` and apply the per-job value cap. The cap runs after
/// the full pass, so retained counts are exact; `true` means the listing was
/// truncated to the top `MAX_SCAN_VALUES` values.
fn finalize_value_results(counts: HashMap<String, u64>) -> (Vec<FieldValueResult>, bool) {
    let mut results: Vec<FieldValueResult> = counts
        .into_iter()
        .map(|(value, doc_count)| FieldValueResult { value, doc_count })
        .collect();
    results.sort_by(|a, b| {
        b.doc_count
            .cmp(&a.doc_count)
            .then_with(|| a.value.cmp(&b.value))
    });
    let truncated = results.len() > MAX_SCAN_VALUES;
    results.truncate(MAX_SCAN_VALUES);
    (results, truncated)
}

/// Origin scatter/gather count for an `fts` or `ann` selector. Snapshot and
/// cache sources are rejected (neither has an FTS or distance evaluator);
/// omitted, `auto`, and `origin` all resolve to origin.
async fn create_ranked_scan_count(
    state: Arc<AppState>,
    namespace: String,
    request: CreateScanRequest,
    requested_threads: Option<u32>,
) -> Result<Response, AppError> {
    let start = Instant::now();

    let selector = match (
        request.fts.as_ref(),
        request.hybrid_text.as_ref(),
        request.ann.as_ref(),
    ) {
        (Some(fts), None, None) => ScanSelector::Ranked(RankedSelector::Fts(fts)),
        (None, Some(hybrid), None) => ScanSelector::HybridText(hybrid),
        (None, None, Some(ann)) => ScanSelector::Ranked(RankedSelector::Ann(ann)),
        _ => unreachable!("ranked selector presence and exclusivity checked in create_scan"),
    };
    let selector_label = selector.label();

    let observe_err = |status: &str| {
        state.metrics.observe_scan(
            &namespace,
            "count",
            selector_label,
            "origin",
            status,
            start.elapsed().as_secs_f64(),
            0,
        );
    };

    match request.source.clone().unwrap_or_default() {
        ScanCountSource::Auto | ScanCountSource::Origin => {}
        ScanCountSource::Snapshot | ScanCountSource::Cache => {
            observe_err(STATUS_LAYER_ERROR);
            return Err(AppError::Validation(
                "ranked scans (fts/ann) run origin only; source must be omitted, \"auto\", or \"origin\""
                    .to_string(),
            ));
        }
    }

    if let ScanSelector::HybridText(hybrid) = selector {
        return create_hybrid_text_scan_count(
            state,
            namespace,
            hybrid.clone(),
            request,
            requested_threads,
            start,
        )
        .await;
    }

    let ScanSelector::Ranked(selector) = selector else {
        unreachable!("hybrid_text returned above")
    };
    let (rank_by, rank_mode, ann_radius) = match build_rank_by(&selector) {
        Ok(parts) => parts,
        Err(err) => {
            observe_err(scan_error_status(&err));
            return Err(err);
        }
    };

    let active = active_shard_count(&state, &namespace).await;
    let shard_count = active.unwrap_or(1);
    let use_shard_filter = active.is_some();
    let threads = resolve_scan_threads_with_active(&state, &namespace, requested_threads, active);

    // Caller filter only; ANN radius is enforced gateway-side from returned
    // `$dist` rows because upstream does not accept `$dist` in filters.
    let base_filter = request.filters.clone();

    let timeout =
        Duration::from_secs(request.timeout_seconds.min(MAX_SCAN_COUNT_TIMEOUT_SECONDS) as u64);
    let deadline = Instant::now() + timeout;

    let mut shards: Vec<ShardState> = (0..shard_count).map(|_| ShardState::default()).collect();

    let timed_out = match run_ranked_count(
        &state,
        &namespace,
        &rank_by,
        base_filter.as_ref(),
        use_shard_filter,
        shard_count,
        rank_mode,
        ann_radius,
        request.exhaustive,
        deadline,
        threads,
        &mut shards,
        None,
    )
    .await
    {
        Ok(timed_out) => timed_out,
        Err(err) => {
            observe_err(scan_error_status(&err));
            return Err(err);
        }
    };

    let count: u64 = shards.iter().map(|s| s.count).sum();
    let shards_saturated = shards.iter().filter(|s| !s.exhausted).count() as u64;
    let bounded = shards_saturated > 0;
    // ANN counts carry recall error regardless of saturation; FTS is exact.
    let approximate = matches!(rank_mode, RankMode::Ann).then_some(true);

    state.metrics.observe_scan(
        &namespace,
        "count",
        selector_label,
        "origin",
        STATUS_OK,
        start.elapsed().as_secs_f64(),
        count,
    );
    info!(
        namespace = %namespace,
        selector = selector_label,
        count,
        bounded,
        threads,
        "ranked scan count completed"
    );

    Ok(scan_count_response(ScanCountResponse {
        count,
        served_by: ScanCountServedBy::Origin,
        snapshot_sha: None,
        watermark_ms: None,
        bounded: Some(bounded),
        timed_out: Some(timed_out),
        shards_saturated: Some(shards_saturated),
        shards_total: Some(shard_count),
        approximate,
        threads: Some(threads),
        elapsed_ms: start.elapsed().as_millis() as u64,
    }))
}

/// Ranked values scan: the same origin scatter/gather as ranked counts, but
/// run as a background job that also accumulates a per-value histogram from
/// the matched rows' attributes.
async fn create_ranked_scan_values(
    state: Arc<AppState>,
    namespace: String,
    request: CreateScanRequest,
    field: String,
    requested_threads: Option<u32>,
) -> Result<Response, AppError> {
    if request.timeout_seconds == 0 {
        return Err(AppError::Validation(
            "timeout_seconds must be > 0".to_string(),
        ));
    }
    let selector = match (
        request.fts.as_ref(),
        request.hybrid_text.as_ref(),
        request.ann.as_ref(),
    ) {
        (Some(fts), None, None) => ScanSelector::Ranked(RankedSelector::Fts(fts)),
        (None, Some(hybrid), None) => ScanSelector::HybridText(hybrid),
        (None, None, Some(ann)) => ScanSelector::Ranked(RankedSelector::Ann(ann)),
        _ => unreachable!("ranked selector presence and exclusivity checked in create_scan"),
    };
    let selector_label = selector.label();

    let source = request.source.clone().unwrap_or_default();
    match source {
        ScanCountSource::Auto | ScanCountSource::Origin => {}
        ScanCountSource::Snapshot | ScanCountSource::Cache => {
            return Err(AppError::Validation(
                "ranked scans (fts/hybrid_text/ann) run origin only; source must be omitted, \"auto\", or \"origin\""
                    .to_string(),
            ));
        }
    }

    if let ScanSelector::HybridText(hybrid) = selector {
        return create_hybrid_text_scan_values(
            state,
            namespace,
            hybrid.clone(),
            request,
            field,
            source,
            requested_threads,
        )
        .await;
    }

    let ScanSelector::Ranked(selector) = selector else {
        unreachable!("hybrid_text returned above")
    };
    let (rank_by, rank_mode, ann_radius) = build_rank_by(&selector)?;
    // Caller filter only; ANN radius is enforced gateway-side from returned
    // `$dist` rows because upstream does not accept `$dist` in filters.
    let base_filter = request.filters.clone();
    let timeout =
        Duration::from_secs(request.timeout_seconds.min(MAX_SCAN_COUNT_TIMEOUT_SECONDS) as u64);

    let scan_id = uuid::Uuid::new_v4().to_string();
    let response = ExecutorJob {
        id: scan_id.clone(),
        namespace: namespace.clone(),
        kind: ExecutorKind::Values,
        source: match source {
            ScanCountSource::Origin => ExecutorSource::Origin,
            _ => ExecutorSource::Auto,
        },
        effective_source: None,
        status: JobStatus::Running,
        progress: 0.0,
        documents_scanned: 0,
        field: Some(field.clone()),
        unique_values: None,
        truncated: None,
        bounded: None,
        approximate: None,
        stable_as_of: None,
        threads: None,
        created_at: now_iso(),
        completed_at: None,
        error: None,
    };
    let job_state = JobState {
        response,
        field_results: None,
        id_results: None,
        snapshot_field: None,
        snapshot_sha: None,
    };
    let job = scan_job_from_state(&job_state)
        .ok_or_else(|| AppError::Validation("invalid scan job".to_string()))?;
    state.jobs.insert(scan_id.clone(), job_state);

    let exhaustive = request.exhaustive;
    tokio::spawn(async move {
        execute_ranked_values_scan(
            state,
            scan_id,
            namespace,
            field,
            rank_by,
            rank_mode,
            ann_radius,
            base_filter,
            exhaustive,
            timeout,
            requested_threads,
            selector_label,
        )
        .await;
    });

    Ok((StatusCode::ACCEPTED, Json(job)).into_response())
}

fn hybrid_text_leg_specs(
    selector: &HybridTextScan,
    user_filter: Option<&Value>,
    include_surfacing_legs: bool,
) -> Result<Vec<LegSpec>, AppError> {
    if selector.field.trim().is_empty() {
        return Err(AppError::Validation(
            "hybrid_text.field is required".to_string(),
        ));
    }
    if selector.query.trim().is_empty() {
        return Err(AppError::Validation(
            "hybrid_text.query must be non-empty".to_string(),
        ));
    }
    let rank_by = match selector.fuzziness.clone() {
        Some(fuzziness) => json!([
            selector.field,
            "HybridText",
            selector.query,
            {"fuzziness": fuzziness}
        ]),
        None => json!([selector.field, "HybridText", selector.query]),
    };
    let expr = parse_hybrid_text_expr(&rank_by)?;
    let policy = tokenize_query_input(&expr.input);
    if policy.tokens.is_empty() {
        return Err(AppError::Validation(
            "hybrid_text.query yields no tokens under the tokenizer policy".to_string(),
        ));
    }
    let mut specs = build_hybrid_leg_specs(&expr, &policy.tokens, user_filter);
    if include_surfacing_legs {
        specs.extend(build_surfacing_leg_specs(
            &expr,
            &policy.tokens,
            user_filter,
        ));
    }
    Ok(specs)
}

#[allow(clippy::too_many_arguments)]
async fn run_hybrid_text_scan(
    state: &AppState,
    namespace: &str,
    specs: &[LegSpec],
    include: Option<&IncludeAttributes>,
    use_shard_filter: bool,
    shard_count: u64,
    deadline: Instant,
    threads: u32,
) -> Result<(HashMap<String, QueryResult>, bool, bool), AppError> {
    let calls: Vec<(u64, LegSpec)> = (0..shard_count)
        .flat_map(|shard| specs.iter().cloned().map(move |spec| (shard, spec)))
        .collect();
    let collect = stream::iter(calls)
        .map(|(shard, spec)| {
            let filter = with_shard(spec.filter.as_ref(), use_shard_filter, shard);
            async move {
                let rows = state
                    .turbopuffer()
                    .ranked_query(
                        namespace,
                        &spec.rank_by,
                        TPUF_TOP_K_MAX,
                        filter.as_ref(),
                        include,
                    )
                    .await
                    .map(|outcome| outcome.rows)
                    .map_err(map_tpuf_err)?;
                Ok::<_, AppError>((rows.len() == TPUF_TOP_K_MAX as usize, rows))
            }
        })
        .buffer_unordered(threads as usize)
        .try_collect::<Vec<_>>();

    let Some(outcomes) = await_with_deadline(collect, deadline).await else {
        return Ok((HashMap::new(), true, true));
    };

    let mut rows_by_id: HashMap<String, QueryResult> = HashMap::new();
    let mut bounded = false;
    for (saturated, rows) in outcomes? {
        bounded |= saturated;
        for row in rows {
            rows_by_id.entry(row.id.clone()).or_insert(row);
        }
    }
    Ok((rows_by_id, bounded, false))
}

#[allow(clippy::too_many_arguments)]
async fn create_hybrid_text_scan_count(
    state: Arc<AppState>,
    namespace: String,
    selector: HybridTextScan,
    request: CreateScanRequest,
    requested_threads: Option<u32>,
    start: Instant,
) -> Result<Response, AppError> {
    let specs = hybrid_text_leg_specs(
        &selector,
        request.filters.as_ref(),
        !state.namespace_uses_search_store(&namespace),
    )?;
    let active = active_shard_count(&state, &namespace).await;
    let shard_count = active.unwrap_or(1);
    let use_shard_filter = active.is_some();
    let threads = resolve_scan_threads_with_active(&state, &namespace, requested_threads, active);
    let timeout =
        Duration::from_secs(request.timeout_seconds.min(MAX_SCAN_COUNT_TIMEOUT_SECONDS) as u64);
    let deadline = Instant::now() + timeout;

    let (rows, bounded, timed_out) = run_hybrid_text_scan(
        &state,
        &namespace,
        &specs,
        None,
        use_shard_filter,
        shard_count,
        deadline,
        threads,
    )
    .await?;
    let count = rows.len() as u64;

    state.metrics.observe_scan(
        &namespace,
        "count",
        "hybrid_text",
        "origin",
        STATUS_OK,
        start.elapsed().as_secs_f64(),
        count,
    );

    Ok(scan_count_response(ScanCountResponse {
        count,
        served_by: ScanCountServedBy::Origin,
        snapshot_sha: None,
        watermark_ms: None,
        bounded: Some(bounded || timed_out),
        timed_out: Some(timed_out),
        shards_saturated: Some(if bounded { 1 } else { 0 }),
        shards_total: Some(shard_count),
        approximate: Some(false),
        threads: Some(threads),
        elapsed_ms: start.elapsed().as_millis() as u64,
    }))
}

#[allow(clippy::too_many_arguments)]
async fn create_hybrid_text_scan_values(
    state: Arc<AppState>,
    namespace: String,
    selector: HybridTextScan,
    request: CreateScanRequest,
    field: String,
    source: ScanCountSource,
    requested_threads: Option<u32>,
) -> Result<Response, AppError> {
    let specs = hybrid_text_leg_specs(
        &selector,
        request.filters.as_ref(),
        !state.namespace_uses_search_store(&namespace),
    )?;
    let timeout =
        Duration::from_secs(request.timeout_seconds.min(MAX_SCAN_COUNT_TIMEOUT_SECONDS) as u64);
    let scan_id = uuid::Uuid::new_v4().to_string();
    let response = ExecutorJob {
        id: scan_id.clone(),
        namespace: namespace.clone(),
        kind: ExecutorKind::Values,
        source: match source {
            ScanCountSource::Origin => ExecutorSource::Origin,
            _ => ExecutorSource::Auto,
        },
        effective_source: None,
        status: JobStatus::Running,
        progress: 0.0,
        documents_scanned: 0,
        field: Some(field.clone()),
        unique_values: None,
        truncated: None,
        bounded: None,
        approximate: None,
        stable_as_of: None,
        threads: None,
        created_at: now_iso(),
        completed_at: None,
        error: None,
    };
    let job_state = JobState {
        response,
        field_results: None,
        id_results: None,
        snapshot_field: None,
        snapshot_sha: None,
    };
    let job = scan_job_from_state(&job_state)
        .ok_or_else(|| AppError::Validation("invalid scan job".to_string()))?;
    state.jobs.insert(scan_id.clone(), job_state);

    tokio::spawn(async move {
        let start = Instant::now();
        let active = active_shard_count(&state, &namespace).await;
        let shard_count = active.unwrap_or(1);
        let use_shard_filter = active.is_some();
        let threads =
            resolve_scan_threads_with_active(&state, &namespace, requested_threads, active);
        if let Some(mut entry) = state.jobs.get_mut(&scan_id) {
            entry.response.effective_source = Some(ExecutorSource::Origin);
            entry.response.threads = Some(threads);
        }
        let deadline = Instant::now() + timeout;
        let include = IncludeAttributes::Fields(vec![field.clone()]);
        let outcome = run_hybrid_text_scan(
            &state,
            &namespace,
            &specs,
            Some(&include),
            use_shard_filter,
            shard_count,
            deadline,
            threads,
        )
        .await;
        match outcome {
            Ok((rows, bounded, timed_out)) => {
                let mut counts = HashMap::new();
                for row in rows.values() {
                    if let Some(value) = row.attributes.get(&field) {
                        record_doc_value(&mut counts, &ExecutorKind::Values, value);
                    }
                }
                let (field_results, truncated) = finalize_value_results(counts);
                let documents_scanned = rows.len() as u64;
                if let Some(mut entry) = state.jobs.get_mut(&scan_id) {
                    entry.response.status = JobStatus::Completed;
                    entry.response.progress = 1.0;
                    entry.response.documents_scanned = documents_scanned;
                    entry.response.unique_values = Some(field_results.len() as u64);
                    entry.response.truncated = Some(truncated);
                    entry.response.bounded = Some(bounded || timed_out);
                    entry.response.approximate = Some(false);
                    entry.response.completed_at = Some(now_iso());
                    entry.field_results = Some(field_results);
                }
                state.metrics.observe_scan(
                    &namespace,
                    "values",
                    "hybrid_text",
                    "origin",
                    STATUS_OK,
                    start.elapsed().as_secs_f64(),
                    documents_scanned,
                );
            }
            Err(err) => {
                let message = err.to_string();
                if let Some(mut entry) = state.jobs.get_mut(&scan_id) {
                    entry.response.status = JobStatus::Failed;
                    entry.response.error = Some(message.clone());
                    entry.response.completed_at = Some(now_iso());
                }
                state.metrics.observe_scan(
                    &namespace,
                    "values",
                    "hybrid_text",
                    "origin",
                    scan_error_status(&err),
                    start.elapsed().as_secs_f64(),
                    0,
                );
                warn!(
                    namespace = %namespace,
                    scan_id = %scan_id,
                    error = %message,
                    "hybrid_text values scan failed"
                );
            }
        }
    });

    Ok((StatusCode::ACCEPTED, Json(job)).into_response())
}

#[allow(clippy::too_many_arguments)]
async fn execute_ranked_values_scan(
    state: Arc<AppState>,
    scan_id: String,
    namespace: String,
    field: String,
    rank_by: Value,
    rank_mode: RankMode,
    ann_radius: Option<f64>,
    base_filter: Option<Value>,
    exhaustive: bool,
    timeout: Duration,
    requested_threads: Option<u32>,
    selector_label: &'static str,
) {
    let start = Instant::now();
    let active = active_shard_count(&state, &namespace).await;
    let shard_count = active.unwrap_or(1);
    let use_shard_filter = active.is_some();
    let threads = resolve_scan_threads_with_active(&state, &namespace, requested_threads, active);

    if let Some(mut entry) = state.jobs.get_mut(&scan_id) {
        entry.response.effective_source = Some(ExecutorSource::Origin);
        entry.response.threads = Some(threads);
    }

    let deadline = Instant::now() + timeout;
    let mut shards: Vec<ShardState> = (0..shard_count).map(|_| ShardState::default()).collect();
    let mut values = RankedValuesAcc {
        field: field.clone(),
        counts: HashMap::new(),
    };

    let outcome = run_ranked_count(
        &state,
        &namespace,
        &rank_by,
        base_filter.as_ref(),
        use_shard_filter,
        shard_count,
        rank_mode,
        ann_radius,
        exhaustive,
        deadline,
        threads,
        &mut shards,
        Some(&mut values),
    )
    .await;

    match outcome {
        Ok(timed_out) => {
            let documents_scanned: u64 = shards.iter().map(|s| s.count).sum();
            let shards_saturated = shards.iter().filter(|s| !s.exhausted).count() as u64;
            let bounded = shards_saturated > 0 || timed_out;
            let (results, truncated) = finalize_value_results(values.counts);
            if let Some(mut entry) = state.jobs.get_mut(&scan_id) {
                entry.response.status = JobStatus::Completed;
                entry.response.progress = 1.0;
                entry.response.documents_scanned = documents_scanned;
                entry.response.completed_at = Some(now_iso());
                entry.response.unique_values = Some(results.len() as u64);
                entry.response.truncated = Some(truncated);
                entry.response.bounded = Some(bounded);
                entry.response.approximate = matches!(rank_mode, RankMode::Ann).then_some(true);
                entry.field_results = Some(results);
            }
            state.metrics.observe_scan(
                &namespace,
                "values",
                selector_label,
                "origin",
                STATUS_OK,
                start.elapsed().as_secs_f64(),
                documents_scanned,
            );
            info!(
                namespace = %namespace,
                scan_id = %scan_id,
                selector = selector_label,
                field = %field,
                docs = documents_scanned,
                bounded,
                threads,
                "ranked values scan completed"
            );
        }
        Err(err) => {
            let message = err.to_string();
            if let Some(mut entry) = state.jobs.get_mut(&scan_id) {
                entry.response.status = JobStatus::Failed;
                entry.response.error = Some(message.clone());
                entry.response.completed_at = Some(now_iso());
            }
            state.metrics.observe_scan(
                &namespace,
                "values",
                selector_label,
                "origin",
                scan_error_status(&err),
                start.elapsed().as_secs_f64(),
                0,
            );
            warn!(
                namespace = %namespace,
                scan_id = %scan_id,
                error = %message,
                "ranked values scan failed"
            );
        }
    }
}

/// Build the turbopuffer `rank_by` tuple plus the ANN radius the gateway will
/// enforce from returned `$dist` rows.
fn build_rank_by(selector: &RankedSelector) -> Result<(Value, RankMode, Option<f64>), AppError> {
    match selector {
        RankedSelector::Fts(FtsScan { field, query }) => {
            if field.trim().is_empty() {
                return Err(AppError::Validation("fts.field is required".to_string()));
            }
            if query.trim().is_empty() {
                return Err(AppError::Validation(
                    "fts.query must be non-empty".to_string(),
                ));
            }
            Ok((json!([field, "BM25", query]), RankMode::Bm25, None))
        }
        RankedSelector::Ann(AnnScan {
            vector,
            field,
            radius,
        }) => {
            if vector.is_empty() {
                return Err(AppError::Validation(
                    "ann.vector must be non-empty".to_string(),
                ));
            }
            if !radius.is_finite() {
                return Err(AppError::Validation(
                    "ann.radius must be a finite number".to_string(),
                ));
            }
            let field = field.as_deref().unwrap_or("vector");
            Ok((json!([field, "ANN", vector]), RankMode::Ann, Some(*radius)))
        }
    }
}

/// One initial scatter/gather, then (when `exhaustive`) recurse saturated
/// shards until short or the deadline elapses. Returns `true` if it timed out.
#[allow(clippy::too_many_arguments)]
async fn run_ranked_count(
    state: &AppState,
    namespace: &str,
    rank_by: &Value,
    base_filter: Option<&Value>,
    use_shard_filter: bool,
    shard_count: u64,
    mode: RankMode,
    ann_radius: Option<f64>,
    exhaustive: bool,
    deadline: Instant,
    threads: u32,
    shards: &mut [ShardState],
    mut values: Option<&mut RankedValuesAcc>,
) -> Result<bool, AppError> {
    let mut timed_out = run_initial_round(
        state,
        namespace,
        rank_by,
        base_filter,
        use_shard_filter,
        shard_count,
        mode,
        ann_radius,
        deadline,
        threads,
        shards,
        values.as_deref_mut(),
    )
    .await?;

    if exhaustive && !timed_out {
        timed_out = run_exhaustive_loop(
            state,
            namespace,
            rank_by,
            base_filter,
            use_shard_filter,
            mode,
            ann_radius,
            deadline,
            threads,
            shards,
            values,
        )
        .await?;
    }
    Ok(timed_out)
}

fn compose(user: Option<&Value>, extra: Option<Value>) -> Option<Value> {
    match (user, extra) {
        (Some(u), Some(e)) => Some(json!(["And", [u.clone(), e]])),
        (Some(u), None) => Some(u.clone()),
        (None, Some(e)) => Some(e),
        (None, None) => None,
    }
}

fn with_shard(filter: Option<&Value>, use_shard_filter: bool, shard: u64) -> Option<Value> {
    if use_shard_filter {
        Some(combine_shard_filter(filter, shard_filter(shard)))
    } else {
        filter.cloned()
    }
}

/// One scatter/gather over every shard. Populates per-shard state and returns
/// `true` if the deadline elapsed before the round finished.
#[allow(clippy::too_many_arguments)]
async fn run_initial_round(
    state: &AppState,
    namespace: &str,
    rank_by: &Value,
    base_filter: Option<&Value>,
    use_shard_filter: bool,
    shard_count: u64,
    mode: RankMode,
    ann_radius: Option<f64>,
    deadline: Instant,
    threads: u32,
    shards: &mut [ShardState],
    mut values: Option<&mut RankedValuesAcc>,
) -> Result<bool, AppError> {
    let include = values
        .as_ref()
        .map(|acc| IncludeAttributes::Fields(vec![acc.field.clone()]));
    let include = include.as_ref();
    let collect = stream::iter(0..shard_count)
        .map(|shard| {
            let filter = with_shard(base_filter, use_shard_filter, shard);
            async move {
                let rows = state
                    .turbopuffer()
                    .ranked_query(namespace, rank_by, TPUF_TOP_K_MAX, filter.as_ref(), include)
                    .await
                    .map(|outcome| outcome.rows)
                    .map_err(map_tpuf_err)?;
                Ok::<_, AppError>((shard, rows))
            }
        })
        .buffer_unordered(threads as usize)
        .try_collect::<Vec<_>>();

    let outcomes = match await_with_deadline(collect, deadline).await {
        Some(results) => results,
        None => return Ok(true),
    }?;

    for (shard, rows) in outcomes {
        record_page(
            &mut shards[shard as usize],
            rows,
            mode,
            ann_radius,
            values.as_deref_mut(),
        )?;
    }
    Ok(false)
}

/// Recurse on saturated shards in parallel rounds until every shard is short
/// or `deadline` elapses.
#[allow(clippy::too_many_arguments)]
async fn run_exhaustive_loop(
    state: &AppState,
    namespace: &str,
    rank_by: &Value,
    base_filter: Option<&Value>,
    use_shard_filter: bool,
    mode: RankMode,
    ann_radius: Option<f64>,
    deadline: Instant,
    threads: u32,
    shards: &mut [ShardState],
    mut values: Option<&mut RankedValuesAcc>,
) -> Result<bool, AppError> {
    if matches!(mode, RankMode::Ann) {
        return Ok(false);
    }

    let include = values
        .as_ref()
        .map(|acc| IncludeAttributes::Fields(vec![acc.field.clone()]));
    let include = include.as_ref();
    loop {
        let saturated: Vec<usize> = shards
            .iter()
            .enumerate()
            .filter(|(_, s)| !s.exhausted)
            .map(|(i, _)| i)
            .collect();
        if saturated.is_empty() {
            return Ok(false);
        }
        if Instant::now() >= deadline {
            return Ok(true);
        }

        let collect = stream::iter(saturated.into_iter().map(|i| {
            let last_score = shards[i].last_score.expect("saturated shard tracks score");
            let last_id = shards[i]
                .last_id
                .clone()
                .expect("saturated shard tracks last id");
            let band = next_page_filter(mode, last_score, &last_id)
                .expect("only rank modes with score-band pagination recurse");
            let mut filter = compose(base_filter, Some(band));
            if use_shard_filter {
                filter = Some(combine_shard_filter(
                    filter.as_ref(),
                    shard_filter(i as u64),
                ));
            }
            async move {
                let rows = state
                    .turbopuffer()
                    .ranked_query(namespace, rank_by, TPUF_TOP_K_MAX, filter.as_ref(), include)
                    .await
                    .map(|outcome| outcome.rows)
                    .map_err(map_tpuf_err)?;
                Ok::<_, AppError>((i, rows))
            }
        }))
        .buffer_unordered(threads as usize)
        .try_collect::<Vec<_>>();

        let outcomes = match await_with_deadline(collect, deadline).await {
            Some(results) => results,
            None => return Ok(true),
        }?;

        for (i, rows) in outcomes {
            record_page(
                &mut shards[i],
                rows,
                mode,
                ann_radius,
                values.as_deref_mut(),
            )?;
        }
    }
}

/// Inspect the latest page and update shard accounting. A shard is "exhausted"
/// once a page returns fewer than `top_k` rows. With a values accumulator the
/// page's rows also feed the per-value histogram.
fn record_page(
    shard: &mut ShardState,
    rows: Vec<QueryResult>,
    mode: RankMode,
    ann_radius: Option<f64>,
    mut values: Option<&mut RankedValuesAcc>,
) -> Result<(), AppError> {
    let mut counted = 0usize;
    let mut over_radius = false;

    for row in &rows {
        if let Some(radius) = ann_radius {
            let dist = row.dist.ok_or_else(|| {
                AppError::Upstream("Turbopuffer ANN scan row missing $dist".to_string())
            })?;
            if !dist.is_finite() {
                return Err(AppError::Upstream(
                    "Turbopuffer ANN scan row returned non-finite $dist".to_string(),
                ));
            }
            if dist > radius {
                over_radius = true;
                break;
            }
        }
        if let Some(acc) = values.as_deref_mut() {
            if let Some(val) = row.attributes.get(&acc.field) {
                record_doc_value(&mut acc.counts, &ExecutorKind::Values, val);
            }
        }
        counted += 1;
    }

    let len = rows.len();
    shard.count += counted as u64;
    if over_radius || len < TPUF_TOP_K_MAX as usize {
        shard.exhausted = true;
        return Ok(());
    }
    if let Some(last) = rows.last() {
        shard.last_score = last.dist;
        shard.last_id = Some(last.id.clone());
    }
    if matches!(mode, RankMode::Ann) && ann_radius.is_none() {
        return Err(AppError::Upstream(
            "ANN scan missing gateway radius bound".to_string(),
        ));
    }
    Ok(())
}

/// Score-band filter for the next page of a saturated shard. Strict inequality
/// on the score with an `id > last_id` tiebreaker so float ties at the boundary
/// don't cause double-counting or skipping.
fn next_page_filter(mode: RankMode, last_score: f64, last_id: &str) -> Option<Value> {
    let (field, strict, tie) = match mode {
        // BM25 sorts descending; the next page wants scores below the last one.
        RankMode::Bm25 => ("$score", "Lt", "Eq"),
        // Turbopuffer does not accept `$dist` in filters, so ANN radius scans
        // cannot use score-band pagination. Saturated all-in-radius pages stay
        // bounded instead of recursing with a broken pseudo-field filter.
        RankMode::Ann => return None,
    };
    Some(json!([
        "Or",
        [
            [field, strict, last_score],
            ["And", [[field, tie, last_score], ["id", "Gt", last_id]]]
        ]
    ]))
}

async fn await_with_deadline<F: std::future::Future>(
    fut: F,
    deadline: Instant,
) -> Option<F::Output> {
    let now = Instant::now();
    if now >= deadline {
        return None;
    }
    tokio::time::timeout(deadline - now, fut).await.ok()
}

fn map_tpuf_err(e: TurbopufferError) -> AppError {
    warn!(error = %e, "Turbopuffer ranked_query failed during scan count");
    if is_unsupported_by_store(&e) {
        return AppError::from_store_support_error(
            e,
            Some("search".to_string()),
            Some("createScan".to_string()),
        );
    }
    AppError::Upstream(format!("Turbopuffer scan count failed: {}", e))
}

/// GET /v2/namespaces/{namespace}/scans
pub async fn list_scans(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
) -> Result<Json<ScanJobList>, AppError> {
    let mut scans: Vec<ScanJob> = state
        .jobs
        .iter()
        .filter(|entry| entry.value().response.namespace == namespace)
        .filter_map(|entry| scan_job_from_state(entry.value()))
        .collect();
    scans.sort_by(|a, b| a.created_at.cmp(&b.created_at));
    Ok(Json(ScanJobList { scans }))
}

/// GET /v2/namespaces/{namespace}/scans/{scan_id}
pub async fn get_scan(
    State(state): State<Arc<AppState>>,
    Path((namespace, scan_id)): Path<(String, String)>,
) -> Result<Json<ScanJob>, AppError> {
    let scan = state
        .jobs
        .get(&scan_id)
        .ok_or_else(|| AppError::NotFound(format!("Scan '{}' not found", scan_id)))?;
    if scan.response.namespace != namespace {
        return Err(AppError::NotFound(format!("Scan '{}' not found", scan_id)));
    }
    scan_job_from_state(&scan)
        .map(Json)
        .ok_or_else(|| AppError::NotFound(format!("Scan '{}' not found", scan_id)))
}

/// DELETE /v2/namespaces/{namespace}/scans/{scan_id}
pub async fn delete_scan(
    State(state): State<Arc<AppState>>,
    Path((namespace, scan_id)): Path<(String, String)>,
) -> Result<Json<StatusResponse>, AppError> {
    let Some((_, scan)) = state.jobs.remove(&scan_id) else {
        return Err(AppError::NotFound(format!("Scan '{}' not found", scan_id)));
    };
    if scan.response.namespace != namespace
        || !matches!(scan.response.kind, ExecutorKind::Ids | ExecutorKind::Values)
    {
        state.jobs.insert(scan_id.clone(), scan);
        return Err(AppError::NotFound(format!("Scan '{}' not found", scan_id)));
    }
    Ok(Json(StatusResponse::default()))
}

/// GET /v2/namespaces/{namespace}/scans/{scan_id}/results
///
/// Mode-dependent shape: ID jobs return `{ids, total}`, values jobs return
/// `{values: [{v, n}], total, truncated}`. Both paginate with `limit`/`offset`.
pub async fn get_scan_results(
    State(state): State<Arc<AppState>>,
    Path((namespace, scan_id)): Path<(String, String)>,
    Query(query): Query<ScanResultsQuery>,
) -> Result<Response, AppError> {
    let scan = state
        .jobs
        .get(&scan_id)
        .ok_or_else(|| AppError::NotFound(format!("Scan '{}' not found", scan_id)))?;
    if scan.response.namespace != namespace
        || !matches!(scan.response.kind, ExecutorKind::Ids | ExecutorKind::Values)
    {
        return Err(AppError::NotFound(format!("Scan '{}' not found", scan_id)));
    }
    if scan.response.status == JobStatus::Running {
        return Err(AppError::Validation("Scan is still running".to_string()));
    }
    if scan.response.status == JobStatus::Failed {
        return Err(AppError::Validation(format!(
            "Scan failed: {}",
            scan.response.error.as_deref().unwrap_or("unknown error")
        )));
    }

    let limit = query.limit.unwrap_or(1000).min(10_000);
    let offset = query.offset.unwrap_or(0);
    if matches!(scan.response.kind, ExecutorKind::Values) {
        let results = scan.field_results.clone().unwrap_or_default();
        let total = results.len() as u64;
        let truncated = scan.response.truncated.unwrap_or(false);
        let values = results
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|r| ScanValue {
                v: r.value,
                n: r.doc_count,
            })
            .collect();
        return Ok(Json(ScanValuesResponse {
            values,
            total,
            truncated,
        })
        .into_response());
    }
    let ids = scan.id_results.clone().unwrap_or_default();
    let total = ids.len() as u64;
    Ok(Json(ScanIdsResponse {
        ids: ids.into_iter().skip(offset).take(limit).collect(),
        total,
    })
    .into_response())
}

/// POST /v2/namespaces/{namespace}/warm
///
/// Convenience endpoint: creates a full_document scan with source=origin so it
/// also refreshes the Aerospike cache and bumps `cache_warmed_through`.
pub async fn warm_namespace(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
    Query(query): Query<WarmJobQuery>,
) -> Result<(StatusCode, Json<WarmJob>), AppError> {
    state.observe_cache_demand(&namespace);
    if !state.cache_available().await {
        state.observe_cache_cold_response(&namespace);
        return Err(AppError::CacheCold(format!(
            "cache is cold for namespace '{}'; retry after Aerospike reconnects",
            namespace
        )));
    }
    let page_size = query.page_size.unwrap_or(1000);
    validate_page_size(page_size)?;
    let scan = start_document_warm_scan(state, namespace, page_size).await?;
    warm_job_from_response(&scan)
        .map(|job| (StatusCode::ACCEPTED, Json(job)))
        .ok_or_else(|| AppError::Validation("invalid warm job".to_string()))
}

/// GET /v2/namespaces/{namespace}/warm-jobs
pub async fn list_warm_jobs(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
) -> Result<Json<WarmJobList>, AppError> {
    let mut warm_jobs: Vec<WarmJob> = state
        .jobs
        .iter()
        .filter(|entry| entry.value().response.namespace == namespace)
        .filter_map(|entry| warm_job_from_response(&entry.value().response))
        .collect();
    warm_jobs.sort_by(|a, b| a.created_at.cmp(&b.created_at));
    Ok(Json(WarmJobList { warm_jobs }))
}

/// GET /v2/namespaces/{namespace}/warm-jobs/{job_id}
pub async fn get_warm_job(
    State(state): State<Arc<AppState>>,
    Path((namespace, job_id)): Path<(String, String)>,
) -> Result<Json<WarmJob>, AppError> {
    let job = state
        .jobs
        .get(&job_id)
        .ok_or_else(|| AppError::NotFound(format!("Warm job '{}' not found", job_id)))?;
    if job.response.namespace != namespace {
        return Err(AppError::NotFound(format!(
            "Warm job '{}' not found",
            job_id
        )));
    }
    warm_job_from_response(&job.response)
        .map(Json)
        .ok_or_else(|| AppError::NotFound(format!("Warm job '{}' not found", job_id)))
}

/// GET /v1/namespaces/{namespace}/hint_cache_warm
///
/// Turbopuffer-compatible cache warm endpoint. Without any of the layer warm
/// options (`turbopuffer`, `documents`, `snapshots`, `blobs`,
/// `blob_budget_bytes`, `page_size`) this passes through to Turbopuffer
/// verbatim — including any unrecognized query
/// parameters, so upstream-native clients keep their exact wire behavior.
/// Supplying a warm option enables the gateway's extra cache work:
///
/// - forwards the warm hint to Turbopuffer,
/// - starts an async full-document origin scan to populate Aerospike, and
/// - mirrors the latest S3 snapshot body into Aerospike.
/// - when `blobs=true`, hydrates declared blob references from S3 into
///   Aerospike until `blob_budget_bytes` is exhausted.
pub async fn hint_cache_warm(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
    OriginalUri(uri): OriginalUri,
    Query(options): Query<WarmCacheOptions>,
) -> Result<Response, AppError> {
    if !has_warm_options(uri.query()) {
        return crate::routes::turbopuffer::passthrough(
            state,
            "GET",
            uri.path(),
            uri.query(),
            None,
        )
        .await;
    }

    validate_page_size(options.page_size)?;
    validate_blob_warm_options(&options)?;
    state.observe_cache_demand(&namespace);
    if (options.documents || options.snapshots || options.blobs) && !state.cache_available().await {
        state.observe_cache_cold_response(&namespace);
        return Err(AppError::CacheCold(format!(
            "cache is cold for namespace '{}'; retry after Aerospike reconnects",
            namespace
        )));
    }

    let turbopuffer = if options.turbopuffer {
        state
            .turbopuffer()
            .hint_cache_warm(&namespace)
            .await
            .map_err(|e| AppError::Upstream(format!("Turbopuffer warm cache failed: {}", e)))?;
        WarmStepResponse {
            enabled: true,
            status: WarmStepStatus::Completed,
        }
    } else {
        WarmStepResponse {
            enabled: false,
            status: WarmStepStatus::Skipped,
        }
    };

    let documents = if options.documents {
        let scan =
            start_document_warm_scan(Arc::clone(&state), namespace.clone(), options.page_size)
                .await?;
        WarmDocumentsResponse {
            enabled: true,
            status: WarmStepStatus::Started,
            job: warm_job_from_response(&scan),
        }
    } else {
        WarmDocumentsResponse {
            enabled: false,
            status: WarmStepStatus::Skipped,
            job: None,
        }
    };

    let snapshots = if options.snapshots {
        let snapshot_result =
            cache_latest_snapshot_from_s3(state.s3.as_ref(), state.aerospike.as_ref(), &namespace)
                .await;
        match snapshot_result {
            Ok(Some(outcome)) => WarmSnapshotsResponse {
                enabled: true,
                status: WarmStepStatus::Completed,
                key: Some(outcome.key),
                watermark_ms: Some(outcome.watermark_ms),
                sha: Some(outcome.sha),
            },
            Ok(None) => WarmSnapshotsResponse {
                enabled: true,
                status: WarmStepStatus::NoSnapshot,
                key: None,
                watermark_ms: None,
                sha: None,
            },
            Err(e) => {
                let msg = format!("snapshot warm: {}", e);
                if msg.to_ascii_lowercase().contains("aerospike")
                    || msg.to_ascii_lowercase().contains("cache cold")
                {
                    state.observe_cache_cold_response(&namespace);
                    return Err(AppError::CacheCold(msg));
                }
                return Err(AppError::Upstream(msg));
            }
        }
    } else {
        WarmSnapshotsResponse {
            enabled: false,
            status: WarmStepStatus::Skipped,
            key: None,
            watermark_ms: None,
            sha: None,
        }
    };

    let blobs = if options.blobs {
        let attributes = state
            .blob_reference_attributes_for(&namespace)
            .ok_or_else(|| {
                AppError::Validation(format!(
                    "blob warm requested for namespace '{namespace}' but Index.spec.blobs.referenceAttributes is empty"
                ))
            })?;
        hydrate_blobs(
            &state,
            &namespace,
            &attributes,
            options.blob_budget_bytes.expect("validated"),
            options.page_size,
        )
        .await?
    } else {
        WarmBlobsResponse {
            enabled: false,
            status: WarmStepStatus::Skipped,
            attributes: Vec::new(),
            budget_bytes: options.blob_budget_bytes,
            documents_scanned: 0,
            refs_seen: 0,
            objects: 0,
            bytes: 0,
            missing: 0,
            invalid_refs: 0,
            budget_exhausted: false,
        }
    };

    Ok(Json(WarmCacheResponse {
        namespace,
        turbopuffer,
        documents,
        snapshots,
        blobs,
    })
    .into_response())
}

async fn start_document_warm_scan(
    state: Arc<AppState>,
    namespace: String,
    page_size: u32,
) -> Result<ExecutorJob, AppError> {
    let origin_threads = resolve_scan_threads(&state, &namespace, None).await;
    enqueue_scan(
        state,
        namespace,
        CreateExecutorRequest {
            kind: ExecutorKind::FullDocument,
            field: None,
            source: Some(ExecutorSource::Origin),
            filters: None,
            page_size: Some(page_size),
        },
        origin_threads,
    )
    .map(|job_state| job_state.response)
}

fn validate_blob_warm_options(options: &WarmCacheOptions) -> Result<(), AppError> {
    if options.blobs {
        match options.blob_budget_bytes {
            Some(budget) if budget > 0 => Ok(()),
            Some(_) => Err(AppError::Validation(
                "blob_budget_bytes must be greater than 0 when blobs=true".to_string(),
            )),
            None => Err(AppError::Validation(
                "blob_budget_bytes is required when blobs=true".to_string(),
            )),
        }
    } else if options.blob_budget_bytes.is_some() {
        Err(AppError::Validation(
            "blobs=true is required when blob_budget_bytes is supplied".to_string(),
        ))
    } else {
        Ok(())
    }
}

/// Blob fetches (S3 GET) kept in flight per page during cache warm. The S3
/// round-trip dominates warm latency; this bounds the fan-out while keeping the
/// `buffered` cutoff cheap. Writes (Aerospike put_raw) remain serial.
const BLOB_WARM_FETCH_CONCURRENCY: usize = 16;

async fn hydrate_blobs(
    state: &AppState,
    namespace: &str,
    attributes: &[String],
    budget_bytes: u64,
    page_size: u32,
) -> Result<WarmBlobsResponse, AppError> {
    let mut cursor: Option<String> = None;
    let mut documents_scanned = 0_u64;
    let mut refs_seen = 0_u64;
    let mut objects = 0_u64;
    let mut bytes = 0_u64;
    let mut missing = 0_u64;
    let mut invalid_refs = 0_u64;
    let mut budget_exhausted = false;
    let mut seen = HashSet::new();

    while !budget_exhausted {
        let page: DocumentPage = state
            .turbopuffer()
            .scan_page(namespace, cursor.as_deref(), page_size, None, None)
            .await
            .map_err(|e| AppError::Upstream(format!("Turbopuffer blob warm scan failed: {e}")))?;

        documents_scanned += page.documents.len() as u64;

        // Collect this page's unique, not-yet-seen blob refs in document order so
        // the byte-budget cutoff below stops at the exact same blob a serial walk
        // would. Dedup against `seen` here (cheap, in-memory) before any S3 fetch.
        let mut page_refs: Vec<String> = Vec::new();
        for doc in &page.documents {
            for attribute in attributes {
                let Some(value) = doc.attributes.get(attribute) else {
                    continue;
                };
                let mut refs = Vec::new();
                invalid_refs += collect_blob_refs(namespace, value, &mut refs);
                refs_seen += refs.len() as u64;
                for sha256 in refs {
                    if seen.insert(sha256.clone()) {
                        page_refs.push(sha256);
                    }
                }
            }
        }

        // The warm's dominant cost is the per-blob S3 round-trip, so fetch blobs
        // concurrently — this is the parallelism that matters. `buffered` (not
        // `buffer_unordered`) preserves document order, so the byte budget still
        // cuts off at the exact blob the old serial loop did; any prefetch past
        // the cutoff is bounded by the in-flight count and dropped. Writes stay
        // serial: put_raw is intra-cluster/cheap, and serial writes keep the
        // running byte total and budget cutoff exact.
        let s3 = state.s3.clone();
        let mut fetches = stream::iter(page_refs)
            .map(|sha256| {
                let s3 = s3.clone();
                let key = blob_s3_key(namespace, &sha256);
                async move {
                    let blob = s3
                        .get(&key)
                        .await
                        .map_err(|e| AppError::Upstream(format!("read blob from S3: {e}")))?;
                    Ok::<(String, Option<Vec<u8>>), AppError>((sha256, blob))
                }
            })
            .buffered(BLOB_WARM_FETCH_CONCURRENCY);

        while let Some(result) = fetches.next().await {
            let (sha256, blob) = result?;
            let Some(blob) = blob else {
                missing += 1;
                continue;
            };
            let blob_len = blob.len() as u64;
            if bytes.saturating_add(blob_len) > budget_bytes {
                budget_exhausted = true;
                break;
            }
            state
                .aerospike
                .put_raw(&blob_cache_set(namespace), &sha256, &blob)
                .await
                .map_err(|e| {
                    state.observe_cache_cold_response(namespace);
                    AppError::CacheCold(format!("blob warm cache write failed: {e}"))
                })?;
            objects += 1;
            bytes += blob_len;
        }

        match page.next_cursor {
            Some(next) if !budget_exhausted => cursor = Some(next),
            _ => break,
        }
    }

    Ok(WarmBlobsResponse {
        enabled: true,
        status: WarmStepStatus::Completed,
        attributes: attributes.to_vec(),
        budget_bytes: Some(budget_bytes),
        documents_scanned,
        refs_seen,
        objects,
        bytes,
        missing,
        invalid_refs,
        budget_exhausted,
    })
}

fn collect_blob_refs(namespace: &str, value: &Value, refs: &mut Vec<String>) -> u64 {
    match value {
        Value::String(raw) => parse_blob_ref(namespace, raw)
            .map(|maybe| {
                if let Some(sha256) = maybe {
                    refs.push(sha256);
                }
                0
            })
            .unwrap_or(1),
        Value::Array(values) => values
            .iter()
            .map(|value| collect_blob_refs(namespace, value, refs))
            .sum(),
        Value::Null => 0,
        _ => 1,
    }
}

fn parse_blob_ref(namespace: &str, raw: &str) -> Result<Option<String>, ()> {
    let reference = raw.trim();
    if reference.is_empty() {
        return Ok(None);
    }
    let Some(rest) = reference.strip_prefix("blob://") else {
        return Err(());
    };
    let Some((ref_namespace, sha256)) = rest.split_once('/') else {
        return Err(());
    };
    if ref_namespace != namespace || !is_valid_sha256(sha256) {
        return Err(());
    }
    Ok(Some(sha256.to_ascii_lowercase()))
}

/// True when the query string names at least one layer warm option. Only
/// these keys opt the request into gateway orchestration; a stray or
/// upstream-native parameter must stay on the passthrough path rather than
/// silently kicking off warm jobs and changing the response shape.
fn has_warm_options(query: Option<&str>) -> bool {
    const WARM_OPTION_KEYS: [&str; 6] = [
        "turbopuffer",
        "documents",
        "snapshots",
        "blobs",
        "blob_budget_bytes",
        "page_size",
    ];
    query.is_some_and(|q| {
        q.split('&').any(|pair| {
            let key = pair.split('=').next().unwrap_or("");
            WARM_OPTION_KEYS.contains(&key)
        })
    })
}

fn validate_page_size(page_size: u32) -> Result<(), AppError> {
    if page_size == 0 || page_size > 10_000 {
        return Err(AppError::Validation(
            "page_size must be between 1 and 10000".to_string(),
        ));
    }
    Ok(())
}

impl From<SnapshotSource> for ExecutorSource {
    fn from(source: SnapshotSource) -> Self {
        match source {
            SnapshotSource::Auto => ExecutorSource::Auto,
            SnapshotSource::Stored => ExecutorSource::Snapshot,
            SnapshotSource::Cache => ExecutorSource::Cache,
            SnapshotSource::Origin => ExecutorSource::Origin,
        }
    }
}

impl From<ScanSource> for ExecutorSource {
    fn from(source: ScanSource) -> Self {
        match source {
            ScanSource::Auto => ExecutorSource::Auto,
            ScanSource::Cache => ExecutorSource::Cache,
            ScanSource::Origin => ExecutorSource::Origin,
            ScanSource::Snapshot => ExecutorSource::Snapshot,
        }
    }
}

fn scan_source_for_ids(source: ScanCountSource) -> Result<ScanSource, AppError> {
    match source {
        ScanCountSource::Auto => Ok(ScanSource::Auto),
        ScanCountSource::Cache => Ok(ScanSource::Cache),
        ScanCountSource::Origin => Ok(ScanSource::Origin),
        ScanCountSource::Snapshot => Err(AppError::Validation(
            "source=snapshot is only supported with mode=count".to_string(),
        )),
    }
}

fn scan_source_to_snapshot_source(source: &ExecutorSource) -> Option<SnapshotSource> {
    match source {
        ExecutorSource::Auto => Some(SnapshotSource::Auto),
        ExecutorSource::Snapshot => Some(SnapshotSource::Stored),
        ExecutorSource::Cache => Some(SnapshotSource::Cache),
        ExecutorSource::Origin => Some(SnapshotSource::Origin),
    }
}

fn scan_source_to_executor_source(source: &ExecutorSource) -> Option<ScanSource> {
    match source {
        ExecutorSource::Auto => Some(ScanSource::Auto),
        ExecutorSource::Cache => Some(ScanSource::Cache),
        ExecutorSource::Origin => Some(ScanSource::Origin),
        ExecutorSource::Snapshot => Some(ScanSource::Snapshot),
    }
}

fn snapshot_job_from_state(state: &JobState) -> Option<SnapshotJob> {
    if !matches!(state.response.kind, ExecutorKind::FieldValues) {
        return None;
    }
    let field = state.snapshot_field.clone()?;
    Some(SnapshotJob {
        id: state.response.id.clone(),
        namespace: state.response.namespace.clone(),
        field,
        source: scan_source_to_snapshot_source(&state.response.source)?,
        effective_source: state
            .response
            .effective_source
            .as_ref()
            .and_then(scan_source_to_snapshot_source),
        status: state.response.status.clone(),
        progress: state.response.progress,
        documents_scanned: state.response.documents_scanned,
        sha: state.snapshot_sha.clone(),
        stable_as_of: state.response.stable_as_of,
        created_at: state.response.created_at.clone(),
        completed_at: state.response.completed_at.clone(),
        error: state.response.error.clone(),
    })
}

fn warm_job_from_response(response: &ExecutorJob) -> Option<WarmJob> {
    if !matches!(response.kind, ExecutorKind::FullDocument) {
        return None;
    }
    Some(WarmJob {
        id: response.id.clone(),
        namespace: response.namespace.clone(),
        status: response.status.clone(),
        progress: response.progress,
        documents_scanned: response.documents_scanned,
        stable_as_of: response.stable_as_of,
        created_at: response.created_at.clone(),
        completed_at: response.completed_at.clone(),
        error: response.error.clone(),
    })
}

fn scan_job_from_state(state: &JobState) -> Option<ScanJob> {
    let response = &state.response;
    let mode = match response.kind {
        ExecutorKind::Ids => ScanMode::Ids,
        ExecutorKind::Values => ScanMode::Values,
        ExecutorKind::FieldValues | ExecutorKind::FullDocument => return None,
    };
    Some(ScanJob {
        id: response.id.clone(),
        namespace: response.namespace.clone(),
        mode,
        field: response.field.clone(),
        source: scan_source_to_executor_source(&response.source)?,
        effective_source: response
            .effective_source
            .as_ref()
            .and_then(scan_source_to_executor_source),
        status: response.status.clone(),
        progress: response.progress,
        documents_scanned: response.documents_scanned,
        unique_values: response.unique_values,
        truncated: response.truncated,
        bounded: response.bounded,
        approximate: response.approximate,
        snapshot_sha: state.snapshot_sha.clone(),
        watermark_ms: state
            .snapshot_sha
            .is_some()
            .then_some(response.stable_as_of)
            .flatten(),
        threads: response.threads,
        created_at: response.created_at.clone(),
        completed_at: response.completed_at.clone(),
        error: response.error.clone(),
    })
}

/// Source resolved by auto-mode or echoed from an explicit request.
#[derive(Debug, Clone, Copy)]
enum ResolvedSource {
    Cache,
    Origin,
    /// Pre-aggregated snapshot histogram pulled from the Aerospike snapshot
    /// mirror or S3. No document-set or upstream scan touched.
    Snapshot,
}

impl From<ResolvedSource> for ExecutorSource {
    fn from(r: ResolvedSource) -> Self {
        match r {
            ResolvedSource::Cache => ExecutorSource::Cache,
            ResolvedSource::Origin => ExecutorSource::Origin,
            ResolvedSource::Snapshot => ExecutorSource::Snapshot,
        }
    }
}

fn resolved_source_label(resolved: ResolvedSource) -> &'static str {
    match resolved {
        ResolvedSource::Cache => "cache",
        ResolvedSource::Origin => "origin",
        ResolvedSource::Snapshot => "snapshot",
    }
}

fn count_served_by_label(served_by: &ScanCountServedBy) -> &'static str {
    match served_by {
        ScanCountServedBy::Snapshot => "snapshot",
        ScanCountServedBy::Cache => "cache",
        ScanCountServedBy::Origin => "origin",
    }
}

fn scan_error_status(err: &AppError) -> &'static str {
    match err {
        AppError::CacheCold(_) => STATUS_AEROSPIKE_ERROR,
        _ => STATUS_LAYER_ERROR,
    }
}

#[derive(Debug, Clone)]
enum SnapshotLookup {
    Cached(SnapshotBody),
    S3Key(String),
}

struct SnapshotCountHit {
    count: u64,
    sha: String,
    watermark_ms: u64,
}

struct LiveCountOutcome {
    count: u64,
    documents_scanned: u64,
    timed_out: bool,
    shards_saturated: u64,
    shards_total: u64,
}

async fn try_count_from_snapshot(
    state: &AppState,
    namespace: &str,
    filters: Option<&Value>,
) -> Result<Option<SnapshotCountHit>, String> {
    let Some((field, values)) = snapshot_count_filter(filters) else {
        return Ok(None);
    };
    let Some(snapshot) = latest_snapshot_lookup(state, namespace).await? else {
        return Ok(None);
    };
    let body = load_snapshot_body(state, namespace, Some(snapshot)).await?;
    let Some(summary) = body
        .fields
        .iter()
        .find(|candidate| candidate.name == field && !candidate.legacy_truncated)
    else {
        return Ok(None);
    };

    let counts: HashMap<&str, u64> = summary
        .values
        .iter()
        .map(|value| (value.v.as_str(), value.n))
        .collect();
    let count = values
        .iter()
        .map(|value| counts.get(value.as_str()).copied().unwrap_or(0))
        .sum();

    Ok(Some(SnapshotCountHit {
        count,
        sha: body.sha,
        watermark_ms: body.watermark_ms,
    }))
}

fn snapshot_count_filter(filters: Option<&Value>) -> Option<(String, Vec<String>)> {
    let filter = filters?;
    let arr = filter.as_array()?;
    if arr.len() != 3 {
        return None;
    }
    let field = arr.first()?.as_str()?.to_string();
    let op = arr.get(1)?.as_str()?;
    let rhs = arr.get(2)?;
    if op.eq_ignore_ascii_case("Eq") {
        return Some((field, vec![snapshot_value_key(rhs)]));
    }
    if op.eq_ignore_ascii_case("In") {
        let values = rhs
            .as_array()?
            .iter()
            .map(snapshot_value_key)
            .collect::<Vec<_>>();
        return Some((field, values));
    }
    None
}

fn snapshot_value_key(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

async fn execute_cache_filter_count(
    state: &AppState,
    namespace: &str,
    filters: Option<&Value>,
    _deadline: Instant,
) -> Result<LiveCountOutcome, String> {
    let (count, _, _, _) =
        execute_cache_scan(state, namespace, &ExecutorKind::FullDocument, None, filters).await?;
    Ok(LiveCountOutcome {
        count,
        documents_scanned: count,
        timed_out: false,
        shards_saturated: 0,
        shards_total: 1,
    })
}

async fn execute_origin_filter_count(
    state: &AppState,
    namespace: &str,
    filters: Option<&Value>,
    page_size: u32,
    deadline: Instant,
    active_shards: Option<u64>,
    threads: u32,
) -> Result<LiveCountOutcome, String> {
    if let Some(shard_count) = active_shards {
        let outcomes = stream::iter(0..shard_count)
            .map(|shard| {
                let filter = combine_shard_filter(filters, shard_filter(shard));
                async move {
                    execute_origin_filter_count_partition(
                        state,
                        namespace,
                        Some(filter),
                        page_size,
                        deadline,
                    )
                    .await
                }
            })
            .buffer_unordered(threads as usize)
            .try_collect::<Vec<_>>()
            .await?;
        let count = outcomes.iter().map(|outcome| outcome.count).sum();
        let documents_scanned = outcomes
            .iter()
            .map(|outcome| outcome.documents_scanned)
            .sum();
        let shards_saturated = outcomes.iter().filter(|outcome| outcome.timed_out).count() as u64;
        return Ok(LiveCountOutcome {
            count,
            documents_scanned,
            timed_out: shards_saturated > 0,
            shards_saturated,
            shards_total: shard_count,
        });
    }

    let outcome = execute_origin_filter_count_partition(
        state,
        namespace,
        filters.cloned(),
        page_size,
        deadline,
    )
    .await?;
    Ok(LiveCountOutcome {
        count: outcome.count,
        documents_scanned: outcome.documents_scanned,
        timed_out: outcome.timed_out,
        shards_saturated: u64::from(outcome.timed_out),
        shards_total: 1,
    })
}

async fn execute_origin_filter_count_partition(
    state: &AppState,
    namespace: &str,
    filters: Option<Value>,
    page_size: u32,
    deadline: Instant,
) -> Result<LiveCountOutcome, String> {
    let include: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut count: u64 = 0;

    loop {
        let now = Instant::now();
        if now >= deadline {
            return Ok(LiveCountOutcome {
                count,
                documents_scanned: count,
                timed_out: true,
                shards_saturated: 1,
                shards_total: 1,
            });
        }

        let page = match tokio::time::timeout(
            deadline - now,
            state.turbopuffer().scan_page(
                namespace,
                cursor.as_deref(),
                page_size,
                filters.as_ref(),
                Some(&include),
            ),
        )
        .await
        {
            Ok(Ok(page)) => page,
            Ok(Err(e)) => return Err(format!("Turbopuffer scan failed: {}", e)),
            Err(_) => {
                return Ok(LiveCountOutcome {
                    count,
                    documents_scanned: count,
                    timed_out: true,
                    shards_saturated: 1,
                    shards_total: 1,
                });
            }
        };

        count += page.documents.len() as u64;
        state
            .metrics
            .observe_fetch_batch_size("scan", page.documents.len());

        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => {
                return Ok(LiveCountOutcome {
                    count,
                    documents_scanned: count,
                    timed_out: false,
                    shards_saturated: 0,
                    shards_total: 1,
                });
            }
        }
    }
}

fn should_record_scan_cache_demand(explicit_cache_request: bool, resolved: ResolvedSource) -> bool {
    if explicit_cache_request {
        return false;
    }
    matches!(resolved, ResolvedSource::Cache | ResolvedSource::Origin)
}

/// True when a `field_values` scan can be served directly from the latest
/// snapshot: the namespace has facet config and the requested field is in it.
fn snapshot_eligible(
    state: &AppState,
    namespace: &str,
    kind: &ExecutorKind,
    field: Option<&str>,
) -> bool {
    if !matches!(kind, ExecutorKind::FieldValues | ExecutorKind::Values) {
        return false;
    }
    let field_name = match field {
        Some(f) => f,
        None => return false,
    };
    state.has_facet_field(namespace, field_name)
}

fn should_record_snapshot_cache_demand(
    explicit_cache_request: bool,
    resolved: &SnapshotSource,
) -> bool {
    if explicit_cache_request {
        return false;
    }
    matches!(resolved, SnapshotSource::Cache | SnapshotSource::Origin)
}

struct SnapshotMaterialization {
    documents_scanned: u64,
    field_results: Vec<FieldValueResult>,
    watermark_ms: u64,
    sha: String,
}

async fn materialize_stored_snapshot(
    state: &AppState,
    namespace: &str,
    field: &str,
    known_snapshot: Option<SnapshotLookup>,
) -> Result<SnapshotMaterialization, String> {
    let body = load_snapshot_body(state, namespace, known_snapshot).await?;
    let (documents_scanned, field_results) = field_results_from_snapshot_body(&body, field)?;
    Ok(SnapshotMaterialization {
        documents_scanned,
        field_results,
        watermark_ms: body.watermark_ms,
        sha: body.sha,
    })
}

async fn materialize_snapshot_from_field_results(
    state: &AppState,
    namespace: &str,
    field: &str,
    documents_scanned: u64,
    field_results: Option<Vec<FieldValueResult>>,
    watermark_ms: Option<u64>,
) -> Result<SnapshotMaterialization, String> {
    let watermark_ms = watermark_ms.ok_or_else(|| {
        format!(
            "no stable watermark observed for namespace '{}'; retry after the consistency watcher polls it",
            namespace
        )
    })?;
    let field_results = field_results.ok_or_else(|| {
        format!(
            "snapshot job did not produce field results for namespace '{}'",
            namespace
        )
    })?;
    let (fields, fields_skipped) = snapshot_fields_from_results(field, &field_results);
    for skipped in &fields_skipped {
        state.metrics.observe_snapshot_field_skipped(
            namespace,
            &skipped.name,
            skipped.reason.as_str(),
        );
    }
    let body = persist_snapshot_body(
        state.s3.as_ref(),
        namespace,
        watermark_ms,
        documents_scanned,
        fields,
        fields_skipped,
    )
    .await?;
    let bytes =
        serde_json::to_vec_pretty(&body).map_err(|e| format!("serialize snapshot: {}", e))?;
    if let Err(e) = cache_latest_snapshot(state.aerospike.as_ref(), namespace, &bytes).await {
        warn!(
            namespace = %namespace,
            error = %e,
            "failed to mirror on-demand snapshot to Aerospike"
        );
    }
    Ok(SnapshotMaterialization {
        documents_scanned,
        field_results,
        watermark_ms: body.watermark_ms,
        sha: body.sha,
    })
}

fn field_results_from_snapshot_body(
    body: &SnapshotBody,
    field: &str,
) -> Result<(u64, Vec<FieldValueResult>), String> {
    let summary = body
        .fields
        .iter()
        .find(|candidate| candidate.name == field && !candidate.legacy_truncated)
        .ok_or_else(|| {
            format!(
                "field '{}' is not present in snapshot '{}' for namespace '{}'",
                field, body.sha, body.namespace
            )
        })?;
    let mut results: Vec<FieldValueResult> = summary
        .values
        .iter()
        .map(|value| FieldValueResult {
            value: value.v.clone(),
            doc_count: value.n,
        })
        .collect();
    results.sort_by(|a, b| {
        b.doc_count
            .cmp(&a.doc_count)
            .then_with(|| a.value.cmp(&b.value))
    });
    let documents_scanned = results.iter().map(|result| result.doc_count).sum();
    Ok((documents_scanned, results))
}

fn snapshot_fields_from_results(
    field: &str,
    results: &[FieldValueResult],
) -> (Vec<FieldSummary>, Vec<SnapshotFieldSkipped>) {
    if results.len() > MAX_FACET_VALUES {
        return (
            Vec::new(),
            vec![SnapshotFieldSkipped {
                name: field.to_string(),
                reason: SkipReason::ExceededCap,
                distinct_observed: results.len() as u64,
                cap: MAX_FACET_VALUES as u64,
            }],
        );
    }
    let values = results
        .iter()
        .map(|result| ValueCount {
            v: result.value.clone(),
            n: result.doc_count,
        })
        .collect();
    (
        vec![FieldSummary {
            name: field.to_string(),
            values,
            legacy_truncated: false,
        }],
        Vec::new(),
    )
}

#[allow(clippy::too_many_arguments)]
async fn execute_snapshot_job(
    state: Arc<AppState>,
    job_id: String,
    namespace: String,
    field: String,
    requested_source: SnapshotSource,
    filters: Option<Value>,
    page_size: u32,
) {
    let scan_start_watermark = state.consistency.get(&namespace);
    let filters_supported = filters.as_ref().map(filter_supported).unwrap_or(true);
    let explicit_cache_request = matches!(&requested_source, SnapshotSource::Cache);

    let auto_snapshot = if matches!(requested_source, SnapshotSource::Auto) && filters.is_none() {
        match latest_snapshot_lookup(&state, &namespace).await {
            Ok(snapshot) => snapshot,
            Err(e) => {
                warn!(
                    namespace = %namespace,
                    error = %e,
                    "snapshot lookup failed in auto-mode; falling back to materialization"
                );
                None
            }
        }
    } else {
        None
    };
    let auto_snapshot_body = match auto_snapshot.clone() {
        Some(snapshot) => match load_snapshot_body(&state, &namespace, Some(snapshot)).await {
            Ok(body) => Some(body),
            Err(e) => {
                warn!(
                    namespace = %namespace,
                    field = %field,
                    error = %e,
                    "snapshot coverage check failed in auto-mode; falling back to materialization"
                );
                None
            }
        },
        None => None,
    };
    let auto_snapshot_covers_field = auto_snapshot_body
        .as_ref()
        .is_some_and(|body| snapshot_contains_field(body, &field));
    let auto_snapshot_watermark = auto_snapshot_body.as_ref().map(|body| body.watermark_ms);

    let (resolved, schedule_warm) = match requested_source {
        SnapshotSource::Auto => {
            if auto_snapshot_covers_field {
                (SnapshotSource::Stored, false)
            } else if !filters_supported {
                (SnapshotSource::Origin, false)
            } else {
                let (source, warm) = decide_auto(&state, &namespace, scan_start_watermark).await;
                (
                    match source {
                        ResolvedSource::Cache
                            if state.cache_warmed_through.contains_key(&namespace) =>
                        {
                            SnapshotSource::Cache
                        }
                        ResolvedSource::Cache => SnapshotSource::Origin,
                        ResolvedSource::Origin => SnapshotSource::Origin,
                        ResolvedSource::Snapshot => SnapshotSource::Stored,
                    },
                    warm,
                )
            }
        }
        SnapshotSource::Stored => (SnapshotSource::Stored, false),
        SnapshotSource::Cache => (SnapshotSource::Cache, false),
        SnapshotSource::Origin => (SnapshotSource::Origin, false),
    };

    if should_record_snapshot_cache_demand(explicit_cache_request, &resolved) {
        state.observe_cache_demand(&namespace);
    }

    if let Some(mut entry) = state.jobs.get_mut(&job_id) {
        entry.response.effective_source = Some(resolved.clone().into());
    }

    let result: Result<SnapshotMaterialization, String> = match resolved {
        SnapshotSource::Auto => Err("internal error: unresolved snapshot source".to_string()),
        SnapshotSource::Stored => {
            if filters.is_some() {
                Err("filters are not supported with source=stored".to_string())
            } else {
                materialize_stored_snapshot(&state, &namespace, &field, auto_snapshot).await
            }
        }
        SnapshotSource::Cache => {
            match execute_cache_scan(
                &state,
                &namespace,
                &ExecutorKind::FieldValues,
                Some(&field),
                filters.as_ref(),
            )
            .await
            {
                Ok((docs, field_results, _, _)) => {
                    materialize_snapshot_from_field_results(
                        &state,
                        &namespace,
                        &field,
                        docs,
                        field_results,
                        state
                            .cache_warmed_through
                            .get(&namespace)
                            .map(|r| *r.value()),
                    )
                    .await
                }
                Err(e) => Err(e),
            }
        }
        SnapshotSource::Origin => {
            let origin_threads = resolve_scan_threads(&state, &namespace, None).await;
            match execute_origin_scan(
                &state,
                &job_id,
                &namespace,
                &ExecutorKind::FieldValues,
                Some(&field),
                filters.as_ref(),
                page_size,
                origin_threads,
            )
            .await
            {
                Ok((docs, field_results, _, _)) => {
                    materialize_snapshot_from_field_results(
                        &state,
                        &namespace,
                        &field,
                        docs,
                        field_results,
                        scan_start_watermark.or(auto_snapshot_watermark),
                    )
                    .await
                }
                Err(e) => Err(e),
            }
        }
    };

    if schedule_warm {
        spawn_background_warm(
            Arc::clone(&state),
            namespace.clone(),
            page_size,
            scan_start_watermark,
        );
    }

    match result {
        Ok(materialized) => {
            if let Some(mut entry) = state.jobs.get_mut(&job_id) {
                entry.response.status = JobStatus::Completed;
                entry.response.documents_scanned = materialized.documents_scanned;
                entry.response.progress = 1.0;
                entry.response.completed_at = Some(now_iso());
                entry.response.stable_as_of = Some(materialized.watermark_ms);
                entry.response.unique_values = Some(materialized.field_results.len() as u64);
                entry.field_results = Some(materialized.field_results);
                entry.snapshot_sha = Some(materialized.sha.clone());
            }
            info!(
                namespace = %namespace,
                job_id = %job_id,
                sha = %materialized.sha,
                source = ?resolved,
                "snapshot job completed"
            );
        }
        Err(err) => {
            if let Some(mut entry) = state.jobs.get_mut(&job_id) {
                entry.response.status = JobStatus::Failed;
                entry.response.error = Some(err.clone());
                entry.response.completed_at = Some(now_iso());
            }
            warn!(namespace = %namespace, job_id = %job_id, error = %err, "snapshot job failed");
        }
    }
}

fn snapshot_contains_field(body: &SnapshotBody, field: &str) -> bool {
    body.fields.iter().any(|summary| summary.name == field)
}

/// Background scan executor. Picks a source (cache vs. origin) and runs the
/// scan, then writes the result back into `state.jobs`.
#[allow(clippy::too_many_arguments)]
async fn execute_scan(
    state: Arc<AppState>,
    scan_id: String,
    namespace: String,
    kind: ExecutorKind,
    requested_source: ExecutorSource,
    field: Option<String>,
    filters: Option<Value>,
    page_size: u32,
    origin_threads: u32,
) {
    let started = Instant::now();
    let scan_metric_mode = match kind {
        ExecutorKind::Ids => Some("ids"),
        ExecutorKind::Values => Some("values"),
        ExecutorKind::FieldValues | ExecutorKind::FullDocument => None,
    };

    // Watermark observed at the start of this scan. Origin scans that complete
    // successfully will publish this as the new `cache_warmed_through` for the
    // namespace. Cache scans use the *existing* `cache_warmed_through` as the
    // filter cut so the snapshot is stable.
    let scan_start_watermark = state.consistency.get(&namespace);

    // If the caller supplied filters we can't evaluate against cache, force
    // the scan onto the origin path so they don't silently get wrong results.
    let filters_supported = filters.as_ref().map(filter_supported).unwrap_or(true);
    let explicit_cache_request = matches!(&requested_source, ExecutorSource::Cache);

    // Snapshot eligibility: field_values + field is pre-aggregated + no filter.
    // For auto-mode we additionally require that a snapshot is available in
    // the Aerospike mirror or S3 — first-boot before the watermark has
    // advanced falls through to cache.
    let snapshot_field_eligible =
        snapshot_eligible(&state, &namespace, &kind, field.as_deref()) && filters.is_none();
    let auto_snapshot =
        if matches!(requested_source, ExecutorSource::Auto) && snapshot_field_eligible {
            match latest_snapshot_lookup(&state, &namespace).await {
                Ok(snapshot) => snapshot,
                Err(e) => {
                    warn!(
                        namespace = %namespace,
                        error = %e,
                        "snapshot lookup failed in auto-mode; falling back to scan policy"
                    );
                    None
                }
            }
        } else {
            None
        };

    let (resolved, schedule_warm) = match requested_source {
        ExecutorSource::Auto => {
            if auto_snapshot.is_some() {
                (ResolvedSource::Snapshot, false)
            } else if !filters_supported {
                (ResolvedSource::Origin, false)
            } else {
                decide_auto(&state, &namespace, scan_start_watermark).await
            }
        }
        ExecutorSource::Snapshot => (ResolvedSource::Snapshot, false),
        ExecutorSource::Cache => (ResolvedSource::Cache, false),
        ExecutorSource::Origin => (ResolvedSource::Origin, false),
    };

    if should_record_scan_cache_demand(explicit_cache_request, resolved) {
        state.observe_cache_demand(&namespace);
    }

    if let Some(mut entry) = state.jobs.get_mut(&scan_id) {
        entry.response.effective_source = Some(resolved.into());
        entry.response.threads =
            matches!(resolved, ResolvedSource::Origin).then_some(origin_threads);
    }

    let result: Result<ScanOutcome, String> = match resolved {
        ResolvedSource::Snapshot => {
            let fetch_start = Instant::now();
            let r = execute_snapshot_scan(
                &state,
                &namespace,
                &kind,
                field.as_deref(),
                filters.as_ref(),
                auto_snapshot,
            )
            .await;
            state.metrics.observe_fetch(
                "snapshot_lookup",
                &namespace,
                if r.is_ok() { CACHE_HIT } else { CACHE_MISS },
                fetch_start.elapsed().as_secs_f64(),
            );
            r.map(
                |(docs, field_results, id_results, snapshot_watermark, snapshot_sha)| ScanOutcome {
                    docs_scanned: docs,
                    field_results,
                    id_results,
                    stable_as_of: snapshot_watermark,
                    threads: None,
                    truncated: false,
                    snapshot_sha,
                },
            )
        }
        ResolvedSource::Cache => {
            let fetch_start = Instant::now();
            let r = execute_cache_scan(
                &state,
                &namespace,
                &kind,
                field.as_deref(),
                filters.as_ref(),
            )
            .await;
            state.metrics.observe_fetch(
                "scan",
                &namespace,
                if r.is_ok() { CACHE_HIT } else { CACHE_ERROR },
                fetch_start.elapsed().as_secs_f64(),
            );
            r.map(|(docs, field_results, id_results, truncated)| ScanOutcome {
                docs_scanned: docs,
                field_results,
                id_results,
                stable_as_of: state
                    .cache_warmed_through
                    .get(&namespace)
                    .map(|r| *r.value()),
                threads: None,
                truncated,
                snapshot_sha: None,
            })
        }
        ResolvedSource::Origin => {
            let fetch_start = Instant::now();
            let r = execute_origin_scan(
                &state,
                &scan_id,
                &namespace,
                &kind,
                field.as_deref(),
                filters.as_ref(),
                page_size,
                origin_threads,
            )
            .await;
            state.metrics.observe_fetch(
                "scan",
                &namespace,
                CACHE_MISS,
                fetch_start.elapsed().as_secs_f64(),
            );
            // Origin scan also acts as a cache warm. Publish the
            // scan-start watermark so subsequent cache scans use it.
            if r.is_ok() {
                if let Some(w) = scan_start_watermark {
                    state.cache_warmed_through.insert(namespace.clone(), w);
                }
            }
            r.map(|(docs, field_results, id_results, truncated)| ScanOutcome {
                docs_scanned: docs,
                field_results,
                id_results,
                stable_as_of: state
                    .cache_warmed_through
                    .get(&namespace)
                    .map(|r| *r.value()),
                threads: Some(origin_threads),
                truncated,
                snapshot_sha: None,
            })
        }
    };

    if schedule_warm {
        spawn_background_warm(
            Arc::clone(&state),
            namespace.clone(),
            page_size,
            scan_start_watermark,
        );
    }

    match result {
        Ok(ScanOutcome {
            docs_scanned,
            field_results,
            id_results,
            stable_as_of,
            threads,
            truncated,
            snapshot_sha,
        }) => {
            if let Some(mut entry) = state.jobs.get_mut(&scan_id) {
                entry.response.status = JobStatus::Completed;
                entry.response.documents_scanned = docs_scanned;
                entry.response.progress = 1.0;
                entry.response.completed_at = Some(now_iso());
                entry.response.stable_as_of = stable_as_of;
                entry.response.threads = threads;
                if let Some(ref results) = field_results {
                    entry.response.unique_values = Some(results.len() as u64);
                }
                if matches!(kind, ExecutorKind::Values) {
                    entry.response.truncated = Some(truncated);
                }
                entry.field_results = field_results;
                entry.id_results = id_results;
                if snapshot_sha.is_some() {
                    entry.snapshot_sha = snapshot_sha;
                }
            }
            info!(
                namespace = %namespace,
                scan_id = %scan_id,
                docs = docs_scanned,
                source = ?ExecutorSource::from(resolved),
                stable_as_of = ?stable_as_of,
                threads = ?threads,
                "Scan completed"
            );
            if let Some(mode) = scan_metric_mode {
                state.metrics.observe_scan(
                    &namespace,
                    mode,
                    "filter",
                    resolved_source_label(resolved),
                    STATUS_OK,
                    started.elapsed().as_secs_f64(),
                    docs_scanned,
                );
            }
        }
        Err(err) => {
            if let Some(mut entry) = state.jobs.get_mut(&scan_id) {
                entry.response.status = JobStatus::Failed;
                entry.response.error = Some(err.clone());
                entry.response.completed_at = Some(now_iso());
            }
            warn!(namespace = %namespace, scan_id = %scan_id, error = %err, "Scan failed");
            if let Some(mode) = scan_metric_mode {
                state.metrics.observe_scan(
                    &namespace,
                    mode,
                    "filter",
                    resolved_source_label(resolved),
                    STATUS_LAYER_ERROR,
                    started.elapsed().as_secs_f64(),
                    0,
                );
            }
        }
    }
}

/// Auto-mode decision: serve from cache if warm, otherwise origin.
///
/// Returns `(source, schedule_background_warm)`.
async fn decide_auto(
    state: &AppState,
    namespace: &str,
    current_watermark: Option<u64>,
) -> (ResolvedSource, bool) {
    let count = match state.aerospike.count_set(namespace).await {
        Ok(count) => count,
        Err(e) => {
            state.metrics.set_cache_state(namespace, "cold");
            warn!(
                namespace = %namespace,
                error = %e,
                "Aerospike count failed in auto scan; falling back to origin"
            );
            return (ResolvedSource::Origin, false);
        }
    };
    let cache_through = state
        .cache_warmed_through
        .get(namespace)
        .map(|r| *r.value());

    // Empty cache → synchronous origin scan so the caller gets data on the
    // first hit. The origin scan doubles as a warm.
    if count == 0 {
        return (ResolvedSource::Origin, false);
    }

    match (cache_through, current_watermark) {
        // Cache is at least as fresh as the index — serve directly.
        (Some(ct), Some(w)) if ct >= w => (ResolvedSource::Cache, false),
        // Cache trails the index — serve what we have, schedule a warm.
        (Some(_), Some(_)) => (ResolvedSource::Cache, true),
        // Cache has data but no warm has stamped a watermark yet — serve it
        // and schedule a warm so future scans can apply the consistency cut.
        (None, Some(_)) => (ResolvedSource::Cache, true),
        // Watermark not observed yet. Cache is the best we have.
        (_, None) => (ResolvedSource::Cache, false),
    }
}

/// Fire-and-forget cache warm. Single-flight per namespace via
/// `state.warm_inflight` so concurrent auto-scans don't double up.
fn spawn_background_warm(
    state: Arc<AppState>,
    namespace: String,
    page_size: u32,
    watermark_at_start: Option<u64>,
) {
    match state.warm_inflight.entry(namespace.clone()) {
        Entry::Occupied(_) => {
            // Another warm is already running for this namespace.
            return;
        }
        Entry::Vacant(v) => {
            v.insert(());
        }
    }

    tokio::spawn(async move {
        state.metrics.set_cache_state(&namespace, "warming");
        info!(namespace = %namespace, "Starting background cache warm");
        if let Err(e) = state.turbopuffer().hint_cache_warm(&namespace).await {
            warn!(namespace = %namespace, error = %e, "Turbopuffer warm hint failed during background warm");
        }
        match cache_latest_snapshot_from_s3(state.s3.as_ref(), state.aerospike.as_ref(), &namespace)
            .await
        {
            Ok(Some(outcome)) => info!(
                namespace = %namespace,
                key = %outcome.key,
                watermark_ms = outcome.watermark_ms,
                "Background warm mirrored latest snapshot"
            ),
            Ok(None) => {}
            Err(e) => warn!(namespace = %namespace, error = %e, "Background snapshot warm failed"),
        }
        let result = warm_cache(&state, &namespace, page_size).await;
        match result {
            Ok(scanned) => {
                if let Some(w) = watermark_at_start {
                    state.cache_warmed_through.insert(namespace.clone(), w);
                }
                info!(
                    namespace = %namespace,
                    docs = scanned,
                    watermark = ?watermark_at_start,
                    "Background warm completed"
                );
            }
            Err(e) => {
                state.metrics.set_cache_state(&namespace, "cold");
                warn!(namespace = %namespace, error = %e, "Background warm failed");
            }
        }
        state.warm_inflight.remove(&namespace);
        state.cache_state_for_namespace(&namespace).await;
    });
}

/// After an Aerospike reconnect, the first cache miss per namespace kicks off
/// a deduped warm so scale-to-zero cold starts recover without a manual warm.
pub(crate) async fn maybe_spawn_reactive_warm(state: Arc<AppState>, namespace: String) {
    let generation = state.aerospike_runtime.generation();
    if generation == 0 || !state.aerospike_runtime.is_connected().await {
        return;
    }

    match state.reactive_warm_generations.entry(namespace.clone()) {
        Entry::Occupied(mut entry) => {
            if *entry.get() >= generation {
                return;
            }
            entry.insert(generation);
        }
        Entry::Vacant(entry) => {
            entry.insert(generation);
        }
    }

    spawn_background_warm(
        Arc::clone(&state),
        namespace.clone(),
        1000,
        state.consistency.get(&namespace),
    );
}

/// Paginate over turbopuffer and backfill Aerospike. Same shape as the inner
/// loop in `execute_origin_scan`, minus the scan_id state updates and
/// field-value aggregation.
async fn warm_cache(state: &AppState, namespace: &str, page_size: u32) -> Result<u64, String> {
    let active = active_shard_count(state, namespace).await;
    let threads = resolve_scan_threads_with_active(state, namespace, None, active);
    if let Some(shard_count) = active {
        let totals =
            stream::iter(0..shard_count)
                .map(|shard| {
                    let filter = shard_filter(shard);
                    async move {
                        warm_cache_with_filter(state, namespace, page_size, Some(filter)).await
                    }
                })
                .buffer_unordered(threads as usize)
                .try_collect::<Vec<_>>()
                .await?;
        return Ok(totals.into_iter().sum());
    }

    warm_cache_with_filter(state, namespace, page_size, None).await
}

async fn warm_cache_with_filter(
    state: &AppState,
    namespace: &str,
    page_size: u32,
    filters: Option<Value>,
) -> Result<u64, String> {
    let mut cursor: Option<String> = None;
    let mut total: u64 = 0;
    loop {
        let page: DocumentPage = state
            .turbopuffer()
            .scan_page(
                namespace,
                cursor.as_deref(),
                page_size,
                filters.as_ref(),
                None,
            )
            .await
            .map_err(|e| format!("Turbopuffer scan failed: {}", e))?;

        total += page.documents.len() as u64;
        state
            .metrics
            .observe_fetch_batch_size("scan", page.documents.len());
        if !page.documents.is_empty() {
            let docs: HashMap<String, HashMap<String, Value>> = page
                .documents
                .iter()
                .map(|d| (d.id.clone(), d.attributes.clone()))
                .collect();
            let cache_set = state.aerospike_set_name(namespace);
            let backfill_start = Instant::now();
            let backfill = state.aerospike.put_many(namespace, &docs).await;
            match backfill {
                Ok(()) => state.metrics.observe_cache_backfill(
                    &cache_set,
                    STATUS_OK,
                    backfill_start.elapsed().as_secs_f64(),
                    None,
                ),
                Err(e) => {
                    state.metrics.observe_cache_backfill(
                        &cache_set,
                        STATUS_AEROSPIKE_ERROR,
                        backfill_start.elapsed().as_secs_f64(),
                        None,
                    );
                    warn!(namespace = %namespace, error = %e, "Aerospike backfill failed during warm");
                }
            }
        }

        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    Ok(total)
}

/// Aggregated result of any scan path, so the caller can branch on source
/// while still funnelling into one completion handler.
struct ScanOutcome {
    docs_scanned: u64,
    field_results: Option<Vec<FieldValueResult>>,
    id_results: Option<Vec<String>>,
    /// Watermark to surface on the scan response. For snapshot scans this is
    /// the snapshot's own `watermark_ms`; for cache/origin scans it mirrors
    /// `cache_warmed_through`.
    stable_as_of: Option<u64>,
    /// Effective origin fan-out width, present only for origin scans.
    threads: Option<u32>,
    /// Values scans: the per-job distinct-value cap was crossed.
    truncated: bool,
    /// Snapshot-served scans echo the snapshot body's content SHA.
    snapshot_sha: Option<String>,
}

/// Serve a snapshot job from the latest pre-aggregated snapshot. The
/// steady-state path reads one Aerospike record from the snapshot mirror; S3
/// is the durable fallback when the mirror is cold.
///
/// Returns `(documents_scanned, field_results, snapshot_watermark_ms)`.
/// `documents_scanned` is the sum of `n` across all values, which matches
/// what a freshly aggregating cache snapshot job would have reported.
async fn execute_snapshot_scan(
    state: &AppState,
    namespace: &str,
    kind: &ExecutorKind,
    field: Option<&str>,
    filters: Option<&Value>,
    // Pre-resolved snapshot from auto-mode's eligibility lookup. When `None`,
    // the executor does its own Aerospike/S3 lookup (explicit-source path).
    known_snapshot: Option<SnapshotLookup>,
) -> Result<
    (
        u64,
        Option<Vec<FieldValueResult>>,
        Option<Vec<String>>,
        Option<u64>,
        Option<String>,
    ),
    String,
> {
    // Snapshot bodies are pre-aggregated histograms — they don't carry the
    // per-document attributes a filter would need to evaluate against.
    if filters.is_some() {
        return Err(
            "filters are not supported with source=stored; use source=cache or source=origin"
                .to_string(),
        );
    }
    if !matches!(kind, ExecutorKind::FieldValues | ExecutorKind::Values) {
        return Err("source=stored only supports snapshot field materialization".to_string());
    }
    let field_name = field.ok_or("field is required for snapshot job")?;

    let body = load_snapshot_body(state, namespace, known_snapshot).await?;

    let summary = body
        .fields
        .iter()
        .find(|f| f.name == field_name && !f.legacy_truncated)
        .ok_or_else(|| {
            format!(
                "field '{}' is not pre-aggregated in snapshots for namespace '{}'; add it to Index.spec.snapshot.facetFields",
                field_name, namespace
            )
        })?;

    let results: Vec<FieldValueResult> = summary
        .values
        .iter()
        .map(|vc| FieldValueResult {
            value: vc.v.clone(),
            doc_count: vc.n,
        })
        .collect();
    // Results from `build_field_summaries` are sorted lexically by value name
    // for canonical SHA stability. The scan API contract is "sorted by count
    // desc"; re-sort here so callers see the expected shape.
    let mut results = results;
    results.sort_by(|a, b| {
        b.doc_count
            .cmp(&a.doc_count)
            .then_with(|| a.value.cmp(&b.value))
    });
    let total: u64 = results.iter().map(|r| r.doc_count).sum();
    Ok((
        total,
        Some(results),
        None,
        Some(body.watermark_ms),
        Some(body.sha),
    ))
}

async fn latest_snapshot_lookup(
    state: &AppState,
    namespace: &str,
) -> Result<Option<SnapshotLookup>, String> {
    match latest_snapshot_bytes_from_cache(state.aerospike.as_ref(), namespace).await {
        Ok(Some(bytes)) => match decode_snapshot_body(namespace, &bytes) {
            Ok(body) => return Ok(Some(SnapshotLookup::Cached(body))),
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

    let key = latest_snapshot_key(state.s3.as_ref(), namespace)
        .await
        .map_err(|e| format!("s3 list: {}", e))?;
    Ok(key.map(SnapshotLookup::S3Key))
}

async fn load_snapshot_body(
    state: &AppState,
    namespace: &str,
    known_snapshot: Option<SnapshotLookup>,
) -> Result<SnapshotBody, String> {
    match known_snapshot {
        Some(SnapshotLookup::Cached(body)) => Ok(body),
        Some(SnapshotLookup::S3Key(key)) => fetch_snapshot_from_s3(state, namespace, &key).await,
        None => match latest_snapshot_lookup(state, namespace).await? {
            Some(SnapshotLookup::Cached(body)) => Ok(body),
            Some(SnapshotLookup::S3Key(key)) => {
                fetch_snapshot_from_s3(state, namespace, &key).await
            }
            None => Err(format!(
                "no snapshot available for namespace '{}' yet; retry once the watcher has run",
                namespace
            )),
        },
    }
}

async fn fetch_snapshot_from_s3(
    state: &AppState,
    namespace: &str,
    key: &str,
) -> Result<SnapshotBody, String> {
    let bytes = state
        .s3
        .get(key)
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

    Ok(body)
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

/// Scan from Aerospike cache. Applies the watermark filter
/// (`_hevlayer_upserted_at <= cache_warmed_through`) followed by any user filters so
/// the result is a consistent snapshot.
type ScanEvalOutcome = (
    u64,
    Option<Vec<FieldValueResult>>,
    Option<Vec<String>>,
    bool,
);

async fn execute_cache_scan(
    state: &AppState,
    namespace: &str,
    kind: &ExecutorKind,
    field: Option<&str>,
    filters: Option<&Value>,
) -> Result<ScanEvalOutcome, String> {
    let cache_set = state.aerospike_set_name(namespace);
    let cache_start = Instant::now();
    let records_result = state.aerospike.scan(namespace, None).await;
    let records = match records_result {
        Ok(records) => {
            state.metrics.observe_cache_lookup(
                &cache_set,
                namespace,
                CACHE_HIT,
                records.len() as u64,
                cache_start.elapsed().as_secs_f64(),
                None,
            );
            records
        }
        Err(e) => {
            state.metrics.observe_cache_lookup(
                &cache_set,
                namespace,
                CACHE_ERROR,
                1,
                cache_start.elapsed().as_secs_f64(),
                None,
            );
            return Err(format!("Aerospike scan failed: {}", e));
        }
    };

    let watermark = state
        .cache_warmed_through
        .get(namespace)
        .map(|r| *r.value());

    let mut filtered: Vec<(String, HashMap<String, Value>)> = Vec::with_capacity(records.len());
    for (id, attrs) in records {
        if let Some(bound) = watermark {
            let ts = attrs
                .get(UPSERTED_AT_ATTR)
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            if ts > bound {
                continue;
            }
        }
        if let Some(f) = filters {
            if !match_filter(&attrs, f)? {
                continue;
            }
        }
        filtered.push((id, attrs));
    }

    let docs_scanned = filtered.len() as u64;

    let (field_results, truncated) = match kind {
        ExecutorKind::FieldValues | ExecutorKind::Values => {
            let field_name = field.ok_or("field is required for field_values scan")?;
            let mut counts: HashMap<String, u64> = HashMap::new();
            for (_id, attrs) in &filtered {
                if let Some(val) = attrs.get(field_name) {
                    record_doc_value(&mut counts, kind, val);
                }
            }
            if matches!(kind, ExecutorKind::Values) {
                let (results, truncated) = finalize_value_results(counts);
                (Some(results), truncated)
            } else {
                let mut results: Vec<FieldValueResult> = counts
                    .into_iter()
                    .map(|(value, doc_count)| FieldValueResult { value, doc_count })
                    .collect();
                results.sort_by_key(|r| std::cmp::Reverse(r.doc_count));
                (Some(results), false)
            }
        }
        ExecutorKind::FullDocument | ExecutorKind::Ids => (None, false),
    };

    let id_results = if matches!(kind, ExecutorKind::Ids) {
        Some(filtered.iter().map(|(id, _attrs)| id.clone()).collect())
    } else {
        None
    };

    Ok((docs_scanned, field_results, id_results, truncated))
}

/// Scan from Turbopuffer with keyset pagination + Aerospike backfill.
#[allow(clippy::too_many_arguments)]
async fn execute_origin_scan(
    state: &AppState,
    scan_id: &str,
    namespace: &str,
    kind: &ExecutorKind,
    field: Option<&str>,
    filters: Option<&Value>,
    page_size: u32,
    threads: u32,
) -> Result<ScanEvalOutcome, String> {
    // Keep FieldValues on the row-scan path: snapshot materialization uses it
    // and must enforce Layer's per-element array facet invariant even when a
    // backend facet endpoint buckets whole arrays.
    if let (ExecutorKind::Values, Some(field)) = (kind, field) {
        match state
            .turbopuffer()
            .facet(namespace, filters, field, MAX_SCAN_VALUES + 1)
            .await
        {
            Ok(mut results) => {
                results.sort_by(|a, b| {
                    b.doc_count
                        .cmp(&a.doc_count)
                        .then_with(|| a.value.cmp(&b.value))
                });
                let documents_scanned = results.iter().map(|result| result.doc_count).sum();
                let truncated =
                    matches!(kind, ExecutorKind::Values) && results.len() > MAX_SCAN_VALUES;
                if truncated {
                    results.truncate(MAX_SCAN_VALUES);
                }
                return Ok((documents_scanned, Some(results), None, truncated));
            }
            Err(error) if is_unsupported_by_store(&error) => {}
            Err(error) => return Err(format!("Turbopuffer facet failed: {}", error)),
        }
    }

    let outcomes = shard_fanout(state, namespace, threads, |shard_filter| {
        let partitioned = shard_filter.is_some();
        let partition_filters = match shard_filter {
            Some(shard_filter) => Some(combine_shard_filter(filters, shard_filter)),
            None => filters.cloned(),
        };
        async move {
            execute_origin_scan_partition(
                state,
                namespace,
                kind,
                field,
                partition_filters,
                page_size,
                if partitioned { None } else { Some(scan_id) },
            )
            .await
        }
    })
    .await?;
    let mut total_scanned = 0_u64;
    let mut counts: HashMap<String, u64> = HashMap::new();
    let mut ids = Vec::new();
    for (docs_scanned, shard_counts, shard_ids) in outcomes {
        total_scanned += docs_scanned;
        for (value, count) in shard_counts {
            *counts.entry(value).or_insert(0) += count;
        }
        ids.extend(shard_ids);
    }
    ids.sort();
    let id_results = if matches!(kind, ExecutorKind::Ids) {
        Some(ids)
    } else {
        None
    };
    let (field_results, truncated) = if matches!(kind, ExecutorKind::Values) {
        let (results, truncated) = finalize_value_results(counts);
        (Some(results), truncated)
    } else {
        (field_results_from_counts(kind, counts), false)
    };
    Ok((total_scanned, field_results, id_results, truncated))
}

async fn execute_origin_scan_partition(
    state: &AppState,
    namespace: &str,
    kind: &ExecutorKind,
    field: Option<&str>,
    filters: Option<Value>,
    page_size: u32,
    progress_scan_id: Option<&str>,
) -> Result<(u64, HashMap<String, u64>, Vec<String>), String> {
    let mut cursor: Option<String> = None;
    let mut total_scanned: u64 = 0;
    let mut counts: HashMap<String, u64> = HashMap::new();
    let scan_attrs = match (kind, field) {
        (ExecutorKind::FieldValues | ExecutorKind::Values, Some(field_name)) => {
            Some(vec![field_name.to_string()])
        }
        (ExecutorKind::Ids, _) => Some(Vec::new()),
        _ => None,
    };
    let mut ids = Vec::new();

    loop {
        let page_result = state
            .turbopuffer()
            .scan_page(
                namespace,
                cursor.as_deref(),
                page_size,
                filters.as_ref(),
                scan_attrs.as_deref(),
            )
            .await;
        let page: DocumentPage = match page_result {
            Ok(page) => page,
            Err(e) => {
                let missing_attr = scan_attrs
                    .as_ref()
                    .and_then(|attrs| missing_include_attribute(&e, attrs))
                    .map(str::to_string);
                if let Some(missing_attr) = missing_attr {
                    warn!(
                        namespace = %namespace,
                        attribute = %missing_attr,
                        error = %e,
                        "field scan attribute is absent from upstream schema; returning empty field result"
                    );
                    return Ok((total_scanned, counts, ids));
                } else {
                    return Err(format!("Turbopuffer scan failed: {}", e));
                }
            }
        };

        let page_count = page.documents.len() as u64;
        total_scanned += page_count;
        state
            .metrics
            .observe_fetch_batch_size("scan", page.documents.len());

        // Backfill Aerospike cache (best-effort). Skip when the scan only
        // requested a subset of attributes — a partial-attribute put_many
        // would clobber fuller cached records with thin ones, and a
        // subsequent fetch_document would happily return that thin record
        // as if it were the whole doc.
        if !page.documents.is_empty() && scan_attrs.is_none() {
            let cache_docs: HashMap<String, HashMap<String, Value>> = page
                .documents
                .iter()
                .map(|d| (d.id.clone(), d.attributes.clone()))
                .collect();
            let cache_set = state.aerospike_set_name(namespace);
            let backfill_start = Instant::now();
            let backfill = state.aerospike.put_many(namespace, &cache_docs).await;
            match backfill {
                Ok(()) => state.metrics.observe_cache_backfill(
                    &cache_set,
                    STATUS_OK,
                    backfill_start.elapsed().as_secs_f64(),
                    None,
                ),
                Err(e) => {
                    state.metrics.observe_cache_backfill(
                        &cache_set,
                        STATUS_AEROSPIKE_ERROR,
                        backfill_start.elapsed().as_secs_f64(),
                        None,
                    );
                    warn!(namespace = %namespace, error = %e, "Aerospike backfill failed during scan");
                }
            }
        }

        // Aggregate field values if needed
        if matches!(kind, ExecutorKind::FieldValues | ExecutorKind::Values) {
            if let Some(field_name) = field {
                for doc in &page.documents {
                    if let Some(val) = doc.attributes.get(field_name) {
                        record_doc_value(&mut counts, kind, val);
                    }
                }
            }
        }
        if matches!(kind, ExecutorKind::Ids) {
            ids.extend(page.documents.iter().map(|doc| doc.id.clone()));
        }

        // Update progress
        if let Some(scan_id) = progress_scan_id {
            if let Some(mut entry) = state.jobs.get_mut(scan_id) {
                entry.response.documents_scanned = total_scanned;
            }
        }

        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }

    Ok((total_scanned, counts, ids))
}

fn is_unsupported_by_store(error: &TurbopufferError) -> bool {
    AppError::is_store_support_error(error)
}

fn missing_include_attribute<'a>(error: &TurbopufferError, attrs: &'a [String]) -> Option<&'a str> {
    let msg = error.to_string();
    if !msg.contains("include_attributes") || !msg.contains("not found in schema") {
        return None;
    }
    attrs.iter().find_map(|attr| {
        let raw = format!("attribute \"{}\" not found in schema", attr);
        let escaped = format!("attribute \\\"{}\\\" not found in schema", attr);
        (msg.contains(&raw) || msg.contains(&escaped)).then_some(attr.as_str())
    })
}

fn field_results_from_counts(
    kind: &ExecutorKind,
    counts: HashMap<String, u64>,
) -> Option<Vec<FieldValueResult>> {
    match kind {
        ExecutorKind::FieldValues => {
            let mut results: Vec<FieldValueResult> = counts
                .into_iter()
                .map(|(value, doc_count)| FieldValueResult { value, doc_count })
                .collect();
            results.sort_by_key(|r| std::cmp::Reverse(r.doc_count));
            Some(results)
        }
        // `Values` results are finalized (sorted + capped) by the caller.
        ExecutorKind::Values | ExecutorKind::FullDocument | ExecutorKind::Ids => None,
    }
}

// --- Filter evaluation -----------------------------------------------------
//
// Turbopuffer's filter syntax is array-based:
//   leaf:     ["field", "Op", value]
//   compound: ["And"|"Or", [filter, ...]]
//   negation: ["Not", filter]
//
// We evaluate against the document's attribute map. Unsupported ops/shapes
// bubble up as `Err(String)` so callers can route those scans to the origin
// path instead of returning silently-wrong results.

const SUPPORTED_OPS: &[&str] = &["Eq", "NotEq", "Gt", "Gte", "Lt", "Lte", "In", "NotIn"];

/// Check whether every leaf op in `filter` is one we can evaluate. Used by
/// auto-mode to short-circuit to origin when a filter would otherwise force a
/// fallback after a full set scan.
fn filter_supported(filter: &Value) -> bool {
    let arr = match filter.as_array() {
        Some(a) => a,
        None => return false,
    };
    let head = match arr.first().and_then(|v| v.as_str()) {
        Some(s) => s,
        None => return false,
    };
    match head {
        "And" | "and" | "Or" | "or" => arr
            .get(1)
            .and_then(|v| v.as_array())
            .map(|subs| subs.iter().all(filter_supported))
            .unwrap_or(false),
        "Not" | "not" => arr.get(1).map(filter_supported).unwrap_or(false),
        _ => arr
            .get(1)
            .and_then(|v| v.as_str())
            .map(|op| SUPPORTED_OPS.iter().any(|s| s.eq_ignore_ascii_case(op)))
            .unwrap_or(false),
    }
}

fn match_filter(doc: &HashMap<String, Value>, filter: &Value) -> Result<bool, String> {
    let arr = filter
        .as_array()
        .ok_or_else(|| format!("filter must be array: {}", filter))?;
    let head = arr
        .first()
        .and_then(|v| v.as_str())
        .ok_or_else(|| "filter[0] must be string".to_string())?;

    match head {
        "And" | "and" => {
            let subs = arr
                .get(1)
                .and_then(|v| v.as_array())
                .ok_or_else(|| "And expects array of filters at index 1".to_string())?;
            for sub in subs {
                if !match_filter(doc, sub)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        "Or" | "or" => {
            let subs = arr
                .get(1)
                .and_then(|v| v.as_array())
                .ok_or_else(|| "Or expects array of filters at index 1".to_string())?;
            for sub in subs {
                if match_filter(doc, sub)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        "Not" | "not" => {
            let sub = arr
                .get(1)
                .ok_or_else(|| "Not expects filter at index 1".to_string())?;
            Ok(!match_filter(doc, sub)?)
        }
        field => {
            let op = arr
                .get(1)
                .and_then(|v| v.as_str())
                .ok_or_else(|| "filter[1] must be op string".to_string())?;
            let rhs = arr
                .get(2)
                .ok_or_else(|| "filter[2] (value) missing".to_string())?;
            let lhs = doc.get(field).unwrap_or(&Value::Null);
            apply_op(op, lhs, rhs)
        }
    }
}

fn apply_op(op: &str, lhs: &Value, rhs: &Value) -> Result<bool, String> {
    match op {
        s if s.eq_ignore_ascii_case("Eq") => Ok(lhs == rhs),
        s if s.eq_ignore_ascii_case("NotEq") => Ok(lhs != rhs),
        s if s.eq_ignore_ascii_case("Gt") => Ok(cmp_values(lhs, rhs)? == Ordering::Greater),
        s if s.eq_ignore_ascii_case("Gte") => Ok(cmp_values(lhs, rhs)? != Ordering::Less),
        s if s.eq_ignore_ascii_case("Lt") => Ok(cmp_values(lhs, rhs)? == Ordering::Less),
        s if s.eq_ignore_ascii_case("Lte") => Ok(cmp_values(lhs, rhs)? != Ordering::Greater),
        s if s.eq_ignore_ascii_case("In") => {
            let arr = rhs
                .as_array()
                .ok_or_else(|| "In expects array rhs".to_string())?;
            Ok(arr.contains(lhs))
        }
        s if s.eq_ignore_ascii_case("NotIn") => {
            let arr = rhs
                .as_array()
                .ok_or_else(|| "NotIn expects array rhs".to_string())?;
            Ok(!arr.contains(lhs))
        }
        other => Err(format!("unsupported filter op '{}'", other)),
    }
}

fn cmp_values(a: &Value, b: &Value) -> Result<Ordering, String> {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            let xf = x
                .as_f64()
                .ok_or_else(|| "non-finite number lhs".to_string())?;
            let yf = y
                .as_f64()
                .ok_or_else(|| "non-finite number rhs".to_string())?;
            xf.partial_cmp(&yf)
                .ok_or_else(|| "NaN comparison".to_string())
        }
        (Value::String(x), Value::String(y)) => Ok(x.cmp(y)),
        (Value::Bool(x), Value::Bool(y)) => Ok(x.cmp(y)),
        _ => Err(format!("cannot compare {} with {}", a, b)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn doc(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn rank_by_rejects_empty_fts_query() {
        let fts = FtsScan {
            field: "body".into(),
            query: "".into(),
        };
        let err = build_rank_by(&RankedSelector::Fts(&fts)).unwrap_err();
        match err {
            AppError::Validation(msg) => assert!(msg.contains("non-empty")),
            _ => panic!("expected validation error"),
        }
    }

    #[test]
    fn rank_by_requires_finite_radius() {
        let ann = AnnScan {
            vector: vec![0.1, 0.2],
            field: None,
            radius: f64::NAN,
        };
        let err = build_rank_by(&RankedSelector::Ann(&ann)).unwrap_err();
        match err {
            AppError::Validation(msg) => assert!(msg.contains("finite")),
            _ => panic!("expected validation error"),
        }
    }

    #[test]
    fn radius_scan_returns_gateway_side_bound() {
        let ann = AnnScan {
            vector: vec![1.0, 2.0, 3.0],
            field: None,
            radius: 0.25,
        };
        let (_, mode, ann_radius) = build_rank_by(&RankedSelector::Ann(&ann)).unwrap();
        assert_eq!(mode, RankMode::Ann);
        assert_eq!(ann_radius, Some(0.25));
    }

    #[test]
    fn fts_scan_has_no_radius_bound() {
        let fts = FtsScan {
            field: "title".into(),
            query: "wireless".into(),
        };
        let (rank_by, mode, ann_radius) = build_rank_by(&RankedSelector::Fts(&fts)).unwrap();
        assert_eq!(rank_by, json!(["title", "BM25", "wireless"]));
        assert_eq!(mode, RankMode::Bm25);
        assert!(ann_radius.is_none());
    }

    #[test]
    fn hybrid_text_scan_builds_bm25_and_fuzzy_union_legs() {
        let scan = HybridTextScan {
            field: "title".into(),
            query: "wireles headphones".into(),
            fuzziness: None,
        };
        let base = json!(["category", "Eq", "Electronics"]);
        let specs = hybrid_text_leg_specs(&scan, Some(&base), true).unwrap();

        assert_eq!(specs.len(), 5);
        assert_eq!(specs[0].label, "bm25");
        assert_eq!(
            specs[0].rank_by,
            json!(["title", "BM25", "wireles headphones"])
        );
        assert_eq!(specs[0].filter, Some(base.clone()));
        assert!(specs.iter().any(|spec| {
            spec.label == "fuzzy:wireles"
                && spec.filter
                    == Some(json!([
                        "And",
                        [
                            base.clone(),
                            [
                                "title",
                                "Fuzzy",
                                "wireles",
                                {"max_edit_distance": [
                                    {"min_query_chars": 3, "distance": 0},
                                    {"min_query_chars": 6, "distance": 1},
                                    {"min_query_chars": 9, "distance": 2}
                                ]}
                            ]
                        ]
                    ]))
        }));
    }

    #[test]
    fn next_page_filter_bm25_is_descending_with_id_tiebreak() {
        let f = next_page_filter(RankMode::Bm25, 0.5, "doc-100").unwrap();
        assert_eq!(
            f,
            json!([
                "Or",
                [
                    ["$score", "Lt", 0.5],
                    ["And", [["$score", "Eq", 0.5], ["id", "Gt", "doc-100"]]]
                ]
            ])
        );
    }

    #[test]
    fn next_page_filter_ann_is_not_supported() {
        assert!(next_page_filter(RankMode::Ann, 0.25, "doc-1").is_none());
    }

    #[test]
    fn record_page_marks_exhausted_when_short() {
        let mut shard = ShardState::default();
        let rows: Vec<QueryResult> = (0..5)
            .map(|i| QueryResult {
                id: format!("d-{i}"),
                dist: Some(i as f64),
                attributes: HashMap::new(),
            })
            .collect();
        record_page(&mut shard, rows, RankMode::Bm25, None, None).unwrap();
        assert_eq!(shard.count, 5);
        assert!(shard.exhausted);
    }

    #[test]
    fn record_page_carries_last_score_when_saturated() {
        let mut shard = ShardState::default();
        let rows: Vec<QueryResult> = (0..TPUF_TOP_K_MAX as usize)
            .map(|i| QueryResult {
                id: format!("d-{:06}", i),
                dist: Some(i as f64 / TPUF_TOP_K_MAX as f64),
                attributes: HashMap::new(),
            })
            .collect();
        record_page(&mut shard, rows, RankMode::Bm25, None, None).unwrap();
        assert!(!shard.exhausted);
        assert_eq!(shard.count, TPUF_TOP_K_MAX as u64);
        assert!(shard.last_score.is_some());
        assert!(shard.last_id.is_some());
    }

    #[test]
    fn record_page_ann_stops_at_radius_boundary() {
        let mut shard = ShardState::default();
        let rows = vec![
            QueryResult {
                id: "d-1".into(),
                dist: Some(0.1),
                attributes: HashMap::new(),
            },
            QueryResult {
                id: "d-2".into(),
                dist: Some(0.2),
                attributes: HashMap::new(),
            },
            QueryResult {
                id: "d-3".into(),
                dist: Some(0.3),
                attributes: HashMap::new(),
            },
        ];
        record_page(&mut shard, rows, RankMode::Ann, Some(0.2), None).unwrap();
        assert_eq!(shard.count, 2);
        assert!(shard.exhausted);
        assert!(shard.last_score.is_none());
    }

    #[test]
    fn record_page_ann_over_radius_exhausts_even_when_page_saturated() {
        let mut shard = ShardState::default();
        let rows: Vec<QueryResult> = (0..TPUF_TOP_K_MAX as usize)
            .map(|i| QueryResult {
                id: format!("d-{:06}", i),
                dist: Some(if i < 3 { 0.1 + i as f64 * 0.05 } else { 0.9 }),
                attributes: HashMap::new(),
            })
            .collect();
        record_page(&mut shard, rows, RankMode::Ann, Some(0.21), None).unwrap();
        assert_eq!(shard.count, 3);
        assert!(shard.exhausted);
        assert!(shard.last_score.is_none());
    }

    #[test]
    fn record_page_ann_saturated_inside_radius_remains_bounded() {
        let mut shard = ShardState::default();
        let rows: Vec<QueryResult> = (0..TPUF_TOP_K_MAX as usize)
            .map(|i| QueryResult {
                id: format!("d-{:06}", i),
                dist: Some(i as f64 / TPUF_TOP_K_MAX as f64),
                attributes: HashMap::new(),
            })
            .collect();
        record_page(&mut shard, rows, RankMode::Ann, Some(2.0), None).unwrap();
        assert_eq!(shard.count, TPUF_TOP_K_MAX as u64);
        assert!(!shard.exhausted);
        assert!(shard.last_score.is_some());
        assert!(shard.last_id.is_some());
    }

    #[test]
    fn compose_ands_user_filter_and_extra_filter() {
        let user = json!(["color", "Eq", "red"]);
        let extra = json!(["category", "Eq", "sale"]);
        let combined = compose(Some(&user), Some(extra.clone())).unwrap();
        assert_eq!(combined, json!(["And", [user, extra]]));
    }

    #[test]
    fn eq_filter() {
        let d = doc(&[("category", json!("Electronics"))]);
        assert!(match_filter(&d, &json!(["category", "Eq", "Electronics"])).unwrap());
        assert!(!match_filter(&d, &json!(["category", "Eq", "Books"])).unwrap());
    }

    #[test]
    fn numeric_comparisons() {
        let d = doc(&[("price", json!(120))]);
        assert!(match_filter(&d, &json!(["price", "Lte", 200])).unwrap());
        assert!(match_filter(&d, &json!(["price", "Gt", 100])).unwrap());
        assert!(!match_filter(&d, &json!(["price", "Lt", 100])).unwrap());
    }

    #[test]
    fn in_filter() {
        let d = doc(&[("category", json!("Books"))]);
        assert!(match_filter(&d, &json!(["category", "In", ["Books", "DVD"]])).unwrap());
        assert!(!match_filter(&d, &json!(["category", "NotIn", ["Books", "DVD"]])).unwrap());
    }

    #[test]
    fn compound_filters() {
        let d = doc(&[("category", json!("Electronics")), ("price", json!(150))]);
        let f = json!([
            "And",
            [["category", "Eq", "Electronics"], ["price", "Lt", 200]]
        ]);
        assert!(match_filter(&d, &f).unwrap());

        let f = json!(["Or", [["category", "Eq", "Books"], ["price", "Lt", 100]]]);
        assert!(!match_filter(&d, &f).unwrap());
    }

    #[test]
    fn missing_field_is_null() {
        let d = doc(&[]);
        assert!(!match_filter(&d, &json!(["price", "Eq", 5])).unwrap());
    }

    #[test]
    fn unsupported_op_errors() {
        let d = doc(&[("title", json!("hi"))]);
        let r = match_filter(&d, &json!(["title", "Regex", ".*"]));
        assert!(r.is_err());
        assert!(!filter_supported(&json!(["title", "Regex", ".*"])));
    }

    #[test]
    fn filter_supported_walks_compound() {
        assert!(filter_supported(&json!([
            "And",
            [["a", "Eq", 1], ["b", "Lt", 2]]
        ])));
        assert!(!filter_supported(&json!([
            "And",
            [["a", "Eq", 1], ["b", "Regex", ".*"]]
        ])));
    }

    #[test]
    fn scan_cache_demand_skips_snapshot_sources() {
        assert!(!should_record_scan_cache_demand(
            false,
            ResolvedSource::Snapshot
        ));
        assert!(should_record_scan_cache_demand(
            false,
            ResolvedSource::Cache
        ));
        assert!(should_record_scan_cache_demand(
            false,
            ResolvedSource::Origin
        ));
    }

    #[test]
    fn explicit_cache_scan_records_demand_before_enqueue_only() {
        assert!(!should_record_scan_cache_demand(
            true,
            ResolvedSource::Cache
        ));
    }
}
