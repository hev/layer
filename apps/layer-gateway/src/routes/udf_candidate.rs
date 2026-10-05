//! Candidate runs for Functions.
//!
//! A candidate is a second registration derived from a live Function
//! (`<id>-candidate`). It reuses the whole queue, claim and completion
//! machinery, but its completions land in shadow attributes
//! (`_hevlayer_cand_<function>_<output>`) instead of the live outputs.
//! Compare reads both columns; promote copies shadow to live in one step.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::clients::turbopuffer::PatchColumns;
use crate::error::AppError;
use crate::models::{GetUdfResponse, StatusResponse, UdfCandidateSpec, UdfSpec, UdfWorkerSpec};
use crate::routes::udf::{
    candidate_shadow_attr, map_udf_error, marker_columns_for_ids, patch_tpuf_and_cache,
    udf_response, udf_status_response, udf_version_marker_attr, validate_udf_spec,
};
use crate::udf::UdfResource;
use crate::AppState;

const SCAN_PAGE_SIZE: u32 = 500;
const DEFAULT_COMPARE_ROWS: u32 = 10_000;
const MAX_COMPARE_ROWS: u32 = 100_000;
const DEFAULT_SAMPLE: u32 = 10;
const MAX_SAMPLE: u32 = 100;

pub fn candidate_id(live_id: &str) -> String {
    format!("{live_id}-candidate")
}

#[derive(Debug, Deserialize)]
pub struct CreateCandidateRequest {
    /// Version stamped on rows the candidate completes.
    pub version: String,
    /// Live output attributes the candidate produces.
    pub outputs: Vec<String>,
    #[serde(default)]
    pub inputs: Option<Vec<String>>,
    #[serde(default)]
    pub filter: Option<Value>,
    #[serde(default)]
    pub worker: Option<UdfWorkerSpec>,
    #[serde(default)]
    pub target_namespaces: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize)]
pub struct PromoteCandidateRequest {
    /// Promote even if the candidate queue still has pending or processing rows.
    #[serde(default)]
    pub allow_partial: bool,
    /// Namespaces to promote. Defaults to the candidate's `target_namespaces`.
    #[serde(default)]
    pub namespaces: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct CompareQuery {
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default)]
    pub max_rows: Option<u32>,
    #[serde(default)]
    pub sample: Option<u32>,
}

async fn get_live(state: &Arc<AppState>, id: &str) -> Result<UdfResource, AppError> {
    state
        .udf_store()
        .get_udf(id)
        .await
        .map_err(map_udf_error)?
        .ok_or_else(|| AppError::NotFound(format!("UDF '{id}' not found")))
}

async fn get_candidate(
    state: &Arc<AppState>,
    live_id: &str,
) -> Result<(UdfResource, UdfCandidateSpec), AppError> {
    let candidate = state
        .udf_store()
        .get_udf(&candidate_id(live_id))
        .await
        .map_err(map_udf_error)?
        .filter(|udf| udf.spec.candidate.as_ref().is_some_and(|c| c.of == live_id))
        .ok_or_else(|| AppError::NotFound(format!("UDF '{live_id}' has no candidate run")))?;
    let spec = candidate.spec.candidate.clone().expect("filtered above");
    Ok((candidate, spec))
}

pub async fn create_candidate(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(request): Json<CreateCandidateRequest>,
) -> Result<Json<GetUdfResponse>, AppError> {
    let live = get_live(&state, &id).await?;
    if live.spec.candidate.is_some() {
        return Err(AppError::Validation(
            "a candidate run cannot have its own candidate".into(),
        ));
    }
    if request.outputs.is_empty() {
        return Err(AppError::Validation("outputs must not be empty".into()));
    }
    for output in &request.outputs {
        if output.trim().is_empty() || output.starts_with("_hevlayer_") {
            return Err(AppError::Validation(format!(
                "outputs entry '{output}' must be a non-empty, non-reserved attribute name"
            )));
        }
    }
    let mut spec: UdfSpec = live.spec.clone();
    spec.version = request.version;
    if let Some(inputs) = request.inputs {
        spec.inputs = inputs;
    }
    if request.filter.is_some() {
        spec.filter = request.filter;
    }
    if let Some(worker) = request.worker {
        spec.worker = worker;
    }
    if let Some(namespaces) = request.target_namespaces {
        spec.target_namespaces = namespaces;
    }
    // A candidate never marks dependents stale; promotion does.
    spec.invalidates = Vec::new();
    spec.candidate = Some(UdfCandidateSpec {
        of: id.clone(),
        outputs: request.outputs,
    });
    let cid = candidate_id(&id);
    validate_udf_spec(&cid, &spec)?;
    let created = state
        .udf_store()
        .create_udf_paused(&cid, &spec, live.paused)
        .await
        .map_err(map_udf_error)?;
    candidate_response(&state, created).await
}

pub async fn get_candidate_run(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<GetUdfResponse>, AppError> {
    let (candidate, _) = get_candidate(&state, &id).await?;
    candidate_response(&state, candidate).await
}

async fn candidate_response(
    state: &Arc<AppState>,
    candidate: UdfResource,
) -> Result<Json<GetUdfResponse>, AppError> {
    let status = state
        .udf_store()
        .get_status(&candidate.id)
        .await
        .map_err(map_udf_error)?;
    Ok(Json(GetUdfResponse {
        udf: udf_response(candidate),
        status: udf_status_response(status),
    }))
}

/// Filter selecting rows the candidate completed, or `None` when the namespace
/// cannot contain any (it is missing, or its schema lacks the marker).
async fn completed_filter(
    state: &Arc<AppState>,
    namespace: &str,
    candidate: &UdfResource,
) -> Result<Option<Value>, AppError> {
    let marker = udf_version_marker_attr(&candidate.id);
    let meta = match state.turbopuffer().head_namespace(namespace).await {
        Ok(meta) => meta,
        Err(e) if e.is_not_found() => return Ok(None),
        Err(e) => {
            return Err(AppError::Upstream(format!(
                "candidate metadata failed: {e}"
            )))
        }
    };
    if let Some(schema) = meta.raw.get("schema").and_then(Value::as_object) {
        if !schema.contains_key(&marker) {
            return Ok(None);
        }
    }
    Ok(Some(json!([marker, "Eq", candidate.spec.version])))
}

fn namespaces_for(
    candidate: &UdfResource,
    requested: Vec<String>,
) -> Result<Vec<String>, AppError> {
    let mut namespaces = if requested.is_empty() {
        candidate.spec.target_namespaces.clone()
    } else {
        requested
    };
    namespaces.sort();
    namespaces.dedup();
    if namespaces.is_empty() {
        return Err(AppError::Validation(
            "candidate has no target_namespaces; pass a namespace".into(),
        ));
    }
    Ok(namespaces)
}

pub async fn compare_candidate(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(query): Query<CompareQuery>,
) -> Result<Json<Value>, AppError> {
    crate::metrics::scope_billing_caller(
        crate::metrics::BillingCaller::function(&id),
        compare_inner(state, id, query),
    )
    .await
}

async fn compare_inner(
    state: Arc<AppState>,
    id: String,
    query: CompareQuery,
) -> Result<Json<Value>, AppError> {
    let live = get_live(&state, &id).await?;
    let (candidate, spec) = get_candidate(&state, &id).await?;
    let max_rows = query.max_rows.unwrap_or(DEFAULT_COMPARE_ROWS);
    if !(1..=MAX_COMPARE_ROWS).contains(&max_rows) {
        return Err(AppError::Validation(format!(
            "max_rows must be 1..{MAX_COMPARE_ROWS}"
        )));
    }
    let sample = query.sample.unwrap_or(DEFAULT_SAMPLE);
    if sample > MAX_SAMPLE {
        return Err(AppError::Validation(format!(
            "sample must be 0..{MAX_SAMPLE}"
        )));
    }
    let namespaces = namespaces_for(&candidate, query.namespace.into_iter().collect())?;
    let shadows: Vec<String> = spec
        .outputs
        .iter()
        .map(|output| candidate_shadow_attr(&id, output))
        .collect();
    let include: Vec<String> = spec.outputs.iter().chain(&shadows).cloned().collect();

    let mut stats: Vec<OutputStats> = spec
        .outputs
        .iter()
        .map(|_| OutputStats::default())
        .collect();
    let mut rows = 0u64;
    let mut rows_agreed = 0u64;
    let mut samples: Vec<Value> = Vec::new();
    let mut truncated = false;
    'namespaces: for namespace in &namespaces {
        let Some(filter) = completed_filter(&state, namespace, &candidate).await? else {
            continue;
        };
        let mut cursor: Option<String> = None;
        loop {
            let remaining = max_rows as u64 - rows;
            let page = state
                .turbopuffer()
                .scan_page(
                    namespace,
                    cursor.as_deref(),
                    SCAN_PAGE_SIZE.min(remaining as u32),
                    Some(&filter),
                    Some(&include),
                )
                .await
                .map_err(|e| AppError::Upstream(format!("candidate scan failed: {e}")))?;
            for doc in page.documents {
                rows += 1;
                let mut row_agrees = true;
                for (index, output) in spec.outputs.iter().enumerate() {
                    let live_value = doc.attributes.get(output).unwrap_or(&Value::Null);
                    let candidate_value =
                        doc.attributes.get(&shadows[index]).unwrap_or(&Value::Null);
                    let agree = stats[index].record(live_value, candidate_value);
                    row_agrees &= agree;
                    if !agree && samples.len() < sample as usize {
                        samples.push(json!({
                            "namespace": namespace,
                            "id": doc.id,
                            "output": output,
                            "live": live_value,
                            "candidate": candidate_value,
                        }));
                    }
                }
                rows_agreed += row_agrees as u64;
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
            if rows >= max_rows as u64 {
                truncated = true;
                break 'namespaces;
            }
        }
    }

    let status = state
        .udf_store()
        .get_status(&candidate.id)
        .await
        .map_err(map_udf_error)?;
    let outputs: Vec<Value> = spec
        .outputs
        .iter()
        .zip(stats)
        .map(|(output, stats)| stats.into_json(output))
        .collect();
    Ok(Json(json!({
        "udf_id": id,
        "candidate_id": candidate.id,
        "live_version": live.spec.version,
        "candidate_version": candidate.spec.version,
        "rows_compared": rows,
        "rows_agreed": rows_agreed,
        "agreement_rate": rate(rows_agreed, rows),
        "truncated": truncated,
        "pending_count": status.pending_count,
        "processing_count": status.processing_count,
        "failed_count": status.failed_count,
        "outputs": outputs,
        "disagreements": samples,
    })))
}

fn rate(agreed: u64, total: u64) -> Option<f64> {
    (total > 0).then(|| agreed as f64 / total as f64)
}

#[derive(Default)]
struct OutputStats {
    compared: u64,
    agreed: u64,
    categorical: bool,
    seen_non_scalar: bool,
    confusion: BTreeMap<String, BTreeMap<String, u64>>,
}

impl OutputStats {
    fn record(&mut self, live: &Value, candidate: &Value) -> bool {
        let agree = values_agree(live, candidate);
        self.compared += 1;
        self.agreed += agree as u64;
        match (label(live), label(candidate)) {
            (Some(l), Some(c)) if !self.seen_non_scalar => {
                *self.confusion.entry(l).or_default().entry(c).or_default() += 1;
                self.categorical = true;
            }
            _ => {
                self.seen_non_scalar = true;
                self.categorical = false;
                self.confusion.clear();
            }
        }
        agree
    }

    fn into_json(self, output: &str) -> Value {
        json!({
            "output": output,
            "compared": self.compared,
            "agreed": self.agreed,
            "agreement_rate": rate(self.agreed, self.compared),
            // Row counts by live value, then candidate value. Present for
            // outputs whose values are all scalars (strings, numbers, booleans)
            // or null; null shows as "null".
            "confusion": self.categorical.then_some(self.confusion),
        })
    }
}

fn label(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Array(_) | Value::Object(_) => None,
        other => Some(other.to_string()),
    }
}

/// Arrays compare as multisets: tag order is not a disagreement. Numbers
/// compare by value, so `1` and `1.0` agree.
fn values_agree(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Array(x), Value::Array(y)) => {
            let mut x: Vec<String> = x.iter().map(Value::to_string).collect();
            let mut y: Vec<String> = y.iter().map(Value::to_string).collect();
            x.sort();
            y.sort();
            x == y
        }
        (Value::Number(x), Value::Number(y)) => x.as_f64() == y.as_f64(),
        _ => a == b,
    }
}

pub async fn promote_candidate(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(request): Json<PromoteCandidateRequest>,
) -> Result<Json<Value>, AppError> {
    crate::metrics::scope_billing_caller(
        crate::metrics::BillingCaller::function(&id),
        promote_inner(state, id, request),
    )
    .await
}

async fn promote_inner(
    state: Arc<AppState>,
    id: String,
    request: PromoteCandidateRequest,
) -> Result<Json<Value>, AppError> {
    let live = get_live(&state, &id).await?;
    let (candidate, spec) = get_candidate(&state, &id).await?;
    let namespaces = namespaces_for(&candidate, request.namespaces)?;
    if !request.allow_partial {
        let status = state
            .udf_store()
            .get_status(&candidate.id)
            .await
            .map_err(map_udf_error)?;
        if status.pending_count + status.processing_count > 0 {
            return Err(AppError::Conflict(format!(
                "candidate still has {} pending and {} processing rows; wait for it or pass allow_partial",
                status.pending_count, status.processing_count
            )));
        }
    }

    let mut promoted_spec = live.spec.clone();
    promoted_spec.version = candidate.spec.version.clone();
    promoted_spec.inputs = candidate.spec.inputs.clone();
    promoted_spec.filter = candidate.spec.filter.clone();
    promoted_spec.worker = candidate.spec.worker.clone();
    promoted_spec.target_namespaces = candidate.spec.target_namespaces.clone();

    // Stop both queues while shadow values move so no claim or discovery
    // races the copy. Restore the live pause state whatever happens.
    state
        .udf_store()
        .set_paused(&candidate.id, true)
        .await
        .map_err(map_udf_error)?;
    state
        .udf_store()
        .set_paused(&id, true)
        .await
        .map_err(map_udf_error)?;
    let result = async {
        let mut promoted = 0;
        for namespace in &namespaces {
            promoted += move_shadows(
                &state,
                &candidate,
                &spec,
                namespace,
                Some((&id, &promoted_spec)),
            )
            .await?;
        }
        state
            .udf_store()
            .upsert_udf(&id, &promoted_spec)
            .await
            .map_err(map_udf_error)?;
        state
            .udf_store()
            .delete_udf(&candidate.id)
            .await
            .map_err(map_udf_error)?;
        Ok::<_, AppError>(promoted)
    }
    .await;
    if !live.paused {
        state
            .udf_store()
            .set_paused(&id, false)
            .await
            .map_err(map_udf_error)?;
    }
    for udf_id in [&id, &candidate.id] {
        state.udf_status_cache.remove(udf_id);
        state.udf_status_inflight.remove(udf_id);
    }
    let promoted = result?;
    Ok(Json(json!({
        "udf_id": id,
        "promoted_rows": promoted,
        "version": promoted_spec.version,
    })))
}

pub async fn discard_candidate(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<StatusResponse>, AppError> {
    let (candidate, spec) = get_candidate(&state, &id).await?;
    state
        .udf_store()
        .set_paused(&candidate.id, true)
        .await
        .map_err(map_udf_error)?;
    for namespace in namespaces_for(&candidate, Vec::new())? {
        move_shadows(&state, &candidate, &spec, &namespace, None).await?;
    }
    state
        .udf_store()
        .delete_udf(&candidate.id)
        .await
        .map_err(map_udf_error)?;
    state.udf_status_cache.remove(&candidate.id);
    state.udf_status_inflight.remove(&candidate.id);
    Ok(Json(StatusResponse::default()))
}

/// Clear the candidate's shadow attributes and completion marker on every row
/// it completed in `namespace`. With `promote`, copy each shadow into the live
/// output and stamp the live Function's markers in the same patch.
async fn move_shadows(
    state: &Arc<AppState>,
    candidate: &UdfResource,
    spec: &UdfCandidateSpec,
    namespace: &str,
    promote: Option<(&str, &UdfSpec)>,
) -> Result<u64, AppError> {
    let Some(filter) = completed_filter(state, namespace, candidate).await? else {
        return Ok(0);
    };
    let shadows: Vec<String> = spec
        .outputs
        .iter()
        .map(|output| candidate_shadow_attr(&spec.of, output))
        .collect();
    let marker = udf_version_marker_attr(&candidate.id);
    let mut moved = 0;
    let mut cursor: Option<String> = None;
    loop {
        let page = state
            .turbopuffer()
            .scan_page(
                namespace,
                cursor.as_deref(),
                SCAN_PAGE_SIZE,
                Some(&filter),
                Some(&shadows),
            )
            .await
            .map_err(|e| AppError::Upstream(format!("candidate scan failed: {e}")))?;
        if !page.documents.is_empty() {
            let ids: Vec<&str> = page.documents.iter().map(|doc| doc.id.as_str()).collect();
            let mut columns: HashMap<String, Vec<Value>> = match promote {
                Some((live_id, live_spec)) => {
                    marker_columns_for_ids(state, live_id, live_spec, &ids)
                }
                None => HashMap::new(),
            };
            for (output, shadow) in spec.outputs.iter().zip(&shadows) {
                if promote.is_some() {
                    columns.insert(
                        output.clone(),
                        page.documents
                            .iter()
                            .map(|doc| doc.attributes.get(shadow).cloned().unwrap_or(Value::Null))
                            .collect(),
                    );
                }
                columns.insert(shadow.clone(), vec![Value::Null; ids.len()]);
            }
            columns.insert(marker.clone(), vec![Value::Null; ids.len()]);
            let patch =
                PatchColumns::new(ids.iter().map(|id| id.to_string()).collect(), columns)
                    .map_err(|e| AppError::Validation(format!("invalid candidate patch: {e}")))?;
            let guard = state
                .udf_store()
                .lock_namespaces(&[namespace.to_string()])
                .await
                .map_err(map_udf_error)?;
            patch_tpuf_and_cache(state, namespace, &patch).await?;
            guard.validate().await.map_err(map_udf_error)?;
            moved += ids.len() as u64;
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            return Ok(moved);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrays_agree_regardless_of_order_and_numbers_by_value() {
        assert!(values_agree(&json!(["a", "b"]), &json!(["b", "a"])));
        assert!(!values_agree(&json!(["a"]), &json!(["a", "b"])));
        assert!(values_agree(&json!(1), &json!(1.0)));
        assert!(!values_agree(&json!("a"), &Value::Null));
    }

    #[test]
    fn confusion_counts_live_by_candidate_and_drops_for_non_scalars() {
        let mut stats = OutputStats::default();
        assert!(stats.record(&json!("invoice"), &json!("invoice")));
        assert!(!stats.record(&json!("invoice"), &json!("receipt")));
        assert!(!stats.record(&Value::Null, &json!("receipt")));
        let out = stats.into_json("doc_type");
        assert_eq!(out["agreed"], 1);
        assert_eq!(out["confusion"]["invoice"]["receipt"], 1);
        assert_eq!(out["confusion"]["null"]["receipt"], 1);

        let mut stats = OutputStats::default();
        stats.record(&json!("a"), &json!("a"));
        stats.record(&json!(["a"]), &json!(["a"]));
        assert!(stats.into_json("tags")["confusion"].is_null());
    }
}
