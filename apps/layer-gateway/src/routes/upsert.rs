use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use axum::body::{Body, Bytes};
use axum::extract::{OriginalUri, Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{Map, Value};
use tracing::warn;

use crate::clients::turbopuffer::{
    PatchDoc, TurbopufferPassthroughResponse, UpsertDoc, UPSERTED_AT_ATTR,
};
use crate::consistency::now_ms;
use crate::error::AppError;
use crate::metrics::{DIRECT_PIPELINE_ID, STATUS_OK, STATUS_TPUF_ERROR};
use crate::models::StatusResponse;
use crate::shards::{read_namespace_marker, shard_for_id, SHARD_ATTR};

use crate::AppState;

#[derive(Debug, Clone)]
struct UpsertEffect {
    id: String,
    attributes: HashMap<String, Value>,
    vector: Option<Vec<f64>>,
    vectors: Option<Vec<Vec<f64>>>,
}

#[derive(Debug, Clone)]
struct PatchEffect {
    id: String,
    attributes: HashMap<String, Value>,
}

#[derive(Debug, Default)]
struct NativeWritePlan {
    upserts: Vec<UpsertEffect>,
    patches: Vec<PatchEffect>,
    deletes: Vec<String>,
    filter_delete: Option<Value>,
    filter_patch_attrs: Option<HashMap<String, Value>>,
    has_filter_delete: bool,
    has_filter_patch: bool,
    has_condition: bool,
}

impl NativeWritePlan {
    fn is_managed(&self) -> bool {
        !self.upserts.is_empty()
            || !self.patches.is_empty()
            || !self.deletes.is_empty()
            || self.has_filter_delete
            || self.has_filter_patch
    }

    fn response_driven(&self) -> bool {
        self.has_condition || self.has_filter_delete || self.has_filter_patch
    }

    fn portable_without_affected_ids(&self) -> bool {
        !self.has_condition
            && self.has_filter_delete
            && !self.has_filter_patch
            && self.upserts.is_empty()
            && self.patches.is_empty()
            && self.deletes.is_empty()
    }

    fn metric_batch_size(&self) -> usize {
        self.upserts.len()
            + self.patches.len()
            + usize::from(self.has_filter_patch)
            + usize::from(self.has_filter_delete)
    }
}

#[derive(Debug, Default)]
struct AffectedIds {
    upserted: HashSet<String>,
    patched: HashSet<String>,
    deleted: HashSet<String>,
}

/// POST /v2/namespaces/{namespace}
///
/// Native Turbopuffer write bodies are the only public write surface. The
/// gateway enriches row-producing native shapes in place, applies Layer cache
/// and UDF side effects, and returns the upstream Turbopuffer response body.
pub async fn upsert_or_delete(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
    OriginalUri(uri): OriginalUri,
    Json(mut body): Json<Value>,
) -> Result<Response, AppError> {
    let search_store = state.namespace_uses_search_store(&namespace);
    let embed = crate::routes::embed_wire::prepare_write(
        state.as_ref(),
        &namespace,
        &mut body,
        search_store,
    )
    .await?;
    if crate::routes::embed_wire::write_needs_distance_metric(&body, embed.requires_distance_check)
    {
        let has_existing_embed_schema = match state.turbopuffer().head_namespace(&namespace).await {
            Ok(metadata) => crate::routes::embed_wire::metadata_has_embed_schema(&metadata.raw),
            Err(error) if error.is_not_found() => false,
            Err(error) => return Err(AppError::from_turbopuffer(error, "turbopuffer metadata")),
        };
        if !has_existing_embed_schema {
            return Err(AppError::Validation(
                "`distance_metric` is required on the first write to a namespace with an `embed` schema attribute".to_string(),
            ));
        }
    }
    let object_schema_attributes = object_schema_attributes(&body);
    if !object_schema_attributes.is_empty() && search_store {
        return Err(unsupported_object_schema_error(
            "search",
            &object_schema_attributes,
        ));
    }
    let effective_shard_count = read_namespace_marker(state.turbopuffer(), &namespace)
        .await
        .map_err(|e| AppError::Upstream(format!("namespace marker read failed: {e}")))?
        .unwrap_or(state.shard_count);
    let mut plan = native_write_plan(
        &mut body,
        effective_shard_count,
        embed.generated_chunk_attributes,
    )?;
    if !plan.is_managed() {
        let response = crate::routes::turbopuffer::passthrough(
            Arc::clone(&state),
            "POST",
            uri.path(),
            uri.query(),
            Some(body),
        )
        .await?;
        if response.status().is_success() {
            crate::routes::embed_wire::commit_profiles(&state, &namespace, &embed).await?;
        }
        return Ok(response);
    }

    let total_start = Instant::now();
    let mut tpuf_seconds = 0.0;
    let metric_batch_size = plan.metric_batch_size();
    state.observe_cache_demand(&namespace);

    if plan.response_driven() {
        force_return_affected_ids(&mut body);
    } else {
        apply_prewrite_cache(&state, &namespace, &plan).await;
    }

    let tpuf_start = Instant::now();
    let mut upstream = match state
        .turbopuffer()
        .passthrough("POST", uri.path(), uri.query(), Some(body))
        .await
    {
        Ok(response) => response,
        Err(e)
            if is_unsupported_by_store(&e)
                && (!plan.response_driven() || plan.portable_without_affected_ids()) =>
        {
            match portable_write(&state, &namespace, &plan).await {
                Ok(response) => response,
                Err(e) => {
                    tpuf_seconds += tpuf_start.elapsed().as_secs_f64();
                    observe_write_metric(
                        &state,
                        &namespace,
                        STATUS_TPUF_ERROR,
                        total_start.elapsed().as_secs_f64(),
                        tpuf_seconds,
                        metric_batch_size,
                    );
                    if is_unsupported_by_store(&e) {
                        return Err(AppError::from_store_support_error(
                            e,
                            Some("search".to_string()),
                            Some("writeNamespace".to_string()),
                        ));
                    }
                    return Err(AppError::Upstream(format!("VectorStore write failed: {e}")));
                }
            }
        }
        Err(e) => {
            tpuf_seconds += tpuf_start.elapsed().as_secs_f64();
            observe_write_metric(
                &state,
                &namespace,
                STATUS_TPUF_ERROR,
                total_start.elapsed().as_secs_f64(),
                tpuf_seconds,
                metric_batch_size,
            );
            return Err(AppError::Upstream(format!(
                "Turbopuffer write failed: {}",
                e
            )));
        }
    };
    tpuf_seconds += tpuf_start.elapsed().as_secs_f64();

    let mut status = StatusCode::from_u16(upstream.status)
        .map_err(|e| AppError::Upstream(format!("invalid Turbopuffer status: {}", e)))?;
    if !status.is_success()
        && !object_schema_attributes.is_empty()
        && upstream_rejected_object_schema(&upstream)
    {
        observe_write_metric(
            &state,
            &namespace,
            STATUS_TPUF_ERROR,
            total_start.elapsed().as_secs_f64(),
            tpuf_seconds,
            metric_batch_size,
        );
        return Err(unsupported_object_schema_error(
            "turbopuffer",
            &object_schema_attributes,
        ));
    }
    if !status.is_success()
        && (!plan.response_driven() || plan.portable_without_affected_ids())
        && String::from_utf8_lossy(&upstream.body).contains("UnsupportedByStore")
    {
        let portable_start = Instant::now();
        upstream = match portable_write(&state, &namespace, &plan).await {
            Ok(response) => response,
            Err(e) => {
                tpuf_seconds += portable_start.elapsed().as_secs_f64();
                observe_write_metric(
                    &state,
                    &namespace,
                    STATUS_TPUF_ERROR,
                    total_start.elapsed().as_secs_f64(),
                    tpuf_seconds,
                    metric_batch_size,
                );
                if is_unsupported_by_store(&e) {
                    return Err(AppError::from_store_support_error(
                        e,
                        Some("search".to_string()),
                        Some("writeNamespace".to_string()),
                    ));
                }
                return Err(AppError::Upstream(format!("VectorStore write failed: {e}")));
            }
        };
        tpuf_seconds += portable_start.elapsed().as_secs_f64();
        status = StatusCode::from_u16(upstream.status)
            .map_err(|e| AppError::Upstream(format!("invalid VectorStore status: {}", e)))?;
    }
    if !status.is_success() {
        observe_write_metric(
            &state,
            &namespace,
            STATUS_TPUF_ERROR,
            total_start.elapsed().as_secs_f64(),
            tpuf_seconds,
            metric_batch_size,
        );
        return passthrough_response(upstream);
    }

    crate::routes::embed_wire::merge_response_performance(&mut upstream.body, &embed.performance);
    crate::routes::embed_wire::commit_profiles(&state, &namespace, &embed).await?;

    if plan.response_driven() && !plan.portable_without_affected_ids() {
        let affected = affected_ids_from_response(&upstream.body);
        apply_response_driven_effects(&state, &namespace, &mut plan, &affected).await;
    }

    state.consistency.register_due(&namespace);
    enqueue_write_udfs(Arc::clone(&state), &namespace, &plan).await;
    observe_write_metric(
        &state,
        &namespace,
        STATUS_OK,
        total_start.elapsed().as_secs_f64(),
        tpuf_seconds,
        metric_batch_size,
    );

    passthrough_response(upstream)
}

fn object_schema_attributes(body: &Value) -> Vec<String> {
    let Some(schema) = body.get("schema").and_then(Value::as_object) else {
        return Vec::new();
    };

    let mut attributes = schema
        .iter()
        .filter_map(|(attribute, config)| {
            let attribute_type = config
                .as_str()
                .or_else(|| config.get("type").and_then(Value::as_str));
            (attribute_type == Some("object")).then(|| attribute.clone())
        })
        .collect::<Vec<_>>();
    attributes.sort_unstable();
    attributes
}

fn upstream_rejected_object_schema(upstream: &TurbopufferPassthroughResponse) -> bool {
    matches!(upstream.status, 400 | 422)
        && String::from_utf8_lossy(&upstream.body).contains("AttributeSchemaInput")
}

fn unsupported_object_schema_error(store: &str, attributes: &[String]) -> AppError {
    AppError::unsupported_by_store(
        format!(
            "schema type `object` is not supported by the {store} store for attributes: {}; use a supported scalar, array, vector, or sparse-vector type",
            attributes.join(", ")
        ),
        Some(store.to_string()),
        Some("writeNamespace".to_string()),
    )
}

/// POST /v2/namespaces/{namespace}/import
///
/// Binary bulk import for backends that expose a native Arrow IPC stream path.
/// Search maps this to `POST /ns/{namespace}/import`; other backends return
/// `UnsupportedByStore`.
pub async fn import_arrow(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, AppError> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/octet-stream");
    let upstream = state
        .turbopuffer()
        .import_arrow(&namespace, content_type, body.to_vec())
        .await
        .map_err(|e| {
            if is_unsupported_by_store(&e) {
                AppError::from_store_support_error(
                    e,
                    Some("search".to_string()),
                    Some("importNamespace".to_string()),
                )
            } else {
                AppError::Upstream(e.to_string())
            }
        })?;

    let status = StatusCode::from_u16(upstream.status).unwrap_or(StatusCode::BAD_GATEWAY);
    let mut response = Response::builder().status(status);
    if let Some(content_type) = upstream.content_type {
        response = response.header(header::CONTENT_TYPE, content_type);
    }
    response
        .body(Body::from(upstream.body))
        .map_err(|e| AppError::Upstream(e.to_string()))
}

async fn portable_write(
    state: &Arc<AppState>,
    namespace: &str,
    plan: &NativeWritePlan,
) -> Result<TurbopufferPassthroughResponse, crate::clients::turbopuffer::TurbopufferError> {
    if !plan.upserts.is_empty() {
        let docs = plan
            .upserts
            .iter()
            .map(|doc| UpsertDoc {
                id: doc.id.clone(),
                vector: doc.vector.clone(),
                vectors: doc.vectors.clone(),
                attributes: portable_attributes(&doc.attributes),
            })
            .collect::<Vec<_>>();
        state.turbopuffer().upsert(namespace, &docs).await?;
    }
    if !plan.patches.is_empty() {
        let docs = plan
            .patches
            .iter()
            .map(|doc| PatchDoc {
                id: doc.id.clone(),
                attributes: portable_attributes(&doc.attributes),
            })
            .collect::<Vec<_>>();
        state.turbopuffer().patch(namespace, &docs).await?;
    }
    if !plan.deletes.is_empty() {
        state.turbopuffer().delete(namespace, &plan.deletes).await?;
    }
    if let Some(filter) = plan.filter_delete.as_ref() {
        state
            .turbopuffer()
            .delete_by_filter(namespace, filter)
            .await?;
        if let Err(e) = state.aerospike.delete_set(namespace).await {
            warn!(namespace = %namespace, error = %e, "Aerospike namespace cache purge failed after filter delete (best-effort)");
        }
    }
    let body = StatusResponse::write_result(
        plan.upserts.len() as u64,
        plan.patches.len() as u64,
        plan.deletes.len() as u64,
    );
    Ok(TurbopufferPassthroughResponse {
        status: 200,
        content_type: Some("application/json".to_string()),
        body: serde_json::to_vec(&body).unwrap_or_else(|_| br#"{"status":"OK"}"#.to_vec()),
    })
}

fn portable_attributes(attributes: &HashMap<String, Value>) -> HashMap<String, Value> {
    let mut attributes = attributes.clone();
    attributes.remove(SHARD_ATTR);
    attributes
}

fn is_unsupported_by_store(error: &crate::clients::turbopuffer::TurbopufferError) -> bool {
    AppError::is_store_support_error(error)
}

fn native_write_plan(
    body: &mut Value,
    shard_count: u64,
    allow_chunk_attributes: bool,
) -> Result<NativeWritePlan, AppError> {
    let Some(obj) = body.as_object_mut() else {
        return Ok(NativeWritePlan::default());
    };

    reject_removed_dialect(obj)?;
    if obj.is_empty() {
        return Err(AppError::Validation(
            "Request must include a native Turbopuffer write operation".to_string(),
        ));
    }

    let stamp_ms = now_ms();
    let stamp_value = Value::from(stamp_ms);
    let mut plan = NativeWritePlan {
        has_condition: has_write_condition(obj),
        ..NativeWritePlan::default()
    };

    if let Some(rows) = obj.get_mut("upsert_rows") {
        enrich_upsert_rows(
            rows,
            &stamp_value,
            shard_count,
            allow_chunk_attributes,
            &mut plan,
        )?;
    }
    if let Some(columns) = obj.get_mut("upsert_columns") {
        enrich_upsert_columns(columns, &stamp_value, shard_count, &mut plan)?;
    }
    if let Some(rows) = obj.get_mut("patch_rows") {
        enrich_patch_rows(rows, &stamp_value, shard_count, &mut plan)?;
    }
    if let Some(columns) = obj.get_mut("patch_columns") {
        enrich_patch_columns(columns, &stamp_value, shard_count, &mut plan)?;
    }
    if let Some(deletes) = obj.get("deletes") {
        plan.deletes = ids_from_value_array(deletes, "deletes")?;
    }
    if let Some(filter) = obj.get("delete_by_filter") {
        plan.has_filter_delete = true;
        plan.filter_delete = Some(filter.clone());
    }
    if let Some(patch_by_filter) = obj.get_mut("patch_by_filter") {
        plan.has_filter_patch = true;
        plan.filter_patch_attrs = enrich_patch_by_filter(patch_by_filter, &stamp_value)?;
    }

    Ok(plan)
}

fn reject_removed_dialect(obj: &Map<String, Value>) -> Result<(), AppError> {
    if obj.contains_key("upserts") || obj.contains_key("patches") {
        return Err(AppError::Validation(
            "custom write bodies were removed; use native Turbopuffer write keys such as upsert_rows, patch_rows, patch_columns, and deletes".to_string(),
        ));
    }
    Ok(())
}

fn has_write_condition(obj: &Map<String, Value>) -> bool {
    obj.contains_key("upsert_condition")
        || obj.contains_key("patch_condition")
        || obj.contains_key("delete_condition")
}

fn enrich_upsert_rows(
    rows: &mut Value,
    stamp_value: &Value,
    shard_count: u64,
    allow_chunk_attributes: bool,
    plan: &mut NativeWritePlan,
) -> Result<(), AppError> {
    let rows = rows.as_array_mut().ok_or_else(|| {
        AppError::Validation("upsert_rows must be an array of row objects".to_string())
    })?;
    for row_value in rows {
        let row = row_value.as_object_mut().ok_or_else(|| {
            AppError::Validation("upsert_rows entries must be objects".to_string())
        })?;
        reject_removed_blob_write(row)?;
        reject_reserved_attribute_names(row.keys().filter(|name| {
            !allow_chunk_attributes
                || !matches!(
                    name.as_str(),
                    "_hevlayer_parent_id" | "_hevlayer_chunk_index"
                )
        }))?;
        let id = row_id(row, "upsert_rows")?;
        stamp_row(row, &id, stamp_value, shard_count);
        let vector = optional_vector(row.get("vector"), "upsert_rows vector")?;
        let vectors = optional_vectors(row.get("vectors"), "upsert_rows vectors")?;
        plan.upserts.push(UpsertEffect {
            id,
            attributes: row_attributes(row),
            vector,
            vectors,
        });
    }
    Ok(())
}

fn enrich_patch_rows(
    rows: &mut Value,
    stamp_value: &Value,
    shard_count: u64,
    plan: &mut NativeWritePlan,
) -> Result<(), AppError> {
    let rows = rows.as_array_mut().ok_or_else(|| {
        AppError::Validation("patch_rows must be an array of row objects".to_string())
    })?;
    for row_value in rows {
        let row = row_value.as_object_mut().ok_or_else(|| {
            AppError::Validation("patch_rows entries must be objects".to_string())
        })?;
        reject_vector_patch(row, "patch_rows")?;
        reject_reserved_attribute_names(row.keys())?;
        let id = row_id(row, "patch_rows")?;
        stamp_row(row, &id, stamp_value, shard_count);
        plan.patches.push(PatchEffect {
            id,
            attributes: row_attributes(row),
        });
    }
    Ok(())
}

fn enrich_upsert_columns(
    columns: &mut Value,
    stamp_value: &Value,
    shard_count: u64,
    plan: &mut NativeWritePlan,
) -> Result<(), AppError> {
    let columns = columns.as_object_mut().ok_or_else(|| {
        AppError::Validation("upsert_columns must be an object of column arrays".to_string())
    })?;
    if columns.contains_key("blobs") {
        return Err(removed_blob_write_error());
    }
    reject_reserved_attribute_names(columns.keys())?;
    let ids = column_ids(columns, "upsert_columns")?;
    validate_column_lengths(columns, ids.len(), "upsert_columns")?;
    columns.insert(
        UPSERTED_AT_ATTR.to_string(),
        Value::Array(vec![stamp_value.clone(); ids.len()]),
    );
    columns.insert(
        SHARD_ATTR.to_string(),
        Value::Array(
            ids.iter()
                .map(|id| Value::from(shard_for_id(id, shard_count)))
                .collect(),
        ),
    );

    let snapshot = columns.clone();
    for (idx, id) in ids.into_iter().enumerate() {
        let vector = snapshot
            .get("vector")
            .and_then(Value::as_array)
            .and_then(|values| values.get(idx));
        let vectors = snapshot
            .get("vectors")
            .and_then(Value::as_array)
            .and_then(|values| values.get(idx));
        plan.upserts.push(UpsertEffect {
            id,
            attributes: column_attributes(&snapshot, idx),
            vector: optional_vector(vector, "upsert_columns vector")?,
            vectors: optional_vectors(vectors, "upsert_columns vectors")?,
        });
    }
    Ok(())
}

fn enrich_patch_columns(
    columns: &mut Value,
    stamp_value: &Value,
    shard_count: u64,
    plan: &mut NativeWritePlan,
) -> Result<(), AppError> {
    let columns = columns.as_object_mut().ok_or_else(|| {
        AppError::Validation("patch_columns must be an object of column arrays".to_string())
    })?;
    reject_vector_patch(columns, "patch_columns")?;
    reject_reserved_attribute_names(columns.keys())?;
    let ids = column_ids(columns, "patch_columns")?;
    validate_column_lengths(columns, ids.len(), "patch_columns")?;
    columns.insert(
        UPSERTED_AT_ATTR.to_string(),
        Value::Array(vec![stamp_value.clone(); ids.len()]),
    );
    columns.insert(
        SHARD_ATTR.to_string(),
        Value::Array(
            ids.iter()
                .map(|id| Value::from(shard_for_id(id, shard_count)))
                .collect(),
        ),
    );

    let snapshot = columns.clone();
    for (idx, id) in ids.into_iter().enumerate() {
        plan.patches.push(PatchEffect {
            id,
            attributes: column_attributes(&snapshot, idx),
        });
    }
    Ok(())
}

fn enrich_patch_by_filter(
    patch_by_filter: &mut Value,
    stamp_value: &Value,
) -> Result<Option<HashMap<String, Value>>, AppError> {
    let Some(obj) = patch_by_filter.as_object_mut() else {
        return Ok(None);
    };
    let Some(patch) = obj.get_mut("patch").and_then(Value::as_object_mut) else {
        return Ok(None);
    };
    if patch.contains_key("vector") || patch.contains_key("vectors") {
        return Err(AppError::Validation(
            "patch_by_filter cannot patch vector or vectors values; upsert full rows instead"
                .to_string(),
        ));
    }
    reject_reserved_attribute_names(patch.keys())?;
    patch.insert(UPSERTED_AT_ATTR.to_string(), stamp_value.clone());
    Ok(Some(row_attributes(patch)))
}

fn stamp_row(row: &mut Map<String, Value>, id: &str, stamp_value: &Value, shard_count: u64) {
    row.insert(UPSERTED_AT_ATTR.to_string(), stamp_value.clone());
    row.insert(
        SHARD_ATTR.to_string(),
        Value::from(shard_for_id(id, shard_count)),
    );
}

fn row_id(row: &Map<String, Value>, shape: &str) -> Result<String, AppError> {
    let value = row
        .get("id")
        .ok_or_else(|| AppError::Validation(format!("{shape} entries must include id")))?;
    id_to_string(value, &format!("{shape} id"))
}

fn column_ids(columns: &Map<String, Value>, shape: &str) -> Result<Vec<String>, AppError> {
    let ids = columns
        .get("id")
        .ok_or_else(|| AppError::Validation(format!("{shape} must include an id column")))?;
    let ids = ids
        .as_array()
        .ok_or_else(|| AppError::Validation(format!("{shape} id column must be an array")))?;
    if ids.is_empty() {
        return Err(AppError::Validation(format!(
            "{shape} id column must not be empty"
        )));
    }
    ids.iter()
        .map(|value| id_to_string(value, &format!("{shape} id")))
        .collect()
}

fn ids_from_value_array(value: &Value, field: &str) -> Result<Vec<String>, AppError> {
    let ids = value
        .as_array()
        .ok_or_else(|| AppError::Validation(format!("{field} must be an array")))?;
    ids.iter().map(|value| id_to_string(value, field)).collect()
}

fn id_to_string(value: &Value, field: &str) -> Result<String, AppError> {
    match value {
        Value::String(value) => Ok(value.clone()),
        Value::Number(value) => Ok(value.to_string()),
        _ => Err(AppError::Validation(format!(
            "{field} values must be strings or numbers"
        ))),
    }
}

fn validate_column_lengths(
    columns: &Map<String, Value>,
    expected: usize,
    shape: &str,
) -> Result<(), AppError> {
    for (name, values) in columns {
        let values = values.as_array().ok_or_else(|| {
            AppError::Validation(format!("{shape} column '{name}' must be an array"))
        })?;
        if values.len() != expected {
            return Err(AppError::Validation(format!(
                "{shape} column '{name}' has {} values for {expected} ids",
                values.len()
            )));
        }
    }
    Ok(())
}

fn row_attributes(row: &Map<String, Value>) -> HashMap<String, Value> {
    row.iter()
        .filter(|(name, _)| !is_system_column(name))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

fn column_attributes(columns: &Map<String, Value>, idx: usize) -> HashMap<String, Value> {
    columns
        .iter()
        .filter(|(name, _)| !is_system_column(name))
        .filter_map(|(name, values)| {
            values
                .as_array()
                .and_then(|values| values.get(idx))
                .map(|value| (name.clone(), value.clone()))
        })
        .collect()
}

fn is_system_column(name: &str) -> bool {
    matches!(name, "id" | "vector" | "vectors")
}

fn reject_vector_patch(values: &Map<String, Value>, shape: &str) -> Result<(), AppError> {
    if values.contains_key("vector") || values.contains_key("vectors") {
        return Err(AppError::Validation(format!(
            "{shape} cannot patch vector or vectors values; upsert full rows instead"
        )));
    }
    Ok(())
}

fn vector_from_value(value: &Value) -> Option<Vec<f64>> {
    value
        .as_array()?
        .iter()
        .map(Value::as_f64)
        .collect::<Option<Vec<_>>>()
}

fn vectors_from_value(value: &Value) -> Option<Vec<Vec<f64>>> {
    let vectors = value
        .as_array()?
        .iter()
        .map(vector_from_value)
        .collect::<Option<Vec<_>>>()?;
    (!vectors.is_empty()).then_some(vectors)
}

fn optional_vector(value: Option<&Value>, field: &str) -> Result<Option<Vec<f64>>, AppError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let Some(vector) = vector_from_value(value) else {
        return Err(AppError::Validation(format!(
            "{field} must be an array of numbers"
        )));
    };
    if vector.is_empty() {
        return Err(AppError::Validation(format!("{field} must not be empty")));
    }
    Ok(Some(vector))
}

fn optional_vectors(value: Option<&Value>, field: &str) -> Result<Option<Vec<Vec<f64>>>, AppError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let Some(vectors) = vectors_from_value(value) else {
        return Err(AppError::Validation(format!(
            "{field} must be a non-empty array of non-empty numeric vectors"
        )));
    };
    if vectors.iter().any(Vec::is_empty) {
        return Err(AppError::Validation(format!(
            "{field} must be a non-empty array of non-empty numeric vectors"
        )));
    }
    Ok(Some(vectors))
}

fn reject_reserved_attribute_names<'a>(
    names: impl IntoIterator<Item = &'a String>,
) -> Result<(), AppError> {
    for name in names {
        reject_reserved_attribute_name(name)?;
    }
    Ok(())
}

fn reject_reserved_attribute_name(name: &str) -> Result<(), AppError> {
    if name.starts_with("_hevlayer_") && name != UPSERTED_AT_ATTR && name != SHARD_ATTR {
        return Err(AppError::Validation(format!(
            "attribute '{}' uses the reserved _hevlayer_* prefix",
            name
        )));
    }
    Ok(())
}

fn reject_removed_blob_write(row: &Map<String, Value>) -> Result<(), AppError> {
    if row.contains_key("blobs") {
        return Err(removed_blob_write_error());
    }
    Ok(())
}

fn removed_blob_write_error() -> AppError {
    AppError::Validation(
        "document blob writes are no longer supported; store binary payloads externally and write an ordinary attribute with that location".to_string(),
    )
}

fn force_return_affected_ids(body: &mut Value) {
    if let Some(obj) = body.as_object_mut() {
        obj.insert("return_affected_ids".to_string(), Value::Bool(true));
    }
}

async fn apply_prewrite_cache(state: &Arc<AppState>, namespace: &str, plan: &NativeWritePlan) {
    mirror_upserts(state, namespace, &plan.upserts).await;
    mirror_patches(state, namespace, &plan.patches).await;
    delete_cache_ids(state, namespace, &plan.deletes).await;
}

async fn apply_response_driven_effects(
    state: &Arc<AppState>,
    namespace: &str,
    plan: &mut NativeWritePlan,
    affected: &AffectedIds,
) {
    plan.upserts
        .retain(|effect| affected.upserted.contains(&effect.id));
    plan.patches
        .retain(|effect| affected.patched.contains(&effect.id));
    plan.deletes.retain(|id| affected.deleted.contains(id));

    mirror_upserts(state, namespace, &plan.upserts).await;
    mirror_patches(state, namespace, &plan.patches).await;
    delete_cache_ids(state, namespace, &plan.deletes).await;

    let filter_patched: Vec<String> = affected.patched.iter().cloned().collect();
    if plan.has_filter_patch {
        delete_cache_ids(state, namespace, &filter_patched).await;
        if let Some(attrs) = plan.filter_patch_attrs.clone() {
            plan.patches.extend(filter_patched.into_iter().map(|id| {
                let mut attributes = attrs.clone();
                attributes.insert("id".to_string(), Value::String(id.clone()));
                PatchEffect { id, attributes }
            }));
        }
    }

    if plan.has_filter_delete {
        let filter_deleted: Vec<String> = affected.deleted.iter().cloned().collect();
        delete_cache_ids(state, namespace, &filter_deleted).await;
    }
}

async fn mirror_upserts(state: &Arc<AppState>, namespace: &str, upserts: &[UpsertEffect]) {
    if upserts.is_empty() {
        return;
    }

    let docs: HashMap<String, HashMap<String, Value>> = upserts
        .iter()
        .map(|doc| (doc.id.clone(), doc.attributes.clone()))
        .collect();
    if let Err(e) = state.aerospike.put_many(namespace, &docs).await {
        warn!(namespace = %namespace, error = %e, "Aerospike cache write failed (best-effort)");
    }

    for doc in upserts {
        if let Some(vector) = doc.vector.as_ref() {
            if let Err(e) = state.aerospike.put_vector(namespace, &doc.id, vector).await {
                warn!(
                    namespace = %namespace,
                    id = %doc.id,
                    error = %e,
                    "Aerospike cache vector write failed (best-effort)"
                );
            }
        }
    }
}

async fn mirror_patches(state: &Arc<AppState>, namespace: &str, patches: &[PatchEffect]) {
    if patches.is_empty() {
        return;
    }

    let ids: Vec<String> = patches.iter().map(|patch| patch.id.clone()).collect();
    match state.aerospike.get_many(namespace, &ids, None).await {
        Ok(existing) => {
            let mut merged: HashMap<String, HashMap<String, Value>> = HashMap::new();
            for patch in patches {
                let mut row = existing.get(&patch.id).cloned().unwrap_or_default();
                for (key, value) in &patch.attributes {
                    row.insert(key.clone(), value.clone());
                }
                merged.insert(patch.id.clone(), row);
            }
            if let Err(e) = state.aerospike.put_many(namespace, &merged).await {
                warn!(namespace = %namespace, error = %e, "Aerospike cache patch write failed (best-effort)");
            }
        }
        Err(e) => {
            warn!(namespace = %namespace, error = %e, "Aerospike cache patch read failed (best-effort)");
        }
    }
}

async fn delete_cache_ids(state: &Arc<AppState>, namespace: &str, ids: &[String]) {
    for id in ids {
        if let Err(e) = state.aerospike.delete(namespace, id).await {
            warn!(namespace = %namespace, id = %id, error = %e, "Aerospike cache delete failed (best-effort)");
        }
    }
}

async fn enqueue_write_udfs(state: Arc<AppState>, namespace: &str, plan: &NativeWritePlan) {
    if !plan.upserts.is_empty() {
        let rows: Vec<HashMap<String, Value>> = plan
            .upserts
            .iter()
            .map(|doc| {
                let mut row = doc.attributes.clone();
                row.insert("id".to_string(), Value::String(doc.id.clone()));
                if let Some(vector) = &doc.vector {
                    row.insert(
                        "vector".to_string(),
                        serde_json::to_value(vector).unwrap_or(Value::Null),
                    );
                }
                if let Some(vectors) = &doc.vectors {
                    row.insert(
                        "vectors".to_string(),
                        serde_json::to_value(vectors).unwrap_or(Value::Null),
                    );
                }
                row
            })
            .collect();
        if let Some(trigger) = state.write_trigger.clone() {
            trigger
                .enqueue_write_rows(Arc::clone(&state), namespace, rows, false)
                .await;
        }
    }

    if !plan.patches.is_empty() {
        let rows: Vec<HashMap<String, Value>> = plan
            .patches
            .iter()
            .map(|doc| {
                let mut row = doc.attributes.clone();
                row.insert("id".to_string(), Value::String(doc.id.clone()));
                row
            })
            .collect();
        if let Some(trigger) = state.write_trigger.clone() {
            trigger
                .enqueue_write_rows(state, namespace, rows, true)
                .await;
        }
    }
}

fn affected_ids_from_response(body: &[u8]) -> AffectedIds {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return AffectedIds::default();
    };
    AffectedIds {
        upserted: response_ids(&value, &["upserted_ids", "affected_ids"]),
        patched: response_ids(&value, &["patched_ids", "affected_ids"]),
        deleted: response_ids(&value, &["deleted_ids", "affected_ids"]),
    }
}

fn response_ids(value: &Value, keys: &[&str]) -> HashSet<String> {
    keys.iter()
        .filter_map(|key| value.get(*key).and_then(Value::as_array))
        .flat_map(|ids| ids.iter())
        .filter_map(|id| id_to_string(id, "affected id").ok())
        .collect()
}

fn observe_write_metric(
    state: &AppState,
    namespace: &str,
    status: &'static str,
    total_seconds: f64,
    tpuf_seconds: f64,
    batch_size: usize,
) {
    if batch_size == 0 {
        return;
    }
    state.metrics.observe_upsert(
        DIRECT_PIPELINE_ID,
        namespace,
        status,
        total_seconds,
        tpuf_seconds,
        Some(batch_size),
    );
}

fn passthrough_response(response: TurbopufferPassthroughResponse) -> Result<Response, AppError> {
    let status = StatusCode::from_u16(response.status)
        .map_err(|e| AppError::Upstream(format!("invalid Turbopuffer status: {}", e)))?;
    let mut builder = Response::builder().status(status);
    if let Some(content_type) = response.content_type {
        builder = builder.header(header::CONTENT_TYPE, content_type);
    }
    builder
        .body(Body::from(response.body))
        .map(IntoResponse::into_response)
        .map_err(|e| AppError::Upstream(format!("failed to build passthrough response: {}", e)))
}
