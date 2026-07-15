use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::Json;
use dashmap::mapref::entry::Entry;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::clients::turbopuffer::PatchColumns;
use crate::error::AppError;
use crate::shards::{
    read_namespace_marker, shard_drain_filter, shard_for_id, write_namespace_marker, SHARD_ATTR,
};
use crate::AppState;

#[derive(Debug, Clone, Deserialize)]
pub struct InitNamespaceRequest {
    #[serde(default = "default_schema_version")]
    pub schema_version: u64,
    pub shard_count: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct InitNamespaceResponse {
    pub namespace: String,
    pub layer: InitLayerStatus,
}

#[derive(Debug, Clone, Serialize)]
pub struct InitLayerStatus {
    pub schema_version: u64,
    pub init_state: String,
    pub init_lag_rows: u64,
    pub shard_count: Option<u64>,
    pub shard_state: String,
    pub shard_lag_rows: u64,
    pub scatter_gather_active: bool,
}

fn default_schema_version() -> u64 {
    1
}

/// POST /v2/namespaces/{namespace}/init
pub async fn init_namespace(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
    request: Option<Json<InitNamespaceRequest>>,
) -> Result<Json<InitNamespaceResponse>, AppError> {
    let request = request
        .map(|Json(request)| request)
        .unwrap_or(InitNamespaceRequest {
            schema_version: default_schema_version(),
            shard_count: None,
        });
    if request.schema_version == 0 {
        return Err(AppError::Validation(
            "schema_version must be > 0".to_string(),
        ));
    }
    if state.namespace_uses_search_store(&namespace) {
        return Err(AppError::unsupported_by_store(
            "UnsupportedByStore: namespace init shard backfill requires Turbopuffer",
            Some("search".to_string()),
            Some("initNamespace".to_string()),
        ));
    }

    state
        .turbopuffer()
        .head_namespace(&namespace)
        .await
        .map_err(|e| match e {
            e if e.is_not_found() => {
                AppError::NotFound(format!("namespace '{}' not found", namespace))
            }
            e => AppError::from_turbopuffer(e, "turbopuffer metadata"),
        })?;

    let existing = read_namespace_marker(state.turbopuffer(), &namespace)
        .await
        .map_err(|e| AppError::Upstream(format!("namespace marker read failed: {e}")))?;
    let shard_count = match (existing, request.shard_count) {
        (Some(existing), Some(requested)) if existing != requested => {
            return Err(AppError::Conflict(format!(
                "namespace '{}' already initialized with shard_count={existing}; requested shard_count={requested}",
                namespace
            )));
        }
        (Some(existing), _) => Some(existing),
        (None, requested) => requested,
    };

    if let Some(count) = shard_count {
        if count == 0 {
            return Err(AppError::Validation("shard_count must be > 0".to_string()));
        }
        if existing.is_none() {
            write_namespace_marker(
                state.turbopuffer(),
                &namespace,
                count,
                request.schema_version,
            )
            .await
            .map_err(|e| AppError::Upstream(format!("namespace marker write failed: {e}")))?;
        }
        spawn_backfill_if_needed(Arc::clone(&state), namespace.clone(), count);
    }

    let status = init_layer_status(&state, &namespace, request.schema_version, shard_count).await?;
    Ok(Json(InitNamespaceResponse {
        namespace,
        layer: status,
    }))
}

pub async fn init_layer_status(
    state: &AppState,
    namespace: &str,
    schema_version: u64,
    shard_count: Option<u64>,
) -> Result<InitLayerStatus, AppError> {
    let shard_lag_rows = if shard_count.is_some() {
        count_shard_lag_rows(state, namespace).await?
    } else {
        0
    };
    let scatter_gather_active = shard_count.is_some() && shard_lag_rows == 0;
    if scatter_gather_active {
        if let Some(count) = shard_count {
            state
                .sharded_namespaces
                .insert(namespace.to_string(), count);
        }
    }
    Ok(InitLayerStatus {
        schema_version,
        init_state: if shard_lag_rows == 0 {
            "ready".to_string()
        } else {
            "running".to_string()
        },
        init_lag_rows: shard_lag_rows,
        shard_count,
        shard_state: match shard_count {
            None => "unsharded".to_string(),
            Some(_) if shard_lag_rows == 0 => "ready".to_string(),
            Some(_) => "backfilling".to_string(),
        },
        shard_lag_rows,
        scatter_gather_active,
    })
}

pub async fn count_shard_lag_rows(state: &AppState, namespace: &str) -> Result<u64, AppError> {
    let mut cursor = None;
    let mut total = 0_u64;
    let filter = shard_drain_filter();
    loop {
        let page = state
            .turbopuffer()
            .scan_page(
                namespace,
                cursor.as_deref(),
                state.init_backfill_batch_size,
                Some(&filter),
                None,
            )
            .await
            .map_err(|e| AppError::Upstream(format!("Turbopuffer shard lag scan failed: {e}")))?;
        total += page.documents.len() as u64;
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    Ok(total)
}

fn spawn_backfill_if_needed(state: Arc<AppState>, namespace: String, shard_count: u64) {
    let guard = match state.init_tasks.entry(namespace.clone()) {
        Entry::Occupied(_) => return,
        Entry::Vacant(entry) => entry.insert(Arc::new(Mutex::new(()))).clone(),
    };
    tokio::spawn(async move {
        let _lock = guard.lock().await;
        if let Err(e) = run_backfill(Arc::clone(&state), &namespace, shard_count).await {
            warn!(namespace = %namespace, error = %e, "namespace init backfill failed");
        }
        state.init_tasks.remove(&namespace);
    });
}

async fn run_backfill(
    state: Arc<AppState>,
    namespace: &str,
    shard_count: u64,
) -> Result<(), AppError> {
    let filter = shard_drain_filter();
    let pause = if state.init_backfill_rps == 0 {
        None
    } else {
        Some(Duration::from_secs_f64(
            1.0 / state.init_backfill_rps as f64,
        ))
    };

    loop {
        let page = state
            .turbopuffer()
            .scan_page(
                namespace,
                None,
                state.init_backfill_batch_size,
                Some(&filter),
                None,
            )
            .await
            .map_err(|e| AppError::Upstream(format!("Turbopuffer init scan failed: {e}")))?;
        if page.documents.is_empty() {
            state
                .sharded_namespaces
                .insert(namespace.to_string(), shard_count);
            info!(namespace = %namespace, shard_count, "namespace init backfill drained");
            return Ok(());
        }

        let ids: Vec<String> = page.documents.iter().map(|doc| doc.id.clone()).collect();
        let shards: Vec<Value> = page
            .documents
            .iter()
            .map(|doc| Value::from(shard_for_id(&doc.id, shard_count)))
            .collect();
        state
            .turbopuffer()
            .patch_columns(
                namespace,
                &PatchColumns {
                    ids,
                    columns: [(SHARD_ATTR.to_string(), shards)].into_iter().collect(),
                },
            )
            .await
            .map_err(|e| AppError::Upstream(format!("Turbopuffer init patch failed: {e}")))?;

        if let Some(pause) = pause {
            tokio::time::sleep(pause).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::shards::NAMESPACE_META_ID;

    #[test]
    fn shard_drain_filter_excludes_marker_and_missing_shard() {
        assert_eq!(
            shard_drain_filter(),
            json!([
                "And",
                [
                    ["id", "NotEq", NAMESPACE_META_ID],
                    [SHARD_ATTR, "NotExists", true]
                ]
            ])
        );
    }
}
