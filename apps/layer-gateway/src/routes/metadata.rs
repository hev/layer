use std::sync::Arc;
use std::time::Instant;

use axum::extract::{Path, State};
use axum::Json;
use serde_json::{json, Value};
use tracing::warn;

use crate::clients::turbopuffer::{IndexStatus, TurbopufferError};
use crate::error::AppError;
use crate::metrics::{DIRECT_PIPELINE_ID, STATUS_OK, STATUS_TPUF_ERROR};
use crate::routes::init::init_layer_status;
use crate::shards::read_namespace_marker;
use crate::snapshots::latest_snapshot_body;
use crate::AppState;

/// GET /v2/namespaces/{namespace}/metadata
///
/// Proxies turbopuffer's `/v2/namespaces/{namespace}/metadata` response
/// (so the shape matches https://turbopuffer.com/docs/metadata —
/// `schema`, `approx_row_count`, `approx_logical_bytes`, `created_at`,
/// `updated_at`, `last_write_at`, `encryption`, `index`, `pinning`) and adds
/// the path namespace as `id` when upstream omits it, plus a `layer`
/// sub-object with the gateway's enhancement fields:
///
///   * `stable_as_of` — epoch-ms cut up to which the upstream index is
///     known caught up (the watermark used by `/query`).
///   * `is_stable` — whether the most recent consistency poll observed
///     `index.status == "up-to-date"`. `Unknown` (no signal yet) reports
///     `false`, matching the watcher's "do not advance" behavior.
///   * `indexed` — whether the latest snapshot's indexed-row count has
///     caught up to the upstream namespace row count. `null` when the
///     namespace has no vector column or the latest snapshot predates row
///     count capture.
///   * `index_lag_rows` — `approx_row_count - snapshot.row_count`, clamped at
///     zero. `null` when `indexed` is `null`.
pub async fn get_namespace_metadata(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
) -> Result<Json<Value>, AppError> {
    let start = Instant::now();
    let result = state.turbopuffer().head_namespace(&namespace).await;
    let meta = match result {
        Ok(meta) => {
            state.metrics.observe_head(
                DIRECT_PIPELINE_ID,
                &namespace,
                STATUS_OK,
                start.elapsed().as_secs_f64(),
            );
            meta
        }
        Err(e) => {
            state.metrics.observe_head(
                DIRECT_PIPELINE_ID,
                &namespace,
                STATUS_TPUF_ERROR,
                start.elapsed().as_secs_f64(),
            );
            // Preserve upstream's 404 so namespace-existence checks see the
            // status Turbopuffer's own metadata endpoint would return.
            return Err(match e {
                TurbopufferError::NotFound(_) => {
                    AppError::NotFound(format!("namespace '{}' not found", namespace))
                }
                e => AppError::Upstream(format!("turbopuffer metadata: {}", e)),
            });
        }
    };

    state
        .consistency
        .register_from_metadata(&namespace, meta.index_status);

    let readiness = index_readiness(&state, &namespace, &meta).await;
    let mut body = match meta.raw {
        Value::Object(map) => map,
        // Defensive: if upstream returned a non-object, wrap it under a
        // `raw` key so we can still attach our layer block.
        other => {
            let mut m = serde_json::Map::new();
            m.insert("raw".into(), other);
            m
        }
    };

    body.entry("id")
        .or_insert_with(|| Value::String(namespace.clone()));

    let marker_shard_count = read_namespace_marker(state.turbopuffer(), &namespace)
        .await
        .map_err(|e| AppError::Upstream(format!("namespace marker read failed: {e}")))?;
    let init = init_layer_status(&state, &namespace, 1, marker_shard_count).await?;
    let layer = json!({
        "stable_as_of": state.consistency.get(&namespace),
        "is_stable": matches!(
            state.consistency.current_status(&namespace),
            IndexStatus::Stable
        ),
        "indexed": readiness.indexed,
        "index_lag_rows": readiness.index_lag_rows,
        "schema_version": init.schema_version,
        "init_state": init.init_state,
        "init_lag_rows": init.init_lag_rows,
        "shard_count": init.shard_count,
        "shard_state": init.shard_state,
        "shard_lag_rows": init.shard_lag_rows,
        "scatter_gather_active": init.scatter_gather_active,
    });
    body.insert("layer".into(), layer);

    Ok(Json(Value::Object(body)))
}

struct IndexReadiness {
    indexed: Option<bool>,
    index_lag_rows: Option<u64>,
}

async fn index_readiness(
    state: &AppState,
    namespace: &str,
    meta: &crate::clients::turbopuffer::NamespaceMeta,
) -> IndexReadiness {
    if !has_vector_column(&meta.raw) {
        return IndexReadiness {
            indexed: None,
            index_lag_rows: None,
        };
    }

    let snapshot_row_count = match latest_snapshot_body(state, namespace).await {
        Ok(Some(snapshot)) => snapshot.row_count,
        Ok(None) => Some(0),
        Err(e) => {
            warn!(
                namespace = %namespace,
                error = %e,
                "failed to load latest snapshot for index readiness"
            );
            None
        }
    };

    let Some(indexed_row_count) = snapshot_row_count else {
        return IndexReadiness {
            indexed: None,
            index_lag_rows: None,
        };
    };

    let lag = meta.approx_row_count.saturating_sub(indexed_row_count);
    IndexReadiness {
        indexed: Some(indexed_row_count >= meta.approx_row_count),
        index_lag_rows: Some(lag),
    }
}

fn has_vector_column(raw: &Value) -> bool {
    raw.get("schema")
        .and_then(|v| v.as_object())
        .is_some_and(|schema| {
            schema
                .iter()
                .any(|(name, field)| is_vector_field(name, field))
        })
}

fn is_vector_field(name: &str, field: &Value) -> bool {
    if name == "vector" {
        return true;
    }
    if field.get("dimensions").and_then(|v| v.as_u64()).is_some()
        || field.get("dims").and_then(|v| v.as_u64()).is_some()
        || field
            .get("ann")
            .and_then(|ann| ann.get("dimensions"))
            .and_then(|v| v.as_u64())
            .is_some()
    {
        return true;
    }
    field
        .get("type")
        .and_then(|v| v.as_str())
        .is_some_and(|ty| ty == "vector")
}
