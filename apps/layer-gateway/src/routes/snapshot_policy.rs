use std::sync::Arc;

use axum::extract::{Path, State};
use axum::Json;

use crate::error::AppError;
use crate::snapshot_policy::{get_snapshot_policy, put_snapshot_policy, SnapshotPolicy};
use crate::AppState;

/// GET /v2/namespaces/{namespace}/snapshot-policy
pub async fn get_policy(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
) -> Result<Json<SnapshotPolicy>, AppError> {
    let policy = get_snapshot_policy(&state, &namespace)
        .await?
        .ok_or_else(|| {
            AppError::NotFound(format!("snapshot policy for '{namespace}' not found"))
        })?;
    Ok(Json(policy))
}

/// PUT /v2/namespaces/{namespace}/snapshot-policy
pub async fn put_policy(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
    Json(policy): Json<SnapshotPolicy>,
) -> Result<Json<SnapshotPolicy>, AppError> {
    Ok(Json(put_snapshot_policy(&state, &namespace, policy).await?))
}
