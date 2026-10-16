//! Store-owned record history: no cached projection may decide activation.
use crate::{error::AppError, AppState};
use axum::{
    extract::{OriginalUri, Path, State},
    response::Response,
    Json,
};
use serde_json::Value;
use std::sync::Arc;

pub(crate) fn require_store(state: &AppState, namespace: &str) -> Result<(), AppError> {
    let kind = state
        .turbopuffer()
        .capabilities_for_namespace(namespace)
        .kind;
    if kind != "pgvector" {
        return Err(AppError::UnsupportedByStore {
            store: Some(kind.into()),
            route: None,
            feature: Some("record_versioning".into()),
            message: "record_versioning requires pgvector".into(),
        });
    }
    Ok(())
}

pub async fn read(
    State(state): State<Arc<AppState>>,
    Path(params): Path<Vec<String>>,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, AppError> {
    require_store(&state, &params[0])?;
    super::turbopuffer::passthrough(state, "GET", uri.path(), uri.query(), None).await
}

pub async fn activate(
    State(state): State<Arc<AppState>>,
    Path(params): Path<Vec<String>>,
    OriginalUri(uri): OriginalUri,
    Json(body): Json<Value>,
) -> Result<Response, AppError> {
    require_store(&state, &params[0])?;
    super::turbopuffer::passthrough(state, "POST", uri.path(), uri.query(), Some(body)).await
}
