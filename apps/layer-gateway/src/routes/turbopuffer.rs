use std::sync::Arc;

use axum::body::Body;
use axum::extract::{OriginalUri, Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::Value;

use crate::error::AppError;
use crate::AppState;

pub async fn passthrough_get(
    State(state): State<Arc<AppState>>,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, AppError> {
    passthrough(state, "GET", uri.path(), uri.query(), None).await
}

pub async fn passthrough_post(
    State(state): State<Arc<AppState>>,
    OriginalUri(uri): OriginalUri,
    Json(body): Json<Value>,
) -> Result<Response, AppError> {
    passthrough(state, "POST", uri.path(), uri.query(), Some(body)).await
}

pub async fn passthrough_query_post(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
    OriginalUri(uri): OriginalUri,
    Json(body): Json<Value>,
) -> Result<Response, AppError> {
    crate::routes::embed_wire::prepare_query(&body, state.namespace_uses_search_store(&namespace))?;
    let upstream_path = uri.path().replacen("/v1/namespaces/", "/v2/namespaces/", 1);
    passthrough(state, "POST", &upstream_path, uri.query(), Some(body)).await
}

pub async fn passthrough_patch(
    State(state): State<Arc<AppState>>,
    OriginalUri(uri): OriginalUri,
    Json(body): Json<Value>,
) -> Result<Response, AppError> {
    passthrough(state, "PATCH", uri.path(), uri.query(), Some(body)).await
}

pub async fn passthrough_delete(
    State(state): State<Arc<AppState>>,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, AppError> {
    passthrough(state, "DELETE", uri.path(), uri.query(), None).await
}

pub async fn passthrough(
    state: Arc<AppState>,
    method: &str,
    path: &str,
    query: Option<&str>,
    body: Option<Value>,
) -> Result<Response, AppError> {
    let response = state
        .turbopuffer()
        .passthrough(method, path, query, body)
        .await
        .map_err(|e| AppError::Upstream(format!("Turbopuffer passthrough failed: {}", e)))?;

    let status = StatusCode::from_u16(response.status)
        .map_err(|e| AppError::Upstream(format!("invalid Turbopuffer status: {}", e)))?;
    let mut builder = Response::builder().status(status);
    if let Some(content_type) = response.content_type {
        builder = builder.header(header::CONTENT_TYPE, content_type);
    }
    builder
        .body(Body::from(response.body))
        .map(|response| response.into_response())
        .map_err(|e| AppError::Upstream(format!("failed to build passthrough response: {}", e)))
}
