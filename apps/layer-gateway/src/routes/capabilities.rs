//! Runtime store capabilities (RFC 0117, LYR-86).
//!
//! - `GET /v2/namespaces/{ns}/capabilities`: the store the namespace resolves
//!   to (its Index `storeRef`, else the default store).
//! - `GET /v2/vectorstores/{name}/capabilities`: a configured store by name.
//!
//! Both answer from this build's store declarations, without calling the
//! store. They carry the same rows as `store-capabilities.json`, including
//! `branch_from_namespace` and `copy_from_namespace` (RFC 0124).
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::Json;
use serde_json::Value;

use crate::error::AppError;
use crate::AppState;

pub async fn namespace_capabilities(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
) -> Json<Value> {
    let store = state.store_for_namespace(&namespace);
    Json(
        state
            .turbopuffer()
            .capabilities_for_namespace(&namespace)
            .report(&store),
    )
}

pub async fn vectorstore_capabilities(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<Value>, AppError> {
    let capabilities = state
        .turbopuffer()
        .store_capabilities(&name)
        .or_else(|| (name == state.default_store).then(|| state.turbopuffer().capabilities()))
        .ok_or_else(|| AppError::NotFound(format!("VectorStore `{name}` is not configured")))?;
    Ok(Json(capabilities.report(&name)))
}
