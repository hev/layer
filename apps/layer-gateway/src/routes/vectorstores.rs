use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::vector_store::{ResolvedVectorStore, ResolvedVectorStoreKind};
use crate::AppState;

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VectorStoreList {
    pub vectorstores: Vec<VectorStore>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VectorStore {
    pub name: String,
    pub kind: String,
    #[serde(default)]
    pub default: bool,
    pub endpoint: VectorStoreEndpoint,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turbopuffer: Option<TurbopufferMetadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential: Option<VectorStoreCredential>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inbound_auth: Option<VectorStoreInboundAuth>,
    pub status: VectorStoreStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turbopuffer_url: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VectorStoreEndpoint {
    pub url: String,
    pub region: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TurbopufferMetadata {
    pub org_id: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VectorStoreCredential {
    pub secret_ref: SecretKeyRef,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SecretKeyRef {
    pub name: String,
    pub key: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VectorStoreInboundAuth {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VectorStoreStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reachable: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    #[serde(default)]
    pub conditions: Vec<Value>,
}

pub async fn list_vectorstores(State(state): State<Arc<AppState>>) -> Response {
    let mut vectorstores = state
        .resolved_vectorstores
        .values()
        .map(|store| project_resolved_vectorstore(&state, store))
        .collect::<Vec<_>>();
    vectorstores.sort_by(|a, b| b.default.cmp(&a.default).then_with(|| a.name.cmp(&b.name)));
    Json(VectorStoreList { vectorstores }).into_response()
}

pub async fn get_vectorstore(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Response {
    match state.resolved_vectorstores.get(&name) {
        Some(store) => Json(project_resolved_vectorstore(&state, store)).into_response(),
        None => error_response(StatusCode::NOT_FOUND, format!("VectorStore {name} not found")),
    }
}

fn project_resolved_vectorstore(state: &AppState, store: &ResolvedVectorStore) -> VectorStore {
    VectorStore {
        name: store.name.clone(),
        kind: match store.kind {
            ResolvedVectorStoreKind::Turbopuffer => "turbopuffer".to_string(),
            ResolvedVectorStoreKind::Search => "search".to_string(),
        },
        default: store.name == state.default_store,
        endpoint: VectorStoreEndpoint {
            url: store.endpoint_url.clone(),
            region: store.endpoint_region.clone().unwrap_or_default(),
        },
        turbopuffer: None,
        credential: None,
        inbound_auth: if store.name == state.default_store {
            Some(VectorStoreInboundAuth {
                mode: Some("deriveFromStore".to_string()),
            })
        } else {
            None
        },
        status: VectorStoreStatus::default(),
        turbopuffer_url: None,
    }
}

fn error_response(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({ "error": message.into() }))).into_response()
}
