use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;

use crate::models::{AerospikeHealthResponse, HealthResponse, NamespaceCacheStateResponse};
use crate::AppState;

pub async fn health(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    let aerospike = state.aerospike_runtime.status().await;
    let mut cache_state = Vec::new();
    for namespace in state.cache_namespace_list() {
        let state_name = state.cache_state_for_namespace(&namespace).await.as_str();
        cache_state.push(NamespaceCacheStateResponse {
            warmed_through: state
                .cache_warmed_through
                .get(&namespace)
                .map(|r| *r.value()),
            warm_inflight: state.warm_inflight.contains_key(&namespace),
            namespace,
            state: state_name.to_string(),
        });
    }

    Json(HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        aerospike: AerospikeHealthResponse {
            connected: aerospike.connected,
            generation: aerospike.generation,
            last_error: aerospike.last_error,
        },
        cache_state,
    })
}

pub async fn ready(State(state): State<Arc<AppState>>) -> Response {
    if state.is_draining() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "status": "draining",
                "version": env!("CARGO_PKG_VERSION")
            })),
        )
            .into_response();
    }

    health(State(state)).await.into_response()
}
