use std::sync::Arc;

use axum::extract::State;
use axum::http::{header, Method, Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
pub use vectorstore_core::auth::{ApiScope, InboundAuth, InboundKey};

use crate::AppState;

const BEARER_PREFIX: &str = "Bearer ";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthenticatedApiKey {
    pub name: String,
    pub scopes: Vec<ApiScope>,
}

impl AuthenticatedApiKey {
    pub fn has_scope(&self, required: ApiScope) -> bool {
        self.scopes.contains(&ApiScope::Admin) || self.scopes.contains(&required)
    }
}

#[allow(dead_code)]
pub trait MintedKeyVerifier: Send + Sync {}

pub async fn require_api_key(
    State(state): State<Arc<AppState>>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let required_scope = required_scope(request.method(), request.uri().path());
    if state.inbound_auth.is_open() {
        return next.run(request).await;
    }

    let Some(provided) = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix(BEARER_PREFIX))
        .map(str::to_string)
    else {
        return unauthorized();
    };

    if matches!(state.inbound_auth, InboundAuth::DeriveFromRequest) {
        let authenticated = AuthenticatedApiKey {
            name: "deriveFromStore".to_string(),
            scopes: vec![ApiScope::Admin, ApiScope::Read, ApiScope::Write],
        };
        request.extensions_mut().insert(authenticated);
        return vectorstore_core::turbopuffer::scope_upstream_api_key(provided, next.run(request))
            .await;
    }

    let InboundAuth::Keys(keys) = &state.inbound_auth else {
        return next.run(request).await;
    };

    for key in keys {
        if constant_time_eq(provided.as_bytes(), key.token.as_bytes()) {
            let authenticated = AuthenticatedApiKey {
                name: key.name.clone(),
                scopes: key.scopes.clone(),
            };
            if !authenticated.has_scope(required_scope) {
                return insufficient_scope(required_scope);
            }
            request.extensions_mut().insert(authenticated);
            return next.run(request).await;
        }
    }

    forbidden()
}

fn insufficient_scope(required: ApiScope) -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(json!({"error": "insufficient API key scope", "required_scope": required.as_str()})),
    )
        .into_response()
}

fn required_scope(method: &Method, path: &str) -> ApiScope {
    if is_admin_route(method, path) {
        return ApiScope::Admin;
    }
    if is_read_route(method, path) {
        ApiScope::Read
    } else {
        ApiScope::Write
    }
}

fn is_admin_route(method: &Method, path: &str) -> bool {
    if path_has_prefix_segments(path, &["v2", "keys"]) {
        return true;
    }
    if method == Method::POST && path == "/v2/pipelines" {
        return true;
    }
    if method == Method::POST && path == "/v2/udfs" {
        return true;
    }
    if method == Method::DELETE
        && (path_has_prefix_segments(path, &["v2", "pipelines"])
            || path_has_prefix_segments(path, &["v2", "udfs"]))
    {
        return true;
    }
    if method == Method::POST && path_has_prefix_segments(path, &["v2", "udfs"]) {
        return path.ends_with("/pause")
            || path.ends_with("/resume")
            || path.ends_with("/reset-failed")
            || path.ends_with("/discover");
    }
    false
}

fn is_read_route(method: &Method, path: &str) -> bool {
    if method == Method::GET {
        return true;
    }
    if method == Method::POST {
        return path.ends_with("/query")
            || path.ends_with("/multi_query")
            || path.ends_with("/explain_query")
            || path.ends_with("/scans")
            || path.contains("/scans/");
    }
    false
}

fn path_has_prefix_segments(path: &str, segments: &[&str]) -> bool {
    let mut actual = path.trim_start_matches('/').split('/');
    for expected in segments {
        if actual.next() != Some(*expected) {
            return false;
        }
    }
    true
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({"error": "missing or malformed Authorization: Bearer header"})),
    )
        .into_response()
}

fn forbidden() -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(json!({"error": "invalid API key"})),
    )
        .into_response()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}
