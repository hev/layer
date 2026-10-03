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
    pub namespaces: Vec<String>,
}

impl AuthenticatedApiKey {
    pub fn has_scope(&self, required: ApiScope) -> bool {
        self.scopes.contains(&ApiScope::Admin) || self.scopes.contains(&required)
    }

    pub fn allows_namespace(&self, _namespace: &str) -> bool {
        true
    }
}

#[allow(dead_code)]
pub trait MintedKeyVerifier: Send + Sync {}

/// What the caller's key was granted, kept on the request so a handler can
/// authorize a second namespace named in the body (RFC 0124: the source of
/// a branch or copy). Absent in open and `deriveFromStore` modes.
#[derive(Clone)]
pub enum CallerGrant {
    /// A declared or environment key: scopes, every namespace.
    Declared(Vec<ApiScope>),
}

pub fn can_list_store(_state: &AppState, grant: Option<&CallerGrant>) -> bool {
    match grant {
        None => true,
        Some(CallerGrant::Declared(scopes)) => scopes.contains(&ApiScope::Admin) || scopes.contains(&ApiScope::Read),
    }
}

pub fn can_list_namespace(state: &AppState, grant: Option<&CallerGrant>, namespace: &str) -> bool {
    authorize_namespace(state, grant, ApiScope::Read, namespace).is_ok()
}

/// Require `scope` on `namespace` for the caller. `None` (open or
/// `deriveFromStore` mode) allows.
pub fn authorize_namespace(
    _state: &AppState,
    grant: Option<&CallerGrant>,
    scope: ApiScope,
    namespace: &str,
) -> Result<(), crate::error::AppError> {
    match grant {
        None => Ok(()),
        Some(CallerGrant::Declared(scopes)) => {
            if scopes.contains(&ApiScope::Admin) || scopes.contains(&scope) {
                Ok(())
            } else {
                Err(crate::error::AppError::Forbidden(format!(
                    "the key needs {} scope on namespace `{namespace}`",
                    scope.as_str()
                )))
            }
        }
    }
}

pub async fn require_api_key(
    State(state): State<Arc<AppState>>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let required_scope = required_scope(request.method(), request.uri().path());
    if state.inbound_auth.is_open() {
        return run_with_billing_caller(request, next).await;
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
            namespaces: Vec::new(),
        };
        request.extensions_mut().insert(authenticated);
        return vectorstore_core::turbopuffer::scope_upstream_api_key(provided, run_with_billing_caller(request, next))
            .await;
    }

    let InboundAuth::Keys(keys) = &state.inbound_auth else {
        return run_with_billing_caller(request, next).await;
    };

    for key in keys {
        if constant_time_eq(provided.as_bytes(), key.token.as_bytes()) {
            let authenticated = AuthenticatedApiKey {
                name: key.name.clone(),
                scopes: key.scopes.clone(),
                namespaces: Vec::new(),
            };
            if !authenticated.has_scope(required_scope) {
                return insufficient_scope(required_scope);
            }
            request
                .extensions_mut()
                .insert(CallerGrant::Declared(key.scopes.clone()));
            request.extensions_mut().insert(authenticated);
            return run_with_billing_caller(request, next).await;
        }
    }

    forbidden()
}

async fn run_with_billing_caller(request: Request<axum::body::Body>, next: Next) -> Response {
    let name = request.extensions().get::<AuthenticatedApiKey>().map(|key| key.name.clone());
    if let Some(name) = name {
        crate::metrics::scope_billing_caller(crate::metrics::BillingCaller::api_key(&name), next.run(request)).await
    } else { next.run(request).await }
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
            || path.contains("/scans/")
            || is_namespace_search_route(path);
    }
    false
}

/// `POST /v2/namespaces/{namespace}/search` exactly, so a write to a
/// namespace named `search` stays a write.
fn is_namespace_search_route(path: &str) -> bool {
    let segments: Vec<&str> = path.trim_matches('/').split('/').collect();
    matches!(segments.as_slice(), ["v2", "namespaces", _, "search"])
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
