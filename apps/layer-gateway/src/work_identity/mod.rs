use std::{sync::Arc, time::Duration};

use axum::{
    extract::{DefaultBodyLimit, Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Extension, Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, PgPool};
use uuid::Uuid;

use crate::{
    auth::{constant_time_eq, ApiScope},
    AppState,
};

mod store;

const QUERY_ROUTE: &str = "/v2/namespaces/{namespace}/work-identity/query";
const WRITE_ROUTE: &str = "/v2/namespaces/{namespace}/work-identity";

#[derive(Clone)]
pub(super) struct Database(Option<PgPool>);

fn configured_pool() -> Option<Database> {
    let url = std::env::var_os("LAYER_WORK_IDENTITY_DATABASE_URL")?;
    let Some(url) = url.to_str() else {
        return Some(Database(None));
    };
    match PgPoolOptions::new()
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(1))
        .connect_lazy(url)
    {
        Ok(pool) => Some(Database(Some(pool))),
        Err(_) => {
            tracing::error!("Invalid work identity database configuration");
            Some(Database(None))
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalKey {
    token: String,
    store: String,
    namespaces: Vec<String>,
    scopes: Vec<String>,
}

#[derive(Clone)]
struct Authorization {
    state: Arc<AppState>,
    keys: Arc<[LocalKey]>,
}

pub fn configured_router(state: Arc<AppState>) -> Option<Router> {
    let database = configured_pool()?;
    let keys: Vec<LocalKey> = match std::env::var_os("LAYER_WORK_IDENTITY_KEYS_FILE") {
        None => Vec::new(),
        Some(path) => std::fs::read(path)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .expect("failed to read LAYER_WORK_IDENTITY_KEYS_FILE as work identity key grants"),
    };
    assert!(
        keys.iter().enumerate().all(|(index, key)| {
            is_local_token(&key.token)
                && keys[..index]
                    .iter()
                    .all(|previous| previous.token != key.token)
        }),
        "invalid or duplicate local work identity bearer token"
    );
    Some(router(
        Authorization {
            state,
            keys: keys.into(),
        },
        database,
    ))
}

fn is_local_token(token: &str) -> bool {
    token.len() <= 256
        && token.strip_prefix("hvl_").is_some_and(|value| {
            !value.is_empty()
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        })
}

fn router(auth: Authorization, database: Database) -> Router {
    Router::new()
        .route(WRITE_ROUTE, post(write))
        .route(QUERY_ROUTE, post(query))
        .layer(DefaultBodyLimit::max(4096))
        .layer(Extension(database))
        .layer(axum::middleware::from_fn_with_state(auth, authorize))
}

// Resolve grants against the immutable namespace before processing a body
// or opening a transaction. Broad gateway and wildcard grants are insufficient.
fn workspace_grant(
    state: &AppState,
    key: &LocalKey,
    namespace: &str,
    scope: ApiScope,
) -> Option<Uuid> {
    let workspace = namespace.strip_prefix("work-")?.parse::<Uuid>().ok()?;
    if namespace != format!("work-{workspace}") {
        return None;
    }
    (key.store == state.store_for_namespace(namespace)
        && key.namespaces.iter().any(|name| name == namespace)
        && key.scopes.iter().any(|s| s == scope.as_str()))
    .then_some(workspace)
}

async fn authorize(
    State(auth): State<Authorization>,
    namespace: Result<Path<String>, axum::extract::rejection::PathRejection>,
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let scope = if request.uri().path().ends_with("/query") {
        ApiScope::Read
    } else {
        ApiScope::Write
    };
    let key = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|token| is_local_token(token))
        .and_then(|token| {
            auth.keys
                .iter()
                .find(|key| constant_time_eq(token.as_bytes(), key.token.as_bytes()))
        });
    let Some(key) = key else {
        return Failure::Unauthorized.into_response();
    };
    let Ok(Path(namespace)) = namespace else {
        return Failure::NotFound.into_response();
    };
    let Some(workspace) = workspace_grant(&auth.state, key, &namespace, scope) else {
        return Failure::NotFound.into_response();
    };
    request.extensions_mut().insert(workspace);
    next.run(request).await
}

#[derive(Debug)]
enum Failure {
    Invalid,
    Unauthorized,
    NotFound,
    Conflict,
    Capacity,
    Unavailable,
}

impl From<sqlx::Error> for Failure {
    fn from(_: sqlx::Error) -> Self {
        Self::Unavailable
    }
}

impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        let (status, error) = match self {
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized"),
            Self::Invalid => (StatusCode::BAD_REQUEST, "invalid_request"),
            Self::NotFound => (StatusCode::NOT_FOUND, "not_found"),
            Self::Conflict => (StatusCode::CONFLICT, "conflict"),
            Self::Capacity => (StatusCode::CONFLICT, "capacity"),
            Self::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "unavailable"),
        };
        (
            status,
            Json(json!({"error": error, "retryable": status == StatusCode::SERVICE_UNAVAILABLE})),
        )
            .into_response()
    }
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Query {
    Resolve {
        address: String,
    },
    Get {
        id: Uuid,
    },
    List {
        after: Option<i64>,
        limit: Option<u32>,
    },
    History {
        after: Option<i64>,
        limit: Option<u32>,
    },
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Write {
    Create {
        idempotency_key: Uuid,
        parent_id: Option<Uuid>,
        request_ref: Uuid,
        scope_ref: Uuid,
    },
    Move {
        id: Uuid,
        expected_revision: i64,
        parent_id: Option<Uuid>,
        segment: i32,
    },
    Revise {
        id: Uuid,
        expected_revision: i64,
        scope_ref: Uuid,
    },
    Link {
        id: Uuid,
        expected_revision: i64,
        kind: LinkKind,
        reference: Uuid,
    },
    Accept {
        id: Uuid,
        expected_revision: i64,
        scope_revision: i64,
        evidence_ref: Uuid,
    },
}

#[derive(Clone, Copy, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum LinkKind {
    Attempt,
    Pr,
    Execution,
    Artifact,
}

type Input<T> = Result<Json<T>, axum::extract::rejection::JsonRejection>;

async fn query(
    Extension(Database(pool)): Extension<Database>,
    Extension(workspace): Extension<Uuid>,
    input: Input<Query>,
) -> Result<Json<Value>, Failure> {
    let Json(query) = input.map_err(|_| Failure::Invalid)?;
    bounded(store::query(
        pool.as_ref().ok_or(Failure::Unavailable)?,
        workspace,
        query,
    ))
    .await
}

async fn write(
    Extension(Database(pool)): Extension<Database>,
    Extension(workspace): Extension<Uuid>,
    input: Input<Write>,
) -> Result<Json<Value>, Failure> {
    let Json(write) = input.map_err(|_| Failure::Invalid)?;
    bounded(store::write(
        pool.as_ref().ok_or(Failure::Unavailable)?,
        workspace,
        write,
    ))
    .await
}

async fn bounded(
    call: impl std::future::Future<Output = Result<Value, Failure>>,
) -> Result<Json<Value>, Failure> {
    tokio::time::timeout(Duration::from_secs(5), call)
        .await
        .map_err(|_| Failure::Unavailable)?
        .map(Json)
}
