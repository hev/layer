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
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
    PgPool,
};
use uuid::Uuid;

use crate::{auth::ApiScope, AppState};

mod adapter;
mod store;

use adapter::Authorization;

const QUERY_ROUTE: &str = "/v2/namespaces/{namespace}/work-identity/query";
const WRITE_ROUTE: &str = "/v2/namespaces/{namespace}/work-identity";

#[derive(Clone)]
struct Database(Option<PgPool>);

fn configured_pool() -> Option<Database> {
    let url = std::env::var_os("LAYER_WORK_IDENTITY_DATABASE_URL")?;
    let Some(mut options) = url
        .to_str()
        .and_then(|url| url.parse::<PgConnectOptions>().ok())
    else {
        tracing::error!("Invalid work identity database configuration");
        return Some(Database(None));
    };
    if std::env::var_os("LAYER_WORK_IDENTITY_REQUIRE_TLS").is_some() {
        options = options
            .ssl_mode(PgSslMode::VerifyFull)
            .ssl_root_cert("/etc/ssl/certs/ca-certificates.crt");
    }
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(1))
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET search_path = pg_catalog, pg_temp")
                    .execute(&mut *connection)
                    .await?;
                let compatible: bool = sqlx::query_scalar(
                    "SELECT count(version) = 1 AND min(version) = 1
                     FROM work_identity.schema_migrations",
                )
                .fetch_one(connection)
                .await?;
                if !compatible {
                    return Err(sqlx::Error::Protocol(
                        "Unsupported work identity schema version".into(),
                    ));
                }
                Ok(())
            })
        })
        .connect_lazy_with(options);
    Some(Database(Some(pool)))
}

pub(super) fn configured_router(state: Arc<AppState>) -> Option<Router> {
    let database = configured_pool()?;
    let auth = Authorization::new(state);
    Some(
        Router::new()
            .route(WRITE_ROUTE, post(write))
            .route(QUERY_ROUTE, post(query))
            .layer(DefaultBodyLimit::max(4096))
            .layer(Extension(database))
            .layer(axum::middleware::from_fn_with_state(auth, authorize)),
    )
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
        .filter(|token| token.len() <= 256)
        .and_then(|token| auth.authenticate(token));
    let Some(key) = key else {
        return Failure::Unauthorized.into_response();
    };
    let Ok(Path(namespace)) = namespace else {
        return Failure::NotFound.into_response();
    };
    let Ok(workspace) = namespace
        .strip_prefix("work-")
        .unwrap_or("")
        .parse::<Uuid>()
    else {
        return Failure::NotFound.into_response();
    };
    if namespace != format!("work-{workspace}") || !auth.grants(key, &namespace, scope) {
        return Failure::NotFound.into_response();
    }
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
