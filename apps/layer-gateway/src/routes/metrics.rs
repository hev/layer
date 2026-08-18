use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{RawQuery, State};
use axum::http::{header, HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use tracing::warn;

use crate::AppState;

#[cfg(feature = "pro")]
const METRICS_REFRESH_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(750);

pub async fn prometheus_metrics(State(state): State<Arc<AppState>>) -> Response {
    #[cfg(feature = "pro")]
    {
        state
            .metrics
            .set_license_state(&state.license, state.license_grace_gateway);
        if let Some(store) = state.pipeline_store.as_ref() {
            match tokio::time::timeout(METRICS_REFRESH_TIMEOUT, store.collect_metrics()).await {
                Ok(Ok(snapshot)) => state.metrics.refresh_pipeline_metrics(snapshot),
                Ok(Err(error)) => warn!(%error, "pipeline metrics refresh failed"),
                Err(_) => warn!("pipeline metrics refresh timed out; serving cached metrics"),
            }
        }
        if let Some(store) = state.udf_store.as_ref() {
            match tokio::time::timeout(METRICS_REFRESH_TIMEOUT, store.collect_metrics()).await {
                Ok(Ok(snapshot)) => state.metrics.refresh_udf_metrics(snapshot),
                Ok(Err(error)) => warn!(%error, "UDF metrics refresh failed"),
                Err(_) => warn!("UDF metrics refresh timed out; serving cached metrics"),
            }
        }
    }

    match state.metrics.encode() {
        Ok(body) => (
            StatusCode::OK,
            [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
            body,
        )
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}

pub async fn prometheus_proxy_query(
    State(state): State<Arc<AppState>>,
    method: Method,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    proxy_prometheus_api(state, "query", method, headers, query, body).await
}

pub async fn prometheus_proxy_query_range(
    State(state): State<Arc<AppState>>,
    method: Method,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    proxy_prometheus_api(state, "query_range", method, headers, query, body).await
}

pub async fn prometheus_proxy_import(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    proxy_prometheus_api(
        state,
        "import/prometheus",
        Method::POST,
        headers,
        None,
        body,
    )
    .await
}

async fn proxy_prometheus_api(
    state: Arc<AppState>,
    endpoint: &str,
    method: Method,
    headers: HeaderMap,
    query: Option<String>,
    body: Bytes,
) -> Response {
    let Some(base_url) = state.metrics_backend_url.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "metrics backend not configured",
        )
            .into_response();
    };
    let suffix = query.map(|q| format!("?{}", q)).unwrap_or_default();
    let url = format!("{}/api/v1/{}{}", base_url, endpoint, suffix);

    let client = reqwest::Client::new();
    let reqwest_method =
        reqwest::Method::from_bytes(method.as_str().as_bytes()).unwrap_or(reqwest::Method::GET);
    let mut request = client.request(reqwest_method, &url);
    if let Some(content_type) = headers.get(header::CONTENT_TYPE) {
        request = request.header(reqwest::header::CONTENT_TYPE, content_type.as_bytes());
    }
    if !body.is_empty() {
        request = request.body(body);
    }

    match request.send().await {
        Ok(resp) => {
            let status =
                StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            match resp.bytes().await {
                Ok(body) => (
                    status,
                    [("content-type", "application/json; charset=utf-8")],
                    body,
                )
                    .into_response(),
                Err(e) => {
                    warn!(error = %e, "failed to read metrics backend response");
                    (
                        StatusCode::BAD_GATEWAY,
                        format!("metrics backend response: {}", e),
                    )
                        .into_response()
                }
            }
        }
        Err(e) => {
            warn!(error = %e, "failed to query metrics backend");
            (
                StatusCode::BAD_GATEWAY,
                format!("metrics backend request: {}", e),
            )
                .into_response()
        }
    }
}
