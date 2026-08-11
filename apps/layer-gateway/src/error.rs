use axum::body::Body;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("Upstream HTTP response: {status}")]
    UpstreamResponse {
        status: u16,
        content_type: Option<String>,
        body: Vec<u8>,
    },

    #[error("Upstream error: {0}")]
    Upstream(String),

    #[error("Retryable upstream error: {message}")]
    RetryableUpstream {
        status: StatusCode,
        message: String,
        retry_after: Option<String>,
    },

    #[error("Service unavailable: {0}")]
    ServiceUnavailable(String),

    #[error("Cache cold: {0}")]
    CacheCold(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Unsupported by store: {message}")]
    UnsupportedByStore {
        store: Option<String>,
        route: Option<String>,
        message: String,
    },

    #[error("Forbidden: {0}")]
    Forbidden(String),

    #[error("Payload too large: {0}")]
    PayloadTooLarge(String),

    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Precondition failed: {0}")]
    PreconditionFailed(String),

    #[error("Gateway timeout: {0}")]
    GatewayTimeout(String),
}

#[cfg(feature = "pro")]
impl From<layer_agentic::AgenticError> for AppError {
    fn from(error: layer_agentic::AgenticError) -> Self {
        match error {
            layer_agentic::AgenticError::Upstream(message) => Self::Upstream(message),
            layer_agentic::AgenticError::ServiceUnavailable(message) => {
                Self::ServiceUnavailable(message)
            }
            layer_agentic::AgenticError::Validation(message) => Self::Validation(message),
        }
    }
}

#[cfg(not(feature = "pro"))]
impl From<crate::agent::AgenticError> for AppError {
    fn from(error: crate::agent::AgenticError) -> Self {
        match error {
            crate::agent::AgenticError::Upstream(message) => Self::Upstream(message),
            crate::agent::AgenticError::ServiceUnavailable(message) => {
                Self::ServiceUnavailable(message)
            }
            crate::agent::AgenticError::Validation(message) => Self::Validation(message),
        }
    }
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    store: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    route: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_state: Option<&'static str>,
}

impl AppError {
    pub fn from_turbopuffer(
        error: crate::clients::turbopuffer::TurbopufferError,
        context: impl AsRef<str>,
    ) -> Self {
        match error {
            crate::clients::turbopuffer::TurbopufferError::Response(response) => {
                Self::UpstreamResponse {
                    status: response.status,
                    content_type: response.content_type,
                    body: response.body,
                }
            }
            error => Self::Upstream(format!("{}: {error}", context.as_ref())),
        }
    }

    pub fn unsupported_by_store(
        message: impl Into<String>,
        store: Option<String>,
        route: Option<String>,
    ) -> Self {
        Self::UnsupportedByStore {
            store,
            route,
            message: message.into(),
        }
    }

    pub fn from_store_support_error(
        error: impl ToString,
        store: Option<String>,
        route: Option<String>,
    ) -> Self {
        Self::unsupported_by_store(error.to_string(), store, route)
    }

    pub fn is_store_support_error(error: impl ToString) -> bool {
        error.to_string().contains("UnsupportedByStore")
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        if let AppError::UpstreamResponse {
            status,
            content_type,
            body,
        } = &self
        {
            let mut response = Response::new(Body::from(body.clone()));
            *response.status_mut() =
                StatusCode::from_u16(*status).unwrap_or(StatusCode::BAD_GATEWAY);
            if let Some(content_type) = content_type
                .as_deref()
                .and_then(|value| HeaderValue::from_str(value).ok())
            {
                response
                    .headers_mut()
                    .insert(header::CONTENT_TYPE, content_type);
            }
            return response;
        }

        let retry_after = match &self {
            AppError::RetryableUpstream { retry_after, .. } => retry_after.as_deref(),
            _ => None,
        };
        let (status, error_type, message, store, route, cache_state) = match &self {
            AppError::UpstreamResponse { .. } => unreachable!("handled above"),
            AppError::Upstream(msg) => (
                StatusCode::BAD_GATEWAY,
                "upstream_error",
                msg.clone(),
                None,
                None,
                None,
            ),
            AppError::RetryableUpstream {
                status, message, ..
            } => (
                *status,
                if *status == StatusCode::TOO_MANY_REQUESTS {
                    "upstream_error"
                } else {
                    "service_unavailable"
                },
                message.clone(),
                None,
                None,
                None,
            ),
            AppError::ServiceUnavailable(msg) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "service_unavailable",
                msg.clone(),
                None,
                None,
                None,
            ),
            AppError::CacheCold(msg) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "cache_cold",
                msg.clone(),
                None,
                None,
                Some("cold"),
            ),
            AppError::NotFound(msg) => (
                StatusCode::NOT_FOUND,
                "not_found",
                msg.clone(),
                None,
                None,
                None,
            ),
            AppError::Validation(msg) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "validation_error",
                msg.clone(),
                None,
                None,
                None,
            ),
            AppError::UnsupportedByStore {
                store,
                route,
                message,
            } => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "UnsupportedByStore",
                message.clone(),
                store.clone(),
                route.clone(),
                None,
            ),
            AppError::Forbidden(msg) => (
                StatusCode::FORBIDDEN,
                "forbidden",
                msg.clone(),
                None,
                None,
                None,
            ),
            AppError::PayloadTooLarge(msg) => (
                StatusCode::PAYLOAD_TOO_LARGE,
                "payload_too_large",
                msg.clone(),
                None,
                None,
                None,
            ),
            AppError::Conflict(msg) => (
                StatusCode::CONFLICT,
                "conflict",
                msg.clone(),
                None,
                None,
                None,
            ),
            AppError::PreconditionFailed(msg) => (
                StatusCode::PRECONDITION_FAILED,
                "precondition_failed",
                msg.clone(),
                None,
                None,
                None,
            ),
            AppError::GatewayTimeout(msg) => (
                StatusCode::GATEWAY_TIMEOUT,
                "gateway_timeout",
                msg.clone(),
                None,
                None,
                None,
            ),
        };

        let body = ErrorBody {
            error: error_type.to_string(),
            message,
            store,
            route,
            cache_state,
        };

        let mut response = (status, axum::Json(body)).into_response();
        if let Some(retry_after) = retry_after.and_then(|value| HeaderValue::from_str(value).ok()) {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, retry_after);
        }
        response
    }
}
