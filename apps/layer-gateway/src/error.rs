use axum::body::Body;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use vectorstore_core::capabilities::{StoreRejection, UNSUPPORTED_BY_STORE};

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("Upstream HTTP response {status}: {}", String::from_utf8_lossy(.body))]
    UpstreamResponse {
        status: u16,
        content_type: Option<String>,
        body: Vec<u8>,
    },

    #[error("Upstream error: {0}")]
    Upstream(String),

    /// Function completion never exposes upstream response bodies or inputs.
    #[error("Function store completion failed ({category})")]
    CompletionUpstream {
        upstream_status: Option<u16>,
        category: &'static str,
        retryable: Option<bool>,
    },

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

    #[error("GPU embedding worker unavailable: {0}")]
    EmbedWorkerUnavailable(String),

    #[error("Unsupported by store: {message}")]
    UnsupportedByStore {
        store: Option<String>,
        route: Option<String>,
        /// Stable identifier for what was rejected (RFC 0118 § "The 422
        /// body"): a wire-feature id where one exists, otherwise the wire
        /// key, dotted when nested. Recovered from a canonical message;
        /// free-form rejections carry none.
        feature: Option<String>,
        message: String,
    },

    /// A `/search` precondition the namespace or gateway does not meet
    /// (RFC 0116). `code` is the body's `error` string, the machine code:
    /// `embed_attribute_missing`, `embed_attribute_invalid`,
    /// `full_text_attribute_missing`, `rerank_unconfigured`.
    #[error("Search rejected ({code}): {message}")]
    SearchRejected { code: &'static str, message: String },

    /// The rerank provider failed and the request set `rerank.required`.
    #[error("Rerank unavailable: {0}")]
    RerankUnavailable(String),

    #[error("Forbidden: {0}")]
    Forbidden(String),

    #[error("Namespace not in grant: {namespace}")]
    NamespaceNotInGrant { namespace: String },

    #[error("Payload too large: {0}")]
    PayloadTooLarge(String),

    #[error("Conflict: {0}")]
    Conflict(String),

    #[cfg(feature = "pro")]
    #[error("Function completion conflict: {0:?}")]
    CompletionConflict(layer_transform::udf::CompletionConflictReason),

    #[error("Precondition failed: {0}")]
    PreconditionFailed(String),

    #[error("Gateway timeout: {0}")]
    GatewayTimeout(String),

    #[error("Object store not configured: {0}")]
    ObjectStoreNotConfigured(String),

    /// A blob over its store's native value cap, with no S3 to hold it.
    #[error("Blob exceeds store cap: {0}")]
    BlobExceedsStoreCap(String),

    /// A request body turbopuffer would also refuse, rejected before any
    /// side effect (RFC 0124: a branch or copy combined with a write).
    #[error("Bad request: {0}")]
    BadRequest(String),

    /// A branch or copy whose source resolves to a different VectorStore
    /// than its target (RFC 0124 § routing).
    #[error("Branch across stores: {0}")]
    BranchAcrossStores(String),
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
    feature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_state: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    upstream_status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    upstream_category: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retryable: Option<bool>,
}

impl AppError {
    pub fn from_completion_store(error: crate::clients::turbopuffer::TurbopufferError) -> Self {
        use crate::clients::turbopuffer::TurbopufferError;
        if matches!(error, TurbopufferError::QueryBudgetExhausted) {
            return Self::Conflict("Function provider query budget exhausted".into());
        }
        let upstream_status = match error {
            TurbopufferError::Response(response) => Some(response.status),
            TurbopufferError::RateLimited(_) => Some(429),
            TurbopufferError::NotFound(_) => Some(404),
            TurbopufferError::Other(_) | TurbopufferError::QueryBudgetExhausted => None,
        };
        // Classify structured status only. Free-form bodies and adapter strings
        // are private and cannot manufacture a permanent/transient diagnosis.
        let (category, retryable) = match upstream_status {
            Some(400 | 422) => ("validation", Some(false)),
            Some(429) => ("rate_limited", Some(true)),
            Some(503) => ("unavailable", Some(true)),
            Some(504) => ("timeout", Some(true)),
            _ => ("unknown", None),
        };
        Self::CompletionUpstream {
            upstream_status,
            category,
            retryable,
        }
    }

    pub fn from_turbopuffer(
        error: crate::clients::turbopuffer::TurbopufferError,
        context: impl AsRef<str>,
    ) -> Self {
        match error {
            crate::clients::turbopuffer::TurbopufferError::QueryBudgetExhausted => {
                Self::Conflict("Function provider query budget exhausted".into())
            }
            crate::clients::turbopuffer::TurbopufferError::Response(response) => {
                Self::UpstreamResponse {
                    // Caller errors retain the origin status. Origin failures
                    // are bad-gateway responses, with the original body intact.
                    status: if (400..500).contains(&response.status) {
                        response.status
                    } else {
                        StatusCode::BAD_GATEWAY.as_u16()
                    },
                    content_type: response.content_type,
                    body: response.body,
                }
            }
            error => Self::Upstream(format!("{}: {error}", context.as_ref())),
        }
    }

    /// A store rejection. A message in the canonical
    /// `UnsupportedByStore: {store}: {feature}` shape yields the typed
    /// `feature` and is trimmed to start at the marker; any other message is
    /// kept as written and names no feature.
    pub fn unsupported_by_store(
        message: impl Into<String>,
        store: Option<String>,
        route: Option<String>,
    ) -> Self {
        let message = message.into();
        let (feature, message) = match StoreRejection::parse(&message) {
            Some(rejection) => (
                Some(rejection.feature.to_string()),
                rejection.message.to_string(),
            ),
            None => (None, message),
        };
        Self::UnsupportedByStore {
            store,
            route,
            feature,
            message,
        }
    }

    /// A gateway-originated store rejection with an explicit feature id.
    pub fn unsupported_feature(
        store: &str,
        route: Option<String>,
        feature: &str,
        detail: impl AsRef<str>,
    ) -> Self {
        let detail = detail.as_ref();
        let mut message = format!("{UNSUPPORTED_BY_STORE}: {store}: {feature}");
        if !detail.is_empty() {
            message.push_str(": ");
            message.push_str(detail);
        }
        Self::UnsupportedByStore {
            store: Some(store.to_string()),
            route,
            feature: Some(feature.to_string()),
            message,
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
        error.to_string().contains(UNSUPPORTED_BY_STORE)
    }

    /// Map an S3 client failure into a response error: a gateway composed
    /// without an object store gets a clear 4xx naming the missing
    /// configuration; real object-store failures stay 502s.
    pub fn from_s3(error: crate::clients::s3::S3Error, context: impl AsRef<str>) -> Self {
        if error.is_not_configured() {
            Self::object_store_not_configured(context)
        } else {
            Self::Upstream(format!("{}: {error}", context.as_ref()))
        }
    }

    pub fn object_store_not_configured(feature: impl AsRef<str>) -> Self {
        Self::ObjectStoreNotConfigured(format!(
            "{} requires an S3-compatible object store, and this gateway has none \
             configured (set S3_BUCKET)",
            feature.as_ref()
        ))
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
        let feature = match &self {
            AppError::UnsupportedByStore { feature, .. } => feature.clone(),
            _ => None,
        };
        let (status, error_type, message, store, route, cache_state) = match &self {
            AppError::UpstreamResponse { .. } => unreachable!("handled above"),
            AppError::CompletionUpstream { category, .. } => (
                StatusCode::BAD_GATEWAY,
                "upstream_error",
                format!("Function completion store failure: {category}"),
                None,
                None,
                None,
            ),
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
            AppError::EmbedWorkerUnavailable(message) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "embed_worker_unavailable",
                message.clone(),
                None,
                None,
                None,
            ),
            AppError::UnsupportedByStore {
                store,
                route,
                message,
                ..
            } => (
                StatusCode::UNPROCESSABLE_ENTITY,
                UNSUPPORTED_BY_STORE,
                message.clone(),
                store.clone(),
                route.clone(),
                None,
            ),
            AppError::SearchRejected { code, message } => (
                StatusCode::UNPROCESSABLE_ENTITY,
                *code,
                message.clone(),
                None,
                None,
                None,
            ),
            AppError::RerankUnavailable(msg) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "rerank_unavailable",
                msg.clone(),
                None,
                None,
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
            AppError::NamespaceNotInGrant { namespace } => (
                StatusCode::FORBIDDEN,
                "namespace not in key grant",
                format!("namespace `{namespace}` is not in the authenticated key grant"),
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
            #[cfg(feature = "pro")]
            AppError::CompletionConflict(reason) => {
                return (
                    StatusCode::CONFLICT,
                    axum::Json(serde_json::json!({
                        "error": "conflict", "message": "Function completion rejected",
                        "reason": reason, "disposition": reason.disposition()
                    })),
                )
                    .into_response();
            }
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
            AppError::ObjectStoreNotConfigured(msg) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "object_store_not_configured",
                msg.clone(),
                None,
                None,
                None,
            ),
            AppError::BlobExceedsStoreCap(msg) => (
                StatusCode::PAYLOAD_TOO_LARGE,
                "blob_exceeds_store_cap",
                msg.clone(),
                None,
                None,
                None,
            ),
            AppError::BadRequest(msg) => (
                StatusCode::BAD_REQUEST,
                "invalid_request",
                msg.clone(),
                None,
                None,
                None,
            ),
            AppError::BranchAcrossStores(msg) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "BranchAcrossStores",
                msg.clone(),
                None,
                None,
                None,
            ),
        };

        let (upstream_status, upstream_category, retryable) = match &self {
            AppError::CompletionUpstream {
                upstream_status,
                category,
                retryable,
            } => (*upstream_status, Some(*category), *retryable),
            _ => (None, None, None),
        };
        let body = ErrorBody {
            error: error_type.to_string(),
            message,
            store,
            route,
            feature,
            cache_state,
            upstream_status,
            upstream_category,
            retryable,
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

#[cfg(test)]
mod capability_tests {
    use super::*;
    use vectorstore_core::capabilities::WireFeature;
    use vectorstore_core::pgvector_capabilities::PGVECTOR_CAPABILITIES;

    #[tokio::test]
    async fn declared_unsupported_feature_keeps_the_existing_422_wire_shape() {
        let error = PGVECTOR_CAPABILITIES
            .require(WireFeature::MultiQuery)
            .unwrap_err();
        assert!(AppError::is_store_support_error(&error));
        let response = AppError::from_store_support_error(
            error,
            Some("pgvector".into()),
            Some("batchQueryNamespace".into()),
        )
        .into_response();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"], "UnsupportedByStore");
        assert_eq!(body["store"], "pgvector");
        assert_eq!(body["route"], "batchQueryNamespace");
        assert_eq!(body["message"], "UnsupportedByStore: pgvector: multi_query");
        assert_eq!(body["feature"], "multi_query");
    }

    /// A free-form rejection names no feature rather than inventing one, and
    /// its message is kept as written.
    #[tokio::test]
    async fn free_form_rejection_carries_no_feature() {
        let response = AppError::unsupported_by_store(
            "UnsupportedByStore: namespace init shard backfill requires Turbopuffer",
            Some("search".into()),
            None,
        )
        .into_response();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"], "UnsupportedByStore");
        assert!(body.get("feature").is_none(), "{body}");
        assert_eq!(
            body["message"],
            "UnsupportedByStore: namespace init shard backfill requires Turbopuffer"
        );
    }

    #[tokio::test]
    async fn gateway_originated_rejection_names_its_feature() {
        let response = AppError::unsupported_feature(
            "pgvector",
            Some("queryNamespace".into()),
            "fuzzy",
            "phase-one hybrid requires fuzziness: 0",
        )
        .into_response();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["feature"], "fuzzy");
        assert_eq!(
            body["message"],
            "UnsupportedByStore: pgvector: fuzzy: phase-one hybrid requires fuzziness: 0"
        );
    }
}

#[cfg(test)]
mod completion_error_tests {
    use super::*;
    use crate::clients::turbopuffer::{TurbopufferError, TurbopufferPassthroughResponse};

    #[tokio::test]
    async fn completion_errors_classify_only_structured_status_and_hide_private_bodies() {
        for (status, category, retryable) in [
            (400, "validation", Some(false)),
            (422, "validation", Some(false)),
            (429, "rate_limited", Some(true)),
            (503, "unavailable", Some(true)),
            (504, "timeout", Some(true)),
            (502, "unknown", None),
            (404, "unknown", None),
        ] {
            let response = AppError::from_completion_store(TurbopufferError::Response(
                TurbopufferPassthroughResponse {
                    status,
                    content_type: Some("application/json".into()),
                    body:
                        br#"{"error":"synthetic_private_input","message":"schema type inference"}"#
                            .to_vec(),
                },
            ))
            .into_response();
            assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert!(!String::from_utf8_lossy(&bytes).contains("synthetic_private_input"));
            assert!(!String::from_utf8_lossy(&bytes).contains("schema type inference"));
            let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(body["upstream_status"], status);
            assert_eq!(body["upstream_category"], category);
            assert_eq!(
                body.get("retryable").and_then(serde_json::Value::as_bool),
                retryable
            );
        }
        let response = AppError::from_completion_store(TurbopufferError::Other(
            "HTTP 400 schema: synthetic_private_input".into(),
        ))
        .into_response();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["upstream_category"], "unknown");
        assert!(body.get("upstream_status").is_none());
        assert!(body.get("retryable").is_none());
    }
}
