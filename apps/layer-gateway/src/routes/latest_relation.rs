//! Gateway-owned complete latest-before-filter aggregate contract.
use crate::{error::AppError, AppState};
use axum::{
    response::{IntoResponse, Response},
    Json,
};
use serde_json::Value;
use std::sync::Arc;
use vectorstore_core::latest_relation::LatestRequest;

tokio::task_local! { pub(crate) static CALLER_SCOPE: String; }

pub(crate) async fn run(
    state: Arc<AppState>,
    namespace: String,
    body: Value,
) -> Result<Response, AppError> {
    let request: LatestRequest = serde_json::from_value(body)
        .map_err(|e| AppError::BadRequest(format!("invalid latest relation: {e}")))?;
    request.validate().map_err(AppError::BadRequest)?;
    state
        .turbopuffer()
        .capabilities_for_namespace(&namespace)
        .require(vectorstore_core::capabilities::WireFeature::LatestRelation)
        .map_err(|e| {
            AppError::from_store_support_error(
                e,
                Some(
                    state
                        .turbopuffer()
                        .capabilities_for_namespace(&namespace)
                        .kind
                        .into(),
                ),
                Some("aggregate".into()),
            )
        })?;
    if crate::shards::active_shard_count(&state, &namespace)
        .await
        .is_some()
    {
        return Err(AppError::BadRequest(
            "latest_by does not support sharded namespaces".into(),
        ));
    }
    let source = state
        .turbopuffer()
        .latest_relation_source_for_request(&namespace, &request)
        .await
        .map_err(|e| {
            if let vectorstore_core::turbopuffer::TurbopufferError::Other(message) = &e {
                if message == "latest relation: scan row budget exceeded; no partial result"
                    || message == "latest relation: scan byte budget exceeded; no partial result"
                {
                    return AppError::BadRequest(message.clone());
                }
            }
            if let Some(rejection) =
                vectorstore_core::capabilities::StoreRejection::parse(&e.to_string())
            {
                AppError::unsupported_feature(
                    rejection.store,
                    Some("aggregate".into()),
                    rejection.feature,
                    rejection.message.splitn(4, ": ").nth(3).unwrap_or(""),
                )
            } else {
                AppError::from_turbopuffer(e, "latest relation source failed")
            }
        })?;
    let caller = CALLER_SCOPE
        .try_with(Clone::clone)
        .map_err(|_| AppError::BadRequest("authenticated executor scope unavailable".into()))?;
    let scope = serde_json::json!({
        "namespace":namespace,
        "store":state.store_for_namespace(&namespace),
        "store_namespace":state.vector_store_namespace,
        "caller":caller,
    })
    .to_string();
    let response = request
        .execute(source, scope)
        .map_err(AppError::BadRequest)?;
    Ok(Json(response).into_response())
}

pub(crate) async fn aggregate_latest(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    axum::extract::Path(namespace): axum::extract::Path<String>,
    Json(body): Json<Value>,
) -> Result<Response, AppError> {
    run(state, namespace, body).await
}

pub(crate) fn reject_nested(body: &Value) -> Result<(), AppError> {
    if body
        .get("queries")
        .and_then(Value::as_array)
        .is_some_and(|queries| queries.iter().any(|q| q.get("latest_by").is_some()))
    {
        return Err(AppError::BadRequest(
            "latest_by is not supported inside multi-query bodies".into(),
        ));
    }
    Ok(())
}
