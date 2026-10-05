//! Namespace branch and copy through the gateway (RFC 0124).
//!
//! A branch is the turbopuffer write `POST /v2/namespaces/{target}` with
//! `branch_from_namespace` (a copy: `copy_from_namespace`). The body goes to
//! the store byte-for-byte. Around it the gateway rejects bodies turbopuffer
//! would refuse, checks the caller can read the source, keeps source and
//! target on one VectorStore, and after upstream success carries the Layer
//! state that lives outside the namespace: the target's residue is reset,
//! embedding profiles are copied, the blob set is branched alongside, and the
//! lineage is recorded.
use std::sync::Arc;

use axum::body::Body;
use axum::http::{header, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use serde_json::Value;
use tracing::{info, warn};
use vectorstore_core::capabilities::{unsupported_by_store, Support, WireFeature};
use vectorstore_core::turbopuffer::{blob_set_namespace, branch_source_namespace};

use crate::auth::{authorize_namespace, ApiScope, CallerGrant};
use crate::clients::turbopuffer::TurbopufferPassthroughResponse;
use crate::error::AppError;
use crate::lineage::{read_lineage, write_lineage, Lineage};
use crate::AppState;

/// Write keys that put documents in the same request. turbopuffer requires a
/// branch or copy to carry none.
const DOCUMENT_WRITE_KEYS: [&str; 7] = [
    "upsert_rows",
    "upsert_columns",
    "patch_rows",
    "patch_columns",
    "deletes",
    "delete_by_filter",
    "patch_by_filter",
];

/// Client-side overload discriminator the generated SDKs append. It is not
/// an upstream parameter, so no store sees it.
const STAINLESS_OVERLOAD: &str = "stainless_overload";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CloneOp {
    Branch,
    Copy,
}

impl CloneOp {
    fn key(self) -> &'static str {
        self.feature().id()
    }

    fn feature(self) -> WireFeature {
        match self {
            Self::Branch => WireFeature::Branch,
            Self::Copy => WireFeature::Copy,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Branch => "branch",
            Self::Copy => "copy",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CloneRequest {
    pub op: CloneOp,
    pub source: String,
    /// A copy naming `source_api_key` or `source_region`: the source is in
    /// another organization or region, outside this gateway's view.
    pub external: bool,
}

/// A write that names `branch_from_namespace` or `copy_from_namespace`.
/// `request` is `Err` when turbopuffer would refuse the body's shape. The
/// store's capability is checked first, so a store that cannot branch at all
/// answers with its named 422 whatever else the body carries.
pub(crate) struct Classified {
    pub op: CloneOp,
    pub request: Result<CloneRequest, AppError>,
}

/// Whether `body` is a branch or copy. `None` is any other write.
pub(crate) fn classify(body: &Value) -> Option<Classified> {
    let object = body.as_object()?;
    let branch = object.get(CloneOp::Branch.key());
    let copy = object.get(CloneOp::Copy.key());
    let op = match (branch, copy) {
        (None, None) => return None,
        (Some(_), _) => CloneOp::Branch,
        (None, Some(_)) => CloneOp::Copy,
    };
    Some(Classified {
        op,
        request: validate(object, branch, copy),
    })
}

fn validate(
    object: &serde_json::Map<String, Value>,
    branch: Option<&Value>,
    copy: Option<&Value>,
) -> Result<CloneRequest, AppError> {
    let (op, value) = match (branch, copy) {
        (Some(value), None) => (CloneOp::Branch, value),
        (None, Some(value)) => (CloneOp::Copy, value),
        _ => return Err(AppError::BadRequest(
            "`branch_from_namespace` and `copy_from_namespace` cannot be combined in one request"
                .to_string(),
        )),
    };
    let key = op.key();
    if let Some(conflict) = DOCUMENT_WRITE_KEYS
        .iter()
        .find(|write| object.contains_key(**write))
    {
        return Err(AppError::BadRequest(format!(
            "`{key}` cannot be combined with `{conflict}`; {} first, then write",
            op.label()
        )));
    }
    if object.contains_key("schema") {
        return Err(AppError::BadRequest(format!(
            "`{key}` cannot be combined with `schema`; {} first, then change the schema",
            op.label()
        )));
    }
    let source = branch_source_namespace(value)
        .filter(|source| !source.trim().is_empty())
        .ok_or_else(|| {
            AppError::BadRequest(format!(
                "`{key}` must be a namespace name or an object with `source_namespace`"
            ))
        })?;
    let external = op == CloneOp::Copy
        && ["source_api_key", "source_region"]
            .iter()
            .any(|field| value.get(*field).is_some_and(|value| !value.is_null()));
    Ok(CloneRequest {
        op,
        source,
        external,
    })
}

/// The query string without `stainless_overload`, or `None` when nothing
/// else remains.
fn strip_overload(query: Option<&str>) -> Option<String> {
    let kept: Vec<&str> = query?
        .split('&')
        .filter(|pair| !pair.is_empty() && pair.split('=').next() != Some(STAINLESS_OVERLOAD))
        .collect();
    (!kept.is_empty()).then(|| kept.join("&"))
}

/// The same body, naming `source` instead: the blob-set branch or copy
/// that rides along with the data one.
fn retarget_body(body: &Value, op: CloneOp, source: &str) -> Value {
    let mut body = body.clone();
    if let Some(value) = body.get_mut(op.key()) {
        match value {
            Value::Object(object) => {
                object.insert(
                    "source_namespace".to_string(),
                    Value::String(source.to_string()),
                );
            }
            value => *value = Value::String(source.to_string()),
        }
    }
    body
}

/// turbopuffer namespace names are `[A-Za-z0-9-_.]`, so a name is its own
/// path segment.
fn namespace_path(namespace: &str) -> String {
    format!("/v2/namespaces/{namespace}")
}

fn into_response(response: TurbopufferPassthroughResponse) -> Result<Response, AppError> {
    let status = StatusCode::from_u16(response.status)
        .map_err(|e| AppError::Upstream(format!("invalid Turbopuffer status: {e}")))?;
    let mut builder = Response::builder().status(status);
    if let Some(content_type) = response.content_type {
        builder = builder.header(header::CONTENT_TYPE, content_type);
    }
    builder
        .body(Body::from(response.body))
        .map(IntoResponse::into_response)
        .map_err(|e| AppError::Upstream(format!("failed to build passthrough response: {e}")))
}

fn is_success(status: u16) -> bool {
    (200..300).contains(&status)
}

/// POST /v2/namespaces/{target} carrying `branch_from_namespace` or
/// `copy_from_namespace`.
pub(crate) async fn branch_or_copy(
    state: Arc<AppState>,
    target: &str,
    uri: &Uri,
    grant: Option<&CallerGrant>,
    classified: Classified,
    body: Value,
) -> Result<Response, AppError> {
    let op = classified.op;
    let capabilities = state.turbopuffer().capabilities_for_namespace(target);
    let store_ref = state.store_for_namespace(target);
    let observe = |outcome: &str| {
        state
            .metrics
            .observe_namespace_branch(capabilities.kind, &store_ref, op.label(), outcome);
    };

    // A store without native branching gets the named 422, never an
    // emulated copy.
    if capabilities.get(op.feature()).support == Support::Unsupported {
        observe("unsupported");
        return Err(AppError::from_store_support_error(
            unsupported_by_store(capabilities.kind, op.key(), None),
            Some(capabilities.kind.to_string()),
            Some("writeNamespace".to_string()),
        ));
    }
    let request = match classified.request {
        Ok(request) => request,
        Err(error) => {
            observe("invalid");
            return Err(error);
        }
    };

    if !request.external {
        if let Err(error) = authorize_namespace(&state, grant, ApiScope::Read, &request.source) {
            observe("forbidden");
            return Err(error);
        }
        let source_store = state.store_for_namespace(&request.source);
        if source_store != store_ref {
            observe("across_stores");
            return Err(AppError::BranchAcrossStores(format!(
                "`{}` resolves to VectorStore `{source_store}` and `{target}` to `{store_ref}`; \
                 a {} stays within one store. Declare an Index for `{target}` on `{source_store}`, \
                 or move data between stores with a migration",
                request.source,
                op.label()
            )));
        }
    }

    let _function_guard = if let Some(trigger) = state.write_trigger.as_ref() {
        Some(trigger.prepare_replacement(state.clone(), target).await?)
    } else {
        None
    };
    crate::run_guarded_write(_function_guard.as_deref(), async {
        let query = strip_overload(uri.query());
        let upstream = match state
            .turbopuffer()
            .passthrough("POST", uri.path(), query.as_deref(), Some(body.clone()))
            .await
        {
            Ok(response) => response,
            Err(error) => {
                observe("upstream_error");
                return Err(AppError::Upstream(format!(
                    "Turbopuffer passthrough failed: {error}"
                )));
            }
        };
        if !is_success(upstream.status) {
            // Return the store's answer unchanged. Prepared source proof remains
            // conservatively revoked after an attempted namespace replacement.
            observe("upstream_error");
            return into_response(upstream);
        }

        match carry_layer_state(&state, target, &request, &body, capabilities.blobs.native).await {
            Ok(lineage) => {
                observe("ok");
                info!(
                    op = op.label(),
                    source = %request.source,
                    target,
                    store_ref = %store_ref,
                    lineage_depth = lineage.ancestors.len(),
                    key = grant.map(grant_name).unwrap_or("open"),
                    "namespace {} created",
                    op.label()
                );
                into_response(upstream)
            }
            Err(error) => {
                observe("layer_error");
                Err(error)
            }
        }
    })
    .await
}

fn grant_name(grant: &CallerGrant) -> &str {
    match grant {
        CallerGrant::Declared(_) => "declared",
        #[cfg(feature = "pro")]
        CallerGrant::Minted(key) => key.resource_name.as_str(),
    }
}

/// Everything after upstream success. A failure deletes what the store just
/// made, so a branch is never half-made.
async fn carry_layer_state(
    state: &AppState,
    target: &str,
    request: &CloneRequest,
    body: &Value,
    store_holds_blobs: bool,
) -> Result<Lineage, AppError> {
    let mut created = vec![target.to_string()];
    let result = async {
        // The blob set rides along so the target owns its bytes. A source
        // that never stored a blob has no blob set.
        let blob_sets = !request.external
            && store_holds_blobs
            && state.turbopuffer().blob_storage(&request.source).native
            && state.turbopuffer().blob_storage(target).native;
        let source_set = blob_set_namespace(&request.source);
        let source_has_blobs = blob_sets
            && match state.turbopuffer().head_namespace(&source_set).await {
                Ok(_) => true,
                Err(error) if error.is_not_found() => false,
                Err(error) => {
                    return Err(AppError::from_turbopuffer(
                        error,
                        "source blob set metadata",
                    ))
                }
            };
        if source_has_blobs {
            let target_set = blob_set_namespace(target);
            let response = state
                .turbopuffer()
                .passthrough(
                    "POST",
                    &namespace_path(&target_set),
                    None,
                    Some(retarget_body(body, request.op, &source_set)),
                )
                .await
                .map_err(|e| AppError::Upstream(format!("blob set {}: {e}", request.op.label())))?;
            if is_success(response.status) {
                created.push(target_set);
            } else {
                return Err(AppError::UpstreamResponse {
                    status: response.status,
                    content_type: response.content_type,
                    body: response.body,
                });
            }
        }

        // The destination was empty to the store, but a deleted namespace of
        // the same name can have left cache, snapshots or history behind.
        let outcome = crate::routes::namespaces::reset_namespace_layer_state(state, target).await;
        if !outcome.errors.is_empty() {
            warn!(target, errors = ?outcome.errors, "branch target residue reset was incomplete");
        }
        crate::routes::namespaces::purge_in_memory_namespace_state(state, target);

        if request.external {
            crate::routes::embed_wire::clear_profiles(state, target).await?;
            write_lineage(state, target, &Lineage::default()).await?;
            return Ok(Lineage::default());
        }
        crate::routes::embed_wire::copy_profiles(state, &request.source, target).await?;
        let source_lineage = read_lineage(state, &request.source).await;
        let (at, _) = crate::history::now_timestamp();
        let lineage = match request.op {
            CloneOp::Branch => Lineage::branch_of(&request.source, &source_lineage, &at),
            CloneOp::Copy => Lineage::copy_of(&request.source, &source_lineage, &at),
        };
        write_lineage(state, target, &lineage).await?;
        Ok(lineage)
    }
    .await;

    if result.is_err() {
        for namespace in created.iter().rev() {
            match state.turbopuffer().delete_namespace(namespace).await {
                Ok(response) if is_success(response.status) || response.status == 404 => {}
                Ok(response) => warn!(
                    namespace = %namespace,
                    status = response.status,
                    "rollback delete of a half-made branch failed"
                ),
                Err(error) => warn!(
                    namespace = %namespace,
                    %error,
                    "rollback delete of a half-made branch failed"
                ),
            }
        }
        crate::routes::namespaces::purge_in_memory_namespace_state(state, target);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn classified(body: Value) -> Result<Option<CloneRequest>, AppError> {
        classify(&body).map(|c| c.request).transpose()
    }

    #[test]
    fn classifies_both_spellings() {
        for value in [json!("trunk"), json!({"source_namespace": "trunk"})] {
            let request = classified(json!({"branch_from_namespace": value}))
                .unwrap()
                .unwrap();
            assert_eq!(request.op, CloneOp::Branch);
            assert_eq!(request.source, "trunk");
            assert!(!request.external);
        }
        assert_eq!(classified(json!({"upsert_rows": []})).unwrap(), None);
    }

    #[test]
    fn a_copy_with_a_foreign_key_or_region_is_external() {
        let request = classified(json!({"copy_from_namespace": {
            "source_namespace": "a", "source_api_key": "tpuf_x"
        }}))
        .unwrap()
        .unwrap();
        assert!(request.external);
        let request = classified(json!({"copy_from_namespace": {
            "source_namespace": "a", "source_region": "gcp-us-central1"
        }}))
        .unwrap()
        .unwrap();
        assert!(request.external);
        let request = classified(json!({"copy_from_namespace": "a"}))
            .unwrap()
            .unwrap();
        assert!(!request.external);
    }

    #[test]
    fn rejects_mixed_bodies_naming_the_conflict() {
        for key in DOCUMENT_WRITE_KEYS {
            let mut body = json!({"branch_from_namespace": "trunk"});
            body[key] = json!([]);
            let Err(AppError::BadRequest(message)) = classified(body) else {
                panic!("{key} must be rejected");
            };
            assert!(message.contains(key), "{message}");
        }
        let Err(AppError::BadRequest(message)) =
            classified(json!({"copy_from_namespace": "a", "schema": {}}))
        else {
            panic!("schema must be rejected");
        };
        assert!(message.contains("schema"));
        assert!(matches!(
            classified(json!({"copy_from_namespace": "a", "branch_from_namespace": "b"})),
            Err(AppError::BadRequest(_))
        ));
        assert!(matches!(
            classified(json!({"branch_from_namespace": 7})),
            Err(AppError::BadRequest(_))
        ));
    }

    #[test]
    fn strips_only_the_overload_parameter() {
        assert_eq!(strip_overload(Some("stainless_overload=branchFrom")), None);
        assert_eq!(
            strip_overload(Some("a=1&stainless_overload=copyFrom&b=2")).as_deref(),
            Some("a=1&b=2")
        );
        assert_eq!(strip_overload(None), None);
    }

    #[test]
    fn retargets_either_spelling() {
        assert_eq!(
            retarget_body(
                &json!({"branch_from_namespace": "a"}),
                CloneOp::Branch,
                "a__hevlayer_blobs"
            ),
            json!({"branch_from_namespace": "a__hevlayer_blobs"})
        );
        assert_eq!(
            retarget_body(
                &json!({"copy_from_namespace": {"source_namespace": "a"}, "encryption": {"cmek": {"key_name": "k"}}}),
                CloneOp::Copy,
                "a__hevlayer_blobs"
            ),
            json!({"copy_from_namespace": {"source_namespace": "a__hevlayer_blobs"}, "encryption": {"cmek": {"key_name": "k"}}})
        );
    }
}
