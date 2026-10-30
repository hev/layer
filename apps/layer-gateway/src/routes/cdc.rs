//! Read-only CDC readiness inspection. Capture and buffer payload access are not
//! enabled by this route; both store grants are checked before projection.
use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use kube::{
    api::{Api, DynamicObject},
    core::{ApiResource, GroupVersionKind},
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    auth::{ApiScope, CallerGrant},
    AppState,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Reference {
    pub name: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Scope {
    source_ref: Reference,
    destination_ref: Reference,
    buffer_ref: Reference,
    namespaces: Vec<String>,
    #[serde(default = "disabled")]
    capture: String,
}
fn disabled() -> String {
    "disabled".into()
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    #[serde(rename = "type")]
    pub type_: String,
    pub status: String,
    pub reason: Option<String>,
    pub message: Option<String>,
    pub last_transition_time: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Observed {
    observed_generation: Option<i64>,
    capture_state: Option<String>,
    #[serde(default)]
    conditions: Vec<Condition>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Inspection {
    pub name: String,
    pub generation: i64,
    pub observed_generation: Option<i64>,
    pub source_ref: Reference,
    pub destination_ref: Reference,
    pub buffer_ref: Reference,
    pub namespaces: Vec<String>,
    pub capture_requested: String,
    pub capture_state: String,
    pub ready: bool,
    pub conditions: Vec<Condition>,
}

#[derive(Debug)]
struct Failure(StatusCode, &'static str);
impl Failure {
    #[cfg(test)]
    fn status(&self) -> StatusCode {
        self.0
    }
}
impl IntoResponse for Failure {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error":{"code":self.1}}))).into_response()
    }
}
fn failure(status: StatusCode, code: &'static str) -> Failure {
    Failure(status, code)
}
fn error(status: StatusCode, code: &'static str) -> Response {
    failure(status, code).into_response()
}

fn authenticated(grant: Option<&CallerGrant>) -> Result<&CallerGrant, Failure> {
    grant.ok_or_else(|| failure(StatusCode::UNAUTHORIZED, "cdc_authentication_required"))
}

fn authorize(grant: &CallerGrant, spec: &Scope) -> Result<(), Failure> {
    if spec.buffer_ref.name.is_empty()
        || !matches!(spec.capture.as_str(), "disabled" | "enabled")
        || spec
            .namespaces
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != spec.namespaces.len()
        || spec.namespaces.is_empty()
        || spec.namespaces.len() > 100
        || spec.source_ref.name.is_empty()
        || spec.destination_ref.name.is_empty()
        || spec.source_ref.name == spec.destination_ref.name
        || spec
            .namespaces
            .iter()
            .any(|n| n.is_empty() || n.contains('/') || n.contains('*'))
    {
        return Err(failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "cdc_invalid_scope",
        ));
    }
    match grant {
        CallerGrant::Declared(scopes)
            if scopes.contains(&ApiScope::Read) || scopes.contains(&ApiScope::Admin) =>
        {
            Ok(())
        }
        CallerGrant::Declared(_) => Err(failure(StatusCode::FORBIDDEN, "cdc_scope_denied")),
        CallerGrant::Minted(key) => {
            for namespace in &spec.namespaces {
                for store in [&spec.source_ref.name, &spec.destination_ref.name] {
                    layer_rbac::authorize_minted_key(
                        key,
                        ApiScope::Read,
                        &format!("/v2/namespaces/{namespace}"),
                        |_| store.clone(),
                    )
                    .map_err(|_| failure(StatusCode::FORBIDDEN, "cdc_scope_denied"))?;
                }
            }
            Ok(())
        }
    }
}

fn project(item: DynamicObject, grant: &CallerGrant) -> Result<Inspection, Failure> {
    let spec: Scope = serde_json::from_value(item.data.get("spec").cloned().unwrap_or_default())
        .map_err(|_| failure(StatusCode::SERVICE_UNAVAILABLE, "cdc_invalid_scope"))?;
    authorize(grant, &spec)?;
    let observed: Observed = serde_json::from_value(
        item.data
            .get("status")
            .cloned()
            .unwrap_or_else(|| json!({})),
    )
    .map_err(|_| failure(StatusCode::SERVICE_UNAVAILABLE, "cdc_invalid_status"))?;
    let generation = item
        .metadata
        .generation
        .filter(|g| *g > 0)
        .ok_or_else(|| failure(StatusCode::SERVICE_UNAVAILABLE, "cdc_invalid_scope"))?;
    if item.metadata.name.as_deref().is_none_or(str::is_empty) {
        return Err(failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "cdc_invalid_scope",
        ));
    }
    let current = observed.observed_generation == Some(generation);
    // No active capture implementation is installed at this checkpoint. Never
    // turn a stale or hand-authored status.active into a readiness claim.
    let capture_state = if current
        && spec.capture == "disabled"
        && observed.capture_state.as_deref() == Some("disabled")
    {
        "disabled"
    } else {
        "blocked"
    };
    let mut conditions = observed.conditions;
    conditions.retain(|condition| condition.type_ != "Ready");
    conditions.push(Condition {
        type_: "Ready".into(),
        status: "False".into(),
        reason: Some(
            if current {
                "CaptureImplementationIncomplete"
            } else {
                "ObservedGenerationStale"
            }
            .into(),
        ),
        message: Some("Readiness inspection does not enable capture".into()),
        last_transition_time: None,
    });
    Ok(Inspection {
        name: item.metadata.name.unwrap_or_default(),
        generation,
        observed_generation: observed.observed_generation,
        source_ref: spec.source_ref,
        destination_ref: spec.destination_ref,
        buffer_ref: spec.buffer_ref,
        namespaces: spec.namespaces,
        capture_requested: spec.capture,
        capture_state: capture_state.into(),
        ready: false,
        conditions,
    })
}

pub async fn inspect(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    grant: Option<Extension<CallerGrant>>,
) -> Response {
    let grant = match authenticated(grant.as_ref().map(|Extension(g)| g)) {
        Ok(g) => g,
        Err(r) => return r.into_response(),
    };
    #[cfg(feature = "cdc-capture")]
    if let Some(binding) = state
        .cdc_capture
        .as_ref()
        .and_then(|r| r.observation_named(&name))
    {
        if !binding.authorized(Some(grant)) {
            return error(StatusCode::FORBIDDEN, "cdc_scope_denied");
        }
        return binding.inspect(None).await;
    }
    let Some(client) = state.kube.clone() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "cdc_control_plane_unavailable",
        );
    };
    inspect_with_client(client, &state.vector_store_namespace, &name, grant).await
}

async fn inspect_with_client(
    client: kube::Client,
    namespace: &str,
    name: &str,
    grant: &CallerGrant,
) -> Response {
    let mut resource =
        ApiResource::from_gvk(&GroupVersionKind::gvk("hevlayer.com", "v1alpha1", "CDC"));
    resource.plural = "cdcs".into();
    let api: Api<DynamicObject> = Api::namespaced_with(client, namespace, &resource);
    match api.get(name).await {
        Ok(item) => match project(item, grant) {
            Ok(value) => Json(value).into_response(),
            Err(response) => response.into_response(),
        },
        Err(kube::Error::Api(e)) if e.code == 404 => {
            error(StatusCode::NOT_FOUND, "cdc_binding_not_found")
        }
        Err(_) => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "cdc_control_plane_unavailable",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use layer_rbac::{RbacEntitlement, VerifiedMintedKey};
    use serde_json::Value;
    fn item() -> DynamicObject {
        serde_json::from_value(json!({"apiVersion":"hevlayer.com/v1alpha1","kind":"CDC",
            "metadata":{"name":"archive","generation":2},
            "spec":{"sourceRef":{"name":"source"},"destinationRef":{"name":"dest"},
                "bufferRef":{"name":"buffer"},"namespaces":["archive","sessions"],"capture":"disabled",
                "privateIgnored":"never expose"},
            "status":{"observedGeneration":2,"captureState":"disabled","privateIgnored":"never expose",
                "conditions":[{"type":"Ready","status":"True"}]}})).unwrap()
    }
    fn grant(stores: &[(&str, &[&str])]) -> CallerGrant {
        let mut key = VerifiedMintedKey::default();
        for (store, namespaces) in stores {
            key.entitlements.insert(
                format!("vectorstore.{store}"),
                RbacEntitlement {
                    scopes: vec!["read".into()],
                    namespaces: namespaces.iter().map(|s| (*s).into()).collect(),
                },
            );
        }
        CallerGrant::Minted(Arc::new(key))
    }
    #[test]
    fn both_store_scopes_cover_every_namespace_before_inspection() {
        assert_eq!(
            project(item(), &grant(&[("source", &["*"])]))
                .unwrap_err()
                .status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            project(
                item(),
                &grant(&[("source", &["*"]), ("dest", &["archive"])])
            )
            .unwrap_err()
            .status(),
            StatusCode::FORBIDDEN
        );
        let value = serde_json::to_value(
            project(item(), &grant(&[("source", &["*"]), ("dest", &["*"])])).unwrap(),
        )
        .unwrap();
        assert_eq!(value["captureState"], "disabled");
        assert_eq!(value["ready"], false);
        assert!(!value.to_string().contains("never expose"));
        assert_eq!(value["conditions"][0]["status"], "False");
    }
    #[test]
    fn anonymous_stale_and_enabled_never_claim_ready() {
        assert_eq!(
            authenticated(None).err().unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        let admin = CallerGrant::Declared(vec![ApiScope::Admin]);
        let mut object = item();
        object.data["status"]["observedGeneration"] = json!(1);
        let value = project(object, &admin).unwrap();
        assert_eq!(value.capture_state, "blocked");
        let mut blocked = item();
        blocked.data["status"]["captureState"] = json!("blocked");
        assert_eq!(project(blocked, &admin).unwrap().capture_state, "blocked");
        let mut active = item();
        active.data["status"]["captureState"] = json!("active");
        assert_eq!(project(active, &admin).unwrap().capture_state, "blocked");
        let mut object = item();
        object.data["spec"]["capture"] = json!("enabled");
        let value = project(object, &admin).unwrap();
        assert_eq!(value.capture_state, "blocked");
        assert!(!value.ready);
    }
    #[test]
    fn malformed_resources_fail_closed() {
        let admin = CallerGrant::Declared(vec![ApiScope::Admin]);
        let mut cases = Vec::new();
        let mut value = item();
        value.data["spec"]["bufferRef"]["name"] = json!("");
        cases.push(value);
        let mut value = item();
        value.data["spec"]["namespaces"] = json!(["archive", "archive"]);
        cases.push(value);
        let mut value = item();
        value.data["spec"]["capture"] = json!("active");
        cases.push(value);
        let mut value = item();
        value.metadata.generation = None;
        cases.push(value);
        let mut value = item();
        value.metadata.name = None;
        cases.push(value);
        let mut value = item();
        value.data["spec"] = json!([]);
        cases.push(value);
        let mut value = item();
        value.data["status"]["conditions"] = json!("invalid");
        cases.push(value);
        for value in cases {
            assert_eq!(
                project(value, &admin).unwrap_err().status(),
                StatusCode::SERVICE_UNAVAILABLE
            );
        }
    }

    #[tokio::test]
    async fn real_handler_projects_kubernetes_response_over_http() {
        use axum::{
            body::Body,
            http::{Request, Response as HttpResponse},
        };
        use tower::ServiceExt;
        let object = serde_json::to_vec(&item()).unwrap();
        let mock = tower::service_fn(move |request: Request<kube::client::Body>| {
            assert_eq!(
                request.uri().path(),
                "/apis/hevlayer.com/v1alpha1/namespaces/layer/cdcs/archive"
            );
            let body = object.clone();
            async move { Ok::<_, std::convert::Infallible>(HttpResponse::new(Body::from(body))) }
        });
        let client = kube::Client::new(mock, "layer");
        let grant = grant(&[("source", &["*"]), ("dest", &["*"])]);
        let router = axum::Router::new().route(
            "/v2/cdc/{binding}",
            axum::routing::get(move |Path(name): Path<String>| {
                let client = client.clone();
                let grant = grant.clone();
                async move { inspect_with_client(client, "layer", &name, &grant).await }
            }),
        );
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/v2/cdc/archive")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), 65536)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["name"], "archive");
        assert_eq!(value["ready"], false);
    }
}
