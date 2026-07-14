use std::collections::HashMap;
use std::env;

use serde::Deserialize;
use serde_json::Value;

use crate::auth::InboundAuth;

#[derive(Debug, Clone)]
pub struct ResolvedVectorStore {
    pub name: String,
    pub kind: ResolvedVectorStoreKind,
    pub endpoint_url: String,
    pub endpoint_region: Option<String>,
    pub upstream_api_key: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedVectorStoreKind {
    Turbopuffer,
    Search,
}

#[derive(Debug, Clone)]
pub struct ResolvedVectorStores {
    pub default_store: String,
    pub stores: HashMap<String, ResolvedVectorStore>,
    pub inbound_auth: InboundAuth,
}

#[derive(Debug, thiserror::Error)]
pub enum VectorStoreError {
    #[error("VectorStore resource in {config_source} is missing metadata.name")]
    MissingName { config_source: String },
    #[error("VectorStore {namespace}/{name} is invalid: {message}")]
    InvalidSpec {
        namespace: String,
        name: String,
        message: String,
    },
    #[error("no default VectorStore found in namespace {namespace}")]
    MissingDefault { namespace: String },
    #[error("multiple default VectorStores found in namespace {namespace}: {names}")]
    DuplicateDefault { namespace: String, names: String },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct VectorStoreSpec {
    kind: String,
    #[serde(default)]
    default: bool,
    endpoint: EndpointSpec,
    #[serde(default)]
    credential: Option<CredentialSpec>,
    #[serde(default)]
    inbound_auth: Option<InboundAuthSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct VectorStoreResource {
    #[serde(default)]
    kind: Option<String>,
    metadata: ResourceMetadata,
    spec: VectorStoreSpec,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResourceMetadata {
    #[serde(default)]
    name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct VectorStoreResourceList {
    items: Vec<VectorStoreResource>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EndpointSpec {
    url: String,
    #[serde(default)]
    region: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CredentialSpec {
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default)]
    secret_ref: SecretRef,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SecretRef {
    #[serde(default)]
    name: String,
    #[serde(default)]
    key: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InboundAuthSpec {
    #[serde(default)]
    mode: InboundAuthMode,
}

impl Default for InboundAuthSpec {
    fn default() -> Self {
        Self {
            mode: InboundAuthMode::DeriveFromStore,
        }
    }
}

#[derive(Debug, Default, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
enum InboundAuthMode {
    #[default]
    DeriveFromStore,
}

pub async fn resolve_vector_stores_from_json(
    raw: &str,
    namespace: &str,
) -> Result<ResolvedVectorStores, VectorStoreError> {
    let value: Value =
        serde_json::from_str(raw).map_err(|error| VectorStoreError::InvalidSpec {
            namespace: namespace.to_string(),
            name: "LAYER_STORE_JSON".to_string(),
            message: error.to_string(),
        })?;
    let specs = parse_vector_store_config_value(value, "LAYER_STORE_JSON", namespace)?;
    resolve_vector_store_specs(namespace, specs)
}

pub async fn resolve_vector_stores_from_yaml(
    raw: &str,
    source: &str,
    namespace: &str,
) -> Result<ResolvedVectorStores, VectorStoreError> {
    let value: Value =
        serde_yaml::from_str(raw).map_err(|error| VectorStoreError::InvalidSpec {
            namespace: namespace.to_string(),
            name: source.to_string(),
            message: error.to_string(),
        })?;
    let specs = parse_vector_store_config_value(value, source, namespace)?;
    resolve_vector_store_specs(namespace, specs)
}

fn parse_vector_store_config_value(
    value: Value,
    source: &str,
    namespace: &str,
) -> Result<Vec<(String, VectorStoreSpec)>, VectorStoreError> {
    if value.get("spec").is_some() {
        let resource: VectorStoreResource =
            serde_json::from_value(value).map_err(|error| VectorStoreError::InvalidSpec {
                namespace: namespace.to_string(),
                name: source.to_string(),
                message: error.to_string(),
            })?;
        return Ok(vec![resource_into_spec(resource, source, namespace)?]);
    }

    if value.get("items").is_some() {
        let list: VectorStoreResourceList =
            serde_json::from_value(value).map_err(|error| VectorStoreError::InvalidSpec {
                namespace: namespace.to_string(),
                name: source.to_string(),
                message: error.to_string(),
            })?;
        let mut specs = Vec::with_capacity(list.items.len());
        for resource in list.items {
            specs.push(resource_into_spec(resource, source, namespace)?);
        }
        return Ok(specs);
    }

    let specs: HashMap<String, VectorStoreSpec> =
        serde_json::from_value(value).map_err(|error| VectorStoreError::InvalidSpec {
            namespace: namespace.to_string(),
            name: source.to_string(),
            message: error.to_string(),
        })?;
    Ok(specs.into_iter().collect())
}

fn resource_into_spec(
    resource: VectorStoreResource,
    source: &str,
    namespace: &str,
) -> Result<(String, VectorStoreSpec), VectorStoreError> {
    if !resource
        .kind
        .as_deref()
        .unwrap_or("VectorStore")
        .eq_ignore_ascii_case("VectorStore")
    {
        return Err(VectorStoreError::InvalidSpec {
            namespace: namespace.to_string(),
            name: source.to_string(),
            message: "resource kind must be VectorStore".to_string(),
        });
    }
    let name = resource.metadata.name.trim();
    if name.is_empty() {
        return Err(VectorStoreError::MissingName {
            config_source: source.to_string(),
        });
    }
    Ok((name.to_string(), resource.spec))
}

fn resolve_vector_store_specs(
    namespace: &str,
    specs: Vec<(String, VectorStoreSpec)>,
) -> Result<ResolvedVectorStores, VectorStoreError> {
    let mut defaults = specs
        .iter()
        .filter(|(_, spec)| spec.default)
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();

    let default_store_name = match defaults.len() {
        1 => defaults.remove(0),
        0 if specs.len() == 1 => specs[0].0.clone(),
        0 => {
            return Err(VectorStoreError::MissingDefault {
                namespace: namespace.to_string(),
            })
        }
        _ => {
            defaults.sort();
            return Err(VectorStoreError::DuplicateDefault {
                namespace: namespace.to_string(),
                names: defaults.join(","),
            });
        }
    };

    let mut resolved = HashMap::new();
    let mut inbound_auth = None;
    for (name, spec) in specs {
        let kind = match spec.kind.as_str() {
            kind if kind.eq_ignore_ascii_case("turbopuffer") => {
                ResolvedVectorStoreKind::Turbopuffer
            }
            kind if kind.eq_ignore_ascii_case("search") => ResolvedVectorStoreKind::Search,
            _ => {
                return Err(VectorStoreError::InvalidSpec {
                    namespace: namespace.to_string(),
                    name,
                    message: format!("spec.kind={} is reserved but not implemented", spec.kind),
                });
            }
        };
        if spec.endpoint.url.trim().is_empty() {
            return Err(VectorStoreError::InvalidSpec {
                namespace: namespace.to_string(),
                name,
                message: "spec.endpoint.url is required".to_string(),
            });
        }
        let upstream_api_key = spec.credential.as_ref().and_then(resolve_credential);
        let inbound_mode = spec
            .inbound_auth
            .as_ref()
            .map(|auth| auth.mode)
            .unwrap_or_default();
        if upstream_api_key.is_none()
            && kind == ResolvedVectorStoreKind::Turbopuffer
            && (name != default_store_name || inbound_mode != InboundAuthMode::DeriveFromStore)
        {
            return Err(VectorStoreError::InvalidSpec {
                namespace: namespace.to_string(),
                name,
                message: "spec.credential.apiKey is required for non-default standalone turbopuffer stores and non-deriveFromStore auth"
                    .to_string(),
            });
        }
        let auth = resolve_inbound_auth(namespace, &name, &spec, upstream_api_key.as_deref())?;
        if name == default_store_name {
            inbound_auth = Some(auth);
        }
        resolved.insert(
            name.clone(),
            ResolvedVectorStore {
                name,
                kind,
                endpoint_url: spec.endpoint.url.trim().to_string(),
                endpoint_region: spec
                    .endpoint
                    .region
                    .as_deref()
                    .and_then(trimmed_non_empty)
                    .map(str::to_string),
                upstream_api_key,
            },
        );
    }

    if !resolved.contains_key(&default_store_name) {
        return Err(VectorStoreError::InvalidSpec {
            namespace: namespace.to_string(),
            name: default_store_name,
            message: "default VectorStore was not found after listing stores".to_string(),
        });
    }

    Ok(ResolvedVectorStores {
        default_store: default_store_name,
        stores: resolved,
        inbound_auth: inbound_auth.expect("default VectorStore inbound auth was resolved"),
    })
}

fn resolve_inbound_auth(
    _namespace: &str,
    _store_name: &str,
    spec: &VectorStoreSpec,
    upstream_api_key: Option<&str>,
) -> Result<InboundAuth, VectorStoreError> {
    let mode = spec
        .inbound_auth
        .as_ref()
        .map(|auth| auth.mode)
        .unwrap_or_default();
    match mode {
        InboundAuthMode::DeriveFromStore => {
            if let Some(api_key) = upstream_api_key.and_then(trimmed_non_empty) {
                Ok(InboundAuth::derived_admin_key(api_key.to_string()))
            } else {
                Ok(InboundAuth::derive_from_request())
            }
        }
    }
}

fn resolve_credential(credential: &CredentialSpec) -> Option<String> {
    credential
        .api_key
        .as_deref()
        .and_then(trimmed_non_empty)
        .map(str::to_string)
        .or_else(|| standalone_secret_value(&credential.secret_ref))
}

fn standalone_secret_value(secret_ref: &SecretRef) -> Option<String> {
    env::var(standalone_secret_env_name(secret_ref))
        .ok()
        .and_then(|value| trimmed_non_empty(&value).map(str::to_string))
}

fn standalone_secret_env_name(secret_ref: &SecretRef) -> String {
    format!(
        "LAYER_SECRET_{}_{}",
        env_key_part(&secret_ref.name),
        env_key_part(&secret_ref.key)
    )
}

fn env_key_part(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

fn trimmed_non_empty(value: &str) -> Option<&str> {
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

