use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tracing::{info, warn};

use crate::AppState;

#[derive(Debug, thiserror::Error)]
pub enum IndexConfigError {
    #[error("kubernetes: {0}")]
    Kube(String),
    #[error("index {index}: {message}")]
    InvalidIndex { index: String, message: String },
    #[error("multiple Index CRs resolve to backend namespace '{namespace}': {first}, {second}")]
    DuplicateNamespace {
        namespace: String,
        first: String,
        second: String,
    },
    #[error("Index CRs {first} ('{first_pattern}') and {second} ('{second_pattern}') declare overlapping namespace patterns")]
    OverlappingNamespacePatterns {
        first: String,
        first_pattern: String,
        second: String,
        second_pattern: String,
    },
}

#[async_trait]
pub trait IndexConfigSource: Send + Sync {
    async fn load_index_config(&self) -> Result<IndexConfig, IndexConfigError>;

    async fn load_facet_fields(&self) -> Result<HashMap<String, Vec<String>>, IndexConfigError> {
        Ok(self.load_index_config().await?.facet_fields)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IndexConfig {
    pub field_stats: HashMap<String, FieldStatsPolicy>,
    pub facet_fields: HashMap<String, Vec<String>>,
    pub scan_threads: HashMap<String, u32>,
    pub snapshot_interval_ms: HashMap<String, u64>,
    pub snapshot_retention: HashMap<String, Retention>,
    pub blob_reference_attributes: HashMap<String, Vec<String>>,
    pub namespace_store_refs: HashMap<String, String>,
    pub embedding_profiles: HashMap<String, EmbeddingProfile>,
    pub display: HashMap<String, IndexDisplay>,
}

/// Presentation metadata from `Index.spec.metadata.{title,description}`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IndexDisplay {
    pub title: Option<String>,
    pub description: Option<String>,
}

/// Declarative coverage. Resolution uses the current upstream schema, never a row scan.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FieldStatsPolicy {
    #[serde(default)]
    pub fields: StatsFields,
    #[serde(default)]
    pub exclude: Vec<String>,
    #[serde(default = "default_stats_cap")]
    pub value_cap: usize,
    #[serde(default = "default_stats_interval")]
    pub interval: String,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum StatsFields {
    Auto(String),
    List(Vec<String>),
}
impl Default for StatsFields {
    fn default() -> Self {
        Self::Auto("auto".into())
    }
}
fn default_stats_cap() -> usize {
    10_000
}
fn default_stats_interval() -> String {
    "5m".into()
}
impl Default for FieldStatsPolicy {
    fn default() -> Self {
        Self {
            fields: StatsFields::default(),
            exclude: vec![],
            value_cap: default_stats_cap(),
            interval: default_stats_interval(),
        }
    }
}
impl FieldStatsPolicy {
    pub fn validate(&self) -> Result<(), String> {
        if matches!(&self.fields, StatsFields::Auto(v) if v != "auto") {
            return Err("fields must be auto or a list".into());
        }
        if self.value_cap == 0 || self.value_cap > u32::MAX as usize {
            return Err("valueCap must be positive".into());
        }
        if humantime::parse_duration(&self.interval)
            .map_err(|e| e.to_string())?
            .as_millis()
            == 0
        {
            return Err("interval must be positive".into());
        }
        let fields = match &self.fields {
            StatsFields::List(v) => v.as_slice(),
            _ => &[],
        };
        if fields
            .iter()
            .chain(self.exclude.iter())
            .any(|v| v.trim().is_empty() || v != v.trim() || v.starts_with("_hevlayer_"))
        {
            return Err("field names must be nonempty, trimmed and not reserved".into());
        }
        Ok(())
    }
    pub fn resolve(&self, filterable: &HashSet<String>) -> Vec<String> {
        let mut fields: Vec<String> = match &self.fields {
            StatsFields::Auto(_) => filterable.iter().cloned().collect(),
            StatsFields::List(fields) => fields
                .iter()
                .filter(|f| filterable.contains(*f))
                .cloned()
                .collect(),
        };
        fields.retain(|f| !self.exclude.contains(f) && !f.starts_with("_hevlayer_"));
        fields.sort();
        fields.dedup();
        fields
    }
    pub fn interval_ms(&self) -> u64 {
        humantime::parse_duration(&self.interval)
            .expect("validated policy")
            .as_millis()
            .min(u64::MAX as u128) as u64
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingProfile {
    pub model: String,
    pub output_dim: u32,
    pub distance_metric: String,
    pub normalization: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Retention {
    Never,
    After(Duration),
}

pub struct StaticIndexConfigSource {
    facet_fields: HashMap<String, Vec<String>>,
}

impl StaticIndexConfigSource {
    pub fn new(facet_fields: HashMap<String, Vec<String>>) -> Self {
        Self { facet_fields }
    }
}

#[async_trait]
impl IndexConfigSource for StaticIndexConfigSource {
    async fn load_index_config(&self) -> Result<IndexConfig, IndexConfigError> {
        let mut normalized = HashMap::new();
        for (namespace, fields) in &self.facet_fields {
            let index = format!("LAYER_FACET_FIELDS[{namespace}]");
            let fields = normalize_facet_field_strings(&index, fields.iter().map(String::as_str))?;
            if !fields.is_empty() {
                normalized.insert(namespace.clone(), fields);
            }
        }
        Ok(IndexConfig {
            facet_fields: normalized,
            field_stats: HashMap::new(),
            scan_threads: HashMap::new(),
            snapshot_interval_ms: HashMap::new(),
            snapshot_retention: HashMap::new(),
            blob_reference_attributes: HashMap::new(),
            namespace_store_refs: HashMap::new(),
            embedding_profiles: HashMap::new(),
            display: HashMap::new(),
        })
    }
}

// Kubernetes Index config loading is pro-only and is not included in the public mirror.

pub async fn refresh_facet_fields_once(
    state: &AppState,
    source: &dyn IndexConfigSource,
) -> Result<(), IndexConfigError> {
    refresh_index_config_once(state, source, false).await
}

pub async fn refresh_index_config_once(
    state: &AppState,
    source: &dyn IndexConfigSource,
    preserve_facet_fields: bool,
) -> Result<(), IndexConfigError> {
    let config = source.load_index_config().await?;
    let namespaces: Vec<String> = config
        .facet_fields
        .keys()
        .chain(config.field_stats.keys())
        .filter(|namespace| !vectorstore_core::namespace_pattern::is_pattern(namespace))
        .cloned()
        .collect();
    if !preserve_facet_fields {
        state.replace_facet_fields(config.facet_fields);
    }
    *state
        .field_stats_policy
        .write()
        .unwrap_or_else(|p| p.into_inner()) = config.field_stats;
    state.replace_scan_threads(config.scan_threads);
    state.replace_snapshot_interval_ms(config.snapshot_interval_ms);
    state.replace_snapshot_retention(config.snapshot_retention);
    state.replace_blob_reference_attributes(config.blob_reference_attributes);
    state.replace_namespace_store_refs(config.namespace_store_refs);
    state.replace_embedding_profiles(config.embedding_profiles);
    state.replace_index_display(config.display);
    if let Err(error) = crate::snapshot_policy::apply_persisted_snapshot_policies(state).await {
        warn!(
            error = %error,
            "API snapshot policy refresh failed; keeping Index CR snapshot policy only"
        );
    }
    for namespace in &namespaces {
        state.consistency.register(namespace);
    }
    Ok(())
}

pub async fn run_facet_refresh_loop(
    state: Arc<AppState>,
    source: Arc<dyn IndexConfigSource>,
    interval: Duration,
) {
    run_index_config_refresh_loop(state, source, interval, false).await
}

pub async fn run_index_config_refresh_loop(
    state: Arc<AppState>,
    source: Arc<dyn IndexConfigSource>,
    interval: Duration,
    preserve_facet_fields: bool,
) {
    loop {
        match refresh_index_config_once(&state, source.as_ref(), preserve_facet_fields).await {
            Ok(()) => {
                let namespace_count = state.facet_field_namespaces().len();
                let scan_thread_namespaces = state
                    .scan_threads
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .len();
                let snapshot_interval_namespaces = state
                    .snapshot_interval_ms
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .len();
                let snapshot_retention_namespaces = state
                    .snapshot_retention
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .len();
                let blob_reference_namespaces = state
                    .blob_reference_attributes
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .len();
                let store_ref_namespaces = state
                    .namespace_store_refs
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .len();
                let embedding_profile_namespaces = state
                    .embedding_profiles
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .len();
                info!(
                    namespace_count,
                    scan_thread_namespaces,
                    snapshot_interval_namespaces,
                    snapshot_retention_namespaces,
                    blob_reference_namespaces,
                    store_ref_namespaces,
                    embedding_profile_namespaces,
                    "Index config refreshed"
                );
            }
            Err(error) => {
                warn!(
                    error = %error,
                    "Index facet field config refresh failed; keeping last good map"
                );
            }
        }
        tokio::time::sleep(interval).await;
    }
}

// Kubernetes Index parsing helpers are pro-only and are not included in the public mirror.

fn normalize_facet_field_strings<'a, I>(
    index_name: &str,
    raw_fields: I,
) -> Result<Vec<String>, IndexConfigError>
where
    I: IntoIterator<Item = &'a str>,
{
    normalize_field_strings(index_name, "spec.snapshot.facetFields", raw_fields)
}

fn normalize_field_strings<'a, I>(
    index_name: &str,
    field_path: &str,
    raw_fields: I,
) -> Result<Vec<String>, IndexConfigError>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut fields = Vec::new();
    let mut seen = HashSet::new();
    for raw in raw_fields {
        let field = raw.trim();
        if field.is_empty() {
            return Err(IndexConfigError::InvalidIndex {
                index: index_name.to_string(),
                message: format!("{field_path} must not contain empty strings"),
            });
        }
        if seen.insert(field.to_string()) {
            fields.push(field.to_string());
        }
    }

    Ok(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_stats_auto_list_exclude_off_and_schema_changes() {
        let schema = HashSet::from(["a".into(), "b".into()]);
        let mut policy = FieldStatsPolicy::default();
        assert_eq!(policy.value_cap, 10000);
        assert_eq!(policy.interval_ms(), 300000);
        assert_eq!(policy.resolve(&schema), vec!["a", "b"]);
        policy.exclude = vec!["a".into()];
        assert_eq!(policy.resolve(&schema), vec!["b"]);
        policy.fields = StatsFields::List(vec!["a".into(), "c".into()]);
        assert!(policy.resolve(&schema).is_empty());
        let evolved = HashSet::from(["b".into(), "c".into()]);
        assert_eq!(policy.resolve(&evolved), vec!["c"]);
        policy.fields = StatsFields::List(vec![]);
        assert!(policy.resolve(&evolved).is_empty());
    }
    #[test]
    fn field_stats_rejects_invalid_contracts() {
        for value in [
            serde_json::json!({"fields":"all"}),
            serde_json::json!({"valueCap":0}),
            serde_json::json!({"interval":"0s"}),
            serde_json::json!({"interval":"soon"}),
            serde_json::json!({"fields":[""]}),
            serde_json::json!({"exclude":["_hevlayer_private"]}),
        ] {
            assert!(serde_json::from_value::<FieldStatsPolicy>(value)
                .unwrap()
                .validate()
                .is_err());
        }
        assert!(
            serde_json::from_value::<FieldStatsPolicy>(serde_json::json!({"fields":42})).is_err()
        );
    }

    #[tokio::test]
    async fn static_source_normalizes_field_lists() {
        let source = StaticIndexConfigSource::new(HashMap::from([(
            "products".to_string(),
            vec![
                " category ".to_string(),
                "brand".to_string(),
                "category".to_string(),
            ],
        )]));

        let fields = source.load_facet_fields().await.unwrap();
        assert_eq!(
            fields.get("products"),
            Some(&vec!["category".to_string(), "brand".to_string()])
        );
    }
}

