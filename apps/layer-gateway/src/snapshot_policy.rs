use std::collections::HashSet;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::AppError;
use crate::index_config::Retention;
use crate::AppState;

const SNAPSHOT_POLICY_PREFIX: &str = "snapshot-policies";

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotPolicy {
    #[serde(default = "default_snapshot_interval")]
    pub interval: String,
    #[serde(default = "default_snapshot_retention")]
    pub retention: String,
    #[serde(default)]
    pub facet_fields: Vec<String>,
}

pub fn default_snapshot_interval() -> String {
    "5m".to_string()
}

pub fn default_snapshot_retention() -> String {
    "never".to_string()
}

fn snapshot_policy_key(namespace: &str) -> String {
    format!("{SNAPSHOT_POLICY_PREFIX}/{namespace}.json")
}

pub async fn get_snapshot_policy(
    state: &AppState,
    namespace: &str,
) -> Result<Option<SnapshotPolicy>, AppError> {
    let Some(body) = state
        .s3
        .get(&snapshot_policy_key(namespace))
        .await
        .map_err(|err| AppError::Upstream(format!("s3 get snapshot policy: {err}")))?
    else {
        return Ok(None);
    };
    let policy = serde_json::from_slice(&body)
        .map_err(|err| AppError::Upstream(format!("decode snapshot policy: {err}")))?;
    Ok(Some(policy))
}

pub async fn put_snapshot_policy(
    state: &AppState,
    namespace: &str,
    policy: SnapshotPolicy,
) -> Result<SnapshotPolicy, AppError> {
    let applied = validate_snapshot_policy(namespace, policy)?;
    let body = serde_json::to_vec_pretty(&applied)
        .map_err(|err| AppError::Upstream(format!("encode snapshot policy: {err}")))?;
    state
        .s3
        .put(&snapshot_policy_key(namespace), body)
        .await
        .map_err(|err| AppError::from_s3(err, "s3 put snapshot policy"))?;
    apply_snapshot_policy(state, namespace, &applied)?;
    Ok(applied)
}

pub async fn apply_persisted_snapshot_policies(state: &AppState) -> Result<usize, AppError> {
    let keys = state
        .s3
        .list_keys(&format!("{SNAPSHOT_POLICY_PREFIX}/"))
        .await
        .map_err(|err| AppError::Upstream(format!("s3 list snapshot policies: {err}")))?;
    let mut applied = 0;
    for key in keys {
        let Some(namespace) = key
            .strip_prefix(&format!("{SNAPSHOT_POLICY_PREFIX}/"))
            .and_then(|suffix| suffix.strip_suffix(".json"))
            .filter(|namespace| !namespace.is_empty())
        else {
            continue;
        };
        if let Some(policy) = get_snapshot_policy(state, namespace).await? {
            apply_snapshot_policy(state, namespace, &policy)?;
            applied += 1;
        }
    }
    Ok(applied)
}

pub fn apply_snapshot_policy(
    state: &AppState,
    namespace: &str,
    policy: &SnapshotPolicy,
) -> Result<(), AppError> {
    let facet_fields = normalize_facet_fields(&policy.facet_fields)?;
    let interval_ms = parse_duration_ms(namespace, "spec.snapshot.interval", &policy.interval)?;
    let retention = parse_retention(namespace, &policy.retention)?;

    {
        let mut fields = state
            .facet_fields
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if facet_fields.is_empty() {
            fields.remove(namespace);
        } else {
            fields.insert(namespace.to_string(), facet_fields);
        }
    }
    {
        let mut intervals = state
            .snapshot_interval_ms
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        intervals.insert(namespace.to_string(), interval_ms);
    }
    {
        let mut retentions = state
            .snapshot_retention
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        retentions.insert(namespace.to_string(), retention);
    }
    if !policy.facet_fields.is_empty() {
        state.consistency.register(namespace);
    }
    Ok(())
}

fn validate_snapshot_policy(
    namespace: &str,
    policy: SnapshotPolicy,
) -> Result<SnapshotPolicy, AppError> {
    let facet_fields = normalize_facet_fields(&policy.facet_fields)?;
    let interval = policy.interval.trim();
    if interval.is_empty() {
        return Err(AppError::Validation(
            "spec.snapshot.interval must be a duration string".to_string(),
        ));
    }
    parse_duration_ms(namespace, "spec.snapshot.interval", interval)?;

    let retention = policy.retention.trim();
    if retention.is_empty() {
        return Err(AppError::Validation(
            "spec.snapshot.retention must be 'never' or a duration string".to_string(),
        ));
    }
    parse_retention(namespace, retention)?;

    Ok(SnapshotPolicy {
        interval: interval.to_string(),
        retention: retention.to_string(),
        facet_fields,
    })
}

fn normalize_facet_fields(raw_fields: &[String]) -> Result<Vec<String>, AppError> {
    let mut fields = Vec::new();
    let mut seen = HashSet::new();
    for raw in raw_fields {
        let field = raw.trim();
        if field.is_empty() {
            return Err(AppError::Validation(
                "spec.snapshot.facetFields must not contain empty strings".to_string(),
            ));
        }
        if seen.insert(field.to_string()) {
            fields.push(field.to_string());
        }
    }
    Ok(fields)
}

fn parse_duration_ms(namespace: &str, field: &str, raw: &str) -> Result<u64, AppError> {
    let duration = humantime::parse_duration(raw.trim()).map_err(|err| {
        AppError::Validation(format!(
            "{field} for namespace '{namespace}' must be valid: {err}"
        ))
    })?;
    let millis = duration.as_millis();
    if millis > u64::MAX as u128 {
        return Err(AppError::Validation(format!(
            "{field} for namespace '{namespace}' is too large"
        )));
    }
    Ok(millis as u64)
}

fn parse_retention(namespace: &str, raw: &str) -> Result<Retention, AppError> {
    let value = raw.trim();
    if value.eq_ignore_ascii_case("never") {
        return Ok(Retention::Never);
    }
    Ok(Retention::After(Duration::from_millis(parse_duration_ms(
        namespace,
        "spec.snapshot.retention",
        value,
    )?)))
}
