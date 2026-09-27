//! Where a branch or copy came from (RFC 0124).
//!
//! The record lives in object storage next to the namespace's embedding
//! profiles, at `lineage/{namespace}.json`. It is provenance plus the one rule
//! that needs it: a branch's rows keep the source's `blob://{source}/…`
//! references, and those resolve because the source is in the branch's
//! lineage. Without an object store no lineage is recorded, and a branch only
//! resolves its own `blob://{namespace}/…` references.
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::error::AppError;
use crate::AppState;

const LINEAGE_PREFIX: &str = "lineage";

/// One ancestor: the namespace and when the target was cut from it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineageEntry {
    pub namespace: String,
    pub at: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lineage {
    /// The branch chain, nearest first: `wt-lyr-130 ← layer-pro-trunk ← …`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub branched_from: Vec<LineageEntry>,
    /// A same-store copy. Provenance only: a copy owns its bytes, but its
    /// rows still name the source in their blob references.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub copied_from: Option<LineageEntry>,
    /// Every namespace whose `blob://` references this namespace resolves,
    /// nearest first, excluding the namespace itself.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ancestors: Vec<String>,
}

impl Lineage {
    /// The lineage of a branch cut from `source` at `at`.
    pub fn branch_of(source: &str, source_lineage: &Lineage, at: &str) -> Self {
        let mut branched_from = vec![LineageEntry {
            namespace: source.to_string(),
            at: at.to_string(),
        }];
        branched_from.extend(source_lineage.branched_from.iter().cloned());
        Self {
            branched_from,
            copied_from: None,
            ancestors: Self::chain(source, source_lineage),
        }
    }

    /// The lineage of a same-store copy of `source` taken at `at`.
    pub fn copy_of(source: &str, source_lineage: &Lineage, at: &str) -> Self {
        Self {
            branched_from: Vec::new(),
            copied_from: Some(LineageEntry {
                namespace: source.to_string(),
                at: at.to_string(),
            }),
            ancestors: Self::chain(source, source_lineage),
        }
    }

    fn chain(source: &str, source_lineage: &Lineage) -> Vec<String> {
        let mut ancestors = vec![source.to_string()];
        for ancestor in &source_lineage.ancestors {
            if !ancestors.contains(ancestor) {
                ancestors.push(ancestor.clone());
            }
        }
        ancestors
    }
}

pub fn lineage_key(namespace: &str) -> String {
    format!("{LINEAGE_PREFIX}/{namespace}.json")
}

/// Read a namespace's lineage. Absent, unreadable or no object store all
/// mean "no ancestors": lineage only widens what a namespace accepts.
pub async fn read_lineage(state: &AppState, namespace: &str) -> Lineage {
    match state.s3.get(&lineage_key(namespace)).await {
        Ok(Some(body)) => serde_json::from_slice(&body).unwrap_or_else(|error| {
            warn!(namespace, %error, "invalid lineage object; treating namespace as unbranched");
            Lineage::default()
        }),
        Ok(None) => Lineage::default(),
        Err(error) => {
            warn!(namespace, %error, "lineage read failed; treating namespace as unbranched");
            Lineage::default()
        }
    }
}

/// Record `lineage` for `namespace`, replacing any residue under the name.
/// An empty lineage deletes the object.
pub async fn write_lineage(
    state: &AppState,
    namespace: &str,
    lineage: &Lineage,
) -> Result<(), AppError> {
    if !state.s3.is_configured() {
        debug!(
            namespace,
            "no object store configured; branch lineage not recorded"
        );
        return Ok(());
    }
    let key = lineage_key(namespace);
    if lineage == &Lineage::default() {
        return state
            .s3
            .delete_key(&key)
            .await
            .map_err(|error| AppError::Upstream(format!("delete lineage {key}: {error}")));
    }
    let body = serde_json::to_vec(lineage)
        .map_err(|error| AppError::Upstream(format!("serialize lineage: {error}")))?;
    state
        .s3
        .put(&key, body)
        .await
        .map_err(|error| AppError::Upstream(format!("persist lineage {key}: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_branch_of_a_branch_resolves_both_ancestors() {
        let trunk = Lineage::branch_of("root", &Lineage::default(), "t0");
        let branch = Lineage::branch_of("trunk", &trunk, "t1");
        assert_eq!(branch.ancestors, vec!["trunk", "root"]);
        assert_eq!(branch.branched_from.len(), 2);
        assert_eq!(branch.branched_from[0].namespace, "trunk");
        let copy = Lineage::copy_of("trunk", &trunk, "t2");
        assert_eq!(copy.ancestors, vec!["trunk", "root"]);
        assert!(copy.branched_from.is_empty());
        let json = serde_json::to_value(&branch).unwrap();
        assert!(json.get("copied_from").is_none());
    }
}
