//! Pure review of retained source metadata and persisted embedding evidence.
//! This does not fetch evidence, certify its provenance, or authorize a scan.
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub struct ReviewedSearchProfile {
    namespace: String,
    projection: Vec<String>,
    dimensions: usize,
    model: String,
    distance_metric: String,
    full_text: BTreeMap<String, Value>,
    binding: String,
    evidence_reference: String,
    persisted_profiles: Value,
    retained_profile_bytes: Option<Vec<u8>>,
}

/// Explicit review references from guarded durable retrieval, never defaults.
#[derive(Debug, Clone)]
pub struct PersistedProfileAssociation {
    pub store_ref: String,
    pub writer_domain: String,
    pub account_evidence: String,
    pub object_version: String,
}

/// Dimension comes from the source vector type, never a caller/default/row.
pub fn vector_dimensions(metadata: &Value) -> Result<usize, String> {
    metadata
        .pointer("/schema/vector/type")
        .and_then(Value::as_str)
        .and_then(|ty| ty.strip_prefix('['))
        .and_then(|ty| ty.strip_suffix("]f32"))
        .and_then(|size| size.parse::<usize>().ok())
        .filter(|size| (1..=8192).contains(size))
        .ok_or_else(|| "source vector dimensions unavailable".into())
}

impl ReviewedSearchProfile {
    /// `persisted_profiles` must be bytes retained from the source's durable
    /// profile store, not a reconstructed Kit/Index default. The caller owns
    /// source/account association, receipt guards, and evidence authenticity.
    /// Bind the complete metadata/profile/projection so changed options cannot
    /// silently reuse a previously reviewed contract or payload calibration.
    pub fn from_source(
        namespace: &str,
        metadata: &Value,
        projection: &[String],
        persisted_profiles: Option<&Value>,
        evidence_reference: &str,
    ) -> Result<Self, String> {
        if namespace.trim().is_empty()
            || evidence_reference.trim().is_empty()
            || projection.len() > 64
            || projection
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != projection.len()
            || !projection.iter().any(|f| f == "vector")
            || projection.iter().any(|f| f.trim().is_empty())
        {
            return Err("explicit source/projection/profile evidence required".into());
        }
        if serde_json::to_vec(metadata)
            .map_err(|_| "invalid metadata")?
            .len()
            > 1024 * 1024
            || persisted_profiles
                .map(serde_json::to_vec)
                .transpose()
                .map_err(|_| "invalid profiles")?
                .is_some_and(|bytes| bytes.len() > 256 * 1024)
        {
            return Err("search evidence size limit exceeded".into());
        }
        let schema = metadata
            .get("schema")
            .and_then(Value::as_object)
            .ok_or("source schema unavailable")?;
        let dimensions = vector_dimensions(metadata)?;
        let distance_metric = metadata
            .get("distance_metric")
            .and_then(Value::as_str)
            .filter(|metric| ["cosine_distance", "euclidean_squared"].contains(metric))
            .ok_or("source distance metric unavailable/unsupported")?;
        let profiles = persisted_profiles
            .and_then(Value::as_array)
            .ok_or("persisted embedding profiles unavailable")?;
        let matching: Vec<_> = profiles
            .iter()
            .filter(|p| p.get("target").and_then(Value::as_str) == Some("vector"))
            .collect();
        if matching.len() != 1 {
            return Err("exactly one persisted vector profile required".into());
        }
        let profile = matching[0];
        let model = profile
            .get("model")
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
            .ok_or("persisted model unavailable")?;
        let source = profile
            .get("source")
            .and_then(Value::as_str)
            .filter(|s| projection.iter().any(|f| f == s))
            .ok_or("embedding source absent from projection")?;
        let source_schema = schema
            .get(source)
            .and_then(Value::as_object)
            .ok_or("embedding source schema unavailable")?;
        if source_schema.get("type").and_then(Value::as_str) != Some("string")
            || !source_schema
                .get("full_text_search")
                .is_some_and(Value::is_object)
        {
            return Err("mandatory string text/full-text source unavailable".into());
        }
        if profile.get("dims").and_then(Value::as_u64) != u64::try_from(dimensions).ok() {
            return Err("persisted/source dimensions mismatch or unavailable".into());
        }
        if let Some(metric) = profile.get("distance_metric") {
            if metric.as_str() != Some(distance_metric) {
                return Err("persisted/source distance mismatch".into());
            }
        }
        if let Some(embed) = source_schema.get("embed") {
            if embed.get("model").and_then(Value::as_str) != Some(model)
                || embed.get("attribute").and_then(Value::as_str) != Some("vector")
                || embed
                    .get("dims")
                    .is_some_and(|dims| dims.as_u64() != u64::try_from(dimensions).ok())
            {
                return Err("source embedding declaration/profile mismatch".into());
            }
        }
        let mut full_text = BTreeMap::new();
        for field in projection {
            if field == "vector" || field == "id" {
                continue;
            }
            let definition = schema
                .get(field)
                .and_then(Value::as_object)
                .ok_or("projected field schema unavailable")?;
            if let Some(options) = definition.get("full_text_search") {
                if options == &Value::Bool(false) {
                    continue;
                }
                // A bare true relies on unrecorded provider defaults. Explicit
                // source options are retained verbatim, including tokenizer.
                if !options.is_object() || options.as_object().is_some_and(|o| o.is_empty()) {
                    return Err("explicit full-text profile options unavailable".into());
                }
                let tokenizer = options
                    .get("tokenizer")
                    .ok_or("source tokenizer unavailable")?;
                if !tokenizer.as_str().is_some_and(|s| !s.trim().is_empty())
                    && !tokenizer
                        .get("type")
                        .and_then(Value::as_str)
                        .is_some_and(|s| !s.trim().is_empty())
                {
                    return Err("source tokenizer unavailable".into());
                }
                full_text.insert(field.clone(), options.clone());
            }
        }
        let binding_bytes = serde_json::to_vec(&(
            namespace,
            metadata,
            projection,
            profiles,
            evidence_reference,
        ))
        .map_err(|_| "search profile serialization failed")?;
        Ok(Self {
            namespace: namespace.into(),
            projection: projection.to_vec(),
            dimensions,
            model: model.into(),
            distance_metric: distance_metric.into(),
            full_text,
            binding: format!("{:x}", Sha256::digest(binding_bytes)),
            evidence_reference: evidence_reference.into(),
            persisted_profiles: Value::Array(profiles.clone()),
            retained_profile_bytes: None,
        })
    }
    /// Byte-bound validation for guarded durable retrieval. The caller supplies
    /// actual store/domain/account association and persisted object version.
    /// Neither these references nor successful parsing certify provenance.
    pub fn from_retained_bytes(
        namespace: &str,
        metadata_bytes: &[u8],
        projection: &[String],
        profile_bytes: Option<&[u8]>,
        evidence: &PersistedProfileAssociation,
    ) -> Result<Self, String> {
        let profile_bytes = profile_bytes.ok_or("persisted embedding profiles unavailable")?;
        if metadata_bytes.len() > 1024 * 1024 || profile_bytes.len() > 256 * 1024 {
            return Err("search evidence size limit exceeded".into());
        }
        if [
            &evidence.store_ref,
            &evidence.writer_domain,
            &evidence.account_evidence,
            &evidence.object_version,
        ]
        .iter()
        .any(|s| s.trim().is_empty())
        {
            return Err("durable profile source association unavailable".into());
        }
        let metadata: Value =
            serde_json::from_slice(metadata_bytes).map_err(|_| "invalid retained metadata")?;
        let profiles: Value =
            serde_json::from_slice(profile_bytes).map_err(|_| "invalid retained profiles")?;
        let reference = serde_json::to_string(&(
            &evidence.store_ref,
            &evidence.writer_domain,
            &evidence.account_evidence,
            &evidence.object_version,
        ))
        .map_err(|_| "invalid evidence association")?;
        let mut reviewed = Self::from_source(
            namespace,
            &metadata,
            projection,
            Some(&profiles),
            &reference,
        )?;
        // Bind original bytes, including all retained profile extension fields.
        let binding = serde_json::to_vec(&(reviewed.binding(), metadata_bytes, profile_bytes))
            .map_err(|_| "invalid retained binding")?;
        reviewed.binding = format!("{:x}", Sha256::digest(binding));
        reviewed.retained_profile_bytes = Some(profile_bytes.to_vec());
        Ok(reviewed)
    }
    pub fn matches_retained_bytes(
        &self,
        namespace: &str,
        metadata_bytes: &[u8],
        projection: &[String],
        profile_bytes: Option<&[u8]>,
        evidence: &PersistedProfileAssociation,
    ) -> Result<bool, String> {
        Ok(Self::from_retained_bytes(
            namespace,
            metadata_bytes,
            projection,
            profile_bytes,
            evidence,
        )?
        .binding
            == self.binding)
    }
    pub fn persisted_profiles(&self) -> &Value {
        &self.persisted_profiles
    }
    pub fn retained_profile_bytes(&self) -> Option<&[u8]> {
        self.retained_profile_bytes.as_deref()
    }

    /// Recheck the exact retained inputs after await/publication boundaries.
    /// A matching hash is input association, not provider change detection.
    pub fn matches_source(
        &self,
        namespace: &str,
        metadata: &Value,
        projection: &[String],
        persisted_profiles: Option<&Value>,
        evidence_reference: &str,
    ) -> Result<bool, String> {
        Ok(Self::from_source(
            namespace,
            metadata,
            projection,
            persisted_profiles,
            evidence_reference,
        )?
        .binding
            == self.binding)
    }
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
    pub fn projection(&self) -> &[String] {
        &self.projection
    }
    pub fn dimensions(&self) -> usize {
        self.dimensions
    }
    pub fn model(&self) -> &str {
        &self.model
    }
    pub fn distance_metric(&self) -> &str {
        &self.distance_metric
    }
    pub fn full_text(&self) -> &BTreeMap<String, Value> {
        &self.full_text
    }
    pub fn binding(&self) -> &str {
        &self.binding
    }
    pub fn evidence_reference(&self) -> &str {
        &self.evidence_reference
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn inputs() -> (Value, Vec<String>, Value) {
        (
            serde_json::json!({"distance_metric":"cosine_distance","schema":{
                "vector":{"type":"[2]f32"},"text":{"type":"string","full_text_search":{"tokenizer":"word","language":"english"},"embed":{"model":"source-model","attribute":"vector","dims":2}}
            }}),
            vec!["text".into(), "vector".into()],
            serde_json::json!([{"source":"text","target":"vector","model":"source-model","dims":2,"revision":"retained-r1"}]),
        )
    }
    #[test]
    fn retained_evidence_binds_all_profile_options_and_projection() {
        let (metadata, projection, profiles) = inputs();
        let first = ReviewedSearchProfile::from_source(
            "ns",
            &metadata,
            &projection,
            Some(&profiles),
            "s3-object-version",
        )
        .unwrap();
        assert!(first
            .matches_source(
                "ns",
                &metadata,
                &projection,
                Some(&profiles),
                "s3-object-version"
            )
            .unwrap());
        assert!(!first
            .matches_source(
                "other",
                &metadata,
                &projection,
                Some(&profiles),
                "s3-object-version"
            )
            .unwrap());
        assert!(!first
            .matches_source(
                "ns",
                &metadata,
                &projection,
                Some(&profiles),
                "different-object-version"
            )
            .unwrap());
        assert_eq!(first.dimensions(), 2);
        assert_eq!(first.model(), "source-model");
        assert_eq!(first.distance_metric(), "cosine_distance");
        assert_eq!(first.full_text()["text"]["tokenizer"], "word");
        let mut changed = profiles.clone();
        changed[0]["revision"] = "retained-r2".into();
        assert_ne!(
            first.binding(),
            ReviewedSearchProfile::from_source(
                "ns",
                &metadata,
                &projection,
                Some(&changed),
                "s3-object-version"
            )
            .unwrap()
            .binding()
        );
        let mut changed = metadata.clone();
        changed["schema"]["text"]["full_text_search"]["language"] = "german".into();
        assert_ne!(
            first.binding(),
            ReviewedSearchProfile::from_source(
                "ns",
                &changed,
                &projection,
                Some(&profiles),
                "s3-object-version"
            )
            .unwrap()
            .binding()
        );
        let mut reordered = projection.clone();
        reordered.reverse();
        assert_ne!(
            first.binding(),
            ReviewedSearchProfile::from_source(
                "ns",
                &metadata,
                &reordered,
                Some(&profiles),
                "s3-object-version"
            )
            .unwrap()
            .binding()
        );
    }
    #[test]
    fn absent_unknown_and_mismatched_source_evidence_refuses() {
        let (metadata, projection, profiles) = inputs();
        assert!(
            ReviewedSearchProfile::from_source("ns", &metadata, &projection, None, "ref").is_err()
        );
        assert!(ReviewedSearchProfile::from_source(
            "ns",
            &metadata,
            &projection,
            Some(&profiles),
            ""
        )
        .is_err());
        for path in [
            "/distance_metric",
            "/schema/vector/type",
            "/schema/text/embed/model",
            "/schema/text/full_text_search",
        ] {
            let mut changed = metadata.clone();
            *changed.pointer_mut(path).unwrap() = Value::Bool(true);
            assert!(ReviewedSearchProfile::from_source(
                "ns",
                &changed,
                &projection,
                Some(&profiles),
                "ref"
            )
            .is_err());
        }
        let mut changed = profiles.clone();
        changed[0]["dims"] = 3.into();
        assert!(ReviewedSearchProfile::from_source(
            "ns",
            &metadata,
            &projection,
            Some(&changed),
            "ref"
        )
        .is_err());
        let mut duplicate = profiles.clone();
        duplicate.as_array_mut().unwrap().push(profiles[0].clone());
        assert!(ReviewedSearchProfile::from_source(
            "ns",
            &metadata,
            &projection,
            Some(&duplicate),
            "ref"
        )
        .is_err());
    }
    #[test]
    fn consumer_limits_and_mandatory_text_profile_refuse() {
        let (mut metadata, projection, profiles) = inputs();
        for dimension in [0, 8193] {
            metadata["schema"]["vector"]["type"] = format!("[{dimension}]f32").into();
            assert!(ReviewedSearchProfile::from_source(
                "ns",
                &metadata,
                &projection,
                Some(&profiles),
                "ref"
            )
            .is_err());
        }
        let (mut metadata, projection, profiles) = inputs();
        metadata["schema"]["text"]["type"] = "[]string".into();
        assert!(ReviewedSearchProfile::from_source(
            "ns",
            &metadata,
            &projection,
            Some(&profiles),
            "ref"
        )
        .is_err());
        let (metadata, projection, profiles) = inputs();
        assert!(ReviewedSearchProfile::from_source(
            "ns",
            &metadata,
            &vec!["vector".into(); 65],
            Some(&profiles),
            "ref"
        )
        .is_err());
        let association = PersistedProfileAssociation {
            store_ref: "store".into(),
            writer_domain: "domain".into(),
            account_evidence: "account-receipt".into(),
            object_version: "s3-version".into(),
        };
        assert!(ReviewedSearchProfile::from_retained_bytes(
            "ns",
            &vec![b' '; 1024 * 1024 + 1],
            &projection,
            Some(b"[]"),
            &association
        )
        .is_err());
        assert!(ReviewedSearchProfile::from_retained_bytes(
            "ns",
            b"{}",
            &projection,
            Some(&vec![b' '; 256 * 1024 + 1]),
            &association
        )
        .is_err());
    }
    #[test]
    fn retained_profile_bytes_and_source_association_are_bound() {
        let (metadata, projection, mut profiles) = inputs();
        profiles[0]["artifact_sha256"] = "actual-artifact".into();
        profiles[0]["instructions"] = serde_json::json!({"query":"retained instruction"});
        let metadata = serde_json::to_vec(&metadata).unwrap();
        let profiles = serde_json::to_vec(&profiles).unwrap();
        let mut association = PersistedProfileAssociation {
            store_ref: "store".into(),
            writer_domain: "domain".into(),
            account_evidence: "account-receipt".into(),
            object_version: "s3-version".into(),
        };
        let first = ReviewedSearchProfile::from_retained_bytes(
            "ns",
            &metadata,
            &projection,
            Some(&profiles),
            &association,
        )
        .unwrap();
        assert_eq!(first.retained_profile_bytes(), Some(profiles.as_slice()));
        assert_eq!(
            first.persisted_profiles()[0]["artifact_sha256"],
            "actual-artifact"
        );
        assert!(first
            .matches_retained_bytes("ns", &metadata, &projection, Some(&profiles), &association)
            .unwrap());
        association.account_evidence = "different-account".into();
        assert!(!first
            .matches_retained_bytes("ns", &metadata, &projection, Some(&profiles), &association)
            .unwrap());
        association.object_version.clear();
        assert!(first
            .matches_retained_bytes("ns", &metadata, &projection, Some(&profiles), &association)
            .is_err());
        assert!(ReviewedSearchProfile::from_retained_bytes(
            "ns",
            &metadata,
            &projection,
            None,
            &association
        )
        .is_err());
    }
    #[test]
    fn dimension_and_projection_boundary_values_are_explicit() {
        let (mut metadata, mut projection, mut profiles) = inputs();
        for d in [1, 8192] {
            metadata["schema"]["vector"]["type"] = format!("[{d}]f32").into();
            metadata["schema"]["text"]["embed"]["dims"] = d.into();
            profiles[0]["dims"] = d.into();
            assert_eq!(
                ReviewedSearchProfile::from_source(
                    "ns",
                    &metadata,
                    &projection,
                    Some(&profiles),
                    "ref"
                )
                .unwrap()
                .dimensions(),
                d as usize
            );
        }
        for i in 0..62 {
            let field = format!("field{i}");
            metadata["schema"][&field] = serde_json::json!({"type":"int"});
            projection.push(field);
        }
        assert_eq!(projection.len(), 64);
        assert!(ReviewedSearchProfile::from_source(
            "ns",
            &metadata,
            &projection,
            Some(&profiles),
            "ref"
        )
        .is_ok());
        projection.push("text".into());
        assert!(ReviewedSearchProfile::from_source(
            "ns",
            &metadata,
            &projection,
            Some(&profiles),
            "ref"
        )
        .is_err());
    }
}
