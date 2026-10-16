//! Namespace-bound, source-addressed originals without search rows. Catalog
//! keys are always derived, never accepted as caller bucket/key authority.
use crate::{is_sha256, reference, sha256_hex};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
pub const PREFIX: &str = "original:";
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Entry {
    pub schema: u32,
    pub namespace: String,
    pub warehouse: String,
    pub drive: String,
    pub item: String,
    pub etag: String,
    pub version: Option<String>,
    pub sha256: String,
    pub bytes: u64,
    pub mime: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declared_mime: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format_validation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_format_mismatch: Option<bool>,
    pub name: String,
    pub path: String,
    pub web_url: String,
}
pub fn source_id(namespace: &str, warehouse: &str, drive: &str, item: &str) -> String {
    format!(
        "{PREFIX}{}",
        sha256_hex(
            &serde_json::to_vec(&(namespace, warehouse, drive, item)).expect("source identity")
        )
    )
}
pub fn source_key(namespace: &str, id: &str) -> Option<String> {
    let suffix = id.strip_prefix(PREFIX)?;
    is_sha256(suffix).then(|| format!("original-catalog/{namespace}/sources/{suffix}.json"))
}
/// Append-only established-corruption assessment, scoped to exact retained bytes.
pub fn corruption_key(namespace: &str, sha: &str) -> Option<String> {
    (!namespace.is_empty()
        && !namespace.contains('/')
        && !matches!(namespace, "." | "..")
        && is_sha256(sha))
    .then(|| {
        format!(
            "original-catalog/{namespace}/corruptions/{}.json",
            sha.to_ascii_lowercase()
        )
    })
}
pub fn hash_key(namespace: &str, sha: &str) -> Option<String> {
    is_sha256(sha).then(|| {
        format!(
            "original-catalog/{namespace}/hashes/{}.json",
            sha.to_ascii_lowercase()
        )
    })
}
pub fn version_key(namespace: &str, drive: &str, item: &str, version: &str, etag: bool) -> String {
    format!(
        "original-catalog/{namespace}/versions/{}.json",
        sha256_hex(&serde_json::to_vec(&(drive, item, version, etag)).expect("version identity"))
    )
}
impl Entry {
    pub fn id(&self) -> String {
        source_id(&self.namespace, &self.warehouse, &self.drive, &self.item)
    }
    pub fn validate(&self, namespace: &str) -> bool {
        let citation = url::Url::parse(&self.web_url).ok();
        ((self.schema == 1
            && self.format_validation.is_none()
            && (self.mime == "application/pdf"
                || (self.mime == "image/png" && self.source_format_mismatch == Some(true))))
            || (self.schema == 3
                && self.mime == "application/octet-stream"
                && self.format_validation.as_deref() == Some("unverified")
                && self.source_format_mismatch.is_none()
                && self
                    .version
                    .as_deref()
                    .is_some_and(|v| v.starts_with("quickXor:") && v.len() > 9)))
            && self.namespace == namespace
            && !namespace.is_empty()
            && !namespace.contains('/')
            && !matches!(namespace, "." | "..")
            && !self.warehouse.is_empty()
            && !self.drive.is_empty()
            && !self.item.is_empty()
            && !self.etag.is_empty()
            && is_sha256(&self.sha256)
            && self.bytes > 0
            && citation.is_some_and(|u| {
                u.scheme() == "https"
                    && u.username().is_empty()
                    && u.password().is_none()
                    && !u.query_pairs().any(|(k, _)| {
                        matches!(
                            k.to_ascii_lowercase().as_str(),
                            "access_token" | "token" | "authkey" | "sig" | "tempauth"
                        )
                    })
            })
    }
    pub fn attrs(&self) -> Value {
        let mut attrs = Map::new();
        attrs.insert("name".into(), json!(self.name));
        attrs.insert("path".into(), json!(self.path));
        attrs.insert("webUrl".into(), json!(self.web_url));
        attrs.insert("original_sha256".into(), json!(self.sha256));
        attrs.insert(
            "original_blob".into(),
            json!(reference(&self.namespace, &self.sha256)),
        );
        attrs.insert("original_bytes".into(), json!(self.bytes));
        attrs.insert("original_mime".into(), json!(self.mime));
        if let Some(validation) = &self.format_validation {
            attrs.insert("original_format_validation".into(), json!(validation));
        }
        if let Some(mime) = &self.declared_mime {
            attrs.insert("original_declared_mime".into(), json!(mime));
        }
        if let Some(mismatch) = self.source_format_mismatch {
            attrs.insert("original_source_format_mismatch".into(), json!(mismatch));
        }
        attrs.insert("original_drive".into(), json!(self.drive));
        attrs.insert("original_item".into(), json!(self.item));
        attrs.insert("original_etag".into(), json!(self.etag));
        attrs.insert("original_version".into(), json!(self.version));
        Value::Object(attrs)
    }
    /// The authorized row must name exact bytes, or an exact source version.
    /// Missing/unknown legacy identity is not a license to use a current source.
    pub fn matches_row(&self, attrs: &Value) -> bool {
        if let Some(sha) = row_hash(attrs) {
            return sha.eq_ignore_ascii_case(&self.sha256);
        }
        if attrs["original_drive"].as_str() != Some(&self.drive)
            || attrs["original_item"].as_str() != Some(&self.item)
        {
            return false;
        }
        let etag = attrs
            .get("original_etag")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty());
        if etag.is_some_and(|etag| etag != self.etag) {
            return false;
        }
        match attrs
            .get("original_version")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        {
            Some(version) => self.version.as_deref() == Some(version),
            None => etag.is_some(),
        }
    }
}
pub fn row_hash(attrs: &Value) -> Option<&str> {
    ["original_sha256", "content_hash"]
        .into_iter()
        .find_map(|key| {
            attrs
                .get(key)
                .and_then(Value::as_str)
                .filter(|s| is_sha256(s))
        })
}
pub fn row_key(namespace: &str, attrs: &Value) -> Option<String> {
    if let Some(sha) = row_hash(attrs) {
        return hash_key(namespace, sha);
    }
    let drive = attrs
        .get("original_drive")?
        .as_str()
        .filter(|s| !s.is_empty())?;
    let item = attrs
        .get("original_item")?
        .as_str()
        .filter(|s| !s.is_empty())?;
    // Prefer the provider revision when present, rather than a content-hash
    // version alias shared by multiple provider revisions of the same file.
    if let Some(etag) = attrs
        .get("original_etag")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        return Some(version_key(namespace, drive, item, etag, true));
    }
    if let Some(version) = attrs
        .get("original_version")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        Some(version_key(namespace, drive, item, version, false))
    } else {
        let etag = attrs
            .get("original_etag")?
            .as_str()
            .filter(|s| !s.is_empty())?;
        Some(version_key(namespace, drive, item, etag, true))
    }
}
/// Schema 2 is deliberately separate: schema 1 readers must reject it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Snapshot {
    pub schema: u32,
    pub original: Entry,
    pub warehouse_origin: String,
    pub inventory_binding: String,
    pub inventory_generation: String,
    pub provider_checksum: String,
    pub verification_receipt: String,
    pub observations: [UnavailableObservation; 2],
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UnavailableObservation {
    pub status: u16,
    pub observed_unix_seconds: u64,
}
impl Snapshot {
    /// JSON array tuple, versioned discriminator, fixed field order, UTF-8.
    /// No timestamp, attempt ID, mutable alias or caller-provided key authority.
    pub fn id(&self) -> String {
        format!(
            "{PREFIX}{}",
            sha256_hex(
                &serde_json::to_vec(&(
                    "layer-original-snapshot-v2",
                    &self.original.namespace,
                    &self.warehouse_origin,
                    &self.inventory_binding,
                    &self.inventory_generation,
                    &self.original.drive,
                    &self.original.item,
                    &self.original.etag,
                    &self.original.version,
                    &self.provider_checksum,
                    self.original.bytes,
                    &self.original.sha256,
                ))
                .expect("snapshot identity")
            )
        )
    }
    pub fn validate(&self, namespace: &str) -> bool {
        self.schema == 2
            && self.original.validate(namespace)
            && is_sha256(&self.warehouse_origin)
            && is_sha256(&self.inventory_binding)
            && !self.inventory_generation.is_empty()
            && !self.provider_checksum.is_empty()
            && self.original.version.as_deref()
                == Some(&format!("quickXor:{}", self.provider_checksum))
            && is_sha256(&self.verification_receipt)
            && self
                .observations
                .iter()
                .all(|o| matches!(o.status, 404 | 410) && o.observed_unix_seconds > 0)
            && self.observations[0].observed_unix_seconds
                <= self.observations[1].observed_unix_seconds
    }
    /// A concurrent winner may have earlier observations, but cannot change
    /// frozen metadata, proof identity or source binding.
    pub fn same_snapshot(&self, other: &Self) -> bool {
        let mut comparable = other.clone();
        comparable.observations = self.observations.clone();
        comparable.verification_receipt = self.verification_receipt.clone();
        comparable == *self
    }
    pub fn attrs(&self) -> Value {
        let mut attrs = self.original.attrs();
        attrs["original_retention_basis"] = json!("inventorySnapshot");
        attrs["original_source_availability"] = json!("unavailable");
        attrs["original_source_observations"] = json!(self.observations);
        attrs["original_deletion_status"] = json!("unknown");
        attrs["original_citation_basis"] = json!("inventoryObservation");
        attrs
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn entry() -> Entry {
        Entry {
            schema: 1,
            namespace: "docs".into(),
            warehouse: "source".into(),
            drive: "d".into(),
            item: "i".into(),
            etag: "e".into(),
            version: Some("v".into()),
            sha256: "a".repeat(64),
            bytes: 5,
            mime: "application/pdf".into(),
            declared_mime: None,
            format_validation: None,
            source_format_mismatch: None,
            name: "test.pdf".into(),
            path: "/test.pdf".into(),
            web_url: "https://example.test/test.pdf".into(),
        }
    }
    #[test]
    fn unclassified_schema_is_explicit_fail_closed_and_does_not_invent_mismatch() {
        let mut e = entry();
        e.schema = 3;
        e.mime = "application/octet-stream".into();
        e.format_validation = Some("unverified".into());
        e.source_format_mismatch = None;
        e.version = Some("quickXor:provider".into());
        assert!(e.validate("docs"));
        assert!(!e.validate("other"));
        assert_eq!(e.attrs()["original_format_validation"], "unverified");
        assert!(e.attrs()["original_source_format_mismatch"].is_null());
        let mut old = e.clone();
        old.schema = 1;
        assert!(!old.validate("docs"));
        let mut lie = e.clone();
        lie.source_format_mismatch = Some(false);
        assert!(!lie.validate("docs"));
        let mut missing = e.clone();
        missing.format_validation = None;
        assert!(!missing.validate("docs"));
    }
    #[test]
    fn typed_catalog_preserves_actual_and_declared_mime_and_reads_legacy_v1() {
        let old = entry();
        let mut legacy = serde_json::to_value(&old).unwrap();
        legacy.as_object_mut().unwrap().remove("declaredMime");
        legacy
            .as_object_mut()
            .unwrap()
            .remove("sourceFormatMismatch");
        let old: Entry = serde_json::from_value(legacy).unwrap();
        assert!(old.validate("docs"));
        assert!(old.declared_mime.is_none());
        let mut typed = old;
        typed.mime = "image/png".into();
        assert!(!typed.validate("docs"));
        typed.declared_mime = Some("application/pdf".into());
        typed.source_format_mismatch = Some(true);
        assert!(typed.validate("docs"));
        let attrs = typed.attrs();
        assert_eq!(attrs["original_mime"], "image/png");
        assert_eq!(attrs["original_declared_mime"], "application/pdf");
        assert_eq!(attrs["original_source_format_mismatch"], true);
        assert_eq!(attrs["webUrl"], typed.web_url);
        typed.mime = "text/html".into();
        assert!(!typed.validate("docs"));
    }
    #[test]
    fn catalog_ids_are_namespace_bound_and_rows_need_exact_hash_or_source_version() {
        let e = entry();
        assert!(e.validate("docs"));
        assert!(!e.validate("other"));
        assert_ne!(e.id(), source_id("other", &e.warehouse, &e.drive, &e.item));
        assert!(source_key("docs", "original:../other").is_none());
        assert!(e.matches_row(&json!({"content_hash":"a".repeat(64)})));
        assert!(!e.matches_row(&json!({"content_hash":"b".repeat(64)})));
        let source = json!({"original_drive":"d","original_item":"i","original_version":"v"});
        assert!(e.matches_row(&source));
        assert_eq!(
            row_key("docs", &source),
            Some(version_key("docs", "d", "i", "v", false))
        );
        let revision = json!({"original_drive":"d","original_item":"i","original_version":"v","original_etag":"e"});
        assert!(e.matches_row(&revision));
        assert_eq!(
            row_key("docs", &revision),
            Some(version_key("docs", "d", "i", "e", true))
        );
        let old = json!({"original_drive":"d","original_item":"i","original_version":"v","original_etag":"older"});
        assert!(!e.matches_row(&old));
        assert!(!e.matches_row(&json!({"original_drive":"d","original_item":"i"})));
        assert!(row_key("docs", &json!({"webUrl":"https://example.test/test.pdf"})).is_none());
    }
    #[test]
    fn snapshot_id_is_canonical_versioned_and_excludes_observation_times_but_binds_origin_revision()
    {
        let mut original = entry();
        original.version = Some("quickXor:checksum".into());
        let snapshot = Snapshot {
            schema: 2,
            original,
            warehouse_origin: "b".repeat(64),
            inventory_binding: "c".repeat(64),
            inventory_generation: "generation".into(),
            provider_checksum: "checksum".into(),
            verification_receipt: "d".repeat(64),
            observations: [
                UnavailableObservation {
                    status: 404,
                    observed_unix_seconds: 1,
                },
                UnavailableObservation {
                    status: 410,
                    observed_unix_seconds: 2,
                },
            ],
        };
        assert!(snapshot.validate("docs"));
        assert!(!snapshot.validate("other"));
        let mut later = snapshot.clone();
        later.observations[1].observed_unix_seconds = 3;
        later.verification_receipt = "e".repeat(64);
        assert_eq!(later.id(), snapshot.id());
        assert!(snapshot.same_snapshot(&later));
        for field in [
            "warehouse",
            "binding",
            "generation",
            "etag",
            "version",
            "bytes",
            "sha",
            "drive",
            "item",
            "namespace",
        ] {
            let mut changed = snapshot.clone();
            match field {
                "warehouse" => changed.warehouse_origin = "a".repeat(64),
                "binding" => changed.inventory_binding = "a".repeat(64),
                "generation" => changed.inventory_generation = "other".into(),
                "etag" => changed.original.etag = "other".into(),
                "version" => changed.original.version = Some("other".into()),
                "bytes" => changed.original.bytes += 1,
                "sha" => changed.original.sha256 = "b".repeat(64),
                "drive" => changed.original.drive = "other".into(),
                "item" => changed.original.item = "other".into(),
                "namespace" => changed.original.namespace = "other".into(),
                _ => unreachable!(),
            }
            assert_ne!(changed.id(), snapshot.id(), "{field}");
        }
        assert!(serde_json::from_slice::<Entry>(&serde_json::to_vec(&snapshot).unwrap()).is_err());
    }
}
