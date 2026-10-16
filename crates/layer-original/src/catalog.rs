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
        self.schema == 1
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
            && (self.mime == "application/pdf"
                || (self.mime == "image/png" && self.source_format_mismatch == Some(true)))
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
            source_format_mismatch: None,
            name: "test.pdf".into(),
            path: "/test.pdf".into(),
            web_url: "https://example.test/test.pdf".into(),
        }
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
}
