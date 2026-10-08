//! A page row points at its retained original with `original_blob`
//! (`blob://<namespace>/<sha256>`) and `original_bytes`. The reference is
//! derived, never free-form: it is exactly the address of the row's own
//! indexed `original_sha256` in the row's own namespace. The pipeline writes it
//! at ingest, the backfill Function writes it for existing rows, and the
//! gateway re-derives it before serving a byte. All three use this crate.
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

pub const BLOB_ATTR: &str = "original_blob";
pub const BYTES_ATTR: &str = "original_bytes";
/// Written by the extract worker on every page row, before retention existed.
pub const SHA_ATTR: &str = "original_sha256";

/// Every page row has carried the SHA-256 of its original bytes as
/// `content_hash` since the first extract release, long before
/// `original_sha256` existed. Legacy rows identify their file by it.
pub const LEGACY_HASH_ATTR: &str = "content_hash";

/// Attributes a Function needs to resolve and verify an original.
pub const INPUT_ATTRS: [&str; 8] = [
    SHA_ATTR,
    "original_version",
    "original_etag",
    "original_item",
    "original_drive",
    "original_mime",
    BLOB_ATTR,
    BYTES_ATTR,
];

/// Candidate provider URLs for a legacy row: its `webUrl`, then every copy in
/// its `locations` (content-deduplicated documents list each copy). Bounded.
pub fn legacy_urls(attrs: &Value) -> Vec<String> {
    let mut urls = Vec::new();
    let mut add = |url: Option<&str>| {
        if let Some(url) = url.filter(|u| u.starts_with("https://")) {
            if !urls.iter().any(|seen| seen == url) && urls.len() < 5 {
                urls.push(url.to_owned());
            }
        }
    };
    add(attrs.get("webUrl").and_then(Value::as_str));
    let locations = match attrs.get("locations") {
        Some(Value::String(text)) => serde_json::from_str::<Value>(text).ok(),
        Some(other) => Some(other.clone()),
        None => None,
    };
    if let Some(Value::Array(list)) = locations {
        for location in &list {
            add(location.get("webUrl").and_then(Value::as_str));
        }
    }
    urls
}

/// A REST copy of a document, as `locations` records it (`source: "rest"`):
/// the Warehouse, the mapped row id and the row's mapped attributes, which is
/// everything the connector needs to rebuild the attachment request.
#[derive(Debug, Clone, PartialEq)]
pub struct RestCopy {
    pub warehouse: String,
    pub row_id: String,
    pub attributes: serde_json::Map<String, Value>,
}

/// The REST copies named by a legacy row's `locations`. Bounded.
pub fn legacy_rest_copies(attrs: &Value) -> Vec<RestCopy> {
    let locations = match attrs.get("locations") {
        Some(Value::String(text)) => serde_json::from_str::<Value>(text).ok(),
        Some(other) => Some(other.clone()),
        None => None,
    };
    let Some(Value::Array(list)) = locations else {
        return Vec::new();
    };
    let mut copies: Vec<RestCopy> = Vec::new();
    for location in &list {
        let rest = location.get("rest").unwrap_or(location);
        let (Some(warehouse), Some(row_id)) = (
            rest.get("warehouse").and_then(Value::as_str),
            rest.get("rowId").and_then(Value::as_str),
        ) else {
            continue;
        };
        let copy = RestCopy {
            warehouse: warehouse.to_owned(),
            row_id: row_id.to_owned(),
            attributes: rest
                .get("attributes")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default(),
        };
        if !copies
            .iter()
            .any(|c| c.warehouse == copy.warehouse && c.row_id == copy.row_id)
            && copies.len() < 5
        {
            copies.push(copy);
        }
    }
    copies
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// PDF readers accept up to 1 KiB of junk before `%PDF-`, and so does the REST
/// connector's own attachment check; a byte-identical copy of a file that was
/// indexed that way must not be refused for it. The hash is the real proof.
pub fn has_pdf_header(bytes: &[u8]) -> bool {
    bytes[..bytes.len().min(1024 + 5)]
        .windows(5)
        .any(|w| w == b"%PDF-")
}

pub fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

pub fn reference(namespace: &str, sha256: &str) -> String {
    format!("blob://{namespace}/{}", sha256.to_ascii_lowercase())
}

/// The attribute patch that records a retained original. Identical for the
/// pipeline's staged rows, the backfill's completion patch and `patch_rows`.
pub fn patch(namespace: &str, sha256: &str, bytes: u64) -> Map<String, Value> {
    let mut attrs = Map::new();
    attrs.insert(BLOB_ATTR.into(), json!(reference(namespace, sha256)));
    attrs.insert(BYTES_ATTR.into(), json!(bytes));
    attrs
}

/// What a row says its original is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub sha256: String,
    pub bytes: Option<u64>,
    pub mime: String,
    /// The provider version the row was indexed from (`original_version`).
    pub version: Option<String>,
    pub etag: Option<String>,
    pub item: Option<String>,
    pub drive: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Refusal {
    /// The row has no retained original reference.
    NotRetained,
    /// The row has no usable indexed hash, so nothing can be verified.
    NoHash,
    /// The stored reference is not the one derived from the row and namespace.
    NotBound,
    /// The bytes are not the document the row was indexed from.
    Mismatch(&'static str),
}

fn string(attrs: &Value, name: &str) -> Option<String> {
    attrs
        .get(name)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

impl Identity {
    /// The row's indexed identity, without requiring a retained reference.
    pub fn of_row(attrs: &Value) -> Result<Self, Refusal> {
        let sha256 = string(attrs, SHA_ATTR)
            .filter(|sha| is_sha256(sha))
            .map(|sha| sha.to_ascii_lowercase())
            .ok_or(Refusal::NoHash)?;
        Ok(Self {
            sha256,
            bytes: attrs.get(BYTES_ATTR).and_then(Value::as_u64),
            mime: string(attrs, "original_mime").unwrap_or_else(|| "application/pdf".into()),
            version: string(attrs, "original_version"),
            etag: string(attrs, "original_etag"),
            item: string(attrs, "original_item"),
            drive: string(attrs, "original_drive"),
        })
    }

    /// The identity of a row indexed before `original_*` existed: the exact
    /// bytes are named by `content_hash`; size is `source_bytes`. The provider
    /// item is not recorded on such rows, so it is resolved from the row's
    /// `webUrl` and proven by hashing what comes back.
    pub fn of_legacy_row(attrs: &Value) -> Result<Self, Refusal> {
        let sha256 = string(attrs, LEGACY_HASH_ATTR)
            .filter(|sha| is_sha256(sha))
            .map(|sha| sha.to_ascii_lowercase())
            .ok_or(Refusal::NoHash)?;
        Ok(Self {
            sha256,
            // Advisory pre-download gate only; 0 means "not recorded".
            bytes: attrs
                .get("source_bytes")
                .and_then(Value::as_u64)
                .filter(|n| *n > 0),
            // Unknown until the provider says; a PDF header is checked once it does.
            mime: "application/octet-stream".into(),
            version: None,
            etag: None,
            item: None,
            drive: None,
        })
    }

    /// Bind a retained reference to this row and namespace: it must be exactly
    /// the derived address, so another namespace's blob, other bytes, a bucket
    /// or a URL are refused.
    pub fn bound(namespace: &str, attrs: &Value) -> Result<Self, Refusal> {
        let Some(stored) = attrs.get(BLOB_ATTR).and_then(Value::as_str) else {
            return Err(Refusal::NotRetained);
        };
        let identity = Self::of_row(attrs)?;
        if stored != reference(namespace, &identity.sha256) {
            return Err(Refusal::NotBound);
        }
        Ok(identity)
    }

    /// Whether the row already carries exactly this retained original.
    pub fn is_retained_in(namespace: &str, attrs: &Value) -> bool {
        Self::bound(namespace, attrs).is_ok()
    }

    /// Bytes must hash to the indexed hash and, for a
    /// PDF, start like one.
    pub fn verify(&self, bytes: &[u8]) -> Result<(), Refusal> {
        if sha256_hex(bytes) != self.sha256 {
            return Err(Refusal::Mismatch("hash differs from the indexed original"));
        }
        // No size check here. Equal SHA-256 means equal length, and the recorded
        // size is not always right: a REST source lists size 0 until downloaded,
        // so `source_bytes` of a REST document can be wrong for byte-identical
        // bytes. `bytes` is only a cheap pre-download gate (Graph item size).
        if self.mime == "application/pdf" && !has_pdf_header(bytes) {
            return Err(Refusal::Mismatch("not a PDF"));
        }
        Ok(())
    }

    /// The provider copy the bytes came from must be the version the row was
    /// indexed from. Absent facts on either side are not a disagreement; a
    /// present one that differs is.
    pub fn check_source(
        &self,
        etag: Option<&str>,
        item: Option<&str>,
        drive: Option<&str>,
    ) -> Result<(), Refusal> {
        for (row, source, what) in [
            (&self.etag, etag, "etag differs from the indexed version"),
            (&self.item, item, "item differs from the indexed document"),
            (
                &self.drive,
                drive,
                "drive differs from the indexed document",
            ),
        ] {
            if let (Some(row), Some(source)) = (row, source) {
                if row != source {
                    return Err(Refusal::Mismatch(what));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(ns: &str, bytes: &[u8]) -> Value {
        let sha = sha256_hex(bytes);
        json!({"original_sha256": sha, "original_blob": reference(ns, &sha),
               "original_bytes": bytes.len(), "original_etag": "e1"})
    }

    #[test]
    fn one_reference_format_and_patch() {
        let sha = sha256_hex(b"%PDF-1");
        assert_eq!(
            reference("ns", &sha.to_uppercase()),
            format!("blob://ns/{sha}")
        );
        let p = patch("ns", &sha, 6);
        assert_eq!(p[BLOB_ATTR], format!("blob://ns/{sha}"));
        assert_eq!(p[BYTES_ATTR], 6);
        assert_eq!(p.len(), 2);
    }

    #[test]
    fn legacy_rows_are_identified_by_content_hash_and_candidate_urls() {
        let sha = sha256_hex(b"%PDF-1");
        let attrs = json!({"content_hash": sha, "source_bytes": 6,
            "webUrl": "https://a.example/x.pdf",
            "locations": "[{\"webUrl\":\"https://a.example/x.pdf\"},{\"webUrl\":\"https://b.example/y.pdf\"},{\"webUrl\":\"http://insecure\"}]"});
        let id = Identity::of_legacy_row(&attrs).unwrap();
        assert_eq!((id.sha256.as_str(), id.bytes), (sha.as_str(), Some(6)));
        assert!(id.verify(b"%PDF-1").is_ok());
        assert_eq!(
            legacy_urls(&attrs),
            ["https://a.example/x.pdf", "https://b.example/y.pdf"]
        );
        assert_eq!(
            Identity::of_legacy_row(&json!({"content_hash": "nope"})),
            Err(Refusal::NoHash)
        );
        assert!(legacy_urls(&json!({})).is_empty());
    }

    #[test]
    fn rest_copies_are_read_from_locations_with_their_row_identity() {
        let attrs = json!({"locations": serde_json::to_string(&json!([
            {"source": "clouddrive", "webUrl": "https://a.example/x.pdf"},
            {"source": "rest", "path": "bcc-ki/1.pdf", "mime": "application/pdf",
             "rest": {"warehouse": "bcc-ki", "rowId": "1", "url": "rest://bcc-ki/1.pdf",
                      "attributePrefix": "ki_", "attributes": {"has_document": true}}},
            {"source": "rest", "rest": {"warehouse": "bcc-ki", "rowId": "1", "attributes": {}}},
            {"source": "rest", "rest": {"warehouse": "bcc-ki"}}
        ])).unwrap()});
        let copies = legacy_rest_copies(&attrs);
        assert_eq!(copies.len(), 1);
        assert_eq!(
            (copies[0].warehouse.as_str(), copies[0].row_id.as_str()),
            ("bcc-ki", "1")
        );
        assert_eq!(copies[0].attributes["has_document"], true);
        assert!(legacy_rest_copies(&json!({})).is_empty());
    }

    #[test]
    fn binds_only_the_derived_address() {
        let attrs = row("docs", b"%PDF-1");
        assert!(Identity::bound("docs", &attrs).is_ok());
        assert_eq!(Identity::bound("other", &attrs), Err(Refusal::NotBound));
        let mut url = attrs.clone();
        url[BLOB_ATTR] = json!("s3://bucket/key");
        assert_eq!(Identity::bound("docs", &url), Err(Refusal::NotBound));
        assert_eq!(
            Identity::bound("docs", &json!({})),
            Err(Refusal::NotRetained)
        );
        let mut no_hash = attrs;
        no_hash.as_object_mut().unwrap().remove(SHA_ATTR);
        assert_eq!(Identity::bound("docs", &no_hash), Err(Refusal::NoHash));
    }

    #[test]
    fn a_pdf_header_may_follow_up_to_a_kib_of_junk() {
        assert!(has_pdf_header(b"%PDF-1.4"));
        assert!(has_pdf_header(b"\r\n  \xef\xbb\xbf%PDF-1.7 body"));
        let mut late = vec![b' '; 1100];
        late.extend_from_slice(b"%PDF-1.4");
        assert!(!has_pdf_header(&late));
        assert!(!has_pdf_header(b"<html>error</html>"));
        assert!(!has_pdf_header(b""));
        // The hash is still the proof: junk before the header passes the type
        // check only for bytes that hash to the indexed original.
        let junk: Vec<u8> = [b"\n".as_slice(), b"%PDF-1.4 x"].concat();
        let mut id = Identity::of_legacy_row(
            &json!({"content_hash": sha256_hex(&junk), "source_bytes": junk.len()}),
        )
        .unwrap();
        id.mime = "application/pdf".into();
        assert!(id.verify(&junk).is_ok());
        assert!(id.verify(b"\n%PDF-1.4 y").is_err());
    }

    #[test]
    fn a_recorded_size_never_overrides_an_equal_hash() {
        // A REST source lists size 0 until downloaded, so source_bytes can be 0
        // or stale for byte-identical bytes: the hash alone proves the file.
        let body = b"%PDF-1 identical";
        for recorded in [json!(0), json!(9999), Value::Null] {
            let attrs = json!({"content_hash": sha256_hex(body), "source_bytes": recorded});
            let id = Identity::of_legacy_row(&attrs).unwrap();
            assert!(id.verify(body).is_ok(), "{recorded}");
            assert!(id.verify(b"%PDF-1 different").is_err());
        }
        // A zero size is "not recorded", not a gate.
        let zero = json!({"content_hash": sha256_hex(body), "source_bytes": 0});
        assert_eq!(Identity::of_legacy_row(&zero).unwrap().bytes, None);
    }

    #[test]
    fn verifies_bytes_and_source_version() {
        let attrs = row("docs", b"%PDF-1");
        let id = Identity::bound("docs", &attrs).unwrap();
        assert!(id.verify(b"%PDF-1").is_ok());
        assert!(id.verify(b"%PDF-2").is_err());
        assert!(id.check_source(Some("e1"), None, None).is_ok());
        assert!(id.check_source(None, Some("x"), None).is_ok());
        assert!(id.check_source(Some("e2"), None, None).is_err());
    }
}
