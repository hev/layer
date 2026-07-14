use axum::extract::{Path, Query};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use metrics_catalog::{catalog, MetricDoc, MetricFamily};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::OnceLock;

const CATALOG_VERSION: &str = "1";
const CACHE_CONTROL: &str = "public, max-age=86400";

#[derive(Debug, Deserialize)]
pub struct CatalogQuery {
    pub family: Option<String>,
}

#[derive(Debug, Serialize)]
struct CatalogEnvelope<'a> {
    version: &'static str,
    entries: Vec<&'a MetricDoc>,
}

/// Returns the full catalog as JSON, sorted by metric name. Optional
/// `?family=` filter restricts the result server-side.
pub async fn list_catalog(Query(params): Query<CatalogQuery>) -> Response {
    let family = match params.family.as_deref() {
        None => None,
        Some(value) => match MetricFamily::parse(value) {
            Some(parsed) => Some(parsed),
            None => return unknown_family(value),
        },
    };

    let entries = sorted_catalog();
    let entries: Vec<&MetricDoc> = match family {
        None => entries.to_vec(),
        Some(family) => entries
            .iter()
            .copied()
            .filter(|doc| doc.family == family)
            .collect(),
    };

    let envelope = CatalogEnvelope {
        version: CATALOG_VERSION,
        entries,
    };
    json_with_cache(StatusCode::OK, &envelope)
}

/// Returns a single catalog entry by name. Convenience surface for
/// `layer explain <metric>`. 404 when unknown.
pub async fn get_catalog_entry(Path(name): Path<String>) -> Response {
    if let Some(doc) = sorted_catalog().iter().copied().find(|d| d.name == name) {
        json_with_cache(StatusCode::OK, doc)
    } else {
        let body = json!({
            "error": "not_found",
            "message": format!("metric {name:?} is not registered in the catalog"),
        });
        (StatusCode::NOT_FOUND, Json(body)).into_response()
    }
}

fn unknown_family(value: &str) -> Response {
    let body = json!({
        "error": "invalid_family",
        "message": format!("unknown metric family {value:?}"),
    });
    (StatusCode::BAD_REQUEST, Json(body)).into_response()
}

fn json_with_cache<T: Serialize>(status: StatusCode, body: &T) -> Response {
    let payload = match serde_json::to_vec(body) {
        Ok(payload) => payload,
        Err(err) => {
            let msg = format!("catalog encoding error: {err}");
            return (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response();
        }
    };
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(CACHE_CONTROL),
    );
    headers.insert(header::ETAG, catalog_etag().clone());
    (status, headers, payload).into_response()
}

fn sorted_catalog() -> &'static [&'static MetricDoc] {
    static SORTED: OnceLock<Vec<&'static MetricDoc>> = OnceLock::new();
    SORTED.get_or_init(|| {
        let mut docs: Vec<&'static MetricDoc> = catalog().iter().collect();
        docs.sort_by_key(|doc| doc.name);
        docs
    })
}

fn catalog_etag() -> &'static HeaderValue {
    static ETAG: OnceLock<HeaderValue> = OnceLock::new();
    ETAG.get_or_init(|| {
        let envelope = json!({
            "version": CATALOG_VERSION,
            "entries": sorted_catalog(),
        });
        let bytes: Value = envelope;
        let serialized = serde_json::to_vec(&bytes).expect("catalog serializes");
        let mut hasher = Sha256::new();
        hasher.update(&serialized);
        let digest = hasher.finalize();
        let hex: String = digest.iter().take(16).map(|b| format!("{b:02x}")).collect();
        HeaderValue::from_str(&format!("\"{hex}\"")).expect("etag is ascii")
    })
}
