//! Regenerate with scripts/generate-store-capabilities; CI checks the diff.
use serde_json::{json, Value};
use vectorstore_core::capabilities::{WireFeature, SCHEMA_FIELDS};
use vectorstore_core::pgvector_capabilities;
use vectorstore_core::search::HttpSearchClient;
use vectorstore_core::turbopuffer::{HttpTurbopufferClient, TurbopufferClient};

fn main() {
    // Fail closed when a typed query option is added without a coverage row.
    let spec: Value = serde_yaml::from_str(
        &std::fs::read_to_string("apps/layer-gateway/openapi.yaml")
            .expect("run from repository root"),
    )
    .expect("valid OpenAPI");
    for (schema, fields) in SCHEMA_FIELDS {
        let definition = spec["components"]["schemas"][schema]
            .as_object()
            .expect("request schema");
        let properties = definition
            .get("properties")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        for field in properties.keys() {
            assert!(
                fields.iter().any(|(name, _)| name == field),
                "{schema}.{field} needs a wire-feature declaration"
            );
        }
        for (field, _) in *fields {
            assert!(properties.contains_key(*field), "stale {schema}.{field}");
        }
    }
    let stores = [
        HttpTurbopufferClient::new("", "http://localhost").capabilities(),
        HttpSearchClient::new(None, "http://localhost").capabilities(),
        pgvector_capabilities::capabilities(),
    ];
    let features: Vec<Value> = WireFeature::ALL.iter().map(|&feature| {
        let coverage: serde_json::Map<String, Value> = stores.iter().map(|store| {
            (store.kind.to_string(), serde_json::to_value(store.get(feature)).expect("coverage serializes"))
        }).collect();
        json!({"id": feature.id(), "label": feature.label(), "page": feature.page(), "schema_fields": SCHEMA_FIELDS.iter().flat_map(|(schema, fields)| fields.iter().filter(move |(_, owner)| *owner == feature).map(move |(name, _)| format!("{schema}.{name}"))).collect::<Vec<_>>(), "stores": coverage})
    }).collect();
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "generated_by": "scripts/generate-store-capabilities",
            "fts_ranking_note": "BM25-class scoring; tokenization differs; fused order may differ across backends",
        "features": features
        }))
        .expect("artifact serializes")
    );
}
