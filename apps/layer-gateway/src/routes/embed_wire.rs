//! Validation and routing for Turbopuffer-compatible native embeddings.
//!
//! The native leg is deliberately transparent: after Layer consumes its
//! `serving` extension, the tpuf-compatible `embed` / `Embed` wire is sent to
//! Turbopuffer unchanged. The autoscaler leg is kept behind a clear service
//! error until a production gateway embedding provider exists (#385).

use serde_json::{Map, Value};

use crate::error::AppError;

const AUTOSCALER_UNAVAILABLE: &str = "embedding serving with `prefer: autoscaler` requires a production gateway inference provider; tracking issue: hev/layer-pro#385";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServingPreference {
    Native,
    Autoscaler,
    Blended,
}

/// Validate schema-attribute embedding and consume Layer's serving policy.
/// Returns true when the request contains at least one active `embed` field.
pub(crate) fn prepare_write(body: &mut Value, search_store: bool) -> Result<bool, AppError> {
    let Some(body) = body.as_object_mut() else {
        return Ok(false);
    };
    let Some(schema) = body.get_mut("schema").and_then(Value::as_object_mut) else {
        return Ok(false);
    };

    let mut has_embed = false;
    for (attribute, config) in schema {
        let Some(config) = config.as_object_mut() else {
            continue;
        };
        let Some(embed) = config.get_mut("embed") else {
            continue;
        };
        if embed.is_null() {
            continue;
        }
        has_embed = true;
        let preference = validate_embed(attribute, embed)?;
        ensure_serving_available(preference, search_store)?;
    }

    Ok(has_embed)
}

pub(crate) fn write_needs_distance_metric(body: &Value, has_embed: bool) -> bool {
    let Some(body) = body.as_object() else {
        return false;
    };
    has_embed && has_row_write(body) && !body.contains_key("distance_metric")
}

pub(crate) fn metadata_has_embed_schema(metadata: &Value) -> bool {
    metadata
        .get("schema")
        .and_then(Value::as_object)
        .is_some_and(|schema| {
            schema.values().any(|attribute| {
                attribute
                    .as_object()
                    .and_then(|attribute| attribute.get("embed"))
                    .is_some_and(|embed| !embed.is_null())
            })
        })
}

/// Validate every query leg containing an `Embed` vector source. Returns true
/// when at least one expression is present, so callers can preserve the
/// upstream response verbatim instead of rebuilding it through Layer's
/// portable query response.
pub(crate) fn prepare_query(body: &Value, search_store: bool) -> Result<bool, AppError> {
    let mut found = false;
    if let Some(rank_by) = body.get("rank_by") {
        found |= validate_rank_by(rank_by, search_store)?;
    }
    if let Some(queries) = body.get("queries").and_then(Value::as_array) {
        for query in queries {
            if let Some(rank_by) = query.get("rank_by") {
                found |= validate_rank_by(rank_by, search_store)?;
            }
        }
    }
    Ok(found)
}

fn validate_embed(attribute: &str, embed: &mut Value) -> Result<ServingPreference, AppError> {
    match embed {
        Value::String(model) => {
            validate_model(model)?;
            Ok(ServingPreference::Native)
        }
        Value::Object(options) => validate_embed_options(attribute, options),
        _ => Err(AppError::Validation(format!(
            "schema attribute `{attribute}` must set `embed` to a provider-namespaced model string, an options object, or null"
        ))),
    }
}

fn validate_embed_options(
    attribute: &str,
    options: &mut Map<String, Value>,
) -> Result<ServingPreference, AppError> {
    let model = options
        .get("model")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            AppError::Validation(format!(
                "schema attribute `{attribute}` extended `embed` form requires a string `model`"
            ))
        })?;
    validate_model(model)?;

    if let Some(dims) = options.get("dims") {
        if dims.as_u64().is_none_or(|dims| dims == 0) {
            return Err(AppError::Validation(format!(
                "schema attribute `{attribute}` `embed.dims` must be a positive integer"
            )));
        }
    }
    if let Some(target) = options.get("attribute") {
        if target.as_str().is_none_or(str::is_empty) {
            return Err(AppError::Validation(format!(
                "schema attribute `{attribute}` `embed.attribute` must be a non-empty string"
            )));
        }
    }

    for extension in ["revision", "instructions", "chunk", "modality"] {
        if options.contains_key(extension) {
            return Err(AppError::ServiceUnavailable(format!(
                "`embed.{extension}` requires Layer-served embedding; {AUTOSCALER_UNAVAILABLE}"
            )));
        }
    }

    let preference = parse_serving_preference(options.get("serving"), attribute)?;
    // `serving` is a Layer policy extension, not part of tpuf's native wire.
    // Consume it before transparently forwarding the compatible fields.
    options.remove("serving");
    Ok(preference)
}

fn parse_serving_preference(
    serving: Option<&Value>,
    attribute: &str,
) -> Result<ServingPreference, AppError> {
    let Some(serving) = serving else {
        return Ok(ServingPreference::Native);
    };
    let prefer = serving
        .as_object()
        .and_then(|serving| serving.get("prefer"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            AppError::Validation(format!(
                "schema attribute `{attribute}` `embed.serving` requires `prefer: native`, `prefer: autoscaler`, or `prefer: blended`"
            ))
        })?;
    match prefer {
        "native" => Ok(ServingPreference::Native),
        "autoscaler" => Ok(ServingPreference::Autoscaler),
        "blended" => Ok(ServingPreference::Blended),
        _ => Err(AppError::Validation(format!(
            "schema attribute `{attribute}` has unsupported `embed.serving.prefer` value `{prefer}`"
        ))),
    }
}

fn ensure_serving_available(
    preference: ServingPreference,
    search_store: bool,
) -> Result<(), AppError> {
    match (preference, search_store) {
        (ServingPreference::Native, false) => Ok(()),
        (ServingPreference::Native, true) => Err(AppError::ServiceUnavailable(format!(
            "`prefer: native` is not served by the search store, and its autoscaler fallback is unavailable; {AUTOSCALER_UNAVAILABLE}"
        ))),
        (ServingPreference::Autoscaler, _) => Err(AppError::ServiceUnavailable(
            AUTOSCALER_UNAVAILABLE.to_string(),
        )),
        (ServingPreference::Blended, _) => Err(AppError::ServiceUnavailable(format!(
            "`prefer: blended` requires the autoscaler handoff; {AUTOSCALER_UNAVAILABLE}"
        ))),
    }
}

fn validate_rank_by(rank_by: &Value, search_store: bool) -> Result<bool, AppError> {
    let Some(rank_by) = rank_by.as_array() else {
        return Ok(false);
    };
    let Some(embed) = rank_by.get(2).and_then(Value::as_array) else {
        return Ok(false);
    };
    if embed.first().and_then(Value::as_str) != Some("Embed") {
        return Ok(false);
    }

    if embed.len() != 2 && embed.len() != 3 {
        return Err(AppError::Validation(
            "`Embed` must be `[\"Embed\", text]` or `[\"Embed\", text, {model}]`".to_string(),
        ));
    }
    if embed.get(1).and_then(Value::as_str).is_none() {
        return Err(AppError::Validation(
            "`Embed` text must be a string".to_string(),
        ));
    }

    let explicit_model = match embed.get(2) {
        Some(options) => {
            let model = options
                .as_object()
                .and_then(|options| options.get("model"))
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    AppError::Validation("`Embed` options require a string `model`".to_string())
                })?;
            validate_model(model)?;
            true
        }
        None => false,
    };

    let target = rank_by.first().and_then(Value::as_str).unwrap_or_default();
    if target.starts_with("embed_") && !explicit_model {
        return Err(AppError::Validation(
            "a model name must be provided".to_string(),
        ));
    }

    ensure_serving_available(ServingPreference::Native, search_store)?;
    Ok(true)
}

fn validate_model(model: &str) -> Result<(), AppError> {
    let valid = model
        .split_once('/')
        .is_some_and(|(provider, name)| !provider.is_empty() && !name.is_empty())
        && !model.chars().any(char::is_whitespace);
    if valid {
        Ok(())
    } else {
        Err(AppError::Validation(format!(
            "embedding model `{model}` must be provider-namespaced (for example `voyage/voyage-4-lite`)"
        )))
    }
}

fn has_row_write(body: &Map<String, Value>) -> bool {
    ["upsert_rows", "upsert_columns"]
        .iter()
        .any(|key| body.get(*key).is_some_and(non_empty_collection))
}

fn non_empty_collection(value: &Value) -> bool {
    value.as_array().is_some_and(|value| !value.is_empty())
        || value.as_object().is_some_and(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn native_serving_is_consumed_but_tpuf_fields_remain() {
        let mut body = json!({
            "distance_metric": "cosine_distance",
            "upsert_rows": [{"id": 1, "title": "hello"}],
            "schema": {"title": {"type": "string", "embed": {
                "model": "voyage/voyage-4-lite",
                "dims": 512,
                "attribute": "title_vector",
                "serving": {"prefer": "native"}
            }}}
        });

        assert!(prepare_write(&mut body, false).unwrap());
        assert_eq!(
            body["schema"]["title"]["embed"],
            json!({
                "model": "voyage/voyage-4-lite",
                "dims": 512,
                "attribute": "title_vector"
            })
        );
    }

    #[test]
    fn derived_column_requires_explicit_model() {
        let error = prepare_query(
            &json!({"rank_by": ["embed_title", "ANN", ["Embed", "query"]]}),
            false,
        )
        .unwrap_err();
        assert!(
            matches!(error, AppError::Validation(message) if message == "a model name must be provided")
        );
    }

    #[test]
    fn source_column_infers_model() {
        assert!(prepare_query(
            &json!({"rank_by": ["title", "ANN", ["Embed", "query"]]}),
            false,
        )
        .unwrap());
    }

    #[test]
    fn existing_embed_schema_is_detected_from_metadata() {
        assert!(metadata_has_embed_schema(&json!({
            "schema": {
                "title": {"type": "string", "embed": "voyage/voyage-4-lite"}
            }
        })));
        assert!(!metadata_has_embed_schema(&json!({
            "schema": {"title": {"type": "string"}}
        })));
    }
}
