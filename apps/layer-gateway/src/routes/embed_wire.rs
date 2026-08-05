//! Validation and routing for Turbopuffer-compatible native embeddings.
//!
//! Native requests remain transparent on Turbopuffer stores. Autoscaler
//! requests (and native requests targeting hev search) are resolved through
//! the gateway's Turbopuffer-native provider and lowered to concrete vectors,
//! so Layer-only serving policy and `embed` / `Embed` are never forwarded.

use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use crate::embedding::{EmbeddingModality, EmbeddingRequest};
use crate::error::AppError;
use crate::AppState;

const PROFILE_PREFIX: &str = "embedding-profiles";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ServingPreference {
    Native,
    Autoscaler,
}

impl ServingPreference {
    fn label(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Autoscaler => "autoscaler",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbeddingProfile {
    source: String,
    target: String,
    model: String,
    dims: Option<u64>,
    serving: ServingPreference,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    revision: Option<String>,
    #[serde(default)]
    instructions: EmbeddingInstructions,
    #[serde(default)]
    modality: EmbeddingModality,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    chunk: Option<ChunkConfig>,
    #[serde(default)]
    layer_extensions: bool,
    #[serde(default)]
    materialized: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct EmbeddingInstructions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    document: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    query: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
struct ChunkConfig {
    strategy: String,
    #[serde(default)]
    unit: Option<String>,
    #[serde(default)]
    size: Option<usize>,
    #[serde(default)]
    overlap: Option<usize>,
    #[serde(default)]
    tokenizer: Option<String>,
    #[serde(default)]
    section_source: Option<String>,
    #[serde(default)]
    fields: Vec<String>,
    #[serde(default)]
    section_attribute: Option<String>,
    #[serde(default)]
    split: Option<Box<ChunkConfig>>,
}

impl EmbeddingProfile {
    fn has_extensions(&self) -> bool {
        self.layer_extensions
            || self.revision.is_some()
            || self.instructions != EmbeddingInstructions::default()
            || self.modality != EmbeddingModality::Text
            || self.chunk.is_some()
    }
}

#[derive(Debug, Default)]
pub(crate) struct WritePreparation {
    pub requires_distance_check: bool,
    pub performance: Value,
    pub generated_chunk_attributes: bool,
    profiles_to_save: Option<Vec<EmbeddingProfile>>,
    profile_persistence_required: bool,
}

#[derive(Debug, Default)]
pub(crate) struct QueryPreparation {
    pub found: bool,
    pub passthrough: bool,
    pub performance: Value,
}

/// Validate schema-attribute embedding, persist Layer-served profiles, and
/// lower any row writes to concrete vectors.
pub(crate) async fn prepare_write(
    state: &AppState,
    namespace: &str,
    body: &mut Value,
    search_store: bool,
) -> Result<WritePreparation, AppError> {
    let mut profiles = load_profiles(state, namespace).await?;
    let row_write = has_row_write(body.as_object());
    let had_persisted_profiles = profiles
        .iter()
        .any(|profile| profile.serving != ServingPreference::Native || profile.materialized);
    let mut profile_changed = false;
    let mut has_embed_schema = false;
    let mut native_embed_schema = false;

    if let Some(schema) = body.get_mut("schema").and_then(Value::as_object_mut) {
        let attributes = schema.keys().cloned().collect::<Vec<_>>();
        for attribute in attributes {
            let Some(config) = schema.get_mut(&attribute).and_then(Value::as_object_mut) else {
                continue;
            };
            let Some(embed) = config.get_mut("embed") else {
                continue;
            };
            if embed.is_null() {
                let previous = profiles.len();
                profiles.retain(|profile| profile.source != attribute);
                profile_changed |= profiles.len() != previous;
                if profiles.len() != previous {
                    config.remove("embed");
                }
                continue;
            }
            has_embed_schema = true;
            let mut parsed = validate_embed(&attribute, embed)?;
            let previous = profiles
                .iter()
                .find(|profile| profile.source == attribute)
                .cloned();
            if let Some(previous) = previous.as_ref() {
                parsed.materialized = previous.materialized;
            }
            let gateway_served = parsed.serving == ServingPreference::Autoscaler || search_store;
            if gateway_served {
                if previous.as_ref().is_some_and(|previous| {
                    previous.source == parsed.source
                        && previous.target == parsed.target
                        && previous.model == parsed.model
                        && previous.dims == parsed.dims
                        && previous.serving == parsed.serving
                        && previous.revision == parsed.revision
                        && previous.instructions == parsed.instructions
                        && previous.modality == parsed.modality
                        && previous.chunk == parsed.chunk
                        && previous.layer_extensions == parsed.layer_extensions
                }) {
                    parsed.materialized = previous
                        .as_ref()
                        .is_some_and(|previous| previous.materialized);
                }
                profiles.retain(|profile| profile.source != attribute);
                profiles.push(parsed.clone());
                profile_changed |= previous.as_ref() != Some(&parsed);
                config.remove("embed");
            } else {
                native_embed_schema = true;
                profiles.retain(|profile| profile.source != attribute);
                profiles.push(parsed.clone());
                profile_changed |= previous.as_ref() != Some(&parsed);
                consume_serving(embed);
            }
            if parsed.has_extensions() {
                if parsed.serving == ServingPreference::Native {
                    return Err(AppError::Validation(format!(
                        "schema attribute `{attribute}` Layer embedding extensions require `embed.serving.prefer` to be `autoscaler`"
                    )));
                }
                if state.embedding_provider.is_none() {
                    return Err(AppError::ServiceUnavailable(
                        "Layer embedding extensions require a configured production autoscaler inference provider"
                            .to_string(),
                    ));
                }
            }
        }
    }

    for profile in &profiles {
        state.metrics.remember_embed_model(
            namespace,
            &profile.source,
            &profile.target,
            &profile.model,
        );
        state.metrics.remember_embed_serving(
            namespace,
            &profile.source,
            &profile.target,
            profile.serving.label(),
        );
    }

    let gateway_profiles = profiles
        .iter()
        .filter(|profile| profile.serving == ServingPreference::Autoscaler || search_store)
        .collect::<Vec<_>>();

    reject_source_patches(body, &gateway_profiles)?;

    if gateway_profiles.is_empty() || !row_write {
        let requires_distance_check = native_embed_schema
            && has_embed_schema
            && profiles.iter().any(|profile| !profile.materialized);
        let materialized_changed =
            row_write && profiles.iter().any(|profile| !profile.materialized);
        if row_write {
            for profile in &mut profiles {
                profile.materialized = true;
            }
        }
        return Ok(WritePreparation {
            requires_distance_check,
            performance: json!({}),
            profiles_to_save: (profile_changed || materialized_changed).then_some(profiles.clone()),
            profile_persistence_required: had_persisted_profiles
                || profiles
                    .iter()
                    .any(|profile| profile.serving != ServingPreference::Native)
                || (profile_changed && search_store),
            generated_chunk_attributes: false,
        });
    }
    if search_store && gateway_profiles.len() > 1 {
        return Err(AppError::Validation(
            "hev search currently supports one autoscaler-served embedding attribute per namespace"
                .to_string(),
        ));
    }
    if gateway_profiles
        .iter()
        .any(|profile| profile.chunk.is_some())
        && gateway_profiles.len() > 1
    {
        return Err(AppError::Validation(
            "chunked embedding currently supports one autoscaler-served embedding attribute per namespace"
                .to_string(),
        ));
    }

    let mut performance = json!({});
    for profile in gateway_profiles {
        let inputs = prepare_write_inputs(body, profile)?;
        if inputs.values.is_empty() {
            continue;
        }
        let values = inputs
            .values
            .iter()
            .map(|value| apply_instruction(profile.instructions.document.as_deref(), value))
            .collect::<Vec<_>>();
        let vectors = resolve_vectors(
            state,
            namespace,
            profile,
            profile.modality,
            &values,
            &mut performance,
        )
        .await?;
        apply_write_vectors(body, profile, &inputs.row_indices, &vectors, search_store)?;
    }

    let requires_distance_check = native_embed_schema
        || profiles.iter().any(|profile| {
            (profile.serving == ServingPreference::Autoscaler || search_store)
                && !profile.materialized
        });
    let materialized_changed = profiles.iter().any(|profile| {
        (profile.serving == ServingPreference::Autoscaler || search_store) && !profile.materialized
    });
    for profile in &mut profiles {
        if profile.serving == ServingPreference::Autoscaler || search_store {
            profile.materialized = true;
        }
    }
    let generated_chunk_attributes = profiles.iter().any(|profile| profile.chunk.is_some());

    Ok(WritePreparation {
        requires_distance_check,
        performance,
        profiles_to_save: (profile_changed || materialized_changed).then_some(profiles),
        profile_persistence_required: true,
        generated_chunk_attributes,
    })
}

pub(crate) fn write_needs_distance_metric(body: &Value, requires_distance_check: bool) -> bool {
    let Some(body) = body.as_object() else {
        return false;
    };
    requires_distance_check && has_row_write(Some(body)) && !body.contains_key("distance_metric")
}

pub(crate) async fn commit_profiles(
    state: &AppState,
    namespace: &str,
    preparation: &WritePreparation,
) -> Result<(), AppError> {
    if let Some(profiles) = preparation.profiles_to_save.as_deref() {
        if preparation.profile_persistence_required {
            save_profiles(state, namespace, profiles).await?;
        } else {
            state
                .wire_embedding_profiles
                .insert(namespace.to_string(), profiles.to_vec());
        }
    }
    Ok(())
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

/// Validate and lower every query leg containing an `Embed` vector source.
pub(crate) async fn prepare_query(
    state: &AppState,
    namespace: &str,
    body: &mut Value,
    search_store: bool,
) -> Result<QueryPreparation, AppError> {
    if !contains_embed_expression(body) {
        return Ok(QueryPreparation::default());
    }
    let profiles = load_profiles(state, namespace).await?;
    let mut preparation = QueryPreparation {
        passthrough: true,
        performance: json!({}),
        ..QueryPreparation::default()
    };
    if let Some(rank_by) = body.get_mut("rank_by") {
        prepare_rank_by(
            state,
            namespace,
            rank_by,
            search_store,
            &profiles,
            &mut preparation,
        )
        .await?;
    }
    if let Some(queries) = body.get_mut("queries").and_then(Value::as_array_mut) {
        for query in queries {
            if let Some(rank_by) = query.get_mut("rank_by") {
                prepare_rank_by(
                    state,
                    namespace,
                    rank_by,
                    search_store,
                    &profiles,
                    &mut preparation,
                )
                .await?;
            }
        }
    }
    Ok(preparation)
}

fn contains_embed_expression(body: &Value) -> bool {
    let rank_by_has_embed = |rank_by: &Value| {
        rank_by
            .as_array()
            .and_then(|rank_by| rank_by.get(2))
            .and_then(Value::as_array)
            .and_then(|embed| embed.first())
            .and_then(Value::as_str)
            == Some("Embed")
    };
    body.get("rank_by").is_some_and(rank_by_has_embed)
        || body
            .get("queries")
            .and_then(Value::as_array)
            .is_some_and(|queries| {
                queries
                    .iter()
                    .any(|query| query.get("rank_by").is_some_and(rank_by_has_embed))
            })
}

async fn prepare_rank_by(
    state: &AppState,
    namespace: &str,
    rank_by: &mut Value,
    search_store: bool,
    profiles: &[EmbeddingProfile],
    preparation: &mut QueryPreparation,
) -> Result<(), AppError> {
    let Some(rank_by) = rank_by.as_array_mut() else {
        return Ok(());
    };
    let Some(embed) = rank_by.get(2).and_then(Value::as_array) else {
        return Ok(());
    };
    if embed.first().and_then(Value::as_str) != Some("Embed") {
        return Ok(());
    }
    preparation.found = true;
    validate_embed_expression(embed)?;

    let target = rank_by.first().and_then(Value::as_str).unwrap_or_default();
    let explicit_model = embed
        .get(2)
        .and_then(Value::as_object)
        .and_then(|options| options.get("model"))
        .and_then(Value::as_str);
    if let Some(model) = explicit_model {
        validate_model(model)?;
    }
    let declared = profiles
        .iter()
        .find(|profile| profile.source == target || profile.target == target);
    if let Some(profile) = declared {
        state.metrics.remember_embed_model(
            namespace,
            &profile.source,
            &profile.target,
            &profile.model,
        );
        state.metrics.remember_embed_serving(
            namespace,
            &profile.source,
            &profile.target,
            profile.serving.label(),
        );
    }
    if target.starts_with("embed_") && explicit_model.is_none() {
        return Err(AppError::Validation(
            "a model name must be provided".to_string(),
        ));
    }

    let serving = declared
        .map(|profile| profile.serving)
        .unwrap_or(ServingPreference::Native);
    let gateway_served = serving == ServingPreference::Autoscaler || search_store;
    if !gateway_served {
        let resolved = explicit_model
            .map(|model| {
                (
                    target.to_string(),
                    format!("embed_{target}"),
                    model.to_string(),
                )
            })
            .or_else(|| {
                declared.map(|profile| {
                    (
                        profile.source.clone(),
                        profile.target.clone(),
                        profile.model.clone(),
                    )
                })
            })
            .or_else(|| {
                state
                    .metrics
                    .embed_model_hint(namespace, target)
                    .map(|model| (target.to_string(), format!("embed_{target}"), model))
            });
        let resolved = match resolved {
            Some(resolved) => Some(resolved),
            None => state
                .turbopuffer()
                .head_namespace(namespace)
                .await
                .ok()
                .and_then(|metadata| metadata_embed_profile(&metadata.raw, target)),
        };
        if let Some((source, target, model)) = resolved {
            state
                .metrics
                .remember_embed_model(namespace, &source, &target, &model);
        }
        return Ok(());
    }
    let model = explicit_model
        .map(str::to_string)
        .or_else(|| declared.map(|profile| profile.model.clone()))
        .ok_or_else(|| {
            AppError::Validation(
                "a model name must be provided when Layer resolves `Embed`".to_string(),
            )
        })?;
    let text = embed[1].as_str().expect("validated string").to_string();
    let profile = EmbeddingProfile {
        source: target.to_string(),
        target: target.to_string(),
        model,
        dims: declared.and_then(|profile| profile.dims),
        serving,
        revision: declared.and_then(|profile| profile.revision.clone()),
        instructions: declared
            .map(|profile| profile.instructions.clone())
            .unwrap_or_default(),
        modality: declared.map(|profile| profile.modality).unwrap_or_default(),
        chunk: None,
        layer_extensions: declared.is_some_and(|profile| profile.layer_extensions),
        materialized: declared.is_some_and(|profile| profile.materialized),
    };
    let text = apply_instruction(profile.instructions.query.as_deref(), &text);
    let vectors = resolve_vectors(
        state,
        namespace,
        &profile,
        EmbeddingModality::Text,
        &[text],
        &mut preparation.performance,
    )
    .await?;
    rank_by[2] = serde_json::to_value(&vectors[0]).expect("vector is JSON");
    if search_store {
        rank_by[0] = Value::String("vector".to_string());
    } else if let Some(profile) = declared {
        rank_by[0] = Value::String(profile.target.clone());
    }
    preparation.passthrough = false;
    Ok(())
}

fn metadata_embed_profile(metadata: &Value, source: &str) -> Option<(String, String, String)> {
    let embed = metadata.get("schema")?.get(source)?.get("embed")?;
    let model = embed
        .as_str()
        .or_else(|| embed.get("model").and_then(Value::as_str))?;
    let target = embed
        .get("attribute")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| format!("embed_{source}"));
    Some((source.to_string(), target, model.to_string()))
}

fn validate_embed(attribute: &str, embed: &Value) -> Result<EmbeddingProfile, AppError> {
    match embed {
        Value::String(model) => {
            validate_model(model)?;
            Ok(EmbeddingProfile {
                source: attribute.to_string(),
                target: format!("embed_{attribute}"),
                model: model.clone(),
                dims: None,
                serving: ServingPreference::Native,
                revision: None,
                instructions: EmbeddingInstructions::default(),
                modality: EmbeddingModality::Text,
                chunk: None,
                layer_extensions: false,
                materialized: false,
            })
        }
        Value::Object(options) => validate_embed_options(attribute, options),
        _ => Err(AppError::Validation(format!(
            "schema attribute `{attribute}` must set `embed` to a provider-namespaced model string, an options object, or null"
        ))),
    }
}

fn validate_embed_options(
    attribute: &str,
    options: &Map<String, Value>,
) -> Result<EmbeddingProfile, AppError> {
    let model = options
        .get("model")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            AppError::Validation(format!(
                "schema attribute `{attribute}` extended `embed` form requires a string `model`"
            ))
        })?;
    validate_model(model)?;
    let dims = match options.get("dims") {
        Some(dims) => Some(dims.as_u64().filter(|dims| *dims > 0).ok_or_else(|| {
            AppError::Validation(format!(
                "schema attribute `{attribute}` `embed.dims` must be a positive integer"
            ))
        })?),
        None => None,
    };
    let target = options
        .get("attribute")
        .map(|target| {
            target
                .as_str()
                .filter(|target| !target.is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    AppError::Validation(format!(
                        "schema attribute `{attribute}` `embed.attribute` must be a non-empty string"
                    ))
                })
        })
        .transpose()?
        .unwrap_or_else(|| format!("embed_{attribute}"));
    if target == attribute {
        return Err(AppError::Validation(format!(
            "schema attribute `{attribute}` cannot store its embedding in the source attribute"
        )));
    }
    let revision = optional_non_empty_string(options, "revision", attribute)?;
    if revision
        .as_deref()
        .is_some_and(|revision| revision.chars().any(char::is_whitespace))
    {
        return Err(AppError::Validation(format!(
            "schema attribute `{attribute}` `embed.revision` must not contain whitespace"
        )));
    }
    let instructions = parse_instructions(options.get("instructions"), attribute)?;
    let modality = match options.get("modality") {
        None => EmbeddingModality::Text,
        Some(Value::String(value)) if value == "text" => EmbeddingModality::Text,
        Some(Value::String(value)) if value == "image" => EmbeddingModality::Image,
        Some(_) => {
            return Err(AppError::Validation(format!(
                "schema attribute `{attribute}` `embed.modality` must be `text` or `image`"
            )))
        }
    };
    let chunk = options
        .get("chunk")
        .map(|value| parse_chunk(value, attribute))
        .transpose()?;
    if modality == EmbeddingModality::Image && chunk.is_some() {
        return Err(AppError::Validation(format!(
            "schema attribute `{attribute}` cannot combine `embed.modality: image` with `embed.chunk`"
        )));
    }
    if modality == EmbeddingModality::Image && !model.to_ascii_lowercase().contains("clip") {
        return Err(AppError::Validation(format!(
            "schema attribute `{attribute}` `embed.modality: image` requires a CLIP-family model"
        )));
    }
    let layer_extensions = ["revision", "instructions", "chunk", "modality"]
        .iter()
        .any(|key| options.contains_key(*key));
    for key in options.keys() {
        if ![
            "model",
            "dims",
            "attribute",
            "serving",
            "revision",
            "instructions",
            "chunk",
            "modality",
        ]
        .contains(&key.as_str())
        {
            return Err(AppError::Validation(format!(
                "schema attribute `{attribute}` has unsupported `embed.{key}` field"
            )));
        }
    }
    Ok(EmbeddingProfile {
        source: attribute.to_string(),
        target,
        model: model.to_string(),
        dims,
        serving: parse_serving_preference(options.get("serving"), attribute)?,
        revision,
        instructions,
        modality,
        chunk,
        layer_extensions,
        materialized: false,
    })
}

fn consume_serving(embed: &mut Value) {
    if let Some(options) = embed.as_object_mut() {
        options.remove("serving");
    }
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
                "schema attribute `{attribute}` `embed.serving` requires `prefer: native` or `prefer: autoscaler`"
            ))
        })?;
    match prefer {
        "native" => Ok(ServingPreference::Native),
        "autoscaler" => Ok(ServingPreference::Autoscaler),
        _ => Err(AppError::Validation(format!(
            "schema attribute `{attribute}` has unsupported `embed.serving.prefer` value `{prefer}`"
        ))),
    }
}

fn optional_non_empty_string(
    options: &Map<String, Value>,
    key: &str,
    attribute: &str,
) -> Result<Option<String>, AppError> {
    options
        .get(key)
        .map(|value| {
            value
                .as_str()
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    AppError::Validation(format!(
                        "schema attribute `{attribute}` `embed.{key}` must be a non-empty string"
                    ))
                })
        })
        .transpose()
}

fn parse_instructions(
    value: Option<&Value>,
    attribute: &str,
) -> Result<EmbeddingInstructions, AppError> {
    let Some(value) = value else {
        return Ok(EmbeddingInstructions::default());
    };
    let options = value.as_object().ok_or_else(|| {
        AppError::Validation(format!(
            "schema attribute `{attribute}` `embed.instructions` must be an object"
        ))
    })?;
    for key in options.keys() {
        if !["document", "query"].contains(&key.as_str()) {
            return Err(AppError::Validation(format!(
                "schema attribute `{attribute}` has unsupported `embed.instructions.{key}` field"
            )));
        }
    }
    Ok(EmbeddingInstructions {
        document: optional_non_empty_string(options, "document", attribute)?,
        query: optional_non_empty_string(options, "query", attribute)?,
    })
}

fn parse_chunk(value: &Value, attribute: &str) -> Result<ChunkConfig, AppError> {
    let chunk = serde_json::from_value::<ChunkConfig>(value.clone()).map_err(|error| {
        AppError::Validation(format!(
            "schema attribute `{attribute}` has invalid `embed.chunk`: {error}"
        ))
    })?;
    validate_chunk(&chunk, attribute, false)?;
    Ok(chunk)
}

fn validate_chunk(chunk: &ChunkConfig, attribute: &str, nested: bool) -> Result<(), AppError> {
    let scalar = ["none", "fixed", "recursive", "sentence", "markdown"];
    if chunk.strategy == "section" {
        if nested {
            return Err(AppError::Validation(format!(
                "schema attribute `{attribute}` `embed.chunk.split` cannot use `strategy: section`"
            )));
        }
        if chunk.section_source.as_deref() != Some("jsonFields")
            || chunk.fields.is_empty()
            || chunk
                .fields
                .iter()
                .any(|field| field.is_empty() || field.contains('#'))
            || chunk
                .fields
                .iter()
                .enumerate()
                .any(|(index, field)| chunk.fields[index + 1..].contains(field))
        {
            return Err(AppError::Validation(format!(
                "schema attribute `{attribute}` section chunking requires `sectionSource: jsonFields` and non-empty `fields`"
            )));
        }
        if chunk.unit.is_some()
            || chunk.size.is_some()
            || chunk.overlap.is_some()
            || chunk.tokenizer.is_some()
        {
            return Err(AppError::Validation(format!(
                "schema attribute `{attribute}` section chunking puts window fields under `embed.chunk.split`"
            )));
        }
        if chunk
            .section_attribute
            .as_deref()
            .is_some_and(|value| value.is_empty() || value.starts_with("_hevlayer_"))
        {
            return Err(AppError::Validation(format!(
                "schema attribute `{attribute}` `embed.chunk.sectionAttribute` must be non-empty and must not use the reserved _hevlayer_* prefix"
            )));
        }
        if let Some(split) = chunk.split.as_deref() {
            validate_chunk(split, attribute, true)?;
        }
        return Ok(());
    }
    if !scalar.contains(&chunk.strategy.as_str()) {
        return Err(AppError::Validation(format!(
            "schema attribute `{attribute}` has unsupported `embed.chunk.strategy` `{}`",
            chunk.strategy
        )));
    }
    if chunk.section_source.is_some()
        || !chunk.fields.is_empty()
        || chunk.section_attribute.is_some()
        || chunk.split.is_some()
    {
        return Err(AppError::Validation(format!(
            "schema attribute `{attribute}` section composition fields require `embed.chunk.strategy: section`"
        )));
    }
    if chunk.strategy == "none" {
        return Ok(());
    }
    if chunk.size.is_none_or(|size| size == 0) {
        return Err(AppError::Validation(format!(
            "schema attribute `{attribute}` `embed.chunk.size` must be a positive integer"
        )));
    }
    let unit = chunk.unit.as_deref().unwrap_or("characters");
    if !["characters", "tokens"].contains(&unit) {
        return Err(AppError::Validation(format!(
            "schema attribute `{attribute}` `embed.chunk.unit` must be `characters` or `tokens`"
        )));
    }
    if unit == "tokens" && chunk.tokenizer.as_deref().is_none_or(str::is_empty) {
        return Err(AppError::Validation(format!(
            "schema attribute `{attribute}` token chunking requires a non-empty `embed.chunk.tokenizer`"
        )));
    }
    if chunk.overlap.unwrap_or(0) >= chunk.size.unwrap_or(0) {
        return Err(AppError::Validation(format!(
            "schema attribute `{attribute}` `embed.chunk.overlap` must be smaller than `size`"
        )));
    }
    Ok(())
}

fn apply_instruction(instruction: Option<&str>, value: &str) -> String {
    match instruction {
        Some(instruction) => format!("{instruction}{value}"),
        None => value.to_string(),
    }
}

fn validate_embed_expression(embed: &[Value]) -> Result<(), AppError> {
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
    if let Some(options) = embed.get(2) {
        if options
            .as_object()
            .and_then(|options| options.get("model"))
            .and_then(Value::as_str)
            .is_none()
        {
            return Err(AppError::Validation(
                "`Embed` options require a string `model`".to_string(),
            ));
        }
    }
    Ok(())
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

fn has_row_write(body: Option<&Map<String, Value>>) -> bool {
    body.is_some_and(|body| {
        ["upsert_rows", "upsert_columns"]
            .iter()
            .any(|key| body.get(*key).is_some_and(non_empty_collection))
    })
}

fn non_empty_collection(value: &Value) -> bool {
    value.as_array().is_some_and(|value| !value.is_empty())
        || value.as_object().is_some_and(|value| !value.is_empty())
}

fn reject_source_patches(body: &Value, profiles: &[&EmbeddingProfile]) -> Result<(), AppError> {
    for profile in profiles {
        let mut sources = vec![profile.source.as_str()];
        if let Some(chunk) = profile
            .chunk
            .as_ref()
            .filter(|chunk| chunk.strategy == "section")
        {
            sources.extend(chunk.fields.iter().map(String::as_str));
        }
        for source in sources {
            let patches_source = body
                .get("patch_rows")
                .and_then(Value::as_array)
                .is_some_and(|rows| {
                    rows.iter()
                        .any(|row| row.as_object().is_some_and(|row| row.contains_key(source)))
                })
                || body
                    .get("patch_columns")
                    .and_then(Value::as_object)
                    .is_some_and(|columns| columns.contains_key(source))
                || body
                    .pointer("/patch_by_filter/patch")
                    .and_then(Value::as_object)
                    .is_some_and(|patch| patch.contains_key(source));
            if patches_source {
                return Err(AppError::Validation(format!(
                    "patching autoscaler-embedded source attribute `{source}` is unsupported; upsert the full row so Layer can recompute `{}`",
                    profile.target
                )));
            }
        }
    }
    Ok(())
}

struct PreparedWriteInputs {
    values: Vec<String>,
    row_indices: Vec<usize>,
}

fn prepare_write_inputs(
    body: &mut Value,
    profile: &EmbeddingProfile,
) -> Result<PreparedWriteInputs, AppError> {
    if let Some(chunk) = profile.chunk.as_ref() {
        return prepare_chunk_rows(body, profile, chunk);
    }
    if let Some(rows) = body.get("upsert_rows").and_then(Value::as_array) {
        let values = rows
            .iter()
            .map(|row| {
                row.get(&profile.source)
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .ok_or_else(|| {
                        AppError::Validation(format!(
                            "upsert row must include string attribute `{}` for embedding",
                            profile.source
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(PreparedWriteInputs {
            row_indices: (0..values.len()).collect(),
            values,
        });
    }
    if let Some(columns) = body.get("upsert_columns").and_then(Value::as_object) {
        let values = columns
            .get(&profile.source)
            .and_then(Value::as_array)
            .ok_or_else(|| {
                AppError::Validation(format!(
                    "upsert columns must include string column `{}` for embedding",
                    profile.source
                ))
            })?
            .iter()
            .map(|text| {
                text.as_str().map(str::to_string).ok_or_else(|| {
                    AppError::Validation(format!(
                        "upsert column `{}` values must be strings for embedding",
                        profile.source
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(PreparedWriteInputs {
            row_indices: (0..values.len()).collect(),
            values,
        });
    }
    Ok(PreparedWriteInputs {
        values: Vec::new(),
        row_indices: Vec::new(),
    })
}

fn prepare_chunk_rows(
    body: &mut Value,
    profile: &EmbeddingProfile,
    chunk: &ChunkConfig,
) -> Result<PreparedWriteInputs, AppError> {
    if body.get("upsert_columns").is_some() {
        return Err(AppError::Validation(
            "chunked embedding requires `upsert_rows`; columnar writes cannot fan out rows"
                .to_string(),
        ));
    }
    let rows = body
        .get_mut("upsert_rows")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| {
            AppError::Validation("chunked embedding requires `upsert_rows`".to_string())
        })?;
    let originals = std::mem::take(rows);
    let mut expanded = Vec::new();
    let mut values = Vec::new();
    let mut row_indices = Vec::new();
    for original in originals {
        let object = original.as_object().ok_or_else(|| {
            AppError::Validation("upsert rows must be objects for chunked embedding".to_string())
        })?;
        if let Some(name) = object.keys().find(|name| name.starts_with("_hevlayer_")) {
            return Err(AppError::Validation(format!(
                "attribute '{name}' uses the reserved _hevlayer_* prefix"
            )));
        }
        let id = object
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                AppError::Validation(
                    "chunked embedding requires every upsert row to have a string `id`".to_string(),
                )
            })?
            .to_string();
        let chunks = row_chunks(object, profile, chunk)?;
        if chunks.is_empty() {
            return Err(AppError::Validation(format!(
                "upsert row `{id}` produced no non-empty chunks for embedded attribute `{}`",
                profile.source
            )));
        }
        if chunks.len() == 1
            && chunks[0].section.is_none()
            && chunks[0].value == object[&profile.source]
        {
            row_indices.push(expanded.len());
            values.push(chunks[0].value.as_str().expect("scalar chunk").to_string());
            expanded.push(original);
            continue;
        }
        expanded.push(original.clone());
        for generated in chunks {
            let mut generated_row = original.clone();
            let row = generated_row.as_object_mut().expect("validated object");
            let suffix = generated
                .section
                .as_ref()
                .map(|section| format!("{section}#{}", generated.index))
                .unwrap_or_else(|| generated.index.to_string());
            row.insert("id".to_string(), Value::String(format!("{id}#{suffix}")));
            row.insert(profile.source.clone(), generated.value.clone());
            row.insert("_hevlayer_parent_id".to_string(), Value::String(id.clone()));
            row.insert(
                "_hevlayer_chunk_index".to_string(),
                Value::from(generated.index as u64),
            );
            if let Some(section) = generated.section {
                row.insert(
                    chunk
                        .section_attribute
                        .clone()
                        .unwrap_or_else(|| "section".to_string()),
                    Value::String(section),
                );
            }
            row_indices.push(expanded.len());
            values.push(
                generated
                    .value
                    .as_str()
                    .expect("validated chunk value")
                    .to_string(),
            );
            expanded.push(generated_row);
        }
    }
    *rows = expanded;
    Ok(PreparedWriteInputs {
        values,
        row_indices,
    })
}

struct GeneratedChunk {
    value: Value,
    section: Option<String>,
    index: usize,
}

fn row_chunks(
    row: &Map<String, Value>,
    profile: &EmbeddingProfile,
    chunk: &ChunkConfig,
) -> Result<Vec<GeneratedChunk>, AppError> {
    if chunk.strategy == "section" {
        let mut output = Vec::new();
        for field in &chunk.fields {
            let Some(value) = row.get(field) else {
                continue;
            };
            let text = value.as_str().ok_or_else(|| {
                AppError::Validation(format!(
                    "upsert row section field `{field}` must be a string"
                ))
            })?;
            if text.trim().is_empty() {
                continue;
            }
            let parts = match chunk.split.as_deref() {
                Some(split) => split_text(text, split),
                None => vec![text.to_string()],
            };
            output.extend(
                parts
                    .into_iter()
                    .enumerate()
                    .map(|(index, value)| GeneratedChunk {
                        value: Value::String(value),
                        section: Some(field.clone()),
                        index,
                    }),
            );
        }
        return Ok(output);
    }
    let source = row.get(&profile.source).ok_or_else(|| {
        AppError::Validation(format!(
            "upsert row must include attribute `{}` for chunked embedding",
            profile.source
        ))
    })?;
    let text = source.as_str().ok_or_else(|| {
        AppError::Validation(format!(
            "upsert row attribute `{}` must be a string for embedding",
            profile.source
        ))
    })?;
    Ok(split_text(text, chunk)
        .into_iter()
        .enumerate()
        .map(|(index, value)| GeneratedChunk {
            value: Value::String(value),
            section: None,
            index,
        })
        .collect())
}

fn split_text(text: &str, chunk: &ChunkConfig) -> Vec<String> {
    if chunk.strategy == "none" {
        return vec![text.to_string()];
    }
    let size = chunk.size.expect("validated chunk size");
    let overlap = chunk.overlap.unwrap_or(0);
    if chunk.unit.as_deref() == Some("tokens") {
        let units = text.split_whitespace().collect::<Vec<_>>();
        return token_windows(&units, size, overlap, &chunk.strategy);
    }
    let units = text.chars().collect::<Vec<_>>();
    character_windows(&units, size, overlap, &chunk.strategy)
}

fn character_windows(units: &[char], size: usize, overlap: usize, strategy: &str) -> Vec<String> {
    if units.is_empty() {
        return Vec::new();
    }
    let mut output = Vec::new();
    let mut start = 0;
    while start < units.len() {
        let limit = (start + size).min(units.len());
        let end = if limit == units.len() || strategy == "fixed" {
            limit
        } else {
            (start + overlap + 1..limit)
                .rev()
                .find(|index| character_boundary(units, *index, strategy))
                .unwrap_or(limit)
        };
        output.push(units[start..end].iter().collect());
        if end == units.len() {
            break;
        }
        start = end - overlap;
    }
    output
}

fn character_boundary(units: &[char], index: usize, strategy: &str) -> bool {
    let previous = units[index - 1];
    match strategy {
        "sentence" => matches!(previous, '.' | '!' | '?'),
        "markdown" => previous == '\n' && units.get(index).is_some_and(|next| *next == '#'),
        "recursive" => previous == '\n' || matches!(previous, '.' | '!' | '?') || previous == ' ',
        _ => false,
    }
}

fn token_windows(units: &[&str], size: usize, overlap: usize, strategy: &str) -> Vec<String> {
    if units.is_empty() {
        return Vec::new();
    }
    let mut output = Vec::new();
    let mut start = 0;
    while start < units.len() {
        let limit = (start + size).min(units.len());
        let end = if limit == units.len() || strategy == "fixed" {
            limit
        } else {
            (start + overlap + 1..limit)
                .rev()
                .find(|index| token_boundary(units, *index, strategy))
                .unwrap_or(limit)
        };
        output.push(units[start..end].join(" "));
        if end == units.len() {
            break;
        }
        start = end - overlap;
    }
    output
}

fn token_boundary(units: &[&str], index: usize, strategy: &str) -> bool {
    let previous = units[index - 1];
    match strategy {
        "sentence" => previous.ends_with(['.', '!', '?']),
        "markdown" => units.get(index).is_some_and(|next| next.starts_with('#')),
        "recursive" => true,
        _ => false,
    }
}

fn apply_write_vectors(
    body: &mut Value,
    profile: &EmbeddingProfile,
    row_indices: &[usize],
    vectors: &[Vec<f64>],
    search_store: bool,
) -> Result<(), AppError> {
    if let Some(rows) = body.get_mut("upsert_rows").and_then(Value::as_array_mut) {
        for (&row_index, vector) in row_indices.iter().zip(vectors) {
            let row = rows[row_index]
                .as_object_mut()
                .expect("prepare_write_inputs validated rows");
            let vector = serde_json::to_value(vector).expect("vector is JSON");
            if search_store {
                row.remove(&profile.target);
                row.insert("vector".to_string(), vector);
            } else {
                row.insert(profile.target.clone(), vector);
            }
        }
    } else if let Some(columns) = body
        .get_mut("upsert_columns")
        .and_then(Value::as_object_mut)
    {
        let vectors = serde_json::to_value(vectors).expect("vectors are JSON");
        if search_store {
            columns.remove(&profile.target);
            columns.insert("vector".to_string(), vectors);
        } else {
            columns.insert(profile.target.clone(), vectors);
        }
    }
    let Some(first) = vectors.first() else {
        return Ok(());
    };
    let dims = profile.dims.unwrap_or(first.len() as u64);
    if first.len() as u64 != dims {
        return Err(AppError::Upstream(format!(
            "embedding provider returned {} dimensions for requested {dims}",
            first.len()
        )));
    }
    let schema = body
        .as_object_mut()
        .expect("write body is object")
        .entry("schema")
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .ok_or_else(|| AppError::Validation("schema must be an object".to_string()))?;
    if !search_store {
        schema.insert(
            profile.target.clone(),
            json!({"type": format!("[{dims}]f32"), "ann": true}),
        );
    }
    Ok(())
}

async fn resolve_vectors(
    state: &AppState,
    namespace: &str,
    profile: &EmbeddingProfile,
    modality: EmbeddingModality,
    texts: &[String],
    performance: &mut Value,
) -> Result<Vec<Vec<f64>>, AppError> {
    let request = EmbeddingRequest {
        model: &profile.model,
        dims: profile.dims,
        revision: profile.revision.as_deref(),
        modality,
    };
    let provider_model = request.provider_model();
    let mut vectors = vec![None; texts.len()];
    let mut misses = Vec::new();
    let mut miss_positions = Vec::new();
    let mut miss_keys = Vec::new();
    let keys = texts
        .iter()
        .map(|text| cache_key(&provider_model, profile.dims, modality, text))
        .collect::<Vec<_>>();
    for (position, key) in keys.iter().enumerate() {
        if let Some(cached) = state.embedding_cache.get(key) {
            if cached.value().0.elapsed() < state.embedding_cache_ttl {
                vectors[position] = Some(cached.value().1.as_ref().clone());
                continue;
            }
        }
        state.embedding_cache.remove(key);
        misses.push(texts[position].clone());
        miss_positions.push(position);
        miss_keys.push(key.clone());
    }

    if !misses.is_empty() {
        let provider = state.embedding_provider.as_ref().ok_or_else(|| {
            AppError::ServiceUnavailable(
                "Layer-served embedding requires a configured kind=turbopuffer VectorStore credential"
                    .to_string(),
            )
        })?;
        let batch = provider
            .embed(&request, &misses)
            .await
            .map_err(|error| AppError::Upstream(format!("embedding provider failed: {error}")))?;
        if batch.vectors.len() != misses.len() {
            return Err(AppError::Upstream(format!(
                "embedding provider returned {} vectors for {} inputs",
                batch.vectors.len(),
                misses.len()
            )));
        }
        merge_performance(performance, &batch.performance);
        state.metrics.observe_embed_performance(
            namespace,
            &profile.model,
            profile.serving.label(),
            &batch.performance,
        );
        if let Some(billing) = batch.billing.as_ref() {
            state.metrics.observe_tpuf_billing(namespace, billing);
        }
        for ((position, vector), key) in
            miss_positions.into_iter().zip(batch.vectors).zip(miss_keys)
        {
            state
                .embedding_cache
                .insert(key, (Instant::now(), Arc::new(vector.clone())));
            vectors[position] = Some(vector);
        }
    }

    vectors
        .into_iter()
        .map(|vector| {
            vector
                .ok_or_else(|| AppError::Upstream("embedding cache resolution failed".to_string()))
        })
        .collect()
}

fn cache_key(model: &str, dims: Option<u64>, modality: EmbeddingModality, text: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(model.as_bytes());
    hash.update([0]);
    hash.update(dims.unwrap_or_default().to_le_bytes());
    hash.update([0]);
    hash.update(match modality {
        EmbeddingModality::Text => b"text".as_slice(),
        EmbeddingModality::Image => b"image".as_slice(),
    });
    hash.update([0]);
    hash.update(text.as_bytes());
    format!("{:x}", hash.finalize())
}

pub(crate) fn merge_performance(target: &mut Value, source: &Value) {
    let Some(source) = source.as_object() else {
        return;
    };
    if !target.is_object() {
        *target = json!({});
    }
    let target = target.as_object_mut().expect("set to object");
    for key in ["embedding_tokens", "embedding_ms"] {
        let Some(value) = source.get(key).and_then(Value::as_f64) else {
            continue;
        };
        let current = target.get(key).and_then(Value::as_f64).unwrap_or(0.0);
        target.insert(key.to_string(), Value::from(current + value));
    }
}

pub(crate) fn merge_response_performance(body: &mut Vec<u8>, embedding: &Value) {
    if embedding.as_object().is_none_or(Map::is_empty) {
        return;
    }
    let Ok(mut response) = serde_json::from_slice::<Value>(body) else {
        return;
    };
    let performance = response
        .as_object_mut()
        .map(|response| response.entry("performance").or_insert_with(|| json!({})));
    if let Some(performance) = performance {
        merge_performance(performance, embedding);
        if let Ok(encoded) = serde_json::to_vec(&response) {
            *body = encoded;
        }
    }
}

async fn load_profiles(
    state: &AppState,
    namespace: &str,
) -> Result<Vec<EmbeddingProfile>, AppError> {
    if let Some(profiles) = state.wire_embedding_profiles.get(namespace) {
        return Ok(profiles.clone());
    }
    let key = profile_key(namespace);
    let profiles = match state.s3.get(&key).await {
        Ok(Some(body)) => serde_json::from_slice(&body).map_err(|error| {
            AppError::Upstream(format!("invalid embedding profile object {key}: {error}"))
        })?,
        Ok(None) => Vec::new(),
        Err(error) => {
            return Err(AppError::Upstream(format!(
                "failed to read embedding profiles: {error}"
            )))
        }
    };
    state
        .wire_embedding_profiles
        .insert(namespace.to_string(), profiles.clone());
    Ok(profiles)
}

async fn save_profiles(
    state: &AppState,
    namespace: &str,
    profiles: &[EmbeddingProfile],
) -> Result<(), AppError> {
    let key = profile_key(namespace);
    let body = serde_json::to_vec(profiles)
        .map_err(|error| AppError::Upstream(format!("serialize embedding profiles: {error}")))?;
    state
        .s3
        .put(&key, body)
        .await
        .map_err(|error| AppError::Upstream(format!("persist embedding profiles: {error}")))?;
    state
        .wire_embedding_profiles
        .insert(namespace.to_string(), profiles.to_vec());
    Ok(())
}

fn profile_key(namespace: &str) -> String {
    format!("{PROFILE_PREFIX}/{namespace}.json")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_serving_is_consumed_but_tpuf_fields_remain() {
        let mut embed = json!({
            "model": "voyage/voyage-4-lite",
            "dims": 512,
            "attribute": "title_vector",
            "serving": {"prefer": "native"}
        });
        let profile = validate_embed("title", &embed).unwrap();
        consume_serving(&mut embed);
        assert_eq!(profile.serving, ServingPreference::Native);
        assert_eq!(
            embed,
            json!({
                "model": "voyage/voyage-4-lite",
                "dims": 512,
                "attribute": "title_vector"
            })
        );
    }

    #[test]
    fn serving_preference_accepts_only_native_or_autoscaler() {
        assert_eq!(
            parse_serving_preference(Some(&json!({"prefer": "native"})), "title").unwrap(),
            ServingPreference::Native
        );
        assert_eq!(
            parse_serving_preference(Some(&json!({"prefer": "autoscaler"})), "title").unwrap(),
            ServingPreference::Autoscaler
        );
        assert!(parse_serving_preference(Some(&json!({"prefer": "automatic"})), "title").is_err());
    }

    #[test]
    fn derived_column_requires_explicit_model_without_profile() {
        let embed = json!(["Embed", "query"]);
        validate_embed_expression(embed.as_array().unwrap()).unwrap();
        assert!("embed_title".starts_with("embed_"));
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
        assert_eq!(
            metadata_embed_profile(
                &json!({
                    "schema": {"title": {"type": "string", "embed": {
                        "model": "voyage/voyage-4-lite",
                        "attribute": "title_vector"
                    }}}
                }),
                "title"
            ),
            Some((
                "title".to_string(),
                "title_vector".to_string(),
                "voyage/voyage-4-lite".to_string()
            ))
        );
    }

    #[test]
    fn performance_values_accumulate() {
        let mut performance = json!({"server_total_ms": 3});
        merge_performance(
            &mut performance,
            &json!({"embedding_tokens": 8, "embedding_ms": 12}),
        );
        merge_performance(
            &mut performance,
            &json!({"embedding_tokens": 2, "embedding_ms": 4}),
        );
        assert_eq!(performance["embedding_tokens"], 10.0);
        assert_eq!(performance["embedding_ms"], 16.0);
    }
}
