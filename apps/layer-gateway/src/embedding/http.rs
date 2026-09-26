//! Private inference protocol v1 client (RFC 0120).
//!
//! One `HttpEmbeddingProvider` serves the CE CPU sidecar (`apps/layer-embed`)
//! and later GPU workers that speak the same protocol. The gateway owns
//! purpose (document versus query), applies each model's registry prefix
//! exactly once, batches under the advertised limits, and distrusts every
//! response: count, model, fingerprint, dimensions, finiteness, and unit norm
//! are verified before any vector is returned, cached, or stored.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::json;

use super::{
    is_clip_model, EmbeddingBatch, EmbeddingError, EmbeddingModality, EmbeddingProvider,
    EmbeddingPurpose, EmbeddingRequest,
};

pub const PROTOCOL_VERSION: u64 = 1;
pub const DEFAULT_QUERY_BUDGET: Duration = Duration::from_secs(10);
pub const DEFAULT_WRITE_BUDGET: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);
/// Response bodies above this are protocol failures, whatever they contain.
const MAX_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
/// JSON envelope overhead reserved when packing inputs under `max_body_bytes`.
const REQUEST_OVERHEAD_BYTES: u64 = 512;
const UNIT_NORM_TOLERANCE: f64 = 1e-4;

/// Operator configuration for the HTTP provider.
#[derive(Debug, Clone)]
pub struct HttpEmbeddingOptions {
    /// Whole-request inference budget for query embeddings.
    pub query_budget: Duration,
    /// Whole-request inference budget for write embeddings, shared by every
    /// batch of one logical request.
    pub write_budget: Duration,
    /// Model ids an in-process provider already claims (for example the
    /// Lattice model). A sidecar registry that advertises one of them is a
    /// configuration conflict, never a silent choice.
    pub reserved_models: Vec<String>,
    /// Whether an in-process CLIP provider is configured, which reserves
    /// every CLIP-family model id.
    pub reserved_clip: bool,
}

impl Default for HttpEmbeddingOptions {
    fn default() -> Self {
        Self {
            query_budget: DEFAULT_QUERY_BUDGET,
            write_budget: DEFAULT_WRITE_BUDGET,
            reserved_models: Vec::new(),
            reserved_clip: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Limits {
    pub max_body_bytes: u64,
    pub max_batch_items: u64,
    pub max_batch_tokens: u64,
    pub max_input_bytes: u64,
    pub max_timeout_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Prefixes {
    pub document: String,
    pub query: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ModelRecord {
    pub id: String,
    pub artifact_sha256: String,
    pub dimensions: u64,
    pub modalities: Vec<String>,
    pub max_tokens: u64,
    pub prefixes: Prefixes,
}

impl ModelRecord {
    fn prefix(&self, purpose: EmbeddingPurpose) -> &str {
        match purpose {
            EmbeddingPurpose::Document => &self.prefixes.document,
            EmbeddingPurpose::Query => &self.prefixes.query,
        }
    }

    fn supports(&self, modality: EmbeddingModality) -> bool {
        let wanted = match modality {
            EmbeddingModality::Text => "text",
            EmbeddingModality::Image => "image",
        };
        self.modalities.iter().any(|modality| modality == wanted)
    }
}

#[derive(Debug, Deserialize)]
struct Discovery {
    protocol_version: u64,
    manifest_sha256: String,
    limits: Limits,
    models: Vec<ModelRecord>,
}

/// The pinned model registry of one embedder process. Loaded once per
/// gateway process; a changed fingerprint is only observed after an explicit
/// gateway restart, and even then it is rejected against pinned profiles
/// instead of silently upgrading them.
#[derive(Debug)]
pub struct Registry {
    pub manifest_sha256: String,
    pub limits: Limits,
    pub models: BTreeMap<String, ModelRecord>,
}

/// What a namespace profile records about the model it was declared with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfilePin {
    pub artifact_sha256: String,
    pub dimensions: u64,
}

#[derive(Debug, Deserialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Debug, Deserialize)]
struct ErrorBody {
    code: String,
    #[serde(default)]
    message: String,
    #[serde(default)]
    index: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct Output {
    model: String,
    artifact_sha256: String,
    dimensions: u64,
    vectors: Vec<Vec<f64>>,
    usage: Usage,
    timing: Timing,
}

#[derive(Debug, Deserialize)]
struct Usage {
    input_tokens: u64,
}

#[derive(Debug, Deserialize)]
struct Timing {
    inference_ms: f64,
}

pub struct HttpEmbeddingProvider {
    origin: String,
    client: reqwest::Client,
    options: HttpEmbeddingOptions,
    registry: RwLock<Option<Arc<Registry>>>,
}

impl std::fmt::Debug for HttpEmbeddingProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpEmbeddingProvider")
            .field("origin", &self.origin)
            .finish_non_exhaustive()
    }
}

impl HttpEmbeddingProvider {
    /// Build a provider for an operator-controlled origin such as
    /// `http://embed:8081`. Credentials, paths, query strings, and fragments
    /// are rejected; redirects and ambient proxies are disabled.
    pub fn new(url: &str, options: HttpEmbeddingOptions) -> Result<Self, String> {
        let origin = validate_origin(url)?;
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .user_agent(concat!(
                "hevlayer-gateway/",
                env!("CARGO_PKG_VERSION"),
                " (+https://hevlayer.com)"
            ))
            .build()
            .map_err(|error| format!("could not construct embedder HTTP client: {error}"))?;
        Ok(Self {
            origin,
            client,
            options,
            registry: RwLock::new(None),
        })
    }

    pub fn origin(&self) -> &str {
        &self.origin
    }

    /// The registry, loading it from `GET /v1/models` on first use. Once
    /// loaded it is pinned for the life of this process.
    pub async fn registry(&self) -> Result<Arc<Registry>, EmbeddingError> {
        if let Some(registry) = self.registry.read().expect("registry lock").as_ref() {
            return Ok(Arc::clone(registry));
        }
        let registry = Arc::new(self.discover().await?);
        let mut slot = self.registry.write().expect("registry lock");
        if let Some(existing) = slot.as_ref() {
            return Ok(Arc::clone(existing));
        }
        *slot = Some(Arc::clone(&registry));
        Ok(registry)
    }

    async fn discover(&self) -> Result<Registry, EmbeddingError> {
        let response = self
            .client
            .get(format!("{}/v1/models", self.origin))
            .timeout(DISCOVERY_TIMEOUT)
            .send()
            .await
            .map_err(|error| transport_error("discovery", error))?;
        let status = response.status().as_u16();
        let body = read_body(response, DISCOVERY_TIMEOUT).await?;
        if status != 200 {
            return Err(match status {
                503 => EmbeddingError::Unavailable(format!(
                    "local embedder at {} is not ready",
                    self.origin
                )),
                _ => EmbeddingError::Upstream(format!(
                    "local embedder discovery returned HTTP {status}"
                )),
            });
        }
        let discovery: Discovery = serde_json::from_slice(&body).map_err(|error| {
            EmbeddingError::Upstream(format!(
                "local embedder discovery returned an invalid document: {error}"
            ))
        })?;
        if discovery.protocol_version != PROTOCOL_VERSION {
            return Err(EmbeddingError::Upstream(format!(
                "local embedder speaks protocol version {}, gateway requires {PROTOCOL_VERSION}",
                discovery.protocol_version
            )));
        }
        if !valid_hash(&discovery.manifest_sha256) {
            return Err(EmbeddingError::Upstream(
                "local embedder discovery has an invalid manifest fingerprint".to_string(),
            ));
        }
        if discovery.limits.max_batch_items == 0
            || discovery.limits.max_batch_tokens == 0
            || discovery.limits.max_body_bytes <= REQUEST_OVERHEAD_BYTES
            || discovery.limits.max_input_bytes == 0
            || discovery.limits.max_timeout_ms == 0
        {
            return Err(EmbeddingError::Upstream(
                "local embedder discovery advertises zero limits".to_string(),
            ));
        }
        let mut models = BTreeMap::new();
        for record in discovery.models {
            if !valid_hash(&record.artifact_sha256)
                || record.dimensions == 0
                || record.max_tokens == 0
            {
                return Err(EmbeddingError::Upstream(format!(
                    "local embedder advertises an invalid record for model `{}`",
                    record.id
                )));
            }
            if self.options.reserved_models.contains(&record.id)
                || (self.options.reserved_clip && is_clip_model(&record.id))
            {
                return Err(EmbeddingError::Unavailable(format!(
                    "model `{}` is served both by LAYER_EMBED_URL and by an in-process provider; configure exactly one",
                    record.id
                )));
            }
            if models.insert(record.id.clone(), record).is_some() {
                return Err(EmbeddingError::Upstream(
                    "local embedder discovery lists a model id twice".to_string(),
                ));
            }
        }
        Ok(Registry {
            manifest_sha256: discovery.manifest_sha256,
            limits: discovery.limits,
            models,
        })
    }

    /// Resolve the registry record a profile pins to. Unknown models,
    /// revision pins, unsupported modalities, and mismatched dimensions fail
    /// before any inference.
    pub async fn pin(
        &self,
        model: &str,
        dims: Option<u64>,
        modality: EmbeddingModality,
        revision: Option<&str>,
    ) -> Result<ProfilePin, EmbeddingError> {
        let registry = self.registry().await?;
        let record = resolve_record(&registry, model, dims, modality, revision)?;
        Ok(ProfilePin {
            artifact_sha256: record.artifact_sha256.clone(),
            dimensions: record.dimensions,
        })
    }

    async fn run(
        &self,
        request: &EmbeddingRequest<'_>,
        modality: EmbeddingModality,
        inputs: Vec<String>,
    ) -> Result<EmbeddingBatch, EmbeddingError> {
        if request.modality != modality {
            return Err(EmbeddingError::Validation(format!(
                "local embedder received {} inputs for a {} request",
                modality_label(modality),
                modality_label(request.modality)
            )));
        }
        if inputs.is_empty() {
            return Ok(EmbeddingBatch {
                vectors: Vec::new(),
                performance: json!({}),
                billing: None,
            });
        }
        let registry = self.registry().await?;
        let record = resolve_record(
            &registry,
            request.model,
            request.dims,
            request.modality,
            request.revision,
        )?;
        if let Some(pinned) = request.artifact {
            if pinned != record.artifact_sha256 {
                return Err(EmbeddingError::Validation(format!(
                    "model `{}` now serves a different artifact than this namespace was indexed with; re-index into a fresh namespace before querying or writing",
                    record.id
                )));
            }
        }
        let limits = &registry.limits;

        let mut unique = Vec::<String>::new();
        let mut positions = HashMap::<String, usize>::new();
        let mut requested = Vec::with_capacity(inputs.len());
        for (index, input) in inputs.iter().enumerate() {
            let prepared = match modality {
                EmbeddingModality::Text => {
                    if input.trim().is_empty() {
                        return Err(EmbeddingError::Validation(format!(
                            "embedding input {index} is empty"
                        )));
                    }
                    format!("{}{input}", record.prefix(request.purpose))
                }
                EmbeddingModality::Image => input.clone(),
            };
            if prepared.len() as u64 > limits.max_input_bytes {
                return Err(EmbeddingError::Validation(format!(
                    "embedding input {index} is {} bytes; the local embedder accepts at most {} bytes per input",
                    prepared.len(),
                    limits.max_input_bytes
                )));
            }
            let position = match positions.get(&prepared) {
                Some(position) => *position,
                None => {
                    let position = unique.len();
                    positions.insert(prepared.clone(), position);
                    unique.push(prepared);
                    position
                }
            };
            requested.push(position);
        }

        let budget = match request.purpose {
            EmbeddingPurpose::Query => self.options.query_budget,
            EmbeddingPurpose::Document => self.options.write_budget,
        };
        let deadline = Instant::now() + budget;
        let mut unique_vectors = Vec::<Vec<f64>>::with_capacity(unique.len());
        let mut tokens = 0_u64;
        let mut inference_ms = 0.0_f64;
        for batch in pack_batches(&unique, limits, record.max_tokens) {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(EmbeddingError::Timeout(format!(
                    "local embedding exceeded its {}ms budget",
                    budget.as_millis()
                )));
            }
            let timeout_ms = (remaining.as_millis() as u64).clamp(1, limits.max_timeout_ms);
            let body = json!({
                "model": record.id,
                "artifact_sha256": record.artifact_sha256,
                "dimensions": record.dimensions,
                "purpose": request.purpose.label(),
                "modality": modality_label(modality),
                "inputs": batch,
                "timeout_ms": timeout_ms,
            });
            let output = self.post_embeddings(&body, remaining).await?;
            let vectors = validate_output(record, batch.len(), output)?;
            unique_vectors.extend(vectors.0);
            tokens = tokens.saturating_add(vectors.1);
            inference_ms += vectors.2;
        }

        Ok(EmbeddingBatch {
            vectors: requested
                .into_iter()
                .map(|position| unique_vectors[position].clone())
                .collect(),
            performance: json!({
                "embedding_tokens": tokens,
                "embedding_ms": inference_ms,
            }),
            billing: None,
        })
    }

    async fn post_embeddings(
        &self,
        body: &serde_json::Value,
        remaining: Duration,
    ) -> Result<Output, EmbeddingError> {
        let response = self
            .client
            .post(format!("{}/v1/embeddings", self.origin))
            .timeout(remaining)
            .json(body)
            .send()
            .await
            .map_err(|error| transport_error("inference", error))?;
        let status = response.status().as_u16();
        let bytes = read_body(response, remaining).await?;
        if status == 200 {
            return serde_json::from_slice::<Output>(&bytes).map_err(|error| {
                EmbeddingError::Upstream(format!(
                    "local embedder returned an invalid response document: {error}"
                ))
            });
        }
        Err(map_error(status, &bytes))
    }
}

#[async_trait]
impl EmbeddingProvider for HttpEmbeddingProvider {
    async fn embed(
        &self,
        request: &EmbeddingRequest<'_>,
        texts: &[String],
    ) -> Result<EmbeddingBatch, EmbeddingError> {
        self.run(request, EmbeddingModality::Text, texts.to_vec())
            .await
    }

    async fn embed_images(
        &self,
        request: &EmbeddingRequest<'_>,
        images: &[Vec<u8>],
    ) -> Result<EmbeddingBatch, EmbeddingError> {
        let encoded = images
            .iter()
            .map(|bytes| BASE64_STANDARD.encode(bytes))
            .collect();
        self.run(request, EmbeddingModality::Image, encoded).await
    }
}

fn validate_origin(url: &str) -> Result<String, String> {
    let parsed = reqwest::Url::parse(url.trim())
        .map_err(|error| format!("LAYER_EMBED_URL `{url}` is not a valid URL: {error}"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(format!("LAYER_EMBED_URL `{url}` must use http or https"));
    }
    if parsed.host_str().is_none() {
        return Err(format!("LAYER_EMBED_URL `{url}` has no host"));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(format!(
            "LAYER_EMBED_URL `{url}` must not carry credentials"
        ));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(format!(
            "LAYER_EMBED_URL `{url}` must not carry a query string or fragment"
        ));
    }
    if !matches!(parsed.path(), "" | "/") {
        return Err(format!(
            "LAYER_EMBED_URL `{url}` must be an origin without a path"
        ));
    }
    Ok(parsed.origin().ascii_serialization())
}

fn resolve_record<'r>(
    registry: &'r Registry,
    model: &str,
    dims: Option<u64>,
    modality: EmbeddingModality,
    revision: Option<&str>,
) -> Result<&'r ModelRecord, EmbeddingError> {
    if revision.is_some() {
        return Err(EmbeddingError::Validation(
            "local embedding does not accept `embed.revision`; the local embedder serves pinned artifacts only".to_string(),
        ));
    }
    let record = registry.models.get(model).ok_or_else(|| {
        let available = registry.models.keys().cloned().collect::<Vec<_>>();
        EmbeddingError::Validation(format!(
            "model `{model}` is not served by the local embedder (available: {})",
            available.join(", ")
        ))
    })?;
    if !record.supports(modality) {
        return Err(EmbeddingError::Validation(format!(
            "model `{model}` does not support {} inputs on the local embedder",
            modality_label(modality)
        )));
    }
    if let Some(dims) = dims {
        if dims != record.dimensions {
            return Err(EmbeddingError::Validation(format!(
                "model `{model}` emits {} dimensions, but `embed.dims` requested {dims}",
                record.dimensions
            )));
        }
    }
    Ok(record)
}

/// Split prepared inputs into consecutive batches under the advertised item,
/// token, and body limits. Without a tokenizer in the gateway, the item cap
/// is `min(max_batch_items, max_batch_tokens / max_tokens)`; the sidecar
/// enforces exact tokens.
fn pack_batches<'a>(inputs: &'a [String], limits: &Limits, max_tokens: u64) -> Vec<Vec<&'a str>> {
    let item_cap = limits
        .max_batch_items
        .min(limits.max_batch_tokens / max_tokens.max(1))
        .max(1) as usize;
    let body_cap = limits.max_body_bytes - REQUEST_OVERHEAD_BYTES;
    let mut batches = Vec::new();
    let mut current = Vec::new();
    let mut current_bytes = 0_u64;
    for input in inputs {
        let encoded = serde_json::to_string(input)
            .map(|s| s.len() as u64 + 1)
            .unwrap_or(0);
        if !current.is_empty() && (current.len() >= item_cap || current_bytes + encoded > body_cap)
        {
            batches.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current.push(input.as_str());
        current_bytes += encoded;
    }
    if !current.is_empty() {
        batches.push(current);
    }
    batches
}

fn validate_output(
    record: &ModelRecord,
    expected: usize,
    output: Output,
) -> Result<(Vec<Vec<f64>>, u64, f64), EmbeddingError> {
    if output.model != record.id {
        return Err(EmbeddingError::Upstream(format!(
            "local embedder answered for model `{}` instead of `{}`",
            output.model, record.id
        )));
    }
    if output.artifact_sha256 != record.artifact_sha256 {
        return Err(EmbeddingError::Upstream(format!(
            "local embedder answered with a different artifact fingerprint for model `{}`",
            record.id
        )));
    }
    if output.dimensions != record.dimensions {
        return Err(EmbeddingError::Upstream(format!(
            "local embedder answered with {} dimensions instead of {}",
            output.dimensions, record.dimensions
        )));
    }
    if output.vectors.len() != expected {
        return Err(EmbeddingError::Upstream(format!(
            "local embedder returned {} vectors for {expected} inputs",
            output.vectors.len()
        )));
    }
    if !output.timing.inference_ms.is_finite() || output.timing.inference_ms < 0.0 {
        return Err(EmbeddingError::Upstream(
            "local embedder reported an invalid inference time".to_string(),
        ));
    }
    for (index, vector) in output.vectors.iter().enumerate() {
        if vector.len() as u64 != record.dimensions {
            return Err(EmbeddingError::Upstream(format!(
                "local embedder vector {index} has {} dimensions instead of {}",
                vector.len(),
                record.dimensions
            )));
        }
        if vector.iter().any(|value| !value.is_finite()) {
            return Err(EmbeddingError::Upstream(format!(
                "local embedder vector {index} contains a non-finite value"
            )));
        }
        let norm = vector.iter().map(|value| value * value).sum::<f64>().sqrt();
        if (norm - 1.0).abs() > UNIT_NORM_TOLERANCE {
            return Err(EmbeddingError::Upstream(format!(
                "local embedder vector {index} is not unit length (norm {norm:.6})"
            )));
        }
    }
    Ok((
        output.vectors,
        output.usage.input_tokens,
        output.timing.inference_ms,
    ))
}

/// Map the private error envelope to a gateway error class. Only the exact
/// status/code pairs the protocol defines are trusted; anything else is a
/// protocol failure.
fn map_error(status: u16, body: &[u8]) -> EmbeddingError {
    let envelope = serde_json::from_slice::<ErrorEnvelope>(body).ok();
    let Some(envelope) = envelope else {
        return EmbeddingError::Upstream(format!(
            "local embedder returned HTTP {status} without a readable error envelope"
        ));
    };
    let ErrorBody {
        code,
        message,
        index,
    } = envelope.error;
    let detail = match index {
        Some(index) => format!("input {index}: {message}"),
        None => message,
    };
    match (status, code.as_str()) {
        (400, "invalid_request" | "invalid_input")
        | (404, "model_not_found")
        | (409, "artifact_mismatch")
        | (413, "body_too_large" | "batch_too_large")
        | (422, "input_too_long" | "unsupported_modality") => EmbeddingError::Validation(format!(
            "local embedder rejected the request ({code}): {detail}"
        )),
        (429, "overloaded") | (503, "not_ready") => {
            EmbeddingError::Unavailable(format!("local embedder is unavailable ({code}): {detail}"))
        }
        (504, "deadline_exceeded") => EmbeddingError::Timeout(format!(
            "local embedder did not finish within the request deadline: {detail}"
        )),
        _ => EmbeddingError::Upstream(format!(
            "local embedder failed (HTTP {status} {code}): {detail}"
        )),
    }
}

fn transport_error(stage: &str, error: reqwest::Error) -> EmbeddingError {
    if error.is_timeout() {
        EmbeddingError::Timeout(format!("local embedder {stage} timed out"))
    } else if error.is_connect() {
        EmbeddingError::Unavailable(format!(
            "local embedder is unreachable during {stage}: {error}"
        ))
    } else {
        EmbeddingError::Upstream(format!("local embedder {stage} failed: {error}"))
    }
}

async fn read_body(
    response: reqwest::Response,
    remaining: Duration,
) -> Result<Vec<u8>, EmbeddingError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
    {
        return Err(EmbeddingError::Upstream(
            "local embedder response exceeds 2 MiB".to_string(),
        ));
    }
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    let read = async {
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| transport_error("response read", error))?;
            if body.len() + chunk.len() > MAX_RESPONSE_BYTES {
                return Err(EmbeddingError::Upstream(
                    "local embedder response exceeds 2 MiB".to_string(),
                ));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(())
    };
    match tokio::time::timeout(remaining, read).await {
        Ok(result) => result?,
        Err(_) => {
            return Err(EmbeddingError::Timeout(
                "local embedder response timed out".to_string(),
            ))
        }
    }
    Ok(body)
}

fn valid_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn modality_label(modality: EmbeddingModality) -> &'static str {
    match modality {
        EmbeddingModality::Text => "text",
        EmbeddingModality::Image => "image",
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::routing::{get, post};
    use axum::Json;
    use serde_json::Value;

    use super::*;

    const MINILM: &str = "sentence-transformers/all-MiniLM-L6-v2";
    const BGE: &str = "BAAI/bge-small-en-v1.5";
    const BGE_QUERY_PREFIX: &str = "Represent this sentence for searching relevant passages: ";

    fn fingerprint(seed: u8) -> String {
        format!("{:02x}", seed).repeat(32)
    }

    fn unit(index: usize, dims: usize) -> Vec<f64> {
        let mut vector = vec![0.0; dims];
        vector[index % dims] = 1.0;
        vector
    }

    /// Fake sidecar. Behaviour is scripted through `Script`; every request is
    /// recorded so tests can assert on prefixes, batching, and purpose.
    #[derive(Default)]
    struct Script {
        ready: bool,
        dims: usize,
        limits: Option<Value>,
        /// `(status, body)` returned by the next inference calls, in order.
        /// When exhausted the fake answers honestly.
        canned: Mutex<Vec<(StatusCode, Value)>>,
        /// Delay applied to inference responses.
        delay: Option<Duration>,
        extra_models: Vec<Value>,
        answer_fingerprint: Option<String>,
        swap_positions: bool,
    }

    type FakeState = (Arc<Script>, Arc<Mutex<Vec<Value>>>, Arc<AtomicUsize>);

    struct Fake {
        origin: String,
        requests: Arc<Mutex<Vec<Value>>>,
        discoveries: Arc<AtomicUsize>,
        _server: tokio::task::JoinHandle<()>,
    }

    fn discovery(script: &Script) -> Value {
        let mut models = vec![
            json!({
                "id": BGE,
                "artifact_sha256": fingerprint(0xbb),
                "dimensions": script.dims,
                "modalities": ["text"],
                "max_tokens": 512,
                "normalization": "l2",
                "pooling": "cls",
                "prefixes": {"document": "", "query": BGE_QUERY_PREFIX}
            }),
            json!({
                "id": MINILM,
                "artifact_sha256": fingerprint(0xaa),
                "dimensions": script.dims,
                "modalities": ["text"],
                "max_tokens": 256,
                "normalization": "l2",
                "pooling": "mean_masked",
                "prefixes": {"document": "", "query": ""}
            }),
        ];
        models.extend(script.extra_models.iter().cloned());
        json!({
            "protocol_version": 1,
            "manifest_sha256": fingerprint(0x11),
            "limits": script.limits.clone().unwrap_or_else(|| json!({
                "max_body_bytes": 1048576,
                "max_batch_items": 32,
                "max_batch_tokens": 4096,
                "max_input_bytes": 65536,
                "max_timeout_ms": 120000
            })),
            "models": models,
        })
    }

    async fn spawn(script: Script) -> Fake {
        let script = Arc::new(script);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let discoveries = Arc::new(AtomicUsize::new(0));
        let state = (script, Arc::clone(&requests), Arc::clone(&discoveries));
        let app = axum::Router::new()
            .route(
                "/v1/models",
                get(|State((script, _, discoveries)): State<FakeState>| async move {
                    discoveries.fetch_add(1, Ordering::SeqCst);
                    if !script.ready {
                        return (
                            StatusCode::SERVICE_UNAVAILABLE,
                            Json(json!({"error": {"code": "not_ready", "message": "loading", "index": null, "retryable": true}})),
                        )
                            .into_response();
                    }
                    Json(discovery(&script)).into_response()
                }),
            )
            .route(
                "/v1/embeddings",
                post(|State((script, requests, _)): State<FakeState>, Json(body): Json<Value>| async move {
                    requests.lock().unwrap().push(body.clone());
                    if let Some(delay) = script.delay {
                        tokio::time::sleep(delay).await;
                    }
                    let canned = {
                        let mut canned = script.canned.lock().unwrap();
                        if canned.is_empty() { None } else { Some(canned.remove(0)) }
                    };
                    if let Some((status, body)) = canned {
                        return (status, Json(body)).into_response();
                    }
                    let inputs = body["inputs"].as_array().unwrap();
                    let mut vectors = inputs
                        .iter()
                        .enumerate()
                        .map(|(index, _)| unit(index, script.dims))
                        .collect::<Vec<_>>();
                    if script.swap_positions && vectors.len() >= 2 {
                        vectors.swap(0, 1);
                    }
                    let discovery = discovery(&script);
                    let record = discovery["models"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|record| record["id"] == body["model"])
                        .cloned()
                        .unwrap_or_else(|| json!({"artifact_sha256": fingerprint(0)}));
                    Json(json!({
                        "model": body["model"],
                        "artifact_sha256": script.answer_fingerprint.clone().map(Value::from).unwrap_or_else(|| record["artifact_sha256"].clone()),
                        "dimensions": body["dimensions"],
                        "vectors": vectors,
                        "usage": {"input_tokens": inputs.len() * 3},
                        "timing": {"inference_ms": 1.5},
                        "order_marker": "ignored-by-clients"
                    }))
                    .into_response()
                }),
            )
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Fake {
            origin,
            requests,
            discoveries,
            _server: server,
        }
    }

    fn ready(dims: usize) -> Script {
        Script {
            ready: true,
            dims,
            ..Script::default()
        }
    }

    fn provider(fake: &Fake) -> HttpEmbeddingProvider {
        HttpEmbeddingProvider::new(&fake.origin, HttpEmbeddingOptions::default()).unwrap()
    }

    fn request<'a>(model: &'a str, purpose: EmbeddingPurpose) -> EmbeddingRequest<'a> {
        EmbeddingRequest {
            model,
            dims: None,
            revision: None,
            modality: EmbeddingModality::Text,
            purpose,
            artifact: None,
        }
    }

    fn texts(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn origin_validation_rejects_credentials_paths_queries_and_schemes() {
        assert_eq!(
            validate_origin("http://embed:8081").unwrap(),
            "http://embed:8081"
        );
        assert_eq!(
            validate_origin("https://embed.internal/").unwrap(),
            "https://embed.internal"
        );
        for bad in [
            "embed:8081",
            "ftp://embed:8081",
            "http://user:pw@embed:8081",
            "http://embed:8081/v1",
            "http://embed:8081?x=1",
            "http://embed:8081#frag",
        ] {
            assert!(validate_origin(bad).is_err(), "{bad} should be rejected");
        }
    }

    #[test]
    fn batches_pack_under_item_token_and_body_limits() {
        let limits = Limits {
            max_body_bytes: 1024,
            max_batch_items: 32,
            max_batch_tokens: 4096,
            max_input_bytes: 65536,
            max_timeout_ms: 120000,
        };
        let inputs = (0..20).map(|i| format!("input {i}")).collect::<Vec<_>>();
        // 4096 / 512 = 8 items per batch for BGE.
        let batches = pack_batches(&inputs, &limits, 512);
        assert_eq!(
            batches.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![8, 8, 4]
        );
        // 4096 / 256 = 16 items per batch for MiniLM.
        let batches = pack_batches(&inputs, &limits, 256);
        assert_eq!(
            batches.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![16, 4]
        );
        // Body cap: 1024 - 512 overhead = 512 bytes of encoded inputs.
        let big = vec!["x".repeat(300), "y".repeat(300), "z".repeat(10)];
        let batches = pack_batches(&big, &limits, 256);
        assert_eq!(batches.iter().map(Vec::len).collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(batches.concat().len(), 3);
    }

    #[test]
    fn error_envelopes_map_to_exact_classes() {
        let envelope = |code: &str, index: Option<u64>| {
            serde_json::to_vec(&json!({"error": {"code": code, "message": "m", "index": index, "retryable": false}})).unwrap()
        };
        assert!(
            matches!(map_error(400, &envelope("invalid_input", Some(2))), EmbeddingError::Validation(ref m) if m.contains("input 2"))
        );
        assert!(matches!(
            map_error(404, &envelope("model_not_found", None)),
            EmbeddingError::Validation(_)
        ));
        assert!(matches!(
            map_error(404, &envelope("not_found", None)),
            EmbeddingError::Upstream(_)
        ));
        assert!(matches!(
            map_error(405, &envelope("method_not_allowed", None)),
            EmbeddingError::Upstream(_)
        ));
        assert!(matches!(
            map_error(409, &envelope("artifact_mismatch", None)),
            EmbeddingError::Validation(_)
        ));
        assert!(matches!(
            map_error(413, &envelope("batch_too_large", None)),
            EmbeddingError::Validation(_)
        ));
        assert!(matches!(
            map_error(422, &envelope("input_too_long", Some(0))),
            EmbeddingError::Validation(_)
        ));
        assert!(matches!(
            map_error(422, &envelope("unsupported_modality", None)),
            EmbeddingError::Validation(_)
        ));
        assert!(matches!(
            map_error(422, &envelope("dimension_mismatch", None)),
            EmbeddingError::Upstream(_)
        ));
        assert!(matches!(
            map_error(422, &envelope("batch_token_limit", None)),
            EmbeddingError::Upstream(_)
        ));
        assert!(matches!(
            map_error(429, &envelope("overloaded", None)),
            EmbeddingError::Unavailable(_)
        ));
        assert!(matches!(
            map_error(503, &envelope("not_ready", None)),
            EmbeddingError::Unavailable(_)
        ));
        assert!(matches!(
            map_error(504, &envelope("deadline_exceeded", None)),
            EmbeddingError::Timeout(_)
        ));
        assert!(matches!(
            map_error(500, &envelope("invalid_output", None)),
            EmbeddingError::Upstream(_)
        ));
        assert!(matches!(
            map_error(500, &envelope("inference_failed", None)),
            EmbeddingError::Upstream(_)
        ));
        // Mismatched pairs and unknown codes are protocol failures, never retryable.
        assert!(matches!(
            map_error(200, &envelope("overloaded", None)),
            EmbeddingError::Upstream(_)
        ));
        assert!(matches!(
            map_error(418, &envelope("teapot", None)),
            EmbeddingError::Upstream(_)
        ));
        assert!(matches!(
            map_error(503, b"<html>"),
            EmbeddingError::Upstream(_)
        ));
    }

    #[tokio::test]
    async fn applies_registry_prefix_once_by_purpose_and_restores_order() {
        let fake = spawn(ready(4)).await;
        let provider = provider(&fake);

        let documents = provider
            .embed(
                &request(BGE, EmbeddingPurpose::Document),
                &texts(&["red shoes", "blue shoes", "red shoes"]),
            )
            .await
            .unwrap();
        assert_eq!(documents.vectors.len(), 3);
        assert_eq!(documents.vectors[0], documents.vectors[2]);
        assert_ne!(documents.vectors[0], documents.vectors[1]);
        assert_eq!(documents.performance["embedding_tokens"], 6);
        assert!(documents.billing.is_none());

        let queries = provider
            .embed(
                &request(BGE, EmbeddingPurpose::Query),
                &texts(&["red shoes"]),
            )
            .await
            .unwrap();
        assert_eq!(queries.vectors.len(), 1);

        let requests = fake.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0]["purpose"], "document");
        assert_eq!(requests[0]["inputs"], json!(["red shoes", "blue shoes"]));
        assert_eq!(requests[0]["artifact_sha256"], fingerprint(0xbb));
        assert_eq!(requests[0]["dimensions"], 4);
        assert_eq!(requests[0]["modality"], "text");
        let write_timeout = requests[0]["timeout_ms"].as_u64().unwrap();
        assert!((1..=60_000).contains(&write_timeout), "{write_timeout}");
        assert_eq!(requests[1]["purpose"], "query");
        let query_timeout = requests[1]["timeout_ms"].as_u64().unwrap();
        assert!((1..=10_000).contains(&query_timeout), "{query_timeout}");
        assert_eq!(
            requests[1]["inputs"],
            json!([format!("{BGE_QUERY_PREFIX}red shoes")])
        );
        assert_eq!(
            fake.discoveries.load(Ordering::SeqCst),
            1,
            "discovery is loaded once"
        );
    }

    #[tokio::test]
    async fn minilm_is_symmetric_and_omitted_dims_derive_from_discovery() {
        let fake = spawn(ready(4)).await;
        let provider = provider(&fake);
        let pin = provider
            .pin(MINILM, None, EmbeddingModality::Text, None)
            .await
            .unwrap();
        assert_eq!(pin.dimensions, 4);
        assert_eq!(pin.artifact_sha256, fingerprint(0xaa));
        provider
            .embed(
                &request(MINILM, EmbeddingPurpose::Query),
                &texts(&["hello"]),
            )
            .await
            .unwrap();
        let requests = fake.requests.lock().unwrap();
        assert_eq!(requests[0]["inputs"], json!(["hello"]));
    }

    #[tokio::test]
    async fn splits_writes_into_bounded_batches_under_one_deadline() {
        let fake = spawn(Script {
            limits: Some(json!({
                "max_body_bytes": 1048576,
                "max_batch_items": 3,
                "max_batch_tokens": 4096,
                "max_input_bytes": 65536,
                "max_timeout_ms": 120000
            })),
            ..ready(4)
        })
        .await;
        let provider = provider(&fake);
        let inputs = (0..7).map(|i| format!("doc {i}")).collect::<Vec<_>>();
        let batch = provider
            .embed(&request(MINILM, EmbeddingPurpose::Document), &inputs)
            .await
            .unwrap();
        assert_eq!(batch.vectors.len(), 7);
        for (index, vector) in batch.vectors.iter().enumerate() {
            assert_eq!(*vector, unit(index % 3, 4));
        }
        assert_eq!(batch.performance["embedding_tokens"], 21);
        let requests = fake.requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        let first = requests[0]["timeout_ms"].as_u64().unwrap();
        let last = requests[2]["timeout_ms"].as_u64().unwrap();
        assert!(
            first <= 60_000 && last <= first,
            "later batches share the remaining write budget"
        );
    }

    #[tokio::test]
    async fn validation_failures_happen_before_any_http_call() {
        let fake = spawn(ready(4)).await;
        let provider = provider(&fake);
        let cases: Vec<(EmbeddingRequest<'_>, Vec<String>)> = vec![
            (
                request("openai/text-embedding-3-small", EmbeddingPurpose::Query),
                texts(&["x"]),
            ),
            (
                EmbeddingRequest {
                    dims: Some(384),
                    ..request(MINILM, EmbeddingPurpose::Query)
                },
                texts(&["x"]),
            ),
            (
                EmbeddingRequest {
                    revision: Some("main"),
                    ..request(MINILM, EmbeddingPurpose::Query)
                },
                texts(&["x"]),
            ),
            (
                request(MINILM, EmbeddingPurpose::Document),
                texts(&["ok", "   "]),
            ),
            (
                request(MINILM, EmbeddingPurpose::Document),
                vec!["x".repeat(65537)],
            ),
            (
                EmbeddingRequest {
                    artifact: Some(
                        "0000000000000000000000000000000000000000000000000000000000000000",
                    ),
                    ..request(MINILM, EmbeddingPurpose::Query)
                },
                texts(&["x"]),
            ),
        ];
        for (request, inputs) in cases {
            let error = provider.embed(&request, &inputs).await.unwrap_err();
            assert!(
                matches!(error, EmbeddingError::Validation(_)),
                "{request:?}: {error}"
            );
        }
        let error = provider
            .embed_images(
                &EmbeddingRequest {
                    modality: EmbeddingModality::Image,
                    ..request(MINILM, EmbeddingPurpose::Document)
                },
                &[vec![1, 2, 3]],
            )
            .await
            .unwrap_err();
        assert!(matches!(error, EmbeddingError::Validation(ref m) if m.contains("image")));
        assert!(fake.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn not_ready_discovery_is_unavailable_and_is_retried_on_the_next_call() {
        let fake = spawn(Script {
            ready: false,
            dims: 4,
            ..Script::default()
        })
        .await;
        let provider = provider(&fake);
        let error = provider
            .embed(&request(MINILM, EmbeddingPurpose::Query), &texts(&["x"]))
            .await
            .unwrap_err();
        assert!(matches!(error, EmbeddingError::Unavailable(_)), "{error}");
        let error = provider
            .pin(MINILM, None, EmbeddingModality::Text, None)
            .await
            .unwrap_err();
        assert!(matches!(error, EmbeddingError::Unavailable(_)), "{error}");
        assert_eq!(fake.discoveries.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn unreachable_embedder_is_unavailable() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let provider =
            HttpEmbeddingProvider::new(&origin, HttpEmbeddingOptions::default()).unwrap();
        let error = provider
            .embed(&request(MINILM, EmbeddingPurpose::Query), &texts(&["x"]))
            .await
            .unwrap_err();
        assert!(matches!(error, EmbeddingError::Unavailable(_)), "{error}");
    }

    #[tokio::test]
    async fn reserved_in_process_models_make_configuration_fail() {
        let fake = spawn(Script {
            extra_models: vec![json!({
                "id": "erikkaum/lattice-retrieval",
                "artifact_sha256": fingerprint(0xcc),
                "dimensions": 4,
                "modalities": ["text"],
                "max_tokens": 256,
                "prefixes": {"document": "", "query": ""}
            })],
            ..ready(4)
        })
        .await;
        let provider = HttpEmbeddingProvider::new(
            &fake.origin,
            HttpEmbeddingOptions {
                reserved_models: vec!["erikkaum/lattice-retrieval".to_string()],
                ..HttpEmbeddingOptions::default()
            },
        )
        .unwrap();
        let error = provider.registry().await.unwrap_err();
        assert!(matches!(error, EmbeddingError::Unavailable(ref m) if m.contains("exactly one")));
        // Without the in-process claim the same registry loads.
        let provider =
            HttpEmbeddingProvider::new(&fake.origin, HttpEmbeddingOptions::default()).unwrap();
        assert!(provider.registry().await.is_ok());
    }

    #[tokio::test]
    async fn distrusts_every_malformed_success_response() {
        let good_vectors = || json!([unit(0, 4)]);
        let output = |patch: fn(&mut Value)| {
            let mut body = json!({
                "model": MINILM,
                "artifact_sha256": fingerprint(0xaa),
                "dimensions": 4,
                "vectors": good_vectors(),
                "usage": {"input_tokens": 3},
                "timing": {"inference_ms": 1.0}
            });
            patch(&mut body);
            (StatusCode::OK, body)
        };
        let cases: Vec<(StatusCode, Value)> = vec![
            output(|b| b["vectors"] = json!([unit(0, 4), unit(1, 4)])),
            output(|b| b["vectors"] = json!([])),
            output(|b| b["model"] = json!(BGE)),
            output(|b| b["artifact_sha256"] = json!(fingerprint(0xbb))),
            output(|b| b["dimensions"] = json!(3)),
            output(|b| b["vectors"] = json!([[1.0, 0.0, 0.0]])),
            output(|b| b["vectors"] = json!([[0.5, 0.5, 0.0, 0.0]])),
            output(|b| b["vectors"] = json!([[0.0, 0.0, 0.0, 0.0]])),
            output(|b| b["vectors"] = json!([["nan", 0.0, 0.0, 0.0]])),
            output(|b| b["usage"] = json!({})),
            output(|b| b["timing"] = json!({"inference_ms": -1.0})),
            (StatusCode::OK, json!("not an object")),
            (
                StatusCode::IM_A_TEAPOT,
                json!({"error": {"code": "teapot", "message": "", "index": null, "retryable": false}}),
            ),
        ];
        let count = cases.len();
        let fake = spawn(Script {
            canned: Mutex::new(cases),
            ..ready(4)
        })
        .await;
        let provider = provider(&fake);
        for _ in 0..count {
            let error = provider
                .embed(&request(MINILM, EmbeddingPurpose::Query), &texts(&["x"]))
                .await
                .unwrap_err();
            assert!(matches!(error, EmbeddingError::Upstream(_)), "{error}");
        }
        // Honest answers resume once the script is exhausted.
        assert!(provider
            .embed(&request(MINILM, EmbeddingPurpose::Query), &texts(&["x"]))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn a_failing_later_batch_fails_the_whole_request() {
        // Two-item batches: the first answer is honest, the second fails.
        let fake = spawn(Script {
            limits: Some(json!({
                "max_body_bytes": 1048576,
                "max_batch_items": 2,
                "max_batch_tokens": 4096,
                "max_input_bytes": 65536,
                "max_timeout_ms": 120000
            })),
            canned: Mutex::new(vec![
                (
                    StatusCode::OK,
                    json!({
                        "model": MINILM,
                        "artifact_sha256": fingerprint(0xaa),
                        "dimensions": 4,
                        "vectors": [unit(0, 4), unit(1, 4)],
                        "usage": {"input_tokens": 6},
                        "timing": {"inference_ms": 1.0}
                    }),
                ),
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    json!({"error": {"code": "inference_failed", "message": "kernel", "index": null, "retryable": false}}),
                ),
            ]),
            ..ready(4)
        })
        .await;
        let provider = provider(&fake);
        let error = provider
            .embed(
                &request(MINILM, EmbeddingPurpose::Document),
                &texts(&["a", "b", "c"]),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, EmbeddingError::Upstream(_)), "{error}");
        assert_eq!(fake.requests.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn slow_embedder_maps_to_timeout() {
        let fake = spawn(Script {
            delay: Some(Duration::from_millis(400)),
            ..ready(4)
        })
        .await;
        let slow = HttpEmbeddingProvider::new(
            &fake.origin,
            HttpEmbeddingOptions {
                query_budget: Duration::from_millis(100),
                ..HttpEmbeddingOptions::default()
            },
        )
        .unwrap();
        let error = slow
            .embed(&request(MINILM, EmbeddingPurpose::Query), &texts(&["x"]))
            .await
            .unwrap_err();
        assert!(matches!(error, EmbeddingError::Timeout(_)), "{error}");
        // Deadline_exceeded from the sidecar maps the same way.
        let fake = spawn(Script {
            canned: Mutex::new(vec![(
                StatusCode::GATEWAY_TIMEOUT,
                json!({"error": {"code": "deadline_exceeded", "message": "late", "index": null, "retryable": true}}),
            )]),
            ..ready(4)
        })
        .await;
        let provider = provider(&fake);
        let error = provider
            .embed(&request(MINILM, EmbeddingPurpose::Query), &texts(&["x"]))
            .await
            .unwrap_err();
        assert!(matches!(error, EmbeddingError::Timeout(_)), "{error}");
    }

    #[tokio::test]
    async fn overloaded_and_not_ready_inference_are_unavailable() {
        let fake = spawn(Script {
            canned: Mutex::new(vec![
                (StatusCode::TOO_MANY_REQUESTS, json!({"error": {"code": "overloaded", "message": "full", "index": null, "retryable": true}})),
                (StatusCode::SERVICE_UNAVAILABLE, json!({"error": {"code": "not_ready", "message": "loading", "index": null, "retryable": true}})),
                (StatusCode::CONFLICT, json!({"error": {"code": "artifact_mismatch", "message": "changed", "index": null, "retryable": false}})),
            ]),
            ..ready(4)
        })
        .await;
        let provider = provider(&fake);
        for expected_unavailable in [true, true, false] {
            let error = provider
                .embed(&request(MINILM, EmbeddingPurpose::Query), &texts(&["x"]))
                .await
                .unwrap_err();
            if expected_unavailable {
                assert!(matches!(error, EmbeddingError::Unavailable(_)), "{error}");
            } else {
                assert!(
                    matches!(error, EmbeddingError::Validation(ref m) if m.contains("artifact_mismatch")),
                    "{error}"
                );
            }
        }
        // No automatic retry within a request: one HTTP call per attempt.
        assert_eq!(fake.requests.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn swapped_valid_vectors_pass_runtime_validation_but_fail_fixture_comparison() {
        // RFC 0120 ordering conformance: a positional swap of two valid unit
        // vectors is undetectable at runtime, so the check is a fixture
        // comparison, not a runtime rejection.
        let honest = spawn(ready(4)).await;
        let swapped = spawn(Script {
            swap_positions: true,
            ..ready(4)
        })
        .await;
        let inputs = texts(&["A", "B", "A"]);
        let reference = provider(&honest)
            .embed(&request(MINILM, EmbeddingPurpose::Document), &inputs)
            .await
            .unwrap();
        let observed = provider(&swapped)
            .embed(&request(MINILM, EmbeddingPurpose::Document), &inputs)
            .await
            .unwrap();
        assert_eq!(reference.vectors[0], reference.vectors[2]);
        assert_eq!(reference.vectors[0], unit(0, 4));
        assert_eq!(reference.vectors[1], unit(1, 4));
        assert_ne!(observed.vectors, reference.vectors);
    }

    #[tokio::test]
    async fn image_modality_is_sent_as_base64_when_advertised() {
        let fake = spawn(Script {
            extra_models: vec![json!({
                "id": "acme/vision-encoder",
                "artifact_sha256": fingerprint(0xdd),
                "dimensions": 4,
                "modalities": ["image"],
                "max_tokens": 1,
                "prefixes": {"document": "", "query": ""}
            })],
            ..ready(4)
        })
        .await;
        let provider = provider(&fake);
        let batch = provider
            .embed_images(
                &EmbeddingRequest {
                    modality: EmbeddingModality::Image,
                    ..request("acme/vision-encoder", EmbeddingPurpose::Document)
                },
                &[vec![1, 2, 3], vec![1, 2, 3]],
            )
            .await
            .unwrap();
        assert_eq!(batch.vectors.len(), 2);
        let requests = fake.requests.lock().unwrap();
        assert_eq!(requests[0]["modality"], "image");
        assert_eq!(requests[0]["inputs"], json!(["AQID"]));
    }
}
