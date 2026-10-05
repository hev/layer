//! Rerank providers for `/search` (RFC 0116, stage 7).
//!
//! A [`RerankProvider`] takes the query, a named question and a batch of
//! documents and returns one calibrated probability per document, plus token
//! counts. Jev (TypeSafe's System One model) is the first implementor; the
//! state it sends is the [`hev-rerank`](https://github.com/hev/reranker)
//! shape ported to Rust: the query and the candidates go into one *state*,
//! one `Noul` ("is this document relevant?") question per document, and the
//! answer's `noul` is the probability. The PyPI package is the reference for
//! the shape, not a dependency.
//!
//! [`rerank_pool`] owns batching: the pool is chunked into calls of
//! `docs_per_call`, the calls run concurrently under the gateway-wide
//! in-flight cap, and each call has a deadline that starts once it holds a
//! permit (the wait for one is bounded separately). Probabilities are comparable
//! across calls, which is what makes chunking sound. One failed call fails
//! the stage, so a response never mixes probabilities with RRF sums.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
use tokio::sync::Semaphore;

pub const DEFAULT_PROVIDER: &str = "jev";
pub const DEFAULT_MODEL: &str = "jev-latest";
pub const DEFAULT_BASE_URL: &str = "https://api.typesafe.ai";
pub const DEFAULT_QUESTION: &str = "generic-1";
pub const DEFAULT_MAX_INFLIGHT: usize = 6;
pub const DEFAULT_TIMEOUT_MS: u64 = 3_000;
pub const DEFAULT_DOCS_PER_CALL: usize = 30;
pub const MAX_DOCS_PER_CALL: usize = 50;
pub const DEFAULT_MAX_CHARS: usize = 2_000;
pub const MAX_MAX_CHARS: usize = 20_000;
/// hev-rerank's `DEFAULT_MAX_STATE_CHARS`: TypeSafe documents a ~32k-token
/// request budget (state + questions), ~150k characters of English; this
/// keeps headroom for the questions and for non-English text.
pub const MAX_STATE_CHARS: usize = 100_000;

const RESERVED_ATTRIBUTE_PREFIX: &str = "_hevlayer_";
const SYSTEM_ONE_PATH: &str = "/v1/systemone";
const RATE_LIMIT_RETRIES: u32 = 2;
const RATE_LIMIT_BACKOFF_FLOOR: Duration = Duration::from_millis(100);
const RATE_LIMIT_BACKOFF_CEIL: Duration = Duration::from_secs(1);

/// A provider credential. `Debug` is redacted so a `Config` or state dump
/// can never print it; the only reader is the provider's auth header.
#[derive(Clone, PartialEq, Eq)]
pub struct RerankKey(String);

impl RerankKey {
    pub fn new(value: impl Into<String>) -> Option<Self> {
        let value = value.into();
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| Self(trimmed.to_string()))
    }

    fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for RerankKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RerankKey(***)")
    }
}

/// A named question version. The text ships in the gateway; callers pick a
/// version by name (RFC 0116 open question 4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RerankQuestion {
    pub version: &'static str,
    /// `{id}` is replaced with the document's key in the state.
    pub instructions: &'static str,
    pub criteria_true: &'static str,
    pub criteria_false: &'static str,
}

/// hev-rerank `prompt.yaml`, verbatim. Domain-neutral phrasing: on SciFact,
/// NFCorpus and FiQA it matched or beat a corpus-tailored one.
const GENERIC_1: RerankQuestion = RerankQuestion {
    version: "generic-1",
    instructions: "Document `documents.{id}` is relevant to `query`: it contains information that answers or directly addresses it.",
    criteria_true: "The document contains information that answers the query or directly addresses what it asks about.",
    criteria_false: "The document is only loosely related, on a similar topic, or does not address what the query asks.",
};

pub fn question(version: &str) -> Option<&'static RerankQuestion> {
    match version {
        "generic-1" => Some(&GENERIC_1),
        _ => None,
    }
}

/// One candidate: its id and the text the reranker reads, as ordered
/// `(attribute, text)` fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RerankDocument {
    pub id: String,
    pub fields: Vec<(String, String)>,
}

impl RerankDocument {
    fn chars(&self) -> usize {
        self.fields
            .iter()
            .map(|(name, text)| name.chars().count() + text.chars().count())
            .sum()
    }
}

/// True for gateway-stamped attributes (`_hevlayer_fetch_count`,
/// `_hevlayer_upserted_at`, `_hevlayer_shard`, ...). They may be L1 numeric
/// features; they are never a text leg and never reranker input.
pub fn is_reserved_attribute(name: &str) -> bool {
    name.starts_with(RESERVED_ATTRIBUTE_PREFIX)
}

/// The one place that decides what text the reranker sees, and therefore the
/// guard: reserved attributes are skipped here by construction, whatever the
/// caller listed. String attributes (and string arrays, joined) are taken in
/// the given order and the document is cut to `max_chars` characters in
/// total, so an early attribute is never starved by a later one.
pub fn reranker_fields(
    attributes: &[String],
    row: &HashMap<String, Value>,
    max_chars: usize,
) -> Vec<(String, String)> {
    let mut remaining = max_chars;
    let mut fields = Vec::new();
    for name in attributes {
        if remaining == 0 {
            break;
        }
        if is_reserved_attribute(name) {
            continue;
        }
        let text = match row.get(name) {
            Some(Value::String(text)) => text.clone(),
            Some(Value::Array(items)) => items
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join("\n"),
            _ => continue,
        };
        let text: String = text.trim().chars().take(remaining).collect();
        if text.is_empty() {
            continue;
        }
        remaining -= text.chars().count();
        fields.push((name.clone(), text));
    }
    fields
}

/// Default reranker attributes: every full-text attribute, an attribute named
/// `title` first, then schema order.
pub fn default_rerank_attributes(full_text: &[String]) -> Vec<String> {
    let mut attributes: Vec<String> = full_text
        .iter()
        .filter(|name| !is_reserved_attribute(name))
        .cloned()
        .collect();
    if let Some(position) = attributes.iter().position(|name| name == "title") {
        let title = attributes.remove(position);
        attributes.insert(0, title);
    }
    attributes
}

pub struct RerankRequest<'a> {
    pub query: &'a str,
    pub question: &'a RerankQuestion,
    pub model: &'a str,
    pub documents: &'a [RerankDocument],
}

#[derive(Debug, Clone, PartialEq)]
pub struct RerankScore {
    pub id: String,
    /// Calibrated probability the document is relevant, `0..=1`.
    pub probability: f64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct RerankBatch {
    pub scores: Vec<RerankScore>,
    /// The model that answered, as the provider reports it.
    pub model: Option<String>,
    pub input_tokens: u64,
    pub output_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RerankError {
    /// The provider answered with an error, or could not be reached.
    Provider(String),
    /// The provider kept answering 429.
    RateLimited(String),
    /// The call did not finish inside `LAYER_RERANK_TIMEOUT_MS`.
    Timeout,
    /// The call never reached the provider: it waited its whole queue
    /// allowance for a gateway-wide in-flight permit.
    QueueTimeout,
}

impl RerankError {
    /// The `rerank.reason` echo and the `hevlayer_rerank_degraded_total`
    /// label. A closed set, so it is safe as a metric label.
    pub fn reason(&self) -> &'static str {
        match self {
            Self::Provider(_) => "provider_error",
            Self::RateLimited(_) => "rate_limited",
            Self::Timeout => "timeout",
            Self::QueueTimeout => "queue_timeout",
        }
    }
}

impl std::fmt::Display for RerankError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Provider(message) => write!(f, "rerank provider error: {message}"),
            Self::RateLimited(message) => write!(f, "rerank provider rate limited: {message}"),
            Self::Timeout => f.write_str("rerank provider call timed out"),
            Self::QueueTimeout => {
                f.write_str("rerank call timed out waiting for an in-flight permit")
            }
        }
    }
}

#[async_trait]
pub trait RerankProvider: Send + Sync {
    fn name(&self) -> &'static str;

    /// Score one batch. The key travels with the call so a per-namespace
    /// credential needs no second provider instance.
    async fn rerank(
        &self,
        key: &RerankKey,
        request: &RerankRequest<'_>,
    ) -> Result<RerankBatch, RerankError>;
}

// --- Jev ---

/// Jev over TypeSafe's `POST /v1/systemone`.
pub struct JevProvider {
    client: reqwest::Client,
    base_url: String,
}

impl JevProvider {
    pub fn new(base_url: &str, timeout: Duration) -> Result<Self, String> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .user_agent(format!("hevlayer-gateway/{}", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|error| format!("build rerank HTTP client: {error}"))?;
        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
        })
    }
}

/// Serializes `(attribute, text)` fields as a JSON object in the given order
/// (title first), which `serde_json::Map` would re-sort.
struct OrderedFields<'a>(&'a [(String, String)]);

impl Serialize for OrderedFields<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (name, text) in self.0 {
            map.serialize_entry(name, text)?;
        }
        map.end()
    }
}

#[derive(Serialize)]
struct JevState<'a> {
    query: &'a str,
    documents: BTreeMap<String, OrderedFields<'a>>,
}

#[derive(Serialize)]
struct JevCriteria<'a> {
    #[serde(rename = "true")]
    yes: &'a str,
    #[serde(rename = "false")]
    no: &'a str,
}

#[derive(Serialize)]
struct JevNoul<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    instructions: String,
    criteria: JevCriteria<'a>,
}

#[derive(Serialize)]
struct JevRequestBody<'a> {
    state: JevState<'a>,
    model: &'a str,
    questions: BTreeMap<String, JevNoul<'a>>,
}

#[derive(Deserialize)]
struct JevAnswer {
    noul: Option<f64>,
}

#[derive(Deserialize, Default)]
struct JevUsage {
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
}

#[derive(Deserialize)]
struct JevResponseBody {
    model: Option<String>,
    #[serde(default)]
    usage: JevUsage,
    #[serde(default)]
    answers: HashMap<String, JevAnswer>,
}

/// hev-rerank keys candidates `D00`, `D01`, ... in shortlist order; question
/// ids match the document keys.
fn document_key(index: usize) -> String {
    format!("D{index:02}")
}

fn jev_request_body<'a>(request: &'a RerankRequest<'a>) -> JevRequestBody<'a> {
    let mut documents = BTreeMap::new();
    let mut questions = BTreeMap::new();
    for (index, document) in request.documents.iter().enumerate() {
        let key = document_key(index);
        questions.insert(
            key.clone(),
            JevNoul {
                kind: "noul",
                instructions: request.question.instructions.replace("{id}", &key),
                criteria: JevCriteria {
                    yes: request.question.criteria_true,
                    no: request.question.criteria_false,
                },
            },
        );
        documents.insert(key, OrderedFields(&document.fields));
    }
    JevRequestBody {
        state: JevState {
            query: request.query,
            documents,
        },
        model: request.model,
        questions,
    }
}

fn retry_after(headers: &reqwest::header::HeaderMap, attempt: u32) -> Duration {
    let millis = headers
        .get("retry-after-ms")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<f64>().ok());
    let seconds = headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<f64>().ok())
        .map(|seconds| seconds * 1_000.0);
    let hinted = millis
        .or(seconds)
        .filter(|ms| ms.is_finite() && *ms >= 0.0)
        .map(|ms| Duration::from_millis(ms as u64));
    hinted
        .unwrap_or(RATE_LIMIT_BACKOFF_FLOOR * 2u32.pow(attempt))
        .clamp(RATE_LIMIT_BACKOFF_FLOOR, RATE_LIMIT_BACKOFF_CEIL)
}

#[async_trait]
impl RerankProvider for JevProvider {
    fn name(&self) -> &'static str {
        "jev"
    }

    async fn rerank(
        &self,
        key: &RerankKey,
        request: &RerankRequest<'_>,
    ) -> Result<RerankBatch, RerankError> {
        let body = jev_request_body(request);
        let url = format!("{}{SYSTEM_ONE_PATH}", self.base_url);
        let mut attempt = 0;
        let response = loop {
            let response = self
                .client
                .post(&url)
                .bearer_auth(key.expose())
                .json(&body)
                .send()
                .await
                .map_err(|error| {
                    if error.is_timeout() {
                        RerankError::Timeout
                    } else {
                        // `without_url` keeps the message free of anything
                        // but the transport failure.
                        RerankError::Provider(error.without_url().to_string())
                    }
                })?;
            if response.status() != reqwest::StatusCode::TOO_MANY_REQUESTS {
                break response;
            }
            if attempt >= RATE_LIMIT_RETRIES {
                return Err(RerankError::RateLimited(format!(
                    "429 after {} attempts",
                    attempt + 1
                )));
            }
            tokio::time::sleep(retry_after(response.headers(), attempt)).await;
            attempt += 1;
        };
        let status = response.status();
        if !status.is_success() {
            // The body stays in the log. A provider that echoes request
            // state would otherwise reflect candidate text to the caller.
            let detail = response.text().await.unwrap_or_default();
            let detail: String = detail.chars().take(200).collect();
            tracing::warn!(provider = "jev", %status, %detail, "rerank provider error response");
            return Err(RerankError::Provider(format!("jev answered HTTP {status}")));
        }
        let parsed: JevResponseBody = response.json().await.map_err(|error| {
            tracing::warn!(provider = "jev", %error, "rerank provider response did not parse");
            RerankError::Provider("jev answered with an unreadable response".to_string())
        })?;
        let mut scores = Vec::with_capacity(request.documents.len());
        for (index, document) in request.documents.iter().enumerate() {
            let key = document_key(index);
            let probability = parsed
                .answers
                .get(&key)
                .and_then(|answer| answer.noul)
                .filter(|noul| noul.is_finite())
                .ok_or_else(|| {
                    RerankError::Provider(format!("response carries no noul answer for {key}"))
                })?;
            scores.push(RerankScore {
                id: document.id.clone(),
                probability: probability.clamp(0.0, 1.0),
            });
        }
        Ok(RerankBatch {
            scores,
            model: parsed.model,
            input_tokens: parsed.usage.input_tokens.unwrap_or(0),
            output_tokens: parsed.usage.output_tokens.unwrap_or(0),
        })
    }
}

// --- Runtime: key lookup, batching, in-flight cap, deadline ---

/// Everything `/search` needs from the rerank side, owned by `AppState`.
pub struct RerankRuntime {
    provider: Option<Arc<dyn RerankProvider>>,
    default_key: Option<RerankKey>,
    model: String,
    inflight: Arc<Semaphore>,
    timeout: Duration,
}

impl RerankRuntime {
    pub fn new(
        provider: Option<Arc<dyn RerankProvider>>,
        default_key: Option<RerankKey>,
        model: impl Into<String>,
        max_inflight: usize,
        timeout: Duration,
    ) -> Self {
        Self {
            provider,
            default_key,
            model: model.into(),
            inflight: Arc::new(Semaphore::new(max_inflight.max(1))),
            timeout,
        }
    }

    /// No provider and no key: every rerank request answers
    /// `422 rerank_unconfigured`.
    pub fn unconfigured() -> Self {
        Self::new(
            None,
            None,
            DEFAULT_MODEL,
            DEFAULT_MAX_INFLIGHT,
            Duration::from_millis(DEFAULT_TIMEOUT_MS),
        )
    }

    pub fn from_config(config: &crate::config::Config) -> Self {
        let timeout = Duration::from_millis(config.rerank_timeout_ms.max(1));
        let provider: Option<Arc<dyn RerankProvider>> = match config.rerank_provider.as_str() {
            DEFAULT_PROVIDER => match JevProvider::new(&config.rerank_base_url, timeout) {
                Ok(provider) => Some(Arc::new(provider)),
                Err(error) => {
                    tracing::error!(%error, "rerank provider disabled");
                    None
                }
            },
            other => {
                tracing::error!(
                    provider = %other,
                    "unknown LAYER_RERANK_PROVIDER; /search rerank is unconfigured"
                );
                None
            }
        };
        Self::new(
            provider,
            config.rerank_api_key.clone(),
            config.rerank_model.clone(),
            config.rerank_max_inflight,
            timeout,
        )
    }

    pub fn provider(&self) -> Option<&Arc<dyn RerankProvider>> {
        self.provider.as_ref()
    }

    pub fn model(&self) -> &str {
        &self.model
    }

    /// The credential for a namespace. Today that is the gateway-wide
    /// `TYPESAFE_API_KEY`; the Index CR's `search.rerank.apiKeySecretRef`
    /// becomes a second source here without touching the handler.
    pub fn rerank_key_for(&self, _namespace: &str) -> Option<RerankKey> {
        self.default_key.clone()
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct RerankOutcome {
    /// Probability by document id.
    pub scores: HashMap<String, f64>,
    pub model: Option<String>,
    pub calls: usize,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub latency_ms: u64,
}

/// hev-rerank's `_chunk`: split when the call is full by count or by state
/// size, never emitting an empty call.
fn chunk_documents(documents: &[RerankDocument], docs_per_call: usize) -> Vec<&[RerankDocument]> {
    let docs_per_call = docs_per_call.clamp(1, MAX_DOCS_PER_CALL);
    let mut chunks = Vec::new();
    let mut start = 0;
    let mut chars = 0;
    for (index, document) in documents.iter().enumerate() {
        let size = document.chars();
        let count = index - start;
        if count > 0 && (count >= docs_per_call || chars + size > MAX_STATE_CHARS) {
            chunks.push(&documents[start..index]);
            start = index;
            chars = 0;
        }
        chars += size;
    }
    if start < documents.len() {
        chunks.push(&documents[start..]);
    }
    chunks
}

/// One provider call: wait (bounded) for an in-flight permit, then score the
/// chunk under the per-call deadline, which starts once the permit is held.
fn rerank_call<'a>(
    runtime: &'a RerankRuntime,
    provider: &'a Arc<dyn RerankProvider>,
    key: &'a RerankKey,
    query: &'a str,
    question: &'a RerankQuestion,
    chunk: &'a [RerankDocument],
) -> futures::future::BoxFuture<'a, Result<RerankBatch, RerankError>> {
    Box::pin(async move {
        // The queue wait and the provider deadline are separate clocks: a
        // call that never got a permit is a saturated gateway, not a slow
        // provider, and is reported as such.
        let _permit = match tokio::time::timeout(runtime.timeout, runtime.inflight.acquire()).await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => {
                return Err(RerankError::Provider(
                    "rerank in-flight cap closed".to_string(),
                ))
            }
            Err(_) => return Err(RerankError::QueueTimeout),
        };
        let request = RerankRequest {
            query,
            question,
            model: runtime.model(),
            documents: chunk,
        };
        match tokio::time::timeout(runtime.timeout, provider.rerank(key, &request)).await {
            Ok(result) => result,
            Err(_) => Err(RerankError::Timeout),
        }
    })
}

/// Score the whole pool: chunked calls, concurrent, each under the
/// gateway-wide in-flight cap and its own deadline.
pub async fn rerank_pool(
    runtime: &RerankRuntime,
    provider: &Arc<dyn RerankProvider>,
    key: &RerankKey,
    query: &str,
    question: &RerankQuestion,
    documents: &[RerankDocument],
    docs_per_call: usize,
) -> Result<RerankOutcome, RerankError> {
    let started = Instant::now();
    let chunks = chunk_documents(documents, docs_per_call);
    let calls = chunks.len();
    let batches = futures::future::join_all(
        chunks
            .into_iter()
            .map(|chunk| rerank_call(runtime, provider, key, query, question, chunk)),
    )
    .await;

    let mut outcome = RerankOutcome {
        calls,
        ..RerankOutcome::default()
    };
    for batch in batches {
        let batch = batch?;
        outcome.input_tokens += batch.input_tokens;
        outcome.output_tokens += batch.output_tokens;
        if outcome.model.is_none() {
            outcome.model = batch.model;
        }
        for score in batch.scores {
            outcome.scores.insert(score.id, score.probability);
        }
    }
    outcome.latency_ms = started.elapsed().as_millis() as u64;
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn document(id: &str, fields: &[(&str, &str)]) -> RerankDocument {
        RerankDocument {
            id: id.to_string(),
            fields: fields
                .iter()
                .map(|(name, text)| (name.to_string(), text.to_string()))
                .collect(),
        }
    }

    #[test]
    fn jev_body_is_the_hev_rerank_state_shape() {
        let documents = vec![
            document("a", &[("title", "T"), ("text", "body")]),
            document("b", &[("text", "other")]),
        ];
        let request = RerankRequest {
            query: "q",
            question: question(DEFAULT_QUESTION).unwrap(),
            model: "jev-latest",
            documents: &documents,
        };
        let rendered = serde_json::to_string(&jev_request_body(&request)).unwrap();
        // Field order inside a document is the caller's (title first), not
        // alphabetical.
        assert!(
            rendered.contains(r#""D00":{"title":"T","text":"body"}"#),
            "{rendered}"
        );
        let body: Value = serde_json::from_str(&rendered).unwrap();
        assert_eq!(body["model"], "jev-latest");
        assert_eq!(body["state"]["query"], "q");
        assert_eq!(body["state"]["documents"]["D01"], json!({"text": "other"}));
        assert_eq!(body["questions"]["D01"]["type"], "noul");
        assert_eq!(
            body["questions"]["D01"]["instructions"],
            "Document `documents.D01` is relevant to `query`: it contains information that answers or directly addresses it."
        );
        assert!(body["questions"]["D00"]["criteria"]["true"].is_string());
        assert!(body["questions"]["D00"]["criteria"]["false"].is_string());
        assert_eq!(body["questions"].as_object().unwrap().len(), 2);
    }

    #[test]
    fn reserved_attributes_are_never_reranker_text() {
        let row = HashMap::from([
            ("title".to_string(), json!("A title")),
            ("_hevlayer_fetch_count".to_string(), json!("9000")),
            (
                "_hevlayer_upserted_at".to_string(),
                json!(1_700_000_000_000u64),
            ),
            ("year".to_string(), json!(2019)),
            ("tags".to_string(), json!(["x", "y"])),
        ]);
        let attributes: Vec<String> = [
            "_hevlayer_fetch_count",
            "title",
            "_hevlayer_upserted_at",
            "year",
            "tags",
        ]
        .iter()
        .map(|name| name.to_string())
        .collect();
        let fields = reranker_fields(&attributes, &row, 100);
        assert_eq!(
            fields,
            vec![
                ("title".to_string(), "A title".to_string()),
                ("tags".to_string(), "x\ny".to_string()),
            ]
        );
        assert_eq!(
            default_rerank_attributes(&[
                "_hevlayer_note".to_string(),
                "body".to_string(),
                "title".to_string()
            ]),
            vec!["title".to_string(), "body".to_string()]
        );
    }

    #[test]
    fn reranker_text_is_cut_to_the_character_budget_in_order() {
        let row = HashMap::from([
            ("title".to_string(), json!("héllo")),
            ("text".to_string(), json!("world and more")),
        ]);
        let attributes = vec!["title".to_string(), "text".to_string()];
        let fields = reranker_fields(&attributes, &row, 8);
        assert_eq!(
            fields,
            vec![
                ("title".to_string(), "héllo".to_string()),
                ("text".to_string(), "wor".to_string()),
            ]
        );
    }

    #[test]
    fn chunking_splits_by_count_and_by_state_size() {
        let small: Vec<RerankDocument> = (0..50)
            .map(|i| document(&i.to_string(), &[("text", "x")]))
            .collect();
        let sizes: Vec<usize> = chunk_documents(&small, 30)
            .iter()
            .map(|c| c.len())
            .collect();
        assert_eq!(sizes, vec![30, 20]);

        let big_text = "y".repeat(60_000);
        let big: Vec<RerankDocument> = (0..3)
            .map(|i| document(&i.to_string(), &[("text", &big_text)]))
            .collect();
        let sizes: Vec<usize> = chunk_documents(&big, 30).iter().map(|c| c.len()).collect();
        assert_eq!(sizes, vec![1, 1, 1]);
        assert!(chunk_documents(&[], 30).is_empty());
    }

    struct SleepyProvider(Duration);

    #[async_trait]
    impl RerankProvider for SleepyProvider {
        fn name(&self) -> &'static str {
            "sleepy"
        }

        async fn rerank(
            &self,
            _key: &RerankKey,
            request: &RerankRequest<'_>,
        ) -> Result<RerankBatch, RerankError> {
            tokio::time::sleep(self.0).await;
            Ok(RerankBatch {
                scores: request
                    .documents
                    .iter()
                    .map(|document| RerankScore {
                        id: document.id.clone(),
                        probability: 0.5,
                    })
                    .collect(),
                ..RerankBatch::default()
            })
        }
    }

    async fn pool_of(
        calls: usize,
        call: Duration,
        allowance: Duration,
    ) -> Result<RerankOutcome, RerankError> {
        let provider: Arc<dyn RerankProvider> = Arc::new(SleepyProvider(call));
        let runtime = RerankRuntime::new(Some(Arc::clone(&provider)), None, "m", 1, allowance);
        let documents: Vec<RerankDocument> = (0..calls)
            .map(|i| document(&i.to_string(), &[("text", "x")]))
            .collect();
        rerank_pool(
            &runtime,
            &provider,
            &RerankKey::new("k").unwrap(),
            "q",
            question(DEFAULT_QUESTION).unwrap(),
            &documents,
            1,
        )
        .await
    }

    /// Cap 1, 80 ms calls, 100 ms allowance. Queue time and provider time are
    /// separate clocks: the second call waits 80 ms and still gets its full
    /// deadline; the third would wait 160 ms and is a `queue_timeout`, never
    /// a provider `timeout`.
    #[tokio::test(start_paused = true)]
    async fn queue_wait_and_provider_deadline_are_separate_clocks() {
        let call = Duration::from_millis(80);
        let allowance = Duration::from_millis(100);
        let two = pool_of(2, call, allowance)
            .await
            .expect("second call is not a timeout");
        assert_eq!(two.calls, 2);
        let three = pool_of(3, call, allowance).await.unwrap_err();
        assert_eq!(three, RerankError::QueueTimeout);
        assert_eq!(three.reason(), "queue_timeout");
        let slow = pool_of(1, Duration::from_millis(150), allowance)
            .await
            .unwrap_err();
        assert_eq!(slow, RerankError::Timeout);
    }

    #[test]
    fn key_debug_is_redacted() {
        let key = RerankKey::new("  sk-secret  ").unwrap();
        assert_eq!(format!("{key:?}"), "RerankKey(***)");
        assert_eq!(key.expose(), "sk-secret");
        assert!(RerankKey::new("   ").is_none());
    }
}
