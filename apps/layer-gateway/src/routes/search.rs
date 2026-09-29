//! `POST /v2/namespaces/{namespace}/search` (RFC 0116, phase 1).
//!
//! One Layer-native call that composes pieces the gateway already owns:
//! embed the query from the schema's `embed:` attribute, run an ANN leg plus
//! BM25 and fuzzy legs over every full-text attribute, scatter across shards
//! under one stable-read cut, fuse with RRF, cut to `pool` (the L1 seam), and
//! rerank the pool to a calibrated probability per row.
//!
//! This is the degenerate cascade of the amended RFC: no planner (the `plan`
//! echo always reports `unconfigured`), an identity L1 scorer, RRF pool →
//! rerank. The seams the later stages need are here so they add behaviour,
//! not wire breaks: [`LegPlanBuilder`] accepts additional legs, dispatch is
//! "calls of at most 16 subqueries, issued concurrently, fused together", and
//! [`L1Scorer`] sits between fuse and rerank over an explicit feature vector.
//!
//! `/query`, `HybridText` and `Auto` are not touched; nothing here is on
//! their path.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue};
use axum::response::{IntoResponse, Response};
use axum::Json;
use dashmap::DashMap;
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use tracing::warn;

use crate::clients::turbopuffer::{TurbopufferError, UPSERTED_AT_ATTR};
use crate::error::AppError;
use crate::history::{
    log_search_history, now_timestamp, tags_from_headers, traceparent_for_query, TRACEPARENT_HEADER,
};
use crate::metrics::{STATUS_LAYER_ERROR, STATUS_OK, STATUS_TPUF_ERROR};
use crate::models::{IncludeAttributes, QueryResult, SearchHistoryEntry};
use crate::rerank::{
    default_rerank_attributes, is_reserved_attribute, question, rerank_pool, reranker_fields,
    RerankDocument, RerankKey, RerankProvider, RerankQuestion, RerankRuntime,
    DEFAULT_DOCS_PER_CALL, DEFAULT_MAX_CHARS, DEFAULT_PROVIDER, DEFAULT_QUESTION,
    MAX_DOCS_PER_CALL, MAX_MAX_CHARS,
};
use crate::routes::hybrid_text::{
    collect_surfacing_leg, default_per_leg_limit, is_unsupported_by_store, parse_fuzziness,
    parse_stopwords, scatter_leg, tokenize_query_input, tokenize_query_input_with_stopwords,
    Fuzziness, LegSpec, StopwordsOption, DEFAULT_RANK_CONSTANT,
};
use crate::routes::query::{
    compose_read_filter, insert_optional_u64_header, LAYER_STABLE_AS_OF_HEADER,
    LAYER_WARNING_HEADER,
};
use crate::routes::query_router::{route_for_tokens, ROUTING_POLICY_VERSION};
use crate::shards::active_shard_count;
use crate::AppState;

/// Legs one request may run. New to `/search`: `HybridText` bounds its
/// expansion by a token policy, not a leg cap.
pub(crate) const LEG_BUDGET: usize = 16;
/// Turbopuffer's multi-query limit, and the unit of dispatch. With the
/// raw-query leg set one call holds the whole budget; planner legs become
/// further calls, not a refactor.
const MAX_SUBQUERIES_PER_CALL: usize = 16;
const DEFAULT_TOP_K: u32 = 10;
const MAX_TOP_K: u32 = 100;
const DEFAULT_POOL: u32 = 50;
const MIN_POOL: u32 = 50;
const MAX_POOL: u32 = 200;
const SCHEMA_TTL: Duration = Duration::from_secs(30);
const ROUTE_NAME: &str = "searchNamespace";
pub const RERANK_DEGRADED_WARNING: &str = "rerank_degraded";

// --- Runtime owned by AppState ---

pub struct SearchRuntime {
    pub rerank: RerankRuntime,
    schemas: DashMap<String, (Instant, Arc<SearchSchema>)>,
}

impl SearchRuntime {
    pub fn new(rerank: RerankRuntime) -> Self {
        Self {
            rerank,
            schemas: DashMap::new(),
        }
    }
}

// --- Request ---

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchRequestBody {
    query: String,
    top_k: Option<u32>,
    filters: Option<Value>,
    include_attributes: Option<Value>,
    pool: Option<u32>,
    embed: Option<EmbedOptionsBody>,
    text: Option<TextOptionsBody>,
    rerank: Option<RerankField>,
    explain: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EmbedOptionsBody {
    attribute: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TextOptionsBody {
    fuzziness: Option<Value>,
    stopwords: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RerankField {
    Enabled(bool),
    Options(RerankOptionsBody),
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RerankOptionsBody {
    provider: Option<String>,
    threshold: Option<f64>,
    attributes: Option<Vec<String>>,
    docs_per_call: Option<usize>,
    max_chars: Option<usize>,
    question: Option<String>,
    required: Option<bool>,
}

#[derive(Debug, Clone)]
struct RerankOptions {
    provider: String,
    threshold: f64,
    attributes: Option<Vec<String>>,
    docs_per_call: usize,
    max_chars: usize,
    question: &'static RerankQuestion,
    required: bool,
}

#[derive(Debug)]
struct SearchRequest {
    query: String,
    top_k: u32,
    filters: Option<Value>,
    include: IncludeAttributes,
    pool: u32,
    embed_attribute: Option<String>,
    fuzziness: Fuzziness,
    stopwords: StopwordsOption,
    /// `None` when the caller sent `rerank: false`.
    rerank: Option<RerankOptions>,
    explain: bool,
}

fn validation(message: impl Into<String>) -> AppError {
    AppError::Validation(message.into())
}

fn parse_request(body: &Value) -> Result<SearchRequest, AppError> {
    let parsed: SearchRequestBody = serde_json::from_value(body.clone())
        .map_err(|error| validation(format!("invalid search request: {error}")))?;
    if parsed.query.trim().is_empty() {
        return Err(validation("`query` must be a non-empty string"));
    }
    let pool = parsed.pool.unwrap_or(DEFAULT_POOL);
    if !(MIN_POOL..=MAX_POOL).contains(&pool) {
        return Err(validation(format!(
            "`pool` must be between {MIN_POOL} and {MAX_POOL}"
        )));
    }
    let top_k = parsed.top_k.unwrap_or(DEFAULT_TOP_K);
    if !(1..=MAX_TOP_K).contains(&top_k) {
        return Err(validation(format!(
            "`top_k` must be between 1 and {MAX_TOP_K}"
        )));
    }
    if top_k > pool {
        return Err(validation("`top_k` must not exceed `pool`"));
    }
    let include = match parsed.include_attributes {
        None => IncludeAttributes::All(true),
        Some(value) => serde_json::from_value(value)
            .map_err(|error| validation(format!("invalid include_attributes: {error}")))?,
    };
    let (fuzziness, stopwords) = match parsed.text {
        None => (Fuzziness::Auto, StopwordsOption::default()),
        Some(text) => (
            text.fuzziness
                .as_ref()
                .map(parse_fuzziness)
                .transpose()?
                .unwrap_or(Fuzziness::Auto),
            text.stopwords
                .as_ref()
                .map(parse_stopwords)
                .transpose()?
                .unwrap_or_default(),
        ),
    };
    let rerank = match parsed.rerank {
        Some(RerankField::Enabled(false)) => None,
        Some(RerankField::Enabled(true)) | None => {
            Some(rerank_options(RerankOptionsBody::default())?)
        }
        Some(RerankField::Options(options)) => Some(rerank_options(options)?),
    };
    Ok(SearchRequest {
        query: parsed.query,
        top_k,
        filters: parsed.filters,
        include,
        pool,
        embed_attribute: parsed.embed.and_then(|embed| embed.attribute),
        fuzziness,
        stopwords,
        rerank,
        explain: parsed.explain.unwrap_or(false),
    })
}

fn rerank_options(body: RerankOptionsBody) -> Result<RerankOptions, AppError> {
    let provider = body
        .provider
        .unwrap_or_else(|| DEFAULT_PROVIDER.to_string());
    if provider != DEFAULT_PROVIDER {
        return Err(validation(format!(
            "unknown rerank provider `{provider}`; `{DEFAULT_PROVIDER}` is the only provider"
        )));
    }
    let threshold = body.threshold.unwrap_or(0.0);
    if !(0.0..=1.0).contains(&threshold) {
        return Err(validation("`rerank.threshold` must be between 0 and 1"));
    }
    let docs_per_call = body.docs_per_call.unwrap_or(DEFAULT_DOCS_PER_CALL);
    if !(1..=MAX_DOCS_PER_CALL).contains(&docs_per_call) {
        return Err(validation(format!(
            "`rerank.docs_per_call` must be between 1 and {MAX_DOCS_PER_CALL}"
        )));
    }
    let max_chars = body.max_chars.unwrap_or(DEFAULT_MAX_CHARS);
    if !(1..=MAX_MAX_CHARS).contains(&max_chars) {
        return Err(validation(format!(
            "`rerank.max_chars` must be between 1 and {MAX_MAX_CHARS}"
        )));
    }
    let version = body.question.as_deref().unwrap_or(DEFAULT_QUESTION);
    let question = question(version)
        .ok_or_else(|| validation(format!("unknown rerank question `{version}`")))?;
    if let Some(attributes) = body.attributes.as_ref() {
        if attributes.is_empty() {
            return Err(validation("`rerank.attributes` must not be empty"));
        }
        if let Some(reserved) = attributes.iter().find(|name| is_reserved_attribute(name)) {
            return Err(validation(format!(
                "`rerank.attributes` must not name reserved attribute `{reserved}`"
            )));
        }
    }
    Ok(RerankOptions {
        provider,
        threshold,
        attributes: body.attributes,
        docs_per_call,
        max_chars,
        question,
        required: body.required.unwrap_or(false),
    })
}

// --- Namespace search schema ---

/// What `/search` needs from a namespace schema. Net-new: nothing else in
/// the gateway enumerates full-text attributes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SearchSchema {
    /// Attributes that declare `embed:`, sorted.
    pub embed_attributes: Vec<String>,
    /// Attributes with `full_text_search` set, in schema order. Reserved
    /// `_hevlayer_*` attributes are never listed, so they are never a text
    /// leg.
    pub full_text_attributes: Vec<String>,
    /// Every non-vector, non-reserved attribute the schema lists. When
    /// known, "all attributes" is asked of the store as this explicit list,
    /// so a leg never hauls vector columns back. Empty when the store does
    /// not report an attribute schema.
    pub returnable_attributes: Vec<String>,
}

/// Read the two attribute sets out of a store's metadata body. Turbopuffer
/// (and pgvector) report `schema` as an object keyed by attribute; hev search
/// reports a field list and has one full-text column, `text`.
pub(crate) fn schema_from_metadata(metadata: &Value, search_store: bool) -> SearchSchema {
    let mut schema = SearchSchema::default();
    match metadata.get("schema") {
        Some(Value::Object(attributes)) => {
            for (name, declaration) in attributes {
                if is_reserved_attribute(name) {
                    continue;
                }
                let kind = declaration
                    .as_str()
                    .or_else(|| declaration.get("type").and_then(Value::as_str))
                    .unwrap_or_default();
                if !kind.starts_with('[') && name != "vector" {
                    schema.returnable_attributes.push(name.clone());
                }
                let full_text = declaration
                    .get("full_text_search")
                    .is_some_and(|value| !value.is_null() && *value != Value::Bool(false));
                if full_text {
                    schema.full_text_attributes.push(name.clone());
                }
                if declaration
                    .get("embed")
                    .is_some_and(|embed| !embed.is_null())
                {
                    schema.embed_attributes.push(name.clone());
                }
            }
        }
        Some(Value::Array(fields)) if search_store => {
            let has_text = fields
                .iter()
                .any(|field| field.get("name").and_then(Value::as_str) == Some("text"));
            if has_text {
                schema.full_text_attributes.push("text".to_string());
            }
        }
        None if search_store => schema.full_text_attributes.push("text".to_string()),
        _ => {}
    }
    schema
}

async fn resolve_schema(state: &AppState, namespace: &str) -> Result<Arc<SearchSchema>, AppError> {
    if let Some(entry) = state.search.schemas.get(namespace) {
        if entry.0.elapsed() < SCHEMA_TTL {
            return Ok(Arc::clone(&entry.1));
        }
    }
    let metadata = state
        .turbopuffer()
        .head_namespace(namespace)
        .await
        .map_err(|error| {
            if error.is_not_found() {
                AppError::NotFound(format!("namespace `{namespace}` not found"))
            } else {
                AppError::from_turbopuffer(error, "namespace metadata read failed")
            }
        })?;
    let mut schema =
        schema_from_metadata(&metadata.raw, state.namespace_uses_search_store(namespace));
    for source in crate::routes::embed_wire::declared_embed_sources(state, namespace).await? {
        if !schema.embed_attributes.contains(&source) {
            schema.embed_attributes.push(source);
        }
    }
    schema.embed_attributes.sort();
    let schema = Arc::new(schema);
    // Cache only a schema `/search` can run on, so adding the missing
    // attribute takes effect on the next request rather than after the TTL.
    if !schema.embed_attributes.is_empty() && !schema.full_text_attributes.is_empty() {
        state
            .search
            .schemas
            .insert(namespace.to_string(), (Instant::now(), Arc::clone(&schema)));
    }
    Ok(schema)
}

fn choose_embed_attribute(
    namespace: &str,
    schema: &SearchSchema,
    requested: Option<&str>,
) -> Result<String, AppError> {
    if schema.embed_attributes.is_empty() {
        return Err(AppError::SearchRejected {
            code: "embed_attribute_missing",
            message: format!(
                "namespace `{namespace}` declares no `embed:` attribute; /search embeds the query from the schema. Add `embed` to a text attribute in the namespace schema."
            ),
        });
    }
    match requested {
        Some(name) if schema.embed_attributes.iter().any(|a| a == name) => Ok(name.to_string()),
        Some(name) => Err(AppError::SearchRejected {
            code: "embed_attribute_invalid",
            message: format!(
                "`embed.attribute` names `{name}`, which does not declare `embed:` in namespace `{namespace}`"
            ),
        }),
        None if schema.embed_attributes.len() == 1 => Ok(schema.embed_attributes[0].clone()),
        None => Err(AppError::SearchRejected {
            code: "embed_attribute_invalid",
            message: format!(
                "namespace `{namespace}` declares several `embed:` attributes ({}); name one in `embed.attribute`",
                schema.embed_attributes.join(", ")
            ),
        }),
    }
}

// --- Legs ---

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LegKind {
    Ann,
    Bm25,
    Fuzzy,
}

#[derive(Debug, Clone)]
pub(crate) struct SearchLeg {
    pub kind: LegKind,
    pub attribute: Option<String>,
    /// The fuzzy leg's token; surfacing re-ranks by distance to it.
    pub token: Option<String>,
    pub spec: LegSpec,
}

#[derive(Debug, Default)]
pub(crate) struct LegPlan {
    pub legs: Vec<SearchLeg>,
    pub dropped: usize,
}

impl LegPlan {
    /// Dispatch units: at most [`MAX_SUBQUERIES_PER_CALL`] legs each, issued
    /// concurrently and fused together. One call today.
    fn calls(&self) -> Vec<&[SearchLeg]> {
        self.legs.chunks(MAX_SUBQUERIES_PER_CALL).collect()
    }
}

/// Collects candidate legs in three tiers and spends the budget in the RFC's
/// order: ANN legs, then BM25 breadth, then fuzzy depth. A later plan adds
/// rewrites, an `ann_text` leg or a filtered leg through the same `add_*`
/// calls; nothing downstream assumes the raw-query leg set.
#[derive(Default)]
pub(crate) struct LegPlanBuilder {
    ann: Vec<SearchLeg>,
    bm25: Vec<SearchLeg>,
    /// One queue per attribute, drained round-robin.
    fuzzy: Vec<Vec<SearchLeg>>,
    /// Legs a store cannot serve at all (pgvector fuzzy).
    unsupported: usize,
}

impl LegPlanBuilder {
    pub fn add_ann(
        &mut self,
        label: &str,
        target: &str,
        attribute: &str,
        vector: &[f64],
        filter: Option<&Value>,
    ) {
        self.ann.push(SearchLeg {
            kind: LegKind::Ann,
            attribute: Some(attribute.to_string()),
            token: None,
            spec: LegSpec {
                label: label.to_string(),
                rank_by: json!([target, "ANN", vector]),
                filter: filter.cloned(),
            },
        });
    }

    pub fn add_bm25(&mut self, attribute: &str, input: &str, filter: Option<&Value>) {
        if is_reserved_attribute(attribute) {
            return;
        }
        self.bm25.push(SearchLeg {
            kind: LegKind::Bm25,
            attribute: Some(attribute.to_string()),
            token: None,
            spec: LegSpec {
                label: format!("bm25:{attribute}"),
                rank_by: json!([attribute, "BM25", input]),
                filter: filter.cloned(),
            },
        });
    }

    /// Fuzzy legs for one attribute, in token order. Each is BM25-ranked over
    /// the whole input and filtered to rows fuzzy-matching its token, the
    /// `HybridText` leg shape.
    pub fn add_fuzzy(
        &mut self,
        attribute: &str,
        input: &str,
        tokens: &[String],
        fuzziness: Fuzziness,
        filter: Option<&Value>,
    ) {
        if is_reserved_attribute(attribute) {
            return;
        }
        let max_edit_distance = fuzziness.max_edit_distance();
        let queue = tokens
            .iter()
            .map(|token| {
                let fuzzy = json!([
                    attribute,
                    "Fuzzy",
                    token,
                    {"max_edit_distance": max_edit_distance.clone()}
                ]);
                let filter = match filter {
                    Some(base) => json!(["And", [base.clone(), fuzzy]]),
                    None => fuzzy,
                };
                SearchLeg {
                    kind: LegKind::Fuzzy,
                    attribute: Some(attribute.to_string()),
                    token: Some(token.clone()),
                    spec: LegSpec {
                        label: format!("fuzzy:{attribute}:{token}"),
                        rank_by: json!([attribute, "BM25", input]),
                        filter: Some(filter),
                    },
                }
            })
            .collect();
        self.fuzzy.push(queue);
    }

    /// The fuzzy tier for a store whose fuzzy match takes the whole query
    /// text rather than one token (hev search: the adapter reads only
    /// `max_edit_distance` from a `Fuzzy` filter and sends the query text).
    /// Per-token legs would be identical engine requests there, so the tier
    /// is at most one leg per attribute, labelled without a token. At edit
    /// distance 0 that one leg would repeat the BM25 leg, so none is built.
    /// Returns how many legs were built; the caller counts the rest as not run.
    pub fn add_whole_query_fuzzy(
        &mut self,
        attribute: &str,
        input: &str,
        fuzziness: Fuzziness,
        filter: Option<&Value>,
    ) -> usize {
        if is_reserved_attribute(attribute) || fuzziness == Fuzziness::Fixed(0) {
            return 0;
        }
        let fuzzy = json!([
            attribute,
            "Fuzzy",
            input,
            {"max_edit_distance": fuzziness.max_edit_distance()}
        ]);
        let filter = match filter {
            Some(base) => json!(["And", [base.clone(), fuzzy]]),
            None => fuzzy,
        };
        self.fuzzy.push(vec![SearchLeg {
            kind: LegKind::Fuzzy,
            attribute: Some(attribute.to_string()),
            token: None,
            spec: LegSpec {
                label: format!("fuzzy:{attribute}"),
                rank_by: json!([attribute, "BM25", input]),
                filter: Some(filter),
            },
        }]);
        1
    }

    pub fn add_unsupported(&mut self, count: usize) {
        self.unsupported += count;
    }

    pub fn build(self, budget: usize) -> LegPlan {
        let mut fuzzy_round_robin = Vec::new();
        let depth = self.fuzzy.iter().map(Vec::len).max().unwrap_or(0);
        let mut queues: Vec<std::vec::IntoIter<SearchLeg>> =
            self.fuzzy.into_iter().map(Vec::into_iter).collect();
        for _ in 0..depth {
            for queue in &mut queues {
                if let Some(leg) = queue.next() {
                    fuzzy_round_robin.push(leg);
                }
            }
        }
        let mut legs: Vec<SearchLeg> = self
            .ann
            .into_iter()
            .chain(self.bm25)
            .chain(fuzzy_round_robin)
            .collect();
        let dropped = legs.len().saturating_sub(budget) + self.unsupported;
        legs.truncate(budget);
        LegPlan { legs, dropped }
    }
}

/// The read cut every leg shares: taken once per request.
struct ReadCut {
    watermark: Option<u64>,
    inject_filter: bool,
}

struct LegContext<'a> {
    state: &'a AppState,
    namespace: &'a str,
    cut: &'a ReadCut,
    per_leg_limit: u64,
    include: Option<&'a IncludeAttributes>,
    shard_count: Option<u64>,
    threads: u32,
    /// `Some` when re-running fuzzy legs for the surfacing fallback.
    surfacing: bool,
}

async fn query_leg(
    ctx: &LegContext<'_>,
    leg: &SearchLeg,
    filter: Option<&Value>,
) -> Result<Vec<QueryResult>, TurbopufferError> {
    if ctx.surfacing {
        let attribute = leg.attribute.as_deref().unwrap_or_default();
        let token = leg.token.as_deref().unwrap_or_default();
        let all = IncludeAttributes::All(true);
        return collect_surfacing_leg(
            ctx.state,
            ctx.namespace,
            &leg.spec,
            token,
            attribute,
            filter,
            ctx.per_leg_limit,
            ctx.include.unwrap_or(&all),
            ctx.shard_count,
            ctx.threads,
        )
        .await;
    }
    match ctx.shard_count {
        Some(_) => {
            scatter_leg(
                ctx.state,
                ctx.namespace,
                &leg.spec,
                filter,
                ctx.per_leg_limit,
                ctx.include,
                ctx.threads,
            )
            .await
        }
        None => ctx
            .state
            .turbopuffer()
            .ranked_query(
                ctx.namespace,
                &leg.spec.rank_by,
                ctx.per_leg_limit as u32,
                filter,
                ctx.include,
            )
            .await
            .map(|outcome| outcome.rows),
    }
}

/// `Ok(None)`: a fuzzy leg the store cannot serve, dropped and counted.
async fn run_leg(
    ctx: &LegContext<'_>,
    leg: &SearchLeg,
) -> Result<Option<Vec<QueryResult>>, AppError> {
    let first_watermark = ctx.cut.inject_filter.then_some(ctx.cut.watermark).flatten();
    let filter = compose_read_filter(leg.spec.filter.as_ref(), None, first_watermark);
    match query_leg(ctx, leg, filter.as_ref()).await {
        Ok(rows) => Ok(Some(rows)),
        Err(error) if error.is_rate_limited() && !ctx.cut.inject_filter => {
            warn!(
                namespace = %ctx.namespace,
                leg = %leg.spec.label,
                %error,
                "429 on unfiltered search leg; retrying with watermark filter",
            );
            let retry = compose_read_filter(leg.spec.filter.as_ref(), None, ctx.cut.watermark);
            query_leg(ctx, leg, retry.as_ref())
                .await
                .map(Some)
                .map_err(|e| AppError::from_turbopuffer(e, "search leg failed (retry)"))
        }
        Err(error) if leg.kind == LegKind::Fuzzy && is_unsupported_by_store(&error) => {
            warn!(
                namespace = %ctx.namespace,
                leg = %leg.spec.label,
                %error,
                "Skipping search fuzzy leg unsupported by VectorStore"
            );
            Ok(None)
        }
        Err(error) if is_unsupported_by_store(&error) => Err(AppError::from_store_support_error(
            format!("{error} (leg `{}`)", leg.spec.label),
            Some(
                ctx.state
                    .turbopuffer()
                    .capabilities_for_namespace(ctx.namespace)
                    .kind
                    .to_string(),
            ),
            Some(ROUTE_NAME.to_string()),
        )),
        Err(error) => Err(AppError::from_turbopuffer(error, "search leg failed")),
    }
}

pub(crate) struct LegRun {
    leg: SearchLeg,
    rows: Vec<QueryResult>,
}

type LegOutcome<'a> = Result<(&'a SearchLeg, Option<Vec<QueryResult>>), AppError>;

fn run_leg_boxed<'a>(
    ctx: &'a LegContext<'a>,
    leg: &'a SearchLeg,
) -> futures::future::BoxFuture<'a, LegOutcome<'a>> {
    Box::pin(async move { run_leg(ctx, leg).await.map(|rows| (leg, rows)) })
}

/// One dispatch unit: its legs in order, at most `leg_concurrency` in flight.
fn run_call<'a>(
    ctx: &'a LegContext<'a>,
    call: &'a [SearchLeg],
    leg_concurrency: usize,
) -> futures::future::BoxFuture<'a, Vec<LegOutcome<'a>>> {
    Box::pin(
        stream::iter(call.iter().map(move |leg| run_leg_boxed(ctx, leg)))
            .buffered(leg_concurrency)
            .collect::<Vec<_>>(),
    )
}

/// Run every call of the plan concurrently. Inside a call, leg concurrency is
/// bounded so the upstream in-flight count stays near one multi-query call's
/// worth (16) whether or not the namespace is sharded.
async fn dispatch<'a>(
    ctx: &'a LegContext<'a>,
    plan: &'a LegPlan,
) -> Result<(Vec<LegRun>, usize), AppError> {
    let leg_concurrency = match ctx.shard_count {
        Some(_) => (MAX_SUBQUERIES_PER_CALL / ctx.threads.max(1) as usize).max(1),
        None => MAX_SUBQUERIES_PER_CALL,
    };
    let calls = futures::future::join_all(
        plan.calls()
            .into_iter()
            .map(|call| run_call(ctx, call, leg_concurrency)),
    )
    .await;
    let mut runs = Vec::with_capacity(plan.legs.len());
    let mut unsupported = 0;
    for outcome in calls.into_iter().flatten() {
        match outcome? {
            (leg, Some(rows)) => runs.push(LegRun {
                leg: leg.clone(),
                rows,
            }),
            (_, None) => unsupported += 1,
        }
    }
    Ok((runs, unsupported))
}

// --- Fuse ---

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct LegFeature {
    pub label: String,
    /// 1-based rank within the leg after the shard merge.
    pub rank: u64,
    /// The leg's own score (BM25 score or ANN distance).
    pub score: Option<f64>,
}

#[derive(Debug, Clone)]
pub(crate) struct Candidate {
    pub id: String,
    pub numeric_id: bool,
    pub rrf_sum: f64,
    /// First occurrence's attributes are kept.
    pub attributes: HashMap<String, Value>,
    pub legs: Vec<LegFeature>,
}

/// Reciprocal rank fusion over every leg, `Σ 1/(rank_constant + rank)`. The
/// same id in several legs (or shards of one leg, already merged) accumulates:
/// that is the fusion signal, not a duplicate. Order: score descending, id
/// ascending, the `HybridText` tie-break.
pub(crate) fn rrf_fuse(runs: &[LegRun], rank_constant: u64) -> Vec<Candidate> {
    let mut order: Vec<String> = Vec::new();
    let mut fused: HashMap<String, Candidate> = HashMap::new();
    for run in runs {
        for (index, row) in run.rows.iter().enumerate() {
            let rank = index as u64 + 1;
            let candidate = fused.entry(row.id.clone()).or_insert_with(|| {
                order.push(row.id.clone());
                Candidate {
                    id: row.id.clone(),
                    numeric_id: row.numeric_id,
                    rrf_sum: 0.0,
                    attributes: row.attributes.clone(),
                    legs: Vec::new(),
                }
            });
            candidate.rrf_sum += 1.0 / (rank_constant as f64 + rank as f64);
            candidate.legs.push(LegFeature {
                label: run.leg.spec.label.clone(),
                rank,
                score: row.dist,
            });
        }
    }
    let mut candidates: Vec<Candidate> = order
        .into_iter()
        .filter_map(|id| fused.remove(&id))
        .collect();
    candidates.sort_by(|a, b| {
        b.rrf_sum
            .partial_cmp(&a.rrf_sum)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.id.cmp(&b.id))
    });
    candidates
}

// --- L1 seam ---

/// The per-row feature vector the L1 stage scores. An absent feature is
/// absent, not zero.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct L1Features {
    pub rrf_sum: f64,
    /// 30-day fetch count (LYR-92). No counter exists on this base, so the
    /// feature is absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fetch_count_30d: Option<f64>,
    /// Now minus `_hevlayer_upserted_at`, when the row carries the stamp.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age_seconds: Option<f64>,
    #[serde(skip)]
    pub legs: Vec<LegFeature>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct L1Contributions {
    pub rrf_sum: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fetch_count_30d: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age_seconds: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct L1Score {
    pub score: f64,
    pub contributions: L1Contributions,
}

/// Between fuse and rerank. Phase 1 ships [`IdentityL1`]; the hand-tuned
/// boost and the learned ranker arrive behind this trait.
pub(crate) trait L1Scorer: Send + Sync {
    fn score(&self, features: &L1Features) -> L1Score;
}

/// RRF order, unchanged: the score is `rrf_sum` and every other term
/// contributes zero.
pub(crate) struct IdentityL1;

impl L1Scorer for IdentityL1 {
    fn score(&self, features: &L1Features) -> L1Score {
        L1Score {
            score: features.rrf_sum,
            contributions: L1Contributions {
                rrf_sum: features.rrf_sum,
                fetch_count_30d: features.fetch_count_30d.map(|_| 0.0),
                age_seconds: features.age_seconds.map(|_| 0.0),
            },
        }
    }
}

fn l1_features(candidate: &Candidate, now_ms: u64) -> L1Features {
    let age_seconds = candidate
        .attributes
        .get(UPSERTED_AT_ATTR)
        .and_then(Value::as_u64)
        .map(|stamp| now_ms.saturating_sub(stamp) as f64 / 1_000.0);
    L1Features {
        rrf_sum: candidate.rrf_sum,
        fetch_count_30d: None,
        age_seconds,
        legs: candidate.legs.clone(),
    }
}

struct Scored {
    candidate: Candidate,
    features: L1Features,
    l1: L1Score,
}

/// Score the fused list and cut it to `pool`. The sort is stable, so a
/// scorer that returns `rrf_sum` leaves the fused order exactly as it was.
fn l1_cut(
    scorer: &dyn L1Scorer,
    candidates: Vec<Candidate>,
    pool: usize,
    now_ms: u64,
) -> Vec<Scored> {
    let mut scored: Vec<Scored> = candidates
        .into_iter()
        .map(|candidate| {
            let features = l1_features(&candidate, now_ms);
            let l1 = scorer.score(&features);
            Scored {
                candidate,
                features,
                l1,
            }
        })
        .collect();
    scored.sort_by(|a, b| {
        b.l1.score
            .partial_cmp(&a.l1.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    scored.truncate(pool);
    scored
}

// --- Response ---

#[derive(Debug, Serialize)]
struct RoutingEcho {
    route: &'static str,
    policy: &'static str,
    tokens: usize,
    executed: bool,
    advisory: bool,
}

#[derive(Debug, Serialize)]
struct PlanEcho {
    executed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
}

#[derive(Debug, Serialize)]
struct LegEcho {
    label: String,
    kind: LegKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    attribute: Option<String>,
    rows: usize,
}

#[derive(Debug, Serialize)]
struct HybridEcho {
    tokens: Vec<String>,
    tokens_dropped: usize,
    stopwords: Value,
    stopwords_dropped: Vec<String>,
    fuzziness: Value,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    fuzziness_clamped: bool,
    rank_constant: u64,
    per_leg_limit: u64,
    legs: Vec<LegEcho>,
    dropped_legs: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    threads: Option<u32>,
    surfaced: bool,
}

#[derive(Debug, Serialize)]
struct RerankEcho {
    provider: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<String>,
    question: &'static str,
    executed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
    pool: usize,
    calls: usize,
    docs_per_call: usize,
    threshold: f64,
    pruned: usize,
    input_tokens: u64,
    latency_ms: u64,
}

#[derive(Debug, Default, Serialize)]
struct Performance {
    #[serde(skip_serializing_if = "Option::is_none")]
    embedding_tokens: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    embedding_ms: Option<f64>,
    legs_ms: u64,
    fuse_ms: u64,
    l1_ms: u64,
    rerank_ms: u64,
    total_ms: u64,
}

#[derive(Debug, Serialize)]
struct RowExplain {
    features: L1Features,
    contributions: L1Contributions,
    l1_score: f64,
    legs: Vec<LegFeature>,
}

#[derive(Debug, Serialize)]
struct SearchRow {
    id: Value,
    score: f64,
    attributes: Map<String, Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    explain: Option<RowExplain>,
}

#[derive(Debug, Serialize)]
struct SearchResponse {
    rows: Vec<SearchRow>,
    routing: RoutingEcho,
    plan: PlanEcho,
    hybrid: HybridEcho,
    rerank: RerankEcho,
    performance: Performance,
}

/// The attributes the caller asked for. Text fetched only for the reranker,
/// and the write stamp fetched only for the L1 feature, are stripped here.
fn response_attributes(
    row: &HashMap<String, Value>,
    include: &IncludeAttributes,
    vector_target: &str,
) -> Map<String, Value> {
    let is_vector = |name: &str| name == "vector" || name == vector_target;
    let mut names: Vec<&String> = match include {
        IncludeAttributes::All(false) => Vec::new(),
        IncludeAttributes::All(true) => row
            .keys()
            .filter(|name| !is_reserved_attribute(name) && !is_vector(name))
            .collect(),
        IncludeAttributes::Fields(fields) => row
            .keys()
            .filter(|name| fields.contains(name) && !is_vector(name))
            .collect(),
    };
    names.sort();
    names
        .into_iter()
        .map(|name| (name.clone(), row[name].clone()))
        .collect()
}

/// What every leg asks the store for: the caller's attributes, the reranker's
/// text, and (where the store stamps rows) the write stamp for the L1 age
/// feature.
fn leg_include(
    include: &IncludeAttributes,
    returnable: &[String],
    rerank_attributes: &[String],
    stamp: bool,
) -> Option<IncludeAttributes> {
    let mut fields: Vec<String> = match include {
        IncludeAttributes::All(true) if returnable.is_empty() => {
            return Some(IncludeAttributes::All(true))
        }
        IncludeAttributes::All(true) => returnable.to_vec(),
        IncludeAttributes::All(false) => Vec::new(),
        IncludeAttributes::Fields(fields) => {
            fields.iter().filter(|f| *f != "vector").cloned().collect()
        }
    };
    for attribute in rerank_attributes {
        if !fields.contains(attribute) {
            fields.push(attribute.clone());
        }
    }
    if stamp && !fields.iter().any(|f| f == UPSERTED_AT_ATTR) {
        fields.push(UPSERTED_AT_ATTR.to_string());
    }
    (!fields.is_empty()).then_some(IncludeAttributes::Fields(fields))
}

/// Native SQL adapters carry wire ids JSON-encoded in the string slot while
/// RRF groups candidates; the response restores the original type.
fn response_id(native_wire: bool, id: &str, numeric_id: bool) -> Value {
    if native_wire || numeric_id {
        if let Ok(value) = serde_json::from_str::<Value>(id) {
            return value;
        }
    }
    Value::String(id.to_string())
}

// --- Handler ---

struct SearchOutcome {
    response: SearchResponse,
    watermark: Option<u64>,
    degraded: bool,
}

pub async fn search(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, AppError> {
    state.telemetry.touch_search();
    let outcome = run_search(&state, &namespace, &body).await;
    let status = match &outcome {
        Ok(_) => STATUS_OK,
        Err(AppError::Validation(_) | AppError::SearchRejected { .. }) => STATUS_LAYER_ERROR,
        Err(AppError::UnsupportedByStore { .. } | AppError::RerankUnavailable(_)) => {
            STATUS_LAYER_ERROR
        }
        Err(_) => STATUS_TPUF_ERROR,
    };
    state.metrics.observe_search(&namespace, status);
    let outcome = outcome?;

    log_history(&state, &namespace, &headers, &body, &outcome);

    let mut response_headers = HeaderMap::new();
    let (traceparent, _) = traceparent_for_query(&headers);
    if let Ok(value) = HeaderValue::from_str(&traceparent) {
        response_headers.insert(TRACEPARENT_HEADER, value);
    }
    insert_optional_u64_header(
        &mut response_headers,
        LAYER_STABLE_AS_OF_HEADER,
        outcome.watermark,
    );
    if outcome.degraded {
        response_headers.insert(
            LAYER_WARNING_HEADER,
            HeaderValue::from_static(RERANK_DEGRADED_WARNING),
        );
    }
    Ok((response_headers, Json(outcome.response)).into_response())
}

/// Everything the rerank stage needs, resolved before any leg runs so an
/// unconfigured key fails fast and costs no store reads.
struct RerankPlan {
    options: RerankOptions,
    provider: Arc<dyn RerankProvider>,
    key: RerankKey,
    attributes: Vec<String>,
}

fn plan_rerank(
    state: &AppState,
    namespace: &str,
    request: &SearchRequest,
    schema: &SearchSchema,
) -> Result<Option<RerankPlan>, AppError> {
    let Some(options) = request.rerank.clone() else {
        return Ok(None);
    };
    let runtime = &state.search.rerank;
    let unconfigured = || {
        AppError::SearchRejected {
        code: "rerank_unconfigured",
        message: "the rerank stage is on and no provider key resolves: set `TYPESAFE_API_KEY` on the gateway (the Index `search.rerank.apiKeySecretRef` is not resolved by this release), or send `rerank: false`".to_string(),
    }
    };
    let provider = runtime.provider().cloned().ok_or_else(unconfigured)?;
    let key = runtime.rerank_key_for(namespace).ok_or_else(unconfigured)?;
    let attributes = options
        .attributes
        .clone()
        .unwrap_or_else(|| default_rerank_attributes(&schema.full_text_attributes));
    Ok(Some(RerankPlan {
        options,
        provider,
        key,
        attributes,
    }))
}

async fn run_search(
    state: &AppState,
    namespace: &str,
    body: &Value,
) -> Result<SearchOutcome, AppError> {
    let started = Instant::now();
    let request = parse_request(body)?;

    let capabilities = state.turbopuffer().capabilities_for_namespace(namespace);
    capabilities
        .require(vectorstore_core::capabilities::WireFeature::Search)
        .map_err(|error| {
            AppError::from_store_support_error(
                error,
                Some(capabilities.kind.to_string()),
                Some(ROUTE_NAME.to_string()),
            )
        })?;

    // Routing is advisory: reported for the UI and the history, never acted
    // on. It tokenizes as `Auto` does, without the stop-word stage.
    let route_tokens = tokenize_query_input(&request.query).tokens.len();
    if route_tokens == 0 {
        return Err(validation(
            "`query` yields no tokens under the tokenizer policy",
        ));
    }
    let routing = RoutingEcho {
        route: route_for_tokens(route_tokens).as_str(),
        policy: ROUTING_POLICY_VERSION,
        tokens: route_tokens,
        executed: true,
        advisory: true,
    };

    let schema = resolve_schema(state, namespace).await?;
    let embed_attribute =
        choose_embed_attribute(namespace, &schema, request.embed_attribute.as_deref())?;
    if schema.full_text_attributes.is_empty() {
        return Err(AppError::SearchRejected {
            code: "full_text_attribute_missing",
            message: format!(
                "namespace `{namespace}` has no `full_text_search` attribute; /search needs at least one for its BM25 legs"
            ),
        });
    }
    let rerank_plan = plan_rerank(state, namespace, &request, &schema)?;

    // Stage: embed.
    let search_store = state.namespace_uses_search_store(namespace);
    let native_wire = state.turbopuffer().requires_native_wire(namespace);
    let embed_started = Instant::now();
    let embedded = crate::routes::embed_wire::resolve_auto_embed(
        state,
        namespace,
        &embed_attribute,
        json!(["Embed", request.query]),
        crate::routes::embed_wire::EmbedStore::for_namespace(state, namespace),
    )
    .await?;
    state
        .metrics
        .observe_search_stage(namespace, "embed", embed_started.elapsed());

    // Stage: legs.
    let policy = tokenize_query_input_with_stopwords(&request.query, &request.stopwords);
    // `/search` inherits the `HybridText` interim clamp for `kind: search`
    // stores as it stands today.
    let clamped = request.fuzziness == Fuzziness::Auto && search_store;
    let fuzziness = if clamped {
        Fuzziness::Fixed(0)
    } else {
        request.fuzziness
    };
    let filter = request.filters.as_ref();
    let mut builder = LegPlanBuilder::default();
    builder.add_ann(
        "ann",
        &embedded.target,
        &embed_attribute,
        &embedded.vector,
        filter,
    );
    for attribute in &schema.full_text_attributes {
        builder.add_bm25(attribute, &request.query, filter);
    }
    for attribute in &schema.full_text_attributes {
        if native_wire {
            // The phase-one SQL bundle serves BM25 and dense legs only.
            builder.add_unsupported(policy.tokens.len());
        } else if search_store {
            // hev search matches fuzzily on the whole query, not per token:
            // one leg at most, and the per-token legs it replaces are
            // reported as not run so RRF does not count one ranking N times.
            let built = builder.add_whole_query_fuzzy(attribute, &request.query, fuzziness, filter);
            builder.add_unsupported(policy.tokens.len().saturating_sub(built));
        } else {
            builder.add_fuzzy(attribute, &request.query, &policy.tokens, fuzziness, filter);
        }
    }
    let plan = builder.build(LEG_BUDGET);

    // One stable-read cut for every leg and every shard.
    let (watermark, inject_filter) = state.query_consistency(namespace);
    let cut = ReadCut {
        watermark,
        inject_filter,
    };
    let shard_count = active_shard_count(state, namespace).await;
    let threads =
        crate::routes::scans::resolve_scan_threads_with_active(state, namespace, None, shard_count);
    let per_leg_limit = default_per_leg_limit(request.pool);
    let rerank_attributes: &[String] = rerank_plan
        .as_ref()
        .map(|plan| plan.attributes.as_slice())
        .unwrap_or_default();
    let include = leg_include(
        &request.include,
        &schema.returnable_attributes,
        rerank_attributes,
        !search_store && !native_wire,
    );
    let ctx = LegContext {
        state,
        namespace,
        cut: &cut,
        per_leg_limit,
        include: include.as_ref(),
        shard_count,
        threads,
        surfacing: false,
    };
    let legs_started = Instant::now();
    let (mut runs, mut unsupported) = dispatch(&ctx, &plan).await?;

    // RFC 0057 surfacing, unchanged in spirit: when every text leg came back
    // empty, the BM25-ranked fuzzy legs cannot score a fully misspelled
    // query, so re-run them ordered by edit distance.
    let text_rows: usize = runs
        .iter()
        .filter(|run| run.leg.kind != LegKind::Ann)
        .map(|run| run.rows.len())
        .sum();
    let fuzzy_legs: Vec<SearchLeg> = plan
        .legs
        .iter()
        // Surfacing re-ranks a leg by distance to its token; a whole-query
        // fuzzy leg has none.
        .filter(|leg| leg.kind == LegKind::Fuzzy && leg.token.is_some())
        .cloned()
        .collect();
    let surfaced = text_rows == 0 && !fuzzy_legs.is_empty();
    if surfaced {
        let surfacing_plan = LegPlan {
            legs: fuzzy_legs
                .into_iter()
                .map(|mut leg| {
                    leg.spec.rank_by = json!(["id", "asc"]);
                    leg
                })
                .collect(),
            dropped: 0,
        };
        // Surfacing orders by the attribute's text, so it must be fetched.
        let surfacing_include = match include.clone() {
            Some(IncludeAttributes::Fields(mut fields)) => {
                for attribute in &schema.full_text_attributes {
                    if !fields.contains(attribute) {
                        fields.push(attribute.clone());
                    }
                }
                Some(IncludeAttributes::Fields(fields))
            }
            Some(all) => Some(all),
            None => Some(IncludeAttributes::Fields(
                schema.full_text_attributes.clone(),
            )),
        };
        let surfacing_ctx = LegContext {
            include: surfacing_include.as_ref(),
            surfacing: true,
            ..ctx
        };
        let (surfaced_runs, surfaced_unsupported) =
            dispatch(&surfacing_ctx, &surfacing_plan).await?;
        runs.retain(|run| run.leg.kind == LegKind::Ann);
        runs.extend(surfaced_runs);
        unsupported += surfaced_unsupported;
    }
    let legs_elapsed = legs_started.elapsed();
    state
        .metrics
        .observe_search_stage(namespace, "legs", legs_elapsed);

    // Stage: fuse.
    let fuse_started = Instant::now();
    let candidates = rrf_fuse(&runs, DEFAULT_RANK_CONSTANT);
    let fuse_elapsed = fuse_started.elapsed();
    state
        .metrics
        .observe_search_stage(namespace, "fuse", fuse_elapsed);

    // Stage: L1 cut.
    let l1_started = Instant::now();
    let now_ms = now_timestamp().1 / 1_000_000;
    let pool = l1_cut(&IdentityL1, candidates, request.pool as usize, now_ms);
    let l1_elapsed = l1_started.elapsed();
    state
        .metrics
        .observe_search_stage(namespace, "l1", l1_elapsed);

    // Stage: rerank.
    let rerank_started = Instant::now();
    let pool_size = pool.len();
    let mut degraded = false;
    let (ranked, rerank_echo) = match rerank_plan {
        None => (
            fused_scores(pool),
            rerank_echo_skipped(None, "disabled", pool_size),
        ),
        Some(plan) if pool.is_empty() => (
            Vec::new(),
            rerank_echo_skipped(Some(&plan.options), "empty_pool", 0),
        ),
        Some(plan) => {
            let documents: Vec<RerankDocument> = pool
                .iter()
                .map(|scored| RerankDocument {
                    id: scored.candidate.id.clone(),
                    fields: reranker_fields(
                        &plan.attributes,
                        &scored.candidate.attributes,
                        plan.options.max_chars,
                    ),
                })
                .collect();
            let result = rerank_pool(
                &state.search.rerank,
                &plan.provider,
                &plan.key,
                &request.query,
                plan.options.question,
                &documents,
                plan.options.docs_per_call,
            )
            .await;
            match result {
                Ok(outcome) => {
                    state.telemetry.touch_rerank();
                    let mut ranked: Vec<(Scored, f64)> = pool
                        .into_iter()
                        .map(|scored| {
                            let probability = outcome
                                .scores
                                .get(&scored.candidate.id)
                                .copied()
                                .unwrap_or(0.0);
                            (scored, probability)
                        })
                        .collect();
                    // Stable: ties keep the L1 (fused) order.
                    ranked
                        .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                    let before = ranked.len();
                    ranked.retain(|(_, probability)| *probability >= plan.options.threshold);
                    let pruned = before - ranked.len();
                    state.metrics.observe_rerank(
                        namespace,
                        plan.provider.name(),
                        state.search.rerank.model(),
                        before as u64,
                        pruned as u64,
                        outcome.input_tokens,
                    );
                    let echo = RerankEcho {
                        provider: plan.options.provider.clone(),
                        model: outcome
                            .model
                            .clone()
                            .or_else(|| Some(state.search.rerank.model().to_string())),
                        question: plan.options.question.version,
                        executed: true,
                        reason: None,
                        pool: before,
                        calls: outcome.calls,
                        docs_per_call: plan.options.docs_per_call,
                        threshold: plan.options.threshold,
                        pruned,
                        input_tokens: outcome.input_tokens,
                        latency_ms: outcome.latency_ms,
                    };
                    (ranked, echo)
                }
                Err(error) if plan.options.required => {
                    state
                        .metrics
                        .observe_rerank_degraded(namespace, error.reason());
                    return Err(AppError::RerankUnavailable(error.to_string()));
                }
                Err(error) => {
                    warn!(namespace = %namespace, %error, "rerank degraded to fused order");
                    state
                        .metrics
                        .observe_rerank_degraded(namespace, error.reason());
                    degraded = true;
                    (
                        fused_scores(pool),
                        rerank_echo_skipped(Some(&plan.options), error.reason(), pool_size),
                    )
                }
            }
        }
    };
    let rerank_elapsed = rerank_started.elapsed();
    if rerank_echo.executed || degraded {
        state
            .metrics
            .observe_search_stage(namespace, "rerank", rerank_elapsed);
    }

    let rows: Vec<SearchRow> = ranked
        .into_iter()
        .take(request.top_k as usize)
        .map(|(scored, score)| SearchRow {
            id: response_id(
                native_wire,
                &scored.candidate.id,
                scored.candidate.numeric_id,
            ),
            score,
            attributes: response_attributes(
                &scored.candidate.attributes,
                &request.include,
                &embedded.target,
            ),
            explain: request.explain.then(|| RowExplain {
                legs: scored.features.legs.clone(),
                features: scored.features,
                l1_score: scored.l1.score,
                contributions: scored.l1.contributions,
            }),
        })
        .collect();

    let hybrid = HybridEcho {
        tokens: policy.tokens,
        tokens_dropped: policy.dropped_by_cap,
        stopwords: request.stopwords.echo(),
        stopwords_dropped: policy.stopwords_dropped,
        fuzziness: fuzziness.echo(),
        fuzziness_clamped: clamped,
        rank_constant: DEFAULT_RANK_CONSTANT,
        per_leg_limit,
        legs: runs
            .iter()
            .map(|run| LegEcho {
                label: run.leg.spec.label.clone(),
                kind: run.leg.kind,
                attribute: run.leg.attribute.clone(),
                rows: run.rows.len(),
            })
            .collect(),
        dropped_legs: plan.dropped + unsupported,
        threads: shard_count.map(|_| threads),
        surfaced,
    };
    let performance = Performance {
        embedding_tokens: embedded
            .performance
            .get("embedding_tokens")
            .and_then(Value::as_f64),
        embedding_ms: embedded
            .performance
            .get("embedding_ms")
            .and_then(Value::as_f64),
        legs_ms: legs_elapsed.as_millis() as u64,
        fuse_ms: fuse_elapsed.as_millis() as u64,
        l1_ms: l1_elapsed.as_millis() as u64,
        rerank_ms: rerank_elapsed.as_millis() as u64,
        total_ms: started.elapsed().as_millis() as u64,
    };
    Ok(SearchOutcome {
        response: SearchResponse {
            rows,
            routing,
            plan: PlanEcho {
                executed: false,
                reason: Some("unconfigured"),
            },
            hybrid,
            rerank: rerank_echo,
            performance,
        },
        watermark,
        degraded,
    })
}

/// Fused order with the RRF sum as `score`: the stage did not execute.
fn fused_scores(pool: Vec<Scored>) -> Vec<(Scored, f64)> {
    pool.into_iter()
        .map(|scored| {
            let score = scored.candidate.rrf_sum;
            (scored, score)
        })
        .collect()
}

fn rerank_echo_skipped(
    options: Option<&RerankOptions>,
    reason: &'static str,
    pool: usize,
) -> RerankEcho {
    RerankEcho {
        provider: options
            .map(|options| options.provider.clone())
            .unwrap_or_else(|| DEFAULT_PROVIDER.to_string()),
        model: None,
        question: options
            .map(|options| options.question.version)
            .unwrap_or(DEFAULT_QUESTION),
        executed: false,
        reason: Some(reason),
        pool,
        calls: 0,
        docs_per_call: options
            .map(|options| options.docs_per_call)
            .unwrap_or(DEFAULT_DOCS_PER_CALL),
        threshold: options.map(|options| options.threshold).unwrap_or(0.0),
        pruned: 0,
        input_tokens: 0,
        latency_ms: 0,
    }
}

/// One history entry per request. `raw_query` comes from the body: a search
/// request carries its own text, so no `x-hevlayer-search-query` header.
fn log_history(
    state: &AppState,
    namespace: &str,
    headers: &HeaderMap,
    body: &Value,
    outcome: &SearchOutcome,
) {
    let (_, trace_id) = traceparent_for_query(headers);
    let tags = tags_from_headers(headers).unwrap_or_default();
    let (timestamp, timestamp_nanos) = now_timestamp();
    let top_result_ids = outcome
        .response
        .rows
        .iter()
        .take(10)
        .map(|row| match &row.id {
            Value::String(id) => id.clone(),
            other => other.to_string(),
        })
        .collect();
    let mut query = body.as_object().cloned().unwrap_or_default();
    query.insert("kind".to_string(), json!("search"));
    let entry = SearchHistoryEntry {
        timestamp,
        timestamp_nanos,
        namespace: namespace.to_string(),
        trace_id: Some(trace_id),
        raw_query: body
            .get("query")
            .and_then(Value::as_str)
            .map(str::to_string),
        stable_as_of: outcome.watermark,
        query: Value::Object(query),
        top_result_ids,
        tags,
    };
    let aerospike = Arc::clone(&state.aerospike);
    let s3 = Arc::clone(&state.s3);
    tokio::spawn(async move {
        log_search_history(aerospike, s3, entry).await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokens(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("tok{i}")).collect()
    }

    fn labels(plan: &LegPlan) -> Vec<&str> {
        plan.legs
            .iter()
            .map(|leg| leg.spec.label.as_str())
            .collect()
    }

    #[test]
    fn search_preserves_numeric_and_string_wire_ids_through_fusion() {
        let mut run = run("ann", &["42"]);
        run.rows[0].numeric_id = true;
        let fused = rrf_fuse(&[run], DEFAULT_RANK_CONSTANT);
        assert_eq!(
            response_id(false, &fused[0].id, fused[0].numeric_id),
            json!(42)
        );
        assert_eq!(response_id(false, "42", false), json!("42"));
        assert_eq!(response_id(true, "42", false), json!(42));
        assert_eq!(response_id(true, "\"42\"", false), json!("42"));
    }

    #[test]
    fn budget_is_spent_ann_then_bm25_breadth_then_fuzzy_round_robin() {
        let mut builder = LegPlanBuilder::default();
        builder.add_ann("ann", "embed_text", "text", &[0.1], None);
        for attribute in ["body", "title"] {
            builder.add_bm25(attribute, "q", None);
        }
        for attribute in ["body", "title"] {
            builder.add_fuzzy(attribute, "q", &tokens(3), Fuzziness::Auto, None);
        }
        let plan = builder.build(LEG_BUDGET);
        assert_eq!(
            labels(&plan),
            vec![
                "ann",
                "bm25:body",
                "bm25:title",
                "fuzzy:body:tok0",
                "fuzzy:title:tok0",
                "fuzzy:body:tok1",
                "fuzzy:title:tok1",
                "fuzzy:body:tok2",
                "fuzzy:title:tok2",
            ]
        );
        assert_eq!(plan.dropped, 0);
        assert_eq!(plan.calls().len(), 1);
    }

    #[test]
    fn legs_past_the_budget_are_dropped_and_counted() {
        let mut builder = LegPlanBuilder::default();
        builder.add_ann("ann", "embed_text", "text", &[0.1], None);
        builder.add_bm25("text", "q", None);
        builder.add_fuzzy("text", "q", &tokens(15), Fuzziness::Auto, None);
        // 1 + 1 + 15 = 17: `HybridText` would run all 17; `/search` runs 16.
        let plan = builder.build(LEG_BUDGET);
        assert_eq!(plan.legs.len(), 16);
        assert_eq!(plan.dropped, 1);
        assert_eq!(plan.legs.last().unwrap().spec.label, "fuzzy:text:tok13");
    }

    #[test]
    fn many_full_text_attributes_spend_the_budget_on_breadth() {
        let mut builder = LegPlanBuilder::default();
        builder.add_ann("ann", "embed_a0", "a0", &[0.1], None);
        let attributes: Vec<String> = (0..20).map(|i| format!("a{i:02}")).collect();
        for attribute in &attributes {
            builder.add_bm25(attribute, "q", None);
        }
        for attribute in &attributes {
            builder.add_fuzzy(attribute, "q", &tokens(2), Fuzziness::Auto, None);
        }
        let plan = builder.build(LEG_BUDGET);
        assert_eq!(plan.legs.len(), 16);
        assert!(plan.legs[1..].iter().all(|leg| leg.kind == LegKind::Bm25));
        assert_eq!(plan.dropped, 1 + 20 + 40 - 16);
    }

    #[test]
    fn whole_query_fuzzy_is_one_leg_and_none_at_distance_zero() {
        let mut builder = LegPlanBuilder::default();
        builder.add_ann("ann", "vector", "text", &[0.1], None);
        builder.add_bm25("text", "eight token query", None);
        assert_eq!(
            builder.add_whole_query_fuzzy("text", "eight token query", Fuzziness::Fixed(0), None),
            0
        );
        let built =
            builder.add_whole_query_fuzzy("text", "eight token query", Fuzziness::Fixed(1), None);
        builder.add_unsupported(8 - built);
        let plan = builder.build(LEG_BUDGET);
        assert_eq!(labels(&plan), vec!["ann", "bm25:text", "fuzzy:text"]);
        assert_eq!(plan.dropped, 7);
        assert!(plan.legs[2].token.is_none());
        assert_eq!(plan.legs[2].spec.filter.as_ref().unwrap()[1], "Fuzzy");
    }

    #[test]
    fn a_wider_plan_becomes_more_calls_not_a_refactor() {
        let mut builder = LegPlanBuilder::default();
        for i in 0..40 {
            builder.add_bm25(&format!("a{i}"), "q", None);
        }
        let plan = builder.build(40);
        let sizes: Vec<usize> = plan.calls().iter().map(|call| call.len()).collect();
        assert_eq!(sizes, vec![16, 16, 8]);
    }

    #[test]
    fn reserved_attributes_never_become_text_legs() {
        let mut builder = LegPlanBuilder::default();
        builder.add_bm25("_hevlayer_fetch_count", "q", None);
        builder.add_fuzzy("_hevlayer_shard", "q", &tokens(2), Fuzziness::Auto, None);
        assert!(builder.build(LEG_BUDGET).legs.is_empty());

        let schema = schema_from_metadata(
            &json!({"schema": {
                "_hevlayer_note": {"type": "string", "full_text_search": true},
                "title": {"type": "string", "full_text_search": true},
                "body": {"type": "string", "full_text_search": {"stemming": true}, "embed": "voyage/voyage-4-lite"},
                "year": {"type": "uint"},
                "off": {"type": "string", "full_text_search": false}
            }}),
            false,
        );
        assert_eq!(schema.full_text_attributes, vec!["body", "title"]);
        assert_eq!(schema.embed_attributes, vec!["body"]);
        assert_eq!(
            schema.returnable_attributes,
            vec!["body", "off", "title", "year"]
        );
        let vectors = schema_from_metadata(
            &json!({"schema": {"embed_body": "[512]f32", "vector": {"type": "[2]f16"}, "t": "string"}}),
            false,
        );
        assert_eq!(vectors.returnable_attributes, vec!["t"]);
    }

    #[test]
    fn hev_search_has_one_full_text_column() {
        let listed = json!({"schema": [{"name": "id"}, {"name": "text"}, {"name": "vector"}]});
        assert_eq!(
            schema_from_metadata(&listed, true).full_text_attributes,
            vec!["text"]
        );
        let no_text = json!({"schema": [{"name": "id"}, {"name": "vector"}]});
        assert!(schema_from_metadata(&no_text, true)
            .full_text_attributes
            .is_empty());
        assert!(schema_from_metadata(&listed, false)
            .full_text_attributes
            .is_empty());
    }

    fn run(label: &str, ids: &[&str]) -> LegRun {
        LegRun {
            leg: SearchLeg {
                kind: LegKind::Bm25,
                attribute: None,
                token: None,
                spec: LegSpec {
                    label: label.to_string(),
                    rank_by: Value::Null,
                    filter: None,
                },
            },
            rows: ids
                .iter()
                .map(|id| QueryResult {
                    numeric_id: false,
                    id: id.to_string(),
                    dist: Some(1.0),
                    attributes: HashMap::from([("from".to_string(), json!(label))]),
                })
                .collect(),
        }
    }

    #[test]
    fn fusion_dedups_by_id_and_keeps_the_first_occurrence() {
        let fused = rrf_fuse(&[run("a", &["x", "y"]), run("b", &["y", "z"])], 60);
        let ids: Vec<&str> = fused.iter().map(|c| c.id.as_str()).collect();
        assert_eq!(ids, vec!["y", "x", "z"]);
        let y = &fused[0];
        assert!((y.rrf_sum - (1.0 / 62.0 + 1.0 / 61.0)).abs() < 1e-12);
        assert_eq!(y.attributes["from"], "a");
        assert_eq!(y.legs.len(), 2);
        assert_eq!((y.legs[0].label.as_str(), y.legs[0].rank), ("a", 2));
        assert_eq!((y.legs[1].label.as_str(), y.legs[1].rank), ("b", 1));
    }

    #[test]
    fn identity_l1_keeps_fused_order_and_cuts_to_pool() {
        let ids: Vec<String> = (0..60).map(|i| format!("d{i:02}")).collect();
        let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        let mut candidates = rrf_fuse(&[run("a", &refs)], 60);
        candidates[0]
            .attributes
            .insert(UPSERTED_AT_ATTR.to_string(), json!(1_000_000u64));
        let fused_order: Vec<String> = candidates.iter().map(|c| c.id.clone()).collect();
        let pool = l1_cut(&IdentityL1, candidates, 50, 1_060_000);
        assert_eq!(pool.len(), 50);
        let pool_order: Vec<String> = pool.iter().map(|s| s.candidate.id.clone()).collect();
        assert_eq!(pool_order, fused_order[..50]);
        assert_eq!(pool[0].features.age_seconds, Some(60.0));
        assert_eq!(pool[0].l1.contributions.age_seconds, Some(0.0));
        assert_eq!(pool[0].l1.score, pool[0].features.rrf_sum);
        assert_eq!(pool[1].features.age_seconds, None);
        assert_eq!(pool[1].l1.contributions.age_seconds, None);
        assert_eq!(pool[0].features.fetch_count_30d, None);
        let rendered = serde_json::to_value(&pool[1].features).unwrap();
        assert_eq!(rendered, json!({"rrf_sum": pool[1].features.rrf_sum}));
    }

    #[test]
    fn request_defaults_and_rejections() {
        let request = parse_request(&json!({"query": "q", "embed": {}, "text": {}})).unwrap();
        assert_eq!((request.top_k, request.pool), (10, 50));
        assert!(request.rerank.is_some());
        assert!(parse_request(&json!({"query": "q", "rerank": false}))
            .unwrap()
            .rerank
            .is_none());
        for bad in [
            json!({}),
            json!({"query": "  "}),
            json!({"query": "q", "pool": 49}),
            json!({"query": "q", "pool": 201}),
            json!({"query": "q", "top_k": 0}),
            json!({"query": "q", "top_k": 101, "pool": 200}),
            json!({"query": "q", "top_k": 60}),
            json!({"query": "q", "plan": true}),
            json!({"query": "q", "rank": {"popularity": {"weight": 0.1}}}),
            json!({"query": "q", "rerank": {"provider": "cohere"}}),
            json!({"query": "q", "rerank": {"question": "custom"}}),
            json!({"query": "q", "rerank": {"docs_per_call": 51}}),
            json!({"query": "q", "rerank": {"threshold": 1.5}}),
            json!({"query": "q", "rerank": {"attributes": ["_hevlayer_fetch_count"]}}),
            json!({"query": "q", "text": {"fuzziness": 3}}),
        ] {
            assert!(
                matches!(parse_request(&bad), Err(AppError::Validation(_))),
                "{bad}"
            );
        }
    }

    #[test]
    fn response_strips_what_the_caller_did_not_ask_for() {
        let row = HashMap::from([
            ("title".to_string(), json!("t")),
            ("text".to_string(), json!("body")),
            ("embed_text".to_string(), json!([0.1])),
            (UPSERTED_AT_ATTR.to_string(), json!(5)),
        ]);
        let all = response_attributes(&row, &IncludeAttributes::All(true), "embed_text");
        assert_eq!(all.keys().collect::<Vec<_>>(), vec!["text", "title"]);
        let named = response_attributes(
            &row,
            &IncludeAttributes::Fields(vec!["title".into(), UPSERTED_AT_ATTR.into()]),
            "embed_text",
        );
        assert_eq!(
            named.keys().collect::<Vec<_>>(),
            vec![UPSERTED_AT_ATTR, "title"]
        );
        assert!(response_attributes(&row, &IncludeAttributes::All(false), "embed_text").is_empty());

        let include = leg_include(
            &IncludeAttributes::Fields(vec!["title".into()]),
            &[],
            &["title".to_string(), "text".to_string()],
            true,
        );
        assert!(matches!(
            include,
            Some(IncludeAttributes::Fields(fields))
                if fields == vec!["title", "text", UPSERTED_AT_ATTR]
        ));
        // "All attributes" becomes the schema's non-vector list when known.
        let all = leg_include(
            &IncludeAttributes::All(true),
            &["title".to_string(), "year".to_string()],
            &["text".to_string()],
            false,
        );
        assert!(matches!(
            all,
            Some(IncludeAttributes::Fields(fields)) if fields == vec!["title", "year", "text"]
        ));
        assert!(matches!(
            leg_include(&IncludeAttributes::All(true), &[], &[], true),
            Some(IncludeAttributes::All(true))
        ));
    }
}
