//! Hybrid text fusion (RFC 0022): the `HybridText` rank expression.
//!
//! The gateway tokenizes the caller's input string with `alyze` —
//! Turbopuffer's open-source tokenizer, the same code behind the production
//! `word_v4` analyzer — expands one full-input BM25 leg plus one fuzzy leg
//! per token, and delegates fusion to upstream `rerank_by: ["RRF", ...]`
//! unless the caller asks for per-leg attribution. In that opt-in mode, or
//! when scattering across shards, the gateway runs each leg and computes the
//! same RRF sum so it can return leg ranks.

use std::cell::RefCell;
use std::sync::Arc;

use alyze::analyze::{
    AnalysisOptions, Analyzer, LanguageWithStopwords, ReusableBuffer, StopwordRemoval,
    TokenizerOptions,
};
use axum::http::{HeaderMap, HeaderValue};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use tracing::warn;

use crate::clients::turbopuffer::TurbopufferError;
use crate::error::AppError;
use crate::history::{
    header_to_string, log_search_history, now_timestamp, tags_from_headers, traceparent_for_query,
    TRACEPARENT_HEADER,
};
use crate::metrics::{STATUS_LAYER_ERROR, STATUS_OK, STATUS_TPUF_ERROR};
use crate::models::{IncludeAttributes, QueryResult, SearchHistoryEntry};
use crate::routes::query::{
    compose_read_filter, insert_optional_u64_header, temporal_filter, TemporalFilter,
    LAYER_NEXT_CURSOR_HEADER, LAYER_STABLE_AS_OF_HEADER,
};
use crate::shards::active_shard_count;
use crate::AppState;

/// 15 fuzzy legs + 1 BM25 leg = 16, the upstream multi-query subquery cap.
pub(crate) const MAX_QUERY_TOKENS: usize = 15;
const MIN_TOKEN_CHARS: usize = 2;
/// Turbopuffer's index-time `max_token_length` default — mirrored at query
/// time so we never emit a token longer than any term the index stored.
const MAX_TOKEN_LENGTH_BYTES: usize = 39;

const DEFAULT_RANK_CONSTANT: u64 = 60;
const PER_LEG_LIMIT_FLOOR: u64 = 50;
const PER_LEG_LIMIT_CEIL: u64 = 200;
const FUSED_CURSOR_MAX_OFFSET: u32 = 10_000;

// --- Tokenizer policy ---

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PolicyTokens {
    /// Post-policy tokens, input order, deduped, capped at MAX_QUERY_TOKENS.
    pub tokens: Vec<String>,
    /// Tokens removed by the 15-token cap (not by length/punctuation rules).
    pub dropped_by_cap: usize,
    /// Tokens removed by the stop-word policy (RFC 0090), in input order.
    /// Always empty when the policy is `StopwordsOption::Off`.
    pub stopwords_dropped: Vec<String>,
}

/// The `stopwords` option on `HybridText`/`Auto` (RFC 0090): which tokens
/// are suppressed from the per-token *fuzzy-leg* expansion. The full-input
/// BM25 anchor is never affected — the index-time analyzer owns stop-word
/// treatment there.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) enum StopwordsOption {
    /// Built-in English list (alyze's) — the default.
    #[default]
    English,
    /// Every token spawns a fuzzy leg (pre-RFC-0090 behavior).
    Off,
    /// Caller-supplied list; matched against post-policy (lowercased) tokens.
    Custom(Vec<String>),
}

impl StopwordsOption {
    fn is_stopword(&self, token: &str) -> bool {
        match self {
            Self::Off => false,
            Self::Custom(list) => list.iter().any(|w| w == token),
            // Reuse alyze's English list by analyzing the single token with
            // stop-word removal on: a stop word yields no output token. The
            // token is already post-policy (lowercased, word-like), so the
            // analysis is a pure list lookup.
            Self::English => {
                let analyzer = Analyzer::new(AnalysisOptions {
                    tokenizer: TokenizerOptions::UAX29Word(Default::default()),
                    maximum_token_length: Some(MAX_TOKEN_LENGTH_BYTES),
                    case_sensitive: false,
                    stopword_removal: Some(StopwordRemoval::ForLanguage(
                        LanguageWithStopwords::English,
                    )),
                    stemming: None,
                    ascii_folding: false,
                });
                let mut survived = false;
                ANALYZE_BUFFER.with(|buffer| {
                    let mut buffer = buffer.borrow_mut();
                    buffer.reset_keep_stemming_cache();
                    analyzer.analyze(token, &mut buffer, |_| {
                        survived = true;
                        false
                    });
                });
                !survived
            }
        }
    }

    fn echo(&self) -> Value {
        match self {
            Self::English => Value::String("en".to_string()),
            Self::Off => Value::Bool(false),
            Self::Custom(list) => json!(list),
        }
    }
}

thread_local! {
    /// Reused across queries on the same worker thread: `ReusableBuffer`
    /// preallocates a 32k-capacity stemming cache we'd otherwise pay per call.
    static ANALYZE_BUFFER: RefCell<ReusableBuffer> = RefCell::new(ReusableBuffer::new());
}

/// The documented v1 tokenizer policy: UAX #29 word boundaries + lowercase
/// via `alyze` (word-like filtering already drops punctuation-only tokens),
/// then drop tokens shorter than 2 chars, dedupe preserving order, cap at 15.
/// Stemming stays off to match `word_v4` index-time defaults; enabling it
/// must mirror the field's index-time analyzer. This no-stop-word variant
/// feeds routing (token counts) and other non-leg consumers; the fuzzy-leg
/// expansion uses [`tokenize_query_input_with_stopwords`].
pub(crate) fn tokenize_query_input(input: &str) -> PolicyTokens {
    tokenize_query_input_with_stopwords(input, &StopwordsOption::Off)
}

/// The tokenizer policy with the RFC 0090 stop-word stage: stop words are
/// removed from the fuzzy-leg token set *after* length/dedupe and *before*
/// the 15-token cap, so on long queries the cap spends its legs on content
/// tokens. Removed tokens are reported in `stopwords_dropped` (they do not
/// count against `dropped_by_cap`). The BM25 anchor is unaffected — it ranks
/// over the raw input string, not these tokens.
pub(crate) fn tokenize_query_input_with_stopwords(
    input: &str,
    stopwords: &StopwordsOption,
) -> PolicyTokens {
    let analyzer = Analyzer::new(AnalysisOptions {
        tokenizer: TokenizerOptions::UAX29Word(Default::default()),
        maximum_token_length: Some(MAX_TOKEN_LENGTH_BYTES),
        case_sensitive: false,
        stopword_removal: None,
        stemming: None,
        ascii_folding: false,
    });
    let mut tokens: Vec<String> = Vec::new();
    ANALYZE_BUFFER.with(|buffer| {
        let mut buffer = buffer.borrow_mut();
        buffer.reset_keep_stemming_cache();
        analyzer.analyze(input, &mut buffer, |token| {
            if token.text.chars().count() >= MIN_TOKEN_CHARS
                && !tokens.iter().any(|t| t == token.text)
            {
                tokens.push(token.text.to_string());
            }
            true
        });
    });
    let mut stopwords_dropped = Vec::new();
    tokens.retain(|token| {
        if stopwords.is_stopword(token) {
            stopwords_dropped.push(token.clone());
            false
        } else {
            true
        }
    });
    let dropped_by_cap = tokens.len().saturating_sub(MAX_QUERY_TOKENS);
    tokens.truncate(MAX_QUERY_TOKENS);
    PolicyTokens {
        tokens,
        dropped_by_cap,
        stopwords_dropped,
    }
}

// --- Expression parsing ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Fuzziness {
    Auto,
    Fixed(u8),
}

impl Fuzziness {
    /// The Turbopuffer `Fuzzy` filter `max_edit_distance` ladder: an array of
    /// `{min_query_chars, distance}` rules. Upstream enforces
    /// `min_query_chars >= 3 * (distance + 1)` — a query term needs at least
    /// three characters per permitted edit — so the ladder lets longer terms
    /// tolerate more typos while short terms stay exact. `Auto` allows up to
    /// edit distance 2; `Fixed(d)` caps the ladder at `d`, so `Fixed(0)` is
    /// exact-only. A term below the smallest `min_query_chars` (3) gets no rule
    /// and matches exactly, mirroring upstream.
    fn max_edit_distance(self) -> Value {
        let cap = match self {
            Self::Auto => 2,
            Self::Fixed(d) => d,
        };
        let rules: Vec<Value> = (0..=cap)
            .map(|d| json!({"min_query_chars": 3 * (u64::from(d) + 1), "distance": d}))
            .collect();
        Value::Array(rules)
    }

    fn echo(self) -> Value {
        match self {
            Self::Auto => Value::String("auto".to_string()),
            Self::Fixed(d) => Value::from(d),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct HybridTextExpr {
    pub field: String,
    pub input: String,
    pub fuzziness: Fuzziness,
    /// RFC 0090: which tokens are suppressed from the fuzzy-leg expansion.
    pub stopwords: StopwordsOption,
    pub rank_constant: u64,
    pub per_leg_limit: Option<u64>,
    /// Fan-out control for sharded scatter/gather — the same `threads`
    /// vocabulary as scans. Defaults from `Index.spec.scan.threads`, then 8;
    /// clamped to active shards. No effect on unsharded namespaces (the
    /// expansion is one fused upstream call).
    pub threads: Option<u32>,
}

/// The operator string at `rank_by[1]`, when the body's `rank_by` is a
/// Layer-shaped tuple. Used to intercept `HybridText` / `Auto` ahead of
/// passthrough.
pub(crate) fn rank_by_operator(body: &Value) -> Option<&str> {
    body.get("rank_by")?.get(1)?.as_str()
}

/// True when any leg of a `queries` body uses a Layer-only operator. Those
/// expansions are one multi-query deep by construction, so nesting is a 422.
pub(crate) fn queries_contain_layer_operator(body: &Value) -> bool {
    body.get("queries")
        .and_then(Value::as_array)
        .is_some_and(|legs| {
            legs.iter().any(|leg| {
                matches!(rank_by_operator(leg), Some(op) if op == "HybridText" || op == "Auto")
            })
        })
}

/// Parse `["field", "HybridText", "input", {options}?]`. The caller has
/// already matched the operator; everything else validates here (422s).
pub(crate) fn parse_hybrid_text_expr(rank_by: &Value) -> Result<HybridTextExpr, AppError> {
    let tuple = rank_by
        .as_array()
        .filter(|t| t.len() == 3 || t.len() == 4)
        .ok_or_else(|| {
            AppError::Validation(
                "HybridText rank_by must be [field, \"HybridText\", input, {options}?]".to_string(),
            )
        })?;
    let field = tuple[0]
        .as_str()
        .filter(|f| !f.is_empty())
        .ok_or_else(|| {
            AppError::Validation("HybridText field (rank_by[0]) must be a string".to_string())
        })?
        .to_string();
    let input = tuple[2]
        .as_str()
        .ok_or_else(|| {
            AppError::Validation("HybridText input (rank_by[2]) must be a string".to_string())
        })?
        .to_string();

    let mut fuzziness = Fuzziness::Auto;
    let mut stopwords = StopwordsOption::default();
    let mut rank_constant = DEFAULT_RANK_CONSTANT;
    let mut per_leg_limit = None;
    let mut threads = None;
    if let Some(options) = tuple.get(3) {
        let options = options.as_object().ok_or_else(|| {
            AppError::Validation("HybridText options (rank_by[3]) must be an object".to_string())
        })?;
        for (key, value) in options {
            match key.as_str() {
                "fuzziness" => fuzziness = parse_fuzziness(value)?,
                "stopwords" => stopwords = parse_stopwords(value)?,
                "rank_constant" => {
                    rank_constant = value.as_u64().filter(|n| *n > 0).ok_or_else(|| {
                        AppError::Validation(
                            "HybridText rank_constant must be an integer > 0".to_string(),
                        )
                    })?;
                }
                "per_leg_limit" => {
                    per_leg_limit = match value {
                        Value::Null => None,
                        other => Some(other.as_u64().filter(|n| *n > 0).ok_or_else(|| {
                            AppError::Validation(
                                "HybridText per_leg_limit must be an integer > 0".to_string(),
                            )
                        })?),
                    };
                }
                "threads" => {
                    threads = match value {
                        Value::Null => None,
                        other => Some(
                            other
                                .as_u64()
                                .filter(|n| *n >= 1)
                                .and_then(|n| u32::try_from(n).ok())
                                .ok_or_else(|| {
                                    AppError::Validation("threads must be >= 1".to_string())
                                })?,
                        ),
                    };
                }
                other => {
                    return Err(AppError::Validation(format!(
                        "unknown HybridText option `{other}`"
                    )));
                }
            }
        }
    }

    Ok(HybridTextExpr {
        field,
        input,
        fuzziness,
        stopwords,
        rank_constant,
        per_leg_limit,
        threads,
    })
}

/// Parse the `stopwords` option: `"en"` (default), `false`, or an explicit
/// token array (lowercased; matched against post-policy tokens).
pub(crate) fn parse_stopwords(value: &Value) -> Result<StopwordsOption, AppError> {
    match value {
        Value::String(s) if s == "en" => Ok(StopwordsOption::English),
        Value::Bool(false) => Ok(StopwordsOption::Off),
        Value::Array(words) => {
            let mut list = Vec::with_capacity(words.len());
            for word in words {
                let word = word.as_str().ok_or_else(|| {
                    AppError::Validation("stopwords list entries must be strings".to_string())
                })?;
                list.push(word.to_lowercase());
            }
            Ok(StopwordsOption::Custom(list))
        }
        _ => Err(AppError::Validation(
            "stopwords must be \"en\", false, or an array of strings".to_string(),
        )),
    }
}

pub(crate) fn parse_fuzziness(value: &Value) -> Result<Fuzziness, AppError> {
    match value {
        Value::String(s) if s == "auto" => Ok(Fuzziness::Auto),
        Value::Number(n) => match n.as_u64() {
            Some(d @ 0..=2) => Ok(Fuzziness::Fixed(d as u8)),
            _ => Err(AppError::Validation(
                "HybridText fuzziness must be \"auto\", 0, 1, or 2".to_string(),
            )),
        },
        _ => Err(AppError::Validation(
            "HybridText fuzziness must be \"auto\", 0, 1, or 2".to_string(),
        )),
    }
}

/// `clamp(5 × top_k, 50, 200)` — how deep each leg retrieves before fusion.
pub(crate) fn default_per_leg_limit(top_k: u32) -> u64 {
    (5 * top_k as u64).clamp(PER_LEG_LIMIT_FLOOR, PER_LEG_LIMIT_CEIL)
}

// --- Request envelope shared with the router ---

/// The non-`rank_by` parts of a hybrid/routed query body, validated to the
/// docs' 422 rules (no vector shapes — `HybridText` owns ranking).
pub(crate) struct HybridRequest {
    pub top_k: u32,
    pub filters: Option<Value>,
    pub include_attributes: Option<Value>,
    pub include_leg_breakdown: bool,
    pub cursor: Option<FusedCursor>,
    pub as_of: Option<u64>,
    pub between: Option<[u64; 2]>,
    pub temporal_filter: Option<TemporalFilter>,
}

pub(crate) fn parse_hybrid_request(body: &Map<String, Value>) -> Result<HybridRequest, AppError> {
    if body.contains_key("vector") || body.contains_key("nearest_to_id") {
        return Err(AppError::Validation(
            "vector and nearest_to_id are not valid alongside a HybridText/Auto rank_by; the expression owns ranking"
                .to_string(),
        ));
    }
    let top_k = match body.get("top_k") {
        None => 10,
        Some(v) => {
            let n = v.as_u64().filter(|n| *n > 0 && *n <= u32::MAX as u64);
            n.ok_or_else(|| AppError::Validation("top_k must be a positive integer".to_string()))?
                as u32
        }
    };
    let cursor = match body.get("cursor") {
        Some(Value::Null) | None => None,
        Some(Value::String(s)) if s.is_empty() => None,
        Some(Value::String(s)) => Some(FusedCursor::decode(s).map_err(AppError::Validation)?),
        Some(_) => return Err(AppError::Validation("cursor must be a string".to_string())),
    };
    if let Some(cursor) = cursor.as_ref() {
        let end = cursor.offset.checked_add(top_k).ok_or_else(|| {
            AppError::Validation("cursor offset exceeds fused pagination depth".to_string())
        })?;
        if end > FUSED_CURSOR_MAX_OFFSET {
            return Err(AppError::Validation(format!(
                "HybridText/Auto cursor depth is capped at {FUSED_CURSOR_MAX_OFFSET} rows"
            )));
        }
    }
    // Sanitize `vector` out of explicit include_attributes lists like the
    // single-query path does; the gateway never returns vectors.
    let include_attributes = match body.get("include_attributes").cloned() {
        None => None,
        Some(value) => {
            let include: IncludeAttributes = serde_json::from_value(value)
                .map_err(|e| AppError::Validation(format!("invalid include_attributes: {e}")))?;
            Some(match include {
                IncludeAttributes::Fields(mut fields) => {
                    fields.retain(|f| f != "vector");
                    json!(fields)
                }
                IncludeAttributes::All(all) => Value::Bool(all),
            })
        }
    };
    let include_leg_breakdown = match body.get("include_leg_breakdown") {
        None | Some(Value::Null) => false,
        Some(Value::Bool(value)) => *value,
        Some(_) => {
            return Err(AppError::Validation(
                "`include_leg_breakdown` must be a boolean".to_string(),
            ))
        }
    };
    let as_of = match body.get("as_of") {
        Some(Value::Null) | None => None,
        Some(value) => Some(value.as_u64().ok_or_else(|| {
            AppError::Validation("`as_of` must be an epoch-ms integer".to_string())
        })?),
    };
    let between = match body.get("between") {
        Some(Value::Null) | None => None,
        Some(Value::Array(values)) if values.len() == 2 => {
            let lo = values[0].as_u64().ok_or_else(|| {
                AppError::Validation("`between[0]` must be an epoch-ms integer".to_string())
            })?;
            let hi = values[1].as_u64().ok_or_else(|| {
                AppError::Validation("`between[1]` must be an epoch-ms integer".to_string())
            })?;
            Some([lo, hi])
        }
        Some(_) => {
            return Err(AppError::Validation(
                "`between` must be a two-element epoch-ms array".to_string(),
            ))
        }
    };
    let temporal_filter = temporal_filter(as_of, between)?;
    Ok(HybridRequest {
        top_k,
        filters: body.get("filters").cloned(),
        include_attributes,
        include_leg_breakdown,
        cursor,
        as_of,
        between,
        temporal_filter,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct FusedCursor {
    offset: u32,
}

impl FusedCursor {
    pub(crate) fn next(offset: u32) -> Self {
        Self { offset }
    }

    pub(crate) fn offset(&self) -> u32 {
        self.offset
    }

    pub(crate) fn encode(&self) -> String {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
        use base64::Engine;
        let bytes = serde_json::to_vec(self).expect("FusedCursor is JSON-encodable");
        B64.encode(bytes)
    }

    fn decode(s: &str) -> Result<Self, String> {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
        use base64::Engine;
        let bytes = B64
            .decode(s)
            .map_err(|e| format!("invalid cursor: base64 decode: {e}"))?;
        let cursor: FusedCursor = serde_json::from_slice(&bytes)
            .map_err(|e| format!("invalid cursor: json decode: {e}"))?;
        if cursor.offset >= FUSED_CURSOR_MAX_OFFSET {
            return Err(format!(
                "invalid cursor: fused pagination depth is capped at {FUSED_CURSOR_MAX_OFFSET} rows"
            ));
        }
        Ok(cursor)
    }
}

// --- Expansion ---

/// One subquery in the expansion: a rank expression plus the caller-side
/// filter (user filter, and the per-token fuzzy predicate on fuzzy legs).
/// The watermark cut is applied at render/scatter time so the 429 retry can
/// flip it on without rebuilding the expansion.
#[derive(Debug, Clone)]
pub(crate) struct LegSpec {
    pub label: String,
    pub rank_by: Value,
    pub filter: Option<Value>,
}

/// Build the leg specs for a `HybridText` expression: one BM25 leg over the
/// full input, then one leg per token whose filter additionally requires a
/// fuzzy match on that token. Every leg is BM25-ranked over the full input
/// so RRF rewards rows that fuzzy-match more tokens *and* rank well on
/// relevance.
pub(crate) fn build_hybrid_leg_specs(
    expr: &HybridTextExpr,
    tokens: &[String],
    user_filter: Option<&Value>,
) -> Vec<LegSpec> {
    let bm25 = json!([expr.field, "BM25", expr.input]);
    // Every fuzzy leg carries the same length-keyed ladder; upstream picks the
    // applicable edit distance per token from its character length.
    let max_edit_distance = expr.fuzziness.max_edit_distance();
    let mut legs = Vec::with_capacity(tokens.len() + 1);
    legs.push(LegSpec {
        label: "bm25".to_string(),
        rank_by: bm25.clone(),
        filter: user_filter.cloned(),
    });
    for token in tokens {
        let fuzzy = json!([
            expr.field,
            "Fuzzy",
            token,
            {"max_edit_distance": max_edit_distance.clone()}
        ]);
        let filter = match user_filter {
            Some(base) => json!(["And", [base.clone(), fuzzy]]),
            None => fuzzy,
        };
        legs.push(LegSpec {
            label: format!("fuzzy:{token}"),
            rank_by: bm25.clone(),
            filter: Some(filter),
        });
    }
    legs
}

/// Build the surfacing legs for RFC 0057's empty-result fallback: one leg per
/// token, filtered to rows fuzzy-matching the token. The upstream query still
/// uses `id asc` only to collect a candidate window; the gateway reorders each
/// leg by field/token edit distance before RRF so the fused page is ranked by
/// typo closeness, not document id.
pub(crate) fn build_surfacing_leg_specs(
    expr: &HybridTextExpr,
    tokens: &[String],
    user_filter: Option<&Value>,
) -> Vec<LegSpec> {
    let neutral = json!(["id", "asc"]);
    let max_edit_distance = expr.fuzziness.max_edit_distance();
    tokens
        .iter()
        .map(|token| {
            let fuzzy = json!([
                expr.field,
                "Fuzzy",
                token,
                {"max_edit_distance": max_edit_distance.clone()}
            ]);
            let filter = match user_filter {
                Some(base) => json!(["And", [base.clone(), fuzzy]]),
                None => fuzzy,
            };
            LegSpec {
                label: format!("fuzzy:{token}"),
                rank_by: neutral.clone(),
                filter: Some(filter),
            }
        })
        .collect()
}

fn include_with_field(include: Option<&Value>, field: &str) -> Result<IncludeAttributes, AppError> {
    match include {
        Some(Value::Bool(true)) | None => Ok(IncludeAttributes::All(true)),
        Some(Value::Bool(false)) => Ok(IncludeAttributes::Fields(vec![field.to_string()])),
        Some(Value::Array(fields)) => {
            let mut out = Vec::with_capacity(fields.len() + 1);
            let mut has_field = false;
            for value in fields {
                let field_name = value.as_str().ok_or_else(|| {
                    AppError::Validation("include_attributes fields must be strings".to_string())
                })?;
                if field_name == field {
                    has_field = true;
                }
                out.push(field_name.to_string());
            }
            if !has_field {
                out.push(field.to_string());
            }
            Ok(IncludeAttributes::Fields(out))
        }
        Some(_) => Err(AppError::Validation(
            "include_attributes must be a boolean or string array".to_string(),
        )),
    }
}

fn caller_requested_field(include: Option<&Value>, field: &str) -> bool {
    match include {
        Some(Value::Bool(false)) => false,
        Some(Value::Array(fields)) => fields.iter().any(|value| value.as_str() == Some(field)),
        _ => true,
    }
}

/// Render specs into upstream leg bodies. `temporal: Some(_)` conjoins the
/// request's `as_of`/`between` cut, and `watermark: Some(_)` the consistency
/// cut, into every leg's filter from the same read.
#[cfg(test)]
pub(crate) fn render_leg_bodies(
    specs: &[LegSpec],
    watermark: Option<u64>,
    temporal: Option<&TemporalFilter>,
    per_leg_limit: u64,
    include_attributes: Option<&Value>,
) -> Vec<Value> {
    specs
        .iter()
        .map(|spec| {
            let mut leg = Map::new();
            leg.insert("rank_by".to_string(), spec.rank_by.clone());
            leg.insert("top_k".to_string(), Value::from(per_leg_limit));
            leg.insert("consistency".to_string(), json!({"level": "eventual"}));
            if let Some(filter) = compose_read_filter(spec.filter.as_ref(), temporal, watermark) {
                leg.insert("filters".to_string(), filter);
            }
            if let Some(include) = include_attributes {
                leg.insert("include_attributes".to_string(), include.clone());
            }
            Value::Object(leg)
        })
        .collect()
}

/// Pull the fused row list out of the upstream fused multi-query response.
/// Accepts top-level `rows` (fused responses) and tolerates a single-entry
/// `results` wrapper so a parsing-shape change upstream degrades loudly
/// rather than silently.
#[cfg(test)]
pub(crate) fn fused_rows(body: &Value) -> Result<Vec<Value>, AppError> {
    if let Some(rows) = body.get("rows").and_then(Value::as_array) {
        return Ok(rows.clone());
    }
    if let Some(results) = body.get("results").and_then(Value::as_array) {
        if results.len() == 1 {
            if let Some(rows) = results[0].get("rows").and_then(Value::as_array) {
                return Ok(rows.clone());
            }
        }
    }
    Err(AppError::Upstream(
        "fused multi-query response carried no rows".to_string(),
    ))
}

/// Fusion knobs shared by the upstream and sharded paths.
struct FusionParams {
    watermark: Option<u64>,
    inject_filter: bool,
    per_leg_limit: u64,
    rank_constant: u64,
    include_leg_breakdown: bool,
    temporal_filter: Option<TemporalFilter>,
}

impl FusionParams {
    /// The watermark to render into leg filters on the first attempt —
    /// `None` when the consistency watcher says injection is unnecessary.
    fn first_attempt_watermark(&self) -> Option<u64> {
        if self.inject_filter {
            self.watermark
        } else {
            None
        }
    }
}

/// Turbopuffer's RRF rerank requires at least two queries. A single-leg
/// expansion — the RFC 0057 surfacing fallback on a one-token query — has
/// nothing to fuse, so duplicate the lone leg; RRF over two identical legs
/// preserves that leg's ordering and returns the same rows. (The sharded path
/// fuses gateway-side over its own legs and is unaffected.)
#[cfg(test)]
fn ensure_fusable(mut legs: Vec<Value>) -> Vec<Value> {
    if legs.len() == 1 {
        legs.push(legs[0].clone());
    }
    legs
}

fn is_unsupported_by_store(error: &TurbopufferError) -> bool {
    AppError::is_store_support_error(error)
}

async fn run_unsharded_fused(
    state: &AppState,
    namespace: &str,
    specs: &[LegSpec],
    params: &FusionParams,
    include_attributes: Option<&Value>,
) -> Result<Vec<Value>, AppError> {
    let include: Option<IncludeAttributes> = match include_attributes {
        None => None,
        Some(value) => Some(
            serde_json::from_value(value.clone())
                .map_err(|e| AppError::Validation(format!("invalid include_attributes: {e}")))?,
        ),
    };
    let mut leg_results = Vec::with_capacity(specs.len());
    let mut labels = Vec::with_capacity(specs.len());
    for spec in specs {
        let filter = compose_read_filter(
            spec.filter.as_ref(),
            params.temporal_filter.as_ref(),
            params.first_attempt_watermark(),
        );
        let first = state
            .turbopuffer()
            .ranked_query(
                namespace,
                &spec.rank_by,
                params.per_leg_limit as u32,
                filter.as_ref(),
                include.as_ref(),
            )
            .await;
        let rows = match first {
            Ok(outcome) => outcome.rows,
            Err(error) if error.is_rate_limited() && !params.inject_filter => {
                warn!(
                    namespace = %namespace,
                    %error,
                    "turbopuffer 429 on unfiltered fused leg; retrying with watermark filter",
                );
                let retry_filter = compose_read_filter(
                    spec.filter.as_ref(),
                    params.temporal_filter.as_ref(),
                    params.watermark,
                );
                state
                    .turbopuffer()
                    .ranked_query(
                        namespace,
                        &spec.rank_by,
                        params.per_leg_limit as u32,
                        retry_filter.as_ref(),
                        include.as_ref(),
                    )
                    .await
                    .map_err(|e| {
                        AppError::from_turbopuffer(e, "Turbopuffer fused leg failed (retry)")
                    })?
                    .rows
            }
            Err(e) if spec.label.starts_with("fuzzy:") && is_unsupported_by_store(&e) => {
                warn!(
                    namespace = %namespace,
                    leg = %spec.label,
                    error = %e,
                    "Skipping HybridText fuzzy leg unsupported by VectorStore"
                );
                continue;
            }
            Err(e) => {
                return Err(AppError::from_turbopuffer(
                    e,
                    "Turbopuffer fused leg failed",
                ));
            }
        };
        labels.push(spec.label.clone());
        leg_results.push(rows);
    }
    if leg_results.is_empty() {
        return Err(AppError::Upstream(
            "VectorStore does not support any HybridText legs".to_string(),
        ));
    }
    Ok(rrf_fuse_legs_with_breakdown(
        &leg_results,
        params.rank_constant,
        params.include_leg_breakdown.then_some(labels.as_slice()),
    ))
}

/// The sharded equivalent of upstream fusion, and exact — not an
/// approximation. RRF scores are rank-based and ranks are shard-local, so
/// upstream can't fuse across shards; but BM25 scores and ANN distances are
/// global quantities, so scattering each *leg* across shards and merging
/// per leg by score reproduces exactly the global per-leg ranking upstream
/// would have seen unsharded. The gateway then computes the same
/// deterministic RRF sum (`Σ 1/(rank_constant + rank)`) over the merged
/// legs. Sharding stays invisible to the client.
async fn run_sharded_fused(
    state: &AppState,
    namespace: &str,
    specs: &[LegSpec],
    params: &FusionParams,
    include_attributes: Option<&IncludeAttributes>,
    threads: u32,
) -> Result<Vec<Value>, AppError> {
    let mut leg_results = Vec::with_capacity(specs.len());
    for spec in specs {
        let filter = compose_read_filter(
            spec.filter.as_ref(),
            params.temporal_filter.as_ref(),
            params.first_attempt_watermark(),
        );
        let first = scatter_leg(
            state,
            namespace,
            spec,
            filter.as_ref(),
            params.per_leg_limit,
            include_attributes,
            threads,
        )
        .await;
        let rows = match first {
            Ok(rows) => rows,
            Err(error) if error.is_rate_limited() && !params.inject_filter => {
                warn!(
                    namespace = %namespace,
                    %error,
                    "turbopuffer 429 on unfiltered sharded fused leg; retrying with watermark filter",
                );
                let retry_filter = compose_read_filter(
                    spec.filter.as_ref(),
                    params.temporal_filter.as_ref(),
                    params.watermark,
                );
                scatter_leg(
                    state,
                    namespace,
                    spec,
                    retry_filter.as_ref(),
                    params.per_leg_limit,
                    include_attributes,
                    threads,
                )
                .await
                .map_err(|e| {
                    AppError::from_turbopuffer(e, "Turbopuffer fused leg failed (retry)")
                })?
            }
            Err(e) => {
                return Err(AppError::from_turbopuffer(
                    e,
                    "Turbopuffer fused leg failed",
                ));
            }
        };
        leg_results.push(rows);
    }
    let labels: Vec<String> = specs.iter().map(|spec| spec.label.clone()).collect();
    Ok(rrf_fuse_legs_with_breakdown(
        &leg_results,
        params.rank_constant,
        params.include_leg_breakdown.then_some(labels.as_slice()),
    ))
}

#[allow(clippy::too_many_arguments)]
async fn run_surfacing_fused(
    state: &AppState,
    namespace: &str,
    expr: &HybridTextExpr,
    specs: &[LegSpec],
    tokens: &[String],
    params: &FusionParams,
    include_attributes: Option<&Value>,
    shard_count: Option<u64>,
    threads: u32,
) -> Result<Vec<Value>, AppError> {
    let include = include_with_field(include_attributes, &expr.field)?;
    let keep_field = caller_requested_field(include_attributes, &expr.field);
    let mut leg_results = Vec::with_capacity(specs.len());
    for (spec, token) in specs.iter().zip(tokens) {
        let filter = compose_read_filter(
            spec.filter.as_ref(),
            params.temporal_filter.as_ref(),
            params.first_attempt_watermark(),
        );
        let first = collect_surfacing_leg(
            state,
            namespace,
            spec,
            token,
            &expr.field,
            filter.as_ref(),
            params.per_leg_limit,
            &include,
            shard_count,
            threads,
        )
        .await;
        let rows = match first {
            Ok(rows) => rows,
            Err(error) if error.is_rate_limited() && !params.inject_filter => {
                warn!(
                    namespace = %namespace,
                    %error,
                    "turbopuffer 429 on unfiltered surfacing leg; retrying with watermark filter",
                );
                let retry_filter = compose_read_filter(
                    spec.filter.as_ref(),
                    params.temporal_filter.as_ref(),
                    params.watermark,
                );
                collect_surfacing_leg(
                    state,
                    namespace,
                    spec,
                    token,
                    &expr.field,
                    retry_filter.as_ref(),
                    params.per_leg_limit,
                    &include,
                    shard_count,
                    threads,
                )
                .await
                .map_err(|e| {
                    AppError::from_turbopuffer(e, "Turbopuffer surfacing leg failed (retry)")
                })?
            }
            Err(e) => {
                return Err(AppError::from_turbopuffer(
                    e,
                    "Turbopuffer surfacing leg failed",
                ));
            }
        };
        leg_results.push(rows);
    }
    let labels: Vec<String> = specs.iter().map(|spec| spec.label.clone()).collect();
    let mut rows = rrf_fuse_legs_with_breakdown(
        &leg_results,
        params.rank_constant,
        params.include_leg_breakdown.then_some(labels.as_slice()),
    );
    if !keep_field {
        for row in &mut rows {
            if let Some(obj) = row.as_object_mut() {
                obj.remove(&expr.field);
            }
        }
    }
    Ok(rows)
}

#[allow(clippy::too_many_arguments)]
async fn collect_surfacing_leg(
    state: &AppState,
    namespace: &str,
    spec: &LegSpec,
    token: &str,
    field: &str,
    filter: Option<&Value>,
    per_leg_limit: u64,
    include_attributes: &IncludeAttributes,
    shard_count: Option<u64>,
    threads: u32,
) -> Result<Vec<QueryResult>, TurbopufferError> {
    let mut rows = match shard_count {
        Some(_) => {
            let results = crate::shards::shard_fanout(state, namespace, threads, |shard_filter| {
                let filter = match shard_filter {
                    Some(shard) => Some(crate::shards::combine_filter(filter, shard)),
                    None => filter.cloned(),
                };
                async move {
                    state
                        .turbopuffer()
                        .ranked_query(
                            namespace,
                            &spec.rank_by,
                            per_leg_limit as u32,
                            filter.as_ref(),
                            Some(include_attributes),
                        )
                        .await
                        .map(|outcome| outcome.rows)
                }
            })
            .await?;
            results.into_iter().flatten().collect()
        }
        None => {
            state
                .turbopuffer()
                .ranked_query(
                    namespace,
                    &spec.rank_by,
                    per_leg_limit as u32,
                    filter,
                    Some(include_attributes),
                )
                .await?
                .rows
        }
    };
    rows.sort_by(|a, b| compare_surfacing_rows(a, b, field, token));
    rows.truncate(per_leg_limit as usize);
    Ok(rows)
}

fn compare_surfacing_rows(
    a: &QueryResult,
    b: &QueryResult,
    field: &str,
    token: &str,
) -> std::cmp::Ordering {
    let a_distance = closest_token_distance(a.attributes.get(field), token);
    let b_distance = closest_token_distance(b.attributes.get(field), token);
    a_distance
        .cmp(&b_distance)
        .then_with(|| stable_id_hash(&a.id).cmp(&stable_id_hash(&b.id)))
        .then_with(|| a.id.cmp(&b.id))
}

fn closest_token_distance(value: Option<&Value>, token: &str) -> usize {
    let Some(text) = value.and_then(Value::as_str) else {
        return usize::MAX;
    };
    tokenize_query_input(text)
        .tokens
        .iter()
        .map(|candidate| edit_distance(candidate, token))
        .min()
        .unwrap_or(usize::MAX)
}

fn stable_id_hash(id: &str) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    id.as_bytes().iter().fold(FNV_OFFSET, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}

fn edit_distance(a: &str, b: &str) -> usize {
    let b_chars: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b_chars.len()).collect();
    let mut curr = vec![0; b_chars.len() + 1];
    for (i, ac) in a.chars().enumerate() {
        curr[0] = i + 1;
        for (j, bc) in b_chars.iter().enumerate() {
            let cost = usize::from(ac != *bc);
            curr[j + 1] = (prev[j + 1] + 1).min(curr[j] + 1).min(prev[j] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[b_chars.len()]
}

/// Scatter one leg across every shard — at most `threads` concurrent
/// upstream requests, the same fan-out control as scans (`shard_fanout`) —
/// and merge to the global per-leg ranking: sort by the leg's score order
/// (BM25 descending, ANN ascending) and truncate to `per_leg_limit`, the
/// same view upstream fusion ranks.
async fn scatter_leg(
    state: &AppState,
    namespace: &str,
    spec: &LegSpec,
    filter: Option<&Value>,
    per_leg_limit: u64,
    include_attributes: Option<&IncludeAttributes>,
    threads: u32,
) -> Result<Vec<QueryResult>, TurbopufferError> {
    use crate::routes::multi_query::{compare_ranked_results, RankMode};
    let results = crate::shards::shard_fanout(state, namespace, threads, |shard_filter| {
        let filter = match shard_filter {
            Some(shard) => Some(crate::shards::combine_filter(filter, shard)),
            None => filter.cloned(),
        };
        async move {
            state
                .turbopuffer()
                .ranked_query(
                    namespace,
                    &spec.rank_by,
                    per_leg_limit as u32,
                    filter.as_ref(),
                    include_attributes,
                )
                .await
                .map(|outcome| outcome.rows)
        }
    })
    .await?;
    let mut rows: Vec<QueryResult> = results.into_iter().flatten().collect();
    let mode = RankMode::from_rank_by(&spec.rank_by);
    rows.sort_by(|a, b| compare_ranked_results(a, b, mode));
    rows.truncate(per_leg_limit as usize);
    Ok(rows)
}

/// The same reciprocal rank fusion upstream computes: each row scores
/// `Σ 1/(rank_constant + rank)` across the legs that returned it, rank
/// 1-based within the leg's merged ranking. Rows sort by `$score`
/// descending with `id` as tiebreaker.
#[cfg(test)]
fn rrf_fuse_legs(legs: &[Vec<QueryResult>], rank_constant: u64) -> Vec<Value> {
    rrf_fuse_legs_with_breakdown(legs, rank_constant, None)
}

#[derive(Debug, Clone)]
struct LegHit {
    rank: u64,
    score: Option<f64>,
}

#[derive(Debug, Clone)]
struct FusedAccumulator {
    score: f64,
    row: QueryResult,
    leg_hits: Option<Vec<Option<LegHit>>>,
}

fn rrf_fuse_legs_with_breakdown(
    legs: &[Vec<QueryResult>],
    rank_constant: u64,
    leg_labels: Option<&[String]>,
) -> Vec<Value> {
    use std::collections::{hash_map::Entry, HashMap};
    let mut order: Vec<String> = Vec::new();
    let mut fused: HashMap<String, FusedAccumulator> = HashMap::new();
    for (leg_idx, leg) in legs.iter().enumerate() {
        for (idx, row) in leg.iter().enumerate() {
            let contribution = 1.0 / (rank_constant as f64 + idx as f64 + 1.0);
            let entry = match fused.entry(row.id.clone()) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    order.push(row.id.clone());
                    entry.insert(FusedAccumulator {
                        score: 0.0,
                        row: row.clone(),
                        leg_hits: leg_labels.map(|labels| vec![None; labels.len()]),
                    })
                }
            };
            entry.score += contribution;
            if let Some(hits) = entry.leg_hits.as_mut() {
                if leg_idx < hits.len() {
                    hits[leg_idx] = Some(LegHit {
                        rank: idx as u64 + 1,
                        score: row.dist,
                    });
                }
            }
        }
    }
    let mut ranked: Vec<(&String, &FusedAccumulator)> =
        order.iter().map(|id| (id, &fused[id])).collect();
    ranked.sort_by(|a, b| {
        b.1.score
            .partial_cmp(&a.1.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(b.0))
    });
    ranked
        .into_iter()
        .map(|(id, entry)| {
            let mut obj = Map::new();
            obj.insert("id".to_string(), Value::String(id.clone()));
            obj.insert("$score".to_string(), Value::from(entry.score));
            for (key, value) in &entry.row.attributes {
                obj.insert(key.clone(), value.clone());
            }
            if let (Some(labels), Some(hits)) = (leg_labels, entry.leg_hits.as_ref()) {
                let legs = labels
                    .iter()
                    .enumerate()
                    .map(
                        |(idx, label)| match hits.get(idx).and_then(Option::as_ref) {
                            Some(hit) => json!({
                                "leg": label,
                                "rank": hit.rank,
                                "score": hit.score,
                            }),
                            None => json!({
                                "leg": label,
                                "rank": Value::Null,
                                "score": Value::Null,
                            }),
                        },
                    )
                    .collect::<Vec<_>>();
                obj.insert("$fused".to_string(), json!({ "legs": legs }));
            }
            Value::Object(obj)
        })
        .collect()
}

// --- Handler ---

pub async fn hybrid_text_query(
    state: Arc<AppState>,
    namespace: String,
    headers: HeaderMap,
    body: Map<String, Value>,
) -> Result<Response, AppError> {
    let rank_by = body.get("rank_by").cloned().unwrap_or(Value::Null);
    let expr = parse_hybrid_text_expr(&rank_by)?;
    let request = parse_hybrid_request(&body)?;
    state.telemetry.touch_hybrid_rrf();
    state.telemetry.touch_fuzzy_surfacing();
    let outcome = run_hybrid_text(&state, &namespace, &expr, &request, None).await;
    match &outcome {
        Ok(out) => {
            state
                .metrics
                .observe_hybrid_text_query(&namespace, STATUS_OK, Some(out.tokens.len()))
        }
        Err(AppError::Validation(_)) => {
            state
                .metrics
                .observe_hybrid_text_query(&namespace, STATUS_LAYER_ERROR, None)
        }
        Err(_) => state
            .metrics
            .observe_hybrid_text_query(&namespace, STATUS_TPUF_ERROR, None),
    }
    let out = outcome?;

    log_hybrid_history(&state, &namespace, &headers, &rank_by, &request, &out);

    let mut response_headers = HeaderMap::new();
    let (traceparent, _) = traceparent_for_query(&headers);
    if let Ok(value) = HeaderValue::from_str(&traceparent) {
        response_headers.insert(TRACEPARENT_HEADER, value);
    }
    insert_optional_u64_header(
        &mut response_headers,
        LAYER_STABLE_AS_OF_HEADER,
        out.watermark,
    );
    if let Some(next_cursor) = out.next_cursor.as_ref() {
        if let Ok(value) = HeaderValue::from_str(next_cursor) {
            response_headers.insert(LAYER_NEXT_CURSOR_HEADER, value);
        }
    }

    let body = json!({
        "rows": out.rows,
        "hybrid": out.echo,
        "next_cursor": out.next_cursor,
    });
    Ok((response_headers, Json(body)).into_response())
}

pub(crate) struct HybridOutcome {
    pub rows: Vec<Value>,
    pub echo: Value,
    pub tokens: Vec<String>,
    pub watermark: Option<u64>,
    pub next_cursor: Option<String>,
}

/// Dispatch a set of leg specs to the right fusion path: upstream RRF on
/// unsharded namespaces, gateway RRF over scatter/gathered legs on sharded
/// ones. Shared by the primary expansion and RFC 0057's surfacing fallback.
async fn run_fused_specs(
    state: &AppState,
    namespace: &str,
    specs: &[LegSpec],
    params: &FusionParams,
    include_attributes: Option<&Value>,
    shard_count: Option<u64>,
    threads: u32,
) -> Result<Vec<Value>, AppError> {
    match shard_count {
        None => run_unsharded_fused(state, namespace, specs, params, include_attributes).await,
        _ => {
            let include: Option<IncludeAttributes> = match include_attributes {
                None => None,
                Some(value) => Some(serde_json::from_value(value.clone()).map_err(|e| {
                    AppError::Validation(format!("invalid include_attributes: {e}"))
                })?),
            };
            run_sharded_fused(state, namespace, specs, params, include.as_ref(), threads).await
        }
    }
}

/// The expansion shared by the `HybridText` handler and the router's
/// `hybrid_text` / `fused` routes: tokenize, build leg specs (plus any
/// router-supplied extra leg), fuse — upstream RRF on unsharded namespaces,
/// gateway RRF over scatter/gathered legs on sharded ones — and truncate to
/// `top_k`. The `hybrid` echo block always reflects the effective expansion.
pub(crate) async fn run_hybrid_text(
    state: &AppState,
    namespace: &str,
    expr: &HybridTextExpr,
    request: &HybridRequest,
    extra_leg: Option<LegSpec>,
) -> Result<HybridOutcome, AppError> {
    let policy = tokenize_query_input_with_stopwords(&expr.input, &expr.stopwords);
    // Zero-token guard (RFC 0090): stop-word removal must not turn a valid
    // input into a 422 — an all-stop-word query keeps its BM25 anchor leg
    // and fuses on that alone. Only an input that yields no tokens *before*
    // the stop-word stage is invalid.
    if policy.tokens.is_empty() && policy.stopwords_dropped.is_empty() {
        return Err(AppError::Validation(
            "HybridText input yields no tokens under the tokenizer policy".to_string(),
        ));
    }

    // Interim clamp: the search store's fuzzy match misbehaves for edit
    // distance >= 1 / auto (token-independent match-all), so on `kind=search`
    // namespaces the default `auto` fuzziness degrades to exact-only, which
    // fuses BM25 + semantic correctly. An explicit numeric `fuzziness` still
    // passes through so callers can probe the engine fix. Remove when the
    // engine-side fuzzy match is fixed.
    let clamped = expr.fuzziness == Fuzziness::Auto && state.namespace_uses_search_store(namespace);
    let expr = if clamped {
        let mut clamped_expr = expr.clone();
        clamped_expr.fuzziness = Fuzziness::Fixed(0);
        std::borrow::Cow::Owned(clamped_expr)
    } else {
        std::borrow::Cow::Borrowed(expr)
    };
    let expr: &HybridTextExpr = &expr;

    let watermark = state.consistency.get(namespace);
    let inject_filter = state.consistency.should_inject_filter(namespace);
    let shard_count = active_shard_count(state, namespace).await;
    let offset = request.cursor.as_ref().map_or(0, |cursor| cursor.offset);
    let page_end = offset.checked_add(request.top_k).ok_or_else(|| {
        AppError::Validation("cursor offset exceeds fused pagination depth".to_string())
    })?;
    // Per-leg retrieval depth scales with how deep this page reaches
    // (`page_end`), not the +1 has-next look-ahead — that signal is derived
    // from `page_end` directly below (`has_more`). Feeding `page_end + 1` here
    // only inflated per_leg_limit past the documented clamp(5 × top_k, 50, 200)
    // (api/query.mdx): a plain top_k:10 query echoed 55 instead of 50.
    let fetch_depth = page_end.min(FUSED_CURSOR_MAX_OFFSET);
    let per_leg_limit = expr
        .per_leg_limit
        .unwrap_or_else(|| default_per_leg_limit(fetch_depth));

    let had_extra_leg = extra_leg.is_some();
    let mut specs = build_hybrid_leg_specs(expr, &policy.tokens, request.filters.as_ref());
    if let Some(extra) = extra_leg {
        specs.push(extra);
    }
    let mut effective_leg_count = specs.len();

    let params = FusionParams {
        watermark,
        inject_filter,
        per_leg_limit,
        rank_constant: expr.rank_constant,
        include_leg_breakdown: request.include_leg_breakdown,
        temporal_filter: request.temporal_filter.clone(),
    };
    // Fan-out control shares the scans `threads` vocabulary and resolution:
    // request option, else `Index.spec.scan.threads`, else 8 — clamped to
    // active shards. Only meaningful when the namespace is sharded.
    let threads = crate::routes::scans::resolve_scan_threads_with_active(
        state,
        namespace,
        expr.threads,
        shard_count,
    );
    let mut rows = run_fused_specs(
        state,
        namespace,
        &specs,
        &params,
        request.include_attributes.as_ref(),
        shard_count,
        threads,
    )
    .await?;

    // RFC 0057: empty-result fallback. Every primary leg ranks by BM25 over the
    // full input, which upstream scores at zero — and drops — when no token
    // matches a stored term exactly, so a fully-misspelled query fuses to zero
    // rows. When that happens (and we are not inside the router's fused/`Auto`
    // path, which owns its own semantics), collect fuzzy candidates and rank
    // each leg gateway-side by field/token edit distance before RRF. Working
    // queries never reach this branch — the fallback is purely additive.
    // An all-stop-word query has no fuzzy tokens to surface on; the BM25-only
    // fusion result stands.
    let surfaced = rows.is_empty() && !had_extra_leg && !policy.tokens.is_empty();
    if surfaced {
        let surfacing = build_surfacing_leg_specs(expr, &policy.tokens, request.filters.as_ref());
        effective_leg_count = surfacing.len();
        rows = run_surfacing_fused(
            state,
            namespace,
            expr,
            &surfacing,
            &policy.tokens,
            &params,
            request.include_attributes.as_ref(),
            shard_count,
            threads,
        )
        .await?;
    }
    let has_more = rows.len() > page_end as usize;
    rows = rows
        .into_iter()
        .skip(offset as usize)
        .take(request.top_k as usize)
        .collect();
    let next_cursor = if has_more && page_end < FUSED_CURSOR_MAX_OFFSET {
        Some(FusedCursor::next(page_end).encode())
    } else {
        None
    };

    let mut echo = json!({
        "tokens": policy.tokens,
        "tokens_dropped": policy.dropped_by_cap,
        "stopwords": expr.stopwords.echo(),
        "stopwords_dropped": policy.stopwords_dropped,
        "fuzziness": expr.fuzziness.echo(),
        "rank_constant": expr.rank_constant,
        "legs": effective_leg_count,
        "per_leg_limit": per_leg_limit,
    });
    // The fan-out width only exists on the scatter/gather path; echo it
    // there so the effective (clamped) value is visible, same posture as
    // the scan job status reporting its threads.
    if shard_count.is_some() {
        echo["threads"] = json!(threads);
    }
    // Surface when the empty-result fallback fired, so the behavior is visible
    // in the echo (same posture as the rest of the expansion report).
    if surfaced {
        echo["surfaced"] = json!(true);
    }
    // Make the interim kind=search clamp visible: the echoed `fuzziness` is
    // the effective (clamped) value, and this flag says why it differs from
    // the request.
    if clamped {
        echo["fuzziness_clamped"] = json!(true);
    }

    Ok(HybridOutcome {
        rows,
        echo,
        tokens: policy.tokens,
        watermark,
        next_cursor,
    })
}

pub(crate) fn log_hybrid_history(
    state: &AppState,
    namespace: &str,
    headers: &HeaderMap,
    rank_by: &Value,
    request: &HybridRequest,
    out: &HybridOutcome,
) {
    let (_, trace_id) = traceparent_for_query(headers);
    let tags = tags_from_headers(headers).unwrap_or_default();
    let (timestamp, timestamp_nanos) = now_timestamp();
    let top_result_ids = out
        .rows
        .iter()
        .take(10)
        .filter_map(|row| row.get("id").and_then(Value::as_str).map(str::to_string))
        .collect();
    let entry = SearchHistoryEntry {
        timestamp,
        timestamp_nanos,
        namespace: namespace.to_string(),
        trace_id: Some(trace_id),
        raw_query: header_to_string(headers, "x-hevlayer-search-query"),
        stable_as_of: out.watermark,
        // The entry carries the expression, not the expanded legs, so a
        // replay reproduces the whole expansion as a unit.
        query: json!({
            "rank_by": rank_by,
            "top_k": request.top_k,
            "filters": request.filters,
            "include_leg_breakdown": request.include_leg_breakdown,
            "hybrid": out.echo,
        }),
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

    #[test]
    fn tokenizer_lowercases_and_splits_on_word_boundaries() {
        let got = tokenize_query_input("Conection TIMOUT kubernets");
        assert_eq!(got.tokens, vec!["conection", "timout", "kubernets"]);
        assert_eq!(got.dropped_by_cap, 0);
    }

    #[test]
    fn tokenizer_drops_punctuation_and_short_tokens() {
        let got = tokenize_query_input("a b, -- cd!! (e)");
        assert_eq!(got.tokens, vec!["cd"]);
    }

    #[test]
    fn tokenizer_dedupes_preserving_order() {
        let got = tokenize_query_input("red shoe RED Shoe blue");
        assert_eq!(got.tokens, vec!["red", "shoe", "blue"]);
    }

    #[test]
    fn tokenizer_caps_at_fifteen_and_counts_dropped() {
        let input = (0..18)
            .map(|i| format!("tok{i:02}"))
            .collect::<Vec<_>>()
            .join(" ");
        let got = tokenize_query_input(&input);
        assert_eq!(got.tokens.len(), MAX_QUERY_TOKENS);
        assert_eq!(got.dropped_by_cap, 3);
    }

    #[test]
    fn tokenizer_handles_empty_and_punctuation_only_input() {
        assert!(tokenize_query_input("").tokens.is_empty());
        assert!(tokenize_query_input("!?! .. --").tokens.is_empty());
    }

    // --- RFC 0090: stop words dropped from the fuzzy-leg token set ---

    #[test]
    fn stopwords_drop_from_fuzzy_token_set_by_default() {
        // The motivating shelf query: `a` is already gone (< 2 chars);
        // `to` must now drop as a stop word, content tokens survive.
        let got = tokenize_query_input_with_stopwords(
            "a quest to destroy a magic ring",
            &StopwordsOption::English,
        );
        assert_eq!(got.tokens, vec!["quest", "destroy", "magic", "ring"]);
        assert_eq!(got.stopwords_dropped, vec!["to"]);
        assert_eq!(got.dropped_by_cap, 0);
    }

    #[test]
    fn stopwords_off_reproduces_old_behavior() {
        let got = tokenize_query_input_with_stopwords(
            "a quest to destroy a magic ring",
            &StopwordsOption::Off,
        );
        assert_eq!(got.tokens, vec!["quest", "to", "destroy", "magic", "ring"]);
        assert!(got.stopwords_dropped.is_empty());
    }

    #[test]
    fn stopwords_custom_list_overrides_builtin() {
        let got = tokenize_query_input_with_stopwords(
            "the quick brown fox",
            &StopwordsOption::Custom(vec!["quick".to_string()]),
        );
        // `the` survives (not in the custom list); `quick` drops.
        assert_eq!(got.tokens, vec!["the", "brown", "fox"]);
        assert_eq!(got.stopwords_dropped, vec!["quick"]);
    }

    #[test]
    fn stopwords_drop_before_the_cap_spends_legs_on_content() {
        // 14 content tokens + 3 stop words: the stop words must not consume
        // fuzzy-leg budget, so no content token is cut by the cap.
        let mut words: Vec<String> = (0..14).map(|i| format!("tok{i:02}")).collect();
        words.insert(3, "the".to_string());
        words.insert(7, "and".to_string());
        words.insert(11, "with".to_string());
        let got = tokenize_query_input_with_stopwords(&words.join(" "), &StopwordsOption::English);
        assert_eq!(got.tokens.len(), 14);
        assert!(got.tokens.iter().all(|t| t.starts_with("tok")));
        assert_eq!(got.stopwords_dropped, vec!["the", "and", "with"]);
        assert_eq!(got.dropped_by_cap, 0);
    }

    #[test]
    fn all_stopword_input_keeps_empty_token_set_with_drops_recorded() {
        let got = tokenize_query_input_with_stopwords("the to and", &StopwordsOption::English);
        assert!(got.tokens.is_empty());
        assert_eq!(got.stopwords_dropped, vec!["the", "to", "and"]);
    }

    #[test]
    fn parse_stopwords_accepts_en_false_and_lists() {
        assert_eq!(
            parse_stopwords(&json!("en")).unwrap(),
            StopwordsOption::English
        );
        assert_eq!(
            parse_stopwords(&json!(false)).unwrap(),
            StopwordsOption::Off
        );
        assert_eq!(
            parse_stopwords(&json!(["The", "ring"])).unwrap(),
            StopwordsOption::Custom(vec!["the".to_string(), "ring".to_string()])
        );
        assert!(parse_stopwords(&json!(true)).is_err());
        assert!(parse_stopwords(&json!("fr")).is_err());
        assert!(parse_stopwords(&json!([1, 2])).is_err());
    }

    #[test]
    fn parse_hybrid_text_accepts_stopwords_option() {
        let expr = parse_hybrid_text_expr(&json!([
            "content",
            "HybridText",
            "a quest to destroy",
            {"stopwords": false}
        ]))
        .unwrap();
        assert_eq!(expr.stopwords, StopwordsOption::Off);
        let expr = parse_hybrid_text_expr(&json!(["content", "HybridText", "a quest to destroy"]))
            .unwrap();
        assert_eq!(expr.stopwords, StopwordsOption::English);
    }

    #[test]
    fn fuzziness_builds_the_max_edit_distance_ladder() {
        // Auto: full ladder up to distance 2, honoring upstream's
        // `min_query_chars >= 3 * (distance + 1)` floor.
        assert_eq!(
            Fuzziness::Auto.max_edit_distance(),
            json!([
                {"min_query_chars": 3, "distance": 0},
                {"min_query_chars": 6, "distance": 1},
                {"min_query_chars": 9, "distance": 2}
            ])
        );
        // Fixed(1) caps the ladder at distance 1.
        assert_eq!(
            Fuzziness::Fixed(1).max_edit_distance(),
            json!([
                {"min_query_chars": 3, "distance": 0},
                {"min_query_chars": 6, "distance": 1}
            ])
        );
        // Fixed(0) is exact-only.
        assert_eq!(
            Fuzziness::Fixed(0).max_edit_distance(),
            json!([{"min_query_chars": 3, "distance": 0}])
        );
    }

    #[test]
    fn parse_rejects_malformed_tuples() {
        assert!(parse_hybrid_text_expr(&json!(["content", "HybridText"])).is_err());
        assert!(parse_hybrid_text_expr(&json!([42, "HybridText", "q"])).is_err());
        assert!(parse_hybrid_text_expr(&json!(["content", "HybridText", 42])).is_err());
        assert!(
            parse_hybrid_text_expr(&json!(["content", "HybridText", "q", {"bogus": 1}])).is_err()
        );
    }

    #[test]
    fn parse_applies_documented_defaults() {
        let expr = parse_hybrid_text_expr(&json!(["content", "HybridText", "q"])).unwrap();
        assert_eq!(expr.fuzziness, Fuzziness::Auto);
        assert_eq!(expr.rank_constant, 60);
        assert_eq!(expr.per_leg_limit, None);
        assert_eq!(expr.threads, None);
    }

    #[test]
    fn parse_accepts_threads_like_scans() {
        let expr =
            parse_hybrid_text_expr(&json!(["content", "HybridText", "q", {"threads": 4}])).unwrap();
        assert_eq!(expr.threads, Some(4));
    }

    #[test]
    fn parse_rejects_out_of_range_options() {
        for options in [
            json!({"fuzziness": 3}),
            json!({"fuzziness": "fuzzy"}),
            json!({"rank_constant": 0}),
            json!({"per_leg_limit": 0}),
            json!({"threads": 0}),
            json!({"threads": "many"}),
        ] {
            assert!(
                parse_hybrid_text_expr(&json!(["content", "HybridText", "q", options])).is_err()
            );
        }
    }

    #[test]
    fn per_leg_limit_clamps_to_documented_band() {
        assert_eq!(default_per_leg_limit(1), 50);
        assert_eq!(default_per_leg_limit(10), 50);
        assert_eq!(default_per_leg_limit(20), 100);
        assert_eq!(default_per_leg_limit(100), 200);
    }

    fn expr(input: &str) -> HybridTextExpr {
        HybridTextExpr {
            field: "content".to_string(),
            input: input.to_string(),
            fuzziness: Fuzziness::Auto,
            stopwords: StopwordsOption::default(),
            rank_constant: 60,
            per_leg_limit: None,
            threads: None,
        }
    }

    #[test]
    fn expansion_matches_rfc_shape() {
        let expr = expr("conection timout kubernets");
        let tokens = vec![
            "conection".to_string(),
            "timout".to_string(),
            "kubernets".to_string(),
        ];
        let base = json!(["tenant", "Eq", "t-42"]);
        let specs = build_hybrid_leg_specs(&expr, &tokens, Some(&base));
        let legs = render_leg_bodies(&specs, None, None, 50, None);
        assert_eq!(legs.len(), 4);
        // BM25 leg: caller filter only.
        assert_eq!(
            legs[0]["rank_by"],
            json!(["content", "BM25", "conection timout kubernets"])
        );
        assert_eq!(legs[0]["filters"], base);
        assert_eq!(legs[0]["top_k"], json!(50));
        // Fuzzy legs: same rank_by, filter ANDs in the per-token predicate.
        assert_eq!(legs[1]["rank_by"], legs[0]["rank_by"]);
        assert_eq!(
            legs[1]["filters"],
            json!(["And", [base, ["content", "Fuzzy", "conection", {"max_edit_distance": [
                {"min_query_chars": 3, "distance": 0},
                {"min_query_chars": 6, "distance": 1},
                {"min_query_chars": 9, "distance": 2}
            ]}]]])
        );
    }

    #[test]
    fn expansion_without_filter_uses_bare_fuzzy_predicate() {
        let expr = expr("timout");
        let specs = build_hybrid_leg_specs(&expr, &["timout".to_string()], None);
        let legs = render_leg_bodies(&specs, None, None, 50, None);
        assert_eq!(legs.len(), 2);
        assert!(legs[0].get("filters").is_none());
        assert_eq!(
            legs[1]["filters"],
            json!(["content", "Fuzzy", "timout", {"max_edit_distance": [
                {"min_query_chars": 3, "distance": 0},
                {"min_query_chars": 6, "distance": 1},
                {"min_query_chars": 9, "distance": 2}
            ]}])
        );
    }

    #[test]
    fn surfacing_legs_collect_fuzzy_candidates_with_one_leg_per_token() {
        // RFC 0057 fallback: one candidate-collection leg per token, filtered
        // to rows fuzzy-matching that token. No BM25 leg.
        let expr = expr("conection timout");
        let tokens = vec!["conection".to_string(), "timout".to_string()];
        let base = json!(["tenant", "Eq", "t-42"]);
        let specs = build_surfacing_leg_specs(&expr, &tokens, Some(&base));
        let legs = render_leg_bodies(&specs, None, None, 50, None);
        assert_eq!(legs.len(), 2, "one leg per token, no BM25 leg");
        let ladder = json!([
            {"min_query_chars": 3, "distance": 0},
            {"min_query_chars": 6, "distance": 1},
            {"min_query_chars": 9, "distance": 2}
        ]);
        for (leg, token) in legs.iter().zip(&tokens) {
            // This is only the upstream candidate window order; the gateway
            // reorders each surfacing leg by edit distance before RRF.
            assert_eq!(leg["rank_by"], json!(["id", "asc"]));
            assert_eq!(
                leg["filters"],
                json!(["And", [base, ["content", "Fuzzy", token, {"max_edit_distance": ladder}]]])
            );
        }
    }

    #[test]
    fn surfacing_legs_without_filter_use_bare_fuzzy_predicate() {
        let expr = expr("timout");
        let specs = build_surfacing_leg_specs(&expr, &["timout".to_string()], None);
        let legs = render_leg_bodies(&specs, None, None, 50, None);
        assert_eq!(legs.len(), 1);
        assert_eq!(legs[0]["rank_by"], json!(["id", "asc"]));
        assert_eq!(
            legs[0]["filters"],
            json!(["content", "Fuzzy", "timout", {"max_edit_distance": [
                {"min_query_chars": 3, "distance": 0},
                {"min_query_chars": 6, "distance": 1},
                {"min_query_chars": 9, "distance": 2}
            ]}])
        );
    }

    #[test]
    fn surfacing_distance_prefers_nearest_field_token_over_low_id() {
        let mut low_id_far = result("0001");
        low_id_far
            .attributes
            .insert("content".to_string(), json!("glimepiride section"));
        let mut high_id_close = result("9999");
        high_id_close
            .attributes
            .insert("content".to_string(), json!("metformin hydrochloride"));

        let mut rows = [low_id_far, high_id_close];
        rows.sort_by(|a, b| compare_surfacing_rows(a, b, "content", "metaformin"));

        assert_eq!(rows[0].id, "9999");
    }

    #[test]
    fn surfacing_internal_field_is_added_only_when_needed() {
        assert_eq!(
            include_with_field(Some(&json!(["generic_name"])), "content")
                .unwrap()
                .to_turbopuffer_value(),
            json!(["generic_name", "content"])
        );
        assert!(!caller_requested_field(
            Some(&json!(["generic_name"])),
            "content"
        ));
        assert!(caller_requested_field(Some(&json!(true)), "content"));
    }

    #[test]
    fn rendering_with_watermark_conjoins_the_same_cut_into_every_leg() {
        let expr = expr("timout kubernets");
        let tokens = vec!["timout".to_string(), "kubernets".to_string()];
        let specs = build_hybrid_leg_specs(&expr, &tokens, None);
        let legs = render_leg_bodies(&specs, Some(1234), None, 50, None);
        for leg in &legs {
            let filter = leg["filters"].to_string();
            assert!(
                filter.contains("1234"),
                "every leg carries the watermark cut, got {filter}"
            );
        }
    }

    fn result(id: &str) -> QueryResult {
        QueryResult {
            id: id.to_string(),
            dist: Some(0.0),
            attributes: Default::default(),
        }
    }

    #[test]
    fn gateway_rrf_matches_the_upstream_formula() {
        // Leg A ranks [x, y]; leg B ranks [y, z]. With k=60:
        // x = 1/61, y = 1/62 + 1/61, z = 1/62 → y, x, z.
        let legs = vec![
            vec![result("x"), result("y")],
            vec![result("y"), result("z")],
        ];
        let rows = rrf_fuse_legs(&legs, 60);
        let ids: Vec<&str> = rows.iter().map(|r| r["id"].as_str().unwrap()).collect();
        assert_eq!(ids, vec!["y", "x", "z"]);
        let y_score = rows[0]["$score"].as_f64().unwrap();
        assert!((y_score - (1.0 / 62.0 + 1.0 / 61.0)).abs() < 1e-12);
        assert!(rows.iter().all(|r| r.get("$dist").is_none()));
    }

    #[test]
    fn gateway_rrf_can_emit_per_leg_breakdown() {
        let mut x = result("x");
        x.dist = Some(11.2);
        let mut y_a = result("y");
        y_a.dist = Some(7.4);
        let mut y_b = result("y");
        y_b.dist = Some(0.25);
        let legs = vec![vec![x, y_a], vec![y_b]];
        let labels = vec!["bm25".to_string(), "semantic".to_string()];
        let rows = rrf_fuse_legs_with_breakdown(&legs, 60, Some(&labels));

        let y = rows.iter().find(|row| row["id"] == "y").unwrap();
        assert_eq!(
            y["$fused"]["legs"],
            json!([
                {"leg": "bm25", "rank": 2, "score": 7.4},
                {"leg": "semantic", "rank": 1, "score": 0.25}
            ])
        );
        let x = rows.iter().find(|row| row["id"] == "x").unwrap();
        assert_eq!(
            x["$fused"]["legs"],
            json!([
                {"leg": "bm25", "rank": 1, "score": 11.2},
                {"leg": "semantic", "rank": null, "score": null}
            ])
        );
    }

    #[test]
    fn fused_rows_accepts_rows_and_single_result_wrapper() {
        let rows = json!([{"id": "a", "$score": 0.1}]);
        assert_eq!(
            fused_rows(&json!({"rows": rows})).unwrap(),
            rows.as_array().unwrap().clone()
        );
        assert_eq!(
            fused_rows(&json!({"results": [{"rows": rows}]})).unwrap(),
            rows.as_array().unwrap().clone()
        );
        assert!(fused_rows(&json!({"results": [{}, {}]})).is_err());
        assert!(fused_rows(&json!({})).is_err());
    }

    #[test]
    fn ensure_fusable_duplicates_a_lone_leg() {
        // Upstream RRF rejects a single-query rerank ("requires multiple
        // queries"); the one-token surfacing fallback would otherwise send one
        // leg. Duplicating it keeps the same rows and order under RRF.
        let one = vec![json!({"rank_by": ["id", "asc"], "top_k": 50})];
        let doubled = ensure_fusable(one.clone());
        assert_eq!(doubled.len(), 2);
        assert_eq!(doubled[0], doubled[1]);
        assert_eq!(doubled[0], one[0]);
        // Two or more legs pass through untouched.
        let many = vec![json!({"a": 1}), json!({"b": 2})];
        assert_eq!(ensure_fusable(many.clone()), many);
    }

    #[test]
    fn request_envelope_accepts_cursor_and_rejects_vector_shapes() {
        let body = |extra: Value| {
            let mut map = Map::new();
            map.insert("rank_by".to_string(), json!(["c", "HybridText", "q"]));
            if let Value::Object(extra) = extra {
                map.extend(extra);
            }
            map
        };
        let cursor = FusedCursor::next(10).encode();
        let parsed = parse_hybrid_request(&body(json!({"cursor": cursor}))).unwrap();
        assert_eq!(parsed.cursor.unwrap().offset(), 10);
        assert!(parse_hybrid_request(&body(json!({"vector": [0.1]}))).is_err());
        assert!(parse_hybrid_request(&body(json!({"nearest_to_id": ["a"]}))).is_err());
        assert!(parse_hybrid_request(&body(json!({"top_k": 0}))).is_err());
        let ok = parse_hybrid_request(&body(json!({}))).unwrap();
        assert_eq!(ok.top_k, 10);
        assert!(!ok.include_leg_breakdown);
        let with_breakdown =
            parse_hybrid_request(&body(json!({"include_leg_breakdown": true}))).unwrap();
        assert!(with_breakdown.include_leg_breakdown);
        assert!(parse_hybrid_request(&body(json!({"include_leg_breakdown": "yes"}))).is_err());
    }

    #[test]
    fn request_envelope_strips_vector_from_include_attributes() {
        let mut map = Map::new();
        map.insert("rank_by".to_string(), json!(["c", "HybridText", "q"]));
        map.insert("include_attributes".to_string(), json!(["title", "vector"]));
        let parsed = parse_hybrid_request(&map).unwrap();
        assert_eq!(parsed.include_attributes, Some(json!(["title"])));
    }

    #[test]
    fn nested_layer_operator_detection() {
        assert!(queries_contain_layer_operator(&json!({
            "queries": [
                {"rank_by": ["t", "BM25", "x"]},
                {"rank_by": ["c", "HybridText", "q"]}
            ]
        })));
        assert!(queries_contain_layer_operator(&json!({
            "queries": [{"rank_by": ["c", "Auto", "q"]}]
        })));
        assert!(!queries_contain_layer_operator(&json!({
            "queries": [{"rank_by": ["t", "BM25", "x"]}]
        })));
    }
}
