use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::http::{HeaderMap, HeaderValue};
use tracing::{error, warn};
use uuid::Uuid;

use crate::clients::aerospike::AerospikeClient;
use crate::clients::s3::S3Client;
use crate::models::{
    ClickstreamEvent, ClickstreamListResponse, SearchHistoryEntry, SearchHistoryListResponse,
};

const SEARCH_HISTORY_CACHE_PREFIX: &str = "_hevlayer_search_history_";
pub const TRACEPARENT_HEADER: &str = "traceparent";
pub const HISTORY_TAG_MAX_COUNT: usize = 32;
pub const HISTORY_TAG_MAX_LENGTH: usize = 128;

#[derive(Debug, Clone)]
pub struct SearchHistoryListOptions {
    pub tags: Vec<String>,
    pub from_nanos: Option<u64>,
    pub to_nanos: Option<u64>,
    pub before_nanos: Option<u64>,
    pub limit: usize,
}

#[derive(Debug, Clone)]
pub struct ClickstreamListOptions {
    pub tags: Vec<String>,
    pub trace_id: Option<String>,
    pub from_nanos: Option<u64>,
    pub to_nanos: Option<u64>,
    pub before_nanos: Option<u64>,
    pub limit: usize,
}

pub fn search_history_cache_namespace(namespace: &str) -> String {
    format!("{SEARCH_HISTORY_CACHE_PREFIX}{namespace}")
}

pub fn now_timestamp() -> (String, u64) {
    let now = SystemTime::now();
    let timestamp = humantime::format_rfc3339_millis(now).to_string();
    let nanos = now
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    (timestamp, nanos)
}

pub fn tags_from_headers(headers: &HeaderMap) -> Result<Vec<String>, String> {
    let mut tags = Vec::new();
    collect_tags(headers.get_all("x-hevlayer-tag").iter(), &mut tags)?;
    collect_tags(headers.get_all("x-hevlayer-tags").iter(), &mut tags)?;
    normalize_history_tags(tags)
}

pub fn normalize_history_tags<I, S>(raw_tags: I) -> Result<Vec<String>, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut tags = Vec::new();
    for raw_tag in raw_tags {
        let tag = raw_tag.as_ref().trim();
        if tag.is_empty() {
            continue;
        }
        validate_history_tag(tag)?;
        tags.push(tag.to_string());
    }

    tags.sort();
    tags.dedup();
    if tags.len() > HISTORY_TAG_MAX_COUNT {
        return Err(format!(
            "history tags are limited to {HISTORY_TAG_MAX_COUNT} unique tags"
        ));
    }
    Ok(tags)
}

pub fn header_to_string(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

pub fn traceparent_for_query(headers: &HeaderMap) -> (String, String) {
    if let Some(value) = headers
        .get(TRACEPARENT_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(valid_traceparent)
    {
        return value;
    }
    mint_traceparent()
}

pub fn trace_id_from_headers(headers: &HeaderMap) -> Option<String> {
    headers
        .get(TRACEPARENT_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(valid_traceparent)
        .map(|(_, trace_id)| trace_id)
}

fn collect_tags<'a>(
    values: impl Iterator<Item = &'a HeaderValue>,
    tags: &mut Vec<String>,
) -> Result<(), String> {
    for value in values {
        let value = value
            .to_str()
            .map_err(|_| "history tags must be ASCII header values".to_string())?;
        for tag in value
            .split(',')
            .map(str::trim)
            .filter(|tag| !tag.is_empty())
        {
            tags.push(tag.to_string());
        }
    }
    Ok(())
}

fn validate_history_tag(tag: &str) -> Result<(), String> {
    if tag.len() > HISTORY_TAG_MAX_LENGTH {
        return Err(format!(
            "history tag {tag:?} exceeds {HISTORY_TAG_MAX_LENGTH} bytes"
        ));
    }
    if !tag.chars().all(is_history_tag_char) {
        return Err(format!(
            "history tag {tag:?} contains unsupported characters; use ASCII letters, digits, ':', '_', '-', '.', '/', '=', or '+'. Commas separate tags and cannot be escaped"
        ));
    }
    Ok(())
}

fn is_history_tag_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, ':' | '_' | '-' | '.' | '/' | '=' | '+')
}

fn mint_traceparent() -> (String, String) {
    let trace_id = Uuid::new_v4().simple().to_string();
    let parent_source = Uuid::new_v4().simple().to_string();
    let parent_id = &parent_source[..16];
    (format!("00-{trace_id}-{parent_id}-01"), trace_id)
}

fn valid_traceparent(value: &str) -> Option<(String, String)> {
    let mut parts = value.trim().split('-');
    let version = parts.next()?;
    let trace_id = parts.next()?;
    let parent_id = parts.next()?;
    let flags = parts.next()?;
    if parts.next().is_some()
        || version.len() != 2
        || trace_id.len() != 32
        || parent_id.len() != 16
        || flags.len() != 2
        || trace_id.chars().all(|c| c == '0')
        || parent_id.chars().all(|c| c == '0')
        || !version.chars().all(|c| c.is_ascii_hexdigit())
        || !trace_id.chars().all(|c| c.is_ascii_hexdigit())
        || !parent_id.chars().all(|c| c.is_ascii_hexdigit())
        || !flags.chars().all(|c| c.is_ascii_hexdigit())
    {
        return None;
    }
    Some((value.trim().to_string(), trace_id.to_ascii_lowercase()))
}

pub async fn log_search_history(
    aerospike: Arc<dyn AerospikeClient>,
    s3: Arc<dyn S3Client>,
    entry: SearchHistoryEntry,
) {
    let json = match serde_json::to_string(&entry) {
        Ok(json) => json,
        Err(e) => {
            error!(error = %e, "Failed to serialize search history entry");
            return;
        }
    };

    let cache_namespace = search_history_cache_namespace(&entry.namespace);
    let id = entry.timestamp_nanos.to_string();
    let mut cache_doc = HashMap::new();
    cache_doc.insert("entry".to_string(), serde_json::Value::String(json.clone()));
    if let Err(e) = aerospike.put(&cache_namespace, &id, &cache_doc).await {
        warn!(
            namespace = %entry.namespace,
            error = %e,
            "Failed to write search history entry to Aerospike"
        );
    }

    let key = search_history_s3_key(&entry);
    let line = format!("{json}\n");
    if let Err(e) = s3.put_if_not_exists(&key, line.into_bytes()).await {
        warn!(
            namespace = %entry.namespace,
            key = %key,
            error = %e,
            "Failed to write search history entry to S3"
        );
    }
}

pub async fn list_search_history(
    aerospike: &dyn AerospikeClient,
    s3: &dyn S3Client,
    namespace: &str,
    options: SearchHistoryListOptions,
) -> Result<SearchHistoryListResponse, String> {
    let mut entries = Vec::new();
    let mut seen = HashSet::new();

    let cache_namespace = search_history_cache_namespace(namespace);
    match aerospike.scan(&cache_namespace, None).await {
        Ok(rows) => {
            for (_, doc) in rows {
                if let Some(entry) = search_history_from_cache_doc(doc) {
                    push_matching(namespace, &options, &mut seen, &mut entries, entry);
                }
            }
        }
        Err(e) => {
            warn!(
                namespace = %namespace,
                error = %e,
                "Search history Aerospike read failed; falling back to S3"
            );
        }
    }

    if entries.len() < options.limit.saturating_add(1) {
        let prefix = format!("search-history/{namespace}/");
        let mut keys = s3.list_keys(&prefix).await.map_err(|e| e.to_string())?;
        keys.reverse();
        for key in keys {
            let Some(bytes) = s3.get(&key).await.map_err(|e| e.to_string())? else {
                continue;
            };
            for line in String::from_utf8_lossy(&bytes).lines() {
                if line.trim().is_empty() {
                    continue;
                }
                match serde_json::from_str::<SearchHistoryEntry>(line) {
                    Ok(entry) => {
                        push_matching(namespace, &options, &mut seen, &mut entries, entry);
                    }
                    Err(e) => {
                        warn!(key = %key, error = %e, "Skipping malformed search history entry");
                    }
                }
            }
            if entries.len() >= options.limit.saturating_add(1) {
                break;
            }
        }
    }

    entries.sort_by_key(|e| std::cmp::Reverse(e.timestamp_nanos));
    let next_cursor = if entries.len() > options.limit {
        entries
            .get(options.limit.saturating_sub(1))
            .map(|entry| entry.timestamp_nanos.to_string())
    } else {
        None
    };
    entries.truncate(options.limit);

    Ok(SearchHistoryListResponse {
        entries,
        next_cursor,
    })
}

pub async fn log_clickstream_events(s3: Arc<dyn S3Client>, events: Vec<ClickstreamEvent>) {
    for event in events {
        let json = match serde_json::to_string(&event) {
            Ok(json) => json,
            Err(e) => {
                error!(error = %e, "Failed to serialize clickstream event");
                continue;
            }
        };
        let key = clickstream_s3_key(&event);
        let line = format!("{json}\n");
        if let Err(e) = s3.put_if_not_exists(&key, line.into_bytes()).await {
            warn!(
                namespace = %event.namespace,
                key = %key,
                error = %e,
                "Failed to write clickstream event to S3"
            );
        }
    }
}

pub async fn list_clickstream_events(
    s3: &dyn S3Client,
    namespace: &str,
    options: ClickstreamListOptions,
) -> Result<ClickstreamListResponse, String> {
    let prefix = format!("clickstream/{namespace}/");
    let mut keys = s3.list_keys(&prefix).await.map_err(|e| e.to_string())?;
    keys.reverse();

    let mut events = Vec::new();
    let mut seen = HashSet::new();
    for key in keys {
        let Some(bytes) = s3.get(&key).await.map_err(|e| e.to_string())? else {
            continue;
        };
        for line in String::from_utf8_lossy(&bytes).lines() {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<ClickstreamEvent>(line) {
                Ok(event) => {
                    if matches_clickstream_event(namespace, &options, &event)
                        && seen.insert((event.timestamp_nanos, event.doc_id.clone()))
                    {
                        events.push(event);
                    }
                }
                Err(e) => {
                    warn!(key = %key, error = %e, "Skipping malformed clickstream event");
                }
            }
        }
        if events.len() >= options.limit.saturating_add(1) {
            break;
        }
    }

    events.sort_by_key(|e| std::cmp::Reverse(e.timestamp_nanos));
    let next_cursor = if events.len() > options.limit {
        events
            .get(options.limit.saturating_sub(1))
            .map(|event| event.timestamp_nanos.to_string())
    } else {
        None
    };
    events.truncate(options.limit);

    Ok(ClickstreamListResponse {
        events,
        next_cursor,
    })
}

fn search_history_s3_key(entry: &SearchHistoryEntry) -> String {
    let date = &entry.timestamp[..10];
    format!(
        "search-history/{}/{}/{}.jsonl",
        entry.namespace, date, entry.timestamp_nanos
    )
}

fn clickstream_s3_key(event: &ClickstreamEvent) -> String {
    let date = &event.timestamp[..10];
    format!(
        "clickstream/{}/{}/{}.jsonl",
        event.namespace, date, event.timestamp_nanos
    )
}

fn search_history_from_cache_doc(
    doc: HashMap<String, serde_json::Value>,
) -> Option<SearchHistoryEntry> {
    doc.get("entry")
        .and_then(|value| value.as_str())
        .and_then(|json| serde_json::from_str(json).ok())
}

fn push_matching(
    namespace: &str,
    options: &SearchHistoryListOptions,
    seen: &mut HashSet<u64>,
    entries: &mut Vec<SearchHistoryEntry>,
    entry: SearchHistoryEntry,
) {
    if !matches_search_history_entry(namespace, options, &entry) {
        return;
    }
    if seen.insert(entry.timestamp_nanos) {
        entries.push(entry);
    }
}

fn matches_search_history_entry(
    namespace: &str,
    options: &SearchHistoryListOptions,
    entry: &SearchHistoryEntry,
) -> bool {
    if entry.namespace != namespace {
        return false;
    }
    if let Some(before) = options.before_nanos {
        if entry.timestamp_nanos >= before {
            return false;
        }
    }
    if let Some(from) = options.from_nanos {
        if entry.timestamp_nanos < from {
            return false;
        }
    }
    if let Some(to) = options.to_nanos {
        if entry.timestamp_nanos > to {
            return false;
        }
    }
    options
        .tags
        .iter()
        .all(|tag| entry.tags.iter().any(|entry_tag| entry_tag == tag))
}

fn matches_clickstream_event(
    namespace: &str,
    options: &ClickstreamListOptions,
    event: &ClickstreamEvent,
) -> bool {
    if event.namespace != namespace {
        return false;
    }
    if let Some(trace_id) = &options.trace_id {
        if event.trace_id != *trace_id {
            return false;
        }
    }
    if let Some(before) = options.before_nanos {
        if event.timestamp_nanos >= before {
            return false;
        }
    }
    if let Some(from) = options.from_nanos {
        if event.timestamp_nanos < from {
            return false;
        }
    }
    if let Some(to) = options.to_nanos {
        if event.timestamp_nanos > to {
            return false;
        }
    }
    options
        .tags
        .iter()
        .all(|tag| event.tags.iter().any(|entry_tag| entry_tag == tag))
}
