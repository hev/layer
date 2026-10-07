//! Prospective Function cost evidence. No payloads, raw document IDs, auth
//! headers, budget decisions, extra provider calls or Prometheus document labels.
use crate::udf::{UdfItemKey, UdfResource};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use vectorstore_core::turbopuffer::receipts;
const MAX_OWNERS: usize = 1024;
const CONTEXT_CHUNK: usize = 16;
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
pub(crate) struct DocumentOwner {
    function: String,
    version: String,
    namespace: String,
    document_token: String,
}
tokio::task_local! {
    static DOCUMENTS: BTreeSet<DocumentOwner>;
    static STAGE: &'static str;
}
pub(crate) fn utc_day() -> Option<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|d| d.as_secs() / 86400)
}
fn document_token(day: u64, namespace: &str, id: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(b"hevlayer-function-document-v1\0");
    hash.update(day.to_le_bytes());
    for value in [namespace, id] {
        hash.update((value.len() as u64).to_le_bytes());
        hash.update(value.as_bytes());
    }
    format!("{:x}", hash.finalize())
}
fn owner(day: u64, function: &str, version: &str, key: &UdfItemKey) -> Option<DocumentOwner> {
    if [function, version, &key.namespace]
        .iter()
        .any(|s| s.len() > 128)
    {
        return None;
    }
    Some(DocumentOwner {
        function: function.into(),
        version: version.into(),
        namespace: key.namespace.clone(),
        document_token: document_token(day, &key.namespace, &key.document_id),
    })
}
fn emit(mut event: serde_json::Value) {
    event["unix_seconds"] = serde_json::json!(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|t| t.as_secs_f64()));
    tracing::info!(target: "function_cost_receipt", receipt = %event, "Function cost receipt");
}
// Allocate at construction, before wrapping a potentially large route future.
// An async function that boxes only after its first poll still stores F inline.
pub(crate) fn stage<F: std::future::Future>(
    stage: &'static str,
    work: F,
) -> impl std::future::Future<Output = F::Output> {
    STAGE.scope(stage, Box::pin(work))
}
pub(crate) fn with_owners<'a, F: std::future::Future + 'a>(
    owners: &'a [(UdfResource, Vec<UdfItemKey>)],
    work: F,
) -> impl std::future::Future<Output = F::Output> + 'a {
    let work = Box::pin(work);
    async move {
        let Some(day) = utc_day() else {
            return work.await;
        };
        let mut documents = DOCUMENTS.try_with(Clone::clone).unwrap_or_default();
        let mut complete = true;
        for (udf, keys) in owners {
            for key in keys {
                match owner(day, &udf.id, &udf.spec.version, key) {
                    Some(document)
                        if documents.len() < MAX_OWNERS || documents.contains(&document) =>
                    {
                        documents.insert(document);
                    }
                    _ => complete = false,
                }
            }
        }
        let scope = uuid::Uuid::new_v4().to_string();
        let stage = STAGE.try_with(|s| *s).unwrap_or("other");
        let context: Vec<_> = documents.iter().collect();
        let chunk_count = context.len().div_ceil(CONTEXT_CHUNK).max(1);
        for index in 0..chunk_count {
            let begin = index * CONTEXT_CHUNK;
            let chunk = &context[begin..(begin + CONTEXT_CHUNK).min(context.len())];
            emit(
                serde_json::json!({"event":"scope","schema":1,"scope":scope,"utc_day":day,"stage":stage,"documents_complete":complete,"documents":chunk,"chunk_index":index,"chunk_count":chunk_count,"document_count":context.len()}),
            );
        }
        let counts = Arc::new((AtomicU64::new(0), AtomicU64::new(0)));
        let mut end = ScopeEnd {
            scope: scope.clone(),
            day,
            counts: counts.clone(),
            finished: false,
        };
        let observer: receipts::ReadReceiptObserver = Arc::new(move |receipt| {
            let (caller_kind, caller) = crate::metrics::billing_caller_receipt();
            if receipt.phase == "send_attempt" {
                counts.0.fetch_add(1, Ordering::Relaxed);
            } else {
                counts.1.fetch_add(1, Ordering::Relaxed);
            }
            emit(
                serde_json::json!({"event":"provider_read","schema":1,"scope":scope,"utc_day":day,"caller_kind":caller_kind,"caller":caller,"receipt":receipt}),
            );
        });
        let result = DOCUMENTS
            .scope(documents, receipts::scope(observer, work))
            .await;
        end.finished = true;
        result
    }
}
struct ScopeEnd {
    scope: String,
    day: u64,
    counts: Arc<(AtomicU64, AtomicU64)>,
    finished: bool,
}
impl Drop for ScopeEnd {
    fn drop(&mut self) {
        emit(
            serde_json::json!({"event":"scope_end","schema":1,"scope":self.scope,"utc_day":self.day,"finished":self.finished,"send_attempts":self.counts.0.load(Ordering::Relaxed),"outcomes":self.counts.1.load(Ordering::Relaxed)}),
        );
    }
}
/// Associate returned sibling row tokens with the claimed page-one token.
/// This uses rows already returned; it never fetches rows or logs their data.
pub(crate) fn pages(function: &str, namespace: &str, id: &str, rows: &[serde_json::Value]) {
    let Some(day) = utc_day() else {
        return;
    };
    let pages: Vec<_> = rows
        .iter()
        .filter_map(|row| row["id"].as_str())
        .map(|id| document_token(day, namespace, id))
        .collect();
    let chunk_count = pages.len().div_ceil(CONTEXT_CHUNK).max(1);
    for index in 0..chunk_count {
        let begin = index * CONTEXT_CHUNK;
        let chunk = &pages[begin..(begin + CONTEXT_CHUNK).min(pages.len())];
        emit(
            serde_json::json!({"event":"document_pages","schema":1,"utc_day":day,"function":function.chars().take(128).collect::<String>(),"namespace":namespace.chars().take(128).collect::<String>(),"document_token":document_token(day, namespace, id),"page_tokens":chunk,"chunk_index":index,"chunk_count":chunk_count,"page_count":pages.len()}),
        );
    }
}
/// Captured revisions come from the existing claim result, not another read.
pub(crate) fn claimed(function: &str, items: &[crate::models::UdfClaimedItem]) {
    let Some(day) = utc_day() else {
        return;
    };
    for item in items {
        let key = UdfItemKey {
            namespace: item.namespace.clone(),
            document_id: item.id.clone(),
        };
        if let Some(document) = owner(day, function, "", &key) {
            emit(
                serde_json::json!({"event":"claim_revision","schema":1,"utc_day":day,"document":document,"input_revision":item.input_revision}),
            );
        }
    }
}
/// Original submitted keys, BEFORE page expansion: only an acknowledged whole
/// item supplies the denominator. Retries deduplicate by token across replicas.
pub(crate) fn completion(
    function: &str,
    keys: &[UdfItemKey],
    revisions: &[Option<u64>],
    expanded_count: usize,
    response: &serde_json::Value,
) {
    let Some(day) = utc_day() else {
        return;
    };
    let legacy_complete =
        response.get("updated").and_then(serde_json::Value::as_u64) == Some(expanded_count as u64);
    let outcomes = response.get("items").and_then(serde_json::Value::as_array);
    for (index, key) in keys.iter().enumerate() {
        let accepted = outcomes.map_or(legacy_complete, |rows| {
            rows.iter().any(|r| {
                r["index"].as_u64() == Some(index as u64) && r["disposition"] == "completed"
            })
        });
        if accepted {
            if let Some(document) = owner(day, function, "", key) {
                emit(
                    serde_json::json!({"event":"completion_ack","schema":1,"utc_day":day,"document":document,"input_revision":revisions.get(index).copied().flatten()}),
                );
            }
        }
    }
    if outcomes.is_none() && !legacy_complete {
        emit(
            serde_json::json!({"event":"completion_unmatched","schema":1,"utc_day":day,"function":function.chars().take(128).collect::<String>(),"expected_expanded":expanded_count,"updated":response.get("updated").and_then(serde_json::Value::as_u64)}),
        );
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn receipt_wrappers_keep_large_route_futures_off_the_stack() {
        let payload = [7_u8; 128 * 1024];
        let work = async move {
            tokio::task::yield_now().await;
            assert_eq!(std::hint::black_box(payload)[0], 7);
        };
        assert!(std::mem::size_of_val(&work) >= 128 * 1024);
        let owners = [];
        let wrapped = with_owners(&owners, work);
        assert!(std::mem::size_of_val(&wrapped) < 4096);
        let staged = stage("lookup", wrapped);
        assert!(std::mem::size_of_val(&staged) < 4096);
        staged.await;
    }
    #[test]
    fn token_is_replica_stable_daily_and_length_delimited_without_raw_ids() {
        let token = document_token(1, "ab", "private-id");
        assert_eq!(token, document_token(1, "ab", "private-id"));
        assert_ne!(token, document_token(2, "ab", "private-id"));
        assert_ne!(document_token(1, "ab", "c"), document_token(1, "a", "bc"));
        assert_eq!(token.len(), 64);
        assert!(!token.contains("private-id"));
    }
}
