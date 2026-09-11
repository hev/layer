//! Restart-recoverable namespace cleanup. Intents precede upstream deletion;
//! recovery never deletes upstream and only purges after confirming absence.
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use futures::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Notify};
use tokio::time::Instant;
use tracing::{error, warn};

use crate::{
    error::AppError,
    routes::namespaces::{cleanup_namespace_state, purge_in_memory_namespace_state},
    AppState,
};

pub const INTENT_PREFIX: &str = "namespace-purges/v1/";
pub const DELETE_MESSAGE: &str = "namespace deleted; snapshot and cache purge continues in the background and may take a few minutes";
const INTENT_TIMEOUT: Duration = Duration::from_millis(500);
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Serialize, Deserialize)]
struct Intent {
    namespace: String,
    store: String,
}

#[derive(Clone)]
struct Pending {
    intent: Intent,
    upstream_deleted: bool,
    durable: bool,
    attempts: u32,
    next_attempt: Instant,
}

#[derive(Default)]
pub struct NamespacePurges {
    pending: DashMap<String, Pending>,
    cycle: Mutex<()>,
    wake: Notify,
    started: AtomicBool,
}

impl NamespacePurges {
    /// A failed or ambiguous intent PUT cannot authorize an upstream delete.
    /// A PUT that timed out but committed is harmless: recovery checks absence.
    pub async fn prepare(
        &self,
        state: &AppState,
        namespace: &str,
    ) -> Result<(String, String), AppError> {
        let key = format!("{INTENT_PREFIX}{}.json", uuid::Uuid::new_v4());
        let intent = Intent {
            namespace: namespace.into(),
            store: state.store_for_namespace(namespace),
        };
        let durable = state.s3.is_configured();
        if durable {
            let body = serde_json::to_vec(&intent).expect("intent contains only strings");
            let result = tokio::time::timeout(INTENT_TIMEOUT, state.s3.put(&key, body)).await;
            match result {
                Ok(Ok(())) => {}
                result => {
                    error!(namespace, error = ?result, "Purge intent persistence failed; upstream delete not attempted");
                    return Err(AppError::ServiceUnavailable(
                    "could not persist namespace purge intent; upstream namespace was not deleted; retry the delete".into(),
                ));
                }
            }
        }
        let store = intent.store.clone();
        self.pending.entry(key.clone()).or_insert(Pending {
            intent,
            upstream_deleted: false,
            durable,
            attempts: 0,
            next_attempt: Instant::now(),
        });
        self.refresh_metrics(state);
        Ok((key, store))
    }

    pub fn upstream_deleted(&self, state: &AppState, key: &str) {
        if let Some(mut pending) = self.pending.get_mut(key) {
            pending.upstream_deleted = true;
            pending.next_attempt = Instant::now();
        }
        self.refresh_metrics(state);
        self.wake.notify_one();
    }

    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    fn refresh_metrics(&self, state: &AppState) {
        let mut counts = HashMap::<String, i64>::new();
        for pending in self.pending.iter() {
            *counts.entry(pending.intent.namespace.clone()).or_default() += 1;
        }
        state.metrics.set_namespace_purges(&counts);
    }

    /// Public for deterministic lifecycle tests and embedding runtimes.
    pub async fn recover(&self, state: &AppState) -> Result<(), String> {
        let _cycle = self.cycle.lock().await;
        if !state.s3.is_configured() {
            state.metrics.set_namespace_purge_discovery_ready(true);
            self.refresh_metrics(state);
            return Ok(());
        }
        let result = tokio::time::timeout(ATTEMPT_TIMEOUT, async {
            let keys = state
                .s3
                .list_keys(INTENT_PREFIX)
                .await
                .map_err(|e| e.to_string())?;
            let mut errors = Vec::new();
            for key in keys {
                if self.pending.contains_key(&key) {
                    continue;
                }
                let body = match state.s3.get(&key).await {
                    Ok(Some(body)) => body,
                    Ok(None) => continue,
                    Err(error) => {
                        errors.push(format!("{key}: {error}"));
                        continue;
                    }
                };
                let intent: Intent = match serde_json::from_slice(&body) {
                    Ok(intent) => intent,
                    Err(error) => {
                        errors.push(format!("{key}: {error}"));
                        continue;
                    }
                };
                if intent.namespace.trim().is_empty() || intent.store.trim().is_empty() {
                    errors.push(format!("{key}: empty namespace or store"));
                    continue;
                }
                self.pending.entry(key).or_insert(Pending {
                    intent,
                    upstream_deleted: false,
                    durable: true,
                    attempts: 0,
                    next_attempt: Instant::now(),
                });
            }
            if errors.is_empty() {
                Ok(())
            } else {
                Err(errors.join("; "))
            }
        })
        .await
        .unwrap_or_else(|_| Err("purge intent discovery timed out".into()));
        state
            .metrics
            .set_namespace_purge_discovery_ready(result.is_ok());
        self.refresh_metrics(state);
        result
    }

    /// One bounded retry cycle. Failed intents stay in S3 and in the metric.
    pub async fn process_pending(&self, state: &AppState) {
        let _cycle = self.cycle.lock().await;
        let mut due: Vec<_> = self
            .pending
            .iter()
            .filter(|p| p.next_attempt <= Instant::now())
            .map(|p| (p.key().clone(), p.value().clone()))
            .collect();
        // Bound the entire cycle, not just concurrent I/O, so discovery and
        // newly queued work are not held behind a large stalled backlog.
        due.sort_by_key(|(_, pending)| pending.next_attempt);
        due.truncate(4);
        stream::iter(due)
            .for_each_concurrent(4, |(key, pending)| async move {
                let result =
                    tokio::time::timeout(ATTEMPT_TIMEOUT, self.attempt(state, &key, &pending))
                        .await
                        .unwrap_or_else(|_| Err("namespace purge attempt timed out".into()));
                match result {
                    Ok(()) => {
                        self.pending.remove(&key);
                    }
                    Err(error) => {
                        if let Some(mut current) = self.pending.get_mut(&key) {
                            current.attempts = current.attempts.saturating_add(1);
                            current.next_attempt = Instant::now() + retry_delay(current.attempts);
                        }
                        warn!(namespace = %pending.intent.namespace, intent = %key, %error,
                        "Namespace purge remains pending; retrying with backoff");
                    }
                }
            })
            .await;
        self.refresh_metrics(state);
    }

    async fn attempt(&self, state: &AppState, key: &str, pending: &Pending) -> Result<(), String> {
        // Another replica may have drained the intent, but this process can
        // still retain namespace memory from an interrupted foreground delete.
        let intent_missing = pending.durable
            && state
                .s3
                .get(key)
                .await
                .map_err(|e| e.to_string())?
                .is_none();
        if !pending.upstream_deleted || intent_missing {
            match state
                .turbopuffer()
                .head_namespace_in_store(&pending.intent.namespace, &pending.intent.store)
                .await
            {
                Err(error) if error.is_not_found() => {}
                Err(error) => return Err(format!("cannot verify upstream absence: {error}")),
                Ok(_) => {
                    return Err(
                        "upstream namespace still exists; retry the namespace delete".into(),
                    )
                }
            }
        }
        purge_in_memory_namespace_state(state, &pending.intent.namespace);
        if intent_missing {
            return Ok(());
        }
        let outcome = cleanup_namespace_state(state, &pending.intent.namespace).await;
        if !outcome.errors.is_empty() {
            return Err(outcome.errors.join("; "));
        }
        if pending.durable {
            state.s3.delete_key(key).await.map_err(|e| e.to_string())
        } else {
            Ok(())
        }
    }
}

fn retry_delay(attempts: u32) -> Duration {
    Duration::from_secs(1u64 << attempts.saturating_sub(1).min(6)).min(Duration::from_secs(60))
}

/// Start once per gateway. A Weak reference allows embedding runtimes to drop
/// their state; callers can also abort the returned task during shutdown/tests.
pub fn spawn_worker(state: &Arc<AppState>) -> Option<tokio::task::JoinHandle<()>> {
    if state.namespace_purges.started.swap(true, Ordering::SeqCst) {
        return None;
    }
    let weak = Arc::downgrade(state);
    let purges = state.namespace_purges.clone();
    Some(tokio::spawn(async move {
        let mut next_discovery = Instant::now();
        loop {
            let Some(state) = weak.upgrade() else {
                break;
            };
            if Instant::now() >= next_discovery {
                if let Err(error) = purges.recover(&state).await {
                    warn!(%error, "Namespace purge recovery failed; discovery will retry");
                }
                next_discovery = Instant::now() + Duration::from_secs(30);
            }
            purges.process_pending(&state).await;
            drop(state);
            tokio::select! {
                _ = purges.wake.notified() => {},
                _ = tokio::time::sleep(Duration::from_millis(250)) => {},
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retry_backoff_is_exponential_and_capped() {
        assert_eq!(
            (1..=8)
                .map(|n| retry_delay(n).as_secs())
                .collect::<Vec<_>>(),
            [1, 2, 4, 8, 16, 32, 60, 60]
        );
    }
}
