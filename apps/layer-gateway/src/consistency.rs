use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use tokio::time::sleep;
use tracing::{debug, warn};

use crate::clients::turbopuffer::{IndexStatus, TurbopufferClient};

/// Fired by the watcher each time a namespace's watermark advances (i.e. the
/// upstream index was just observed `Stable`). The implementation in
/// `crate::snapshots` uses this to spawn a snapshot job; tests pass `None`.
///
/// Must be cheap and non-blocking — typically just spawns a task. The watcher
/// continues polling regardless of what the trigger does.
pub trait SnapshotTrigger: Send + Sync {
    fn on_stable(&self, namespace: &str, watermark_ms: u64);
}

/// Per-namespace consistency state.
///
/// Two distinct facts are tracked:
///   * `watermarks` — an "as-of" epoch-ms timestamp, the most recent moment
///     we positively confirmed the upstream index was caught up. Used as the
///     `_hevlayer_upserted_at <= watermark` cut when the index is currently updating.
///   * `current_status` — the *latest-observed* `IndexStatus`. Used to
///     decide, at query time, whether to inject the watermark filter at all.
///     Skipping the filter when the namespace is currently stable avoids
///     overhead; we still retry with the filter on a 429 race.
pub struct ConsistencyWatcher {
    watermarks: DashMap<String, u64>,
    current_status: DashMap<String, IndexStatus>,
    last_settle_counts: DashMap<String, u64>,
    registered: DashMap<String, ()>,
    last_polled: DashMap<String, Instant>,
}

impl Default for ConsistencyWatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl ConsistencyWatcher {
    pub fn new() -> Self {
        Self {
            watermarks: DashMap::new(),
            current_status: DashMap::new(),
            last_settle_counts: DashMap::new(),
            registered: DashMap::new(),
            last_polled: DashMap::new(),
        }
    }

    pub fn register(&self, namespace: &str) {
        self.registered.entry(namespace.to_string()).or_insert(());
    }

    pub fn is_registered(&self, namespace: &str) -> bool {
        self.registered.contains_key(namespace)
    }

    pub fn mark_due(&self, namespace: &str) {
        self.last_polled.remove(namespace);
    }

    pub fn register_due(&self, namespace: &str) {
        self.register(namespace);
        self.mark_due(namespace);
    }

    /// Arm consistency polling after a successful metadata read. Metadata
    /// paths can discover namespaces that never flowed through this gateway's
    /// write path; registering them keeps snapshots and stable watermarks from
    /// depending on a local upsert having happened first.
    pub fn register_from_metadata(&self, namespace: &str, status: IndexStatus) {
        if !matches!(status, IndexStatus::Unknown) {
            self.register_due(namespace);
        }
    }

    pub fn get(&self, namespace: &str) -> Option<u64> {
        self.watermarks.get(namespace).map(|r| *r.value())
    }

    /// Latest-observed `IndexStatus`. `Unknown` means we have not yet
    /// completed a successful poll for this namespace.
    pub fn current_status(&self, namespace: &str) -> IndexStatus {
        self.current_status
            .get(namespace)
            .map(|r| *r.value())
            .unwrap_or(IndexStatus::Unknown)
    }

    /// Whether the query path should inject the watermark filter on the
    /// first try. Only `Updating` requires it; both `Stable` and `Unknown`
    /// skip the filter and rely on the 429-retry fallback.
    pub fn should_inject_filter(&self, namespace: &str) -> bool {
        matches!(self.current_status(namespace), IndexStatus::Updating)
    }

    pub fn registered_namespaces(&self) -> Vec<String> {
        self.registered.iter().map(|e| e.key().clone()).collect()
    }

    pub fn forget_namespace(&self, namespace: &str) {
        self.watermarks.remove(namespace);
        self.current_status.remove(namespace);
        self.last_settle_counts.remove(namespace);
        self.registered.remove(namespace);
        self.last_polled.remove(namespace);
    }

    /// Poll every registered namespace once, ignoring interval due checks.
    /// Updates `current_status` from the upstream response; advances the
    /// watermark only when the response shows positive "stable" evidence
    /// (`meta.is_stable()`).
    pub async fn poll_once(&self, tpuf: &dyn TurbopufferClient, safety_margin: Duration) {
        self.poll_once_with_trigger(tpuf, safety_margin, None).await
    }

    /// Same as [`poll_once`] but fires `trigger.on_stable` for every namespace
    /// whose watermark advanced this tick. Used by `run` to drive snapshot
    /// writes from the consistency loop without coupling the watcher to
    /// `AppState`.
    pub async fn poll_once_with_trigger(
        &self,
        tpuf: &dyn TurbopufferClient,
        safety_margin: Duration,
        trigger: Option<&dyn SnapshotTrigger>,
    ) {
        for namespace in self.registered_namespaces() {
            self.poll_namespace(tpuf, &namespace, safety_margin, trigger)
                .await;
        }
    }

    /// Poll only registered namespaces whose status-specific interval has
    /// elapsed. `Updating` and `Unknown` use the fast interval; `Stable` uses
    /// the slower stable interval. This is the path used by the background
    /// watcher loop.
    pub async fn poll_due_once_with_trigger(
        &self,
        tpuf: &dyn TurbopufferClient,
        safety_margin: Duration,
        fast_interval: Duration,
        stable_interval: Duration,
        trigger: Option<&dyn SnapshotTrigger>,
    ) {
        let tick_started = Instant::now();
        for namespace in self.registered_namespaces() {
            if !self.is_poll_due(&namespace, tick_started, fast_interval, stable_interval) {
                continue;
            }

            self.last_polled.insert(namespace.clone(), tick_started);
            self.poll_namespace(tpuf, &namespace, safety_margin, trigger)
                .await;
        }
    }

    fn is_poll_due(
        &self,
        namespace: &str,
        now: Instant,
        fast_interval: Duration,
        stable_interval: Duration,
    ) -> bool {
        let interval = match self.current_status(namespace) {
            IndexStatus::Stable => stable_interval,
            IndexStatus::Updating | IndexStatus::Unknown => fast_interval,
        };

        self.last_polled
            .get(namespace)
            .map(|last| now.saturating_duration_since(*last.value()) >= interval)
            .unwrap_or(true)
    }

    async fn poll_namespace(
        &self,
        tpuf: &dyn TurbopufferClient,
        namespace: &str,
        safety_margin: Duration,
        trigger: Option<&dyn SnapshotTrigger>,
    ) {
        let poll_start_ms = now_ms();
        match tpuf.head_namespace(namespace).await {
            Ok(meta) => {
                if meta.is_stable() {
                    if let Some(count) = meta.count_settle {
                        let settled = self
                            .last_settle_counts
                            .get(namespace)
                            .is_some_and(|previous| *previous.value() == count);
                        self.last_settle_counts.insert(namespace.to_string(), count);
                        if !settled {
                            self.current_status
                                .insert(namespace.to_string(), IndexStatus::Updating);
                            debug!(
                                namespace = %namespace,
                                count,
                                "namespace count has not settled; holding watermark"
                            );
                            return;
                        }
                    } else {
                        self.last_settle_counts.remove(namespace);
                    }
                    self.current_status
                        .insert(namespace.to_string(), meta.index_status);
                    let safety_ms = safety_margin.as_millis() as u64;
                    let new_watermark = poll_start_ms.saturating_sub(safety_ms);
                    self.watermarks.insert(namespace.to_string(), new_watermark);
                    debug!(
                        namespace = %namespace,
                        watermark_ms = new_watermark,
                        "consistency watermark advanced"
                    );
                    if let Some(t) = trigger {
                        t.on_stable(namespace, new_watermark);
                    }
                } else {
                    self.current_status
                        .insert(namespace.to_string(), meta.index_status);
                    self.last_settle_counts.remove(namespace);
                    debug!(
                        namespace = %namespace,
                        status = ?meta.index_status,
                        unindexed_bytes = ?meta.unindexed_bytes,
                        "namespace not yet consistent; holding watermark"
                    );
                }
            }
            Err(e) => {
                warn!(
                    namespace = %namespace,
                    error = %e,
                    "consistency poll failed"
                );
            }
        }
    }

    /// Background loop. Fast-ticks the registered set and only issues an
    /// upstream metadata call when a namespace's status-specific interval has
    /// elapsed. Pass `trigger: None` to disable snapshot firing.
    pub async fn run(
        self: Arc<Self>,
        tpuf: Arc<dyn TurbopufferClient>,
        poll_interval: Duration,
        stable_poll_interval: Duration,
        safety_margin: Duration,
        trigger: Option<Arc<dyn SnapshotTrigger>>,
    ) {
        loop {
            self.poll_due_once_with_trigger(
                tpuf.as_ref(),
                safety_margin,
                poll_interval,
                stable_poll_interval,
                trigger.as_deref(),
            )
            .await;
            sleep(poll_interval).await;
        }
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::turbopuffer::MockTurbopufferClient;

    #[tokio::test]
    async fn watermark_advances_when_index_status_is_up_to_date() {
        let tpuf = MockTurbopufferClient::new();
        let watcher = ConsistencyWatcher::new();
        watcher.register("ns-1");

        tpuf.set_stable("ns-1").await;
        watcher.poll_once(&tpuf, Duration::from_millis(5)).await;

        let watermark = watcher.get("ns-1").expect("watermark should be set");
        assert!(watermark > 0);
        assert!(watermark <= now_ms());
        assert_eq!(watcher.current_status("ns-1"), IndexStatus::Stable);
        assert!(!watcher.should_inject_filter("ns-1"));
    }

    #[tokio::test]
    async fn watermark_holds_when_index_is_updating() {
        let tpuf = MockTurbopufferClient::new();
        let watcher = ConsistencyWatcher::new();
        watcher.register("ns-2");

        tpuf.set_stable("ns-2").await;
        watcher.poll_once(&tpuf, Duration::from_millis(0)).await;
        let first = watcher.get("ns-2").expect("first watermark");

        // Simulate streaming writes: namespace flips to `updating`.
        tpuf.set_updating("ns-2", 1024).await;
        watcher.poll_once(&tpuf, Duration::from_millis(0)).await;

        let held = watcher.get("ns-2").expect("watermark still present");
        assert_eq!(
            first, held,
            "watermark should not advance while status is updating"
        );
        assert_eq!(watcher.current_status("ns-2"), IndexStatus::Updating);
        assert!(watcher.should_inject_filter("ns-2"));
    }

    #[tokio::test]
    async fn unknown_status_does_not_advance_watermark() {
        // Regression for the silent `unwrap_or(0)` bug: a namespace whose
        // metadata response carries no status and no unindexed_bytes signal
        // must NOT be treated as caught-up.
        let tpuf = MockTurbopufferClient::new();
        let watcher = ConsistencyWatcher::new();
        watcher.register("ns-cold");

        // No `set_stable` / `set_updating` call — mock returns Unknown.
        watcher.poll_once(&tpuf, Duration::from_millis(0)).await;

        assert!(
            watcher.get("ns-cold").is_none(),
            "Unknown status must not advance the watermark"
        );
        assert_eq!(watcher.current_status("ns-cold"), IndexStatus::Unknown);
        // Per the cold-start contract, queries skip the filter under Unknown
        // and rely on the 429-retry fallback.
        assert!(!watcher.should_inject_filter("ns-cold"));
    }

    #[tokio::test]
    async fn register_is_idempotent() {
        let watcher = ConsistencyWatcher::new();
        watcher.register("ns-3");
        watcher.register("ns-3");
        watcher.register("ns-3");
        assert_eq!(watcher.registered_namespaces(), vec!["ns-3".to_string()]);
    }

    #[tokio::test]
    async fn stable_namespace_waits_for_slow_interval() {
        let tpuf = MockTurbopufferClient::new();
        let watcher = ConsistencyWatcher::new();
        watcher.register("ns-stable");

        tpuf.set_stable("ns-stable").await;
        watcher
            .poll_due_once_with_trigger(
                &tpuf,
                Duration::from_millis(0),
                Duration::from_millis(0),
                Duration::from_secs(60),
                None,
            )
            .await;
        assert_eq!(watcher.current_status("ns-stable"), IndexStatus::Stable);

        tpuf.set_updating("ns-stable", 512).await;
        watcher
            .poll_due_once_with_trigger(
                &tpuf,
                Duration::from_millis(0),
                Duration::from_millis(0),
                Duration::from_secs(60),
                None,
            )
            .await;

        assert_eq!(
            watcher.current_status("ns-stable"),
            IndexStatus::Stable,
            "stable namespaces should skip metadata calls until the slow interval elapses"
        );
    }

    #[tokio::test]
    async fn updating_namespace_polls_on_fast_tick() {
        let tpuf = MockTurbopufferClient::new();
        let watcher = ConsistencyWatcher::new();
        watcher.register("ns-updating");

        tpuf.set_updating("ns-updating", 1024).await;
        watcher
            .poll_due_once_with_trigger(
                &tpuf,
                Duration::from_millis(0),
                Duration::from_millis(0),
                Duration::from_secs(60),
                None,
            )
            .await;
        assert_eq!(watcher.current_status("ns-updating"), IndexStatus::Updating);

        tpuf.set_stable("ns-updating").await;
        watcher
            .poll_due_once_with_trigger(
                &tpuf,
                Duration::from_millis(0),
                Duration::from_millis(0),
                Duration::from_secs(60),
                None,
            )
            .await;

        assert_eq!(watcher.current_status("ns-updating"), IndexStatus::Stable);
    }

    #[tokio::test]
    async fn mark_due_rearms_stable_namespace() {
        let tpuf = MockTurbopufferClient::new();
        let watcher = ConsistencyWatcher::new();
        watcher.register("ns-rearm");

        tpuf.set_stable("ns-rearm").await;
        watcher
            .poll_due_once_with_trigger(
                &tpuf,
                Duration::from_millis(0),
                Duration::from_millis(0),
                Duration::from_secs(60),
                None,
            )
            .await;
        assert_eq!(watcher.current_status("ns-rearm"), IndexStatus::Stable);

        tpuf.set_updating("ns-rearm", 2048).await;
        watcher.register_due("ns-rearm");
        watcher
            .poll_due_once_with_trigger(
                &tpuf,
                Duration::from_millis(0),
                Duration::from_millis(0),
                Duration::from_secs(60),
                None,
            )
            .await;

        assert_eq!(watcher.current_status("ns-rearm"), IndexStatus::Updating);
    }
}
