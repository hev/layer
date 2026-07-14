use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::RwLock;

/// Cache calls are best-effort and must not occupy request tasks long enough
/// to starve the PostgreSQL-backed pipeline paths during an Aerospike outage.
pub const CACHE_OPERATION_TIMEOUT: Duration = Duration::from_millis(200);
/// One timeout/connect failure is enough to treat Aerospike as unavailable.
pub const CACHE_BREAKER_FAILURE_THRESHOLD: u64 = 1;
/// Open-circuit window before a single half-open cache probe is allowed.
pub const CACHE_BREAKER_OPEN_FOR: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AerospikeErrorKind {
    Other,
    StopWrites,
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("Aerospike error: {message}")]
pub struct AerospikeError {
    message: String,
    kind: AerospikeErrorKind,
}

impl AerospikeError {
    pub fn other(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: AerospikeErrorKind::Other,
        }
    }

    pub fn stop_writes(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: AerospikeErrorKind::StopWrites,
        }
    }

    pub fn kind(&self) -> AerospikeErrorKind {
        self.kind
    }

    pub fn is_stop_writes(&self) -> bool {
        self.kind == AerospikeErrorKind::StopWrites
    }
}

#[async_trait]
pub trait AerospikeClient: Send + Sync {
    async fn put(
        &self,
        namespace: &str,
        id: &str,
        doc: &HashMap<String, Value>,
    ) -> Result<(), AerospikeError>;

    async fn put_many(
        &self,
        namespace: &str,
        docs: &HashMap<String, HashMap<String, Value>>,
    ) -> Result<(), AerospikeError>;

    async fn get(
        &self,
        namespace: &str,
        id: &str,
        include_attributes: Option<&[String]>,
    ) -> Result<Option<HashMap<String, Value>>, AerospikeError>;

    async fn get_many(
        &self,
        namespace: &str,
        ids: &[String],
        include_attributes: Option<&[String]>,
    ) -> Result<HashMap<String, HashMap<String, Value>>, AerospikeError>;

    /// Store the document's embedding vector in a dedicated `vec` bin.
    /// Vectors are kept out of the normal attribute path: they are written
    /// here, not in the `attrs` bin, and `get`/`get_many` never return them.
    /// Only the search-by-id resolver (`get_vector`) reads them back.
    async fn put_vector(
        &self,
        namespace: &str,
        id: &str,
        vector: &[f64],
    ) -> Result<(), AerospikeError>;

    /// Resolve a document's embedding vector from cache.
    /// Returns `None` if the doc is absent or the `vec` bin is missing.
    async fn get_vector(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<Vec<f64>>, AerospikeError>;

    async fn delete(&self, namespace: &str, id: &str) -> Result<(), AerospikeError>;

    async fn scan(
        &self,
        namespace: &str,
        include_attributes: Option<&[String]>,
    ) -> Result<Vec<(String, HashMap<String, Value>)>, AerospikeError>;

    async fn put_raw(&self, namespace: &str, key: &str, data: &[u8]) -> Result<(), AerospikeError>;

    async fn get_raw(&self, namespace: &str, key: &str) -> Result<Option<Vec<u8>>, AerospikeError>;

    async fn delete_set(&self, namespace: &str) -> Result<(), AerospikeError>;

    async fn count_set(&self, namespace: &str) -> Result<u64, AerospikeError>;
}

#[derive(Debug, Clone)]
pub struct AerospikeRuntimeStatus {
    pub connected: bool,
    pub generation: u64,
    pub last_error: Option<String>,
}

pub struct AerospikeRuntime {
    client: RwLock<Option<Arc<dyn AerospikeClient>>>,
    last_error: RwLock<Option<String>>,
    breaker_opened_at: RwLock<Option<Instant>>,
    connected: AtomicBool,
    generation: AtomicU64,
    consecutive_failures: AtomicU64,
    half_open_probe_inflight: AtomicBool,
}

impl AerospikeRuntime {
    pub fn new(client: Option<Arc<dyn AerospikeClient>>) -> Self {
        let generation = if client.is_some() { 1 } else { 0 };
        Self {
            client: RwLock::new(client),
            last_error: RwLock::new(None),
            breaker_opened_at: RwLock::new(None),
            connected: AtomicBool::new(generation > 0),
            generation: AtomicU64::new(generation),
            consecutive_failures: AtomicU64::new(0),
            half_open_probe_inflight: AtomicBool::new(false),
        }
    }

    pub async fn set_connected(&self, client: Arc<dyn AerospikeClient>) -> bool {
        let mut guard = self.client.write().await;
        *guard = Some(client);
        drop(guard);

        *self.last_error.write().await = None;
        *self.breaker_opened_at.write().await = None;
        self.consecutive_failures.store(0, Ordering::SeqCst);
        self.half_open_probe_inflight.store(false, Ordering::SeqCst);
        let was_disconnected = !self.connected.swap(true, Ordering::SeqCst);
        if was_disconnected {
            self.generation.fetch_add(1, Ordering::SeqCst);
        }
        was_disconnected
    }

    pub async fn mark_disconnected(&self, error: impl Into<String>) {
        *self.client.write().await = None;
        *self.last_error.write().await = Some(error.into());
        *self.breaker_opened_at.write().await = Some(Instant::now());
        self.consecutive_failures
            .store(CACHE_BREAKER_FAILURE_THRESHOLD, Ordering::SeqCst);
        self.half_open_probe_inflight.store(false, Ordering::SeqCst);
        self.connected.store(false, Ordering::SeqCst);
    }

    pub async fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    pub fn is_connected_now(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    pub async fn status(&self) -> AerospikeRuntimeStatus {
        AerospikeRuntimeStatus {
            connected: self.is_connected().await,
            generation: self.generation(),
            last_error: self.last_error.read().await.clone(),
        }
    }

    async fn current(&self) -> Result<Arc<dyn AerospikeClient>, AerospikeError> {
        let client = self
            .client
            .read()
            .await
            .as_ref()
            .cloned()
            .ok_or_else(|| AerospikeError::other("cache cold: Aerospike unavailable"))?;

        if let Some(opened_at) = *self.breaker_opened_at.read().await {
            if opened_at.elapsed() < CACHE_BREAKER_OPEN_FOR {
                return Err(AerospikeError::other(
                    "cache cold: Aerospike circuit breaker open",
                ));
            }
            if self
                .half_open_probe_inflight
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                return Err(AerospikeError::other(
                    "cache cold: Aerospike circuit breaker probe in flight",
                ));
            }
        }

        Ok(client)
    }

    async fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::SeqCst);
        self.half_open_probe_inflight.store(false, Ordering::SeqCst);
        *self.breaker_opened_at.write().await = None;
        *self.last_error.write().await = None;
        self.connected.store(true, Ordering::SeqCst);
    }

    async fn record_failure(&self, error: String) {
        *self.last_error.write().await = Some(error);
        self.half_open_probe_inflight.store(false, Ordering::SeqCst);
        let failures = self.consecutive_failures.fetch_add(1, Ordering::SeqCst) + 1;
        if failures >= CACHE_BREAKER_FAILURE_THRESHOLD {
            *self.breaker_opened_at.write().await = Some(Instant::now());
            self.connected.store(false, Ordering::SeqCst);
        }
    }

    async fn call<T, Fut>(&self, fut: Fut) -> Result<T, AerospikeError>
    where
        Fut: std::future::Future<Output = Result<T, AerospikeError>>,
    {
        let result = match tokio::time::timeout(CACHE_OPERATION_TIMEOUT, fut).await {
            Ok(result) => result,
            Err(_) => Err(AerospikeError::other(format!(
                "Aerospike operation timed out after {}ms",
                CACHE_OPERATION_TIMEOUT.as_millis()
            ))),
        };

        match result {
            Ok(value) => {
                self.record_success().await;
                Ok(value)
            }
            Err(error) => {
                self.record_failure(error.to_string()).await;
                Err(error)
            }
        }
    }
}

#[async_trait]
impl AerospikeClient for AerospikeRuntime {
    async fn put(
        &self,
        namespace: &str,
        id: &str,
        doc: &HashMap<String, Value>,
    ) -> Result<(), AerospikeError> {
        let client = self.current().await?;
        self.call(client.put(namespace, id, doc)).await
    }

    async fn put_many(
        &self,
        namespace: &str,
        docs: &HashMap<String, HashMap<String, Value>>,
    ) -> Result<(), AerospikeError> {
        let client = self.current().await?;
        self.call(client.put_many(namespace, docs)).await
    }

    async fn get(
        &self,
        namespace: &str,
        id: &str,
        include_attributes: Option<&[String]>,
    ) -> Result<Option<HashMap<String, Value>>, AerospikeError> {
        let client = self.current().await?;
        self.call(client.get(namespace, id, include_attributes))
            .await
    }

    async fn get_many(
        &self,
        namespace: &str,
        ids: &[String],
        include_attributes: Option<&[String]>,
    ) -> Result<HashMap<String, HashMap<String, Value>>, AerospikeError> {
        let client = self.current().await?;
        self.call(client.get_many(namespace, ids, include_attributes))
            .await
    }

    async fn put_vector(
        &self,
        namespace: &str,
        id: &str,
        vector: &[f64],
    ) -> Result<(), AerospikeError> {
        let client = self.current().await?;
        self.call(client.put_vector(namespace, id, vector)).await
    }

    async fn get_vector(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<Vec<f64>>, AerospikeError> {
        let client = self.current().await?;
        self.call(client.get_vector(namespace, id)).await
    }

    async fn delete(&self, namespace: &str, id: &str) -> Result<(), AerospikeError> {
        let client = self.current().await?;
        self.call(client.delete(namespace, id)).await
    }

    async fn scan(
        &self,
        namespace: &str,
        include_attributes: Option<&[String]>,
    ) -> Result<Vec<(String, HashMap<String, Value>)>, AerospikeError> {
        let client = self.current().await?;
        self.call(client.scan(namespace, include_attributes)).await
    }

    async fn put_raw(&self, namespace: &str, key: &str, data: &[u8]) -> Result<(), AerospikeError> {
        let client = self.current().await?;
        self.call(client.put_raw(namespace, key, data)).await
    }

    async fn get_raw(&self, namespace: &str, key: &str) -> Result<Option<Vec<u8>>, AerospikeError> {
        let client = self.current().await?;
        self.call(client.get_raw(namespace, key)).await
    }

    async fn delete_set(&self, namespace: &str) -> Result<(), AerospikeError> {
        let client = self.current().await?;
        self.call(client.delete_set(namespace)).await
    }

    async fn count_set(&self, namespace: &str) -> Result<u64, AerospikeError> {
        let client = self.current().await?;
        self.call(client.count_set(namespace)).await
    }
}

// The concrete Aerospike-backed implementation is pro-only and is not
// included in the public mirror. The open gateway keeps the trait,
// disconnected runtime, and mock implementation so cache-dependent open
// routes degrade cleanly without shipping the private cache client.

fn base64_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{:02x}", b);
    }
    s
}

// --- Mock implementation for testing ---

type NamespaceStore = HashMap<String, HashMap<String, HashMap<String, Value>>>;
type VectorNamespaceStore = HashMap<String, HashMap<String, Vec<f64>>>;

pub struct MockAerospikeClient {
    store: tokio::sync::RwLock<NamespaceStore>,
    vectors: tokio::sync::RwLock<VectorNamespaceStore>,
}

impl Default for MockAerospikeClient {
    fn default() -> Self {
        Self::new()
    }
}

impl MockAerospikeClient {
    pub fn new() -> Self {
        Self {
            store: tokio::sync::RwLock::new(HashMap::new()),
            vectors: tokio::sync::RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl AerospikeClient for MockAerospikeClient {
    async fn put(
        &self,
        namespace: &str,
        id: &str,
        doc: &HashMap<String, Value>,
    ) -> Result<(), AerospikeError> {
        let mut store = self.store.write().await;
        store
            .entry(namespace.to_string())
            .or_default()
            .insert(id.to_string(), doc.clone());
        Ok(())
    }

    async fn put_raw(&self, namespace: &str, key: &str, data: &[u8]) -> Result<(), AerospikeError> {
        let mut store = self.store.write().await;
        let mut doc = HashMap::new();
        doc.insert("data".to_string(), Value::String(base64_encode(data)));
        store
            .entry(namespace.to_string())
            .or_default()
            .insert(key.to_string(), doc);
        Ok(())
    }

    async fn get_raw(&self, namespace: &str, key: &str) -> Result<Option<Vec<u8>>, AerospikeError> {
        let store = self.store.read().await;
        Ok(store
            .get(namespace)
            .and_then(|ns| ns.get(key))
            .and_then(|doc| doc.get("data"))
            .and_then(|v| v.as_str())
            .map(|hex| {
                (0..hex.len())
                    .step_by(2)
                    .filter_map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
                    .collect()
            }))
    }

    async fn delete_set(&self, namespace: &str) -> Result<(), AerospikeError> {
        let mut store = self.store.write().await;
        store.remove(namespace);
        self.vectors.write().await.remove(namespace);
        Ok(())
    }

    async fn count_set(&self, namespace: &str) -> Result<u64, AerospikeError> {
        let store = self.store.read().await;
        Ok(store.get(namespace).map_or(0, |ns| ns.len() as u64))
    }

    async fn put_many(
        &self,
        namespace: &str,
        docs: &HashMap<String, HashMap<String, Value>>,
    ) -> Result<(), AerospikeError> {
        let mut store = self.store.write().await;
        let ns = store.entry(namespace.to_string()).or_default();
        for (id, doc) in docs {
            ns.insert(id.clone(), doc.clone());
        }
        Ok(())
    }

    async fn get(
        &self,
        namespace: &str,
        id: &str,
        include_attributes: Option<&[String]>,
    ) -> Result<Option<HashMap<String, Value>>, AerospikeError> {
        let store = self.store.read().await;
        let doc = store.get(namespace).and_then(|ns| ns.get(id)).cloned();
        Ok(doc.map(|d| filter_attributes(d, include_attributes)))
    }

    async fn get_many(
        &self,
        namespace: &str,
        ids: &[String],
        include_attributes: Option<&[String]>,
    ) -> Result<HashMap<String, HashMap<String, Value>>, AerospikeError> {
        let store = self.store.read().await;
        let mut result = HashMap::new();
        if let Some(ns) = store.get(namespace) {
            for id in ids {
                if let Some(doc) = ns.get(id) {
                    result.insert(
                        id.clone(),
                        filter_attributes(doc.clone(), include_attributes),
                    );
                }
            }
        }
        Ok(result)
    }

    async fn put_vector(
        &self,
        namespace: &str,
        id: &str,
        vector: &[f64],
    ) -> Result<(), AerospikeError> {
        let mut vectors = self.vectors.write().await;
        vectors
            .entry(namespace.to_string())
            .or_default()
            .insert(id.to_string(), vector.to_vec());
        Ok(())
    }

    async fn get_vector(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<Vec<f64>>, AerospikeError> {
        let vectors = self.vectors.read().await;
        Ok(vectors.get(namespace).and_then(|ns| ns.get(id)).cloned())
    }

    async fn delete(&self, namespace: &str, id: &str) -> Result<(), AerospikeError> {
        let mut store = self.store.write().await;
        if let Some(ns) = store.get_mut(namespace) {
            ns.remove(id);
        }
        if let Some(ns) = self.vectors.write().await.get_mut(namespace) {
            ns.remove(id);
        }
        Ok(())
    }

    async fn scan(
        &self,
        namespace: &str,
        include_attributes: Option<&[String]>,
    ) -> Result<Vec<(String, HashMap<String, Value>)>, AerospikeError> {
        let store = self.store.read().await;
        let mut results = Vec::new();
        if let Some(ns) = store.get(namespace) {
            for (id, doc) in ns {
                results.push((
                    id.clone(),
                    filter_attributes(doc.clone(), include_attributes),
                ));
            }
        }
        Ok(results)
    }
}

fn filter_attributes(
    doc: HashMap<String, Value>,
    include: Option<&[String]>,
) -> HashMap<String, Value> {
    match include {
        Some(attrs) => doc
            .into_iter()
            .filter(|(k, _)| attrs.iter().any(|a| a == k))
            .collect(),
        None => doc,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct SleepingAerospikeClient {
        calls: AtomicU64,
        sleep_for: Duration,
        fail: AtomicBool,
    }

    impl SleepingAerospikeClient {
        fn new(sleep_for: Duration) -> Self {
            Self {
                calls: AtomicU64::new(0),
                sleep_for,
                fail: AtomicBool::new(true),
            }
        }
    }

    #[async_trait]
    impl AerospikeClient for SleepingAerospikeClient {
        async fn put(
            &self,
            _namespace: &str,
            _id: &str,
            _doc: &HashMap<String, Value>,
        ) -> Result<(), AerospikeError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.sleep_for).await;
            if self.fail.load(Ordering::SeqCst) {
                Err(AerospikeError::other("simulated cache outage"))
            } else {
                Ok(())
            }
        }

        async fn put_many(
            &self,
            namespace: &str,
            docs: &HashMap<String, HashMap<String, Value>>,
        ) -> Result<(), AerospikeError> {
            for (id, doc) in docs {
                self.put(namespace, id, doc).await?;
            }
            Ok(())
        }

        async fn get(
            &self,
            _namespace: &str,
            _id: &str,
            _include_attributes: Option<&[String]>,
        ) -> Result<Option<HashMap<String, Value>>, AerospikeError> {
            unimplemented!()
        }

        async fn get_many(
            &self,
            _namespace: &str,
            _ids: &[String],
            _include_attributes: Option<&[String]>,
        ) -> Result<HashMap<String, HashMap<String, Value>>, AerospikeError> {
            unimplemented!()
        }

        async fn put_vector(
            &self,
            _namespace: &str,
            _id: &str,
            _vector: &[f64],
        ) -> Result<(), AerospikeError> {
            unimplemented!()
        }

        async fn get_vector(
            &self,
            _namespace: &str,
            _id: &str,
        ) -> Result<Option<Vec<f64>>, AerospikeError> {
            unimplemented!()
        }

        async fn delete(&self, _namespace: &str, _id: &str) -> Result<(), AerospikeError> {
            unimplemented!()
        }

        async fn scan(
            &self,
            _namespace: &str,
            _include_attributes: Option<&[String]>,
        ) -> Result<Vec<(String, HashMap<String, Value>)>, AerospikeError> {
            unimplemented!()
        }

        async fn put_raw(
            &self,
            _namespace: &str,
            _key: &str,
            _data: &[u8],
        ) -> Result<(), AerospikeError> {
            unimplemented!()
        }

        async fn get_raw(
            &self,
            _namespace: &str,
            _key: &str,
        ) -> Result<Option<Vec<u8>>, AerospikeError> {
            unimplemented!()
        }

        async fn delete_set(&self, _namespace: &str) -> Result<(), AerospikeError> {
            unimplemented!()
        }

        async fn count_set(&self, _namespace: &str) -> Result<u64, AerospikeError> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn runtime_times_out_and_circuit_breaks_slow_cache() {
        let slow = Arc::new(SleepingAerospikeClient::new(
            CACHE_OPERATION_TIMEOUT + Duration::from_secs(5),
        ));
        let runtime = AerospikeRuntime::new(Some(slow.clone()));
        let doc = HashMap::new();

        let started = Instant::now();
        let err = runtime.put("ns", "doc", &doc).await.unwrap_err();
        assert!(
            started.elapsed() < CACHE_OPERATION_TIMEOUT + Duration::from_millis(150),
            "cache call should be bounded by runtime timeout, got {:?}",
            started.elapsed()
        );
        assert!(err.to_string().contains("timed out"));
        assert!(!runtime.is_connected_now());

        let started = Instant::now();
        let err = runtime.put("ns", "doc", &doc).await.unwrap_err();
        assert!(
            started.elapsed() < Duration::from_millis(50),
            "open circuit should fail fast, got {:?}",
            started.elapsed()
        );
        assert!(err.to_string().contains("circuit breaker open"));
        assert_eq!(
            slow.calls.load(Ordering::SeqCst),
            1,
            "open circuit must not fan out to the cache client"
        );
    }

    #[tokio::test]
    async fn runtime_half_open_probe_recovers_after_open_interval() {
        let slow = Arc::new(SleepingAerospikeClient::new(Duration::from_millis(1)));
        let runtime = AerospikeRuntime::new(Some(slow.clone()));
        let doc = HashMap::new();

        runtime.put("ns", "doc", &doc).await.unwrap_err();
        assert!(!runtime.is_connected_now());

        slow.fail.store(false, Ordering::SeqCst);
        tokio::time::sleep(CACHE_BREAKER_OPEN_FOR + Duration::from_millis(25)).await;

        runtime.put("ns", "doc", &doc).await.unwrap();
        assert!(runtime.is_connected_now());
        assert!(runtime.status().await.last_error.is_none());
    }
}
