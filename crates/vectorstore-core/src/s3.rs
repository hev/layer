use async_trait::async_trait;

/// Marker message carried by [`S3Error::not_configured`] errors so call sites
/// can distinguish "this deployment has no object store" from a transient or
/// upstream S3 failure.
pub const OBJECT_STORE_NOT_CONFIGURED: &str = "object store not configured";

#[derive(Debug, thiserror::Error)]
#[error("S3 error: {0}")]
pub struct S3Error(pub String);

impl S3Error {
    /// Typed error returned by [`NoopS3Client`] writes: the gateway was
    /// composed without an object store (no `S3_BUCKET` configured).
    pub fn not_configured() -> Self {
        Self(OBJECT_STORE_NOT_CONFIGURED.to_string())
    }

    pub fn is_not_configured(&self) -> bool {
        self.0 == OBJECT_STORE_NOT_CONFIGURED
    }
}

/// A scoped client PUT. Treat the returned URL as a bearer credential.
pub struct PresignedPut {
    pub url: String,
    pub headers: std::collections::BTreeMap<String, String>,
}

pub const STAGED_UPLOAD_NOT_CONFIGURED: &str =
    "staged uploads require S3 with a matching, explicit or inferable region";

#[async_trait]
pub trait S3Client: Send + Sync {
    /// Put an object, replacing any existing object at the same key.
    async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), S3Error>;

    /// Put an object with conditional write (If-None-Match: *).
    /// Returns Ok(true) if written, Ok(false) if object already existed.
    async fn put_if_not_exists(&self, key: &str, body: Vec<u8>) -> Result<bool, S3Error>;

    /// Get an object by key.
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, S3Error>;

    /// Read through a bounded buffer. Staged-upload providers must implement
    /// this without reading a larger body into memory.
    async fn get_limited(&self, _key: &str, _limit: u64) -> Result<Option<Vec<u8>>, S3Error> {
        Err(S3Error(STAGED_UPLOAD_NOT_CONFIGURED.into()))
    }

    /// Sign a create-only PUT for a single key, exact length and SHA-256.
    async fn presign_put(
        &self,
        _key: &str,
        _size: usize,
        _sha256: [u8; 32],
        _now: std::time::SystemTime,
        _ttl: std::time::Duration,
    ) -> Result<PresignedPut, S3Error> {
        Err(S3Error(STAGED_UPLOAD_NOT_CONFIGURED.into()))
    }

    /// List objects with a given prefix, sorted by key.
    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, S3Error>;

    /// Delete an object by key. Missing keys are treated as success.
    async fn delete_key(&self, key: &str) -> Result<(), S3Error>;

    /// Delete at most 1,000 keys. Missing keys succeed; any per-object error
    /// fails the batch so callers can safely retry the idempotent operation.
    async fn delete_keys(&self, keys: &[String]) -> Result<(), S3Error> {
        if keys.len() > 1000 {
            return Err(S3Error("delete batch exceeds 1000 keys".into()));
        }
        for key in keys {
            self.delete_key(key).await?;
        }
        Ok(())
    }

    /// Whether this client is backed by a real object store. Callers use this
    /// to skip S3-backed work entirely (background writers) or fail fast with
    /// a clear error (feature endpoints) instead of surfacing per-op failures.
    fn is_configured(&self) -> bool {
        true
    }
}

/// Conditional object access for correctness witnesses, separate from bulk S3
/// projections. Missing/unsupported stores must fail, never pretend persistence.
#[derive(Clone, Debug)]
pub struct WitnessObject {
    pub bytes: Vec<u8>,
    pub etag: String,
}
#[async_trait]
pub trait WitnessObjectStore: Send + Sync {
    async fn witness_get(&self, key: &str) -> Result<Option<WitnessObject>, S3Error>;
    /// None means create-only; Some(etag) means replace exactly that version.
    async fn witness_cas(
        &self,
        key: &str,
        expected: Option<&str>,
        bytes: Vec<u8>,
    ) -> Result<bool, S3Error>;
}
pub fn witness_scope_key(domain: &str, namespace: &str) -> String {
    use sha2::{Digest, Sha256};
    format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&(domain, namespace)).unwrap())
    )
}

pub use crate::blob_store::ObjectStoreClient;

// --- Noop implementation for deployments without an object store ---

/// Composed when no object store is configured (no `S3_BUCKET`). Reads degrade
/// instantly to "no objects"; writes fail with a typed
/// "object store not configured" error. Never touches the AWS credential
/// chain, so a standalone gateway pays no IMDS lookup or timeout.
pub struct NoopS3Client;

#[async_trait]
impl S3Client for NoopS3Client {
    async fn put(&self, _key: &str, _body: Vec<u8>) -> Result<(), S3Error> {
        Err(S3Error::not_configured())
    }

    async fn put_if_not_exists(&self, _key: &str, _body: Vec<u8>) -> Result<bool, S3Error> {
        Err(S3Error::not_configured())
    }

    async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>, S3Error> {
        Ok(None)
    }

    async fn list_keys(&self, _prefix: &str) -> Result<Vec<String>, S3Error> {
        Ok(Vec::new())
    }

    async fn delete_key(&self, _key: &str) -> Result<(), S3Error> {
        Err(S3Error::not_configured())
    }

    fn is_configured(&self) -> bool {
        false
    }
}

// --- Mock implementation for testing ---

pub struct MockS3Client {
    store: tokio::sync::RwLock<std::collections::HashMap<String, Vec<u8>>>,
}

impl Default for MockS3Client {
    fn default() -> Self {
        Self::new()
    }
}

impl MockS3Client {
    pub fn new() -> Self {
        Self {
            store: tokio::sync::RwLock::new(std::collections::HashMap::new()),
        }
    }
}

#[async_trait]
impl S3Client for MockS3Client {
    async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), S3Error> {
        self.store.write().await.insert(key.to_string(), body);
        Ok(())
    }

    async fn put_if_not_exists(&self, key: &str, body: Vec<u8>) -> Result<bool, S3Error> {
        let mut store = self.store.write().await;
        if store.contains_key(key) {
            Ok(false)
        } else {
            store.insert(key.to_string(), body);
            Ok(true)
        }
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, S3Error> {
        let store = self.store.read().await;
        Ok(store.get(key).cloned())
    }

    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, S3Error> {
        let store = self.store.read().await;
        let mut keys: Vec<String> = store
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        keys.sort();
        Ok(keys)
    }

    async fn delete_key(&self, key: &str) -> Result<(), S3Error> {
        self.store.write().await.remove(key);
        Ok(())
    }
}

#[async_trait]
impl WitnessObjectStore for MockS3Client {
    async fn witness_get(&self, key: &str) -> Result<Option<WitnessObject>, S3Error> {
        use sha2::{Digest, Sha256};
        Ok(self.store.read().await.get(key).map(|bytes| WitnessObject {
            bytes: bytes.clone(),
            etag: format!("{:x}", Sha256::digest(bytes)),
        }))
    }
    async fn witness_cas(
        &self,
        key: &str,
        expected: Option<&str>,
        bytes: Vec<u8>,
    ) -> Result<bool, S3Error> {
        use sha2::{Digest, Sha256};
        let mut store = self.store.write().await;
        let actual = store
            .get(key)
            .map(|bytes| format!("{:x}", Sha256::digest(bytes)));
        if actual.as_deref() != expected {
            return Ok(false);
        }
        store.insert(key.into(), bytes);
        Ok(true)
    }
}
