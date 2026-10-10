//! Certified canonical reads. The backend is deliberately separate from the
//! ordinary, possibly projected/eventual document cache.
use crate::turbopuffer::TurbopufferError;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

pub type Attributes = HashMap<String, Value>;
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HydrationAdmission {
    pub run_id: String,
    pub namespace_bytes: i64,
    pub max_queries: i64,
    pub max_queried_bytes: i64,
    pub max_rows: i64,
    pub max_storage_bytes: i64,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct HydrationProgress {
    pub run_id: String,
    pub epoch: String,
    pub revision: i64,
    pub cursor: Option<String>,
    pub complete: bool,
    pub queries_reserved: i64,
    pub bytes_reserved: i64,
    pub rows: i64,
    pub storage_bytes: i64,
    pub observed_row_bytes: i64,
    pub row_receipts: i64,
    pub blocked: bool,
}
pub struct HydratedRow {
    pub id: String,
    pub key: String,
    pub attributes: Option<Attributes>,
}
#[async_trait]
pub trait DocumentReadCache: Send + Sync {
    async fn reserve_hydration(
        &self,
        _scope: &str,
        _version: &FenceVersion,
        _admission: &HydrationAdmission,
    ) -> Result<HydrationProgress, TurbopufferError> {
        Err(TurbopufferError::Other(
            "durable bulk hydration unavailable".into(),
        ))
    }
    async fn publish_hydration(
        &self,
        _scope: &str,
        _progress: &HydrationProgress,
        _rows: &[HydratedRow],
        _next: Option<&str>,
        _queried_bytes: Option<u64>,
    ) -> Result<HydrationProgress, TurbopufferError> {
        Err(TurbopufferError::Other(
            "durable bulk hydration unavailable".into(),
        ))
    }
    async fn materialization_identity(
        &self,
        _scope: &str,
        _version: &FenceVersion,
    ) -> Result<Option<String>, TurbopufferError> {
        Ok(None)
    }
    async fn materialized_page(
        &self,
        _scope: &str,
        _version: &FenceVersion,
        _identity: &str,
        _cursor: Option<&str>,
        _page_size: u32,
    ) -> Result<crate::models::DocumentPage, TurbopufferError> {
        Err(TurbopufferError::Other(
            "local materialized page unavailable".into(),
        ))
    }
    async fn materialized_rows(
        &self,
        _scope: &str,
        _version: &FenceVersion,
        _ids: &[String],
    ) -> Result<Option<HashMap<String, Option<Attributes>>>, TurbopufferError> {
        Ok(None)
    }
    async fn materialized_group(
        &self,
        _scope: &str,
        _version: &FenceVersion,
        _parent: &str,
    ) -> Result<Option<Vec<String>>, TurbopufferError> {
        Ok(None)
    }
    async fn materialized_write(
        &self,
        _scope: &str,
        _version: &FenceVersion,
        _rows: &[HydratedRow],
        _complete: bool,
    ) -> Result<(), TurbopufferError> {
        Ok(())
    }
    // None is a certified strong-read tombstone; a missing key is a cache miss.
    async fn get_many(
        &self,
        scope: &str,
        ids: &[String],
    ) -> Result<HashMap<String, Option<Attributes>>, TurbopufferError>;
    async fn invalidate(&self, scope: &str, ids: &[String]) -> Result<(), TurbopufferError>;
    async fn put_many(
        &self,
        scope: &str,
        rows: &HashMap<String, Option<Attributes>>,
    ) -> Result<(), TurbopufferError>;
}

pub(crate) struct NamespaceCache {
    pub scope: String,
    pub groups: HashMap<String, Vec<String>>,
}
pub struct CanonicalCache {
    pub backend: Arc<dyn DocumentReadCache>,
    prefix: String,
    next: std::sync::atomic::AtomicU64,
    namespaces: Mutex<HashMap<String, Arc<AsyncMutex<NamespaceCache>>>>,
}
impl CanonicalCache {
    pub fn new(backend: Arc<dyn DocumentReadCache>, prefix: String) -> Self {
        Self {
            backend,
            prefix,
            next: Default::default(),
            namespaces: Default::default(),
        }
    }
    pub(crate) fn reset(&self, state: &mut NamespaceCache) {
        state.scope = format!(
            "{}-{}",
            self.prefix,
            self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        state.groups.clear();
    }
    pub(crate) async fn lock(&self, namespace: &str) -> Option<OwnedMutexGuard<NamespaceCache>> {
        let lock = {
            let mut namespaces = self.namespaces.lock().unwrap();
            // Never evict a live namespace lock: another reader could bypass
            // an in-flight invalidation fence. Additional namespaces go uncached.
            if namespaces.len() >= 4096 && !namespaces.contains_key(namespace) {
                return None;
            }
            namespaces
                .entry(namespace.to_owned())
                .or_insert_with(|| {
                    let mut state = NamespaceCache {
                        scope: String::new(),
                        groups: HashMap::new(),
                    };
                    self.reset(&mut state);
                    Arc::new(AsyncMutex::new(state))
                })
                .clone()
        };
        Some(lock.lock_owned().await)
    }
}

/// Cache use requires a separately reviewed, enforced upstream credential
/// boundary. This enum does not configure or change upstream permissions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriterClass {
    Unverified,
    FencedGateway,
}
#[derive(Clone, Debug)]
pub struct FenceVersion {
    pub epoch: String,
    pub revision: i64,
    pub readable: bool,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct NamespaceIdentity {
    pub integer_ids: bool,
    pub generated: HashMap<String, String>,
    #[serde(default)]
    pub schema: Option<Value>,
}
#[async_trait]
pub trait CacheFence: Send + Sync {
    fn version(&self) -> FenceVersion;
    async fn identity(&self) -> Result<Option<NamespaceIdentity>, TurbopufferError>;
    async fn publish_identity(&self, identity: &NamespaceIdentity) -> Result<(), TurbopufferError>;
    async fn validate(&self) -> Result<(), TurbopufferError>;
    async fn lost(&self);
    async fn versions(&self, ids: &[String]) -> Result<HashMap<String, i64>, TurbopufferError>;
    async fn begin_write(&mut self, ids: Option<&[String]>) -> Result<(), TurbopufferError>;
    async fn complete_write(&mut self) -> Result<(), TurbopufferError>;
}
#[async_trait]
pub trait CacheFences: Send + Sync {
    async fn acquire(
        &self,
        domain: &str,
        namespace: &str,
    ) -> Result<Box<dyn CacheFence>, TurbopufferError>;
}

/// Must come from one operator-reviewed canonical configuration for every
/// writer of this provider namespace. Creating it does not enforce credentials.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriterIdentity {
    pub domain: String,
    pub endpoint: String,
    pub key_fingerprint: String,
}
impl WriterIdentity {
    pub fn for_static_key(domain: &str, endpoint: &str, key: &str) -> Option<Self> {
        use sha2::{Digest, Sha256};
        let url = reqwest::Url::parse(endpoint).ok()?;
        if domain.is_empty()
            || key.is_empty()
            || !matches!(url.scheme(), "http" | "https")
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return None;
        }
        Some(Self {
            domain: domain.into(),
            endpoint: url.as_str().trim_end_matches('/').into(),
            key_fingerprint: format!("{:x}", Sha256::digest(key.as_bytes())),
        })
    }
}

/// Obtained only from a bound, reviewed SharedCache; does not itself enforce upstream ACLs.
#[derive(Clone)]
pub struct CaptureWriterBoundary {
    domain: String,
}
impl CaptureWriterBoundary {
    pub fn domain(&self) -> &str {
        &self.domain
    }
}
pub struct SharedCache {
    backend: Arc<dyn DocumentReadCache>,
    fences: Arc<dyn CacheFences>,
    domain: String,
    writer: WriterClass,
    reviewed_identity: Option<WriterIdentity>,
    bound: bool,
}
impl SharedCache {
    pub fn new(
        backend: Arc<dyn DocumentReadCache>,
        fences: Arc<dyn CacheFences>,
        domain: String,
        writer: WriterClass,
    ) -> Self {
        Self {
            backend,
            fences,
            domain,
            writer,
            reviewed_identity: None,
            bound: false,
        }
    }
    /// Missing or mismatched canonical identity fails closed. The operator
    /// must enforce this SAME tuple for all writer replicas, prohibit aliases
    /// and bypass keys, and drain/revoke old generations before replacement.
    pub fn with_reviewed_writer_identity(mut self, identity: WriterIdentity) -> Self {
        self.reviewed_identity = Some(identity);
        self
    }
    /// Bind caller domain identity to this actual provider and static key.
    /// Credentials are hashed, never put into certificate keys or logs. A
    /// replacement key/endpoint cannot reuse the previous certificate scope.
    pub(crate) fn bind_provider(&mut self, endpoint: &str, key: Option<&str>) {
        self.bound = false;
        let Some(identity) =
            key.and_then(|key| WriterIdentity::for_static_key(&self.domain, endpoint, key))
        else {
            return;
        };
        if self.reviewed_identity.as_ref() != Some(&identity) {
            return;
        }
        self.domain = serde_json::to_string(&(
            &identity.domain,
            &identity.endpoint,
            &identity.key_fingerprint,
        ))
        .unwrap();
        self.bound = true;
    }

    pub fn certified(&self) -> bool {
        self.bound && self.writer == WriterClass::FencedGateway && !self.domain.is_empty()
    }
    pub fn capture_boundary(&self) -> Result<CaptureWriterBoundary, TurbopufferError> {
        if !self.certified() {
            return Err(TurbopufferError::Other(
                "unverified capture writer boundary".into(),
            ));
        }
        Ok(CaptureWriterBoundary {
            domain: self.domain.clone(),
        })
    }
    pub async fn acquire(&self, namespace: &str) -> Result<CacheSession, TurbopufferError> {
        if !self.certified() {
            return Err(TurbopufferError::Other(
                "unverified cache writer boundary".into(),
            ));
        }
        let fence = self.fences.acquire(&self.domain, namespace).await?;
        let scope = serde_json::to_string(&(&self.domain, namespace)).unwrap();
        Ok(CacheSession {
            backend: self.backend.clone(),
            fence,
            scope,
        })
    }
}

/// Owns the original shared lock until all protected HTTP/cache work finishes.
/// No methods acquire Function source locks. Old versions are never republished
/// under newer identities, even after cancellation or delayed cache responses.
pub struct CacheSession {
    backend: Arc<dyn DocumentReadCache>,
    pub fence: Box<dyn CacheFence>,
    scope: String,
}
impl CacheSession {
    pub async fn protect<T>(
        &self,
        work: impl std::future::Future<Output = Result<T, TurbopufferError>>,
    ) -> Result<T, TurbopufferError> {
        self.fence.validate().await?;
        tokio::select! {
            biased;
            _ = self.fence.lost() => Err(TurbopufferError::Other("shared cache fence lost".into())),
            result = work => { self.fence.validate().await?; result }
        }
    }
    async fn keys(&self, ids: &[String]) -> Result<HashMap<String, String>, TurbopufferError> {
        let version = self.fence.version();
        let versions = self.fence.versions(ids).await?;
        ids.iter()
            .map(|id| {
                let revision = versions.get(id).ok_or_else(|| {
                    TurbopufferError::Other("missing durable document version".into())
                })?;
                Ok((
                    id.clone(),
                    serde_json::to_string(&("row", &version.epoch, revision, id)).unwrap(),
                ))
            })
            .collect()
    }
    pub async fn get_many(
        &self,
        ids: &[String],
    ) -> Result<HashMap<String, Option<Attributes>>, TurbopufferError> {
        if !self.fence.version().readable {
            return Ok(HashMap::new());
        }
        self.protect(async {
            if let Some(rows) = self
                .backend
                .materialized_rows(&self.scope, &self.fence.version(), ids)
                .await?
            {
                return Ok(rows);
            }
            let keys = self.keys(ids).await?;
            let physical: Vec<_> = keys.values().cloned().collect();
            let mut rows = self.backend.get_many(&self.scope, &physical).await?;
            Ok(keys
                .into_iter()
                .filter_map(|(id, key)| rows.remove(&key).map(|row| (id, row)))
                .collect())
        })
        .await
    }
    pub async fn put_many(
        &self,
        rows: &HashMap<String, Option<Attributes>>,
        mutation: bool,
    ) -> Result<(), TurbopufferError> {
        // Pending rows may only be published by the still-held mutation guard.
        // A poisoned namespace never becomes eligible through a later write.
        if !self.fence.version().readable && !mutation {
            return Ok(());
        }
        self.protect(async {
            let keys = self.keys(&rows.keys().cloned().collect::<Vec<_>>()).await?;
            let physical = rows
                .iter()
                .map(|(id, row)| (keys[id].clone(), row.clone()))
                .collect();
            self.backend.put_many(&self.scope, &physical).await
        })
        .await
    }
    pub async fn schema(&self) -> Result<Option<NamespaceIdentity>, TurbopufferError> {
        self.protect(self.fence.identity()).await
    }
    pub async fn put_schema(&self, identity: &NamespaceIdentity) -> Result<(), TurbopufferError> {
        if !self.fence.version().readable {
            return Ok(());
        }
        self.protect(self.fence.publish_identity(identity)).await
    }
    fn group_key(&self, parent: &str) -> String {
        let version = self.fence.version();
        serde_json::to_string(&("group", version.epoch, version.revision, parent)).unwrap()
    }
    pub async fn group(&self, parent: &str) -> Result<Option<Vec<String>>, TurbopufferError> {
        if !self.fence.version().readable {
            return Ok(None);
        }
        self.protect(async {
            if let Some(ids) = self
                .backend
                .materialized_group(&self.scope, &self.fence.version(), parent)
                .await?
            {
                return Ok(Some(ids));
            }
            let key = self.group_key(parent);
            let mut rows = self
                .backend
                .get_many(&self.scope, std::slice::from_ref(&key))
                .await?;
            Ok(rows
                .remove(&key)
                .flatten()
                .and_then(|mut row| row.remove("ids"))
                .and_then(|value| serde_json::from_value(value).ok()))
        })
        .await
    }
    pub async fn put_group(&self, parent: &str, ids: &[String]) -> Result<(), TurbopufferError> {
        if !self.fence.version().readable {
            return Ok(());
        }
        self.protect(self.backend.put_many(
            &self.scope,
            &HashMap::from([(
                self.group_key(parent),
                Some(HashMap::from([("ids".into(), serde_json::json!(ids))])),
            )]),
        ))
        .await
    }
    pub async fn materialization_identity(&self) -> Result<Option<String>, TurbopufferError> {
        if !self.fence.version().readable {
            return Ok(None);
        }
        self.protect(
            self.backend
                .materialization_identity(&self.scope, &self.fence.version()),
        )
        .await
    }
    pub async fn materialized_page(
        &self,
        identity: &str,
        cursor: Option<&str>,
        page_size: u32,
    ) -> Result<crate::models::DocumentPage, TurbopufferError> {
        if !self.fence.version().readable {
            return Err(TurbopufferError::Other(
                "local materialization authority unreadable".into(),
            ));
        }
        self.protect(self.backend.materialized_page(
            &self.scope,
            &self.fence.version(),
            identity,
            cursor,
            page_size,
        ))
        .await
    }
    pub async fn reserve_hydration(
        &self,
        admission: &HydrationAdmission,
    ) -> Result<HydrationProgress, TurbopufferError> {
        if !self.fence.version().readable {
            return Err(TurbopufferError::Other(
                "bulk hydration requires readable verified authority".into(),
            ));
        }
        self.protect(
            self.backend
                .reserve_hydration(&self.scope, &self.fence.version(), admission),
        )
        .await
    }
    pub async fn publish_hydration(
        &self,
        progress: &HydrationProgress,
        rows: &HashMap<String, Option<Attributes>>,
        next: Option<&str>,
        queried_bytes: Option<u64>,
    ) -> Result<HydrationProgress, TurbopufferError> {
        let keys = self.keys(&rows.keys().cloned().collect::<Vec<_>>()).await?;
        let rows = rows
            .iter()
            .map(|(id, attributes)| HydratedRow {
                id: id.clone(),
                key: keys[id].clone(),
                attributes: attributes.clone(),
            })
            .collect::<Vec<_>>();
        self.protect(self.backend.publish_hydration(
            &self.scope,
            progress,
            &rows,
            next,
            queried_bytes,
        ))
        .await
    }
    pub async fn materialized_write(
        &self,
        ids: Option<&[String]>,
        rows: &HashMap<String, Option<Attributes>>,
    ) -> Result<(), TurbopufferError> {
        let keys = self.keys(&rows.keys().cloned().collect::<Vec<_>>()).await?;
        let complete = ids.is_some_and(|ids| {
            ids.len() == rows.len() && ids.iter().all(|id| rows.contains_key(id))
        });
        let rows = rows
            .iter()
            .map(|(id, attributes)| HydratedRow {
                id: id.clone(),
                key: keys[id].clone(),
                attributes: attributes.clone(),
            })
            .collect::<Vec<_>>();
        self.protect(self.backend.materialized_write(
            &self.scope,
            &self.fence.version(),
            &rows,
            complete,
        ))
        .await
    }
}

#[cfg(test)]
mod writer_identity_tests {
    use super::*;
    struct NoIo;
    #[async_trait]
    impl DocumentReadCache for NoIo {
        async fn get_many(
            &self,
            _: &str,
            _: &[String],
        ) -> Result<HashMap<String, Option<Attributes>>, TurbopufferError> {
            panic!("identity gate must precede cache IO")
        }
        async fn invalidate(&self, _: &str, _: &[String]) -> Result<(), TurbopufferError> {
            panic!("identity gate must precede cache IO")
        }
        async fn put_many(
            &self,
            _: &str,
            _: &HashMap<String, Option<Attributes>>,
        ) -> Result<(), TurbopufferError> {
            panic!("identity gate must precede cache IO")
        }
    }
    #[async_trait]
    impl CacheFences for NoIo {
        async fn acquire(&self, _: &str, _: &str) -> Result<Box<dyn CacheFence>, TurbopufferError> {
            panic!("identity gate must precede lock IO")
        }
    }
    fn cache(domain: &str) -> SharedCache {
        SharedCache::new(
            Arc::new(NoIo),
            Arc::new(NoIo),
            domain.into(),
            WriterClass::FencedGateway,
        )
    }
    fn reviewed() -> WriterIdentity {
        WriterIdentity::for_static_key("canonical", "https://provider.example", "key-a").unwrap()
    }
    #[test]
    fn provider_key_rotation_requires_new_review_and_old_generation_drain() {
        let mut c = cache("canonical").with_reviewed_writer_identity(reviewed());
        c.bind_provider("https://provider.example", Some("key-b"));
        assert!(!c.certified());
        let mut unreviewed = cache("canonical");
        unreviewed.bind_provider("https://provider.example", Some("key-a"));
        assert!(!unreviewed.certified());
    }
    #[test]
    fn supplied_domain_rotation_cannot_split_reviewed_writer_locks() {
        let mut c = cache("different").with_reviewed_writer_identity(reviewed());
        c.bind_provider("https://provider.example", Some("key-a"));
        assert!(!c.certified());
    }
    #[test]
    fn provider_url_aliases_fail_closed_except_equivalent_url_normalization() {
        let mut alias = cache("canonical").with_reviewed_writer_identity(reviewed());
        alias.bind_provider("https://alias.example", Some("key-a"));
        assert!(!alias.certified());
        let mut equivalent = cache("canonical").with_reviewed_writer_identity(reviewed());
        equivalent.bind_provider("https://PROVIDER.example:443/", Some("key-a"));
        assert!(equivalent.certified());
        for url in [
            "https://provider.example?alias=1",
            "https://provider.example#alias",
            "https://user@provider.example",
        ] {
            assert!(WriterIdentity::for_static_key("canonical", url, "key-a").is_none());
        }
    }
    #[test]
    fn capture_source_retains_the_exact_bound_client_and_refuses_other_identity() {
        use crate::turbopuffer::HttpTurbopufferClient;
        let actual = Arc::new(
            HttpTurbopufferClient::new("key-a", "https://provider.example")
                .with_shared_document_cache(
                    cache("canonical").with_reviewed_writer_identity(reviewed()),
                ),
        );
        let source = actual.capture_source().unwrap();
        assert!(Arc::ptr_eq(source.client(), &actual));
        let identity: Vec<String> = serde_json::from_str(source.boundary().domain()).unwrap();
        assert_eq!(identity[0], "canonical");
        assert_eq!(identity[1], "https://provider.example");
        for (key, url) in [
            ("key-b", "https://provider.example"),
            ("key-a", "https://alias.example"),
            ("", "https://provider.example"),
        ] {
            let different = Arc::new(
                HttpTurbopufferClient::new(key, url).with_shared_document_cache(
                    cache("canonical").with_reviewed_writer_identity(reviewed()),
                ),
            );
            assert!(different.capture_source().is_err());
        }
        let absent = Arc::new(HttpTurbopufferClient::new(
            "key-a",
            "https://provider.example",
        ));
        assert!(absent.capture_source().is_err());
        let unreviewed = Arc::new(
            HttpTurbopufferClient::new("key-a", "https://provider.example")
                .with_shared_document_cache(cache("canonical")),
        );
        assert!(unreviewed.capture_source().is_err());
    }
}
