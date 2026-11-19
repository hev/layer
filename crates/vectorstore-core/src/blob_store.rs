//! One object-store implementation for every durable-state path, on the
//! `object_store` crate. The backend and location come from a URL:
//!
//! - `s3://bucket[/prefix]`: Amazon S3 or an S3-compatible store (MinIO).
//! - `az://container[/prefix]`: Azure Blob Storage (or Azurite).
//!
//! [`ObjectStoreClient::from_env`] resolves `OBJECT_STORE_URL` first and falls
//! back to the legacy bucket variables (`S3_BUCKET` / `S3_REGION` /
//! `S3_ENDPOINT` for the gateway, `HEVLAYER_SNAPSHOT_*` for pipeline workers),
//! so an S3 deployment that never sets `OBJECT_STORE_URL` keeps its config.
//!
//! Azure credentials come from the environment the way AKS injects them:
//! `AZURE_STORAGE_ACCOUNT_NAME` plus Workload Identity (`AZURE_CLIENT_ID`,
//! `AZURE_TENANT_ID`, `AZURE_FEDERATED_TOKEN_FILE`, `AZURE_AUTHORITY_HOST`).
//! `AZURE_STORAGE_USE_EMULATOR=true` targets Azurite (endpoint from
//! `AZURITE_BLOB_STORAGE_URL`, default `http://127.0.0.1:10000`), and
//! `AZURE_STORAGE_ENDPOINT` overrides the account endpoint otherwise.

use std::sync::Arc;

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use object_store::{
    aws::{AmazonS3Builder, S3ConditionalPut},
    azure::MicrosoftAzureBuilder,
    path::Path,
    ObjectMeta, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion,
};

use crate::s3::{S3Client, S3Error, WitnessObject, WitnessObjectStore};

/// Selects the backend and location of durable state.
pub const OBJECT_STORE_URL_ENV: &str = "OBJECT_STORE_URL";

/// Largest witness object [`WitnessObjectStore::witness_get`] accepts.
const WITNESS_MAX_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Backend {
    S3,
    Azure,
}

impl Backend {
    pub fn scheme(self) -> &'static str {
        match self {
            Self::S3 => "s3",
            Self::Azure => "az",
        }
    }
}

/// A parsed `s3://` or `az://` location.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectStoreUrl {
    pub backend: Backend,
    /// S3 bucket or Azure container.
    pub bucket: String,
    /// Key prefix inside the bucket: empty, or ending in `/`.
    pub prefix: String,
}

impl ObjectStoreUrl {
    pub fn parse(url: &str) -> Result<Self, S3Error> {
        let url = url.trim();
        let (backend, rest) = if let Some(rest) = url.strip_prefix("s3://") {
            (Backend::S3, rest)
        } else if let Some(rest) = url
            .strip_prefix("az://")
            .or_else(|| url.strip_prefix("azure://"))
        {
            (Backend::Azure, rest)
        } else {
            return Err(S3Error(format!(
                "{OBJECT_STORE_URL_ENV} must be s3://bucket[/prefix] or az://container[/prefix], got {url:?}"
            )));
        };
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        if bucket.is_empty() {
            return Err(S3Error(format!(
                "{OBJECT_STORE_URL_ENV} {url:?} has no bucket or container"
            )));
        }
        let prefix = prefix.trim_matches('/');
        let prefix = if prefix.is_empty() {
            String::new()
        } else {
            Path::parse(prefix)
                .map_err(|error| S3Error(format!("invalid object store prefix: {error}")))?;
            format!("{prefix}/")
        };
        Ok(Self {
            backend,
            bucket: bucket.to_string(),
            prefix,
        })
    }

    /// `s3://bucket/prefix/` form, with the same prefix rules as [`Self::parse`].
    pub fn uri_of(&self, key: &str) -> String {
        format!(
            "{}://{}/{}{key}",
            self.backend.scheme(),
            self.bucket,
            self.prefix
        )
    }

    /// The same location narrowed to `segment/` below the prefix.
    pub fn child(&self, segment: &str) -> Self {
        let segment = segment.trim_matches('/');
        let mut child = self.clone();
        if !segment.is_empty() {
            child.prefix = format!("{}{segment}/", self.prefix);
        }
        child
    }
}

/// S3 settings that do not live in the URL. Empty means the AWS defaults
/// (`AWS_REGION`, `AWS_ENDPOINT`, the credential chain).
#[derive(Clone, Debug, Default)]
pub struct S3Settings {
    pub region: Option<String>,
    /// Custom endpoint (MinIO). Forces path-style requests and allows HTTP.
    pub endpoint: Option<String>,
}

/// Names of the legacy bucket variables a binary reads when
/// `OBJECT_STORE_URL` is unset.
#[derive(Clone, Copy, Debug)]
pub struct LegacyEnv {
    pub bucket: &'static str,
    pub region: &'static str,
    pub endpoint: &'static str,
}

/// The gateway and transform runtime: `S3_BUCKET` / `S3_REGION` / `S3_ENDPOINT`.
pub const GATEWAY_ENV: LegacyEnv = LegacyEnv {
    bucket: "S3_BUCKET",
    region: "S3_REGION",
    endpoint: "S3_ENDPOINT",
};

/// Pipeline workers: the operator-injected `HEVLAYER_SNAPSHOT_*` variables.
pub const SNAPSHOT_ENV: LegacyEnv = LegacyEnv {
    bucket: "HEVLAYER_SNAPSHOT_BUCKET",
    region: "HEVLAYER_SNAPSHOT_REGION",
    endpoint: "HEVLAYER_SNAPSHOT_ENDPOINT",
};

fn env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

impl LegacyEnv {
    /// The configured location: `OBJECT_STORE_URL` when set, else
    /// `s3://<legacy bucket>`, else `None` (no object store).
    pub fn location(&self) -> Result<Option<(ObjectStoreUrl, S3Settings)>, S3Error> {
        let settings = S3Settings {
            region: env_value(self.region),
            endpoint: env_value(self.endpoint),
        };
        if let Some(url) = env_value(OBJECT_STORE_URL_ENV) {
            return Ok(Some((ObjectStoreUrl::parse(&url)?, settings)));
        }
        Ok(env_value(self.bucket).map(|bucket| {
            (
                ObjectStoreUrl {
                    backend: Backend::S3,
                    bucket,
                    prefix: String::new(),
                },
                settings,
            )
        }))
    }
}

/// [`S3Client`] and [`WitnessObjectStore`] on S3 or Azure Blob.
pub struct ObjectStoreClient {
    store: Arc<dyn ObjectStore>,
    location: ObjectStoreUrl,
}

impl ObjectStoreClient {
    /// Build from the environment: `OBJECT_STORE_URL`, else the legacy
    /// variables in `legacy`. `Ok(None)` means no object store is configured.
    pub fn from_env(legacy: LegacyEnv) -> Result<Option<Self>, S3Error> {
        legacy
            .location()?
            .map(|(location, settings)| Self::new(location, &settings))
            .transpose()
    }

    /// Like [`Self::from_env`], but a missing store is an error naming both
    /// settings.
    pub fn required_from_env(legacy: LegacyEnv) -> Result<Self, S3Error> {
        Self::from_env(legacy)?.ok_or_else(|| {
            S3Error(format!(
                "no object store configured: set {OBJECT_STORE_URL_ENV} or {}",
                legacy.bucket
            ))
        })
    }

    /// An S3 bucket with explicit region and optional custom endpoint.
    pub fn s3(bucket: &str, region: &str, endpoint: Option<&str>) -> Result<Self, S3Error> {
        Self::new(
            ObjectStoreUrl {
                backend: Backend::S3,
                bucket: bucket.to_string(),
                prefix: String::new(),
            },
            &S3Settings {
                region: Some(region.to_string()),
                endpoint: endpoint.map(str::to_string),
            },
        )
    }

    pub fn new(location: ObjectStoreUrl, s3: &S3Settings) -> Result<Self, S3Error> {
        let store: Arc<dyn ObjectStore> = match location.backend {
            Backend::S3 => {
                let mut builder = AmazonS3Builder::from_env()
                    .with_bucket_name(&location.bucket)
                    .with_conditional_put(S3ConditionalPut::ETagMatch);
                if let Some(region) = &s3.region {
                    builder = builder.with_region(region);
                } else if std::env::var("AWS_REGION").is_err()
                    && std::env::var("AWS_DEFAULT_REGION").is_err()
                {
                    builder = builder.with_region("us-east-1");
                }
                if let Some(endpoint) = &s3.endpoint {
                    builder = builder
                        .with_endpoint(endpoint)
                        .with_virtual_hosted_style_request(false)
                        .with_allow_http(endpoint.starts_with("http://"));
                }
                Arc::new(builder.build().map_err(store_error)?)
            }
            Backend::Azure => Arc::new(
                MicrosoftAzureBuilder::from_env()
                    .with_container_name(&location.bucket)
                    .build()
                    .map_err(store_error)?,
            ),
        };
        Ok(Self { store, location })
    }

    /// Wrap an existing store (tests, or a store built elsewhere).
    pub fn from_store(store: Arc<dyn ObjectStore>, location: ObjectStoreUrl) -> Self {
        Self { store, location }
    }

    pub fn location(&self) -> &ObjectStoreUrl {
        &self.location
    }

    pub fn backend(&self) -> Backend {
        self.location.backend
    }

    /// The same bucket narrowed to `segment/` below the current prefix.
    pub fn child(&self, segment: &str) -> Self {
        Self {
            store: self.store.clone(),
            location: self.location.child(segment),
        }
    }

    fn path(&self, key: &str) -> Result<Path, S3Error> {
        let full = format!("{}{key}", self.location.prefix);
        let path = Path::parse(&full)
            .map_err(|error| S3Error(format!("invalid object key {key:?}: {error}")))?;
        // object_store drops leading/trailing delimiters; refuse keys that
        // would silently alias another key.
        if path.as_ref() != full {
            return Err(S3Error(format!(
                "invalid object key {key:?}: leading or trailing '/'"
            )));
        }
        Ok(path)
    }

    /// Objects whose key (relative to the prefix) starts with `prefix`, as a
    /// plain string prefix like S3 `ListObjectsV2`, sorted by key.
    pub async fn list_objects(&self, prefix: &str) -> Result<Vec<ObjectEntry>, S3Error> {
        let full = format!("{}{prefix}", self.location.prefix);
        // object_store lists by whole path segments; list the enclosing
        // directory and filter by the raw string prefix.
        let directory = match full.rfind('/') {
            Some(slash) => Some(self.path_raw(&full[..slash])?),
            None => None,
        };
        let mut entries = self
            .store
            .list(directory.as_ref())
            .map_err(store_error)
            .try_filter_map(|meta: ObjectMeta| {
                let entry = meta
                    .location
                    .as_ref()
                    .strip_prefix(self.location.prefix.as_str())
                    .filter(|key| key.starts_with(prefix))
                    .map(|key| ObjectEntry {
                        key: key.to_string(),
                        size: meta.size,
                        e_tag: meta.e_tag.clone(),
                        last_modified: meta.last_modified,
                    });
                async move { Ok(entry) }
            })
            .try_collect::<Vec<_>>()
            .await?;
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(entries)
    }

    fn path_raw(&self, full: &str) -> Result<Path, S3Error> {
        Path::parse(full).map_err(|error| S3Error(format!("invalid object prefix: {error}")))
    }

    /// Read at most `max_bytes`; a larger object is an error, a missing one `None`.
    pub async fn get_limited(&self, key: &str, max_bytes: u64) -> Result<Option<Vec<u8>>, S3Error> {
        let result = match self.store.get(&self.path(key)?).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(store_error(error)),
        };
        if result.meta.size > max_bytes {
            return Err(S3Error("object exceeds byte limit".into()));
        }
        let mut body = Vec::with_capacity(result.meta.size as usize);
        let mut stream = result.into_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(store_error)?;
            if body.len() as u64 + chunk.len() as u64 > max_bytes {
                return Err(S3Error("object exceeds byte limit".into()));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(Some(body))
    }

    async fn put_mode(&self, key: &str, body: Vec<u8>, mode: PutMode) -> Result<bool, S3Error> {
        let options = PutOptions {
            mode,
            ..Default::default()
        };
        match self
            .store
            .put_opts(&self.path(key)?, PutPayload::from(body), options)
            .await
        {
            Ok(_) => Ok(true),
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => Ok(false),
            Err(error) => Err(store_error(error)),
        }
    }
}

/// One listed object, relative to the client's prefix.
#[derive(Clone, Debug)]
pub struct ObjectEntry {
    pub key: String,
    pub size: u64,
    pub e_tag: Option<String>,
    pub last_modified: chrono::DateTime<chrono::Utc>,
}

fn store_error(error: object_store::Error) -> S3Error {
    S3Error(error.to_string())
}

#[async_trait]
impl S3Client for ObjectStoreClient {
    async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), S3Error> {
        self.store
            .put(&self.path(key)?, PutPayload::from(body))
            .await
            .map_err(store_error)?;
        Ok(())
    }

    async fn put_if_not_exists(&self, key: &str, body: Vec<u8>) -> Result<bool, S3Error> {
        self.put_mode(key, body, PutMode::Create).await
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, S3Error> {
        match self.store.get(&self.path(key)?).await {
            Ok(result) => Ok(Some(result.bytes().await.map_err(store_error)?.to_vec())),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(store_error(error)),
        }
    }

    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, S3Error> {
        Ok(self
            .list_objects(prefix)
            .await?
            .into_iter()
            .map(|entry| entry.key)
            .collect())
    }

    async fn delete_key(&self, key: &str) -> Result<(), S3Error> {
        match self.store.delete(&self.path(key)?).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(store_error(error)),
        }
    }

    async fn delete_keys(&self, keys: &[String]) -> Result<(), S3Error> {
        if keys.len() > 1000 {
            return Err(S3Error("delete batch exceeds 1000 keys".into()));
        }
        if keys.is_empty() {
            return Ok(());
        }
        let paths = keys
            .iter()
            .map(|key| {
                self.path(key)
                    .map_err(|S3Error(message)| object_store::Error::Generic {
                        store: "layer",
                        source: message.into(),
                    })
            })
            .collect::<Vec<_>>();
        // S3 sends one DeleteObjects request, Azure one blob batch per 256 keys.
        let mut results = self
            .store
            .delete_stream(futures::stream::iter(paths).boxed());
        let mut failures = Vec::new();
        while let Some(result) = results.next().await {
            match result {
                Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => failures.push(error.to_string()),
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(S3Error(format!(
                "batch delete failed for {} of {} keys: {}",
                failures.len(),
                keys.len(),
                failures.join("; ")
            )))
        }
    }
}

#[async_trait]
impl WitnessObjectStore for ObjectStoreClient {
    async fn witness_get(&self, key: &str) -> Result<Option<WitnessObject>, S3Error> {
        let result = match self.store.get(&self.path(key)?).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(store_error(error)),
        };
        if result.meta.size > WITNESS_MAX_BYTES {
            return Err(S3Error("witness object missing/big content length".into()));
        }
        let etag = result
            .meta
            .e_tag
            .clone()
            .ok_or_else(|| S3Error("witness object lacks ETag".into()))?;
        let bytes = result.bytes().await.map_err(store_error)?.to_vec();
        Ok(Some(WitnessObject { bytes, etag }))
    }

    async fn witness_cas(
        &self,
        key: &str,
        expected: Option<&str>,
        bytes: Vec<u8>,
    ) -> Result<bool, S3Error> {
        let mode = match expected {
            None => PutMode::Create,
            Some(etag) => PutMode::Update(UpdateVersion {
                e_tag: Some(etag.to_string()),
                version: None,
            }),
        };
        self.put_mode(key, bytes, mode).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn parses_urls() {
        let url = ObjectStoreUrl::parse("az://layer/state/").unwrap();
        assert_eq!(url.backend, Backend::Azure);
        assert_eq!(url.bucket, "layer");
        assert_eq!(url.prefix, "state/");
        assert_eq!(url.uri_of("a/b"), "az://layer/state/a/b");
        assert_eq!(url.child("x").uri_of("y"), "az://layer/state/x/y");
        let url = ObjectStoreUrl::parse("s3://bucket").unwrap();
        assert_eq!(url.backend, Backend::S3);
        assert_eq!(url.prefix, "");
        assert!(ObjectStoreUrl::parse("gs://bucket").is_err());
        assert!(ObjectStoreUrl::parse("s3:///prefix").is_err());
        assert!(ObjectStoreUrl::parse("s3://b/a//c").is_err());
    }

    #[tokio::test]
    async fn prefix_listing_is_a_string_prefix_and_keys_stay_relative() {
        let client = ObjectStoreClient::from_store(
            Arc::new(object_store::memory::InMemory::new()),
            ObjectStoreUrl::parse("s3://b/root").unwrap(),
        );
        for key in ["snap/ns-1/a", "snap/ns-10/b", "snap/other", "top"] {
            client.put(key, b"x".to_vec()).await.unwrap();
        }
        assert_eq!(
            client.list_keys("snap/ns-1").await.unwrap(),
            vec!["snap/ns-1/a", "snap/ns-10/b"]
        );
        assert_eq!(client.list_keys("").await.unwrap().len(), 4);
        assert_eq!(client.list_keys("t").await.unwrap(), vec!["top"]);
        assert!(client.put("/lead", vec![]).await.is_err());
        assert!(client.put("trail/", vec![]).await.is_err());
    }

    async fn mock_s3(app: axum::Router) -> (ObjectStoreClient, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let store = AmazonS3Builder::new()
            .with_bucket_name("bucket")
            .with_region("us-east-1")
            .with_access_key_id("test")
            .with_secret_access_key("test")
            .with_endpoint(format!("http://{address}"))
            .with_allow_http(true)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .with_retry(object_store::RetryConfig {
                max_retries: 0,
                ..Default::default()
            })
            .build()
            .unwrap();
        let client = ObjectStoreClient::from_store(
            Arc::new(store),
            ObjectStoreUrl::parse("s3://bucket").unwrap(),
        );
        (client, server)
    }

    #[tokio::test]
    async fn s3_witness_cas_sends_exact_provider_conditions() {
        let conditions = Arc::new(Mutex::new(Vec::new()));
        let captured = conditions.clone();
        let app = axum::Router::new().route(
            "/bucket/key",
            axum::routing::get(|| async { ([("etag", "\"v1\"")], "original") }).put(
                move |headers: axum::http::HeaderMap| {
                    let captured = captured.clone();
                    async move {
                        let header = |name: &str| {
                            headers
                                .get(name)
                                .and_then(|v| v.to_str().ok())
                                .map(str::to_owned)
                        };
                        let matched = header("if-match");
                        captured
                            .lock()
                            .unwrap()
                            .push((matched.clone(), header("if-none-match")));
                        if matched.as_deref() == Some("\"v1\"") {
                            (axum::http::StatusCode::OK, [("etag", "\"v2\"")], "")
                        } else {
                            (
                                axum::http::StatusCode::PRECONDITION_FAILED,
                                [("content-type", "application/xml")],
                                "<Error><Code>PreconditionFailed</Code><Message>changed</Message></Error>",
                            )
                        }
                    }
                },
            ),
        );
        let (client, server) = mock_s3(app).await;
        let object = client.witness_get("key").await.unwrap().unwrap();
        assert_eq!(object.etag, "\"v1\"");
        assert_eq!(object.bytes, b"original");
        assert!(client
            .witness_cas("key", Some(&object.etag), b"next".to_vec())
            .await
            .unwrap());
        assert!(!client
            .witness_cas("key", Some("\"v0\""), b"stale".to_vec())
            .await
            .unwrap());
        assert!(!client
            .witness_cas("key", None, b"create".to_vec())
            .await
            .unwrap());
        assert_eq!(
            *conditions.lock().unwrap(),
            vec![
                (Some("\"v1\"".into()), None),
                (Some("\"v0\"".into()), None),
                (None, Some("*".into()))
            ]
        );
        server.abort();
    }

    #[tokio::test]
    async fn s3_batch_delete_sends_one_request_and_reports_partial_errors() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let app = axum::Router::new().route(
            "/bucket",
            axum::routing::post(move |body: String| {
                let captured = captured.clone();
                async move {
                    let mut requests = captured.lock().unwrap();
                    requests.push(body);
                    if requests.len() > 1 {
                        return ([("content-type", "application/xml")],
                            "<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Deleted><Key>key-0</Key></Deleted></DeleteResult>");
                    }
                    ([("content-type", "application/xml")],
                     "<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Error><Key>key-7</Key><Code>AccessDenied</Code><Message>denied</Message></Error></DeleteResult>")
                }
            }),
        );
        let (client, server) = mock_s3(app).await;
        client.delete_keys(&[]).await.unwrap();
        assert!(client.delete_keys(&vec!["key".into(); 1001]).await.is_err());
        assert!(requests.lock().unwrap().is_empty());
        let keys = (0..1000).map(|i| format!("key-{i}")).collect::<Vec<_>>();
        let error = client.delete_keys(&keys).await.unwrap_err();
        assert!(error.to_string().contains("AccessDenied"), "{error}");
        client.delete_keys(&keys).await.unwrap();
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        for body in requests.iter() {
            assert_eq!(body.matches("<Object>").count(), 1000);
        }
        server.abort();
    }
}
