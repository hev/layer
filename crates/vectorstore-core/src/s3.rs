use async_trait::async_trait;

#[derive(Debug, thiserror::Error)]
#[error("S3 error: {0}")]
pub struct S3Error(pub String);

#[async_trait]
pub trait S3Client: Send + Sync {
    /// Put an object, replacing any existing object at the same key.
    async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), S3Error>;

    /// Put an object with conditional write (If-None-Match: *).
    /// Returns Ok(true) if written, Ok(false) if object already existed.
    async fn put_if_not_exists(&self, key: &str, body: Vec<u8>) -> Result<bool, S3Error>;

    /// Get an object by key.
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, S3Error>;

    /// List objects with a given prefix, sorted by key.
    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, S3Error>;

    /// Delete an object by key. Missing keys are treated as success.
    async fn delete_key(&self, key: &str) -> Result<(), S3Error>;
}

// --- Real implementation using aws-sdk-s3 ---

pub struct AwsS3Client {
    client: aws_sdk_s3::Client,
    bucket: String,
}

impl AwsS3Client {
    pub async fn new(bucket: &str, region: &str, endpoint: Option<&str>) -> Self {
        let mut config_builder = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region.to_string()));

        if let Some(ep) = endpoint {
            config_builder = config_builder.endpoint_url(ep);
        }

        let config = config_builder.load().await;

        let s3_config = aws_sdk_s3::config::Builder::from(&config)
            .force_path_style(endpoint.is_some()) // MinIO needs path-style
            .build();

        let client = aws_sdk_s3::Client::from_conf(s3_config);

        Self {
            client,
            bucket: bucket.to_string(),
        }
    }
}

#[async_trait]
impl S3Client for AwsS3Client {
    async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), S3Error> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body.into())
            .send()
            .await
            .map_err(|e| S3Error(format!("{}", e.into_service_error())))?;
        Ok(())
    }

    async fn put_if_not_exists(&self, key: &str, body: Vec<u8>) -> Result<bool, S3Error> {
        let result = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body.into())
            .if_none_match("*")
            .send()
            .await;

        match result {
            Ok(_) => Ok(true),
            Err(err) => {
                // PreconditionFailed / 412 means object already exists
                let service_err = err.into_service_error();
                let meta = service_err.meta();
                if meta.code() == Some("PreconditionFailed") {
                    Ok(false)
                } else {
                    Err(S3Error(format!("{}", service_err)))
                }
            }
        }
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, S3Error> {
        let result = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await;

        match result {
            Ok(output) => {
                let bytes = output
                    .body
                    .collect()
                    .await
                    .map_err(|e| S3Error(e.to_string()))?
                    .into_bytes()
                    .to_vec();
                Ok(Some(bytes))
            }
            Err(err) => {
                let service_err = err.into_service_error();
                if service_err.is_no_such_key() {
                    Ok(None)
                } else {
                    Err(S3Error(format!("{}", service_err)))
                }
            }
        }
    }

    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, S3Error> {
        let mut keys = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);

            if let Some(token) = &continuation_token {
                req = req.continuation_token(token);
            }

            let resp = req
                .send()
                .await
                .map_err(|e| S3Error(format!("{}", e.into_service_error())))?;

            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    keys.push(key.to_string());
                }
            }

            if resp.is_truncated() == Some(true) {
                continuation_token = resp.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }

        keys.sort();
        Ok(keys)
    }

    async fn delete_key(&self, key: &str) -> Result<(), S3Error> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| S3Error(format!("{}", e.into_service_error())))?;
        Ok(())
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
