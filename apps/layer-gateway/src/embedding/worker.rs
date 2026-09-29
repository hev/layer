//! OpenAI-compatible, InfraRules-declared embedding containers.
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::{json, Value};

use super::{
    EmbeddingBatch, EmbeddingError, EmbeddingModality, EmbeddingProvider, EmbeddingRequest,
};

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerEmbedder {
    pub name: String,
    pub model: String,
    pub dims: u64,
    pub max_input_tokens: u64,
    pub endpoint: String,
    pub image_digest: Option<String>,
    pub ready: bool,
}

/// Read on demand rather than retaining stale routing or image identity after
/// an InfraRules edit. Static entries are for standalone embedding tests.
#[derive(Default)]
pub struct WorkerEmbedders {
    pub entries: std::sync::RwLock<Vec<WorkerEmbedder>>,
    #[cfg(feature = "pro")]
    pub kube: Option<kube::Client>,
}

impl WorkerEmbedders {
    pub async fn resolve(&self, model: &str) -> Result<WorkerEmbedder, EmbeddingError> {
        #[cfg(feature = "pro")]
        if let Some(client) = &self.kube {
            use kube::{
                api::DynamicObject,
                core::{ApiResource, GroupVersionKind},
                Api,
            };
            let resource = ApiResource::from_gvk_with_plural(
                &GroupVersionKind::gvk("hevlayer.com", "v1alpha1", "InfraRules"),
                "infrarules",
            );
            let api: Api<DynamicObject> = Api::all_with(client.clone(), &resource);
            let rules = tokio::time::timeout(Duration::from_secs(5), api.get("default"))
                .await
                .map_err(|_| {
                    EmbeddingError::Unavailable("embedder configuration lookup timed out".into())
                })?
                .map_err(|e| {
                    EmbeddingError::Unavailable(format!("embedder configuration unavailable: {e}"))
                })?;
            return select(&entries_from_rules(&rules.data)?, model);
        }
        select(
            &self.entries.read().unwrap_or_else(|e| e.into_inner()),
            model,
        )
    }
}

fn select(entries: &[WorkerEmbedder], model: &str) -> Result<WorkerEmbedder, EmbeddingError> {
    let mut matches = entries.iter().filter(|entry| entry.model == model);
    let selected = matches.next().ok_or_else(|| {
        EmbeddingError::Validation(format!("no InfraRules embedder declares model `{model}`"))
    })?;
    if matches.next().is_some() {
        return Err(EmbeddingError::Validation(format!(
            "multiple InfraRules embedders declare model `{model}`"
        )));
    }
    Ok(selected.clone())
}

#[cfg(any(feature = "pro", test))]
fn entries_from_rules(rules: &Value) -> Result<Vec<WorkerEmbedder>, EmbeddingError> {
    let mut entries = Vec::new();
    for spec in rules
        .pointer("/spec/embedders")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let status = rules
            .pointer("/status/embedders")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .find(|status| {
                status["name"] == spec["name"]
                    && status["model"] == spec["model"]
                    && status["image"] == spec["image"]
            });
        let mut entry = spec.clone();
        entry["endpoint"] = status.map(|s| s["endpoint"].clone()).unwrap_or(json!(""));
        entry["imageDigest"] = status
            .map(|s| s["imageDigest"].clone())
            .unwrap_or(Value::Null);
        entry["ready"] =
            json!(status
                .and_then(|s| s["conditions"].as_array())
                .is_some_and(|conditions| conditions
                    .iter()
                    .any(|c| c["type"] == "Ready" && c["status"] == "True")));
        let entry: WorkerEmbedder = serde_json::from_value(entry).map_err(|_| {
            EmbeddingError::Unavailable("invalid InfraRules embedder configuration".into())
        })?;
        if entry.dims == 0 || entry.max_input_tokens == 0 {
            return Err(EmbeddingError::Unavailable(
                "embedder dimensions and token limit must be positive".into(),
            ));
        }
        entries.push(entry);
    }
    Ok(entries)
}

impl WorkerEmbedder {
    pub fn validate_inputs(&self, texts: &[String]) -> Result<(), EmbeddingError> {
        // OpenAI's protocol has no tokenizer endpoint. Use a conservative
        // UTF-8 byte budget with two special tokens, never a chars/4 estimate.
        // The container must also reject, never truncate, overlength inputs.
        if texts
            .iter()
            .any(|text| text.len().saturating_add(2) as u64 > self.max_input_tokens)
        {
            return Err(EmbeddingError::Validation(format!("prepared input exceeds embedder maxInputTokens {} (conservative UTF-8 byte budget including two special tokens)", self.max_input_tokens)));
        }
        Ok(())
    }

    pub fn require_ready(&self) -> Result<(), EmbeddingError> {
        if !self.ready {
            return Err(EmbeddingError::Unavailable(format!(
                "embedder `{}` is waking or unavailable",
                self.name
            )));
        }
        Ok(())
    }

    pub fn check(&self, request: &EmbeddingRequest<'_>) -> Result<(), EmbeddingError> {
        if request.modality != EmbeddingModality::Text || request.revision.is_some() {
            return Err(EmbeddingError::Validation(
                "worker embeddings support text; pin model revisions in the declared image".into(),
            ));
        }
        if request.model != self.model || request.dims.is_some_and(|dims| dims != self.dims) {
            return Err(EmbeddingError::Validation(
                "worker model or dimensions differ from the embedding profile".into(),
            ));
        }
        let digest = self
            .image_digest
            .as_deref()
            .filter(|digest| {
                digest.strip_prefix("sha256:").is_some_and(|hash| {
                    hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit())
                })
            })
            .ok_or_else(|| {
                EmbeddingError::Unavailable(format!(
                    "embedder `{}` is waiting for an image digest",
                    self.name
                ))
            })?;
        if request.artifact.is_some_and(|pinned| pinned != digest) {
            return Err(EmbeddingError::Validation(
                "worker image digest changed; re-index into a fresh namespace".into(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl EmbeddingProvider for WorkerEmbedder {
    async fn embed(
        &self,
        request: &EmbeddingRequest<'_>,
        texts: &[String],
    ) -> Result<EmbeddingBatch, EmbeddingError> {
        self.check(request)?;
        self.validate_inputs(texts)?;
        self.require_ready()?;
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(1))
            .timeout(Duration::from_secs(60))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| EmbeddingError::Upstream(e.to_string()))?;
        let start = Instant::now();
        let mut vectors = Vec::with_capacity(texts.len());
        let mut tokens = 0u64;
        // TEI's default maximum client batch is 32. Validate every microbatch
        // before returning any vectors to the route's cache transaction.
        for texts in texts.chunks(32) {
            let response = client
                .post(format!(
                    "{}/v1/embeddings",
                    self.endpoint.trim_end_matches('/')
                ))
                .json(&json!({"model":request.model,"input":texts,"encoding_format":"float"}))
                .send()
                .await
                .map_err(|e| {
                    if e.is_connect() {
                        EmbeddingError::Unavailable(format!(
                            "embedder `{}` has no reachable replica",
                            self.name
                        ))
                    } else if e.is_timeout() {
                        EmbeddingError::Timeout("worker embedding timed out".into())
                    } else {
                        EmbeddingError::Upstream("worker embedding request failed".into())
                    }
                })?;
            let status = response.status();
            if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
                return Err(EmbeddingError::Unavailable(format!(
                    "embedder `{}` is unavailable",
                    self.name
                )));
            }
            if !status.is_success() {
                return Err(EmbeddingError::Upstream(format!(
                    "worker returned HTTP {status}"
                )));
            }
            let mut bytes = Vec::new();
            let mut stream = response.bytes_stream();
            while let Some(part) = stream.next().await {
                let part = part
                    .map_err(|_| EmbeddingError::Upstream("worker response read failed".into()))?;
                if bytes.len().saturating_add(part.len()) > 32 * 1024 * 1024 {
                    return Err(EmbeddingError::Upstream(
                        "worker response exceeds 32 MiB".into(),
                    ));
                }
                bytes.extend_from_slice(&part);
            }
            let response: Value = serde_json::from_slice(&bytes)
                .map_err(|_| EmbeddingError::Upstream("worker returned invalid JSON".into()))?;
            vectors.extend(validate_response(
                &response,
                texts.len(),
                self.dims,
                &self.model,
            )?);
            tokens = tokens.saturating_add(
                response
                    .pointer("/usage/total_tokens")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
            );
        }
        Ok(EmbeddingBatch {
            vectors,
            performance: json!({"embedding_tokens": tokens, "embedding_ms":start.elapsed().as_secs_f64()*1000.0}),
            billing: None,
        })
    }
}

fn validate_response(
    response: &Value,
    count: usize,
    dims: u64,
    model: &str,
) -> Result<Vec<Vec<f64>>, EmbeddingError> {
    let bad = || {
        EmbeddingError::Upstream("worker returned invalid embedding count, index, dimensions, finite values or unit norm".into())
    };
    if response
        .get("model")
        .is_some_and(|value| value.as_str() != Some(model))
    {
        return Err(bad());
    }
    let data = response["data"]
        .as_array()
        .filter(|data| data.len() == count)
        .ok_or_else(bad)?;
    let mut vectors = vec![None; count];
    for item in data {
        let index = item["index"]
            .as_u64()
            .filter(|index| *index < count as u64)
            .ok_or_else(bad)? as usize;
        if vectors[index].is_some() {
            return Err(bad());
        }
        let values = item["embedding"]
            .as_array()
            .filter(|v| v.len() as u64 == dims)
            .ok_or_else(bad)?;
        let vector: Vec<f64> = values
            .iter()
            .map(|v| {
                v.as_f64()
                    .filter(|v| v.is_finite() && (*v as f32).is_finite())
                    .ok_or_else(bad)
            })
            .collect::<Result<_, _>>()?;
        let norm = vector.iter().map(|v| v * v).sum::<f64>().sqrt();
        if !norm.is_finite() || (norm - 1.0).abs() > 1e-4 {
            return Err(bad());
        }
        vectors[index] = Some(vector);
    }
    vectors.into_iter().map(|v| v.ok_or_else(bad)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "pro")]
    #[tokio::test]
    async fn discovers_cluster_scoped_infrarules_at_the_declared_plural() {
        let service = tower::service_fn(
            |request: axum::http::Request<kube::client::Body>| async move {
                assert_eq!(
                    request.uri().path(),
                    "/apis/hevlayer.com/v1alpha1/infrarules/default"
                );
                Ok::<_, std::convert::Infallible>(axum::http::Response::new(axum::body::Body::from(
                json!({"apiVersion":"hevlayer.com/v1alpha1","kind":"InfraRules","metadata":{"name":"default"},"spec":{"embedders":[{"name":"bge","model":"BAAI/bge-m3","image":"model:v1","dims":1024,"maxInputTokens":8192}]}}).to_string()
            )))
            },
        );
        let registry = WorkerEmbedders {
            kube: Some(kube::Client::new(service, "layer")),
            ..Default::default()
        };
        let worker = registry.resolve("BAAI/bge-m3").await.unwrap();
        assert_eq!(worker.dims, 1024);
        assert!(!worker.ready);
    }

    #[test]
    fn discovery_matches_current_image_and_rejects_duplicate_models() {
        let mut rules = json!({"spec":{"embedders":[{"name":"bge","model":"BAAI/bge-m3","image":"registry/model:v2","dims":1024,"maxInputTokens":8192}]},"status":{"embedders":[{"name":"bge","model":"BAAI/bge-m3","image":"registry/model:v1","imageDigest":format!("sha256:{}","a".repeat(64)),"endpoint":"http://worker:8080","conditions":[{"type":"Ready","status":"True"}]}]}});
        let entries = entries_from_rules(&rules).unwrap();
        assert!(!entries[0].ready);
        assert!(entries[0].image_digest.is_none());
        rules["status"]["embedders"][0]["image"] = json!("registry/model:v2");
        let mut entries = entries_from_rules(&rules).unwrap();
        assert!(entries[0].ready);
        assert!(select(&entries, "unknown").is_err());
        entries.push(entries[0].clone());
        assert!(select(&entries, "BAAI/bge-m3").is_err());
    }

    #[test]
    fn response_indices_must_be_a_permutation_and_model_must_match() {
        let good = json!({"model":"bge","data":[{"index":1,"embedding":[0.0,1.0]},{"index":0,"embedding":[1.0,0.0]}]});
        assert_eq!(
            validate_response(&good, 2, 2, "bge").unwrap(),
            vec![vec![1.0, 0.0], vec![0.0, 1.0]]
        );
        let mut duplicate = good.clone();
        duplicate["data"][0]["index"] = json!(0);
        assert!(validate_response(&duplicate, 2, 2, "bge").is_err());
        assert!(validate_response(&good, 2, 2, "other").is_err());
        let huge = json!({"data":[{"index":0,"embedding":[1e100,0.0]}]});
        assert!(validate_response(&huge, 1, 2, "bge").is_err());
    }
}
