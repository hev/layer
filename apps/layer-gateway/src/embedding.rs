use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::clients::turbopuffer::{
    TurbopufferClient, TurbopufferError, TurbopufferPassthroughResponse,
};

mod local_clip;
pub(crate) use local_clip::is_clip_model;
pub use local_clip::LocalClipEmbeddingProvider;

pub type EmbeddingCache = dashmap::DashMap<String, (std::time::Instant, Arc<Vec<f64>>)>;

#[derive(Debug, Clone)]
pub struct EmbeddingBatch {
    pub vectors: Vec<Vec<f64>>,
    pub performance: Value,
    pub billing: Option<Value>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingModality {
    #[default]
    Text,
    Image,
}

#[derive(Debug, Clone)]
pub struct EmbeddingRequest<'a> {
    pub model: &'a str,
    pub dims: Option<u64>,
    pub revision: Option<&'a str>,
    pub modality: EmbeddingModality,
}

impl EmbeddingRequest<'_> {
    pub fn provider_model(&self) -> String {
        match self.revision {
            Some(revision) => format!("{}@{revision}", self.model),
            None => self.model.to_string(),
        }
    }
}

#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    async fn embed(
        &self,
        request: &EmbeddingRequest<'_>,
        texts: &[String],
    ) -> Result<EmbeddingBatch, TurbopufferError>;

    async fn embed_images(
        &self,
        _request: &EmbeddingRequest<'_>,
        _images: &[Vec<u8>],
    ) -> Result<EmbeddingBatch, TurbopufferError> {
        Err(TurbopufferError::Other(
            "embedding provider does not support image inputs".to_string(),
        ))
    }
}

/// Production embedding provider backed by Turbopuffer native embeddings.
///
/// Turbopuffer exposes native embedding through namespace writes rather than
/// a standalone vector endpoint. A content-addressed provider namespace per
/// model/dimensionality lets the gateway submit a batch through that native
/// wire and strongly read the computed vectors back. The caller then writes
/// only concrete vectors to its actual VectorStore.
pub struct TurbopufferEmbeddingProvider {
    client: Arc<dyn TurbopufferClient>,
}

/// CPU-only embedding provider backed by Erik Kaum's Lattice runtime.
///
/// The deployment artifact is generated separately from the canonical
/// `erikkaum/lattice-retrieval` checkpoint. Lattice's quantization applies to
/// this lookup table only; the provider returns normalized floating-point
/// vectors for storage by the active VectorStore.
pub struct LatticeEmbeddingProvider {
    model: Arc<lattice::Model>,
    tokenizer: Arc<lattice::LatticeTokenizer>,
}

impl LatticeEmbeddingProvider {
    pub const MODEL: &'static str = "erikkaum/lattice-retrieval";

    pub fn load(model_path: &Path) -> Result<Self, TurbopufferError> {
        let tokenizer_path = model_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("tokenizer.json");
        let model = lattice::Model::load(model_path).map_err(|error| {
            TurbopufferError::Other(format!(
                "failed to load Lattice model {}: {error}",
                model_path.display()
            ))
        })?;
        let tokenizer = lattice::LatticeTokenizer::load(&tokenizer_path).map_err(|error| {
            TurbopufferError::Other(format!(
                "failed to load Lattice tokenizer {}: {error}",
                tokenizer_path.display()
            ))
        })?;
        Ok(Self {
            model: Arc::new(model),
            tokenizer: Arc::new(tokenizer),
        })
    }
}

#[async_trait]
impl EmbeddingProvider for LatticeEmbeddingProvider {
    async fn embed(
        &self,
        request: &EmbeddingRequest<'_>,
        texts: &[String],
    ) -> Result<EmbeddingBatch, TurbopufferError> {
        if request.model != Self::MODEL {
            return Err(TurbopufferError::Other(format!(
                "Lattice provider supports only model `{}` (got `{}`)",
                Self::MODEL,
                request.model
            )));
        }
        if request.revision.is_some() {
            return Err(TurbopufferError::Other(
                "Lattice provider does not support model revisions".to_string(),
            ));
        }
        if request.modality != EmbeddingModality::Text {
            return Err(TurbopufferError::Other(
                "Lattice provider supports only text embeddings".to_string(),
            ));
        }
        let model_dim = self.model.dim() as u64;
        if request.dims.is_some_and(|dims| dims != model_dim) {
            return Err(TurbopufferError::Other(format!(
                "Lattice artifact has {model_dim} dimensions, but {} were requested",
                request.dims.expect("checked Some")
            )));
        }
        if texts.is_empty() {
            return Ok(EmbeddingBatch {
                vectors: Vec::new(),
                performance: json!({}),
                billing: None,
            });
        }

        let mut unique = Vec::<String>::new();
        let mut positions = HashMap::<String, usize>::new();
        let mut requested = Vec::with_capacity(texts.len());
        for text in texts {
            let id = content_id(text);
            let position = if let Some(position) = positions.get(&id) {
                *position
            } else {
                let position = unique.len();
                positions.insert(id, position);
                unique.push(text.clone());
                position
            };
            requested.push(position);
        }

        let model = Arc::clone(&self.model);
        let tokenizer = Arc::clone(&self.tokenizer);
        let started = Instant::now();
        let (unique_vectors, token_count) = tokio::task::spawn_blocking(move || {
            let token_ids = tokenizer.encode_batch(unique).map_err(|error| {
                TurbopufferError::Other(format!("Lattice tokenization failed: {error}"))
            })?;
            let token_count = token_ids.iter().map(Vec::len).sum::<usize>();
            let mut scratch = model.scratch();
            let mut vectors = Vec::with_capacity(token_ids.len());
            for ids in token_ids {
                let mut vector = vec![0.0_f32; model.dim()];
                model
                    .embed(&ids, &mut vector, &mut scratch)
                    .map_err(|error| {
                        TurbopufferError::Other(format!("Lattice embedding failed: {error}"))
                    })?;
                lattice::kernel::l2_normalize(&mut vector);
                vectors.push(vector.into_iter().map(f64::from).collect::<Vec<_>>());
            }
            Ok::<_, TurbopufferError>((vectors, token_count))
        })
        .await
        .map_err(|error| TurbopufferError::Other(format!("Lattice worker failed: {error}")))??;
        let vectors = requested
            .into_iter()
            .map(|position| unique_vectors[position].clone())
            .collect();

        Ok(EmbeddingBatch {
            vectors,
            performance: json!({
                "embedding_tokens": token_count,
                "embedding_ms": started.elapsed().as_secs_f64() * 1000.0,
            }),
            billing: None,
        })
    }
}

impl TurbopufferEmbeddingProvider {
    pub fn new(client: Arc<dyn TurbopufferClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl EmbeddingProvider for TurbopufferEmbeddingProvider {
    async fn embed(
        &self,
        request: &EmbeddingRequest<'_>,
        texts: &[String],
    ) -> Result<EmbeddingBatch, TurbopufferError> {
        if texts.is_empty() {
            return Ok(EmbeddingBatch {
                vectors: Vec::new(),
                performance: json!({}),
                billing: None,
            });
        }

        let namespace = provider_namespace(request);
        let mut unique = Vec::<(String, String)>::new();
        let mut positions = HashMap::<String, usize>::new();
        let mut requested = Vec::with_capacity(texts.len());
        for text in texts {
            let id = content_id(text);
            let position = if let Some(position) = positions.get(&id) {
                *position
            } else {
                let position = unique.len();
                positions.insert(id.clone(), position);
                unique.push((id, text.clone()));
                position
            };
            requested.push(position);
        }

        let mut embed = json!({
            "model": request.provider_model(),
            "attribute": "vector"
        });
        if let Some(dims) = request.dims {
            embed["dims"] = Value::from(dims);
        }
        let write = json!({
            "upsert_rows": unique.iter().map(|(id, text)| json!({
                "id": id,
                "input": text,
            })).collect::<Vec<_>>(),
            "distance_metric": "cosine_distance",
            "schema": {
                "input": {
                    "type": "string",
                    "filterable": false,
                    "embed": embed,
                }
            }
        });
        let write_response = self
            .client
            .passthrough(
                "POST",
                &format!("/v2/namespaces/{namespace}"),
                None,
                Some(write),
            )
            .await?;
        let write_body = successful_json(write_response, "embedding provider write")?;

        let ids = unique.iter().map(|(id, _)| id).collect::<Vec<_>>();
        let query = json!({
            "rank_by": ["id", "asc"],
            "top_k": ids.len(),
            "filters": ["id", "In", ids],
            "include_attributes": ["id", "vector"],
            "consistency": {"level": "strong"},
        });
        let query_response = self
            .client
            .passthrough(
                "POST",
                &format!("/v2/namespaces/{namespace}/query"),
                None,
                Some(query),
            )
            .await?;
        let query_body = successful_json(query_response, "embedding provider read")?;

        let mut by_id = query_body
            .get("rows")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|row| {
                let id = row.get("id")?.as_str()?.to_string();
                let vector = row
                    .get("vector")?
                    .as_array()?
                    .iter()
                    .map(Value::as_f64)
                    .collect::<Option<Vec<_>>>()?;
                Some((id, vector))
            })
            .collect::<HashMap<_, _>>();
        let unique_vectors = unique
            .iter()
            .map(|(id, _)| {
                by_id.remove(id).ok_or_else(|| {
                    TurbopufferError::Other(format!(
                        "embedding provider did not return vector for content id {id}"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let vectors = requested
            .into_iter()
            .map(|position| unique_vectors[position].clone())
            .collect();

        Ok(EmbeddingBatch {
            vectors,
            performance: write_body
                .get("performance")
                .cloned()
                .unwrap_or_else(|| json!({})),
            billing: merge_billing(write_body.get("billing"), query_body.get("billing")),
        })
    }
}

fn successful_json(
    response: TurbopufferPassthroughResponse,
    operation: &str,
) -> Result<Value, TurbopufferError> {
    if !(200..300).contains(&response.status) {
        return Err(TurbopufferError::Response(response));
    }
    serde_json::from_slice(&response.body).map_err(|error| {
        TurbopufferError::Other(format!("{operation} returned invalid JSON: {error}"))
    })
}

fn provider_namespace(request: &EmbeddingRequest<'_>) -> String {
    let mut hash = Sha256::new();
    hash.update(request.model.as_bytes());
    hash.update([0]);
    hash.update(request.dims.unwrap_or_default().to_le_bytes());
    if request.revision.is_some() || request.modality != EmbeddingModality::Text {
        hash.update([0]);
        hash.update(request.revision.unwrap_or_default().as_bytes());
        hash.update([0]);
        hash.update(match request.modality {
            EmbeddingModality::Text => b"text".as_slice(),
            EmbeddingModality::Image => b"image".as_slice(),
        });
    }
    format!("_hevlayer-embed-{:x}", hash.finalize())
        .chars()
        .take(49)
        .collect()
}

fn content_id(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

fn merge_billing(left: Option<&Value>, right: Option<&Value>) -> Option<Value> {
    let mut merged = serde_json::Map::new();
    for billing in [left, right].into_iter().flatten() {
        let Some(billing) = billing.as_object() else {
            continue;
        };
        for (key, value) in billing {
            if let Some(value) = value.as_u64() {
                let current = merged.get(key).and_then(Value::as_u64).unwrap_or(0);
                merged.insert(key.clone(), Value::from(current.saturating_add(value)));
            } else {
                merged.entry(key.clone()).or_insert_with(|| value.clone());
            }
        }
    }
    (!merged.is_empty()).then_some(Value::Object(merged))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_lattice_fixture(directory: &Path) {
        use safetensors::tensor::{serialize_to_file, Dtype, TensorView};
        use tokenizers::models::wordpiece::WordPiece;
        use tokenizers::normalizers::bert::BertNormalizer;
        use tokenizers::pre_tokenizers::whitespace::Whitespace;
        use tokenizers::Tokenizer;

        std::fs::create_dir_all(directory).unwrap();
        let weights = [
            [0.0_f32, 0.0, 0.0, 0.0],
            [1.0_f32, 0.0, 0.0, 0.0],
            [0.0_f32, 1.0, 0.0, 0.0],
            [0.0_f32, 0.0, 1.0, 0.0],
        ];
        let weight_bytes = weights
            .into_iter()
            .flatten()
            .flat_map(f32::to_le_bytes)
            .collect::<Vec<_>>();
        let weight = TensorView::new(Dtype::F32, vec![4, 4], &weight_bytes).unwrap();
        let metadata = HashMap::from([
            ("lattice_variant".to_string(), "fp32".to_string()),
            ("bits".to_string(), "32".to_string()),
            ("axis".to_string(), "none".to_string()),
            ("dim".to_string(), "4".to_string()),
            ("vocab_size".to_string(), "4".to_string()),
        ]);
        serialize_to_file(
            [("weight", weight)],
            Some(metadata),
            &directory.join("model.safetensors"),
        )
        .unwrap();

        let vocab = [
            ("[UNK]".to_string(), 0),
            ("hello".to_string(), 1),
            ("world".to_string(), 2),
            ("static".to_string(), 3),
        ];
        let wordpiece = WordPiece::builder().vocab(vocab).build().unwrap();
        let mut tokenizer = Tokenizer::new(wordpiece);
        tokenizer
            .with_normalizer(Some(BertNormalizer::default()))
            .unwrap();
        tokenizer.with_pre_tokenizer(Some(Whitespace));
        tokenizer
            .save(directory.join("tokenizer.json"), false)
            .unwrap();
    }

    #[test]
    fn provider_namespaces_are_stable_and_profile_specific() {
        let request = |dims, revision, modality| EmbeddingRequest {
            model: "baai/bge-m3",
            dims,
            revision,
            modality,
        };
        let base = request(None, None, EmbeddingModality::Text);
        let full = provider_namespace(&base);
        assert_eq!(full, provider_namespace(&base));
        assert_ne!(
            full,
            provider_namespace(&request(Some(512), None, EmbeddingModality::Text))
        );
        assert_ne!(
            full,
            provider_namespace(&request(None, Some("abc"), EmbeddingModality::Text))
        );
        assert_ne!(
            full,
            provider_namespace(&request(None, None, EmbeddingModality::Image))
        );
        assert!(full.len() <= 49);
    }

    #[test]
    fn billing_fields_are_summed() {
        assert_eq!(
            merge_billing(
                Some(&json!({"billable_logical_bytes_written": 10})),
                Some(
                    &json!({"billable_logical_bytes_written": 2, "billable_logical_bytes_queried": 3})
                )
            ),
            Some(json!({
                "billable_logical_bytes_written": 12,
                "billable_logical_bytes_queried": 3,
            }))
        );
    }

    #[tokio::test]
    async fn lattice_embeds_real_text_normalizes_and_deduplicates() {
        let directory = std::env::temp_dir().join(format!(
            "hevlayer-lattice-provider-test-{}",
            uuid::Uuid::new_v4()
        ));
        write_lattice_fixture(&directory);
        let provider = LatticeEmbeddingProvider::load(&directory.join("model.safetensors"))
            .expect("load fixture");
        let batch = provider
            .embed(
                &EmbeddingRequest {
                    model: LatticeEmbeddingProvider::MODEL,
                    dims: Some(4),
                    revision: None,
                    modality: EmbeddingModality::Text,
                },
                &["hello world".to_string(), "hello world".to_string()],
            )
            .await
            .expect("embed fixture text");
        std::fs::remove_dir_all(&directory).unwrap();

        assert_eq!(batch.vectors.len(), 2);
        assert_eq!(batch.vectors[0], batch.vectors[1]);
        let expected = 1.0_f64 / 2.0_f64.sqrt();
        assert!((batch.vectors[0][0] - expected).abs() < 1e-6);
        assert!((batch.vectors[0][1] - expected).abs() < 1e-6);
        assert_eq!(&batch.vectors[0][2..], &[0.0, 0.0]);
        assert_eq!(batch.performance["embedding_tokens"], 2);
        assert!(batch.billing.is_none());
    }
}
