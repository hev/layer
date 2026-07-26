use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::clients::turbopuffer::{
    TurbopufferClient, TurbopufferError, TurbopufferPassthroughResponse,
};

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
}
