use crate::{
    protocol::{
        Error, Modality, Output, Purpose, Request, Timing, Usage, MAX_INPUT, MAX_ITEMS,
        MAX_TIMEOUT, MAX_TOKENS,
    },
    registry::ModelRecord,
};
use candle_core::{DType, Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::bert::{BertModel, Config};
use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Instant,
};
use tokenizers::{Encoding, Tokenizer};

pub struct Model {
    pub record: ModelRecord,
    pub fingerprint: String,
    pub tokenizer: Tokenizer,
    bert: BertModel,
    pad_id: u32,
}
impl Model {
    pub fn load(
        record: ModelRecord,
        fingerprint: String,
        config: &[u8],
        tokenizer: &[u8],
        weights: Vec<u8>,
    ) -> anyhow::Result<Self> {
        let raw: serde_json::Value = serde_json::from_slice(config)?;
        anyhow::ensure!(
            raw.get("is_decoder").and_then(|v| v.as_bool()) != Some(true)
                && raw.get("add_cross_attention").and_then(|v| v.as_bool()) != Some(true),
            "decoder BERT is unsupported"
        );
        let header = safetensors::SafeTensors::deserialize(&weights)?;
        anyhow::ensure!(
            header
                .tensors()
                .iter()
                .all(|(name, tensor)| tensor.dtype() == safetensors::Dtype::F32
                    || (name.ends_with("position_ids")
                        && tensor.dtype() == safetensors::Dtype::I64)),
            "weights must be f32"
        );
        let config: Config = serde_json::from_slice(config)?;
        anyhow::ensure!(
            config.model_type.as_deref() == Some("bert")
                && config.hidden_size == record.dimensions
                && config.max_position_embeddings >= record.max_tokens
                && config.num_attention_heads > 0
                && config
                    .hidden_size
                    .is_multiple_of(config.num_attention_heads),
            "incompatible BERT config"
        );
        anyhow::ensure!(
            config.pad_token_id < config.vocab_size && config.pad_token_id <= u32::MAX as usize,
            "invalid padding token"
        );
        let mut tokenizer =
            Tokenizer::from_bytes(tokenizer).map_err(|_| anyhow::anyhow!("invalid tokenizer"))?;
        tokenizer
            .with_truncation(None)
            .map_err(|_| anyhow::anyhow!("invalid truncation config"))?;
        tokenizer.with_padding(None);
        anyhow::ensure!(
            tokenizer
                .get_vocab(true)
                .values()
                .all(|&id| (id as usize) < config.vocab_size),
            "tokenizer exceeds vocabulary"
        );
        let vb = VarBuilder::from_buffered_safetensors(weights, DType::F32, &Device::Cpu)?;
        let bert = BertModel::load(vb, &config)?;
        Ok(Self {
            record,
            fingerprint,
            tokenizer,
            bert,
            pad_id: config.pad_token_id as u32,
        })
    }
    pub fn validate(&self, req: &Request) -> Result<Vec<Encoding>, Error> {
        if req.timeout_ms == 0 || req.timeout_ms > MAX_TIMEOUT || req.inputs.is_empty() {
            return Err(Error::new("invalid_request"));
        }
        if req.inputs.len() > MAX_ITEMS {
            return Err(Error::new("batch_too_large"));
        }
        if req.artifact_sha256 != self.fingerprint {
            return Err(Error::new("artifact_mismatch"));
        }
        if req.dimensions != self.record.dimensions {
            return Err(Error::new("dimension_mismatch"));
        }
        if req.modality != Modality::Text {
            return Err(Error::new("unsupported_modality"));
        }
        let prefix = match req.purpose {
            Purpose::Document => &self.record.prefixes.document,
            Purpose::Query => &self.record.prefixes.query,
        };
        let mut encoded = Vec::with_capacity(req.inputs.len());
        let mut total = 0;
        for (index, input) in req.inputs.iter().enumerate() {
            if input.len() > MAX_INPUT {
                return Err(Error::new("input_too_long").at(index));
            }
            let content = input
                .strip_prefix(prefix)
                .ok_or_else(|| Error::new("invalid_input").at(index))?;
            if content.trim().is_empty() {
                return Err(Error::new("invalid_input").at(index));
            }
            let encoding = self
                .tokenizer
                .encode(input.as_str(), true)
                .map_err(|_| Error::new("invalid_input").at(index))?;
            if encoding.len() > self.record.max_tokens {
                return Err(Error::new("input_too_long").at(index));
            }
            total += encoding.len();
            if total > MAX_TOKENS {
                return Err(Error::new("batch_token_limit"));
            }
            encoded.push(encoding);
        }
        Ok(encoded)
    }
    pub fn infer(
        &self,
        req: &Request,
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> Result<Output, Error> {
        let encodings = self.validate(req)?;
        let start = Instant::now();
        let mut vectors = Vec::with_capacity(encodings.len());
        // Small microbatches bound attention scratch memory. Never sort or deduplicate:
        // concatenation preserves positions, including repeated inputs.
        for batch in encodings.chunks(4) {
            if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
                return Err(Error::new("deadline_exceeded"));
            }
            let width = batch.iter().map(Encoding::len).max().unwrap_or(0);
            let mut ids = vec![self.pad_id; batch.len() * width];
            let mut types = vec![0u32; ids.len()];
            let mut masks = vec![0u32; ids.len()];
            for (row, encoding) in batch.iter().enumerate() {
                let range = row * width..row * width + encoding.len();
                ids[range.clone()].copy_from_slice(encoding.get_ids());
                types[range.clone()].copy_from_slice(encoding.get_type_ids());
                masks[range].copy_from_slice(encoding.get_attention_mask());
            }
            let forward = || -> candle_core::Result<Vec<Vec<f32>>> {
                let ids = Tensor::from_vec(ids, (batch.len(), width), &Device::Cpu)?;
                let types = Tensor::from_vec(types, (batch.len(), width), &Device::Cpu)?;
                let mask = Tensor::from_vec(masks, (batch.len(), width), &Device::Cpu)?;
                let hidden = self.bert.forward(&ids, &types, Some(&mask))?;
                pool(&hidden, &mask, &self.record.pooling)?.to_vec2::<f32>()
            };
            for mut vector in forward().map_err(|_| Error::new("inference_failed"))? {
                normalize(&mut vector)?;
                if vector.len() != self.record.dimensions {
                    return Err(Error::new("invalid_output"));
                }
                vectors.push(vector);
            }
        }
        if cancelled.load(Ordering::Acquire) || Instant::now() >= deadline {
            return Err(Error::new("deadline_exceeded"));
        }
        Ok(Output {
            model: req.model.clone(),
            artifact_sha256: self.fingerprint.clone(),
            dimensions: self.record.dimensions,
            vectors,
            usage: Usage {
                input_tokens: encodings.iter().map(Encoding::len).sum(),
            },
            timing: Timing {
                inference_ms: start.elapsed().as_secs_f64() * 1000.,
            },
        })
    }
}
pub fn pool(hidden: &Tensor, mask: &Tensor, mode: &str) -> candle_core::Result<Tensor> {
    match mode {
        "cls" => hidden.narrow(1, 0, 1)?.squeeze(1),
        "mean_masked" => {
            let mask = mask.to_dtype(DType::F32)?.unsqueeze(2)?;
            hidden
                .broadcast_mul(&mask)?
                .sum(1)?
                .broadcast_div(&mask.sum(1)?)
        }
        _ => candle_core::bail!("unsupported pooling"),
    }
}
pub fn normalize(vector: &mut [f32]) -> Result<(), Error> {
    let norm = vector.iter().map(|v| v * v).sum::<f32>().sqrt();
    if !norm.is_finite() || norm == 0. {
        return Err(Error::new("invalid_output"));
    }
    for value in vector {
        *value /= norm;
    }
    Ok(())
}
