use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use candle::{DType, Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::clip::{
    div_l2_norm,
    text_model::{Activation, ClipTextConfig},
    vision_model::ClipVisionConfig,
    ClipConfig, ClipModel,
};
use image::imageops::FilterType;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokenizers::{PaddingParams, PaddingStrategy, Tokenizer, TruncationParams};
use tokio::sync::Semaphore;

use super::{EmbeddingBatch, EmbeddingModality, EmbeddingProvider, EmbeddingRequest};
use crate::clients::turbopuffer::TurbopufferError;

const CLIP_CONTEXT_LENGTH: usize = 77;
const DEFAULT_COMPUTE_CONCURRENCY: usize = 2;

/// In-process, CPU-only CLIP provider backed by Candle.
///
/// The configured directory is a Hugging Face CLIP artifact containing
/// `model.safetensors`, `config.json`, `tokenizer.json`, and
/// `preprocessor_config.json`.
pub struct LocalClipEmbeddingProvider {
    model: Arc<ClipModel>,
    tokenizer: Arc<Tokenizer>,
    preprocessor: ClipPreprocessor,
    projection_dim: usize,
    pad_token_id: u32,
    compute_slots: Arc<Semaphore>,
}

impl LocalClipEmbeddingProvider {
    pub fn load(directory: &Path) -> Result<Self, TurbopufferError> {
        let config_path = directory.join("config.json");
        let tokenizer_path = directory.join("tokenizer.json");
        let preprocessor_path = directory.join("preprocessor_config.json");
        let weights_path = directory.join("model.safetensors");

        let config: HfClipConfig = read_json(&config_path, "CLIP config")?;
        let clip_config = config.to_candle()?;
        if clip_config.text_config.max_position_embeddings != CLIP_CONTEXT_LENGTH {
            return Err(TurbopufferError::Other(format!(
                "CLIP config {} must use a {CLIP_CONTEXT_LENGTH}-token context (got {})",
                config_path.display(),
                clip_config.text_config.max_position_embeddings
            )));
        }
        let projection_dim = clip_config.text_config.projection_dim;
        if projection_dim != clip_config.vision_config.projection_dim {
            return Err(TurbopufferError::Other(format!(
                "CLIP config {} has mismatched text ({projection_dim}) and vision ({}) projection dimensions",
                config_path.display(),
                clip_config.vision_config.projection_dim
            )));
        }

        let mut tokenizer = Tokenizer::from_file(&tokenizer_path).map_err(|error| {
            TurbopufferError::Other(format!(
                "failed to load CLIP tokenizer {}: {error}",
                tokenizer_path.display()
            ))
        })?;
        let pad_token_id = config.text_config.pad_token_id.unwrap_or(1);
        let pad_token = tokenizer
            .id_to_token(pad_token_id)
            .unwrap_or_else(|| "<|endoftext|>".to_string());
        tokenizer.with_padding(Some(PaddingParams {
            strategy: PaddingStrategy::Fixed(CLIP_CONTEXT_LENGTH),
            pad_id: pad_token_id,
            pad_token,
            ..PaddingParams::default()
        }));
        tokenizer
            .with_truncation(Some(TruncationParams {
                max_length: CLIP_CONTEXT_LENGTH,
                ..TruncationParams::default()
            }))
            .map_err(|error| {
                TurbopufferError::Other(format!(
                    "failed to configure CLIP tokenizer {}: {error}",
                    tokenizer_path.display()
                ))
            })?;

        let preprocessor_config: HfPreprocessorConfig =
            read_json(&preprocessor_path, "CLIP preprocessor config")?;
        let preprocessor = ClipPreprocessor::try_from(preprocessor_config)?;

        let device = Device::Cpu;
        // The artifact is immutable after startup. Mapping it avoids a second
        // 600MB buffered copy while Candle constructs the model tensors.
        let builder = unsafe {
            VarBuilder::from_mmaped_safetensors(&[weights_path.as_path()], DType::F32, &device)
        }
        .map_err(|error| {
            TurbopufferError::Other(format!(
                "failed to load CLIP weights {}: {error}",
                weights_path.display()
            ))
        })?;
        let model = ClipModel::new(builder, &clip_config).map_err(|error| {
            TurbopufferError::Other(format!(
                "failed to construct CLIP model from {}: {error}",
                weights_path.display()
            ))
        })?;

        Ok(Self {
            model: Arc::new(model),
            tokenizer: Arc::new(tokenizer),
            preprocessor,
            projection_dim,
            pad_token_id,
            compute_slots: Arc::new(Semaphore::new(DEFAULT_COMPUTE_CONCURRENCY)),
        })
    }

    fn validate_request(
        &self,
        request: &EmbeddingRequest<'_>,
        modality: EmbeddingModality,
    ) -> Result<(), TurbopufferError> {
        if !is_clip_model(request.model) {
            return Err(TurbopufferError::Other(format!(
                "local CLIP provider requires a CLIP-family model (got `{}`)",
                request.model
            )));
        }
        if request.revision.is_some() {
            return Err(TurbopufferError::Other(
                "local CLIP provider does not support model revisions".to_string(),
            ));
        }
        if request.modality != modality {
            return Err(TurbopufferError::Other(format!(
                "local CLIP provider received {:?} inputs for a {:?} request",
                modality, request.modality
            )));
        }
        if request
            .dims
            .is_some_and(|dims| dims != self.projection_dim as u64)
        {
            return Err(TurbopufferError::Other(format!(
                "local CLIP artifact has {} dimensions, but {} were requested",
                self.projection_dim,
                request.dims.expect("checked Some")
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl EmbeddingProvider for LocalClipEmbeddingProvider {
    async fn embed(
        &self,
        request: &EmbeddingRequest<'_>,
        texts: &[String],
    ) -> Result<EmbeddingBatch, TurbopufferError> {
        self.validate_request(request, EmbeddingModality::Text)?;
        if texts.is_empty() {
            return Ok(empty_batch());
        }

        let (unique, requested) = deduplicate(texts, |text| content_id(text.as_bytes()));
        let model = Arc::clone(&self.model);
        let tokenizer = Arc::clone(&self.tokenizer);
        let pad_token_id = self.pad_token_id;
        let permit = Arc::clone(&self.compute_slots)
            .acquire_owned()
            .await
            .map_err(|error| {
                TurbopufferError::Other(format!("local CLIP compute pool closed: {error}"))
            })?;
        let started = Instant::now();
        let (unique_vectors, token_count) = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let encodings = tokenizer.encode_batch(unique, true).map_err(|error| {
                TurbopufferError::Other(format!("CLIP tokenization failed: {error}"))
            })?;
            let token_count = encodings
                .iter()
                .flat_map(|encoding| encoding.get_ids())
                .filter(|id| **id != pad_token_id)
                .count();
            let token_ids = encodings
                .iter()
                .map(|encoding| encoding.get_ids().to_vec())
                .collect::<Vec<_>>();
            let input_ids = Tensor::new(token_ids, &Device::Cpu).map_err(candle_error)?;
            let features = model.get_text_features(&input_ids).map_err(candle_error)?;
            let normalized = div_l2_norm(&features).map_err(candle_error)?;
            let vectors = normalized.to_vec2::<f32>().map_err(candle_error)?;
            Ok::<_, TurbopufferError>((vectors, token_count))
        })
        .await
        .map_err(|error| TurbopufferError::Other(format!("local CLIP worker failed: {error}")))??;

        Ok(EmbeddingBatch {
            vectors: restore_requested(unique_vectors, requested),
            performance: json!({
                "embedding_tokens": token_count,
                "embedding_ms": started.elapsed().as_secs_f64() * 1000.0,
            }),
            billing: None,
        })
    }

    async fn embed_images(
        &self,
        request: &EmbeddingRequest<'_>,
        images: &[Vec<u8>],
    ) -> Result<EmbeddingBatch, TurbopufferError> {
        self.validate_request(request, EmbeddingModality::Image)?;
        if images.is_empty() {
            return Ok(empty_batch());
        }

        let (unique, requested) = deduplicate(images, |bytes| content_id(bytes));
        let model = Arc::clone(&self.model);
        let preprocessor = self.preprocessor.clone();
        let permit = Arc::clone(&self.compute_slots)
            .acquire_owned()
            .await
            .map_err(|error| {
                TurbopufferError::Other(format!("local CLIP compute pool closed: {error}"))
            })?;
        let started = Instant::now();
        let unique_vectors = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let pixel_values = preprocessor.batch(&unique)?;
            let features = model
                .get_image_features(&pixel_values)
                .map_err(candle_error)?;
            let normalized = div_l2_norm(&features).map_err(candle_error)?;
            normalized.to_vec2::<f32>().map_err(candle_error)
        })
        .await
        .map_err(|error| TurbopufferError::Other(format!("local CLIP worker failed: {error}")))??;

        Ok(EmbeddingBatch {
            vectors: restore_requested(unique_vectors, requested),
            performance: json!({
                "embedding_images": images.len(),
                "embedding_ms": started.elapsed().as_secs_f64() * 1000.0,
            }),
            billing: None,
        })
    }
}

pub(crate) fn is_clip_model(model: &str) -> bool {
    model.to_ascii_lowercase().contains("clip")
}

fn empty_batch() -> EmbeddingBatch {
    EmbeddingBatch {
        vectors: Vec::new(),
        performance: json!({}),
        billing: None,
    }
}

fn content_id(content: &[u8]) -> String {
    format!("{:x}", Sha256::digest(content))
}

fn deduplicate<T: Clone>(values: &[T], id: impl Fn(&T) -> String) -> (Vec<T>, Vec<usize>) {
    let mut unique = Vec::new();
    let mut positions = HashMap::<String, usize>::new();
    let mut requested = Vec::with_capacity(values.len());
    for value in values {
        let content_id = id(value);
        let position = if let Some(position) = positions.get(&content_id) {
            *position
        } else {
            let position = unique.len();
            positions.insert(content_id, position);
            unique.push(value.clone());
            position
        };
        requested.push(position);
    }
    (unique, requested)
}

fn restore_requested(vectors: Vec<Vec<f32>>, requested: Vec<usize>) -> Vec<Vec<f64>> {
    requested
        .into_iter()
        .map(|position| vectors[position].iter().copied().map(f64::from).collect())
        .collect()
}

fn candle_error(error: candle::Error) -> TurbopufferError {
    TurbopufferError::Other(format!("CLIP inference failed: {error}"))
}

fn read_json<T: for<'de> Deserialize<'de>>(
    path: &Path,
    description: &str,
) -> Result<T, TurbopufferError> {
    let bytes = std::fs::read(path).map_err(|error| {
        TurbopufferError::Other(format!(
            "failed to read {description} {}: {error}",
            path.display()
        ))
    })?;
    serde_json::from_slice(&bytes).map_err(|error| {
        TurbopufferError::Other(format!(
            "failed to parse {description} {}: {error}",
            path.display()
        ))
    })
}

#[derive(Deserialize)]
struct HfClipConfig {
    projection_dim: usize,
    #[serde(default = "default_logit_scale")]
    logit_scale_init_value: f32,
    text_config: HfTextConfig,
    vision_config: HfVisionConfig,
}

impl HfClipConfig {
    fn to_candle(&self) -> Result<ClipConfig, TurbopufferError> {
        for (tower, activation) in [
            ("text", self.text_config.hidden_act.as_deref()),
            ("vision", self.vision_config.hidden_act.as_deref()),
        ] {
            if activation.is_some_and(|activation| activation != "quick_gelu") {
                return Err(TurbopufferError::Other(format!(
                    "local CLIP {tower} tower supports only quick_gelu activation"
                )));
            }
        }
        let text_projection = self
            .text_config
            .projection_dim
            .unwrap_or(self.projection_dim);
        let vision_projection = self
            .vision_config
            .projection_dim
            .unwrap_or(self.projection_dim);
        Ok(ClipConfig {
            text_config: ClipTextConfig {
                vocab_size: self.text_config.vocab_size,
                embed_dim: self.text_config.hidden_size,
                activation: Activation::QuickGelu,
                intermediate_size: self.text_config.intermediate_size,
                max_position_embeddings: self.text_config.max_position_embeddings,
                pad_with: None,
                num_hidden_layers: self.text_config.num_hidden_layers,
                num_attention_heads: self.text_config.num_attention_heads,
                projection_dim: text_projection,
            },
            vision_config: ClipVisionConfig {
                embed_dim: self.vision_config.hidden_size,
                activation: Activation::QuickGelu,
                intermediate_size: self.vision_config.intermediate_size,
                num_hidden_layers: self.vision_config.num_hidden_layers,
                num_attention_heads: self.vision_config.num_attention_heads,
                projection_dim: vision_projection,
                num_channels: self.vision_config.num_channels,
                image_size: self.vision_config.image_size,
                patch_size: self.vision_config.patch_size,
            },
            logit_scale_init_value: self.logit_scale_init_value,
            image_size: self.vision_config.image_size,
        })
    }
}

fn default_logit_scale() -> f32 {
    2.6592
}

#[derive(Deserialize)]
struct HfTextConfig {
    vocab_size: usize,
    hidden_size: usize,
    intermediate_size: usize,
    max_position_embeddings: usize,
    num_hidden_layers: usize,
    num_attention_heads: usize,
    #[serde(default)]
    projection_dim: Option<usize>,
    #[serde(default)]
    pad_token_id: Option<u32>,
    #[serde(default)]
    hidden_act: Option<String>,
}

#[derive(Deserialize)]
struct HfVisionConfig {
    hidden_size: usize,
    intermediate_size: usize,
    num_hidden_layers: usize,
    num_attention_heads: usize,
    #[serde(default = "default_num_channels")]
    num_channels: usize,
    image_size: usize,
    patch_size: usize,
    #[serde(default)]
    projection_dim: Option<usize>,
    #[serde(default)]
    hidden_act: Option<String>,
}

fn default_num_channels() -> usize {
    3
}

#[derive(Clone)]
struct ClipPreprocessor {
    resize_shortest_edge: u32,
    crop_width: u32,
    crop_height: u32,
    mean: [f32; 3],
    std: [f32; 3],
    filter: FilterType,
}

impl ClipPreprocessor {
    fn batch(&self, images: &[Vec<u8>]) -> Result<Tensor, TurbopufferError> {
        let image_len = 3 * self.crop_width as usize * self.crop_height as usize;
        let mut batch = Vec::with_capacity(images.len() * image_len);
        for bytes in images {
            batch.extend(self.preprocess(bytes)?);
        }
        Tensor::from_vec(
            batch,
            (
                images.len(),
                3,
                self.crop_height as usize,
                self.crop_width as usize,
            ),
            &Device::Cpu,
        )
        .map_err(candle_error)
    }

    fn preprocess(&self, bytes: &[u8]) -> Result<Vec<f32>, TurbopufferError> {
        let decoded = image::load_from_memory(bytes).map_err(|error| {
            TurbopufferError::Other(format!("failed to decode CLIP image: {error}"))
        })?;
        let rgb = decoded.to_rgb8();
        let (width, height) = rgb.dimensions();
        if width == 0 || height == 0 {
            return Err(TurbopufferError::Other(
                "CLIP image has zero width or height".to_string(),
            ));
        }
        let scale = self.resize_shortest_edge as f64 / f64::from(width.min(height));
        let resized_width = (f64::from(width) * scale).round().max(1.0) as u32;
        let resized_height = (f64::from(height) * scale).round().max(1.0) as u32;
        let resized = image::imageops::resize(&rgb, resized_width, resized_height, self.filter);
        if resized_width < self.crop_width || resized_height < self.crop_height {
            return Err(TurbopufferError::Other(format!(
                "CLIP resized image {resized_width}x{resized_height} is smaller than configured crop {}x{}",
                self.crop_width, self.crop_height
            )));
        }
        let left = (resized_width - self.crop_width) / 2;
        let top = (resized_height - self.crop_height) / 2;
        let cropped =
            image::imageops::crop_imm(&resized, left, top, self.crop_width, self.crop_height)
                .to_image();

        let plane = self.crop_width as usize * self.crop_height as usize;
        let mut output = vec![0.0_f32; plane * 3];
        for (index, pixel) in cropped.pixels().enumerate() {
            for channel in 0..3 {
                output[channel * plane + index] =
                    (f32::from(pixel[channel]) / 255.0 - self.mean[channel]) / self.std[channel];
            }
        }
        Ok(output)
    }
}

#[derive(Deserialize)]
struct HfPreprocessorConfig {
    size: ImageSize,
    crop_size: ImageSize,
    image_mean: Vec<f32>,
    image_std: Vec<f32>,
    #[serde(default)]
    resample: Option<u8>,
}

impl TryFrom<HfPreprocessorConfig> for ClipPreprocessor {
    type Error = TurbopufferError;

    fn try_from(config: HfPreprocessorConfig) -> Result<Self, Self::Error> {
        let resize_shortest_edge = config.size.shortest_edge().ok_or_else(|| {
            TurbopufferError::Other(
                "CLIP preprocessor `size` must provide a positive shortest edge".to_string(),
            )
        })?;
        let (crop_width, crop_height) = config.crop_size.dimensions().ok_or_else(|| {
            TurbopufferError::Other(
                "CLIP preprocessor `crop_size` must provide positive width and height".to_string(),
            )
        })?;
        let mean: [f32; 3] = config.image_mean.try_into().map_err(|_| {
            TurbopufferError::Other("CLIP preprocessor `image_mean` must have 3 values".to_string())
        })?;
        let std: [f32; 3] = config.image_std.try_into().map_err(|_| {
            TurbopufferError::Other("CLIP preprocessor `image_std` must have 3 values".to_string())
        })?;
        if std.contains(&0.0) {
            return Err(TurbopufferError::Other(
                "CLIP preprocessor standard deviations must be non-zero".to_string(),
            ));
        }
        let filter = match config.resample.unwrap_or(3) {
            0 => FilterType::Nearest,
            1 => FilterType::Lanczos3,
            2 => FilterType::Triangle,
            3 => FilterType::CatmullRom,
            value => {
                return Err(TurbopufferError::Other(format!(
                    "unsupported CLIP preprocessor resample value {value}"
                )))
            }
        };
        Ok(Self {
            resize_shortest_edge,
            crop_width,
            crop_height,
            mean,
            std,
            filter,
        })
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ImageSize {
    Square(u32),
    Dimensions {
        #[serde(default)]
        width: Option<u32>,
        #[serde(default)]
        height: Option<u32>,
        #[serde(default)]
        shortest_edge: Option<u32>,
    },
}

impl ImageSize {
    fn shortest_edge(&self) -> Option<u32> {
        match self {
            Self::Square(value) => (*value > 0).then_some(*value),
            Self::Dimensions {
                width,
                height,
                shortest_edge,
            } => shortest_edge
                .filter(|value| *value > 0)
                .or_else(|| match (width, height) {
                    (Some(width), Some(height)) if *width > 0 && *height > 0 => {
                        Some((*width).min(*height))
                    }
                    _ => None,
                }),
        }
    }

    fn dimensions(&self) -> Option<(u32, u32)> {
        match self {
            Self::Square(value) if *value > 0 => Some((*value, *value)),
            Self::Square(_) => None,
            Self::Dimensions {
                width,
                height,
                shortest_edge,
            } => match (width, height) {
                (Some(width), Some(height)) if *width > 0 && *height > 0 => Some((*width, *height)),
                _ => shortest_edge
                    .filter(|value| *value > 0)
                    .map(|value| (value, value)),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use candle_nn::VarMap;
    use image::{DynamicImage, ImageBuffer, ImageFormat, Rgb};
    use tokenizers::models::wordlevel::WordLevel;
    use tokenizers::pre_tokenizers::whitespace::Whitespace;
    use tokenizers::processors::template::TemplateProcessing;

    use super::*;

    fn write_clip_fixture(directory: &Path) {
        std::fs::create_dir_all(directory).unwrap();
        let config = ClipConfig {
            text_config: ClipTextConfig {
                vocab_size: 6,
                embed_dim: 4,
                activation: Activation::QuickGelu,
                intermediate_size: 8,
                max_position_embeddings: CLIP_CONTEXT_LENGTH,
                pad_with: None,
                num_hidden_layers: 1,
                num_attention_heads: 1,
                projection_dim: 2,
            },
            vision_config: ClipVisionConfig {
                embed_dim: 4,
                activation: Activation::QuickGelu,
                intermediate_size: 8,
                num_hidden_layers: 1,
                num_attention_heads: 1,
                projection_dim: 2,
                num_channels: 3,
                image_size: 4,
                patch_size: 2,
            },
            logit_scale_init_value: default_logit_scale(),
            image_size: 4,
        };
        let variables = VarMap::new();
        let builder = VarBuilder::from_varmap(&variables, DType::F32, &Device::Cpu);
        ClipModel::new(builder, &config).unwrap();
        variables.save(directory.join("model.safetensors")).unwrap();

        std::fs::write(
            directory.join("config.json"),
            serde_json::to_vec_pretty(&json!({
                "projection_dim": 2,
                "logit_scale_init_value": default_logit_scale(),
                "text_config": {
                    "vocab_size": 6,
                    "hidden_size": 4,
                    "intermediate_size": 8,
                    "max_position_embeddings": CLIP_CONTEXT_LENGTH,
                    "num_hidden_layers": 1,
                    "num_attention_heads": 1,
                    "projection_dim": 2,
                    "pad_token_id": 0,
                    "hidden_act": "quick_gelu"
                },
                "vision_config": {
                    "hidden_size": 4,
                    "intermediate_size": 8,
                    "num_hidden_layers": 1,
                    "num_attention_heads": 1,
                    "projection_dim": 2,
                    "image_size": 4,
                    "patch_size": 2,
                    "hidden_act": "quick_gelu"
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let vocab = [
            ("[PAD]".to_string(), 0),
            ("[UNK]".to_string(), 1),
            ("hello".to_string(), 2),
            ("world".to_string(), 3),
            ("[BOS]".to_string(), 4),
            ("[EOS]".to_string(), 5),
        ]
        .into_iter()
        .collect();
        let word_level = WordLevel::builder()
            .vocab(vocab)
            .unk_token("[UNK]".to_string())
            .build()
            .unwrap();
        let mut tokenizer = Tokenizer::new(word_level);
        tokenizer.with_pre_tokenizer(Some(Whitespace));
        tokenizer.with_post_processor(Some(
            TemplateProcessing::builder()
                .try_single("[BOS] $A [EOS]")
                .unwrap()
                .special_tokens(vec![("[BOS]", 4), ("[EOS]", 5)])
                .build()
                .unwrap(),
        ));
        tokenizer
            .save(directory.join("tokenizer.json"), false)
            .unwrap();

        std::fs::write(
            directory.join("preprocessor_config.json"),
            serde_json::to_vec_pretty(&json!({
                "size": {"shortest_edge": 4},
                "crop_size": {"width": 4, "height": 4},
                "image_mean": [0.5, 0.5, 0.5],
                "image_std": [0.5, 0.5, 0.5],
                "resample": 3
            }))
            .unwrap(),
        )
        .unwrap();
    }

    fn fixture_image() -> Vec<u8> {
        let image = ImageBuffer::from_fn(6, 4, |x, y| Rgb([(x * 30) as u8, (y * 50) as u8, 120]));
        let mut bytes = Cursor::new(Vec::new());
        DynamicImage::ImageRgb8(image)
            .write_to(&mut bytes, ImageFormat::Png)
            .unwrap();
        bytes.into_inner()
    }

    fn assert_normalized(vector: &[f64]) {
        let norm = vector.iter().map(|value| value * value).sum::<f64>().sqrt();
        assert!((norm - 1.0).abs() < 1e-5, "vector norm was {norm}");
    }

    #[tokio::test]
    async fn fixture_checkpoint_embeds_both_towers_normalizes_and_deduplicates() {
        let directory = std::env::temp_dir().join(format!(
            "hevlayer-local-clip-provider-test-{}",
            uuid::Uuid::new_v4()
        ));
        write_clip_fixture(&directory);
        let provider = LocalClipEmbeddingProvider::load(&directory).expect("load fixture");

        let text_batch = provider
            .embed(
                &EmbeddingRequest {
                    model: "openai/clip-vit-base-patch32",
                    dims: Some(2),
                    revision: None,
                    modality: EmbeddingModality::Text,
                },
                &["hello world".to_string(), "hello world".to_string()],
            )
            .await
            .expect("embed fixture text");
        let image = fixture_image();
        let image_batch = provider
            .embed_images(
                &EmbeddingRequest {
                    model: "openai/clip-vit-base-patch32",
                    dims: Some(2),
                    revision: None,
                    modality: EmbeddingModality::Image,
                },
                &[image.clone(), image],
            )
            .await
            .expect("embed fixture image");
        std::fs::remove_dir_all(&directory).unwrap();

        assert_eq!(text_batch.vectors.len(), 2);
        assert_eq!(text_batch.vectors[0], text_batch.vectors[1]);
        assert_eq!(text_batch.vectors[0].len(), 2);
        assert_normalized(&text_batch.vectors[0]);
        assert_eq!(text_batch.performance["embedding_tokens"], 4);
        assert!(text_batch.billing.is_none());

        assert_eq!(image_batch.vectors.len(), 2);
        assert_eq!(image_batch.vectors[0], image_batch.vectors[1]);
        assert_eq!(image_batch.vectors[0].len(), 2);
        assert_normalized(&image_batch.vectors[0]);
        assert_eq!(image_batch.performance["embedding_images"], 2);
        assert!(image_batch.billing.is_none());
    }
}
