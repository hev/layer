use crate::{
    model::Model,
    protocol::{Modality, Purpose, Request, MAX_TOKENS},
};
use anyhow::{ensure, Context};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Component, Path},
    sync::{atomic::AtomicBool, Arc},
    time::{Duration, Instant},
};

pub const STOCK_IDS: [&str; 2] = [
    "BAAI/bge-small-en-v1.5",
    "sentence-transformers/all-MiniLM-L6-v2",
];
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Prefixes {
    pub document: String,
    pub query: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    pub path: String,
    pub size: u64,
    pub sha256: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRecord {
    pub id: String,
    pub architecture: String,
    pub dimensions: usize,
    pub dtype: String,
    pub max_tokens: usize,
    pub pooling: String,
    pub normalization: String,
    pub prefixes: Prefixes,
    pub preprocessing_version: u32,
    pub files: Vec<Artifact>,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub version: u32,
    pub models: Vec<ModelRecord>,
}
pub struct Registry {
    pub models: BTreeMap<String, Arc<Model>>,
    pub manifest_sha256: String,
}
pub fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub fn valid_hash(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
pub fn read_manifest(root: &Path) -> anyhow::Result<Manifest> {
    let canonical_root = root.canonicalize().context("artifact root missing")?;
    let manifest_path = canonical_root
        .join("manifest.json")
        .canonicalize()
        .context("manifest.json missing")?;
    ensure!(
        manifest_path.starts_with(&canonical_root),
        "manifest.json escapes artifact root"
    );
    let bytes = std::fs::read(manifest_path).context("manifest.json unreadable")?;
    let manifest: Manifest = serde_json::from_slice(&bytes).context("invalid manifest.json")?;
    ensure!(manifest.version == 1, "unsupported manifest version");
    let mut ids = BTreeSet::new();
    for model in &manifest.models {
        ensure!(ids.insert(&model.id), "duplicate model ID: {}", model.id);
        ensure!(
            model.id.split('/').count() == 2
                && model.id.split('/').all(|part| !part.is_empty()
                    && part
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))),
            "invalid namespaced model ID"
        );
        ensure!(
            model.architecture == "bert"
                && model.dtype == "f32"
                && model.normalization == "l2"
                && model.preprocessing_version == 1
                && matches!(model.pooling.as_str(), "cls" | "mean_masked"),
            "unsupported preprocessing for {}",
            model.id
        );
        ensure!(
            model.dimensions > 0 && model.max_tokens > 0 && model.max_tokens <= MAX_TOKENS,
            "invalid dimensions/token ceiling for {}",
            model.id
        );
        let mut paths = BTreeSet::new();
        for artifact in &model.files {
            ensure!(
                paths.insert(&artifact.path) && valid_hash(&artifact.sha256) && artifact.size > 0,
                "invalid file entry for {}",
                model.id
            );
            ensure!(
                !artifact.path.is_empty()
                    && Path::new(&artifact.path)
                        .components()
                        .all(|c| matches!(c, Component::Normal(_))),
                "unsafe artifact path for {}",
                model.id
            );
            let normalized: std::path::PathBuf = Path::new(&artifact.path).components().collect();
            ensure!(
                normalized.as_os_str() == std::ffi::OsStr::new(&artifact.path)
                    && !artifact.path.contains('\\'),
                "artifact path must be canonical relative POSIX form for {}",
                model.id
            );
        }
        let weights: Vec<_> = model
            .files
            .iter()
            .filter(|f| {
                Path::new(&f.path).file_name().and_then(|x| x.to_str()) == Some("model.safetensors")
            })
            .collect();
        ensure!(
            weights.len() == 1,
            "{} requires one model.safetensors",
            model.id
        );
        let parent = Path::new(&weights[0].path).parent().unwrap();
        for required in ["config.json", "tokenizer.json"] {
            ensure!(
                model
                    .files
                    .iter()
                    .any(|f| Path::new(&f.path) == parent.join(required)),
                "{} requires {} beside weights",
                model.id,
                required
            );
        }
    }
    Ok(manifest)
}
fn verified_files(root: &Path, model: &ModelRecord) -> anyhow::Result<BTreeMap<String, Vec<u8>>> {
    let root = root.canonicalize().context("artifact root missing")?;
    let mut files = BTreeMap::new();
    for entry in &model.files {
        let verify = || -> anyhow::Result<Vec<u8>> {
            let path = root.join(&entry.path).canonicalize()?;
            ensure!(path.starts_with(&root), "artifact escapes root");
            ensure!(
                path.metadata()?.is_file() && path.metadata()?.len() == entry.size,
                "artifact size mismatch"
            );
            let bytes = std::fs::read(path)?;
            ensure!(
                bytes.len() as u64 == entry.size && hash(&bytes) == entry.sha256,
                "artifact integrity mismatch"
            );
            Ok(bytes)
        };
        // Do not expose absolute paths or input data in startup diagnostics.
        let bytes = verify().map_err(|_| {
            anyhow::anyhow!("model {} file {} failed verification", model.id, entry.path)
        })?;
        files.insert(entry.path.clone(), bytes);
    }
    Ok(files)
}
impl Registry {
    pub fn load(baked: &Path, mounted: Option<&Path>) -> anyhow::Result<Self> {
        let baked_manifest = read_manifest(baked)?;
        ensure!(
            baked_manifest.models.len() == 2
                && STOCK_IDS
                    .iter()
                    .all(|id| baked_manifest.models.iter().any(|m| m.id == *id)),
            "baked registry must contain both stock models only"
        );
        let mut effective = BTreeMap::new();
        for model in baked_manifest.models {
            effective.insert(model.id.clone(), (model, baked.to_owned()));
        }
        if let Some(root) = mounted {
            for model in read_manifest(root)?.models {
                effective.insert(model.id.clone(), (model, root.to_owned()));
            }
        }
        let manifest = Manifest {
            version: 1,
            models: effective.values().map(|(m, _)| m.clone()).collect(),
        };
        let manifest_sha256 = hash(&serde_jcs::to_vec(&manifest)?);
        let mut models = BTreeMap::new();
        for (id, (record, root)) in effective {
            let mut files = verified_files(&root, &record)?;
            let weights_path = record
                .files
                .iter()
                .find(|f| f.path.ends_with("/model.safetensors") || f.path == "model.safetensors")
                .unwrap();
            let parent = Path::new(&weights_path.path).parent().unwrap();
            let mut take = |name: &str| -> Vec<u8> {
                files.remove(parent.join(name).to_str().unwrap()).unwrap()
            };
            let config = take("config.json");
            let tokenizer = take("tokenizer.json");
            let weights = take("model.safetensors");
            let fingerprint = hash(&serde_jcs::to_vec(&record)?);
            let model =
                Model::load(record, fingerprint, &config, &tokenizer, weights).map_err(|_| {
                    anyhow::anyhow!("model {id} failed config/tokenizer/model.safetensors load")
                })?;
            for purpose in [Purpose::Document, Purpose::Query] {
                let prefix = if purpose == Purpose::Query {
                    &model.record.prefixes.query
                } else {
                    &model.record.prefixes.document
                };
                let request = Request {
                    model: id.clone(),
                    artifact_sha256: model.fingerprint.clone(),
                    dimensions: model.record.dimensions,
                    purpose,
                    modality: Modality::Text,
                    inputs: vec![format!("{prefix}warmup")],
                    timeout_ms: 120000,
                };
                model
                    .infer(
                        &request,
                        Instant::now() + Duration::from_secs(120),
                        &AtomicBool::new(false),
                    )
                    .map_err(|_| anyhow::anyhow!("model {id} failed warmup"))?;
            }
            models.insert(id, Arc::new(model));
        }
        Ok(Self {
            models,
            manifest_sha256,
        })
    }
    pub fn discovery(&self) -> serde_json::Value {
        serde_json::json!({"protocol_version":1,"manifest_sha256":self.manifest_sha256,"limits":{"max_body_bytes":1048576,"max_batch_items":32,"max_batch_tokens":4096,"max_input_bytes":65536,"max_timeout_ms":120000},"models":self.models.values().map(|m| serde_json::json!({"id":m.record.id,"artifact_sha256":m.fingerprint,"dimensions":m.record.dimensions,"modalities":["text"],"max_tokens":m.record.max_tokens,"normalization":m.record.normalization,"pooling":m.record.pooling,"prefixes":m.record.prefixes})).collect::<Vec<_>>()})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn integrity_and_symlink_escape_fail_closed() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let bytes = b"verified artifact";
        std::fs::write(root.path().join("config.json"), bytes).unwrap();
        let mut model = ModelRecord {
            id: "test/model".into(),
            architecture: "bert".into(),
            dimensions: 384,
            dtype: "f32".into(),
            max_tokens: 256,
            pooling: "cls".into(),
            normalization: "l2".into(),
            prefixes: Prefixes {
                document: "".into(),
                query: "".into(),
            },
            preprocessing_version: 1,
            files: vec![Artifact {
                path: "config.json".into(),
                size: bytes.len() as u64,
                sha256: hash(bytes),
            }],
        };
        assert!(verified_files(root.path(), &model).is_ok());
        model.files[0].sha256 = "0".repeat(64);
        assert!(verified_files(root.path(), &model).is_err());
        model.files[0].sha256 = hash(bytes);
        model.files[0].size += 1;
        assert!(verified_files(root.path(), &model).is_err());
        model.files[0].size -= 1;
        std::fs::remove_file(root.path().join("config.json")).unwrap();
        assert!(verified_files(root.path(), &model).is_err());
        #[cfg(unix)]
        {
            std::fs::write(outside.path().join("config.json"), bytes).unwrap();
            std::os::unix::fs::symlink(
                outside.path().join("config.json"),
                root.path().join("config.json"),
            )
            .unwrap();
            assert!(verified_files(root.path(), &model).is_err());
        }
    }
}
