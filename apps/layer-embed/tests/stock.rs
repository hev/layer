//! Explicit offline acceptance with previously prepared stock artifacts.
use layer_embed::{
    protocol::{Modality, Purpose, Request},
    registry::Registry,
};
use std::{
    path::PathBuf,
    sync::atomic::AtomicBool,
    time::{Duration, Instant},
};

#[test]
#[ignore = "requires prepared stock artifacts and independent reference vectors; see README"]
fn stock_cpu_reference_parity() {
    let root = PathBuf::from(std::env::var_os("LAYER_EMBED_TEST_MODELS").expect("model directory"));
    let references: serde_json::Value = serde_json::from_slice(
        &std::fs::read(std::env::var_os("LAYER_EMBED_TEST_REFERENCE").expect("reference file"))
            .unwrap(),
    )
    .unwrap();
    let start = Instant::now();
    let registry = Registry::load(&root, None).unwrap();
    println!(
        "offline load and both-model warmup: {:?}; manifest {}",
        start.elapsed(),
        registry.manifest_sha256
    );
    assert_eq!(registry.models.len(), 2);
    for case in references["cases"].as_array().unwrap() {
        let model = &registry.models[case["model"].as_str().unwrap()];
        let purpose: Purpose = serde_json::from_value(case["purpose"].clone()).unwrap();
        let inputs: Vec<String> = serde_json::from_value(case["inputs"].clone()).unwrap();
        let expected: Vec<Vec<f32>> = serde_json::from_value(case["vectors"].clone()).unwrap();
        assert!(expected[0]
            .iter()
            .zip(&expected[1])
            .any(|(a, b)| (a - b).abs() > 1e-3));
        for positions in [
            vec![0, 1],
            vec![1, 0],
            vec![0, 1, 0],
            vec![0, 1, 0, 1, 0],
            vec![0],
            vec![1],
        ] {
            let request = Request {
                model: model.record.id.clone(),
                artifact_sha256: model.fingerprint.clone(),
                dimensions: 384,
                purpose,
                modality: Modality::Text,
                inputs: positions.iter().map(|&i| inputs[i].clone()).collect(),
                timeout_ms: 120000,
            };
            let output = model
                .infer(
                    &request,
                    Instant::now() + Duration::from_secs(120),
                    &AtomicBool::new(false),
                )
                .unwrap();
            if positions == [0, 1] {
                assert_eq!(
                    output.usage.input_tokens,
                    case["tokens"].as_u64().unwrap() as usize
                );
            }
            let mut worst = 0f32;
            for (vector, &position) in output.vectors.iter().zip(&positions) {
                assert_eq!(vector.len(), 384);
                assert!(vector.iter().all(|x| x.is_finite()));
                let norm = vector.iter().map(|v| v * v).sum::<f32>().sqrt();
                assert!((norm - 1.).abs() <= 1e-4);
                let target = &expected[position];
                let cosine = vector.iter().zip(target).map(|(a, b)| a * b).sum::<f32>()
                    / (norm * target.iter().map(|x| x * x).sum::<f32>().sqrt());
                let error = vector
                    .iter()
                    .zip(target)
                    .map(|(a, b)| (a - b).abs())
                    .fold(0f32, f32::max);
                assert!(
                    cosine >= 0.9999,
                    "{} {:?} cosine {cosine}",
                    model.record.id,
                    purpose
                );
                assert!(
                    error <= 1e-4,
                    "{} {:?} error {error}",
                    model.record.id,
                    purpose
                );
                worst = worst.max(error);
            }
            println!(
                "{} {:?} {:?}: max abs error {worst}, tokens {}, inference {} ms",
                model.record.id,
                purpose,
                positions,
                output.usage.input_tokens,
                output.timing.inference_ms
            );
        }
    }
}

#[test]
#[ignore = "requires prepared stock artifacts; see README"]
fn mounted_bundle_add_override_and_changed_space() {
    let root = PathBuf::from(std::env::var_os("LAYER_EMBED_TEST_MODELS").expect("model directory"));
    let original: layer_embed::registry::Manifest =
        serde_json::from_slice(&std::fs::read(root.join("manifest.json")).unwrap()).unwrap();
    let mount = tempfile::tempdir().unwrap();
    let mut override_model = original
        .models
        .iter()
        .find(|m| m.id.contains("MiniLM"))
        .unwrap()
        .clone();
    for file in &override_model.files {
        let to = mount.path().join(&file.path);
        std::fs::create_dir_all(to.parent().unwrap()).unwrap();
        std::fs::hard_link(root.join(&file.path), to).unwrap();
    }
    // Read-only hardlinks to unmodified files; only this owned manifest changes.
    override_model.prefixes.query = "query: ".into();
    let mut added = override_model.clone();
    added.id = "local/my-bert".into();
    let manifest = layer_embed::registry::Manifest {
        version: 1,
        models: vec![override_model.clone(), added],
    };
    std::fs::write(
        mount.path().join("manifest.json"),
        serde_jcs::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    let registry = Registry::load(&root, Some(mount.path())).unwrap();
    assert_eq!(registry.models.len(), 3);
    assert!(registry.models.contains_key("local/my-bert"));
    let model = &registry.models[&override_model.id];
    let old = original
        .models
        .iter()
        .find(|m| m.id == override_model.id)
        .unwrap();
    let request = Request {
        model: old.id.clone(),
        artifact_sha256: layer_embed::registry::hash(&serde_jcs::to_vec(old).unwrap()),
        dimensions: 384,
        purpose: Purpose::Document,
        modality: Modality::Text,
        inputs: vec!["red shoes".into()],
        timeout_ms: 1000,
    };
    assert_eq!(
        model.validate(&request).unwrap_err().code,
        "artifact_mismatch"
    );
    println!("Mount add/override ready; changed preprocessing fingerprint rejects old pin.");
}
