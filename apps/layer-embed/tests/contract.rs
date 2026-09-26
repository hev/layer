use axum::{
    body::{to_bytes, Body},
    http::{Request as HttpRequest, StatusCode},
};
use candle_core::{DType, Device, Tensor};
use candle_nn::{VarBuilder, VarMap};
use candle_transformers::models::bert::{BertModel, Config};
use layer_embed::{
    model::{normalize, pool, Model},
    protocol::{Modality, Purpose, Request},
    registry::{hash, read_manifest, ModelRecord, Prefixes, Registry},
    service::{router, Service},
};
use serde_json::{json, Value};
use std::future::IntoFuture;
use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicBool, Arc, OnceLock},
    time::{Duration, Instant},
};
use tokenizers::{models::wordlevel::WordLevel, pre_tokenizers::whitespace::Whitespace, Tokenizer};
use tower::ServiceExt;

// Real tiny randomly initialized BERT fixture, never selected by the executable.
// It exercises the CPU path without downloading stock weights in ordinary tests.
fn registry() -> Arc<Registry> {
    static REGISTRY: OnceLock<Arc<Registry>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let config = json!({"model_type":"bert","vocab_size":8,"hidden_size":384,"num_hidden_layers":1,"num_attention_heads":12,"intermediate_size":32,"hidden_act":"gelu","hidden_dropout_prob":0.0,"max_position_embeddings":512,"type_vocab_size":2,"initializer_range":0.02,"layer_norm_eps":1e-12,"pad_token_id":0,"position_embedding_type":"absolute","use_cache":false});
        let parsed: Config = serde_json::from_value(config.clone()).unwrap();
        let vars = VarMap::new();
        let _bert = BertModel::load(VarBuilder::from_varmap(&vars,DType::F32,&Device::Cpu),&parsed).unwrap();
        let file = tempfile::NamedTempFile::new().unwrap(); vars.save(file.path()).unwrap();
        let tokenizer_model = WordLevel::builder().vocab([("[PAD]".to_owned(),0),("[UNK]".to_owned(),1),("red".to_owned(),2),("blue".to_owned(),3),("shoes".to_owned(),4),("query".to_owned(),5),("long".to_owned(),6),("text".to_owned(),7)].into_iter().collect()).unk_token("[UNK]".into()).build().unwrap();
        let mut tokenizer = Tokenizer::new(tokenizer_model);tokenizer.with_pre_tokenizer(Some(Whitespace));
        let mut models = BTreeMap::new();
        for (id,pooling,prefix) in [("test/mean","mean_masked",""),("test/cls","cls","query ")] {
            let record = ModelRecord { id:id.into(), architecture:"bert".into(),dimensions:384,dtype:"f32".into(),max_tokens:256,pooling:pooling.into(),normalization:"l2".into(),prefixes:Prefixes { document:"".into(),query:prefix.into() },preprocessing_version:1,files:vec![] };
            let fingerprint = hash(&serde_jcs::to_vec(&record).unwrap());
            let model = Model::load(record,fingerprint,&serde_json::to_vec(&config).unwrap(),tokenizer.to_string(false).unwrap().as_bytes(),std::fs::read(file.path()).unwrap()).unwrap();
            models.insert(id.into(),Arc::new(model));
        }
        Arc::new(Registry {models,manifest_sha256:"a".repeat(64)})
    }).clone()
}
fn request(model: &Model, purpose: Purpose, inputs: &[&str]) -> Request {
    Request {
        model: model.record.id.clone(),
        artifact_sha256: model.fingerprint.clone(),
        dimensions: 384,
        purpose,
        modality: Modality::Text,
        inputs: inputs.iter().map(|s| s.to_string()).collect(),
        timeout_ms: 10000,
    }
}
fn infer(model: &Model, inputs: &[&str]) -> Vec<Vec<f32>> {
    model
        .infer(
            &request(model, Purpose::Document, inputs),
            Instant::now() + Duration::from_secs(30),
            &AtomicBool::new(false),
        )
        .unwrap()
        .vectors
}
#[test]
fn pooling_ignores_padding_and_cls_selects_first() {
    let hidden = Tensor::new(&[[[1f32, 2.], [3., 6.], [900., 900.]]], &Device::Cpu).unwrap();
    let mask = Tensor::new(&[[1u32, 1, 0]], &Device::Cpu).unwrap();
    assert_eq!(
        pool(&hidden, &mask, "mean_masked")
            .unwrap()
            .to_vec2::<f32>()
            .unwrap(),
        vec![vec![2., 4.]]
    );
    assert_eq!(
        pool(&hidden, &mask, "cls")
            .unwrap()
            .to_vec2::<f32>()
            .unwrap(),
        vec![vec![1., 2.]]
    );
    for mut bad in [vec![0., 0.], vec![f32::NAN, 1.], vec![f32::INFINITY, 1.]] {
        assert!(normalize(&mut bad).is_err());
    }
}
#[test]
fn cpu_padding_microbatches_order_duplicates_and_unit_norm() {
    for model in registry().models.values() {
        let a = infer(model, &["red shoes"]).remove(0);
        let b = infer(model, &["blue long text shoes"]).remove(0);
        assert!(a.iter().zip(&b).any(|(a, b)| (a - b).abs() > 1e-4));
        for inputs in [
            vec!["red shoes", "blue long text shoes"],
            vec!["blue long text shoes", "red shoes"],
            vec![
                "red shoes",
                "blue long text shoes",
                "red shoes",
                "blue long text shoes",
                "red shoes",
            ],
        ] {
            for (input, vector) in inputs.iter().zip(infer(model, &inputs)) {
                assert_eq!(vector.len(), 384);
                assert!(vector.iter().all(|x| x.is_finite()));
                assert!((vector.iter().map(|x| x * x).sum::<f32>().sqrt() - 1.).abs() < 1e-4);
                let expected = if *input == "red shoes" { &a } else { &b };
                assert!(vector
                    .iter()
                    .zip(expected)
                    .all(|(x, y)| (x - y).abs() < 1e-4));
            }
        }
    }
}
#[test]
fn prefix_is_validated_only_for_query_and_never_inserted_twice() {
    let registry = registry();
    let model = &registry.models["test/cls"];
    assert_eq!(
        model
            .validate(&request(model, Purpose::Query, &["red shoes"]))
            .unwrap_err()
            .code,
        "invalid_input"
    );
    assert!(model
        .validate(&request(model, Purpose::Document, &["red shoes"]))
        .is_ok());
    let encoded = model
        .validate(&request(model, Purpose::Query, &["query red shoes"]))
        .unwrap();
    assert_eq!(encoded[0].get_ids(), &[5, 2, 4]);
    for input in ["", " ", "query ", "query \t"] {
        assert!(model
            .validate(&request(model, Purpose::Query, &[input]))
            .is_err());
    }
}
#[test]
fn input_limits_identity_and_no_truncation() {
    let registry = registry();
    let model = &registry.models["test/mean"];
    for (mut req, code) in [
        (request(model, Purpose::Document, &[]), "invalid_request"),
        (
            request(model, Purpose::Document, &["red"; 33]),
            "batch_too_large",
        ),
        (request(model, Purpose::Document, &[" "]), "invalid_input"),
        (
            request(model, Purpose::Document, &[&"r".repeat(65537)]),
            "input_too_long",
        ),
        (
            request(model, Purpose::Document, &[&"red ".repeat(257)]),
            "input_too_long",
        ),
    ] {
        assert_eq!(model.validate(&req).unwrap_err().code, code);
        req.inputs.clear();
    }
    assert_eq!(
        model
            .validate(&request(model, Purpose::Document, &[&"red ".repeat(256)]))
            .unwrap()[0]
            .len(),
        256
    );
    let mut req = request(model, Purpose::Document, &["red"]);
    req.dimensions = 383;
    assert_eq!(model.validate(&req).unwrap_err().code, "dimension_mismatch");
    req.dimensions = 384;
    req.artifact_sha256 = "b".repeat(64);
    assert_eq!(model.validate(&req).unwrap_err().code, "artifact_mismatch");
    req.artifact_sha256 = model.fingerprint.clone();
    req.modality = Modality::Image;
    assert_eq!(
        model.validate(&req).unwrap_err().code,
        "unsupported_modality"
    );
    req.modality = Modality::Text;
    req.inputs = vec!["red ".repeat(256); 16];
    assert!(model.validate(&req).is_ok());
    req.inputs.push("red".into());
    assert_eq!(model.validate(&req).unwrap_err().code, "batch_token_limit");
}
async fn call(
    service: Arc<Service>,
    method: &str,
    path: &str,
    content_type: &str,
    body: Vec<u8>,
) -> (StatusCode, Value) {
    let response = router(service)
        .oneshot(
            HttpRequest::builder()
                .method(method)
                .uri(path)
                .header("content-type", content_type)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 3_000_000).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}
#[tokio::test]
async fn http_errors_and_readiness() {
    let service = Service::new();
    assert_eq!(
        call(service.clone(), "GET", "/health/live", "", vec![])
            .await
            .1,
        json!({"status":"live"})
    );
    assert_eq!(
        call(service.clone(), "GET", "/health/ready", "", vec![])
            .await
            .1["reason"],
        "loading_models"
    );
    *service.registry.write().unwrap() = Some(registry());
    for (method, path, content, body, code, status) in [
        ("GET", "/unknown", "", vec![], "not_found", 404),
        (
            "PUT",
            "/v1/embeddings",
            "",
            vec![],
            "method_not_allowed",
            405,
        ),
        (
            "POST",
            "/v1/embeddings",
            "text/plain",
            vec![],
            "unsupported_media_type",
            415,
        ),
        (
            "POST",
            "/v1/embeddings",
            "application/json",
            vec![b' '; 1_048_577],
            "body_too_large",
            413,
        ),
        (
            "POST",
            "/v1/embeddings",
            "application/json",
            vec![0xff],
            "invalid_request",
            400,
        ),
    ] {
        let (actual, error) = call(service.clone(), method, path, content, body).await;
        assert_eq!(actual.as_u16(), status);
        assert_eq!(error["error"]["code"], code);
    }
    let registry = registry();
    let model = &registry.models["test/mean"];
    let valid = serde_json::to_value(request(model, Purpose::Document, &["red shoes"])).unwrap();
    for (key, value) in [
        ("purpose", json!("other")),
        ("purpose", Value::Null),
        ("extra", json!(1)),
        ("inputs", json!([])),
        ("inputs", json!([null])),
        ("timeout_ms", json!(-1)),
        ("timeout_ms", json!(0)),
        ("timeout_ms", json!(120001)),
        ("timeout_ms", json!(1.5)),
    ] {
        let mut invalid = valid.clone();
        invalid[key] = value;
        assert_eq!(
            call(
                service.clone(),
                "POST",
                "/v1/embeddings",
                "application/json",
                serde_json::to_vec(&invalid).unwrap()
            )
            .await
            .1["error"]["code"],
            "invalid_request"
        );
    }
    let duplicate =
        serde_json::to_string(&valid)
            .unwrap()
            .replacen('{', "{\"purpose\":\"query\",", 1);
    assert_eq!(
        call(
            service.clone(),
            "POST",
            "/v1/embeddings",
            "application/json",
            duplicate.into_bytes()
        )
        .await
        .1["error"]["code"],
        "invalid_request"
    );
    let mut invalid = valid.clone();
    invalid["model"] = json!("unknown/model");
    assert_eq!(
        call(
            service.clone(),
            "POST",
            "/v1/embeddings",
            "application/json",
            serde_json::to_vec(&invalid).unwrap()
        )
        .await
        .1["error"]["code"],
        "model_not_found"
    );
    let response = call(
        service.clone(),
        "POST",
        "/v1/embeddings",
        "application/json",
        serde_json::to_vec(&valid).unwrap(),
    )
    .await;
    assert_eq!(response.0, StatusCode::OK);
    assert_eq!(response.1["vectors"][0].as_array().unwrap().len(), 384);
    service
        .shutting_down
        .store(true, std::sync::atomic::Ordering::Release);
    assert_eq!(
        call(service, "GET", "/health/ready", "", vec![]).await.1["reason"],
        "shutting_down"
    );
}
#[tokio::test]
async fn overload_and_queued_deadline_release_capacity() {
    let service = Service::new();
    *service.registry.write().unwrap() = Some(registry());
    let model = &registry().models["test/mean"];
    let mut req = request(model, Purpose::Document, &["red"]);
    req.timeout_ms = 10;
    let capacity = service.admitted.available_permits();
    let admission = service
        .admitted
        .clone()
        .acquire_many_owned(capacity as u32)
        .await
        .unwrap();
    let response = call(
        service.clone(),
        "POST",
        "/v1/embeddings",
        "application/json",
        serde_json::to_vec(&req).unwrap(),
    )
    .await;
    assert_eq!(response.1["error"]["code"], "overloaded");
    drop(admission);
    let active = service
        .active
        .clone()
        .acquire_many_owned(service.active.available_permits() as u32)
        .await
        .unwrap();
    assert_eq!(
        call(
            service.clone(),
            "POST",
            "/v1/embeddings",
            "application/json",
            serde_json::to_vec(&req).unwrap()
        )
        .await
        .1["error"]["code"],
        "deadline_exceeded"
    );
    assert_eq!(service.admitted.available_permits(), capacity);
    drop(active);
}
#[test]
fn missing_artifacts_and_strict_manifest() {
    let dir = tempfile::tempdir().unwrap();
    assert!(Registry::load(dir.path(), None).is_err());
    let original: Value = serde_json::from_str(include_str!("../manifest.json")).unwrap();
    for mutate in [0, 1, 2, 3, 4, 5, 6, 7] {
        let mut manifest = original.clone();
        match mutate {
            0 => manifest["version"] = json!(2),
            1 => manifest["unknown"] = json!(true),
            2 => manifest["models"][0]["pooling"] = json!("unknown"),
            3 => manifest["models"][0]["files"][0]["path"] = json!("../escape"),
            4 => manifest["models"][0]["preprocessing_version"] = json!(2),
            6 => manifest["models"][0]["files"][0]["path"] = json!("bge//config.json"),
            7 => manifest["models"][0]["files"][0]["path"] = json!("bge/config.json/"),
            _ => {
                let duplicate = manifest["models"][0].clone();
                manifest["models"].as_array_mut().unwrap().push(duplicate);
            }
        }
        std::fs::write(
            dir.path().join("manifest.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        assert!(read_manifest(dir.path()).is_err());
    }
    std::fs::write(
        dir.path().join("manifest.json"),
        serde_json::to_vec(&original).unwrap(),
    )
    .unwrap();
    assert!(read_manifest(dir.path()).is_ok());
    assert!(Registry::load(dir.path(), None).is_err());
}

#[tokio::test]
async fn connection_close_cancels_queued_work() {
    use tokio::io::AsyncWriteExt;
    let service = Service::new();
    *service.registry.write().unwrap() = Some(registry());
    let capacity = service.admitted.available_permits();
    let _active = service
        .active
        .clone()
        .acquire_many_owned(service.active.available_permits() as u32)
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(
        axum::serve(
            layer_embed::connection::TrackingListener(listener),
            router(service.clone())
                .into_make_service_with_connect_info::<layer_embed::connection::Disconnect>(),
        )
        .into_future(),
    );
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let body = serde_json::to_string(&request(
        &registry().models["test/mean"],
        Purpose::Document,
        &["red shoes"],
    ))
    .unwrap();
    stream.write_all(format!("POST /v1/embeddings HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",body.len(),body).as_bytes()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while service.admitted.available_permits() == capacity {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(stream);
    tokio::time::timeout(Duration::from_secs(2), async {
        while service.admitted.available_permits() != capacity {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    server.abort();
}

#[test]
fn cancelled_blocking_job_keeps_permits_until_it_exits() {
    // Occupy the sole blocking thread so the real inference closure is definitely
    // pending when its HTTP future is cancelled. Tokio cannot cancel that closure.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let service = Service::new();
        *service.registry.write().unwrap() = Some(registry());
        let active_capacity = service.active.available_permits();
        let admission_capacity = service.admitted.available_permits();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        entered_rx.await.unwrap();
        let req = request(&registry().models["test/mean"], Purpose::Document, &["red"]);
        let owned = service.clone();
        let http = tokio::spawn(async move {
            call(
                owned,
                "POST",
                "/v1/embeddings",
                "application/json",
                serde_json::to_vec(&req).unwrap(),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while service.active.available_permits() == active_capacity {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        http.abort();
        assert!(http.await.unwrap_err().is_cancelled());
        assert_eq!(service.active.available_permits(), active_capacity - 1);
        assert_eq!(service.admitted.available_permits(), admission_capacity - 1);
        release_tx.send(()).unwrap();
        blocker.await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while service.active.available_permits() != active_capacity {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(service.admitted.available_permits(), admission_capacity);
    });
}

#[test]
fn stock_manifest_semantics_are_fixed() {
    let manifest: layer_embed::registry::Manifest =
        serde_json::from_str(include_str!("../manifest.json")).unwrap();
    assert_eq!(manifest.models.len(), 2);
    for model in manifest.models {
        assert_eq!(model.dimensions, 384);
        assert_eq!(model.normalization, "l2");
        assert_eq!(model.prefixes.document, "");
        match model.id.as_str() {
            "BAAI/bge-small-en-v1.5" => {
                assert_eq!(model.pooling, "cls");
                assert_eq!(model.max_tokens, 512);
                assert_eq!(
                    model.prefixes.query,
                    "Represent this sentence for searching relevant passages: "
                );
            }
            "sentence-transformers/all-MiniLM-L6-v2" => {
                assert_eq!(model.pooling, "mean_masked");
                assert_eq!(model.max_tokens, 256);
                assert_eq!(model.prefixes.query, "");
            }
            id => panic!("unexpected stock model {id}"),
        }
    }
}
