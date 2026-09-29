#![cfg(not(feature = "pro"))]
//! `/search` in the open gateway (RFC 0116: ships in CE, enabled; the
//! provider key is the switch). Boots the standalone binary against a local
//! Turbopuffer-shaped upstream and a hand-rolled Jev backend.

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::http::StatusCode as UpstreamStatus;
use axum::response::IntoResponse;
use serde_json::{json, Value};

const STORE_KEY: &str = "tpuf_local";
const RERANK_KEY: &str = "ts-ce-key-never-echoed";

struct Gateway {
    child: Child,
    base: String,
    store_file: std::path::PathBuf,
}

impl Drop for Gateway {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.store_file);
    }
}

async fn serve(router: axum::Router) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    address
}

/// A Turbopuffer-shaped upstream: `papers` declares `embed:` and two
/// full-text attributes, `bare` declares neither. Queries answer the native
/// embedding read-back and every leg.
async fn spawn_upstream() -> std::net::SocketAddr {
    let router = axum::Router::new()
        .route(
            "/v2/namespaces/{namespace}/metadata",
            axum::routing::get(
                |axum::extract::Path(namespace): axum::extract::Path<String>| async move {
                    let schema = if namespace == "papers" {
                        json!({
                            "title": {"type": "string", "full_text_search": true},
                            "text": {"type": "string", "full_text_search": true,
                                     "embed": "voyage/voyage-4-lite"},
                            "year": {"type": "uint"}
                        })
                    } else {
                        json!({"year": {"type": "uint"}})
                    };
                    axum::Json(json!({
                        "schema": schema,
                        "approx_row_count": 3,
                        "index": {"status": "up-to-date"}
                    }))
                },
            ),
        )
        .route(
            "/v2/namespaces/{namespace}",
            axum::routing::post(|axum::Json(_body): axum::Json<Value>| async {
                axum::Json(json!({
                    "status": "OK",
                    "rows_affected": 1,
                    "performance": {"embedding_tokens": 5, "embedding_ms": 2},
                    "billing": {"billable_logical_bytes_written": 10}
                }))
            }),
        )
        .route(
            "/v2/namespaces/{namespace}/query",
            axum::routing::post(|axum::Json(body): axum::Json<Value>| async move {
                // Shard-manifest probe: this namespace is not sharded.
                if body.get("filters") == Some(&json!(["id", "Eq", "_hevlayer:namespace_meta"])) {
                    return (
                        UpstreamStatus::NOT_FOUND,
                        axum::Json(json!({"error": "namespace not found"})),
                    )
                        .into_response();
                }
                // Native embedding read-back: a vector per content id.
                if body["filters"][1] == "In" {
                    let rows: Vec<Value> = body["filters"][2]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|id| json!({"id": id, "vector": [0.6, 0.8]}))
                        .collect();
                    return axum::Json(json!({"rows": rows, "billing": {}})).into_response();
                }
                axum::Json(json!({
                    "rows": [
                        {"id": "p1", "$dist": 0.1, "title": "Vitamin D and bone",
                         "text": "supplementation raised bone density p=0.91",
                         "_hevlayer_upserted_at": 1700000000000u64, "_hevlayer_shard": 0},
                        {"id": "p2", "$dist": 0.2, "title": "Ingress timeouts",
                         "text": "kubernetes ingress drops connections p=0.07",
                         "_hevlayer_upserted_at": 1700000000000u64, "_hevlayer_shard": 1}
                    ],
                    "billing": {"billable_logical_bytes_queried": 1, "billable_logical_bytes_returned": 1}
                }))
                .into_response()
            }),
        )
        .fallback(|| async {
            (
                UpstreamStatus::NOT_FOUND,
                axum::Json(json!({"error": "not found"})),
            )
        });
    serve(router).await
}

async fn spawn_jev(bodies: Arc<Mutex<Vec<Value>>>) -> std::net::SocketAddr {
    let router = axum::Router::new().route(
        "/v1/systemone",
        axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
            let bodies = Arc::clone(&bodies);
            async move {
                bodies.lock().unwrap().push(body.clone());
                let answers: serde_json::Map<String, Value> = body["state"]["documents"]
                    .as_object()
                    .unwrap()
                    .iter()
                    .map(|(key, document)| {
                        let text = document.to_string();
                        let p: f64 = text
                            .split("p=")
                            .nth(1)
                            .map(|rest| rest.chars().take(4).collect::<String>())
                            .and_then(|number| number.parse().ok())
                            .unwrap_or(0.5);
                        (key.clone(), json!({"type": "noul", "noul": p}))
                    })
                    .collect();
                axum::Json(json!({
                    "model": "jev-test-1",
                    "usage": {"input_tokens": 321, "output_tokens": 2},
                    "answers": answers
                }))
            }
        }),
    );
    serve(router).await
}

async fn boot(upstream: std::net::SocketAddr, rerank: Option<std::net::SocketAddr>) -> Gateway {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let store_file = std::env::temp_dir().join(format!(
        "hevlayer-search-ce-{}-{port}.yaml",
        std::process::id()
    ));
    std::fs::write(
        &store_file,
        format!(
            r#"
apiVersion: hevlayer.com/v1alpha1
kind: VectorStore
metadata:
  name: local
spec:
  kind: turbopuffer
  default: true
  endpoint:
    url: http://{upstream}
    region: aws-us-east-1
  credential:
    secretRef:
      name: local
      key: api-key
  inboundAuth:
    mode: deriveFromStore
"#
        ),
    )
    .unwrap();

    let mut command = Command::new(env!("CARGO_BIN_EXE_hevlayer-gateway"));
    command
        .env("KUBECONFIG", "/dev/null")
        .env("PORT", port.to_string())
        .env("LAYER_STORE_FILE", &store_file)
        .env("LAYER_SECRET_LOCAL_API_KEY", STORE_KEY)
        .env("LAYER_AWS_COST_EXPLORER_ENABLED", "false")
        .env("LAYER_TELEMETRY", "off")
        .env_remove("S3_BUCKET")
        .env_remove("S3_ENDPOINT")
        .env_remove("TYPESAFE_API_KEY")
        .env_remove("TYPESAFE_BASE_URL")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(rerank) = rerank {
        command
            .env("TYPESAFE_API_KEY", RERANK_KEY)
            .env("TYPESAFE_BASE_URL", format!("http://{rerank}"));
    }
    let child = command.spawn().unwrap();
    let gateway = Gateway {
        child,
        base: format!("http://127.0.0.1:{port}"),
        store_file,
    };

    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match client.get(format!("{}/health", gateway.base)).send().await {
            Ok(response) if response.status().is_success() => break,
            _ if Instant::now() > deadline => panic!("gateway did not answer /health"),
            _ => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    gateway
}

async fn search(gateway: &Gateway, namespace: &str, body: Value) -> (reqwest::StatusCode, Value) {
    let response = reqwest::Client::new()
        .post(format!("{}/v2/namespaces/{namespace}/search", gateway.base))
        .bearer_auth(STORE_KEY)
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = response.status();
    (status, response.json().await.unwrap())
}

#[tokio::test]
async fn open_gateway_searches_end_to_end_with_a_key() {
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let upstream = spawn_upstream().await;
    let jev = spawn_jev(Arc::clone(&bodies)).await;
    let gateway = boot(upstream, Some(jev)).await;

    let (status, body) = search(
        &gateway,
        "papers",
        json!({"query": "does vitamin D improve bone density", "top_k": 2, "explain": true}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK, "{body}");
    assert_eq!(body["rerank"]["executed"], true);
    assert_eq!(body["rerank"]["calls"], 1);
    assert_eq!(body["rerank"]["model"], "jev-test-1");
    assert_eq!(body["rerank"]["input_tokens"], 321);
    assert_eq!(
        body["plan"],
        json!({"executed": false, "reason": "unconfigured"})
    );
    assert_eq!(body["routing"]["advisory"], true);
    assert_eq!(body["rows"][0]["id"], "p1");
    assert_eq!(body["rows"][0]["score"], 0.91);
    assert_eq!(body["rows"][1]["score"], 0.07);
    assert!(body["rows"][0]["explain"]["features"]["age_seconds"].is_number());
    assert!(body["rows"][0]["attributes"]
        .get("_hevlayer_shard")
        .is_none());
    let labels: Vec<&str> = body["hybrid"]["legs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|leg| leg["label"].as_str().unwrap())
        .collect();
    assert_eq!(labels[..3], ["ann", "bm25:text", "bm25:title"]);
    assert_eq!(body["performance"]["embedding_tokens"], 5.0);
    assert!(!body.to_string().contains(RERANK_KEY));

    let bodies = bodies.lock().unwrap();
    assert_eq!(bodies.len(), 1);
    assert!(
        !bodies[0].to_string().contains("_hevlayer_"),
        "{}",
        bodies[0]
    );
    assert_eq!(
        bodies[0]["state"]["documents"]["D00"]["title"],
        "Vitamin D and bone"
    );
}

#[tokio::test]
async fn open_gateway_without_a_key_answers_rerank_unconfigured() {
    let upstream = spawn_upstream().await;
    let gateway = boot(upstream, None).await;

    let (status, body) = search(&gateway, "papers", json!({"query": "vitamin d"})).await;
    assert_eq!(status, reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"], "rerank_unconfigured");

    let (status, body) = search(&gateway, "bare", json!({"query": "vitamin d"})).await;
    assert_eq!(status, reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"], "embed_attribute_missing");

    let (status, body) = search(&gateway, "papers", json!({"query": "x y", "plan": true})).await;
    assert_eq!(status, reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"], "validation_error");

    // The stage off: fused order, no provider, still a 200.
    let (status, body) = search(
        &gateway,
        "papers",
        json!({"query": "vitamin d", "rerank": false}),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::OK, "{body}");
    assert_eq!(body["rerank"]["reason"], "disabled");
    assert_eq!(body["rows"].as_array().unwrap().len(), 2);
}
