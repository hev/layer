use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

#[tokio::test]
async fn boots_without_kubernetes_when_store_json_is_unset() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let mut child = Command::new(env!("CARGO_BIN_EXE_hevlayer-gateway"))
        .env("KUBECONFIG", "/dev/null")
        .env("PORT", port.to_string())
        .env("LAYER_AWS_COST_EXPLORER_ENABLED", "false")
        .env("LAYER_TELEMETRY", "off")
        .env(
            "LAYER_AGENTS_JSON",
            r#"{"demo":{"model":{"provider":"openrouter","name":"m","apiKey":"k"},"indices":["default"]}}"#,
        )
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    let url = format!("http://127.0.0.1:{port}/health");
    let mut healthy = false;
    while Instant::now() < deadline {
        match client.get(&url).send().await {
            Ok(response) if response.status().is_success() => {
                healthy = true;
                break;
            }
            _ => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }

    assert!(healthy, "gateway did not answer /health before timeout");

    let base = format!("http://127.0.0.1:{port}");
    let license_response = client
        .get(format!("{base}/v2/license"))
        .bearer_auth("test-token")
        .send()
        .await
        .unwrap();
    assert_eq!(license_response.status(), reqwest::StatusCode::OK);
    let license: serde_json::Value = license_response.json().await.unwrap();
    assert_eq!(license["gateway"]["state"], "floor");

    let pro_route_checks = [
        client
            .post(format!("{base}/v2/agents/demo/query"))
            .bearer_auth("test-token")
            .json(&serde_json::json!({"query": "hello"})),
        client
            .post(format!("{base}/v2/pipelines"))
            .bearer_auth("test-token")
            .json(&serde_json::json!({"id": "p", "target_namespace": "n"})),
        client
            .post(format!("{base}/v2/udfs"))
            .bearer_auth("test-token")
            .json(&serde_json::json!({"id": "u"})),
        client
            .post(format!("{base}/v2/keys/authenticate"))
            .bearer_auth("test-token")
            .json(&serde_json::json!({"token": "layer_test"})),
        client
            .get(format!("{base}/v2/warehouses"))
            .bearer_auth("test-token"),
        client
            .put(format!("{base}/v1/namespaces/demo/blobs"))
            .bearer_auth("test-token"),
        client
            .get(format!("{base}/v2/namespaces/demo/history"))
            .bearer_auth("test-token"),
        client
            .get(format!("{base}/v2/namespaces/demo/search-history"))
            .bearer_auth("test-token"),
        client
            .get(format!("{base}/v2/activity/snapshots"))
            .bearer_auth("test-token"),
        client
            .post(format!("{base}/v1/control/restores"))
            .bearer_auth("test-token")
            .json(&serde_json::json!({"id": "r", "source_dsn": "s", "target_dsn": "t"})),
        client
            .post(format!("{base}/v2/namespaces/demo/checkpoints"))
            .bearer_auth("test-token")
            .json(&serde_json::json!({"label": "l"})),
        client
            .post(format!("{base}/v2/namespaces/demo/shard/migrate"))
            .bearer_auth("test-token"),
        client
            .get(format!("{base}/v2/cost"))
            .bearer_auth("test-token"),
    ];
    let mut pro_statuses = Vec::new();
    for request in pro_route_checks {
        pro_statuses.push(request.send().await.unwrap().status());
    }

    let _ = child.kill();
    let _ = child.wait();

    assert!(
        pro_statuses
            .iter()
            .all(|status| *status == reqwest::StatusCode::NOT_FOUND),
        "expected pro routes to be absent in open gateway, got {pro_statuses:?}"
    );
}

#[tokio::test]
async fn boots_with_vectorstore_resource_file_without_kubernetes() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let store_file = std::env::temp_dir().join(format!(
        "hevlayer-vectorstore-{}-{port}.yaml",
        std::process::id()
    ));
    std::fs::write(
        &store_file,
        r#"
apiVersion: hevlayer.com/v1alpha1
kind: VectorStore
metadata:
  name: local
spec:
  kind: turbopuffer
  default: true
  endpoint:
    url: https://api.turbopuffer.com
    region: aws-us-east-1
  credential:
    secretRef:
      name: local
      key: api-key
  inboundAuth:
    mode: deriveFromStore
"#,
    )
    .unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_hevlayer-gateway"))
        .env("KUBECONFIG", "/dev/null")
        .env("PORT", port.to_string())
        .env("LAYER_STORE_FILE", &store_file)
        .env("LAYER_SECRET_LOCAL_API_KEY", "tpuf_local")
        .env("LAYER_AWS_COST_EXPLORER_ENABLED", "false")
        .env("LAYER_TELEMETRY", "off")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    let base = format!("http://127.0.0.1:{port}");
    let mut healthy = false;
    while Instant::now() < deadline {
        match client.get(format!("{base}/health")).send().await {
            Ok(response) if response.status().is_success() => {
                healthy = true;
                break;
            }
            _ => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }

    let response = client
        .get(format!("{base}/v2/vectorstores/local"))
        .bearer_auth("tpuf_local")
        .send()
        .await
        .unwrap();

    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_file(&store_file);

    assert!(healthy, "gateway did not answer /health before timeout");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["name"], "local");
    assert_eq!(body["endpoint"]["region"], "aws-us-east-1");
}
