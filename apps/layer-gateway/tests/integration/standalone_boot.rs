#![cfg(not(feature = "pro"))]

use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

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

/// Upstream ceiling for a request that should never touch the AWS credential
/// chain. The stall this guards against is the AWS SDK's ~1s IMDS timeout, so
/// the bound has an order of magnitude of headroom on a loaded runner while
/// still failing hard if any S3 call path regresses into a credential lookup.
const NO_OBJECT_STORE_REQUEST_BUDGET: Duration = Duration::from_millis(100);

fn assert_timed(
    label: &str,
    started: Instant,
    status: reqwest::StatusCode,
    expected: reqwest::StatusCode,
    failures: &mut Vec<String>,
) {
    let elapsed = started.elapsed();
    if status != expected {
        failures.push(format!("{label}: expected {expected}, got {status}"));
    }
    if elapsed > NO_OBJECT_STORE_REQUEST_BUDGET {
        failures.push(format!(
            "{label}: took {elapsed:?} (> {NO_OBJECT_STORE_REQUEST_BUDGET:?}) — \
             credential-lookup stall?"
        ));
    }
}

/// No-object-store profile: boot the standalone binary with no `S3_BUCKET` and
/// no AWS credentials against a local mock upstream, exercise the core write /
/// query path plus every S3-backed feature endpoint in the open surface, and
/// assert nothing stalls on a credential lookup.
#[tokio::test]
async fn no_object_store_profile_serves_instantly_and_degrades_deliberately() {
    use axum::http::StatusCode as UpstreamStatus;
    use axum::response::IntoResponse;

    // Local mock Turbopuffer upstream: accepts writes/queries/deletes.
    let upstream = axum::Router::new()
        .route(
            "/v2/namespaces/{namespace}",
            axum::routing::post(|axum::Json(_body): axum::Json<Value>| async {
                axum::Json(json!({
                    "status": "OK",
                    "message": "documents committed successfully",
                    "rows_affected": 1,
                    "billing": {"billable_logical_bytes_written": 4137}
                }))
            })
            .delete(|| async { axum::Json(json!({"status": "OK"})) }),
        )
        .route(
            "/v1/namespaces/{namespace}",
            axum::routing::delete(|| async { axum::Json(json!({"status": "OK"})) }),
        )
        .route(
            "/v2/namespaces/{namespace}/query",
            axum::routing::post(|axum::Json(body): axum::Json<Value>| async move {
                if body.get("filters") == Some(&json!(["id", "Eq", "_hevlayer:namespace_meta"])) {
                    return (
                        UpstreamStatus::NOT_FOUND,
                        axum::Json(json!({"error": "namespace not found"})),
                    )
                        .into_response();
                }
                axum::Json(json!({
                    "rows": [{"id": "no-s3-doc-1", "$dist": 0.1}],
                    "billing": {
                        "billable_logical_bytes_queried": 1024,
                        "billable_logical_bytes_returned": 12
                    }
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

    let upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_address = upstream_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(upstream_listener, upstream).await.unwrap();
    });

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let store_file = std::env::temp_dir().join(format!(
        "hevlayer-no-object-store-{}-{port}.yaml",
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
    url: http://{upstream_address}
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

    let mut child = Command::new(env!("CARGO_BIN_EXE_hevlayer-gateway"))
        .env("KUBECONFIG", "/dev/null")
        .env("PORT", port.to_string())
        .env("LAYER_STORE_FILE", &store_file)
        .env("LAYER_SECRET_LOCAL_API_KEY", "tpuf_local")
        .env("LAYER_AWS_COST_EXPLORER_ENABLED", "false")
        .env("LAYER_TELEMETRY", "off")
        // The profile under test: no object store configured and no ambient
        // AWS credentials, so any AWS SDK construction would stall on IMDS.
        .env_remove("S3_BUCKET")
        .env_remove("S3_ENDPOINT")
        .env_remove("AWS_ACCESS_KEY_ID")
        .env_remove("AWS_SECRET_ACCESS_KEY")
        .env_remove("AWS_SESSION_TOKEN")
        .env_remove("AWS_PROFILE")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    let deadline = Instant::now() + Duration::from_secs(15);
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
    assert!(healthy, "gateway did not answer /health before timeout");

    let mut failures: Vec<String> = Vec::new();

    // First write: the v0.5.0 regression path (load_profiles read S3 here).
    let started = Instant::now();
    let write = client
        .post(format!("{base}/v2/namespaces/no-s3"))
        .bearer_auth("tpuf_local")
        .json(&json!({
            "distance_metric": "cosine_distance",
            "upsert_rows": [{"id": "no-s3-doc-1", "vector": [0.1, 0.2, 0.3], "title": "standalone"}]
        }))
        .send()
        .await
        .unwrap();
    assert_timed(
        "first write",
        started,
        write.status(),
        reqwest::StatusCode::OK,
        &mut failures,
    );

    // Query: exercises the search-history logging side effect.
    let started = Instant::now();
    let query = client
        .post(format!("{base}/v2/namespaces/no-s3/query"))
        .bearer_auth("tpuf_local")
        .json(&json!({"vector": [0.1, 0.2, 0.3], "top_k": 5}))
        .send()
        .await
        .unwrap();
    assert_timed(
        "query",
        started,
        query.status(),
        reqwest::StatusCode::OK,
        &mut failures,
    );

    // Snapshot policy read degrades to "not found" instead of stalling.
    let started = Instant::now();
    let policy_get = client
        .get(format!("{base}/v2/namespaces/no-s3/snapshot-policy"))
        .bearer_auth("tpuf_local")
        .send()
        .await
        .unwrap();
    assert_timed(
        "snapshot-policy get",
        started,
        policy_get.status(),
        reqwest::StatusCode::NOT_FOUND,
        &mut failures,
    );

    // S3-requiring endpoints answer with a clear 4xx, never a 502.
    let started = Instant::now();
    let policy_put = client
        .put(format!("{base}/v2/namespaces/no-s3/snapshot-policy"))
        .bearer_auth("tpuf_local")
        .json(&json!({"interval": "5m", "retention": "never"}))
        .send()
        .await
        .unwrap();
    let policy_put_status = policy_put.status();
    let policy_put_body: Value = policy_put.json().await.unwrap();
    assert_timed(
        "snapshot-policy put",
        started,
        policy_put_status,
        reqwest::StatusCode::UNPROCESSABLE_ENTITY,
        &mut failures,
    );
    if policy_put_body["error"] != "object_store_not_configured" {
        failures.push(format!(
            "snapshot-policy put: expected object_store_not_configured error body, got {policy_put_body}"
        ));
    }

    let started = Instant::now();
    let snapshot_job = client
        .post(format!("{base}/v2/namespaces/no-s3/snapshots"))
        .bearer_auth("tpuf_local")
        .json(&json!({"field": "title"}))
        .send()
        .await
        .unwrap();
    assert_timed(
        "snapshot job create",
        started,
        snapshot_job.status(),
        reqwest::StatusCode::UNPROCESSABLE_ENTITY,
        &mut failures,
    );

    // Namespace delete runs the S3 purge sweep, which must no-op instantly.
    let started = Instant::now();
    let delete = client
        .delete(format!("{base}/v2/namespaces/no-s3"))
        .bearer_auth("tpuf_local")
        .send()
        .await
        .unwrap();
    assert_timed(
        "namespace delete",
        started,
        delete.status(),
        reqwest::StatusCode::OK,
        &mut failures,
    );

    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_file(&store_file);

    assert!(
        failures.is_empty(),
        "no-object-store profile failures:\n{}",
        failures.join("\n")
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
