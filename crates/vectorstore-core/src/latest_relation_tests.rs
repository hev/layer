use super::*;
use crate::turbopuffer::HttpTurbopufferClient;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

struct Source {
    client: HttpTurbopufferClient,
    rows: Arc<Mutex<Vec<Value>>>,
    calls: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Source {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn source(count: usize, mutate: bool, repeat: bool) -> Source {
    let rows = Arc::new(Mutex::new(
        (0..count)
            .map(|i| json!({"id":format!("{i:06}"),"entity":i,"version":1,"amount":1}))
            .collect::<Vec<_>>(),
    ));
    let calls = Arc::new(AtomicUsize::new(0));
    let (r, c) = (rows.clone(), calls.clone());
    let router = axum::Router::new().route(
        "/v2/namespaces/{ns}/query",
        axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
            let (r, c) = (r.clone(), c.clone());
            async move {
                assert_eq!(body["consistency"], json!({"level":"strong"}));
                assert_eq!(body["rank_by"], json!(["id", "asc"]));
                assert_eq!(body["include_attributes"], true);
                let call = c.fetch_add(1, AtomicOrdering::SeqCst);
                let mut rows = r.lock().unwrap();
                if mutate && call == 1 {
                    // These writes happen between pages, behind and ahead of the cursor.
                    rows.push(json!({"id":"000050-new","entity":99999,"version":1,"amount":5}));
                    rows.push(json!({"id":"999999","entity":0,"version":2,"amount":7}));
                    rows.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
                }
                let after = body.get("filters").map(|f| f[2].as_str().unwrap());
                let page: Vec<_> = rows
                    .iter()
                    .filter(|r| repeat || after.is_none_or(|a| r["id"].as_str().unwrap() > a))
                    .take(body["top_k"].as_u64().unwrap() as usize)
                    .cloned()
                    .collect();
                axum::Json(json!({"rows":page}))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    Source {
        client: HttpTurbopufferClient::new("fixture", &format!("http://{address}")),
        rows,
        calls,
        task,
    }
}
fn budget(rows: usize) -> ScanBudget {
    ScanBudget {
        max_rows: rows,
        max_bytes: 10_000_000,
        consistency: ReadConsistency::Observed,
    }
}
fn latest() -> LatestSpec {
    serde_json::from_value(json!({"key":["entity"],"order_by":[{"field":"version","direction":"desc","nulls":"last"}]})).unwrap()
}
#[tokio::test]
async fn http_scan_exhausts_more_than_ten_thousand_without_consumer_rows() {
    let source = source(10007, false, false).await;
    let start = std::time::Instant::now();
    let data = source
        .client
        .latest_relation_source("fixture", &budget(20000))
        .await
        .unwrap();
    assert_eq!(data.rows.len(), 10007);
    assert_eq!(data.pages, 11);
    let bytes = data.bytes;
    let request = LatestRequest {
        latest_by: latest(),
        scan: budget(20000),
        filters: None,
        grouped: BTreeMap::new(),
        aggregate_by: BTreeMap::from([
            ("n".into(), json!(["Count"])),
            ("amount".into(), json!(["Sum", "amount"])),
        ]),
    };
    let out = request.execute(data, "fixture".into()).unwrap();
    assert_eq!(out["aggregations"], json!({"n":10007,"amount":10007}));
    assert!(out.get("rows").is_none());
    assert_eq!(source.calls.load(AtomicOrdering::SeqCst), 11);
    eprintln!("synthetic_latest source_rows=10007 source_pages=11 source_bytes={bytes} output_bytes={} elapsed_ms={} plan=strong_id_pages_then_latest_filter_scalar gateway_rows=O(revisions) backend_work=unmeasured billing=unmeasured",out.to_string().len(),start.elapsed().as_millis());
}
#[tokio::test]
async fn concurrent_page_writes_are_observed_not_fabricated_snapshot() {
    let source = source(2001, true, false).await;
    let data = source
        .client
        .latest_relation_source("fixture", &budget(5000))
        .await
        .unwrap();
    assert_eq!(source.rows.lock().unwrap().len(), 2003);
    assert_eq!(
        data.rows.len(),
        2002,
        "insert behind cursor is not observed"
    );
    let relation = latest()
        .select(data.rows, "fixture".into(), data.consistency)
        .unwrap();
    assert_eq!(relation.consistency, ReadConsistency::Observed);
    assert_eq!(relation.read_boundary, "observed_pages");
    assert!(!relation.rows.iter().any(|r| r["entity"] == 99999));
    assert_eq!(
        relation.rows.iter().find(|r| r["entity"] == 0).unwrap()["amount"],
        7
    );
    let mut snapshot = budget(5000);
    snapshot.consistency = ReadConsistency::Snapshot;
    let calls = source.calls.load(AtomicOrdering::SeqCst);
    assert!(source
        .client
        .latest_relation_source("fixture", &snapshot)
        .await
        .unwrap_err()
        .to_string()
        .contains("snapshot"));
    assert_eq!(source.calls.load(AtomicOrdering::SeqCst), calls);
}
#[tokio::test]
async fn scan_budgets_and_nonadvancing_source_fail_instead_of_partial_totals() {
    let src = source(2001, false, false).await;
    assert!(src
        .client
        .latest_relation_source("fixture", &budget(1000))
        .await
        .unwrap_err()
        .to_string()
        .contains("row budget"));
    let mut bytes = budget(3000);
    bytes.max_bytes = 10;
    assert!(src
        .client
        .latest_relation_source("fixture", &bytes)
        .await
        .unwrap_err()
        .to_string()
        .contains("byte budget"));
    let src = source(2001, false, true).await;
    assert!(src
        .client
        .latest_relation_source("fixture", &budget(3000))
        .await
        .unwrap_err()
        .to_string()
        .contains("strictly advancing"));
}

#[test]
fn scalar_projection_covers_every_filter_branch_and_literal_field() {
    let operators = [
        "Eq",
        "NotEq",
        "Gt",
        "Gte",
        "Lt",
        "Lte",
        "In",
        "NotIn",
        "Contains",
        "NotContains",
        "ContainsAny",
        "NotContainsAny",
    ];
    let filters: Vec<_> = operators
        .iter()
        .enumerate()
        .map(|(i, op)| {
            let value = if matches!(*op, "In" | "NotIn" | "ContainsAny" | "NotContainsAny") {
                json!([1])
            } else {
                json!(1)
            };
            json!([format!("filter.{i}"), op, value])
        })
        .collect();
    let mut request = LatestRequest {
        latest_by: latest(),
        scan: budget(5000),
        filters: Some(json!(["Not", ["Or", [["And", filters]]]])),
        aggregate_by: BTreeMap::from([("sum".into(), json!(["Sum", "metrics.tokens"]))]),
        grouped: BTreeMap::new(),
    };
    let fields = request.source_projection().unwrap().unwrap();
    for field in ["id", "entity", "version", "metrics.tokens"] {
        assert!(fields.contains(&field.to_string()));
    }
    for i in 0..operators.len() {
        assert!(fields.contains(&format!("filter.{i}")));
    }
    assert_eq!(fields.len(), 16);
    // Dotted names are literal attributes throughout this executor. Do not
    // replace them with a parent object projection and invent path semantics.
    assert_eq!(
        aggregate(
            &[json!({"metrics":{"tokens":99},"metrics.tokens":3})],
            &request.aggregate_by
        )
        .unwrap(),
        json!({"sum":3})
    );
    assert_eq!(
        aggregate(&[json!({"metrics":{"tokens":99}})], &request.aggregate_by).unwrap(),
        json!({"sum":0})
    );
    request.filters = Some(json!(["omitted", "Regex", ".*"]));
    assert!(request.source_projection().is_err());
}

#[tokio::test]
async fn http_projection_matched_fixture_and_unsupported_rejection() {
    // This source explicitly executes ID range scan -> limit -> attribute read.
    // Counters are synthetic executor evidence, not a vendor storage/billing plan.
    let rows: Arc<Vec<Value>> = Arc::new(
        (0..1201)
            .flat_map(|i| {
                let mut winner = json!({"id":format!("{i:06}-new"),"entity":i,"version":2,
            "active":i%3!=0,"metrics.tokens":2,"payload":"x".repeat(32768)});
                if i % 5 == 0 {
                    winner.as_object_mut().unwrap().remove("metrics.tokens");
                } else if i % 5 == 1 {
                    winner["metrics.tokens"] = Value::Null;
                }
                [
                    json!({"id":format!("{i:06}-old"),"entity":i,"version":1,
            "active":true,"metrics.tokens":900,"payload":"x".repeat(32768)}),
                    winner,
                ]
            })
            .collect(),
    );
    let requests = Arc::new(Mutex::new(Vec::<Value>::new()));
    let scanned = Arc::new(AtomicUsize::new(0));
    let response_bytes = Arc::new(AtomicUsize::new(0));
    let reject = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (r, q, s, b, reject_source) = (
        rows.clone(),
        requests.clone(),
        scanned.clone(),
        response_bytes.clone(),
        reject.clone(),
    );
    let router = axum::Router::new().route(
        "/v2/namespaces/{ns}/query",
        axum::routing::post(move |axum::Json(body): axum::Json<Value>| {
            let (r, q, s, b, reject) = (
                r.clone(),
                q.clone(),
                s.clone(),
                b.clone(),
                reject_source.clone(),
            );
            async move {
                q.lock().unwrap().push(body.clone());
                if reject.load(AtomicOrdering::SeqCst) && body["include_attributes"].is_array() {
                    return (
                        axum::http::StatusCode::UNPROCESSABLE_ENTITY,
                        axum::Json(json!({"error":"projection unsupported"})),
                    );
                }
                assert_eq!(body["rank_by"], json!(["id", "asc"]));
                assert_eq!(body["consistency"], json!({"level":"strong"}));
                assert!(body.get("filters").is_none() || body["filters"][0] == "id");
                s.fetch_add(r.len(), AtomicOrdering::SeqCst);
                let after = body.get("filters").and_then(|f| f[2].as_str());
                let mut candidates: Vec<_> = r
                    .iter()
                    .filter(|row| after.is_none_or(|id| row["id"].as_str().unwrap() > id))
                    .collect();
                candidates.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
                let page: Vec<Value> = candidates
                    .into_iter()
                    .take(body["top_k"].as_u64().unwrap() as usize)
                    .map(|row| {
                        if let Some(fields) = body["include_attributes"].as_array() {
                            Value::Object(
                                row.as_object()
                                    .unwrap()
                                    .iter()
                                    .filter(|(name, _)| {
                                        name.as_str() == "id"
                                            || fields.iter().any(|f| f.as_str() == Some(name))
                                    })
                                    .map(|(k, v)| (k.clone(), v.clone()))
                                    .collect(),
                            )
                        } else {
                            row.clone()
                        }
                    })
                    .collect();
                let out = json!({"rows":page});
                b.fetch_add(
                    serde_json::to_vec(&out).unwrap().len(),
                    AtomicOrdering::SeqCst,
                );
                (axum::http::StatusCode::OK, axum::Json(out))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let client = HttpTurbopufferClient::new("fixture", &format!("http://{addr}"));
    let mut scan = budget(5000);
    scan.max_bytes = 268_435_456;
    let mut request = LatestRequest {
        latest_by: latest(),
        scan,
        filters: Some(json!([
            "And",
            [
                ["active", "Eq", true],
                ["Not", ["metrics.tokens", "Eq", 900]]
            ]
        ])),
        aggregate_by: BTreeMap::from([
            ("n".into(), json!(["Count"])),
            ("sum".into(), json!(["Sum", "metrics.tokens"])),
        ]),
        grouped: BTreeMap::new(),
    };
    let start = std::time::Instant::now();
    let before = client
        .latest_relation_source("fixture", &request.scan)
        .await
        .unwrap();
    let before_bytes = before.bytes;
    let before_out = request.execute(before, "fixture".into()).unwrap();
    let before_ms = start.elapsed().as_micros();
    let before_scanned = scanned.swap(0, AtomicOrdering::SeqCst);
    assert_eq!(before_bytes, response_bytes.swap(0, AtomicOrdering::SeqCst));
    assert_eq!(requests.lock().unwrap().len(), 3);
    requests.lock().unwrap().clear();
    let start = std::time::Instant::now();
    let after = client
        .latest_relation_source_for_request("fixture", &request)
        .await
        .unwrap();
    let after_bytes = after.bytes;
    assert!(after.rows.iter().all(|r| r.get("payload").is_none()));
    assert!(after.rows.iter().any(|r| r.get("metrics.tokens").is_none()));
    assert!(after
        .rows
        .iter()
        .any(|r| r.get("metrics.tokens") == Some(&Value::Null)));
    let after_out = request.execute(after, "fixture".into()).unwrap();
    let after_ms = start.elapsed().as_micros();
    assert_eq!(before_out["aggregations"], after_out["aggregations"]);
    assert_eq!(after_out["aggregations"]["n"], 800);
    assert_eq!(after_out["relation"]["source_rows"], 2402);
    assert_eq!(after_out["relation"]["source_pages"], 3);
    assert_eq!(after_out["relation"]["consistency"], "observed");
    assert_eq!(before_scanned, scanned.load(AtomicOrdering::SeqCst));
    assert_eq!(after_bytes, response_bytes.load(AtomicOrdering::SeqCst));
    assert!(after_bytes * 100 < before_bytes);
    let expected = json!(["active", "entity", "id", "metrics.tokens", "version"]);
    assert!(requests
        .lock()
        .unwrap()
        .iter()
        .all(|q| q["include_attributes"] == expected));
    eprintln!("projection_outbound_first={}", requests.lock().unwrap()[0]);
    eprintln!("projection_matched rows=2402 queries_before=3 queries_after=3 synthetic_rows_examined_before={before_scanned} synthetic_rows_examined_after={} decoded_bytes_before={before_bytes} decoded_bytes_after={after_bytes} latency_us_before={before_ms} latency_us_after={after_ms} output_bytes_before={} output_bytes_after={} totals={} backend_plan=synthetic_full_id_range_scan_sort_limit_attribute_projection vendor_physical_bytes=unmeasured billing=unmeasured", scanned.load(AtomicOrdering::SeqCst),before_out.to_string().len(),after_out.to_string().len(),after_out["aggregations"]);
    // Grouped plans must still request complete rows, including unused payload.
    request.grouped = serde_json::from_value(json!({"g":{"group_by":[{"kind":"field","field":"entity"}],"metrics":{"n":{"kind":"count"}},"page_size":1}})).unwrap();
    assert!(request.source_projection().unwrap().is_none());
    requests.lock().unwrap().clear();
    let full = client
        .latest_relation_source_for_request("fixture", &request)
        .await
        .unwrap();
    assert!(full.rows.iter().all(|r| r.get("payload").is_some()));
    assert!(requests
        .lock()
        .unwrap()
        .iter()
        .all(|q| q["include_attributes"] == true));
    request.grouped.clear();
    reject.store(true, AtomicOrdering::SeqCst);
    let calls = requests.lock().unwrap().len();
    let error = client
        .latest_relation_source_for_request("fixture", &request)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("projection unsupported"));
    assert_eq!(
        requests.lock().unwrap().len(),
        calls + 1,
        "no hidden full scan retry"
    );
    task.abort();
}

#[tokio::test]
async fn request_aware_source_default_preserves_existing_mock_implementation() {
    let client = crate::turbopuffer::MockTurbopufferClient::default();
    let request = LatestRequest {
        latest_by: latest(),
        scan: budget(10),
        filters: None,
        aggregate_by: BTreeMap::from([("n".into(), json!(["Count"]))]),
        grouped: BTreeMap::new(),
    };
    let data = client
        .latest_relation_source_for_request("empty", &request)
        .await
        .unwrap();
    assert!(data.rows.is_empty());
}
