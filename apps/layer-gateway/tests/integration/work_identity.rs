//! Disposable Postgres and the standalone gateway, exercised over loopback HTTP.
use std::{process::Stdio, time::Duration};

use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Connection, PgConnection,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, Command},
};
use uuid::Uuid;

const WORKSPACE: &str = "10000000-0000-4000-8000-000000000001";
const OTHER: &str = "10000000-0000-4000-8000-000000000002";
const ABSENT: &str = "10000000-0000-4000-8000-000000000003";
const KEY: &str = "hvl_synthetic_workspace_key";
const OTHER_KEY: &str = "hvl_synthetic_other_workspace_key";
const READ_KEY: &str = "hvl_synthetic_read_key";
const WILDCARD: &str = "hvl_synthetic_wildcard_key";

const WRONG_STORE: &str = "hvl_synthetic_wrong_store_key";
const WRITE_KEY: &str = "hvl_synthetic_write_key";
const ABSENT_KEY: &str = "hvl_synthetic_absent_workspace_key";

fn grant(token: &str, store: &str, workspace: &str, scopes: &[&str]) -> Value {
    json!({"token": token, "store": store, "namespaces": [format!("work-{workspace}")], "scopes": scopes})
}

fn fixture_keys() -> Vec<Value> {
    vec![
        grant(KEY, "default", WORKSPACE, &["read", "write"]),
        grant(OTHER_KEY, "default", OTHER, &["read", "write"]),
        grant(READ_KEY, "default", WORKSPACE, &["read"]),
        grant(WILDCARD, "default", "*", &["read", "write"]),
        grant(WRONG_STORE, "other", WORKSPACE, &["read", "write"]),
        grant(WRITE_KEY, "default", WORKSPACE, &["write"]),
        grant(ABSENT_KEY, "default", ABSENT, &["read", "write"]),
    ]
}

struct Gateway {
    _child: Child,
    client: Client,
    endpoint: String,
}

impl Gateway {
    async fn start(keys: &[Value]) -> Self {
        let file = tempfile::NamedTempFile::new().unwrap();
        serde_json::to_writer(file.as_file(), keys).unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_hevlayer-gateway"))
            .env("PORT", "0")
            .env("LAYER_BIND_HOST", "127.0.0.1")
            .env("LAYER_TELEMETRY", "off")
            .env("RUST_LOG", "hevlayer_gateway=info")
            .env("NO_COLOR", "1")
            .env_remove("LAYER_STORE_FILE")
            .env("LAYER_STORE_JSON", r#"{"default":{"kind":"search","endpoint":{"url":"http://127.0.0.1:9"},"credential":{"apiKey":"synthetic-gateway-admin"},"inboundAuth":{"mode":"deriveFromStore"}}}"#)
            .env("LAYER_WORK_IDENTITY_KEYS_FILE", file.path())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        let addr = tokio::time::timeout(Duration::from_secs(15), async {
            while let Some(line) = lines.next_line().await.unwrap() {
                if line.contains("Hevlayer gateway listening") {
                    return line
                        .split_once("addr=")
                        .unwrap()
                        .1
                        .parse::<std::net::SocketAddr>()
                        .unwrap();
                }
            }
            panic!("gateway exited before listening");
        })
        .await
        .expect("gateway did not listen before timeout");
        // Drain logging for the lifetime of the child so its output pipe cannot fill.
        tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
        Self {
            _child: child,
            client: Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
            endpoint: format!("http://{addr}"),
        }
    }

    async fn health(&self) -> StatusCode {
        self.client
            .get(format!("{}/health", self.endpoint))
            .send()
            .await
            .unwrap()
            .status()
    }
}

async fn call(
    app: &Gateway,
    workspace: &str,
    key: &str,
    query: bool,
    data: Value,
) -> (StatusCode, Value) {
    let suffix = if query { "/query" } else { "" };
    let mut request = app.client.post(format!(
        "{}/v2/namespaces/work-{workspace}/work-identity{suffix}",
        app.endpoint
    ));
    if !key.is_empty() {
        request = request.bearer_auth(key);
    }
    let response = request.json(&data).send().await.unwrap();
    let status = response.status();
    (status, response.json().await.unwrap())
}

async fn write(app: &Gateway, data: Value) -> (StatusCode, Value) {
    call(app, WORKSPACE, KEY, false, data).await
}

async fn read(app: &Gateway, data: Value) -> (StatusCode, Value) {
    call(app, WORKSPACE, KEY, true, data).await
}

fn ok((status, value): (StatusCode, Value)) -> Value {
    assert_eq!(status, StatusCode::OK, "{value}");
    value
}

fn create(parent: Option<&Value>) -> Value {
    json!({"op":"create", "idempotency_key":Uuid::new_v4(), "parent_id":parent,
           "request_ref":Uuid::new_v4(), "scope_ref":Uuid::new_v4()})
}

fn resolve(address: &str) -> Value {
    json!({"op": "resolve", "address": address})
}

fn movement(ask: &Value, parent: Option<&Value>, segment: i32) -> Value {
    json!({"op":"move", "id":ask["id"], "expected_revision":ask["revision"],
           "parent_id":parent, "segment":segment})
}

#[tokio::test]
#[ignore = "requires scripts/test-work-identity-postgres"]
async fn work_identity_authenticated_transactions() {
    let url = std::env::var("LAYER_WORK_IDENTITY_DATABASE_URL").unwrap();
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .unwrap();
    let keys = fixture_keys();
    let app = Gateway::start(&keys).await;
    assert_eq!(
        call(&app, WORKSPACE, "", true, json!({"op":"list"}))
            .await
            .0,
        401
    );
    for key in [KEY, WILDCARD] {
        for workspace in [OTHER, ABSENT] {
            assert_eq!(
                call(&app, workspace, key, true, json!({"op":"list"})).await,
                call(&app, workspace, key, true, resolve("hev-1")).await
            );
            assert_eq!(call(&app, workspace, key, false, create(None)).await.0, 404);
        }
    }
    assert_eq!(
        call(&app, WORKSPACE, WILDCARD, true, json!({"op":"list"}))
            .await
            .0,
        404
    );
    assert_eq!(
        call(&app, WORKSPACE, READ_KEY, false, create(None)).await.0,
        404
    );

    for key in [WRONG_STORE, WRITE_KEY] {
        assert_eq!(
            call(&app, WORKSPACE, key, true, json!({"op":"list"}))
                .await
                .0,
            404
        );
    }
    assert_eq!(
        call(&app, ABSENT, ABSENT_KEY, true, json!({"op":"list"}))
            .await
            .0,
        404
    );
    assert_eq!(
        call(&app, WORKSPACE, "hvl_unknown", true, json!({"op":"list"}))
            .await
            .0,
        401
    );
    assert_eq!(
        call(&app, WORKSPACE, READ_KEY, true, json!({"op":"list"}))
            .await
            .0,
        200
    );

    assert_eq!(
        call(
            &app,
            WORKSPACE,
            "synthetic-gateway-admin",
            true,
            json!({"op":"list"})
        )
        .await
        .0,
        401
    );

    let request = create(None);
    let (a, b) = tokio::join!(write(&app, request.clone()), write(&app, request.clone()));
    let root = ok(a);
    assert_eq!(root, ok(b));
    assert_eq!(root["address"], "hev-1");
    let mut conflict = request.clone();
    conflict["scope_ref"] = json!(Uuid::new_v4());
    let (same, different) = tokio::join!(write(&app, request.clone()), write(&app, conflict));
    assert_eq!(ok(same), root);
    assert_eq!(different.0, 409);

    let (a, b) = tokio::join!(write(&app, create(None)), write(&app, create(None)));
    let (a, b) = (ok(a), ok(b));
    assert_ne!(a["address"], b["address"]);
    let other_root = if a["address"] == "hev-2" { a } else { b };
    assert_eq!(other_root["address"], "hev-2");

    let original = create(None);
    let mut competing = original.clone();
    competing["request_ref"] = json!(Uuid::new_v4());
    let (a, b) = tokio::join!(write(&app, original), write(&app, competing));
    match (a.0, b.0) {
        (StatusCode::OK, StatusCode::CONFLICT) | (StatusCode::CONFLICT, StatusCode::OK) => {}
        _ => panic!("expected exactly one allocation under a conflicting new key"),
    }

    let child = ok(write(&app, create(Some(&root["id"]))).await);
    for _ in 2..10 {
        ok(write(&app, create(Some(&root["id"]))).await);
    }
    let tenth = ok(write(&app, create(Some(&root["id"]))).await);
    assert_eq!(child["address"], "hev-1.1");
    assert_eq!(tenth["address"], "hev-1.10");
    let grandchild = ok(write(&app, create(Some(&child["id"]))).await);

    // Refuse cycles and current collisions.
    assert_eq!(
        write(&app, movement(&root, Some(&grandchild["id"]), 1))
            .await
            .0,
        409
    );
    assert_eq!(
        write(&app, movement(&child, Some(&root["id"]), 10)).await.0,
        409
    );

    let mut child = child;
    let evidence = Uuid::new_v4();
    for kind in ["attempt", "pr", "execution", "artifact"] {
        child = ok(write(
            &app,
            json!({"op":"link", "id":child["id"],
            "expected_revision":child["revision"], "kind":kind, "reference":evidence}),
        )
        .await);
    }
    child = ok(write(
        &app,
        json!({"op":"accept", "id":child["id"],
        "expected_revision":child["revision"], "scope_revision":1, "evidence_ref":evidence}),
    )
    .await);
    assert_eq!(child["acceptance"]["scope_revision"], 1);
    assert!(ok(read(&app, resolve("hev-1")).await)["acceptance"].is_null());
    assert_eq!(
        write(
            &app,
            json!({"op":"accept", "id":root["id"],
        "expected_revision":1, "scope_revision":1, "evidence_ref":evidence})
        )
        .await
        .0,
        409
    );

    // Fail at the descendant history insert. The parent changes, aliases, and
    // parent history have already been written inside this transaction.
    sqlx::raw_sql("CREATE FUNCTION fixture_fail_change() RETURNS trigger LANGUAGE plpgsql AS
       'BEGIN IF NEW.body->>''address'' = ''hev-2.9.1'' THEN RAISE EXCEPTION ''fixture failure''; END IF; RETURN NEW; END';
       CREATE TRIGGER fail_change BEFORE INSERT ON work_identity_change
       FOR EACH ROW EXECUTE FUNCTION fixture_fail_change();").execute(&admin).await.unwrap();
    let before = ok(read(&app, json!({"op":"history","limit":100})).await);
    let failed = write(&app, movement(&child, Some(&other_root["id"]), 9)).await;
    assert_eq!(
        failed,
        (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error":"unavailable","retryable":true})
        )
    );
    assert_eq!(ok(read(&app, resolve("hev-1.1")).await), child);
    assert_eq!(ok(read(&app, resolve("hev-1.1.1")).await), grandchild);
    assert_eq!(read(&app, resolve("hev-2.9")).await.0, 404);
    assert_eq!(read(&app, resolve("hev-2.9.1")).await.0, 404);
    assert_eq!(
        ok(read(&app, json!({"op":"history","limit":100})).await),
        before
    );
    sqlx::query("DROP TRIGGER fail_change ON work_identity_change")
        .execute(&admin)
        .await
        .unwrap();

    // Reparent, rename, promote, and reuse own aliases. Other asks cannot
    // claim retired addresses, including a retired descendant address.
    let reparented = ok(write(&app, movement(&child, Some(&other_root["id"]), 9)).await);
    assert_eq!(reparented["address"], "hev-2.9");
    let renamed = ok(write(&app, movement(&reparented, Some(&other_root["id"]), 8)).await);
    let promoted = ok(write(&app, movement(&renamed, None, 17)).await);
    assert_eq!(promoted["address"], "hev-17");
    assert_eq!(promoted["origin_parent_id"], root["id"]);
    assert_eq!(promoted["request_ref"], child["request_ref"]);
    assert_eq!(promoted["origin_address"], child["address"]);
    assert_eq!(promoted["links"], child["links"]);
    for alias in ["hev-1.1", "hev-2.9", "hev-2.8", "hev-17", "hev-1.1"] {
        assert_eq!(ok(read(&app, resolve(alias)).await), promoted);
    }
    for alias in ["hev-1.1.1", "hev-2.9.1", "hev-2.8.1", "hev-17.1"] {
        let resolved = ok(read(&app, resolve(alias)).await);
        assert_eq!(resolved["address"], "hev-17.1");
        assert_eq!(resolved["id"], grandchild["id"]);
        assert_eq!(resolved["parent_id"], promoted["id"]);
    }
    assert_eq!(ok(read(&app, resolve("hev-1.10")).await), tenth);
    assert_eq!(
        write(&app, movement(&tenth, Some(&root["id"]), 1)).await.0,
        409
    );
    assert_eq!(
        call(&app, OTHER, KEY, true, resolve("hev-1.1")).await,
        call(&app, ABSENT, KEY, true, resolve("hev-1.1")).await
    );
    assert_eq!(
        call(&app, OTHER, KEY, false, movement(&promoted, None, 19))
            .await
            .0,
        404
    );
    assert_eq!(
        ok(call(&app, WORKSPACE, READ_KEY, true, resolve("hev-1.1")).await),
        promoted
    );

    let foreign_id = json!({"op":"get", "id":promoted["id"]});
    assert_eq!(call(&app, OTHER, OTHER_KEY, true, foreign_id).await.0, 404);
    assert_eq!(
        call(&app, OTHER, OTHER_KEY, false, movement(&promoted, None, 19))
            .await
            .0,
        404
    );
    assert_eq!(
        call(&app, OTHER, OTHER_KEY, false, create(Some(&promoted["id"])))
            .await
            .0,
        404
    );
    assert!(
        ok(call(&app, OTHER, OTHER_KEY, true, json!({"op":"history"})).await)["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    let revise = |ask: &Value| json!({"op":"revise","id":ask["id"],"expected_revision":ask["revision"],"scope_ref":Uuid::new_v4()});
    let (a, b) = tokio::join!(
        write(&app, revise(&promoted)),
        write(&app, revise(&promoted))
    );
    let current = match (a.0, b.0) {
        (StatusCode::OK, StatusCode::CONFLICT) => a.1,
        (StatusCode::CONFLICT, StatusCode::OK) => b.1,
        _ => panic!("expected one committed current revision and one stale conflict"),
    };
    assert_eq!(current["scope_revision"], 2);
    assert!(current["acceptance"].is_null());
    assert_eq!(ok(write(&app, revise(&current)).await)["scope_revision"], 3);

    // The HTTP layer never accepts free-form paths or credentials as refs.
    let mut invalid = create(None);
    invalid["request_ref"] = json!("/home/synthetic/private.txt");
    invalid["scope_ref"] = json!("synthetic_secret_marker");
    let error = write(&app, invalid).await;
    assert_eq!(
        error,
        (
            StatusCode::BAD_REQUEST,
            json!({"error":"invalid_request","retryable":false})
        )
    );
    assert_eq!(
        read(&app, resolve("/home/synthetic/private.txt")).await.0,
        400
    );
    assert_eq!(read(&app, json!({"op":"history","limit":101})).await.0, 400);
    let page = ok(read(&app, json!({"op":"history","limit":2})).await);
    assert_eq!(page["items"].as_array().unwrap().len(), 2);
    assert_eq!(page["has_more"], true);
    drop(app);
    let app = Gateway::start(&keys).await;
    let page2 = ok(read(
        &app,
        json!({"op":"history","limit":100,"after":page["next_after"]}),
    )
    .await);
    assert_eq!(page2["items"][0]["cursor"], 3);
    assert!(!page2.to_string().contains("synthetic_secret_marker"));
    assert!(!page2.to_string().contains("/home/synthetic"));
    assert!(page2["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item["event"] == "accepted"));
    assert_eq!(
        ok(read(&app, json!({"op":"list","limit":1})).await)["items"][0]["id"],
        root["id"]
    );

    // Hold the lock in an independent connection. The gateway deadline and
    // server lock timeout bound the failure, which leaves no partial write.
    let mut lock = admin.begin().await.unwrap();
    sqlx::query("SELECT id FROM work_identity_workspace WHERE id = $1 FOR UPDATE")
        .bind(WORKSPACE.parse::<Uuid>().unwrap())
        .execute(&mut *lock)
        .await
        .unwrap();
    let timed = create(None);
    let start = std::time::Instant::now();
    assert_eq!(write(&app, timed.clone()).await.0, 503);
    assert!(start.elapsed() < std::time::Duration::from_secs(6));
    assert_eq!(app.health().await, StatusCode::OK);
    lock.rollback().await.unwrap();
    ok(write(&app, timed).await);

    // A root links its own execution and evidence and can accept directly.
    let root_execution = ok(write(
        &app,
        json!({"op":"link","id":root["id"],
        "expected_revision":root["revision"],"kind":"execution","reference":Uuid::new_v4()}),
    )
    .await);
    let root_evidence = Uuid::new_v4();
    let root_with_evidence = ok(write(&app, json!({"op":"link","id":root["id"],
        "expected_revision":root_execution["revision"],"kind":"artifact","reference":root_evidence})).await);
    let accepted_root = ok(write(&app, json!({"op":"accept","id":root["id"],
        "expected_revision":root_with_evidence["revision"],"scope_revision":1,"evidence_ref":root_evidence})).await);
    assert_eq!(
        accepted_root["acceptance"]["evidence_ref"],
        json!(root_evidence)
    );

    // Simulate database unavailability at the connection boundary. Keep the
    // control connection to restore the fresh database after the assertion.
    let control_options = url
        .parse::<PgConnectOptions>()
        .unwrap()
        .database("template1");
    let mut control = PgConnection::connect_with(&control_options).await.unwrap();
    sqlx::query("ALTER DATABASE postgres ALLOW_CONNECTIONS false")
        .execute(&mut control)
        .await
        .unwrap();
    sqlx::query(
        "SELECT pg_terminate_backend(pid, 5000) FROM pg_stat_activity
                 WHERE datname = 'postgres'",
    )
    .execute(&mut control)
    .await
    .unwrap();
    assert_eq!(
        call(&app, WORKSPACE, "", true, json!({"op":"list"})).await,
        (
            StatusCode::UNAUTHORIZED,
            json!({"error":"unauthorized","retryable":false})
        )
    );
    assert_eq!(
        call(&app, WORKSPACE, WRONG_STORE, true, json!({"op":"list"})).await,
        (
            StatusCode::NOT_FOUND,
            json!({"error":"not_found","retryable":false})
        )
    );
    let unavailable = create(None);
    assert_eq!(
        write(&app, unavailable.clone()).await,
        (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error":"unavailable","retryable":true})
        )
    );
    assert_eq!(app.health().await, StatusCode::OK);
    sqlx::query("ALTER DATABASE postgres ALLOW_CONNECTIONS true")
        .execute(&mut control)
        .await
        .unwrap();
    let committed = ok(write(&app, unavailable.clone()).await);
    assert_eq!(committed, ok(write(&app, unavailable).await));
}

#[tokio::test]
#[ignore = "requires scripts/test-work-identity-postgres -- /path/to/client-check"]
async fn work_identity_fixture_client() {
    let executable = std::env::var_os("WORK_IDENTITY_FIXTURE_CLIENT").unwrap();
    let key = format!("hvl_{}", Uuid::new_v4().simple());
    let app = Gateway::start(&[grant(&key, "default", WORKSPACE, &["read", "write"])]).await;
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(120),
        tokio::process::Command::new(executable)
            .env_remove("LAYER_WORK_IDENTITY_DATABASE_URL")
            .env_remove("LAYER_WORK_IDENTITY_KEYS_FILE")
            .env("WORK_IDENTITY_FIXTURE_LAYER_URL", &app.endpoint)
            .env(
                "WORK_IDENTITY_FIXTURE_NAMESPACE",
                format!("work-{WORKSPACE}"),
            )
            .env("WORK_IDENTITY_FIXTURE_BEARER", key)
            .kill_on_drop(true)
            .status(),
    )
    .await;
    assert!(result.expect("fixture client timed out").unwrap().success());
}
