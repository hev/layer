//! Conformance suite for [`S3Client`] + [`WitnessObjectStore`] on
//! [`ObjectStoreClient`]. Every backend must behave the same for the
//! operations Layer's durable state relies on.
//!
//! The in-memory case runs with `cargo test`. The real backends are opt-in
//! (`--ignored`) and fail loudly when their endpoint is missing; run them with
//! `scripts/object-store-conformance.sh`, or by hand:
//!
//! ```sh
//! docker compose up -d minio minio-init azurite
//! AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
//! LAYER_CONFORMANCE_S3_ENDPOINT=http://127.0.0.1:9000 \
//! AZURE_STORAGE_USE_EMULATOR=true AZURITE_BLOB_STORAGE_URL=http://127.0.0.1:10000 \
//!   cargo test -p vectorstore-core --test object_store_conformance -- --ignored
//! ```

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use vectorstore_core::blob_store::{ObjectStoreClient, ObjectStoreUrl, S3Settings};
use vectorstore_core::s3::{S3Client, WitnessObjectStore};

fn run_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("conformance-{nanos:x}-{}", std::process::id())
}

/// The trait contract, against one fresh prefix.
async fn conformance(client: &ObjectStoreClient) {
    let backend = format!("{:?}", client.backend());

    // get / delete on a missing key.
    assert_eq!(
        client.get("missing").await.unwrap(),
        None,
        "{backend}: get missing"
    );
    client
        .delete_key("missing")
        .await
        .unwrap_or_else(|e| panic!("{backend}: delete missing: {e}"));

    // put + get, then overwrite.
    client.put("a/one", b"first".to_vec()).await.unwrap();
    assert_eq!(
        client.get("a/one").await.unwrap().as_deref(),
        Some(&b"first"[..])
    );
    client.put("a/one", b"second".to_vec()).await.unwrap();
    assert_eq!(
        client.get("a/one").await.unwrap().as_deref(),
        Some(&b"second"[..]),
        "{backend}: unconditional put replaces"
    );

    // create-only put: first wins, a conflict reports false and keeps the body.
    assert!(client
        .put_if_not_exists("a/create", b"v1".to_vec())
        .await
        .unwrap());
    assert!(
        !client
            .put_if_not_exists("a/create", b"v2".to_vec())
            .await
            .unwrap(),
        "{backend}: create-only conflict"
    );
    assert_eq!(
        client.get("a/create").await.unwrap().as_deref(),
        Some(&b"v1"[..])
    );

    // list by prefix: sorted, relative, a string prefix (not a path segment).
    for key in ["a/two", "a/twenty/x", "b/one", "ab"] {
        client.put(key, key.as_bytes().to_vec()).await.unwrap();
    }
    assert_eq!(
        client.list_keys("a/").await.unwrap(),
        vec!["a/create", "a/one", "a/twenty/x", "a/two"],
        "{backend}: list a/"
    );
    assert_eq!(
        client.list_keys("a/tw").await.unwrap(),
        vec!["a/twenty/x", "a/two"],
        "{backend}: list partial segment"
    );
    assert_eq!(
        client.list_keys("a").await.unwrap(),
        vec!["a/create", "a/one", "a/twenty/x", "a/two", "ab"],
        "{backend}: list bare prefix"
    );
    assert_eq!(client.list_keys("").await.unwrap().len(), 6);
    assert!(client.list_keys("zzz").await.unwrap().is_empty());

    // delete one.
    client.delete_key("ab").await.unwrap();
    assert_eq!(client.get("ab").await.unwrap(), None, "{backend}: delete");

    // batch delete, including a key that does not exist.
    let batch: Vec<String> = (0..300).map(|i| format!("batch/{i:04}")).collect();
    for key in &batch {
        client.put(key, vec![1]).await.unwrap();
    }
    assert_eq!(client.list_keys("batch/").await.unwrap().len(), 300);
    let mut doomed = batch.clone();
    doomed.push("batch/never-written".into());
    client
        .delete_keys(&doomed)
        .await
        .unwrap_or_else(|e| panic!("{backend}: batch delete: {e}"));
    assert!(
        client.list_keys("batch/").await.unwrap().is_empty(),
        "{backend}: batch delete removed everything"
    );
    assert!(client
        .delete_keys(&vec!["x".to_string(); 1001])
        .await
        .is_err());

    // witness: create-only, CAS success, CAS conflict.
    assert!(client.witness_get("w/ledger").await.unwrap().is_none());
    assert!(client
        .witness_cas("w/ledger", None, b"gen-1".to_vec())
        .await
        .unwrap());
    assert!(
        !client
            .witness_cas("w/ledger", None, b"again".to_vec())
            .await
            .unwrap(),
        "{backend}: witness create-only conflict"
    );
    let first = client.witness_get("w/ledger").await.unwrap().unwrap();
    assert_eq!(first.bytes, b"gen-1");
    assert!(!first.etag.is_empty());
    assert!(
        client
            .witness_cas("w/ledger", Some(&first.etag), b"gen-2".to_vec())
            .await
            .unwrap(),
        "{backend}: CAS with current ETag"
    );
    let second = client.witness_get("w/ledger").await.unwrap().unwrap();
    assert_eq!(second.bytes, b"gen-2");
    assert_ne!(second.etag, first.etag);
    assert!(
        !client
            .witness_cas("w/ledger", Some(&first.etag), b"stale".to_vec())
            .await
            .unwrap(),
        "{backend}: CAS with stale ETag"
    );
    assert_eq!(
        client.witness_get("w/ledger").await.unwrap().unwrap().bytes,
        b"gen-2"
    );

    // Clean up the run's prefix.
    let leftovers = client.list_keys("").await.unwrap();
    client.delete_keys(&leftovers).await.unwrap();
    assert!(client.list_keys("").await.unwrap().is_empty());
}

#[tokio::test]
async fn in_memory_conformance() {
    let client = ObjectStoreClient::from_store(
        Arc::new(object_store::memory::InMemory::new()),
        ObjectStoreUrl::parse(&format!("s3://memory/{}", run_id())).unwrap(),
    );
    conformance(&client).await;
}

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is required for this conformance run"))
}

/// MinIO (S3 API). Uses the bucket `docker compose`'s `minio-init` creates.
#[tokio::test]
#[ignore = "needs MinIO: LAYER_CONFORMANCE_S3_ENDPOINT + AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY"]
async fn minio_conformance() {
    let endpoint = required("LAYER_CONFORMANCE_S3_ENDPOINT");
    let bucket =
        std::env::var("LAYER_CONFORMANCE_S3_BUCKET").unwrap_or_else(|_| "hevlayer-wal".into());
    let client = ObjectStoreClient::new(
        ObjectStoreUrl::parse(&format!("s3://{bucket}/{}", run_id())).unwrap(),
        &S3Settings {
            region: Some("us-east-1".into()),
            endpoint: Some(endpoint),
        },
    )
    .unwrap();
    conformance(&client).await;
}

/// Azurite (Azure Blob API), through the same `AZURE_*` environment AKS uses
/// (`AZURE_STORAGE_USE_EMULATOR=true` plus `AZURITE_BLOB_STORAGE_URL`).
#[tokio::test]
#[ignore = "needs Azurite: AZURE_STORAGE_USE_EMULATOR=true + AZURITE_BLOB_STORAGE_URL"]
async fn azurite_conformance() {
    let blob_url = required("AZURITE_BLOB_STORAGE_URL");
    assert_eq!(required("AZURE_STORAGE_USE_EMULATOR"), "true");
    let container = std::env::var("LAYER_CONFORMANCE_AZURE_CONTAINER")
        .unwrap_or_else(|_| "layer-conformance".into());
    azurite::create_container(&blob_url, &container).await;
    let client = ObjectStoreClient::new(
        ObjectStoreUrl::parse(&format!("az://{container}/{}", run_id())).unwrap(),
        &S3Settings::default(),
    )
    .unwrap();
    conformance(&client).await;
}

/// `object_store` cannot create containers; sign one Create Container call
/// with Azurite's well-known development account key.
mod azurite {
    use base64::Engine;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    const ACCOUNT: &str = "devstoreaccount1";
    const KEY: &str =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    const VERSION: &str = "2021-08-06";

    pub async fn create_container(blob_url: &str, container: &str) {
        let engine = base64::engine::general_purpose::STANDARD;
        let date = chrono::Utc::now()
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string();
        let string_to_sign = format!(
            "PUT\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:{date}\nx-ms-version:{VERSION}\n/{ACCOUNT}/{ACCOUNT}/{container}\nrestype:container"
        );
        let mut mac = Hmac::<Sha256>::new_from_slice(&engine.decode(KEY).unwrap()).unwrap();
        mac.update(string_to_sign.as_bytes());
        let signature = engine.encode(mac.finalize().into_bytes());
        let response = reqwest::Client::new()
            .put(format!(
                "{}/{ACCOUNT}/{container}?restype=container",
                blob_url.trim_end_matches('/')
            ))
            .header("x-ms-date", date)
            .header("x-ms-version", VERSION)
            .header("content-length", "0")
            .header("authorization", format!("SharedKey {ACCOUNT}:{signature}"))
            .send()
            .await
            .expect("reach Azurite");
        let status = response.status();
        // 201 created, 409 already exists.
        assert!(
            status.as_u16() == 201 || status.as_u16() == 409,
            "create container: {status} {}",
            response.text().await.unwrap_or_default()
        );
    }
}
