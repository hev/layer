use super::*;

#[test]
fn namespace_identifiers_include_scope_and_complete_name() {
    assert_ne!(
        table_name("tenant-a/store", "same"),
        table_name("tenant-b/store", "same")
    );
    assert_ne!(
        table_name("tenant/store", &"x".repeat(100)),
        table_name("tenant/store", &format!("{}y", "x".repeat(100)))
    );
    assert!(table_name("scope", "'; DROP TABLE x;--")
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_'));
    assert_ne!(id_key(&json!(7)).unwrap(), id_key(&json!("7")).unwrap());
}

#[test]
fn schema_preflights_types_and_dimensions() {
    let schema = Schema::parse(
        &json!({"vector":"[2]f32", "n":"uint", "text":{"type":"string","full_text_search":true}}),
    )
    .unwrap();
    for row in [
        json!({"id":"a","vector":[1]}),
        json!({"id":"a","n":-1}),
        json!({"id":"a","vector":[0,0]}),
    ] {
        assert!(schema.validate_row(&row, "cosine_distance").is_err());
    }
    assert!(schema
        .merge(Some(&json!({"vector":"[3]f32"})), &[])
        .is_err());
    assert!(Schema::parse(&json!({"a":"[2]f32","b":"[2]f32"}))
        .unwrap_err()
        .to_string()
        .contains("multiple vector"));
    assert!(rows_for_test().is_err());
}
fn rows_for_test() -> Result<Vec<Value>> {
    schema::rows(&json!({"upsert_columns":{"id":["a","b"],"n":[1]}}))
}

/// Run against the pinned ParadeDB service. No fallback or implicit skip:
/// PGVECTOR_TEST_URL=postgresql://... cargo test -p vectorstore-core --features pgvector pgvector_live -- --ignored
#[tokio::test]
#[ignore = "requires pinned ParadeDB; set PGVECTOR_TEST_URL"]
async fn pgvector_live_isolation_atomicity_and_filtered_hnsw() {
    let url = std::env::var("PGVECTOR_TEST_URL").expect("PGVECTOR_TEST_URL required");
    let a = PgvectorClient::connect(&url, "test/tenant-a/store")
        .await
        .unwrap();
    let b = PgvectorClient::connect(&url, "test/tenant-b/store")
        .await
        .unwrap();
    let ns = format!("scratch-lyr20-20260906-rust-{}", std::process::id());
    let result: Result<()> = async {
        a.write(&ns,&json!({"schema":{"text":{"type":"string","full_text_search":true}},"upsert_rows":[{"id":"same","text":"tenant alpha","vector":[1,0],"n":1}]})).await?;
        b.write(&ns,&json!({"upsert_rows":[{"id":"same","text":"tenant beta","vector":[0,1],"n":2}]})).await?;
        assert_eq!(a.fetch(&ns,"same").await?.unwrap().attributes["text"],json!("tenant alpha"));
        assert_eq!(b.fetch(&ns,"same").await?.unwrap().attributes["text"],json!("tenant beta"));
        let err=a.write(&ns,&json!({"schema":{"uncommitted":"string"},"upsert_rows":[{"id":"bad","vector":[1,2,3]}]})).await.unwrap_err();
        assert!(!err.to_string().contains("UnsupportedByStore"));
        assert!(a.metadata(&ns).await?["schema"].get("uncommitted").is_none());
        assert!(a.fetch(&ns,"bad").await?.is_none());
        let rows: Vec<Value>=(0..1024).map(|n| {
            let angle=n as f64*std::f64::consts::TAU/1024.;
            json!({"id":format!("doc-{n}"),"vector":[angle.cos(),angle.sin()],"n":n,"text":"database"})
        }).collect();
        a.write(&ns,&json!({"upsert_rows":rows})).await?;
        let q=json!({"rank_by":["vector","ANN",[1,0]],"top_k":10,"filters":["n","Gte",1000]});
        let found=a.query_wire(&ns,&q).await?;
        let ids:Vec<_>=found["rows"].as_array().unwrap().iter().map(|v|v["id"].as_str().unwrap().to_owned()).collect();
        let expected:Vec<_>=(1014..1024).rev().map(|n|format!("doc-{n}")).collect();
        assert_eq!(ids,expected,"filtered recall@10 must be 1.0 on the deterministic fixture");
        let mut tx=a.begin(&ns).await?;
        let n=a.required(&mut tx,&ns).await?;
        let vector=n.schema.get("vector").unwrap().column();
        sqlx::query("SET LOCAL enable_seqscan=off").execute(&mut *tx).await.map_err(db)?;
        let plan:Vec<String>=sqlx::query_scalar(&format!("EXPLAIN SELECT data FROM layer_pgvector.{} ORDER BY {vector} <=> '[1,0]'::vector LIMIT 10",n.table))
            .fetch_all(&mut *tx).await.map_err(db)?;
        assert!(plan.join("\n").contains("Index Scan"),"{plan:?}");
        let indexes:Vec<String>=sqlx::query_scalar("SELECT indexdef FROM pg_indexes WHERE schemaname='layer_pgvector' AND tablename=$1")
            .bind(&n.table).fetch_all(&mut *tx).await.map_err(db)?;
        assert!(indexes.iter().any(|s|s.contains("USING hnsw")&&s.contains("vector_cosine_ops")));
        assert!(indexes.iter().any(|s|s.contains("USING bm25")));
        tx.commit().await.map_err(db)?;
        // Two simultaneous schema additions cannot overwrite each other.
        let first=json!({"schema":{"first":"string"}});
        let second=json!({"schema":{"second":"int"}});
        let (x,y)=tokio::join!(a.write(&ns,&first),a.write(&ns,&second));x?;y?;
        let schema=a.metadata(&ns).await?["schema"].clone();
        assert!(schema.get("first").is_some() && schema.get("second").is_some());
        // Long text must not accidentally acquire a size-limited B-tree index.
        a.write(&ns,&json!({"upsert_rows":[{"id":"long","text":"longword ".repeat(5000),"payload":"attribute ".repeat(5000)}]})).await?;
        assert_eq!(a.fetch(&ns,"long").await?.unwrap().attributes["text"].as_str().unwrap().len(),45000);
        let ids=a.ranked_query(&ns,&json!(["text","BM25","tenant"]),10,None,None).await?;
        assert_eq!(ids.rows[0].id,json!("same").to_string());
        Ok(())
    }.await;
    for client in [&a, &b] {
        let response = client.delete_namespace(&ns).await.unwrap();
        assert!(
            response.status == 200 || response.status == 404,
            "namespace cleanup failed"
        );
    }
    result.unwrap();
}

#[tokio::test]
async fn adapter_capabilities_match_matrix_and_default_rejections() {
    use crate::capabilities::{Support, WireFeature};
    let client = PgvectorClient {
        pool: sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap(),
        scope: "capabilities-test".into(),
    };
    let declared = crate::pgvector_capabilities::capabilities();
    assert_eq!(client.capabilities().kind, declared.kind);
    for &feature in WireFeature::ALL {
        assert_eq!(
            client.capabilities().get(feature).support,
            declared.get(feature).support
        );
    }
    assert_eq!(
        client.capabilities().get(WireFeature::Fts).support,
        Support::Supported
    );
    assert!(client.requires_native_wire("unused"));
    let error = client
        .delete_by_filter("unused", &json!({}))
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("UnsupportedByStore: pgvector: delete_by_filter"));
}
