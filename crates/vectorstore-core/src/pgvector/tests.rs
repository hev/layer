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

#[test]
fn rank_by_orderings_and_id_sort_keys() {
    assert_eq!(
        ordering(&json!(["ts", "desc"])).unwrap(),
        Some(vec![("ts".into(), true)])
    );
    assert_eq!(
        ordering(&json!([["a", "asc"], ["id", "desc"]])).unwrap(),
        Some(vec![("a".into(), false), ("id".into(), true)])
    );
    // Ranking operators are not orderings.
    for rank in [
        json!(["vector", "ANN", [1, 0]]),
        json!(["text", "BM25", "q"]),
        json!(["vectors", "ANN", [[1, 0]]]),
        json!(["ts", "sideways"]),
    ] {
        assert_eq!(ordering(&rank).unwrap(), None, "{rank}");
    }
    assert!(ordering(&json!([["a", "asc"], ["b", "up"]])).is_err());
    assert!(ordering(&json!(vec![json!(["a", "asc"]); 9])).is_err());
    // Integer keys compare numerically as text and sort before strings.
    let keys: Vec<String> = [
        json!(9),
        json!(10),
        json!(u64::MAX),
        json!("10"),
        json!("9"),
    ]
    .iter()
    .map(|id| id_order_key(id).unwrap())
    .collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);
    assert!(id_order_key(&json!(-1)).is_err());
    assert!(id_order_key(&json!("")).is_err());
}

fn ids(result: &Value) -> Vec<Value> {
    result["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].clone())
        .collect()
}

/// LYR-112 against the pinned ParadeDB service, like the test above:
/// PGVECTOR_TEST_URL=postgresql://... cargo test -p vectorstore-core --features pgvector pgvector_live -- --ignored
#[tokio::test]
#[ignore = "requires pinned ParadeDB; set PGVECTOR_TEST_URL"]
async fn pgvector_live_ordered_scan_and_conditional_writes() {
    let url = std::env::var("PGVECTOR_TEST_URL").expect("PGVECTOR_TEST_URL required");
    let a = PgvectorClient::connect(&url, "test/lyr112/store")
        .await
        .unwrap();
    let ns = format!("scratch-lyr112-rust-{}", std::process::id());
    let result: Result<()> = async {
        // Byte order puts "Zed" before "alpha"; en_US collation would not.
        a.write(&ns, &json!({"upsert_rows":[
            {"id":"b","ts":30,"title":"alpha","vector":[1,0]},
            {"id":"a","ts":10,"title":"Zed","vector":[0,1]},
            {"id":"c","ts":20,"title":"beta"},
            {"id":"d","title":"gamma"},
            {"id":10,"ts":20,"title":"ten"},
            {"id":9,"ts":null,"title":"nine"}
        ]})).await?;
        let q = |body: Value| {
            let (a, ns) = (&a, &ns);
            async move { a.query_wire(ns, &body).await }
        };
        // Filter-only: id ascending, unsigned integers before strings.
        let found = q(json!({"top_k":10})).await?;
        assert_eq!(ids(&found), vec![json!(9), json!(10), json!("a"), json!("b"), json!("c"), json!("d")]);
        assert!(found["rows"][0].get("$dist").is_none());
        assert_eq!(found["rows"][0].as_object().unwrap().len(), 1, "default projection is id only");
        // Attribute order: nulls/missing first ascending, id breaks ties.
        let found = q(json!({"rank_by":["ts","asc"],"limit":10,"include_attributes":["ts"]})).await?;
        assert_eq!(ids(&found), vec![json!(9), json!("d"), json!("a"), json!(10), json!("c"), json!("b")]);
        assert!(found["rows"][0].get("$dist").is_none());
        assert_eq!(found["rows"][2]["ts"], json!(10));
        // Descending: nulls last; top_k cuts after ordering.
        let found = q(json!({"rank_by":["ts","desc"],"top_k":3})).await?;
        assert_eq!(ids(&found), vec![json!("b"), json!(10), json!("c")]);
        // Filters compose with ordering.
        let found = q(json!({"rank_by":["ts","desc"],"filters":["ts","Lt",30],"top_k":10})).await?;
        assert_eq!(ids(&found), vec![json!(10), json!("c"), json!("a")]);
        // Strings order and compare in byte order.
        let found = q(json!({"rank_by":["title","asc"],"filters":["title","Lt","b"],"top_k":10})).await?;
        assert_eq!(ids(&found), vec![json!("a"), json!("b")]);
        // Multiple keys.
        let found = q(json!({"rank_by":[["ts","desc"],["id","desc"]],"filters":["ts","Eq",20],"top_k":10})).await?;
        assert_eq!(ids(&found), vec![json!("c"), json!(10)]);
        let found = q(json!({"rank_by":["id","desc"],"top_k":2})).await?;
        assert_eq!(ids(&found), vec![json!("d"), json!("c")]);
        // Pagination: advance an id filter, as the Turbopuffer docs describe.
        let mut seen = vec![];
        let mut last: Option<Value> = None;
        loop {
            let mut body = json!({"rank_by":["id","asc"],"top_k":4});
            if let Some(last) = &last {
                body["filters"] = json!(["id","Gt",last]);
            }
            let page = ids(&q(body).await?);
            if page.is_empty() { break; }
            last = page.last().cloned();
            seen.extend(page);
        }
        assert_eq!(seen, vec![json!(9), json!(10), json!("a"), json!("b"), json!("c"), json!("d")]);
        // Malformed or unsupported ordering is rejected before any SQL.
        for (body, unsupported) in [
            (json!({"rank_by":["missing","asc"]}), false),
            (json!({"rank_by":["vector","asc"]}), false),
            (json!({"rank_by":[["ts","asc"],["title","sideways"]]}), false),
            (json!({"rank_by":["ts","asc"],"cursor":"x"}), true),
        ] {
            let error = q(body).await.unwrap_err().to_string();
            assert_eq!(error.contains("UnsupportedByStore"), unsupported, "{error}");
        }
        // scan_page walks the same order with an opaque cursor.
        let mut cursor = None;
        let mut scanned = vec![];
        loop {
            let page = a.scan_page(&ns, cursor.as_deref(), 4, Some(&json!(["title","NotEq","gamma"])), None).await?;
            scanned.extend(page.documents.iter().map(|d| (d.id.clone(), d.attributes.contains_key("title"))));
            cursor = page.next_cursor;
            if cursor.is_none() { break; }
        }
        assert_eq!(scanned.iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>(), ["9", "10", "a", "b", "c"]);
        assert!(scanned.iter().all(|(_, title)| *title));
        // Both order keys are index-backed.
        let mut tx = a.begin(&ns).await?;
        let n = a.required(&mut tx, &ns).await?;
        sqlx::query("SET LOCAL enable_seqscan=off").execute(&mut *tx).await.map_err(db)?;
        for order in [format!("{} DESC", id_order("data")), format!("{} DESC NULLS LAST", quoted(&n.schema.get("ts").unwrap().column()))] {
            let plan: Vec<String> = sqlx::query_scalar(&format!("EXPLAIN SELECT data FROM layer_pgvector.{} ORDER BY {order} LIMIT 10", quoted(&n.table)))
                .fetch_all(&mut *tx).await.map_err(db)?;
            let plan = plan.join("\n");
            assert!(plan.contains("Index Scan Backward"), "{order}: {plan}");
        }
        tx.commit().await.map_err(db)?;

        // Conditional upserts: the documented version check.
        let cw = format!("{ns}-cw");
        a.write(&cw, &json!({"upsert_rows":[{"id":101,"version":2,"title":"v2"},{"id":102,"version":5,"title":"v5"}]})).await?;
        let written = a.write(&cw, &json!({
            "upsert_rows":[{"id":101,"version":3,"title":"v3"},{"id":102,"version":4,"title":"v4"},{"id":103,"version":1,"title":"v1"}],
            "upsert_condition":["version","Lt",{"$ref_new":"version"}]
        })).await?;
        assert_eq!(written["rows_affected"], json!(2), "{written}");
        let titles = |r: Value| r["rows"].as_array().unwrap().iter().map(|r| r["title"].clone()).collect::<Vec<_>>();
        let all = json!({"rank_by":["id","asc"],"include_attributes":["title"]});
        assert_eq!(titles(a.query_wire(&cw, &all).await?), vec![json!("v3"), json!("v5"), json!("v1")]);
        // Insert-only: an existing id is skipped, a new one is written.
        let written = a.write(&cw, &json!({"upsert_rows":[{"id":101,"title":"again"},{"id":104,"title":"new"}],"upsert_condition":["id","Eq",null]})).await?;
        assert_eq!(written["rows_affected"], json!(1));
        // A missing attribute on the stored row compares as null (false).
        let written = a.write(&cw, &json!({"upsert_rows":[{"id":104,"version":9,"title":"v9"}],"upsert_condition":["version","Lt",{"$ref_new":"version"}]})).await?;
        assert_eq!(written["rows_affected"], json!(0));
        // Delete conditions see $ref_new as null and skip missing ids.
        let written = a.write(&cw, &json!({"deletes":[101,102,999],"delete_condition":["version","Gte",4]})).await?;
        assert_eq!(written["rows_deleted"], json!(1));
        let written = a.write(&cw, &json!({"deletes":[101],"delete_condition":["title","Eq",{"$ref_new":"title"}]})).await?;
        assert_eq!(written["rows_deleted"], json!(0));
        assert_eq!(ids(&a.query_wire(&cw, &json!({})).await?), vec![json!(101), json!(103), json!(104)]);
        // A rejected condition leaves no effects, including schema changes.
        for condition in [json!(["version","Lt",{"$ref_new":"title"}]), json!(["version","Regex","x"]), json!(["nope","Eq",1])] {
            assert!(a.write(&cw, &json!({"schema":{"added":"string"},"upsert_rows":[{"id":105}],"upsert_condition":condition})).await.is_err());
        }
        assert!(a.metadata(&cw).await?["schema"].get("added").is_none());
        assert_eq!(a.metadata(&cw).await?["approx_row_count"], json!(3));
        assert!(q(json!({"filters":["ts","Eq",{"$ref_new":"ts"}]})).await.is_err());
        Ok(())
    }.await;
    for namespace in [ns.clone(), format!("{ns}-cw")] {
        let response = a.delete_namespace(&namespace).await.unwrap();
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

/// The two hybrid bodies clients actually send: kit's multi-query + RRF, and
/// the legs the gateway issues for a `HybridText` expansion.
fn hybrid_route_body(route: crate::capabilities::HybridRoute) -> Value {
    use crate::capabilities::HybridRoute;
    match route {
        HybridRoute::HybridText => json!({"rank_by":["text","HybridText","database"],"top_k":5}),
        HybridRoute::MultiQuery => json!({
            "queries":[
                {"rank_by":["vector","ANN",[1,0]],"top_k":5},
                {"rank_by":["text","BM25","database"],"top_k":5}
            ],
            "rerank_by":["RRF"]
        }),
    }
}

/// LYR-85: the declared per-route hybrid answer must equal what the adapter
/// does with the request. No database: a route the adapter accepts gets as far
/// as the (unreachable) pool; a route it rejects is a 422 before any SQL.
#[tokio::test]
async fn declared_hybrid_route_coverage_matches_adapter_behavior() {
    use crate::capabilities::{HybridRoute, Support};
    let client = PgvectorClient {
        pool: sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(std::time::Duration::from_millis(500))
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap(),
        scope: "lyr85-route-test".into(),
    };
    for &route in HybridRoute::ALL {
        let body = hybrid_route_body(route);
        assert_eq!(HybridRoute::for_query_body(&body), Some(route));
        let declared = client.capabilities().hybrid_route(route);
        // What the gateway sends the store for this route.
        let rejection = match route {
            HybridRoute::MultiQuery => {
                let response = client
                    .passthrough("POST", "/v2/namespaces/ns/query", None, Some(body))
                    .await
                    .expect("wire failures are responses");
                assert_eq!(response.status, 422);
                let body: Value = serde_json::from_slice(&response.body).unwrap();
                assert_eq!(body["error"], "UnsupportedByStore");
                Some(body["message"].as_str().unwrap().to_owned())
            }
            HybridRoute::HybridText => {
                let mut rejected = None;
                for leg in [
                    json!(["text", "BM25", "database"]),
                    json!(["vector", "ANN", [1, 0]]),
                ] {
                    let error = client
                        .ranked_query("ns", &leg, 5, None, None)
                        .await
                        .unwrap_err()
                        .to_string();
                    if error.contains("UnsupportedByStore") {
                        rejected = Some(error);
                    } else {
                        assert!(error.contains("database operation failed"), "{error}");
                    }
                }
                rejected
            }
        };
        assert_eq!(
            declared.support == Support::Unsupported,
            rejection.is_some(),
            "{}: declared {:?}, runtime rejection {rejection:?}",
            route.id(),
            declared.support
        );
        if declared.support == Support::Approximate {
            // The limit the gateway enforces must be readable from the cell.
            assert!(
                declared.note.contains("fuzziness"),
                "{}: note must name the fuzziness limit: {:?}",
                route.id(),
                declared.note
            );
        }
        if let Some(message) = rejection {
            assert!(
                message.contains(route.feature().id()),
                "422 must name the declared feature {}: {message}",
                route.feature().id()
            );
        }
    }
}

#[tokio::test]
async fn multi_query_rejection_names_capability_and_wire_key() {
    let client = PgvectorClient {
        pool: sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap(),
        scope: "lyr85-wire-key-test".into(),
    };
    for (body, key) in [
        (json!({"queries": []}), "queries"),
        (json!({"rerank_by": ["RRF"]}), "rerank_by"),
        (
            hybrid_route_body(crate::capabilities::HybridRoute::MultiQuery),
            "queries",
        ),
    ] {
        let response = client
            .passthrough("POST", "/v2/namespaces/ns/query", None, Some(body))
            .await
            .unwrap();
        assert_eq!(response.status, 422);
        let body: Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["error"], "UnsupportedByStore");
        assert!(
            body["message"].as_str().unwrap().ends_with(&format!(
                "UnsupportedByStore: pgvector: multi_query (wire key: {key})"
            )),
            "{body}"
        );
    }
}
