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
        .contains("UnsupportedByStore: pgvector: max_vector_fields"));
    assert!(rows_for_test().is_err());
}

#[test]
fn schema_keeps_the_collapse_document_marker() {
    let marked = json!({"doc_id": {"type": "string", "document": true}});
    let schema = Schema::parse(&marked).unwrap();
    assert_eq!(schema.value(), marked, "metadata echoes the marker");
    for bad in [
        json!({"n": {"type": "int", "document": true}}),
        json!({"doc_id": {"type": "string", "document": "yes"}}),
    ] {
        assert!(Schema::parse(&bad).is_err(), "{bad}");
    }
}

#[test]
fn schema_accepts_several_full_text_search_fields() {
    let fts = json!({"type":"string","full_text_search":true});
    let schema = Schema::parse(&json!({"text": fts, "workdir": fts, "vector": "[2]f32"})).unwrap();
    assert_eq!(schema.fields().filter(|f| f.text).count(), 2);
    // Declaring a further text field on an existing namespace is compatible;
    // dropping full_text_search from a declared one is not.
    let grown = schema.merge(Some(&json!({"title": fts})), &[]).unwrap();
    assert_eq!(grown.fields().filter(|f| f.text).count(), 3);
    assert!(schema
        .merge(Some(&json!({"workdir": "string"})), &[])
        .is_err());
    assert!(Schema::parse(&json!({"n": {"type":"int","full_text_search":true}})).is_err());
}
#[test]
fn schema_declares_and_infers_array_types() {
    let schema = Schema::parse(&json!({"tags":"[]string","ts":{"type":"[]uint"}})).unwrap();
    assert!(schema.get("tags").unwrap().array());
    assert!(
        schema.get("tags").unwrap().scalar(),
        "arrays are not vectors"
    );
    for row in [
        json!({"id":"a","tags":["x","y"],"ts":[1,2]}),
        json!({"id":"a","tags":[],"ts":null}),
    ] {
        schema.validate_row(&row, "cosine_distance").unwrap();
    }
    for row in [
        json!({"id":"a","tags":"x"}),
        json!({"id":"a","tags":[1]}),
        json!({"id":"a","tags":["x",null]}),
        json!({"id":"a","ts":[-1]}),
    ] {
        assert!(
            schema.validate_row(&row, "cosine_distance").is_err(),
            "{row}"
        );
    }
    for kind in ["[]uuid", "[]datetime", "[][2]f32", "[]f32"] {
        assert!(Schema::parse(&json!({ "a": kind }))
            .unwrap_err()
            .to_string()
            .contains("UnsupportedByStore: pgvector: schema.type"));
    }
    assert!(Schema::parse(&json!({"a":{"type":"[]string","full_text_search":true}})).is_err());
    let inferred = Schema::default()
        .merge(
            None,
            &[json!({"id":"a","s":["x"],"i":[-1,2],"u":[1,18446744073709551615u64],"f":[1,2.5],"b":[true],"vector":[1,0]})],
        )
        .unwrap()
        .value();
    assert_eq!(
        inferred,
        json!({"s":{"type":"[]string"},"i":{"type":"[]int"},"u":{"type":"[]uint"},"f":{"type":"[]float"},"b":{"type":"[]bool"},"vector":{"type":"[2]f32"}})
    );
    for row in [json!({"id":"a","e":[]}), json!({"id":"a","m":["x",1]})] {
        assert!(Schema::default().merge(None, &[row]).is_err());
    }
}

#[test]
fn array_filters_compile_to_overlap_and_containment() {
    let schema = Schema::parse(&json!({"tags":"[]string","n":"int"})).unwrap();
    let sql = |filter: Value| {
        let mut q = QueryBuilder::<Postgres>::new("");
        filter::compile(&mut q, &schema, &filter, 0, filter::Refs::Rejected)
            .map(|_| q.sql().to_owned())
    };
    let any = sql(json!(["tags", "ContainsAny", ["Bash", "Read"]])).unwrap();
    assert!(
        any.starts_with("COALESCE(") && any.contains(" && "),
        "{any}"
    );
    assert!(any.contains("::text[]") && any.contains("$1"), "{any}");
    let not_any = sql(json!(["tags", "NotContainsAny", []])).unwrap();
    assert!(not_any.starts_with("NOT COALESCE("), "{not_any}");
    let one = sql(json!(["tags", "Contains", "Bash"])).unwrap();
    assert!(
        one.contains(" @> ARRAY[") && one.contains("::text]"),
        "{one}"
    );
    assert!(sql(json!(["tags", "NotContains", "Bash"]))
        .unwrap()
        .starts_with("NOT "));
    assert!(sql(json!(["Not", ["tags", "Contains", "x"]])).is_ok());
    // Malformed is 400, not a store gap.
    for filter in [
        json!(["n", "ContainsAny", [1]]),
        json!(["id", "Contains", "a"]),
        json!(["tags", "ContainsAny", "Bash"]),
        json!(["tags", "Contains", 7]),
        json!(["tags", "ContainsAny", ["x", null]]),
        json!(["tags", "Contains", {"$ref_new": "tags"}]),
    ] {
        let e = sql(filter.clone()).unwrap_err().to_string();
        assert!(!e.contains("UnsupportedByStore"), "{filter}: {e}");
    }
    // Scalar operators on an array attribute are a declared gap.
    let e = sql(json!(["tags", "Eq", ["x"]])).unwrap_err().to_string();
    assert!(e.contains("UnsupportedByStore: pgvector: Eq"), "{e}");
    assert!(sql(json!(["tags", "ContainsAll", ["x"]]))
        .unwrap_err()
        .to_string()
        .contains("UnsupportedByStore: pgvector: ContainsAll"));
}

#[test]
fn exclude_attributes_drops_listed_names_from_the_full_projection() {
    let schema = Schema::parse(&json!({"vector":"[2]f32","a":"string","b":"string"})).unwrap();
    let row = json!({"id":"x","vector":[1,0],"a":"1","b":"2","c":3});
    let projected = |body: Value| {
        let mut data = row.clone();
        project(&mut data, projection(&body).unwrap().as_ref(), &schema);
        data
    };
    assert_eq!(
        projected(json!({"exclude_attributes":["a","vector"]})),
        json!({"id":"x","b":"2","c":3})
    );
    // The same set as include_attributes: true, so vectors stay out.
    assert_eq!(
        projected(json!({"exclude_attributes":["b"]})),
        json!({"id":"x","a":"1","c":3})
    );
    assert_eq!(
        projected(json!({"exclude_attributes":["id","nope"]})),
        json!({"id":"x","a":"1","b":"2","c":3})
    );
    assert_eq!(
        projected(json!({"exclude_attributes":[]})),
        projected(json!({"include_attributes":true}))
    );
    assert_eq!(projected(json!({})), json!({"id":"x"}));
    for body in [
        json!({"include_attributes":["a"],"exclude_attributes":["b"]}),
        json!({"exclude_attributes":"a"}),
    ] {
        assert!(!projection(&body)
            .err()
            .unwrap()
            .to_string()
            .contains("UnsupportedByStore"));
    }
}

#[tokio::test]
async fn include_with_exclude_is_a_400_before_sql() {
    let client = PgvectorClient {
        pool: sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap(),
        scope: "projection-test".into(),
    };
    let response = client
        .passthrough(
            "POST",
            "/v2/namespaces/ns/query",
            None,
            Some(json!({"rank_by":["start","desc"],"top_k":10,"include_attributes":["a"],"exclude_attributes":["b"]})),
        )
        .await
        .unwrap();
    assert_eq!(response.status, 400);
}

/// LYR-137 and LYR-138 against the pinned ParadeDB service:
/// PGVECTOR_TEST_URL=postgresql://... cargo test -p vectorstore-core --features pgvector pgvector_live -- --ignored
#[tokio::test]
#[ignore = "requires pinned ParadeDB; set PGVECTOR_TEST_URL"]
async fn pgvector_live_array_filters_and_exclude_attributes() {
    let url = std::env::var("PGVECTOR_TEST_URL").expect("PGVECTOR_TEST_URL required");
    let a = PgvectorClient::connect(&url, "test/lyr138/store")
        .await
        .unwrap();
    let ns = format!("scratch-lyr138-rust-{}", std::process::id());
    let result: Result<()> = async {
        a.write(&ns, &json!({"schema":{"tool_names":"[]string","prompt_ts":{"type":"[]uint"}},"upsert_rows":[
            {"id":"s1","start":3,"first_prompt":"one","tool_names":["Bash","Read"],"prompt_ts":[1,2],"vector":[1,0]},
            {"id":"s2","start":2,"first_prompt":"two","tool_names":["Edit"],"prompt_ts":[18446744073709551615u64],"vector":[0,1]},
            {"id":"s3","start":1,"first_prompt":"three","tool_names":[],"vector":[1,1]},
            {"id":"s4","start":0,"first_prompt":"four","vector":[1,2],"labels":[1.5,2]}
        ]})).await?;
        let schema = a.metadata(&ns).await?["schema"].clone();
        assert_eq!(schema["labels"], json!({"type":"[]float"}));
        let q = |body: Value| {
            let (a, ns) = (&a, &ns);
            async move { a.query_wire(ns, &body).await }
        };
        // kit's session list (LYR-137).
        let found = q(json!({"rank_by":["start","desc"],"top_k":1000,"exclude_attributes":["first_prompt","vector"]})).await?;
        assert_eq!(ids(&found), vec![json!("s1"), json!("s2"), json!("s3"), json!("s4")]);
        assert_eq!(found["rows"][0], json!({"id":"s1","start":3,"tool_names":["Bash","Read"],"prompt_ts":[1,2]}));
        assert_eq!(found["rows"][1]["prompt_ts"], json!([18446744073709551615u64]));
        // Ranked queries take the projection too.
        let found = q(json!({"rank_by":["vector","ANN",[1,0]],"top_k":1,"exclude_attributes":["tool_names","prompt_ts","labels"]})).await?;
        assert_eq!(found["rows"][0]["first_prompt"], json!("one"));
        assert!(found["rows"][0].get("tool_names").is_none() && found["rows"][0].get("$dist").is_some());
        // Array filters (LYR-138). A missing or empty array contains nothing.
        let hits = |filters: Value| {
            let q = &q;
            async move { Ok::<_, TurbopufferError>(ids(&q(json!({"rank_by":["start","desc"],"top_k":10,"filters":filters})).await?)) }
        };
        assert_eq!(hits(json!(["tool_names","ContainsAny",["Bash"]])).await?, vec![json!("s1")]);
        assert_eq!(hits(json!(["tool_names","ContainsAny",["Edit","Read","Grep"]])).await?, vec![json!("s1"), json!("s2")]);
        assert_eq!(hits(json!(["tool_names","ContainsAny",[]])).await?, Vec::<Value>::new());
        assert_eq!(hits(json!(["tool_names","NotContainsAny",["Bash","Edit"]])).await?, vec![json!("s3"), json!("s4")]);
        assert_eq!(hits(json!(["tool_names","NotContainsAny",[]])).await?.len(), 4);
        assert_eq!(hits(json!(["tool_names","Contains","Read"])).await?, vec![json!("s1")]);
        assert_eq!(hits(json!(["tool_names","NotContains","Read"])).await?, vec![json!("s2"), json!("s3"), json!("s4")]);
        assert_eq!(hits(json!(["Not",["tool_names","Contains","Read"]])).await?, vec![json!("s2"), json!("s3"), json!("s4")]);
        assert_eq!(hits(json!(["prompt_ts","Contains",18446744073709551615u64])).await?, vec![json!("s2")]);
        assert_eq!(hits(json!(["labels","ContainsAny",[2]])).await?, vec![json!("s4")]);
        assert_eq!(hits(json!(["And",[["tool_names","ContainsAny",["Bash","Edit"]],["start","Lt",3]]])).await?, vec![json!("s2")]);
        // Array conditions guard writes too.
        let written = a.write(&ns, &json!({"upsert_condition":["tool_names","Contains","Edit"],"upsert_rows":[{"id":"s1","start":9},{"id":"s2","start":8}]})).await?;
        assert_eq!(written["rows_upserted"], json!(1));
        assert_eq!(hits(json!(["start","Eq",8])).await?, vec![json!("s2")]);
        let err = q(json!({"rank_by":["tool_names","asc"],"top_k":10})).await.unwrap_err();
        assert!(!err.to_string().contains("UnsupportedByStore"));
        Ok(())
    }
    .await;
    let response = a
        .passthrough("DELETE", &format!("/v2/namespaces/{ns}"), None, None)
        .await
        .unwrap();
    assert!(response.status == 200 || response.status == 404);
    result.unwrap();
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

        // A second full_text_search field on an existing namespace rebuilds the
        // single BM25 index; each field ranks by its own text.
        a.write(&ns,&json!({"schema":{"workdir":{"type":"string","full_text_search":true}},"upsert_rows":[{"id":"kit","text":"alpha notes","workdir":"tenant/kit","vector":[1,1],"n":3}]})).await?;
        let by_workdir=a.ranked_query(&ns,&json!(["workdir","BM25","tenant"]),10,None,None).await?;
        assert_eq!(by_workdir.rows.iter().map(|r|r.id.clone()).collect::<Vec<_>>(),vec![json!("kit").to_string()]);
        let by_text=a.ranked_query(&ns,&json!(["text","BM25","tenant"]),10,None,None).await?;
        assert_eq!(by_text.rows.iter().map(|r|r.id.clone()).collect::<Vec<_>>(),vec![json!("same").to_string()]);
        // Both fields declared up front on a fresh namespace.
        let fresh=format!("{ns}-fresh");
        let fts=json!({"type":"string","full_text_search":true});
        b.write(&fresh,&json!({"schema":{"text":fts,"workdir":fts},"upsert_rows":[{"id":"x","text":"database","workdir":"home"},{"id":"y","text":"home","workdir":"database"}]})).await?;
        assert_eq!(b.ranked_query(&fresh,&json!(["text","BM25","database"]),10,None,None).await?.rows[0].id,json!("x").to_string());
        assert_eq!(b.ranked_query(&fresh,&json!(["workdir","BM25","database"]),10,None,None).await?.rows[0].id,json!("y").to_string());
        b.delete_namespace(&fresh).await?;
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

/// The body of a 400, which `Display` omits.
fn body_of(error: TurbopufferError) -> String {
    match error {
        TurbopufferError::Response(r) if r.status == 400 => {
            String::from_utf8_lossy(&r.body).into_owned()
        }
        error => panic!("expected a 400: {error}"),
    }
}

/// LYR-140 against the pinned ParadeDB service, like the tests above:
/// PGVECTOR_TEST_URL=postgresql://... cargo test -p vectorstore-core --features pgvector pgvector_live -- --ignored
#[tokio::test]
#[ignore = "requires pinned ParadeDB; set PGVECTOR_TEST_URL"]
async fn pgvector_live_patches_and_filtered_writes() {
    let url = std::env::var("PGVECTOR_TEST_URL").expect("PGVECTOR_TEST_URL required");
    let a: &'static PgvectorClient = Box::leak(Box::new(
        PgvectorClient::connect(&url, "test/lyr140/store")
            .await
            .unwrap(),
    ));
    let ns = format!("scratch-lyr140-rust-{}", std::process::id());
    let result: Result<()> = async {
        let all = json!({"rank_by":["id","asc"],"top_k":100,"include_attributes":true});
        let rows = |a: &'static PgvectorClient| {
            let (ns, all) = (ns.clone(), all.clone());
            async move { Ok::<_, TurbopufferError>(a.query_wire(&ns, &all).await?["rows"].clone()) }
        };
        a.write(&ns, &json!({"upsert_rows":[
            {"id":"s1","first_prompt":"one","n":1,"vector":[1,0],"tags":["x"]},
            {"id":"s2","first_prompt":"two","n":2,"vector":[0,1]},
            {"id":"s3","first_prompt":"three","n":3,"vector":[1,1]}
        ]})).await?;
        // kit's summary patch: only the named attribute changes, a new schema
        // attribute is declared in the same write, a missing id is ignored.
        let written = a.write(&ns, &json!({
            "patch_rows":[{"id":"s1","summary":"first"},{"id":"gone","summary":"never"}],
            "schema":{"summary":{"type":"string"}}
        })).await?;
        assert_eq!(written["rows_affected"], json!(1), "{written}");
        assert_eq!(written["rows_patched"], json!(1));
        let found = rows(a).await?;
        assert_eq!(found[0], json!({"id":"s1","first_prompt":"one","n":1,"tags":["x"],"summary":"first"}));
        assert_eq!(found.as_array().unwrap().len(), 3, "a patch never creates a row");
        assert!(a.fetch_vector(&ns, "s1").await?.is_some(), "the vector survives a patch");
        // Patched attributes are filterable through their columns.
        let hit = a.query_wire(&ns, &json!({"filters":["summary","Eq","first"]})).await?;
        assert_eq!(ids(&hit), vec![json!("s1")]);
        // patch_columns, null clears; patch_condition sees stored and $ref_new.
        let written = a.write(&ns, &json!({
            "patch_columns":{"id":["s1","s2","s3"],"n":[10,1,30],"summary":[null,"second","third"]},
            "patch_condition":["n","Lt",{"$ref_new":"n"}]
        })).await?;
        assert_eq!(written["rows_patched"], json!(2), "{written}");
        let found = rows(a).await?;
        assert_eq!(found[0]["n"], json!(10));
        assert_eq!(found[0]["summary"], Value::Null);
        assert_eq!(found[1]["n"], json!(2), "failed condition keeps s2");
        assert!(found[1].get("summary").is_none());
        assert_eq!(found[2]["summary"], json!("third"));
        let hit = a.query_wire(&ns, &json!({"filters":["summary","Eq",null]})).await?;
        assert_eq!(ids(&hit), vec![json!("s1"), json!("s2")]);
        // Postgres patches a client vector, validated like an upsert.
        a.write(&ns, &json!({"patch_rows":[{"id":"s2","vector":[0.5,0.5]}]})).await?;
        assert_eq!(a.fetch_vector(&ns, "s2").await?, Some(vec![0.5, 0.5]));
        assert!(a.write(&ns, &json!({"patch_rows":[{"id":"s2","vector":[1,2,3]}]})).await.is_err());
        // patch_by_filter and delete_by_filter, combined with the other keys:
        // delete_by_filter runs first, then patch_by_filter, then upserts,
        // patches and deletes.
        let written = a.write(&ns, &json!({
            "delete_by_filter":["n","Gte",30],
            "patch_by_filter":{"filters":["n","Lt",30],"patch":{"status":"archived"}},
            "upsert_rows":[{"id":"s4","n":4,"status":"new"}],
            "patch_rows":[{"id":"s4","status":"patched"},{"id":"s3","status":"deleted first"}],
            "deletes":["s2"]
        })).await?;
        assert_eq!(written["rows_deleted"], json!(2), "{written}");
        assert_eq!(written["rows_patched"], json!(3), "{written}");
        assert_eq!(written["rows_upserted"], json!(1), "{written}");
        assert_eq!(written["rows_affected"], json!(6), "{written}");
        assert_eq!(written["rows_remaining"], json!(false));
        let found = rows(a).await?;
        assert_eq!(ids(&json!({"rows":found})), vec![json!("s1"), json!("s4")]);
        assert_eq!(found[0]["status"], json!("archived"));
        assert_eq!(found[1]["status"], json!("patched"));
        let hit = a.query_wire(&ns, &json!({"filters":["status","Eq","archived"]})).await?;
        assert_eq!(ids(&hit), vec![json!("s1")]);
        // Rejected writes change nothing: schema, rows and counts stay.
        let before = rows(a).await?;
        for body in [
            json!({"schema":{"added":"string"},"upsert_rows":[{"id":"s5"}],"patch_rows":[{"id":"s1","n":"wrong"}]}),
            json!({"schema":{"added":"string"},"deletes":["s1"],"patch_by_filter":{"filters":["n","Regex","x"],"patch":{"n":1}}}),
            json!({"schema":{"added":"string"},"deletes":["s1"],"delete_by_filter":["nope","Eq",1]}),
            json!({"schema":{"added":"string"},"deletes":["s1"],"patch_rows":[{"id":"s1","n":5}],"patch_condition":["n","Lt",{"$ref_new":"status"}]}),
            json!({"schema":{"added":"string"},"deletes":["s1"],"patch_by_filter":{"filters":["n","Eq",{"$ref_new":"n"}],"patch":{"n":1}}}),
        ] {
            assert!(a.write(&ns, &body).await.is_err(), "{body}");
        }
        assert_eq!(rows(a).await?, before);
        assert!(a.metadata(&ns).await?["schema"].get("added").is_none());
        // Past the limit without allow_partial is a 400 and rolls back.
        let mut tx = a.begin(&ns).await?;
        let n = a.required(&mut tx, &ns).await?;
        let error = filtered_target(&mut tx, &n.table, &n.schema, &json!(["n","Gte",0]), 1, false, "patch_by_filter")
            .await.err().unwrap();
        assert!(body_of(error).contains("patch_by_filter_allow_partial"));
        let every = json!(["n","Gte",0]);
        let (target, more) = filtered_target(&mut tx, &n.table, &n.schema, &every, 1, true, "patch_by_filter").await?;
        assert!(more && matches!(target, Target::First(_, 1, _)));
        tx.commit().await.map_err(db)?;
        // An embedded namespace refuses patches to its source and target.
        let embedded = format!("{ns}-embed");
        let profile = json!([{"source":"text","target":"embed_text","model":"m/x"}]);
        a.write(&embedded, &json!({
            EMBEDDING_PROFILES_KEY: profile,
            "upsert_rows":[{"id":"e1","text":"hello","embed_text":[1,0],"n":1}]
        })).await?;
        for patch in [json!({"id":"e1","text":"bye"}), json!({"id":"e1","embed_text":[0,1]})] {
            let error = body_of(a.write(&embedded, &json!({"patch_rows":[patch]})).await.unwrap_err());
            assert!(error.contains("gateway-embedded"), "{error}");
        }
        let error = body_of(a.write(&embedded, &json!({"patch_by_filter":{"filters":["n","Eq",1],"patch":{"text":"bye"}}})).await.unwrap_err());
        assert!(error.contains("gateway-embedded"), "{error}");
        assert_eq!(a.write(&embedded, &json!({"patch_rows":[{"id":"e1","n":2}]})).await?["rows_patched"], json!(1));
        a.delete_namespace(&embedded).await?;
        Ok(())
    }.await;
    for namespace in [ns.clone(), format!("{ns}-embed")] {
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
    for feature in [
        WireFeature::PatchRows,
        WireFeature::PatchColumns,
        WireFeature::DeleteByFilter,
        WireFeature::ConditionalWrites,
    ] {
        assert_eq!(
            client.capabilities().get(feature).support,
            Support::Supported
        );
    }
}

/// LYR-140: malformed patch and filtered-write bodies are 400s decided from
/// the request alone, before the transaction (the pool is unreachable).
#[tokio::test]
async fn malformed_patches_are_400_before_sql() {
    let client = PgvectorClient {
        pool: sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap(),
        scope: "lyr140-test".into(),
    };
    for body in [
        json!({"patch_rows": [{"n": 1}]}),
        json!({"patch_rows": [{"id": "", "n": 1}]}),
        json!({"patch_rows": {"id": "a"}}),
        json!({"patch_columns": {"id": ["a", "b"], "n": [1]}}),
        json!({"patch_rows": [], "patch_columns": {"id": []}}),
        json!({"patch_by_filter": {"patch": {"n": 1}}}),
        json!({"patch_by_filter": {"filters": ["n", "Eq", 1]}}),
        json!({"patch_by_filter": {"filters": ["n", "Eq", 1], "patch": {"id": "b"}}}),
        json!({"delete_by_filter": ["n", "Eq", 1], "delete_by_filter_allow_partial": "yes"}),
    ] {
        let response = client
            .passthrough("POST", "/v2/namespaces/ns", None, Some(body.clone()))
            .await
            .unwrap();
        assert_eq!(response.status, 400, "{body}");
    }
    let response = client
        .passthrough(
            "POST",
            "/v2/namespaces/ns",
            None,
            Some(json!({"patch_by_filter": {"filters": ["n", "Eq", 1], "patch": {"n": 2}, "extra": 1}})),
        )
        .await
        .unwrap();
    assert_eq!(response.status, 422);
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
            body["message"].as_str().unwrap()
                == format!("UnsupportedByStore: pgvector: multi_query: wire key: {key}"),
            "{body}"
        );
        assert_eq!(body["feature"], "multi_query", "{body}");
    }
}

/// RFC 0118 step A: every pgvector 422 carries the typed `feature`, and the
/// message is the canonical `UnsupportedByStore: pgvector: {feature}` with no
/// transport prefix.
#[tokio::test]
async fn rejections_carry_a_typed_feature() {
    let client = PgvectorClient {
        pool: sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap(),
        scope: "lyr88-feature-test".into(),
    };
    let rows = json!([{"id": "rogue", "vector": [1, 0]}]);
    for (body, feature) in [
        (
            json!({"upsert_rows": rows, "copy_from_namespace": "other"}),
            "copy_from_namespace",
        ),
        (
            json!({"upsert_rows": rows, "distance_metric": "dot_product"}),
            "distance_metric",
        ),
        (
            json!({"schema": {"a": "[2]f32", "b": "[2]f32"}}),
            "max_vector_fields",
        ),
    ] {
        let response = client
            .passthrough("POST", "/v2/namespaces/ns", None, Some(body))
            .await
            .unwrap();
        assert_eq!(response.status, 422);
        let body: Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["error"], "UnsupportedByStore");
        assert_eq!(body["store"], "pgvector");
        assert_eq!(body["route"], "/v2/namespaces/ns");
        assert_eq!(body["feature"], feature, "{body}");
        let message = body["message"].as_str().unwrap();
        assert!(
            message.starts_with(&format!("UnsupportedByStore: pgvector: {feature}")),
            "{message}"
        );
    }
    for (body, feature) in [
        (
            json!({"rank_by": ["vector", "ANN", [1, 0]], "searchAfter": "x"}),
            "search_after",
        ),
        (
            json!({"rank_by": ["vector", "ANN", [1, 0]], "group_by": ["n"]}),
            "aggregate_by",
        ),
    ] {
        let response = client
            .passthrough("POST", "/v2/namespaces/ns/query", None, Some(body))
            .await
            .unwrap();
        assert_eq!(response.status, 422);
        let body: Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["feature"], feature, "{body}");
    }
}

/// RFC 0118: the adapter itself never accepts `embed`. Since step C the
/// gateway strips it and sends vectors plus the profile under
/// `EMBEDDING_PROFILES_KEY`, so a raw declaration here means a caller
/// bypassed the gateway; it stays a 422 naming `schema.embed`, rejected
/// before any SQL, and `embed` never enters what `Schema::parse` re-reads.
#[tokio::test]
async fn schema_embed_is_rejected_as_schema_embed() {
    let error = Schema::parse(&json!({
        "text": {"type": "string", "embed": {"model": "qwen/qwen3-embedding-8b"}}
    }))
    .unwrap_err();
    let message = error.to_string();
    let rejection = crate::capabilities::StoreRejection::parse(&message).unwrap();
    assert_eq!(rejection.store, "pgvector");
    assert_eq!(rejection.feature, "schema.embed");
    assert_eq!(
        rejection.message,
        "UnsupportedByStore: pgvector: schema.embed"
    );

    let client = PgvectorClient {
        pool: sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap(),
        scope: "lyr88-embed-test".into(),
    };
    let response = client
        .passthrough(
            "POST",
            "/v2/namespaces/traces",
            None,
            Some(json!({
                "schema": {"text": {"type": "string", "embed": {"model": "qwen/qwen3-embedding-8b"}}},
                "upsert_rows": [{"id": "a", "text": "no vector here"}]
            })),
        )
        .await
        .expect("rejected before the (unreachable) pool is touched");
    assert_eq!(response.status, 422);
    let body: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(body["error"], "UnsupportedByStore");
    assert_eq!(body["feature"], "schema.embed");
    assert_eq!(
        body["message"],
        "UnsupportedByStore: pgvector: schema.embed"
    );
}

/// RFC 0118 step E: the gateway's profile value is validated before the
/// transaction. One embedded attribute per namespace, and each profile names
/// its source and target.
#[tokio::test]
async fn embedding_profiles_are_validated_before_sql() {
    let client = PgvectorClient {
        pool: sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap(),
        scope: "lyr88-profiles-test".into(),
    };
    for (profiles, expected) in [
        (
            json!([{"source": "a", "target": "embed_a"}, {"source": "b", "target": "embed_b"}]),
            Some("schema.embed"),
        ),
        (json!([{"source": "a"}]), None),
        (json!({"source": "a"}), None),
    ] {
        let response = client
            .passthrough(
                "POST",
                "/v2/namespaces/profiles",
                None,
                Some(json!({"upsert_rows": [{"id": "a"}], EMBEDDING_PROFILES_KEY: profiles})),
            )
            .await
            .expect("rejected before the (unreachable) pool is touched");
        let body: Value = serde_json::from_slice(&response.body).unwrap();
        match expected {
            Some(feature) => {
                assert_eq!(response.status, 422, "{body}");
                assert_eq!(body["feature"], feature, "{body}");
            }
            None => assert_eq!(response.status, 400, "{body}"),
        }
    }
    let mut schema = json!({"text": {"type": "string"}, "n": "int"});
    merge_embed_declarations(
        &mut schema,
        &json!([{"source": "text", "target": "embed_text", "declaration": {"model": "m"}}]),
    );
    assert_eq!(
        schema["text"],
        json!({"type": "string", "embed": {"model": "m"}})
    );
    assert_eq!(schema["n"], "int");
}

/// RFC 0124: a branch-only body gets the branch-named 422, never an emulated
/// copy, and the capability row says so. The gateway strips the SDK's
/// `stainless_overload` before dispatch, so the adapter sees this body.
#[tokio::test]
async fn branch_from_namespace_is_the_named_422() {
    let client = PgvectorClient {
        pool: sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap(),
        scope: "lyr130-branch-test".into(),
    };
    for (body, feature) in [
        (
            json!({"branch_from_namespace": "trunk"}),
            "branch_from_namespace",
        ),
        (
            json!({"branch_from_namespace": {"source_namespace": "trunk"}}),
            "branch_from_namespace",
        ),
        (
            json!({"copy_from_namespace": "trunk"}),
            "copy_from_namespace",
        ),
    ] {
        let response = client
            .passthrough("POST", "/v2/namespaces/wt-1", None, Some(body))
            .await
            .unwrap();
        assert_eq!(response.status, 422);
        let body: Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["error"], "UnsupportedByStore");
        assert_eq!(body["feature"], feature, "{body}");
        assert_eq!(
            body["message"],
            format!("UnsupportedByStore: pgvector: {feature}"),
            "{body}"
        );
    }
    let branch = client
        .capabilities()
        .get(crate::capabilities::WireFeature::Branch);
    assert_eq!(branch.support, crate::capabilities::Support::Unsupported);
    assert_eq!(branch.note, crate::capabilities::NO_NATIVE_BRANCH);
}
