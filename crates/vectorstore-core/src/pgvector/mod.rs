//! RFC 0114 phase-one adapter. SQL identifiers are generated locally; every
//! request value is bound. The registry and document tables belong exclusively
//! to this backend, never to the gateway's indexing-state database.
mod filter;
mod schema;
#[cfg(test)]
mod tests;

use crate::models::{DocumentPage, DocumentResponse, IncludeAttributes};
use crate::turbopuffer::*;
use async_trait::async_trait;
use schema::Schema;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use sqlx::{postgres::PgPoolOptions, PgPool, Postgres, QueryBuilder, Row, Transaction};
use std::collections::HashMap;

type Result<T> = std::result::Result<T, TurbopufferError>;
pub const PG_SEARCH_VERSION: &str = "0.18.0";
pub const VECTOR_VERSION: &str = "0.8.0";

pub struct PgvectorClient {
    pool: PgPool,
    scope: String,
}
struct Namespace {
    table: String,
    schema: Schema,
    metric: String,
}

fn unsupported(feature: &str) -> TurbopufferError {
    TurbopufferError::Other(format!("UnsupportedByStore: pgvector {feature}"))
}
fn invalid(message: impl Into<String>) -> TurbopufferError {
    TurbopufferError::from_status(
        reqwest::StatusCode::BAD_REQUEST,
        &json!({"error":"validation_error", "message":message.into()}).to_string(),
    )
}
fn db(error: sqlx::Error) -> TurbopufferError {
    // Do not include SQL connection options, which may contain credentials.
    TurbopufferError::Other(format!("pgvector database operation failed: {error}"))
}
fn identifier(prefix: &str, name: &str) -> String {
    format!("{prefix}{:x}", Sha256::digest(name.as_bytes()))[..62].to_string()
}
fn quoted(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}
fn response(status: u16, body: Value) -> TurbopufferPassthroughResponse {
    TurbopufferPassthroughResponse {
        status,
        content_type: Some("application/json".into()),
        body: body.to_string().into_bytes(),
    }
}
fn object(value: &Value) -> Result<&Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| invalid("expected an object"))
}
fn keys(value: &Value, allowed: &[&str]) -> Result<()> {
    for key in object(value)?.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(unsupported(key));
        }
    }
    Ok(())
}
fn id_key(id: &Value) -> Result<String> {
    if id.as_str().is_some_and(|s| !s.is_empty()) || id.as_u64().is_some() {
        Ok(id.to_string())
    } else {
        Err(invalid("id must be a nonempty string or unsigned integer"))
    }
}
fn table_name(scope: &str, namespace: &str) -> String {
    identifier("n_", &json!([scope, namespace]).to_string())
}

impl PgvectorClient {
    pub async fn connect(url: &str, scope: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .acquire_timeout(std::time::Duration::from_secs(3))
            .connect(url)
            .await
            .map_err(db)?;
        Self {
            pool: pool.clone(),
            scope: scope.into(),
        }
        .check_readiness()
        .await?;
        let mut bootstrap = pool.begin().await.map_err(db)?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('layer_pgvector:bootstrap',0))")
            .execute(&mut *bootstrap)
            .await
            .map_err(db)?;
        sqlx::query("CREATE SCHEMA IF NOT EXISTS layer_pgvector")
            .execute(&mut *bootstrap)
            .await
            .map_err(db)?;
        sqlx::query("CREATE TABLE IF NOT EXISTS layer_pgvector.namespaces (scope text NOT NULL, name text NOT NULL, table_name text NOT NULL UNIQUE, schema jsonb NOT NULL, metric text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(scope,name))")
            .execute(&mut *bootstrap).await.map_err(db)?;
        bootstrap.commit().await.map_err(db)?;
        Ok(Self {
            pool,
            scope: scope.into(),
        })
    }

    // All operations hold the namespace lock until their transaction ends. This
    // serializes schema changes with reads, writes, and deletion across processes.
    async fn begin(&self, namespace: &str) -> Result<Transaction<'_, Postgres>> {
        if namespace.is_empty() {
            return Err(invalid("namespace must not be empty"));
        }
        let mut tx = self.pool.begin().await.map_err(db)?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(table_name(&self.scope, namespace))
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        Ok(tx)
    }
    async fn load(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        namespace: &str,
    ) -> Result<Option<Namespace>> {
        let row = sqlx::query("SELECT table_name,schema,metric FROM layer_pgvector.namespaces WHERE scope=$1 AND name=$2")
            .bind(&self.scope).bind(namespace).fetch_optional(&mut **tx).await.map_err(db)?;
        row.map(|r| {
            Ok(Namespace {
                table: r.get("table_name"),
                schema: Schema::parse(&r.get::<Value, _>("schema"))?,
                metric: r.get("metric"),
            })
        })
        .transpose()
    }
    async fn required(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        namespace: &str,
    ) -> Result<Namespace> {
        self.load(tx, namespace)
            .await?
            .ok_or_else(|| TurbopufferError::NotFound(format!("namespace {namespace}")))
    }

    async fn write(&self, namespace: &str, body: &Value) -> Result<Value> {
        keys(
            body,
            &[
                "schema",
                "distance_metric",
                "upsert_rows",
                "upsert_columns",
                "deletes",
            ],
        )?;
        let rows = schema::rows(body)?;
        let deletes = match body.get("deletes") {
            None => vec![],
            Some(v) => v
                .as_array()
                .ok_or_else(|| invalid("deletes must be an array"))?
                .iter()
                .map(id_key)
                .collect::<Result<Vec<_>>>()?,
        };
        let mut tx = self.begin(namespace).await?;
        let previous = self.load(&mut tx, namespace).await?;
        let old_schema = previous
            .as_ref()
            .map(|n| n.schema.clone())
            .unwrap_or_default();
        let schema = old_schema.merge(body.get("schema"), &rows)?;
        let metric = body
            .get("distance_metric")
            .map(|v| {
                v.as_str()
                    .ok_or_else(|| invalid("distance_metric must be a string"))
            })
            .transpose()?
            .unwrap_or_else(|| {
                previous
                    .as_ref()
                    .map(|n| n.metric.as_str())
                    .unwrap_or("cosine_distance")
            });
        if !["cosine_distance", "euclidean_squared"].contains(&metric) {
            return Err(unsupported("distance_metric"));
        }
        if previous.as_ref().is_some_and(|n| n.metric != metric) {
            return Err(invalid("distance_metric cannot change"));
        }
        for row in &rows {
            schema.validate_row(row, metric)?;
        }
        let table = table_name(&self.scope, namespace);
        if previous.is_none() {
            sqlx::query(&format!(
                "CREATE TABLE layer_pgvector.\"{table}\" (rid bigserial PRIMARY KEY, key text NOT NULL UNIQUE, data jsonb NOT NULL)"
            ))
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        schema.install(&mut tx, &table, &old_schema, metric).await?;
        sqlx::query("INSERT INTO layer_pgvector.namespaces (scope,name,table_name,schema,metric) VALUES($1,$2,$3,$4,$5) ON CONFLICT(scope,name) DO UPDATE SET schema=excluded.schema,updated_at=now()")
            .bind(&self.scope).bind(namespace).bind(&table).bind(schema.value()).bind(metric).execute(&mut *tx).await.map_err(db)?;
        for row in &rows {
            let key = id_key(&row["id"])?;
            let mut sql = QueryBuilder::<Postgres>::new(format!(
                "INSERT INTO layer_pgvector.\"{table}\" (key,data"
            ));
            for field in schema.fields() {
                sql.push(",").push(quoted(&field.column()));
            }
            sql.push(") VALUES (")
                .push_bind(key)
                .push(",")
                .push_bind(row.clone());
            for field in schema.fields() {
                sql.push(",");
                field.bind(&mut sql, row.get(&field.name).unwrap_or(&Value::Null));
            }
            sql.push(") ON CONFLICT(key) DO UPDATE SET data=excluded.data");
            for field in schema.fields() {
                sql.push(",")
                    .push(quoted(&field.column()))
                    .push("=excluded.")
                    .push(quoted(&field.column()));
            }
            sql.build().execute(&mut *tx).await.map_err(db)?;
        }
        let deleted = sqlx::query(&format!(
            "DELETE FROM layer_pgvector.\"{table}\" WHERE key=ANY($1)"
        ))
        .bind(&deletes)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(
            json!({"status":"OK", "message":"write committed", "billing":{}, "rows_affected":rows.len() as u64+deleted, "rows_upserted":rows.len(), "rows_deleted":deleted}),
        )
    }

    async fn query_wire(&self, namespace: &str, body: &Value) -> Result<Value> {
        keys(
            body,
            &[
                "rank_by",
                "vector",
                "top_k",
                "limit",
                "filters",
                "include_attributes",
            ],
        )?;
        if body.get("limit").is_some() && body.get("top_k").is_some() {
            return Err(invalid("limit and top_k are mutually exclusive"));
        }
        if body.get("rank_by").is_some() && body.get("vector").is_some() {
            return Err(invalid("rank_by and vector are mutually exclusive"));
        }
        let k = body
            .get("top_k")
            .or_else(|| body.get("limit"))
            .unwrap_or(&json!(10))
            .as_u64()
            .filter(|n| *n > 0 && *n <= 10000)
            .ok_or_else(|| invalid("top_k must be between 1 and 10000"))?;
        let rank = body
            .get("rank_by")
            .cloned()
            .or_else(|| body.get("vector").map(|v| json!(["vector", "ANN", v])))
            .ok_or_else(|| unsupported("ordered_scan"))?;
        let rank = rank
            .as_array()
            .filter(|r| r.len() == 3)
            .ok_or_else(|| unsupported("rank_by expression"))?;
        let field = rank[0]
            .as_str()
            .ok_or_else(|| invalid("rank field must be a string"))?;
        let mode = rank[1]
            .as_str()
            .ok_or_else(|| invalid("rank operator must be a string"))?;
        if !["ANN", "BM25"].contains(&mode) {
            return Err(unsupported(mode));
        }
        let include: Option<IncludeAttributes> = body
            .get("include_attributes")
            .map(|v| {
                serde_json::from_value(v.clone()).map_err(|_| invalid("invalid include_attributes"))
            })
            .transpose()?;
        let mut tx = self.begin(namespace).await?;
        let ns = self.required(&mut tx, namespace).await?;
        let field = ns
            .schema
            .get(field)
            .ok_or_else(|| invalid("rank field is not in schema"))?;
        let mut sql = QueryBuilder::<Postgres>::new(if mode == "ANN" {
            "SELECT data,"
        } else {
            "WITH hits AS MATERIALIZED (SELECT *,"
        });
        if mode == "ANN" {
            field.validate_vector(&rank[2], &ns.metric)?;
            let op = if ns.metric == "cosine_distance" {
                " <=> "
            } else {
                " <-> "
            };
            sql.push(quoted(&field.column()))
                .push(op)
                .push_bind(rank[2].to_string())
                .push("::vector AS score FROM layer_pgvector.")
                .push(quoted(&ns.table))
                .push(" WHERE ")
                .push(quoted(&field.column()))
                .push(" IS NOT NULL");
            // pgvector 0.8 iterative scan continues after filters remove candidates.
            sqlx::query("SET LOCAL hnsw.iterative_scan = 'strict_order'")
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            sqlx::query("SET LOCAL hnsw.ef_search = 100")
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            sqlx::query("SET LOCAL hnsw.max_scan_tuples = 20000")
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        } else {
            if !field.text {
                return Err(invalid("BM25 requires a full_text_search field"));
            }
            let text = rank[2]
                .as_str()
                .ok_or_else(|| unsupported("BM25 expression"))?;
            sql.push("paradedb.score(rid)::float8 AS score FROM layer_pgvector.")
                .push(quoted(&ns.table))
                .push(" WHERE rid @@@ paradedb.match(")
                .push_bind(field.column())
                .push("::text,")
                .push_bind(text.to_owned())
                // Materialize BM25 scores before scalar filtering. pg_search
                // 0.18 can choose a B-tree filter plan that leaves score NULL
                // when unindexed scalar predicates share the search scan.
                .push(
                    ") ORDER BY paradedb.score(rid) DESC) SELECT data,score FROM hits WHERE TRUE",
                );
        }
        if let Some(filters) = body.get("filters").filter(|v| !v.is_null()) {
            sql.push(" AND (");
            filter::compile(&mut sql, &ns.schema, filters, 0)?;
            sql.push(")");
        }
        if mode == "ANN" {
            // Keep raw distance ascending in ORDER BY so HNSW remains usable.
            sql.push(" ORDER BY ")
                .push(quoted(&field.column()))
                .push(if ns.metric == "cosine_distance" {
                    " <=> "
                } else {
                    " <-> "
                })
                .push_bind(rank[2].to_string())
                .push("::vector");
        } else {
            sql.push(" ORDER BY score DESC");
        }
        sql.push(" LIMIT ").push_bind(k as i64);
        let rows = sql.build().fetch_all(&mut *tx).await.map_err(db)?;
        let rows: Vec<Value> = rows
            .into_iter()
            .map(|r| {
                let mut data: Value = r.get("data");
                let score: f64 = r.try_get("score").map_err(db)?;
                project(&mut data, include.as_ref(), &ns.schema);
                data["$dist"] = json!(if mode == "ANN" && ns.metric == "euclidean_squared" {
                    score * score
                } else {
                    score
                });
                Ok(data)
            })
            .collect::<Result<Vec<_>>>()?;
        tx.commit().await.map_err(db)?;
        Ok(json!({"rows":rows}))
    }

    async fn metadata(&self, namespace: &str) -> Result<Value> {
        let mut tx = self.begin(namespace).await?;
        let ns = self.required(&mut tx, namespace).await?;
        let stats = sqlx::query(&format!("SELECT count(*) AS count, COALESCE(sum(octet_length(data::text)),0)::bigint AS bytes FROM layer_pgvector.\"{}\"", ns.table))
            .fetch_one(&mut *tx).await.map_err(db)?;
        let times = sqlx::query("SELECT created_at::text,updated_at::text FROM layer_pgvector.namespaces WHERE scope=$1 AND name=$2")
            .bind(&self.scope).bind(namespace).fetch_one(&mut *tx).await.map_err(db)?;
        Ok(
            json!({"id":namespace,"schema":ns.schema.value(), "approx_row_count":stats.get::<i64,_>("count"),
            "approx_logical_bytes":stats.get::<i64,_>("bytes"),"created_at":times.get::<String,_>("created_at"),
            "updated_at":times.get::<String,_>("updated_at"),"last_write_at":times.get::<String,_>("updated_at"),
            "config":{"distance_metric":ns.metric}}),
        )
    }

    async fn fetch_data(&self, namespace: &str, ids: &[String]) -> Result<Vec<Value>> {
        let mut tx = self.begin(namespace).await?;
        let Some(ns) = self.load(&mut tx, namespace).await? else {
            return Ok(vec![]);
        };
        sqlx::query_scalar(&format!(
            "SELECT data FROM layer_pgvector.\"{}\" WHERE key=ANY($1)",
            ns.table
        ))
        .bind(
            ids.iter()
                .map(|id| json!(id).to_string())
                .collect::<Vec<_>>(),
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(db)
    }
    async fn list(&self, query: Option<&str>) -> Result<Value> {
        let mut prefix = String::new();
        let mut cursor = String::new();
        let mut size = 1000i64;
        for (key, value) in
            reqwest::Url::parse(&format!("http://localhost/?{}", query.unwrap_or_default()))
                .map_err(|_| invalid("invalid query string"))?
                .query_pairs()
        {
            match key.as_ref() {
                "prefix" => prefix = value.into_owned(),
                "cursor" => cursor = value.into_owned(),
                "page_size" => {
                    size = value
                        .parse::<i64>()
                        .ok()
                        .filter(|s| *s > 0 && *s <= 1000)
                        .ok_or_else(|| invalid("page_size must be between 1 and 1000"))?
                }
                _ => return Err(unsupported(&key)),
            }
        }
        let mut names: Vec<String> = sqlx::query_scalar("SELECT name FROM layer_pgvector.namespaces WHERE scope=$1 AND starts_with(name,$2) AND name COLLATE \"C\" > $3 COLLATE \"C\" ORDER BY name COLLATE \"C\" LIMIT $4")
            .bind(&self.scope).bind(prefix).bind(cursor).bind(size+1).fetch_all(&self.pool).await.map_err(db)?;
        let next = if names.len() > size as usize {
            names.truncate(size as usize);
            names.last().cloned()
        } else {
            None
        };
        Ok(
            json!({"namespaces":names.iter().map(|id| json!({"id":id})).collect::<Vec<_>>(), "next_cursor":next}),
        )
    }
    async fn dispatch(
        &self,
        method: &str,
        path: &str,
        query: Option<&str>,
        body: Option<Value>,
    ) -> Result<Value> {
        let parts: Vec<_> = path.trim_matches('/').split('/').collect();
        if parts.len() == 2
            && ["v1", "v2"].contains(&parts[0])
            && parts[1] == "namespaces"
            && method == "GET"
        {
            return self.list(query).await;
        }
        if parts.len() < 3 || !["v1", "v2"].contains(&parts[0]) || parts[1] != "namespaces" {
            return Err(unsupported(&format!("{method} {path}")));
        }
        if query.is_some_and(|s| !s.is_empty()) {
            return Err(unsupported("query parameters"));
        }
        let ns = percent_encoding::percent_decode_str(parts[2])
            .decode_utf8()
            .map_err(|_| invalid("invalid namespace encoding"))?;
        match (method, parts.get(3).copied(), parts.len()) {
            ("POST", None, 3) => self.write(&ns, &body.unwrap_or(Value::Null)).await,
            ("POST", Some("query"), 4) => self.query_wire(&ns, &body.unwrap_or(Value::Null)).await,
            ("GET", Some("metadata"), 4) => self.metadata(&ns).await,
            ("GET", Some("schema"), 4) => Ok(self.metadata(&ns).await?["schema"].clone()),
            ("DELETE", None, 3) => {
                let mut tx = self.begin(&ns).await?;
                let n = self.required(&mut tx, &ns).await?;
                sqlx::query(&format!("DROP TABLE layer_pgvector.\"{}\"", n.table))
                    .execute(&mut *tx)
                    .await
                    .map_err(db)?;
                sqlx::query("DELETE FROM layer_pgvector.namespaces WHERE scope=$1 AND name=$2")
                    .bind(&self.scope)
                    .bind(ns.as_ref())
                    .execute(&mut *tx)
                    .await
                    .map_err(db)?;
                tx.commit().await.map_err(db)?;
                Ok(json!({"status":"OK"}))
            }
            _ => Err(unsupported(parts.get(3).copied().unwrap_or(method))),
        }
    }
}

fn project(data: &mut Value, include: Option<&IncludeAttributes>, schema: &Schema) {
    data.as_object_mut().unwrap().retain(|key, _| {
        key == "id"
            || match include {
                Some(IncludeAttributes::All(true)) => {
                    schema.get(key).is_none_or(|field| field.scalar())
                }
                Some(IncludeAttributes::Fields(fields)) => fields.contains(key),
                _ => false,
            }
    });
}
fn doc(mut value: Value) -> DocumentResponse {
    let id = value.as_object_mut().unwrap().remove("id").unwrap();
    value.as_object_mut().unwrap().remove("vector");
    DocumentResponse {
        id: id
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| id.to_string()),
        attributes: serde_json::from_value(value).unwrap(),
    }
}

#[async_trait]
impl TurbopufferClient for PgvectorClient {
    fn capabilities(&self) -> crate::capabilities::Capabilities {
        crate::pgvector_capabilities::PGVECTOR_CAPABILITIES
    }

    async fn check_readiness(&self) -> Result<()> {
        let versions: Vec<(String, String)> = sqlx::query_as(
            "SELECT extname, extversion FROM pg_extension WHERE extname IN ('vector','pg_search')",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        for (extension, version) in [("vector", VECTOR_VERSION), ("pg_search", PG_SEARCH_VERSION)] {
            if !versions.iter().any(|(n, v)| n == extension && v == version) {
                return Err(TurbopufferError::Other(format!(
                    "pgvector requires {extension} {version}; install the pinned ParadeDB bundle"
                )));
            }
        }
        Ok(())
    }

    fn requires_native_wire(&self, _: &str) -> bool {
        true
    }
    async fn passthrough(
        &self,
        method: &str,
        path: &str,
        query: Option<&str>,
        body: Option<Value>,
    ) -> Result<TurbopufferPassthroughResponse> {
        // Return wire failures as responses: an UnsupportedByStore error would
        // activate the legacy gateway's non-atomic portable write fallback.
        match self.dispatch(method, path, query, body).await {
            Ok(body) => Ok(response(200, body)),
            Err(TurbopufferError::Response(r)) => Ok(r),
            Err(TurbopufferError::NotFound(message)) => Ok(response(
                404,
                json!({"error":"not_found","message":message}),
            )),
            Err(e) if e.to_string().contains("UnsupportedByStore") => Ok(response(
                422,
                json!({"error":"UnsupportedByStore","store":"pgvector","route":path,"message":e.to_string()}),
            )),
            Err(e) => Err(e),
        }
    }
    async fn delete_namespace(&self, namespace: &str) -> Result<TurbopufferPassthroughResponse> {
        let encoded =
            percent_encoding::utf8_percent_encode(namespace, percent_encoding::NON_ALPHANUMERIC);
        self.passthrough("DELETE", &format!("/v2/namespaces/{encoded}"), None, None)
            .await
    }
    async fn hint_cache_warm(&self, _: &str) -> Result<()> {
        Err(unsupported("hint_cache_warm"))
    }
    async fn upsert(&self, namespace: &str, docs: &[UpsertDoc]) -> Result<TurbopufferWriteOutcome> {
        let mut rows = vec![];
        for d in docs {
            if d.vectors.is_some() {
                return Err(unsupported("multi_vector"));
            }
            let mut row = json!(d.attributes);
            row["id"] = json!(d.id);
            if let Some(v) = &d.vector {
                row["vector"] = json!(v);
            }
            rows.push(row);
        }
        self.write(namespace, &json!({"upsert_rows":rows})).await?;
        Ok(TurbopufferWriteOutcome::default())
    }
    async fn patch(&self, _: &str, _: &[PatchDoc]) -> Result<TurbopufferWriteOutcome> {
        Err(unsupported("patch_rows"))
    }
    async fn patch_columns(&self, _: &str, _: &PatchColumns) -> Result<TurbopufferWriteOutcome> {
        Err(unsupported("patch_columns"))
    }
    async fn delete(&self, ns: &str, ids: &[String]) -> Result<TurbopufferWriteOutcome> {
        self.write(ns, &json!({"deletes":ids})).await?;
        Ok(TurbopufferWriteOutcome::default())
    }
    async fn query(
        &self,
        ns: &str,
        vector: &[f64],
        k: u32,
        filters: Option<&Value>,
        include: Option<&IncludeAttributes>,
    ) -> Result<TurbopufferQueryOutcome> {
        self.ranked_query(ns, &json!(["vector", "ANN", vector]), k, filters, include)
            .await
    }
    async fn ranked_query(
        &self,
        ns: &str,
        rank: &Value,
        k: u32,
        filters: Option<&Value>,
        include: Option<&IncludeAttributes>,
    ) -> Result<TurbopufferQueryOutcome> {
        let mut body = json!({"rank_by":rank,"top_k":k});
        if let Some(f) = filters {
            body["filters"] = f.clone();
        }
        if let Some(i) = include {
            body["include_attributes"] = i.to_turbopuffer_value();
        }
        let result = self.query_wire(ns, &body).await?;
        let rows = result["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                let mut r = r.clone();
                let dist = r
                    .as_object_mut()
                    .unwrap()
                    .remove("$dist")
                    .and_then(|v| v.as_f64());
                // The shared trait uses string IDs. Encode the complete wire ID
                // for gateway fusion so numeric 7 and string "7" never collide.
                let wire_id = r["id"].to_string();
                let d = doc(r);
                crate::models::QueryResult {
                    id: wire_id,
                    attributes: d.attributes,
                    dist,
                }
            })
            .collect();
        Ok(TurbopufferQueryOutcome {
            rows,
            billing: None,
        })
    }
    async fn multi_ranked_query(&self, _: &str, _: &[Value], _: Option<&Value>) -> Result<Value> {
        Err(unsupported("multi_query"))
    }
    async fn fetch(&self, ns: &str, id: &str) -> Result<Option<DocumentResponse>> {
        Ok(self
            .fetch_data(ns, &[id.into()])
            .await?
            .into_iter()
            .next()
            .map(doc))
    }
    async fn fetch_many(
        &self,
        ns: &str,
        ids: &[String],
    ) -> Result<HashMap<String, DocumentResponse>> {
        Ok(self
            .fetch_data(ns, ids)
            .await?
            .into_iter()
            .map(doc)
            .map(|d| (d.id.clone(), d))
            .collect())
    }
    async fn fetch_vector(&self, ns: &str, id: &str) -> Result<Option<Vec<f64>>> {
        self.fetch_data(ns, &[id.into()])
            .await?
            .first()
            .and_then(|v| v.get("vector"))
            .map(|v| {
                serde_json::from_value(v.clone()).map_err(|_| invalid("invalid stored vector"))
            })
            .transpose()
    }
    async fn scan_page(
        &self,
        _: &str,
        _: Option<&str>,
        _: u32,
        _: Option<&Value>,
        _: Option<&[String]>,
    ) -> Result<DocumentPage> {
        Err(unsupported("ordered_scan"))
    }
    async fn head_namespace(&self, ns: &str) -> Result<NamespaceMeta> {
        let raw = self.metadata(ns).await?;
        Ok(NamespaceMeta {
            approx_row_count: raw["approx_row_count"].as_u64().unwrap_or(0),
            raw,
            ..NamespaceMeta::default()
        })
    }
}
