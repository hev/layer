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
    /// Gateway embedding profiles (RFC 0118 step E). A registry column beside
    /// `schema`, never parsed by `Schema::parse`, so a gateway that predates
    /// it still loads every namespace.
    embed: Option<Value>,
}

/// A rejection naming a stable feature id (RFC 0118 § "The 422 body").
fn unsupported(feature: &str) -> TurbopufferError {
    crate::capabilities::unsupported_by_store("pgvector", feature, None)
}
/// A rejection whose human-readable detail follows the feature id.
fn unsupported_detail(feature: &str, detail: &str) -> TurbopufferError {
    crate::capabilities::unsupported_by_store("pgvector", feature, Some(detail))
}
/// A rejected request-body key. A key an inventoried wire feature owns
/// reports that feature's id and names the key in the detail, so a client
/// matches one string against the capability report and the 422.
fn unsupported_key(key: &str) -> TurbopufferError {
    match crate::capabilities::WireFeature::for_wire_key(key) {
        Some(feature) if feature.id() != key => {
            unsupported_detail(feature.id(), &format!("wire key: {key}"))
        }
        _ => unsupported(key),
    }
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
/// The 422 body, in the same shape the gateway's error module renders: the
/// canonical message plus the typed `feature` recovered from it.
fn rejection_body(route: &str, error: &str) -> Value {
    use crate::capabilities::{StoreRejection, UNSUPPORTED_BY_STORE};
    let mut body = json!({
        "error": UNSUPPORTED_BY_STORE,
        "store": "pgvector",
        "route": route,
        "message": StoreRejection::canonical_message(error),
    });
    if let Some(rejection) = StoreRejection::parse(error) {
        body["feature"] = json!(rejection.feature);
    }
    body
}
fn object(value: &Value) -> Result<&Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| invalid("expected an object"))
}
fn keys(value: &Value, allowed: &[&str]) -> Result<()> {
    for key in object(value)?.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(unsupported_key(key));
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
/// The id sort key: unsigned integers numerically, then strings in byte
/// order. Turbopuffer namespaces hold one id type; pgvector keeps 7 and "7"
/// distinct, so a mixed namespace needs a total order. Each document table
/// has an expression index on exactly this text, which the planner matches.
fn id_order(data: &str) -> String {
    format!("((CASE WHEN jsonb_typeof({data}->'id')='number' THEN 'n'||lpad({data}->>'id',20,'0') ELSE 's'||({data}->>'id') END) COLLATE \"C\")")
}
/// `id_order` for a request id, bound as a filter bound or scan cursor.
fn id_order_key(id: &Value) -> Result<String> {
    id_key(id)?;
    Ok(match id.as_u64() {
        Some(n) => format!("n{n:020}"),
        None => format!("s{}", id.as_str().unwrap()),
    })
}
/// `[attribute, "asc"|"desc"]`, as `(attribute, descending)`.
fn order_key(item: &Value) -> Option<(String, bool)> {
    let [attribute, direction] = item.as_array()?.as_slice() else {
        return None;
    };
    let descending = match direction.as_str()? {
        "asc" => false,
        "desc" => true,
        _ => return None,
    };
    Some((attribute.as_str()?.to_owned(), descending))
}
/// Attribute ordering forms of rank_by; `None` for ranking operators.
fn ordering(rank: &Value) -> Result<Option<Vec<(String, bool)>>> {
    if let Some(key) = order_key(rank) {
        return Ok(Some(vec![key]));
    }
    let Some(items) = rank
        .as_array()
        .filter(|items| items.first().is_some_and(Value::is_array))
    else {
        return Ok(None);
    };
    if items.len() > 8 {
        return Err(invalid("rank_by orders by at most 8 attributes"));
    }
    items
        .iter()
        .map(|item| {
            order_key(item)
                .ok_or_else(|| invalid("rank_by order must be [attribute, \"asc\"|\"desc\"]"))
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
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
        // RFC 0118 step E: gateway embedding profiles survive a gateway
        // restart with no object store. Additive and nullable.
        sqlx::query("ALTER TABLE layer_pgvector.namespaces ADD COLUMN IF NOT EXISTS embed jsonb")
            .execute(&mut *bootstrap)
            .await
            .map_err(db)?;
        // Content-addressed blobs (RFC 0123): one bytea row per sha256.
        sqlx::query("CREATE TABLE IF NOT EXISTS layer_pgvector.blobs (scope text NOT NULL, namespace text NOT NULL, sha256 text NOT NULL, data bytea NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(scope,namespace,sha256))")
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
        let row = sqlx::query("SELECT table_name,schema,metric,embed FROM layer_pgvector.namespaces WHERE scope=$1 AND name=$2")
            .bind(&self.scope).bind(namespace).fetch_optional(&mut **tx).await.map_err(db)?;
        row.map(|r| {
            Ok(Namespace {
                table: r.get("table_name"),
                schema: Schema::parse(&r.get::<Value, _>("schema"))?,
                metric: r.get("metric"),
                embed: r.get("embed"),
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
                "upsert_condition",
                "delete_condition",
                EMBEDDING_PROFILES_KEY,
            ],
        )?;
        let rows = schema::rows(body)?;
        // Validate what the request alone can prove before the transaction
        // (RFC 0114 atomicity): a declaration this store cannot serve, or a
        // metric it cannot index, is rejected with no lock and no SQL. The
        // merge below re-parses against the stored schema and can still
        // reject; it never accepts what this pass rejects.
        if let Some(update) = body.get("schema") {
            Schema::parse(update)?;
        }
        // The gateway's embedding profiles (RFC 0118 step E): absent keeps the
        // stored set, an empty array or null clears it. Validated before the
        // transaction too.
        let profiles = match body.get(EMBEDDING_PROFILES_KEY).cloned() {
            Some(profiles) if embed_targets(&profiles)?.is_empty() => Some(None),
            Some(profiles) => Some(Some(profiles)),
            None => None,
        };
        if let Some(metric) = body.get("distance_metric") {
            let metric = metric
                .as_str()
                .ok_or_else(|| invalid("distance_metric must be a string"))?;
            if !["cosine_distance", "euclidean_squared"].contains(&metric) {
                return Err(unsupported("distance_metric"));
            }
        }
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
        let embed = match profiles {
            Some(profiles) => profiles,
            None => previous.as_ref().and_then(|n| n.embed.clone()),
        };
        // RFC 0118 rule 7: an embedded namespace holds only its embedding
        // target as a vector field; a client vector is a second one.
        if let Some(embed) = embed.as_ref() {
            let targets = embed_targets(embed)?;
            if schema
                .fields()
                .any(|f| !f.scalar() && !targets.iter().any(|(_, target)| *target == f.name))
            {
                return Err(unsupported_detail(
                    "max_vector_fields",
                    "one vector field per namespace; this namespace's is its gateway-embedded attribute",
                ));
            }
        }
        // Conditions apply per targeted document: an upsert of a new id always
        // proceeds, a delete of a missing id is a no-op.
        let upsert_condition = body.get("upsert_condition").filter(|v| !v.is_null());
        let delete_condition = body.get("delete_condition").filter(|v| !v.is_null());
        for (condition, refs) in [
            (upsert_condition, filter::Refs::Excluded),
            (delete_condition, filter::Refs::Null),
        ] {
            if let Some(condition) = condition {
                filter::compile(&mut QueryBuilder::new(""), &schema, condition, 0, refs)?;
            }
        }
        let table = table_name(&self.scope, namespace);
        if previous.is_none() {
            sqlx::query(&format!(
                "CREATE TABLE layer_pgvector.\"{table}\" (rid bigserial PRIMARY KEY, key text NOT NULL UNIQUE, data jsonb NOT NULL)"
            ))
            .execute(&mut *tx)
            .await
            .map_err(db)?;
            // Serves rank_by id, filter-only queries, ordered id filters and scans.
            sqlx::query(&format!(
                "CREATE INDEX ON layer_pgvector.\"{table}\" ({})",
                id_order("data")
            ))
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        schema.install(&mut tx, &table, &old_schema, metric).await?;
        sqlx::query("INSERT INTO layer_pgvector.namespaces (scope,name,table_name,schema,metric,embed) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(scope,name) DO UPDATE SET schema=excluded.schema,embed=excluded.embed,updated_at=now()")
            .bind(&self.scope).bind(namespace).bind(&table).bind(schema.value()).bind(metric).bind(&embed).execute(&mut *tx).await.map_err(db)?;
        let mut upserted = 0;
        for row in &rows {
            let key = id_key(&row["id"])?;
            let mut sql = QueryBuilder::<Postgres>::new(format!(
                "INSERT INTO layer_pgvector.\"{table}\" AS cur (key,data"
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
            if let Some(condition) = upsert_condition {
                // A failed condition leaves the stored row and affects 0 rows.
                sql.push(" WHERE (");
                filter::compile(&mut sql, &schema, condition, 0, filter::Refs::Excluded)?;
                sql.push(")");
            }
            upserted += sql
                .build()
                .execute(&mut *tx)
                .await
                .map_err(db)?
                .rows_affected();
        }
        let mut sql = QueryBuilder::<Postgres>::new(format!(
            "DELETE FROM layer_pgvector.\"{table}\" WHERE key=ANY("
        ));
        sql.push_bind(deletes).push(")");
        if let Some(condition) = delete_condition {
            sql.push(" AND (");
            filter::compile(&mut sql, &schema, condition, 0, filter::Refs::Null)?;
            sql.push(")");
        }
        let deleted = sql
            .build()
            .execute(&mut *tx)
            .await
            .map_err(db)?
            .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(
            json!({"status":"OK", "message":"write committed", "billing":{}, "rows_affected":upserted+deleted, "rows_upserted":upserted, "rows_deleted":deleted}),
        )
    }

    async fn query_wire(&self, namespace: &str, body: &Value) -> Result<Value> {
        // Name both the declared feature and the submitted wire key so clients
        // can correlate the rejection with either contract.
        if let Some(route) = crate::capabilities::HybridRoute::for_query_body(body) {
            self.capabilities()
                .require(route.feature())
                .map_err(|error| {
                    let key = if body.get("queries").is_some() {
                        "queries"
                    } else if body.get("rerank_by").is_some() {
                        "rerank_by"
                    } else {
                        "rank_by"
                    };
                    match error {
                        TurbopufferError::Other(message) => {
                            TurbopufferError::Other(format!("{message}: wire key: {key}"))
                        }
                        error => error,
                    }
                })?;
        }
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
        let include: Option<IncludeAttributes> = body
            .get("include_attributes")
            .map(|v| {
                serde_json::from_value(v.clone()).map_err(|_| invalid("invalid include_attributes"))
            })
            .transpose()?;
        // A filter-only query (no rank_by, no vector) is ordered by id ascending.
        let order = match body.get("rank_by") {
            Some(rank) => ordering(rank)?,
            None if body.get("vector").is_none() => Some(vec![("id".into(), false)]),
            None => None,
        };
        if let Some(order) = order {
            let filters = body.get("filters").filter(|v| !v.is_null());
            let rows = self
                .ordered_rows(namespace, &order, k, filters, None, include.as_ref())
                .await?;
            // No $dist: Turbopuffer omits it when ordering by an attribute.
            return Ok(json!({"rows": rows.into_iter().map(|(row, _)| row).collect::<Vec<_>>()}));
        }
        let rank = body
            .get("rank_by")
            .cloned()
            .or_else(|| body.get("vector").map(|v| json!(["vector", "ANN", v])))
            .ok_or_else(|| invalid("rank_by or vector is required"))?;
        let rank = rank.as_array().filter(|r| r.len() == 3).ok_or_else(|| {
            unsupported_detail("rank_by", "expression must be [field, operator, input]")
        })?;
        let field = rank[0]
            .as_str()
            .ok_or_else(|| invalid("rank field must be a string"))?;
        let mode = rank[1]
            .as_str()
            .ok_or_else(|| invalid("rank operator must be a string"))?;
        if !["ANN", "BM25"].contains(&mode) {
            return Err(unsupported(mode));
        }
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
                .ok_or_else(|| unsupported_detail("fts", "BM25 input must be a string"))?;
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
            filter::compile(&mut sql, &ns.schema, filters, 0, filter::Refs::Rejected)?;
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

    /// Rows in rank_by attribute order, id ascending as the final tiebreaker.
    /// Nulls sort first ascending and last descending, as on Turbopuffer.
    /// Each row is returned with its `id_order` key; `after` is exclusive.
    async fn ordered_rows(
        &self,
        namespace: &str,
        order: &[(String, bool)],
        limit: u64,
        filters: Option<&Value>,
        after: Option<&str>,
        include: Option<&IncludeAttributes>,
    ) -> Result<Vec<(Value, String)>> {
        let mut tx = self.begin(namespace).await?;
        let ns = self.required(&mut tx, namespace).await?;
        let id = id_order("data");
        let mut sql = QueryBuilder::<Postgres>::new(format!(
            "SELECT data,{id} AS okey FROM layer_pgvector.{} WHERE TRUE",
            quoted(&ns.table)
        ));
        if let Some(after) = after {
            sql.push(format!(" AND {id} > "))
                .push_bind(after.to_owned());
        }
        if let Some(filters) = filters {
            sql.push(" AND (");
            filter::compile(&mut sql, &ns.schema, filters, 0, filter::Refs::Rejected)?;
            sql.push(")");
        }
        sql.push(" ORDER BY ");
        for (attribute, descending) in order {
            if attribute == "id" {
                sql.push(&id);
                sql.push(if *descending { " DESC," } else { " ASC," });
                continue;
            }
            let field = ns
                .schema
                .get(attribute)
                .ok_or_else(|| invalid(format!("unknown rank_by attribute {attribute}")))?;
            if !field.scalar() {
                return Err(invalid("rank_by cannot order by a vector attribute"));
            }
            sql.push(field.order_expr("")).push(if *descending {
                " DESC NULLS LAST,"
            } else {
                " ASC NULLS FIRST,"
            });
        }
        sql.push(&id)
            .push(" ASC LIMIT ")
            .push_bind(i64::try_from(limit).unwrap_or(i64::MAX));
        let rows = sql.build().fetch_all(&mut *tx).await.map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(rows
            .into_iter()
            .map(|r| {
                let mut data: Value = r.get("data");
                project(&mut data, include, &ns.schema);
                (data, r.get("okey"))
            })
            .collect())
    }

    async fn metadata(&self, namespace: &str) -> Result<Value> {
        let mut tx = self.begin(namespace).await?;
        let ns = self.required(&mut tx, namespace).await?;
        let stats = sqlx::query(&format!("SELECT count(*) AS count, COALESCE(sum(octet_length(data::text)),0)::bigint AS bytes FROM layer_pgvector.\"{}\"", ns.table))
            .fetch_one(&mut *tx).await.map_err(db)?;
        let times = sqlx::query("SELECT created_at::text,updated_at::text FROM layer_pgvector.namespaces WHERE scope=$1 AND name=$2")
            .bind(&self.scope).bind(namespace).fetch_one(&mut *tx).await.map_err(db)?;
        let mut schema = ns.schema.value();
        if let Some(embed) = ns.embed.as_ref() {
            merge_embed_declarations(&mut schema, embed);
        }
        Ok(
            json!({"id":namespace,"schema":schema, "approx_row_count":stats.get::<i64,_>("count"),
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
                _ => return Err(unsupported_key(&key)),
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
            return Err(unsupported_detail(
                "passthrough",
                &format!("{method} {path}"),
            ));
        }
        if query.is_some_and(|s| !s.is_empty()) {
            return Err(unsupported_detail("passthrough", "query parameters"));
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
            // The fourth segment is the wire operation (`hint_cache_warm`,
            // `export`, ...); a bare namespace verb has no better name than
            // the passthrough it fell through to.
            _ => Err(unsupported_detail(
                parts.get(3).copied().unwrap_or("passthrough"),
                &format!("{method} {path}"),
            )),
        }
    }
}

/// `(source, target)` of each gateway embedding profile (RFC 0118 step E).
/// Everything else in a profile is the gateway's and stays opaque here.
fn embed_targets(profiles: &Value) -> Result<Vec<(&str, &str)>> {
    let profiles = match profiles {
        Value::Null => return Ok(vec![]),
        Value::Array(profiles) => profiles,
        _ => return Err(invalid("embedding profiles must be an array")),
    };
    let targets = profiles
        .iter()
        .map(|profile| {
            let field = |key| profile.get(key).and_then(Value::as_str);
            field("source")
                .zip(field("target"))
                .ok_or_else(|| invalid("embedding profile requires source and target"))
        })
        .collect::<Result<Vec<_>>>()?;
    if targets.len() > 1 {
        return Err(unsupported_detail(
            "schema.embed",
            "one embedded attribute per namespace",
        ));
    }
    Ok(targets)
}

/// Put each profile's client-written `embed` declaration back on its source
/// attribute, so `GET .../schema` returns what the client wrote.
fn merge_embed_declarations(schema: &mut Value, profiles: &Value) {
    let Some(profiles) = profiles.as_array() else {
        return;
    };
    for profile in profiles {
        let (Some(source), Some(declaration)) = (
            profile.get("source").and_then(Value::as_str),
            profile.get("declaration"),
        ) else {
            continue;
        };
        let Some(attribute) = schema.get_mut(source) else {
            continue;
        };
        if let Some(kind) = attribute.as_str() {
            *attribute = json!({"type": kind});
        }
        if let Some(attribute) = attribute.as_object_mut() {
            attribute.insert("embed".into(), declaration.clone());
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

    async fn put_blob(&self, namespace: &str, sha256: &str, bytes: &[u8]) -> Result<()> {
        sqlx::query("INSERT INTO layer_pgvector.blobs (scope,namespace,sha256,data) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING")
            .bind(&self.scope)
            .bind(namespace)
            .bind(sha256)
            .bind(bytes)
            .execute(&self.pool)
            .await
            .map_err(db)?;
        Ok(())
    }

    async fn get_blob(&self, namespace: &str, sha256: &str) -> Result<Option<Vec<u8>>> {
        sqlx::query_scalar(
            "SELECT data FROM layer_pgvector.blobs WHERE scope=$1 AND namespace=$2 AND sha256=$3",
        )
        .bind(&self.scope)
        .bind(namespace)
        .bind(sha256)
        .fetch_optional(&self.pool)
        .await
        .map_err(db)
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
            Err(e)
                if e.to_string()
                    .contains(crate::capabilities::UNSUPPORTED_BY_STORE) =>
            {
                Ok(response(422, rejection_body(path, &e.to_string())))
            }
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
                return Err(unsupported(
                    crate::capabilities::WireFeature::MultiVector.id(),
                ));
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
                    numeric_id: false,
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
        self.capabilities()
            .unimplemented(crate::capabilities::WireFeature::MultiQuery)
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
        ns: &str,
        cursor: Option<&str>,
        page_size: u32,
        filters: Option<&Value>,
        include_attributes: Option<&[String]>,
    ) -> Result<DocumentPage> {
        let include = include_attributes
            .map(|fields| IncludeAttributes::Fields(fields.to_vec()))
            .unwrap_or(IncludeAttributes::All(true));
        let page_size = page_size as usize;
        // The cursor is the last row's opaque id sort key, so numeric 7 and
        // string "7" page distinctly.
        let mut rows = self
            .ordered_rows(
                ns,
                &[("id".into(), false)],
                page_size as u64 + 1,
                filters,
                cursor,
                Some(&include),
            )
            .await?;
        let next_cursor = if rows.len() > page_size {
            rows.truncate(page_size);
            rows.last().map(|(_, key)| key.clone())
        } else {
            None
        };
        Ok(DocumentPage {
            documents: rows.into_iter().map(|(row, _)| doc(row)).collect(),
            next_cursor,
        })
    }
    async fn embedding_profiles(&self, namespace: &str) -> Result<Option<Value>> {
        let embed: Option<Option<Value>> = sqlx::query_scalar(
            "SELECT embed FROM layer_pgvector.namespaces WHERE scope=$1 AND name=$2",
        )
        .bind(&self.scope)
        .bind(namespace)
        .fetch_optional(&self.pool)
        .await
        .map_err(db)?;
        Ok(embed.flatten())
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
