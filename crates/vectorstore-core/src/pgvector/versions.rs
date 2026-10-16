//! Retained content never enters the search relation. Pointer, projection and
//! activation event change under the same cross-process namespace transaction.
use super::*;

fn conflict(message: &str) -> TurbopufferError {
    TurbopufferError::from_status(
        reqwest::StatusCode::CONFLICT,
        &json!({"error":"version_conflict","message":message}).to_string(),
    )
}
fn version_id(value: &Value) -> Result<&str> {
    value
        .as_str()
        .filter(|s| {
            !s.is_empty()
                && s.len() <= 128
                && s.bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))
        })
        .ok_or_else(|| invalid("version_id must contain 1–128 ASCII letters, digits, -, _ or ."))
}

pub(super) fn options(body: &Value) -> Result<Option<&Value>> {
    let Some(options) = body.get("_hevlayer") else {
        return Ok(None);
    };
    keys(options, &["versioning", "version_id"])?;
    if let Some(id) = options.get("version_id") {
        version_id(id)?;
    }
    if let Some(config) = options.get("versioning") {
        keys(config, &["enabled", "activation"])?;
        if config.get("enabled").and_then(Value::as_bool).is_none() {
            return Err(invalid("versioning.enabled must be a boolean"));
        }
        if !matches!(
            config.get("activation").and_then(Value::as_str),
            Some("automatic" | "explicit")
        ) {
            return Err(invalid(
                "versioning.activation must be automatic or explicit",
            ));
        }
    }
    Ok(Some(options))
}

impl PgvectorClient {
    pub(super) async fn bootstrap_versions(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
        sqlx::query(
            "ALTER TABLE layer_pgvector.namespaces ADD COLUMN IF NOT EXISTS versioning text",
        )
        .execute(&mut **tx)
        .await
        .map_err(db)?;
        // Namespace deletion is the sole purge operation. These foreign keys
        // make erasure atomic with dropping its active projection.
        for ddl in [
            "CREATE TABLE IF NOT EXISTS layer_pgvector.record_versions (scope text NOT NULL, namespace text NOT NULL, version_id text COLLATE \"C\" NOT NULL, record_key text NOT NULL, data jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(scope,namespace,version_id), FOREIGN KEY(scope,namespace) REFERENCES layer_pgvector.namespaces(scope,name) ON DELETE CASCADE)",
            "CREATE INDEX IF NOT EXISTS record_versions_record ON layer_pgvector.record_versions(scope,namespace,record_key,version_id)",
            "CREATE TABLE IF NOT EXISTS layer_pgvector.record_heads (scope text NOT NULL, namespace text NOT NULL, record_key text NOT NULL, version_id text, PRIMARY KEY(scope,namespace,record_key), FOREIGN KEY(scope,namespace) REFERENCES layer_pgvector.namespaces(scope,name) ON DELETE CASCADE)",
            "CREATE TABLE IF NOT EXISTS layer_pgvector.record_activations (sequence bigserial PRIMARY KEY, scope text NOT NULL, namespace text NOT NULL, record_key text NOT NULL, previous_version_id text, version_id text, created_at timestamptz NOT NULL DEFAULT clock_timestamp(), FOREIGN KEY(scope,namespace) REFERENCES layer_pgvector.namespaces(scope,name) ON DELETE CASCADE)",
            "CREATE INDEX IF NOT EXISTS record_activations_record ON layer_pgvector.record_activations(scope,namespace,record_key,sequence)",
        ] { sqlx::query(ddl).execute(&mut **tx).await.map_err(db)?; }
        Ok(())
    }

    pub(super) async fn versioning_mode(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        previous: Option<&Namespace>,
        body: &Value,
        rows: &[Value],
    ) -> Result<Option<String>> {
        let opts = options(body)?;
        let mut mode = previous.and_then(|n| n.versioning.clone());
        if let Some(config) = opts.and_then(|o| o.get("versioning")) {
            let enabled = config["enabled"] == true;
            if mode.is_some() && (!enabled || mode.as_deref() != config["activation"].as_str()) {
                return Err(conflict(
                    "versioning cannot be disabled or its activation mode changed",
                ));
            }
            if enabled && mode.is_none() {
                if let Some(ns) = previous {
                    let count: i64 = sqlx::query_scalar(&format!(
                        "SELECT count(*) FROM layer_pgvector.\"{}\"",
                        ns.table
                    ))
                    .fetch_one(&mut **tx)
                    .await
                    .map_err(db)?;
                    if count != 0 {
                        return Err(conflict("enable versioning on an empty namespace"));
                    }
                }
                mode = config["activation"].as_str().map(str::to_owned);
            }
        }
        if mode.is_some() {
            if opts.and_then(|o| o.get("version_id")).is_some() && rows.len() != 1 {
                return Err(invalid("version_id requires exactly one upsert row"));
            }
            for key in [
                "patch_rows",
                "patch_columns",
                "patch_by_filter",
                "delete_by_filter",
                "patch_condition",
                "delete_condition",
                "upsert_condition",
                "patch_by_filter_allow_partial",
                "delete_by_filter_allow_partial",
            ] {
                if body.get(key).is_some() {
                    return Err(unsupported_detail("record_versioning", key));
                }
            }
            for row in rows {
                if row["id"].as_str().is_none() {
                    return Err(invalid("versioned records require string ids"));
                }
            }
            if body
                .get("deletes")
                .and_then(Value::as_array)
                .is_some_and(|ids| ids.iter().any(|id| !id.is_string()))
            {
                return Err(invalid("versioned records require string ids"));
            }
        } else if opts.and_then(|o| o.get("version_id")).is_some() {
            return Err(invalid("version_id requires enabled versioning"));
        }
        Ok(mode)
    }

    pub(super) async fn head(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        ns: &str,
        key: &str,
    ) -> Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, Option<String>>("SELECT version_id FROM layer_pgvector.record_heads WHERE scope=$1 AND namespace=$2 AND record_key=$3")
            .bind(&self.scope).bind(ns).bind(key).fetch_optional(&mut **tx).await.map_err(db)?.flatten())
    }

    async fn set_head(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        ns: &str,
        key: &str,
        target: Option<&str>,
    ) -> Result<()> {
        let previous = self.head(tx, ns, key).await?;
        if previous.as_deref() == target {
            return Ok(());
        }
        sqlx::query("INSERT INTO layer_pgvector.record_heads(scope,namespace,record_key,version_id) VALUES($1,$2,$3,$4) ON CONFLICT(scope,namespace,record_key) DO UPDATE SET version_id=excluded.version_id")
            .bind(&self.scope).bind(ns).bind(key).bind(target).execute(&mut **tx).await.map_err(db)?;
        sqlx::query("INSERT INTO layer_pgvector.record_activations(scope,namespace,record_key,previous_version_id,version_id) VALUES($1,$2,$3,$4,$5)")
            .bind(&self.scope).bind(ns).bind(key).bind(previous).bind(target).execute(&mut **tx).await.map_err(db)?;
        Ok(())
    }

    pub(super) async fn retain_row(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        ns: &str,
        row: &Value,
        supplied: Option<&str>,
        automatic: bool,
    ) -> Result<(Value, bool)> {
        let key = id_key(&row["id"])?;
        let id = match supplied {
            Some(id) => id.to_owned(),
            None => sqlx::query_scalar::<_, String>("SELECT gen_random_uuid()::text")
                .fetch_one(&mut **tx)
                .await
                .map_err(db)?,
        };
        let existing: Option<(String, Value)> = sqlx::query_as("SELECT record_key,data FROM layer_pgvector.record_versions WHERE scope=$1 AND namespace=$2 AND version_id=$3")
            .bind(&self.scope).bind(ns).bind(&id).fetch_optional(&mut **tx).await.map_err(db)?;
        let fresh = existing.is_none();
        if let Some((stored_key, content)) = existing {
            if stored_key != key || content != *row {
                return Err(conflict("version_id already identifies different content"));
            }
        } else {
            sqlx::query("INSERT INTO layer_pgvector.record_versions(scope,namespace,version_id,record_key,data) VALUES($1,$2,$3,$4,$5)")
                .bind(&self.scope).bind(ns).bind(&id).bind(&key).bind(row).execute(&mut **tx).await.map_err(db)?;
            if automatic {
                self.set_head(tx, ns, &key, Some(&id)).await?;
            }
        }
        let active = self.head(tx, ns, &key).await?.as_deref() == Some(id.as_str());
        Ok((
            json!({"id":row["id"],"version_id":id,"active":active}),
            fresh && automatic,
        ))
    }

    pub(super) async fn version_delete(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        ns: &str,
        key: &str,
    ) -> Result<()> {
        self.set_head(tx, ns, key, None).await
    }

    pub(super) async fn record_api(
        &self,
        ns: &str,
        parts: &[&str],
        method: &str,
        query: Option<&str>,
        body: Option<Value>,
    ) -> Result<Value> {
        let record = percent_encoding::percent_decode_str(parts[4])
            .decode_utf8()
            .map_err(|_| invalid("invalid record id encoding"))?;
        let key = id_key(&json!(record))?;
        let mut tx = self.begin(ns).await?;
        let n = self.required(&mut tx, ns).await?;
        if n.versioning.is_none() {
            return Err(conflict("record versioning is not enabled"));
        }
        if method == "POST" && parts.len() == 6 && parts[5] == "activate" {
            if query.is_some_and(|q| !q.is_empty()) {
                return Err(invalid("activation has no query parameters"));
            }
            let body = body.unwrap_or(Value::Null);
            keys(&body, &["version_id", "expected_current_version"])?;
            let target = version_id(&body["version_id"])?;
            let expected = body.get("expected_current_version").ok_or_else(|| {
                invalid("expected_current_version is required; null means no active version")
            })?;
            let expected = if expected.is_null() {
                None
            } else {
                Some(version_id(expected)?)
            };
            if self.head(&mut tx, ns, &key).await?.as_deref() != expected {
                return Err(conflict(
                    "expected_current_version does not match active version",
                ));
            }
            let row: Value = sqlx::query_scalar("SELECT data FROM layer_pgvector.record_versions WHERE scope=$1 AND namespace=$2 AND record_key=$3 AND version_id=$4")
                .bind(&self.scope).bind(ns).bind(&key).bind(target).fetch_optional(&mut *tx).await.map_err(db)?
                .ok_or_else(|| TurbopufferError::NotFound("record version".into()))?;
            n.schema.validate_row(&row, &n.metric)?;
            upsert_row(&mut tx, &n.table, &n.schema, &row, None).await?;
            self.set_head(&mut tx, ns, &key, Some(target)).await?;
            tx.commit().await.map_err(db)?;
            return Ok(json!({"id":record,"active_version_id":target}));
        }
        if method != "GET" {
            return Err(unsupported("record_versioning"));
        }
        if parts.len() == 7 && parts[5] == "versions" {
            let version = percent_encoding::percent_decode_str(parts[6])
                .decode_utf8()
                .map_err(|_| invalid("invalid version encoding"))?;
            let row = sqlx::query("SELECT data,created_at::text FROM layer_pgvector.record_versions WHERE scope=$1 AND namespace=$2 AND record_key=$3 AND version_id=$4")
                .bind(&self.scope).bind(ns).bind(&key).bind(version.as_ref()).fetch_optional(&mut *tx).await.map_err(db)?
                .ok_or_else(|| TurbopufferError::NotFound("record version".into()))?;
            return Ok(
                json!({"id":record,"version_id":version,"row":row.get::<Value,_>("data"),"created_at":row.get::<String,_>("created_at")}),
            );
        }
        if parts.len() != 6 || !["versions", "activations"].contains(&parts[5]) {
            return Err(unsupported("record_versioning"));
        }
        let mut limit = 100i64;
        let mut after = String::new();
        for pair in query.unwrap_or("").split('&').filter(|p| !p.is_empty()) {
            let (name, value) = pair
                .split_once('=')
                .ok_or_else(|| invalid("invalid pagination"))?;
            let value = percent_encoding::percent_decode_str(value)
                .decode_utf8()
                .map_err(|_| invalid("invalid cursor"))?;
            match name {
                "limit" => {
                    limit = value
                        .parse()
                        .ok()
                        .filter(|n| (1..=100).contains(n))
                        .ok_or_else(|| invalid("limit must be 1–100"))?
                }
                "after" => after = value.into_owned(),
                _ => return Err(invalid("unknown pagination parameter")),
            }
        }
        if parts[5] == "versions" {
            let rows = sqlx::query("SELECT version_id,data,created_at::text FROM layer_pgvector.record_versions WHERE scope=$1 AND namespace=$2 AND record_key=$3 AND version_id>$4 ORDER BY version_id LIMIT $5")
                .bind(&self.scope).bind(ns).bind(&key).bind(after).bind(limit+1).fetch_all(&mut *tx).await.map_err(db)?;
            let more = rows.len() > limit as usize;
            let items: Vec<Value> = rows.iter().take(limit as usize).map(|r| json!({"id":record,"version_id":r.get::<String,_>("version_id"),"row":r.get::<Value,_>("data"),"created_at":r.get::<String,_>("created_at")})).collect();
            let cursor = more.then(|| items.last().unwrap()["version_id"].clone());
            return Ok(
                json!({"versions":items,"active_version_id":self.head(&mut tx, ns, &key).await?,"next_cursor":cursor}),
            );
        }
        let after = if after.is_empty() {
            0
        } else {
            after
                .parse::<i64>()
                .ok()
                .filter(|n| *n >= 0)
                .ok_or_else(|| invalid("activation cursor must be a nonnegative integer"))?
        };
        let rows = sqlx::query("SELECT sequence,previous_version_id,version_id,created_at::text FROM layer_pgvector.record_activations WHERE scope=$1 AND namespace=$2 AND record_key=$3 AND sequence>$4 ORDER BY sequence LIMIT $5")
            .bind(&self.scope).bind(ns).bind(&key).bind(after).bind(limit+1).fetch_all(&mut *tx).await.map_err(db)?;
        let more = rows.len() > limit as usize;
        let items: Vec<Value> = rows.iter().take(limit as usize).map(|r| json!({"sequence":r.get::<i64,_>("sequence"),"previous_version_id":r.get::<Option<String>,_>("previous_version_id"),"version_id":r.get::<Option<String>,_>("version_id"),"created_at":r.get::<String,_>("created_at")})).collect();
        let cursor = more.then(|| items.last().unwrap()["sequence"].to_string());
        Ok(json!({"activations":items,"next_cursor":cursor}))
    }
}
