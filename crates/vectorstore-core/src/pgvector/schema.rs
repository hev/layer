use super::*;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default)]
pub(super) struct Schema(BTreeMap<String, Field>);
#[derive(Clone, Debug)]
pub(super) struct Field {
    pub name: String,
    kind: String,
    pub text: bool,
    declaration: Value,
}
impl Field {
    pub fn column(&self) -> String {
        identifier("a_", &self.name)
    }
    fn dimension(&self) -> Option<usize> {
        self.kind
            .strip_prefix('[')?
            .strip_suffix("]f32")?
            .parse()
            .ok()
    }
    fn sql_type(&self) -> String {
        if let Some(d) = self.dimension() {
            return format!("vector({d})");
        }
        match self.kind.as_str() {
            "string" => "text",
            "int" => "bigint",
            "uint" => "numeric(20,0)",
            "float" => "double precision",
            "bool" => "boolean",
            _ => unreachable!(),
        }
        .into()
    }
    pub fn validate(&self, v: &Value) -> Result<()> {
        if v.is_null() {
            return Ok(());
        }
        if self.dimension().is_some() {
            return self.validate_vector(v, "euclidean_squared");
        }
        let valid = match self.kind.as_str() {
            "string" => v.is_string(),
            "int" => v.as_i64().is_some(),
            "uint" => v.as_u64().is_some(),
            "float" => v.as_f64().is_some_and(|v| v.is_finite()),
            "bool" => v.is_boolean(),
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(invalid(format!(
                "schema mismatch for {}: expected {}",
                self.name, self.kind
            )))
        }
    }
    fn scalar_index(&self) -> bool {
        self.scalar() && self.kind != "string" && self.filterable()
    }
    pub fn filterable(&self) -> bool {
        self.declaration.get("filterable") != Some(&Value::Bool(false))
    }
    pub fn scalar(&self) -> bool {
        self.dimension().is_none()
    }
    pub fn validate_vector(&self, v: &Value, metric: &str) -> Result<()> {
        let dim = self
            .dimension()
            .ok_or_else(|| invalid("ANN requires a vector field"))?;
        let values = v
            .as_array()
            .filter(|a| a.len() == dim)
            .ok_or_else(|| invalid(format!("vector dimension must be {dim}")))?;
        if !values.iter().all(|v| {
            v.as_f64()
                .is_some_and(|n| n.is_finite() && (n as f32).is_finite())
        }) {
            return Err(invalid("vector values must be finite float32 numbers"));
        }
        if metric == "cosine_distance" && values.iter().all(|v| v.as_f64() == Some(0.0)) {
            return Err(invalid("cosine_distance requires a nonzero vector"));
        }
        Ok(())
    }
    pub fn bind(&self, sql: &mut QueryBuilder<'_, Postgres>, value: &Value) {
        // A JSON scalar is bound, then cast through text to the declared SQL
        // type. JSON null becomes SQL NULL; no request text becomes SQL syntax.
        if self.dimension().is_some() {
            sql.push_bind(if value.is_null() {
                None
            } else {
                Some(value.to_string())
            })
            .push("::")
            .push(self.sql_type());
        } else {
            sql.push("(")
                .push_bind(value.clone())
                .push("::jsonb #>> '{}')::")
                .push(self.sql_type());
        }
    }
}
impl Schema {
    pub fn parse(value: &Value) -> Result<Self> {
        let mut fields = BTreeMap::new();
        for (name, decl) in object(value)? {
            if name == "id" {
                return Err(unsupported("id schema"));
            }
            let kind = if let Some(kind) = decl.as_str() {
                kind
            } else {
                keys(decl, &["type", "full_text_search", "filterable"])?;
                decl.get("type")
                    .and_then(Value::as_str)
                    .ok_or_else(|| invalid("schema requires type"))?
            };
            let text = match decl.get("full_text_search") {
                None | Some(Value::Bool(false)) => false,
                Some(Value::Bool(true)) => true,
                _ => return Err(unsupported("full_text_search configuration")),
            };
            if decl.get("filterable").is_some_and(|v| !v.is_boolean()) {
                return Err(invalid("filterable must be boolean"));
            }
            let f = Field {
                name: name.clone(),
                kind: kind.into(),
                text,
                declaration: decl.clone(),
            };
            if !["string", "int", "uint", "float", "bool"].contains(&kind)
                && !f.dimension().is_some_and(|d| d > 0 && d <= 2000)
            {
                return Err(unsupported(&format!("schema type {kind}")));
            }
            if text && kind != "string" {
                return Err(invalid("full_text_search requires string"));
            }
            fields.insert(name.clone(), f);
        }
        if fields.values().filter(|f| f.dimension().is_some()).count() > 1 {
            return Err(unsupported("multiple vector fields"));
        }
        if fields.values().filter(|f| f.text).count() > 1 {
            return Err(unsupported("multiple full_text_search fields"));
        }
        Ok(Self(fields))
    }
    pub fn get(&self, name: &str) -> Option<&Field> {
        self.0.get(name)
    }
    pub fn fields(&self) -> impl Iterator<Item = &Field> {
        self.0.values()
    }
    pub fn value(&self) -> Value {
        Value::Object(
            self.0
                .iter()
                .map(|(k, v)| (k.clone(), v.declaration.clone()))
                .collect(),
        )
    }
    pub fn merge(&self, update: Option<&Value>, rows: &[Value]) -> Result<Self> {
        let mut value = self.value();
        if let Some(update) = update {
            for (k, v) in object(update)? {
                if let Some(old) = self.get(k) {
                    let candidate = Self::parse(&json!({k:v}))?;
                    let new = candidate.get(k).unwrap();
                    if old.kind != new.kind || (old.text && !new.text) {
                        return Err(invalid(format!("incompatible schema change for {k}")));
                    }
                }
                value[k] = v.clone();
            }
        }
        for row in rows {
            for (k, v) in object(row)? {
                if k == "id" || value.get(k).is_some() || v.is_null() {
                    continue;
                }
                let kind = if k == "vector" && v.is_array() {
                    format!("[{}]f32", v.as_array().unwrap().len())
                } else if v.is_string() {
                    "string".into()
                } else if v.is_boolean() {
                    "bool".into()
                } else if v.as_i64().is_some() {
                    "int".into()
                } else if v.as_u64().is_some() {
                    "uint".into()
                } else if v.is_number() {
                    "float".into()
                } else {
                    return Err(unsupported(&format!("attribute type for {k}")));
                };
                value[k] = json!({"type":kind});
            }
        }
        Self::parse(&value)
    }
    pub fn validate_row(&self, row: &Value, metric: &str) -> Result<()> {
        id_key(row.get("id").ok_or_else(|| invalid("row requires id"))?)?;
        for (k, v) in object(row)? {
            if k == "id" {
                continue;
            }
            if let Some(f) = self.get(k) {
                f.validate(v)?;
                if f.dimension().is_some() && !v.is_null() {
                    f.validate_vector(v, metric)?;
                }
            } else if !v.is_null() {
                return Err(invalid(format!("unknown attribute {k}")));
            }
        }
        Ok(())
    }
    pub async fn install(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        table: &str,
        old: &Schema,
        metric: &str,
    ) -> Result<()> {
        for f in self.fields() {
            let col = f.column();
            if old.get(&f.name).is_none() {
                sqlx::query(&format!(
                    "ALTER TABLE layer_pgvector.\"{table}\" ADD COLUMN \"{col}\" {}",
                    f.sql_type()
                ))
                .execute(&mut **tx)
                .await
                .map_err(db)?;
                // Existing rows may contain an attribute that was previously only
                // null. New declarations therefore need no data conversion.
                if f.dimension().is_some() {
                    let ops = if metric == "cosine_distance" {
                        "vector_cosine_ops"
                    } else {
                        "vector_l2_ops"
                    };
                    sqlx::query(&format!("CREATE INDEX ON layer_pgvector.\"{table}\" USING hnsw (\"{col}\" {ops}) WITH (m=16,ef_construction=64)"))
                        .execute(&mut **tx).await.map_err(db)?;
                } else if f.scalar_index() {
                    // Text attributes can exceed PostgreSQL's B-tree tuple
                    // limit. BM25 owns text indexing; scalar text filters scan.
                    sqlx::query(&format!(
                        "CREATE INDEX ON layer_pgvector.\"{table}\" (\"{col}\")"
                    ))
                    .execute(&mut **tx)
                    .await
                    .map_err(db)?;
                }
            }
            if f.text && !old.get(&f.name).is_some_and(|f| f.text) {
                let options = json!({col.clone():{"tokenizer":{"type":"default"}}});
                // The only interpolated strings are hash identifiers and a JSON
                // document made exclusively from those identifiers and constants.
                sqlx::query(&format!("CREATE INDEX \"{}\" ON layer_pgvector.\"{table}\" USING bm25 (rid,\"{col}\") WITH (key_field='rid',text_fields='{}')",identifier("b_",table),options))
                    .execute(&mut **tx).await.map_err(db)?;
            }
        }
        Ok(())
    }
}
pub(super) fn rows(body: &Value) -> Result<Vec<Value>> {
    if body.get("upsert_rows").is_some() && body.get("upsert_columns").is_some() {
        return Err(invalid(
            "upsert_rows and upsert_columns are mutually exclusive",
        ));
    }
    if let Some(rows) = body.get("upsert_rows") {
        let rows = rows
            .as_array()
            .ok_or_else(|| invalid("upsert_rows must be an array"))?;
        for row in rows {
            object(row)?;
        }
        return Ok(rows.clone());
    }
    if let Some(columns) = body.get("upsert_columns") {
        let columns = object(columns)?;
        let count = columns
            .get("id")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("upsert_columns requires id array"))?
            .len();
        if !columns
            .values()
            .all(|v| v.as_array().is_some_and(|a| a.len() == count))
        {
            return Err(invalid("column lengths must match"));
        }
        return Ok((0..count)
            .map(|i| {
                Value::Object(
                    columns
                        .iter()
                        .map(|(k, v)| (k.clone(), v[i].clone()))
                        .collect(),
                )
            })
            .collect());
    }
    Ok(vec![])
}
