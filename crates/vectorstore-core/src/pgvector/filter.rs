use super::*;

pub(super) fn compile(
    sql: &mut QueryBuilder<'_, Postgres>,
    schema: &Schema,
    value: &Value,
    depth: usize,
) -> Result<()> {
    if depth > 64 {
        return Err(invalid("filter nesting exceeds 64"));
    }
    let a = value
        .as_array()
        .ok_or_else(|| invalid("filter must be an array"))?;
    if a.len() == 2 {
        let op = a[0]
            .as_str()
            .ok_or_else(|| invalid("filter operator must be a string"))?;
        match op {
            "Not" => {
                sql.push("NOT (");
                compile(sql, schema, &a[1], depth + 1)?;
                sql.push(")");
            }
            "And" | "Or" => {
                let children = a[1]
                    .as_array()
                    .ok_or_else(|| invalid("logical filter requires array"))?;
                sql.push("(");
                if children.is_empty() {
                    sql.push(if op == "And" { "TRUE" } else { "FALSE" });
                }
                for (i, child) in children.iter().enumerate() {
                    if i > 0 {
                        sql.push(if op == "And" { " AND " } else { " OR " });
                    }
                    compile(sql, schema, child, depth + 1)?;
                }
                sql.push(")");
            }
            _ => return Err(unsupported(op)),
        }
        return Ok(());
    }
    if a.len() != 3 {
        return Err(invalid(
            "scalar filter requires [attribute, operator, value]",
        ));
    }
    let name = a[0]
        .as_str()
        .ok_or_else(|| invalid("filter attribute must be a string"))?;
    let op = a[1]
        .as_str()
        .ok_or_else(|| invalid("filter operator must be a string"))?;
    if !["Eq", "NotEq", "Gt", "Gte", "Lt", "Lte", "In", "NotIn"].contains(&op) {
        return Err(unsupported(op));
    }
    // IDs keep their JSON type. All other values are compared using declared
    // SQL types. Missing attributes and explicit null both map to SQL NULL.
    let field = if name == "id" {
        None
    } else {
        Some(
            schema
                .get(name)
                .ok_or_else(|| invalid(format!("unknown filter attribute {name}")))?,
        )
    };
    if field.is_some_and(|f| !f.scalar()) {
        return Err(unsupported("vector filter"));
    }
    if field.is_some_and(|f| !f.filterable()) {
        return Err(invalid(format!("attribute {name} is not filterable")));
    }
    let col = quoted(&field.map(|f| f.column()).unwrap_or_else(|| "key".into()));
    if ["In", "NotIn"].contains(&op) {
        let values = a[2]
            .as_array()
            .ok_or_else(|| invalid("In/NotIn requires an array"))?;
        if op == "NotIn" {
            sql.push("NOT ");
        }
        sql.push("(");
        if values.is_empty() {
            sql.push("FALSE");
        }
        for (i, v) in values.iter().enumerate() {
            if i > 0 {
                sql.push(" OR ");
            }
            comparison(sql, field, &col, "Eq", v)?;
        }
        sql.push(")");
    } else {
        comparison(sql, field, &col, op, &a[2])?;
    }
    Ok(())
}
fn comparison(
    sql: &mut QueryBuilder<'_, Postgres>,
    field: Option<&schema::Field>,
    col: &str,
    op: &str,
    v: &Value,
) -> Result<()> {
    if let Some(f) = field {
        f.validate(v)?;
    } else if !v.is_null() {
        id_key(v)?;
    }
    if !["Eq", "NotEq"].contains(&op) && field.is_none() {
        return Err(unsupported("ordered id filter"));
    }
    if ["Eq", "NotEq"].contains(&op) {
        sql.push(col).push(if op == "Eq" {
            " IS NOT DISTINCT FROM "
        } else {
            " IS DISTINCT FROM "
        });
    } else {
        // Ordering comparisons on missing/null are false; Not then yields true.
        sql.push("COALESCE(").push(col).push(match op {
            "Gt" => " > ",
            "Gte" => " >= ",
            "Lt" => " < ",
            "Lte" => " <= ",
            _ => unreachable!(),
        });
    }
    if let Some(f) = field {
        f.bind(sql, v);
    } else {
        sql.push_bind(if v.is_null() { None } else { Some(id_key(v)?) });
    }
    if !["Eq", "NotEq"].contains(&op) {
        sql.push(",FALSE)");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn filters_bind_values_and_generate_only_identifiers() {
        let schema = Schema::parse(&json!({"x'; DROP TABLE t;--":"string"})).unwrap();
        let mut q = QueryBuilder::<Postgres>::new("");
        compile(
            &mut q,
            &schema,
            &json!(["x'; DROP TABLE t;--", "Eq", "'; SELECT 1;--"]),
            0,
        )
        .unwrap();
        assert!(!q.sql().contains("DROP"));
        assert!(!q.sql().contains("SELECT"));
        assert!(q.sql().contains("$1"));
        assert!(q.sql().contains("IS NOT DISTINCT FROM"));
    }
    #[test]
    fn unsupported_and_malformed_are_distinct() {
        let schema = Schema::parse(&json!({"n":"int"})).unwrap();
        for (filter, unsupported) in [
            (json!(["n", "Regex", ".*"]), true),
            (json!(["n", "Eq", "oops"]), false),
        ] {
            let e = compile(&mut QueryBuilder::new(""), &schema, &filter, 0).unwrap_err();
            assert_eq!(e.to_string().contains("UnsupportedByStore"), unsupported);
        }
    }
}
