use super::*;

/// What a `{"$ref_new": attribute}` filter value resolves to. Query filters
/// reject it; an upsert condition reads the proposed row; a delete condition
/// sees null for every attribute, as on the Turbopuffer wire.
#[derive(Clone, Copy, PartialEq)]
pub(super) enum Refs {
    Rejected,
    Excluded,
    Null,
}

pub(super) fn compile(
    sql: &mut QueryBuilder<'_, Postgres>,
    schema: &Schema,
    value: &Value,
    depth: usize,
    refs: Refs,
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
                compile(sql, schema, &a[1], depth + 1, refs)?;
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
                    compile(sql, schema, child, depth + 1, refs)?;
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
            comparison(sql, schema, field, "Eq", v, refs)?;
        }
        sql.push(")");
    } else {
        comparison(sql, schema, field, op, &a[2], refs)?;
    }
    Ok(())
}

/// Resolve `{"$ref_new": attribute}`. `Some(None)` references the id.
fn reference<'a>(
    schema: &'a Schema,
    field: Option<&schema::Field>,
    v: &Value,
    refs: Refs,
) -> Result<Option<Option<&'a schema::Field>>> {
    let Some(object) = v.as_object() else {
        return Ok(None);
    };
    let name = object
        .get("$ref_new")
        .and_then(Value::as_str)
        .filter(|_| object.len() == 1)
        .ok_or_else(|| invalid("filter value must be a scalar or {\"$ref_new\": attribute}"))?;
    if refs == Refs::Rejected {
        return Err(invalid("$ref_new is only valid in a write condition"));
    }
    let target = if name == "id" {
        None
    } else {
        Some(
            schema
                .get(name)
                .ok_or_else(|| invalid(format!("unknown $ref_new attribute {name}")))?,
        )
    };
    let same = match (field, target) {
        (None, None) => true,
        (Some(f), Some(t)) => f.same_type(t),
        _ => false,
    };
    if !same {
        return Err(invalid(format!(
            "$ref_new {name} must have the compared attribute's type"
        )));
    }
    Ok(Some(target))
}

fn comparison(
    sql: &mut QueryBuilder<'_, Postgres>,
    schema: &Schema,
    field: Option<&schema::Field>,
    op: &str,
    v: &Value,
    refs: Refs,
) -> Result<()> {
    let reference = reference(schema, field, v, refs)?;
    if reference.is_none() {
        if let Some(f) = field {
            f.validate(v)?;
        } else if !v.is_null() {
            id_key(v)?;
        }
    }
    let ordered = !["Eq", "NotEq"].contains(&op);
    // Ordered comparisons use the same keys as rank_by ordering, so paging by
    // advancing a filter on the order attribute visits every row once.
    // An upsert condition qualifies the stored row; `excluded` is the new one.
    let own = if refs == Refs::Excluded { "cur." } else { "" };
    let lhs = match field {
        None if ordered => id_order(&format!("{own}data")),
        None => format!("{own}key"),
        Some(f) => f.order_expr(own),
    };
    if ordered {
        // Ordering comparisons on missing/null are false; Not then yields true.
        sql.push("COALESCE(").push(lhs).push(match op {
            "Gt" => " > ",
            "Gte" => " >= ",
            "Lt" => " < ",
            "Lte" => " <= ",
            _ => unreachable!(),
        });
    } else {
        sql.push(lhs).push(if op == "Eq" {
            " IS NOT DISTINCT FROM "
        } else {
            " IS DISTINCT FROM "
        });
    }
    match (reference, refs) {
        (Some(_), Refs::Null) => {
            sql.push("NULL");
        }
        (Some(None), _) => {
            sql.push(if ordered {
                id_order("excluded.data")
            } else {
                "excluded.key".into()
            });
        }
        (Some(Some(target)), _) => {
            sql.push(target.order_expr("excluded."));
        }
        (None, _) => match field {
            Some(f) => f.bind(sql, v),
            None if v.is_null() => {
                sql.push_bind(None::<String>);
            }
            None if ordered => {
                sql.push_bind(id_order_key(v)?);
            }
            None => {
                sql.push_bind(id_key(v)?);
            }
        },
    }
    if ordered {
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
            Refs::Rejected,
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
            let e = compile(
                &mut QueryBuilder::new(""),
                &schema,
                &filter,
                0,
                Refs::Rejected,
            )
            .unwrap_err();
            assert_eq!(e.to_string().contains("UnsupportedByStore"), unsupported);
        }
    }
}
