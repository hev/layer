//! Complete latest-row selection, post-selection predicates and scalar totals.
//! Source adapters must explicitly opt in; no capped ranked-query fallback.
use crate::turbopuffer::{TurbopufferClient, TurbopufferError};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

/// Gateway-owned marker, never a logical data row (including during backfill).
pub const NAMESPACE_META_ID: &str = "_hevlayer:namespace_meta";

type Result<T> = std::result::Result<T, String>;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadConsistency {
    Observed,
    Snapshot,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScanBudget {
    pub max_rows: usize,
    pub max_bytes: usize,
    pub consistency: ReadConsistency,
}
impl ScanBudget {
    pub fn validate(&self) -> Result<()> {
        if self.max_rows == 0
            || self.max_rows > 1_000_000
            || self.max_bytes == 0
            || self.max_bytes > 268_435_456
        {
            return Err("scan budgets require 1..1000000 rows and 1..268435456 bytes".into());
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Asc,
    Desc,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Nulls {
    First,
    Last,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WinnerOrder {
    pub field: String,
    pub direction: Direction,
    pub nulls: Nulls,
    #[serde(default)]
    pub canonical_key: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LatestSpec {
    pub key: Vec<String>,
    pub order_by: Vec<WinnerOrder>,
}
#[derive(Debug, Clone)]
pub struct SelectedRelation {
    pub rows: Vec<Value>,
    pub scope: String,
    pub read_boundary: String,
    pub consistency: ReadConsistency,
    pub complete: bool,
}
#[derive(Debug)]
pub struct RelationSource {
    pub rows: Vec<Value>,
    pub pages: usize,
    pub bytes: usize,
    pub consistency: ReadConsistency,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LatestRequest {
    pub latest_by: LatestSpec,
    pub scan: ScanBudget,
    #[serde(default)]
    pub filters: Option<Value>,
    pub aggregate_by: BTreeMap<String, Value>,
    #[serde(default)]
    pub grouped: BTreeMap<String, crate::grouped_aggregate::GroupRequest>,
}
impl LatestRequest {
    pub fn validate(&self) -> Result<()> {
        self.scan.validate()?;
        if self.grouped.len() > 16
            || self
                .grouped
                .keys()
                .any(|name| name.is_empty() || name.len() > 128)
        {
            return Err("grouped requires at most 16 nonempty names of at most 128 bytes".into());
        }
        self.latest_by.validate()?;
        if let Some(f) = &self.filters {
            validate_filter(f, 0)?;
        }
        if self.aggregate_by.is_empty() || self.aggregate_by.len() > 64 {
            return Err("aggregate_by requires 1..64 metrics".into());
        }
        for plan in self.grouped.values() {
            crate::grouped_aggregate::validate(
                plan,
                &crate::grouped_aggregate::RelationBinding {
                    scope: "preflight".into(),
                    read_boundary: "preflight".into(),
                    complete: true,
                    consistency: crate::grouped_aggregate::ReadConsistency::ObservedScan,
                },
                crate::grouped_aggregate::Limits::default(),
            )
            .map_err(|e| e.to_string())?;
        }
        for (name, spec) in &self.aggregate_by {
            if name.is_empty() {
                return Err("metric name is empty".into());
            }
            metric(spec)?;
        }
        Ok(())
    }
    /// Scalar plans need only referenced literal attributes. Group cursors bind
    /// complete prefilter winners, so any grouped plan must retain whole rows.
    pub fn source_projection(&self) -> Result<Option<Vec<String>>> {
        self.validate()?;
        if !self.grouped.is_empty() {
            return Ok(None);
        }
        let mut fields = std::collections::BTreeSet::from(["id".to_string()]);
        fields.extend(self.latest_by.key.iter().cloned());
        for term in &self.latest_by.order_by {
            fields.insert(term.field.clone());
            fields.extend(term.canonical_key.iter().cloned());
        }
        fn dependencies(filter: &Value, fields: &mut std::collections::BTreeSet<String>) {
            // The complete filter has already passed validate_filter. Traverse
            // every logical branch, even branches evaluation could short circuit.
            let terms = filter.as_array().expect("validated filter");
            if terms.len() == 3 {
                fields.insert(terms[0].as_str().expect("validated field").into());
            } else if terms[0] == "Not" {
                dependencies(&terms[1], fields);
            } else {
                for child in terms[1].as_array().expect("validated logical filter") {
                    dependencies(child, fields);
                }
            }
        }
        if let Some(filter) = &self.filters {
            dependencies(filter, &mut fields);
        }
        for spec in self.aggregate_by.values() {
            if let Some(field) = metric(spec)? {
                fields.insert(field.into());
            }
        }
        // Attribute access uses literal names (including dots), never JSON paths.
        Ok(Some(fields.into_iter().collect()))
    }
    pub fn execute(&self, source: RelationSource, scope: String) -> Result<Value> {
        self.validate()?;
        if source.consistency != self.scan.consistency {
            return Err("source consistency does not match request".into());
        }
        let source_rows = source.rows.len();
        if source_rows > self.scan.max_rows || source.bytes > self.scan.max_bytes {
            return Err("latest relation scan budget exceeded; no partial result".into());
        }
        // Bind the entire validated typed request, excluding continuation values.
        // All named plans participate, including plans without a cursor this page.
        let mut identity = self.clone();
        for plan in identity.grouped.values_mut() {
            plan.cursor = None;
        }
        let identity = serde_json::to_value(identity).map_err(|e| e.to_string())?;
        // Value object maps serialize canonically in this crate (sorted keys).
        let identity = serde_json::to_vec(&identity).map_err(|e| e.to_string())?;
        let scope = json!({
            "executor": scope,
            "query": format!("{:x}", Sha256::digest(identity)),
        })
        .to_string();
        let mut relation = self
            .latest_by
            .select(source.rows, scope, source.consistency)?;
        let selected_rows = relation.rows.len();
        // Selection returns winners in stable logical-key order. Bind every typed
        // attribute before predicates discard rows, including excluded winners.
        // Losing revisions do not participate; this is still an observed scan.
        let mut content = Sha256::new();
        content.update(b"layer-latest-winners-v1\0");
        content.update((selected_rows as u64).to_be_bytes());
        for row in &relation.rows {
            let bytes = serde_json::to_vec(row).map_err(|e| e.to_string())?;
            content.update((bytes.len() as u64).to_be_bytes());
            content.update(bytes);
        }
        relation.scope = json!({
            "relation": relation.scope,
            "selected_content": format!("{:x}", content.finalize()),
        })
        .to_string();
        if let Some(filter) = &self.filters {
            relation.rows = relation
                .rows
                .into_iter()
                .filter_map(|row| match matches_filter(&row, filter) {
                    Ok(true) => Some(Ok(row)),
                    Ok(false) => None,
                    Err(e) => Some(Err(e)),
                })
                .collect::<Result<Vec<_>>>()?;
        }
        let totals = aggregate(&relation.rows, &self.aggregate_by)?;
        let mut grouped = BTreeMap::new();
        let count = self.grouped.len().max(1);
        let binding = crate::grouped_aggregate::RelationBinding {
            scope: relation.scope.clone(),
            read_boundary: relation.read_boundary.clone(),
            complete: relation.complete,
            consistency: match relation.consistency {
                ReadConsistency::Observed => {
                    crate::grouped_aggregate::ReadConsistency::ObservedScan
                }
                ReadConsistency::Snapshot => crate::grouped_aggregate::ReadConsistency::Snapshot,
            },
        };
        for (name, plan) in &self.grouped {
            let mut named_binding = binding.clone();
            named_binding.scope = json!({"relation":binding.scope,"name":name}).to_string();
            let limits = crate::grouped_aggregate::Limits {
                max_instances: 10_000_000 / count,
                max_groups: 100_000 / count,
                ..Default::default()
            };
            let page =
                crate::grouped_aggregate::execute(&relation.rows, plan, &named_binding, limits)
                    .map_err(|error| error.to_string())?;
            grouped.insert(name, page);
        }
        Ok(json!({"aggregations":totals,"grouped":grouped,"relation":{
            "complete":true,"consistency":relation.consistency,"read_boundary":relation.read_boundary,
            "source_rows":source_rows,"source_pages":source.pages,"source_bytes":source.bytes,
            "selected_rows":selected_rows,"matching_rows":relation.rows.len(),"plan":"bounded_source_scan"
        }}))
    }
}
impl LatestSpec {
    pub fn validate(&self) -> Result<()> {
        if self.key.is_empty()
            || self.key.len() > 8
            || self.key.iter().any(|k| k.is_empty() || k == "vector")
        {
            return Err("latest_by.key requires 1..8 nonempty scalar attributes".into());
        }
        if self.order_by.is_empty() || self.order_by.len() > 16 {
            return Err("latest_by.order_by requires 1..16 terms".into());
        }
        for order in &self.order_by {
            if order.field.is_empty()
                || order.field == "vector"
                || order
                    .canonical_key
                    .as_ref()
                    .is_some_and(|k| order.field != "id" || !self.key.contains(k))
            {
                return Err(
                    "invalid order term or canonical_key (must be a logical key on id)".into(),
                );
            }
        }
        Ok(())
    }
    pub fn select(
        &self,
        rows: Vec<Value>,
        scope: String,
        consistency: ReadConsistency,
    ) -> Result<SelectedRelation> {
        self.validate()?;
        let mut winners: BTreeMap<String, Value> = BTreeMap::new();
        let mut types = HashMap::new();
        let mut numeric_order_domains: HashMap<String, u8> = HashMap::new();
        let mut id_type = None;
        let mut physical_ids = std::collections::HashSet::new();
        for row in rows {
            if !row.is_object() {
                return Err("source row must be an object".into());
            }
            let id = attr(&row, "id");
            if !id.as_str().is_some_and(|s| !s.is_empty()) && id.as_u64().is_none() {
                return Err("source row requires a nonempty string or uint id".into());
            }
            ensure_type(&mut id_type, id)?;
            if !physical_ids.insert(id.to_string()) {
                return Err("duplicate physical ID in source relation".into());
            }
            let mut key = Vec::new();
            for field in &self.key {
                let v = attr(&row, field);
                if v.is_null() || v.as_str() == Some("") || v.is_array() || v.is_object() {
                    return Err(format!("missing or nonscalar logical key: {field}"));
                }
                ensure_type(types.entry(field.clone()).or_insert(None), v)?;
                key.push(normalize_key(v));
            }
            for term in &self.order_by {
                if term.canonical_key.is_none() {
                    ensure_numeric_order_domain(
                        numeric_order_domains.entry(term.field.clone()).or_default(),
                        attr(&row, &term.field),
                    )?;
                    ensure_type(
                        types.entry(term.field.clone()).or_insert(None),
                        attr(&row, &term.field),
                    )?;
                }
            }
            let key = serde_json::to_string(&key).map_err(|e| e.to_string())?;
            if let Some(old) = winners.get(&key) {
                if self.compare(&row, old)? == Ordering::Less {
                    winners.insert(key, row);
                }
            } else {
                winners.insert(key, row);
            }
        }
        Ok(SelectedRelation {
            rows: winners.into_values().collect(),
            scope,
            read_boundary: match consistency {
                ReadConsistency::Observed => "observed_pages",
                ReadConsistency::Snapshot => "source_statement",
            }
            .into(),
            consistency,
            complete: true,
        })
    }
    fn compare(&self, a: &Value, b: &Value) -> Result<Ordering> {
        for term in &self.order_by {
            let (av, bv) = if let Some(key) = &term.canonical_key {
                (
                    Value::Bool(normalize_key(attr(a, "id")) == normalize_key(attr(a, key))),
                    Value::Bool(normalize_key(attr(b, "id")) == normalize_key(attr(b, key))),
                )
            } else {
                (attr(a, &term.field).clone(), attr(b, &term.field).clone())
            };
            let cmp = if av.is_null() || bv.is_null() {
                match (av.is_null(), bv.is_null(), term.nulls) {
                    (true, true, _) => Ordering::Equal,
                    (true, false, Nulls::First) | (false, true, Nulls::Last) => Ordering::Less,
                    _ => Ordering::Greater,
                }
            } else {
                let c = compare_scalar(&av, &bv)?;
                match term.direction {
                    Direction::Asc => c,
                    Direction::Desc => c.reverse(),
                }
            };
            if cmp != Ordering::Equal {
                return Ok(cmp);
            }
        }
        Ok(compare_scalar(attr(a, "id"), attr(b, "id"))?.reverse())
    }
}
fn normalize_key(value: &Value) -> Value {
    if value.is_number() && value.as_i64().is_none() && value.as_u64().is_none() {
        if let Some(f) = value.as_f64() {
            if f.fract() == 0.0 && f >= i64::MIN as f64 && f < 9_223_372_036_854_775_808.0 {
                return json!(f as i64);
            }
            if f.fract() == 0.0 && (0.0..18_446_744_073_709_551_616.0).contains(&f) {
                return json!(f as u64);
            }
        }
    }
    value.clone()
}
fn attr<'a>(row: &'a Value, field: &str) -> &'a Value {
    row.get(field).unwrap_or(&Value::Null)
}
fn ensure_type(kind: &mut Option<u8>, value: &Value) -> Result<()> {
    let new = match value {
        Value::Null => return Ok(()),
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        _ => return Err("ordering/key value must be scalar".into()),
    };
    if kind.is_some_and(|k| k != new) {
        return Err("mixed attribute types in latest relation".into());
    }
    *kind = Some(new);
    Ok(())
}
// Validate the whole order domain, even if a particular incompatible pair
// would never meet during winner replacement. Otherwise errors depend on input order.
fn ensure_numeric_order_domain(domain: &mut u8, value: &Value) -> Result<()> {
    if let Value::Number(number) = value {
        let integer = number
            .as_i64()
            .map(i128::from)
            .or_else(|| number.as_u64().map(i128::from));
        if let Some(integer) = integer {
            if integer.abs() > (1i128 << 53) {
                *domain |= 2;
            }
        } else {
            *domain |= 1;
        }
        if *domain == 3 {
            return Err("mixed fractional/large integer ordering is unsupported".into());
        }
    }
    Ok(())
}

fn compare_scalar(a: &Value, b: &Value) -> Result<Ordering> {
    match (a, b) {
        (Value::String(a), Value::String(b)) => Ok(a.cmp(b)),
        (Value::Bool(a), Value::Bool(b)) => Ok(a.cmp(b)),
        (Value::Number(a), Value::Number(b)) => {
            let ai = a
                .as_i64()
                .map(i128::from)
                .or_else(|| a.as_u64().map(i128::from));
            let bi = b
                .as_i64()
                .map(i128::from)
                .or_else(|| b.as_u64().map(i128::from));
            if let (Some(a), Some(b)) = (ai, bi) {
                return Ok(a.cmp(&b));
            }
            if ai.is_some_and(|v| v.abs() > (1i128 << 53))
                || bi.is_some_and(|v| v.abs() > (1i128 << 53))
            {
                return Err("mixed fractional/large integer comparison is unsupported".into());
            }
            a.as_f64()
                .and_then(|a| b.as_f64().and_then(|b| a.partial_cmp(&b)))
                .ok_or_else(|| "invalid numeric comparison".into())
        }
        _ => Err("incompatible scalar types".into()),
    }
}
fn metric(spec: &Value) -> Result<Option<&str>> {
    let a = spec.as_array().ok_or("metric requires array")?;
    match a.as_slice() {
        [Value::String(op)] if op == "Count" => Ok(None),
        [Value::String(op), Value::String(field)]
            if op == "Sum" && !field.is_empty() && field != "vector" =>
        {
            Ok(Some(field))
        }
        _ => Err("only [Count] and [Sum, attribute] are supported".into()),
    }
}
pub fn aggregate(rows: &[Value], metrics: &BTreeMap<String, Value>) -> Result<Value> {
    let mut out = Map::new();
    for (name, spec) in metrics {
        let Some(field) = metric(spec)? else {
            out.insert(name.clone(), json!(rows.len()));
            continue;
        };
        let mut integer = 0i128;
        let mut fraction = 0f64;
        let mut correction = 0f64;
        let mut fractional = false;
        for row in rows {
            let v = attr(row, field);
            if v.is_null() {
                continue;
            }
            if let Some(i) = v
                .as_i64()
                .map(i128::from)
                .or_else(|| v.as_u64().map(i128::from))
            {
                integer = integer.checked_add(i).ok_or("integer sum overflow")?;
            } else if let Some(f) = v.as_f64().filter(|_| v.is_number()) {
                fractional = true;
                let next = fraction + f;
                correction += if fraction.abs() >= f.abs() {
                    (fraction - next) + f
                } else {
                    (f - next) + fraction
                };
                fraction = next;
            } else {
                return Err(format!("Sum requires numeric or null values: {field}"));
            }
        }
        let value = if fractional {
            if integer.abs() > (1i128 << 53) {
                return Err("fractional sum with large integer total is unsupported".into());
            }
            let total = (fraction + correction) + integer as f64;
            if !total.is_finite() {
                return Err("fractional sum overflow".into());
            }
            json!(total)
        } else if let Ok(i) = i64::try_from(integer) {
            json!(i)
        } else if let Ok(i) = u64::try_from(integer) {
            json!(i)
        } else {
            return Err("integer sum exceeds JSON int64/uint64".into());
        };
        out.insert(name.clone(), value);
    }
    Ok(Value::Object(out))
}
fn validate_filter(f: &Value, depth: usize) -> Result<()> {
    if depth > 32 {
        return Err("filter nesting exceeds 32".into());
    }
    let a = f.as_array().ok_or("filter requires array")?;
    if a.len() == 2 {
        match a[0].as_str() {
            Some("And" | "Or") => {
                for child in a[1].as_array().ok_or("logical filter requires array")? {
                    validate_filter(child, depth + 1)?;
                }
            }
            Some("Not") => validate_filter(&a[1], depth + 1)?,
            _ => return Err("unsupported logical filter".into()),
        }
    } else if a.len() == 3
        && a[0]
            .as_str()
            .is_some_and(|s| !s.is_empty() && s != "vector")
    {
        match a[1].as_str() {
            Some("Eq" | "NotEq") if !a[2].is_array() && !a[2].is_object() => {}
            Some("Gt" | "Gte" | "Lt" | "Lte") if a[2].is_number() || a[2].is_string() => {}
            Some("Contains" | "NotContains") if !a[2].is_array() && !a[2].is_object() => {}
            Some("In" | "NotIn" | "ContainsAny" | "NotContainsAny")
                if a[2]
                    .as_array()
                    .is_some_and(|vs| vs.iter().all(|v| !v.is_array() && !v.is_object())) => {}
            _ => return Err("unsupported filter operator/value".into()),
        }
    } else {
        return Err("invalid filter shape".into());
    }
    Ok(())
}
pub fn matches_filter(row: &Value, filter: &Value) -> Result<bool> {
    validate_filter(filter, 0)?;
    evaluate_filter(row, filter)
}
fn equal(a: &Value, b: &Value) -> Result<bool> {
    if a.is_null() || b.is_null() {
        return Ok(a.is_null() && b.is_null());
    }
    Ok(compare_scalar(a, b)? == Ordering::Equal)
}
fn evaluate_filter(row: &Value, f: &Value) -> Result<bool> {
    let a = f.as_array().ok_or("invalid filter")?;
    if a.len() == 2 {
        return match a[0].as_str() {
            Some("Not") => Ok(!evaluate_filter(row, &a[1])?),
            Some("And" | "Or") => {
                let and = a[0] == "And";
                let mut result = and;
                // Evaluate every child, including type errors in otherwise short-circuited terms.
                for child in a[1].as_array().ok_or("invalid logical filter")? {
                    let value = evaluate_filter(row, child)?;
                    result = if and {
                        result && value
                    } else {
                        result || value
                    };
                }
                Ok(result)
            }
            _ => Err("invalid logical operator".into()),
        };
    }
    let value = attr(row, a[0].as_str().ok_or("invalid field")?);
    let target = &a[2];
    let op = a[1].as_str().ok_or("invalid operator")?;
    match op {
        "Eq" => equal(value, target),
        "NotEq" => Ok(!equal(value, target)?),
        "In" | "NotIn" => {
            let mut found = false;
            for t in target.as_array().ok_or("invalid In")? {
                found |= equal(value, t)?;
            }
            Ok(if op == "In" { found } else { !found })
        }
        "Contains" | "NotContains" | "ContainsAny" | "NotContainsAny" => {
            let mut found = false;
            if !value.is_null() {
                for v in value
                    .as_array()
                    .ok_or("Contains requires array attribute")?
                {
                    if op.ends_with("Any") {
                        for t in target.as_array().ok_or("invalid ContainsAny")? {
                            found |= equal(v, t)?;
                        }
                    } else {
                        found |= equal(v, target)?;
                    }
                }
            }
            Ok(if op.starts_with("Not") { !found } else { found })
        }
        _ if value.is_null() => Ok(false),
        _ => {
            let c = compare_scalar(value, target)?;
            Ok(match op {
                "Gt" => c.is_gt(),
                "Gte" => c.is_ge(),
                "Lt" => c.is_lt(),
                "Lte" => c.is_le(),
                _ => return Err("invalid comparison".into()),
            })
        }
    }
}

/// Semi/anti relation membership. Both inputs must already be latest-selected.
/// This does not promise a common read snapshot between independently read sources.
pub fn join_selected(
    local: SelectedRelation,
    foreign: &SelectedRelation,
    local_key: &str,
    foreign_key: &str,
    foreign_filter: &Value,
    anti: bool,
) -> Result<SelectedRelation> {
    if !local.complete || !foreign.complete {
        return Err("join requires complete relations".into());
    }
    validate_filter(foreign_filter, 0)?;
    let mut members = std::collections::HashSet::new();
    for row in &foreign.rows {
        let key = attr(row, foreign_key);
        if key.is_null() {
            return Err("foreign join key is missing".into());
        }
        if matches_filter(row, foreign_filter)? {
            members.insert(key.to_string());
        }
    }
    let mut rows = Vec::new();
    for row in local.rows {
        let key = attr(&row, local_key);
        if key.is_null() {
            return Err("local join key is missing".into());
        }
        if members.contains(&key.to_string()) != anti {
            rows.push(row);
        }
    }
    Ok(SelectedRelation {
        rows,
        read_boundary: "independent_sources".into(),
        consistency: ReadConsistency::Observed,
        ..local
    })
}

/// Complete ID-ordered upstream scan. Strong per-page reads are not a snapshot.
/// Budget exhaustion or malformed/nonadvancing pages aborts, never truncates.
pub async fn scan_ordered_source<C: TurbopufferClient + ?Sized>(
    client: &C,
    namespace: &str,
    budget: &ScanBudget,
) -> std::result::Result<RelationSource, TurbopufferError> {
    scan_ordered_source_projected(client, namespace, budget, None).await
}

// Recognize only the vendor's explicit missing include_attributes diagnostic.
// Never infer absence from metadata: the next request must try projection again.
fn missing_projected_attribute(
    response: &crate::turbopuffer::TurbopufferPassthroughResponse,
    fields: &[String],
) -> bool {
    if response.status != 400 {
        return false;
    }
    let Ok(body) = serde_json::from_slice::<Value>(&response.body) else {
        return false;
    };
    if body.get("status").and_then(Value::as_str) != Some("error") {
        return false;
    }
    let Some(message) = body.get("error").and_then(Value::as_str) else {
        return false;
    };
    let message = message.strip_prefix("💔 ").unwrap_or(message);
    fields.iter().any(|field| message == format!("attribute \"{field}\" not found in schema, cannot be part of `include_attributes`. consider passing `include_attributes=True` to return all attribute data instead"))
}

/// Shared accepted-row and decoded-response-byte limits for one complete read.
/// No task is spawned: dropping fan-out drops every pending provider future.
#[derive(Debug)]
pub struct SourceBudget {
    budget: ScanBudget,
    used: std::sync::Mutex<(usize, usize)>,
}
impl SourceBudget {
    pub fn new(budget: ScanBudget) -> Self {
        Self {
            budget,
            used: std::sync::Mutex::new((0, 0)),
        }
    }
    fn limit(&self) -> usize {
        let used = self.used.lock().unwrap_or_else(|e| e.into_inner());
        1000.min(
            self.budget
                .max_rows
                .saturating_sub(used.0)
                .saturating_add(1),
        )
    }
    fn charge(&self, rows: usize, bytes: usize) -> std::result::Result<(), TurbopufferError> {
        let mut used = self.used.lock().unwrap_or_else(|e| e.into_inner());
        used.0 = used.0.saturating_add(rows);
        used.1 = used.1.saturating_add(bytes);
        if used.1 > self.budget.max_bytes {
            return Err(TurbopufferError::Other(
                "latest relation: scan byte budget exceeded; no partial result".into(),
            ));
        }
        if used.0 > self.budget.max_rows {
            return Err(TurbopufferError::Other(
                "latest relation: scan row budget exceeded; no partial result".into(),
            ));
        }
        Ok(())
    }
}

/// Only an adapter with explicit projection support may call this path.
/// Projection reduces returned attributes, not source row scans or billing.
pub async fn scan_ordered_source_projected<C: TurbopufferClient + ?Sized>(
    client: &C,
    namespace: &str,
    budget: &ScanBudget,
    fields: Option<&[String]>,
) -> std::result::Result<RelationSource, TurbopufferError> {
    scan_ordered_partition_projected(
        client,
        namespace,
        &SourceBudget::new(budget.clone()),
        fields,
        None,
    )
    .await
}

/// One independent complete physical partition; callers share accounting.
pub async fn scan_ordered_partition_projected<C: TurbopufferClient + ?Sized>(
    client: &C,
    namespace: &str,
    shared: &SourceBudget,
    fields: Option<&[String]>,
    partition: Option<&Value>,
) -> std::result::Result<RelationSource, TurbopufferError> {
    scan_ordered_partition_projected_strict(client, namespace, shared, fields, partition, false)
        .await
}

/// Complete source reads and their topology discovery must fail before using rows.
pub(crate) fn reject_source_error_envelope(
    body: &Value,
) -> std::result::Result<(), TurbopufferError> {
    if body.get("error").is_some()
        || body.get("errors").is_some()
        || body.get("status").and_then(Value::as_str) == Some("error")
    {
        return Err(TurbopufferError::Other(
            "latest relation: source response contains an error envelope".into(),
        ));
    }
    Ok(())
}

/// Strict projections never widen into full trace attributes on schema errors.
pub async fn scan_ordered_partition_projected_strict<C: TurbopufferClient + ?Sized>(
    client: &C,
    namespace: &str,
    shared: &SourceBudget,
    fields: Option<&[String]>,
    partition: Option<&Value>,
    strict: bool,
) -> std::result::Result<RelationSource, TurbopufferError> {
    let budget = &shared.budget;
    let fail = |message: &str| TurbopufferError::Other(format!("latest relation: {message}"));
    budget.validate().map_err(|e| fail(&e))?;
    if budget.consistency != ReadConsistency::Observed {
        return Err(crate::capabilities::unsupported_by_store(
            "turbopuffer",
            "latest_by",
            Some("snapshot source consistency is unavailable"),
        ));
    }
    let mut fields = fields;
    let mut rows = Vec::new();
    let mut bytes: usize = 0;
    let mut pages = 0;
    let mut cursor: Option<Value> = None;
    let path = format!(
        "/v2/namespaces/{}/query",
        percent_encoding::utf8_percent_encode(namespace, percent_encoding::NON_ALPHANUMERIC)
    );
    loop {
        let limit = shared.limit();
        let mut body = json!({"rank_by":["id","asc"],"top_k":limit,"include_attributes":true,"consistency":{"level":"strong"}});
        if let Some(fields) = fields {
            body["include_attributes"] = json!(fields);
        }
        let continuation = cursor.as_ref().map(|id| json!(["id", "Gt", id]));
        body["filters"] = match (partition, continuation) {
            (Some(partition), Some(after)) => json!(["And", [partition, after]]),
            (Some(partition), None) => partition.clone(),
            (None, Some(after)) => after,
            (None, None) => Value::Null,
        };
        if body["filters"].is_null() {
            body.as_object_mut().unwrap().remove("filters");
        }
        let response = match client.passthrough("POST", &path, None, Some(body)).await {
            Ok(response) | Err(TurbopufferError::Response(response)) => response,
            Err(error) => return Err(error),
        };
        let retry_full = !strict
            && pages == 0
            && fields.is_some_and(|fields| missing_projected_attribute(&response, fields));
        if !(200..300).contains(&response.status) && !retry_full {
            return Err(TurbopufferError::Response(response));
        }
        pages += 1;
        bytes = bytes
            .checked_add(response.body.len())
            .ok_or_else(|| fail("byte counter overflow"))?;
        let body: Value =
            serde_json::from_slice(&response.body).map_err(|_| fail("invalid source JSON"))?;
        crate::turbopuffer::observe_projected_billing(namespace, &body);
        shared.charge(0, response.body.len())?;
        if retry_full {
            // No successful page exists, so there are no rows/cursors to mix.
            // Keep the failed attempt's bytes and query count in the budget/result.
            fields = None;
            continue;
        }
        reject_source_error_envelope(&body)?;
        let page = body
            .get("rows")
            .and_then(Value::as_array)
            .ok_or_else(|| fail("source response requires rows"))?;
        if page.len() > limit {
            return Err(fail("source exceeded requested page size"));
        }
        shared.charge(
            page.iter()
                .filter(|row| attr(row, "id") != NAMESPACE_META_ID)
                .count(),
            0,
        )?;
        for row in page {
            let id = attr(row, "id");
            if !id.as_str().is_some_and(|s| !s.is_empty()) && id.as_u64().is_none() {
                return Err(fail("source row has invalid id"));
            }
            if let Some(previous) = &cursor {
                if compare_scalar(id, previous).map_err(|e| fail(&e))? != Ordering::Greater {
                    return Err(fail("source IDs are not strictly advancing"));
                }
            }
            cursor = Some(id.clone());
            // An unready/backfilling topology uses the unpartitioned cursor.
            // Skip its marker here without a string-ID filter on uint stores.
            if id != NAMESPACE_META_ID {
                rows.push(row.clone());
            }
        }
        if page.len() < limit {
            break;
        }
    }
    Ok(RelationSource {
        rows,
        pages,
        bytes,
        consistency: ReadConsistency::Observed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_schema_diagnostic_requires_exact_requested_field_and_vendor_shape() {
        use crate::turbopuffer::TurbopufferPassthroughResponse;
        let message = "attribute \"absent\" not found in schema, cannot be part of `include_attributes`. consider passing `include_attributes=True` to return all attribute data instead";
        let mut response = TurbopufferPassthroughResponse {
            status: 400,
            content_type: Some("application/json".into()),
            body: serde_json::to_vec(&json!({"status":"error","error":message})).unwrap(),
        };
        assert!(missing_projected_attribute(&response, &["absent".into()]));
        assert!(!missing_projected_attribute(&response, &["other".into()]));
        response.body = serde_json::to_vec(&json!({"error":message})).unwrap();
        assert!(!missing_projected_attribute(&response, &["absent".into()]));
        response.body = b"unknown transport/server error".to_vec();
        assert!(!missing_projected_attribute(&response, &["absent".into()]));
    }

    #[tokio::test]
    async fn absent_schema_projection_fallback_is_bounded_and_not_cached() {
        use crate::turbopuffer::HttpTurbopufferClient;
        use axum::{routing::post, Json, Router};
        use std::sync::{Arc, Mutex};
        let seen = Arc::new(Mutex::new(Vec::<Value>::new()));
        let observed = seen.clone();
        let missing = json!({"status":"error","error":"💔 attribute \"absent\" not found in schema, cannot be part of `include_attributes`. consider passing `include_attributes=True` to return all attribute data instead"});
        let mut missing = missing;
        missing["billing"] = json!({"billable_logical_bytes_queried":37});
        let error_len = serde_json::to_vec(&missing).unwrap().len();
        let app = Router::new().route(
            "/v2/namespaces/ns/query",
            post(move |Json(body): Json<Value>| {
                let observed = observed.clone();
                let missing = missing.clone();
                async move {
                    let mut seen = observed.lock().unwrap();
                    let first = seen.is_empty();
                    seen.push(body.clone());
                    if first {
                        return (axum::http::StatusCode::BAD_REQUEST, Json(missing));
                    }
                    // A later request sees the actual schema reappear, without metadata/cache.
                    let row = if body["include_attributes"].is_array() {
                        json!({"id":"a","entity":"one","version":1,"absent":7})
                    } else {
                        json!({"id":"a","entity":"one","version":1})
                    };
                    (axum::http::StatusCode::OK, Json(json!({"rows":[row]})))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = HttpTurbopufferClient::new("fixture", &format!("http://{addr}"));
        let mut req = request();
        req.filters = Some(json!([
            "And",
            [["absent", "Eq", null], ["Not", ["absent", "Eq", 1]]]
        ]));
        req.aggregate_by
            .insert("sum".into(), json!(["Sum", "absent"]));
        // Ordering also depends on an attribute absent from the entire schema.
        req.latest_by.order_by[0].field = "absent".into();
        let billed = Arc::new(Mutex::new(Vec::new()));
        let receipts = billed.clone();
        let observer: crate::turbopuffer::ReadBillingObserver = Arc::new(move |_, billing| {
            receipts.lock().unwrap().push(billing.clone());
        });
        let source = crate::turbopuffer::scope_read_billing(
            observer,
            client.latest_relation_source_for_request("ns", &req),
        )
        .await
        .unwrap();
        assert_eq!(
            *billed.lock().unwrap(),
            vec![json!({"billable_logical_bytes_queried":37})]
        );
        assert_eq!(source.pages, 2);
        assert_eq!(
            source.bytes,
            error_len
                + serde_json::to_vec(&json!({"rows":[{"id":"a","entity":"one","version":1}]}))
                    .unwrap()
                    .len()
        );
        // A schema-absent logical/canonical key must retain the executor's
        // missing-key error, rather than being silently pruned from ordering.
        let mut canonical = req.clone();
        canonical.latest_by.key = vec!["absent".into()];
        canonical.latest_by.order_by[1].canonical_key = Some("absent".into());
        let copy = RelationSource {
            rows: source.rows.clone(),
            pages: source.pages,
            bytes: source.bytes,
            consistency: source.consistency,
        };
        assert!(canonical
            .execute(copy, "scope".into())
            .unwrap_err()
            .contains("key"));
        let result = req.execute(source, "scope".into()).unwrap();
        assert_eq!(result["aggregations"], json!({"n":1,"sum":0}));
        req.filters = None;
        let source = client
            .latest_relation_source_for_request("ns", &req)
            .await
            .unwrap();
        assert_eq!(source.pages, 1);
        assert_eq!(
            req.execute(source, "scope".into()).unwrap()["aggregations"],
            json!({"n":1,"sum":7})
        );
        let bodies = seen.lock().unwrap();
        assert_eq!(bodies.len(), 3);
        assert!(bodies[0]["include_attributes"]
            .as_array()
            .unwrap()
            .contains(&json!("absent")));
        assert_eq!(bodies[1]["include_attributes"], true);
        assert_eq!(
            bodies[2]["include_attributes"],
            bodies[0]["include_attributes"]
        );
        server.abort();
    }

    #[tokio::test]
    async fn projection_errors_and_later_schema_loss_never_restart() {
        use crate::turbopuffer::HttpTurbopufferClient;
        use axum::{routing::post, Json, Router};
        use std::sync::{
            atomic::{AtomicUsize, Ordering as AtomicOrdering},
            Arc,
        };
        let missing = json!({"status":"error","error":"💔 attribute \"amount\" not found in schema, cannot be part of `include_attributes`. consider passing `include_attributes=True` to return all attribute data instead"});
        // General/unsupported/auth/server errors, repeated missing field, byte limit,
        // and loss after an accumulated successful page all have exact call bounds.
        for (status, diagnostic, later, byte_limit, expected_calls) in [
            (400, false, false, 1000000, 1),
            (401, true, false, 1000000, 1),
            (403, true, false, 1000000, 1),
            (404, true, false, 1000000, 1),
            (422, true, false, 1000000, 1),
            (500, true, false, 1000000, 1),
            (400, true, false, 1000000, 2),
            (400, true, false, 1, 1),
            (400, true, true, 1000000, 2),
        ] {
            let calls = Arc::new(AtomicUsize::new(0));
            let observed = calls.clone();
            let missing = missing.clone();
            let app = Router::new().route("/v2/namespaces/ns/query", post(move |Json(body): Json<Value>| {
                let observed = observed.clone(); let missing = missing.clone();
                async move {
                    let index = observed.fetch_add(1, AtomicOrdering::SeqCst);
                    if later && index == 0 {
                        assert!(body["include_attributes"].is_array());
                        return (axum::http::StatusCode::OK, Json(json!({"rows":(0..1000).map(|n| json!({"id":format!("{n:04}"),"entity":"one","version":n})).collect::<Vec<_>>()})));
                    }
                    if later { assert!(body["include_attributes"].is_array()); }
                    (axum::http::StatusCode::from_u16(status).unwrap(), Json(if diagnostic { missing } else { json!({"status":"error","error":"unsupported projection"}) }))
                }
            }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let client = HttpTurbopufferClient::new("fixture", &format!("http://{addr}"));
            let mut req = request();
            req.scan.max_bytes = byte_limit;
            assert!(client
                .latest_relation_source_for_request("ns", &req)
                .await
                .is_err());
            assert_eq!(
                calls.load(AtomicOrdering::SeqCst),
                expected_calls,
                "status={status}, later={later}, bytes={byte_limit}"
            );
            // A denied fallback reservation must never dispatch another read.
            if status == 400 && diagnostic && !later && byte_limit > 1 {
                calls.store(0, AtomicOrdering::SeqCst);
                let admissions = Arc::new(AtomicUsize::new(0));
                let admitted = admissions.clone();
                let permit: crate::turbopuffer::QueryPermit = Arc::new(move |_, count| {
                    assert_eq!(count, 1);
                    let index = admitted.fetch_add(1, AtomicOrdering::SeqCst);
                    Box::pin(async move {
                        if index == 0 {
                            Ok(())
                        } else {
                            Err(TurbopufferError::QueryBudgetExhausted)
                        }
                    })
                });
                let result = crate::turbopuffer::scope_query_permit(
                    permit,
                    client.latest_relation_source_for_request("ns", &req),
                )
                .await;
                assert!(matches!(
                    result,
                    Err(TurbopufferError::QueryBudgetExhausted)
                ));
                assert_eq!(admissions.load(AtomicOrdering::SeqCst), 2);
                assert_eq!(calls.load(AtomicOrdering::SeqCst), 1);
            }
            server.abort();
        }
    }

    fn spec() -> LatestSpec {
        serde_json::from_value(json!({"key":["entity"],"order_by":[{"field":"version","direction":"desc","nulls":"last"},{"field":"id","direction":"desc","nulls":"last","canonical_key":"entity"}]})).unwrap()
    }
    fn request() -> LatestRequest {
        serde_json::from_value(json!({"latest_by":spec(),"scan":{"max_rows":20000,"max_bytes":10000000,"consistency":"observed"},"filters":["active","Eq",true],"aggregate_by":{"n":["Count"],"sum":["Sum","amount"]}})).unwrap()
    }
    #[test]
    fn continuation_binds_complete_prefilter_winners() {
        let mut req = request();
        req.grouped = serde_json::from_value(json!({"g":{
            "group_by":[{"kind":"field","field":"entity"}],
            "metrics":{"n":{"kind":"count"}},"page_size":1
        }}))
        .unwrap();
        let rows = vec![
            json!({"id":"a","entity":"a","version":2,"active":true,"amount":3}),
            json!({"id":"b","entity":"b","version":2,"active":true,"amount":4}),
            json!({"id":"c","entity":"c","version":2,"active":false,"amount":1,
                "extra":{"typed":[null,true,1,"1"]}}),
            json!({"id":"c-old","entity":"c","version":1,"active":true,"amount":900}),
        ];
        let run = |req: &LatestRequest, rows: Vec<Value>| {
            req.execute(
                RelationSource {
                    rows,
                    pages: 1,
                    bytes: 100,
                    consistency: ReadConsistency::Observed,
                },
                "same-auth-store-namespace".into(),
            )
        };
        let first = run(&req, rows.clone()).unwrap();
        assert_eq!(first["aggregations"], json!({"n":2,"sum":7}));
        assert_eq!(first["relation"]["selected_rows"], 3);
        assert_eq!(first["relation"]["matching_rows"], 2);
        req.grouped.get_mut("g").unwrap().cursor = Some(
            first["grouped"]["g"]["next_cursor"]
                .as_str()
                .unwrap()
                .into(),
        );
        let mut reordered = rows.clone();
        reordered.reverse();
        let resumed = run(&req, reordered).unwrap();
        assert_eq!(resumed["aggregations"], first["aggregations"]);
        assert_eq!(resumed["grouped"]["g"]["total_groups"], 2);
        assert_eq!(resumed["grouped"]["g"]["groups"][0]["key"][0]["value"], "b");
        // Outer metrics, every other named plan and scan limits bind the cursor.
        let mut changed = req.clone();
        changed
            .aggregate_by
            .insert("extra_sum".into(), json!(["Sum", "amount"]));
        assert!(run(&changed, rows.clone())
            .unwrap_err()
            .contains("continuation"));
        changed = req.clone();
        changed
            .aggregate_by
            .insert("sum".into(), json!(["Sum", "version"]));
        assert!(run(&changed, rows.clone())
            .unwrap_err()
            .contains("continuation"));
        changed = req.clone();
        let mut other = changed.grouped["g"].clone();
        other.cursor = None;
        changed.grouped.insert("other".into(), other);
        let mut other_first = changed.clone();
        other_first.grouped.get_mut("g").unwrap().cursor = None;
        let other_page = run(&other_first, rows.clone()).unwrap();
        changed.grouped.get_mut("g").unwrap().cursor = Some(
            other_page["grouped"]["g"]["next_cursor"]
                .as_str()
                .unwrap()
                .into(),
        );
        assert!(run(&changed, rows.clone()).is_ok());
        changed.grouped.get_mut("other").unwrap().page_size = 2;
        assert!(run(&changed, rows.clone())
            .unwrap_err()
            .contains("continuation"));
        changed = req.clone();
        changed.scan.max_rows += 1;
        assert!(run(&changed, rows.clone())
            .unwrap_err()
            .contains("continuation"));
        changed = req.clone();
        changed.scan.max_bytes += 1;
        assert!(run(&changed, rows.clone())
            .unwrap_err()
            .contains("continuation"));
        let mut losing_change = rows.clone();
        losing_change[3]["amount"] = json!(901);
        assert!(run(&req, losing_change).is_ok());
        for (index, field, value) in [
            (2, "amount", json!(2)),
            (2, "extra", json!({"typed":[null,true,"1","1"]})),
            (0, "amount", json!(5)),
        ] {
            let mut changed = rows.clone();
            changed[index][field] = value;
            let mut fresh = request();
            fresh.grouped = req.grouped.clone();
            fresh.grouped.get_mut("g").unwrap().cursor = None;
            let fresh = run(&fresh, changed.clone()).unwrap();
            if index == 2 {
                assert_eq!(fresh["aggregations"], first["aggregations"]);
                assert_eq!(
                    fresh["grouped"]["g"]["groups"],
                    first["grouped"]["g"]["groups"]
                );
                assert_eq!(fresh["grouped"]["g"]["total_groups"], 2);
            }
            assert!(run(&req, changed).unwrap_err().contains("continuation"));
        }
    }
    #[test]
    fn latest_precedes_predicates_and_whole_row_sum() {
        let r = request()
            .execute(
                RelationSource {
                    rows: vec![
                        json!({"id":"a-old","entity":"a","version":1,"active":true,"amount":900}),
                        json!({"id":"a","entity":"a","version":2,"active":false,"amount":100}),
                        json!({"id":"b","entity":"b","version":2,"active":true,"amount":7}),
                        json!({"id":"b-z","entity":"b","version":2,"active":true,"amount":99}),
                    ],
                    pages: 1,
                    bytes: 100,
                    consistency: ReadConsistency::Observed,
                },
                "test".into(),
            )
            .unwrap();
        assert_eq!(r["aggregations"], json!({"n":1,"sum":7}));
        assert_eq!(r["relation"]["selected_rows"], 2);
        assert!(r.get("rows").is_none());
    }
    #[test]
    fn ties_nulls_missing_and_input_order_are_deterministic() {
        let rows = vec![
            json!({"id":"z","entity":"a","version":9,"amount":4}),
            json!({"id":"a","entity":"a","version":9,"amount":3}),
            json!({"id":"old","entity":"a","amount":100}),
            json!({"id":"b-a","entity":"b","version":null}),
            json!({"id":"b-z","entity":"b"}),
        ];
        let first = spec()
            .select(rows.clone(), "t".into(), ReadConsistency::Observed)
            .unwrap();
        let second = spec()
            .select(
                rows.into_iter().rev().collect(),
                "t".into(),
                ReadConsistency::Observed,
            )
            .unwrap();
        assert_eq!(first.rows, second.rows);
        assert_eq!(first.rows[0]["id"], "a");
        assert_eq!(first.rows[1]["id"], "b-z");
        let m = BTreeMap::from([("s".into(), json!(["Sum", "amount"]))]);
        assert_eq!(aggregate(&first.rows, &m).unwrap(), json!({"s":3}));
        assert_eq!(aggregate(&[], &m).unwrap(), json!({"s":0}));
    }
    #[test]
    fn null_predicates_and_arrays() {
        let row = json!({"a":null,"tags":["x","y"],"n":0});
        for f in [
            json!(["a", "Eq", null]),
            json!(["missing", "Eq", null]),
            json!(["tags", "Contains", "x"]),
            json!(["missing", "NotContains", "x"]),
            json!(["n", "NotIn", [1, 2]]),
        ] {
            assert!(matches_filter(&row, &f).unwrap(), "{f}");
        }
        for f in [
            json!(["missing", "Gt", 0]),
            json!(["missing", "Eq", 0]),
            json!(["tags", "ContainsAny", ["z"]]),
        ] {
            assert!(!matches_filter(&row, &f).unwrap());
        }
        assert!(matches_filter(&row, &json!(["x", "Regex", ".*"])).is_err());
    }
    #[test]
    fn malformed_keys_types_overflows_fail_closed() {
        for row in [
            json!({"id":"x"}),
            json!({"id":"x","entity":null}),
            json!({"id":"x","entity":""}),
            json!({"id":null,"entity":"x"}),
        ] {
            assert!(spec()
                .select(vec![row], "t".into(), ReadConsistency::Observed)
                .is_err());
        }
        assert!(spec()
            .select(
                vec![
                    json!({"id":"x","entity":"x","version":1}),
                    json!({"id":"y","entity":"x","version":"2"})
                ],
                "t".into(),
                ReadConsistency::Observed
            )
            .is_err());
        let m = BTreeMap::from([("s".into(), json!(["Sum", "n"]))]);
        assert!(aggregate(&[json!({"n":u64::MAX}), json!({"n":1})], &m).is_err());
        assert!(aggregate(&[json!({"n":"1"})], &m).is_err());
        assert_eq!(
            compare_scalar(&json!(9007199254740993u64), &json!(9007199254740992u64)).unwrap(),
            Ordering::Greater
        );
    }
    #[test]
    fn latest_grade_and_ungraded_anti_join() {
        let local = spec()
            .select(
                vec![
                    json!({"id":"a","entity":"a"}),
                    json!({"id":"b","entity":"b"}),
                    json!({"id":"c","entity":"c"}),
                ],
                "sessions".into(),
                ReadConsistency::Observed,
            )
            .unwrap();
        let grades = spec()
            .select(
                vec![
                    json!({"id":"g1","entity":"a","version":1,"poor":true}),
                    json!({"id":"g2","entity":"a","version":2,"poor":false}),
                    json!({"id":"g3","entity":"b","poor":true}),
                ],
                "grades".into(),
                ReadConsistency::Observed,
            )
            .unwrap();
        let joined = join_selected(
            local,
            &grades,
            "entity",
            "entity",
            &json!(["poor", "Eq", true]),
            true,
        )
        .unwrap();
        assert_eq!(
            joined
                .rows
                .iter()
                .map(|r| r["entity"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["a", "c"]
        );
        assert_eq!(joined.read_boundary, "independent_sources");
    }
    #[test]
    fn numeric_order_support_is_independent_of_revision_permutation() {
        let rows = [
            json!({"id":"a","entity":"one","version":9007199254740993u64}),
            json!({"id":"b","entity":"one","version":1}),
            json!({"id":"c","entity":"one","version":0.5}),
        ];
        for order in [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ] {
            let result = spec().select(
                order.into_iter().map(|i| rows[i].clone()).collect(),
                "fixture".into(),
                ReadConsistency::Observed,
            );
            assert!(
                result.is_err(),
                "mixed unsupported order domain must reject for permutation {order:?}"
            );
        }
    }
    #[test]
    fn canonical_id_uses_the_same_numeric_key_normalization_as_partitioning() {
        let rows = vec![
            json!({"id":1,"entity":1.0,"version":1}),
            json!({"id":2,"entity":1,"version":1}),
        ];
        let selected = spec()
            .select(rows, "fixture".into(), ReadConsistency::Observed)
            .unwrap();
        assert_eq!(selected.rows.len(), 1);
        assert_eq!(
            selected.rows[0]["id"], 1,
            "numeric canonical ID must beat physical-ID descending"
        );
        let rows = vec![
            json!({"id":1,"entity":"1","version":1}),
            json!({"id":2,"entity":"1","version":1}),
        ];
        let selected = spec()
            .select(rows, "fixture".into(), ReadConsistency::Observed)
            .unwrap();
        assert_eq!(
            selected.rows[0]["id"], 2,
            "numeric and string identities are distinct"
        );
    }
    #[test]
    fn normalized_numeric_keys_and_duplicate_physical_ids() {
        let rows = vec![
            json!({"id":"a","entity":1,"version":1}),
            json!({"id":"b","entity":1.0,"version":2}),
        ];
        let selected = spec()
            .select(rows, "fixture".into(), ReadConsistency::Observed)
            .unwrap();
        assert_eq!(selected.rows.len(), 1);
        assert_eq!(selected.rows[0]["id"], "b");
        assert!(spec()
            .select(
                vec![
                    json!({"id":"a","entity":"one"}),
                    json!({"id":"a","entity":"two"})
                ],
                "fixture".into(),
                ReadConsistency::Observed
            )
            .is_err());
    }
    #[test]
    fn null_placement_is_independent_of_direction_and_fractional_sum_is_compensated() {
        let mut plan = spec();
        plan.order_by.truncate(1);
        let rows = vec![
            json!({"id":"a","entity":"one","version":null}),
            json!({"id":"b","entity":"one","version":5}),
        ];
        for direction in [Direction::Asc, Direction::Desc] {
            plan.order_by[0].direction = direction;
            plan.order_by[0].nulls = Nulls::First;
            assert_eq!(
                plan.select(rows.clone(), "fixture".into(), ReadConsistency::Observed)
                    .unwrap()
                    .rows[0]["id"],
                "a"
            );
            plan.order_by[0].nulls = Nulls::Last;
            assert_eq!(
                plan.select(rows.clone(), "fixture".into(), ReadConsistency::Observed)
                    .unwrap()
                    .rows[0]["id"],
                "b"
            );
        }
        let metrics = BTreeMap::from([("n".into(), json!(["Sum", "n"]))]);
        assert_eq!(
            aggregate(
                &[json!({"n":1e16}), json!({"n":1.0}), json!({"n":-1e16})],
                &metrics
            )
            .unwrap(),
            json!({"n":1.0})
        );
    }
    #[test]
    fn over_ten_thousand_complete_latest_rows() {
        let rows=(0..10007).flat_map(|n|vec![json!({"id":format!("{n:06}-old"),"entity":n,"version":1,"active":true,"amount":100}),json!({"id":format!("{n:06}-new"),"entity":n,"version":2,"active":true,"amount":1})]).collect();
        let mut req = request();
        req.scan.max_rows = 30000;
        let result = req
            .execute(
                RelationSource {
                    rows,
                    pages: 21,
                    bytes: 2000000,
                    consistency: ReadConsistency::Observed,
                },
                "t".into(),
            )
            .unwrap();
        assert_eq!(result["aggregations"], json!({"n":10007,"sum":10007}));
    }
}

#[cfg(test)]
#[path = "latest_relation_tests.rs"]
mod source_tests;
