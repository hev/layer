//! Complete grouped aggregation over an upstream-selected relation.
//!
//! This executor never loads a source, selects revisions, or applies predicates.
//! Its caller supplies every selected row, after latest selection and filtering.
//! Pagination slices finished groups, never the input relation. Re-execution costs
//! O(rows + expanded instances + groups log groups); no persistent cache is used.

use std::collections::BTreeMap;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use chrono::{DateTime, Datelike, NaiveDateTime, Offset, TimeDelta, Timelike, Utc};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RelationBinding {
    /// Tenant/namespace scope AND latest/filter/join query identity, supplied by
    /// the executor, never accepted from an untrusted request as authorization.
    pub scope: String,
    /// Upstream read boundary description, not a generated-at timestamp.
    pub read_boundary: String,
    /// True only after the source scan and selection finished without truncation.
    pub complete: bool,
    pub consistency: ReadConsistency,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReadConsistency {
    /// A complete observed scan; concurrent writes may affect its pages.
    ObservedScan,
    /// Only usable if the upstream actually supplies a repeatable snapshot.
    Snapshot,
    /// The source cannot execute this relation; must fail, never return success.
    Unavailable,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Expansion {
    Timestamps {
        field: String,
        timezone: String,
    },
    /// Object mapping model names to objects of numeric per-model metrics.
    Models {
        field: String,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    deny_unknown_fields,
    from = "StrictDimension"
)]
pub enum Dimension {
    Field {
        field: String,
    },
    /// UTC calendar date of a selected-row epoch-millisecond timestamp.
    UtcDay {
        field: String,
    },
    /// Monday = 0 through Sunday = 6, over expanded timestamp instances.
    Weekday,
    Hour,
    Model,
}

// Empty struct variants make serde validate every field, including unit payloads.
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum StrictDimension {
    Field {
        field: String,
    },
    /// UTC calendar date of a selected-row epoch-millisecond timestamp.
    UtcDay {
        field: String,
    },
    /// Monday = 0 through Sunday = 6, over expanded timestamp instances.
    Weekday {},
    Hour {},
    Model {},
}

impl From<StrictDimension> for Dimension {
    fn from(value: StrictDimension) -> Self {
        match value {
            StrictDimension::Field { field } => Self::Field { field },
            StrictDimension::UtcDay { field } => Self::UtcDay { field },
            StrictDimension::Weekday {} => Self::Weekday,
            StrictDimension::Hour {} => Self::Hour,
            StrictDimension::Model {} => Self::Model,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    deny_unknown_fields,
    from = "StrictMetric"
)]
pub enum Metric {
    Count,
    Sum {
        field: String,
    },
    /// Sum from the expanded model's metric object, never the row-wide total.
    ModelSum {
        field: String,
    },
}

// Empty struct variants make serde validate every field, including unit payloads.
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum StrictMetric {
    Count {},
    Sum {
        field: String,
    },
    /// Sum from the expanded model's metric object, never the row-wide total.
    ModelSum {
        field: String,
    },
}

impl From<StrictMetric> for Metric {
    fn from(value: StrictMetric) -> Self {
        match value {
            StrictMetric::Sum { field } => Self::Sum { field },
            StrictMetric::ModelSum { field } => Self::ModelSum { field },
            StrictMetric::Count {} => Self::Count,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GroupRequest {
    pub group_by: Vec<Dimension>,
    #[serde(default)]
    pub expand: Option<Expansion>,
    pub metrics: BTreeMap<String, Metric>,
    pub page_size: usize,
    #[serde(default)]
    pub cursor: Option<String>,
}

/// Resource ceilings are executor policy, not client-adjustable truncation.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_rows: usize,
    pub max_instances: usize,
    pub max_groups: usize,
    pub max_page_size: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_rows: 1_000_000,
            max_instances: 10_000_000,
            max_groups: 100_000,
            max_page_size: 10_000,
        }
    }
}

/// Null is explicit and sorts first. Non-null scalar keys retain their JSON type;
/// numeric keys preserve exact integers (negative zero = zero).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum Key {
    Null,
    Bool(bool),
    Number(String),
    String(String),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Group {
    pub key: Vec<Key>,
    pub aggregates: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct GroupPage {
    pub groups: Vec<Group>,
    pub next_cursor: Option<String>,
    /// All groups have been delivered through this page; source completeness is
    /// separately certified in relation.complete.
    pub complete: bool,
    pub total_groups: usize,
    pub relation: RelationBinding,
    pub binding: String,
    pub selected_rows: usize,
    pub expanded_instances: usize,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum GroupError {
    #[error("unsupported grouped plan: {0}")]
    Unsupported(String),
    #[error("invalid grouped input: {0}")]
    Invalid(String),
    #[error("grouped execution budget exceeded: {0}; no partial result")]
    Budget(String),
    #[error("group continuation does not match this relation, plan, or read boundary")]
    Continuation,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u8,
    binding: String,
    after: Vec<Key>,
}

enum Instance<'a> {
    Row,
    Timestamp(NaiveDateTime),
    Model(&'a str, &'a serde_json::Map<String, Value>),
}

#[derive(Clone)]
enum Accumulator {
    Count(u64),
    Sum {
        integer: Option<i128>,
        inexact_integer_input: bool,
        sum: f64,
        correction: f64,
    },
}

impl Accumulator {
    fn new(metric: &Metric) -> Self {
        match metric {
            Metric::Count => Self::Count(0),
            _ => Self::Sum {
                integer: Some(0),
                inexact_integer_input: false,
                sum: 0.0,
                correction: 0.0,
            },
        }
    }

    fn add(&mut self, value: Option<Numeric>) -> Result<(), GroupError> {
        match self {
            Self::Count(n) => {
                *n = n
                    .checked_add(1)
                    .ok_or_else(|| GroupError::Invalid("count overflow".into()))?;
            }
            Self::Sum {
                integer,
                inexact_integer_input,
                sum,
                correction,
            } => {
                if let Some(value) = value {
                    if let Numeric::Integer(n) = value {
                        *inexact_integer_input |= exact_float(n).is_err();
                    }
                    if *inexact_integer_input
                        && (integer.is_none() || matches!(value, Numeric::Float(_)))
                    {
                        return Err(invalid(
                            "mixed integer/float sum exceeds exact integer conversion range",
                        ));
                    }
                    if let (Some(total), Numeric::Integer(value)) = (*integer, value) {
                        *integer = Some(
                            total
                                .checked_add(value)
                                .ok_or_else(|| invalid("integer sum overflow"))?,
                        );
                        return Ok(());
                    }
                    if let Some(total) = integer.take() {
                        // Preserve the integer subtotal as a compensated pair. The
                        // exact-range rule applies to inputs, not their subtotal.
                        *sum = total as f64;
                        *correction = (total - *sum as i128) as f64;
                    }
                    let value = match value {
                        Numeric::Integer(value) => exact_float(value)?,
                        Numeric::Float(value) => value,
                    };
                    // Neumaier compensation reduces order-dependent rounding.
                    let next = *sum + value;
                    *correction += if sum.abs() >= value.abs() {
                        (*sum - next) + value
                    } else {
                        (value - next) + *sum
                    };
                    *sum = next;
                    if !sum.is_finite() || !correction.is_finite() {
                        return Err(GroupError::Invalid("sum overflow".into()));
                    }
                }
            }
        }
        Ok(())
    }

    fn finish(self) -> Result<Value, GroupError> {
        match self {
            Self::Count(n) => Ok(Value::from(n)),
            Self::Sum {
                integer: Some(n), ..
            } => {
                if let Ok(n) = i64::try_from(n) {
                    Ok(Value::from(n))
                } else if let Ok(n) = u64::try_from(n) {
                    Ok(Value::from(n))
                } else {
                    Err(invalid("integer sum exceeds JSON integer range"))
                }
            }
            Self::Sum {
                integer: None,
                sum,
                correction,
                ..
            } => serde_json::Number::from_f64(sum + correction)
                .map(Value::Number)
                .ok_or_else(|| invalid("sum overflow")),
        }
    }
}

fn invalid(message: &str) -> GroupError {
    GroupError::Invalid(message.into())
}

fn timestamp(value: &Value) -> Result<DateTime<Utc>, GroupError> {
    let ms = value
        .as_i64()
        .ok_or_else(|| invalid("timestamp must be integer epoch milliseconds"))?;
    DateTime::from_timestamp_millis(ms).ok_or_else(|| invalid("timestamp out of range"))
}

fn local_timestamp(value: &Value, timezone: Tz) -> Result<NaiveDateTime, GroupError> {
    let utc = timestamp(value)?;
    let zoned = utc.with_timezone(&timezone);
    // DateTime::naive_local can panic when a valid extreme UTC date plus its
    // timezone offset leaves chrono's calendar range. Treat that as bad data.
    utc.naive_utc()
        .checked_add_signed(TimeDelta::seconds(
            zoned.offset().fix().local_minus_utc().into(),
        ))
        .ok_or_else(|| invalid("local timestamp out of range"))
}

#[derive(Clone, Copy)]
enum Numeric {
    Integer(i128),
    Float(f64),
}

fn exact_float(n: i128) -> Result<f64, GroupError> {
    if !(-(1i128 << 53)..=(1i128 << 53)).contains(&n) {
        return Err(invalid(
            "mixed integer/float sum exceeds exact integer conversion range",
        ));
    }
    Ok(n as f64)
}

fn number(value: Option<&Value>) -> Result<Option<Numeric>, GroupError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => {
            if let Some(n) = n.as_i64() {
                return Ok(Some(Numeric::Integer(n.into())));
            }
            if let Some(n) = n.as_u64() {
                return Ok(Some(Numeric::Integer(n.into())));
            }
            n.as_f64()
                .filter(|n| n.is_finite())
                .map(|n| Some(Numeric::Float(n)))
                .ok_or_else(|| invalid("non-finite numeric value"))
        }
        _ => Err(invalid("sum input must be numeric or null")),
    }
}

fn scalar(value: Option<&Value>) -> Result<Key, GroupError> {
    match value {
        None | Some(Value::Null) => Ok(Key::Null),
        Some(Value::Bool(b)) => Ok(Key::Bool(*b)),
        Some(Value::String(s)) => Ok(Key::String(s.clone())),
        Some(Value::Number(n)) => {
            // Preserve exact JSON integer keys, including values above 2^53.
            Ok(Key::Number(if let Some(i) = n.as_i64() {
                i.to_string()
            } else if let Some(u) = n.as_u64() {
                u.to_string()
            } else {
                let f = n.as_f64().ok_or_else(|| invalid("invalid number key"))?;
                if f == 0.0 {
                    "0".into()
                } else {
                    f.to_string()
                }
            }))
        }
        _ => Err(invalid("group field must be scalar or null")),
    }
}

pub(crate) fn validate(
    request: &GroupRequest,
    binding: &RelationBinding,
    limits: Limits,
) -> Result<Option<Tz>, GroupError> {
    if !binding.complete || binding.consistency == ReadConsistency::Unavailable {
        return Err(GroupError::Unsupported(
            "complete selected relation unavailable".into(),
        ));
    }
    if binding.scope.is_empty() || binding.read_boundary.is_empty() {
        return Err(invalid("scope and read boundary are required"));
    }
    if request.page_size == 0 || request.page_size > limits.max_page_size {
        return Err(invalid("page_size outside executor limits"));
    }
    if request.group_by.is_empty()
        || request.group_by.len() > 16
        || request.metrics.is_empty()
        || request.metrics.len() > 64
    {
        return Err(invalid("require 1..16 dimensions and 1..64 named metrics"));
    }
    if request.metrics.keys().any(|k| k.is_empty()) {
        return Err(invalid("empty metric name"));
    }
    for dimension in &request.group_by {
        match dimension {
            Dimension::Weekday | Dimension::Hour
                if !matches!(request.expand, Some(Expansion::Timestamps { .. })) =>
            {
                return Err(GroupError::Unsupported(
                    "weekday/hour requires timestamps expansion".into(),
                ))
            }
            Dimension::Model if !matches!(request.expand, Some(Expansion::Models { .. })) => {
                return Err(GroupError::Unsupported(
                    "model requires models expansion".into(),
                ))
            }
            _ => (),
        }
    }
    if request
        .metrics
        .values()
        .any(|m| matches!(m, Metric::ModelSum { .. }))
        && !matches!(request.expand, Some(Expansion::Models { .. }))
    {
        return Err(GroupError::Unsupported(
            "model_sum requires models expansion".into(),
        ));
    }
    // Row-wide sums under expansion multiply totals by instance count. Require
    // explicit per-model metrics; avoid plausible but incorrect spend totals.
    if matches!(request.expand, Some(Expansion::Models { .. }))
        && request
            .metrics
            .values()
            .any(|m| matches!(m, Metric::Sum { .. }))
    {
        return Err(GroupError::Unsupported(
            "models expansion requires model_sum rather than row sum".into(),
        ));
    }
    match &request.expand {
        Some(Expansion::Timestamps { timezone, .. }) => timezone
            .parse::<Tz>()
            .map(Some)
            .map_err(|_| invalid("unknown IANA timezone")),
        _ => Ok(None),
    }
}

/// Execute on complete latest-selected, joined and filtered rows. A cursor is a
/// binding check, not authorization and not a backend snapshot. Callers must
/// derive scope/read boundary from the source execution. The same exact input
/// relation and plan can resume without storing a server-side result cache.
pub fn execute(
    rows: &[Value],
    request: &GroupRequest,
    relation: &RelationBinding,
    limits: Limits,
) -> Result<GroupPage, GroupError> {
    let timezone = validate(request, relation, limits)?;
    if rows.len() > limits.max_rows {
        return Err(GroupError::Budget("selected rows".into()));
    }
    let mut plan = request.clone();
    plan.cursor = None;
    let mut hash = Sha256::new();
    hash.update(b"layer-grouped-v1\0");
    hash.update(chrono_tz::IANA_TZDB_VERSION.as_bytes());
    // Hash each row separately instead of allocating a second full relation.
    for bytes in [serde_json::to_vec(relation), serde_json::to_vec(&plan)] {
        let bytes = bytes.map_err(|_| invalid("relation serialization failed"))?;
        hash.update((bytes.len() as u64).to_be_bytes());
        hash.update(bytes);
    }
    hash.update((rows.len() as u64).to_be_bytes());
    for row in rows {
        let bytes = serde_json::to_vec(row).map_err(|_| invalid("row serialization failed"))?;
        hash.update((bytes.len() as u64).to_be_bytes());
        hash.update(bytes);
    }
    let binding = format!("{:x}", hash.finalize());
    let after = if let Some(encoded) = &request.cursor {
        if encoded.len() > 32_768 {
            return Err(GroupError::Continuation);
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| GroupError::Continuation)?;
        let cursor: Cursor =
            serde_json::from_slice(&bytes).map_err(|_| GroupError::Continuation)?;
        if cursor.version != 1 || cursor.binding != binding {
            return Err(GroupError::Continuation);
        }
        Some(cursor.after)
    } else {
        None
    };
    let mut groups: BTreeMap<Vec<Key>, Vec<Accumulator>> = BTreeMap::new();
    let mut expanded_instances = 0usize;
    for row in rows {
        let row = row
            .as_object()
            .ok_or_else(|| invalid("selected row must be an object"))?;
        let mut add = |instance: Instance<'_>| -> Result<(), GroupError> {
            expanded_instances = expanded_instances
                .checked_add(1)
                .ok_or_else(|| GroupError::Budget("instances".into()))?;
            if expanded_instances > limits.max_instances {
                return Err(GroupError::Budget("instances".into()));
            }
            let key = request
                .group_by
                .iter()
                .map(|dimension| match dimension {
                    Dimension::Field { field } => scalar(row.get(field)),
                    Dimension::UtcDay { field } => match row.get(field) {
                        None | Some(Value::Null) => Ok(Key::Null),
                        Some(value) => Ok(Key::String(
                            timestamp(value)?.format("%Y-%m-%d").to_string(),
                        )),
                    },
                    Dimension::Weekday => match &instance {
                        Instance::Timestamp(time) => Ok(Key::Number(
                            time.weekday().num_days_from_monday().to_string(),
                        )),
                        _ => Err(invalid("weekday instance unavailable")),
                    },
                    Dimension::Hour => match &instance {
                        Instance::Timestamp(time) => Ok(Key::Number(time.hour().to_string())),
                        _ => Err(invalid("hour instance unavailable")),
                    },
                    Dimension::Model => match &instance {
                        Instance::Model(name, _) => Ok(Key::String((*name).into())),
                        _ => Err(invalid("model instance unavailable")),
                    },
                })
                .collect::<Result<Vec<_>, _>>()?;
            // Every emitted key must fit the continuation representation. A long
            // final key may become a cursor if more groups arrive on a rerun.
            if serde_json::to_vec(&key)
                .map_err(|_| invalid("key serialization failed"))?
                .len()
                > 20_000
            {
                return Err(GroupError::Budget("group key bytes".into()));
            }
            if !groups.contains_key(&key) && groups.len() >= limits.max_groups {
                return Err(GroupError::Budget("groups".into()));
            }
            let accumulators = groups
                .entry(key)
                .or_insert_with(|| request.metrics.values().map(Accumulator::new).collect());
            for (accumulator, metric) in accumulators.iter_mut().zip(request.metrics.values()) {
                let value = match metric {
                    Metric::Count => None,
                    Metric::Sum { field } => number(row.get(field))?,
                    Metric::ModelSum { field } => match &instance {
                        Instance::Model(_, values) => number(values.get(field))?,
                        _ => return Err(invalid("model metric instance unavailable")),
                    },
                };
                accumulator.add(value)?;
            }
            Ok(())
        };
        match &request.expand {
            None => add(Instance::Row)?,
            Some(Expansion::Timestamps { field, .. }) => match row.get(field) {
                None | Some(Value::Null) => (),
                Some(Value::Array(values)) => {
                    for value in values {
                        // Null array elements carry no timestamp; duplicates DO count.
                        if !value.is_null() {
                            add(Instance::Timestamp(local_timestamp(
                                value,
                                timezone.expect("validated timezone"),
                            )?))?;
                        }
                    }
                }
                _ => return Err(invalid("timestamps expansion requires array or null")),
            },
            Some(Expansion::Models { field }) => match row.get(field) {
                None | Some(Value::Null) => (),
                Some(Value::Object(values)) => {
                    for (name, value) in values {
                        let values = value
                            .as_object()
                            .ok_or_else(|| invalid("each model value must be an object"))?;
                        add(Instance::Model(name, values))?;
                    }
                }
                _ => return Err(invalid("models expansion requires object or null")),
            },
        }
    }
    if after.as_ref().is_some_and(|key| !groups.contains_key(key)) {
        return Err(GroupError::Continuation);
    }
    let total_groups = groups.len();
    // Finalize EVERY group before returning any page. Invalid/overflowed sums
    // beyond the first output boundary must not yield partial successful pages.
    let finalized = groups
        .into_iter()
        .map(|(key, values)| {
            let aggregates = request
                .metrics
                .keys()
                .cloned()
                .zip(values)
                .map(|(name, value)| Ok((name, value.finish()?)))
                .collect::<Result<_, GroupError>>()?;
            Ok(Group { key, aggregates })
        })
        .collect::<Result<Vec<_>, GroupError>>()?;
    let remaining = finalized
        .into_iter()
        .filter(|group| after.as_ref().is_none_or(|after| &group.key > after));
    let take = request
        .page_size
        .checked_add(1)
        .ok_or_else(|| invalid("page_size overflow"))?;
    let mut groups = remaining.take(take).collect::<Vec<_>>();
    let more = groups.len() > request.page_size;
    if more {
        groups.pop();
    }
    let next_cursor = if more {
        Some(
            URL_SAFE_NO_PAD.encode(
                serde_json::to_vec(&Cursor {
                    version: 1,
                    binding: binding.clone(),
                    after: groups.last().expect("nonempty page").key.clone(),
                })
                .map_err(|_| invalid("cursor serialization failed"))?,
            ),
        )
    } else {
        None
    };
    Ok(GroupPage {
        groups,
        next_cursor,
        complete: !more,
        total_groups,
        relation: relation.clone(),
        binding,
        selected_rows: rows.len(),
        expanded_instances,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn relation() -> RelationBinding {
        RelationBinding {
            scope: "tenant/store/namespace/query-v1".into(),
            read_boundary: "observed_pages".into(),
            complete: true,
            consistency: ReadConsistency::ObservedScan,
        }
    }
    fn request() -> GroupRequest {
        GroupRequest {
            group_by: vec![Dimension::Field {
                field: "project".into(),
            }],
            expand: None,
            metrics: BTreeMap::from([
                ("count".into(), Metric::Count),
                (
                    "spend".into(),
                    Metric::Sum {
                        field: "spend".into(),
                    },
                ),
            ]),
            page_size: 2,
            cursor: None,
        }
    }
    fn run(rows: &[Value], request: &GroupRequest) -> GroupPage {
        execute(rows, request, &relation(), Limits::default()).unwrap()
    }
    fn ms(s: &str) -> i64 {
        DateTime::parse_from_rfc3339(s).unwrap().timestamp_millis()
    }

    #[test]
    fn complete_groups_accumulate_across_input_and_output_boundaries() {
        let rows = (0..10_013)
            .map(|i| json!({"project": format!("p{:03}", i % 103), "spend": 1}))
            .collect::<Vec<_>>();
        let mut request = request();
        request.page_size = 7;
        let mut groups = Vec::new();
        let first = run(&rows, &request);
        assert_eq!(first.total_groups, 103);
        assert!(!first.complete);
        assert_eq!(first.groups[0].aggregates["count"], 98);
        assert_eq!(first.groups[0].aggregates["spend"], 98);
        groups.extend(first.groups);
        request.cursor = first.next_cursor;
        loop {
            let page = run(&rows, &request);
            groups.extend(page.groups);
            if page.complete {
                assert!(page.next_cursor.is_none());
                break;
            }
            request.cursor = page.next_cursor;
        }
        assert_eq!(groups.len(), 103);
        assert!(groups.windows(2).all(|w| w[0].key < w[1].key));
        assert_eq!(
            groups
                .iter()
                .map(|g| g.aggregates["count"].as_u64().unwrap())
                .sum::<u64>(),
            10_013
        );
        assert_eq!(
            groups
                .iter()
                .map(|g| g.aggregates["spend"].as_u64().unwrap())
                .sum::<u64>(),
            10_013
        );
    }

    #[test]
    fn missing_null_zero_empty_and_typed_group_keys_remain_distinct() {
        let rows = vec![
            json!({}),
            json!({"project": null}),
            json!({"project": "", "spend": 0}),
            json!({"project": 0, "spend": null}),
            json!({"project": false, "spend": -2}),
        ];
        let mut request = request();
        request.page_size = 20;
        let page = run(&rows, &request);
        assert_eq!(page.groups.len(), 4);
        assert_eq!(page.groups[0].key, vec![Key::Null]);
        assert_eq!(page.groups[0].aggregates["count"], 2);
        assert_eq!(page.groups[0].aggregates["spend"], 0);
        assert_eq!(page.groups[1].key, vec![Key::Bool(false)]);
        assert_eq!(page.groups[2].key, vec![Key::Number("0".into())]);
        assert_eq!(page.groups[3].key, vec![Key::String("".into())]);
        let empty = run(&[], &request);
        assert!(empty.complete && empty.groups.is_empty() && empty.next_cursor.is_none());
        assert_eq!(empty.total_groups, 0);
    }

    #[test]
    fn utc_days_use_utc_and_keep_missing_day_explicit() {
        let rows = vec![
            json!({"start": ms("2026-03-08T23:59:59Z"), "spend": 2}),
            json!({"start": ms("2026-03-09T00:00:00Z"), "spend": 3}),
            json!({"spend": 4}),
            json!({"start": -1, "spend": 1}),
        ];
        let mut request = request();
        request.page_size = 20;
        request.group_by = vec![Dimension::UtcDay {
            field: "start".into(),
        }];
        let page = run(&rows, &request);
        assert_eq!(
            page.groups
                .iter()
                .map(|g| g.key.clone())
                .collect::<Vec<_>>(),
            vec![
                vec![Key::Null],
                vec![Key::String("1969-12-31".into())],
                vec![Key::String("2026-03-08".into())],
                vec![Key::String("2026-03-09".into())]
            ]
        );
        assert_eq!(page.groups[3].aggregates["spend"], 3);
    }

    fn heatmap() -> GroupRequest {
        GroupRequest {
            group_by: vec![Dimension::Weekday, Dimension::Hour],
            expand: Some(Expansion::Timestamps {
                field: "times".into(),
                timezone: "America/Denver".into(),
            }),
            metrics: BTreeMap::from([("prompts".into(), Metric::Count)]),
            page_size: 168,
            cursor: None,
        }
    }

    #[test]
    fn timestamp_instances_honor_fall_back_duplicates_and_missing_arrays() {
        let rows = vec![
            json!({"times": [ms("2026-11-01T07:30:00Z"), ms("2026-11-01T08:30:00Z"), ms("2026-11-01T08:30:00Z"), null]}),
            json!({}),
            json!({"times": null}),
            json!({"times": []}),
        ];
        let page = run(&rows, &heatmap());
        assert_eq!(page.expanded_instances, 3);
        assert_eq!(page.groups.len(), 1);
        assert_eq!(
            page.groups[0].key,
            vec![Key::Number("6".into()), Key::Number("1".into())]
        );
        assert_eq!(page.groups[0].aggregates["prompts"], 3);
        let empty = run(&[json!({"times": []})], &heatmap());
        assert!(empty.groups.is_empty());
    }

    #[test]
    fn timestamp_instances_skip_spring_gap_and_convert_across_day_boundary() {
        let rows = vec![
            json!({"times": [ms("2026-03-08T08:59:59Z"), ms("2026-03-08T09:00:00Z"), ms("2026-03-09T00:30:00Z") ]}),
        ];
        let page = run(&rows, &heatmap());
        assert_eq!(page.groups.len(), 3);
        assert!(!page
            .groups
            .iter()
            .any(|g| g.key[1] == Key::Number("2".into())));
        assert!(page
            .groups
            .iter()
            .all(|g| g.key[0] == Key::Number("6".into())));
        assert!(page
            .groups
            .iter()
            .any(|g| g.key[1] == Key::Number("18".into())));
        assert_eq!(
            page.groups
                .iter()
                .map(|g| g.aggregates["prompts"].as_u64().unwrap())
                .sum::<u64>(),
            3
        );
    }

    #[test]
    fn multiple_model_values_use_each_models_metrics_not_session_totals() {
        let rows = vec![
            json!({"project":"p", "start": 0, "tokens":999, "spend":999, "usage": {"a":{"tokens":10,"spend":0.5},"b":{"tokens":20,"spend":0.25}}}),
            json!({"project":"p", "start": 0, "usage":{"a":{"tokens":0,"spend":null},"c":{}}}),
            json!({"project":"p", "usage": {}}),
            json!({"project":"p"}),
        ];
        let mut request = request();
        request.expand = Some(Expansion::Models {
            field: "usage".into(),
        });
        request.group_by = vec![
            Dimension::Field {
                field: "project".into(),
            },
            Dimension::UtcDay {
                field: "start".into(),
            },
            Dimension::Model,
        ];
        request.metrics = BTreeMap::from([
            ("instances".into(), Metric::Count),
            (
                "tokens".into(),
                Metric::ModelSum {
                    field: "tokens".into(),
                },
            ),
            (
                "spend".into(),
                Metric::ModelSum {
                    field: "spend".into(),
                },
            ),
        ]);
        request.page_size = 10;
        let page = run(&rows, &request);
        assert_eq!(page.groups.len(), 3);
        assert_eq!(page.expanded_instances, 4);
        assert_eq!(page.groups[0].aggregates["instances"], 2);
        assert_eq!(page.groups[0].aggregates["tokens"], 10);
        assert_eq!(page.groups[0].aggregates["spend"], 0.5);
        assert_eq!(page.groups[1].aggregates["tokens"], 20);
        assert_eq!(page.groups[2].aggregates["tokens"], 0);
        assert_eq!(page.groups[2].aggregates["spend"], 0);
        assert_eq!(page.groups[0].key[2], Key::String("a".into()));
    }

    #[test]
    fn continuation_is_deterministic_and_binds_content_plan_boundary_and_scope() {
        let rows = vec![
            json!({"project":"a","spend":1}),
            json!({"project":"b","spend":2}),
            json!({"project":"c","spend":3}),
        ];
        let first = run(&rows, &request());
        assert_eq!(first, run(&rows, &request()));
        let mut request = request();
        request.cursor = first.next_cursor;
        assert_eq!(run(&rows, &request).groups.len(), 1);
        assert_eq!(run(&rows, &request), run(&rows, &request));
        let mut changed = rows.clone();
        changed[0]["spend"] = json!(4);
        assert_eq!(
            execute(&changed, &request, &relation(), Limits::default()),
            Err(GroupError::Continuation)
        );
        changed = rows.clone();
        changed.reverse();
        assert_eq!(
            execute(&changed, &request, &relation(), Limits::default()),
            Err(GroupError::Continuation)
        );
        for binding in [
            RelationBinding {
                scope: "another tenant".into(),
                ..relation()
            },
            RelationBinding {
                read_boundary: "another snapshot".into(),
                ..relation()
            },
            RelationBinding {
                consistency: ReadConsistency::Snapshot,
                ..relation()
            },
        ] {
            assert_eq!(
                execute(&rows, &request, &binding, Limits::default()),
                Err(GroupError::Continuation)
            );
        }
        let mut changed = request.clone();
        changed.page_size = 1;
        assert_eq!(
            execute(&rows, &changed, &relation(), Limits::default()),
            Err(GroupError::Continuation)
        );
        let mut changed = request.clone();
        changed.metrics.insert("other".into(), Metric::Count);
        assert_eq!(
            execute(&rows, &changed, &relation(), Limits::default()),
            Err(GroupError::Continuation)
        );
        let mut changed = request.clone();
        changed.cursor = Some("garbage".into());
        assert_eq!(
            execute(&rows, &changed, &relation(), Limits::default()),
            Err(GroupError::Continuation)
        );
    }

    #[test]
    fn unavailable_incomplete_and_unsupported_plans_fail_closed() {
        for binding in [
            RelationBinding {
                complete: false,
                ..relation()
            },
            RelationBinding {
                consistency: ReadConsistency::Unavailable,
                ..relation()
            },
        ] {
            assert!(matches!(
                execute(&[], &request(), &binding, Limits::default()),
                Err(GroupError::Unsupported(_))
            ));
        }
        let mut request = request();
        request.group_by = vec![Dimension::Hour];
        assert!(matches!(
            execute(&[], &request, &relation(), Limits::default()),
            Err(GroupError::Unsupported(_))
        ));
        request = heatmap();
        request.expand = Some(Expansion::Timestamps {
            field: "times".into(),
            timezone: "Mars/Olympus".into(),
        });
        assert!(matches!(
            execute(&[], &request, &relation(), Limits::default()),
            Err(GroupError::Invalid(_))
        ));
        request = super::tests::request();
        request.expand = Some(Expansion::Models {
            field: "usage".into(),
        });
        assert!(matches!(
            execute(&[], &request, &relation(), Limits::default()),
            Err(GroupError::Unsupported(_))
        ));
    }

    #[test]
    fn malformed_data_and_all_execution_budgets_error_without_partial_results() {
        for row in [
            json!({"project":{}}),
            json!({"project":[]}),
            json!({"spend":"1"}),
            json!([]),
        ] {
            assert!(matches!(
                execute(&[row], &request(), &relation(), Limits::default()),
                Err(GroupError::Invalid(_))
            ));
        }
        for row in [
            json!({"times":"[]"}),
            json!({"times":["yesterday"]}),
            json!({"times":[0.5]}),
            json!({"times":[i64::MAX]}),
        ] {
            assert!(matches!(
                execute(&[row], &heatmap(), &relation(), Limits::default()),
                Err(GroupError::Invalid(_))
            ));
        }
        let rows = vec![json!({"project":"a"}), json!({"project":"b"})];
        for limits in [
            Limits {
                max_rows: 1,
                ..Limits::default()
            },
            Limits {
                max_groups: 1,
                ..Limits::default()
            },
            Limits {
                max_instances: 1,
                ..Limits::default()
            },
        ] {
            assert!(matches!(
                execute(&rows, &request(), &relation(), limits),
                Err(GroupError::Budget(_))
            ));
        }
    }

    #[test]
    fn unit_variants_reject_extra_fields_and_preserve_wire_shape() {
        for (kind, expected) in [
            ("hour", Dimension::Hour),
            ("weekday", Dimension::Weekday),
            ("model", Dimension::Model),
        ] {
            let valid = json!({"kind": kind});
            assert_eq!(
                serde_json::from_value::<Dimension>(valid.clone()).unwrap(),
                expected
            );
            assert_eq!(serde_json::to_value(&expected).unwrap(), valid);
            for extra in [json!(1), Value::Null, json!({})] {
                let invalid = json!({"kind": kind, "unknown": extra});
                assert!(serde_json::from_value::<Dimension>(invalid.clone()).is_err());
                let mut request = serde_json::to_value(request()).unwrap();
                request["group_by"] = json!([invalid]);
                assert!(serde_json::from_value::<GroupRequest>(request).is_err());
            }
        }
        assert_eq!(
            serde_json::from_value::<Metric>(json!({"kind":"count"})).unwrap(),
            Metric::Count
        );
        assert_eq!(
            serde_json::to_value(Metric::Count).unwrap(),
            json!({"kind":"count"})
        );
        for extra in [json!(1), Value::Null, json!({})] {
            let invalid = json!({"kind":"count", "unknown":extra});
            assert!(serde_json::from_value::<Metric>(invalid.clone()).is_err());
            let mut request = serde_json::to_value(request()).unwrap();
            request["metrics"]["count"] = invalid;
            assert!(serde_json::from_value::<GroupRequest>(request).is_err());
        }
    }

    #[test]
    fn mixed_sum_exact_range_is_per_input_in_every_permutation() {
        // Test both row and model metrics through the public executor.
        for (values, expected) in [
            (
                [
                    json!(9007199254740993u64),
                    json!(-9007199254740993i64),
                    json!(0.5),
                ],
                None,
            ),
            (
                [
                    json!(9007199254740992u64),
                    json!(-9007199254740992i64),
                    json!(0.5),
                ],
                Some(json!(0.5)),
            ),
            (
                [json!(9007199254740992u64), json!(1), json!(0.5)],
                Some(json!(9007199254740994.0)),
            ),
            (
                [json!(-9007199254740992i64), json!(-1), json!(-0.5)],
                Some(json!(-9007199254740994.0)),
            ),
        ] {
            for order in [
                [0, 1, 2],
                [0, 2, 1],
                [1, 0, 2],
                [1, 2, 0],
                [2, 0, 1],
                [2, 1, 0],
            ] {
                for models in [false, true] {
                    let mut request = request();
                    if models {
                        request.expand = Some(Expansion::Models {
                            field: "models".into(),
                        });
                        request.metrics.insert(
                            "spend".into(),
                            Metric::ModelSum {
                                field: "spend".into(),
                            },
                        );
                    }
                    let rows = order
                        .iter()
                        .map(|&i| {
                            if models {
                                json!({"models":{"m":{"spend":values[i]}}})
                            } else {
                                json!({"spend":values[i]})
                            }
                        })
                        .collect::<Vec<_>>();
                    let result = execute(&rows, &request, &relation(), Limits::default());
                    if let Some(expected) = &expected {
                        assert_eq!(
                            &result.unwrap().groups[0].aggregates["spend"],
                            expected,
                            "{order:?}"
                        );
                    } else {
                        assert_eq!(
                            result.unwrap_err(),
                            invalid(
                                "mixed integer/float sum exceeds exact integer conversion range"
                            ),
                            "{order:?}"
                        );
                    }
                }
            }
        }
        // The same large inputs remain legal when no float occurs.
        let rows = vec![
            json!({"spend":9007199254740993u64}),
            json!({"spend":-9007199254740993i64}),
        ];
        assert_eq!(run(&rows, &request()).groups[0].aggregates["spend"], 0);
    }

    #[test]
    fn exact_integer_sums_and_compensated_float_sums() {
        let rows = vec![json!({"spend":9007199254740993u64}), json!({"spend":2})];
        assert_eq!(
            run(&rows, &request()).groups[0].aggregates["spend"],
            9007199254740995u64
        );
        let rows = vec![
            json!({"spend":1e16}),
            json!({"spend":1.0}),
            json!({"spend":-1e16}),
        ];
        assert_eq!(run(&rows, &request()).groups[0].aggregates["spend"], 1.0);
        let rows = vec![json!({"spend":u64::MAX}), json!({"spend":1})];
        assert!(matches!(
            execute(&rows, &request(), &relation(), Limits::default()),
            Err(GroupError::Invalid(_))
        ));
        let rows = vec![json!({"spend":9007199254740993u64}), json!({"spend":0.5})];
        assert!(matches!(
            execute(&rows, &request(), &relation(), Limits::default()),
            Err(GroupError::Invalid(_))
        ));
    }

    #[test]
    fn more_than_ten_thousand_groups_resume_without_top_k_loss() {
        let rows = (0..10_007)
            .map(|i| json!({"project":format!("p{i:05}"),"spend":1}))
            .collect::<Vec<_>>();
        let mut request = request();
        request.page_size = 10_000;
        let first = run(&rows, &request);
        assert_eq!(first.groups.len(), 10_000);
        assert_eq!(first.total_groups, 10_007);
        assert!(!first.complete);
        request.cursor = first.next_cursor;
        let last = run(&rows, &request);
        assert_eq!(last.groups.len(), 7);
        assert!(last.complete && last.next_cursor.is_none());
        assert_eq!(last.groups[6].key, vec![Key::String("p10006".into())]);
    }

    #[test]
    fn non_hour_iana_offset_and_extreme_local_date_do_not_panic() {
        let mut request = heatmap();
        request.expand = Some(Expansion::Timestamps {
            field: "times".into(),
            timezone: "Asia/Kathmandu".into(),
        });
        let page = run(&[json!({"times":[ms("2026-03-08T23:55:00Z")]})], &request);
        assert_eq!(
            page.groups[0].key,
            vec![Key::Number("0".into()), Key::Number("5".into())]
        );
        let max_ms = DateTime::<Utc>::MAX_UTC.timestamp_millis();
        assert!(matches!(
            execute(
                &[json!({"times":[max_ms]})],
                &request,
                &relation(),
                Limits::default()
            ),
            Err(GroupError::Invalid(_))
        ));
    }

    #[test]
    fn numeric_keys_preserve_large_integer_distinctions_and_normalize_numeric_zero() {
        let rows = vec![
            json!({"project":9007199254740992u64}),
            json!({"project":9007199254740993u64}),
            json!({"project":0}),
            json!({"project":-0.0}),
            json!({"project":1}),
            json!({"project":1.0}),
        ];
        let mut request = request();
        request.page_size = 10;
        let page = run(&rows, &request);
        assert_eq!(page.groups.len(), 4);
        assert_eq!(page.groups[0].aggregates["count"], 2);
        assert_eq!(page.groups[1].aggregates["count"], 2);
        assert_ne!(page.groups[2].key, page.groups[3].key);
    }

    #[test]
    fn errors_in_later_groups_fail_before_first_page_and_long_keys_never_strand_cursors() {
        let rows = vec![
            json!({"project":"a","spend":1}),
            json!({"project":"b","spend":1}),
            json!({"project":"z","spend":u64::MAX}),
            json!({"project":"z","spend":1}),
        ];
        assert!(matches!(
            execute(&rows, &request(), &relation(), Limits::default()),
            Err(GroupError::Invalid(_))
        ));
        let rows = vec![json!({"project":"x".repeat(25000)})];
        assert!(matches!(
            execute(&rows, &request(), &relation(), Limits::default()),
            Err(GroupError::Budget(_))
        ));
    }

    #[test]
    fn invalid_model_values_and_scalar_time_inputs_never_silently_drop() {
        let mut model_request = request();
        model_request.expand = Some(Expansion::Models {
            field: "usage".into(),
        });
        model_request.group_by = vec![Dimension::Model];
        model_request.metrics = BTreeMap::from([(
            "tokens".into(),
            Metric::ModelSum {
                field: "tokens".into(),
            },
        )]);
        for row in [
            json!({"usage":"{}"}),
            json!({"usage":[]}),
            json!({"usage":{"a":null}}),
            json!({"usage":{"a":{"tokens":"3"}}}),
        ] {
            assert!(matches!(
                execute(&[row], &model_request, &relation(), Limits::default()),
                Err(GroupError::Invalid(_))
            ));
        }
        let mut day_request = request();
        day_request.group_by = vec![Dimension::UtcDay {
            field: "start".into(),
        }];
        for row in [
            json!({"start":"2026-01-01"}),
            json!({"start":0.5}),
            json!({"start":i64::MAX}),
        ] {
            assert!(matches!(
                execute(&[row], &day_request, &relation(), Limits::default()),
                Err(GroupError::Invalid(_))
            ));
        }
    }

    #[test]
    fn model_and_timestamp_expansion_preserve_filtered_input_only() {
        // The latest relation already chose and filtered rows. This executor
        // cannot resurrect a revision or add a missing model/timestamp row.
        let selected = vec![
            json!({"project":"current","times":[0,0],"usage":{"a":{"tokens":2},"b":{"tokens":3}}}),
        ];
        let page = run(&selected, &heatmap());
        assert_eq!(page.selected_rows, 1);
        assert_eq!(page.expanded_instances, 2);
        assert_eq!(page.groups[0].aggregates["prompts"], 2);
        let mut model_request = request();
        model_request.expand = Some(Expansion::Models {
            field: "usage".into(),
        });
        model_request.metrics = BTreeMap::from([(
            "tokens".into(),
            Metric::ModelSum {
                field: "tokens".into(),
            },
        )]);
        let page = run(&selected, &model_request);
        assert_eq!(page.groups[0].key, vec![Key::String("current".into())]);
        assert_eq!(page.groups[0].aggregates["tokens"], 5);
    }

    #[test]
    fn measure_synthetic_executor_cost_and_output_without_backend_claim() {
        let rows = (0..1200).map(|i| json!({"project":format!("p{}",i%12),"start":(i%30)*86400000i64,"spend":0.125,"times":[0,3600000,7200000],"usage":{"a":{"tokens":10,"spend":0.1},"b":{"tokens":5,"spend":0.025}}})).collect::<Vec<_>>();
        let mut model_request = request();
        model_request.expand = Some(Expansion::Models {
            field: "usage".into(),
        });
        model_request.group_by = vec![
            Dimension::Field {
                field: "project".into(),
            },
            Dimension::UtcDay {
                field: "start".into(),
            },
            Dimension::Model,
        ];
        model_request.metrics = BTreeMap::from([
            (
                "tokens".into(),
                Metric::ModelSum {
                    field: "tokens".into(),
                },
            ),
            (
                "spend".into(),
                Metric::ModelSum {
                    field: "spend".into(),
                },
            ),
            ("instances".into(), Metric::Count),
        ]);
        model_request.page_size = 1000;
        let input_bytes = serde_json::to_vec(&rows).unwrap().len();
        for (name, request) in [
            ("project_day_model", model_request),
            ("prompt_heatmap", heatmap()),
        ] {
            let start = std::time::Instant::now();
            let page = run(&rows, &request);
            let elapsed = start.elapsed();
            let output_bytes = serde_json::to_vec(&page).unwrap().len();
            println!("synthetic_executor name={name} selected_rows={} expanded_instances={} input_json_bytes={input_bytes} groups={} output_json_bytes={output_bytes} elapsed_us={} backend_queries=0 source_scan_bytes=not_measured",rows.len(),page.expanded_instances,page.total_groups,elapsed.as_micros());
            assert!(page.complete);
            assert!(output_bytes < input_bytes);
        }
    }

    #[test]
    fn wire_schema_roundtrip_is_strict_and_output_is_compact() {
        let request = heatmap();
        assert_eq!(
            serde_json::from_value::<GroupRequest>(serde_json::to_value(&request).unwrap())
                .unwrap(),
            request
        );
        assert!(serde_json::from_value::<GroupRequest>(json!({"group_by":[{"kind":"hour"}],"expand":{"kind":"timestamps","field":"times","timezone":"UTC","cross_product":true},"metrics":{"n":{"kind":"count"}},"page_size":2})).is_err());
        let rows = (0..1000)
            .map(|_| json!({"times":[0],"secret_text":"not in output"}))
            .collect::<Vec<_>>();
        let output = serde_json::to_string(&run(&rows, &request)).unwrap();
        assert!(!output.contains("secret_text") && !output.contains("not in output"));
        assert!(output.len() < 1000);
    }
}
