use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum IncludeAttributes {
    All(bool),
    Fields(Vec<String>),
}

impl IncludeAttributes {
    pub fn to_turbopuffer_value(&self) -> Value {
        match self {
            Self::All(value) => Value::Bool(*value),
            Self::Fields(fields) => serde_json::to_value(fields).unwrap_or(Value::Bool(false)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentResponse {
    pub id: String,
    #[serde(default)]
    pub attributes: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub id: String,
    /// True when the store returned `id` as an unsigned integer. The gateway
    /// keys rows by string internally and writes the integer back out, so a
    /// Turbopuffer client sees the id type it wrote.
    #[serde(skip)]
    pub numeric_id: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dist: Option<f64>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub attributes: HashMap<String, Value>,
}

impl QueryResult {
    /// The id in its wire form: a JSON number for integer ids, else a string.
    pub fn wire_id(&self) -> Value {
        if self.numeric_id {
            if let Ok(id) = self.id.parse::<u64>() {
                return Value::from(id);
            }
        }
        Value::String(self.id.clone())
    }
}

/// Reads a document id off a store row. Turbopuffer ids are unsigned
/// integers, UUIDs or strings; integers come back as JSON numbers. Returns
/// the id as a string and whether it was an integer on the wire.
pub fn id_from_wire(value: &Value) -> Option<(String, bool)> {
    match value {
        Value::String(id) => Some((id.clone(), false)),
        Value::Number(id) => id.as_u64().map(|id| (id.to_string(), true)),
        _ => None,
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct FieldValueResult {
    pub value: String,
    pub doc_count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DocumentPage {
    pub documents: Vec<DocumentResponse>,
    pub next_cursor: Option<String>,
}
