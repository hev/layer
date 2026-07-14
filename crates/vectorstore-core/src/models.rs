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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dist: Option<f64>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub attributes: HashMap<String, Value>,
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
