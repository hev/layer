#[cfg(feature = "pro")]
pub use layer_agentic::*;

#[cfg(not(feature = "pro"))]
mod open {
    use std::collections::HashMap;
    use std::sync::Arc;

    use async_trait::async_trait;
    use dashmap::DashMap;
    use serde::{Deserialize, Serialize};
    use serde_json::Value;

    #[derive(Debug, thiserror::Error)]
    pub enum AgenticError {
        #[error("Validation error: {0}")]
        Validation(String),
        #[error("Upstream error: {0}")]
        Upstream(String),
        #[error("Service unavailable: {0}")]
        ServiceUnavailable(String),
    }

    #[derive(Clone, Debug, Default, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct AgentSpec {
        #[serde(default)]
        pub index_schemas: Vec<AgentIndexSchema>,
    }

    #[derive(Clone, Debug, Default, Deserialize, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct AgentIndexSchema {
        pub namespace: String,
        #[serde(default)]
        pub attributes: Vec<Value>,
    }

    #[derive(Clone, Debug, Default)]
    pub struct AgentUsage {
        pub prompt_tokens: u64,
        pub completion_tokens: u64,
        pub total_tokens: u64,
    }

    #[derive(Clone, Debug, Default)]
    pub struct PlannedQuery {
        pub query: String,
        pub namespaces: Vec<String>,
        pub rank_by: Option<String>,
        pub filters: Option<Value>,
        pub include_attributes: Option<Value>,
    }

    #[derive(Clone, Debug, Default)]
    pub struct AgentPlan {
        pub queries: Vec<PlannedQuery>,
        pub trace: Option<String>,
        pub usage: Option<AgentUsage>,
    }

    #[derive(Clone, Debug, Default)]
    pub struct ScoredCandidate {
        pub key: String,
        pub relevance_score: f64,
    }

    #[derive(Clone, Debug, Default)]
    pub struct AgentScores {
        pub scores: Vec<ScoredCandidate>,
        pub usage: Option<AgentUsage>,
    }

    #[async_trait]
    pub trait AgentInferenceProvider: Send + Sync {
        async fn plan(
            &self,
            _spec: &AgentSpec,
            _query: &str,
            _query_vector_available: bool,
        ) -> Result<AgentPlan, AgenticError> {
            Err(AgenticError::Validation(
                "agentic search is disabled".to_string(),
            ))
        }

        async fn score(
            &self,
            _spec: &AgentSpec,
            _query: &str,
            _rows: &[Value],
        ) -> Result<AgentScores, AgenticError> {
            Err(AgenticError::Validation(
                "agentic search is disabled".to_string(),
            ))
        }
    }

    pub struct DisabledAgentProvider;

    #[async_trait]
    impl AgentInferenceProvider for DisabledAgentProvider {}

    pub type AgentRegistry = DashMap<String, AgentSpec>;

    pub fn registry_from_json(raw: Option<&str>) -> Result<Arc<AgentRegistry>, AgenticError> {
        let registry = Arc::new(DashMap::new());
        if let Some(raw) = raw {
            let specs: HashMap<String, AgentSpec> = serde_json::from_str(raw)
                .map_err(|error| AgenticError::Validation(error.to_string()))?;
            for (name, spec) in specs {
                registry.insert(name, spec);
            }
        }
        Ok(registry)
    }
}

#[cfg(not(feature = "pro"))]
pub use open::*;
