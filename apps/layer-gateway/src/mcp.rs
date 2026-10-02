//! Seeded, stateless, read-only MCP data servers. No independent namespace ACL.
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use axum::extract::{Extension, OriginalUri, Path, Query, Request, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::Json;
use rmcp::model::*;
use rmcp::service::RequestContext;
use rmcp::transport::streamable_http_server::{
    session::never::NeverSessionManager, StreamableHttpServerConfig, StreamableHttpService,
};
use rmcp::{ErrorData, RoleServer, ServerHandler};
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::auth::{authorize_namespace, ApiScope, CallerGrant};
use crate::error::AppError;
use crate::AppState;

#[derive(Clone, Debug)]
pub struct McpRegistry {
    servers: BTreeMap<String, McpServer>,
    transport: StreamableHttpServerConfig,
}

impl McpRegistry {
    pub fn has_host_routes(&self) -> bool {
        self.servers.values().any(|server| server.host.is_some())
    }

    pub fn is_empty(&self) -> bool {
        self.servers.is_empty()
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServer {
    /// Optional dedicated host serving this server at `/<name>`.
    pub host: Option<String>,
    #[serde(default, rename = "genericTools")]
    pub generic_tools: bool,
    pub namespaces: Vec<McpNamespace>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct McpNamespace {
    pub name: String,
    pub tool_name: Option<String>,
    pub description: Option<String>,
    pub filters: Option<Vec<String>>,
    pub link: Option<String>,
}

impl McpNamespace {
    fn tool_name(&self) -> String {
        self.tool_name.clone().unwrap_or_else(|| {
            self.name
                .chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                        c
                    } else {
                        '_'
                    }
                })
                .collect()
        })
    }
}

/// Keep small-server tool choice while bounding discovery metadata work.
pub const PER_NAMESPACE_TOOL_THRESHOLD: usize = 8;
pub const MAX_SERVER_NAMESPACES: usize = 512;

impl McpServer {
    fn generic_tools(&self) -> bool {
        self.generic_tools || self.namespaces.len() > PER_NAMESPACE_TOOL_THRESHOLD
    }
}

fn discovery_tool() -> Tool {
    Tool::new(
        "list_namespaces",
        "List readable namespaces, tool names, descriptions, approximate row counts, search kinds, typed filters and source link attributes. Call this before choosing a namespace.",
        json!({"type":"object","properties":{},"additionalProperties":false}).as_object().unwrap().clone(),
    ).with_annotations(ToolAnnotations::new().read_only(true))
}

fn generic_tools() -> Vec<Tool> {
    let search = json!({"type":"object","properties":{
        "namespace":{"type":"string","minLength":1},
        "query":{"type":"string","minLength":1},
        "filters":{"type":"object","description":"Use the chosen namespace's typed filters from list_namespaces. Strings/booleans take exact values; numbers take min/max; dates take after/before."},
        "limit":{"type":"integer","minimum":1,"maximum":50,"default":10}
    },"required":["namespace","query"],"additionalProperties":false});
    let get = json!({"type":"object","properties":{
        "namespace":{"type":"string","minLength":1},
        "id":{"anyOf":[{"type":"string","minLength":1},{"type":"integer","minimum":0}]}
    },"required":["namespace","id"],"additionalProperties":false});
    [("search", "Search one readable namespace. Call list_namespaces for namespace names and filter types.", search),
     ("get", "Fetch one record by id from a readable namespace. Call list_namespaces for namespace names.", get)]
        .into_iter().map(|(name, description, schema)| Tool::new(name, description, schema.as_object().unwrap().clone())
            .with_annotations(ToolAnnotations::new().read_only(true))).collect()
}

fn invalid(message: impl Into<String>) -> AppError {
    AppError::Validation(message.into())
}

pub fn registry_from_json(raw: Option<&str>) -> Result<Arc<McpRegistry>, AppError> {
    let registry: BTreeMap<String, McpServer> = serde_json::from_str(raw.unwrap_or("{}"))
        .map_err(|e| invalid(format!("invalid LAYER_MCP_JSON: {e}")))?;
    for (name, spec) in &registry {
        if !valid_name(name, 64)
            || spec.namespaces.is_empty()
            || spec.namespaces.len() > MAX_SERVER_NAMESPACES
        {
            return Err(invalid(format!(
                "MCP server `{name}` needs a valid name and 1–512 namespaces"
            )));
        }
        if let Some(host) = &spec.host {
            if host.is_empty()
                || !host
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'.' || c == b'-')
            {
                return Err(invalid(
                    "MCP dedicated host must be an explicit DNS hostname",
                ));
            }
        }
        let mut namespaces = HashSet::new();
        let mut tools = HashSet::new();
        for ns in &spec.namespaces {
            if ns.name.is_empty()
                || ns.name.len() > 128
                || ns
                    .name
                    .chars()
                    .any(|c| c.is_whitespace() || c.is_control() || "/\\*?%#".contains(c))
                || !namespaces.insert(&ns.name)
            {
                return Err(invalid(format!(
                    "invalid or duplicate MCP namespace `{}`",
                    ns.name
                )));
            }
            let tool = ns.tool_name();
            if !valid_name(&tool, 57) || !tools.insert(tool) {
                return Err(invalid("invalid or colliding MCP tool names"));
            }
            if ns.link.as_ref().is_some_and(|v| v.trim().is_empty()) {
                return Err(invalid("MCP link must name an attribute"));
            }
            if let Some(filters) = &ns.filters {
                let mut seen = HashSet::new();
                if filters
                    .iter()
                    .any(|f| f.trim().is_empty() || !seen.insert(f))
                {
                    return Err(invalid("MCP filters must be distinct attribute names"));
                }
            }
        }
    }
    Ok(Arc::new(McpRegistry {
        servers: registry,
        transport: StreamableHttpServerConfig::default()
            .with_legacy_session_mode(false)
            .with_json_response(true)
            .enforce_origin_validation(),
    }))
}

fn valid_name(name: &str, max: usize) -> bool {
    !name.is_empty()
        && name.len() <= max
        && name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
}

pub fn registry_from_env() -> Result<Option<Arc<McpRegistry>>, AppError> {
    let enabled = std::env::var("LAYER_MCP_ENABLED").unwrap_or_else(|_| "false".into());
    match enabled.as_str() {
        "false" | "0" => Ok(None),
        "true" | "1" => {
            let mut registry = registry_from_json(std::env::var("LAYER_MCP_JSON").ok().as_deref())?;
            let config = &mut Arc::make_mut(&mut registry).transport;
            if let Ok(hosts) = std::env::var("LAYER_MCP_ALLOWED_HOSTS") {
                config.allowed_hosts = config_list(&hosts)?;
            }
            if let Ok(origins) = std::env::var("LAYER_MCP_ALLOWED_ORIGINS") {
                config.allowed_origins = config_list(&origins)?;
            }
            Ok(Some(registry))
        }
        _ => Err(invalid("LAYER_MCP_ENABLED must be true or false")),
    }
}

fn config_list(value: &str) -> Result<Vec<String>, AppError> {
    let entries: Vec<String> = value
        .split(',')
        .map(str::trim)
        .map(str::to_string)
        .collect();
    if entries
        .iter()
        .any(|entry| entry.is_empty() || entry.contains('*'))
    {
        return Err(invalid(
            "MCP host/origin allowlists require nonempty explicit entries",
        ));
    }
    Ok(entries)
}

/// Rewrite before routing and auth so both surfaces use identical authorization.
pub async fn route_host(
    State(registry): State<Arc<McpRegistry>>,
    mut request: Request,
    next: axum::middleware::Next,
) -> Response {
    if request.method() == axum::http::Method::POST {
        let host = request
            .headers()
            .get(axum::http::header::HOST)
            .and_then(|value| value.to_str().ok());
        let name = request.uri().path().strip_prefix('/').unwrap_or("");
        if let Some(expected) = registry
            .servers
            .get(name)
            .and_then(|server| server.host.as_deref())
        {
            if Some(expected) != host {
                return axum::http::StatusCode::NOT_FOUND.into_response();
            }
            let query = request
                .uri()
                .query()
                .map(|q| format!("?{q}"))
                .unwrap_or_default();
            // Names are validated at registry construction; query is already a valid URI.
            *request.uri_mut() = format!("/mcp/{name}{query}")
                .parse()
                .expect("valid MCP alias URI");
        }
    }
    next.run(request).await
}

pub async fn handle(
    State(state): State<Arc<AppState>>,
    Extension(registry): Extension<Arc<McpRegistry>>,
    Path(name): Path<String>,
    request: Request,
) -> Result<Response, AppError> {
    let grant = request.extensions().get::<CallerGrant>().cloned();
    // The normal middleware authenticates; require the explicit entitlement
    // here as well so open/declared/derived auth cannot bypass MCP auth.
    #[cfg(feature = "pro")]
    let entitled = match &grant {
        Some(CallerGrant::Minted(key)) => key
            .entitlements
            .get(&format!("mcp.{name}"))
            .is_some_and(|e| e.scopes.is_empty() && e.namespaces.is_empty()),
        _ => false,
    };
    #[cfg(not(feature = "pro"))]
    let entitled = false;
    if !entitled {
        return Err(AppError::Forbidden(format!(
            "the key needs mcp.{name} entitlement"
        )));
    }
    let spec = registry
        .servers
        .get(&name)
        .cloned()
        .ok_or_else(|| AppError::NotFound(format!("MCP server `{name}` not found")))?;
    let handler = McpHandler {
        state,
        spec,
        grant,
        headers: request.headers().clone(),
        name,
    };
    let service = StreamableHttpService::new(
        move || Ok(handler.clone()),
        Arc::new(NeverSessionManager::default()),
        registry.transport.clone(),
    );
    Ok(service.handle(request).await.into_response())
}

#[derive(Clone)]
struct McpHandler {
    state: Arc<AppState>,
    spec: McpServer,
    grant: Option<CallerGrant>,
    headers: HeaderMap,
    name: String,
}

impl ServerHandler for McpHandler {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("hevlayer", env!("CARGO_PKG_VERSION")))
    }

    async fn list_tools(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let mut tools = vec![discovery_tool()];
        if self.spec.generic_tools() {
            tools.extend(generic_tools());
            return Ok(ListToolsResult {
                tools,
                ..Default::default()
            });
        }
        for ns in &self.spec.namespaces {
            if self.authorize(ns).is_err() {
                continue;
            }
            let schema = self.schema(ns).await.map_err(mcp_error)?;
            tools.extend(schema.tools(ns));
        }
        Ok(ListToolsResult {
            tools,
            ..Default::default()
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        match self
            .execute(&request.name, request.arguments.unwrap_or_default())
            .await
        {
            Ok(result) => Ok(result.into()),
            Err(error) => {
                Ok(CallToolResult::error(vec![ContentBlock::text(error.to_string())]).into())
            }
        }
    }
}

fn mcp_error(error: AppError) -> ErrorData {
    ErrorData::internal_error(error.to_string(), None)
}

impl McpHandler {
    fn authorize(&self, ns: &McpNamespace) -> Result<(), AppError> {
        authorize_namespace(&self.state, self.grant.as_ref(), ApiScope::Read, &ns.name)
    }

    async fn schema(&self, ns: &McpNamespace) -> Result<NamespaceSchema, AppError> {
        self.authorize(ns)?;
        let metadata = self
            .state
            .turbopuffer()
            .head_namespace(&ns.name)
            .await
            .map_err(|e| AppError::from_turbopuffer(e, "MCP namespace schema read failed"))?;
        let row_count = metadata.approx_row_count;
        let mut metadata = metadata.raw;
        // The gateway holds profiles for stores that do not retain `embed` in
        // their wire schema. Reuse the same persisted Index configuration as /search.
        if let Some(schema) = metadata.get_mut("schema") {
            crate::routes::embed_wire::annotate_schema(&self.state, &ns.name, schema).await?;
        }
        let mut schema = NamespaceSchema::from_metadata(ns, &metadata)?;
        schema.row_count = row_count;
        Ok(schema)
    }

    async fn execute(
        &self,
        tool: &str,
        mut arguments: Map<String, Value>,
    ) -> Result<CallToolResult, AppError> {
        if tool == "list_namespaces" {
            if !arguments.is_empty() {
                return Err(invalid("list_namespaces takes no arguments"));
            }
            let mut namespaces = Vec::new();
            let mut text = String::new();
            for ns in &self.spec.namespaces {
                if self.authorize(ns).is_err() {
                    continue;
                }
                let schema = self.schema(ns).await?;
                let kind = schema.search_kind()?;
                let filters: Map<String, Value> = schema
                    .filters
                    .iter()
                    .map(|(name, ty)| {
                        (
                            name.clone(),
                            json!({"type":ty.name(),"inputSchema":ty.schema()}),
                        )
                    })
                    .collect();
                let tool_name = if self.spec.generic_tools() {
                    "search".into()
                } else {
                    format!("search_{}", ns.tool_name())
                };
                let description = ns.description.as_deref().unwrap_or(&ns.name);
                let filter_text = schema
                    .filters
                    .iter()
                    .map(|(name, ty)| format!("{name}: {}", ty.name()))
                    .collect::<Vec<_>>()
                    .join(", ");
                text.push_str(&format!(
                    "\n{} ({tool_name}): {description}; ~{} rows; {kind}; filters: {}; link: {}",
                    ns.name,
                    schema.row_count,
                    if filter_text.is_empty() {
                        "none"
                    } else {
                        &filter_text
                    },
                    ns.link.as_deref().unwrap_or("none")
                ));
                namespaces.push(json!({"name":ns.name,"toolName":tool_name,"description":description,
                    "rowCount":schema.row_count,"searchKind":kind,"filterableAttributes":filters,"linkAttribute":ns.link}));
            }
            let mut result = CallToolResult::structured(json!({"namespaces":namespaces}));
            result.content = vec![ContentBlock::text(format!(
                "{} readable namespace(s){text}",
                namespaces.len()
            ))];
            return Ok(result);
        }
        let (search, ns) = if self.spec.generic_tools() {
            let search = match tool {
                "search" => true,
                "get" => false,
                _ => return Err(invalid("unknown MCP tool")),
            };
            let namespace = arguments
                .remove("namespace")
                .and_then(|v| v.as_str().map(str::to_owned))
                .ok_or_else(|| {
                    invalid("namespace must be an exact namespace name from list_namespaces")
                })?;
            let ns = self
                .spec
                .namespaces
                .iter()
                .find(|ns| ns.name == namespace)
                .ok_or_else(|| invalid("namespace is not a member of this MCP server"))?;
            (search, ns)
        } else {
            let (search, suffix) = if let Some(suffix) = tool.strip_prefix("search_") {
                (true, suffix)
            } else if let Some(suffix) = tool.strip_prefix("get_") {
                (false, suffix)
            } else {
                return Err(invalid("unknown MCP tool"));
            };
            let ns = self
                .spec
                .namespaces
                .iter()
                .find(|ns| ns.tool_name() == suffix)
                .ok_or_else(|| invalid("unknown MCP tool"))?;
            (search, ns)
        };
        self.authorize(ns)?;
        let mut headers = self.headers.clone();
        // Existing history tags carry the source without a second history writer.
        headers.append(
            "x-hevlayer-tag",
            format!("mcp.{}", self.name)
                .parse()
                .map_err(|_| invalid("invalid MCP history tag"))?,
        );
        let response = if search {
            let schema = self.schema(ns).await?;
            let query = arguments
                .remove("query")
                .and_then(|v| v.as_str().map(str::to_owned))
                .filter(|v| !v.trim().is_empty())
                .ok_or_else(|| invalid("query must be a nonempty string"))?;
            let limit = match arguments.remove("limit") {
                None => 10,
                Some(v) => v
                    .as_u64()
                    .filter(|n| (1..=50).contains(n))
                    .ok_or_else(|| invalid("limit must be an integer from 1 to 50"))?,
            };
            let filters = schema.filters(arguments.remove("filters"))?;
            if !arguments.is_empty() {
                return Err(invalid("unknown search argument"));
            }
            let rank_by = schema.rank_by(&query)?;
            let mut body = json!({"rank_by":rank_by,"top_k":limit,"include_attributes":true});
            if let Some(filters) = filters {
                body["filters"] = filters;
            }
            crate::routes::query::query(
                State(self.state.clone()),
                Path(ns.name.clone()),
                OriginalUri(
                    format!("/v2/namespaces/{}/query", ns.name)
                        .parse()
                        .map_err(|_| invalid("invalid namespace URI"))?,
                ),
                headers,
                Json(body),
            )
            .await?
        } else {
            let id = match arguments.remove("id") {
                Some(Value::String(id)) if !id.is_empty() => id,
                Some(Value::Number(id)) if id.as_u64().is_some() => id.to_string(),
                _ => return Err(invalid("id must be a nonempty string or unsigned integer")),
            };
            if !arguments.is_empty() {
                return Err(invalid("unknown get argument"));
            }
            crate::routes::fetch::fetch_document(
                State(self.state.clone()),
                Path((ns.name.clone(), id)),
                Query(crate::routes::fetch::FetchQueryParams {
                    include_attributes: None,
                }),
                headers,
            )
            .await?
            .into_response()
        };
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 16 * 1024 * 1024)
            .await
            .map_err(|e| AppError::ServiceUnavailable(e.to_string()))?;
        let body: Value = serde_json::from_slice(&bytes)
            .map_err(|e| AppError::ServiceUnavailable(e.to_string()))?;
        if !status.is_success() {
            return Err(AppError::ServiceUnavailable(format!(
                "query failed ({status}): {body}"
            )));
        }
        let text = render(&body, ns.link.as_deref());
        let mut result = CallToolResult::structured(body);
        result.content = vec![ContentBlock::text(text)];
        Ok(result)
    }
}

#[derive(Clone, Debug)]
enum FilterType {
    String,
    Boolean,
    Number,
    Integer,
    Date,
}

struct NamespaceSchema {
    row_count: u64,
    filters: BTreeMap<String, FilterType>,
    text: Option<String>,
    fuzzy: bool,
    embed: Option<String>,
}

impl NamespaceSchema {
    fn from_metadata(ns: &McpNamespace, metadata: &Value) -> Result<Self, AppError> {
        let attrs = metadata
            .get("schema")
            .and_then(Value::as_object)
            .ok_or_else(|| invalid("namespace has no attribute schema"))?;
        let mut filters = BTreeMap::new();
        let mut text = Vec::new();
        let mut embed = Vec::new();
        for (name, attr) in attrs {
            if name.starts_with("_hevlayer_") {
                continue;
            }
            if attr
                .get("full_text_search")
                .is_some_and(|v| v == true || v.is_object())
            {
                text.push(name.clone());
            }
            if attr
                .get("embed")
                .is_some_and(|v| !v.is_null() && v != false)
            {
                embed.push(name.clone());
            }
            if attr.get("filterable") == Some(&Value::Bool(false)) {
                continue;
            }
            let ty = attr
                .get("type")
                .and_then(Value::as_str)
                .or_else(|| attr.as_str());
            let ty = match ty {
                Some("string" | "uuid") => FilterType::String,
                Some("bool" | "boolean") => FilterType::Boolean,
                Some("int" | "uint" | "int64" | "uint64") => FilterType::Integer,
                Some("float" | "float64" | "number") => FilterType::Number,
                Some("datetime" | "date") => FilterType::Date,
                _ => continue,
            };
            if ns
                .filters
                .as_ref()
                .is_none_or(|allowed| allowed.contains(name))
            {
                filters.insert(name.clone(), ty);
            }
        }
        if let Some(allowed) = &ns.filters {
            for name in allowed {
                if !filters.contains_key(name) {
                    return Err(invalid(format!("MCP filter `{name}` is missing, not filterable, or has an unsupported type")));
                }
            }
        }
        // An ambiguous schema must be fixed by the operator, never silently
        // search an arbitrary field. Prefer the canonical text field if set.
        let choose = |fields: Vec<String>| -> Option<String> {
            if fields.iter().any(|f| f == "text") {
                Some("text".into())
            } else if fields.len() == 1 {
                fields.into_iter().next()
            } else {
                None
            }
        };
        let text = choose(text);
        let fuzzy = text.as_ref().is_some_and(|field| {
            crate::routes::hybrid_text::attribute_fuzzy_enabled(metadata, field) == Some(true)
        });
        Ok(Self {
            row_count: metadata
                .get("approx_row_count")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            filters,
            fuzzy,
            text,
            embed: choose(embed),
        })
    }

    fn search_kind(&self) -> Result<&'static str, AppError> {
        match (&self.text, &self.embed) {
            (Some(_), Some(_)) => Ok("Auto"),
            (Some(_), None) => Ok(if self.fuzzy { "HybridText" } else { "BM25" }),
            (None, Some(_)) => Ok("ANN/Embed"),
            _ => Err(invalid("MCP search requires an unambiguous full-text or embed attribute in the Index schema")),
        }
    }

    fn rank_by(&self, query: &str) -> Result<Value, AppError> {
        match (&self.text, &self.embed) {
            (Some(text), Some(embed)) => {
                let mut options = json!({"vector":["Embed",query,{"field":embed}]});
                if !self.fuzzy { options["fuzziness"] = json!(0); }
                Ok(json!([text, "Auto", query, options]))
            },
            (Some(text), None) => Ok(json!([text, if self.fuzzy { "HybridText" } else { "BM25" }, query])),
            (None, Some(embed)) => Ok(json!([embed, "ANN", ["Embed",query]])),
            _ => Err(invalid("MCP search requires an unambiguous full-text or embed attribute in the Index schema")),
        }
    }

    fn tools(&self, ns: &McpNamespace) -> Vec<Tool> {
        let properties: Map<String, Value> = self
            .filters
            .iter()
            .map(|(name, ty)| (name.clone(), ty.schema()))
            .collect();
        let filter_names = self.filters.keys().cloned().collect::<Vec<_>>().join(", ");
        let description = format!(
            "{} Available filters: {}.",
            ns.description.as_deref().unwrap_or(&ns.name),
            if filter_names.is_empty() {
                "none"
            } else {
                &filter_names
            }
        );
        let search_schema = json!({"type":"object","properties":{
            "query":{"type":"string","minLength":1},
            "filters":{"type":"object","properties":properties,"additionalProperties":false},
            "limit":{"type":"integer","minimum":1,"maximum":50,"default":10}
        },"required":["query"],"additionalProperties":false});
        let get_schema = json!({"type":"object","properties":{"id":{"anyOf":[{"type":"string","minLength":1},{"type":"integer","minimum":0}]}},"required":["id"],"additionalProperties":false});
        vec![
            Tool::new(
                format!("search_{}", ns.tool_name()),
                format!("Search. {description}"),
                search_schema.as_object().unwrap().clone(),
            )
            .with_annotations(ToolAnnotations::new().read_only(true)),
            Tool::new(
                format!("get_{}", ns.tool_name()),
                format!("Fetch one record by id. {description}"),
                get_schema.as_object().unwrap().clone(),
            )
            .with_annotations(ToolAnnotations::new().read_only(true)),
        ]
    }

    fn filters(&self, value: Option<Value>) -> Result<Option<Value>, AppError> {
        let Some(value) = value else {
            return Ok(None);
        };
        let values = value
            .as_object()
            .ok_or_else(|| invalid("filters must be an object"))?;
        let mut predicates = Vec::new();
        for (name, value) in values {
            let ty = self
                .filters
                .get(name)
                .ok_or_else(|| invalid(format!("unknown filter `{name}`")))?;
            match ty {
                FilterType::String if value.is_string() => {
                    predicates.push(json!([name, "Eq", value]))
                }
                FilterType::Boolean if value.is_boolean() => {
                    predicates.push(json!([name, "Eq", value]))
                }
                FilterType::Number | FilterType::Integer | FilterType::Date => {
                    let bounds = value
                        .as_object()
                        .filter(|v| !v.is_empty())
                        .ok_or_else(|| invalid("range filter needs bounds"))?;
                    for (bound, value) in bounds {
                        let (lower, upper) = if matches!(ty, FilterType::Date) {
                            ("after", "before")
                        } else {
                            ("min", "max")
                        };
                        let op = if bound == lower {
                            "Gte"
                        } else if bound == upper {
                            "Lte"
                        } else {
                            return Err(invalid("unknown range bound"));
                        };
                        let valid = match ty {
                            FilterType::Date => value
                                .as_str()
                                .is_some_and(|v| chrono::DateTime::parse_from_rfc3339(v).is_ok()),
                            FilterType::Integer => value.is_i64() || value.is_u64(),
                            _ => value.is_number(),
                        };
                        if !valid {
                            return Err(invalid(format!("invalid type for filter `{name}`")));
                        }
                        predicates.push(json!([name, op, value]));
                    }
                }
                _ => return Err(invalid(format!("invalid type for filter `{name}`"))),
            }
        }
        Ok(match predicates.len() {
            0 => None,
            1 => predicates.pop(),
            _ => Some(json!(["And", predicates])),
        })
    }
}

impl FilterType {
    fn name(&self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Boolean => "boolean",
            Self::Number => "number",
            Self::Integer => "integer",
            Self::Date => "datetime",
        }
    }

    fn schema(&self) -> Value {
        match self {
            Self::String => json!({"type":"string"}),
            Self::Boolean => json!({"type":"boolean"}),
            Self::Date => {
                json!({"type":"object","properties":{"after":{"type":"string","format":"date-time"},"before":{"type":"string","format":"date-time"}},"minProperties":1,"additionalProperties":false})
            }
            Self::Number | Self::Integer => {
                let ty = if matches!(self, Self::Integer) {
                    "integer"
                } else {
                    "number"
                };
                json!({"type":"object","properties":{"min":{"type":ty},"max":{"type":ty}},"minProperties":1,"additionalProperties":false})
            }
        }
    }
}

fn render(body: &Value, link: Option<&str>) -> String {
    let rows = body.get("rows").and_then(Value::as_array);
    let hits: Vec<&Value> = rows
        .map(|rows| rows.iter().collect())
        .unwrap_or_else(|| vec![body]);
    let mut text = format!("{} record(s)", hits.len());
    for hit in hits {
        let id = hit.get("id").unwrap_or(&Value::Null);
        let attrs = hit.get("attributes").unwrap_or(hit);
        let title = attrs
            .get("title")
            .or_else(|| attrs.get("text"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let excerpt: String = title.chars().take(240).collect();
        text.push_str(&format!("\n{id}: {excerpt}"));
        if let Some(url) = link
            .and_then(|field| attrs.get(field))
            .and_then(Value::as_str)
        {
            text.push_str(&format!(" — {url}"));
        }
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ns() -> McpNamespace {
        McpNamespace {
            name: "docs".into(),
            tool_name: None,
            description: None,
            filters: None,
            link: None,
        }
    }

    #[test]
    fn search_uses_schema_text_and_embedding_fields() {
        let text = NamespaceSchema::from_metadata(
            &ns(),
            &json!({"schema":{"body":{"type":"string","full_text_search":true}}}),
        )
        .unwrap();
        assert_eq!(text.search_kind().unwrap(), "BM25");
        assert_eq!(text.rank_by("q").unwrap(), json!(["body", "BM25", "q"]));
        let vector = NamespaceSchema::from_metadata(
            &ns(),
            &json!({"schema":{"body":{"type":"string","embed":{"model":"test"}}}}),
        )
        .unwrap();
        assert_eq!(vector.search_kind().unwrap(), "ANN/Embed");
        assert_eq!(
            vector.rank_by("q").unwrap(),
            json!(["body", "ANN", ["Embed", "q"]])
        );
        let both = NamespaceSchema::from_metadata(&ns(), &json!({"schema":{"body":{"type":"string","full_text_search":true,"embed":{"model":"test"}}}})).unwrap();
        assert_eq!(both.search_kind().unwrap(), "Auto");
        assert_eq!(
            both.rank_by("q").unwrap(),
            json!(["body","Auto","q",{"vector":["Embed","q",{"field":"body"}],"fuzziness":0}])
        );
        let ambiguous = NamespaceSchema::from_metadata(&ns(), &json!({"schema":{"a":{"type":"string","full_text_search":true},"b":{"type":"string","full_text_search":true}}})).unwrap();
        assert!(ambiguous.rank_by("q").is_err());
        assert!(ambiguous.search_kind().is_err());
    }

    #[test]
    fn fuzzy_text_and_auto_keep_fuzzy_routing() {
        for embed in [false, true] {
            let mut metadata =
                json!({"schema":{"body":{"type":"string","full_text_search":{},"fuzzy":true}}});
            if embed {
                metadata["schema"]["body"]["embed"] = json!({"model":"test"});
            }
            let schema = NamespaceSchema::from_metadata(&ns(), &metadata).unwrap();
            let rank = schema.rank_by("q").unwrap();
            assert_eq!(
                schema.search_kind().unwrap(),
                if embed { "Auto" } else { "HybridText" }
            );
            assert_eq!(rank[1], if embed { "Auto" } else { "HybridText" });
            assert!(rank
                .get(3)
                .is_none_or(|options| options.get("fuzziness").is_none()));
        }
    }

    #[test]
    fn configured_filters_must_resolve_to_filterable_attributes() {
        let mut spec = ns();
        spec.filters = Some(vec!["secret".into()]);
        assert!(NamespaceSchema::from_metadata(
            &spec,
            &json!({"schema":{"secret":{"type":"string","filterable":false}}})
        )
        .is_err());
        assert!(NamespaceSchema::from_metadata(&spec, &json!({"schema":{}})).is_err());
    }
}
