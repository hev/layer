//! Seeded, stateless, read-only MCP data servers. No independent namespace ACL.
use std::collections::{BTreeMap, HashMap, HashSet};
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
    /// Attribute -> exact value ANDed into every search, get, aggregate and
    /// fetch_original on this tool. Clients cannot see, widen or override it.
    #[serde(default, rename = "where")]
    pub pinned: Option<BTreeMap<String, Value>>,
    pub link: Option<String>,
    /// Also show each page row's `page` as a PDF page-anchored copy of `link`.
    #[serde(default)]
    pub page_link: bool,
    /// Serve the retained original file (and bounded page images) on demand
    /// through `fetch_original`. Search and `get` stay text-only.
    pub originals: Option<crate::mcp_original::OriginalsConfig>,
    /// Gateway collapse configuration; search returns only each group's best row.
    pub collapse: Option<Value>,
}

impl McpNamespace {
    /// The pinned filter as a store predicate, if the namespace has one.
    fn pin(&self) -> Option<Value> {
        let pinned = self.pinned.as_ref().filter(|p| !p.is_empty())?;
        let mut predicates: Vec<Value> = pinned
            .iter()
            .map(|(name, value)| json!([name, "Eq", value]))
            .collect();
        Some(if predicates.len() == 1 {
            predicates.remove(0)
        } else {
            json!(["And", predicates])
        })
    }

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

const SEARCH_SCOPE_GUIDANCE: &str = "Query text ranks content; it does not constrain attribute values. For a job, project, company or document-type constraint, use the declared typed filters with exact stored values. If an identifier is unknown, search its name alone and inspect matching records, or aggregate the declared field's values; do not guess. If an optional document-type filter returns no hits, retry without only that type filter while retaining the exact job, project, company and other constraints; classification may be incomplete. Report the relaxed type filter and inspect content before claiming a match. Keep an explicitly requested exact-classification constraint. Do not present unfiltered hits as matches for a requested project, or substitute other projects when a scoped search is empty.";
const SEARCH_QUERY_DESCRIPTION: &str = "Text to rank. Naming a project here does not filter its attributes; resolve its stored identifier and pass the appropriate typed filters.";

impl McpServer {
    fn generic_tools(&self) -> bool {
        self.generic_tools || self.namespaces.len() > PER_NAMESPACE_TOOL_THRESHOLD
    }
}

fn discovery_tool() -> Tool {
    Tool::new(
        "list_namespaces",
        "List readable namespaces, tool names, descriptions, approximate row counts, search kinds, typed filters, the fields aggregate can group and sum, and source link attributes. Call this before choosing a namespace.",
        json!({"type":"object","properties":{},"additionalProperties":false}).as_object().unwrap().clone(),
    ).with_annotations(ToolAnnotations::new().read_only(true))
}

fn generic_tools() -> Vec<Tool> {
    let search = json!({"type":"object","properties":{
        "namespace":{"type":"string","minLength":1},
        "query":{"type":"string","minLength":1,"description":SEARCH_QUERY_DESCRIPTION},
        "filters":{"type":"object","description":"Use the chosen namespace's typed filters from list_namespaces. Strings/booleans take exact values; numbers take min/max; dates take after/before."},
        "limit":{"type":"integer","minimum":1,"maximum":50,"default":10},
        "cursor":{"type":"string","minLength":1,"description":"The next_cursor from the previous page of the same query. Omit for the first page."}
    },"required":["namespace","query"],"additionalProperties":false});
    let get = json!({"type":"object","properties":{
        "namespace":{"type":"string","minLength":1},
        "id":{"anyOf":[{"type":"string","minLength":1},{"type":"integer","minimum":0}]}
    },"required":["namespace","id"],"additionalProperties":false});
    let aggregate = json!({"type":"object","properties":{
        "namespace":{"type":"string","minLength":1},
        "group_by":aggregate_group_by_schema(),
        "aggregates":aggregate_aggregates_schema(),
        "filters":{"type":"object","description":"The same typed filters as search. Strings/booleans take exact values; numbers take min/max; dates take after/before."},
        "limit":aggregate_limit_schema()
    },"required":["namespace"],"additionalProperties":false});
    [("search", "Search one readable namespace. Call list_namespaces for namespace names and filter types. Returns total and next_cursor; pass next_cursor as cursor, with the same query and filters, for the next page.", search),
     ("get", "Fetch one record by id from a readable namespace. Call list_namespaces for namespace names.", get),
     ("aggregate", AGGREGATE_DESCRIPTION, aggregate)]
        .into_iter().map(|(name, description, schema)| {
            let description = if name == "search" {
                format!("{description} {SEARCH_SCOPE_GUIDANCE}")
            } else {
                description.to_owned()
            };
            Tool::new(name, description, schema.as_object().unwrap().clone())
                .with_annotations(ToolAnnotations::new().read_only(true))
        }).collect()
}

fn fetch_original_description() -> &'static str {
    "Fetch the retained ORIGINAL file by a record id from search/get, or a private original:<source-id> catalog reference for an unindexed source file. \
     Without `pages` it returns the whole typed original as an embedded resource when it is small enough; some \
     hosts do not show embedded files to the model. With `pages` (page numbers of the original PDF) it \
     returns those pages as images the model can see, for charts, drawings, stamps and anything OCR text \
     missed. The source (SharePoint) link and page links are always in the text, never replaced."
}

fn fetch_original_schema(generic: bool, max_pages: usize) -> Map<String, Value> {
    let mut properties = json!({
        "id":{"anyOf":[{"type":"string","minLength":1},{"type":"integer","minimum":0}]},
        "pages":{"type":"array","items":{"type":"integer","minimum":1},"minItems":1,"maxItems":max_pages,
            "description":"Original PDF page numbers to return as images."}
    });
    let mut required = vec!["id"];
    if generic {
        properties["namespace"] = json!({"type":"string","minLength":1});
        required.push("namespace");
    }
    json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
        .as_object()
        .unwrap()
        .clone()
}

const AGGREGATE_DESCRIPTION: &str = "Count rows, or sum, min and max a numeric field, optionally grouped by one or two fields, with the same filters as search. Call list_namespaces for the groupable and summable fields. Every call is billed by the store at the namespace's size, so ask for everything in one call: at most 4 aggregates, and min/max (ungrouped only) each cost one more store query.";

fn aggregate_group_by_schema() -> Value {
    json!({"type":"array","items":{"type":"string"},"maxItems":MAX_GROUP_BY,"uniqueItems":true,
        "description":"One or two groupable fields from list_namespaces. Omit for a single total."})
}

fn aggregate_aggregates_schema() -> Value {
    json!({"type":"array","minItems":1,"maxItems":MAX_AGGREGATES,"items":{"type":"object","properties":{
        "fn":{"type":"string","enum":["count","sum","min","max"]},
        "field":{"type":"string","description":"A summable field for sum; a numeric or date field for min/max. Not used by count."}
    },"required":["fn"],"additionalProperties":false},
        "description":"Default: [{\"fn\":\"count\"}]. Results are named count, sum_<field>, min_<field>, max_<field>. min and max are not available with group_by."})
}

fn aggregate_limit_schema() -> Value {
    json!({"type":"integer","minimum":1,"maximum":MAX_GROUPS,"default":DEFAULT_GROUPS,
        "description":"Most groups returned. Groups come in key order, so a truncated result says so; add filters to narrow it."})
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
            if let Some(collapse) = &ns.collapse {
                crate::routes::collapse::parse(collapse)?;
            }
            if let Some(originals) = &ns.originals {
                originals.validate()?;
            }
            if let Some(pinned) = &ns.pinned {
                if pinned.is_empty()
                    || pinned.iter().any(|(name, value)| {
                        name.trim().is_empty()
                            || name.starts_with("_hevlayer_")
                            || !(value.is_string() || value.is_boolean() || value.is_number())
                    })
                {
                    return Err(invalid(
                        "MCP where must pin one or more attributes to a string, boolean or number",
                    ));
                }
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
    let billing_caller = request
        .extensions()
        .get::<crate::auth::AuthenticatedApiKey>()
        .map(|key| crate::metrics::BillingCaller::api_key(&key.name))
        .unwrap_or_else(crate::metrics::BillingCaller::unknown);
    let handler = McpHandler {
        billing_caller,
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
    billing_caller: crate::metrics::BillingCaller,
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
            if let Some(max_pages) = self
                .spec
                .namespaces
                .iter()
                .filter(|ns| self.authorize(ns).is_ok())
                .filter_map(|ns| ns.originals.as_ref().map(|o| o.max_pages()))
                .max()
            {
                tools.push(
                    Tool::new(
                        "fetch_original",
                        fetch_original_description(),
                        fetch_original_schema(true, max_pages),
                    )
                    .with_annotations(ToolAnnotations::new().read_only(true)),
                );
            }
            return Ok(ListToolsResult {
                tools,
                ttl_ms: Some(0),
                cache_scope: Some(CacheScope::Private),
                ..Default::default()
            });
        }
        for ns in &self.spec.namespaces {
            if self.authorize(ns).is_err() {
                continue;
            }
            let schema = self.schema(ns).await.map_err(mcp_error)?;
            tools.extend(schema.tools(ns));
            if let Some(originals) = &ns.originals {
                tools.push(
                    Tool::new(
                        format!("fetch_original_{}", ns.tool_name()),
                        fetch_original_description(),
                        fetch_original_schema(false, originals.max_pages()),
                    )
                    .with_annotations(ToolAnnotations::new().read_only(true)),
                );
            }
        }
        Ok(ListToolsResult {
            tools,
            ttl_ms: Some(0),
            cache_scope: Some(CacheScope::Private),
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
            Err(failure) => Ok(failure.into_result().into()),
        }
    }
}

/// A tool failure the model can act on: a stable `code`, a message and the
/// details that name the fix (the accepted arguments, a near miss).
enum ToolFailure {
    App(AppError),
    Typed {
        code: &'static str,
        message: String,
        details: Value,
    },
}

impl From<AppError> for ToolFailure {
    fn from(error: AppError) -> Self {
        Self::App(error)
    }
}

impl ToolFailure {
    fn typed(code: &'static str, message: impl Into<String>, details: Value) -> Self {
        Self::Typed {
            code,
            message: message.into(),
            details,
        }
    }

    fn into_result(self) -> CallToolResult {
        let (code, message, details) = match self {
            Self::Typed {
                code,
                message,
                details,
            } => (code, message, details),
            Self::App(AppError::Validation(message)) => ("invalid_argument", message, json!({})),
            Self::App(other) => ("tool_failed", other.to_string(), json!({})),
        };
        let mut error = json!({"code":code,"message":message});
        if let (Some(error), Some(details)) = (error.as_object_mut(), details.as_object()) {
            error.extend(details.clone());
        }
        let mut result = CallToolResult::error(vec![ContentBlock::text(format!(
            "{code}: {}",
            error["message"].as_str().unwrap_or_default()
        ))]);
        result.structured_content = Some(json!({"error":error}));
        result
    }
}

/// Reject argument names the tool does not take, naming the nearest accepted one.
fn check_arguments(
    tool: &str,
    arguments: &Map<String, Value>,
    allowed: &[&str],
) -> Result<(), ToolFailure> {
    let unknown: Vec<&String> = arguments
        .keys()
        .filter(|name| !allowed.contains(&name.as_str()))
        .collect();
    let Some(first) = unknown.first() else {
        return Ok(());
    };
    let suggestion = did_you_mean(first, allowed);
    let hint = suggestion
        .map(|s| format!(" Did you mean `{s}`?"))
        .unwrap_or_default();
    Err(ToolFailure::typed(
        "unknown_argument",
        format!(
            "`{first}` is not an argument of {tool}; it takes {}.{hint}",
            if allowed.is_empty() {
                "no arguments".to_string()
            } else {
                allowed.join(", ")
            }
        ),
        json!({"tool":tool,"unknown":unknown,"allowed":allowed,"did_you_mean":suggestion}),
    ))
}

fn did_you_mean<'a>(given: &str, allowed: &[&'a str]) -> Option<&'a str> {
    let given = given.to_ascii_lowercase();
    let alias = match given.as_str() {
        "q" | "text" | "search" | "term" | "terms" | "keywords" | "search_query" => "query",
        "n" | "k" | "top_k" | "topk" | "page_size" | "pagesize" | "per_page" | "max_results"
        | "size" | "count" => "limit",
        "filter" | "where" => "filters",
        "ns" | "index" | "collection" => "namespace",
        "group" | "groupby" | "group_by_fields" | "by" | "groups" | "dimensions" => "group_by",
        "aggregate" | "aggregations" | "metrics" | "measures" | "functions" | "agg" => "aggregates",
        "page_token" | "next_cursor" | "next_page_token" | "pagetoken" | "next" | "after" => {
            "cursor"
        }
        "doc_id" | "document_id" | "record_id" | "key" | "_id" => "id",
        _ => "",
    };
    if let Some(found) = allowed.iter().find(|a| **a == alias) {
        return Some(found);
    }
    allowed
        .iter()
        .map(|a| (edit_distance(&given, a), *a))
        .filter(|(d, a)| *d <= 2.max(a.len() / 4))
        .min_by_key(|(d, _)| *d)
        .map(|(_, a)| a)
}

fn edit_distance(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut row: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.chars().enumerate() {
        let mut prev = row[0];
        row[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let next = (row[j + 1] + 1)
                .min(row[j] + 1)
                .min(prev + usize::from(ca != *cb));
            prev = row[j + 1];
            row[j + 1] = next;
        }
    }
    row[b.len()]
}

fn mcp_error(error: AppError) -> ErrorData {
    ErrorData::internal_error(error.to_string(), None)
}

impl McpHandler {
    fn authorize(&self, ns: &McpNamespace) -> Result<(), AppError> {
        authorize_namespace(&self.state, self.grant.as_ref(), ApiScope::Read, &ns.name)
    }

    async fn schema(&self, ns: &McpNamespace) -> Result<NamespaceSchema, AppError> {
        crate::metrics::scope_billing_caller(
            self.billing_caller.clone(),
            self.schema_attributed(ns),
        )
        .await
    }

    async fn schema_attributed(&self, ns: &McpNamespace) -> Result<NamespaceSchema, AppError> {
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
        arguments: Map<String, Value>,
    ) -> Result<CallToolResult, ToolFailure> {
        crate::metrics::scope_billing_caller(
            self.billing_caller.clone(),
            self.execute_attributed(tool, arguments),
        )
        .await
    }

    async fn execute_attributed(
        &self,
        tool: &str,
        mut arguments: Map<String, Value>,
    ) -> Result<CallToolResult, ToolFailure> {
        if tool == "list_namespaces" {
            check_arguments(tool, &arguments, &[])?;
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
                let fields = |keep: fn(&FilterType) -> bool| -> Vec<&String> {
                    schema
                        .filters
                        .iter()
                        .filter(|(_, ty)| keep(ty))
                        .map(|(name, _)| name)
                        .collect()
                };
                let (groupable, summable, extrema) = (
                    fields(FilterType::groupable),
                    fields(FilterType::summable),
                    fields(FilterType::extrema),
                );
                let names = |fields: &[&String]| {
                    if fields.is_empty() {
                        "none".to_string()
                    } else {
                        fields
                            .iter()
                            .map(|f| f.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    }
                };
                let aggregate_tool = if self.spec.generic_tools() {
                    "aggregate".into()
                } else {
                    format!("aggregate_{}", ns.tool_name())
                };
                text.push_str(&format!(
                    "\n{} ({tool_name}): {description}; ~{} rows; {kind}; filters: {}; groupable: {}; summable: {}; link: {}",
                    ns.name,
                    schema.row_count,
                    if filter_text.is_empty() {
                        "none"
                    } else {
                        &filter_text
                    },
                    names(&groupable),
                    names(&summable),
                    ns.link.as_deref().unwrap_or("none")
                ));
                namespaces.push(
                    json!({"name":ns.name,"toolName":tool_name,"description":description,
                    "rowCount":schema.row_count,"searchKind":kind,"filterableAttributes":filters,
                    "aggregateToolName":aggregate_tool,"groupableAttributes":groupable,
                    "summableAttributes":summable,"extremaAttributes":extrema,
                    "linkAttribute":ns.link,"collapse":ns.collapse}),
                );
            }
            let mut result = CallToolResult::structured(json!({"namespaces":namespaces}));
            result.content = vec![ContentBlock::text(format!(
                "{} readable namespace(s){text}",
                namespaces.len()
            ))];
            return Ok(result);
        }
        let (kind, ns) = if self.spec.generic_tools() {
            let kind = match tool {
                "search" => ToolKind::Search,
                "get" => ToolKind::Get,
                "aggregate" => ToolKind::Aggregate,
                "fetch_original" => ToolKind::Original,
                _ => return Err(invalid("unknown MCP tool").into()),
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
            (kind, ns)
        } else {
            let (kind, suffix) = if let Some(suffix) = tool.strip_prefix("search_") {
                (ToolKind::Search, suffix)
            } else if let Some(suffix) = tool.strip_prefix("get_") {
                (ToolKind::Get, suffix)
            } else if let Some(suffix) = tool.strip_prefix("aggregate_") {
                (ToolKind::Aggregate, suffix)
            } else if let Some(suffix) = tool.strip_prefix("fetch_original_") {
                (ToolKind::Original, suffix)
            } else {
                return Err(invalid("unknown MCP tool").into());
            };
            let ns = self
                .spec
                .namespaces
                .iter()
                .find(|ns| ns.tool_name() == suffix)
                .ok_or_else(|| invalid("unknown MCP tool"))?;
            (kind, ns)
        };
        check_arguments(
            tool,
            &arguments,
            match kind {
                ToolKind::Search => &["query", "filters", "limit", "cursor"],
                ToolKind::Get => &["id"],
                ToolKind::Aggregate => &["group_by", "aggregates", "filters", "limit"],
                ToolKind::Original => &["id", "pages"],
            },
        )?;
        self.authorize(ns)?;
        if matches!(kind, ToolKind::Original) {
            return self
                .fetch_original(ns, arguments)
                .await
                .map_err(ToolFailure::from);
        }
        let mut headers = self.headers.clone();
        // Existing history tags carry the source without a second history writer.
        headers.append(
            "x-hevlayer-tag",
            format!("mcp.{}", self.name)
                .parse()
                .map_err(|_| invalid("invalid MCP history tag"))?,
        );
        match kind {
            ToolKind::Search => return self.search_page(ns, headers, arguments).await,
            ToolKind::Aggregate => return self.aggregate(ns, headers, arguments).await,
            ToolKind::Get => {}
            ToolKind::Original => unreachable!("handled above"),
        }
        let id = match arguments.remove("id") {
            Some(Value::String(id)) if !id.is_empty() => id,
            Some(Value::Number(id)) if id.as_u64().is_some() => id.to_string(),
            _ => return Err(invalid("id must be a nonempty string or unsigned integer").into()),
        };
        self.require_in_pin(ns, &headers, &id).await?;
        let id_for_pin = id.clone();
        let mut body = if ns.originals.is_some() {
            self.current_original_record(ns, &id_for_pin, &headers)
                .await?
        } else {
            let response = crate::routes::fetch::fetch_document(
                State(self.state.clone()),
                Path((ns.name.clone(), id)),
                Query(crate::routes::fetch::FetchQueryParams {
                    include_attributes: None,
                }),
                headers,
            )
            .await?
            .into_response();
            decode_response(response).await?
        };
        self.check_pin(ns, &id_for_pin, body.get("attributes").unwrap_or(&body))?;
        body["links"] = json!(result_links(&body, ns.link.as_deref(), ns.page_link));
        let mut text = render(&body, ns.link.as_deref(), ns.page_link);
        if ns.originals.is_some()
            && body
                .pointer("/attributes/original_blob")
                .or(body.get("original_blob"))
                .is_some()
        {
            text.push_str("\nThe original file is retained: call fetch_original with this id (optionally `pages`).");
        }
        let mut result = CallToolResult::structured(body);
        result.content = vec![ContentBlock::text(text)];
        Ok(result)
    }

    /// Retention can change after a Function completion while the document cache
    /// still holds an older full row. Never bind originals from that snapshot.
    async fn current_original_record(
        &self,
        ns: &McpNamespace,
        id: &str,
        headers: &HeaderMap,
    ) -> Result<Value, AppError> {
        let started = std::time::Instant::now();
        let trace_id = crate::history::trace_id_from_headers(headers);
        let tags = crate::history::tags_from_headers(headers).map_err(AppError::Validation)?;
        let doc = self
            .state
            .turbopuffer()
            .fetch(&ns.name, id)
            .await
            .map_err(|error| {
                AppError::from_turbopuffer(error, "Current original metadata fetch failed")
            })?
            .ok_or_else(|| AppError::NotFound(format!("record `{id}` was not found")))?;
        self.state.metrics.observe_fetch(
            "get_doc",
            &ns.name,
            crate::metrics::CACHE_MISS,
            started.elapsed().as_secs_f64(),
        );
        crate::routes::fetch::spawn_clickstream_event(
            &self.state,
            trace_id.as_deref(),
            &ns.name,
            id,
            &tags,
            "single",
            "upstream",
        );
        Ok(json!({"id": id, "attributes": doc.attributes}))
    }

    /// A pinned tool serves a record by id only when the pin matches it. A
    /// record outside the pin is reported exactly like a missing one.
    async fn require_in_pin(
        &self,
        ns: &McpNamespace,
        headers: &HeaderMap,
        id: &str,
    ) -> Result<(), AppError> {
        if ns.pin().is_none() {
            return Ok(());
        }
        // Validates the pinned attributes against the live schema.
        self.schema(ns).await?;
        let found = self
            .query_body(
                ns,
                headers,
                json!({"rank_by":["id","asc"],"top_k":1,"include_attributes":false,
                       "filters":["id","In",[id]]}),
            )
            .await?;
        let listed = found["rows"].as_array().is_some_and(|rows| {
            rows.iter().any(|row| {
                row["id"]
                    .as_str()
                    .map_or(row["id"].to_string(), str::to_owned)
                    == id
            })
        });
        if listed {
            Ok(())
        } else {
            Err(pinned_out(id))
        }
    }

    /// The fetched record itself must carry the pinned values. A partial cached
    /// row without the attribute leaves the decision to the id lookup above.
    fn check_pin(&self, ns: &McpNamespace, id: &str, attrs: &Value) -> Result<(), AppError> {
        for (name, want) in ns.pinned.iter().flatten() {
            if attrs.get(name).is_some_and(|have| have != want) {
                return Err(pinned_out(id));
            }
        }
        Ok(())
    }

    async fn query_body(
        &self,
        ns: &McpNamespace,
        headers: &HeaderMap,
        mut body: Value,
    ) -> Result<Value, AppError> {
        if let Some(pin) = ns.pin() {
            body["filters"] = match body.get("filters").filter(|f| !f.is_null()) {
                Some(filters) => json!(["And", [filters, pin]]),
                None => pin,
            };
        }
        let response = crate::routes::query::query(
            State(self.state.clone()),
            Path(ns.name.clone()),
            OriginalUri(
                format!("/v2/namespaces/{}/query", ns.name)
                    .parse()
                    .map_err(|_| invalid("invalid namespace URI"))?,
            ),
            headers.clone(),
            Json(body),
        )
        .await?;
        let mut body = decode_response(response).await?;
        if body.get("groups").is_some() {
            body = compact_groups(body)?;
        }
        Ok(body)
    }

    /// Replace the ids a ranking returned with their rows, in rank order. A row
    /// deleted since stays as its id, so a page never loses a place.
    async fn fill_attributes(
        &self,
        ns: &McpNamespace,
        headers: &HeaderMap,
        page: &mut [Value],
    ) -> Result<(), AppError> {
        self.fill_selected_attributes(ns, headers, page, json!(true))
            .await
    }

    async fn fill_selected_attributes(
        &self,
        ns: &McpNamespace,
        headers: &HeaderMap,
        page: &mut [Value],
        attributes: Value,
    ) -> Result<(), AppError> {
        let row_of = |item: &Value| -> Value {
            if ns.collapse.is_some() {
                item["best"].clone()
            } else {
                item.clone()
            }
        };
        let ids: Vec<Value> = page.iter().map(|item| row_of(item)["id"].clone()).collect();
        if ids.is_empty() {
            return Ok(());
        }
        let found = self
            .query_body(
                ns,
                headers,
                json!({"rank_by":["id","asc"],"top_k":ids.len(),"include_attributes":attributes,
                       "filters":["id","In",ids]}),
            )
            .await?;
        let rows: HashMap<String, Value> = found["rows"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|row| (row["id"].to_string(), row.clone()))
            .collect();
        for item in page.iter_mut() {
            let id = row_of(item)["id"].to_string();
            let Some(mut row) = rows.get(&id).cloned() else {
                continue;
            };
            let slot = if ns.collapse.is_some() {
                &mut item["best"]
            } else {
                &mut *item
            };
            // The ranking's score (`$score`, `$dist`) stays; the id lookup has none.
            if let (Some(ranked), Some(row)) = (slot.as_object(), row.as_object_mut()) {
                row.retain(|name, _| !name.starts_with('$'));
                for (name, value) in ranked.iter().filter(|(name, _)| name.starts_with('$')) {
                    row.insert(name.clone(), value.clone());
                }
            }
            *slot = row;
        }
        Ok(())
    }

    /// One page of a search. The first page pins `as_of` and counts the
    /// matches; the cursor carries both, so every later page reads the same
    /// snapshot: rows written after page one cannot shift the offset.
    async fn search_page(
        &self,
        ns: &McpNamespace,
        headers: HeaderMap,
        mut arguments: Map<String, Value>,
    ) -> Result<CallToolResult, ToolFailure> {
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
        let requested_filters = arguments.remove("filters");
        let filters = schema.filters(requested_filters.clone())?;
        let rank_by = schema.rank_by(&query)?;
        let pagination_order = if schema.labels.is_empty() {
            rank_by.clone()
        } else {
            json!({"rank":rank_by,"labels":schema.labels,"label_window":LABEL_WINDOW,"label_policy":2})
        };
        let fingerprint = page_fingerprint(ns, &pagination_order, filters.as_ref());
        let (as_of, offset, known_total) = match arguments.remove("cursor") {
            None => (crate::consistency::now_ms(), 0, None),
            Some(Value::String(cursor)) if !cursor.is_empty() => {
                let cursor = PageCursor::decode(&cursor, &fingerprint)?;
                (cursor.as_of, cursor.offset, Some(cursor.total))
            }
            Some(_) => {
                return Err(
                    invalid("cursor must be the next_cursor string of a previous page").into(),
                )
            }
        };
        let take = limit.min(MAX_WINDOW.saturating_sub(offset));
        // Rank ids only, over the same fixed window on every page: approximate
        // ranking can reorder when `top_k` changes, and a page of full text
        // down to a deep offset can outgrow a response. Page two reads the
        // ranking page one saw.
        let mut base = json!({"rank_by":rank_by,"top_k":MAX_WINDOW,"include_attributes":false});
        if let Some(collapse) = &ns.collapse {
            base["collapse"] = collapse.clone();
        }
        // Pin the snapshot: rows written after page one are not in the walk.
        // Rows with no stamp (written around the gateway) stay visible.
        let pinned = schema.stamped.then(|| {
            json!([
                "Or",
                [
                    [crate::clients::turbopuffer::UPSERTED_AT_ATTR, "Lte", as_of],
                    [crate::clients::turbopuffer::UPSERTED_AT_ATTR, "Eq", null]
                ]
            ])
        });
        match (filters, pinned) {
            (Some(filters), Some(pinned)) => base["filters"] = json!(["And", [filters, pinned]]),
            (Some(filter), None) | (None, Some(filter)) => base["filters"] = filter,
            (None, None) => {}
        }
        let mut body = self.query_body(ns, &headers, base).await?;
        let key = if ns.collapse.is_some() {
            "groups"
        } else {
            "rows"
        };
        let mut ranked = body
            .get_mut(key)
            .and_then(Value::as_array_mut)
            .map(std::mem::take)
            .unwrap_or_default();
        // A page body may be image-only while its title names the drawing.
        // Read labels only for a fixed bounded prefix, on every cursor page.
        if !schema.labels.is_empty() {
            let count = ranked.len().min(LABEL_WINDOW);
            self.fill_selected_attributes(ns, &headers, &mut ranked[..count], json!(schema.labels))
                .await?;
            rank_mcp_labels(
                &mut ranked[..count],
                &schema.labels,
                &query,
                ns.collapse.is_some(),
            );
        }
        let (total, exact) = match known_total {
            Some(total) => (total.count, total.exact),
            None => (ranked.len() as u64, (ranked.len() as u64) < MAX_WINDOW),
        };
        let start = (offset as usize).min(ranked.len());
        let mut page: Vec<Value> =
            ranked[start..(start + take as usize).min(ranked.len())].to_vec();
        self.fill_attributes(ns, &headers, &mut page).await?;
        let next = offset + page.len() as u64;
        let next_cursor = (!page.is_empty() && next < total).then(|| {
            PageCursor {
                v: 1,
                q: fingerprint,
                as_of,
                offset: next,
                total: PageTotal {
                    count: total,
                    exact,
                },
            }
            .encode()
        });
        let returned = page.len();
        body[key] = Value::Array(page);
        body["total"] = json!(total);
        body["total_exact"] = json!(exact);
        body["next_cursor"] = json!(next_cursor);
        body["offset"] = json!(offset);
        body["snapshot"] = json!(schema.stamped);
        body["links"] = json!(result_links(&body, ns.link.as_deref(), ns.page_link));
        let applied_filters = requested_filters.unwrap_or_else(|| json!({}));
        body["applied_filters"] = applied_filters.clone();
        let mut text = render(&body, ns.link.as_deref(), ns.page_link);
        if applied_filters.as_object().is_some_and(Map::is_empty) {
            text.push_str("\nRequested attribute filters: none. Query text does not constrain attribute values; use typed filters for a project-specific request.");
        } else {
            text.push_str(&format!(
                "\nRequested attribute filters: {applied_filters}."
            ));
        }
        text.push_str(&format!(
            "\nShowing {}-{} of {}{}.",
            offset + 1.min(returned as u64),
            offset + returned as u64,
            total,
            if exact { "" } else { "+" }
        ));
        match &next_cursor {
            Some(cursor) => text.push_str(&format!(" next_cursor: {cursor}")),
            None => text.push_str(" No further pages."),
        }
        let mut result = CallToolResult::structured(body);
        result.content = vec![ContentBlock::text(text)];
        Ok(result)
    }
}

/// Add bounded title evidence to the store's relevance. Stable ties keep
/// the original order; a label match cannot replace the retrieval signal.
fn rank_mcp_labels(rows: &mut [Value], labels: &[String], query: &str, collapsed: bool) {
    let words = crate::routes::hybrid_text::tokenize_query_input(query).tokens;
    let strongest = rows
        .iter()
        .map(|item| {
            let row = if collapsed { &item["best"] } else { item };
            mcp_store_relevance(row)
        })
        .fold(0.0, f64::max);
    for item in rows.iter_mut() {
        let row = if collapsed { &mut item["best"] } else { item };
        let title = labels
            .iter()
            .filter_map(|label| row[label].as_str())
            .collect::<Vec<_>>()
            .join(" ");
        let title: String = title
            .chars()
            .map(|c| if c.is_alphanumeric() { c } else { ' ' })
            .collect();
        let terms = crate::routes::hybrid_text::tokenize_query_input(&title).tokens;
        let mut matched = vec![false; words.len()];
        for (i, word) in words.iter().enumerate() {
            if terms.contains(word) {
                matched[i] = true;
            }
            if i + 1 < words.len() && terms.contains(&format!("{}{}", word, words[i + 1])) {
                matched[i] = true;
                matched[i + 1] = true;
            }
        }
        let coverage = matched.iter().filter(|v| **v).count() as f64 / words.len().max(1) as f64;
        let relevance = if strongest > 0.0 {
            mcp_store_relevance(row) / strongest
        } else {
            0.0
        };
        row["$mcp_score"] = json!(relevance + 0.25 * coverage);
    }
    rows.sort_by(|a, b| {
        let score = |item: &Value| {
            let row = if collapsed { &item["best"] } else { item };
            row["$mcp_score"].as_f64().unwrap_or_default()
        };
        score(b).total_cmp(&score(a))
    });
}

fn mcp_store_relevance(row: &Value) -> f64 {
    if let Some(score) = row["$score"].as_f64().filter(|v| v.is_finite()) {
        score.max(0.0)
    } else if let Some(distance) = row["$dist"].as_f64().filter(|v| v.is_finite()) {
        1.0 / (1.0 + distance.max(0.0))
    } else {
        0.0
    }
}

/// One requested aggregate: its result name and what it computes.
enum AggregateFn {
    Count,
    Sum(String),
    Min(String),
    Max(String),
}

impl AggregateFn {
    fn name(&self) -> String {
        match self {
            Self::Count => "count".into(),
            Self::Sum(field) => format!("sum_{field}"),
            Self::Min(field) => format!("min_{field}"),
            Self::Max(field) => format!("max_{field}"),
        }
    }
}

fn unsupported(message: impl Into<String>, details: Value) -> ToolFailure {
    ToolFailure::typed("unsupported_aggregate", message, details)
}

/// Store bytes billed by one aggregate call, summed over its queries.
#[derive(Default)]
struct BilledBytes {
    queries: u64,
    queried: u64,
    returned: u64,
}

impl BilledBytes {
    fn add(&mut self, body: &Value) {
        self.queries += 1;
        let bytes = |key: &str| {
            body.pointer(&format!("/billing/{key}"))
                .and_then(Value::as_u64)
                .unwrap_or(0)
        };
        self.queried += bytes("billable_logical_bytes_queried");
        self.returned += bytes("billable_logical_bytes_returned");
    }

    fn json(&self) -> Value {
        json!({"queries":self.queries,"billable_logical_bytes_queried":self.queried,
            "billable_logical_bytes_returned":self.returned})
    }
}

impl McpHandler {
    /// Parse `aggregates` against the namespace's summable and orderable fields.
    fn aggregate_functions(
        schema: &NamespaceSchema,
        value: Option<Value>,
        grouped: bool,
    ) -> Result<Vec<AggregateFn>, ToolFailure> {
        let Some(value) = value else {
            return Ok(vec![AggregateFn::Count]);
        };
        let items = value
            .as_array()
            .filter(|items| (1..=MAX_AGGREGATES).contains(&items.len()))
            .ok_or_else(|| {
                invalid(format!(
                    "aggregates must be a list of 1 to {MAX_AGGREGATES} objects like {{\"fn\":\"count\"}}"
                ))
            })?;
        let mut functions: Vec<AggregateFn> = Vec::new();
        for item in items {
            let object = item
                .as_object()
                .ok_or_else(|| invalid("each aggregate must be an object with `fn`"))?;
            check_arguments("aggregates item", object, &["fn", "field"])?;
            let name = object.get("fn").and_then(Value::as_str).unwrap_or_default();
            let field = object.get("field").map(|f| {
                f.as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| invalid("aggregate field must be a field name"))
            });
            let field = field.transpose()?;
            let needs = |keep: fn(&FilterType) -> bool,
                         what: &str|
             -> Result<String, ToolFailure> {
                let field = field
                    .clone()
                    .ok_or_else(|| invalid(format!("{name} needs a `field`")))?;
                if schema.filters.get(&field).is_some_and(keep) {
                    Ok(field)
                } else {
                    Err(ToolFailure::typed(
                        "invalid_argument",
                        format!("`{field}` is not a {what} field of this namespace. Call list_namespaces for the fields."),
                        json!({"field":field}),
                    ))
                }
            };
            let function = match name {
                "count" if field.is_none() => AggregateFn::Count,
                "count" => return Err(invalid("count takes no `field`").into()),
                "sum" => AggregateFn::Sum(needs(FilterType::summable, "summable")?),
                "min" | "max" => {
                    if grouped {
                        return Err(unsupported(
                            "min and max are not available with group_by: the store aggregates only count and sum per group. Filter to the group and call again without group_by.",
                            json!({"fn":name}),
                        ));
                    }
                    let field = needs(FilterType::extrema, "numeric or date")?;
                    if name == "min" {
                        AggregateFn::Min(field)
                    } else {
                        AggregateFn::Max(field)
                    }
                }
                _ => return Err(invalid("aggregate fn must be count, sum, min or max").into()),
            };
            if functions.iter().any(|f| f.name() == function.name()) {
                return Err(invalid(format!("duplicate aggregate {}", function.name())).into());
            }
            functions.push(function);
        }
        let extrema = functions
            .iter()
            .filter(|f| matches!(f, AggregateFn::Min(_) | AggregateFn::Max(_)))
            .count();
        if extrema > MAX_EXTREMA {
            return Err(unsupported(
                format!("at most {MAX_EXTREMA} of min and max per call: each is another billed store query"),
                json!({"max":MAX_EXTREMA}),
            ));
        }
        Ok(functions)
    }

    /// Count, sum, min and max over the rows a filter matches, optionally
    /// grouped. Count and sum share one native aggregate query; each min or
    /// max is one ordered one-row query. Every query is billed at the
    /// namespace's size, so the call is bounded to three and reports the bytes.
    async fn aggregate(
        &self,
        ns: &McpNamespace,
        headers: HeaderMap,
        mut arguments: Map<String, Value>,
    ) -> Result<CallToolResult, ToolFailure> {
        let schema = self.schema(ns).await?;
        let group_by: Vec<String> = match arguments.remove("group_by") {
            None => Vec::new(),
            Some(value) => serde_json::from_value(value)
                .ok()
                .filter(|fields: &Vec<String>| fields.len() <= MAX_GROUP_BY)
                .ok_or_else(|| {
                    invalid(format!(
                        "group_by must be a list of at most {MAX_GROUP_BY} field names"
                    ))
                })?,
        };
        for (index, field) in group_by.iter().enumerate() {
            if group_by[..index].contains(field) {
                return Err(invalid(format!("group_by repeats `{field}`")).into());
            }
            if !schema.filters.get(field).is_some_and(FilterType::groupable) {
                return Err(ToolFailure::typed(
                    "invalid_argument",
                    format!(
                        "`{field}` cannot be grouped by. Groupable fields: {}.",
                        schema.field_names(FilterType::groupable)
                    ),
                    json!({"field":field}),
                ));
            }
        }
        let limit = match arguments.remove("limit") {
            None => DEFAULT_GROUPS,
            Some(v) => v
                .as_u64()
                .filter(|n| (1..=MAX_GROUPS).contains(n))
                .ok_or_else(|| {
                    invalid(format!("limit must be an integer from 1 to {MAX_GROUPS}"))
                })?,
        };
        let functions = Self::aggregate_functions(
            &schema,
            arguments.remove("aggregates"),
            !group_by.is_empty(),
        )?;
        let filters = schema.filters(arguments.remove("filters"))?;

        let mut billed = BilledBytes::default();
        let native: Map<String, Value> = functions
            .iter()
            .filter_map(|f| match f {
                AggregateFn::Count => Some((f.name(), json!(["Count"]))),
                AggregateFn::Sum(field) => Some((f.name(), json!(["Sum", field]))),
                _ => None,
            })
            .collect();
        let mut groups: Vec<Value> = vec![json!({})];
        let mut truncated = false;
        if !native.is_empty() {
            let mut body = json!({"aggregate_by":native});
            if let Some(filters) = &filters {
                body["filters"] = filters.clone();
            }
            if !group_by.is_empty() {
                body["group_by"] = json!(group_by);
                // One past the limit shows whether the store cut the groups off.
                body["top_k"] = json!(limit + 1);
            }
            let response = self.query_body(ns, &headers, body).await?;
            billed.add(&response);
            if group_by.is_empty() {
                groups = vec![response
                    .get("aggregations")
                    .filter(|v| v.is_object())
                    .cloned()
                    .ok_or_else(|| invalid("the store returned no aggregations"))?];
            } else {
                groups = response
                    .get("aggregation_groups")
                    .and_then(Value::as_array)
                    .cloned()
                    .ok_or_else(|| invalid("the store returned no aggregation groups"))?;
                truncated = groups.len() as u64 > limit;
                groups.truncate(limit as usize);
            }
        }
        for function in &functions {
            let (field, direction) = match function {
                AggregateFn::Min(field) => (field, "asc"),
                AggregateFn::Max(field) => (field, "desc"),
                _ => continue,
            };
            let present = json!([field, "NotEq", null]);
            let mut body = json!({"rank_by":[field,direction],"top_k":1,"include_attributes":[field],
                "filters":filters.as_ref().map_or(present.clone(), |f| json!(["And", [f, present]]))});
            body["consistency"] = json!({"level":"strong"});
            let response = self.query_body(ns, &headers, body).await?;
            billed.add(&response);
            groups[0][function.name()] = response
                .pointer("/rows/0")
                .and_then(|row| row.get(field))
                .cloned()
                .unwrap_or(Value::Null);
        }

        let mut text = if group_by.is_empty() {
            "Totals".to_string()
        } else {
            format!("{} group(s) by {}", groups.len(), group_by.join(", "))
        };
        for group in groups.iter().take(100) {
            let cell = |name: &str| match &group[name] {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            let key = group_by
                .iter()
                .map(|f| cell(f))
                .collect::<Vec<_>>()
                .join(" | ");
            let values = functions
                .iter()
                .map(|f| format!("{}={}", f.name(), cell(&f.name())))
                .collect::<Vec<_>>()
                .join(", ");
            text.push_str(&format!(
                "\n{}{values}",
                if key.is_empty() {
                    String::new()
                } else {
                    format!("{key}: ")
                }
            ));
        }
        if truncated {
            text.push_str(&format!(
                "\nTruncated at {limit} groups, in key order: add filters or raise limit."
            ));
        }
        text.push_str(&format!(
            "\nBilled by the store: {:.2} GB queried over {} quer{}.",
            billed.queried as f64 / 1e9,
            billed.queries,
            if billed.queries == 1 { "y" } else { "ies" }
        ));
        tracing::info!(
            server = %self.name,
            namespace = %ns.name,
            queries = billed.queries,
            billable_logical_bytes_queried = billed.queried,
            billable_logical_bytes_returned = billed.returned,
            "MCP aggregate billed"
        );
        let mut result = CallToolResult::structured(json!({
            "namespace":ns.name,"group_by":group_by,"groups":groups,"truncated":truncated,
            "limit":limit,"counts":if ns.collapse.is_some() { "rows" } else { "records" },
            "billing":billed.json()
        }));
        result.content = vec![ContentBlock::text(text)];
        Ok(result)
    }
}

async fn decode_response(response: Response) -> Result<Value, AppError> {
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 16 * 1024 * 1024)
        .await
        .map_err(|e| AppError::ServiceUnavailable(e.to_string()))?;
    let body: Value =
        serde_json::from_slice(&bytes).map_err(|e| AppError::ServiceUnavailable(e.to_string()))?;
    if !status.is_success() {
        return Err(AppError::ServiceUnavailable(format!(
            "query failed ({status}): {body}"
        )));
    }
    Ok(body)
}

#[derive(Clone, Copy)]
enum ToolKind {
    Search,
    Get,
    Aggregate,
    Original,
}

/// Most fields one aggregate groups by, as the store allows.
const MAX_GROUP_BY: usize = 2;
const MAX_AGGREGATES: usize = 4;
/// Min and max are ordered one-row queries, each billed like any query.
const MAX_EXTREMA: usize = 2;
const DEFAULT_GROUPS: u64 = 100;
const MAX_GROUPS: u64 = 1000;
const AGGREGATE_COST: &str = "The store bills each query at the namespace's size (about 2 GB for the BCC pages), whatever the filters; a call makes at most three queries and returns the billed bytes.";

/// Deepest rank a cursor can reach, and the most matches counted.
const MAX_WINDOW: u64 = 1000;
const LABEL_WINDOW: usize = 50;

#[derive(serde::Serialize, Deserialize)]
struct PageTotal {
    count: u64,
    exact: bool,
}

/// The opaque `next_cursor`: which query it belongs to, the snapshot it reads
/// and where the next page starts.
#[derive(serde::Serialize, Deserialize)]
struct PageCursor {
    v: u8,
    q: String,
    as_of: u64,
    offset: u64,
    total: PageTotal,
}

impl PageCursor {
    fn encode(&self) -> String {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
        use base64::Engine;
        B64.encode(serde_json::to_vec(self).expect("cursor is JSON-encodable"))
    }

    fn decode(raw: &str, fingerprint: &str) -> Result<Self, ToolFailure> {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
        use base64::Engine;
        let bad = |why: &str| {
            ToolFailure::typed(
                "invalid_cursor",
                format!("cursor is not a next_cursor from this search: {why}. Repeat the search without a cursor for page one."),
                json!({"reason":why}),
            )
        };
        let cursor: Self = B64
            .decode(raw)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .ok_or_else(|| bad("it does not decode"))?;
        if cursor.v != 1 {
            return Err(bad("unsupported cursor version"));
        }
        if cursor.q != fingerprint {
            return Err(ToolFailure::typed(
                "cursor_mismatch",
                "cursor was issued for a different namespace, query or filters; send the same query and filters with it, or omit the cursor to start over",
                json!({}),
            ));
        }
        if cursor.offset == 0 || cursor.offset >= MAX_WINDOW || cursor.total.count > MAX_WINDOW {
            return Err(bad("its position is out of range"));
        }
        Ok(cursor)
    }
}

fn pinned_out(id: &str) -> AppError {
    AppError::NotFound(format!(
        "record `{id}` was not found or is no longer available"
    ))
}

/// Identifies the search a cursor continues: namespace, ranking, filters and
/// grouping. `limit` is left out, so page size can change between pages.
fn page_fingerprint(ns: &McpNamespace, rank_by: &Value, filters: Option<&Value>) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(
        json!([ns.name, rank_by, filters, ns.collapse])
            .to_string()
            .as_bytes(),
    );
    digest[..8].iter().map(|b| format!("{b:02x}")).collect()
}

impl McpHandler {
    /// The authorized read of the record, then the bytes its own row binds to.
    /// Auth order: MCP entitlement (handle), namespace read grant (caller),
    /// record read through the normal fetch path, reference binding, hash check.
    async fn fetch_original(
        &self,
        ns: &McpNamespace,
        mut arguments: Map<String, Value>,
    ) -> Result<CallToolResult, AppError> {
        use crate::mcp_original as original;
        let config = ns
            .originals
            .as_ref()
            .ok_or_else(|| invalid("originals are not enabled for this namespace"))?;
        let id = match arguments.remove("id") {
            Some(Value::String(id)) if !id.is_empty() => id,
            Some(Value::Number(id)) if id.as_u64().is_some() => id.to_string(),
            _ => return Err(invalid("id must be a nonempty string or unsigned integer")),
        };
        let pages = original::parse_pages(arguments.remove("pages"), config.max_pages())?;
        if !arguments.is_empty() {
            return Err(invalid("unknown fetch_original argument"));
        }
        let mut headers = self.headers.clone();
        headers.append(
            "x-hevlayer-tag",
            format!("mcp.{}", self.name)
                .parse()
                .map_err(|_| invalid("invalid MCP history tag"))?,
        );
        let body = if id.starts_with(layer_original::catalog::PREFIX) {
            let body = original::catalog_record(&self.state, &ns.name, &id)
                .await
                .map_err(|error| {
                    if ns.pin().is_some() && matches!(&error, AppError::NotFound(_)) {
                        pinned_out(&id)
                    } else {
                        error
                    }
                })?;
            let attrs = body.get("attributes").unwrap_or(&body);
            // Catalog records have no search row to prove a pinned predicate.
            // Every pinned value must be present and equal, otherwise fail closed.
            if ns
                .pinned
                .iter()
                .flatten()
                .any(|(name, want)| attrs.get(name) != Some(want))
            {
                return Err(pinned_out(&id));
            }
            body
        } else {
            self.require_in_pin(ns, &headers, &id).await?;
            let body = self.current_original_record(ns, &id, &headers).await?;
            self.check_pin(ns, &id, body.get("attributes").unwrap_or(&body))?;
            original::catalog_fallback(&self.state, &ns.name, body).await?
        };
        let attrs = body.get("attributes").unwrap_or(&body);
        let source = ns
            .link
            .as_deref()
            .and_then(|field| attrs.get(field))
            .and_then(Value::as_str);
        let with_source = |error| match (error, source) {
            (AppError::NotFound(message), Some(url)) => {
                AppError::NotFound(format!("{message}\nSource link: {url}"))
            }
            (AppError::PayloadTooLarge(message), Some(url)) => {
                AppError::PayloadTooLarge(format!("{message}\nSource link: {url}"))
            }
            (error, _) => error,
        };
        let bound = original::bind(&ns.name, attrs).map_err(with_source)?;
        let is_pdf = bound.mime == "application/pdf";
        let hard_limit = if pages.is_some() {
            10 * 1024 * 1024
        } else {
            config.max_bytes()
        };
        if pages.is_some() && !is_pdf {
            return Err(invalid("page images are only available for PDF originals"));
        }
        if attrs
            .get("page_basis")
            .and_then(Value::as_str)
            .is_some_and(|b| b != "original_pdf")
        {
            return Err(invalid("this record's pages come from a converted copy; page images of the original are not available"));
        }
        let file = original::load(&self.state, &ns.name, &bound, hard_limit)
            .await
            .map_err(with_source)?;
        let name = attrs
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or(if is_pdf { "original.pdf" } else { "original" });
        let page_number = attrs.get("page").and_then(Value::as_u64);
        let mut text = format!(
            "Original file `{name}` ({} bytes, sha256 {})",
            file.len(),
            bound.sha256
        );
        if let Some(url) = source {
            text.push_str(&format!("\nSource link: {url}"));
            if let (true, Some(page), false) =
                (ns.page_link && is_pdf, page_number, url.contains('#'))
            {
                text.push_str(&format!("\nPage {page} link: {url}#page={page}"));
            }
        }
        if attrs
            .get("original_source_format_mismatch")
            .and_then(Value::as_bool)
            == Some(true)
        {
            text.push_str(&format!(
                "\nSource format mismatch: actual {}, provider declared {}.",
                bound.mime,
                attrs
                    .get("original_declared_mime")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
            ));
        }
        let mut structured = json!({
            "id": id, "namespace": ns.name, "name": name, "mimeType": bound.mime,
            "bytes": file.len(), "sha256": bound.sha256, "version": bound.version,
            "sourceLink": source,
            "declaredMimeType": attrs.get("original_declared_mime"),
            "sourceFormatMismatch": attrs.get("original_source_format_mismatch"),
            "sourceIdentity": {"drive":attrs.get("original_drive"),"item":attrs.get("original_item"),"version":attrs.get("original_version"),"etag":attrs.get("original_etag")},
            "pageLink": match (ns.page_link && is_pdf, source, page_number) {
                (true, Some(url), Some(page)) if !url.contains('#') => json!(format!("{url}#page={page}")),
                _ => Value::Null,
            },
        });
        let mut content = Vec::new();
        match pages {
            None => {
                text.push_str("\nThe original is attached as an embedded resource.");
                if is_pdf {
                    text.push_str(" If your host does not show it, call again with `pages`.");
                }
                content.push(ContentBlock::text(text));
                content.push(ContentBlock::resource(
                    ResourceContents::blob(
                        original::b64(&file),
                        format!("layer://{}/original/{}", ns.name, bound.sha256),
                    )
                    .with_mime_type(bound.mime.clone()),
                ));
            }
            Some(pages) => {
                let outcome = original::render_pages(file, pages, config.max_width()).await?;
                text.push_str(&format!(
                    "\nPage images of the original ({} pages total): {}",
                    outcome.page_count,
                    outcome
                        .images
                        .iter()
                        .map(|i| i.page.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
                if !outcome.omitted.is_empty() {
                    text.push_str(&format!(
                        "\nOmitted to stay within the response limit; request them separately: {}",
                        outcome
                            .omitted
                            .iter()
                            .map(u32::to_string)
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
                structured["pageCount"] = json!(outcome.page_count);
                structured["pages"] =
                    json!(outcome.images.iter().map(|i| i.page).collect::<Vec<_>>());
                structured["omittedPages"] = json!(outcome.omitted);
                content.push(ContentBlock::text(text));
                for image in outcome.images {
                    content.push(ContentBlock::text(format!("Page {}", image.page)));
                    content.push(ContentBlock::image(original::b64(&image.png), "image/png"));
                }
            }
        }
        let mut result = CallToolResult::structured(structured);
        result.content = content;
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
    /// A `[]string` attribute; a filter value matches rows whose list contains it.
    StringList,
}

struct NamespaceSchema {
    row_count: u64,
    filters: BTreeMap<String, FilterType>,
    text: Option<String>,
    fuzzy: bool,
    embed: Option<String>,
    labels: Vec<String>,
    /// Rows carry the gateway's write stamp, so a page walk can pin a snapshot.
    stamped: bool,
}

impl NamespaceSchema {
    fn from_metadata(ns: &McpNamespace, metadata: &Value) -> Result<Self, AppError> {
        let attrs = metadata
            .get("schema")
            .and_then(Value::as_object)
            .ok_or_else(|| invalid("namespace has no attribute schema"))?;
        let mut filters = BTreeMap::new();
        let mut all_filterable = BTreeMap::new();
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
                Some("[]string" | "[]uuid") => FilterType::StringList,
                Some("bool" | "boolean") => FilterType::Boolean,
                Some("int" | "uint" | "int64" | "uint64") => FilterType::Integer,
                Some("float" | "float64" | "number") => FilterType::Number,
                Some("datetime" | "date") => FilterType::Date,
                _ => continue,
            };
            all_filterable.insert(name.clone(), ty.clone());
            if ns
                .filters
                .as_ref()
                .is_none_or(|allowed| allowed.contains(name))
            {
                filters.insert(name.clone(), ty);
            }
        }
        if let Some(pinned) = &ns.pinned {
            for (name, value) in pinned {
                let ok = match all_filterable.get(name) {
                    Some(FilterType::String) => value.is_string(),
                    Some(FilterType::Boolean) => value.is_boolean(),
                    Some(FilterType::Integer) => value.is_i64() || value.is_u64(),
                    Some(FilterType::Number) => value.is_number(),
                    _ => false,
                };
                if !ok {
                    return Err(invalid(format!("MCP where `{name}` is missing, not filterable, or its value does not match the attribute type")));
                }
                // The client sees only what it can still choose.
                filters.remove(name);
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
            labels: ["title", "name", "filename"]
                .into_iter()
                .filter(|name| {
                    attrs
                        .get(*name)
                        .and_then(|v| v.get("type"))
                        .and_then(Value::as_str)
                        == Some("string")
                })
                .map(str::to_owned)
                .collect(),
            stamped: attrs.contains_key(crate::clients::turbopuffer::UPSERTED_AT_ATTR),
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
                // Sparse extracted text can miss a short lexical query even
                // when its page embedding matches. Conversely, long questions
                // still need exact lexical names. Keep both retrieval paths.
                let mut options = json!({"vector":["Embed",query,{"field":embed}],"route":"fused"});
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
        let mut search_description = description.clone();
        search_description.push(' ');
        search_description.push_str(SEARCH_SCOPE_GUIDANCE);
        if ns.collapse.is_some() {
            search_description.push_str(" Returns distinct groups with one best matching row per group; limit counts groups. Use get with the best row id for details.");
        }
        search_description.push_str(" Returns total and next_cursor; pass next_cursor as cursor, with the same query and filters, for the next page.");
        let search_schema = json!({"type":"object","properties":{
            "query":{"type":"string","minLength":1,"description":SEARCH_QUERY_DESCRIPTION},
            "filters":{"type":"object","properties":properties.clone(),"additionalProperties":false},
            "limit":{"type":"integer","minimum":1,"maximum":50,"default":10},
            "cursor":{"type":"string","minLength":1,"description":"The next_cursor from the previous page of the same query. Omit for the first page."}
        },"required":["query"],"additionalProperties":false});
        let get_schema = json!({"type":"object","properties":{"id":{"anyOf":[{"type":"string","minLength":1},{"type":"integer","minimum":0}]}},"required":["id"],"additionalProperties":false});
        let aggregate_description = format!(
            "Count rows, or sum/min/max a numeric field, optionally grouped by one or two fields, with the same filters as search. {description} Groupable: {}. Summable: {}.{} {AGGREGATE_COST}",
            self.field_names(FilterType::groupable),
            self.field_names(FilterType::summable),
            if ns.collapse.is_some() { " Counts rows (pages), not collapsed documents." } else { "" },
        );
        let aggregate_schema = json!({"type":"object","properties":{
            "group_by":aggregate_group_by_schema(),
            "aggregates":aggregate_aggregates_schema(),
            "filters":{"type":"object","properties":properties,"additionalProperties":false},
            "limit":aggregate_limit_schema()
        },"additionalProperties":false});
        vec![
            Tool::new(
                format!("search_{}", ns.tool_name()),
                format!("Search. {search_description}"),
                search_schema.as_object().unwrap().clone(),
            )
            .with_annotations(ToolAnnotations::new().read_only(true)),
            Tool::new(
                format!("get_{}", ns.tool_name()),
                format!("Fetch one record by id. {description}"),
                get_schema.as_object().unwrap().clone(),
            )
            .with_annotations(ToolAnnotations::new().read_only(true)),
            Tool::new(
                format!("aggregate_{}", ns.tool_name()),
                aggregate_description,
                aggregate_schema.as_object().unwrap().clone(),
            )
            .with_annotations(ToolAnnotations::new().read_only(true)),
        ]
    }

    fn field_names(&self, keep: fn(&FilterType) -> bool) -> String {
        let names: Vec<&str> = self
            .filters
            .iter()
            .filter(|(_, ty)| keep(ty))
            .map(|(name, _)| name.as_str())
            .collect();
        if names.is_empty() {
            "none".into()
        } else {
            names.join(", ")
        }
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
                FilterType::StringList if value.is_string() => {
                    predicates.push(json!([name, "Contains", value]))
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
    /// Fields `group_by` takes. Floats and timestamps make one group per row.
    fn groupable(&self) -> bool {
        matches!(self, Self::String | Self::Boolean | Self::Integer)
    }

    fn summable(&self) -> bool {
        matches!(self, Self::Integer | Self::Number)
    }

    /// Fields min and max can order.
    fn extrema(&self) -> bool {
        matches!(self, Self::Integer | Self::Number | Self::Date)
    }

    fn name(&self) -> &'static str {
        match self {
            Self::String => "string",
            Self::StringList => "string list",
            Self::Boolean => "boolean",
            Self::Number => "number",
            Self::Integer => "integer",
            Self::Date => "datetime",
        }
    }

    fn schema(&self) -> Value {
        match self {
            Self::String | Self::StringList => json!({"type":"string"}),
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

/// Keep gateway ordering and identities, without repeating best rows or sending
/// every matching page to the model. The gateway owns grouping and overfetch.
fn compact_groups(body: Value) -> Result<Value, AppError> {
    let groups = body
        .get("groups")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("collapsed MCP search requires gateway groups"))?;
    let mut compact = Vec::with_capacity(groups.len());
    for group in groups {
        let best = group
            .get("rows")
            .and_then(Value::as_array)
            .and_then(|rows| rows.first())
            .ok_or_else(|| invalid("collapsed MCP group has no best row"))?;
        compact.push(json!({"key":group["key"],"by":group["by"],"best":best}));
    }
    Ok(json!({"groups":compact,"collapse":body["collapse"]}))
}

/// Where else the same file lives, from a row's `locations` attribute (a JSON
/// list written when a document has several copies): each copy's source and its
/// link, `webUrl` for a drive copy and `rest.url` for a REST one. None unless
/// there is more than one copy.
fn other_copies(attrs: &Value) -> Option<String> {
    let locations: Vec<Value> = serde_json::from_str(attrs.get("locations")?.as_str()?).ok()?;
    if locations.len() < 2 {
        return None;
    }
    let links: Vec<String> = locations
        .iter()
        .filter_map(|location| {
            let (source, url) = match location.get("rest") {
                Some(rest) => ("rest", rest.get("url")?.as_str()?),
                None => ("clouddrive", location.get("webUrl")?.as_str()?),
            };
            Some(format!("{source} {url}"))
        })
        .collect();
    (links.len() > 1).then(|| links.join("; "))
}

fn result_hits(body: &Value) -> Vec<&Value> {
    let rows = body.get("rows").and_then(Value::as_array);
    if let Some(groups) = body
        .get("groups")
        .and_then(Value::as_array)
        .filter(|_| body.get("id").is_none())
    {
        groups
            .iter()
            .filter_map(|group| group.get("best"))
            .collect()
    } else {
        rows.map(|rows| rows.iter().collect())
            .unwrap_or_else(|| vec![body])
    }
}

fn pdf_page_link(attrs: &Value, url: &str, enabled: bool) -> Option<String> {
    if !enabled || url.contains('#') {
        return None;
    }
    let page = attrs.get("page")?.as_u64().filter(|page| *page > 0)?;
    let mime = attrs.get("original_mime").and_then(Value::as_str);
    let basis = attrs.get("page_basis").and_then(Value::as_str);
    if mime.is_some_and(|mime| mime != "application/pdf")
        || basis.is_some_and(|basis| basis != "original_pdf")
    {
        return None;
    }
    // Legacy rows may lack the source MIME/basis. Only a PDF URL is evidence
    // in that case; a page number alone can come from an Office conversion.
    if mime.is_none()
        && basis.is_none()
        && !url
            .split('?')
            .next()
            .unwrap_or(url)
            .to_ascii_lowercase()
            .ends_with(".pdf")
    {
        return None;
    }
    Some(format!("{url}#page={page}"))
}

fn result_links(body: &Value, link: Option<&str>, page_link: bool) -> Vec<Value> {
    result_hits(body)
        .into_iter()
        .filter_map(|hit| {
            let attrs = hit.get("attributes").unwrap_or(hit);
            let url = attrs.get(link?)?.as_str()?;
            Some(json!({
                "id":hit.get("id").unwrap_or(&Value::Null),
                "sourceLink":url,
                "pageLink":pdf_page_link(attrs, url, page_link)
            }))
        })
        .collect()
}

fn render(body: &Value, link: Option<&str>, page_link: bool) -> String {
    let hits = result_hits(body);
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
            if let Some(page_url) = pdf_page_link(attrs, url, page_link) {
                let page = attrs["page"].as_u64().unwrap();
                text.push_str(&format!(" (page {page}: {page_url})"));
            }
        }
        if let Some(copies) = other_copies(attrs) {
            text.push_str(&format!(" [also: {copies}]"));
        }
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bcc_values_seed_is_valid_and_enables_originals_on_the_pdf_namespaces() {
        let values: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../infra/helm/layer/values-mcp-bcc.yaml"
        )))
        .unwrap();
        let servers = &values["gateway"]["mcp"]["servers"];
        let registry = registry_from_json(Some(&servers.to_string())).unwrap();
        let bcc = &registry.servers["bcc"];
        for name in [
            "pov-bcc-pages",
            "pov-bcc-story-pages",
            "pov-bcc-invoice-docs",
        ] {
            let ns = bcc.namespaces.iter().find(|n| n.name == name).unwrap();
            let originals = ns.originals.as_ref().expect(name);
            assert_eq!(originals.max_bytes(), 700_000);
            assert_eq!(originals.max_pages(), 3);
        }
        assert!(
            bcc.namespaces
                .iter()
                .filter(|n| n.originals.is_some())
                .count()
                == 3
        );
        let story = &registry.servers["story"];
        assert_eq!(story.namespaces.len(), 2);
        assert!(story.namespaces.len() <= PER_NAMESPACE_TOOL_THRESHOLD);
        let photos = story
            .namespaces
            .iter()
            .find(|n| n.name == "pov-bcc-photos")
            .unwrap();
        assert_eq!(photos.pin(), Some(json!(["company", "Eq", "Story"])));
        assert!(photos
            .filters
            .as_ref()
            .is_some_and(|f| !f.iter().any(|f| f == "company")));
    }

    fn ns() -> McpNamespace {
        McpNamespace {
            name: "docs".into(),
            tool_name: None,
            description: None,
            filters: None,
            pinned: None,
            link: None,
            page_link: false,
            originals: None,
            collapse: None,
        }
    }

    #[test]
    fn compact_groups_preserves_order_best_row_and_window_metadata() {
        let best =
            json!({"id":"p2","text":"best","page":2,"$score":0.9,"url":"https://example.com/a"});
        let body = compact_groups(json!({"rows":[best],"groups":[
            {"key":"a","by":"document","rows":[best,{"id":"p3","text":"continuation"}]}
        ],"collapse":{"read":20,"exhausted":false}}))
        .unwrap();
        assert_eq!(body["groups"][0]["best"], best);
        assert!(body.get("rows").is_none());
        assert!(body["groups"][0].get("rows").is_none());
        assert_eq!(body["collapse"]["read"], 20);
        assert!(render(&body, Some("url"), false).contains("https://example.com/a"));
        assert!(compact_groups(json!({"rows":[]})).is_err());
        assert!(compact_groups(json!({"groups":[{"rows":[]}]})).is_err());
        assert_eq!(
            render(
                &compact_groups(json!({"groups":[],"collapse":{"read":0,"exhausted":true}}))
                    .unwrap(),
                None,
                false
            ),
            "0 record(s)"
        );
    }

    #[test]
    fn get_record_with_groups_attribute_keeps_its_excerpt_and_link() {
        let record =
            json!({"id":"p1","text":"record excerpt","url":"https://example.com/p1", "groups":[]});
        let rendered = render(&record, Some("url"), false);
        assert!(rendered.starts_with("1 record(s)"));
        assert!(rendered.contains("record excerpt"));
        assert!(rendered.contains("https://example.com/p1"));
    }

    #[test]
    fn collapsed_search_description_does_not_change_get_contract() {
        let mut namespace = ns();
        namespace.collapse = Some(json!({"by":"document"}));
        let schema = NamespaceSchema::from_metadata(
            &namespace,
            &json!({"schema":{
                "text":{"type":"string","full_text_search":true}
            }}),
        )
        .unwrap();
        let tools = schema.tools(&namespace);
        assert!(tools[0]
            .description
            .as_ref()
            .unwrap()
            .contains("distinct groups"));
        assert!(!tools[1]
            .description
            .as_ref()
            .unwrap()
            .contains("distinct groups"));
    }

    #[test]
    fn a_document_with_several_copies_lists_every_link() {
        let locations = json!([
            {"source":"clouddrive","webUrl":"https://example.test/a.pdf"},
            {"source":"rest","rest":{"url":"rest://ki/17.pdf","rowId":"17"}}
        ])
        .to_string();
        let body = json!({"id":"doc#p1","attributes":{"text":"t","webUrl":"https://example.test/a.pdf","locations":locations}});
        let text = render(&body, Some("webUrl"), false);
        assert!(text.contains("— https://example.test/a.pdf [also: clouddrive https://example.test/a.pdf; rest rest://ki/17.pdf]"));
        let single = json!({"id":"d#p1","attributes":{"text":"t","locations":json!([{"webUrl":"https://example.test/a.pdf"}]).to_string()}});
        assert!(!render(&single, None, false).contains("also"));
    }

    #[test]
    fn structured_links_keep_pdf_pages_without_retention_and_reject_conversions() {
        let body = json!({"rows":[
            {"id":"pdf#p2","page":2,"webUrl":"https://example.test/plan.pdf",
             "original_mime":"application/pdf","page_basis":"original_pdf"},
            {"id":"office#p2","page":2,"webUrl":"https://example.test/view?file=plan.docx",
             "original_mime":"application/vnd.openxmlformats-officedocument.wordprocessingml.document",
             "page_basis":"converted_pdf"},
            {"id":"conflict#p2","page":2,"webUrl":"https://example.test/converted.pdf",
             "original_mime":"application/pdf","page_basis":"converted_pdf"},
            {"id":"unknown#p2","page":2,"webUrl":"https://example.test/sheet.csv"}
        ]});
        let links = result_links(&body, Some("webUrl"), true);
        assert_eq!(links.len(), 4);
        assert_eq!(links[0]["id"], "pdf#p2");
        assert_eq!(links[0]["pageLink"], "https://example.test/plan.pdf#page=2");
        for link in &links[1..] {
            assert!(link["pageLink"].is_null(), "{link}");
            assert!(link["sourceLink"].is_string());
        }
        let text = render(&body, Some("webUrl"), true);
        assert!(text.contains("plan.pdf#page=2"));
        assert!(!text.contains("plan.docx#page=") && !text.contains("converted.pdf#page="));
        let grouped = json!({"groups":[{"best":body["rows"][0]}]});
        assert_eq!(result_links(&grouped, Some("webUrl"), true), links[..1]);
        assert!(result_links(&body, None, true).is_empty());
    }

    #[test]
    fn page_link_adds_page_anchored_source_link() {
        let hit = json!({"id":"d#p4","attributes":{"text":"x","page":4,"webUrl":"https://example.com/doc.pdf"}});
        let body = json!({"rows":[hit]});
        let on = render(&body, Some("webUrl"), true);
        assert!(on.contains(
            " — https://example.com/doc.pdf (page 4: https://example.com/doc.pdf#page=4)"
        ));
        assert_eq!(
            render(&body, Some("webUrl"), false),
            "1 record(s)\n\"d#p4\": x — https://example.com/doc.pdf"
        );
        let anchored =
            json!({"rows":[{"id":"a","attributes":{"page":2,"webUrl":"https://e.com/a#x"}}]});
        assert!(!render(&anchored, Some("webUrl"), true).contains("page 2"));
        assert!(!render(
            &json!({"rows":[{"id":"b","attributes":{"webUrl":"https://e.com/b"}}]}),
            Some("webUrl"),
            true
        )
        .contains("page"));
    }

    #[test]
    fn story_seed_and_native_voyage_profile_search_contract() {
        let registry = registry_from_json(Some(&json!({"bcc":{"namespaces":[{
            "name":"pov-bcc-story-pages","toolName":"story_documents","link":"webUrl","pageLink":true,
            "collapse":{"by":"document"},
            "filters":["company","job","division","region","doc_type"]}]}}).to_string())).unwrap();
        let spec = &registry.servers["bcc"].namespaces[0];
        assert!(spec.page_link);
        let metadata = json!({"schema":{
            "text":{"type":"string","full_text_search":true,"fuzzy":true,"filterable":false,
                    "embed":{"model":"voyage/voyage-4","dims":1024}},
            "company":{"type":"string"},"job":{"type":"string"},"division":{"type":"string"},
            "region":{"type":"string"},"doc_type":{"type":"string"},
            "webUrl":{"type":"string","filterable":false}}});
        let schema = NamespaceSchema::from_metadata(spec, &metadata).unwrap();
        assert_eq!(schema.search_kind().unwrap(), "Auto");
        assert_eq!(
            schema.rank_by("q").unwrap(),
            json!(["text","Auto","q",{"vector":["Embed","q",{"field":"text"}],"route":"fused"}])
        );
        let filters = schema
            .filters(Some(json!({"company":"story","job":"J1","division":"05","region":"r","doc_type":"permit"})))
            .unwrap();
        assert!(filters.is_some());
        assert!(schema.filters(Some(json!({"folder_1":"x"}))).is_err());
        // a missing configured filter must fail loudly, not be dropped
        let mut missing = metadata.clone();
        missing["schema"]
            .as_object_mut()
            .unwrap()
            .remove("division");
        assert!(NamespaceSchema::from_metadata(spec, &missing).is_err());
    }

    #[test]
    fn invalid_collapse_seed_is_rejected() {
        for collapse in [
            json!({"by":"document","expansion":0}),
            json!({"by":"document","typo":true}),
        ] {
            assert!(registry_from_json(Some(
                &json!({"demo":{"namespaces":[{"name":"docs","collapse":collapse}]}}).to_string()
            ))
            .is_err());
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
            json!(["body","Auto","q",{"vector":["Embed","q",{"field":"body"}],"route":"fused","fuzziness":0}])
        );
        let ambiguous = NamespaceSchema::from_metadata(&ns(), &json!({"schema":{"a":{"type":"string","full_text_search":true},"b":{"type":"string","full_text_search":true}}})).unwrap();
        assert!(ambiguous.rank_by("q").is_err());
        assert!(ambiguous.search_kind().is_err());
    }

    #[test]
    fn title_matching_keeps_compound_words_and_stable_ties() {
        let mut rows = vec![
            json!({"id":"notes","name":"2017-12-07 REFERENCE STRUCTURAL NOTES.pdf","$score":0.0479}),
            json!({"id":"finish","name":"Cedar Hall Finish Presentation.pdf","$score":0.043}),
            json!({"id":"revised","name":"21.11.09_Cedar Hall_Revised Floorplan.pdf","$score":0.04455}),
            json!({"id":"same-score","name":"Cedar Hall revised floorplan copy.pdf","$score":0.04455}),
        ];
        rank_mcp_labels(
            &mut rows,
            &["name".into()],
            "Cedar Hall office revised floor plan drawings",
            false,
        );
        assert_eq!(
            rows.iter()
                .map(|r| r["id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["revised", "same-score", "notes", "finish"]
        );
        let mut groups = vec![
            json!({"best":{"id":"missing"}}),
            json!({"best":{"id":"hit","name":"Cedar Hall revised floorplan"}}),
        ];
        rank_mcp_labels(
            &mut groups,
            &["name".into()],
            "Cedar Hall revised floor plan",
            true,
        );
        assert_eq!(groups[0]["best"]["id"], "hit");
    }

    #[test]
    fn title_evidence_preserves_a_strong_specific_retrieval_match() {
        let mut rows = vec![
            json!({"id":"exact","name":"Prime Change_02.pdf","$score":0.0984}),
            json!({"id":"generic","name":"Prime Change_Exterior Upgrades.pdf","$score":0.0744}),
            json!({"id":"other-work","name":"Cedar Remodel Sub CO1 Sawcutting and Patchback Change Order.pdf","$score":0.0528}),
        ];
        rank_mcp_labels(
            &mut rows,
            &["name".into()],
            "Cedar Remodel prime change order exterior and closet",
            false,
        );
        assert_eq!(rows[0]["id"], "exact");
        assert_eq!(rows[0]["$score"], 0.0984);
        assert!(rows
            .windows(2)
            .all(|w| w[0]["$mcp_score"].as_f64().unwrap() >= w[1]["$mcp_score"].as_f64().unwrap()));
        let mut images = vec![
            json!({"id":"near","$dist":0.1}),
            json!({"id":"far","$dist":0.9}),
        ];
        rank_mcp_labels(&mut images, &["name".into()], "ceiling drawing", false);
        assert_eq!(images[0]["id"], "near");
    }

    #[test]
    fn mcp_questions_keep_semantic_recall_and_lexical_names_at_every_length() {
        let schema = NamespaceSchema::from_metadata(&ns(), &json!({"schema":{"body":{"type":"string","full_text_search":true,"embed":{"model":"test"}}}})).unwrap();
        // The two-word case regressed on a signed page whose extracted body
        // contained only a signature/date. Longer questions must retain lexical
        // evidence as well; neither branch may silently become single-path.
        for query in [
            "change order",
            "CO #",
            "additional cost",
            "revised contract sum",
            "Cedar Remodel prime change order exterior and closet",
        ] {
            let rank = schema.rank_by(query).unwrap();
            assert_eq!(rank[3]["route"], "fused", "{query}");
            assert_eq!(rank[3]["fuzziness"], 0);
            assert_eq!(rank[3]["vector"], json!(["Embed", query, {"field":"body"}]));
        }
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

    #[test]
    fn string_list_filters_match_rows_whose_list_contains_the_value() {
        let mut spec = ns();
        spec.filters = Some(vec!["codes".into()]);
        let schema = NamespaceSchema::from_metadata(
            &spec,
            &json!({"schema":{"codes":{"type":"[]string","filterable":true}}}),
        )
        .unwrap();
        assert_eq!(
            schema.filters(Some(json!({"codes":"06-110"}))).unwrap(),
            Some(json!(["codes", "Contains", "06-110"]))
        );
        assert!(schema.filters(Some(json!({"codes":["06-110"]}))).is_err());
        assert_eq!(schema.field_names(FilterType::groupable), "none");
    }
    #[test]
    fn numeric_estimate_ranges_use_inclusive_store_predicates() {
        let schema = NamespaceSchema::from_metadata(
            &ns(),
            &json!({"schema":{
                "ki_estimate_total":{"type":"float","filterable":true}
            }}),
        )
        .unwrap();
        assert_eq!(
            schema
                .filters(Some(json!({"ki_estimate_total":{"min":100.5,"max":200.5}})))
                .unwrap(),
            Some(json!([
                "And",
                [
                    ["ki_estimate_total", "Lte", 200.5],
                    ["ki_estimate_total", "Gte", 100.5]
                ]
            ]))
        );
        assert!(schema
            .filters(Some(json!({"ki_estimate_total":{"min":"100.5"}})))
            .is_err());
    }
}
