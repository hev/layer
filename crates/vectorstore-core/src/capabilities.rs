//! Wire-feature declarations shared by adapter rejection and generated docs.
//! Missing entries are unsupported, including future wire options.
use crate::turbopuffer::TurbopufferError;
use serde::Serialize;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Support {
    Supported,
    Approximate,
    Unsupported,
}

// Keep enum IDs, generated labels, and API-page ownership in one inventory.
macro_rules! wire_features {
    ($( $variant:ident => ($id:literal, $label:literal, $page:literal) ),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
        pub enum WireFeature { $( #[serde(rename = $id)] $variant, )+ }
        impl WireFeature {
            pub const ALL: &'static [Self] = &[$(Self::$variant,)+];
            pub fn id(self) -> &'static str { match self { $(Self::$variant => $id,)+ } }
            pub fn label(self) -> &'static str { match self { $(Self::$variant => $label,)+ } }
            pub fn page(self) -> &'static str { match self { $(Self::$variant => $page,)+ } }
        }
    };
}
wire_features! {
    NamespaceCrud => ("namespace_crud", "Namespace/schema CRUD and listing", "api/namespace-metadata"),
    UpsertRows => ("upsert_rows", "Row upserts", "api/write"),
    UpsertColumns => ("upsert_columns", "Column upserts", "api/write"),
    DeleteIds => ("deletes", "Delete by ID", "api/write"),
    Fetch => ("fetch", "Single/batch fetch", "api/query"),
    Dense => ("dense", "Dense top-k (ANN)", "api/query"),
    DistanceMetric => ("distance_metric", "Cosine / squared-Euclidean distance", "api/query"),
    Fts => ("fts", "BM25 text rank", "api/query"),
    NativeText => ("native_text", "Explicit native Postgres text fallback", "api/query"),
    Hybrid => ("hybrid", "HybridText rank operator (gateway dense + text RRF)", "api/query"),
    Projection => ("include_attributes", "Attribute projection", "api/query"),
    ScalarFilters => ("scalar_filters", "Eq / NotEq / Gt / Gte / Lt / Lte / In; And / Or", "api/query"),
    NotFilters => ("not_filters", "Not / NotIn filters", "api/query"),
    ArrayFilters => ("array_filters", "Contains / ContainsAny filters", "api/query"),
    RegexFilters => ("regex_filters", "Regex filters", "api/query"),
    AdvancedFilters => ("advanced_filters", "Other filter operators", "api/query"),
    Fuzzy => ("fuzzy", "Fuzzy text filters", "api/query"),
    AdvancedText => ("advanced_text", "Advanced text rank expressions", "api/query"),
    MultiVector => ("multivector", "Multi-vector ANN", "api/query"),
    Sparse => ("sparse", "Sparse rank", "api/query"),
    MultipleFields => ("multiple_fields", "Multiple vector/text fields", "api/query"),
    MultiQuery => ("multi_query", "Multi-query queries body / rerank_by (client-composed hybrid)", "api/query"),
    Pagination => ("search_after", "Ranked cursor / searchAfter", "api/query"),
    OrderedScan => ("ordered_scan", "Ordered scans", "api/scans"),
    Facet => ("facet", "Facets", "api/scans"),
    Aggregate => ("aggregate_by", "Aggregates / group_by", "api/query"),
    DeleteByFilter => ("delete_by_filter", "Delete by filter", "api/write"),
    PatchRows => ("patch_rows", "Row patches", "api/write"),
    PatchColumns => ("patch_columns", "Column patches", "api/write"),
    ConditionalWrites => ("conditional_writes", "Conditional writes", "api/write"),
    Copy => ("copy_from_namespace", "Copy namespace", "api/write"),
    Branch => ("branch_from_namespace", "Branch namespace", "api/write"),
    Encryption => ("encryption", "Vendor encryption controls", "api/write"),
    ImportArrow => ("import_arrow", "Arrow IPC import", "api/write"),
    Export => ("export", "Export formats", "api/scans"),
    Warm => ("hint_cache_warm", "Backend warm hints", "api/warm-cache"),
    Consistency => ("consistency", "Backend stability / watermark signals", "api/query"),
    Snapshots => ("snapshots", "Backend snapshot integration", "api/snapshots"),
    Udf => ("udf", "UDF discovery / writeback primitives", "api/data-supply"),
    Passthrough => ("passthrough", "Arbitrary vendor administrative passthrough", "api/introduction"),
    Embed => ("embed", "Embedding expressions / schema", "api/embed"),
    NearestToId => ("nearest_to_id", "Query by stored vector ID", "api/query"),
    Temporal => ("temporal", "as_of / between filters", "api/query"),
    LegBreakdown => ("include_leg_breakdown", "Fused leg provenance", "api/query"),
    Auto => ("auto", "Auto query routing", "api/query"),
    Threads => ("threads", "Query scatter/gather", "api/query"),
    VectorEncoding => ("vector_encoding", "Wire vector encoding", "api/query"),
}

impl WireFeature {
    /// The feature that owns a request-body key, for stores that reject the
    /// key. A rejected key an inventoried feature owns is reported as that
    /// feature's id (RFC 0118 § "The 422 body"): a client matches one string
    /// against the capability report and the 422. `None` means the wire key
    /// itself is the feature name.
    pub fn for_wire_key(key: &str) -> Option<Self> {
        match key {
            // Keys that native query/write bodies admit through
            // additionalProperties, so SCHEMA_FIELDS does not list them.
            "queries" | "rerank_by" => return Some(Self::MultiQuery),
            "group_by" => return Some(Self::Aggregate),
            "searchAfter" | "cursor" => return Some(Self::Pagination),
            "upsert_condition" | "patch_condition" | "delete_condition" => {
                return Some(Self::ConditionalWrites)
            }
            _ => {}
        }
        SCHEMA_FIELDS
            .iter()
            .flat_map(|(_, fields)| fields.iter())
            .find(|(name, _)| *name == key)
            .map(|(_, feature)| *feature)
    }

    pub fn for_rank(rank_by: &serde_json::Value) -> Option<Self> {
        let op = rank_by.get(1)?.as_str()?;
        if op.eq_ignore_ascii_case("BM25") || op.eq_ignore_ascii_case("HybridText") {
            Some(Self::Fts)
        } else if op.eq_ignore_ascii_case("ANN") {
            Some(
                if rank_by
                    .get(2)
                    .and_then(|value| value.get(0))
                    .is_some_and(serde_json::Value::is_array)
                {
                    Self::MultiVector
                } else {
                    Self::Dense
                },
            )
        } else {
            None
        }
    }
}

/// The two query routes that express hybrid retrieval. Each is governed by
/// exactly one wire feature, so a store's declaration answers per route.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HybridRoute {
    /// `rank_by: [field, "HybridText", ...]`; the gateway issues one ranked
    /// query per leg and fuses with RRF.
    HybridText,
    /// A `queries` body: independent legs, fused by the store when
    /// `rerank_by` is set.
    MultiQuery,
}
impl HybridRoute {
    pub const ALL: &'static [Self] = &[Self::HybridText, Self::MultiQuery];
    pub fn id(self) -> &'static str {
        match self {
            Self::HybridText => "hybrid_text",
            Self::MultiQuery => "multi_query",
        }
    }
    /// The wire feature whose declared coverage governs this route.
    pub fn feature(self) -> WireFeature {
        match self {
            Self::HybridText => WireFeature::Hybrid,
            Self::MultiQuery => WireFeature::MultiQuery,
        }
    }
    /// Classify a query body; `None` is a single, non-hybrid query.
    pub fn for_query_body(body: &serde_json::Value) -> Option<Self> {
        if body.get("queries").is_some() || body.get("rerank_by").is_some() {
            return Some(Self::MultiQuery);
        }
        let op = body.get("rank_by")?.get(1)?.as_str()?;
        // Exact, like the gateway's HybridText interception.
        (op == "HybridText").then_some(Self::HybridText)
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Coverage {
    pub support: Support,
    pub note: &'static str,
}
impl Coverage {
    pub const fn supported() -> Self {
        Self {
            support: Support::Supported,
            note: "",
        }
    }
    pub const fn approximate(note: &'static str) -> Self {
        Self {
            support: Support::Approximate,
            note,
        }
    }
    pub const fn unsupported() -> Self {
        Self {
            support: Support::Unsupported,
            note: "",
        }
    }
    pub const fn unsupported_because(note: &'static str) -> Self {
        Self {
            support: Support::Unsupported,
            note,
        }
    }
}

/// Cell note for a store with no native namespace branch (RFC 0124). The
/// gateway returns `422 UnsupportedByStore` and never emulates one with a copy.
pub const NO_NATIVE_BRANCH: &str =
    "No native namespace branching; the gateway does not emulate one.";

/// Cell note for a store that serves hybrid only through `HybridText`.
pub const MULTI_QUERY_USE_HYBRID_TEXT: &str =
    "422 for a queries or rerank_by body; hybrid retrieval is the HybridText rank operator";

/// Schema-shape limits a store enforces (RFC 0117 `schema_limits`). Each
/// `max_*` is `None` when the store declares no limit. Rendered in the
/// generated store matrix and, once the capabilities endpoint ships, in
/// `CapabilitiesReport.schema_limits`.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct SchemaLimits {
    /// Whether a schema attribute may declare `embed`; the `embed` cell.
    pub embed: Coverage,
    pub max_gateway_embed_attributes: Option<u32>,
    pub max_full_text_search_fields: Option<u32>,
    pub max_vector_fields: Option<u32>,
}
impl SchemaLimits {
    /// The undeclared row: no claim on any limit.
    pub const UNDECLARED: Self = Self {
        embed: Coverage::unsupported(),
        max_gateway_embed_attributes: None,
        max_full_text_search_fields: None,
        max_vector_fields: None,
    };
}

/// Where a store keeps blob bytes (RFC 0123 § "Finishing Layer's blob
/// store"). A native store holds blobs up to `max_value_bytes` itself; larger
/// blobs, and every blob on a store without native bytes, go to S3.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct BlobStorage {
    pub native: bool,
    /// Largest blob the store holds, in bytes. `None` is no store cap below
    /// the gateway's own blob request cap.
    pub max_value_bytes: Option<u64>,
}
impl BlobStorage {
    /// No native bytes: every blob goes to S3.
    pub const NONE: Self = Self {
        native: false,
        max_value_bytes: None,
    };
    pub const fn native(max_value_bytes: Option<u64>) -> Self {
        Self {
            native: true,
            max_value_bytes,
        }
    }
    /// Whether the store itself holds a blob of `len` bytes.
    pub fn holds(self, len: usize) -> bool {
        self.native && self.max_value_bytes.is_none_or(|max| len as u64 <= max)
    }
}

#[derive(Clone, Copy)]
pub struct Capabilities {
    pub kind: &'static str,
    pub coverage: fn(WireFeature) -> Coverage,
    pub limits: SchemaLimits,
    pub blobs: BlobStorage,
}
impl Capabilities {
    pub fn get(self, feature: WireFeature) -> Coverage {
        (self.coverage)(feature)
    }
    /// Whether this store accepts a hybrid route; enumerate `HybridRoute::ALL`.
    pub fn hybrid_route(self, route: HybridRoute) -> Coverage {
        self.get(route.feature())
    }
    /// Preserve the existing string-sniffed UnsupportedByStore dispatch.
    pub fn require(self, feature: WireFeature) -> Result<(), TurbopufferError> {
        if self.get(feature).support == Support::Unsupported {
            Err(unsupported_by_store(self.kind, feature.id(), None))
        } else {
            Ok(())
        }
    }
    /// Default trait methods have no implementation. Consult the declaration
    /// before reporting an unsupported feature; an inconsistent declaration is
    /// an adapter bug, not a new 422 contract.
    pub fn unimplemented<T>(self, feature: WireFeature) -> Result<T, TurbopufferError> {
        self.require(feature)?;
        if self.get(feature).support == Support::Approximate {
            return Err(TurbopufferError::Other(format!(
                "UnsupportedByStore: {}: {}: {}",
                self.kind,
                feature.id(),
                self.get(feature).note
            )));
        }
        Err(TurbopufferError::Other(format!(
            "{} declares {} but has no adapter implementation",
            self.kind,
            feature.id()
        )))
    }
}

impl Capabilities {
    /// Whether this build declares the store at all.
    pub fn declared(self) -> bool {
        self.kind != UNDECLARED.kind
    }

    /// The runtime report (RFC 0117 `CapabilitiesReport`): every wire
    /// feature, both hybrid routes and the schema limits, for the store
    /// resource `store_name`. Undeclared stores report `undeclared` cells.
    pub fn report(self, store_name: &str) -> serde_json::Value {
        let declared = self.declared();
        let cell = |coverage: Coverage| {
            if declared {
                serde_json::json!({"support": coverage.support, "note": coverage.note})
            } else {
                serde_json::json!({"support": "undeclared", "note": ""})
            }
        };
        let features: Vec<serde_json::Value> = WireFeature::ALL
            .iter()
            .map(|&feature| {
                let mut entry = serde_json::json!({
                    "id": feature.id(),
                    "label": feature.label(),
                    "page": feature.page(),
                });
                merge(&mut entry, cell(self.get(feature)));
                entry
            })
            .collect();
        let hybrid_routes: Vec<serde_json::Value> = HybridRoute::ALL
            .iter()
            .map(|&route| {
                let mut entry = serde_json::json!({
                    "route": route.id(),
                    "feature": route.feature().id(),
                });
                merge(&mut entry, cell(self.hybrid_route(route)));
                entry
            })
            .collect();
        let limits = if declared {
            serde_json::json!({
                "embed": cell(self.limits.embed),
                "max_gateway_embed_attributes": self.limits.max_gateway_embed_attributes,
                "max_full_text_search_fields": self.limits.max_full_text_search_fields,
                "max_vector_fields": self.limits.max_vector_fields,
            })
        } else {
            serde_json::json!({
                "embed": cell(self.limits.embed),
                "max_gateway_embed_attributes": null,
                "max_full_text_search_fields": null,
                "max_vector_fields": null,
            })
        };
        serde_json::json!({
            "store": {"name": store_name, "kind": self.kind},
            "declared": declared,
            "features": features,
            "hybrid_routes": hybrid_routes,
            "schema_limits": limits,
        })
    }
}

fn merge(target: &mut serde_json::Value, extra: serde_json::Value) {
    if let (Some(target), serde_json::Value::Object(extra)) = (target.as_object_mut(), extra) {
        target.extend(extra);
    }
}

/// Marker every store rejection carries; gateway routes dispatch on it.
pub const UNSUPPORTED_BY_STORE: &str = "UnsupportedByStore";

/// The canonical rejection: `UnsupportedByStore: {store}: {feature}` with an
/// optional `: {detail}` tail. `feature` is a stable identifier: a
/// [`WireFeature`] id where one exists, otherwise the rejected wire key,
/// dotted when nested (`schema.embed`). Prose belongs in `detail`, never in
/// `feature`, so [`StoreRejection::parse`] can recover the typed field from
/// the message on every path that only carries a string.
pub fn unsupported_by_store(store: &str, feature: &str, detail: Option<&str>) -> TurbopufferError {
    let mut message = format!("{UNSUPPORTED_BY_STORE}: {store}: {feature}");
    if let Some(detail) = detail.filter(|d| !d.is_empty()) {
        message.push_str(": ");
        message.push_str(detail);
    }
    TurbopufferError::Other(message)
}

/// The typed view of a canonical rejection message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreRejection<'a> {
    pub store: &'a str,
    pub feature: &'a str,
    /// The message from the `UnsupportedByStore` marker on, without any
    /// transport prefix such as `Turbopuffer error: `.
    pub message: &'a str,
}
impl<'a> StoreRejection<'a> {
    fn is_identifier(token: &str) -> bool {
        !token.is_empty()
            && token
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
    }
    /// Recover `store` and `feature` from a message in the canonical shape.
    /// Free-form rejections (`UnsupportedByStore: search backend does not
    /// support …`) return `None`: they name no feature and must not invent
    /// one.
    pub fn parse(message: &'a str) -> Option<Self> {
        let start = message.find(UNSUPPORTED_BY_STORE)?;
        let message = &message[start..];
        let rest = message
            .strip_prefix(UNSUPPORTED_BY_STORE)?
            .strip_prefix(": ")?;
        let (store, rest) = rest.split_once(": ")?;
        let feature = rest.split([':', ' ']).next().unwrap_or_default();
        (Self::is_identifier(store) && Self::is_identifier(feature)).then_some(Self {
            store,
            feature,
            message,
        })
    }
    /// The message from the marker on, for any string that carries it.
    pub fn canonical_message(message: &str) -> &str {
        message
            .find(UNSUPPORTED_BY_STORE)
            .map_or(message, |start| &message[start..])
    }
}

pub const UNDECLARED: Capabilities = Capabilities {
    kind: "undeclared",
    coverage: |_| Coverage::unsupported(),
    limits: SchemaLimits::UNDECLARED,
    blobs: BlobStorage::NONE,
};

/// Explicit schema-property ownership. Native query/write bodies also permit
/// additional properties; ALL inventories their named operators. Subset adapters
/// must reject unlisted options, never infer support from additionalProperties.
pub const SCHEMA_FIELDS: &[(&str, &[(&str, WireFeature)])] = &[
    (
        "QueryRequest",
        &[
            ("vector", WireFeature::Dense),
            ("nearest_to_id", WireFeature::NearestToId),
            ("top_k", WireFeature::Dense),
            ("filters", WireFeature::ScalarFilters),
            ("as_of", WireFeature::Temporal),
            ("between", WireFeature::Temporal),
            ("include_attributes", WireFeature::Projection),
            ("include_leg_breakdown", WireFeature::LegBreakdown),
            ("cursor", WireFeature::Pagination),
            ("rank_by", WireFeature::Dense),
        ],
    ),
    (
        "BatchQueryRequest",
        &[
            ("queries", WireFeature::MultiQuery),
            ("consistency", WireFeature::Consistency),
            ("vector_encoding", WireFeature::VectorEncoding),
        ],
    ),
    ("TurbopufferQueryRequest", &[]),
    ("TurbopufferWriteRequest", &[]),
    (
        "TurbopufferBranchFromRequest",
        &[("branch_from_namespace", WireFeature::Branch)],
    ),
    (
        "TurbopufferCopyFromRequest",
        &[("copy_from_namespace", WireFeature::Copy)],
    ),
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgvector_capabilities::PGVECTOR_CAPABILITIES;
    use crate::search::{HttpSearchClient, SEARCH_CAPABILITIES};
    use crate::turbopuffer::{HttpTurbopufferClient, TurbopufferClient, TURBOPUFFER_CAPABILITIES};

    #[test]
    fn phase_one_is_an_explicit_allowlist() {
        use WireFeature::*;
        let allowed = [
            NamespaceCrud,
            UpsertRows,
            UpsertColumns,
            DeleteIds,
            Fetch,
            Dense,
            DistanceMetric,
            Fts,
            Hybrid,
            Projection,
            ScalarFilters,
            NotFilters,
            OrderedScan,
            ConditionalWrites,
        ];
        // Served with the limits stated in the cell: HybridText with
        // fuzziness 0, conditional upserts/deletes, and any number of text
        // fields but one vector field.
        let approximate = [Hybrid, ConditionalWrites, MultipleFields];
        for &feature in WireFeature::ALL {
            let coverage = PGVECTOR_CAPABILITIES.get(feature);
            assert_eq!(
                coverage.support,
                if approximate.contains(&feature) {
                    Support::Approximate
                } else if allowed.contains(&feature) {
                    Support::Supported
                } else {
                    Support::Unsupported
                },
                "{}",
                feature.id()
            );
            assert!(
                coverage.support != Support::Approximate || !coverage.note.is_empty(),
                "{}: approximate coverage states its limits",
                feature.id()
            );
            assert_eq!(
                PGVECTOR_CAPABILITIES.require(feature).is_ok(),
                allowed.contains(&feature) || approximate.contains(&feature)
            );
        }
        // RFC 0117 declarations table, pgvector row, after LYR-87.
        let limits = PGVECTOR_CAPABILITIES.limits;
        assert_eq!(
            limits.embed.support,
            PGVECTOR_CAPABILITIES.get(Embed).support
        );
        assert_eq!(limits.max_gateway_embed_attributes, Some(0));
        assert_eq!(limits.max_full_text_search_fields, None);
        assert_eq!(limits.max_vector_fields, Some(1));
    }

    #[tokio::test]
    async fn adapter_rejections_follow_the_declared_features_without_network() {
        let search = HttpSearchClient::new(None, "http://127.0.0.1:1");
        assert_eq!(search.capabilities().kind, SEARCH_CAPABILITIES.kind);
        let error = search
            .multi_ranked_query("unused", &[], None)
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("UnsupportedByStore: search: multi_query"));
        let response = search
            .passthrough("GET", "/v1/namespaces", None, None)
            .await
            .unwrap();
        assert_eq!(response.status, 422);
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["error"], "UnsupportedByStore");
        assert!(body["message"].as_str().unwrap().contains("passthrough"));
        let turbopuffer = HttpTurbopufferClient::new("unused", "http://127.0.0.1:1");
        assert_eq!(
            turbopuffer.capabilities().kind,
            TURBOPUFFER_CAPABILITIES.kind
        );
        let error = turbopuffer
            .import_arrow("unused", "application/octet-stream", vec![])
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("UnsupportedByStore: turbopuffer: import_arrow"));
        for &feature in WireFeature::ALL {
            assert_eq!(
                search.capabilities().get(feature).support,
                SEARCH_CAPABILITIES.get(feature).support
            );
            assert_eq!(
                turbopuffer.capabilities().get(feature).support,
                TURBOPUFFER_CAPABILITIES.get(feature).support
            );
        }
    }
}
