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
    Fts => ("fts", "Single-field BM25 text rank", "api/query"),
    NativeText => ("native_text", "Explicit native Postgres text fallback", "api/query"),
    Hybrid => ("hybrid", "Gateway dense + text RRF", "api/query"),
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
    MultiQuery => ("multi_query", "Raw multi-query / rerank_by", "api/query"),
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
}

#[derive(Clone, Copy)]
pub struct Capabilities {
    pub kind: &'static str,
    pub coverage: fn(WireFeature) -> Coverage,
}
impl Capabilities {
    pub fn get(self, feature: WireFeature) -> Coverage {
        (self.coverage)(feature)
    }
    /// Preserve the existing string-sniffed UnsupportedByStore dispatch.
    pub fn require(self, feature: WireFeature) -> Result<(), TurbopufferError> {
        if self.get(feature).support == Support::Unsupported {
            Err(TurbopufferError::Other(format!(
                "UnsupportedByStore: {}: {}",
                self.kind,
                feature.id()
            )))
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

pub const UNDECLARED: Capabilities = Capabilities {
    kind: "undeclared",
    coverage: |_| Coverage::unsupported(),
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
        ];
        for &feature in WireFeature::ALL {
            assert_eq!(
                PGVECTOR_CAPABILITIES.get(feature).support,
                if allowed.contains(&feature) {
                    Support::Supported
                } else {
                    Support::Unsupported
                },
                "{}",
                feature.id()
            );
            assert_eq!(
                PGVECTOR_CAPABILITIES.require(feature).is_ok(),
                allowed.contains(&feature)
            );
        }
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
