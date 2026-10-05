//! RFC 0114 phase-1 contract, shared with the pgvector adapter.
//! Kept dependency-free: declaring coverage must not enable SQLx or pro code.
pub const PGVECTOR_CAPABILITIES: crate::capabilities::Capabilities =
    crate::capabilities::Capabilities {
        kind: "pgvector",
        coverage: pgvector_coverage,
        // RFC 0117 declarations table: gateway-served embed on one attribute
        // (RFC 0118 step C); one BM25 index covers every text field; one HNSW
        // vector field.
        limits: crate::capabilities::SchemaLimits {
            embed: crate::capabilities::Coverage::approximate(GATEWAY_EMBED_ONE_ATTRIBUTE),
            max_gateway_embed_attributes: Some(1),
            max_full_text_search_fields: None,
            max_vector_fields: Some(1),
        },
        // A bytea table; the gateway's blob request cap bounds each value.
        blobs: crate::capabilities::BlobStorage::native(None),
    };

fn pgvector_coverage(feature: crate::capabilities::WireFeature) -> crate::capabilities::Coverage {
    use crate::capabilities::{Coverage, WireFeature::*};
    match feature {
        NamespaceCrud | UpsertRows | UpsertColumns | DeleteIds | Fetch | Dense | DistanceMetric
        | Fts | Projection | ScalarFilters | NotFilters | ArrayFilters | OrderedScan
        | PatchRows | PatchColumns | DeleteByFilter | ConditionalWrites | Collapse => Coverage::supported(),
        // Mirrors the phase-one gate in the gateway's run_hybrid_text.
        Hybrid => Coverage::approximate(HYBRID_TEXT_FUZZINESS_ZERO_ONLY),
        MultipleFields => Coverage::approximate(MULTIPLE_TEXT_FIELDS_ONE_VECTOR),
        Embed => Coverage::approximate(GATEWAY_EMBED_ONE_ATTRIBUTE),
        MultiQuery => {
            Coverage::unsupported_because(crate::capabilities::MULTI_QUERY_USE_HYBRID_TEXT)
        }
        Branch => Coverage::unsupported_because(crate::capabilities::NO_NATIVE_BRANCH),
        Search => Coverage::approximate(
            "BM25 + dense subset: one BM25 leg per full-text attribute, no fuzzy legs (reported in hybrid.dropped_legs).",
        ),
        _ => Coverage::unsupported(),
    }
}

/// RFC 0118 step C: the gateway embeds for Postgres, which cannot.
pub const GATEWAY_EMBED_ONE_ATTRIBUTE: &str = "Gateway-resolved embedding only; one embedded attribute per namespace; chunked embedding requires explicit turbopuffer serving (autoscaler alias).";

/// HybridText on phase one: BM25 + dense legs with gateway RRF, nothing else.
pub const HYBRID_TEXT_FUZZINESS_ZERO_ONLY: &str = "HybridText with fuzziness: 0 only (BM25 + dense legs, gateway RRF); auto/1/2 fuzziness and cursor/temporal_filter return 422";

/// Every `full_text_search` field shares one pg_search BM25 index; a second
/// `[N]f32` field returns 422.
pub const MULTIPLE_TEXT_FIELDS_ONE_VECTOR: &str = "Any number of full_text_search fields, ranked one at a time by BM25; a second vector field returns 422";

/// Phase-1 adapter entry point; no database connection is needed for coverage.
pub fn capabilities() -> crate::capabilities::Capabilities {
    PGVECTOR_CAPABILITIES
}
