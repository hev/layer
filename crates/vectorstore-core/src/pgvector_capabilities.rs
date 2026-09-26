//! RFC 0114 phase-1 contract, shared with the pgvector adapter.
//! Kept dependency-free: declaring coverage must not enable SQLx or pro code.
pub const PGVECTOR_CAPABILITIES: crate::capabilities::Capabilities =
    crate::capabilities::Capabilities {
        kind: "pgvector",
        coverage: pgvector_coverage,
        // RFC 0117 declarations table: no embed until RFC 0118 step C; one
        // BM25 index covers every text field; one HNSW vector field.
        limits: crate::capabilities::SchemaLimits {
            embed: crate::capabilities::Coverage::unsupported(),
            max_gateway_embed_attributes: Some(0),
            max_full_text_search_fields: None,
            max_vector_fields: Some(1),
        },
    };

fn pgvector_coverage(feature: crate::capabilities::WireFeature) -> crate::capabilities::Coverage {
    use crate::capabilities::{Coverage, WireFeature::*};
    match feature {
        NamespaceCrud | UpsertRows | UpsertColumns | DeleteIds | Fetch | Dense | DistanceMetric
        | Fts | Projection | ScalarFilters | NotFilters | OrderedScan => Coverage::supported(),
        ConditionalWrites => Coverage::approximate(CONDITIONAL_WRITES_UPSERT_AND_DELETE),
        // Mirrors the phase-one gate in the gateway's run_hybrid_text.
        Hybrid => Coverage::approximate(HYBRID_TEXT_FUZZINESS_ZERO_ONLY),
        MultipleFields => Coverage::approximate(MULTIPLE_TEXT_FIELDS_ONE_VECTOR),
        MultiQuery => {
            Coverage::unsupported_because(crate::capabilities::MULTI_QUERY_USE_HYBRID_TEXT)
        }
        _ => Coverage::unsupported(),
    }
}

/// HybridText on phase one: BM25 + dense legs with gateway RRF, nothing else.
pub const HYBRID_TEXT_FUZZINESS_ZERO_ONLY: &str = "HybridText with fuzziness: 0 only (BM25 + dense legs, gateway RRF); auto/1/2 fuzziness and cursor/temporal_filter return 422";

/// Every `full_text_search` field shares one pg_search BM25 index; a second
/// `[N]f32` field returns 422.
pub const MULTIPLE_TEXT_FIELDS_ONE_VECTOR: &str = "Any number of full_text_search fields, ranked one at a time by BM25; a second vector field returns 422";

/// Conditions run in the write's transaction: `INSERT ... ON CONFLICT DO UPDATE
/// ... WHERE` for upserts, `DELETE ... WHERE` for deletes.
pub const CONDITIONAL_WRITES_UPSERT_AND_DELETE: &str = "upsert_condition and delete_condition, including $ref_new; patch_condition returns 422 because row and column patches are unsupported";

/// Phase-1 adapter entry point; no database connection is needed for coverage.
pub fn capabilities() -> crate::capabilities::Capabilities {
    PGVECTOR_CAPABILITIES
}
