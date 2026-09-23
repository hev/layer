//! RFC 0114 phase-1 contract, shared with the pgvector adapter.
//! Kept dependency-free: declaring coverage must not enable SQLx or pro code.
pub const PGVECTOR_CAPABILITIES: crate::capabilities::Capabilities =
    crate::capabilities::Capabilities {
        kind: "pgvector",
        coverage: pgvector_coverage,
    };

fn pgvector_coverage(feature: crate::capabilities::WireFeature) -> crate::capabilities::Coverage {
    use crate::capabilities::{Coverage, WireFeature::*};
    match feature {
        NamespaceCrud | UpsertRows | UpsertColumns | DeleteIds | Fetch | Dense | DistanceMetric
        | Fts | Projection | ScalarFilters | NotFilters => Coverage::supported(),
        // Mirrors the phase-one gate in the gateway's run_hybrid_text.
        Hybrid => Coverage::approximate(HYBRID_TEXT_FUZZINESS_ZERO_ONLY),
        MultiQuery => {
            Coverage::unsupported_because(crate::capabilities::MULTI_QUERY_USE_HYBRID_TEXT)
        }
        _ => Coverage::unsupported(),
    }
}

/// HybridText on phase one: BM25 + dense legs with gateway RRF, nothing else.
pub const HYBRID_TEXT_FUZZINESS_ZERO_ONLY: &str = "HybridText with fuzziness: 0 only (BM25 + dense legs, gateway RRF); auto/1/2 fuzziness and cursor/temporal_filter return 422";

/// Phase-1 adapter entry point; no database connection is needed for coverage.
pub fn capabilities() -> crate::capabilities::Capabilities {
    PGVECTOR_CAPABILITIES
}
