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
        | Fts | Hybrid | Projection | ScalarFilters | NotFilters => Coverage::supported(),
        _ => Coverage::unsupported(),
    }
}

/// Phase-1 adapter entry point; no database connection is needed for coverage.
pub fn capabilities() -> crate::capabilities::Capabilities {
    PGVECTOR_CAPABILITIES
}
