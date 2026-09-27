//! Index config keyed by namespace name or by a namespace pattern (RFC 0124).
//!
//! A key ending in `*` is a pattern: it matches every namespace that starts
//! with the text before the `*`, the same glob the `ApiKey` `namespaces`
//! grants use. An exact key wins over a pattern, and the longest matching
//! pattern wins over a shorter one. Config loaders reject two patterns that
//! overlap, so the longest-match rule only breaks ties with exact names.
use std::collections::HashMap;

/// Whether a namespace config key is a pattern rather than one namespace.
pub fn is_pattern(key: &str) -> bool {
    key.ends_with('*')
}

/// Whether `pattern` matches `namespace`. A non-pattern key matches only
/// itself.
pub fn matches(pattern: &str, namespace: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => namespace.starts_with(prefix),
        None => pattern == namespace,
    }
}

/// Whether two config keys can match the same namespace. Two patterns
/// overlap when one prefix starts with the other.
pub fn overlaps(a: &str, b: &str) -> bool {
    match (a.strip_suffix('*'), b.strip_suffix('*')) {
        (Some(a), Some(b)) => a.starts_with(b) || b.starts_with(a),
        (Some(prefix), None) => b.starts_with(prefix),
        (None, Some(prefix)) => a.starts_with(prefix),
        (None, None) => a == b,
    }
}

/// Resolve `namespace` against a map keyed by exact names and patterns.
pub fn resolve<'a, T>(map: &'a HashMap<String, T>, namespace: &str) -> Option<&'a T> {
    if let Some(value) = map.get(namespace) {
        return Some(value);
    }
    map.iter()
        .filter(|(key, _)| is_pattern(key) && matches(key, namespace))
        .max_by_key(|(key, _)| key.len())
        .map(|(_, value)| value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_name_wins_over_pattern() {
        let map = HashMap::from([
            ("wt-*".to_string(), "family"),
            ("wt-main".to_string(), "exact"),
        ]);
        assert_eq!(resolve(&map, "wt-main"), Some(&"exact"));
        assert_eq!(resolve(&map, "wt-lyr-130"), Some(&"family"));
        assert_eq!(resolve(&map, "trunk"), None);
    }

    #[test]
    fn a_pattern_does_not_match_its_own_prefix_elsewhere() {
        let map = HashMap::from([("wt-*".to_string(), 1)]);
        assert_eq!(resolve(&map, "wt-"), Some(&1));
        assert_eq!(resolve(&map, "xwt-1"), None);
    }

    #[test]
    fn overlap_is_symmetric() {
        assert!(overlaps("wt-*", "wt-lyr-*"));
        assert!(overlaps("wt-lyr-*", "wt-*"));
        assert!(overlaps("*", "wt-*"));
        assert!(!overlaps("wt-*", "tag-*"));
        assert!(overlaps("wt-*", "wt-main"));
        assert!(!overlaps("wt-main", "wt-other"));
    }
}
