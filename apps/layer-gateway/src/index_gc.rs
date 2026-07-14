use async_trait::async_trait;

#[derive(Debug, thiserror::Error)]
pub enum IndexDeleteError {
    #[error("kubernetes: {0}")]
    Kube(String),
}

#[async_trait]
pub trait IndexDeleter: Send + Sync {
    async fn delete_index_for_namespace(
        &self,
        namespace: &str,
    ) -> Result<Option<String>, IndexDeleteError>;
}

// Kubernetes Index deletion is pro-only and is not included in the public mirror.

pub fn index_name_for_namespace(namespace: &str) -> Option<String> {
    let normalized = namespace
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|ch| {
            if ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string();

    if normalized.is_empty() {
        return None;
    }

    let valid_as_is = normalized == namespace && normalized.len() <= 63;
    if valid_as_is {
        return Some(normalized);
    }

    let hash = fnv1a32(namespace);
    let mut prefix = normalized.chars().take(54).collect::<String>();
    prefix = prefix.trim_matches('-').to_string();
    if prefix.is_empty() {
        prefix = "namespace".to_string();
    }
    Some(format!("{prefix}-{hash:08x}"))
}

fn fnv1a32(value: &str) -> u32 {
    let mut hash = 0x811c9dc5u32;
    for byte in value.as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_name_preserves_valid_namespace() {
        assert_eq!(
            index_name_for_namespace("products-2026"),
            Some("products-2026".to_string())
        );
    }

    #[test]
    fn index_name_hashes_invalid_namespace() {
        assert_eq!(
            index_name_for_namespace("Shop/Products"),
            Some("shop-products-7d9f38a6".to_string())
        );
    }

    #[test]
    fn index_name_handles_empty_normalized_namespace() {
        assert_eq!(index_name_for_namespace("___"), None);
        assert_eq!(index_name_for_namespace("   "), None);
    }
}
