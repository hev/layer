// Local provisioning from the standalone identity companion (hev/layer #4).
use std::{fs::File, io::Read, sync::Arc};

use serde::Deserialize;

use crate::{
    auth::{constant_time_eq, ApiScope},
    AppState,
};

const MAX_GRANT_FILE_BYTES: u64 = 64 * 1024;
const MAX_KEYS: usize = 128;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LocalKey {
    token: String,
    store: String,
    namespaces: Vec<String>,
    scopes: Vec<String>,
}

#[derive(Clone)]
pub(super) struct Authorization {
    state: Arc<AppState>,
    keys: Arc<[LocalKey]>,
}

impl Authorization {
    pub(super) fn new(state: Arc<AppState>) -> Self {
        let keys: Vec<LocalKey> = match std::env::var_os("LAYER_WORK_IDENTITY_KEYS_FILE") {
            None => Vec::new(),
            Some(path) => {
                let mut bytes = Vec::new();
                File::open(path)
                    .and_then(|file| file.take(MAX_GRANT_FILE_BYTES + 1).read_to_end(&mut bytes))
                    .ok()
                    .filter(|&len| len as u64 <= MAX_GRANT_FILE_BYTES)
                    .and_then(|_| serde_json::from_slice(&bytes).ok())
                    .expect(
                        "failed to read LAYER_WORK_IDENTITY_KEYS_FILE as work identity key grants",
                    )
            }
        };
        assert!(
            keys.len() <= MAX_KEYS
                && keys.iter().enumerate().all(|(index, key)| {
                    is_local_token(&key.token)
                        && keys[..index]
                            .iter()
                            .all(|previous| previous.token != key.token)
                }),
            "invalid, duplicate or excessive local work identity bearer grants"
        );
        Self {
            state,
            keys: keys.into(),
        }
    }

    pub(super) fn authenticate(&self, token: &str) -> Option<&LocalKey> {
        if !is_local_token(token) {
            return None;
        }
        self.keys
            .iter()
            .find(|key| constant_time_eq(token.as_bytes(), key.token.as_bytes()))
    }

    pub(super) fn grants(&self, key: &LocalKey, namespace: &str, scope: ApiScope) -> bool {
        key.store == self.state.store_for_namespace(namespace)
            && key.namespaces.iter().any(|name| name == namespace)
            && key.scopes.iter().any(|name| name == scope.as_str())
    }
}

fn is_local_token(token: &str) -> bool {
    token.len() <= 256
        && token.strip_prefix("hvl_").is_some_and(|value| {
            !value.is_empty()
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        })
}
