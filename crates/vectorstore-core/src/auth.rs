#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApiScope {
    Read,
    Write,
    Admin,
}

impl ApiScope {
    pub fn as_str(self) -> &'static str {
        match self {
            ApiScope::Read => "read",
            ApiScope::Write => "write",
            ApiScope::Admin => "admin",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboundKey {
    pub name: String,
    pub token: String,
    pub scopes: Vec<ApiScope>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InboundAuth {
    Open,
    DeriveFromRequest,
    Keys(Vec<InboundKey>),
}

impl InboundAuth {
    pub fn open() -> Self {
        Self::Open
    }

    pub fn derived_admin_key(token: String) -> Self {
        Self::Keys(vec![InboundKey {
            name: "deriveFromStore".to_string(),
            token,
            scopes: vec![ApiScope::Admin, ApiScope::Read, ApiScope::Write],
        }])
    }

    pub fn derive_from_request() -> Self {
        Self::DeriveFromRequest
    }

    pub fn is_open(&self) -> bool {
        matches!(self, Self::Open)
    }
}
