use std::collections::BTreeMap;

use crate::core::ProxyAuth;
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
pub(super) struct ValidationIssue {
    pub(super) path: String,
    pub(super) message: String,
}

#[derive(Serialize)]
pub(super) struct ValidateConfigResponse {
    pub(super) valid: bool,
    pub(super) errors: BTreeMap<String, Vec<ValidationIssue>>,
}

/// Request body for setting proxy authentication.
#[derive(Debug, Deserialize)]
pub struct ProxyAuthPayload {
    /// Proxy username.
    username: Option<String>,
    /// Proxy password.
    password: Option<String>,
}

impl ProxyAuthPayload {
    pub(super) fn into_proxy_auth(self) -> Option<ProxyAuth> {
        let username = self.username.unwrap_or_default();
        if username.trim().is_empty() {
            return None;
        }

        Some(ProxyAuth {
            username,
            password: self.password.unwrap_or_default(),
        })
    }
}
