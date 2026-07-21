use serde::{Deserialize, Serialize};

/// Request payload for creating or updating an environment variable.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpsertEnvVarRequest {
    pub expected_revision: u64,
    #[serde(flatten)]
    pub entry: EnvVarInput,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvVarInput {
    pub name: String,
    /// Missing keeps an existing value; `""` explicitly clears it.
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub secret: bool,
    #[serde(default)]
    pub description: Option<String>,
}

/// Request payload for bulk-replacing the entire env vars list.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplaceEnvVarsRequest {
    pub expected_revision: u64,
    pub entries: Vec<EnvVarInput>,
}

/// CAS precondition for DELETE requests.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteEnvVarQuery {
    pub expected_revision: u64,
}

/// Single env var in API response (secrets are masked).
#[derive(Debug, Clone, Serialize)]
pub struct EnvVarResponse {
    pub name: String,
    /// Masked for secrets (`****...****`); plaintext for non-secrets.
    pub value: String,
    pub secret: bool,
    /// Whether a real value is configured (useful for secret entries where value is masked).
    pub has_value: bool,
    /// Truthful configured status (kept alongside `has_value` for API
    /// compatibility).
    pub configured: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Full list response.
#[derive(Debug, Clone, Serialize)]
pub struct EnvVarsListResponse {
    /// Credential document revision to bind to the next write.
    pub revision: u64,
    pub entries: Vec<EnvVarResponse>,
}

const SECRET_MASK: &str = "****...****";

impl EnvVarResponse {
    pub fn from_entry(entry: &bamboo_config::EnvVarEntry) -> Self {
        let has_value = if entry.secret {
            entry.configured
        } else {
            !entry.value.is_empty()
        };
        let display_value = if entry.secret {
            SECRET_MASK.to_string()
        } else {
            entry.value.clone()
        };
        Self {
            name: entry.name.clone(),
            value: display_value,
            secret: entry.secret,
            has_value,
            configured: has_value,
            description: entry.description.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_config::EnvVarEntry;

    fn plain_entry() -> EnvVarEntry {
        EnvVarEntry {
            name: "NODE_ENV".to_string(),
            value: "production".to_string(),
            secret: false,
            value_encrypted: None,
            credential_ref: None,
            configured: true,
            description: Some("Node environment".to_string()),
        }
    }

    fn secret_entry_with_value() -> EnvVarEntry {
        EnvVarEntry {
            name: "API_KEY".to_string(),
            value: "sk-real-secret".to_string(),
            secret: true,
            value_encrypted: Some("enc-data".to_string()),
            credential_ref: Some(bamboo_config::credential_ref("env", "API_KEY", "value").unwrap()),
            configured: true,
            description: None,
        }
    }

    fn secret_entry_without_value() -> EnvVarEntry {
        EnvVarEntry {
            name: "EMPTY_SECRET".to_string(),
            value: "".to_string(),
            secret: true,
            value_encrypted: None,
            credential_ref: Some(
                bamboo_config::credential_ref("env", "EMPTY_SECRET", "value").unwrap(),
            ),
            configured: false,
            description: None,
        }
    }

    #[test]
    fn from_entry_plain_shows_value() {
        let resp = EnvVarResponse::from_entry(&plain_entry());
        assert_eq!(resp.name, "NODE_ENV");
        assert_eq!(resp.value, "production");
        assert!(!resp.secret);
        assert!(resp.has_value);
        assert_eq!(resp.description.as_deref(), Some("Node environment"));
    }

    #[test]
    fn from_entry_secret_masks_value() {
        let resp = EnvVarResponse::from_entry(&secret_entry_with_value());
        assert_eq!(resp.name, "API_KEY");
        assert_eq!(resp.value, "****...****");
        assert!(resp.secret);
        assert!(resp.has_value);
    }

    #[test]
    fn from_entry_secret_without_value_shows_not_set() {
        let resp = EnvVarResponse::from_entry(&secret_entry_without_value());
        assert_eq!(resp.value, "****...****");
        assert!(resp.secret);
        assert!(!resp.has_value);
    }

    #[test]
    fn from_entry_plain_empty_value() {
        let mut entry = plain_entry();
        entry.value = "".to_string();
        let resp = EnvVarResponse::from_entry(&entry);
        assert_eq!(resp.value, "");
        assert!(!resp.has_value);
    }
}
