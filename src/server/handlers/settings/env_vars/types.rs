use serde::{Deserialize, Serialize};

/// Request payload for creating or updating an environment variable.
#[derive(Debug, Clone, Deserialize)]
pub struct UpsertEnvVarRequest {
    pub name: String,
    pub value: String,
    #[serde(default)]
    pub secret: bool,
    #[serde(default)]
    pub description: Option<String>,
}

/// Request payload for bulk-replacing the entire env vars list.
#[derive(Debug, Clone, Deserialize)]
pub struct ReplaceEnvVarsRequest {
    pub entries: Vec<UpsertEnvVarRequest>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Full list response.
#[derive(Debug, Clone, Serialize)]
pub struct EnvVarsListResponse {
    pub entries: Vec<EnvVarResponse>,
}

const SECRET_MASK: &str = "****...****";

impl EnvVarResponse {
    pub fn from_entry(entry: &crate::core::EnvVarEntry) -> Self {
        let has_value = !entry.value.trim().is_empty();
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
            description: entry.description.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::EnvVarEntry;

    fn plain_entry() -> EnvVarEntry {
        EnvVarEntry {
            name: "NODE_ENV".to_string(),
            value: "production".to_string(),
            secret: false,
            value_encrypted: None,
            description: Some("Node environment".to_string()),
        }
    }

    fn secret_entry_with_value() -> EnvVarEntry {
        EnvVarEntry {
            name: "API_KEY".to_string(),
            value: "sk-real-secret".to_string(),
            secret: true,
            value_encrypted: Some("enc-data".to_string()),
            description: None,
        }
    }

    fn secret_entry_without_value() -> EnvVarEntry {
        EnvVarEntry {
            name: "EMPTY_SECRET".to_string(),
            value: "".to_string(),
            secret: true,
            value_encrypted: None,
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
