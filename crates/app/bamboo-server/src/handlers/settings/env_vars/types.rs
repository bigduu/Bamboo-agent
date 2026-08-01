use serde::{Deserialize, Serialize};

use crate::handlers::settings::credential_action::{
    credential_status_view, CredentialAction, CredentialState,
};

/// Request payload for creating or updating an environment variable.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpsertEnvVarRequest {
    pub expected_revision: u64,
    #[serde(flatten)]
    pub entry: EnvVarInput,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvVarInput {
    pub name: String,
    /// Missing keeps an existing value; `""` explicitly clears it.
    #[serde(default)]
    pub value: Option<String>,
    /// Explicit secret mutation. Omission retains the compatibility form:
    /// missing `value` keeps, nonempty `value` replaces, and `""` clears.
    #[serde(default)]
    pub(crate) credential_change: Option<CredentialAction>,
    #[serde(default)]
    pub secret: bool,
    #[serde(default)]
    pub description: Option<String>,
}

impl std::fmt::Debug for EnvVarInput {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EnvVarInput")
            .field("name", &self.name)
            .field("value", &self.value.as_ref().map(|_| "[REDACTED]"))
            .field("credential_change", &self.credential_change)
            .field("secret", &self.secret)
            .field("description", &self.description)
            .finish()
    }
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

/// Single env var in API response. Secret values are never serialized.
#[derive(Debug, Clone, Serialize)]
pub struct EnvVarResponse {
    pub name: String,
    /// Plaintext is returned only for non-secret entries. A secret entry omits
    /// this field entirely; masks are not part of the wire contract.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    pub secret: bool,
    /// Whether a real value is configured (useful for secret entries whose
    /// value is omitted).
    pub has_value: bool,
    /// Truthful configured status (kept alongside `has_value` for API
    /// compatibility).
    pub configured: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential_state: Option<CredentialState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<bamboo_config::CredentialSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Full list response.
#[derive(Debug, Clone, Serialize)]
pub struct EnvVarsListResponse {
    /// Env section revision to bind to the next write.
    pub revision: u64,
    pub entries: Vec<EnvVarResponse>,
    /// Exact typed Env section generation paired with this projection.
    pub section: bamboo_config::SectionEnvelope<serde_json::Value>,
    /// Internal credential-document health is diagnostic only; clients must
    /// never use its revision as the Env mutation precondition.
    pub credential_health: bamboo_config::CredentialStoreHealth,
}

impl EnvVarResponse {
    pub fn from_entry(
        entry: &bamboo_config::EnvVarEntry,
        status: Option<&bamboo_config::CredentialStatus>,
        health: &bamboo_config::CredentialStoreHealth,
    ) -> Self {
        let credential = entry.secret.then(|| {
            credential_status_view(
                entry.credential_ref.as_ref(),
                entry.configured,
                status,
                health,
            )
        });
        let has_value = credential
            .as_ref()
            .map(|credential| credential.configured)
            .unwrap_or_else(|| !entry.value.is_empty());
        Self {
            name: entry.name.clone(),
            value: (!entry.secret).then(|| entry.value.clone()),
            secret: entry.secret,
            has_value,
            configured: has_value,
            credential_state: credential.as_ref().map(|credential| credential.state),
            credential_ref: entry
                .credential_ref
                .as_ref()
                .map(|reference| reference.as_str().to_string()),
            source: credential.as_ref().and_then(|credential| credential.source),
            updated_at: credential
                .and_then(|credential| credential.updated_at)
                .map(|updated_at| updated_at.to_rfc3339()),
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
        let resp = EnvVarResponse::from_entry(
            &plain_entry(),
            None,
            &bamboo_config::CredentialStoreHealth::committed(0),
        );
        assert_eq!(resp.name, "NODE_ENV");
        assert_eq!(resp.value.as_deref(), Some("production"));
        assert!(!resp.secret);
        assert!(resp.has_value);
        assert_eq!(resp.description.as_deref(), Some("Node environment"));
    }

    #[test]
    fn from_entry_secret_omits_value() {
        let entry = secret_entry_with_value();
        let status = bamboo_config::CredentialStatus {
            credential_ref: entry.credential_ref.clone().unwrap(),
            configured: true,
            source: bamboo_config::CredentialSource::User,
            updated_at: None,
        };
        let resp = EnvVarResponse::from_entry(
            &entry,
            Some(&status),
            &bamboo_config::CredentialStoreHealth::committed(1),
        );
        assert_eq!(resp.name, "API_KEY");
        assert_eq!(resp.value, None);
        assert!(resp.secret);
        assert!(resp.has_value);
        assert_eq!(resp.credential_state, Some(CredentialState::Configured));
        assert_eq!(resp.source, Some(bamboo_config::CredentialSource::User));
    }

    #[test]
    fn from_entry_secret_without_value_shows_not_set() {
        let resp = EnvVarResponse::from_entry(
            &secret_entry_without_value(),
            None,
            &bamboo_config::CredentialStoreHealth::committed(0),
        );
        assert_eq!(resp.value, None);
        assert!(resp.secret);
        assert!(!resp.has_value);
        assert_eq!(resp.credential_state, Some(CredentialState::Missing));
    }

    #[test]
    fn from_entry_plain_empty_value() {
        let mut entry = plain_entry();
        entry.value = "".to_string();
        let resp = EnvVarResponse::from_entry(
            &entry,
            None,
            &bamboo_config::CredentialStoreHealth::committed(0),
        );
        assert_eq!(resp.value.as_deref(), Some(""));
        assert!(!resp.has_value);
    }
}
