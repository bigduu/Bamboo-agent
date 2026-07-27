use std::collections::BTreeMap;

use bamboo_config::ProxyAuth;
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
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProxyAuthPayload {
    /// Core section revision required for replace or clear.
    expected_revision: u64,
    /// Explicit mutation intent. Omission retains the legacy behavior where a
    /// nonempty username replaces and an empty username clears.
    #[serde(default)]
    action: Option<ProxyCredentialAction>,
    /// Proxy username.
    username: Option<String>,
    /// Proxy password.
    password: Option<String>,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProxyCredentialAction {
    Keep,
    Replace,
    Clear,
}

impl std::fmt::Debug for ProxyAuthPayload {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProxyAuthPayload")
            .field("expected_revision", &self.expected_revision)
            .field(
                "action",
                &self.action.map(|action| match action {
                    ProxyCredentialAction::Keep => "keep",
                    ProxyCredentialAction::Replace => "replace",
                    ProxyCredentialAction::Clear => "clear",
                }),
            )
            .field("username", &self.username.as_ref().map(|_| "[REDACTED]"))
            .field("password", &self.password.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

impl ProxyAuthPayload {
    pub(super) fn expected_revision(&self) -> u64 {
        self.expected_revision
    }

    pub(super) fn into_proxy_auth(self) -> Result<Option<ProxyAuth>, String> {
        let username = self.username.unwrap_or_default();
        let password = self.password.unwrap_or_default();
        let action = self.action.unwrap_or_else(|| {
            if username.trim().is_empty() {
                ProxyCredentialAction::Clear
            } else {
                ProxyCredentialAction::Replace
            }
        });
        match action {
            ProxyCredentialAction::Keep => {
                if !username.is_empty() || !password.is_empty() {
                    return Err("proxy keep must not include credential values".to_string());
                }
                Err("proxy keep is not a mutation; omit the request".to_string())
            }
            ProxyCredentialAction::Clear => {
                if !username.is_empty() || !password.is_empty() {
                    return Err("proxy clear must not include credential values".to_string());
                }
                Ok(None)
            }
            ProxyCredentialAction::Replace => {
                if username.trim().is_empty() {
                    return Err("proxy replace requires a username".to_string());
                }
                if bamboo_config::patch::is_masked_api_key(&username)
                    || bamboo_config::patch::is_masked_api_key(&password)
                {
                    return Err("proxy credential value must not be a mask".to_string());
                }
                Ok(Some(ProxyAuth { username, password }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_issue_serialization() {
        let issue = ValidationIssue {
            path: "config.api_key".to_string(),
            message: "API key is required".to_string(),
        };

        let json = serde_json::to_string(&issue).unwrap();
        assert!(json.contains("config.api_key"));
        assert!(json.contains("API key is required"));
    }

    #[test]
    fn test_validate_config_response_valid() {
        let response = ValidateConfigResponse {
            valid: true,
            errors: BTreeMap::new(),
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"valid\":true"));
    }

    #[test]
    fn test_validate_config_response_with_errors() {
        let mut errors = BTreeMap::new();
        errors.insert(
            "provider".to_string(),
            vec![ValidationIssue {
                path: "provider.api_key".to_string(),
                message: "Missing API key".to_string(),
            }],
        );

        let response = ValidateConfigResponse {
            valid: false,
            errors,
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"valid\":false"));
        assert!(json.contains("provider"));
    }

    #[test]
    fn test_proxy_auth_payload_deserialization() {
        let json = r#"{"expected_revision":4,"username":"user","password":"pass"}"#;
        let payload: ProxyAuthPayload = serde_json::from_str(json).unwrap();

        assert_eq!(payload.username, Some("user".to_string()));
        assert_eq!(payload.password, Some("pass".to_string()));
        assert_eq!(payload.expected_revision(), 4);
    }

    #[test]
    fn test_proxy_auth_payload_only_username() {
        let json = r#"{"expected_revision":0,"username":"user"}"#;
        let payload: ProxyAuthPayload = serde_json::from_str(json).unwrap();

        assert_eq!(payload.username, Some("user".to_string()));
        assert_eq!(payload.password, None);
    }

    #[test]
    fn test_proxy_auth_payload_empty() {
        let json = r#"{"expected_revision":0}"#;
        let payload: ProxyAuthPayload = serde_json::from_str(json).unwrap();

        assert_eq!(payload.username, None);
        assert_eq!(payload.password, None);
    }

    #[test]
    fn test_proxy_auth_payload_requires_revision_precondition() {
        let error =
            serde_json::from_str::<ProxyAuthPayload>(r#"{"username":"user","password":"pass"}"#)
                .unwrap_err();
        assert!(error.to_string().contains("expected_revision"));
    }

    #[test]
    fn test_proxy_auth_payload_into_proxy_auth_valid() {
        let payload = ProxyAuthPayload {
            expected_revision: 0,
            action: None,
            username: Some("user".to_string()),
            password: Some("pass".to_string()),
        };

        let auth = payload.into_proxy_auth().unwrap().unwrap();
        assert_eq!(auth.username, "user");
        assert_eq!(auth.password, "pass");
    }

    #[test]
    fn test_proxy_auth_payload_into_proxy_auth_empty_username() {
        let payload = ProxyAuthPayload {
            expected_revision: 0,
            action: None,
            username: Some("".to_string()),
            password: Some("pass".to_string()),
        };

        assert!(payload.into_proxy_auth().is_err());
    }

    #[test]
    fn test_proxy_auth_payload_into_proxy_auth_whitespace_username() {
        let payload = ProxyAuthPayload {
            expected_revision: 0,
            action: None,
            username: Some("   ".to_string()),
            password: Some("pass".to_string()),
        };

        assert!(payload.into_proxy_auth().is_err());
    }

    #[test]
    fn test_proxy_auth_payload_into_proxy_auth_none_username() {
        let payload = ProxyAuthPayload {
            expected_revision: 0,
            action: None,
            username: None,
            password: Some("pass".to_string()),
        };

        assert!(payload.into_proxy_auth().is_err());
    }

    #[test]
    fn test_proxy_auth_payload_into_proxy_auth_no_password() {
        let payload = ProxyAuthPayload {
            expected_revision: 0,
            action: None,
            username: Some("user".to_string()),
            password: None,
        };

        let auth = payload.into_proxy_auth().unwrap().unwrap();
        assert_eq!(auth.username, "user");
        assert_eq!(auth.password, "");
    }

    #[test]
    fn test_proxy_auth_payload_debug() {
        let payload = ProxyAuthPayload {
            expected_revision: 0,
            action: None,
            username: Some("user".to_string()),
            password: Some("pass".to_string()),
        };

        let debug_str = format!("{:?}", payload);
        assert!(debug_str.contains("ProxyAuthPayload"));
        assert!(debug_str.contains("[REDACTED]"));
        assert!(!debug_str.contains("Some(\"user\")"));
        assert!(!debug_str.contains("Some(\"pass\")"));
    }
}
