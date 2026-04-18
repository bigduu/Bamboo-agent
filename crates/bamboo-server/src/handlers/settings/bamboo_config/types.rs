use std::collections::BTreeMap;

use bamboo_infrastructure_config::ProxyAuth;
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
        let json = r#"{"username":"user","password":"pass"}"#;
        let payload: ProxyAuthPayload = serde_json::from_str(json).unwrap();

        assert_eq!(payload.username, Some("user".to_string()));
        assert_eq!(payload.password, Some("pass".to_string()));
    }

    #[test]
    fn test_proxy_auth_payload_only_username() {
        let json = r#"{"username":"user"}"#;
        let payload: ProxyAuthPayload = serde_json::from_str(json).unwrap();

        assert_eq!(payload.username, Some("user".to_string()));
        assert_eq!(payload.password, None);
    }

    #[test]
    fn test_proxy_auth_payload_empty() {
        let json = r#"{}"#;
        let payload: ProxyAuthPayload = serde_json::from_str(json).unwrap();

        assert_eq!(payload.username, None);
        assert_eq!(payload.password, None);
    }

    #[test]
    fn test_proxy_auth_payload_into_proxy_auth_valid() {
        let payload = ProxyAuthPayload {
            username: Some("user".to_string()),
            password: Some("pass".to_string()),
        };

        let auth = payload.into_proxy_auth();
        assert!(auth.is_some());

        let auth = auth.unwrap();
        assert_eq!(auth.username, "user");
        assert_eq!(auth.password, "pass");
    }

    #[test]
    fn test_proxy_auth_payload_into_proxy_auth_empty_username() {
        let payload = ProxyAuthPayload {
            username: Some("".to_string()),
            password: Some("pass".to_string()),
        };

        let auth = payload.into_proxy_auth();
        assert!(auth.is_none());
    }

    #[test]
    fn test_proxy_auth_payload_into_proxy_auth_whitespace_username() {
        let payload = ProxyAuthPayload {
            username: Some("   ".to_string()),
            password: Some("pass".to_string()),
        };

        let auth = payload.into_proxy_auth();
        assert!(auth.is_none());
    }

    #[test]
    fn test_proxy_auth_payload_into_proxy_auth_none_username() {
        let payload = ProxyAuthPayload {
            username: None,
            password: Some("pass".to_string()),
        };

        let auth = payload.into_proxy_auth();
        assert!(auth.is_none());
    }

    #[test]
    fn test_proxy_auth_payload_into_proxy_auth_no_password() {
        let payload = ProxyAuthPayload {
            username: Some("user".to_string()),
            password: None,
        };

        let auth = payload.into_proxy_auth().unwrap();
        assert_eq!(auth.username, "user");
        assert_eq!(auth.password, "");
    }

    #[test]
    fn test_proxy_auth_payload_debug() {
        let payload = ProxyAuthPayload {
            username: Some("user".to_string()),
            password: Some("pass".to_string()),
        };

        let debug_str = format!("{:?}", payload);
        assert!(debug_str.contains("ProxyAuthPayload"));
    }
}
