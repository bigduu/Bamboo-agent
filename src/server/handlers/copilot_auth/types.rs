use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct AuthStatus {
    pub authenticated: bool,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeviceCodeInfo {
    pub device_code: String,
    pub user_code: String,
    pub verification_uri: String,
    pub expires_in: u64,
    pub interval: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CompleteAuthRequest {
    pub device_code: String,
    pub interval: u64,
    pub expires_in: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_status_authenticated() {
        let status = AuthStatus {
            authenticated: true,
            message: None,
        };
        assert!(status.authenticated);
        assert!(status.message.is_none());
    }

    #[test]
    fn test_auth_status_with_message() {
        let status = AuthStatus {
            authenticated: false,
            message: Some("Token expired".to_string()),
        };
        assert!(!status.authenticated);
        assert_eq!(status.message, Some("Token expired".to_string()));
    }

    #[test]
    fn test_auth_status_serialization() {
        let status = AuthStatus {
            authenticated: true,
            message: Some("Success".to_string()),
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("authenticated"));
        assert!(json.contains("Success"));
    }

    #[test]
    fn test_auth_status_clone() {
        let status = AuthStatus {
            authenticated: true,
            message: Some("test".to_string()),
        };
        let cloned = status.clone();
        assert_eq!(status.authenticated, cloned.authenticated);
        assert_eq!(status.message, cloned.message);
    }

    #[test]
    fn test_auth_status_debug() {
        let status = AuthStatus {
            authenticated: true,
            message: None,
        };
        let debug_str = format!("{:?}", status);
        assert!(debug_str.contains("AuthStatus"));
    }

    #[test]
    fn test_auth_status_partial_eq() {
        let status1 = AuthStatus {
            authenticated: true,
            message: None,
        };
        let status2 = AuthStatus {
            authenticated: true,
            message: None,
        };
        assert_eq!(status1, status2);
    }

    #[test]
    fn test_device_code_info() {
        let info = DeviceCodeInfo {
            device_code: "device123".to_string(),
            user_code: "USER123".to_string(),
            verification_uri: "https://example.com/verify".to_string(),
            expires_in: 900,
            interval: 5,
        };
        assert_eq!(info.device_code, "device123");
        assert_eq!(info.user_code, "USER123");
        assert_eq!(info.verification_uri, "https://example.com/verify");
        assert_eq!(info.expires_in, 900);
        assert_eq!(info.interval, 5);
    }

    #[test]
    fn test_device_code_info_serialization() {
        let info = DeviceCodeInfo {
            device_code: "code".to_string(),
            user_code: "USER".to_string(),
            verification_uri: "https://test.com".to_string(),
            expires_in: 600,
            interval: 10,
        };
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("code"));
        assert!(json.contains("USER"));
        assert!(json.contains("https://test.com"));
    }

    #[test]
    fn test_device_code_info_clone() {
        let info = DeviceCodeInfo {
            device_code: "test".to_string(),
            user_code: "TEST".to_string(),
            verification_uri: "https://example.com".to_string(),
            expires_in: 300,
            interval: 5,
        };
        let cloned = info.clone();
        assert_eq!(info.device_code, cloned.device_code);
    }

    #[test]
    fn test_device_code_info_debug() {
        let info = DeviceCodeInfo {
            device_code: "dev".to_string(),
            user_code: "USR".to_string(),
            verification_uri: "https://uri".to_string(),
            expires_in: 100,
            interval: 1,
        };
        let debug_str = format!("{:?}", info);
        assert!(debug_str.contains("DeviceCodeInfo"));
    }

    #[test]
    fn test_complete_auth_request_deserialization() {
        let json = r#"{"device_code":"code123","interval":5,"expires_in":900}"#;
        let req: CompleteAuthRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.device_code, "code123");
        assert_eq!(req.interval, 5);
        assert_eq!(req.expires_in, 900);
    }

    #[test]
    fn test_complete_auth_request_debug() {
        let req = CompleteAuthRequest {
            device_code: "test".to_string(),
            interval: 10,
            expires_in: 600,
        };
        let debug_str = format!("{:?}", req);
        assert!(debug_str.contains("CompleteAuthRequest"));
    }

    #[test]
    fn test_complete_auth_request_clone() {
        let req = CompleteAuthRequest {
            device_code: "code".to_string(),
            interval: 5,
            expires_in: 300,
        };
        let cloned = req.clone();
        assert_eq!(req.device_code, cloned.device_code);
    }
}
