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
