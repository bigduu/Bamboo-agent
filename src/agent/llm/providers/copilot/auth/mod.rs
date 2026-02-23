//! Copilot Authentication Module
//!
//! Device Code Flow:
//! 1. Get device code from github.com/login/device/code
//! 2. User authorizes at github.com/login/device
//! 3. Poll for access token
//! 4. Exchange for copilot token
//! 5. Cache and use

pub mod cache;
pub mod device_code;
pub mod handler;
pub mod token;

pub use cache::TokenCache;
pub use device_code::{get_device_code, present_device_code, DeviceCodeResponse};
pub use handler::{CopilotAuthHandler, CopilotConfig};
pub use token::{get_copilot_token, poll_access_token, CopilotToken};
