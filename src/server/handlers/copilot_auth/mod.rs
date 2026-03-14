mod client;
mod legacy;
mod routes;
mod start_complete;
mod status_logout;
#[cfg(test)]
mod tests;
mod types;

pub use legacy::authenticate_copilot;
pub use routes::config;
pub use start_complete::{complete_copilot_auth, start_copilot_auth};
pub use status_logout::{get_copilot_auth_status, logout_copilot};
pub use types::{AuthStatus, CompleteAuthRequest, DeviceCodeInfo};
