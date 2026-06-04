//! GitHub Copilot authentication handler.
//!
//! This module provides authentication handling for GitHub Copilot,
//! including device code flow, token caching, and automatic refresh.
//!
//! # Authentication Flow
//!
//! The authentication process follows GitHub's OAuth device flow:
//!
//! 1. **Device Code Request**: The client requests a device code from GitHub
//! 2. **User Authorization**: The user visits the verification URL and enters the code
//! 3. **Token Polling**: The client polls GitHub for an access token
//! 4. **Copilot Token Exchange**: The access token is exchanged for a Copilot API token
//! 5. **Token Caching**: Tokens are cached locally for future use
//!
//! # Example Usage
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use reqwest_middleware::ClientWithMiddleware;
//! use bamboo_agent::agent::llm::providers::copilot::auth::handler::CopilotAuthHandler;
//!
//! async fn authenticate() -> anyhow::Result<String> {
//!     // Create HTTP client with middleware
//!     let client = Arc::new(ClientWithMiddleware::new(/* ... */));
//!
//!     // Create auth handler with data directory
//!     let handler = CopilotAuthHandler::new(
//!         client,
//!         std::path::PathBuf::from("/path/to/bamboo-data-dir"),
//!         false, // Set to true for CLI mode
//!     );
//!
//!     // Get token (will trigger device flow if needed)
//!     let token = handler.get_token().await?;
//!     Ok(token)
//! }
//! ```
//!
//! # Token Caching Strategy
//!
//! The handler implements a multi-level token caching strategy:
//!
//! 1. **Copilot Token Cache**: Checks `.copilot_token.json` for valid tokens
//! 2. **Environment Variable**: Falls back to `COPILOT_API_KEY` if set
//! 3. **Access Token Cache**: Uses cached GitHub access token to request new Copilot token
//! 4. **Interactive Flow**: Only triggers device flow if all silent methods fail
//!
//! # Token Validation
//!
//! Tokens are validated with a 60-second buffer to ensure they don't expire
//! during use. This proactive refresh ensures seamless operation.

use crate::ProxyAuthRequiredError;
use anyhow::anyhow;
use reqwest::StatusCode;
use reqwest_middleware::ClientWithMiddleware;
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;
use std::{
    fs::{read_to_string, File},
    io::Write,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::error;

use super::device_code::DeviceCodeResponse;

/// Copilot API configuration returned from GitHub.
///
/// Contains the authentication token, feature flags, and endpoint URLs
/// for the Copilot service.
///
/// This configuration is obtained by exchanging a GitHub access token
/// for a Copilot-specific token via the `/copilot_internal/v2/token` endpoint.
///
/// # Fields
///
/// - `token`: The Copilot API token used for authentication
/// - `expires_at`: Unix timestamp when the token expires
/// - `refresh_in`: Suggested refresh interval in seconds
/// - `endpoints`: API endpoints for Copilot services
/// - Various feature flags controlling available functionality
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CopilotConfig {
    pub token: String,
    #[serde(default)]
    pub annotations_enabled: bool,
    #[serde(default)]
    pub chat_enabled: bool,
    #[serde(default)]
    pub chat_jetbrains_enabled: bool,
    #[serde(default)]
    pub code_quote_enabled: bool,
    #[serde(default)]
    pub code_review_enabled: bool,
    #[serde(default)]
    pub codesearch: bool,
    #[serde(default)]
    pub copilotignore_enabled: bool,
    #[serde(default)]
    pub endpoints: Endpoints,
    pub expires_at: u64,
    #[serde(default)]
    pub individual: bool,
    pub limited_user_quotas: Option<String>,
    pub limited_user_reset_date: Option<String>,
    #[serde(default)]
    pub prompt_8k: bool,
    #[serde(default)]
    pub public_suggestions: String,
    pub refresh_in: u64,
    #[serde(default)]
    pub sku: String,
    #[serde(default)]
    pub snippy_load_test_enabled: bool,
    #[serde(default)]
    pub telemetry: String,
    #[serde(default)]
    pub tracking_id: String,
    #[serde(default)]
    pub vsc_electron_fetcher_v2: bool,
    #[serde(default)]
    pub xcode: bool,
    #[serde(default)]
    pub xcode_chat: bool,
    #[serde(default, flatten)]
    pub extra: std::collections::HashMap<String, serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// Creates a test HTTP client without proxy for unit tests.
    fn test_http_client() -> Arc<ClientWithMiddleware> {
        use reqwest::Client as ReqwestClient;
        use reqwest_middleware::ClientBuilder;
        let client = ReqwestClient::builder().no_proxy().build().expect("client");
        Arc::new(ClientBuilder::new(client).build())
    }

    /// Creates a sample CopilotConfig for testing with specified expiration time.
    fn sample_config(expires_at: u64) -> CopilotConfig {
        CopilotConfig {
            token: "cached-token".to_string(),
            annotations_enabled: false,
            chat_enabled: true,
            chat_jetbrains_enabled: false,
            code_quote_enabled: false,
            code_review_enabled: false,
            codesearch: false,
            copilotignore_enabled: false,
            endpoints: Endpoints {
                api: Some("https://api.example.com".to_string()),
                origin_tracker: None,
                proxy: None,
                telemetry: None,
                extra: Default::default(),
            },
            expires_at,
            individual: true,
            limited_user_quotas: None,
            limited_user_reset_date: None,
            prompt_8k: false,
            public_suggestions: "disabled".to_string(),
            refresh_in: 300,
            sku: "test".to_string(),
            snippy_load_test_enabled: false,
            telemetry: "disabled".to_string(),
            tracking_id: "test".to_string(),
            vsc_electron_fetcher_v2: false,
            xcode: false,
            xcode_chat: false,
            extra: Default::default(),
        }
    }

    /// Tests that read_access_token properly trims whitespace and newlines.
    #[test]
    fn read_access_token_trims() {
        let dir = tempdir().expect("tempdir");
        let token_path = dir.path().join(".token");
        std::fs::write(&token_path, "  token-value \n").expect("write token");

        let token = CopilotAuthHandler::read_access_token(&token_path);
        assert_eq!(token.as_deref(), Some("token-value"));
    }

    /// Tests that CopilotConfig can be written to and read from cache.
    #[test]
    fn cached_copilot_config_round_trip() {
        let dir = tempdir().expect("tempdir");
        let handler = CopilotAuthHandler::new(test_http_client(), dir.path().to_path_buf(), false);
        let token_path = dir.path().join(".copilot_token.json");
        let config = sample_config(1234567890);

        handler
            .write_cached_copilot_config(&token_path, &config)
            .expect("write cache");
        let loaded = handler
            .read_cached_copilot_config(&token_path)
            .expect("read cache");

        assert_eq!(loaded.token, config.token);
        assert_eq!(loaded.expires_at, config.expires_at);
    }

    /// Tests that token validation uses a 60-second buffer.
    ///
    /// Tokens expiring within 60 seconds should be considered invalid
    /// to ensure proactive refresh.
    #[test]
    fn copilot_token_expiry_buffer() {
        let dir = tempdir().expect("tempdir");
        let handler = CopilotAuthHandler::new(test_http_client(), dir.path().to_path_buf(), false);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or(0);

        let valid = sample_config(now + 120);
        let stale = sample_config(now + 30);

        assert!(handler.is_copilot_token_valid(&valid));
        assert!(!handler.is_copilot_token_valid(&stale));
    }

    #[test]
    fn access_token_should_only_be_discarded_on_auth_errors() {
        let err_401 =
            anyhow::Error::msg("Copilot token request failed: HTTP 401 - bad credentials");
        assert!(CopilotAuthHandler::should_discard_access_token(&err_401));

        let err_403 = anyhow::Error::msg("Copilot token request failed: HTTP 403 - forbidden");
        assert!(CopilotAuthHandler::should_discard_access_token(&err_403));

        let err_407 = anyhow::Error::new(ProxyAuthRequiredError);
        assert!(!CopilotAuthHandler::should_discard_access_token(&err_407));

        let err_503 =
            anyhow::Error::msg("Copilot token request failed: HTTP 503 - service unavailable");
        assert!(!CopilotAuthHandler::should_discard_access_token(&err_503));
    }
}

/// API endpoint configuration for Copilot services.
///
/// Contains URLs for various Copilot API endpoints returned during
/// the token exchange process.
///
/// # Fields
///
/// - `api`: Primary API endpoint for Copilot requests
/// - `origin_tracker`: Endpoint for tracking request origins
/// - `proxy`: Proxy endpoint for proxied requests
/// - `telemetry`: Endpoint for sending telemetry data
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Endpoints {
    pub api: Option<String>,
    #[serde(rename = "origin-tracker")]
    pub origin_tracker: Option<String>,
    pub proxy: Option<String>,
    pub telemetry: Option<String>,
    #[serde(default, flatten)]
    pub extra: std::collections::HashMap<String, serde_json::Value>,
}

/// Access token response from GitHub OAuth.
///
/// Contains the access token or error information from the OAuth device flow.
/// This is the response from GitHub's `/login/oauth/access_token` endpoint
/// when polling for authorization completion.
///
/// # Fields
///
/// - `access_token`: The OAuth access token on successful authorization
/// - `token_type`: Token type (typically "bearer")
/// - `scope`: OAuth scopes granted to the token
/// - `error`: Error code if authorization failed or is pending
/// - `error_description`: Human-readable error description
#[derive(Debug, Deserialize)]
pub(crate) struct AccessTokenResponse {
    /// The OAuth access token (present on successful authorization)
    pub access_token: Option<String>,
    /// Token type (typically "bearer")
    #[allow(dead_code)] // Needed for JSON deserialization from GitHub API
    pub token_type: Option<String>,
    /// OAuth scopes granted to this token
    #[allow(dead_code)] // Needed for JSON deserialization from GitHub API
    pub scope: Option<String>,
    /// Error code (e.g., "authorization_pending", "slow_down", "expired_token")
    pub error: Option<String>,
    /// Human-readable error description
    #[serde(rename = "error_description")]
    pub error_description: Option<String>,
}

impl AccessTokenResponse {
    /// Creates a new access token response from a token string.
    ///
    /// This is a convenience constructor for creating an `AccessTokenResponse`
    /// from a previously cached token string.
    ///
    /// # Arguments
    ///
    /// * `token` - The access token string
    ///
    /// # Example
    ///
    /// ```ignore
    /// use bamboo_agent::agent::llm::providers::copilot::auth::handler::AccessTokenResponse;
    ///
    /// let response = AccessTokenResponse::from_token("gho_xxxx".to_string());
    /// assert_eq!(response.access_token, Some("gho_xxxx".to_string()));
    /// ```
    pub(crate) fn from_token(token: String) -> Self {
        Self {
            access_token: Some(token),
            token_type: None,
            scope: None,
            error: None,
            error_description: None,
        }
    }
}

// Global lock for chat token operations.
//
// This mutex ensures that only one token request can be in flight at a time
// across the entire application. This prevents race conditions where multiple
// concurrent requests could trigger separate authentication flows.
//
// The lock is acquired in `CopilotAuthHandler::get_chat_token` before
// attempting silent authentication or starting a new device flow.
static CHAT_TOKEN_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// Handler for GitHub Copilot authentication.
///
/// Manages the complete authentication lifecycle including:
/// - Device code flow for initial authentication
/// - Token caching and validation
/// - Automatic token refresh
/// - Silent authentication attempts
///
/// # Architecture
///
/// The handler implements a hierarchical token resolution strategy:
///
/// 1. **Cached Copilot Token**: Check local cache for valid token
/// 2. **Environment Variable**: Check `COPILOT_API_KEY`
/// 3. **Cached Access Token**: Use cached GitHub token to fetch new Copilot token
/// 4. **Interactive Flow**: Prompt user via device code flow
///
/// # Thread Safety
///
/// The handler is thread-safe and can be cloned cheaply. Authentication
/// operations are protected by a global lock to prevent concurrent flows.
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use reqwest_middleware::ClientWithMiddleware;
/// use bamboo_agent::agent::llm::providers::copilot::auth::handler::CopilotAuthHandler;
///
/// async fn example() -> anyhow::Result<()> {
///     let client = Arc::new(ClientWithMiddleware::new(/* ... */));
///     let handler = CopilotAuthHandler::new(
///         client,
///         std::path::PathBuf::from("/path/to/bamboo-data-dir"),
///         false,
///     );
///
///     // Will use cached token or trigger device flow
///     let token = handler.get_token().await?;
///     println!("Got token: {}", token);
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone)]
pub struct CopilotAuthHandler {
    /// HTTP client with middleware for retry logic
    client: Arc<ClientWithMiddleware>,
    /// Directory for storing cached tokens
    app_data_dir: PathBuf,
    /// Whether to print authentication instructions to console
    headless_auth: bool,
    /// GitHub API base URL (customizable for testing)
    github_api_base_url: String,
    /// GitHub login base URL (customizable for testing)
    github_login_base_url: String,
}

impl CopilotAuthHandler {
    /// Creates a new authentication handler.
    ///
    /// # Arguments
    ///
    /// * `client` - HTTP client with middleware for retry logic and error handling
    /// * `app_data_dir` - Directory for storing cached tokens (`.token` and `.copilot_token.json`)
    /// * `headless_auth` - Whether to print authentication instructions to console.
    ///   Set to `true` for CLI applications, `false` for GUI applications.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use std::sync::Arc;
    /// use reqwest_middleware::ClientWithMiddleware;
    /// use bamboo_agent::agent::llm::providers::copilot::auth::handler::CopilotAuthHandler;
    ///
    /// let client = Arc::new(ClientWithMiddleware::new(/* ... */));
    /// let handler = CopilotAuthHandler::new(
    ///     client,
    ///     std::path::PathBuf::from("/path/to/bamboo-data-dir"),
    ///     true, // CLI mode
    /// );
    /// ```
    pub fn new(
        client: Arc<ClientWithMiddleware>,
        app_data_dir: PathBuf,
        headless_auth: bool,
    ) -> Self {
        CopilotAuthHandler {
            client,
            app_data_dir,
            headless_auth,
            github_api_base_url: "https://api.github.com".to_string(),
            github_login_base_url: "https://github.com".to_string(),
        }
    }

    /// Returns the application data directory path.
    ///
    /// This directory contains cached tokens:
    /// - `.token`: GitHub OAuth access token
    /// - `.copilot_token.json`: Copilot API configuration
    pub fn app_data_dir(&self) -> &PathBuf {
        &self.app_data_dir
    }

    /// Sets a custom GitHub API base URL for testing.
    ///
    /// This allows tests to mock GitHub's API without hitting production.
    #[cfg(test)]
    fn with_github_api_base_url(mut self, url: impl Into<String>) -> Self {
        self.github_api_base_url = url.into();
        self
    }

    /// Sets a custom GitHub login base URL for testing.
    ///
    /// This allows tests to mock GitHub's OAuth endpoints without hitting production.
    #[cfg(test)]
    fn with_github_login_base_url(mut self, url: impl Into<String>) -> Self {
        self.github_login_base_url = url.into();
        self
    }

    /// Performs authentication and returns an access token.
    ///
    /// This is the primary entry point for authentication. It will attempt
    /// silent authentication first, then fall back to interactive device flow
    /// if necessary.
    ///
    /// # Returns
    ///
    /// A Copilot API token on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - All authentication methods fail
    /// - User denies authorization during device flow
    /// - Device code expires before authorization
    /// - Network errors occur
    pub async fn authenticate(&self) -> anyhow::Result<String> {
        self.get_chat_token().await
    }

    /// Ensures the handler is authenticated, without returning the token.
    ///
    /// This is useful for pre-authenticating or verifying credentials
    /// without needing the actual token value.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// # use bamboo_agent::agent::llm::providers::copilot::auth::handler::CopilotAuthHandler;
    /// # async fn example(handler: CopilotAuthHandler) -> anyhow::Result<()> {
    /// // Pre-authenticate before starting the application
    /// handler.ensure_authenticated().await?;
    /// println!("Authentication successful!");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn ensure_authenticated(&self) -> anyhow::Result<()> {
        self.get_chat_token().await.map(|_| ())
    }

    /// Gets the current access token, authenticating if necessary.
    ///
    /// Alias for [`authenticate`](Self::authenticate).
    pub async fn get_token(&self) -> anyhow::Result<String> {
        self.get_chat_token().await
    }

    /// Gets a chat token, using cached credentials or triggering device flow.
    ///
    /// This method attempts silent authentication first, then falls back
    /// to interactive device code flow if necessary.
    ///
    /// # Silent Authentication Priority
    ///
    /// 1. Check cached Copilot token (`.copilot_token.json`)
    /// 2. Check `COPILOT_API_KEY` environment variable
    /// 3. Check cached GitHub access token (`.token`) and exchange for new Copilot token
    ///
    /// # Thread Safety
    ///
    /// This method acquires a global lock to prevent concurrent authentication
    /// flows. Only one authentication attempt can be in progress at a time.
    ///
    /// # Returns
    ///
    /// A valid Copilot API token.
    ///
    /// # Errors
    ///
    /// Returns an error if all authentication methods fail.
    pub async fn get_chat_token(&self) -> anyhow::Result<String> {
        // Acquire global lock to ensure sequential execution
        let _guard = CHAT_TOKEN_LOCK.lock().await;

        // Try silent authentication first
        if let Some(token) = self.try_get_chat_token_silent().await? {
            return Ok(token);
        }

        // Need interactive authentication
        let device_code = self.start_authentication().await?;
        let copilot_config = self.complete_authentication(&device_code).await?;
        Ok(copilot_config.token)
    }

    /// Reads an access token from a file, trimming whitespace.
    ///
    /// This utility function reads a token from a file and trims any
    /// leading/trailing whitespace or newlines.
    ///
    /// # Arguments
    ///
    /// * `token_path` - Path to the token file
    ///
    /// # Returns
    ///
    /// - `Some(token)` if the file exists and contains non-whitespace content
    /// - `None` if the file doesn't exist or is empty/whitespace only
    fn read_access_token(token_path: &PathBuf) -> Option<String> {
        if !token_path.exists() {
            return None;
        }
        let access_token_str = read_to_string(token_path).ok()?;
        let trimmed = access_token_str.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    }

    /// Reads a cached Copilot configuration from a file.
    ///
    /// Attempts to deserialize a JSON-formatted Copilot configuration
    /// from the specified file.
    ///
    /// # Arguments
    ///
    /// * `token_path` - Path to the JSON cache file
    ///
    /// # Returns
    ///
    /// - `Some(config)` if the file exists and contains valid JSON
    /// - `None` if the file doesn't exist or has invalid JSON
    fn read_cached_copilot_config(&self, token_path: &PathBuf) -> Option<CopilotConfig> {
        let cached_str = read_to_string(token_path).ok()?;
        serde_json::from_str::<CopilotConfig>(&cached_str).ok()
    }

    /// Writes a Copilot configuration to a cache file.
    ///
    /// Serializes the configuration as JSON and writes it to the specified file.
    ///
    /// # Arguments
    ///
    /// * `token_path` - Path where the JSON should be written
    /// * `copilot_config` - Configuration to cache
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - JSON serialization fails
    /// - File creation fails
    /// - Writing to file fails
    fn write_cached_copilot_config(
        &self,
        token_path: &PathBuf,
        copilot_config: &CopilotConfig,
    ) -> anyhow::Result<()> {
        let serialized = serde_json::to_string(copilot_config)?;
        let mut file = File::create(token_path)?;
        file.write_all(serialized.as_bytes())?;
        Ok(())
    }

    /// Checks if a Copilot token is valid with a 60-second buffer.
    ///
    /// This method checks whether the token has expired, with a 60-second
    /// buffer to ensure tokens are refreshed before they actually expire.
    ///
    /// # Arguments
    ///
    /// * `copilot_config` - Configuration containing the token expiration time
    ///
    /// # Returns
    ///
    /// - `true` if the token is valid for at least 60 more seconds
    /// - `false` if the token has expired or will expire within 60 seconds
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// # use bamboo_agent::agent::llm::providers::copilot::auth::handler::{CopilotAuthHandler, CopilotConfig};
    /// # fn example(handler: CopilotAuthHandler, config: CopilotConfig) {
    /// if handler.is_copilot_token_valid(&config) {
    ///     println!("Token is valid");
    /// } else {
    ///     println!("Token needs refresh");
    /// }
    /// # }
    /// ```
    fn is_copilot_token_valid(&self, copilot_config: &CopilotConfig) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or(0);
        copilot_config.expires_at.saturating_sub(60) > now
    }

    /// Requests a device code from GitHub for OAuth flow.
    ///
    /// This is the first step in the OAuth device flow. It requests a
    /// device code and user code from GitHub that the user must enter
    /// at the verification URL.
    ///
    /// # Returns
    ///
    /// A [`DeviceCodeResponse`] containing:
    /// - `device_code`: Unique identifier for this authentication session
    /// - `user_code`: Code the user must enter at the verification URL
    /// - `verification_uri`: URL where user should enter the code
    /// - `expires_in`: Seconds until the device code expires
    /// - `interval`: Recommended polling interval in seconds
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - GitHub API is unreachable
    /// - Proxy authentication is required
    /// - API returns an error response
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// # use bamboo_agent::agent::llm::providers::copilot::auth::handler::CopilotAuthHandler;
    /// # async fn example(handler: CopilotAuthHandler) -> anyhow::Result<()> {
    /// let device_code = handler.get_device_code().await?;
    /// println!("Visit: {}", device_code.verification_uri);
    /// println!("Enter code: {}", device_code.user_code);
    /// # Ok(())
    /// # }
    /// ```
    pub(super) async fn get_device_code(&self) -> anyhow::Result<DeviceCodeResponse> {
        let params = [
            ("client_id", "Iv1.b507a08c87ecfe98"),
            ("scope", "read:user"),
        ];
        let url = format!("{}/login/device/code", self.github_login_base_url);

        let response = self
            .client
            .post(&url)
            .header("Accept", "application/json")
            .header("User-Agent", "BambooCopilot/1.0")
            .form(&params)
            .send()
            .await?;

        if response.status() == StatusCode::PROXY_AUTHENTICATION_REQUIRED {
            return Err(anyhow!(ProxyAuthRequiredError));
        }

        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Device code request failed: HTTP {} - {}",
                status,
                text
            ));
        }

        Ok(response.json::<DeviceCodeResponse>().await?)
    }

    /// Starts the authentication process by getting a device code.
    ///
    /// This method initiates the OAuth device flow by requesting a device
    /// code from GitHub. If `headless_auth` is `false`, it prints user-friendly
    /// instructions to the console.
    ///
    /// # Display Behavior
    ///
    /// - **Headless mode (`headless_auth = true`)**: Prints full instructions with ASCII art
    /// - **GUI mode (`headless_auth = false`)**: Returns device code for custom UI
    ///
    /// # Returns
    ///
    /// A [`DeviceCodeResponse`] with the device code and verification information.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// # use bamboo_agent::agent::llm::providers::copilot::auth::handler::CopilotAuthHandler;
    /// # async fn example(handler: CopilotAuthHandler) -> anyhow::Result<()> {
    /// let device_code = handler.start_authentication().await?;
    /// // In GUI mode, display these values to the user
    /// println!("URL: {}", device_code.verification_uri);
    /// println!("Code: {}", device_code.user_code);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn start_authentication(&self) -> anyhow::Result<DeviceCodeResponse> {
        let device_code = self.get_device_code().await?;

        if self.headless_auth {
            // CLI mode: print to console
            println!("\n╔════════════════════════════════════════════════════════════╗");
            println!("║     🔐 GitHub Copilot Authorization Required              ║");
            println!("╚════════════════════════════════════════════════════════════╝");
            println!();
            println!("  1. Open your browser and navigate to:");
            println!("     {}", device_code.verification_uri);
            println!();
            println!("  2. Enter the following code:");
            println!();
            println!("     ┌─────────────────────────┐");
            println!("     │  {:^23} │", device_code.user_code);
            println!("     └─────────────────────────┘");
            println!();
            println!("  3. Click 'Authorize' and wait...");
            println!();
            println!(
                "  ⏳ Waiting for authorization (expires in {} seconds)...",
                device_code.expires_in
            );
            println!();
        }

        Ok(device_code)
    }

    /// Completes authentication by polling for access token and exchanging for Copilot token.
    ///
    /// This method completes the OAuth flow by:
    /// 1. Polling GitHub for the access token (waits for user authorization)
    /// 2. Exchanging the access token for a Copilot API token
    /// 3. Caching both tokens to disk for future use
    ///
    /// # Arguments
    ///
    /// * `device_code` - Device code response from [`start_authentication`](Self::start_authentication)
    ///
    /// # Returns
    ///
    /// A [`CopilotConfig`] containing the Copilot API token and configuration.
    ///
    /// # Side Effects
    ///
    /// Writes the following files to `app_data_dir`:
    /// - `.token`: GitHub OAuth access token
    /// - `.copilot_token.json`: Copilot API configuration
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - User denies authorization
    /// - Device code expires before authorization
    /// - Token exchange fails
    /// - File writing fails
    pub async fn complete_authentication(
        &self,
        device_code: &DeviceCodeResponse,
    ) -> anyhow::Result<CopilotConfig> {
        let access_token = self.get_access_token(device_code).await?;

        // Extract access token string before passing to get_copilot_token
        let access_token_str = access_token
            .access_token
            .clone()
            .ok_or_else(|| anyhow!("Access token not found"))?;

        let copilot_config = self.get_copilot_token(access_token).await?;

        // Write tokens to disk
        let token_path = self.app_data_dir.join(".token");
        let copilot_token_path = self.app_data_dir.join(".copilot_token.json");

        // Write access token
        let mut file = File::create(&token_path)?;
        file.write_all(access_token_str.as_bytes())?;

        // Write copilot config
        self.write_cached_copilot_config(&copilot_token_path, &copilot_config)?;

        Ok(copilot_config)
    }

    /// Attempts silent authentication without user interaction.
    ///
    /// This method tries multiple authentication strategies in order of preference,
    /// all of which can succeed without requiring user interaction:
    ///
    /// 1. **Cached Copilot Token**: Load from `.copilot_token.json` if still valid
    /// 2. **Environment Variable**: Check `COPILOT_API_KEY`
    /// 3. **Cached Access Token**: Use cached GitHub token to fetch new Copilot token
    ///
    /// # Returns
    ///
    /// - `Ok(Some(token))` if silent authentication succeeded
    /// - `Ok(None)` if silent authentication is not possible (triggers interactive flow)
    /// - `Err(...)` if an unexpected error occurred
    ///
    /// # Side Effects
    ///
    /// If using a cached access token, this method will:
    /// - Fetch a new Copilot token from GitHub
    /// - Cache the new Copilot token to `.copilot_token.json`
    /// - Remove the cached access token if it's invalid
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// # use bamboo_agent::agent::llm::providers::copilot::auth::handler::CopilotAuthHandler;
    /// # async fn example(handler: CopilotAuthHandler) -> anyhow::Result<()> {
    /// match handler.try_get_chat_token_silent().await? {
    ///     Some(token) => println!("Got token silently: {}", token),
    ///     None => println!("Need interactive authentication"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn try_get_chat_token_silent(&self) -> anyhow::Result<Option<String>> {
        let copilot_token_path = self.app_data_dir.join(".copilot_token.json");

        // Check cached copilot token
        if let Some(cached_config) = self.read_cached_copilot_config(&copilot_token_path) {
            if self.is_copilot_token_valid(&cached_config) {
                return Ok(Some(cached_config.token));
            }
        }

        // Check env var
        if let Ok(token) = std::env::var("COPILOT_API_KEY") {
            let trimmed = token.trim();
            if !trimmed.is_empty() {
                return Ok(Some(trimmed.to_string()));
            }
        }

        // Check access token file and try to exchange
        let token_path = self.app_data_dir.join(".token");
        if let Some(access_token_str) = Self::read_access_token(&token_path) {
            let access_token = AccessTokenResponse::from_token(access_token_str);
            match self.get_copilot_token(access_token).await {
                Ok(copilot_config) => {
                    self.write_cached_copilot_config(&copilot_token_path, &copilot_config)?;
                    return Ok(Some(copilot_config.token));
                }
                Err(e) => {
                    // Only discard the cached access token when we are confident it is invalid.
                    // Copilot tokens are short-lived; the GitHub OAuth access token should be
                    // long-lived, so removing it on transient failures causes unnecessary re-auth.
                    if Self::should_discard_access_token(&e) {
                        let _ = std::fs::remove_file(&token_path);
                    }
                }
            }
        }

        Ok(None)
    }

    /// Force refresh a Copilot token using the cached GitHub OAuth access token.
    ///
    /// This bypasses the `.copilot_token.json` cache and is useful when the cached
    /// Copilot token is rejected early (e.g. revoked) even if it hasn't reached
    /// `expires_at` yet.
    ///
    /// Returns:
    /// - `Ok(Some(token))` if the refresh succeeded
    /// - `Ok(None)` if no cached access token exists
    pub async fn force_refresh_chat_token(&self) -> anyhow::Result<Option<String>> {
        let token_path = self.app_data_dir.join(".token");
        let Some(access_token_str) = Self::read_access_token(&token_path) else {
            return Ok(None);
        };

        let access_token = AccessTokenResponse::from_token(access_token_str);
        match self.get_copilot_token(access_token).await {
            Ok(copilot_config) => {
                let copilot_token_path = self.app_data_dir.join(".copilot_token.json");
                self.write_cached_copilot_config(&copilot_token_path, &copilot_config)?;
                Ok(Some(copilot_config.token))
            }
            Err(e) => {
                if Self::should_discard_access_token(&e) {
                    let _ = std::fs::remove_file(&token_path);
                }
                Err(e)
            }
        }
    }

    fn should_discard_access_token_message(msg: &str) -> bool {
        // get_copilot_token formats errors like:
        // "Copilot token request failed: HTTP {status} - {text}"
        msg.contains("HTTP 401") || msg.contains("HTTP 403")
    }

    fn should_discard_access_token(err: &anyhow::Error) -> bool {
        if err.downcast_ref::<ProxyAuthRequiredError>().is_some() {
            return false;
        }
        Self::should_discard_access_token_message(&err.to_string())
    }

    /// Polls GitHub for an access token after user completes device flow.
    ///
    /// This method continuously polls GitHub's OAuth endpoint until either:
    /// - The user authorizes the application (success)
    /// - The device code expires (error)
    /// - The user denies authorization (error)
    ///
    /// # Polling Behavior
    ///
    /// The method polls at the interval specified in the device code response
    /// (minimum 5 seconds). It handles various OAuth states:
    ///
    /// - `authorization_pending`: User hasn't authorized yet, keep polling
    /// - `slow_down`: Server requested slower polling, increase interval
    /// - `expired_token`: Device code expired, return error
    /// - `access_denied`: User denied authorization, return error
    ///
    /// # Arguments
    ///
    /// * `device_code` - Device code response from [`get_device_code`](Self::get_device_code)
    ///
    /// # Returns
    ///
    /// An [`AccessTokenResponse`] containing the GitHub OAuth access token.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Device code expires before user authorizes
    /// - User denies authorization
    /// - Proxy authentication is required
    /// - Network errors occur
    ///
    /// # Display Output
    ///
    /// In headless mode, prints progress dots. In GUI mode, shows polling status.
    pub(super) async fn get_access_token(
        &self,
        device_code: &DeviceCodeResponse,
    ) -> anyhow::Result<AccessTokenResponse> {
        let params = [
            ("client_id", "Iv1.b507a08c87ecfe98"),
            ("device_code", &device_code.device_code),
            ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
        ];

        let poll_interval = Duration::from_secs(device_code.interval.max(5));
        let max_duration = Duration::from_secs(device_code.expires_in);
        let start = std::time::Instant::now();

        if !self.headless_auth {
            println!("  🔄 Polling for authorization...");
        }

        loop {
            if start.elapsed() > max_duration {
                return Err(anyhow!("❌ Device code expired. Please try again."));
            }

            let url = format!("{}/login/oauth/access_token", self.github_login_base_url);
            let response = self
                .client
                .post(&url)
                .header("Accept", "application/json")
                .header("User-Agent", "BambooCopilot/1.0")
                .form(&params)
                .send()
                .await?;

            if response.status() == StatusCode::PROXY_AUTHENTICATION_REQUIRED {
                return Err(anyhow!(ProxyAuthRequiredError));
            }

            let response = response.json::<AccessTokenResponse>().await?;

            if let Some(token) = response.access_token {
                if !self.headless_auth {
                    println!("  ✅ Access token received!");
                }
                return Ok(AccessTokenResponse::from_token(token));
            }

            if let Some(error) = &response.error {
                match error.as_str() {
                    "authorization_pending" => {
                        if self.headless_auth {
                            print!(".");
                            std::io::Write::flush(&mut std::io::stdout()).ok();
                        }
                    }
                    "slow_down" => {
                        if !self.headless_auth {
                            println!("\n  ⚠️  Server requested slower polling...");
                        }
                        sleep(Duration::from_secs(device_code.interval + 5)).await;
                        continue;
                    }
                    "expired_token" => {
                        return Err(anyhow!("❌ Device code expired. Please try again."));
                    }
                    "access_denied" => {
                        return Err(anyhow!("❌ Authorization denied by user."));
                    }
                    _ => {
                        let desc = response.error_description.as_deref().unwrap_or("");
                        return Err(anyhow!("❌ Auth error: {} - {}", error, desc));
                    }
                }
            }

            sleep(poll_interval).await;
        }
    }

    /// Exchanges a GitHub access token for a Copilot API token.
    ///
    /// This method exchanges a GitHub OAuth access token for a Copilot-specific
    /// API token by calling GitHub's `/copilot_internal/v2/token` endpoint.
    ///
    /// # Arguments
    ///
    /// * `access_token` - GitHub OAuth access token response
    ///
    /// # Returns
    ///
    /// A [`CopilotConfig`] containing:
    /// - Copilot API token
    /// - Token expiration time
    /// - Feature flags and settings
    /// - API endpoints
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Access token is invalid or expired
    /// - Copilot is not enabled for the GitHub account
    /// - Proxy authentication is required
    /// - Network errors occur
    /// - Response parsing fails
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// # use bamboo_agent::agent::llm::providers::copilot::auth::handler::{CopilotAuthHandler, AccessTokenResponse};
    /// # async fn example(handler: CopilotAuthHandler, access_token: AccessTokenResponse) -> anyhow::Result<()> {
    /// let config = handler.get_copilot_token(access_token).await?;
    /// println!("Got Copilot token, expires at: {}", config.expires_at);
    /// # Ok(())
    /// # }
    /// ```
    pub(super) async fn get_copilot_token(
        &self,
        access_token: AccessTokenResponse,
    ) -> anyhow::Result<CopilotConfig> {
        let url = format!("{}/copilot_internal/v2/token", self.github_api_base_url);
        let actual_github_token = access_token
            .access_token
            .ok_or_else(|| anyhow!("Access token not found"))?;

        let response = self
            .client
            .get(url)
            .header("Authorization", format!("token {}", actual_github_token))
            .header("Accept", "application/json")
            .header("User-Agent", "BambooCopilot/1.0")
            .send()
            .await?;

        if response.status() == StatusCode::PROXY_AUTHENTICATION_REQUIRED {
            return Err(anyhow!(ProxyAuthRequiredError));
        }

        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Copilot token request failed: HTTP {} - {}",
                status,
                text
            ));
        }

        let body = response.bytes().await?;
        match serde_json::from_slice::<CopilotConfig>(&body) {
            Ok(copilot_config) => {
                if !copilot_config.chat_enabled {
                    return Err(anyhow!("❌ Copilot chat is not enabled for this account."));
                }
                if !self.headless_auth {
                    println!("  ✅ Copilot token received!");
                }
                Ok(copilot_config)
            }
            Err(_) => {
                let body_str = String::from_utf8_lossy(&body);
                let error_msg = format!("Failed to get copilot config: {body_str}");
                error!("{error_msg}");
                Err(anyhow!(error_msg))
            }
        }
    }
}

/// Integration tests for authentication retry logic.
///
/// These tests verify that authentication requests properly retry
/// on transient failures (e.g., 503 errors) while failing fast
/// on authentication errors (e.g., 401 unauthorized).
#[cfg(test)]
mod retry_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex as StdMutex;

    // use http; // TODO: add http crate if needed
    use reqwest::Method;
    use reqwest_middleware::{ClientBuilder, Middleware, Next, Result as MiddlewareResult};
    use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};

    /// Mock HTTP response for testing.
    #[derive(Clone)]
    struct MockReply {
        /// HTTP status code
        status: u16,
        /// Response body
        body: String,
        /// Content-Type header value
        content_type: Option<&'static str>,
    }

    impl MockReply {
        /// Creates a text response with the given status and body.
        fn text(status: u16, body: impl Into<String>) -> Self {
            Self {
                status,
                body: body.into(),
                content_type: Some("application/json"),
            }
        }

        /// Creates a JSON response with the given status and JSON value.
        fn json(status: u16, value: serde_json::Value) -> Self {
            Self {
                status,
                body: value.to_string(),
                content_type: Some("application/json"),
            }
        }
    }

    /// Middleware that mocks HTTP responses for testing.
    ///
    /// Returns responses in sequence, allowing tests to simulate
    /// retry scenarios (e.g., return 503 twice, then 200).
    #[derive(Clone)]
    struct MockResponder {
        /// Expected HTTP method
        expected_method: Method,
        /// Expected URL path
        expected_path: String,
        /// Counter for number of calls
        call_count: Arc<AtomicUsize>,
        /// Queue of responses to return
        replies: Arc<StdMutex<Vec<MockReply>>>,
    }

    impl MockResponder {
        /// Creates a new mock responder.
        ///
        /// # Arguments
        ///
        /// * `expected_method` - HTTP method to expect
        /// * `expected_path` - URL path to expect
        /// * `call_count` - Counter to track number of calls
        /// * `replies` - Queue of responses to return in sequence
        fn new(
            expected_method: Method,
            expected_path: impl Into<String>,
            call_count: Arc<AtomicUsize>,
            replies: Vec<MockReply>,
        ) -> Self {
            Self {
                expected_method,
                expected_path: expected_path.into(),
                call_count,
                replies: Arc::new(StdMutex::new(replies)),
            }
        }
    }

    #[async_trait::async_trait]
    impl Middleware for MockResponder {
        async fn handle(
            &self,
            req: reqwest::Request,
            _extensions: &mut http::Extensions,
            _next: Next<'_>,
        ) -> MiddlewareResult<reqwest::Response> {
            assert_eq!(
                req.method(),
                &self.expected_method,
                "unexpected method for {}",
                req.url()
            );
            assert_eq!(
                req.url().path(),
                self.expected_path.as_str(),
                "unexpected path for {}",
                req.url()
            );

            let idx = self.call_count.fetch_add(1, Ordering::SeqCst);
            let reply = {
                let mut guard = self.replies.lock().expect("lock");
                if guard.is_empty() {
                    panic!("no mock reply left for call #{idx}");
                }
                guard.remove(0)
            };

            let mut builder = http::Response::builder().status(reply.status);
            if let Some(ct) = reply.content_type {
                builder = builder.header("content-type", ct);
            }

            let http_response = builder.body(reply.body).expect("http response");
            Ok(reqwest::Response::from(http_response))
        }
    }

    /// Creates a test HTTP client with retry middleware and mock responder.
    fn create_test_client_with_retry(mock: MockResponder) -> Arc<ClientWithMiddleware> {
        use reqwest::Client as ReqwestClient;

        // Use a zero-delay retry policy to keep tests fast and deterministic.
        let retry_policy = ExponentialBackoff::builder()
            .retry_bounds(Duration::from_millis(0), Duration::from_millis(0))
            .build_with_max_retries(3);

        let client = ReqwestClient::builder().build().expect("client");

        Arc::new(
            ClientBuilder::new(client)
                .with(RetryTransientMiddleware::new_with_policy(retry_policy))
                .with(mock)
                .build(),
        )
    }

    /// Creates a sample CopilotConfig for testing with specified expiration time.
    fn sample_config(expires_at: u64) -> CopilotConfig {
        CopilotConfig {
            token: "cached-token".to_string(),
            annotations_enabled: false,
            chat_enabled: true,
            chat_jetbrains_enabled: false,
            code_quote_enabled: false,
            code_review_enabled: false,
            codesearch: false,
            copilotignore_enabled: false,
            endpoints: Endpoints {
                api: Some("https://api.example.com".to_string()),
                origin_tracker: None,
                proxy: None,
                telemetry: None,
                extra: Default::default(),
            },
            expires_at,
            individual: true,
            limited_user_quotas: None,
            limited_user_reset_date: None,
            prompt_8k: false,
            public_suggestions: "disabled".to_string(),
            refresh_in: 300,
            sku: "test".to_string(),
            snippy_load_test_enabled: false,
            telemetry: "disabled".to_string(),
            tracking_id: "test".to_string(),
            vsc_electron_fetcher_v2: false,
            xcode: false,
            xcode_chat: false,
            extra: Default::default(),
        }
    }

    /// Test that auth requests are retried on transient failures.
    ///
    /// Simulates a scenario where the Copilot token endpoint returns
    /// 503 (Service Unavailable) twice before succeeding. Verifies that:
    /// - The request is retried automatically
    /// - Eventually succeeds after retries
    /// - Total call count is 3 (2 failures + 1 success)
    #[tokio::test]
    async fn test_auth_retry_on_server_error() {
        let request_count = Arc::new(AtomicUsize::new(0));

        let mock = MockResponder::new(
            Method::GET,
            "/copilot_internal/v2/token",
            request_count.clone(),
            vec![
                MockReply::text(503, r#"{"error":"Service Unavailable"}"#),
                MockReply::text(503, r#"{"error":"Service Unavailable"}"#),
                MockReply::json(
                    200,
                    serde_json::json!({
                        "token": "test-copilot-token",
                        "expires_at": (SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 3600),
                        "annotations_enabled": true,
                        "chat_enabled": true,
                        "chat_jetbrains_enabled": false,
                        "code_quote_enabled": true,
                        "code_review_enabled": false,
                        "codesearch": false,
                        "copilotignore_enabled": true,
                        "endpoints": {
                            "api": "https://api.githubcopilot.com"
                        },
                        "individual": true,
                        "prompt_8k": true,
                        "public_suggestions": "disabled",
                        "refresh_in": 300,
                        "sku": "copilot_individual",
                        "snippy_load_test_enabled": false,
                        "telemetry": "disabled",
                        "tracking_id": "test-tracking-id",
                        "vsc_electron_fetcher_v2": true,
                        "xcode": false,
                        "xcode_chat": false
                    }),
                ),
            ],
        );

        let client = create_test_client_with_retry(mock);
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let handler = CopilotAuthHandler::new(client, temp_dir.path().to_path_buf(), true)
            .with_github_api_base_url("http://mock.local");

        // Create a valid access token
        let access_token = AccessTokenResponse {
            access_token: Some("test-github-token".to_string()),
            token_type: Some("bearer".to_string()),
            scope: Some("read:user".to_string()),
            error: None,
            error_description: None,
        };

        // This should retry and eventually succeed
        let result = handler.get_copilot_token(access_token).await;
        assert!(
            result.is_ok(),
            "Should succeed after retries: {:?}",
            result.err()
        );
        assert_eq!(request_count.load(Ordering::SeqCst), 3);

        let config = result.unwrap();
        assert_eq!(config.token, "test-copilot-token");
    }

    /// Test that auth requests fail fast on 401 (no retry).
    ///
    /// Verifies that authentication errors (401 Unauthorized) are not
    /// retried, as retrying would not fix the underlying issue.
    #[tokio::test]
    async fn test_auth_no_retry_on_unauthorized() {
        let request_count = Arc::new(AtomicUsize::new(0));

        let mock = MockResponder::new(
            Method::GET,
            "/copilot_internal/v2/token",
            request_count.clone(),
            vec![MockReply::text(401, r#"{"error":"Unauthorized"}"#)],
        );

        let client = create_test_client_with_retry(mock);
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let handler = CopilotAuthHandler::new(client, temp_dir.path().to_path_buf(), true)
            .with_github_api_base_url("http://mock.local");

        let access_token = AccessTokenResponse {
            access_token: Some("invalid-token".to_string()),
            token_type: Some("bearer".to_string()),
            scope: Some("read:user".to_string()),
            error: None,
            error_description: None,
        };

        let result = handler.get_copilot_token(access_token).await;
        assert!(result.is_err());
        assert_eq!(request_count.load(Ordering::SeqCst), 1);
    }

    /// Test device code endpoint retry.
    ///
    /// Simulates transient failures when requesting a device code
    /// and verifies that the request is retried until success.
    #[tokio::test]
    async fn test_device_code_retry() {
        let request_count = Arc::new(AtomicUsize::new(0));

        let mock = MockResponder::new(
            Method::POST,
            "/login/device/code",
            request_count.clone(),
            vec![
                MockReply::text(503, ""),
                MockReply::text(503, ""),
                MockReply::json(
                    200,
                    serde_json::json!({
                        "device_code": "test-device-code",
                        "user_code": "ABCD-EFGH",
                        "verification_uri": "https://github.com/login/device",
                        "expires_in": 900,
                        "interval": 5
                    }),
                ),
            ],
        );

        let client = create_test_client_with_retry(mock);
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let handler = CopilotAuthHandler::new(client, temp_dir.path().to_path_buf(), true)
            .with_github_login_base_url("http://mock.local");

        // Call the actual method - it should retry and eventually succeed
        let result = handler.get_device_code().await;

        assert!(
            result.is_ok(),
            "Should succeed after retries: {:?}",
            result.err()
        );
        assert_eq!(request_count.load(Ordering::SeqCst), 3);

        let device_code = result.unwrap();
        assert_eq!(device_code.device_code, "test-device-code");
        assert_eq!(device_code.user_code, "ABCD-EFGH");
    }

    /// Test token cache validation.
    ///
    /// Verifies that the 60-second buffer for token validation works correctly:
    /// - Tokens valid for > 60 seconds are considered valid
    /// - Tokens expired or expiring within 60 seconds are considered invalid
    #[test]
    fn test_token_cache_validation() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let client = create_test_client_with_retry(MockResponder::new(
            Method::GET,
            "/__unused__",
            Arc::new(AtomicUsize::new(0)),
            vec![],
        ));
        let handler = CopilotAuthHandler::new(client, temp_dir.path().to_path_buf(), true);

        // Valid token (expires in 1 hour)
        let valid_config = sample_config(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 3600,
        );
        assert!(handler.is_copilot_token_valid(&valid_config));

        // Expired token (expired 1 hour ago)
        let expired_config = sample_config(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                - 3600,
        );
        assert!(!handler.is_copilot_token_valid(&expired_config));

        // Token expiring soon (30 seconds left, but we use 60s buffer)
        let expiring_soon_config = sample_config(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 30,
        );
        assert!(!handler.is_copilot_token_valid(&expiring_soon_config));
    }

    /// Test cached config round-trip with retry client.
    ///
    /// Verifies that CopilotConfig can be written to disk and read back
    /// correctly when using an HTTP client with retry middleware.
    #[test]
    fn test_cached_copilot_config_with_retry_client() {
        let dir = tempfile::tempdir().expect("tempdir");
        let client = create_test_client_with_retry(MockResponder::new(
            Method::GET,
            "/__unused__",
            Arc::new(AtomicUsize::new(0)),
            vec![],
        ));
        let handler = CopilotAuthHandler::new(client, dir.path().to_path_buf(), false);
        let token_path = dir.path().join(".copilot_token.json");

        let expires_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let config = sample_config(expires_at);

        handler
            .write_cached_copilot_config(&token_path, &config)
            .expect("write cache");
        let loaded = handler
            .read_cached_copilot_config(&token_path)
            .expect("read cache");

        assert_eq!(loaded.token, config.token);
        assert_eq!(loaded.expires_at, config.expires_at);
    }
}
