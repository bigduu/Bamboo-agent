use crate::error::ProxyAuthRequiredError;
use anyhow::anyhow;
use lazy_static::lazy_static;
use log::error;
use reqwest::StatusCode;
use reqwest_middleware::ClientWithMiddleware;
use serde::{Deserialize, Serialize};
use std::{
    fs::{read_to_string, File},
    io::Write,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;
use tokio::time::sleep;

use super::device_code::DeviceCodeResponse;

// Models for GitHub authentication flow
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CopilotConfig {
    pub token: String,
    // Other fields are not used directly by the auth handler, but are kept for completeness
    pub annotations_enabled: bool,
    pub chat_enabled: bool,
    pub chat_jetbrains_enabled: bool,
    pub code_quote_enabled: bool,
    pub code_review_enabled: bool,
    pub codesearch: bool,
    pub copilotignore_enabled: bool,
    pub endpoints: Endpoints,
    pub expires_at: u64,
    pub individual: bool,
    pub limited_user_quotas: Option<String>,
    pub limited_user_reset_date: Option<String>,
    pub prompt_8k: bool,
    pub public_suggestions: String,
    pub refresh_in: u64,
    pub sku: String,
    pub snippy_load_test_enabled: bool,
    pub telemetry: String,
    pub tracking_id: String,
    pub vsc_electron_fetcher_v2: bool,
    pub xcode: bool,
    pub xcode_chat: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_http_client() -> Arc<ClientWithMiddleware> {
        use reqwest::Client as ReqwestClient;
        use reqwest_middleware::ClientBuilder;
        let client = ReqwestClient::builder().no_proxy().build().expect("client");
        Arc::new(ClientBuilder::new(client).build())
    }

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
        }
    }

    #[test]
    fn read_access_token_trims() {
        let dir = tempdir().expect("tempdir");
        let token_path = dir.path().join(".token");
        std::fs::write(&token_path, "  token-value \n").expect("write token");

        let token = CopilotAuthHandler::read_access_token(&token_path);
        assert_eq!(token.as_deref(), Some("token-value"));
    }

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
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Endpoints {
    pub api: Option<String>,
    pub origin_tracker: Option<String>,
    pub proxy: Option<String>,
    pub telemetry: Option<String>,
}

/// Access token response from GitHub
#[derive(Debug, Deserialize)]
pub(crate) struct AccessTokenResponse {
    pub access_token: Option<String>,
    #[allow(dead_code)] // Needed for JSON deserialization from GitHub API
    pub token_type: Option<String>,
    #[allow(dead_code)] // Needed for JSON deserialization from GitHub API
    pub scope: Option<String>,
    pub error: Option<String>,
    #[serde(rename = "error_description")]
    pub error_description: Option<String>,
}
impl AccessTokenResponse {
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

// Global static lock for get_chat_token
lazy_static! {
    static ref CHAT_TOKEN_LOCK: Mutex<()> = Mutex::new(());
}

// Struct for handling authentication logic
#[derive(Debug, Clone)]
pub struct CopilotAuthHandler {
    client: Arc<ClientWithMiddleware>,
    app_data_dir: PathBuf,
    headless_auth: bool,
    github_api_base_url: String,
    github_login_base_url: String,
}

impl CopilotAuthHandler {
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

    /// Get app data directory
    pub fn app_data_dir(&self) -> &PathBuf {
        &self.app_data_dir
    }

    /// Create handler with custom GitHub API base URL (for testing)
    #[cfg(test)]
    fn with_github_api_base_url(mut self, url: impl Into<String>) -> Self {
        self.github_api_base_url = url.into();
        self
    }

    /// Create handler with custom GitHub login base URL (for testing)
    #[cfg(test)]
    fn with_github_login_base_url(mut self, url: impl Into<String>) -> Self {
        self.github_login_base_url = url.into();
        self
    }

    pub async fn authenticate(&self) -> anyhow::Result<String> {
        self.get_chat_token().await
    }

    pub async fn ensure_authenticated(&self) -> anyhow::Result<()> {
        self.get_chat_token().await.map(|_| ())
    }

    pub async fn get_token(&self) -> anyhow::Result<String> {
        self.get_chat_token().await
    }

    // get_chat_token remains in CopilotClient, delegates to auth_handler
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

    fn read_cached_copilot_config(&self, token_path: &PathBuf) -> Option<CopilotConfig> {
        let cached_str = read_to_string(token_path).ok()?;
        serde_json::from_str::<CopilotConfig>(&cached_str).ok()
    }

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

    fn is_copilot_token_valid(&self, copilot_config: &CopilotConfig) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or(0);
        copilot_config.expires_at.saturating_sub(60) > now
    }

    pub(super) async fn get_device_code(&self) -> anyhow::Result<DeviceCodeResponse> {
        let params = [("client_id", "Iv1.b507a08c87ecfe98"), ("scope", "read:user")];
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

    /// Start authentication - get device code
    /// If headless_auth is false, prints instructions to console
    /// Always returns device code info for caller to display
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

    /// Complete authentication - poll for access token and exchange for copilot token
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

    /// Try to get chat token silently (from cache or env, without triggering device flow)
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
                Err(_) => {
                    // Invalid access token, remove it
                    let _ = std::fs::remove_file(&token_path);
                }
            }
        }

        Ok(None)
    }

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
                    return Err(anyhow!(
                        "❌ Copilot chat is not enabled for this account."
                    ));
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

#[cfg(test)]
mod retry_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex as StdMutex;

    use http;
    use reqwest::Method;
    use reqwest_middleware::{ClientBuilder, Middleware, Next, Result as MiddlewareResult};
    use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};

    #[derive(Clone)]
    struct MockReply {
        status: u16,
        body: String,
        content_type: Option<&'static str>,
    }

    impl MockReply {
        fn text(status: u16, body: impl Into<String>) -> Self {
            Self {
                status,
                body: body.into(),
                content_type: Some("application/json"),
            }
        }

        fn json(status: u16, value: serde_json::Value) -> Self {
            Self {
                status,
                body: value.to_string(),
                content_type: Some("application/json"),
            }
        }
    }

    #[derive(Clone)]
    struct MockResponder {
        expected_method: Method,
        expected_path: String,
        call_count: Arc<AtomicUsize>,
        replies: Arc<StdMutex<Vec<MockReply>>>,
    }

    impl MockResponder {
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
                guard
                    .get(0)
                    .cloned()
                    .unwrap_or_else(|| panic!("no mock reply left for call #{idx}"))
            };

            // Pop after cloning so we can include `idx` in the panic above without borrow issues.
            {
                let mut guard = self.replies.lock().expect("lock");
                guard.remove(0);
            }

            let mut builder = http::Response::builder().status(reply.status);
            if let Some(ct) = reply.content_type {
                builder = builder.header("content-type", ct);
            }

            let http_response = builder.body(reply.body).expect("http response");
            Ok(reqwest::Response::from(http_response))
        }
    }

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
        }
    }

    /// Test that auth requests are retried on transient failures
    /// Test server error retries (integration test)
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

    /// Test that auth requests fail fast on 401 (no retry)
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

    /// Test device code endpoint retry
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

        assert!(result.is_ok(), "Should succeed after retries: {:?}", result.err());
        assert_eq!(request_count.load(Ordering::SeqCst), 3);

        let device_code = result.unwrap();
        assert_eq!(device_code.device_code, "test-device-code");
        assert_eq!(device_code.user_code, "ABCD-EFGH");
    }

    /// Test token cache validation
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

    /// Test cached config round-trip with retry client
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
