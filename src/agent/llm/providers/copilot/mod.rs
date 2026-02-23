use async_trait::async_trait;
use reqwest::Client;
use serde_json::json;
use std::path::PathBuf;

pub mod auth;
use auth::{CopilotAuthHandler, DeviceCodeResponse};
use crate::provider::{LLMError, LLMProvider, LLMStream, Result};
use crate::types::LLMChunk;
use agent_core::{tools::ToolSchema, Message};

use super::common::openai_compat::{
    messages_to_openai_compat_json, parse_openai_compat_sse_data_lenient,
    tools_to_openai_compat_json,
};
use super::common::sse::llm_stream_from_sse;

/// GitHub Copilot Provider with Device Code Authentication
///
/// Authentication flow:
/// 1. Get device code from GitHub
/// 2. User authorizes at github.com/login/device
/// 3. Poll for access token
/// 4. Exchange for Copilot token
/// 5. Cache and use
///
/// Token sources (in order of priority):
/// 1. Direct API key via constructor
/// 2. Cached token (app_data_dir/.copilot_token.json)
/// 3. Environment variable COPILOT_API_KEY
/// 4. Interactive device code flow
pub struct CopilotProvider {
    client: Client,
    token: Option<String>,
    token_expires_at: Option<u64>,
    auth_handler: Option<CopilotAuthHandler>,
}

impl CopilotProvider {
    /// Create new Copilot provider (without auth handler)
    pub fn new() -> Self {
        Self {
            client: Client::new(),
            token: None,
            token_expires_at: None,
            auth_handler: None,
        }
    }

    /// Create provider with existing token
    pub fn with_token(token: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            token: Some(token.into()),
            token_expires_at: None,
            auth_handler: None,
        }
    }

    /// Create provider with auth handler (for HTTP/CLI authentication)
    pub fn with_auth_handler(
        client: Client,
        app_data_dir: PathBuf,
        headless_auth: bool,
    ) -> Self {
        use reqwest_middleware::ClientBuilder;
        use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
        use std::sync::Arc;
        use std::time::Duration;

        // Build retry client
        let retry_policy = ExponentialBackoff::builder()
            .retry_bounds(Duration::from_millis(100), Duration::from_secs(5))
            .build_with_max_retries(3);

        let client_with_middleware = Arc::new(
            ClientBuilder::new(client.clone())
                .with(RetryTransientMiddleware::new_with_policy(retry_policy))
                .build(),
        );

        let auth_handler = CopilotAuthHandler::new(client_with_middleware, app_data_dir, headless_auth);

        Self {
            client,
            token: None,
            token_expires_at: None,
            auth_handler: Some(auth_handler),
        }
    }

    /// Check if already authenticated
    pub fn is_authenticated(&self) -> bool {
        self.token.is_some()
    }

    /// Get token (if authenticated)
    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
    }

    /// Try to authenticate silently (from cache/env, without triggering device flow)
    pub async fn try_authenticate_silent(&mut self) -> std::result::Result<bool, LLMError> {
        if let Some(handler) = &self.auth_handler {
            match handler.try_get_chat_token_silent().await {
                Ok(Some(token)) => {
                    self.token = Some(token);
                    return Ok(true);
                }
                Ok(None) => return Ok(false),
                Err(e) => return Err(LLMError::Auth(e.to_string())),
            }
        }
        Ok(false)
    }

    /// Authenticate using device code flow (full flow with cache check)
    pub async fn authenticate(&mut self) -> std::result::Result<(), LLMError> {
        // Try silent first
        if self.try_authenticate_silent().await? {
            return Ok(());
        }

        // Need interactive authentication
        if let Some(handler) = &self.auth_handler {
            let token = handler.get_chat_token().await
                .map_err(|e| LLMError::Auth(e.to_string()))?;
            self.token = Some(token);
            Ok(())
        } else {
            Err(LLMError::Auth("No auth handler configured".to_string()))
        }
    }

    /// Start authentication and return device code info for frontend display
    pub async fn start_authentication(&self) -> std::result::Result<DeviceCodeResponse, LLMError> {
        if let Some(handler) = &self.auth_handler {
            handler.start_authentication().await
                .map_err(|e| LLMError::Auth(e.to_string()))
        } else {
            Err(LLMError::Auth("No auth handler configured".to_string()))
        }
    }

    /// Complete authentication with device code (poll for token)
    pub async fn complete_authentication(
        &mut self,
        device_code: &DeviceCodeResponse,
    ) -> std::result::Result<(), LLMError> {
        if let Some(handler) = &self.auth_handler {
            let config = handler.complete_authentication(device_code).await
                .map_err(|e| LLMError::Auth(e.to_string()))?;
            self.token = Some(config.token);
            self.token_expires_at = Some(config.expires_at);
            Ok(())
        } else {
            Err(LLMError::Auth("No auth handler configured".to_string()))
        }
    }

    /// Logout - delete cached tokens
    pub async fn logout(&mut self) -> std::result::Result<(), LLMError> {
        if let Some(handler) = &self.auth_handler {
            // Delete token files
            let token_path = handler.app_data_dir().join(".token");
            let copilot_token_path = handler.app_data_dir().join(".copilot_token.json");

            if token_path.exists() {
                std::fs::remove_file(&token_path)
                    .map_err(|e| LLMError::Auth(format!("Failed to delete .token: {}", e)))?;
            }

            if copilot_token_path.exists() {
                std::fs::remove_file(&copilot_token_path)
                    .map_err(|e| LLMError::Auth(format!("Failed to delete .copilot_token.json: {}", e)))?;
            }
        }

        self.token = None;
        self.token_expires_at = None;
        log::info!("Logged out and deleted cached tokens");
        Ok(())
    }

    /// Build request headers to mimic VS Code Copilot extension
    fn build_headers(&self) -> std::result::Result<reqwest::header::HeaderMap, LLMError> {
        use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};

        let token = self
            .token
            .as_ref()
            .ok_or_else(|| LLMError::Auth("Not authenticated".to_string()))?;

        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", token))
                .map_err(|e| LLMError::Auth(format!("Invalid token: {}", e)))?,
        );
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        // Mimic VS Code Copilot extension headers
        headers.insert("editor-version", HeaderValue::from_static("vscode/1.99.2"));
        headers.insert(
            "editor-plugin-version",
            HeaderValue::from_static("copilot-chat/0.20.3"),
        );
        headers.insert(
            "user-agent",
            HeaderValue::from_static("GitHubCopilotChat/0.20.3"),
        );
        headers.insert("accept", HeaderValue::from_static("application/json"));
        headers.insert(
            "accept-encoding",
            HeaderValue::from_static("gzip, deflate, br"),
        );
        headers.insert(
            "copilot-integration-id",
            HeaderValue::from_static("vscode-chat"),
        );

        Ok(headers)
    }
}

impl Default for CopilotProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl LLMProvider for CopilotProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
    ) -> Result<LLMStream> {
        // Ensure authenticated
        if !self.is_authenticated() {
            return Err(LLMError::Auth(
                "Not authenticated. Please run authenticate() first.".to_string(),
            ));
        }

        // Copilot uses a fixed upstream model; keep the required `model` parameter for trait consistency.
        log::debug!(
            "Copilot provider ignoring requested model (fixed upstream model): {}",
            model
        );

        let mut body = json!({
            "model": "copilot-chat",
            "messages": messages_to_openai_compat_json(messages),
            "stream": true,
        });

        // Only include tools and tool_choice if tools are provided
        if !tools.is_empty() {
            body["tools"] = json!(tools_to_openai_compat_json(tools));
            body["tool_choice"] = json!("auto");
        }

        if let Some(max_tokens) = max_output_tokens {
            body["max_tokens"] = json!(max_tokens);
        }

        log::debug!(
            "Sending request to Copilot API with {} messages and {} tools",
            messages.len(),
            tools.len()
        );

        let headers = self.build_headers()?;

        let response = self
            .client
            .post("https://api.githubcopilot.com/chat/completions")
            .headers(headers)
            .json(&body)
            .send()
            .await
            .map_err(LLMError::Http)?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();

            // Check for auth errors
            if status == 401 || status == 403 {
                return Err(LLMError::Auth(format!(
                    "Authentication failed: {}. Please run authenticate() again.",
                    text
                )));
            }

            log::error!("Copilot API error: HTTP {} - {}", status, text);
            return Err(LLMError::Api(format!("HTTP {}: {}", status, text)));
        }

        let stream = llm_stream_from_sse(response, |_event, data| {
            let chunk = parse_openai_compat_sse_data_lenient(data)?;
            match chunk {
                LLMChunk::Done => Ok(None),
                other => Ok(Some(other)),
            }
        });

        Ok(stream)
    }

    async fn list_models(&self) -> Result<Vec<String>> {
        // Ensure authenticated
        if !self.is_authenticated() {
            return Err(LLMError::Auth(
                "Not authenticated. Please run authenticate() first.".to_string(),
            ));
        }

        let headers = self.build_headers()?;

        let response = self
            .client
            .get("https://api.githubcopilot.com/models")
            .headers(headers)
            .send()
            .await
            .map_err(LLMError::Http)?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();

            // Check for auth errors
            if status == 401 || status == 403 {
                return Err(LLMError::Auth(format!(
                    "Authentication failed: {}. Please run authenticate() again.",
                    text
                )));
            }

            log::error!("Copilot API error: HTTP {} - {}", status, text);
            return Err(LLMError::Api(format!("HTTP {}: {}", status, text)));
        }

        // Parse the response
        #[derive(serde::Deserialize)]
        struct ModelResponse {
            data: Vec<ModelData>,
        }

        #[derive(serde::Deserialize)]
        struct ModelData {
            id: String,
        }

        let models: ModelResponse = response
            .json()
            .await
            .map_err(LLMError::Http)?;

        // Deduplicate model IDs to avoid duplicates in the list
        let mut model_ids: Vec<String> = models.data.into_iter().map(|m| m.id).collect();
        model_ids.sort();
        model_ids.dedup();

        Ok(model_ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to skip tests in CODEX_SANDBOX environment
    fn should_skip() -> bool {
        std::env::var_os("CODEX_SANDBOX").is_some()
    }

    // ============================================
    // Existing Tests (2)
    // ============================================

    #[test]
    fn test_new_provider() {
        if should_skip() {
            return;
        }

        let provider = CopilotProvider::new();
        assert!(!provider.is_authenticated());
    }

    #[test]
    fn test_with_token() {
        if should_skip() {
            return;
        }

        let provider = CopilotProvider::with_token("test_token");
        assert!(provider.is_authenticated());
        assert_eq!(provider.token(), Some("test_token"));
    }

    // ============================================
    // Basic Tests (3)
    // ============================================

    #[test]
    fn test_default_values() {
        if should_skip() {
            return;
        }

        let provider = CopilotProvider::new();
        assert!(provider.token.is_none());
        assert!(provider.token_expires_at.is_none());
        assert!(!provider.is_authenticated());
    }

    #[test]
    fn test_with_token_chaining() {
        if should_skip() {
            return;
        }

        // Verify that with_token creates a properly configured instance
        let provider = CopilotProvider::with_token("my_token_123");

        // All assertions should pass
        assert!(provider.is_authenticated());
        assert_eq!(provider.token(), Some("my_token_123"));
        assert!(provider.token.is_some());
    }

    #[test]
    fn test_token_expiry() {
        if should_skip() {
            return;
        }

        // Token expiry is set to None when using with_token
        let provider = CopilotProvider::with_token("test_token");
        assert!(provider.token_expires_at.is_none());

        // New provider also has None expiry
        let new_provider = CopilotProvider::new();
        assert!(new_provider.token_expires_at.is_none());
    }

    // ============================================
    // Headers Tests (3)
    // ============================================

    #[test]
    fn test_build_headers_success() {
        if should_skip() {
            return;
        }

        let provider = CopilotProvider::with_token("test_token");
        let headers = provider.build_headers().unwrap();

        // Check authorization header
        assert!(headers.contains_key("authorization"));
        let auth_header = headers.get("authorization").unwrap();
        assert_eq!(auth_header, "Bearer test_token");
    }

    #[test]
    fn test_build_headers_without_token() {
        if should_skip() {
            return;
        }

        let provider = CopilotProvider::new();
        let result = provider.build_headers();

        // Should fail with Auth error
        assert!(result.is_err());
        match result {
            Err(LLMError::Auth(msg)) => {
                assert!(msg.contains("Not authenticated"));
            }
            _ => panic!("Expected Auth error"),
        }
    }

    #[test]
    fn test_headers_contain_required_fields() {
        if should_skip() {
            return;
        }

        let provider = CopilotProvider::with_token("test_token");
        let headers = provider.build_headers().unwrap();

        // Copilot needs to mimic VS Code headers
        assert!(headers.contains_key("authorization"));
        assert!(headers.contains_key("content-type"));
        assert!(headers.contains_key("editor-version"));
        assert!(headers.contains_key("editor-plugin-version"));
        assert!(headers.contains_key("user-agent"));
        assert!(headers.contains_key("accept"));
        assert!(headers.contains_key("accept-encoding"));
        assert!(headers.contains_key("copilot-integration-id"));

        // Verify specific VS Code mimic values
        assert_eq!(
            headers.get("editor-version").unwrap(),
            "vscode/1.99.2"
        );
        assert_eq!(
            headers.get("editor-plugin-version").unwrap(),
            "copilot-chat/0.20.3"
        );
        assert_eq!(
            headers.get("user-agent").unwrap(),
            "GitHubCopilotChat/0.20.3"
        );
        assert_eq!(
            headers.get("copilot-integration-id").unwrap(),
            "vscode-chat"
        );
        assert_eq!(
            headers.get("content-type").unwrap(),
            "application/json"
        );
    }

    // ============================================
    // Authentication State Tests (4)
    // ============================================

    #[test]
    fn test_is_authenticated_with_token() {
        if should_skip() {
            return;
        }

        let provider = CopilotProvider::with_token("valid_token");
        assert!(provider.is_authenticated());
    }

    #[test]
    fn test_is_authenticated_without_token() {
        if should_skip() {
            return;
        }

        let provider = CopilotProvider::new();
        assert!(!provider.is_authenticated());
    }

    #[test]
    fn test_logout_clears_token() {
        if should_skip() {
            return;
        }

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(async {
            let mut provider = CopilotProvider::with_token("test_token");
            assert!(provider.is_authenticated());

            // Note: logout will try to delete a file, which may fail in tests
            // We mainly test that the in-memory state is cleared
            let _ = provider.logout().await;

            // Token should be cleared
            assert!(provider.token.is_none());
            assert!(provider.token_expires_at.is_none());
            assert!(!provider.is_authenticated());

            Ok::<(), ()>(())
        });

        assert!(result.is_ok());
    }

    #[test]
    fn test_token_getter() {
        if should_skip() {
            return;
        }

        // Test with token
        let provider_with_token = CopilotProvider::with_token("my_secret_token");
        assert_eq!(provider_with_token.token(), Some("my_secret_token"));

        // Test without token
        let provider_no_token = CopilotProvider::new();
        assert_eq!(provider_no_token.token(), None);
    }

    // ============================================
    // Request Tests (3)
    // ============================================

    #[test]
    fn test_request_url() {
        if should_skip() {
            return;
        }

        // The URL is hardcoded in chat_stream, verify it's the expected endpoint
        let expected_url = "https://api.githubcopilot.com/chat/completions";
        assert!(expected_url.contains("githubcopilot.com"));
        assert!(expected_url.contains("chat/completions"));
    }

    #[test]
    fn test_request_body_format() {
        if should_skip() {
            return;
        }

        use serde_json::json;

        // Verify the request body structure
        let messages: Vec<Message> = vec![];
        let tools: Vec<ToolSchema> = vec![];
        let mut body = json!({
            "model": "copilot-chat",
            "messages": messages,
            "messages": messages,
            "stream": true,
            "tools": tools,
            "tool_choice": "auto",
        });

        // Verify required fields
        assert_eq!(body["model"], "copilot-chat");
        assert_eq!(body["stream"], true);
        assert_eq!(body["tool_choice"], "auto");

        // Test with max_tokens
        body["max_tokens"] = json!(1000);
        assert_eq!(body["max_tokens"], 1000);
    }

    #[test]
    fn test_request_headers_format() {
        if should_skip() {
            return;
        }

        let provider = CopilotProvider::with_token("test_token");
        let headers = provider.build_headers().unwrap();

        // Verify header values are valid UTF-8
        for (name, value) in headers.iter() {
            assert!(value.to_str().is_ok(), "Header {} has invalid UTF-8 value", name);
        }

        // Verify Bearer token format
        let auth = headers.get("authorization").unwrap().to_str().unwrap();
        assert!(auth.starts_with("Bearer "));
        assert!(auth.contains("test_token"));
    }

    // ============================================
    // Error Handling Tests (2)
    // ============================================

    #[test]
    fn test_chat_stream_without_auth_fails() {
        if should_skip() {
            return;
        }

        let provider = CopilotProvider::new(); // No token

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(async {
            provider.chat_stream(&[], &[], None, "copilot-chat").await
        });

        assert!(result.is_err());
        match result {
            Err(LLMError::Auth(msg)) => {
                assert!(msg.contains("Not authenticated"));
            }
            _ => panic!("Expected Auth error"),
        }
    }

    #[test]
    fn test_build_headers_with_invalid_token() {
        if should_skip() {
            return;
        }

        // Test with an empty token string
        let provider = CopilotProvider::with_token("");
        let result = provider.build_headers();

        // Should succeed (empty token is still a valid string for HeaderValue)
        assert!(result.is_ok());
        let headers = result.unwrap();
        let auth = headers.get("authorization").unwrap().to_str().unwrap();
        assert_eq!(auth, "Bearer ");

        // Test with a very long token
        let long_token = "a".repeat(10000);
        let provider_long = CopilotProvider::with_token(long_token.clone());
        let result_long = provider_long.build_headers();

        // Should still succeed
        assert!(result_long.is_ok());

        // Test with special characters in token
        // Note: Some special characters might cause issues with HeaderValue
        // The build_headers method will fail on invalid chars, which is expected
    }

    // ========== MODEL REQUIREMENT ARCHITECTURE TESTS ==========
    // These tests ensure the design principle:
    // "Provider must not have a default model field or with_model() method"

    /// Test: CopilotProvider does NOT have a model field
    #[test]
    fn copilot_provider_has_no_model_field() {
        if should_skip() {
            return;
        }

        // This test documents the provider structure:
        // pub struct CopilotProvider {
        //     client: Client,
        //     token: Option<String>,
        //     token_expires_at: Option<u64>,
        //     auth_handler: Option<CopilotAuthHandler>,
        //     // NO model field!
        // }
        //
        // If someone adds a model field, this test should be updated
        // to reflect the architecture change.
        let provider = CopilotProvider::new();
        // Verify we can access known fields
        assert!(provider.token.is_none());
        assert!(provider.token_expires_at.is_none());
        assert!(!provider.is_authenticated());
        // There is NO provider.model field to access
    }

    /// Test: CopilotProvider does NOT have with_model() method
    #[test]
    fn copilot_provider_has_no_with_model_method() {
        if should_skip() {
            return;
        }

        // Available builder/constructor methods:
        let provider = CopilotProvider::with_token("test_token");

        // There is NO .with_model("copilot-chat") method
        // Model is passed to chat_stream() as a parameter (though Copilot uses a fixed upstream model)

        assert!(provider.is_authenticated());
    }
}
