//! HTTP request tool for making web requests.
//!
//! This tool provides support for making HTTP requests (GET, POST, PUT, DELETE, PATCH)
//! with configurable headers, body, and timeout. It's useful for:
//! - Calling REST APIs
//! - Fetching remote resources
//! - Webhooks
//!
//! # Security
//!
//! This tool requires `PermissionType::HttpRequest` permission. The URL domain
//! is extracted and checked against the whitelist/session grants.

use agent_core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
#[cfg(feature = "http")]
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;

/// HTTP method
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
#[derive(Default)]
pub enum HttpMethod {
    #[default]
    Get,
    Post,
    Put,
    Delete,
    Patch,
    Head,
    Options,
}

impl HttpMethod {
    /// Convert to reqwest method
    #[cfg(feature = "http")]
    fn to_reqwest(&self) -> reqwest::Method {
        match self {
            HttpMethod::Get => reqwest::Method::GET,
            HttpMethod::Post => reqwest::Method::POST,
            HttpMethod::Put => reqwest::Method::PUT,
            HttpMethod::Delete => reqwest::Method::DELETE,
            HttpMethod::Patch => reqwest::Method::PATCH,
            HttpMethod::Head => reqwest::Method::HEAD,
            HttpMethod::Options => reqwest::Method::OPTIONS,
        }
    }
}


/// Arguments for HTTP request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpRequestArgs {
    /// HTTP method (GET, POST, PUT, DELETE, PATCH)
    #[serde(default)]
    pub method: HttpMethod,
    /// URL to request
    pub url: String,
    /// Optional headers to include
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, String>>,
    /// Optional request body (for POST, PUT, PATCH)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    /// Request timeout in seconds (default: 30)
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
    /// Maximum response body size in bytes (default: 1MB)
    #[serde(default = "default_max_size")]
    pub max_response_size: usize,
}

fn default_timeout() -> u64 {
    30
}

fn default_max_size() -> usize {
    1024 * 1024 // 1MB
}

/// Check if an IP address is private/internal and should be blocked for SSRF protection.
///
/// This blocks:
/// - Private IPv4 ranges (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16)
/// - Loopback addresses (127.0.0.0/8, ::1)
/// - Link-local addresses (169.254.0.0/16, fe80::/10)
/// - IPv6 unique local addresses (fc00::/7)
/// - AWS metadata endpoint (169.254.169.254)
#[cfg(feature = "http")]
fn is_private_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(ipv4) => {
            let octets = ipv4.octets();
            match octets {
                // 10.0.0.0/8 - Private Class A
                [10, ..] => true,
                // 172.16.0.0/12 - Private Class B
                [172, 16..=31, ..] => true,
                // 192.168.0.0/16 - Private Class C
                [192, 168, ..] => true,
                // 127.0.0.0/8 - Loopback
                [127, ..] => true,
                // 169.254.0.0/16 - Link-local and AWS metadata endpoint
                [169, 254, ..] => true,
                // 0.0.0.0/8 - Current network
                [0, ..] => true,
                _ => false,
            }
        }
        IpAddr::V6(ipv6) => {
            let segments = ipv6.segments();
            // ::1 - Loopback
            if ipv6.is_loopback() {
                return true;
            }
            // fe80::/10 - Link-local
            if (segments[0] & 0xffc0) == 0xfe80 {
                return true;
            }
            // fc00::/7 - Unique local
            if (segments[0] & 0xfe00) == 0xfc00 {
                return true;
            }
            // IPv4-mapped IPv6 addresses - check the embedded IPv4
            if let Some(v4) = ipv6.to_ipv4_mapped() {
                return is_private_ip(&IpAddr::V4(v4));
            }
            false
        }
    }
}

/// Validate that a URL's host does not resolve to a private/internal IP address.
///
/// This prevents SSRF attacks by blocking requests to internal network resources.
/// Returns an error if the host is private or if DNS resolution fails in a way
/// that might indicate an attack.
#[cfg(feature = "http")]
async fn validate_url_not_internal(parsed_url: &url::Url) -> Result<(), String> {
    let host = parsed_url.host_str().ok_or("URL has no host")?;

    // Try to parse as IP address directly
    if let Ok(ip) = host.parse::<IpAddr>() {
        if is_private_ip(&ip) {
            return Err(format!(
                "SSRF protection: Request to private IP address {} is blocked",
                ip
            ));
        }
        return Ok(());
    }

    // For domain names, perform DNS lookup and check all resolved IPs
    // Use tokio's DNS resolution via tokio::net::lookup_host
    let port = parsed_url.port_or_known_default().unwrap_or(80);

    let lookup_result = tokio::net::lookup_host(format!("{}:{}", host, port)).await;

    match lookup_result {
        Ok(addrs) => {
            for addr in addrs {
                if is_private_ip(&addr.ip()) {
                    return Err(format!(
                        "SSRF protection: Request to host {} which resolves to private IP {} is blocked",
                        host, addr.ip()
                    ));
                }
            }
            Ok(())
        }
        Err(e) => {
            // If DNS resolution fails, we can't verify it's safe
            // Be conservative and reject the request
            Err(format!(
                "SSRF protection: Unable to verify host safety for '{}': {}",
                host, e
            ))
        }
    }
}

impl HttpRequestArgs {
    /// Extract the domain from the URL for permission checking
    pub fn extract_domain(&self) -> Option<String> {
        extract_domain_from_url(&self.url)
    }
}

/// Tool for making HTTP requests
pub struct HttpRequestTool {
    #[cfg(feature = "http")]
    client: reqwest::Client,
}

impl HttpRequestTool {
    /// Create a new HTTP request tool
    pub fn new() -> Self {
        #[cfg(feature = "http")]
        {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .user_agent("Bamboo-Agent-Tools/0.1.0")
                // Disable automatic redirects to prevent bypassing domain whitelist checks
                // If redirects are needed, the application should handle them explicitly
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("Failed to build HTTP client");

            Self { client }
        }

        #[cfg(not(feature = "http"))]
        {
            Self {}
        }
    }

    /// Execute an HTTP request
    #[cfg(feature = "http")]
    pub async fn execute_request(&self, args: HttpRequestArgs) -> Result<HttpResponse, String> {
        // Validate URL using Url::parse for consistency with permission checking
        let url_str = args.url.trim();
        let parsed_url = url::Url::parse(url_str)
            .map_err(|e| format!("Invalid URL: {}", e))?;

        // Check scheme is http or https (case-insensitive)
        match parsed_url.scheme().to_lowercase().as_str() {
            "http" | "https" => {}
            scheme => return Err(format!("URL scheme must be http:// or https://, got: {}://", scheme)),
        }

        // Validate SSRF protection - block internal/private IP addresses
        validate_url_not_internal(&parsed_url).await?;

        // Build request
        let method = args.method.to_reqwest();
        let mut request_builder = self.client.request(method, url_str);

        // Set timeout
        request_builder = request_builder.timeout(Duration::from_secs(args.timeout_seconds));

        // Add headers
        if let Some(headers) = &args.headers {
            for (key, value) in headers {
                request_builder = request_builder.header(key, value);
            }
        }

        // Add body for appropriate methods
        if matches!(args.method, HttpMethod::Post | HttpMethod::Put | HttpMethod::Patch) {
            if let Some(body) = &args.body {
                // Try to determine content type from body
                if body.trim().starts_with('{') || body.trim().starts_with('[') {
                    request_builder = request_builder.header("Content-Type", "application/json");
                }
                request_builder = request_builder.body(body.clone());
            }
        }

        // Execute request
        let response = request_builder
            .send()
            .await
            .map_err(|e| format!("Request failed: {}", e))?;

        // Extract response info
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .filter_map(|(k, v)| {
                v.to_str()
                    .ok()
                    .map(|v| (k.to_string(), v.to_string()))
            })
            .collect();

        // Get response body with size limit
        // Check content-length first to avoid loading large responses into memory
        if let Some(content_length) = response.content_length() {
            if content_length > args.max_response_size as u64 {
                return Ok(HttpResponse {
                    status,
                    headers,
                    body: format!(
                        "[Response body too large: {} bytes exceeds limit of {} bytes]",
                        content_length, args.max_response_size
                    ),
                });
            }
        }

        // Stream the response body with a size limit to prevent memory exhaustion
        let mut stream = response.bytes_stream();
        let mut body_bytes = Vec::new();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| format!("Failed to read response chunk: {}", e))?;
            body_bytes.extend_from_slice(&chunk);

            if body_bytes.len() > args.max_response_size {
                return Ok(HttpResponse {
                    status,
                    headers,
                    body: format!(
                        "[Response body exceeded limit of {} bytes]",
                        args.max_response_size
                    ),
                });
            }
        }

        let body = String::from_utf8_lossy(&body_bytes).to_string();

        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }

    /// Execute an HTTP request (stub when http feature is disabled)
    #[cfg(not(feature = "http"))]
    pub async fn execute_request(&self, _args: HttpRequestArgs) -> Result<HttpResponse, String> {
        Err("HTTP support is not enabled. Enable the 'http' feature to use this tool.".to_string())
    }
}

impl Default for HttpRequestTool {
    fn default() -> Self {
        Self::new()
    }
}

/// HTTP response structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpResponse {
    /// HTTP status code
    pub status: u16,
    /// Response headers
    pub headers: HashMap<String, String>,
    /// Response body
    pub body: String,
}

impl HttpResponse {
    /// Format the response for display
    pub fn format(&self) -> String {
        let mut output = format!("HTTP Status: {}\n", self.status);

        if !self.headers.is_empty() {
            output.push_str("\nHeaders:\n");
            for (key, value) in &self.headers {
                output.push_str(&format!("  {}: {}\n", key, value));
            }
        }

        if !self.body.is_empty() {
            output.push_str("\nBody:\n");
            // Truncate if too long for display
            if self.body.len() > 10000 {
                output.push_str(&self.body[..10000]);
                output.push_str("\n\n[Body truncated for display]");
            } else {
                output.push_str(&self.body);
            }
        }

        output
    }
}

#[async_trait]
impl Tool for HttpRequestTool {
    fn name(&self) -> &str {
        "http_request"
    }

    fn description(&self) -> &str {
        "Make HTTP requests to external services. Supports GET, POST, PUT, DELETE, PATCH methods with custom headers and body. Useful for calling APIs, fetching data, or webhooks."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "method": {
                    "type": "string",
                    "enum": ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
                    "default": "GET",
                    "description": "HTTP method to use"
                },
                "url": {
                    "type": "string",
                    "format": "uri",
                    "description": "URL to request (must start with http:// or https://)"
                },
                "headers": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "string"
                    },
                    "description": "Optional HTTP headers to include"
                },
                "body": {
                    "type": "string",
                    "description": "Request body (for POST, PUT, PATCH methods)"
                },
                "timeout_seconds": {
                    "type": "integer",
                    "default": 30,
                    "minimum": 1,
                    "maximum": 300,
                    "description": "Request timeout in seconds (max 300)"
                },
                "max_response_size": {
                    "type": "integer",
                    "default": 1048576,
                    "description": "Maximum response body size in bytes (default 1MB)"
                }
            },
            "required": ["url"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let http_args: HttpRequestArgs =
            serde_json::from_value(args).map_err(|e| ToolError::InvalidArguments(e.to_string()))?;

        match self.execute_request(http_args).await {
            Ok(response) => {
                let success = response.status >= 200 && response.status < 300;
                Ok(ToolResult {
                    success,
                    result: response.format(),
                    display_preference: Some("markdown".to_string()),
                })
            }
            Err(e) => Ok(ToolResult {
                success: false,
                result: e,
                display_preference: None,
            }),
        }
    }
}

/// Extract domain from a URL for permission checking
pub fn extract_domain_from_url(url: &str) -> Option<String> {
    let parsed = url::Url::parse(url).ok()?;
    let host = parsed.host_str()?.to_string();
    match parsed.port() {
        Some(port) => Some(format!("{}:{}", host, port)),
        None => Some(host),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    mod extract_domain_from_url_tests {
        use super::extract_domain_from_url;

        #[test]
        fn returns_host_for_standard_https_url() {
            assert_eq!(
                extract_domain_from_url("https://api.example.com/users"),
                Some("api.example.com".to_string())
            );
        }

        #[test]
        fn includes_port_when_present() {
            assert_eq!(
                extract_domain_from_url("http://example.com:8080/path"),
                Some("example.com:8080".to_string())
            );
        }

        #[test]
        fn handles_ipv6_host_and_port() {
            assert_eq!(
                extract_domain_from_url("http://[::1]:8080/path"),
                Some("[::1]:8080".to_string())
            );
        }

        #[test]
        fn ignores_userinfo() {
            assert_eq!(
                extract_domain_from_url("http://user:pass@example.com"),
                Some("example.com".to_string())
            );
        }

        #[test]
        fn handles_internationalized_domain_names() {
            assert_eq!(
                extract_domain_from_url("https://bücher.example"),
                Some("xn--bcher-kva.example".to_string())
            );
        }

        #[test]
        fn returns_host_for_other_schemes() {
            assert_eq!(
                extract_domain_from_url("ftp://example.com/file"),
                Some("example.com".to_string())
            );
        }
    }

    #[test]
    fn test_http_method_serialization() {
        let method: HttpMethod = serde_json::from_str("\"POST\"").unwrap();
        assert_eq!(method, HttpMethod::Post);

        let method: HttpMethod = serde_json::from_str("\"GET\"").unwrap();
        assert_eq!(method, HttpMethod::Get);
    }

    #[test]
    fn test_http_response_format() {
        let mut headers = HashMap::new();
        headers.insert("Content-Type".to_string(), "application/json".to_string());

        let response = HttpResponse {
            status: 200,
            headers,
            body: r#"{"message": "Hello"}"#.to_string(),
        };

        let formatted = response.format();
        assert!(formatted.contains("HTTP Status: 200"));
        assert!(formatted.contains("Content-Type"));
        assert!(formatted.contains("Hello"));
    }

    #[test]
    fn test_tool_name_and_description() {
        let tool = HttpRequestTool::new();
        assert_eq!(tool.name(), "http_request");
        assert!(tool.description().contains("HTTP"));
    }

    #[tokio::test]
    #[cfg(feature = "http")]
    async fn test_http_request_invalid_url() {
        let tool = HttpRequestTool::new();
        let result = tool
            .execute(json!({
                "url": "not-a-valid-url"
            }))
            .await
            .unwrap();

        assert!(!result.success);
        assert!(result.result.contains("Invalid URL") || result.result.contains("URL scheme"));
    }

    #[tokio::test]
    #[cfg(feature = "http")]
    async fn test_http_request_get() {
        let tool = HttpRequestTool::new();
        let result = tool
            .execute(json!({
                "method": "GET",
                "url": "https://httpbin.org/get",
                "timeout_seconds": 10
            }))
            .await
            .unwrap();

        // Note: This test requires internet connectivity
        // httpbin.org is a reliable test service
        if result.success {
            assert!(result.result.contains("HTTP Status: 200"));
        }
    }

    // SSRF Protection Tests
    #[tokio::test]
    #[cfg(feature = "http")]
    async fn test_ssrf_private_ip_blocked() {
        let tool = HttpRequestTool::new();

        // Private IPv4 ranges
        let private_ips = vec![
            "http://10.0.0.1/test",
            "http://172.16.0.1/test",
            "http://192.168.1.1/test",
            "http://192.168.0.1/test",
        ];

        for url in private_ips {
            let result = tool
                .execute(json!({"url": url}))
                .await
                .unwrap();

            assert!(!result.success, "Should block private IP: {}", url);
            assert!(result.result.contains("SSRF protection"), "Should mention SSRF protection: {}", url);
            assert!(result.result.contains("private IP"), "Should mention private IP: {}", url);
        }
    }

    #[tokio::test]
    #[cfg(feature = "http")]
    async fn test_ssrf_loopback_blocked() {
        let tool = HttpRequestTool::new();

        // Loopback addresses
        let loopback_ips = vec![
            "http://127.0.0.1/",
            "http://127.0.0.1:8080/admin",
            "http://127.1.1.1/",
        ];

        for url in loopback_ips {
            let result = tool
                .execute(json!({"url": url}))
                .await
                .unwrap();

            assert!(!result.success, "Should block loopback IP: {}", url);
            assert!(result.result.contains("SSRF protection"), "Should mention SSRF protection: {}", url);
        }
    }

    #[tokio::test]
    #[cfg(feature = "http")]
    async fn test_ssrf_aws_metadata_blocked() {
        let tool = HttpRequestTool::new();

        // AWS metadata endpoint
        let result = tool
            .execute(json!({"url": "http://169.254.169.254/latest/meta-data/"}))
            .await
            .unwrap();

        assert!(!result.success);
        assert!(result.result.contains("SSRF protection"));
        assert!(result.result.contains("private IP"));
    }

    #[tokio::test]
    #[cfg(feature = "http")]
    async fn test_ssrf_valid_external_allowed() {
        let tool = HttpRequestTool::new();

        // This test checks that valid URLs don't trigger SSRF errors
        // We can't actually make the request in unit tests, so we just verify
        // the URL parsing and scheme validation passes

        let result = tool
            .execute(json!({
                "url": "https://example.com",
                "timeout_seconds": 1
            }))
            .await
            .unwrap();

        // Either success or a network error, NOT an SSRF error
        if !result.success {
            assert!(!result.result.contains("SSRF protection"),
                "Should not block valid external URL: {}", result.result);
        }
    }

    #[tokio::test]
    #[cfg(feature = "http")]
    async fn test_ssrf_dns_to_private_blocked() {
        // Note: This test would require a controlled DNS environment
        // In practice, we'd need a mock DNS server or known test domain
        // For now, we test the IP validation logic directly

        use std::net::IpAddr;

        // Test the internal IP checking function
        assert!(is_private_ip(&"10.0.0.1".parse::<IpAddr>().unwrap()));
        assert!(is_private_ip(&"127.0.0.1".parse::<IpAddr>().unwrap()));
        assert!(is_private_ip(&"169.254.169.254".parse::<IpAddr>().unwrap()));
        assert!(is_private_ip(&"::1".parse::<IpAddr>().unwrap()));
        assert!(!is_private_ip(&"8.8.8.8".parse::<IpAddr>().unwrap()));
        assert!(!is_private_ip(&"1.1.1.1".parse::<IpAddr>().unwrap()));
    }

    #[test]
    fn test_http_request_args_domain_extraction() {
        let args = HttpRequestArgs {
            method: HttpMethod::Get,
            url: "https://api.github.com/users/octocat".to_string(),
            headers: None,
            body: None,
            timeout_seconds: 30,
            max_response_size: 1024 * 1024,
        };

        assert_eq!(
            args.extract_domain(),
            Some("api.github.com".to_string())
        );
    }
}
