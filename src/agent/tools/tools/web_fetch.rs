use crate::agent::core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use futures::StreamExt;
use regex::Regex;
use serde::Deserialize;
use serde_json::json;
use std::net::IpAddr;
use std::time::Duration;

const MAX_RESPONSE_BYTES: usize = 1_000_000;

#[derive(Debug, Deserialize)]
struct WebFetchArgs {
    url: String,
    prompt: String,
}

pub struct WebFetchTool;

impl WebFetchTool {
    pub fn new() -> Self {
        Self
    }

    fn strip_html(input: &str) -> Result<String, ToolError> {
        let script_re = Regex::new(r"(?is)<script[^>]*>.*?</script>")
            .map_err(|e| ToolError::Execution(format!("Failed to compile script regex: {}", e)))?;
        let style_re = Regex::new(r"(?is)<style[^>]*>.*?</style>")
            .map_err(|e| ToolError::Execution(format!("Failed to compile style regex: {}", e)))?;
        let tag_re = Regex::new(r"(?is)<[^>]+>")
            .map_err(|e| ToolError::Execution(format!("Failed to compile tag regex: {}", e)))?;
        let whitespace_re = Regex::new(r"[ \t\n\r]+").map_err(|e| {
            ToolError::Execution(format!("Failed to compile whitespace regex: {}", e))
        })?;

        let without_scripts = script_re.replace_all(input, " ");
        let without_styles = style_re.replace_all(&without_scripts, " ");
        let without_tags = tag_re.replace_all(&without_styles, " ");
        Ok(whitespace_re
            .replace_all(&without_tags, " ")
            .trim()
            .to_string())
    }

    fn is_disallowed_ip(ip: IpAddr) -> bool {
        match ip {
            IpAddr::V4(ipv4) => {
                ipv4.is_loopback()
                    || ipv4.is_private()
                    || ipv4.is_link_local()
                    || ipv4.is_multicast()
                    || ipv4.is_unspecified()
            }
            IpAddr::V6(ipv6) => {
                ipv6.is_loopback()
                    || ipv6.is_multicast()
                    || ipv6.is_unspecified()
                    || ipv6.is_unique_local()
                    || ipv6.is_unicast_link_local()
            }
        }
    }

    fn is_disallowed_host(host: &str) -> bool {
        let host = host.trim().to_ascii_lowercase();
        if host == "localhost" || host.ends_with(".localhost") || host.ends_with(".local") {
            return true;
        }

        let Ok(ip) = host.parse::<IpAddr>() else {
            return false;
        };
        Self::is_disallowed_ip(ip)
    }

    fn resolved_ips_include_disallowed<I>(ips: I) -> bool
    where
        I: IntoIterator<Item = IpAddr>,
    {
        ips.into_iter().any(Self::is_disallowed_ip)
    }

    async fn host_resolves_to_disallowed_ip(host: &str, port: u16) -> Result<bool, ToolError> {
        let addrs = tokio::net::lookup_host((host, port)).await.map_err(|e| {
            ToolError::Execution(format!("Failed to resolve host '{}': {}", host, e))
        })?;
        let ips: Vec<IpAddr> = addrs.map(|addr| addr.ip()).collect();
        Ok(Self::resolved_ips_include_disallowed(ips))
    }
}

impl Default for WebFetchTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for WebFetchTool {
    fn name(&self) -> &str {
        "WebFetch"
    }

    fn description(&self) -> &str {
        "Fetch an HTTP(S) URL and return a cleaned text excerpt plus metadata. The `prompt` field is caller context only; this tool does not run an extra model."
    }

    fn mutability(&self) -> crate::agent::tools::ToolMutability {
        crate::agent::tools::ToolMutability::ReadOnly
    }

    fn concurrency_safe(&self) -> bool {
        true
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "format": "uri",
                    "description": "The URL to fetch"
                },
                "prompt": {
                    "type": "string",
                    "description": "Caller-supplied extraction intent note; echoed in output for downstream processing"
                }
            },
            "required": ["url", "prompt"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let parsed: WebFetchArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid WebFetch args: {}", e)))?;
        let url = parsed.url.trim();
        let parsed_url = url::Url::parse(url)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid URL: {}", e)))?;
        let scheme = parsed_url.scheme();
        if scheme != "http" && scheme != "https" {
            return Err(ToolError::InvalidArguments(
                "Only http/https URLs are allowed".to_string(),
            ));
        }
        let Some(host) = parsed_url.host_str() else {
            return Err(ToolError::InvalidArguments(
                "URL must include a host".to_string(),
            ));
        };
        if Self::is_disallowed_host(host) {
            return Err(ToolError::Execution(format!(
                "Refusing to fetch restricted host: {}",
                host
            )));
        }
        if host.parse::<IpAddr>().is_err() {
            let port = parsed_url.port_or_known_default().unwrap_or(80);
            if Self::host_resolves_to_disallowed_ip(host, port).await? {
                return Err(ToolError::Execution(format!(
                    "Refusing to fetch host '{}' because DNS resolved to a restricted IP",
                    host
                )));
            }
        }

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| ToolError::Execution(format!("Failed to build HTTP client: {}", e)))?;

        let response = client
            .get(url)
            .send()
            .await
            .map_err(|e| ToolError::Execution(format!("Failed to fetch URL: {}", e)))?;

        let status = response.status().as_u16();
        let mut stream = response.bytes_stream();
        let mut bytes = Vec::with_capacity(64 * 1024);
        let mut response_truncated = false;
        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result.map_err(|e| {
                ToolError::Execution(format!("Failed reading response body: {}", e))
            })?;
            if bytes.len() + chunk.len() > MAX_RESPONSE_BYTES {
                let remaining = MAX_RESPONSE_BYTES.saturating_sub(bytes.len());
                if remaining > 0 {
                    bytes.extend_from_slice(&chunk[..remaining]);
                }
                response_truncated = true;
                break;
            }
            bytes.extend_from_slice(&chunk);
        }

        let body = String::from_utf8_lossy(&bytes).to_string();

        let text = Self::strip_html(&body)?;
        let excerpt: String = text.chars().take(20_000).collect();

        Ok(ToolResult {
            success: true,
            result: json!({
                "url": parsed.url,
                "status": status,
                "prompt": parsed.prompt,
                "content": excerpt,
                "response_truncated": response_truncated,
            })
            .to_string(),
            display_preference: Some("Collapsible".to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disallowed_host_rejects_local_and_private_targets() {
        assert!(WebFetchTool::is_disallowed_host("localhost"));
        assert!(WebFetchTool::is_disallowed_host("api.localhost"));
        assert!(WebFetchTool::is_disallowed_host("service.local"));
        assert!(WebFetchTool::is_disallowed_host("127.0.0.1"));
        assert!(WebFetchTool::is_disallowed_host("10.0.0.1"));
        assert!(WebFetchTool::is_disallowed_host("192.168.1.1"));
        assert!(WebFetchTool::is_disallowed_host("::1"));
        assert!(!WebFetchTool::is_disallowed_host("example.com"));
        assert!(!WebFetchTool::is_disallowed_host("8.8.8.8"));
    }

    #[test]
    fn resolved_ips_include_disallowed_detects_any_private_or_loopback_ip() {
        assert!(WebFetchTool::resolved_ips_include_disallowed(vec![
            "8.8.8.8".parse::<IpAddr>().unwrap(),
            "10.0.0.8".parse::<IpAddr>().unwrap(),
        ]));
        assert!(WebFetchTool::resolved_ips_include_disallowed(vec!["::1"
            .parse::<IpAddr>()
            .unwrap(),]));
        assert!(!WebFetchTool::resolved_ips_include_disallowed(vec![
            "1.1.1.1".parse::<IpAddr>().unwrap(),
            "8.8.8.8".parse::<IpAddr>().unwrap(),
        ]));
    }

    #[tokio::test]
    async fn execute_rejects_non_http_schemes() {
        let tool = WebFetchTool::new();
        let err = tool
            .execute(json!({
                "url": "file:///etc/passwd",
                "prompt": "read"
            }))
            .await
            .expect_err("non-http scheme should fail");

        assert!(matches!(err, ToolError::InvalidArguments(msg) if msg.contains("http/https")));
    }

    #[tokio::test]
    async fn execute_rejects_restricted_hosts_before_network_call() {
        let tool = WebFetchTool::new();
        let err = tool
            .execute(json!({
                "url": "http://localhost:8080",
                "prompt": "read"
            }))
            .await
            .expect_err("localhost should be blocked");

        assert!(matches!(err, ToolError::Execution(msg) if msg.contains("restricted host")));
    }
}
