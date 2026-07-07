use async_trait::async_trait;
use bamboo_agent_core::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult};
use futures::StreamExt;
use regex::Regex;
use serde::Deserialize;
use serde_json::json;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::OnceLock;
use std::time::Duration;

const MAX_RESPONSE_BYTES: usize = 1_000_000;
/// Cap on redirect hops we follow manually (reqwest's auto-follow is disabled so
/// each hop can be re-validated against the SSRF guard).
const MAX_REDIRECTS: usize = 10;

// Static, compile-time-constant patterns: compile each exactly once and reuse.
// `expect` is safe here because the patterns are hardcoded and verified valid.
static SCRIPT_RE: OnceLock<Regex> = OnceLock::new();
static STYLE_RE: OnceLock<Regex> = OnceLock::new();
static TAG_RE: OnceLock<Regex> = OnceLock::new();
static WHITESPACE_RE: OnceLock<Regex> = OnceLock::new();

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
        let script_re = SCRIPT_RE.get_or_init(|| {
            Regex::new(r"(?is)<script[^>]*>.*?</script>").expect("valid static regex")
        });
        let style_re = STYLE_RE.get_or_init(|| {
            Regex::new(r"(?is)<style[^>]*>.*?</style>").expect("valid static regex")
        });
        let tag_re =
            TAG_RE.get_or_init(|| Regex::new(r"(?is)<[^>]+>").expect("valid static regex"));
        let whitespace_re =
            WHITESPACE_RE.get_or_init(|| Regex::new(r"[ \t\n\r]+").expect("valid static regex"));

        let without_scripts = script_re.replace_all(input, " ");
        let without_styles = style_re.replace_all(&without_scripts, " ");
        let without_tags = tag_re.replace_all(&without_styles, " ");
        Ok(whitespace_re
            .replace_all(&without_tags, " ")
            .trim()
            .to_string())
    }

    fn is_disallowed_ipv4(ipv4: Ipv4Addr) -> bool {
        let octets = ipv4.octets();
        // 100.64.0.0/10 — CGNAT shared address space (Ipv4Addr::is_shared is
        // still unstable, so match the range directly).
        let is_shared = octets[0] == 100 && (64..=127).contains(&octets[1]);
        // 0.0.0.0/8 — `is_unspecified()` only matches the exact 0.0.0.0, but the
        // whole /8 is routed as loopback-equivalent on several OSes.
        let is_this_network = octets[0] == 0;
        ipv4.is_loopback()
            || ipv4.is_private()
            || ipv4.is_link_local()
            || ipv4.is_multicast()
            || ipv4.is_unspecified()
            || is_shared
            || is_this_network
    }

    /// The IPv4 address embedded in an IPv6 address, across the forms that carry
    /// a routable v4 target: IPv4-mapped (`::ffff:a.b.c.d`), the deprecated
    /// IPv4-compatible (`::a.b.c.d`), and NAT64 (`64:ff9b::/96`). Returns `None`
    /// for `::` / `::1`, which the plain IPv6 checks already handle.
    fn embedded_ipv4(ipv6: std::net::Ipv6Addr) -> Option<Ipv4Addr> {
        if let Some(mapped) = ipv6.to_ipv4_mapped() {
            return Some(mapped);
        }
        let seg = ipv6.segments();
        let low = Ipv4Addr::new(
            (seg[6] >> 8) as u8,
            (seg[6] & 0xff) as u8,
            (seg[7] >> 8) as u8,
            (seg[7] & 0xff) as u8,
        );
        // NAT64 well-known prefix 64:ff9b::/96.
        if seg[0] == 0x0064 && seg[1] == 0xff9b && seg[2..6].iter().all(|s| *s == 0) {
            return Some(low);
        }
        // Deprecated IPv4-compatible ::a.b.c.d (high 96 bits zero), excluding
        // :: (unspecified) and ::1 (loopback).
        if seg[0..6].iter().all(|s| *s == 0) && !(seg[6] == 0 && (seg[7] == 0 || seg[7] == 1)) {
            return Some(low);
        }
        None
    }

    fn is_disallowed_ip(ip: IpAddr) -> bool {
        match ip {
            IpAddr::V4(ipv4) => Self::is_disallowed_ipv4(ipv4),
            IpAddr::V6(ipv6) => {
                // An embedded IPv4 target (mapped / compatible / NAT64) is judged
                // by the IPv4 rules — otherwise ::ffff:127.0.0.1, ::169.254.169.254,
                // 64:ff9b::169.254.169.254 etc. slip past the IPv6-only checks.
                if let Some(v4) = Self::embedded_ipv4(ipv6) {
                    if Self::is_disallowed_ipv4(v4) {
                        return true;
                    }
                }
                let segments = ipv6.segments();
                let first = segments[0];
                let is_unique_local = (first & 0xfe00) == 0xfc00;
                let is_unicast_link_local = (first & 0xffc0) == 0xfe80;
                ipv6.is_loopback()
                    || ipv6.is_multicast()
                    || ipv6.is_unspecified()
                    || is_unique_local
                    || is_unicast_link_local
            }
        }
    }

    /// Validate a URL's host against the SSRF guard and return the concrete
    /// addresses to connect to. Resolving here and **pinning** these exact
    /// addresses on the request (so reqwest doesn't re-resolve) closes the
    /// DNS-rebinding TOCTOU where the validated IP differs from the connected IP.
    /// Rejects if the host is disallowed or *any* resolved address is restricted.
    async fn validate_and_resolve(url: &url::Url) -> Result<(String, Vec<SocketAddr>), ToolError> {
        let host = url
            .host_str()
            .ok_or_else(|| ToolError::InvalidArguments("URL must include a host".to_string()))?;
        if Self::is_disallowed_host(host) {
            return Err(ToolError::Execution(format!(
                "Refusing to fetch restricted host: {}",
                host
            )));
        }
        let port = url.port_or_known_default().unwrap_or(80);

        let addrs: Vec<SocketAddr> = if let Ok(ip) = host.parse::<IpAddr>() {
            vec![SocketAddr::new(ip, port)]
        } else {
            tokio::net::lookup_host((host, port))
                .await
                .map_err(|e| {
                    ToolError::Execution(format!("Failed to resolve host '{}': {}", host, e))
                })?
                .collect()
        };

        if addrs.is_empty() {
            return Err(ToolError::Execution(format!(
                "Host '{}' resolved to no addresses",
                host
            )));
        }
        if Self::resolved_ips_include_disallowed(addrs.iter().map(|addr| addr.ip())) {
            return Err(ToolError::Execution(format!(
                "Refusing to fetch host '{}' because it resolved to a restricted IP",
                host
            )));
        }

        Ok((host.to_string(), addrs))
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

    fn classify(&self, _args: &serde_json::Value) -> ToolClass {
        ToolClass::READONLY_PARALLEL.promotable()
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

    async fn invoke(
        &self,
        args: serde_json::Value,
        _ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let parsed: WebFetchArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid WebFetch args: {}", e)))?;
        let url = parsed.url.trim();
        let mut current_url = url::Url::parse(url)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid URL: {}", e)))?;

        // Follow redirects manually: reqwest's auto-follow would re-resolve and
        // connect to the redirect target WITHOUT re-running the SSRF guard, so
        // `http://ok.example/ → 302 http://169.254.169.254/…` would leak. We
        // validate-and-pin every hop instead. The WHOLE chain shares one 30s
        // deadline so a server dribbling redirects can't stretch the request to
        // MAX_REDIRECTS × 30s.
        let fetch = async {
            let mut hop_count = 0usize;
            loop {
                let scheme = current_url.scheme();
                if scheme != "http" && scheme != "https" {
                    return Err(ToolError::InvalidArguments(
                        "Only http/https URLs are allowed".to_string(),
                    ));
                }

                let (host, addrs) = Self::validate_and_resolve(&current_url).await?;

                let client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(30))
                    // Don't let reqwest silently follow to an unvalidated host.
                    .redirect(reqwest::redirect::Policy::none())
                    // Pin the connection to the exact addresses we just validated
                    // so the guard and the socket use the same IP (no rebind).
                    .resolve_to_addrs(&host, &addrs)
                    .build()
                    .map_err(|e| {
                        ToolError::Execution(format!("Failed to build HTTP client: {}", e))
                    })?;

                let resp = client
                    .get(current_url.clone())
                    .send()
                    .await
                    .map_err(|e| ToolError::Execution(format!("Failed to fetch URL: {}", e)))?;

                if resp.status().is_redirection() {
                    let location = resp
                        .headers()
                        .get(reqwest::header::LOCATION)
                        .and_then(|value| value.to_str().ok());
                    if let Some(location) = location {
                        let next = current_url.join(location).map_err(|e| {
                            ToolError::Execution(format!("Invalid redirect Location: {}", e))
                        })?;
                        if current_url == next {
                            return Ok(resp); // self-redirect; stop to avoid a loop
                        }
                        hop_count += 1;
                        if hop_count > MAX_REDIRECTS {
                            return Err(ToolError::Execution(format!(
                                "Too many redirects (>{})",
                                MAX_REDIRECTS
                            )));
                        }
                        current_url = next;
                        continue;
                    }
                }
                return Ok(resp);
            }
        };
        let response = tokio::time::timeout(Duration::from_secs(30), fetch)
            .await
            .map_err(|_| ToolError::Execution("WebFetch timed out after 30s".to_string()))??;

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

        Ok(ToolOutcome::Completed(ToolResult {
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
            images: Vec::new(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_html_strips_scripts_styles_tags_and_collapses_whitespace() {
        // Scripts and styles are dropped, tags are stripped, and runs of
        // whitespace are collapsed to a single space, then trimmed.
        let html = "<html><head><style>body{color:red}</style></head><body>\
<script>alert(1)</script><h1>Title</h1><p>Hello   world</p></body></html>";
        assert_eq!(WebFetchTool::strip_html(html).unwrap(), "Title Hello world");

        // A second call exercises the already-initialized (cached) static regexes
        // and must produce identical semantics.
        let html2 = "<div>  <b>A</b>  <i>B</i>  </div>";
        assert_eq!(WebFetchTool::strip_html(html2).unwrap(), "A B");
    }

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
    fn is_disallowed_ip_catches_ipv4_mapped_ipv6_and_cgnat() {
        // IPv4-mapped IPv6 (::ffff:a.b.c.d) must be judged by IPv4 rules —
        // otherwise loopback/link-local/private slip past the v6-only checks.
        for mapped in [
            "::ffff:127.0.0.1",
            "::ffff:10.0.0.5",
            "::ffff:192.168.1.1",
            "::ffff:169.254.169.254", // cloud metadata
        ] {
            assert!(
                WebFetchTool::is_disallowed_ip(mapped.parse().unwrap()),
                "expected {mapped} to be disallowed"
            );
        }
        // Deprecated IPv4-compatible (::a.b.c.d) and NAT64 (64:ff9b::/96) forms
        // also embed a routable v4 target and must be canonicalized.
        for embedded in [
            "::169.254.169.254",
            "::127.0.0.1",
            "64:ff9b::169.254.169.254",
            "64:ff9b::7f00:1", // 127.0.0.1
        ] {
            assert!(
                WebFetchTool::is_disallowed_ip(embedded.parse().unwrap()),
                "expected {embedded} to be disallowed"
            );
        }

        // CGNAT 100.64.0.0/10 shared space.
        assert!(WebFetchTool::is_disallowed_ip(
            "100.64.0.1".parse().unwrap()
        ));
        assert!(WebFetchTool::is_disallowed_ip(
            "100.127.255.255".parse().unwrap()
        ));

        // 0.0.0.0/8 — not just the exact 0.0.0.0.
        assert!(WebFetchTool::is_disallowed_ip("0.0.0.0".parse().unwrap()));
        assert!(WebFetchTool::is_disallowed_ip("0.1.2.3".parse().unwrap()));

        // Boundaries just outside CGNAT and genuine public addresses stay allowed.
        assert!(!WebFetchTool::is_disallowed_ip(
            "100.63.255.255".parse().unwrap()
        ));
        assert!(!WebFetchTool::is_disallowed_ip(
            "100.128.0.0".parse().unwrap()
        ));
        assert!(!WebFetchTool::is_disallowed_ip("8.8.8.8".parse().unwrap()));
        assert!(!WebFetchTool::is_disallowed_ip(
            "2606:4700:4700::1111".parse().unwrap()
        ));
        // Same reachable through the host-string path.
        assert!(WebFetchTool::is_disallowed_host("::ffff:169.254.169.254"));
        assert!(WebFetchTool::is_disallowed_host("100.64.0.1"));
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
            .invoke(
                json!({
                    "url": "file:///etc/passwd",
                    "prompt": "read"
                }),
                ToolCtx::none("t"),
            )
            .await
            .expect_err("non-http scheme should fail");

        assert!(matches!(err, ToolError::InvalidArguments(msg) if msg.contains("http/https")));
    }

    #[tokio::test]
    async fn execute_rejects_restricted_hosts_before_network_call() {
        let tool = WebFetchTool::new();
        let err = tool
            .invoke(
                json!({
                    "url": "http://localhost:8080",
                    "prompt": "read"
                }),
                ToolCtx::none("t"),
            )
            .await
            .expect_err("localhost should be blocked");

        assert!(matches!(err, ToolError::Execution(msg) if msg.contains("restricted host")));
    }
}
