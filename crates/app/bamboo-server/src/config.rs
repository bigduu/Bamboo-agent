//! Server configuration utilities
//!
//! This module provides functions to configure security headers and CORS policies
//! for the Actix-web server based on the deployment environment.
//!
//! # Security Headers
//!
//! The server applies production-ready security headers:
//! - X-Frame-Options: DENY
//! - X-Content-Type-Options: nosniff
//! - X-XSS-Protection: 1; mode=block
//! - Referrer-Policy: strict-origin-when-cross-origin
//! - Content-Security-Policy: Customizable CSP
//!
//! # CORS Configuration
//!
//! CORS policies are automatically adjusted based on bind address:
//! - **localhost**: Development mode with permissive CORS
//! - **0.0.0.0**: Docker production mode (localhost only via reverse proxy)
//! - **Custom**: Restrictive CORS for specific addresses

use actix_cors::Cors;
use actix_governor::governor::middleware::NoOpMiddleware;
use actix_governor::{GovernorConfig, GovernorConfigBuilder, KeyExtractor, SimpleKeyExtractionError};
use actix_web::body::MessageBody;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::http::header;
use actix_web::middleware::{DefaultHeaders, Next};
use std::collections::HashSet;
use std::net::IpAddr;
use tracing::info;
use tracing::warn;

/// Default sustained per-IP request rate (requests/second) for the production
/// (network-exposed) server. Overridable via `BAMBOO_RATE_LIMIT_PER_SECOND`.
const DEFAULT_RATE_LIMIT_PER_SECOND: u64 = 10;
/// Default per-IP burst allowance. Overridable via `BAMBOO_RATE_LIMIT_BURST`.
const DEFAULT_RATE_LIMIT_BURST: u32 = 20;

/// Rate-limiter key extractor. Defaults to the TCP peer IP (non-spoofable), but
/// can be switched to an OPT-IN `X-Forwarded-For` mode for reverse-proxy
/// deployments where the peer IP is always the proxy (which would otherwise
/// collapse the per-IP limit to global). #169.
///
/// SECURITY: XFF mode is only safe behind a trusted proxy — a directly-reachable
/// server trusting XFF lets any client spoof its key and bypass the limiter. It
/// is therefore off unless `BAMBOO_RATE_LIMIT_TRUST_XFF` is set, and it fails
/// CLOSED to the peer IP whenever the header is absent, unparseable, or shorter
/// than the configured trusted-hop count (so a rogue/short XFF can't inject a key).
#[derive(Clone, Debug)]
pub struct ClientIpKeyExtractor {
    trust_xff: bool,
    /// Number of trusted proxies between us and the client. The real client is
    /// the `trusted_hops`-th entry from the RIGHT of `X-Forwarded-For` (each proxy
    /// appends the peer it saw as the request travels outward-to-inward).
    trusted_hops: usize,
}

impl ClientIpKeyExtractor {
    /// The default, non-spoofable peer-IP extractor.
    #[cfg(test)]
    fn peer_ip() -> Self {
        Self {
            trust_xff: false,
            trusted_hops: 1,
        }
    }

    fn client_ip_from_xff(&self, req: &ServiceRequest) -> Option<IpAddr> {
        let hops = self.trusted_hops.max(1);
        let header_value = req.headers().get("x-forwarded-for")?.to_str().ok()?;
        let entries: Vec<&str> = header_value
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();
        // Fail closed: a header with fewer entries than the trusted hop count is
        // not the shape a trusted proxy chain produces, so don't trust it.
        if entries.len() < hops {
            return None;
        }
        parse_forwarded_ip(entries[entries.len() - hops])
    }
}

impl KeyExtractor for ClientIpKeyExtractor {
    type Key = IpAddr;
    type KeyExtractionError = SimpleKeyExtractionError<&'static str>;

    fn extract(&self, req: &ServiceRequest) -> Result<Self::Key, Self::KeyExtractionError> {
        if self.trust_xff {
            if let Some(client) = self.client_ip_from_xff(req) {
                return Ok(mask_ipv6_prefix(client));
            }
            // else: fall through to the peer IP (fail closed).
        }
        let ip = req.peer_addr().map(|socket| socket.ip()).ok_or_else(|| {
            SimpleKeyExtractionError::new("Could not extract peer IP address from request")
        })?;
        Ok(mask_ipv6_prefix(ip))
    }
}

/// Rate-limit IPv6 clients per /56 prefix rather than per address (customers are
/// often handed a whole prefix), mirroring `PeerIpKeyExtractor`. IPv4 is unchanged.
fn mask_ipv6_prefix(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => {
            let mut octets = v6.octets();
            octets[7..16].fill(0);
            IpAddr::V6(octets.into())
        }
        v4 => v4,
    }
}

/// Parse one `X-Forwarded-For` entry into an IP, tolerating a `host:port` or
/// bracketed-IPv6 form some proxies emit.
fn parse_forwarded_ip(s: &str) -> Option<IpAddr> {
    let s = s.trim();
    if let Ok(ip) = s.parse::<IpAddr>() {
        return Some(ip);
    }
    if let Ok(sa) = s.parse::<std::net::SocketAddr>() {
        return Some(sa.ip());
    }
    // Bracketed IPv6 without a port, e.g. "[::1]".
    let unbracketed = s.strip_prefix('[').and_then(|x| x.strip_suffix(']'))?;
    unbracketed.parse::<IpAddr>().ok()
}

fn rate_limiter_config(
    per_second: u64,
    burst: u32,
    key_extractor: ClientIpKeyExtractor,
) -> GovernorConfig<ClientIpKeyExtractor, NoOpMiddleware> {
    // One cell replenishes every `1000 / per_second` ms (>=1), allowing `per_second`
    // sustained req/s with a `burst` bucket. Clamp to >=1 so a bad env value can't
    // produce a zero period/burst (which finish() would reject).
    let ms_per_request = (1000 / per_second.max(1)).max(1);
    GovernorConfigBuilder::default()
        .milliseconds_per_request(ms_per_request)
        .burst_size(burst.max(1))
        .key_extractor(key_extractor)
        .finish()
        .expect("rate limiter config is valid (non-zero period and burst)")
}

/// Build the per-IP rate-limiter config applied to the PRODUCTION (network-bound)
/// server via the `actix-governor` middleware. Throttles each client IP to
/// `BAMBOO_RATE_LIMIT_PER_SECOND` (default 10) req/s with a `BAMBOO_RATE_LIMIT_BURST`
/// (default 20) burst, returning 429 Too Many Requests when exceeded. Desktop
/// (localhost) mode does not apply it. #13.
///
/// Keys on the TCP PEER IP by default (non-spoofable). Behind a reverse proxy
/// every client shares the proxy's IP, collapsing the per-IP limit to global; set
/// `BAMBOO_RATE_LIMIT_TRUST_XFF=1` to key on `X-Forwarded-For` instead (with
/// `BAMBOO_RATE_LIMIT_TRUSTED_HOPS`, default one hop). #169. XFF mode is OPT-IN
/// because trusting the header when NOT behind a trusted proxy lets any client
/// spoof its rate-limit key; see [`ClientIpKeyExtractor`].
pub fn build_rate_limiter() -> GovernorConfig<ClientIpKeyExtractor, NoOpMiddleware> {
    let per_second = std::env::var("BAMBOO_RATE_LIMIT_PER_SECOND")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_RATE_LIMIT_PER_SECOND);
    let burst = std::env::var("BAMBOO_RATE_LIMIT_BURST")
        .ok()
        .and_then(|v| v.trim().parse::<u32>().ok())
        .unwrap_or(DEFAULT_RATE_LIMIT_BURST);

    let trust_xff = std::env::var("BAMBOO_RATE_LIMIT_TRUST_XFF")
        .ok()
        .map(|v| {
            let t = v.trim();
            t == "1" || t.eq_ignore_ascii_case("true")
        })
        .unwrap_or(false);
    let trusted_hops = std::env::var("BAMBOO_RATE_LIMIT_TRUSTED_HOPS")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n >= 1)
        .unwrap_or(1);

    if trust_xff {
        warn!(
            "Rate limiter is trusting X-Forwarded-For (trusted_hops={trusted_hops}). \
             Only enable this when the server is reachable exclusively through a trusted \
             reverse proxy — otherwise clients can spoof their rate-limit key."
        );
    }

    rate_limiter_config(
        per_second,
        burst,
        ClientIpKeyExtractor {
            trust_xff,
            trusted_hops,
        },
    )
}

/// True when `bind` is a loopback/desktop address, for which the per-IP DoS
/// rate limiter ([`build_rate_limiter`], #13) is intentionally SKIPPED. The
/// desktop sidecar serves the local frontend, which legitimately bursts ~45
/// hashed `/assets/*` requests on load and would otherwise trip the 429 limit
/// (`burst` default 20). Mirrors the loopback special-casing already used for
/// CORS; network binds (`0.0.0.0`) are still throttled.
pub fn is_loopback_bind(bind: &str) -> bool {
    matches!(bind, "127.0.0.1" | "localhost" | "::1")
}

// Keep the default CSP reasonably strict while remaining compatible with the Lotus UI runtime.
// Lotus + Ant Design inject runtime styles, so `style-src 'unsafe-inline'` is required for the
// current frontend bundle. Keep scripts strict (no `unsafe-eval`) and allow operators to override
// via `BAMBOO_CSP` when needed.
const DEFAULT_CSP: &str = concat!(
    "default-src 'self'; ",
    "base-uri 'self'; ",
    "object-src 'none'; ",
    "frame-ancestors 'none'; ",
    "script-src 'self'; ",
    "style-src 'self' 'unsafe-inline'; ",
    "img-src 'self' data: https:; ",
    "font-src 'self' data:; ",
    "connect-src 'self' ws: wss: http://127.0.0.1:* http://localhost:* http://bodhi.bigduu.com:9562 https://bodhi.bigduu.com:9562; ",
    "form-action 'self';"
);

fn normalize_csp_source_token(token: &str) -> Option<String> {
    let trimmed = token.trim();
    if trimmed.is_empty() {
        return None;
    }

    if trimmed.starts_with("'") {
        return Some(trimmed.to_string());
    }

    normalize_origin(trimmed).or_else(|| Some(trimmed.to_string()))
}

fn parse_csp_connect_src_append(raw: &str) -> Vec<String> {
    raw.split(|c: char| c == ',' || c.is_ascii_whitespace())
        .filter_map(normalize_csp_source_token)
        .collect()
}

fn append_connect_src_sources(base_csp: &str, extra_sources: &[String]) -> String {
    if extra_sources.is_empty() {
        return base_csp.to_string();
    }

    let connect_src_marker = "connect-src ";
    if let Some(start) = base_csp.find(connect_src_marker) {
        let value_start = start + connect_src_marker.len();
        if let Some(relative_end) = base_csp[value_start..].find(';') {
            let value_end = value_start + relative_end;
            let existing_value = base_csp[value_start..value_end].trim();
            let mut merged = if existing_value.is_empty() {
                String::new()
            } else {
                existing_value.to_string()
            };

            for source in extra_sources {
                if merged.split_whitespace().any(|token| token == source) {
                    continue;
                }
                if !merged.is_empty() {
                    merged.push(' ');
                }
                merged.push_str(source);
            }

            let mut result = String::with_capacity(base_csp.len() + merged.len() + 1);
            result.push_str(&base_csp[..value_start]);
            result.push_str(&merged);
            result.push_str(&base_csp[value_end..]);
            return result;
        }
    }

    base_csp.to_string()
}

fn resolve_default_csp() -> String {
    const ENV_KEY: &str = "BAMBOO_CSP_CONNECT_SRC";

    let extra_sources = match std::env::var(ENV_KEY) {
        Ok(raw) => parse_csp_connect_src_append(&raw),
        Err(_) => Vec::new(),
    };

    if !extra_sources.is_empty() {
        info!(
            "Extending CSP connect-src via {} with {} source(s)",
            ENV_KEY,
            extra_sources.len()
        );
    }

    append_connect_src_sources(DEFAULT_CSP, &extra_sources)
}

fn resolve_csp_header_value(override_value: Option<&str>) -> header::HeaderValue {
    let default_csp = resolve_default_csp();
    let csp = override_value.unwrap_or(default_csp.as_str());
    match header::HeaderValue::from_str(csp) {
        Ok(v) => v,
        Err(e) => {
            // Avoid failing to start due to a malformed override; fall back to the safe default.
            warn!(
                "Invalid BAMBOO_CSP value ({}); falling back to DEFAULT_CSP",
                e
            );
            header::HeaderValue::from_str(default_csp.as_str())
                .unwrap_or_else(|_| header::HeaderValue::from_static(DEFAULT_CSP))
        }
    }
}

/// CORS allowlist sourced from env vars.
///
/// Supported entries:
/// - Exact origins: `https://app.example.com`, `http://localhost:5173`
/// - Hosts (any scheme/port): `app.example.com`, `127.0.0.1`
/// - Wildcard subdomains (any scheme/port): `*.example.com`
#[derive(Debug, Clone, Default)]
struct CorsAllowlist {
    exact_origins: HashSet<String>,
    hosts: Vec<HostPattern>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum HostPattern {
    Exact(String),
    Suffix(String), // stored with leading dot, e.g. ".example.com"
}

fn normalize_origin(origin: &str) -> Option<String> {
    let url = url::Url::parse(origin).ok()?;

    let scheme = url.scheme().to_ascii_lowercase();
    let host = url.host()?;
    let host_str = match host {
        url::Host::Domain(d) => d.to_ascii_lowercase(),
        url::Host::Ipv4(v4) => v4.to_string(),
        url::Host::Ipv6(v6) => format!("[{v6}]"),
    };

    let port = url.port();
    let default_port = match scheme.as_str() {
        "http" => Some(80),
        "https" => Some(443),
        _ => None,
    };
    let port = match (port, default_port) {
        (Some(p), Some(d)) if p == d => None,
        (p, _) => p,
    };

    Some(match port {
        Some(p) => format!("{scheme}://{host_str}:{p}"),
        None => format!("{scheme}://{host_str}"),
    })
}

fn parse_cors_allowlist(raw: &str) -> CorsAllowlist {
    let mut allow = CorsAllowlist::default();

    for item in raw.split(',') {
        let token = item.trim();
        if token.is_empty() {
            continue;
        }

        if token.contains("://") {
            // Exact origin match. Normalize to an origin-like form so common inputs
            // (trailing slashes, explicit :443, etc.) still match real Origin headers.
            match normalize_origin(token) {
                Some(origin) => {
                    allow.exact_origins.insert(origin);
                }
                None => {
                    warn!(
                        "Invalid CORS origin entry '{}'; expected an origin like https://app.example.com",
                        token
                    );
                }
            }
            continue;
        }

        // Host-based match.
        let host = token.to_ascii_lowercase();
        if let Some(rest) = host.strip_prefix("*.") {
            // Wildcard subdomains.
            if !rest.is_empty() {
                allow.hosts.push(HostPattern::Suffix(format!(".{rest}")));
            }
        } else {
            allow.hosts.push(HostPattern::Exact(host));
        }
    }

    allow
}

fn parse_cors_allowlist_env() -> CorsAllowlist {
    // Comma-separated list. Examples:
    //   BAMBOO_CORS_ALLOW_ORIGINS="https://app.example.com,http://localhost:5173,*.example.com"
    //   BAMBOO_CORS_ALLOW_ORIGINS="app.example.com,127.0.0.1"
    const ENV_KEY: &str = "BAMBOO_CORS_ALLOW_ORIGINS";

    let raw = match std::env::var(ENV_KEY) {
        Ok(v) => v,
        Err(_) => return CorsAllowlist::default(),
    };

    let allow = parse_cors_allowlist(&raw);

    if !allow.exact_origins.is_empty() || !allow.hosts.is_empty() {
        info!(
            "CORS allowlist enabled via BAMBOO_CORS_ALLOW_ORIGINS ({} exact origin(s), {} host pattern(s))",
            allow.exact_origins.len(),
            allow.hosts.len()
        );
    }

    allow
}

fn is_allowed_by_allowlist(origin: &str, allow: &CorsAllowlist) -> bool {
    if let Some(normalized) = normalize_origin(origin) {
        if allow.exact_origins.contains(&normalized) {
            return true;
        }
    }

    // Keep a strict string match fallback (covers unusual schemes like tauri://).
    if allow.exact_origins.contains(origin) {
        return true;
    }

    // Try to parse a host from the origin. Origin header values are serialized origins like:
    // - https://app.example.com
    // - http://127.0.0.1:5173
    // - http://[::1]:5173
    let url = match url::Url::parse(origin) {
        Ok(u) => u,
        Err(_) => return false,
    };

    let host = match url.host_str() {
        Some(h) => h.to_ascii_lowercase(),
        None => return false,
    };

    for pat in &allow.hosts {
        match pat {
            HostPattern::Exact(h) => {
                if &host == h {
                    return true;
                }
            }
            HostPattern::Suffix(suffix) => {
                if host.ends_with(suffix) {
                    // Ensure we only match subdomains, not the apex itself when suffix is ".example.com".
                    // (host == "example.com" should not match ".example.com".)
                    return true;
                }
            }
        }
    }

    false
}

fn is_local_dev_origin(o: &str) -> bool {
    o.starts_with("http://localhost:")
        || o.starts_with("http://127.0.0.1:")
        || o.starts_with("https://localhost:")
        || o.starts_with("https://127.0.0.1:")
        || o.starts_with("http://mac.local:")
        || o.starts_with("https://mac.local:")
        || o.starts_with("http://bodhi.bigduu.com:")
        || o.starts_with("https://bodhi.bigduu.com:")
        || o.starts_with("http://[::1]:")
        || o.starts_with("https://[::1]:")
}

/// Build security headers middleware for production deployments
///
/// Applies standard security headers to all HTTP responses:
/// - Prevents clickjacking (X-Frame-Options)
/// - Prevents MIME type sniffing (X-Content-Type-Options)
/// - Enables XSS protection (X-XSS-Protection)
/// - Controls referrer information (Referrer-Policy)
/// - Restricts resource loading (Content-Security-Policy)
///
/// # Example
///
/// ```rust,ignore
/// use actix_web::App;
/// use bamboo_agent::server::config::build_security_headers;
///
/// let app = App::new()
///     .wrap(build_security_headers());
/// ```
pub fn build_security_headers() -> DefaultHeaders {
    let csp_override = std::env::var("BAMBOO_CSP").ok();
    let csp_value = resolve_csp_header_value(csp_override.as_deref());

    DefaultHeaders::new()
        .add(("X-Frame-Options", "DENY"))
        .add(("X-Content-Type-Options", "nosniff"))
        .add(("X-XSS-Protection", "1; mode=block"))
        .add(("Referrer-Policy", "strict-origin-when-cross-origin"))
        // Note: customize at runtime via `BAMBOO_CSP` if your frontend requires a relaxed policy.
        .add((header::CONTENT_SECURITY_POLICY, csp_value))
}

/// Long-cache content-hashed frontend assets at the proxy/CDN edge.
///
/// Vite emits hashed filenames under `/assets/` (e.g. `main-B6snAd4S.css`), so
/// they are inherently immutable — any content change yields a NEW filename.
/// Tagging them `immutable, max-age=1y` lets Cloudflare and browsers cache them
/// at the edge instead of round-tripping every chunk through the tunnel to
/// origin. Besides being faster, this removes the transient per-asset failures
/// (an occasional reset of one of many parallel preload requests over a
/// cloudflared tunnel) that surface in the browser as Vite's
/// "Unable to preload CSS for …" / "Failed to fetch dynamically imported module".
///
/// Only `/assets/*` is affected; `index.html` and API routes are left untouched
/// so they always serve fresh (a new deploy must be picked up immediately).
pub async fn add_asset_cache_headers<B: MessageBody + 'static>(
    req: ServiceRequest,
    next: Next<B>,
) -> Result<ServiceResponse<B>, actix_web::Error> {
    let is_asset = req.path().starts_with("/assets/");
    let mut res = next.call(req).await?;
    if is_asset {
        res.headers_mut().insert(
            header::CACHE_CONTROL,
            header::HeaderValue::from_static("public, max-age=31536000, immutable"),
        );
    }
    Ok(res)
}

/// Build CORS middleware based on bind address and port
///
/// Automatically configures CORS policy based on deployment environment:
///
/// # Development Mode (localhost)
///
/// When binding to `127.0.0.1`, `localhost`, or `::1`:
/// - Allows all origins, methods, and headers
/// - Suitable for local development
/// - Safe because server is only accessible locally
///
/// # Docker Production Mode (0.0.0.0)
///
/// When binding to `0.0.0.0`:
/// - Only allows `http://localhost:{port}`
/// - Requires reverse proxy for external access
/// - Restrictive CORS for security
///
/// # Custom Address
///
/// For any other bind address:
/// - Only allows that specific address
/// - Most restrictive configuration
///
/// # Arguments
///
/// * `bind_addr` - The address the server binds to
/// * `port` - The port number the server listens on
///
/// # Example
///
/// ```rust,ignore
/// use actix_web::HttpServer;
/// use bambooagent::server::config::build_cors;
///
/// let cors = build_cors("127.0.0.1", 9562);
/// // Use cors middleware in HttpServer
/// ```
pub fn build_cors(bind_addr: &str, port: u16) -> Cors {
    let allowlist = parse_cors_allowlist_env();

    let cors = if bind_addr == "127.0.0.1" || bind_addr == "localhost" || bind_addr == "::1" {
        // Development/Desktop mode. Keep origins permissive for local/Tauri callers, but do not
        // combine wildcard `Access-Control-Allow-Origin: *` with credentialed requests. The Lotus
        // client sends `credentials: "include"` so browsers require a concrete echoed Origin.
        info!("CORS configured for development mode: allowing local/Tauri origins (+ optional allowlist)");
        Cors::default()
            .allowed_origin_fn(move |origin, _req_head| {
                let o = match origin.to_str() {
                    Ok(v) => v,
                    Err(_) => return false,
                };

                if is_allowed_by_allowlist(o, &allowlist) {
                    return true;
                }

                if is_local_dev_origin(o) {
                    return true;
                }

                o == "tauri://localhost"
                    || o == "https://tauri.localhost"
                    || o == "http://tauri.localhost"
            })
            .allow_any_method()
            .allow_any_header()
            .supports_credentials()
            .max_age(3600)
    } else if bind_addr == "0.0.0.0" {
        // Docker/sidecar mode.
        //
        // We still want to restrict origins to "local" callers, but ports and schemes
        // can differ between:
        // - Vite dev server (http://127.0.0.1:5173, http://localhost:5173)
        // - Tauri webview (tauri://localhost, https://tauri.localhost)
        // - Reverse proxy setups (http://localhost:{port})
        //
        // Accept any localhost/loopback origin (any port) and common Tauri origins.
        info!("CORS configured for 0.0.0.0 bind: allowing localhost/loopback origins (+ optional allowlist)");
        Cors::default()
            .allowed_origin_fn(move |origin, _req_head| {
                let o = match origin.to_str() {
                    Ok(v) => v,
                    Err(_) => return false,
                };

                // Explicit allowlist (for remote UI domains, etc).
                if is_allowed_by_allowlist(o, &allowlist) {
                    return true;
                }

                // Common local HTTP(S) dev origins (any port).
                if is_local_dev_origin(o) {
                    return true;
                }

                // Tauri webview origins (vary by version/config).
                if o == "tauri://localhost"
                    || o == "https://tauri.localhost"
                    || o == "http://tauri.localhost"
                {
                    return true;
                }

                // Some setups might load the UI from the same port as the backend.
                if o == format!("http://localhost:{port}")
                    || o == format!("http://127.0.0.1:{port}")
                {
                    return true;
                }

                false
            })
            // This server is commonly used as a local relay for multiple upstream clients
            // (OpenAI/Anthropic/Gemini). Avoid CORS preflight failures by not restricting methods.
            .allow_any_method()
            // OpenAI's official JS client sends additional `x-stainless-*` headers which would
            // otherwise fail preflight; keep headers permissive while origin stays locked down.
            .allow_any_header()
            .supports_credentials()
            .max_age(3600)
    } else {
        // Custom bind address - restrictive by default, but allow explicit env allowlist.
        info!(
            "CORS configured for custom bind address: {} (+ optional allowlist)",
            bind_addr
        );
        let bind_host = bind_addr.to_ascii_lowercase();
        let allowlist = allowlist.clone();
        Cors::default()
            .allowed_origin_fn(move |origin, _req_head| {
                let o = match origin.to_str() {
                    Ok(v) => v,
                    Err(_) => return false,
                };

                if is_allowed_by_allowlist(o, &allowlist) {
                    return true;
                }

                // Allow same-host origins (any scheme/port) for the bind address itself.
                // This keeps the default "tight" without requiring users to enumerate ports.
                let url = match url::Url::parse(o) {
                    Ok(u) => u,
                    Err(_) => return false,
                };
                let Some(host) = url.host_str() else {
                    return false;
                };
                host.eq_ignore_ascii_case(&bind_host)
            })
            .allow_any_method()
            .allow_any_header()
            .supports_credentials()
            .max_age(3600)
    };

    cors
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limiter_config_clamps_degenerate_values() {
        // 0 per_second / 0 burst would make finish() reject; the clamps keep it
        // valid (no panic).
        let _ = rate_limiter_config(0, 0, ClientIpKeyExtractor::peer_ip());
        let _ = rate_limiter_config(1000, 1, ClientIpKeyExtractor::peer_ip());
    }

    #[test]
    fn loopback_binds_skip_rate_limiter() {
        // Desktop sidecar binds must be exempt (frontend bursts asset requests);
        // network-exposed binds must stay throttled.
        for b in ["127.0.0.1", "localhost", "::1"] {
            assert!(
                is_loopback_bind(b),
                "{b} should be loopback (limiter skipped)"
            );
        }
        for b in ["0.0.0.0", "192.168.1.10", "::"] {
            assert!(!is_loopback_bind(b), "{b} should be throttled");
        }
    }

    #[actix_web::test]
    async fn asset_cache_headers_only_tag_hashed_assets() {
        use actix_web::http::header::CACHE_CONTROL;
        use actix_web::{test, web, App, HttpResponse};

        let app = test::init_service(
            App::new()
                .wrap(actix_web::middleware::from_fn(add_asset_cache_headers))
                .route(
                    "/assets/main-abc123.css",
                    web::get().to(|| async { HttpResponse::Ok().finish() }),
                )
                .route(
                    "/index.html",
                    web::get().to(|| async { HttpResponse::Ok().finish() }),
                ),
        )
        .await;

        // A hashed `/assets/*` file gets the immutable long-cache header.
        let req = test::TestRequest::get()
            .uri("/assets/main-abc123.css")
            .to_request();
        let res = test::call_service(&app, req).await;
        assert_eq!(
            res.headers()
                .get(CACHE_CONTROL)
                .and_then(|v| v.to_str().ok()),
            Some("public, max-age=31536000, immutable"),
        );

        // `index.html` (and anything outside `/assets/`) must stay fresh so a new
        // deploy is picked up immediately — no long-cache header added.
        let req = test::TestRequest::get().uri("/index.html").to_request();
        let res = test::call_service(&app, req).await;
        assert!(
            res.headers().get(CACHE_CONTROL).is_none(),
            "non-asset routes must not be long-cached"
        );
    }

    #[actix_web::test]
    async fn rate_limiter_throttles_with_429_after_burst() {
        use actix_governor::Governor;
        use actix_web::http::StatusCode;
        use actix_web::{test, web, App, HttpResponse};
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        // burst=2: the first two requests from an IP pass, the rest are throttled.
        let conf = rate_limiter_config(1, 2, ClientIpKeyExtractor::peer_ip());
        let app = test::init_service(
            App::new()
                .wrap(Governor::new(&conf))
                .route("/", web::get().to(|| async { HttpResponse::Ok().finish() })),
        )
        .await;

        let ip = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7)), 9999);
        let (mut saw_ok, mut saw_429) = (false, false);
        for _ in 0..6 {
            let req = test::TestRequest::get().uri("/").peer_addr(ip).to_request();
            match test::call_service(&app, req).await.status() {
                StatusCode::OK => saw_ok = true,
                StatusCode::TOO_MANY_REQUESTS => saw_429 = true,
                other => panic!("unexpected status {other}"),
            }
        }
        assert!(saw_ok, "requests within the burst must pass");
        assert!(saw_429, "requests beyond the burst must be 429'd (#13)");

        // A DIFFERENT client IP has its OWN bucket — proving per-IP keying (a
        // global bucket would 429 this too); guards against a regression to a
        // global key extractor.
        let other_ip = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 9)), 8888);
        let req = test::TestRequest::get()
            .uri("/")
            .peer_addr(other_ip)
            .to_request();
        assert_eq!(
            test::call_service(&app, req).await.status(),
            StatusCode::OK,
            "a different IP gets its own fresh bucket (per-IP, not global)"
        );
    }

    #[actix_web::test]
    async fn key_extractor_default_ignores_xff_and_uses_peer_ip() {
        use actix_web::test;
        use std::net::{Ipv4Addr, SocketAddr};

        // Default (trust_xff = false): a client-supplied XFF must be ignored so it
        // can't spoof its rate-limit key on a directly-exposed server.
        let ke = ClientIpKeyExtractor::peer_ip();
        let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7)), 5000);
        let req = test::TestRequest::get()
            .peer_addr(peer)
            .insert_header(("x-forwarded-for", "1.2.3.4"))
            .to_srv_request();
        assert_eq!(
            ke.extract(&req).unwrap(),
            IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7))
        );
    }

    #[actix_web::test]
    async fn key_extractor_xff_uses_rightmost_at_one_hop_not_client_prefix() {
        use actix_web::test;
        use std::net::{Ipv4Addr, SocketAddr};

        // trusted_hops = 1: only the entry OUR proxy appended (rightmost) is
        // trusted; a client prepending a fake IP can't change the key.
        let ke = ClientIpKeyExtractor {
            trust_xff: true,
            trusted_hops: 1,
        };
        let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 5000); // proxy
        let req = test::TestRequest::get()
            .peer_addr(peer)
            .insert_header(("x-forwarded-for", "1.1.1.1, 2.2.2.2"))
            .to_srv_request();
        assert_eq!(
            ke.extract(&req).unwrap(),
            IpAddr::V4(Ipv4Addr::new(2, 2, 2, 2))
        );
    }

    #[actix_web::test]
    async fn key_extractor_xff_two_hops_takes_second_from_right() {
        use actix_web::test;
        use std::net::{Ipv4Addr, SocketAddr};

        let ke = ClientIpKeyExtractor {
            trust_xff: true,
            trusted_hops: 2,
        };
        let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 5000);
        let req = test::TestRequest::get()
            .peer_addr(peer)
            .insert_header(("x-forwarded-for", "1.1.1.1, 2.2.2.2, 3.3.3.3"))
            .to_srv_request();
        assert_eq!(
            ke.extract(&req).unwrap(),
            IpAddr::V4(Ipv4Addr::new(2, 2, 2, 2))
        );
    }

    #[actix_web::test]
    async fn key_extractor_xff_fails_closed_to_peer_when_header_too_short_or_absent() {
        use actix_web::test;
        use std::net::{Ipv4Addr, SocketAddr};

        let ke = ClientIpKeyExtractor {
            trust_xff: true,
            trusted_hops: 2,
        };
        let peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 5000);

        // Fewer entries than trusted hops → not a trusted-proxy shape → peer IP.
        let short = test::TestRequest::get()
            .peer_addr(peer)
            .insert_header(("x-forwarded-for", "9.9.9.9"))
            .to_srv_request();
        assert_eq!(
            ke.extract(&short).unwrap(),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))
        );

        // No XFF at all → peer IP.
        let none = test::TestRequest::get().peer_addr(peer).to_srv_request();
        assert_eq!(
            ke.extract(&none).unwrap(),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))
        );
    }

    #[test]
    fn parse_forwarded_ip_handles_bare_port_and_bracketed_forms() {
        use std::net::{Ipv4Addr, Ipv6Addr};

        assert_eq!(
            parse_forwarded_ip("1.2.3.4"),
            Some(IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)))
        );
        assert_eq!(
            parse_forwarded_ip("1.2.3.4:5678"),
            Some(IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)))
        );
        assert_eq!(
            parse_forwarded_ip("[::1]:9000"),
            Some(IpAddr::V6(Ipv6Addr::LOCALHOST))
        );
        assert_eq!(
            parse_forwarded_ip("[::1]"),
            Some(IpAddr::V6(Ipv6Addr::LOCALHOST))
        );
        assert_eq!(parse_forwarded_ip("not-an-ip"), None);
    }

    #[test]
    fn mask_ipv6_prefix_zeroes_lower_bytes_and_leaves_ipv4() {
        use std::net::{Ipv4Addr, Ipv6Addr};

        let v4 = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));
        assert_eq!(mask_ipv6_prefix(v4), v4);

        let v6 = IpAddr::V6(Ipv6Addr::new(
            0x2001, 0xdb8, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6,
        ));
        // /56: first 7 bytes preserved, remaining 9 zeroed.
        assert_eq!(
            mask_ipv6_prefix(v6),
            IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0))
        );
    }

    #[test]
    fn default_csp_keeps_scripts_strict_but_allows_inline_styles() {
        assert!(DEFAULT_CSP.contains("script-src 'self'"));
        assert!(DEFAULT_CSP.contains("style-src 'self' 'unsafe-inline'"));
        assert!(!DEFAULT_CSP.contains("unsafe-eval"));
    }

    #[test]
    fn connect_src_append_normalizes_explicit_origins() {
        let sources = parse_csp_connect_src_append(
            "https://bodhi.bigduu.com:9562, http://bodhi.bigduu.com:9562/",
        );
        assert_eq!(
            sources,
            vec![
                "https://bodhi.bigduu.com:9562".to_string(),
                "http://bodhi.bigduu.com:9562".to_string(),
            ]
        );
    }

    #[test]
    fn append_connect_src_sources_extends_default_csp() {
        let csp = append_connect_src_sources(
            DEFAULT_CSP,
            &[
                "https://bodhi.bigduu.com:9562".to_string(),
                "http://bodhi.bigduu.com:9562".to_string(),
            ],
        );

        assert!(csp.contains("connect-src 'self' ws: wss:"));
        assert!(csp.contains("https://bodhi.bigduu.com:9562"));
        assert!(csp.contains("http://bodhi.bigduu.com:9562"));
    }

    #[test]
    fn invalid_override_falls_back_to_default() {
        // Header values cannot contain newlines.
        let v = resolve_csp_header_value(Some("default-src 'self'\nscript-src 'self'"));
        let rendered = v.to_str().expect("header should be valid utf-8");
        assert!(rendered.contains("connect-src 'self' ws: wss:"));
        assert!(rendered.contains("http://127.0.0.1:*"));
        assert!(rendered.contains("http://localhost:*"));
        assert!(rendered.contains("http://bodhi.bigduu.com:9562"));
        assert!(rendered.contains("https://bodhi.bigduu.com:9562"));
        assert!(rendered.contains("style-src 'self' 'unsafe-inline'"));
    }

    #[test]
    fn cors_allowlist_parses_hosts_and_origins() {
        let allow = parse_cors_allowlist(
            "https://app.example.com/, app.example2.com, *.example.net , http://localhost:5173",
        );
        assert!(allow.exact_origins.contains("https://app.example.com"));
        assert!(allow.exact_origins.contains("http://localhost:5173"));
        assert!(allow
            .hosts
            .contains(&HostPattern::Exact("app.example2.com".to_string())));
        assert!(allow
            .hosts
            .contains(&HostPattern::Suffix(".example.net".to_string())));
    }

    #[test]
    fn cors_allowlist_matches_exact_and_wildcard_hosts() {
        let mut allow = CorsAllowlist::default();
        allow
            .exact_origins
            .insert("https://app.example.com".to_string());
        allow
            .hosts
            .push(HostPattern::Exact("app2.example.com".to_string()));
        allow
            .hosts
            .push(HostPattern::Suffix(".example.net".to_string()));

        assert!(is_allowed_by_allowlist("https://app.example.com", &allow));
        assert!(is_allowed_by_allowlist(
            "https://app.example.com:443",
            &allow
        ));
        assert!(is_allowed_by_allowlist(
            "http://app2.example.com:5173",
            &allow
        ));
        assert!(is_allowed_by_allowlist("https://x.example.net", &allow));
        assert!(!is_allowed_by_allowlist("https://example.net", &allow));
        assert!(!is_allowed_by_allowlist("https://evil.com", &allow));
    }

    #[test]
    fn local_dev_origin_allows_mac_local_and_bodhi_domain() {
        assert!(is_local_dev_origin("http://mac.local:1420"));
        assert!(is_local_dev_origin("https://mac.local:1420"));
        assert!(is_local_dev_origin("http://bodhi.bigduu.com:9562"));
        assert!(is_local_dev_origin("https://bodhi.bigduu.com:9562"));
        assert!(!is_local_dev_origin("http://evil.com:1420"));
    }
}
