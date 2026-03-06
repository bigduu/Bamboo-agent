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
use actix_web::http::header;
use actix_web::middleware::DefaultHeaders;
use log::info;
use log::warn;
use std::collections::HashSet;

// Keep the default CSP strict (no `unsafe-*`) to avoid weakening XSS protections.
// If your UI requires inline scripts/styles or eval-like behavior, override with
// `BAMBOO_CSP` at runtime.
const DEFAULT_CSP: &str = concat!(
    "default-src 'self'; ",
    "base-uri 'self'; ",
    "object-src 'none'; ",
    "frame-ancestors 'none'; ",
    "script-src 'self'; ",
    "style-src 'self'; ",
    "img-src 'self' data: https:; ",
    "font-src 'self' data:; ",
    "connect-src 'self' ws: wss:; ",
    "form-action 'self';"
);

fn resolve_csp_header_value(override_value: Option<&str>) -> header::HeaderValue {
    let csp = override_value.unwrap_or(DEFAULT_CSP);
    match header::HeaderValue::from_str(csp) {
        Ok(v) => v,
        Err(e) => {
            // Avoid failing to start due to a malformed override; fall back to the safe default.
            warn!(
                "Invalid BAMBOO_CSP value ({}); falling back to DEFAULT_CSP",
                e
            );
            header::HeaderValue::from_static(DEFAULT_CSP)
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
        // Development/Desktop mode - allow all origins and headers for maximum flexibility
        // This is safe because the server only binds to localhost
        info!("CORS configured for development mode: allowing all origins and headers (localhost only)");
        Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header()
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
                if o == format!("http://localhost:{port}") || o == format!("http://127.0.0.1:{port}") {
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
            .max_age(3600)
    };

    cors
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_csp_has_no_unsafe_keywords() {
        assert!(!DEFAULT_CSP.contains("unsafe-inline"));
        assert!(!DEFAULT_CSP.contains("unsafe-eval"));
    }

    #[test]
    fn invalid_override_falls_back_to_default() {
        // Header values cannot contain newlines.
        let v = resolve_csp_header_value(Some("default-src 'self'\nscript-src 'self'"));
        assert_eq!(v, header::HeaderValue::from_static(DEFAULT_CSP));
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
        allow.hosts.push(HostPattern::Exact("app2.example.com".to_string()));
        allow
            .hosts
            .push(HostPattern::Suffix(".example.net".to_string()));

        assert!(is_allowed_by_allowlist("https://app.example.com", &allow));
        assert!(is_allowed_by_allowlist("https://app.example.com:443", &allow));
        assert!(is_allowed_by_allowlist("http://app2.example.com:5173", &allow));
        assert!(is_allowed_by_allowlist("https://x.example.net", &allow));
        assert!(!is_allowed_by_allowlist("https://example.net", &allow));
        assert!(!is_allowed_by_allowlist("https://evil.com", &allow));
    }
}
