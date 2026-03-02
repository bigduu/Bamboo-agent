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
        info!("CORS configured for 0.0.0.0 bind: allowing localhost/loopback origins");
        Cors::default()
            .allowed_origin_fn(move |origin, _req_head| {
                let o = match origin.to_str() {
                    Ok(v) => v,
                    Err(_) => return false,
                };

                // Common local HTTP(S) dev origins (any port).
                if o.starts_with("http://localhost:")
                    || o.starts_with("http://127.0.0.1:")
                    || o.starts_with("https://localhost:")
                    || o.starts_with("https://127.0.0.1:")
                    || o.starts_with("http://[::1]:")
                    || o.starts_with("https://[::1]:")
                {
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
            .allowed_methods(vec!["GET", "POST", "PUT", "DELETE", "OPTIONS"])
            .allowed_headers(vec![
                header::AUTHORIZATION,
                header::ACCEPT,
                header::CONTENT_TYPE,
            ])
            .max_age(3600)
    } else {
        // Custom bind address - be restrictive
        info!("CORS configured for custom bind address: {}", bind_addr);
        Cors::default()
            .allowed_origin(&format!("http://{}", bind_addr))
            .allowed_methods(vec!["GET", "POST", "PUT", "DELETE", "OPTIONS"])
            .allowed_headers(vec![
                header::AUTHORIZATION,
                header::ACCEPT,
                header::CONTENT_TYPE,
            ])
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
}
