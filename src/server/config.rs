use actix_cors::Cors;
use actix_web::http::header;
use actix_web::middleware::DefaultHeaders;
use log::info;

/// Build security headers for production deployments
pub fn build_security_headers() -> DefaultHeaders {
    DefaultHeaders::new()
        .add(("X-Frame-Options", "DENY"))
        .add(("X-Content-Type-Options", "nosniff"))
        .add(("X-XSS-Protection", "1; mode=block"))
        .add(("Referrer-Policy", "strict-origin-when-cross-origin"))
        // Note: CSP should be customized based on your specific needs
        .add(("Content-Security-Policy", "default-src 'self'; script-src 'self' 'unsafe-inline' 'unsafe-eval'; style-src 'self' 'unsafe-inline'; img-src 'self' data: https:; font-src 'self' data:; connect-src 'self' ws: wss;"))
}

/// Build CORS configuration based on bind address and port
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
        // Docker production mode (localhost only via reverse proxy)
        info!("CORS configured for Docker production mode (localhost only)");
        Cors::default()
            .allowed_origin(&format!("http://localhost:{}", port))
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
