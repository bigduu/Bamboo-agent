use std::path::PathBuf;

use actix_files as fs;
use actix_web::{web, App, HttpServer};
use log::{error, info};

use super::listeners::{build_bind_listeners, build_desktop_listeners, resolve_worker_count};
use crate::server::app_state::AppState;
use crate::server::config::{build_cors, build_security_headers};
use crate::server::routes::{configure_routes, configure_routes_with_rate_limiting};

/// Run the unified server in desktop mode (localhost only, no rate limiting)
///
/// This is the simplest mode for desktop applications:
/// - Binds to 127.0.0.1 only (safe, localhost-only)
/// - No rate limiting (assumes single user)
/// - No security headers (development mode)
///
/// # Arguments
/// * `bamboo_home_dir` - Bamboo home directory containing all app data (config, sessions, skills, etc.)
///                       Equivalent to `${HOME}/.bamboo` in standard installations.
/// * `port` - Port to listen on
pub async fn run(bamboo_home_dir: PathBuf, port: u16) -> Result<(), String> {
    info!("Starting unified server in desktop mode...");

    let app_state = web::Data::new(AppState::new(bamboo_home_dir.clone()).await);
    let workers = resolve_worker_count();

    let app_factory = move || {
        App::new()
            .app_data(app_state.clone())
            .wrap(build_cors("127.0.0.1", port))
            .configure(configure_routes) // No rate limiting for desktop mode
    };

    let listeners = build_desktop_listeners(port)?;

    let mut http = HttpServer::new(app_factory).workers(workers);
    for (idx, listener) in listeners.into_iter().enumerate() {
        http = http
            .listen(listener)
            .map_err(|e| format!("Failed to attach listener #{idx}: {e}"))?;
    }

    let server = http.run();

    info!("Unified server running on http://127.0.0.1:{port}");

    if let Err(e) = server.await {
        error!("Server error: {}", e);
        return Err(format!("Server error: {e}"));
    }

    Ok(())
}

/// Run the unified server with custom bind address (Docker/production mode)
///
/// Production mode features:
/// - Custom bind address (0.0.0.0 for Docker, custom for standalone)
/// - Rate limiting enabled (10 req/sec, burst 20)
/// - Security headers enabled
/// - Request size limits (25MB JSON, 30MB payload)
///
/// # Arguments
/// * `bamboo_home_dir` - Bamboo home directory containing all app data (config, sessions, skills, etc.)
///                       Equivalent to `${HOME}/.bamboo` in standard installations.
/// * `port` - Port to listen on
/// * `bind` - Bind address (127.0.0.1, 0.0.0.0, or custom)
pub async fn run_with_bind(bamboo_home_dir: PathBuf, port: u16, bind: &str) -> Result<(), String> {
    info!("Starting unified server on {}:{}", bind, port);

    let app_state = web::Data::new(AppState::new(bamboo_home_dir.clone()).await);
    let workers = resolve_worker_count();

    let bind_for_cors = bind.to_string();
    let app_factory = move || {
        App::new()
            // Request size limits to prevent DoS
            // Chat requests may include base64 images; keep limits high enough for local usage.
            .app_data(web::JsonConfig::default().limit(25 * 1024 * 1024)) // 25MB JSON limit
            .app_data(web::PayloadConfig::new(30 * 1024 * 1024)) // 30MB payload limit
            .app_data(app_state.clone())
            .wrap(build_cors(&bind_for_cors, port))
            .wrap(build_security_headers())
            .configure(configure_routes_with_rate_limiting) // Enable rate limiting
    };

    let listeners = build_bind_listeners(bind, port)?;

    let mut http = HttpServer::new(app_factory).workers(workers);
    for (idx, listener) in listeners.into_iter().enumerate() {
        http = http
            .listen(listener)
            .map_err(|e| format!("Failed to attach listener #{idx}: {e}"))?;
    }

    let server = http.run();

    info!("Unified server running on http://{}:{}", bind, port);

    if let Err(e) = server.await {
        error!("Server error: {}", e);
        return Err(format!("Server error: {e}"));
    }

    Ok(())
}

/// Run the unified server with custom bind address and static file serving
///
/// Production mode with frontend serving:
/// - All features from run_with_bind()
/// - Static file serving for frontend (index.html, assets, etc.)
///
/// # Arguments
/// * `bamboo_home_dir` - Bamboo home directory containing all app data (config, sessions, skills, etc.)
///                       Equivalent to `${HOME}/.bamboo` in standard installations.
/// * `port` - Port to listen on
/// * `bind` - Bind address (127.0.0.1 for localhost, 0.0.0.0 for all interfaces)
/// * `static_dir` - Optional directory containing built frontend files
///
/// # Example
/// ```bash
/// # Docker mode (serve frontend)
/// bamboo serve --port 9562 --bind 0.0.0.0 --static-dir /app/static
///
/// # Standalone production mode (serve frontend)
/// bamboo serve --port 9562 --static-dir ./dist
/// ```
pub async fn run_with_bind_and_static(
    bamboo_home_dir: PathBuf,
    port: u16,
    bind: &str,
    static_dir: Option<PathBuf>,
) -> Result<(), String> {
    info!("Starting unified server on {}:{}...", bind, port);

    // Canonicalize static_dir path to absolute path before passing to workers
    // This is required for fs::Files to work correctly in multi-threaded environment
    let static_dir: Option<PathBuf> = match static_dir {
        Some(path) => {
            let canonicalized = path
                .canonicalize()
                .map_err(|e| format!("Static directory not found: {:?}: {}", path, e))?;
            if !canonicalized.is_dir() {
                return Err(format!(
                    "Static path is not a directory: {}",
                    canonicalized.display()
                ));
            }
            info!("Serving static files from: {:?}", canonicalized);
            Some(canonicalized)
        }
        None => None,
    };

    let app_state = web::Data::new(AppState::new(bamboo_home_dir.clone()).await);
    let workers = resolve_worker_count();

    let bind_for_cors = bind.to_string();
    let app_factory = move || {
        let mut app = App::new()
            // Request size limits to prevent DoS
            // Chat requests may include base64 images; keep limits high enough for local usage.
            .app_data(web::JsonConfig::default().limit(25 * 1024 * 1024)) // 25MB JSON limit
            .app_data(web::PayloadConfig::new(30 * 1024 * 1024)) // 30MB payload limit
            .app_data(app_state.clone())
            .wrap(build_cors(&bind_for_cors, port))
            .wrap(build_security_headers())
            .configure(configure_routes_with_rate_limiting); // Enable rate limiting

        // Add static file serving if directory is provided
        if let Some(static_path) = &static_dir {
            info!("Serving static files from: {:?}", static_path);

            // Serve static files with security restrictions
            // Note: fs::Files automatically handles path traversal via canonicalization
            // Use a specific path for static assets to avoid conflicting with API routes
            app = app.service(
                fs::Files::new("/", static_path)
                    .index_file("index.html")
                    .prefer_utf8(true)
                    // Disable listing directories
                    .disable_content_disposition()
                    // Don't show index file for directories (security)
                    .disable_content_disposition(),
            );
        }

        app
    };

    let listeners = build_bind_listeners(bind, port)?;

    let mut http = HttpServer::new(app_factory).workers(workers);
    for (idx, listener) in listeners.into_iter().enumerate() {
        http = http
            .listen(listener)
            .map_err(|e| format!("Failed to attach listener #{idx}: {e}"))?;
    }

    let server = http.run();

    info!("Unified server running on http://{}:{}", bind, port);

    if let Err(e) = server.await {
        error!("Server error: {}", e);
        return Err(format!("Server error: {e}"));
    }

    Ok(())
}
