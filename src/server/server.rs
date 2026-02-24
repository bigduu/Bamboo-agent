//! Unified HTTP server entry points
//!
//! Consolidates run(), run_with_bind(), WebService from web_service/server.rs
//! Eliminates the proxy pattern by using unified AppState

use std::path::PathBuf;

use actix_files as fs;
use actix_web::{web, App, HttpServer};
use log::{error, info};
use tokio::sync::oneshot;

use crate::server::app_state::AppState;
use crate::server::config::{build_cors, build_security_headers};
use crate::server::routes::{configure_routes, configure_routes_with_rate_limiting};

const DEFAULT_WORKER_COUNT: usize = 10;

/// Run the unified server in desktop mode (localhost only, no rate limiting)
///
/// This is the simplest mode for desktop applications:
/// - Binds to 127.0.0.1 only (safe, localhost-only)
/// - No rate limiting (assumes single user)
/// - No security headers (development mode)
///
/// # Arguments
/// * `app_data_dir` - Application data directory
/// * `port` - Port to listen on
pub async fn run(app_data_dir: PathBuf, port: u16) -> Result<(), String> {
    info!("Starting unified server in desktop mode...");

    let app_state = web::Data::new(AppState::new(app_data_dir.clone()).await);

    let server = HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .wrap(build_cors("127.0.0.1", port))
            .configure(configure_routes) // No rate limiting for desktop mode
    })
    .workers(DEFAULT_WORKER_COUNT)
    .bind(format!("127.0.0.1:{port}"))
    .map_err(|e| format!("Failed to bind server: {e}"))?
    .run();

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
/// - Request size limits (1MB JSON, 10MB payload)
///
/// # Arguments
/// * `app_data_dir` - Application data directory
/// * `port` - Port to listen on
/// * `bind` - Bind address (127.0.0.1, 0.0.0.0, or custom)
pub async fn run_with_bind(app_data_dir: PathBuf, port: u16, bind: &str) -> Result<(), String> {
    info!("Starting unified server on {}:{}", bind, port);

    let app_state = web::Data::new(AppState::new(app_data_dir.clone()).await);

    // Move bind_addr into the closure
    let bind_for_closure = bind.to_string();
    let bind_for_cors = bind.to_string();

    let server = HttpServer::new(move || {
        App::new()
            // Request size limits to prevent DoS
            .app_data(web::JsonConfig::default().limit(1024 * 1024)) // 1MB JSON limit
            .app_data(web::PayloadConfig::new(10 * 1024 * 1024)) // 10MB payload limit
            .app_data(app_state.clone())
            .wrap(build_cors(&bind_for_cors, port))
            .wrap(build_security_headers())
            .configure(configure_routes_with_rate_limiting) // Enable rate limiting
    })
    .workers(DEFAULT_WORKER_COUNT)
    .bind(format!("{}:{}", bind_for_closure, port))
    .map_err(|e| format!("Failed to bind server: {e}"))?
    .run();

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
/// * `app_data_dir` - Application data directory
/// * `port` - Port to listen on
/// * `bind` - Bind address (127.0.0.1 for localhost, 0.0.0.0 for all interfaces)
/// * `static_dir` - Optional directory containing built frontend files
///
/// # Example
/// ```bash
/// # Docker mode (serve frontend)
/// bamboo serve --port 8080 --bind 0.0.0.0 --static-dir /app/static
///
/// # Standalone production mode (serve frontend)
/// bamboo serve --port 8080 --static-dir ./dist
/// ```
pub async fn run_with_bind_and_static(
    app_data_dir: PathBuf,
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

    let app_state = web::Data::new(AppState::new(app_data_dir.clone()).await);

    // Move bind_addr into the closure
    let bind_addr = bind.to_string();
    let bind_for_closure = bind_addr.clone();
    let bind_for_cors = bind_addr.clone();

    let server = HttpServer::new(move || {
        let mut app = App::new()
            // Request size limits to prevent DoS
            .app_data(web::JsonConfig::default().limit(1024 * 1024)) // 1MB JSON limit
            .app_data(web::PayloadConfig::new(10 * 1024 * 1024)) // 10MB payload limit
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
    })
    .workers(DEFAULT_WORKER_COUNT)
    .bind(format!("{}:{}", bind_for_closure, port))
    .map_err(|e| format!("Failed to bind server: {e}"))?
    .run();

    info!("Unified server running on http://{}:{}", bind, port);

    if let Err(e) = server.await {
        error!("Server error: {}", e);
        return Err(format!("Server error: {e}"));
    }

    Ok(())
}

/// Manageable web service with start/stop lifecycle
///
/// Use this when you need to programmatically control the server lifecycle,
/// such as in tests or embedded scenarios.
pub struct WebService {
    shutdown_tx: Option<oneshot::Sender<()>>,
    server_handle: Option<tokio::task::JoinHandle<()>>,
    app_data_dir: PathBuf,
    port: u16,
}

impl WebService {
    /// Create a new WebService instance
    pub fn new(app_data_dir: PathBuf) -> Self {
        Self {
            shutdown_tx: None,
            server_handle: None,
            app_data_dir,
            port: 3456, // Default port
        }
    }

    /// Start the web service on the specified port
    pub async fn start(&mut self, port: u16) -> Result<(), String> {
        info!("Starting web service...");
        if self.server_handle.is_some() {
            return Err("Web service is already running".to_string());
        }

        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
        self.port = port;

        let app_state = web::Data::new(AppState::new(self.app_data_dir.clone()).await);
        let bind_addr = "127.0.0.1".to_string();

        let server = HttpServer::new(move || {
            App::new()
                .app_data(app_state.clone())
                .wrap(build_cors(&bind_addr, port))
                .configure(configure_routes) // No rate limiting for WebService
        })
        .workers(DEFAULT_WORKER_COUNT)
        .bind(format!("127.0.0.1:{port}"))
        .map_err(|e| format!("Failed to bind server: {e}"))?
        .run();

        let server_handle = tokio::spawn(async move {
            tokio::select! {
                result = server => {
                    if let Err(e) = result {
                        error!("Server error: {}", e);
                    }
                }
                _ = &mut shutdown_rx => {
                    info!("Web service shutdown signal received");
                }
            }
        });

        self.shutdown_tx = Some(shutdown_tx);
        self.server_handle = Some(server_handle);

        info!(
            "Web service started successfully on http://127.0.0.1:{}",
            port
        );
        Ok(())
    }

    /// Stop the web service
    pub async fn stop(&mut self) -> Result<(), String> {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            if shutdown_tx.send(()).is_err() {
                error!("Failed to send shutdown signal");
                return Err("Error sending shutdown signal".to_string());
            }

            if let Some(handle) = self.server_handle.take() {
                if let Err(e) = handle.await {
                    error!("Error waiting for server shutdown: {}", e);
                    return Err(format!("Error waiting for server shutdown: {}", e));
                }
            }

            info!("Web service stopped successfully");
        }

        Ok(())
    }

    /// Check if the web service is currently running
    pub fn is_running(&self) -> bool {
        self.server_handle.is_some()
    }

    /// Get the port the web service is running on
    pub fn port(&self) -> u16 {
        self.port
    }
}

impl Drop for WebService {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_web_service_lifecycle() {
        let temp_dir = tempfile::tempdir().unwrap();
        let ws = WebService::new(temp_dir.path().to_path_buf());
        assert!(!ws.is_running());
    }
}
