use std::path::PathBuf;

use actix_files as fs;
use actix_web::{web, App, HttpServer};
use tokio::sync::oneshot;
use tracing::{error, info};

use super::listeners::DEFAULT_WORKER_COUNT;
use super::tls::build_rustls_config;
use crate::app_state::AppState;
use crate::config::{build_cors, build_rate_limiter, build_security_headers};
use crate::routes::{configure_routes, configure_routes_with_rate_limiting};
use actix_governor::Governor;
use bamboo_config::TlsConfig;

/// Manageable web service with start/stop lifecycle
///
/// Use this when you need to programmatically control the server lifecycle,
/// such as in tests or embedded scenarios.
pub struct WebService {
    shutdown_tx: Option<oneshot::Sender<()>>,
    server_handle: Option<tokio::task::JoinHandle<()>>,
    /// Handle to the running server's [`AppState`], retained so [`WebService::stop`]
    /// /[`Drop`] can gracefully stop AppState-owned background tasks (the #47
    /// MCP-proxy reconnect supervisor) instead of leaking them until process exit.
    /// #119.
    app_state: Option<web::Data<AppState>>,
    /// Bamboo home directory containing all application data (config, sessions, skills, etc.)
    bamboo_home_dir: PathBuf,
    port: u16,
}

impl WebService {
    /// Create a new WebService instance
    ///
    /// # Arguments
    /// * `bamboo_home_dir` - Bamboo home directory (e.g., `${HOME}/.bamboo` or custom path)
    pub fn new(bamboo_home_dir: PathBuf) -> Self {
        Self {
            shutdown_tx: None,
            server_handle: None,
            app_state: None,
            bamboo_home_dir,
            port: 3456, // Default port
        }
    }

    /// Start the web service on the specified port using the default localhost bind.
    pub async fn start(&mut self, port: u16) -> Result<(), String> {
        self.start_with_bind(port, "127.0.0.1").await
    }

    /// Start the web service on the specified port and bind address.
    pub async fn start_with_bind(&mut self, port: u16, bind: &str) -> Result<(), String> {
        self.start_with_bind_tls(port, bind, None).await
    }

    /// Start the web service, terminating TLS itself when `tls` is `Some` (#181).
    ///
    /// `None` keeps the plaintext `.bind()` path unchanged (desktop loopback).
    pub async fn start_with_bind_tls(
        &mut self,
        port: u16,
        bind: &str,
        tls: Option<&TlsConfig>,
    ) -> Result<(), String> {
        info!("Starting web service...");
        if self.server_handle.is_some() {
            return Err("Web service is already running".to_string());
        }

        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
        self.port = port;

        let app_state = web::Data::new(
            AppState::new(self.bamboo_home_dir.clone())
                .await
                .map_err(|e| format!("Failed to initialize app state: {e}"))?,
        );
        // Retain a handle so stop()/Drop can stop AppState-owned background tasks. #119
        self.app_state = Some(app_state.clone());
        let bind_addr = bind.to_string();
        let listen_addr = format!("{bind}:{port}");
        let bind_for_log = bind_addr.clone();

        let server = HttpServer::new(move || {
            App::new()
                .app_data(app_state.clone())
                .wrap(build_cors(&bind_addr, port))
                .configure(configure_routes) // No rate limiting for WebService
        })
        .workers(DEFAULT_WORKER_COUNT);

        // Fail-fast: build the rustls config before binding; `None` → unchanged
        // plaintext `.bind()` path. #181.
        let server = match tls {
            Some(tls) => server
                .bind_rustls_0_23(&listen_addr, build_rustls_config(tls)?)
                .map_err(|e| format!("Failed to bind TLS server: {e}"))?,
            None => server
                .bind(&listen_addr)
                .map_err(|e| format!("Failed to bind server: {e}"))?,
        }
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

        let scheme = if tls.is_some() { "https" } else { "http" };
        info!(
            "Web service started successfully on {scheme}://{}:{}",
            bind_for_log, port
        );
        Ok(())
    }

    /// Start the web service on the specified port and bind address, serving static files
    /// alongside the API routes.
    pub async fn start_with_bind_and_static(
        &mut self,
        port: u16,
        bind: &str,
        static_dir: PathBuf,
    ) -> Result<(), String> {
        self.start_with_bind_and_static_tls(port, bind, static_dir, None)
            .await
    }

    /// Like [`WebService::start_with_bind_and_static`], terminating TLS itself
    /// when `tls` is `Some` (#181). `None` keeps the plaintext path unchanged.
    pub async fn start_with_bind_and_static_tls(
        &mut self,
        port: u16,
        bind: &str,
        static_dir: PathBuf,
        tls: Option<&TlsConfig>,
    ) -> Result<(), String> {
        info!("Starting web service with static frontend...");
        if self.server_handle.is_some() {
            return Err("Web service is already running".to_string());
        }

        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
        self.port = port;

        let static_dir = static_dir
            .canonicalize()
            .map_err(|e| format!("Static directory not found: {:?}: {}", static_dir, e))?;
        if !static_dir.is_dir() {
            return Err(format!(
                "Static path is not a directory: {}",
                static_dir.display()
            ));
        }

        let app_state = web::Data::new(
            AppState::new(self.bamboo_home_dir.clone())
                .await
                .map_err(|e| format!("Failed to initialize app state: {e}"))?,
        );
        // Retain a handle so stop()/Drop can stop AppState-owned background tasks. #119
        self.app_state = Some(app_state.clone());
        // Per-IP rate limiter for this network-exposed production server. #13
        let rate_limiter = build_rate_limiter();
        let bind_addr = bind.to_string();
        let listen_addr = format!("{bind}:{port}");
        let bind_for_log = bind_addr.clone();

        let server = HttpServer::new(move || {
            App::new()
                .app_data(web::JsonConfig::default().limit(25 * 1024 * 1024))
                .app_data(web::PayloadConfig::new(30 * 1024 * 1024))
                .app_data(app_state.clone())
                .wrap(Governor::new(&rate_limiter))
                .wrap(build_cors(&bind_addr, port))
                .wrap(build_security_headers())
                // Immutable long-cache for hashed `/assets/*` so a proxy/CDN
                // (e.g. Cloudflare tunnel) caches chunks at the edge instead of
                // round-tripping each one to origin (#preload-error fix).
                .wrap(actix_web::middleware::from_fn(
                    crate::config::add_asset_cache_headers,
                ))
                .configure(configure_routes_with_rate_limiting)
                .service(
                    fs::Files::new("/", static_dir.clone())
                        .index_file("index.html")
                        .prefer_utf8(true)
                        .disable_content_disposition()
                        .disable_content_disposition(),
                )
        })
        .workers(DEFAULT_WORKER_COUNT);

        // Fail-fast: build the rustls config before binding; `None` → unchanged
        // plaintext `.bind()` path. #181.
        let server = match tls {
            Some(tls) => server
                .bind_rustls_0_23(&listen_addr, build_rustls_config(tls)?)
                .map_err(|e| format!("Failed to bind TLS server: {e}"))?,
            None => server
                .bind(&listen_addr)
                .map_err(|e| format!("Failed to bind server: {e}"))?,
        }
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

        let scheme = if tls.is_some() { "https" } else { "http" };
        info!(
            "Web service with static frontend started successfully on {scheme}://{}:{}",
            bind_for_log, port
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

            // Gracefully stop AppState-owned background tasks — the #47 MCP-proxy
            // reconnect supervisor (via its cancellation token) and the MCP servers.
            // Without this the token was wired but never cancelled, so the
            // supervisor only died at process exit. #119.
            if let Some(state) = self.app_state.take() {
                state.shutdown().await;
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
        // Drop can't run the async shutdown(), but cancelling the MCP-proxy
        // supervisor's token is synchronous — so a WebService dropped without an
        // explicit stop() still tears down the reconnect loop. (The async MCP
        // server cleanup is left to process exit on this fallback path.) #119.
        if let Some(state) = self.app_state.take() {
            state.mcp_proxy_shutdown.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #119 e2e: WebService::stop() must cancel the AppState-owned MCP-proxy
    /// reconnect supervisor's token, so it terminates on server stop rather than
    /// leaking until process exit.
    #[tokio::test]
    async fn stop_cancels_mcp_proxy_supervisor_token() {
        let home = tempfile::TempDir::new().expect("tempdir");
        let mut service = WebService::new(home.path().to_path_buf());
        // Port 0 -> OS-assigned ephemeral port (no conflict).
        service
            .start_with_bind(0, "127.0.0.1")
            .await
            .expect("web service starts");

        // Capture the supervisor's cancellation token while the service runs.
        let token = service
            .app_state
            .as_ref()
            .expect("app_state retained after start")
            .mcp_proxy_shutdown
            .clone();
        assert!(
            !token.is_cancelled(),
            "supervisor token is live while the service runs"
        );

        service.stop().await.expect("web service stops");

        assert!(
            token.is_cancelled(),
            "stop() must cancel the MCP-proxy supervisor token so it terminates"
        );
    }
}
