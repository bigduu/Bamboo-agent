use std::path::PathBuf;

use actix_web::{web, App, HttpServer};
use tracing::{error, info};
use tokio::sync::oneshot;

use super::listeners::DEFAULT_WORKER_COUNT;
use crate::server::app_state::AppState;
use crate::server::config::build_cors;
use crate::server::routes::configure_routes;

/// Manageable web service with start/stop lifecycle
///
/// Use this when you need to programmatically control the server lifecycle,
/// such as in tests or embedded scenarios.
pub struct WebService {
    shutdown_tx: Option<oneshot::Sender<()>>,
    server_handle: Option<tokio::task::JoinHandle<()>>,
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
            bamboo_home_dir,
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

        let app_state = web::Data::new(
            AppState::new(self.bamboo_home_dir.clone())
                .await
                .map_err(|e| format!("Failed to initialize app state: {e}"))?,
        );
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
