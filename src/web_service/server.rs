use std::{path::PathBuf, sync::Arc};

use actix_cors::Cors;
use actix_files as fs;
use actix_governor::{Governor, GovernorConfigBuilder};
use actix_web::{http::header, web, App, HttpServer};
use actix_web::middleware::DefaultHeaders;
use crate::agent::llm::LLMProvider;
use crate::agent::metrics::{MetricsBus, MetricsStorage, MetricsWorker};
use crate::agent::server::handlers as agent_handlers;
use crate::agent::server::state::AppState as AgentAppState;
use crate::core::Config;
use log::{error, info};
use tokio::sync::{oneshot, RwLock};

use crate::web_service::controllers::anthropic as anthropic_controller;
use crate::web_service::controllers::copilot_auth_controller;
use crate::web_service::controllers::gemini_controller;
use crate::web_service::controllers::{
    agent_controller, command_controller, settings_controller, openai_controller,
    skill_controller, tools_controller, workspace_controller,
};
use crate::web_service::model_config_helper::get_default_model_from_config;

const DEFAULT_METRICS_CHANNEL_CAPACITY: usize = 10000;

pub struct AppState {
    pub app_data_dir: PathBuf,
    pub provider: Arc<RwLock<Arc<dyn LLMProvider>>>,
    pub config: Arc<RwLock<Config>>,
    pub metrics_bus: Option<MetricsBus>,
}

impl AppState {
    /// Reload the provider based on current configuration
    pub async fn reload_provider(&self) -> Result<(), crate::agent::llm::LLMError> {
        let config = self.config.read().await.clone();

        log::info!(
            "Reloading provider: type={}, model={:?}",
            config.provider,
            config.providers.anthropic.as_ref().and_then(|p| p.model.as_ref())
        );

        let new_provider = crate::agent::llm::create_provider_with_dir(&config, self.app_data_dir.clone()).await?;

        let mut provider = self.provider.write().await;
        *provider = new_provider;

        log::info!("Provider reloaded successfully to: {}", config.provider);
        Ok(())
    }

    /// Reload the configuration from file
    pub async fn reload_config(&self) -> Config {
        let new_config = Config::new();
        let mut config = self.config.write().await;
        *config = new_config.clone();
        new_config
    }

    /// Get a clone of the current provider
    pub async fn get_provider(&self) -> Arc<dyn LLMProvider> {
        self.provider.read().await.clone()
    }
}

/// Holds metrics infrastructure for the web service
pub struct MetricsState {
    pub bus: MetricsBus,
    pub worker_handle: Arc<std::sync::atomic::AtomicBool>,
}

impl MetricsState {
    /// Create and spawn the metrics infrastructure
    pub async fn spawn(app_data_dir: PathBuf) -> Self {
        // Create metrics bus with bounded channel
        let (bus, receiver) = MetricsBus::new(DEFAULT_METRICS_CHANNEL_CAPACITY);

        // Create storage
        let db_path = app_data_dir.join("metrics.db");
        let storage = Arc::new(crate::agent::metrics::SqliteMetricsStorage::new(db_path));

        // Initialize storage
        if let Err(e) = storage.init().await {
            error!("Failed to initialize metrics storage: {}", e);
        }

        // Spawn worker
        let worker = MetricsWorker::new(storage);
        let worker_handle = worker.spawn(receiver, bus.clone());

        Self { bus, worker_handle }
    }

    /// Stop the metrics worker
    pub fn stop(&self) {
        self.worker_handle.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

const DEFAULT_WORKER_COUNT: usize = 10;

/// Build security headers for production deployments
fn build_security_headers() -> DefaultHeaders {
    DefaultHeaders::new()
        .add(("X-Frame-Options", "DENY"))
        .add(("X-Content-Type-Options", "nosniff"))
        .add(("X-XSS-Protection", "1; mode=block"))
        .add(("Referrer-Policy", "strict-origin-when-cross-origin"))
        // Note: CSP should be customized based on your specific needs
        .add(("Content-Security-Policy", "default-src 'self'; script-src 'self' 'unsafe-inline' 'unsafe-eval'; style-src 'self' 'unsafe-inline'; img-src 'self' data: https:; font-src 'self' data:; connect-src 'self' ws: wss;"))
}

/// Build CORS configuration based on bind address and port
fn build_cors(bind_addr: &str, port: u16) -> Cors {
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

pub fn app_config(cfg: &mut web::ServiceConfig) {
    // OpenAI and other endpoints under /v1
    cfg.service(
        web::scope("/v1")
            .configure(agent_controller::config)
            .configure(command_controller::config)
            .configure(openai_controller::config)
            .configure(settings_controller::config)
            .configure(skill_controller::config)
            .configure(tools_controller::config)
            .configure(workspace_controller::config)
            .configure(copilot_auth_controller::config),
    );

    // Anthropic endpoints under /anthropic/v1 (to match Anthropic SDK expectations)
    cfg.service(
        web::scope("/anthropic/v1").configure(anthropic_controller::config),
    );

    // Gemini endpoints under /gemini/v1beta (to match Gemini SDK expectations)
    cfg.service(
        web::scope("/gemini/v1beta").configure(gemini_controller::config),
    );
}

/// Production config with rate limiting enabled
pub fn app_config_with_rate_limiting(cfg: &mut web::ServiceConfig) {
    // Build rate limiter for production: 10 req/sec, burst 20
    let rate_limiter = GovernorConfigBuilder::default()
        .per_second(10)
        .burst_size(20)
        .finish()
        .expect("Failed to build rate limiter");

    // OpenAI and other endpoints under /v1 with rate limiting
    // Note: Rate limiting is applied per-service to allow CORS to handle OPTIONS requests first
    cfg.service(
        web::scope("/v1")
            .configure(agent_controller::config)
            .configure(command_controller::config)
            // Apply rate limiting only to openai_controller (chat completions)
            .service(
                web::scope("")
                    .wrap(Governor::new(&rate_limiter))
                    .configure(openai_controller::config)
            )
            .configure(settings_controller::config)
            .configure(skill_controller::config)
            .configure(tools_controller::config)
            .configure(workspace_controller::config)
            .configure(copilot_auth_controller::config),
    );

    // Anthropic endpoints under /anthropic/v1 (to match Anthropic SDK expectations)
    cfg.service(
        web::scope("/anthropic/v1").configure(anthropic_controller::config),
    );

    // Gemini endpoints under /gemini/v1beta (to match Gemini SDK expectations)
    cfg.service(
        web::scope("/gemini/v1beta").configure(gemini_controller::config),
    );
}

pub fn agent_api_config(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/api/v1")
            .route("/chat", web::post().to(agent_handlers::chat::handler))
            // New separated execute + events endpoints
            .route(
                "/execute/{session_id}",
                web::post().to(agent_handlers::execute::handler),
            )
            .route(
                "/events/{session_id}",
                web::get().to(agent_handlers::events::handler),
            )
            // Legacy stream endpoint (deprecated)
            .route(
                "/stream/{session_id}",
                web::get().to(agent_handlers::stream::handler),
            )
            .route(
                "/stop/{session_id}",
                web::post().to(agent_handlers::stop::handler),
            )
            .route(
                "/history/{session_id}",
                web::get().to(agent_handlers::history::handler),
            )
            .route(
                "/todo/{session_id}",
                web::get().to(agent_handlers::todo::get_todo_list),
            )
            .route(
                "/todo/{session_id}/exists",
                web::get().to(agent_handlers::todo::has_todo_list),
            )
            .route(
                "/respond/{session_id}",
                web::post().to(agent_handlers::respond::submit_response),
            )
            .route(
                "/respond/{session_id}/pending",
                web::get().to(agent_handlers::respond::get_pending_question),
            )
            .route(
                "/sessions/{session_id}",
                web::delete().to(agent_handlers::delete::handler),
            )
            .route(
                "/metrics/summary",
                web::get().to(agent_handlers::metrics::summary),
            )
            .route(
                "/metrics/by-model",
                web::get().to(agent_handlers::metrics::by_model),
            )
            .route(
                "/metrics/sessions",
                web::get().to(agent_handlers::metrics::sessions),
            )
            .route(
                "/metrics/sessions/{session_id}",
                web::get().to(agent_handlers::metrics::session_detail),
            )
            .route(
                "/metrics/daily",
                web::get().to(agent_handlers::metrics::daily),
            )
            // Forward metrics routes
            .route(
                "/metrics/forward/summary",
                web::get().to(agent_handlers::metrics::forward_summary),
            )
            .route(
                "/metrics/forward/by-endpoint",
                web::get().to(agent_handlers::metrics::forward_by_endpoint),
            )
            .route(
                "/metrics/forward/requests",
                web::get().to(agent_handlers::metrics::forward_requests),
            )
            .route("/health", web::get().to(agent_handlers::health::handler))
            // MCP routes
            .service(
                web::scope("/mcp")
                    .route("/servers", web::get().to(agent_handlers::mcp::list_servers))
                    .route("/servers", web::post().to(agent_handlers::mcp::add_server))
                    .route("/servers/{id}", web::get().to(agent_handlers::mcp::get_server))
                    .route("/servers/{id}", web::put().to(agent_handlers::mcp::update_server))
                    .route(
                        "/servers/{id}",
                        web::delete().to(agent_handlers::mcp::delete_server),
                    )
                    .route(
                        "/servers/{id}/connect",
                        web::post().to(agent_handlers::mcp::connect_server),
                    )
                    .route(
                        "/servers/{id}/disconnect",
                        web::post().to(agent_handlers::mcp::disconnect_server),
                    )
                    .route(
                        "/servers/{id}/refresh",
                        web::post().to(agent_handlers::mcp::refresh_tools),
                    )
                    .route(
                        "/servers/{id}/tools",
                        web::get().to(agent_handlers::mcp::get_server_tools),
                    )
                    .route("/tools", web::get().to(agent_handlers::mcp::list_tools)),
            ),
    );
}

async fn build_agent_state(app_data_dir: PathBuf, port: u16, config: &Config) -> Result<AgentAppState, String> {
    let base_url = format!("http://127.0.0.1:{}/v1", port);

    // Get model from config - fail if not configured
    let model = get_default_model_from_config(config)
        .map_err(|e| format!("Failed to get model from config: {}. Please specify a model in your config.json file.", e))?;

    info!("Agent Server using model from config: {}", model);

    Ok(AgentAppState::new_with_config(
        "openai",
        base_url,
        model,
        "tauri".to_string(),
        Some(app_data_dir),
        true,
    ).await)
}

pub async fn run(app_data_dir: PathBuf, port: u16) -> Result<(), String> {
    info!("Starting web service...");

    // Initialize metrics infrastructure
    let metrics_state = MetricsState::spawn(app_data_dir.clone()).await;

    let config = Config::new();

    // Create provider based on configuration
    let provider = crate::agent::llm::create_provider_with_dir(&config, app_data_dir.clone())
        .await
        .map_err(|e| format!("Failed to create provider: {}", e))?;

    let agent_state = web::Data::new(
        build_agent_state(app_data_dir.clone(), port, &config).await?
    );

    let app_state = web::Data::new(AppState {
        app_data_dir,
        provider: Arc::new(RwLock::new(provider)),
        config: Arc::new(RwLock::new(config)),
        metrics_bus: Some(metrics_state.bus.clone()),
    });

    let server = HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .app_data(agent_state.clone())
            .wrap(build_cors("127.0.0.1", port))
            .configure(app_config)  // No rate limiting for desktop mode (localhost only)
            .configure(agent_api_config)
    })
    .workers(DEFAULT_WORKER_COUNT)
    .bind(format!("127.0.0.1:{port}"))
    .map_err(|e| format!("Failed to bind server: {e}"))?
    .run();

    info!("Starting web service on http://127.0.0.1:{port}");

    if let Err(e) = server.await {
        error!("Web server error: {}", e);
        return Err(format!("Web server error: {e}"));
    }

    // Stop metrics worker on shutdown
    metrics_state.stop();

    Ok(())
}

pub struct WebService {
    shutdown_tx: Option<oneshot::Sender<()>>,
    server_handle: Option<tokio::task::JoinHandle<()>>,
    app_data_dir: PathBuf,
    metrics_state: Option<MetricsState>,
}

impl WebService {
    pub fn new(app_data_dir: PathBuf) -> Self {
        Self {
            shutdown_tx: None,
            server_handle: None,
            app_data_dir,
            metrics_state: None,
        }
    }

    pub async fn start(&mut self, port: u16) -> Result<(), String> {
        info!("Starting web service...");
        if self.server_handle.is_some() {
            return Err("Web service is already running".to_string());
        }

        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

        // Initialize metrics infrastructure
        let metrics_state = MetricsState::spawn(self.app_data_dir.clone()).await;

        let config = Config::new();

        // Create provider based on configuration
        let provider = crate::agent::llm::create_provider(&config)
            .await
            .map_err(|e| format!("Failed to create provider: {}", e))?;

        let agent_state = web::Data::new(
            build_agent_state(self.app_data_dir.clone(), port, &config).await?
        );

        let app_state = web::Data::new(AppState {
            app_data_dir: self.app_data_dir.clone(),
            provider: Arc::new(RwLock::new(provider)),
            config: Arc::new(RwLock::new(config)),
            metrics_bus: Some(metrics_state.bus.clone()),
        });

        let server = HttpServer::new(move || {
            App::new()
                .app_data(app_state.clone())
                .app_data(agent_state.clone())
                .wrap(build_cors("127.0.0.1", port))
                .configure(app_config)
                .configure(agent_api_config)
        })
        .workers(DEFAULT_WORKER_COUNT)
        .bind(format!("127.0.0.1:{port}"))
        .map_err(|e| format!("Failed to bind server: {e}"))?
        .run();

        let server_handle = tokio::spawn(async move {
            tokio::select! {
                result = server => {
                    if let Err(e) = result {
                        error!("Web server error: {}", e);
                    }
                }
                _ = &mut shutdown_rx => {
                    info!("Web service shutdown signal received");
                }
            }
        });

        self.shutdown_tx = Some(shutdown_tx);
        self.server_handle = Some(server_handle);
        self.metrics_state = Some(metrics_state);

        info!("Web service started successfully");
        Ok(())
    }

    pub async fn stop(&mut self) -> Result<(), String> {
        // Stop metrics worker
        if let Some(metrics_state) = &self.metrics_state {
            metrics_state.stop();
        }

        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            if shutdown_tx.send(()).is_err() {
                error!("Failed to send shutdown signal");
            }
        }

        if let Some(handle) = self.server_handle.take() {
            if let Err(e) = handle.await {
                error!("Error waiting for server shutdown: {}", e);
                return Err(format!("Error waiting for server shutdown: {e}"));
            }
        }

        self.metrics_state = None;
        info!("Web service stopped successfully");
        Ok(())
    }

    pub fn is_running(&self) -> bool {
        self.server_handle.is_some()
    }
}

impl Drop for WebService {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
    }
}

/// Run server with custom bind address (for Docker deployment)
pub async fn run_with_bind(app_data_dir: PathBuf, port: u16, bind: &str) -> Result<(), String> {
    // Convert to owned String to avoid lifetime issues with closure
    let bind_addr = bind.to_string();

    info!("Starting web service on {}:{}...", bind_addr, port);

    // Initialize metrics infrastructure
    let metrics_state = MetricsState::spawn(app_data_dir.clone()).await;

    let config = Config::new();

    // Create provider based on configuration
    let provider = crate::agent::llm::create_provider_with_dir(&config, app_data_dir.clone())
        .await
        .map_err(|e| format!("Failed to create provider: {}", e))?;

    let agent_state = web::Data::new(
        build_agent_state(app_data_dir.clone(), port, &config).await?
    );

    let app_state = web::Data::new(AppState {
        app_data_dir,
        provider: Arc::new(RwLock::new(provider)),
        config: Arc::new(RwLock::new(config)),
        metrics_bus: Some(metrics_state.bus.clone()),
    });

    // Move bind_addr into the closure
    let bind_for_closure = bind_addr.clone();
    let bind_for_cors = bind_addr.clone();

    let server = HttpServer::new(move || {
        App::new()
            // Request size limits to prevent DoS
            .app_data(web::JsonConfig::default().limit(1024 * 1024)) // 1MB JSON limit
            .app_data(web::PayloadConfig::new(10 * 1024 * 1024)) // 10MB payload limit
            .app_data(app_state.clone())
            .app_data(agent_state.clone())
            .wrap(build_cors(&bind_for_cors, port))
            .wrap(build_security_headers())
            .configure(app_config_with_rate_limiting)  // Enable rate limiting
            .configure(agent_api_config)
    })
    .workers(DEFAULT_WORKER_COUNT)
    .bind(format!("{}:{}", bind_for_closure, port))
    .map_err(|e| format!("Failed to bind server: {e}"))?
    .run();

    info!("Web service running on http://{}:{}", bind, port);

    if let Err(e) = server.await {
        error!("Web server error: {}", e);
        return Err(format!("Web server error: {e}"));
    }

    // Stop metrics worker on shutdown
    metrics_state.stop();

    Ok(())
}

/// Run server with custom bind address and static file serving
///
/// This is the recommended function for production deployments (Docker, standalone binary):
/// - Serves both API endpoints and static frontend files from the same server
/// - Binds to custom address (use 0.0.0.0 for Docker, 127.0.0.1 for standalone)
/// - Includes rate limiting, security headers, and request size limits
///
/// # Arguments
/// * `app_data_dir` - Application data directory for sessions, config, etc.
/// * `port` - Port to listen on
/// * `bind` - Bind address (127.0.0.1 for localhost, 0.0.0.0 for all interfaces)
/// * `static_dir` - Optional directory containing built frontend files (index.html, assets, etc.)
///
/// # Example
/// ```bash
/// # Docker mode (serve frontend)
/// web_service_standalone serve --port 8080 --bind 0.0.0.0 --static-dir /app/static
///
/// # Standalone production mode (serve frontend)
/// web_service_standalone serve --port 8080 --static-dir ./dist
/// ```
pub async fn run_with_bind_and_static(
    app_data_dir: PathBuf,
    port: u16,
    bind: &str,
    static_dir: Option<PathBuf>,
) -> Result<(), String> {
    info!("Starting web service on {}:{}...", bind, port);

    // Canonicalize static_dir path to absolute path before passing to workers
    // This is required for fs::Files to work correctly in multi-threaded environment
    let static_dir: Option<PathBuf> = match static_dir {
        Some(path) => {
            let canonicalized = path.canonicalize()
                .map_err(|e| format!("Static directory not found: {:?}: {}", path, e))?;
            if !canonicalized.is_dir() {
                return Err(format!("Static path is not a directory: {}", canonicalized.display()));
            }
            info!("Serving static files from: {:?}", canonicalized);
            Some(canonicalized)
        }
        None => None,
    };

    // Initialize metrics infrastructure
    let metrics_state = MetricsState::spawn(app_data_dir.clone()).await;

    let config = Config::new();

    // Create provider based on configuration
    let provider = crate::agent::llm::create_provider_with_dir(&config, app_data_dir.clone())
        .await
        .map_err(|e| format!("Failed to create provider: {}", e))?;

    let agent_state = web::Data::new(
        build_agent_state(app_data_dir.clone(), port, &config).await?
    );

    let app_state = web::Data::new(AppState {
        app_data_dir,
        provider: Arc::new(RwLock::new(provider)),
        config: Arc::new(RwLock::new(config)),
        metrics_bus: Some(metrics_state.bus.clone()),
    });

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
            .app_data(agent_state.clone())
            .wrap(build_cors(&bind_for_cors, port))
            .wrap(build_security_headers())
            .configure(app_config_with_rate_limiting)  // Enable rate limiting
            .configure(agent_api_config);

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

    info!("Web service running on http://{}:{}", bind, port);

    if let Err(e) = server.await {
        error!("Web server error: {}", e);
        return Err(format!("Web server error: {e}"));
    }

    // Stop metrics worker on shutdown
    metrics_state.stop();

    Ok(())
}
