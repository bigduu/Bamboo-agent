use std::path::PathBuf;

use actix_files as fs;
use actix_web::dev::{ServiceFactory, ServiceRequest, ServiceResponse};
use actix_web::{web, App, HttpServer};
use tokio::sync::oneshot;
use tracing::{error, info};

use super::listeners::DEFAULT_WORKER_COUNT;

/// Request body size limits, applied on EVERY serve path so the desktop/embedded
/// server accepts the same payloads (e.g. an inline-image chat request) as the
/// production server, instead of falling back to actix's ~2MB JSON / 256KB
/// payload defaults and rejecting them with 413 (#252).
pub(crate) const MAX_JSON_BODY_BYTES: usize = 25 * 1024 * 1024;
pub(crate) const MAX_PAYLOAD_BYTES: usize = 30 * 1024 * 1024;

/// Install the shared request body-size limits ([`MAX_JSON_BODY_BYTES`] /
/// [`MAX_PAYLOAD_BYTES`]) onto an actix `App`.
///
/// EVERY serve path — desktop (`run_with_tls`), production
/// (`run_with_bind_and_static_tls`), and `WebService::start*` — funnels its
/// `App::new()` through this one helper, so the limits can no longer drift
/// between paths. That drift was the #252 bug: the desktop/embedded server set
/// neither limit and rejected an inline-image chat request with 413 while the
/// production server (which set them) accepted it. Callers layer their own app
/// data, middleware, routes, and static files on top of the returned `App`.
pub(crate) fn with_body_limits<T>(app: App<T>) -> App<T>
where
    T: ServiceFactory<
        ServiceRequest,
        Config = (),
        Response = ServiceResponse,
        Error = actix_web::Error,
        InitError = (),
    >,
{
    app.app_data(web::JsonConfig::default().limit(MAX_JSON_BODY_BYTES))
        .app_data(web::PayloadConfig::new(MAX_PAYLOAD_BYTES))
}
use super::tls::build_rustls_config;
use crate::app_state::AppState;
use crate::config::{
    build_cors, build_rate_limiter, build_security_headers, is_loopback_bind,
    require_limiter_for_nonloopback, wrap_governor_and_cors,
};
use crate::routes::{configure_routes, configure_routes_with_rate_limiting};
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

        // This serve path installs NO rate limiter (API-only WebService). Refuse a
        // non-loopback bind so it can't silently run unthrottled on a routable
        // interface — that would re-open the #13 DoS surface. Loopback binds stay
        // allowed (desktop behavior preserved). #169 part 3.
        require_limiter_for_nonloopback(bind, false)?;

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
            with_body_limits(App::new())
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

        // Per-IP rate limiter (#13) will be applied below for non-loopback binds.
        // SKIPPED for loopback/desktop binds: the local frontend legitimately
        // bursts ~45 hashed `/assets/*` requests on load and would otherwise be
        // throttled to a 429 (chunk import fails / "Too many requests").
        let apply_rate_limit = !is_loopback_bind(bind);
        // Bind-aware guard: a non-loopback bind must have the limiter applied
        // (it is here for non-loopback binds). Belt-and-suspenders against a
        // future edit that flips `apply_rate_limit` off for a routable bind.
        // Checked BEFORE the async app-state setup so a bad bind fails fast,
        // consistent with `start_with_bind_tls`. #169, #428.
        require_limiter_for_nonloopback(bind, apply_rate_limit)?;

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
        // Per-IP rate limiter for the network-exposed production server (#13).
        let rate_limiter = build_rate_limiter();
        let bind_addr = bind.to_string();
        let listen_addr = format!("{bind}:{port}");
        let bind_for_log = bind_addr.clone();

        let server = HttpServer::new(move || {
            // WRAP ORDER (#169 part 2, #428): Governor + CORS are applied
            // together, in the fixed order enforced by the shared
            // `wrap_governor_and_cors` helper (Governor inner, CORS outer) —
            // see its doc comment in config.rs for why the order is
            // load-bearing, and the `governor_*_cors_*` regression tests
            // there, which exercise this SAME helper so a swap can no longer
            // regress in only one call site.
            wrap_governor_and_cors(
                with_body_limits(App::new()).app_data(app_state.clone()),
                &rate_limiter,
                apply_rate_limit,
                &bind_addr,
                port,
            )
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

    /// #252: the shared [`with_body_limits`] factory must raise actix's default
    /// ~2MB JSON limit to 25MB on every serve path. This is the limit the
    /// desktop/embedded serve path previously lacked (rejecting inline-image
    /// chat requests with 413 while production accepted them). The control app
    /// built without `with_body_limits` rejects the same body, proving the
    /// factory — not a default — is doing the work, so this test fails without
    /// the shared-factory change.
    #[actix_web::test]
    async fn shared_factory_raises_json_body_limit() {
        use actix_web::{http::StatusCode, test, HttpResponse};

        async fn echo(_body: web::Json<serde_json::Value>) -> HttpResponse {
            HttpResponse::Ok().finish()
        }

        // ~3MB JSON body: over actix's ~2MB default, under the 25MB shared limit.
        let big = "x".repeat(3 * 1024 * 1024);
        let payload = serde_json::json!({ "data": big });

        // Via the shared factory (what every serve path now funnels through).
        let app =
            test::init_service(with_body_limits(App::new()).route("/echo", web::post().to(echo)))
                .await;
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/echo")
                .set_json(&payload)
                .to_request(),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "shared factory must accept a >2MB JSON body (#252)"
        );

        // Control: a plain `App` (actix's ~2MB default) rejects the same body.
        let app_default = test::init_service(App::new().route("/echo", web::post().to(echo))).await;
        let resp_default = test::call_service(
            &app_default,
            test::TestRequest::post()
                .uri("/echo")
                .set_json(&payload)
                .to_request(),
        )
        .await;
        assert_eq!(
            resp_default.status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "actix's default JSON limit must reject a >2MB body"
        );
    }

    /// #169 part 3: the no-limiter `start_with_bind` path must REFUSE a
    /// non-loopback bind (which would run unthrottled on a routable interface),
    /// while still accepting a loopback bind. Without the guard, this call would
    /// happily start an unthrottled network server (returning `Ok`), so the
    /// `is_err()` assertion fails without the fix.
    #[tokio::test]
    async fn start_with_bind_rejects_nonloopback_without_limiter() {
        let home = tempfile::TempDir::new().expect("tempdir");
        let mut service = WebService::new(home.path().to_path_buf());

        // Port 0 → OS-assigned ephemeral port, so a false "Ok" would actually bind.
        let err = service
            .start_with_bind(0, "0.0.0.0")
            .await
            .expect_err("non-loopback bind without a limiter must be rejected (#169 part 3)");
        assert!(
            err.contains("without a rate limiter"),
            "rejection must explain the missing limiter, got: {err}"
        );
        assert!(
            !service.is_running(),
            "the guard must reject BEFORE the server starts"
        );

        // Sanity: loopback is still accepted (desktop behavior preserved).
        service
            .start_with_bind(0, "127.0.0.1")
            .await
            .expect("loopback bind must still start without a limiter");
        service.stop().await.expect("web service stops");
    }

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
