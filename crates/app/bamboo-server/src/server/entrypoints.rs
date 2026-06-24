use std::path::{Path, PathBuf};

use actix_files as fs;
use actix_web::{
    dev::{fn_service, ServiceRequest, ServiceResponse},
    web, App, HttpResponse, HttpServer,
};
use tracing::{error, info};

use super::listeners::{build_bind_listeners, build_desktop_listeners, resolve_worker_count};
use super::tls::build_rustls_config;
use crate::app_state::AppState;
use crate::config::{build_cors, build_rate_limiter, build_security_headers};
use crate::routes::{configure_routes, configure_routes_with_rate_limiting};
use crate::services::frontend_package::{
    ensure_current_frontend_dir_in, has_embedded_frontend_package, resolve_frontend_package_path,
};
use actix_governor::Governor;
use bamboo_config::TlsConfig;

fn canonicalize_static_dir(path: &Path) -> Result<PathBuf, String> {
    let canonicalized = path
        .canonicalize()
        .map_err(|e| format!("Static directory not found: {:?}: {}", path, e))?;
    if !canonicalized.is_dir() {
        return Err(format!(
            "Static path is not a directory: {}",
            canonicalized.display()
        ));
    }
    Ok(canonicalized)
}

fn resolve_runtime_static_dir(
    bamboo_home_dir: &Path,
    configured_static_dir: Option<PathBuf>,
) -> Result<Option<PathBuf>, String> {
    if let Some(path) = configured_static_dir {
        let canonicalized = canonicalize_static_dir(&path)?;
        info!(
            "Serving static files from configured directory: {:?}",
            canonicalized
        );
        return Ok(Some(canonicalized));
    }

    if !has_embedded_frontend_package() && resolve_frontend_package_path(None).is_none() {
        info!("No embedded or sidecar Bamboo frontend package found; starting API-only server");
        return Ok(None);
    }

    let status = ensure_current_frontend_dir_in(bamboo_home_dir, None)
        .map_err(|e| format!("Failed to prepare Bamboo frontend assets: {e}"))?;
    let frontend_dir = canonicalize_static_dir(&status.frontend_dir)?;

    if status.refreshed {
        info!(
            "Refreshed Bamboo frontend assets at {} (version {}, hash {})",
            frontend_dir.display(),
            status.bundled_manifest.frontend_version,
            status.bundled_manifest.bundle_hash
        );
    } else {
        info!(
            "Using existing Bamboo frontend assets at {} (version {}, hash {})",
            frontend_dir.display(),
            status.bundled_manifest.frontend_version,
            status.bundled_manifest.bundle_hash
        );
    }

    Ok(Some(frontend_dir))
}

/// Run the unified server in desktop mode (localhost only, no rate limiting)
///
/// This is the simplest mode for desktop applications:
/// - Binds to 127.0.0.1 only (safe, localhost-only)
/// - No rate limiting (assumes single user)
/// - No security headers (development mode)
///
/// # Arguments
/// * `bamboo_home_dir` - Bamboo home directory containing all app data (config, sessions, skills, etc.)
///   Equivalent to `${HOME}/.bamboo` in standard installations.
/// * `port` - Port to listen on
pub async fn run(bamboo_home_dir: PathBuf, port: u16) -> Result<(), String> {
    run_with_tls(bamboo_home_dir, port, None).await
}

/// Like [`run`], but terminates TLS itself when `tls` is `Some` (#181).
///
/// Desktop loopback callers pass `None` and get the unchanged plaintext path.
pub async fn run_with_tls(
    bamboo_home_dir: PathBuf,
    port: u16,
    tls: Option<TlsConfig>,
) -> Result<(), String> {
    info!("Starting unified server in desktop mode...");

    let static_dir = resolve_runtime_static_dir(&bamboo_home_dir, None)?;

    let app_state = web::Data::new(
        AppState::new(bamboo_home_dir.clone())
            .await
            .map_err(|e| format!("Failed to initialize app state: {e}"))?,
    );
    // Retained for graceful shutdown after the server stops — the `move` factory
    // below consumes `app_state`. #119.
    let app_state_for_shutdown = app_state.clone();
    let workers = resolve_worker_count();

    let app_factory = move || {
        let mut app = App::new()
            .app_data(app_state.clone())
            .wrap(build_cors("127.0.0.1", port))
            // Immutable long-cache for hashed `/assets/*` (parity with the web
            // service path; harmless on localhost, useful when this binary is
            // fronted by a proxy/CDN).
            .wrap(actix_web::middleware::from_fn(
                crate::config::add_asset_cache_headers,
            ))
            .configure(configure_routes); // No rate limiting for desktop mode

        if let Some(static_path) = &static_dir {
            let index_file = static_path.join("index.html");
            info!("Serving static files from: {:?}", static_path);
            app = app.service(
                fs::Files::new("/", static_path)
                    .index_file("index.html")
                    .prefer_utf8(true)
                    .disable_content_disposition()
                    .default_handler(fn_service(move |req: ServiceRequest| {
                        let index_file = index_file.clone();
                        async move {
                            let path = req.path().to_string();
                            if path.starts_with("/api/")
                                || path.starts_with("/v1/")
                                || path.starts_with("/openai/")
                                || path.starts_with("/anthropic/")
                                || path.starts_with("/gemini/")
                            {
                                let response = HttpResponse::NotFound().finish();
                                return Ok(ServiceResponse::new(req.into_parts().0, response));
                            }

                            let (http_req, _) = req.into_parts();
                            match actix_files::NamedFile::open_async(index_file).await {
                                Ok(file) => Ok(ServiceResponse::new(
                                    http_req.clone(),
                                    file.into_response(&http_req),
                                )),
                                Err(_) => Ok(ServiceResponse::new(
                                    http_req,
                                    HttpResponse::NotFound().finish(),
                                )),
                            }
                        }
                    })),
            );
        }

        app
    };

    // Fail-fast: when TLS is configured, build the rustls config up front so a
    // bad/missing cert refuses startup instead of silently falling back to
    // plaintext. `None` → unchanged plaintext path.
    let rustls_cfg = match &tls {
        Some(tls) => Some(build_rustls_config(tls)?),
        None => None,
    };

    let listeners = build_desktop_listeners(port)?;

    let mut http = HttpServer::new(app_factory).workers(workers);
    for (idx, listener) in listeners.into_iter().enumerate() {
        http = match &rustls_cfg {
            Some(cfg) => http
                .listen_rustls_0_23(listener, cfg.clone())
                .map_err(|e| format!("Failed to attach TLS listener #{idx}: {e}"))?,
            None => http
                .listen(listener)
                .map_err(|e| format!("Failed to attach listener #{idx}: {e}"))?,
        };
    }

    let server = http.run();

    let scheme = if rustls_cfg.is_some() {
        "https"
    } else {
        "http"
    };
    info!("Unified server running on {scheme}://127.0.0.1:{port}");

    let result = server.await;

    // The server has stopped (actix handles SIGINT/SIGTERM, returning here on an
    // intended stop). Gracefully stop AppState-owned background tasks — the #47
    // MCP-proxy reconnect supervisor + MCP servers — instead of leaking them until
    // process exit. Runs on both the clean and error exit paths. #119.
    app_state_for_shutdown.shutdown().await;

    if let Err(e) = result {
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
///   Equivalent to `${HOME}/.bamboo` in standard installations.
/// * `port` - Port to listen on
/// * `bind` - Bind address (127.0.0.1, 0.0.0.0, or custom)
pub async fn run_with_bind(bamboo_home_dir: PathBuf, port: u16, bind: &str) -> Result<(), String> {
    run_with_bind_and_static(bamboo_home_dir, port, bind, None).await
}

/// Like [`run_with_bind`], but terminates TLS itself when `tls` is `Some` (#181).
pub async fn run_with_bind_tls(
    bamboo_home_dir: PathBuf,
    port: u16,
    bind: &str,
    tls: Option<TlsConfig>,
) -> Result<(), String> {
    run_with_bind_and_static_tls(bamboo_home_dir, port, bind, None, tls).await
}

/// Run the unified server with custom bind address and static file serving
///
/// Production mode with frontend serving:
/// - All features from run_with_bind()
/// - Static file serving for frontend (index.html, assets, etc.)
///
/// # Arguments
/// * `bamboo_home_dir` - Bamboo home directory containing all app data (config, sessions, skills, etc.)
///   Equivalent to `${HOME}/.bamboo` in standard installations.
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
    run_with_bind_and_static_tls(bamboo_home_dir, port, bind, static_dir, None).await
}

/// Like [`run_with_bind_and_static`], but terminates TLS itself when `tls` is
/// `Some` (#181). When `None`, the plaintext `.listen()` path is unchanged.
pub async fn run_with_bind_and_static_tls(
    bamboo_home_dir: PathBuf,
    port: u16,
    bind: &str,
    static_dir: Option<PathBuf>,
    tls: Option<TlsConfig>,
) -> Result<(), String> {
    info!("Starting unified server on {}:{}...", bind, port);

    let static_dir = resolve_runtime_static_dir(&bamboo_home_dir, static_dir)?;

    let app_state = web::Data::new(
        AppState::new(bamboo_home_dir.clone())
            .await
            .map_err(|e| format!("Failed to initialize app state: {e}"))?,
    );
    // Retained for graceful shutdown after the server stops — the `move` factory
    // below consumes `app_state`. #119.
    let app_state_for_shutdown = app_state.clone();
    let workers = resolve_worker_count();

    // Per-IP rate limiter for the network-exposed production server (#13). Built
    // once and shared (Clone) across workers; `Governor` is the outermost wrap so
    // a throttled request is rejected with 429 before any handler work.
    let rate_limiter = build_rate_limiter();
    let bind_for_cors = bind.to_string();
    let app_factory = move || {
        let mut app = App::new()
            // Request size limits to prevent DoS
            // Chat requests may include base64 images; keep limits high enough for local usage.
            .app_data(web::JsonConfig::default().limit(25 * 1024 * 1024)) // 25MB JSON limit
            .app_data(web::PayloadConfig::new(30 * 1024 * 1024)) // 30MB payload limit
            .app_data(app_state.clone())
            .wrap(Governor::new(&rate_limiter))
            .wrap(build_cors(&bind_for_cors, port))
            .wrap(build_security_headers())
            .configure(configure_routes_with_rate_limiting);

        if let Some(static_path) = &static_dir {
            let index_file = static_path.join("index.html");
            info!("Serving static files from: {:?}", static_path);
            app = app.service(
                fs::Files::new("/", static_path)
                    .index_file("index.html")
                    .prefer_utf8(true)
                    .disable_content_disposition()
                    .default_handler(fn_service(move |req: ServiceRequest| {
                        let index_file = index_file.clone();
                        async move {
                            let path = req.path().to_string();
                            if path.starts_with("/api/")
                                || path.starts_with("/v1/")
                                || path.starts_with("/openai/")
                                || path.starts_with("/anthropic/")
                                || path.starts_with("/gemini/")
                            {
                                let response = HttpResponse::NotFound().finish();
                                return Ok(ServiceResponse::new(req.into_parts().0, response));
                            }

                            let (http_req, _) = req.into_parts();
                            match actix_files::NamedFile::open_async(index_file).await {
                                Ok(file) => Ok(ServiceResponse::new(
                                    http_req.clone(),
                                    file.into_response(&http_req),
                                )),
                                Err(_) => Ok(ServiceResponse::new(
                                    http_req,
                                    HttpResponse::NotFound().finish(),
                                )),
                            }
                        }
                    })),
            );
        }

        app
    };

    // Fail-fast: build the rustls config before binding so a bad/missing cert
    // refuses startup rather than silently downgrading to plaintext. `None` →
    // unchanged plaintext path (desktop/loopback behavior preserved). #181.
    let rustls_cfg = match &tls {
        Some(tls) => Some(build_rustls_config(tls)?),
        None => None,
    };

    let listeners = build_bind_listeners(bind, port)?;

    let mut http = HttpServer::new(app_factory).workers(workers);
    for (idx, listener) in listeners.into_iter().enumerate() {
        http = match &rustls_cfg {
            Some(cfg) => http
                .listen_rustls_0_23(listener, cfg.clone())
                .map_err(|e| format!("Failed to attach TLS listener #{idx}: {e}"))?,
            None => http
                .listen(listener)
                .map_err(|e| format!("Failed to attach listener #{idx}: {e}"))?,
        };
    }

    let server = http.run();

    let scheme = if rustls_cfg.is_some() {
        "https"
    } else {
        "http"
    };
    info!("Unified server running on {scheme}://{}:{}", bind, port);

    let result = server.await;

    // Gracefully stop AppState-owned background tasks (the #47 MCP-proxy reconnect
    // supervisor + MCP servers) once the server stops, instead of leaking them
    // until process exit. Runs on both the clean and error exit paths. #119.
    app_state_for_shutdown.shutdown().await;

    if let Err(e) = result {
        error!("Server error: {}", e);
        return Err(format!("Server error: {e}"));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn resolve_runtime_static_dir_uses_configured_dir_when_present() {
        let bamboo_home = tempdir().unwrap();
        let static_dir = tempdir().unwrap();
        std::fs::write(static_dir.path().join("index.html"), "ok").unwrap();

        let resolved =
            resolve_runtime_static_dir(bamboo_home.path(), Some(static_dir.path().to_path_buf()))
                .expect("configured static dir should resolve")
                .expect("configured static dir should be returned");

        assert_eq!(resolved, static_dir.path().canonicalize().unwrap());
    }
}
