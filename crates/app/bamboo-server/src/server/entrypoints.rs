use std::path::{Path, PathBuf};

use actix_files as fs;
use actix_web::{
    dev::{fn_service, ServiceRequest, ServiceResponse},
    web, App, HttpResponse,
};
use tracing::{error, info};

use super::h1::build_h1_server;
use super::listeners::{build_bind_listeners, build_desktop_listeners, resolve_worker_count};
use super::tls::build_rustls_config;
use crate::app_state::AppState;
use crate::config::{
    build_cors, build_rate_limiter, build_security_headers, is_loopback_bind,
    require_limiter_for_nonloopback, wrap_governor_and_cors,
};
use crate::routes::{configure_routes, configure_routes_with_rate_limiting};
use crate::services::frontend_package::{
    ensure_current_frontend_dir_in, has_embedded_frontend_package, resolve_frontend_package_path,
};
use bamboo_config::TlsConfig;

/// Whether `path` belongs to bamboo's API surface (as opposed to a SPA
/// frontend route the static-file fallback should serve `index.html` for).
///
/// Shared by both SPA-fallback closures below (desktop + production/Docker
/// serve paths) so the allow-list can't drift between them — previously each
/// closure hand-duplicated this list and neither included `/v2/` (the pairing/
/// device/WS-multiplex prefix), so an unmatched `/v2/*` path would silently
/// fall through to `index.html` instead of a real 404. #251 (finding 7).
fn is_api_path(path: &str) -> bool {
    path.starts_with("/api/")
        || path.starts_with("/v1/")
        || path.starts_with("/v2/")
        || path.starts_with("/openai/")
        || path.starts_with("/anthropic/")
        || path.starts_with("/gemini/")
}

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
/// Desktop loopback callers pass `None` and get the unchanged plaintext H1 path.
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
        // Body limits (and any future shared app config) come from the one shared
        // factory used by every serve path, so a desktop chat request with an
        // inline image isn't rejected with 413 while production accepts it — the
        // paths can no longer drift apart (#252).
        let mut app = super::web_service::with_body_limits(App::new())
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
                            if is_api_path(&path) {
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
    // plaintext. `None` → unchanged plaintext H1 path.
    let rustls_cfg = match &tls {
        Some(tls) => Some(build_rustls_config(tls)?),
        None => None,
    };

    let listeners = build_desktop_listeners(port)?;

    let server = build_h1_server(app_factory, listeners, workers, rustls_cfg.clone())
        .map_err(|e| format!("Failed to build HTTP/1.1 server: {e}"))?;

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
/// `Some` (#181). When `None`, the plaintext HTTP/1.1 path is unchanged.
pub async fn run_with_bind_and_static_tls(
    bamboo_home_dir: PathBuf,
    port: u16,
    bind: &str,
    static_dir: Option<PathBuf>,
    tls: Option<TlsConfig>,
) -> Result<(), String> {
    info!("Starting unified server on {}:{}...", bind, port);

    // Loopback/desktop binds skip the limiter (see is_loopback_bind): the local
    // frontend bursts ~45 asset requests on load. Network binds stay throttled.
    let apply_rate_limit = !is_loopback_bind(bind);
    // Bind-aware guard: a non-loopback bind must have the limiter applied (it is,
    // below). Defends against a future edit that disables it for a routable bind.
    // Checked BEFORE the async app-state/static-dir setup so a bad bind fails fast,
    // consistent with `start_with_bind_tls`. #169, #428.
    require_limiter_for_nonloopback(bind, apply_rate_limit)?;

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
    // once and shared (Clone) across workers. It is wrapped so that a throttled
    // request is rejected with 429 before any handler work runs.
    let rate_limiter = build_rate_limiter();
    let bind_for_cors = bind.to_string();
    let app_factory = move || {
        // Request size limits (base64-image chats) come from the one shared
        // factory used by every serve path — same limits everywhere, no drift
        // (#252).
        // WRAP ORDER (#169 part 2, #428): Governor + CORS are applied together,
        // in the fixed order enforced by the shared `wrap_governor_and_cors`
        // helper (Governor inner, CORS outer) — see its doc comment in
        // config.rs for why the order is load-bearing, and the
        // `governor_*_cors_*` regression tests there, which exercise this SAME
        // helper so a swap can no longer regress in only one call site.
        let mut app = wrap_governor_and_cors(
            super::web_service::with_body_limits(App::new()).app_data(app_state.clone()),
            &rate_limiter,
            apply_rate_limit,
            &bind_for_cors,
            port,
        )
        .wrap(build_security_headers())
        // Immutable long-cache for hashed `/assets/*` (Docker / `serve -s`
        // path, fronted by a proxy/CDN — same fix as the other factories).
        .wrap(actix_web::middleware::from_fn(
            crate::config::add_asset_cache_headers,
        ))
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
                            if is_api_path(&path) {
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
    // unchanged plaintext H1 path (desktop/loopback behavior preserved). #181.
    let rustls_cfg = match &tls {
        Some(tls) => Some(build_rustls_config(tls)?),
        None => None,
    };

    let listeners = build_bind_listeners(bind, port)?;

    let server = build_h1_server(app_factory, listeners, workers, rustls_cfg.clone())
        .map_err(|e| format!("Failed to build HTTP/1.1 server: {e}"))?;

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

    #[test]
    fn is_api_path_covers_every_registered_version_prefix() {
        // #251 (finding 7): every prefix `routes::configure_routes` actually
        // registers must be recognized here, or an unmatched sub-path under it
        // would wrongly fall through to the SPA `index.html` instead of a 404.
        for api_path in [
            "/api/v1/sessions",
            "/v1/bamboo/workflows",
            "/v2/unknown",
            "/openai/v1/models",
            "/anthropic/v1/messages",
            "/gemini/v1beta/models",
        ] {
            assert!(is_api_path(api_path), "{api_path} must be an API path");
        }

        for frontend_path in ["/", "/index.html", "/assets/app.js", "/settings"] {
            assert!(
                !is_api_path(frontend_path),
                "{frontend_path} must NOT be treated as an API path"
            );
        }
    }

    /// #512: every prior route/allow-list test (`routes::tests`,
    /// `is_api_path_covers_every_registered_version_prefix` above) exercises
    /// `configure_routes`/`is_api_path` in isolation — never together, and
    /// never with the `actix_files::Files` SPA-fallback service actually
    /// mounted the way the real `run`/`run_with_bind_and_static_tls` server
    /// factories mount it. That gap matters: `Files::new("/", ..)` is
    /// registered LAST (after `.configure(configure_routes)`), and its
    /// `default_handler` is the ONLY thing standing between an unmatched path
    /// and the SPA `index.html`. A route-table assertion can't see that
    /// interaction; only a real `test::call_service` against the exact same
    /// composed `App` can. This test builds that composition (routes +
    /// Files-with-`is_api_path`-gated-fallback, mirroring the closures in
    /// `run_with_tls`/`run_with_bind_and_static_tls` above) and drives every
    /// native-API prefix plus the SPA fallback through it end-to-end.
    #[actix_web::test]
    async fn full_app_assembly_forwards_every_api_prefix_and_still_serves_spa_fallback() {
        use actix_web::dev::{fn_service, ServiceRequest, ServiceResponse};
        use actix_web::http::StatusCode;
        use actix_web::{test, App};

        let static_dir = tempdir().unwrap();
        let index_file = static_dir.path().join("index.html");
        std::fs::write(&index_file, "<html>spa-fallback-marker</html>").unwrap();

        let app = test::init_service(
            App::new()
                .configure(crate::routes::configure_routes)
                .service(
                    fs::Files::new("/", static_dir.path())
                        .index_file("index.html")
                        .default_handler(fn_service(move |req: ServiceRequest| {
                            let index_file = index_file.clone();
                            async move {
                                let path = req.path().to_string();
                                if is_api_path(&path) {
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
                ),
        )
        .await;

        // Every native-API prefix (legacy /v1 alias, canonical /api/v1, the
        // /api/v1-nested session sub-resource alias, /v2, and the three
        // provider-forwarding prefixes) must still reach ITS OWN handler, not
        // get swallowed by the Files fallback registered after it.
        for (method, uri) in [
            ("GET", "/v1/bamboo/workflows"),
            ("GET", "/api/v1/bamboo/workflows"),
            ("GET", "/api/v1/sessions"),
            ("GET", "/api/v1/sessions/does-not-exist/history"),
            ("GET", "/api/v1/history/does-not-exist"),
            ("GET", "/v2/stream"),
            ("GET", "/openai/v1/models"),
            ("GET", "/anthropic/v1/models"),
            ("GET", "/gemini/v1beta/models"),
        ] {
            let req = test::TestRequest::with_uri(uri)
                .method(method.parse().unwrap())
                .to_request();
            let resp = test::call_service(&app, req).await;
            assert_ne!(
                resp.status(),
                StatusCode::NOT_FOUND,
                "{method} {uri} must be routed to its real handler, not 404 via the SPA fallback"
            );
        }

        // The two flat/nested session-history aliases must resolve to the SAME
        // handler (both "session not found", not one 404-route/one 404-session).
        let flat = test::TestRequest::get()
            .uri("/api/v1/history/does-not-exist")
            .to_request();
        let flat_status = test::call_service(&app, flat).await.status();
        let nested = test::TestRequest::get()
            .uri("/api/v1/sessions/does-not-exist/history")
            .to_request();
        let nested_status = test::call_service(&app, nested).await.status();
        assert_eq!(
            flat_status, nested_status,
            "flat and nested history aliases must behave identically"
        );

        // An unmatched path UNDER a real API prefix must 404 for real — it must
        // NOT fall through to index.html just because Files is mounted at "/".
        let bogus_api_req = test::TestRequest::get()
            .uri("/api/v1/totally-not-a-real-route")
            .to_request();
        let bogus_api_resp = test::call_service(&app, bogus_api_req).await;
        assert_eq!(
            bogus_api_resp.status(),
            StatusCode::NOT_FOUND,
            "an unmatched /api/v1/* path must 404, not silently serve the SPA"
        );

        // A genuine frontend deep-link (not under any API prefix) must serve
        // index.html via the SPA fallback, proving the fallback still works
        // once every API scope above it has had its shot at matching first.
        let spa_req = test::TestRequest::get()
            .uri("/chat/some-deep-route")
            .to_request();
        let spa_resp = test::call_service(&app, spa_req).await;
        assert_eq!(spa_resp.status(), StatusCode::OK);
        let body = actix_web::body::to_bytes(spa_resp.into_body())
            .await
            .unwrap();
        assert!(
            String::from_utf8_lossy(&body).contains("spa-fallback-marker"),
            "non-API deep link must serve the SPA index.html"
        );
    }
}
