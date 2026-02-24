use actix_cors::Cors;
use actix_web::{web, App, HttpServer};
use std::io;
use std::path::PathBuf;
use std::thread;

use crate::agent::server::state::AppState;
use crate::server::handlers::agent;

#[allow(dead_code)]
pub async fn run_server(port: u16) -> io::Result<()> {
    run_server_with_config(
        port,
        "openai",
        "http://localhost:12123".to_string(),
        "kimi-for-coding".to_string(),
        "sk-test".to_string(),
    )
    .await
}

pub async fn run_server_with_config(
    port: u16,
    provider: &str,
    llm_base_url: String,
    model: String,
    api_key: String,
) -> io::Result<()> {
    run_server_with_config_and_mode(port, provider, llm_base_url, model, api_key, None, false).await
}

pub async fn run_server_with_config_and_mode(
    port: u16,
    provider: &str,
    llm_base_url: String,
    model: String,
    api_key: String,
    app_data_dir: Option<PathBuf>,
    tauri_mode: bool,
) -> io::Result<()> {
    log::info!(
        "Initializing server with provider: {}, base URL: {}",
        provider,
        llm_base_url
    );
    let state = web::Data::new(
        AppState::new_with_config(
            provider,
            llm_base_url,
            model,
            api_key,
            app_data_dir,
            tauri_mode,
        )
        .await,
    );

    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .wrap(Cors::permissive())
            .service(
                web::scope("/api/v1")
                    .route("/chat", web::post().to(agent::chat::handler))
                    // New separated execute + events endpoints
                    .route(
                        "/execute/{session_id}",
                        web::post().to(agent::execute::handler),
                    )
                    .route(
                        "/events/{session_id}",
                        web::get().to(agent::events::handler),
                    )
                    // Legacy stream endpoint (deprecated)
                    .route(
                        "/stream/{session_id}",
                        web::get().to(agent::stream::handler),
                    )
                    .route("/stop/{session_id}", web::post().to(agent::stop::handler))
                    .route(
                        "/history/{session_id}",
                        web::get().to(agent::history::handler),
                    )
                    .route(
                        "/todo/{session_id}",
                        web::get().to(agent::todo::get_todo_list),
                    )
                    .route(
                        "/todo/{session_id}/exists",
                        web::get().to(agent::todo::has_todo_list),
                    )
                    .route(
                        "/respond/{session_id}",
                        web::post().to(agent::respond::submit_response),
                    )
                    .route(
                        "/respond/{session_id}/pending",
                        web::get().to(agent::respond::get_pending_question),
                    )
                    .route(
                        "/sessions/{session_id}",
                        web::delete().to(agent::delete::handler),
                    )
                    .route("/metrics/summary", web::get().to(agent::metrics::summary))
                    .route("/metrics/by-model", web::get().to(agent::metrics::by_model))
                    .route("/metrics/sessions", web::get().to(agent::metrics::sessions))
                    .route(
                        "/metrics/sessions/{session_id}",
                        web::get().to(agent::metrics::session_detail),
                    )
                    .route("/metrics/daily", web::get().to(agent::metrics::daily))
                    // Unified v2 API routes
                    .route(
                        "/metrics/v2/summary",
                        web::get().to(agent::metrics::v2_unified_summary),
                    )
                    .route(
                        "/metrics/v2/timeline",
                        web::get().to(agent::metrics::v2_unified_timeline),
                    )
                    .route("/health", web::get().to(agent::health::handler))
                    // MCP routes
                    .service(
                        web::scope("/mcp")
                            .route("/servers", web::get().to(agent::mcp::list_servers))
                            .route("/servers", web::post().to(agent::mcp::add_server))
                            .route("/servers/{id}", web::get().to(agent::mcp::get_server))
                            .route("/servers/{id}", web::put().to(agent::mcp::update_server))
                            .route("/servers/{id}", web::delete().to(agent::mcp::delete_server))
                            .route(
                                "/servers/{id}/connect",
                                web::post().to(agent::mcp::connect_server),
                            )
                            .route(
                                "/servers/{id}/disconnect",
                                web::post().to(agent::mcp::disconnect_server),
                            )
                            .route(
                                "/servers/{id}/refresh",
                                web::post().to(agent::mcp::refresh_tools),
                            )
                            .route(
                                "/servers/{id}/tools",
                                web::get().to(agent::mcp::get_server_tools),
                            )
                            .route("/tools", web::get().to(agent::mcp::list_tools)),
                    ),
            )
    })
    .bind(format!("0.0.0.0:{}", port))?
    .run()
    .await
}

/// Start the agent server in a separate thread
/// This is a public API kept for potential external use
#[allow(dead_code)]
pub fn start_server_in_thread(
    port: u16,
    provider: &str,
    llm_base_url: String,
    model: String,
    api_key: String,
    app_data_dir: Option<PathBuf>,
    tauri_mode: bool,
) -> thread::JoinHandle<()> {
    let provider = provider.to_string();
    thread::spawn(move || {
        let system = actix_web::rt::System::new();
        let result = system.block_on(run_server_with_config_and_mode(
            port,
            &provider,
            llm_base_url,
            model,
            api_key,
            app_data_dir,
            tauri_mode,
        ));
        if let Err(err) = result {
            log::error!("Agent server exited with error: {}", err);
        }
    })
}
