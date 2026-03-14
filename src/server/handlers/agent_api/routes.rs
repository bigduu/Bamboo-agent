use actix_web::web;

use super::{
    cancel_claude_execution, create_project, execute_claude_code, get_claude_settings,
    get_project_sessions, get_session_jsonl, get_system_prompt, list_projects,
    list_running_claude_sessions, save_claude_settings, save_system_prompt,
};

/// Configures agent API routes.
pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/agent")
            .route("/projects", web::get().to(list_projects))
            .route("/projects", web::post().to(create_project))
            .route(
                "/projects/{project_id}/sessions",
                web::get().to(get_project_sessions),
            )
            .route("/settings", web::get().to(get_claude_settings))
            .route("/settings", web::post().to(save_claude_settings))
            .route("/system-prompt", web::get().to(get_system_prompt))
            .route("/system-prompt", web::post().to(save_system_prompt))
            .route(
                "/sessions/running",
                web::get().to(list_running_claude_sessions),
            )
            .route("/sessions/execute", web::post().to(execute_claude_code))
            .route("/sessions/cancel", web::post().to(cancel_claude_execution))
            .route(
                "/sessions/{session_id}/jsonl",
                web::get().to(get_session_jsonl),
            ),
    );
}
