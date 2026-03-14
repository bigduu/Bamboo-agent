use actix_web::web;

use super::browsing::{browse_folder, list_workspace_files};
use super::recent::{
    add_recent_workspace, get_recent_workspaces, get_workspace_suggestions, validate_workspace,
};

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.route("/workspace/validate", web::post().to(validate_workspace))
        .route("/workspace/recent", web::get().to(get_recent_workspaces))
        .route("/workspace/recent", web::post().to(add_recent_workspace))
        .route(
            "/workspace/suggestions",
            web::get().to(get_workspace_suggestions),
        )
        .route("/workspace/browse-folder", web::post().to(browse_folder))
        .route("/workspace/files", web::post().to(list_workspace_files));
}
