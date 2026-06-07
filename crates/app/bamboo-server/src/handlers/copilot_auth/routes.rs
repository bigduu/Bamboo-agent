use actix_web::web;

use super::{
    legacy::authenticate_copilot,
    start_complete::{complete_copilot_auth, start_copilot_auth},
    status_logout::{get_copilot_auth_status, logout_copilot},
};

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.route(
        "/bamboo/copilot/auth/start",
        web::post().to(start_copilot_auth),
    )
    .route(
        "/bamboo/copilot/auth/complete",
        web::post().to(complete_copilot_auth),
    )
    .route(
        "/bamboo/copilot/authenticate",
        web::post().to(authenticate_copilot),
    )
    .route(
        "/bamboo/copilot/auth/status",
        web::post().to(get_copilot_auth_status),
    )
    .route("/bamboo/copilot/logout", web::post().to(logout_copilot));
}
