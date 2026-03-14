use actix_web::web;

use super::{chat_completions, get_models, responses_create};

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.route("/models", web::get().to(get_models))
        .route("/chat/completions", web::post().to(chat_completions))
        .route("/responses", web::post().to(responses_create));
}
