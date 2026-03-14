use actix_web::web;

use super::{complete, get_models, messages};

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.route("/messages", web::post().to(messages))
        .route("/complete", web::post().to(complete))
        .route("/models", web::get().to(get_models));
}
