use actix_web::web;

use super::{generate_content, list_models, stream_generate_content};

/// Configure Gemini API routes.
pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.route("/models", web::get().to(list_models))
        .route(
            "/models/{model}:generateContent",
            web::post().to(generate_content),
        )
        .route(
            "/models/{model}:streamGenerateContent",
            web::post().to(stream_generate_content),
        );
}
