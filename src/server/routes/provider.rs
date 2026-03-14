use actix_web::web;

use crate::server::handlers::{anthropic, gemini, openai};

/// Configure OpenAI-compatible API routes with an explicit prefix (/openai/v1/*)
///
/// This mirrors the provider-specific prefixes used by Anthropic (/anthropic/v1/*)
/// and Gemini (/gemini/v1beta/*), making it easier to configure OpenAI clients with a base URL
/// like `http://localhost:9562/openai`.
pub fn openai_prefixed_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/openai/v1")
            .route(
                "/chat/completions",
                web::post().to(openai::chat_completions),
            )
            .route("/responses", web::post().to(openai::responses_create))
            .route("/models", web::get().to(openai::get_models)),
    );
}

/// Configure Anthropic API routes (/anthropic/v1/*)
pub fn anthropic_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/anthropic/v1")
            .route("/messages", web::post().to(anthropic::messages))
            .route("/complete", web::post().to(anthropic::complete))
            .route("/models", web::get().to(anthropic::get_models)),
    );
}

/// Configure Gemini API routes (/gemini/v1beta/*)
pub fn gemini_routes(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/gemini/v1beta")
            .route("/models", web::get().to(gemini::list_models))
            .route(
                "/models/{model}:generateContent",
                web::post().to(gemini::generate_content),
            )
            .route(
                "/models/{model}:streamGenerateContent",
                web::post().to(gemini::stream_generate_content),
            ),
    );
}
