use actix_web::{web, HttpResponse};

use crate::server::error::AppError;
use crate::server::prompt_defaults::{
    resolve_global_default_system_prompt_template, GlobalSystemPromptSource,
};

use super::super::fs::write_file;
use super::super::types::SaveSystemPromptRequest;

/// Gets the custom system prompt.
pub async fn get_system_prompt() -> Result<HttpResponse, AppError> {
    let default_prompt_path = crate::core::paths::bamboo_dir().join("system-prompt.md");
    let resolved = resolve_global_default_system_prompt_template();
    let source = match resolved.source {
        GlobalSystemPromptSource::BambooFile => "bamboo_file",
        GlobalSystemPromptSource::ClaudeLegacyFile => "claude_legacy_file",
        GlobalSystemPromptSource::BuiltinDefault => "builtin_default",
    };
    let prompt_path = resolved.path.unwrap_or(default_prompt_path);

    // Keep backward-compatible fields while clarifying semantics:
    // this endpoint now represents the global template for new sessions.
    Ok(HttpResponse::Ok().json(serde_json::json!({
        "content": resolved.content,
        "path": prompt_path,
        "scope": "global_default_template",
        "source": source
    })))
}

/// Saves the custom system prompt.
pub async fn save_system_prompt(
    req: web::Json<SaveSystemPromptRequest>,
) -> Result<HttpResponse, AppError> {
    let data_dir = crate::core::paths::ensure_bamboo_dir().map_err(|error| {
        AppError::InternalError(anyhow::anyhow!(
            "Could not create bamboo data directory: {}",
            error
        ))
    })?;
    let prompt_path = data_dir.join("system-prompt.md");

    write_file(&prompt_path, req.content.as_bytes(), "system prompt")?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "path": prompt_path
    })))
}
