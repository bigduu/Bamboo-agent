use actix_web::{web, HttpResponse};

use crate::server::error::AppError;

use super::super::fs::{claude_home_file, get_claude_dir, read_text_file, write_file};
use super::super::types::SaveSystemPromptRequest;

/// Gets the custom system prompt.
pub async fn get_system_prompt() -> Result<HttpResponse, AppError> {
    let prompt_path = claude_home_file("system-prompt.md")?;

    if prompt_path.exists() {
        let content = read_text_file(&prompt_path, "system prompt")?;
        Ok(HttpResponse::Ok().json(serde_json::json!({
            "content": content,
            "path": prompt_path
        })))
    } else {
        Ok(HttpResponse::Ok().json(serde_json::json!({
            "content": "",
            "path": prompt_path
        })))
    }
}

/// Saves the custom system prompt.
pub async fn save_system_prompt(
    req: web::Json<SaveSystemPromptRequest>,
) -> Result<HttpResponse, AppError> {
    let prompt_path = get_claude_dir()?.join("system-prompt.md");

    write_file(&prompt_path, req.content.as_bytes(), "system prompt")?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "path": prompt_path
    })))
}
