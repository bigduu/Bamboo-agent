use actix_web::{web, HttpResponse};

use crate::server::error::AppError;

use super::super::fs::{
    claude_home_file, get_claude_dir, parse_json, read_text_file, serialize_json_pretty, write_file,
};
use super::super::types::{ClaudeSettings, SaveSettingsRequest};

fn parse_settings(content: &str) -> Result<serde_json::Value, AppError> {
    parse_json(content, "settings")
}

/// Gets Claude Code settings.
pub async fn get_claude_settings() -> Result<HttpResponse, AppError> {
    let settings_path = claude_home_file("settings.json")?;

    if settings_path.exists() {
        let content = read_text_file(&settings_path, "settings")?;
        let data = parse_settings(&content)?;
        Ok(HttpResponse::Ok().json(ClaudeSettings { data }))
    } else {
        Ok(HttpResponse::Ok().json(ClaudeSettings::default()))
    }
}

/// Saves Claude Code settings.
pub async fn save_claude_settings(
    req: web::Json<SaveSettingsRequest>,
) -> Result<HttpResponse, AppError> {
    let settings_path = get_claude_dir()?.join("settings.json");

    let content = serialize_json_pretty(&req.settings, "settings")?;
    write_file(&settings_path, content, "settings")?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "path": settings_path
    })))
}

#[cfg(test)]
mod tests {
    use super::parse_settings;

    #[test]
    fn parse_settings_accepts_valid_json() {
        let parsed = parse_settings(r#"{"model":"claude"}"#).expect("valid json");
        assert_eq!(parsed["model"], "claude");
    }

    #[test]
    fn parse_settings_rejects_invalid_json() {
        let result = parse_settings("{not-json");
        assert!(result.is_err());
    }
}
