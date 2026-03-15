use std::collections::HashMap;

use actix_web::{web, HttpResponse};

use crate::server::error::AppError;

use super::super::fs::{get_claude_dir, read_text_file};

fn parse_jsonl_lines(content: &str) -> Vec<serde_json::Value> {
    content
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

/// Gets session JSONL content (conversation history).
pub async fn get_session_jsonl(
    path: web::Path<String>,
    query: web::Query<HashMap<String, String>>,
) -> Result<HttpResponse, AppError> {
    let claude_dir = get_claude_dir()?;
    let session_id = path.into_inner();
    let project_id = query.get("project_id").ok_or_else(|| {
        AppError::InternalError(anyhow::anyhow!("project_id query parameter required"))
    })?;

    let project_dir = claude_dir.join(project_id);
    let session_path = project_dir.join(format!("{}.jsonl", session_id));

    if !session_path.exists() {
        return Err(AppError::InternalError(anyhow::anyhow!(
            "Session not found"
        )));
    }

    let content = read_text_file(&session_path, "session")?;

    Ok(HttpResponse::Ok().json(parse_jsonl_lines(&content)))
}

#[cfg(test)]
mod tests {
    use super::parse_jsonl_lines;

    #[test]
    fn parse_jsonl_lines_skips_invalid_records() {
        let content = r#"{"type":"start","ok":true}
not-json
{"type":"end","count":2}"#;

        let records = parse_jsonl_lines(content);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["type"], "start");
        assert_eq!(records[1]["type"], "end");
    }
}
