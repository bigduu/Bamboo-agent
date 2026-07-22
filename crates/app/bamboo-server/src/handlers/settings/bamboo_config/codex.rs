use actix_web::{web, HttpResponse};
use bamboo_subagent::codex_discovery::discover_codex_cli;
use serde::Deserialize;

use crate::error::AppError;

#[derive(Debug, Deserialize)]
pub struct DetectCodexRequest {
    #[serde(default)]
    binary: Option<String>,
}

/// Resolve and capability-check the configured Codex CLI without persisting
/// settings. The worker executor uses the same shared discovery function.
///
/// # HTTP Route
/// `POST /bamboo/config/codex/detect`
pub async fn detect_codex_cli(
    payload: web::Json<DetectCodexRequest>,
) -> Result<HttpResponse, AppError> {
    let binary = payload
        .binary
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let discovery = discover_codex_cli(binary)
        .await
        .map_err(AppError::BadRequest)?;
    Ok(HttpResponse::Ok().json(discovery))
}

#[cfg(all(test, unix))]
mod tests {
    use std::io::Write as _;
    use std::os::unix::fs::PermissionsExt as _;

    use actix_web::{body::to_bytes, http::StatusCode};

    use super::*;

    fn write_codex_stub() -> (tempfile::TempDir, String) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("codex");
        let mut file = std::fs::File::create(&path).unwrap();
        writeln!(file, "#!/bin/sh").unwrap();
        writeln!(
            file,
            r#"case "$*" in
  "--version") echo 'codex-cli 0.144.5' ;;
  "exec --help") echo '--json --output-last-message --config --sandbox --dangerously-bypass-approvals-and-sandbox prompt from stdin' ;;
  "exec resume --help") echo '--json' ;;
  *) exit 2 ;;
esac"#
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&path, permissions).unwrap();
        (directory, path.to_string_lossy().into_owned())
    }

    #[actix_web::test]
    async fn detect_returns_the_shared_preflight_path_and_version() {
        let (_directory, binary) = write_codex_stub();
        let response = detect_codex_cli(web::Json(DetectCodexRequest {
            binary: Some(binary.clone()),
        }))
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body()).await.unwrap()).unwrap();
        assert_eq!(body["path"], binary);
        assert_eq!(body["version"], "codex-cli 0.144.5");
    }

    #[actix_web::test]
    async fn detect_rejects_a_missing_override_with_install_guidance() {
        let error = detect_codex_cli(web::Json(DetectCodexRequest {
            binary: Some("/definitely/missing/codex".to_string()),
        }))
        .await
        .unwrap_err();
        assert!(error.to_string().contains("npm i -g @openai/codex"));
        assert!(error.to_string().contains("codex_binary"));
    }
}
