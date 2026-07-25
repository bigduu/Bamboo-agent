use actix_web::{web, HttpResponse};
use bamboo_subagent::codex_discovery::{discover_codex_app_server, discover_codex_cli};
use serde::Deserialize;

use crate::error::AppError;

#[derive(Debug, Deserialize)]
pub struct DetectCodexRequest {
    #[serde(default)]
    binary: Option<String>,
    #[serde(default)]
    mode: Option<String>,
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
    let discovery = match payload.mode.as_deref().unwrap_or("exec") {
        "exec" => discover_codex_cli(binary).await,
        "app_server" => discover_codex_app_server(binary).await,
        other => Err(format!(
            "unknown Codex mode '{other}'; expected exec or app_server"
        )),
    }
    .map_err(AppError::BadRequest)?;
    Ok(HttpResponse::Ok().json(discovery))
}

#[cfg(all(test, unix))]
mod tests {
    use actix_web::{body::to_bytes, http::StatusCode};

    use super::*;

    fn codex_stub_fixture() -> String {
        // During a parallel Linux suite, a concurrently spawned child can briefly inherit an
        // O_CLOEXEC writer until its own exec. Never opening this tracked inode for writing
        // prevents that transient descriptor from making the fixture text-busy.
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/handlers/settings/bamboo_config/fixtures/codex-stub")
            .to_string_lossy()
            .into_owned()
    }

    #[actix_web::test]
    async fn detect_returns_the_shared_preflight_path_and_version() {
        let binary = codex_stub_fixture();
        let response = detect_codex_cli(web::Json(DetectCodexRequest {
            binary: Some(binary.clone()),
            mode: None,
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
            mode: Some("app_server".to_string()),
        }))
        .await
        .unwrap_err();
        assert!(error.to_string().contains("npm i -g @openai/codex"));
        assert!(error.to_string().contains("codex_binary"));
    }

    #[actix_web::test]
    async fn app_server_detection_uses_the_extra_capability_gate() {
        let binary = codex_stub_fixture();
        let response = detect_codex_cli(web::Json(DetectCodexRequest {
            binary: Some(binary),
            mode: Some("app_server".to_string()),
        }))
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[actix_web::test]
    async fn tracked_codex_stub_survives_repeated_parallel_detection() {
        const STRESS_ITERATIONS: usize = 32;

        for iteration in 0..STRESS_ITERATIONS {
            let binary = codex_stub_fixture();
            let exec_detection = detect_codex_cli(web::Json(DetectCodexRequest {
                binary: Some(binary.clone()),
                mode: None,
            }));
            let app_server_detection = detect_codex_cli(web::Json(DetectCodexRequest {
                binary: Some(binary),
                mode: Some("app_server".to_string()),
            }));

            let (exec_response, app_server_response) =
                tokio::join!(exec_detection, app_server_detection);
            for (mode, response) in [("exec", exec_response), ("app_server", app_server_response)] {
                let response = response.unwrap_or_else(|error| {
                    panic!("{mode} detection failed during stress iteration {iteration}: {error}")
                });
                assert_eq!(response.status(), StatusCode::OK);
            }
        }
    }
}
