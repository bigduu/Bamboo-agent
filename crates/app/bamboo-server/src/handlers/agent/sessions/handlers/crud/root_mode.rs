//! Recoverable, message-free Root tool-mode operations.

use actix_web::{web, HttpResponse};
use bamboo_domain::{
    RootModeOperationAction, RootModeOperationDecision, RootModeOperationOutcome,
    RootModeOperationRequest, RootThinkingMode,
};
use serde::Deserialize;

use crate::app_state::AppState;

#[derive(Deserialize)]
pub struct RootModeOperationBody {
    birth_token: String,
    expected_epoch: u64,
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default, deserialize_with = "RootThinkingMode::deserialize_selection")]
    thinking_mode: Option<RootThinkingMode>,
}

fn error(status: actix_web::http::StatusCode, code: &str, message: &str) -> HttpResponse {
    HttpResponse::build(status)
        .insert_header((actix_web::http::header::CACHE_CONTROL, "no-store"))
        .json(serde_json::json!({
            "error": { "type": "api_error", "code": code, "message": message }
        }))
}

fn operation_response(decision: RootModeOperationDecision, recovering: bool) -> HttpResponse {
    use actix_web::http::StatusCode;
    match decision {
        RootModeOperationDecision::Terminal(receipt) => {
            let status = match receipt.outcome {
                RootModeOperationOutcome::Committed => "committed",
                RootModeOperationOutcome::Fenced => "fenced",
                RootModeOperationOutcome::RejectedIncompatible => "rejected_incompatible",
            };
            if !recovering && receipt.outcome != RootModeOperationOutcome::Committed {
                let (code, message) = match receipt.outcome {
                    RootModeOperationOutcome::Fenced => (
                        "root_mode_operation_fenced",
                        "Root mode operation was durably fenced by recovery",
                    ),
                    RootModeOperationOutcome::RejectedIncompatible => (
                        "root_orchestration_incompatible_mode",
                        "Root mode is incompatible with the current Plan, Skill, or Workflow",
                    ),
                    RootModeOperationOutcome::Committed => unreachable!(),
                };
                return error(StatusCode::CONFLICT, code, message);
            }
            HttpResponse::Ok()
                .insert_header((actix_web::http::header::CACHE_CONTROL, "no-store"))
                .json(serde_json::json!({
                    "status": status,
                    "operation_id": receipt.operation_id,
                    "expected_epoch": receipt.expected_epoch,
                    "resulting_epoch": receipt.resulting_epoch,
                    "enabled_at_completion": receipt.enabled_at_completion,
                    "thinking_mode_at_completion": RootThinkingMode::from_enabled(receipt.enabled_at_completion),
                    "root_tool_authority_revision": receipt.tool_authority_revision,
                }))
        }
        RootModeOperationDecision::FencedBySuccessor {
            operation_id,
            expected_epoch,
            current_epoch,
            current_enabled,
            current_tool_revision,
        } if recovering => HttpResponse::Ok()
            .insert_header((actix_web::http::header::CACHE_CONTROL, "no-store"))
            .json(serde_json::json!({
                "status": "fenced_by_successor",
                "operation_id": operation_id,
                "expected_epoch": expected_epoch,
                "current_epoch": current_epoch,
                "current_enabled": current_enabled,
                "current_thinking_mode": RootThinkingMode::from_enabled(current_enabled),
                "root_tool_authority_revision": current_tool_revision,
            })),
        RootModeOperationDecision::FencedBySuccessor { .. } => error(
            StatusCode::PRECONDITION_FAILED,
            "root_mode_precondition_failed",
            "Root mode authority changed; read session detail before a new operation",
        ),
        RootModeOperationDecision::NotFound => error(
            StatusCode::NOT_FOUND,
            "session_not_found",
            "Session not found",
        ),
        RootModeOperationDecision::NotRoot => error(
            StatusCode::BAD_REQUEST,
            "root_orchestration_requires_root",
            "Only a Root Session may change Root mode",
        ),
        RootModeOperationDecision::BirthMismatch => error(
            StatusCode::PRECONDITION_FAILED,
            "root_mode_birth_mismatch",
            "Session lifetime changed; read session detail before a new operation",
        ),
        RootModeOperationDecision::OperationConflict => error(
            StatusCode::CONFLICT,
            "root_mode_operation_conflict",
            "Operation ID was already used with a different Root mode request",
        ),
        RootModeOperationDecision::FutureEpoch => error(
            StatusCode::PRECONDITION_FAILED,
            "root_mode_precondition_failed",
            "Expected Root mode epoch is ahead of durable authority",
        ),
    }
}

fn operation_io_error_response(io_error: std::io::Error) -> HttpResponse {
    use actix_web::http::StatusCode;

    match io_error.kind() {
        std::io::ErrorKind::Unsupported => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "root_mode_authority_unavailable",
            "This storage backend does not support recoverable Root mode operations",
        ),
        std::io::ErrorKind::InvalidInput => error(
            StatusCode::BAD_REQUEST,
            "invalid_root_mode_operation",
            "Root mode operation request is invalid",
        ),
        _ => {
            tracing::error!(%io_error, "Root mode operation failed at durable authority");
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "root_mode_outcome_unconfirmed",
                "Root mode outcome could not be verified; keep this operation ID and retry recovery",
            )
        }
    }
}

async fn run(
    state: web::Data<AppState>,
    path: web::Path<(String, String)>,
    body: web::Json<RootModeOperationBody>,
    action: RootModeOperationAction,
) -> HttpResponse {
    use actix_web::http::StatusCode;
    let requested_enabled =
        match RootThinkingMode::resolve_selection(body.thinking_mode, body.enabled) {
            Ok(Some(enabled)) => enabled,
            Ok(None) => {
                return error(
                    StatusCode::BAD_REQUEST,
                    "invalid_root_mode_operation",
                    "Provide thinking_mode or enabled",
                )
            }
            Err(_) => {
                return error(
                    StatusCode::BAD_REQUEST,
                    "root_thinking_mode_conflict",
                    "thinking_mode and enabled conflict",
                )
            }
        };
    let (session_id, operation_id) = path.into_inner();
    if !bamboo_domain::root_mode_operation_id_matches_epoch(&operation_id, body.expected_epoch)
        || body.birth_token.len() != 64
        || !body
            .birth_token
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_root_mode_operation",
            "Provide <expected_epoch>:<UUID> as operation ID and the Root birth token from session detail",
        );
    }
    let request = RootModeOperationRequest {
        session_id: session_id.clone(),
        operation_id,
        birth_token: body.birth_token.clone(),
        expected_epoch: body.expected_epoch,
        requested_enabled,
        action,
    };
    // The owned task survives an HTTP timeout. The V2 writer lock, proof and
    // session save determine the terminal result; dropping this waiter does not
    // cancel a write between proof Prepared and Committed.
    let owner = tokio::spawn(async move {
        let result = state.storage.root_mode_operation(&request).await;
        if matches!(
            &result,
            Ok(RootModeOperationDecision::Terminal(_))
                | Ok(RootModeOperationDecision::FencedBySuccessor { .. })
        ) {
            // Do not wait on the process-local chat persistence lock after the
            // durable terminal decision. A slow chat may own that lock while
            // recovery must still return. Chat re-reads durable authority at
            // admission, and evicting this cache entry makes later cache-only
            // reads reload instead of observing a pre-transition snapshot.
            state.sessions.remove(&session_id);
        }
        result
    });
    match owner.await {
        Ok(Ok(decision)) => {
            operation_response(decision, action == RootModeOperationAction::Recover)
        }
        Ok(Err(io_error)) => operation_io_error_response(io_error),
        Err(join_error) => {
            tracing::error!(%join_error, "Root mode operation owner failed");
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "root_mode_operation_failed",
                "Root mode operation failed; recover its terminal status before retrying",
            )
        }
    }
}

pub async fn select_root_mode(
    state: web::Data<AppState>,
    path: web::Path<(String, String)>,
    body: web::Json<RootModeOperationBody>,
) -> HttpResponse {
    run(state, path, body, RootModeOperationAction::Select).await
}

pub async fn recover_root_mode(
    state: web::Data<AppState>,
    path: web::Path<(String, String)>,
    body: web::Json<RootModeOperationBody>,
) -> HttpResponse {
    run(state, path, body, RootModeOperationAction::Recover).await
}

#[cfg(test)]
mod tests {
    use actix_web::{http::StatusCode, test, web, App};
    use bamboo_agent_core::Session;
    use serde_json::Value;

    use crate::routes::configure_routes;

    use super::*;

    #[actix_web::test]
    async fn ultra_mode_reuses_terminal_proof_and_exposes_its_temporal_meaning() {
        let home = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(home.path().to_path_buf()).await.unwrap());
        let mut root = Session::new("ultra-mode-http", "test-model");
        root.reasoning_effort = Some(bamboo_domain::ReasoningEffort::Max);
        state.save_and_cache_session(&mut root).await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let base = format!("/api/v1/sessions/{}", root.id);
        let operation_id = format!("0:{}", uuid::Uuid::new_v4());
        let operation = format!("{base}/root-mode-operations/{operation_id}");
        let body = serde_json::json!({"birth_token": root.root_mode_birth_token(), "expected_epoch": 0, "thinking_mode": "ultra"});
        let selected: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::post()
                .uri(&operation)
                .set_json(&body)
                .to_request(),
        )
        .await;
        assert_eq!(selected["status"], "committed");
        assert_eq!(selected["thinking_mode_at_completion"], "ultra");
        assert!(selected.get("thinking_mode").is_none());
        let restarted = web::Data::new(AppState::new(home.path().to_path_buf()).await.unwrap());
        let app = test::init_service(
            App::new()
                .app_data(restarted.clone())
                .configure(configure_routes),
        )
        .await;
        let detail: Value =
            test::call_and_read_body_json(&app, test::TestRequest::get().uri(&base).to_request())
                .await;
        assert_eq!(detail["session"]["thinking_mode"], "ultra");
        assert_eq!(detail["session"]["reasoning_effort"], "max");
        let next = format!("{base}/root-mode-operations/1:{}", uuid::Uuid::new_v4());
        let standard = serde_json::json!({"birth_token": root.root_mode_birth_token(), "expected_epoch": 1, "thinking_mode": "standard", "enabled": false});
        let changed: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::post()
                .uri(&next)
                .set_json(&standard)
                .to_request(),
        )
        .await;
        assert_eq!(changed["thinking_mode_at_completion"], "standard");
        // An old canonical request can recover using its legacy representation.
        let legacy = serde_json::json!({"birth_token": root.root_mode_birth_token(), "expected_epoch": 0, "enabled": true});
        let historical: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::post()
                .uri(&format!("{operation}/recover"))
                .set_json(&legacy)
                .to_request(),
        )
        .await;
        assert_eq!(historical["thinking_mode_at_completion"], "ultra");
        let stale = format!(
            "{base}/root-mode-operations/0:{}/recover",
            uuid::Uuid::new_v4()
        );
        let successor: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::post()
                .uri(&stale)
                .set_json(&body)
                .to_request(),
        )
        .await;
        assert_eq!(successor["status"], "fenced_by_successor");
        assert_eq!(successor["current_thinking_mode"], "standard");
        assert!(successor.get("thinking_mode_at_completion").is_none());
        let durable_before = restarted
            .storage
            .load_session(&root.id)
            .await
            .unwrap()
            .unwrap();
        for selector in [
            serde_json::json!({}),
            serde_json::json!({"thinking_mode": "ultra", "enabled": false}),
            serde_json::json!({"thinking_mode": "max"}),
            serde_json::json!({"thinking_mode": null, "enabled": true}),
        ] {
            let mut invalid = selector;
            invalid["birth_token"] = root.root_mode_birth_token().into();
            invalid["expected_epoch"] = 2.into();
            let response = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri(&format!(
                        "{base}/root-mode-operations/2:{}",
                        uuid::Uuid::new_v4()
                    ))
                    .set_json(&invalid)
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        }
        for mode in [
            serde_json::json!("ultra"),
            serde_json::json!("standard"),
            Value::Null,
            serde_json::json!(7),
        ] {
            let response = test::call_service(
                &app,
                test::TestRequest::patch()
                    .uri(&base)
                    .set_json(serde_json::json!({"thinking_mode": mode, "title": "must not write"}))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let error: Value = test::read_body_json(response).await;
            assert_eq!(error["error"]["code"], "root_mode_operation_required");
        }
        let durable_after = restarted
            .storage
            .load_session(&root.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(durable_after).unwrap(),
            serde_json::to_value(durable_before).unwrap()
        );
        let detail: Value =
            test::call_and_read_body_json(&app, test::TestRequest::get().uri(&base).to_request())
                .await;
        assert_eq!(detail["session"]["thinking_mode"], "standard");
        assert_eq!(detail["session"]["reasoning_effort"], "max");
        let mut child =
            Session::new_child_of("ultra-mode-child-http", &root, "test-model", "child");
        restarted.save_and_cache_session(&mut child).await;
        let response = test::call_service(&app, test::TestRequest::post()
            .uri(&format!("/api/v1/sessions/{}/root-mode-operations/0:{}", child.id, uuid::Uuid::new_v4()))
            .set_json(serde_json::json!({"birth_token": child.root_mode_birth_token(), "expected_epoch":0, "thinking_mode":"ultra"})).to_request()).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let error: Value = test::read_body_json(response).await;
        assert_eq!(error["error"]["code"], "root_orchestration_requires_root");
    }

    #[actix_web::test]
    async fn two_app_states_recover_only_after_cross_process_mode_commit() {
        let home = tempfile::tempdir().unwrap().keep();
        bamboo_config::paths::init_bamboo_dir(home.clone());
        let first = web::Data::new(AppState::new(home.clone()).await.unwrap());
        let mut root = Session::new("mode-http-two-app-states", "test-model");
        first.save_and_cache_session(&mut root).await;
        let second = web::Data::new(AppState::new(home).await.unwrap());
        let first_app = test::init_service(
            App::new()
                .app_data(first.clone())
                .configure(configure_routes),
        )
        .await;
        let second_app = test::init_service(
            App::new()
                .app_data(second.clone())
                .configure(configure_routes),
        )
        .await;
        let (reached, release) = first
            .session_store
            .pause_full_save_before_filesystem_commit_for_test(&root.id);
        let operation_id = format!("0:{}", uuid::Uuid::new_v4());
        let path = format!(
            "/api/v1/sessions/{}/root-mode-operations/{operation_id}",
            root.id
        );
        let body = serde_json::json!({
            "birth_token": root.root_mode_birth_token(),
            "expected_epoch": 0,
            "enabled": true,
        });
        let (selected, recovered) = tokio::join!(
            async {
                test::call_service(
                    &first_app,
                    test::TestRequest::post()
                        .uri(&path)
                        .set_json(&body)
                        .to_request(),
                )
                .await
            },
            async {
                reached.wait().await;
                let recovery = test::call_service(
                    &second_app,
                    test::TestRequest::post()
                        .uri(&format!("{path}/recover"))
                        .set_json(&body)
                        .to_request(),
                );
                tokio::pin!(recovery);
                assert!(
                    tokio::time::timeout(std::time::Duration::from_millis(100), &mut recovery)
                        .await
                        .is_err()
                );
                release.wait().await;
                recovery.await
            }
        );
        assert_eq!(selected.status(), StatusCode::OK);
        assert_eq!(recovered.status(), StatusCode::OK);
        let selected: Value = test::read_body_json(selected).await;
        let recovered: Value = test::read_body_json(recovered).await;
        assert_eq!(selected, recovered);
        assert_eq!(recovered["status"], "committed");
        assert_eq!(recovered["resulting_epoch"], 1);
        let durable = second
            .storage
            .load_session(&root.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(durable.root_mode_transition_epoch, 1);
        assert!(durable.messages.is_empty());
    }

    #[actix_web::test]
    async fn mode_terminal_response_does_not_wait_for_chat_process_lock() {
        let home = tempfile::tempdir().unwrap().keep();
        bamboo_config::paths::init_bamboo_dir(home.clone());
        let state = web::Data::new(AppState::new(home).await.unwrap());
        let mut root = Session::new("mode-http-lock-barrier", "test-model");
        state.save_and_cache_session(&mut root).await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let detail: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get()
                .uri(&format!("/api/v1/sessions/{}", root.id))
                .to_request(),
        )
        .await;
        let birth_token = detail["session"]["root_mode_birth_token"].as_str().unwrap();
        assert_eq!(detail["session"]["root_mode_transition_epoch"], 0);
        let operation_id = format!("0:{}", uuid::Uuid::new_v4());
        let path = format!(
            "/api/v1/sessions/{}/root-mode-operations/{operation_id}",
            root.id
        );
        let body = serde_json::json!({
            "birth_token": birth_token,
            "expected_epoch": 0,
            "enabled": true,
        });

        let _chat_lock = state.persistence.acquire_lock(&root.id).await;
        let selected = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            test::call_service(
                &app,
                test::TestRequest::post()
                    .uri(&path)
                    .set_json(&body)
                    .to_request(),
            ),
        )
        .await
        .expect("durable select response must not wait for chat process lock");
        assert_eq!(selected.status(), StatusCode::OK);
        let selected: Value = test::read_body_json(selected).await;
        assert_eq!(selected["status"], "committed");
        assert_eq!(selected["resulting_epoch"], 1);

        let recovered = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            test::call_service(
                &app,
                test::TestRequest::post()
                    .uri(&format!("{path}/recover"))
                    .set_json(&body)
                    .to_request(),
            ),
        )
        .await
        .expect("durable recovery response must not wait for chat process lock");
        assert_eq!(recovered.status(), StatusCode::OK);
        let recovered: Value = test::read_body_json(recovered).await;
        assert_eq!(recovered["status"], "committed");
        assert_eq!(recovered["resulting_epoch"], 1);

        let old_operation_id = format!("0:{}", uuid::Uuid::new_v4());
        let successor = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!(
                    "/api/v1/sessions/{}/root-mode-operations/{old_operation_id}/recover",
                    root.id
                ))
                .set_json(&body)
                .to_request(),
        )
        .await;
        assert_eq!(successor.status(), StatusCode::OK);
        let successor: Value = test::read_body_json(successor).await;
        assert_eq!(successor["status"], "fenced_by_successor");
        assert_eq!(successor["operation_id"], old_operation_id);
        assert_eq!(successor["expected_epoch"], 0);
        assert_eq!(successor["current_epoch"], 1);

        let durable = state.storage.load_session(&root.id).await.unwrap().unwrap();
        assert!(durable.root_orchestration_only_enabled());
        assert_eq!(durable.messages.len(), 0);
    }

    #[actix_web::test]
    async fn storage_error_after_possible_commit_reports_unconfirmed_outcome() {
        let response = operation_io_error_response(std::io::Error::other("after proof commit"));
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let bytes = actix_web::body::to_bytes(response.into_body())
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["code"], "root_mode_outcome_unconfirmed");
    }
}
