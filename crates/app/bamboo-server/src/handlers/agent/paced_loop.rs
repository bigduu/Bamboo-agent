//! Human control of a fixed-interval loop in an existing Session.

use actix_web::{http::StatusCode, web, HttpRequest, HttpResponse, ResponseError};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::app_state::{
    mutation_idempotency,
    paced_loop::{PacedLoopCommand, PacedLoopError, PacedLoopRecord},
    AppState,
};
use crate::error::json_error;

use super::chat::ChatRequest;
use super::tickets::{require_supervisor_owner, TicketHttpError};

const CHAT_USAGE: &str = "Use /loop <interval> <prompt> (for example, /loop 5m check the build), /loop status, /loop stop, or /loop resume";

#[derive(Deserialize, Serialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum LoopMutationRequest {
    Start {
        interval_seconds: u64,
        prompt: String,
    },
    Stop {},
    Resume {},
}

impl From<LoopMutationRequest> for PacedLoopCommand {
    fn from(request: LoopMutationRequest) -> Self {
        match request {
            LoopMutationRequest::Start {
                interval_seconds,
                prompt,
            } => Self::Start {
                interval_seconds,
                prompt,
            },
            LoopMutationRequest::Stop {} => Self::Stop,
            LoopMutationRequest::Resume {} => Self::Resume,
        }
    }
}

#[derive(Serialize)]
struct LoopResponse<'a> {
    session_id: &'a str,
    #[serde(rename = "loop")]
    record: Option<PacedLoopRecord>,
}

pub(crate) async fn status(
    state: web::Data<AppState>,
    request: HttpRequest,
    path: web::Path<String>,
) -> HttpResponse {
    let session_id = path.into_inner();
    if let Err(error) = require_supervisor_owner(&state, &request, &session_id).await {
        return TicketHttpError::from(error).error_response();
    }
    apply(&state, &session_id, PacedLoopCommand::Status).await
}

pub(crate) async fn mutate(
    state: web::Data<AppState>,
    request: HttpRequest,
    path: web::Path<String>,
    body: web::Json<LoopMutationRequest>,
) -> HttpResponse {
    let session_id = path.into_inner();
    if let Err(error) = require_supervisor_owner(&state, &request, &session_id).await {
        return TicketHttpError::from(error).error_response();
    }

    let target = format!("POST /api/v1/sessions/{session_id}/loop");
    let prepared = match mutation_idempotency::prepare(&request, "session-loop", &target, &*body) {
        Ok(prepared) => prepared,
        Err(response) => return *response,
    };
    let operation = || apply(&state, &session_id, body.into_inner().into());
    match prepared {
        Some(prepared) => {
            state
                .mutation_idempotency
                .execute(prepared, operation)
                .await
        }
        None => operation().await,
    }
}

async fn apply(state: &AppState, session_id: &str, command: PacedLoopCommand) -> HttpResponse {
    match state.apply_paced_loop(session_id, command).await {
        Ok(record) => HttpResponse::Ok()
            .insert_header(("Cache-Control", "no-store"))
            .json(LoopResponse { session_id, record }),
        Err(error) => error_response(error),
    }
}

fn error_response(error: PacedLoopError) -> HttpResponse {
    match error {
        PacedLoopError::Invalid(message) => json_error(StatusCode::BAD_REQUEST, message),
        PacedLoopError::Conflict(message) => json_error(StatusCode::CONFLICT, message),
        PacedLoopError::NotFound => json_error(StatusCode::NOT_FOUND, "Session not found"),
        PacedLoopError::Internal(error) => {
            tracing::error!(%error, "failed to apply session loop command");
            json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to manage the Session loop",
            )
        }
    }
}

/// Recognize only a standalone slash-command name, with an explicit interval
/// and prompt for starts. Prefix matches stay on the ordinary chat path.
pub(crate) fn parse_chat_command(message: &str) -> Option<Result<PacedLoopCommand, &'static str>> {
    let message = message.trim();
    let name_end = message.find(char::is_whitespace).unwrap_or(message.len());
    if !message[..name_end].eq_ignore_ascii_case("/loop") {
        return None;
    }

    let args = message[name_end..].trim_start();
    let arg_end = args.find(char::is_whitespace).unwrap_or(args.len());
    let (action, prompt) = args.split_at(arg_end);
    let prompt = prompt.trim_start();
    if prompt.is_empty() {
        return Some(match action.to_ascii_lowercase().as_str() {
            "status" => Ok(PacedLoopCommand::Status),
            "stop" => Ok(PacedLoopCommand::Stop),
            "resume" => Ok(PacedLoopCommand::Resume),
            _ => Err(CHAT_USAGE),
        });
    }

    Some(
        parse_interval_seconds(action)
            .map(|interval_seconds| PacedLoopCommand::Start {
                interval_seconds,
                prompt: prompt.into(),
            })
            .ok_or(CHAT_USAGE),
    )
}

fn parse_interval_seconds(token: &str) -> Option<u64> {
    let (digits, multiplier) = match token.as_bytes().last()? {
        b's' => (token.strip_suffix('s')?, 1),
        b'm' => (token.strip_suffix('m')?, 60),
        b'h' => (token.strip_suffix('h')?, 60 * 60),
        b'd' => (token.strip_suffix('d')?, 24 * 60 * 60),
        _ => return None,
    };
    if digits.is_empty() || !digits.bytes().all(|digit| digit.is_ascii_digit()) {
        return None;
    }
    let interval = digits.parse::<u64>().ok()?.checked_mul(multiplier)?;
    (interval > 0).then_some(interval)
}

pub(crate) async fn handle_chat_command(
    state: &AppState,
    request: &ChatRequest,
    command: Result<PacedLoopCommand, &'static str>,
) -> HttpResponse {
    let Some(session_id) = request
        .session_id
        .as_deref()
        .filter(|id| !id.trim().is_empty())
    else {
        return json_error(StatusCode::BAD_REQUEST, "Open a Session before using /loop");
    };
    if request
        .images
        .as_ref()
        .is_some_and(|images| !images.is_empty())
        || request
            .selected_skill_ids
            .as_ref()
            .is_some_and(|skills| !skills.is_empty())
        || request.workflow_selection.is_some()
    {
        return json_error(
            StatusCode::BAD_REQUEST,
            "Send /loop without images, skills, or workflow selections",
        );
    }
    let command = match command {
        Ok(command) => command,
        Err(message) => return json_error(StatusCode::BAD_REQUEST, message),
    };
    let action = match &command {
        PacedLoopCommand::Status => "status",
        PacedLoopCommand::Start { .. } => "start",
        PacedLoopCommand::Stop => "stop",
        PacedLoopCommand::Resume => "resume",
    };

    match state.apply_paced_loop(session_id, command).await {
        Ok(record) => HttpResponse::Ok()
            .insert_header(("Cache-Control", "no-store"))
            .json(json!({
                "session_id": session_id,
                "stream_url": format!("/api/v1/events/{session_id}"),
                "status": "accepted",
                "loop_command": {
                    "action": action,
                    "should_execute": false,
                    "loop": record,
                },
            })),
        Err(error) => error_response(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chat_recognizes_only_the_exact_command_name() {
        for text in ["", "check /loop", "/loops 5m check", "/looping", "/loop/"] {
            assert!(parse_chat_command(text).is_none(), "{text}");
        }
        assert!(matches!(
            parse_chat_command("  /LOOP STATUS\n"),
            Some(Ok(PacedLoopCommand::Status))
        ));
        assert!(matches!(
            parse_chat_command("/loop stop"),
            Some(Ok(PacedLoopCommand::Stop))
        ));
        assert!(matches!(
            parse_chat_command("/loop resume"),
            Some(Ok(PacedLoopCommand::Resume))
        ));
    }

    #[test]
    fn chat_parses_an_interval_and_preserves_prompt_contents() {
        let Some(Ok(PacedLoopCommand::Start {
            interval_seconds,
            prompt,
        })) = parse_chat_command("  /loop 5m\t检查部署\nKeep  spacing  ")
        else {
            panic!("expected a fixed-interval start");
        };
        assert_eq!(interval_seconds, 300);
        assert_eq!(prompt, "检查部署\nKeep  spacing");
        assert_eq!(parse_interval_seconds("90s"), Some(90));
        assert_eq!(parse_interval_seconds("2h"), Some(7_200));
        assert_eq!(parse_interval_seconds("1d"), Some(86_400));
    }

    #[test]
    fn chat_rejects_missing_malformed_and_overflowing_arguments() {
        for text in [
            "/loop",
            "/loop check the build",
            "/loop 5m",
            "/loop 0m check",
            "/loop +5m check",
            "/loop -5m check",
            "/loop 1.5h check",
            "/loop ５ｍ check",
            "/loop 18446744073709551615m check",
            "/loop stop extra",
        ] {
            assert!(matches!(parse_chat_command(text), Some(Err(_))), "{text}");
        }
    }

    #[test]
    fn post_mutation_rejects_extra_fields_and_unrecognized_actions() {
        for payload in [
            json!({"action": "status"}),
            json!({"action": "stop", "interval_seconds": 300}),
            json!({"action": "resume", "prompt": "unused"}),
            json!({"action": "start", "interval_seconds": 300}),
            json!({"action": "start", "interval_seconds": 300, "prompt": "check", "session_id": "other"}),
        ] {
            assert!(serde_json::from_value::<LoopMutationRequest>(payload).is_err());
        }
    }

    #[actix_web::test]
    async fn chat_and_http_manage_the_same_loop_without_sending_a_turn() {
        use actix_web::{test, App};
        use bamboo_agent_core::Session;
        use serde_json::Value;

        let home = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(home.path().into()).await.unwrap());
        let session_id = "loop-http-session";
        state
            .storage
            .save_session(&Session::new(session_id, "test-model"))
            .await
            .unwrap();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(crate::routes::agent_routes),
        )
        .await;
        let loop_api = format!("/api/v1/sessions/{session_id}/loop");

        for (payload, expected_status) in [
            (json!({"message":"/loop 5m check"}), StatusCode::BAD_REQUEST),
            (
                json!({"session_id":"missing-loop-session","message":"/loop 5m check"}),
                StatusCode::NOT_FOUND,
            ),
            (
                json!({"session_id":session_id,"message":"/loop 5m check","selected_skill_ids":["some-skill"]}),
                StatusCode::BAD_REQUEST,
            ),
        ] {
            let request = test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(payload)
                .to_request();
            let response = test::call_service(&app, request).await;
            assert_eq!(response.status(), expected_status);
        }
        assert!(state
            .storage
            .load_session("missing-loop-session")
            .await
            .unwrap()
            .is_none());

        let start = test::TestRequest::post()
            .uri("/api/v1/chat")
            .set_json(json!({"session_id":session_id,"message":"/loop 5m check the build"}))
            .to_request();
        let start: Value = test::call_and_read_body_json(&app, start).await;
        assert_eq!(start["loop_command"]["action"], "start");
        assert_eq!(start["loop_command"]["should_execute"], false);
        assert!(start.get("message_id").is_none());
        assert!(start["loop_command"]["loop"].is_object());

        let mut runner = bamboo_engine::AgentRunner::new();
        runner.status = crate::app_state::AgentStatus::Running;
        let cancel = runner.cancel_token.clone();
        state
            .agent_runners
            .write()
            .await
            .insert(session_id.into(), runner);
        let status = test::TestRequest::get().uri(&loop_api).to_request();
        let status: Value = test::call_and_read_body_json(&app, status).await;
        assert_eq!(status["session_id"], session_id);
        assert_eq!(status["loop"], start["loop_command"]["loop"]);

        let stop = test::TestRequest::post()
            .uri("/api/v1/chat")
            .set_json(json!({"session_id":session_id,"message":"/loop stop"}))
            .to_request();
        let stop: Value = test::call_and_read_body_json(&app, stop).await;
        assert_eq!(stop["loop_command"]["action"], "stop");
        assert_eq!(stop["loop_command"]["should_execute"], false);
        assert!(!cancel.is_cancelled());

        let repeated_stop = test::TestRequest::post()
            .uri(&loop_api)
            .set_json(json!({"action": "stop"}))
            .to_request();
        let repeated_stop: Value = test::call_and_read_body_json(&app, repeated_stop).await;
        assert_eq!(repeated_stop["loop"], stop["loop_command"]["loop"]);
        let durable = state
            .storage
            .load_session(session_id)
            .await
            .unwrap()
            .unwrap();
        assert!(durable.messages.is_empty());
        assert_eq!(
            state
                .session_inbox
                .inspect(session_id)
                .await
                .unwrap()
                .pending,
            0
        );
    }
}
