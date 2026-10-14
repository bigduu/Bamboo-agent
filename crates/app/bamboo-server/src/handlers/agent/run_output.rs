//! Finite, exact-run visible output over the existing runner-owned buffer.
use crate::app_state::{AgentStatus, AppState};
use actix_web::{http::StatusCode, web, HttpRequest, HttpResponse, ResponseError};

fn error(status: StatusCode, message: &str) -> HttpResponse {
    let mut response = crate::error::json_error(status, message);
    response.headers_mut().insert(
        actix_web::http::header::CACHE_CONTROL,
        actix_web::http::header::HeaderValue::from_static("no-store"),
    );
    response
}

/// Read a replacement snapshot, never Session history or a successor's output.
pub async fn handler(
    state: web::Data<AppState>,
    http: HttpRequest,
    path: web::Path<(String, String)>,
) -> HttpResponse {
    let (id, run_id) = path.into_inner();
    if let Err(reason) =
        crate::handlers::agent::tickets::require_supervisor_owner(&state, &http, &id).await
    {
        return crate::handlers::agent::tickets::TicketHttpError::from(reason).error_response();
    }
    let expected = if http.headers().get_all("Bamboo-Session-Created-At").count() == 1 {
        http.headers()
            .get("Bamboo-Session-Created-At")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc))
    } else {
        None
    };
    let Some(expected) = expected else {
        return error(
            StatusCode::BAD_REQUEST,
            "Exact output requires one Bamboo-Session-Created-At header",
        );
    };
    if run_id.is_empty() || run_id.len() > 128 || !run_id.bytes().all(|b| b.is_ascii_graphic()) {
        return error(StatusCode::BAD_REQUEST, "Invalid run identity");
    }
    let session = match state.storage.load_session(&id).await {
        Ok(Some(session)) => session,
        Ok(None) => {
            return error(
                StatusCode::GONE,
                "Selected run output is no longer available",
            )
        }
        Err(_) => {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "Root lifetime could not be verified",
            )
        }
    };
    if session.created_at != expected || !crate::skill_runtime::registered_main(&session) {
        return error(
            StatusCode::CONFLICT,
            "Selected output Root lifetime or kind differs",
        );
    }
    let output = {
        let runners = state.agent_runners.read().await;
        runners
            .get(&id)
            .filter(|r| r.run_id == run_id)
            .and_then(|r| {
                let status = match r.status {
                    AgentStatus::Pending => "pending",
                    AgentStatus::Running => "running",
                    AgentStatus::Completed => "completed",
                    AgentStatus::Cancelled => "cancelled",
                    AgentStatus::Error(_) => "error",
                };
                r.visible_messages
                    .selected_output_snapshot()
                    .map(|output| (status, output))
            })
    };
    let Some((runner_status, output)) = output.filter(|(_, o)| {
        o.identity.session_id == id
            && o.identity.run_id == run_id
            && o.identity.session_created_at == expected
    }) else {
        return error(
            StatusCode::GONE,
            "Selected run output is no longer retained",
        );
    };
    HttpResponse::Ok().insert_header((actix_web::http::header::CACHE_CONTROL, "no-store"))
        .json(serde_json::json!({
            "session_id": id, "run_id": run_id, "session_created_at": expected,
            "input": {"message_id": output.identity.receipt.id.as_str(), "ingress_seq": output.identity.receipt.generation},
            "output_scope": "visible_messages", "runner_status": runner_status, "output": output.output,
        }))
}
