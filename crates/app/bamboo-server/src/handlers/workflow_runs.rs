//! Safe HTTP facade for durable catalog-pinned workflow runs.

use actix_web::{web, HttpResponse};
use bamboo_engine::WorkflowRunError;
use serde::Deserialize;
use serde_json::Value;

use crate::app_state::AppState;
use crate::workflow::{public_workflow_event, public_workflow_snapshot};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartWorkflowRunRequest {
    pub workflow_id: String,
    pub revision: u64,
    #[serde(default = "empty_object")]
    pub args: Value,
    #[serde(default)]
    pub budget: Option<bamboo_domain::WorkflowBudgets>,
}

fn empty_object() -> Value {
    serde_json::json!({})
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowEventsQuery {
    #[serde(default)]
    pub since: u64,
}

pub async fn start(
    state: web::Data<AppState>,
    session_id: web::Path<String>,
    body: web::Json<StartWorkflowRunRequest>,
) -> HttpResponse {
    match state
        .workflow_runs
        .start(
            &session_id,
            &body.workflow_id,
            body.revision,
            body.args.clone(),
            body.budget.clone(),
        )
        .await
    {
        Ok(snapshot) => HttpResponse::Accepted().json(public_workflow_snapshot(snapshot)),
        Err(error) => workflow_error(error),
    }
}

pub async fn list(state: web::Data<AppState>, session_id: web::Path<String>) -> HttpResponse {
    match state.workflow_runs.list_for_session(&session_id).await {
        Ok(snapshots) => HttpResponse::Ok().json(
            snapshots
                .into_iter()
                .map(public_workflow_snapshot)
                .collect::<Vec<_>>(),
        ),
        Err(error) => workflow_error(error),
    }
}

pub async fn get(state: web::Data<AppState>, path: web::Path<(String, String)>) -> HttpResponse {
    let (session_id, run_id) = path.into_inner();
    match state
        .workflow_runs
        .progress_for_session(&session_id, &run_id, u64::MAX)
        .await
    {
        Ok(progress) => HttpResponse::Ok().json(public_workflow_snapshot(progress.snapshot)),
        Err(error) => workflow_error(error),
    }
}

pub async fn events(
    state: web::Data<AppState>,
    path: web::Path<(String, String)>,
    query: web::Query<WorkflowEventsQuery>,
) -> HttpResponse {
    let (session_id, run_id) = path.into_inner();
    match state
        .workflow_runs
        .progress_for_session(&session_id, &run_id, query.since)
        .await
    {
        Ok(progress) => HttpResponse::Ok().json(
            progress
                .events
                .into_iter()
                .map(public_workflow_event)
                .collect::<Vec<_>>(),
        ),
        Err(error) => workflow_error(error),
    }
}

pub async fn cancel(state: web::Data<AppState>, path: web::Path<(String, String)>) -> HttpResponse {
    let (session_id, run_id) = path.into_inner();
    match state
        .workflow_runs
        .cancel_for_session(&session_id, &run_id)
        .await
    {
        Ok(snapshot) => HttpResponse::Ok().json(public_workflow_snapshot(snapshot)),
        Err(error) => workflow_error(error),
    }
}

pub async fn restart(
    state: web::Data<AppState>,
    path: web::Path<(String, String)>,
) -> HttpResponse {
    let (session_id, run_id) = path.into_inner();
    match state
        .workflow_runs
        .restart_for_session(&session_id, &run_id)
        .await
    {
        Ok(snapshot) => HttpResponse::Accepted().json(public_workflow_snapshot(snapshot)),
        Err(error) => workflow_error(error),
    }
}

fn workflow_error(error: WorkflowRunError) -> HttpResponse {
    match error {
        WorkflowRunError::NotFound => HttpResponse::NotFound().json(serde_json::json!({
            "error": crate::error::error_value("workflow run not found")
        })),
        WorkflowRunError::Terminal => HttpResponse::Conflict().json(serde_json::json!({
            "error": crate::error::error_value("workflow run is already terminal")
        })),
        WorkflowRunError::Storage(details) => {
            tracing::error!(%details, "workflow HTTP storage unavailable");
            let recovery_run_id = recovery_run_id_from_storage_details(&details);
            HttpResponse::InternalServerError().json(match recovery_run_id {
                Some(run_id) => serde_json::json!({
                    "error": crate::error::error_value(
                        "workflow storage unavailable; run recovery is required"
                    ),
                    "recovery_run_id": run_id,
                }),
                None => serde_json::json!({
                    "error": crate::error::error_value("workflow storage unavailable")
                }),
            })
        }
        WorkflowRunError::Compile(_) => HttpResponse::BadRequest().json(serde_json::json!({
            "error": crate::error::error_value("workflow definition is invalid")
        })),
        WorkflowRunError::InvalidInput(_) => HttpResponse::BadRequest().json(serde_json::json!({
            "error": crate::error::error_value("workflow input is invalid")
        })),
        WorkflowRunError::Preflight(_) => HttpResponse::BadRequest().json(serde_json::json!({
            "error": crate::error::error_value("workflow preflight failed")
        })),
    }
}

fn recovery_run_id_from_storage_details(details: &str) -> Option<&str> {
    details
        .split("orphan run ")
        .nth(1)
        .and_then(|tail| tail.split_whitespace().next())
        .filter(|value| uuid::Uuid::parse_str(value).is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_request_rejects_spoofed_trust_and_capabilities() {
        for field in ["workspace_trusted", "allowed_capabilities"] {
            let mut value = serde_json::json!({
                "workflow_id": "safe",
                "revision": 1
            });
            value[field] = serde_json::json!(true);
            assert!(serde_json::from_value::<StartWorkflowRunRequest>(value).is_err());
        }
        assert!(
            serde_json::from_value::<StartWorkflowRunRequest>(serde_json::json!({
                "workflow_id": "safe",
                "revision": 1,
                "session_id": "caller-controlled"
            }))
            .is_err()
        );
    }

    #[actix_web::test]
    async fn orphan_storage_error_returns_safe_recovery_handle() {
        let response = workflow_error(WorkflowRunError::Storage(
            "run index persistence failed; orphan run 123e4567-e89b-12d3-a456-426614174000 could not be cancelled (failure)"
                .to_string(),
        ));
        assert_eq!(
            response.status(),
            actix_web::http::StatusCode::INTERNAL_SERVER_ERROR
        );
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .expect("body");
        let value: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(
            value["recovery_run_id"],
            "123e4567-e89b-12d3-a456-426614174000"
        );
        assert_eq!(value["error"]["type"], "api_error");
        assert_eq!(
            value["error"]["message"],
            "workflow storage unavailable; run recovery is required"
        );
        assert!(!value["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("failure"));
        assert_eq!(recovery_run_id_from_storage_details("disk /secret"), None);
        assert_eq!(
            recovery_run_id_from_storage_details(
                "storage failed; orphan run CREDENTIALSENTINEL could not be cancelled"
            ),
            None
        );
    }

    #[actix_web::test]
    async fn workflow_errors_preserve_status_and_redact_private_diagnostics() {
        let sentinel = "/private/workspace/credentials-PRIVATE-SENTINEL";
        let cases = [
            (
                WorkflowRunError::NotFound,
                actix_web::http::StatusCode::NOT_FOUND,
                "workflow run not found",
            ),
            (
                WorkflowRunError::Terminal,
                actix_web::http::StatusCode::CONFLICT,
                "workflow run is already terminal",
            ),
            (
                WorkflowRunError::InvalidInput(sentinel.to_string()),
                actix_web::http::StatusCode::BAD_REQUEST,
                "workflow input is invalid",
            ),
            (
                WorkflowRunError::Preflight(sentinel.to_string()),
                actix_web::http::StatusCode::BAD_REQUEST,
                "workflow preflight failed",
            ),
            (
                WorkflowRunError::Compile(bamboo_domain::WorkflowCompileError::InvalidSchema(
                    sentinel.to_string(),
                )),
                actix_web::http::StatusCode::BAD_REQUEST,
                "workflow definition is invalid",
            ),
        ];

        for (error, expected_status, expected_message) in cases {
            let response = workflow_error(error);
            assert_eq!(response.status(), expected_status);
            let body = actix_web::body::to_bytes(response.into_body())
                .await
                .expect("body");
            let value: serde_json::Value = serde_json::from_slice(&body).expect("json");
            assert_eq!(value["error"]["message"], expected_message);
            assert_eq!(value["error"]["type"], "api_error");
            assert!(!value.to_string().contains(sentinel));
        }
    }
}
