//! Safe HTTP facade for durable catalog-pinned workflow runs.

use actix_web::{web, HttpResponse};
use bamboo_engine::WorkflowRunError;
use serde::Deserialize;
use serde_json::Value;

use crate::app_state::AppState;
use crate::workflow::public_workflow_snapshot;

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
        Ok(progress) => HttpResponse::Ok().json(progress.events),
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
    let message = error.to_string();
    match error {
        WorkflowRunError::NotFound => {
            HttpResponse::NotFound().json(serde_json::json!({"error": message}))
        }
        WorkflowRunError::Terminal => {
            HttpResponse::Conflict().json(serde_json::json!({"error": message}))
        }
        WorkflowRunError::Storage(details) => {
            let recovery_run_id = recovery_run_id_from_storage_details(&details);
            HttpResponse::InternalServerError().json(match recovery_run_id {
                Some(run_id) => serde_json::json!({
                    "error": "workflow storage unavailable; run recovery is required",
                    "recovery_run_id": run_id,
                }),
                None => serde_json::json!({"error": "workflow storage unavailable"}),
            })
        }
        WorkflowRunError::Compile(_)
        | WorkflowRunError::InvalidInput(_)
        | WorkflowRunError::Preflight(_) => {
            HttpResponse::BadRequest().json(serde_json::json!({"error": message}))
        }
    }
}

fn recovery_run_id_from_storage_details(details: &str) -> Option<&str> {
    details
        .split("orphan run ")
        .nth(1)
        .and_then(|tail| tail.split_whitespace().next())
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 64
                && value
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || character == '-')
        })
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
        assert!(!value["error"]
            .as_str()
            .unwrap_or_default()
            .contains("failure"));
        assert_eq!(recovery_run_id_from_storage_details("disk /secret"), None);
    }
}
