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
    #[serde(default)]
    pub args: Value,
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
        )
        .await
    {
        Ok(snapshot) => HttpResponse::Accepted().json(public_workflow_snapshot(snapshot)),
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
        WorkflowRunError::Storage(_) => HttpResponse::InternalServerError()
            .json(serde_json::json!({"error": "workflow storage unavailable"})),
        WorkflowRunError::Compile(_)
        | WorkflowRunError::InvalidInput(_)
        | WorkflowRunError::Preflight(_) => {
            HttpResponse::BadRequest().json(serde_json::json!({"error": message}))
        }
    }
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
}
