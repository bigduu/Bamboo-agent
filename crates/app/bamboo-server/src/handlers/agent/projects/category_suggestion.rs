use actix_web::{http::StatusCode, web, HttpResponse, Resource, Result};
use bamboo_domain::{ProjectId, ProjectStatus};
use bamboo_engine::session_app::category_suggestion::{suggest, Suggestion, DEADLINE};
use bamboo_projects::ProjectStoreError;
use serde::Deserialize;
use tokio::time::{timeout_at, Instant};

use crate::{app_state::AppState, error};

const MAX_TASK_CHARS: usize = 2_000;

#[derive(Deserialize)]
struct SuggestionRequest {
    task_text: String,
}

pub(crate) fn resource() -> Resource {
    web::resource("/projects/{project_id}/session-category-suggestion")
        .app_data(web::JsonConfig::default().limit(32 * 1024))
        .route(web::post().to(post))
}

async fn post(
    state: web::Data<AppState>,
    path: web::Path<String>,
    body: web::Json<SuggestionRequest>,
) -> Result<HttpResponse> {
    let id = match super::parse_id(&path) {
        Ok(id) => id,
        Err(response) => return Ok(*response),
    };
    let deadline = Instant::now() + DEADLINE;
    timeout_at(
        deadline,
        suggest_in_project(&state, &id, &body.task_text, deadline),
    )
    .await
    .unwrap_or_else(|_| Ok(suggestion_response(&id, None, "timeout")))
}

async fn suggest_in_project(
    state: &AppState,
    id: &ProjectId,
    task_text: &str,
    deadline: Instant,
) -> Result<HttpResponse> {
    let projects = state.project_store.clone();
    let project_id = id.clone();
    let project = match web::block(move || projects.get_read_only(&project_id)).await? {
        Ok(project) => project,
        Err(ProjectStoreError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(error::json_error_with_code(
                StatusCode::CONFLICT,
                "Current Project state is unavailable".into(),
                Some("project_unavailable"),
            ));
        }
        Err(ProjectStoreError::Io(error))
            if error.raw_os_error() == fs2::lock_contended_error().raw_os_error() =>
        {
            return Ok(error::json_error_with_code(
                StatusCode::SERVICE_UNAVAILABLE,
                "Project is temporarily unavailable".into(),
                Some("project_unavailable"),
            ));
        }
        Err(error) => return Ok(super::project_error(error)),
    };
    if project.status != ProjectStatus::Active {
        return Ok(error::json_error_with_code(
            StatusCode::CONFLICT,
            "Session category suggestions require an active Project".into(),
            Some("project_archived"),
        ));
    }
    let task_text = task_text.trim();
    if task_text.is_empty() || task_text.chars().take(MAX_TASK_CHARS + 1).count() > MAX_TASK_CHARS {
        return Ok(error::json_error(
            StatusCode::BAD_REQUEST,
            "task_text must contain 1–2,000 characters after trimming",
        ));
    }
    let candidates = state
        .session_store
        .project_session_category_counts(id)
        .await
        .into_keys()
        .collect();
    let selection = suggest(
        &state.config,
        &state.provider_registry,
        task_text,
        candidates,
        deadline,
    )
    .await;
    let (category, outcome) = match selection {
        Suggestion::Selected(category) => (Some(category), "selected"),
        Suggestion::Uncertain => (None, "uncertain"),
        Suggestion::Unavailable => (None, "unavailable"),
        Suggestion::Timeout => (None, "timeout"),
    };
    Ok(suggestion_response(id, category, outcome))
}

fn suggestion_response(id: &ProjectId, category: Option<String>, outcome: &str) -> HttpResponse {
    HttpResponse::Ok()
        .insert_header(("Cache-Control", "no-store"))
        .json(serde_json::json!({
            "project_id": id,
            "category": category,
            "outcome": outcome,
        }))
}

#[cfg(test)]
#[path = "category_suggestion_tests.rs"]
mod tests;
