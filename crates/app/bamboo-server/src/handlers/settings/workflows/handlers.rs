use actix_web::{web, HttpResponse};
use bamboo_skills::legacy::LegacySyncOutcome;
use bamboo_skills::types::SkillDefinition;
use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::{app_state::AppState, error::AppError};

use super::types::{
    SaveWorkflowRequest, WorkflowCatalogQuery, WorkflowGetResponse, WorkflowListItem,
};
use super::validation::is_safe_workflow_name;

fn legacy_workflow_io_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// Metadata-only catalog shared by Lotus palette, explicit selection and model matching.
pub async fn list_workflow_catalog(
    app_state: web::Data<AppState>,
    query: web::Query<WorkflowCatalogQuery>,
) -> Result<HttpResponse, AppError> {
    let snapshot = if let Some(session_id) = query
        .session_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let session = app_state
            .load_session(session_id)
            .await
            .ok_or_else(|| AppError::NotFound(format!("Session '{session_id}'")))?;
        if let Some(workspace) = session.workspace_path_meta() {
            let workspace = tokio::fs::canonicalize(workspace).await.map_err(|error| {
                AppError::BadRequest(format!("Invalid session workspace: {error}"))
            })?;
            if !workspace.is_dir() {
                return Err(AppError::BadRequest(
                    "Session workspace must be a directory".to_string(),
                ));
            }
            app_state
                .skill_manager
                .store()
                .workflow_catalog_for_workspace(&workspace)
                .await
                .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?
        } else {
            app_state
                .skill_manager
                .store()
                .workflow_catalog_snapshot()
                .await
        }
    } else {
        app_state
            .skill_manager
            .store()
            .workflow_catalog_snapshot()
            .await
    };
    Ok(HttpResponse::Ok()
        .insert_header(("Cache-Control", "no-store"))
        .json(snapshot))
}

/// Lists all workflow markdown files
///
/// # HTTP Route
/// `GET /bamboo/workflows`
///
/// # Response Format
/// Returns array of workflow metadata:
/// ```json
/// [
///   {
///     "name": "myworkflow",
///     "filename": "myworkflow.md",
///     "size": 1234,
///     "modified_at": null
///   }
/// ]
/// ```
///
/// # Response Status
/// - `200 OK`: Successfully retrieved workflow list
pub async fn list_workflows(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let workflows_dir = app_state.app_data_dir.join("workflows");
    let mut workflows: Vec<WorkflowListItem> = app_state
        .skill_manager
        .store()
        .list_skills(None, false)
        .await
        .into_iter()
        .filter(|skill| legacy_source(skill, &workflows_dir).is_some())
        .map(|skill| WorkflowListItem {
            name: legacy_name(&skill).to_string(),
            filename: format!("{}.md", legacy_name(&skill)),
            size: skill.prompt.len() as u64,
            modified_at: None,
        })
        .collect();

    workflows.sort_by(|left, right| left.name.cmp(&right.name));

    Ok(legacy_response().json(workflows))
}

/// Gets a specific workflow by name.
///
/// # HTTP Route
/// `GET /bamboo/workflows/{name}`
pub async fn get_workflow(
    app_state: web::Data<AppState>,
    workflow_name: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let name = workflow_name.into_inner();
    if !is_safe_workflow_name(&name) {
        // An invalid (malformed) name is a 400, matching every other workflow
        // handler — not a 404, which would imply a valid-but-absent workflow. #97.
        return Err(AppError::BadRequest("Invalid workflow name".to_string()));
    }

    let dir = app_state.app_data_dir.join("workflows");
    let filename = format!("{name}.md");
    let skill_id = bamboo_skills::legacy::legacy_workflow_skill_id(&name);
    let skill = app_state
        .skill_manager
        .store()
        .get_skill(&skill_id)
        .await
        .map_err(|_| AppError::NotFound(format!("Workflow '{name}'")))?;
    if legacy_source(&skill, &dir).is_none() || legacy_name(&skill) != name {
        return Err(AppError::NotFound(format!("Workflow '{name}'")));
    }
    let content = skill.prompt;
    let size = content.len() as u64;

    Ok(legacy_response().json(WorkflowGetResponse {
        name,
        filename,
        content,
        size,
        modified_at: None,
    }))
}

/// Creates or updates a workflow.
///
/// # HTTP Route
/// `POST /bamboo/workflows`
pub async fn save_workflow(
    app_state: web::Data<AppState>,
    payload: web::Json<SaveWorkflowRequest>,
) -> Result<HttpResponse, AppError> {
    let _io_guard = legacy_workflow_io_lock().lock().await;
    let name = payload.name.trim();
    if !is_safe_workflow_name(name) {
        return Err(AppError::BadRequest("Invalid workflow name".to_string()));
    }

    let dir = app_state.app_data_dir.join("workflows");
    fs::create_dir_all(&dir).await?;

    let file_path = dir.join(format!("{}.md", name));
    let skill_id = bamboo_skills::legacy::legacy_workflow_skill_id(name);
    let preflight = bamboo_skills::legacy::legacy_bundle_preflight(
        &file_path,
        &app_state.app_data_dir.join("skills"),
        &skill_id,
    )
    .await;
    match preflight {
        Ok(true) => {}
        Ok(false) => {
            return Err(AppError::BadRequest(format!(
                "Workflow '{name}' conflicts with a non-legacy skill bundle"
            )))
        }
        Err(error) => {
            return Err(AppError::InternalError(anyhow::anyhow!(error)));
        }
    }

    let temporary = dir.join(format!(".{name}.{}.tmp", uuid::Uuid::new_v4()));
    let mut staging = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .await?;
    if let Err(error) = async {
        staging.write_all(payload.content.as_bytes()).await?;
        staging.flush().await?;
        staging.sync_all().await?;
        drop(staging);
        bamboo_skills::legacy::atomic_replace_file(&temporary, &file_path).await
    }
    .await
    {
        let _ = fs::remove_file(&temporary).await;
        return Err(error.into());
    }

    // Source is authoritative and durable first. If bundle sync fails, the watcher/import pass
    // retries from this committed source instead of leaving an unrecoverable split-brain write.
    let outcome = bamboo_skills::legacy::sync_legacy_markdown_bundle(
        &file_path,
        &app_state.app_data_dir.join("skills"),
        &skill_id,
        &payload.content,
    )
    .await
    .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?;
    if outcome == LegacySyncOutcome::Conflict {
        return Err(AppError::BadRequest(format!(
            "Workflow '{name}' ownership changed during update; source was committed and will not overwrite the bundle"
        )));
    }
    app_state
        .skill_manager
        .store()
        .reload()
        .await
        .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?;

    Ok(legacy_response().json(serde_json::json!({
        "success": true,
        "path": file_path.to_string_lossy(),
        "catalog_revision": app_state.skill_manager.store().workflow_catalog_snapshot().await.revision,
    })))
}

/// Deletes a workflow file.
///
/// # HTTP Route
/// `DELETE /bamboo/workflows/{name}`
pub async fn delete_workflow(
    app_state: web::Data<AppState>,
    workflow_name: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let _io_guard = legacy_workflow_io_lock().lock().await;
    let name = workflow_name.into_inner();
    if !is_safe_workflow_name(&name) {
        return Err(AppError::BadRequest("Invalid workflow name".to_string()));
    }

    let dir = app_state.app_data_dir.join("workflows");
    let file_path = dir.join(format!("{}.md", name));
    let skill_id = bamboo_skills::legacy::legacy_workflow_skill_id(&name);

    if !file_path.exists() {
        return Err(AppError::NotFound(format!("Workflow '{}'", name)));
    }

    let removed_bundle = app_state
        .skill_manager
        .store()
        .remove_legacy_workflow(&file_path, &skill_id)
        .await
        .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?;
    if !removed_bundle {
        return Err(AppError::BadRequest(format!(
            "Workflow '{name}' is not owned by the legacy adapter"
        )));
    }

    Ok(legacy_response().json(serde_json::json!({ "success": true })))
}

fn legacy_source(skill: &SkillDefinition, workflows_dir: &std::path::Path) -> Option<String> {
    let metadata = skill.metadata.as_ref()?;
    if metadata
        .get("legacy_import")
        .and_then(|value| value.as_bool())
        != Some(true)
    {
        return None;
    }
    let source = metadata.get("original_source")?.as_str()?;
    let expected = workflows_dir.join(format!("{}.md", legacy_name(skill)));
    (std::path::Path::new(source) == expected).then(|| source.to_string())
}

fn legacy_name(skill: &SkillDefinition) -> &str {
    skill
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("legacy_name"))
        .and_then(|value| value.as_str())
        .unwrap_or(&skill.id)
}

fn legacy_response() -> actix_web::HttpResponseBuilder {
    let mut response = HttpResponse::Ok();
    response
        .insert_header(("Deprecation", "true"))
        .insert_header(("Sunset", "2026-12-01"))
        .insert_header((
            "Link",
            "</api/v1/bamboo/workflow-catalog>; rel=\"successor-version\"",
        ));
    response
}
