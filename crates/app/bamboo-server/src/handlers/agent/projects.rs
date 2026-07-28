//! First-class Project registry HTTP API.
//!
//! Project manifests are authoritative, revisioned documents. Every mutation
//! uses optimistic CAS and emits a durable account change-feed event. Resource
//! responses are counts/revisions only and never include file contents,
//! environment values, headers, or credentials.

use actix_web::{http::StatusCode, web, HttpRequest, HttpResponse, Result};
use bamboo_agent_core::AgentEvent;
use bamboo_domain::{
    LegacySessionProjectInput, ProjectId, ProjectManifest, ProjectStatus, WorkspaceBinding,
};
use bamboo_projects::{plan_legacy_migration, ProjectStoreError};
use serde::Deserialize;

use crate::app_state::AppState;

#[derive(Debug, Deserialize)]
pub struct CreateProjectRequest {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    /// Existing user source/work folder. New active Projects cannot be
    /// created without an authoritative default execution directory.
    pub project_path: String,
    #[serde(default)]
    pub workspace_bindings: Vec<WorkspaceBinding>,
}

/// Explicitly-present nullable description: absent leaves it unchanged, null clears it.
fn deserialize_nullable_description<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Option<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(Some)
}

#[derive(Debug, Deserialize)]
pub struct PatchProjectRequest {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default, deserialize_with = "deserialize_nullable_description")]
    pub description: Option<Option<String>>,
    /// Select a new authoritative Project folder using the same Project CAS
    /// revision as name/description updates.
    #[serde(default)]
    pub project_path: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct WorkspaceMutationRequest {
    pub path: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub git_common_dir: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct LegacyDryRunRequest {
    #[serde(default)]
    pub sessions: Vec<LegacySessionProjectInput>,
}

#[derive(Debug, Deserialize)]
pub struct LegacyMemoryMigrationRequest {
    pub legacy_project_key: String,
}

#[derive(Debug, Deserialize)]
pub struct LegacyMemoryMigrationStatusQuery {
    pub legacy_project_key: String,
}

fn parse_if_match(req: &HttpRequest) -> std::result::Result<u64, HttpResponse> {
    let Some(raw) = req.headers().get(actix_web::http::header::IF_MATCH) else {
        return Err(crate::error::json_error(
            StatusCode::PRECONDITION_REQUIRED,
            "If-Match with the current Project revision is required",
        ));
    };
    let value = raw
        .to_str()
        .ok()
        .map(str::trim)
        .and_then(|value| value.strip_prefix("W/").or(Some(value)))
        .map(|value| value.trim_matches('"'))
        .and_then(|value| value.parse::<u64>().ok());
    value.ok_or_else(|| {
        crate::error::json_error(
            StatusCode::BAD_REQUEST,
            "If-Match must be a Project revision integer or quoted ETag",
        )
    })
}

fn parse_id(raw: &str) -> std::result::Result<ProjectId, HttpResponse> {
    raw.parse::<ProjectId>()
        .map_err(|_| crate::error::json_error(StatusCode::BAD_REQUEST, "invalid Project id"))
}

fn project_error(error: ProjectStoreError) -> HttpResponse {
    if let ProjectStoreError::NotArchived(project_id) = &error {
        return HttpResponse::Conflict().json(serde_json::json!({
            "error": {
                "type": "api_error",
                "code": "project_not_archived",
                "message": "Project is not archived"
            },
            "project_id": project_id,
        }));
    }
    if let ProjectStoreError::ProjectPathUnbindConflict {
        project_id,
        project_path,
    } = &error
    {
        return HttpResponse::Conflict().json(serde_json::json!({
            "error": {
                "type": "api_error",
                "code": "project_path_unbind_conflict",
                "message": "Select another Project path before unbinding the current primary folder"
            },
            "project_id": project_id,
            "project_path": project_path,
        }));
    }
    let (status, message) = match error {
        ProjectStoreError::NotFound(_) => (StatusCode::NOT_FOUND, error.to_string()),
        ProjectStoreError::Conflict { .. } => (StatusCode::PRECONDITION_FAILED, error.to_string()),
        ProjectStoreError::AlreadyExists(_)
        | ProjectStoreError::NotArchived(_)
        | ProjectStoreError::Validation(_)
        | ProjectStoreError::InvalidPathComponent(_)
        | ProjectStoreError::ProjectPathUnbindConflict { .. } => {
            (StatusCode::CONFLICT, error.to_string())
        }
        ProjectStoreError::Io(_) | ProjectStoreError::Json(_) => {
            tracing::error!(%error, "Project registry operation failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Project registry operation failed".to_string(),
            )
        }
    };
    crate::error::json_error(status, message)
}

fn project_path_validation_error(
    error: crate::project_context::ProjectWorkspaceValidationError,
) -> HttpResponse {
    match error {
        crate::project_context::ProjectWorkspaceValidationError::Invalid {
            code,
            workspace,
            message,
        } => HttpResponse::Conflict().json(serde_json::json!({
            "error": {
                "type": "api_error",
                "code": code,
                "message": message
            },
            "project_path": workspace,
        })),
        crate::project_context::ProjectWorkspaceValidationError::Conflict {
            workspace,
            owner_project_id,
            session_project_id,
        } => HttpResponse::Conflict().json(serde_json::json!({
            "error": {
                "type": "api_error",
                "code": "project_path_conflict",
                "message": "Project path belongs to another Project"
            },
            "project_path": workspace,
            "owner_project_id": owner_project_id,
            "project_id": session_project_id,
        })),
        crate::project_context::ProjectWorkspaceValidationError::Store(error) => {
            project_error(error)
        }
    }
}

fn with_etag(project: &ProjectManifest, status: StatusCode) -> HttpResponse {
    HttpResponse::build(status)
        .insert_header((
            actix_web::http::header::ETAG,
            format!("\"{}\"", project.revision),
        ))
        .json(project)
}

pub async fn list_projects(state: web::Data<AppState>) -> Result<HttpResponse> {
    match state.project_store.list() {
        Ok(projects) => Ok(HttpResponse::Ok().json(serde_json::json!({ "projects": projects }))),
        Err(error) => Ok(project_error(error)),
    }
}

pub async fn create_project(
    state: web::Data<AppState>,
    request: web::Json<CreateProjectRequest>,
) -> Result<HttpResponse> {
    let project_path = match crate::project_context::validate_project_path_candidate_with_resolver(
        &state.project_store,
        None,
        &request.project_path,
        &state.workspace_resolver,
    ) {
        Ok(project_path) => bamboo_config::paths::path_to_display_string(&project_path),
        Err(error) => return Ok(project_path_validation_error(error)),
    };
    let project = match state.project_store.create_with_project_path(
        request.name.clone(),
        request.description.clone(),
        project_path,
        request.workspace_bindings.clone(),
    ) {
        Ok(project) => project,
        Err(error) => return Ok(project_error(error)),
    };
    state.account_sink.record(
        None,
        &AgentEvent::ProjectCreated {
            project_id: project.id.to_string(),
            revision: project.revision,
        },
    );
    Ok(with_etag(&project, StatusCode::CREATED))
}

pub async fn get_project(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let id = match parse_id(&path) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    match state.project_store.get(&id) {
        Ok(project) => Ok(with_etag(&project, StatusCode::OK)),
        Err(error) => Ok(project_error(error)),
    }
}

pub async fn patch_project(
    state: web::Data<AppState>,
    path: web::Path<String>,
    http_request: HttpRequest,
    request: web::Json<PatchProjectRequest>,
) -> Result<HttpResponse> {
    let id = match parse_id(&path) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let expected = match parse_if_match(&http_request) {
        Ok(revision) => revision,
        Err(response) => return Ok(response),
    };
    let mutate = |project: &mut ProjectManifest| {
        if let Some(name) = request.name.as_ref() {
            project.name = name.clone();
        }
        if let Some(description) = request.description.as_ref() {
            project.description = description.clone();
        }
        Ok(())
    };
    let result = if let Some(project_path) = request.project_path.as_deref() {
        let project_path =
            match crate::project_context::validate_project_path_candidate_with_resolver(
                &state.project_store,
                Some(&id),
                project_path,
                &state.workspace_resolver,
            ) {
                Ok(project_path) => bamboo_config::paths::path_to_display_string(&project_path),
                Err(error) => return Ok(project_path_validation_error(error)),
            };
        state
            .project_store
            .update_with_project_path(&id, expected, &project_path, mutate)
    } else {
        state.project_store.update(&id, expected, mutate)
    };
    let project = match result {
        Ok(project) => project,
        Err(error) => return Ok(project_error(error)),
    };
    state.account_sink.record(
        None,
        &AgentEvent::ProjectUpdated {
            project_id: project.id.to_string(),
            revision: project.revision,
        },
    );
    Ok(with_etag(&project, StatusCode::OK))
}

pub async fn bind_workspace(
    state: web::Data<AppState>,
    path: web::Path<String>,
    http_request: HttpRequest,
    request: web::Json<WorkspaceMutationRequest>,
) -> Result<HttpResponse> {
    let id = match parse_id(&path) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let expected = match parse_if_match(&http_request) {
        Ok(revision) => revision,
        Err(response) => return Ok(response),
    };
    let project = match state.project_store.bind_workspace(
        &id,
        expected,
        WorkspaceBinding {
            path: request.path.clone(),
            label: request.label.clone(),
            git_common_dir: request.git_common_dir.clone(),
        },
    ) {
        Ok(project) => project,
        Err(error) => return Ok(project_error(error)),
    };
    state.account_sink.record(
        None,
        &AgentEvent::ProjectUpdated {
            project_id: project.id.to_string(),
            revision: project.revision,
        },
    );
    Ok(with_etag(&project, StatusCode::OK))
}

pub async fn unbind_workspace(
    state: web::Data<AppState>,
    path: web::Path<String>,
    http_request: HttpRequest,
    request: web::Json<WorkspaceMutationRequest>,
) -> Result<HttpResponse> {
    let id = match parse_id(&path) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let expected = match parse_if_match(&http_request) {
        Ok(revision) => revision,
        Err(response) => return Ok(response),
    };
    let project = match state
        .project_store
        .unbind_workspace(&id, expected, &request.path)
    {
        Ok(project) => project,
        Err(error) => return Ok(project_error(error)),
    };
    state.account_sink.record(
        None,
        &AgentEvent::ProjectUpdated {
            project_id: project.id.to_string(),
            revision: project.revision,
        },
    );
    Ok(with_etag(&project, StatusCode::OK))
}

pub async fn project_resources(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let id = match parse_id(&path) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    match state.project_store.resource_summary(&id) {
        Ok(summary) => Ok(HttpResponse::Ok().json(summary)),
        Err(error) => Ok(project_error(error)),
    }
}

pub async fn archive_project(
    state: web::Data<AppState>,
    path: web::Path<String>,
    http_request: HttpRequest,
) -> Result<HttpResponse> {
    let id = match parse_id(&path) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let expected = match parse_if_match(&http_request) {
        Ok(revision) => revision,
        Err(response) => return Ok(response),
    };
    let project = match state.project_store.archive(&id, expected) {
        Ok(project) => project,
        Err(error) => return Ok(project_error(error)),
    };
    state.account_sink.record(
        None,
        &AgentEvent::ProjectArchived {
            project_id: project.id.to_string(),
            revision: project.revision,
        },
    );
    Ok(with_etag(&project, StatusCode::OK))
}

pub async fn unarchive_project(
    state: web::Data<AppState>,
    path: web::Path<String>,
    http_request: HttpRequest,
) -> Result<HttpResponse> {
    let id = match parse_id(&path) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let expected = match parse_if_match(&http_request) {
        Ok(revision) => revision,
        Err(response) => return Ok(response),
    };
    let project = match state.project_store.unarchive(&id, expected) {
        Ok(project) => project,
        Err(error) => return Ok(project_error(error)),
    };
    state.account_sink.record(
        None,
        &AgentEvent::ProjectUpdated {
            project_id: project.id.to_string(),
            revision: project.revision,
        },
    );
    Ok(with_etag(&project, StatusCode::OK))
}

pub async fn legacy_dry_run(
    state: web::Data<AppState>,
    request: web::Json<LegacyDryRunRequest>,
) -> Result<HttpResponse> {
    let projects = match state.project_store.list() {
        Ok(projects) => projects,
        Err(error) => return Ok(project_error(error)),
    };
    Ok(HttpResponse::Ok().json(plan_legacy_migration(&request.sessions, &projects)))
}

pub async fn migrate_legacy_memory(
    state: web::Data<AppState>,
    path: web::Path<String>,
    http_request: HttpRequest,
    request: web::Json<LegacyMemoryMigrationRequest>,
) -> Result<HttpResponse> {
    let id = match parse_id(&path) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let expected = match parse_if_match(&http_request) {
        Ok(revision) => revision,
        Err(response) => return Ok(response),
    };
    let current = match state.project_store.get(&id) {
        Ok(project) => project,
        Err(error) => return Ok(project_error(error)),
    };
    if current.revision != expected {
        return Ok(project_error(ProjectStoreError::Conflict {
            expected,
            actual: current.revision,
        }));
    }
    let project = if current
        .legacy_project_keys
        .iter()
        .any(|key| key == &request.legacy_project_key)
    {
        current
    } else {
        match state.project_store.update(&id, expected, |project| {
            project
                .legacy_project_keys
                .push(request.legacy_project_key.clone());
            Ok(())
        }) {
            Ok(project) => {
                state.account_sink.record(
                    None,
                    &AgentEvent::ProjectUpdated {
                        project_id: project.id.to_string(),
                        revision: project.revision,
                    },
                );
                project
            }
            Err(error) => return Ok(project_error(error)),
        }
    };
    match state
        .project_store
        .migrate_legacy_memory(&id, &request.legacy_project_key)
    {
        Ok(report) => {
            let authoritative = match state.project_store.get(&id) {
                Ok(project) => project,
                Err(error) => return Ok(project_error(error)),
            };
            if authoritative.revision != project.revision {
                state.account_sink.record(
                    None,
                    &AgentEvent::ProjectUpdated {
                        project_id: authoritative.id.to_string(),
                        revision: authoritative.revision,
                    },
                );
            }
            Ok(HttpResponse::Ok()
                .insert_header((
                    actix_web::http::header::ETAG,
                    format!("\"{}\"", authoritative.revision),
                ))
                .json(serde_json::json!({
                    "project_id": id,
                    "project_revision": authoritative.revision,
                    "migration": report,
                })))
        }
        Err(error) => Ok(project_error(error)),
    }
}

pub async fn legacy_memory_migration_status(
    state: web::Data<AppState>,
    path: web::Path<String>,
    query: web::Query<LegacyMemoryMigrationStatusQuery>,
) -> Result<HttpResponse> {
    let id = match parse_id(&path) {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    match state
        .project_store
        .legacy_memory_migration_status(&id, &query.legacy_project_key)
    {
        Ok(status) => Ok(HttpResponse::Ok().json(serde_json::json!({
            "project_id": id,
            "legacy_project_key": query.legacy_project_key,
            "migration": status,
        }))),
        Err(error) => Ok(project_error(error)),
    }
}

pub fn is_active(project: &ProjectManifest) -> bool {
    project.status == ProjectStatus::Active
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{http::header, test, App};
    use serde_json::Value;

    async fn app_state() -> (tempfile::TempDir, web::Data<AppState>) {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path().to_path_buf())
            .await
            .expect("test AppState");
        (dir, web::Data::new(state))
    }

    macro_rules! project_app {
        ($state:expr) => {
            App::new()
                .app_data($state)
                .route("/projects", web::get().to(list_projects))
                .route("/projects", web::post().to(create_project))
                .route("/projects/{id}", web::get().to(get_project))
                .route("/projects/{id}", web::patch().to(patch_project))
                .route("/projects/{id}/workspaces", web::post().to(bind_workspace))
                .route(
                    "/projects/{id}/workspaces",
                    web::delete().to(unbind_workspace),
                )
                .route("/projects/{id}/resources", web::get().to(project_resources))
                .route("/projects/{id}/archive", web::post().to(archive_project))
                .route(
                    "/projects/{id}/unarchive",
                    web::post().to(unarchive_project),
                )
                .route(
                    "/projects/migrations/legacy/dry-run",
                    web::post().to(legacy_dry_run),
                )
                .route(
                    "/projects/{id}/migrations/legacy-memory",
                    web::post().to(migrate_legacy_memory),
                )
                .route(
                    "/projects/{id}/migrations/legacy-memory",
                    web::get().to(legacy_memory_migration_status),
                )
        };
    }

    #[actix_web::test]
    async fn create_project_rejects_unavailable_path_without_registry_side_effects() {
        let (dir, state) = app_state().await;
        let app = test::init_service(project_app!(state.clone())).await;
        let missing = dir.path().join("missing-project");

        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/projects")
                .set_json(serde_json::json!({
                    "name": "Must not persist",
                    "project_path": missing
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body: Value = test::read_body_json(response).await;
        assert_eq!(
            body["error"]["code"], "project_path_unavailable",
            "unexpected response: {body}"
        );
        assert!(state.project_store.list().unwrap().is_empty());
    }

    #[actix_web::test]
    async fn project_routes_enforce_etag_cas_and_keep_identity_stable() {
        let (dir, state) = app_state().await;
        let mut feed = state.account_sink.subscribe();
        let app = test::init_service(project_app!(state.clone())).await;
        let project_path = dir.path().join("zenith");
        std::fs::create_dir_all(&project_path).unwrap();

        let create = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/projects")
                .set_json(serde_json::json!({
                    "name": "Zenith",
                    "description": "first",
                    "project_path": project_path
                }))
                .to_request(),
        )
        .await;
        assert_eq!(create.status(), StatusCode::CREATED);
        assert_eq!(create.headers().get(header::ETAG).unwrap(), "\"1\"");
        let created: ProjectManifest = test::read_body_json(create).await;
        assert_eq!(
            created.project_path.as_deref(),
            Some(
                project_path
                    .canonicalize()
                    .unwrap()
                    .to_string_lossy()
                    .as_ref()
            )
        );
        assert_eq!(
            created.project_path_status,
            bamboo_domain::ProjectPathStatus::Configured
        );
        let home = state.project_store.paths().project_home(&created.id);
        assert!(home.ends_with(created.id.as_str()));
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), feed.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            &event.event,
            AgentEvent::ProjectCreated { project_id, .. } if project_id == created.id.as_str()
        ));

        let missing_precondition = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&format!("/projects/{}", created.id))
                .set_json(serde_json::json!({"name":"Renamed"}))
                .to_request(),
        )
        .await;
        assert_eq!(
            missing_precondition.status(),
            StatusCode::PRECONDITION_REQUIRED
        );

        let stale = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&format!("/projects/{}", created.id))
                .insert_header((header::IF_MATCH, "\"9\""))
                .set_json(serde_json::json!({"name":"Renamed"}))
                .to_request(),
        )
        .await;
        assert_eq!(stale.status(), StatusCode::PRECONDITION_FAILED);

        let patched = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&format!("/projects/{}", created.id))
                .insert_header((header::IF_MATCH, "\"1\""))
                .set_json(serde_json::json!({"name":"Renamed"}))
                .to_request(),
        )
        .await;
        assert_eq!(patched.status(), StatusCode::OK);
        assert_eq!(patched.headers().get(header::ETAG).unwrap(), "\"2\"");
        let renamed: ProjectManifest = test::read_body_json(patched).await;
        assert_eq!(renamed.id, created.id);
        assert_eq!(renamed.name, "Renamed");
        assert_eq!(state.project_store.paths().project_home(&renamed.id), home);
        let updated_event = tokio::time::timeout(std::time::Duration::from_secs(1), feed.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            &updated_event.event,
            AgentEvent::ProjectUpdated { project_id, .. } if project_id == renamed.id.as_str()
        ));

        let moved_project_path = dir.path().join("zenith-moved");
        std::fs::create_dir_all(&moved_project_path).unwrap();
        let moved = test::call_service(
            &app,
            test::TestRequest::patch()
                .uri(&format!("/projects/{}", renamed.id))
                .insert_header((header::IF_MATCH, "\"2\""))
                .set_json(serde_json::json!({"project_path": moved_project_path}))
                .to_request(),
        )
        .await;
        assert_eq!(moved.status(), StatusCode::OK);
        assert_eq!(moved.headers().get(header::ETAG).unwrap(), "\"3\"");
        let moved: ProjectManifest = test::read_body_json(moved).await;
        assert_eq!(moved.id, created.id);
        assert_eq!(
            moved.project_path.as_deref(),
            Some(
                moved_project_path
                    .canonicalize()
                    .unwrap()
                    .to_string_lossy()
                    .as_ref()
            )
        );
        let path_updated_event =
            tokio::time::timeout(std::time::Duration::from_secs(1), feed.recv())
                .await
                .unwrap()
                .unwrap();
        assert!(matches!(
            &path_updated_event.event,
            AgentEvent::ProjectUpdated {
                project_id,
                revision: 3,
            } if project_id == renamed.id.as_str()
        ));

        let primary_unbind = test::call_service(
            &app,
            test::TestRequest::delete()
                .uri(&format!("/projects/{}/workspaces", renamed.id))
                .insert_header((header::IF_MATCH, "\"3\""))
                .set_json(serde_json::json!({"path": moved_project_path}))
                .to_request(),
        )
        .await;
        assert_eq!(primary_unbind.status(), StatusCode::CONFLICT);
        let primary_unbind: Value = test::read_body_json(primary_unbind).await;
        assert_eq!(
            primary_unbind["error"]["code"],
            "project_path_unbind_conflict"
        );
        assert_eq!(
            state.project_store.get(&renamed.id).unwrap().project_path,
            moved.project_path
        );

        let listed =
            test::call_service(&app, test::TestRequest::get().uri("/projects").to_request()).await;
        assert_eq!(listed.status(), StatusCode::OK);
        let listed: Value = test::read_body_json(listed).await;
        assert_eq!(listed["projects"][0]["id"], renamed.id.to_string());
        assert_eq!(listed["projects"][0]["project_path_status"], "configured");

        // Resource API returns only counts/revisions; file contents and secret
        // values never cross the contract.
        let commands = home.join("commands");
        std::fs::create_dir_all(&commands).unwrap();
        std::fs::write(commands.join("private.md"), "TOP-SECRET-VALUE").unwrap();
        let resources = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/projects/{}/resources", renamed.id))
                .to_request(),
        )
        .await;
        assert_eq!(resources.status(), StatusCode::OK);
        let body = test::read_body(resources).await;
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(!body.contains("TOP-SECRET-VALUE"));
        assert!(body.contains("resource_revision"));

        let legacy_key = "legacy-key";
        let legacy_root = dir
            .path()
            .join("memory/v1/scopes/projects")
            .join(legacy_key);
        std::fs::create_dir_all(&legacy_root).unwrap();
        std::fs::write(legacy_root.join("index.json"), r#"{"legacy":true}"#).unwrap();
        let migration_revision = state.project_store.get(&renamed.id).unwrap().revision;
        let migration = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!(
                    "/projects/{}/migrations/legacy-memory",
                    renamed.id
                ))
                .insert_header((header::IF_MATCH, format!("\"{migration_revision}\"")))
                .set_json(serde_json::json!({"legacy_project_key": legacy_key}))
                .to_request(),
        )
        .await;
        assert_eq!(migration.status(), StatusCode::OK);
        let migration_etag = migration
            .headers()
            .get(header::ETAG)
            .expect("migration ETag")
            .to_str()
            .unwrap()
            .to_string();
        let migration_body: Value = test::read_body_json(migration).await;
        assert_eq!(migration_body["migration"]["phase"], "committed");
        let response_revision = migration_body["project_revision"]
            .as_u64()
            .expect("project revision");
        assert_eq!(migration_etag, format!("\"{response_revision}\""));
        assert_eq!(
            state.project_store.get(&renamed.id).unwrap().revision,
            response_revision,
            "endpoint must return the authoritative post-migration CAS revision"
        );
        let mut observed_final_event = false;
        for _ in 0..8 {
            let event = tokio::time::timeout(std::time::Duration::from_secs(1), feed.recv())
                .await
                .unwrap()
                .unwrap();
            if matches!(
                &event.event,
                AgentEvent::ProjectUpdated {
                    project_id,
                    revision,
                } if project_id == renamed.id.as_str() && *revision == response_revision
            ) {
                observed_final_event = true;
                break;
            }
        }
        assert!(
            observed_final_event,
            "post-migration revision must be published to the Project change feed"
        );
        let before_external_write = state.project_store.get(&renamed.id).unwrap().revision;
        std::fs::write(
            commands.join("independent.md"),
            "independent external change",
        )
        .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
        loop {
            let current = state.project_store.get(&renamed.id).unwrap().revision;
            if current > before_external_write {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "an external resource write interleaved after migration must advance revision"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert!(legacy_root.join("index.json").is_file());
        assert!(home.join("memory/v1/index.json").is_file());

        let migration_status = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!(
                    "/projects/{}/migrations/legacy-memory?legacy_project_key={legacy_key}",
                    renamed.id
                ))
                .to_request(),
        )
        .await;
        assert_eq!(migration_status.status(), StatusCode::OK);
        let status_body: Value = test::read_body_json(migration_status).await;
        assert_eq!(status_body["migration"]["phase"], "committed");

        let archive_revision = state.project_store.get(&renamed.id).unwrap().revision;
        let archive = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/projects/{}/archive", renamed.id))
                .insert_header((header::IF_MATCH, format!("\"{archive_revision}\"")))
                .to_request(),
        )
        .await;
        assert_eq!(archive.status(), StatusCode::OK);
        let archived: ProjectManifest = test::read_body_json(archive).await;
        assert_eq!(archived.status, ProjectStatus::Archived);
        loop {
            let event = tokio::time::timeout(std::time::Duration::from_secs(1), feed.recv())
                .await
                .unwrap()
                .unwrap();
            if matches!(
                &event.event,
                AgentEvent::ProjectArchived { project_id, .. }
                    if project_id == archived.id.as_str()
            ) {
                break;
            }
        }

        let dry_run = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/projects/migrations/legacy/dry-run")
                .set_json(serde_json::json!({"sessions":[]}))
                .to_request(),
        )
        .await;
        assert_eq!(dry_run.status(), StatusCode::OK);
        drop(dir);
    }

    #[actix_web::test]
    async fn unarchive_route_is_cas_guarded_replayable_and_preserves_ownership() {
        let (dir, state) = app_state().await;
        let app = test::init_service(project_app!(state.clone())).await;
        let project_path = dir.path().join("zenith");
        let workspace_path = dir.path().join("worktree");
        std::fs::create_dir_all(&project_path).unwrap();
        std::fs::create_dir_all(&workspace_path).unwrap();

        let project = state
            .project_store
            .create_with_project_path(
                "Zenith",
                Some("Restore me".to_string()),
                project_path.to_string_lossy(),
                vec![WorkspaceBinding {
                    path: workspace_path.to_string_lossy().into_owned(),
                    label: Some("Issue worktree".to_string()),
                    git_common_dir: None,
                }],
            )
            .unwrap();
        let project = state
            .project_store
            .update(&project.id, project.revision, |manifest| {
                manifest
                    .legacy_project_keys
                    .push("legacy-zenith".to_string());
                Ok(())
            })
            .unwrap();
        let archived = state
            .project_store
            .archive(&project.id, project.revision)
            .unwrap();

        let session_id = "project-unarchive-session";
        let mut session = bamboo_agent_core::Session::new(session_id, "test-model");
        session.set_project_id_meta(archived.id.to_string());
        session.set_workspace_path_meta(archived.workspace_bindings[0].path.clone());
        state.storage.save_session(&session).await.unwrap();

        let missing = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/projects/{}/unarchive", archived.id))
                .to_request(),
        )
        .await;
        assert_eq!(missing.status(), StatusCode::PRECONDITION_REQUIRED);

        let stale = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/projects/{}/unarchive", archived.id))
                .insert_header((header::IF_MATCH, format!("\"{}\"", archived.revision + 1)))
                .to_request(),
        )
        .await;
        assert_eq!(stale.status(), StatusCode::PRECONDITION_FAILED);
        assert_eq!(
            state.project_store.get(&archived.id).unwrap(),
            archived,
            "stale restore must not mutate the Project"
        );

        let journal_cursor = state.account_sink.latest_seq();
        let mut feed = state.account_sink.subscribe();
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/projects/{}/unarchive", archived.id))
                .insert_header((header::IF_MATCH, format!("\"{}\"", archived.revision)))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::ETAG).unwrap(),
            format!("\"{}\"", archived.revision + 1).as_str()
        );
        let restored: ProjectManifest = test::read_body_json(response).await;
        assert_eq!(restored.status, ProjectStatus::Active);
        assert_eq!(restored.revision, archived.revision + 1);
        assert_eq!(restored.id, archived.id);
        assert_eq!(restored.project_path, archived.project_path);
        assert_eq!(restored.project_path_status, archived.project_path_status);
        assert_eq!(restored.workspace_bindings, archived.workspace_bindings);
        assert_eq!(restored.legacy_project_keys, archived.legacy_project_keys);
        assert_eq!(restored.resource_revision, archived.resource_revision);
        assert_eq!(restored.created_at, archived.created_at);

        let event = tokio::time::timeout(std::time::Duration::from_secs(1), feed.recv())
            .await
            .expect("ProjectUpdated delivery")
            .expect("account feed event");
        assert!(matches!(
            &event.event,
            AgentEvent::ProjectUpdated {
                project_id,
                revision,
            } if project_id == restored.id.as_str() && *revision == restored.revision
        ));
        let replay = bamboo_engine::events::journal::read_since(
            state.account_sink.events_dir(),
            journal_cursor,
        )
        .expect("journal replay");
        assert!(replay.iter().any(|change| matches!(
            &change.event,
            AgentEvent::ProjectUpdated {
                project_id,
                revision,
            } if project_id == restored.id.as_str() && *revision == restored.revision
        )));

        let persisted_session = state
            .storage
            .load_session(session_id)
            .await
            .unwrap()
            .expect("persisted session");
        assert_eq!(
            persisted_session.project_id_meta().as_deref(),
            Some(restored.id.as_str())
        );
        assert_eq!(
            persisted_session.workspace_path_meta(),
            Some(restored.workspace_bindings[0].path.clone())
        );

        let repeated = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/projects/{}/unarchive", restored.id))
                .insert_header((header::IF_MATCH, format!("\"{}\"", restored.revision)))
                .to_request(),
        )
        .await;
        assert_eq!(repeated.status(), StatusCode::CONFLICT);
        let body: Value = test::read_body_json(repeated).await;
        assert_eq!(body["error"]["code"], "project_not_archived");
        assert_eq!(body["project_id"], restored.id.to_string());
        assert_eq!(
            state.project_store.get(&restored.id).unwrap(),
            restored,
            "repeated restore must not create stale optimistic state"
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), feed.recv())
                .await
                .is_err(),
            "rejected restore must not publish ProjectUpdated"
        );
    }

    #[actix_web::test]
    async fn workspace_binding_routes_are_cas_guarded_and_conflict_safe() {
        let (dir, state) = app_state().await;
        let app = test::init_service(project_app!(state.clone())).await;
        let workspace = dir.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let first = state.project_store.create("First", None).unwrap();
        let second = state.project_store.create("Second", None).unwrap();

        let bind = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/projects/{}/workspaces", first.id))
                .insert_header((header::IF_MATCH, format!("\"{}\"", first.revision)))
                .set_json(serde_json::json!({"path": workspace}))
                .to_request(),
        )
        .await;
        assert_eq!(bind.status(), StatusCode::OK);
        let first_bound: ProjectManifest = test::read_body_json(bind).await;
        assert_eq!(first_bound.workspace_bindings.len(), 1);

        let conflict = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/projects/{}/workspaces", second.id))
                .insert_header((header::IF_MATCH, format!("\"{}\"", second.revision)))
                .set_json(serde_json::json!({"path": workspace}))
                .to_request(),
        )
        .await;
        assert_eq!(conflict.status(), StatusCode::CONFLICT);
        assert!(state
            .project_store
            .get(&second.id)
            .unwrap()
            .workspace_bindings
            .is_empty());

        let stale_unbind = test::call_service(
            &app,
            test::TestRequest::delete()
                .uri(&format!("/projects/{}/workspaces", first.id))
                .insert_header((header::IF_MATCH, "\"1\""))
                .set_json(serde_json::json!({"path": workspace}))
                .to_request(),
        )
        .await;
        assert_eq!(stale_unbind.status(), StatusCode::PRECONDITION_FAILED);

        let unbind = test::call_service(
            &app,
            test::TestRequest::delete()
                .uri(&format!("/projects/{}/workspaces", first.id))
                .insert_header((header::IF_MATCH, format!("\"{}\"", first_bound.revision)))
                .set_json(serde_json::json!({"path": workspace}))
                .to_request(),
        )
        .await;
        assert_eq!(unbind.status(), StatusCode::OK);
    }
}
