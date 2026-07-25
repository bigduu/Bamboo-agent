use actix_web::{web, HttpResponse, Result};
use uuid::Uuid;

use crate::app_state::AppState;
use bamboo_agent_core::Session;
use bamboo_engine::model_config_helper::normalize_gold_config_json;

use super::super::super::types::{CreateSessionRequest, CreateSessionResponse, SessionSummary};

/// Sync runtime workspace so tools can resolve the working directory. Mirrors
/// `chat::handler::sync_runtime_workspace` — #480 gives `POST /sessions` the
/// same `workspace_path` semantics as `POST /chat`.
fn sync_runtime_workspace(session_id: &str, workspace_path: Option<&str>) {
    if let Some(workspace) = workspace_path
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(std::path::PathBuf::from)
        .and_then(|path| std::fs::canonicalize(&path).ok().or(Some(path)))
    {
        bamboo_tools::tools::workspace_state::publish_resolved_workspace(session_id, workspace);
    }
}

/// `POST /api/v1/sessions`
pub async fn create_session(
    state: web::Data<AppState>,
    req: web::Json<CreateSessionRequest>,
) -> Result<HttpResponse> {
    if let Some(project_id) = req.project_id.as_ref() {
        match state.project_store.get(project_id) {
            Ok(project) if project.status == bamboo_domain::ProjectStatus::Active => {}
            Ok(_) => {
                return Ok(HttpResponse::Conflict().json(serde_json::json!({
                    "error": {
                        "type": "api_error",
                        "code": "project_archived",
                        "message": "Sessions can only be created in an active Project"
                    },
                    "project_id": project_id,
                })));
            }
            Err(bamboo_projects::ProjectStoreError::NotFound(_)) => {
                return Ok(crate::error::json_error(
                    actix_web::http::StatusCode::NOT_FOUND,
                    "target Project not found",
                ));
            }
            Err(error) => {
                return Err(crate::error::json_internal_server_error(format!(
                    "Failed to validate target Project: {error}"
                )));
            }
        }
    }
    let final_workspace = match crate::project_context::validate_workspace_assignment(
        &state.project_store,
        req.project_id.as_ref(),
        req.workspace_path.as_deref(),
    ) {
        Ok(workspace) => workspace,
        Err(error) => {
            return match error {
                crate::project_context::ProjectWorkspaceValidationError::Invalid {
                    code,
                    workspace,
                    message,
                } => Ok(HttpResponse::BadRequest().json(serde_json::json!({
                    "error": {
                        "type": "api_error",
                        "code": code,
                        "message": message
                    },
                    "workspace": workspace,
                }))),
                crate::project_context::ProjectWorkspaceValidationError::Conflict {
                    workspace,
                    owner_project_id,
                    session_project_id,
                } => Ok(HttpResponse::Conflict().json(serde_json::json!({
                    "error": {
                        "type": "api_error",
                        "code": "project_workspace_conflict",
                        "message": "Workspace belongs to another Project"
                    },
                    "workspace": workspace,
                    "owner_project_id": owner_project_id,
                    "session_project_id": session_project_id,
                }))),
                crate::project_context::ProjectWorkspaceValidationError::Store(error) => {
                    Err(crate::error::json_internal_server_error(format!(
                        "Failed to validate workspace Project ownership: {error}"
                    )))
                }
            };
        }
    };
    let final_workspace_display = final_workspace
        .as_deref()
        .map(bamboo_config::paths::path_to_display_string);
    let id = Uuid::new_v4().to_string();
    let global_default_prompt =
        bamboo_engine::prompt_defaults::read_global_default_system_prompt_template();
    let config_snapshot = state.config.read().await.clone();
    let gold_config_json = match req
        .gold_config
        .as_ref()
        .map(normalize_gold_config_json)
        .transpose()
    {
        Ok(value) => value,
        Err(error) => {
            // Canonical nested error envelope (#251 finding 2); `message` is kept
            // as a top-level sibling field too since existing callers already
            // read the detail from there, not from `error.message`.
            return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                "error": crate::error::error_value("Invalid gold_config"),
                "message": error.to_string()
            })));
        }
    };

    let mut session = build_new_session(
        &id,
        &req,
        gold_config_json,
        global_default_prompt.as_str(),
        &config_snapshot,
    );
    if let Some(workspace) = final_workspace_display.as_deref() {
        session.set_workspace_path_meta(workspace);
    } else if let Some(workspace) = config_snapshot.get_default_work_area_path() {
        session.set_workspace_path_meta(bamboo_config::paths::path_to_display_string(&workspace));
    }
    if let Err(error) = state
        .project_context_resolver
        .refresh_session_prompt_read_only(&mut session)
        .await
    {
        return Ok(match error {
            bamboo_engine::project_context::ProjectContextError::WorkspaceConflict {
                workspace,
                owner_project_id,
                session_project_id,
            } => HttpResponse::Conflict().json(serde_json::json!({
                "error": {
                    "type": "api_error",
                    "code": "project_workspace_conflict",
                    "message": "Workspace belongs to another Project"
                },
                "workspace": workspace,
                "owner_project_id": owner_project_id,
                "session_project_id": session_project_id,
            })),
            bamboo_engine::project_context::ProjectContextError::UnassignedWorkspaceConflict {
                workspace,
                owner_project_id,
            } => HttpResponse::Conflict().json(serde_json::json!({
                "error": {
                    "type": "api_error",
                    "code": "project_workspace_conflict",
                    "message": "Workspace belongs to another Project"
                },
                "workspace": workspace,
                "owner_project_id": owner_project_id,
                "session_project_id": "unassigned",
            })),
            bamboo_engine::project_context::ProjectContextError::WorkspaceInvalid {
                workspace,
                message,
            } => HttpResponse::BadRequest().json(serde_json::json!({
                "error": {
                    "type": "api_error",
                    "code": "workspace_invalid",
                    "message": message
                },
                "workspace": workspace,
            })),
            error => {
                return Err(crate::error::json_internal_server_error(format!(
                    "Failed to initialize Project prompt context: {error}"
                )));
            }
        });
    }

    state
        .storage
        .save_session(&session)
        .await
        .map_err(|error| {
            crate::error::json_internal_server_error(format!("Failed to save session: {error}"))
        })?;

    state.sessions.insert(
        id.clone(),
        std::sync::Arc::new(parking_lot::RwLock::new(session.clone())),
    );

    // Publish only the exact candidate that passed Project ownership checks,
    // and only after the authoritative session is durable. A storage failure
    // must not leave an orphan runtime workspace entry for an ID the API never
    // created.
    sync_runtime_workspace(&id, session.workspace_path_meta().as_deref());

    // Publish onto the account change feed so other clients insert the new
    // session into their list without polling `GET /sessions`.
    state.account_sink.record(
        Some(&id),
        &bamboo_agent_core::AgentEvent::SessionCreated {
            session_id: id.clone(),
            project_id: session.project_id_meta(),
            title: session.title.clone(),
            kind: session.kind,
            created_at: session.created_at,
        },
    );

    match state.session_store.get_index_entry(&id).await {
        // 201 Created — a new resource was created. Aligns `POST /api/v1/sessions`
        // with every other create endpoint (chat, mcp-add, prompt-presets,
        // provider-instances, cluster-nodes), which already return 201. #251
        // (finding 3).
        Some(entry) => Ok(HttpResponse::Created().json(CreateSessionResponse {
            session: SessionSummary::from_entry(entry, false),
        })),
        None => Ok(HttpResponse::InternalServerError().json(serde_json::json!({
            "error": crate::error::error_value("Session created but missing from index"),
            "session_id": id
        }))),
    }
}

fn build_new_session(
    id: &str,
    req: &CreateSessionRequest,
    gold_config_json: Option<String>,
    global_default_prompt: &str,
    config: &bamboo_llm::Config,
) -> Session {
    use bamboo_engine::session_app::session_create::{
        build_new_session as crate_build, CreateSessionConfig, CreateSessionInput,
    };

    let input = CreateSessionInput {
        id: id.to_string(),
        project_id: req.project_id.clone(),
        title: req.title.clone(),
        system_prompt: req.system_prompt.clone(),
        model: req.model.clone(),
        model_ref: req.model_ref.clone(),
        reasoning_effort: req.reasoning_effort,
        gold_config_json,
        workspace_path: req.workspace_path.clone(),
    };
    let create_config = CreateSessionConfig {
        default_model: config.get_model(),
        default_reasoning_effort: config.get_reasoning_effort(),
        global_default_prompt: global_default_prompt.to_string(),
        builtin_fallback_prompt: crate::app_state::DEFAULT_BASE_PROMPT,
    };

    crate_build(&input, &create_config)
}

#[cfg(test)]
mod tests {
    use actix_web::{http::StatusCode, test, web, App};
    use serde_json::Value;
    use tempfile::tempdir;

    use crate::routes::configure_routes;
    use crate::AppState;

    async fn new_state() -> web::Data<AppState> {
        let temp_dir = tempdir().expect("tempdir").keep();
        bamboo_config::paths::init_bamboo_dir(temp_dir.clone());
        web::Data::new(AppState::new(temp_dir).await.expect("app state"))
    }

    #[actix_web::test]
    async fn create_session_returns_201_created() {
        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .set_json(serde_json::json!({ "title": "New session" }))
                .to_request(),
        )
        .await;

        // 201 Created — aligns with the other create endpoints. #251 (finding 3).
        assert_eq!(resp.status(), StatusCode::CREATED);

        let body: Value = test::read_body_json(resp).await;
        assert!(
            body["session"]["id"].as_str().is_some(),
            "response should carry the created session summary"
        );
    }

    /// #480: `POST /sessions` gets the same `workspace_path` semantics as
    /// `POST /chat` — the created session's metadata carries the resolved
    /// workspace path.
    #[actix_web::test]
    async fn create_session_with_workspace_path_sets_it() {
        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let workspace_dir = tempdir().expect("workspace tempdir");
        let workspace_path = workspace_dir.path().to_string_lossy().to_string();
        let canonical_workspace_path = std::fs::canonicalize(workspace_dir.path())
            .expect("canonical workspace")
            .to_string_lossy()
            .into_owned();

        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .set_json(serde_json::json!({
                    "title": "Session with workspace",
                    "workspace_path": workspace_path,
                }))
                .to_request(),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::CREATED);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(
            body["session"]["workspace_path"].as_str(),
            Some(canonical_workspace_path.as_str())
        );
        let session_id = body["session"]["id"]
            .as_str()
            .expect("session id")
            .to_string();

        let session = state
            .storage
            .load_session(&session_id)
            .await
            .expect("load")
            .expect("session exists");
        assert_eq!(
            session.workspace_path_meta().as_deref(),
            Some(canonical_workspace_path.as_str())
        );

        let list_resp = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/sessions")
                .to_request(),
        )
        .await;
        assert_eq!(list_resp.status(), StatusCode::OK);
        let list_body: Value = test::read_body_json(list_resp).await;
        assert_eq!(
            list_body["sessions"][0]["workspace_path"].as_str(),
            Some(canonical_workspace_path.as_str())
        );

        let detail_resp = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/api/v1/sessions/{session_id}"))
                .to_request(),
        )
        .await;
        assert_eq!(detail_resp.status(), StatusCode::OK);
        let detail_body: Value = test::read_body_json(detail_resp).await;
        assert_eq!(
            detail_body["session"]["workspace_path"].as_str(),
            Some(canonical_workspace_path.as_str())
        );
    }

    /// Omitting `workspace_path` persists the same validated fallback that
    /// runtime tools will use.
    #[actix_web::test]
    async fn create_session_without_workspace_path_persists_validated_fallback() {
        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .set_json(serde_json::json!({ "title": "No workspace" }))
                .to_request(),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::CREATED);
        let body: Value = test::read_body_json(resp).await;
        assert!(
            body["session"].get("project_id").is_some(),
            "create response must expose the Unassigned Project as null"
        );
        assert!(body["session"]["project_id"].is_null());
        let session_id = body["session"]["id"]
            .as_str()
            .expect("session id")
            .to_string();

        let session = state
            .storage
            .load_session(&session_id)
            .await
            .expect("load")
            .expect("session exists");
        let workspace = session
            .workspace_path_meta()
            .expect("validated session fallback workspace");
        assert!(
            std::path::Path::new(&workspace).is_dir(),
            "authoritative create must materialize the validated fallback"
        );
        assert_eq!(
            bamboo_agent_core::workspace_state::get_workspace(&session_id)
                .as_deref()
                .map(bamboo_config::paths::path_to_display_string),
            Some(workspace)
        );
        assert!(
            bamboo_tools::tools::workspace_state::workspace_or_process_cwd(Some(&session_id))
                .is_dir(),
            "tool cwd must be usable immediately after create"
        );

        let list = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/sessions")
                .to_request(),
        )
        .await;
        let list: Value = test::read_body_json(list).await;
        let listed = list["sessions"]
            .as_array()
            .expect("sessions")
            .iter()
            .find(|entry| entry["id"] == session_id)
            .expect("created session in list");
        assert!(
            listed.get("project_id").is_some(),
            "list response must expose the Unassigned Project as null"
        );
        assert!(listed["project_id"].is_null());

        let detail = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/api/v1/sessions/{session_id}"))
                .to_request(),
        )
        .await;
        let detail: Value = test::read_body_json(detail).await;
        assert!(
            detail["session"].get("project_id").is_some(),
            "detail response must expose the Unassigned Project as null"
        );
        assert!(detail["session"]["project_id"].is_null());
    }

    #[actix_web::test]
    async fn project_workspace_ownership_is_checked_before_create_side_effects() {
        let state = new_state().await;
        let owner_workspace = tempdir().expect("owner workspace");
        let nested_workspace = owner_workspace.path().join("nested");
        std::fs::create_dir_all(&nested_workspace).expect("nested workspace");
        let owner = state
            .project_store
            .create_with_bindings(
                "Owner",
                None,
                vec![bamboo_domain::WorkspaceBinding {
                    path: owner_workspace.path().to_string_lossy().to_string(),
                    label: None,
                    git_common_dir: None,
                }],
            )
            .expect("owner Project");
        let other = state
            .project_store
            .create("Other", None)
            .expect("other Project");
        let nested_workspace_display = nested_workspace.to_string_lossy().to_string();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let conflict = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .set_json(serde_json::json!({
                    "title": "Must not exist",
                    "project_id": other.id.to_string(),
                    "workspace_path": nested_workspace_display.clone(),
                }))
                .to_request(),
        )
        .await;
        assert_eq!(conflict.status(), StatusCode::CONFLICT);
        let conflict_body: Value = test::read_body_json(conflict).await;
        assert_eq!(conflict_body["error"]["code"], "project_workspace_conflict");

        let list = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/sessions")
                .to_request(),
        )
        .await;
        let list: Value = test::read_body_json(list).await;
        assert_eq!(list["sessions"].as_array().map(Vec::len), Some(0));

        let created = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .set_json(serde_json::json!({
                    "title": "Owned nested workspace",
                    "project_id": owner.id.to_string(),
                    "workspace_path": nested_workspace_display.clone(),
                }))
                .to_request(),
        )
        .await;
        assert_eq!(created.status(), StatusCode::CREATED);
        let created: Value = test::read_body_json(created).await;
        assert_eq!(created["session"]["project_id"], owner.id.as_str());
        let session_id = created["session"]["id"].as_str().expect("session id");

        let prompt = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/api/v1/sessions/{session_id}/system-prompt"))
                .to_request(),
        )
        .await;
        assert_eq!(prompt.status(), StatusCode::OK);
        let prompt: Value = test::read_body_json(prompt).await;
        assert!(prompt["project_context"]
            .as_str()
            .is_some_and(|value| value.contains(owner.id.as_str())));
        let effective = prompt["effective_system_prompt"]
            .as_str()
            .expect("effective prompt");
        assert_eq!(
            effective
                .matches("<!-- BAMBOO_PROJECT_CONTEXT_START -->")
                .count(),
            1
        );
        assert_eq!(
            effective
                .matches("<!-- BAMBOO_WORKSPACE_CONTEXT_START -->")
                .count(),
            1
        );
        assert!(effective.contains("Binding status: registered"));
    }

    #[actix_web::test]
    async fn assigned_session_created_event_replays_project_identity_from_journal() {
        let state = new_state().await;
        let project = state
            .project_store
            .create("Journal Project", None)
            .expect("Project");
        let mut live = state.account_sink.subscribe();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .set_json(serde_json::json!({
                    "title": "Journaled session",
                    "project_id": project.id.to_string()
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let body: Value = test::read_body_json(response).await;
        let session_id = body["session"]["id"].as_str().expect("session id");
        let live_event = tokio::time::timeout(std::time::Duration::from_secs(1), live.recv())
            .await
            .expect("live event timeout")
            .expect("live event");
        assert!(matches!(
            &live_event.event,
            bamboo_agent_core::AgentEvent::SessionCreated {
                session_id: event_session_id,
                project_id: Some(event_project_id),
                ..
            } if event_session_id == session_id && event_project_id == project.id.as_str()
        ));

        let replay = bamboo_engine::events::journal::read_since(
            state.account_sink.events_dir(),
            live_event.seq.saturating_sub(1),
        )
        .expect("journal replay");
        assert!(replay.iter().any(|change| matches!(
            &change.event,
            bamboo_agent_core::AgentEvent::SessionCreated {
                session_id: event_session_id,
                project_id: Some(event_project_id),
                ..
            } if event_session_id == session_id && event_project_id == project.id.as_str()
        )));
    }

    #[actix_web::test]
    async fn configured_default_workspace_is_validated_before_session_creation() {
        let state = new_state().await;
        let workspace = tempdir().expect("default workspace");
        let owner = state
            .project_store
            .create_with_bindings(
                "Default Owner",
                None,
                vec![bamboo_domain::WorkspaceBinding {
                    path: workspace.path().to_string_lossy().into_owned(),
                    label: None,
                    git_common_dir: None,
                }],
            )
            .expect("owner Project");
        let other = state
            .project_store
            .create("Other Project", None)
            .expect("other Project");
        state.config.write().await.default_work_area = Some(bamboo_config::DefaultWorkAreaConfig {
            path: Some(workspace.path().to_string_lossy().into_owned()),
        });
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let conflict = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .set_json(serde_json::json!({
                    "title": "Must not persist",
                    "project_id": other.id.to_string()
                }))
                .to_request(),
        )
        .await;
        assert_eq!(conflict.status(), StatusCode::CONFLICT);
        let body: Value = test::read_body_json(conflict).await;
        assert_eq!(body["error"]["code"], "project_workspace_conflict");
        assert_eq!(body["owner_project_id"], owner.id.as_str());
        assert!(state.session_store.list_index_entries().await.is_empty());
    }

    #[actix_web::test]
    async fn same_project_default_workspace_is_persisted_with_prompt_marker() {
        let state = new_state().await;
        let workspace = tempdir().expect("default workspace");
        let project = state
            .project_store
            .create_with_bindings(
                "Default Owner",
                None,
                vec![bamboo_domain::WorkspaceBinding {
                    path: workspace.path().to_string_lossy().into_owned(),
                    label: None,
                    git_common_dir: None,
                }],
            )
            .expect("Project");
        state.config.write().await.default_work_area = Some(bamboo_config::DefaultWorkAreaConfig {
            path: Some(workspace.path().to_string_lossy().into_owned()),
        });
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .set_json(serde_json::json!({
                    "title": "Uses validated default",
                    "project_id": project.id.to_string()
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let body: Value = test::read_body_json(response).await;
        let session_id = body["session"]["id"].as_str().expect("session id");
        let canonical = workspace
            .path()
            .canonicalize()
            .expect("canonical workspace");
        let canonical_display = bamboo_config::paths::path_to_display_string(&canonical);
        assert_eq!(
            body["session"]["workspace_path"].as_str(),
            Some(canonical_display.as_str())
        );
        let session = state
            .storage
            .load_session(session_id)
            .await
            .expect("load")
            .expect("session");
        assert_eq!(
            session.workspace_path_meta().as_deref(),
            Some(canonical_display.as_str())
        );
        assert_eq!(
            bamboo_agent_core::workspace_state::get_workspace(session_id).as_deref(),
            Some(canonical.as_path())
        );
        let snapshot = session.prompt_snapshot.expect("prompt snapshot");
        assert!(snapshot
            .workspace_context
            .as_deref()
            .is_some_and(|value| value.contains("Binding status: registered")));
        assert_eq!(
            snapshot
                .effective_system_prompt
                .matches("BAMBOO_WORKSPACE_CONTEXT_START")
                .count(),
            1
        );
    }

    #[actix_web::test]
    async fn same_project_identity_is_stable_across_root_session_workspaces_and_apis() {
        let state = new_state().await;
        let first_workspace = tempdir().expect("first workspace");
        let second_workspace = tempdir().expect("second workspace");
        let project = state
            .project_store
            .create_with_bindings(
                "Multi-workspace Project",
                None,
                vec![
                    bamboo_domain::WorkspaceBinding {
                        path: first_workspace.path().to_string_lossy().into_owned(),
                        label: Some("first".to_string()),
                        git_common_dir: None,
                    },
                    bamboo_domain::WorkspaceBinding {
                        path: second_workspace.path().to_string_lossy().into_owned(),
                        label: Some("second".to_string()),
                        git_common_dir: None,
                    },
                ],
            )
            .expect("Project");
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let mut created = Vec::new();
        for (title, workspace) in [
            ("First root", first_workspace.path()),
            ("Second root", second_workspace.path()),
        ] {
            let canonical_workspace = std::fs::canonicalize(workspace)
                .expect("canonical workspace")
                .to_string_lossy()
                .into_owned();
            let response = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri("/api/v1/sessions")
                    .set_json(serde_json::json!({
                        "title": title,
                        "project_id": project.id,
                        "workspace_path": workspace,
                    }))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::CREATED);
            let body: Value = test::read_body_json(response).await;
            assert_eq!(body["session"]["project_id"], project.id.as_str());
            assert_eq!(body["session"]["workspace_path"], canonical_workspace);
            created.push((
                body["session"]["id"]
                    .as_str()
                    .expect("session id")
                    .to_string(),
                canonical_workspace,
            ));
        }

        let list = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/sessions")
                .to_request(),
        )
        .await;
        let list: Value = test::read_body_json(list).await;
        for (session_id, workspace) in &created {
            let listed = list["sessions"]
                .as_array()
                .expect("sessions")
                .iter()
                .find(|entry| entry["id"] == session_id.as_str())
                .expect("session in list");
            assert_eq!(listed["project_id"], project.id.as_str());
            assert_eq!(listed["workspace_path"], workspace.as_str());

            let detail = test::call_service(
                &app,
                test::TestRequest::get()
                    .uri(&format!("/api/v1/sessions/{session_id}"))
                    .to_request(),
            )
            .await;
            let detail: Value = test::read_body_json(detail).await;
            assert_eq!(detail["session"]["project_id"], project.id.as_str());
            assert_eq!(detail["session"]["workspace_path"], workspace.as_str());
        }
    }

    #[actix_web::test]
    async fn invalid_workspace_inputs_return_400_without_creating_sessions() {
        let state = new_state().await;
        let fixture = tempdir().expect("workspace fixture");
        let missing = fixture.path().join("missing");
        let file = fixture.path().join("file.txt");
        std::fs::write(&file, "not a directory").expect("file fixture");
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        for (path, code) in [
            (missing.to_string_lossy().to_string(), "workspace_not_found"),
            (
                file.to_string_lossy().to_string(),
                "workspace_not_directory",
            ),
        ] {
            let response = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri("/api/v1/sessions")
                    .set_json(serde_json::json!({
                        "title": "Must not exist",
                        "workspace_path": path,
                    }))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let body: Value = test::read_body_json(response).await;
            assert_eq!(body["error"]["code"], code);
        }

        let list = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/sessions")
                .to_request(),
        )
        .await;
        let list: Value = test::read_body_json(list).await;
        assert_eq!(list["sessions"].as_array().map(Vec::len), Some(0));
    }
}
