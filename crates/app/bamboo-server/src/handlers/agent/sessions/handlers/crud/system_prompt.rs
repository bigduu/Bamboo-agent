use actix_web::{web, HttpResponse, Result};

use crate::app_state::AppState;

use super::super::super::types::SessionSystemPromptResponse;

fn project_snapshot_error(
    error: bamboo_engine::project_context::ProjectContextError,
) -> HttpResponse {
    crate::project_context::project_context_error_response(error)
}

/// `GET /api/v1/sessions/{session_id}/system-prompt`
pub async fn get_system_prompt_snapshot(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let session_id = path.into_inner();

    let session = { bamboo_engine::read_cached_session(&state.sessions, &session_id) };

    let mut session = match session {
        Some(session) => session,
        None => match state
            .storage
            .load_session(&session_id)
            .await
            .map_err(|error| {
                crate::error::json_internal_server_error(format!("Failed to load session: {error}"))
            })? {
            Some(session) => session,
            None => {
                return Ok(HttpResponse::NotFound().json(serde_json::json!({
                    "error": crate::error::error_value("Session not found"),
                    "session_id": session_id
                })));
            }
        },
    };

    // A schedule or child created with execution disabled may never have
    // entered the runner, so its persisted system message can legitimately
    // predate Project prompt injection. Resolve a temporary clone here to make
    // the snapshot contract independent of execution. The read-only resolver
    // does not update runtime workspace state or persist this temporary view.
    if let Err(error) = state
        .project_context_resolver
        .refresh_session_prompt_read_only(&mut session)
        .await
    {
        return Ok(project_snapshot_error(error));
    }

    let default_prompt =
        bamboo_engine::prompt_defaults::read_global_default_system_prompt_template();
    let snapshot = bamboo_engine::session_app::system_prompt::build_system_prompt_snapshot(
        &session,
        &default_prompt,
    );

    Ok(HttpResponse::Ok().json(SessionSystemPromptResponse {
        session_id: session_id.to_string(),
        base_system_prompt: snapshot.base_system_prompt,
        enhancement_prompt: snapshot.enhancement_prompt,
        project_context: snapshot.project_context,
        workspace_context: snapshot.workspace_context,
        instruction_context: snapshot.instruction_context,
        env_context: snapshot.env_context,
        skill_context: snapshot.skill_context,
        tool_guide_context: snapshot.tool_guide_context,
        dream_notebook: snapshot.dream_notebook,
        session_memory_note: snapshot.session_memory_note,
        project_memory_index: snapshot.project_memory_index,
        relevant_durable_memories: snapshot.relevant_durable_memories,
        project_dream: snapshot.project_dream,
        global_dream_fallback: snapshot.global_dream_fallback,
        prompt_memory_observability: snapshot.prompt_memory_observability,
        external_memory: snapshot.external_memory,
        task_list: snapshot.task_list,
        effective_system_prompt: snapshot.effective_system_prompt,
    }))
}

#[cfg(test)]
mod tests {
    use super::get_system_prompt_snapshot;
    use actix_web::{body::to_bytes, http::StatusCode, web};
    use bamboo_agent_core::{Message, Role, Session, SessionKind};

    fn publish_test_env_context() {
        let mut config = bamboo_llm::Config::default();
        config.env_vars = vec![bamboo_config::EnvVarEntry {
            name: "TEST_TOOL_TOKEN".to_string(),
            value: "hidden-value".to_string(),
            secret: true,
            value_encrypted: None,
            credential_ref: None,
            configured: true,
            description: Some("Snapshot test token".to_string()),
        }];
        config.publish_env_vars();
    }

    #[actix_web::test]
    async fn handler_returns_project_dream_snapshot_from_persisted_session() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let state = crate::app_state::AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state should initialize");
        let state = web::Data::new(state);

        let mut session = Session::new("session-handler-project-dream", "gpt-5");
        session.add_message(Message::system(
            "Base prompt\n\n<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n## External Memory (Persistent)\n\n### Project Dream Summary\n````md\nHandler project dream content\n````\n\n### Session Memory Note (markdown)\n````md\nHandler session note content\n````\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->",
        ));

        state
            .storage
            .save_session(&session)
            .await
            .expect("save session");

        let response = get_system_prompt_snapshot(
            state.clone(),
            web::Path::from("session-handler-project-dream".to_string()),
        )
        .await
        .expect("handler should succeed");
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body())
            .await
            .expect("response body should read");
        let snapshot: serde_json::Value =
            serde_json::from_slice(&body).expect("response should deserialize");

        assert_eq!(snapshot["session_id"], "session-handler-project-dream");
        assert_eq!(snapshot["dream_notebook"], "Handler project dream content");
        assert_eq!(
            snapshot["session_memory_note"],
            "Handler session note content"
        );
        assert!(snapshot["external_memory"]
            .as_str()
            .is_some_and(|value| value.contains("### Project Dream Summary")));
        assert!(snapshot["external_memory"]
            .as_str()
            .is_some_and(|value| value.contains("Handler project dream content")));
    }

    #[actix_web::test]
    async fn system_prompt_get_does_not_materialize_fallback_workspace() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let state = crate::app_state::AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state");
        let session_id = format!("prompt-read-only-{}", uuid::Uuid::new_v4());
        let mut session = Session::new(&session_id, "gpt-5");
        session
            .metadata
            .insert("base_system_prompt".to_string(), "Base prompt".to_string());
        state
            .storage
            .save_session(&session)
            .await
            .expect("save session");
        let candidate =
            bamboo_engine::project_context::ProjectContextResolver::resolve_workspace_candidate(
                &session, None,
            )
            .expect("resolve candidate")
            .expect("workspace-root fallback");
        assert!(
            !candidate.exists(),
            "pure preflight must not create the fallback directory"
        );
        let state = web::Data::new(state);

        let response = get_system_prompt_snapshot(state, web::Path::from(session_id.clone()))
            .await
            .expect("snapshot");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            !candidate.exists(),
            "GET system-prompt must leave the fallback directory absent"
        );
        assert!(bamboo_agent_core::workspace_state::peek_workspace(&session_id).is_none());
    }

    #[actix_web::test]
    async fn unexecuted_schedule_and_child_snapshots_materialize_project_context_read_only() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let workspace = temp_dir.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let state = crate::app_state::AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state should initialize");
        let project = state
            .project_store
            .create_with_bindings(
                "Prompt Snapshot Project",
                None,
                vec![bamboo_domain::WorkspaceBinding {
                    path: workspace.to_string_lossy().into_owned(),
                    label: None,
                    git_common_dir: None,
                }],
            )
            .expect("create Project");
        let state = web::Data::new(state);

        for (session_id, kind, schedule_id) in [
            ("unexecuted-schedule", SessionKind::Root, Some("schedule-1")),
            ("unexecuted-child", SessionKind::Child, None),
        ] {
            let mut session = match kind {
                SessionKind::Child => {
                    Session::new_child(session_id, "unexecuted-root", "gpt-5", "Unexecuted child")
                }
                SessionKind::Root => Session::new(session_id, "gpt-5"),
            };
            session.set_project_id_meta(project.id.to_string());
            session.set_workspace_path_meta(workspace.to_string_lossy().into_owned());
            if let Some(schedule_id) = schedule_id {
                session.metadata.insert(
                    "created_by_schedule_id".to_string(),
                    schedule_id.to_string(),
                );
            }
            session
                .metadata
                .insert("base_system_prompt".to_string(), "Base prompt".to_string());
            state
                .storage
                .save_session(&session)
                .await
                .expect("save unexecuted session");

            let response =
                get_system_prompt_snapshot(state.clone(), web::Path::from(session_id.to_string()))
                    .await
                    .expect("snapshot request");
            assert_eq!(response.status(), StatusCode::OK);
            let body = to_bytes(response.into_body()).await.expect("response body");
            let snapshot: serde_json::Value = serde_json::from_slice(&body).expect("snapshot JSON");
            let effective = snapshot["effective_system_prompt"]
                .as_str()
                .expect("effective prompt");
            assert_eq!(effective.matches("BAMBOO_PROJECT_CONTEXT_START").count(), 1);
            assert_eq!(
                effective.matches("BAMBOO_WORKSPACE_CONTEXT_START").count(),
                1
            );
            assert!(snapshot["project_context"]
                .as_str()
                .is_some_and(|value| value.contains(project.id.as_str())));
            assert!(snapshot["workspace_context"]
                .as_str()
                .is_some_and(|value| value.contains(workspace.to_string_lossy().as_ref())));

            let persisted = state
                .storage
                .load_session(session_id)
                .await
                .expect("load persisted session")
                .expect("persisted session");
            assert!(
                !persisted
                    .messages
                    .iter()
                    .any(|message| matches!(message.role, Role::System)),
                "GET must not persist the temporary prompt view"
            );
            assert!(
                persisted.prompt_snapshot.is_none(),
                "GET must not persist the temporary prompt snapshot"
            );
        }
    }

    #[actix_web::test]
    async fn snapshot_fails_closed_for_invalid_identity_and_cross_project_workspace() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let workspace = temp_dir.path().join("foreign-workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let state = crate::app_state::AppState::new(temp_dir.path().to_path_buf())
            .await
            .expect("app state should initialize");
        let assigned_project = state
            .project_store
            .create("Assigned Project", None)
            .expect("assigned Project");
        let _workspace_owner = state
            .project_store
            .create_with_bindings(
                "Workspace Owner",
                None,
                vec![bamboo_domain::WorkspaceBinding {
                    path: workspace.to_string_lossy().into_owned(),
                    label: None,
                    git_common_dir: None,
                }],
            )
            .expect("workspace owner");
        let state = web::Data::new(state);

        let mut invalid = Session::new("invalid-project-snapshot", "gpt-5");
        invalid.set_project_id_meta("../invalid".to_string());
        state
            .storage
            .save_session(&invalid)
            .await
            .expect("save invalid session");
        let response =
            get_system_prompt_snapshot(state.clone(), web::Path::from(invalid.id.clone()))
                .await
                .expect("invalid snapshot response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body()).await.unwrap()).unwrap();
        assert_eq!(body["error"]["code"], "invalid_project_identity");

        let mut conflict = Session::new("conflicting-project-snapshot", "gpt-5");
        conflict.set_project_id_meta(assigned_project.id.to_string());
        conflict.set_workspace_path_meta(workspace.to_string_lossy().into_owned());
        state
            .storage
            .save_session(&conflict)
            .await
            .expect("save conflicting session");
        let response =
            get_system_prompt_snapshot(state.clone(), web::Path::from(conflict.id.clone()))
                .await
                .expect("conflict snapshot response");
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body()).await.unwrap()).unwrap();
        assert_eq!(body["error"]["code"], "project_workspace_conflict");

        let mut missing = Session::new("missing-project-path-snapshot", "gpt-5");
        missing.set_project_id_meta(assigned_project.id.to_string());
        state
            .storage
            .save_session(&missing)
            .await
            .expect("save missing-path session");
        let response = get_system_prompt_snapshot(state, web::Path::from(missing.id.clone()))
            .await
            .expect("missing-path snapshot response");
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body()).await.unwrap()).unwrap();
        assert_eq!(body["error"]["code"], "project_path_missing");
        assert_eq!(body["project_id"], assigned_project.id.to_string());
    }

    #[test]
    fn snapshot_extracts_generated_sections_workspace_and_env_context() {
        let _lock = bamboo_infrastructure::test_support::env_cache_lock_acquire();
        publish_test_env_context();

        let root = tempfile::tempdir().expect("temp dir");
        let workspace = root.path().join("workspace");
        std::fs::create_dir_all(root.path().join(".git")).expect("git marker");
        std::fs::create_dir_all(&workspace).expect("workspace dir");
        std::fs::write(root.path().join("AGENTS.md"), "Snapshot instruction policy")
            .expect("agents file");

        let mut session = Session::new("session-1", "gpt-5");
        session
            .metadata
            .insert("base_system_prompt".to_string(), "Base prompt".to_string());
        session
            .metadata
            .insert("enhance_prompt".to_string(), "Extra guidance".to_string());
        session.metadata.insert(
            "workspace_path".to_string(),
            workspace.to_string_lossy().to_string(),
        );

        let workspace_context =
            crate::app_state::build_workspace_prompt_context(workspace.to_string_lossy().as_ref())
                .expect("workspace context");
        let instruction_context =
            bamboo_engine::context::instruction::build_instruction_prompt_context(
                workspace.to_string_lossy().as_ref(),
            )
            .expect("instruction context");
        // Build env context directly from the cache we just published, with a
        // retry to handle concurrent tests that may overwrite the global cache.
        let env_context = (|| -> Option<String> {
            for _ in 0..10 {
                publish_test_env_context();
                if let Some(ctx) = crate::app_state::build_env_prompt_context() {
                    return Some(ctx);
                }
            }
            None
        })()
        .expect("env context should exist for snapshot test");
        session.add_message(Message::system(format!(
            "Base prompt\n\nExtra guidance\n\n{workspace_context}\n\n{instruction_context}\n\n{env_context}\n\n<!-- BAMBOO_SKILL_CONTEXT_START -->\n## Skill System\n\nSkill details\n<!-- BAMBOO_SKILL_CONTEXT_END -->\n\n<!-- BAMBOO_TOOL_GUIDE_START -->\n## Tool Usage Guidelines\n\nGuide details\n<!-- BAMBOO_TOOL_GUIDE_END -->\n\n<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n## External Memory (Persistent)\n\nMemory details\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->\n\n<!-- BAMBOO_TASK_LIST_START -->\n## Current Task List:\n- [ ] item\n<!-- BAMBOO_TASK_LIST_END -->"
        )));

        let default_prompt =
            bamboo_engine::prompt_defaults::read_global_default_system_prompt_template();
        let snapshot = bamboo_engine::session_app::system_prompt::build_system_prompt_snapshot(
            &session,
            &default_prompt,
        );
        assert_eq!(snapshot.base_system_prompt, "Base prompt");
        assert_eq!(
            snapshot.enhancement_prompt.as_deref(),
            Some("Extra guidance")
        );
        assert!(snapshot
            .workspace_context
            .as_deref()
            .is_some_and(|value| value.contains(workspace.to_string_lossy().as_ref())));
        assert!(snapshot
            .instruction_context
            .as_deref()
            .is_some_and(|value| value.contains("Snapshot instruction policy")));
        assert!(snapshot
            .env_context
            .as_deref()
            .is_some_and(|value| value.contains(
                "environment variables were explicitly configured by the user inside Bodhi"
            )));
        assert!(snapshot
            .env_context
            .as_deref()
            .is_some_and(|value| value.contains("Bash/tool processes launched by Bodhi")));
        assert!(snapshot
            .skill_context
            .as_deref()
            .is_some_and(|value| value.contains("Skill details")));
        assert!(snapshot
            .tool_guide_context
            .as_deref()
            .is_some_and(|value| value.contains("Guide details")));
        assert!(snapshot
            .external_memory
            .as_deref()
            .is_some_and(|value| value.contains("Memory details")));
        assert!(snapshot
            .task_list
            .as_deref()
            .is_some_and(|value| value.contains("Current Task List")));
    }

    #[test]
    fn snapshot_extracts_workspace_from_legacy_unwrapped_context() {
        let mut session = Session::new("session-legacy", "gpt-5");
        let guidance = crate::app_state::workspace_prompt_guidance();
        session.add_message(Message::system(format!(
            "Base prompt\n\nExtra guidance\n\nWorkspace path: /tmp/legacy-workspace\n{guidance}\n\n<!-- BAMBOO_SKILL_CONTEXT_START -->\n## Skill System\n\nSkill details\n<!-- BAMBOO_SKILL_CONTEXT_END -->"
        )));

        let default_prompt =
            bamboo_engine::prompt_defaults::read_global_default_system_prompt_template();
        let snapshot = bamboo_engine::session_app::system_prompt::build_system_prompt_snapshot(
            &session,
            &default_prompt,
        );
        assert_eq!(snapshot.base_system_prompt, "Base prompt\n\nExtra guidance");
        assert!(snapshot.enhancement_prompt.is_none());
        assert!(snapshot
            .workspace_context
            .as_deref()
            .is_some_and(|value| value.contains("/tmp/legacy-workspace")));
    }
}
