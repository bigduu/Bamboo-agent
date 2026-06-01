use actix_web::{web, HttpResponse, Result};

use crate::app_state::AppState;

use super::super::super::types::SessionSystemPromptResponse;

/// `GET /api/v1/sessions/{session_id}/system-prompt`
pub async fn get_system_prompt_snapshot(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let session_id = path.into_inner();

    let session = {
        let sessions = state.sessions.read().await;
        sessions.get(&session_id).cloned()
    };

    let session = match session {
        Some(session) => session,
        None => match state
            .storage
            .load_session(&session_id)
            .await
            .map_err(|error| {
                actix_web::error::ErrorInternalServerError(format!(
                    "Failed to load session: {error}"
                ))
            })? {
            Some(session) => session,
            None => {
                return Ok(HttpResponse::NotFound().json(serde_json::json!({
                    "error": "Session not found",
                    "session_id": session_id
                })));
            }
        },
    };

    let default_prompt = crate::prompt_defaults::read_global_default_system_prompt_template();
    let snapshot =
        crate::session_app::system_prompt::build_system_prompt_snapshot(&session, &default_prompt);

    Ok(HttpResponse::Ok().json(SessionSystemPromptResponse {
        session_id: session_id.to_string(),
        base_system_prompt: snapshot.base_system_prompt,
        enhancement_prompt: snapshot.enhancement_prompt,
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
    use bamboo_agent_core::{Message, Session};

    fn publish_test_env_context() {
        let config = bamboo_infrastructure::Config {
            env_vars: vec![bamboo_infrastructure::EnvVarEntry {
                name: "TEST_TOOL_TOKEN".to_string(),
                value: "hidden-value".to_string(),
                secret: true,
                value_encrypted: None,
                description: Some("Snapshot test token".to_string()),
            }],
            ..bamboo_infrastructure::Config::default()
        };
        config.publish_env_vars();
    }

    #[actix_web::test]
    async fn handler_returns_project_dream_snapshot_from_persisted_session() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        bamboo_infrastructure::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
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

    #[test]
    fn snapshot_extracts_generated_sections_workspace_and_env_context() {
        let _lock = bamboo_infrastructure::test_support::env_cache_lock_acquire();
        publish_test_env_context();

        let root = tempfile::tempdir().expect("temp dir");
        let workspace = root.path().join("workspace");
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

        let default_prompt = crate::prompt_defaults::read_global_default_system_prompt_template();
        let snapshot = crate::session_app::system_prompt::build_system_prompt_snapshot(
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

        let default_prompt = crate::prompt_defaults::read_global_default_system_prompt_template();
        let snapshot = crate::session_app::system_prompt::build_system_prompt_snapshot(
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
