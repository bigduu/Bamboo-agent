use actix_web::{web, HttpResponse, Result};

use crate::agent::core::{parse_prompt_external_memory_sections, Role, Session};
use crate::server::app_state::{workspace_prompt_guidance, AppState};

use super::super::super::types::SessionSystemPromptResponse;

const SKILL_CONTEXT_START_MARKER: &str = "<!-- BAMBOO_SKILL_CONTEXT_START -->";
const SKILL_CONTEXT_END_MARKER: &str = "<!-- BAMBOO_SKILL_CONTEXT_END -->";
const TOOL_GUIDE_START_MARKER: &str = "<!-- BAMBOO_TOOL_GUIDE_START -->";
const TOOL_GUIDE_END_MARKER: &str = "<!-- BAMBOO_TOOL_GUIDE_END -->";
const EXTERNAL_MEMORY_START_MARKER: &str = "<!-- BAMBOO_EXTERNAL_MEMORY_START -->";
const EXTERNAL_MEMORY_END_MARKER: &str = "<!-- BAMBOO_EXTERNAL_MEMORY_END -->";
const TASK_LIST_START_MARKER: &str = "<!-- BAMBOO_TASK_LIST_START -->";
const TASK_LIST_END_MARKER: &str = "<!-- BAMBOO_TASK_LIST_END -->";
const LEGACY_TODO_LIST_START_MARKER: &str = "<!-- BAMBOO_TODO_LIST_START -->";
const LEGACY_TODO_LIST_END_MARKER: &str = "<!-- BAMBOO_TODO_LIST_END -->";
const WORKSPACE_CONTEXT_START_MARKER: &str =
    crate::server::app_state::WORKSPACE_CONTEXT_START_MARKER;
const WORKSPACE_CONTEXT_END_MARKER: &str = crate::server::app_state::WORKSPACE_CONTEXT_END_MARKER;
const WORKSPACE_CONTEXT_PREFIX: &str = crate::server::app_state::WORKSPACE_CONTEXT_PREFIX;
const INSTRUCTION_CONTEXT_START_MARKER: &str =
    crate::server::instruction_layer::INSTRUCTION_CONTEXT_START_MARKER;
const INSTRUCTION_CONTEXT_END_MARKER: &str =
    crate::server::instruction_layer::INSTRUCTION_CONTEXT_END_MARKER;
const ENV_CONTEXT_START_MARKER: &str = crate::server::app_state::ENV_CONTEXT_START_MARKER;
const ENV_CONTEXT_END_MARKER: &str = crate::server::app_state::ENV_CONTEXT_END_MARKER;

fn global_default_prompt() -> String {
    crate::server::prompt_defaults::read_global_default_system_prompt_template()
}

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

    Ok(HttpResponse::Ok().json(build_system_prompt_response(&session_id, &session)))
}

fn build_system_prompt_response(
    session_id: &str,
    session: &Session,
) -> SessionSystemPromptResponse {
    if let Some(snapshot) = crate::agent::loop_module::runner::read_prompt_snapshot(session) {
        return SessionSystemPromptResponse {
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
        };
    }

    let effective_system_prompt = resolve_effective_system_prompt(session);
    let skill_context = extract_wrapped_section(
        &effective_system_prompt,
        SKILL_CONTEXT_START_MARKER,
        SKILL_CONTEXT_END_MARKER,
    );
    let tool_guide_context = extract_wrapped_section(
        &effective_system_prompt,
        TOOL_GUIDE_START_MARKER,
        TOOL_GUIDE_END_MARKER,
    );
    let external_memory = extract_wrapped_section(
        &effective_system_prompt,
        EXTERNAL_MEMORY_START_MARKER,
        EXTERNAL_MEMORY_END_MARKER,
    );
    let external_memory_parts = parse_prompt_external_memory_sections(external_memory.as_deref());
    let task_list = extract_wrapped_section(
        &effective_system_prompt,
        TASK_LIST_START_MARKER,
        TASK_LIST_END_MARKER,
    )
    .or_else(|| {
        extract_wrapped_section(
            &effective_system_prompt,
            LEGACY_TODO_LIST_START_MARKER,
            LEGACY_TODO_LIST_END_MARKER,
        )
    });

    let prompt_without_generated_sections = strip_generated_sections(&effective_system_prompt);
    let (prompt_without_workspace, workspace_from_prompt) =
        split_workspace_context(&prompt_without_generated_sections);
    let (prompt_without_instruction, instruction_from_prompt) =
        split_instruction_context(&prompt_without_workspace);
    let (prompt_without_env, env_context) = split_env_context(&prompt_without_instruction);

    let base_system_prompt = metadata_value(session, "base_system_prompt").unwrap_or_else(|| {
        let derived = prompt_without_env.trim();
        if derived.is_empty() {
            global_default_prompt()
        } else {
            derived.to_string()
        }
    });

    let enhancement_prompt = metadata_value(session, "enhance_prompt")
        .or_else(|| derive_enhancement_prompt_legacy(&base_system_prompt, &prompt_without_env));

    let workspace_context = metadata_value(session, "workspace_path")
        .and_then(|workspace_path| {
            crate::server::app_state::build_workspace_prompt_context(&workspace_path)
        })
        .or(workspace_from_prompt);

    let instruction_context = metadata_value(session, "workspace_path")
        .and_then(|workspace_path| {
            crate::server::instruction_layer::build_instruction_prompt_context(&workspace_path)
        })
        .or(instruction_from_prompt);

    SessionSystemPromptResponse {
        session_id: session_id.to_string(),
        base_system_prompt,
        enhancement_prompt,
        workspace_context,
        instruction_context,
        env_context,
        skill_context,
        tool_guide_context,
        dream_notebook: external_memory_parts.dream_notebook,
        session_memory_note: external_memory_parts.session_memory_note,
        project_memory_index: external_memory_parts.project_memory_index,
        relevant_durable_memories: external_memory_parts.relevant_durable_memories,
        project_dream: external_memory_parts.project_dream,
        global_dream_fallback: external_memory_parts.global_dream_fallback,
        prompt_memory_observability: None,
        external_memory,
        task_list,
        effective_system_prompt,
    }
}

fn resolve_effective_system_prompt(session: &Session) -> String {
    let system_message = session
        .messages
        .iter()
        .find(|message| matches!(message.role, Role::System))
        .map(|message| message.content.trim().to_string())
        .filter(|content| !content.is_empty());

    system_message
        .or_else(|| metadata_value(session, "base_system_prompt"))
        .unwrap_or_else(global_default_prompt)
}

fn metadata_value(session: &Session, key: &str) -> Option<String> {
    session
        .metadata
        .get(key)
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn extract_wrapped_section(prompt: &str, start_marker: &str, end_marker: &str) -> Option<String> {
    let start_idx = prompt.find(start_marker)?;
    let section_start = start_idx + start_marker.len();
    let end_rel_idx = prompt[section_start..].find(end_marker)?;
    let end_idx = section_start + end_rel_idx;
    let section = prompt[section_start..end_idx].trim();
    if section.is_empty() {
        None
    } else {
        Some(section.to_string())
    }
}

fn strip_wrapped_sections(prompt: &str, start_marker: &str, end_marker: &str) -> String {
    let mut current = prompt.to_string();

    loop {
        let Some(start_idx) = current.find(start_marker) else {
            break;
        };
        let search_from = start_idx + start_marker.len();
        let Some(end_rel_idx) = current[search_from..].find(end_marker) else {
            break;
        };
        let end_idx = search_from + end_rel_idx + end_marker.len();

        let before = current[..start_idx].trim_end();
        let after = current[end_idx..].trim_start();
        current = match (before.is_empty(), after.is_empty()) {
            (true, true) => String::new(),
            (true, false) => after.to_string(),
            (false, true) => before.to_string(),
            (false, false) => format!("{before}\n\n{after}"),
        };
    }

    current
}

fn strip_generated_sections(prompt: &str) -> String {
    let prompt = strip_wrapped_sections(
        prompt,
        EXTERNAL_MEMORY_START_MARKER,
        EXTERNAL_MEMORY_END_MARKER,
    );
    let prompt = strip_wrapped_sections(&prompt, TASK_LIST_START_MARKER, TASK_LIST_END_MARKER);
    let prompt = strip_wrapped_sections(
        &prompt,
        LEGACY_TODO_LIST_START_MARKER,
        LEGACY_TODO_LIST_END_MARKER,
    );
    let prompt = strip_wrapped_sections(
        &prompt,
        SKILL_CONTEXT_START_MARKER,
        SKILL_CONTEXT_END_MARKER,
    );
    let prompt = strip_wrapped_sections(&prompt, TOOL_GUIDE_START_MARKER, TOOL_GUIDE_END_MARKER);
    strip_wrapped_sections(
        &prompt,
        INSTRUCTION_CONTEXT_START_MARKER,
        INSTRUCTION_CONTEXT_END_MARKER,
    )
}

fn split_workspace_context(prompt: &str) -> (String, Option<String>) {
    let marker_workspace = extract_wrapped_section(
        prompt,
        WORKSPACE_CONTEXT_START_MARKER,
        WORKSPACE_CONTEXT_END_MARKER,
    );
    if marker_workspace.is_some() {
        let stripped = strip_wrapped_sections(
            prompt,
            WORKSPACE_CONTEXT_START_MARKER,
            WORKSPACE_CONTEXT_END_MARKER,
        );
        return (stripped.trim().to_string(), marker_workspace);
    }
    split_legacy_workspace_context(prompt)
}

fn split_instruction_context(prompt: &str) -> (String, Option<String>) {
    let instruction_context = extract_wrapped_section(
        prompt,
        INSTRUCTION_CONTEXT_START_MARKER,
        INSTRUCTION_CONTEXT_END_MARKER,
    );
    if instruction_context.is_some() {
        let stripped = strip_wrapped_sections(
            prompt,
            INSTRUCTION_CONTEXT_START_MARKER,
            INSTRUCTION_CONTEXT_END_MARKER,
        );
        return (stripped.trim().to_string(), instruction_context);
    }
    (prompt.trim().to_string(), None)
}

fn split_env_context(prompt: &str) -> (String, Option<String>) {
    let env_context =
        extract_wrapped_section(prompt, ENV_CONTEXT_START_MARKER, ENV_CONTEXT_END_MARKER);
    if env_context.is_some() {
        let stripped =
            strip_wrapped_sections(prompt, ENV_CONTEXT_START_MARKER, ENV_CONTEXT_END_MARKER);
        return (stripped.trim().to_string(), env_context);
    }
    (prompt.trim().to_string(), None)
}

fn split_legacy_workspace_context(prompt: &str) -> (String, Option<String>) {
    let Some(start_idx) = prompt.find(WORKSPACE_CONTEXT_PREFIX) else {
        return (prompt.trim().to_string(), None);
    };
    let guidance = workspace_prompt_guidance();
    let end_idx = if let Some(guidance_rel_idx) = prompt[start_idx..].find(&guidance) {
        start_idx + guidance_rel_idx + guidance.len()
    } else {
        prompt.len()
    };

    let workspace_context = prompt[start_idx..end_idx].trim().to_string();
    let before = prompt[..start_idx].trim_end();
    let after = prompt[end_idx..].trim_start();
    let stripped = match (before.is_empty(), after.is_empty()) {
        (true, true) => String::new(),
        (true, false) => after.to_string(),
        (false, true) => before.to_string(),
        (false, false) => format!("{before}\n\n{after}"),
    };

    let workspace_context = if workspace_context.is_empty() {
        None
    } else {
        Some(workspace_context)
    };

    (stripped, workspace_context)
}

fn derive_enhancement_prompt_legacy(
    base_system_prompt: &str,
    prompt_without_workspace: &str,
) -> Option<String> {
    let base = base_system_prompt.trim();
    let prompt = prompt_without_workspace.trim();

    if base.is_empty() || prompt.is_empty() || prompt == base {
        return None;
    }

    if !prompt.starts_with(base) {
        return None;
    }

    let enhancement = prompt[base.len()..].trim();
    if enhancement.is_empty() {
        None
    } else {
        Some(enhancement.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::{build_system_prompt_response, get_system_prompt_snapshot};
    use crate::agent::core::{Message, Session};
    use actix_web::{body::to_bytes, http::StatusCode, web};

    fn publish_test_env_context() {
        let config = crate::core::Config {
            env_vars: vec![crate::core::EnvVarEntry {
                name: "TEST_TOOL_TOKEN".to_string(),
                value: "hidden-value".to_string(),
                secret: true,
                value_encrypted: None,
                description: Some("Snapshot test token".to_string()),
            }],
            ..crate::core::Config::default()
        };
        config.publish_env_vars();
    }

    #[actix_web::test]
    async fn handler_returns_project_dream_snapshot_from_persisted_session() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        crate::core::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        let state = crate::server::app_state::AppState::new(temp_dir.path().to_path_buf())
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
        let _lock = crate::test_support::env_cache_lock_acquire();
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

        let workspace_context = crate::server::app_state::build_workspace_prompt_context(
            workspace.to_string_lossy().as_ref(),
        )
        .expect("workspace context");
        let instruction_context =
            crate::server::instruction_layer::build_instruction_prompt_context(
                workspace.to_string_lossy().as_ref(),
            )
            .expect("instruction context");
        let env_context = crate::server::app_state::build_env_prompt_context()
            .expect("env context should exist for snapshot test");
        session.add_message(Message::system(format!(
            "Base prompt\n\nExtra guidance\n\n{workspace_context}\n\n{instruction_context}\n\n{env_context}\n\n<!-- BAMBOO_SKILL_CONTEXT_START -->\n## Skill System\n\nSkill details\n<!-- BAMBOO_SKILL_CONTEXT_END -->\n\n<!-- BAMBOO_TOOL_GUIDE_START -->\n## Tool Usage Guidelines\n\nGuide details\n<!-- BAMBOO_TOOL_GUIDE_END -->\n\n<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n## External Memory (Persistent)\n\nMemory details\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->\n\n<!-- BAMBOO_TASK_LIST_START -->\n## Current Task List:\n- [ ] item\n<!-- BAMBOO_TASK_LIST_END -->"
        )));

        let snapshot = build_system_prompt_response("session-1", &session);
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
    fn snapshot_extracts_dream_notebook_and_session_memory_note_from_external_memory() {
        let mut session = Session::new("session-memory-split", "gpt-5");
        session.add_message(Message::system(
            "Base prompt\n\n<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n## External Memory (Persistent)\n\n### Cross-session Dream Notebook (read-only)\n````md\nDream note content\n````\n\n### Session Memory Note (markdown)\n````md\nSession note content\n````\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->",
        ));

        let snapshot = build_system_prompt_response("session-memory-split", &session);
        assert_eq!(
            snapshot.dream_notebook.as_deref(),
            Some("Dream note content")
        );
        assert_eq!(
            snapshot.session_memory_note.as_deref(),
            Some("Session note content")
        );
        assert!(snapshot
            .external_memory
            .as_deref()
            .is_some_and(|value| value.contains("Dream note content")));
    }

    #[test]
    fn snapshot_extracts_project_dream_heading_from_external_memory() {
        let mut session = Session::new("session-memory-project-dream", "gpt-5");
        session.add_message(Message::system(
            "Base prompt\n\n<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n## External Memory (Persistent)\n\n### Project Dream Summary\n````md\nProject dream content\n````\n\n### Session Memory Note (markdown)\n````md\nSession note content\n````\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->",
        ));

        let snapshot = build_system_prompt_response("session-memory-project-dream", &session);
        assert_eq!(
            snapshot.dream_notebook.as_deref(),
            Some("Project dream content")
        );
        assert_eq!(
            snapshot.session_memory_note.as_deref(),
            Some("Session note content")
        );
    }

    #[test]
    fn snapshot_extracts_global_dream_fallback_heading_from_external_memory() {
        let mut session = Session::new("session-memory-fallback-dream", "gpt-5");
        session.add_message(Message::system(
            "Base prompt\n\n<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n## External Memory (Persistent)\n\n### Global Dream Summary (fallback)\n````md\nDream fallback content\n````\n\n### Session Memory Note (markdown)\n````md\nSession note content\n````\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->",
        ));

        let snapshot = build_system_prompt_response("session-memory-fallback-dream", &session);
        assert_eq!(
            snapshot.dream_notebook.as_deref(),
            Some("Dream fallback content")
        );
        assert_eq!(
            snapshot.session_memory_note.as_deref(),
            Some("Session note content")
        );
    }

    #[test]
    fn snapshot_extracts_multi_topic_session_memory_note_from_external_memory() {
        let mut session = Session::new("session-memory-topics", "gpt-5");
        session.add_message(Message::system(
            "Base prompt\n\n<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n## External Memory (Persistent)\n\n### Cross-session Dream Notebook (read-only)\n````md\nDream note content\n````\n\n### Session Memory Topic: `backend-api`\n````md\n/users and /orders finalized\n````\n\n### Session Memory Topic: `ui-copy`\n````md\nCTA wording approved\n````\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->",
        ));

        let snapshot = build_system_prompt_response("session-memory-topics", &session);
        assert_eq!(
            snapshot.dream_notebook.as_deref(),
            Some("Dream note content")
        );
        let merged = snapshot
            .session_memory_note
            .as_deref()
            .expect("session memory note should be merged from topic blocks");
        assert!(merged.contains("### Session Memory Topic: `backend-api`"));
        assert!(merged.contains("/users and /orders finalized"));
        assert!(merged.contains("### Session Memory Topic: `ui-copy`"));
        assert!(merged.contains("CTA wording approved"));
    }

    #[test]
    fn snapshot_preserves_truncated_topic_placeholder_in_multi_topic_memory_note() {
        let mut session = Session::new("session-memory-truncated-topic", "gpt-5");
        session.add_message(Message::system(
            "Base prompt\n\n<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n## External Memory (Persistent)\n\n### Session Memory Topic: `backend-api`\n````md\n_(truncated — use action=read topic=backend-api to view)_\n````\n\n### Session Memory Topic: `ui-copy`\n````md\nCTA wording approved\n````\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->",
        ));

        let snapshot = build_system_prompt_response("session-memory-truncated-topic", &session);
        let merged = snapshot
            .session_memory_note
            .as_deref()
            .expect("session memory note should be merged from topic blocks");
        assert!(merged.contains("### Session Memory Topic: `backend-api`"));
        assert!(merged.contains("_(truncated — use action=read topic=backend-api to view)_"));
        assert!(merged.contains("### Session Memory Topic: `ui-copy`"));
        assert!(merged.contains("CTA wording approved"));
    }

    #[test]
    fn snapshot_extracts_fine_grained_external_memory_sections() {
        let mut session = Session::new("session-memory-fine-grained", "gpt-5");
        session.add_message(Message::system(
            "Base prompt\n\n<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n## External Memory (Persistent)\n\n### Relevant Durable Memories\nTurn-specific historical memories shortlisted for the latest user request.\n- [active][project] Release rule\n  Summary: Use the release checklist.\n\n### Project Durable Memory Index\n````md\n# Bamboo Memory Index\n- memory entry\n````\n\n### Project Dream Summary\n````md\nProject dream content\n````\n\n### Session Memory Note (markdown)\n````md\nSession note content\n````\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->",
        ));

        let snapshot = build_system_prompt_response("session-memory-fine-grained", &session);
        assert!(snapshot
            .relevant_durable_memories
            .as_deref()
            .is_some_and(|value| value.contains("Release rule")));
        assert_eq!(
            snapshot.project_memory_index.as_deref(),
            Some("# Bamboo Memory Index\n- memory entry")
        );
        assert_eq!(
            snapshot.project_dream.as_deref(),
            Some("Project dream content")
        );
        assert!(snapshot.global_dream_fallback.is_none());
        assert_eq!(
            snapshot.dream_notebook.as_deref(),
            Some("Project dream content")
        );
    }

    #[test]
    fn snapshot_extracts_workspace_from_legacy_unwrapped_context() {
        let mut session = Session::new("session-legacy", "gpt-5");
        let guidance = crate::server::app_state::workspace_prompt_guidance();
        session.add_message(Message::system(format!(
            "Base prompt\n\nExtra guidance\n\nWorkspace path: /tmp/legacy-workspace\n{guidance}\n\n<!-- BAMBOO_SKILL_CONTEXT_START -->\n## Skill System\n\nSkill details\n<!-- BAMBOO_SKILL_CONTEXT_END -->"
        )));

        let snapshot = build_system_prompt_response("session-legacy", &session);
        assert_eq!(snapshot.base_system_prompt, "Base prompt\n\nExtra guidance");
        assert!(snapshot.enhancement_prompt.is_none());
        assert!(snapshot
            .workspace_context
            .as_deref()
            .is_some_and(|value| value.contains("/tmp/legacy-workspace")));
    }

    #[test]
    fn snapshot_uses_default_prompt_when_session_has_no_system_message() {
        let session = Session::new("session-1", "gpt-5");
        let snapshot = build_system_prompt_response("session-1", &session);
        let expected = crate::server::prompt_defaults::read_global_default_system_prompt_template();

        assert_eq!(snapshot.base_system_prompt, expected);
        assert_eq!(snapshot.effective_system_prompt, expected);
        assert!(snapshot.enhancement_prompt.is_none());
    }

    #[test]
    fn snapshot_prefers_shared_prompt_snapshot_metadata_when_present() {
        let mut session = Session::new("session-shared", "gpt-5");
        session.add_message(Message::system("legacy system prompt"));
        session.metadata.insert(
            "runtime_prompt_snapshot".to_string(),
            serde_json::to_string(&crate::agent::loop_module::runner::PromptSnapshot {
                base_system_prompt: "Shared base".to_string(),
                enhancement_prompt: Some("Shared enhancement".to_string()),
                workspace_context: Some("Shared workspace".to_string()),
                instruction_context: Some("Shared instruction".to_string()),
                env_context: Some("Shared env".to_string()),
                skill_context: Some("Shared skill".to_string()),
                tool_guide_context: Some("Shared tool guide".to_string()),
                dream_notebook: Some("Dream block".to_string()),
                session_memory_note: Some("Session note block".to_string()),
                project_memory_index: Some("Shared project index".to_string()),
                relevant_durable_memories: Some("Shared relevant memories".to_string()),
                project_dream: Some("Shared project dream".to_string()),
                global_dream_fallback: Some("Shared global fallback".to_string()),
                prompt_memory_observability: Some(crate::agent::core::PromptMemoryObservability {
                    project_prompt_injection_enabled: true,
                    relevant_recall_enabled: true,
                    relevant_recall_rerank_enabled: false,
                    project_first_dream_enabled: true,
                    latest_user_query_present: true,
                    resolved_project_key: Some("shared-project".to_string()),
                    session_notes_status: "loaded".to_string(),
                    project_memory_index_status: "loaded".to_string(),
                    relevant_memory_status: "lexical".to_string(),
                    project_dream_status: "loaded".to_string(),
                    global_dream_fallback_status: "skipped_project_memory_or_dream_present"
                        .to_string(),
                    dream_source: "project".to_string(),
                    session_topic_count: 1,
                    truncated_session_topic_count: 0,
                    relevant_memory_count: 1,
                    session_note_section_chars: 10,
                    project_memory_index_section_chars: 20,
                    relevant_memory_section_chars: 30,
                    project_dream_section_chars: 40,
                    global_dream_fallback_section_chars: 0,
                    context_pressure_warning_chars: 0,
                    external_memory_section_chars: 120,
                }),
                external_memory: Some("Shared memory".to_string()),
                task_list: Some("Shared task list".to_string()),
                effective_system_prompt: "Shared effective prompt".to_string(),
            })
            .expect("snapshot should serialize"),
        );

        let snapshot = build_system_prompt_response("session-shared", &session);
        assert_eq!(snapshot.base_system_prompt, "Shared base");
        assert_eq!(
            snapshot.enhancement_prompt.as_deref(),
            Some("Shared enhancement")
        );
        assert_eq!(
            snapshot.workspace_context.as_deref(),
            Some("Shared workspace")
        );
        assert_eq!(
            snapshot.project_memory_index.as_deref(),
            Some("Shared project index")
        );
        assert_eq!(
            snapshot.relevant_durable_memories.as_deref(),
            Some("Shared relevant memories")
        );
        assert_eq!(
            snapshot.project_dream.as_deref(),
            Some("Shared project dream")
        );
        assert_eq!(
            snapshot.global_dream_fallback.as_deref(),
            Some("Shared global fallback")
        );
        assert_eq!(
            snapshot
                .prompt_memory_observability
                .as_ref()
                .and_then(|item| item.resolved_project_key.as_deref()),
            Some("shared-project")
        );
        assert_eq!(snapshot.effective_system_prompt, "Shared effective prompt");
    }

    #[test]
    fn snapshot_derives_enhancement_when_metadata_is_missing() {
        let mut session = Session::new("session-1", "gpt-5");
        session
            .metadata
            .insert("base_system_prompt".to_string(), "Base prompt".to_string());
        session.add_message(Message::system("Base prompt\n\nDerived enhancement"));

        let snapshot = build_system_prompt_response("session-1", &session);
        assert_eq!(
            snapshot.enhancement_prompt.as_deref(),
            Some("Derived enhancement")
        );
    }
}
