use actix_web::{web, HttpResponse, Result};

use crate::agent::core::{Role, Session};
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
    let (prompt_without_env, env_context) = split_env_context(&prompt_without_workspace);

    let base_system_prompt = metadata_value(session, "base_system_prompt").unwrap_or_else(|| {
        let derived = prompt_without_env.trim();
        if derived.is_empty() {
            global_default_prompt()
        } else {
            derived.to_string()
        }
    });

    let enhancement_prompt = metadata_value(session, "enhance_prompt")
        .or_else(|| derive_enhancement_prompt(&base_system_prompt, &prompt_without_env));

    let workspace_context = metadata_value(session, "workspace_path")
        .and_then(|workspace_path| {
            crate::server::app_state::build_workspace_prompt_context(&workspace_path)
        })
        .or(workspace_from_prompt);

    SessionSystemPromptResponse {
        session_id: session_id.to_string(),
        base_system_prompt,
        enhancement_prompt,
        workspace_context,
        env_context,
        skill_context,
        tool_guide_context,
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
    strip_wrapped_sections(&prompt, TOOL_GUIDE_START_MARKER, TOOL_GUIDE_END_MARKER)
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

fn split_env_context(prompt: &str) -> (String, Option<String>) {
    let env_context = extract_wrapped_section(prompt, ENV_CONTEXT_START_MARKER, ENV_CONTEXT_END_MARKER);
    if env_context.is_some() {
        let stripped = strip_wrapped_sections(prompt, ENV_CONTEXT_START_MARKER, ENV_CONTEXT_END_MARKER);
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

fn derive_enhancement_prompt(
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
    use super::build_system_prompt_response;
    use crate::agent::core::{Message, Session};

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

    #[test]
    fn snapshot_extracts_generated_sections_workspace_and_env_context() {
        publish_test_env_context();

        let mut session = Session::new("session-1", "gpt-5");
        session
            .metadata
            .insert("base_system_prompt".to_string(), "Base prompt".to_string());
        session
            .metadata
            .insert("enhance_prompt".to_string(), "Extra guidance".to_string());
        session
            .metadata
            .insert("workspace_path".to_string(), "/tmp/workspace".to_string());

        let workspace_context =
            crate::server::app_state::build_workspace_prompt_context("/tmp/workspace")
                .expect("workspace context");
        let env_context = crate::server::app_state::build_env_prompt_context()
            .expect("env context should exist for snapshot test");
        session.add_message(Message::system(format!(
            "Base prompt\n\nExtra guidance\n\n{workspace_context}\n\n{env_context}\n\n<!-- BAMBOO_SKILL_CONTEXT_START -->\n## Skill System\n\nSkill details\n<!-- BAMBOO_SKILL_CONTEXT_END -->\n\n<!-- BAMBOO_TOOL_GUIDE_START -->\n## Tool Usage Guidelines\n\nGuide details\n<!-- BAMBOO_TOOL_GUIDE_END -->\n\n<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n## External Memory (Persistent)\n\nMemory details\n<!-- BAMBOO_EXTERNAL_MEMORY_END -->\n\n<!-- BAMBOO_TASK_LIST_START -->\n## Current Task List:\n- [ ] item\n<!-- BAMBOO_TASK_LIST_END -->"
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
            .is_some_and(|value| value.contains("/tmp/workspace")));
        assert!(snapshot
            .env_context
            .as_deref()
            .is_some_and(|value| value.contains("environment variables were explicitly configured by the user inside Bodhi")));
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
