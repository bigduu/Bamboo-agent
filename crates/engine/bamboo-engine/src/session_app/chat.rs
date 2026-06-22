//! Chat use case: prepare a chat turn for execution.

use crate::context::{build_env_prompt_context, build_workspace_prompt_context};
use crate::runner::refresh_prompt_snapshot;
use bamboo_agent_core::{Role, Session};
use bamboo_config::paths::path_to_display_string;
use bamboo_domain::Message;
use bamboo_skills::selection::normalize_selected_skill_ids;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

use super::errors::ChatError;
use super::provider_model::{derive_model_ref, persist_legacy_model_provider, persist_model_ref};
use super::repository::SessionAccess;
use super::types::ChatTurnInput;

// ---- Metadata keys ----
const BASE_SYSTEM_PROMPT_KEY: &str = "base_system_prompt";
const SKILL_RUNTIME_LOADED_KEY: &str = "skill_runtime_loaded_skill_ids";
const SKILL_RUNTIME_LAST_KEY: &str = "skill_runtime_last_loaded_skill_id";
const COPILOT_CONCLUSION_KEY: &str = "copilot_conclusion_with_options_enhancement_enabled";
const PROMPT_COMPOSER_VERSION_KEY: &str = "prompt_composer_version";
const PROMPT_FINGERPRINT_KEY: &str = "prompt_fingerprint";
const PROMPT_COMPONENT_FLAGS_KEY: &str = "prompt_component_flags";
const PROMPT_COMPONENT_LENGTHS_KEY: &str = "prompt_component_lengths";

const PROMPT_COMPOSER_VERSION: &str = "bamboo.prompt-composer.v2";

/// Prepare a chat turn: load/create session, resolve prompts, update metadata,
/// append user message, persist.
///
/// Returns the prepared session ready for execution.
///
/// **Note**: Image handling and workspace sync (`ensure_session_workspace`)
/// are NOT included here — those remain in the handler layer.
pub async fn prepare_chat_turn(
    repo: &dyn SessionAccess,
    input: ChatTurnInput,
    global_default_prompt: &str,
    builtin_fallback_prompt: &str,
) -> Result<Session, ChatError> {
    let mut session = repo.load_or_create(&input.session_id, &input.model).await?;

    // ---- Resolve base prompt ----
    let base_prompt = resolve_base_prompt(
        &mut session,
        input.system_prompt.as_deref(),
        global_default_prompt,
        builtin_fallback_prompt,
    );

    // ---- Resolve enhance prompt ----
    resolve_enhance_prompt(&mut session, input.enhance_prompt.as_deref());

    // ---- Resolve copilot conclusion with options enhancement ----
    resolve_copilot_conclusion_with_options_enhancement(
        &mut session,
        input.copilot_conclusion_with_options_enhancement_enabled,
    );

    // ---- Resolve workspace path (metadata only, no filesystem) ----
    let workspace_path = resolve_workspace_path(
        &mut session,
        input.workspace_path.as_deref(),
        input.data_dir.as_deref(),
    );

    // ---- Resolve selected skill IDs ----
    resolve_selected_skill_ids(
        &mut session,
        input.selected_skill_ids.as_deref(),
        &input.message,
    );

    // ---- Clear skill runtime state ----
    session.metadata.remove(SKILL_RUNTIME_LOADED_KEY);
    session.metadata.remove(SKILL_RUNTIME_LAST_KEY);

    // ---- Build enhanced system prompt with profile ----
    let (system_prompt, prompt_profile) =
        build_enhanced_system_prompt_with_profile(&base_prompt, None, workspace_path.as_deref());

    session.metadata.insert(
        PROMPT_COMPOSER_VERSION_KEY.to_string(),
        prompt_profile.version.to_string(),
    );
    session.metadata.insert(
        PROMPT_FINGERPRINT_KEY.to_string(),
        prompt_profile.fingerprint.clone(),
    );
    session.metadata.insert(
        PROMPT_COMPONENT_FLAGS_KEY.to_string(),
        prompt_profile.component_flags_value(),
    );
    session.metadata.insert(
        PROMPT_COMPONENT_LENGTHS_KEY.to_string(),
        prompt_profile.component_lengths_value(),
    );

    // ---- Upsert system prompt message ----
    session
        .messages
        .retain(|message| !matches!(message.role, Role::System));
    session.messages.insert(0, Message::system(system_prompt));
    refresh_prompt_snapshot(&mut session);

    // ---- Persist model/provider selection ----
    let request_model_ref = derive_model_ref(
        input.model_ref.as_ref(),
        input.provider.as_deref(),
        Some(input.model.as_str()),
    );
    if let Some(model_ref) = request_model_ref.as_ref() {
        persist_model_ref(&mut session, model_ref);
    } else {
        persist_legacy_model_provider(
            &mut session,
            Some(input.model.as_str()),
            input.provider.as_deref(),
        );
    }

    // ---- Save ----
    repo.save_and_cache(&mut session).await?;

    Ok(session)
}

// ---- Internal helpers ----

pub fn resolve_base_prompt(
    session: &mut Session,
    base_prompt_from_request: Option<&str>,
    global_default_template: &str,
    builtin_fallback: &str,
) -> String {
    let resolved = base_prompt_from_request
        .map(ToString::to_string)
        .or_else(|| {
            session
                .metadata
                .get(BASE_SYSTEM_PROMPT_KEY)
                .map(String::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
        })
        .or_else(|| {
            session
                .messages
                .iter()
                .find(|message| matches!(message.role, Role::System))
                .map(|message| message.content.trim().to_string())
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_else(|| {
            let trimmed = global_default_template.trim();
            if trimmed.is_empty() {
                builtin_fallback.to_string()
            } else {
                trimmed.to_string()
            }
        });

    session
        .metadata
        .insert(BASE_SYSTEM_PROMPT_KEY.to_string(), resolved.clone());
    resolved
}

pub fn resolve_enhance_prompt(session: &mut Session, enhance_prompt_from_request: Option<&str>) {
    if let Some(prompt) = enhance_prompt_from_request {
        session.set_enhance_prompt(prompt);
    } else {
        session.clear_enhance_prompt();
    }
}

pub fn resolve_copilot_conclusion_with_options_enhancement(
    session: &mut Session,
    enabled_from_request: Option<bool>,
) {
    if let Some(enabled) = enabled_from_request {
        session
            .metadata
            .insert(COPILOT_CONCLUSION_KEY.to_string(), enabled.to_string());
    } else {
        session.metadata.remove(COPILOT_CONCLUSION_KEY);
    }
}

pub fn resolve_workspace_path(
    session: &mut Session,
    workspace_path_from_request: Option<&str>,
    data_dir: Option<&Path>,
) -> Option<String> {
    if let Some(path) = workspace_path_from_request {
        session.set_workspace_path_meta(path);
    }

    workspace_path_from_request
        .map(ToString::to_string)
        .or_else(|| session.workspace_path_meta())
        .or_else(|| resolve_default_workspace(data_dir))
}

/// Resolve the configured default workspace (display string), preferring the
/// server's live in-memory config.
///
/// If a workspace provider IS registered (the server, which owns the live
/// `Arc<RwLock<Config>>`), it is AUTHORITATIVE: we use its result and never disk
/// read — even when it resolves to `None` (no default work area configured).
/// That closes the divergent disk read + global env-var-cache clobber for the
/// whole server runtime (#38 / #131). Only when NO provider is registered
/// (non-server contexts — SDK / CLI / unit tests) do we fall back to a direct
/// `from_data_dir` read of `data_dir`.
fn resolve_default_workspace(data_dir: Option<&Path>) -> Option<String> {
    let configured = if bamboo_agent_core::workspace_state::has_default_workspace_provider() {
        bamboo_agent_core::workspace_state::get_configured_default_workspace()
    } else {
        default_workspace_from_data_dir(data_dir)
    };
    configured.map(|path| path_to_display_string(&path))
}

/// Legacy non-server fallback: load `{data_dir}/config.json` from disk and read
/// its default work area. Only used when no workspace provider is registered.
fn default_workspace_from_data_dir(data_dir: Option<&Path>) -> Option<PathBuf> {
    bamboo_llm::Config::from_data_dir(data_dir.map(Path::to_path_buf)).get_default_work_area_path()
}

pub fn resolve_selected_skill_ids(
    session: &mut Session,
    selected_skill_ids_from_request: Option<&[String]>,
    message: &str,
) {
    if let Some(request_ids) = selected_skill_ids_from_request {
        let normalized = normalize_selected_skill_ids(request_ids.iter().cloned());
        persist_selected_skill_ids_metadata(session, normalized.as_deref());
        return;
    }

    let from_hint = normalize_selected_skill_ids(extract_skill_ids_from_hint(message));
    if let Some(ids) = from_hint.as_ref() {
        persist_selected_skill_ids_metadata(session, Some(ids));
        return;
    }

    session.clear_selected_skill_ids();
}

/// Clear skill runtime state markers from session metadata.
pub fn clear_skill_runtime_state(session: &mut Session) {
    session.metadata.remove(SKILL_RUNTIME_LOADED_KEY);
    session.metadata.remove(SKILL_RUNTIME_LAST_KEY);
}

fn persist_selected_skill_ids_metadata(
    session: &mut Session,
    selected_skill_ids: Option<&[String]>,
) {
    match selected_skill_ids {
        Some(ids) if !ids.is_empty() => {
            session.set_selected_skill_ids(ids.to_vec());
        }
        _ => {
            session.clear_selected_skill_ids();
        }
    }
}

// ---- Goal command parsing ----

/// Parsed result of a `/goal` command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GoalCommand {
    /// `/goal status` or bare `/goal` — read-only status query.
    Status,
    /// `/goal off` or `/goal disable` or `/goal disabled`.
    Off,
    /// `/goal clear` or `/goal reset`.
    Clear,
    /// `/goal on` or `/goal enable` or `/goal enabled`.
    On,
    /// `/goal <prompt text>` — set the goal evaluation prompt and enable.
    SetPrompt(String),
}

/// Attempt to parse a `/goal` command from the raw user message.
/// Returns `None` if the message is not a `/goal` command.
pub fn parse_goal_command(message: &str) -> Option<GoalCommand> {
    let trimmed = message.trim();
    if !trimmed.to_ascii_lowercase().starts_with("/goal") {
        return None;
    }
    // Ensure "/goal" is followed by end-of-string or whitespace (not "/goalpost").
    let rest = &trimmed[5..]; // skip "/goal"
    if !rest.is_empty() && !rest.starts_with(char::is_whitespace) {
        return None;
    }

    let arg = rest.trim().to_ascii_lowercase();

    if arg.is_empty() {
        return Some(GoalCommand::Status);
    }

    match arg.as_str() {
        "status" => Some(GoalCommand::Status),
        "off" | "disable" | "disabled" => Some(GoalCommand::Off),
        "clear" | "reset" => Some(GoalCommand::Clear),
        "on" | "enable" | "enabled" => Some(GoalCommand::On),
        _ => {
            // Everything else is treated as the goal prompt text.
            // Use the original (non-lowercased) arg to preserve casing.
            let prompt = trimmed
                .strip_prefix("/goal")
                .unwrap_or(trimmed)
                .trim()
                .to_string();
            if prompt.is_empty() {
                Some(GoalCommand::Status)
            } else {
                Some(GoalCommand::SetPrompt(prompt))
            }
        }
    }
}

fn extract_skill_ids_from_hint(message: &str) -> Vec<String> {
    const HINT_PREFIX: &str = "[User explicitly selected skill:";
    let mut extracted = Vec::new();

    for line in message.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with(HINT_PREFIX) || !trimmed.ends_with(']') {
            continue;
        }

        let Some(id_marker_index) = trimmed.rfind("(ID:") else {
            continue;
        };
        let id_segment = &trimmed[id_marker_index + "(ID:".len()..];
        let Some(close_paren_index) = id_segment.find(')') else {
            continue;
        };
        let id = id_segment[..close_paren_index].trim();
        if !id.is_empty() {
            extracted.push(id.to_string());
        }
    }

    extracted
}

// ---- Prompt building ----

#[derive(Debug, Clone, PartialEq, Eq)]
struct PromptCompositionProfile {
    version: &'static str,
    fingerprint: String,
    has_enhancement: bool,
    has_workspace_context: bool,
    has_env_context: bool,
    base_len: usize,
    enhancement_len: usize,
    workspace_context_len: usize,
    env_context_len: usize,
    final_len: usize,
}

impl PromptCompositionProfile {
    fn component_flags_value(&self) -> String {
        format!(
            "enhance={};workspace={};env={}",
            self.has_enhancement as u8,
            self.has_workspace_context as u8,
            self.has_env_context as u8,
        )
    }

    fn component_lengths_value(&self) -> String {
        format!(
            "base={};enhance={};workspace={};env={};final={}",
            self.base_len,
            self.enhancement_len,
            self.workspace_context_len,
            self.env_context_len,
            self.final_len
        )
    }
}

fn build_prompt_fingerprint(
    base_prompt: &str,
    enhancement: Option<&str>,
    workspace: Option<&str>,
    env_context: Option<&str>,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(PROMPT_COMPOSER_VERSION.as_bytes());
    hasher.update([0u8]);
    hasher.update(base_prompt.as_bytes());
    hasher.update([0u8]);
    hasher.update(enhancement.unwrap_or_default().as_bytes());
    hasher.update([0u8]);
    hasher.update(workspace.unwrap_or_default().as_bytes());
    hasher.update([0u8]);
    hasher.update(env_context.unwrap_or_default().as_bytes());
    format!("{:x}", hasher.finalize())
}

fn build_enhanced_system_prompt_with_profile(
    base_prompt: &str,
    enhance_prompt: Option<&str>,
    workspace_path: Option<&str>,
) -> (String, PromptCompositionProfile) {
    let mut merged_prompt = base_prompt.to_string();

    let enhancement = enhance_prompt
        .map(str::trim)
        .filter(|enhancement| !enhancement.is_empty())
        .map(ToString::to_string);
    if let Some(enhancement) = enhancement.as_ref() {
        merged_prompt.push_str("\n\n");
        merged_prompt.push_str(enhancement.as_str());
    }

    let workspace_context = workspace_path
        .map(str::trim)
        .filter(|workspace_path| !workspace_path.is_empty())
        .and_then(build_workspace_prompt_context);
    if let Some(workspace_context) = workspace_context.as_ref() {
        merged_prompt.push_str("\n\n");
        merged_prompt.push_str(workspace_context.as_str());
    }

    let env_context = build_env_prompt_context();
    if let Some(env_context) = env_context.as_ref() {
        merged_prompt.push_str("\n\n");
        merged_prompt.push_str(env_context.as_str());
    }

    let profile = PromptCompositionProfile {
        version: PROMPT_COMPOSER_VERSION,
        fingerprint: build_prompt_fingerprint(
            base_prompt,
            enhancement.as_deref(),
            workspace_context.as_deref(),
            env_context.as_deref(),
        ),
        has_enhancement: enhancement.is_some(),
        has_workspace_context: workspace_context.is_some(),
        has_env_context: env_context.is_some(),
        base_len: base_prompt.len(),
        enhancement_len: enhancement.as_ref().map(|s| s.len()).unwrap_or(0),
        workspace_context_len: workspace_context.as_ref().map(|s| s.len()).unwrap_or(0),
        env_context_len: env_context.as_ref().map(|s| s.len()).unwrap_or(0),
        final_len: merged_prompt.len(),
    };

    (merged_prompt, profile)
}

#[cfg(test)]
mod tests {
    use super::*;

    // The non-server disk fallback (`default_workspace_from_data_dir`) tested
    // directly + deterministically — no global workspace-provider involved (the
    // server-side, provider-gated path can't be unit-tested due to the
    // first-wins OnceLock). #38 / #131.

    #[test]
    fn default_workspace_from_data_dir_reads_configured_work_area() {
        let temp = tempfile::tempdir().expect("temp dir");
        let workspace = temp.path().join("default-workspace");
        std::fs::create_dir_all(&workspace).expect("workspace dir");
        std::fs::write(
            temp.path().join("config.json"),
            serde_json::json!({
                "default_work_area": { "path": workspace.to_string_lossy() }
            })
            .to_string(),
        )
        .expect("write config.json");

        let resolved = default_workspace_from_data_dir(Some(temp.path())).expect("resolves");
        // get_default_work_area_path returns the non-canonical candidate, and temp
        // dirs live under a symlinked prefix on macOS (/var -> /private/var), so
        // canonicalize BOTH sides before comparing.
        assert_eq!(
            resolved.canonicalize().unwrap(),
            workspace.canonicalize().unwrap()
        );
    }

    #[test]
    fn default_workspace_from_data_dir_is_none_without_config() {
        let temp = tempfile::tempdir().expect("temp dir");
        assert!(default_workspace_from_data_dir(Some(temp.path())).is_none());
    }
}
