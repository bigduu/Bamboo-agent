//! Chat use case: prepare a chat turn for execution.

use crate::context::build_env_prompt_context;
use crate::runner::refresh_prompt_snapshot;
use bamboo_agent_core::{Role, Session};
use bamboo_config::paths::path_to_display_string;
use bamboo_domain::Message;
use bamboo_skills::selection::normalize_selected_skill_ids;
use bamboo_skills::{
    ActiveWorkflow, WorkflowActivationStatus, WorkflowSelection, ACTIVE_WORKFLOW_METADATA_KEY,
    ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY, WORKFLOW_ACTIVATION_EVENT_METADATA_KEY,
    WORKFLOW_ORCHESTRATION_OPT_IN_METADATA_KEY, WORKFLOW_SELECTION_METADATA_KEY,
};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

use super::errors::ChatError;
use super::provider_model::{derive_model_ref, persist_legacy_model_provider, persist_model_ref};
use super::repository::SessionAccess;
use super::types::{ChatTurnInput, ChatWorkspaceFallbackPolicy};

// ---- Metadata keys ----
const BASE_SYSTEM_PROMPT_KEY: &str = "base_system_prompt";
const SKILL_RUNTIME_LOADED_KEY: &str = "skill_runtime_loaded_skill_ids";
const SKILL_RUNTIME_LAST_KEY: &str = "skill_runtime_last_loaded_skill_id";
const COPILOT_CONCLUSION_KEY: &str = "copilot_conclusion_with_options_enhancement_enabled";
const PROMPT_COMPOSER_VERSION_KEY: &str = "prompt_composer_version";
const PROMPT_FINGERPRINT_KEY: &str = "prompt_fingerprint";
const PROMPT_COMPONENT_FLAGS_KEY: &str = "prompt_component_flags";
const PROMPT_COMPONENT_LENGTHS_KEY: &str = "prompt_component_lengths";

const PROMPT_COMPOSER_VERSION: &str = "bamboo.prompt-composer.v3";
pub const SESSION_START_SOURCE_METADATA_KEY: &str = "runtime.session_start_source";

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
    prepare_chat_turn_inner(
        repo,
        input,
        global_default_prompt,
        builtin_fallback_prompt,
        true,
    )
    .await
}

/// Prepare a chat turn without persisting it.
///
/// The server uses this variant so its authoritative Project/workspace
/// resolver can run after the second session load and before any session or
/// runtime workspace side effect. Callers must save the returned session after
/// their external invariants pass.
pub async fn prepare_chat_turn_unpersisted(
    repo: &dyn SessionAccess,
    input: ChatTurnInput,
    global_default_prompt: &str,
    builtin_fallback_prompt: &str,
) -> Result<Session, ChatError> {
    prepare_chat_turn_inner(
        repo,
        input,
        global_default_prompt,
        builtin_fallback_prompt,
        false,
    )
    .await
}

async fn prepare_chat_turn_inner(
    repo: &dyn SessionAccess,
    input: ChatTurnInput,
    global_default_prompt: &str,
    builtin_fallback_prompt: &str,
    persist: bool,
) -> Result<Session, ChatError> {
    let existing = repo.load_merged(&input.session_id).await?;
    let mut session = prepare_chat_turn_from_authoritative_session(
        existing,
        input,
        global_default_prompt,
        builtin_fallback_prompt,
    )?;
    if persist {
        repo.save_and_cache(&mut session).await?;
    }
    Ok(session)
}

/// Prepare a turn from a caller-supplied authoritative durable snapshot.
///
/// HTTP transaction paths acquire their per-session persistence lock, load
/// directly from `LockedSessionStore::storage()`, and call this function so a
/// stale cache can never win the Project membership re-check.
pub fn prepare_chat_turn_from_authoritative_session(
    existing: Option<Session>,
    input: ChatTurnInput,
    global_default_prompt: &str,
    builtin_fallback_prompt: &str,
) -> Result<Session, ChatError> {
    prepare_chat_turn_from_authoritative_session_with_workspace_policy(
        existing,
        input,
        global_default_prompt,
        builtin_fallback_prompt,
        ChatWorkspaceFallbackPolicy::Legacy,
    )
}

/// Prepare a turn with an explicit caller-owned workspace fallback policy.
///
/// The server uses this entrypoint after loading its own live config snapshot
/// and previewing its AppState-scoped session fallback. Existing callers use
/// [`prepare_chat_turn_from_authoritative_session`] and preserve legacy
/// process-global/data-directory fallback behavior.
pub fn prepare_chat_turn_from_authoritative_session_with_workspace_policy(
    existing: Option<Session>,
    input: ChatTurnInput,
    global_default_prompt: &str,
    builtin_fallback_prompt: &str,
    workspace_fallback_policy: ChatWorkspaceFallbackPolicy,
) -> Result<Session, ChatError> {
    let (mut session, session_start_source) = match existing {
        Some(session) => (session, "resume"),
        None => (
            Session::new(input.session_id.clone(), input.model.clone()),
            "startup",
        ),
    };
    match crate::project_context::ProjectContextResolver::session_project_identity(&session) {
        crate::project_context::SessionProjectIdentity::Invalid { raw, message } => {
            return Err(ChatError::InvalidProjectIdentity { raw, message });
        }
        crate::project_context::SessionProjectIdentity::Assigned(actual)
            if input.project_id.as_ref() != Some(&actual) =>
        {
            return Err(ChatError::ProjectIdentityConflict {
                expected: input.project_id,
                actual: Some(actual),
            });
        }
        crate::project_context::SessionProjectIdentity::Unassigned
            if session_start_source == "resume" && input.project_id.is_some() =>
        {
            return Err(ChatError::ProjectIdentityConflict {
                expected: input.project_id,
                actual: None,
            });
        }
        crate::project_context::SessionProjectIdentity::Assigned(_)
        | crate::project_context::SessionProjectIdentity::Unassigned => {}
    }
    crate::runtime::runner::session_setup::migrate_legacy_workspace_prompt(&mut session);
    session.metadata.insert(
        SESSION_START_SOURCE_METADATA_KEY.to_string(),
        session_start_source.to_string(),
    );
    if session_start_source == "startup" {
        if let Some(project_id) = input.project_id.as_ref() {
            session.set_project_id_meta(project_id.to_string());
        }
        session.reasoning_effort = input.reasoning_effort;
    }

    // ---- Resolve base prompt ----
    let base_prompt = resolve_base_prompt(
        &mut session,
        input.system_prompt.as_deref(),
        global_default_prompt,
        builtin_fallback_prompt,
    );

    // ---- Resolve enhance prompt ----
    resolve_enhance_prompt(&mut session, input.enhance_prompt.as_deref());
    let enhance_prompt = session.enhance_prompt();

    // ---- Resolve copilot conclusion with options enhancement ----
    resolve_copilot_conclusion_with_options_enhancement(
        &mut session,
        input.copilot_conclusion_with_options_enhancement_enabled,
    );

    // ---- Resolve workspace path (metadata only, no filesystem) ----
    let allow_legacy_workspace_fallback = matches!(
        crate::project_context::ProjectContextResolver::session_project_identity(&session),
        crate::project_context::SessionProjectIdentity::Unassigned
    );
    let workspace_path = resolve_workspace_path_with_default(
        &mut session,
        input.workspace_path.as_deref(),
        input.default_workspace_path.as_deref(),
        &workspace_fallback_policy,
        input.data_dir.as_deref(),
        allow_legacy_workspace_fallback,
    );

    // ---- Resolve typed workflow selection / legacy skill IDs ----
    resolve_workflow_selection(
        &mut session,
        input.workflow_selection.as_ref(),
        input.selected_skill_ids.as_deref(),
        &input.message,
    )?;
    if let Some(opted_in) = input.orchestration_opt_in {
        session.metadata.insert(
            WORKFLOW_ORCHESTRATION_OPT_IN_METADATA_KEY.to_string(),
            opted_in.to_string(),
        );
    }

    // ---- Build enhanced system prompt with profile ----
    let (system_prompt, prompt_profile) = build_enhanced_system_prompt_with_profile(
        &base_prompt,
        enhance_prompt.as_deref(),
        workspace_path.as_deref(),
    );

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
    // A workspace-only switch leaves this stable text byte-identical and must
    // not rewrite Session.messages.
    let system_messages = session
        .messages
        .iter()
        .filter(|message| matches!(message.role, Role::System))
        .collect::<Vec<_>>();
    let system_is_current = system_messages.len() == 1
        && system_messages[0].content == system_prompt
        && session
            .messages
            .first()
            .is_some_and(|message| matches!(message.role, Role::System));
    if !system_is_current {
        session
            .messages
            .retain(|message| !matches!(message.role, Role::System));
        session.messages.insert(0, Message::system(system_prompt));
    }
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
    let resolved =
        crate::runtime::runner::session_setup::prompt_setup::normalize_base_prompt(&resolved);

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
    resolve_workspace_path_with_default(
        session,
        workspace_path_from_request,
        None,
        &ChatWorkspaceFallbackPolicy::Legacy,
        data_dir,
        true,
    )
}

fn resolve_workspace_path_with_default(
    session: &mut Session,
    workspace_path_from_request: Option<&str>,
    default_workspace_path: Option<&str>,
    fallback_policy: &ChatWorkspaceFallbackPolicy,
    data_dir: Option<&Path>,
    allow_legacy_fallback: bool,
) -> Option<String> {
    let explicit = workspace_path_from_request
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);
    let persisted = session.workspace_path_meta();
    let configured_default = allow_legacy_fallback
        .then(|| {
            default_workspace_path
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
        })
        .flatten();
    let fallback = (explicit.is_none() && persisted.is_none() && configured_default.is_none())
        .then(|| {
            allow_legacy_fallback
                .then(|| {
                    resolve_workspace_fallback_with(fallback_policy, || {
                        resolve_default_workspace(data_dir)
                    })
                })
                .flatten()
        })
        .flatten();
    let (resolved, source, binding_reset) = if let Some(workspace) = explicit {
        (
            Some(workspace),
            crate::project_context::WorkspaceSource::Explicit,
            true,
        )
    } else if let Some(workspace) = persisted {
        (
            Some(workspace),
            crate::project_context::WorkspaceSource::from_metadata(
                session
                    .metadata
                    .get(crate::project_context::WORKSPACE_SOURCE_METADATA_KEY)
                    .map(String::as_str),
            ),
            false,
        )
    } else if let Some(workspace) = configured_default {
        (
            Some(workspace),
            crate::project_context::WorkspaceSource::Session,
            true,
        )
    } else {
        (
            fallback,
            crate::project_context::WorkspaceSource::Session,
            true,
        )
    };
    if let Some(workspace) = resolved.as_ref() {
        // Persist the effective post-lock choice, including a live-config or
        // session-root fallback. Otherwise the subsequent Project resolver
        // would see no metadata and independently pick a different global
        // fallback.
        session.set_workspace_path_meta(workspace.clone());
        session.metadata.insert(
            crate::project_context::WORKSPACE_SOURCE_METADATA_KEY.to_string(),
            source.as_str().to_string(),
        );
        if binding_reset {
            session.metadata.insert(
                crate::project_context::WORKSPACE_BINDING_STATUS_METADATA_KEY.to_string(),
                crate::project_context::WorkspaceBindingStatus::Unregistered
                    .as_str()
                    .to_string(),
            );
        }
    }
    resolved
}

fn resolve_workspace_fallback_with<L>(
    fallback_policy: &ChatWorkspaceFallbackPolicy,
    legacy_lookup: L,
) -> Option<String>
where
    L: FnOnce() -> Option<String>,
{
    match fallback_policy {
        ChatWorkspaceFallbackPolicy::Legacy => legacy_lookup(),
        ChatWorkspaceFallbackPolicy::Authoritative {
            session_fallback_path,
        } => session_fallback_path.clone(),
    }
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

pub fn resolve_workflow_selection(
    session: &mut Session,
    workflow_selection: Option<&WorkflowSelection>,
    selected_skill_ids_from_request: Option<&[String]>,
    message: &str,
) -> Result<(), ChatError> {
    if let Some(selection) = workflow_selection {
        let id = selection.id.trim();
        if id.is_empty() || selection.revision == 0 || !selection.args.is_object() {
            return Err(ChatError::InvalidWorkflowSelection(
                "id must be non-empty, revision must be positive, and args must be an object"
                    .to_string(),
            ));
        }
        let previous = session
            .metadata
            .get(WORKFLOW_SELECTION_METADATA_KEY)
            .and_then(|raw| serde_json::from_str::<WorkflowSelection>(raw).ok());
        let selection_changed = previous.as_ref() != Some(selection);
        session.metadata.insert(
            WORKFLOW_SELECTION_METADATA_KEY.to_string(),
            serde_json::to_string(selection).map_err(|_| {
                ChatError::InvalidWorkflowSelection("selection cannot be serialized".to_string())
            })?,
        );
        persist_selected_skill_ids_metadata(session, Some(&[id.to_string()]));
        if selection_changed {
            deactivate_active_workflow(session);
            clear_skill_runtime_state(session);
        }
        return Ok(());
    }

    if selected_skill_ids_from_request.is_some() {
        // Legacy explicit selection remains compatible, but can never override
        // an authoritative typed selection in the same request.
        session.metadata.remove(WORKFLOW_SELECTION_METADATA_KEY);
        deactivate_active_workflow(session);
        resolve_selected_skill_ids(session, selected_skill_ids_from_request, message);
        clear_skill_runtime_state(session);
        return Ok(());
    }

    // A typed chat candidate is durable authority for the next execute, even
    // when another ordinary message is appended before execution starts.
    // Keep its visible selected id aligned with the retained immutable
    // snapshot; only an explicit replacement/deactivation may cancel it.
    if let Some(pending) = session
        .metadata
        .get(WORKFLOW_SELECTION_METADATA_KEY)
        .and_then(|raw| serde_json::from_str::<WorkflowSelection>(raw).ok())
        .filter(|_| {
            session
                .metadata
                .get(bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTION_SOURCE_KEY)
                .is_some_and(|source| source == "explicit")
                && session.metadata.contains_key(
                    bamboo_skills::runtime_metadata::SKILL_RUNTIME_PINNED_SNAPSHOT_KEY,
                )
        })
    {
        persist_selected_skill_ids_metadata(session, Some(&[pending.id]));
        return Ok(());
    }

    if let Some(active) = session
        .metadata
        .get(ACTIVE_WORKFLOW_METADATA_KEY)
        .and_then(|raw| serde_json::from_str::<ActiveWorkflow>(raw).ok())
        .filter(|active| active.status == WorkflowActivationStatus::Active)
    {
        persist_selected_skill_ids_metadata(session, Some(&[active.id]));
        return Ok(());
    }

    // Legacy natural-language hints are parsed only when there is no typed or
    // durable active workflow. They remain compatibility input, never authority.
    resolve_selected_skill_ids(session, None, message);
    Ok(())
}

fn deactivate_active_workflow(session: &mut Session) {
    if let Some(active) = session
        .metadata
        .get(ACTIVE_WORKFLOW_METADATA_KEY)
        .and_then(|raw| serde_json::from_str::<ActiveWorkflow>(raw).ok())
        .filter(|active| active.status == WorkflowActivationStatus::Active)
    {
        session.metadata.insert(
            WORKFLOW_ACTIVATION_EVENT_METADATA_KEY.to_string(),
            serde_json::json!({
                "type": "workflow.deactivated",
                "workflow_id": active.id,
                "revision": active.revision,
                "deactivated_at": chrono::Utc::now(),
            })
            .to_string(),
        );
    }
    session.metadata.remove(ACTIVE_WORKFLOW_METADATA_KEY);
    session
        .metadata
        .remove(ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY);
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

fn build_prompt_fingerprint(base_prompt: &str, enhancement: Option<&str>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(PROMPT_COMPOSER_VERSION.as_bytes());
    hasher.update([0u8]);
    hasher.update(base_prompt.as_bytes());
    hasher.update([0u8]);
    hasher.update(enhancement.unwrap_or_default().as_bytes());
    hex::encode(hasher.finalize())
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

    let has_workspace_context = workspace_path
        .map(str::trim)
        .is_some_and(|workspace_path| !workspace_path.is_empty());

    // Environment details are discovered from the same authority during round
    // assembly and emitted as a typed EnvSnapshot. Keep the compatibility flag
    // observable without copying those details into System or its fingerprint.
    let has_env_context = build_env_prompt_context().is_some();

    let profile = PromptCompositionProfile {
        version: PROMPT_COMPOSER_VERSION,
        fingerprint: build_prompt_fingerprint(base_prompt, enhancement.as_deref()),
        has_enhancement: enhancement.is_some(),
        has_workspace_context,
        has_env_context,
        base_len: base_prompt.len(),
        enhancement_len: enhancement.as_ref().map(|s| s.len()).unwrap_or(0),
        workspace_context_len: 0,
        env_context_len: 0,
        final_len: merged_prompt.len(),
    };

    (merged_prompt, profile)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session_app::errors::{SessionLoadError, SessionSaveError};
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    struct InMemorySessionAccess;

    struct ExistingSessionAccess(Session);

    struct BarrierSessionAccess {
        session: Mutex<Session>,
        load_started: tokio::sync::Notify,
        release_load: tokio::sync::Notify,
        save_count: AtomicUsize,
    }

    #[async_trait]
    impl SessionAccess for InMemorySessionAccess {
        async fn load_session(&self, _id: &str) -> Result<Option<Session>, SessionLoadError> {
            Ok(None)
        }

        async fn load_or_create(&self, id: &str, model: &str) -> Result<Session, SessionLoadError> {
            Ok(Session::new(id, model))
        }

        async fn load_merged(&self, _id: &str) -> Result<Option<Session>, SessionLoadError> {
            Ok(None)
        }

        async fn save_session(&self, _session: &mut Session) -> Result<(), SessionSaveError> {
            Ok(())
        }

        async fn save_and_cache(&self, _session: &mut Session) -> Result<(), SessionSaveError> {
            Ok(())
        }
    }

    #[async_trait]
    impl SessionAccess for ExistingSessionAccess {
        async fn load_session(&self, _id: &str) -> Result<Option<Session>, SessionLoadError> {
            Ok(Some(self.0.clone()))
        }

        async fn load_or_create(
            &self,
            _id: &str,
            _model: &str,
        ) -> Result<Session, SessionLoadError> {
            panic!("existing chat session must not be recreated")
        }

        async fn load_merged(&self, _id: &str) -> Result<Option<Session>, SessionLoadError> {
            Ok(Some(self.0.clone()))
        }

        async fn save_session(&self, _session: &mut Session) -> Result<(), SessionSaveError> {
            Ok(())
        }

        async fn save_and_cache(&self, _session: &mut Session) -> Result<(), SessionSaveError> {
            Ok(())
        }
    }

    #[async_trait]
    impl SessionAccess for BarrierSessionAccess {
        async fn load_session(&self, _id: &str) -> Result<Option<Session>, SessionLoadError> {
            Ok(Some(self.session.lock().expect("session lock").clone()))
        }

        async fn load_or_create(
            &self,
            _id: &str,
            _model: &str,
        ) -> Result<Session, SessionLoadError> {
            panic!("barrier session already exists")
        }

        async fn load_merged(&self, _id: &str) -> Result<Option<Session>, SessionLoadError> {
            self.load_started.notify_one();
            self.release_load.notified().await;
            Ok(Some(self.session.lock().expect("session lock").clone()))
        }

        async fn save_session(&self, _session: &mut Session) -> Result<(), SessionSaveError> {
            self.save_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn save_and_cache(&self, _session: &mut Session) -> Result<(), SessionSaveError> {
            self.save_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn chat_turn_input(enhance_prompt: Option<&str>) -> super::super::types::ChatTurnInput {
        super::super::types::ChatTurnInput {
            session_id: "session-enhance".to_string(),
            project_id: None,
            model: "gpt-5".to_string(),
            model_ref: None,
            provider: None,
            reasoning_effort: None,
            message: "hello".to_string(),
            system_prompt: Some("Base prompt".to_string()),
            enhance_prompt: enhance_prompt.map(ToString::to_string),
            workspace_path: None,
            default_workspace_path: None,
            selected_skill_ids: None,
            workflow_selection: None,
            orchestration_opt_in: None,
            copilot_conclusion_with_options_enhancement_enabled: None,
            data_dir: None,
        }
    }

    fn system_message_content(session: &Session) -> String {
        session
            .messages
            .iter()
            .find(|message| matches!(message.role, Role::System))
            .map(|message| message.content.clone())
            .expect("session should have a system message")
    }

    #[test]
    fn new_chat_session_persists_explicit_reasoning_profile() {
        let mut input = chat_turn_input(None);
        input.reasoning_effort = Some(bamboo_domain::ReasoningEffort::Xhigh);

        let session = prepare_chat_turn_from_authoritative_session(
            None,
            input,
            "Global prompt",
            "Builtin prompt",
        )
        .expect("new chat session");

        assert_eq!(
            session.reasoning_effort,
            Some(bamboo_domain::ReasoningEffort::Xhigh)
        );
    }

    fn active_workflow(id: &str, revision: u64) -> ActiveWorkflow {
        ActiveWorkflow {
            id: id.to_string(),
            source: bamboo_skills::WorkflowSource::User,
            revision,
            kind: bamboo_skills::WorkflowKind::Instruction,
            args: serde_json::json!({}),
            invoked_by: bamboo_skills::WorkflowInvokedBy::User,
            activated_at: chrono::Utc::now(),
            status: WorkflowActivationStatus::Active,
            diagnostic: None,
            context_fingerprint: Some("fingerprint".to_string()),
            dynamic_context: Vec::new(),
        }
    }

    #[tokio::test]
    async fn prepare_chat_turn_classifies_new_and_existing_sessions() {
        let startup = prepare_chat_turn(
            &InMemorySessionAccess,
            chat_turn_input(None),
            "global",
            "builtin",
        )
        .await
        .expect("new turn");
        assert_eq!(
            startup
                .metadata
                .get(SESSION_START_SOURCE_METADATA_KEY)
                .map(String::as_str),
            Some("startup")
        );

        let existing = prepare_chat_turn(
            &ExistingSessionAccess(Session::new("session-enhance", "gpt-5")),
            chat_turn_input(None),
            "global",
            "builtin",
        )
        .await
        .expect("existing turn");
        assert_eq!(
            existing
                .metadata
                .get(SESSION_START_SOURCE_METADATA_KEY)
                .map(String::as_str),
            Some("resume")
        );
    }

    #[tokio::test]
    async fn prepare_chat_turn_rechecks_project_identity_after_preflight_race() {
        let project_a =
            bamboo_domain::ProjectId::parse("project-chat-a").expect("Project A identity");
        let project_b =
            bamboo_domain::ProjectId::parse("project-chat-b").expect("Project B identity");
        let workspace_a = tempfile::tempdir().expect("workspace A");
        let workspace_b = tempfile::tempdir().expect("workspace B");
        let mut session = Session::new("chat-project-race", "gpt-5");
        session.set_project_id_meta(project_a.to_string());
        session.set_workspace_path_meta(workspace_a.path().to_string_lossy().into_owned());
        let repo = Arc::new(BarrierSessionAccess {
            session: Mutex::new(session),
            load_started: tokio::sync::Notify::new(),
            release_load: tokio::sync::Notify::new(),
            save_count: AtomicUsize::new(0),
        });
        let mut input = chat_turn_input(None);
        input.session_id = "chat-project-race".to_string();
        input.project_id = Some(project_a.clone());
        input.workspace_path = Some(workspace_a.path().to_string_lossy().into_owned());

        let task_repo = repo.clone();
        let task = tokio::spawn(async move {
            prepare_chat_turn(task_repo.as_ref(), input, "global", "builtin").await
        });
        repo.load_started.notified().await;
        {
            let mut raced = repo.session.lock().expect("session lock");
            raced.set_project_id_meta(project_b.to_string());
            raced.set_workspace_path_meta(workspace_b.path().to_string_lossy().into_owned());
        }
        repo.release_load.notify_one();

        let error = task
            .await
            .expect("chat task")
            .expect_err("membership race must fail closed");
        assert!(matches!(
            error,
            ChatError::ProjectIdentityConflict {
                expected: Some(expected),
                actual: Some(actual),
            } if expected == project_a && actual == project_b
        ));
        assert_eq!(repo.save_count.load(Ordering::SeqCst), 0);
        let persisted = repo.session.lock().expect("session lock");
        assert_eq!(
            persisted.workspace_path_meta().as_deref(),
            Some(workspace_b.path().to_string_lossy().as_ref())
        );
        assert!(persisted.messages.is_empty());
        assert!(!persisted
            .metadata
            .contains_key(SESSION_START_SOURCE_METADATA_KEY));
    }

    #[test]
    fn typed_workflow_selection_is_authoritative_over_legacy_ids_and_hint() {
        let mut session = Session::new("typed-selection", "model");
        let selection = WorkflowSelection {
            id: "review".to_string(),
            source: bamboo_skills::WorkflowSource::User,
            revision: 7,
            args: serde_json::json!({"depth": "full"}),
        };
        resolve_workflow_selection(
            &mut session,
            Some(&selection),
            Some(&["plan".to_string()]),
            "use skill plan",
        )
        .expect("typed selection");
        assert_eq!(
            session.selected_skill_ids(),
            Some(vec!["review".to_string()])
        );
        assert_eq!(
            session
                .metadata
                .get(WORKFLOW_SELECTION_METADATA_KEY)
                .and_then(|raw| serde_json::from_str::<WorkflowSelection>(raw).ok()),
            Some(selection)
        );
    }

    #[test]
    fn active_workflow_survives_turn_without_new_selection() {
        let mut session = Session::new("active-selection", "model");
        session.metadata.insert(
            ACTIVE_WORKFLOW_METADATA_KEY.to_string(),
            serde_json::to_string(&active_workflow("review", 7)).expect("active json"),
        );
        resolve_workflow_selection(&mut session, None, None, "use skill plan")
            .expect("preserve active");
        assert_eq!(
            session.selected_skill_ids(),
            Some(vec!["review".to_string()])
        );
        assert!(session.metadata.contains_key(ACTIVE_WORKFLOW_METADATA_KEY));
    }

    #[test]
    fn pending_typed_workflow_survives_an_ordinary_chat_before_execute() {
        let mut session = Session::new("pending-selection", "model");
        let selection = WorkflowSelection {
            id: "review".to_string(),
            source: bamboo_skills::WorkflowSource::Builtin,
            revision: 7,
            args: serde_json::json!({}),
        };
        session.metadata.insert(
            WORKFLOW_SELECTION_METADATA_KEY.to_string(),
            serde_json::to_string(&selection).expect("selection json"),
        );
        session.metadata.insert(
            bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTION_SOURCE_KEY.to_string(),
            "explicit".to_string(),
        );
        session.metadata.insert(
            bamboo_skills::runtime_metadata::SKILL_RUNTIME_PINNED_SNAPSHOT_KEY.to_string(),
            "opaque durable snapshot".to_string(),
        );

        resolve_workflow_selection(&mut session, None, None, "one more detail")
            .expect("retain pending selection");

        assert_eq!(
            session.selected_skill_ids(),
            Some(vec!["review".to_string()])
        );
        assert_eq!(
            session
                .metadata
                .get(WORKFLOW_SELECTION_METADATA_KEY)
                .and_then(|raw| serde_json::from_str::<WorkflowSelection>(raw).ok()),
            Some(selection)
        );
        assert!(session
            .metadata
            .contains_key(bamboo_skills::runtime_metadata::SKILL_RUNTIME_PINNED_SNAPSHOT_KEY));
    }

    #[test]
    fn explicit_empty_legacy_selection_deactivates_active_workflow() {
        let mut session = Session::new("deactivate-selection", "model");
        session.metadata.insert(
            ACTIVE_WORKFLOW_METADATA_KEY.to_string(),
            serde_json::to_string(&active_workflow("review", 7)).expect("active json"),
        );
        resolve_workflow_selection(&mut session, None, Some(&[]), "plain message")
            .expect("deactivate");
        assert!(session.selected_skill_ids().is_none());
        assert!(!session.metadata.contains_key(ACTIVE_WORKFLOW_METADATA_KEY));
        assert!(session
            .metadata
            .get(WORKFLOW_ACTIVATION_EVENT_METADATA_KEY)
            .is_some_and(|event| event.contains("workflow.deactivated")));
    }

    // Regression: the request's enhance_prompt must land in the upserted system
    // message, not just in session metadata (it was silently dropped once).
    #[tokio::test]
    async fn prepare_chat_turn_merges_enhance_prompt_into_system_message() {
        let session = prepare_chat_turn(
            &InMemorySessionAccess,
            chat_turn_input(Some("Extra enhancement guidance")),
            "",
            "Builtin fallback",
        )
        .await
        .expect("prepare_chat_turn should succeed");

        let system_prompt = system_message_content(&session);
        assert!(system_prompt.starts_with("Base prompt"));
        assert!(system_prompt.contains("Extra enhancement guidance"));
        assert!(!system_prompt.contains(crate::runtime::context::PROJECT_CONTEXT_START_MARKER));
        assert!(!system_prompt.contains(crate::runtime::context::WORKSPACE_CONTEXT_START_MARKER));
        assert!(!system_prompt
            .contains(crate::runtime::context::instruction::INSTRUCTION_CONTEXT_START_MARKER));
        assert!(!system_prompt.contains(crate::runtime::context::ENV_CONTEXT_START_MARKER));
        assert!(!system_prompt.contains("BAMBOO_SKILL_CONTEXT_START"));
        assert!(!system_prompt.contains("BAMBOO_TOOL_GUIDE_START"));
        assert_eq!(
            session.enhance_prompt().as_deref(),
            Some("Extra enhancement guidance")
        );
        assert!(session
            .metadata
            .get(PROMPT_COMPONENT_FLAGS_KEY)
            .is_some_and(|flags| flags.contains("enhance=1")));
    }

    #[tokio::test]
    async fn prepare_chat_turn_without_enhance_prompt_keeps_base_only() {
        let session = prepare_chat_turn(
            &InMemorySessionAccess,
            chat_turn_input(None),
            "",
            "Builtin fallback",
        )
        .await
        .expect("prepare_chat_turn should succeed");

        let system_prompt = system_message_content(&session);
        assert!(system_prompt.starts_with("Base prompt"));
        assert!(session.enhance_prompt().is_none());
        assert!(session
            .metadata
            .get(PROMPT_COMPONENT_FLAGS_KEY)
            .is_some_and(|flags| flags.contains("enhance=0")));
    }

    #[test]
    fn workspace_switch_preserves_system_identity_and_prompt_fingerprint() {
        let first_workspace = "/workspace/first-private";
        let second_workspace = "/workspace/second-private";
        let mut first_input = chat_turn_input(Some("Stable enhancement"));
        first_input.workspace_path = Some(first_workspace.to_string());
        let first = prepare_chat_turn_from_authoritative_session(
            None,
            first_input,
            "Global prompt",
            "Builtin prompt",
        )
        .expect("first workspace turn");
        let first_system = first
            .messages
            .iter()
            .find(|message| matches!(message.role, Role::System))
            .expect("first system message")
            .clone();
        let first_fingerprint = first
            .metadata
            .get(PROMPT_FINGERPRINT_KEY)
            .expect("first prompt fingerprint")
            .clone();

        let mut switched_input = chat_turn_input(Some("Stable enhancement"));
        switched_input.workspace_path = Some(second_workspace.to_string());
        let switched = prepare_chat_turn_from_authoritative_session(
            Some(first),
            switched_input,
            "Global prompt",
            "Builtin prompt",
        )
        .expect("switched workspace turn");
        let switched_system = switched
            .messages
            .iter()
            .find(|message| matches!(message.role, Role::System))
            .expect("switched system message");

        assert_eq!(switched_system.id, first_system.id);
        assert_eq!(switched_system.content, first_system.content);
        assert_eq!(
            switched.metadata.get(PROMPT_FINGERPRINT_KEY),
            Some(&first_fingerprint)
        );
        assert_eq!(
            switched.workspace_path_meta().as_deref(),
            Some(second_workspace)
        );
        assert!(!switched_system.content.contains(first_workspace));
        assert!(!switched_system.content.contains(second_workspace));
        assert!(!switched_system
            .content
            .contains(crate::runtime::context::WORKSPACE_CONTEXT_START_MARKER));
        assert!(switched
            .prompt_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.workspace_context.as_deref())
            .is_some_and(|context| context.contains(second_workspace)));
    }

    #[test]
    fn authoritative_none_uses_session_fallback_during_prompt_composition() {
        let fixture = tempfile::tempdir().expect("fixture");
        let foreign_default = fixture.path().join("foreign-default");
        std::fs::create_dir_all(&foreign_default).expect("foreign default");
        std::fs::write(
            fixture.path().join("config.json"),
            serde_json::json!({
                "default_work_area": { "path": foreign_default.to_string_lossy() }
            })
            .to_string(),
        )
        .expect("write legacy config");
        let session_fallback = fixture
            .path()
            .join("request-state-root/workspaces/session-enhance");
        let session_fallback_display = path_to_display_string(&session_fallback);
        let mut input = chat_turn_input(None);
        input.data_dir = Some(fixture.path().to_path_buf());

        let session = prepare_chat_turn_from_authoritative_session_with_workspace_policy(
            None,
            input,
            "global",
            "builtin",
            ChatWorkspaceFallbackPolicy::Authoritative {
                session_fallback_path: Some(session_fallback_display.clone()),
            },
        )
        .expect("authoritative turn");

        assert_eq!(
            session.workspace_path_meta().as_deref(),
            Some(session_fallback_display.as_str())
        );
        let system_prompt = system_message_content(&session);
        assert!(
            !system_prompt.contains(&session_fallback_display),
            "workspace path must stay out of the persisted System message"
        );
        assert!(
            !system_prompt.contains(foreign_default.to_string_lossy().as_ref()),
            "authoritative None must suppress the legacy configured default"
        );
        assert!(
            session
                .prompt_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.workspace_context.as_deref())
                .is_some_and(|context| context.contains(&session_fallback_display)),
            "prompt snapshot must retain the authoritative session fallback"
        );
    }

    #[test]
    fn authoritative_none_without_session_fallback_skips_legacy_lookup() {
        let resolved = resolve_workspace_fallback_with(
            &ChatWorkspaceFallbackPolicy::Authoritative {
                session_fallback_path: None,
            },
            || panic!("authoritative None must not read a process-global or disk default"),
        );

        assert_eq!(resolved, None);
    }

    #[test]
    fn legacy_policy_preserves_non_server_default_workspace_lookup() {
        let fixture = tempfile::tempdir().expect("fixture");
        let configured_default = fixture.path().join("configured-default");
        std::fs::create_dir_all(&configured_default).expect("configured default");
        std::fs::write(
            fixture.path().join("config.json"),
            serde_json::json!({
                "default_work_area": { "path": configured_default.to_string_lossy() }
            })
            .to_string(),
        )
        .expect("write config");
        let mut input = chat_turn_input(None);
        input.data_dir = Some(fixture.path().to_path_buf());

        let session =
            prepare_chat_turn_from_authoritative_session(None, input, "global", "builtin")
                .expect("legacy turn");
        let resolved = session
            .workspace_path_meta()
            .map(PathBuf::from)
            .expect("legacy configured default");

        assert_eq!(
            resolved.canonicalize().expect("resolved canonical"),
            configured_default
                .canonicalize()
                .expect("configured canonical")
        );
        assert_eq!(
            session
                .metadata
                .get(crate::project_context::WORKSPACE_SOURCE_METADATA_KEY)
                .map(String::as_str),
            Some(crate::project_context::WorkspaceSource::Session.as_str())
        );
        assert!(session
            .prompt_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.workspace_context.as_deref())
            .is_some_and(|context| context.contains("Workspace source: session")));
    }

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
