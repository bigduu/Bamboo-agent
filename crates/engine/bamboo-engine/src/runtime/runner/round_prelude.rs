//! Minimal round prelude — provides `prepare_round` for lifecycle adapter
//! and `refresh_round_prompt_context` for the pipeline.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::task_context::TaskLoopContext;
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentError, Role, Session};
use bamboo_llm::LLMProvider;
use bamboo_metrics::MetricsCollector;

use super::prompt_context::{
    inject_external_memory_into_system_message, PromptMemoryRuntimeContext,
    PROMPT_MEMORY_OBSERVABILITY_KEY,
};
use super::session_setup::prompt_setup::{persist_prompt_snapshot_metadata, PromptAssemblyReport};
use bamboo_agent_core::PromptSnapshot;

/// Round-prelude frame bundling per-round identification and observability
/// parameters.  Passed into [`prepare_round`] to keep its parameter count
/// below the clippy threshold.
pub(crate) struct RoundPreludeFrame<'a> {
    pub round: usize,
    pub max_rounds: usize,
    pub debug_enabled: bool,
    pub cancel_token: &'a CancellationToken,
    pub metrics_collector: Option<&'a MetricsCollector>,
    pub session_id: &'a str,
    pub model_name: &'a str,
}

// ---- prompt_updates functions ----

const RUNTIME_PROMPT_FLAGS_KEY: &str = "runtime_prompt_component_flags";
const RUNTIME_PROMPT_LENGTHS_KEY: &str = "runtime_prompt_component_lengths";
const RUNTIME_PROMPT_SECTION_LAYOUT_KEY: &str = "runtime_prompt_section_layout";

pub(crate) async fn refresh_round_prompt_context(
    session: &mut Session,
    prompt_memory_flags: crate::runtime::config::PromptMemoryFlags,
    runtime_context: Option<&PromptMemoryRuntimeContext>,
) {
    inject_external_memory_into_system_message(session, prompt_memory_flags, runtime_context).await;
    // Task list, goal, plan-mode, and plan-runtime context are NOT injected into
    // the system message — they are built as dedicated volatile blocks directly
    // from session state during request assembly (cache-stable system prefix).

    let session_id = session.id.clone();
    let prompt_for_metadata = session
        .messages
        .iter_mut()
        .find(|message| matches!(message.role, Role::System))
        .map(|system_message| system_message.content.clone());

    if let Some(prompt) = prompt_for_metadata {
        persist_round_prompt_metadata(session, &prompt);
        log_round_prompt_refresh_summary(session_id.as_str(), &prompt);
    }
}

// ---- round_state functions ----

pub(super) fn update_task_round_state(
    task_context: &mut Option<TaskLoopContext>,
    round: usize,
    max_rounds: usize,
) {
    if let Some(ctx) = task_context.as_mut() {
        ctx.current_round = round as u32;
        ctx.max_rounds = max_rounds as u32;
    }
}

pub(super) fn build_round_id(session_id: &str, round: usize) -> String {
    format!("{}-round-{}", session_id, round + 1)
}

pub(super) fn log_round_start(
    debug_enabled: bool,
    session_id: &str,
    round: usize,
    max_rounds: usize,
    message_count: usize,
) {
    if debug_enabled {
        tracing::debug!(
            "[{}] round_start: {}",
            session_id,
            serde_json::json!({
                "round": round + 1,
                "total_rounds": max_rounds,
                "message_count": message_count,
            })
        );
    }
}

// ---- cancellation ----

fn ensure_not_cancelled(
    cancel_token: &CancellationToken,
    metrics_collector: Option<&MetricsCollector>,
    session_id: &str,
    message_count: usize,
) -> Result<(), AgentError> {
    if cancel_token.is_cancelled() {
        super::metrics_lifecycle::record_session_cancelled(
            metrics_collector,
            session_id,
            message_count as u32,
        );
        return Err(AgentError::Cancelled);
    }
    Ok(())
}

// ---- prompt metadata ----

fn persist_round_prompt_metadata(session: &mut Session, prompt: &str) {
    // Task list and external memory are sourced from session state/field (not
    // reparsed from system-message markers), since they ride volatile blocks now.
    let task_list_text = session.format_task_list_for_prompt();
    let external_memory = super::prompt_context::render_external_memory_section(session);
    let sections = build_round_prompt_sections(
        prompt,
        &task_list_text,
        external_memory.as_deref().unwrap_or_default(),
    );
    let report = PromptAssemblyReport::from_sections(sections, prompt);
    session.metadata.insert(
        RUNTIME_PROMPT_FLAGS_KEY.to_string(),
        report.component_flags_value(),
    );
    session.metadata.insert(
        RUNTIME_PROMPT_LENGTHS_KEY.to_string(),
        report.component_lengths_value(),
    );
    session.metadata.insert(
        RUNTIME_PROMPT_SECTION_LAYOUT_KEY.to_string(),
        report.section_layout_value(),
    );

    let task_list = (!task_list_text.trim().is_empty()).then(|| task_list_text.clone());

    let mut snapshot = super::session_setup::prompt_setup::read_prompt_snapshot_metadata(session)
        .unwrap_or_else(|| PromptSnapshot {
            base_system_prompt: session
                .metadata
                .get("base_system_prompt")
                .cloned()
                .unwrap_or_default(),
            enhancement_prompt: session.enhance_prompt(),
            workspace_context: session.workspace_path_meta().and_then(|workspace_path| {
                crate::runtime::context::build_workspace_prompt_context(&workspace_path)
            }),
            instruction_context: session.workspace_path_meta().and_then(|workspace_path| {
                crate::runtime::context::instruction::build_instruction_prompt_context(
                    &workspace_path,
                )
            }),
            env_context: None,
            skill_context: None,
            tool_guide_context: None,
            dream_notebook: None,
            session_memory_note: None,
            project_memory_index: None,
            relevant_durable_memories: None,
            project_dream: None,
            global_dream_fallback: None,
            prompt_memory_observability: None,
            external_memory: None,
            task_list: None,
            effective_system_prompt: prompt.trim().to_string(),
        });
    let external_memory_parts =
        bamboo_agent_core::parse_prompt_external_memory_sections(external_memory.as_deref());
    snapshot.dream_notebook = external_memory_parts.dream_notebook;
    snapshot.session_memory_note = external_memory_parts.session_memory_note;
    snapshot.project_memory_index = external_memory_parts.project_memory_index;
    snapshot.relevant_durable_memories = external_memory_parts.relevant_durable_memories;
    snapshot.project_dream = external_memory_parts.project_dream;
    snapshot.global_dream_fallback = external_memory_parts.global_dream_fallback;
    snapshot.prompt_memory_observability = session
        .metadata
        .get(PROMPT_MEMORY_OBSERVABILITY_KEY)
        .and_then(|raw| {
            serde_json::from_str::<bamboo_agent_core::PromptMemoryObservability>(raw).ok()
        });
    snapshot.external_memory = external_memory;
    snapshot.task_list = task_list;
    snapshot.effective_system_prompt = prompt.trim().to_string();
    persist_prompt_snapshot_metadata(session, snapshot);
}

fn build_round_prompt_sections(
    prompt: &str,
    task_list: &str,
    external_memory: &str,
) -> Vec<super::session_setup::prompt_setup::PromptSection> {
    use super::session_setup::prompt_setup::{PromptLayer, PromptSection};

    vec![
        PromptSection::new("round_base_prompt", PromptLayer::CoreStatic, false, prompt),
        PromptSection::new(
            "external_memory",
            PromptLayer::EnvironmentWorkspace,
            true,
            external_memory,
        ),
        PromptSection::new(
            "task_list",
            PromptLayer::EnvironmentWorkspace,
            true,
            task_list,
        ),
    ]
}

fn log_round_prompt_refresh_summary(session_id: &str, prompt: &str) {
    tracing::info!(
        "[{}] Round prompt refresh summary: effective_len={} chars",
        session_id,
        prompt.len(),
    );
}

// ---- Main prepare_round function (for lifecycle adapter) ----

pub(crate) async fn prepare_round(
    session: &mut Session,
    task_context: &mut Option<TaskLoopContext>,
    config: &AgentLoopConfig,
    llm: Arc<dyn LLMProvider>,
    _tools: &dyn ToolExecutor,
    frame: &RoundPreludeFrame<'_>,
) -> Result<String, AgentError> {
    // Bind frame fields as locals so the rest of the function body stays unchanged.
    let round = frame.round;
    let max_rounds = frame.max_rounds;
    let cancel_token = frame.cancel_token;
    let metrics_collector = frame.metrics_collector;
    let session_id = frame.session_id;
    let model_name = frame.model_name;
    let debug_enabled = frame.debug_enabled;

    let runtime_context = PromptMemoryRuntimeContext {
        llm: config.background_model_provider.clone().unwrap_or(llm),
        background_model_name: config.background_model_name.clone(),
    };
    refresh_round_prompt_context(session, config.prompt_memory_flags, Some(&runtime_context)).await;
    update_task_round_state(task_context, round, max_rounds);

    let round_id = build_round_id(session_id, round);
    log_round_start(
        debug_enabled,
        session_id,
        round,
        max_rounds,
        session.messages.len(),
    );
    ensure_not_cancelled(
        cancel_token,
        metrics_collector,
        session_id,
        session.messages.len(),
    )?;

    super::metrics_lifecycle::record_round_started(
        metrics_collector,
        &round_id,
        session_id,
        model_name,
    );

    Ok(round_id)
}
