//! Minimal round prelude — provides `prepare_round` for lifecycle adapter
//! and `refresh_round_prompt_context` for the pipeline.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::metrics::MetricsCollector;
use crate::runtime::config::AgentLoopConfig;
use crate::runtime::task_context::TaskLoopContext;
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentError, Role, Session};
use bamboo_infrastructure::LLMProvider;

use super::prompt_context::{
    inject_external_memory_into_system_message, inject_plan_mode_instructions,
    inject_task_list_into_system_message, PromptMemoryRuntimeContext,
    PROMPT_MEMORY_OBSERVABILITY_KEY,
};
use super::session_setup::prompt_setup::{persist_prompt_snapshot_metadata, PromptAssemblyReport};
use bamboo_agent_core::PromptSnapshot;

// ---- prompt_updates functions ----

const EXTERNAL_MEMORY_START_MARKER: &str = "<!-- BAMBOO_EXTERNAL_MEMORY_START -->";
const EXTERNAL_MEMORY_END_MARKER: &str = "<!-- BAMBOO_EXTERNAL_MEMORY_END -->";
const TASK_LIST_START_MARKER: &str = "<!-- BAMBOO_TASK_LIST_START -->";
const TASK_LIST_END_MARKER: &str = "<!-- BAMBOO_TASK_LIST_END -->";
const RUNTIME_PROMPT_FLAGS_KEY: &str = "runtime_prompt_component_flags";
const RUNTIME_PROMPT_LENGTHS_KEY: &str = "runtime_prompt_component_lengths";
const RUNTIME_PROMPT_SECTION_LAYOUT_KEY: &str = "runtime_prompt_section_layout";

pub(crate) async fn refresh_round_prompt_context(
    session: &mut Session,
    prompt_memory_flags: crate::runtime::config::PromptMemoryFlags,
    runtime_context: Option<&PromptMemoryRuntimeContext>,
) {
    inject_external_memory_into_system_message(session, prompt_memory_flags, runtime_context).await;
    inject_task_list_into_system_message(session);
    inject_plan_mode_instructions(session);

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
    let sections = build_round_prompt_sections(prompt);
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

    let external_memory = extract_wrapped_section(
        prompt,
        EXTERNAL_MEMORY_START_MARKER,
        EXTERNAL_MEMORY_END_MARKER,
    )
    .map(|section| {
        strip_wrapped_markers(
            &section,
            EXTERNAL_MEMORY_START_MARKER,
            EXTERNAL_MEMORY_END_MARKER,
        )
    });
    let task_list = extract_wrapped_section(prompt, TASK_LIST_START_MARKER, TASK_LIST_END_MARKER)
        .map(|section| {
            strip_wrapped_markers(&section, TASK_LIST_START_MARKER, TASK_LIST_END_MARKER)
        });

    let mut snapshot = super::session_setup::prompt_setup::read_prompt_snapshot_metadata(session)
        .unwrap_or_else(|| PromptSnapshot {
            base_system_prompt: session
                .metadata
                .get("base_system_prompt")
                .cloned()
                .unwrap_or_default(),
            enhancement_prompt: session.metadata.get("enhance_prompt").cloned(),
            workspace_context: session
                .metadata
                .get("workspace_path")
                .and_then(|workspace_path| {
                    crate::runtime::context::build_workspace_prompt_context(workspace_path)
                }),
            instruction_context: session.metadata.get("workspace_path").and_then(
                |workspace_path| {
                    crate::runtime::context::instruction::build_instruction_prompt_context(
                        workspace_path,
                    )
                },
            ),
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
) -> Vec<super::session_setup::prompt_setup::PromptSection> {
    use super::session_setup::prompt_setup::{PromptLayer, PromptSection};

    let external_memory = extract_wrapped_section(
        prompt,
        EXTERNAL_MEMORY_START_MARKER,
        EXTERNAL_MEMORY_END_MARKER,
    )
    .unwrap_or_default();
    let task_list = extract_wrapped_section(prompt, TASK_LIST_START_MARKER, TASK_LIST_END_MARKER)
        .unwrap_or_default();

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

fn extract_wrapped_section(prompt: &str, start_marker: &str, end_marker: &str) -> Option<String> {
    let start_idx = prompt.find(start_marker)?;
    let section_start = start_idx + start_marker.len();
    let end_rel_idx = prompt[section_start..].find(end_marker)?;
    let section_end = section_start + end_rel_idx;
    let section = prompt[start_idx..section_end + end_marker.len()].trim();
    (!section.is_empty()).then(|| section.to_string())
}

fn strip_wrapped_markers(section: &str, start_marker: &str, end_marker: &str) -> String {
    section
        .trim()
        .trim_start_matches(start_marker)
        .trim_end_matches(end_marker)
        .trim()
        .to_string()
}

fn log_round_prompt_refresh_summary(session_id: &str, prompt: &str) {
    let external_memory_len = wrapped_section_len(
        prompt,
        EXTERNAL_MEMORY_START_MARKER,
        EXTERNAL_MEMORY_END_MARKER,
    );
    let task_list_len = wrapped_section_len(prompt, TASK_LIST_START_MARKER, TASK_LIST_END_MARKER);

    tracing::info!(
        "[{}] Round prompt refresh summary: effective_len={} chars, has_external_memory={}, external_memory_len={}, has_task_list={}, task_list_len={}",
        session_id,
        prompt.len(),
        external_memory_len > 0,
        external_memory_len,
        task_list_len > 0,
        task_list_len,
    );
}

fn wrapped_section_len(prompt: &str, start_marker: &str, end_marker: &str) -> usize {
    let Some(start_idx) = prompt.find(start_marker) else {
        return 0;
    };
    let section_start = start_idx + start_marker.len();
    let Some(end_rel_idx) = prompt[section_start..].find(end_marker) else {
        return 0;
    };
    prompt[section_start..section_start + end_rel_idx]
        .trim()
        .len()
}

// ---- Main prepare_round function (for lifecycle adapter) ----

pub(crate) async fn prepare_round(
    session: &mut Session,
    task_context: &mut Option<TaskLoopContext>,
    round: usize,
    max_rounds: usize,
    cancel_token: &CancellationToken,
    metrics_collector: Option<&MetricsCollector>,
    session_id: &str,
    model_name: &str,
    debug_enabled: bool,
    config: &AgentLoopConfig,
    llm: Arc<dyn LLMProvider>,
    _tools: &dyn ToolExecutor,
) -> Result<String, AgentError> {
    let runtime_context = PromptMemoryRuntimeContext {
        llm,
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
