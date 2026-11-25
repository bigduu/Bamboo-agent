//! LLM round lifecycle helpers for the agent loop runner.

use std::borrow::Cow;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::stream::handler::StreamHandlingOutput;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_agent_core::{AgentError, AgentEvent, Role, Session};
use bamboo_config::ContextManagementStrategy;
use bamboo_domain::{
    CapabilityLoadingClass, CapabilityLoadingMode, ClassifiedToolIdentity, ProviderFamily,
    ProviderProtocol, ProviderTranscriptGroup, ProviderTranscriptItem, ProviderTranscriptItemKind,
    ReasoningEffort, SessionKind,
};
use bamboo_llm::provider::{
    OutputLimitRecovery, RequestToolSelection, REASONING_REPETITION_FINISH_REASON,
};
use bamboo_llm::providers::gemini::{reasoning_control_for_model, GeminiReasoningControl};
use bamboo_llm::LLMProvider;
use bamboo_metrics::TokenUsage as MetricsTokenUsage;

use token_estimation::estimate_completion_tokens;

mod context_ledger;
mod context_preparation;
mod prefix_drift;
mod stream_execution;
mod token_budget;
pub(crate) use token_budget::{configured_model_ceilings, resolve_known_token_ceilings};
mod token_estimation;
#[cfg(test)]
mod tool_loading_http_tests;

pub(crate) use context_preparation::force_overflow_context_recovery;
pub(crate) use context_preparation::stage_user_context_compaction;
#[cfg(test)]
pub(in crate::runtime::runner) use stream_execution::effective_tool_schemas;
pub(in crate::runtime::runner) use stream_execution::required_tool_for_session;
pub(crate) use stream_execution::{
    discard_latest_interrupted_assistant_output, worker_timeout_usage,
};

/// Select the highest tier for a known provider/model. Alias routes retain the
/// canonical Max request unless their adapter prescribes a lower wire ceiling.
fn ultra_reasoning_effort(provider_type: Option<&str>, model: &str) -> Option<ReasoningEffort> {
    let model = model.rsplit('/').next().unwrap_or(model);
    let belongs_to = |family: &str| {
        model == family
            || model
                .strip_prefix(family)
                .is_some_and(|suffix| suffix.starts_with('-'))
    };
    // These routes have no named reasoning tier in their native request.
    if ["gpt-4.1", "gpt-4o", "gpt-3.5-turbo"]
        .iter()
        .any(|family| belongs_to(family))
        || model == "gemini-2.5"
    {
        return None;
    }
    if matches!(provider_type.map(str::trim), Some("anthropic" | "bodhi"))
        && legacy_claude_model(model)
    {
        // The Messages adapters pair a named effort with adaptive thinking.
        // Earlier Claude models use extended thinking and reject that mode.
        return None;
    }
    match reasoning_control_for_model(model) {
        GeminiReasoningControl::Level => return Some(ReasoningEffort::High),
        GeminiReasoningControl::Budget | GeminiReasoningControl::Unsupported => return None,
        GeminiReasoningControl::Unknown if provider_type == Some("gemini") => {
            return Some(ReasoningEffort::High);
        }
        GeminiReasoningControl::Unknown => {}
    }
    if [
        "gpt-5.2",
        "gpt-5.3-codex",
        "gpt-5.4",
        "gpt-5.5",
        "gpt-5.1-codex-max",
    ]
    .iter()
    .any(|family| belongs_to(family))
    {
        return Some(ReasoningEffort::Xhigh);
    }
    if ["o1", "o3", "o4-mini", "gpt-5.1", "gpt-5", "claude-opus-4-5"]
        .iter()
        .any(|family| belongs_to(family))
    {
        return Some(ReasoningEffort::High);
    }
    Some(ReasoningEffort::Max)
}

fn legacy_claude_model(model: &str) -> bool {
    // Dated snapshots and moving aliases use the same wire configuration as
    // their family. Match exact 4.x families so newer tiers stay enabled.
    let model = model.strip_suffix("-latest").unwrap_or(model);
    let family = model
        .rsplit_once('-')
        .filter(|(_, suffix)| suffix.len() == 8 && suffix.bytes().all(|byte| byte.is_ascii_digit()))
        .map_or(model, |(family, _)| family);
    matches!(
        family,
        "claude-opus-4"
            | "claude-opus-4-1"
            | "claude-opus-4-5"
            | "claude-sonnet-4"
            | "claude-sonnet-4-5"
            | "claude-haiku-4-5"
            | "claude-2"
            | "claude-3"
    ) || family.starts_with("claude-3-")
        || family.starts_with("claude-2.")
        || family.starts_with("claude-instant-")
}

#[cfg(test)]
#[test]
fn ultra_effort_uses_the_selected_routes_named_ceiling() {
    for (provider, model, expected) in [
        ("openai", "gpt-6.2-sol", Some(ReasoningEffort::Max)),
        ("openai", "gpt-5.5-2026-04-23", Some(ReasoningEffort::Xhigh)),
        ("openai", "o3", Some(ReasoningEffort::High)),
        ("openai", "gpt-4.1-mini", None),
        (
            "gemini",
            "models/gemini-3.1-pro",
            Some(ReasoningEffort::High),
        ),
        ("gemini", "models/gemini-2.5-flash", None),
        ("bodhi", "gemini-2.0-flash", None),
        ("bodhi", "gemini-3.5-flash", Some(ReasoningEffort::High)),
        ("bodhi", "gemini-flash-latest", Some(ReasoningEffort::High)),
        ("anthropic", "claude-3-7-sonnet-20250219", None),
        ("anthropic", "claude-sonnet-4-20250514", None),
        ("anthropic", "claude-opus-4-1-20250805", None),
        ("anthropic", "claude-opus-4-5", None),
        ("anthropic", "claude-sonnet-4-5-latest", None),
        ("bodhi", "claude-haiku-4-5-20251001", None),
        ("anthropic", "claude-sonnet-4-6", Some(ReasoningEffort::Max)),
        (
            "anthropic",
            "claude-opus-4-8-20260830",
            Some(ReasoningEffort::Max),
        ),
        (
            "openai",
            "anthropic/claude-opus-4-5",
            Some(ReasoningEffort::High),
        ),
        ("openai", "local-alias", Some(ReasoningEffort::Max)),
    ] {
        assert_eq!(
            ultra_reasoning_effort(Some(provider), model),
            expected,
            "{model}"
        );
    }
}

pub(in crate::runtime::runner) fn is_openai_client_tool_search_boundary(
    items: &[bamboo_domain::ProviderTranscriptItem],
) -> bool {
    items.iter().any(|item| {
        item.family() == bamboo_domain::ProviderFamily::OpenAi
            && item.protocol() == bamboo_domain::ProviderProtocol::OpenAiResponsesV1
            && item.kind() == bamboo_domain::ProviderTranscriptItemKind::OpenAiToolSearchCall
            && item.payload()["execution"].as_str() == Some("client")
    })
}

fn tool_choice_requirement_met(
    selection: &RequestToolSelection,
    mode: CapabilityLoadingMode,
    has_tool_calls: bool,
    native_items: &[ProviderTranscriptItem],
) -> bool {
    if !selection.requires_call() || has_tool_calls {
        return true;
    }
    matches!(selection, RequestToolSelection::Required)
        && mode == CapabilityLoadingMode::Progressive
        && native_items.iter().any(|item| {
            matches!(
                (item.family(), item.protocol(), item.kind()),
                (
                    ProviderFamily::OpenAi,
                    ProviderProtocol::OpenAiResponsesV1,
                    ProviderTranscriptItemKind::OpenAiToolSearchCall
                ) | (
                    ProviderFamily::Anthropic,
                    ProviderProtocol::AnthropicMessages2023_06_01,
                    ProviderTranscriptItemKind::AnthropicServerToolUse
                )
            )
        })
        && ProviderTranscriptGroup::validate_items(native_items).is_ok()
}

fn rejected_output_limit(
    output: &StreamHandlingOutput,
    finish_reason: &str,
    task_split: bool,
) -> bool {
    // Responses explicitly marks this frame incomplete. A task split also
    // requires a complete plan, checkpoint or report on the other protocols.
    finish_reason == "max_output_tokens"
        || (matches!(finish_reason, "max_tokens" | "length" | "MAX_TOKENS")
            && (task_split
                || output.content.trim().is_empty()
                || !output.tool_calls.is_empty()
                || output.provider_transcript_items.iter().any(|item| {
                    item.kind().is_discovery()
                        || matches!(
                            item.kind(),
                            ProviderTranscriptItemKind::OpenAiFunctionCall
                                | ProviderTranscriptItemKind::AnthropicToolUse
                        )
                })))
}

pub(super) async fn request_tool_policy(
    llm: &dyn LLMProvider,
    model: &str,
    required_tool: Option<&str>,
) -> Result<(CapabilityLoadingMode, RequestToolSelection), AgentError> {
    let configured = llm
        .request_tool_selection(model)
        .await
        .map_err(|error| AgentError::LLMNonRetryable(error.to_string()))?;
    let selection = if let Some(name) = required_tool {
        if !configured.allows(name) {
            return Err(AgentError::LLMNonRetryable(format!(
                "required tool '{name}' is excluded by the provider request's tool selection"
            )));
        }
        RequestToolSelection::Forced {
            name: name.to_owned(),
            exactly_one: true,
        }
    } else {
        configured
    };
    let mode = if selection.restricts_names() {
        // Named definitions must be present in the provider's initial tools.
        // The catalog for this mode is reduced to the selected names first.
        CapabilityLoadingMode::LegacyFullCatalog
    } else {
        match llm.capability_loading_mode(model, None).await {
            CapabilityLoadingMode::StickyFallbackForRootChat => {
                CapabilityLoadingMode::StickyFallback
            }
            mode => mode,
        }
    };
    Ok((mode, selection))
}

pub(super) fn uses_sticky_fallback(mode: CapabilityLoadingMode) -> bool {
    matches!(
        mode,
        CapabilityLoadingMode::StickyFallback | CapabilityLoadingMode::StickyFallbackForRootChat
    )
}

pub(super) fn select_request_tool_schemas<'a>(
    session: &Session,
    tool_schemas: &'a [ToolSchema],
    selection: &RequestToolSelection,
) -> Result<Cow<'a, [ToolSchema]>, AgentError> {
    let eligible = |schema: &ToolSchema| {
        let name = &schema.function.name;
        selection.allows(name)
            && session.allows_model_tool_execution(name)
            && ClassifiedToolIdentity::from_schema_name(name)
                .is_some_and(|tool| tool.loading_class() != CapabilityLoadingClass::HostOnly)
    };
    let selected = if tool_schemas.iter().all(eligible) {
        Cow::Borrowed(tool_schemas)
    } else {
        Cow::Owned(
            tool_schemas
                .iter()
                .filter(|schema| eligible(schema))
                .cloned()
                .collect(),
        )
    };
    if let Some(name) = selection
        .names()
        .iter()
        .find(|name| !selected.iter().any(|tool| tool.function.name == **name))
    {
        return Err(AgentError::LLMNonRetryable(format!(
            "requested tool '{name}' is unavailable under the current tool policy"
        )));
    }
    Ok(selected)
}

pub(in crate::runtime::runner) fn fallback_tool_is_resident(session: &Session, name: &str) -> bool {
    ClassifiedToolIdentity::from_schema_name(name)
        .is_some_and(|tool| tool.loading_class() == CapabilityLoadingClass::Core)
        || match name {
            // These controls have runtime contracts for delegation, private
            // steps, active goals and suspended/legacy loops.
            "SubAgent" => session.supervisor_incarnation_id().is_none(),
            "Task" | "update_goal" | "loop_control" | "ExitPlanMode" => true,
            "skills_list" | "skills_read" => {
                session.kind == SessionKind::Root && !session.model_orchestration_only_enabled()
            }
            _ => false,
        }
}

pub(super) fn request_tool_schemas_for_loading_mode<'a>(
    session: &Session,
    tool_schemas: &'a [ToolSchema],
    mode: CapabilityLoadingMode,
    allow_discovery: bool,
) -> Cow<'a, [ToolSchema]> {
    if !uses_sticky_fallback(mode) {
        return Cow::Borrowed(tool_schemas);
    }

    let loaded =
        crate::runtime::runner::loop_execution::validated_sticky_fallback_loaded_tool_names(
            session,
            tool_schemas,
        );
    let mut projected = tool_schemas
        .iter()
        .filter(|schema| {
            let name = &schema.function.name;
            fallback_tool_is_resident(session, name) || loaded.contains(name)
        })
        .cloned()
        .collect::<Vec<_>>();
    // The gateway remains available for new queries after an earlier lookup.
    if allow_discovery
        && tool_schemas
            .iter()
            .any(|schema| !fallback_tool_is_resident(session, &schema.function.name))
    {
        projected.push(bamboo_domain::discovery_control_fallback_schema());
    }
    Cow::Owned(projected)
}

#[cfg(test)]
pub(crate) async fn request_tool_schemas_for_session<'a>(
    session: &Session,
    llm: &Arc<dyn LLMProvider>,
    model_name: &str,
    tool_schemas: &'a [ToolSchema],
) -> Vec<ToolSchema> {
    let required_tool = required_tool_for_session(session);
    let (mode, selection) = request_tool_policy(llm.as_ref(), model_name, required_tool)
        .await
        .unwrap();
    let eligible = select_request_tool_schemas(session, tool_schemas, &selection).unwrap();
    request_tool_schemas_for_loading_mode(session, eligible.as_ref(), mode, true).into_owned()
}

pub(super) struct RoundToolDispatch {
    pub mode: CapabilityLoadingMode,
    pub catalog: Vec<ToolSchema>,
    pub fallback_discovery: bool,
    pub selection: RequestToolSelection,
}

pub(crate) struct RoundLlmExecutionOutput {
    pub stream_output: StreamHandlingOutput,
    pub(super) tool_dispatch: RoundToolDispatch,
    pub standalone_supervisor_spawn: bool,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    /// Canonical usage for this single billed provider attempt.
    pub attempt_usage: MetricsTokenUsage,
    /// Validation that can only run after a provider stream has completed.
    ///
    /// The pipeline must absorb `attempt_usage` before surfacing this error so
    /// a billed response is not turned into a zero-usage terminal round.
    pub terminal_validation_error: Option<AgentError>,
    pub output_limit_recovery: Option<OutputLimitRecovery>,
    pub can_resume_truncated_messages: bool,
    pub(crate) host_gold_attempt: Option<Arc<crate::runtime::host_gold::HostGoldAttempt>>,
}

#[derive(Clone, Copy)]
pub(crate) struct TaskSplitPrompt<'a> {
    pub advisory: &'a str,
    pub private_plan: bool,
}

#[derive(Clone, Copy)]
pub(crate) struct RoundRecoveryPolicy<'a> {
    pub retry: Option<OutputLimitRecovery>,
    pub remaining_run_tokens: Option<u64>,
    pub previous_prompt_tokens: u64,
    pub task_split: Option<TaskSplitPrompt<'a>>,
    pub standalone_discovery: bool,
    pub round_cap_summary: bool,
}

/// Trusted execution-local prompt-memory snapshot paired with the stable
/// execution-scoped round identity used for durable metrics deduplication.
///
/// `None` means this execution path does not implement #1077 capture. It must
/// not be converted into a supported zero-exposure observation.
#[derive(Clone, Copy)]
pub(crate) struct PromptMemoryExposureFrame<'a> {
    pub round_id: &'a str,
    pub provenance: &'a crate::runtime::runner::prompt_context::PromptMemoryExposureProvenance,
}

/// Resolve one billed attempt's canonical token usage.
///
/// Availability is decided independently for prompt and completion tokens:
/// an authoritative provider snapshot wins (including an explicit zero), a
/// non-zero legacy provider counter is the compatibility fallback, and the
/// local tokenizer estimate is used only when that component was not reported.
/// The total is always recomputed from the selected components so runtime
/// budgets and durable metrics cannot disagree with an inconsistent provider
/// `total_tokens` field.
pub(crate) fn canonical_attempt_usage(
    stream_output: &StreamHandlingOutput,
    estimated_prompt_tokens: u64,
    estimated_completion_tokens: u64,
) -> MetricsTokenUsage {
    canonical_token_usage(
        stream_output.provider_usage,
        stream_output.input_tokens,
        stream_output.output_tokens,
        estimated_prompt_tokens,
        estimated_completion_tokens,
    )
}

pub(crate) fn canonical_token_usage(
    provider_usage: Option<crate::runtime::stream::handler::ProviderUsageSnapshot>,
    legacy_prompt_tokens: u64,
    legacy_completion_tokens: u64,
    estimated_prompt_tokens: u64,
    estimated_completion_tokens: u64,
) -> MetricsTokenUsage {
    let prompt_tokens = provider_usage
        .and_then(|usage| usage.input_tokens)
        .or_else(|| (legacy_prompt_tokens > 0).then_some(legacy_prompt_tokens))
        .unwrap_or(estimated_prompt_tokens);
    let completion_tokens = provider_usage
        .and_then(|usage| usage.output_tokens)
        .or_else(|| (legacy_completion_tokens > 0).then_some(legacy_completion_tokens))
        .unwrap_or(estimated_completion_tokens);

    MetricsTokenUsage {
        prompt_tokens,
        completion_tokens,
        total_tokens: prompt_tokens.saturating_add(completion_tokens),
    }
    .clamped_for_durable_metrics()
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_llm_round(
    session: &mut Session,
    config: &AgentLoopConfig,
    llm: &Arc<dyn LLMProvider>,
    event_tx: &mpsc::Sender<AgentEvent>,
    cancel_token: &CancellationToken,
    session_id: &str,
    model_name: &str,
    tool_schemas: &[ToolSchema],
    observation_progress_hint: Option<&str>,
    prompt_memory_exposure: Option<PromptMemoryExposureFrame<'_>>,
    output_limit_recovery: Option<RoundRecoveryPolicy<'_>>,
) -> Result<RoundLlmExecutionOutput, AgentError> {
    // Clear the previous receipt even when preflight or admission fails.
    let host_gold_attempt = config.host_gold_run.as_ref().map(|run| run.begin());
    let round_cap_summary = output_limit_recovery.is_some_and(|policy| policy.round_cap_summary);
    if round_cap_summary {
        // This request and any later resume must use the canonical transcript.
        // Clear before context preflight as well as provider transport failures.
        session
            .metadata
            .remove(stream_execution::SESSION_RESPONSES_PREVIOUS_RESPONSE_ID_KEY);
    }
    // Resolve mutable host handles once for the whole request. The model
    // catalog, capability projection and provider send must use one route.
    let request_provider = bamboo_llm::provider::queued_snapshot_for(llm.clone())
        .map_err(|error| AgentError::LLM(error.to_string()))?;
    let llm = &request_provider;
    // Scope prepared image descriptions to this request. A configuration reused
    // across rounds must never serve a description produced for another request.
    config.vision_description_reuse.clear();
    let is_recovery = output_limit_recovery.is_some_and(|policy| policy.retry.is_some());
    let prepared = if is_recovery {
        context_preparation::prepare_output_limit_recovery_context(
            session,
            config,
            model_name,
            session_id,
            tool_schemas,
            llm,
        )
        .await?
    } else {
        context_preparation::prepare_round_context(
            session,
            config,
            model_name,
            session_id,
            tool_schemas,
            llm,
            Some(event_tx),
        )
        .await?
    };

    // Use model from config (provided by execute request), not from session.
    let model = config
        .model_name
        .as_deref()
        .ok_or_else(|| AgentError::LLM("model_name is required in AgentLoopConfig".to_string()))?;

    let highest_reasoning = session.root_orchestration_only_enabled()
        || (session.kind == SessionKind::Child
            && config.ticket_worker_plan.is_some()
            && crate::ticket_runtime::read_dispatch(session)
                .ok()
                .flatten()
                .is_some_and(|dispatch| dispatch.highest_reasoning));
    let frame = stream_execution::LlmStreamFrame {
        event_tx,
        cancel_token,
        session_id,
        model,
        provider_name: config.provider_name.as_deref(),
        provider_type: config.provider_type.as_deref(),
        reasoning_effort: if highest_reasoning {
            ultra_reasoning_effort(config.provider_type.as_deref(), model)
        } else {
            config.reasoning_effort
        },
        output_limit_recovery,
        budget: &prepared.budget,
        provider_default_output: prepared.provider_default_output,
        observation_progress_hint,
        prompt_memory_exposure,
    };

    let execution = stream_execution::execute_llm_stream(
        session,
        config,
        llm,
        &prepared.prepared_context,
        tool_schemas,
        &frame,
    );
    let (stream_output, llm_duration, prompt_tokens, completion, tool_dispatch) =
        match host_gold_attempt.as_ref() {
            Some(attempt) => {
                bamboo_llm::provider::scope_main_dispatch(attempt.observer(), execution).await?
            }
            None => execution.await?,
        };

    // This is a terminal validation error, but the completed stream was still
    // billed. Return it alongside the canonical attempt usage so the runner can
    // account for the attempt before ending the round.
    let openai_client_search_boundary =
        is_openai_client_tool_search_boundary(&stream_output.provider_transcript_items);
    let empty_answer = stream_output.tool_calls.is_empty()
        && stream_output.content.trim().is_empty()
        && !openai_client_search_boundary;
    let finish_reason = completion.finish_reason();
    let task_split = output_limit_recovery.is_some_and(|policy| policy.task_split.is_some());
    let output_limited = rejected_output_limit(
        &stream_output,
        finish_reason,
        task_split || round_cap_summary,
    );
    let reasoning_repeated = finish_reason == REASONING_REPETITION_FINISH_REASON;
    let response_rejected = output_limited || reasoning_repeated;
    let reasoning_only = empty_answer
        && !completion.saw_provider_tool()
        && (reasoning_repeated
            || !stream_output.reasoning_content.trim().is_empty()
            || stream_output.thinking_tokens > 0
            || !stream_output.provider_transcript_items.is_empty())
        && stream_output.provider_transcript_items.iter().all(|item| {
            matches!(
                item.kind(),
                ProviderTranscriptItemKind::OpenAiReasoning
                    | ProviderTranscriptItemKind::AnthropicThinking
                    | ProviderTranscriptItemKind::AnthropicRedactedThinking
            )
        });
    // The capped summary path independently discards all unexpected tool
    // output. It can still keep a complete text response alongside it.
    let terminal_validation_error = if !round_cap_summary
        && (stream_output
            .tool_calls
            .iter()
            .any(|call| !tool_dispatch.selection.allows(&call.function.name))
            || (openai_client_search_boundary
                && tool_dispatch.mode != CapabilityLoadingMode::Progressive))
    {
        Some(AgentError::LLMNonRetryable(
            "provider returned a tool call outside the current request's tool selection".into(),
        ))
    } else if reasoning_repeated {
        Some(AgentError::ReasoningRepetition {
            response_id: stream_output.response_id.clone(),
        })
    } else if output_limited {
        Some(AgentError::OutputTokenLimit {
            finish_reason,
            response_id: stream_output.response_id.clone(),
            reasoning_only,
        })
    } else if matches!(
        tool_dispatch.selection,
        RequestToolSelection::Forced {
            exactly_one: true,
            ..
        }
    ) && stream_output.tool_calls.len() > 1
    {
        Some(AgentError::LLMNonRetryable(
            "provider returned multiple tool calls for a request that requires exactly one".into(),
        ))
    } else if !tool_choice_requirement_met(
        &tool_dispatch.selection,
        tool_dispatch.mode,
        !stream_output.tool_calls.is_empty(),
        &stream_output.provider_transcript_items,
    ) {
        Some(AgentError::LLMNonRetryable(
            "provider completed without the tool call required by this request".into(),
        ))
    } else if is_recovery && !completion.has_recovery_completion() {
        Some(AgentError::LLMNonRetryable(
            "output-limit recovery ended without a successful provider completion".into(),
        ))
    } else {
        empty_answer.then(|| AgentError::EmptyAssistantResponse {
            response_id: stream_output.response_id.clone(),
        })
    };

    let completion_tokens =
        estimate_completion_tokens(&stream_output.content, &stream_output.tool_calls);
    // Missing usage on these paths can hide reasoning spend. Keep reported
    // usage authoritative and reserve the entire wire cap as the fallback.
    let estimated_output = if response_rejected || is_recovery {
        let reasoning_estimate = estimate_completion_tokens(&stream_output.reasoning_content, &[]);
        completion_tokens
            .saturating_add(reasoning_estimate.max(stream_output.thinking_tokens))
            .max(completion.output_limit().unwrap_or_default())
    } else {
        completion_tokens
    };
    let attempt_usage = if response_rejected || is_recovery {
        // A partial usage snapshot can omit its full prompt total even when
        // the known cached input is large. Reserve at least that known sum
        // and the projected wire input when the total is missing.
        let known_input = stream_output
            .input_tokens
            .saturating_add(stream_output.cache_creation_input_tokens)
            .saturating_add(stream_output.cache_read_input_tokens);
        let known_output = if completion.legacy_counts_reasoning_separately() {
            let missing_candidates = stream_output.output_tokens == 0 && completion_tokens > 0;
            let missing_thoughts = stream_output.thinking_tokens == 0
                && !stream_output.reasoning_content.trim().is_empty();
            if missing_candidates || missing_thoughts {
                0 // The aggregate is unknown. Use the conservative reserve.
            } else {
                stream_output
                    .output_tokens
                    .saturating_add(stream_output.thinking_tokens)
            }
        } else {
            stream_output.output_tokens
        };
        let mut usage = stream_output.provider_usage;
        let (known_output, estimated_output) = if reasoning_repeated {
            // An interrupted stream can report a cumulative snapshot without
            // its final usage. Reserve the wire cap even when a partial count
            // exists, while keeping that count as an additional lower bound.
            let partial_output = usage
                .as_mut()
                .and_then(|usage| usage.output_tokens.take())
                .unwrap_or_default();
            (0, estimated_output.max(known_output).max(partial_output))
        } else {
            (known_output, estimated_output)
        };
        canonical_token_usage(
            usage,
            0,
            known_output,
            prompt_tokens.max(known_input),
            estimated_output,
        )
    } else {
        canonical_attempt_usage(&stream_output, prompt_tokens, estimated_output)
    };
    // A compatible Messages gateway can hide its thinking or leave a
    // provisional client tool call at the cap. Discarding this turn is safe
    // only when it contains no provider-side work.
    let can_resume_truncated_messages = response_rejected
        && completion.is_compatible_messages()
        && !completion.saw_provider_tool()
        && stream_output
            .provider_transcript_items
            .iter()
            .all(|item| !item.kind().is_discovery());
    let output_limit_recovery = completion
        .output_limit_recovery()
        .filter(|_| response_rejected && (can_resume_truncated_messages || reasoning_only));

    tracing::debug!(
        "[{}] LLM response completed in {}ms, answer_chars={}, reasoning_chars={}, estimated_tokens={}, canonical_tokens={}",
        session_id,
        llm_duration,
        stream_output.token_count,
        stream_output.reasoning_content.len(),
        prompt_tokens.saturating_add(completion_tokens),
        attempt_usage.total_tokens,
    );

    Ok(RoundLlmExecutionOutput {
        stream_output,
        standalone_supervisor_spawn:
            super::session_setup::tool_schemas::offers_standalone_supervisor_spawn(
                session,
                &tool_dispatch.catalog,
            ),
        tool_dispatch,
        prompt_tokens,
        completion_tokens,
        attempt_usage,
        terminal_validation_error,
        output_limit_recovery,
        can_resume_truncated_messages,
        host_gold_attempt,
    })
}

pub(crate) async fn maybe_apply_mid_turn_context_compression(
    session: &mut Session,
    config: &AgentLoopConfig,
    llm: &Arc<dyn LLMProvider>,
    event_tx: &mpsc::Sender<AgentEvent>,
    session_id: &str,
    model_name: &str,
    tool_schemas: &[ToolSchema],
) -> Result<bool, AgentError> {
    if config.context_management.strategy == ContextManagementStrategy::MessageSummary {
        // Tool execution reaches this check after each result. Only the latest
        // assistant batch can still be receiving results at this boundary.
        let current_batch = session
            .messages
            .iter()
            .rposition(|message| {
                !message.compressed && matches!(message.role, Role::Assistant | Role::User)
            })
            .map(|start| &session.messages[start..]);
        if current_batch.is_some_and(|batch| {
            !crate::session_app::truncation::unresolved_tool_call_ids(batch).is_empty()
        }) {
            return Ok(false);
        }
    }

    context_preparation::maybe_apply_host_context_compression(
        session,
        config,
        model_name,
        session_id,
        tool_schemas,
        llm,
        Some(event_tx),
        "mid-turn",
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::{
        execute_llm_round, is_openai_client_tool_search_boundary, request_tool_policy,
        request_tool_schemas_for_loading_mode, request_tool_schemas_for_session,
        select_request_tool_schemas, RequestToolSelection,
    };
    use async_trait::async_trait;
    use bamboo_agent_core::tools::{FunctionCall, FunctionSchema, ToolCall, ToolSchema};
    use bamboo_agent_core::{AgentEvent, Message, Session};
    use bamboo_domain::{
        CapabilityLoadingMode, ProviderFamily, ProviderProtocol, ProviderTranscriptAuthor,
        ProviderTranscriptItem, ProviderTranscriptOrigin,
    };
    use bamboo_llm::{LLMChunk, LLMProvider, LLMRequestOptions, LLMStream};
    use futures::stream;
    use serde_json::json;
    use std::sync::{Arc, Mutex};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use crate::runtime::config::AgentLoopConfig;

    fn client_search_item(family: ProviderFamily) -> ProviderTranscriptItem {
        ProviderTranscriptItem::try_from_payload(
            family,
            ProviderProtocol::OpenAiResponsesV1,
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::Model,
            json!({
                "type":"tool_search_call","id":"tsc_boundary","execution":"client",
                "call_id":"search_boundary","status":"completed",
                "arguments":{"query":"files"}
            }),
        )
        .unwrap()
    }

    #[test]
    fn client_search_boundary_is_exactly_openai_responses() {
        assert!(is_openai_client_tool_search_boundary(&[
            client_search_item(ProviderFamily::OpenAi)
        ]));
        assert!(!is_openai_client_tool_search_boundary(&[
            client_search_item(ProviderFamily::Copilot)
        ]));
    }

    #[test]
    fn unnamed_required_selection_accepts_only_valid_native_search_progress() {
        use super::tool_choice_requirement_met;

        let native = |family, protocol, author, payload| {
            ProviderTranscriptItem::try_from_payload(
                family,
                protocol,
                ProviderTranscriptOrigin::Provider,
                author,
                payload,
            )
            .unwrap()
        };
        let responses = |author, payload| {
            native(
                ProviderFamily::OpenAi,
                ProviderProtocol::OpenAiResponsesV1,
                author,
                payload,
            )
        };
        let messages = |author, payload| {
            native(
                ProviderFamily::Anthropic,
                ProviderProtocol::AnthropicMessages2023_06_01,
                author,
                payload,
            )
        };
        let client = vec![client_search_item(ProviderFamily::OpenAi)];
        let hosted = vec![
            responses(
                ProviderTranscriptAuthor::Model,
                json!({
                    "type":"tool_search_call","id":"hosted","call_id":"server-search","execution":"server",
                    "status":"completed","arguments":{"query":"files"}
                }),
            ),
            responses(
                ProviderTranscriptAuthor::ToolResult,
                json!({
                    "type":"tool_search_output","id":"hosted-output","call_id":"server-search",
                    "execution":"server","status":"completed","tools":[]
                }),
            ),
        ];
        let anthropic = vec![
            messages(
                ProviderTranscriptAuthor::Model,
                json!({
                    "type":"server_tool_use","id":"search","name":"tool_search_tool_regex",
                    "input":{"pattern":"files"}
                }),
            ),
            messages(
                ProviderTranscriptAuthor::ToolResult,
                json!({
                    "type":"tool_search_tool_result","tool_use_id":"search",
                    "content":{"type":"tool_search_tool_search_result","tool_references":[]}
                }),
            ),
        ];
        let named = RequestToolSelection::Named {
            names: vec!["Read".into()],
            required: true,
        };
        for items in [&client, &hosted, &anthropic] {
            assert!(tool_choice_requirement_met(
                &RequestToolSelection::Required,
                CapabilityLoadingMode::Progressive,
                false,
                items
            ));
            assert!(!tool_choice_requirement_met(
                &named,
                CapabilityLoadingMode::Progressive,
                false,
                items
            ));
            assert!(!tool_choice_requirement_met(
                &RequestToolSelection::Required,
                CapabilityLoadingMode::StickyFallback,
                false,
                items
            ));
        }
        for items in [&[][..], &hosted[..1], &anthropic[..1]] {
            assert!(!tool_choice_requirement_met(
                &RequestToolSelection::Required,
                CapabilityLoadingMode::Progressive,
                false,
                items
            ));
        }
    }

    struct StickyCapturingProvider {
        mode: CapabilityLoadingMode,
        requests: Mutex<Vec<Vec<ToolSchema>>>,
    }

    #[async_trait]
    impl LLMProvider for StickyCapturingProvider {
        async fn capability_loading_mode(
            &self,
            _model: &str,
            required_tool: Option<&str>,
        ) -> CapabilityLoadingMode {
            if required_tool.is_none() {
                self.mode
            } else {
                CapabilityLoadingMode::LegacyFullCatalog
            }
        }

        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            panic!("the engine must dispatch PromptIR")
        }

        async fn chat_stream_ir(
            &self,
            _ir: &bamboo_llm::PromptIR,
            tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
            _options: Option<&LLMRequestOptions>,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            self.requests.lock().unwrap().push(tools.to_vec());
            Ok(Box::pin(stream::iter(vec![Ok(LLMChunk::Done)])))
        }
    }

    fn schema(name: &str) -> ToolSchema {
        ToolSchema {
            schema_type: "function".to_string(),
            function: FunctionSchema {
                name: name.to_string(),
                description: format!("{name} description"),
                parameters: json!({"type":"object","properties":{}}),
            },
        }
    }

    #[test]
    fn named_selection_declares_only_exact_eligible_tools() {
        let mut ultra = Session::new("forced-ultra", "model");
        ultra.set_root_orchestration_only(true).unwrap();
        let tools = ["Read", "Glob", "browser", "load_skill", "Workspace"].map(schema);
        for (session, names, expected) in [
            (&ultra, vec!["Glob"], Some(vec!["Glob"])),
            (&ultra, vec!["Read", "Glob"], Some(vec!["Read", "Glob"])),
            (&ultra, vec!["Read", "browser"], None),
            (&ultra, vec!["load_skill"], None),
            (&ultra, vec!["Workspace"], None),
            (&ultra, vec!["read_file"], None),
        ] {
            let selection = RequestToolSelection::Named {
                names: names.into_iter().map(str::to_owned).collect(),
                required: false,
            };
            let selected = select_request_tool_schemas(session, &tools, &selection);
            match expected {
                Some(names) => assert_eq!(
                    selected
                        .unwrap()
                        .iter()
                        .map(|tool| tool.function.name.as_str())
                        .collect::<Vec<_>>(),
                    names,
                ),
                None => assert!(selected.is_err()),
            }
        }
        assert!(
            select_request_tool_schemas(&ultra, &tools, &RequestToolSelection::Disabled)
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn sticky_overflow_projection_uses_core_plus_discovery_catalog() {
        let provider = Arc::new(StickyCapturingProvider {
            mode: CapabilityLoadingMode::StickyFallback,
            requests: Mutex::new(Vec::new()),
        });
        let llm: Arc<dyn LLMProvider> = provider;
        let session = Session::new("sticky-overflow-tools", "chat-model");
        let tools = vec![
            schema("Read"),
            schema("ReadArchive"),
            schema("Workspace"),
            schema("exec_command"),
        ];

        let request_tools =
            request_tool_schemas_for_session(&session, &llm, "chat-model", &tools).await;

        assert_eq!(
            request_tools
                .iter()
                .map(|tool| tool.function.name.as_str())
                .collect::<Vec<_>>(),
            vec!["Read", "exec_command", "discover_capabilities"]
        );
        assert!(request_tools.iter().all(|tool| {
            tool.function.name != "ReadArchive" && tool.function.name != "Workspace"
        }));
    }

    #[tokio::test]
    async fn compatibility_providers_load_by_role_for_every_session_kind() {
        use bamboo_domain::{SessionAuthorityIdentity, DEFAULT_SUPERVISOR_SESSION_ID};

        let root = Session::new("chat-root", "chat-model");
        let child = Session::new_child_of("worker", &root, "chat-model", "read files");
        let mut supervisor = Session::new(DEFAULT_SUPERVISOR_SESSION_ID, "chat-model");
        supervisor.authority_identity = SessionAuthorityIdentity::Supervisor {
            incarnation_id: uuid::Uuid::new_v4(),
        };
        let mut ultra = root.clone();
        ultra.set_root_orchestration_only(true).unwrap();
        let mut ticket = root.clone();
        ticket.metadata.insert(
            crate::ticket_worker_plan::TICKET_LOCAL_PLAN_KEY.into(),
            "assignment".into(),
        );
        let schemas = [
            "Read",
            "SubAgent",
            "skills_list",
            "skills_read",
            "Glob",
            "work_overview",
            "session_control",
            "Task",
            "Workspace",
        ]
        .map(schema);
        for provider_mode in [
            CapabilityLoadingMode::StickyFallback,
            CapabilityLoadingMode::StickyFallbackForRootChat,
        ] {
            let provider = StickyCapturingProvider {
                mode: provider_mode,
                requests: Mutex::new(Vec::new()),
            };
            for (session, expected) in [
                (
                    &root,
                    vec![
                        "Read",
                        "SubAgent",
                        "skills_list",
                        "skills_read",
                        "Task",
                        "discover_capabilities",
                    ],
                ),
                (
                    &child,
                    vec!["Read", "SubAgent", "Task", "discover_capabilities"],
                ),
                (
                    &ticket,
                    vec![
                        "Read",
                        "SubAgent",
                        "skills_list",
                        "skills_read",
                        "Task",
                        "discover_capabilities",
                    ],
                ),
                (
                    &ultra,
                    vec!["Read", "SubAgent", "Task", "discover_capabilities"],
                ),
                (
                    &supervisor,
                    vec!["work_overview", "Task", "discover_capabilities"],
                ),
            ] {
                let (mode, selection) = request_tool_policy(&provider, "chat-model", None)
                    .await
                    .unwrap();
                assert_eq!(mode, CapabilityLoadingMode::StickyFallback);
                let eligible = select_request_tool_schemas(session, &schemas, &selection).unwrap();
                let projected =
                    request_tool_schemas_for_loading_mode(session, &eligible, mode, true);
                assert_eq!(
                    projected
                        .iter()
                        .map(|tool| tool.function.name.as_str())
                        .collect::<Vec<_>>(),
                    expected
                );
                let callable =
                    crate::runtime::runner::loop_execution::effective_callable_set_for_round(
                        session, &eligible, mode,
                    );
                assert!(!callable.contains_execution_name("Glob"));
                assert_eq!(
                    callable.contains_execution_name("work_overview"),
                    session.supervisor_incarnation_id().is_some()
                );
            }
        }
    }

    #[tokio::test]
    async fn selected_root_can_call_its_advertised_tools_in_sticky_mode() {
        let provider = Arc::new(StickyCapturingProvider {
            mode: CapabilityLoadingMode::StickyFallback,
            requests: Mutex::new(Vec::new()),
        });
        let llm: Arc<dyn LLMProvider> = provider.clone();
        let mut session = Session::new("selected-root-tools", "chat-model");
        session.set_root_orchestration_only(true).unwrap();
        session.add_message(Message::user("coordinate this task"));
        let config = AgentLoopConfig {
            model_name: Some("chat-model".to_string()),
            ..Default::default()
        };
        let tools = vec![
            schema("Read"),
            schema("SubAgent"),
            schema("Plan"),
            schema("Task"),
            schema("exec_command"),
            schema("write_stdin"),
            schema("discover_capabilities"),
        ];
        let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(16);
        execute_llm_round(
            &mut session,
            &config,
            &llm,
            &event_tx,
            &CancellationToken::new(),
            "selected-root-tools",
            "chat-model",
            &tools,
            None,
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            provider.requests.lock().unwrap()[0]
                .iter()
                .map(|tool| tool.function.name.as_str())
                .collect::<Vec<_>>(),
            vec!["Read", "SubAgent", "Task", "exec_command", "write_stdin"]
        );
        let callable = crate::runtime::runner::loop_execution::effective_callable_set_for_round(
            &session,
            &tools,
            CapabilityLoadingMode::StickyFallback,
        );
        for name in ["Read", "SubAgent", "Task", "exec_command", "write_stdin"] {
            assert!(callable.contains_execution_name(name), "{name}");
        }
        for name in ["Plan", "discover_capabilities"] {
            assert!(!callable.contains_execution_name(name), "{name}");
        }
    }

    #[tokio::test]
    async fn sticky_next_round_declares_only_the_newly_discovered_function() {
        let provider = Arc::new(StickyCapturingProvider {
            mode: CapabilityLoadingMode::StickyFallback,
            requests: Mutex::new(Vec::new()),
        });
        let llm: Arc<dyn LLMProvider> = provider.clone();
        let mut session = Session::new("sticky-round-tools", "chat-model");
        session.add_message(Message::user("find an archive reader"));
        let config = AgentLoopConfig {
            model_name: Some("chat-model".to_string()),
            ..Default::default()
        };
        let tools = vec![
            schema("Read"),
            schema("ReadArchive"),
            schema("Workspace"),
            schema("exec_command"),
        ];
        let (event_tx, _event_rx) = mpsc::channel::<AgentEvent>(16);
        let cancel = CancellationToken::new();

        execute_llm_round(
            &mut session,
            &config,
            &llm,
            &event_tx,
            &cancel,
            "sticky-round-tools",
            "chat-model",
            &tools,
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let mut discovery_call = Message::assistant(
            "",
            Some(vec![ToolCall {
                id: "sticky-loaded-read-archive".to_string(),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: bamboo_domain::DISCOVERY_CONTROL_FALLBACK_TOOL_NAME.to_string(),
                    arguments: r#"{"query":"archive","kinds":["tool"],"limit":1}"#.to_string(),
                },
            }]),
        );
        discovery_call.never_compress = true;
        discovery_call.metadata = Some(json!({
            "runtime_kind":"sticky_capability_discovery",
            "version":1
        }));
        session.add_message(discovery_call);
        let payload = serde_json::to_string(&json!({
            "tools":[serde_json::to_value(schema("ReadArchive")).unwrap()]
        }))
        .unwrap();
        let mut discovery_result = Message::tool_result_with_status(
            "sticky-loaded-read-archive",
            format!("<loaded_tools>{payload}</loaded_tools>"),
            true,
        );
        discovery_result.never_compress = true;
        discovery_result.metadata = Some(json!({
            "runtime_kind":"sticky_capability_discovery",
            "version":1,
            "canonical_new_names":["ReadArchive"]
        }));
        session.add_message(discovery_result);
        execute_llm_round(
            &mut session,
            &config,
            &llm,
            &event_tx,
            &cancel,
            "sticky-round-tools",
            "chat-model",
            &tools,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let requests = provider.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[1]
                .iter()
                .map(|tool| tool.function.name.as_str())
                .collect::<Vec<_>>(),
            [
                "Read",
                "ReadArchive",
                "exec_command",
                "discover_capabilities"
            ]
        );
        assert_eq!(
            requests[0]
                .iter()
                .map(|tool| tool.function.name.as_str())
                .collect::<Vec<_>>(),
            vec!["Read", "exec_command", "discover_capabilities"]
        );
        assert_eq!(
            requests[0][2].function.parameters["properties"]["query"]["maxLength"],
            bamboo_domain::MAX_DISCOVERY_QUERY_CHARS
        );
        assert!(requests[0]
            .iter()
            .all(|tool| tool.function.name != "ReadArchive" && tool.function.name != "Workspace"));
    }
}
