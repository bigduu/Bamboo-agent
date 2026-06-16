use std::error::Error as StdError;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::runner::prompt_context::{
    strip_existing_core_directives, strip_existing_external_memory,
    strip_existing_plan_mode_instructions, strip_existing_plan_runtime_context,
    strip_existing_task_list,
};
use crate::runtime::runner::session_setup::prompt_envelope::{
    assemble_prompt_envelope, build_conversation_summary_context_block,
    build_external_memory_context_block, build_goal_context_block, build_plan_mode_context_block,
    build_plan_runtime_context_block, build_task_list_context_block, envelope_to_chat_messages,
    envelope_to_responses_view,
};
use crate::runtime::runner::session_setup::prompt_setup::{
    build_stable_prompt_frame_with_sections, StablePrefixSection,
};
use bamboo_agent_core::agent::events::TokenBudgetUsage;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_agent_core::{
    AgentError, AgentEvent, ContextBlock, ContextBlockPriority, ContextBlockStability,
    ContextBlockType, Message, Role, Session,
};
use bamboo_compression::PreparedContext;
use bamboo_domain::ReasoningEffort;
use bamboo_llm::provider::ResponsesRequestOptions;
use bamboo_llm::{CacheTtl, LLMProvider, LLMRequestOptions, PromptCachePlan, PromptLanes};
use bamboo_tools::exposure::activated_discoverable_tools;

/// LLM-stream frame bundling per-request identification, observability, and
/// model configuration parameters.  Passed into [`execute_llm_stream`] to
/// keep its parameter count below the clippy threshold.
pub(in crate::runtime::runner) struct LlmStreamFrame<'a> {
    pub event_tx: &'a mpsc::Sender<AgentEvent>,
    pub cancel_token: &'a CancellationToken,
    pub session_id: &'a str,
    pub model: &'a str,
    pub provider_name: Option<&'a str>,
    pub provider_type: Option<&'a str>,
    pub reasoning_effort: Option<ReasoningEffort>,
    pub max_context_tokens: u32,
    pub max_output_tokens: u32,
}

const SESSION_RESPONSES_PREVIOUS_RESPONSE_ID_KEY: &str = "responses.previous_response_id";
const CONVERSATION_SUMMARY_START_MARKER: &str = "<!-- CONVERSATION_SUMMARY_START -->";

fn session_previous_response_id(session: &Session) -> Option<&str> {
    session
        .metadata
        .get(SESSION_RESPONSES_PREVIOUS_RESPONSE_ID_KEY)
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn continuation_messages(messages: &[Message]) -> Option<&[Message]> {
    let last_assistant_index = messages
        .iter()
        .rposition(|message| matches!(message.role, Role::Assistant))?;
    let continuation = messages.get(last_assistant_index + 1..)?;
    (!continuation.is_empty()).then_some(continuation)
}

fn provider_supports_previous_response_id(provider_type: Option<&str>) -> bool {
    !matches!(provider_type.map(str::trim), Some("copilot"))
}

fn format_reqwest_transport_error(error: &reqwest::Error) -> String {
    let mut kinds = Vec::new();
    if error.is_timeout() {
        kinds.push("timeout");
    }
    if error.is_connect() {
        kinds.push("connect");
    }
    if error.is_request() {
        kinds.push("request");
    }
    if error.is_body() {
        kinds.push("body");
    }
    if error.is_decode() {
        kinds.push("decode");
    }
    if error.is_redirect() {
        kinds.push("redirect");
    }
    if error.is_builder() {
        kinds.push("builder");
    }
    if error.is_status() {
        kinds.push("status");
    }

    let kind = if kinds.is_empty() {
        "unknown".to_string()
    } else {
        kinds.join("+")
    };
    let url = error
        .url()
        .map(ToString::to_string)
        .unwrap_or_else(|| "<unknown>".to_string());

    let mut causes = Vec::new();
    let mut source = StdError::source(error);
    while let Some(cause) = source {
        causes.push(cause.to_string());
        source = cause.source();
        if causes.len() >= 4 {
            break;
        }
    }

    if causes.is_empty() {
        format!(
            "HTTP transport error [{}] for url ({}): {}",
            kind, url, error
        )
    } else {
        format!(
            "HTTP transport error [{}] for url ({}): {} | causes: {}",
            kind,
            url,
            error,
            causes.join(" | ")
        )
    }
}

fn format_provider_error(error: bamboo_llm::provider::LLMError) -> String {
    match error {
        bamboo_llm::provider::LLMError::Http(http) => format_reqwest_transport_error(&http),
        other => other.to_string(),
    }
}

fn is_llm_overflow_error(message: &str) -> bool {
    let normalized = message.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return false;
    }

    let overflow_patterns = [
        "prompt too long",
        "context too long",
        "maximum context length",
        "maximum context size",
        "context length exceeded",
        "context window exceeded",
        "request too large",
        "too many tokens",
        "input is too long",
        "input too long",
        "token limit exceeded",
    ];

    overflow_patterns
        .iter()
        .any(|pattern| normalized.contains(pattern))
}

fn is_conversation_summary_message(message: &Message) -> bool {
    matches!(message.role, Role::System)
        && message.content.contains(CONVERSATION_SUMMARY_START_MARKER)
}

fn derive_system_remainder_message(
    message: &Message,
    stable_instructions: &str,
) -> Option<Message> {
    if !matches!(message.role, Role::System) || is_conversation_summary_message(message) {
        return None;
    }

    let without_external_memory = strip_existing_external_memory(&message.content);
    let without_task_list = strip_existing_task_list(&without_external_memory);
    let without_plan_mode = strip_existing_plan_mode_instructions(&without_task_list);
    let without_plan_runtime = strip_existing_plan_runtime_context(&without_plan_mode);
    // Framework directives always live in the assembled system field and are not
    // part of the persisted base, so strip them from both sides before comparing.
    // The directives are inserted INTO the `base` section (between the base text
    // and the skill/tool-guide/workspace/env contexts) — not as a leading prefix
    // of the whole frame — so stripping them from both the persisted message and
    // the reference makes the comparison apples-to-apples (base+contexts vs
    // base+contexts). Without it, the directive-bearing reference no longer equals
    // the directive-free persisted base, so the dedup falls through and re-emits
    // the entire base as a redundant system message. Stripping both sides also
    // discards a STALE directive block in an old persisted message, so the current
    // directives (already in the system field) win.
    let without_directives = strip_existing_core_directives(&without_plan_runtime);
    let trimmed = without_directives.trim();
    if trimmed.is_empty() {
        return None;
    }

    let stable_without_directives = strip_existing_core_directives(stable_instructions);
    let stable_trimmed = stable_without_directives.trim();
    if stable_trimmed.is_empty() {
        return Some(Message::system(trimmed.to_string()));
    }

    if trimmed == stable_trimmed {
        return None;
    }

    if let Some(remainder) = trimmed.strip_prefix(stable_trimmed) {
        // Persisted ⊇ reference: the persisted message carries genuine extra
        // content beyond the assembled system field — re-emit only that tail.
        let remainder = remainder.trim();
        return (!remainder.is_empty()).then(|| Message::system(remainder.to_string()));
    }

    // Persisted ⊆ reference: the persisted message (typically the bare configured
    // base, since contexts are injected only at request time) sits at the head of
    // the assembled system field, and the remainder is framework-injected context
    // (workspace/skill/tool-guide/env) already present in the system field and the
    // relocated context messages. There is nothing extra to surface, so do not
    // re-emit the base into the conversation every round. The `\n` boundary check
    // avoids a mid-token false prefix (e.g. "base" vs "basement").
    if stable_trimmed.starts_with(&format!("{trimmed}\n")) {
        return None;
    }

    Some(Message::system(trimmed.to_string()))
}

struct PreparedRequestEnvelope {
    chat_messages: Vec<Message>,
    /// Canonical, provider-facing prompt structure (the four lanes). Its
    /// `flatten()` reproduces `chat_messages` exactly, so routing the normal
    /// (non-continuation) request through `chat_stream_lanes` is byte-identical
    /// until providers override it to consume the lanes structurally.
    lanes: PromptLanes,
    responses_input_messages: Vec<Message>,
    system_remainder_messages: Vec<Message>,
    dynamic_context_messages: Vec<Message>,
    conversation_messages: Vec<Message>,
    /// Per-round volatile context (recalled memory, task list, plan state),
    /// rendered as messages and placed after the conversation history so it
    /// never sits inside the cached prefix.
    volatile_context_messages: Vec<Message>,
    instructions: Option<String>,
    envelope_observability:
        crate::runtime::runner::session_setup::prompt_envelope::PromptEnvelopeObservability,
    /// Prompt-cache plan for this request (cacheable system/tools plus rolling
    /// summary and conversation-tail breakpoints).
    cache_plan: PromptCachePlan,
    /// Per-section breakdown of the cacheable stable prefix, kept for prompt-cache
    /// drift diagnostics (not sent to the provider).
    stable_prefix_sections: Vec<StablePrefixSection>,
}

fn build_request_envelope(
    session: &Session,
    prepared_context: &PreparedContext,
    config: &AgentLoopConfig,
    tool_schemas: &[ToolSchema],
) -> PreparedRequestEnvelope {
    let activated = activated_discoverable_tools(session);
    let (stable_frame, stable_prefix_sections) =
        build_stable_prompt_frame_with_sections(session, config, tool_schemas, &activated);
    let stable_instructions = stable_frame.stable_instructions.clone();

    // Per-round volatile context (recalled memory, task list, plan state) is
    // placed AFTER the conversation history so it never sits inside the cached
    // prefix and invalidates it each round. The conversation summary stays at the
    // front: it represents older history and changes only on re-summarization, so
    // it is cache-friendly there and gets its own (mostly stable) breakpoint.
    let mut front_blocks = Vec::new();
    let mut volatile_blocks = Vec::new();
    if let Some(block) = build_external_memory_context_block(session) {
        volatile_blocks.push(block);
    }
    if let Some(block) = build_task_list_context_block(session) {
        volatile_blocks.push(block);
    }
    // Session goal rides the volatile tail (built directly from the active goal),
    // NOT injected into the system message — so a goal change never invalidates
    // the cached system prefix (goal-leak fix).
    if let Some(block) = build_goal_context_block(config.active_goal()) {
        volatile_blocks.push(block);
    }
    // Plan runtime + plan mode blocks are built DIRECTLY from session state (the
    // active PlanModeState + persisted plan artifacts), not reparsed from markers
    // injected into the system message — so the system prefix stays cache-stable
    // across plan transitions.
    if let Some(block) = build_plan_runtime_context_block(session, config.app_data_dir.as_deref()) {
        volatile_blocks.push(block);
    }
    if let Some(block) = build_plan_mode_context_block(session) {
        volatile_blocks.push(block);
    }
    if let Some(block) = build_conversation_summary_context_block(session) {
        front_blocks.push(block);
    }

    let volatile_context_messages: Vec<Message> = volatile_blocks
        .iter()
        .map(|block| block.render_runtime_context_message())
        .collect();

    let mut system_remainder_messages = Vec::new();
    let mut conversation_messages = Vec::new();
    for message in &prepared_context.messages {
        if matches!(message.role, Role::System) {
            if let Some(remainder_message) =
                derive_system_remainder_message(message, &stable_instructions)
            {
                system_remainder_messages.push(remainder_message);
            }
        } else {
            conversation_messages.push(message.clone());
        }
    }

    let mut envelope_conversation_messages = system_remainder_messages.clone();
    envelope_conversation_messages.extend(conversation_messages.clone());
    // Last message of the cached region (before the volatile context appended
    // below). A rolling breakpoint here lets the growing conversation cache
    // incrementally turn over turn.
    let conversation_breakpoint_id = envelope_conversation_messages
        .last()
        .map(|message| message.id.clone());

    let mut envelope =
        assemble_prompt_envelope(stable_frame, front_blocks, envelope_conversation_messages);
    // Fill the (otherwise unset) stable-prefix hash so per-round drift can be
    // observed and so the value surfaces in envelope observability/logs.
    envelope.observability.stable_prefix_hash =
        Some(super::prefix_drift::hash_sections(&stable_prefix_sections));
    // The only front block is the conversation summary (if present); its rendered
    // message id becomes the summary breakpoint.
    let summary_breakpoint_id = envelope
        .dynamic_context_messages
        .last()
        .map(|message| message.id.clone());

    let responses_view = envelope_to_responses_view(&envelope);
    let mut chat_messages = envelope_to_chat_messages(&envelope);
    chat_messages.extend(volatile_context_messages.clone());
    let envelope_observability = envelope.observability.clone();

    // Canonical prompt structure — where Bamboo OWNS assembly and providers are
    // pure adapters.
    //
    // The tool/server guide (tool schemas + each connected MCP server's
    // `initialize` instructions — e.g. nova's targeting workflow) is relocated OUT
    // of the system prompt and INTO a fixed prefix MESSAGE: the system keeps only
    // the static identity/workspace/env/skill, and the large, session-stable guide
    // rides as a typed context block at a known position with its own cache
    // breakpoint. Applied to BOTH the chat lanes and the Responses-API view, so
    // every provider family gets the same static-system structure. (The legacy
    // `chat_messages` keeps the merged system, used only for the continuation
    // delta and logging.)
    let section = |name: &str| -> String {
        stable_prefix_sections
            .iter()
            .find(|s| s.name == name)
            .map(|s| s.content.clone())
            .unwrap_or_default()
    };
    let tool_guide = section("tool_guide");
    let relocate_tool_guide = !tool_guide.trim().is_empty();
    // Keep ONLY cross-session-invariant content in the system field: the static
    // identity (`base`) plus the globally-stable env snapshot. Session-variable
    // context — workspace path, project instruction overlay (CLAUDE.md/AGENTS.md),
    // loaded skills — is relocated into context-block MESSAGES placed AFTER the
    // large, invariant tool guide (see `session_context_messages` below). This
    // keeps the cacheable prefix (system + tool guide) byte-identical across
    // sessions and across a mid-session workspace/skill injection, which is what
    // lets an automatic prefix cache (OpenAI/GLM-style, which has no explicit
    // breakpoints) actually hit on the big block instead of re-reading it.
    // Assemble the cross-session-invariant system field as DISCRETE structured
    // blocks (identity `base`, framework `core_directives`, globally-stable
    // `env`) instead of one glued string. `system_blocks` is the structured form
    // (observability/analysis + a block-native provider can render one wire block
    // per entry); `lane_system` is their byte join for the legacy string wire
    // path. Session-variable context (workspace/instruction/skill) is still
    // relocated into context-block MESSAGES after the tool guide below.
    let mut system_blocks: Vec<bamboo_domain::PromptBlock> = [
        ("base", bamboo_domain::ContextBlockType::Base),
        (
            "core_directives",
            bamboo_domain::ContextBlockType::CoreDirectives,
        ),
        ("env", bamboo_domain::ContextBlockType::EnvSnapshot),
    ]
    .into_iter()
    .filter_map(|(name, kind)| {
        let text = section(name);
        (!text.trim().is_empty()).then(|| {
            bamboo_domain::PromptBlock::new(name, kind, text)
                .with_stability(bamboo_domain::ContextBlockStability::Stable)
        })
    })
    .collect();
    // The single system cache breakpoint anchors on the last system block.
    if let Some(last) = system_blocks.last_mut() {
        last.cache_anchor = true;
    }
    let lane_system = if relocate_tool_guide {
        system_blocks
            .iter()
            .map(|b| b.text.as_str())
            .collect::<Vec<_>>()
            .join("\n\n")
    } else {
        // Zero-tools fallback: keep the legacy merged system string and no blocks.
        system_blocks.clear();
        envelope.stable_instructions.clone()
    };
    let tool_guide_message = relocate_tool_guide.then(|| {
        ContextBlock::new(
            ContextBlockType::ToolGuide,
            ContextBlockPriority::High,
            ContextBlockStability::SessionStable,
            "Tool & Connected-Server Guide",
            tool_guide,
        )
        .render_runtime_context_message()
    });
    let tool_guide_breakpoint_id = tool_guide_message.as_ref().map(|m| m.id.clone());

    // Session-variable context (workspace path, project instruction overlay,
    // loaded skills), relocated to ride AFTER the invariant tool guide so it never
    // shifts the cached head. Each block is session-stable and emitted only when
    // present; keeping them as separate blocks lets an unchanged block stay inside
    // the shared prefix even when a sibling changes. The instruction overlay keeps
    // Critical priority so its authority survives the move out of the system field.
    let session_context_messages: Vec<Message> = if relocate_tool_guide {
        [
            (
                ContextBlockType::Workspace,
                ContextBlockPriority::High,
                "Workspace",
                section("workspace"),
            ),
            (
                ContextBlockType::InstructionOverlay,
                ContextBlockPriority::Critical,
                "Project Instructions",
                section("instruction"),
            ),
            (
                ContextBlockType::SkillContext,
                ContextBlockPriority::High,
                "Loaded Skills",
                section("skill"),
            ),
        ]
        .into_iter()
        .filter(|(_, _, _, content)| !content.trim().is_empty())
        .map(|(block_type, priority, title, content)| {
            ContextBlock::new(
                block_type,
                priority,
                ContextBlockStability::SessionStable,
                title,
                content,
            )
            .render_runtime_context_message()
        })
        .collect()
    } else {
        Vec::new()
    };

    // Responses-API view mirrors the relocation: the guide leaves `instructions`
    // and rides at the front of the input messages.
    let instructions = if relocate_tool_guide {
        let trimmed = lane_system.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    } else {
        responses_view.instructions
    };
    let mut responses_input_messages = Vec::new();
    if let Some(message) = tool_guide_message.clone() {
        responses_input_messages.push(message);
    }
    responses_input_messages.extend(session_context_messages.clone());
    responses_input_messages.extend(responses_view.input_messages);
    responses_input_messages.extend(volatile_context_messages.clone());

    let mut stable_prefix_messages = envelope.stable_prefix_messages.clone();
    if let Some(message) = tool_guide_message {
        stable_prefix_messages.push(message);
    }
    stable_prefix_messages.extend(session_context_messages);

    let lanes = PromptLanes {
        stable_instructions: lane_system,
        system_blocks,
        stable_prefix_messages,
        dynamic_context_messages: envelope.dynamic_context_messages.clone(),
        conversation_messages: {
            let mut tail = envelope.conversation_messages.clone();
            tail.extend(volatile_context_messages.clone());
            tail
        },
    };

    // tools + system + (summary) + (conversation tail) — at most the
    // 4-breakpoint Anthropic budget. Providers without explicit breakpoints
    // (OpenAI/Gemini/Copilot) still benefit from the stable-prefix ordering.
    let mut breakpoint_message_ids = Vec::new();
    // The relocated tool guide is large and session-stable, so it earns a
    // dedicated breakpoint; when present it takes priority over the summary
    // breakpoint to stay within Anthropic's marker budget (the summary still
    // caches as part of the prefix up to the conversation tail, just without its
    // own boundary). For the legacy/continuation paths this id simply isn't found
    // in the message array, so the breakpoint is harmlessly ignored there.
    if let Some(id) = tool_guide_breakpoint_id {
        breakpoint_message_ids.push(id);
    } else if let Some(id) = summary_breakpoint_id {
        breakpoint_message_ids.push(id);
    }
    if let Some(id) = conversation_breakpoint_id {
        breakpoint_message_ids.push(id);
    }
    let cache_plan = PromptCachePlan {
        cache_tools: true,
        cache_system: true,
        breakpoint_message_ids,
        // Use the 1-hour extended cache TTL for the whole stable prefix
        // (tools + system + tool guide + summary + conversation history). The
        // default 5-minute TTL expires across any pause longer than 5 min
        // (waiting on the user, slow tools, long model think time), forcing a
        // full re-read of the large append-only history — including big tool
        // results — at full price. The 1-hour TTL survives those gaps; the 2x
        // write premium is paid once and amortized over many 0.1x cache reads.
        // Gated behind the `extended-cache-ttl-2025-04-11` beta header, which
        // the Anthropic provider adds whenever the plan's TTL is Extended.
        ttl: CacheTtl::Extended,
    };

    PreparedRequestEnvelope {
        chat_messages,
        lanes,
        responses_input_messages,
        system_remainder_messages,
        dynamic_context_messages: envelope.dynamic_context_messages.clone(),
        conversation_messages,
        volatile_context_messages,
        instructions,
        envelope_observability,
        cache_plan,
        stable_prefix_sections,
    }
}

pub(super) async fn execute_llm_stream(
    session: &mut Session,
    config: &AgentLoopConfig,
    llm: &Arc<dyn LLMProvider>,
    prepared_context: &PreparedContext,
    tool_schemas: &[ToolSchema],
    frame: &LlmStreamFrame<'_>,
) -> Result<(crate::runtime::stream::handler::StreamHandlingOutput, u128), AgentError> {
    // Bind frame fields as locals so the rest of the function body stays unchanged.
    let event_tx = frame.event_tx;
    let cancel_token = frame.cancel_token;
    let max_context_tokens = frame.max_context_tokens;
    let max_output_tokens = frame.max_output_tokens;
    let model = frame.model;
    let provider_name = frame.provider_name;
    let provider_type = frame.provider_type;
    let reasoning_effort = frame.reasoning_effort;
    let session_id = frame.session_id;

    let llm_started_at = std::time::Instant::now();
    let supports_previous_response_id = provider_supports_previous_response_id(provider_type);
    // Owned (not borrowed) so the immutable borrow of `session` ends here and the
    // drift diagnostic below can take `&mut session`.
    let previous_response_id = if supports_previous_response_id {
        session_previous_response_id(session).map(str::to_string)
    } else {
        None
    };

    let prepared_envelope = build_request_envelope(session, prepared_context, config, tool_schemas);
    // Side-channel diagnostic: record whether the cacheable stable prefix drifted
    // from the previous round (esp. shrinks, which drop cached content). Never
    // affects what is sent below.
    super::prefix_drift::record_prefix_drift(
        session,
        config.app_data_dir.as_deref(),
        &prepared_envelope.stable_prefix_sections,
    );
    let request_messages_buf = if previous_response_id.is_some() {
        let mut delta_messages = prepared_envelope.system_remainder_messages.clone();
        delta_messages.extend(prepared_envelope.dynamic_context_messages.clone());
        if let Some(conversation_delta) =
            continuation_messages(&prepared_envelope.conversation_messages)
        {
            delta_messages.extend_from_slice(conversation_delta);
        } else {
            delta_messages.extend(prepared_envelope.conversation_messages.clone());
        }
        // Volatile context is re-sent each turn (it changes every round) and
        // belongs at the tail, matching the non-delta ordering.
        delta_messages.extend(prepared_envelope.volatile_context_messages.clone());
        delta_messages
    } else {
        prepared_envelope.chat_messages.clone()
    };
    let request_messages = request_messages_buf.as_slice();

    let mut responses_options = ResponsesRequestOptions {
        store: Some(false),
        // Encourage the model to emit visible narration alongside tool calls.
        text_verbosity: Some("high".to_string()),
        reasoning_summary: Some("auto".to_string()),
        include: Some(vec!["reasoning.encrypted_content".to_string()]),
        instructions: prepared_envelope.instructions.clone(),
        input_messages: Some(prepared_envelope.responses_input_messages.clone()),
        ..Default::default()
    };
    if let Some(response_id) = previous_response_id.as_deref() {
        responses_options.previous_response_id = Some(response_id.to_string());
    }
    // Cache plan computed by the envelope: stable system prompt + tool
    // definitions, plus rolling summary and conversation-tail breakpoints. The
    // envelope keeps per-round volatile content (task list, recalled memory, plan
    // state) in trailing context-block messages, so everything up to the
    // conversation-tail breakpoint is byte-stable across rounds and caches
    // incrementally — a stable, growing cache read instead of one that swings or
    // drops to zero.
    let request_options = LLMRequestOptions {
        session_id: Some(session_id.to_string()),
        reasoning_effort,
        parallel_tool_calls: Some(true),
        responses: Some(responses_options),
        request_purpose: Some("agent_loop".to_string()),
        cache: Some(prepared_envelope.cache_plan.clone()),
    };

    if !supports_previous_response_id {
        tracing::debug!(
            "[{}] Responses API previous_response_id disabled for provider={}",
            session_id,
            provider_name.unwrap_or("unknown")
        );
    } else if let Some(response_id) = previous_response_id.as_deref() {
        tracing::debug!(
            "[{}] Continuing Responses API turn with previous_response_id={} using {} delta messages ({} total messages in context)",
            session_id,
            response_id,
            request_messages.len(),
            request_messages_buf.len()
        );
    }

    tracing::info!(
        "[{}] LLM request: model={}, parallel_tool_calls={:?}, reasoning_effort={:?}, tools={}, messages={}, responses_input_messages={}, dynamic_context_messages={}, envelope_blocks={:?}",
        session_id,
        model,
        request_options.parallel_tool_calls,
        request_options.reasoning_effort,
        tool_schemas.len(),
        request_messages.len(),
        prepared_envelope.responses_input_messages.len(),
        prepared_envelope.envelope_observability.dynamic_context_message_count,
        prepared_envelope.envelope_observability.included_block_types,
    );
    // Normal request → canonical lanes (provider renders system + cache
    // breakpoints structurally; default impl flattens to the same bytes as
    // `request_messages`). The Responses-API continuation path sends only a delta
    // of messages, which the lane model doesn't represent, so it stays on the
    // flat `chat_stream_with_options` entry point.
    let stream = if previous_response_id.is_some() {
        llm.chat_stream_with_options(
            request_messages,
            tool_schemas,
            Some(max_output_tokens),
            model,
            Some(&request_options),
        )
        .await
    } else {
        llm.chat_stream_lanes(
            &prepared_envelope.lanes,
            tool_schemas,
            Some(max_output_tokens),
            model,
            Some(&request_options),
        )
        .await
    }
    .map_err(|error| {
        let message = format_provider_error(error);
        if is_llm_overflow_error(&message) {
            AgentError::LLMOverflow(message)
        } else {
            AgentError::LLM(message)
        }
    })?;

    // Send token budget update AFTER LLM call succeeds.
    // This timing gives frontend time to subscribe to /events endpoint.
    let usage = TokenBudgetUsage {
        system_tokens: prepared_context.token_usage.system_tokens,
        summary_tokens: prepared_context.token_usage.summary_tokens,
        window_tokens: prepared_context.token_usage.window_tokens,
        total_tokens: prepared_context.token_usage.total_tokens,
        max_context_tokens,
        budget_limit: prepared_context.token_usage.budget_limit,
        truncation_occurred: prepared_context.truncation_occurred,
        segments_removed: prepared_context.segments_removed,
        prompt_cached_tool_outputs: prepared_context.prompt_cached_tool_outputs,
        prompt_cached_tool_tokens_saved: prepared_context.prompt_cached_tool_tokens_saved,
        thinking_tokens: 0,
        cache_read_input_tokens: 0,
    };

    session.token_usage = Some(usage.clone());

    let budget_event = AgentEvent::TokenBudgetUpdated { usage };
    if let Err(error) = event_tx.send(budget_event).await {
        tracing::warn!(
            "[{}] Failed to send token budget event: {}",
            session_id,
            error
        );
    }

    let stream_output = crate::runtime::stream::handler::consume_llm_stream(
        stream,
        event_tx,
        cancel_token,
        session_id,
    )
    .await?;

    // Update session token usage with actual output/thinking/cache stats from the LLM response.
    if let Some(ref mut usage) = session.token_usage {
        usage.thinking_tokens = stream_output.thinking_tokens as u32;
        usage.cache_read_input_tokens = stream_output.cache_read_input_tokens as u32;
    }

    if let Some(usage) = session.token_usage.clone() {
        let final_budget_event = AgentEvent::TokenBudgetUpdated { usage };
        if let Err(error) = event_tx.send(final_budget_event).await {
            tracing::warn!(
                "[{}] Failed to send final token budget event: {}",
                session_id,
                error
            );
        }
    }

    if stream_output.cache_creation_input_tokens > 0 || stream_output.cache_read_input_tokens > 0 {
        tracing::info!(
            "[{}] Anthropic prompt cache: creation={}, read={}, output={}, thinking={}",
            session_id,
            stream_output.cache_creation_input_tokens,
            stream_output.cache_read_input_tokens,
            stream_output.output_tokens,
            stream_output.thinking_tokens,
        );
    }

    // Append a per-call record to the session's dedicated, append-only
    // `token-usage.jsonl` (next to session.json) for offline cache/cost
    // analysis. `session.token_usage` only keeps the latest overwritten
    // snapshot; this log keeps the full per-round history. `cache_creation`
    // lives only on the stream output (not on the budget snapshot), so it is
    // read from there.
    //
    // Gated to dev: active in any debug build, and in release only when the
    // `token-usage-log` feature is enabled. `cfg!(...)` keeps the code compiled
    // either way (no unused-binding churn); the compiler eliminates the block in
    // a release build with the feature off, so nothing is written.
    if cfg!(any(debug_assertions, feature = "token-usage-log")) {
        if let Some(persistence) = config.persistence.as_ref() {
            let record = crate::token_usage_log::TokenUsageRecord::new(
                chrono::Utc::now().to_rfc3339(),
                session_id,
                model,
                provider_name.unwrap_or(""),
                session.messages.len(),
                session.token_usage.as_ref(),
                stream_output.cache_creation_input_tokens,
                stream_output.cache_read_input_tokens,
                stream_output.input_tokens,
                stream_output.output_tokens,
                stream_output.thinking_tokens,
            );
            match record.to_json_line() {
                Ok(line) => {
                    if let Err(error) = persistence
                        .append_token_usage_record(session_id, &line)
                        .await
                    {
                        tracing::warn!(
                            "[{}] Failed to append token-usage record: {}",
                            session_id,
                            error
                        );
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        "[{}] Failed to serialize token-usage record: {}",
                        session_id,
                        error
                    );
                }
            }
        }
    }

    if supports_previous_response_id {
        if let Some(response_id) = stream_output
            .response_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            session.metadata.insert(
                SESSION_RESPONSES_PREVIOUS_RESPONSE_ID_KEY.to_string(),
                response_id.to_string(),
            );
        } else {
            session
                .metadata
                .remove(SESSION_RESPONSES_PREVIOUS_RESPONSE_ID_KEY);
        }
    } else {
        session
            .metadata
            .remove(SESSION_RESPONSES_PREVIOUS_RESPONSE_ID_KEY);
    }

    let llm_duration = llm_started_at.elapsed().as_millis();

    Ok((stream_output, llm_duration))
}

#[cfg(test)]
mod tests;
