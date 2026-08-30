//! LLM round lifecycle helpers for the agent loop runner.

use std::borrow::Cow;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::runtime::config::AgentLoopConfig;
use crate::runtime::stream::handler::StreamHandlingOutput;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_agent_core::{AgentError, AgentEvent, Session};
use bamboo_llm::LLMProvider;
use bamboo_metrics::TokenUsage as MetricsTokenUsage;

use token_estimation::{estimate_completion_tokens, estimate_prompt_tokens};

mod context_ledger;
mod context_preparation;
mod prefix_drift;
mod stream_execution;
mod token_budget;
mod token_estimation;

pub(crate) use context_preparation::force_overflow_context_recovery;
pub(crate) use stream_execution::discard_latest_interrupted_assistant_output;
pub(in crate::runtime::runner) use stream_execution::{
    effective_tool_schemas, required_tool_for_session,
};

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

fn request_tool_schemas_for_loading_mode<'a>(
    tool_schemas: &'a [ToolSchema],
    mode: bamboo_domain::CapabilityLoadingMode,
) -> Cow<'a, [ToolSchema]> {
    if mode != bamboo_domain::CapabilityLoadingMode::StickyFallback {
        return Cow::Borrowed(tool_schemas);
    }

    let mut projected = tool_schemas
        .iter()
        .cloned()
        .filter_map(bamboo_domain::ClassifiedToolSchema::new)
        .filter(|entry| entry.loading_class() == bamboo_domain::CapabilityLoadingClass::Core)
        .map(bamboo_domain::ClassifiedToolSchema::into_schema)
        .collect::<Vec<_>>();
    projected.push(bamboo_domain::discovery_control_fallback_schema());
    Cow::Owned(projected)
}

pub(crate) struct RoundLlmExecutionOutput {
    pub stream_output: StreamHandlingOutput,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    /// Canonical usage for this single billed provider attempt.
    pub attempt_usage: MetricsTokenUsage,
    /// Validation that can only run after a provider stream has completed.
    ///
    /// The pipeline must absorb `attempt_usage` before surfacing this error so
    /// a billed response is not turned into a zero-usage terminal round.
    pub terminal_validation_error: Option<AgentError>,
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
    let prompt_tokens = stream_output
        .provider_usage
        .and_then(|usage| usage.input_tokens)
        .or_else(|| (stream_output.input_tokens > 0).then_some(stream_output.input_tokens))
        .unwrap_or(estimated_prompt_tokens);
    let completion_tokens = stream_output
        .provider_usage
        .and_then(|usage| usage.output_tokens)
        .or_else(|| (stream_output.output_tokens > 0).then_some(stream_output.output_tokens))
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
) -> Result<RoundLlmExecutionOutput, AgentError> {
    let required_tool = required_tool_for_session(session);
    let capability_loading_mode = llm.capability_loading_mode(model_name, required_tool).await;
    let request_tool_schemas =
        request_tool_schemas_for_loading_mode(tool_schemas, capability_loading_mode);
    let tool_schemas = request_tool_schemas.as_ref();
    let prepared = context_preparation::prepare_round_context(
        session,
        config,
        model_name,
        session_id,
        tool_schemas,
        llm,
        Some(event_tx),
    )
    .await?;

    // Use model from config (provided by execute request), not from session.
    let model = config
        .model_name
        .as_deref()
        .ok_or_else(|| AgentError::LLM("model_name is required in AgentLoopConfig".to_string()))?;

    let frame = stream_execution::LlmStreamFrame {
        event_tx,
        cancel_token,
        session_id,
        model,
        provider_name: config.provider_name.as_deref(),
        provider_type: config.provider_type.as_deref(),
        reasoning_effort: config.reasoning_effort,
        max_context_tokens: prepared.budget.max_context_tokens,
        max_output_tokens: prepared.budget.max_output_tokens,
    };

    let (stream_output, llm_duration) = stream_execution::execute_llm_stream(
        session,
        config,
        llm,
        &prepared.prepared_context,
        tool_schemas,
        &frame,
    )
    .await?;

    // This is a terminal validation error, but the completed stream was still
    // billed. Return it alongside the canonical attempt usage so the runner can
    // account for the attempt before ending the round.
    let openai_client_search_boundary =
        is_openai_client_tool_search_boundary(&stream_output.provider_transcript_items);
    let terminal_validation_error = (stream_output.tool_calls.is_empty()
        && stream_output.content.trim().is_empty()
        && !openai_client_search_boundary)
        .then(|| AgentError::EmptyAssistantResponse {
            response_id: stream_output.response_id.clone(),
        });

    let prompt_tokens = estimate_prompt_tokens(&prepared.prepared_context.messages);
    let completion_tokens =
        estimate_completion_tokens(&stream_output.content, &stream_output.tool_calls);
    let attempt_usage = canonical_attempt_usage(&stream_output, prompt_tokens, completion_tokens);

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
        prompt_tokens,
        completion_tokens,
        attempt_usage,
        terminal_validation_error,
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
    use super::{execute_llm_round, is_openai_client_tool_search_boundary};
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

    struct StickyCapturingProvider {
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
                CapabilityLoadingMode::StickyFallback
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

    #[tokio::test]
    async fn sticky_first_and_next_round_use_byte_identical_core_plus_discovery_tools() {
        let provider = Arc::new(StickyCapturingProvider {
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
            schema("Bash"),
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
        )
        .await
        .unwrap();

        let requests = provider.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            serde_json::to_vec(&requests[0]).unwrap(),
            serde_json::to_vec(&requests[1]).unwrap(),
            "loaded history must not rewrite the sticky top-level tools array"
        );
        assert_eq!(
            requests[0]
                .iter()
                .map(|tool| tool.function.name.as_str())
                .collect::<Vec<_>>(),
            vec!["Read", "Bash", "discover_capabilities"]
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
