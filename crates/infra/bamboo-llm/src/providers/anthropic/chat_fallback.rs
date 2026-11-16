use std::sync::{Arc, Mutex};

use bamboo_domain::{Message, ReasoningEffort, ToolSchema};
use futures::{stream, StreamExt};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use serde_json::{json, Value};

use crate::provider::{CompletionDiagnostics, LLMError, LLMStream, Result};
use crate::providers::common::{openai_compat, request_overrides};
use crate::providers::openai::chat_completions_stream;
use crate::retry::RemainingInitialRequestAttempt;
use crate::types::{LLMChunk, ProviderUsageReplacement};

use super::{AnthropicProvider, AnthropicStreamState};

/// One retry at the same gateway for a Messages request whose effort was
/// translated into a numeric budget the upstream rejected.
pub(super) struct ChatFallback {
    request: reqwest::RequestBuilder,
    body: Value,
    attempt: RemainingInitialRequestAttempt,
    model: String,
    effort: ReasoningEffort,
    limit: u32,
    diagnostics: Option<CompletionDiagnostics>,
}

impl ChatFallback {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn prepare(
        provider: &AnthropicProvider,
        messages: &[Message],
        tools: &[ToolSchema],
        model: &str,
        wire: &Value,
        headers: &HeaderMap,
        attempt: RemainingInitialRequestAttempt,
        diagnostics: Option<CompletionDiagnostics>,
    ) -> Option<Self> {
        if !request_overrides::messages_fallback_patches_supported(
            provider.request_overrides.as_ref(),
            model,
        ) || wire["stream"] != true
            || wire["thinking"] != json!({"type":"adaptive"})
        {
            return None;
        }
        let wire_effort = wire.pointer("/output_config/effort")?.as_str()?;
        let effort = ReasoningEffort::parse(wire_effort)?;
        if effort == ReasoningEffort::Disabled
            || wire["output_config"] != json!({"effort": effort.as_str()})
        {
            return None;
        }
        let limit = u32::try_from(wire["max_tokens"].as_u64()?).ok()?;
        let parallel_tools = wire
            .pointer("/tool_choice/disable_parallel_tool_use")
            .and_then(Value::as_bool)
            .map(|disabled| !disabled);
        let mut chat = openai_compat::build_openai_compat_body(
            model,
            messages,
            tools,
            None,
            Some(limit),
            Some(effort),
            parallel_tools,
        );
        crate::masking::mask_outbound_body(&mut chat, &provider.masking_config);
        // The effective selected model, effort and limit belong to the
        // original request, including any explicit output-control patch.
        let model = wire["model"].as_str()?.to_owned();
        chat["model"] = json!(model);
        chat["reasoning_effort"] = json!(wire_effort);
        chat["max_completion_tokens"] = json!(limit);
        if let Some(name) = wire.pointer("/tool_choice/name").and_then(Value::as_str) {
            chat["tool_choice"] = json!({"type":"function","function":{"name":name}});
        }
        for (messages_key, chat_key) in [
            ("temperature", "temperature"),
            ("top_p", "top_p"),
            ("stop_sequences", "stop"),
        ] {
            if let Some(value) = wire.get(messages_key) {
                chat[chat_key] = value.clone();
            }
        }
        copy_system_and_reasoning(wire, chat["messages"].as_array_mut()?)?;

        let mut headers = headers.clone();
        headers.remove("anthropic-version");
        headers.remove("anthropic-beta");
        if !headers.contains_key(AUTHORIZATION) {
            let mut bearer = b"Bearer ".to_vec();
            bearer.extend_from_slice(headers.get("x-api-key")?.as_bytes());
            let mut bearer = HeaderValue::from_bytes(&bearer).ok()?;
            bearer.set_sensitive(true);
            headers.insert(AUTHORIZATION, bearer);
        }
        let url = format!(
            "{}/chat/completions",
            provider.base_url.trim_end_matches('/')
        );
        let request = provider.client.post(url).headers(headers);
        Some(Self {
            request,
            body: chat,
            attempt,
            model,
            effort,
            limit,
            diagnostics,
        })
    }

    pub(super) async fn retry(self, rejection: LLMError) -> Result<LLMStream> {
        tracing::info!("Retrying a rejected Messages thinking budget via Chat Completions");
        let response = self
            .attempt
            .send(|| self.request.json(&self.body))
            .await
            .map_err(|error| {
                LLMError::RequestFailed(format!(
                    "{rejection}. Chat Completions fallback failed: {error}"
                ))
            })?;
        let Some(response) = response else {
            return Err(rejection);
        };
        if !response.status().is_success() {
            return Err(LLMError::RequestFailed(format!(
                "{rejection}. Chat Completions fallback returned HTTP {}",
                response.status()
            )));
        }
        if let Some(diagnostics) = self.diagnostics.as_ref() {
            diagnostics.record_pending_usage(ProviderUsageReplacement::default());
            if diagnostics.is_agent_round() {
                diagnostics.observe_agent_round_request(
                    request_overrides::ENDPOINT_CHAT_COMPLETIONS,
                    Some(self.limit.into()),
                    None,
                )?;
            }
        }
        Ok(chat_completions_stream(
            response,
            self.model,
            Some(self.effort),
            self.diagnostics,
        ))
    }

    pub(super) fn recover_stream(
        self,
        upstream: LLMStream,
        messages_state: Arc<Mutex<AnthropicStreamState>>,
    ) -> LLMStream {
        Box::pin(stream::unfold(
            (upstream, Some((self, messages_state))),
            |(mut upstream, mut fallback)| async move {
                let item = upstream.next().await?;
                let (started, rejected) = fallback.as_ref().map_or((false, false), |(_, state)| {
                    let state = state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    (state.model_output_started, state.thinking_budget_rejected)
                });
                // A usage or model chunk belongs to the original request.
                // Transport-only frames may precede the validation error.
                if started
                    || matches!(&item, Ok(chunk) if !matches!(chunk, LLMChunk::TransportActivity))
                {
                    fallback = None;
                }
                match (item, fallback) {
                    (Err(error), Some((fallback, _))) if rejected => {
                        drop(upstream);
                        match fallback.retry(error).await {
                            Ok(mut chat) => {
                                let first = chat.next().await?;
                                Some((first, (chat, None)))
                            }
                            Err(error) => {
                                Some((Err(error), (Box::pin(stream::empty()) as LLMStream, None)))
                            }
                        }
                    }
                    (item, fallback) => Some((item, (upstream, fallback))),
                }
            },
        ))
    }
}

pub(super) fn rejects_thinking_budget(error: &str) -> bool {
    let error = error
        .replace(['"', '\'', '`', '\\'], "")
        .to_ascii_lowercase();
    ["thinking_budget", "thinking.budget_tokens"]
        .iter()
        .any(|field| {
            error.match_indices(field).any(|(offset, _)| {
                let before = error[..offset].chars().next_back();
                let after = error[offset + field.len()..].trim_start();
                !before.is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_')
                    && after.starts_with("is not supported")
            })
        })
}

fn copy_system_and_reasoning(wire: &Value, chat: &mut Vec<Value>) -> Option<()> {
    chat.retain(|message| message["role"] != "system");
    if let Some(system) = wire.get("system") {
        let system = if let Some(text) = system.as_str() {
            text.to_owned()
        } else {
            system
                .as_array()?
                .iter()
                .map(|block| block["text"].as_str())
                .collect::<Option<Vec<_>>>()?
                .join("\n\n")
        };
        chat.insert(0, json!({"role":"system","content":system}));
    }
    // Carry only thinking that the original Messages request replayed. Native
    // opaque blocks cannot safely be translated by the generic Chat adapter.
    let mut reasoning_by_turn = Vec::new();
    for message in wire["messages"].as_array()? {
        let mut reasoning = String::new();
        if let Some(blocks) = message["content"].as_array() {
            for block in blocks {
                match block["type"].as_str()? {
                    "thinking" => reasoning.push_str(block["thinking"].as_str()?),
                    "text" | "image" | "tool_use" | "tool_result" => {}
                    _ => return None,
                }
            }
        }
        if message["role"] == "assistant" {
            reasoning_by_turn.push(reasoning);
        }
    }
    if reasoning_by_turn
        .iter()
        .any(|reasoning| !reasoning.is_empty())
    {
        if chat
            .iter()
            .filter(|message| message["role"] == "assistant")
            .count()
            != reasoning_by_turn.len()
        {
            return None;
        }
        for (message, reasoning) in chat
            .iter_mut()
            .filter(|message| message["role"] == "assistant")
            .zip(reasoning_by_turn)
        {
            if !reasoning.is_empty() {
                message["reasoning_content"] = json!(reasoning);
            }
        }
    }
    Some(())
}

#[cfg(test)]
mod tests {
    use bamboo_config::RequestOverridesConfig;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::provider::{LLMProvider, LLMRequestOptions};

    const REJECTION: &str = r#"{"type":"error","error":{"type":"api_error","message":"litellm.BadRequestError: event:error\ndata:{\"code\":\"InvalidParameter\",\"message\":\"Parameter thinking_budget is not supported.\"}"}}"#;
    const CHAT_SSE: &str = concat!(
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"reasoning\"}}]}\n\n",
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"answer\"},\"finish_reason\":\"stop\"}]}\n\n",
        "data: [DONE]\n\n",
    );

    fn options(effort: ReasoningEffort) -> LLMRequestOptions {
        LLMRequestOptions {
            reasoning_effort: Some(effort),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn budget_rejections_retry_the_same_model_and_exact_effort() {
        for (model, effort, status) in [
            ("proxy.reasoning-a", ReasoningEffort::Low, 200),
            ("proxy.reasoning-b", ReasoningEffort::Medium, 400),
            ("proxy.reasoning-b", ReasoningEffort::High, 200),
            ("proxy.reasoning-a", ReasoningEffort::Xhigh, 200),
            ("proxy.reasoning-a", ReasoningEffort::Max, 400),
        ] {
            let server = MockServer::start().await;
            let rejection = if status == 200 {
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(format!(
                        "event: message_start\ndata: {{\"type\":\"message_start\",\"message\":{{\"usage\":{{\"input_tokens\":0}}}}}}\n\nevent: error\ndata: {REJECTION}\n\n"
                    ))
            } else {
                ResponseTemplate::new(400).set_body_string(REJECTION)
            };
            Mock::given(method("POST"))
                .and(path("/v1/messages"))
                .respond_with(rejection)
                .expect(1)
                .mount(&server)
                .await;
            Mock::given(method("POST"))
                .and(path("/v1/chat/completions"))
                .and(header("authorization", "Bearer fixture-key"))
                .and(header("x-fixture-route", "same-account"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .insert_header("content-type", "text/event-stream")
                        .set_body_string(CHAT_SSE),
                )
                .expect(1)
                .mount(&server)
                .await;
            let overrides: RequestOverridesConfig = serde_json::from_value(json!({
                "endpoints": {"messages": {
                    "headers": {"X-Fixture-Route": "same-account"},
                    "body_patch": [{"op":"set", "path":"/max_tokens", "value":2048}]
                }}
            }))
            .unwrap();
            let provider = AnthropicProvider::new("fixture-key")
                .with_base_url(format!("{}/v1", server.uri()))
                .with_request_overrides(Some(overrides));
            let messages = [Message::system("system context"), Message::user("hello")];
            let chunks = provider
                .chat_stream_with_options(&messages, &[], Some(4096), model, Some(&options(effort)))
                .await
                .unwrap()
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>>>()
                .unwrap();
            assert!(chunks
                .iter()
                .any(|chunk| matches!(chunk, LLMChunk::Token(text) if text == "answer")));
            assert!(chunks.iter().any(
                |chunk| matches!(chunk, LLMChunk::ReasoningToken(text) if text == "reasoning")
            ));
            assert_eq!(
                chunks
                    .iter()
                    .filter(|chunk| matches!(chunk, LLMChunk::Done))
                    .count(),
                1
            );
            let requests = server.received_requests().await.unwrap();
            let primary: Value = requests[0].body_json().unwrap();
            let chat: Value = requests[1].body_json().unwrap();
            assert_eq!(primary["output_config"]["effort"], effort.as_str());
            assert_eq!(chat["model"], model);
            assert_eq!(chat["reasoning_effort"], effort.as_str());
            assert_eq!(chat["max_completion_tokens"], 2048);
            assert!(chat.get("thinking").is_none());
            assert!(chat.get("thinking_budget").is_none());
            assert_eq!(
                chat["messages"],
                json!([
                    {"role":"system","content":"system context"},
                    {"role":"user","content":"hello"}
                ])
            );
        }
    }

    #[tokio::test]
    async fn a_budget_rejection_after_a_content_block_does_not_replay_the_request() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/messages"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(format!(
                        "event: content_block_start\ndata: {{\"index\":0,\"content_block\":{{\"type\":\"thinking\",\"thinking\":\"\"}}}}\n\nevent: error\ndata: {REJECTION}\n\n"
                    )),
            )
            .expect(1)
            .mount(&server)
            .await;
        let provider = AnthropicProvider::new("fixture-key").with_base_url(server.uri());
        let chunks = provider
            .chat_stream_with_options(
                &[Message::user("hello")],
                &[],
                Some(4096),
                "proxy.reasoning",
                Some(&options(ReasoningEffort::High)),
            )
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert!(chunks.iter().any(
            |chunk| matches!(chunk, Err(LLMError::Stream(error)) if rejects_thinking_budget(error))
        ));
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_reserved_work_attempt_never_sends_a_second_request() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/messages"))
            .respond_with(ResponseTemplate::new(400).set_body_string(REJECTION))
            .expect(1)
            .mount(&server)
            .await;
        let provider = AnthropicProvider::new("fixture-key").with_base_url(server.uri());
        let reply = crate::retry::WorkPhysicalRequest::default()
            .scope(provider.chat_stream_with_options(
                &[Message::user("hello")],
                &[],
                Some(4096),
                "proxy.reasoning",
                Some(&options(ReasoningEffort::High)),
            ))
            .await;
        assert!(reply.is_err());
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    #[test]
    fn rejects_only_unsupported_numeric_thinking_controls() {
        for message in [
            REJECTION,
            "Parameter `thinking_budget` is not supported.",
            "thinking.budget_tokens is not supported for this model",
        ] {
            assert!(rejects_thinking_budget(message));
        }
        for message in [
            "invalid_thinking_budget is not supported",
            "reasoning_effort is not supported",
            "thinking_budget must be an integer",
            "thinking_budget is missing, and max_tokens is not supported",
        ] {
            assert!(!rejects_thinking_budget(message));
        }
    }
}
