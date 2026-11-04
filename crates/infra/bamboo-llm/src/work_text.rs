//! Bounded text completion through the existing managed provider transport.

use std::time::Duration;

use bamboo_domain::{Message, TokenUsage};
use futures::StreamExt;
use tokio::time::{timeout, timeout_at, Instant};

use crate::{
    retry::WorkPhysicalRequest,
    work_usage::{ObservedWorkUsage, WorkUsage, POST_DONE_USAGE_TIMEOUT},
    LLMChunk, LLMProvider, LLMRequestOptions,
};

const MAX_TEXT_BYTES: usize = 64 * 1024;
const MAX_STREAM_FRAMES: usize = 65_536;
pub(crate) const MAX_BODY_BYTES: usize = 1024 * 1024;
const MAX_REQUEST_TIME: Duration = Duration::from_secs(60);

tokio::task_local! {
    static WORK_TEXT: ();
}

pub(crate) fn active() -> bool {
    WORK_TEXT.try_with(|_| ()).is_ok()
}

#[derive(Debug, PartialEq, Eq)]
pub enum WorkTextReply {
    Complete {
        text: Option<String>,
        exact_usage: Option<TokenUsage>,
    },
    OverCap(ObservedWorkUsage),
    Unknown,
}

/// Consume one already admitted text-only request through a Host-managed
/// provider. The Host supplies the limits for the original reservation and
/// owns route, scope, and receipt checks before calling this function.
pub async fn complete(
    provider: &dyn LLMProvider,
    model: &str,
    messages: &[Message],
    input_cap: u32,
    output_cap: u32,
    remaining: Duration,
) -> WorkTextReply {
    if input_cap == 0 || output_cap == 0 || remaining.is_zero() {
        return WorkTextReply::Unknown;
    }
    let transport = WorkPhysicalRequest::default();
    timeout(
        remaining.min(MAX_REQUEST_TIME),
        WORK_TEXT.scope(
            (),
            transport.scope(async {
                let options = LLMRequestOptions {
                    request_purpose: Some("semantic_patrol".into()),
                    ..Default::default()
                };
                let Ok(mut stream) = provider
                    .chat_stream_with_options(
                        messages,
                        &[],
                        Some(output_cap),
                        model,
                        Some(&options),
                    )
                    .await
                else {
                    return WorkTextReply::Unknown;
                };
                let mut usage = WorkUsage::default();
                let mut text = Some(String::new());
                let mut trailer_deadline = None;
                for frame in 0..MAX_STREAM_FRAMES {
                    if frame % 64 == 0 {
                        tokio::task::yield_now().await;
                    }
                    let next = if let Some(deadline) = trailer_deadline {
                        if Instant::now() >= deadline {
                            return WorkTextReply::Unknown;
                        }
                        match timeout_at(deadline, stream.next()).await {
                            Ok(next) => next,
                            Err(_) => return WorkTextReply::Unknown,
                        }
                    } else {
                        stream.next().await
                    };
                    let chunk = match next {
                        Some(Ok(chunk)) => chunk,
                        None if usage.is_done() => {
                            return WorkTextReply::Complete {
                                text: text.filter(|text| !text.is_empty()),
                                exact_usage: usage.exact(),
                            };
                        }
                        _ => return WorkTextReply::Unknown,
                    };
                    let after_done = usage.is_done();
                    usage.observe(&chunk);
                    if let Some(report) = usage.over_cap(input_cap, output_cap) {
                        return WorkTextReply::OverCap(report);
                    }
                    match chunk {
                        LLMChunk::Done => {
                            trailer_deadline
                                .get_or_insert_with(|| Instant::now() + POST_DONE_USAGE_TIMEOUT);
                        }
                        LLMChunk::Token(token) if !after_done => {
                            if let Some(output) = &mut text {
                                if token.len() <= MAX_TEXT_BYTES - output.len() {
                                    output.push_str(&token);
                                } else {
                                    text = None;
                                }
                            }
                        }
                        LLMChunk::ToolCalls(_) | LLMChunk::ToolCallsIndexed(_) => text = None,
                        _ => {}
                    }
                }
                WorkTextReply::Unknown
            }),
        ),
    )
    .await
    .unwrap_or(WorkTextReply::Unknown)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{http_client::build_http_client, Config, OpenAIProvider};
    use wiremock::{
        matchers::{method, path},
        Mock, MockServer, ResponseTemplate,
    };

    async fn run(server: &MockServer, input_cap: u32) -> WorkTextReply {
        let client = build_http_client(&Config::default()).unwrap();
        let provider = OpenAIProvider::new("controlled-key")
            .with_client(client)
            .with_base_url(server.uri());
        complete(
            &provider,
            "configured-model",
            &[Message::user("Evidence")],
            input_cap,
            64,
            Duration::from_secs(5),
        )
        .await
    }

    #[tokio::test]
    async fn reads_exact_usage_from_the_existing_chat_transport() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                concat!(
                    "data: {\"choices\":[{\"delta\":{\"content\":\"advice\"}}]}\n\n",
                    "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":12,",
                    "\"completion_tokens\":3,\"total_tokens\":15}}\n\n",
                    "data: [DONE]\n\n",
                ),
                "text/event-stream",
            ))
            .expect(1)
            .mount(&server)
            .await;
        assert_eq!(
            run(&server, 20).await,
            WorkTextReply::Complete {
                text: Some("advice".into()),
                exact_usage: Some(TokenUsage {
                    prompt_tokens: 12,
                    completion_tokens: 3,
                    total_tokens: 15,
                }),
            }
        );
        let requests = server.received_requests().await.unwrap();
        let body: serde_json::Value = requests[0].body_json().unwrap();
        assert_eq!(body["model"], "configured-model");
        assert_eq!(body["max_tokens"], 64);
        assert!(body["tools"].is_null() || body["tools"].as_array().is_some_and(Vec::is_empty));
        assert_eq!(body["stream_options"]["include_usage"], true);
    }

    #[tokio::test]
    async fn a_reservation_cannot_follow_a_redirect_or_retry_a_rate_limit() {
        for status in [307, 429] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/chat/completions"))
                .respond_with(
                    ResponseTemplate::new(status)
                        .insert_header("Location", format!("{}/second", server.uri())),
                )
                .expect(1)
                .mount(&server)
                .await;
            assert_eq!(run(&server, 20).await, WorkTextReply::Unknown);
            assert_eq!(server.received_requests().await.unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn oversized_raw_sse_cannot_reach_a_valid_terminal() {
        let server = MockServer::start().await;
        let body = format!(
            ": {}\n\ndata: {{\"choices\":[],\"usage\":{{\"prompt_tokens\":12,\"completion_tokens\":3}}}}\n\ndata: [DONE]\n\n",
            "x".repeat(MAX_BODY_BYTES),
        );
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(body, "text/event-stream"))
            .expect(1)
            .mount(&server)
            .await;
        assert_eq!(run(&server, 20).await, WorkTextReply::Unknown);
    }

    #[tokio::test]
    async fn idle_stream_stops_at_its_request_timeout() {
        struct Idle;

        #[async_trait::async_trait]
        impl LLMProvider for Idle {
            async fn chat_stream(
                &self,
                _: &[Message],
                _: &[bamboo_domain::ToolSchema],
                _: Option<u32>,
                _: &str,
            ) -> crate::provider::Result<crate::LLMStream> {
                Ok(Box::pin(futures::stream::pending()))
            }
        }

        let remaining = Duration::from_millis(20);
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            complete(&Idle, "idle", &[], 100, 10, remaining),
        )
        .await;
        assert_eq!(result.unwrap(), WorkTextReply::Unknown);
    }
}
