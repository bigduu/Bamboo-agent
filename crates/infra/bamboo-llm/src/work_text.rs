//! Bounded text completion through the existing managed provider transport.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use bamboo_domain::{Message, ReasoningEffort, TokenUsage};
use futures::{future::BoxFuture, StreamExt};
use reqwest::{Client, RequestBuilder, Response};
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
    static WORK_TEXT: Dispatch;
}

pub type Admission = Arc<dyn Fn() -> BoxFuture<'static, crate::provider::Result<()>> + Send + Sync>;

#[derive(Clone)]
struct Dispatch {
    client: Client,
    admit: Admission,
    admission_refused: Arc<AtomicBool>,
    physical_send_started: Arc<AtomicBool>,
}

pub(crate) fn active() -> bool {
    WORK_TEXT.try_with(|_| ()).is_ok()
}

pub(crate) async fn send(request: RequestBuilder) -> crate::provider::Result<Response> {
    match WORK_TEXT.try_with(Clone::clone) {
        Ok(dispatch) => {
            let request = request.build()?;
            if let Err(error) = (dispatch.admit)().await {
                dispatch.admission_refused.store(true, Ordering::Release);
                return Err(error);
            }
            dispatch
                .physical_send_started
                .store(true, Ordering::Release);
            Ok(dispatch.client.execute(request).await?)
        }
        Err(_) => Ok(request.send().await?),
    }
}

/// A separate HTTP pool for reserved text calls. The configured provider
/// continues to build the request and parse its native reply.
pub struct WorkTextClient(Client);

impl WorkTextClient {
    pub fn from_config(config: &crate::Config) -> crate::provider::Result<Self> {
        Ok(Self(crate::http_client::build_work_text_client(config)?))
    }
}

pub struct Request<'a> {
    pub model: &'a str,
    pub messages: &'a [Message],
    pub input_cap: u32,
    pub output_cap: u32,
    pub remaining: Duration,
}

#[derive(Debug, PartialEq, Eq)]
pub enum WorkTextReply {
    Complete {
        text: Option<String>,
        exact_usage: Option<TokenUsage>,
    },
    OverCap(ObservedWorkUsage),
    /// A retained Host proof explicitly refused the only model send.
    NotSent,
    Unknown,
}

/// Consume one already admitted text-only request through a Host-managed
/// provider. Run the Host's exact admission proof at the physical send
/// boundary, including after asynchronous provider bootstrap.
pub async fn complete(
    client: &WorkTextClient,
    provider: &dyn LLMProvider,
    request: Request<'_>,
    admit: Admission,
) -> WorkTextReply {
    let reply = complete_with_options(
        client,
        provider,
        request,
        admit,
        LLMRequestOptions {
            request_purpose: Some("semantic_patrol".into()),
            ..Default::default()
        },
    )
    .await;
    match reply {
        WorkTextReply::NotSent => WorkTextReply::Unknown,
        other => other,
    }
}

/// One explicit Work review over the same bounded managed transport. The role
/// selection supplies the admitted provider and its configured effort.
pub async fn review(
    client: &WorkTextClient,
    provider: &dyn LLMProvider,
    request: Request<'_>,
    admit: Admission,
    effort: Option<ReasoningEffort>,
) -> WorkTextReply {
    complete_with_options(
        client,
        provider,
        request,
        admit,
        LLMRequestOptions {
            request_purpose: Some("work_review".into()),
            reasoning_effort: effort,
            ..Default::default()
        },
    )
    .await
}

async fn complete_with_options(
    client: &WorkTextClient,
    provider: &dyn LLMProvider,
    request: Request<'_>,
    admit: Admission,
    options: LLMRequestOptions,
) -> WorkTextReply {
    let Request {
        model,
        messages,
        input_cap,
        output_cap,
        remaining,
    } = request;
    if input_cap == 0 || output_cap == 0 || remaining.is_zero() {
        return WorkTextReply::Unknown;
    }
    let transport = WorkPhysicalRequest::default();
    let admission_refused = Arc::new(AtomicBool::new(false));
    let physical_send_started = Arc::new(AtomicBool::new(false));
    let reply = timeout(
        remaining.min(MAX_REQUEST_TIME),
        WORK_TEXT.scope(
            Dispatch {
                client: client.0.clone(),
                admit,
                admission_refused: admission_refused.clone(),
                physical_send_started: physical_send_started.clone(),
            },
            transport.scope(async {
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
    .unwrap_or(WorkTextReply::Unknown);
    if reply == WorkTextReply::Unknown
        && admission_refused.load(Ordering::Acquire)
        && !physical_send_started.load(Ordering::Acquire)
    {
        WorkTextReply::NotSent
    } else {
        reply
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{http_client::build_http_client, Config, OpenAIProvider};
    use wiremock::{
        matchers::{method, path},
        Mock, MockServer, ResponseTemplate,
    };

    fn allow() -> Admission {
        Arc::new(|| Box::pin(async { Ok(()) }))
    }

    async fn run(server: &MockServer, input_cap: u32) -> WorkTextReply {
        let client = build_http_client(&Config::default()).unwrap();
        let provider = OpenAIProvider::new("controlled-key")
            .with_client(client)
            .with_base_url(server.uri());
        complete(
            &WorkTextClient::from_config(&Config::default()).unwrap(),
            &provider,
            Request {
                model: "configured-model",
                messages: &[Message::user("Evidence")],
                input_cap,
                output_cap: 64,
                remaining: Duration::from_secs(5),
            },
            allow(),
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
            complete(
                &WorkTextClient::from_config(&Config::default()).unwrap(),
                &Idle,
                Request {
                    model: "idle",
                    messages: &[],
                    input_cap: 100,
                    output_cap: 10,
                    remaining,
                },
                allow(),
            ),
        )
        .await;
        assert_eq!(result.unwrap(), WorkTextReply::Unknown);
    }

    #[tokio::test]
    async fn review_reports_positive_refusal_before_the_physical_request() {
        let server = MockServer::start().await;
        let provider = OpenAIProvider::new("controlled-key").with_base_url(server.uri());
        let admission: Admission = Arc::new(|| {
            Box::pin(async { Err(crate::LLMError::Auth("controlled refusal".into())) })
        });
        let reply = review(
            &WorkTextClient::from_config(&Config::default()).unwrap(),
            &provider,
            Request {
                model: "review",
                messages: &[Message::user("bounded evidence")],
                input_cap: 100,
                output_cap: 10,
                remaining: Duration::from_secs(1),
            },
            admission,
            Some(ReasoningEffort::Low),
        )
        .await;
        assert_eq!(reply, WorkTextReply::NotSent);
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn authorization_revoked_during_provider_bootstrap_blocks_the_physical_send() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use tokio::sync::Notify;

        struct Bootstrap<'a> {
            inner: OpenAIProvider,
            entered: &'a Notify,
            resume: &'a Notify,
        }

        #[async_trait::async_trait]
        impl LLMProvider for Bootstrap<'_> {
            async fn chat_stream(
                &self,
                messages: &[Message],
                tools: &[bamboo_domain::ToolSchema],
                output: Option<u32>,
                model: &str,
            ) -> crate::provider::Result<crate::LLMStream> {
                self.entered.notify_one();
                self.resume.notified().await;
                self.inner.chat_stream(messages, tools, output, model).await
            }
        }

        let server = MockServer::start().await;
        let entered = Notify::new();
        let resume = Notify::new();
        let provider = Bootstrap {
            inner: OpenAIProvider::new("controlled-key").with_base_url(server.uri()),
            entered: &entered,
            resume: &resume,
        };
        let revoked = Arc::new(AtomicBool::new(false));
        let proof = revoked.clone();
        let guard: Admission = Arc::new(move || {
            let proof = proof.clone();
            Box::pin(async move {
                if proof.load(Ordering::SeqCst) {
                    return Err(crate::LLMError::RequestFailed("revoked".into()));
                }
                Ok(())
            })
        });
        let client = WorkTextClient::from_config(&Config::default()).unwrap();
        let messages = [Message::user("Scoped evidence")];
        let (result, ()) = tokio::join!(
            complete(
                &client,
                &provider,
                Request {
                    model: "configured-model",
                    messages: &messages,
                    input_cap: 100,
                    output_cap: 10,
                    remaining: Duration::from_secs(3),
                },
                guard
            ),
            async {
                entered.notified().await;
                revoked.store(true, Ordering::SeqCst);
                resume.notify_one();
            },
        );
        assert_eq!(result, WorkTextReply::Unknown);
        assert!(server.received_requests().await.unwrap().is_empty());
    }
}
