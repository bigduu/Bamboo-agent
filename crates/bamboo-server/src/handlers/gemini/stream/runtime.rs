use actix_web::Error as ActixError;
use bytes::Bytes;
use futures::{Stream, StreamExt};

use bamboo_infrastructure::{provider::LLMStream, LLMChunk};
use bamboo_engine::{ForwardStatus, MetricsCollector};

use super::super::usage::{build_estimated_usage, estimate_completion_tokens};
use super::sse;

#[derive(Clone)]
pub(super) struct StreamRuntimeContext {
    pub(super) metrics: MetricsCollector,
    pub(super) forward_id: String,
    pub(super) estimated_prompt_tokens: u64,
}

pub(super) fn build_gemini_event_stream(
    mut stream: LLMStream,
    context: StreamRuntimeContext,
) -> impl Stream<Item = Result<Bytes, ActixError>> {
    async_stream::stream! {
        let mut had_error = false;
        let mut streamed_text = String::new();

        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(LLMChunk::ResponseId(_)) => {}
                Ok(LLMChunk::Token(token)) => {
                    streamed_text.push_str(&token);
                    match sse::token_chunk_bytes(token) {
                        Ok(bytes) => yield Ok::<_, ActixError>(bytes),
                        Err(error) => {
                            yield Err(error);
                            continue;
                        }
                    }
                }
                Ok(LLMChunk::ReasoningToken(_)) => {}
                Ok(LLMChunk::ToolCalls(tool_calls)) => {
                    for tool_call in tool_calls {
                        match sse::tool_call_chunk_bytes(tool_call) {
                            Ok(bytes) => yield Ok::<_, ActixError>(bytes),
                            Err(error) => {
                                yield Err(error);
                                continue;
                            }
                        }
                    }
                }
                Ok(LLMChunk::Done) => {
                    yield Ok::<_, ActixError>(sse::done_chunk_bytes());
                    break;
                }
                Err(error) => {
                    had_error = true;
                    context.metrics.forward_completed(
                        context.forward_id.clone(),
                        chrono::Utc::now(),
                        None,
                        ForwardStatus::Error,
                        None,
                        Some(format!("Stream error: {error}")),
                    );
                    yield Err(sse::stream_error(format!("Stream error: {error}")));
                    break;
                }
            }
        }

        if !had_error {
            let completion_tokens = estimate_completion_tokens(&streamed_text);
            context.metrics.forward_completed(
                context.forward_id,
                chrono::Utc::now(),
                Some(200),
                ForwardStatus::Success,
                Some(build_estimated_usage(
                    context.estimated_prompt_tokens,
                    completion_tokens,
                )),
                None,
            );
        }
    }
}
