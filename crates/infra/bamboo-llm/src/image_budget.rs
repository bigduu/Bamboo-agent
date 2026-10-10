//! Bound cumulative tool-image URL bytes before native request cloning/serialization.
use bamboo_domain::{Message, MessagePart, Role};

pub(crate) const MAX_TOOL_IMAGE_URL_BYTES: usize = 32 * 1024 * 1024;

pub(crate) fn validate_tool_image_budget<'a>(
    messages: impl IntoIterator<Item = &'a Message>,
) -> Result<(), String> {
    let mut total = 0usize;
    for message in messages
        .into_iter()
        .filter(|message| matches!(message.role, Role::Tool))
    {
        for part in message.content_parts.iter().flatten() {
            if let MessagePart::ImageUrl { image_url } = part {
                let size = image_url.url.len();
                if size > MAX_TOOL_IMAGE_URL_BYTES.saturating_sub(total) {
                    return Err("Tool image batch exceeds the 32 MiB cumulative encoded-image limit; no request was sent and image history was preserved. Use smaller images or a smaller batch.".into());
                }
                total += size;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn image(size: usize) -> Message {
        Message::tool_result_with_images(
            "image",
            "image",
            true,
            vec![bamboo_domain::ToolResultImage {
                mime_type: "image/png".into(),
                data: "A".repeat(size),
            }],
        )
    }
    #[test]
    fn vision_tool_image_budget_checks_aggregate_boundary_without_mutation() {
        let prefix = "data:image/png;base64,".len();
        let half = MAX_TOOL_IMAGE_URL_BYTES / 2 - prefix;
        let mut messages = vec![image(half), image(half)];
        assert!(validate_tool_image_budget(&messages).is_ok());
        let bamboo_domain::MessagePart::ImageUrl { image_url } =
            &mut messages[1].content_parts.as_mut().unwrap()[0]
        else {
            panic!("image part")
        };
        image_url.url.push('A');
        let before = messages[1].content_parts.as_ref().unwrap()[0].clone();
        assert!(validate_tool_image_budget(&messages)
            .unwrap_err()
            .contains("32 MiB"));
        assert_eq!(messages[1].content_parts.as_ref().unwrap()[0], before);
    }
    #[test]
    fn vision_tool_image_budget_borrows_only_the_effective_ir_body() {
        use crate::prompt_ir::{PromptIR, Segment, SegmentRole};
        let mut ir = PromptIR {
            segments: vec![
                Segment::new(
                    SegmentRole::Conversation,
                    vec![image(MAX_TOOL_IMAGE_URL_BYTES)],
                ),
                Segment::new(SegmentRole::ModelTranscript, vec![image(1)]),
            ],
            ..PromptIR::default()
        };
        let borrowed: Vec<_> = ir.body_chat_iter().collect();
        assert_eq!(borrowed.len(), 1);
        assert!(std::ptr::eq(borrowed[0], &ir.segments[1].messages[0]));
        assert!(validate_tool_image_budget(ir.body_chat_iter()).is_ok());
        // Without a canonical transcript, the same retained legacy image is active.
        ir.segments.pop();
        assert!(validate_tool_image_budget(ir.body_chat_iter()).is_err());
        assert!(ir.segments[0].messages[0].content_parts.is_some());
    }
    #[tokio::test]
    async fn vision_native_providers_reject_large_batches_before_network_or_json() {
        use crate::providers::BodhiProvider;
        use crate::{AnthropicProvider, GeminiProvider, LLMProvider, OpenAIProvider, ToProvider};
        let messages = vec![
            image(MAX_TOOL_IMAGE_URL_BYTES / 2),
            image(MAX_TOOL_IMAGE_URL_BYTES / 2),
        ];
        let mut providers: Vec<Box<dyn LLMProvider>> = vec![
            Box::new(
                BodhiProvider::new("offline-fixture")
                    .with_base_url("http://127.0.0.1:1")
                    .with_target_provider("anthropic"),
            ),
            Box::new(
                BodhiProvider::new("offline-fixture")
                    .with_base_url("http://127.0.0.1:1")
                    .with_target_provider("gemini"),
            ),
            Box::new(AnthropicProvider::new("offline-fixture").with_base_url("http://127.0.0.1:1")),
            Box::new(GeminiProvider::new("offline-fixture").with_base_url("http://127.0.0.1:1")),
        ];
        for provider in &providers {
            let error = match provider.chat_stream(&messages, &[], None, "test").await {
                Ok(_) => panic!("oversized image request must fail"),
                Err(error) => error,
            };
            assert!(
                error.to_string().contains("cumulative encoded-image limit"),
                "{error}"
            );
        }
        // OpenAI has its own IR lowering; it must enforce the same pre-clone bound.
        providers.push(Box::new(
            OpenAIProvider::new("offline-fixture").with_base_url("http://127.0.0.1:1"),
        ));
        use crate::prompt_ir::{Continuation, PromptIR, Segment, SegmentRole};
        let mut ir = PromptIR {
            segments: vec![Segment::new(SegmentRole::ModelTranscript, messages)],
            ..PromptIR::default()
        };
        // Production engine entry, including full, structured-system and continuation views.
        for mode in 0..3 {
            if mode == 1 {
                ir.system_blocks.push(bamboo_domain::PromptBlock::new(
                    "base",
                    bamboo_domain::ContextBlockType::Base,
                    "system",
                ));
            }
            if mode == 2 {
                ir.continuation = Some(Continuation {
                    previous_response_id: "previous".into(),
                    last_committed_assistant_id: None,
                });
            }
            for provider in &providers {
                let error = match provider.chat_stream_ir(&ir, &[], None, "test", None).await {
                    Ok(_) => panic!("oversized IR must fail before lowering"),
                    Err(error) => error,
                };
                assert!(
                    error.to_string().contains("cumulative encoded-image limit"),
                    "{error}"
                );
            }
        }
        let converted: crate::protocol::ProtocolResult<crate::protocol::gemini::GeminiRequest> =
            ir.segments.remove(0).messages.to_provider();
        assert!(converted
            .unwrap_err()
            .to_string()
            .contains("cumulative encoded-image limit"));
    }
}
