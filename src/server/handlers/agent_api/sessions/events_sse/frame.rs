use actix_web::web::Bytes;

use crate::agent::core::AgentEvent;

pub(super) fn serialize_event_frame(event: &AgentEvent) -> Option<Bytes> {
    let payload = serde_json::to_string(event).ok()?;
    Some(Bytes::from(format!("data: {payload}\n\n")))
}

#[cfg(test)]
mod tests {
    use crate::agent::core::{AgentEvent, TokenUsage};

    use super::serialize_event_frame;

    #[test]
    fn serialize_event_frame_formats_sse_data_line() {
        let event = AgentEvent::Complete {
            usage: TokenUsage {
                prompt_tokens: 0,
                completion_tokens: 0,
                total_tokens: 0,
            },
        };

        let frame = serialize_event_frame(&event).expect("frame");
        let text = String::from_utf8(frame.to_vec()).expect("utf8");

        assert!(text.starts_with("data: "));
        assert!(text.ends_with("\n\n"));
        assert!(text.contains("\"type\":\"complete\""));
    }
}
