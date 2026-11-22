use bamboo_domain::{
    Message, ProviderFamily, ProviderProtocol, ProviderTranscriptAuthor, ProviderTranscriptGroup,
    ProviderTranscriptItem, ProviderTranscriptOrigin,
};
use reqwest::header::{HeaderMap, AUTHORIZATION};
use ring::hmac;
use serde_json::{json, Map, Value};

use crate::types::LLMChunk;

pub(in crate::providers::gemini) const PROTOCOL: ProviderProtocol =
    ProviderProtocol::GeminiGenerateContentV1Beta;
const MAX_REPLAY_PARTS: usize = 4096;
const MAX_REPLAY_FRAME_BYTES: usize = 8 * 1024 * 1024;

pub(in crate::providers::gemini) fn signing_scope(url: &str, headers: &HeaderMap) -> String {
    let key = hmac::Key::new(
        hmac::HMAC_SHA256,
        headers
            .get("x-goog-api-key")
            .map_or(&[], |key| key.as_bytes()),
    );
    let mut context = hmac::Context::with_key(&key);
    context.update(b"bamboo/gemini-signed-content/v1\0");
    for part in [
        url.as_bytes(),
        headers
            .get(AUTHORIZATION)
            .map_or(&[], |value| value.as_bytes()),
        headers
            .get("x-goog-user-project")
            .map_or(&[], |value| value.as_bytes()),
    ] {
        context.update(&(part.len() as u64).to_be_bytes());
        context.update(part);
    }
    hex::encode(context.sign())
}

pub(in crate::providers) struct NativeReplay<'a> {
    pub scope: &'a str,
    pub groups: &'a [ProviderTranscriptGroup],
}

pub(super) struct NativeTurn<'a> {
    pub parts: &'a Value,
    pub call_ids: Vec<Option<&'a str>>,
}

impl<'a> NativeReplay<'a> {
    pub(super) fn turn(&self, message: &Message) -> Option<NativeTurn<'a>> {
        let group = self.groups.iter().rev().find(|group| {
            group.family() == ProviderFamily::Gemini
                && group.protocol() == PROTOCOL
                && group.anchor_message_id() == message.id
        })?;
        let payload = group.items().first()?.payload();
        if payload["scope_sha256"].as_str() != Some(self.scope) || message.content_parts.is_some() {
            return None;
        }
        let parts = payload["parts"].as_array()?;
        let ids = payload["tool_call_ids"].as_array()?;
        let calls = message.tool_calls.as_deref().unwrap_or_default();
        if ids.len() != calls.len() {
            return None;
        }
        let mut content = String::new();
        let mut call_ids = Vec::with_capacity(calls.len());
        for part in parts {
            if let Some(call) = part.get("functionCall") {
                let index = call_ids.len();
                let normalized = calls.get(index)?;
                if ids[index].as_str() != Some(normalized.id.as_str())
                    || call["name"].as_str() != Some(normalized.function.name.as_str())
                    || call
                        .get("args")
                        .and_then(Value::as_object)
                        .cloned()
                        .unwrap_or_default()
                        != serde_json::from_str::<Map<String, Value>>(
                            &normalized.function.arguments,
                        )
                        .ok()?
                {
                    return None;
                }
                call_ids.push(call["id"].as_str());
            }
            if part["thought"].as_bool() != Some(true) {
                if let Some(text) = part["text"].as_str() {
                    content.push_str(text);
                }
            }
        }
        (content == message.content && call_ids.len() == calls.len()).then_some(NativeTurn {
            parts: &payload["parts"],
            call_ids,
        })
    }
}

pub(in crate::providers::gemini) struct ReplayCapture {
    scope: String,
    parts: Vec<Value>,
    call_ids: Vec<String>,
    saw_signature: bool,
    frame_bytes: usize,
    discarded: bool,
}

impl ReplayCapture {
    pub(in crate::providers::gemini) fn new(scope: String) -> Self {
        Self {
            scope,
            parts: Vec::new(),
            call_ids: Vec::new(),
            saw_signature: false,
            frame_bytes: 0,
            discarded: false,
        }
    }

    pub(in crate::providers::gemini) fn observe(
        &mut self,
        value: &Value,
        chunks: &[LLMChunk],
        frame_bytes: usize,
    ) -> Option<LLMChunk> {
        if self.discarded {
            return None;
        }
        let candidate = value["candidates"].as_array()?.first()?;
        if let Some(parts) = candidate["content"]["parts"].as_array() {
            let total_bytes = self.frame_bytes.saturating_add(frame_bytes);
            if total_bytes > MAX_REPLAY_FRAME_BYTES
                || self.parts.len().saturating_add(parts.len()) > MAX_REPLAY_PARTS
            {
                self.parts.clear();
                self.call_ids.clear();
                self.discarded = true;
                return None;
            }
            self.frame_bytes = total_bytes;
            for part in parts {
                self.saw_signature |= part.get("thoughtSignature").is_some();
            }
            self.parts.extend(parts.iter().cloned());
            for calls in chunks.iter().filter_map(|chunk| match chunk {
                LLMChunk::ToolCalls(calls) => Some(calls),
                _ => None,
            }) {
                self.call_ids
                    .extend(calls.iter().map(|call| call.id.clone()));
            }
        }
        let reason = candidate["finishReason"].as_str()?;
        if reason.is_empty() {
            return None;
        }
        let parts = std::mem::take(&mut self.parts);
        let ids = std::mem::take(&mut self.call_ids);
        self.frame_bytes = 0;
        if !std::mem::take(&mut self.saw_signature) || reason != "STOP" {
            return None;
        }
        let payload = json!({
            "type": "gemini_model_content",
            "scope_sha256": self.scope,
            "parts": parts,
            "tool_call_ids": ids,
        });
        ProviderTranscriptItem::try_from_payload(
            ProviderFamily::Gemini,
            PROTOCOL,
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::Model,
            payload,
        )
        .ok()
        .map(LLMChunk::ProviderTranscriptItem)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::gemini::{parse_gemini_sse_event, GeminiStreamState};

    #[test]
    fn oversize_replay_capture_stops_without_losing_normal_tokens() {
        let mut state = GeminiStreamState::default();
        state.capture_native_replay("a".repeat(64));
        let signed = json!({"candidates":[{"content":{"parts":[
            {"thoughtSignature":"fixture-signature","text":"First"},
        ]}}]})
        .to_string();
        let _: Vec<_> = parse_gemini_sse_event(&mut state, "", &signed).unwrap();
        let excess = json!({"candidates":[{"content":{"parts":vec![json!({"text":"continued"}); MAX_REPLAY_PARTS]}}]}).to_string();
        let overflow = parse_gemini_sse_event(&mut state, "", &excess).unwrap();
        assert!(overflow
            .iter()
            .any(|chunk| matches!(chunk, LLMChunk::Token(text) if text == "continued")));
        let finish = json!({"candidates":[{"content":{"parts":[{"text":"finished"}]},"finishReason":"STOP"}]}).to_string();
        let finished = parse_gemini_sse_event(&mut state, "", &finish).unwrap();
        assert!(finished
            .iter()
            .any(|chunk| matches!(chunk, LLMChunk::Token(text) if text == "finished")));
        assert!(!finished
            .iter()
            .any(|chunk| matches!(chunk, LLMChunk::ProviderTranscriptItem(_))));
    }

    #[test]
    fn replay_byte_budget_discards_in_progress_candidate() {
        let mut capture = ReplayCapture::new("a".repeat(64));
        let signed = json!({"candidates":[{"content":{"parts":[
            {"thoughtSignature":"fixture-signature","text":"First"},
        ]}}]});
        assert!(capture
            .observe(&signed, &[], MAX_REPLAY_FRAME_BYTES)
            .is_none());
        assert_eq!(capture.parts.len(), 1);
        assert!(capture.observe(&signed, &[], 1).is_none());
        assert!(capture.discarded);
        assert!(capture.parts.is_empty());
    }
}
