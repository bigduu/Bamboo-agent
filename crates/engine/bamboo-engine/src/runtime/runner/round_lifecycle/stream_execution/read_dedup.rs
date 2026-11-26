use std::collections::{hash_map::Entry, HashMap, HashSet};

use bamboo_compression::{TiktokenTokenCounter, TokenCounter};
use bamboo_domain::{Message, Role};
use bamboo_llm::{PromptIR, SegmentRole};
use bamboo_tools::tools::read::{ReadResultIdentity, READ_RESULT_IDENTITY_KEY};
use serde::Deserialize;

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct ReadDedupStats {
    pub(super) duplicate_count: usize,
    pub(super) estimated_saved_tokens: u32,
}

/// Run once on the final request, after pruning and refitting its transcript.
/// Older results refer only to full counterparts present in this request.
pub(super) fn project_repeated_reads(ir: &mut PromptIR) -> ReadDedupStats {
    let mut stats = ReadDedupStats::default();
    if ir.continuation.is_some() {
        return stats;
    }
    let mut transcripts = ir
        .segments
        .iter()
        .enumerate()
        .filter(|(_, segment)| segment.role == SegmentRole::ModelTranscript);
    let Some((segment_index, segment)) = transcripts.next() else {
        return stats;
    };
    if transcripts.next().is_some() {
        return stats;
    }

    let messages = &segment.messages;
    let identified: Vec<_> = messages
        .iter()
        .enumerate()
        .rev()
        .filter_map(|(index, message)| read_identity(message).map(|identity| (index, identity)))
        .collect();
    if identified.len() < 2 {
        return stats;
    }
    let paired = paired_read_results(ir, messages);
    let counter = TiktokenTokenCounter::default();
    let mut newest: HashMap<ReadResultIdentity, Option<&str>> = HashMap::new();
    let mut replacements = Vec::new();
    for (index, identity) in identified {
        let message = &messages[index];
        let full_call_id = if paired.contains(&index) {
            full_read_call_id(message, &identity)
        } else {
            None
        };

        match newest.entry(identity) {
            Entry::Vacant(entry) => {
                // If the latest known result was downgraded, earlier full
                // results stay intact. A missing result has no comparable key.
                entry.insert(full_call_id);
            }
            Entry::Occupied(entry) if full_call_id.is_some() => {
                let Some(later_call_id) = entry.get() else {
                    continue;
                };
                let marker = format!(
                    "[Repeated Read of the same source version and range. Full result: later tool call {later_call_id}.]"
                );
                let saved = counter
                    .count_text(&message.content)
                    .saturating_sub(counter.count_text(&marker));
                if saved > 0 {
                    stats.duplicate_count += 1;
                    stats.estimated_saved_tokens =
                        stats.estimated_saved_tokens.saturating_add(saved);
                    replacements.push((index, marker));
                }
            }
            Entry::Occupied(_) => {}
        }
    }
    for (index, marker) in replacements {
        ir.segments[segment_index].messages[index].content = marker;
    }
    stats
}

fn read_identity(message: &Message) -> Option<ReadResultIdentity> {
    if message.role != Role::Tool {
        return None;
    }
    let metadata = message.metadata.as_ref()?;
    if !is_read_name(metadata.get("tool_name")?.as_str()?) {
        return None;
    }
    ReadResultIdentity::deserialize(metadata.get(READ_RESULT_IDENTITY_KEY)?).ok()
}

fn is_read_name(name: &str) -> bool {
    name == "Read" || bamboo_domain::tool_names::normalize_tool_ref(name).as_deref() == Some("Read")
}

fn full_read_call_id<'a>(message: &'a Message, identity: &ReadResultIdentity) -> Option<&'a str> {
    let call_id = message.tool_call_id.as_deref()?;
    if !marker_safe_call_id(call_id)
        || message.tool_success != Some(true)
        || message.metadata.as_ref()?.get("success")?.as_bool() != Some(true)
        || message.content_parts.is_some()
        || message.image_ocr.is_some()
        || message.reasoning.is_some()
        || message.reasoning_signature.is_some()
        || message.tool_calls.is_some()
        || message.never_compress
        || message.compressed
        || message.compression_level != 0
        || message.compressed_by_event_id.is_some()
    {
        return None;
    }
    identity
        .matches_full_result(&message.content)
        .then_some(call_id)
}

fn marker_safe_call_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
}

fn paired_read_results(ir: &PromptIR, transcript: &[Message]) -> HashSet<usize> {
    let mut message_counts = HashMap::<&str, usize>::new();
    let mut call_counts = HashMap::<&str, usize>::new();
    let mut result_counts = HashMap::<&str, usize>::new();
    for message in ir.body_chat_iter() {
        *message_counts.entry(&message.id).or_default() += 1;
        for call in message.tool_calls.iter().flatten() {
            *call_counts.entry(&call.id).or_default() += 1;
        }
        if let Some(call_id) = message.tool_call_id.as_deref() {
            *result_counts.entry(call_id).or_default() += 1;
        }
    }

    let native_anchors: HashSet<_> = ir
        .provider_transcript_groups
        .iter()
        .map(|group| group.anchor_message_id())
        .collect();
    let ordinary_message = |message: &Message| {
        !message.id.is_empty()
            && message_counts.get(message.id.as_str()) == Some(&1)
            && !native_anchors.contains(message.id.as_str())
    };

    let mut paired = HashSet::new();
    for (index, assistant) in transcript.iter().enumerate() {
        let Some(calls) = assistant.tool_calls.as_ref() else {
            continue;
        };
        if assistant.role != Role::Assistant
            || !ordinary_message(assistant)
            || !calls
                .iter()
                .any(|call| call.tool_type == "function" && is_read_name(&call.function.name))
            || calls.iter().any(|call| {
                let id = call.id.as_str();
                id.trim().is_empty()
                    || call_counts.get(id) != Some(&1)
                    || result_counts.get(id) != Some(&1)
            })
        {
            continue;
        }
        let Some((results, remaining)) = transcript[index + 1..].split_at_checked(calls.len())
        else {
            continue;
        };
        let call_by_id: HashMap<_, _> = calls.iter().map(|call| (call.id.as_str(), call)).collect();
        // Verify the entire visible batch, including sibling tools, before
        // using a Read in it. Every call and result occurs once in the request.
        if remaining
            .first()
            .is_some_and(|message| message.role == Role::Tool)
            || results.iter().any(|result| {
                result.role != Role::Tool
                    || !ordinary_message(result)
                    || !result
                        .tool_call_id
                        .as_deref()
                        .is_some_and(|id| call_by_id.contains_key(id))
            })
        {
            continue;
        }
        paired.extend(results.iter().enumerate().filter_map(|(offset, result)| {
            let call = call_by_id.get(result.tool_call_id.as_deref()?)?;
            (call.tool_type == "function" && is_read_name(&call.function.name))
                .then_some(index + 1 + offset)
        }));
    }
    paired
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::{
        FunctionCall, MessagePart, ProviderFamily, ProviderProtocol, ProviderTranscriptAuthor,
        ProviderTranscriptItem, ProviderTranscriptOrigin, ProviderTranscriptState, ToolCall,
    };
    use bamboo_llm::{Continuation, Segment};
    use serde_json::{json, Value};
    use sha2::{Digest, Sha256};

    fn call(id: &str, name: &str) -> ToolCall {
        ToolCall {
            id: id.to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: name.to_string(),
                arguments: r#"{"file_path":"src/example.rs"}"#.to_string(),
            },
        }
    }

    fn output() -> String {
        (1..=20)
            .map(|line| format!("{line}: local_count += items.len();\n"))
            .collect()
    }

    fn pair(id: &str, version: u8, range_start: usize) -> [Message; 2] {
        let output = output();
        let version = [version; 32];
        let digest: [u8; 32] = Sha256::digest(output.as_bytes()).into();
        let mut result = Message::tool_result(id, output);
        result.metadata = Some(json!({
            "tool_name": "Read",
            "success": true,
            (READ_RESULT_IDENTITY_KEY): {
                "canonical_path": "/fixture/src/example.rs",
                "source_kind": "file",
                "source_version": version,
                "range_start": range_start,
                "range_end": range_start + 20,
                "result_sha256": digest,
            },
        }));
        [Message::assistant("", Some(vec![call(id, "Read")])), result]
    }

    fn ir(messages: impl IntoIterator<Item = Message>) -> PromptIR {
        PromptIR {
            segments: vec![Segment::new(
                SegmentRole::ModelTranscript,
                messages.into_iter().collect(),
            )],
            ..PromptIR::default()
        }
    }

    fn transcript(ir: &PromptIR) -> &[Message] {
        ir.run(SegmentRole::ModelTranscript)
    }

    fn assert_untouched(mut ir: PromptIR) {
        let before = json!(transcript(&ir));
        assert_eq!(project_repeated_reads(&mut ir), ReadDedupStats::default());
        assert_eq!(json!(transcript(&ir)), before);
    }

    #[test]
    fn rewrites_only_older_read_contents_and_keeps_mixed_tool_batches() {
        let [mut assistant, older] = pair("call-old", 1, 0);
        assistant
            .tool_calls
            .as_mut()
            .unwrap()
            .insert(0, call("call-other", "Workspace"));
        let other = Message::tool_result("call-other", "different output");
        let mut prompt = ir([assistant, older, other]
            .into_iter()
            .chain(pair("call-new", 1, 0)));
        let prefix = pair("prefix-read", 1, 0).to_vec();
        prompt
            .segments
            .insert(0, Segment::new(SegmentRole::StablePrefix, prefix.clone()));
        let original = transcript(&prompt).to_vec();

        let stats = project_repeated_reads(&mut prompt);
        assert_eq!(stats.duplicate_count, 1);
        let projected = transcript(&prompt);
        assert!(projected[1].content.contains("later tool call call-new"));
        let counter = TiktokenTokenCounter::default();
        assert_eq!(
            stats.estimated_saved_tokens,
            counter.count_text(&original[1].content) - counter.count_text(&projected[1].content)
        );
        assert!(stats.estimated_saved_tokens > 0);
        let mut expected = json!(original);
        expected[1]["content"] = json!(projected[1].content);
        assert_eq!(json!(projected), expected);
        assert_eq!(json!(prompt.run(SegmentRole::StablePrefix)), json!(prefix));
        assert_untouched(prompt);
    }

    #[test]
    fn distinct_versions_and_effective_ranges_remain_full() {
        assert_untouched(ir(pair("baseline", 1, 0)
            .into_iter()
            .chain(pair("different-version", 2, 0))
            .chain(pair("different-range", 1, 10))));
    }

    #[test]
    fn newest_full_counterpart_must_survive_final_prompt_fitting() {
        let messages: Vec<_> = pair("old", 1, 0)
            .into_iter()
            .chain(pair("middle", 1, 0))
            .chain(pair("newest", 1, 0))
            .collect();
        let mut fitted = ir(messages[..4].iter().cloned());
        assert_eq!(project_repeated_reads(&mut fitted).duplicate_count, 1);
        assert!(transcript(&fitted)[1]
            .content
            .contains("later tool call middle"));
        assert_eq!(transcript(&fitted)[3].content, output());

        let mut fitted = ir(messages.clone());
        fitted.segments[0].messages[5].content = "[Compacted Read]".to_string();
        assert_untouched(fitted);
        let mut fitted = ir(messages);
        fitted.segments[0].messages[5].compression_level = 1;
        assert_untouched(fitted);
    }

    #[test]
    fn incomplete_duplicate_and_unrelated_pairs_are_unchanged() {
        let [mut incomplete, result] = pair("incomplete", 1, 0);
        incomplete
            .tool_calls
            .as_mut()
            .unwrap()
            .push(call("missing", "Workspace"));
        assert_untouched(ir([incomplete, result]
            .into_iter()
            .chain(pair("new", 1, 0))));
        assert_untouched(ir(pair("collision", 1, 0).into_iter().chain(pair(
            "collision",
            1,
            0,
        ))));

        let [assistant, result] = pair("duplicate-result", 1, 0);
        assert_untouched(ir([assistant, result.clone(), result]
            .into_iter()
            .chain(pair("new", 1, 0))));
        let [_, orphan] = pair("orphan", 1, 0);
        assert_untouched(ir(pair("older", 1, 0).into_iter().chain([orphan])));
        let [mut assistant, result] = pair("other", 1, 0);
        assistant.tool_calls = Some(vec![call("other", "Workspace")]);
        assert_untouched(ir([assistant, result].into_iter().chain(pair("new", 1, 0))));
    }

    #[test]
    fn failed_legacy_and_multipart_results_are_unchanged() {
        let [failed_call, mut failed] = pair("failed", 1, 0);
        failed.tool_success = Some(false);
        let [legacy_call, mut legacy] = pair("legacy", 1, 0);
        legacy.metadata = None;
        let [multipart_call, mut multipart] = pair("multipart", 1, 0);
        multipart.content_parts = Some(vec![MessagePart::Text {
            text: "part".to_string(),
        }]);
        assert_untouched(ir([
            failed_call,
            failed,
            legacy_call,
            legacy,
            multipart_call,
            multipart,
        ]
        .into_iter()
        .chain(pair("new", 1, 0))));
    }

    fn native_group(prompt: &mut PromptIR, anchor: &str) {
        let items = [
            (ProviderTranscriptAuthor::Model, json!({
                "type": "tool_search_call", "id": "tsc_1", "execution": "server",
                "call_id": "search_1", "status": "completed", "arguments": {"paths": ["fixture"]}
            })),
            (ProviderTranscriptAuthor::ToolResult, json!({
                "type": "tool_search_output", "id": "tso_1", "execution": "server",
                "call_id": "search_1", "status": "completed", "tools": [{"type": "function", "name": "lookup"}]
            })),
        ].into_iter().map(|(author, payload)| ProviderTranscriptItem::try_from_payload(
            ProviderFamily::OpenAi,
            ProviderProtocol::OpenAiResponsesV1,
            ProviderTranscriptOrigin::Provider,
            author,
            payload,
        ).unwrap()).collect();
        let mut state = ProviderTranscriptState::default();
        state.append_group(anchor, None, items).unwrap();
        prompt.provider_transcript_groups = state.groups().to_vec();
    }

    #[test]
    fn native_replay_and_continuations_keep_their_exact_transcript() {
        for message_index in [0, 1, 2, 3] {
            let mut prompt = ir(pair("old", 1, 0).into_iter().chain(pair("new", 1, 0)));
            let anchor = transcript(&prompt)[message_index].id.clone();
            native_group(&mut prompt, &anchor);
            let groups: Value = json!(prompt.provider_transcript_groups);
            let before = json!(transcript(&prompt));
            assert_eq!(
                project_repeated_reads(&mut prompt),
                ReadDedupStats::default()
            );
            assert_eq!(json!(prompt.provider_transcript_groups), groups);
            assert_eq!(json!(transcript(&prompt)), before);
        }
        let mut prompt = ir(pair("old", 1, 0).into_iter().chain(pair("new", 1, 0)));
        prompt.continuation = Some(Continuation {
            previous_response_id: "resp_prev".to_string(),
            last_committed_assistant_id: Some(transcript(&prompt)[0].id.clone()),
        });
        let original = json!(transcript(&prompt));
        assert_eq!(
            project_repeated_reads(&mut prompt),
            ReadDedupStats::default()
        );
        assert_eq!(json!(transcript(&prompt)), original);
        assert_eq!(
            prompt.continuation.unwrap().previous_response_id,
            "resp_prev"
        );
    }
}
