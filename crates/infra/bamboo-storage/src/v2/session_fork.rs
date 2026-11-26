use std::collections::{HashMap, HashSet};
use std::io;

use bamboo_domain::{is_post_compaction_recovery, Message, MessagePart, MessagePhase, Role};

#[derive(Debug, thiserror::Error)]
pub enum SessionForkError {
    #[error("Fork point not found")]
    AnchorNotFound,
    #[error("Fork point must identify one stored user message or standalone assistant reply")]
    InvalidAnchor,
    #[error(transparent)]
    Io(#[from] io::Error),
}

pub(super) fn retain_message_prefix(
    messages: &mut Vec<Message>,
    anchor_id: &str,
) -> Result<(), SessionForkError> {
    if anchor_id.trim().is_empty() || anchor_id.trim() != anchor_id {
        return Err(SessionForkError::InvalidAnchor);
    }
    let mut matches = messages
        .iter()
        .enumerate()
        .filter(|(_, row)| row.id == anchor_id);
    let (anchor_index, anchor) = matches.next().ok_or(SessionForkError::AnchorNotFound)?;
    if matches.next().is_some() || !is_fork_anchor(anchor) {
        return Err(SessionForkError::InvalidAnchor);
    }

    messages.truncate(anchor_index + 1);
    let mut source = std::mem::take(messages).into_iter().peekable();
    while let Some(mut message) = source.next() {
        if message.role == Role::Tool || is_post_compaction_recovery(&message) {
            continue;
        }
        message.tool_call_id = None;
        let calls = message.tool_calls.take().unwrap_or_default();
        if message.role != Role::Assistant || calls.is_empty() {
            messages.push(message);
            continue;
        }

        let mut results = Vec::new();
        while let Some(row) = source.next_if(|row| row.role == Role::Tool) {
            results.push(row);
        }
        // IDs only relate a call to its adjacent results. A stale batch can
        // reuse one of its IDs in a later turn or contain ambiguous duplicates.
        let call_ids = unique_ids(calls.iter().map(|call| call.id.as_str()));
        let result_ids = unique_ids(results.iter().filter_map(|row| row.tool_call_id.as_deref()));
        let paired: HashSet<String> = call_ids
            .intersection(&result_ids)
            .map(|id| id.to_string())
            .collect();
        let calls: Vec<_> = calls
            .into_iter()
            .filter(|call| paired.contains(&call.id))
            .collect();
        if !calls.is_empty() {
            message.tool_calls = Some(calls);
        }
        if message.tool_calls.is_some() || has_content(&message) || message.reasoning.is_some() {
            messages.push(message);
        }
        messages.extend(results.into_iter().filter_map(|mut row| {
            let id = row.tool_call_id.as_deref()?;
            if paired.contains(id) {
                row.tool_calls = None;
                return Some(row);
            }
            None
        }));
    }
    Ok(())
}

fn unique_ids<'a>(ids: impl Iterator<Item = &'a str>) -> HashSet<&'a str> {
    let mut occurrences = HashMap::<_, usize>::new();
    for id in ids.filter(|id| !id.trim().is_empty()) {
        *occurrences.entry(id).or_default() += 1;
    }
    occurrences
        .into_iter()
        .filter_map(|(id, count)| (count == 1).then_some(id))
        .collect()
}

fn is_fork_anchor(message: &Message) -> bool {
    matches!(message.role, Role::User | Role::Assistant)
        && message.phase != Some(MessagePhase::Commentary)
        && message.tool_call_id.is_none()
        && message.tool_calls.as_ref().is_none_or(Vec::is_empty)
        && !is_post_compaction_recovery(message)
        && !message.metadata.as_ref().is_some_and(|metadata| {
            metadata
                .get("hidden_from_ui")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
                || metadata
                    .get("runtime_kind")
                    .and_then(serde_json::Value::as_str)
                    .is_some()
        })
        && has_content(message)
}

fn has_content(message: &Message) -> bool {
    !message.content.trim().is_empty()
        || message.content_parts.as_ref().is_some_and(|parts| {
            parts.iter().any(|part| match part {
                MessagePart::Text { text } => !text.trim().is_empty(),
                MessagePart::ImageUrl { image_url } => !image_url.url.trim().is_empty(),
            })
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::{SessionArchiveOperation, SessionArchiveOutcome, SessionStoreV2};
    use bamboo_domain::{
        AgentRuntimeState, ConversationSummary, FunctionCall, ImageOcrResult, ImageUrlRef, Session,
        SessionKind, SessionPermissionMode, Storage, ToolCall,
    };
    use chrono::{Duration, Utc};

    #[tokio::test]
    async fn session_fork_keeps_the_inclusive_archived_prefix_and_its_attachments() {
        let directory = tempfile::tempdir().unwrap();
        let store = SessionStoreV2::new(directory.path().to_owned())
            .await
            .unwrap();
        let mut source = Session::new("source", "model");
        source.title = "Original".into();
        source.set_project_id_meta("project");
        source.set_workspace_path_meta("/work/project");
        source.agent_runtime_state = Some(AgentRuntimeState::new("old-run"));
        source
            .agent_runtime_state
            .as_mut()
            .unwrap()
            .set_permission_mode(SessionPermissionMode::Auto);
        let mut legacy = AgentRuntimeState::new("legacy-run");
        legacy.set_permission_mode(SessionPermissionMode::Bypass);
        source.metadata.insert(
            "agent.runtime.state".into(),
            serde_json::to_string(&legacy).unwrap(),
        );
        store.save_session(&source).await.unwrap();

        let (image, image_url) = store
            .write_image_attachment(&source, "aW1hZ2U=", Some("image/png"))
            .await
            .unwrap();
        let (ocr_only, ocr_url) = store
            .write_image_attachment(&source, "b2Ny", Some("image/png"))
            .await
            .unwrap();
        let (later, later_url) = store
            .write_image_attachment(&source, "bGF0ZXI=", Some("image/png"))
            .await
            .unwrap();
        let image_part = |url| MessagePart::ImageUrl {
            image_url: ImageUrlRef { url, detail: None },
        };
        let mut user = Message::user_with_parts("first question", vec![image_part(image_url)]);
        user.image_ocr = Some(vec![ImageOcrResult {
            image_url: ocr_url,
            lines: vec![],
            error: None,
        }]);
        let user_id = user.id.clone();
        source.add_message(user);
        let mut assistant = Message::assistant("First answer", None);
        assistant.phase = Some(MessagePhase::FinalAnswer);
        assistant.compressed = true;
        assistant.compressed_by_event_id = Some("an-event".into());
        let assistant_id = assistant.id.clone();
        source.add_message(assistant);
        source.add_message(Message::user_with_parts(
            "later question",
            vec![image_part(later_url)],
        ));
        source.add_message(Message::assistant("discard ".repeat(150_000), None));
        source.conversation_summary =
            Some(ConversationSummary::new("includes later question", 4, 12));
        source.metadata.insert(
            "responses.previous_response_id".into(),
            "old-response".into(),
        );
        store.save_session(&source).await.unwrap();
        let cutoff = Utc::now() + Duration::days(1);
        assert!(matches!(
            store
                .maintain_session_archive(&source.id, cutoff, SessionArchiveOperation::Archive)
                .await
                .unwrap(),
            Some(SessionArchiveOutcome::Archived { .. })
        ));
        let source_before =
            serde_json::to_value(store.load_session(&source.id).await.unwrap().unwrap()).unwrap();

        let (created, guard) = store
            .fork_session_with_projection_guard(&source.id, "fork", &assistant_id)
            .await
            .unwrap()
            .unwrap();
        drop(guard);
        assert_eq!(created.id, "fork");
        assert_eq!(created.root_session_id, "fork");
        assert_eq!(created.title, "Original (fork)");
        assert_eq!(created.project_id_meta().as_deref(), Some("project"));
        assert_eq!(
            created.workspace_path_meta().as_deref(),
            Some("/work/project")
        );
        assert_eq!(
            created
                .agent_runtime_state
                .as_ref()
                .unwrap()
                .effective_permission_mode(),
            SessionPermissionMode::Auto
        );

        let loaded = store.load_session("fork").await.unwrap().unwrap();
        assert_eq!(
            loaded
                .messages
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            [user_id.as_str(), assistant_id.as_str()]
        );
        assert_eq!(loaded.messages[1].content, "First answer");
        assert!(!loaded.messages[1].compressed);
        assert!(loaded.messages[1].compressed_by_event_id.is_none());
        assert!(loaded.conversation_summary.is_none());
        assert!(loaded.model_context_state.is_none());
        assert!(!loaded
            .metadata
            .contains_key("responses.previous_response_id"));
        assert_eq!(
            loaded.messages[0].content_parts,
            Some(vec![image_part(format!(
                "bamboo-attachment://fork/{image}"
            ))])
        );
        assert_eq!(
            loaded.messages[0].image_ocr.as_ref().unwrap()[0].image_url,
            format!("bamboo-attachment://fork/{ocr_only}")
        );
        assert_eq!(
            store
                .read_attachment("fork", &image)
                .await
                .unwrap()
                .unwrap()
                .0,
            b"image"
        );
        assert_eq!(
            store
                .read_attachment("fork", &ocr_only)
                .await
                .unwrap()
                .unwrap()
                .0,
            b"ocr"
        );
        assert!(store
            .read_attachment("fork", &later)
            .await
            .unwrap()
            .is_none());
        assert!(store
            .read_attachment(&source.id, &later)
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            serde_json::to_value(store.load_session(&source.id).await.unwrap().unwrap()).unwrap(),
            source_before
        );
    }

    #[tokio::test]
    async fn session_fork_rejects_invalid_or_missing_anchors_without_publishing() {
        let directory = tempfile::tempdir().unwrap();
        let store = SessionStoreV2::new(directory.path().to_owned())
            .await
            .unwrap();
        let mut source = Session::new("source", "model");
        let duplicate = Message::user("ambiguous original");
        let mut hidden = Message::user("resume signal");
        hidden.metadata = Some(serde_json::json!({"hidden_from_ui": true}));
        let tool = Message::tool_result("call", "tool output");
        let mut commentary = Message::assistant("Working", None);
        commentary.phase = Some(MessagePhase::Commentary);
        let control = Message::assistant("[post-compaction-recovery]\nDerived context", None);
        let anchors = [
            duplicate.id.clone(),
            hidden.id.clone(),
            tool.id.clone(),
            commentary.id.clone(),
            control.id.clone(),
        ];
        source.messages.extend([
            duplicate.clone(),
            duplicate,
            hidden,
            tool,
            commentary,
            control,
        ]);
        store.save_session(&source).await.unwrap();

        assert!(store
            .fork_session_with_projection_guard("missing", "target", "unknown")
            .await
            .unwrap()
            .is_none());
        assert!(matches!(
            store
                .fork_session_with_projection_guard(&source.id, "target", "unknown")
                .await,
            Err(SessionForkError::AnchorNotFound)
        ));
        for anchor in anchors.iter().map(String::as_str).chain([" "]) {
            assert!(
                matches!(
                    store
                        .fork_session_with_projection_guard(&source.id, "target", anchor)
                        .await,
                    Err(SessionForkError::InvalidAnchor)
                ),
                "anchor {anchor}"
            );
        }
        assert!(store.get_index_entry("target").await.is_none());
        assert!(!directory.path().join("sessions/target").exists());
        assert!(store.session_copy_journal_paths().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn session_fork_clears_lifecycle_state_from_a_child() {
        let directory = tempfile::tempdir().unwrap();
        let store = SessionStoreV2::new(directory.path().to_owned())
            .await
            .unwrap();
        let parent = Session::new("root", "model");
        store.save_session(&parent).await.unwrap();
        let mut child = Session::new_child("child", &parent.id, "model", "Child");
        assert_eq!(child.advance_child_launch_generation(), Some(1));
        child.mark_child_auto_run_launch_intent();
        child.cancel_child_launch_generation();
        let mut legacy = AgentRuntimeState::new("legacy-run");
        legacy.set_permission_mode(SessionPermissionMode::Auto);
        child.metadata.insert(
            "agent.runtime.state".into(),
            serde_json::to_string(&legacy).unwrap(),
        );
        let prompt = Message::user("Child request");
        let anchor_id = prompt.id.clone();
        child.add_message(prompt);
        store.save_session(&child).await.unwrap();

        let (forked, guard) = store
            .fork_session_with_projection_guard(&child.id, "fork", &anchor_id)
            .await
            .unwrap()
            .unwrap();
        drop(guard);
        assert_eq!(forked.kind, SessionKind::Root);
        assert!(forked.parent_session_id.is_none());
        assert_eq!(forked.messages[0].id, anchor_id);
        assert!(forked.runtime_metadata.is_none());
        assert!(!forked.metadata.contains_key("agent.runtime.state"));
        let forked_state = forked.agent_runtime_state.unwrap();
        assert_eq!(
            forked_state.effective_permission_mode(),
            SessionPermissionMode::Auto
        );
        assert_ne!(forked_state.run_id, legacy.run_id);
        let stored = store.load_session(&child.id).await.unwrap().unwrap();
        let runtime = stored.runtime_metadata.unwrap();
        assert_eq!(runtime.child_launch_generation, Some(1));
        assert_eq!(runtime.child_cancelled_generation, Some(1));
        assert_eq!(runtime.child_auto_run_launch_intent_generation, Some(1));
    }

    #[test]
    fn session_fork_pairs_only_adjacent_tool_calls_in_retained_history() {
        let call = |id: &str| ToolCall {
            id: id.into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "test".into(),
                arguments: "{}".into(),
            },
        };
        let old = Message::assistant(
            "First round",
            Some(vec![
                call("a"),
                call("b"),
                call("duplicate"),
                call("duplicate"),
            ]),
        );
        let next = Message::assistant("Second round", Some(vec![call("b")]));
        let anchor = Message::user("continue here");
        let anchor_id = anchor.id.clone();
        let mut messages = vec![
            Message::tool_result("b", "leading orphan"),
            old,
            Message::tool_result("a", "first result"),
            Message::tool_result("duplicate", "ambiguous result"),
            Message::user("next turn"),
            next,
            Message::tool_result("b", "later result"),
            anchor,
            Message::tool_result("a", "after fork point"),
        ];

        retain_message_prefix(&mut messages, &anchor_id).unwrap();
        assert_eq!(
            messages
                .iter()
                .map(|row| row.content.as_str())
                .collect::<Vec<_>>(),
            [
                "First round",
                "first result",
                "next turn",
                "Second round",
                "later result",
                "continue here"
            ]
        );
        assert_eq!(
            messages[0]
                .tool_calls
                .as_ref()
                .unwrap()
                .iter()
                .map(|call| call.id.as_str())
                .collect::<Vec<_>>(),
            ["a"]
        );
        assert_eq!(messages[3].tool_calls.as_ref().unwrap()[0].id, "b");
        assert_eq!(messages.last().unwrap().id, anchor_id);
    }
}
