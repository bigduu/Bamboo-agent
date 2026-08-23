//! Durable reconciliation and chronological projection for host model context.

use std::collections::{BTreeMap, BTreeSet};

use bamboo_agent_core::{ContextBlock, ContextBlockType, Message, Session};
use bamboo_domain::{
    deterministic_model_context_event_id, model_context_block_sha256, removed_model_context_sha256,
    render_model_context_removal, render_model_context_snapshot, sha256_hex, ContextBlockBaseline,
    ModelContextEvent, ModelContextEventKind, ModelContextResetReason, ModelContextState,
    MAX_MODEL_CONTEXT_EVENTS, MAX_MODEL_CONTEXT_RENDERED_BYTES, MODEL_CONTEXT_SCHEMA_VERSION,
};

const ANCHOR_DOMAIN: &[u8] = b"bamboo/model-context-anchor/v1\0";

#[derive(Debug)]
pub(super) struct ContextLedgerOutcome {
    pub transcript: Vec<Message>,
    pub changed: bool,
    pub prefix_epoch: u64,
    pub reset_reason: Option<ModelContextResetReason>,
}

/// Reconcile current typed blocks and project the durable events into one model
/// transcript.  Event identity and anchors are derived only from semantic
/// content/order; random `Message.id` values never participate.
pub(super) fn reconcile_model_context(
    session: &mut Session,
    current_blocks: Vec<ContextBlock>,
    real_transcript: &[Message],
    cache_scope_sha256: String,
    hard_truncation: bool,
) -> ContextLedgerOutcome {
    let current_item_sha256 = real_transcript
        .iter()
        .map(model_message_sha256)
        .collect::<Vec<_>>();
    let had_state = session.model_context_state.is_some();
    let mut state = session.model_context_state.take().unwrap_or_default();
    let original_state = state.clone();
    let original_revision = state.state_revision;

    // A reset may already have been declared atomically by compression or a
    // rollback use case.  In that case cache_scope is intentionally empty and
    // the reconciliation below simply seeds the new epoch.
    let reset_was_pending = had_state
        && state.cache_scope_sha256.is_none()
        && state.last_reset_reason.is_some()
        && state.baselines.is_empty()
        && state.events.is_empty();

    if state.schema_version != MODEL_CONTEXT_SCHEMA_VERSION {
        state.reset_epoch(ModelContextResetReason::ExplicitHistoryRewrite);
    } else if let Some(previous_scope) = state.cache_scope_sha256.as_deref() {
        if previous_scope != cache_scope_sha256 {
            state.reset_epoch(ModelContextResetReason::CacheScopeChanged);
        } else if !current_item_sha256.starts_with(&state.transcript_item_sha256) {
            let reason = if hard_truncation {
                ModelContextResetReason::HardTruncation
            } else if session.messages.iter().any(|message| message.compressed) {
                ModelContextResetReason::Compression
            } else {
                ModelContextResetReason::Rollback
            };
            state.reset_epoch(reason);
        }
    }

    let seeding_epoch = !had_state
        || reset_was_pending
        || (state.cache_scope_sha256.is_none()
            && state.baselines.is_empty()
            && state.events.is_empty()
            && state.last_reset_reason.is_some());
    let anchor_message_id = if seeding_epoch {
        None
    } else {
        real_transcript
            .last()
            .map(|message| deterministic_anchor(real_transcript.len().saturating_sub(1), message))
    };

    let current = current_blocks
        .into_iter()
        .map(|block| (block.block_type, block))
        .collect::<BTreeMap<_, _>>();
    append_reconciliation_events(&session.id, &mut state, &current, anchor_message_id.clone());

    if state.events.len() > MAX_MODEL_CONTEXT_EVENTS
        || rendered_event_bytes(&state.events) > MAX_MODEL_CONTEXT_RENDERED_BYTES
    {
        state.reset_epoch(ModelContextResetReason::RetentionLimit);
        append_reconciliation_events(&session.id, &mut state, &current, None);
    }

    state.cache_scope_sha256 = Some(cache_scope_sha256);
    state.transcript_item_sha256 = current_item_sha256;
    // A reset already advances the revision. Otherwise advance exactly once
    // for this reconciliation transaction, including a transcript-only update
    // where `next_sequence` remains unchanged.
    if state != original_state && state.state_revision == original_revision {
        state.advance_state_revision();
    }
    let transcript = interleave_transcript(real_transcript, &state.events);
    let changed = state != original_state;
    let prefix_epoch = state.prefix_epoch;
    // `last_reset_reason` remains durable history, but it is observable on the
    // wire only for the request that seeds the new epoch. Normal requests in
    // that epoch must not look like repeated resets.
    let reset_reason = (reset_was_pending || state.prefix_epoch != original_state.prefix_epoch)
        .then_some(state.last_reset_reason)
        .flatten();
    session.model_context_state = Some(state);

    ContextLedgerOutcome {
        transcript,
        changed,
        prefix_epoch,
        reset_reason,
    }
}

fn rendered_event_bytes(events: &[ModelContextEvent]) -> usize {
    events.iter().fold(0usize, |total, event| {
        total.saturating_add(event.rendered_text.len())
    })
}

fn append_reconciliation_events(
    session_id: &str,
    state: &mut ModelContextState,
    current: &BTreeMap<ContextBlockType, ContextBlock>,
    anchor_message_id: Option<String>,
) {
    let types = state
        .baselines
        .keys()
        .chain(current.keys())
        .copied()
        .collect::<BTreeSet<_>>();

    for block_type in types {
        match current.get(&block_type) {
            Some(block) => {
                let digest = model_context_block_sha256(block);
                let previous = state.baselines.get(&block_type).cloned();
                if previous
                    .as_ref()
                    .is_some_and(|baseline| baseline.content_sha256 == digest)
                {
                    continue;
                }
                let revision = previous
                    .as_ref()
                    .map_or(1, |baseline| baseline.revision.saturating_add(1));
                let supersedes_revision = previous.as_ref().map(|baseline| baseline.revision);
                let sequence = state.next_sequence;
                let id = deterministic_model_context_event_id(
                    session_id,
                    state.prefix_epoch,
                    block_type,
                    revision,
                    &digest,
                );
                let rendered_text = render_model_context_snapshot(
                    &id,
                    state.prefix_epoch,
                    sequence,
                    block,
                    revision,
                    supersedes_revision,
                );
                state.events.push(ModelContextEvent {
                    id,
                    epoch: state.prefix_epoch,
                    sequence,
                    anchor_message_id: anchor_message_id.clone(),
                    block_type,
                    revision,
                    supersedes_revision,
                    kind: ModelContextEventKind::Snapshot,
                    content_sha256: digest.clone(),
                    rendered_text,
                });
                state.baselines.insert(
                    block_type,
                    ContextBlockBaseline {
                        revision,
                        content_sha256: digest,
                    },
                );
                state.next_sequence = state.next_sequence.saturating_add(1);
            }
            None => {
                let Some(previous) = state.baselines.get(&block_type).cloned() else {
                    continue;
                };
                let digest = removed_model_context_sha256(block_type);
                if previous.content_sha256 == digest {
                    continue;
                }
                let revision = previous.revision.saturating_add(1);
                let sequence = state.next_sequence;
                let id = deterministic_model_context_event_id(
                    session_id,
                    state.prefix_epoch,
                    block_type,
                    revision,
                    &digest,
                );
                let rendered_text = render_model_context_removal(
                    &id,
                    state.prefix_epoch,
                    sequence,
                    block_type,
                    revision,
                    previous.revision,
                );
                state.events.push(ModelContextEvent {
                    id,
                    epoch: state.prefix_epoch,
                    sequence,
                    anchor_message_id: anchor_message_id.clone(),
                    block_type,
                    revision,
                    supersedes_revision: Some(previous.revision),
                    kind: ModelContextEventKind::Removed,
                    content_sha256: digest.clone(),
                    rendered_text,
                });
                state.baselines.insert(
                    block_type,
                    ContextBlockBaseline {
                        revision,
                        content_sha256: digest,
                    },
                );
                state.next_sequence = state.next_sequence.saturating_add(1);
            }
        }
    }
}

fn interleave_transcript(
    real_transcript: &[Message],
    events: &[ModelContextEvent],
) -> Vec<Message> {
    let mut transcript = events
        .iter()
        .filter(|event| event.anchor_message_id.is_none())
        .map(ModelContextEvent::render_message)
        .collect::<Vec<_>>();
    let mut emitted = events
        .iter()
        .filter(|event| event.anchor_message_id.is_none())
        .map(|event| event.id.as_str())
        .collect::<BTreeSet<_>>();

    for (index, message) in real_transcript.iter().enumerate() {
        transcript.push(message.clone());
        let anchor = deterministic_anchor(index, message);
        for event in events
            .iter()
            .filter(|event| event.anchor_message_id.as_deref() == Some(anchor.as_str()))
        {
            transcript.push(event.render_message());
            emitted.insert(event.id.as_str());
        }
    }

    // An anchor from an older incompatible transcript should normally have
    // caused an epoch reset.  Fail open by retaining any unmatched durable event
    // at the end rather than silently dropping model-visible state.
    transcript.extend(
        events
            .iter()
            .filter(|event| !emitted.contains(event.id.as_str()))
            .map(ModelContextEvent::render_message),
    );
    transcript
}

fn deterministic_anchor(index: usize, message: &Message) -> String {
    let mut bytes = Vec::with_capacity(ANCHOR_DOMAIN.len() + 8 + 64);
    bytes.extend_from_slice(ANCHOR_DOMAIN);
    bytes.extend_from_slice(&(index as u64).to_be_bytes());
    bytes.extend_from_slice(model_message_sha256(message).as_bytes());
    format!("anchor_{}", sha256_hex(&bytes))
}

/// Hash only fields that can affect provider rendering. Random ids, timestamps,
/// compression bookkeeping, and UI metadata are intentionally excluded.
fn model_message_sha256(message: &Message) -> String {
    let value = serde_json::json!({
        "role": message.role,
        "content": message.content,
        "reasoning": message.reasoning,
        "reasoning_signature": message.reasoning_signature,
        "content_parts": message.content_parts,
        "phase": message.phase,
        "tool_calls": message.tool_calls,
        "tool_call_id": message.tool_call_id,
        "tool_success": message.tool_success,
    });
    sha256_hex(&serde_json::to_vec(&value).expect("message semantic JSON is serializable"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::{ContextBlockPriority, ContextBlockStability, Role};

    fn block(content: &str) -> ContextBlock {
        ContextBlock::new(
            ContextBlockType::TaskSnapshot,
            ContextBlockPriority::High,
            ContextBlockStability::RoundDynamic,
            "Task",
            content,
        )
    }

    #[test]
    fn unchanged_update_removal_and_retry_are_append_only_and_deterministic() {
        let mut session = Session::new("ledger-session", "model");
        session.add_message(Message::user("first"));
        let first = reconcile_model_context(
            &mut session,
            vec![block("v1")],
            &[Message::user("first")],
            "scope".to_string(),
            false,
        );
        assert!(first.changed);
        assert_eq!(first.transcript.len(), 2);
        assert!(first.transcript[0].content.contains("event_kind: snapshot"));
        assert_eq!(first.transcript[1].content, "first");
        assert!(session
            .messages
            .iter()
            .all(|message| { !message.content.contains("BAMBOO_MODEL_CONTEXT_EVENT_START") }));

        let persisted = serde_json::to_string(&session).unwrap();
        let mut resumed: Session = serde_json::from_str(&persisted).unwrap();
        let retry = reconcile_model_context(
            &mut resumed,
            vec![block("v1")],
            &[Message::user("first")],
            "scope".to_string(),
            false,
        );
        assert!(!retry.changed);
        assert_eq!(
            first
                .transcript
                .iter()
                .map(|message| message.content.as_str())
                .collect::<Vec<_>>(),
            retry
                .transcript
                .iter()
                .map(|message| message.content.as_str())
                .collect::<Vec<_>>()
        );

        let real = vec![Message::user("first"), Message::assistant("answer", None)];
        let updated = reconcile_model_context(
            &mut resumed,
            vec![block("v2")],
            &real,
            "scope".to_string(),
            false,
        );
        assert_eq!(
            updated.transcript[..first.transcript.len()]
                .iter()
                .map(|message| (&message.role, message.content.as_str()))
                .collect::<Vec<_>>(),
            first
                .transcript
                .iter()
                .map(|message| (&message.role, message.content.as_str()))
                .collect::<Vec<_>>()
        );
        assert!(updated
            .transcript
            .last()
            .unwrap()
            .content
            .contains("supersedes_revision: 1"));

        let removed =
            reconcile_model_context(&mut resumed, vec![], &real, "scope".to_string(), false);
        assert_eq!(
            removed.transcript[..updated.transcript.len()]
                .iter()
                .map(|message| (&message.role, message.content.as_str()))
                .collect::<Vec<_>>(),
            updated
                .transcript
                .iter()
                .map(|message| (&message.role, message.content.as_str()))
                .collect::<Vec<_>>()
        );
        assert!(removed
            .transcript
            .last()
            .unwrap()
            .content
            .contains("event_kind: removed"));
        assert!(matches!(removed.transcript[0].role, Role::User));
    }

    #[test]
    fn hard_truncation_resets_epoch_and_coalesces_current_state() {
        let mut session = Session::new("ledger-reset", "model");
        let old = vec![Message::user("one"), Message::assistant("two", None)];
        let first = reconcile_model_context(
            &mut session,
            vec![block("v1")],
            &old,
            "scope".to_string(),
            false,
        );
        let rewritten = vec![Message::user("replacement")];
        let reset = reconcile_model_context(
            &mut session,
            vec![block("v1")],
            &rewritten,
            "scope".to_string(),
            true,
        );
        assert_eq!(reset.prefix_epoch, first.prefix_epoch + 1);
        assert_eq!(
            reset.reset_reason,
            Some(ModelContextResetReason::HardTruncation)
        );
        assert_eq!(reset.transcript.len(), 2);
        assert!(reset.transcript[0].content.contains("prefix_epoch: 1"));
        assert_eq!(reset.transcript[1].content, "replacement");

        let next = reconcile_model_context(
            &mut session,
            vec![block("v1")],
            &rewritten,
            "scope".to_string(),
            false,
        );
        assert_eq!(next.prefix_epoch, reset.prefix_epoch);
        assert_eq!(next.reset_reason, None, "reset is observable exactly once");
        assert!(!next.changed);
    }

    #[test]
    fn retention_limit_starts_one_bounded_epoch_with_current_snapshot() {
        let mut session = Session::new("ledger-retention", "model");
        let empty_transcript = Vec::<Message>::new();

        for revision in 0..=MAX_MODEL_CONTEXT_EVENTS {
            let outcome = reconcile_model_context(
                &mut session,
                vec![block(&format!("current-v{revision}"))],
                &empty_transcript,
                "scope".to_string(),
                false,
            );
            assert!(outcome.transcript.len() <= MAX_MODEL_CONTEXT_EVENTS);
        }

        let state = session.model_context_state.as_ref().unwrap();
        assert_eq!(state.prefix_epoch, 1);
        assert_eq!(
            state.last_reset_reason,
            Some(ModelContextResetReason::RetentionLimit)
        );
        assert_eq!(state.events.len(), 1);
        assert_eq!(state.events[0].epoch, state.prefix_epoch);
        assert_eq!(state.events[0].sequence, 0);
        assert!(state.events[0].rendered_text.contains("current-v256"));
        assert_eq!(state.next_sequence, 1);

        let before = state.clone();
        let retry = reconcile_model_context(
            &mut session,
            vec![block("current-v256")],
            &empty_transcript,
            "scope".to_string(),
            false,
        );
        assert!(!retry.changed);
        assert_eq!(session.model_context_state.as_ref(), Some(&before));
    }
}
