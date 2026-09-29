//! One cross-process Child question transaction. The caller supplies only a
//! bounded execution observation; this store rereads canonical identity and
//! transcript under the same writer lock used by ordinary full saves.

use super::*;
use bamboo_domain::{
    ParentQuestion, ParentQuestionCheckpointV1, ParentQuestionResolution, PendingQuestionSource,
    PARENT_QUESTION_REQUEST_KEY, PARENT_QUESTION_RESOLUTION_KEY,
};

fn parent_request_is_durable(parent: &Session, question: &ParentQuestion) -> bool {
    let expected = question.envelope();
    let mut matches = parent
        .messages
        .iter()
        .filter(|message| message.id == expected.id.as_str());
    let Some(message) = matches.next() else {
        return false;
    };
    matches.next().is_none()
        && message
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("session_message"))
            .and_then(|marker| {
                serde_json::from_value::<SessionMessageEnvelope>(marker.clone()).ok()
            })
            .as_ref()
            == Some(&expected)
        && is_matching_session_message(message, &expected)
        && expected.to_provider_message().is_ok_and(|provider| {
            serde_json::to_value(message).ok() == serde_json::to_value(provider).ok()
        })
}

impl SessionStoreV2 {
    pub async fn checkpoint_parent_question(
        &self,
        expected_child: &Session,
        parent: &Session,
        observation: &ParentQuestionCheckpointV1,
    ) -> io::Result<Option<(Session, ParentQuestion)>> {
        if observation.validate_shape().is_err() || expected_child.kind != SessionKind::Child {
            return Ok(None);
        }
        let started = Instant::now();
        let lifecycle = self.lock_default_writer_lifecycle().await?;
        let task = self.lock_runtime_task_sidecar_shared().await?;
        let writer = self
            .acquire_session_write_lock(&expected_child.id, SaveKind::Full)
            .await?;
        let guards = DefaultWriterGuards::shared(lifecycle, task, writer);
        let Some(mut child) = self.load_session_unlocked(&expected_child.id).await? else {
            return Ok(None);
        };
        if child.id != expected_child.id
            || child.kind != SessionKind::Child
            || child.created_at != expected_child.created_at
            || child.parent_session_id != expected_child.parent_session_id
            || child.root_session_id != expected_child.root_session_id
            || child.spawn_depth != expected_child.spawn_depth
            || child.project_id_meta() != expected_child.project_id_meta()
            || !child.provider_transcript.groups().is_empty()
            || child.parent_session_id.as_deref() != Some(parent.id.as_str())
            || child.last_run_status().as_deref() != Some("running")
            || child.messages.len() < observation.prefix_message_count
            || ParentQuestion::prefix_digest(&child.messages[..observation.prefix_message_count])
                .as_ref()
                != Some(&observation.prefix_digest)
        {
            return Ok(None);
        }
        if let Some(raw) = child.metadata.get(PARENT_QUESTION_REQUEST_KEY) {
            let Ok(existing) = serde_json::from_str::<ParentQuestion>(raw) else {
                return Ok(None);
            };
            // A lost ACK may be replayed even if the parent already answered.
            let original_pair = observation
                .suffix
                .iter()
                .find(|message| message.id == observation.tool_result_message_id);
            if existing.child.session_id == child.id
                && existing.child.created_at == child.created_at
                && existing.parent.session_id == parent.id
                && existing.parent.created_at == parent.created_at
                && existing.tool_call_id == observation.tool_call_id
                && existing.tool_result_message_id == observation.tool_result_message_id
                && existing.question == observation.pending.question
                && existing.options == observation.pending.options
                && existing.allow_custom == observation.pending.allow_custom
                && original_pair.is_some_and(|pair| {
                    bamboo_domain::SessionMessageId::stable(
                        "direct-parent-question-result-v1",
                        &serde_json::to_value(pair).unwrap_or_default(),
                    ) == existing.paired_result_digest
                })
                && (ParentQuestion::for_pending(parent, &child).as_ref() == Some(&existing)
                    || bamboo_domain::ParentQuestionResolution::from_child(
                        parent,
                        &child,
                        &existing.id,
                    )
                    .is_some_and(|resolution| resolution.request == existing))
            {
                return Ok(Some((child, existing)));
            }
            return Ok(None);
        }
        // The Host can have checkpointed an initial Inbox delivery after the
        // Worker captured its prefix. Require any overlap to be byte-exact.
        // A replay of an already answered question is checked above: its
        // paired Tool result was correctly rewritten by the parent CAS.
        let overlapping = child.messages.len() - observation.prefix_message_count;
        if overlapping > observation.suffix.len()
            || serde_json::to_value(&child.messages[observation.prefix_message_count..]).ok()
                != serde_json::to_value(&observation.suffix[..overlapping]).ok()
        {
            return Ok(None);
        }
        if child.pending_question.is_some()
            || child.metadata.contains_key(PARENT_QUESTION_RESOLUTION_KEY)
        {
            return Ok(None);
        }
        let ids = child
            .messages
            .iter()
            .map(|message| message.id.as_str())
            .collect::<std::collections::HashSet<_>>();
        if observation.suffix[overlapping..]
            .iter()
            .any(|message| ids.contains(message.id.as_str()))
        {
            return Ok(None);
        }
        child
            .messages
            .extend_from_slice(&observation.suffix[overlapping..]);
        let mut pending = observation.pending.clone();
        pending.source = PendingQuestionSource::DirectParent;
        child.pending_question = Some(pending);
        child.metadata.insert(
            "runtime.suspend_reason".into(),
            "awaiting_clarification".into(),
        );
        child.updated_at = Utc::now();
        let Some(question) = ParentQuestion::issue_at(parent, &child, child.updated_at) else {
            return Ok(None);
        };
        child.metadata.insert(
            PARENT_QUESTION_REQUEST_KEY.into(),
            serde_json::to_string(&question).map_err(|error| other_io_error(error.to_string()))?,
        );
        self.save_session_after_lock(&child, started, &guards, None)
            .await?;
        Ok(Some((child, question)))
    }

    /// The only writer allowed to replace an ACKed suspended Tool result with
    /// a parent's answer. The parent request, Child pending state, answer and
    /// transcript all receive fresh canonical checks under the Child V2 lock.
    /// Generic Session save/mutate paths never receive this one-use permit.
    pub async fn answer_parent_question<F>(
        &self,
        question: &ParentQuestion,
        text: &str,
        publish: F,
    ) -> io::Result<Option<(Session, bool)>>
    where
        F: FnOnce(&Session) + Send,
    {
        if question.validate_answer(text).is_err()
            || validate_session_id(&question.child.session_id).is_err()
            || validate_session_id(&question.parent.session_id).is_err()
        {
            return Ok(None);
        }
        let started = Instant::now();
        let lifecycle = self.lock_default_writer_lifecycle().await?;
        let task = self.lock_runtime_task_sidecar_shared().await?;
        let writer = self
            .acquire_session_write_lock(&question.child.session_id, SaveKind::Full)
            .await?;
        let guards = DefaultWriterGuards::shared(lifecycle, task, writer);
        let Some(parent) = self
            .load_session_unlocked(&question.parent.session_id)
            .await?
        else {
            return Ok(None);
        };
        let Some(mut child) = self
            .load_session_unlocked(&question.child.session_id)
            .await?
        else {
            return Ok(None);
        };
        if parent.id != question.parent.session_id
            || parent.created_at != question.parent.created_at
            || child.id != question.child.session_id
            || child.created_at != question.child.created_at
            || !parent_request_is_durable(&parent, question)
        {
            return Ok(None);
        }
        if let Some(terminal) = ParentQuestionResolution::from_child(&parent, &child, &question.id)
        {
            if terminal.request == *question {
                publish(&child);
                return Ok(Some((child, false)));
            }
            return Ok(None);
        }
        if Utc::now() >= question.deadline
            || ParentQuestion::for_pending(&parent, &child).as_ref() != Some(question)
        {
            return Ok(None);
        }
        let Some(paired) = child
            .messages
            .iter_mut()
            .find(|message| message.id == question.tool_result_message_id)
        else {
            return Ok(None);
        };
        paired.content = text.to_owned();
        paired.tool_success = Some(true);
        let Some(resolution) = ParentQuestionResolution::answered(question, Utc::now(), text)
        else {
            return Ok(None);
        };
        child.metadata.insert(
            PARENT_QUESTION_RESOLUTION_KEY.into(),
            serde_json::to_string(&resolution)
                .map_err(|error| other_io_error(error.to_string()))?,
        );
        self.save_session_after_lock(&child, started, &guards, Some(question))
            .await?;
        publish(&child);
        Ok(Some((child, true)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::{FunctionCall, ParentQuestionResolution, PendingQuestion, ToolCall};

    fn observation(
        prefix: &[Message],
        source: PendingQuestionSource,
    ) -> ParentQuestionCheckpointV1 {
        let assistant = Message::assistant(
            "",
            Some(vec![ToolCall {
                id: "question-call".into(),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: "AgenticQuestion".into(),
                    arguments: "{}".into(),
                },
            }]),
        );
        let result = Message::tool_result_with_status(
            "question-call",
            "Clarification needed: Which option?",
            true,
        );
        let pending = PendingQuestion {
            tool_call_id: "question-call".into(),
            tool_name: "AgenticQuestion".into(),
            question: "Which option?".into(),
            options: vec!["A".into(), "B".into()],
            allow_custom: true,
            source,
        };
        ParentQuestionCheckpointV1 {
            version: 1,
            prefix_message_count: prefix.len(),
            prefix_digest: ParentQuestion::prefix_digest(prefix).unwrap(),
            suffix: vec![assistant, result.clone()],
            tool_call_id: pending.tool_call_id.clone(),
            question_digest: ParentQuestion::question_digest(&pending, &result.id),
            tool_result_message_id: result.id,
            pending,
        }
    }

    #[tokio::test]
    async fn child_question_checkpoint_is_atomic_replayable_and_rejects_pause_source() {
        let temp = tempfile::tempdir().unwrap();
        let store = SessionStoreV2::new(temp.path().to_path_buf())
            .await
            .unwrap();
        let parent = Session::new("question-parent", "model");
        store.save_session(&parent).await.unwrap();
        let mut child = Session::new_child_of("question-child", &parent, "model", "Child");
        child.add_message(Message::user("assignment"));
        child.set_last_run_status("running");
        store.save_session(&child).await.unwrap();
        let mut forged = observation(&child.messages, PendingQuestionSource::PauseTool);
        assert!(store
            .checkpoint_parent_question(&child, &parent, &forged)
            .await
            .unwrap()
            .is_none());
        forged.pending.source = PendingQuestionSource::AgenticClarification;
        forged.question_digest =
            ParentQuestion::question_digest(&forged.pending, &forged.tool_result_message_id);
        forged.suffix[1].content = "Clarification needed: a different question".into();
        assert!(store
            .checkpoint_parent_question(&child, &parent, &forged)
            .await
            .unwrap()
            .is_none());
        let valid = observation(&child.messages, PendingQuestionSource::AgenticClarification);
        let (saved, issued) = store
            .checkpoint_parent_question(&child, &parent, &valid)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(saved.messages.len(), child.messages.len() + 2);
        assert_eq!(
            ParentQuestion::for_pending(&parent, &saved),
            Some(issued.clone())
        );
        let reopened = SessionStoreV2::new(temp.path().to_path_buf())
            .await
            .unwrap();
        let (replayed, same) = reopened
            .checkpoint_parent_question(&child, &parent, &valid)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(same.id, issued.id);
        assert_eq!(replayed.messages.len(), saved.messages.len());
        let mut answered = replayed;
        let resolution = ParentQuestionResolution::answered(&issued, Utc::now(), "A").unwrap();
        answered
            .messages
            .iter_mut()
            .find(|message| message.id == issued.tool_result_message_id)
            .unwrap()
            .content = "A".into();
        answered.metadata.insert(
            PARENT_QUESTION_RESOLUTION_KEY.into(),
            serde_json::to_string(&resolution).unwrap(),
        );
        reopened.save_session(&answered).await.unwrap();
        let (replayed_after_answer, same) = reopened
            .checkpoint_parent_question(&child, &parent, &valid)
            .await
            .unwrap()
            .expect("lost ACK is stable after the parent rewrites the Tool result");
        assert_eq!(same.id, issued.id);
        assert_eq!(
            replayed_after_answer.messages.len(),
            answered.messages.len()
        );
        assert_eq!(replayed_after_answer.messages.last().unwrap().content, "A");
        let mut stale = valid;
        stale.prefix_digest = bamboo_domain::SessionMessageId::new();
        assert!(reopened
            .checkpoint_parent_question(&child, &parent, &stale)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn choice_only_child_question_checkpoints_with_exact_options() {
        let temp = tempfile::tempdir().unwrap();
        let store = SessionStoreV2::new(temp.path().to_path_buf())
            .await
            .unwrap();
        let parent = Session::new("choice-parent", "model");
        store.save_session(&parent).await.unwrap();
        let mut child = Session::new_child_of("choice-child", &parent, "model", "Child");
        child.add_message(Message::user("assignment"));
        child.set_last_run_status("running");
        store.save_session(&child).await.unwrap();

        let mut observed =
            observation(&child.messages, PendingQuestionSource::AgenticClarification);
        observed.pending.allow_custom = false;
        observed.pending.options.clear();
        observed.question_digest =
            ParentQuestion::question_digest(&observed.pending, &observed.tool_result_message_id);
        assert!(store
            .checkpoint_parent_question(&child, &parent, &observed)
            .await
            .unwrap()
            .is_none());

        observed.pending.options = vec!["A".into(), "B".into()];
        observed.question_digest =
            ParentQuestion::question_digest(&observed.pending, &observed.tool_result_message_id);
        let (saved, question) = store
            .checkpoint_parent_question(&child, &parent, &observed)
            .await
            .unwrap()
            .expect("Host must accept a choice-only Child question with offered options");
        assert!(!question.allow_custom);
        assert_eq!(
            ParentQuestion::for_pending(&parent, &saved),
            Some(question.clone())
        );
        assert!(question.validate_answer("A").is_ok());
        assert!(question.validate_answer("C").is_err());
        assert_eq!(
            store
                .checkpoint_parent_question(&child, &parent, &observed)
                .await
                .unwrap()
                .unwrap()
                .1,
            question
        );
    }
}
