//! Canonical native Actor publication, without a transport or Worker envelope.
use super::super::actor_event_stream::{
    opaque_event_id, ActorEventObserver, ActorEventSourceOrder, PublicActorEvent,
    PublicActorEventClass,
};
use bamboo_agent_core::AgentEvent;
use bamboo_domain::{
    ChildActorRuntimeWrite, ParentQuestion, ParentQuestionResolution, PendingQuestionSource,
    Session, Storage, PARENT_QUESTION_REQUEST_KEY,
};
use bamboo_storage::SessionStoreV2;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

fn classify(event: &AgentEvent) -> PublicActorEventClass {
    // Keep the same projection as the legacy Actor event router. Token bodies
    // never enter the public Actor projection, even when classified ephemeral.
    match event {
        AgentEvent::PermissionPostureActivated { .. } => PublicActorEventClass::Lifecycle,
        AgentEvent::RunnerProgress { .. } => PublicActorEventClass::Snapshot,
        event if event.is_durable_change() => PublicActorEventClass::Semantic,
        _ => PublicActorEventClass::Ephemeral,
    }
}

fn projection(
    owner: &ChildActorRuntimeWrite,
    sequence: u64,
    class: PublicActorEventClass,
) -> PublicActorEvent {
    let fence = &owner.fence;
    PublicActorEvent {
        actor_id: fence.actor_id.clone(),
        root_actor_id: owner.root_session_id.clone(),
        parent_actor_id: Some(owner.parent_session_id.clone()),
        activation_id: fence.activation_id.clone(),
        attempt: fence.attempt,
        event_id: opaque_event_id(&format!(
            "local-task:{}:{}:{}:{}:{}:{sequence}",
            fence.actor_id, fence.activation_id, fence.run_id, fence.attempt, fence.lease_epoch
        )),
        class,
        source_order: ActorEventSourceOrder {
            lease_epoch: fence.lease_epoch,
            // Native execution is admitted exactly once per canonical fence;
            // unlike a resident Worker, it has no transport execution epochs.
            execution_epoch: 1,
            sequence,
        },
    }
}

fn is_parent_question(event: &AgentEvent) -> bool {
    matches!(
        event,
        AgentEvent::NeedClarification {
            source: Some(
                PendingQuestionSource::AgenticClarification | PendingQuestionSource::DirectParent
            ),
            ..
        }
    )
}

fn proven_parent_question(
    owner: &ChildActorRuntimeWrite,
    parent: &Session,
    child: &Session,
    event: &AgentEvent,
) -> bool {
    let AgentEvent::NeedClarification {
        question,
        options,
        tool_call_id: Some(tool_call_id),
        tool_name: Some(tool_name),
        allow_custom,
        ..
    } = event
    else {
        return false;
    };
    if !is_parent_question(event)
        || child.id != owner.fence.actor_id
        || child.created_at != owner.created_at
        || parent.id != owner.parent_session_id
        || parent.created_at != owner.parent_created_at
        || child.root_session_id != owner.root_session_id
        || child.spawn_depth != owner.spawn_depth
        || child.project_id_meta() != owner.project_id
    {
        return false;
    }
    let Some(raw) = child.metadata.get(PARENT_QUESTION_REQUEST_KEY) else {
        return false;
    };
    let Ok(recorded) = serde_json::from_str::<ParentQuestion>(raw) else {
        return false;
    };
    if recorded.tool_call_id != *tool_call_id
        || recorded.tool_name != *tool_name
        || recorded.question != *question
        || recorded.options.as_slice() != options.as_deref().unwrap_or_default()
        || recorded.allow_custom != *allow_custom
    {
        return false;
    }
    // The bound save already persisted the exact parent outbox. A parent may
    // answer before this event is dequeued; its canonical resolution proves
    // the same occurrence and must not turn that successful race into failure.
    ParentQuestion::for_pending(parent, child).as_ref() == Some(&recorded)
        || ParentQuestionResolution::from_child(parent, child, &recorded.id)
            .is_some_and(|resolution| resolution.request == recorded)
}

async fn check_parent_question(
    store: &SessionStoreV2,
    owner: &ChildActorRuntimeWrite,
    event: &AgentEvent,
) -> Result<(), String> {
    let parent = store
        .load_session(&owner.parent_session_id)
        .await
        .map_err(|error| format!("local parent question parent unavailable: {error}"))?
        .ok_or("local parent question parent disappeared")?;
    let child = store
        .load_session(&owner.fence.actor_id)
        .await
        .map_err(|error| format!("local parent question Child unavailable: {error}"))?
        .ok_or("local parent question Child disappeared")?;
    if !proven_parent_question(owner, &parent, &child, event) {
        return Err("local parent question lacks its exact canonical request".into());
    }
    Ok(())
}

/// Run inside the owned Agent task and join after all event producers drain.
/// The caller must drop its internal sender before joining this forwarder.
/// No await, queue reservation or subscriber work may hold the physical writer
/// guards; only the final bounded send and nonblocking observer run there.
pub(super) async fn forward(
    store: Arc<SessionStoreV2>,
    owner: ChildActorRuntimeWrite,
    observer: Option<Arc<dyn ActorEventObserver>>,
    mut rx: mpsc::Receiver<AgentEvent>,
    tx: mpsc::Sender<AgentEvent>,
    cancel: CancellationToken,
) -> Result<(), String> {
    let result = async {
        if tx.is_closed() {
            return Err("local Actor event queue closed".to_string());
        }
        let mut sequence = 1_u64;
        // A fresh activation must introduce its source order before semantic
        // events. This metadata-only marker asserts no permission posture and
        // also works for legacy/test children without a permission audit.
        let lifecycle = projection(&owner, sequence, PublicActorEventClass::Lifecycle);
        let initial_observer = observer.clone();
        store
            .publish_child_actor_runtime_event(
                &owner,
                Box::new(move |validate| {
                    validate()?;
                    if let Some(observer) = initial_observer {
                        observer.publish(lifecycle);
                    }
                    Ok(())
                }),
            )
            .await
            .map_err(|error| format!("local Actor lifecycle fence rejected: {error}"))?;
        while let Some(event) = rx.recv().await {
            if tx.is_closed() {
                return Err("local Actor event queue closed".to_string());
            }
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| "local Actor event sequence exhausted".to_string())?;
            let public = projection(&owner, sequence, classify(&event));
            if is_parent_question(&event) {
                check_parent_question(&store, &owner, &event).await?;
                let observer = observer.clone();
                store
                    .publish_child_actor_runtime_event(
                        &owner,
                        Box::new(move |validate| {
                            validate()?;
                            if let Some(observer) = observer {
                                observer.publish(public);
                            }
                            Ok(())
                        }),
                    )
                    .await
                    .map_err(|error| format!("local parent question fence rejected: {error}"))?;
                // ParentQuestionCoordinator delivers the durable outbox. Never
                // turn a direct-parent question into a human UI clarification.
                continue;
            }
            // Reserve capacity before taking the canonical physical guards.
            // Prefer an available slot even during cancellation so queued
            // final events still drain. Cancellation breaks blocked consumers.
            let permit = tokio::select! {
                biased;
                permit = tx.clone().reserve_owned() => permit
                    .map_err(|_| "local Actor event queue closed".to_string())?,
                _ = cancel.cancelled() => return Err("local Actor event queue cancelled while blocked".to_string()),
            };
            let observer = observer.clone();
            store
                .publish_child_actor_runtime_event(
                    &owner,
                    Box::new(move |validate| {
                        validate()?;
                        let sender = permit.send(event);
                        if sender.is_closed() {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::BrokenPipe,
                                "local Actor event queue closed",
                            ));
                        }
                        if let Some(observer) = observer {
                            validate()?;
                            observer.publish(public);
                        }
                        Ok(())
                    }),
                )
                .await
                .map_err(|error| format!("local Actor event fence rejected: {error}"))?;
        }
        Ok(())
    }
    .await;
    if result.is_err() {
        cancel.cancel();
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::{
        ActorActivationClaim, ActorActivationFinish, ActorDirectoryPort, FunctionCall, Message,
        ToolCall, PARENT_QUESTION_RESOLUTION_KEY,
    };
    use std::{sync::Mutex, time::Duration};

    #[derive(Default)]
    struct Observer(Mutex<Vec<PublicActorEvent>>);
    impl ActorEventObserver for Observer {
        fn publish(&self, event: PublicActorEvent) {
            self.0.lock().unwrap().push(event);
        }
    }
    struct Fixture {
        store: Arc<SessionStoreV2>,
        owner: ChildActorRuntimeWrite,
        observer: Arc<Observer>,
        cancel: CancellationToken,
        _home: tempfile::TempDir,
    }
    impl Fixture {
        async fn new() -> Self {
            Self::build(None).await
        }
        async fn build(answered_question: Option<bool>) -> Self {
            let home = tempfile::tempdir().unwrap();
            let store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
            let root = Session::new("event-root", "model");
            store.save_session(&root).await.unwrap();
            let mut child = Session::new_child_of("event-child", &root, "model", "Child");
            if let Some(answered) = answered_question {
                child.messages.push(Message::assistant(
                    "",
                    Some(vec![ToolCall {
                        id: "question-call".into(),
                        tool_type: "function".into(),
                        function: FunctionCall {
                            name: "AskUserQuestion".into(),
                            arguments: "{}".into(),
                        },
                    }]),
                ));
                child.messages.push(Message::tool_result_with_status(
                    "question-call",
                    "Clarification needed: Which option?",
                    true,
                ));
                child.set_pending_question_with_source(
                    "question-call".into(),
                    "AskUserQuestion".into(),
                    "Which option?".into(),
                    vec!["A".into(), "B".into()],
                    true,
                    PendingQuestionSource::DirectParent,
                );
                child.metadata.insert(
                    "runtime.suspend_reason".into(),
                    "awaiting_clarification".into(),
                );
                let question = ParentQuestion::issue_at(&root, &child, chrono::Utc::now()).unwrap();
                child.metadata.insert(
                    PARENT_QUESTION_REQUEST_KEY.into(),
                    serde_json::to_string(&question).unwrap(),
                );
                if answered {
                    let resolution =
                        ParentQuestionResolution::answered(&question, chrono::Utc::now(), "A")
                            .unwrap();
                    child.messages.last_mut().unwrap().content = "A".into();
                    child.pending_question = None;
                    child.metadata.remove("runtime.suspend_reason");
                    child.metadata.insert(
                        PARENT_QUESTION_RESOLUTION_KEY.into(),
                        serde_json::to_string(&resolution).unwrap(),
                    );
                }
            }
            store.save_session(&child).await.unwrap();
            let now = chrono::Utc::now();
            let activation = store
                .claim_activation(&ActorActivationClaim {
                    actor_id: child.id.clone(),
                    run_id: "event-run".into(),
                    lease_owner: "local-event-host".into(),
                    lease_expires_at: now + chrono::Duration::minutes(1),
                    inbox_generation: 0,
                    placement_ref: None,
                    now,
                })
                .await
                .unwrap();
            store
                .start_activation(&activation.fence(), now)
                .await
                .unwrap();
            Self {
                owner: ChildActorRuntimeWrite::from_session(activation.fence(), &child).unwrap(),
                store,
                observer: Arc::default(),
                cancel: CancellationToken::new(),
                _home: home,
            }
        }
        async fn retire(&self) {
            self.store
                .finish_activation(
                    &self.owner.fence,
                    chrono::Utc::now(),
                    ActorActivationFinish::Cancelled,
                )
                .await
                .unwrap();
        }
        fn forward(
            &self,
            rx: mpsc::Receiver<AgentEvent>,
            tx: mpsc::Sender<AgentEvent>,
        ) -> tokio::task::JoinHandle<Result<(), String>> {
            tokio::spawn(forward(
                self.store.clone(),
                self.owner.clone(),
                Some(self.observer.clone()),
                rx,
                tx,
                self.cancel.clone(),
            ))
        }
    }

    fn question_event(source: PendingQuestionSource) -> AgentEvent {
        AgentEvent::NeedClarification {
            question: "Which option?".into(),
            options: Some(vec!["A".into(), "B".into()]),
            tool_call_id: Some("question-call".into()),
            tool_name: Some("AskUserQuestion".into()),
            allow_custom: true,
            source: Some(source),
        }
    }

    #[tokio::test]
    async fn direct_parent_question_suppresses_human_ui_even_after_fast_answer() {
        for answered in [false, true] {
            for source in [
                PendingQuestionSource::AgenticClarification,
                PendingQuestionSource::DirectParent,
            ] {
                let f = Fixture::build(Some(answered)).await;
                let (input, rx) = mpsc::channel(1);
                input.send(question_event(source)).await.unwrap();
                drop(input);
                let (tx, mut output) = mpsc::channel(1);
                f.forward(rx, tx).await.unwrap().unwrap();
                assert!(output.recv().await.is_none());
                assert!(!f.cancel.is_cancelled());
                assert_eq!(f.observer.0.lock().unwrap().len(), 2);
            }
        }
    }

    #[tokio::test]
    async fn unproven_parent_question_fails_closed_without_human_ui() {
        for changed_question in [false, true] {
            let f = if changed_question {
                Fixture::build(Some(false)).await
            } else {
                Fixture::new().await
            };
            let (input, rx) = mpsc::channel(1);
            let mut event = question_event(PendingQuestionSource::AgenticClarification);
            if changed_question {
                let AgentEvent::NeedClarification { question, .. } = &mut event else {
                    unreachable!()
                };
                *question = "Substituted question".into();
            }
            input.send(event).await.unwrap();
            drop(input);
            let (tx, mut output) = mpsc::channel(1);
            assert!(f.forward(rx, tx).await.unwrap().is_err());
            assert!(output.recv().await.is_none());
            assert!(f.cancel.is_cancelled());
            assert_eq!(f.observer.0.lock().unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn ordered_events_use_canonical_fence_and_safe_public_projection() {
        let f = Fixture::new().await;
        let events = [
            AgentEvent::Token {
                content: "private-token".into(),
            },
            AgentEvent::RunnerProgress {
                session_id: f.owner.fence.actor_id.clone(),
                round_count: 1,
            },
            AgentEvent::Error {
                message: "private-error".into(),
            },
            AgentEvent::PermissionPostureActivated {
                session_id: f.owner.fence.actor_id.clone(),
                policy_revision: 1,
                requested_mode: "default".into(),
                effective_mode: "default".into(),
                executor_mapping: "native".into(),
            },
        ];
        let (input, rx) = mpsc::channel(4);
        let (tx, mut output) = mpsc::channel(4);
        for event in &events {
            input.send(event.clone()).await.unwrap();
        }
        drop(input);
        // Cancellation must not discard final events while output is writable.
        f.cancel.cancel();
        f.forward(rx, tx).await.unwrap().unwrap();
        for event in events {
            assert_eq!(
                serde_json::to_value(output.recv().await.unwrap()).unwrap(),
                serde_json::to_value(event).unwrap()
            );
        }
        assert!(output.recv().await.is_none());
        let public = f.observer.0.lock().unwrap();
        assert_eq!(
            public.iter().map(|event| event.class).collect::<Vec<_>>(),
            vec![
                PublicActorEventClass::Lifecycle,
                PublicActorEventClass::Ephemeral,
                PublicActorEventClass::Snapshot,
                PublicActorEventClass::Semantic,
                PublicActorEventClass::Lifecycle
            ]
        );
        for (index, event) in public.iter().enumerate() {
            assert_eq!(event.actor_id, f.owner.fence.actor_id);
            assert_eq!(event.activation_id, f.owner.fence.activation_id);
            assert_eq!(event.attempt, f.owner.fence.attempt);
            assert_eq!(event.source_order.lease_epoch, f.owner.fence.lease_epoch);
            assert_eq!(event.source_order.sequence, index as u64 + 1);
            let serialized = serde_json::to_string(event).unwrap();
            for private in [
                "private-token",
                "private-error",
                "event-run",
                "local-event-host",
                "source_order",
                "lease_epoch",
            ] {
                assert!(!serialized.contains(private));
            }
        }
        assert_eq!(
            public
                .iter()
                .map(|event| &event.event_id)
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            public.len()
        );
    }

    #[tokio::test]
    async fn retired_owner_publishes_neither_payload_nor_lifecycle() {
        let f = Fixture::new().await;
        f.retire().await;
        let (input, rx) = mpsc::channel(1);
        input
            .send(AgentEvent::Token {
                content: "stale".into(),
            })
            .await
            .unwrap();
        drop(input);
        let (tx, mut output) = mpsc::channel(1);
        assert!(f.forward(rx, tx).await.unwrap().is_err());
        assert!(f.cancel.is_cancelled());
        assert!(output.recv().await.is_none());
        assert!(f.observer.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn backpressure_holds_no_authority_guard_and_rechecks_after_reservation() {
        let f = Fixture::new().await;
        let (input, rx) = mpsc::channel(1);
        input
            .send(AgentEvent::Token {
                content: "stale".into(),
            })
            .await
            .unwrap();
        drop(input);
        let (tx, mut output) = mpsc::channel(1);
        tx.send(AgentEvent::Token {
            content: "prefill".into(),
        })
        .await
        .unwrap();
        let task = f.forward(rx, tx);
        tokio::time::timeout(Duration::from_secs(5), async {
            while f.observer.0.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
            f.retire().await;
        })
        .await
        .expect("backpressure must not retain the writer guard");
        output.recv().await.unwrap();
        assert!(task.await.unwrap().is_err());
        assert!(output.recv().await.is_none());
        assert!(f.cancel.is_cancelled());
        assert_eq!(f.observer.0.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn closed_queue_cancels_without_publication() {
        let f = Fixture::new().await;
        let (input, rx) = mpsc::channel(1);
        drop(input);
        let (tx, output) = mpsc::channel(1);
        drop(output);
        assert!(f.forward(rx, tx).await.unwrap().is_err());
        assert!(f.cancel.is_cancelled());
        assert!(f.observer.0.lock().unwrap().is_empty());
    }
}
