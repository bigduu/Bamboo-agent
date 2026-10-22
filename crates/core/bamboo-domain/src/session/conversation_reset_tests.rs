//! Pure synthetic preparation. These are not storage/HTTP/provider executions.

use super::*;
use crate::{
    AgentRuntimeState, CompressionEvent, CompressionTriggerType, ConversationSummary,
    ModelContextResetReason, ModelContextState, PendingQuestion, PendingQuestionSource, ProjectId,
    ProviderFamily, ProviderProtocol, ProviderTranscriptAuthor, ProviderTranscriptItem,
    ProviderTranscriptOrigin, ProviderTranscriptResetReason, SessionAuthorityIdentity,
    SessionPermissionMode, SupervisorManagedLink, SupervisorManagementState, TaskItem, TaskList,
    DEFAULT_SUPERVISOR_SESSION_ID,
};
use serde_json::json;

const OLD: &str = "synthetic-old-private-payload";

fn source(supervisor: bool) -> Session {
    let mut session = Session::new(
        if supervisor {
            DEFAULT_SUPERVISOR_SESSION_ID
        } else {
            "reset-root"
        },
        "model",
    );
    session.title = "Retain title".into();
    session.pinned = true;
    session.set_project_id_meta("reset-project");
    session.set_workspace_path_meta("/synthetic/workspace");
    session.workspace = Some("/synthetic/workspace".into());
    session
        .metadata
        .insert("work.owner.reference".into(), "retained-work".into());
    session
        .metadata
        .insert("goal.objective".into(), "retained objective".into());
    session
        .metadata
        .insert("responses.previous_response_id".into(), OLD.into());
    session.metadata.insert(
        "runtime.plugin_hook_contexts".into(),
        json!([OLD]).to_string(),
    );
    session.add_message(crate::Message::system("retained system"));
    session.add_message(crate::Message::user(OLD));
    session.add_message(crate::Message::system("later system must be removed"));
    session.conversation_summary = Some(ConversationSummary::new(OLD, 2, 100));
    session.compression_events.push(CompressionEvent::new(
        2,
        1,
        80.0,
        40.0,
        10,
        CompressionTriggerType::Auto,
        0.5,
        None,
        0,
    ));
    session.prompt_snapshot = Some(
        serde_json::from_value(json!({
            "base_system_prompt": OLD, "effective_system_prompt": OLD,
        }))
        .unwrap(),
    );
    session.token_usage = Some(
        serde_json::from_value(json!({
            "system_tokens":1, "summary_tokens":2, "window_tokens":3,
            "total_tokens":6, "budget_limit":100, "truncation_occurred":false, "segments_removed":0,
        }))
        .unwrap(),
    );
    session.model_context_state = Some(ModelContextState {
        prefix_epoch: 7,
        state_revision: 9,
        cache_scope_sha256: Some("a".repeat(64)),
        transcript_item_sha256: vec!["b".repeat(64)],
        ..Default::default()
    });
    session.task_list = Some(TaskList {
        session_id: session.id.clone(),
        title: "Unfinished independent work".into(),
        items: vec![TaskItem {
            id: "retained-task".into(),
            description: "unfinished task".into(),
            ..Default::default()
        }],
        created_at: session.created_at,
        updated_at: session.created_at,
    });
    let mut runtime = AgentRuntimeState::new("retained-terminal-run");
    runtime.status = AgentStatusState::Completed;
    runtime.permission_mode = SessionPermissionMode::Auto;
    runtime.round.total_tool_calls = 17;
    runtime.hook_contexts = vec![OLD.into()];
    runtime.llm.responses_previous_id = Some(OLD.into());
    session.agent_runtime_state = Some(runtime);
    if supervisor {
        let incarnation = uuid::Uuid::new_v4();
        session.authority_identity = SessionAuthorityIdentity::Supervisor {
            incarnation_id: incarnation,
        };
        let project = ProjectId::parse("reset-project").unwrap();
        session.supervisor_management = Some(SupervisorManagementState {
            schema_version: 1,
            incarnation_id: incarnation,
            revision: 3,
            allowed_projects: [project.clone()].into(),
            links: [(
                "independent-worker".into(),
                SupervisorManagedLink {
                    revision: 2,
                    enabled: true,
                    target_created_at: session.created_at,
                    target_project_id: project,
                    target_metadata_version: 4,
                },
            )]
            .into(),
        });
    }
    // Deliberately anchor the native group to the retained System message.
    // Merely pruning anchors from the remaining history would leak its payload.
    session
        .activate_provider_transcript_route(
            ProviderFamily::Anthropic,
            ProviderProtocol::AnthropicMessages2023_06_01,
            &"c".repeat(64),
        )
        .unwrap();
    let items = [
        (ProviderTranscriptAuthor::Model, json!({"type":"text", "text":OLD})),
        (ProviderTranscriptAuthor::Model, json!({"type":"server_tool_use", "id":"search", "name":"tool_search_tool_regex", "input":{"pattern":"weather"}})),
        (ProviderTranscriptAuthor::ToolResult, json!({"type":"tool_search_tool_result", "tool_use_id":"search", "content":{"type":"tool_search_tool_search_result", "tool_references":[{"type":"tool_reference", "tool_name":"get_weather"}]}})),
    ].into_iter().map(|(author,payload)| ProviderTranscriptItem::try_from_payload(
        ProviderFamily::Anthropic, ProviderProtocol::AnthropicMessages2023_06_01,
        ProviderTranscriptOrigin::Provider, author, payload,
    ).unwrap()).collect();
    session
        .append_provider_transcript_group(
            session.messages[0].id.clone(),
            Some("synthetic-native"),
            items,
        )
        .unwrap();
    session
}

fn request(session: &Session, revision: u64) -> ConversationResetRequest {
    ConversationResetRequest {
        session_id: session.id.clone(),
        operation_id: format!("{revision}:{}", uuid::Uuid::new_v4()),
        expected_birth_token: session.root_mode_birth_token(),
        expected_conversation_revision: revision,
        expected_snapshot_sha256: conversation_reset_snapshot_sha256(session).unwrap(),
    }
}

fn prepare(
    session: &Session,
    state: &ConversationResetState,
    request: &ConversationResetRequest,
) -> ConversationResetPreparation {
    prepare_conversation_reset(
        session,
        state,
        request,
        ConversationResetReadSafety::IdleObserved,
        Utc::now(),
    )
    .unwrap()
}

fn prepared(session: &Session) -> Box<PreparedConversationReset> {
    let ConversationResetPreparation::Prepared(value) =
        prepare(session, &Default::default(), &request(session, 0))
    else {
        panic!("expected pure reset preparation");
    };
    value
}

#[test]
fn reset_preparation_purges_old_payload_and_preserves_identity_scope_and_unfinished_work() {
    for supervisor in [false, true] {
        let original = source(supervisor);
        let before = serde_json::to_value(&original).unwrap();
        let candidate = prepared(&original);
        let after = &candidate.proposed_session;
        assert_eq!(
            serde_json::to_value(&original).unwrap(),
            before,
            "preparation mutated source"
        );
        assert_eq!(after.id, original.id);
        assert_eq!(after.created_at, original.created_at);
        assert_eq!(after.authority_identity, original.authority_identity);
        assert_eq!(after.supervisor_management, original.supervisor_management);
        assert_eq!(after.project_id_meta(), original.project_id_meta());
        assert_eq!(after.workspace_path_meta(), original.workspace_path_meta());
        assert_eq!(after.workspace, original.workspace);
        assert_eq!(after.session_version, original.session_version);
        assert_eq!(
            after.root_mode_transition_epoch,
            original.root_mode_transition_epoch
        );
        assert_eq!(
            after.root_tool_authority_revision,
            original.root_tool_authority_revision
        );
        assert_eq!(after.title, original.title);
        assert_eq!(after.pinned, original.pinned);
        assert_eq!(
            serde_json::to_value(&after.task_list).unwrap(),
            serde_json::to_value(&original.task_list).unwrap()
        );
        assert_eq!(after.metadata["work.owner.reference"], "retained-work");
        assert_eq!(after.metadata["goal.objective"], "retained objective");
        let runtime = after.agent_runtime_state.as_ref().unwrap();
        assert_eq!(runtime.permission_mode, SessionPermissionMode::Auto);
        assert_eq!(
            runtime.round.total_tool_calls, 17,
            "reset cannot launder run budget"
        );
        assert_eq!(after.messages.len(), 1);
        assert_eq!(after.messages[0].id, original.messages[0].id);
        assert_eq!(after.messages[0].content, "retained system");
        assert!(after.token_usage.is_none() && after.conversation_summary.is_none());
        assert!(after.compression_events.is_empty() && after.prompt_snapshot.is_none());
        assert!(after.provider_transcript.groups().is_empty());
        assert!(after.provider_transcript.epoch() > original.provider_transcript.epoch());
        assert_eq!(
            after.provider_transcript.last_reset_reason(),
            Some(ProviderTranscriptResetReason::ExplicitHistoryRewrite)
        );
        let context = after.model_context_state.as_ref().unwrap();
        assert!(context.prefix_epoch > original.model_context_state.as_ref().unwrap().prefix_epoch);
        assert!(
            context.events.is_empty()
                && context.baselines.is_empty()
                && context.transcript_item_sha256.is_empty()
        );
        assert_eq!(
            context.last_reset_reason,
            Some(ModelContextResetReason::ExplicitHistoryRewrite)
        );
        assert!(
            !serde_json::to_string(after).unwrap().contains(OLD),
            "old fixture payload retained"
        );
        assert_eq!(candidate.proposed_state.conversation_revision, 1);
        assert_eq!(
            candidate.proposed_state.receipts[0].retained_system_messages,
            1
        );
    }
}

#[test]
fn reset_replay_recovers_one_exact_receipt_and_does_not_reset_newer_messages() {
    let original = source(true);
    let request = request(&original, 0);
    let ConversationResetPreparation::Prepared(first) =
        prepare(&original, &Default::default(), &request)
    else {
        panic!()
    };
    let mut continued = first.proposed_session.clone();
    continued.add_message(crate::Message::user("newer conversation must remain"));
    let before = serde_json::to_value(&continued).unwrap();
    let replay = prepare_conversation_reset(
        &continued,
        &first.proposed_state,
        &request,
        ConversationResetReadSafety::ActiveRun,
        Utc::now(),
    )
    .unwrap();
    assert!(
        matches!(replay,ConversationResetPreparation::AlreadyRecorded(ref receipt) if receipt == &first.proposed_state.receipts[0])
    );
    assert_eq!(serde_json::to_value(&continued).unwrap(), before);
    let mut changed = request.clone();
    changed.expected_snapshot_sha256 = "d".repeat(64);
    assert!(matches!(
        prepare(&continued, &first.proposed_state, &changed),
        ConversationResetPreparation::Rejected(ConversationResetRejection::OperationConflict)
    ));
}

#[test]
fn reset_preview_fences_new_messages_control_changes_birth_and_revision() {
    let original = source(false);
    let request = request(&original, 0);
    let mut changed = original.clone();
    changed.add_message(crate::Message::user("arrived after preview"));
    assert!(matches!(
        prepare(&changed, &Default::default(), &request),
        ConversationResetPreparation::Rejected(ConversationResetRejection::StaleObservation)
    ));
    let mut changed = original.clone();
    changed
        .metadata
        .insert("work.owner.reference".into(), "new reference".into());
    assert!(matches!(
        prepare(&changed, &Default::default(), &request),
        ConversationResetPreparation::Rejected(ConversationResetRejection::StaleObservation)
    ));
    let mut recreated = original.clone();
    recreated.created_at += chrono::Duration::seconds(1);
    assert!(matches!(
        prepare(&recreated, &Default::default(), &request),
        ConversationResetPreparation::Rejected(ConversationResetRejection::BirthMismatch)
    ));
    let mut future = request.clone();
    future.expected_conversation_revision = 1;
    future.operation_id = format!("1:{}", uuid::Uuid::new_v4());
    assert!(matches!(
        prepare(&original, &Default::default(), &future),
        ConversationResetPreparation::Rejected(ConversationResetRejection::StaleObservation)
    ));
}

#[test]
fn reset_refuses_external_unknown_live_and_pending_observations() {
    let source = source(true);
    let initial_request = request(&source, 0);
    for safety in [
        ConversationResetReadSafety::ActiveRun,
        ConversationResetReadSafety::PendingInput,
        ConversationResetReadSafety::UncertainOperation,
        ConversationResetReadSafety::AuthorityUnavailable,
    ] {
        assert!(matches!(
            prepare_conversation_reset(
                &source,
                &Default::default(),
                &initial_request,
                safety,
                Utc::now()
            )
            .unwrap(),
            ConversationResetPreparation::Rejected(_)
        ));
    }
    for key in [
        "permission.reexecute_tool_call_id",
        "permission.reexecute_request_generation",
        "permission.replay_save_recovery_required",
        "agent.runtime.state",
    ] {
        let mut pending = source.clone();
        pending
            .metadata
            .insert(key.into(), "synthetic marker".into());
        assert!(matches!(
            prepare(&pending, &Default::default(), &request(&pending, 0)),
            ConversationResetPreparation::Rejected(_)
        ));
    }
    let mut pending = source.clone();
    pending.pending_question = Some(PendingQuestion {
        tool_call_id: "call".into(),
        tool_name: "Bash".into(),
        question: "keep this question".into(),
        options: vec![],
        allow_custom: true,
        source: PendingQuestionSource::PauseTool,
    });
    assert!(matches!(
        prepare(&pending, &Default::default(), &request(&pending, 0)),
        ConversationResetPreparation::Rejected(ConversationResetRejection::PendingQuestion)
    ));
    let mut pending = source.clone();
    pending.agent_runtime_state.as_mut().unwrap().status = AgentStatusState::Running;
    assert!(matches!(
        prepare(&pending, &Default::default(), &request(&pending, 0)),
        ConversationResetPreparation::Rejected(ConversationResetRejection::ActiveRun)
    ));
    let mut pending = source.clone();
    pending.set_pending_injected_messages(vec![json!({"id":"accepted-unconsumed"})]);
    assert!(matches!(
        prepare(&pending, &Default::default(), &request(&pending, 0)),
        ConversationResetPreparation::Rejected(ConversationResetRejection::PendingInput)
    ));
    let mut pending = source.clone();
    pending
        .agent_runtime_state
        .as_mut()
        .unwrap()
        .waiting_for_children = Some(crate::WaitingForChildrenState::for_children(
        vec!["unfinished-child".into()],
        crate::ChildWaitPolicy::All,
        Utc::now(),
    ));
    assert!(matches!(
        prepare(&pending, &Default::default(), &request(&pending, 0)),
        ConversationResetPreparation::Rejected(ConversationResetRejection::PendingWait)
    ));
}

#[test]
fn reset_rejects_unsupported_schema_and_counter_exhaustion() {
    let mut source = source(false);
    source.session_version = Some(99);
    assert!(matches!(
        prepare(&source, &Default::default(), &request(&source, 0)),
        ConversationResetPreparation::Rejected(ConversationResetRejection::IncompatibleSession)
    ));
    source.session_version = Some(2);
    source.model_context_state.as_mut().unwrap().prefix_epoch = u64::MAX;
    assert!(matches!(
        prepare(&source, &Default::default(), &request(&source, 0)),
        ConversationResetPreparation::Rejected(ConversationResetRejection::CounterOverflow)
    ));
    source.model_context_state.as_mut().unwrap().prefix_epoch = 7;
    let future = ConversationResetState {
        schema_version: 99,
        ..Default::default()
    };
    assert!(matches!(
        prepare(&source, &future, &request(&source, 0)),
        ConversationResetPreparation::Rejected(ConversationResetRejection::InvalidLedger)
    ));
}

#[test]
fn reset_digest_is_independent_of_hashmap_insertion_order_and_child_is_rejected() {
    let original = source(false);
    let mut reordered = original.clone();
    let mut entries = original.metadata.iter().collect::<Vec<_>>();
    entries.sort_by(|a, b| b.0.cmp(a.0));
    reordered.metadata = entries
        .into_iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    assert_eq!(
        conversation_reset_snapshot_sha256(&original).unwrap(),
        conversation_reset_snapshot_sha256(&reordered).unwrap()
    );
    let child = Session::new_child_of("child", &original, "model", "child");
    assert!(matches!(
        prepare(&child, &Default::default(), &request(&child, 0)),
        ConversationResetPreparation::Rejected(ConversationResetRejection::NotRoot)
    ));
    let mut no_system = source(false);
    no_system
        .messages
        .retain(|message| message.role != Role::System);
    let candidate = prepared(&no_system);
    assert!(candidate.proposed_session.messages.is_empty());
    assert_eq!(
        candidate.proposed_state.receipts[0].retained_system_messages,
        0
    );
}

#[test]
fn reset_receipt_eviction_cannot_reexecute_an_old_operation() {
    let original = source(false);
    let oldest = request(&original, 0);
    let mut current = original;
    let mut state = ConversationResetState::default();
    for revision in 0..10 {
        let call = if revision == 0 {
            oldest.clone()
        } else {
            request(&current, revision)
        };
        let ConversationResetPreparation::Prepared(next) = prepare(&current, &state, &call) else {
            panic!()
        };
        current = next.proposed_session;
        state = next.proposed_state;
    }
    assert_eq!(state.receipts.len(), CONVERSATION_RESET_RECEIPT_LIMIT);
    assert!(matches!(
        prepare(&current, &state, &oldest),
        ConversationResetPreparation::Rejected(ConversationResetRejection::StaleObservation)
    ));
    let mut forged = state.clone();
    forged.receipts[0].birth_token = "0".repeat(64);
    assert!(matches!(
        prepare(&current, &forged, &request(&current, 10)),
        ConversationResetPreparation::Rejected(ConversationResetRejection::InvalidLedger)
    ));
}

#[test]
fn reset_request_has_no_force_stop_permission_or_role_argument() {
    let source = source(false);
    let mut raw = serde_json::to_value(request(&source, 0)).unwrap();
    for key in ["force", "stop", "supervisor", "permission_mode", "idle"] {
        raw[key] = json!(true);
        assert!(serde_json::from_value::<ConversationResetRequest>(raw.clone()).is_err());
        raw.as_object_mut().unwrap().remove(key);
    }
    let mut source = source;
    source.metadata.insert("role".into(), "supervisor".into());
    assert!(prepared(&source)
        .proposed_session
        .authority_identity
        .is_ordinary());
}
