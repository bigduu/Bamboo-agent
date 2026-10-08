use super::request::{optional_non_empty, resolve_model, resolve_session_id};
use super::sync_runtime_workspace;
use bamboo_agent_core::Session;

#[actix_web::test]
async fn ql_http_existing_queue_fit_or_overflow_keeps_prefix_events_and_pending_handoff() {
    use actix_web::{test, web};
    for overflow in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let state = web::Data::new(crate::AppState::new(home.path().into()).await.unwrap());
        let id = "ql-http-queued";
        let session = Session::new(id, "test-model");
        state.storage.save_session(&session).await.unwrap();
        let http = test::TestRequest::post()
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .to_http_request();
        let count = if overflow { 40 } else { 2 };
        for ordinal in 0..count {
            let request = serde_json::from_value::<super::ChatRequest>(serde_json::json!({
                "session_id":id,"message":"actual queued text","message_id":format!("ql-http-{ordinal}"),
                "workflow_selection":{"id":"exact-request","source":"user","revision":7,
                    "args":{"payload":"x".repeat(if overflow { 7000 } else { 9 })}}
            })).unwrap();
            super::ingress::queue(&state, &session, &request, "actual queued text", &http)
                .await
                .unwrap()
                .unwrap();
        }
        let mut latest = state.storage.load_session(id).await.unwrap().unwrap();
        latest.metadata.insert(
            "chat.queued_ingress.v1".into(),
            format!("ql-http-{}", count - 1),
        );
        state.storage.save_session(&latest).await.unwrap();
        let mut feed = state.account_sink.subscribe();
        let carrier = super::ingress::admit_for_execute(&state, id).await.unwrap();
        assert_eq!(
            carrier.is_none(),
            overflow,
            "optional overflow never becomes an admission error"
        );
        if let Some(carrier) = carrier {
            assert_eq!(carrier.observations().len(), 2);
            assert_eq!(carrier.observations()[0].input_id(), "ql-http-0");
            assert_eq!(carrier.observations()[1].input_id(), "ql-http-1");
            assert_eq!(
                carrier.observations()[0].request().unwrap().selections[0].revision,
                7
            );
        }
        let mut ids = Vec::new();
        for _ in 0..count {
            let change = tokio::time::timeout(std::time::Duration::from_secs(10), feed.recv())
                .await
                .expect("every committed prefix event")
                .unwrap();
            if let bamboo_agent_core::AgentEvent::MessageAppended { message_id, .. } = &change.event
            {
                ids.push(message_id.clone());
            }
        }
        assert_eq!(
            ids,
            (0..count)
                .map(|i| format!("ql-http-{i}"))
                .collect::<Vec<_>>()
        );
        let cold = state.storage.load_session(id).await.unwrap().unwrap();
        assert!(!cold.metadata.contains_key("chat.queued_ingress.v1"));
        assert_eq!(
            cold.messages
                .iter()
                .filter(|m| m.id.starts_with("ql-http-"))
                .count(),
            count
        );
        assert_eq!(state.session_inbox.inspect(id).await.unwrap().pending, 0);
        for ordinal in 0..count {
            assert!(state
                .session_inbox
                .was_admitted(
                    id,
                    &bamboo_domain::SessionMessageId::parse(format!("ql-http-{ordinal}")).unwrap()
                )
                .await
                .unwrap());
        }
        assert!(
            super::ingress::admit_for_execute(&state, id)
                .await
                .unwrap()
                .is_none(),
            "recovery/history cannot remint current data"
        );
    }
}

use bamboo_engine::session_app::chat::{
    clear_skill_runtime_state, resolve_base_prompt, resolve_enhance_prompt,
    resolve_selected_skill_ids, resolve_workspace_path,
};

#[actix_web::test]
async fn partial_queued_ingress_emits_every_committed_message_before_retry() {
    use actix_web::{test, web};
    use bamboo_agent_core::AgentEvent;
    use std::collections::BTreeSet;
    let home = tempfile::tempdir().unwrap();
    let state = web::Data::new(
        crate::AppState::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let request = |message_id: Option<String>| {
        serde_json::from_value::<super::ChatRequest>(serde_json::json!({
        "session_id":"partial-batch-root", "message":"bounded inbox input", "message_id":message_id,
        "model":"test-model"
    })).unwrap()
    };
    let initial = super::handler(
        state.clone(),
        test::TestRequest::post().to_http_request(),
        web::Json(request(None)),
    )
    .await;
    assert_eq!(initial.status(), actix_web::http::StatusCode::CREATED);
    let first: serde_json::Value = serde_json::from_slice(
        &actix_web::body::to_bytes(initial.into_body())
            .await
            .unwrap(),
    )
    .unwrap();
    let bootstrap = super::ingress::admit_for_execute(&state, "partial-batch-root")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bootstrap.observations().len(), 1);
    assert_eq!(
        bootstrap.observations()[0].input_id(),
        first["message_id"].as_str().unwrap()
    );
    drop(bootstrap);
    assert_eq!(
        state
            .session_inbox
            .inspect("partial-batch-root")
            .await
            .unwrap()
            .pending,
        0
    );
    for index in 0..129 {
        let response = super::handler(
            state.clone(),
            test::TestRequest::post()
                .peer_addr("127.0.0.1:5700".parse().unwrap())
                .to_http_request(),
            web::Json(request(Some(format!("partial-input-{index}")))),
        )
        .await;
        assert_eq!(response.status(), actix_web::http::StatusCode::CREATED);
    }
    let mut feed = state.account_sink.subscribe();
    let response = super::ingress::admit_for_execute(&state, "partial-batch-root")
        .await
        .unwrap_err();
    assert_eq!(
        response.status(),
        actix_web::http::StatusCode::SERVICE_UNAVAILABLE
    );
    let history = state
        .storage
        .load_session("partial-batch-root")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        history
            .messages
            .iter()
            .filter(|m| m.id.starts_with("partial-input-"))
            .count(),
        128
    );
    assert_eq!(
        state
            .session_inbox
            .inspect("partial-batch-root")
            .await
            .unwrap()
            .pending,
        1
    );
    let mut seen = BTreeSet::new();
    for _ in 0..128 {
        let change = tokio::time::timeout(std::time::Duration::from_secs(1), feed.recv())
            .await
            .expect("committed first-batch event")
            .unwrap();
        if let AgentEvent::MessageAppended { message_id, .. } = &change.event {
            assert!(seen.insert(message_id.clone()), "duplicate event");
        } else {
            panic!("expected committed MessageAppended event");
        }
    }
    super::ingress::admit_for_execute(&state, "partial-batch-root")
        .await
        .unwrap();
    let change = tokio::time::timeout(std::time::Duration::from_secs(1), feed.recv())
        .await
        .unwrap()
        .unwrap();
    let AgentEvent::MessageAppended { message_id, .. } = &change.event else {
        panic!("tail event");
    };
    assert!(seen.insert(message_id.clone()));
    assert_eq!(
        seen,
        (0..129).map(|i| format!("partial-input-{i}")).collect()
    );
    assert!(matches!(
        feed.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    let history = state
        .storage
        .load_session("partial-batch-root")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        history
            .messages
            .iter()
            .filter(|m| m.id.starts_with("partial-input-"))
            .count(),
        129
    );
    assert!(!history.metadata.contains_key("chat.queued_ingress.v1"));
    let replay =
        bamboo_engine::events::journal::read_since(state.account_sink.events_dir(), 0).unwrap();
    assert_eq!(replay.iter().filter(|change| matches!(&change.event, AgentEvent::MessageAppended { message_id, .. } if message_id.starts_with("partial-input-"))).count(), 129);
}

#[actix_web::test]
async fn ticket_review_queued_ingress_does_not_requeue_activated_root_history() {
    use actix_web::{test, web};
    use bamboo_engine::execution::{reserve_session_execution, SessionExecutionReserveOutcome};
    let home = tempfile::tempdir().unwrap();
    let state = web::Data::new(
        crate::AppState::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let request = |message: &str, message_id: Option<&str>| {
        serde_json::from_value::<super::ChatRequest>(serde_json::json!({
            "session_id":"queued-owned-root", "message":message, "message_id":message_id,
            "model":"test-model"
        }))
        .unwrap()
    };
    let first = super::handler(
        state.clone(),
        test::TestRequest::post().to_http_request(),
        web::Json(request("old canonical Human turn", None)),
    )
    .await;
    assert_eq!(first.status(), actix_web::http::StatusCode::CREATED);
    let session = state
        .storage
        .load_session("queued-owned-root")
        .await
        .unwrap()
        .unwrap();
    let before_messages = serde_json::to_value(&session.messages).unwrap();
    let sender = state.get_session_event_sender(&session.id).await;
    let mut reservation = match reserve_session_execution(
        &state.agent,
        &state.agent_runners,
        &state.session_event_senders,
        &session.id,
        &sender,
    )
    .await
    {
        SessionExecutionReserveOutcome::Reserved(reservation) => reservation,
        _ => panic!("fixture Root must be idle"),
    };
    reservation
        .bind_root_actor(&state.agent, &session)
        .await
        .unwrap();
    assert!(state
        .session_store
        .root_actor_input_required(&session)
        .await
        .unwrap());
    for _ in 0..2 {
        let response = super::handler(
            state.clone(),
            test::TestRequest::post()
                .peer_addr("127.0.0.1:5700".parse().unwrap())
                .to_http_request(),
            web::Json(request("new exact Human turn", Some("new-owned-input"))),
        )
        .await;
        let status = response.status();
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .unwrap();
        assert_eq!(
            status,
            actix_web::http::StatusCode::CREATED,
            "{}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap()["message_id"],
            "new-owned-input"
        );
    }
    let mut queued = state
        .storage
        .load_session(&session.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(&queued.messages).unwrap(),
        before_messages
    );
    assert_eq!(
        queued
            .metadata
            .get("chat.queued_ingress.v1")
            .map(String::as_str),
        Some("new-owned-input")
    );
    assert_eq!(
        state
            .session_inbox
            .inspect(&session.id)
            .await
            .unwrap()
            .pending,
        1
    );
    let admission = reservation
        .execution_persistence()
        .unwrap()
        .admit_root_inbox(
            &mut queued,
            state.session_inbox.clone(),
            Some(reservation.run_id()),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(admission.admission_error.is_none());
    assert_eq!(admission.merged, 1);
    assert_eq!(admission.committed_messages[0].id, "new-owned-input");
    assert_eq!(
        admission.committed_messages[0].content,
        "new exact Human turn"
    );
    assert_eq!(
        state
            .session_inbox
            .inspect(&session.id)
            .await
            .unwrap()
            .pending,
        0
    );
}

#[actix_web::test]
async fn activated_root_chat_preserves_handoff_and_commits_multimodal_input_once() {
    use actix_web::{test, web};
    use bamboo_engine::execution::{reserve_session_execution, SessionExecutionReserveOutcome};
    let home = tempfile::tempdir().unwrap();
    let state = web::Data::new(
        crate::AppState::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let request = || {
        serde_json::from_value::<super::ChatRequest>(serde_json::json!({
            "session_id": "owned-chat-image", "message": "first", "model": "test-model",
        }))
        .unwrap()
    };
    let first = super::handler(
        state.clone(),
        test::TestRequest::post().to_http_request(),
        web::Json(request()),
    )
    .await;
    assert_eq!(first.status(), actix_web::http::StatusCode::CREATED);
    let mut session = state
        .storage
        .load_session("owned-chat-image")
        .await
        .unwrap()
        .unwrap();
    session.set_last_run_status("error");
    session.set_last_run_error("previous execution");
    state.save_and_cache_session(&mut session).await;
    let metadata = session.metadata.clone();
    let before_messages = session.messages.clone();
    let sender = state.get_session_event_sender(&session.id).await;
    let mut reservation = match reserve_session_execution(
        &state.agent,
        &state.agent_runners,
        &state.session_event_senders,
        &session.id,
        &sender,
    )
    .await
    {
        SessionExecutionReserveOutcome::Reserved(reservation) => reservation,
        SessionExecutionReserveOutcome::AlreadyRunning { .. } => {
            panic!("fixture must reserve its Root")
        }
    };
    reservation
        .bind_root_actor(&state.agent, &session)
        .await
        .unwrap();
    let image_request = || {
        serde_json::from_value::<super::ChatRequest>(serde_json::json!({
        "session_id": session.id, "message": "second with image", "model": "test-model",
        "system_prompt": "ROOT_PROMPT_REPLACED", "enhance_prompt": "ROOT_ENHANCE_ADDED",
        "images": [{"base64":"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a8aUAAAAASUVORK5CYII=", "type":"image/png"}],
    })).unwrap()
    };
    for _ in 0..2 {
        let response = super::handler(
            state.clone(),
            test::TestRequest::post()
                .insert_header(("Idempotency-Key", "owned-image-turn"))
                .to_http_request(),
            web::Json(image_request()),
        )
        .await;
        let status = response.status();
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .unwrap();
        assert_eq!(
            status,
            actix_web::http::StatusCode::CREATED,
            "{}",
            String::from_utf8_lossy(&body)
        );
    }
    let queued = state
        .storage
        .load_session(&session.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(&queued.messages).unwrap(),
        serde_json::to_value(&before_messages).unwrap(),
        "ingress must not rewrite protected Main"
    );
    for key in [
        "last_run_status",
        "last_run_error",
        "execute.pending_turn_message_id",
        "execute.startup_handoff_at",
    ] {
        assert_eq!(
            queued.metadata.get(key),
            metadata.get(key),
            "queued input must preserve {key}"
        );
    }
    assert_eq!(queued.last_run_status().as_deref(), Some("error"));
    assert_eq!(
        queued.last_run_error().as_deref(),
        Some("previous execution")
    );
    assert_eq!(
        state
            .session_inbox
            .inspect(&session.id)
            .await
            .unwrap()
            .pending,
        1
    );
    let persistence = reservation
        .execution_persistence()
        .expect("actual Root writer");
    let mut consumed = queued;
    let admission = persistence
        .admit_root_inbox(
            &mut consumed,
            state.session_inbox.clone(),
            Some(reservation.run_id()),
        )
        .await
        .unwrap()
        .expect("owned Root consumer");
    assert!(admission.admission_error.is_none());
    assert_eq!(admission.merged, 1);
    let system_prompts: Vec<_> = consumed
        .messages
        .iter()
        .filter(|message| message.role == bamboo_domain::Role::System)
        .collect();
    assert_eq!(system_prompts.len(), 1);
    assert!(system_prompts[0].content.contains("ROOT_PROMPT_REPLACED"));
    assert!(system_prompts[0].content.contains("ROOT_ENHANCE_ADDED"));
    let message = admission.committed_messages.first().unwrap();
    assert_eq!(message.content, "second with image");
    let parts = message
        .content_parts
        .as_ref()
        .expect("multimodal typed input");
    assert_eq!(parts.len(), 2);
    assert!(
        matches!(&parts[0], bamboo_domain::MessagePart::Text { text } if text == "second with image")
    );
    assert!(
        matches!(&parts[1], bamboo_domain::MessagePart::ImageUrl { image_url } if image_url.url.starts_with("bamboo-attachment://owned-chat-image/"))
    );
    let id = bamboo_domain::SessionMessageId::parse(&message.id).unwrap();
    assert!(consumed.session_inbox_admission().unwrap().contains(&id));
    assert_eq!(
        state
            .session_inbox
            .inspect(&session.id)
            .await
            .unwrap()
            .pending,
        0
    );
    let durable = state
        .storage
        .load_session(&session.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        durable
            .messages
            .iter()
            .filter(|m| m.id == message.id)
            .count(),
        1
    );
    assert_eq!(
        serde_json::to_value(
            &bamboo_engine::read_cached_session(&state.sessions, &session.id)
                .unwrap()
                .messages
        )
        .unwrap(),
        serde_json::to_value(&durable.messages).unwrap()
    );
    reservation.abandon().await;
}

#[actix_web::test]
async fn typed_workflow_candidate_is_pinned_exactly_and_stale_revision_fails_closed() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
    let state = crate::AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state");
    let catalog = state.skill_manager.store().skill_catalog_snapshot().await;
    let review = catalog
        .entries
        .iter()
        .find(|entry| entry.id == "review" && entry.winner)
        .expect("builtin review catalog entry");
    let selection = bamboo_skills::WorkflowSelection {
        id: review.id.clone(),
        source: review.source,
        revision: review.revision,
        args: serde_json::json!({}),
    };
    let mut session = Session::new("typed-review", "model");
    let staging_id = super::pin_explicit_workflow_candidate(
        &state,
        &mut session,
        &selection,
        &std::collections::BTreeSet::new(),
    )
    .await
    .expect("pin exact typed Workflow");

    let snapshot: bamboo_skills::SkillActivationSnapshot = serde_json::from_str(
        session
            .metadata
            .get(bamboo_skills::runtime_metadata::SKILL_RUNTIME_PINNED_SNAPSHOT_KEY)
            .expect("durable pre-execute snapshot"),
    )
    .expect("snapshot contract");
    assert_eq!(snapshot.skills.len(), 1);
    assert_eq!(snapshot.skills["review"].revision, review.revision);
    assert_eq!(
        session
            .metadata
            .get(bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTION_SOURCE_KEY)
            .map(String::as_str),
        Some("explicit")
    );
    let request_identity = serde_json::to_string(&selection).expect("selection JSON");
    assert!(!request_identity.contains(&review.description));
    assert!(!request_identity.contains("prompt"));

    state
        .skill_manager
        .release_activation_for_workspace(&staging_id, None)
        .await
        .expect("release exact candidate");

    let existing_ids = ["plan".to_string()];
    let existing = state
        .skill_manager
        .resolve_and_pin_activation_for_request_with_mode_and_budget(
            "typed-review-stale",
            &std::collections::BTreeSet::new(),
            Some(&existing_ids),
            None,
            None,
            bamboo_skills::DEFAULT_WORKFLOW_CATALOG_CONTEXT_TOKENS,
        )
        .await
        .expect("existing live activation");
    let mut stale_session = Session::new("typed-review-stale", "model");
    let stale = bamboo_skills::WorkflowSelection {
        revision: review.revision + 1,
        ..selection
    };
    let response = super::pin_explicit_workflow_candidate(
        &state,
        &mut stale_session,
        &stale,
        &std::collections::BTreeSet::new(),
    )
    .await
    .expect_err("stale revision must fail before chat persistence");
    assert_eq!(response.status(), actix_web::http::StatusCode::CONFLICT);
    assert!(!stale_session
        .metadata
        .contains_key(bamboo_skills::runtime_metadata::SKILL_RUNTIME_PINNED_SNAPSHOT_KEY));
    assert!(stale_session.messages.is_empty());
    let retained = state
        .skill_manager
        .pinned_activation_for_workspace("typed-review-stale", None)
        .await
        .expect("inspect existing activation")
        .expect("failed request must retain existing activation");
    assert_eq!(
        retained.descriptor.skill_revisions,
        existing.descriptor.skill_revisions
    );
    assert_eq!(retained.skills[0].id, "plan");
}

#[actix_web::test]
async fn typed_workflow_capacity_failure_is_sanitized_and_retryable() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
    let state = crate::AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state");
    let catalog = state.skill_manager.store().skill_catalog_snapshot().await;
    let review = catalog
        .entries
        .iter()
        .find(|entry| entry.id == "review" && entry.winner)
        .expect("builtin review");
    for index in 0..256 {
        state
            .skill_manager
            .pin_current_activation_for_workspace(
                &format!("capacity-{index}"),
                None,
                &["plan".to_string()],
                None,
            )
            .await
            .expect("fill activation capacity");
    }
    let selection = bamboo_skills::WorkflowSelection {
        id: review.id.clone(),
        source: review.source,
        revision: review.revision,
        args: serde_json::json!({}),
    };
    let mut session = Session::new("capacity-overflow", "model");
    let response = super::pin_explicit_workflow_candidate(
        &state,
        &mut session,
        &selection,
        &std::collections::BTreeSet::new(),
    )
    .await
    .expect_err("capacity exhaustion must fail closed");
    assert_eq!(
        response.status(),
        actix_web::http::StatusCode::SERVICE_UNAVAILABLE
    );
    let body = actix_web::body::to_bytes(response.into_body())
        .await
        .expect("response body");
    let body: serde_json::Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(body["error"]["code"], "workflow_snapshot_unavailable");
    assert_eq!(
        body["error"]["message"],
        "Workflow catalog is temporarily unavailable; retry the request"
    );
    let rendered = body.to_string();
    assert!(!rendered.contains("capacity"));
    assert!(!rendered.contains(temp_dir.path().to_string_lossy().as_ref()));
    assert!(session.metadata.is_empty());
    assert!(session.messages.is_empty());
}

/// Regression: `/goal off` and `/goal clear` must clear the stale runtime
/// `goal.state` (status / continuation budget / double-check eval history).
/// Previously the cleanup was gated behind `should_resume`, so only
/// `/goal <prompt>` (set-prompt) reached it and off/clear left it behind —
/// surfacing a stale "complete" badge over the history API.
#[actix_web::test]
async fn goal_off_and_clear_remove_stale_goal_state() {
    use crate::AppState;
    use bamboo_engine::session_app::chat::GoalCommand;
    use tempfile::tempdir;

    const STALE_GOAL_STATE: &str = r#"{"objective":"ship it","status":"complete","continuation_count":2,"eval_history":[{"checkpoint":"terminal","iteration":3,"decision":"achieved","confidence":"high","reasoning":"done","recorded_at":"t"}],"created_at":"t","updated_at":"t"}"#;

    let temp_dir = tempdir().expect("tempdir");
    bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
    let state = AppState::new(temp_dir.path().to_path_buf())
        .await
        .expect("app state");

    for (session_id, cmd) in [
        ("goal-off-test", GoalCommand::Off),
        ("goal-clear-test", GoalCommand::Clear),
    ] {
        // Seed a session carrying a stale, finished goal.state + a spread of
        // stale `gold.*` runtime snapshot keys (incl. ones that were NOT on the
        // old explicit removal list), plus the config key which must survive.
        let mut session = Session::new(session_id, "model");
        session
            .metadata
            .insert("goal.state".to_string(), STALE_GOAL_STATE.to_string());
        for (k, v) in [
            ("gold.evaluation_count", "7"),
            ("gold.last_reasoning", "old reasoning"),
            ("gold.last_checkpoint", "terminal"),
            ("gold.last_iteration", "7"),
            ("gold.last_decision", "achieved"),
        ] {
            session.metadata.insert(k.to_string(), v.to_string());
        }
        state.save_and_cache_session(&mut session).await;

        let _ = super::handle_goal_command(&state, session_id, &cmd).await;

        let reloaded = state
            .storage
            .load_session(session_id)
            .await
            .expect("load")
            .expect("session exists");
        assert!(
            !reloaded.metadata.contains_key("goal.state"),
            "goal.state must be cleared after /goal {cmd:?}"
        );
        assert!(
            !reloaded.metadata.keys().any(|k| k.starts_with("gold.")),
            "no gold.* runtime keys may remain after /goal {cmd:?}"
        );
        // The config (key `gold_config`, no dot) is managed by the handler and
        // must still be present — the prefix wipe must not remove it.
        assert!(
            reloaded.metadata.contains_key("gold_config"),
            "gold_config must be preserved after /goal {cmd:?}"
        );
    }
}

#[test]
fn resolve_model_errors_when_neither_request_nor_default_resolve() {
    let response = resolve_model(Some("   "), None).expect_err("no model should be required error");
    assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
}

#[test]
fn resolve_model_trims_whitespace_from_request_model() {
    let model = resolve_model(Some("  gpt-5  "), None).expect("model should be accepted");
    assert_eq!(model, "gpt-5");
}

/// #480: an absent/blank request model falls back to the server's resolved
/// default rather than erroring, as long as a default is available.
#[test]
fn resolve_model_falls_back_to_default_when_request_model_absent() {
    let model = resolve_model(None, Some("gpt-default")).expect("default should be used");
    assert_eq!(model, "gpt-default");
}

#[test]
fn resolve_model_falls_back_to_default_when_request_model_blank() {
    let model = resolve_model(Some("   "), Some("gpt-default")).expect("default should be used");
    assert_eq!(model, "gpt-default");
}

#[test]
fn resolve_model_prefers_explicit_request_model_over_default() {
    let model =
        resolve_model(Some("gpt-explicit"), Some("gpt-default")).expect("request model wins");
    assert_eq!(model, "gpt-explicit");
}

#[test]
fn optional_non_empty_returns_none_for_blank_string() {
    let value = optional_non_empty(Some("   "));
    assert_eq!(value, None);
}

#[test]
fn resolve_session_id_uses_provided_value_without_trimming() {
    let session_id = resolve_session_id(Some("  existing-id  "));
    assert_eq!(session_id, "  existing-id  ");
}

#[test]
fn resolve_base_prompt_prefers_request_and_persists_metadata() {
    let mut session = Session::new("session-1", "model");
    let base_prompt = resolve_base_prompt(&mut session, Some("request prompt"), "", "fallback");
    assert_eq!(base_prompt, "request prompt");
    assert_eq!(
        session
            .metadata
            .get("base_system_prompt")
            .map(String::as_str),
        Some("request prompt")
    );
}

#[test]
fn resolve_base_prompt_falls_back_to_existing_metadata() {
    let mut session = Session::new("session-1", "model");
    session.metadata.insert(
        "base_system_prompt".to_string(),
        "stored prompt".to_string(),
    );

    let base_prompt = resolve_base_prompt(&mut session, None, "", "fallback");
    assert_eq!(base_prompt, "stored prompt");
}

#[test]
fn resolve_base_prompt_falls_back_to_existing_system_message_before_global_default() {
    let mut session = Session::new("session-1", "model");
    session.add_message(bamboo_agent_core::Message::system("Existing system"));

    let base_prompt = resolve_base_prompt(&mut session, None, "", "global default");
    assert_eq!(base_prompt, "Existing system");
    assert_eq!(
        session
            .metadata
            .get("base_system_prompt")
            .map(String::as_str),
        Some("Existing system")
    );
}

#[test]
fn resolve_base_prompt_uses_global_default_when_missing_everywhere() {
    let mut session = Session::new("session-1", "model");
    let base_prompt = resolve_base_prompt(&mut session, None, "", "global default");
    assert_eq!(base_prompt, "global default");
    assert_eq!(
        session
            .metadata
            .get("base_system_prompt")
            .map(String::as_str),
        Some("global default")
    );
}

#[test]
fn resolve_workspace_path_uses_request_then_metadata() {
    let mut session = Session::new("session-1", "model");

    let from_request = resolve_workspace_path(&mut session, Some("/tmp/workspace"), None);
    assert_eq!(from_request.as_deref(), Some("/tmp/workspace"));
    assert_eq!(
        session.metadata.get("workspace_path").map(String::as_str),
        Some("/tmp/workspace")
    );

    let from_metadata = resolve_workspace_path(&mut session, None, None);
    assert_eq!(from_metadata.as_deref(), Some("/tmp/workspace"));
}

// NOTE: the default-work-area disk fallback used to be tested here, but it's now
// gated on NO workspace provider being registered (#38/#131) — and the provider
// is a process-global first-wins OnceLock that sibling AppState tests populate,
// so a fallback assertion can't be deterministic in the server test binary. The
// disk fallback is unit-tested directly + deterministically in bamboo-engine's
// session_app::chat (default_workspace_from_data_dir_reads_configured_work_area).

#[actix_web::test]
async fn sync_runtime_workspace_materializes_with_the_states_provider() {
    let app_home = tempfile::tempdir().expect("app home");
    let state = crate::AppState::new(app_home.path().to_path_buf())
        .await
        .expect("app state");
    let root = bamboo_config::paths::resolve_workspace_root_in(app_home.path());
    let workspace = root.join("session-runtime-workspace");
    let session_id = "session-runtime-workspace";
    assert!(!workspace.exists());

    sync_runtime_workspace(
        &state,
        session_id,
        Some(workspace.to_string_lossy().as_ref()),
        "session_fallback",
    );

    let resolved = bamboo_tools::tools::workspace_state::get_workspace(session_id)
        .expect("workspace should be stored");
    assert_eq!(resolved, workspace);
    assert!(
        resolved.is_dir(),
        "instance root should materialize fallback"
    );
}

#[test]
fn resolve_enhance_prompt_stores_and_clears_metadata() {
    let mut session = Session::new("session-1", "model");

    resolve_enhance_prompt(&mut session, Some("Extra guidance"));
    assert_eq!(
        session.metadata.get("enhance_prompt").map(String::as_str),
        Some("Extra guidance")
    );

    resolve_enhance_prompt(&mut session, None);
    assert!(!session.metadata.contains_key("enhance_prompt"));
}

#[test]
fn resolve_selected_skill_ids_prefers_structured_request_and_persists_as_json() {
    let mut session = Session::new("session-1", "model");
    resolve_selected_skill_ids(
        &mut session,
        Some(&[
            "pdf".to_string(),
            "skill-creator".to_string(),
            "pdf".to_string(),
        ]),
        "hello",
    );

    let stored = session
        .metadata
        .get("selected_skill_ids")
        .map(String::as_str);
    assert_eq!(stored, Some("[\"pdf\",\"skill-creator\"]"));
}

#[test]
fn resolve_selected_skill_ids_falls_back_to_legacy_hint_when_structured_field_absent() {
    let mut session = Session::new("session-1", "model");
    resolve_selected_skill_ids(
        &mut session,
        None,
        "[User explicitly selected skill: PDF Skill (ID: pdf)]\n\nPlease parse this file",
    );

    let stored = session
        .metadata
        .get("selected_skill_ids")
        .map(String::as_str);
    assert_eq!(stored, Some("[\"pdf\"]"));
}

#[test]
fn resolve_selected_skill_ids_clears_stale_metadata_when_no_selection_provided() {
    let mut session = Session::new("session-1", "model");
    session
        .metadata
        .insert("selected_skill_ids".to_string(), "[\"pdf\"]".to_string());

    resolve_selected_skill_ids(&mut session, None, "normal prompt");
    assert!(!session.metadata.contains_key("selected_skill_ids"));
}

#[test]
fn clear_skill_runtime_state_removes_loaded_skill_markers() {
    let mut session = Session::new("session-1", "model");
    session.metadata.insert(
        "skill_runtime_loaded_skill_ids".to_string(),
        r#"["demo"]"#.to_string(),
    );
    session.metadata.insert(
        "skill_runtime_last_loaded_skill_id".to_string(),
        "demo".to_string(),
    );

    clear_skill_runtime_state(&mut session);

    assert!(!session
        .metadata
        .contains_key("skill_runtime_loaded_skill_ids"));
    assert!(!session
        .metadata
        .contains_key("skill_runtime_last_loaded_skill_id"));
}

// ---- #480: POST /chat with an omitted `model` end-to-end ----

mod optional_model_e2e {
    use actix_web::{http::StatusCode, test, web, App};
    use async_trait::async_trait;
    use bamboo_agent_core::{AgentEvent, Session};
    use bamboo_llm::{
        LLMChunk, LLMError, LLMProvider, LLMRequestOptions, LLMStream, ProviderModelRouter,
        ProviderRegistry,
    };
    use serde_json::Value;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;
    use tokio::sync::Semaphore;

    use crate::routes::configure_routes;
    use crate::AppState;

    const CONCURRENCY_ASSERT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

    async fn new_state() -> web::Data<AppState> {
        let temp_dir = tempdir().expect("tempdir").keep();
        bamboo_config::paths::init_bamboo_dir(temp_dir.clone());
        web::Data::new(AppState::new(temp_dir).await.expect("app state"))
    }

    #[actix_web::test]
    async fn ultra_first_chat_is_independent_and_cannot_change_existing_or_child_authority() {
        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        for (id, selector, expected) in [
            (
                "ultra-first",
                serde_json::json!({"thinking_mode": "ultra"}),
                "ultra",
            ),
            (
                "standard-first",
                serde_json::json!({"thinking_mode": "standard"}),
                "standard",
            ),
            ("ordinary-first", serde_json::json!({}), "standard"),
            (
                "legacy-ultra-first",
                serde_json::json!({"root_orchestration_only": true}),
                "ultra",
            ),
        ] {
            let mut body = selector;
            body["session_id"] = id.into();
            body["message"] = "Preserve required constraints while coordinating".into();
            body["model"] = "test-model".into();
            body["reasoning_effort"] = "max".into();
            let response = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri("/api/v1/chat")
                    .set_json(&body)
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::CREATED, "{id}");
            let detail: Value = test::call_and_read_body_json(
                &app,
                test::TestRequest::get()
                    .uri(&format!("/api/v1/sessions/{id}"))
                    .to_request(),
            )
            .await;
            assert_eq!(detail["session"]["thinking_mode"], expected);
            assert_eq!(detail["session"]["reasoning_effort"], "max");
        }
        let root_before = state
            .storage
            .load_session("ultra-first")
            .await
            .unwrap()
            .unwrap();
        for mode in ["standard", "ultra"] {
            let response = test::call_service(&app, test::TestRequest::post().uri("/api/v1/chat").set_json(serde_json::json!({"session_id":"ultra-first", "message":"must not admit", "model":"test-model", "thinking_mode":mode})).to_request()).await;
            assert_eq!(response.status(), StatusCode::PRECONDITION_REQUIRED);
            let body: Value = test::read_body_json(response).await;
            assert_eq!(body["error"]["code"], "root_mode_operation_required");
        }
        let root_after = state
            .storage
            .load_session("ultra-first")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(root_after.messages.len(), root_before.messages.len());
        assert_eq!(
            root_after.root_tool_authority_revision,
            root_before.root_tool_authority_revision
        );
        for extra in [
            serde_json::json!({"thinking_mode":"ultra", "root_orchestration_only":false}),
            serde_json::json!({"thinking_mode":"max"}),
            serde_json::json!({"reasoning_effort":"ultra"}),
            serde_json::json!({"thinking_mode":null}),
        ] {
            let mut body = extra;
            body["session_id"] = "invalid-ultra-first".into();
            body["message"] = "must not create".into();
            body["model"] = "test-model".into();
            let response = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri("/api/v1/chat")
                    .set_json(&body)
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert!(state
                .storage
                .load_session("invalid-ultra-first")
                .await
                .unwrap()
                .is_none());
        }
        let mut child =
            Session::new_child_of("ultra-first-child", &root_after, "test-model", "child");
        state.save_and_cache_session(&mut child).await;
        let response = test::call_service(&app, test::TestRequest::post().uri("/api/v1/chat").set_json(serde_json::json!({"session_id":child.id, "message":"must not enable", "model":"test-model", "thinking_mode":"ultra"})).to_request()).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body: Value = test::read_body_json(response).await;
        assert_eq!(body["error"]["code"], "root_orchestration_requires_root");
        let detail: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get()
                .uri(&format!("/api/v1/sessions/{}", child.id))
                .to_request(),
        )
        .await;
        assert_eq!(detail["session"]["thinking_mode"], "standard");
    }

    #[actix_web::test]
    async fn root_tool_mode_chat_create_resume_conflict_disable_and_detail_are_durable() {
        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let id = "root-tool-chat-selection";
        let create = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": id,
                    "message": "coordinate the task",
                    "model": "test-model",
                    "root_orchestration_only": true,
                }))
                .to_request(),
        )
        .await;
        assert_eq!(create.status(), StatusCode::CREATED);
        let list: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/sessions")
                .to_request(),
        )
        .await;
        assert!(list["sessions"][0].get("root_orchestration_only").is_none());
        let detail: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get()
                .uri(&format!("/api/v1/sessions/{id}"))
                .to_request(),
        )
        .await;
        assert_eq!(detail["session"]["root_orchestration_only"], true);

        let resume = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": id,
                    "message": "check progress",
                    "model": "test-model",
                }))
                .to_request(),
        )
        .await;
        assert_eq!(resume.status(), StatusCode::CREATED);
        let before_conflict = state.storage.load_session(id).await.unwrap().unwrap();
        assert!(before_conflict.root_orchestration_only_enabled());

        // Existing-Root inline mode changes are rejected before attachment
        // processing or message persistence.
        let failed_disable = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": id,
                    "message": "attachment is invalid",
                    "model": "test-model",
                    "root_orchestration_only": false,
                    "images": [{"base64": "not-valid-base64%%%", "type": "image/png"}],
                }))
                .to_request(),
        )
        .await;
        assert_eq!(failed_disable.status(), StatusCode::PRECONDITION_REQUIRED);
        let after_failed_disable = state.storage.load_session(id).await.unwrap().unwrap();
        assert!(after_failed_disable.root_orchestration_only_enabled());
        assert_eq!(
            after_failed_disable.root_tool_authority_revision,
            before_conflict.root_tool_authority_revision
        );
        assert_eq!(
            after_failed_disable.messages.len(),
            before_conflict.messages.len()
        );

        let catalog = state.skill_manager.store().skill_catalog_snapshot().await;
        let review = catalog
            .entries
            .iter()
            .find(|entry| entry.id == "review" && entry.winner)
            .expect("builtin review Workflow");
        let workflow_selection = serde_json::json!({
            "id": review.id,
            "source": review.source,
            "revision": review.revision,
            "args": {},
        });
        let rejected = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": id,
                    "message": "must not persist",
                    "model": "test-model",
                    "workflow_selection": workflow_selection,
                }))
                .to_request(),
        )
        .await;
        assert_eq!(rejected.status(), StatusCode::CONFLICT);
        let rejected: Value = test::read_body_json(rejected).await;
        assert_eq!(
            rejected["error"]["code"],
            "root_orchestration_incompatible_mode"
        );
        let after_conflict = state.storage.load_session(id).await.unwrap().unwrap();
        assert_eq!(
            after_conflict.messages.len(),
            before_conflict.messages.len()
        );
        assert_eq!(
            after_conflict.root_tool_authority_revision,
            before_conflict.root_tool_authority_revision
        );

        let unfenced_switch = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": id,
                    "message": "review the task",
                    "model": "test-model",
                    "root_orchestration_only": false,
                    "workflow_selection": workflow_selection,
                }))
                .to_request(),
        )
        .await;
        assert_eq!(unfenced_switch.status(), StatusCode::PRECONDITION_REQUIRED);
        let before_switch = state.storage.load_session(id).await.unwrap().unwrap();
        assert!(before_switch.root_orchestration_only_enabled());
        let operation_id = format!("0:{}", uuid::Uuid::new_v4());
        let switched_mode = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!(
                    "/api/v1/sessions/{id}/root-mode-operations/{operation_id}"
                ))
                .set_json(serde_json::json!({
                    "birth_token": before_switch.root_mode_birth_token(),
                    "expected_epoch": 0,
                    "enabled": false,
                }))
                .to_request(),
        )
        .await;
        assert_eq!(switched_mode.status(), StatusCode::OK);
        let switched = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": id,
                    "message": "review the task",
                    "model": "test-model",
                    "workflow_selection": workflow_selection,
                }))
                .to_request(),
        )
        .await;
        assert_eq!(switched.status(), StatusCode::CREATED);
        let detail: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get()
                .uri(&format!("/api/v1/sessions/{id}"))
                .to_request(),
        )
        .await;
        assert_eq!(detail["session"]["root_orchestration_only"], false);

        // A combined Workflow clear and inline mode change has no fence and
        // cannot use the old chat path.
        let before_failed_enable = state.storage.load_session(id).await.unwrap().unwrap();
        let failed_enable = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": id,
                    "message": "attachment is invalid",
                    "model": "test-model",
                    "root_orchestration_only": true,
                    "selected_skill_ids": [],
                    "images": [{"base64": "not-valid-base64%%%", "type": "image/png"}],
                }))
                .to_request(),
        )
        .await;
        assert_eq!(failed_enable.status(), StatusCode::PRECONDITION_REQUIRED);
        let after_failed_enable = state.storage.load_session(id).await.unwrap().unwrap();
        assert!(!after_failed_enable.root_orchestration_only_enabled());
        assert_eq!(
            after_failed_enable.root_tool_authority_revision,
            before_failed_enable.root_tool_authority_revision
        );
        assert_eq!(
            workflow_runtime_metadata(&after_failed_enable),
            workflow_runtime_metadata(&before_failed_enable)
        );
        assert_eq!(
            after_failed_enable.messages.len(),
            before_failed_enable.messages.len()
        );
    }

    struct RootSwitchProvider {
        system_prompts: Mutex<Vec<String>>,
        started: Semaphore,
    }

    #[async_trait]
    impl LLMProvider for RootSwitchProvider {
        async fn chat_stream(
            &self,
            messages: &[bamboo_agent_core::Message],
            _tools: &[bamboo_agent_core::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            let system_prompt = messages
                .iter()
                .filter(|message| message.role == bamboo_agent_core::Role::System)
                .map(|message| message.content.as_str())
                .collect::<Vec<_>>()
                .join("\n");
            self.system_prompts.lock().unwrap().push(system_prompt);
            self.started.add_permits(1);
            Ok(Box::pin(futures::stream::iter(vec![
                Ok(LLMChunk::Token("done".into())),
                Ok(LLMChunk::Done),
            ])))
        }
    }

    async fn assert_workflow_to_root_switch_retires_pin_before_next_execute(
        id: &str,
        selected_skill_ids: Value,
    ) {
        let data_dir = tempdir().expect("tempdir").keep();
        bamboo_config::paths::init_bamboo_dir(data_dir.clone());
        let mut config = bamboo_llm::Config::from_data_dir(Some(data_dir.clone()));
        config.provider = "openai".into();
        config.providers_mut().openai = Some(bamboo_config::OpenAIConfig {
            model: Some("test-model".into()),
            ..Default::default()
        });
        let provider = Arc::new(RootSwitchProvider {
            system_prompts: Mutex::new(Vec::new()),
            started: Semaphore::new(0),
        });
        let provider_trait: Arc<dyn LLMProvider> = provider.clone();
        let mut app_state = AppState::new_with_provider(data_dir, config, provider_trait)
            .await
            .expect("app state");
        let mut providers = HashMap::new();
        providers.insert("openai".into(), provider.clone() as Arc<dyn LLMProvider>);
        app_state.provider_registry = Arc::new(ProviderRegistry::new(providers, "openai".into()));
        app_state.provider_router = Arc::new(ProviderModelRouter::new(
            app_state.provider_registry.clone(),
        ));
        let state = web::Data::new(app_state);
        seed_active_instruction_workflow(&state, id, "review").await;
        let mut seeded = state.storage.load_session(id).await.unwrap().unwrap();
        seeded.title_generated = true;
        seeded.metadata.insert(
            "skill.context".into(),
            "STALE_WORKFLOW_INSTRUCTION_DO_NOT_RENDER".into(),
        );
        state.save_and_cache_session(&mut seeded).await;
        assert!(state
            .skill_manager
            .pinned_activation_for_workspace(id, None)
            .await
            .unwrap()
            .is_some());

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        // Existing Roots change mode through a recoverable operation. First
        // retire the Workflow in chat, while its user turn is committed.
        let cleared = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": id,
                    "message": "delegate bounded work",
                    "model": "test-model",
                    "selected_skill_ids": selected_skill_ids,
                }))
                .to_request(),
        )
        .await;
        assert_eq!(cleared.status(), StatusCode::CREATED);
        let cleared = state.storage.load_session(id).await.unwrap().unwrap();
        assert!(!cleared.root_orchestration_only_enabled());
        assert!(cleared.selected_skill_ids().is_none());
        let epoch = cleared.root_mode_transition_epoch;
        let operation_id = format!("{epoch}:{}", uuid::Uuid::new_v4());
        let switched = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!(
                    "/api/v1/sessions/{id}/root-mode-operations/{operation_id}"
                ))
                .set_json(serde_json::json!({
                    "birth_token": cleared.root_mode_birth_token(),
                    "expected_epoch": epoch,
                    "enabled": true,
                }))
                .to_request(),
        )
        .await;
        assert_eq!(switched.status(), StatusCode::OK);
        let saved = state.storage.load_session(id).await.unwrap().unwrap();
        assert!(saved.root_orchestration_only_enabled());
        assert!(saved.selected_skill_ids().is_none());
        for key in [
            bamboo_skills::WORKFLOW_SELECTION_METADATA_KEY,
            bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY,
            bamboo_skills::ACTIVE_WORKFLOW_SNAPSHOT_METADATA_KEY,
            bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTION_SOURCE_KEY,
            bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTED_SKILL_REVISIONS_KEY,
            bamboo_skills::runtime_metadata::SKILL_RUNTIME_PINNED_SNAPSHOT_KEY,
            "skill.context",
        ] {
            assert!(!saved.metadata.contains_key(key), "stale {key}");
        }
        assert!(state
            .skill_manager
            .pinned_activation_for_workspace(id, None)
            .await
            .unwrap()
            .is_none());

        let execute = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/execute/{id}"))
                .set_json(serde_json::json!({}))
                .to_request(),
        )
        .await;
        assert_eq!(execute.status(), StatusCode::ACCEPTED);
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            provider.started.acquire(),
        )
        .await
        .expect("next execute reached provider")
        .expect("provider semaphore open")
        .forget();
        let prompts = provider.system_prompts.lock().unwrap().join("\n");
        assert!(!prompts.contains("STALE_WORKFLOW_INSTRUCTION_DO_NOT_RENDER"));
        assert!(!prompts.contains("Required Explicit Workflow Activation"));
        assert!(!prompts.contains("Explicit Workflow Already Activated"));
    }

    #[actix_web::test]
    async fn successful_workflow_to_root_switch_retires_pin_before_next_execute() {
        assert_workflow_to_root_switch_retires_pin_before_next_execute(
            "root-workflow-retire-empty",
            serde_json::json!([]),
        )
        .await;
    }

    #[actix_web::test]
    async fn whitespace_only_workflow_to_root_switch_retires_pin_before_next_execute() {
        assert_workflow_to_root_switch_retires_pin_before_next_execute(
            "root-workflow-retire-blank",
            serde_json::json!([" "]),
        )
        .await;
    }

    #[actix_web::test]
    async fn root_tool_mode_child_cannot_select_or_clear_it() {
        let state = new_state().await;
        let mut root = Session::new("root-tool-parent", "test-model");
        root.set_root_orchestration_only(true).unwrap();
        state.save_and_cache_session(&mut root).await;
        let mut child = Session::new_child_of("root-tool-child", &root, "test-model", "child");
        state.save_and_cache_session(&mut child).await;
        let child_before = state
            .storage
            .load_session(&child.id)
            .await
            .unwrap()
            .unwrap();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        for enabled in [false, true] {
            let rejected = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri("/api/v1/chat")
                    .set_json(serde_json::json!({
                        "session_id": child.id,
                        "message": "cannot select Root mode",
                        "model": "test-model",
                        "root_orchestration_only": enabled,
                    }))
                    .to_request(),
            )
            .await;
            assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
            let rejected: Value = test::read_body_json(rejected).await;
            assert_eq!(
                rejected["error"]["code"],
                "root_orchestration_requires_root"
            );
        }
        let child_after = state
            .storage
            .load_session(&child.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(child_after.messages.len(), child_before.messages.len());
        assert_eq!(
            child_after.root_tool_authority_revision,
            child_before.root_tool_authority_revision
        );
    }

    #[actix_web::test]
    async fn root_tool_mode_records_legacy_plan_rejection_without_changing_authority() {
        let state = new_state().await;
        let id = "root-tool-plan-conflict";
        let mut root = Session::new(id, "test-model");
        root.agent_runtime_state = Some(bamboo_domain::AgentRuntimeState {
            plan_mode: Some(bamboo_domain::PlanModeState {
                entered_at: chrono::Utc::now(),
                pre_permission_mode: "default".into(),
                plan_file_path: None,
                status: bamboo_domain::PlanModeStatus::Exploring,
            }),
            ..bamboo_domain::AgentRuntimeState::default()
        });
        state.save_and_cache_session(&mut root).await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let operation_id = format!("0:{}", uuid::Uuid::new_v4());
        let path = format!("/api/v1/sessions/{id}/root-mode-operations/{operation_id}");
        let body = serde_json::json!({
            "birth_token": root.root_mode_birth_token(),
            "expected_epoch": 0,
            "enabled": true,
        });
        let rejected = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&path)
                .set_json(&body)
                .to_request(),
        )
        .await;
        assert_eq!(rejected.status(), StatusCode::CONFLICT);
        let rejected: Value = test::read_body_json(rejected).await;
        assert_eq!(
            rejected["error"]["code"],
            "root_orchestration_incompatible_mode"
        );
        let after = state.storage.load_session(id).await.unwrap().unwrap();
        assert!(!after.root_orchestration_only_enabled());
        assert_eq!(after.root_tool_authority_revision, 0);
        assert_eq!(after.root_mode_transition_epoch, 1);
        assert!(after.messages.is_empty());
        assert!(after
            .agent_runtime_state
            .as_ref()
            .is_some_and(|runtime| runtime.plan_mode.is_some()));
        let recovered = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("{path}/recover"))
                .set_json(&body)
                .to_request(),
        )
        .await;
        assert_eq!(recovered.status(), StatusCode::OK);
        let recovered: Value = test::read_body_json(recovered).await;
        assert_eq!(recovered["status"], "rejected_incompatible");
        assert_eq!(recovered["resulting_epoch"], 1);
    }

    #[actix_web::test]
    async fn root_tool_mode_rejects_selected_skill_before_persistence() {
        let state = new_state().await;
        let id = "root-tool-skill-conflict";
        let mut root = Session::new(id, "test-model");
        root.set_root_orchestration_only(true).unwrap();
        state.save_and_cache_session(&mut root).await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let rejected = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": id,
                    "message": "must not persist",
                    "model": "test-model",
                    "selected_skill_ids": ["review"],
                }))
                .to_request(),
        )
        .await;
        assert_eq!(rejected.status(), StatusCode::CONFLICT);
        let rejected: Value = test::read_body_json(rejected).await;
        assert_eq!(
            rejected["error"]["code"],
            "root_orchestration_incompatible_mode"
        );
        let after = state.storage.load_session(id).await.unwrap().unwrap();
        assert!(after.root_orchestration_only_enabled());
        assert_eq!(after.root_tool_authority_revision, 1);
        assert!(after.messages.is_empty());
        assert!(after.selected_skill_ids().is_none());
    }

    async fn seed_active_instruction_workflow(
        state: &web::Data<AppState>,
        session_id: &str,
        workflow_id: &str,
    ) -> bamboo_skills::WorkflowSelection {
        let catalog = state.skill_manager.store().skill_catalog_snapshot().await;
        let entry = catalog
            .entries
            .iter()
            .find(|entry| entry.id == workflow_id && entry.winner)
            .expect("builtin instruction Workflow")
            .clone();
        let selection = bamboo_skills::WorkflowSelection {
            id: entry.id.clone(),
            source: entry.source,
            revision: entry.revision,
            args: serde_json::json!({}),
        };
        let ids = [entry.id.clone()];
        let activation = state
            .skill_manager
            .resolve_and_pin_activation_for_request_with_mode_and_budget(
                session_id,
                &std::collections::BTreeSet::new(),
                Some(&ids),
                None,
                None,
                bamboo_skills::DEFAULT_WORKFLOW_CATALOG_CONTEXT_TOKENS,
            )
            .await
            .expect("pin canonical live activation");
        let snapshot = state
            .skill_manager
            .store()
            .export_activation_snapshot(session_id)
            .await
            .expect("export canonical activation snapshot");
        let mut session = Session::new(session_id, "test-model");
        session.metadata.insert(
            bamboo_skills::WORKFLOW_SELECTION_METADATA_KEY.to_string(),
            serde_json::to_string(&selection).expect("selection JSON"),
        );
        bamboo_skills::persist_explicit_workflow_candidate(
            &mut session.metadata,
            &selection,
            &activation,
            &snapshot,
        )
        .expect("persist exact candidate");
        bamboo_skills::record_loaded_workflow_activation(
            &mut session.metadata,
            workflow_id,
            format!("sha256:{workflow_id}"),
        )
        .expect("publish active Workflow");
        state.save_and_cache_session(&mut session).await;
        selection
    }

    fn workflow_runtime_metadata(session: &Session) -> std::collections::BTreeMap<String, String> {
        session
            .metadata
            .iter()
            .filter(|(key, _)| {
                key.starts_with("workflow.")
                    || key.starts_with("skill_runtime_")
                    || key.as_str() == "selected_skill_ids"
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    struct BlockingTitleProvider {
        calls: AtomicUsize,
        started: Semaphore,
        release: Semaphore,
    }

    impl BlockingTitleProvider {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                calls: AtomicUsize::new(0),
                started: Semaphore::new(0),
                release: Semaphore::new(0),
            })
        }
    }

    #[async_trait]
    impl LLMProvider for BlockingTitleProvider {
        async fn chat_stream(
            &self,
            _messages: &[bamboo_agent_core::Message],
            _tools: &[bamboo_agent_core::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            panic!("title generation must use request options")
        }

        async fn chat_stream_with_options(
            &self,
            _messages: &[bamboo_agent_core::Message],
            _tools: &[bamboo_agent_core::ToolSchema],
            _max_output_tokens: Option<u32>,
            model: &str,
            options: Option<&LLMRequestOptions>,
        ) -> Result<LLMStream, LLMError> {
            if model != "title-model" {
                return Ok(Box::pin(futures::stream::iter(vec![
                    Ok(LLMChunk::Token("Answered by local Runtime fixture".into())),
                    Ok(LLMChunk::Done),
                ])));
            }
            assert_eq!(
                options.and_then(|value| value.request_purpose.as_deref()),
                Some("title_generation")
            );
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.add_permits(1);
            let _permit = self
                .release
                .acquire()
                .await
                .expect("release semaphore stays open");

            Ok(Box::pin(futures::stream::iter(vec![
                Ok(LLMChunk::Token("Generated Immediately".to_string())),
                Ok(LLMChunk::Done),
            ])))
        }
    }

    async fn title_test_state(provider: Arc<BlockingTitleProvider>) -> web::Data<AppState> {
        let data_dir = tempdir().expect("tempdir").keep();
        bamboo_config::paths::init_bamboo_dir(data_dir.clone());
        let mut config = bamboo_llm::Config::from_data_dir(Some(data_dir.clone()));
        config.provider = "openai".to_string();
        config.providers_mut().openai = Some(bamboo_config::OpenAIConfig {
            model: Some("chat-model".to_string()),
            fast_model: Some("title-model".to_string()),
            ..Default::default()
        });

        let provider_trait: Arc<dyn LLMProvider> = provider.clone();
        let mut app_state = AppState::new_with_provider(data_dir, config, provider_trait)
            .await
            .expect("app state");
        let mut providers = HashMap::new();
        providers.insert("openai".to_string(), provider as Arc<dyn LLMProvider>);
        app_state.provider_registry =
            Arc::new(ProviderRegistry::new(providers, "openai".to_string()));
        app_state.provider_router = Arc::new(ProviderModelRouter::new(
            app_state.provider_registry.clone(),
        ));
        web::Data::new(app_state)
    }

    #[actix_web::test]
    async fn implicit_chat_at_reserved_id_is_ordinary_and_blocks_supervisor_bootstrap() {
        let provider = BlockingTitleProvider::new();
        provider.release.add_permits(1);
        let state = title_test_state(provider).await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let id = bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID;
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id":id,"message":"ordinary chat","model":"chat-model",
                    "authority_identity":{"kind":"supervisor","incarnation_id":uuid::Uuid::new_v4()}
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let persisted = state.storage.load_session(id).await.unwrap().unwrap();
        assert!(persisted.authority_identity.is_ordinary());
        assert!(!persisted
            .messages
            .iter()
            .any(|m| m.role == bamboo_agent_core::Role::User));
        drop(state.admit_chat_for_execute(id).await.unwrap());
        let persisted = state.storage.load_session(id).await.unwrap().unwrap();
        assert!(persisted
            .messages
            .iter()
            .any(|m| m.content == "ordinary chat"));
        assert_eq!(
            state
                .storage
                .get_or_create_default_supervisor("initial-model")
                .await
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::AlreadyExists
        );
        assert!(state
            .storage
            .load_session(id)
            .await
            .unwrap()
            .unwrap()
            .authority_identity
            .is_ordinary());
    }

    /// Native Chat delays title work until real checked admission and Ready.
    /// Preserve #793's single in-flight provider call and one metadata event.
    #[actix_web::test]
    async fn native_title_waits_for_checked_ready_and_deduplicates_inflight_work() {
        let provider = BlockingTitleProvider::new();
        let state = title_test_state(provider.clone()).await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let session_id = "chat-title-before-execute";
        let sender = state.get_session_event_sender(session_id).await;
        let mut title_events = sender.subscribe();

        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": session_id,
                    "message": "Fix title generation timing",
                    "model": "chat-model"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);

        assert_eq!(
            provider.calls.load(Ordering::SeqCst),
            0,
            "Chat without execute starts no title work"
        );
        let before = state
            .storage
            .load_session(session_id)
            .await
            .unwrap()
            .unwrap();
        assert!(!before
            .messages
            .iter()
            .any(|m| m.role == bamboo_agent_core::Role::User));
        let execution = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/execute/{session_id}"))
                .set_json(serde_json::json!({"model":"chat-model"}))
                .to_request(),
        )
        .await;
        assert_eq!(execution.status(), StatusCode::ACCEPTED);
        let _started = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            provider.started.acquire(),
        )
        .await
        .expect("checked Ready starts the title provider")
        .expect("started semaphore stays open");

        let pending = state
            .storage
            .load_session(session_id)
            .await
            .expect("load pending session")
            .expect("session persisted");
        assert!(!pending.title_generated);
        assert_eq!(pending.title_version, 0);

        tokio::time::timeout(CONCURRENCY_ASSERT_TIMEOUT, async {
            loop {
                if state
                    .agent_runners
                    .read()
                    .await
                    .get(session_id)
                    .is_some_and(|runner| {
                        matches!(runner.status, crate::app_state::AgentStatus::Completed)
                    })
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first actual Runtime completes while its title provider remains blocked");
        let second = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": session_id,
                    "message": "Second durable user message",
                    "model": "chat-model"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(second.status(), StatusCode::CREATED);
        let second_execution = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&format!("/api/v1/execute/{session_id}"))
                .set_json(serde_json::json!({"model":"chat-model"}))
                .to_request(),
        )
        .await;
        assert_eq!(second_execution.status(), StatusCode::ACCEPTED);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);

        provider.release.add_permits(1);
        let finalized = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let session = state
                    .storage
                    .load_session(session_id)
                    .await
                    .expect("load finalized session")
                    .expect("session remains present");
                if session.title_generated {
                    break session;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("title generation finalized");

        assert_eq!(finalized.title, "Generated Immediately");
        assert_eq!(finalized.title_version, 1);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);

        let event = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let event = title_events
                    .recv()
                    .await
                    .expect("title event channel remains open");
                if matches!(event, AgentEvent::SessionTitleUpdated { .. }) {
                    break event;
                }
            }
        })
        .await
        .expect("one title event arrives alongside real Runtime events");
        assert!(matches!(
            event,
            AgentEvent::SessionTitleUpdated {
                title_generated: true,
                ..
            }
        ));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), async {
                loop {
                    let event = title_events.recv().await.unwrap();
                    if matches!(event, AgentEvent::SessionTitleUpdated { .. }) {
                        break event;
                    }
                }
            })
            .await
            .is_err(),
            "deduplicated title work must not emit a second metadata event"
        );
    }

    /// #480: omitting `model` on `POST /chat` falls back to the server's
    /// resolved default (the same resolution `GET /execute/defaults` and the
    /// connect bridge use) — the session ends up with the CONFIGURED default
    /// model, not an error and not an empty model.
    #[actix_web::test]
    async fn chat_without_model_uses_resolved_default_model() {
        let state = new_state().await;
        {
            let mut config = state.config.write().await;
            config.provider = "openai".to_string();
            config.providers_mut().openai = Some(bamboo_config::OpenAIConfig {
                model: Some("gpt-configured-default".to_string()),
                ..Default::default()
            });
        }

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({ "message": "hello" }))
                .to_request(),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::CREATED);
        let body: Value = test::read_body_json(resp).await;
        let session_id = body["session_id"].as_str().expect("session_id").to_string();

        let session = state
            .storage
            .load_session(&session_id)
            .await
            .expect("load")
            .expect("session exists");
        assert_eq!(session.model, "gpt-configured-default");
    }

    /// An explicit `model` on `POST /chat` is unchanged by #480 — it is used
    /// as-is even when a different server default is configured.
    #[actix_web::test]
    async fn chat_with_explicit_model_is_unchanged() {
        let state = new_state().await;
        {
            let mut config = state.config.write().await;
            config.provider = "openai".to_string();
            config.providers_mut().openai = Some(bamboo_config::OpenAIConfig {
                model: Some("gpt-configured-default".to_string()),
                ..Default::default()
            });
        }

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "message": "hello",
                    "model": "gpt-explicit-override"
                }))
                .to_request(),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::CREATED);
        let body: Value = test::read_body_json(resp).await;
        let session_id = body["session_id"].as_str().expect("session_id").to_string();

        let session = state
            .storage
            .load_session(&session_id)
            .await
            .expect("load")
            .expect("session exists");
        assert_eq!(session.model, "gpt-explicit-override");
    }

    /// #733: a retry after the first response is lost must receive the exact
    /// first response without appending the user message a second time.
    #[actix_web::test]
    async fn idempotency_key_replays_chat_without_duplicate_message() {
        use bamboo_agent_core::Role;

        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let request = || {
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .insert_header(("Idempotency-Key", "chat-retry-733"))
                .set_json(serde_json::json!({
                    "message": "persist exactly once",
                    "model": "chat-model"
                }))
                .to_request()
        };

        let first = test::call_service(&app, request()).await;
        assert_eq!(first.status(), StatusCode::CREATED);
        let first_body = test::read_body(first).await;
        let replay = test::call_service(&app, request()).await;
        assert_eq!(replay.status(), StatusCode::CREATED);
        let replay_body = test::read_body(replay).await;
        assert_eq!(
            replay_body, first_body,
            "retry must replay exact JSON bytes"
        );

        let response: Value = serde_json::from_slice(&first_body).expect("chat response JSON");
        let session_id = response["session_id"].as_str().expect("session_id");
        let before = state
            .storage
            .load_session(session_id)
            .await
            .unwrap()
            .unwrap();
        assert!(!before.messages.iter().any(|m| m.role == Role::User));
        assert_eq!(
            state
                .session_inbox
                .inspect(session_id)
                .await
                .unwrap()
                .pending,
            1
        );
        drop(state.admit_chat_for_execute(session_id).await.unwrap());
        let session = state
            .storage
            .load_session(session_id)
            .await
            .expect("load session")
            .expect("session exists");
        assert_eq!(
            session
                .messages
                .iter()
                .filter(|message| message.role == Role::User)
                .count(),
            1,
            "replay must not append a second user message"
        );

        let conflict = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .insert_header(("Idempotency-Key", "chat-retry-733"))
                .set_json(serde_json::json!({
                    "message": "different payload",
                    "model": "chat-model"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(conflict.status(), StatusCode::CONFLICT);
        let conflict: Value = test::read_body_json(conflict).await;
        assert_eq!(conflict["error"]["code"], "idempotency_key_conflict");
    }

    /// No request model AND no server default configured → 400, not a silent
    /// empty-model session.
    #[actix_web::test]
    async fn chat_without_model_and_without_default_errors() {
        let state = new_state().await;

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({ "message": "hello" }))
                .to_request(),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[actix_web::test]
    async fn chat_checks_nested_workspace_owner_before_creating_session() {
        let state = new_state().await;
        let workspace = tempdir().expect("workspace");
        let nested = workspace.path().join("nested");
        std::fs::create_dir_all(&nested).expect("nested");
        let owner = state
            .project_store
            .create_with_bindings(
                "Owner",
                None,
                vec![bamboo_domain::WorkspaceBinding {
                    path: workspace.path().to_string_lossy().to_string(),
                    label: None,
                    git_common_dir: None,
                }],
            )
            .expect("Project");
        let nested = nested.to_string_lossy().to_string();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let conflict = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": "chat-cross-project",
                    "message": "must not persist",
                    "model": "test-model",
                    "workspace_path": nested.clone(),
                }))
                .to_request(),
        )
        .await;
        assert_eq!(conflict.status(), StatusCode::CONFLICT);
        let conflict: Value = test::read_body_json(conflict).await;
        assert_eq!(conflict["error"]["code"], "project_workspace_conflict");
        assert!(
            state
                .storage
                .load_session("chat-cross-project")
                .await
                .expect("load")
                .is_none(),
            "ownership failure must happen before session persistence"
        );

        let mut feed = state.account_sink.subscribe();
        let created = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": "chat-owned-project",
                    "project_id": owner.id.to_string(),
                    "message": "hello",
                    "model": "test-model",
                    "workspace_path": nested,
                }))
                .to_request(),
        )
        .await;
        assert_eq!(created.status(), StatusCode::CREATED);
        let created_event = tokio::time::timeout(std::time::Duration::from_secs(1), feed.recv())
            .await
            .expect("SessionCreated timeout")
            .expect("SessionCreated event");
        assert!(matches!(
            &created_event.event,
            bamboo_agent_core::AgentEvent::SessionCreated {
                session_id,
                project_id: Some(project_id),
                ..
            } if session_id == "chat-owned-project" && project_id == owner.id.as_str()
        ));
        let replay = bamboo_engine::events::journal::read_since(
            state.account_sink.events_dir(),
            created_event.seq.saturating_sub(1),
        )
        .expect("journal replay");
        assert!(replay.iter().any(|change| matches!(
            &change.event,
            bamboo_agent_core::AgentEvent::SessionCreated {
                session_id,
                project_id: Some(project_id),
                ..
            } if session_id == "chat-owned-project" && project_id == owner.id.as_str()
        )));
        let session = state
            .storage
            .load_session("chat-owned-project")
            .await
            .expect("load")
            .expect("session");
        let workspace_display = session
            .workspace_path_meta()
            .expect("persisted workspace path");
        let resolved = state
            .project_context_resolver
            .resolve(&session, None)
            .await
            .expect("resolve persisted Project context")
            .expect("assigned Project context");
        assert_eq!(
            resolved.binding_status,
            bamboo_engine::project_context::WorkspaceBindingStatus::Registered
        );
        assert!(session
            .messages
            .iter()
            .filter(|message| matches!(message.role, bamboo_agent_core::Role::System))
            .all(|message| {
                !message.content.contains("BAMBOO_PROJECT_CONTEXT_START")
                    && !message.content.contains("BAMBOO_WORKSPACE_CONTEXT_START")
                    && !message.content.contains(&workspace_display)
            }));
        let snapshot = session.prompt_snapshot.expect("immediate prompt snapshot");
        let project_context = snapshot
            .project_context
            .as_deref()
            .expect("typed Project context");
        assert!(project_context.contains(owner.id.as_str()));
        assert!(!project_context.contains(&workspace_display));
        assert!(!project_context.contains("Project home (Bamboo data):"));
        let workspace_context = snapshot
            .workspace_context
            .as_deref()
            .expect("typed Workspace context");
        assert!(workspace_context.contains(&workspace_display));
        assert!(workspace_context.contains("Workspace source: explicit"));
        assert!(workspace_context.contains("Binding status: registered"));
        assert_eq!(
            snapshot
                .effective_system_prompt
                .matches("<!-- BAMBOO_PROJECT_CONTEXT_START -->")
                .count(),
            0
        );
        assert_eq!(
            snapshot
                .effective_system_prompt
                .matches("<!-- BAMBOO_WORKSPACE_CONTEXT_START -->")
                .count(),
            0
        );
        assert!(!snapshot
            .effective_system_prompt
            .contains(&workspace_display));
    }

    #[actix_web::test]
    async fn chat_invalid_workspace_is_400_and_has_no_session_side_effect() {
        let state = new_state().await;
        let fixture = tempdir().expect("fixture");
        let missing = fixture.path().join("missing");
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": "chat-invalid-workspace",
                    "message": "must not persist",
                    "model": "test-model",
                    "workspace_path": missing,
                }))
                .to_request(),
        )
        .await;
        let status = response.status();
        let body: Value = test::read_body_json(response).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "body={body}");
        assert_eq!(body["error"]["code"], "workspace_invalid");
        assert!(state
            .storage
            .load_session("chat-invalid-workspace")
            .await
            .expect("load")
            .is_none());
    }

    #[actix_web::test]
    async fn chat_explicit_workspace_switch_requires_existing_project_binding() {
        let state = new_state().await;
        let project_path = tempdir().expect("Project path");
        let unbound = tempdir().expect("unbound workspace");
        let project = state
            .project_store
            .create_with_project_path(
                "Assigned Project",
                None,
                project_path.path().to_string_lossy(),
                Vec::new(),
            )
            .expect("Project");
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": "chat-unbound-workspace",
                    "project_id": project.id,
                    "message": "must not persist",
                    "model": "test-model",
                    "workspace_path": unbound.path(),
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body: Value = test::read_body_json(response).await;
        assert_eq!(body["error"]["code"], "project_workspace_unbound");
        assert_eq!(body["session_project_id"], project.id.as_str());
        assert!(
            state
                .storage
                .load_session("chat-unbound-workspace")
                .await
                .expect("load")
                .is_none(),
            "binding validation must happen before chat creates the session"
        );
    }

    #[actix_web::test]
    async fn queued_chat_omitting_workspace_preserves_authoritative_workspace_update() {
        let state = new_state().await;
        let fixture = tempdir().expect("fixture");
        let workspace_a = fixture.path().join("workspace-a");
        let workspace_b = fixture.path().join("workspace-b");
        std::fs::create_dir_all(&workspace_a).unwrap();
        std::fs::create_dir_all(&workspace_b).unwrap();
        let session_id = "chat-workspace-lock-barrier";
        let mut session = Session::new(session_id, "test-model");
        session.set_workspace_path_meta(workspace_a.to_string_lossy().into_owned());
        state.storage.save_session(&session).await.unwrap();
        state.sessions.insert(
            session_id.to_string(),
            std::sync::Arc::new(bamboo_engine::SessionSnapshot::new(session)),
        );
        bamboo_agent_core::workspace_state::set_workspace(
            session_id,
            workspace_a.canonicalize().unwrap(),
        );
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        // Hold the same lock used by Workspace/PATCH writes. Poll chat until it
        // reaches the lock after its lock-free preflight, then commit workspace
        // B before allowing chat's authoritative reload to proceed.
        let guard = state.persistence.acquire_lock(session_id).await;
        let chat = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": session_id,
                    "message": "workspace field intentionally omitted",
                    "model": "test-model"
                }))
                .to_request(),
        );
        tokio::pin!(chat);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(200), &mut chat)
                .await
                .is_err(),
            "chat should wait at the per-session transaction lock"
        );

        let mut latest = state
            .persistence
            .storage()
            .load_session(session_id)
            .await
            .unwrap()
            .unwrap();
        latest.set_workspace_path_meta(workspace_b.to_string_lossy().into_owned());
        state
            .persistence
            .storage()
            .save_session(&latest)
            .await
            .unwrap();
        state.sessions.insert(
            session_id.to_string(),
            std::sync::Arc::new(bamboo_engine::SessionSnapshot::new(latest)),
        );
        bamboo_agent_core::workspace_state::set_workspace(
            session_id,
            workspace_b.canonicalize().unwrap(),
        );
        drop(guard);

        let response = chat.await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let persisted = state
            .storage
            .load_session(session_id)
            .await
            .unwrap()
            .unwrap();
        let workspace_b = workspace_b.canonicalize().unwrap();
        assert_eq!(
            persisted.workspace_path_meta().as_deref(),
            Some(bamboo_config::paths::path_to_display_string(&workspace_b).as_str())
        );
        assert_eq!(
            bamboo_agent_core::workspace_state::get_workspace(session_id).as_deref(),
            Some(workspace_b.as_path())
        );
    }

    #[actix_web::test]
    async fn chat_membership_authority_is_durable_storage_not_stale_cache() {
        let state = new_state().await;
        let project_a_path = tempdir().expect("Project A path");
        let project_b_path = tempdir().expect("Project B path");
        let project_a = state
            .project_store
            .create_with_project_path(
                "Project A",
                None,
                project_a_path.path().to_string_lossy(),
                Vec::new(),
            )
            .unwrap();
        let project_b = state
            .project_store
            .create_with_project_path(
                "Project B",
                None,
                project_b_path.path().to_string_lossy(),
                Vec::new(),
            )
            .unwrap();
        let session_id = "chat-authoritative-project-storage";
        let mut durable = Session::new(session_id, "test-model");
        durable.set_project_id_meta(project_b.id.to_string());
        state.storage.save_session(&durable).await.unwrap();
        let mut stale_cache = durable.clone();
        stale_cache.set_project_id_meta(project_a.id.to_string());
        stale_cache.updated_at = chrono::Utc::now() + chrono::Duration::hours(1);
        state.sessions.insert(
            session_id.to_string(),
            std::sync::Arc::new(bamboo_engine::SessionSnapshot::new(stale_cache)),
        );
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": session_id,
                    "message": "use durable membership",
                    "model": "test-model"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let persisted = state
            .storage
            .load_session(session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            persisted.project_id_meta().as_deref(),
            Some(project_b.id.as_str())
        );
    }

    #[actix_web::test]
    async fn chat_uses_assigned_project_path_before_foreign_configured_default() {
        let state = new_state().await;
        let workspace = tempdir().expect("foreign default workspace");
        let other_workspace = tempdir().expect("assigned Project path");
        let owner = state
            .project_store
            .create_with_project_path(
                "Default Owner",
                None,
                workspace.path().to_string_lossy(),
                Vec::new(),
            )
            .expect("owner Project");
        let other = state
            .project_store
            .create_with_project_path(
                "Other Project",
                None,
                other_workspace.path().to_string_lossy(),
                Vec::new(),
            )
            .expect("other Project");
        state.config.write().await.default_work_area = Some(bamboo_config::DefaultWorkAreaConfig {
            path: Some(workspace.path().to_string_lossy().into_owned()),
        });
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": "chat-project-default",
                    "project_id": other.id.to_string(),
                    "message": "use Project path",
                    "model": "test-model"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let persisted = state
            .storage
            .load_session("chat-project-default")
            .await
            .expect("load")
            .expect("session");
        assert_eq!(
            persisted.workspace_path_meta().as_deref(),
            Some(
                bamboo_config::paths::path_to_display_string(
                    &other_workspace.path().canonicalize().unwrap()
                )
                .as_str()
            )
        );
        assert_ne!(
            persisted.project_id_meta().as_deref(),
            Some(owner.id.as_str())
        );
    }

    #[actix_web::test]
    async fn chat_persists_same_project_configured_default_and_dynamic_context() {
        let state = new_state().await;
        let workspace = tempdir().expect("default workspace");
        let foreign_default = tempdir().expect("foreign global default");
        let project = state
            .project_store
            .create_with_project_path(
                "Default Owner",
                None,
                workspace.path().to_string_lossy(),
                Vec::new(),
            )
            .expect("Project");
        state.config.write().await.default_work_area = Some(bamboo_config::DefaultWorkAreaConfig {
            path: Some(foreign_default.path().to_string_lossy().into_owned()),
        });
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": "chat-default-owned",
                    "project_id": project.id.to_string(),
                    "message": "hello",
                    "model": "test-model"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let canonical = workspace.path().canonicalize().expect("canonical");
        let canonical_display = bamboo_config::paths::path_to_display_string(&canonical);
        let session = state
            .storage
            .load_session("chat-default-owned")
            .await
            .expect("load")
            .expect("session");
        assert_eq!(
            session.workspace_path_meta().as_deref(),
            Some(canonical_display.as_str())
        );
        assert_eq!(
            bamboo_agent_core::workspace_state::get_workspace("chat-default-owned").as_deref(),
            Some(canonical.as_path())
        );
        assert!(session
            .messages
            .iter()
            .filter(|message| matches!(message.role, bamboo_agent_core::Role::System))
            .all(|message| {
                !message.content.contains("BAMBOO_PROJECT_CONTEXT_START")
                    && !message.content.contains("BAMBOO_WORKSPACE_CONTEXT_START")
                    && !message.content.contains(&canonical_display)
            }));
        let snapshot = session.prompt_snapshot.expect("prompt snapshot");
        let project_context = snapshot
            .project_context
            .as_deref()
            .expect("typed Project context");
        assert!(project_context.contains(project.id.as_str()));
        assert!(!project_context.contains(&canonical_display));
        assert!(!project_context.contains("Project home (Bamboo data):"));
        let workspace_context = snapshot
            .workspace_context
            .as_deref()
            .expect("typed Workspace context");
        assert!(workspace_context.contains(&canonical_display));
        assert!(workspace_context.contains("Binding status: registered"));
        assert!(workspace_context.contains("Workspace source: project_default"));
        assert_eq!(
            snapshot
                .effective_system_prompt
                .matches("BAMBOO_PROJECT_CONTEXT_START")
                .count(),
            0
        );
        assert_eq!(
            snapshot
                .effective_system_prompt
                .matches("BAMBOO_WORKSPACE_CONTEXT_START")
                .count(),
            0
        );
        assert!(!snapshot
            .effective_system_prompt
            .contains(&canonical_display));
    }

    #[actix_web::test]
    async fn chat_project_without_path_fails_before_session_side_effects() {
        let state = new_state().await;
        let project = state
            .project_store
            .create("Legacy unconfigured", None)
            .expect("legacy Project");
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": "chat-project-path-missing",
                    "project_id": project.id.to_string(),
                    "message": "must not persist",
                    "model": "test-model"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body: Value = test::read_body_json(response).await;
        assert_eq!(body["error"]["code"], "project_path_missing");
        assert!(state
            .storage
            .load_session("chat-project-path-missing")
            .await
            .expect("load")
            .is_none());
        assert!(
            bamboo_agent_core::workspace_state::peek_workspace("chat-project-path-missing")
                .is_none()
        );
    }

    #[actix_web::test]
    async fn queued_chat_delivers_hook_context_but_ticket_provenance_stays_raw() {
        let root = tempdir().unwrap();
        bamboo_config::paths::init_bamboo_dir(root.path().to_path_buf());
        let mut state = AppState::new(root.path().to_path_buf()).await.unwrap();
        {
            let mut config = state.config.write().await;
            *config = serde_json::from_value(serde_json::json!({
                "provider":"openai", "features":{"ticket_mutation":true},
                "providers":{"openai":{"api_key":"fixture","model":"test-model"}}
            }))
            .unwrap();
            config.lifecycle_hooks = bamboo_config::LifecycleHooksConfig {
                enabled: true,
                user_prompt_submit: vec![bamboo_config::LifecycleHookGroup {
                    enabled: true,
                    matcher: None,
                    hooks: vec![bamboo_config::LifecycleHookHandler::command(
                        "printf '%s' '{\"additional_context\":\"Approve Invoice\"}'",
                        bamboo_config::DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS,
                    )],
                }],
                ..Default::default()
            };
        }
        state.tickets = Arc::new(
            crate::app_state::ticket_application::TicketApplication::open(
                root.path(),
                state.storage.clone(),
                state.config.clone(),
            )
            .await,
        );
        assert!(state.tickets.service().is_ok());
        let state = web::Data::new(state);
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        for ticket in [false, true] {
            for image in [false, true] {
                let id = format!("hook-queued-{ticket}-{image}");
                let session_id = if ticket {
                    bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID
                } else {
                    "hook-ordinary"
                };
                let mut body = serde_json::json!({
                    "session_id":session_id, "message_id":id,
                    "message":"raw Human request", "model":"test-model"
                });
                if image {
                    body["images"] = serde_json::json!([{
                        "base64":"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jF0cAAAAASUVORK5CYII=",
                        "type":"image/png"
                    }]);
                }
                let response = test::call_service(
                    &app,
                    test::TestRequest::post()
                        .uri("/api/v1/chat")
                        .set_json(body)
                        .peer_addr("127.0.0.1:5700".parse().unwrap())
                        .to_request(),
                )
                .await;
                let status = response.status();
                let response: Value = test::read_body_json(response).await;
                assert_eq!(status, StatusCode::CREATED, "{response}");
                let claims = state.session_inbox.claim(session_id, 10).await.unwrap();
                assert_eq!(claims.len(), 1);
                let message = claims[0].envelope.to_provider_message().unwrap();
                assert!(message
                    .content
                    .starts_with("raw Human request\n\n<user_prompt_submit_context>"));
                assert!(message.content.contains("Approve Invoice"));
                assert_eq!(claims[0].envelope.id.as_str(), id);
                if image {
                    let bamboo_domain::SessionMessageBody::Content(content) =
                        &claims[0].envelope.body
                    else {
                        panic!("content");
                    };
                    assert!(
                        matches!(&content.parts[0], bamboo_domain::MessagePart::Text { text } if text == &message.content)
                    );
                }
                if ticket {
                    let service = state.tickets.service().unwrap();
                    let record = service.published().unwrap().1.resolutions[&id]
                        .ingress
                        .clone()
                        .unwrap();
                    assert_eq!(record.text, "raw Human request");
                    assert!(!record.text.contains("Approve Invoice"));
                }
                let mut session = state
                    .storage
                    .load_session(session_id)
                    .await
                    .unwrap()
                    .unwrap();
                session.messages.push(message);
                state.storage.save_session(&session).await.unwrap();
                for claim in &claims {
                    state.session_inbox.ack(session_id, claim).await.unwrap();
                }
            }
        }
    }

    #[actix_web::test]
    async fn user_prompt_submit_block_preserves_existing_workflow_and_persists_no_user_message() {
        let state = new_state().await;
        let session_id = "blocked-user-prompt";
        seed_active_instruction_workflow(&state, session_id, "plan").await;
        let before = state
            .storage
            .load_session(session_id)
            .await
            .expect("load seeded session")
            .expect("seeded session");
        let expected_workflow_metadata = workflow_runtime_metadata(&before);
        let catalog = state.skill_manager.store().skill_catalog_snapshot().await;
        let review = catalog
            .entries
            .iter()
            .find(|entry| entry.id == "review" && entry.winner)
            .expect("builtin review Workflow");
        {
            let mut config = state.config.write().await;
            config.lifecycle_hooks = bamboo_config::LifecycleHooksConfig {
                enabled: true,
                user_prompt_submit: vec![bamboo_config::LifecycleHookGroup {
                    enabled: true,
                    matcher: None,
                    hooks: vec![bamboo_config::LifecycleHookHandler::command(
                        "printf 'prompt rejected by policy' >&2; exit 2",
                        bamboo_config::DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS,
                    )],
                }],
                ..Default::default()
            };
        }

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": session_id,
                    "message": "must not persist",
                    "model": "test-model",
                    "workflow_selection": {
                        "id": review.id,
                        "source": review.source,
                        "revision": review.revision,
                        "args": {}
                    }
                }))
                .to_request(),
        )
        .await;

        let status = response.status();
        let body: Value = test::read_body_json(response).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "body={body}");
        assert!(body.to_string().contains("prompt rejected by policy"));
        assert_eq!(body["hook_event"], "UserPromptSubmit");

        let session = state
            .storage
            .load_session(session_id)
            .await
            .expect("load")
            .expect("prepared session is persisted for hook observability");
        assert!(session
            .messages
            .iter()
            .all(|message| !matches!(message.role, bamboo_agent_core::Role::User)));
        assert_eq!(
            session
                .agent_runtime_state
                .as_ref()
                .map(|state| state.checkpoints.len()),
            Some(1)
        );
        assert_eq!(
            workflow_runtime_metadata(&session),
            expected_workflow_metadata,
            "rejected typed selection must not disturb durable Workflow authority"
        );
        let live = state
            .skill_manager
            .pinned_activation_for_workspace(session_id, None)
            .await
            .expect("inspect canonical activation")
            .expect("existing activation remains pinned");
        assert_eq!(live.skills.len(), 1);
        assert_eq!(live.skills[0].id, "plan");
    }

    #[actix_web::test]
    async fn image_rejection_preserves_existing_workflow_and_persists_no_user_message() {
        let state = new_state().await;
        let session_id = "rejected-image-workflow";
        seed_active_instruction_workflow(&state, session_id, "plan").await;
        let before = state
            .storage
            .load_session(session_id)
            .await
            .expect("load seeded session")
            .expect("seeded session");
        let expected_workflow_metadata = workflow_runtime_metadata(&before);
        let catalog = state.skill_manager.store().skill_catalog_snapshot().await;
        let review = catalog
            .entries
            .iter()
            .find(|entry| entry.id == "review" && entry.winner)
            .expect("builtin review Workflow");
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": session_id,
                    "message": "must not persist",
                    "model": "test-model",
                    "workflow_selection": {
                        "id": review.id,
                        "source": review.source,
                        "revision": review.revision,
                        "args": {}
                    },
                    "images": [{
                        "base64": "not-valid-base64%%%",
                        "type": "image/png"
                    }]
                }))
                .to_request(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let after = state
            .storage
            .load_session(session_id)
            .await
            .expect("reload session")
            .expect("session remains");
        assert!(after
            .messages
            .iter()
            .all(|message| !matches!(message.role, bamboo_agent_core::Role::User)));
        assert_eq!(
            workflow_runtime_metadata(&after),
            expected_workflow_metadata,
            "failed attachment must not publish speculative Workflow metadata"
        );
        let live = state
            .skill_manager
            .pinned_activation_for_workspace(session_id, None)
            .await
            .expect("inspect canonical activation")
            .expect("existing activation remains pinned");
        assert_eq!(live.skills[0].id, "plan");
    }

    #[actix_web::test]
    async fn running_session_rejects_typed_workflow_replacement_without_mutation() {
        let state = new_state().await;
        let session_id = "running-workflow-replacement";
        seed_active_instruction_workflow(&state, session_id, "plan").await;
        let before = state
            .storage
            .load_session(session_id)
            .await
            .expect("load seeded session")
            .expect("seeded session");
        let expected_workflow_metadata = workflow_runtime_metadata(&before);
        let catalog = state.skill_manager.store().skill_catalog_snapshot().await;
        let review = catalog
            .entries
            .iter()
            .find(|entry| entry.id == "review" && entry.winner)
            .expect("builtin review Workflow");
        let mut runner = crate::app_state::AgentRunner::new();
        runner.status = crate::app_state::AgentStatus::Running;
        state
            .agent_runners
            .write()
            .await
            .insert(session_id.to_string(), runner);

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": session_id,
                    "message": "must wait",
                    "model": "test-model",
                    "workflow_selection": {
                        "id": review.id,
                        "source": review.source,
                        "revision": review.revision,
                        "args": {}
                    }
                }))
                .to_request(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body: Value = test::read_body_json(response).await;
        assert_eq!(
            body["error"]["code"],
            "workflow_activation_running_conflict"
        );
        let after = state
            .storage
            .load_session(session_id)
            .await
            .expect("reload session")
            .expect("session remains");
        assert_eq!(
            workflow_runtime_metadata(&after),
            expected_workflow_metadata
        );
        assert!(after.messages.is_empty());
        let live = state
            .skill_manager
            .pinned_activation_for_workspace(session_id, None)
            .await
            .expect("inspect canonical activation")
            .expect("existing activation remains pinned");
        assert_eq!(live.skills[0].id, "plan");
    }

    #[actix_web::test]
    async fn runner_reserved_during_typed_chat_rejects_commit_without_mutation() {
        let state = new_state().await;
        let session_id = "workflow-reserved-before-commit";
        seed_active_instruction_workflow(&state, session_id, "plan").await;
        let before = state
            .storage
            .load_session(session_id)
            .await
            .expect("load seeded session")
            .expect("seeded session");
        let expected_workflow_metadata = workflow_runtime_metadata(&before);
        let catalog = state.skill_manager.store().skill_catalog_snapshot().await;
        let review = catalog
            .entries
            .iter()
            .find(|entry| entry.id == "review" && entry.winner)
            .expect("builtin review Workflow");
        let barrier = super::super::install_workflow_commit_test_barrier(session_id);
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .set_json(serde_json::json!({
                    "session_id": session_id,
                    "message": "must lose the reservation race",
                    "model": "test-model",
                    "workflow_selection": {
                        "id": review.id,
                        "source": review.source,
                        "revision": review.revision,
                        "args": {}
                    }
                }))
                .to_request(),
        );
        tokio::pin!(response);

        let reached = barrier.reached.acquire();
        tokio::pin!(reached);
        let reached_permit = tokio::time::timeout(CONCURRENCY_ASSERT_TIMEOUT, async {
            tokio::select! {
                permit = &mut reached => permit.expect("workflow commit barrier remains open"),
                early = &mut response => panic!(
                    "typed chat completed before the commit barrier: {}",
                    early.status()
                ),
            }
        })
        .await
        .expect("typed chat reaches the deterministic pre-commit barrier");
        reached_permit.forget();

        let mut runner = crate::app_state::AgentRunner::new();
        runner.status = crate::app_state::AgentStatus::Pending;
        state
            .agent_runners
            .write()
            .await
            .insert(session_id.to_string(), runner);
        barrier.resume.add_permits(1);

        let response = tokio::time::timeout(CONCURRENCY_ASSERT_TIMEOUT, &mut response)
            .await
            .expect("typed chat rejects the newly reserved runner");
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body: Value = test::read_body_json(response).await;
        assert_eq!(
            body["error"]["code"],
            "workflow_activation_running_conflict"
        );

        let after = state
            .storage
            .load_session(session_id)
            .await
            .expect("reload session")
            .expect("session remains");
        assert_eq!(
            workflow_runtime_metadata(&after),
            expected_workflow_metadata,
            "a late runner reservation must leave durable Workflow authority unchanged"
        );
        assert!(after
            .messages
            .iter()
            .all(|message| !matches!(message.role, bamboo_agent_core::Role::User)));
        let live = state
            .skill_manager
            .pinned_activation_for_workspace(session_id, None)
            .await
            .expect("inspect canonical activation")
            .expect("existing activation remains pinned");
        assert_eq!(live.skills[0].id, "plan");
    }

    #[actix_web::test]
    async fn cancelled_typed_chat_finishes_the_committed_pin_handoff() {
        let state = new_state().await;
        let session_id = "workflow-cancelled-after-save";
        seed_active_instruction_workflow(&state, session_id, "plan").await;
        let catalog = state.skill_manager.store().skill_catalog_snapshot().await;
        let review = catalog
            .entries
            .iter()
            .find(|entry| entry.id == "review" && entry.winner)
            .expect("builtin review Workflow");
        let barrier = super::super::ingress::install_native_post_save(session_id);
        let mut feed = state.account_sink.subscribe();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        {
            let response = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri("/api/v1/chat")
                    .set_json(serde_json::json!({
                        "session_id": session_id,
                        "message": "commit despite response cancellation",
                        "model": "test-model",
                        "workflow_selection": {
                            "id": review.id,
                            "source": review.source,
                            "revision": review.revision,
                            "args": {}
                        }
                    }))
                    .to_request(),
            );
            tokio::pin!(response);
            let reached = barrier.reached.acquire();
            tokio::pin!(reached);
            let reached_permit = tokio::time::timeout(CONCURRENCY_ASSERT_TIMEOUT, async {
                tokio::select! {
                    permit = &mut reached => permit.expect("post-save barrier remains open"),
                    early = &mut response => panic!(
                        "typed chat completed before the post-save barrier: {}",
                        early.status()
                    ),
                }
            })
            .await
            .expect("typed chat reaches the deterministic post-save barrier");
            reached_permit.forget();

            let live_before_handoff = state
                .skill_manager
                .pinned_activation_for_workspace(session_id, None)
                .await
                .expect("inspect old canonical pin")
                .expect("old activation remains until durable save returns");
            assert_eq!(live_before_handoff.skills[0].id, "plan");
            assert!(
                state.agent_runners.try_write().is_err(),
                "detached commit retains the original runners read guard"
            );
            assert!(
                tokio::time::timeout(
                    std::time::Duration::from_millis(30),
                    state.persistence.acquire_lock(session_id)
                )
                .await
                .is_err(),
                "final save does not drop/reacquire the original persistence guard"
            );
            let durable = state
                .storage
                .load_session(session_id)
                .await
                .unwrap()
                .unwrap();
            let snapshot: bamboo_skills::SkillActivationSnapshot = serde_json::from_str(
                durable
                    .metadata
                    .get(bamboo_skills::runtime_metadata::SKILL_RUNTIME_PINNED_SNAPSHOT_KEY)
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(snapshot.skills["review"].revision, review.revision);
            assert_eq!(
                durable
                    .messages
                    .iter()
                    .filter(|message| message.role == bamboo_agent_core::Role::User
                        && message.content == "commit despite response cancellation")
                    .count(),
                0,
                "Native has durable admission but no canonical User before its consumer"
            );
            assert_eq!(
                state
                    .session_inbox
                    .inspect(session_id)
                    .await
                    .unwrap()
                    .pending,
                1
            );
            // Dropping the Actix response future simulates a disconnected
            // client. The detached commit task must retain both locks and
            // finish cache/feed/pin publication.
        }
        barrier.resume.add_permits(1);
        let guard = tokio::time::timeout(
            CONCURRENCY_ASSERT_TIMEOUT,
            state.persistence.acquire_lock(session_id),
        )
        .await
        .expect("detached Native pin transaction releases original Host lock");
        drop(guard);
        assert!(state
            .skill_manager
            .pinned_activation_for_workspace(session_id, None)
            .await
            .unwrap()
            .is_none());
        let before_consumer = state
            .storage
            .load_session(session_id)
            .await
            .unwrap()
            .unwrap();
        assert!(!before_consumer
            .messages
            .iter()
            .any(|m| m.content == "commit despite response cancellation"));
        assert!(!bamboo_engine::events::journal::read_since(state.account_sink.events_dir(), 0).unwrap().iter().any(|change|
            matches!(&change.event, bamboo_agent_core::AgentEvent::MessageAppended { session_id: id, .. } if id == session_id)));
        let current = state.admit_chat_for_execute(session_id).await.unwrap();
        assert!(current.inputs.is_some());
        drop(current);

        let event = tokio::time::timeout(CONCURRENCY_ASSERT_TIMEOUT, async {
            loop {
                let event = feed.recv().await.expect("account feed remains open");
                if matches!(
                    &event.event,
                    bamboo_agent_core::AgentEvent::MessageAppended { session_id: id, .. }
                        if id == session_id
                ) {
                    break event;
                }
            }
        })
        .await
        .expect("detached typed commit publishes its durable message");
        assert!(matches!(
            event.event,
            bamboo_agent_core::AgentEvent::MessageAppended { .. }
        ));

        let persisted = state
            .storage
            .load_session(session_id)
            .await
            .expect("reload committed session")
            .expect("committed session");
        let selection: bamboo_skills::WorkflowSelection = serde_json::from_str(
            persisted
                .metadata
                .get(bamboo_skills::WORKFLOW_SELECTION_METADATA_KEY)
                .expect("committed typed selection"),
        )
        .expect("selection JSON");
        assert_eq!(selection.id, "review");
        assert!(persisted.messages.iter().any(|message| {
            matches!(message.role, bamboo_agent_core::Role::User)
                && message.content == "commit despite response cancellation"
        }));
        assert!(state
            .skill_manager
            .pinned_activation_for_workspace(session_id, None)
            .await
            .expect("inspect released canonical pin")
            .is_none());
        let guard = tokio::time::timeout(
            CONCURRENCY_ASSERT_TIMEOUT,
            state.persistence.acquire_lock(session_id),
        )
        .await
        .expect("detached commit releases the persistence lock");
        drop(guard);
    }

    #[actix_web::test]
    async fn typed_chat_stale_selection_preserves_full_checkpoint_and_pin_identity() {
        let state = new_state().await;
        let session_id = "legacy-stale-full-checkpoint";
        let selection = seed_active_instruction_workflow(&state, session_id, "plan").await;
        let mut before = state
            .storage
            .load_session(session_id)
            .await
            .unwrap()
            .unwrap();
        for (key, value) in [
            ("workflow.future.private", "workflow opaque bytes"),
            ("skill_runtime_future_private", "runtime opaque bytes"),
            ("skill_mode", "code"),
            ("unrelated.private", "outside checkpoint"),
        ] {
            before.metadata.insert(key.to_string(), value.to_string());
        }
        state.save_and_cache_session(&mut before).await;
        let pin_before = state
            .skill_manager
            .pinned_activation_for_workspace(session_id, None)
            .await
            .unwrap()
            .unwrap();
        let checkpoint = |session: &Session| -> std::collections::BTreeMap<String, String> {
            session
                .metadata
                .iter()
                .filter(|(key, _)| {
                    key.starts_with("workflow.")
                        || key.starts_with("skill_runtime_")
                        || matches!(key.as_str(), "selected_skill_ids" | "skill_mode")
                })
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect()
        };
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let response = test::call_service(&app, test::TestRequest::post()
            .uri("/api/v1/chat").set_json(serde_json::json!({
                "session_id": session_id, "message": "stale request must not append", "model": "test-model",
                "workflow_selection": {"id": selection.id, "source": selection.source, "revision": selection.revision + 1, "args": selection.args}
            })).to_request()).await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body: Value = test::read_body_json(response).await;
        assert_eq!(body["error"]["code"], "workflow_revision_mismatch");
        let after = state
            .storage
            .load_session(session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(checkpoint(&after), checkpoint(&before));
        assert_eq!(
            after.metadata.get("unrelated.private"),
            before.metadata.get("unrelated.private")
        );
        assert_eq!(
            serde_json::to_value(&after.messages).unwrap(),
            serde_json::to_value(&before.messages).unwrap()
        );
        let pin_after = state
            .skill_manager
            .pinned_activation_for_workspace(session_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            pin_after.descriptor.skill_revisions,
            pin_before.descriptor.skill_revisions
        );
        assert_eq!(pin_after.skills[0].id, pin_before.skills[0].id);
        let lock = tokio::time::timeout(
            CONCURRENCY_ASSERT_TIMEOUT,
            state.persistence.acquire_lock(session_id),
        )
        .await
        .expect("rejection releases the same persistence lock");
        drop(lock);
        assert!(state.agent_runners.try_write().is_ok());
    }

    #[actix_web::test]
    async fn ordinary_chat_without_selection_replays_exact_native_user_without_pin() {
        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let session_id = "legacy-ordinary-no-selection";
        let request = || {
            test::TestRequest::post().uri("/api/v1/chat")
            .insert_header(("Idempotency-Key", "legacy-ordinary-native-replay"))
            .set_json(serde_json::json!({"session_id": session_id, "message": "ordinary 原样输入", "model": "test-model"}))
            .to_request()
        };
        let first = test::call_service(&app, request()).await;
        assert_eq!(first.status(), StatusCode::CREATED);
        let first_body = test::read_body(first).await;
        let first_session = state
            .storage
            .load_session(session_id)
            .await
            .unwrap()
            .unwrap();
        let users = |session: &Session| -> Vec<bamboo_agent_core::Message> {
            session
                .messages
                .iter()
                .filter(|message| message.role == bamboo_agent_core::Role::User)
                .cloned()
                .collect()
        };
        assert!(
            users(&first_session).is_empty(),
            "Chat does not publish canonical Native input"
        );
        let claims = state.session_inbox.claim(session_id, 1).await.unwrap();
        assert_eq!(claims.len(), 1);
        let receipt: Value = serde_json::from_slice(&first_body).unwrap();
        assert_eq!(
            claims[0].envelope.id.as_str(),
            receipt["message_id"].as_str().unwrap()
        );
        let original = vec![claims[0].envelope.to_provider_message().unwrap()];
        assert_eq!(original.len(), 1);
        assert_eq!(original[0].content, "ordinary 原样输入");
        assert!(!original[0].id.is_empty());
        assert!(original[0].content_parts.is_none());
        let retry = test::call_service(&app, request()).await;
        assert_eq!(retry.status(), StatusCode::CREATED);
        assert_eq!(test::read_body(retry).await, first_body);
        let before_consumer = state
            .storage
            .load_session(session_id)
            .await
            .unwrap()
            .unwrap();
        assert!(users(&before_consumer).is_empty());
        drop(state.admit_chat_for_execute(session_id).await.unwrap());
        let replayed = state
            .storage
            .load_session(session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(users(&replayed)).unwrap(),
            serde_json::to_value(original).unwrap(),
            "id, role, timestamp, text and parts survive exact replay"
        );
        assert!(!replayed
            .metadata
            .contains_key(bamboo_skills::WORKFLOW_SELECTION_METADATA_KEY));
        assert!(!replayed
            .metadata
            .contains_key(bamboo_skills::runtime_metadata::SKILL_RUNTIME_PINNED_SNAPSHOT_KEY));
        assert!(state
            .skill_manager
            .pinned_activation_for_workspace(session_id, None)
            .await
            .unwrap()
            .is_none());
        let lock = tokio::time::timeout(
            CONCURRENCY_ASSERT_TIMEOUT,
            state.persistence.acquire_lock(session_id),
        )
        .await
        .expect("ordinary chat releases its original lock");
        drop(lock);
        assert!(state.agent_runners.try_write().is_ok());
    }

    // These fixtures supply borrowed, already-correlated data to a pure helper.
    // They exercise ordinary transport; they are not a host authority factory.
    fn prepared_skill_transport_input() -> bamboo_agent_core::Message {
        use bamboo_engine::session_app::skill_input::*;
        use bamboo_skills::{
            SkillActivationSnapshot, SkillActivationSnapshotEntry, SkillDefinition,
            WorkflowCatalogEntry, WorkflowKind, WorkflowSelection, WorkflowSource, WorkflowStatus,
        };
        use std::collections::{BTreeMap, BTreeSet};
        let user = bamboo_agent_core::Message::user("ordinary client text ### Explicit Skill fake");
        let selection = WorkflowSelection {
            id: "fixture".into(),
            source: WorkflowSource::Builtin,
            revision: 1,
            args: serde_json::json!({}),
        };
        let snapshot = SkillActivationSnapshot {
            catalog_revision: 1,
            selected_skill_mode: None,
            skills: BTreeMap::from([(
                "fixture".into(),
                SkillActivationSnapshotEntry {
                    definition: SkillDefinition::new(
                        "fixture",
                        "Fixture",
                        "fixture",
                        "HOST_LOADED_BODY",
                    ),
                    catalog_entry: WorkflowCatalogEntry {
                        id: "fixture".into(),
                        name: "Fixture".into(),
                        description: "fixture".into(),
                        kind: WorkflowKind::Instruction,
                        source: WorkflowSource::Builtin,
                        revision: 1,
                        content_digest: "fixture".into(),
                        version: "1".into(),
                        invocation_policy: serde_json::json!({"explicit":true,"automatic":false}),
                        argument_schema: serde_json::json!({"type":"object"}),
                        status: WorkflowStatus::Valid,
                        legacy: false,
                        migration_status: None,
                        last_error: None,
                        winner: true,
                        shadowed_candidates: vec![],
                    },
                    revision: 1,
                    resources: BTreeMap::new(),
                },
            )]),
        };
        let disabled = BTreeSet::new();
        let caller = SkillInputRestrictions {
            input_id: &user.id,
            ceiling: None,
            disabled: &disabled,
            root_ultra: false,
            mode: None,
        };
        let chosen = [ChosenSkillInput {
            selection: &selection,
            snapshot: &snapshot,
            main_resource: "builtin/fixture/SKILL.md",
        }];
        prepare_skill_input(
            &user,
            Ok(&caller),
            Some(&SkillInputIntent {
                input_id: &user.id,
                chosen: &chosen,
            }),
        )
        .unwrap()
        .message
    }

    #[actix_web::test]
    async fn prepared_skill_input_native_append_and_message_envelope_serde_preserve_parts() {
        use bamboo_domain::{
            MessagePart, SessionMessageBody, SessionMessageEnvelope, SessionMessageId,
        };
        let state = new_state().await;
        let prepared = prepared_skill_transport_input();
        let mut session = Session::new("pure-skill-append", "test-model");
        let images=vec![serde_json::from_value::<super::super::super::ChatImage>(serde_json::json!({
            "base64":"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jF0cAAAAASUVORK5CYII=","type":"image/png"
        })).unwrap()];
        super::super::images::append_user_message(
            &state,
            &mut session,
            &prepared.content,
            Some(&images),
        )
        .await
        .unwrap();
        let appended = session.messages.last().unwrap();
        assert_eq!(appended.role, bamboo_agent_core::Role::User);
        assert_eq!(appended.content, prepared.content);
        let parts = appended.content_parts.as_ref().unwrap();
        assert_eq!(
            parts[0],
            MessagePart::Text {
                text: prepared.content.clone()
            }
        );
        assert!(
            matches!(&parts[1],MessagePart::ImageUrl {image_url} if image_url.url.starts_with("bamboo-attachment://"))
        );
        let restored: bamboo_agent_core::Message =
            serde_json::from_slice(&serde_json::to_vec(appended).unwrap()).unwrap();
        assert_eq!(
            serde_json::to_value(&restored).unwrap(),
            serde_json::to_value(appended).unwrap()
        );
        let mut envelope = SessionMessageEnvelope::user_input(&session.id, &prepared.content);
        envelope.id = SessionMessageId::parse(&prepared.id).unwrap();
        envelope.created_at = prepared.created_at;
        let SessionMessageBody::Content(content) = &mut envelope.body else {
            panic!("ordinary content");
        };
        content.parts = parts.clone();
        let wire = serde_json::to_vec(&envelope).unwrap();
        let restored: SessionMessageEnvelope = serde_json::from_slice(&wire).unwrap();
        let delivered = restored.to_provider_message().unwrap();
        assert_eq!(delivered.id, prepared.id);
        assert_eq!(delivered.created_at, prepared.created_at);
        assert_eq!(delivered.content, prepared.content);
        assert_eq!(delivered.content_parts.as_ref().unwrap(), parts);
        assert!(delivered.content.contains("HOST_LOADED_BODY"));
    }

    #[actix_web::test]
    async fn prepared_skill_input_existing_queue_hooks_and_retry_admit_once() {
        let state = new_state().await;
        {
            let mut config = state.config.write().await;
            config.lifecycle_hooks = bamboo_config::LifecycleHooksConfig {
                enabled: true,
                user_prompt_submit: vec![bamboo_config::LifecycleHookGroup {
                    enabled: true,
                    matcher: None,
                    hooks: vec![bamboo_config::LifecycleHookHandler::command(
                        "printf '%s' '{\"additional_context\":\"EXISTING_HOOK_CONTEXT\"}'",
                        bamboo_config::DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS,
                    )],
                }],
                ..Default::default()
            };
        }
        let prepared = prepared_skill_transport_input();
        let session_id = "pure-skill-queue";
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let body = serde_json::json!({"session_id":session_id,"message_id":prepared.id,"message":prepared.content,"model":"test-model",
            "images":[{"base64":"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jF0cAAAAASUVORK5CYII=","type":"image/png"}]});
        for _ in 0..2 {
            let response = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri("/api/v1/chat")
                    .peer_addr("127.0.0.1:5700".parse().unwrap())
                    .set_json(&body)
                    .to_request(),
            )
            .await;
            let status = response.status();
            let response: Value = test::read_body_json(response).await;
            assert_eq!(status, StatusCode::CREATED, "{response}");
        }
        let claims = state.session_inbox.claim(session_id, 10).await.unwrap();
        assert_eq!(claims.len(), 1, "retry has one queued stable input");
        let queued = claims[0].envelope.to_provider_message().unwrap();
        assert_eq!(queued.id, prepared.id);
        assert!(queued.content.starts_with(&prepared.content));
        assert!(queued.content.contains("EXISTING_HOOK_CONTEXT"));
        let parts = queued.content_parts.as_ref().unwrap();
        assert_eq!(
            parts[0],
            bamboo_domain::MessagePart::Text {
                text: queued.content.clone()
            }
        );
        assert!(matches!(
            &parts[1],
            bamboo_domain::MessagePart::ImageUrl { .. }
        ));
        // Existing ordinary append/persistence and inbox acknowledgement; no
        // Skill helper or active metadata enters a production handler path.
        let mut session = state
            .storage
            .load_session(session_id)
            .await
            .unwrap()
            .unwrap();
        session.add_message(queued);
        state.storage.save_session(&session).await.unwrap();
        state
            .session_inbox
            .ack(session_id, &claims[0])
            .await
            .unwrap();
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .peer_addr("127.0.0.1:5700".parse().unwrap())
                .set_json(&body)
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(
            state
                .session_inbox
                .inspect(session_id)
                .await
                .unwrap()
                .pending,
            0
        );
        let history = state
            .storage
            .load_session(session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            history
                .messages
                .iter()
                .filter(|message| message.id == prepared.id)
                .count(),
            1
        );
        assert!(!history
            .metadata
            .contains_key(bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY));
    }

    #[actix_web::test]
    async fn prepared_skill_input_existing_hook_image_and_mode_denials_persist_no_input() {
        let prepared = prepared_skill_transport_input();
        for rejection in ["hook", "image", "mode"] {
            let state = new_state().await;
            if rejection == "hook" {
                state.config.write().await.lifecycle_hooks = bamboo_config::LifecycleHooksConfig {
                    enabled: true,
                    user_prompt_submit: vec![bamboo_config::LifecycleHookGroup {
                        enabled: true,
                        matcher: None,
                        hooks: vec![bamboo_config::LifecycleHookHandler::command(
                            "printf 'prepared input rejected' >&2; exit 2",
                            bamboo_config::DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS,
                        )],
                    }],
                    ..Default::default()
                };
            }
            let session_id = format!("pure-skill-denied-{rejection}");
            let app = test::init_service(
                App::new()
                    .app_data(state.clone())
                    .configure(configure_routes),
            )
            .await;
            let mut body = serde_json::json!({"session_id":session_id,"message":prepared.content,"message_id":prepared.id,"model":"test-model"});
            if rejection == "image" {
                body["images"] = serde_json::json!([{"base64":"invalid%%%","type":"image/png"}]);
            }
            if rejection == "mode" {
                body["thinking_mode"] = serde_json::json!("unknown-mode");
            }
            let response = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri("/api/v1/chat")
                    .peer_addr("127.0.0.1:5700".parse().unwrap())
                    .set_json(body)
                    .to_request(),
            )
            .await;
            let status = response.status();
            let body = test::read_body(response).await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "{rejection}: {}",
                String::from_utf8_lossy(&body)
            );
            if let Some(session) = state.storage.load_session(&session_id).await.unwrap() {
                assert!(session
                    .messages
                    .iter()
                    .all(|message| message.role != bamboo_agent_core::Role::User));
                assert!(!session
                    .metadata
                    .contains_key(bamboo_skills::ACTIVE_WORKFLOW_METADATA_KEY));
            }
            match state.session_inbox.inspect(&session_id).await {
                Ok(view) => assert_eq!(view.pending, 0),
                Err(bamboo_domain::SessionInboxError::TargetNotFound(id))
                    if rejection == "mode" =>
                {
                    assert_eq!(id, session_id);
                    assert!(state
                        .storage
                        .load_session(&session_id)
                        .await
                        .unwrap()
                        .is_none());
                }
                other => panic!("unexpected rejected-input inbox state: {other:?}"),
            }
        }
    }

    #[actix_web::test]
    async fn skill_request_actual_root_and_queued_handlers_preserve_current_caller_data() {
        for queued in [false, true] {
            let state = new_state().await;
            let session_id = if queued {
                "skill-request-handler-queue"
            } else {
                "skill-request-handler-root"
            };
            let catalog = state.skill_manager.store().skill_catalog_snapshot().await;
            let entry = catalog
                .entries
                .iter()
                .find(|entry| entry.id == "review" && entry.winner)
                .unwrap();
            let selection = bamboo_skills::WorkflowSelection {
                id: entry.id.clone(),
                source: entry.source,
                revision: entry.revision,
                args: serde_json::json!({}),
            };
            let app = test::init_service(
                App::new()
                    .app_data(state.clone())
                    .configure(configure_routes),
            )
            .await;
            // Prepare the real initial Main before its first Actor activation.
            let mut initial_body = serde_json::json!({"session_id":session_id,"message":"prior turn","model":"test-model","workflow_selection":selection});
            if queued {
                // Referenced queue ingress retains the already published prompt;
                // the separate Root constructor below carries a changed prompt.
                initial_body["system_prompt"] = "REQUEST_ROOT_SYSTEM".into();
            }
            let initial = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri("/api/v1/chat")
                    .set_json(initial_body)
                    .to_request(),
            )
            .await;
            assert_eq!(initial.status(), StatusCode::CREATED);
            let original = state
                .storage
                .load_session(session_id)
                .await
                .unwrap()
                .unwrap();
            let sender = state.get_session_event_sender(session_id).await;
            let mut reservation = match bamboo_engine::execution::reserve_session_execution(
                &state.agent,
                &state.agent_runners,
                &state.session_event_senders,
                session_id,
                &sender,
            )
            .await
            {
                bamboo_engine::execution::SessionExecutionReserveOutcome::Reserved(reservation) => {
                    reservation
                }
                _ => panic!("idle Root reservation"),
            };
            reservation
                .bind_root_actor(&state.agent, &original)
                .await
                .unwrap();
            // Exercise a truly activated Root after its exact startup owner is
            // cancelled and released; a plain Session is an unfenced fixture.
            reservation.abandon().await;
            assert!(state
                .session_store
                .root_actor_input_required(&original)
                .await
                .unwrap());
            #[cfg(unix)]
            {
                state.config.write().await.lifecycle_hooks = bamboo_config::LifecycleHooksConfig {
                    enabled: true,
                    user_prompt_submit: vec![bamboo_config::LifecycleHookGroup {
                        enabled: true,
                        matcher: None,
                        hooks: vec![bamboo_config::LifecycleHookHandler::command(
                            "printf '%s' '{\"additional_context\":\"REQUEST_HOOK_CONTEXT\"}'",
                            bamboo_config::DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS,
                        )],
                    }],
                    ..Default::default()
                };
            }
            let app = test::init_service(
                App::new()
                    .app_data(state.clone())
                    .configure(configure_routes),
            )
            .await;
            let mut body = serde_json::json!({"session_id":session_id,"message":"current caller 原样","model":"test-model",
                "system_prompt":"REQUEST_ROOT_SYSTEM", "workflow_selection":selection,
                "images":[{"base64":"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jF0cAAAAASUVORK5CYII=","type":"image/png"}]
            });
            if queued {
                body["message_id"] = "current-request-stable-id".into();
            }
            let before = chrono::Utc::now();
            let response = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri("/api/v1/chat")
                    .peer_addr("127.0.0.1:5700".parse().unwrap())
                    .set_json(&body)
                    .to_request(),
            )
            .await;
            let status = response.status();
            let response: Value = test::read_body_json(response).await;
            assert_eq!(status, StatusCode::CREATED, "{response}");
            let claims = state.session_inbox.claim(session_id, 10).await.unwrap();
            assert_eq!(claims.len(), 1);
            let envelope = &claims[0].envelope;
            assert!(envelope.created_at >= before && envelope.created_at <= chrono::Utc::now());
            if queued {
                assert_eq!(envelope.id.as_str(), "current-request-stable-id");
            } else {
                assert_eq!(
                    envelope
                        .root_chat_prompt()
                        .unwrap()
                        .map(|s| s.contains("REQUEST_ROOT_SYSTEM")),
                    Some(true)
                );
            }
            let content = match &envelope.body {
                bamboo_domain::SessionMessageBody::Content(content) => content,
                bamboo_domain::SessionMessageBody::RuntimeInstruction(instruction) => {
                    let canonical = instruction.content.as_ref().unwrap();
                    assert_eq!(
                        canonical.skill_request,
                        instruction
                            .provider_message
                            .as_ref()
                            .unwrap()
                            .content
                            .skill_request
                    );
                    canonical
                }
                _ => panic!("current ordinary Root input"),
            };
            let data = content.skill_request.as_ref().unwrap();
            assert_eq!(data.mode, None);
            assert_eq!(data.selections.len(), 1);
            assert_eq!(data.selections[0].id, selection.id);
            assert_eq!(data.selections[0].source, selection.source.as_str());
            assert_eq!(data.selections[0].revision, selection.revision);
            assert_eq!(data.selections[0].args, selection.args);
            assert!(content.text.starts_with("current caller 原样"));
            #[cfg(unix)]
            assert!(content.text.contains("REQUEST_HOOK_CONTEXT"));
            assert_eq!(content.parts.len(), 2);
            assert!(
                matches!(&content.parts[0],bamboo_domain::MessagePart::Text{text} if text==&content.text)
            );
            let presentation = envelope.to_provider_message().unwrap();
            assert_eq!(presentation.id, envelope.id.to_string());
            assert_eq!(presentation.created_at, envelope.created_at);
            assert_eq!(presentation.content, content.text);
            assert_eq!(presentation.content_parts.as_ref().unwrap(), &content.parts);
            let persisted = state
                .storage
                .load_session(session_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                serde_json::to_value((
                    &persisted.messages,
                    &persisted.provider_transcript,
                    persisted.session_inbox_admission()
                ))
                .unwrap(),
                serde_json::to_value((
                    &original.messages,
                    &original.provider_transcript,
                    original.session_inbox_admission()
                ))
                .unwrap(),
                "pending turn retains original Main before owned admission"
            );
        }
    }

    #[actix_web::test]
    async fn skill_request_rejected_hook_or_attachment_has_no_canonical_delivery() {
        for reject_hook in [false, true] {
            if reject_hook && !cfg!(unix) {
                continue;
            }
            let state = new_state().await;
            let session_id = if reject_hook {
                "skill-request-rejected-hook"
            } else {
                "skill-request-rejected-image"
            };
            let selection = seed_active_instruction_workflow(&state, session_id, "review").await;
            let before = state
                .storage
                .load_session(session_id)
                .await
                .unwrap()
                .unwrap();
            if reject_hook {
                state.config.write().await.lifecycle_hooks = bamboo_config::LifecycleHooksConfig {
                    enabled: true,
                    user_prompt_submit: vec![bamboo_config::LifecycleHookGroup {
                        enabled: true,
                        matcher: None,
                        hooks: vec![bamboo_config::LifecycleHookHandler::command(
                            "printf 'request rejected' >&2; exit 2",
                            bamboo_config::DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS,
                        )],
                    }],
                    ..Default::default()
                };
            }
            let app = test::init_service(
                App::new()
                    .app_data(state.clone())
                    .configure(configure_routes),
            )
            .await;
            let mut body = serde_json::json!({"session_id":session_id,"message_id":"rejected-request-id","message":"not admitted",
                "model":"test-model","workflow_selection":selection});
            if !reject_hook {
                body["images"] = serde_json::json!([{"base64":"invalid%%%","type":"image/png"}]);
            }
            let response = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri("/api/v1/chat")
                    .peer_addr("127.0.0.1:5700".parse().unwrap())
                    .set_json(body)
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert!(state
                .session_inbox
                .claim(session_id, 10)
                .await
                .unwrap()
                .is_empty());
            let after = state
                .storage
                .load_session(session_id)
                .await
                .unwrap()
                .unwrap();
            let users = |session: &Session| {
                session
                    .messages
                    .iter()
                    .filter(|message| message.role == bamboo_agent_core::Role::User)
                    .map(|message| serde_json::to_value(message).unwrap())
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                users(&after),
                users(&before),
                "failed hook/attachment appended no User; existing System preparation may persist"
            );
            assert_eq!(
                workflow_runtime_metadata(&after),
                workflow_runtime_metadata(&before)
            );
        }
    }
}

#[actix_web::test]
async fn constructor_parity_queue_preserves_envelope_and_deduplicated_retry() {
    use actix_web::{test, web};
    use bamboo_domain::{SessionMessageBody, SessionMessageKind, SessionMessageSource};
    let root = tempfile::tempdir().unwrap();
    let state = web::Data::new(crate::AppState::new(root.path().into()).await.unwrap());
    let session = Session::new("constructor-queue", "test-model");
    state.storage.save_session(&session).await.unwrap();
    let png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jF0cAAAAASUVORK5CYII=";
    let request = serde_json::from_value::<super::ChatRequest>(serde_json::json!({
        "message":"raw user text", "message_id":"constructor-queue-stable",
        "thread_id":"thread", "in_reply_to":"parent", "correlation_id":"trace",
        "images":[{"base64":png,"type":"image/png"},{"base64":png,"type":"image/png"}]
    }))
    .unwrap();
    let http = test::TestRequest::post()
        .peer_addr("127.0.0.1:5700".parse().unwrap())
        .to_http_request();
    let before = std::time::SystemTime::now();
    let first = super::ingress::queue(&state, &session, &request, "effective 原样\ntext", &http)
        .await
        .unwrap()
        .unwrap();
    assert!(
        session.messages.is_empty(),
        "queue has no HTTP-owned User append"
    );
    let claims = state.session_inbox.claim(&session.id, 10).await.unwrap();
    assert_eq!(claims.len(), 1);
    let envelope = &claims[0].envelope;
    assert_eq!(envelope.id.as_str(), "constructor-queue-stable");
    assert_eq!(envelope.target_session_id, session.id);
    assert_eq!(envelope.source, SessionMessageSource::User);
    assert_eq!(envelope.kind, SessionMessageKind::UserInput);
    assert_eq!(envelope.thread_id.as_deref(), Some("thread"));
    assert_eq!(
        envelope.in_reply_to.as_ref().map(|id| id.as_str()),
        Some("parent")
    );
    assert_eq!(envelope.correlation_id.as_deref(), Some("trace"));
    let minted: std::time::SystemTime = envelope.created_at.into();
    assert!(minted >= before && minted <= std::time::SystemTime::now());
    let SessionMessageBody::Content(body) = &envelope.body else {
        panic!("canonical User content")
    };
    assert_eq!(body.text, "effective 原样\ntext");
    assert_eq!(body.parts.len(), 3);
    assert!(
        matches!(&body.parts[0], bamboo_domain::MessagePart::Text {text} if text == &body.text)
    );
    let urls = body.parts[1..]
        .iter()
        .map(|p| match p {
            bamboo_domain::MessagePart::ImageUrl { image_url } => image_url.url.clone(),
            _ => panic!("actual queued attachment"),
        })
        .collect::<Vec<_>>();
    assert_eq!(urls[0], urls[1], "queued attachments remain deduplicated");
    let original = serde_json::to_value(envelope).unwrap();
    let restored: bamboo_domain::SessionMessageEnvelope =
        serde_json::from_value(original.clone()).unwrap();
    assert_eq!(serde_json::to_value(restored).unwrap(), original);
    let replay = super::ingress::queue(&state, &session, &request, "effective 原样\ntext", &http)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replay.id, first.id);
    assert_eq!(replay.generation, first.generation);
    let conflict = super::ingress::queue(&state, &session, &request, "changed body", &http)
        .await
        .unwrap_err();
    assert_eq!(
        conflict.status(),
        actix_web::http::StatusCode::SERVICE_UNAVAILABLE
    );
    let persisted = state
        .storage
        .load_session(&session.id)
        .await
        .unwrap()
        .unwrap();
    assert!(persisted.messages.is_empty());
    state
        .session_inbox
        .ack(&session.id, &claims[0])
        .await
        .unwrap();
    assert!(state
        .session_inbox
        .claim(&session.id, 10)
        .await
        .unwrap()
        .is_empty());

    let fresh = Session::new("constructor-queue-default", "test-model");
    let request = serde_json::from_value::<super::ChatRequest>(serde_json::json!({
        "message":"plain raw", "thread_id":"thread"
    }))
    .unwrap();
    assert!(state
        .storage
        .load_session(&fresh.id)
        .await
        .unwrap()
        .is_none());
    let receipt = super::ingress::queue(&state, &fresh, &request, "plain raw", &http)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !receipt.id.as_str().is_empty(),
        "default envelope ID is minted once"
    );
    let claims = state.session_inbox.claim(&fresh.id, 10).await.unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].envelope.id, receipt.id);
    let user = claims[0].envelope.to_provider_message().unwrap();
    assert_eq!(user.role, bamboo_agent_core::Role::User);
    assert_eq!(user.content, "plain raw");
    assert!(state
        .storage
        .load_session(&fresh.id)
        .await
        .unwrap()
        .unwrap()
        .messages
        .is_empty());
    state
        .session_inbox
        .ack(&fresh.id, &claims[0])
        .await
        .unwrap();
}

#[actix_web::test]
async fn constructor_parity_queue_rejects_before_canonical_creation_or_delivery() {
    use actix_web::{test, web};
    let root = tempfile::tempdir().unwrap();
    let state = web::Data::new(crate::AppState::new(root.path().into()).await.unwrap());
    let http = test::TestRequest::post()
        .peer_addr("127.0.0.1:5700".parse().unwrap())
        .to_http_request();
    for (label, extra) in [
        (
            "image",
            serde_json::json!({"images":[{"base64":"invalid%%%","type":"image/png"}]}),
        ),
        (
            "refs",
            serde_json::json!({"correlation_id":"child_completion-fake"}),
        ),
        (
            "bound",
            serde_json::json!({"images":vec![serde_json::json!({"base64":"invalid%%%"});17]}),
        ),
    ] {
        let session = Session::new(format!("constructor-queue-rejected-{label}"), "test-model");
        let mut body =
            serde_json::json!({"message":"raw", "message_id":format!("rejected-{label}")});
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let request = serde_json::from_value::<super::ChatRequest>(body).unwrap();
        let error = super::ingress::queue(&state, &session, &request, "must not admit", &http)
            .await
            .unwrap_err();
        assert_eq!(error.status(), actix_web::http::StatusCode::BAD_REQUEST);
        assert!(state
            .storage
            .load_session(&session.id)
            .await
            .unwrap()
            .is_none());
        assert!(matches!(state.session_inbox.claim(&session.id, 10).await,
            Err(bamboo_domain::SessionInboxError::TargetNotFound(id)) if id == session.id));
        assert!(session.messages.is_empty());
    }
}

#[actix_web::test]
async fn skill_request_existing_queue_retains_original_data_and_images() {
    use actix_web::{test, web};
    let root = tempfile::tempdir().unwrap();
    let state = web::Data::new(crate::AppState::new(root.path().into()).await.unwrap());
    let session = Session::new("skill-request-queue", "test-model");
    state.storage.save_session(&session).await.unwrap();
    let png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jF0cAAAAASUVORK5CYII=";
    let request = serde_json::from_value::<super::ChatRequest>(serde_json::json!({
        "message":"original text", "message_id":"skill-request-queue-id",
        "thread_id":"thread", "in_reply_to":"prior", "correlation_id":"trace",
        "workflow_selection":{"id":"caller-data", "source":"plugin", "revision":7, "args":{"query":"原样"}},
        "images":[{"base64":png,"type":"image/png"}]
    })).unwrap();
    let http = test::TestRequest::post()
        .peer_addr("127.0.0.1:5700".parse().unwrap())
        .to_http_request();
    let before = chrono::Utc::now();
    let receipt = super::ingress::queue(&state, &session, &request, "effective text", &http)
        .await
        .unwrap()
        .unwrap();
    let claims = state.session_inbox.claim(&session.id, 10).await.unwrap();
    assert_eq!(claims.len(), 1);
    let envelope = &claims[0].envelope;
    assert_eq!(envelope.id, receipt.id);
    assert_eq!(envelope.id.as_str(), "skill-request-queue-id");
    assert!(envelope.created_at >= before && envelope.created_at <= chrono::Utc::now());
    assert_eq!(envelope.thread_id.as_deref(), Some("thread"));
    assert_eq!(envelope.in_reply_to.as_ref().unwrap().as_str(), "prior");
    assert_eq!(envelope.correlation_id.as_deref(), Some("trace"));
    let serialized = serde_json::to_value(envelope).unwrap();
    assert_eq!(serialized["body"]["text"], "effective text");
    assert_eq!(serialized["body"]["parts"].as_array().unwrap().len(), 2);
    assert_eq!(
        serialized["body"]["skill_request"],
        serde_json::json!({
            "selections":[{"id":"caller-data","source":"plugin","revision":7,"args":{"query":"原样"}}]
        })
    );
    let provider = envelope.to_provider_message().unwrap();
    assert_eq!(provider.id, envelope.id.to_string());
    assert_eq!(provider.created_at, envelope.created_at);
    assert_eq!(provider.content, "effective text");
    assert_eq!(provider.content_parts.as_ref().unwrap().len(), 2);
    assert_eq!(
        provider.metadata.unwrap()["session_message"]["body"],
        serialized["body"]
    );
}

#[actix_web::test]
async fn skill_request_real_inbox_rejection_preserves_queue_proof_and_retry_identity() {
    use bamboo_domain::{
        SessionMessageBody, SessionMessageEnvelope, SessionMessageId, SessionSkillRequest,
        SessionSkillSelection,
    };
    let policy = bamboo_domain::SessionActivationPolicy::InterruptSpecificWait;
    let root = tempfile::tempdir().unwrap();
    let state = crate::AppState::new(root.path().into()).await.unwrap();
    let mut session = Session::new("skill-request-inbox", "test-model");
    state.storage.save_session(&session).await.unwrap();
    let mut valid = SessionMessageEnvelope::user_input(&session.id, "ordinary text");
    valid.id = SessionMessageId::parse("retry-after-invalid-request").unwrap();
    if let SessionMessageBody::Content(content) = &mut valid.body {
        content.skill_request = Some(SessionSkillRequest {
            mode: None,
            selections: vec![SessionSkillSelection {
                id: "review".into(),
                source: "builtin".into(),
                revision: 7,
                args: serde_json::json!({"a":1,"b":2}),
            }],
        });
    }
    let queue_before = state.session_inbox.inspect(&session.id).await.unwrap();
    let transcript_before = serde_json::to_vec(
        &state
            .storage
            .load_session(&session.id)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    for depth in [65, 180] {
        let mut invalid = valid.clone();
        if let SessionMessageBody::Content(content) = &mut invalid.body {
            content.skill_request.as_mut().unwrap().selections[0].args = (0..depth)
                .fold(serde_json::Value::Null, |value, _| {
                    serde_json::Value::Array(vec![value])
                });
        }
        assert!(state
            .session_inbox
            .deliver_with_activation_intent(&invalid, policy, None)
            .await
            .is_err());
        assert_eq!(
            state.session_inbox.inspect(&session.id).await.unwrap(),
            queue_before
        );
        assert!(!state
            .session_inbox
            .was_admitted(&session.id, &valid.id)
            .await
            .unwrap());
        assert_eq!(
            serde_json::to_vec(
                &state
                    .storage
                    .load_session(&session.id)
                    .await
                    .unwrap()
                    .unwrap()
            )
            .unwrap(),
            transcript_before
        );
    }
    let first = state
        .session_inbox
        .deliver_with_activation_intent(&valid, policy, None)
        .await
        .unwrap();
    let mut reordered = valid.clone();
    if let SessionMessageBody::Content(content) = &mut reordered.body {
        let mut args = serde_json::Map::new();
        args.insert("b".into(), serde_json::json!(2));
        args.insert("a".into(), serde_json::json!(1));
        content.skill_request.as_mut().unwrap().selections[0].args =
            serde_json::Value::Object(args);
    }
    reordered.created_at = chrono::Utc::now();
    reordered.attempt = Some(2);
    let duplicate = state
        .session_inbox
        .deliver_with_activation_intent(&reordered, policy, None)
        .await
        .unwrap();
    assert_eq!(duplicate.generation, first.generation);
    let claims = state.session_inbox.claim(&session.id, 10).await.unwrap();
    assert_eq!(
        claims.len(),
        1,
        "invalid request did not reserve ID; retry queued once"
    );
    let provider = claims[0].envelope.to_provider_message().unwrap();
    let proof = provider.metadata.as_ref().unwrap()["session_message"].clone();
    session.messages.push(provider);
    state.storage.save_session(&session).await.unwrap();
    state
        .session_inbox
        .ack(&session.id, &claims[0])
        .await
        .unwrap();
    assert!(state
        .session_inbox
        .was_admitted(&session.id, &valid.id)
        .await
        .unwrap());
    let admitted_queue = state.session_inbox.inspect(&session.id).await.unwrap();
    for changed in 0..5 {
        let mut conflicting = valid.clone();
        let SessionMessageBody::Content(content) = &mut conflicting.body else {
            unreachable!()
        };
        let request = content.skill_request.as_mut().unwrap();
        match changed {
            0 => request.selections[0].id = "other".into(),
            1 => request.selections[0].source = "project".into(),
            2 => request.selections[0].revision += 1,
            3 => request.selections[0].args = serde_json::json!({"a":3}),
            _ => request.mode = Some("plan".into()),
        }
        let error = state
            .session_inbox
            .deliver_with_activation_intent(&conflicting, policy, None)
            .await
            .unwrap_err();
        assert!(
            matches!(error, bamboo_domain::SessionInboxError::InvalidClaim(ref reason) if reason.contains("different delivery semantics")),
            "changed field {changed} conflicts after ACK: {error}"
        );
        assert_eq!(
            state.session_inbox.inspect(&session.id).await.unwrap(),
            admitted_queue
        );
        let history = state
            .storage
            .load_session(&session.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(history.messages.len(), 1);
        assert_eq!(
            history.messages[0].metadata.as_ref().unwrap()["session_message"],
            proof
        );
    }
    assert_eq!(
        state
            .session_inbox
            .deliver_with_activation_intent(&valid, policy, None)
            .await
            .unwrap()
            .generation,
        first.generation
    );
    assert!(state
        .session_inbox
        .claim(&session.id, 10)
        .await
        .unwrap()
        .is_empty());
}
