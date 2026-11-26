use super::*;
use actix_web::{http::StatusCode, test, web, App};
use bamboo_agent_core::ToolSchema;
use bamboo_domain::{OverlapPolicy, ScheduleTrigger};
use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMStream};
use futures::StreamExt;
use serde_json::{json, Value};
use tokio::sync::oneshot;

struct HeldProvider(mpsc::UnboundedSender<oneshot::Sender<()>>);

#[async_trait::async_trait]
impl LLMProvider for HeldProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        let (release, wait) = oneshot::channel();
        self.0
            .send(release)
            .map_err(|_| LLMError::Api("fixture stopped listening for provider calls".into()))?;
        let first = futures::stream::once(async {
            wait.await
                .map_err(|_| LLMError::Api("fixture dropped provider release".into()))?;
            Ok(LLMChunk::Token("Scheduled response".into()))
        });
        Ok(Box::pin(
            first.chain(futures::stream::iter([Ok(LLMChunk::Done)])),
        ))
    }
}

#[tokio::test]
async fn cancelling_a_live_occurrence_preserves_other_runs_and_the_schedule() {
    let directory = tempfile::tempdir().unwrap();
    let (provider_calls, mut provider_entered) = mpsc::unbounded_channel();
    let mut config = bamboo_llm::Config::default();
    config.features.provider_model_ref = false;
    config
        .memory_mut()
        .get_or_insert_with(Default::default)
        .auto_dream_enabled = false;
    let state = web::Data::new(
        crate::AppState::new_with_provider(
            directory.path().to_path_buf(),
            config,
            Arc::new(HeldProvider(provider_calls)),
        )
        .await
        .unwrap(),
    );
    let schedule = state
        .schedule_store
        .create_schedule_with_definition(
            "Held ordinary schedule".into(),
            true,
            ScheduleRunConfig {
                task_message: Some("Give a short confirmation".into()),
                model: Some("held-schedule-model".into()),
                auto_execute: true,
                ..Default::default()
            },
            crate::schedule_app::store::ScheduleDefinitionChanges {
                trigger: Some(ScheduleTrigger::legacy_interval(3600, None)),
                overlap_policy: Some(OverlapPolicy::Allow),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(crate::routes::configure_routes),
    )
    .await;

    let mut runs = Vec::new();
    for _ in 0..3 {
        let request = test::TestRequest::post()
            .uri(&format!("/api/v1/schedules/{}/run", schedule.id))
            .to_request();
        let response = test::call_service(&app, request).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = test::read_body_json(response).await;
        let run_id = body["run_id"].as_str().unwrap().to_owned();
        let release = tokio::time::timeout(Duration::from_secs(30), provider_entered.recv())
            .await
            .expect("scheduled execution must reach its synthetic provider")
            .unwrap();
        let record = state.schedule_store.get_run_record(&run_id).await.unwrap();
        assert_eq!(record.status, ScheduleRunStatus::Running);
        let session_id = record.session_id.unwrap();
        let finalized = state.agent_runners.read().await[&session_id].wait_for_exact_finalization();
        runs.push((run_id, session_id, release, finalized));
    }
    let [(cancelled, cancelled_session, held_cancelled, cancelled_finalized), (peer, peer_session, release_peer, peer_finalized), (racing, _, release_racing, racing_finalized)] =
        runs.try_into().ok().unwrap();
    assert!(
        !state
            .schedule_manager
            .request_ordinary_run_cancel(&schedule.id, &cancelled, &peer_session)
            .await
    );
    let cancel_request = |run_id: &str| {
        test::TestRequest::post()
            .uri(&format!(
                "/api/v1/schedules/{}/runs/{run_id}/cancel",
                schedule.id
            ))
            .to_request()
    };
    let response = test::call_service(&app, cancel_request(&cancelled)).await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body: Value = test::read_body_json(response).await;
    assert_eq!(
        body,
        json!({"schedule_id": schedule.id, "run_id": cancelled, "status": "cancel_requested"})
    );
    let duplicate = test::call_service(&app, cancel_request(&cancelled)).await;
    let status = duplicate.status();
    let body: Value = test::read_body_json(duplicate).await;
    assert_eq!(body["run_id"], cancelled);
    match status {
        StatusCode::ACCEPTED => assert_eq!(body["status"], "cancel_requested"),
        StatusCode::OK => assert_eq!(body["status"], "cancelled"),
        other => panic!("repeat cancel must acknowledge the same execution: {other} {body}"),
    }
    assert!(!state.agent_runners.read().await[&peer_session]
        .cancel_token
        .is_cancelled());
    release_peer.send(()).unwrap();
    let (request, ()) = tokio::join!(test::call_service(&app, cancel_request(&racing)), async {
        let _ = release_racing.send(());
    });
    assert!(matches!(
        request.status(),
        StatusCode::ACCEPTED | StatusCode::OK
    ));
    tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(cancelled_finalized, peer_finalized, racing_finalized);
    })
    .await
    .expect("each exact execution must finalize");
    drop(held_cancelled);

    let cancelled_record = state
        .schedule_store
        .get_run_record(&cancelled)
        .await
        .unwrap();
    assert_eq!(cancelled_record.status, ScheduleRunStatus::Cancelled);
    assert_eq!(
        cancelled_record.session_id.as_deref(),
        Some(cancelled_session.as_str())
    );
    let peer_record = state.schedule_store.get_run_record(&peer).await.unwrap();
    assert_eq!(peer_record.status, ScheduleRunStatus::Success);
    let raced = state.schedule_store.get_run_record(&racing).await.unwrap();
    assert!(matches!(
        raced.status,
        ScheduleRunStatus::Success | ScheduleRunStatus::Cancelled
    ));
    for record in [cancelled_record, raced] {
        let response = test::call_service(&app, cancel_request(&record.run_id)).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = test::read_body_json(response).await;
        assert_eq!(body["status"], json!(record.status));
        assert_eq!(body["completed_at"], json!(record.completed_at));
    }
    let after = state
        .schedule_store
        .get_schedule(&schedule.id)
        .await
        .unwrap();
    assert!(after.enabled);
    assert_eq!(after.state.next_fire_at, schedule.state.next_fire_at);
    assert_eq!(after.state.running_run_count, 0);
    assert_eq!(after.state.total_run_count, 3);
    assert_eq!(
        after.state.total_success_count + after.state.total_failure_count,
        3
    );
    assert!(state
        .schedule_manager
        .ordinary_run_owners
        .lock()
        .unwrap()
        .is_empty());
    state.shutdown().await;
}

#[tokio::test]
async fn old_occurrence_cancellation_never_signals_a_replacement_generation() {
    let job = ScheduleRunJob {
        run_id: "occurrence".into(),
        schedule_id: "schedule".into(),
        schedule_name: "Schedule".into(),
        run_config: ScheduleRunConfig::default(),
        scheduled_for: Utc::now(),
        claimed_at: Utc::now(),
        was_catch_up: false,
    };
    for acknowledged in [false, true] {
        let (tx, _rx) = mpsc::channel(1);
        let runner = AgentRunner::new();
        let original_token = runner.cancel_token.clone();
        let owner_table = OrdinaryRunOwners::default();
        let owner =
            OrdinaryRunRegistration::new(&owner_table, &job, "same-session", &runner.run_id)
                .unwrap();
        let manager = ScheduleManager {
            tx,
            update_admission: Default::default(),
            ordinary_run_owners: owner_table,
            agent_runners: Arc::new(RwLock::new(HashMap::from([(
                "same-session".into(),
                runner,
            )]))),
            guard_progress: Default::default(),
        };
        if acknowledged {
            assert!(
                manager
                    .request_ordinary_run_cancel("schedule", "occurrence", "same-session")
                    .await
            );
            let runners = manager.agent_runners.read().await;
            assert!(matches!(
                runners["same-session"].status,
                AgentStatus::Pending
            ));
        }
        let successor = AgentRunner::new();
        let successor_token = successor.cancel_token.clone();
        manager
            .agent_runners
            .write()
            .await
            .insert("same-session".into(), successor);
        assert_eq!(
            manager
                .request_ordinary_run_cancel("schedule", "occurrence", "same-session")
                .await,
            acknowledged
        );
        assert_eq!(original_token.is_cancelled(), acknowledged);
        assert!(!successor_token.is_cancelled());
        drop(owner);
        assert!(manager.ordinary_run_owners.lock().unwrap().is_empty());
        assert!(
            !manager
                .request_ordinary_run_cancel("schedule", "occurrence", "same-session")
                .await
        );
    }
}
