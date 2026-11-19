//! Exercise real Session persistence, Inbox admission, and the Root runner.

use super::{
    Due, LoopStatus, PacedLoopCommand, PacedLoopState, PacedLoops, RETRY_DELAY, SYSTEM_GUIDANCE,
};
use crate::app_state::{AgentStatus, AppState};
use actix_web::{test, web, App};
use bamboo_agent_core::tools::ToolSchema;
use bamboo_agent_core::{Message, Role, Session};
use bamboo_domain::session::inbox::{PACED_LOOP_INSTRUCTION, PACED_LOOP_SUBSYSTEM};
use bamboo_domain::session::PACED_LOOP_OUTCOME_METADATA_KEY;
use bamboo_domain::AgentStatusState;
use bamboo_llm::{Config, LLMChunk, LLMError, LLMProvider, LLMStream, ProviderRegistry};
use chrono::TimeDelta;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

const SESSION_ID: &str = "paced-loop-native-root";
const PROVIDER: &str = "paced-loop-test-provider";
const MODEL: &str = "paced-loop-test-model";
const CONTEXT: &str = "original context already in this Session";
const PROMPT: &str = "inspect the synthetic build status";
const HUMAN: &str = "a regular follow-up from the user";
const SUCCESS: &str = "synthetic build check complete";

fn saved_outcome(session: &Session) -> Value {
    serde_json::from_str(&session.metadata[PACED_LOOP_OUTCOME_METADATA_KEY]).unwrap()
}

#[derive(Default)]
struct Probe {
    requests: Mutex<Vec<Vec<Message>>>,
    first_entered: Notify,
    release_first: Notify,
}

#[async_trait::async_trait]
impl LLMProvider for Probe {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        assert_eq!(model, MODEL, "the Root's existing model must survive");
        let ordinal = {
            let mut requests = self.requests.lock().unwrap();
            requests.push(messages.to_vec());
            requests.len()
        };
        if ordinal == 1
            && messages.iter().any(|message| {
                message.role == Role::System && message.content.contains(SYSTEM_GUIDANCE)
            })
        {
            self.first_entered.notify_one();
            self.release_first.notified().await;
            // RequestFailed is terminal for this turn. Only the next due
            // occurrence should call the provider again.
            return Err(LLMError::RequestFailed("synthetic upstream failure".into()));
        }
        Ok(Box::pin(futures::stream::iter([
            Ok(LLMChunk::Token(SUCCESS.into())),
            Ok(LLMChunk::Done),
        ])))
    }
}

struct Fixture {
    state: web::Data<AppState>,
    probe: Arc<Probe>,
    _home: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> Self {
        let home = tempfile::tempdir().unwrap();
        let probe = Arc::new(Probe::default());
        let mut config = Config::from_data_dir(Some(home.path().to_path_buf()));
        config.provider = PROVIDER.into();
        config
            .memory_mut()
            .get_or_insert_with(Default::default)
            .auto_dream_enabled = false;
        // This fixture manually drives the same Host tick using durable due
        // times. Leave the periodic wake scan off to avoid a second driver.
        let state = AppState::new_with_provider_for_legacy_ingress_test(
            home.path().to_path_buf(),
            config,
            probe.clone(),
        )
        .await
        .unwrap();
        state.provider_registry.replace_with(ProviderRegistry::new(
            HashMap::from([(PROVIDER.into(), probe.clone() as Arc<dyn LLMProvider>)]),
            PROVIDER.into(),
        ));
        let workspace = home.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        let workspace = std::fs::canonicalize(workspace).unwrap();
        state
            .skill_manager
            .store_for_workspace(Some(&workspace))
            .await
            .unwrap();
        let mut session = Session::new(SESSION_ID, MODEL);
        session.title_generated = true;
        session.set_workspace_path_meta(workspace.to_string_lossy());
        session.metadata.insert("provider".into(), PROVIDER.into());
        state.storage.save_session(&session).await.unwrap();
        Self {
            state: web::Data::new(state),
            probe,
            _home: home,
        }
    }

    async fn session(&self) -> Session {
        self.state
            .storage
            .load_session(SESSION_ID)
            .await
            .unwrap()
            .unwrap()
    }

    async fn start(&self) -> PacedLoopState {
        self.state
            .apply_paced_loop(
                SESSION_ID,
                PacedLoopCommand::Start {
                    interval_seconds: 60,
                    prompt: PROMPT.into(),
                },
            )
            .await
            .unwrap()
            .unwrap();
        self.loop_state().await
    }

    async fn loop_state(&self) -> PacedLoopState {
        PacedLoopState::read(&self.session().await)
            .unwrap()
            .unwrap()
    }

    async fn settled(&self, expected_requests: usize) -> Session {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let requested = self.probe.requests.lock().unwrap().len() >= expected_requests;
                let active = self
                    .state
                    .agent_runners
                    .read()
                    .await
                    .get(SESSION_ID)
                    .is_some_and(|runner| {
                        matches!(runner.status, AgentStatus::Pending | AgentStatus::Running)
                    });
                if requested
                    && !active
                    && self
                        .state
                        .session_activation_router
                        .current_run_id(SESSION_ID)
                        .await
                        .is_none()
                {
                    break self.session().await;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("real Root execution did not settle after calling the provider")
    }

    async fn human_turn(&self, message: &str, needs_execute: bool) {
        let app = test::init_service(
            App::new()
                .app_data(self.state.clone())
                .configure(crate::routes::agent_routes),
        )
        .await;
        let chat = test::TestRequest::post()
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .uri("/api/v1/chat")
            .set_json(json!({
                "session_id": SESSION_ID,
                "message": message,
                "model": MODEL,
            }))
            .to_request();
        let chat: Value = test::call_and_read_body_json(&app, chat).await;
        assert_eq!(chat["session_id"], SESSION_ID, "{chat}");
        let input_id = chat["message_id"].as_str().unwrap();
        if !needs_execute {
            // An already activated Root resumes from its newly admitted Inbox
            // input. The first cold turn needs the explicit Execute handoff.
            return;
        }
        let execute = test::TestRequest::post()
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .uri(&format!("/api/v1/sessions/{SESSION_ID}/execute"))
            .insert_header(("Bamboo-Input-Id", input_id))
            .insert_header((
                "Bamboo-Ingress-Seq",
                chat["ingress_seq"].as_u64().unwrap().to_string(),
            ))
            .insert_header((
                "Bamboo-Session-Created-At",
                self.session().await.created_at.to_rfc3339(),
            ))
            .insert_header(("Idempotency-Key", format!("execute-{input_id}")))
            .set_json(json!({}))
            .to_request();
        let response = test::call_service(&app, execute).await;
        let status = response.status();
        let body = test::read_body(response).await;
        assert!(
            status.is_success(),
            "{status} {}",
            String::from_utf8_lossy(&body)
        );
    }
}

#[actix_web::test]
async fn host_paced_loop_retries_next_due_after_failure_and_scopes_system_guidance() {
    let fixture = Fixture::new().await;
    // Seed an ordinary conversation through its real ingress and prompt
    // checkpoint. A hand-built transcript skips that Root context setup.
    fixture.human_turn(CONTEXT, true).await;
    fixture.settled(1).await;
    fixture.probe.requests.lock().unwrap().clear();

    let initial = fixture.start().await;
    fixture
        .state
        .paced_loops
        .tick(initial.next_fire_at - TimeDelta::milliseconds(1))
        .await;
    assert!(fixture.probe.requests.lock().unwrap().is_empty());

    fixture.state.paced_loops.tick(initial.next_fire_at).await;
    if tokio::time::timeout(
        Duration::from_secs(20),
        fixture.probe.first_entered.notified(),
    )
    .await
    .is_err()
    {
        let session = fixture.session().await;
        panic!(
            "first scheduled turn did not enter the provider: {:?}",
            session.last_run_error()
        );
    }

    // Several deadlines pass while the exact Root owner remains in the
    // provider. The tick preserves its due occurrence without launching it.
    let scheduled_after_first = initial.next_fire_at + TimeDelta::minutes(1);
    let late = scheduled_after_first + TimeDelta::minutes(3);
    fixture.state.paced_loops.tick(late).await;
    assert_eq!(fixture.loop_state().await.iteration, 1);
    assert_eq!(fixture.probe.requests.lock().unwrap().len(), 1);
    fixture.probe.release_first.notify_one();

    let failed = fixture.settled(1).await;
    let first = PacedLoopState::read(&failed).unwrap().unwrap();
    assert_eq!(first.id, initial.id);
    assert_eq!(first.iteration, 1);
    assert_eq!(
        saved_outcome(&failed),
        json!({"run_id": first.last_run_id.as_deref().unwrap(), "status": "failed"})
    );
    assert_eq!(
        failed.agent_runtime_state.unwrap().status,
        AgentStatusState::Failed
    );
    assert_eq!(first.next_fire_at, scheduled_after_first);
    assert_eq!(fixture.probe.requests.lock().unwrap().len(), 1);

    // The existing wake retry produces one successor in the same Session.
    let retry = late + RETRY_DELAY;
    fixture.state.paced_loops.tick(retry).await;
    fixture.state.paced_loops.tick(retry).await;
    let recovered = fixture.settled(2).await;
    let second = PacedLoopState::read(&recovered).unwrap().unwrap();
    assert_eq!(second.id, initial.id);
    assert_eq!(second.iteration, 2);
    assert_ne!(second.last_run_id, first.last_run_id);
    let completed_outcome =
        json!({"run_id": second.last_run_id.as_deref().unwrap(), "status": "completed"});
    assert_eq!(saved_outcome(&recovered), completed_outcome);
    assert_eq!(second.next_fire_at, late + TimeDelta::minutes(1));
    assert_eq!(
        recovered.agent_runtime_state.as_ref().unwrap().status,
        AgentStatusState::Completed
    );
    let scheduled: Vec<_> = recovered
        .messages
        .iter()
        .filter(|message| message.content == PROMPT)
        .collect();
    assert_eq!(scheduled.len(), 2);
    for (index, message) in scheduled.iter().enumerate() {
        let metadata = message.metadata.as_ref().unwrap();
        let envelope = &metadata["session_message"];
        assert_eq!(envelope["target_session_id"], SESSION_ID);
        assert_eq!(envelope["source"]["type"], "runtime");
        assert_eq!(envelope["source"]["subsystem"], PACED_LOOP_SUBSYSTEM);
        assert_eq!(envelope["body"]["instruction"], PACED_LOOP_INSTRUCTION);
        assert_eq!(
            envelope["body"]["data"]["occurrence"].as_u64(),
            Some(index as u64 + 1)
        );
    }
    assert!(recovered.messages.iter().any(
        |message| matches!(message.role, Role::Assistant) && message.content.contains(SUCCESS)
    ));
    assert!(!recovered
        .messages
        .iter()
        .any(|message| message.content.contains(SYSTEM_GUIDANCE)));

    fixture.human_turn(HUMAN, false).await;
    let after_human = fixture.settled(3).await;
    assert_eq!(saved_outcome(&after_human), completed_outcome);
    let requests = fixture.probe.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    for (index, messages) in requests[..2].iter().enumerate() {
        let guidance = messages
            .iter()
            .find(|message| {
                matches!(message.role, Role::System) && message.content.contains(SYSTEM_GUIDANCE)
            })
            .expect("scheduled request must receive ephemeral system guidance");
        assert!(guidance
            .content
            .contains(&format!("Scheduled iteration {}.", index + 1)));
        let expected_previous = if index == 0 { "none" } else { "failed" };
        assert!(guidance.content.contains(&format!(
            "Previous scheduled turn status: {expected_previous}."
        )));
        assert!(messages.iter().any(|message| message.content == CONTEXT));
        assert!(messages
            .iter()
            .any(|message| matches!(message.role, Role::User) && message.content == PROMPT));
    }
    assert!(requests[2].iter().any(|message| message.content == HUMAN));
    assert!(!requests[2]
        .iter()
        .any(|message| message.content.contains(SYSTEM_GUIDANCE)));
}

#[actix_web::test]
async fn host_paced_loop_rejects_stopped_generations_and_expired_due_occurrences() {
    let fixture = Fixture::new().await;
    let original = fixture.start().await;
    let stale = Due {
        generation: original.id,
        at: original.next_fire_at,
    };
    let stopped = fixture
        .state
        .apply_paced_loop(SESSION_ID, PacedLoopCommand::Stop)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(stopped.state, LoopStatus::Stopped));
    fixture.state.paced_loops.tick(stale.at).await;
    assert_eq!(fixture.loop_state().await.iteration, 0);

    let replacement = fixture.start().await;
    assert_ne!(replacement.id, original.id);
    assert!(!fixture
        .state
        .paced_loops
        .fire(SESSION_ID, stale, replacement.next_fire_at)
        .await
        .unwrap());
    fixture.state.paced_loops.tick(replacement.expires_at).await;
    assert_eq!(fixture.loop_state().await.iteration, 0);
    assert!(fixture.probe.requests.lock().unwrap().is_empty());
    assert!(fixture
        .state
        .session_activation_router
        .current_run_id(SESSION_ID)
        .await
        .is_none());
}

#[actix_web::test]
async fn host_paced_loop_restarts_paused_and_resumes_without_replaying_offline_ticks() {
    let fixture = Fixture::new().await;
    let original = fixture.start().await;
    let old_service = &fixture.state.paced_loops;
    let new_service = PacedLoops::new(
        old_service.store.clone(),
        old_service.persistence.clone(),
        old_service.sessions.clone(),
        old_service.inbox.clone(),
        old_service.coordinator.clone(),
    );
    let offline = original.next_fire_at + TimeDelta::minutes(5);
    new_service.tick(offline).await;
    let projected = new_service
        .apply(SESSION_ID, PacedLoopCommand::Status)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(projected.state, LoopStatus::Paused));
    assert_eq!(projected.next_fire_at, None);

    let resumed = new_service
        .apply(SESSION_ID, PacedLoopCommand::Resume)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(resumed.state, LoopStatus::Scheduled));
    assert_eq!(resumed.id, original.id);
    assert_eq!(resumed.expires_at, original.expires_at);
    assert_eq!(resumed.iteration, 0);
    assert!(resumed.next_fire_at.is_some());
    old_service.tick(offline).await;
    assert!(fixture.probe.requests.lock().unwrap().is_empty());
}
