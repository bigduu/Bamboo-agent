//! Exercise real Session persistence, Inbox admission, and the Root runner.

use super::{
    Due, LoopStatus, PacedLoopCommand, PacedLoopState, PacedLoops, ADAPTIVE_FALLBACK_SECONDS,
    ADAPTIVE_SYSTEM_GUIDANCE, RETRY_DELAY, SYSTEM_GUIDANCE,
};
use crate::app_state::{AgentStatus, AppState};
use actix_web::{test, web, App};
use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolSchema};
use bamboo_agent_core::{Message, Role, Session};
use bamboo_domain::session::inbox::{PACED_LOOP_INSTRUCTION, PACED_LOOP_SUBSYSTEM};
use bamboo_domain::session::PACED_LOOP_OUTCOME_METADATA_KEY;
use bamboo_domain::{ActorDirectoryEntry, AgentStatusState, PACED_LOOP_WAKEUP_TOOL_NAME};
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
const ASK_START: &str = "每隔五分钟检查构建状态，有变化再告诉我。";
const ASK_STATUS: &str = "这个循环下次什么时候运行？";
const ASK_STOP: &str = "停止这个循环。";
const ASK_UNGROUNDED_STOP: &str = "请汇报最新构建状态。";

fn saved_outcome(session: &Session) -> Value {
    serde_json::from_str(&session.metadata[PACED_LOOP_OUTCOME_METADATA_KEY]).unwrap()
}

#[derive(Default)]
struct Probe {
    requests: Mutex<Vec<Vec<Message>>>,
    scripted_loop: bool,
    scripted_adaptive: bool,
    first_entered: Notify,
    release_first: Notify,
}

#[async_trait::async_trait]
impl LLMProvider for Probe {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        assert_eq!(model, MODEL, "the Root's existing model must survive");
        let ordinal = {
            let mut requests = self.requests.lock().unwrap();
            requests.push(messages.to_vec());
            requests.len()
        };
        let paced = messages.iter().any(|message| {
            message.role == Role::System && message.content.contains(SYSTEM_GUIDANCE)
        });
        let adaptive = messages.iter().find(|message| {
            message.role == Role::System && message.content.contains(ADAPTIVE_SYSTEM_GUIDANCE)
        });
        let wakeup_tool = tools
            .iter()
            .any(|tool| tool.function.name == PACED_LOOP_WAKEUP_TOOL_NAME);
        assert_eq!(
            wakeup_tool,
            adaptive.is_some(),
            "wakeup tool belongs to adaptive turns"
        );
        if self.scripted_adaptive {
            if let Some(message) = adaptive {
                assert!(!tools
                    .iter()
                    .any(|tool| tool.function.name == "session_loop"));
                let iteration = (1..=5)
                    .find(|iteration| {
                        message
                            .content
                            .contains(&format!("Scheduled iteration {iteration}."))
                    })
                    .expect("expected adaptive occurrence");
                let id = format!("adaptive-wakeup-{iteration}");
                let result = messages
                    .iter()
                    .find(|message| message.tool_call_id.as_deref() == Some(id.as_str()));
                if let Some(delay) = match iteration {
                    1 => Some(120),
                    2 => Some(3601),
                    4 => Some(600),
                    _ => None,
                } {
                    if let Some(result) = result {
                        assert_eq!(
                            result.tool_success,
                            Some(iteration != 2),
                            "{}",
                            result.content
                        );
                    } else {
                        return Ok(Box::pin(futures::stream::iter([
                            Ok(LLMChunk::ToolCalls(vec![ToolCall {
                                id,
                                tool_type: "function".into(),
                                function: FunctionCall {
                                    name: PACED_LOOP_WAKEUP_TOOL_NAME.into(),
                                    arguments: json!({
                                        "delay_seconds": delay,
                                        "reason": "Synthetic build state"
                                    })
                                    .to_string(),
                                },
                            }])),
                            Ok(LLMChunk::Done),
                        ])));
                    }
                }
                if matches!(iteration, 3 | 4) {
                    return Err(LLMError::RequestFailed("synthetic adaptive failure".into()));
                }
            }
        }
        if self.scripted_loop {
            let latest = messages.iter().rev().find(|message| {
                message.role == Role::User
                    && matches!(
                        message.content.as_str(),
                        ASK_START | ASK_STATUS | ASK_STOP | ASK_UNGROUNDED_STOP
                    )
            });
            let schema = tools
                .iter()
                .find(|tool| tool.function.name == "session_loop");
            if paced {
                assert!(schema.is_none(), "scheduled turns cannot control the timer");
            } else if let Some(latest) = latest {
                let schema = schema.expect("native human turn should advertise session_loop");
                assert!(schema.function.description.contains("current chat"));
                let (id, args) = match latest.content.as_str() {
                    ASK_START => (
                        "natural-start",
                        json!({
                            "action": "start",
                            "interval_seconds": 300,
                            "user_quote": "每隔五分钟检查构建状态"
                        }),
                    ),
                    ASK_STATUS => ("natural-status", json!({"action": "status"})),
                    ASK_STOP => (
                        "natural-stop",
                        json!({"action": "stop", "user_quote": ASK_STOP}),
                    ),
                    _ => (
                        "ungrounded-stop",
                        json!({"action": "stop", "user_quote": ASK_UNGROUNDED_STOP}),
                    ),
                };
                if messages
                    .iter()
                    .all(|message| message.tool_call_id.as_deref() != Some(id))
                {
                    return Ok(Box::pin(futures::stream::iter([
                        Ok(LLMChunk::ToolCalls(vec![ToolCall {
                            id: id.into(),
                            tool_type: "function".into(),
                            function: FunctionCall {
                                name: "session_loop".into(),
                                arguments: args.to_string(),
                            },
                        }])),
                        Ok(LLMChunk::Done),
                    ])));
                }
            }
        }
        if ordinal == 1 && paced {
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

#[actix_web::test]
async fn host_paced_loop_adaptive_uses_model_delay_and_repeats_the_fallback_after_errors() {
    let fixture = Fixture::with_probe(Probe {
        scripted_adaptive: true,
        ..Probe::default()
    })
    .await;
    fixture.human_turn(CONTEXT, true).await;
    fixture.settled(1).await;
    fixture
        .state
        .apply_paced_loop(
            SESSION_ID,
            PacedLoopCommand::StartAdaptive {
                prompt: PROMPT.into(),
            },
        )
        .await
        .unwrap()
        .unwrap();
    let initial = fixture.loop_state().await;
    assert!(!initial.mode.is_fixed());
    assert!(initial.next_fire_at <= chrono::Utc::now());
    let mut due_at = initial.next_fire_at;

    for (index, (requests, terminal, delay)) in [
        (3, "completed", 120),
        (5, "completed", ADAPTIVE_FALLBACK_SECONDS), // Invalid proposal
        (6, "failed", ADAPTIVE_FALLBACK_SECONDS),    // No proposal and provider failure
        (8, "failed", ADAPTIVE_FALLBACK_SECONDS),    // Valid proposal before provider failure
        (9, "completed", ADAPTIVE_FALLBACK_SECONDS), // No proposal on an otherwise good turn
    ]
    .into_iter()
    .enumerate()
    {
        fixture
            .state
            .paced_loops
            .tick(due_at - TimeDelta::milliseconds(1))
            .await;
        assert_eq!(fixture.loop_state().await.iteration, index as u64);
        fixture.state.paced_loops.tick(due_at).await;
        let session = fixture.settled(requests).await;
        let state = PacedLoopState::read(&session).unwrap().unwrap();
        assert_eq!(state.iteration, index as u64 + 1);
        assert_eq!(state.last_run_status(&session), Some(terminal));
        let outcome = state.outcome(&session).unwrap();
        let expected = outcome.finished_at.unwrap().max(due_at) + TimeDelta::seconds(delay as i64);
        let projected = state.project(
            &session,
            fixture.state.paced_loops.host_instance,
            chrono::Utc::now(),
        );
        assert_eq!(projected.next_fire_at, Some(expected));
        assert_eq!(projected.interval_seconds, delay);
        assert_eq!(
            projected.next_wakeup_reason.as_deref(),
            (index == 0).then_some("Synthetic build state")
        );
        due_at = expected;
    }
    fixture
        .state
        .apply_paced_loop(SESSION_ID, PacedLoopCommand::Stop)
        .await
        .unwrap();
    fixture.state.paced_loops.tick(due_at).await;
    assert_eq!(fixture.probe.requests.lock().unwrap().len(), 9);
}

struct Fixture {
    state: web::Data<AppState>,
    probe: Arc<Probe>,
    _home: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> Self {
        Self::with_probe(Probe::default()).await
    }

    async fn with_probe(probe: Probe) -> Self {
        let home = tempfile::tempdir().unwrap();
        let probe = Arc::new(probe);
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
        self.wait_for_settle(expected_requests).await;
        self.session().await
    }

    async fn wait_for_settle(&self, expected_requests: usize) {
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
                    break;
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
async fn host_paced_loop_accepts_only_the_current_natural_language_turn() {
    let fixture = Fixture::with_probe(Probe {
        scripted_loop: true,
        ..Probe::default()
    })
    .await;
    fixture.human_turn(ASK_START, true).await;
    let started = fixture.settled(2).await;
    let state = PacedLoopState::read(&started).unwrap().unwrap();
    assert_eq!(state.interval_seconds, 300);
    assert_eq!(state.prompt, ASK_START);
    assert!(!state.stopped);
    let result = |session: &Session, id: &str| {
        session
            .messages
            .iter()
            .find(|message| message.tool_call_id.as_deref() == Some(id))
            .unwrap_or_else(|| panic!("missing tool result {id}"))
            .clone()
    };
    assert_eq!(result(&started, "natural-start").tool_success, Some(true));

    fixture.state.paced_loops.tick(state.next_fire_at).await;
    fixture.settled(3).await;
    fixture.human_turn(ASK_UNGROUNDED_STOP, false).await;
    let ungrounded = fixture.settled(5).await;
    assert_eq!(
        result(&ungrounded, "ungrounded-stop").tool_success,
        Some(false)
    );
    assert!(!PacedLoopState::read(&ungrounded).unwrap().unwrap().stopped);

    fixture.human_turn(ASK_STATUS, false).await;
    let status = fixture.settled(7).await;
    assert_eq!(result(&status, "natural-status").tool_success, Some(true));
    fixture.human_turn(ASK_STOP, false).await;
    let stopped = fixture.settled(9).await;
    assert_eq!(result(&stopped, "natural-stop").tool_success, Some(true));
    assert!(PacedLoopState::read(&stopped).unwrap().unwrap().stopped);
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
async fn host_paced_loop_recovers_after_final_save_and_actor_closure_fail() {
    let fixture = Fixture::new().await;
    fixture.human_turn(CONTEXT, true).await;
    fixture.settled(1).await;
    fixture.probe.requests.lock().unwrap().clear();

    let initial = fixture.start().await;
    let loops = &fixture.state.paced_loops;
    loops.tick(initial.next_fire_at).await;
    tokio::time::timeout(
        Duration::from_secs(20),
        fixture.probe.first_entered.notified(),
    )
    .await
    .expect("first scheduled turn did not enter the provider");
    let running = fixture.session().await;
    let active = PacedLoopState::read(&running).unwrap().unwrap();
    let failed_run = active.last_run_id.as_deref().unwrap();
    assert_eq!(active.committed_run(&running), Some(failed_run));
    assert!(super::reports_active_execution(&running));
    assert!(loops.waiting_for_existing_turn(&running, &active).await);

    // Temporarily make the exact Actor unavailable on this fixture's real
    // filesystem so both fenced completion writes fail. Restore the record
    // after the local runner settles, with its lease already expired.
    let actor_file = fixture
        .state
        .app_data_dir
        .join("sessions")
        .join(SESSION_ID)
        .join("actor-authority.json");
    let offline = actor_file.with_extension("offline");
    tokio::fs::rename(&actor_file, &offline).await.unwrap();
    fixture.probe.release_first.notify_one();
    fixture.wait_for_settle(1).await;
    let mut actor: ActorDirectoryEntry =
        serde_json::from_slice(&tokio::fs::read(&offline).await.unwrap()).unwrap();
    let activation = actor.activation.as_mut().unwrap();
    assert_eq!(activation.run_id, failed_run);
    assert!(activation.status.is_live());
    activation.lease_expires_at = chrono::Utc::now() - TimeDelta::seconds(1);
    tokio::fs::write(&offline, serde_json::to_vec(&actor).unwrap())
        .await
        .unwrap();
    tokio::fs::rename(&offline, &actor_file).await.unwrap();

    let stale = fixture.session().await;
    assert_eq!(active.committed_run(&stale), Some(failed_run));
    assert!(super::reports_active_execution(&stale));
    assert!(active.outcome(&stale).is_none());
    assert!(super::waiting_for_existing_work(&stale));
    assert!(!loops.waiting_for_existing_turn(&stale, &active).await);

    let mut waiting = stale.clone();
    waiting.set_pending_question(
        "pending-tool-call".into(),
        "pause".into(),
        "Waiting for the user".into(),
        vec![],
        true,
    );
    assert!(loops.waiting_for_existing_turn(&waiting, &active).await);
    let mut other_run = stale.clone();
    other_run.set_host_run_id("another-run");
    assert!(loops.waiting_for_existing_turn(&other_run, &active).await);
    actor.activation.as_mut().unwrap().run_id = "another-run".into();
    tokio::fs::write(&actor_file, serde_json::to_vec(&actor).unwrap())
        .await
        .unwrap();
    assert!(loops.waiting_for_existing_turn(&stale, &active).await);
    actor.activation.as_mut().unwrap().run_id = failed_run.into();
    tokio::fs::write(&actor_file, serde_json::to_vec(&actor).unwrap())
        .await
        .unwrap();

    loops.tick(active.next_fire_at).await;
    let recovered = fixture.settled(2).await;
    let successor = PacedLoopState::read(&recovered).unwrap().unwrap();
    assert_eq!(successor.iteration, 2);
    assert_ne!(successor.last_run_id, active.last_run_id);
    assert_eq!(successor.last_run_status(&recovered), Some("completed"));
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
