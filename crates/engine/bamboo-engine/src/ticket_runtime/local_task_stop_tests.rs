//! Ticket stop recovery for the production Tokio backend. Positive task proof
//! comes only from LocalTaskChildRunner joining its Agent and draining resources;
//! JSON below is limited to controlled persistence-corruption/legacy fixtures.
//! This isolates the direct runner/receipt adapter; SDK spawn and its publication
//! barrier are exercised separately by external_agents::local_task::tests.
use super::*;
use crate::execution::spawn::{ExternalChildRunner, SpawnJob};
use crate::external_agents::local_task::{LocalTaskChildRunner, LocalTaskRuntime};
use crate::external_agents::runtime::NativeToolCeilingSource;
use crate::runtime::Agent;
use async_trait::async_trait;
use bamboo_agent_core::tools::{ToolCall, ToolError, ToolExecutor, ToolResult, ToolSchema};
use bamboo_domain::{ActorActivationStatus, ActorDirectoryPort, Message};
use bamboo_llm::{Config, LLMChunk, LLMError, LLMProvider, LLMStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;

const RUN: &str = "host-run";
const OUTPUT: &str = "Result from the actually joined local Ticket task";

struct NoTools;
#[async_trait]
impl ToolExecutor for NoTools {
    async fn execute(&self, call: &ToolCall) -> std::result::Result<ToolResult, ToolError> {
        Err(ToolError::NotFound(call.function.name.clone()))
    }
    fn list_tools(&self) -> Vec<ToolSchema> {
        Vec::new()
    }
}
struct NoToolCeiling;
#[async_trait]
impl NativeToolCeilingSource for NoToolCeiling {
    async fn observe(&self, _: &Session) -> std::result::Result<Vec<String>, String> {
        Ok(Vec::new())
    }
}
struct OneReply(AtomicUsize);
#[async_trait]
impl LLMProvider for OneReply {
    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        model: &str,
    ) -> std::result::Result<LLMStream, LLMError> {
        assert_eq!(model, "fixture-model");
        assert_eq!(self.0.fetch_add(1, Ordering::SeqCst), 0);
        Ok(Box::pin(futures::stream::iter(vec![
            Ok(LLMChunk::Token(OUTPUT.into())),
            Ok(LLMChunk::Done),
        ])))
    }
}

#[tokio::test]
async fn actual_local_task_stop_is_uuid_bound_and_result_replays_after_restart() {
    let mut f = Fixture::new().await;
    f.child.add_message(Message::system("Return the result."));
    f.child.add_message(Message::user("Complete this Ticket."));
    f.storage.save_session(&f.child).await.unwrap();
    let registration = f
        .runtime
        .router
        .register_run(&f.child.id, RUN)
        .await
        .unwrap();
    let provider = Arc::new(OneReply(AtomicUsize::new(0)));
    let tools: Arc<dyn ToolExecutor> = Arc::new(NoTools);
    let config = Config::default();
    let agent = Arc::new(
        Agent::builder()
            .storage(f.storage.clone())
            .persistence(f.runtime.persistence.clone())
            .session_inbox(f.runtime.inbox.clone())
            .activation_router(f.runtime.router.clone())
            .attachment_reader(f.storage.clone())
            .skill_manager(Arc::new(bamboo_skills::SkillManager::new()))
            .metrics_collector(bamboo_metrics::MetricsCollector::spawn(
                Arc::new(bamboo_metrics::SqliteMetricsStorage::new(
                    f.dir.path().join("metrics.db"),
                )),
                7,
            ))
            .config(Arc::new(RwLock::new(config.clone())))
            .provider(provider.clone())
            .default_tools(tools.clone())
            .build()
            .unwrap(),
    );
    let runner = LocalTaskChildRunner::new(&config, Some(Arc::new(NoToolCeiling)), None);
    runner.set_actor_directory_store(Some(f.storage.clone()));
    runner.set_session_inbox_runtime(Some(f.runtime.clone()));
    runner.set_ticket_service(Some(f.service.clone()));
    runner.set_local_task_runtime(Some(LocalTaskRuntime {
        agent,
        tools,
        provider_router: None,
        app_data_dir: Some(f.dir.path().join("host")),
        sessions_cache: Arc::default(),
    }));
    let cancel = CancellationToken::new();
    let persistence = runner
        .prepare_child_execution(&f.child, RUN, cancel.clone())
        .await
        .unwrap()
        .unwrap();
    assert!(runner
        .track_child_task(&f.child.id, RUN, tokio::spawn(async {}))
        .is_none());
    assert!(runner
        .prepare_durable_child_delivery(&f.child, RUN)
        .await
        .is_err());
    let (events, mut receive) = mpsc::channel(256);
    let drain = tokio::spawn(async move { while receive.recv().await.is_some() {} });
    let job = SpawnJob {
        parent_session_id: f.supervisor.id.clone(),
        child_session_id: f.child.id.clone(),
        model: "fixture-model".into(),
        disabled_tools: None,
    };
    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        runner.execute_external_child(&mut f.child, &job, events, cancel),
    )
    .await
    .expect("real local task must finish")
    .unwrap();
    drain.await.unwrap();
    assert_eq!(provider.0.load(Ordering::SeqCst), 1);
    assert!(runner
        .prepare_durable_child_delivery(&f.child, RUN)
        .await
        .unwrap());
    let mut wrong_birth = f.child.clone();
    wrong_birth.created_at -= chrono::Duration::seconds(1);
    assert!(runner
        .prepare_durable_child_delivery(&wrong_birth, RUN)
        .await
        .is_err());
    assert!(runner
        .prepare_durable_child_delivery(&f.child, "another-run")
        .await
        .is_err());

    let raw: serde_json::Value =
        serde_json::from_str(&f.child.metadata[TICKET_OWNED_STOP_KEY]).unwrap();
    let instance = uuid::Uuid::parse_str(raw["task_instance"].as_str().unwrap()).unwrap();
    assert!(!instance.is_nil());
    assert!(
        raw.get("pid").is_none(),
        "task proof must never invent a PID"
    );
    assert_eq!(raw["child_birth"], serde_json::json!(f.child.created_at));
    assert_eq!(raw["receipt"]["run_id"], RUN);
    assert_eq!(raw["completed"], true);
    assert_eq!(
        f.child.metadata["runtime.local_task.instance"],
        instance.to_string()
    );
    assert_eq!(f.child.metadata["runtime.local_task.state"], "stopped");
    let snapshot = f.service.published().unwrap().1;
    assert!(snapshot.assignments[&f.spec.assignment_id].process_stopped);
    assert!(
        snapshot.submissions.is_empty(),
        "stop alone is not a result"
    );
    assert_eq!(
        f.storage
            .inspect_actor(&f.child.id)
            .await
            .unwrap()
            .activation
            .unwrap()
            .status,
        ActorActivationStatus::Running,
        "Actor ownership remains live through result publication"
    );

    f.child.set_last_run_status("completed");
    persistence
        .checkpoint_runtime_session(&mut f.child)
        .await
        .unwrap();
    let canonical = f.storage.load_session(&f.child.id).await.unwrap().unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&canonical.metadata[TICKET_OWNED_STOP_KEY])
            .unwrap(),
        raw
    );
    assert_eq!(canonical.messages.last().unwrap().content, OUTPUT);
    let first = checkpoint_ticket_result(&f.service, f.storage.as_ref(), &canonical, RUN)
        .await
        .unwrap()
        .unwrap();
    let before_replay = canonical_bytes(&f.service.published().unwrap()).unwrap();
    assert_eq!(
        checkpoint_ticket_result(&f.service, f.storage.as_ref(), &canonical, RUN)
            .await
            .unwrap(),
        Some(first.clone())
    );
    assert_eq!(
        canonical_bytes(&f.service.published().unwrap()).unwrap(),
        before_replay
    );
    runner
        .finish_child_execution(&canonical, RUN, true)
        .await
        .unwrap();
    assert_eq!(
        f.storage
            .inspect_actor(&f.child.id)
            .await
            .unwrap()
            .activation
            .unwrap()
            .status,
        ActorActivationStatus::Succeeded
    );
    runner.shutdown().await.unwrap();
    drop(registration);
    drop(runner);
    let binding = f.service.published().unwrap().1.binding;
    drop(f.service);
    let restarted = TicketService::open(f.dir.path().join("tickets"), binding).unwrap();
    let before_replay = canonical_bytes(&restarted.published().unwrap()).unwrap();
    assert_eq!(
        checkpoint_ticket_result(&restarted, f.storage.as_ref(), &canonical, RUN)
            .await
            .unwrap(),
        Some(first)
    );
    assert_eq!(
        canonical_bytes(&restarted.published().unwrap()).unwrap(),
        before_replay
    );
    assert_eq!(restarted.published().unwrap().1.submissions.len(), 1);
}

async fn admitted_fixture() -> Fixture {
    let mut f = Fixture::new().await;
    let _registration = f
        .runtime
        .router
        .register_run(&f.child.id, RUN)
        .await
        .unwrap();
    admit_ticket_run(&f.service, &f.runtime, &mut f.child, RUN)
        .await
        .unwrap();
    f.child.set_last_run_status("completed");
    f.child
        .add_message(Message::assistant("Durable Host fixture result", None));
    f
}

// Controlled canonical corruption only. This is not a ConfirmedTaskStop and is
// never passed to checkpoint_owned_task_stop or used as positive execution proof.
fn task_stop_fixture(child: &mut Session, instance: uuid::Uuid) -> serde_json::Value {
    child
        .metadata
        .insert("runtime.local_task.state".into(), "stopped".into());
    child
        .metadata
        .insert("runtime.local_task.instance".into(), instance.to_string());
    serde_json::json!({
        "receipt": read_dispatch(child).unwrap().unwrap().receipt.unwrap(),
        "child_birth": child.created_at,
        "task_instance": instance,
        "completed": true,
    })
}

async fn assert_rejected_without_ticket_change(
    f: &Fixture,
    authority_unavailable: bool,
    case: &str,
) {
    let before = canonical_bytes(&f.service.published().unwrap()).unwrap();
    let result = checkpoint_ticket_result(&f.service, f.storage.as_ref(), &f.child, RUN).await;
    if authority_unavailable {
        assert!(
            matches!(result, Err(Error::AuthorityUnavailable(_))),
            "{case}: {result:?}"
        );
    } else {
        assert!(
            matches!(result, Err(Error::ScopeDenied(_))),
            "{case}: {result:?}"
        );
    }
    let after = f.service.published().unwrap();
    assert_eq!(canonical_bytes(&after).unwrap(), before, "{case}");
    assert!(
        !after.1.assignments[&f.spec.assignment_id].process_stopped,
        "{case}"
    );
    assert!(after.1.submissions.is_empty(), "{case}");
}

#[tokio::test]
async fn task_uuid_and_stopped_display_without_owned_stop_receipt_cannot_submit() {
    let mut f = admitted_fixture().await;
    let _untrusted_shape = task_stop_fixture(&mut f.child, uuid::Uuid::new_v4());
    f.child.metadata_version += 1;
    f.storage.save_session(&f.child).await.unwrap();
    assert_rejected_without_ticket_change(&f, true, "UUID/status without independent stop").await;
}

#[tokio::test]
async fn task_stop_receipt_requires_exact_canonical_stopped_instance_metadata() {
    for case in [
        "missing_state",
        "running",
        "missing_instance",
        "wrong_instance",
    ] {
        let mut f = admitted_fixture().await;
        let proof = task_stop_fixture(&mut f.child, uuid::Uuid::new_v4());
        match case {
            "missing_state" => {
                f.child.metadata.remove("runtime.local_task.state");
            }
            "running" => {
                f.child
                    .metadata
                    .insert("runtime.local_task.state".into(), "running".into());
            }
            "missing_instance" => {
                f.child.metadata.remove("runtime.local_task.instance");
            }
            "wrong_instance" => {
                f.child.metadata.insert(
                    "runtime.local_task.instance".into(),
                    uuid::Uuid::new_v4().to_string(),
                );
            }
            _ => unreachable!(),
        }
        f.child
            .metadata
            .insert(TICKET_OWNED_STOP_KEY.into(), proof.to_string());
        f.child.metadata_version += 1;
        f.storage.save_session(&f.child).await.unwrap();
        assert_rejected_without_ticket_change(&f, false, case).await;
    }
}

#[tokio::test]
async fn task_stop_receipt_rejects_changed_birth_run_instance_and_ambiguous_stop_kind() {
    for case in [
        "birth",
        "run",
        "session",
        "dispatch",
        "spec",
        "instance",
        "nil",
        "both",
        "neither",
        "incomplete",
    ] {
        let mut f = admitted_fixture().await;
        let mut proof = task_stop_fixture(&mut f.child, uuid::Uuid::new_v4());
        match case {
            "birth" => {
                proof["child_birth"] =
                    serde_json::json!(f.child.created_at - chrono::Duration::seconds(1))
            }
            "run" => proof["receipt"]["run_id"] = serde_json::json!("another-run"),
            "session" => proof["receipt"]["session_id"] = serde_json::json!("another-child"),
            "dispatch" => proof["receipt"]["dispatch_key"] = serde_json::json!("another-dispatch"),
            "spec" => proof["receipt"]["spec_hash"] = serde_json::json!("another-spec"),
            "instance" => proof["task_instance"] = serde_json::json!(uuid::Uuid::new_v4()),
            "nil" => {
                proof["task_instance"] = serde_json::json!(uuid::Uuid::nil());
                f.child.metadata.insert(
                    "runtime.local_task.instance".into(),
                    uuid::Uuid::nil().to_string(),
                );
            }
            "both" => proof["pid"] = serde_json::json!(1234),
            "neither" => {
                proof.as_object_mut().unwrap().remove("task_instance");
            }
            "incomplete" => proof["completed"] = serde_json::json!(false),
            _ => unreachable!(),
        }
        f.child
            .metadata
            .insert(TICKET_OWNED_STOP_KEY.into(), proof.to_string());
        f.child.metadata_version += 1;
        f.storage.save_session(&f.child).await.unwrap();
        assert_rejected_without_ticket_change(&f, false, case).await;
    }
}

#[tokio::test]
async fn legacy_pid_stop_receipt_still_replays_without_task_metadata() {
    let mut f = admitted_fixture().await;
    save_owned_stop_fixture(&mut f.child, true);
    assert!(!f.child.metadata.contains_key("runtime.local_task.instance"));
    assert!(!f.child.metadata.contains_key("runtime.local_task.state"));
    let raw: serde_json::Value =
        serde_json::from_str(&f.child.metadata[TICKET_OWNED_STOP_KEY]).unwrap();
    assert_eq!(raw["pid"], 1234);
    assert!(raw.get("task_instance").is_none());
    f.storage.save_session(&f.child).await.unwrap();
    let first = checkpoint_ticket_result(&f.service, f.storage.as_ref(), &f.child, RUN)
        .await
        .unwrap()
        .unwrap();
    let before_replay = canonical_bytes(&f.service.published().unwrap()).unwrap();
    assert_eq!(
        checkpoint_ticket_result(&f.service, f.storage.as_ref(), &f.child, RUN)
            .await
            .unwrap(),
        Some(first)
    );
    assert_eq!(
        canonical_bytes(&f.service.published().unwrap()).unwrap(),
        before_replay
    );
    let snapshot = f.service.published().unwrap().1;
    assert!(snapshot.assignments[&f.spec.assignment_id].process_stopped);
    assert_eq!(snapshot.submissions.len(), 1);
}
