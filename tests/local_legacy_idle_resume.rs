//! Explicit local legacy compatibility, through the real Host/SDK/Actor runner.
//!
//! The only simulated component is a loopback HTTP model provider. This proves
//! a Host SessionMessenger idle-Child reservation; it does not claim coverage
//! of the nested Worker's private SessionMessenger assembly. Linux /proc gives
//! independent PID/PPID evidence that both requests use the real worker binary,
//! rather than the default native Tokio backend or a mocked ExternalChildRunner.
#![cfg(target_os = "linux")]

use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};
use std::time::Duration;

use actix_web::{web, App, HttpResponse, HttpServer};
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolExecutionContext, ToolOutcome};
use bamboo_agent_core::{Message, Role, Session};
use bamboo_domain::{
    ProviderModelRef, SessionActivationDisposition, SessionMessageBody, SessionMessageContent,
    SessionMessageEnvelope, SessionMessageId, SessionMessageKind, SessionMessageSource,
};
use bamboo_server::app_state::{AgentStatus, AppState, MemoryStore};
use bamboo_server::tools::ToolSurface;
use bamboo_storage::SessionStoreV2;
use bamboo_tools::permission::{PermissionMatcher, PermissionMatcherKind, PermissionType};
use serde_json::{json, Value};
use tokio::sync::Notify;

const WAIT: Duration = Duration::from_secs(45);
const MODEL: &str = "legacy-local-test";
const PARENT: &str = "legacy-idle-parent";
const INITIAL: &str = "LEGACY_INITIAL: return one plain answer without tools";
const FOLLOWUP: &str = "LEGACY_FOLLOWUP: continue this same Child without tools";
const REPLIES: [&str; 2] = ["LEGACY_FIRST_COMPLETED", "LEGACY_RESUME_COMPLETED"];

#[derive(Default)]
struct ProviderProbe {
    requests: Mutex<Vec<Value>>,
    entered: Notify,
    released: AtomicUsize,
    release: Notify,
}

async fn model_response(body: web::Json<Value>, probe: web::Data<ProviderProbe>) -> HttpResponse {
    let index = {
        let mut requests = probe.requests.lock().unwrap();
        let index = requests.len();
        requests.push(body.into_inner());
        index
    };
    probe.entered.notify_one();
    let Some(reply) = REPLIES.get(index) else {
        return HttpResponse::BadRequest().body("unexpected extra legacy provider request");
    };
    // These are actual HTTP requests made by the actual Worker. Neither a
    // reservation alone nor a fast failure can satisfy this deterministic gate.
    loop {
        let wake = probe.release.notified();
        if probe.released.load(Ordering::SeqCst) > index {
            break;
        }
        wake.await;
    }
    HttpResponse::Ok()
        .content_type("text/event-stream")
        .body(format!(
            "data: {}\n\ndata: [DONE]\n\n",
            json!({"id":format!("legacy-local-{index}"),"object":"chat.completion.chunk",
                "choices":[{"index":0,"delta":{"content":reply},"finish_reason":"stop"}]})
        ))
}

async fn provider_entered(probe: &ProviderProbe, count: usize, state: &AppState, id: &str) {
    let observed = tokio::time::timeout(WAIT, async {
        loop {
            let wake = probe.entered.notified();
            if probe.requests.lock().unwrap().len() >= count {
                break;
            }
            wake.await;
        }
    })
    .await;
    if observed.is_err() {
        let child = state.storage.load_session(id).await.unwrap().unwrap();
        panic!(
            "real legacy provider request {count} missing: status={:?}, error={:?}",
            child.last_run_status(),
            child.last_run_error()
        );
    }
    let requests = probe.requests.lock().unwrap();
    assert_eq!(
        requests.len(),
        count,
        "exactly one HTTP request per activation"
    );
    assert_eq!(requests[count - 1]["model"], MODEL);
}

fn release_provider(probe: &ProviderProbe, count: usize) {
    probe.released.store(count, Ordering::SeqCst);
    probe.release.notify_waiters();
}

/// Read-only observation of actual local workers. Exe identity, command, and
/// PPID must all match; another test's bamboo process cannot satisfy this.
fn owned_worker_pids() -> Vec<u32> {
    let executable = Path::new(env!("CARGO_BIN_EXE_bamboo"))
        .canonicalize()
        .unwrap();
    let mut pids = Vec::new();
    for entry in std::fs::read_dir("/proc").unwrap().flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|s| s.parse::<u32>().ok())
        else {
            continue;
        };
        let dir = entry.path();
        if std::fs::read_link(dir.join("exe")).ok().as_ref() != Some(&executable) {
            continue;
        }
        let Ok(status) = std::fs::read_to_string(dir.join("status")) else {
            continue;
        };
        let parent = status
            .lines()
            .find_map(|line| line.strip_prefix("PPid:")?.trim().parse::<u32>().ok());
        let args = std::fs::read(dir.join("cmdline")).unwrap_or_default();
        if parent == Some(std::process::id())
            && args
                .split(|byte| *byte == 0)
                .any(|arg| arg == b"subagent-worker")
        {
            assert_ne!(pid, std::process::id());
            pids.push(pid);
        }
    }
    pids.sort_unstable();
    pids
}

/// Inspect, never claim or ACK, the real broker's physical Run messages.
fn pending_runs(home: &Path, child_id: &str) -> Vec<(PathBuf, bamboo_subagent::InboxMessage)> {
    let mut runs = Vec::new();
    let root = home.join("broker/mailboxes");
    let Ok(mailboxes) = std::fs::read_dir(root) else {
        return runs;
    };
    for mailbox in mailboxes.flatten() {
        for state in ["new", "cur"] {
            let Ok(entries) = std::fs::read_dir(mailbox.path().join(state)) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) != Some("json") {
                    continue;
                }
                let bytes = match std::fs::read(&path) {
                    Ok(bytes) => bytes,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => panic!("cannot inspect {}: {error}", path.display()),
                };
                let message: bamboo_subagent::InboxMessage =
                    serde_json::from_slice(&bytes).unwrap();
                if message.kind == bamboo_subagent::InboxKind::Run {
                    let run: bamboo_subagent::RunSpec =
                        serde_json::from_value(message.body.clone()).unwrap();
                    if run
                        .logical_session
                        .as_ref()
                        .is_some_and(|s| s.session_id == child_id)
                    {
                        runs.push((path, message));
                    }
                }
            }
        }
    }
    runs
}

async fn wait_completed(state: &AppState, home: &Path, id: &str, run_id: &str) -> Session {
    tokio::time::timeout(WAIT, async {
        loop {
            let child = state.storage.load_session(id).await.unwrap().unwrap();
            if matches!(
                child.last_run_status().as_deref(),
                Some("error" | "timeout" | "cancelled")
            ) {
                panic!("legacy run failed: {:?}", child.last_run_error());
            }
            let finalized = state
                .agent_runners
                .read()
                .await
                .get(id)
                .is_some_and(|runner| {
                    runner.run_id == run_id && matches!(runner.status, AgentStatus::Completed)
                });
            if finalized
                && child.last_run_status().as_deref() == Some("completed")
                && state
                    .session_activation_router
                    .current_run_id(id)
                    .await
                    .is_none()
                && pending_runs(home, id).is_empty()
            {
                let source: Value = serde_json::from_str(
                    child
                        .metadata
                        .get("runtime.child_completion_source_v1")
                        .expect("real final writer publishes its terminal source"),
                )
                .unwrap();
                assert_eq!(source["activation_run_id"], run_id);
                assert_eq!(source["child_session_id"], id);
                assert_eq!(source["parent_session_id"], PARENT);
                assert_eq!(source["child_created_at"], json!(child.created_at));
                assert_eq!(source["status"], "completed");
                assert!(source["error_sha256"].is_null());
                let final_message = child
                    .messages
                    .iter()
                    .rev()
                    .find(|m| m.role == Role::Assistant)
                    .unwrap();
                assert_eq!(source["result_message_id"], final_message.id);
                assert_eq!(source["last_assistant"]["message_id"], final_message.id);
                return child;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("legacy terminal, canonical source, released router and ACKed Run must converge")
}

#[test]
fn explicit_local_legacy_child_idle_resume_reserves_once_and_acks_one_input() {
    // AppState's debug future is larger than libtest's default thread stack.
    std::thread::Builder::new()
        .name("legacy-idle-resume-fixture".into())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| actix_web::rt::System::new().block_on(run_fixture()))
        .unwrap()
        .join()
        .unwrap();
}

async fn run_fixture() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().canonicalize().unwrap();
    let jiandu = home.join("jiandu");
    // This standalone integration binary has one test. The real Worker's
    // independent Jiandu store must also remain inside this disposable fixture.
    struct RestoreJiandu(Option<std::ffi::OsString>);
    impl Drop for RestoreJiandu {
        fn drop(&mut self) {
            match &self.0 {
                Some(old) => std::env::set_var("BAMBOO_JIANDU_DATA_DIR", old),
                None => std::env::remove_var("BAMBOO_JIANDU_DATA_DIR"),
            }
        }
    }
    let _restore = RestoreJiandu(std::env::var_os("BAMBOO_JIANDU_DATA_DIR"));
    std::env::set_var("BAMBOO_JIANDU_DATA_DIR", &jiandu);
    let probe = web::Data::new(ProviderProbe::default());
    let server_probe = probe.clone();
    let server = HttpServer::new(move || {
        App::new()
            .app_data(server_probe.clone())
            .route("/v1/chat/completions", web::post().to(model_response))
            .route(
                "/v1/models",
                web::get().to(|| async { HttpResponse::Ok().json(json!({"data":[{"id":MODEL}]})) }),
            )
    })
    .workers(1)
    .bind(("127.0.0.1", 0))
    .unwrap();
    let base_url = format!("http://{}/v1", server.addrs()[0]);
    let running = server.run();
    let server_handle = running.handle();
    actix_web::rt::spawn(running);
    std::fs::write(home.join("config.json"), serde_json::to_vec(&json!({
        "provider":"openai",
        "features":{"provider_model_ref":true},
        "providers":{"openai":{"api_key":"local-fixture-only","base_url":base_url,"model":MODEL}},
        "defaults":{"chat":{"provider":"openai","model":MODEL}},
        "subagents":{"runtime":"actor","executor":"bamboo_runtime",
            "worker_bin":env!("CARGO_BIN_EXE_bamboo"),"worker_args":["subagent-worker"],
            "fabric_dir":home.join("fabric"),"max_concurrent":1}
    })).unwrap()).unwrap();
    let state = AppState::new_with_memory_store(home.clone(), MemoryStore::new(jiandu))
        .await
        .unwrap();
    {
        let config = state.config.read().await;
        assert!(bamboo_engine::external_agents::runtime::requires_legacy_child_runtime(&config));
        assert_eq!(
            config.subagents().worker_bin.as_deref(),
            Some(env!("CARGO_BIN_EXE_bamboo"))
        );
    }
    assert!(
        owned_worker_pids().is_empty(),
        "no preexisting fixture Worker"
    );
    let project = state
        .project_store
        .create("Local legacy idle resume", None)
        .unwrap();
    let mut parent = Session::new(PARENT, MODEL);
    parent.model_ref = Some(ProviderModelRef::new("openai", MODEL));
    parent.set_provider_name("openai");
    parent.set_project_id_meta(project.id.to_string());
    parent.workspace = Some(home.to_string_lossy().into_owned());
    parent.set_workspace_path_meta(home.to_string_lossy());
    parent.title_generated = true;
    parent.add_message(Message::user(INITIAL));
    state.storage.save_session(&parent).await.unwrap();
    state.session_store.save_session(&parent).await.unwrap();

    // Exactly this parent may create a Child. No global grant, Root write grant,
    // permission bypass, synthetic runner, or widened Child permission mode.
    state
        .permission_checker
        .permission_config()
        .unwrap()
        .grant_typed_scoped_session_permission(
            PARENT,
            PermissionType::ExecuteCommand,
            PermissionMatcher {
                id: "legacy-create-only".into(),
                kind: PermissionMatcherKind::ExactResource,
                value: "SubAgent create".into(),
            },
        )
        .unwrap();
    let policy_before =
        serde_json::to_value(state.permission_section.snapshot().data.as_ref()).unwrap();
    let create = ToolCall {
        id: "legacy-create".into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "SubAgent".into(),
            arguments: json!({
                "action":"create","title":"Local legacy compatibility",
                "responsibility":"Return one plain answer per input without tools",
                // A compatibility role deliberately uses ordinary legacy history;
                // this is not the separately restricted named plain Actor mode.
                "subagent_type":"legacy-idle-resume-fixture","prompt":INITIAL,
                "wait":false,"auto_run":true
            })
            .to_string(),
        },
    };
    let mut context = ToolExecutionContext::none(&create.id);
    context.session_id = Some(PARENT);
    context.root_session_id = Some(PARENT);
    let result = state
        .tool_factory
        .get(ToolSurface::Root)
        .execute_with_context_outcome(&create, context)
        .await
        .unwrap();
    let ToolOutcome::Completed(result) = result else {
        panic!("authorized create must complete")
    };
    assert!(result.success, "create failed: {}", result.result);
    let payload: Value = serde_json::from_str(&result.result).unwrap();
    let child_id = payload["child_session_id"].as_str().unwrap();
    provider_entered(&probe, 1, &state, child_id).await;
    let workers = owned_worker_pids();
    assert_eq!(
        workers.len(),
        1,
        "real same-binary worker with this Host PPID"
    );
    let first_run_id = state
        .session_activation_router
        .current_run_id(child_id)
        .await
        .unwrap();
    let first_runs = pending_runs(&home, child_id);
    assert_eq!(first_runs.len(), 1, "one real initial physical Run");
    let first_run: bamboo_subagent::RunSpec =
        serde_json::from_value(first_runs[0].1.body.clone()).unwrap();
    assert_eq!(
        first_run.activation_run_id.as_deref(),
        Some(first_run_id.as_str())
    );
    assert!(first_run.initial_session_messages.is_empty());
    release_provider(&probe, 1);
    let first = wait_completed(&state, &home, child_id, &first_run_id).await;
    assert_eq!(first.kind, bamboo_domain::SessionKind::Child);
    assert_eq!(first.parent_session_id.as_deref(), Some(PARENT));
    assert_eq!(first.root_session_id, PARENT);
    assert_eq!(
        first.metadata.get("runtime.kind").map(String::as_str),
        Some("external")
    );
    assert_eq!(
        first.metadata.get("external.protocol").map(String::as_str),
        Some("actor")
    );
    assert!(first
        .metadata
        .get("external.agent_id")
        .is_some_and(|v| !v.is_empty()));
    assert_eq!(
        first
            .messages
            .iter()
            .filter(|m| m.role == Role::Assistant && m.content == REPLIES[0])
            .count(),
        1
    );

    let envelope = SessionMessageEnvelope {
        id: SessionMessageId::parse("legacy-idle-resume-input").unwrap(),
        source: SessionMessageSource::Session {
            session_id: PARENT.into(),
        },
        target_session_id: child_id.into(),
        kind: SessionMessageKind::PeerMessage,
        body: SessionMessageBody::Content(SessionMessageContent::text(FOLLOWUP)),
        created_at: chrono::Utc::now(),
        thread_id: None,
        in_reply_to: None,
        attempt: None,
        correlation_id: None,
    };
    let admission = state
        .session_messenger
        .admit(envelope.clone())
        .await
        .unwrap();
    assert_eq!(
        state.session_inbox.inspect(child_id).await.unwrap().pending,
        1
    );
    assert!(!state
        .session_inbox
        .was_admitted(child_id, &envelope.id)
        .await
        .unwrap());
    let receipt = state.session_messenger.activate(&admission).await.unwrap();
    assert_eq!(
        receipt.activation,
        SessionActivationDisposition::ActivationReserved
    );
    provider_entered(&probe, 2, &state, child_id).await;
    let resumed_run_id = state
        .session_activation_router
        .current_run_id(child_id)
        .await
        .unwrap();
    assert_ne!(
        resumed_run_id, first_run_id,
        "idle resumption gets a real new reservation"
    );
    assert_eq!(
        owned_worker_pids(),
        workers,
        "ordinary legacy reuses its actual local worker"
    );
    let resumed_runs = pending_runs(&home, child_id);
    assert_eq!(resumed_runs.len(), 1, "only one resumed physical Run");
    assert_ne!(resumed_runs[0].1.id, first_runs[0].1.id);
    let resumed_run: bamboo_subagent::RunSpec =
        serde_json::from_value(resumed_runs[0].1.body.clone()).unwrap();
    assert_eq!(
        resumed_run.activation_run_id.as_deref(),
        Some(resumed_run_id.as_str())
    );
    assert_eq!(resumed_run.logical_session, first_run.logical_session);
    assert_eq!(resumed_run.project_id, first_run.project_id);
    assert_eq!(resumed_run.initial_session_messages.len(), 1);
    let input = &resumed_run.initial_session_messages[0];
    assert_eq!(input.envelope, envelope);
    assert_eq!(input.activation_run_id, resumed_run_id);
    assert_eq!(
        input.canonical_claim_generation,
        admission.delivery.generation
    );
    let permission = resumed_run.permission_policy.as_ref().unwrap();
    assert_eq!(permission.session_id, child_id);
    assert!(
        !permission.bypass_permissions
            && !permission.auto_approve_permissions
            && !permission.inherit_session_grants
    );
    let request = probe.requests.lock().unwrap()[1].clone();
    assert_eq!(
        request["messages"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|m| m["content"] == FOLLOWUP)
            .count(),
        1
    );
    assert_eq!(
        request["messages"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|m| m["content"] == REPLIES[0])
            .count(),
        1
    );

    // The real second provider call is still held. Duplicate activation must
    // notify this runnable owner, not merely race its eventual backend failure.
    let duplicate = state.session_messenger.activate(&admission).await.unwrap();
    assert_eq!(
        duplicate.activation,
        SessionActivationDisposition::ActiveNotified
    );
    assert_eq!(duplicate.delivery, admission.delivery);
    assert_eq!(
        state
            .session_activation_router
            .current_run_id(child_id)
            .await
            .as_deref(),
        Some(resumed_run_id.as_str())
    );
    {
        let runners = state.agent_runners.read().await;
        let runner = runners.get(child_id).unwrap();
        assert_eq!(runner.run_id, resumed_run_id);
        assert!(matches!(runner.status, AgentStatus::Running));
    }
    assert_eq!(
        pending_runs(&home, child_id),
        resumed_runs,
        "duplicate wake publishes no second Run"
    );
    assert_eq!(
        probe.requests.lock().unwrap().len(),
        2,
        "one initial plus one resumed provider call"
    );
    release_provider(&probe, 2);
    let completed = wait_completed(&state, &home, child_id, &resumed_run_id).await;
    assert_eq!(completed.created_at, first.created_at);
    assert_eq!(completed.root_session_id, first.root_session_id);
    assert_eq!(completed.project_id_meta(), first.project_id_meta());
    assert_eq!(
        completed
            .messages
            .iter()
            .filter(|m| m.id == envelope.id.as_str())
            .count(),
        1
    );
    assert_eq!(
        completed
            .messages
            .iter()
            .filter(|m| bamboo_domain::is_matching_session_message(m, &envelope))
            .count(),
        1
    );
    assert_eq!(
        completed
            .messages
            .iter()
            .filter(|m| m.role == Role::Assistant && m.content == REPLIES[0])
            .count(),
        1
    );
    assert_eq!(
        completed
            .messages
            .iter()
            .filter(|m| m.role == Role::Assistant && m.content == REPLIES[1])
            .count(),
        1
    );
    assert!(completed
        .session_inbox_admission()
        .unwrap()
        .contains(&envelope.id));
    let cursor = serde_json::to_value(completed.session_inbox_admission().unwrap()).unwrap();
    assert_eq!(
        cursor["admitted"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|v| v["id"] == envelope.id.as_str())
            .count(),
        1
    );
    assert!(state
        .session_inbox
        .was_admitted(child_id, &envelope.id)
        .await
        .unwrap());
    let backlog = state.session_inbox.inspect(child_id).await.unwrap();
    assert_eq!((backlog.pending, backlog.claimed), (0, 0));
    let cold = SessionStoreV2::new(home.clone())
        .await
        .unwrap()
        .load_session(child_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(&cold.messages).unwrap(),
        serde_json::to_value(&completed.messages).unwrap(),
        "canonical transcript survives a fresh storage instance"
    );
    let repeated = state.session_messenger.admit(envelope).await.unwrap();
    assert_eq!(
        repeated.delivery, admission.delivery,
        "same input retains the one durable receipt"
    );
    let settled = state.session_messenger.activate(&repeated).await.unwrap();
    assert_eq!(
        settled.activation,
        SessionActivationDisposition::ActivationCoalesced
    );
    assert!(state
        .session_activation_router
        .current_run_id(child_id)
        .await
        .is_none());
    assert!(pending_runs(&home, child_id).is_empty());
    assert_eq!(probe.requests.lock().unwrap().len(), 2);
    assert_eq!(
        state
            .agent_runners
            .read()
            .await
            .get(child_id)
            .unwrap()
            .run_id,
        resumed_run_id
    );
    assert_eq!(
        serde_json::to_value(state.permission_section.snapshot().data.as_ref()).unwrap(),
        policy_before
    );
    state.shutdown().await;
    server_handle.stop(true).await;
}
