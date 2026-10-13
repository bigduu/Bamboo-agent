//! Explicit local legacy compatibility, through the real Host/SDK/Actor runner.
//!
//! The only simulated component is a loopback HTTP model provider. This proves
//! a Host SessionMessenger idle-Child reservation; it does not claim coverage
//! of the nested Worker's private SessionMessenger assembly. Linux /proc gives
//! independent PID/PPID evidence that both Child requests use the real worker binary,
//! rather than the default native Tokio backend or a mocked ExternalChildRunner.
#![cfg(target_os = "linux")]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

use actix_web::{web, App, HttpResponse, HttpServer};
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{
    FunctionCall, ToolCall, ToolExecutionContext, ToolOutcome, ToolSchema,
};
use bamboo_agent_core::{Message, Role, Session};
use bamboo_domain::{
    ContextBlock, ContextBlockPriority, ContextBlockStability, ContextBlockType, ProviderModelRef,
    SessionActivationDisposition, SessionMessageBody, SessionMessageContent,
    SessionMessageEnvelope, SessionMessageId, SessionMessageKind, SessionMessageSource,
};
use bamboo_server::app_state::{AgentStatus, AppState, MemoryStore};
use bamboo_server::tools::ToolSurface;
use bamboo_storage::SessionStoreV2;
use bamboo_tools::guide::{context::GuideBuildContext, EnhancedPromptBuilder};
use bamboo_tools::permission::{PermissionMatcher, PermissionMatcherKind, PermissionType};
use serde_json::{json, Value};
use tokio::sync::Notify;

const WAIT: Duration = Duration::from_secs(45);
const MODEL: &str = "legacy-local-test";
const PARENT_MODEL: &str = "legacy-local-parent-test";
const PARENT: &str = "legacy-idle-parent";
const INITIAL: &str = "LEGACY_INITIAL: return one plain answer without tools";
const FOLLOWUP: &str = "LEGACY_FOLLOWUP: continue this same Child without tools";
const REPLIES: [&str; 2] = ["LEGACY_FIRST_COMPLETED", "LEGACY_RESUME_COMPLETED"];
const PARENT_ACK: &str = "LEGACY_PARENT_ACK: received the Child result";

#[derive(Default)]
struct ProviderProbe {
    requests: Mutex<Vec<Value>>,
    child_requests: Mutex<Vec<Value>>,
    parent_requests: Mutex<Vec<Value>>,
    parent_consumed: Mutex<[bool; 2]>,
    parent_notifications: Mutex<Vec<(usize, String)>>,
    unexpected_requests: Mutex<Vec<Value>>,
    child_id: Mutex<Option<String>>,
    storage: Mutex<Option<Arc<dyn Storage>>>,
    parent_tool_names: Mutex<BTreeSet<String>>,
    entered: Notify,
    released: AtomicUsize,
    release: Notify,
}

fn plain_response(id: &str, reply: &str) -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/event-stream")
        .body(format!(
            "data: {}\n\ndata: [DONE]\n\n",
            json!({"id":id,"object":"chat.completion.chunk",
                "choices":[{"index":0,"delta":{"content":reply},"finish_reason":"stop"}]})
        ))
}

fn unexpected_request(probe: &ProviderProbe, body: &Value, reason: &str) -> HttpResponse {
    // Retain bounded identity evidence rather than repeating entire prompts in
    // the request handler and again in the terminal assertion.
    let summary = json!({
        "reason": reason,
        "model": body["model"],
        "messages": body["messages"].as_array().map(|messages| messages.iter().enumerate()
            .map(|(index, message)| message_diagnostic(index, message)).collect::<Vec<_>>()),
    });
    eprintln!("unexpected legacy fixture provider request: {summary}");
    probe.unexpected_requests.lock().unwrap().push(summary);
    HttpResponse::BadRequest().body("unexpected legacy fixture provider request")
}

fn text_diagnostic(content: &str) -> Value {
    json!({"bytes":content.len(), "sha256":bamboo_domain::sha256_hex(content.as_bytes())})
}

fn message_diagnostic(index: usize, message: &Value) -> Value {
    let content = message["content"].as_str().unwrap_or_default();
    json!({"index":index, "role":message["role"],
        "context_type":content.lines().find_map(|line| line.strip_prefix("context_type: ")),
        "content":text_diagnostic(content)})
}

fn parent_context_diagnostic(parent: &Session, expected_guide: &str) -> Value {
    json!({
        "prompt_snapshot_present":parent.prompt_snapshot.is_some(),
        "snapshot_guide":parent.prompt_snapshot.as_ref()
            .and_then(|snapshot| snapshot.tool_guide_context.as_deref()).map(text_diagnostic),
        "actual_root_guide":text_diagnostic(expected_guide),
        "ledger":parent.model_context_state.as_ref().map(|state| state.events.iter().map(|event|
            json!({"context_type":event.block_type, "content":text_diagnostic(&event.rendered_text)})
        ).collect::<Vec<_>>()),
    })
}

fn is_persisted_parent_context(message: &Value, parent: &Session, expected_guide: &str) -> bool {
    if message["role"] != "user" {
        return false;
    }
    let Some(content) = message["content"].as_str() else {
        return false;
    };
    // OpenAI wire messages omit the Host metadata. The production runner
    // checkpoints these byte-authoritative events before provider dispatch;
    // match that evidence instead of trusting marker-shaped user text.
    if parent.model_context_state.as_ref().is_some_and(|state| {
        state.events.iter().any(|event| {
            matches!(
                event.block_type,
                ContextBlockType::SessionIdentity
                    | ContextBlockType::Workspace
                    | ContextBlockType::ProjectResources
                    | ContextBlockType::ExternalMemory
            ) && event.rendered_text == content
        })
    }) {
        return true;
    }
    // Stable tool guidance is built from the final provider catalog, outside
    // the ledger. Neither the unfiltered Root surface nor PromptSnapshot is
    // authoritative for this freshly generated provider prefix.
    !expected_guide.is_empty() && content == expected_guide
}

async fn model_response(body: web::Json<Value>, probe: web::Data<ProviderProbe>) -> HttpResponse {
    let body = body.into_inner();
    probe.requests.lock().unwrap().push(body.clone());
    let messages = body["messages"].as_array();
    if messages.is_some_and(|messages| {
        messages.iter().any(|message| {
            message["role"] == "system"
                && message["content"].as_str().is_some_and(|content| {
                    content.contains("Bamboo's background Dream consolidator")
                })
        })
    }) {
        return unexpected_request(&probe, &body, "background Dream call");
    }
    if body["model"] == PARENT_MODEL {
        // Incremental Child outcomes can legitimately wake an idle parent even
        // when create used wait=false. Historical notifications alone must not
        // authorize another request: inspect only inputs after the last ACK.
        let child_id = probe.child_id.lock().unwrap().clone();
        let (Some(child_id), Some(messages)) = (child_id, messages) else {
            return unexpected_request(&probe, &body, "unrecognized parent call");
        };
        let storage = probe.storage.lock().unwrap().clone().unwrap();
        let parent = match storage.load_session(PARENT).await {
            Ok(Some(parent)) => parent,
            _ => return unexpected_request(&probe, &body, "Parent context checkpoint unavailable"),
        };
        let schemas: Vec<ToolSchema> = match serde_json::from_value(body["tools"].clone()) {
            Ok(schemas) => schemas,
            Err(_) => return unexpected_request(&probe, &body, "invalid Parent tool catalog"),
        };
        {
            let allowed_names = probe.parent_tool_names.lock().unwrap();
            let mut seen = BTreeSet::new();
            if schemas.is_empty()
                || schemas.iter().any(|schema| {
                    schema.schema_type != "function"
                        || (!allowed_names.contains(&schema.function.name)
                            && schema.function.name
                                != bamboo_domain::DISCOVERY_CONTROL_FALLBACK_TOOL_NAME
                            // ServerSkillRuntime adds these exact progressive
                            // catalog tools around the Root executor per run.
                            && !matches!(schema.function.name.as_str(), "skills_list" | "skills_read"))
                        || !seen.insert(schema.function.name.as_str())
                })
            {
                eprintln!(
                    "Parent catalog mismatch: actual={:?}, root={allowed_names:?}",
                    schemas
                        .iter()
                        .map(|schema| (&schema.schema_type, &schema.function.name))
                        .collect::<Vec<_>>()
                );
                return unexpected_request(&probe, &body, "unknown or repeated Parent tool");
            }
        }
        // Use the real post-filter provider catalog. The Root surface also
        // contains HostOnly/legacy tools that production removes, while the
        // compatibility adapter appends its public discovery-control schema.
        let expected_guide = ContextBlock::new(
            ContextBlockType::ToolGuide,
            ContextBlockPriority::High,
            ContextBlockStability::SessionStable,
            "Tool & Connected-Server Guide",
            EnhancedPromptBuilder::build(None, &schemas, &GuideBuildContext::default()),
        )
        .render_runtime_context_text();
        let prefix = format!(
            "Runtime notification: child session `{child_id}` finished with status `completed`."
        );
        let last_ack = messages.iter().rposition(|message| {
            message["role"] == "assistant" && message["content"] == PARENT_ACK
        });
        let mut fresh = Vec::new();
        let mut initial_seen = false;
        for (index, message) in messages
            .iter()
            .enumerate()
            .skip(last_ack.map_or(0, |index| index + 1))
        {
            if message["role"] == "system"
                || is_persisted_parent_context(message, &parent, &expected_guide)
            {
                continue;
            }
            if last_ack.is_none()
                && !initial_seen
                && fresh.is_empty()
                && message["role"] == "user"
                && message["content"] == INITIAL
            {
                initial_seen = true;
                continue;
            }
            let content = message["content"].as_str().unwrap_or_default();
            let outcome = REPLIES
                .iter()
                .position(|reply| content.contains(&format!("Child final response:\n{reply}\n\n")));
            if message["role"] != "user" || !content.starts_with(&prefix) || outcome.is_none() {
                eprintln!(
                    "unrecognized Parent input: message={}, durable_context={}",
                    message_diagnostic(index, message),
                    parent_context_diagnostic(&parent, &expected_guide)
                );
                return unexpected_request(&probe, &body, "unknown new parent input");
            }
            fresh.push((outcome.unwrap(), content.to_string()));
        }
        if fresh.is_empty() {
            return unexpected_request(&probe, &body, "replay-only parent call");
        }
        {
            let mut consumed = probe.parent_consumed.lock().unwrap();
            let mut next = *consumed;
            for (outcome, _) in &fresh {
                if next[*outcome] {
                    return unexpected_request(&probe, &body, "repeated parent outcome");
                }
                next[*outcome] = true;
            }
            *consumed = next;
        }
        probe.parent_notifications.lock().unwrap().extend(fresh);
        let index = {
            let mut requests = probe.parent_requests.lock().unwrap();
            let index = requests.len();
            requests.push(body);
            index
        };
        eprintln!("accepted legacy fixture parent notification request {index}");
        return plain_response(&format!("legacy-parent-{index}"), PARENT_ACK);
    }
    if body["model"] != MODEL {
        return unexpected_request(&probe, &body, "unknown model");
    }
    let index = {
        let mut requests = probe.child_requests.lock().unwrap();
        let index = requests.len();
        requests.push(body.clone());
        index
    };
    probe.entered.notify_one();
    let Some(reply) = REPLIES.get(index) else {
        return unexpected_request(&probe, &body, "extra Child call");
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
    plain_response(&format!("legacy-local-{index}"), reply)
}

async fn provider_entered(probe: &ProviderProbe, count: usize, state: &AppState, id: &str) {
    let observed = tokio::time::timeout(WAIT, async {
        loop {
            let wake = probe.entered.notified();
            if probe.child_requests.lock().unwrap().len() >= count {
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
    let requests = probe.child_requests.lock().unwrap();
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

fn parent_outcome(
    parent: &Session,
    child: &Session,
    run_id: &str,
) -> Option<(usize, SessionMessageEnvelope)> {
    let source: Value = serde_json::from_str(
        child
            .metadata
            .get("runtime.child_completion_source_v1")
            .unwrap(),
    )
    .unwrap();
    assert_eq!(source["activation_run_id"], run_id);
    let matches: Vec<_> = parent
        .messages
        .iter()
        .enumerate()
        .filter(|(_, message)| {
            message
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("child_completion_source"))
                == Some(&source)
        })
        .collect();
    if matches.is_empty() {
        return None;
    }
    assert_eq!(
        matches.len(),
        1,
        "one canonical Parent outcome per Child seal"
    );
    let (index, message) = matches[0];
    let envelope: SessionMessageEnvelope =
        serde_json::from_value(message.metadata.as_ref().unwrap()["session_message"].clone())
            .unwrap();
    assert_eq!(envelope.target_session_id, PARENT);
    assert_eq!(envelope.kind, SessionMessageKind::ChildOutcome);
    assert_eq!(
        envelope.source,
        SessionMessageSource::Runtime {
            subsystem: "child_completion_coordinator".into()
        }
    );
    let SessionMessageBody::ChildOutcome(outcome) = &envelope.body else {
        panic!("Parent completion must retain its typed ChildOutcome")
    };
    assert_eq!(outcome.child_session_id, child.id);
    assert_eq!(outcome.status, "completed");
    assert!(outcome.error.is_none());
    let final_message = child
        .messages
        .iter()
        .rev()
        .find(|m| m.role == Role::Assistant)
        .unwrap();
    assert_eq!(
        outcome.result.as_deref(),
        Some(final_message.content.as_str())
    );
    assert_eq!(
        outcome.provider_message.as_ref().unwrap().metadata["child_completion_source"],
        source
    );
    assert!(bamboo_domain::is_matching_session_message(
        message, &envelope
    ));
    assert!(parent
        .session_inbox_admission()
        .unwrap()
        .contains(&envelope.id));
    Some((index, envelope))
}

async fn wait_parent_settled(
    state: &AppState,
    probe: &ProviderProbe,
    child: &Session,
    run_id: &str,
    outcome_index: usize,
) -> Session {
    tokio::time::timeout(WAIT, async {
        loop {
            {
                let unexpected = probe.unexpected_requests.lock().unwrap();
                assert!(
                    unexpected.is_empty(),
                    "unexpected requests: {unexpected:#?}"
                );
            }
            let parent = state.storage.load_session(PARENT).await.unwrap().unwrap();
            if matches!(
                parent.last_run_status().as_deref(),
                Some("error" | "timeout" | "cancelled")
            ) {
                panic!(
                    "Parent notification run failed: {:?}",
                    parent.last_run_error()
                );
            }
            if let Some((index, envelope)) = parent_outcome(&parent, child, run_id) {
                let acknowledged = state
                    .session_inbox
                    .was_admitted(PARENT, &envelope.id)
                    .await
                    .unwrap();
                let inbox = state.session_inbox.inspect(PARENT).await.unwrap();
                let completed = state
                    .agent_runners
                    .read()
                    .await
                    .get(PARENT)
                    .is_some_and(|runner| matches!(runner.status, AgentStatus::Completed));
                let idle = state
                    .session_activation_router
                    .current_run_id(PARENT)
                    .await
                    .is_none();
                let answered = parent.messages[index + 1..].iter().any(|message| {
                    message.role == Role::Assistant && message.content == PARENT_ACK
                });
                if acknowledged
                    && completed
                    && idle
                    && answered
                    && parent.last_run_status().as_deref() == Some("completed")
                    && (inbox.pending, inbox.claimed) == (0, 0)
                    && !inbox.activation_pending()
                    && probe.parent_consumed.lock().unwrap()[outcome_index]
                {
                    return parent;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Child outcome, canonical Parent ACK and idle Root must converge")
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
                web::get().to(|| async {
                    HttpResponse::Ok().json(json!({"data":[{"id":MODEL},{"id":PARENT_MODEL}]}))
                }),
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
        "providers":{"openai":{"api_key":"local-fixture-only","base_url":base_url,"model":PARENT_MODEL,
            "runtime_models":[MODEL,PARENT_MODEL]}},
        "defaults":{"chat":{"provider":"openai","model":PARENT_MODEL}},
        "subagents":{"runtime":"actor","executor":"bamboo_runtime",
            "worker_bin":env!("CARGO_BIN_EXE_bamboo"),"worker_args":["subagent-worker"],
            "fabric_dir":home.join("fabric"),"max_concurrent":1}
    })).unwrap()).unwrap();
    let state = AppState::new_with_memory_store(home.clone(), MemoryStore::new(jiandu))
        .await
        .unwrap();
    *probe.storage.lock().unwrap() = Some(state.storage.clone());
    // Bound catalog identity by the actual Root surface; request-time guide
    // rendering uses the provider's final filtered schemas instead.
    *probe.parent_tool_names.lock().unwrap() = state
        .tool_factory
        .get(ToolSurface::Root)
        .list_tools()
        .into_iter()
        .map(|schema| schema.function.name)
        .collect();
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
    let mut parent = Session::new(PARENT, PARENT_MODEL);
    parent.model_ref = Some(ProviderModelRef::new("openai", PARENT_MODEL));
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
                "model":format!("openai:{MODEL}"),
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
    *probe.child_id.lock().unwrap() = Some(child_id.to_string());
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

    // Let the first incremental outcome settle before making the Child active
    // again. Otherwise the Root may correctly establish a new implicit wait
    // and withhold its first ACK, which this fixture's strict replay detector
    // must not confuse with a duplicated notification.
    wait_parent_settled(&state, &probe, &first, &first_run_id, 0).await;

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
    let request = probe.child_requests.lock().unwrap()[1].clone();
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
        probe.child_requests.lock().unwrap().len(),
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
    assert_eq!(probe.child_requests.lock().unwrap().len(), 2);
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
    // The SDK can release the Child router before publishing its Parent
    // callback. Observe the final exact source through the Root checkpoint and
    // receipt, and let its real reasoning owner settle before stopping HTTP.
    let parent = wait_parent_settled(&state, &probe, &completed, &resumed_run_id, 1).await;
    let notifications = probe.parent_notifications.lock().unwrap().clone();
    for (outcome, text) in notifications {
        let (child, run_id) = match outcome {
            0 => (&first, first_run_id.as_str()),
            1 => (&completed, resumed_run_id.as_str()),
            _ => unreachable!(),
        };
        let (index, envelope) = parent_outcome(&parent, child, run_id).expect(
            "accepted provider notification must have exact canonical envelope/source proof",
        );
        assert_eq!(parent.messages[index].content, text);
        assert!(state
            .session_inbox
            .was_admitted(PARENT, &envelope.id)
            .await
            .unwrap());
    }
    state.shutdown().await;
    server_handle.stop(true).await;
    let child_requests = probe.child_requests.lock().unwrap();
    let parent_requests = probe.parent_requests.lock().unwrap();
    let unexpected_requests = probe.unexpected_requests.lock().unwrap();
    let requests = probe.requests.lock().unwrap();
    eprintln!(
        "legacy fixture provider requests classified: Child={}, parent={}, unexpected={}",
        child_requests.len(),
        parent_requests.len(),
        unexpected_requests.len()
    );
    assert_eq!(
        child_requests.len(),
        2,
        "Child requests: {child_requests:#?}"
    );
    assert!(
        unexpected_requests.is_empty(),
        "unclassified provider calls: {unexpected_requests:#?}"
    );
    assert_eq!(
        requests.len(),
        child_requests.len() + parent_requests.len(),
        "every provider call must have an explicit fixture owner: {requests:#?}"
    );
}
