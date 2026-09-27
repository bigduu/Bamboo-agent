//! Real serve/SubAgent/Host/current_exe worker. Only the provider is simulated.
//! Explicit Ultra + a normally selected deny-all profile opts in; no manual claim.
#![cfg(unix)]
use actix_web::{web, App, HttpResponse, HttpServer};
use bamboo_agent_core::storage::Storage;
use bamboo_domain::{
    ActorActivationStatus, ActorDirectoryPort, ActorSnapshotLimits, ActorSnapshotPort,
    ActorSnapshotPrincipal,
};
use bamboo_storage::SessionStoreV2;
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Mutex,
    },
    time::Duration,
};

struct Probe {
    root_calls: AtomicUsize,
    child_calls: AtomicUsize,
    ready: AtomicBool,
    release: AtomicBool,
    wake: tokio::sync::Notify,
    requests: Mutex<Vec<Value>>,
    child_id: Mutex<Option<String>>,
    first_worker_run: Mutex<Option<(PathBuf, Vec<u8>)>>,
    data: PathBuf,
    workspace: PathBuf,
    reasoning: bool,
    replay: bool,
    glob: bool,
    correction: bool,
    retry: bool,
    recovery: bool,
    two: Option<TwoFollowups>,
    deliveries: Mutex<Vec<(bamboo_domain::SessionMessageId, u64)>>,
    runs: Mutex<Vec<(bamboo_subagent::MsgId, bamboo_subagent::RunSpec)>>,
    ack_fault: Mutex<Option<AckWriteFault>>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TwoFollowups {
    Complete,
    Overflow,
    SecondAckFailure,
}
fn followup_call(id: &str, index: usize) -> Value {
    let mut result = call(json!({"action":"send_message","child_session_id":id,
        "message":format!("OWNED_ROOT_CORRECTION_{index}"),"interrupt_running":false,"auto_run":false}));
    result["tool_calls"][0]["id"] = json!(format!("owned-followup-{index}"));
    result
}
async fn two_root_step(body: &Value, probe: &Probe, round: usize) -> (Value, &'static str) {
    if round == 0 {
        return (
            call(
                json!({"action":"create","title":"Two corrections", "responsibility":"Answer each bounded input without tools",
            "prompt":"Return one plain reply per assigned input", "subagent_type":"plain-reply", "workspace":probe.workspace,"auto_run":true}),
            ),
            "tool_calls",
        );
    }
    let tool_id = if round == 1 {
        "subagent-create".to_owned()
    } else {
        format!("owned-followup-{}", round - 1)
    };
    let text = body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .find(|m| m["role"] == "tool" && m["tool_call_id"] == tool_id)
        .unwrap()["content"]
        .as_str()
        .unwrap();
    let result: Value = serde_json::from_str(text)
        .unwrap_or_else(|_| panic!("actual Root tool result: {}", bounded_diagnostic(text, 512)));
    if round == 1 {
        assert_eq!(result["status"], "running_in_background");
        let store = SessionStoreV2::new(probe.data.clone()).await.unwrap();
        let id = store
            .list_index_entries()
            .await
            .into_iter()
            .find(|r| r.parent_session_id.as_deref() == Some("plain-root"))
            .unwrap()
            .id;
        *probe.child_id.lock().unwrap() = Some(id.clone());
        return (followup_call(&id, 1), "tool_calls");
    }
    assert_eq!(result["status"], "message_delivered_live");
    let id = probe.child_id.lock().unwrap().clone().unwrap();
    assert_eq!(result["child_session_id"], id);
    probe.deliveries.lock().unwrap().push((
        bamboo_domain::SessionMessageId::parse(result["message_id"].as_str().unwrap()).unwrap(),
        result["inbox_generation"].as_u64().unwrap(),
    ));
    let limit = if probe.two == Some(TwoFollowups::Overflow) {
        3
    } else {
        2
    };
    if round <= limit {
        (followup_call(&id, round), "tool_calls")
    } else {
        (json!({"content":"ROOT_DONE"}), "stop")
    }
}
async fn observe_two_provider(body: &Value, probe: &Probe, index: usize) {
    assert!(index < 3, "no fourth provider admission");
    assert!(body["tools"].is_null() || body["tools"].as_array().is_some_and(Vec::is_empty));
    let id = probe.child_id.lock().unwrap().clone().unwrap();
    let store = std::sync::Arc::new(SessionStoreV2::new(probe.data.clone()).await.unwrap());
    let child = store.load_session(&id).await.unwrap().unwrap();
    let actor = store.inspect_actor(&id).await.unwrap();
    assert_eq!(actor.actor.current_attempt, 1);
    assert_eq!(
        actor.activation.as_ref().unwrap().status,
        ActorActivationStatus::Running
    );
    let current = claimed_required_runs(&probe.data);
    assert_eq!(current.len(), 1, "actual single worker Run in physical cur");
    let message: bamboo_subagent::InboxMessage = serde_json::from_slice(&current[0].1).unwrap();
    let run = current[0].2.clone();
    assert_eq!(
        run.activation_run_id.as_deref(),
        Some(actor.activation.as_ref().unwrap().run_id.as_str())
    );
    assert_eq!(
        run.logical_session
            .as_ref()
            .unwrap()
            .creation
            .as_ref()
            .unwrap()
            .created_at,
        child.created_at
    );
    {
        let mut runs = probe.runs.lock().unwrap();
        assert_eq!(runs.len(), index);
        if let Some((previous_id, previous)) = runs.last() {
            assert_ne!(&message.id, previous_id);
            assert!(run.execution_epoch > previous.execution_epoch);
            assert_eq!(run.logical_session, previous.logical_session);
            assert_eq!(run.project_id, previous.project_id);
            assert_eq!(run.activation_run_id, previous.activation_run_id);
        }
        assert_eq!(run.initial_session_messages.len(), usize::from(index > 0));
        runs.push((message.id, run));
    }
    let inbox = bamboo_storage::FileSessionInbox::new(
        store.clone(),
        bamboo_domain::SessionInboxLimits::default(),
    );
    let deliveries = probe.deliveries.lock().unwrap().clone();
    for (i, (envelope, generation)) in deliveries.iter().enumerate() {
        let marker = format!("OWNED_ROOT_CORRECTION_{}", i + 1);
        let admitted = i < index;
        assert_eq!(
            body["messages"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|m| m["content"] == marker)
                .count(),
            usize::from(admitted)
        );
        assert_eq!(
            child
                .messages
                .iter()
                .filter(|m| m.id == envelope.as_str())
                .count(),
            usize::from(admitted)
        );
        assert_eq!(
            bamboo_domain::SessionInboxPort::was_admitted(&inbox, &id, envelope)
                .await
                .unwrap(),
            admitted
        );
        if admitted {
            let input = child
                .messages
                .iter()
                .position(|m| m.id == envelope.as_str())
                .unwrap();
            assert_eq!(
                child.messages[input - 1].content,
                format!("OWNED_REPLY_{i}")
            );
            assert!(child.session_inbox_admission().unwrap().contains(envelope));
            assert_eq!(
                child
                    .session_inbox_admission()
                    .unwrap()
                    .admitted
                    .iter()
                    .find(|r| &r.id == envelope)
                    .unwrap()
                    .sequence,
                *generation
            );
        }
    }
    if index == 1 && probe.two == Some(TwoFollowups::SecondAckFailure) {
        let path = probe
            .data
            .join(store.resolve_rel_path(&id).await.unwrap())
            .join("inbox/admitted");
        *probe.ack_fault.lock().unwrap() = Some(AckWriteFault::new(path));
    }
}
fn call(args: Value) -> Value {
    json!({"tool_calls":[{"index":0,"id":format!("subagent-{}",args["action"].as_str().unwrap()),"type":"function","function":{"name":"SubAgent","arguments":args.to_string()}}]})
}
fn provider_request_shape(body: &Value) -> String {
    let messages = body["messages"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let text_has = |needle: &str| {
        messages.iter().any(|message| {
            message["content"]
                .as_str()
                .is_some_and(|text| text.contains(needle))
        })
    };
    let mut diagnostic = json!({
        "stream":body["stream"].as_bool(),
        "messages":messages.len(),
        "roles":messages.iter().take(16).map(|m| match m["role"].as_str() {
            Some("system") => "system", Some("user") => "user", Some("assistant") => "assistant",
            Some("tool") => "tool", _ => "other",
        }).collect::<Vec<_>>(),
        "content_shapes":messages.iter().take(16).map(|m| {
            if m["content"].is_string() { "string" } else if m["content"].is_array() { "array" } else { "other" }
        }).collect::<Vec<_>>(),
        "tools":body["tools"].as_array().map(|tools| tools.iter().take(8).map(|tool| {
            tool["function"]["name"].as_str().unwrap_or("unknown").chars().take(32).collect::<String>()
        }).collect::<Vec<_>>()),
        "correction_exact":messages.iter().filter(|m| m["content"] == "CORRECTION_FROM_ACTUAL_ROOT").count(),
        "correction_embedded":text_has("CORRECTION_FROM_ACTUAL_ROOT"),
        "array_correction_text_parts":messages.iter().flat_map(|m| m["content"].as_array().into_iter().flatten()).filter(|part| part["text"].as_str().is_some_and(|text| text.contains("CORRECTION_FROM_ACTUAL_ROOT"))).count(),
        "assignment":text_has("Delegated child assignment"),
        "task_evaluation":text_has("You are a task progress evaluator"),
        "permission_reviewer":text_has("You are a security reviewer"),
        "max_tokens":body["max_tokens"].as_u64(),
        "max_completion_tokens":body["max_completion_tokens"].as_u64(),
    }).to_string();
    let mut end = diagnostic.len().min(1900);
    while !diagnostic.is_char_boundary(end) {
        end -= 1;
    }
    diagnostic.truncate(end);
    diagnostic
}
fn bounded_diagnostic(text: &str, cap: usize) -> &str {
    let mut end = text.len().min(cap);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}
async fn provider_host_phase(probe: &Probe) -> Value {
    let Some(id) = probe.child_id.lock().unwrap().clone() else {
        return json!({"child_id_available":false});
    };
    let Ok(store) = SessionStoreV2::new(probe.data.clone()).await else {
        return json!({"host_store_available":false});
    };
    let store = std::sync::Arc::new(store);
    let actor = store.inspect_actor(&id).await.ok().map(|entry| {
        json!({"attempt":entry.actor.current_attempt,"state":entry.actor.state,
            "activation":entry.activation.map(|a| json!({"status":a.status,
                "run_id":a.run_id,"inbox_generation":a.inbox_generation}))})
    });
    let child = store.load_session(&id).await.ok().flatten();
    let inbox =
        bamboo_storage::FileSessionInbox::new(store, bamboo_domain::SessionInboxLimits::default());
    let generation = bamboo_domain::SessionInboxPort::inspect(&inbox, &id)
        .await
        .ok()
        .map(|state| state.generation);
    json!({"actor":actor,"last_run_status":child.and_then(|c| c.last_run_status()),
        "inbox_generation":generation})
}
fn print_bounded_retry_log(data: &Path) {
    use std::io::{Read, Seek, SeekFrom};
    let Ok(mut file) = std::fs::File::open(data.join("host.log")) else {
        return;
    };
    let Ok(length) = file.metadata().map(|metadata| metadata.len()) else {
        return;
    };
    if file
        .seek(SeekFrom::Start(length.saturating_sub(16 * 1024)))
        .is_err()
    {
        return;
    }
    let mut tail = Vec::new();
    if file.take(16 * 1024).read_to_end(&mut tail).is_err() {
        return;
    }
    for line in String::from_utf8_lossy(&tail)
        .lines()
        .filter(|line| {
            line.contains("Turn") && line.contains("failed") && line.contains("Retrying")
        })
        .take(4)
    {
        eprintln!(
            "actual existing Engine retry: {}",
            bounded_diagnostic(line, 1024)
        );
    }
}
fn claimed_required_runs(data: &Path) -> Vec<(PathBuf, Vec<u8>, bamboo_subagent::RunSpec)> {
    use std::io::Read;
    let mut runs = Vec::new();
    for mailbox in std::fs::read_dir(data.join("broker/mailboxes"))
        .unwrap()
        .take(17)
    {
        let mailbox = mailbox.unwrap();
        if !mailbox
            .file_name()
            .to_str()
            .unwrap()
            .starts_with("required-worker-")
        {
            continue;
        }
        for entry in std::fs::read_dir(mailbox.path().join("cur"))
            .unwrap()
            .take(5)
        {
            let path = entry.unwrap().path();
            let mut bytes = Vec::new();
            std::fs::File::open(&path)
                .unwrap()
                .take(256 * 1024 + 1)
                .read_to_end(&mut bytes)
                .unwrap();
            assert!(bytes.len() <= 256 * 1024);
            let message: bamboo_subagent::InboxMessage = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(message.kind, bamboo_subagent::InboxKind::Run);
            runs.push((path, bytes, serde_json::from_value(message.body).unwrap()));
        }
    }
    assert!(runs.len() <= 2);
    runs
}
async fn provider(body: web::Json<Value>, probe: web::Data<Probe>) -> HttpResponse {
    let body = body.into_inner();
    probe.requests.lock().unwrap().push(body.clone());
    let mut emits_reasoning = false;
    let (delta, finish) = if body["model"] == "plain-child" {
        let child_call = probe.child_calls.fetch_add(1, Ordering::SeqCst);
        eprintln!(
            "actual plain-child request index={child_call} shape={}",
            provider_request_shape(&body)
        );
        probe.ready.store(true, Ordering::SeqCst);
        while !probe.release.load(Ordering::SeqCst) {
            let wake = probe.wake.notified();
            if probe.release.load(Ordering::SeqCst) {
                break;
            }
            wake.await;
        }
        if probe.two.is_some() {
            observe_two_provider(&body, &probe, child_call).await;
        }
        if probe.retry {
            {
                let runs = claimed_required_runs(&probe.data);
                let mut first = probe.first_worker_run.lock().unwrap();
                if child_call == 0 {
                    assert_eq!(runs.len(), 1);
                    assert!(runs[0].2.initial_session_messages.is_empty());
                    *first = Some((runs[0].0.clone(), runs[0].1.clone()));
                } else if child_call == 1 {
                    assert_eq!(runs.len(), 2);
                    let (old_path, old_bytes) = first.as_ref().unwrap();
                    let old = runs.iter().find(|r| &r.0 == old_path).unwrap();
                    assert_eq!(&old.1, old_bytes, "old unACKed Run remains untouched");
                    let new = runs.iter().find(|r| &r.0 != old_path).unwrap();
                    assert_ne!(
                        new.0.parent().unwrap().parent(),
                        old_path.parent().unwrap().parent()
                    );
                    assert_eq!(new.2.logical_session, old.2.logical_session);
                    assert_eq!(new.2.project_id, old.2.project_id);
                    assert_ne!(new.2.activation_run_id, old.2.activation_run_id);
                    assert_eq!(new.2.initial_session_messages.len(), 1);
                    let mailbox = |path: &Path| {
                        path.parent()
                            .unwrap()
                            .parent()
                            .unwrap()
                            .file_name()
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .to_owned()
                    };
                    eprintln!(
                        "actual distinct worker mailboxes old={} new={} old Run still unACKed",
                        mailbox(old_path),
                        mailbox(&new.0)
                    );
                    assert_eq!(
                        new.2.logical_session.as_ref().unwrap().session_id,
                        probe.child_id.lock().unwrap().as_ref().unwrap().as_str()
                    );
                }
            }
            eprintln!(
                "actual plain-child request index={child_call} Host phase={}",
                provider_host_phase(&probe).await
            );
        }
        if probe.recovery && child_call == 1 {
            assert_eq!(child_call, 1, "one replacement provider entry");
            assert_eq!(
                body["messages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|m| m["content"] == "CORRECTION_FROM_ACTUAL_ROOT")
                    .count(),
                1
            );
            let id = probe.child_id.lock().unwrap().clone().unwrap();
            let store = std::sync::Arc::new(SessionStoreV2::new(probe.data.clone()).await.unwrap());
            let actor = store.inspect_actor(&id).await.unwrap();
            assert_eq!(actor.actor.current_attempt, 2);
            assert_eq!(
                actor.activation.unwrap().status,
                ActorActivationStatus::Running
            );
            let child = store.load_session(&id).await.unwrap().unwrap();
            let input = child
                .messages
                .iter()
                .find(|m| m.content == "CORRECTION_FROM_ACTUAL_ROOT")
                .unwrap();
            assert!(input
                .metadata
                .as_ref()
                .unwrap()
                .get("_bamboo_owned_input_checkpoint")
                .is_some());
            let envelope_id = bamboo_domain::SessionMessageId::parse(input.id.clone()).unwrap();
            assert!(child
                .session_inbox_admission()
                .unwrap()
                .contains(&envelope_id));
            let inbox = bamboo_storage::FileSessionInbox::new(
                store,
                bamboo_domain::SessionInboxLimits::default(),
            );
            assert!(
                bamboo_domain::SessionInboxPort::was_admitted(&inbox, &id, &envelope_id)
                    .await
                    .unwrap(),
                "real permanent Host ACK precedes provider"
            );
        }
        if (probe.correction || probe.retry) && child_call == 1 {
            assert_eq!(
                body["messages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|message| message["content"] == "CORRECTION_FROM_ACTUAL_ROOT")
                    .count(),
                1
            );
            let id = probe.child_id.lock().unwrap().clone().unwrap();
            let store = std::sync::Arc::new(SessionStoreV2::new(probe.data.clone()).await.unwrap());
            let canonical = store.load_session(&id).await.unwrap().unwrap();
            if probe.retry {
                let entry = store.inspect_actor(&id).await.unwrap();
                assert_eq!(entry.actor.current_attempt, 2);
                assert_eq!(
                    entry.activation.unwrap().status,
                    ActorActivationStatus::Running
                );
                assert!(!canonical
                    .messages
                    .iter()
                    .any(|m| m.role == bamboo_domain::Role::Assistant));
            }
            let message = canonical
                .messages
                .iter()
                .find(|m| m.content == "CORRECTION_FROM_ACTUAL_ROOT")
                .unwrap();
            if probe.correction {
                let first = canonical
                    .messages
                    .iter()
                    .position(|m| m.content == "INITIAL_BEFORE_CORRECTION")
                    .unwrap();
                assert_eq!(canonical.messages[first + 1].id, message.id);
            }
            let envelope_id = bamboo_domain::SessionMessageId::parse(message.id.clone()).unwrap();
            assert!(
                canonical
                    .session_inbox_admission()
                    .unwrap()
                    .contains(&envelope_id),
                "Host first reply + correction checkpoint precede second provider admission"
            );
            let inbox = bamboo_storage::FileSessionInbox::new(
                store,
                bamboo_domain::SessionInboxLimits::default(),
            );
            assert!(
                bamboo_domain::SessionInboxPort::was_admitted(&inbox, &id, &envelope_id).await.unwrap(),
                "actual Host ACK must precede this provider request, not become true after admission"
            );
        }
        if probe.two.is_some() {
            (
                json!({"content":format!("OWNED_REPLY_{child_call}")}),
                "stop",
            )
        } else if probe.reasoning || (probe.retry && child_call == 0) {
            emits_reasoning = true;
            (json!({"content":"UNSUPPORTED_REPLY"}), "stop")
        } else if (probe.correction || probe.recovery) && child_call == 0 {
            (json!({"content":"INITIAL_BEFORE_CORRECTION"}), "stop")
        } else if probe.glob && child_call == 0 {
            assert_eq!(body["tools"].as_array().unwrap().len(), 1);
            assert_eq!(body["tools"][0]["function"]["name"], "Glob");
            (
                json!({"tool_calls":[{"index":0,"id":"owned-glob-once","type":"function","function":{"name":"Glob","arguments":json!({"pattern":"owned-marker.txt","limit":1}).to_string()}}]}),
                "tool_calls",
            )
        } else {
            if probe.glob {
                let results: Vec<_> = body["messages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|m| m["role"] == "tool" && m["tool_call_id"] == "owned-glob-once")
                    .collect();
                assert_eq!(results.len(), 1);
                assert!(results[0]["content"]
                    .as_str()
                    .unwrap()
                    .contains(probe.workspace.join("owned-marker.txt").to_str().unwrap()));
            }
            (json!({"content":"FENCED_PLAIN_REPLY"}), "stop")
        }
    } else if body["model"] == "plain-root" && body["tools"].to_string().contains("SubAgent") {
        let round = probe.root_calls.fetch_add(1, Ordering::SeqCst);
        if probe.two.is_some() {
            two_root_step(&body, &probe, round).await
        } else {
            match round {
                0 => (
                    call(
                        json!({"action":"create","title":"Zero-tool Child","responsibility":if probe.glob {"Verify the assigned file using Glob once, then return one plain reply"} else {"Return exactly one plain answer; do not use tools"},"prompt":if probe.glob {"Find owned-marker.txt with Glob once and report the result"} else {"Respond with a plain answer inside this task boundary"},"subagent_type":if probe.glob {"explorer"} else {"plain-reply"},"workspace":probe.workspace,"auto_run":probe.correction || probe.retry}),
                    ),
                    "tool_calls",
                ),
                1 => {
                    let content = body["messages"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .rev()
                        .find(|message| {
                            message["role"] == "tool"
                                && message["tool_call_id"] == "subagent-create"
                        })
                        .expect("actual Root SubAgent create tool result")["content"]
                        .as_str()
                        .unwrap();
                    let diagnostic: String = content.chars().take(512).collect();
                    let created: Value = serde_json::from_str(content).unwrap_or_else(|_| {
                        panic!("actual Root create did not return JSON: {diagnostic}")
                    });
                    assert_eq!(
                        created["status"],
                        if probe.correction || probe.retry {
                            "running_in_background"
                        } else {
                            "created"
                        },
                        "actual Root create failed: {diagnostic}"
                    );
                    let store = SessionStoreV2::new(probe.data.clone()).await.unwrap();
                    let id = store
                        .list_index_entries()
                        .await
                        .into_iter()
                        .find(|row| row.parent_session_id.as_deref() == Some("plain-root"))
                        .unwrap()
                        .id;
                    *probe.child_id.lock().unwrap() = Some(id.clone());
                    if probe.recovery {
                        // Ordinary, pre-activation fixture configuration only: the
                        // Host still probes/spawns and writes every claim/placement.
                        let mut child = store.load_session(&id).await.unwrap().unwrap();
                        child
                            .metadata
                            .insert("child_watchdog.max_total_secs".into(), "30".into());
                        store.save_session(&child).await.unwrap();
                    }
                    if probe.correction || probe.retry {
                        tokio::time::timeout(Duration::from_secs(30), async {
                            while !probe.ready.load(Ordering::SeqCst) {
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                        })
                        .await
                        .unwrap();
                        if probe.retry {
                            tokio::time::timeout(Duration::from_secs(60), async {
                                loop {
                                    let actual = store.inspect_actor(&id).await.unwrap();
                                    let child = store.load_session(&id).await.unwrap().unwrap();
                                    if actual.activation.as_ref().unwrap().status
                                        == ActorActivationStatus::Failed
                                        && child.last_run_status().as_deref() == Some("error")
                                    {
                                        eprintln!(
                                            "actual first Failed run={} error={}",
                                            actual.activation.as_ref().unwrap().run_id,
                                            bounded_diagnostic(
                                                child.last_run_error().as_deref().unwrap_or("none"),
                                                512
                                            )
                                        );
                                        assert_eq!(actual.actor.current_attempt, 1);
                                        assert!(!child
                                            .messages
                                            .iter()
                                            .any(|m| m.role == bamboo_domain::Role::Assistant));
                                        break;
                                    }
                                    tokio::time::sleep(Duration::from_millis(10)).await;
                                }
                            })
                            .await
                            .unwrap();
                        }
                        (
                            call(json!({"action":"send_message","child_session_id":id,
                        "message":"CORRECTION_FROM_ACTUAL_ROOT","interrupt_running":false,"auto_run":true})),
                            "tool_calls",
                        )
                    } else {
                        (
                            call(
                                json!({"action":"run","child_session_id":id,"reset_to_last_user":false}),
                            ),
                            "tool_calls",
                        )
                    }
                }
                2 if probe.recovery => {
                    tokio::time::timeout(Duration::from_secs(30), async {
                        while !probe.ready.load(Ordering::SeqCst) {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                    })
                    .await
                    .unwrap();
                    (
                        call(
                            json!({"action":"send_message", "child_session_id":probe.child_id.lock().unwrap().clone().unwrap(),
                    "message":"CORRECTION_FROM_ACTUAL_ROOT", "interrupt_running":false, "auto_run":true}),
                        ),
                        "tool_calls",
                    )
                }
                4 if probe.recovery => (
                    call(
                        json!({"action":"run", "child_session_id":probe.child_id.lock().unwrap().clone().unwrap(),
                    "reset_to_last_user":false}),
                    ),
                    "tool_calls",
                ),
                2 if probe.replay => (
                    call(
                        json!({"action":"run","child_session_id":probe.child_id.lock().unwrap().clone().unwrap(),"reset_to_last_user":false}),
                    ),
                    "tool_calls",
                ),
                _ => (json!({"content":"ROOT_DONE"}), "stop"),
            }
        }
    } else {
        (json!({"content":"auxiliary"}), "stop")
    };
    // Keep the published #1414 reasoning-only fixture shape when adding this
    // correction case: answer+reasoning in one delta is not a ReasoningToken.
    let reasoning_event = if emits_reasoning {
        let reasoning = json!({"id":"plain-actor","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"reasoning_content":"UNSUPPORTED_NATIVE_REASONING"},"finish_reason":null}]});
        format!("data: {reasoning}\n\n")
    } else {
        String::new()
    };
    let event = json!({"id":"plain-actor","object":"chat.completion.chunk","choices":[{"index":0,"delta":delta,"finish_reason":finish}]});
    HttpResponse::Ok()
        .content_type("text/event-stream")
        .body(format!(
            "{reasoning_event}data: {event}\n\ndata: [DONE]\n\n"
        ))
}
// ACK-only real filesystem fault. No claim, placement, cursor or lease is
// authored by this helper; Unix root cannot provide this fault boundary.
struct AckWriteFault(PathBuf, std::fs::Permissions);
impl AckWriteFault {
    fn new(path: PathBuf) -> Self {
        use std::os::unix::fs::PermissionsExt;
        std::fs::create_dir_all(&path).unwrap();
        let original = std::fs::metadata(&path).unwrap().permissions();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o555)).unwrap();
        Self(path, original)
    }
}
impl Drop for AckWriteFault {
    fn drop(&mut self) {
        std::fs::set_permissions(&self.0, self.1.clone()).unwrap();
    }
}
async fn await_host_pre_ack_cut(
    probe: &Probe,
    id: &str,
) -> (bamboo_domain::Session, chrono::DateTime<chrono::Utc>) {
    use bamboo_domain::SessionInboxPort;
    let store = std::sync::Arc::new(SessionStoreV2::new(probe.data.clone()).await.unwrap());
    let inbox = bamboo_storage::FileSessionInbox::new(
        store.clone(),
        bamboo_domain::SessionInboxLimits::default(),
    );
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let child = store.load_session(id).await.unwrap().unwrap();
            let entry = store.inspect_actor(id).await.unwrap();
            if entry.activation.as_ref().unwrap().status == ActorActivationStatus::Failed
                && child.last_run_status().as_deref() == Some("error")
            {
                assert!(
                    child.last_run_error().unwrap().contains("ACK unresolved"),
                    "actual error: {}",
                    bounded_diagnostic(&child.last_run_error().unwrap(), 512)
                );
                assert_eq!(entry.actor.current_attempt, 1);
                let placement = entry.activation.unwrap().placement_ref.unwrap();
                assert_eq!(placement.class, bamboo_domain::ActorPlacementClass::Local);
                assert!(placement
                    .lease_id
                    .starts_with("owned-initial-release-v1:required-worker-"));
                let input = child.messages.last().unwrap();
                assert_eq!(input.content, "CORRECTION_FROM_ACTUAL_ROOT");
                assert!(input
                    .metadata
                    .as_ref()
                    .unwrap()
                    .get("_bamboo_owned_input_checkpoint")
                    .is_some());
                assert!(!inbox
                    .was_admitted(
                        id,
                        &bamboo_domain::SessionMessageId::parse(input.id.clone()).unwrap()
                    )
                    .await
                    .unwrap());
                let leases = inbox
                    .inspect_owned_leases(id, 2, chrono::Utc::now())
                    .await
                    .unwrap();
                assert_eq!(leases.len(), 1);
                assert_eq!(leases[0].generation, 1);
                assert_eq!(leases[0].reclaim_count, 0);
                assert!(
                    leases[0].expires_at <= chrono::Utc::now() + chrono::Duration::seconds(100)
                );
                assert_eq!(
                    probe.child_calls.load(Ordering::SeqCst),
                    1,
                    "unreleased continuation has no provider admission"
                );
                return (child, leases[0].expires_at);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}
struct Host(Child);
impl Drop for Host {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn start(data: &Path, port: u16) -> Host {
    let log = std::fs::File::create(data.join("host.log")).unwrap();
    Host(
        Command::new(env!("CARGO_BIN_EXE_bamboo"))
            .args([
                "serve",
                "--bind",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--data-dir",
            ])
            .arg(data)
            .current_dir(data)
            .env("HOME", data.join("home"))
            .env("BAMBOO_JIANDU_DATA_DIR", data.join("jiandu"))
            .env("RUST_LOG", "warn")
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap(),
    )
}
async fn fixture(
    ultra: bool,
    reasoning: bool,
    correction: bool,
    glob: bool,
    retry: bool,
    recovery: bool,
) {
    Box::pin(fixture_with_followups(
        ultra, reasoning, correction, glob, retry, recovery, None,
    ))
    .await;
}
async fn fixture_with_followups(
    ultra: bool,
    reasoning: bool,
    correction: bool,
    glob: bool,
    retry: bool,
    recovery: bool,
    two: Option<TwoFollowups>,
) {
    let temp = tempfile::tempdir().unwrap();
    let temp_root = temp.path().canonicalize().unwrap();
    let data = temp_root.join("host");
    let workspace = temp_root.join("workspace");
    std::fs::create_dir_all(&data).unwrap();
    std::fs::create_dir_all(&workspace).unwrap();
    let projects = bamboo_projects::ProjectStore::open(&data).unwrap();
    let project = projects
        .create_with_project_path("plain", None, workspace.to_string_lossy(), vec![])
        .unwrap();
    let agents = projects.paths().project_home(&project.id).join("agents");
    std::fs::create_dir_all(&agents).unwrap();
    // This is the public named-profile selection path, not a test capability switch.
    if glob {
        std::fs::write(workspace.join("owned-marker.txt"), "actual read-only file").unwrap();
    }
    std::fs::write(agents.join(if glob {"explorer.md"} else {"plain-reply.md"}), if glob {
        "---\nschema_version: 1\nname: explorer\ndescription: Verify one assigned file\nmodel_hint: openai:plain-child\ntools:\n  allow: [Glob]\n---\nUse Glob exactly once for the assigned file, then return one plain answer.\n"
    } else {
        "---\nschema_version: 1\nname: plain-reply\ndescription: One bounded plain reply\nmodel_hint: openai:plain-child\ntools:\n  deny: [Bash, Read, Glob, Edit, Write]\n---\nDo not use tools; return one plain answer and stop.\n"
    }).unwrap();
    let probe = web::Data::new(Probe {
        root_calls: AtomicUsize::new(0),
        child_calls: AtomicUsize::new(0),
        ready: AtomicBool::new(false),
        release: AtomicBool::new(false),
        wake: Default::default(),
        requests: Mutex::new(vec![]),
        child_id: Mutex::new(None),
        first_worker_run: Mutex::new(None),
        data: data.clone(),
        workspace: workspace.clone(),
        reasoning,
        replay: ultra && !reasoning && !correction && !glob && !retry && !recovery && two.is_none(),
        correction,
        glob,
        retry,
        recovery,
        two,
        deliveries: Mutex::new(vec![]),
        runs: Mutex::new(vec![]),
        ack_fault: Mutex::new(None),
    });
    let server_probe = probe.clone();
    let server = HttpServer::new(move || {
        App::new()
            .app_data(server_probe.clone())
            .route("/v1/chat/completions", web::post().to(provider))
            .route(
                "/v1/models",
                web::get().to(|| async {
                    HttpResponse::Ok()
                        .json(json!({"data":[{"id":"plain-root"},{"id":"plain-child"}]}))
                }),
            )
    })
    .workers(1)
    .bind(("127.0.0.1", 0))
    .unwrap();
    let provider_url = format!("http://{}/v1", server.addrs()[0]);
    let running = server.run();
    let handle = running.handle();
    actix_web::rt::spawn(running);
    std::fs::write(data.join("config.json"),serde_json::to_vec(&json!({"provider":"openai","features":{"provider_model_ref":true},"providers":{"openai":{"api_key":"fixture","base_url":provider_url,"model":"plain-root"}},"defaults":{"chat":{"provider":"openai","model":"plain-root"}},"subagents":{"runtime":"actor","executor":"bamboo_runtime","max_concurrent":1}})).unwrap()).unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut host = start(&data, port);
    let base = format!("http://127.0.0.1:{port}/api/v1");
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            assert!(
                host.0.try_wait().unwrap().is_none(),
                "Host exited: {}",
                std::fs::read_to_string(data.join("host.log")).unwrap()
            );
            if client
                .get(format!("{base}/health"))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let response = client.post(format!("{base}/chat")).json(&json!({"session_id":"plain-root","message":"Delegate one plain answer","model":"plain-root","provider":"openai","model_ref":{"provider":"openai","model":"plain-root"},"thinking_mode":if ultra {"ultra"} else {"standard"},"permission_mode":"bypass","workspace_path":workspace,"project_id":project.id})).send().await.unwrap();
    assert!(
        response.status().is_success(),
        "chat: {}",
        response.text().await.unwrap()
    );
    assert!(client
        .post(format!("{base}/execute/plain-root"))
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .status()
        .is_success());
    tokio::time::timeout(Duration::from_secs(60), async {
        while !probe.ready.load(Ordering::SeqCst)
            || (two.is_some() && probe.child_id.lock().unwrap().is_none())
        {
            assert!(host.0.try_wait().unwrap().is_none(), "actual Host exited");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let id = probe.child_id.lock().unwrap().clone().unwrap();
    let store = SessionStoreV2::new(data.clone()).await.unwrap();
    let before = store.load_session(&id).await.unwrap().unwrap();
    assert_eq!(before.root_session_id, "plain-root");
    assert_eq!(before.parent_session_id.as_deref(), Some("plain-root"));
    assert_eq!(before.spawn_depth, 1);
    let binding: Value = serde_json::from_str(&before.metadata["child.named_profile.v1"]).unwrap();
    assert_eq!(
        binding["tools"],
        if glob { json!(["Glob"]) } else { json!([]) }
    );
    if glob {
        assert!(before.agent_runtime_state.as_ref().unwrap().read_only);
    }
    let mut recovery_prefix = None;
    if recovery {
        let inbox = bamboo_storage::FileSessionInbox::new(
            std::sync::Arc::new(SessionStoreV2::new(data.clone()).await.unwrap()),
            bamboo_domain::SessionInboxLimits::default(),
        );
        tokio::time::timeout(Duration::from_secs(30), async {
            while !bamboo_domain::SessionInboxPort::inspect(&inbox, &id)
                .await
                .unwrap()
                .activation_pending()
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let fault = AckWriteFault::new(
            data.join(store.resolve_rel_path(&id).await.unwrap())
                .join("inbox/admitted"),
        );
        probe.release.store(true, Ordering::SeqCst);
        probe.wake.notify_waiters();
        let (cut, deadline) = await_host_pre_ack_cut(&probe, &id).await;
        drop(host); // Actual kill/wait; absence of Host is not our pre-release proof.
        drop(fault);
        probe.release.store(false, Ordering::SeqCst);
        let delay = (deadline - chrono::Utc::now()).to_std().unwrap_or_default();
        tokio::time::sleep(delay + Duration::from_millis(10)).await;
        host = start(&data, port);
        tokio::time::timeout(Duration::from_secs(30), async {
            while !client
                .get(format!("{base}/health"))
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                assert!(host.0.try_wait().unwrap().is_none());
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        let cold_cut = SessionStoreV2::new(data.clone())
            .await
            .unwrap()
            .load_session(&id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(&cold_cut.messages).unwrap(),
            serde_json::to_value(&cut.messages).unwrap()
        );
        recovery_prefix = Some(cut);
        assert!(client.post(format!("{base}/chat")).json(&json!({"session_id":"plain-root","message":"Recover the same checkpointed Child through run(false)","model":"plain-root","provider":"openai","thinking_mode":"ultra"})).send().await.unwrap().status().is_success());
        assert!(client
            .post(format!("{base}/execute/plain-root"))
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .status()
            .is_success());
        tokio::time::timeout(Duration::from_secs(60), async {
            while probe.child_calls.load(Ordering::SeqCst) < 2 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
    }
    let requests = probe.requests.lock().unwrap().clone();
    let child_wire = requests
        .iter()
        .find(|body| body["model"] == "plain-child")
        .unwrap();
    assert!(
        (glob
            && child_wire["tools"]
                .as_array()
                .is_some_and(|tools| tools.len() == 1 && tools[0]["function"]["name"] == "Glob"))
            || (!glob
                && (child_wire["tools"].is_null()
                    || child_wire["tools"].as_array().is_some_and(Vec::is_empty)))
    );
    let mut original_activation = None;
    if ultra {
        let running = store.inspect_actor(&id).await.unwrap();
        assert_eq!(running.actor.actor_id, id);
        assert_eq!(running.actor.session_created_at, before.created_at);
        assert_eq!(
            running.activation.as_ref().unwrap().status,
            ActorActivationStatus::Running
        );
        assert_eq!(running.actor.current_attempt, if recovery { 2 } else { 1 });
        original_activation = running.activation;
    }
    if correction {
        let inbox = bamboo_storage::FileSessionInbox::new(
            std::sync::Arc::new(SessionStoreV2::new(data.clone()).await.unwrap()),
            bamboo_domain::SessionInboxLimits::default(),
        );
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let backlog = bamboo_domain::SessionInboxPort::inspect(&inbox, &id)
                    .await
                    .unwrap();
                if backlog.activation_pending() {
                    let actual = store.load_session(&id).await.unwrap().unwrap();
                    assert!(!actual
                        .messages
                        .iter()
                        .any(|m| m.content == "CORRECTION_FROM_ACTUAL_ROOT"));
                    break;
                }
                assert!(
                    host.0.try_wait().unwrap().is_none(),
                    "actual Host exited before eligible correction delivery"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    if let Some(mode) = two {
        let count = if mode == TwoFollowups::Overflow { 3 } else { 2 };
        let inbox = bamboo_storage::FileSessionInbox::new(
            std::sync::Arc::new(SessionStoreV2::new(data.clone()).await.unwrap()),
            bamboo_domain::SessionInboxLimits::default(),
        );
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let backlog = bamboo_domain::SessionInboxPort::inspect(&inbox, &id)
                    .await
                    .unwrap();
                if probe.deliveries.lock().unwrap().len() == count
                    && backlog.pending == count
                    && backlog.claimed == 0
                {
                    assert_eq!(backlog.generation, count as u64);
                    let actual = store.load_session(&id).await.unwrap().unwrap();
                    assert!(!actual
                        .messages
                        .iter()
                        .any(|m| m.content.starts_with("OWNED_ROOT_CORRECTION_")));
                    break;
                }
                assert!(host.0.try_wait().unwrap().is_none());
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    probe.release.store(true, Ordering::SeqCst);
    probe.wake.notify_waiters();
    let completed = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let child = store.load_session(&id).await.unwrap().unwrap();
            let status = child.last_run_status();
            if matches!(status.as_deref(), Some("completed" | "error" | "cancelled"))
                && (!retry
                    || store
                        .inspect_actor(&id)
                        .await
                        .unwrap()
                        .actor
                        .current_attempt
                        == 2)
            {
                break child;
            }
            assert!(host.0.try_wait().unwrap().is_none(), "actual Host exited");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    if probe.replay {
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                let parent = store.load_session("plain-root").await.unwrap().unwrap();
                if parent
                    .messages
                    .iter()
                    .any(|message| message.content == "ROOT_DONE")
                {
                    break;
                }
                assert!(host.0.try_wait().unwrap().is_none(), "actual Host exited");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            probe.root_calls.load(Ordering::SeqCst) >= 4,
            "actual second SubAgent run was dispatched"
        );
    }
    if let Some(mode) = two {
        finish_two_fixture(
            &probe,
            mode,
            host,
            store,
            &client,
            &base,
            before,
            original_activation.unwrap(),
            completed,
        )
        .await;
        handle.stop(true).await;
        return;
    }
    let child_calls = probe.child_calls.load(Ordering::SeqCst);
    let expected_calls = if correction || retry || recovery || (glob && !reasoning) {
        2
    } else {
        1
    };
    if child_calls != expected_calls {
        if retry {
            print_bounded_retry_log(&data);
        }
        let actor = store.inspect_actor(&id).await.map(|entry| {
            json!({"state":entry.actor.state,"attempt":entry.actor.current_attempt,
                "activation":entry.activation.map(|a| a.status)})
        });
        let inbox = bamboo_storage::FileSessionInbox::new(
            std::sync::Arc::new(SessionStoreV2::new(data.clone()).await.unwrap()),
            bamboo_domain::SessionInboxLimits::default(),
        );
        let backlog = bamboo_domain::SessionInboxPort::inspect(&inbox, &id).await;
        let mut diagnostic = json!({
            "status":completed.last_run_status(),
            "error":completed.last_run_error().map(|s| s.chars().take(384).collect::<String>()),
            "actor":actor.map_err(|e| e.to_string()),
            "inbox":backlog.map(|b| json!({"pending":b.pending,"claimed":b.claimed,
                "generation":b.generation,"eligible":b.activation_generation})).map_err(|e| e.to_string()),
            "tail":completed.messages.iter().rev().take(3).map(|m| json!({
                "id":m.id,"role":m.role,"content":m.content.chars().take(128).collect::<String>(),
                "owned_marker":m.metadata.as_ref().is_some_and(|v| v.get("_bamboo_owned_input_checkpoint").is_some())
            })).collect::<Vec<_>>()
        }).to_string();
        let mut end = diagnostic.len().min(2048);
        while !diagnostic.is_char_boundary(end) {
            end -= 1;
        }
        diagnostic.truncate(end);
        assert_eq!(
            child_calls, expected_calls,
            "actual Child phase: {diagnostic}"
        );
    }
    if ultra {
        let finished = store.inspect_actor(&id).await.unwrap();
        let finished_activation = finished.activation.unwrap();
        if correction {
            let original = original_activation.as_ref().unwrap();
            assert_eq!(
                finished_activation.lease_expires_at,
                original.lease_expires_at
            );
            assert_eq!(finished_activation.lease_owner, original.lease_owner);
            assert_eq!(finished_activation.lease_epoch, original.lease_epoch);
        }
        if retry {
            let original = original_activation.as_ref().unwrap();
            assert_eq!(finished.actor.current_attempt, 2);
            assert!(finished_activation.lease_epoch > original.lease_epoch);
            assert_ne!(finished_activation.run_id, original.run_id);
            assert_ne!(finished_activation.lease_owner, original.lease_owner);
        }
        assert_eq!(
            finished_activation.status,
            if reasoning {
                ActorActivationStatus::Failed
            } else {
                ActorActivationStatus::Succeeded
            },
            "actual terminal: status={:?}, error={:?}",
            completed.last_run_status(),
            completed
                .last_run_error()
                .map(|error| error.chars().take(384).collect::<String>())
        );
    } else {
        let observed = store
            .actor_subtree_snapshot(
                ActorSnapshotPrincipal::host_owner(),
                "plain-root",
                &id,
                ActorSnapshotLimits::default(),
            )
            .await
            .unwrap();
        assert!(observed
            .nodes
            .iter()
            .find(|node| node.actor_id == id)
            .unwrap()
            .activation
            .is_none());
    }
    assert_eq!(
        completed
            .messages
            .iter()
            .filter(|message| message.content == "FENCED_PLAIN_REPLY")
            .count(),
        usize::from(!reasoning)
    );
    assert!(!completed
        .messages
        .iter()
        .any(|message| message.content.contains("UNSUPPORTED_REPLY")));
    // Genuine cold reopen, without a live worker/Host cache or a manual authority claim.
    drop(host);
    drop(store);
    let reopened = std::sync::Arc::new(SessionStoreV2::new(data).await.unwrap());
    let cold = reopened.load_session(&id).await.unwrap().unwrap();
    assert_eq!(cold.id, before.id);
    assert_eq!(cold.created_at, before.created_at);
    assert_eq!(cold.parent_session_id, before.parent_session_id);
    assert_eq!(
        serde_json::to_value(&cold.messages).unwrap(),
        serde_json::to_value(&completed.messages).unwrap()
    );
    if glob && !reasoning {
        let tail = &cold.messages[cold.messages.len() - 3..];
        assert_eq!(tail[0].role, bamboo_domain::Role::Assistant);
        let calls = tail[0].tool_calls.as_ref().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].id, "owned-glob-once");
        assert_eq!(calls[0].function.name, "Glob");
        assert_eq!(tail[1].role, bamboo_domain::Role::Tool);
        assert_eq!(tail[1].tool_call_id.as_deref(), Some("owned-glob-once"));
        assert_eq!(tail[1].tool_success, Some(true));
        assert!(tail[1].content.contains("owned-marker.txt"));
        assert_eq!(tail[2].content, "FENCED_PLAIN_REPLY");
        assert!(tail.iter().all(|m| m.reasoning.is_none()));
        assert!(tail[0].metadata.is_none() && tail[2].metadata.is_none());
        let lifecycle = tail[1].metadata.as_ref().unwrap();
        assert!(lifecycle["elapsed_ms"].as_u64().is_some());
        assert_eq!(
            lifecycle,
            &json!({
                "elapsed_ms": lifecycle["elapsed_ms"], "is_mutating": false,
                "auto_approved": true, "tool_name": "Glob", "success": true,
            })
        );
    }
    if correction {
        assert!(
            bamboo_domain::PermissionAuditSnapshot::from_metadata(&cold.metadata)
                .unwrap()
                .audit_revision
                > bamboo_domain::PermissionAuditSnapshot::from_metadata(&before.metadata)
                    .unwrap()
                    .audit_revision,
            "the second Run must durably confirm a fresh Host permission audit"
        );
        let first = cold
            .messages
            .iter()
            .position(|m| m.content == "INITIAL_BEFORE_CORRECTION")
            .unwrap();
        assert_eq!(cold.messages[first].role, bamboo_domain::Role::Assistant);
        assert_eq!(cold.messages[first + 1].role, bamboo_domain::Role::User);
        assert_eq!(
            cold.messages[first + 1].content,
            "CORRECTION_FROM_ACTUAL_ROOT"
        );
        assert_eq!(
            cold.messages[first + 2].role,
            bamboo_domain::Role::Assistant
        );
        assert_eq!(cold.messages[first + 2].content, "FENCED_PLAIN_REPLY");
        assert_eq!(first + 3, cold.messages.len());
        assert_eq!(
            cold.messages
                .iter()
                .filter(|m| m.metadata.as_ref().is_some_and(|metadata| metadata
                    .get("_bamboo_owned_input_checkpoint")
                    .is_some()))
                .count(),
            1
        );
        let message = cold
            .messages
            .iter()
            .find(|m| m.content == "CORRECTION_FROM_ACTUAL_ROOT")
            .unwrap();
        assert_eq!(
            cold.messages
                .iter()
                .filter(|m| m.content == "CORRECTION_FROM_ACTUAL_ROOT")
                .count(),
            1
        );
        let envelope_id = bamboo_domain::SessionMessageId::parse(message.id.clone()).unwrap();
        assert!(cold
            .session_inbox_admission()
            .unwrap()
            .contains(&envelope_id));
        let inbox = bamboo_storage::FileSessionInbox::new(
            reopened.clone(),
            bamboo_domain::SessionInboxLimits::default(),
        );
        assert!(
            bamboo_domain::SessionInboxPort::was_admitted(&inbox, &id, &envelope_id)
                .await
                .unwrap()
        );
        let backlog = bamboo_domain::SessionInboxPort::inspect(&inbox, &id)
            .await
            .unwrap();
        assert_eq!((backlog.pending, backlog.claimed), (0, 0));
    }
    if retry {
        assert_eq!(cold.root_session_id, before.root_session_id);
        assert_eq!(cold.spawn_depth, before.spawn_depth);
        assert_eq!(cold.project_id_meta(), before.project_id_meta());
        let corrective = cold
            .messages
            .iter()
            .position(|m| m.content == "CORRECTION_FROM_ACTUAL_ROOT")
            .unwrap();
        assert_eq!(
            serde_json::to_value(&cold.messages[..corrective]).unwrap(),
            serde_json::to_value(&before.messages).unwrap()
        );
        assert_eq!(cold.messages[corrective].role, bamboo_domain::Role::User);
        assert_eq!(cold.messages[corrective + 1].content, "FENCED_PLAIN_REPLY");
        assert_eq!(corrective + 2, cold.messages.len());
        assert_eq!(
            cold.messages
                .iter()
                .filter(|m| m
                    .metadata
                    .as_ref()
                    .is_some_and(|v| v.get("_bamboo_owned_input_checkpoint").is_some()))
                .count(),
            1
        );
        let id =
            bamboo_domain::SessionMessageId::parse(cold.messages[corrective].id.clone()).unwrap();
        assert!(cold.session_inbox_admission().unwrap().contains(&id));
        let inbox = bamboo_storage::FileSessionInbox::new(
            reopened.clone(),
            bamboo_domain::SessionInboxLimits::default(),
        );
        assert!(
            bamboo_domain::SessionInboxPort::was_admitted(&inbox, &cold.id, &id)
                .await
                .unwrap()
        );
        let backlog = bamboo_domain::SessionInboxPort::inspect(&inbox, &cold.id)
            .await
            .unwrap();
        assert_eq!(
            (backlog.pending, backlog.claimed, backlog.generation),
            (0, 0, 1)
        );
        assert!(reopened
            .finish_activation(
                &original_activation.as_ref().unwrap().fence(),
                chrono::Utc::now(),
                bamboo_domain::ActorActivationFinish::Failed
            )
            .await
            .is_err());
        assert_eq!(
            serde_json::to_value(
                &reopened
                    .load_session(&cold.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .messages
            )
            .unwrap(),
            serde_json::to_value(&cold.messages).unwrap()
        );
    }
    if recovery {
        let prefix = recovery_prefix.as_ref().unwrap();
        assert_eq!(probe.child_calls.load(Ordering::SeqCst), 2);
        assert_eq!(cold.created_at, before.created_at);
        assert_eq!(cold.project_id_meta(), before.project_id_meta());
        assert_eq!(
            serde_json::to_value(&cold.messages[..prefix.messages.len()]).unwrap(),
            serde_json::to_value(&prefix.messages).unwrap()
        );
        assert_eq!(cold.messages.len(), prefix.messages.len() + 1);
        assert_eq!(cold.messages.last().unwrap().content, "FENCED_PLAIN_REPLY");
        assert_eq!(
            cold.messages[cold.messages.len() - 3].content,
            "INITIAL_BEFORE_CORRECTION"
        );
        assert_eq!(
            cold.messages[cold.messages.len() - 2].content,
            "CORRECTION_FROM_ACTUAL_ROOT"
        );
        assert_eq!(
            cold.messages
                .iter()
                .filter(|m| m
                    .metadata
                    .as_ref()
                    .is_some_and(|v| v.get("_bamboo_owned_input_checkpoint").is_some()))
                .count(),
            1
        );
        let inbox = bamboo_storage::FileSessionInbox::new(
            reopened.clone(),
            bamboo_domain::SessionInboxLimits::default(),
        );
        let backlog = bamboo_domain::SessionInboxPort::inspect(&inbox, &id)
            .await
            .unwrap();
        assert_eq!(
            (backlog.pending, backlog.claimed, backlog.generation),
            (0, 0, 1)
        );
    }
    if ultra {
        assert_eq!(
            reopened
                .inspect_actor(&id)
                .await
                .unwrap()
                .actor
                .current_attempt,
            if retry || recovery { 2 } else { 1 }
        );
    }
    handle.stop(true).await;
}
#[actix_web::test]
async fn actual_zero_tool_child_uses_host_actor_commit_and_preserves_legacy() {
    for (ultra, reasoning) in [(true, false), (true, true), (false, false)] {
        Box::pin(fixture(ultra, reasoning, false, false, false, false)).await;
    }
}

#[actix_web::test]
async fn actual_owned_readonly_glob_has_one_pair_and_cold_host_history() {
    Box::pin(fixture(true, false, false, true, false, false)).await;
}

#[actix_web::test]
async fn actual_owned_child_admits_root_correction_before_second_provider() {
    Box::pin(fixture(true, false, true, false, false, false)).await;
}

#[actix_web::test]
async fn actual_failed_owned_child_retries_one_new_root_input() {
    Box::pin(fixture(true, false, false, false, true, false)).await;
}

#[actix_web::test]
async fn actual_run_false_recovers_one_expired_pre_ack_checkpoint() {
    Box::pin(fixture(true, false, false, false, false, true)).await;
}

async fn finish_two_fixture(
    probe: &Probe,
    mode: TwoFollowups,
    mut host: Host,
    store: SessionStoreV2,
    client: &reqwest::Client,
    base: &str,
    before: bamboo_domain::Session,
    original: bamboo_domain::ActorActivation,
    completed: bamboo_domain::Session,
) {
    let id = before.id.clone();
    let expected_calls = if mode == TwoFollowups::SecondAckFailure {
        2
    } else {
        3
    };
    // Observe actual runner finalization, including any pending-successor attempt.
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let rows: Value = client
                .get(format!("{base}/sessions"))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            let row = rows
                .as_array()
                .unwrap()
                .iter()
                .find(|r| r["id"] == id)
                .unwrap();
            if row["is_running"] == false {
                break;
            }
            assert!(host.0.try_wait().unwrap().is_none());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(probe.child_calls.load(Ordering::SeqCst), expected_calls);
    assert_eq!(probe.runs.lock().unwrap().len(), expected_calls);
    let finished = store.inspect_actor(&id).await.unwrap();
    assert_eq!(finished.actor.current_attempt, 1);
    let activation = finished.activation.unwrap();
    assert_eq!(activation.run_id, original.run_id);
    assert_eq!(activation.lease_owner, original.lease_owner);
    assert_eq!(activation.lease_epoch, original.lease_epoch);
    assert_eq!(activation.lease_expires_at, original.lease_expires_at);
    assert_eq!(
        activation.status,
        if mode == TwoFollowups::Complete {
            ActorActivationStatus::Succeeded
        } else {
            ActorActivationStatus::Failed
        },
        "actual status={:?} error={:?}",
        completed.last_run_status(),
        completed
            .last_run_error()
            .map(|e| e.chars().take(384).collect::<String>())
    );
    if mode == TwoFollowups::SecondAckFailure {
        assert!(completed
            .last_run_error()
            .unwrap()
            .contains("ACK unresolved"));
    }
    host.0.kill().unwrap();
    host.0.wait().unwrap();
    drop(host);
    drop(store);
    drop(probe.ack_fault.lock().unwrap().take());
    let cold_store = std::sync::Arc::new(SessionStoreV2::new(probe.data.clone()).await.unwrap());
    let cold = cold_store.load_session(&id).await.unwrap().unwrap();
    assert_eq!(cold.created_at, before.created_at);
    assert_eq!(cold.project_id_meta(), before.project_id_meta());
    assert_eq!(cold.root_session_id, before.root_session_id);
    assert_eq!(cold.parent_session_id, before.parent_session_id);
    let first = cold
        .messages
        .iter()
        .position(|m| m.content == "OWNED_REPLY_0")
        .unwrap();
    let expected_tail: Vec<_> = (0..expected_calls)
        .flat_map(|i| {
            [
                Some(format!("OWNED_REPLY_{i}")),
                (i < 2).then(|| format!("OWNED_ROOT_CORRECTION_{}", i + 1)),
            ]
        })
        .flatten()
        .collect();
    assert_eq!(
        cold.messages[first..]
            .iter()
            .map(|m| m.content.clone())
            .collect::<Vec<_>>(),
        expected_tail
    );
    assert_eq!(
        cold.messages
            .iter()
            .filter(|m| m
                .metadata
                .as_ref()
                .is_some_and(|v| v.get("_bamboo_owned_input_checkpoint").is_some()))
            .count(),
        2
    );
    let inbox = bamboo_storage::FileSessionInbox::new(
        cold_store.clone(),
        bamboo_domain::SessionInboxLimits::default(),
    );
    for (i, (envelope, _)) in probe
        .deliveries
        .lock()
        .unwrap()
        .clone()
        .into_iter()
        .enumerate()
    {
        assert_eq!(
            cold.messages
                .iter()
                .filter(|m| m.id == envelope.as_str())
                .count(),
            usize::from(i < 2)
        );
        assert_eq!(
            cold.session_inbox_admission().unwrap().contains(&envelope),
            i < 2
        );
        assert_eq!(
            bamboo_domain::SessionInboxPort::was_admitted(&inbox, &id, &envelope)
                .await
                .unwrap(),
            i < expected_calls - 1
        );
    }
    let backlog = bamboo_domain::SessionInboxPort::inspect(&inbox, &id)
        .await
        .unwrap();
    assert_eq!(
        (backlog.pending, backlog.claimed),
        match mode {
            TwoFollowups::Complete => (0, 0),
            TwoFollowups::Overflow => (1, 0),
            TwoFollowups::SecondAckFailure => (0, 1),
        }
    );
    assert_eq!(
        probe.child_calls.load(Ordering::SeqCst),
        expected_calls,
        "no hidden fourth provider after actual Host settled"
    );
}
#[actix_web::test]
async fn actual_owned_child_admits_two_corrections_and_preserves_overflow_and_ack_failure() {
    for mode in [
        TwoFollowups::Complete,
        TwoFollowups::Overflow,
        TwoFollowups::SecondAckFailure,
    ] {
        Box::pin(fixture_with_followups(
            true,
            false,
            false,
            false,
            false,
            false,
            Some(mode),
        ))
        .await;
    }
}
