//! Ordinary Root MVP: four real Worker processes, two assignments each, durable results.
//! Extracted from the cumulative #791 acceptance fixture; provider responses are controlled.
#![cfg(unix)]
use actix_web::{web, App, HttpResponse, HttpServer};
use bamboo_agent_core::storage::Storage;
use bamboo_domain::ActorDirectoryPort;
use bamboo_storage::SessionStoreV2;
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    time::Duration,
};

fn bounded_diagnostic(text: &str, cap: usize) -> &str {
    let mut end = text.len().min(cap);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
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

fn claimed_required_runs_up_to(
    data: &Path,
    limit: usize,
) -> Vec<(PathBuf, Vec<u8>, bamboo_subagent::RunSpec)> {
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
            let file = match std::fs::File::open(&path) {
                Ok(file) => file,
                // A completed Run can be ACKed between directory enumeration
                // and open; that no longer represents a claimed mailbox entry.
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => panic!("cannot open claimed Run {}: {error}", path.display()),
            };
            file.take(256 * 1024 + 1).read_to_end(&mut bytes).unwrap();
            assert!(bytes.len() <= 256 * 1024);
            let message: bamboo_subagent::InboxMessage = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(message.kind, bamboo_subagent::InboxKind::Run);
            runs.push((path, bytes, serde_json::from_value(message.body).unwrap()));
        }
    }
    assert!(runs.len() <= limit);
    runs
}

struct FourChildProbe {
    round: AtomicUsize,
    emitted: AtomicUsize,
    ready: AtomicUsize,
    ids: Mutex<Vec<String>>,
    release: tokio::sync::watch::Sender<usize>,
}
fn four_child_report(round: usize, slot: usize) -> Value {
    json!({"version":1,"outcome":"completed","summary":format!("MVP_RESULT_{round}_{slot}"),
        "reported_evidence":[],"reported_verification":[],"proposals":[],"blockers":[],"open_decisions":[]})
}
fn four_child_projection(content: &str) -> Option<Value> {
    let (_, rest) = content.split_once("Child typed result:\n")?;
    serde_json::from_str(rest.lines().next()?).ok()
}
async fn four_child_provider(
    body: web::Json<Value>,
    probe: web::Data<FourChildProbe>,
) -> HttpResponse {
    let round = probe.round.load(Ordering::SeqCst);
    let (delta, finish) = if body["model"] == "plain-child" {
        let assignment = body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .rev()
            .filter(|message| message["role"] == "user")
            .filter_map(|message| message["content"].as_str())
            .find(|content| content.contains(&format!("MVP_ASSIGN_{round}_")))
            .unwrap();
        let brief = assignment.split_once("<task-brief>\n").map(|(_, rest)| {
            serde_json::from_str::<Value>(rest.split_once("\n</task-brief>").unwrap().0).unwrap()
        });
        let task = brief
            .as_ref()
            .and_then(|value| value["task_brief"].as_str())
            .unwrap_or(assignment);
        let slot = (0..4)
            .find(|slot| task.contains(&format!("MVP_ASSIGN_{round}_{slot}")))
            .expect("current assignment identifies the actual Worker");
        assert_eq!(
            probe.ready.fetch_or(1 << slot, Ordering::SeqCst) & (1 << slot),
            0,
            "no duplicate Worker provider admission"
        );
        let mut release = probe.release.subscribe();
        tokio::time::timeout(Duration::from_secs(90), async {
            while *release.borrow_and_update() < round {
                release.changed().await.unwrap();
            }
        })
        .await
        .expect("four actual Workers are released together");
        (
            json!({"content":four_child_report(round, slot).to_string()}),
            "stop",
        )
    } else if body["model"] == "plain-root" && body["tools"].to_string().contains("SubAgent") {
        if probe.emitted.swap(round, Ordering::SeqCst) != round {
            let ids = probe.ids.lock().unwrap();
            let calls: Vec<_> = (0..4).map(|slot| {
                let mut args = json!({"message":format!("For MVP_ASSIGN_{round}_{slot}, return this exact v1 child report; use no tools: {}", four_child_report(round, slot))});
                if round == 2 { args["target"] = json!(ids[slot]); }
                json!({"index":slot,"id":format!("mvp-{round}-{slot}"),"type":"function",
                    "function":{"name":"SubAgent","arguments":args.to_string()}})
            }).collect();
            (json!({"tool_calls":calls}), "tool_calls")
        } else {
            let complete = (0..4).all(|slot| {
                body["messages"].as_array().unwrap().iter().any(|message| {
                    message["content"]
                        .as_str()
                        .and_then(four_child_projection)
                        .is_some_and(|projection| {
                            projection["available"] == true
                                && projection["child_report"] == four_child_report(round, slot)
                        })
                })
            });
            (
                json!({"content":if complete { format!("MVP_PARENT_COLLECTED_{round}") } else { format!("MVP_PARENT_WAITING_{round}") }}),
                "stop",
            )
        }
    } else {
        (json!({"content":"auxiliary"}), "stop")
    };
    let event = json!({"id":"four-child-mvp","object":"chat.completion.chunk",
        "choices":[{"index":0,"delta":delta,"finish_reason":finish}]});
    HttpResponse::Ok()
        .content_type("text/event-stream")
        .body(format!("data: {event}\n\ndata: [DONE]\n\n"))
}

// Only the owned Host's descendants enter numeric evidence; never retain argv.
fn four_child_process_sample(host: u32) -> Option<(usize, f64, u64)> {
    let output = Command::new("ps")
        .args(["-axo", "pid=,ppid=,%cpu=,rss=,comm="])
        .output()
        .unwrap();
    assert!(output.status.success());
    let rows: Vec<_> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            Some((
                fields.next()?.parse::<u32>().ok()?,
                fields.next()?.parse::<u32>().ok()?,
                fields.next()?.parse::<f64>().ok()?,
                fields.next()?.parse::<u64>().ok()?,
                fields.next()?.to_owned(),
            ))
        })
        .collect();
    if !rows.iter().any(|row| row.0 == host) {
        return None;
    }
    let mut owned = std::collections::BTreeSet::from([host]);
    loop {
        let before = owned.len();
        for row in &rows {
            if owned.contains(&row.1) {
                owned.insert(row.0);
            }
        }
        if owned.len() == before {
            break;
        }
    }
    let selected: Vec<_> = rows.iter().filter(|row| owned.contains(&row.0)).collect();
    let workers = selected
        .iter()
        .filter(|row| {
            row.0 != host
                && Path::new(&row.4)
                    .file_name()
                    .is_some_and(|name| name == "bamboo")
        })
        .count();
    Some((
        workers,
        selected.iter().map(|row| row.2).sum(),
        selected.iter().map(|row| row.3).sum(),
    ))
}
fn four_child_outcomes(parent: &bamboo_domain::Session) -> Vec<bamboo_domain::SessionChildOutcome> {
    parent
        .messages
        .iter()
        .filter_map(|message| {
            let envelope: bamboo_domain::SessionMessageEnvelope =
                serde_json::from_value(message.metadata.as_ref()?.get("session_message")?.clone())
                    .ok()?;
            match &envelope.body {
                bamboo_domain::SessionMessageBody::ChildOutcome(outcome) => {
                    assert!(serde_json::to_vec(&envelope).unwrap().len() <= 8192);
                    assert!(
                        serde_json::to_vec(&envelope.to_provider_message().unwrap())
                            .unwrap()
                            .len()
                            <= 8192
                    );
                    Some(outcome.clone())
                }
                _ => None,
            }
        })
        .collect()
}

#[actix_web::test]
async fn actual_four_children_two_rounds_collect_parent_results() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let data = root.join("host");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&data).unwrap();
    std::fs::create_dir_all(&workspace).unwrap();
    let projects = bamboo_projects::ProjectStore::open(&data).unwrap();
    let project = projects
        .create_with_project_path("four-child-mvp", None, workspace.to_string_lossy(), vec![])
        .unwrap();
    let agents = projects.paths().project_home(&project.id).join("agents");
    std::fs::create_dir_all(&agents).unwrap();
    std::fs::write(agents.join("worker.md"), "---\nschema_version: 1\nname: worker\ndescription: One bounded typed report\nmodel_hint: openai:plain-child\ntools:\n  deny: [Bash, Read, Glob, Edit, Write]\n---\nReturn the assigned v1 child report exactly; use no tools.\n").unwrap();
    let (release, _) = tokio::sync::watch::channel(0);
    let probe = web::Data::new(FourChildProbe {
        round: AtomicUsize::new(1),
        emitted: AtomicUsize::new(0),
        ready: AtomicUsize::new(0),
        ids: Mutex::new(vec![]),
        release,
    });
    let provider_probe = probe.clone();
    let server = HttpServer::new(move || {
        App::new()
            .app_data(provider_probe.clone())
            .route("/v1/chat/completions", web::post().to(four_child_provider))
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
    std::fs::write(data.join("config.json"), serde_json::to_vec(&json!({"provider":"openai","features":{"provider_model_ref":true},
        "providers":{"openai":{"api_key":"fixture","base_url":provider_url,"model":"plain-root"}},
        "defaults":{"chat":{"provider":"openai","model":"plain-root"}},"subagents":{"runtime":"actor","executor":"bamboo_runtime","max_concurrent":4}})).unwrap()).unwrap();
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
            assert!(host.0.try_wait().unwrap().is_none(), "actual Host exited");
            if client
                .get(format!("{base}/health"))
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let samples = std::sync::Arc::new(Mutex::new(Vec::new()));
    let sample_output = samples.clone();
    let pid = host.0.id();
    let sampler = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        for second in 0..256 {
            interval.tick().await;
            let Some((workers, cpu, rss_kib)) = four_child_process_sample(pid) else {
                break;
            };
            sample_output
                .lock()
                .unwrap()
                .push(json!([second, workers, cpu, rss_kib]));
        }
    });
    let mut round_ms = Vec::new();
    for round in 1..=2 {
        probe.round.store(round, Ordering::SeqCst);
        probe.ready.store(0, Ordering::SeqCst);
        let started = std::time::Instant::now();
        let mut chat = json!({"session_id":"four-child-root",
            "message":format!("Delegate four independent assignments for round {round}, then collect all four exact results."),
            "model":"plain-root","provider":"openai","model_ref":{"provider":"openai","model":"plain-root"},
            "workspace_path":workspace,"project_id":project.id});
        if round == 1 {
            chat["thinking_mode"] = json!("ultra");
            chat["permission_mode"] = json!("bypass");
        }
        let response = client
            .post(format!("{base}/chat"))
            .json(&chat)
            .send()
            .await
            .unwrap();
        assert!(
            response.status().is_success(),
            "chat: {}",
            response.text().await.unwrap()
        );
        let store = SessionStoreV2::new(data.clone()).await.unwrap();
        if round == 1 {
            let dispatch: Value = client
                .post(format!("{base}/execute/four-child-root"))
                .json(&json!({}))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(dispatch["status"], "started");
        }
        // Later chat inputs activate the existing Root. The held four-child
        // wait and all canonical outcomes below prove that actual round.
        let held_parent = tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                assert!(host.0.try_wait().unwrap().is_none(), "actual Host exited");
                if let Some(parent) = store.load_session("four-child-root").await.unwrap() {
                    for result in parent.messages.iter().filter(|message| {
                        message.role == bamboo_domain::Role::Tool
                            && message
                                .tool_call_id
                                .as_deref()
                                .is_some_and(|id| id.starts_with(&format!("mvp-{round}-")))
                    }) {
                        assert_ne!(result.tool_success, Some(false), "{}", result.content);
                    }
                    if probe.ready.load(Ordering::SeqCst) == 15
                        && parent.last_run_status().as_deref() == Some("suspended")
                    {
                        break parent;
                    }
                    assert_ne!(
                        parent.last_run_status().as_deref(),
                        Some("error"),
                        "Root: {:?}",
                        parent.last_run_error()
                    );
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        if held_parent.is_err() {
            let current = std::sync::Arc::new(SessionStoreV2::new(data.clone()).await.unwrap());
            let inbox = bamboo_storage::FileSessionInbox::new(
                current.clone(),
                bamboo_domain::SessionInboxLimits::default(),
            );
            let parent = current
                .load_session("four-child-root")
                .await
                .unwrap()
                .unwrap();
            let results: Vec<_> = parent.messages.iter().filter(|message| {
                message.role == bamboo_domain::Role::Tool
                    && message.tool_call_id.as_deref().is_some_and(|id| id.starts_with(&format!("mvp-{round}-")))
            }).map(|message| json!({"call":message.tool_call_id,"success":message.tool_success,"result":message.content})).collect();
            let mut children = Vec::new();
            let observed_ids = probe.ids.lock().unwrap().clone();
            for id in observed_ids {
                let child = current.load_session(&id).await.unwrap().unwrap();
                let actor = current
                    .inspect_actor(&id)
                    .await
                    .map(|entry| {
                        json!({"state":entry.actor.state,"attempt":entry.actor.current_attempt,
                        "activation":entry.activation.map(|activation| json!({"status":activation.status,
                        "checkpoint_revision":activation.checkpoint_revision,"inbox_generation":activation.inbox_generation}))})
                    })
                    .unwrap_or_else(|error| json!({"inspection_error":error.to_string()}));
                let backlog = bamboo_domain::SessionInboxPort::inspect(&inbox, &id)
                    .await
                    .unwrap();
                children.push(json!({"actor_id":id,"run_status":child.last_run_status(),"run_error":child.last_run_error(),
                    "actor":actor,"inbox":{"pending":backlog.pending,"claimed":backlog.claimed,"generation":backlog.generation},
                    "current_task_in_history":child.messages.iter().any(|message| message.content.contains(&format!("MVP_ASSIGN_{round}_")))}));
            }
            let marker = format!("MVP_ASSIGN_{round}_");
            let runs: Vec<_> = claimed_required_runs_up_to(&data, 4).into_iter().map(|(_, _, run)| {
                json!({"actor_id":run.logical_session.as_ref().map(|logical| &logical.session_id),
                    "run_id":run.activation_run_id,"prefix_messages":run.messages.len(),
                    "initial_deliveries":run.initial_session_messages.len(),
                    "current_task_in_prefix":run.messages.iter().any(|message| message.to_string().contains(&marker)),
                    "current_task_in_delivery":run.initial_session_messages.iter().any(|delivery| serde_json::to_value(delivery).unwrap().to_string().contains(&marker))})
            }).collect();
            let mut worker_outcomes = Vec::new();
            for mailbox in std::fs::read_dir(data.join("broker/mailboxes"))
                .unwrap()
                .take(17)
            {
                let mailbox = mailbox.unwrap();
                let Ok(entries) = std::fs::read_dir(mailbox.path().join("cur")) else {
                    continue;
                };
                for entry in entries.take(5) {
                    let Ok(bytes) = std::fs::read(entry.unwrap().path()) else {
                        continue;
                    };
                    if bytes.len() > 64 * 1024 {
                        continue;
                    }
                    let Ok(message) =
                        serde_json::from_slice::<bamboo_subagent::InboxMessage>(&bytes)
                    else {
                        continue;
                    };
                    if message.kind == bamboo_subagent::InboxKind::Outcome {
                        worker_outcomes.push(json!({"status":message.body["status"],
                            "error":message.body["error"].as_str().map(|error| bounded_diagnostic(error, 512))}));
                    }
                }
            }
            panic!(
                "four-child held timeout: {}",
                json!({"round":round,"ready_mask":probe.ready.load(Ordering::SeqCst),
                "parent_status":parent.last_run_status(),"results":results,"children":children,"runs":runs,"worker_outcomes":worker_outcomes,
                "checkpoint_errors":std::fs::read_to_string(data.join("host.log")).unwrap_or_default()
                    .lines().filter(|line| line.contains("Actor correction checkpoint rejected or unconfirmed"))
                    .take(4).map(|line| bounded_diagnostic(line, 1024).to_owned()).collect::<Vec<_>>()})
            );
        }
        let held_parent = held_parent.unwrap();
        let mut ids = Vec::new();
        for slot in 0..4 {
            let call_id = format!("mvp-{round}-{slot}");
            let results: Vec<_> = held_parent
                .messages
                .iter()
                .filter(|m| {
                    m.role == bamboo_domain::Role::Tool
                        && m.tool_call_id.as_deref() == Some(call_id.as_str())
                })
                .collect();
            assert_eq!(results.len(), 1);
            assert_eq!(
                results[0].tool_success,
                Some(true),
                "{}",
                results[0].content
            );
            let result: Value = serde_json::from_str(&results[0].content).unwrap();
            ids.push(result["actor_id"].as_str().unwrap().to_owned());
        }
        assert_eq!(
            ids.iter().collect::<std::collections::BTreeSet<_>>().len(),
            4
        );
        if round == 1 {
            *probe.ids.lock().unwrap() = ids.clone();
        } else {
            assert_eq!(ids, *probe.ids.lock().unwrap());
        }
        // The external observer's index predates first-round Child creation.
        let store = SessionStoreV2::new(data.clone()).await.unwrap();
        let runs = claimed_required_runs_up_to(&data, 4);
        assert_eq!(runs.len(), 4, "four concurrent real broker Runs");
        for id in &ids {
            assert!(runs.iter().any(|(_, _, run)| {
                run.logical_session
                    .as_ref()
                    .is_some_and(|logical| logical.session_id == *id)
            }));
        }
        tokio::time::sleep(Duration::from_secs(2)).await; // Two one-second samples at the held concurrency boundary.
        assert!(
            four_child_process_sample(pid).unwrap().0 >= 4,
            "four actual Worker processes"
        );
        probe.release.send_replace(round);
        let completed = tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                assert!(host.0.try_wait().unwrap().is_none(), "actual Host exited");
                let parent = store
                    .load_session("four-child-root")
                    .await
                    .unwrap()
                    .unwrap();
                let outcomes = four_child_outcomes(&parent);
                if parent.last_run_status().as_deref() == Some("completed")
                    && outcomes.len() == round * 4
                    && parent
                        .messages
                        .iter()
                        .any(|m| m.content == format!("MVP_PARENT_COLLECTED_{round}"))
                {
                    break parent;
                }
                assert_ne!(
                    parent.last_run_status().as_deref(),
                    Some("error"),
                    "Root: {:?}",
                    parent.last_run_error()
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        if completed.is_err() {
            let parent = store
                .load_session("four-child-root")
                .await
                .unwrap()
                .unwrap();
            let mut children = Vec::new();
            for id in &ids {
                let child = store.load_session(id).await.unwrap().unwrap();
                children.push(json!({"id":id,"status":child.last_run_status(),"error":child.last_run_error(),
                    "tail":child.messages.iter().rev().take(2).map(|m| json!({"role":m.role,"content":bounded_diagnostic(&m.content,512)})).collect::<Vec<_>>()}));
            }
            panic!(
                "collection timeout: {}",
                json!({"round":round,"parent_status":parent.last_run_status(),
                "parent_error":parent.last_run_error(),"outcomes":four_child_outcomes(&parent),"children":children,
                "host_log":std::fs::read_to_string(data.join("host.log")).unwrap_or_default().chars().rev().take(6000).collect::<String>().chars().rev().collect::<String>()})
            );
        }
        let completed = completed.unwrap();
        for (slot, id) in ids.iter().enumerate() {
            let child = store.load_session(id).await.unwrap().unwrap();
            assert_eq!(child.last_run_status().as_deref(), Some("completed"));
            for prior in 1..=round {
                let report = four_child_report(prior, slot);
                assert_eq!(
                    child
                        .messages
                        .iter()
                        .filter(|m| m.role == bamboo_domain::Role::Assistant
                            && serde_json::from_str::<Value>(&m.content)
                                .is_ok_and(|value| value == report))
                        .count(),
                    1
                );
                let matching: Vec<_> = four_child_outcomes(&completed)
                    .into_iter()
                    .filter(|outcome| {
                        outcome.child_session_id == *id
                            && outcome
                                .provider_message
                                .as_ref()
                                .and_then(|message| four_child_projection(&message.content.text))
                                .is_some_and(|projection| {
                                    projection["available"] == true
                                        && projection["child_report"] == report
                                })
                    })
                    .collect();
                assert_eq!(matching.len(), 1);
                assert_eq!(matching[0].status, "completed");
                assert!(matching[0].result.is_none());
                assert!(matching[0].error.is_none());
                let projection = four_child_projection(
                    &matching[0].provider_message.as_ref().unwrap().content.text,
                )
                .unwrap();
                assert_eq!(
                    projection["host_observation"]["kind"],
                    "committed_terminal_source"
                );
                assert_eq!(
                    projection["host_observation"]["terminal_source"]["child_session_id"],
                    *id
                );
            }
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                assert!(host.0.try_wait().unwrap().is_none(), "actual Host exited");
                if four_child_process_sample(pid).unwrap().0 == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("completed Children release all four actual Worker processes");
        round_ms.push(started.elapsed().as_millis());
        eprintln!(
            "four-child round evidence: {}",
            json!({"round":round,"outcomes":round*4,"round_ms":round_ms.last(),
                "samples":*samples.lock().unwrap()})
        );
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    sampler.abort();
    let _ = sampler.await;
    host.0.kill().unwrap();
    host.0.wait().unwrap();
    eprintln!(
        "four-child MVP evidence: {}",
        json!({"children":4,"rounds":2,"round_ms":round_ms,"outcomes":8,
        "sample_fields":["elapsed_s","worker_count","host_tree_cpu_percent","host_tree_rss_kib"],"samples":*samples.lock().unwrap()})
    );
    handle.stop(true).await;
}
