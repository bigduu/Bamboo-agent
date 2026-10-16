// Real serve + public SubAgent create/followup/run. Only loopback provider replies
// are simulated. QueueOnly enters the public lifetime-fenced Inbox; no Actor
// claim, placement, lease, cursor or receipt is fabricated by the controller.
struct ColdBatchProbe {
    phase: AtomicUsize,
    child_calls: AtomicUsize,
    count: usize,
    fail_ack: bool,
    child_id: Mutex<Option<String>>,
    observed_runs: Mutex<Vec<bamboo_subagent::RunSpec>>,
    data: PathBuf,
    workspace: PathBuf,
}

async fn cold_batch_provider(
    body: web::Json<Value>,
    probe: web::Data<ColdBatchProbe>,
) -> HttpResponse {
    let body = body.into_inner();
    let (delta, finish) = if body["model"] == "cold-batch-child" {
        let index = probe.child_calls.fetch_add(1, Ordering::SeqCst);
        let store = std::sync::Arc::new(SessionStoreV2::new(probe.data.clone()).await.unwrap());
        let known_child = probe.child_id.lock().unwrap().clone();
        let expected_run = if let Some(id) = &known_child {
            Some(
                store
                    .inspect_actor(id)
                    .await
                    .unwrap()
                    .activation
                    .unwrap()
                    .run_id,
            )
        } else {
            None
        };
        let runs = claimed_required_runs(&probe.data);
        let run = runs
            .iter()
            .map(|(_, _, run)| run)
            .find(|run| {
                expected_run
                    .as_ref()
                    .is_none_or(|run_id| run.activation_run_id.as_ref() == Some(run_id))
                    && run.logical_session.as_ref().is_some_and(|identity| {
                        probe
                            .child_id
                            .lock()
                            .unwrap()
                            .as_ref()
                            .is_none_or(|id| id == &identity.session_id)
                    })
            })
            .expect("provider must have an actual owned physical Run")
            .clone();
        let identity = run.logical_session.as_ref().unwrap();
        *probe.child_id.lock().unwrap() = Some(identity.session_id.clone());
        let child = store
            .load_session(&identity.session_id)
            .await
            .unwrap()
            .unwrap();
        let actor = store.inspect_actor(&child.id).await.unwrap();
        let activation = actor.activation.as_ref().unwrap();
        assert_eq!(activation.status, ActorActivationStatus::Running);
        assert_eq!(
            run.activation_run_id.as_deref(),
            Some(activation.run_id.as_str())
        );
        assert_eq!(
            identity.creation.as_ref().unwrap().created_at,
            child.created_at
        );
        assert_eq!(child.parent_session_id.as_deref(), Some("plain-root"));
        assert!(body["tools"]
            .as_array()
            .is_none_or(|tools| tools.is_empty()));
        if index == 0 {
            assert_eq!(actor.actor.current_attempt, 1);
            assert!(run.initial_session_messages.is_empty());
        } else {
            assert_eq!(index, 1, "exactly one cold provider entry; no replay");
            assert!(
                !probe.fail_ack,
                "provider must not enter after an unconfirmed Host ACK"
            );
            assert_eq!(actor.actor.current_attempt, 2);
            assert_eq!(run.initial_session_messages.len(), probe.count);
            let inbox = bamboo_storage::FileSessionInbox::new(store.clone(), Default::default());
            let messages = body["messages"].as_array().unwrap();
            let mut previous_position = None;
            for delivery in &run.initial_session_messages {
                assert_eq!(delivery.activation_run_id, activation.run_id);
                assert!(
                    bamboo_domain::SessionInboxPort::was_admitted(
                        &inbox,
                        &child.id,
                        &delivery.envelope.id
                    )
                    .await
                    .unwrap(),
                    "every physical Host receipt must precede provider entry"
                );
                let expected = delivery.envelope.to_provider_message().unwrap();
                assert_eq!(
                    child
                        .messages
                        .iter()
                        .filter(|message| message.id == expected.id)
                        .count(),
                    1
                );
                assert!(child
                    .session_inbox_admission()
                    .unwrap()
                    .contains(&delivery.envelope.id));
                let positions = messages
                    .iter()
                    .enumerate()
                    .filter(|(_, message)| {
                        message["role"] == "user" && message["content"] == expected.content
                    })
                    .map(|(position, _)| position)
                    .collect::<Vec<_>>();
                assert_eq!(
                    positions.len(),
                    1,
                    "each actual input appears once in provider context"
                );
                assert!(previous_position.is_none_or(|previous| previous < positions[0]));
                previous_position = Some(positions[0]);
            }
        }
        probe.observed_runs.lock().unwrap().push(run);
        (
            json!({"content":if index == 0 {"COLD_INITIAL_REPLY"} else {"COLD_BATCH_REPLY"}}),
            "stop",
        )
    } else {
        assert_eq!(body["model"], "cold-batch-root");
        let phase = probe.phase.load(Ordering::SeqCst);
        let call_id = format!("cold-batch-phase-{phase}");
        let finished = body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| message["role"] == "tool" && message["tool_call_id"] == call_id);
        if finished {
            (json!({"content":"COLD_ROOT_DONE"}), "stop")
        } else {
            let args = match phase {
                0 => {
                    json!({"action":"create","title":"Cold batch","responsibility":"One plain reply","prompt":"Return one plain reply","subagent_type":"cold-plain-reply","workspace":probe.workspace,"auto_run":true})
                }
                1 => {
                    json!({"action":"send_message","child_session_id":probe.child_id.lock().unwrap().clone().unwrap(),"message":format!("COLD_BATCH_INPUT_{}",probe.count-1),"interrupt_running":false,"auto_run":true})
                }
                2 => {
                    json!({"action":"run","child_session_id":probe.child_id.lock().unwrap().clone().unwrap(),"reset_to_last_user":false})
                }
                _ => panic!("unexpected cold batch phase"),
            };
            let mut delta = call(args);
            delta["tool_calls"][0]["id"] = json!(call_id);
            (delta, "tool_calls")
        }
    };
    let event = json!({"id":"cold-batch","object":"chat.completion.chunk","choices":[{"index":0,"delta":delta,"finish_reason":finish}]});
    HttpResponse::Ok()
        .content_type("text/event-stream")
        .body(format!("data: {event}\n\ndata: [DONE]\n\n"))
}

async fn cold_batch_wait_settled(
    probe: &ColdBatchProbe,
    host: &mut Host,
    client: &reqwest::Client,
    base: &str,
    phase: usize,
) -> (bamboo_domain::Session, bamboo_domain::ActorDirectoryEntry) {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            assert!(
                host.0.try_wait().unwrap().is_none(),
                "fixture Host exited: {}",
                fixture_host_log_tail(&probe.data)
            );
            let store = SessionStoreV2::new(probe.data.clone()).await.unwrap();
            let parent = store.load_session("plain-root").await.unwrap().unwrap();
            let call_id = format!("cold-batch-phase-{phase}");
            let tool = parent.messages.iter().find(|message| {
                message.role == bamboo_domain::Role::Tool
                    && message.tool_call_id.as_deref() == Some(call_id.as_str())
            });
            let rows: Value = client
                .get(format!("{base}/sessions"))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            let parent_idle = rows["sessions"]
                .as_array()
                .unwrap()
                .iter()
                .any(|row| row["id"] == "plain-root" && row["is_running"] == false);
            if parent_idle && tool.is_some() {
                let id = if phase == 0 {
                    let receipt: Value = serde_json::from_str(&tool.unwrap().content).unwrap();
                    receipt["child_session_id"].as_str().map(str::to_owned)
                } else {
                    probe.child_id.lock().unwrap().clone()
                };
                if let Some(id) = id {
                    let child = store.load_session(&id).await.unwrap().unwrap();
                    let actor = store.inspect_actor(&id).await.unwrap();
                    // A baseline refusal is returned immediately as a real
                    // failed child run, rather than hidden by a fixed sleep.
                    let child_idle = rows["sessions"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|row| row["id"] == id && row["is_running"] == false);
                    let progressed = phase == 0
                        || actor.actor.current_attempt >= 2
                        || child.last_run_status().as_deref() == Some("error");
                    if child_idle
                        && progressed
                        && parent.last_run_status().as_deref() == Some("completed")
                        && matches!(
                            child.last_run_status().as_deref(),
                            Some("completed" | "error")
                        )
                        && actor
                            .activation
                            .as_ref()
                            .is_some_and(|activation| !activation.status.is_live())
                    {
                        return (child, actor);
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|error| {
        panic!(
            "cold phase {phase}: {error}; {}",
            fixture_host_log_tail(&probe.data)
        )
    })
}

#[actix_web::test]
async fn actual_completed_owned_child_cold_batch_requires_every_host_ack() {
    use bamboo_domain::{
        ActorSession, SessionInboxPort, SessionMessageDeliveryIntent, SessionMessageKind,
        SessionMessageSource,
    };
    for (count, fail_ack) in [(2, false), (4, false), (4, true)] {
        let listener_admission = FIXTURE_LISTENER_ADMISSION.lock().await;
        let temp = tempfile::tempdir().unwrap();
        let data = temp.path().canonicalize().unwrap().join("host");
        let workspace = temp.path().canonicalize().unwrap().join("workspace");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::create_dir_all(&workspace).unwrap();
        let projects = bamboo_projects::ProjectStore::open(&data).unwrap();
        let project = projects
            .create_with_project_path("cold batch", None, workspace.to_string_lossy(), vec![])
            .unwrap();
        let agents = projects.paths().project_home(&project.id).join("agents");
        std::fs::create_dir_all(&agents).unwrap();
        std::fs::write(agents.join("cold-plain-reply.md"), "---\nschema_version: 1\nname: cold-plain-reply\ndescription: One plain reply\nmodel_hint: openai:cold-batch-child\ntools:\n  deny: [Bash, Read, Glob, Edit, Write]\n---\nReturn one plain reply without tools.\n").unwrap();
        let probe = web::Data::new(ColdBatchProbe {
            phase: AtomicUsize::new(0),
            child_calls: AtomicUsize::new(0),
            count,
            fail_ack,
            child_id: Mutex::new(None),
            observed_runs: Mutex::new(Vec::new()),
            data: data.clone(),
            workspace: workspace.clone(),
        });
        let server_probe = probe.clone();
        let server = HttpServer::new(move || {
            App::new()
                .app_data(server_probe.clone())
                .route("/v1/chat/completions", web::post().to(cold_batch_provider))
                .route(
                    "/v1/models",
                    web::get().to(|| async {
                        HttpResponse::Ok().json(
                            json!({"data":[{"id":"cold-batch-root"},{"id":"cold-batch-child"}]}),
                        )
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
        std::fs::write(data.join("config.json"), serde_json::to_vec(&json!({
            "provider":"openai","features":{"provider_model_ref":true},
            "providers":{"openai":{"api_key":"fixture","base_url":provider_url,"model":"cold-batch-root","runtime_models":["cold-batch-root","cold-batch-child"]}},
            "defaults":{"chat":{"provider":"openai","model":"cold-batch-root"}},
            "subagents":{"runtime":"actor","executor":"bamboo_runtime","worker_bin":env!("CARGO_BIN_EXE_bamboo"),"worker_args":["subagent-worker"],"max_concurrent":1}
        })).unwrap()).unwrap();
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let mut host = start(&data, port);
        host.record_cleanup("completed_cold_batch", None);
        let client = fixture_http_client();
        let base = format!("http://127.0.0.1:{port}/api/v1");
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                assert!(
                    host.0.try_wait().unwrap().is_none(),
                    "{}",
                    fixture_host_log_tail(&data)
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
        drop(listener_admission);
        let response = client.post(format!("{base}/chat")).json(&json!({
            "session_id":"plain-root","message":"Delegate one plain reply","model":"cold-batch-root","provider":"openai","model_ref":{"provider":"openai","model":"cold-batch-root"},
            "thinking_mode":"ultra","permission_mode":"bypass","workspace_path":workspace,"project_id":project.id
        })).send().await.unwrap();
        assert!(
            response.status().is_success(),
            "{}",
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
        let (before, old_actor) =
            cold_batch_wait_settled(&probe, &mut host, &client, &base, 0).await;
        *probe.child_id.lock().unwrap() = Some(before.id.clone());
        assert_eq!(old_actor.actor.current_attempt, 1);
        assert_eq!(
            old_actor.activation.as_ref().unwrap().status,
            ActorActivationStatus::Succeeded
        );
        assert_eq!(probe.child_calls.load(Ordering::SeqCst), 1);
        eprintln!(
            "cold-batch-stage {}",
            json!({"stage":"initial_completed","count":count,"ack_fault":fail_ack,"attempt":old_actor.actor.current_attempt,"provider_calls":probe.child_calls.load(Ordering::SeqCst)})
        );
        let store = std::sync::Arc::new(SessionStoreV2::new(data.clone()).await.unwrap());
        let parent = store.load_session("plain-root").await.unwrap().unwrap();
        let source = ActorSession::from_session(&parent).unwrap();
        let target = ActorSession::from_session(&before).unwrap();
        let inbox = bamboo_storage::FileSessionInbox::new(store.clone(), Default::default());
        let mut queued = Vec::new();
        for slot in 0..count - 1 {
            let mut envelope = bamboo_domain::SessionMessageEnvelope::user_input(
                &before.id,
                format!("COLD_BATCH_INPUT_{slot}"),
            );
            envelope.source = SessionMessageSource::Session {
                session_id: parent.id.clone(),
            };
            envelope.kind = SessionMessageKind::PeerMessage;
            envelope.thread_id = Some(format!("native-cold-thread-{slot}"));
            envelope.correlation_id = Some(format!("native-cold-operation-{slot}"));
            inbox
                .deliver_peer_message_with_intent(
                    &envelope,
                    &source,
                    &target,
                    None,
                    SessionMessageDeliveryIntent::QueueOnly,
                )
                .await
                .unwrap();
            queued.push(envelope);
        }
        assert!(!inbox
            .inspect(&before.id)
            .await
            .unwrap()
            .activation_pending());
        assert_eq!(
            store
                .inspect_actor(&before.id)
                .await
                .unwrap()
                .actor
                .current_attempt,
            1
        );
        assert_eq!(probe.child_calls.load(Ordering::SeqCst), 1);
        let admitted = data
            .join(store.resolve_rel_path(&before.id).await.unwrap())
            .join("inbox/admitted");
        let fault = fail_ack.then(|| AckWriteFault::new(admitted));
        probe.phase.store(1, Ordering::SeqCst);
        assert!(client.post(format!("{base}/chat")).json(&json!({"session_id":"plain-root","message":"Send the prepared followup now","provider":"openai","model":"cold-batch-root"})).send().await.unwrap().status().is_success());
        let (after, actor) = cold_batch_wait_settled(&probe, &mut host, &client, &base, 1).await;
        eprintln!(
            "cold-batch-stage {}",
            json!({"stage":"cold_settled","count":count,"ack_fault":fail_ack,"attempt":actor.actor.current_attempt,"activation_status":format!("{:?}",actor.activation.as_ref().unwrap().status),"child_status":after.last_run_status(),"provider_calls":probe.child_calls.load(Ordering::SeqCst)})
        );
        assert_eq!(
            actor.actor.current_attempt, 2,
            "cold batch must claim exactly one new Actor attempt"
        );
        assert_eq!(after.created_at, before.created_at);
        assert_eq!(after.parent_session_id, before.parent_session_id);
        for envelope in &queued {
            assert_eq!(
                after
                    .messages
                    .iter()
                    .filter(|message| message.id == envelope.id.as_str())
                    .count(),
                1
            );
            assert_eq!(
                inbox.was_admitted(&after.id, &envelope.id).await.unwrap(),
                !fail_ack
            );
        }
        if fail_ack {
            assert_eq!(
                actor.activation.as_ref().unwrap().status,
                ActorActivationStatus::Failed
            );
            assert_eq!(probe.child_calls.load(Ordering::SeqCst), 1);
            assert_eq!(inbox.inspect(&after.id).await.unwrap().claimed, count);
            assert!(!after
                .messages
                .iter()
                .any(|message| message.content == "COLD_BATCH_REPLY"));
            let prefix = serde_json::to_value(&after.messages).unwrap();
            drop(fault);
            probe.phase.store(2, Ordering::SeqCst);
            assert!(client.post(format!("{base}/chat")).json(&json!({"session_id":"plain-root","message":"Try explicit run without reset","provider":"openai","model":"cold-batch-root"})).send().await.unwrap().status().is_success());
            let (retry, unchanged) =
                cold_batch_wait_settled(&probe, &mut host, &client, &base, 2).await;
            assert_eq!(
                unchanged.actor.current_attempt, actor.actor.current_attempt,
                "multi-claim recovery must not be invented"
            );
            assert_eq!(serde_json::to_value(&retry.messages).unwrap(), prefix);
            assert_eq!(probe.child_calls.load(Ordering::SeqCst), 1);
            assert_eq!(inbox.inspect(&retry.id).await.unwrap().claimed, count);
        } else {
            assert_eq!(
                actor.activation.as_ref().unwrap().status,
                ActorActivationStatus::Succeeded
            );
            assert_eq!(probe.child_calls.load(Ordering::SeqCst), 2);
            assert_eq!(
                probe
                    .observed_runs
                    .lock()
                    .unwrap()
                    .last()
                    .unwrap()
                    .initial_session_messages
                    .len(),
                count
            );
            assert_eq!(
                after
                    .messages
                    .iter()
                    .filter(|message| message.content == "COLD_BATCH_REPLY")
                    .count(),
                1
            );
            let remaining = inbox.inspect(&after.id).await.unwrap();
            assert_eq!((remaining.pending, remaining.claimed), (0, 0));
        }
        let (killed, waited) = host.kill_and_wait("fixture_complete");
        assert!(killed.is_ok() && waited.is_ok());
        handle.stop(true).await;
    }
}
