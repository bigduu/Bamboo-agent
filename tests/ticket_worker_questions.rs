//! Real five Native Workers; controlled routing evidence, not model semantics.
#![cfg(unix)]
#[path = "support/ticket_runtime.rs"]
mod fixture;
use fixture::{command, get, post, Fixture};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::atomic::Ordering, time::Duration};

async fn views(f: &Fixture, ids: &[String]) -> Value {
    post(
        &f.client,
        &f.base,
        "/tickets/inspect",
        &json!({"ids":ids,"depth":0,"budget_bytes":65536}),
    )
    .await
}
async fn question_observation(store: &bamboo_storage::SessionStoreV2, row: &Value) -> Value {
    use bamboo_agent_core::storage::Storage;
    // Inspect returns assignments in ID order; a resumed Worker may not be [0].
    let assignment = row["assignments"]
        .as_array()
        .and_then(|assignments| {
            assignments
                .iter()
                .max_by_key(|assignment| assignment["generation"].as_u64().unwrap_or(0))
        })
        .unwrap_or(&Value::Null);
    let mut observation = json!({
        "work_id": row["ticket"]["id"],
        "title": row["ticket"]["contract"]["title"],
        "state": row["ticket"]["state"],
        "active_assignment": row["ticket"]["active_assignment"],
        "requests": row["requests"],
        "request_count": row["requests"].as_array().map(Vec::len),
        "assignments": row["assignments"],
        "assignment_id": assignment["id"],
        "process_stopped": assignment["process_stopped"] == true,
        "completed_question": false,
    });
    let Some(id) = assignment["runtime"]["session_id"].as_str() else {
        observation["child"] = json!({"load_error":"missing runtime session_id"});
        return observation;
    };
    let child = match store.load_session(id).await {
        Ok(Some(child)) => child,
        Ok(None) => {
            observation["child"] = json!({"session_id":id,"load_error":"session missing"});
            return observation;
        }
        Err(error) => {
            observation["child"] = json!({"session_id":id,"load_error":error.to_string()});
            return observation;
        }
    };
    let has_yield = child
        .metadata
        .contains_key("ticket.worker.question_yield.v1");
    let terminal_tool = child
        .messages
        .last()
        .is_some_and(|last| serde_json::to_value(&last.role).ok().as_ref() == Some(&json!("tool")));
    observation["completed_question"] = json!(
        child.last_run_status().as_deref() == Some("completed") && has_yield && terminal_tool
    );
    observation["child"] = json!({
        "session_id": id,
        "last_run_status": child.last_run_status(),
        "last_run_error": child.last_run_error(),
        "question_yield": child.metadata.get("ticket.worker.question_yield.v1"),
        "terminal_tool": terminal_tool,
        "message_count": child.messages.len(),
        "last_message": child.messages.last(),
        "revision_conflicts": child.messages.iter().filter(|message| {
            message.role == bamboo_domain::Role::Tool
                && message.content == "authority_unavailable: revision_conflict"
        }).count(),
    });
    observation
}

async fn completed_question(store: &bamboo_storage::SessionStoreV2, row: &Value) -> bool {
    question_observation(store, row).await["completed_question"] == true
}

async fn await_views(
    f: &Fixture,
    ids: &[String],
    ready: impl Fn(&Value, &[bool]) -> bool,
) -> Value {
    let store = bamboo_storage::SessionStoreV2::new(f.data.clone())
        .await
        .unwrap();
    let mut last = json!({"requested_ids":ids,"workers":[],"inspection":"not completed"});
    let outcome = tokio::time::timeout(Duration::from_secs(40), async {
        loop {
            let value = views(f, ids).await;
            let mut completed = Vec::new();
            let mut workers = Vec::new();
            let rows = value["data"].as_array().unwrap();
            for row in rows {
                let observation = question_observation(&store, row).await;
                completed.push(observation["completed_question"] == true);
                workers.push(observation);
            }
            last = json!({"requested_ids":ids,"workers":workers});
            // An empty/partial inspection must never pass a vacuous all().
            if rows.len() == ids.len() && ready(&value, &completed) {
                break value;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    outcome.unwrap_or_else(|_| {
        last["calls"] = json!(f.probe.calls.load(Ordering::SeqCst));
        last["questions"] = json!(f.probe.questions.load(Ordering::SeqCst));
        last["question_retries"] = json!(f.probe.question_retries.load(Ordering::SeqCst));
        let diagnostic = serde_json::to_string_pretty(&last).unwrap();
        let diagnostic_path = f.data.join("question-readiness.json");
        let saved = std::fs::write(&diagnostic_path, &diagnostic);
        panic!(
            "five questions deadline; {diagnostic}\nretained synthetic evidence: {} (write: {saved:?}); Host log: {}",
            diagnostic_path.display(),
            f.data.join("host.log").display()
        )
    })
}

#[actix_web::test]
async fn five_native_questions_release_workers_survive_restart_and_answer_e_b_d_a_c() {
    let mut f = Fixture::new().await;
    let mut operations = Vec::new();
    for letter in ["A", "B", "C", "D", "E"] {
        operations.push(json!({"op":"create","temp_id":letter,"kind":"work","parent":null,"depends_on":[],"contract":{
            "title":letter,"objective":format!("TICKET_QUESTION_E2E:{letter}"),"constraints":["Own private plan and answer only"],"acceptance":["Exact answer"],"user_acceptance_required":true,"allowed_tools":["Task"]}}));
        operations.push(json!({"op":"ready","work_id":letter}));
    }
    let created = post(
        &f.client,
        &f.base,
        "/tickets/update",
        &command(&f.client, &f.base, "create-five", json!(operations)).await,
    )
    .await;
    let works: BTreeMap<String, String> = ["A", "B", "C", "D", "E"]
        .into_iter()
        .map(|l| (l.into(), created["ids"][l].as_str().unwrap().into()))
        .collect();
    let ids: Vec<_> = works.values().cloned().collect();
    let starts:Vec<_> = works.iter().map(|(letter,id)|json!({"op":"start","work_id":id,"temp_id":format!("assignment-{letter}"),"workspace":null})).collect();
    let dispatched = post(
        &f.client,
        &f.base,
        "/tickets/dispatch",
        &command(&f.client, &f.base, "start-five", json!(starts)).await,
    )
    .await;
    assert_eq!(dispatched["errors"], json!([]), "{dispatched}");
    let pending = await_views(&f, &ids, |v, completed| {
        v["data"]
            .as_array()
            .unwrap()
            .iter()
            .zip(completed)
            .all(|(row, completed)| {
                row["requests"].as_array().unwrap().len() == 1
                    && row["assignments"][0]["process_stopped"] == true
                    && *completed
            })
    })
    .await;
    let question_retries = f.probe.question_retries.load(Ordering::SeqCst);
    assert_eq!(
        f.probe.questions.load(Ordering::SeqCst),
        5 + question_retries
    );
    assert_eq!(
        f.probe.calls.load(Ordering::SeqCst),
        5 + question_retries,
        "only observed revision conflicts may add a model round before question yield"
    );
    let store = bamboo_storage::SessionStoreV2::new(f.data.clone())
        .await
        .unwrap();
    let mut persisted_conflicts = 0;
    for row in pending["data"].as_array().unwrap() {
        let observed = question_observation(&store, row).await;
        persisted_conflicts += observed["child"]["revision_conflicts"].as_u64().unwrap() as usize;
        let letter = row["ticket"]["contract"]["title"].as_str().unwrap();
        assert_eq!(row["requests"][0]["work_id"], works[letter]);
        assert_eq!(
            row["requests"][0]["assignment_id"],
            row["assignments"][0]["id"]
        );
        assert_eq!(
            row["assignments"][0]["awaiting_request"],
            row["requests"][0]["id"]
        );
        assert_eq!(row["ticket"]["state"], "blocked");
        assert!(row["ticket"]["active_assignment"].is_null());
        assert!(
            row["submissions"].as_array().unwrap().is_empty(),
            "question is not a delivery"
        );
        assert_eq!(
            row["assignments"][0]["plan"]["steps"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }
    assert_eq!(
        persisted_conflicts, question_retries,
        "every additional provider round must have a persisted revision conflict"
    );
    let requests: BTreeMap<String, Value> = pending["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row["ticket"]["contract"]["title"].as_str().unwrap().into(),
                row["requests"][0].clone(),
            )
        })
        .collect();
    f.restart().await;
    for row in views(&f, &ids).await["data"].as_array().unwrap() {
        let letter = row["ticket"]["contract"]["title"].as_str().unwrap();
        assert_eq!(
            row["requests"],
            json!([requests[letter]]),
            "restart changed request ownership for {letter}"
        );
        assert_eq!(row["assignments"][0]["process_stopped"], true);
    }
    assert_eq!(
        get(&f.client, &f.base, "/tickets/overview").await["data"]["open_questions"],
        5
    );
    for (index, letter) in ["E", "B", "D", "A", "C"].into_iter().enumerate() {
        let request = &requests[letter];
        let answer=command(&f.client,&f.base,&format!("answer-{letter}"),json!([{"op":"answer","request_id":request["id"],"prompt_revision":request["prompt_revision"],"answer":format!("答案 {letter}")}])).await;
        post(&f.client, &f.base, "/tickets/update", &answer).await;
        assert_eq!(
            get(&f.client, &f.base, "/tickets/overview").await["data"]["open_questions"],
            4 - index
        );
        let fresh = command(
            &f.client,
            &f.base,
            &format!("resume-{letter}"),
            json!([{"op":"start","work_id":works[letter],"temp_id":"fresh","workspace":null}]),
        )
        .await;
        let resumed = post(&f.client, &f.base, "/tickets/dispatch", &fresh).await;
        assert_eq!(resumed["errors"], json!([]), "{resumed}");
        let result = await_views(&f, &[works[letter].clone()], |v, _| {
            v["data"][0]["ticket"]["state"] == "submitted"
        })
        .await;
        assert_eq!(result["data"][0]["ticket"]["generation"], 2);
        assert_eq!(
            result["data"][0]["requests"][0]["answer"],
            format!("答案 {letter}")
        );
    }
    assert_eq!(
        f.probe.question_retries.load(Ordering::SeqCst),
        question_retries
    );
    assert_eq!(
        f.probe.questions.load(Ordering::SeqCst),
        5 + question_retries
    );
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 15 + question_retries);
    assert_eq!(f.probe.answer_checks.load(Ordering::SeqCst), 10);
    assert_eq!(
        get(&f.client, &f.base, "/tickets/overview").await["data"]["needs_acceptance"],
        5
    );
    f.finish().await;
}

#[actix_web::test]
async fn question_waiter_reads_completed_runtime_sidecar_over_stale_main() {
    use bamboo_agent_core::storage::Storage;
    use bamboo_domain::{Message, Session};
    let temp = tempfile::tempdir().unwrap();
    let store = bamboo_storage::SessionStoreV2::new(temp.path().to_path_buf())
        .await
        .unwrap();
    let root = Session::new("bamboo-default-supervisor", "fixture-model");
    store.save_session(&root).await.unwrap();
    let mut child = Session::new_child(
        "runtime-only-question",
        &root.id,
        "fixture-model",
        "Question",
    );
    child.metadata.insert(
        "ticket.worker.question_yield.v1".into(),
        "request-id".into(),
    );
    child.add_message(Message::tool_result("question-tool", "request created"));
    child.set_last_run_status("running");
    store.save_session(&child).await.unwrap();
    let main_path = temp
        .path()
        .join(store.resolve_rel_path(&child.id).await.unwrap())
        .join("session.json");
    let before = std::fs::read(&main_path).unwrap();
    child.set_last_run_status("completed");
    store.save_runtime_state(&child).await.unwrap();
    assert_eq!(
        std::fs::read(&main_path).unwrap(),
        before,
        "runtime-only write preserves Main bytes"
    );
    let canonical = store.load_session(&child.id).await.unwrap().unwrap();
    assert_eq!(canonical.last_run_status().as_deref(), Some("completed"));
    let row = json!({"assignments":[{"runtime":{"session_id":child.id}}]});
    assert!(
        completed_question(&store, &row).await,
        "completed canonical Child must not be rejected by stale Main metadata"
    );
}

#[actix_web::test]
async fn question_diagnostics_identify_each_missing_boundary_and_latest_child() {
    use bamboo_agent_core::storage::Storage;
    use bamboo_domain::{Message, Session};
    let temp = tempfile::tempdir().unwrap();
    let store = bamboo_storage::SessionStoreV2::new(temp.path().into())
        .await
        .unwrap();
    let root = Session::new("diagnostic-supervisor", "fixture-model");
    store.save_session(&root).await.unwrap();
    let mut child = Session::new_child("diagnostic-child", &root.id, "fixture-model", "A");
    child.set_last_run_status("error");
    child.set_last_run_error("local_tool_history_unsupported");
    child
        .metadata
        .insert("ticket.worker.question_yield.v1".into(), "request-A".into());
    child.add_message(Message::assistant("unfinished tool result", None));
    store.save_session(&child).await.unwrap();
    let row = json!({
        "ticket":{"id":"work-A","contract":{"title":"A"},"state":"blocked"},
        "requests":[{"id":"request-A","assignment_id":"current"}],
        "assignments":[
            {"id":"older","generation":1,"process_stopped":true,"runtime":{"session_id":"missing-old-child"}},
            {"id":"current","generation":2,"process_stopped":false,"runtime":{"session_id":child.id}}
        ]
    });
    let failed = question_observation(&store, &row).await;
    assert_eq!(failed["assignment_id"], "current");
    assert_eq!(failed["request_count"], 1);
    assert_eq!(failed["requests"][0]["id"], "request-A");
    assert_eq!(failed["process_stopped"], false);
    assert_eq!(failed["child"]["last_run_status"], "error");
    assert_eq!(
        failed["child"]["last_run_error"],
        "local_tool_history_unsupported"
    );
    assert_eq!(failed["child"]["question_yield"], "request-A");
    assert_eq!(failed["child"]["terminal_tool"], false);
    assert_eq!(failed["child"]["last_message"]["role"], "assistant");
    assert_eq!(failed["completed_question"], false);
    child.set_last_run_status("completed");
    store.save_runtime_state(&child).await.unwrap();
    assert!(
        !completed_question(&store, &row).await,
        "a completed run still needs its Tool tail"
    );
    child.add_message(Message::tool_result("question-A", "request created"));
    child.metadata.remove("ticket.worker.question_yield.v1");
    store.save_session(&child).await.unwrap();
    assert!(
        !completed_question(&store, &row).await,
        "a Tool tail still needs its yield marker"
    );
    let missing = question_observation(&store, &json!({"assignments":[]})).await;
    assert_eq!(missing["child"]["load_error"], "missing runtime session_id");
}

#[test]
fn question_provider_rejects_non_conflict_and_successful_retries() {
    for result in [
        "local_tool_history_unsupported",
        "scope_denied: missing permit",
        "waiting_for_answer",
        "not a revision_conflict",
    ] {
        let probe = fixture::Probe::default();
        let messages =
            json!([{"role":"tool", "tool_call_id":"ticket-native-question-A-0", "content":result}]);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fixture::question_call(
                &messages, "A", &probe
            )))
            .is_err(),
            "unexpected retry for {result}"
        );
        assert_eq!(probe.questions.load(Ordering::SeqCst), 0);
        assert_eq!(probe.question_retries.load(Ordering::SeqCst), 0);
    }
}

// Deterministic adapter + controlled-provider regression. This does not inject a
// conflict into the real Host: the five-Native-Worker test owns that evidence.
#[test]
fn question_provider_retries_real_revision_conflict_with_valid_local_history() {
    use bamboo_domain::{AgentRuntimeState, Message, Role, Session, ToolCall};
    use bamboo_engine::ticket_worker_plan::{
        tickets::{
            Authority, CommandSource, Error, Operation, Principal, RequestKind, RequestStatus,
            RuntimeReceipt, ScopeBinding, TicketService, WorkState,
        },
        TicketWorkerPlan, TICKET_QUESTION_YIELD_KEY,
    };
    use bamboo_subagent::proto::LocalToolMessages;
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();
    let binding = ScopeBinding {
        scope_id: "question-retry-scope".into(),
        supervisor_session_id: "question-retry-supervisor".into(),
        binding_revision: 1,
    };
    let service = Arc::new(TicketService::open(dir.path(), binding.clone()).unwrap());
    let supervisor = Authority::from_verified_host(
        binding.clone(),
        Principal::Supervisor {
            session_id: binding.supervisor_session_id.clone(),
        },
    );
    let contract = json!({
        "title":"A", "objective":"TICKET_QUESTION_E2E:A",
        "constraints":["Own private plan and answer only"],
        "acceptance":["Exact answer"], "user_acceptance_required":true,
        "allowed_tools":["Task"]
    });
    let create = service
        .prepare_command(
            &supervisor,
            "create-question-retry",
            serde_json::from_value(json!([
                {"op":"create", "temp_id":"work", "kind":"work", "parent":null,
                 "depends_on":[], "contract":contract},
                {"op":"ready", "work_id":"work"},
                {"op":"start", "work_id":"work", "temp_id":"assignment", "workspace":null}
            ]))
            .unwrap(),
        )
        .unwrap();
    let created = service.execute(&supervisor, &create).unwrap();
    let work_id = created.ids["work"].clone();
    let assignment_id = created.ids["assignment"].clone();
    let initial = service.published().unwrap().1;
    let assignment = &initial.assignments[&assignment_id];
    let runtime_receipt = RuntimeReceipt {
        dispatch_key: assignment.dispatch_key.clone(),
        spec_hash: initial.intents[&assignment.dispatch_key].spec_hash.clone(),
        run_id: "question-retry-run".into(),
        session_id: "question-retry-child".into(),
    };
    let runtime = Authority::from_verified_host(binding.clone(), Principal::Runtime);
    // Synthetic admission is limited to this public adapter test. It is not
    // evidence of a native process starting or stopping.
    let admit = service
        .prepare_command(
            &runtime,
            "admit-question-retry",
            vec![
                Operation::Admitted {
                    assignment_id: assignment_id.clone(),
                    receipt: runtime_receipt.clone(),
                },
                Operation::Running {
                    assignment_id: assignment_id.clone(),
                },
            ],
        )
        .unwrap();
    service.execute(&runtime, &admit).unwrap();
    let worker = Authority::from_verified_host(
        binding.clone(),
        Principal::Worker {
            assignment_id: assignment_id.clone(),
            generation: assignment.generation,
            run_id: runtime_receipt.run_id.clone(),
            session_id: runtime_receipt.session_id.clone(),
        },
    );
    let mut child = Session::new_child(
        &runtime_receipt.session_id,
        &binding.supervisor_session_id,
        "fixture-model",
        "Question retry",
    );
    child.agent_runtime_state = Some(AgentRuntimeState::new(&runtime_receipt.run_id));
    let plan = TicketWorkerPlan::from_runtime_receipt(service.clone(), &assignment_id).unwrap();
    plan.bind_session(&mut child).unwrap();
    let probe = fixture::Probe::default();
    let first: ToolCall =
        serde_json::from_value(fixture::question_call(&json!([]), "A", &probe)).unwrap();
    assert_eq!(first.id, "ticket-native-question-A-0");
    assert_eq!(first.function.name, "Task");
    let arguments: Value = serde_json::from_str(&first.function.arguments).unwrap();
    let packet = service
        .child_context_packet(&worker, &assignment_id, 65536)
        .unwrap();
    let packet_hash = bamboo_engine::ticket_worker_plan::tickets::content_hash(
        &bamboo_engine::ticket_worker_plan::tickets::canonical_bytes(&packet).unwrap(),
    );
    let failed_operation_id = format!("worker-plan/{}/{}", assignment.dispatch_key, first.id);
    // Freeze the same ReplacePlan + Ask + YieldForInput command the public Task
    // adapter generates, then make its CAS stale using an unrelated real write.
    let stale = service
        .prepare_source_command(
            &worker,
            &failed_operation_id,
            serde_json::from_value(json!([
                {"op":"replace_plan", "assignment_id":assignment_id,
                 "expected_plan_revision":0,
                 "steps":[{"id":arguments["tasks"][0]["id"], "parent":null,
                           "title":arguments["tasks"][0]["content"],
                           "completed":false, "status":"blocked"}]},
                {"op":"ask", "work_id":work_id, "temp_id":"question",
                 "prompt":arguments["question"]["prompt"], "action":null},
                {"op":"yield_for_input", "assignment_id":assignment_id,
                 "request_id":"question", "packet_hash":packet_hash}
            ]))
            .unwrap(),
            CommandSource::WorkerTask {
                arguments: arguments.clone(),
            },
        )
        .unwrap();
    let advance = service
        .prepare_command(
            &supervisor,
            "advance-unrelated-scope-revision",
            serde_json::from_value(json!([
                {"op":"create", "temp_id":"unrelated", "kind":"work", "parent":null,
                 "depends_on":[], "contract":{
                     "title":"Unrelated", "objective":"Advance shared scope revision",
                     "constraints":[], "acceptance":["Remain a draft"],
                     "user_acceptance_required":true, "allowed_tools":["Task"]
                 }}
            ]))
            .unwrap(),
        )
        .unwrap();
    let advance_receipt = service.execute(&supervisor, &advance).unwrap();
    assert!(advance_receipt.committed_seq > stale.expected_seq);
    let error = service.execute(&worker, &stale).unwrap_err();
    assert!(matches!(&error, Error::RevisionConflict), "{error}");
    let after_conflict = service.published().unwrap().1;
    assert_eq!(after_conflict.seq, advance_receipt.committed_seq);
    assert!(after_conflict.requests.is_empty());
    assert_eq!(
        after_conflict.assignments[&assignment_id]
            .plan
            .plan_revision,
        0
    );
    assert!(!after_conflict.receipts.contains_key(&failed_operation_id));
    assert!(!child.metadata.contains_key(TICKET_QUESTION_YIELD_KEY));

    // RemoteWorkerPlan wraps a real HostBridge error as AuthorityUnavailable;
    // result_handler persists this exact text with tool_success=false.
    let conflict_result = Error::AuthorityUnavailable(error.to_string()).to_string();
    assert_eq!(conflict_result, "authority_unavailable: revision_conflict");
    child.add_message(Message::assistant("", Some(vec![first.clone()])));
    child.add_message(Message::tool_result_with_status(
        &first.id,
        conflict_result,
        false,
    ));
    let retry: ToolCall = serde_json::from_value(fixture::question_call(
        &serde_json::to_value(&child.messages).unwrap(),
        "A",
        &probe,
    ))
    .unwrap();
    assert_eq!(retry.id, "ticket-native-question-A-1");
    assert_ne!(retry.id, first.id);
    assert_eq!(retry.function.name, "Task");
    let retry_arguments: Value = serde_json::from_str(&retry.function.arguments).unwrap();
    assert_eq!(
        retry_arguments, arguments,
        "retry preserves the exact private question"
    );
    child.add_message(Message::assistant("", Some(vec![retry.clone()])));
    plan.apply_task(&mut child, &retry.id, &retry_arguments)
        .unwrap();
    let yielded = service.published().unwrap().1;
    assert_eq!(yielded.seq, after_conflict.seq + 1);
    assert_eq!(yielded.requests.len(), 1);
    assert!(yielded.submissions.is_empty());
    assert!(!yielded.receipts.contains_key(&failed_operation_id));
    let retry_operation_id = format!("worker-plan/{}/{}", assignment.dispatch_key, retry.id);
    assert!(yielded.receipts.contains_key(&retry_operation_id));
    let request = yielded.requests.values().next().unwrap();
    assert_eq!(request.kind, RequestKind::Question);
    assert_eq!(request.status, RequestStatus::Open);
    assert_eq!(request.work_id, work_id);
    assert_eq!(
        request.assignment_id.as_deref(),
        Some(assignment_id.as_str())
    );
    assert_eq!(request.generation, assignment.generation);
    assert_eq!(request.contract_revision, assignment.contract_revision);
    assert_eq!(
        request.prompt,
        arguments["question"]["prompt"].as_str().unwrap()
    );
    assert_eq!(
        request.worker_packet_hash.as_deref(),
        Some(packet_hash.as_str())
    );
    let current_assignment = &yielded.assignments[&assignment_id];
    assert_eq!(
        current_assignment.awaiting_request.as_deref(),
        Some(request.id.as_str())
    );
    assert_eq!(current_assignment.plan.plan_revision, 1);
    assert_eq!(current_assignment.plan.steps.len(), 1);
    assert_eq!(yielded.tickets[&work_id].state, WorkState::Blocked);
    assert!(
        !current_assignment.process_stopped,
        "adapter test does not own process-stop proof"
    );
    let yield_marker: Value =
        serde_json::from_str(&child.metadata[TICKET_QUESTION_YIELD_KEY]).unwrap();
    assert_eq!(yield_marker["id"], request.id);
    assert_eq!(yield_marker["assignment_id"], assignment_id);
    child.add_message(Message::tool_result(
        &retry.id,
        json!({"status":"waiting_for_answer", "request":request}).to_string(),
    ));
    assert_eq!(child.messages.last().unwrap().role, Role::Tool);
    assert_eq!(probe.questions.load(Ordering::SeqCst), 2);
    assert_eq!(probe.question_retries.load(Ordering::SeqCst), 1);

    let messages: Vec<Value> = child
        .messages
        .iter()
        .map(|message| serde_json::to_value(message).unwrap())
        .collect();
    let history = LocalToolMessages::Complete {
        version: 1,
        messages: messages.clone(),
    };
    assert_eq!(history.validate().unwrap().len(), 4);
    // Negative control: reproduce the old fixture's repeated tool-call ID while
    // leaving message IDs and every other field valid. Production must reject it.
    let mut duplicate = messages;
    duplicate[2]["tool_calls"][0]["id"] = json!(first.id);
    duplicate[3]["tool_call_id"] = json!(first.id);
    assert_eq!(
        LocalToolMessages::Complete {
            version: 1,
            messages: duplicate,
        }
        .validate()
        .unwrap_err(),
        "local_tool_history_unsupported"
    );
    // An exact Host callback replay still remains idempotent; it is not a new
    // provider round and therefore does not append another local-history pair.
    plan.apply_task(&mut child, &retry.id, &retry_arguments)
        .unwrap();
    let replayed = service.published().unwrap().1;
    assert_eq!(replayed.seq, yielded.seq);
    assert_eq!(replayed.requests.len(), 1);
    assert_eq!(replayed.assignments[&assignment_id].plan.plan_revision, 1);
}
