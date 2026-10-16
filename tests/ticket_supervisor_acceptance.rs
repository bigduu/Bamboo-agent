//! Actual Host/model tools/native Workers, with a scripted HTTP provider.
//! Tests orchestration and authority, not real-model semantic judgment.
#![cfg(unix)]
#[path = "support/ticket_runtime.rs"]
mod fixture;
use bamboo_domain::{Storage, DEFAULT_SUPERVISOR_SESSION_ID};
use bamboo_engine::ticket_worker_plan::tickets::content_hash;
use bamboo_storage::SessionStoreV2;
use fixture::{get, post, Fixture, Probe};
use serde_json::{json, Value};
use std::{sync::atomic::Ordering, time::Duration};

const TITLE: &str = "SUPERVISOR_ACCEPTANCE_2023";
const GOOD: &str = "answer=42";
const BAD: &str = "answer=41";
const CORRECTION: &str = "Rework: the previous artifact failed the exact-byte criterion; return answer=42, not answer=41.";

fn tool_result(messages: &Value, id: &str) -> Value {
    let message = messages
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .find(|m| m["role"] == "tool" && m["tool_call_id"] == id)
        .unwrap_or_else(|| panic!("missing real tool result {id}: {messages}"));
    let value: Value = serde_json::from_str(message["content"].as_str().unwrap()).unwrap();
    value
        .get("result")
        .and_then(Value::as_str)
        .map(|inner| serde_json::from_str(inner).unwrap())
        .unwrap_or(value)
}

fn controlled(body: &Value, probe: &Probe) -> Option<(Value, &'static str)> {
    let tools = body["tools"].as_array()?;
    if tools.len() == 1 && tools[0]["function"]["name"] == "Task" {
        probe.calls.fetch_add(1, Ordering::SeqCst);
        let messages = &body["messages"];
        assert!(messages.to_string().contains(TITLE));
        assert!(!messages
            .to_string()
            .contains("Supervisor delegation-first policy"));
        let has_plan = messages
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["role"] == "tool");
        if !has_plan {
            let args = json!({"tasks":[{"id":"deliver","content":"Produce the assigned exact-byte artifact","status":"pending"}]});
            return Some((
                json!({"tool_calls":[{"index":0,"id":"worker-plan","type":"function","function":{"name":"Task","arguments":args.to_string()}}]}),
                "tool_calls",
            ));
        }
        let rework = messages.to_string().contains(CORRECTION);
        let mut state = probe.scenario.lock().unwrap();
        let delivered = state["deliveries"].as_u64().unwrap();
        assert_eq!(
            rework,
            delivered == 1,
            "rework must reach the actual Worker context"
        );
        state["deliveries"] = json!(delivered + 1);
        return Some((json!({"content":if rework { GOOD } else { BAD }}), "stop"));
    }
    if !tools
        .iter()
        .any(|t| t["function"]["name"] == "work_overview")
    {
        return None;
    }
    probe.root_calls.fetch_add(1, Ordering::SeqCst);
    let mut state = probe.scenario.lock().unwrap();
    let round = state["round"].as_u64().unwrap();
    let ultra = state["ultra"].as_bool().unwrap();
    let messages = &body["messages"];
    let prompt = messages.to_string();
    for required in [
        "Supervisor delegation-first policy",
        "Delegate execution",
        "assigned acceptance criteria",
        "reject the exact current submission",
        "Technical verification does not replace final User acceptance",
    ] {
        assert!(
            prompt.contains(required),
            "actual provider prompt missing {required}"
        );
    }
    assert_eq!(prompt.contains("Ultra Root thinking mode"), ultra);
    assert!(tools.iter().all(|t| !matches!(
        t["function"]["name"].as_str(),
        Some("Write" | "Edit" | "exec_command" | "load_skill" | "workflow_run")
    )));
    let prefix = format!("acceptance-{round}-");
    let id = |name: &str| format!("{prefix}{name}");
    let last_id = messages.as_array().unwrap().last().unwrap()["tool_call_id"]
        .as_str()
        .unwrap_or("");
    let stage = last_id.strip_prefix(&prefix).unwrap_or("start");
    let result = |stage: &str| tool_result(messages, &id(stage));
    let mutation = |snapshot: &Value, op: &str, operations: Value| {
        json!({
        "operation_id":id(op),"expected_seq":snapshot["seq"],
        "expected_epoch":snapshot["authority_epoch"],"operations":operations})
    };
    let call = match stage {
        "start" => Some(("overview", "work_overview", json!({}))),
        "overview" => {
            let overview = result("overview");
            let input = &overview["pending_message"]["input"];
            if !input.is_object() {
                // A real child notification may wake the Supervisor between the
                // explicit review turns. It grants no new Human decision.
                return Some((
                    json!({"content":"Worker status observed; no new User decision or final acceptance."}),
                    "stop",
                ));
            }
            let operations = if round == 0 {
                json!([
                    {"op":"create","temp_id":"work","kind":"work","parent":null,"depends_on":[],"contract":{
                        "title":TITLE,"objective":format!("Return exactly {GOOD}; no extra bytes."),"constraints":["Own private plan only"],
                        "acceptance":[format!("Exact UTF-8 bytes {GOOD}; SHA256 {}",content_hash(GOOD.as_bytes()))],
                        "user_acceptance_required":true,"allowed_tools":["Task"]}},
                    {"op":"ready","target":{"ref":"temporary","id":"work"}},
                    {"op":"start","target":{"ref":"temporary","id":"work"},"temp_id":"first","workspace":null}])
            } else if round == 3 {
                let candidate = input["candidates"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|c| c["contract"]["title"] == TITLE)
                    .unwrap();
                assert_eq!(candidate["current_submission"], state["good_submission"]);
                json!([{"op":"accept","target":candidate["target"],"submission_id":candidate["current_submission"],
                    "evidence":[format!("User explicitly accepted exact submission; verified SHA256 {}",content_hash(GOOD.as_bytes()))]}])
            } else {
                json!([])
            };
            let groups = if operations.as_array().unwrap().is_empty() {
                json!([])
            } else {
                json!([{"group_id":id("intent"),"item_ids":["intent"],"source_quote":input["human"]["text"],
                    "clarification":null,"operations":operations}])
            };
            Some((
                "resolve",
                "work_update",
                json!({"message_id":input["message_id"],"proposal":{"groups":groups}}),
            ))
        }
        "resolve" => {
            let resolved = result("resolve");
            assert!(
                resolved["resolution"]["groups"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|g| g["status"] == "committed"),
                "{resolved}"
            );
            if round == 0 {
                assert_eq!(resolved["dispatch"][0]["status"], "accepted_for_dispatch");
                None
            } else if round == 3 {
                None
            } else {
                Some((
                    "search",
                    "work_search",
                    json!({"filter":{"query":TITLE,"kind":"work","state":"submitted",
                    "updated_after":null,"updated_before":null,"include_archived":false},"limit":10,"cursor":null,"fixed_commit":null}),
                ))
            }
        }
        "search" => {
            let found = result("search");
            assert_eq!(found["data"].as_array().unwrap().len(), 1);
            state["work"] = found["data"][0]["id"].clone();
            Some((
                "inspect",
                "work_inspect",
                json!({"ids":[state["work"]],"depth":0,
                "sections":["assignments","submissions"],"budget_bytes":65536,"fixed_commit":null}),
            ))
        }
        "inspect" => {
            let view = result("inspect");
            let row = &view["data"][0];
            assert_eq!(row["ticket"]["state"], "submitted");
            assert_eq!(row["ticket"]["contract"]["user_acceptance_required"], true);
            let submission = row["submissions"]
                .as_array()
                .unwrap()
                .iter()
                .find(|s| s["id"] == row["ticket"]["current_submission"])
                .unwrap();
            assert_eq!(submission["stale"], false);
            assert_eq!(submission["generation"], row["ticket"]["generation"]);
            assert_eq!(
                submission["contract_revision"],
                row["ticket"]["contract_revision"]
            );
            assert_eq!(submission["artifacts"].as_array().unwrap().len(), 1);
            assert!(!submission["evidence"].as_array().unwrap().is_empty());
            let actual = submission["artifacts"][0]["sha256"].as_str().unwrap();
            assert!(submission["evidence"][0].as_str().unwrap().contains(actual));
            if round == 1 {
                assert_eq!(actual, content_hash(BAD.as_bytes()));
                assert_ne!(actual, content_hash(GOOD.as_bytes()));
                state["bad_submission"] = submission["id"].clone();
                let mut contract = row["ticket"]["contract"].clone();
                contract["objective"] = json!(format!(
                    "{} {CORRECTION}",
                    contract["objective"].as_str().unwrap()
                ));
                Some((
                    "reject",
                    "work_update",
                    mutation(
                        &view["snapshot"],
                        "reject",
                        json!([
                    {"op":"reject","work_id":state["work"],"submission_id":submission["id"],
                        "reason":format!("Exact-byte acceptance criterion failed: expected SHA256 {}, observed {actual}. {CORRECTION}",content_hash(GOOD.as_bytes()))},
                    {"op":"update_contract","work_id":state["work"],"contract":contract}]),
                    ),
                ))
            } else {
                assert_eq!(actual, content_hash(GOOD.as_bytes()));
                state["good_submission"] = submission["id"].clone();
                // A technically correct result still cannot be accepted by the model.
                Some((
                    "auto-accept",
                    "work_update",
                    mutation(
                        &view["snapshot"],
                        "auto-accept",
                        json!([
                    {"op":"accept","work_id":state["work"],"submission_id":submission["id"],"evidence":["Exact bytes and canonical hash verified"]}]),
                    ),
                ))
            }
        }
        "reject" => {
            assert_eq!(result("reject")["status"], "committed");
            Some(("fresh", "work_overview", json!({})))
        }
        "fresh" => Some((
            "dispatch",
            "work_dispatch",
            mutation(
                &result("fresh")["snapshot"],
                "rework",
                json!([
            {"op":"start","work_id":state["work"],"temp_id":"rework","workspace":null}]),
            ),
        )),
        "dispatch" => {
            let dispatch = result("dispatch");
            assert_eq!(dispatch["status"], "accepted_for_dispatch");
            assert_eq!(dispatch["errors"], json!([]));
            None
        }
        "auto-accept" => {
            assert_eq!(result("auto-accept")["status_code"], 403);
            None
        }
        other => panic!("unexpected scripted stage {other}"),
    };
    Some(match call {
        Some((stage, name, args)) => (
            json!({"tool_calls":[{"index":0,"id":id(stage),"type":"function",
            "function":{"name":name,"arguments":args.to_string()}}]}),
            "tool_calls",
        ),
        None => (
            json!({"content": match round {
                0 => "DISPATCHED: worker result and verification pending.",
                1 => "REWORK_DISPATCHED: bad artifact rejected; corrected result pending.",
                2 => "VERIFIED: exact artifact criteria passed; final User acceptance pending.",
                3 => "ACCEPTED: User accepted the exact verified submission.",
                _ => unreachable!(),
            }}),
            "stop",
        ),
    })
}

async fn run_round(f: &Fixture, round: u64, message: String) -> (Value, Value) {
    f.probe.scenario.lock().unwrap()["round"] = json!(round);
    let ultra = f.probe.scenario.lock().unwrap()["ultra"].as_bool().unwrap();
    let human = json!({"session_id":DEFAULT_SUPERVISOR_SESSION_ID,"message_id":format!("acceptance-human-{round}"),
        "message":message,"model":"ticket-model","provider":"openai", "thinking_mode":if ultra {"ultra"} else {"standard"},
        "model_ref":{"provider":"openai","model":"ticket-model"}});
    let admitted = post(&f.client, &f.base, "/chat", &human).await;
    post(
        &f.client,
        &f.base,
        &format!("/execute/{DEFAULT_SUPERVISOR_SESSION_ID}"),
        &json!({}),
    )
    .await;
    let store = SessionStoreV2::new(f.data.clone()).await.unwrap();
    let marker = [
        "DISPATCHED:",
        "REWORK_DISPATCHED:",
        "VERIFIED:",
        "ACCEPTED:",
    ][round as usize];
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let root = store
                .load_session(DEFAULT_SUPERVISOR_SESSION_ID)
                .await
                .unwrap()
                .unwrap();
            assert_ne!(
                root.last_run_status().as_deref(),
                Some("error"),
                "{:?}",
                root.last_run_error()
            );
            if root.last_run_status().as_deref() == Some("completed")
                && root.messages.iter().any(|m| m.content.starts_with(marker))
            {
                assert!(
                    root.messages
                        .iter()
                        .flat_map(|m| m.tool_calls.iter().flatten())
                        .all(|call| matches!(
                            call.function.name.as_str(),
                            "work_overview"
                                | "work_update"
                                | "work_search"
                                | "work_inspect"
                                | "work_dispatch"
                        )),
                    "Supervisor must not implement the deliverable itself"
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "round {round} timed out: {}",
            std::fs::read_to_string(f.data.join("host.log")).unwrap()
        )
    });
    (human, admitted)
}

async fn submitted(f: &Fixture, generation: u64) -> Value {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let scope = get(&f.client, &f.base, "/tickets/overview").await;
            assert_eq!(scope["data"]["work_count"], 1);
            let search = post(
                &f.client,
                &f.base,
                "/tickets/search",
                &json!({"filter":{"query":TITLE,"kind":"work","state":null,"updated_after":null,"updated_before":null,"include_archived":false},"limit":10,"cursor":null,"fixed_commit":null}),
            )
            .await;
            let work = &search["data"][0]["id"];
            let view = post(
                &f.client,
                &f.base,
                "/tickets/inspect",
                &json!({"ids":[work],"depth":0,"budget_bytes":65536,"fixed_commit":null}),
            )
            .await;
            let row = &view["data"][0];
            if row["ticket"]["state"] == "submitted" && row["ticket"]["generation"] == generation {
                assert_eq!(row["ticket"]["accepted_submission"], Value::Null);
                assert!(row["assignments"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|a| a["process_stopped"] == true));
                return view;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("actual Worker publishes its stopped canonical result")
}

#[actix_web::test]
async fn actual_supervisor_rejects_reworks_verifies_and_waits_for_exact_user_acceptance_in_both_modes(
) {
    for ultra in [false, true] {
        let mut f = Fixture::new().await;
        *f.probe.controlled_response.lock().unwrap() = Some(controlled);
        *f.probe.scenario.lock().unwrap() = json!({"round":0,"ultra":ultra,"deliveries":0});
        run_round(&f,0,format!("Create and start {TITLE}: return exactly {GOOD}; independently verify the exact bytes and reject a mismatching result for bounded rework. Wait for my final acceptance.")).await;
        let bad = submitted(&f, 1).await;
        let bad_submission = bad["data"][0]["ticket"]["current_submission"].clone();
        let hash = bad["data"][0]["submissions"][0]["artifacts"][0]["sha256"]
            .as_str()
            .unwrap();
        assert_eq!(
            f.client
                .get(format!("{}/tickets/artifacts/{hash}", f.base))
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap(),
            BAD
        );
        run_round(&f,1,"Inspect the current artifact against the existing exact-byte criterion and arrange bounded rework if it fails; do not accept it for me.".into()).await;
        let good = submitted(&f, 2).await;
        let row = &good["data"][0];
        assert_eq!(row["submissions"].as_array().unwrap().len(), 2);
        assert_eq!(row["assignments"].as_array().unwrap().len(), 2);
        assert_eq!(row["ticket"]["contract_revision"], 2);
        assert_eq!(
            row["ticket"]["contract"]["acceptance"],
            bad["data"][0]["ticket"]["contract"]["acceptance"]
        );
        let good_submission = row["ticket"]["current_submission"].clone();
        assert_ne!(good_submission, bad_submission);
        let artifact = row["submissions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["id"] == good_submission)
            .unwrap();
        let hash = artifact["artifacts"][0]["sha256"].as_str().unwrap();
        assert_eq!(
            f.client
                .get(format!("{}/tickets/artifacts/{hash}", f.base))
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap(),
            GOOD
        );
        run_round(&f,2,"Verify the corrected result against every criterion and report the evidence; final acceptance is still pending my decision.".into()).await;
        let work = row["ticket"]["id"].clone();
        let inspect = json!({"ids":[work],"depth":0,"budget_bytes":65536,"fixed_commit":null});
        assert_eq!(
            post(&f.client, &f.base, "/tickets/inspect", &inspect).await["data"][0]["ticket"]
                ["state"],
            "submitted"
        );
        let (accepted_input, admitted) = run_round(
            &f,
            3,
            format!(
                "验收通过 {TITLE} submission {} 文件SHA256 {hash}",
                good_submission.as_str().unwrap()
            ),
        )
        .await;
        let accepted = post(&f.client, &f.base, "/tickets/inspect", &inspect).await;
        assert_eq!(accepted["data"][0]["ticket"]["state"], "accepted");
        assert_eq!(
            accepted["data"][0]["ticket"]["accepted_submission"],
            good_submission
        );
        assert_eq!(
            f.probe.calls.load(Ordering::SeqCst),
            4,
            "two real Workers, private Task then output"
        );
        let root_calls = f.probe.root_calls.load(Ordering::SeqCst);
        f.restart().await;
        let replayed = post(&f.client, &f.base, "/chat", &accepted_input).await;
        assert_eq!(replayed["ingress_seq"], admitted["ingress_seq"]);
        let restored = post(&f.client, &f.base, "/tickets/inspect", &inspect).await;
        assert_eq!(
            restored["data"][0]["ticket"]["accepted_submission"],
            good_submission
        );
        assert_eq!(
            restored["data"][0]["submissions"],
            accepted["data"][0]["submissions"]
        );
        assert_eq!(f.probe.calls.load(Ordering::SeqCst), 4);
        assert_eq!(f.probe.root_calls.load(Ordering::SeqCst), root_calls);
        f.finish().await;
    }
}
