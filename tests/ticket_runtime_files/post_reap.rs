//! Failure-only evidence for the legacy Worker handoff in `ticket_runtime_files`.

use super::Fixture;
use bamboo_engine::ticket_runtime::ticket_child_id;
use serde_json::{json, Value};
use std::{
    fs::File,
    io::{self, Read, Seek, SeekFrom},
    path::Path,
    sync::atomic::Ordering,
    time::Duration,
};

#[derive(Default)]
pub(super) struct LastInspect {
    pub(super) view: Option<Value>,
    pub(super) completed: usize,
}

fn unavailable(reason: impl Into<String>) -> Value {
    json!({"unavailable": reason.into()})
}

fn fields(value: &Value, names: &[&str]) -> Value {
    Value::Object(
        names
            .iter()
            .filter_map(|&name| Some((name.to_owned(), value.get(name)?.clone())))
            .collect(),
    )
}

fn dispatch_evidence(observation: &Value) -> Value {
    let mut summary = fields(
        observation,
        &["status", "session_id", "reason", "terminal_status"],
    );
    if let Some(receipt) = observation.get("receipt") {
        summary["receipt"] = receipt.clone();
    }
    summary
}

fn inspected_work<'a>(view: &'a Value, work_id: &str) -> Option<&'a Value> {
    view["data"]
        .as_array()?
        .iter()
        .find(|row| row["ticket"]["id"] == work_id)
}

fn last_work_evidence(last: &LastInspect, work_id: &str, assignment_id: Option<&str>) -> Value {
    let Some(view) = &last.view else {
        return unavailable("no completed B inspection before deadline");
    };
    let Some(work) = inspected_work(view, work_id) else {
        return unavailable("last completed inspection has no matching Work B");
    };
    let assignment = assignment_id.and_then(|id| {
        work["assignments"]
            .as_array()?
            .iter()
            .find(|row| row["id"] == id)
    });
    let assignment = assignment.map_or_else(|| unavailable("no assignment matching the B command receipt"), |row| {
        let mut summary = fields(row, &["id", "generation", "state", "dispatch_key", "runtime",
            "process_stopped", "awaiting_request", "updated_seq"]);
        summary["effects"] = row["effects"].as_object().map_or(Value::Null, |effects| {
            json!({"count": effects.len(), "sample": effects.iter().take(6).map(|(id, effect)| {
                json!({"id": id, "state": effect["state"],
                    "provider_receipt_present": effect.get("provider_receipt").is_some_and(|value| !value.is_null())})
            }).collect::<Vec<_>>()})
        });
        summary
    });
    let submissions = work["submissions"].as_array().map_or_else(
        || unavailable("submissions absent"),
        |rows| {
            json!({"count": rows.len(), "sample": rows.iter().take(6).map(|row| {
            fields(row, &["id", "assignment_id", "generation", "stale", "updated_seq"])
        }).collect::<Vec<_>>()})
        },
    );
    json!({
        "snapshot": view["snapshot"],
        "work": fields(&work["ticket"], &["id", "generation", "state", "blocked", "paused", "active_assignment", "current_submission", "accepted_submission", "updated_seq"]),
        "assignment": assignment,
        "submissions": submissions,
    })
}

fn controls_evidence(view: &Value, work_id: &str) -> Value {
    let Some(controls) = inspected_work(view, work_id).and_then(|row| row.get("controls")) else {
        return unavailable("B control facts absent");
    };
    json!({"snapshot": view["snapshot"], "controls": controls})
}

fn operation_evidence(receipt: &Value) -> Value {
    fields(
        receipt,
        &["operation_id", "committed_seq", "request_hash", "ids"],
    )
}

fn tool_history(history: &Value) -> Value {
    let Some(messages) = history["messages"].as_array() else {
        return unavailable("Session history response contains no message list");
    };
    let observed = messages.iter().rev().take(8).map(|message| {
        let mut entry = fields(message, &["id", "role", "tool_call_id", "tool_success"]);
        if let Some(calls) = message["tool_calls"].as_array() {
            entry["tool_calls"] = json!({"count": calls.len(), "sample": calls.iter().take(8).map(|call| {
                json!({"id": call["id"], "name": call["function"]["name"]})
            }).collect::<Vec<_>>()});
        }
        if message["role"] == "assistant" && message["content"] == "TICKET_CODE_DONE" {
            entry["fixture_done_marker"] = json!(true);
        }
        entry
    }).collect::<Vec<_>>();
    json!({"session_id": history["session_id"], "page_count": messages.len(),
        "total_message_count": history["total_message_count"], "page_truncated": history["truncated"],
        "newest_first": observed})
}

fn bounded(text: &str, limit: usize) -> String {
    if text.len() <= limit {
        return text.to_owned();
    }
    format!(
        "{} ... (truncated)",
        &text[..text.floor_char_boundary(limit)]
    )
}

fn report_line(label: &str, value: &Value) -> String {
    format!("{label}: {}", bounded(&value.to_string(), 4096))
}

fn last_inspection_line(last: &LastInspect, work_id: &str, assignment_id: Option<&str>) -> String {
    report_line(
        "Last completed B inspection before deadline",
        &json!({
            "completed_non_submitted_polls": last.completed,
            "view": last_work_evidence(last, work_id, assignment_id),
        }),
    )
}

async fn sample_json(request: reqwest::RequestBuilder) -> Result<Value, String> {
    // Only after the acceptance deadline. Never use these reads as submission proof.
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        let mut response = request.send().await.map_err(|error| error.to_string())?;
        if !response.status().is_success() {
            return Err(format!("HTTP {}", response.status()));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|error| error.to_string())? {
            if chunk.len() > 65536 - body.len() {
                return Err("response exceeded the 65536-byte diagnostic budget".into());
            }
            body.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&body).map_err(|error| error.to_string())
    })
    .await;
    result.unwrap_or_else(|_| Err("read exceeded the 2s diagnostic budget".into()))
}

fn owned_processes(fixture: &Fixture, old_pid: u32) -> Value {
    let Some(host) = &fixture.host else {
        return unavailable("owned Host handle is missing");
    };
    let pid = host.0.id();
    let system = match sysinfo::System::new_all() {
        Ok(system) => system,
        Err(error) => {
            return json!({"host_pid": pid, "old_worker_pid": old_pid,
            "endpoint": fixture.base, "process_table": unavailable(error.to_string())})
        }
    };
    let owned_workers = system
        .processes()
        .values()
        .filter(|process| {
            process
                .parent()
                .is_some_and(|parent| parent.as_u32() == pid)
                && process.cmd().iter().any(|arg| arg == "subagent-worker")
        })
        .collect::<Vec<_>>();
    json!({"host_pid": pid, "endpoint": fixture.base, "old_worker_pid": old_pid,
        "host_status_in_process_table": system.process(sysinfo::Pid::from_u32(pid)).map(|host| format!("{:?}", host.status())),
        "owned_worker_count": owned_workers.len(),
        "owned_worker_sample": owned_workers.iter().take(8).map(|worker| {
            json!({"pid": worker.pid().as_u32(), "status": format!("{:?}",worker.status())})
        }).collect::<Vec<_>>()})
}

fn session_log_tail(path: &Path, session_id: &str) -> io::Result<Value> {
    const LOG_BUDGET: u64 = 128 * 1024;
    let mut file = File::open(path)?;
    let offset = file.metadata()?.len().saturating_sub(LOG_BUDGET);
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = Vec::new();
    file.take(LOG_BUDGET).read_to_end(&mut bytes)?;
    let text = String::from_utf8_lossy(&bytes);
    let lines = text.lines().skip(usize::from(offset > 0));
    let matching = lines
        .filter(|line| line.contains(session_id))
        .collect::<Vec<_>>();
    Ok(
        json!({"session_id": session_id, "source": path, "offset": offset, "bytes_read": bytes.len(),
        "matching_lines_in_sample": matching.len(),
        "latest_matching_lines": matching.iter().rev().take(8).map(|line| bounded(line, 400)).collect::<Vec<_>>() }),
    )
}

fn worktree_write(path: &Path) -> io::Result<Value> {
    let file = File::open(path)?;
    let length = file.metadata()?.len();
    let mut prefix = Vec::new();
    file.take(1024).read_to_end(&mut prefix)?;
    Ok(json!({"path": path, "length": length,
        "exact_expected_bytes": length == prefix.len() as u64 && prefix == b"pub fn answer() -> u8 { 42 }\n"}))
}

pub(super) async fn timeout_evidence(
    fixture: &Fixture,
    work_id: &str,
    response: &Value,
    last: &LastInspect,
    old_pid: u32,
    provider_calls_before_b: usize,
    workspace: &Path,
) -> String {
    let receipt = &response["receipt"];
    let assignment_id = receipt["ids"]["bb"].as_str();
    let runtime = match response["runtime"].as_array().map(Vec::as_slice) {
        Some([entry]) => Some(entry),
        _ => None,
    };
    let key = runtime.and_then(|entry| entry["dispatch_key"].as_str());
    let session_id = key.map(ticket_child_id);
    let provider_calls_on_timeout = fixture.probe.calls.load(Ordering::SeqCst);
    let processes = owned_processes(fixture, old_pid);
    let output_file = worktree_write(&workspace.join("answer-created.rs"))
        .unwrap_or_else(|error| unavailable(error.to_string()));
    // This case never restarts its Host. The log is in its fresh fixture data directory.
    let log = session_id
        .as_deref()
        .map(|id| session_log_tail(&fixture.data.join("host.log"), id));
    let scoped_log = match log {
        Some(Ok(log)) => log,
        Some(Err(error)) => unavailable(error.to_string()),
        None => unavailable("B dispatch key absent"),
    };
    let dispatch_response = runtime.map_or_else(
        || unavailable("one B runtime observation was not returned"),
        |entry| dispatch_evidence(&entry["observation"]),
    );
    // Inspect uses POST for its selector body and is read-only. Keep owner
    // controls out of the pre-deadline polls so the original path stays exact.
    let controls = sample_json(fixture.client.post(format!("{}/tickets/inspect", fixture.base)).json(&json!({
        "ids": [work_id], "depth": 0, "sections": ["controls"], "budget_bytes": 65536, "fixed_commit": null
    })));
    let (controls, dispatch, session, history) =
        if let (Some(key), Some(id)) = (key, session_id.as_deref()) {
            let get = |path: String| fixture.client.get(format!("{}{path}", fixture.base));
            tokio::join!(
                controls,
                sample_json(get(format!("/tickets/dispatch/{key}"))),
                sample_json(get(format!("/sessions/{id}"))),
                sample_json(get(format!("/sessions/{id}/history?limit=8"))),
            )
        } else {
            let missing = || Err("B dispatch key absent".to_owned());
            (controls.await, missing(), missing(), missing())
        };
    let controls = controls
        .map(|value| controls_evidence(&value, work_id))
        .unwrap_or_else(unavailable);
    let dispatch = dispatch
        .map(|value| dispatch_evidence(&value))
        .unwrap_or_else(unavailable);
    let session = session
        .map(|value| {
            fields(
                &value["session"],
                &[
                    "id",
                    "kind",
                    "last_run_status",
                    "last_run_error",
                    "is_running",
                    "message_count",
                ],
            )
        })
        .unwrap_or_else(unavailable);
    let history = history
        .map(|value| tool_history(&value))
        .unwrap_or_else(unavailable);
    [
        report_line(
            "B dispatch response",
            &json!({"work_id": work_id, "assignment_id": assignment_id,
            "dispatch_key": key, "expected_session_id": session_id,
            "status": response["status"], "receipt": operation_evidence(receipt),
            "observation": dispatch_response}),
        ),
        last_inspection_line(last, work_id, assignment_id),
        report_line("Owned processes at timeout", &processes),
        report_line(
            "Fixture provider calls (aggregate, no B attribution)",
            &json!({"before_b_dispatch": provider_calls_before_b,
            "at_timeout": provider_calls_on_timeout}),
        ),
        report_line(
            "Observed worktree output at timeout (no Submission attribution)",
            &output_file,
        ),
        report_line(
            "B Session lines in bounded current-launch Host log sample",
            &scoped_log,
        ),
        report_line("B Work controls queried after deadline", &controls),
        report_line("B dispatch queried after deadline", &dispatch),
        report_line("B Session summary queried after deadline", &session),
        report_line(
            "B Session persisted tool history queried after deadline",
            &history,
        ),
    ]
    .join("\n")
}

#[test]
fn timeout_report_keeps_b_assignment_and_bounded_tool_evidence() {
    let last = LastInspect {
        completed: 12,
        view: Some(json!({"snapshot": {"seq": 7}, "data": [
            {"ticket": {"id": "work-a"}, "assignments": [{"id": "assignment-a", "state": "cancelled"}]},
            {"ticket": {"id": "work-b", "state": "running"}, "submissions": [],
             "assignments": [
                {"id": "wrong-assignment", "state": "outcome_unknown"},
                {"id": "assignment-b", "state": "running", "runtime": {"session_id": "ticket-b", "run_id": "run-b"},
                 "effects": {"write-b": {"state": "succeeded", "provider_receipt": "omit-me"}}}
            ]}
        ]})),
    };
    let rendered = last_inspection_line(&last, "work-b", Some("assignment-b"));
    for expected in [
        "completed_non_submitted_polls",
        "work-b",
        "assignment-b",
        "ticket-b",
        "run-b",
        "submissions",
        "write-b",
        "provider_receipt_present",
    ] {
        assert!(
            rendered.contains(expected),
            "missing {expected}: {rendered}"
        );
    }
    for excluded in ["work-a", "wrong-assignment", "omit-me"] {
        assert!(
            !rendered.contains(excluded),
            "included {excluded}: {rendered}"
        );
    }
    assert!(
        last_inspection_line(&LastInspect::default(), "work-b", None)
            .contains("no completed B inspection")
    );
    let controls = controls_evidence(
        &json!({"snapshot": {"seq": 9}, "data": [
            {"ticket": {"id": "work-a"}, "controls": {"process_stopped": true}},
            {"ticket": {"id": "work-b"}, "controls": {"process_stopped": null}}
        ]}),
        "work-b",
    );
    assert_eq!(controls["snapshot"]["seq"], 9);
    assert_eq!(controls["controls"], json!({"process_stopped": null}));
    let receipt = operation_evidence(
        &json!({"operation_id":"start-b-after-reap", "committed_seq":10,
        "request_hash":"request-b", "ids":{"bb":"assignment-b"}, "canonical_request":"omit-me"}),
    );
    assert_eq!(receipt["committed_seq"], 10);
    assert_eq!(receipt["request_hash"], "request-b");
    assert_eq!(receipt["ids"]["bb"], "assignment-b");
    assert!(receipt.get("canonical_request").is_none());

    let mut messages = vec![json!({"id":"old-tool", "role":"tool"})];
    messages.extend((0..8).map(|i| {
        json!({"id": format!("message-{i}"), "role":"tool", "tool_call_id":"code-write",
        "tool_success": true, "content": "hidden payload".repeat(100)})
    }));
    let history = tool_history(
        &json!({"session_id":"ticket-b", "messages": messages, "total_message_count": 11}),
    );
    let rendered = report_line(
        "B Session persisted tool history queried after deadline",
        &history,
    );
    assert!(rendered.contains("code-write"));
    assert!(!rendered.contains("old-tool") && !rendered.contains("hidden payload"));
    assert_eq!(history["page_count"], 9);
    assert_eq!(history["newest_first"].as_array().unwrap().len(), 8);
    let capped = report_line("sample", &json!({"long": "界".repeat(2000)}));
    assert!(capped.ends_with("(truncated)"));
    assert!(capped.len() < 4200);
}
