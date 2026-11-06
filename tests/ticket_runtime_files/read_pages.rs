//! Actual Native Host/Git/Read/Task delivery; deterministic provider, no live
//! model reasoning or formal installed acceptance claim.
use super::*;
use bamboo_engine::ticket_worker_plan::tickets::{content_hash, FileReply};
use serde_json::Value;

fn response(body: &Value, probe: &fixture::Probe) -> Option<(Value, &'static str)> {
    let tools = body["tools"].as_array()?;
    if tools.len() != 2
        || !tools
            .iter()
            .all(|t| matches!(t["function"]["name"].as_str(), Some("Read" | "Task")))
    {
        return None;
    }
    probe.calls.fetch_add(1, Ordering::SeqCst);
    let mut state = probe.scenario.lock().unwrap();
    let files = state["files"].as_array().unwrap().clone();
    let mut index = state["file_index"].as_u64().unwrap_or(0) as usize;
    let previous = state["last_call"].as_str().map(str::to_owned);
    let mut update_plan = previous.is_none();
    if let Some(previous) = &previous {
        let message = body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["role"] == "tool" && m["tool_call_id"] == previous.as_str())
            .expect("actual result of the preceding native tool call");
        let raw = message["content"].as_str().unwrap();
        if previous.starts_with("source-page-") {
            let value: Value = serde_json::from_str(raw).expect("native file reply JSON");
            let value = if let Some(nested) = value.get("result") {
                nested
                    .as_str()
                    .map_or_else(|| nested.clone(), |s| serde_json::from_str(s).unwrap())
            } else {
                value
            };
            let reply: FileReply = serde_json::from_value(value).expect("typed actual Read page");
            assert_eq!(reply.sha256, files[index]["sha256"].as_str().unwrap());
            let page = reply.page.expect("native Read must disclose its range");
            assert_eq!(page.offset_bytes, state["offset"].as_u64().unwrap());
            assert_eq!(page.total_bytes, files[index]["bytes"].as_u64().unwrap());
            let text = reply.content.unwrap();
            let assembled = state["assembled"][index].as_str().unwrap().to_owned() + &text;
            assert_eq!(assembled.len() as u64, page.end_offset_bytes);
            state["assembled"][index] = json!(assembled);
            probe.input_checks.fetch_add(1, Ordering::SeqCst);
            if let Some(next) = page.next_offset_bytes {
                assert!(!page.eof && next > page.offset_bytes);
                state["offset"] = json!(next);
                state["expected_sha256"] = json!(reply.sha256);
            } else {
                assert!(page.eof);
                let assembled = state["assembled"][index].as_str().unwrap();
                assert_eq!(content_hash(assembled.as_bytes()), files[index]["sha256"]);
                assert_eq!(assembled.lines().count() as u64, files[index]["lines"]);
                eprintln!(
                    "Native paged source verified: {} ({} bytes)",
                    files[index]["relative"],
                    assembled.len()
                );
                index += 1;
                state["file_index"] = json!(index);
                state["offset"] = json!(0);
                state["expected_sha256"] = Value::Null;
                update_plan = true;
            }
        } else {
            assert_eq!(
                raw, "Task list updated with 3 items",
                "native Task acknowledgement"
            );
        }
    }
    let (id, name, arguments) = if update_plan {
        let tasks: Vec<_> = files.iter().enumerate().map(|(i, file)| json!({
            "id":format!("source-{i}"),"content":format!("Read {} with exact source hash and line evidence",file["relative"].as_str().unwrap()),
            "status":if i < index { "completed" } else if i == index { "in_progress" } else { "pending" }
        })).collect();
        (
            format!("source-plan-{index}"),
            "Task",
            json!({"tasks":tasks}),
        )
    } else if index < files.len() {
        let offset = state["offset"].as_u64().unwrap();
        let mut args = json!({"file_path":files[index]["path"]});
        if offset > 0 {
            args["offset_bytes"] = json!(offset);
            args["expected_sha256"] = state["expected_sha256"].clone();
        }
        (format!("source-page-{index}-{offset}"), "Read", args)
    } else {
        let output = format!("PAGED_SOURCE_DONE\n{}\nStatic source reads only; no command, test or browser was executed.", serde_json::to_string(&files).unwrap());
        return Some((json!({"content":output}), "stop"));
    };
    state["last_call"] = json!(id);
    Some((
        json!({"tool_calls":[{"index":0,"id":id,"type":"function","function":{"name":name,"arguments":arguments.to_string()}}]}),
        "tool_calls",
    ))
}

#[actix_web::test]
async fn native_read_task_handoff_consumes_three_large_sources_and_survives_restart() {
    let mut f = Fixture::new().await;
    let repo = f.temp.join("repo");
    let workspace = f.temp.join(".bamboo/worktrees/source-audit");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::create_dir_all(workspace.parent().unwrap()).unwrap();
    git(&repo, &["init", "-b", "main"]);
    let mut files = Vec::new();
    for (name, target) in [
        ("ChatPane.tsx", 89032),
        ("ChatPane.test.tsx", 158590),
        ("new-chat-worktree.spec.ts", 15731),
    ] {
        let mut content =
            "// Purpose: inspect undefined defaults, explicit null, and explicit sectionId.\n"
                .to_owned();
        let mut line = 2;
        while content.len() < target {
            content.push_str(&format!("// {line}: sectionId defaults / null / 显式 🌿\n"));
            line += 1;
        }
        let mut end = target;
        while !content.is_char_boundary(end) {
            end -= 1;
        }
        content.truncate(end);
        std::fs::write(repo.join(name), &content).unwrap();
        files.push(json!({"relative":name,"path":workspace.join(name),"bytes":content.len(),"lines":content.lines().count(),"sha256":content_hash(content.as_bytes())}));
    }
    git(&repo, &["add", "."]);
    git(
        &repo,
        &[
            "-c",
            "user.name=Ticket fixture",
            "-c",
            "user.email=ticket@fixture.invalid",
            "commit",
            "-m",
            "representative source sizes",
        ],
    );
    let base = git(&repo, &["rev-parse", "HEAD"]);
    git(
        &repo,
        &[
            "worktree",
            "add",
            "-b",
            "source-audit",
            workspace.to_str().unwrap(),
        ],
    );
    let workspace = workspace.canonicalize().unwrap();
    for file in &mut files {
        file["path"] = json!(workspace.join(file["relative"].as_str().unwrap()));
    }
    *f.probe.scenario.lock().unwrap() =
        json!({"files":files,"file_index":0,"offset":0,"assembled":["","",""]});
    *f.probe.controlled_response.lock().unwrap() = Some(response);
    let create=command(&f.client,&f.base,"source-create",json!([
        {"op":"create","temp_id":"w","kind":"work","parent":null,"depends_on":[],"contract":{
            "title":"Static large-source audit","objective":"Record a plan, read all three prepared source files with exact line/hash evidence, then deliver a static handoff.",
            "constraints":["Read/Task only","No commands, test runs, writes or browser observations"],"acceptance":["Three exact complete source reads with provenance and a truthful Task plan"],
            "user_acceptance_required":true,"allowed_tools":["Task","Read"]}},
        {"op":"ready","work_id":"w"}])).await;
    let created = post(&f.client, &f.base, "/tickets/update", &create).await;
    let work = created["ids"]["w"].as_str().unwrap();
    let start=command(&f.client,&f.base,"source-start",json!([{"op":"start","work_id":work,"temp_id":"a","workspace":{
        "repo":repo.canonicalize().unwrap(),"base_commit":base,"branch":"source-audit","worktree":workspace,"write_roots":[workspace],"claims":[format!("worktree:{}",workspace.display())]
    }}])).await;
    let started = post(&f.client, &f.base, "/tickets/dispatch", &start).await;
    assert_eq!(started["errors"], json!([]), "{started}");
    eprintln!("Native paged audit dispatched: {}", started["runtime"]);
    let inspect = json!({"ids":[work],"depth":0,"sections":["assignments","submissions"],"budget_bytes":65536,"fixed_commit":null});
    let view = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let view = post(&f.client, &f.base, "/tickets/inspect", &inspect).await;
            if view["data"][0]["ticket"]["state"] == "submitted" {
                break view;
            }
            assert_ne!(
                view["data"][0]["assignments"][0]["state"], "failed",
                "{view}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    let state = f.probe.scenario.lock().unwrap().clone();
    assert_eq!(state["file_index"], 3, "all pages reached verified EOF");
    assert!(f.probe.input_checks.load(Ordering::SeqCst) > 20);
    let row = &view["data"][0];
    assert_eq!(row["assignments"][0]["process_stopped"], true);
    let steps = row["assignments"][0]["plan"]["steps"].as_array().unwrap();
    assert_eq!(steps.len(), 3);
    assert!(
        steps.iter().all(|s| s["status"] == "completed"),
        "{steps:?}"
    );
    assert!(row["ticket"]["accepted_submission"].is_null());
    let artifact = &row["submissions"][0]["artifacts"][0];
    let output = f
        .client
        .get(format!(
            "{}/tickets/artifacts/{}",
            f.base,
            artifact["sha256"].as_str().unwrap()
        ))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(output.contains("PAGED_SOURCE_DONE") && output.contains("Static source reads only"));
    for file in state["files"].as_array().unwrap() {
        assert!(output.contains(file["sha256"].as_str().unwrap()));
        assert_eq!(
            content_hash(
                &std::fs::read(workspace.join(file["relative"].as_str().unwrap())).unwrap()
            ),
            file["sha256"]
        );
    }
    let calls = f.probe.calls.load(Ordering::SeqCst);
    f.restart().await;
    let recovered = post(&f.client, &f.base, "/tickets/inspect", &inspect).await;
    assert_eq!(
        recovered["data"][0]["ticket"]["current_submission"],
        row["ticket"]["current_submission"]
    );
    assert_eq!(recovered["data"][0]["ticket"]["state"], "submitted");
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), calls);
    f.finish().await;
}
