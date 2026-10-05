//! Opt-in process fixture for the current built Lotus and actual Bamboo Host.
//! It uses real Native Workers/Runtime with a controlled provider, not semantic
//! model evaluation. Only isolated temporary records/static artifacts are used.
#![cfg(unix)]
#[path = "support/ticket_runtime.rs"]
mod fixture;
use fixture::{command, get, post, Fixture};
use serde_json::{json, Value};
use std::{collections::BTreeMap, time::Duration};

#[actix_web::test]
#[ignore = "start with explicit static-dir/info-path and run the Lotus Ticket browser test"]
async fn current_lotus_five_native_questions_browser_fixture() {
    let info = std::path::PathBuf::from(
        std::env::var("BAMBOO_TICKET_FIXTURE_INFO").expect("explicit fixture info path"),
    );
    let done = info.with_extension("done");
    let _ = std::fs::remove_file(&done);
    let mut f = Fixture::new().await;
    let mut ops = Vec::new();
    for letter in ["A", "B", "C", "D", "E"] {
        ops.push(json!({"op":"create","temp_id":letter,"kind":"work","parent":null,"depends_on":[],"contract":{"title":letter,"objective":format!("TICKET_QUESTION_E2E:{letter}"),"constraints":["Own plan and exact answer only"],"acceptance":["独立结果证据"],"user_acceptance_required":true,"allowed_tools":["Task"]}}));
        ops.push(json!({"op":"ready","work_id":letter}));
    }
    let setup = command(&f.client, &f.base, "ui-create-five", json!(ops)).await;
    let created = post(&f.client, &f.base, "/tickets/update", &setup).await;
    let works: BTreeMap<String, String> = ["A", "B", "C", "D", "E"]
        .into_iter()
        .map(|l| (l.into(), created["ids"][l].as_str().unwrap().into()))
        .collect();
    let ids: Vec<_> = works.values().cloned().collect();
    let start = command(&f.client,&f.base,"ui-start-five",json!(works.iter().map(|(l,w)|json!({"op":"start","work_id":w,"temp_id":format!("assignment-{l}"),"workspace":null})).collect::<Vec<_>>())).await;
    post(&f.client, &f.base, "/tickets/dispatch", &start).await;
    tokio::time::timeout(Duration::from_secs(50),async {
        loop {
            let views = post(&f.client,&f.base,"/tickets/inspect",&json!({"ids":ids,"depth":0,"sections":["assignments","requests"],"budget_bytes":65536})).await;
            if views["data"].as_array().unwrap().iter().all(|r|r["requests"].as_array().unwrap().len()==1 && r["assignments"][0]["process_stopped"]==true) { break; }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }).await.expect("five real Native question-yield/stop facts");
    f.restart().await;
    let mut approvals = BTreeMap::new();
    for label in ["批准A", "批准B"] {
        let setup=command(&f.client,&f.base,&format!("ui-create-{label}"),json!([
            {"op":"create","temp_id":"w","kind":"work","parent":null,"depends_on":[],"contract":{"title":label,"objective":"Synthetic approval only; no external action tools","constraints":[],"acceptance":["明确验证"],"user_acceptance_required":true,"allowed_tools":["Task"]}},
            {"op":"ready","work_id":"w"},
            {"op":"ask","work_id":"w","temp_id":"q","prompt":format!("{label} 的独立动作？"),"action":{"kind":"payment","target":label,"data_hash":"a".repeat(64),"amount":"100 CNY","permissions":[],"risk":"synthetic only; never executed"}}
        ])).await;
        let receipt = post(&f.client, &f.base, "/tickets/update", &setup).await;
        approvals.insert(
            label,
            json!({"work_id":receipt["ids"]["w"],"request_id":receipt["ids"]["q"]}),
        );
    }
    std::fs::write(&info,serde_json::to_vec_pretty(&json!({"origin":f.base.trim_end_matches("/api/v1"),"api":f.base,"session_id":"bamboo-default-supervisor","works":works,"approvals":approvals,"host_data":f.data,"done":done})).unwrap()).unwrap();
    eprintln!("TICKET_BROWSER_READY {}", info.display());
    tokio::time::timeout(Duration::from_secs(360), async {
        while !done.exists() {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("browser acceptance must finish within the bounded fixture deadline");
    let results: Value = serde_json::from_slice(&std::fs::read(&done).unwrap()).unwrap();
    assert_eq!(
        results["pass"],
        true,
        "browser reported failure; preserve {}",
        f.data.display()
    );
    let final_views=post(&f.client,&f.base,"/tickets/inspect",&json!({"ids":ids,"depth":0,"sections":["requests","submissions","assignments"],"budget_bytes":65536})).await;
    for row in final_views["data"].as_array().unwrap() {
        assert_eq!(row["ticket"]["state"], "submitted");
        assert_eq!(row["ticket"]["generation"], 2);
        assert_eq!(
            row["requests"][0]["answer"],
            format!(
                "答案 {}",
                row["ticket"]["contract"]["title"].as_str().unwrap()
            )
        );
        assert_eq!(row["submissions"].as_array().unwrap().len(), 1);
    }
    std::fs::write(
        info.with_extension("runtime-results.json"),
        serde_json::to_vec_pretty(&final_views).unwrap(),
    )
    .unwrap();
    assert_eq!(
        get(&f.client, &f.base, "/tickets/overview").await["data"]["needs_acceptance"],
        5
    );
    f.finish().await;
}
