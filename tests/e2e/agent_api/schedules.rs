// Test env-lock is a std Mutex intentionally held across .await to serialize env access.
#![allow(clippy::await_holding_lock)]

use actix_web::{test, web, App};
use bamboo_agent::agent::Message;
use bamboo_agent::server::handlers::agent;
use bamboo_agent::server::schedule_app::{ScheduleRunConfig, ScheduleRunStatus, ScheduleTrigger};
use serde_json::Value;

#[actix_web::test]
async fn test_schedule_sessions_endpoint_returns_sessions_for_schedule() {
    let _lock = crate::e2e::common::data_dir_lock();
    let state = crate::e2e::common::create_test_app().await;

    let created = state
        .schedule_store
        .create_schedule(
            "daily".to_string(),
            ScheduleTrigger::Interval {
                every_seconds: 60,
                anchor_at: None,
            },
            true,
            ScheduleRunConfig::default(),
        )
        .await
        .expect("create schedule");

    let mut session = bamboo_agent::agent::Session::new("sched-session-1", "test-model");
    session
        .metadata
        .insert("created_by_schedule_id".to_string(), created.id.clone());
    session.add_message(Message::user("hello".to_string()));
    state
        .storage
        .save_session(&session)
        .await
        .expect("save scheduled session");

    let app = test::init_service(App::new().app_data(state.clone()).route(
        "/api/v1/schedules/{schedule_id}/sessions",
        web::get().to(agent::schedules::list_sessions_for_schedule),
    ))
    .await;

    let uri = format!("/api/v1/schedules/{}/sessions", created.id);
    let req = test::TestRequest::get().uri(&uri).to_request();
    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());
    let body = test::read_body(resp).await;
    let payload: Value = serde_json::from_slice(&body).expect("parse sessions response");
    assert_eq!(payload["schedule_id"], created.id);
    assert_eq!(payload["sessions"].as_array().map(|v| v.len()), Some(1));
    assert_eq!(payload["sessions"][0]["id"], "sched-session-1");
}

#[actix_web::test]
async fn test_schedule_runs_endpoint_returns_recent_run_history() {
    let _lock = crate::e2e::common::data_dir_lock();
    let state = crate::e2e::common::create_test_app().await;

    let created = state
        .schedule_store
        .create_schedule(
            "daily".to_string(),
            ScheduleTrigger::Interval {
                every_seconds: 60,
                anchor_at: None,
            },
            true,
            ScheduleRunConfig::default(),
        )
        .await
        .expect("create schedule");

    let claimed = state
        .schedule_store
        .create_run_now(&created.id)
        .await
        .expect("create run now")
        .expect("claimed run");
    state
        .schedule_store
        .mark_run_started(&created.id, &claimed.run_id)
        .await
        .expect("mark started");
    state
        .schedule_store
        .bind_run_session(&created.id, &claimed.run_id, "sched-session-1")
        .await
        .expect("bind session");
    state
        .schedule_store
        .mark_run_terminal(
            &created.id,
            &claimed.run_id,
            ScheduleRunStatus::Success,
            Some("ok".to_string()),
        )
        .await
        .expect("mark terminal");

    let app = test::init_service(App::new().app_data(state.clone()).route(
        "/api/v1/schedules/{schedule_id}/runs",
        web::get().to(agent::schedules::list_runs_for_schedule),
    ))
    .await;

    let uri = format!("/api/v1/schedules/{}/runs", created.id);
    let req = test::TestRequest::get().uri(&uri).to_request();
    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());
    let body = test::read_body(resp).await;
    let payload: Value = serde_json::from_slice(&body).expect("parse runs response");
    assert_eq!(payload["schedule_id"], created.id);
    let runs = payload["runs"].as_array().expect("runs array");
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0]["run_id"], claimed.run_id);
    assert_eq!(runs[0]["status"], "success");
    assert_eq!(runs[0]["session_id"], "sched-session-1");
    assert_eq!(runs[0]["outcome_reason"], "ok");
}

#[actix_web::test]
async fn test_schedule_runs_endpoint_returns_empty_runs_for_new_schedule() {
    let _lock = crate::e2e::common::data_dir_lock();
    let state = crate::e2e::common::create_test_app().await;

    let created = state
        .schedule_store
        .create_schedule(
            "empty-history".to_string(),
            ScheduleTrigger::Interval {
                every_seconds: 300,
                anchor_at: None,
            },
            true,
            ScheduleRunConfig::default(),
        )
        .await
        .expect("create schedule");

    let app = test::init_service(App::new().app_data(state.clone()).route(
        "/api/v1/schedules/{schedule_id}/runs",
        web::get().to(agent::schedules::list_runs_for_schedule),
    ))
    .await;

    let uri = format!("/api/v1/schedules/{}/runs", created.id);
    let req = test::TestRequest::get().uri(&uri).to_request();
    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());
    let body = test::read_body(resp).await;
    let payload: Value = serde_json::from_slice(&body).expect("parse empty runs response");
    assert_eq!(payload["schedule_id"], created.id);
    assert_eq!(payload["runs"].as_array().map(|v| v.len()), Some(0));
}
