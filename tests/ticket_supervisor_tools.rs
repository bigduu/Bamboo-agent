//! Real Host model-tool dispatch using a controlled provider, not semantics.
#![cfg(unix)]
#[path = "support/ticket_runtime.rs"]
mod fixture;
use bamboo_domain::{Storage, DEFAULT_SUPERVISOR_SESSION_ID};
use bamboo_storage::SessionStoreV2;
use fixture::{post, Fixture};
use serde_json::json;
use std::{sync::atomic::Ordering, time::Duration};

#[actix_web::test]
async fn actual_supervisor_work_tools_return_before_held_native_worker_finishes() {
    let f = Fixture::new().await;
    post(&f.client, &f.base, "/chat", &json!({"session_id":DEFAULT_SUPERVISOR_SESSION_ID,
        "message":"TICKET_SUPERVISOR_E2E create and start one Work then reply without waiting for the Worker.", "model":"ticket-model","provider":"openai",
        "model_ref":{"provider":"openai","model":"ticket-model"}})).await;
    let executed = post(
        &f.client,
        &f.base,
        &format!("/execute/{DEFAULT_SUPERVISOR_SESSION_ID}"),
        &json!({}),
    )
    .await;
    assert_eq!(executed["status"], "started");
    let store = SessionStoreV2::new(f.data.clone()).await.unwrap();
    let root = tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            let root = store
                .load_session(DEFAULT_SUPERVISOR_SESSION_ID)
                .await
                .unwrap()
                .unwrap();
            assert_ne!(
                root.last_run_status().as_deref(),
                Some("error"),
                "{:?}; {}",
                root.last_run_error(),
                std::fs::read_to_string(f.data.join("host.log")).unwrap()
            );
            if root
                .messages
                .iter()
                .any(|m| m.content == "TICKET_SUPERVISOR_DISPATCHED")
                && f.probe.held.load(Ordering::SeqCst) == 1
                && root.last_run_status().as_deref() == Some("completed")
            {
                break root;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "Supervisor did not return while Worker was held: {}",
            std::fs::read_to_string(f.data.join("host.log")).unwrap()
        )
    });
    assert_eq!(f.probe.root_calls.load(Ordering::SeqCst), 3);
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 2);
    assert!(
        root.task_list.is_none(),
        "Work contracts must not double-write the old Root TaskList"
    );
    let overview: serde_json::Value = f
        .client
        .get(format!("{}/tickets/overview", f.base))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(overview["data"]["work_count"], 1);
    assert_eq!(
        overview["data"]["needs_acceptance"], 0,
        "held Worker has not submitted"
    );
    post(&f.client, &f.base, "/chat", &json!({"session_id":DEFAULT_SUPERVISOR_SESSION_ID,
        "message":"TICKET_SUPERVISOR_E2E keep working on my next message while that Worker is held.",
        "model":"ticket-model","provider":"openai"})).await;
    let next = post(
        &f.client,
        &f.base,
        &format!("/execute/{DEFAULT_SUPERVISOR_SESSION_ID}"),
        &json!({}),
    )
    .await;
    assert_eq!(next["status"], "started");
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let current = store
                .load_session(DEFAULT_SUPERVISOR_SESSION_ID)
                .await
                .unwrap()
                .unwrap();
            if current.last_run_status().as_deref() == Some("completed")
                && current
                    .messages
                    .iter()
                    .filter(|m| m.content == "TICKET_SUPERVISOR_DISPATCHED")
                    .count()
                    == 2
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("next Supervisor round completes while Worker remains active");
    assert_eq!(f.probe.root_calls.load(Ordering::SeqCst), 6);
    assert_eq!(f.probe.held.load(Ordering::SeqCst), 1);
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 2);
    f.probe.release.notify_waiters();
    f.finish().await;
}
