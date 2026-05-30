//! Regression e2e for the "user message disappears on existing sessions" bug.
//!
//! Root cause: `PATCH /sessions/{id}` (model / reasoning_effort) used to load a
//! whole session snapshot and full-save it via `merge_save_runtime`, which
//! overwrites the `messages` array. When that patch raced `POST /chat` (which
//! had just appended a user message), the patch reverted the append — the user
//! message vanished and execute looped on `MessageCountMismatch` forever.
//!
//! The fix routes config patches through `update_runtime_config`, which loads
//! the freshest session under the per-session lock and never rewrites messages.
//!
//! These tests use isolated per-test storage (no shared `data_dir_lock`) so a
//! failure in one cannot cascade into the other via a poisoned mutex.

use actix_web::{test, web, App};
use bamboo_agent::server::handlers;
use bamboo_agent::server::handlers::agent::sessions;
use bamboo_agent_core::Session;
use bamboo_domain::reasoning::ReasoningEffort;
use bamboo_domain::session::types::Message;
use serde_json::json;

/// A config-only PATCH over a session that already has conversation history
/// must preserve every message while applying the model / reasoning change.
#[actix_web::test]
async fn test_patch_config_preserves_existing_messages() {
    let state = super::sessions_test_app().await;
    let session_id = uuid::Uuid::new_v4().to_string();

    // Seed an existing session with conversation history (user + assistant + user).
    let mut seeded = Session::new(session_id.clone(), "seed-model".to_string());
    seeded.add_message(Message::user("first question"));
    seeded.add_message(Message::assistant("first answer", None));
    seeded.add_message(Message::user("second question"));
    state
        .storage
        .save_session(&seeded)
        .await
        .expect("seed session save");

    let app = test::init_service(App::new().app_data(state.clone()).route(
        "/api/v1/sessions/{session_id}",
        web::patch().to(sessions::patch_session),
    ))
    .await;

    let patch_req = test::TestRequest::patch()
        .uri(&format!("/api/v1/sessions/{session_id}"))
        .set_json(json!({
            "model": "patched-model",
            "reasoning_effort": "max"
        }))
        .to_request();
    let patch_resp = test::call_service(&app, patch_req).await;
    assert!(patch_resp.status().is_success(), "patch should succeed");

    let stored = state
        .storage
        .load_session(&session_id)
        .await
        .expect("load")
        .expect("exists");
    assert_eq!(
        stored.messages.len(),
        3,
        "config patch must NOT drop conversation messages"
    );
    assert_eq!(stored.model, "patched-model");
    assert_eq!(stored.reasoning_effort, Some(ReasoningEffort::Max));
}

/// End-to-end through the real `/chat` and `PATCH /sessions` handlers: a config
/// patch that lands after chat appended a user message must not revert it.
///
/// Asserted relatively (the patch must not shrink the message set, and the
/// appended user message must survive) so the test does not depend on `/chat`'s
/// internal message bookkeeping.
#[actix_web::test]
async fn test_patch_after_chat_append_keeps_user_message() {
    let state = super::sessions_test_app().await;
    let session_id = uuid::Uuid::new_v4().to_string();

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route("/api/v1/chat", web::post().to(handlers::chat::handler))
            .route(
                "/api/v1/sessions/{session_id}",
                web::patch().to(sessions::patch_session),
            ),
    )
    .await;

    // Build history through the real chat handler (chat only persists; no LLM).
    for message in ["first message", "the brand new question"] {
        let chat_req = test::TestRequest::post()
            .uri("/api/v1/chat")
            .set_json(json!({
                "message": message,
                "session_id": session_id,
                "model": "seed-model"
            }))
            .to_request();
        let chat_resp = test::call_service(&app, chat_req).await;
        assert!(
            chat_resp.status().is_success(),
            "chat should persist the user message, got {}",
            chat_resp.status()
        );
    }

    let after_chat = state
        .storage
        .load_session(&session_id)
        .await
        .expect("load")
        .expect("exists");
    let baseline = after_chat.messages.len();
    assert!(
        baseline >= 2,
        "chat should have appended both user messages (got {baseline})"
    );
    let appended_present = |session: &Session| {
        session
            .messages
            .iter()
            .any(|m| m.content.contains("the brand new question"))
    };
    assert!(
        appended_present(&after_chat),
        "the appended user message must exist after chat"
    );

    // A config patch lands afterwards — the clobbering scenario.
    let patch_req = test::TestRequest::patch()
        .uri(&format!("/api/v1/sessions/{session_id}"))
        .set_json(json!({
            "model": "patched-model",
            "reasoning_effort": "high"
        }))
        .to_request();
    let patch_resp = test::call_service(&app, patch_req).await;
    assert!(patch_resp.status().is_success(), "patch should succeed");

    let stored = state
        .storage
        .load_session(&session_id)
        .await
        .expect("load")
        .expect("exists");
    assert_eq!(
        stored.messages.len(),
        baseline,
        "config patch must not shrink the message set (regression)"
    );
    assert!(
        appended_present(&stored),
        "config patch must preserve the chat-appended user message"
    );
    assert_eq!(stored.model, "patched-model");
    assert_eq!(stored.reasoning_effort, Some(ReasoningEffort::High));
}
