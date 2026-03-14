use super::*;

#[actix_web::test]
async fn test_restore_truncates_to_target_message() {
    let state = crate::e2e::common::create_test_app().await;

    let session_id = "session-restore-1".to_string();
    let mut session = Session::new(session_id.clone(), "test-model");
    session.add_message(Message::system("sys"));
    let user = Message::user("u1");
    let target_message_id = user.id.clone();
    session.add_message(user);
    session.add_message(Message::assistant("a1", None));

    state
        .storage
        .save_session(&session)
        .await
        .expect("save session");

    let app = test::init_service(App::new().app_data(state.clone()).route(
        "/api/v1/sessions/{session_id}/restore",
        web::post().to(messages::restore_session_state),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri(&format!("/api/v1/sessions/{}/restore", session_id))
        .set_json(json!({
            "target_message_id": target_message_id,
            "restore_files": false
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(body["success"], true);
    assert_eq!(body["session_id"], session_id);
    assert_eq!(body["messages_removed"], 1);
    assert_eq!(body["message_count"], 2);
    assert_eq!(body["restored_files"], 0);
    assert_eq!(body["deleted_files"], 0);

    let loaded = state
        .storage
        .load_session(&session_id)
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(loaded.messages.len(), 2);
    assert_eq!(loaded.messages.last().unwrap().content, "u1");
}

#[actix_web::test]
async fn test_restore_replays_tool_checkpoint_when_restore_files_enabled() {
    let state = crate::e2e::common::create_test_app().await;
    let files_dir = tempfile::tempdir().expect("temp dir");

    let target_file = files_dir.path().join("notes.txt");
    let checkpoint_file = files_dir.path().join("notes.checkpoint");
    std::fs::write(&checkpoint_file, "original content").expect("write checkpoint");
    std::fs::write(&target_file, "changed content").expect("write changed file");

    let session_id = "session-restore-2".to_string();
    let mut session = Session::new(session_id.clone(), "test-model");
    session.add_message(Message::system("sys"));
    let user = Message::user("u1");
    let target_message_id = user.id.clone();
    session.add_message(user);
    session.add_message(Message::tool_result(
        "tool-1",
        json!({
            "file_path": target_file.to_string_lossy(),
            "checkpoint": {
                "created": true,
                "path": checkpoint_file.to_string_lossy(),
            }
        })
        .to_string(),
    ));
    session.add_message(Message::assistant("a1", None));

    state
        .storage
        .save_session(&session)
        .await
        .expect("save session");

    let app = test::init_service(App::new().app_data(state.clone()).route(
        "/api/v1/sessions/{session_id}/restore",
        web::post().to(messages::restore_session_state),
    ))
    .await;

    let req = test::TestRequest::post()
        .uri(&format!("/api/v1/sessions/{}/restore", session_id))
        .set_json(json!({
            "target_message_id": target_message_id,
            "restore_files": true
        }))
        .to_request();

    let resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());

    let body: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(body["success"], true);
    assert_eq!(body["messages_removed"], 2);
    assert_eq!(body["message_count"], 2);
    assert_eq!(body["restored_files"], 1);
    assert_eq!(body["deleted_files"], 0);
    assert_eq!(body["file_errors"], json!([]));

    let restored_content = std::fs::read_to_string(&target_file).expect("read restored file");
    assert_eq!(restored_content, "original content");

    let loaded = state
        .storage
        .load_session(&session_id)
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(loaded.messages.len(), 2);
    assert_eq!(loaded.messages.last().unwrap().content, "u1");
}
