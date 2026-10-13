use std::net::SocketAddr;

use actix_web::{http::StatusCode, test, web, App, HttpMessage};
use bamboo_domain::{Session, SessionAuthorityIdentity, DEFAULT_SUPERVISOR_SESSION_ID};
use serde_json::Value;
use tempfile::tempdir;

use crate::{app_state::AppState, routes::configure_routes};

fn open_request() -> test::TestRequest {
    test::TestRequest::post()
        .uri("/api/v1/supervisor/default")
        .peer_addr("127.0.0.1:12345".parse::<SocketAddr>().unwrap())
}

#[actix_web::test]
async fn default_supervisor_opens_once_without_tickets_and_survives_restart_with_history() {
    let directory = tempdir().unwrap();
    let state = web::Data::new(AppState::new(directory.path().into()).await.unwrap());
    assert!(!state.config.read().await.features.ticket_mutation);
    assert!(!state.config.read().await.features.ticket_dispatch);
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(configure_routes),
    )
    .await;

    let before: Value = test::call_and_read_body_json(
        &app,
        test::TestRequest::get()
            .uri("/api/v1/sessions")
            .to_request(),
    )
    .await;
    assert!(before["sessions"].as_array().unwrap().is_empty());

    let mut receipts = Vec::new();
    for response in futures::future::join_all(
        (0..4).map(|_| test::call_service(&app, open_request().to_request())),
    )
    .await
    {
        assert_eq!(response.status(), StatusCode::OK);
        receipts.push(test::read_body_json::<Value, _>(response).await);
    }
    assert_eq!(receipts.iter().filter(|r| r["created"] == true).count(), 1);
    let incarnation = receipts[0]["incarnation_id"].clone();
    for receipt in &receipts {
        assert_eq!(receipt["session_id"], DEFAULT_SUPERVISOR_SESSION_ID);
        assert_eq!(receipt["incarnation_id"], incarnation);
    }
    let authority = state
        .storage
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        authority.authority_identity,
        SessionAuthorityIdentity::Supervisor { .. }
    ));
    assert!(authority.parent_session_id.is_none());
    assert!(authority.messages.is_empty());
    let birth = authority.created_at;
    let initial_model = authority.model.clone();
    assert!(
        initial_model.is_empty(),
        "fresh directory has no configured model"
    );

    // Opening an existing empty authority must not depend on today's provider.
    state.config.write().await.provider = "unconfigured-provider".into();
    let reopened: Value = test::call_and_read_body_json(&app, open_request().to_request()).await;
    assert_eq!(reopened["created"], false);
    assert_eq!(reopened["incarnation_id"], incarnation);

    let list: Value = test::call_and_read_body_json(
        &app,
        test::TestRequest::get()
            .uri("/api/v1/sessions?kind=root")
            .to_request(),
    )
    .await;
    assert_eq!(list["total"], 1);
    assert_eq!(list["sessions"][0]["id"], DEFAULT_SUPERVISOR_SESSION_ID);
    assert_eq!(list["sessions"][0]["message_count"], 0);
    assert!(!state.config.read().await.features.ticket_mutation);
    assert!(!state.config.read().await.features.ticket_dispatch);
    assert!(!directory.path().join("tickets").exists());

    let mut session = state
        .storage
        .load_session(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    session.add_message(bamboo_domain::Message::user("persisted Supervisor history"));
    state.storage.save_session(&session).await.unwrap();
    drop(app);
    drop(state);

    let recovered = web::Data::new(AppState::new(directory.path().into()).await.unwrap());
    let app = test::init_service(
        App::new()
            .app_data(recovered.clone())
            .configure(configure_routes),
    )
    .await;
    let receipt: Value = test::call_and_read_body_json(&app, open_request().to_request()).await;
    assert_eq!(receipt["created"], false);
    assert_eq!(receipt["incarnation_id"], incarnation);
    let history = recovered
        .storage
        .load_session(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(history.created_at, birth);
    assert_eq!(history.model, initial_model);
    assert_eq!(history.messages.len(), 1);
    assert_eq!(history.messages[0].content, "persisted Supervisor history");
    let list: Value = test::call_and_read_body_json(
        &app,
        test::TestRequest::get()
            .uri("/api/v1/sessions?kind=root")
            .to_request(),
    )
    .await;
    assert_eq!(list["total"], 1);
    assert_eq!(list["sessions"][0]["message_count"], 1);
}

#[actix_web::test]
async fn supervisor_open_rejects_ordinary_identity_without_promoting_or_replacing_it() {
    let directory = tempdir().unwrap();
    let state = web::Data::new(AppState::new(directory.path().into()).await.unwrap());
    let ordinary = Session::new(DEFAULT_SUPERVISOR_SESSION_ID, "ordinary-model");
    state.storage.save_session(&ordinary).await.unwrap();
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(configure_routes),
    )
    .await;
    let response = test::call_service(&app, open_request().to_request()).await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let retained = state
        .storage
        .load_session(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.created_at, ordinary.created_at);
    assert!(retained.authority_identity.is_ordinary());
}

#[actix_web::test]
async fn supervisor_open_requires_verified_owner_access() {
    let directory = tempdir().unwrap();
    let state = web::Data::new(AppState::new(directory.path().into()).await.unwrap());
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(configure_routes),
    )
    .await;
    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/supervisor/default")
            .peer_addr("192.0.2.1:12345".parse().unwrap())
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(state
        .storage
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .is_none());

    let worker = open_request().to_http_request();
    worker
        .extensions_mut()
        .insert(crate::codex_run_tokens::CodexRunAuthContext {
            session_id: "worker-session".into(),
        });
    let result = super::open_default(state.clone(), worker).await;
    assert_eq!(
        result.unwrap_err().as_response_error().status_code(),
        StatusCode::FORBIDDEN
    );
    assert!(state
        .storage
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .is_none());
}
