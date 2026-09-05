//! Public SDK bootstrap uses canonical storage without invoking an agent run.

use bamboo_sdk::{Agent, SupervisorBootstrapReceipt, DEFAULT_SUPERVISOR_SESSION_ID};

#[tokio::test]
async fn supervisor_facade_reuses_identity_without_replacing_model_or_history() {
    let home = tempfile::tempdir().unwrap();
    std::fs::write(
        home.path().join("config.json"),
        r#"{
        "provider":"anthropic",
        "providers":{"anthropic":{"api_key":"test-key","model":"sdk-model"}}
    }"#,
    )
    .unwrap();
    let agent = Agent::builder()
        .model("sdk-model")
        .with_defaults_for_data_dir(home.path().to_path_buf())
        .await
        .unwrap()
        .build()
        .unwrap();
    let service: bamboo_sdk::agent::SupervisorSessionService = agent.supervisor_sessions();
    let first: SupervisorBootstrapReceipt = service
        .get_or_create_default("initial-model")
        .await
        .unwrap();
    assert!(first.created);
    assert_eq!(first.session_id, DEFAULT_SUPERVISOR_SESSION_ID);
    let mut session = agent
        .storage()
        .load_session(&first.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(session.model, "initial-model");
    assert!(session.project_id_meta().is_none());
    session.add_message(bamboo_sdk::agent::Message::user("preserved history"));
    agent.storage().save_session(&session).await.unwrap();

    let service = bamboo_sdk::SupervisorSessionService::new(agent.storage().clone());
    let again: bamboo_sdk::agent::SupervisorBootstrapReceipt = service
        .get_or_create_default("different-model")
        .await
        .unwrap();
    assert!(!again.created);
    assert_eq!(again.session_id, first.session_id);
    assert_eq!(again.incarnation_id, first.incarnation_id);
    let loaded = agent
        .storage()
        .load_session(&again.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.model, "initial-model");
    assert_eq!(loaded.messages.last().unwrap().content, "preserved history");
    assert_eq!(
        loaded.authority_identity,
        bamboo_sdk::SessionAuthorityIdentity::Supervisor {
            incarnation_id: first.incarnation_id,
        }
    );
    let value = serde_json::to_value(&again).unwrap();
    assert_eq!(value.as_object().unwrap().len(), 3);
    assert!(value.get("messages").is_none());
}
