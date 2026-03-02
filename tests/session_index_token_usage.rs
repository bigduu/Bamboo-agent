//! Regression tests for SessionStoreV2 index fields.

use bamboo_agent::agent::core::storage::SessionStoreV2;
use bamboo_agent::agent::core::{Session, Storage, TokenBudgetUsage};

mod common;

#[tokio::test]
async fn session_index_persists_token_usage() {
    common::init_test_env();
    let dir = common::create_temp_dir();

    let store = SessionStoreV2::new(dir.path().to_path_buf())
        .await
        .expect("create SessionStoreV2");

    let mut session = Session::new("session-1", "test-model");
    session.token_usage = Some(TokenBudgetUsage {
        system_tokens: 10,
        summary_tokens: 20,
        window_tokens: 30,
        total_tokens: 60,
        budget_limit: 1000,
        truncation_occurred: true,
        segments_removed: 3,
    });

    store.save_session(&session).await.expect("save session");

    let entry = store
        .get_index_entry("session-1")
        .await
        .expect("index entry should exist");

    let usage = entry.token_usage.expect("token usage should be in index");
    assert_eq!(usage.total_tokens, 60);
    assert_eq!(usage.budget_limit, 1000);
    assert!(usage.truncation_occurred);
    assert_eq!(usage.segments_removed, 3);
}
