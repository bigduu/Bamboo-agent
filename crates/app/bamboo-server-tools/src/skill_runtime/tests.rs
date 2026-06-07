use super::{LoadSkillTool, ReadSkillResourceTool};
use bamboo_skills::access_control::{parse_loaded_skill_ids, serialize_loaded_skill_ids};
use bamboo_skills::runtime_metadata::{
    LAST_LOADED_SKILL_SUMMARY_METADATA_KEY, LAST_RESOURCE_READ_SUMMARY_METADATA_KEY,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::RwLock;

use bamboo_skills::{SkillManager, SkillStoreConfig};
use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{Tool, ToolExecutionContext};
use bamboo_agent_core::Session;
use bamboo_llm::Config;

#[test]
fn parse_loaded_skill_ids_supports_json_and_csv() {
    let from_json = parse_loaded_skill_ids(r#"["skill-b","skill-a","skill-a"]"#);
    assert_eq!(from_json.len(), 2);
    assert!(from_json.contains("skill-a"));
    assert!(from_json.contains("skill-b"));

    let from_csv = parse_loaded_skill_ids("skill-c, skill-d , skill-c");
    assert_eq!(from_csv.len(), 2);
    assert!(from_csv.contains("skill-c"));
    assert!(from_csv.contains("skill-d"));
}

#[test]
fn serialize_loaded_skill_ids_is_stable_and_sorted() {
    let mut ids = HashSet::new();
    ids.insert("skill-b".to_string());
    ids.insert("skill-a".to_string());

    assert_eq!(serialize_loaded_skill_ids(&ids), r#"["skill-a","skill-b"]"#);
}

/// Build a per-session-locked session cache pre-populated with one session.
fn test_session_cache(session_id: &str, session: &Session) -> bamboo_engine::SessionCache {
    let cache = Arc::new(dashmap::DashMap::new());
    cache.insert(
        session_id.to_string(),
        Arc::new(parking_lot::RwLock::new(session.clone())),
    );
    cache
}

#[derive(Default)]
struct TestStorage {
    sessions: RwLock<HashMap<String, Session>>,
}

#[async_trait::async_trait]
impl Storage for TestStorage {
    async fn save_session(&self, session: &Session) -> std::io::Result<()> {
        self.sessions
            .write()
            .await
            .insert(session.id.clone(), session.clone());
        Ok(())
    }

    async fn load_session(&self, session_id: &str) -> std::io::Result<Option<Session>> {
        Ok(self.sessions.read().await.get(session_id).cloned())
    }

    async fn delete_session(&self, session_id: &str) -> std::io::Result<bool> {
        Ok(self.sessions.write().await.remove(session_id).is_some())
    }
}

#[tokio::test]
async fn load_skill_rejects_globally_disabled_skill() {
    let temp_dir = tempfile::tempdir().expect("tempdir should be created");
    let skill_dir = temp_dir.path().join("skills").join("demo-skill");
    std::fs::create_dir_all(&skill_dir).expect("skill dir should exist");
    std::fs::write(
        skill_dir.join("SKILL.md"),
        r#"---
name: demo-skill
description: Demo description
---
Use this demo skill."#,
    )
    .expect("skill file should be written");

    let skill_manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
        skills_dir: temp_dir.path().join("skills"),
        project_dir: None,
        active_mode: None,
    }));
    skill_manager
        .initialize()
        .await
        .expect("skill manager should initialize");

    let config = Arc::new(RwLock::new(Config::default()));
    {
        let mut cfg = config.write().await;
        cfg.skills.disabled = vec!["demo-skill".to_string()];
        cfg.normalize_skill_settings();
    }

    let session_id = "session-1";
    let session = Session::new(session_id, "model");
    let sessions = test_session_cache(session_id, &session);
    let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
    storage
        .save_session(&session)
        .await
        .expect("session should be saved");

    let persistence = Arc::new(bamboo_storage::LockedSessionStore::new(
        storage.clone(),
    ));

    let tool = LoadSkillTool::new(
        skill_manager,
        config,
        bamboo_engine::SessionRepository::new(sessions, storage, persistence),
    );
    let ctx = ToolExecutionContext {
        session_id: Some(session_id),
        tool_call_id: "tool-call-1",
        event_tx: None,
        available_tool_schemas: None,
    };

    let error = tool
        .execute_with_context(serde_json::json!({ "skill_id": "demo-skill" }), ctx)
        .await
        .expect_err("disabled skill should be rejected");

    assert!(error
        .to_string()
        .contains("globally disabled in Bamboo settings"));
}

#[tokio::test]
async fn load_skill_persists_last_loaded_skill_summary() {
    let temp_dir = tempfile::tempdir().expect("tempdir should be created");
    let skill_dir = temp_dir.path().join("skills").join("demo-skill");
    std::fs::create_dir_all(&skill_dir).expect("skill dir should exist");
    std::fs::write(
        skill_dir.join("SKILL.md"),
        r#"---
name: demo-skill
description: Demo description
---
Use this demo skill."#,
    )
    .expect("skill file should be written");

    let skill_manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
        skills_dir: temp_dir.path().join("skills"),
        project_dir: None,
        active_mode: None,
    }));
    skill_manager
        .initialize()
        .await
        .expect("skill manager should initialize");

    let config = Arc::new(RwLock::new(Config::default()));
    let session_id = "session-2";
    let session = Session::new(session_id, "model");
    let sessions = test_session_cache(session_id, &session);
    let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
    storage
        .save_session(&session)
        .await
        .expect("session should be saved");
    let persistence = Arc::new(bamboo_storage::LockedSessionStore::new(
        storage.clone(),
    ));

    let tool = LoadSkillTool::new(
        skill_manager,
        config,
        bamboo_engine::SessionRepository::new(sessions.clone(), storage.clone(), persistence.clone()),
    );
    let ctx = ToolExecutionContext {
        session_id: Some(session_id),
        tool_call_id: "tool-call-2",
        event_tx: None,
        available_tool_schemas: None,
    };

    let _ = tool
        .execute_with_context(serde_json::json!({ "skill_id": "demo-skill" }), ctx)
        .await
        .expect("load_skill should succeed");

    let saved = storage
        .load_session(session_id)
        .await
        .expect("load session should succeed")
        .expect("session should exist");
    let summary = saved
        .metadata
        .get(LAST_LOADED_SKILL_SUMMARY_METADATA_KEY)
        .expect("last loaded skill summary should be present");
    assert!(summary.contains("demo-skill"));
}

#[tokio::test]
async fn read_skill_resource_persists_last_resource_read_summary() {
    let temp_dir = tempfile::tempdir().expect("tempdir should be created");
    let skill_dir = temp_dir.path().join("skills").join("demo-skill");
    let refs_dir = skill_dir.join("references");
    std::fs::create_dir_all(&refs_dir).expect("references dir should exist");
    std::fs::write(
        skill_dir.join("SKILL.md"),
        r#"---
name: demo-skill
description: Demo description
---
Use this demo skill."#,
    )
    .expect("skill file should be written");
    std::fs::write(refs_dir.join("policy.md"), "line1\nline2\nline3\n")
        .expect("resource file should be written");

    let skill_manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
        skills_dir: temp_dir.path().join("skills"),
        project_dir: None,
        active_mode: None,
    }));
    skill_manager
        .initialize()
        .await
        .expect("skill manager should initialize");

    let config = Arc::new(RwLock::new(Config::default()));
    let session_id = "session-3";
    let session = Session::new(session_id, "model");
    let sessions = test_session_cache(session_id, &session);
    let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
    storage
        .save_session(&session)
        .await
        .expect("session should be saved");
    let persistence = Arc::new(bamboo_storage::LockedSessionStore::new(
        storage.clone(),
    ));

    let session_repo =
        bamboo_engine::SessionRepository::new(sessions, storage.clone(), persistence);
    let load_tool = LoadSkillTool::new(skill_manager.clone(), config.clone(), session_repo.clone());
    let read_tool = ReadSkillResourceTool::new(skill_manager, config, session_repo);

    let load_ctx = ToolExecutionContext {
        session_id: Some(session_id),
        tool_call_id: "tool-call-load",
        event_tx: None,
        available_tool_schemas: None,
    };
    let read_ctx = ToolExecutionContext {
        session_id: Some(session_id),
        tool_call_id: "tool-call-read",
        event_tx: None,
        available_tool_schemas: None,
    };

    let _ = load_tool
        .execute_with_context(serde_json::json!({ "skill_id": "demo-skill" }), load_ctx)
        .await
        .expect("load_skill should succeed");

    let _ = read_tool
        .execute_with_context(
            serde_json::json!({
                "skill_id": "demo-skill",
                "resource_path": "references/policy.md",
                "offset": 1,
                "limit": 1
            }),
            read_ctx,
        )
        .await
        .expect("read_skill_resource should succeed");

    let saved = storage
        .load_session(session_id)
        .await
        .expect("load session should succeed")
        .expect("session should exist");
    let summary = saved
        .metadata
        .get(LAST_RESOURCE_READ_SUMMARY_METADATA_KEY)
        .expect("last resource read summary should be present");
    assert!(summary.contains("demo-skill"));
    assert!(summary.contains("references/policy.md"));
    assert!(summary.contains("\"offset\":1"));
}
