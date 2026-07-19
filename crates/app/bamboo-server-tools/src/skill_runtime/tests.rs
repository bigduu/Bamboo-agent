use super::{LoadSkillTool, ReadSkillResourceTool};
use bamboo_skills::access_control::{parse_loaded_skill_ids, serialize_loaded_skill_ids};
use bamboo_skills::runtime_metadata::{
    LAST_LOADED_SKILL_SUMMARY_METADATA_KEY, LAST_RESOURCE_READ_SUMMARY_METADATA_KEY,
    SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::RwLock;

use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{Tool, ToolExecutionContext, ToolOutcome};
use bamboo_agent_core::Session;
use bamboo_llm::Config;
use bamboo_skills::{SkillManager, SkillStoreConfig};

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

    let persistence = Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));

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
        bypass_permissions: false,
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
    };

    let error = tool
        .invoke(
            serde_json::json!({ "skill_id": "demo-skill" }),
            ctx.to_tool_ctx(),
        )
        .await
        .expect_err("disabled skill should be rejected");

    assert!(error
        .to_string()
        .contains("globally disabled in Bamboo settings"));
}

#[tokio::test]
async fn load_skill_accepts_only_runtime_advertised_skill_ids() {
    let temp_dir = tempfile::tempdir().expect("tempdir should be created");
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
    let session_id = "session-runtime-allowlist";
    let session = Session::new(session_id, "model");
    let sessions = test_session_cache(session_id, &session);
    let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
    storage
        .save_session(&session)
        .await
        .expect("session should be saved");
    let persistence = Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));
    let repo = bamboo_engine::SessionRepository::new(sessions, storage, persistence);
    let mut automatic_run = session.clone();
    automatic_run.metadata.insert(
        SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
        r#"["review"]"#.to_string(),
    );
    repo.save(&mut automatic_run)
        .await
        .expect("publish automatic runtime selection");
    let tool = LoadSkillTool::new(skill_manager, config, repo.clone());
    let context = ToolExecutionContext {
        session_id: Some(session_id),
        tool_call_id: "tool-call-runtime-allowlist",
        event_tx: None,
        available_tool_schemas: None,
        bypass_permissions: false,
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
    };

    tool.invoke(
        serde_json::json!({ "skill_id": "review" }),
        context.to_tool_ctx(),
    )
    .await
    .expect("advertised review skill should load");

    let error = tool
        .invoke(
            serde_json::json!({ "skill_id": "plan" }),
            context.to_tool_ctx(),
        )
        .await
        .expect_err("manual-only plan must not load in an automatic review session");
    assert!(error.to_string().contains("not selected for this request"));

    let mut explicit_run = repo.load(session_id).await.expect("cached session");
    explicit_run.metadata.insert(
        SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
        r#"["plan"]"#.to_string(),
    );
    repo.save(&mut explicit_run)
        .await
        .expect("publish explicit runtime selection");
    tool.invoke(
        serde_json::json!({ "skill_id": "plan" }),
        context.to_tool_ctx(),
    )
    .await
    .expect("explicitly advertised plan skill should load on the next run");
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
    let mut session = Session::new(session_id, "model");
    session.metadata.insert(
        SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
        r#"["demo-skill"]"#.to_string(),
    );
    let sessions = test_session_cache(session_id, &session);
    let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
    storage
        .save_session(&session)
        .await
        .expect("session should be saved");
    let persistence = Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));

    let tool = LoadSkillTool::new(
        skill_manager,
        config,
        bamboo_engine::SessionRepository::new(
            sessions.clone(),
            storage.clone(),
            persistence.clone(),
        ),
    );
    let ctx = ToolExecutionContext {
        session_id: Some(session_id),
        tool_call_id: "tool-call-2",
        event_tx: None,
        available_tool_schemas: None,
        bypass_permissions: false,
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
    };

    let _ = tool
        .invoke(
            serde_json::json!({ "skill_id": "demo-skill" }),
            ctx.to_tool_ctx(),
        )
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
    let mut session = Session::new(session_id, "model");
    session.metadata.insert(
        SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
        r#"["demo-skill"]"#.to_string(),
    );
    let sessions = test_session_cache(session_id, &session);
    let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
    storage
        .save_session(&session)
        .await
        .expect("session should be saved");
    let persistence = Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));

    let session_repo =
        bamboo_engine::SessionRepository::new(sessions, storage.clone(), persistence);
    let load_tool = LoadSkillTool::new(skill_manager.clone(), config.clone(), session_repo.clone());
    let read_tool = ReadSkillResourceTool::new(skill_manager, config, session_repo);

    let load_ctx = ToolExecutionContext {
        session_id: Some(session_id),
        tool_call_id: "tool-call-load",
        event_tx: None,
        available_tool_schemas: None,
        bypass_permissions: false,
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
    };
    let read_ctx = ToolExecutionContext {
        session_id: Some(session_id),
        tool_call_id: "tool-call-read",
        event_tx: None,
        available_tool_schemas: None,
        bypass_permissions: false,
        can_async_resume: false,
        bash_completion_sink: None,
        pre_parsed_args: None,
    };

    let _ = load_tool
        .invoke(
            serde_json::json!({ "skill_id": "demo-skill" }),
            load_ctx.to_tool_ctx(),
        )
        .await
        .expect("load_skill should succeed");

    let _ = read_tool
        .invoke(
            serde_json::json!({
                "skill_id": "demo-skill",
                "resource_path": "references/policy.md",
                "offset": 1,
                "limit": 1
            }),
            read_ctx.to_tool_ctx(),
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

#[tokio::test]
async fn session_workspace_catalog_selection_and_runtime_roots_are_isolated() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let global_skills = temp_dir.path().join("data/skills");
    let workspace_one = temp_dir.path().join("workspace-one");
    let workspace_two = temp_dir.path().join("workspace-two");

    for (workspace, description, instructions, resource, exclusive) in [
        (
            &workspace_one,
            "alpha needle workflow",
            "Alpha workspace instructions.",
            "alpha resource",
            "only-alpha",
        ),
        (
            &workspace_two,
            "beta needle workflow",
            "Beta workspace instructions.",
            "beta resource",
            "only-beta",
        ),
    ] {
        let shared = workspace.join(".bamboo/skills/shared-workflow");
        std::fs::create_dir_all(shared.join("references")).expect("shared resource dir");
        std::fs::write(
            shared.join("SKILL.md"),
            format!(
                "---\nname: shared-workflow\ndescription: {description}\nallowed-tools:\n  - read_file\n---\n{instructions}\n"
            ),
        )
        .expect("shared skill");
        std::fs::write(shared.join("references/scope.txt"), resource).expect("shared resource");
        let exclusive_root = workspace.join(".bamboo/skills").join(exclusive);
        std::fs::create_dir_all(&exclusive_root).expect("exclusive skill dir");
        std::fs::write(
            exclusive_root.join("SKILL.md"),
            format!(
                "---\nname: {exclusive}\ndescription: {exclusive} project skill\n---\n{exclusive} instructions\n"
            ),
        )
        .expect("exclusive skill");
    }

    let skill_manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
        skills_dir: global_skills,
        project_dir: None,
        active_mode: None,
    }));
    skill_manager
        .initialize()
        .await
        .expect("initialize manager");

    let catalog_one = skill_manager
        .store()
        .workflow_catalog_for_workspace(&workspace_one)
        .await
        .expect("workspace one catalog");
    let catalog_two = skill_manager
        .store()
        .workflow_catalog_for_workspace(&workspace_two)
        .await
        .expect("workspace two catalog");
    assert_eq!(
        catalog_one
            .entries
            .iter()
            .find(|entry| entry.id == "shared-workflow")
            .expect("shared one")
            .description,
        "alpha needle workflow"
    );
    assert_eq!(
        catalog_two
            .entries
            .iter()
            .find(|entry| entry.id == "shared-workflow")
            .expect("shared two")
            .description,
        "beta needle workflow"
    );
    assert!(catalog_one
        .entries
        .iter()
        .any(|entry| entry.id == "only-alpha"));
    assert!(!catalog_one
        .entries
        .iter()
        .any(|entry| entry.id == "only-beta"));
    assert!(catalog_two
        .entries
        .iter()
        .any(|entry| entry.id == "only-beta"));
    assert!(!catalog_two
        .entries
        .iter()
        .any(|entry| entry.id == "only-alpha"));

    let disabled = std::collections::BTreeSet::new();
    let explicitly_selected = vec!["shared-workflow".to_string()];
    let selected_one = skill_manager
        .resolve_skills_for_request_in_workspace_with_mode(
            &workspace_one,
            &disabled,
            Some(&explicitly_selected),
            None,
            None,
        )
        .await
        .expect("explicit selection one");
    let selected_two = skill_manager
        .resolve_skills_for_request_in_workspace_with_mode(
            &workspace_two,
            &disabled,
            Some(&explicitly_selected),
            None,
            None,
        )
        .await
        .expect("explicit selection two");
    assert_eq!(selected_one[0].prompt, "Alpha workspace instructions.");
    assert_eq!(selected_two[0].prompt, "Beta workspace instructions.");

    let auto_one = skill_manager
        .resolve_skills_for_request_in_workspace_with_mode(
            &workspace_one,
            &disabled,
            None,
            None,
            Some("alpha needle"),
        )
        .await
        .expect("auto selection one");
    let auto_two = skill_manager
        .resolve_skills_for_request_in_workspace_with_mode(
            &workspace_two,
            &disabled,
            None,
            None,
            Some("beta needle"),
        )
        .await
        .expect("auto selection two");
    assert!(auto_one.iter().any(|skill| skill.id == "only-alpha"));
    assert!(!auto_one.iter().any(|skill| skill.id == "only-beta"));
    assert!(auto_two.iter().any(|skill| skill.id == "only-beta"));
    assert!(!auto_two.iter().any(|skill| skill.id == "only-alpha"));
    assert_eq!(
        auto_one
            .iter()
            .find(|skill| skill.id == "shared-workflow")
            .expect("auto shared one")
            .description,
        "alpha needle workflow"
    );
    assert_eq!(
        auto_two
            .iter()
            .find(|skill| skill.id == "shared-workflow")
            .expect("auto shared two")
            .description,
        "beta needle workflow"
    );

    let mut session_one = Session::new("workspace-session-one", "model");
    session_one.set_workspace_path_meta(workspace_one.to_string_lossy());
    session_one.metadata.insert(
        SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
        r#"["shared-workflow"]"#.to_string(),
    );
    let mut session_two = Session::new("workspace-session-two", "model");
    session_two.set_workspace_path_meta(workspace_two.to_string_lossy());
    session_two.metadata.insert(
        SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.to_string(),
        r#"["shared-workflow"]"#.to_string(),
    );
    let sessions = Arc::new(dashmap::DashMap::new());
    for session in [&session_one, &session_two] {
        sessions.insert(
            session.id.clone(),
            Arc::new(parking_lot::RwLock::new(session.clone())),
        );
    }
    let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
    storage.save_session(&session_one).await.expect("save one");
    storage.save_session(&session_two).await.expect("save two");
    let persistence = Arc::new(bamboo_storage::LockedSessionStore::new(storage.clone()));
    let repo = bamboo_engine::SessionRepository::new(sessions, storage, persistence);
    let config = Arc::new(RwLock::new(Config::default()));
    let load_tool = LoadSkillTool::new(skill_manager.clone(), config.clone(), repo.clone());
    let read_tool = ReadSkillResourceTool::new(skill_manager, config, repo);

    for (session_id, expected_instructions, expected_resource, expected_workspace) in [
        (
            "workspace-session-one",
            "Alpha workspace instructions.",
            "alpha resource",
            &workspace_one,
        ),
        (
            "workspace-session-two",
            "Beta workspace instructions.",
            "beta resource",
            &workspace_two,
        ),
    ] {
        let context = ToolExecutionContext {
            session_id: Some(session_id),
            tool_call_id: "workspace-skill-call",
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        };
        let ToolOutcome::Completed(loaded) = load_tool
            .invoke(
                serde_json::json!({ "skill_id": "shared-workflow" }),
                context.to_tool_ctx(),
            )
            .await
            .expect("load workspace skill")
        else {
            panic!("load_skill should complete")
        };
        let loaded: serde_json::Value =
            serde_json::from_str(&loaded.result).expect("load result json");
        assert_eq!(loaded["instructions"], expected_instructions);
        let expected_workspace =
            std::fs::canonicalize(expected_workspace).expect("canonical workspace");
        let skill_root = loaded["skill_base_dir"].as_str().expect("skill root");
        assert!(
            skill_root.starts_with(expected_workspace.to_string_lossy().as_ref()),
            "runtime root {skill_root} must stay under {}",
            expected_workspace.display()
        );

        let ToolOutcome::Completed(resource) = read_tool
            .invoke(
                serde_json::json!({
                    "skill_id": "shared-workflow",
                    "resource_path": "references/scope.txt"
                }),
                context.to_tool_ctx(),
            )
            .await
            .expect("read workspace resource")
        else {
            panic!("read_skill_resource should complete")
        };
        let resource: serde_json::Value =
            serde_json::from_str(&resource.result).expect("resource result json");
        assert_eq!(resource["content"], expected_resource);
    }
}
