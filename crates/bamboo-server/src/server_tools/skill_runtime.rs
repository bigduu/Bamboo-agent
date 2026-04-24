use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::RwLock;

use bamboo_engine::access_control::{self, SkillAccessError, SkillSessionPort};
use bamboo_engine::resource_helpers::{
    display_relative_path, list_skill_resource_paths, normalize_relative_resource_path,
    page_text_lines, truncate_text,
};
use bamboo_engine::runtime_metadata::LAST_RESOURCE_READ_SUMMARY_METADATA_KEY;
use bamboo_engine::SkillManager;
use bamboo_infrastructure::Config;

use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use bamboo_agent_core::Session;

const MAX_RESOURCE_CONTENT_CHARS: usize = 50_000;

#[derive(Clone)]
struct SkillToolAccess {
    skill_manager: Arc<SkillManager>,
    config: Arc<RwLock<Config>>,
    sessions: Arc<RwLock<HashMap<String, Session>>>,
    storage: Arc<dyn Storage>,
}

impl SkillToolAccess {
    fn new(
        skill_manager: Arc<SkillManager>,
        config: Arc<RwLock<Config>>,
        sessions: Arc<RwLock<HashMap<String, Session>>>,
        storage: Arc<dyn Storage>,
    ) -> Self {
        Self {
            skill_manager,
            config,
            sessions,
            storage,
        }
    }

    async fn session_for_context(&self, session_id: Option<&str>) -> Option<Session> {
        let session_id = session_id?;

        let in_memory = {
            let sessions = self.sessions.read().await;
            sessions.get(session_id).cloned()
        };

        match in_memory {
            Some(session) => Some(session),
            None => self.storage.load_session(session_id).await.ok().flatten(),
        }
    }

    async fn skill_root(
        &self,
        skill_id: &str,
        skill_mode: Option<&str>,
    ) -> Result<PathBuf, ToolError> {
        self.skill_manager
            .store()
            .get_skill_root_for_mode(skill_id, skill_mode)
            .await
            .map_err(|err| ToolError::Execution(format!("Failed to resolve skill root: {err}")))
    }
}

#[async_trait]
impl SkillSessionPort for SkillToolAccess {
    async fn load_session_metadata(&self, session_id: &str) -> Option<HashMap<String, String>> {
        self.session_for_context(Some(session_id))
            .await
            .map(|session| session.metadata.clone())
    }

    async fn save_metadata_updates(
        &self,
        session_id: &str,
        updates: &[(String, Option<String>)],
    ) -> Result<(), String> {
        let mut session = {
            let sessions = self.sessions.read().await;
            sessions.get(session_id).cloned()
        };

        if session.is_none() {
            session = self
                .storage
                .load_session(session_id)
                .await
                .map_err(|e| e.to_string())?;
        }

        let mut session = session.ok_or_else(|| format!("Session '{session_id}' not found"))?;

        for (key, value) in updates {
            if let Some(val) = value {
                session.metadata.insert(key.clone(), val.clone());
            } else {
                session.metadata.remove(key);
            }
        }

        self.storage
            .save_session(&session)
            .await
            .map_err(|e| e.to_string())?;

        let mut sessions = self.sessions.write().await;
        sessions.insert(session_id.to_string(), session);

        Ok(())
    }

    async fn disabled_skill_ids(&self) -> HashSet<String> {
        let config = self.config.read().await;
        config.disabled_skill_ids().into_iter().collect()
    }
}

fn skill_access_error_to_tool_error(error: SkillAccessError) -> ToolError {
    match error {
        SkillAccessError::NotAllowed(msg)
        | SkillAccessError::NotLoaded(msg)
        | SkillAccessError::SessionRequired(msg)
        | SkillAccessError::SessionNotFound(msg)
        | SkillAccessError::PersistenceError(msg) => ToolError::Execution(msg),
    }
}

#[derive(Debug, Deserialize)]
struct LoadSkillArgs {
    skill_id: String,
}

pub struct LoadSkillTool {
    access: SkillToolAccess,
}

impl LoadSkillTool {
    pub fn new(
        skill_manager: Arc<SkillManager>,
        config: Arc<RwLock<Config>>,
        sessions: Arc<RwLock<HashMap<String, Session>>>,
        storage: Arc<dyn Storage>,
    ) -> Self {
        Self {
            access: SkillToolAccess::new(skill_manager, config, sessions, storage),
        }
    }
}

#[async_trait]
impl Tool for LoadSkillTool {
    fn name(&self) -> &str {
        "load_skill"
    }

    fn description(&self) -> &str {
        "Load a skill's detailed SKILL.md instructions by skill_id."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "skill_id": {
                    "type": "string",
                    "description": "Skill ID from the advertised skill list (for example: skill-creator)."
                }
            },
            "required": ["skill_id"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        self.execute_with_context(args, ToolExecutionContext::none("tool_call"))
            .await
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let parsed: LoadSkillArgs = serde_json::from_value(args).map_err(|err| {
            ToolError::InvalidArguments(format!("Invalid load_skill args: {err}"))
        })?;
        let skill_id = parsed.skill_id.trim();
        if skill_id.is_empty() {
            return Err(ToolError::InvalidArguments(
                "skill_id must be a non-empty string".to_string(),
            ));
        }

        access_control::ensure_skill_allowed(&self.access, skill_id, ctx.session_id)
            .await
            .map_err(skill_access_error_to_tool_error)?;
        let skill_mode = access_control::selected_skill_mode(&self.access, ctx.session_id).await;

        let skill = self
            .access
            .skill_manager
            .store()
            .get_skill_for_mode(skill_id, skill_mode.as_deref())
            .await
            .map_err(|err| {
                ToolError::Execution(format!("Failed to load skill '{skill_id}': {err}"))
            })?;
        let skill_root = self
            .access
            .skill_root(skill_id, skill_mode.as_deref())
            .await?;
        let resources = list_skill_resource_paths(&skill_root).map_err(|err| {
            ToolError::Execution(format!("Failed to list skill resources: {err}"))
        })?;
        let canonical_skill_root = tokio::fs::canonicalize(&skill_root)
            .await
            .unwrap_or(skill_root);
        access_control::mark_skill_loaded(&self.access, skill_id, ctx.session_id)
            .await
            .map_err(skill_access_error_to_tool_error)?;

        Ok(ToolResult {
            success: true,
            result: json!({
                "skill_id": skill.id,
                "name": skill.name,
                "description": skill.description,
                "license": skill.license,
                "compatibility": skill.compatibility,
                "allowed_tools": skill.tool_refs,
                "instructions": skill.prompt,
                "skill_base_dir": bamboo_infrastructure::paths::path_to_display_string(&canonical_skill_root),
                "resource_files": resources
            })
            .to_string(),
            display_preference: Some("Collapsible".to_string()),
        })
    }
}

#[derive(Debug, Deserialize)]
struct ReadSkillResourceArgs {
    skill_id: String,
    resource_path: String,
    #[serde(default)]
    offset: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
}

pub struct ReadSkillResourceTool {
    access: SkillToolAccess,
}

impl ReadSkillResourceTool {
    pub fn new(
        skill_manager: Arc<SkillManager>,
        config: Arc<RwLock<Config>>,
        sessions: Arc<RwLock<HashMap<String, Session>>>,
        storage: Arc<dyn Storage>,
    ) -> Self {
        Self {
            access: SkillToolAccess::new(skill_manager, config, sessions, storage),
        }
    }
}

#[async_trait]
impl Tool for ReadSkillResourceTool {
    fn name(&self) -> &str {
        "read_skill_resource"
    }

    fn description(&self) -> &str {
        "Read a resource file under a skill directory by relative resource_path."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "skill_id": {
                    "type": "string",
                    "description": "Skill ID that owns the resource."
                },
                "resource_path": {
                    "type": "string",
                    "description": "Relative path inside the skill folder (for example: references/policies.md)."
                },
                "offset": {
                    "type": "number",
                    "description": "Optional 0-based line offset for paged text reads."
                },
                "limit": {
                    "type": "number",
                    "description": "Optional line limit for paged text reads."
                }
            },
            "required": ["skill_id", "resource_path"]
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        self.execute_with_context(args, ToolExecutionContext::none("tool_call"))
            .await
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let parsed: ReadSkillResourceArgs = serde_json::from_value(args).map_err(|err| {
            ToolError::InvalidArguments(format!("Invalid read_skill_resource args: {err}"))
        })?;
        let skill_id = parsed.skill_id.trim();
        if skill_id.is_empty() {
            return Err(ToolError::InvalidArguments(
                "skill_id must be a non-empty string".to_string(),
            ));
        }

        access_control::ensure_skill_allowed(&self.access, skill_id, ctx.session_id)
            .await
            .map_err(skill_access_error_to_tool_error)?;
        access_control::ensure_skill_loaded(&self.access, skill_id, ctx.session_id)
            .await
            .map_err(skill_access_error_to_tool_error)?;
        let skill_mode = access_control::selected_skill_mode(&self.access, ctx.session_id).await;

        let resource_path = normalize_relative_resource_path(&parsed.resource_path)
            .map_err(ToolError::InvalidArguments)?;
        if resource_path == Path::new("SKILL.md") {
            return Err(ToolError::InvalidArguments(
                "Use load_skill for SKILL.md instructions; read_skill_resource is for auxiliary files"
                    .to_string(),
            ));
        }

        let skill_root = self
            .access
            .skill_root(skill_id, skill_mode.as_deref())
            .await?;
        let canonical_root = tokio::fs::canonicalize(&skill_root).await.map_err(|_| {
            ToolError::Execution(format!(
                "Skill directory not found for '{skill_id}'. Load the skill list first."
            ))
        })?;
        let canonical_resource = tokio::fs::canonicalize(skill_root.join(&resource_path))
            .await
            .map_err(|_| {
                ToolError::Execution(format!(
                    "Skill resource not found: {}/{}",
                    skill_id,
                    display_relative_path(&resource_path)
                ))
            })?;

        if !canonical_resource.starts_with(&canonical_root) {
            return Err(ToolError::InvalidArguments(
                "resource_path must stay inside the skill directory".to_string(),
            ));
        }

        let metadata = tokio::fs::metadata(&canonical_resource)
            .await
            .map_err(|err| ToolError::Execution(format!("Failed to stat resource: {err}")))?;
        if !metadata.is_file() {
            return Err(ToolError::InvalidArguments(format!(
                "resource_path must reference a file: {}",
                display_relative_path(&resource_path)
            )));
        }

        let bytes = tokio::fs::read(&canonical_resource)
            .await
            .map_err(|err| ToolError::Execution(format!("Failed to read skill resource: {err}")))?;
        let size_bytes = bytes.len();

        let result = match String::from_utf8(bytes) {
            Ok(text) => {
                let offset = parsed.offset.unwrap_or(0);
                let (paged, start, end, total_lines) = page_text_lines(&text, offset, parsed.limit);
                let (excerpt, truncated) = truncate_text(&paged, MAX_RESOURCE_CONTENT_CHARS);
                let has_more = end < total_lines;
                let summary = json!({
                    "skill_id": skill_id,
                    "resource_path": display_relative_path(&resource_path),
                    "offset": start,
                    "limit": parsed.limit,
                    "returned_lines": end.saturating_sub(start),
                    "total_lines": total_lines,
                    "has_more": has_more,
                    "truncated": truncated,
                    "binary": false
                });
                if let Some(session_id) = ctx.session_id {
                    if let Some(mut session) =
                        self.access.session_for_context(Some(session_id)).await
                    {
                        session.metadata.insert(
                            LAST_RESOURCE_READ_SUMMARY_METADATA_KEY.to_string(),
                            summary.to_string(),
                        );
                        let _ = self.access.storage.save_session(&session).await;
                        let mut sessions = self.access.sessions.write().await;
                        sessions.insert(session_id.to_string(), session);
                    }
                }
                json!({
                    "skill_id": skill_id,
                    "resource_path": display_relative_path(&resource_path),
                    "size_bytes": size_bytes,
                    "offset": start,
                    "limit": parsed.limit,
                    "returned_lines": end.saturating_sub(start),
                    "total_lines": total_lines,
                    "has_more": has_more,
                    "next_offset": if has_more { Some(end) } else { None::<usize> },
                    "truncated": truncated,
                    "content": excerpt
                })
            }
            Err(_) => json!({
                "skill_id": skill_id,
                "resource_path": display_relative_path(&resource_path),
                "size_bytes": size_bytes,
                "binary": true,
                "message": "Resource is not UTF-8 text. Use file tools when binary handling is required."
            }),
        };

        Ok(ToolResult {
            success: true,
            result: result.to_string(),
            display_preference: Some("Collapsible".to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{LoadSkillTool, ReadSkillResourceTool};
    use bamboo_engine::access_control::{parse_loaded_skill_ids, serialize_loaded_skill_ids};
    use bamboo_engine::runtime_metadata::{
        LAST_LOADED_SKILL_SUMMARY_METADATA_KEY, LAST_RESOURCE_READ_SUMMARY_METADATA_KEY,
    };
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use tokio::sync::RwLock;

    use bamboo_agent_core::storage::Storage;
    use bamboo_agent_core::tools::{Tool, ToolExecutionContext};
    use bamboo_agent_core::Session;
    use bamboo_engine::{SkillManager, SkillStoreConfig};
    use bamboo_infrastructure::Config;

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
        let sessions = Arc::new(RwLock::new(HashMap::from([(
            session_id.to_string(),
            session.clone(),
        )])));
        let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
        storage
            .save_session(&session)
            .await
            .expect("session should be saved");

        let tool = LoadSkillTool::new(skill_manager, config, sessions, storage);
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
        let sessions = Arc::new(RwLock::new(HashMap::from([(
            session_id.to_string(),
            session.clone(),
        )])));
        let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
        storage
            .save_session(&session)
            .await
            .expect("session should be saved");

        let tool = LoadSkillTool::new(skill_manager, config, sessions.clone(), storage.clone());
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
        let sessions = Arc::new(RwLock::new(HashMap::from([(
            session_id.to_string(),
            session.clone(),
        )])));
        let storage: Arc<dyn Storage> = Arc::new(TestStorage::default());
        storage
            .save_session(&session)
            .await
            .expect("session should be saved");

        let load_tool = LoadSkillTool::new(
            skill_manager.clone(),
            config.clone(),
            sessions.clone(),
            storage.clone(),
        );
        let read_tool =
            ReadSkillResourceTool::new(skill_manager, config, sessions, storage.clone());

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
}
