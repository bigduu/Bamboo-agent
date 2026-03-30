use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::RwLock;
use walkdir::WalkDir;

use crate::agent::core::storage::Storage;
use crate::agent::core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use crate::agent::core::Session;
use crate::agent::skill::SkillManager;
use crate::core::Config;

const SELECTED_SKILL_IDS_METADATA_KEY: &str = "selected_skill_ids";
const SELECTED_SKILL_MODE_METADATA_KEY: &str = "skill_mode";
const MAX_RESOURCE_CONTENT_CHARS: usize = 50_000;
const LOADED_SKILL_IDS_METADATA_KEY: &str = "skill_runtime_loaded_skill_ids";
const LAST_LOADED_SKILL_ID_METADATA_KEY: &str = "skill_runtime_last_loaded_skill_id";

fn parse_loaded_skill_ids(raw: &str) -> HashSet<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return HashSet::new();
    }

    if let Ok(ids) = serde_json::from_str::<Vec<String>>(trimmed) {
        return ids
            .into_iter()
            .map(|id| id.trim().to_string())
            .filter(|id| !id.is_empty())
            .collect();
    }

    trimmed
        .split(',')
        .map(|id| id.trim().to_string())
        .filter(|id| !id.is_empty())
        .collect()
}

fn serialize_loaded_skill_ids(ids: &HashSet<String>) -> String {
    let sorted: BTreeSet<String> = ids
        .iter()
        .map(|id| id.trim().to_string())
        .filter(|id| !id.is_empty())
        .collect();
    serde_json::to_string(&sorted.into_iter().collect::<Vec<String>>()).unwrap_or("[]".to_string())
}

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

    async fn selected_skill_allowlist(&self, session_id: Option<&str>) -> Option<HashSet<String>> {
        let session = self.session_for_context(session_id).await?;

        let selected = session
            .metadata
            .get(SELECTED_SKILL_IDS_METADATA_KEY)
            .and_then(|raw| {
                crate::agent::skill::selection::parse_selected_skill_ids_metadata(raw)
            })?;

        Some(selected.into_iter().collect())
    }

    async fn selected_skill_mode(&self, session_id: Option<&str>) -> Option<String> {
        let session = self.session_for_context(session_id).await?;
        let mode = session
            .metadata
            .get(SELECTED_SKILL_MODE_METADATA_KEY)
            .or_else(|| session.metadata.get("mode"))?;
        let trimmed = mode.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    }

    async fn ensure_skill_allowed(
        &self,
        skill_id: &str,
        session_id: Option<&str>,
    ) -> Result<(), ToolError> {
        let disabled_skill_ids = {
            let config = self.config.read().await;
            config.disabled_skill_ids()
        };
        if disabled_skill_ids.contains(skill_id) {
            return Err(ToolError::Execution(format!(
                "Skill '{skill_id}' is globally disabled in Bamboo settings"
            )));
        }

        let Some(allowlist) = self.selected_skill_allowlist(session_id).await else {
            return Ok(());
        };

        if allowlist.contains(skill_id) {
            return Ok(());
        }

        Err(ToolError::Execution(format!(
            "Skill '{skill_id}' is not selected for this request"
        )))
    }

    async fn ensure_skill_loaded(
        &self,
        skill_id: &str,
        session_id: Option<&str>,
    ) -> Result<(), ToolError> {
        let Some(session_id) = session_id else {
            return Err(ToolError::Execution(
                "read_skill_resource requires a session_id in tool context".to_string(),
            ));
        };

        let session = self
            .session_for_context(Some(session_id))
            .await
            .ok_or_else(|| {
                ToolError::Execution(format!(
                    "Session '{session_id}' was not found while verifying loaded skill state"
                ))
            })?;

        let loaded_ids = session
            .metadata
            .get(LOADED_SKILL_IDS_METADATA_KEY)
            .map(|raw| parse_loaded_skill_ids(raw))
            .unwrap_or_default();

        if loaded_ids.contains(skill_id) {
            return Ok(());
        }

        Err(ToolError::Execution(format!(
            "Skill '{skill_id}' has not been loaded in this session. Call load_skill first."
        )))
    }

    async fn mark_skill_loaded(
        &self,
        skill_id: &str,
        session_id: Option<&str>,
    ) -> Result<(), ToolError> {
        let Some(session_id) = session_id else {
            return Err(ToolError::Execution(
                "load_skill requires a session_id in tool context".to_string(),
            ));
        };

        let mut session = {
            let sessions = self.sessions.read().await;
            sessions.get(session_id).cloned()
        };

        if session.is_none() {
            session = self
                .storage
                .load_session(session_id)
                .await
                .map_err(|error| {
                    ToolError::Execution(format!(
                    "Failed to load session '{session_id}' while persisting loaded skill: {error}"
                ))
                })?;
        }

        let Some(mut session) = session else {
            return Err(ToolError::Execution(format!(
                "Session '{session_id}' not found while persisting loaded skill state"
            )));
        };

        let mut loaded_ids = session
            .metadata
            .get(LOADED_SKILL_IDS_METADATA_KEY)
            .map(|raw| parse_loaded_skill_ids(raw))
            .unwrap_or_default();
        loaded_ids.insert(skill_id.to_string());

        session.metadata.insert(
            LOADED_SKILL_IDS_METADATA_KEY.to_string(),
            serialize_loaded_skill_ids(&loaded_ids),
        );
        session.metadata.insert(
            LAST_LOADED_SKILL_ID_METADATA_KEY.to_string(),
            skill_id.to_string(),
        );

        self.storage.save_session(&session).await.map_err(|error| {
            ToolError::Execution(format!(
                "Failed to save session '{session_id}' after load_skill: {error}"
            ))
        })?;

        let mut sessions = self.sessions.write().await;
        sessions.insert(session_id.to_string(), session);

        Ok(())
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

        self.access
            .ensure_skill_allowed(skill_id, ctx.session_id)
            .await?;
        let skill_mode = self.access.selected_skill_mode(ctx.session_id).await;

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
        self.access
            .mark_skill_loaded(skill_id, ctx.session_id)
            .await?;

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
                "skill_base_dir": crate::core::paths::path_to_display_string(&canonical_skill_root),
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

        self.access
            .ensure_skill_allowed(skill_id, ctx.session_id)
            .await?;
        self.access
            .ensure_skill_loaded(skill_id, ctx.session_id)
            .await?;
        let skill_mode = self.access.selected_skill_mode(ctx.session_id).await;

        let resource_path = normalize_relative_resource_path(&parsed.resource_path)?;
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

fn list_skill_resource_paths(skill_root: &Path) -> std::io::Result<Vec<String>> {
    if !skill_root.exists() {
        return Ok(Vec::new());
    }

    let mut resources = Vec::new();
    for entry in WalkDir::new(skill_root)
        .min_depth(1)
        .into_iter()
        .filter_map(Result::ok)
    {
        if !entry.file_type().is_file() {
            continue;
        }

        let Ok(relative) = entry.path().strip_prefix(skill_root) else {
            continue;
        };
        if relative == Path::new("SKILL.md") {
            continue;
        }

        resources.push(display_relative_path(relative));
    }

    resources.sort();
    resources.dedup();
    Ok(resources)
}

fn normalize_relative_resource_path(raw: &str) -> Result<PathBuf, ToolError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(ToolError::InvalidArguments(
            "resource_path must be a non-empty relative path".to_string(),
        ));
    }

    let path = Path::new(trimmed);
    if path.is_absolute() {
        return Err(ToolError::InvalidArguments(
            "resource_path must be relative, absolute paths are not allowed".to_string(),
        ));
    }

    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(segment) => normalized.push(segment),
            Component::CurDir => {}
            Component::ParentDir | Component::Prefix(_) | Component::RootDir => {
                return Err(ToolError::InvalidArguments(
                    "resource_path cannot contain '..' or root/prefix segments".to_string(),
                ))
            }
        }
    }

    if normalized.as_os_str().is_empty() {
        return Err(ToolError::InvalidArguments(
            "resource_path must resolve to a file path".to_string(),
        ));
    }

    Ok(normalized)
}

fn truncate_text(content: &str, max_chars: usize) -> (&str, bool) {
    if max_chars == 0 {
        return ("", !content.is_empty());
    }

    let mut count = 0usize;
    for (index, _) in content.char_indices() {
        if count == max_chars {
            return (&content[..index], true);
        }
        count += 1;
    }

    (content, false)
}

fn page_text_lines(
    content: &str,
    offset: usize,
    limit: Option<usize>,
) -> (String, usize, usize, usize) {
    let lines: Vec<&str> = content.lines().collect();
    let total = lines.len();
    let start = offset.min(total);
    let end = limit
        .map(|value| start.saturating_add(value).min(total))
        .unwrap_or(total);
    let paged = lines[start..end].join("\n");
    (paged, start, end, total)
}

fn display_relative_path(path: &Path) -> String {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(segment) => Some(segment.to_string_lossy().into_owned()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::{
        normalize_relative_resource_path, page_text_lines, parse_loaded_skill_ids,
        serialize_loaded_skill_ids, truncate_text, LoadSkillTool,
    };
    use std::collections::{HashMap, HashSet};
    use std::path::Path;
    use std::sync::Arc;

    use tokio::sync::RwLock;

    use crate::agent::core::storage::Storage;
    use crate::agent::core::tools::{Tool, ToolExecutionContext};
    use crate::agent::core::{AgentEvent, Session};
    use crate::agent::skill::{SkillManager, SkillStoreConfig};
    use crate::core::Config;

    #[test]
    fn normalize_relative_resource_path_rejects_invalid_paths() {
        assert!(normalize_relative_resource_path("").is_err());
        assert!(normalize_relative_resource_path("../secrets.txt").is_err());
        assert!(normalize_relative_resource_path("/tmp/test.txt").is_err());
    }

    #[test]
    fn normalize_relative_resource_path_accepts_nested_file() {
        let path =
            normalize_relative_resource_path("references/policy.md").expect("path should parse");
        assert_eq!(path, Path::new("references/policy.md"));
    }

    #[test]
    fn truncate_text_reports_truncation() {
        let (text, truncated) = truncate_text("abcde", 3);
        assert_eq!(text, "abc");
        assert!(truncated);
    }

    #[test]
    fn truncate_text_keeps_short_text() {
        let (text, truncated) = truncate_text("abc", 10);
        assert_eq!(text, "abc");
        assert!(!truncated);
    }

    #[test]
    fn page_text_lines_respects_offset_and_limit() {
        let (text, start, end, total) = page_text_lines("a\nb\nc\n", 1, Some(1));
        assert_eq!(text, "b");
        assert_eq!(start, 1);
        assert_eq!(end, 2);
        assert_eq!(total, 3);
    }

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

        async fn append_event(&self, _session_id: &str, _event: &AgentEvent) -> std::io::Result<()> {
            Ok(())
        }

        async fn load_events(&self, _session_id: &str) -> std::io::Result<Vec<AgentEvent>> {
            Ok(Vec::new())
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
name: Demo Skill
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
}
