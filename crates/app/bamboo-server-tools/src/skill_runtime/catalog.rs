// Copyright 2025 OpenAI. Licensed under Apache-2.0.
// Adapted from Codex ext/skills/src/tools/list.rs, revision
// 7f892275e31002f0422477c6219189284560e689. See third_party/codex/NOTICE.
use super::{SkillCatalogCaller, SkillCatalogCallerResolver, SkillToolAccess};
use async_trait::async_trait;
use bamboo_agent_core::tools::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult};
use bamboo_llm::Config;
use bamboo_skills::{
    progressive::{
        render_skill_catalog, skill_metadata_budget, SkillCatalogEligibility, SkillCatalogRender,
        SkillCatalogRenderPolicy, SkillCatalogSnapshot,
    },
    SkillManager, SkillStore,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::RwLock;

pub const MAX_SKILLS_LIST_BYTES: usize = 512 * 1024;
const MAX_HANDLE_BYTES: usize = 2_048;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListArgs {
    cursor: Option<String>,
    limit: Option<usize>,
}
#[derive(Serialize)]
struct ListedSkill {
    package: String,
    name: String,
    description: String,
    main_resource: String,
}
#[derive(Serialize)]
struct ListResponse {
    skills: Vec<ListedSkill>,
    warnings: Vec<String>,
    next_cursor: Option<String>,
}

/// Historical owned bytes with current-call identity; never a bearer grant.
#[derive(Clone)]
pub struct SelectedSkillSource {
    pub snapshot: Arc<bamboo_skills::progressive::SelectedSkillSnapshot>,
    pub identity: String,
}

struct SelectedCatalogContext {
    caller: SkillCatalogCaller,
    snapshot: SkillCatalogSnapshot,
    identity: String,
    store: Arc<SkillStore>,
    access: SkillCatalogEligibility,
    scope: (Option<String>, Option<String>, bool, u64),
}

/// Exported metadata Tool; a trusted resolver is mandatory at construction.
/// It is intentionally not installed in any live registry by this API slice.
pub struct SkillsListTool {
    access: SkillToolAccess,
    resolver: Arc<dyn SkillCatalogCallerResolver>,
    #[cfg(test)]
    pub(super) store_selection_barrier: Option<Arc<(tokio::sync::Notify, tokio::sync::Notify)>>,
}
impl SkillsListTool {
    pub fn new(
        manager: Arc<SkillManager>,
        config: Arc<RwLock<Config>>,
        sessions: bamboo_engine::SessionRepository,
        resolver: Arc<dyn SkillCatalogCallerResolver>,
    ) -> Self {
        Self {
            access: SkillToolAccess::new(manager, config, sessions),
            resolver,
            #[cfg(test)]
            store_selection_barrier: None,
        }
    }
    pub fn with_project_store(mut self, projects: Arc<bamboo_projects::ProjectStore>) -> Self {
        self.access = self.access.with_project_store(projects);
        self
    }
    /// Render eligible metadata using the same fresh caller/input projection as
    /// skills_list. This does not install a live Tool or read Skill bodies.
    pub async fn render_catalog(&self, ctx: &ToolCtx) -> Result<SkillCatalogRender, ToolError> {
        let prepared = self.metadata(ctx).await?;
        Ok(render_skill_catalog(
            &prepared.snapshot.entries,
            SkillCatalogRenderPolicy::ExtensionCompatible,
            skill_metadata_budget(
                prepared.caller.context_window,
                prepared.caller.metadata_tokens,
            ),
        ))
    }

    async fn metadata(&self, ctx: &ToolCtx) -> Result<SelectedCatalogContext, ToolError> {
        // Await the trusted adapter before any Source publication guard is held.
        let caller = self.resolver.resolve(ctx).await?;
        if ctx.session_id() != Some(caller.session_id.as_str())
            || [&caller.caller_id, &caller.input_id, &caller.session_id]
                .iter()
                .any(|id| id.is_empty() || id.len() > MAX_HANDLE_BYTES)
            || caller.mode.as_ref().is_some_and(|mode| {
                mode.is_empty()
                    || mode.len() > 256
                    || !mode
                        .chars()
                        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-')
            })
            || caller
                .invocation
                .as_ref()
                .is_some_and(|intent| intent.input_id != caller.input_id)
        {
            return Err(ToolError::Execution(
                "Skill caller/input binding is stale or invalid".into(),
            ));
        }
        let session = self
            .access
            .session_for_context(ctx.session_id())
            .await
            .ok_or_else(|| ToolError::Execution("Skill caller session is unavailable".into()))?;
        if !session.messages.iter().any(|message| {
            message.id == caller.input_id && message.role == bamboo_agent_core::Role::User
        }) {
            return Err(ToolError::Execution(
                "Accepted Skill input is unavailable".into(),
            ));
        }
        let access = SkillCatalogEligibility {
            ceiling: caller.ceiling.clone(),
            explicit: caller
                .invocation
                .as_ref()
                .map(|intent| intent.skills.clone())
                .unwrap_or_default(),
            disabled: self
                .access
                .config
                .read()
                .await
                .disabled_skill_ids()
                .into_iter()
                .collect(),
            deny_all: session.root_orchestration_only_enabled(),
        };
        let store = self.access.skill_store_for_session(&session).await?;
        #[cfg(test)]
        if let Some((entered, release)) = self.store_selection_barrier.as_deref() {
            entered.notify_one();
            release.notified().await;
        }
        let snapshot = store
            .progressive_catalog_for_mode(caller.mode.as_deref(), &access)
            .await
            .map_err(|error| ToolError::Execution(error.to_string()))?;
        let identity = fingerprint(&(
            &snapshot.identity,
            &caller,
            &access,
            session.workspace_path_meta(),
            session.project_id_meta(),
            session.root_tool_authority_revision,
        ))?;
        let prepared = SelectedCatalogContext {
            caller,
            snapshot,
            identity,
            store,
            access,
            scope: scope_identity(&session),
        };
        self.recheck(ctx, &prepared).await?;
        Ok(prepared)
    }

    async fn recheck(
        &self,
        ctx: &ToolCtx,
        prepared: &SelectedCatalogContext,
    ) -> Result<(), ToolError> {
        // Resolve first: the adapter may await while official Session/config setters run.
        let caller = self.resolver.resolve(ctx).await?;
        // Hold configuration stable before the last Session await. Everything after
        // that read compares the current caller/scope/input/config synchronously.
        let config = self.access.config.read().await;
        let current = self
            .access
            .session_for_context(ctx.session_id())
            .await
            .ok_or_else(|| ToolError::Execution("Skill caller session is unavailable".into()))?;
        if prepared.scope != scope_identity(&current)
            || !current.messages.iter().any(|message| {
                message.id == caller.input_id && message.role == bamboo_agent_core::Role::User
            })
            || fingerprint(&caller)? != fingerprint(&prepared.caller)?
            || prepared.access.disabled != config.disabled_skill_ids().into_iter().collect()
        {
            return Err(ToolError::Execution(
                "Skill host scope/caller changed during projection".into(),
            ));
        }
        Ok(())
    }

    /// Unregistered Rust helper: materialize one complete current chosen file.
    /// Mandatory trusted resolution and the same list/render authorizer apply.
    pub async fn selected_source(
        &self,
        ctx: &ToolCtx,
        package: &str,
        resource: &str,
    ) -> Result<SelectedSkillSource, ToolError> {
        let prepared = self.metadata(ctx).await?;
        let snapshot = prepared
            .store
            .selected_source_for_mode(
                prepared.caller.mode.as_deref(),
                &prepared.access,
                &prepared.snapshot,
                package,
                resource,
            )
            .await
            .map_err(|error| ToolError::Execution(error.to_string()))?;
        self.recheck(ctx, &prepared).await?;
        let identity = fingerprint(&(&prepared.identity, &snapshot.identity))?;
        Ok(SelectedSkillSource { snapshot, identity })
    }

    /// Revalidate historical owned data without materializing another whole file.
    /// R2 may reuse this probe; no cache/cursor/Tool is installed by this API.
    pub async fn probe_selected_source(
        &self,
        ctx: &ToolCtx,
        selected: &SelectedSkillSource,
    ) -> Result<(), ToolError> {
        let prepared = self.metadata(ctx).await?;
        let identity = prepared
            .store
            .probe_selected_source_for_mode(
                prepared.caller.mode.as_deref(),
                &prepared.access,
                &prepared.snapshot,
                &selected.snapshot.package,
                &selected.snapshot.resource,
            )
            .await
            .map_err(|error| ToolError::Execution(error.to_string()))?;
        self.recheck(ctx, &prepared).await?;
        if fingerprint(&(&prepared.identity, identity))? != selected.identity {
            return Err(ToolError::Execution(
                "selected Skill caller/source changed".into(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl Tool for SkillsListTool {
    fn name(&self) -> &str {
        "skills_list"
    }
    fn description(&self) -> &str {
        "List currently eligible Instruction Skill metadata, package and main-resource locators. Continue with next_cursor until metadata EOF."
    }
    fn parameters_schema(&self) -> Value {
        json!({"type":"object","properties":{"cursor":{"type":"string","maxLength":MAX_HANDLE_BYTES},"limit":{"type":"integer","minimum":1,"maximum":20}},"additionalProperties":false})
    }
    fn classify(&self, _: &Value) -> ToolClass {
        ToolClass::READONLY_PARALLEL
    }
    async fn invoke(&self, args: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        let args: ListArgs = serde_json::from_value(args)
            .map_err(|error| ToolError::InvalidArguments(error.to_string()))?;
        let limit = args.limit.unwrap_or(20);
        if !(1..=20).contains(&limit) {
            return Err(ToolError::InvalidArguments(
                "skills_list limit must be 1..20".into(),
            ));
        }
        let prepared = self.metadata(&ctx).await?;
        let snapshot = prepared.snapshot;
        let identity = fingerprint(&(prepared.identity, limit))?;
        let budget = prepared.caller.response_bytes.min(MAX_SKILLS_LIST_BYTES);
        let start = parse_cursor(args.cursor.as_deref(), &identity, snapshot.entries.len())?;
        let mut end = snapshot.entries.len().min(start.saturating_add(limit));
        loop {
            let skills = snapshot.entries[start..end]
                .iter()
                .map(|entry| {
                    let description = entry
                        .short_description
                        .as_deref()
                        .unwrap_or(&entry.description);
                    ListedSkill {
                        package: entry.package.clone(),
                        name: truncate_name(&entry.name),
                        description: truncate_description(description),
                        main_resource: entry.main_resource.clone(),
                    }
                })
                .collect();
            let response = ListResponse {
                skills,
                warnings: Vec::new(),
                next_cursor: (end < snapshot.entries.len()).then(|| format!("{identity}:{end}")),
            };
            let result =
                ToolResult::text(true, serde_json::to_string(&response).map_err(json_error)?);
            if page_size(&result, &ctx.tool_call_id)? <= budget {
                return Ok(ToolOutcome::Completed(result));
            }
            if end > start.saturating_add(1) {
                end -= 1;
            } else {
                return Err(ToolError::Execution(
                    "skills_list response budget cannot fit one complete metadata entry/envelope"
                        .into(),
                ));
            }
        }
    }
}

fn scope_identity(
    session: &bamboo_agent_core::Session,
) -> (Option<String>, Option<String>, bool, u64) {
    (
        session.workspace_path_meta(),
        session.project_id_meta(),
        session.root_orchestration_only_enabled(),
        session.root_tool_authority_revision,
    )
}
fn json_error(error: serde_json::Error) -> ToolError {
    ToolError::Execution(error.to_string())
}
fn fingerprint(value: &impl Serialize) -> Result<String, ToolError> {
    Ok(hex::encode(Sha256::digest(
        serde_json::to_vec(value).map_err(json_error)?,
    )))
}
fn parse_cursor(cursor: Option<&str>, identity: &str, count: usize) -> Result<usize, ToolError> {
    let Some(cursor) = cursor else {
        return Ok(0);
    };
    let invalid = || ToolError::InvalidArguments("skills_list cursor is stale or invalid".into());
    if cursor.len() > MAX_HANDLE_BYTES {
        return Err(invalid());
    }
    let (fingerprint, offset) = cursor.split_once(':').ok_or_else(invalid)?;
    let offset = offset.parse::<usize>().map_err(|_| invalid())?;
    if fingerprint != identity || offset > count {
        return Err(invalid());
    }
    Ok(offset)
}
fn truncate_name(value: &str) -> String {
    let mut end = value.len().min(256);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}
fn truncate_description(value: &str) -> String {
    if value.chars().count() <= 1_024 {
        return value.to_string();
    }
    format!("{}...", value.chars().take(1_021).collect::<String>())
}

// Bound the ToolResult plus actual page-bearing outbound blocks, not an entire
// request containing unrelated history. Maximum Anthropic cache TTL is charged.
fn page_size(result: &ToolResult, call_id: &str) -> Result<usize, ToolError> {
    let openai = json!({"type":"function_call_output","call_id":call_id,"output":result.result});
    let cached_openai = json!({"type":"function_call_output","call_id":call_id,"output":[{"type":"input_text","text":result.result,"prompt_cache_breakpoint":{"mode":"explicit"}}]});
    let anthropic = json!({"type":"tool_result","tool_use_id":call_id,"content":result.result,"is_error":false,"cache_control":{"type":"ephemeral","ttl":"1h"}});
    Ok([
        serde_json::to_vec(result).map_err(json_error)?.len(),
        serde_json::to_vec(&openai).map_err(json_error)?.len(),
        serde_json::to_vec(&cached_openai)
            .map_err(json_error)?
            .len(),
        serde_json::to_vec(&anthropic).map_err(json_error)?.len(),
    ]
    .into_iter()
    .max()
    .unwrap_or(0))
}
