// Copyright 2025 OpenAI. Licensed under Apache-2.0.
// Adapted from Codex ext/skills/src/tools/{list.rs,read.rs,mod.rs}, revision
// 7f892275e31002f0422477c6219189284560e689. See third_party/codex/NOTICE.
use super::{SkillCatalogCaller, SkillCatalogCallerResolver, SkillToolAccess};
use async_trait::async_trait;
use bamboo_agent_core::tools::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult};
use bamboo_llm::Config;
use bamboo_skills::{
    progressive::{
        render_skill_catalog, skill_metadata_budget, CachedSkillRead, SelectedSkillReadCache,
        SkillCatalogEligibility, SkillCatalogRender, SkillCatalogRenderPolicy,
        SkillCatalogSnapshot,
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

/// Explicit host provenance for a real unappended User's Session. New requires
/// the host's actual unsaved Session; storage absence alone cannot create one.
pub enum SkillInputSession<'a> {
    New(&'a bamboo_agent_core::Session),
    Existing,
}

/// Unwired preappend factory. Returns ordinary data, never a reader/activation
/// grant. Call before acquiring a chat persistence owner; this factory acquires
/// that same existing owner itself, ahead of every publication guard.
pub struct SkillInputFactory {
    access: SkillToolAccess,
    resolver: Arc<dyn SkillCatalogCallerResolver>,
}
impl SkillInputFactory {
    pub fn new(
        manager: Arc<SkillManager>,
        config: Arc<RwLock<Config>>,
        sessions: bamboo_engine::SessionRepository,
        resolver: Arc<dyn SkillCatalogCallerResolver>,
    ) -> Self {
        Self {
            access: SkillToolAccess::new(manager, config, sessions),
            resolver,
        }
    }
    pub fn with_project_store(mut self, projects: Arc<bamboo_projects::ProjectStore>) -> Self {
        self.access = self.access.with_project_store(projects);
        self
    }
    pub async fn prepare_input(
        &self,
        ctx: &ToolCtx,
        user: &bamboo_agent_core::Message,
        host: SkillInputSession<'_>,
        selections: &[bamboo_skills::WorkflowSelection],
    ) -> Result<bamboo_engine::session_app::skill_input::PreparedSkillInput, ToolError> {
        use bamboo_engine::session_app::skill_input::{
            prepare_current_skill_input, SkillInputRestrictions, MAX_EXPLICIT_SKILLS,
        };
        let caller = self.resolver.resolve(ctx).await?;
        let invalid =
            || ToolError::Execution("Skill preappend caller/input is stale or invalid".into());
        if ctx.session_id() != Some(caller.session_id.as_str())
            || user.role != bamboo_agent_core::Role::User
            || user.id != caller.input_id
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
                .is_some_and(|intent| intent.input_id != user.id)
            || selections.len() > MAX_EXPLICIT_SKILLS
        {
            return Err(invalid());
        }
        let requested = selections
            .iter()
            .map(|selection| selection.id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        if caller
            .invocation
            .as_ref()
            .is_some_and(|intent| intent.skills != requested)
            || (!requested.is_empty() && caller.invocation.is_none())
        {
            return Err(invalid());
        }
        // A fallible direct storage read cannot turn a failed existing lookup
        // into New. Classification is independent of the cache's fast path.
        let first = self
            .access
            .session_repo
            .storage()
            .load_session(&caller.session_id)
            .await
            .map_err(|error| ToolError::Execution(error.to_string()))?;
        let (session, is_new) = match (host, first) {
            (SkillInputSession::New(session), None)
                if session.id == caller.session_id
                    && self.access.session_repo.cache().get(&session.id).is_none() =>
            {
                (session.clone(), true)
            }
            (SkillInputSession::Existing, Some(session)) if session.id == caller.session_id => {
                let session = self
                    .access
                    .session_repo
                    .try_load(&session.id)
                    .await
                    .map_err(|error| ToolError::Execution(error.to_string()))?
                    .ok_or_else(|| {
                        ToolError::Execution("Skill existing Session is unavailable".into())
                    })?;
                (session, false)
            }
            _ => {
                return Err(ToolError::Execution(
                    "Skill host New/existing Session provenance does not match storage".into(),
                ))
            }
        };
        if session.messages.iter().any(|message| message.id == user.id) {
            return Err(ToolError::Execution(
                "Skill input is already appended or its ID collides".into(),
            ));
        }
        let access = SkillCatalogEligibility {
            ceiling: caller.ceiling.clone(),
            explicit: requested,
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
        // Resolve aliases before choosing a store: legacy alias caching exists
        // for pinned activations and cannot authorize a new current input.
        let scope = canonical_input_scope(&self.access, &session)?;
        let expected_scope = (scope.workspace.clone(), scope.project.clone());
        let store = self.access.store_for_scope(scope).await?;
        let prepared = store
            .prepare_current_input_store(caller.mode.as_deref())
            .await
            .map_err(|error| ToolError::Execution(error.to_string()))?;
        prepared
            .validate_scope(
                expected_scope.1.as_ref().map(|(_, home)| home.as_path()),
                expected_scope.0.as_deref(),
            )
            .map_err(|error| ToolError::Execution(error.to_string()))?;
        let fresh = self.resolver.resolve(ctx).await?;
        let config = self.access.config.read().await;
        if fingerprint(&caller)? != fingerprint(&fresh)?
            || access.disabled != config.disabled_skill_ids().into_iter().collect()
        {
            return Err(ToolError::Execution(
                "Skill host caller/config changed during preparation".into(),
            ));
        }
        let _owner = self
            .access
            .session_repo
            .persistence()
            .acquire_lock(&session.id)
            .await;
        let sessions = self.access.session_repo.clone();
        // Compare JSON structure: Session contains HashMaps whose iteration
        // order legitimately changes across independent durable decodes.
        let expected = serde_json::to_value(&session).map_err(json_error)?;
        let user = user.clone();
        let restrictions = access.clone();
        let host_access = self.access.clone();
        prepared
            .with_current_inputs(&access, selections, move |publication| {
                Box::pin(async move {
                    // Last await: do not call try_load while the owner/publication are
                    // held. That reentry deadlocks on a legitimate cache miss.
                    let current = sessions
                        .storage()
                        .load_session(&caller.session_id)
                        .await
                        .map_err(|error| {
                            bamboo_skills::SkillError::Validation(error.to_string())
                        })?;
                    let matches = if is_new {
                        current.is_none() && sessions.cache().get(&caller.session_id).is_none()
                    } else {
                        current.as_ref().is_some_and(|current| {
                            serde_json::to_value(current).is_ok_and(|actual| actual == expected)
                        })
                    };
                    if !matches {
                        return Err(bamboo_skills::SkillError::Validation(
                            "Skill Session changed during preparation".into(),
                        ));
                    }
                    // Synchronous final scope resolution after the last await.
                    // An unchanged Session path may now resolve elsewhere.
                    let current_scope =
                        canonical_input_scope(&host_access, &session).map_err(|error| {
                            bamboo_skills::SkillError::Validation(error.to_string())
                        })?;
                    if (current_scope.workspace, current_scope.project) != expected_scope {
                        return Err(bamboo_skills::SkillError::Validation(
                            "Skill Project/workspace scope changed during preparation".into(),
                        ));
                    }
                    publication.validate_current()?;
                    let caller_intent = caller.invocation.is_some();
                    let caller = SkillInputRestrictions {
                        input_id: &caller.input_id,
                        ceiling: restrictions.ceiling.as_ref(),
                        disabled: &restrictions.disabled,
                        root_ultra: restrictions.deny_all,
                        mode: caller.mode.as_deref(),
                    };
                    prepare_current_skill_input(
                        &user,
                        Ok(&caller),
                        caller_intent.then_some((user.id.as_str(), publication.inputs())),
                    )
                    .map_err(bamboo_skills::SkillError::Validation)
                })
            })
            .await
            .map_err(|error| ToolError::Execution(error.to_string()))
    }
}

// No await or store/cache mutation: compare the actual canonical scope at the
// acceptance boundary while the Session owner and publication remain held.
fn canonical_input_scope(
    access: &SkillToolAccess,
    session: &bamboo_agent_core::Session,
) -> Result<super::SessionSkillScope, ToolError> {
    let mut scope = access.skill_scope_for_session(session)?;
    let resolve = |path: &std::path::Path| {
        std::fs::canonicalize(path).map_err(|error| {
            ToolError::Execution(format!("Skill input scope is unavailable: {error}"))
        })
    };
    scope.workspace = scope.workspace.as_deref().map(resolve).transpose()?;
    if let Some((_, home)) = &mut scope.project {
        *home = resolve(home)?;
    }
    Ok(scope)
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
        self.selected_prepared(ctx, &prepared, package, resource)
            .await
    }

    async fn selected_prepared(
        &self,
        ctx: &ToolCtx,
        prepared: &SelectedCatalogContext,
        package: &str,
        resource: &str,
    ) -> Result<SelectedSkillSource, ToolError> {
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
        self.recheck(ctx, prepared).await?;
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
        self.probe_prepared(ctx, &prepared, selected).await
    }

    async fn probe_prepared(
        &self,
        ctx: &ToolCtx,
        prepared: &SelectedCatalogContext,
        selected: &SelectedSkillSource,
    ) -> Result<(), ToolError> {
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
        self.recheck(ctx, prepared).await?;
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

// Count serialized bytes without retaining cloned response/provider strings.
#[derive(Default)]
struct WireCount {
    bytes: usize,
    escaped: usize,
}
impl std::io::Write for WireCount {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.bytes += bytes.len();
        self.escaped += bytes
            .iter()
            .map(|byte| match byte {
                b'"' | b'\\' | b'\n' | b'\r' | b'\t' | 8 | 12 => 2,
                0..=31 => 6,
                _ => 1,
            })
            .sum::<usize>();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn serialized_size(value: &impl Serialize) -> Result<usize, ToolError> {
    let mut count = WireCount::default();
    serde_json::to_writer(&mut count, value).map_err(json_error)?;
    Ok(count.bytes)
}
#[derive(Serialize)]
struct OpenAiOutput<'a, T: Serialize> {
    #[serde(rename = "type")]
    kind: &'static str,
    call_id: &'a str,
    output: T,
}
#[derive(Serialize)]
struct CacheText<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    text: &'a str,
    prompt_cache_breakpoint: ExplicitCache,
}
#[derive(Serialize)]
struct ExplicitCache {
    mode: &'static str,
}
#[derive(Serialize)]
struct AnthropicOutput<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    tool_use_id: &'a str,
    content: &'a str,
    is_error: bool,
    cache_control: AnthropicCache,
}
#[derive(Serialize)]
struct AnthropicCache {
    #[serde(rename = "type")]
    kind: &'static str,
    ttl: &'static str,
}
#[derive(Serialize)]
struct ChatOutput<'a> {
    role: &'static str,
    tool_call_id: &'a str,
    content: &'a str,
}
#[derive(Serialize)]
struct GeminiOutput<'a> {
    #[serde(rename = "functionResponse")]
    function_response: GeminiResponse<'a>,
}
#[derive(Serialize)]
struct GeminiResponse<'a> {
    name: &'a str,
    response: Option<()>,
}
// Charge the actual largest page-bearing block, including 1h cache TTL.
// List/read construct compact JSON objects. Gemini keeps that object, so its
// raw serialized length replaces null in a borrowed skeleton without parsing,
// retaining or cloning the page; all other providers carry an escaped string.
pub(super) fn page_size(result: &ToolResult, call_id: &str) -> Result<usize, ToolError> {
    Ok([
        serialized_size(result)?,
        serialized_size(&ChatOutput {
            role: "tool",
            tool_call_id: call_id,
            content: &result.result,
        })?,
        serialized_size(&GeminiOutput {
            function_response: GeminiResponse {
                name: call_id,
                response: None,
            },
        })? - 4
            + result.result.len(),
        serialized_size(&OpenAiOutput {
            kind: "function_call_output",
            call_id,
            output: result.result.as_str(),
        })?,
        serialized_size(&OpenAiOutput {
            kind: "function_call_output",
            call_id,
            output: [CacheText {
                kind: "input_text",
                text: &result.result,
                prompt_cache_breakpoint: ExplicitCache { mode: "explicit" },
            }],
        })?,
        serialized_size(&AnthropicOutput {
            kind: "tool_result",
            tool_use_id: call_id,
            content: &result.result,
            is_error: false,
            cache_control: AnthropicCache {
                kind: "ephemeral",
                ttl: "1h",
            },
        })?,
    ]
    .into_iter()
    .max()
    .unwrap_or(0))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadArgs {
    package: String,
    resource: Option<String>,
    cursor: Option<String>,
}
#[derive(Serialize)]
struct ReadResponse<'a> {
    package: &'a str,
    resource: &'a str,
    contents: &'a str,
    next_cursor: Option<String>,
}

/// Complete-file paging API; deliberately unregistered until runtime migration.
/// It shares list/render's mandatory fresh host resolver and selected source path.
pub struct SkillsReadTool {
    catalog: SkillsListTool,
    cache: SelectedSkillReadCache,
}
impl SkillsReadTool {
    pub fn new(catalog: SkillsListTool) -> Self {
        Self {
            catalog,
            cache: SelectedSkillReadCache::default(),
        }
    }
    fn validate_handle(value: &str) -> Result<(), ToolError> {
        if value.is_empty() || value.len() > MAX_HANDLE_BYTES || value.chars().any(char::is_control)
        {
            return Err(ToolError::InvalidArguments(
                "skills_read handle is empty, oversized or contains controls".into(),
            ));
        }
        Ok(())
    }
    fn response<'a>(
        entry: &'a CachedSkillRead,
        contents: &'a str,
        offset: Option<usize>,
    ) -> ReadResponse<'a> {
        ReadResponse {
            package: &entry.snapshot.package,
            resource: &entry.snapshot.resource,
            contents,
            next_cursor: offset.map(|offset| entry.cursor(offset)),
        }
    }
}
#[async_trait]
impl Tool for SkillsReadTool {
    fn name(&self) -> &str {
        "skills_read"
    }
    fn description(&self) -> &str {
        "Read a selected Skill package file; continue next_cursor until complete EOF."
    }
    fn parameters_schema(&self) -> Value {
        json!({"type":"object","properties":{"package":{"type":"string","maxLength":MAX_HANDLE_BYTES},"resource":{"type":"string","maxLength":MAX_HANDLE_BYTES},"cursor":{"type":"string","maxLength":MAX_HANDLE_BYTES}},"required":["package"],"additionalProperties":false})
    }
    fn classify(&self, _: &Value) -> ToolClass {
        ToolClass::READONLY_PARALLEL
    }
    async fn invoke(&self, args: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        let args: ReadArgs = serde_json::from_value(args)
            .map_err(|error| ToolError::InvalidArguments(error.to_string()))?;
        Self::validate_handle(&args.package)?;
        let resource = args.resource.as_deref().unwrap_or("SKILL.md");
        Self::validate_handle(resource)?;
        Self::validate_handle(&ctx.tool_call_id)?;
        if let Some(cursor) = &args.cursor {
            Self::validate_handle(cursor)?;
        }
        // One context, store and authorizer, resolved before cache access.
        let prepared = self.catalog.metadata(&ctx).await?;
        if !prepared
            .snapshot
            .entries
            .iter()
            .any(|entry| entry.package == args.package)
        {
            return Err(ToolError::Execution(
                "selected Skill is not currently eligible".into(),
            ));
        }
        let (entry, start) = if let Some(cursor) = &args.cursor {
            let (entry, offset) = self.cache.lookup(cursor).map_err(skill_error)?;
            if entry.snapshot.package != args.package || entry.snapshot.resource != resource {
                return Err(ToolError::InvalidArguments(
                    "skills_read cursor resource changed".into(),
                ));
            }
            (entry, offset)
        } else {
            self.cache.clear();
            let selected = self
                .catalog
                .selected_prepared(&ctx, &prepared, &args.package, resource)
                .await?;
            (self.cache.admit(selected.snapshot, selected.identity), 0)
        };
        let budget = prepared.caller.response_bytes.min(MAX_SKILLS_LIST_BYTES);
        let overhead = page_size(&ToolResult::text(true, ""), &ctx.tool_call_id)?;
        let page = entry
            .snapshot
            .page_response(
                start,
                budget,
                |contents, offset| {
                    let mut count = WireCount::default();
                    serde_json::to_writer(&mut count, &Self::response(&entry, contents, offset))
                        .map_err(|e| bamboo_skills::SkillError::Validation(e.to_string()))?;
                    Ok(overhead.saturating_add(count.escaped))
                },
                |contents, offset, writer| {
                    serde_json::to_writer(writer, &Self::response(&entry, contents, offset))
                        .map_err(|e| bamboo_skills::SkillError::Validation(e.to_string()))
                },
            )
            .map_err(skill_error)?;
        // Keep the page and selected allocation charged through final fresh checks.
        let selected = SelectedSkillSource {
            snapshot: entry.snapshot.clone(),
            identity: entry.identity.clone(),
        };
        self.catalog
            .probe_prepared(&ctx, &prepared, &selected)
            .await?;
        if !self.cache.contains(&entry) {
            return Err(ToolError::Execution(
                "skills_read cursor was evicted during read".into(),
            ));
        }
        Ok(ToolOutcome::Completed(ToolResult::text(
            true,
            page.into_text(),
        )))
    }
}
fn skill_error(error: bamboo_skills::SkillError) -> ToolError {
    ToolError::Execution(error.to_string())
}
