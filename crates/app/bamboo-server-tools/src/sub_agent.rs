use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

use bamboo_agent_core::tools::{Tool, ToolCtx, ToolError, ToolOutcome, ToolResult};
use bamboo_domain::session::runtime_state::ChildWaitPolicy;
use bamboo_domain::ReasoningEffort;
use bamboo_engine::session_app::child_session::{
    self, ChildSessionError, ChildSessionPort, CreateChildInput, ModelCatalogPort,
    SubagentResolutionPort,
};

// ---------------------------------------------------------------------------
// Args enum
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum SubAgentArgs {
    Create {
        #[serde(default)]
        title: Option<String>,
        #[serde(default)]
        description: String,
        #[serde(default)]
        responsibility: Option<String>,
        prompt: String,
        /// Optional free-text label for this child (cosmetic only — used for
        /// display and as the warm-worker reuse key). It has NO effect on the
        /// child's tools or system prompt; every sub-agent is a full agent.
        #[serde(default)]
        subagent_type: Option<String>,
        /// Working directory for the child. Optional: defaults to the parent
        /// session's workspace when omitted.
        #[serde(default)]
        workspace: Option<String>,
        #[serde(default)]
        auto_run: Option<bool>,
        /// When `true`, the parent suspends immediately and waits for THIS child
        /// to finish (the legacy one-shot behavior). Defaults to `false`:
        /// `create` runs the child in the background and returns right away so
        /// the parent can spawn more children. Call `action=wait` once, after
        /// spawning everything, to suspend until they finish.
        #[serde(default)]
        wait: Option<bool>,
        /// Optional reasoning effort for the child session. When omitted,
        /// the child stays at `None` so the provider's default applies
        /// (it does NOT inherit the parent's reasoning_effort). The LLM
        /// should pass an explicit value (e.g. `"low"` for cheap fan-outs,
        /// `"high"`/`"max"` for hard reasoning) when it has a preference.
        #[serde(default)]
        reasoning_effort: Option<ReasoningEffort>,
        /// Optional explicit model for the child, `"provider:model"`
        /// (e.g. `"anthropic:claude-sonnet-4-6"`) or a bare model id (resolved
        /// against the parent's provider, falling back to the default
        /// provider). Takes precedence over per-`subagent_type` model routing.
        /// Call `list_models` to see what is available.
        #[serde(default)]
        model: Option<String>,
        /// Lifecycle: `"oneshot"` (default) creates a fresh throwaway child for
        /// this task. `"resident"` reuses a long-lived agent identified by
        /// `name` (scoped to this conversation): the FIRST resident create spins
        /// one up; later creates with the same `name` route the new task to that
        /// same agent instead of spawning another — so repeated similar work
        /// (e.g. an "essayist" handling many essays) stays one agent/one entry,
        /// not N. Use resident for recurring task types; one-shot for
        /// independent throwaway work.
        #[serde(default)]
        lifecycle: Option<String>,
        /// Resident reuse key (required when `lifecycle="resident"`; defaults to
        /// `subagent_type`). The stable name of the resident agent to create or
        /// reuse, e.g. `"essayist"`.
        #[serde(default)]
        name: Option<String>,
        /// For a resident agent, how successive tasks treat prior context:
        /// `"reset"` (default — each task is independent, prior context cleared)
        /// or `"accumulate"` (the agent remembers earlier tasks). Set on first
        /// create; honored on reuse.
        #[serde(default)]
        context: Option<String>,
        /// Phase 3 model-controllable context fork: when `> 0`, carry the last N
        /// of the parent's messages into the child's task brief. `None`/0 (the
        /// default) gives the child a clean, freshly-seeded context.
        #[serde(default)]
        fork_last_messages: Option<usize>,
    },
    /// Suspend the parent run until its background child sessions finish.
    ///
    /// Spawn children with `action=create` (which no longer suspends), then call
    /// this once. By default it waits on every currently-active child; pass
    /// explicit `child_session_ids` to wait on a subset. If no children are
    /// active it is a no-op (the parent keeps running).
    Wait {
        #[serde(default)]
        child_session_ids: Option<Vec<String>>,
        /// Wait policy: `all` (default) resumes when every tracked child is
        /// terminal; `any` resumes on the first; `first_error` resumes early on
        /// any error/timeout/cancel.
        #[serde(default)]
        wait_for: Option<ChildWaitPolicy>,
    },
    List,
    Get {
        child_session_id: String,
    },
    Update {
        child_session_id: String,
        #[serde(default)]
        title: Option<String>,
        #[serde(default)]
        responsibility: Option<String>,
        #[serde(default)]
        prompt: Option<String>,
        #[serde(default)]
        subagent_type: Option<String>,
        #[serde(default)]
        reset_after_update: Option<bool>,
        #[serde(default)]
        auto_run: Option<bool>,
        /// Optional reasoning effort to apply to the existing child session.
        /// `Some(level)` overrides the current value; `None` (the default)
        /// leaves it unchanged.
        #[serde(default)]
        reasoning_effort: Option<ReasoningEffort>,
    },
    Run {
        child_session_id: String,
        #[serde(default)]
        reset_to_last_user: Option<bool>,
    },
    SendMessage {
        child_session_id: String,
        message: String,
        #[serde(default)]
        auto_run: Option<bool>,
        #[serde(default)]
        interrupt_running: Option<bool>,
    },
    Cancel {
        child_session_id: String,
    },
    Delete {
        child_session_id: String,
    },
    /// Enumerate the models the parent can pin a child to via
    /// `create.model`. Read-only; best-effort per configured provider.
    ListModels,
}

// ---------------------------------------------------------------------------
// Normalization helpers (ported from legacy SpawnSessionTool)
// ---------------------------------------------------------------------------

fn normalize_required_text(value: Option<String>, field_name: &str) -> Result<String, ToolError> {
    let Some(value) = value else {
        return Err(ToolError::InvalidArguments(format!(
            "{field_name} must be non-empty"
        )));
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ToolError::InvalidArguments(format!(
            "{field_name} must be non-empty"
        )));
    }
    Ok(trimmed.to_string())
}

fn normalize_title(title: Option<String>, legacy_description: String) -> Result<String, ToolError> {
    let title = title.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    });
    let legacy_description = {
        let trimmed = legacy_description.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    };
    normalize_required_text(title.or(legacy_description), "title")
}

fn tool_result(value: serde_json::Value) -> Result<ToolResult, ToolError> {
    Ok(ToolResult {
        success: true,
        result: value.to_string(),
        display_preference: Some("Collapsible".to_string()),
        images: Vec::new(),
    })
}

fn waiting_for_children_tool_result(mut value: serde_json::Value) -> Result<ToolResult, ToolError> {
    if let Some(object) = value.as_object_mut() {
        object.insert("runtime_control".to_string(), json!("waiting_for_children"));
        // Don't clobber a caller-provided policy (e.g. action=wait with
        // wait_for=any); only default it when absent.
        object
            .entry("wait_for".to_string())
            .or_insert_with(|| json!("all"));
        object.insert(
            "note".to_string(),
            json!("Child session queued. The parent run is suspended and will resume automatically when the child finishes or times out."),
        );
    }

    Ok(ToolResult {
        success: true,
        result: value.to_string(),
        display_preference: Some("runtime_control:waiting_for_children".to_string()),
        images: Vec::new(),
    })
}

/// Map a `ChildSessionError` to a `ToolError`.
fn tool_error_from_child_session(error: ChildSessionError) -> ToolError {
    match error {
        ChildSessionError::NotFound(id) => ToolError::Execution(format!("session not found: {id}")),
        ChildSessionError::NotRootSession(id) => {
            ToolError::Execution(format!("session is not a root session: {id}"))
        }
        ChildSessionError::InvalidArguments(msg) => ToolError::InvalidArguments(msg),
        ChildSessionError::Execution(msg) => ToolError::Execution(msg),
        other => ToolError::Execution(other.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Tool struct
// ---------------------------------------------------------------------------

pub struct SubAgentTool {
    /// Child-session CRUD/lifecycle operations (load/save/run/cancel/…).
    sessions: Arc<dyn ChildSessionPort>,
    /// Subagent-type resolution (model, runtime metadata, active ids).
    resolver: Arc<dyn SubagentResolutionPort>,
    /// Optional model catalog consulted by `action=list_models` and used to
    /// resolve a bare `create.model` id to a provider. `None` keeps the tool
    /// constructible without a live provider registry (tests, embedded use).
    catalog: Option<Arc<dyn ModelCatalogPort>>,
}

impl SubAgentTool {
    pub fn new(
        sessions: Arc<dyn ChildSessionPort>,
        resolver: Arc<dyn SubagentResolutionPort>,
    ) -> Self {
        Self {
            sessions,
            resolver,
            catalog: None,
        }
    }

    /// Attach a model catalog, enabling `action=list_models` and bare-model
    /// resolution for `create.model`.
    pub fn with_model_catalog(mut self, catalog: Arc<dyn ModelCatalogPort>) -> Self {
        self.catalog = Some(catalog);
        self
    }
}

/// Parse an explicit `create.model` spec into a `ProviderModelRef`.
///
/// `"provider:model"` is explicit; a bare model id falls back to the parent
/// session's provider, then the catalog's default provider.
fn parse_model_spec(
    spec: &str,
    parent: &bamboo_agent_core::Session,
    default_provider: Option<String>,
) -> Result<bamboo_domain::ProviderModelRef, ToolError> {
    let spec = spec.trim();
    if spec.is_empty() {
        return Err(ToolError::InvalidArguments(
            "model must be non-empty when provided".to_string(),
        ));
    }
    if let Some((provider, model)) = spec.split_once(':') {
        let (provider, model) = (provider.trim(), model.trim());
        if provider.is_empty() || model.is_empty() {
            return Err(ToolError::InvalidArguments(format!(
                "model '{spec}' must be 'provider:model' with both parts non-empty"
            )));
        }
        return Ok(bamboo_domain::ProviderModelRef::new(provider, model));
    }
    // Bare model id: inherit the parent's provider, else the default provider.
    let provider = parent
        .model_ref
        .as_ref()
        .map(|r| r.provider.clone())
        .filter(|p| !p.trim().is_empty())
        .or(default_provider)
        .ok_or_else(|| {
            ToolError::InvalidArguments(format!(
                "model '{spec}' has no provider prefix and no default provider is known; \
                 use 'provider:model' (see action=list_models)"
            ))
        })?;
    Ok(bamboo_domain::ProviderModelRef::new(provider, spec))
}

/// Default max nesting depth for sub-agent spawning (Phase 6: direct nested
/// execution). An agent at `spawn_depth >= this` may not create more children,
/// bounding worker→worker→… recursion. Root orchestrator = depth 0, so this
/// allows 4 levels of sub-agents below the root.
pub const DEFAULT_MAX_SPAWN_DEPTH: u32 = 4;

/// The `SubAgent` tool description. Exposed standalone so a nested worker's
/// SubAgent proxy can advertise the identical tool to its own LLM (no drift).
pub fn subagent_tool_description() -> &'static str {
    "Create, inspect, and manage child sessions for explicitly requested delegated, parallel, or sub-agent work. A child session is a full agent that runs independently under the current root session with its own conversation context and the full toolset, streams progress back to the parent via sub_agent_* events, and can be reopened from the Sub-agents panel. \
PARALLEL FAN-OUT (important): action=create now runs the child in the BACKGROUND and returns immediately WITHOUT suspending the parent. To launch several agents in parallel, call create once per child (ideally several creates in a single turn), then call action=wait ONCE to suspend until they finish. Do NOT pass wait=true on each create for parallel work — that would serialize them (suspend after the first). action=wait defaults to waiting on every active child; if you forget to call it, the runtime auto-waits at the end of the turn so results are never lost. \
Use list/get to inspect existing children; use update/run/send_message/cancel/delete to manage existing children. Use only when the user explicitly asks for delegation/parallelism or when a side task would otherwise flood the main context. Do not use for simple one-step tasks. IMPORTANT: When a child fails or needs redirection, prefer send_message over creating a duplicate child. Use list before create to avoid spawning redundant children."
}

/// The `SubAgent` parameters schema. Exposed standalone (mirroring
/// [`subagent_tool_description`]) so a nested worker's SubAgent proxy advertises
/// the IDENTICAL schema to its own LLM — no drift between the real tool and the
/// proxy.
pub fn subagent_parameters_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["create", "wait", "list", "get", "update", "run", "send_message", "cancel", "delete", "list_models"],
                "description": "Sub-agent lifecycle operation. To run work in parallel: call create once per child (this no longer suspends the parent — children run in the background), then call wait ONCE to suspend until they all finish. Use list/get to inspect; update/run/send_message/cancel/delete to manage existing children; list_models to enumerate the models you can pin a child to via create.model. \
        A create call requires: title, responsibility, and prompt (workspace is optional and defaults to the parent's workspace). EXAMPLE create: {\"action\":\"create\",\"title\":\"Analyze auth module\",\"responsibility\":\"Map the auth flow and list its public API\",\"prompt\":\"Read crates/auth/src/lib.rs, summarize the login flow, and list every pub fn.\",\"workspace\":\"/abs/path/to/repo\"}. Then EXAMPLE wait: {\"action\":\"wait\"}."
            },
            "child_session_id": {
                "type": "string",
                "description": "Existing child session id. Required for get/update/run/send_message/cancel/delete."
            },
            "child_session_ids": {
                "type": "array",
                "items": { "type": "string" },
                "description": "For wait: optional explicit subset of child sessions to wait on. Omit to wait on every currently-active child."
            },
            "wait_for": {
                "type": "string",
                "enum": ["all", "any", "first_error"],
                "description": "For wait: resume policy. all (default) resumes when every tracked child is done; any resumes on the first; first_error resumes early on any error/timeout/cancel."
            },
            "wait": {
                "type": "boolean",
                "description": "For create: if true, suspend immediately and wait for just THIS child (legacy one-shot behavior). Defaults to false — create returns immediately and the child runs in the background; suspend later with action=wait."
            },
            "title": {
                "type": "string",
                "description": "Short title for a new or updated child session. Required for create. Displayed in the Sub-agents panel."
            },
            "description": {
                "type": "string",
                "description": "Legacy alias of title; prefer title."
            },
            "responsibility": {
                "type": "string",
                "description": "Single explicit responsibility for the child session. Required for create. Keep this narrow and non-overlapping with other child sessions."
            },
            "prompt": {
                "type": "string",
                "description": "Detailed task instructions, context, constraints, and expected output for the child session. Required for create; optional for update."
            },
            "subagent_type": {
                "type": "string",
                "description": "For create: an optional free-text label for this child (e.g. \"researcher\", \"impl\"), used only for display and as the warm-worker reuse key. Cosmetic — it does NOT change the child's tools or system prompt; every sub-agent is a full agent. Optional; omit it if you have no useful label."
            },
            "workspace": {
                "type": "string",
                "description": "For create: absolute path to the child session's working directory for file operations. Optional — defaults to the parent session's workspace when omitted."
            },
            "auto_run": {
                "type": "boolean",
                "description": "For create/send_message/update: whether to enqueue the child session immediately. Defaults to true for create/send_message and false for update."
            },
            "fork_last_messages": {
                "type": "integer",
                "minimum": 0,
                "description": "For create: model-controllable context fork. When > 0, the last N messages of YOUR (the parent's) conversation are carried into the child's task brief as a 'Forked context from parent' block, so the child starts with the recent context it needs. Omit/0 (default) gives the child a clean, freshly-seeded context. Use a small N (e.g. 2-6) to share just the immediately relevant turns; omit it when the task brief is already self-contained."
            },
            "reset_after_update": {
                "type": "boolean",
                "description": "For update: whether to truncate messages after refreshed assignment. Defaults to true."
            },
            "reset_to_last_user": {
                "type": "boolean",
                "description": "For run: whether to truncate messages after the last user message before rerun. Defaults to true."
            },
            "message": {
                "type": "string",
                "description": "Follow-up instruction to append as a new user message for send_message. Required for send_message."
            },
            "interrupt_running": {
                "type": "boolean",
                "description": "For send_message/cancel: if true, cancel a currently running child session before appending or returning. Defaults to false for send_message. When false on a running child, the message is queued and will be picked up at the next turn boundary without canceling progress."
            },
            "reasoning_effort": {
                "type": "string",
                "enum": ["low", "medium", "high", "xhigh", "max"],
                "description": "For create/update: reasoning effort level applied to the child session's own LLM calls. Use \"low\" for trivial fan-outs (e.g. simple lookups), \"medium\"/\"high\" for normal coding/analysis, \"xhigh\"/\"max\" for deep reasoning tasks. Omit to leave at provider default; the child does NOT inherit the parent's reasoning_effort."
            },
            "model": {
                "type": "string",
                "description": "For create: explicit model for the child as 'provider:model' (e.g. 'anthropic:claude-sonnet-4-6'), or a bare model id to use the parent's provider. Takes precedence over per-subagent_type model routing. Pick a cheaper/faster model for simple fan-outs and a stronger model for hard reasoning. Call list_models first to see what is available; omit to use the configured default for the given subagent_type label."
            },
            "lifecycle": {
                "type": "string",
                "enum": ["oneshot", "resident"],
                "description": "For create: 'oneshot' (default) spins up a fresh throwaway child for this task. 'resident' reuses ONE long-lived agent (identified by 'name', scoped to this conversation) across many tasks — the first resident create spins it up, later creates with the same name route the new task to that same agent instead of spawning another. Use resident for recurring task types (e.g. an 'essayist' that writes many essays — one agent, one panel entry, not N); use oneshot for independent throwaway work."
            },
            "name": {
                "type": "string",
                "description": "For create with lifecycle=resident: the resident agent's stable reuse key, e.g. 'essayist'. Required to reuse a resident; defaults to subagent_type when omitted. Reusing the same name routes the new task to the existing resident agent."
            },
            "context": {
                "type": "string",
                "enum": ["reset", "accumulate"],
                "description": "For create with lifecycle=resident: how the resident treats prior tasks. 'reset' (default) makes each task independent (clears prior context). 'accumulate' makes the agent remember earlier tasks (useful for a researcher building up knowledge). Set on first create; honored on reuse."
            }
        },
        "required": ["action"],
        "additionalProperties": false
    })
}

#[async_trait]
impl Tool for SubAgentTool {
    fn name(&self) -> &str {
        "SubAgent"
    }

    fn description(&self) -> &str {
        subagent_tool_description()
    }

    fn parameters_schema(&self) -> serde_json::Value {
        subagent_parameters_schema()
    }

    async fn invoke(
        &self,
        args: serde_json::Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let parent_session_id = ctx.session_id().ok_or_else(|| {
            ToolError::Execution("SubAgent requires a session_id in tool context".to_string())
        })?;

        // Backward compatibility: legacy SubAgent calls did not include an
        // "action" field and always meant "create". If action is missing,
        // default to "create" before deserializing the tagged enum.
        let mut args = args;
        if args.get("action").is_none() {
            args["action"] = json!("create");
        }

        let parsed: SubAgentArgs = serde_json::from_value(args).map_err(|error| {
            ToolError::InvalidArguments(format!("Invalid SubAgent args: {error}"))
        })?;

        // `list_models` is read-only and session-independent.
        if let SubAgentArgs::ListModels = parsed {
            let Some(catalog) = self.catalog.as_ref() else {
                return Err(ToolError::Execution(
                    "model catalog is not configured on this server".to_string(),
                ));
            };
            let providers = catalog.list_models().await;
            return tool_result(json!({
                "default_provider": catalog.default_provider(),
                "providers": providers,
                "usage": "Pass create.model as 'provider:model' (or a bare model id to use the parent's provider).",
            }))
            .map(ToolOutcome::Completed);
        }

        let parent = self
            .sessions
            .as_ref()
            .load_root_session(parent_session_id)
            .await
            .map_err(tool_error_from_child_session)?;

        match parsed {
            SubAgentArgs::Create {
                title,
                description,
                responsibility,
                prompt,
                subagent_type,
                workspace,
                auto_run,
                wait,
                reasoning_effort,
                model,
                lifecycle,
                name,
                context,
                fork_last_messages,
            } => {
                // Phase 6: enforce the max nesting-depth cap. `parent` is this
                // agent's run session; its `spawn_depth` is the current nesting
                // level (workers stamp it from the actor spec, so it accumulates
                // across the actor boundary). Refuse to spawn beyond the cap so
                // worker→worker→… recursion is bounded.
                if parent.spawn_depth >= DEFAULT_MAX_SPAWN_DEPTH {
                    return Err(ToolError::InvalidArguments(format!(
                        "spawn depth limit ({}) reached: this agent is at depth {} and cannot create more sub-agents. Finish the work here, or delegate to a sibling.",
                        DEFAULT_MAX_SPAWN_DEPTH, parent.spawn_depth
                    )));
                }
                let title = normalize_title(title, description)?;
                let responsibility = normalize_required_text(responsibility, "responsibility")?;
                let prompt = normalize_required_text(Some(prompt), "prompt")?;
                // subagent_type is an optional cosmetic label only (display +
                // warm-worker reuse key); it has no behavioral effect. An
                // omitted/blank value falls back to the neutral "worker" label.
                let subagent_type = subagent_type
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
                    .unwrap_or_else(|| "worker".to_string());
                // workspace is optional: default to the parent's workspace.
                let workspace = workspace
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
                    .or_else(|| parent.workspace.clone())
                    .ok_or_else(|| {
                        ToolError::InvalidArguments(
                            "workspace must be non-empty (parent has no workspace to inherit)"
                                .to_string(),
                        )
                    })?;

                if parent.model.trim().is_empty() {
                    return Err(ToolError::Execution(
                        "parent session model is empty".to_string(),
                    ));
                }

                let should_auto_run = auto_run.unwrap_or(true);

                // Resident routing: `lifecycle="resident"` reuses the existing
                // resident agent of the same `name` in this root tree (one stable
                // agent/entry for recurring work) instead of minting a new child.
                // `reset` (default) replaces the resident's task and reruns it;
                // `accumulate` appends the task to its history. The FIRST resident
                // create (none found yet) falls through to a normal create tagged
                // as resident.
                let is_resident = lifecycle.as_deref().map(str::trim) == Some("resident");
                let resident_name = is_resident.then(|| {
                    name.as_deref()
                        .map(str::trim)
                        .filter(|n| !n.is_empty())
                        .map(str::to_string)
                        .unwrap_or_else(|| subagent_type.clone())
                });
                let resident_context = context
                    .as_deref()
                    .map(str::trim)
                    .filter(|c| matches!(*c, "reset" | "accumulate"))
                    .unwrap_or("reset")
                    .to_string();
                let existing_resident = match resident_name.as_deref() {
                    Some(rname) => {
                        // Children are created with `root_session_id == parent.id`,
                        // so the parent's id is the tree root key for the lookup.
                        self.sessions.find_resident_child(&parent.id, rname).await
                    }
                    None => None,
                };

                let (child_session_id, child_model, reused) =
                    if let Some(existing_id) = existing_resident {
                        // A resident processes tasks serially. If it is still running
                        // a previous task, stop it first: otherwise `reset` would
                        // truncate the session while the runner writes back (a
                        // corrupting race + a possible duplicate spawn job), and
                        // `accumulate` would queue a message into a run that may end
                        // before picking it up (the task would never execute). After
                        // cancel the resident is idle, so both paths apply cleanly.
                        if self.sessions.is_child_running(&existing_id).await {
                            self.sessions
                                .cancel_child_run_and_wait(&existing_id)
                                .await
                                .map_err(tool_error_from_child_session)?;
                        }
                        // #74: re-seed the reused resident's posture from the LIVE
                        // parent. The resident-reuse path bypasses
                        // `create_child_action` (which seeds `bypass_permissions` /
                        // `no_human_approver` on the child's first run), so without
                        // this a resident created under one posture keeps a stale
                        // flag when reused under another (e.g. parent flipped from
                        // headless to interactive, or toggled bypass). Mirror BOTH
                        // flags so the reused resident matches the current parent.
                        {
                            let mut child = self
                                .sessions
                                .load_child_for_parent(&parent.id, &existing_id)
                                .await
                                .map_err(tool_error_from_child_session)?;
                            let (parent_bypass, parent_no_human) = parent
                                .agent_runtime_state
                                .as_ref()
                                .map(|s| (s.bypass_permissions, s.no_human_approver))
                                .unwrap_or((false, false));
                            let rs = child
                                .agent_runtime_state
                                .get_or_insert_with(bamboo_domain::AgentRuntimeState::default);
                            rs.bypass_permissions = parent_bypass;
                            rs.no_human_approver = parent_no_human;
                            // Authoritative flag write: persist the freshly
                            // mirrored posture as-is; the adopting save would
                            // revert bypass to the child's stale disk value. #540/#74.
                            self.sessions
                                .save_child_session_authoritative_flags(&mut child)
                                .await
                                .map_err(tool_error_from_child_session)?;
                        }
                        // Reuse: reset => update (truncate + new task) then rerun;
                        // accumulate => send the task as a new message (auto-runs).
                        if resident_context == "accumulate" {
                            child_session::send_message_to_child_action(
                                self.sessions.as_ref(),
                                &parent,
                                existing_id.clone(),
                                format!("# Task: {title}\n\n{responsibility}\n\n{prompt}"),
                                Some(should_auto_run),
                                Some(false),
                            )
                            .await
                            .map_err(tool_error_from_child_session)?;
                        } else {
                            child_session::update_child_action(
                                self.sessions.as_ref(),
                                &parent.id,
                                existing_id.clone(),
                                Some(title.clone()),
                                Some(responsibility.clone()),
                                Some(prompt.clone()),
                                Some(subagent_type.clone()),
                                Some(true),
                                reasoning_effort,
                            )
                            .await
                            .map_err(tool_error_from_child_session)?;
                            if should_auto_run {
                                let child = self
                                    .sessions
                                    .load_child_for_parent(&parent.id, &existing_id)
                                    .await
                                    .map_err(tool_error_from_child_session)?;
                                self.sessions
                                    .enqueue_child_run(&parent, &child)
                                    .await
                                    .map_err(tool_error_from_child_session)?;
                            }
                        }
                        let model = self
                            .sessions
                            .load_child_for_parent(&parent.id, &existing_id)
                            .await
                            .map(|c| c.model)
                            .unwrap_or_default();
                        (existing_id, model, true)
                    } else {
                        let child_id = Uuid::new_v4().to_string();
                        // Model precedence: explicit `model` arg > per-subagent_type
                        // routing (resolver) > engine defaults (None).
                        let model_ref_override =
                            match model.as_deref().map(str::trim).filter(|m| !m.is_empty()) {
                                Some(spec) => Some(parse_model_spec(
                                    spec,
                                    &parent,
                                    self.catalog.as_ref().map(|c| c.default_provider()),
                                )?),
                                None => self.resolver.resolve_subagent_model(&subagent_type).await,
                            };
                        let model_override = model_ref_override
                            .as_ref()
                            .map(|model_ref| model_ref.model.clone());
                        let runtime_metadata =
                            self.resolver.resolve_runtime_metadata(&subagent_type).await;
                        let result = child_session::create_child_action(
                            self.sessions.as_ref(),
                            CreateChildInput {
                                parent_session: parent.clone(),
                                child_id: child_id.clone(),
                                title: title.clone(),
                                responsibility: responsibility.clone(),
                                assignment_prompt: prompt.clone(),
                                subagent_type: subagent_type.clone(),
                                workspace: workspace.clone(),
                                model_override,
                                model_ref_override,
                                runtime_metadata,
                                auto_run: should_auto_run,
                                reasoning_effort,
                                lifecycle: resident_name.as_ref().map(|_| "resident".to_string()),
                                resident_name: resident_name.clone(),
                                resident_context: resident_name
                                    .as_ref()
                                    .map(|_| resident_context.clone()),
                                disabled_tools: None,
                                // Phase 3: model-controllable context fork — carry
                                // the last N parent messages into the child's brief.
                                context_fork: fork_last_messages.filter(|n| *n > 0),
                            },
                        )
                        .await
                        .map_err(tool_error_from_child_session)?;
                        (result.child_session_id, result.model, false)
                    };

                // Ensure index entry is visible immediately (best-effort).
                self.sessions.ensure_child_indexed(&child_session_id).await;

                ctx.emit_tool_token(if reused {
                    format!("Reused resident agent: {child_session_id}")
                } else {
                    format!("Spawned child session: {child_session_id}")
                })
                .await;

                // `wait=true` preserves the legacy one-shot behavior: register a
                // wait for THIS child and suspend now. Default (`wait=false`) runs
                // the child in the background and returns immediately, so the
                // parent can keep spawning; it suspends later via `action=wait`.
                let should_wait = should_auto_run && wait.unwrap_or(false);
                if should_wait {
                    self.sessions
                        .register_parent_wait_for_child(&parent.id, &child_session_id, None)
                        .await
                        .map_err(tool_error_from_child_session)?;
                }

                let status = if !should_auto_run {
                    "created"
                } else if should_wait {
                    "queued"
                } else {
                    "running_in_background"
                };
                let note = if should_wait {
                    "Child session queued (typically 30-120 seconds); the parent is suspended until it finishes. Use send_message (not create) to correct a child in place."
                } else if should_auto_run {
                    "Child session is running in the background (typically 30-120 seconds). Spawn any other children you need, then call action=wait once to suspend until they finish. Use send_message (not create) to correct a child in place."
                } else {
                    "Child session created (not started). Use action=run to start it. Use send_message (not create) to correct a child in place."
                };
                let payload = json!({
                    "title": title.clone(),
                    "description": title,
                    "responsibility": responsibility,
                    "prompt": prompt,
                    "subagent_type": subagent_type,
                    "child_session_id": child_session_id,
                    "parent_session_id": parent_session_id,
                    "model": child_model,
                    "reasoning_effort": reasoning_effort.map(|effort| effort.as_str()),
                    "status": status,
                    "lifecycle": resident_name.as_ref().map(|_| "resident"),
                    "resident_name": resident_name.clone(),
                    "reused": reused,
                    "note": note,
                });
                if should_wait {
                    waiting_for_children_tool_result(payload)
                } else {
                    tool_result(payload)
                }
            }
            SubAgentArgs::Wait {
                child_session_ids,
                wait_for,
            } => {
                let policy = wait_for.unwrap_or(ChildWaitPolicy::All);
                // Default to every currently-active child; honor an explicit
                // subset when provided. `active_child_ids` already excludes
                // terminal AND never-started children (issue #546 row 9), but
                // an EXPLICIT list is caller-supplied and may name a child
                // that already finished (a race) or was never started — those
                // ids are filtered out below (issue #546 row 8) since no
                // future completion event will ever arrive for them.
                let explicit_ids = child_session_ids.filter(|ids| !ids.is_empty());
                let targets = match explicit_ids {
                    Some(ids) => self.sessions.pending_child_ids(&parent.id, &ids).await,
                    None => self.sessions.active_child_ids(&parent.id).await,
                };

                if targets.is_empty() {
                    // Nothing to wait on — never register an empty wait (that
                    // would suspend the parent with no child able to resume it).
                    return tool_result(json!({
                        "status": "no_active_children",
                        "parent_session_id": parent_session_id,
                        "note": "No pending child sessions to wait for (already finished, never \
                                  started, or not found); the parent continues running.",
                    }))
                    .map(ToolOutcome::Completed);
                }

                let count = self
                    .sessions
                    .register_parent_wait_for_children(&parent.id, &targets, policy)
                    .await
                    .map_err(tool_error_from_child_session)?;

                waiting_for_children_tool_result(json!({
                    "status": "waiting",
                    "parent_session_id": parent_session_id,
                    "child_session_ids": targets,
                    "wait_for": policy.as_str(),
                    "waiting_on": count,
                }))
            }
            SubAgentArgs::List => {
                let result =
                    child_session::list_children_action(self.sessions.as_ref(), &parent.id).await;
                tool_result(result)
            }
            SubAgentArgs::Get { child_session_id } => {
                let result = child_session::get_child_action(
                    self.sessions.as_ref(),
                    &parent.id,
                    child_session_id,
                )
                .await
                .map_err(tool_error_from_child_session)?;
                tool_result(result)
            }
            SubAgentArgs::Update {
                child_session_id,
                title,
                responsibility,
                prompt,
                subagent_type,
                reset_after_update,
                auto_run,
                reasoning_effort,
            } => {
                let result = child_session::update_child_action(
                    self.sessions.as_ref(),
                    &parent.id,
                    child_session_id.clone(),
                    title,
                    responsibility,
                    prompt,
                    subagent_type,
                    reset_after_update,
                    reasoning_effort,
                )
                .await
                .map_err(tool_error_from_child_session)?;

                let should_auto_run = auto_run.unwrap_or(false);
                if should_auto_run {
                    let child = self
                        .sessions
                        .load_child_for_parent(&parent.id, &child_session_id)
                        .await
                        .map_err(tool_error_from_child_session)?;
                    self.sessions
                        .enqueue_child_run(&parent, &child)
                        .await
                        .map_err(tool_error_from_child_session)?;
                    // Re-running an existing child keeps its synchronous "wait for
                    // the answer" semantics: register the wait + suspend. (enqueue
                    // itself no longer registers — that is now explicit.)
                    self.sessions
                        .register_parent_wait_for_child(&parent.id, &child_session_id, None)
                        .await
                        .map_err(tool_error_from_child_session)?;
                }

                if should_auto_run {
                    waiting_for_children_tool_result(result)
                } else {
                    tool_result(result)
                }
            }
            SubAgentArgs::Run {
                child_session_id,
                reset_to_last_user,
            } => {
                let result = child_session::run_child_action(
                    self.sessions.as_ref(),
                    &parent,
                    child_session_id.clone(),
                    reset_to_last_user,
                )
                .await
                .map_err(tool_error_from_child_session)?;
                // `run` keeps the synchronous retry semantics: wait for this child.
                self.sessions
                    .register_parent_wait_for_child(&parent.id, &child_session_id, None)
                    .await
                    .map_err(tool_error_from_child_session)?;
                waiting_for_children_tool_result(result)
            }
            SubAgentArgs::SendMessage {
                child_session_id,
                message,
                auto_run,
                interrupt_running,
            } => {
                let should_auto_run = auto_run.unwrap_or(true);
                let result = child_session::send_message_to_child_action(
                    self.sessions.as_ref(),
                    &parent,
                    child_session_id.clone(),
                    message,
                    auto_run,
                    interrupt_running,
                )
                .await
                .map_err(tool_error_from_child_session)?;
                let queued = should_auto_run
                    && result
                        .get("status")
                        .and_then(|value| value.as_str())
                        .is_some_and(|status| status == "queued");
                if queued {
                    // Sending + running keeps synchronous semantics: wait for the
                    // child's response. (enqueue no longer registers the wait.)
                    self.sessions
                        .register_parent_wait_for_child(&parent.id, &child_session_id, None)
                        .await
                        .map_err(tool_error_from_child_session)?;
                    waiting_for_children_tool_result(result)
                } else {
                    tool_result(result)
                }
            }
            SubAgentArgs::Cancel { child_session_id } => {
                let result = child_session::cancel_child_action(
                    self.sessions.as_ref(),
                    &parent.id,
                    child_session_id,
                )
                .await
                .map_err(tool_error_from_child_session)?;
                tool_result(result)
            }
            SubAgentArgs::Delete { child_session_id } => {
                let result = child_session::delete_child_action(
                    self.sessions.as_ref(),
                    &parent.id,
                    child_session_id,
                )
                .await
                .map_err(tool_error_from_child_session)?;
                tool_result(result)
            }
            // Handled by the session-independent short-circuit above.
            SubAgentArgs::ListModels => unreachable!("list_models short-circuits earlier"),
        }
        .map(ToolOutcome::Completed)
    }
}

// ---------------------------------------------------------------------------
// Tests
//
// Pure unit tests for the framework-agnostic helpers live here. Integration
// tests that wire `SubAgentTool` to a real `ChildSessionAdapter` live in
// `bamboo-server` (`tools/sub_agent_tests.rs`), where the adapter + AppState
// types are available.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_title_accepts_legacy_description() {
        let title = normalize_title(None, "Search refs".to_string()).unwrap();
        assert_eq!(title, "Search refs");
    }

    #[test]
    fn normalize_title_prefers_title_over_description() {
        let title =
            normalize_title(Some("Real title".to_string()), "Legacy desc".to_string()).unwrap();
        assert_eq!(title, "Real title");
    }

    #[test]
    fn normalize_title_rejects_both_empty() {
        let err = normalize_title(None, "".to_string()).unwrap_err();
        assert!(matches!(err, ToolError::InvalidArguments(msg) if msg.contains("title")));
    }

    // ---- parse_model_spec ----

    fn parent_session(
        model_ref: Option<bamboo_domain::ProviderModelRef>,
    ) -> bamboo_agent_core::Session {
        let mut session = bamboo_agent_core::Session::new("p1", "gpt-test");
        session.model_ref = model_ref;
        session
    }

    #[test]
    fn model_spec_provider_colon_model_is_explicit() {
        let parent = parent_session(None);
        let r = parse_model_spec("anthropic:claude-sonnet-4-6", &parent, None).unwrap();
        assert_eq!(r.provider, "anthropic");
        assert_eq!(r.model, "claude-sonnet-4-6");
    }

    #[test]
    fn model_spec_bare_inherits_parent_provider() {
        let parent = parent_session(Some(bamboo_domain::ProviderModelRef::new(
            "openai", "gpt-test",
        )));
        let r = parse_model_spec("o4-mini", &parent, Some("anthropic".to_string())).unwrap();
        assert_eq!(r.provider, "openai"); // parent wins over default
        assert_eq!(r.model, "o4-mini");
    }

    #[test]
    fn model_spec_bare_falls_back_to_default_provider() {
        let parent = parent_session(None);
        let r =
            parse_model_spec("claude-haiku-4-5", &parent, Some("anthropic".to_string())).unwrap();
        assert_eq!(r.provider, "anthropic");
    }

    #[test]
    fn model_spec_bare_without_any_provider_errors() {
        let parent = parent_session(None);
        let err = parse_model_spec("mystery-model", &parent, None).unwrap_err();
        assert!(matches!(err, ToolError::InvalidArguments(msg) if msg.contains("provider")));
    }

    #[test]
    fn model_spec_rejects_malformed() {
        let parent = parent_session(None);
        assert!(parse_model_spec("  ", &parent, None).is_err());
        assert!(parse_model_spec("anthropic:", &parent, None).is_err());
        assert!(parse_model_spec(":model", &parent, None).is_err());
    }
}
