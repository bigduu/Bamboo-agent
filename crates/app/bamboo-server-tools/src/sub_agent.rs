use async_trait::async_trait;
use dashmap::DashMap;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

type SynchronousLaunchLocks = Arc<DashMap<String, Arc<tokio::sync::Mutex<()>>>>;

#[derive(Default)]
struct LaunchGate(bamboo_domain::AdmissionGate);

impl LaunchGate {
    fn cancel_if_pending(&self) {
        self.0.cancel_if_pending();
    }

    fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }
}

struct CancelPendingLaunch {
    gate: Arc<LaunchGate>,
    armed: bool,
}

impl Drop for CancelPendingLaunch {
    fn drop(&mut self) {
        if self.armed {
            self.gate.cancel_if_pending();
        }
    }
}

fn cancelled_launch_error() -> ChildSessionError {
    ChildSessionError::Execution("SubAgent tool cancelled before child delivery".to_string())
}

struct SynchronousLaunchGuard {
    locks: SynchronousLaunchLocks,
    parent_id: String,
    mutex: Arc<tokio::sync::Mutex<()>>,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl Drop for SynchronousLaunchGuard {
    fn drop(&mut self) {
        drop(self.guard.take());
        // The map and this guard are the only holders when no peer is queued.
        // Remove the idle slot so long-lived servers do not retain one mutex
        // for every parent session that has ever launched a child.
        self.locks.remove_if(&self.parent_id, |_, current| {
            Arc::ptr_eq(current, &self.mutex) && Arc::strong_count(current) == 2
        });
    }
}

use crate::parent_request_reply::{
    ParentRequestMessageReceipt, ParentRequestReplyPort, ParentRequestReplyState,
};
use crate::sub_agent_facade::{self as facade, Projection};
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
        /// Exact validated named profile on a supported fresh local route;
        /// a genuinely unknown name retains the legacy routing/display label.
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
        /// Optional reasoning effort for the child session. When omitted, the
        /// selected sub-agent model preference applies before the provider
        /// default (it does NOT inherit the parent's reasoning_effort). The
        /// LLM should pass an explicit value (e.g. `"low"` for cheap fan-outs,
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
        /// Opt-in complete instructions for a fresh built-in local worker.
        #[serde(default)]
        context_packet: Option<Box<bamboo_domain::ChildContextPacket>>,
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
        /// Overview is metadata only. Other views return bounded, durable
        /// transcript previews or UTF-8 content slices.
        #[serde(default)]
        view: Option<String>,
        #[serde(default)]
        cursor: Option<String>,
        #[serde(default)]
        message_id: Option<String>,
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default)]
        max_bytes: Option<usize>,
        #[serde(default)]
        expected_child_created_at: Option<String>,
        #[serde(default)]
        expected_assignment_sha256: Option<String>,
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
        /// Optional explicit model for the existing child session. Accepts the
        /// same `provider:model` or bare model id form as create.
        #[serde(default)]
        model: Option<String>,
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

fn bounded_child_result(view: &str, value: serde_json::Value) -> Result<ToolResult, ToolError> {
    let result = tool_result(value)?;
    if result.result.len() <= child_session::MAX_CHILD_RESULT_BYTES
        && serde_json::to_vec(&result)
            .is_ok_and(|bytes| bytes.len() <= child_session::MAX_CHILD_RESULT_BYTES)
    {
        return Ok(result);
    }
    tool_result(child_session::unavailable_child_result(
        view,
        "result_budget_exceeded",
    ))
}

/// The child must not become runnable until its synchronous parent's wait is
/// durable. This is shared by create, update, and run; background fan-out keeps
/// its existing enqueue-only path.
async fn enqueue_waiting_child(
    sessions: &dyn ChildSessionPort,
    parent: &bamboo_agent_core::Session,
    child_session_id: &str,
    tool_call_id: &str,
    launch_gate: Option<&LaunchGate>,
) -> Result<(), ToolError> {
    let child = sessions
        .load_child_for_parent(&parent.id, child_session_id)
        .await
        .map_err(tool_error_from_child_session)?;
    let parent_before = sessions
        .load_parent_session(&parent.id)
        .await
        .map_err(tool_error_from_child_session)?;
    let had_wait = parent_before
        .agent_runtime_state
        .as_ref()
        .and_then(|state| state.waiting_for_children.as_ref())
        .is_some_and(|wait| {
            wait.child_session_ids
                .iter()
                .any(|id| id == child_session_id)
        });
    if let Err(error) = sessions
        .register_parent_wait_for_child(&parent.id, child_session_id, Some(tool_call_id))
        .await
    {
        if had_wait {
            return Err(tool_error_from_child_session(error));
        }
        // The child was persisted as pending while the synchronous launch was
        // prepared, but it was never enqueued. Make it terminal before the
        // parent runner's end-of-turn safety net scans active child sessions.
        let error = mark_failed_child_enqueue(sessions, parent, child_session_id, error).await;
        return Err(tool_error_from_child_session(
            child_session::rollback_failed_wait_launch(
                sessions,
                &parent.id,
                child_session_id,
                error,
            )
            .await,
        ));
    }
    let admit = sessions.admit_child_run(parent, &child, launch_gate.map(|gate| &gate.0));
    let admission = match launch_gate {
        Some(gate) => tokio::select! {
            biased;
            _ = gate.0.cancelled() => Ok(bamboo_domain::AdmissionCommit::Cancelled),
            result = admit => result,
        },
        None => admit.await,
    };
    let error = match admission {
        Ok(bamboo_domain::AdmissionCommit::Committed(()))
        | Ok(bamboo_domain::AdmissionCommit::AlreadyCommitted) => return Ok(()),
        Ok(bamboo_domain::AdmissionCommit::Cancelled) => cancelled_launch_error(),
        Err(error) if launch_gate.is_some_and(|gate| gate.0.is_committed()) => {
            tracing::warn!(child_session_id, %error, "child job was admitted despite a later port error");
            return Ok(());
        }
        Err(error) => error,
    };
    // A confirmed scheduler rejection leaves the prepared child pending.
    // The end-of-turn safety net treats pending as active and would arm a new
    // orphan wait. Mark this run as retryable terminal failure first. Preserve
    // any wait that a different operation already owned.
    if had_wait {
        return Err(tool_error_from_child_session(error));
    }
    let error = mark_failed_child_enqueue(sessions, parent, child_session_id, error).await;
    Err(tool_error_from_child_session(
        child_session::rollback_failed_wait_launch(sessions, &parent.id, child_session_id, error)
            .await,
    ))
}

/// Background launches have no explicit parent wait to compensate, but a
/// prepared Child must become terminal when its queue admission is cancelled
/// or rejected so the parent's end-of-turn orphan scan cannot arm one later.
async fn enqueue_background_child(
    sessions: &dyn ChildSessionPort,
    parent: &bamboo_agent_core::Session,
    child_session_id: &str,
    launch_gate: Option<&LaunchGate>,
) -> Result<(), ToolError> {
    let child = sessions
        .load_child_for_parent(&parent.id, child_session_id)
        .await
        .map_err(tool_error_from_child_session)?;
    let admit = sessions.admit_child_run(parent, &child, launch_gate.map(|gate| &gate.0));
    let admission = match launch_gate {
        Some(gate) => tokio::select! {
            biased;
            _ = gate.0.cancelled() => Ok(bamboo_domain::AdmissionCommit::Cancelled),
            result = admit => result,
        },
        None => admit.await,
    };
    match admission {
        Ok(bamboo_domain::AdmissionCommit::Committed(()))
        | Ok(bamboo_domain::AdmissionCommit::AlreadyCommitted) => Ok(()),
        Ok(bamboo_domain::AdmissionCommit::Cancelled) => {
            let error = mark_failed_child_enqueue(
                sessions,
                parent,
                child_session_id,
                cancelled_launch_error(),
            )
            .await;
            Err(tool_error_from_child_session(error))
        }
        Err(error) if launch_gate.is_some_and(|gate| gate.0.is_committed()) => {
            tracing::warn!(child_session_id, %error, "background child job was admitted despite a later port error");
            Ok(())
        }
        Err(error) => {
            let error = mark_failed_child_enqueue(sessions, parent, child_session_id, error).await;
            Err(tool_error_from_child_session(error))
        }
    }
}

async fn mark_failed_child_enqueue(
    sessions: &dyn ChildSessionPort,
    parent: &bamboo_agent_core::Session,
    child_session_id: &str,
    error: ChildSessionError,
) -> ChildSessionError {
    let status_update = async {
        let mut child = sessions
            .load_child_for_parent(&parent.id, child_session_id)
            .await?;
        if child.last_run_status().as_deref().is_some_and(|status| {
            matches!(status, "completed" | "cancelled" | "timeout" | "skipped")
        }) {
            return Ok(());
        }
        child.set_last_run_status("error");
        child.set_last_run_error(format!("Child launch failed: {error}"));
        child.updated_at = chrono::Utc::now();
        sessions.save_child_session(&mut child).await
    }
    .await;
    match status_update {
        Ok(()) => error,
        Err(status_error) => ChildSessionError::Execution(format!(
            "{error}; failed to persist terminal child launch status: {status_error}"
        )),
    }
}

pub(crate) fn waiting_for_children_tool_result(
    mut value: serde_json::Value,
) -> Result<ToolResult, ToolError> {
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

/// Split an explicit `SubAgent.wait` id list into `(targets, dropped)`:
/// `dropped` = `(id, status)` pairs the index positively reported terminal
/// (waiting on them could never be satisfied — issue #546), `targets` =
/// everything else, including unknown ids (kept: the watchdog rescues a bogus
/// id at runtime; an index-less backend reports nothing terminal and filters
/// nothing).
fn partition_wait_targets(
    requested: Vec<String>,
    known_terminal: &[(String, String)],
) -> (Vec<String>, Vec<(String, String)>) {
    let mut targets = Vec::new();
    let mut dropped = Vec::new();
    for id in requested {
        match known_terminal
            .iter()
            .find(|(terminal_id, _)| *terminal_id == id)
        {
            Some((_, status)) => dropped.push((id, status.clone())),
            None => targets.push(id),
        }
    }
    (targets, dropped)
}

/// Whether the dropped (already-terminal) ids of an explicit wait ALREADY
/// satisfy the requested policy, so the wait must short-circuit to a
/// non-suspending result instead of arming over the remainder (issue #546):
/// `any` is satisfied by any terminal child; `first_error` by any error-like
/// terminal child. For `all`, waiting on the remainder is equivalent, so the
/// residual wait proceeds.
fn wait_already_satisfied_by_dropped(
    policy: ChildWaitPolicy,
    dropped: &[(String, String)],
) -> bool {
    match policy {
        ChildWaitPolicy::All => false,
        ChildWaitPolicy::Any => !dropped.is_empty(),
        ChildWaitPolicy::FirstError => dropped
            .iter()
            .any(|(_, status)| matches!(status.as_str(), "error" | "timeout" | "cancelled")),
    }
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

fn require_resident_project_identity(
    parent_project_id: Option<&bamboo_domain::ProjectId>,
    child: &bamboo_agent_core::Session,
) -> Result<(), ToolError> {
    let child_project_id =
        match bamboo_engine::project_context::ProjectContextResolver::session_project_identity(
            child,
        ) {
            bamboo_engine::project_context::SessionProjectIdentity::Assigned(project_id) => {
                Some(project_id)
            }
            bamboo_engine::project_context::SessionProjectIdentity::Unassigned => None,
            bamboo_engine::project_context::SessionProjectIdentity::Invalid { raw, message } => {
                return Err(ToolError::InvalidArguments(format!(
                    "resident session carries an invalid Project identity '{raw}': {message}"
                )));
            }
        };
    if child_project_id.as_ref() != parent_project_id {
        return Err(ToolError::InvalidArguments(format!(
            "resident_project_scope_conflict: resident Project '{}' does not match parent Project '{}'",
            child_project_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "unassigned".to_string()),
            parent_project_id
                .map(ToString::to_string)
                .unwrap_or_else(|| "unassigned".to_string()),
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tool struct
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct SubAgentTool {
    /// Child-session CRUD/lifecycle operations (load/save/run/cancel/…).
    sessions: Arc<dyn ChildSessionPort>,
    /// Subagent-type resolution (model, runtime metadata, active ids).
    resolver: Arc<dyn SubagentResolutionPort>,
    /// Optional model catalog consulted by `action=list_models` and used to
    /// resolve a bare `create.model` id to a provider. `None` keeps the tool
    /// constructible without a live provider registry (tests, embedded use).
    catalog: Option<Arc<dyn ModelCatalogPort>>,
    /// Serialize wait-owning calls on this tool instance from inspection
    /// through launch/rollback. Two calls for the same child must not both
    /// observe an absent wait and later undo each other's registration.
    synchronous_launch_locks: SynchronousLaunchLocks,
    /// Per-run, read-only canonical Host path for a logical Worker Child.
    owned_tree_host: Option<(bamboo_subagent::executor::HostBridge, String)>,
    /// Server-owned direct-parent resolver; absent in unbound tools.
    parent_request_replies: Option<Arc<dyn ParentRequestReplyPort>>,
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
            synchronous_launch_locks: Arc::new(DashMap::new()),
            owned_tree_host: None,
            parent_request_replies: None,
        }
    }

    /// Attach a model catalog, enabling `action=list_models` and bare-model
    /// resolution for `create.model`.
    pub fn with_model_catalog(mut self, catalog: Arc<dyn ModelCatalogPort>) -> Self {
        self.catalog = Some(catalog);
        self
    }

    /// Bind only this run's active logical Child to the Host's canonical tree.
    pub fn with_owned_tree_host(
        mut self,
        bridge: bamboo_subagent::executor::HostBridge,
        actor_id: String,
    ) -> Self {
        self.owned_tree_host = Some((bridge, actor_id));
        self
    }

    pub fn with_parent_request_replies(mut self, replies: Arc<dyn ParentRequestReplyPort>) -> Self {
        self.parent_request_replies = Some(replies);
        self
    }
}

/// Parse an explicit `create.model` or `update.model` spec into a
/// `ProviderModelRef`.
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
    facade::description()
}

/// The `SubAgent` parameters schema. Exposed standalone (mirroring
/// [`subagent_tool_description`]) so a nested worker's SubAgent proxy advertises
/// the IDENTICAL schema to its own LLM — no drift between the real tool and the
/// proxy.
pub fn subagent_parameters_schema() -> serde_json::Value {
    facade::parameters_schema()
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
        let host_args = args.clone();
        let normalized = facade::normalize(args)?;
        let args = normalized.args;
        let projection = normalized.projection;
        if projection == Some(Projection::Tree) {
            let parent_id = ctx.session_id().ok_or_else(|| {
                ToolError::Execution("SubAgent requires a current session".into())
            })?;
            let cursor = args.get("cursor").and_then(serde_json::Value::as_str);
            if let Some((bridge, actor_id)) = &self.owned_tree_host {
                if parent_id != actor_id {
                    return Err(ToolError::Execution(
                        "Owned tree caller differs from the active logical Child".into(),
                    ));
                }
                let page = bridge
                    .owned_tree_call(cursor)
                    .await
                    .map_err(ToolError::Execution)?;
                return Ok(ToolOutcome::Completed(ToolResult::text(
                    true,
                    page.to_string(),
                )));
            }
            return facade::inspect_tree(self.sessions.as_ref(), parent_id, cursor).await;
        }
        if let Some((bridge, actor_id)) = &self.owned_tree_host {
            if ctx.session_id() != Some(actor_id.as_str()) {
                return Err(ToolError::Execution(
                    "SubAgent caller differs from the active logical Child".into(),
                ));
            }
            let result = bridge
                .subagent_call(host_args, ctx.tool_call_id.as_ref())
                .await
                .map_err(ToolError::Execution)?;
            let result: ToolResult = serde_json::from_value(result).map_err(|_| {
                ToolError::Execution("canonical SubAgent returned an invalid result".into())
            })?;
            return Ok(ToolOutcome::Completed(result));
        }
        if projection == Some(Projection::ParentQuestionAsk) {
            // Only the canonical Host callback is allowed to produce an
            // agentic clarification. A standalone/local Child tool without
            // that bridge must not park itself on an unrouteable question.
            if self.parent_request_replies.is_none() {
                return Err(ToolError::Execution(
                    "ask_parent requires a canonical Host-backed Child run".into(),
                ));
            }
            let caller_id = ctx.session_id().ok_or_else(|| {
                ToolError::Execution("ask_parent requires an active Child Session".into())
            })?;
            let child = self
                .sessions
                .load_tree_caller_session(caller_id)
                .await
                .map_err(tool_error_from_child_session)?;
            let child_actor = bamboo_domain::ActorSession::from_session(&child)
                .map_err(|_| ToolError::Execution("ask_parent Child identity is invalid".into()))?;
            let parent_id = child.parent_session_id.as_deref().ok_or_else(|| {
                ToolError::Execution("ask_parent requires a direct parent".into())
            })?;
            if child.id != caller_id
                || child.kind != bamboo_domain::SessionKind::Child
                || child.pending_question.is_some()
                || ctx.plan_read_only
                || child
                    .subagent_type()
                    .as_deref()
                    .is_some_and(|role| matches!(role, "planner" | "guardian"))
                || child
                    .agent_runtime_state
                    .as_ref()
                    .is_some_and(|state| state.plan_mode.is_some())
            {
                return Err(ToolError::Execution(
                    "ask_parent is unavailable for this Child run".into(),
                ));
            }
            let parent = self
                .sessions
                .load_parent_session(parent_id)
                .await
                .map_err(tool_error_from_child_session)?;
            let parent_actor = bamboo_domain::ActorSession::from_session(&parent)
                .map_err(|_| ToolError::Execution("ask_parent direct parent is invalid".into()))?;
            if child_actor.parent_actor_id.as_deref() != Some(parent.id.as_str())
                || child_actor.root_actor_id != parent_actor.root_actor_id
                || child_actor.project_id != parent_actor.project_id
                || parent_actor.spawn_depth.checked_add(1) != Some(child_actor.spawn_depth)
            {
                return Err(ToolError::Execution(
                    "ask_parent direct-parent lineage changed".into(),
                ));
            }
            let question = args["question"]
                .as_str()
                .ok_or_else(|| ToolError::Execution("ask_parent question is missing".into()))?;
            let result =
                serde_json::to_string(&bamboo_agent_core::AgenticToolResult::NeedClarification {
                    question: question.to_owned(),
                    options: None,
                })
                .map_err(|_| {
                    ToolError::Execution("ask_parent question cannot be encoded".into())
                })?;
            return Ok(ToolOutcome::Completed(ToolResult::text(true, result)));
        }
        if projection == Some(Projection::ParentRequestReply) {
            let caller = ctx.session_id().ok_or_else(|| {
                ToolError::Execution("SubAgent requires a current parent Session".into())
            })?;
            let request_id = args["reply_to"].as_str().ok_or_else(|| {
                ToolError::Execution("Canonical ParentRequest id is missing".into())
            })?;
            let resolver = self.parent_request_replies.as_ref().ok_or_else(|| {
                ToolError::Execution("Direct-parent reply authority is unavailable".into())
            })?;
            let message = args["message"].as_str().ok_or_else(|| {
                ToolError::Execution("Canonical ParentRequest reply message is missing".into())
            })?;
            let receipt = resolver
                .resolve_message(caller, request_id, message)
                .await
                .map_err(ToolError::Execution)?;
            let payload = match receipt {
                ParentRequestMessageReceipt::Permission(receipt) => {
                    let state = match receipt.state {
                        ParentRequestReplyState::Recorded => "decision_recorded",
                        ParentRequestReplyState::AlreadyResolved => "already_resolved",
                    };
                    json!({"request_id":request_id,"decision":receipt.decision,"state":state})
                }
                ParentRequestMessageReceipt::Clarification(receipt) => {
                    let state = match receipt.state {
                        ParentRequestReplyState::Recorded => "answer_recorded",
                        ParentRequestReplyState::AlreadyResolved => "already_resolved",
                    };
                    json!({"request_id":request_id,"answer":receipt.answer,"state":state})
                }
            };
            return Ok(ToolOutcome::Completed(ToolResult::text(
                true,
                payload.to_string(),
            )));
        }
        if projection == Some(Projection::ForcedPermissionAudit) {
            let caller_id = ctx.session_id().ok_or_else(|| {
                ToolError::Execution("SubAgent requires a current session".into())
            })?;
            return facade::inspect_forced_permission_audit(self.sessions.as_ref(), caller_id)
                .await;
        }
        // The owner outlives a cancelled caller so an in-flight registration or
        // delivery can be resolved. The gate prevents a new launch when the
        // caller's cancellation wins before entering the scheduler or Inbox
        // port. Their internal durable-admission boundary is tracked by #1313.
        let action = args.get("action").and_then(serde_json::Value::as_str);
        let owns_launch = match action.unwrap_or("create") {
            "create" => args
                .get("auto_run")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(true),
            "update" => args
                .get("auto_run")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
            "run" | "send_message" => true,
            _ => false,
        };
        if owns_launch {
            let owner = self.clone();
            let gate = Arc::new(LaunchGate::default());
            let mut cancel_on_drop = CancelPendingLaunch {
                gate: gate.clone(),
                armed: true,
            };
            let result = tokio::spawn(async move {
                owner
                    .invoke_inner(args, ctx, Some(gate), projection.is_some())
                    .await
            })
            .await
            .map_err(|error| {
                ToolError::Execution(format!("SubAgent launch owner failed: {error}"))
            })?;
            cancel_on_drop.armed = false;
            return facade::finish(projection, result);
        }
        if action == Some("cancel") {
            let result = self.invoke_cancel(args, ctx).await;
            return facade::finish(projection, result);
        }
        let result = self
            .invoke_inner(args, ctx, None, projection.is_some())
            .await;
        facade::finish(projection, result)
    }
}

struct ParsedSubAgentInvocation<'a> {
    parent_session_id: &'a str,
    parsed: SubAgentArgs,
    report_args: Option<serde_json::Value>,
    has_result_selectors: bool,
}

fn parse_subagent_invocation<'a>(
    args: serde_json::Value,
    ctx: &'a ToolCtx,
) -> Result<ParsedSubAgentInvocation<'a>, ToolError> {
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

    let has_packet = args.get("context_packet").is_some();
    if has_packet && !args["context_packet"].is_object() {
        return Err(ToolError::InvalidArguments(
            bamboo_domain::ChildContextPacketError::Invalid.to_string(),
        ));
    }
    if has_packet
        && serde_json::to_vec(&args).map_or(true, |bytes| {
            bytes.len() > bamboo_domain::MAX_CHILD_PACKET_INPUT_BYTES
        })
    {
        return Err(ToolError::InvalidArguments(
            bamboo_domain::ChildContextPacketError::Budget.to_string(),
        ));
    }
    let report_args = matches!(
        args["view"].as_str(),
        Some("result_binding" | "typed_result")
    )
    .then(|| args.clone());
    let has_result_selectors = args.get("expected_child_created_at").is_some()
        || args.get("expected_assignment_sha256").is_some();
    let parsed: SubAgentArgs = serde_json::from_value(args).map_err(|error| {
        ToolError::InvalidArguments(if has_packet {
            bamboo_domain::ChildContextPacketError::Invalid.to_string()
        } else {
            format!("Invalid SubAgent args: {error}")
        })
    })?;

    Ok(ParsedSubAgentInvocation {
        parent_session_id,
        parsed,
        report_args,
        has_result_selectors,
    })
}

impl SubAgentTool {
    fn invoke_inner<'a>(
        &'a self,
        args: serde_json::Value,
        ctx: ToolCtx,
        launch_gate: Option<Arc<LaunchGate>>,
        compact: bool,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<ToolOutcome, ToolError>> + Send + 'a>,
    > {
        Box::pin(self.invoke_inner_async(args, ctx, launch_gate, compact))
    }

    fn invoke_cancel<'a>(
        &'a self,
        args: serde_json::Value,
        ctx: ToolCtx,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<ToolOutcome, ToolError>> + Send + 'a>,
    > {
        Box::pin(async move {
            let ParsedSubAgentInvocation {
                parent_session_id,
                parsed,
                ..
            } = parse_subagent_invocation(args, &ctx)?;
            let SubAgentArgs::Cancel { child_session_id } = parsed else {
                unreachable!("cancel route is selected only for normalized cancel args");
            };
            let parent = self
                .sessions
                .as_ref()
                .load_parent_session(parent_session_id)
                .await
                .map_err(tool_error_from_child_session)?;
            let result = child_session::cancel_child_action(
                self.sessions.as_ref(),
                &parent.id,
                child_session_id,
            )
            .await
            .map_err(tool_error_from_child_session)?;
            tool_result(result).map(ToolOutcome::Completed)
        })
    }

    async fn invoke_inner_async(
        &self,
        args: serde_json::Value,
        ctx: ToolCtx,
        launch_gate: Option<Arc<LaunchGate>>,
        compact: bool,
    ) -> Result<ToolOutcome, ToolError> {
        if launch_gate.as_ref().is_some_and(|gate| gate.is_cancelled()) {
            return Err(tool_error_from_child_session(cancelled_launch_error()));
        }
        let ParsedSubAgentInvocation {
            parent_session_id,
            parsed,
            report_args,
            has_result_selectors,
        } = parse_subagent_invocation(args, &ctx)?;

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

        // Inspection only needs the trusted current session id. A nested
        // child is also a direct parent of its own children, while the
        // lifecycle path below still loads the root session as before.
        if let SubAgentArgs::Get {
            child_session_id,
            view,
            cursor,
            message_id,
            limit,
            max_bytes,
            expected_child_created_at,
            expected_assignment_sha256,
        } = &parsed
        {
            if let Some(arguments) = report_args.as_ref() {
                let view = view.as_deref().expect("report view");
                let value = child_session::inspect_child_report_action(
                    self.sessions.as_ref(),
                    parent_session_id,
                    child_session_id,
                    view,
                    arguments,
                )
                .await
                .map_err(tool_error_from_child_session)?;
                return bounded_child_result(view, value).map(ToolOutcome::Completed);
            }
            if has_result_selectors
                || expected_child_created_at.is_some()
                || expected_assignment_sha256.is_some()
            {
                return Err(ToolError::InvalidArguments(
                    "result selectors require view=typed_result".into(),
                ));
            }
            let result = match view.as_deref().unwrap_or("overview") {
                "overview" => {
                    if cursor.is_some()
                        || message_id.is_some()
                        || limit.is_some()
                        || max_bytes.is_some()
                    {
                        return Err(ToolError::InvalidArguments(
                            "overview does not accept cursor, message_id, limit, or max_bytes"
                                .to_string(),
                        ));
                    }
                    child_session::get_child_action(
                        self.sessions.as_ref(),
                        parent_session_id,
                        child_session_id.clone(),
                    )
                    .await
                }
                view => {
                    child_session::inspect_child_action(
                        self.sessions.as_ref(),
                        parent_session_id,
                        child_session_id,
                        view,
                        cursor.as_deref(),
                        message_id.as_deref(),
                        *limit,
                        *max_bytes,
                    )
                    .await
                }
            }
            .map_err(tool_error_from_child_session)?;
            return tool_result(result).map(ToolOutcome::Completed);
        }

        let parent = self
            .sessions
            .as_ref()
            .load_parent_session(parent_session_id)
            .await
            .map_err(tool_error_from_child_session)?;

        // The production Root tool is a shared instance. Keep its synchronous
        // launch transactions sequential, including explicit `wait`, so a
        // failed same-child launch cannot roll back another call's wait.
        // Independent one-shot background creates retain concurrent fan-out;
        // resident reuse is gated because it can target an existing child.
        let owns_wait_or_launch = match &parsed {
            SubAgentArgs::Create {
                auto_run,
                wait,
                lifecycle,
                ..
            } => {
                auto_run.unwrap_or(true)
                    && (wait.unwrap_or(false) || lifecycle.as_deref() == Some("resident"))
            }
            SubAgentArgs::Update { auto_run, .. } => auto_run.unwrap_or(false),
            SubAgentArgs::Run { .. } | SubAgentArgs::Wait { .. } => true,
            SubAgentArgs::SendMessage { auto_run, .. } => auto_run.unwrap_or(true),
            _ => false,
        };
        let _launch_guard = if owns_wait_or_launch {
            let mutex = self
                .synchronous_launch_locks
                .entry(parent.id.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone();
            // Construct the lease before awaiting the mutex so cancellation
            // of a queued invocation still releases an otherwise idle slot.
            let mut lease = SynchronousLaunchGuard {
                locks: self.synchronous_launch_locks.clone(),
                parent_id: parent.id.clone(),
                mutex,
                guard: None,
            };
            lease.guard = Some(lease.mutex.clone().lock_owned().await);
            Some(lease)
        } else {
            None
        };

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
                context_packet,
            } => {
                if let Some(packet) = &context_packet {
                    packet.validate().map_err(|error| ToolError::InvalidArguments(error.to_string()))?;
                    if lifecycle.as_deref().is_some_and(|value| value != "oneshot")
                        || name.is_some() || context.is_some() || fork_last_messages.unwrap_or_default() > 0 {
                        return Err(ToolError::InvalidArguments("required_child_context_unsupported: fresh one-shot only".into()));
                    }
                }
                let mut packet_counts = None;
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
                let prompt = if compact {
                    if prompt.trim().is_empty() {
                        return Err(ToolError::InvalidArguments("message must be non-empty".into()));
                    }
                    prompt
                } else {
                    normalize_required_text(Some(prompt), "prompt")?
                };
                // Known catalog names are applied by the canonical creator.
                // Unknown names retain the old routing/display label.
                let subagent_type = subagent_type
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
                    .unwrap_or_else(|| "worker".to_string());
                if (lifecycle.as_deref() == Some("resident") || name.is_some())
                    && self.sessions.resolve_named_profile(&parent, &subagent_type)
                        .await.map_err(tool_error_from_child_session)?.is_some()
                {
                    return Err(ToolError::InvalidArguments("named_profile_requires_fresh_local_child".into()));
                }
                let parent_project_id =
                    match bamboo_engine::project_context::ProjectContextResolver::session_project_identity(&parent) {
                        bamboo_engine::project_context::SessionProjectIdentity::Assigned(
                            project_id,
                        ) => Some(project_id),
                        bamboo_engine::project_context::SessionProjectIdentity::Unassigned => None,
                        bamboo_engine::project_context::SessionProjectIdentity::Invalid {
                            raw,
                            message,
                        } => {
                            return Err(ToolError::InvalidArguments(format!(
                                "parent session carries an invalid Project identity '{raw}': {message}"
                            )));
                        }
                    };
                let (workspace, workspace_source) = if compact {
                    // Chat stores its workspace on the typed metadata plane.
                    // Use the same canonical resolver as Plan before creation.
                    self.sessions
                        .resolve_child_workspace(&parent, workspace.as_deref())
                        .await
                        .map_err(tool_error_from_child_session)?
                } else {
                    // workspace is optional: default to the parent's workspace.
                    let explicit_workspace = workspace
                        .map(|value| value.trim().to_string())
                        .filter(|value| !value.is_empty());
                    let workspace_was_explicit = explicit_workspace.is_some();
                    let parent_workspace_is_project_default = parent
                        .metadata
                        .get(bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY)
                        .map(String::as_str)
                        == Some(
                            bamboo_engine::project_context::WorkspaceSource::ProjectDefault.as_str(),
                        );
                    let requested_workspace = explicit_workspace
                        .or_else(|| {
                            (!parent_workspace_is_project_default)
                                .then(|| parent.workspace.clone())
                                .flatten()
                        })
                        .unwrap_or_default();
                    let workspace_source = if workspace_was_explicit {
                        bamboo_engine::project_context::WorkspaceSource::Explicit
                    } else if parent_workspace_is_project_default
                        || (requested_workspace.is_empty() && parent_project_id.is_some())
                    {
                        bamboo_engine::project_context::WorkspaceSource::ProjectDefault
                    } else {
                        match parent
                            .metadata
                            .get(bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY)
                            .map(String::as_str)
                        {
                            Some("project_default") => {
                                bamboo_engine::project_context::WorkspaceSource::ProjectDefault
                            }
                            _ => bamboo_engine::project_context::WorkspaceSource::Session,
                        }
                    };
                    // This must precede resident lookup/cancellation and every
                    // child/session mutation. Reused residents bypass
                    // `create_child_action`, while new children and guardians use
                    // it as a second fail-closed boundary.
                    let workspace = self
                        .sessions
                        .validate_child_workspace(
                            parent_project_id.as_ref(),
                            &requested_workspace,
                        )
                        .await
                        .map_err(tool_error_from_child_session)?;

                    (workspace, workspace_source)
                };

                if parent.model.trim().is_empty() {
                    return Err(ToolError::Execution(
                        "parent session model is empty".to_string(),
                    ));
                }

                let should_auto_run = auto_run.unwrap_or(true);
                let requested_wait = should_auto_run && wait.unwrap_or(false);

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

                let (child_session_id, child_model, reused, child_reasoning_effort, should_wait) =
                    if let Some(existing_id) = existing_resident {
                        // A resident is stable Project identity. A root may
                        // have been explicitly reassigned since this resident
                        // was created; never silently pull the old child across
                        // that boundary. This check precedes cancellation and
                        // every child/session mutation.
                        let mut child = self
                            .sessions
                            .load_child_for_parent(&parent.id, &existing_id)
                            .await
                            .map_err(tool_error_from_child_session)?;
                        require_resident_project_identity(parent_project_id.as_ref(), &child)?;
                        let mut resident_delivery_gate = launch_gate.as_deref();

                        // A resident processes tasks serially. If it is still running
                        // a previous task, stop it first: otherwise `reset` would
                        // truncate the session while the runner writes back (a
                        // corrupting race + a possible duplicate spawn job), and
                        // `accumulate` would queue a message into a run that may end
                        // before picking it up (the task would never execute). After
                        // cancel the resident is idle, so both paths apply cleanly.
                        if self.sessions.is_child_running(&existing_id).await {
                            if let Some(gate) = resident_delivery_gate {
                                match gate
                                    .0
                                    .commit(|| Ok::<(), std::convert::Infallible>(()))
                                    .unwrap_or_else(|never| match never {})
                                {
                                    bamboo_domain::AdmissionCommit::Cancelled => {
                                        return Err(tool_error_from_child_session(
                                            cancelled_launch_error(),
                                        ));
                                    }
                                    bamboo_domain::AdmissionCommit::Committed(())
                                    | bamboo_domain::AdmissionCommit::AlreadyCommitted => {}
                                }
                            }
                            // Stopping the old run is the first irreversible
                            // effect. The detached owner must finish delivery,
                            // and this spent gate cannot guard another queue
                            // or Inbox admission.
                            resident_delivery_gate = None;
                            self.sessions
                                .cancel_child_run_and_wait(&existing_id)
                                .await
                                .map_err(tool_error_from_child_session)?;
                            child = self
                                .sessions
                                .load_child_for_parent(&parent.id, &existing_id)
                                .await
                                .map_err(tool_error_from_child_session)?;
                            require_resident_project_identity(
                                parent_project_id.as_ref(),
                                &child,
                            )?;
                        }
                        // #74: re-seed the reused resident's posture from the LIVE
                        // parent. The resident-reuse path bypasses
                        // `create_child_action` (which seeds `bypass_permissions` /
                        // `no_human_approver` on the child's first run), so without
                        // this a resident created under one posture keeps a stale
                        // flag when reused under another (e.g. parent flipped from
                        // headless to interactive, or toggled bypass). Mirror BOTH
                        // flags so the reused resident matches the current parent.
                        let (parent_permission_mode, parent_no_human, parent_plan_active) = parent
                            .agent_runtime_state
                            .as_ref()
                            .map(|s| {
                                (
                                    s.effective_permission_mode(),
                                    s.no_human_approver,
                                    s.plan_mode.is_some(),
                                )
                            })
                            .unwrap_or_default();
                        let inherited_audit =
                            bamboo_domain::PermissionAuditSnapshot::from_metadata(&parent.metadata);
                        let policy_revision = inherited_audit
                            .as_ref()
                            .map(|audit| audit.policy_revision)
                            .unwrap_or_default();
                        let inherited_effective = if parent_plan_active {
                            bamboo_domain::PermissionMode::Plan
                        } else {
                            inherited_audit
                                .as_ref()
                                .filter(|audit| {
                                    audit.resolution.requested == parent_permission_mode
                                        && audit.resolution.is_consistent()
                                })
                                .map(|audit| audit.resolution.effective)
                                .unwrap_or_else(|| {
                                    bamboo_domain::resolve_permission_mode(
                                        parent_permission_mode,
                                        bamboo_domain::PermissionMode::Default,
                                    )
                                    .effective
                                })
                        };
                        let resolution = bamboo_domain::PermissionModeResolution {
                            requested: parent_permission_mode,
                            effective: inherited_effective,
                        };
                        let permission_audit = bamboo_domain::PermissionAuditSeed::bamboo_runtime(
                            policy_revision,
                            resolution,
                        );
                        // Commit posture + the newly requested, already
                        // authorized workspace before publishing runtime state
                        // or enqueueing the next resident task.
                        self.sessions
                            .save_resident_reuse_state(
                                &mut child,
                                &workspace,
                                workspace_source,
                                permission_audit,
                                parent_no_human,
                            )
                            .await
                            .map_err(tool_error_from_child_session)?;
                        // Reuse: reset => update (truncate + new task) then rerun;
                        // accumulate => send the task as a new message (auto-runs).
                        // A requested context fork is rendered for this task
                        // frame only. It never enters `assignment_prompt`
                        // metadata, so later resident reuse cannot compound it.
                        let assignment_background = fork_last_messages
                            .filter(|n| *n > 0)
                            .and_then(|n| {
                                child_session::render_forked_parent_context(&parent, n)
                            });
                        let wait_armed = if resident_context == "accumulate" {
                            let assignment = child_session::format_child_assignment_with_background(
                                &title,
                                &responsibility,
                                &subagent_type,
                                &prompt,
                                assignment_background.as_deref(),
                            );
                            let delivery = child_session::send_message_to_child_action_with_gate(
                                self.sessions.as_ref(),
                                &parent,
                                existing_id.clone(),
                                assignment,
                                Some(should_auto_run),
                                Some(false),
                                Some(ctx.tool_call_id.as_ref()),
                                requested_wait,
                                resident_delivery_gate.map(|gate| &gate.0),
                            )
                            .await
                            .map_err(tool_error_from_child_session)?;
                            if !requested_wait {
                                false
                            } else {
                                match delivery.get("status").and_then(|status| status.as_str()) {
                                    Some("queued") => true,
                                    Some("message_queued") => self
                                        .sessions
                                        .load_parent_session(&parent.id)
                                        .await
                                        .map_err(tool_error_from_child_session)?
                                        .agent_runtime_state
                                        .as_ref()
                                        .and_then(|state| state.waiting_for_children.as_ref())
                                        .is_some_and(|wait| {
                                            wait.child_session_ids
                                                .iter()
                                                .any(|id| id == &existing_id)
                                        }),
                                    _ => false,
                                }
                            }
                        } else {
                            child_session::update_child_action_with_background(
                                self.sessions.as_ref(),
                                &parent.id,
                                existing_id.clone(),
                                Some(title.clone()),
                                Some(responsibility.clone()),
                                Some(prompt.clone()),
                                Some(subagent_type.clone()),
                                Some(true),
                                None,
                                reasoning_effort,
                                assignment_background,
                                should_auto_run,
                            )
                            .await
                            .map_err(tool_error_from_child_session)?;
                            if should_auto_run {
                                if requested_wait {
                                    enqueue_waiting_child(
                                        self.sessions.as_ref(),
                                        &parent,
                                        &existing_id,
                                        ctx.tool_call_id.as_ref(),
                                        resident_delivery_gate,
                                    )
                                    .await?;
                                } else {
                                    enqueue_background_child(
                                        self.sessions.as_ref(),
                                        &parent,
                                        &existing_id,
                                        resident_delivery_gate,
                                    )
                                    .await?;
                                }
                            }
                            requested_wait
                        };
                        let (model, child_reasoning_effort) = self
                            .sessions
                            .load_child_for_parent(&parent.id, &existing_id)
                            .await
                            .map(|child| (child.model, child.reasoning_effort))
                            .unwrap_or_default();
                        (existing_id, model, true, child_reasoning_effort, wait_armed)
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
                        let effective_reasoning_effort = reasoning_effort.or_else(|| {
                            model_ref_override
                                .as_ref()
                                .and_then(|model_ref| model_ref.reasoning_effort)
                        });
                        let mut runtime_metadata =
                            self.resolver.resolve_runtime_metadata(&subagent_type).await;
                        if model.as_deref().is_some_and(|model| !model.trim().is_empty()) {
                            runtime_metadata.insert(child_session::named_profile::PROFILE_EXPLICIT_MODEL_KEY.into(), "true".into());
                        }
                        if let Some(packet) = &context_packet {
                            runtime_metadata.insert(bamboo_domain::CHILD_PACKET_INPUT_KEY.into(),
                                serde_json::to_string(packet).map_err(|_| ToolError::InvalidArguments(bamboo_domain::ChildContextPacketError::Invalid.to_string()))?);
                        }
                        let result = Box::pin(child_session::create_child_action(
                            self.sessions.as_ref(),
                            CreateChildInput {
                                parent_session: parent.clone(),
                                child_id: child_id.clone(),
                                title: title.clone(),
                                responsibility: responsibility.clone(),
                                assignment_prompt: prompt.clone(),
                                subagent_type: subagent_type.clone(),
                                workspace: workspace.clone(),
                                workspace_source,
                                model_override,
                                model_ref_override,
                                runtime_metadata,
                                read_only: false,
                                // Both synchronous and background launches use
                                // the same guarded admission after this save.
                                auto_run: false,
                                reasoning_effort: effective_reasoning_effort,
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
                        ))
                        .await
                        .map_err(tool_error_from_child_session)?;
                        {
                            let child = self.sessions.load_child_for_parent(&parent.id, &result.child_session_id)
                                .await.map_err(tool_error_from_child_session)?;
                            if let Some(binding) = bamboo_domain::ChildContextBinding::from_session(&child)
                                .map_err(|error| ToolError::Execution(error.to_string()))? {
                                packet_counts = Some(json!({"background_admitted": binding.payload.background.len(),
                                "background_omitted": binding.payload.background_omitted,
                                "child_created_at":child.created_at,"assignment_sha256":binding.assignment_sha256}));
                            }
                        }
                        // In the synchronous path, make the child visible to
                        // completion reconciliation before it can be launched.
                        self.sessions.ensure_child_indexed(&result.child_session_id).await;
                        if requested_wait {
                            enqueue_waiting_child(
                                self.sessions.as_ref(),
                                &parent,
                                &result.child_session_id,
                                ctx.tool_call_id.as_ref(),
                                launch_gate.as_deref(),
                            )
                            .await?;
                        } else if should_auto_run {
                            enqueue_background_child(
                                self.sessions.as_ref(),
                                &parent,
                                &result.child_session_id,
                                launch_gate.as_deref(),
                            )
                            .await?;
                        }
                        (
                            result.child_session_id,
                            result.model,
                            false,
                            effective_reasoning_effort,
                            requested_wait,
                        )
                    };

                // Synchronous launches were indexed before enqueue; retain the
                // original best-effort indexing for background creations too.
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
                    "reasoning_effort": child_reasoning_effort.map(|effort| effort.as_str()),
                    "status": status,
                    "lifecycle": resident_name.as_ref().map(|_| "resident"),
                    "resident_name": resident_name.clone(),
                    "reused": reused,
                    "context_packet": packet_counts,
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
                // subset when provided. Explicit ids the index POSITIVELY
                // reports terminal are dropped (issue #546): a terminal child
                // fires no further completion, so a wait registered over it
                // previously suspended the parent forever. Unknown ids are
                // KEPT (an index-less backend or a not-yet-indexed child must
                // not be mistaken for finished); if such an id turns out to be
                // bogus, the child-wait watchdog rescues the parent at runtime.
                let (targets, dropped): (Vec<String>, Vec<(String, String)>) =
                    match child_session_ids {
                        Some(ids) if !ids.is_empty() => {
                            let terminal =
                                self.sessions.terminal_child_ids(&parent.id, &ids).await;
                            partition_wait_targets(ids, &terminal)
                        }
                        _ => (
                            self.sessions.active_child_ids(&parent.id).await,
                            Vec::new(),
                        ),
                    };
                let dropped_ids: Vec<String> =
                    dropped.iter().map(|(id, _)| id.clone()).collect();

                // Policy short-circuit (issue #546): if the already-terminal
                // ids satisfy the policy on their own (`any` — any terminal;
                // `first_error` — any error-like terminal), suspending on the
                // remainder would sleep past an answer the model already has.
                if wait_already_satisfied_by_dropped(policy, &dropped) {
                    return tool_result(json!({
                        "status": "already_satisfied",
                        "parent_session_id": parent_session_id,
                        "satisfied_by": dropped
                            .iter()
                            .map(|(id, status)| json!({ "child_session_id": id, "status": status }))
                            .collect::<Vec<_>>(),
                        "still_active_child_ids": targets,
                        "wait_for": policy.as_str(),
                        "note": "The wait policy is already satisfied by finished child \
                                 session(s) — the parent was NOT suspended. Use SubAgent.get \
                                 with view=result to read their answers; call wait again (without those ids) \
                                 if you still need the remaining children.",
                    }))
                    .map(ToolOutcome::Completed);
                }

                if targets.is_empty() {
                    // Nothing left to wait on — never register an empty wait
                    // (that would suspend the parent with no child able to
                    // resume it). Any explicitly named children are already
                    // terminal: tell the model to read their results instead
                    // of suspending.
                    let note = if dropped_ids.is_empty() {
                        "No active child sessions to wait for; the parent continues running."
                            .to_string()
                    } else {
                        format!(
                            "The requested child session(s) [{}] are already finished; nothing \
                             to wait for. Use SubAgent.get with view=result to read their answers.",
                            dropped_ids.join(", ")
                        )
                    };
                    return tool_result(json!({
                        "status": "no_active_children",
                        "parent_session_id": parent_session_id,
                        "already_terminal_child_ids": dropped_ids,
                        "note": note,
                    }))
                    .map(ToolOutcome::Completed);
                }

                let count = self
                    .sessions
                    .register_parent_wait_for_children_tagged(
                        &parent.id,
                        &targets,
                        policy,
                        ctx.tool_call_id.as_ref(),
                    )
                    .await
                    .map_err(tool_error_from_child_session)?;

                waiting_for_children_tool_result(json!({
                    "status": "waiting",
                    "parent_session_id": parent_session_id,
                    "child_session_ids": targets,
                    "already_terminal_child_ids": dropped_ids,
                    "wait_for": policy.as_str(),
                    "waiting_on": count,
                }))
            }
            SubAgentArgs::List => {
                let result =
                    child_session::list_children_action(self.sessions.as_ref(), &parent.id).await;
                tool_result(result)
            }
            SubAgentArgs::Get { .. } => unreachable!("get returns before loading the root"),
            SubAgentArgs::Update {
                child_session_id,
                title,
                responsibility,
                prompt,
                subagent_type,
                reset_after_update,
                auto_run,
                model,
                reasoning_effort,
            } => {
                let model_ref_override = match model.as_deref() {
                    Some(spec) => Some(parse_model_spec(
                        spec,
                        &parent,
                        self.catalog.as_ref().map(|catalog| catalog.default_provider()),
                    )?),
                    None => None,
                };
                let result = child_session::update_child_action(
                    self.sessions.as_ref(),
                    &parent.id,
                    child_session_id.clone(),
                    title,
                    responsibility,
                    prompt,
                    subagent_type,
                    reset_after_update,
                    model_ref_override,
                    reasoning_effort,
                    auto_run.unwrap_or(false),
                )
                .await
                .map_err(tool_error_from_child_session)?;

                let should_auto_run = auto_run.unwrap_or(false);
                if should_auto_run {
                    enqueue_waiting_child(
                        self.sessions.as_ref(),
                        &parent,
                        &child_session_id,
                        ctx.tool_call_id.as_ref(),
                        launch_gate.as_deref(),
                    )
                    .await?;
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
                if result.get("status").and_then(|status| status.as_str()) == Some("queued") {
                    enqueue_waiting_child(
                        self.sessions.as_ref(),
                        &parent,
                        &child_session_id,
                        ctx.tool_call_id.as_ref(),
                        launch_gate.as_deref(),
                    )
                    .await?;
                } else {
                    // No launch is being performed, but preserve the existing
                    // synchronous behavior for an already-running child. The
                    // runner can finish between the running check and this
                    // registration, so recheck durable terminality afterwards.
                    let parent_before = self
                        .sessions
                        .load_parent_session(&parent.id)
                        .await
                        .map_err(tool_error_from_child_session)?;
                    let had_wait = parent_before
                        .agent_runtime_state
                        .as_ref()
                        .and_then(|state| state.waiting_for_children.as_ref())
                        .is_some_and(|wait| {
                            wait.child_session_ids
                                .iter()
                                .any(|id| id == &child_session_id)
                        });
                    if launch_gate.as_ref().is_some_and(|gate| gate.is_cancelled()) {
                        return Err(tool_error_from_child_session(cancelled_launch_error()));
                    }
                    if !had_wait {
                        self.sessions
                            .register_parent_wait_for_child(
                                &parent.id,
                                &child_session_id,
                                Some(ctx.tool_call_id.as_ref()),
                            )
                            .await
                            .map_err(tool_error_from_child_session)?;
                    }
                    if launch_gate
                        .as_ref()
                        .is_some_and(|gate| gate.is_cancelled())
                    {
                        if !had_wait {
                            self.sessions
                                .rollback_parent_wait_for_child(&parent.id, &child_session_id)
                                .await
                                .map_err(tool_error_from_child_session)?;
                        }
                        return Err(tool_error_from_child_session(cancelled_launch_error()));
                    }
                    let terminal = self
                        .sessions
                        .terminal_child_ids(&parent.id, std::slice::from_ref(&child_session_id))
                        .await;
                    let terminal_status = terminal
                        .into_iter()
                        .find(|(id, _)| id == &child_session_id)
                        .map(|(_, status)| status);
                    let terminal_status = match terminal_status {
                        Some(status) => Some(status),
                        None => self
                            .sessions
                            .load_child_for_parent(&parent.id, &child_session_id)
                            .await
                            .ok()
                            .and_then(|child| child.last_run_status())
                            .filter(|status| {
                                matches!(
                                    status.as_str(),
                                    "completed" | "error" | "timeout" | "cancelled" | "skipped"
                                )
                            }),
                    };
                    if let Some(status) = terminal_status {
                        // Reuse an earlier call's durable wait without taking
                        // ownership of it. Only undo a wait this call added.
                        if !had_wait {
                            self.sessions
                                .rollback_parent_wait_for_child(&parent.id, &child_session_id)
                                .await
                                .map_err(tool_error_from_child_session)?;
                        }
                        return tool_result(json!({
                            "child_session_id": child_session_id,
                            "status": "already_terminal",
                            "last_run_status": status,
                            "note": "The child finished while its existing run was being inspected; read its result instead of waiting for another completion.",
                        }))
                        .map(ToolOutcome::Completed);
                    }
                }
                waiting_for_children_tool_result(result)
            }
            SubAgentArgs::SendMessage {
                child_session_id,
                message,
                auto_run,
                interrupt_running,
            } => {
                let should_auto_run = auto_run.unwrap_or(true);
                let result = child_session::send_message_to_child_action_with_gate(
                    self.sessions.as_ref(),
                    &parent,
                    child_session_id.clone(),
                    message,
                    auto_run,
                    interrupt_running,
                    Some(ctx.tool_call_id.as_ref()),
                    should_auto_run,
                    launch_gate.as_deref().map(|gate| &gate.0),
                )
                .await
                .map_err(tool_error_from_child_session)?;
                let queued = should_auto_run
                    && result
                        .get("status")
                        .and_then(|value| value.as_str())
                        .is_some_and(|status| status == "queued");
                if queued {
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
    fn partition_wait_targets_drops_only_known_terminal_ids() {
        let (targets, dropped) = partition_wait_targets(
            vec!["done".into(), "running".into(), "unknown".into()],
            &[("done".to_string(), "completed".to_string())],
        );
        assert_eq!(targets, vec!["running".to_string(), "unknown".to_string()]);
        assert_eq!(dropped, vec![("done".to_string(), "completed".to_string())]);

        // Index-less backend: nothing reported terminal → nothing filtered.
        let (targets, dropped) = partition_wait_targets(vec!["a".into(), "b".into()], &[]);
        assert_eq!(targets, vec!["a".to_string(), "b".to_string()]);
        assert!(dropped.is_empty());

        // Everything already finished → nothing left to wait on.
        let (targets, dropped) = partition_wait_targets(
            vec!["a".into(), "b".into()],
            &[
                ("a".to_string(), "completed".to_string()),
                ("b".to_string(), "error".to_string()),
            ],
        );
        assert!(targets.is_empty());
        assert_eq!(dropped.len(), 2);
    }

    #[test]
    fn wait_short_circuits_when_dropped_ids_satisfy_the_policy() {
        let completed = [("a".to_string(), "completed".to_string())];
        let errored = [("a".to_string(), "timeout".to_string())];

        // `all`: waiting on the remainder is equivalent — never short-circuit.
        assert!(!wait_already_satisfied_by_dropped(
            ChildWaitPolicy::All,
            &completed
        ));
        assert!(!wait_already_satisfied_by_dropped(
            ChildWaitPolicy::All,
            &errored
        ));

        // `any`: ANY terminal child satisfies the wait before it is armed.
        assert!(wait_already_satisfied_by_dropped(
            ChildWaitPolicy::Any,
            &completed
        ));
        assert!(!wait_already_satisfied_by_dropped(
            ChildWaitPolicy::Any,
            &[]
        ));

        // `first_error`: only an error-like terminal child short-circuits; a
        // completed one still waits on the remainder (all-complete fallback).
        assert!(wait_already_satisfied_by_dropped(
            ChildWaitPolicy::FirstError,
            &errored
        ));
        assert!(!wait_already_satisfied_by_dropped(
            ChildWaitPolicy::FirstError,
            &completed
        ));
    }

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

    #[test]
    fn subagent_schema_advertises_the_actual_compact_logical_caller() {
        let schema = subagent_parameters_schema();
        assert!(schema.get("required").is_none());
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(
            schema["properties"]["intent"]["enum"],
            json!(["chat", "inspect", "control", "ask_parent"])
        );
        let actual: std::collections::BTreeSet<_> = schema["properties"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            actual,
            std::collections::BTreeSet::from(["intent", "target", "role", "message", "reply_to",])
        );
        for physical_or_runtime in [
            "model",
            "workspace",
            "worker_bin",
            "endpoint",
            "action",
            "auto_run",
        ] {
            assert!(schema["properties"].get(physical_or_runtime).is_none());
        }
    }

    #[test]
    fn update_deserializes_explicit_model() {
        let parsed: SubAgentArgs = serde_json::from_value(json!({
            "action": "update",
            "child_session_id": "child-1",
            "model": "easycli:gpt-5.6-luna"
        }))
        .expect("valid update args");

        match parsed {
            SubAgentArgs::Update { model, .. } => {
                assert_eq!(model.as_deref(), Some("easycli:gpt-5.6-luna"));
            }
            other => panic!("expected update args, got {other:?}"),
        }
    }

    #[test]
    fn subagent_model_facing_capability_text_is_runtime_scoped() {
        let description = subagent_tool_description();
        assert!(description.contains("tools and permissions exposed to it by the runtime"));
        assert!(!description.contains("full toolset"));
        assert!(!description.contains("full agent"));

        let schema = subagent_parameters_schema();
        let label_description = schema["properties"]["role"]["description"]
            .as_str()
            .expect("role description");
        assert!(label_description.contains("runtime exposes to the child"));
        for role in ["explorer", "implementer", "reviewer"] {
            assert!(label_description.contains(role));
        }
        assert!(label_description.contains("no builtin role is implicitly selected"));
        assert!(!label_description.contains("full agent"));
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
