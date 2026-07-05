//! Application-layer action functions for child session management.

use bamboo_domain::Session;
use chrono::Utc;
use serde_json::json;

use super::helpers::{
    compute_status_guidance, format_child_assignment, map_child_entry, metadata_text,
    normalize_non_empty_optional, normalize_required_text, render_forked_parent_context,
    replace_or_append_last_user_message, truncate_after_index, truncate_after_last_user,
};
use super::DELEGATION_NOTE;
use super::{
    ChildSessionEntry, ChildSessionError, ChildSessionPort, CreateChildInput, CreateChildResult,
    QueuedInjectedMessage,
};

pub async fn create_child_action(
    port: &dyn ChildSessionPort,
    input: CreateChildInput,
) -> Result<CreateChildResult, ChildSessionError> {
    use crate::runner::refresh_prompt_snapshot;
    use bamboo_agent_core::Message;

    // Use `new_child_of` so the child inherits the parent's tree root and a
    // depth of parent+1. For a root parent this is identical to the old
    // flat-tree behavior; for a child parent it enables nesting while keeping
    // `root_session_id` constant across the whole tree (completion/SSE keying).
    let mut child = Session::new_child_of(
        input.child_id.clone(),
        &input.parent_session,
        input
            .model_ref_override
            .as_ref()
            .map(|model_ref| model_ref.model.clone())
            .or_else(|| input.model_override.clone())
            .unwrap_or_else(|| input.parent_session.model.clone()),
        input.title.clone(),
    );

    if let Some(model_ref) = input.model_ref_override.clone() {
        child.model_ref = Some(model_ref.clone());
        child
            .metadata
            .insert("provider_name".to_string(), model_ref.provider);
    } else if let Some(parent_model_ref) = input.parent_session.model_ref.clone() {
        child.model_ref = Some(parent_model_ref.clone());
        child.set_provider_name(parent_model_ref.provider);
    } else if let Some(parent_provider) = input.parent_session.provider_name() {
        child.set_provider_name(parent_provider);
    }

    // Apply explicit reasoning_effort override if the LLM passed one;
    // otherwise leave at `None` (provider default). Per CreateChildInput
    // contract, children do NOT inherit the parent's reasoning_effort.
    if let Some(effort) = input.reasoning_effort {
        child.reasoning_effort = Some(effort);
    }

    // Children inherit the parent's "bypass permissions" mode: a bypassed
    // parent shouldn't be re-gated the moment it delegates work to a sub-agent.
    // Seed the child's runtime state so the flag is live from its first run
    // (startup carries it forward thereafter) and mirrored into the index.
    if input
        .parent_session
        .agent_runtime_state
        .as_ref()
        .is_some_and(|state| state.bypass_permissions)
    {
        child
            .agent_runtime_state
            .get_or_insert_with(bamboo_domain::AgentRuntimeState::default)
            .bypass_permissions = true;
    }

    // #73: children inherit "no interactive human approver" too — if the run has
    // no human to answer approvals (headless / scheduled / deployed), neither do
    // its sub-agents, so their gated actions must be model-reviewed locally
    // rather than escalated to a human who will never answer (300s fail-deny).
    if input
        .parent_session
        .agent_runtime_state
        .as_ref()
        .is_some_and(|state| state.no_human_approver)
    {
        child
            .agent_runtime_state
            .get_or_insert_with(bamboo_domain::AgentRuntimeState::default)
            .no_human_approver = true;
    }

    child.workspace = Some(input.workspace.clone());
    bamboo_agent_core::workspace_state::set_workspace(
        &child.id,
        std::path::PathBuf::from(&input.workspace),
    );

    child
        .metadata
        .insert("spawned_by".to_string(), "SubAgent".to_string());
    child.set_subagent_type(input.subagent_type.clone());
    child
        .metadata
        .insert("responsibility".to_string(), input.responsibility.clone());
    child.metadata.insert(
        "assignment_prompt".to_string(),
        input.assignment_prompt.clone(),
    );
    // Resident-agent tagging (plain metadata, like `responsibility` above). Only
    // a resident carries these; their presence is how a later create reuses this
    // session instead of minting a new one. Mirrored into the session index so
    // the lookup + the frontend can read them without loading session.json.
    if input.lifecycle.as_deref() == Some("resident") {
        child
            .metadata
            .insert("lifecycle".to_string(), "resident".to_string());
        if let Some(name) = input.resident_name.clone().filter(|n| !n.trim().is_empty()) {
            child.metadata.insert("resident_name".to_string(), name);
        }
        child.metadata.insert(
            "resident_context".to_string(),
            input
                .resident_context
                .clone()
                .filter(|c| matches!(c.as_str(), "reset" | "accumulate"))
                .unwrap_or_else(|| "reset".to_string()),
        );
    }
    child.set_last_run_status("pending");
    child.clear_last_run_error();

    // Apply runtime metadata (e.g. external agent routing).
    for (key, value) in input.runtime_metadata {
        child.metadata.insert(key, value);
    }

    // Sub-agents are first-class agents: assemble the SAME base system prompt a
    // top-level (root) session uses, then append a short delegation note. The
    // runtime context enhancement (workspace / instructions / tool guide /
    // memory / task list) is applied uniformly by the runner to whatever base
    // prompt the session carries — there is no root-only gate — so swapping the
    // base prompt is all that's needed to make a child behave like a full agent.
    let base_prompt = {
        let global = crate::prompt_defaults::read_global_default_system_prompt_template();
        if global.trim().is_empty() {
            crate::context::DEFAULT_BASE_PROMPT.to_string()
        } else {
            global
        }
    };
    let system_prompt = format!("{base_prompt}\n\n{DELEGATION_NOTE}");

    child
        .metadata
        .insert("base_system_prompt".to_string(), system_prompt.clone());

    child.add_message(Message::system(&system_prompt));

    // Child sessions get more aggressive compression: trigger at 70% instead
    // of the default 85%, target 35% instead of 40%. This prevents long child
    // tasks from exhausting the context window before the parent can intervene.
    if let Some(parent_budget) = input.parent_session.effective_token_budget() {
        let mut child_budget = parent_budget.clone();
        child_budget.compression_trigger_percent = 70;
        child_budget.compression_target_percent = 35;
        child.token_budget = Some(child_budget);
    }

    refresh_prompt_snapshot(&mut child);
    let assignment = format_child_assignment(
        &input.title,
        &input.responsibility,
        &input.subagent_type,
        &input.assignment_prompt,
    );
    // Phase 3: optionally fork a slice of the parent's recent context into the
    // child's task brief (model-controllable via the SubAgent tool's
    // `fork_last_messages`). `None`/0 keeps the child on a clean fresh context.
    let assignment = match input
        .context_fork
        .and_then(|n| render_forked_parent_context(&input.parent_session, n))
    {
        Some(forked) => format!("{forked}\n\n{assignment}"),
        None => assignment,
    };
    child.add_message(Message::user(assignment));

    if let Some(parent_task_list) = input.parent_session.task_list.clone() {
        child.set_task_list(parent_task_list);
    }

    // Persist any per-child tool denylist so the spawn path (enqueue_child_run
    // → SpawnJob.disabled_tools) can trim the child's toolset (e.g. a read-only
    // Guardian reviewer). Most children carry none and keep the full toolset.
    if let Some(ref disabled) = input.disabled_tools {
        if !disabled.is_empty() {
            child.metadata.insert(
                "disabled_tools".to_string(),
                serde_json::to_string(disabled).unwrap_or_default(),
            );
        }
    }

    let model = child.model.clone();
    port.save_child_session(&mut child).await?;
    if input.auto_run {
        port.enqueue_child_run(&input.parent_session, &child)
            .await?;
    }

    Ok(CreateChildResult {
        child_session_id: child.id,
        model,
    })
}

pub async fn list_children_action(
    port: &dyn ChildSessionPort,
    parent_id: &str,
) -> serde_json::Value {
    let children = port.list_children(parent_id).await;
    json!({
        "parent_session_id": parent_id,
        "children": children.iter().map(map_child_entry).collect::<Vec<_>>(),
        "count": children.len(),
    })
}

/// A node in the materialized parent→child session graph (Phase 6: persistent
/// multi-level nesting graph). `children` are the transitive descendants.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct SessionTreeNode {
    pub session_id: String,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_run_status: Option<String>,
    pub depth: u32,
    pub children: Vec<SessionTreeNode>,
}

/// Assemble the transitive parent→child tree rooted at `root_id` from a
/// pre-fetched adjacency map (pure — unit-testable without a port). Bounded by
/// `max_depth`; a first-visit guard breaks cycles (a re-encountered session
/// becomes a leaf rather than recursing forever).
pub fn assemble_session_tree(
    root_id: &str,
    root_title: &str,
    adjacency: &std::collections::HashMap<String, Vec<ChildSessionEntry>>,
    max_depth: u32,
) -> SessionTreeNode {
    fn build(
        id: &str,
        title: &str,
        status: Option<String>,
        depth: u32,
        max_depth: u32,
        adjacency: &std::collections::HashMap<String, Vec<ChildSessionEntry>>,
        visited: &mut std::collections::HashSet<String>,
    ) -> SessionTreeNode {
        let first_visit = visited.insert(id.to_string());
        let mut children = Vec::new();
        if first_visit && depth < max_depth {
            if let Some(kids) = adjacency.get(id) {
                for kid in kids {
                    children.push(build(
                        &kid.child_session_id,
                        &kid.title,
                        kid.last_run_status.clone(),
                        depth + 1,
                        max_depth,
                        adjacency,
                        visited,
                    ));
                }
            }
        }
        SessionTreeNode {
            session_id: id.to_string(),
            title: title.to_string(),
            last_run_status: status,
            depth,
            children,
        }
    }
    let mut visited = std::collections::HashSet::new();
    build(
        root_id,
        root_title,
        None,
        0,
        max_depth,
        adjacency,
        &mut visited,
    )
}

/// Materialize the full transitive parent→child session graph rooted at
/// `root_id` from the persisted session index (Phase 6). BFS-fetches each
/// level's children via [`ChildSessionPort::list_children`] (a first-visit guard
/// and a hard node cap protect against cycles / runaway trees), then assembles the
/// tree. The graph is derived from durable index state, so it survives restarts.
pub async fn build_session_tree_action(
    port: &dyn ChildSessionPort,
    root_id: &str,
    max_depth: u32,
) -> SessionTreeNode {
    use std::collections::{HashMap, HashSet, VecDeque};
    const NODE_CAP: usize = 5000;

    let root_title = port
        .load_root_session(root_id)
        .await
        .map(|s| s.title)
        .unwrap_or_default();

    let mut adjacency: HashMap<String, Vec<ChildSessionEntry>> = HashMap::new();
    let mut visited: HashSet<String> = HashSet::new();
    let mut queue: VecDeque<(String, u32)> = VecDeque::new();
    queue.push_back((root_id.to_string(), 0));

    while let Some((id, depth)) = queue.pop_front() {
        if depth >= max_depth || adjacency.len() >= NODE_CAP || !visited.insert(id.clone()) {
            continue;
        }
        let kids = port.list_children(&id).await;
        for kid in &kids {
            queue.push_back((kid.child_session_id.clone(), depth + 1));
        }
        adjacency.insert(id, kids);
    }

    assemble_session_tree(root_id, &root_title, &adjacency, max_depth)
}

pub async fn get_child_action(
    port: &dyn ChildSessionPort,
    parent_id: &str,
    child_session_id: String,
) -> Result<serde_json::Value, ChildSessionError> {
    let child = port
        .load_child_for_parent(parent_id, &child_session_id)
        .await?;

    let status = metadata_text(&child, "last_run_status");
    let runner_info = port.get_child_runner_info(&child.id).await;

    Ok(json!({
        "child_session_id": child.id,
        "title": child.title,
        "model": child.model,
        "pinned": child.pinned,
        "message_count": child.messages.len(),
        "is_running": port.is_child_running(&child.id).await,
        "last_run_status": status,
        "last_run_error": metadata_text(&child, "last_run_error"),
        "responsibility": metadata_text(&child, "responsibility"),
        "subagent_type": metadata_text(&child, "subagent_type"),
        "prompt": metadata_text(&child, "assignment_prompt"),
        "latest_user_message": child
            .messages
            .iter()
            .rposition(|message| matches!(message.role, bamboo_agent_core::Role::User))
            .and_then(|idx| child.messages.get(idx))
            .map(|message| message.content.clone()),
        "runtime_kind": metadata_text(&child, "runtime.kind"),
        "external_protocol": metadata_text(&child, "external.protocol"),
        "external_agent_id": metadata_text(&child, "external.agent_id"),
        "a2a_context_id": metadata_text(&child, "a2a.context_id"),
        "a2a_latest_task_id": metadata_text(&child, "a2a.latest_task_id"),
        "a2a_last_state": metadata_text(&child, "a2a.last_state"),
        "runner_started_at": runner_info.as_ref().and_then(|r| r.started_at.map(|t| t.to_rfc3339())),
        "runner_completed_at": runner_info.as_ref().and_then(|r| r.completed_at.map(|t| t.to_rfc3339())),
        "last_tool_name": runner_info.as_ref().and_then(|r| r.last_tool_name.clone()),
        "last_tool_phase": runner_info.as_ref().and_then(|r| r.last_tool_phase.clone()),
        "last_event_at": runner_info.as_ref().and_then(|r| r.last_event_at.map(|t| t.to_rfc3339())),
        "round_count": runner_info.as_ref().map(|r| r.round_count).unwrap_or(0),
        "has_pending_injected_messages": child.has_pending_injected_messages(),
        "guidance": compute_status_guidance(status.as_deref(), runner_info.as_ref(), child.has_pending_injected_messages()),
    }))
}

#[allow(clippy::too_many_arguments)]
pub async fn update_child_action(
    port: &dyn ChildSessionPort,
    parent_id: &str,
    child_session_id: String,
    title: Option<String>,
    responsibility: Option<String>,
    prompt: Option<String>,
    subagent_type: Option<String>,
    reset_after_update: Option<bool>,
    reasoning_effort: Option<bamboo_domain::ReasoningEffort>,
) -> Result<serde_json::Value, ChildSessionError> {
    let mut child = port
        .load_child_for_parent(parent_id, &child_session_id)
        .await?;

    let title = normalize_non_empty_optional(title, "title")?;
    let responsibility = normalize_non_empty_optional(responsibility, "responsibility")?;
    let prompt = normalize_non_empty_optional(prompt, "prompt")?;
    let subagent_type = normalize_non_empty_optional(subagent_type, "subagent_type")?;

    let should_refresh_assignment =
        responsibility.is_some() || prompt.is_some() || subagent_type.is_some();

    if title.is_none() && !should_refresh_assignment && reasoning_effort.is_none() {
        return Err(ChildSessionError::InvalidArguments(
            "update requires at least one field: title/responsibility/prompt/subagent_type/reasoning_effort"
                .to_string(),
        ));
    }

    if let Some(effort) = reasoning_effort {
        child.reasoning_effort = Some(effort);
    }

    if let Some(title) = title {
        child.title = title;
    }

    let mut messages_removed = 0usize;

    if should_refresh_assignment {
        let effective_responsibility = normalize_required_text(
            responsibility.or_else(|| metadata_text(&child, "responsibility")),
            "responsibility",
        )?;
        let effective_subagent_type = normalize_required_text(
            subagent_type.or_else(|| metadata_text(&child, "subagent_type")),
            "subagent_type",
        )?;
        let effective_prompt = normalize_required_text(
            prompt.or_else(|| metadata_text(&child, "assignment_prompt")),
            "prompt",
        )?;

        child.metadata.insert(
            "responsibility".to_string(),
            effective_responsibility.clone(),
        );
        child
            .metadata
            .insert("subagent_type".to_string(), effective_subagent_type.clone());
        child
            .metadata
            .insert("assignment_prompt".to_string(), effective_prompt.clone());
        child.set_last_run_status("pending");
        child.clear_last_run_error();

        let assignment = format_child_assignment(
            &child.title,
            &effective_responsibility,
            &effective_subagent_type,
            &effective_prompt,
        );
        let user_index = replace_or_append_last_user_message(&mut child, assignment);

        if reset_after_update.unwrap_or(true) {
            messages_removed = truncate_after_index(&mut child, user_index);
        }
    }

    child.updated_at = Utc::now();
    port.save_child_session(&mut child).await?;

    Ok(json!({
        "child_session_id": child.id,
        "title": child.title,
        "messages_removed": messages_removed,
        "last_run_status": metadata_text(&child, "last_run_status"),
        "note": "Child session updated in place. Use action=run to execute the same child session.",
    }))
}

pub async fn run_child_action(
    port: &dyn ChildSessionPort,
    parent: &Session,
    child_session_id: String,
    reset_to_last_user: Option<bool>,
) -> Result<serde_json::Value, ChildSessionError> {
    let mut child = port
        .load_child_for_parent(&parent.id, &child_session_id)
        .await?;

    if port.is_child_running(&child.id).await {
        return Ok(json!({
            "child_session_id": child.id,
            "status": "already_running",
            "note": "Child session is already running.",
        }));
    }

    let mut messages_removed = 0usize;
    if reset_to_last_user.unwrap_or(true) {
        messages_removed = truncate_after_last_user(&mut child)?;
    }

    child.set_last_run_status("pending");
    child.clear_last_run_error();
    child.updated_at = Utc::now();
    port.save_child_session(&mut child).await?;

    port.enqueue_child_run(parent, &child).await?;

    Ok(json!({
        "child_session_id": child.id,
        "status": "queued",
        "messages_removed": messages_removed,
        "note": "Queued existing child session for retry in place.",
    }))
}

pub async fn send_message_to_child_action(
    port: &dyn ChildSessionPort,
    parent: &Session,
    child_session_id: String,
    message: String,
    auto_run: Option<bool>,
    interrupt_running: Option<bool>,
) -> Result<serde_json::Value, ChildSessionError> {
    let mut child = port
        .load_child_for_parent(&parent.id, &child_session_id)
        .await?;

    let is_running = port.is_child_running(&child.id).await;
    let should_interrupt = interrupt_running.unwrap_or(false);

    if is_running && should_interrupt {
        port.cancel_child_run_and_wait(&child.id).await?;
        child = port
            .load_child_for_parent(&parent.id, &child_session_id)
            .await?;
    }

    let message = normalize_required_text(Some(message), "message")?;

    if is_running && !should_interrupt {
        // Actor child with a live WS connection: deliver in-band. The worker's
        // agent loop admits it at the next round boundary — the same semantics
        // as the queued path below, extended across the process boundary. The
        // message is appended to the durable transcript immediately so the
        // next activation rehydrates with it and nothing is delivered twice.
        if crate::external_agents::live::deliver_message(&child.id, &message) {
            child.add_message(bamboo_agent_core::Message::user(message.clone()));
            port.save_child_session(&mut child).await?;
            return Ok(json!({
                "child_session_id": child.id,
                "status": "message_delivered_live",
                "auto_run": false,
                "message": message,
                "message_count": child.messages.len(),
                "note": "Message delivered to the running actor in-band; it will be admitted at the next round boundary without canceling progress.",
            }));
        }

        // Store the message in session runtime metadata so the running agent
        // loop can merge it at the next turn boundary without canceling
        // progress. Routed through the typed accessor (dual-writes the typed
        // field + the legacy `pending_injected_messages` JSON string mirror).
        let mut pending = child.pending_injected_messages().unwrap_or_default();
        let queued = QueuedInjectedMessage {
            content: message.clone(),
            created_at: Some(chrono::Utc::now()),
        };
        pending.push(serde_json::to_value(&queued).unwrap_or(serde_json::Value::Null));
        child.set_pending_injected_messages(pending);
        port.save_child_session(&mut child).await?;

        // Race guard: the `is_running` snapshot above may be stale — if the
        // child finished between that check and this queue write, nothing
        // would ever drain the pending message. Re-check and schedule a run
        // so the message is processed instead of stranding.
        if !port.is_child_running(&child.id).await {
            port.enqueue_child_run(parent, &child).await?;
            return Ok(json!({
                "child_session_id": child.id,
                "status": "queued",
                "auto_run": true,
                "message": message,
                "message_count": child.messages.len(),
                "note": "Child finished while the message was being queued; a new run was scheduled to process it.",
            }));
        }

        return Ok(json!({
            "child_session_id": child.id,
            "status": "message_queued",
            "auto_run": false,
            "message": message,
            "message_count": child.messages.len(),
            "note": "Message queued for the child session. It will be picked up at the next turn boundary without canceling current progress.",
        }));
    }

    child.add_message(bamboo_agent_core::Message::user(message.clone()));
    child.set_last_run_status("pending");
    child.clear_last_run_error();
    port.save_child_session(&mut child).await?;

    let should_auto_run = auto_run.unwrap_or(true);
    if should_auto_run {
        port.enqueue_child_run(parent, &child).await?;
    }

    Ok(json!({
        "child_session_id": child.id,
        "status": if should_auto_run { "queued" } else { "pending" },
        "auto_run": should_auto_run,
        "message": message,
        "message_count": child.messages.len(),
        "note": if should_auto_run {
            "Follow-up message appended and child session queued."
        } else {
            "Follow-up message appended. Use action=run to execute the child session."
        },
    }))
}

pub async fn cancel_child_action(
    port: &dyn ChildSessionPort,
    parent_id: &str,
    child_session_id: String,
) -> Result<serde_json::Value, ChildSessionError> {
    // Validate ownership before doing anything.
    let _ = port
        .load_child_for_parent(parent_id, &child_session_id)
        .await?;
    port.cancel_child_run_and_wait(&child_session_id).await?;

    // RELOAD after the wait — writing the pre-wait snapshot would clobber
    // whatever the finishing run persisted (its terminal status AND any
    // messages it appended). And if the child completed naturally while the
    // cancel was in flight, keep that truth instead of mislabeling it.
    let mut child = port
        .load_child_for_parent(parent_id, &child_session_id)
        .await?;
    let latest_status = child.last_run_status().unwrap_or_default();
    if matches!(latest_status.as_str(), "completed" | "error") {
        return Ok(json!({
            "child_session_id": child_session_id,
            "status": latest_status,
            "note": "Child reached a natural terminal state while the cancel was in flight; its real outcome was kept.",
        }));
    }
    child.set_last_run_status("cancelled");
    child.set_last_run_error("Cancelled by parent");
    port.save_child_session(&mut child).await?;
    Ok(json!({
        "child_session_id": child_session_id,
        "status": "cancelled",
    }))
}

pub async fn delete_child_action(
    port: &dyn ChildSessionPort,
    parent_id: &str,
    child_session_id: String,
) -> Result<serde_json::Value, ChildSessionError> {
    // Load child first to get its ID (port.delete_child_session handles cancellation + cleanup)
    let child = port
        .load_child_for_parent(parent_id, &child_session_id)
        .await?;
    let result = port.delete_child_session(parent_id, &child.id).await?;

    if !result.deleted {
        return Err(ChildSessionError::Execution(format!(
            "child session was not deleted: {}",
            child.id
        )));
    }

    Ok(json!({
        "child_session_id": child.id,
        "deleted": true,
        "cancelled_running_child": result.cancelled_running_child,
    }))
}

#[cfg(test)]
mod tree_tests {
    use super::super::ChildSessionEntry;
    use super::assemble_session_tree;
    use std::collections::HashMap;

    fn entry(id: &str, title: &str) -> ChildSessionEntry {
        ChildSessionEntry {
            child_session_id: id.to_string(),
            title: title.to_string(),
            pinned: false,
            message_count: 0,
            updated_at: String::new(),
            last_run_status: Some("completed".to_string()),
            last_run_error: None,
        }
    }

    #[test]
    fn assembles_multi_level_tree() {
        let mut adj: HashMap<String, Vec<ChildSessionEntry>> = HashMap::new();
        adj.insert(
            "root".into(),
            vec![entry("c1", "child 1"), entry("c2", "child 2")],
        );
        adj.insert("c1".into(), vec![entry("g1", "grandchild")]);

        let tree = assemble_session_tree("root", "Root", &adj, 8);
        assert_eq!(tree.session_id, "root");
        assert_eq!(tree.depth, 0);
        assert_eq!(tree.children.len(), 2);
        let c1 = tree.children.iter().find(|n| n.session_id == "c1").unwrap();
        assert_eq!(c1.depth, 1);
        assert_eq!(c1.children.len(), 1);
        assert_eq!(c1.children[0].session_id, "g1");
        assert_eq!(c1.children[0].depth, 2);
        let c2 = tree.children.iter().find(|n| n.session_id == "c2").unwrap();
        assert!(c2.children.is_empty());
    }

    #[test]
    fn depth_cap_stops_descent() {
        let mut adj: HashMap<String, Vec<ChildSessionEntry>> = HashMap::new();
        adj.insert("root".into(), vec![entry("c1", "c1")]);
        adj.insert("c1".into(), vec![entry("g1", "g1")]);
        let tree = assemble_session_tree("root", "Root", &adj, 1);
        assert_eq!(tree.children.len(), 1);
        assert!(
            tree.children[0].children.is_empty(),
            "depth cap stops expansion at depth 1"
        );
    }

    #[test]
    fn cycle_is_broken_by_first_visit_guard() {
        let mut adj: HashMap<String, Vec<ChildSessionEntry>> = HashMap::new();
        adj.insert("a".into(), vec![entry("b", "b")]);
        adj.insert("b".into(), vec![entry("a", "a")]); // cycle a → b → a
        let tree = assemble_session_tree("a", "A", &adj, 100);
        assert_eq!(tree.children.len(), 1);
        let b = &tree.children[0];
        assert_eq!(b.session_id, "b");
        assert_eq!(b.children.len(), 1);
        let a2 = &b.children[0];
        assert_eq!(a2.session_id, "a");
        assert!(a2.children.is_empty(), "cycle must terminate as a leaf");
    }
}
