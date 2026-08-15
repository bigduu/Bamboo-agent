use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};

use crate::api::types::{AgentEvent, SessionTreeKind, SessionTreeSummary};

pub(crate) const MAX_SUBAGENT_TREE_SESSIONS: usize = 1_000;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum SubagentTreeStatus {
    #[default]
    Idle,
    Running,
    WaitingForInput,
    WaitingForPermission,
    Completed,
    Cancelled,
    Error,
}

impl SubagentTreeStatus {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Running => "running",
            Self::WaitingForInput => "waiting for input",
            Self::WaitingForPermission => "waiting for permission",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::Error => "error",
        }
    }

    pub(crate) fn glyph(self) -> &'static str {
        match self {
            Self::Idle => "·",
            Self::Running => "▶",
            Self::WaitingForInput => "?",
            Self::WaitingForPermission => "!",
            Self::Completed => "✓",
            Self::Cancelled => "■",
            Self::Error => "✗",
        }
    }

    pub(crate) fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled | Self::Error)
    }

    pub(crate) fn is_pending(self) -> bool {
        matches!(self, Self::WaitingForInput | Self::WaitingForPermission)
    }

    fn from_terminal_label(label: &str) -> Self {
        let normalized = label.to_ascii_lowercase();
        if normalized.contains("error") || normalized.contains("fail") {
            Self::Error
        } else if normalized.contains("cancel")
            || normalized.contains("stop")
            || normalized.contains("skip")
        {
            Self::Cancelled
        } else if normalized.contains("wait") || normalized.contains("pause") {
            Self::WaitingForInput
        } else {
            Self::Completed
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SubagentTreeNode {
    pub(crate) summary: SessionTreeSummary,
    live_status: Option<SubagentTreeStatus>,
    pub(crate) round_count: Option<u32>,
    pub(crate) activity: Option<String>,
    pub(crate) live_error: Option<String>,
    pub(crate) event_updated_at: Option<DateTime<Utc>>,
    pub(crate) pending_permission: bool,
    queued_permission: bool,
}

impl SubagentTreeNode {
    fn new(summary: SessionTreeSummary) -> Self {
        Self {
            summary,
            live_status: None,
            round_count: None,
            activity: None,
            live_error: None,
            event_updated_at: None,
            pending_permission: false,
            queued_permission: false,
        }
    }

    pub(crate) fn status(&self) -> SubagentTreeStatus {
        if self.pending_permission || self.queued_permission {
            return SubagentTreeStatus::WaitingForPermission;
        }
        if let Some(status) = self.live_status {
            return status;
        }
        if self.summary.has_pending_question {
            return SubagentTreeStatus::WaitingForInput;
        }
        if self.summary.is_running {
            return SubagentTreeStatus::Running;
        }
        self.summary
            .last_run_status
            .as_deref()
            .map(SubagentTreeStatus::from_terminal_label)
            .unwrap_or_default()
    }

    pub(crate) fn title(&self) -> &str {
        let title = self.summary.title.trim();
        if title.is_empty() {
            "sub-agent"
        } else {
            title
        }
    }

    pub(crate) fn error(&self) -> Option<&str> {
        self.live_error
            .as_deref()
            .or(self.summary.last_run_error.as_deref())
    }

    pub(crate) fn last_update(&self) -> Option<DateTime<Utc>> {
        self.event_updated_at
            .or(self.summary.last_activity_at)
            .or(self.summary.updated_at)
    }

    pub(crate) fn metadata_incomplete(&self, root_session_id: &str) -> bool {
        self.summary.id != root_session_id
            && (self
                .summary
                .parent_session_id
                .as_deref()
                .is_none_or(str::is_empty)
                || self.summary.root_session_id.is_empty()
                || self.summary.kind == SessionTreeKind::Unknown)
    }

    fn set_live_status(&mut self, status: SubagentTreeStatus, activity: impl Into<String>) {
        self.live_status = Some(status);
        self.activity = Some(activity.into());
        self.event_updated_at = Some(Utc::now());
        if status != SubagentTreeStatus::Error {
            self.live_error = None;
        }
    }

    fn note_running(&mut self, activity: impl Into<String>, authoritative_successor: bool) {
        let current = self.status();
        if current.is_terminal() && !authoritative_successor {
            return;
        }
        if current.is_pending() && !authoritative_successor {
            self.activity = Some(activity.into());
            self.event_updated_at = Some(Utc::now());
            return;
        }
        self.pending_permission = false;
        self.set_live_status(SubagentTreeStatus::Running, activity);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SubagentTreeRow {
    pub(crate) session_id: String,
    pub(crate) depth: usize,
    pub(crate) has_children: bool,
    pub(crate) expanded: bool,
}

#[derive(Debug)]
pub(crate) struct SubagentTreeState {
    pub(crate) epoch: u64,
    pub(crate) active_session_id: String,
    pub(crate) root_session_id: String,
    pub(crate) nodes: HashMap<String, SubagentTreeNode>,
    pub(crate) visible: Vec<SubagentTreeRow>,
    pub(crate) selected: usize,
    expanded: HashSet<String>,
    explicitly_collapsed: HashSet<String>,
    pub(crate) loading_root: bool,
    pub(crate) loading_page: bool,
    pub(crate) error: Option<String>,
    pub(crate) scanned: usize,
    pub(crate) total: usize,
    pub(crate) page_limit: usize,
    pub(crate) next_offset: Option<usize>,
    pub(crate) capped: bool,
}

impl SubagentTreeState {
    pub(crate) fn new(epoch: u64, active_session_id: String) -> Self {
        Self {
            epoch,
            root_session_id: active_session_id.clone(),
            active_session_id,
            nodes: HashMap::new(),
            visible: Vec::new(),
            selected: 0,
            expanded: HashSet::new(),
            explicitly_collapsed: HashSet::new(),
            loading_root: true,
            loading_page: false,
            error: None,
            scanned: 0,
            total: 0,
            page_limit: 0,
            next_offset: None,
            capped: false,
        }
    }

    pub(crate) fn selected_id(&self) -> Option<&str> {
        self.visible
            .get(self.selected)
            .map(|row| row.session_id.as_str())
    }

    pub(crate) fn selected_node(&self) -> Option<&SubagentTreeNode> {
        self.selected_id().and_then(|id| self.nodes.get(id))
    }

    pub(crate) fn install_root(&mut self, summary: SessionTreeSummary) {
        self.loading_root = false;
        self.upsert_summary(summary);
        self.rebuild();
    }

    pub(crate) fn install_page(
        &mut self,
        sessions: Vec<SessionTreeSummary>,
        total: usize,
        limit: usize,
        offset: usize,
        next_offset: Option<usize>,
    ) {
        self.loading_page = false;
        self.scanned = self.scanned.max(offset.saturating_add(sessions.len()));
        self.total = total;
        if limit > 0 {
            self.page_limit = limit;
        }
        for session in sessions {
            self.upsert_summary(session);
        }
        self.next_offset = next_offset.filter(|_| self.scanned < MAX_SUBAGENT_TREE_SESSIONS);
        self.capped = self.scanned >= MAX_SUBAGENT_TREE_SESSIONS && self.scanned < total;
        self.rebuild();
    }

    fn upsert_summary(&mut self, summary: SessionTreeSummary) {
        if let Some(existing) = self.nodes.get_mut(&summary.id) {
            // Live event state is intentionally retained: a page request may
            // have been issued before the event and complete afterward.
            existing.summary = summary;
        } else {
            self.nodes
                .insert(summary.id.clone(), SubagentTreeNode::new(summary));
        }
    }

    fn ensure_node(&mut self, id: &str) -> &mut SubagentTreeNode {
        self.nodes
            .entry(id.to_string())
            .or_insert_with(|| SubagentTreeNode::new(SessionTreeSummary::placeholder(id)))
    }

    pub(crate) fn apply_started(
        &mut self,
        parent_session_id: &str,
        child_session_id: &str,
        title: Option<String>,
        authoritative_successor: bool,
    ) {
        self.ensure_node(parent_session_id);
        let root = self.root_session_id.clone();
        let child = self.ensure_node(child_session_id);
        if child
            .summary
            .parent_session_id
            .as_deref()
            .is_none_or(str::is_empty)
        {
            child.summary.parent_session_id = Some(parent_session_id.to_string());
        }
        if child.summary.root_session_id.is_empty() {
            child.summary.root_session_id = root;
        }
        if let Some(title) = title.filter(|title| !title.trim().is_empty()) {
            child.summary.title = title;
        }
        child.note_running("started", authoritative_successor);
        if authoritative_successor {
            child.round_count = None;
        }
        self.rebuild();
    }

    pub(crate) fn apply_heartbeat(&mut self, child_session_id: &str) {
        let child = self.ensure_node(child_session_id);
        child.note_running("heartbeat", false);
        self.rebuild();
    }

    pub(crate) fn apply_completed(
        &mut self,
        child_session_id: &str,
        status: &str,
        error: Option<String>,
    ) {
        let child = self.ensure_node(child_session_id);
        child.pending_permission = false;
        child.queued_permission = false;
        let next = SubagentTreeStatus::from_terminal_label(status);
        child.set_live_status(next, status);
        child.live_error = error;
        self.rebuild();
    }

    pub(crate) fn apply_runner_progress(&mut self, session_id: &str, round_count: u32) {
        let child = self.ensure_node(session_id);
        if child.status().is_terminal() {
            return;
        }
        child.round_count = Some(round_count);
        // Unlike a heartbeat, exact runner progress proves that a previously
        // blocked session resumed. Clear the transient pending signal while
        // still refusing to reopen a terminal generation above.
        child.pending_permission = false;
        child.set_live_status(SubagentTreeStatus::Running, format!("round {round_count}"));
        self.rebuild();
    }

    pub(crate) fn mark_running(
        &mut self,
        session_id: &str,
        activity: impl Into<String>,
        authoritative_successor: bool,
    ) {
        self.ensure_node(session_id)
            .note_running(activity, authoritative_successor);
        self.rebuild();
    }

    pub(crate) fn mark_waiting_input(&mut self, session_id: &str) {
        self.ensure_node(session_id)
            .set_live_status(SubagentTreeStatus::WaitingForInput, "clarification pending");
        self.rebuild();
    }

    pub(crate) fn mark_waiting_permission(&mut self, session_id: &str, activity: String) {
        let node = self.ensure_node(session_id);
        node.pending_permission = true;
        node.activity = Some(activity);
        node.event_updated_at = Some(Utc::now());
        self.rebuild();
    }

    pub(crate) fn apply_forwarded_event(&mut self, child_session_id: &str, event: &AgentEvent) {
        match event {
            AgentEvent::ExecutionStarted { session_id, .. } if session_id == child_session_id => {
                self.ensure_node(child_session_id)
                    .note_running("execution started", true);
            }
            AgentEvent::RunnerProgress {
                session_id,
                round_count,
            } if session_id == child_session_id => {
                self.apply_runner_progress(session_id, *round_count);
                return;
            }
            AgentEvent::NeedClarification { .. } => {
                self.ensure_node(child_session_id)
                    .set_live_status(SubagentTreeStatus::WaitingForInput, "clarification pending");
            }
            AgentEvent::ToolApprovalRequested { tool_name, .. } => {
                let child = self.ensure_node(child_session_id);
                child.pending_permission = true;
                child.activity = Some(format!("permission for {tool_name}"));
                child.event_updated_at = Some(Utc::now());
            }
            AgentEvent::Complete { .. } => {
                self.apply_completed(child_session_id, "completed", None);
                return;
            }
            AgentEvent::Cancelled { message } => {
                self.apply_completed(child_session_id, "cancelled", message.clone());
                return;
            }
            AgentEvent::Error { message } => {
                self.apply_completed(child_session_id, "error", Some(message.clone()));
                return;
            }
            AgentEvent::SubAgentStarted {
                child_session_id: descendant_id,
                title,
            } => {
                self.apply_started(child_session_id, descendant_id, title.clone(), true);
                return;
            }
            AgentEvent::SubAgentEvent {
                child_session_id: descendant_id,
                event,
            } => {
                self.apply_started(child_session_id, descendant_id, None, false);
                self.apply_forwarded_event(descendant_id, event);
                return;
            }
            AgentEvent::SubAgentHeartbeat {
                child_session_id: descendant_id,
            } => {
                self.apply_heartbeat(descendant_id);
                return;
            }
            AgentEvent::SubAgentCompleted {
                child_session_id: descendant_id,
                status,
                error,
            } => {
                self.apply_completed(descendant_id, status, error.clone());
                return;
            }
            AgentEvent::ChildApprovalRequested {
                child_session_id: requested_child,
                tool_name,
                ..
            } => {
                let child = self.ensure_node(requested_child);
                child.pending_permission = true;
                child.activity = Some(format!("permission for {tool_name}"));
                child.event_updated_at = Some(Utc::now());
            }
            AgentEvent::ChildApprovalChanged {
                parent_session_id,
                child_session_id: requested_child,
                version,
                status,
                ..
            } => {
                if parent_session_id == child_session_id {
                    self.apply_child_approval_changed(
                        parent_session_id,
                        requested_child,
                        status,
                        *version > 0,
                    );
                }
                return;
            }
            AgentEvent::Token { .. }
            | AgentEvent::ReasoningToken { .. }
            | AgentEvent::ToolToken { .. }
            | AgentEvent::ToolStart { .. }
            | AgentEvent::ToolComplete { .. }
            | AgentEvent::ToolError { .. }
            | AgentEvent::ToolLifecycle { .. } => {
                self.ensure_node(child_session_id)
                    .note_running("active", false);
            }
            _ => {}
        }
        self.rebuild();
    }

    /// Apply the durable parent/child approval identity without allowing a
    /// compatibility terminal frame (version 0) to clear a newer request.
    pub(crate) fn apply_child_approval_changed(
        &mut self,
        parent_session_id: &str,
        child_session_id: &str,
        status: &str,
        authoritative_identity: bool,
    ) {
        self.ensure_node(parent_session_id);
        let root = self.root_session_id.clone();
        let child = self.ensure_node(child_session_id);
        if child
            .summary
            .parent_session_id
            .as_deref()
            .is_none_or(str::is_empty)
        {
            child.summary.parent_session_id = Some(parent_session_id.to_string());
        }
        if child.summary.root_session_id.is_empty() {
            child.summary.root_session_id = root;
        }

        if status == "pending" {
            child.pending_permission = true;
            child.activity = Some("approval pending".to_string());
            child.event_updated_at = Some(Utc::now());
        } else if authoritative_identity {
            child.pending_permission = false;
            child.queued_permission = false;
            if child.live_status == Some(SubagentTreeStatus::WaitingForPermission) {
                child.live_status = None;
            }
            child.activity = Some(format!("approval {status}"));
            child.event_updated_at = Some(Utc::now());
        }
        self.rebuild();
    }

    pub(crate) fn sync_pending_permissions<'a>(
        &mut self,
        approvals: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) {
        let pending = approvals
            .into_iter()
            .map(|(parent, child)| (parent.to_string(), child.to_string()))
            .collect::<Vec<_>>();
        let pending_ids = pending
            .iter()
            .map(|(_, child)| child.clone())
            .collect::<HashSet<_>>();
        for node in self.nodes.values_mut() {
            node.queued_permission = pending_ids.contains(&node.summary.id);
        }
        for (parent_id, child_id) in pending {
            self.ensure_node(&parent_id);
            let root = self.root_session_id.clone();
            let child = self.ensure_node(&child_id);
            if child
                .summary
                .parent_session_id
                .as_deref()
                .is_none_or(str::is_empty)
            {
                child.summary.parent_session_id = Some(parent_id);
            }
            if child.summary.root_session_id.is_empty() {
                child.summary.root_session_id = root;
            }
            child.queued_permission = true;
        }
        self.rebuild();
    }

    pub(crate) fn move_selection(&mut self, delta: isize) {
        if self.visible.is_empty() {
            self.selected = 0;
            return;
        }
        self.selected = (self.selected as isize + delta)
            .clamp(0, self.visible.len().saturating_sub(1) as isize)
            as usize;
    }

    pub(crate) fn select_first(&mut self) {
        self.selected = 0;
    }

    pub(crate) fn select_last(&mut self) {
        self.selected = self.visible.len().saturating_sub(1);
    }

    pub(crate) fn expand_or_descend(&mut self) {
        let Some(row) = self.visible.get(self.selected).cloned() else {
            return;
        };
        if row.has_children && !row.expanded {
            self.explicitly_collapsed.remove(&row.session_id);
            self.expanded.insert(row.session_id);
            self.rebuild();
            return;
        }
        if row.has_children {
            self.move_selection(1);
        }
    }

    pub(crate) fn collapse_or_ascend(&mut self) {
        let Some(row) = self.visible.get(self.selected).cloned() else {
            return;
        };
        if row.has_children && row.expanded {
            self.expanded.remove(&row.session_id);
            self.explicitly_collapsed.insert(row.session_id);
            self.rebuild();
            return;
        }
        if let Some(parent_id) = self.display_parent(&row.session_id) {
            if let Some(index) = self
                .visible
                .iter()
                .position(|candidate| candidate.session_id == parent_id)
            {
                self.selected = index;
            }
        }
    }

    pub(crate) fn breadcrumb(&self, session_id: &str) -> Vec<String> {
        self.ancestry_ids(session_id)
            .into_iter()
            .filter_map(|id| self.nodes.get(&id))
            .map(|node| {
                let title = node.summary.title.trim();
                if title.is_empty() {
                    short_session_id(&node.summary.id)
                } else {
                    title.to_string()
                }
            })
            .collect()
    }

    pub(crate) fn graph_node_count(&self) -> usize {
        self.graph_ids().len()
    }

    pub(crate) fn contains_session(&self, session_id: &str) -> bool {
        self.graph_ids().contains(session_id)
    }

    fn rebuild(&mut self) {
        let selected_id = self.selected_id().map(str::to_string);
        self.root_session_id = self.resolve_root_id();
        let root_session_id = self.root_session_id.clone();
        self.nodes
            .entry(root_session_id.clone())
            .or_insert_with(|| {
                SubagentTreeNode::new(SessionTreeSummary::placeholder(root_session_id))
            });

        if !self.explicitly_collapsed.contains(&self.root_session_id) {
            self.expanded.insert(self.root_session_id.clone());
        }
        for ancestor in self.ancestry_ids(&self.active_session_id) {
            if !self.explicitly_collapsed.contains(&ancestor) {
                self.expanded.insert(ancestor);
            }
        }

        let graph = self.graph_ids();
        let mut children: HashMap<String, Vec<String>> = HashMap::new();
        for id in &graph {
            if id == &self.root_session_id {
                continue;
            }
            if let Some(parent) = self.display_parent_in_graph(id, &graph) {
                children.entry(parent).or_default().push(id.clone());
            }
        }
        for siblings in children.values_mut() {
            siblings.sort_by(|left, right| self.compare_nodes(left, right));
        }

        self.visible.clear();
        let mut visited = HashSet::new();
        self.push_visible(&self.root_session_id.clone(), 0, &children, &mut visited);

        let desired = selected_id
            .filter(|id| self.visible.iter().any(|row| &row.session_id == id))
            .unwrap_or_else(|| self.active_session_id.clone());
        self.selected = self
            .visible
            .iter()
            .position(|row| row.session_id == desired)
            .unwrap_or(0);
    }

    fn push_visible(
        &mut self,
        session_id: &str,
        depth: usize,
        children: &HashMap<String, Vec<String>>,
        visited: &mut HashSet<String>,
    ) {
        if !visited.insert(session_id.to_string()) {
            return;
        }
        let descendants = children.get(session_id);
        let has_children = descendants.is_some_and(|children| !children.is_empty());
        let expanded = has_children && self.expanded.contains(session_id);
        self.visible.push(SubagentTreeRow {
            session_id: session_id.to_string(),
            depth,
            has_children,
            expanded,
        });
        if expanded {
            for child in descendants.into_iter().flatten() {
                self.push_visible(child, depth.saturating_add(1), children, visited);
            }
        }
    }

    fn compare_nodes(&self, left: &str, right: &str) -> std::cmp::Ordering {
        let left_node = self.nodes.get(left);
        let right_node = self.nodes.get(right);
        left_node
            .map(|node| node.summary.spawn_depth)
            .cmp(&right_node.map(|node| node.summary.spawn_depth))
            .then_with(|| {
                left_node
                    .map(SubagentTreeNode::title)
                    .unwrap_or_default()
                    .to_ascii_lowercase()
                    .cmp(
                        &right_node
                            .map(SubagentTreeNode::title)
                            .unwrap_or_default()
                            .to_ascii_lowercase(),
                    )
            })
            .then_with(|| left.cmp(right))
    }

    fn resolve_root_id(&self) -> String {
        if let Some(active) = self.nodes.get(&self.active_session_id) {
            if !active.summary.root_session_id.trim().is_empty() {
                return active.summary.root_session_id.clone();
            }
        }
        let mut current = self.active_session_id.clone();
        let mut seen = HashSet::new();
        while seen.insert(current.clone()) {
            let Some(parent) = self
                .nodes
                .get(&current)
                .and_then(|node| node.summary.parent_session_id.as_deref())
                .filter(|parent| !parent.trim().is_empty() && *parent != current)
            else {
                break;
            };
            current = parent.to_string();
        }
        current
    }

    fn graph_ids(&self) -> HashSet<String> {
        let mut graph = HashSet::from([self.root_session_id.clone()]);
        for (id, node) in &self.nodes {
            if node.summary.root_session_id == self.root_session_id {
                graph.insert(id.clone());
            }
        }
        let mut changed = true;
        while changed {
            changed = false;
            for (id, node) in &self.nodes {
                if graph.contains(id) {
                    continue;
                }
                if node
                    .summary
                    .parent_session_id
                    .as_ref()
                    .is_some_and(|parent| graph.contains(parent))
                {
                    graph.insert(id.clone());
                    changed = true;
                }
            }
        }
        for id in self.ancestry_ids(&self.active_session_id) {
            graph.insert(id);
        }
        graph.insert(self.active_session_id.clone());
        graph
    }

    fn display_parent(&self, session_id: &str) -> Option<String> {
        self.display_parent_in_graph(session_id, &self.graph_ids())
    }

    fn display_parent_in_graph(&self, session_id: &str, graph: &HashSet<String>) -> Option<String> {
        if session_id == self.root_session_id {
            return None;
        }
        let node = self.nodes.get(session_id)?;
        if let Some(parent) = node
            .summary
            .parent_session_id
            .as_deref()
            .filter(|parent| *parent != session_id && graph.contains(*parent))
        {
            return Some(parent.to_string());
        }
        graph
            .contains(&self.root_session_id)
            .then(|| self.root_session_id.clone())
    }

    fn ancestry_ids(&self, session_id: &str) -> Vec<String> {
        let graph = self.graph_ids_without_ancestry();
        let mut path = Vec::new();
        let mut current = session_id.to_string();
        let mut seen = HashSet::new();
        while seen.insert(current.clone()) {
            path.push(current.clone());
            if current == self.root_session_id {
                break;
            }
            let Some(parent) = self.display_parent_in_graph(&current, &graph) else {
                break;
            };
            current = parent;
        }
        path.reverse();
        path
    }

    /// Graph seed used while computing ancestry. Keeping it separate avoids a
    /// graph_ids -> ancestry_ids recursion for malformed legacy metadata.
    fn graph_ids_without_ancestry(&self) -> HashSet<String> {
        let mut graph = HashSet::from([self.root_session_id.clone()]);
        for (id, node) in &self.nodes {
            if node.summary.root_session_id == self.root_session_id || id == &self.active_session_id
            {
                graph.insert(id.clone());
            }
        }
        for id in self.nodes.keys() {
            graph.insert(id.clone());
        }
        graph
    }
}

pub(crate) fn short_session_id(id: &str) -> String {
    const MAX: usize = 8;
    let mut chars = id.chars();
    let prefix = chars.by_ref().take(MAX).collect::<String>();
    if chars.next().is_some() {
        format!("{prefix}…")
    } else {
        prefix
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(id: &str, parent: Option<&str>, root: &str, depth: u32) -> SessionTreeSummary {
        let mut summary = SessionTreeSummary::placeholder(id);
        summary.kind = if parent.is_some() {
            SessionTreeKind::Child
        } else {
            SessionTreeKind::Root
        };
        summary.title = id.to_string();
        summary.parent_session_id = parent.map(str::to_string);
        summary.root_session_id = root.to_string();
        summary.spawn_depth = depth;
        summary
    }

    fn populated_tree() -> SubagentTreeState {
        let mut tree = SubagentTreeState::new(7, "child-b".to_string());
        tree.install_root(summary("child-b", Some("root"), "root", 1));
        tree.install_page(
            vec![
                summary("root", None, "root", 0),
                summary("child-a", Some("root"), "root", 1),
                summary("child-b", Some("root"), "root", 1),
                summary("grandchild", Some("child-b"), "root", 2),
                summary("unrelated", None, "unrelated", 0),
            ],
            5,
            5,
            0,
            None,
        );
        tree
    }

    #[test]
    fn concurrent_siblings_and_nested_descendants_build_one_unambiguous_tree() {
        let tree = populated_tree();
        assert_eq!(tree.root_session_id, "root");
        assert_eq!(tree.graph_node_count(), 4);
        let ids = tree
            .visible
            .iter()
            .map(|row| (row.session_id.as_str(), row.depth))
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec![
                ("root", 0),
                ("child-a", 1),
                ("child-b", 1),
                ("grandchild", 2)
            ]
        );
        assert_eq!(
            tree.breadcrumb("grandchild"),
            ["root", "child-b", "grandchild"]
        );
        assert!(
            tree.nodes.contains_key("unrelated"),
            "the bounded scan retains source rows"
        );
        assert!(tree.visible.iter().all(|row| row.session_id != "unrelated"));
    }

    #[test]
    fn terminal_child_ignores_stale_start_heartbeat_and_wrong_progress_identity() {
        let mut tree = populated_tree();
        tree.apply_completed("child-b", "completed", None);
        tree.apply_heartbeat("child-b");
        tree.apply_started("root", "child-b", None, false);
        tree.apply_forwarded_event(
            "child-b",
            &AgentEvent::RunnerProgress {
                session_id: "child-a".to_string(),
                round_count: 99,
            },
        );
        let child = &tree.nodes["child-b"];
        assert_eq!(child.status(), SubagentTreeStatus::Completed);
        assert_eq!(child.round_count, None);

        tree.apply_started("root", "child-b", Some("resident reused".into()), true);
        assert_eq!(tree.nodes["child-b"].status(), SubagentTreeStatus::Running);
        assert_eq!(tree.nodes["child-b"].title(), "resident reused");
    }

    #[test]
    fn reconnect_replay_and_late_page_cannot_regress_terminal_child() {
        let mut tree = populated_tree();
        let completion = AgentEvent::Complete {
            usage: crate::api::types::TokenUsage {
                prompt_tokens: 0,
                completion_tokens: 0,
                total_tokens: 0,
            },
        };

        tree.apply_forwarded_event("child-b", &completion);
        tree.apply_forwarded_event("child-b", &completion);
        tree.apply_heartbeat("child-b");

        let mut stale_page = summary("child-b", Some("root"), "root", 1);
        stale_page.is_running = true;
        stale_page.last_run_status = None;
        tree.install_page(vec![stale_page], 5, 5, 0, None);

        let child = &tree.nodes["child-b"];
        assert_eq!(child.status(), SubagentTreeStatus::Completed);
        assert_eq!(child.activity.as_deref(), Some("completed"));
    }

    #[test]
    fn pending_permission_is_bound_to_the_exact_child() {
        let mut tree = populated_tree();
        tree.sync_pending_permissions([("root", "child-a")]);
        assert_eq!(
            tree.nodes["child-a"].status(),
            SubagentTreeStatus::WaitingForPermission
        );
        assert_ne!(
            tree.nodes["child-b"].status(),
            SubagentTreeStatus::WaitingForPermission
        );
    }

    #[test]
    fn exact_progress_and_authoritative_approval_resolution_clear_pending_state() {
        let mut tree = populated_tree();
        tree.mark_waiting_input("child-a");
        tree.apply_runner_progress("child-a", 4);
        assert_eq!(tree.nodes["child-a"].status(), SubagentTreeStatus::Running);

        tree.apply_child_approval_changed("root", "child-a", "pending", true);
        assert_eq!(
            tree.nodes["child-a"].status(),
            SubagentTreeStatus::WaitingForPermission
        );
        tree.apply_child_approval_changed("root", "child-a", "approved", false);
        assert_eq!(
            tree.nodes["child-a"].status(),
            SubagentTreeStatus::WaitingForPermission,
            "a compatibility terminal frame cannot clear a concrete request"
        );
        tree.apply_child_approval_changed("root", "child-a", "approved", true);
        assert_eq!(tree.nodes["child-a"].status(), SubagentTreeStatus::Running);
    }

    #[test]
    fn resident_remote_and_error_metadata_survive_page_projection() {
        let mut tree = SubagentTreeState::new(1, "root".into());
        tree.install_root(summary("root", None, "root", 0));
        let mut child = summary("resident", Some("root"), "root", 1);
        child.lifecycle = Some("resident".into());
        child.resident_name = Some("reviewer".into());
        child.subagent_type = Some("guardian".into());
        child.placement.kind = "ssh".into();
        child.placement.host = "worker.example".into();
        child.last_run_status = Some("error".into());
        child.last_run_error = Some("boom".into());
        tree.install_page(vec![child], 1, 1, 0, None);
        let node = &tree.nodes["resident"];
        assert_eq!(node.status(), SubagentTreeStatus::Error);
        assert_eq!(node.error(), Some("boom"));
        assert_eq!(node.summary.lifecycle.as_deref(), Some("resident"));
        assert_eq!(node.summary.placement.host, "worker.example");
    }

    #[test]
    fn legacy_active_session_without_relationship_metadata_degrades_to_one_root() {
        let mut tree = SubagentTreeState::new(1, "legacy".into());
        tree.install_root(SessionTreeSummary::placeholder("legacy"));
        tree.install_page(Vec::new(), 0, 100, 0, None);
        assert_eq!(tree.root_session_id, "legacy");
        assert_eq!(tree.visible.len(), 1);
        assert_eq!(tree.visible[0].session_id, "legacy");
    }
}
