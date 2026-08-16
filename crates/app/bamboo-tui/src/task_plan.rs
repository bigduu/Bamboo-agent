use std::collections::{HashMap, HashSet};

use crate::api::types::{TaskItem, TaskItemStatus, TaskList, TaskListResponse, TaskProgress};

/// Session-owned task state. This lives inside `ChatState`, so a background
/// stream can update only its own cached conversation instead of whichever
/// tab happens to be visible.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TaskProgressState {
    pub(crate) requested_session_id: Option<String>,
    pub(crate) owner_session_id: Option<String>,
    pub(crate) task_list: Option<TaskList>,
    pub(crate) progress: TaskProgress,
    pub(crate) version: u64,
    item_versions: HashMap<String, u64>,
    pub(crate) loading: bool,
    pub(crate) error: Option<String>,
    pub(crate) completed: bool,
    pub(crate) completion_summary: Option<String>,
    pub(crate) evaluation: Option<String>,
    evaluation_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApplyResult {
    Applied,
    Stale,
    WrongSession,
}

impl TaskProgressState {
    pub(crate) fn begin_refresh(&mut self, requested_session_id: &str) {
        if self.requested_session_id.as_deref() != Some(requested_session_id) {
            *self = Self {
                requested_session_id: Some(requested_session_id.to_string()),
                loading: true,
                ..Self::default()
            };
        } else {
            self.loading = true;
            self.error = None;
        }
    }

    pub(crate) fn install_snapshot(
        &mut self,
        requested_session_id: &str,
        snapshot: TaskListResponse,
    ) -> ApplyResult {
        if self.requested_session_id.as_deref() != Some(requested_session_id) {
            return ApplyResult::WrongSession;
        }
        if self.task_list.is_some() && snapshot.version < self.version {
            self.loading = false;
            return ApplyResult::Stale;
        }
        let owner = if snapshot.session_id.is_empty() {
            requested_session_id.to_string()
        } else {
            snapshot.session_id
        };
        let version = snapshot.version;
        let task_list = TaskList {
            session_id: owner.clone(),
            title: snapshot.title.unwrap_or_else(|| "Agent Tasks".to_string()),
            items: snapshot.items,
            created_at: snapshot.created_at.unwrap_or_default(),
            updated_at: snapshot.updated_at.unwrap_or_default(),
        };
        self.owner_session_id = Some(owner);
        self.progress = snapshot.progress;
        self.version = version;
        self.item_versions = task_list
            .items
            .iter()
            .map(|item| (item.id.clone(), version))
            .collect();
        self.completed = self.progress.total > 0 && self.progress.completed == self.progress.total;
        self.task_list = Some(task_list);
        self.loading = false;
        self.error = None;
        ApplyResult::Applied
    }

    pub(crate) fn fail_refresh(&mut self, requested_session_id: &str, error: String) {
        if self.requested_session_id.as_deref() == Some(requested_session_id) {
            self.loading = false;
            self.error = Some(error);
        }
    }

    fn accepts_source(&self, source_session_id: &str, shared_source: bool) -> bool {
        self.requested_session_id.as_deref() == Some(source_session_id)
            || self.owner_session_id.as_deref() == Some(source_session_id)
            || (shared_source && self.requested_session_id.is_some())
    }

    pub(crate) fn apply_list(
        &mut self,
        source_session_id: &str,
        task_list: TaskList,
        version: Option<u64>,
        _shared_source: bool,
    ) -> ApplyResult {
        let belongs_to_view = self.accepts_source(source_session_id, false)
            || self.requested_session_id.as_deref() == Some(task_list.session_id.as_str())
            || self.owner_session_id.as_deref() == Some(task_list.session_id.as_str());
        if !belongs_to_view {
            return ApplyResult::WrongSession;
        }
        if let Some(incoming) = version {
            if incoming <= self.version && self.task_list.is_some() {
                return ApplyResult::Stale;
            }
        } else if self.task_list.as_ref().is_some_and(|current| {
            !task_list.updated_at.is_empty()
                && !current.updated_at.is_empty()
                && task_list.updated_at <= current.updated_at
        }) {
            return ApplyResult::Stale;
        }

        let incoming_version = version.unwrap_or(self.version.saturating_add(1));
        self.owner_session_id = Some(task_list.session_id.clone());
        self.version = incoming_version;
        self.item_versions = task_list
            .items
            .iter()
            .map(|item| (item.id.clone(), incoming_version))
            .collect();
        self.progress = calculate_progress(&task_list.items);
        self.completed = self.progress.total > 0 && self.progress.completed == self.progress.total;
        self.task_list = Some(task_list);
        self.loading = false;
        self.error = None;
        ApplyResult::Applied
    }

    pub(crate) fn apply_item(
        &mut self,
        source_session_id: &str,
        item_id: String,
        status: TaskItemStatus,
        version: u64,
        item: Option<TaskItem>,
        shared_source: bool,
    ) -> ApplyResult {
        if !self.accepts_source(source_session_id, shared_source) {
            return ApplyResult::WrongSession;
        }
        if version < self.version
            || self
                .item_versions
                .get(&item_id)
                .is_some_and(|current| version <= *current)
        {
            return ApplyResult::Stale;
        }
        if item.as_ref().is_some_and(|rich| rich.id != item_id) {
            return ApplyResult::WrongSession;
        }

        let owner = self
            .owner_session_id
            .clone()
            .or_else(|| self.requested_session_id.clone())
            .unwrap_or_else(|| source_session_id.to_string());
        let list = self.task_list.get_or_insert_with(|| TaskList {
            session_id: owner,
            title: "Agent Tasks".to_string(),
            ..TaskList::default()
        });
        let mut incoming = item.unwrap_or_else(|| TaskItem {
            id: item_id.clone(),
            description: item_id.clone(),
            ..TaskItem::default()
        });
        incoming.status = status;
        if let Some(existing) = list
            .items
            .iter_mut()
            .find(|existing| existing.id == item_id)
        {
            *existing = incoming;
        } else {
            list.items.push(incoming);
        }
        self.version = self.version.max(version);
        self.item_versions.insert(item_id, version);
        self.progress = calculate_progress(&list.items);
        self.completed = self.progress.total > 0 && self.progress.completed == self.progress.total;
        self.loading = false;
        self.error = None;
        ApplyResult::Applied
    }

    pub(crate) fn apply_completed(
        &mut self,
        source_session_id: &str,
        version: Option<u64>,
        total_rounds: u32,
        total_tool_calls: usize,
        shared_source: bool,
    ) -> ApplyResult {
        if !self.accepts_source(source_session_id, shared_source) {
            return ApplyResult::WrongSession;
        }
        if version.is_some_and(|incoming| incoming < self.version) {
            return ApplyResult::Stale;
        }
        if let Some(version) = version {
            self.version = self.version.max(version);
        }
        self.completed = true;
        self.completion_summary = Some(format!(
            "completed in {total_rounds} rounds · {total_tool_calls} tool calls"
        ));
        ApplyResult::Applied
    }

    pub(crate) fn evaluation_started(
        &mut self,
        source_session_id: &str,
        items_count: usize,
        generation: Option<u64>,
        shared_source: bool,
    ) -> ApplyResult {
        if !self.accepts_source(source_session_id, shared_source) {
            return ApplyResult::WrongSession;
        }
        if generation.is_some_and(|incoming| incoming < self.evaluation_generation) {
            return ApplyResult::Stale;
        }
        self.evaluation_generation = generation.unwrap_or(self.evaluation_generation);
        self.evaluation = Some(format!("evaluating {items_count} tasks"));
        ApplyResult::Applied
    }

    pub(crate) fn evaluation_finished(
        &mut self,
        source_session_id: &str,
        message: String,
        generation: Option<u64>,
        shared_source: bool,
    ) -> ApplyResult {
        if !self.accepts_source(source_session_id, shared_source) {
            return ApplyResult::WrongSession;
        }
        if generation.is_some_and(|incoming| incoming < self.evaluation_generation) {
            return ApplyResult::Stale;
        }
        self.evaluation_generation = generation.unwrap_or(self.evaluation_generation);
        self.evaluation = Some(message);
        ApplyResult::Applied
    }

    pub(crate) fn ordered_items(&self) -> Vec<(usize, &TaskItem)> {
        let Some(list) = &self.task_list else {
            return Vec::new();
        };
        ordered_items(&list.items)
    }
}

fn calculate_progress(items: &[TaskItem]) -> TaskProgress {
    let completed = items
        .iter()
        .filter(|item| item.status == TaskItemStatus::Completed)
        .count();
    let total = items.len();
    TaskProgress {
        completed,
        total,
        percentage: completed
            .saturating_mul(100)
            .checked_div(total)
            .unwrap_or(0) as u8,
    }
}

fn ordered_items(items: &[TaskItem]) -> Vec<(usize, &TaskItem)> {
    let ids = items
        .iter()
        .map(|item| item.id.as_str())
        .collect::<HashSet<_>>();
    let mut children: HashMap<&str, Vec<usize>> = HashMap::new();
    let mut roots = Vec::new();
    for (index, item) in items.iter().enumerate() {
        match item
            .parent_id
            .as_deref()
            .filter(|parent| ids.contains(parent))
        {
            Some(parent) if parent != item.id => children.entry(parent).or_default().push(index),
            _ => roots.push(index),
        }
    }
    let mut result = Vec::with_capacity(items.len());
    let mut visited = HashSet::new();
    fn walk<'a>(
        index: usize,
        depth: usize,
        items: &'a [TaskItem],
        children: &HashMap<&str, Vec<usize>>,
        visited: &mut HashSet<usize>,
        result: &mut Vec<(usize, &'a TaskItem)>,
    ) {
        if !visited.insert(index) {
            return;
        }
        let item = &items[index];
        result.push((depth, item));
        if let Some(child_indices) = children.get(item.id.as_str()) {
            for child in child_indices {
                walk(
                    *child,
                    depth.saturating_add(1),
                    items,
                    children,
                    visited,
                    result,
                );
            }
        }
    }
    for root in roots {
        walk(root, 0, items, &children, &mut visited, &mut result);
    }
    // Cycles and malformed parent links remain visible instead of vanishing.
    for index in 0..items.len() {
        walk(index, 0, items, &children, &mut visited, &mut result);
    }
    result
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum TaskPlanPane {
    #[default]
    Tasks,
    Plan,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TaskPlanOverlayState {
    pub(crate) pane: TaskPlanPane,
    pub(crate) selected: usize,
    pub(crate) detail_scroll: usize,
}

impl TaskPlanOverlayState {
    pub(crate) fn toggle_pane(&mut self) {
        self.pane = match self.pane {
            TaskPlanPane::Tasks => TaskPlanPane::Plan,
            TaskPlanPane::Plan => TaskPlanPane::Tasks,
        };
        self.detail_scroll = 0;
    }

    pub(crate) fn move_selection(&mut self, delta: isize, item_count: usize) {
        if item_count == 0 {
            self.selected = 0;
            return;
        }
        self.selected = self
            .selected
            .saturating_add_signed(delta)
            .min(item_count.saturating_sub(1));
        self.detail_scroll = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_client_core::{TaskBlocker, TaskBlockerKind};

    fn item(id: &str, status: TaskItemStatus) -> TaskItem {
        TaskItem {
            id: id.to_string(),
            description: format!("Task {id}"),
            status,
            ..TaskItem::default()
        }
    }

    fn state() -> TaskProgressState {
        let mut state = TaskProgressState::default();
        state.begin_refresh("session-a");
        state.install_snapshot(
            "session-a",
            TaskListResponse {
                session_id: "root-a".to_string(),
                title: Some("Tasks".to_string()),
                items: vec![item("one", TaskItemStatus::Pending)],
                progress: TaskProgress {
                    completed: 0,
                    total: 1,
                    percentage: 0,
                },
                version: 4,
                ..TaskListResponse::default()
            },
        );
        state
    }

    #[test]
    fn stale_duplicate_and_cross_session_deltas_do_not_regress() {
        let mut state = state();
        assert_eq!(
            state.apply_item(
                "session-a",
                "one".to_string(),
                TaskItemStatus::Completed,
                5,
                None,
                false,
            ),
            ApplyResult::Applied
        );
        assert_eq!(
            state.apply_item(
                "session-a",
                "one".to_string(),
                TaskItemStatus::Pending,
                4,
                None,
                false,
            ),
            ApplyResult::Stale
        );
        assert_eq!(
            state.apply_item(
                "session-b",
                "one".to_string(),
                TaskItemStatus::Blocked,
                6,
                None,
                false,
            ),
            ApplyResult::WrongSession
        );
        assert_eq!(
            state.task_list.as_ref().unwrap().items[0].status,
            TaskItemStatus::Completed
        );
    }

    #[test]
    fn rich_progress_preserves_blocker_reason_and_wait_target() {
        let mut state = state();
        let mut blocked = item("one", TaskItemStatus::Blocked);
        blocked.blockers.push(TaskBlocker {
            kind: TaskBlockerKind::Dependency,
            summary: "API schema is not ready".to_string(),
            waiting_on: Some("task-two".to_string()),
        });
        assert_eq!(
            state.apply_item(
                "session-a",
                "one".to_string(),
                TaskItemStatus::Blocked,
                5,
                Some(blocked),
                false,
            ),
            ApplyResult::Applied
        );
        let blocker = &state.task_list.as_ref().unwrap().items[0].blockers[0];
        assert_eq!(blocker.summary, "API schema is not ready");
        assert_eq!(blocker.waiting_on.as_deref(), Some("task-two"));
    }

    #[test]
    fn nested_and_cyclic_items_are_all_visible() {
        let mut parent = item("parent", TaskItemStatus::InProgress);
        let mut child = item("child", TaskItemStatus::Pending);
        child.parent_id = Some(parent.id.clone());
        let mut cycle_a = item("cycle-a", TaskItemStatus::Blocked);
        let mut cycle_b = item("cycle-b", TaskItemStatus::Pending);
        cycle_a.parent_id = Some("cycle-b".to_string());
        cycle_b.parent_id = Some("cycle-a".to_string());
        parent.depends_on = vec!["setup".to_string()];
        let items = [parent, child, cycle_a, cycle_b];
        let ordered = ordered_items(&items);
        assert_eq!(ordered.len(), 4);
        assert_eq!(ordered[0].1.id, "parent");
        assert_eq!(ordered[1].0, 1);
    }

    #[test]
    fn authoritative_reconnect_snapshot_converges_at_same_version() {
        let mut state = state();
        state.begin_refresh("session-a");
        let result = state.install_snapshot(
            "session-a",
            TaskListResponse {
                session_id: "root-a".to_string(),
                items: vec![item("one", TaskItemStatus::Blocked)],
                version: 4,
                ..TaskListResponse::default()
            },
        );
        assert_eq!(result, ApplyResult::Applied);
        assert_eq!(
            state.task_list.as_ref().unwrap().items[0].status,
            TaskItemStatus::Blocked
        );
    }

    #[test]
    fn late_older_http_snapshot_cannot_regress_a_live_delta() {
        let mut state = state();
        assert_eq!(
            state.apply_item(
                "session-a",
                "one".to_string(),
                TaskItemStatus::Completed,
                6,
                None,
                false,
            ),
            ApplyResult::Applied
        );
        state.begin_refresh("session-a");
        assert_eq!(
            state.install_snapshot(
                "session-a",
                TaskListResponse {
                    session_id: "root-a".to_string(),
                    items: vec![item("one", TaskItemStatus::Pending)],
                    version: 5,
                    ..TaskListResponse::default()
                },
            ),
            ApplyResult::Stale
        );
        assert!(!state.loading);
        assert_eq!(
            state.task_list.as_ref().unwrap().items[0].status,
            TaskItemStatus::Completed
        );
    }

    #[test]
    fn switching_sessions_clears_stale_rows_and_keeps_refresh_error_visible() {
        let mut state = state();
        state.begin_refresh("session-b");
        assert!(state.task_list.is_none());
        assert_eq!(state.requested_session_id.as_deref(), Some("session-b"));
        state.fail_refresh("session-b", "server offline".to_string());
        assert!(!state.loading);
        assert_eq!(state.error.as_deref(), Some("server offline"));

        // A late response for the previous session cannot repopulate the view.
        assert_eq!(
            state.install_snapshot("session-a", TaskListResponse::default()),
            ApplyResult::WrongSession
        );
        assert!(state.task_list.is_none());
    }
}
