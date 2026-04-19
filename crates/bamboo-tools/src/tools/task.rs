use bamboo_domain::{TaskPhase, TaskPriority};
use bamboo_agent_core::{Tool, ToolError, ToolResult};
use bamboo_domain::{TaskItem, TaskItemStatus, TaskList};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::collections::HashSet;

#[derive(Debug, Deserialize)]
struct TaskArgsRaw {
    tasks: Vec<TaskWriteItem>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct TaskWriteItem {
    #[serde(default)]
    id: Option<String>,
    #[serde(default, rename = "taskId")]
    task_id: Option<String>,
    content: String,
    status: String,
    #[serde(rename = "activeForm")]
    active_form: Option<String>,
    #[serde(default, alias = "dependsOn")]
    depends_on: Vec<String>,
    #[serde(default, alias = "parentId")]
    parent_id: Option<String>,
    #[serde(default)]
    phase: Option<TaskPhase>,
    #[serde(default)]
    priority: Option<TaskPriority>,
    #[serde(default, rename = "completionCriteria", alias = "completion_criteria")]
    completion_criteria: Vec<String>,
    #[serde(default, rename = "criteriaMet", alias = "criteria_met")]
    criteria_met: Vec<String>,
}

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

fn normalize_optional_text(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn normalize_string_list(values: Vec<String>) -> Vec<String> {
    let mut deduped = HashSet::new();
    let mut normalized = Vec::new();
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            continue;
        }
        if deduped.insert(trimmed.to_string()) {
            normalized.push(trimmed.to_string());
        }
    }
    normalized
}

fn parse_requested_task_id(
    id: Option<String>,
    task_id: Option<String>,
) -> Result<Option<String>, ToolError> {
    let id = normalize_optional_text(id);
    let task_id = normalize_optional_text(task_id);
    match (id, task_id) {
        (Some(id), Some(task_id)) if id != task_id => Err(ToolError::InvalidArguments(format!(
            "Conflicting task identifiers in tasks[]: id='{}' does not match taskId='{}'",
            id, task_id
        ))),
        (Some(id), Some(_)) => Ok(Some(id)),
        (Some(id), None) => Ok(Some(id)),
        (None, Some(task_id)) => Ok(Some(task_id)),
        (None, None) => Ok(None),
    }
}

fn normalize_criterion(value: &str) -> Option<String> {
    let normalized = value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_lowercase();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn parse_criterion_ref(value: &str) -> Option<usize> {
    let trimmed = value.trim().to_ascii_lowercase();
    let as_c_ref = trimmed
        .strip_prefix("criterion_")
        .or_else(|| trimmed.strip_prefix("criterion-"))
        .or_else(|| trimmed.strip_prefix('c'));
    if let Some(raw_index) = as_c_ref {
        return raw_index.parse::<usize>().ok().filter(|index| *index > 0);
    }
    None
}

fn missing_completion_criteria(required: &[String], criteria_met: &[String]) -> Vec<String> {
    let mut required_lookup = HashMap::new();
    for (index, criterion) in required.iter().enumerate() {
        if let Some(normalized) = normalize_criterion(criterion) {
            required_lookup.insert(normalized, index + 1);
        }
    }

    let mut met_refs = HashSet::new();
    for criterion in criteria_met {
        if let Some(index) = parse_criterion_ref(criterion) {
            met_refs.insert(index);
            continue;
        }
        if let Some(normalized) = normalize_criterion(criterion) {
            if let Some(index) = required_lookup.get(&normalized).copied() {
                met_refs.insert(index);
            }
        }
    }

    required
        .iter()
        .enumerate()
        .filter_map(|(index, criterion)| {
            if met_refs.contains(&(index + 1)) {
                None
            } else {
                Some(criterion.trim().to_string())
            }
        })
        .collect()
}

fn parse_numeric_task_id(value: &str) -> Option<u64> {
    value
        .strip_prefix("task_")
        .and_then(|suffix| suffix.parse::<u64>().ok())
}

fn next_generated_task_id(next_counter: &mut u64, assigned_ids: &HashSet<String>) -> String {
    loop {
        *next_counter = next_counter.saturating_add(1);
        let candidate = format!("task_{}", *next_counter);
        if !assigned_ids.contains(&candidate) {
            return candidate;
        }
    }
}

fn find_reusable_task_id(
    description: &str,
    existing_items: &[TaskItem],
    used_existing_ids: &HashSet<String>,
    assigned_ids: &HashSet<String>,
) -> Option<String> {
    existing_items
        .iter()
        .find(|item| {
            item.description == description
                && !used_existing_ids.contains(&item.id)
                && !assigned_ids.contains(&item.id)
        })
        .map(|item| item.id.clone())
}

fn find_next_existing_id_by_position(
    position: usize,
    existing_items: &[TaskItem],
    used_existing_ids: &HashSet<String>,
    assigned_ids: &HashSet<String>,
) -> Option<String> {
    existing_items
        .get(position)
        .map(|item| item.id.clone())
        .filter(|id| !used_existing_ids.contains(id) && !assigned_ids.contains(id))
}

pub struct TaskTool;

impl TaskTool {
    pub fn new() -> Self {
        Self
    }

    pub fn task_list_from_args(
        args: &serde_json::Value,
        session_id: &str,
    ) -> Result<TaskList, ToolError> {
        Self::task_list_from_args_with_existing(args, session_id, None)
    }

    pub fn task_list_from_args_with_existing(
        args: &serde_json::Value,
        session_id: &str,
        existing: Option<&TaskList>,
    ) -> Result<TaskList, ToolError> {
        let parsed: TaskArgsRaw = serde_json::from_value(args.clone())
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid Task args: {e}")))?;
        let incoming_count = parsed.tasks.len();

        let items_source = if parsed.tasks.is_empty() {
            return Err(ToolError::InvalidArguments(
                "Task requires a non-empty `tasks` array".to_string(),
            ));
        } else {
            parsed.tasks
        };

        let existing_items = existing
            .map(|task_list| task_list.items.clone())
            .unwrap_or_default();
        let mut used_existing_ids = HashSet::new();
        let mut assigned_ids = HashSet::new();
        let preserve_positional_ids = existing
            .map(|task_list| task_list.items.len() == incoming_count)
            .unwrap_or(false);
        let mut generated_new_ids = false;
        let mut next_generated_counter = existing_items
            .iter()
            .filter_map(|item| parse_numeric_task_id(&item.id))
            .max()
            .unwrap_or(0);

        let mut items = Vec::with_capacity(items_source.len());
        for task in items_source {
            let description = normalize_required_text(Some(task.content), "tasks[].content")?;
            let status = match task.status.as_str() {
                "pending" => TaskItemStatus::Pending,
                "in_progress" => TaskItemStatus::InProgress,
                "completed" => TaskItemStatus::Completed,
                "blocked" => TaskItemStatus::Blocked,
                _ => {
                    return Err(ToolError::InvalidArguments(format!(
                        "Invalid task status '{}' (expected pending/in_progress/completed/blocked)",
                        task.status
                    )))
                }
            };

            let requested_id = parse_requested_task_id(task.id, task.task_id)?;
            let task_id = if let Some(requested_id) = requested_id {
                if assigned_ids.contains(&requested_id) {
                    return Err(ToolError::InvalidArguments(format!(
                        "Duplicate task id '{}' in tasks[] payload",
                        requested_id
                    )));
                }
                requested_id
            } else if let Some(reused_id) = find_reusable_task_id(
                &description,
                &existing_items,
                &used_existing_ids,
                &assigned_ids,
            ) {
                reused_id
            } else if preserve_positional_ids {
                find_next_existing_id_by_position(
                    items.len(),
                    &existing_items,
                    &used_existing_ids,
                    &assigned_ids,
                )
                .unwrap_or_else(|| {
                    generated_new_ids = true;
                    next_generated_task_id(&mut next_generated_counter, &assigned_ids)
                })
            } else {
                generated_new_ids = true;
                next_generated_task_id(&mut next_generated_counter, &assigned_ids)
            };

            assigned_ids.insert(task_id.clone());
            let active_form = normalize_optional_text(task.active_form);
            let existing_item = existing_items
                .iter()
                .find(|item| item.id == task_id)
                .cloned();
            if existing_item.is_some() {
                used_existing_ids.insert(task_id.clone());
            }
            let notes = active_form
                .clone()
                .or_else(|| {
                    existing_item
                        .as_ref()
                        .map(|item| item.notes.trim().to_string())
                        .filter(|notes| !notes.is_empty())
                })
                .unwrap_or_else(|| description.clone());
            let mut depends_on = normalize_string_list(task.depends_on);
            depends_on.retain(|dependency_id| dependency_id != &task_id);
            let mut completion_criteria = normalize_string_list(task.completion_criteria);
            completion_criteria.retain(|criterion| !criterion.is_empty());
            let criteria_met = normalize_string_list(task.criteria_met);
            let mut parent_id = normalize_optional_text(task.parent_id);
            if parent_id.as_deref() == Some(task_id.as_str()) {
                parent_id = None;
            }

            let mut effective_status = status;
            let mut gate_note: Option<String> = None;
            if matches!(effective_status, TaskItemStatus::Completed)
                && !completion_criteria.is_empty()
            {
                let missing = missing_completion_criteria(&completion_criteria, &criteria_met);
                if !missing.is_empty() {
                    effective_status = TaskItemStatus::InProgress;
                    gate_note = Some(format!(
                        "Completion criteria not fully met; keeping task in_progress. Missing: {}",
                        missing.join(" | ")
                    ));
                }
            }

            let mut item = existing_item.unwrap_or_default();
            item.id = task_id;
            item.description = description.clone();
            if item.status != effective_status {
                item.transition_to(effective_status, gate_note.as_deref(), None);
            }
            item.depends_on = depends_on;
            item.notes = if let Some(gate_note) = gate_note {
                if notes.trim().is_empty() {
                    gate_note
                } else {
                    format!("{notes}\n{gate_note}")
                }
            } else {
                notes
            };
            item.active_form = active_form;
            item.parent_id = parent_id;
            item.phase = task.phase.unwrap_or_default();
            item.priority = task.priority.unwrap_or_default();
            item.completion_criteria = completion_criteria;

            items.push(item);
        }

        if !existing_items.is_empty()
            && generated_new_ids
            && used_existing_ids.len() < existing_items.len()
        {
            return Err(ToolError::InvalidArguments(
                "Ambiguous task ID assignment during full-list rewrite. Include stable `id`/`taskId` for retained tasks when adding/removing tasks in the same update."
                    .to_string(),
            ));
        }

        Ok(TaskList {
            session_id: session_id.to_string(),
            title: "Task List".to_string(),
            items,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        })
    }
}

impl Default for TaskTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for TaskTool {
    fn name(&self) -> &str {
        "Task"
    }

    fn description(&self) -> &str {
        "Create or update the shared task list for the current root session tree. Child sessions write to the same task list as their parent/root session."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "tasks": {
                    "type": "array",
                    "description": "Canonical task items for the shared task list.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "id": { "type": "string" },
                            "taskId": { "type": "string" },
                            "content": { "type": "string", "minLength": 1 },
                            "status": {
                                "type": "string",
                                "enum": ["pending", "in_progress", "completed", "blocked"]
                            },
                            "activeForm": { "type": "string" },
                            "dependsOn": {
                                "type": "array",
                                "items": { "type": "string" }
                            },
                            "parentId": { "type": "string" },
                            "phase": {
                                "type": "string",
                                "enum": ["planning", "execution", "verification", "handoff"]
                            },
                            "priority": {
                                "type": "string",
                                "enum": ["low", "medium", "high", "critical"]
                            },
                            "completionCriteria": {
                                "type": "array",
                                "items": { "type": "string" }
                            },
                            "criteriaMet": {
                                "type": "array",
                                "items": { "type": "string" }
                            }
                        },
                        "required": ["content", "status"],
                        "additionalProperties": false
                    }
                }
            },
            "required": ["tasks"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let parsed: TaskArgsRaw = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid Task args: {e}")))?;
        let count = parsed.tasks.len();
        if count == 0 {
            return Err(ToolError::InvalidArguments(
                "Task requires a non-empty `tasks` array".to_string(),
            ));
        }

        Ok(ToolResult {
            success: true,
            result: format!("Task list updated with {count} items"),
            display_preference: Some("Default".to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn task_execute_accepts_tasks_payload() {
        let tool = TaskTool::new();
        let result = tool
            .execute(json!({
                "tasks": [
                    {
                        "content": "Summarize parser entrypoints",
                        "status": "in_progress",
                        "activeForm": "Summarizing parser entrypoints"
                    }
                ]
            }))
            .await
            .expect("Task should validate payload");

        assert!(result.success);
        assert!(result.result.contains("1 items"));
    }

    #[tokio::test]
    async fn task_execute_rejects_empty_payload() {
        let tool = TaskTool::new();
        let err = tool
            .execute(json!({}))
            .await
            .expect_err("Task should reject empty payload");

        assert!(matches!(err, ToolError::InvalidArguments(msg) if msg.contains("tasks")));
    }

    #[tokio::test]
    async fn task_execute_rejects_legacy_todos_field() {
        let tool = TaskTool::new();
        let err = tool
            .execute(json!({
                "todos": [
                    {
                        "content": "Legacy path",
                        "status": "pending"
                    }
                ]
            }))
            .await
            .expect_err("Task should reject legacy todos field");

        assert!(
            matches!(err, ToolError::InvalidArguments(msg) if msg.contains("Invalid Task args"))
        );
    }

    #[test]
    fn task_list_from_args_supports_blocked_status() {
        let list = TaskTool::task_list_from_args(
            &json!({
                "tasks": [
                    {
                        "content": "Waiting on API token",
                        "status": "blocked",
                        "activeForm": "Blocked by missing API token"
                    }
                ]
            }),
            "session_1",
        )
        .expect("blocked status should be accepted");

        assert_eq!(list.session_id, "session_1");
        assert_eq!(list.items.len(), 1);
        assert_eq!(list.items[0].status, TaskItemStatus::Blocked);
    }

    #[test]
    fn task_list_from_args_parses_structured_fields() {
        let list = TaskTool::task_list_from_args(
            &json!({
                "tasks": [
                    {
                        "content": "Implement migration path",
                        "status": "in_progress",
                        "activeForm": "Implementing migration path",
                        "dependsOn": ["task_99", "task_99", " "],
                        "parentId": "epic_1",
                        "phase": "verification",
                        "priority": "high",
                        "completionCriteria": [
                            "All unit tests pass",
                            "No clippy warnings",
                            "All unit tests pass"
                        ]
                    }
                ]
            }),
            "session_2",
        )
        .expect("structured fields should parse");

        let item = &list.items[0];
        assert_eq!(item.id, "task_1");
        assert_eq!(
            item.active_form.as_deref(),
            Some("Implementing migration path")
        );
        assert_eq!(item.depends_on, vec!["task_99".to_string()]);
        assert_eq!(item.parent_id.as_deref(), Some("epic_1"));
        assert_eq!(item.phase, TaskPhase::Verification);
        assert_eq!(item.priority, TaskPriority::High);
        assert_eq!(
            item.completion_criteria,
            vec![
                "All unit tests pass".to_string(),
                "No clippy warnings".to_string()
            ]
        );
    }

    #[test]
    fn task_list_from_args_with_existing_preserves_ids_on_reorder() {
        let existing = TaskList {
            session_id: "session_3".to_string(),
            title: "Task List".to_string(),
            items: vec![
                TaskItem {
                    id: "task_10".to_string(),
                    description: "First task".to_string(),
                    status: TaskItemStatus::Pending,
                    depends_on: Vec::new(),
                    notes: "original".to_string(),
                    ..TaskItem::default()
                },
                TaskItem {
                    id: "task_11".to_string(),
                    description: "Second task".to_string(),
                    status: TaskItemStatus::Pending,
                    depends_on: Vec::new(),
                    notes: "original".to_string(),
                    ..TaskItem::default()
                },
            ],
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        let list = TaskTool::task_list_from_args_with_existing(
            &json!({
                "tasks": [
                    { "content": "Second task", "status": "in_progress" },
                    { "content": "First task", "status": "pending" }
                ]
            }),
            "session_3",
            Some(&existing),
        )
        .expect("ids should be preserved");

        assert_eq!(list.items[0].id, "task_11");
        assert_eq!(list.items[1].id, "task_10");
    }

    #[test]
    fn task_list_from_args_with_existing_accepts_explicit_ids() {
        let list = TaskTool::task_list_from_args(
            &json!({
                "tasks": [
                    { "id": "task_42", "content": "Stable id task", "status": "pending" },
                    { "taskId": "custom-2", "content": "Custom id alias", "status": "in_progress" }
                ]
            }),
            "session_4",
        )
        .expect("explicit ids should parse");

        assert_eq!(list.items[0].id, "task_42");
        assert_eq!(list.items[1].id, "custom-2");
    }

    #[test]
    fn task_list_from_args_accepts_id_and_task_id_when_same() {
        let list = TaskTool::task_list_from_args(
            &json!({
                "tasks": [
                    {
                        "id": "task_100",
                        "taskId": "task_100",
                        "content": "Same identifier aliases",
                        "status": "pending"
                    }
                ]
            }),
            "session_same_id_alias",
        )
        .expect("matching id/taskId should be accepted");

        assert_eq!(list.items.len(), 1);
        assert_eq!(list.items[0].id, "task_100");
    }

    #[test]
    fn task_list_from_args_rejects_conflicting_id_and_task_id() {
        let err = TaskTool::task_list_from_args(
            &json!({
                "tasks": [
                    {
                        "id": "task_100",
                        "taskId": "task_101",
                        "content": "Conflicting identifier aliases",
                        "status": "pending"
                    }
                ]
            }),
            "session_conflicting_id_alias",
        )
        .expect_err("conflicting id/taskId should be rejected");

        assert!(matches!(
            err,
            ToolError::InvalidArguments(message)
                if message.contains("Conflicting task identifiers")
        ));
    }

    #[test]
    fn task_list_from_args_rejects_duplicate_explicit_ids() {
        let err = TaskTool::task_list_from_args(
            &json!({
                "tasks": [
                    { "id": "task_dup", "content": "One", "status": "pending" },
                    { "id": "task_dup", "content": "Two", "status": "pending" }
                ]
            }),
            "session_5",
        )
        .expect_err("duplicate explicit ids must fail");

        assert!(matches!(
            err,
            ToolError::InvalidArguments(message) if message.contains("Duplicate task id")
        ));
    }

    #[test]
    fn task_list_from_args_with_existing_reuses_positional_ids_when_descriptions_change() {
        let existing = TaskList {
            session_id: "session_6".to_string(),
            title: "Task List".to_string(),
            items: vec![
                TaskItem {
                    id: "task_20".to_string(),
                    description: "Old first".to_string(),
                    status: TaskItemStatus::Pending,
                    notes: "old".to_string(),
                    ..TaskItem::default()
                },
                TaskItem {
                    id: "task_21".to_string(),
                    description: "Old second".to_string(),
                    status: TaskItemStatus::Pending,
                    notes: "old".to_string(),
                    ..TaskItem::default()
                },
            ],
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        let list = TaskTool::task_list_from_args_with_existing(
            &json!({
                "tasks": [
                    { "content": "Renamed first", "status": "in_progress" },
                    { "content": "Renamed second", "status": "pending" }
                ]
            }),
            "session_6",
            Some(&existing),
        )
        .expect("positional ids should be reused when item count is unchanged");

        assert_eq!(list.items[0].id, "task_20");
        assert_eq!(list.items[1].id, "task_21");
    }

    #[test]
    fn task_list_from_args_completion_gate_keeps_task_in_progress_when_criteria_unmet() {
        let list = TaskTool::task_list_from_args(
            &json!({
                "tasks": [
                    {
                        "content": "Release package",
                        "status": "completed",
                        "completionCriteria": ["tests pass", "docs updated"],
                        "criteriaMet": ["c1"]
                    }
                ]
            }),
            "session_7",
        )
        .expect("task list should parse");

        assert_eq!(list.items[0].status, TaskItemStatus::InProgress);
        assert!(list.items[0]
            .notes
            .contains("Completion criteria not fully met"));
    }

    #[test]
    fn task_list_from_args_completion_gate_allows_completed_when_criteria_met() {
        let list = TaskTool::task_list_from_args(
            &json!({
                "tasks": [
                    {
                        "content": "Release package",
                        "status": "completed",
                        "completionCriteria": ["tests pass", "docs updated"],
                        "criteriaMet": ["c1", "criterion_2"]
                    }
                ]
            }),
            "session_8",
        )
        .expect("task list should parse");

        assert_eq!(list.items[0].status, TaskItemStatus::Completed);
    }

    #[test]
    fn task_list_from_args_with_existing_rejects_ambiguous_rewrite_when_lengths_change() {
        let existing = TaskList {
            session_id: "session_9".to_string(),
            title: "Task List".to_string(),
            items: vec![
                TaskItem {
                    id: "task_30".to_string(),
                    description: "Keep me".to_string(),
                    status: TaskItemStatus::Pending,
                    ..TaskItem::default()
                },
                TaskItem {
                    id: "task_31".to_string(),
                    description: "Remove me".to_string(),
                    status: TaskItemStatus::Pending,
                    ..TaskItem::default()
                },
            ],
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        let err = TaskTool::task_list_from_args_with_existing(
            &json!({
                "tasks": [
                    { "content": "Brand new replacement", "status": "pending" }
                ]
            }),
            "session_9",
            Some(&existing),
        )
        .expect_err("ambiguous rewrite should be rejected");

        assert!(matches!(
            err,
            ToolError::InvalidArguments(message) if message.contains("Ambiguous task ID assignment")
        ));
    }

    #[test]
    fn task_list_from_args_with_existing_allows_additions_after_existing_are_matched() {
        let existing = TaskList {
            session_id: "session_10".to_string(),
            title: "Task List".to_string(),
            items: vec![
                TaskItem {
                    id: "task_40".to_string(),
                    description: "First".to_string(),
                    status: TaskItemStatus::Pending,
                    ..TaskItem::default()
                },
                TaskItem {
                    id: "task_41".to_string(),
                    description: "Second".to_string(),
                    status: TaskItemStatus::Pending,
                    ..TaskItem::default()
                },
            ],
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };

        let list = TaskTool::task_list_from_args_with_existing(
            &json!({
                "tasks": [
                    { "content": "First", "status": "pending" },
                    { "content": "Second", "status": "pending" },
                    { "content": "Third", "status": "pending" }
                ]
            }),
            "session_10",
            Some(&existing),
        )
        .expect("adding new items after matching existing ids should be allowed");

        assert_eq!(list.items[0].id, "task_40");
        assert_eq!(list.items[1].id, "task_41");
        assert_eq!(list.items[2].id, "task_42");
    }
}
