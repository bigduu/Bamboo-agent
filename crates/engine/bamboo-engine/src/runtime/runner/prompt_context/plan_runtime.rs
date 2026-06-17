use bamboo_agent_core::Session;
use bamboo_domain::{PlanModeStatus, TaskItem, TaskItemStatus};
use bamboo_memory::plan_store::{
    PlanCursorArtifact, PlanSection, PlanSectionArtifact, PlanStateArtifact, PlanStore,
};
use std::path::Path;

use super::system_sections::strip_existing_prompt_block;

const PLAN_RUNTIME_CONTEXT_START_MARKER: &str = "<!-- BAMBOO_PLAN_RUNTIME_CONTEXT_START -->";
const PLAN_RUNTIME_CONTEXT_END_MARKER: &str = "<!-- BAMBOO_PLAN_RUNTIME_CONTEXT_END -->";
const SMALL_PLAN_FULL_INJECTION_CHAR_THRESHOLD: usize = 3_500;
const SELECTIVE_PLAN_EXCERPT_CHAR_BUDGET: usize = 2_400;
const ANCHOR_CONTEXT_LINES_BEFORE: usize = 2;
const ANCHOR_CONTEXT_LINES_AFTER: usize = 8;
const MAX_HEADING_LOOKBACK_LINES: usize = 10;
const EXCERPT_SEPARATOR: &str = "\n...\n";
const TRUNCATION_SUFFIX: &str = "\n...[truncated]";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlanInjectionMode {
    Full,
    IndexedSection,
    SelectiveExcerpt,
    FallbackExcerpt,
}

impl PlanInjectionMode {
    fn label(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::IndexedSection => "indexed_section",
            Self::SelectiveExcerpt => "selective_excerpt",
            Self::FallbackExcerpt => "fallback_excerpt",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlanInjection {
    mode: PlanInjectionMode,
    content: String,
    total_chars: usize,
    injected_chars: usize,
    anchor_terms: Vec<String>,
    matched_anchor_terms: Vec<String>,
    matched_section_ids: Vec<String>,
}

fn task_status_label(status: &TaskItemStatus) -> &'static str {
    match status {
        TaskItemStatus::Pending => "pending",
        TaskItemStatus::InProgress => "in_progress",
        TaskItemStatus::Completed => "completed",
        TaskItemStatus::Blocked => "blocked",
    }
}

fn plan_status_label(status: PlanModeStatus) -> &'static str {
    match status {
        PlanModeStatus::Exploring => "exploring",
        PlanModeStatus::Designing => "designing",
        PlanModeStatus::Reviewing => "reviewing",
        PlanModeStatus::Finalizing => "finalizing",
        PlanModeStatus::AwaitingApproval => "awaiting_approval",
    }
}

fn active_task(session: &Session) -> Option<&TaskItem> {
    let task_list = session.task_list.as_ref()?;
    task_list
        .items
        .iter()
        .find(|item| matches!(item.status, TaskItemStatus::InProgress))
        .or_else(|| {
            task_list
                .items
                .iter()
                .find(|item| matches!(item.status, TaskItemStatus::Pending))
        })
}

fn active_task_summary(session: &Session) -> Option<String> {
    let active = active_task(session)?;

    let mut summary = format!(
        "- id: {}\n- status: {}\n- description: {}",
        active.id,
        task_status_label(&active.status),
        active.description.trim()
    );

    if let Some(active_form) = active.effective_active_form() {
        if !active_form.trim().is_empty() {
            summary.push_str(&format!("\n- active_form: {}", active_form.trim()));
        }
    }

    if !active.depends_on.is_empty() {
        summary.push_str(&format!("\n- depends_on: {}", active.depends_on.join(", ")));
    }

    Some(summary)
}

fn format_state_summary(state: &PlanStateArtifact) -> String {
    let mut lines = vec![
        format!("- updated_at: {}", state.updated_at.to_rfc3339()),
        format!(
            "- status: {}",
            state.status.as_deref().unwrap_or("(unknown)")
        ),
    ];
    if let Some(active_task_id) = state.active_task_id.as_deref() {
        lines.push(format!("- active_task_id: {active_task_id}"));
    }
    if let Some(active_step_id) = state.active_step_id.as_deref() {
        lines.push(format!("- active_step_id: {active_step_id}"));
    }
    if let Some(next_step_id) = state.next_step_id.as_deref() {
        lines.push(format!("- next_step_id: {next_step_id}"));
    }
    if let Some(active_section_id) = state.active_section_id.as_deref() {
        lines.push(format!("- active_section_id: {active_section_id}"));
    }
    if let Some(next_section_id) = state.next_section_id.as_deref() {
        lines.push(format!("- next_section_id: {next_section_id}"));
    }
    if let Some(last_completed_task_id) = state.last_completed_task_id.as_deref() {
        lines.push(format!(
            "- last_completed_task_id: {last_completed_task_id}"
        ));
    }
    if let Some(last_completed_section_id) = state.last_completed_section_id.as_deref() {
        lines.push(format!(
            "- last_completed_section_id: {last_completed_section_id}"
        ));
    }
    if let Some(round_hint) = state.round_hint {
        lines.push(format!("- round_hint: {round_hint}"));
    }
    lines.join("\n")
}

fn format_cursor_summary(cursor: &PlanCursorArtifact) -> String {
    let mut lines = vec![format!("- updated_at: {}", cursor.updated_at.to_rfc3339())];
    if let Some(cursor_type) = cursor.cursor_type.as_deref() {
        lines.push(format!("- cursor_type: {cursor_type}"));
    }
    if let Some(current_task_id) = cursor.current_task_id.as_deref() {
        lines.push(format!("- current_task_id: {current_task_id}"));
    }
    if let Some(current_task_ordinal) = cursor.current_task_ordinal {
        lines.push(format!("- current_task_ordinal: {current_task_ordinal}"));
    }
    if let Some(current_step_id) = cursor.current_step_id.as_deref() {
        lines.push(format!("- current_step_id: {current_step_id}"));
    }
    if let Some(current_section_id) = cursor.current_section_id.as_deref() {
        lines.push(format!("- current_section_id: {current_section_id}"));
    }
    if let Some(next_task_id) = cursor.next_task_id.as_deref() {
        lines.push(format!("- next_task_id: {next_task_id}"));
    }
    if let Some(next_task_ordinal) = cursor.next_task_ordinal {
        lines.push(format!("- next_task_ordinal: {next_task_ordinal}"));
    }
    if let Some(next_section_id) = cursor.next_section_id.as_deref() {
        lines.push(format!("- next_section_id: {next_section_id}"));
    }
    if let Some(last_completed_task_id) = cursor.last_completed_task_id.as_deref() {
        lines.push(format!(
            "- last_completed_task_id: {last_completed_task_id}"
        ));
    }
    if let Some(last_completed_section_id) = cursor.last_completed_section_id.as_deref() {
        lines.push(format!(
            "- last_completed_section_id: {last_completed_section_id}"
        ));
    }
    if let Some(last_completed_checkpoint) = cursor.last_completed_checkpoint.as_deref() {
        lines.push(format!(
            "- last_completed_checkpoint: {last_completed_checkpoint}"
        ));
    }
    if let Some(round_hint) = cursor.round_hint {
        lines.push(format!("- round_hint: {round_hint}"));
    }
    if let Some(round_id_hint) = cursor.round_id_hint.as_deref() {
        lines.push(format!("- round_id_hint: {round_id_hint}"));
    }
    if let Some(suspension_hook_point) = cursor.suspension_hook_point.as_deref() {
        lines.push(format!("- suspension_hook_point: {suspension_hook_point}"));
    }
    if let Some(tool_call_boundary) = cursor.tool_call_boundary.as_deref() {
        lines.push(format!("- tool_call_boundary: {tool_call_boundary}"));
    }
    if let Some(resume_note) = cursor.resume_note.as_deref() {
        lines.push(format!("- resume_note: {resume_note}"));
    }
    lines.join("\n")
}

fn push_unique_term(target: &mut Vec<String>, value: Option<&str>) {
    let Some(trimmed) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return;
    };

    if trimmed.chars().count() < 2 {
        return;
    }

    if !target
        .iter()
        .any(|existing| existing.eq_ignore_ascii_case(trimmed))
    {
        target.push(trimmed.to_string());
    }
}

fn collect_anchor_terms(
    session: &Session,
    state_artifact: Option<&PlanStateArtifact>,
    cursor_artifact: Option<&PlanCursorArtifact>,
) -> Vec<String> {
    let mut terms = Vec::new();

    if let Some(state_artifact) = state_artifact {
        push_unique_term(&mut terms, state_artifact.active_task_id.as_deref());
        push_unique_term(&mut terms, state_artifact.active_step_id.as_deref());
        push_unique_term(&mut terms, state_artifact.next_step_id.as_deref());
        push_unique_term(&mut terms, state_artifact.active_section_id.as_deref());
        push_unique_term(&mut terms, state_artifact.next_section_id.as_deref());
        push_unique_term(&mut terms, state_artifact.last_completed_task_id.as_deref());
        push_unique_term(
            &mut terms,
            state_artifact.last_completed_section_id.as_deref(),
        );
    }

    if let Some(cursor_artifact) = cursor_artifact {
        push_unique_term(&mut terms, cursor_artifact.current_task_id.as_deref());
        push_unique_term(&mut terms, cursor_artifact.current_step_id.as_deref());
        push_unique_term(
            &mut terms,
            cursor_artifact.last_completed_checkpoint.as_deref(),
        );
        push_unique_term(&mut terms, cursor_artifact.current_section_id.as_deref());
        push_unique_term(&mut terms, cursor_artifact.next_task_id.as_deref());
        push_unique_term(&mut terms, cursor_artifact.next_section_id.as_deref());
        push_unique_term(
            &mut terms,
            cursor_artifact.last_completed_task_id.as_deref(),
        );
        push_unique_term(
            &mut terms,
            cursor_artifact.last_completed_section_id.as_deref(),
        );
        push_unique_term(&mut terms, cursor_artifact.round_id_hint.as_deref());
        push_unique_term(&mut terms, cursor_artifact.suspension_hook_point.as_deref());
        push_unique_term(&mut terms, cursor_artifact.tool_call_boundary.as_deref());
    }

    if let Some(active) = active_task(session) {
        push_unique_term(&mut terms, Some(active.id.as_str()));
        push_unique_term(&mut terms, active.effective_active_form());
    }

    terms
}

fn collect_section_ids(
    state_artifact: Option<&PlanStateArtifact>,
    cursor_artifact: Option<&PlanCursorArtifact>,
) -> Vec<String> {
    let mut section_ids = Vec::new();

    if let Some(state_artifact) = state_artifact {
        push_unique_term(
            &mut section_ids,
            state_artifact.active_section_id.as_deref(),
        );
        push_unique_term(&mut section_ids, state_artifact.next_section_id.as_deref());
        push_unique_term(
            &mut section_ids,
            state_artifact.last_completed_section_id.as_deref(),
        );
    }

    if let Some(cursor_artifact) = cursor_artifact {
        push_unique_term(
            &mut section_ids,
            cursor_artifact.current_section_id.as_deref(),
        );
        push_unique_term(&mut section_ids, cursor_artifact.next_section_id.as_deref());
        push_unique_term(
            &mut section_ids,
            cursor_artifact.last_completed_section_id.as_deref(),
        );
    }

    section_ids
}

fn truncate_to_char_budget(text: &str, budget: usize) -> String {
    let char_count = text.chars().count();
    if char_count <= budget {
        return text.to_string();
    }

    if budget == 0 {
        return String::new();
    }

    let suffix_chars = TRUNCATION_SUFFIX.chars().count();
    if budget <= suffix_chars + 1 {
        return text.chars().take(budget).collect();
    }

    let keep_chars = budget - suffix_chars;
    let mut truncated = text.chars().take(keep_chars).collect::<String>();
    while truncated.ends_with(char::is_whitespace) {
        truncated.pop();
    }
    truncated.push_str(TRUNCATION_SUFFIX);
    truncated
}

fn is_markdown_heading(line: &str) -> bool {
    let trimmed = line.trim_start();
    let hashes = trimmed.chars().take_while(|ch| *ch == '#').count();
    hashes > 0
        && trimmed
            .chars()
            .nth(hashes)
            .map(|ch| ch.is_whitespace())
            .unwrap_or(false)
}

fn nearest_heading_index(lines: &[&str], index: usize) -> Option<usize> {
    let lookback_start = index.saturating_sub(MAX_HEADING_LOOKBACK_LINES);
    (lookback_start..=index)
        .rev()
        .find(|candidate| is_markdown_heading(lines[*candidate]))
}

fn anchor_hits(line: &str, anchor_terms: &[String]) -> Vec<String> {
    let normalized_line = line.to_lowercase();
    let mut hits = Vec::new();

    for term in anchor_terms {
        if normalized_line.contains(&term.to_lowercase()) {
            push_unique_term(&mut hits, Some(term));
        }
    }

    hits
}

fn merge_ranges(ranges: &mut Vec<(usize, usize)>) {
    if ranges.is_empty() {
        return;
    }

    ranges.sort_unstable_by_key(|(start, _)| *start);
    let mut merged = Vec::with_capacity(ranges.len());
    let mut current = ranges[0];

    for &(start, end) in ranges.iter().skip(1) {
        if start <= current.1.saturating_add(1) {
            current.1 = current.1.max(end);
        } else {
            merged.push(current);
            current = (start, end);
        }
    }

    merged.push(current);
    *ranges = merged;
}

fn build_excerpt_from_ranges(lines: &[&str], ranges: &[(usize, usize)], budget: usize) -> String {
    let mut excerpt = String::new();

    for &(start, end) in ranges {
        if start >= lines.len() || start > end {
            continue;
        }

        let block = lines[start..=end].join("\n");
        let block = block.trim();
        if block.is_empty() {
            continue;
        }

        let remaining_budget = budget.saturating_sub(excerpt.chars().count());
        if remaining_budget == 0 {
            break;
        }

        let separator = if excerpt.is_empty() {
            ""
        } else {
            EXCERPT_SEPARATOR
        };
        let separator_chars = separator.chars().count();
        if remaining_budget <= separator_chars {
            break;
        }

        let available_for_block = remaining_budget - separator_chars;
        let block_text = truncate_to_char_budget(block, available_for_block);
        if block_text.is_empty() {
            break;
        }

        if !excerpt.is_empty() {
            excerpt.push_str(EXCERPT_SEPARATOR);
        }
        excerpt.push_str(block_text.trim_end());

        if excerpt.chars().count() >= budget {
            break;
        }
    }

    excerpt
}

fn fallback_excerpt(lines: &[&str]) -> String {
    if lines.is_empty() {
        return String::new();
    }

    build_excerpt_from_ranges(
        lines,
        &[(0, lines.len() - 1)],
        SELECTIVE_PLAN_EXCERPT_CHAR_BUDGET,
    )
}

fn section_matches_id(section: &PlanSection, section_id: &str) -> bool {
    section.id.eq_ignore_ascii_case(section_id)
        || section
            .anchor_terms
            .iter()
            .any(|term| term.eq_ignore_ascii_case(section_id))
}

fn indexed_section_excerpt(
    lines: &[&str],
    sections: Option<&PlanSectionArtifact>,
    requested_section_ids: &[String],
) -> Option<(String, Vec<String>)> {
    let sections = sections?;
    if requested_section_ids.is_empty() {
        return None;
    }

    let mut ranges = Vec::new();
    let mut matched_section_ids = Vec::new();

    for requested in requested_section_ids {
        if let Some(section) = sections
            .sections
            .iter()
            .find(|section| section_matches_id(section, requested))
        {
            let start = section.line_start.min(lines.len().saturating_sub(1));
            let end = section.line_end.min(lines.len().saturating_sub(1));
            if start <= end {
                ranges.push((start, end));
                push_unique_term(&mut matched_section_ids, Some(section.id.as_str()));
            }
        }
    }

    merge_ranges(&mut ranges);
    if ranges.is_empty() {
        return None;
    }

    let excerpt = build_excerpt_from_ranges(lines, &ranges, SELECTIVE_PLAN_EXCERPT_CHAR_BUDGET);
    if excerpt.trim().is_empty() {
        None
    } else {
        Some((excerpt, matched_section_ids))
    }
}

fn select_plan_content_for_prompt(
    plan: &str,
    session: &Session,
    state_artifact: Option<&PlanStateArtifact>,
    cursor_artifact: Option<&PlanCursorArtifact>,
    sections: Option<&PlanSectionArtifact>,
) -> PlanInjection {
    let trimmed = plan.trim();
    let total_chars = trimmed.chars().count();
    let anchor_terms = collect_anchor_terms(session, state_artifact, cursor_artifact);
    let requested_section_ids = collect_section_ids(state_artifact, cursor_artifact);

    if total_chars <= SMALL_PLAN_FULL_INJECTION_CHAR_THRESHOLD {
        return PlanInjection {
            mode: PlanInjectionMode::Full,
            content: trimmed.to_string(),
            total_chars,
            injected_chars: total_chars,
            anchor_terms,
            matched_anchor_terms: Vec::new(),
            matched_section_ids: Vec::new(),
        };
    }

    let lines: Vec<&str> = trimmed.lines().collect();

    if let Some((content, matched_section_ids)) =
        indexed_section_excerpt(&lines, sections, &requested_section_ids)
    {
        let injected_chars = content.chars().count();
        return PlanInjection {
            mode: PlanInjectionMode::IndexedSection,
            content,
            total_chars,
            injected_chars,
            anchor_terms,
            matched_anchor_terms: Vec::new(),
            matched_section_ids,
        };
    }

    let mut matched_anchor_terms = Vec::new();
    let mut ranges = Vec::new();
    let mut index = 0;

    while index < lines.len() {
        let hits = anchor_hits(lines[index], &anchor_terms);
        if hits.is_empty() {
            index += 1;
            continue;
        }

        for hit in hits {
            push_unique_term(&mut matched_anchor_terms, Some(hit.as_str()));
        }

        let heading_start = nearest_heading_index(&lines, index);
        let context_start = index.saturating_sub(ANCHOR_CONTEXT_LINES_BEFORE);
        let start = heading_start
            .map(|heading_index| heading_index.min(context_start))
            .unwrap_or(context_start);
        let end = (index + ANCHOR_CONTEXT_LINES_AFTER).min(lines.len().saturating_sub(1));
        ranges.push((start, end));
        index = end.saturating_add(1);
    }

    merge_ranges(&mut ranges);

    let (mode, content) = if ranges.is_empty() {
        (PlanInjectionMode::FallbackExcerpt, fallback_excerpt(&lines))
    } else {
        (
            PlanInjectionMode::SelectiveExcerpt,
            build_excerpt_from_ranges(&lines, &ranges, SELECTIVE_PLAN_EXCERPT_CHAR_BUDGET),
        )
    };

    let content = if content.trim().is_empty() {
        fallback_excerpt(&lines)
    } else {
        content
    };

    let injected_chars = content.chars().count();

    PlanInjection {
        mode,
        content,
        total_chars,
        injected_chars,
        anchor_terms,
        matched_anchor_terms,
        matched_section_ids: Vec::new(),
    }
}

pub(super) fn build_plan_runtime_context(
    session: &Session,
    app_data_dir: Option<&Path>,
) -> Option<String> {
    let plan_mode = session
        .agent_runtime_state
        .as_ref()
        .and_then(|state| state.plan_mode.as_ref())?;

    let plan_file_path = plan_mode.plan_file_path.clone().or_else(|| {
        let app_data_dir = app_data_dir?;
        let store = PlanStore::new(app_data_dir).ok()?;
        store
            .plan_exists(&session.id)
            .then(|| store.plan_file_path(&session.id).display().to_string())
    });

    let plan_content = app_data_dir.and_then(|dir| {
        let store = PlanStore::new(dir).ok()?;
        store.read_plan(&session.id)
    });
    let state_artifact = app_data_dir.and_then(|dir| {
        let store = PlanStore::new(dir).ok()?;
        store.read_state(&session.id).ok().flatten()
    });
    let cursor_artifact = app_data_dir.and_then(|dir| {
        let store = PlanStore::new(dir).ok()?;
        store.read_cursor(&session.id).ok().flatten()
    });
    let sections_artifact = app_data_dir.and_then(|dir| {
        let store = PlanStore::new(dir).ok()?;
        store.read_sections(&session.id).ok().flatten()
    });

    let active_task = active_task_summary(session)
        .unwrap_or_else(|| "- none identified from current task list".to_string());

    let mut context = format!(
        "=== DURABLE PLAN EXECUTION CONTEXT ===\n\
This context exists so plan execution can resume correctly after pause, compression, reconnect, or resume.\n\n\
Plan mode status: {}\n\
Plan file path: {}\n\n\
Current execution anchor from task list:\n{}\n",
        plan_status_label(plan_mode.status),
        plan_file_path.as_deref().unwrap_or("(not yet persisted)"),
        active_task
    );

    if let Some(state_artifact) = state_artifact.as_ref() {
        context.push_str("\nMachine plan state:\n");
        context.push_str(&format_state_summary(state_artifact));
        context.push('\n');
    }

    if let Some(cursor_artifact) = cursor_artifact.as_ref() {
        context.push_str("\nExecution cursor:\n");
        context.push_str(&format_cursor_summary(cursor_artifact));
        context.push('\n');
    }

    if let Some(sections_artifact) = sections_artifact.as_ref() {
        context.push_str("\nPlan section index:\n");
        context.push_str(&format!(
            "- indexed_sections: {}\n",
            sections_artifact.sections.len()
        ));
    }

    if let Some(plan) = plan_content
        .as_deref()
        .map(str::trim)
        .filter(|p| !p.is_empty())
    {
        let selection = select_plan_content_for_prompt(
            plan,
            session,
            state_artifact.as_ref(),
            cursor_artifact.as_ref(),
            sections_artifact.as_ref(),
        );

        context.push_str("\nPlan injection metadata:\n");
        context.push_str(&format!(
            "- mode: {}\n- total_chars: {}\n- injected_chars: {}\n",
            selection.mode.label(),
            selection.total_chars,
            selection.injected_chars
        ));
        if !selection.anchor_terms.is_empty() {
            context.push_str(&format!(
                "- anchor_terms: {}\n",
                selection.anchor_terms.join(", ")
            ));
        }
        if !selection.matched_section_ids.is_empty() {
            context.push_str(&format!(
                "- matched_section_ids: {}\n",
                selection.matched_section_ids.join(", ")
            ));
        }
        if !selection.matched_anchor_terms.is_empty() {
            context.push_str(&format!(
                "- matched_anchor_terms: {}\n",
                selection.matched_anchor_terms.join(", ")
            ));
        } else if matches!(selection.mode, PlanInjectionMode::FallbackExcerpt) {
            context.push_str("- matched_anchor_terms: none (using safe prefix fallback)\n");
        }

        context.push_str(match selection.mode {
            PlanInjectionMode::Full => "\nPersisted plan content (full):\n",
            PlanInjectionMode::IndexedSection => {
                "\nPersisted plan excerpt (indexed section resolution):\n"
            }
            PlanInjectionMode::SelectiveExcerpt => {
                "\nPersisted plan excerpt (selected around active anchors):\n"
            }
            PlanInjectionMode::FallbackExcerpt => {
                "\nPersisted plan excerpt (fallback because active anchors were not found):\n"
            }
        });
        context.push_str(&selection.content);
        context.push('\n');
    } else {
        context.push_str(
            "\nPersisted plan content:\n- unavailable; if you are executing a plan, recover intent from the current task list and recent session history.\n",
        );
    }

    context.push_str(
        "\nResume rule: continue from the current in-progress task when available; otherwise pick the next pending task that best matches the persisted plan. Prefer indexed section matches when section ids are available, and use stronger cursor hints (round, ordinal, completion boundary) to preserve resume precision.\n",
    );

    Some(context)
}

pub(super) fn strip_existing_plan_runtime_context(prompt: &str) -> String {
    strip_existing_prompt_block(
        prompt,
        PLAN_RUNTIME_CONTEXT_START_MARKER,
        PLAN_RUNTIME_CONTEXT_END_MARKER,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::session::runtime_state::{AgentRuntimeState, PlanModeState};
    use bamboo_domain::{TaskItem, TaskList};
    use bamboo_memory::plan_store::{PlanCursorArtifact, PlanStateArtifact};
    use chrono::Utc;

    fn session_with_active_task(session_id: &str, task_id: &str) -> Session {
        let mut session = Session::new(session_id, "model");
        session.task_list = Some(TaskList {
            session_id: session_id.to_string(),
            title: "Plan Tasks".to_string(),
            items: vec![TaskItem {
                id: task_id.to_string(),
                description: format!("Execute {task_id}"),
                status: TaskItemStatus::InProgress,
                ..TaskItem::default()
            }],
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
        session
    }

    #[test]
    fn select_plan_content_for_prompt_returns_full_content_for_small_plan() {
        let session = session_with_active_task("session-small", "task-small");
        let mut state = PlanStateArtifact::new("session-small");
        state.active_task_id = Some("task-small".to_string());

        let plan = "# Plan\n\n## task-small\n- do the small thing\n- verify";
        let selection = select_plan_content_for_prompt(plan, &session, Some(&state), None, None);

        assert_eq!(selection.mode, PlanInjectionMode::Full);
        assert_eq!(selection.content, plan);
        assert_eq!(selection.total_chars, plan.chars().count());
        assert_eq!(selection.injected_chars, plan.chars().count());
    }

    #[test]
    fn select_plan_content_for_prompt_prefers_indexed_section_resolution() {
        let session_id = "session-indexed";
        let session = session_with_active_task(session_id, "task-bravo");
        let mut state = PlanStateArtifact::new(session_id);
        state.active_task_id = Some("task-bravo".to_string());
        state.active_section_id = Some("task-bravo".to_string());
        let mut cursor = PlanCursorArtifact::new(session_id);
        cursor.current_section_id = Some("task-bravo".to_string());

        let plan = format!(
            "# Plan\n\n## task-alpha\n- task_id: task-alpha\n{}\n## task-bravo\n- task_id: task-bravo\n- BRAVO_INDEX_MARKER: execute bravo migration\n{}\n## task-charlie\n- task_id: task-charlie\n- CHARLIE_INDEX_TAIL: archive tail\n{}",
            "alpha-padding-line\n".repeat(180),
            "bravo-padding-line\n".repeat(180),
            "charlie-padding-line\n".repeat(180),
        );
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = PlanStore::new(temp_dir.path()).expect("plan store");
        store.write_plan(session_id, &plan).expect("write plan");
        let sections = store
            .read_sections(session_id)
            .expect("read sections")
            .expect("sections should exist");

        let selection = select_plan_content_for_prompt(
            &plan,
            &session,
            Some(&state),
            Some(&cursor),
            Some(&sections),
        );

        assert_eq!(selection.mode, PlanInjectionMode::IndexedSection);
        assert!(selection.content.contains("BRAVO_INDEX_MARKER"));
        assert!(!selection.content.contains("CHARLIE_INDEX_TAIL"));
        assert!(selection
            .matched_section_ids
            .iter()
            .any(|section_id| section_id == "task-bravo"));
    }

    #[test]
    fn select_plan_content_for_prompt_uses_selective_excerpt_for_large_plan() {
        let session = session_with_active_task("session-large", "task-bravo");
        let mut state = PlanStateArtifact::new("session-large");
        state.active_task_id = Some("task-bravo".to_string());
        let mut cursor = PlanCursorArtifact::new("session-large");
        cursor.current_step_id = Some("step-bravo-2".to_string());

        let plan = format!(
            "# Plan\n\n## Overview\n{}\n## task-alpha\n- task_id: task-alpha\n- detail: alpha section\n{}\n## task-bravo\n- task_id: task-bravo\n- current_step_id: step-bravo-2\n- BRAVO_UNIQUE_MARKER: execute bravo migration\n{}\n## task-charlie\n- task_id: task-charlie\n- CHARLIE_UNIQUE_TAIL_MARKER: archive tail section\n{}",
            "overview-padding-line\n".repeat(180),
            "alpha-padding-line\n".repeat(80),
            "bravo-padding-line\n".repeat(60),
            "charlie-padding-line\n".repeat(180),
        );

        let selection =
            select_plan_content_for_prompt(&plan, &session, Some(&state), Some(&cursor), None);

        assert_eq!(selection.mode, PlanInjectionMode::SelectiveExcerpt);
        assert!(selection.content.contains("task-bravo"));
        assert!(selection.content.contains("BRAVO_UNIQUE_MARKER"));
        assert!(!selection.content.contains("CHARLIE_UNIQUE_TAIL_MARKER"));
        assert!(selection.injected_chars < selection.total_chars);
        assert!(selection
            .matched_anchor_terms
            .iter()
            .any(|term| term == "task-bravo"));
    }

    #[test]
    fn select_plan_content_for_prompt_falls_back_when_anchors_are_missing() {
        let session = session_with_active_task("session-fallback", "missing-task");
        let mut state = PlanStateArtifact::new("session-fallback");
        state.active_task_id = Some("missing-task".to_string());

        let plan = format!(
            "# Plan\n\nHEAD_MARKER\n{}\nTAIL_MARKER\n{}",
            "prefix-padding-line\n".repeat(220),
            "tail-padding-line\n".repeat(220),
        );

        let selection = select_plan_content_for_prompt(&plan, &session, Some(&state), None, None);

        assert_eq!(selection.mode, PlanInjectionMode::FallbackExcerpt);
        assert!(selection.content.contains("HEAD_MARKER"));
        assert!(!selection.content.contains("TAIL_MARKER"));
        assert!(selection.injected_chars < selection.total_chars);
        assert!(selection.matched_anchor_terms.is_empty());
    }

    #[test]
    fn build_plan_runtime_context_includes_persisted_plan_and_active_task() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let store = PlanStore::new(temp_dir.path()).expect("plan store");
        let session_id = "session-plan-runtime";
        store
            .write_plan(
                session_id,
                "# Runtime Plan\n\n## t1\n- task_id: t1\n- Do thing",
            )
            .expect("write plan");
        let mut state = PlanStateArtifact::new(session_id);
        state.status = Some("awaiting_approval".to_string());
        state.active_task_id = Some("t1".to_string());
        state.active_section_id = Some("t1".to_string());
        state.last_completed_task_id = Some("t0".to_string());
        state.last_completed_section_id = Some("t0".to_string());
        state.round_hint = Some(5);
        store.write_state(session_id, &state).expect("write state");
        let mut cursor = PlanCursorArtifact::new(session_id);
        cursor.cursor_type = Some("task_item".to_string());
        cursor.current_task_id = Some("t1".to_string());
        cursor.current_task_ordinal = Some(2);
        cursor.current_section_id = Some("t1".to_string());
        cursor.next_task_id = Some("t2".to_string());
        cursor.next_task_ordinal = Some(3);
        cursor.next_section_id = Some("t2".to_string());
        cursor.last_completed_task_id = Some("t0".to_string());
        cursor.last_completed_section_id = Some("t0".to_string());
        cursor.last_completed_checkpoint = Some("before_user_approval".to_string());
        cursor.round_hint = Some(5);
        cursor.round_id_hint = Some("round-5".to_string());
        cursor.suspension_hook_point = Some("AfterToolExecution".to_string());
        cursor.tool_call_boundary = Some("ExitPlanMode".to_string());
        cursor.resume_note = Some("Continue with task t1".to_string());
        store
            .write_cursor(session_id, &cursor)
            .expect("write cursor");

        let mut session = Session::new(session_id, "model");
        session.agent_runtime_state = Some(AgentRuntimeState::new("run-1"));
        session.agent_runtime_state.as_mut().unwrap().plan_mode = Some(PlanModeState {
            entered_at: Utc::now(),
            pre_permission_mode: "default".to_string(),
            plan_file_path: Some(store.plan_file_path(session_id).display().to_string()),
            status: PlanModeStatus::Designing,
        });
        session.task_list = Some(TaskList {
            session_id: session_id.to_string(),
            title: "Plan Tasks".to_string(),
            items: vec![TaskItem {
                id: "t1".to_string(),
                description: "Implement durable plan mode".to_string(),
                status: TaskItemStatus::InProgress,
                ..TaskItem::default()
            }],
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });

        let context = build_plan_runtime_context(&session, Some(temp_dir.path()))
            .expect("active plan mode yields runtime context");
        assert!(context.contains("DURABLE PLAN EXECUTION CONTEXT"));
        assert!(context.contains("Machine plan state:"));
        assert!(context.contains("last_completed_task_id: t0"));
        assert!(context.contains("round_hint: 5"));
        assert!(context.contains("Execution cursor:"));
        assert!(context.contains("current_task_ordinal: 2"));
        assert!(context.contains("next_task_id: t2"));
        assert!(context.contains("round_id_hint: round-5"));
        assert!(context.contains("tool_call_boundary: ExitPlanMode"));
        assert!(context.contains("Plan section index:"));
        assert!(context.contains("- mode: full"));
    }

    #[test]
    fn build_plan_runtime_context_is_none_when_plan_mode_inactive() {
        let session = Session::new("session-1", "model");
        assert!(build_plan_runtime_context(&session, None).is_none());
    }
}
