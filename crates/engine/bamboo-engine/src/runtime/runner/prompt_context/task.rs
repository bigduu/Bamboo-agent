use super::system_sections::strip_existing_prompt_block;

const TASK_LIST_START_MARKER: &str = "<!-- BAMBOO_TASK_LIST_START -->";
const TASK_LIST_END_MARKER: &str = "<!-- BAMBOO_TASK_LIST_END -->";
use bamboo_domain::{LEGACY_TODO_LIST_END_MARKER, LEGACY_TODO_LIST_START_MARKER};

/// Strip a previously-injected task-list block from a (possibly persisted) system
/// prompt. The task list now rides a dedicated volatile block, but legacy
/// persisted sessions may still carry a marker-wrapped task list in the System
/// message; `normalize_base_prompt` strips it so it never re-leaks into `base`.
pub(super) fn strip_existing_task_list(prompt: &str) -> String {
    let prompt = strip_existing_prompt_block(prompt, TASK_LIST_START_MARKER, TASK_LIST_END_MARKER);
    strip_existing_prompt_block(
        &prompt,
        LEGACY_TODO_LIST_START_MARKER,
        LEGACY_TODO_LIST_END_MARKER,
    )
}
