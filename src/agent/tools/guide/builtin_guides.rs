use std::sync::Arc;

use serde_json::json;

use super::{ToolCategory, ToolExample, ToolGuide, ToolGuideSpec};

pub const BUILTIN_GUIDE_NAMES: [&str; 17] = [
    "read_file",
    "write_file",
    "read_file_range",
    "file_exists",
    "get_file_info",
    "list_directory",
    "set_workspace",
    "search_in_file",
    "search_in_project",
    "apply_patch",
    "execute_command",
    "get_current_dir",
    "git_status",
    "git_diff",
    "create_todo_list",
    "update_todo_item",
    "ask_user",
];

pub fn builtin_tool_guide(tool_name: &str) -> Option<Arc<dyn ToolGuide>> {
    builtin_guide_spec(tool_name).map(|guide| Arc::new(guide) as Arc<dyn ToolGuide>)
}

pub fn builtin_guide_spec(tool_name: &str) -> Option<ToolGuideSpec> {
    match tool_name {
        "read_file" => Some(guide(
            "read_file",
            ToolCategory::FileReading,
            "Read small to medium text files (approx <=500 lines) to understand context / Read full small files.",
            "Do not read entire files for large files or when only partial content is needed / Use read_file_range for large files.",
            &["file_exists", "read_file_range", "search_in_file"],
            vec![example(
                "Read project dependency configuration",
                json!({ "path": "/workspace/project/Cargo.toml" }),
                "Establish context first, then decide whether to modify.",
            )],
        )),
        "write_file" => Some(guide(
            "write_file",
            ToolCategory::FileWriting,
            "Create new files or fully overwrite target files / Create or fully rewrite a file.",
            "Not recommended for partial content changes / Prefer apply_patch for partial edits.",
            &["file_exists", "read_file", "apply_patch"],
            vec![example(
                "Generate configuration file",
                json!({
                    "path": "/workspace/project/.env.example",
                    "content": "API_KEY=\nMODEL=default\n"
                }),
                "Suitable for first-time creation or full replacement.",
            )],
        )),
        "read_file_range" => Some(guide(
            "read_file_range",
            ToolCategory::FileReading,
            "Read large files in chunks or view specific line ranges / Read targeted line ranges.",
            "Not suitable when regex matching is needed / Prefer search_in_file for regex lookup.",
            &["read_file", "search_in_file"],
            vec![example(
                "View implementation near a function",
                json!({
                    "path": "/workspace/project/src/lib.rs",
                    "start_line": 120,
                    "end_line": 180
                }),
                "Focus on necessary context, reduce noise.",
            )],
        )),
        "file_exists" => Some(guide(
            "file_exists",
            ToolCategory::FileReading,
            "Confirm path validity before read/write operations / Validate path existence before operations.",
            "Not suitable when size, type, or timestamp info is needed / Use get_file_info for metadata.",
            &["get_file_info", "read_file", "write_file"],
            vec![example(
                "Confirm target file exists",
                json!({ "path": "/workspace/project/src/main.rs" }),
                "Avoid failures in subsequent tools due to path errors.",
            )],
        )),
        "get_file_info" => Some(guide(
            "get_file_info",
            ToolCategory::FileReading,
            "View file type, size, and modification time / Inspect file metadata.",
            "Not suitable for reading file content / Use read_file or read_file_range.",
            &["file_exists", "list_directory"],
            vec![example(
                "Determine if path is a file or directory",
                json!({ "path": "/workspace/project/src" }),
                "Help select appropriate tools for subsequent reading or traversal.",
            )],
        )),
        "list_directory" => Some(guide(
            "list_directory",
            ToolCategory::FileReading,
            "Quickly understand directory structure and candidate files / Inspect direct folder contents.",
            "Not suitable for cross-directory content search / Use search_in_project for recursive content search.",
            &["file_exists", "get_file_info", "search_in_project"],
            vec![example(
                "View source directory",
                json!({ "path": "/workspace/project/src" }),
                "First locate potential files, then proceed to read or search.",
            )],
        )),
        "set_workspace" => Some(guide(
            "set_workspace",
            ToolCategory::CommandExecution,
            "Unify working directory at the start of a task / Align workspace for path consistency.",
            "Not suitable when only confirming current location / Use get_current_dir when no switch is needed.",
            &["get_current_dir", "list_directory", "execute_command"],
            vec![example(
                "Switch to target repository",
                json!({ "path": "/workspace/project" }),
                "Subsequent commands and file operations will be based on this directory.",
            )],
        )),
        "search_in_file" => Some(guide(
            "search_in_file",
            ToolCategory::CodeSearch,
            "Locate keywords or patterns with regex in a single file / Find matches in one file.",
            "Not suitable for cross-project scanning / Use search_in_project across directories.",
            &["read_file_range", "search_in_project"],
            vec![example(
                "Locate trait implementation",
                json!({
                    "path": "/workspace/project/src/lib.rs",
                    "pattern": "impl\\s+MyTrait",
                    "case_sensitive": false
                }),
                "First find the location, then read local context.",
            )],
        )),
        "search_in_project" => Some(guide(
            "search_in_project",
            ToolCategory::CodeSearch,
            "Search for definitions, references, and patterns across directories / Search definitions and references project-wide.",
            "Not suitable when only checking one file / search_in_file is cheaper for single-file lookup.",
            &["search_in_file", "list_directory", "read_file_range"],
            vec![example(
                "Find Rust trait implementations",
                json!({
                    "directory": "/workspace/project",
                    "pattern": "impl\\s+.*MyTrait",
                    "file_extensions": ["rs"]
                }),
                "Quickly establish impact scope for subsequent modifications.",
            )],
        )),
        "apply_patch" => Some(guide(
            "apply_patch",
            ToolCategory::FileWriting,
            "Apply precise partial modifications to existing files / Apply minimal targeted edits.",
            "Not suitable for full file rewrites / Use write_file for full rewrites.",
            &["read_file_range", "write_file", "git_diff"],
            vec![example(
                "Replace function body by line range",
                json!({
                    "path": "/workspace/project/src/lib.rs",
                    "mode": "line_replace",
                    "start_line": 42,
                    "end_line": 58,
                    "new_content": "pub fn updated() {\n    println!(\"ok\");\n}\n"
                }),
                "Suitable for small, controllable changes.",
            )],
        )),
        "execute_command" => Some(guide(
            "execute_command",
            ToolCategory::CommandExecution,
            "Run build, test, script, and diagnostic commands / Run shell commands for validation.",
            "Not suitable when file tools alone can accomplish the task / Avoid unnecessary command execution.",
            &["get_current_dir", "git_status", "git_diff"],
            vec![example(
                "Run unit tests",
                json!({
                    "command": "cargo",
                    "args": ["test", "-p", "agent-tools"],
                    "cwd": "/workspace/project"
                }),
                "Verify correct behavior before committing.",
            )],
        )),
        "get_current_dir" => Some(guide(
            "get_current_dir",
            ToolCategory::CommandExecution,
            "Confirm current working directory / Confirm workspace context.",
            "Not suitable when directory switching is needed / Use set_workspace for directory changes.",
            &["set_workspace", "list_directory"],
            vec![example(
                "Diagnose relative path issues",
                json!({}),
                "First confirm the process directory, then execute subsequent operations.",
            )],
        )),
        "git_status" => Some(guide(
            "git_status",
            ToolCategory::GitOperations,
            "View branch and file status summary / Inspect branch and working tree state.",
            "Not suitable when line-by-line change content is needed / Use git_diff for patch details.",
            &["git_diff", "execute_command"],
            vec![example(
                "Check if repository is clean",
                json!({ "cwd": "/workspace/project" }),
                "Quickly verify status before and after changes.",
            )],
        )),
        "git_diff" => Some(guide(
            "git_diff",
            ToolCategory::GitOperations,
            "View specific code differences / Inspect actual code changes.",
            "Not suitable when only a change summary is needed / git_status is faster for summaries.",
            &["git_status", "apply_patch"],
            vec![example(
                "View staged changes",
                json!({
                    "cwd": "/workspace/project",
                    "staged": true
                }),
                "Used for pre-commit review.",
            )],
        )),
        "create_todo_list" => Some(guide(
            "create_todo_list",
            ToolCategory::TaskManagement,
            "MANDATORY for multi-step tasks: Create a structured todo list INSTEAD of markdown checklists. This enables real-time progress tracking and automatic status updates.",
            "SKIP only for single-step trivial tasks / DO NOT output markdown checklists - always use this tool for multi-step work.",
            &["update_todo_item", "ask_user"],
            vec![example(
                "Create implementation plan",
                json!({
                    "title": "Implement guide system",
                    "items": [
                        { "id": "1", "description": "Design API" },
                        { "id": "2", "description": "Implement registry", "depends_on": ["1"] }
                    ]
                }),
                "Define order and dependencies first, then execute implementation.",
            )],
        )),
        "update_todo_item" => Some(guide(
            "update_todo_item",
            ToolCategory::TaskManagement,
            "Update status and notes after completing a phase / Keep progress synchronized with execution.",
            "Not suitable when no list has been created / Use after create_todo_list only.",
            &["create_todo_list"],
            vec![example(
                "Mark task as completed",
                json!({
                    "item_id": "2",
                    "status": "completed",
                    "notes": "Registry + tests merged"
                }),
                "Keep context continuously reflecting real progress.",
            )],
        )),
        "ask_user" => Some(guide(
            "ask_user",
            ToolCategory::UserInteraction,
            "Request user decision when requirements are ambiguous, solutions conflict, or risk is high / Ask for clarification before risky choices.",
            "Not suitable when information is sufficient to proceed / Avoid unnecessary interruptions.",
            &["create_todo_list", "update_todo_item"],
            vec![example(
                "Request choice between two implementation options",
                json!({
                    "question": "Which migration strategy should we use?",
                    "options": ["Incremental rollout", "Single cutover"],
                    "allow_custom": true
                }),
                "Reduce misjudgment risk and align expectations.",
            )],
        )),
        _ => None,
    }
}

pub fn builtin_guides() -> Vec<ToolGuideSpec> {
    BUILTIN_GUIDE_NAMES
        .iter()
        .filter_map(|name| builtin_guide_spec(name))
        .collect()
}

fn guide(
    tool_name: &str,
    category: ToolCategory,
    when_to_use: &str,
    when_not_to_use: &str,
    related_tools: &[&str],
    examples: Vec<ToolExample>,
) -> ToolGuideSpec {
    ToolGuideSpec {
        tool_name: tool_name.to_string(),
        when_to_use: when_to_use.to_string(),
        when_not_to_use: when_not_to_use.to_string(),
        examples,
        related_tools: related_tools.iter().map(|name| name.to_string()).collect(),
        category,
    }
}

fn example(scenario: &str, parameters: serde_json::Value, explanation: &str) -> ToolExample {
    ToolExample::new(scenario, parameters, explanation)
}

#[cfg(test)]
mod tests {
    use super::{builtin_guide_spec, BUILTIN_GUIDE_NAMES};

    #[test]
    fn every_builtin_tool_has_a_guide() {
        for name in BUILTIN_GUIDE_NAMES {
            assert!(builtin_guide_spec(name).is_some(), "missing guide for {}", name);
        }
    }
}
