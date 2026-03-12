//! Built-in tool guides for the Claude-style tool surface.

use std::sync::Arc;

use serde_json::json;

use super::{ToolCategory, ToolExample, ToolGuide, ToolGuideSpec};

pub const BUILTIN_GUIDE_NAMES: [&str; 16] = [
    "ask_user",
    "Bash",
    "BashOutput",
    "Edit",
    "ExitPlanMode",
    "Glob",
    "Grep",
    "KillShell",
    "NotebookEdit",
    "Read",
    "SlashCommand",
    "Task",
    "TodoWrite",
    "WebFetch",
    "WebSearch",
    "Write",
];

pub fn builtin_tool_guide(tool_name: &str) -> Option<Arc<dyn ToolGuide>> {
    builtin_guide_spec(tool_name).map(|guide| Arc::new(guide) as Arc<dyn ToolGuide>)
}

pub fn builtin_guide_spec(tool_name: &str) -> Option<ToolGuideSpec> {
    match tool_name {
        "ask_user" => Some(guide(
            "ask_user",
            ToolCategory::UserInteraction,
            "Ask the user for confirmation or missing input with selectable options.",
            "Do not use when the task can safely proceed without user confirmation.",
            &["ExitPlanMode"],
            vec![example(
                "Confirm before finishing",
                json!({"question":"Any other requests before I finish?","options":["OK","Need changes"],"allow_custom":true}),
                "Use when user intent is required before finalizing.",
            )],
        )),
        "Read" => Some(guide(
            "Read",
            ToolCategory::FileReading,
            "Use for reading file contents, especially before Edit/Write on existing files.",
            "Do not use for writing or cross-project search; do not substitute Bash cat/head/tail for this.",
            &["Edit", "Write", "Grep"],
            vec![example(
                "Read a config file",
                json!({"file_path":"/workspace/project/config.toml"}),
                "Use before making any edits.",
            )],
        )),
        "Write" => Some(guide(
            "Write",
            ToolCategory::FileWriting,
            "Use for creating files or replacing full file contents.",
            "Do not use for small in-place updates; prefer Edit after Read.",
            &["Read", "Edit"],
            vec![example(
                "Write a full file",
                json!({"file_path":"/workspace/project/.env.example","content":"API_KEY=\n"}),
                "Suitable for full replacements.",
            )],
        )),
        "Edit" => Some(guide(
            "Edit",
            ToolCategory::FileWriting,
            "Use for precise in-place updates in existing files. Prefer patch mode with SEARCH/REPLACE blocks and enough context to target unique locations.",
            "Do not use before Read on existing files; do not use for full-file rewrites; do not use ambiguous SEARCH blocks that match multiple locations.",
            &["Read", "Write"],
            vec![
                example(
                    "Patch a specific block",
                    json!({"file_path":"/workspace/project/src/main.rs","patch":"<<<<<<< SEARCH\nfn b() {\n    let v = 1;\n}\n=======\nfn b() {\n    let v = 2;\n}\n>>>>>>> REPLACE"}),
                    "Preferred for repeated code because context disambiguates the target.",
                ),
                example(
                    "Legacy single replacement",
                    json!({"file_path":"/workspace/project/src/main.rs","old_string":"foo","new_string":"bar"}),
                    "Use only when old_string is known to match exactly once.",
                ),
            ],
        )),
        "Glob" => Some(guide(
            "Glob",
            ToolCategory::CodeSearch,
            "Find files by glob pattern before deeper content search.",
            "Do not use for content search; avoid unbounded root patterns like **/* without narrowing path/pattern.",
            &["Grep", "Read"],
            vec![example(
                "Find all Rust files",
                json!({"pattern":"**/*.rs","path":"/workspace/project"}),
                "Use before Grep when scope is unknown.",
            )],
        )),
        "Grep" => Some(guide(
            "Grep",
            ToolCategory::CodeSearch,
            "Search project contents using regex; start with files_with_matches then narrow with Read/content mode.",
            "Do not run broad content or multiline searches across the full workspace; always narrow with path/glob/type first.",
            &["Glob", "Read"],
            vec![example(
                "Find function usages",
                json!({"pattern":"execute_with_context","glob":"**/*.rs","output_mode":"files_with_matches","head_limit":200}),
                "First identify candidate files, then use Read or scoped content mode.",
            )],
        )),
        "Bash" => Some(guide(
            "Bash",
            ToolCategory::CommandExecution,
            "Run terminal commands (build/test/git/npm/docker/gh), optionally in background.",
            "Do not use for file reads/edits/search when Read/Edit/Write/Glob/Grep can handle it; do not use shell echo/printf to communicate with the user.",
            &["BashOutput", "KillShell"],
            vec![example(
                "Run tests",
                json!({"command":"cargo test","timeout":120000}),
                "Use for build/test/CLI operations.",
            )],
        )),
        "BashOutput" => Some(guide(
            "BashOutput",
            ToolCategory::CommandExecution,
            "Read incremental output from a background shell.",
            "Do not use without a bash_id from Bash.",
            &["Bash", "KillShell"],
            vec![example(
                "Poll output",
                json!({"bash_id":"abc"}),
                "Use repeatedly until shell completes.",
            )],
        )),
        "KillShell" => Some(guide(
            "KillShell",
            ToolCategory::CommandExecution,
            "Terminate a background shell.",
            "Do not use for foreground commands.",
            &["Bash", "BashOutput"],
            vec![example(
                "Stop runaway process",
                json!({"shell_id":"abc"}),
                "Use when process should no longer run.",
            )],
        )),
        "NotebookEdit" => Some(guide(
            "NotebookEdit",
            ToolCategory::FileWriting,
            "Edit notebook cells by replace/insert/delete.",
            "Do not use for non-notebook files.",
            &["Read", "Write"],
            vec![example(
                "Replace first cell",
                json!({"notebook_path":"/workspace/project/demo.ipynb","new_source":"print('ok')"}),
                "Use with absolute notebook path.",
            )],
        )),
        "SlashCommand" => Some(guide(
            "SlashCommand",
            ToolCategory::UserInteraction,
            "Resolve and execute a slash command template.",
            "Do not use for arbitrary shell execution.",
            &["Bash", "Read"],
            vec![example(
                "Run review command",
                json!({"command":"/review"}),
                "Useful for reusable prompt workflows.",
            )],
        )),
        "Task" => Some(guide(
            "Task",
            ToolCategory::TaskManagement,
            "Delegate a sub-session (sub task/team agent/parallel worker). Always set a clear title and a single explicit responsibility.",
            "Do not use for trivial single-step operations, and do not omit title/responsibility.",
            &["TodoWrite"],
            vec![example(
                "Delegate research",
                json!({"title":"Search refs","responsibility":"Find parser entrypoints and summarize findings","prompt":"Scan parser modules and report key entrypoints with file paths.","subagent_type":"general-purpose"}),
                "Use when work can be isolated and run in parallel.",
            )],
        )),
        "TodoWrite" => Some(guide(
            "TodoWrite",
            ToolCategory::TaskManagement,
            "Maintain a structured task checklist for complex tasks.",
            "Do not use for trivial one-step requests.",
            &["Task", "ExitPlanMode"],
            vec![example(
                "Update task statuses",
                json!({"todos":[{"content":"Run tests","status":"in_progress","activeForm":"Running tests"}]}),
                "Keep exactly one item in_progress whenever possible.",
            )],
        )),
        "ExitPlanMode" => Some(guide(
            "ExitPlanMode",
            ToolCategory::UserInteraction,
            "Ask for confirmation before leaving plan mode.",
            "Do not use if implementation can proceed directly.",
            &["TodoWrite"],
            vec![example(
                "Plan complete",
                json!({"plan":"1. Do A\n2. Do B"}),
                "Use after producing a concrete implementation plan.",
            )],
        )),
        "WebFetch" => Some(guide(
            "WebFetch",
            ToolCategory::CommandExecution,
            "Fetch a specific webpage and summarize content for a prompt.",
            "Do not use for broad discovery queries.",
            &["WebSearch"],
            vec![example(
                "Fetch docs page",
                json!({"url":"https://example.com/docs","prompt":"Extract setup steps"}),
                "Use when you already know the URL.",
            )],
        )),
        "WebSearch" => Some(guide(
            "WebSearch",
            ToolCategory::CommandExecution,
            "Search the web with optional domain allow/block filters.",
            "Do not use for local codebase search.",
            &["WebFetch", "Grep"],
            vec![example(
                "Search official docs",
                json!({"query":"rust async trait object", "allowed_domains":["doc.rust-lang.org"]}),
                "Use before WebFetch when URL is unknown.",
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
            assert!(
                builtin_guide_spec(name).is_some(),
                "missing guide for {}",
                name
            );
        }
    }
}
