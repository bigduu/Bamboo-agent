//! Built-in tool guides for the Claude-style tool surface.

use std::sync::Arc;

use serde_json::json;

use super::{ToolCategory, ToolExample, ToolGuide, ToolGuideSpec};

pub const BUILTIN_GUIDE_NAMES: [&str; 21] = [
    "conclusion_with_options",
    "Bash",
    "BashOutput",
    "Edit",
    "ExitPlanMode",
    "GetFileInfo",
    "Glob",
    "Grep",
    "js_repl",
    "KillShell",
    "memory_note",
    "NotebookEdit",
    "Read",
    "request_permissions",
    "Sleep",
    "Task",
    "tool_search",
    "WebFetch",
    "WebSearch",
    "Workspace",
    "Write",
];

pub fn builtin_tool_guide(tool_name: &str) -> Option<Arc<dyn ToolGuide>> {
    builtin_guide_spec(tool_name).map(|guide| Arc::new(guide) as Arc<dyn ToolGuide>)
}

pub fn builtin_guide_spec(tool_name: &str) -> Option<ToolGuideSpec> {
    match tool_name {
        // apply_patch is now an alias for Edit; return the Edit guide with
        // the alias name so existing callers still see relevant documentation.
        "apply_patch" => Some(guide(
            "apply_patch",
            ToolCategory::FileWriting,
            "Use for patch-only in-place updates using SEARCH/REPLACE blocks, aligned with Edit patch mode.",
            "Do not use before Read on existing files; do not use for full-file rewrites; do not mix with old_string/new_string style arguments.",
            &["Read", "Edit", "Write"],
            vec![example(
                "Apply a patch block",
                json!({"file_path":"/workspace/project/src/main.rs","patch":"<<<<<<< SEARCH\nlet v = 1;\n=======\nlet v = 2;\n>>>>>>> REPLACE"}),
                "Use when you want a dedicated patch tool call instead of Edit.",
            )],
        )),
        "conclusion_with_options" => Some(guide(
            "conclusion_with_options",
            ToolCategory::UserInteraction,
            "Ask the user for confirmation or missing input with selectable options. Include a structured `conclusion` object (summary + Mermaid graph) in the same call when you need a wrapped-up confirmation flow.",
            "Do not use repeatedly for routine status updates during active execution; reserve it for true clarification points, explicit user decisions, or a final confirmation flow when required by the active prompt/policy.",
            &["ExitPlanMode"],
            vec![example(
                "Confirm before finishing",
                json!({
                    "question":"Any other requests before I finish?",
                    "conclusion":{
                        "title":"Conclusion",
                        "summary":"Core validation is complete and release is ready.",
                        "key_points":["All targeted tests passed","No blocking regressions"],
                        "next_steps":["Proceed with release train"],
                        "confidence":"high",
                        "mermaid":{"graph":"graph TD\nA[Validation]-->B[Ready to release]"}
                    }
                }),
                "Use when user intent is required before finalizing. Defaults to options [\"OK\", \"Need changes\"] when options are omitted.",
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
                example(
                    "Legacy replacement with line hint",
                    json!({"file_path":"/workspace/project/src/main.rs","old_string":"value = 1","new_string":"value = 2","line_number":42}),
                    "Use line_number when old_string may appear in multiple places.",
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
            "Do not use for foreground commands; use the ID returned by Bash(run_in_background=true), not chat session_id.",
            &["Bash", "BashOutput"],
            vec![example(
                "Stop runaway process",
                json!({"shell_id":"<bash_id-from-Bash>"}),
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
            "Create or update the shared task list for the current root session tree (root + child sessions share the same task list).",
            "Do not use for trivial one-step requests.",
            &["ExitPlanMode"],
            vec![example(
                "Update shared task statuses",
                json!({"tasks":[{"content":"Run tests","status":"in_progress","activeForm":"Running tests"}]}),
                "Keep exactly one item in_progress whenever possible.",
            )],
        )),
        "ExitPlanMode" => Some(guide(
            "ExitPlanMode",
            ToolCategory::UserInteraction,
            "Ask for confirmation before leaving plan mode.",
            "Do not use if implementation can proceed directly.",
            &["Task"],
            vec![example(
                "Plan complete",
                json!({"plan":"1. Do A\n2. Do B"}),
                "Use after producing a concrete implementation plan.",
            )],
        )),
        "tool_search" => Some(guide(
            "tool_search",
            ToolCategory::TaskManagement,
            "Search for available tools by keyword query. Returns the most relevant tools ranked by BM25 relevance.",
            "Do not use when you already know the exact tool name. Use for discovery only.",
            &["load_skill"],
            vec![example(
                "Find file editing tools",
                json!({"query":"edit file content"}),
                "Use when unsure which tool handles a task.",
            )],
        )),
        // FileExists guide moved next to GetFileInfo (see below).
        "WebFetch" => Some(guide(
            "WebFetch",
            ToolCategory::CommandExecution,
            "Fetch a webpage by URL when you need cleaned page text from a known target.",
            "Do not use for broad discovery queries.",
            &["WebSearch"],
            vec![example(
                "Fetch a target page",
                json!({"url":"https://target-host/path","prompt":"Extract setup steps"}),
                "The prompt field is context for downstream handling; WebFetch itself returns cleaned text + metadata.",
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
        // GetCurrentDir is now an alias for Workspace (get mode)
        "GetCurrentDir" => Some(guide(
            "GetCurrentDir",
            ToolCategory::CommandExecution,
            "Retrieve the session's current workspace directory. (Alias for Workspace with no path argument.)",
            "Do not use when absolute paths are already known.",
            &["Workspace", "Bash", "Read"],
            vec![example(
                "Inspect working directory",
                json!({}),
                "Useful before commands or file ops that rely on relative paths.",
            )],
        )),
        "GetFileInfo" => Some(guide(
            "GetFileInfo",
            ToolCategory::FileReading,
            "Check if a file/dir exists and read metadata (type, size, modified time) without loading content. Returns {exists: false} for missing paths, so this also covers existence checks.",
            "Do not use when you need actual content; use Read instead.",
            &["Read"],
            vec![example(
                "Check metadata before processing",
                json!({"path":"/workspace/project/logs/app.log"}),
                "Use to branch behavior based on file type/size.",
            )],
        )),
        // FileExists is now an alias for GetFileInfo
        "FileExists" => Some(guide(
            "FileExists",
            ToolCategory::FileReading,
            "Check quickly whether a path exists before reading, editing, or writing conditionally. (Alias for GetFileInfo)",
            "Do not use to inspect file content or metadata details.",
            &["GetFileInfo", "Read", "Write"],
            vec![example(
                "Guard before write",
                json!({"path":"/workspace/project/.env"}),
                "Use as a fast existence probe before deciding create vs update.",
            )],
        )),
        "memory_note" => Some(guide(
            "memory_note",
            ToolCategory::TaskManagement,
            "Store durable per-session facts/decisions and retrieve them across turns. Supports multiple topics per session to keep unrelated workstreams separate.",
            "Do not store secrets/tokens or transient one-turn scratch text.",
            &["Task"],
            vec![
                example(
                    "Persist a durable decision",
                    json!({"action":"append","content":"User prefers pnpm and strict TypeScript."}),
                    "Use append for new durable facts; use replace to compress long notes.",
                ),
                example(
                    "Store notes for a specific topic",
                    json!({"action":"append","topic":"backend-api","content":"REST endpoints finalized: /users, /orders."}),
                    "Use topic to keep separate workstreams isolated from each other.",
                ),
            ],
        )),

        // SetWorkspace is now an alias for Workspace (set mode)
        "SetWorkspace" => Some(guide(
            "SetWorkspace",
            ToolCategory::CommandExecution,
            "Change the current session workspace. (Alias for Workspace with path argument.)",
            "Do not use with non-directory or missing paths.",
            &["Workspace", "Bash", "Read"],
            vec![example(
                "Switch session workspace",
                json!({"path":"/workspace/project"}),
                "Use before running commands or edits in another repo root.",
            )],
        )),
        // Workspace is the unified tool replacing GetCurrentDir + SetWorkspace
        "Workspace" => Some(guide(
            "Workspace",
            ToolCategory::CommandExecution,
            "Get or set the current session workspace directory. Call without 'path' to read the current workspace; call with 'path' to change it.",
            "Do not set workspace to a non-directory or missing path.",
            &["Bash", "Read"],
            vec![
                example(
                    "Get current workspace",
                    json!({}),
                    "Useful before commands or file ops that rely on relative paths.",
                ),
                example(
                    "Set workspace",
                    json!({"path":"/workspace/project"}),
                    "Use before running commands or edits in another repo root.",
                ),
            ],
        )),
        "js_repl" => Some(guide(
            "js_repl",
            ToolCategory::CommandExecution,
            "Execute JavaScript code using Node.js with top-level await support. Each invocation runs in a fresh process.",
            "Do not use for tasks better handled by Bash. Requires Node.js installed on the host.",
            &["Bash"],
            vec![example(
                "Evaluate a JS expression",
                json!({"code":"console.log(2 + 2)"}),
                "Use for quick calculations, JSON manipulation, or any JS-specific task.",
            )],
        )),
        "request_permissions" => Some(guide(
            "request_permissions",
            ToolCategory::UserInteraction,
            "Request elevated permissions from the user when a needed operation would be blocked. The agent loop pauses until the user approves or denies.",
            "Do not use pre-emptively for every operation; only request when you know the current permission set is insufficient.",
            &["conclusion_with_options"],
            vec![example(
                "Request file write permission",
                json!({"reason":"Need to write deployment config to /etc/nginx","permissions":[{"type":"write_file","resource":"/etc/nginx/conf.d/*"}]}),
                "Use when a Write/Edit would be denied due to path restrictions.",
            )],
        )),
        "Sleep" => Some(guide(
            "Sleep",
            ToolCategory::CommandExecution,
            "Pause briefly when waiting for an external state change before polling again.",
            "Do not use for normal reasoning pauses or long waits when another tool can fetch status directly.",
            &["BashOutput", "WebFetch"],
            vec![example(
                "Wait before next poll",
                json!({"seconds":2,"reason":"wait for background process output"}),
                "Use short waits between repeated status checks.",
            )],
        )),
        "load_skill" => Some(guide(
            "load_skill",
            ToolCategory::TaskManagement,
            "Load a selected skill's SKILL.md instructions and metadata by skill_id.",
            "Do not use for auxiliary resource files; use read_skill_resource for references/assets.",
            &["read_skill_resource"],
            vec![example(
                "Load skill instructions",
                json!({"skill_id":"rust-best-practices"}),
                "Call this before following a matched skill's detailed workflow.",
            )],
        )),
        "read_skill_resource" => Some(guide(
            "read_skill_resource",
            ToolCategory::FileReading,
            "Read auxiliary files under a loaded skill directory with optional offset/limit paging.",
            "Do not use for SKILL.md itself; call load_skill for primary instructions.",
            &["load_skill"],
            vec![example(
                "Read a skill reference file",
                json!({"skill_id":"rust-best-practices","resource_path":"references/chapter_01.md","offset":0,"limit":80}),
                "Use when the loaded instructions point to additional files.",
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
    use crate::agent::tools::executor::BUILTIN_TOOL_NAMES;

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

    #[test]
    fn builtin_guides_cover_all_builtin_tool_names() {
        for name in BUILTIN_TOOL_NAMES {
            assert!(
                builtin_guide_spec(name).is_some(),
                "missing builtin guide coverage for {}",
                name
            );
        }
    }
}
