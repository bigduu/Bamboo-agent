//! Built-in tool guides for the Claude-style tool surface.

use std::sync::Arc;

use serde_json::json;

use super::{ToolCategory, ToolExample, ToolGuide, ToolGuideSpec};

pub const BUILTIN_GUIDE_NAMES: [&str; 23] = [
    "conclusion_with_options",
    "Bash",
    "BashInput",
    "BashOutput",
    "Edit",
    "EnterPlanMode",
    "ExitPlanMode",
    "GetFileInfo",
    "Glob",
    "Grep",
    "js_repl",
    "KillShell",
    "session_note",
    "NotebookEdit",
    "Read",
    "request_permissions",
    "Sleep",
    "Task",
    "update_goal",
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
            "Ask the user for confirmation or missing input with selectable options. Use this as the final interaction step when wrapping up a task turn, handing off execution, or asking the user to choose next steps.",
            "Do not use repeatedly for routine status updates during active execution; reserve it for true clarification points, explicit user decisions, or the required final confirmation flow.",
            &["ExitPlanMode"],
            vec![
                example(
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
                ),
                example(
                    "Ask the user to choose the next step",
                    json!({
                        "question":"Which next step should I take?",
                        "options":["Implement it","Refine the plan","Stop here","OK"],
                        "conclusion":{
                            "summary":"I finished the review and identified two viable implementation paths.",
                            "key_points":["Minimal change path is lower risk","Broader refactor will simplify future maintenance"],
                            "mermaid":{"graph":"graph TD\nA[Review done]-->B[Choose next step]"}
                        }
                    }),
                    "Use when the user must choose among concrete next actions instead of receiving plain prose.",
                ),
                example(
                    "Wrap up a review turn",
                    json!({
                        "question":"Want me to proceed with the recommended fix?",
                        "options":["Proceed","Need changes","OK"],
                        "conclusion":{
                            "summary":"The review is complete and I found one blocking issue plus two minor cleanups.",
                            "key_points":["Blocking issue identified","Fix scope is localized"],
                            "mermaid":{"graph":"graph TD\nA[Review complete]-->B[Decision needed]"}
                        }
                    }),
                    "Use at the end of review/explanation turns instead of a plain final paragraph.",
                ),
            ],
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
            "Run terminal commands (build/test/git/npm/docker/gh), optionally in background — a backgrounded command runs detached and notifies you with its result when it finishes, so you can keep working.",
            "Do not use for file reads/edits/search when Read/Edit/Write/Glob/Grep can handle it; do not use shell echo/printf to communicate with the user; do not sit polling BashOutput to wait for a background command — the completion notification comes to you.",
            &["BashOutput", "KillShell"],
            vec![example(
                "Run tests",
                json!({"command":"cargo test","timeout":120000}),
                "Use for build/test/CLI operations.",
            )],
        )),
        "BashInput" => Some(guide(
            "BashInput",
            ToolCategory::CommandExecution,
            "Send input to the stdin of an interactive background shell (spawned with Bash interactive=true) to answer a prompt, or close stdin (eof:true) to let a stdin-consumer finish.",
            "Do not use without a bash_id from an interactive Bash shell; the shell must have been spawned with interactive=true (piped stdin), otherwise this returns an error. The input is written as UTF-8 bytes; eof:true sends end-of-input so a consumer that reads until EOF (cat/sort/REPL) terminates instead of hanging.",
            &["Bash", "BashOutput"],
            vec![
                example(
                    "Answer a prompt",
                    json!({"bash_id":"<bash_id-from-Bash-interactive>","input":"y"}),
                    "Use when a background command is waiting for input (e.g. a confirmation prompt).",
                ),
                example(
                    "Send EOF so a consumer finishes",
                    json!({"bash_id":"<bash_id>","input":"data","eof":true}),
                    "Close stdin after writing so a command that reads until EOF terminates instead of running until killed.",
                ),
            ],
        )),
        "BashOutput" => Some(guide(
            "BashOutput",
            ToolCategory::CommandExecution,
            "Read the full/incremental output of a background shell on demand.",
            "Do not poll a background shell to wait for it — you are notified automatically when it completes (with its exit status and an output tail). Use BashOutput only when you want its output BEFORE that notification arrives; needs a bash_id from Bash.",
            &["Bash", "KillShell"],
            vec![example(
                "Inspect output early",
                json!({"bash_id":"abc"}),
                "Use to check on a still-running shell; you don't need it just to learn a shell finished.",
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
        "EnterPlanMode" => Some(guide(
            "EnterPlanMode",
            ToolCategory::UserInteraction,
            "Switch to plan mode for complex tasks requiring exploration and design before implementation.",
            "Do not use for simple tasks that can be implemented directly.",
            &["Task", "ExitPlanMode"],
            vec![example(
                "Start planning a complex refactor",
                json!({"reason":"This refactor touches multiple crates and needs careful design"}),
                "Use when facing a complex task that requires exploration before implementation.",
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
            "Search the web with optional domain allow/block filters. When searching for recent information, documentation, or current events, include the current year in the query to get up-to-date results.",
            "Do not use for local codebase search. Do not specify both allowed_domains and blocked_domains in the same request.",
            &["WebFetch", "Grep"],
            vec![
                example(
                    "Search official docs",
                    json!({"query":"rust async trait object", "allowed_domains":["doc.rust-lang.org"]}),
                    "Use before WebFetch when URL is unknown.",
                ),
                example(
                    "Search recent documentation",
                    json!({"query":"React documentation 2026", "max_results": 5}),
                    "Include the current year for recent docs or current events.",
                ),
            ],
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
        "session_note" | "memory_note" => Some(guide(
            if tool_name == "memory_note" {
                "memory_note"
            } else {
                "session_note"
            },
            ToolCategory::TaskManagement,
            "Store durable session-scoped notes and retrieve them across turns. Use it for local context, user preferences, constraints, and compression-resistant reminders within the current workstream.",
            "Do not store secrets/tokens, one-turn scratch text, or use it as the primary long-term knowledge base.",
            &["Task", "session_history"],
            vec![
                example(
                    "Persist a durable session constraint",
                    json!({"action":"append","content":"User prefers pnpm and strict TypeScript."}),
                    "Use append for stable session/workstream facts; use replace to compress long notes.",
                ),
                example(
                    "Store notes for a specific topic",
                    json!({"action":"append","topic":"backend-api","content":"REST endpoints finalized: /users, /orders."}),
                    "Use topic to keep separate workstreams isolated from each other.",
                ),
            ],
        )),
        "memory" => Some(guide(
            "memory",
            ToolCategory::TaskManagement,
            "Use Session memory for current-session continuity, Project memory for durable project facts and decisions, and Global memory for cross-project user context. Recall with a short lexical query, use Jiandu's compact default top three IDs and summaries, then get only the selected item that needs full context. Query before writing; store one confirmed atomic fact with a few useful keywords, entities, and tags.",
            "Do not store secrets, unverified claims, live state that you can inspect directly, embeddings, or unrelated facts in one memory. Treat canonical Project memory as trusted durable project authority but verify live state before acting; treat Dream as a low-trust derived orientation snapshot. Do not increase the query limit or get every hit by default, and merge or consolidate only the same confirmed fact.",
            &["session_note", "session_history", "Task"],
            vec![
                example(
                    "Read the current session note topic",
                    json!({"action":"session_read","topic":"default","options":{"max_chars":4000}}),
                    "Use for continuity state inside the current session without touching durable project/global memory.",
                ),
                example(
                    "Shortlist durable memories before reading full content",
                    json!({"action":"query","scope":"project","query":"mobile release freeze"}),
                    "Use Jiandu's compact default top three ID/summary results, then select only the item that needs full context.",
                ),
                example(
                    "Get the selected durable memory",
                    json!({"action":"get","id":"mem_20260403_001","options":{"max_chars":5000}}),
                    "Use after query for the one selected ID whose full body or frontmatter is needed.",
                ),
                example(
                    "Recall before answering about the user's own context",
                    json!({"action":"query","scope":"global","query":"preferred testing framework"}),
                    "When the user refers to their own preferences, opinions, or past decisions you don't already know — especially first-person questions ('what do I...', 'did I...', '我...?') — query memory BEFORE answering; do not say you don't know without checking.",
                ),
                example(
                    "Recall a past project decision before acting",
                    json!({"action":"query","scope":"project","query":"HTTP Tauri IPC decision"}),
                    "Treat a matching canonical Project memory as trusted durable project authority, then verify any live implementation state before acting.",
                ),
                example(
                    "Query before writing a durable fact",
                    json!({"action":"query","scope":"project","query":"mobile release freeze Tuesday"}),
                    "Check whether the same fact already exists before write; merge only when the selected existing item is the same confirmed fact.",
                ),
                example(
                    "Write a durable project memory",
                    json!({"action":"write","scope":"project","type":"project","title":"Mobile release freeze is Tuesday","content":"Mobile release freeze begins Tuesday.","keywords":["mobile","release freeze","Tuesday"],"entities":["Mobile"],"tags":["release"]}),
                    "Write one confirmed atomic fact with a specific title and only a few lexical retrieval hints.",
                ),
                example(
                    "Merge follow-up details into an existing durable memory",
                    json!({"action":"merge","id":"mem_20260403_001","content":"The release manager confirmed the Tuesday freeze.","keywords":["release freeze","Tuesday"],"entities":["release manager"],"tags":["confirmed"],"source_memory_ids":["mem_20260403_002"]}),
                    "Use ONLY when the new evidence is the same fact as the target memory and older overlapping items should be superseded. Do not merge unrelated facts together — create a separate memory instead.",
                ),
                example(
                    "Split a multi-topic memory into atomic memories",
                    json!({"action":"split","id":"mem_20260403_001","pieces":[{"title":"User prefers pnpm","type":"user","content":"User prefers pnpm.","keywords":["pnpm","package manager"],"entities":["pnpm"],"tags":["preference"]},{"title":"Mobile release freeze is Tuesday","type":"project","content":"Mobile release freeze begins Tuesday.","keywords":["release freeze","Tuesday"],"entities":["Mobile"],"tags":["release"]}]}),
                    "Use when one memory has accreted several unrelated facts (a 'blob'): split archives the original and creates one atomic memory per fact, preserving lineage via supersedes.",
                ),
                example(
                    "Consolidate near-duplicate memories into one",
                    json!({"action":"consolidate","ids":["mem_20260403_001","mem_20260403_007"],"type":"project","title":"Mobile release freeze is Tuesday","content":"Mobile release freeze begins Tuesday.","keywords":["release freeze","Tuesday"],"entities":["Mobile"],"tags":["release"]}),
                    "Use ONLY when two or more memories are the SAME fact: consolidate archives them all and creates one canonical atomic memory, preserving lineage via supersedes. Confirm sameness (e.g. via scan_duplicates / find_duplicates) before consolidating — never merge distinct facts.",
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
            "Request one or more remembered permission scopes through independent typed decisions. Batch entries are reviewed one at a time.",
            "Do not use this for a one-shot operation: invoke the target tool directly so Allow once is bound to that exact tool-call occurrence.",
            &["conclusion_with_options", "Write", "Edit", "Bash", "WebFetch"],
            vec![example(
                "Request remembered file-write permission",
                json!({"reason":"Need to write deployment config to /etc/nginx","permissions":[{"type":"write_file","resource":"/etc/nginx/conf.d/*"}]}),
                "Use when later target operations need a session/workspace/global remembered scope.",
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
        "update_goal" => Some(guide(
            "update_goal",
            ToolCategory::TaskManagement,
            "Signal the autonomous session goal's status: status \"complete\" only when the full objective is achieved and you can prove every requirement is satisfied; status \"blocked\" only after the same blocker has persisted for at least three consecutive goal turns.",
            "Do not call to pause/resume or for ordinary task updates; do not mark complete merely because you are stopping or the budget is nearly spent. Completion is reverified by a side-channel check before the run ends.",
            &["Task"],
            vec![example(
                "Mark the goal achieved",
                json!({"status":"complete"}),
                "Call only once every requirement is proven satisfied by current-state evidence.",
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
        "session_history" | "recall" | "session_inspector" => Some(guide(
            match tool_name {
                "session_inspector" => "session_inspector",
                "recall" => "recall",
                _ => "session_history",
            },
            ToolCategory::FileReading,
            "Inspect prior Bamboo session history. Use list/get_meta before bounded messages, compressed history or search. A Root caller can export_context for itself or a same-tree target with the same optional Project identity, then Read the immutable status/brief files by offset/limit. Exports contain last persisted observations, not verified live progress. Distinct from memory (durable knowledge).",
            "Do not use as a broad substitute for code search. export_context only materializes bounded read projections; it cannot control sessions, grant cross-tree access, or provide a complete task contract. Quota/corruption errors do not authorize deleting old snapshots. Keep child-session inspection local unless delegation is explicitly requested.",
            &["session_note", "Read", "Task"],
            vec![
                example(
                    "Read a bounded context file for this session tree",
                    json!({"action":"export_context","session_id":"owned-child-123"}),
                    "Use Read on the returned status_path or brief_path with a small offset/limit. Keep the returned revision fixed across continuation reads; a later export may produce a new revision. Output paths are runtime-derived.",
                ),
                example(
                    "Search prior discussion history",
                    json!({"action":"search","query":"release checklist","mode":"tail_messages","max_sessions":10,"tail_messages":6}),
                    "Use when you need to recover what was discussed previously before asking the user to repeat it.",
                ),
                example(
                    "Inspect a specific session with bounded reads",
                    json!({"action":"read_messages","session_id":"session-123","from_end":true,"limit":20,"include_system":false,"truncate_chars":200}),
                    "Prefer bounded slices rather than dumping an entire session at once.",
                ),
            ],
        )),
        "scheduler" | "schedule_tasks" => Some(guide(
            if tool_name == "schedule_tasks" {
                "schedule_tasks"
            } else {
                "scheduler"
            },
            ToolCategory::TaskManagement,
            "Manage Bamboo scheduled automation jobs for recurring or delayed work. Use it to create, inspect, modify, run, or delete schedules without going through HTTP.",
            "Do not use for normal one-shot task planning inside the current conversation; use Task for active execution tracking instead.",
            &["Task", "Workspace"],
            vec![
                example(
                    "Create a recurring schedule",
                    json!({"action":"create","name":"daily-review","trigger":{"type":"interval","every_seconds":86400},"enabled":true,"run_config":{"auto_execute":true,"task_message":"Review new tickets","workspace_path":"/workspace/project"}}),
                    "Use when work should recur automatically over time.",
                ),
                example(
                    "Inspect schedule sessions",
                    json!({"action":"list_sessions","schedule_id":"sch_123"}),
                    "Use to see the sessions a schedule created or ran.",
                ),
            ],
        )),
        "SubAgent" => Some(guide(
            "SubAgent",
            ToolCategory::TaskManagement,
            "Create, inspect, and manage child sessions for explicitly requested delegated, parallel, or sub-agent work. Use action=create to spawn a new child; use list/get to inspect existing children before creating duplicates; use update/run/send_message/cancel/delete to manage existing children.",
            "Do not use proactively for simple one-step tasks; from a child session, create a nested child only when the current assignment explicitly authorizes nested delegation and it is necessary; do not create multiple overlapping children with unclear responsibilities.",
            &["Task"],
            vec![
                example(
                    "Create a read-only research child",
                    json!({"action":"create","title":"Inspect parser module","responsibility":"Find parser entrypoints and summarize data flow","prompt":"Read parser-related files and report key functions. Do not modify files.","subagent_type":"researcher"}),
                    "Use for isolated investigation that would otherwise fill the main context.",
                ),
                example(
                    "List existing children",
                    json!({"action":"list"}),
                    "Use before creating a new child when you might already have a relevant one.",
                ),
                example(
                    "Send follow-up to existing child",
                    json!({"action":"send_message","child_session_id":"child_123","message":"Focus on error handling paths and summarize risks.","auto_run":true}),
                    "Use follow-up instead of creating a duplicate child session.",
                ),
                example(
                    "Cancel a running child",
                    json!({"action":"cancel","child_session_id":"child_123"}),
                    "Use when a child session is no longer needed or has gone off track.",
                ),
            ],
        )),
        "deploy_agent" => Some(guide(
            "deploy_agent",
            ToolCategory::TaskManagement,
            "Spin up a new worker agent on demand and manage its lifecycle. action=deploy launches a fresh broker-agent as a local subprocess (env=local), a Docker container (env=docker, needs image), or a remote SSH host (env=ssh, needs host); it joins the same message broker, inherits your MCP servers + skills, and returns an id you then drive with ask_agent. action=list shows running workers; action=stop tears one down. Use it to scale yourself out — parallel hands locally, sandboxed work in Docker, or compute on another machine.",
            "Do not use when an existing worker (deploy_agent action=list) can take the task — reuse it with ask_agent instead of deploying a duplicate; do not use for in-context delegation that does not need a separate process (use SubAgent); always action=stop workers once their work is collected.",
            &["ask_agent", "SubAgent"],
            vec![
                example(
                    "Deploy a local worker",
                    json!({"action":"deploy","env":"local","role":"tester","model":"anthropic:claude-opus-4-8"}),
                    "Use for an extra parallel agent on this machine; the returned id is ask_agent's target.",
                ),
                example(
                    "Deploy a sandboxed Docker worker",
                    json!({"action":"deploy","env":"docker","image":"bamboo:latest","role":"researcher"}),
                    "Use for isolated/clean-env work; your bamboo home is mounted so it shares your config.",
                ),
                example(
                    "Deploy on a remote host",
                    json!({"action":"deploy","env":"ssh","host":"user@build-box","role":"builder"}),
                    "Use to run work near other machines or to borrow remote compute.",
                ),
                example(
                    "List then stop a worker",
                    json!({"action":"stop","id":"agent-7f8e9d"}),
                    "Use action=list to see running workers, then action=stop once a worker's work is done.",
                ),
            ],
        )),
        "ask_agent" => Some(guide(
            "ask_agent",
            ToolCategory::TaskManagement,
            "Command another running agent (deployed locally, in Docker, or on a remote host) over the message broker and get its answer back synchronously. target is the agent's broker id (from deploy_agent, or a peer's session id). mode=query (default) is READ-ONLY — the target summarizes/extracts from its current state without changing course, safe to poll repeatedly. mode=steer is WRITE — your question is injected into the target's live conversation to redirect or advance its work (assign the next task, hand off context).",
            "Do not use to talk to yourself or to an agent that does not exist yet — deploy it with deploy_agent first; do not use mode=steer when you only want to read progress (use query); for in-context delegation without a separate process, use SubAgent instead.",
            &["deploy_agent", "SubAgent"],
            vec![
                example(
                    "Poll a worker's progress (read-only)",
                    json!({"target":"agent-1a2b3c","question":"Summarize the auth flow you found so far.","mode":"query"}),
                    "Use query to pull a result or check status without disturbing the target's work.",
                ),
                example(
                    "Reassign or advance a worker (steer)",
                    json!({"target":"agent-1a2b3c","question":"Now write the fix to src/auth.rs and run the tests.","mode":"steer","timeout_secs":180}),
                    "Use steer to inject a new task into the target's conversation; raise timeout_secs for slow work.",
                ),
            ],
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
    use crate::BUILTIN_TOOL_NAMES;

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

    #[test]
    fn memory_server_tool_has_fallback_guide_spec() {
        let guide = builtin_guide_spec("memory").expect("memory guide should exist");
        assert_eq!(guide.tool_name, "memory");
        assert!(guide.examples.iter().any(|example| {
            example
                .parameters
                .get("action")
                .and_then(|value| value.as_str())
                == Some("merge")
        }));
    }

    #[test]
    fn memory_guide_teaches_compact_recall_and_small_lexical_hints() {
        let guide = builtin_guide_spec("memory").expect("memory guide should exist");
        let examples_for = |action: &str| {
            guide
                .examples
                .iter()
                .filter(|example| {
                    example
                        .parameters
                        .get("action")
                        .and_then(|value| value.as_str())
                        == Some(action)
                })
                .collect::<Vec<_>>()
        };

        let queries = examples_for("query");
        assert!(!queries.is_empty());
        assert!(queries
            .iter()
            .all(|example| example.parameters.pointer("/options/limit").is_none()));
        assert_eq!(examples_for("get").len(), 1);

        for action in ["write", "merge", "consolidate"] {
            let examples = examples_for(action);
            assert_eq!(examples.len(), 1, "missing {action} example");
            for field in ["keywords", "entities", "tags"] {
                let hints = examples[0].parameters[field]
                    .as_array()
                    .unwrap_or_else(|| panic!("{action} example is missing {field}"));
                assert!(!hints.is_empty() && hints.len() <= 3);
            }
        }

        let split = examples_for("split");
        let pieces = split[0].parameters["pieces"]
            .as_array()
            .expect("split example should contain pieces");
        for piece in pieces {
            for field in ["keywords", "entities", "tags"] {
                let hints = piece[field]
                    .as_array()
                    .unwrap_or_else(|| panic!("split piece is missing {field}"));
                assert!(!hints.is_empty() && hints.len() <= 3);
            }
        }
    }

    #[test]
    fn subagent_guide_makes_nested_delegation_explicit_and_necessary() {
        let guide = builtin_guide_spec("SubAgent").expect("SubAgent guide should exist");
        assert!(guide.when_not_to_use.contains("child session"));
        assert!(guide
            .when_not_to_use
            .contains("explicitly authorizes nested delegation"));
        assert!(guide.when_not_to_use.contains("it is necessary"));
        assert!(!guide
            .when_not_to_use
            .contains("do not spawn children from child sessions"));
    }

    #[test]
    fn broker_server_tools_have_fallback_guide_specs() {
        // deploy_agent: covers all three placements + a steer example via ask_agent.
        let deploy = builtin_guide_spec("deploy_agent").expect("deploy_agent guide should exist");
        assert_eq!(deploy.tool_name, "deploy_agent");
        assert!(deploy.related_tools.iter().any(|t| t == "ask_agent"));
        for env in ["local", "docker", "ssh"] {
            assert!(
                deploy.examples.iter().any(|example| example
                    .parameters
                    .get("env")
                    .and_then(|value| value.as_str())
                    == Some(env)),
                "deploy_agent guide should show an env={env} example"
            );
        }

        // ask_agent: must teach both query (read-only) and steer (write) modes.
        let ask = builtin_guide_spec("ask_agent").expect("ask_agent guide should exist");
        assert_eq!(ask.tool_name, "ask_agent");
        assert!(ask.related_tools.iter().any(|t| t == "deploy_agent"));
        for mode in ["query", "steer"] {
            assert!(
                ask.examples.iter().any(|example| example
                    .parameters
                    .get("mode")
                    .and_then(|value| value.as_str())
                    == Some(mode)),
                "ask_agent guide should show a mode={mode} example"
            );
        }
    }
}
