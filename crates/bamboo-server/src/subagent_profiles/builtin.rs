//! Built-in subagent profiles.
//!
//! These mirror the six roles defined in the SubagentProfile design doc.
//! Tool names follow the canonical names exposed by `bamboo-tools` and the
//! agent runtime. Allow / deny lists are validated lazily — unknown tool
//! names are simply ignored at runtime by the (future) `FilteredExecutor`.
//!
//! `general-purpose` is the **fallback** profile: its `system_prompt` is
//! intentionally identical to today's `CHILD_SYSTEM_PROMPT` so that
//! sessions resolving to this profile behave exactly like the legacy code
//! path.
//!
//! `plan` mirrors today's `PLAN_AGENT_SYSTEM_PROMPT` byte-for-byte and
//! pins the same read-only tool set, so existing plan-mode child sessions
//! keep working unchanged once the registry is wired in.

use bamboo_domain::subagent::{ModelHint, SubagentProfile, ToolPolicy, UiHint};

/// Return the canonical built-in profiles in stable insertion order.
///
/// Stable ordering matters because the registry exposes `iter()` to UIs and
/// to tool schemas (e.g. `subagent_type` enum values) downstream.
pub fn builtin_profiles() -> Vec<SubagentProfile> {
    vec![
        general_purpose(),
        plan(),
        researcher(),
        coder(),
        reviewer(),
        tester(),
    ]
}

// ---------------------------------------------------------------------------
// Individual profiles
// ---------------------------------------------------------------------------

fn general_purpose() -> SubagentProfile {
    SubagentProfile {
        id: "general-purpose".into(),
        display_name: "General".into(),
        description: "Default child session. Inherits the full child tool set; matches \
             the historical CHILD_SYSTEM_PROMPT behaviour."
            .into(),
        system_prompt: GENERAL_PURPOSE_SYSTEM_PROMPT.into(),
        tools: ToolPolicy::Inherit,
        model_hint: Some(ModelHint {
            tier: Some("sub_agent".into()),
            model_ref: None,
        }),
        default_responsibility: None,
        ui: UiHint {
            icon: Some("🤖".into()),
            color: Some("gray".into()),
        },
    }
}

fn plan() -> SubagentProfile {
    SubagentProfile {
        id: "plan".into(),
        display_name: "Planner".into(),
        description: "Read-only exploration specialist used for planning. \
                      Cannot modify files or run shell commands."
            .into(),
        system_prompt: PLAN_SYSTEM_PROMPT.into(),
        tools: ToolPolicy::Allowlist {
            allow: read_only_tools(),
        },
        model_hint: Some(ModelHint {
            tier: Some("chat".into()),
            model_ref: None,
        }),
        default_responsibility: Some("Investigate the codebase and produce findings".into()),
        ui: UiHint {
            icon: Some("📋".into()),
            color: Some("blue".into()),
        },
    }
}

fn researcher() -> SubagentProfile {
    SubagentProfile {
        id: "researcher".into(),
        display_name: "Researcher".into(),
        description: "Deep research specialist with read-only code access plus \
                      web tools and memory."
            .into(),
        system_prompt: RESEARCHER_SYSTEM_PROMPT.into(),
        tools: ToolPolicy::Allowlist {
            allow: research_tools(),
        },
        model_hint: Some(ModelHint {
            tier: Some("chat".into()),
            model_ref: None,
        }),
        default_responsibility: Some(
            "Research the assigned topic and return a structured report".into(),
        ),
        ui: UiHint {
            icon: Some("🔬".into()),
            color: Some("cyan".into()),
        },
    }
}

fn coder() -> SubagentProfile {
    SubagentProfile {
        id: "coder".into(),
        display_name: "Coder".into(),
        description: "Implementation specialist. May edit files and run \
                      shell commands; cannot spawn further sub-agents."
            .into(),
        system_prompt: CODER_SYSTEM_PROMPT.into(),
        tools: ToolPolicy::Denylist {
            deny: vec!["SubAgent".into(), "scheduler".into()],
        },
        model_hint: Some(ModelHint {
            tier: Some("sub_agent".into()),
            model_ref: None,
        }),
        default_responsibility: Some(
            "Implement the assigned change and provide a diff summary".into(),
        ),
        ui: UiHint {
            icon: Some("💻".into()),
            color: Some("green".into()),
        },
    }
}

fn reviewer() -> SubagentProfile {
    SubagentProfile {
        id: "reviewer".into(),
        display_name: "Reviewer".into(),
        description: "Read-only code reviewer. Inspects diffs and surfaces \
                      issues without modifying code."
            .into(),
        system_prompt: REVIEWER_SYSTEM_PROMPT.into(),
        tools: ToolPolicy::Allowlist {
            allow: read_only_tools(),
        },
        model_hint: Some(ModelHint {
            tier: Some("chat".into()),
            model_ref: None,
        }),
        default_responsibility: Some(
            "Review the recent changes and report findings (issues / approvals)".into(),
        ),
        ui: UiHint {
            icon: Some("🔍".into()),
            color: Some("purple".into()),
        },
    }
}

fn tester() -> SubagentProfile {
    SubagentProfile {
        id: "tester".into(),
        display_name: "Tester".into(),
        description: "Test runner. May execute shell commands to run tests \
                      but cannot modify source files or spawn sub-agents."
            .into(),
        system_prompt: TESTER_SYSTEM_PROMPT.into(),
        tools: ToolPolicy::Denylist {
            deny: vec![
                "SubAgent".into(),
                "scheduler".into(),
                "Edit".into(),
                "Write".into(),
                "NotebookEdit".into(),
            ],
        },
        model_hint: Some(ModelHint {
            tier: Some("sub_agent".into()),
            model_ref: None,
        }),
        default_responsibility: Some(
            "Run the relevant test suites and report pass/fail with evidence".into(),
        ),
        ui: UiHint {
            icon: Some("🧪".into()),
            color: Some("orange".into()),
        },
    }
}

// ---------------------------------------------------------------------------
// Tool sets
// ---------------------------------------------------------------------------

/// Read-only tool set shared by `plan` and `reviewer`.
fn read_only_tools() -> Vec<String> {
    vec![
        "Read".into(),
        "Glob".into(),
        "Grep".into(),
        "GetFileInfo".into(),
        "WebFetch".into(),
        "WebSearch".into(),
        "MemoryNote".into(),
        "memory".into(),
        "session_history".into(),
    ]
}

/// Researcher tool set: read-only + web + memory + skill discovery.
fn research_tools() -> Vec<String> {
    let mut tools = read_only_tools();
    tools.extend([
        "load_skill".into(),
        "read_skill_resource".into(),
        "session_note".into(),
    ]);
    tools
}

// ---------------------------------------------------------------------------
// System prompts
// ---------------------------------------------------------------------------

/// Mirrors `crates/bamboo-server/src/session_app/child_session.rs::CHILD_SYSTEM_PROMPT`.
///
/// Kept literally identical so that sessions resolved to `general-purpose`
/// behave exactly like the pre-registry code path.
pub const GENERAL_PURPOSE_SYSTEM_PROMPT: &str = r#"You are a **Child Session**, delegated by a parent session.

Requirements:
- Focus only on the assigned task and avoid unrelated conversation.
- You may use tools to complete the task.
- Do not create or trigger any additional child sessions (no recursive spawn).
- Keep output concise: provide the conclusion first, then only necessary evidence or steps.
"#;

/// Mirrors `crates/bamboo-server/src/session_app/child_session.rs::PLAN_AGENT_SYSTEM_PROMPT`.
pub const PLAN_SYSTEM_PROMPT: &str = r#"You are a **Plan Agent**, a read-only exploration specialist delegated by a parent session.

Your role is EXCLUSIVELY to explore the codebase and gather information to help design an implementation plan. You MUST NOT modify anything.

=== CRITICAL: READ-ONLY MODE — NO FILE MODIFICATIONS ===

You are FORBIDDEN from using these tools:
- Write — do not create new files
- Edit — do not modify existing files
- NotebookEdit — do not edit notebooks
- Bash — do not execute shell commands
- BashOutput — do not execute shell commands
- KillShell — do not manage processes
- SubAgent — do not spawn further child sessions

You MAY use these read-only tools:
- Read — read file contents
- Glob — list files matching patterns
- Grep — search code for patterns
- GetFileInfo — get file metadata
- WebFetch — fetch web content
- WebSearch — search the web
- MemoryNote — write observations to session memory

Requirements:
- Focus only on the assigned exploration task.
- Provide clear, structured findings: what you discovered, where the relevant code is, and what it does.
- Keep output concise but thorough — the parent session needs enough detail to design a plan.
- If you cannot find something after reasonable searching, say so clearly.
"#;

pub const RESEARCHER_SYSTEM_PROMPT: &str = r#"You are a **Researcher**, a deep-dive investigation specialist delegated by a parent session.

Your job is to gather comprehensive, accurate information on the assigned topic — across the codebase, web sources, and prior memory — and return a structured report.

Capabilities:
- Read-only code access: Read, Glob, Grep, GetFileInfo.
- Web access: WebFetch, WebSearch.
- Memory: session_note, memory, session_history to consult and accumulate findings.

Constraints:
- You MUST NOT modify any files or execute shell commands.
- You MUST NOT spawn further child sessions.
- Cite specific files and line numbers (path:line) when referring to code.

Output format:
1. **Summary** — 2–4 lines on what you found.
2. **Key findings** — bulleted list with evidence (file paths, links, quotes).
3. **Open questions / unknowns** — anything that needs follow-up.
"#;

pub const CODER_SYSTEM_PROMPT: &str = r#"You are a **Coder**, an implementation specialist delegated by a parent session.

Your job is to make the requested code change and prove it works.

Capabilities:
- Full file/edit/shell access on the workspace.
- May run build/test commands to verify your change.

Constraints:
- You MUST NOT spawn further child sessions (no recursive delegation).
- You MUST NOT schedule background jobs.
- Make focused, minimal changes that match the assigned scope.
- Run the smallest meaningful verification (`cargo check`, `npm run test:run`, etc.) before reporting done.

Output format:
1. **Change summary** — what you changed and why.
2. **Affected files** — list with one-line description per file.
3. **Verification** — commands run + their outcomes.
4. **Follow-ups** — anything you noticed but did not change.
"#;

pub const REVIEWER_SYSTEM_PROMPT: &str = r#"You are a **Reviewer**, a read-only code reviewer delegated by a parent session.

Your job is to evaluate a recent change (or a specific file/area) and produce inline-style review feedback.

Capabilities:
- Read-only code access: Read, Glob, Grep, GetFileInfo.
- Web/memory tools for cross-referencing prior decisions.

Constraints:
- You MUST NOT modify files or execute shell commands.
- You MUST NOT spawn further child sessions.
- Keep feedback specific and actionable; cite file_path:line for every comment.

Output format:
1. **Verdict** — approve / request changes / blocking concerns.
2. **Findings** — list of comments grouped by severity (blocker / major / minor / nit), each with `path:line` reference and rationale.
3. **Positive notes** — anything done particularly well.
"#;

pub const TESTER_SYSTEM_PROMPT: &str = r#"You are a **Tester**, a verification specialist delegated by a parent session.

Your job is to run the relevant tests for an assigned change or area and report results.

Capabilities:
- Read-only code access (Read/Glob/Grep/GetFileInfo) to locate suites.
- Shell access (Bash/BashOutput) to execute test runners.

Constraints:
- You MUST NOT modify source files (no Edit / Write / NotebookEdit).
- You MUST NOT spawn further child sessions or schedule jobs.
- Prefer the smallest meaningful suite first; escalate to broader runs only if needed.

Output format:
1. **Suites executed** — exact commands invoked.
2. **Results** — pass / fail counts, links to failing test names.
3. **Triage** — for failures: probable cause and the file:line that needs attention.
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::subagent::DEFAULT_FALLBACK_PROFILE_ID;

    #[test]
    fn returns_six_profiles_in_stable_order() {
        let profiles = builtin_profiles();
        let ids: Vec<&str> = profiles.iter().map(|p| p.id.as_str()).collect();
        assert_eq!(
            ids,
            vec![
                "general-purpose",
                "plan",
                "researcher",
                "coder",
                "reviewer",
                "tester",
            ]
        );
    }

    #[test]
    fn fallback_id_is_present() {
        let profiles = builtin_profiles();
        assert!(profiles.iter().any(|p| p.id == DEFAULT_FALLBACK_PROFILE_ID));
    }

    #[test]
    fn general_purpose_prompt_matches_legacy_constant() {
        // Cross-check against the live constant in child_session.rs to
        // guarantee zero behaviour drift for the fallback profile.
        assert_eq!(
            GENERAL_PURPOSE_SYSTEM_PROMPT,
            crate::session_app::child_session::CHILD_SYSTEM_PROMPT
        );
    }

    #[test]
    fn plan_prompt_matches_legacy_constant() {
        assert_eq!(
            PLAN_SYSTEM_PROMPT,
            crate::session_app::child_session::PLAN_AGENT_SYSTEM_PROMPT
        );
    }

    #[test]
    fn coder_denylist_blocks_subagent() {
        let coder = builtin_profiles()
            .into_iter()
            .find(|p| p.id == "coder")
            .unwrap();
        match coder.tools {
            ToolPolicy::Denylist { deny } => {
                assert!(deny.iter().any(|t| t == "SubAgent"));
            }
            other => panic!("expected denylist, got {other:?}"),
        }
    }

    #[test]
    fn reviewer_uses_readonly_allowlist() {
        let reviewer = builtin_profiles()
            .into_iter()
            .find(|p| p.id == "reviewer")
            .unwrap();
        match reviewer.tools {
            ToolPolicy::Allowlist { allow } => {
                assert!(allow.contains(&"Read".to_string()));
                assert!(!allow.iter().any(|t| t == "Edit" || t == "Write"));
            }
            other => panic!("expected allowlist, got {other:?}"),
        }
    }

    #[test]
    fn every_profile_has_non_empty_system_prompt_and_display_name() {
        for p in builtin_profiles() {
            assert!(!p.system_prompt.is_empty(), "{} system_prompt empty", p.id);
            assert!(!p.display_name.is_empty(), "{} display_name empty", p.id);
        }
    }
}
