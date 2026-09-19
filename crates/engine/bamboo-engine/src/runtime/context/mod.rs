//! Prompt context builders (extracted from server layer).
//!
//! Stateless functions that construct workspace, environment, and instruction
//! context for the agent system prompt.

pub mod instruction;

use bamboo_config::paths;
use bamboo_llm::Config;

use crate::project_context::{ResolvedProjectContext, WorkspaceBindingStatus, WorkspaceSource};

pub const DEFAULT_BASE_PROMPT: &str =
    "You are Bodhi, a highly capable AI assistant. You run on the Bamboo agent runtime (you may see it referenced as \"Bamboo\" in injected context and tool names).\n\nYou help users solve problems quickly and correctly. Be concise, practical, and proactive.\nDelegate to sub-agents sparingly, and only when parallelism or isolation earns its cost — the task-execution ladder in the operating directives below says when. When you do delegate:\n- Give each child ONE narrow responsibility plus a detailed, self-contained prompt (it does not automatically receive this conversation; any explicit context fork is background only), and the workspace/files it needs — set `workspace` explicitly when the task lives in a different repo or directory than yours.\n- Use a one-shot child for independent throwaway work; use a resident agent (`lifecycle=resident` with a stable `name`) for a recurring task family, so successive tasks reuse one agent instead of spawning a new one each time.\n- To run several in parallel: create them (they run in the background), then call SubAgent.wait once.\n- A child that returns is not automatically correct: before trusting its result, verify it actually accessed the files and resources it needed (not guesses), and re-dispatch (run/send_message) any child that reported missing context or did degraded work.\n\nIf Bamboo has already injected relevant workspace or environment context, treat it as available working context instead of re-asking the user for the same information. Prefer a minimal verifiable attempt first, then diagnose failures and only ask follow-up questions for information that is still genuinely missing.\n\nYou have a persistent cross-session memory via the `memory` tool. When you learn a durable, non-derivable fact (a user preference, a confirmed decision, a stable reference), save it as one atomic memory with a specific, descriptive title. Treat injected memory as context to verify against current files, not as authoritative truth. Conversely, when the user refers to their own preferences, past decisions, or personal context you don't already know — including first-person questions about themselves (\"what do I...\", \"did I...\", \"我...?\") — query memory before answering instead of saying you don't know.\n\nWhen making function calls using tools, always include a brief text explanation before or alongside the tool calls describing what you are about to do and why. Never silently call tools without any visible narration to the user.";

/// Framework-invariant agent operating directives, applied on top of whatever
/// base prompt is in effect.
///
/// Unlike [`DEFAULT_BASE_PROMPT`] — which a user can fully replace via
/// `${BAMBOO_DATA_DIR}/system-prompt.md` — these directives are appended during
/// per-round prompt assembly (see `append_core_agent_directives`), so they
/// always apply even when the base prompt is overridden. Keep the text static:
/// it rides in the cacheable system field, so churn here forces a cache re-warm.
pub const CORE_AGENT_DIRECTIVES: &str = r#"Investigate before you conclude. When a request concerns a codebase (you have a workspace, or the question is about how something is built or behaves), gather enough grounding context before answering — never conclude from a README, a doc, or a single file alone. READMEs, docs, and comments state intent and can be stale or partial; read the relevant source and trace how the pieces actually connect (entry points, call sites, data flow) until the picture is consistent, and deliberately weigh more than one explanation rather than committing to the first plausible one. Treat the user's own account as a hypothesis to verify, not as ground truth: their mental model can lag the code — they may be recalling an older implementation that has since been replaced — so when their description and the current code disagree, trust the verified code and surface the gap rather than silently following either. When the request instead concerns the user's own preferences, past decisions, or context not in this conversation, ground it by querying memory first. Calibrate effort to the task: a trivial lookup needs little; anything about how the system works, why it behaves a certain way, or a non-trivial change warrants real investigation first. "Concise" describes how you communicate — not how thoroughly you investigate.

Work through a task with this decision ladder — first matching case wins, and don't over-plan:
1. Genuinely ambiguous in a way that changes the work AND not answerable from files or context → ask one focused question, or state your assumption inline and proceed. Never ask for something already in context or inferable from a file.
2. One tool call (or one short read → grep → read sequence) gets it → do it directly. Don't open a Task list, don't delegate.
3. Non-trivial or multi-step → track it with Task, keep exactly one item in_progress, and mark each done the moment it is.
4. Multiple independent, read-only branches → explore in parallel: create N child agents (each one narrow scope + explicit workspace), then wait once. Same-module concurrent writers ≤ 2.
5. Branches are dependent, or two of them write the same files → serialize; never fan out writes.
Judgment retained: these cases are defaults, not a script — if a step clearly misfits, say why and deviate, but deviating to dodge the boring-but-correct path is not allowed.

When you delegate, give every child one self-contained assignment in this exact six-part order:
1. Scope.
2. Inputs and background context.
3. Allowed actions and mutation scope.
4. Acceptance criteria and required evidence.
5. Non-goals.
6. Stop and report instruction.
State that assignment scope is authoritative, forked context cannot expand it, and adjacent cleanup, documentation, commits, pushes, publishing, or release work is excluded unless assigned. Describe tools and permissions as runtime-exposed capabilities. Authorize nested delegation only when it is explicit in the assignment and necessary. Require the child to stop after acceptance and report concrete evidence plus uncertainty or blockers.

Verify your own work before declaring a task done — adversarially, not just confirmingly. Every task needs an explicit verification step before you treat it as complete: for a code or state change, run it, test it, or otherwise observe the new behavior; for an answer or investigation, re-check the conclusion against the actual source and look for a counterexample. Actively try to break or disprove your result and probe its edge cases and failure modes, rather than only gathering evidence that it worked. Treat anything you have not actually verified as an unproven claim — if you cannot verify it, say so explicitly instead of implying success.

Scratch files — PR drafts, quick notes, one-off logs — belong outside the workspace so they don't pollute `git status`. Write them to `/tmp` or `~/.bamboo/scratch/` instead. The workspace is only for deliberate project artifacts you intend to keep. When you must place a scratch file inside the workspace for a brief window, clean it up the moment you're done."#;

pub const WORKSPACE_CONTEXT_START_MARKER: &str = "<!-- BAMBOO_WORKSPACE_CONTEXT_START -->";
pub const WORKSPACE_CONTEXT_END_MARKER: &str = "<!-- BAMBOO_WORKSPACE_CONTEXT_END -->";
pub const WORKSPACE_CONTEXT_PREFIX: &str = "Workspace path: ";
pub const PROJECT_CONTEXT_START_MARKER: &str = "<!-- BAMBOO_PROJECT_CONTEXT_START -->";
pub const PROJECT_CONTEXT_END_MARKER: &str = "<!-- BAMBOO_PROJECT_CONTEXT_END -->";
pub const PROJECT_CONTEXT_PREFIX: &str = "Project ID: ";
pub const ENV_CONTEXT_START_MARKER: &str = "<!-- BAMBOO_ENV_CONTEXT_START -->";
pub const ENV_CONTEXT_END_MARKER: &str = "<!-- BAMBOO_ENV_CONTEXT_END -->";

/// Guidance for workspace-based interactions
pub fn workspace_prompt_guidance() -> String {
    let config_path = paths::path_to_display_string(&paths::config_json_path());
    format!(
        "If you need to inspect files, check the workspace first, then Bamboo data at {}. Bamboo configuration is stored in {} (equivalent to ${{BAMBOO_DATA_DIR}}/config.json).",
        paths::bamboo_dir_display(),
        config_path
    )
}

fn build_env_prompt_guidance() -> Option<String> {
    let env_vars = Config::current_prompt_safe_env_vars();
    if env_vars.is_empty() {
        return None;
    }

    let mut lines = vec![
        "These environment variables were explicitly configured by the user inside Bodhi."
            .to_string(),
        "- They are already available to Bash/tool processes launched by Bodhi and may be relevant to tools and skills."
            .to_string(),
        "- Treat them as user-approved runtime context instead of asking the user to repeat them immediately."
            .to_string(),
        "- Secret values are intentionally hidden from the model.".to_string(),
        "- If the listed variables appear sufficient, prefer a minimal verification or execution attempt before asking follow-up questions."
            .to_string(),
        "- Only ask the user for additional env details after identifying a concrete missing variable, malformed value shape, or execution failure that cannot be resolved from this injected context."
            .to_string(),
    ];

    for entry in env_vars {
        let visibility = if entry.secret { "secret" } else { "non-secret" };
        let mut line = format!("- {} ({})", entry.name, visibility);
        if let Some(description) = entry.description {
            line.push_str(" — ");
            line.push_str(&description);
        }
        lines.push(line);
    }

    Some(lines.join("\n"))
}

pub fn build_env_prompt_context() -> Option<String> {
    let body = build_env_prompt_guidance()?;
    Some(format!(
        "{ENV_CONTEXT_START_MARKER}\n{body}\n{ENV_CONTEXT_END_MARKER}"
    ))
}

pub fn build_workspace_prompt_context(workspace_path: &str) -> Option<String> {
    build_workspace_prompt_context_with_binding(
        workspace_path,
        WorkspaceBindingStatus::Unregistered,
    )
}

pub fn build_workspace_prompt_context_with_binding(
    workspace_path: &str,
    binding_status: WorkspaceBindingStatus,
) -> Option<String> {
    build_workspace_prompt_context_with_binding_and_source(workspace_path, binding_status, None)
}

pub fn build_workspace_prompt_context_with_binding_and_source(
    workspace_path: &str,
    binding_status: WorkspaceBindingStatus,
    source: Option<WorkspaceSource>,
) -> Option<String> {
    let workspace_path = workspace_path.trim();
    if workspace_path.is_empty() {
        return None;
    }

    let body = format!(
        "{WORKSPACE_CONTEXT_PREFIX}{}\nWorkspace source: {}\nBinding status: {}\nWorkspace-local resources may override Project-shared resources.\nChanging the workspace changes only the filesystem execution context; it does not change Project membership or Project memory.\n{}",
        prompt_safe_scalar(workspace_path),
        source.unwrap_or(WorkspaceSource::Session).as_str(),
        binding_status.as_str(),
        workspace_prompt_guidance()
    );

    Some(format!(
        "{WORKSPACE_CONTEXT_START_MARKER}\n{body}\n{WORKSPACE_CONTEXT_END_MARKER}"
    ))
}

/// Locate a complete unwrapped Workspace block emitted by legacy Bamboo builds.
///
/// Some persisted sessions predate the marker wrapper, but an ordinary custom
/// System prompt is also allowed to discuss a `Workspace path:`. Treat the
/// prefix as host authority only when it appears at the start of a line and is
/// followed by the exact generated guidance, with only known generated
/// metadata lines in between. Otherwise migration must leave the text alone.
pub(crate) fn legacy_unwrapped_workspace_context_bounds(prompt: &str) -> Option<(usize, usize)> {
    let guidance = workspace_prompt_guidance();

    for (start_idx, _) in prompt.match_indices(WORKSPACE_CONTEXT_PREFIX) {
        if start_idx > 0 && prompt.as_bytes()[start_idx - 1] != b'\n' {
            continue;
        }

        let path_start = start_idx + WORKSPACE_CONTEXT_PREFIX.len();
        let Some(path_end_rel) = prompt[path_start..].find('\n') else {
            continue;
        };
        let path_end = path_start + path_end_rel;
        if prompt[path_start..path_end].trim().is_empty() {
            continue;
        }

        let metadata_start = path_end + 1;
        let Some(guidance_rel) = prompt[metadata_start..].find(&guidance) else {
            continue;
        };
        let guidance_start = metadata_start + guidance_rel;
        if guidance_start > 0 && prompt.as_bytes()[guidance_start - 1] != b'\n' {
            continue;
        }

        let metadata_is_generated = prompt[metadata_start..guidance_start]
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .all(|line| {
                matches!(
                    line,
                    "Workspace source: explicit"
                        | "Workspace source: project_default"
                        | "Workspace source: session"
                        | "Binding status: registered"
                        | "Binding status: unregistered"
                        | "Workspace-local resources may override Project-shared resources."
                        | "Changing the workspace changes only the filesystem execution context; it does not change Project membership or Project memory."
                )
            });
        if !metadata_is_generated {
            continue;
        }

        return Some((start_idx, guidance_start + guidance.len()));
    }

    None
}

/// Build the stable Project identity block.
///
/// Resource counts and revisions are intentionally excluded: they belong to
/// the per-round dynamic resource envelope. Secret settings, credential
/// values, MCP headers, and environment values do not belong in
/// [`ResolvedProjectContext`] and therefore cannot leak through this builder.
pub fn build_project_prompt_context(context: &ResolvedProjectContext) -> String {
    let project = &context.project;
    let project_path = project
        .project_path
        .as_deref()
        .map(paths::path_to_display_string)
        .unwrap_or_else(|| "not configured".to_string());
    let body = format!(
        "{PROJECT_CONTEXT_PREFIX}{}\nProject name: {}\nProject path: {}\nProject home (Bamboo data): {}\nThis session belongs to this Project.\nWorkspace is mutable execution context; changing it does not change Project membership, sidebar grouping, Project memory, or Project-shared resources.\nProject-shared resource inventory is supplied separately as per-round dynamic context.\nUse Workspace to inspect/change only the current directory.\nUse Project to inspect Project identity, bindings, and shared resources.",
        prompt_safe_scalar(project.id.as_str()),
        prompt_safe_scalar(&project.name),
        prompt_safe_scalar(&project_path),
        prompt_safe_scalar(&paths::path_to_display_string(&project.home)),
    );
    format!("{PROJECT_CONTEXT_START_MARKER}\n{body}\n{PROJECT_CONTEXT_END_MARKER}")
}

/// Build provider-visible Project identity without filesystem locations.
///
/// The active workspace path is rendered exactly once by the sibling Workspace
/// context. Project roots and Bamboo-owned data paths are host details and must
/// not duplicate that path or leak into provider-visible diagnostics.
pub fn build_project_model_context(context: &ResolvedProjectContext) -> String {
    let project = &context.project;
    let body = format!(
        "{PROJECT_CONTEXT_PREFIX}{}\nProject name: {}\nThis session belongs to this Project.\nWorkspace is mutable execution context; changing it does not change Project membership, sidebar grouping, Project memory, or Project-shared resources.\nProject-shared resource inventory is supplied separately as per-round dynamic context.\nThe host owns workspace selection and reports the active execution context separately.",
        prompt_safe_scalar(project.id.as_str()),
        prompt_safe_scalar(&project.name),
    );
    format!("{PROJECT_CONTEXT_START_MARKER}\n{body}\n{PROJECT_CONTEXT_END_MARKER}")
}

/// Replace every existing Project block with exactly one current block.
///
/// Workspace blocks are deliberately outside the removed marker range.
pub fn upsert_project_prompt_context(
    prompt: &str,
    context: Option<&ResolvedProjectContext>,
) -> String {
    replace_prompt_block(
        prompt,
        PROJECT_CONTEXT_START_MARKER,
        PROJECT_CONTEXT_END_MARKER,
        context.map(build_project_prompt_context).as_deref(),
    )
}

/// Replace every existing Workspace block with exactly one current block.
///
/// Project identity is left byte-for-byte unchanged.
pub fn upsert_workspace_prompt_context(
    prompt: &str,
    workspace_path: Option<&str>,
    binding_status: WorkspaceBindingStatus,
) -> String {
    upsert_workspace_prompt_context_with_source(prompt, workspace_path, binding_status, None)
}

pub fn upsert_workspace_prompt_context_with_source(
    prompt: &str,
    workspace_path: Option<&str>,
    binding_status: WorkspaceBindingStatus,
    source: Option<WorkspaceSource>,
) -> String {
    let block = workspace_path.and_then(|workspace| {
        build_workspace_prompt_context_with_binding_and_source(workspace, binding_status, source)
    });
    replace_prompt_block(
        prompt,
        WORKSPACE_CONTEXT_START_MARKER,
        WORKSPACE_CONTEXT_END_MARKER,
        block.as_deref(),
    )
}

fn prompt_safe_scalar(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>()
        .replace("<!--", "< !--")
        .trim()
        .to_string()
}

fn replace_prompt_block(
    prompt: &str,
    start_marker: &str,
    end_marker: &str,
    replacement: Option<&str>,
) -> String {
    let mut current = prompt.to_string();
    while let Some(start) = current.find(start_marker) {
        let content_start = start + start_marker.len();
        let Some(relative_end) = current[content_start..].find(end_marker) else {
            current.truncate(start);
            break;
        };
        let end = content_start + relative_end + end_marker.len();
        let before = current[..start].trim_end();
        let after = current[end..].trim_start();
        current = match (before.is_empty(), after.is_empty()) {
            (true, true) => String::new(),
            (true, false) => after.to_string(),
            (false, true) => before.to_string(),
            (false, false) => format!("{before}\n\n{after}"),
        };
    }

    if let Some(replacement) = replacement.map(str::trim).filter(|value| !value.is_empty()) {
        if !current.trim().is_empty() {
            current = current.trim().to_string();
            current.push_str("\n\n");
        }
        current.push_str(replacement);
    }
    current
}

/// Assemble a full system prompt from base prompt, optional enhancement, and context segments.
///
/// This is the shared prompt assembly logic used by both the HTTP handler and the schedule
/// manager. System text contains only the caller-owned base plus optional
/// enhancement. Project, Workspace, instruction, and environment context are
/// assembled later as typed model-context blocks.
pub fn assemble_system_prompt(
    base: &str,
    enhance: Option<&str>,
    _workspace_path: Option<&str>,
) -> String {
    assemble_system_prompt_with_project(base, enhance, None, None)
}

pub fn assemble_system_prompt_with_project(
    base: &str,
    enhance: Option<&str>,
    _project_context: Option<&ResolvedProjectContext>,
    _workspace_path: Option<&str>,
) -> String {
    let mut prompt = base.trim().to_string();
    if let Some(extra) = enhance.map(str::trim).filter(|v| !v.is_empty()) {
        if !prompt.is_empty() {
            prompt.push_str("\n\n");
        }
        prompt.push_str(extra);
    }
    prompt
}

#[cfg(test)]
mod project_context_tests {
    use std::path::PathBuf;

    use crate::project_context::{
        ProjectDescriptor, ResolvedProjectContext, WorkspaceBindingStatus,
    };
    use bamboo_domain::{
        ProjectId, ProjectResourceEntry, ProjectResourceKind, ProjectResourceSummary,
        WorkspaceBinding,
    };

    use super::*;

    fn project_context(workspace: &str) -> ResolvedProjectContext {
        let project_id = ProjectId::parse("01JPROJECT00000000000000000").expect("project id");
        ResolvedProjectContext {
            project: ProjectDescriptor {
                id: project_id.clone(),
                name: "Zenith".to_string(),
                project_path: Some(PathBuf::from(workspace)),
                home: PathBuf::from("/data/projects/01JPROJECT00000000000000000"),
                workspace_bindings: vec![WorkspaceBinding {
                    path: workspace.to_string(),
                    label: Some("main".to_string()),
                    git_common_dir: None,
                }],
                resources: ProjectResourceSummary {
                    project_id,
                    resource_revision: 9,
                    resources: vec![
                        ProjectResourceEntry {
                            kind: ProjectResourceKind::Memory,
                            present: true,
                            item_count: 1,
                        },
                        ProjectResourceEntry {
                            kind: ProjectResourceKind::Skills,
                            present: true,
                            item_count: 2,
                        },
                        ProjectResourceEntry {
                            kind: ProjectResourceKind::Commands,
                            present: true,
                            item_count: 1,
                        },
                    ],
                },
            },
            workspace: Some(PathBuf::from(workspace)),
            workspace_source: WorkspaceSource::Session,
            binding_status: WorkspaceBindingStatus::Registered,
        }
    }

    #[test]
    fn system_assembly_ignores_legacy_dynamic_context_arguments() {
        let context = project_context("/workspace/private");
        let prompt = assemble_system_prompt_with_project(
            "base",
            None,
            Some(&context),
            Some("/workspace/private"),
        );
        assert_eq!(prompt, "base");
        assert!(!prompt.contains("/workspace/private"));
        assert!(!prompt.contains("/data/projects"));
        assert!(!prompt.contains(PROJECT_CONTEXT_START_MARKER));
        assert!(!prompt.contains(WORKSPACE_CONTEXT_START_MARKER));
        assert!(!prompt.contains(WORKSPACE_CONTEXT_END_MARKER));
        assert!(!prompt.contains(ENV_CONTEXT_START_MARKER));
    }

    #[test]
    fn provider_project_context_contains_no_host_paths() {
        let context = project_context("/workspace/main");
        let prompt = build_project_model_context(&context);
        assert!(prompt.contains("Project ID:"));
        assert!(!prompt.contains("/workspace/main"));
        assert!(!prompt.contains("/data/projects"));
    }

    #[test]
    fn workspace_upsert_preserves_project_block_byte_for_byte() {
        let context = project_context("/workspace/main");
        let project_block = build_project_prompt_context(&context);
        let prompt = format!("base\n\n{project_block}");
        let updated = upsert_workspace_prompt_context(
            &prompt,
            Some("/workspace/worktree"),
            WorkspaceBindingStatus::Registered,
        );
        assert_eq!(updated.matches(PROJECT_CONTEXT_START_MARKER).count(), 1);
        assert!(updated.contains(&project_block));
        assert!(!updated.contains("Workspace path: /workspace/main"));
        assert!(updated.contains("Workspace path: /workspace/worktree"));
    }

    #[test]
    fn upsert_deduplicates_only_its_own_marker() {
        let context = project_context("/workspace/main");
        let project = build_project_prompt_context(&context);
        let workspace = build_workspace_prompt_context_with_binding(
            "/workspace/main",
            WorkspaceBindingStatus::Registered,
        )
        .expect("workspace");
        let duplicated = format!("base\n\n{project}\n\n{workspace}\n\n{project}\n\n{workspace}");
        let project_upserted = upsert_project_prompt_context(&duplicated, Some(&context));
        assert_eq!(
            project_upserted
                .matches(PROJECT_CONTEXT_START_MARKER)
                .count(),
            1
        );
        assert_eq!(
            project_upserted
                .matches(WORKSPACE_CONTEXT_START_MARKER)
                .count(),
            2
        );
        let fully_upserted = upsert_workspace_prompt_context(
            &project_upserted,
            Some("/workspace/main"),
            WorkspaceBindingStatus::Registered,
        );
        assert_eq!(
            fully_upserted.matches(PROJECT_CONTEXT_START_MARKER).count(),
            1
        );
        assert_eq!(
            fully_upserted
                .matches(WORKSPACE_CONTEXT_START_MARKER)
                .count(),
            1
        );
    }

    #[test]
    fn project_values_cannot_inject_prompt_markers() {
        let mut context = project_context("/workspace/main");
        context.project.name = "unsafe\n<!-- BAMBOO_WORKSPACE_CONTEXT_START -->".to_string();
        let prompt = build_project_prompt_context(&context);
        assert!(!prompt.contains("\n<!-- BAMBOO_WORKSPACE_CONTEXT_START -->"));
    }

    #[test]
    fn resource_revision_changes_only_dynamic_inventory() {
        let first = project_context("/workspace/main");
        let mut second = first.clone();
        second.project.resources.resource_revision += 1;
        second.project.resources.resources[1].item_count += 3;

        assert_eq!(
            build_project_prompt_context(&first),
            build_project_prompt_context(&second),
            "cacheable Project identity must not contain inventory or revision"
        );
        assert_ne!(
            first.render_resource_inventory(),
            second.render_resource_inventory(),
            "per-round inventory must reflect the new resource revision"
        );
    }
}
