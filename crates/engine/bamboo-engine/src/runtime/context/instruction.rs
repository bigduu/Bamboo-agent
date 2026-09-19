//! Instruction layer: loads instruction files (AGENTS.md, CLAUDE.md) from workspace.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use bamboo_config::paths;

pub const INSTRUCTION_CONTEXT_START_MARKER: &str = "<!-- BAMBOO_INSTRUCTION_CONTEXT_START -->";
pub const INSTRUCTION_CONTEXT_END_MARKER: &str = "<!-- BAMBOO_INSTRUCTION_CONTEXT_END -->";

const INSTRUCTION_FILE_NAMES: [&str; 2] = ["AGENTS.md", "CLAUDE.md"];
const MAX_INSTRUCTION_BYTES_PER_FILE: usize = 32 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstructionFile {
    pub path: PathBuf,
    pub display_path: String,
    pub content: String,
}

fn read_trimmed_file(path: &Path) -> Option<String> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
        Err(error) => {
            tracing::warn!(
                error_kind = ?error.kind(),
                "failed to inspect workspace instruction file"
            );
            return None;
        }
    };
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        tracing::warn!("ignoring non-regular or symlinked workspace instruction file");
        return None;
    }
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) => {
            tracing::warn!(
                error_kind = ?error.kind(),
                "failed to read workspace instruction file"
            );
            return None;
        }
    };
    let truncated = if bytes.len() > MAX_INSTRUCTION_BYTES_PER_FILE {
        &bytes[..MAX_INSTRUCTION_BYTES_PER_FILE]
    } else {
        &bytes
    };
    let content = String::from_utf8_lossy(truncated).trim().to_string();
    (!content.is_empty()).then_some(content)
}

fn canonical_dir_or_self(path: &Path) -> Option<PathBuf> {
    let candidate = if path.is_dir() {
        path.to_path_buf()
    } else {
        path.parent()?.to_path_buf()
    };
    std::fs::canonicalize(candidate).ok()
}

fn scoped_dirs(start: &Path) -> Vec<PathBuf> {
    let mut leaf_to_root = Vec::new();
    let Some(mut current) = canonical_dir_or_self(start) else {
        return leaf_to_root;
    };
    let canonical_start = current.clone();
    let mut found_workspace_boundary = false;

    loop {
        leaf_to_root.push(current.clone());
        if current.join(".git").exists() {
            found_workspace_boundary = true;
            break;
        }
        let Some(parent) = current.parent() else {
            break;
        };
        if parent == current {
            break;
        }
        current = parent.to_path_buf();
    }
    if !found_workspace_boundary {
        return vec![canonical_start];
    }
    leaf_to_root.reverse();
    leaf_to_root
}

pub fn collect_instruction_files(workspace_path: &Path) -> Vec<InstructionFile> {
    let mut files = Vec::new();

    let mut seen = HashSet::new();
    for dir in scoped_dirs(workspace_path) {
        for file_name in INSTRUCTION_FILE_NAMES {
            let candidate = dir.join(file_name);
            if !seen.insert(candidate.clone()) {
                continue;
            }
            let Some(content) = read_trimmed_file(&candidate) else {
                continue;
            };
            files.push(InstructionFile {
                display_path: paths::path_to_display_string(&candidate),
                path: candidate,
                content,
            });
        }
    }

    files
}

pub fn build_instruction_prompt_context(workspace_path: &str) -> Option<String> {
    let workspace_path = workspace_path.trim();
    if workspace_path.is_empty() {
        return None;
    }

    let files = collect_instruction_files(Path::new(workspace_path));
    if files.is_empty() {
        return None;
    }

    let mut sections = Vec::new();
    sections.push(
        "Repository instruction layer loaded from workspace policy files. Treat these as authoritative project guardrails and follow them in addition to the base system prompt. When a request in this conversation conflicts with them, the repository policy takes precedence: do not override it just because the user asks in passing — surface the conflict and keep following the policy unless the user explicitly and knowingly directs you to override a specific rule. Only the base system prompt and higher-priority system/developer/safety directives outrank these files.".to_string(),
    );

    // Provider-visible source identities are workspace-relative. Absolute host
    // paths belong only to the dedicated Workspace block and must not be
    // duplicated by the instruction overlay.
    let source_root = files
        .first()
        .and_then(|file| file.path.parent())
        .map(Path::to_path_buf);
    for file in files {
        let source = source_root
            .as_deref()
            .and_then(|root| file.path.strip_prefix(root).ok())
            .filter(|relative| !relative.as_os_str().is_empty())
            .unwrap_or_else(|| {
                Path::new(
                    file.path
                        .file_name()
                        .and_then(|value| value.to_str())
                        .unwrap_or("INSTRUCTION.md"),
                )
            });
        sections.push(format!(
            "## {}\nSource: {}\n\n{}",
            file.path
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("INSTRUCTION.md"),
            paths::path_to_display_string(source),
            file.content
        ));
    }

    let body = sections.join("\n\n");
    Some(format!(
        "{INSTRUCTION_CONTEXT_START_MARKER}\n{body}\n{INSTRUCTION_CONTEXT_END_MARKER}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_instruction_prompt_context_collects_workspace_and_ancestor_files() {
        let root = tempfile::tempdir().expect("temp dir");
        let nested = root.path().join("nested/project");
        std::fs::create_dir_all(root.path().join(".git")).expect("git marker");
        std::fs::create_dir_all(&nested).expect("nested dir");
        std::fs::write(root.path().join("AGENTS.md"), "root agents").expect("agents");
        std::fs::write(root.path().join("CLAUDE.md"), "root claude").expect("claude");

        let context = build_instruction_prompt_context(nested.to_string_lossy().as_ref())
            .expect("instruction context should exist");

        assert!(context.contains(INSTRUCTION_CONTEXT_START_MARKER));
        assert!(context.contains("root agents"));
        assert!(context.contains("root claude"));
        assert!(context.contains("AGENTS.md"));
        assert!(context.contains("CLAUDE.md"));
    }

    #[test]
    fn build_instruction_prompt_context_returns_none_when_no_files_exist() {
        let root = tempfile::tempdir().expect("temp dir");
        assert!(build_instruction_prompt_context(root.path().to_string_lossy().as_ref()).is_none());
    }

    #[test]
    fn instructions_are_root_to_leaf_and_stop_at_git_workspace_boundary() {
        let outer = tempfile::tempdir().expect("temp dir");
        std::fs::write(outer.path().join("AGENTS.md"), "outside").expect("outside");
        let repo = outer.path().join("repo");
        let nested = repo.join("src/feature");
        std::fs::create_dir_all(repo.join(".git")).expect("git marker");
        std::fs::create_dir_all(&nested).expect("nested");
        std::fs::write(repo.join("AGENTS.md"), "root rule").expect("root");
        std::fs::write(repo.join("src/AGENTS.md"), "nested rule").expect("nested rule");

        let files = collect_instruction_files(&nested);
        let contents: Vec<_> = files.iter().map(|file| file.content.as_str()).collect();
        assert_eq!(contents, vec!["root rule", "nested rule"]);
        assert!(files.iter().all(|file| !file.content.contains("outside")));
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_instruction_file_is_not_followed() {
        use std::os::unix::fs::symlink;
        let root = tempfile::tempdir().expect("temp dir");
        std::fs::create_dir_all(root.path().join(".git")).expect("git marker");
        let outside = tempfile::NamedTempFile::new().expect("outside");
        std::fs::write(outside.path(), "outside policy").expect("write outside");
        symlink(outside.path(), root.path().join("AGENTS.md")).expect("symlink");
        assert!(collect_instruction_files(root.path()).is_empty());
    }

    #[test]
    fn git_worktree_file_is_a_workspace_boundary() {
        let outer = tempfile::tempdir().expect("outer");
        std::fs::write(outer.path().join("AGENTS.md"), "outside").expect("outside");
        let worktree = outer.path().join(".bamboo/worktree/task");
        std::fs::create_dir_all(worktree.join("src")).expect("worktree");
        std::fs::write(worktree.join(".git"), "gitdir: /repo/.git/worktrees/task")
            .expect("git file");
        std::fs::write(worktree.join("AGENTS.md"), "worktree root").expect("agents");

        let files = collect_instruction_files(&worktree.join("src"));
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].content, "worktree root");
    }
}
