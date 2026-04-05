use std::path::{Path, PathBuf};

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
    let bytes = std::fs::read(path).ok()?;
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

fn ancestor_dirs(start: &Path) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    let Some(mut current) = canonical_dir_or_self(start) else {
        return dirs;
    };

    loop {
        dirs.push(current.clone());
        let Some(parent) = current.parent() else {
            break;
        };
        if parent == current {
            break;
        }
        current = parent.to_path_buf();
    }

    dirs
}

pub fn collect_instruction_files(workspace_path: &Path) -> Vec<InstructionFile> {
    let mut files = Vec::new();

    for dir in ancestor_dirs(workspace_path) {
        for file_name in INSTRUCTION_FILE_NAMES {
            let candidate = dir.join(file_name);
            let Some(content) = read_trimmed_file(&candidate) else {
                continue;
            };
            files.push(InstructionFile {
                display_path: crate::core::paths::path_to_display_string(&candidate),
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
        "Repository instruction layer loaded from workspace policy files. Follow these instructions in addition to the base system prompt unless they conflict with higher-priority system/developer directives.".to_string(),
    );

    for file in files {
        sections.push(format!(
            "## {}\nSource: {}\n\n{}",
            file.path
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("INSTRUCTION.md"),
            file.display_path,
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
}
