//! Pure helper functions for skill resource access.
//!
//! Path normalization, text truncation/paging, and file listing
//! utilities used by the skill runtime tool implementations.

use std::path::{Component, Path, PathBuf};

/// Normalize and validate a relative resource path.
///
/// Rejects empty paths, absolute paths, and paths containing `..` or
/// root/prefix segments. Returns the cleaned `PathBuf`.
pub fn normalize_relative_resource_path(raw: &str) -> Result<PathBuf, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("resource_path must be a non-empty relative path".to_string());
    }

    let path = Path::new(trimmed);
    if path.is_absolute() {
        return Err("resource_path must be relative, absolute paths are not allowed".to_string());
    }

    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(segment) => normalized.push(segment),
            Component::CurDir => {}
            Component::ParentDir | Component::Prefix(_) | Component::RootDir => {
                return Err("resource_path cannot contain '..' or root/prefix segments".to_string())
            }
        }
    }

    if normalized.as_os_str().is_empty() {
        return Err("resource_path must resolve to a file path".to_string());
    }

    Ok(normalized)
}

/// Truncate text to a maximum number of characters, respecting char boundaries.
///
/// Returns `(truncated_text, was_truncated)`.
pub fn truncate_text(content: &str, max_chars: usize) -> (&str, bool) {
    if max_chars == 0 {
        return ("", !content.is_empty());
    }

    let boundary = content
        .char_indices()
        .nth(max_chars)
        .map(|(index, _)| index);

    if let Some(index) = boundary {
        return (&content[..index], true);
    }

    (content, false)
}

/// Page through text lines with offset and optional limit.
///
/// Returns `(paged_text, start_line, end_line, total_lines)`.
pub fn page_text_lines(
    content: &str,
    offset: usize,
    limit: Option<usize>,
) -> (String, usize, usize, usize) {
    let lines: Vec<&str> = content.lines().collect();
    let total = lines.len();
    let start = offset.min(total);
    let end = limit
        .map(|value| start.saturating_add(value).min(total))
        .unwrap_or(total);
    let paged = lines[start..end].join("\n");
    (paged, start, end, total)
}

/// Display a path using forward slashes, stripping prefix/root components.
pub fn display_relative_path(path: &Path) -> String {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(segment) => Some(segment.to_string_lossy().into_owned()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// List all resource file paths under a skill root directory.
///
/// Skips `SKILL.md` (the skill definition) and non-file entries.
/// Returns sorted, deduplicated relative paths.
pub fn list_skill_resource_paths(skill_root: &Path) -> std::io::Result<Vec<String>> {
    list_skill_resource_paths_bounded(skill_root, usize::MAX, usize::MAX)
}

pub fn list_skill_resource_paths_bounded(
    skill_root: &Path,
    max_resources: usize,
    max_relative_path_bytes: usize,
) -> std::io::Result<Vec<String>> {
    if !skill_root.exists() {
        return Ok(Vec::new());
    }

    let mut resources = Vec::new();
    for entry in walkdir::WalkDir::new(skill_root)
        .min_depth(1)
        .into_iter()
        .filter_map(Result::ok)
    {
        if !entry.file_type().is_file() {
            continue;
        }

        let Ok(relative) = entry.path().strip_prefix(skill_root) else {
            continue;
        };
        if relative == Path::new("SKILL.md") {
            continue;
        }

        let displayed = display_relative_path(relative);
        if displayed.len() > max_relative_path_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "workflow resource relative path exceeds limit",
            ));
        }
        if resources.len() >= max_resources {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "workflow resource count exceeds limit",
            ));
        }
        resources.push(displayed);
    }

    resources.sort();
    resources.dedup();
    Ok(resources)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn normalize_relative_resource_path_rejects_invalid_paths() {
        assert!(normalize_relative_resource_path("").is_err());
        assert!(normalize_relative_resource_path("../secrets.txt").is_err());
        assert!(normalize_relative_resource_path("/tmp/test.txt").is_err());
    }

    #[test]
    fn normalize_relative_resource_path_accepts_nested_file() {
        let path =
            normalize_relative_resource_path("references/policy.md").expect("path should parse");
        assert_eq!(path, Path::new("references/policy.md"));
    }

    #[test]
    fn truncate_text_reports_truncation() {
        let (text, truncated) = truncate_text("abcde", 3);
        assert_eq!(text, "abc");
        assert!(truncated);
    }

    #[test]
    fn truncate_text_keeps_short_text() {
        let (text, truncated) = truncate_text("abc", 10);
        assert_eq!(text, "abc");
        assert!(!truncated);
    }

    #[test]
    fn page_text_lines_respects_offset_and_limit() {
        let (text, start, end, total) = page_text_lines("a\nb\nc\n", 1, Some(1));
        assert_eq!(text, "b");
        assert_eq!(start, 1);
        assert_eq!(end, 2);
        assert_eq!(total, 3);
    }

    #[test]
    fn bounded_resource_listing_rejects_count_and_path_limits_while_streaming() {
        let directory = tempfile::tempdir().expect("tempdir");
        std::fs::write(directory.path().join("one.txt"), "one").expect("one");
        std::fs::write(directory.path().join("two.txt"), "two").expect("two");
        assert!(list_skill_resource_paths_bounded(directory.path(), 1, 1024).is_err());
        assert!(list_skill_resource_paths_bounded(directory.path(), 2, 3).is_err());
    }
}
