use ignore::{Walk, WalkBuilder};
use std::path::Path;

const SKIP_DIRS: [&str; 8] = [
    ".git",
    "node_modules",
    "target",
    "dist",
    "build",
    ".next",
    ".cache",
    "coverage",
];

/// Shared directory-discovery policy for Grep and Glob.
///
/// Repository `.gitignore` rules apply, including ancestors up to the Git root.
/// Outside repositories, discovery keeps its previous behavior. Hidden files
/// remain discoverable; machine-specific global ignores, `.ignore` files and
/// `.git/info/exclude` do not affect results. Explicit Grep files bypass this
/// walker. Ignore matching is a discovery filter, not a permission check.
pub(super) fn walk(base: &Path, include_ignored: bool) -> Walk {
    let mut builder = WalkBuilder::new(base);
    // ignore exempts the explicitly supplied root from filter_entry. Keep the
    // previous fixed exclusions even when one of those directories is the root.
    if should_skip_dir(base) {
        builder.max_depth(Some(0));
    }
    builder
        .follow_links(false)
        .hidden(false)
        .parents(!include_ignored)
        .ignore(false)
        .git_ignore(!include_ignored)
        .git_global(false)
        .git_exclude(false)
        .require_git(true)
        .filter_entry(|entry| {
            !entry.file_type().is_some_and(|kind| kind.is_dir()) || !should_skip_dir(entry.path())
        });
    builder.build()
}

fn should_skip_dir(path: &Path) -> bool {
    if path.file_name().and_then(|name| name.to_str()) == Some("worktree")
        && path
            .parent()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            == Some(".bamboo")
    {
        return true;
    }
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| SKIP_DIRS.contains(&name))
}

#[cfg(test)]
mod tests {
    use super::super::{GlobTool, GrepTool};
    use bamboo_agent_core::{Tool, ToolCtx, ToolOutcome};
    use serde_json::json;
    use std::collections::BTreeSet;
    use std::path::Path;

    fn write(root: &Path, relative: &str, content: &str) {
        let path = root.join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }

    async fn search(tool: &dyn Tool, root: &Path, include_ignored: bool) -> BTreeSet<String> {
        let mut args = json!({"path": root, "include_ignored": include_ignored});
        args["pattern"] = json!(if tool.name() == "Glob" {
            "**/*.{log,txt,rs}"
        } else {
            "needle"
        });
        let ToolOutcome::Completed(result) = tool.invoke(args, ToolCtx::none("t")).await.unwrap()
        else {
            panic!("expected Completed")
        };
        assert!(result.success);
        result
            .result
            .lines()
            .map(|line| {
                Path::new(line)
                    .strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/")
            })
            .collect()
    }

    fn names(expected: &[&str]) -> BTreeSet<String> {
        expected.iter().map(|name| name.to_string()).collect()
    }

    #[tokio::test]
    async fn both_tools_apply_nested_gitignores_and_negation_but_keep_hidden_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(".git")).unwrap();
        write(
            dir.path(),
            ".gitignore",
            "*.log\n!keep.log\nignored/\nnested/*.rs\n",
        );
        write(
            dir.path(),
            "nested/.gitignore",
            "!included.rs\nblocked.txt\n",
        );
        for path in [
            "skip.log",
            "keep.log",
            ".hidden.txt",
            ".hidden-dir/hidden-child.txt",
            "ignored/child.txt",
            "nested/skipped.rs",
            "nested/included.rs",
            "nested/blocked.txt",
            "node_modules/dependency.txt",
            ".bamboo/worktree/child/duplicate.txt",
        ] {
            write(dir.path(), path, "needle\n");
        }

        for tool in [&GrepTool::new() as &dyn Tool, &GlobTool::new()] {
            assert_eq!(
                search(tool, dir.path(), false).await,
                names(&[
                    "keep.log",
                    ".hidden.txt",
                    ".hidden-dir/hidden-child.txt",
                    "nested/included.rs",
                ]),
                "{} default traversal",
                tool.name()
            );
            assert_eq!(
                search(tool, dir.path(), true).await,
                names(&[
                    "skip.log",
                    "keep.log",
                    ".hidden.txt",
                    ".hidden-dir/hidden-child.txt",
                    "ignored/child.txt",
                    "nested/skipped.rs",
                    "nested/included.rs",
                    "nested/blocked.txt",
                ]),
                "{} opt-out keeps fixed directory exclusions",
                tool.name()
            );
        }
    }

    #[tokio::test]
    async fn both_tools_read_parent_rules_within_repository_and_stop_at_git_root() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), ".gitignore", "*.txt\n");
        let repo = dir.path().join("repo");
        std::fs::create_dir_all(repo.join(".git")).unwrap();
        write(&repo, ".gitignore", "*.log\n!keep.log\n");
        write(&repo, "src/skip.log", "needle\n");
        write(&repo, "src/keep.log", "needle\n");
        write(&repo, "src/keep.txt", "needle\n");
        write(&repo, "src/.ignore", "keep.txt\n");
        write(&repo, ".git/info/exclude", "src/keep.txt\n");
        let root = repo.join("src");

        for tool in [&GrepTool::new() as &dyn Tool, &GlobTool::new()] {
            assert_eq!(
                search(tool, &root, false).await,
                names(&["keep.log", "keep.txt"]),
                "{} respects repository parents, excludes outside parents/.ignore/local exclude",
                tool.name()
            );
        }
    }

    #[tokio::test]
    async fn both_tools_do_not_apply_gitignore_outside_repositories() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), ".gitignore", "*.txt\n");
        write(dir.path(), "keep.txt", "needle\n");
        write(dir.path(), "nested/.gitignore", "*.log\n");
        write(dir.path(), "nested/keep.log", "needle\n");

        for tool in [&GrepTool::new() as &dyn Tool, &GlobTool::new()] {
            assert_eq!(
                search(tool, dir.path(), false).await,
                names(&["keep.txt", "nested/keep.log"])
            );
        }
    }

    #[tokio::test]
    async fn both_tools_recognize_git_worktree_file_as_repository_boundary() {
        let dir = tempfile::tempdir().unwrap();
        // Linked Git worktrees have a `.git` file rather than a directory.
        write(dir.path(), ".git", "gitdir: /unused/worktree/gitdir\n");
        write(dir.path(), ".gitignore", "skip.txt\n");
        write(dir.path(), "skip.txt", "needle\n");
        write(dir.path(), "keep.txt", "needle\n");

        for tool in [&GrepTool::new() as &dyn Tool, &GlobTool::new()] {
            assert_eq!(search(tool, dir.path(), false).await, names(&["keep.txt"]));
        }
    }

    #[tokio::test]
    async fn both_tools_require_explicit_path_for_ignored_file_opt_out() {
        for tool in [&GrepTool::new() as &dyn Tool, &GlobTool::new()] {
            let error = tool
                .invoke(
                    json!({"pattern": "needle", "include_ignored": true}),
                    ToolCtx::none("t"),
                )
                .await
                .expect_err("opt-out without a path must fail");
            assert!(error
                .to_string()
                .contains("include_ignored requires an explicit path."));
        }
    }

    #[tokio::test]
    async fn both_tools_keep_fixed_exclusions_when_explicitly_used_as_root() {
        let dir = tempfile::tempdir().unwrap();
        for relative in ["node_modules", ".bamboo/worktree"] {
            write(dir.path(), &format!("{relative}/skip.txt"), "needle\n");
            let root = dir.path().join(relative);
            for tool in [&GrepTool::new() as &dyn Tool, &GlobTool::new()] {
                assert!(search(tool, &root, false).await.is_empty());
                assert!(search(tool, &root, true).await.is_empty());
            }
        }
    }

    #[tokio::test]
    async fn explicit_file_paths_keep_each_tools_existing_behavior() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(".git")).unwrap();
        write(dir.path(), ".gitignore", "*.txt\n");
        write(dir.path(), ".hidden.txt", "needle\n");
        let file = dir.path().join(".hidden.txt");

        let ToolOutcome::Completed(result) = GrepTool::new()
            .invoke(
                json!({"pattern": "needle", "path": file}),
                ToolCtx::none("t"),
            )
            .await
            .unwrap()
        else {
            panic!("expected Completed")
        };
        assert!(result.result.contains(".hidden.txt"));
        let error = GlobTool::new()
            .invoke(
                json!({"pattern": "*.txt", "path": file}),
                ToolCtx::none("t"),
            )
            .await
            .expect_err("Glob still requires a directory");
        assert!(error.to_string().contains("Search path is not a directory"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn both_tools_skip_descendant_file_and_directory_symlinks() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        write(dir.path(), "keep.txt", "needle\n");
        write(outside.path(), "outside.txt", "needle\n");
        std::os::unix::fs::symlink(outside.path(), dir.path().join("linked-directory")).unwrap();
        std::os::unix::fs::symlink(
            outside.path().join("outside.txt"),
            dir.path().join("linked.txt"),
        )
        .unwrap();

        for tool in [&GrepTool::new() as &dyn Tool, &GlobTool::new()] {
            assert_eq!(search(tool, dir.path(), false).await, names(&["keep.txt"]));
            assert_eq!(search(tool, dir.path(), true).await, names(&["keep.txt"]));
        }
    }
}
