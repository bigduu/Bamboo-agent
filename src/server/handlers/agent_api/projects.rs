mod create;
mod list;
mod sessions;

pub use create::create_project;
pub use list::list_projects;
pub use sessions::get_project_sessions;

use std::path::{Path, PathBuf};

use super::fs::read_text_file;

fn read_project_path(project_dir: &Path) -> String {
    read_text_file(&project_dir.join(".project_path"), "project path")
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn list_project_session_files(project_dir: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(project_dir)
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| entry.path())
                .filter(|path| {
                    path.is_file() && path.extension().and_then(|ext| ext.to_str()) == Some("jsonl")
                })
                .collect()
        })
        .unwrap_or_default()
}

fn filesystem_created_at(path: &Path) -> u64 {
    std::fs::metadata(path)
        .ok()
        .and_then(|meta| meta.created().ok())
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn project_id_from_canonical_path(path: &Path) -> String {
    path.to_string_lossy().replace(['/', '\\'], "-")
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{list_project_session_files, project_id_from_canonical_path, read_project_path};

    #[test]
    fn project_id_from_canonical_path_replaces_path_separators() {
        let id = project_id_from_canonical_path(std::path::Path::new("/tmp/workspace/demo"));
        assert_eq!(id, "-tmp-workspace-demo");
    }

    #[test]
    fn read_project_path_trims_whitespace() {
        let dir = tempdir().expect("temp dir");
        fs::write(dir.path().join(".project_path"), "/tmp/workspace\n")
            .expect("write .project_path");

        let project_path = read_project_path(dir.path());
        assert_eq!(project_path, "/tmp/workspace");
    }

    #[test]
    fn list_project_session_files_only_returns_jsonl_files() {
        let dir = tempdir().expect("temp dir");
        fs::write(dir.path().join("a.jsonl"), "{}\n").expect("write jsonl");
        fs::write(dir.path().join("b.txt"), "not session").expect("write txt");
        fs::create_dir_all(dir.path().join("nested.jsonl")).expect("create dir");

        let files = list_project_session_files(dir.path());
        let names: Vec<_> = files
            .iter()
            .filter_map(|path| path.file_name().and_then(|name| name.to_str()))
            .collect();

        assert_eq!(names, vec!["a.jsonl"]);
    }
}
