use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct WorkspacePathRequest {
    pub(super) path: String,
}

#[derive(Deserialize)]
pub struct BrowseFolderRequest {
    pub(super) path: Option<String>,
}

#[derive(Deserialize)]
pub struct WorkspaceFilesRequest {
    pub(super) path: String,
    pub(super) max_depth: Option<usize>,
    pub(super) max_entries: Option<usize>,
    pub(super) include_hidden: Option<bool>,
}

#[derive(Serialize)]
pub(super) struct BrowseFolderResponse {
    pub(super) current_path: String,
    pub(super) parent_path: Option<String>,
    pub(super) folders: Vec<FolderItem>,
}

#[derive(Serialize)]
pub(super) struct FolderItem {
    pub(super) name: String,
    pub(super) path: String,
}

#[derive(Serialize)]
pub(super) struct WorkspaceFileEntry {
    pub(super) name: String,
    pub(super) path: String,
    pub(super) is_directory: bool,
}

#[derive(Serialize, Deserialize, Clone)]
pub(super) struct WorkspaceMetadata {
    pub(super) workspace_name: Option<String>,
    pub(super) description: Option<String>,
    pub(super) tags: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(super) struct RecentWorkspaceEntry {
    pub(super) path: String,
    pub(super) metadata: Option<WorkspaceMetadata>,
    pub(super) last_opened: u64,
}

#[derive(Serialize, Deserialize, Default)]
pub(super) struct RecentWorkspaceStore {
    pub(super) items: Vec<RecentWorkspaceEntry>,
}

#[derive(Serialize)]
pub(super) struct WorkspaceInfo {
    pub(super) path: String,
    pub(super) is_valid: bool,
    pub(super) error_message: Option<String>,
    pub(super) file_count: Option<u64>,
    pub(super) last_modified: Option<String>,
    pub(super) size_bytes: Option<u64>,
    pub(super) workspace_name: Option<String>,
}

#[derive(Serialize)]
pub(super) struct PathSuggestion {
    pub(super) path: String,
    pub(super) name: String,
    pub(super) description: Option<String>,
    pub(super) suggestion_type: String,
}

#[derive(Serialize)]
pub(super) struct PathSuggestionsResponse {
    pub(super) suggestions: Vec<PathSuggestion>,
}

#[derive(Deserialize)]
pub struct AddRecentWorkspaceRequest {
    pub(super) path: String,
    pub(super) metadata: Option<WorkspaceMetadata>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workspace_path_request_deserialization() {
        let json = r#"{"path":"/home/user/project"}"#;
        let req: WorkspacePathRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.path, "/home/user/project");
    }

    #[test]
    fn test_browse_folder_request_with_path() {
        let json = r#"{"path":"/home/user"}"#;
        let req: BrowseFolderRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.path, Some("/home/user".to_string()));
    }

    #[test]
    fn test_browse_folder_request_without_path() {
        let json = r#"{}"#;
        let req: BrowseFolderRequest = serde_json::from_str(json).unwrap();
        assert!(req.path.is_none());
    }

    #[test]
    fn test_workspace_files_request_minimal() {
        let json = r#"{"path":"/src"}"#;
        let req: WorkspaceFilesRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.path, "/src");
        assert!(req.max_depth.is_none());
        assert!(req.max_entries.is_none());
        assert!(req.include_hidden.is_none());
    }

    #[test]
    fn test_workspace_files_request_full() {
        let json = r#"{"path":"/src","max_depth":5,"max_entries":100,"include_hidden":true}"#;
        let req: WorkspaceFilesRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.path, "/src");
        assert_eq!(req.max_depth, Some(5));
        assert_eq!(req.max_entries, Some(100));
        assert_eq!(req.include_hidden, Some(true));
    }

    #[test]
    fn test_browse_folder_response_serialization() {
        let response = BrowseFolderResponse {
            current_path: "/home".to_string(),
            parent_path: Some("/".to_string()),
            folders: vec![FolderItem {
                name: "user".to_string(),
                path: "/home/user".to_string(),
            }],
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("/home"));
        assert!(json.contains("user"));
    }

    #[test]
    fn test_folder_item_serialization() {
        let item = FolderItem {
            name: "Documents".to_string(),
            path: "/home/user/Documents".to_string(),
        };

        let json = serde_json::to_string(&item).unwrap();
        assert!(json.contains("Documents"));
    }

    #[test]
    fn test_workspace_file_entry() {
        let entry = WorkspaceFileEntry {
            name: "file.txt".to_string(),
            path: "/path/file.txt".to_string(),
            is_directory: false,
        };

        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("file.txt"));
        assert!(json.contains("false"));
    }

    #[test]
    fn test_workspace_metadata_full() {
        let json =
            r#"{"workspace_name":"MyProject","description":"Test project","tags":["rust","test"]}"#;
        let meta: WorkspaceMetadata = serde_json::from_str(json).unwrap();

        assert_eq!(meta.workspace_name, Some("MyProject".to_string()));
        assert_eq!(meta.description, Some("Test project".to_string()));
        assert_eq!(
            meta.tags,
            Some(vec!["rust".to_string(), "test".to_string()])
        );
    }

    #[test]
    fn test_workspace_metadata_empty() {
        let json = r#"{}"#;
        let meta: WorkspaceMetadata = serde_json::from_str(json).unwrap();

        assert!(meta.workspace_name.is_none());
        assert!(meta.description.is_none());
        assert!(meta.tags.is_none());
    }

    #[test]
    fn test_recent_workspace_entry() {
        let json =
            r#"{"path":"/project","metadata":{"workspace_name":"Test"},"last_opened":1234567890}"#;
        let entry: RecentWorkspaceEntry = serde_json::from_str(json).unwrap();

        assert_eq!(entry.path, "/project");
        assert!(entry.metadata.is_some());
        assert_eq!(entry.last_opened, 1234567890);
    }

    #[test]
    fn test_recent_workspace_store_default() {
        let store = RecentWorkspaceStore::default();
        assert!(store.items.is_empty());
    }

    #[test]
    fn test_workspace_info_serialization() {
        let info = WorkspaceInfo {
            path: "/project".to_string(),
            is_valid: true,
            error_message: None,
            file_count: Some(42),
            last_modified: Some("2024-01-01".to_string()),
            size_bytes: Some(1024),
            workspace_name: Some("Test".to_string()),
        };

        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("/project"));
        assert!(json.contains("\"is_valid\":true"));
    }

    #[test]
    fn test_path_suggestion() {
        let suggestion = PathSuggestion {
            path: "/home/user".to_string(),
            name: "user".to_string(),
            description: Some("User home".to_string()),
            suggestion_type: "folder".to_string(),
        };

        let json = serde_json::to_string(&suggestion).unwrap();
        assert!(json.contains("user"));
        assert!(json.contains("folder"));
    }

    #[test]
    fn test_path_suggestions_response() {
        let response = PathSuggestionsResponse {
            suggestions: vec![],
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"suggestions\":[]"));
    }

    #[test]
    fn test_add_recent_workspace_request() {
        let json = r#"{"path":"/new/project"}"#;
        let req: AddRecentWorkspaceRequest = serde_json::from_str(json).unwrap();

        assert_eq!(req.path, "/new/project");
        assert!(req.metadata.is_none());
    }

    #[test]
    fn test_add_recent_workspace_with_metadata() {
        let json = r#"{"path":"/project","metadata":{"workspace_name":"Test"}}"#;
        let req: AddRecentWorkspaceRequest = serde_json::from_str(json).unwrap();

        assert_eq!(req.path, "/project");
        assert!(req.metadata.is_some());
    }

    #[test]
    fn test_workspace_metadata_clone() {
        let meta = WorkspaceMetadata {
            workspace_name: Some("Test".to_string()),
            description: None,
            tags: None,
        };

        let cloned = meta.clone();
        assert_eq!(meta.workspace_name, cloned.workspace_name);
    }
}
