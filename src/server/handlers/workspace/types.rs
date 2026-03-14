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
