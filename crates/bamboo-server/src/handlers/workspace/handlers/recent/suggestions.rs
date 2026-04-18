use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::handlers::workspace::types::{PathSuggestion, RecentWorkspaceEntry};

pub(super) async fn default_path_suggestions(home: &Path) -> Vec<PathSuggestion> {
    let home_path = home.to_string_lossy().to_string();
    let mut suggestions = vec![PathSuggestion {
        path: home_path,
        name: "Home".to_string(),
        description: None,
        suggestion_type: "home".to_string(),
    }];

    let candidates = vec![
        ("documents", "Documents"),
        ("desktop", "Desktop"),
        ("downloads", "Downloads"),
    ];
    for (suggestion_type, folder) in candidates {
        let path = home.join(folder);
        if tokio::fs::metadata(&path).await.is_ok() {
            suggestions.push(PathSuggestion {
                path: path.to_string_lossy().to_string(),
                name: folder.to_string(),
                description: None,
                suggestion_type: suggestion_type.to_string(),
            });
        }
    }

    suggestions
}

pub(super) fn recent_suggestion_name(item: &RecentWorkspaceEntry) -> String {
    item.metadata
        .as_ref()
        .and_then(|metadata| metadata.workspace_name.clone())
        .or_else(|| {
            PathBuf::from(&item.path)
                .file_name()
                .and_then(|segment| segment.to_str())
                .map(ToString::to_string)
        })
        .unwrap_or_else(|| item.path.clone())
}

pub(super) fn dedupe_suggestions_by_path(suggestions: &mut Vec<PathSuggestion>) {
    let mut seen = HashSet::new();
    suggestions.retain(|item| seen.insert(item.path.clone()));
}
