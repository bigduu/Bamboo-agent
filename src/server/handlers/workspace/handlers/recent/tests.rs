use super::store_ops::upsert_recent_workspace;
use super::suggestions::{dedupe_suggestions_by_path, recent_suggestion_name};
use super::workspace_info::workspace_name_from_path;
use crate::server::handlers::workspace::types::{
    AddRecentWorkspaceRequest, PathSuggestion, RecentWorkspaceEntry, RecentWorkspaceStore,
    WorkspaceMetadata,
};

#[test]
fn upsert_recent_workspace_updates_existing_entry() {
    let mut store = RecentWorkspaceStore {
        items: vec![RecentWorkspaceEntry {
            path: "/tmp/project".to_string(),
            metadata: Some(WorkspaceMetadata {
                workspace_name: Some("Old".to_string()),
                description: None,
                tags: None,
            }),
            last_opened: 10,
        }],
    };
    let payload = AddRecentWorkspaceRequest {
        path: "/tmp/project".to_string(),
        metadata: Some(WorkspaceMetadata {
            workspace_name: Some("New".to_string()),
            description: Some("updated".to_string()),
            tags: None,
        }),
    };

    upsert_recent_workspace(&mut store, &payload, 20);

    assert_eq!(store.items.len(), 1);
    assert_eq!(store.items[0].last_opened, 20);
    assert_eq!(
        store.items[0]
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.workspace_name.as_ref())
            .map(String::as_str),
        Some("New")
    );
}

#[test]
fn upsert_recent_workspace_truncates_to_fifty_items() {
    let mut store = RecentWorkspaceStore {
        items: (0..50)
            .map(|index| RecentWorkspaceEntry {
                path: format!("/tmp/project-{index}"),
                metadata: None,
                last_opened: index,
            })
            .collect(),
    };
    let payload = AddRecentWorkspaceRequest {
        path: "/tmp/latest".to_string(),
        metadata: None,
    };

    upsert_recent_workspace(&mut store, &payload, 100);

    assert_eq!(store.items.len(), 50);
    assert_eq!(store.items[0].path, "/tmp/latest");
}

#[test]
fn recent_suggestion_name_prefers_metadata_then_path_basename() {
    let with_metadata = RecentWorkspaceEntry {
        path: "/tmp/project".to_string(),
        metadata: Some(WorkspaceMetadata {
            workspace_name: Some("Workspace A".to_string()),
            description: None,
            tags: None,
        }),
        last_opened: 1,
    };
    let without_metadata = RecentWorkspaceEntry {
        path: "/tmp/project-b".to_string(),
        metadata: None,
        last_opened: 1,
    };

    assert_eq!(recent_suggestion_name(&with_metadata), "Workspace A");
    assert_eq!(recent_suggestion_name(&without_metadata), "project-b");
}

#[test]
fn dedupe_suggestions_by_path_keeps_first_entry() {
    let mut suggestions = vec![
        PathSuggestion {
            path: "/tmp/a".to_string(),
            name: "First".to_string(),
            description: None,
            suggestion_type: "home".to_string(),
        },
        PathSuggestion {
            path: "/tmp/a".to_string(),
            name: "Second".to_string(),
            description: None,
            suggestion_type: "recent".to_string(),
        },
        PathSuggestion {
            path: "/tmp/b".to_string(),
            name: "Third".to_string(),
            description: None,
            suggestion_type: "recent".to_string(),
        },
    ];

    dedupe_suggestions_by_path(&mut suggestions);

    assert_eq!(suggestions.len(), 2);
    assert_eq!(suggestions[0].name, "First");
    assert_eq!(suggestions[1].name, "Third");
}

#[test]
fn workspace_name_from_path_uses_last_segment() {
    assert_eq!(
        workspace_name_from_path("/tmp/my-workspace").as_deref(),
        Some("my-workspace")
    );
}
