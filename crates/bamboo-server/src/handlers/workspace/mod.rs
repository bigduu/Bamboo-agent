//! Workspace management handlers.

mod handlers;
mod path;
mod store;
mod types;

pub use handlers::{
    add_recent_workspace, browse_folder, config, get_recent_workspaces, get_workspace_suggestions,
    list_workspace_files, validate_workspace,
};

#[cfg(test)]
mod tests;
