//! Error type shared by manifest validation, provenance I/O, and the
//! installer trait.

/// Result alias used throughout this crate.
pub type PluginResult<T> = Result<T, PluginError>;

#[derive(Debug, thiserror::Error)]
pub enum PluginError {
    /// `plugin.json` failed structural validation (see
    /// [`crate::manifest::PluginManifest::validate`]) — includes bad ids,
    /// bad semver shape, path-traversal attempts, duplicate ids, etc.
    #[error("invalid plugin manifest: {0}")]
    InvalidManifest(String),

    /// No installed plugin with this id (uninstall/lookup).
    #[error("plugin not found: {0}")]
    NotFound(String),

    /// A plugin with this id is already installed and `install` was called
    /// with [`crate::installer::InstallDisposition::FailIfInstalled`] (the
    /// `bamboo plugin install` verb). Re-run as an upgrade
    /// (`bamboo plugin update` / [`crate::installer::InstallDisposition::Upgrade`])
    /// to replace it in place.
    #[error("plugin already installed: {0} (use `update` to upgrade in place)")]
    AlreadyInstalled(String),

    /// A capability the plugin declares collides with an existing entry in a
    /// shared store that is NOT owned by this plugin (a user's own entry, or
    /// another plugin's). For MCP servers and workflows the install REFUSES
    /// rather than clobbering — an MCP server id / workflow filename is
    /// referenced elsewhere, so silently overwriting (and later deleting on
    /// uninstall) would destroy the user's entry. `kind` is a short label
    /// such as `"mcp server"` or `"workflow"`.
    #[error(
        "{kind} '{name}' already exists and is not owned by plugin '{plugin_id}'; \
         refusing to overwrite — rename the conflicting entry or the plugin's, then retry"
    )]
    Conflict {
        kind: &'static str,
        name: String,
        plugin_id: String,
    },

    /// The manifest's `platforms` gate excludes the current OS.
    #[error("plugin '{plugin_id}' does not support platform '{platform}'")]
    UnsupportedPlatform { plugin_id: String, platform: String },

    /// A step this foundation crate deliberately leaves for a later agent
    /// (capability-registration wiring — see `PLUGIN_PLAN.md`). Returned
    /// instead of panicking so a partially-stacked branch fails a request
    /// cleanly rather than crashing the process.
    #[error("not yet implemented: {0}")]
    NotImplemented(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}
