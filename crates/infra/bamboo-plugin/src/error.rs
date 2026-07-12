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

    /// A capability failed to register/deregister against `AppState` for a
    /// reason OTHER than an ownership conflict (e.g. `config.json` couldn't
    /// be persisted, a network fetch during source-staging failed). Kept
    /// distinct from [`Self::Conflict`] (a deliberate REFUSAL, not a
    /// failure) so callers/HTTP status mapping can tell "your plugin
    /// collides with something" apart from "something broke while trying to
    /// register/fetch it".
    #[error("plugin registration failed: {0}")]
    Registration(String),

    /// A downloaded artifact's sha256 did not match the manifest's declared
    /// hash. Checked BEFORE unpacking (supply-chain: a URL-installed plugin
    /// ships a binary that will be executed) — never surfaced as a generic
    /// `Registration`/`InvalidManifest` error so callers can distinguish "the
    /// author's manifest is malformed" from "the bytes served at that URL do
    /// not match what the manifest promised".
    #[error("artifact verification failed: {0}")]
    ArtifactVerificationFailed(String),

    /// A downloaded URL plugin BUNDLE (the `plugin.json` or the archive
    /// containing it — whatever the app layer's URL-source fetch downloads)
    /// did not hash to the caller-supplied expected sha256. Checked BEFORE
    /// any extraction/parsing, so a tampered bundle is never trusted even
    /// partially. Distinct from [`Self::ArtifactVerificationFailed`] (which
    /// is pinned by a hash declared INSIDE the manifest — itself only
    /// trustworthy once the bundle carrying it is verified): this is the
    /// root-of-trust check that closes the circular-trust hole where the
    /// manifest's own artifact hash could be rewritten by whoever tampered
    /// with the bundle.
    #[error("bundle verification failed: {0}")]
    BundleVerificationFailed(String),

    /// A URL plugin install/update was requested with neither a `sha256` to
    /// verify the downloaded bundle against, nor an explicit
    /// `allow_unverified` opt-in. This is the secure-by-default refusal: a
    /// URL install must be either checksum-pinned or an explicit,
    /// deliberate risk acceptance — never a silent "download and trust any
    /// tar.gz". Raised BEFORE the URL is ever fetched (no network access
    /// happens for a refused install).
    #[error("{0}")]
    ChecksumRequired(String),

    /// A URL plugin install/update's host (and path) is not in the
    /// configured `plugin_trust.trusted_hosts` allowlist, and the request did
    /// not set `allow_untrusted_host`. This is the SOURCE-authorization
    /// layer (host allowlist) — distinct from [`Self::BundleVerificationFailed`]
    /// (integrity) and [`Self::UnsignedOrUntrustedSignature`] (publisher
    /// authenticity). Raised BEFORE the URL is ever fetched, same posture as
    /// [`Self::ChecksumRequired`].
    #[error("{0}")]
    UntrustedHost(String),

    /// A URL plugin bundle is unsigned, or its `.sig` sidecar does not verify
    /// against any key in `plugin_trust.trusted_keys`, and the request did
    /// not set `allow_unsigned`. This is the PUBLISHER-authenticity layer —
    /// a signature proves who produced the bytes, which a bare sha256 (only
    /// proving WHAT the bytes are) cannot. Raised after the bundle is
    /// downloaded (the signature is verified over the downloaded bytes) but
    /// before anything is extracted/parsed.
    #[error("{0}")]
    UnsignedOrUntrustedSignature(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}
