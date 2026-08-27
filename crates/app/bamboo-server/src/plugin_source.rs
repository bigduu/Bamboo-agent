//! Plugin source staging (Wave 2 § Installer-core agent, `PLUGIN_PLAN.md`
//! Deliverable B): turns whatever the caller (CLI/HTTP) pointed the
//! installer at — a local directory, a local `.zip`/`.tar.gz`/`.tgz`
//! archive, or a URL — into a validated bundle at `plugins_dir()/<id>/`,
//! ready to hand to [`bamboo_plugin::PluginInstaller::install`].
//!
//! # The three sources
//!
//! - [`PluginSourceInput::LocalDir`] — copies the directory tree.
//! - [`PluginSourceInput::LocalArchive`] — unpacks a `.zip`/`.tar.gz`/`.tgz`.
//!   If the archive wraps everything in a single top-level directory (the
//!   common `tar czf bundle.tar.gz plugin-name/` convention), that directory
//!   is flattened up so `plugin.json` ends up at the bundle root either way.
//! - [`PluginSourceInput::Url`] — fetches the manifest bundle (a bare
//!   `plugin.json`, content-only and typically an MCP server backed entirely
//!   by a downloadable binary — e.g. a nova-style plugin with no bundled
//!   skills/prompts — or an archive containing one, same flattening rule as
//!   `LocalArchive`). **Three trust layers, stacked, enforced in
//!   [`fetch_manifest_bundle`] in this order:**
//!
//!   1. **Host allowlist (source authorization)** — is the URL's `<host><path>`
//!      one the operator has trusted (`bamboo_config::PluginTrustConfig::trusted_hosts`)?
//!      Refused BEFORE any network access ([`PluginError::UntrustedHost`])
//!      unless `allow_untrusted_host` is set.
//!   2. **Signature (publisher authenticity)** — after the bundle is
//!      downloaded, does its `<url>.sig` sidecar (a raw 64-byte ed25519
//!      signature, hex-encoded, over the exact bundle bytes) verify against
//!      any `bamboo_config::TrustedKey` in `trusted_keys`? Refused
//!      ([`PluginError::UnsignedOrUntrustedSignature`]) unless `allow_unsigned`
//!      is set.
//!   3. **Checksum (integrity)** — same sha256 pin as before, EXCEPT a
//!      verified signature from layer 2 already proves integrity+authenticity
//!      more strongly than a pasted hash could, so it SATISFIES this layer's
//!      requirement even with neither `sha256` nor `allow_unverified` given
//!      (an `allow_unsigned` bypass grants no such credit — an unsigned
//!      install still needs its own sha256/allow_unverified exactly as
//!      before). See [`fetch_manifest_bundle`] for the precise precedence.
//!
//!   A pasted checksum ALONE never establishes source trust — it is circular
//!   if the attacker controls the page the checksum was copied from — which
//!   is why layers 1 and 2 exist independently of layer 3.
//!
//!   Byte-authenticity note: the host allowlist only vets the FIRST hop's
//!   `<host><path>`, not wherever an HTTP redirect might lead — a signature
//!   or checksum is what actually authenticates the downloaded bytes, so
//!   redirects are followed whenever either will be checked, but disabled
//!   entirely for the fully-opted-out `allow_unsigned && sha256.is_none()`
//!   case, where the host allowlist is the sole control (see
//!   [`http_client_no_redirects`]).
//!
//! # `--insecure` / `plugin_trust.enforcement`: skip ALL three layers at once
//!
//! The three `allow_*` opt-outs above are per-layer. On top of them,
//! [`PluginSourceInput::Url::insecure`] is a convenience AGGREGATE — set it
//! (CLI: `--insecure`; HTTP: `"insecure": true` on the `url` source) and
//! [`fetch_manifest_bundle`] treats `allow_untrusted_host`, `allow_unsigned`
//! AND `allow_unverified` as all `true` for that one install, without the
//! caller spelling out all three. There is also a PERSISTENT, config-level
//! form for an operator who never wants to pass flags at all:
//! `bamboo_config::PluginTrustConfig::enforcement` set to
//! `PluginTrustEnforcement::Off` makes EVERY `url` install/update behave as
//! if `--insecure` were passed, with no per-install flag needed. Precedence,
//! in both cases:
//!
//! - The aggregate ONLY turns per-layer checks OFF — it never turns off a
//!   check the caller opted INTO. A supplied `sha256` is still hashed and
//!   compared; a mismatch is still [`PluginError::BundleVerificationFailed`],
//!   `--insecure`/`enforcement: off` or not. So `--insecure --sha256 <hex>`
//!   means "skip host/signature enforcement AND the bare
//!   sha256-required-by-default rule, but still verify THIS hash".
//! - The per-layer flags keep working independently — a caller who wants to
//!   waive just the host allowlist (say) still passes
//!   `--allow-untrusted-host` alone; the aggregate is a shortcut for "all
//!   three", not a replacement for them.
//! - `plugin_trust.enforcement` defaults to `Strict` (secure by default) for
//!   both a fresh config and one with no `plugin_trust.enforcement` key at
//!   all — this is opt-in relaxation, never a silent weakening.
//!
//! Every install where the aggregate is active — via `insecure: true` on the
//! request OR `plugin_trust.enforcement: off` — logs a prominent
//! `tracing::warn!` naming the source URL, and records `insecure: true` in
//! the resulting `PluginSource::Url` provenance (`bamboo plugin list`/audit
//! can then tell these installs apart from ones where the same three
//! individual `allow_*` flags merely happened to all be set). A server
//! booting with `plugin_trust.enforcement: off` also logs its own startup
//! warning (see `AppState::new`), since that setting silently affects EVERY
//! future install, not just one command invocation.
//!
//!   THEN, separately, for [`Platform::current`] (if the manifest declares an
//!   artifact for it), fetches the per-platform binary archive named in
//!   `manifest.artifacts`, verifies its sha256 BEFORE unpacking (mandatory —
//!   a URL plugin ships a binary that gets executed), and places the single
//!   expected executable at `bin/<platform>/<id>[.exe]` per
//!   [`bamboo_plugin::manifest::PluginArtifact`]'s archive contract. This
//!   artifact-sha256 check is unaffected by the host/signature layers above
//!   (the artifact URL is declared inside a manifest that has ALREADY passed
//!   all three trust layers) — it remains defense in depth for the binary
//!   specifically, closing the gap where the artifact's own declared hash
//!   lives inside the bundle that carries it.
//!
//! All three paths run the SAME safety checks: [`PluginManifest::validate`]
//! before anything is committed to `plugins_dir()`, and path-traversal-safe
//! archive extraction (a zip entry's [`zip::read::ZipFile::enclosed_name`]
//! rejects `..`/absolute entries outright; a tar entry's path is checked for
//! `ParentDir`/root/prefix components before extraction) — a malicious
//! archive must not be able to write outside its own staging directory.
//!
//! # Swap safety (why an upgrade doesn't lose the old bundle on failure)
//!
//! `plugin_dir` is a fixed path per id (`plugins_dir()/<id>/`), so an upgrade
//! necessarily replaces whatever is already there. [`prepare_plugin_source`]
//! builds the new bundle in a scratch `.staging-*` directory first (so a bad
//! source — invalid manifest, failed download, sha256 mismatch — never
//! touches the existing install). The server-owned transaction seam then holds
//! the plugin-operation lock while auditing global ownership; only an accepted
//! [`PreparedPlugin`] is activated, after which the OLD `plugin_dir` is moved
//! aside to a `.backup-*` directory (not deleted) and the candidate is renamed
//! into place. The private staged transaction carries exact directory
//! identities for the candidate and backup. On install failure it quarantines
//! the live entry, restores only an identity-verified backup with NOREPLACE,
//! and reports whether restarting the stopped service is safe. An ambiguous
//! destination, candidate, or backup is preserved and requires manual
//! recovery. Production exposes only [`install_server_plugin_from_source`];
//! low-level staging helpers exist under `cfg(test)` and cannot bypass the
//! server's ownership preflight or operation lock.
//!
//! Residual gap (documented, not solved here): the plugin_dir swap itself and
//! `install()`'s own capability-registration rollback (see
//! `crate::plugin_installer`'s module docs) are two separate best-effort
//! steps, not one atomic transaction. If the process crashes between the
//! swap and `install()` returning, a retry is still safe (staging always
//! starts from a fresh scratch dir; a leftover `.backup-*`/`.staging-*` dir
//! is inert and can be swept by an operator or a future cleanup pass) but is
//! not automatic today.
//!
//! # Known follow-ups (deferred — tracked here, not fixed on this branch)
//!
//! - **URL content-bundle integrity pin: IMPLEMENTED (secure by default).**
//!   Previously only the per-platform BINARY artifact was sha256-pinned
//!   (`PluginArtifact.sha256`, verified in [`fetch_and_place_artifact`]),
//!   while the `plugin.json` / content archive fetched by
//!   [`fetch_manifest_bundle`] was trusted on HTTPS alone — a MITM or a
//!   compromised host could serve a tampered bundle, and since the binary's
//!   sha256 is DECLARED INSIDE that same untrusted manifest, tampering the
//!   bundle could rewrite the artifact hash too (the trust chain was
//!   circular). Fixed: [`PluginSourceInput::Url`] now carries a `sha256`
//!   (the expected hash of the downloaded bundle) and an `allow_unverified`
//!   opt-out. [`fetch_manifest_bundle`] verifies the bundle's actual sha256
//!   against it BEFORE any extraction/parsing on a mismatch
//!   ([`PluginError::BundleVerificationFailed`]); with neither a `sha256`
//!   nor `allow_unverified: true`, the fetch is refused up front — before
//!   the URL is ever requested — with [`PluginError::ChecksumRequired`]. A
//!   URL install can therefore no longer just download-and-trust any
//!   tar.gz. The verified bundle sha256 (not the binary artifact's) is what
//!   `PluginSource::Url.sha256` records for provenance/audit.
//! - **Source-TRUST layer: IMPLEMENTED** (host allowlist + ed25519 publisher
//!   signature — see the module-level "three trust layers" summary above and
//!   [`fetch_manifest_bundle`]). A sha256 pin alone only proves "this is the
//!   bytes the installer expected", not "an entity I trust produced them" —
//!   and worse, a checksum pasted from the SAME page as a malicious URL is
//!   circular, proving nothing about the source. `bamboo_config::PluginTrustConfig`
//!   (`trusted_hosts` + `trusted_keys`, both user-editable in `config.json`)
//!   closes that: a URL install now also needs an operator-trusted host and
//!   (absent `allow_unsigned`) a bundle signature verifying against a trusted
//!   key. Still deferred:
//!   - **SSRF guard**, described next.
//! - **No SSRF guard on URL fetch.** [`download_bytes`] will fetch any URL,
//!   including `http://169.254.169.254/...` (cloud metadata) or private-range
//!   / loopback addresses. In a hosted/multi-tenant deployment a plugin-install
//!   URL is an SSRF vector. A private-IP / metadata-endpoint blocklist (or an
//!   allowlist of plugin registries) is a threat-model call for the deploy
//!   layer; noted here so it isn't forgotten.
//! - **`prompt-presets.json`'s `save_store` is non-atomic** (`fs::write` in
//!   place, pre-existing behaviour shared with the HTTP prompt-preset
//!   handlers): a crash mid-write can truncate `prompt-presets.json`. A
//!   write-to-temp-then-rename would make it atomic, matching what
//!   `bamboo_plugin::registry::InstalledPlugins::save` (`installed.json`) now
//!   does; deferred here as a change to a shared, pre-existing storage
//!   helper rather than this branch's new code.
//!
//! Production install/update handlers retain one `PLUGIN_OP_LOCK` guard across
//! ownership preflight, service shutdown, activation, install, and rollback,
//! so shared server state has no preflight-to-swap race.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use bamboo_config::PluginTrustConfig;
use bamboo_plugin::manifest::Platform;
#[cfg(test)]
use bamboo_plugin::PluginInstaller;
use bamboo_plugin::{
    InstallDisposition, InstalledPlugin, PluginError, PluginManifest, PluginResult, PluginSource,
};
use ed25519_dalek::Verifier;

use crate::plugin_installer::ServerPluginInstaller;

/// What the caller pointed the installer at.
#[derive(Debug, Clone)]
pub enum PluginSourceInput {
    /// A local directory containing `plugin.json` at its root.
    LocalDir(PathBuf),
    /// A local `.zip` / `.tar.gz` / `.tgz` archive containing `plugin.json`
    /// (at its root, or under a single top-level directory).
    LocalArchive(PathBuf),
    /// A URL to either a bare `plugin.json` or an archive containing one
    /// (same root-or-single-subdir rule as `LocalArchive`). Three trust
    /// layers, all enforced in [`fetch_manifest_bundle`] (see the module
    /// docs' "three trust layers" summary):
    ///
    /// - `allow_untrusted_host`: opt out of the host allowlist
    ///   (`bamboo_config::PluginTrustConfig::trusted_hosts`) — see
    ///   [`PluginError::UntrustedHost`].
    /// - `allow_unsigned`: opt out of requiring the bundle's `.sig` to verify
    ///   against a trusted key — see [`PluginError::UnsignedOrUntrustedSignature`].
    /// - `sha256`/`allow_unverified`: the checksum layer, unchanged from
    ///   before EXCEPT a verified signature now also satisfies it (see
    ///   [`fetch_manifest_bundle`]) — see [`PluginError::ChecksumRequired`].
    ///
    /// Plus `insecure`: the convenience AGGREGATE opt-out over all three
    /// above (equivalent to setting `allow_untrusted_host`, `allow_unsigned`
    /// AND `allow_unverified` together for THIS install) — see the module
    /// docs' "`--insecure` / `plugin_trust.enforcement`" section. A supplied
    /// `sha256` is still verified even when `insecure` is set (`insecure`
    /// only turns checks OFF; it never turns a check the caller opted INTO
    /// off too).
    Url {
        url: String,
        sha256: Option<String>,
        allow_unverified: bool,
        allow_untrusted_host: bool,
        allow_unsigned: bool,
        insecure: bool,
    },
}

/// A fully downloaded/copied, extracted, and validated plugin bundle that is
/// still isolated under a UUID staging directory. The private server
/// transaction inspects its manifest and runs shared ownership preflight
/// before activation swaps any existing `plugins/<id>` directory.
#[derive(Debug)]
struct PreparedPlugin {
    manifest: PluginManifest,
    prepared_dir: PathBuf,
    plugin_dir: PathBuf,
    source: PluginSource,
    candidate_identity: BundleIdentity,
    // Keep the directory open for the whole transaction so an unlinked
    // candidate's device/inode pair cannot be recycled and mistaken for a
    // replacement directory before activation or rollback finishes.
    _candidate_handle: std::fs::File,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BundleIdentity {
    device: u64,
    inode: u64,
}

#[derive(Debug)]
struct BundleSnapshot {
    path: PathBuf,
    identity: BundleIdentity,
    // The open handle pins the directory identity across sibling renames.
    // Numeric identity alone is insufficient because filesystems may reuse
    // an inode/file index after deletion.
    _handle: std::fs::File,
}

#[derive(Debug)]
enum BundleRecovery {
    /// The previous live bundle is either unchanged or identity-verified at
    /// its original path, so services stopped for the upgrade may restart.
    RestartSafe,
    /// At least one path is ambiguous. Every object is preserved and stopped
    /// services must remain stopped until an operator reconciles the paths.
    ManualRecoveryRequired(String),
}

impl BundleRecovery {
    fn restart_safe(&self) -> bool {
        matches!(self, Self::RestartSafe)
    }

    fn wrap_error(self, error: PluginError) -> PluginError {
        match self {
            Self::RestartSafe => error,
            Self::ManualRecoveryRequired(detail) => PluginError::Registration(format!(
                "{error}; manual bundle recovery is required and stopped services were not restarted: {detail}"
            )),
        }
    }
}

#[derive(Debug)]
struct BundleTransactionFailure {
    error: PluginError,
    recovery: BundleRecovery,
}

impl BundleTransactionFailure {
    fn restart_safe(&self) -> bool {
        self.recovery.restart_safe()
    }

    fn into_plugin_error(self) -> PluginError {
        self.recovery.wrap_error(self.error)
    }
}

#[cfg(unix)]
fn capture_bundle_directory(path: &Path) -> std::io::Result<(std::fs::File, BundleIdentity)> {
    use std::os::unix::fs::MetadataExt;

    let handle: std::fs::File = rustix::fs::open(
        path,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(std::io::Error::from)?
    .into();
    let metadata = handle.metadata()?;
    if !metadata.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "plugin bundle path must name a real directory",
        ));
    }
    let identity = BundleIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    };
    Ok((handle, identity))
}

#[cfg(windows)]
fn capture_bundle_directory(path: &Path) -> std::io::Result<(std::fs::File, BundleIdentity)> {
    use std::os::windows::fs::{MetadataExt, OpenOptionsExt};
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    };

    let file = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_dir() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "plugin bundle path must name a real directory, not a reparse point",
        ));
    }
    let identity = bamboo_skills::clone_publication::std_file_identity(&file).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "the plugin bundle filesystem did not expose a stable directory identity",
        )
    })?;
    let identity = BundleIdentity {
        device: identity.device,
        inode: identity.inode,
    };
    Ok((file, identity))
}

#[cfg(not(any(unix, windows)))]
fn capture_bundle_directory(_path: &Path) -> std::io::Result<(std::fs::File, BundleIdentity)> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "identity-bound plugin activation is unavailable on this platform",
    ))
}

fn bundle_directory_identity(path: &Path) -> std::io::Result<BundleIdentity> {
    capture_bundle_directory(path).map(|(_handle, identity)| identity)
}

fn restore_verified_backup(backup: &BundleSnapshot, plugin_dir: &Path) -> BundleRecovery {
    match bundle_directory_identity(&backup.path) {
        Ok(identity) if identity == backup.identity => {}
        Ok(_) => {
            return BundleRecovery::ManualRecoveryRequired(format!(
                "the backup at '{}' changed identity and was not moved",
                backup.path.display()
            ));
        }
        Err(error) => {
            return BundleRecovery::ManualRecoveryRequired(format!(
                "the backup at '{}' could not be identity-verified and was not moved: {error}",
                backup.path.display()
            ));
        }
    }
    if let Err(error) = rename_noreplace(&backup.path, plugin_dir) {
        return BundleRecovery::ManualRecoveryRequired(format!(
            "the previous bundle remains at '{}' because '{}' could not be restored without replacement: {error}",
            backup.path.display(),
            plugin_dir.display()
        ));
    }
    match bundle_directory_identity(plugin_dir) {
        Ok(identity) if identity == backup.identity => BundleRecovery::RestartSafe,
        Ok(_) => BundleRecovery::ManualRecoveryRequired(format!(
            "the restored destination '{}' does not have the previous bundle identity",
            plugin_dir.display()
        )),
        Err(error) => BundleRecovery::ManualRecoveryRequired(format!(
            "the restored destination '{}' could not be identity-verified: {error}",
            plugin_dir.display()
        )),
    }
}

impl PreparedPlugin {
    /// Atomically make this candidate the plugin's fixed on-disk bundle,
    /// retaining the previous bundle for commit/rollback. Shared ownership
    /// preflight and upgrade service shutdown must happen before this call.
    #[cfg(test)]
    async fn activate(self) -> Result<StagedPlugin, BundleTransactionFailure> {
        self.activate_inner(ActivationFault::None).await
    }

    async fn activate_inner(
        self,
        fault: ActivationFault,
    ) -> Result<StagedPlugin, BundleTransactionFailure> {
        let backup = match std::fs::symlink_metadata(&self.plugin_dir) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    let _ = tokio::fs::remove_dir_all(&self.prepared_dir).await;
                    return Err(BundleTransactionFailure {
                        error: PluginError::InvalidManifest(format!(
                            "existing plugin destination '{}' must be a real directory",
                            self.plugin_dir.display()
                        )),
                        recovery: BundleRecovery::ManualRecoveryRequired(format!(
                            "the existing destination '{}' was not an identity-verified previous bundle",
                            self.plugin_dir.display()
                        )),
                    });
                }
                let (previous_handle, previous_identity) =
                    match capture_bundle_directory(&self.plugin_dir) {
                        Ok(snapshot) => snapshot,
                        Err(error) => {
                            let _ = tokio::fs::remove_dir_all(&self.prepared_dir).await;
                            return Err(BundleTransactionFailure {
                                error: PluginError::Io(error),
                                recovery: BundleRecovery::ManualRecoveryRequired(format!(
                                    "could not verify the unchanged previous bundle at '{}'",
                                    self.plugin_dir.display()
                                )),
                            });
                        }
                    };
                let Some(root) = self.plugin_dir.parent() else {
                    let _ = tokio::fs::remove_dir_all(&self.prepared_dir).await;
                    return Err(BundleTransactionFailure {
                        error: PluginError::InvalidManifest(
                            "plugin directory has no parent".to_string(),
                        ),
                        recovery: BundleRecovery::ManualRecoveryRequired(
                            "the previous bundle path had no parent".to_string(),
                        ),
                    });
                };
                let backup = root.join(format!(
                    ".backup-{}-{}",
                    self.manifest.id,
                    uuid::Uuid::new_v4()
                ));
                if let Err(error) = rename_noreplace(&self.plugin_dir, &backup) {
                    let _ = tokio::fs::remove_dir_all(&self.prepared_dir).await;
                    let recovery = match bundle_directory_identity(&self.plugin_dir) {
                        Ok(identity) if identity == previous_identity => {
                            BundleRecovery::RestartSafe
                        }
                        Ok(_) => BundleRecovery::ManualRecoveryRequired(format!(
                            "the destination '{}' changed identity while the backup rename failed",
                            self.plugin_dir.display()
                        )),
                        Err(verify_error) => BundleRecovery::ManualRecoveryRequired(format!(
                            "the backup rename failed and the previous bundle at '{}' could not be reverified: {verify_error}",
                            self.plugin_dir.display()
                        )),
                    };
                    return Err(BundleTransactionFailure {
                        error: PluginError::Io(error),
                        recovery,
                    });
                }
                match bundle_directory_identity(&backup) {
                    Ok(identity) if identity == previous_identity => {}
                    Ok(_) => {
                        let _ = tokio::fs::remove_dir_all(&self.prepared_dir).await;
                        return Err(BundleTransactionFailure {
                            error: PluginError::Registration(format!(
                                "the previous plugin bundle changed identity while moving to '{}'",
                                backup.display()
                            )),
                            recovery: BundleRecovery::ManualRecoveryRequired(format!(
                                "the ambiguous backup was preserved at '{}'",
                                backup.display()
                            )),
                        });
                    }
                    Err(error) => {
                        let _ = tokio::fs::remove_dir_all(&self.prepared_dir).await;
                        return Err(BundleTransactionFailure {
                            error: PluginError::Io(error),
                            recovery: BundleRecovery::ManualRecoveryRequired(format!(
                                "the unverified backup was preserved at '{}'",
                                backup.display()
                            )),
                        });
                    }
                }
                Some(BundleSnapshot {
                    path: backup,
                    identity: previous_identity,
                    _handle: previous_handle,
                })
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => {
                let _ = tokio::fs::remove_dir_all(&self.prepared_dir).await;
                return Err(BundleTransactionFailure {
                    error: PluginError::Io(error),
                    recovery: BundleRecovery::ManualRecoveryRequired(format!(
                        "the live destination '{}' could not be inspected",
                        self.plugin_dir.display()
                    )),
                });
            }
        };

        let rename_result = fault.install_destination(&self.plugin_dir).and_then(|()| {
            if fault.fail_candidate_rename() {
                Err(std::io::Error::other(
                    "injected prepared-plugin activation rename failure",
                ))
            } else {
                rename_noreplace(&self.prepared_dir, &self.plugin_dir)
            }
        });
        if let Err(rename_error) = rename_result {
            // Both paths are siblings below one plugins root, so EXDEV is an
            // invariant violation, not a reason to merge-copy into a live
            // destination. Clean only the private UUID candidate. If an old
            // bundle was backed up, restore it with an atomic NOREPLACE
            // rename; a race-created destination is never overwritten or
            // deleted, and a failed restore deliberately leaves the backup
            // intact for operator recovery.
            let _ = tokio::fs::remove_dir_all(&self.prepared_dir).await;
            let recovery = match &backup {
                Some(backup) => restore_verified_backup(backup, &self.plugin_dir),
                None => match std::fs::symlink_metadata(&self.plugin_dir) {
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        BundleRecovery::RestartSafe
                    }
                    Ok(_) => BundleRecovery::ManualRecoveryRequired(format!(
                        "an unexpected destination remains at '{}' and there was no previous bundle",
                        self.plugin_dir.display()
                    )),
                    Err(error) => BundleRecovery::ManualRecoveryRequired(format!(
                        "the destination '{}' could not be inspected after publication failed: {error}",
                        self.plugin_dir.display()
                    )),
                },
            };
            return Err(BundleTransactionFailure {
                error: PluginError::Registration(format!(
                    "failed to atomically activate prepared plugin '{}' with a no-replace rename: {rename_error}",
                    self.manifest.id
                )),
                recovery,
            });
        }

        match bundle_directory_identity(&self.plugin_dir) {
            Ok(identity) if identity == self.candidate_identity => {}
            Ok(_) => {
                return Err(BundleTransactionFailure {
                    error: PluginError::Registration(format!(
                        "activated plugin '{}' changed identity during publication",
                        self.manifest.id
                    )),
                    recovery: BundleRecovery::ManualRecoveryRequired(format!(
                        "the live destination '{}' and backup were preserved",
                        self.plugin_dir.display()
                    )),
                });
            }
            Err(error) => {
                return Err(BundleTransactionFailure {
                    error: PluginError::Io(error),
                    recovery: BundleRecovery::ManualRecoveryRequired(format!(
                        "the activated destination '{}' could not be identity-verified; its backup was preserved",
                        self.plugin_dir.display()
                    )),
                });
            }
        }

        Ok(StagedPlugin {
            manifest: self.manifest,
            plugin_dir: self.plugin_dir,
            source: self.source,
            candidate_identity: self.candidate_identity,
            _candidate_handle: self._candidate_handle,
            backup,
        })
    }

    /// Remove an unactivated candidate after path-id or ownership preflight
    /// refuses it. The live plugin bundle is untouched.
    async fn discard(self) {
        if let Err(error) = tokio::fs::remove_dir_all(&self.prepared_dir).await {
            tracing::warn!(
                %error,
                prepared_dir = %self.prepared_dir.display(),
                "failed to discard isolated plugin candidate"
            );
        }
    }

    #[cfg(test)]
    async fn activate_with_fault(
        self,
        fault: ActivationFault,
    ) -> Result<StagedPlugin, BundleTransactionFailure> {
        self.activate_inner(fault).await
    }
}

#[derive(Debug)]
enum ActivationFault {
    None,
    #[cfg(test)]
    FailCandidateRename,
    #[cfg(test)]
    CreateDestinationDirectory,
    #[cfg(all(test, unix))]
    CreateDestinationSymlink(PathBuf),
}

impl ActivationFault {
    fn fail_candidate_rename(&self) -> bool {
        #[cfg(test)]
        {
            matches!(self, Self::FailCandidateRename)
        }
        #[cfg(not(test))]
        {
            false
        }
    }

    fn install_destination(&self, _destination: &Path) -> std::io::Result<()> {
        match self {
            Self::None => Ok(()),
            #[cfg(test)]
            Self::FailCandidateRename => Ok(()),
            #[cfg(test)]
            Self::CreateDestinationDirectory => {
                std::fs::create_dir(_destination)?;
                std::fs::write(_destination.join("RACE_MARKER"), b"race-owned")?;
                Ok(())
            }
            #[cfg(all(test, unix))]
            Self::CreateDestinationSymlink(target) => {
                std::os::unix::fs::symlink(target, _destination)?;
                Ok(())
            }
        }
    }
}

/// Atomically rename one sibling entry without replacing any destination that
/// appeared after preflight. Prepared-bundle publication and its immediate
/// backup restoration both use this primitive, so neither can overwrite a
/// race-created directory, file, symlink, or Windows reparse point.
#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple",
    target_os = "redox"
))]
fn rename_noreplace(source: &Path, destination: &Path) -> std::io::Result<()> {
    use std::os::fd::AsFd;

    let source_parent = source
        .parent()
        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    let destination_parent = destination
        .parent()
        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    if source_parent != destination_parent {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "prepared plugin activation paths must be siblings",
        ));
    }
    let source_name = source
        .file_name()
        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    let destination_name = destination
        .file_name()
        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    let parent = std::fs::File::open(source_parent)?;
    rustix::fs::renameat_with(
        parent.as_fd(),
        source_name,
        parent.as_fd(),
        destination_name,
        rustix::fs::RenameFlags::NOREPLACE,
    )
    .map_err(std::io::Error::from)
}

#[cfg(windows)]
fn rename_noreplace(source: &Path, destination: &Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::MoveFileExW;

    let source_parent = source
        .parent()
        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    let destination_parent = destination
        .parent()
        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    if source_parent != destination_parent {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "prepared plugin activation paths must be siblings",
        ));
    }

    fn nul_terminated(path: &Path) -> std::io::Result<Vec<u16>> {
        let mut wide = path.as_os_str().encode_wide().collect::<Vec<_>>();
        if wide.contains(&0) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "plugin activation path contains an interior NUL",
            ));
        }
        wide.push(0);
        Ok(wide)
    }

    let source = nul_terminated(source)?;
    let destination = nul_terminated(destination)?;
    // No MOVEFILE_REPLACE_EXISTING flag: a destination that appeared after
    // preflight, including a reparse point, makes this atomic rename fail.
    let result = unsafe { MoveFileExW(source.as_ptr(), destination.as_ptr(), 0) };
    if result == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(any(
    windows,
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple",
    target_os = "redox"
)))]
fn rename_noreplace(_source: &Path, _destination: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "atomic no-replace plugin activation is unavailable on this platform",
    ))
}

/// Private staged source transaction. Keeping both the type and every field
/// private prevents callers from swapping a bundle without server ownership
/// preflight or replacing its identity-bound paths after validation.
#[derive(Debug)]
struct StagedPlugin {
    manifest: PluginManifest,
    plugin_dir: PathBuf,
    source: PluginSource,
    candidate_identity: BundleIdentity,
    _candidate_handle: std::fs::File,
    backup: Option<BundleSnapshot>,
}

#[derive(Debug)]
enum RollbackFault {
    None,
    #[cfg(test)]
    ReplaceDestinationDirectory,
}

impl RollbackFault {
    fn install_destination(&self, _plugin_dir: &Path) -> std::io::Result<()> {
        match self {
            Self::None => Ok(()),
            #[cfg(test)]
            Self::ReplaceDestinationDirectory => {
                let parent = _plugin_dir.parent().ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "plugin directory has no parent",
                    )
                })?;
                let displaced = parent.join(format!(
                    ".fault-displaced-candidate-{}",
                    uuid::Uuid::new_v4()
                ));
                rename_noreplace(_plugin_dir, &displaced)?;
                std::fs::create_dir(_plugin_dir)?;
                std::fs::write(_plugin_dir.join("RACE_MARKER"), b"race-owned")
            }
        }
    }
}

impl StagedPlugin {
    /// Finalize a successful install. Post-commit backup cleanup is
    /// best-effort housekeeping, not part of the live-bundle selection or
    /// service-restart safety contract: a backup is first moved to a private
    /// retirement path, identity-checked, and only then removed. Cleanup
    /// failure is observable but never turns a committed install into a
    /// rollback attempt.
    async fn commit(self) {
        let Some(backup) = self.backup else {
            return;
        };
        let Some(parent) = backup.path.parent() else {
            tracing::warn!(
                backup = %backup.path.display(),
                "committed plugin backup has no parent; leaving it for operator cleanup"
            );
            return;
        };
        let retired = parent.join(format!(
            ".retired-{}-{}",
            self.manifest.id,
            uuid::Uuid::new_v4()
        ));
        if let Err(error) = rename_noreplace(&backup.path, &retired) {
            tracing::warn!(
                %error,
                backup = %backup.path.display(),
                "failed to retire committed plugin backup; leaving it in place"
            );
            return;
        }
        match bundle_directory_identity(&retired) {
            Ok(identity) if identity == backup.identity => {
                if let Err(error) = tokio::fs::remove_dir_all(&retired).await {
                    tracing::warn!(
                        %error,
                        retired = %retired.display(),
                        "failed to remove identity-verified retired plugin backup"
                    );
                }
            }
            identity => {
                let restored = rename_noreplace(&retired, &backup.path);
                tracing::warn!(
                    retired = %retired.display(),
                    backup = %backup.path.display(),
                    observed = ?identity,
                    restore = ?restored,
                    "retired plugin backup identity was ambiguous; preserved without deletion"
                );
            }
        }
    }

    /// Undo a failed install without deleting a path merely because it has
    /// the expected name. The live entry is atomically quarantined first. A
    /// verified candidate remains inert in quarantine after recovery; an
    /// unexpected entry is put back with NOREPLACE and the old backup remains
    /// preserved for manual recovery.
    #[cfg(test)]
    async fn rollback(self) -> BundleRecovery {
        self.rollback_inner(RollbackFault::None).await
    }

    async fn rollback_inner(self, fault: RollbackFault) -> BundleRecovery {
        if let Err(error) = fault.install_destination(&self.plugin_dir) {
            return BundleRecovery::ManualRecoveryRequired(format!(
                "rollback fault setup failed without deleting any bundle path: {error}"
            ));
        }

        let Some(parent) = self.plugin_dir.parent() else {
            return BundleRecovery::ManualRecoveryRequired(
                "the live plugin path has no parent".to_string(),
            );
        };
        let quarantine = parent.join(format!(
            ".rollback-{}-{}",
            self.manifest.id,
            uuid::Uuid::new_v4()
        ));
        match rename_noreplace(&self.plugin_dir, &quarantine) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return match &self.backup {
                    Some(backup) => restore_verified_backup(backup, &self.plugin_dir),
                    None => BundleRecovery::RestartSafe,
                };
            }
            Err(error) => {
                return BundleRecovery::ManualRecoveryRequired(format!(
                    "the live destination '{}' could not be quarantined without replacement and was left untouched: {error}",
                    self.plugin_dir.display()
                ));
            }
        }

        match bundle_directory_identity(&quarantine) {
            Ok(identity) if identity == self.candidate_identity => {}
            observed => {
                let put_back = rename_noreplace(&quarantine, &self.plugin_dir);
                return BundleRecovery::ManualRecoveryRequired(format!(
                    "the live destination was not this transaction's candidate ({observed:?}); the unexpected object was preserved at '{}' (put-back result: {put_back:?}) and the previous backup was not moved",
                    if put_back.is_ok() {
                        self.plugin_dir.display()
                    } else {
                        quarantine.display()
                    }
                ));
            }
        }

        let recovery = match &self.backup {
            Some(backup) => restore_verified_backup(backup, &self.plugin_dir),
            None => BundleRecovery::RestartSafe,
        };
        if !recovery.restart_safe() {
            // The known candidate and old backup are both retained. Deleting
            // either would make an already-ambiguous recovery irreversible.
            return recovery;
        }

        // Keep the failed candidate quarantined. Even a second identity check
        // followed by path-based recursive deletion would leave a
        // check-to-delete race in which a watcher could replace this UUID
        // path. The old live bundle is already restored, so retention is
        // inert and does not prevent a safe service restart.
        tracing::warn!(
            quarantine = %quarantine.display(),
            "failed plugin candidate was quarantined after rollback and retained for operator cleanup"
        );
        recovery
    }
}

/// Prepare a source completely in an isolated scratch directory without
/// swapping `plugins/<id>`. This is the production seam for callers that must
/// perform global provenance/ownership checks before any shared mutation.
async fn prepare_plugin_source(
    input: PluginSourceInput,
    plugins_root: &Path,
    trust: &PluginTrustConfig,
) -> PluginResult<PreparedPlugin> {
    prepare_plugin_source_inner(input, plugins_root, trust, MAX_DECOMPRESSED_BYTES).await
}

/// Test-only low-level staging seam. Production callers cannot activate a
/// bundle outside [`install_server_plugin_from_source`]'s ownership preflight
/// and operation lock.
#[cfg(test)]
async fn stage_plugin_source(
    input: PluginSourceInput,
    plugins_root: &Path,
    trust: &PluginTrustConfig,
) -> PluginResult<StagedPlugin> {
    stage_plugin_source_inner(input, plugins_root, trust, MAX_DECOMPRESSED_BYTES).await
}

/// Test-only seam for [`stage_plugin_source`] that lets a test inject a small
/// `max_decompressed_bytes` cap (the production cap, [`MAX_DECOMPRESSED_BYTES`],
/// is a generous 2 GiB — not practical to actually exceed in a unit test).
/// Exercises the exact same staging/swap machinery as the production path,
/// just with the archive-extraction ceiling parameterized.
#[cfg(test)]
async fn stage_plugin_source_with_decompressed_cap(
    input: PluginSourceInput,
    plugins_root: &Path,
    trust: &PluginTrustConfig,
    max_decompressed_bytes: u64,
) -> PluginResult<StagedPlugin> {
    stage_plugin_source_inner(input, plugins_root, trust, max_decompressed_bytes).await
}

#[cfg(test)]
async fn stage_plugin_source_inner(
    input: PluginSourceInput,
    plugins_root: &Path,
    trust: &PluginTrustConfig,
    max_decompressed_bytes: u64,
) -> PluginResult<StagedPlugin> {
    prepare_plugin_source_inner(input, plugins_root, trust, max_decompressed_bytes)
        .await?
        .activate()
        .await
        .map_err(BundleTransactionFailure::into_plugin_error)
}

async fn prepare_plugin_source_inner(
    input: PluginSourceInput,
    plugins_root: &Path,
    trust: &PluginTrustConfig,
    max_decompressed_bytes: u64,
) -> PluginResult<PreparedPlugin> {
    tokio::fs::create_dir_all(plugins_root).await?;
    let staging_dir = plugins_root.join(format!(".staging-{}", uuid::Uuid::new_v4()));
    tokio::fs::create_dir_all(&staging_dir).await?;

    let staged = stage_into(&input, &staging_dir, trust, max_decompressed_bytes).await;
    let (manifest, source) = match staged {
        Ok(pair) => pair,
        Err(error) => {
            let _ = tokio::fs::remove_dir_all(&staging_dir).await;
            return Err(error);
        }
    };

    if let Err(error) = manifest.validate() {
        let _ = tokio::fs::remove_dir_all(&staging_dir).await;
        return Err(error);
    }

    let (candidate_handle, candidate_identity) = match capture_bundle_directory(&staging_dir) {
        Ok(snapshot) => snapshot,
        Err(error) => {
            let _ = tokio::fs::remove_dir_all(&staging_dir).await;
            return Err(PluginError::Io(error));
        }
    };
    let plugin_dir = plugins_root.join(&manifest.id);
    Ok(PreparedPlugin {
        manifest,
        plugin_dir,
        prepared_dir: staging_dir,
        source,
        candidate_identity,
        _candidate_handle: candidate_handle,
    })
}

/// Server-owned source transaction. This is the only public source-install
/// seam for [`ServerPluginInstaller`]: the same process-wide guard spans
/// provenance preflight, prior-service shutdown, live-bundle activation,
/// installer mutation, and commit/rollback/restart.
///
/// `expected_plugin_id` binds an HTTP path (or another caller-owned identity)
/// before any shared mutation. Pass `None` when the source manifest owns the
/// identity, as in the manual server example.
pub async fn install_server_plugin_from_source(
    installer: &ServerPluginInstaller,
    input: PluginSourceInput,
    plugins_root: &Path,
    trust: &PluginTrustConfig,
    disposition: InstallDisposition,
    expected_plugin_id: Option<&str>,
) -> PluginResult<InstalledPlugin> {
    install_server_plugin_from_source_inner(
        installer,
        input,
        plugins_root,
        trust,
        disposition,
        expected_plugin_id,
        ServerSourceFault::None,
    )
    .await
}

#[derive(Debug)]
enum ServerSourceFault {
    None,
    #[cfg(test)]
    ActivationRenameFailure,
    #[cfg(test)]
    ActivationDestinationDirectory,
    #[cfg(test)]
    RollbackDestinationDirectory,
}

impl ServerSourceFault {
    fn activation_fault(&self) -> ActivationFault {
        match self {
            Self::None => ActivationFault::None,
            #[cfg(test)]
            Self::ActivationRenameFailure => ActivationFault::FailCandidateRename,
            #[cfg(test)]
            Self::ActivationDestinationDirectory => ActivationFault::CreateDestinationDirectory,
            #[cfg(test)]
            Self::RollbackDestinationDirectory => ActivationFault::None,
        }
    }

    fn injected_install_error(&self) -> Option<PluginError> {
        match self {
            #[cfg(test)]
            Self::RollbackDestinationDirectory => Some(PluginError::Registration(
                "injected install failure before rollback destination race".to_string(),
            )),
            _ => None,
        }
    }

    fn rollback_fault(&self) -> RollbackFault {
        match self {
            #[cfg(test)]
            Self::RollbackDestinationDirectory => RollbackFault::ReplaceDestinationDirectory,
            _ => RollbackFault::None,
        }
    }
}

#[cfg(test)]
async fn install_server_plugin_from_source_with_fault(
    installer: &ServerPluginInstaller,
    input: PluginSourceInput,
    plugins_root: &Path,
    trust: &PluginTrustConfig,
    disposition: InstallDisposition,
    expected_plugin_id: Option<&str>,
    fault: ServerSourceFault,
) -> PluginResult<InstalledPlugin> {
    install_server_plugin_from_source_inner(
        installer,
        input,
        plugins_root,
        trust,
        disposition,
        expected_plugin_id,
        fault,
    )
    .await
}

async fn install_server_plugin_from_source_inner(
    installer: &ServerPluginInstaller,
    input: PluginSourceInput,
    plugins_root: &Path,
    trust: &PluginTrustConfig,
    disposition: InstallDisposition,
    expected_plugin_id: Option<&str>,
    fault: ServerSourceFault,
) -> PluginResult<InstalledPlugin> {
    let prepared = prepare_plugin_source(input, plugins_root, trust).await?;
    if let Some(expected_plugin_id) = expected_plugin_id {
        if prepared.manifest.id != expected_plugin_id {
            let manifest_id = prepared.manifest.id.clone();
            prepared.discard().await;
            return Err(PluginError::InvalidManifest(format!(
                "path id '{expected_plugin_id}' does not match the source's manifest id '{manifest_id}'"
            )));
        }
    }

    let plugin_id = prepared.manifest.id.clone();
    let guard = installer.begin_operation().await;
    if let Err(error) = installer
        .preflight_prepared_candidate(
            &prepared.manifest,
            &prepared.prepared_dir,
            disposition,
            &guard,
        )
        .await
    {
        prepared.discard().await;
        return Err(error);
    }

    let stopped_services = if disposition == InstallDisposition::Upgrade {
        installer.stop_services_for_upgrade(&plugin_id).await
    } else {
        Vec::new()
    };
    let staged = match prepared.activate_inner(fault.activation_fault()).await {
        Ok(staged) => staged,
        Err(failure) => {
            let restart_safe = failure.restart_safe();
            let error = failure.into_plugin_error();
            if restart_safe {
                installer
                    .restart_services_after_failed_upgrade(&plugin_id, &stopped_services)
                    .await;
            }
            return Err(error);
        }
    };

    let manifest = staged.manifest.clone();
    let plugin_dir = staged.plugin_dir.clone();
    let source = staged.source.clone();
    let install_result = match fault.injected_install_error() {
        Some(error) => Err(error),
        None => {
            installer
                .install_with_operation(
                    &manifest,
                    &plugin_dir,
                    source,
                    disposition,
                    chrono::Utc::now(),
                    &guard,
                )
                .await
        }
    };
    match install_result {
        Ok(entry) => {
            staged.commit().await;
            Ok(entry)
        }
        Err(error) => {
            let recovery = staged.rollback_inner(fault.rollback_fault()).await;
            if recovery.restart_safe() {
                installer
                    .restart_services_after_failed_upgrade(&plugin_id, &stopped_services)
                    .await;
            }
            Err(recovery.wrap_error(error))
        }
    }
}

/// Stage + `install()` + commit/rollback for a standalone installer that does
/// not share bamboo-server's capability stores or operation lock. Server code
/// must use [`install_server_plugin_from_source`] instead.
#[cfg(test)]
async fn install_plugin_from_source(
    installer: &dyn PluginInstaller,
    input: PluginSourceInput,
    plugins_root: &Path,
    trust: &PluginTrustConfig,
    disposition: InstallDisposition,
) -> PluginResult<InstalledPlugin> {
    let staged = stage_plugin_source(input, plugins_root, trust).await?;
    let manifest = staged.manifest.clone();
    let plugin_dir = staged.plugin_dir.clone();
    let source = staged.source.clone();

    match installer
        .install(
            &manifest,
            &plugin_dir,
            source,
            disposition,
            chrono::Utc::now(),
        )
        .await
    {
        Ok(entry) => {
            staged.commit().await;
            Ok(entry)
        }
        Err(error) => {
            let recovery = staged.rollback().await;
            Err(recovery.wrap_error(error))
        }
    }
}

async fn stage_into(
    input: &PluginSourceInput,
    staging_dir: &Path,
    trust: &PluginTrustConfig,
    max_decompressed_bytes: u64,
) -> PluginResult<(PluginManifest, PluginSource)> {
    match input {
        PluginSourceInput::LocalDir(path) => {
            copy_dir_recursive(path, staging_dir).await?;
            let manifest = read_and_parse_manifest(staging_dir).await?;
            Ok((manifest, PluginSource::LocalDir { path: path.clone() }))
        }
        PluginSourceInput::LocalArchive(path) => {
            let bytes = tokio::fs::read(path).await?;
            let kind = detect_archive_kind(&path.to_string_lossy()).ok_or_else(|| {
                PluginError::InvalidManifest(format!(
                    "unsupported archive extension for '{}': expected .zip/.tar.gz/.tgz",
                    path.display()
                ))
            })?;
            extract_archive(
                bytes,
                kind,
                staging_dir.to_path_buf(),
                max_decompressed_bytes,
            )
            .await?;
            flatten_if_single_subdir(staging_dir).await?;
            let manifest = read_and_parse_manifest(staging_dir).await?;
            Ok((manifest, PluginSource::LocalArchive { path: path.clone() }))
        }
        PluginSourceInput::Url {
            url,
            sha256,
            allow_unverified,
            allow_untrusted_host,
            allow_unsigned,
            insecure,
        } => {
            let flags = UrlTrustFlags {
                sha256: sha256.as_deref(),
                allow_unverified: *allow_unverified,
                allow_untrusted_host: *allow_untrusted_host,
                allow_unsigned: *allow_unsigned,
                insecure: *insecure,
            };
            let fetched =
                fetch_manifest_bundle(url, flags, trust, staging_dir, max_decompressed_bytes)
                    .await?;

            // Security (issue #479 §4 / open question 6): a manifest
            // declaring `provides.services` is the highest-trust plugin
            // artifact kind — a resident, unconstrained process — so it may
            // NEVER install from a URL source whose bytes weren't
            // cryptographically signed by a trusted key, no matter which
            // opt-out flag got it this far (`allow_unsigned` explicitly, or
            // the `--insecure`/`plugin_trust.enforcement: off` aggregate).
            // `fetched.signed_by.is_none()` is exactly that "unsigned"
            // signal regardless of WHY (genuinely unsigned bundle, or an
            // opt-out that let an unsigned/mismatched one through) — see
            // `fetch_manifest_bundle`'s layer-2 doc comment. Checked here
            // (not in `PluginManifest::validate`, which has no visibility
            // into install-time trust flags/signature results) and BEFORE
            // the per-platform binary artifact is fetched, so a refused
            // install downloads no executable at all.
            if !fetched.manifest.provides.services.is_empty() && fetched.signed_by.is_none() {
                return Err(PluginError::UnsignedOrUntrustedSignature(format!(
                    "refusing to install plugin '{}' from '{url}': it declares `provides.services` \
                     (long-running service plugins are the highest-trust artifact kind) but its \
                     bundle is unsigned or its signature does not verify against a trusted key — \
                     `--allow-unsigned`/`--insecure` and `plugin_trust.enforcement: off` are NOT \
                     honoured for a services-declaring manifest; publish a signature from a \
                     trusted key instead",
                    fetched.manifest.id
                )));
            }

            // Binary-artifact verification stays as defense in depth (see
            // the module docs) — its own sha256, declared inside the
            // now-verified manifest, is checked in
            // `fetch_and_place_artifact`, but no longer double-duty as the
            // `PluginSource::Url` provenance hash: that's the bundle's own
            // verified sha256 now, computed above.
            fetch_and_place_artifact(&fetched.manifest, staging_dir, max_decompressed_bytes)
                .await?;
            Ok((
                fetched.manifest,
                PluginSource::Url {
                    url: url.clone(),
                    sha256: fetched.verified_sha256,
                    allow_unverified: *allow_unverified,
                    allow_untrusted_host: *allow_untrusted_host,
                    allow_unsigned: *allow_unsigned,
                    signed_by: fetched.signed_by,
                    // The AGGREGATE, not the raw per-install `insecure` flag:
                    // recorded `true` whenever ALL three layers were actually
                    // skipped for this install, whether that came from the
                    // per-install flag or from `plugin_trust.enforcement:
                    // off` (see `fetch_manifest_bundle`) — either way, this is
                    // the single source of truth for "was this install done
                    // insecurely" that `plugin list`/audit needs.
                    insecure: fetched.insecure_aggregate,
                },
            ))
        }
    }
}

// ---------------------------------------------------------------------
// Manifest bundle fetch (URL source)
// ---------------------------------------------------------------------

/// The caller-supplied bits of a [`PluginSourceInput::Url`] that
/// [`fetch_manifest_bundle`] needs, grouped into one struct purely to keep
/// that function's parameter count sane (`PluginSourceInput::Url` itself
/// carries the same five fields, plus `url`, which stays a separate
/// top-level parameter since [`fetch_and_verify_signature`] and the sha256
/// helpers all key off it directly).
struct UrlTrustFlags<'a> {
    sha256: Option<&'a str>,
    allow_unverified: bool,
    allow_untrusted_host: bool,
    allow_unsigned: bool,
    /// The per-install `--insecure` / `"insecure": true` aggregate opt-out
    /// (see the module docs' "`--insecure` / `plugin_trust.enforcement`"
    /// section). ORed with `trust.enforcement_is_off()` inside
    /// [`fetch_manifest_bundle`] to compute the EFFECTIVE aggregate for this
    /// install — a config-level `enforcement: off` has the same effect as
    /// this flag without the caller having to set it.
    insecure: bool,
}

/// Everything [`fetch_manifest_bundle`] hands back to [`stage_into`].
struct FetchedBundle {
    manifest: PluginManifest,
    /// The verified bundle sha256 (`None` unless a `sha256` was supplied and
    /// confirmed) — for [`PluginSource::Url`] provenance.
    verified_sha256: Option<String>,
    /// The trusted key label the signature verified against (`None` if the
    /// install proceeded unsigned via `allow_unsigned`/the insecure
    /// aggregate).
    signed_by: Option<String>,
    /// The EFFECTIVE aggregate for this install: `true` when ALL three trust
    /// layers were skipped, whether that came from the per-install
    /// `insecure` flag or from `plugin_trust.enforcement: off`. This is what
    /// [`PluginSource::Url::insecure`] provenance records — see
    /// [`stage_into`].
    insecure_aggregate: bool,
}

/// Fetch `url`: either a bare `plugin.json` or an archive containing one
/// (same root-or-single-subdir rule as [`PluginSourceInput::LocalArchive`]).
/// Populates `staging_dir` with whatever the bundle contains (just
/// `plugin.json` for a bare manifest; the full skills/prompts/workflows tree
/// for an archive).
///
/// **Three trust layers, enforced in this order** (see the module docs'
/// summary):
///
/// 1. **Host allowlist.** Before the URL is even requested: if it is not
///    `https` with a `<host><path>` matching one of `trust.trusted_hosts` as a
///    prefix, refuses with [`PluginError::UntrustedHost`] — no network access
///    happens for a refused install — unless `allow_untrusted_host` is `true`
///    (logged).
/// 2. **Signature.** Once the bundle is downloaded, `<url>.sig` is fetched
///    (a missing/unreachable sidecar is treated identically to a malformed
///    one — see [`fetch_and_verify_signature`]) and checked against every
///    `algorithm: "ed25519"` entry in `trust.trusted_keys`. A match records
///    that key's label; no match refuses with
///    [`PluginError::UnsignedOrUntrustedSignature`] unless `allow_unsigned`
///    is `true` (logged).
/// 3. **Checksum.** If `sha256` is `None`, `allow_unverified` is `false`, AND
///    the bundle was NOT signature-verified in step 2, refuses with
///    [`PluginError::ChecksumRequired`] — a verified signature already proves
///    integrity+authenticity more strongly than a pasted hash, so it
///    satisfies this layer on its own (an `allow_unsigned` bypass grants no
///    such credit: an unsigned install still needs its own
///    `sha256`/`allow_unverified`, exactly as before this branch). If
///    `sha256` IS given (signed or not), the downloaded bytes are still
///    hashed and compared (case-insensitive) BEFORE any extraction/parsing —
///    a mismatch is [`PluginError::BundleVerificationFailed`] regardless of
///    signature status.
///
/// Before any of the three layers run, the EFFECTIVE aggregate is computed:
/// `flags.insecure || trust.enforcement_is_off()`. When `true`,
/// `allow_untrusted_host`/`allow_unsigned`/`allow_unverified` are all treated
/// as `true` for the rest of this call (see the module docs'
/// "`--insecure` / `plugin_trust.enforcement`" section) and a prominent
/// `tracing::warn!` names the source URL — this does NOT waive the `sha256`
/// check itself: a supplied hash is still verified in step 3 below.
///
/// Returns a [`FetchedBundle`]: the parsed manifest, the verified bundle
/// sha256 (`None` unless a `sha256` was supplied and confirmed — for
/// [`PluginSource::Url`] provenance), the trusted key label the signature
/// verified against (`None` if the install proceeded unsigned via
/// `allow_unsigned`/the aggregate), and the effective insecure-aggregate flag
/// itself (for `PluginSource::Url::insecure` provenance).
async fn fetch_manifest_bundle(
    url: &str,
    flags: UrlTrustFlags<'_>,
    trust: &PluginTrustConfig,
    staging_dir: &Path,
    max_decompressed_bytes: u64,
) -> PluginResult<FetchedBundle> {
    let UrlTrustFlags {
        sha256,
        allow_unverified,
        allow_untrusted_host,
        allow_unsigned,
        insecure,
    } = flags;

    // The convenience aggregate: a per-install `--insecure` flag OR a
    // config-level `plugin_trust.enforcement: off` both mean "skip all three
    // layers for this install" — computed once, up front, so every layer
    // below sees the SAME effective flags regardless of which of the two
    // triggered it. Shadowing the original `allow_*` bindings means the rest
    // of this function needs no further special-casing.
    let insecure_aggregate = insecure || trust.enforcement_is_off();
    if insecure_aggregate {
        tracing::warn!(
            %url,
            "installing plugin from '{url}' with ALL trust checks disabled (insecure) — host \
             allowlist, signature and checksum-required-by-default are all skipped for this \
             install (a supplied --sha256, if any, is still verified)"
        );
    }
    let allow_untrusted_host = allow_untrusted_host || insecure_aggregate;
    let allow_unsigned = allow_unsigned || insecure_aggregate;
    let allow_unverified = allow_unverified || insecure_aggregate;

    // Layer 1: host allowlist — refuse BEFORE any network access.
    if !trust.is_host_trusted(url) {
        if !allow_untrusted_host {
            return Err(PluginError::UntrustedHost(format!(
                "refusing to install plugin bundle from '{url}': its host is not in the \
                 `plugin_trust.trusted_hosts` allowlist (config.json) — add a matching \
                 host+path prefix there, or explicitly accept the risk (CLI: \
                 `--allow-untrusted-host`; HTTP: `\"allow_untrusted_host\": true`)"
            )));
        }
        tracing::warn!(
            %url,
            "installing plugin bundle from a host outside `plugin_trust.trusted_hosts` \
             (allow_untrusted_host opt-out)"
        );
    }

    // Redirect policy (BLOCKER 1 fix): whether the downloaded bytes WILL be
    // cryptographically authenticated determines whether it's safe to follow
    // a redirect. A signature is REQUIRED whenever `!allow_unsigned` — even
    // though it hasn't been fetched/checked yet, an unverified-but-required
    // signature refuses the install below regardless of which host actually
    // served the bytes, so following a redirect to get here is harmless.
    // Likewise a supplied `sha256` is checked (and refused on mismatch) below
    // regardless of the serving host. Only when NEITHER control is in play —
    // `allow_unsigned` AND no `sha256` — is the host allowlist the SOLE
    // authority over where these bytes came from; it only vetted this exact
    // URL, so redirects must be disabled in that case (see
    // `http_client_no_redirects`). The SAME client is used for both the
    // bundle fetch and the `.sig` fetch below, for one install.
    let bytes_will_be_authenticated = !allow_unsigned || sha256.is_some();
    let client = if bytes_will_be_authenticated {
        http_client_following_redirects()
    } else {
        http_client_no_redirects()
    };

    let bytes = download_bytes(client, url, MAX_DOWNLOAD_BYTES).await?;

    // Layer 2: signature — a valid signature is a STRONGER integrity +
    // authenticity guarantee than a pasted checksum (see layer 3 below).
    let signed_by = fetch_and_verify_signature(client, url, &bytes, &trust.trusted_keys).await;
    if signed_by.is_none() {
        if !allow_unsigned {
            return Err(PluginError::UnsignedOrUntrustedSignature(format!(
                "refusing to install plugin bundle from '{url}': it is unsigned, or its \
                 '{url}.sig' does not verify against any key in `plugin_trust.trusted_keys` \
                 (config.json) — publish a signature from a trusted key, or explicitly accept \
                 the risk (CLI: `--allow-unsigned`; HTTP: `\"allow_unsigned\": true`)"
            )));
        }
        tracing::warn!(
            %url,
            "installing an unsigned (or untrusted-signature) plugin bundle (allow_unsigned opt-out)"
        );
    }

    // Layer 3: checksum — superseded by a verified signature (layer 2), but
    // otherwise unchanged.
    if sha256.is_none() && !allow_unverified && signed_by.is_none() {
        return Err(PluginError::ChecksumRequired(format!(
            "refusing to install plugin bundle from '{url}' without a checksum — pass the \
             bundle's sha256 (from the release page / a trusted source) to verify it before \
             install (CLI: `--sha256 <hex>`; HTTP: `\"sha256\": \"<hex>\"` on the url source), \
             or explicitly accept the risk of an unverified download (CLI: \
             `--allow-unverified`; HTTP: `\"allow_unverified\": true`)"
        )));
    }

    let verified_sha256 = match sha256 {
        Some(expected) => {
            let actual = sha256_hex(&bytes);
            if !actual.eq_ignore_ascii_case(expected) {
                return Err(PluginError::BundleVerificationFailed(format!(
                    "sha256 mismatch for plugin bundle '{url}': expected {expected}, downloaded \
                     bytes hash to {actual} — refusing to unpack (the bundle may be tampered, \
                     corrupted, or the wrong sha256 was supplied)"
                )));
            }
            Some(actual)
        }
        None => {
            if signed_by.is_none() {
                tracing::warn!(
                    %url,
                    "installing plugin bundle from a URL with no checksum verification \
                     (allow_unverified opt-out) — the download is trusted on HTTPS alone"
                );
            }
            None
        }
    };

    let manifest = if let Some(kind) = detect_archive_kind(url) {
        extract_archive(
            bytes,
            kind,
            staging_dir.to_path_buf(),
            max_decompressed_bytes,
        )
        .await?;
        flatten_if_single_subdir(staging_dir).await?;
        read_and_parse_manifest(staging_dir).await?
    } else {
        let raw = String::from_utf8(bytes).map_err(|_| {
            PluginError::InvalidManifest(format!("manifest at '{url}' is not valid UTF-8"))
        })?;
        tokio::fs::create_dir_all(staging_dir).await?;
        tokio::fs::write(staging_dir.join("plugin.json"), &raw).await?;
        PluginManifest::parse_str(&raw)?
    };

    Ok(FetchedBundle {
        manifest,
        verified_sha256,
        signed_by,
        insecure_aggregate,
    })
}

/// Fetch `<url>.sig` and verify it against `bundle_bytes` for every
/// `algorithm: "ed25519"` entry in `trusted_keys`. Returns the label of the
/// FIRST trusted key the signature verifies against, or `None` if the
/// sidecar is missing/unfetchable, malformed (not 128 hex chars — a raw
/// 64-byte ed25519 signature, trailing whitespace trimmed), or does not
/// verify against any trusted key. All of those failure modes are treated
/// identically on purpose: an attacker serving a garbage/absent `.sig` must
/// not be distinguishable from "genuinely unsigned" by anything this
/// function returns.
async fn fetch_and_verify_signature(
    client: &reqwest::Client,
    url: &str,
    bundle_bytes: &[u8],
    trusted_keys: &[bamboo_config::TrustedKey],
) -> Option<String> {
    // Plain release-asset URLs (no query string) are the supported case: this
    // just appends `.sig`, which would misplace the suffix AFTER a query
    // string on a URL like `.../plugin.json?token=...` (`.../plugin.json?token=....sig`,
    // not the sidecar). Plugin bundle URLs are typically bare release-asset
    // URLs with no query string; if that ever needs to change, insert `.sig`
    // before the `?` instead of blindly appending it.
    let sig_url = format!("{url}.sig");
    let sig_bytes = download_bytes(client, &sig_url, MAX_SIGNATURE_DOWNLOAD_BYTES)
        .await
        .ok()?;
    let sig_text = String::from_utf8(sig_bytes).ok()?;
    let sig_raw = hex::decode(sig_text.trim()).ok()?;
    let sig_array: [u8; 64] = sig_raw.try_into().ok()?;
    let signature = ed25519_dalek::Signature::from_bytes(&sig_array);

    for key in trusted_keys {
        if !key.algorithm.eq_ignore_ascii_case("ed25519") {
            continue;
        }
        let Ok(pub_raw) = hex::decode(&key.public_key) else {
            continue;
        };
        let Ok(pub_array) = <[u8; 32]>::try_from(pub_raw.as_slice()) else {
            continue;
        };
        let Ok(verifying_key) = ed25519_dalek::VerifyingKey::from_bytes(&pub_array) else {
            continue;
        };
        if verifying_key.verify(bundle_bytes, &signature).is_ok() {
            return Some(key.label.clone());
        }
    }
    None
}

/// Per-platform binary artifact fetch (URL source only). Fetches the
/// artifact declared for [`Platform::current`] (a no-op if the manifest
/// declares none for this platform — not every plugin needs a binary),
/// verifies its sha256 BEFORE unpacking, and places the single expected
/// executable at `<staging_dir>/bin/<platform>/<id>[.exe]`.
///
/// This stays as defense in depth alongside [`fetch_manifest_bundle`]'s
/// bundle-sha256 check (see the module docs): the artifact hash is declared
/// INSIDE the manifest, so on its own it only proves "the binary matches
/// what this bundle's manifest says", not "this bundle itself is what the
/// caller expected" — that's what the bundle-level check now provides. The
/// verified artifact sha256 is therefore no longer surfaced to the caller
/// (it used to double as [`PluginSource::Url`]'s provenance hash before the
/// bundle-level check existed); this function's contract is purely
/// verify-then-place.
async fn fetch_and_place_artifact(
    manifest: &PluginManifest,
    staging_dir: &Path,
    max_decompressed_bytes: u64,
) -> PluginResult<()> {
    let Some(platform) = Platform::current() else {
        return Ok(());
    };
    let Some(artifact) = manifest.artifacts.get(platform.as_str()) else {
        return Ok(());
    };

    // The artifact's sha256 is verified BELOW, unconditionally (no bypass
    // flag exists for it — see this function's docs) — the downloaded bytes
    // are always cryptographically authenticated here, so redirects are
    // always safe to follow for this fetch.
    let bytes = download_bytes(
        http_client_following_redirects(),
        &artifact.url,
        MAX_DOWNLOAD_BYTES,
    )
    .await?;
    let actual_sha256 = sha256_hex(&bytes);
    if !actual_sha256.eq_ignore_ascii_case(&artifact.sha256) {
        return Err(PluginError::ArtifactVerificationFailed(format!(
            "sha256 mismatch for '{}': manifest declares {}, downloaded bytes hash to {}",
            artifact.url, artifact.sha256, actual_sha256
        )));
    }

    let kind = detect_archive_kind(&artifact.url).ok_or_else(|| {
        PluginError::InvalidManifest(format!(
            "artifact url '{}' is not a .zip/.tar.gz/.tgz",
            artifact.url
        ))
    })?;

    let scratch_dir = staging_dir.join(format!(".artifact-scratch-{}", platform.as_str()));
    extract_archive(bytes, kind, scratch_dir.clone(), max_decompressed_bytes).await?;

    let expected_name = if matches!(platform, Platform::Windows) {
        format!("{}.exe", manifest.id)
    } else {
        manifest.id.clone()
    };
    let source_bin = scratch_dir.join(&expected_name);
    if !tokio::fs::try_exists(&source_bin).await.unwrap_or(false) {
        let _ = tokio::fs::remove_dir_all(&scratch_dir).await;
        return Err(PluginError::InvalidManifest(format!(
            "artifact archive for platform '{}' does not contain the expected root executable '{}'",
            platform.as_str(),
            expected_name
        )));
    }

    let dest_dir = staging_dir.join("bin").join(platform.as_str());
    tokio::fs::create_dir_all(&dest_dir).await?;
    let dest_bin = dest_dir.join(&expected_name);
    move_file(&source_bin, &dest_bin).await?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = tokio::fs::metadata(&dest_bin).await?.permissions();
        perms.set_mode(0o755);
        tokio::fs::set_permissions(&dest_bin, perms).await?;
    }

    let _ = tokio::fs::remove_dir_all(&scratch_dir).await;
    Ok(())
}

/// `rename`, falling back to copy+remove across a device boundary.
async fn move_file(source: &Path, dest: &Path) -> PluginResult<()> {
    if tokio::fs::rename(source, dest).await.is_ok() {
        return Ok(());
    }
    let data = tokio::fs::read(source).await?;
    tokio::fs::write(dest, data).await?;
    tokio::fs::remove_file(source).await?;
    Ok(())
}

// ---------------------------------------------------------------------
// HTTP fetch
// ---------------------------------------------------------------------

/// Client used whenever the downloaded bytes WILL be cryptographically
/// authenticated — a signature is required (`!allow_unsigned`) or a `sha256`
/// pin was supplied. Following redirects is safe here: whichever host
/// actually served the final bytes, the signature/checksum check downstream
/// refuses on a bad result regardless — this is what lets the default
/// official-signed-via-CDN flow (GitHub Releases 302-redirecting to
/// `objects.githubusercontent.com`) and the checksummed flow keep working.
/// Reuses the workspace's pinned (native-tls) `reqwest` — never construct a
/// second client/connector here (see `notify_sinks::ntfy`'s identical
/// pattern).
fn http_client_following_redirects() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::limited(10))
            .build()
            .expect("a reqwest client with only a redirect policy set always builds")
    })
}

/// Client used whenever NEITHER a signature nor a checksum will authenticate
/// the downloaded bytes (`allow_unsigned && sha256.is_none()` — the fully
/// opted-out "host-only trust" case). The host allowlist (layer 1) only
/// vetted the FIRST hop's `<host><path>`; a transparent redirect would let
/// the bytes actually come from anywhere, silently defeating the allowlist
/// as the sole control. Redirects are disabled so a server that tries to
/// redirect is refused outright (see [`download_bytes`]) rather than quietly
/// followed — the approved host must serve the bytes directly.
fn http_client_no_redirects() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("a reqwest client with only a redirect policy set always builds")
    })
}

/// Hard ceiling on any single plugin download (manifest, bundle, or binary
/// artifact archive). A malicious or misconfigured URL must not be able to
/// stream an unbounded body into memory (OOM DoS). 256 MiB is generous for a
/// plugin bundle + one platform binary while still bounding the worst case.
const MAX_DOWNLOAD_BYTES: u64 = 256 * 1024 * 1024;

/// Hard ceiling on the `.sig` sidecar fetch specifically ([`fetch_and_verify_signature`]).
/// A valid signature is exactly 128 hex chars (a raw 64-byte ed25519
/// signature, hex-encoded) — 4 KiB is already wildly generous. Capping it far
/// below [`MAX_DOWNLOAD_BYTES`] means a malicious/misconfigured host serving
/// a `.sig` route can't force multiple hundreds of MiB into memory before the
/// hex-decode simply fails on the (way too long) body.
const MAX_SIGNATURE_DOWNLOAD_BYTES: u64 = 4 * 1024;

/// Hard ceiling on the TOTAL decompressed bytes any single archive (zip or
/// tar.gz) may expand to across ALL of its entries combined. Complements
/// `MAX_DOWNLOAD_BYTES`, which only bounds the COMPRESSED bytes fetched over
/// the wire — a small, highly-compressible archive (a classic
/// decompression/"zip bomb") can still expand to many gigabytes on disk with
/// nothing capping the output side. Enforced incrementally DURING extraction
/// (see [`copy_capped`]) against the ACTUAL bytes read off the decompression
/// stream — never an entry's header-declared size, which a crafted archive
/// can misstate (a zip's `uncompressed_size` field in particular is pure
/// metadata the reader doesn't have to honor) — so a bomb is aborted close to
/// this ceiling rather than after it has already exhausted disk. 2 GiB is
/// generous for any legitimate plugin bundle (skills/prompts/workflows text
/// plus, at most, one platform binary) while still bounding the worst case.
const MAX_DECOMPRESSED_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Fetch `url` via `client`, capping the body at `max_bytes`. `client`'s
/// redirect policy is the caller's decision (see
/// [`http_client_following_redirects`] / [`http_client_no_redirects`]) — this
/// function additionally refuses outright, with [`PluginError::RedirectRefused`]
/// (a clean 403-family trust refusal, NOT a 500), if the FINAL response it
/// receives is itself still a redirect (3xx), which only happens when
/// `client` was built with `redirect::Policy::none()` and the server actually
/// tried to redirect: that means the request's trust flags decided the bytes
/// must come from the vetted host directly (see BLOCKER 1 in the source-trust
/// review / the module docs), so silently treating the redirect response as
/// the payload would defeat that decision.
async fn download_bytes(
    client: &reqwest::Client,
    url: &str,
    max_bytes: u64,
) -> PluginResult<Vec<u8>> {
    use futures::StreamExt;

    let response =
        client.get(url).send().await.map_err(|error| {
            PluginError::Registration(format!("failed to fetch '{url}': {error}"))
        })?;

    if response.status().is_redirection() {
        let status = response.status();
        // Surface the redirect TARGET (host) so the caller can decide whether
        // to trust it / add it to `trusted_hosts` — a redirect with no
        // `Location`, or one whose value isn't valid UTF-8, degrades to a
        // generic "(unspecified)" rather than failing differently.
        let location = response
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let target = location.as_deref().unwrap_or("(unspecified)");
        return Err(PluginError::RedirectRefused(format!(
            "refused to follow an HTTP redirect ({status}) from '{url}' to '{target}': for an \
             unverified install (no signature, no checksum) the approved host must serve the \
             bytes directly, so redirects are not followed — install from the canonical/final \
             URL, or provide a signature / `--sha256` (which authenticates the bytes regardless \
             of which host serves them), or add the redirect target's host to \
             `plugin_trust.trusted_hosts`"
        )));
    }

    let response = response.error_for_status().map_err(|error| {
        PluginError::Registration(format!("'{url}' returned an error status: {error}"))
    })?;

    // Reject up front if the server ADVERTISES an over-cap body (cheap, avoids
    // streaming at all)...
    if let Some(len) = response.content_length() {
        if len > max_bytes {
            return Err(PluginError::Registration(format!(
                "'{url}' advertises a {len}-byte body, over the {max_bytes}-byte download cap; \
                 refusing"
            )));
        }
    }

    // ...and ALSO cap the actually-streamed bytes, since Content-Length can be
    // absent (chunked) or a lie.
    let mut stream = response.bytes_stream();
    let mut buffer: Vec<u8> = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| {
            PluginError::Registration(format!("failed to read response body of '{url}': {error}"))
        })?;
        if buffer.len() as u64 + chunk.len() as u64 > max_bytes {
            return Err(PluginError::Registration(format!(
                "'{url}' streamed more than the {max_bytes}-byte download cap; aborting"
            )));
        }
        buffer.extend_from_slice(&chunk);
    }
    Ok(buffer)
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

// ---------------------------------------------------------------------
// Archive handling (path-traversal-safe)
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum ArchiveKind {
    Zip,
    TarGz,
}

fn detect_archive_kind(name_or_url: &str) -> Option<ArchiveKind> {
    let lower = name_or_url.to_ascii_lowercase();
    // Strip a query string/fragment before checking the extension, in case a
    // URL looks like `.../plugin.tar.gz?token=...`.
    let lower = lower.split(['?', '#']).next().unwrap_or(&lower).to_string();
    if lower.ends_with(".zip") {
        Some(ArchiveKind::Zip)
    } else if lower.ends_with(".tar.gz") || lower.ends_with(".tgz") {
        Some(ArchiveKind::TarGz)
    } else {
        None
    }
}

/// Extract `bytes` (a zip or tar.gz archive) into `dest_dir`, rejecting any
/// entry whose path would escape `dest_dir` (traversal / absolute paths), and
/// aborting if the TOTAL decompressed output across all entries would exceed
/// `max_decompressed_bytes` (decompression-bomb guard — see
/// [`MAX_DECOMPRESSED_BYTES`] / [`copy_capped`]). Runs the (synchronous)
/// extraction on a blocking thread.
async fn extract_archive(
    bytes: Vec<u8>,
    kind: ArchiveKind,
    dest_dir: PathBuf,
    max_decompressed_bytes: u64,
) -> PluginResult<()> {
    tokio::fs::create_dir_all(&dest_dir).await?;
    tokio::task::spawn_blocking(move || match kind {
        ArchiveKind::Zip => extract_zip_sync(&bytes, &dest_dir, max_decompressed_bytes),
        ArchiveKind::TarGz => extract_targz_sync(&bytes, &dest_dir, max_decompressed_bytes),
    })
    .await
    .map_err(|error| {
        PluginError::Registration(format!("archive extraction task panicked: {error}"))
    })?
}

/// Copy `reader` into `writer` in small, bounded chunks, tallying bytes into
/// `running_total` — which the caller carries ACROSS every entry in the
/// archive, so the cap is on the archive's total decompressed output, not
/// any one entry — and aborting the moment the cumulative count would exceed
/// `max_decompressed_bytes`. Chunked copying (rather than `std::io::copy`
/// followed by a size check afterward) keeps the amount ever actually
/// written to disk bounded near the cap even for a single maximally
/// compressible entry: the whole point of the cap is to stop a small archive
/// from exhausting disk, so only checking after a full `io::copy` completed
/// would defeat it.
fn copy_capped(
    reader: &mut impl std::io::Read,
    writer: &mut impl std::io::Write,
    running_total: &mut u64,
    max_decompressed_bytes: u64,
) -> PluginResult<()> {
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            return Ok(());
        }
        *running_total += bytes_read as u64;
        if *running_total > max_decompressed_bytes {
            return Err(PluginError::InvalidManifest(format!(
                "archive expands to more than the {max_decompressed_bytes}-byte decompressed \
                 size cap ({running_total} bytes and counting); refusing to unpack (possible \
                 decompression bomb)"
            )));
        }
        writer.write_all(&buffer[..bytes_read])?;
    }
}

fn extract_zip_sync(
    bytes: &[u8],
    dest_dir: &Path,
    max_decompressed_bytes: u64,
) -> PluginResult<()> {
    use std::io::Cursor;

    let cursor = Cursor::new(bytes);
    let mut archive = zip::ZipArchive::new(cursor)
        .map_err(|error| PluginError::InvalidManifest(format!("invalid zip archive: {error}")))?;

    let mut total_decompressed_bytes: u64 = 0;

    for index in 0..archive.len() {
        let mut file = archive.by_index(index).map_err(|error| {
            PluginError::InvalidManifest(format!("invalid zip entry at index {index}: {error}"))
        })?;
        // `enclosed_name()` is the zip crate's own traversal guard: it
        // returns `None` for any entry whose name contains `..`, is
        // absolute, or otherwise can't be safely joined under `dest_dir`.
        let Some(relative_path) = file.enclosed_name() else {
            return Err(PluginError::InvalidManifest(format!(
                "zip entry '{}' has an unsafe path (traversal/absolute) — refusing to unpack",
                file.name()
            )));
        };
        let out_path = dest_dir.join(&relative_path);
        if file.is_dir() {
            std::fs::create_dir_all(&out_path)?;
            continue;
        }
        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut out_file = std::fs::File::create(&out_path)?;
        if let Err(error) = copy_capped(
            &mut file,
            &mut out_file,
            &mut total_decompressed_bytes,
            max_decompressed_bytes,
        ) {
            drop(out_file);
            // Defense in depth: remove the partial file we were just writing
            // even though the source transaction also wipes the whole
            // staging directory on any `Err` from this function — a
            // direct caller of this lower-level helper should never see a
            // half-written entry either.
            let _ = std::fs::remove_file(&out_path);
            return Err(error);
        }
        drop(out_file);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Some(mode) = file.unix_mode() {
                std::fs::set_permissions(&out_path, std::fs::Permissions::from_mode(mode))?;
            }
        }
    }
    Ok(())
}

fn extract_targz_sync(
    bytes: &[u8],
    dest_dir: &Path,
    max_decompressed_bytes: u64,
) -> PluginResult<()> {
    use flate2::read::GzDecoder;
    use std::path::Component;
    use tar::{Archive, EntryType};

    let decoder = GzDecoder::new(bytes);
    let mut archive = Archive::new(decoder);
    let mut total_decompressed_bytes: u64 = 0;
    for entry_result in archive.entries()? {
        let mut entry = entry_result?;

        // SECURITY (symlink/hardlink escape): reject any Symlink or HardLink
        // entry outright BEFORE unpacking. `entry.unpack()` is the raw tar API
        // — it validates the entry's OWN path (checked below) but NOT a link
        // entry's TARGET (`link_name`), which is fully attacker-controlled and
        // may be absolute or contain `..`. A malicious bundle could ship
        // e.g. `workflows/evil.md` as a symlink to `~/.ssh/id_rsa` or bamboo's
        // `config.json`; a later `fs::read_to_string` (register_workflows) would
        // follow it and copy the victim's real content into a plugin-visible
        // location = arbitrary file exfiltration, and `flatten_if_single_subdir`
        // following a symlink-to-a-real-dir could rename/destroy the victim's
        // files. A plugin bundle has no legitimate reason to ship a link (same
        // rationale as `copy_dir_recursive` skipping symlinks), so refuse the
        // whole archive. (Zip is not affected: `extract_zip_sync` writes every
        // entry as a fresh regular file via `copy_capped`, so an archived
        // "symlink" lands inert as a plain file.)
        let entry_type = entry.header().entry_type();
        if matches!(entry_type, EntryType::Symlink | EntryType::Link) {
            let link_target = entry
                .link_name()
                .ok()
                .flatten()
                .map(|path| path.display().to_string())
                .unwrap_or_default();
            return Err(PluginError::InvalidManifest(format!(
                "tar entry '{}' is a {} (target '{link_target}') — plugin bundles must not ship \
                 links; refusing to unpack",
                entry
                    .path()
                    .map(|p| p.display().to_string())
                    .unwrap_or_default(),
                if entry_type == EntryType::Symlink {
                    "symlink"
                } else {
                    "hardlink"
                },
            )));
        }

        let relative_path = entry.path()?.into_owned();
        let is_unsafe = relative_path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        });
        if is_unsafe {
            return Err(PluginError::InvalidManifest(format!(
                "tar entry '{}' has an unsafe path (traversal/absolute) — refusing to unpack",
                relative_path.display()
            )));
        }
        let out_path = dest_dir.join(&relative_path);

        // Directories carry no content to cap — just create and move on
        // (mirrors `entry.unpack()`'s own directory handling, which this
        // function replaces for content-bearing entries below so the
        // decompressed-size cap can be enforced incrementally; see
        // `copy_capped`).
        if entry_type.is_dir() {
            std::fs::create_dir_all(&out_path)?;
            continue;
        }

        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut out_file = std::fs::File::create(&out_path)?;
        if let Err(error) = copy_capped(
            &mut entry,
            &mut out_file,
            &mut total_decompressed_bytes,
            max_decompressed_bytes,
        ) {
            drop(out_file);
            // Defense in depth: see the identical cleanup in
            // `extract_zip_sync` — the whole staging dir is also wiped by
            // the caller, but a direct caller of this helper shouldn't see
            // a half-written entry either.
            let _ = std::fs::remove_file(&out_path);
            return Err(error);
        }
        drop(out_file);

        // Preserve the entry's permission bits (matches `entry.unpack()`'s
        // own behaviour, which this manual copy replaces).
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Ok(mode) = entry.header().mode() {
                std::fs::set_permissions(&out_path, std::fs::Permissions::from_mode(mode))?;
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------
// Filesystem helpers
// ---------------------------------------------------------------------

async fn read_and_parse_manifest(dir: &Path) -> PluginResult<PluginManifest> {
    let manifest_path = dir.join("plugin.json");
    let raw = tokio::fs::read_to_string(&manifest_path)
        .await
        .map_err(|_| {
            PluginError::InvalidManifest(format!(
                "no plugin.json found at '{}'",
                manifest_path.display()
            ))
        })?;
    PluginManifest::parse_str(&raw)
}

/// If `dir` has no `plugin.json` of its own but contains EXACTLY one
/// subdirectory, move that subdirectory's contents up into `dir` (the common
/// `tar czf bundle.tar.gz plugin-name/`-style archive convention). A no-op if
/// `plugin.json` is already present, or if the shape doesn't match (multiple
/// top-level entries, or a single top-level entry that isn't a directory) —
/// in either case, [`read_and_parse_manifest`] will simply fail to find
/// `plugin.json` afterwards with a clear error.
async fn flatten_if_single_subdir(dir: &Path) -> PluginResult<()> {
    if tokio::fs::try_exists(dir.join("plugin.json"))
        .await
        .unwrap_or(false)
    {
        return Ok(());
    }

    let mut entries = tokio::fs::read_dir(dir).await?;
    let mut only_entry: Option<PathBuf> = None;
    let mut count = 0usize;
    while let Some(entry) = entries.next_entry().await? {
        count += 1;
        if count > 1 {
            return Ok(());
        }
        only_entry = Some(entry.path());
    }
    let Some(candidate) = only_entry else {
        return Ok(());
    };
    // `symlink_metadata` (does NOT follow links), not `metadata`: defense in
    // depth so a symlink-to-a-real-dir can never pass `is_dir()` here and get
    // its real children renamed out. Extraction already rejects link entries
    // (see `extract_targz_sync`) and `copy_dir_recursive` skips symlinks, so
    // in practice `candidate` is always a real dir/file — but never follow a
    // link when deciding whether to descend into and move a directory's
    // contents.
    if !tokio::fs::symlink_metadata(&candidate).await?.is_dir() {
        return Ok(());
    }

    // Move `candidate`'s children up into `dir`, then remove the now-empty
    // `candidate` directory.
    let mut children = tokio::fs::read_dir(&candidate).await?;
    while let Some(child) = children.next_entry().await? {
        let dest = dir.join(child.file_name());
        tokio::fs::rename(child.path(), dest).await?;
    }
    tokio::fs::remove_dir(&candidate).await?;
    Ok(())
}

/// Recursively copy `source` into `dest` (creating `dest`).
fn copy_dir_recursive<'a>(
    source: &'a Path,
    dest: &'a Path,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = PluginResult<()>> + Send + 'a>> {
    Box::pin(async move {
        tokio::fs::create_dir_all(dest).await?;
        let mut entries = tokio::fs::read_dir(source).await?;
        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            let dest_path = dest.join(entry.file_name());
            if file_type.is_dir() {
                copy_dir_recursive(&entry.path(), &dest_path).await?;
            } else if file_type.is_file() {
                tokio::fs::copy(entry.path(), &dest_path).await?;
            }
            // Symlinks are intentionally skipped — a plugin bundle has no
            // legitimate reason to ship one, and following it could escape
            // the source directory.
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests;
