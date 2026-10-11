//! Reviewed first-party metadata over the existing plugin install authority.
//! No memory server is registered: external clients share the active store.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use actix_web::{web, HttpResponse};
use bamboo_config::{PluginTrustConfig, PluginTrustEnforcement, JIANDU_PLUGIN_PUBLIC_KEY};
use bamboo_plugin::{
    InstallDisposition, InstalledPlugin, InstalledPlugins, PluginInstallStatus, PluginSource,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

use super::{api_types::to_view, errors::plugin_error_response};
use crate::app_state::AppState;
use crate::plugin_installer::ServerPluginInstaller;
use crate::plugin_source::{
    install_server_plugin_from_source_with_event_sink_grants, PluginSourceInput,
};

#[derive(Clone, Deserialize, Serialize)]
struct Release {
    version: String,
    manifest_url: String,
    sha256: String,
    #[serde(skip_serializing)]
    manifest_sha256: String,
    #[serde(skip_serializing)]
    executable_sha256: BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct Catalog {
    release: Option<Release>,
}

fn accepted_release() -> Option<Release> {
    // Populated only from a published, signature-verified release receipt.
    serde_json::from_str::<Catalog>(include_str!("official-jiandu.json"))
        .expect("reviewed Jiandu catalog is valid JSON")
        .release
}

#[derive(Clone, Serialize)]
struct Host {
    os: String,
    arch: String,
    libc: Option<String>,
    supported: bool,
    reason: Option<&'static str>,
}

impl Host {
    fn detect() -> Self {
        Self::for_platform(
            std::env::consts::OS,
            std::env::consts::ARCH,
            runtime_glibc(),
        )
    }

    fn for_platform(os: &str, arch: &str, libc: Option<String>) -> Self {
        let reason = match (os, arch) {
            ("macos", "x86_64" | "aarch64") | ("windows", "x86_64") => None,
            ("linux", "x86_64") if libc.as_deref().is_some_and(glibc_supported) => None,
            ("linux", "x86_64") => Some("glibc_2_35_required"),
            _ => Some("unsupported_host"),
        };
        Self {
            os: os.into(),
            arch: arch.into(),
            libc,
            supported: reason.is_none(),
            reason,
        }
    }
}

fn glibc_supported(version: &str) -> bool {
    let mut parts = version.split('.');
    match (
        parts.next().and_then(|v| v.parse::<u32>().ok()),
        parts.next().and_then(|v| v.parse::<u32>().ok()),
    ) {
        (Some(major), Some(minor)) => (major, minor) >= (2, 35),
        _ => false,
    }
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn runtime_glibc() -> Option<String> {
    // glibc returns a process-lifetime NUL-terminated string. No subprocess or PATH.
    Some(
        unsafe { std::ffi::CStr::from_ptr(libc::gnu_get_libc_version()) }
            .to_string_lossy()
            .into_owned(),
    )
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn runtime_glibc() -> Option<String> {
    None
}

#[derive(Serialize)]
struct Availability {
    available: bool,
    reason: Option<&'static str>,
}
#[derive(Serialize)]
struct Trust {
    trusted: bool,
    reason: Option<&'static str>,
}
#[derive(Serialize)]
struct Installed {
    state: &'static str,
    version: Option<String>,
    reason: Option<&'static str>,
}
#[derive(Serialize)]
struct ExternalClient {
    command: String,
    args: Vec<String>,
}
#[derive(Serialize)]
struct Setup {
    plugin_id: &'static str,
    host: Host,
    release: Option<Release>,
    trust: Trust,
    install: Availability,
    installed: Installed,
    external_client: Option<ExternalClient>,
}

fn trust_reason(trust: &PluginTrustConfig, release: Option<&Release>) -> Option<&'static str> {
    if !trust.trusted_keys.iter().any(|key| {
        key.algorithm.eq_ignore_ascii_case("ed25519")
            && key
                .public_key
                .eq_ignore_ascii_case(JIANDU_PLUGIN_PUBLIC_KEY)
    }) {
        return Some("publisher_not_trusted");
    }
    if let Some(release) = release {
        if !trust.is_host_trusted(&release.manifest_url) {
            return Some("source_not_trusted");
        }
    }
    None
}

async fn setup(
    state: &web::Data<AppState>,
    host: Host,
    release: Option<Release>,
) -> Result<Setup, bamboo_plugin::PluginError> {
    let installer = ServerPluginInstaller::new(state.clone());
    // Same lock as install/update/remove: no replacement while reading provenance/files.
    let _guard = installer.begin_operation().await;
    let registry =
        InstalledPlugins::load(&state.app_data_dir.join("plugins/installed.json")).await?;
    let entry = registry.get_unique("jiandu")?;
    let memory_reason = installer
        .jiandu_memory_path_reason(&state.app_data_dir.join("plugins/jiandu"))
        .await;
    let trust = state.config.read().await.plugin_trust.clone();
    let trust_reason = trust_reason(&trust, release.as_ref());
    let mut installed = Installed {
        state: "missing",
        version: None,
        reason: None,
    };
    let mut external_client = None;
    if let Some(entry) = entry {
        installed.version = Some(entry.version.clone());
        if entry.status == PluginInstallStatus::Installing {
            installed.state = "installing";
            installed.reason = Some("incomplete_install");
        } else {
            installed.state = "unverified";
            installed.reason = Some("unverified_install");
            if host.supported && trust_reason.is_none() && memory_reason.is_none() {
                if let Some(release) = &release {
                    if let Ok(command) = validated_executable(state, entry, release, &host.os).await
                    {
                        let root = state.memory_store.data_dir();
                        // Preserve the active resolver's root, including a custom root.
                        // An unrepresentable/relative root is not exported to another process.
                        if let (Some(command), Some(root)) = (
                            command.to_str(),
                            root.to_str().filter(|_| root.is_absolute()),
                        ) {
                            installed.state = "ready";
                            installed.reason = None;
                            external_client = Some(ExternalClient {
                                command: command.into(),
                                args: vec!["--data-dir".into(), root.into()],
                            });
                        } else {
                            installed.reason = Some("memory_root_unavailable");
                        }
                    } else {
                        installed.reason = Some("invalid_managed_files");
                    }
                }
            }
        }
    }
    if let Some(reason) = memory_reason {
        installed.reason = Some(reason);
    }
    let unavailable = host
        .reason
        .or(memory_reason)
        .or(trust_reason)
        .or_else(|| release.is_none().then_some("artifact_unavailable"))
        .or_else(|| {
            entry.map(|_| {
                if installed.state == "installing" {
                    "incomplete_install"
                } else {
                    "already_installed"
                }
            })
        });
    Ok(Setup {
        plugin_id: "jiandu",
        host,
        release,
        trust: Trust {
            trusted: trust_reason.is_none(),
            reason: trust_reason,
        },
        install: Availability {
            available: unavailable.is_none(),
            reason: unavailable,
        },
        installed,
        external_client,
    })
}

fn valid_provenance(entry: &InstalledPlugin, release: &Release) -> bool {
    entry.version == release.version
        && entry.registered.is_empty()
        && matches!(&entry.source,
        PluginSource::Url { url, sha256: Some(hash), signed_by: Some(_), allow_unverified: false, allow_unsigned: false, allow_untrusted_host: false, insecure: false }
        if url == &release.manifest_url && hash.eq_ignore_ascii_case(&release.sha256))
}

fn plain(metadata: &std::fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if metadata.file_attributes() & 0x400 != 0 {
            return false;
        }
    }
    !metadata.file_type().is_symlink()
}

async fn regular_hash(path: &Path, expected: &str, max: u64) -> Result<(), ()> {
    let metadata = tokio::fs::symlink_metadata(path).await.map_err(|_| ())?;
    if !plain(&metadata) || !metadata.is_file() || metadata.len() == 0 || metadata.len() > max {
        return Err(());
    }
    let mut reader = tokio::fs::File::open(path)
        .await
        .map_err(|_| ())?
        .take(max + 1);
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 16384];
    let mut total = 0;
    loop {
        let read = reader.read(&mut buffer).await.map_err(|_| ())?;
        if read == 0 {
            break;
        }
        total += read as u64;
        if total > max {
            return Err(());
        }
        hasher.update(&buffer[..read]);
    }
    if total != metadata.len() || !hex::encode(hasher.finalize()).eq_ignore_ascii_case(expected) {
        return Err(());
    }
    Ok(())
}

async fn validated_executable(
    state: &AppState,
    entry: &InstalledPlugin,
    release: &Release,
    os: &str,
) -> Result<PathBuf, ()> {
    if !valid_provenance(entry, release) {
        return Err(());
    }
    let digest = release.executable_sha256.get(os).ok_or(())?;
    let root = tokio::fs::canonicalize(&state.app_data_dir)
        .await
        .map_err(|_| ())?;
    let mut directory = root.clone();
    for component in ["plugins", "jiandu", "bin", os] {
        directory.push(component);
        let metadata = tokio::fs::symlink_metadata(&directory)
            .await
            .map_err(|_| ())?;
        if !plain(&metadata) || !metadata.is_dir() {
            return Err(());
        }
    }
    let expected_dir = state.app_data_dir.join("plugins/jiandu");
    if entry.plugin_dir != expected_dir
        || tokio::fs::canonicalize(&entry.plugin_dir)
            .await
            .map_err(|_| ())?
            != root.join("plugins/jiandu")
    {
        return Err(());
    }
    regular_hash(
        &root.join("plugins/jiandu/plugin.json"),
        &release.manifest_sha256,
        64 * 1024,
    )
    .await?;
    let executable = directory.join(if os == "windows" {
        "jiandu.exe"
    } else {
        "jiandu"
    });
    regular_hash(&executable, digest, 128 * 1024 * 1024).await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if tokio::fs::metadata(&executable)
            .await
            .map_err(|_| ())?
            .permissions()
            .mode()
            & 0o111
            == 0
        {
            return Err(());
        }
    }
    Ok(executable)
}

/// Passive host/store/package metadata. Does not launch a process or create a store.
pub async fn jiandu_setup(state: web::Data<AppState>) -> HttpResponse {
    match setup(&state, Host::detect(), accepted_release()).await {
        Ok(report) => HttpResponse::Ok().json(report),
        Err(error) => plugin_error_response(&error),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OfficialInstallRequest {}

/// Fixed-source strict installation; callers cannot provide paths, URLs or bypass flags.
pub async fn install_official_jiandu(
    state: web::Data<AppState>,
    _body: web::Json<OfficialInstallRequest>,
) -> HttpResponse {
    let release = accepted_release();
    let report = match setup(&state, Host::detect(), release.clone()).await {
        Ok(report) => report,
        Err(error) => return plugin_error_response(&error),
    };
    if !report.install.available {
        return HttpResponse::Conflict().json(serde_json::json!({"error": {"type":"official_plugin_unavailable", "reason": report.install.reason}}));
    }
    let release = release.expect("availability requires an accepted release");
    let mut trust = state.config.read().await.plugin_trust.clone();
    // Recheck live policy after setup; no user-edited key list is augmented.
    if let Some(reason) = trust_reason(&trust, Some(&release)) {
        return HttpResponse::Forbidden().json(
            serde_json::json!({"error":{"type":"official_plugin_unavailable","reason":reason}}),
        );
    }
    trust.enforcement = PluginTrustEnforcement::Strict;
    trust.trusted_keys.retain(|key| {
        key.algorithm.eq_ignore_ascii_case("ed25519")
            && key
                .public_key
                .eq_ignore_ascii_case(JIANDU_PLUGIN_PUBLIC_KEY)
    });
    let input = PluginSourceInput::Url {
        url: release.manifest_url,
        sha256: Some(release.sha256),
        allow_unverified: false,
        allow_untrusted_host: false,
        allow_unsigned: false,
        insecure: false,
    };
    let installer = ServerPluginInstaller::new(state.clone());
    match install_server_plugin_from_source_with_event_sink_grants(
        &installer,
        input,
        &state.app_data_dir.join("plugins"),
        &trust,
        InstallDisposition::FailIfInstalled,
        Some("jiandu"),
        None,
    )
    .await
    {
        Ok(entry) => HttpResponse::Created()
            .json(to_view(entry, &state.service_manager, &state.tool_event_router).await),
        Err(error) => plugin_error_response(&error),
    }
}

#[cfg(test)]
mod tests;
