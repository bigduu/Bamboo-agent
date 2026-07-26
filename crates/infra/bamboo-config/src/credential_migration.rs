//! Recoverable provider/MCP/external-broker credential extraction.
//!
//! Provider/MCP/root and external-broker planners are independent so a bad
//! optional broker document cannot disable the main configuration. Both use
//! one lock and manifest protocol: candidate bytes are staged and fsynced
//! before the commit point, and every reader recovers a committed transaction
//! before reading any credential transaction member.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::OsStr;
use std::fs::File;
#[cfg(not(unix))]
use std::fs::OpenOptions;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, Weak};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    credential_ref, AtomicFileStore, ConfigStoreError, ConfigStoreResult, CredentialRef,
    CredentialStore, ProviderConfigs,
};

const MIGRATION_VERSION: u32 = 1;
const SECTION_LAYOUT_COMPLETION_LEDGER_VERSION: u32 = 1;
const MANIFEST_FILE: &str = "config-credential-migration.json";
const JOURNAL_FILE: &str = "config-credential-migration.journal.json";
pub(crate) const SECTION_LAYOUT_COMPLETION_FILE: &str = "config-section-layout-completion.json";
const LOCK_FILE: &str = ".config-credential-migration.lock";
const STAGE_PREFIX: &str = ".config-credential-stage-v1-";
const BACKUP_PREFIX: &str = "config-credential-migration-backup-v1-";
const PROVIDERS_FILE: &str = "providers.json";
const MCP_FILE: &str = "mcp.json";
const BROKER_FILE: &str = "broker.json";
const CREDENTIALS_FILE: &str = "credentials.json";
const CONFIG_FILE: &str = "config.json";
const CORE_FILE: &str = "core.json";
const TOOLS_SKILLS_FILE: &str = "tools-skills.json";
const MEMORY_FILE: &str = "memory.json";
const SUBAGENTS_FILE: &str = "subagents.json";
const NOTIFICATIONS_FILE: &str = "notifications.json";
const CONNECT_FILE: &str = "connect.json";
const CLUSTER_FABRIC_FILE: &str = "cluster-fabric.json";
const ENV_FILE: &str = "env.json";
const ACCESS_CONTROL_FILE: &str = "access-control.json";
const HOOKS_FILE: &str = "hooks.json";
const MODEL_POLICY_FILE: &str = "model-policy.json";
const MODEL_LIMITS_FILE: &str = "model_limits.json";
const PROXY_AUTH_DOMAIN_KEYS: [&str; 5] = [
    "proxy_auth",
    "proxy_auth_encrypted",
    "http_proxy_auth_encrypted",
    "https_proxy_auth_encrypted",
    "proxy_auth_credential_ref",
];

static PROCESS_MIGRATION_LOCKS: LazyLock<Mutex<HashMap<PathBuf, Weak<Mutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CredentialMigrationOutcome {
    pub migrated_credentials: usize,
    pub resumed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SectionMigrationOutcome {
    pub activated: bool,
    pub resumed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MigrationManifest {
    version: u32,
    transaction_id: String,
    stage_dir: String,
    state: MigrationState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    exact_scope: Option<ExactTransactionScope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    migration_scope: Option<MigrationScope>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    source_attestation: BTreeMap<String, FacadeSourceBase>,
    files: Vec<StagedFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case")]
enum FacadeSourceBase {
    Missing,
    Regular { sha256: String },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum MigrationState {
    Pending,
    Aborting,
    Complete,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct SectionLayoutCompletionLedger {
    version: u32,
    state: MigrationState,
    #[serde(default)]
    facade_compound: bool,
    marker_sha256: String,
    completion: crate::section_facade::SectionLayoutCompletion,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    legacy_cleanup: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    legacy_source_guards: BTreeMap<String, FacadeSourceBase>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ExactTransactionScope {
    Providers,
    Mcp,
    ProxyAuth,
    EnvVars,
    Notifications,
    Connect,
    AccessControl,
    ClusterFabric,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum MigrationScope {
    ClusterFabric,
    ProviderFacade,
    SectionSplit,
    FacadeCompound,
}

fn is_section_layout_scope(scope: Option<MigrationScope>) -> bool {
    matches!(
        scope,
        Some(MigrationScope::SectionSplit | MigrationScope::FacadeCompound)
    )
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StagedFile {
    name: String,
    staged_name: String,
    sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    original_sha256: Option<String>,
    /// Distinguishes a missing base from an existing empty regular file.
    /// `None` is reserved for manifests written before presence-aware CAS.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    original_present: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    migration_generation: Option<u64>,
    sensitive: bool,
    #[serde(default)]
    install_mode: InstallMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expected_revision: Option<u64>,
    /// Immutable hash of the original credential document used for three-way
    /// merge. `original_sha256` advances after each rebase CAS attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    transaction_base_sha256: Option<String>,
    /// Exact credential transactions use these refs for a three-way rebase.
    /// Empty on migration members and on manifests produced before this field.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    touched_credential_refs: Vec<String>,
    /// Metadata publication is forbidden until each of these refs decrypts.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    required_credential_refs: Vec<String>,
    /// Exact env transactions use names for per-entry three-way config merge.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    touched_env_names: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum InstallMode {
    #[default]
    Migration,
    Exact,
    LegacyCleanup,
    SourceGuard,
}

#[derive(Debug)]
struct PlannedSection {
    name: &'static str,
    bytes: Vec<u8>,
    original: Vec<u8>,
    migration_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FacadeSourceState {
    Missing,
    Regular(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FacadeSourceSnapshot {
    files: BTreeMap<String, FacadeSourceState>,
}

impl FacadeSourceSnapshot {
    pub(crate) fn capture(data_dir: &Path) -> ConfigStoreResult<Self> {
        let mut files = BTreeMap::new();
        for name in crate::section_facade::preflight_source_names() {
            let state = match read_optional_target(&data_dir.join(&name))? {
                Some(bytes) => FacadeSourceState::Regular(bytes),
                None => FacadeSourceState::Missing,
            };
            files.insert(name, state);
        }
        Ok(Self { files })
    }

    pub(crate) fn state(&self, name: &str) -> ConfigStoreResult<&FacadeSourceState> {
        self.files.get(name).ok_or_else(|| {
            ConfigStoreError::Validation(format!(
                "facade source snapshot is missing allowlisted member '{name}'"
            ))
        })
    }

    pub(crate) fn bytes(&self, name: &str) -> ConfigStoreResult<Option<&[u8]>> {
        Ok(match self.state(name)? {
            FacadeSourceState::Missing => None,
            FacadeSourceState::Regular(bytes) => Some(bytes),
        })
    }

    fn attestation(&self) -> BTreeMap<String, FacadeSourceBase> {
        self.files
            .iter()
            .map(|(name, state)| {
                let base = match state {
                    FacadeSourceState::Missing => FacadeSourceBase::Missing,
                    FacadeSourceState::Regular(bytes) => FacadeSourceBase::Regular {
                        sha256: sha256(bytes),
                    },
                };
                (name.clone(), base)
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PlannedLegacyCleanup {
    pub name: String,
    pub original: FacadeSourceState,
    pub candidate: FacadeSourceState,
    pub sensitive: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct FacadeCredentialPlan {
    pub source_snapshot: FacadeSourceSnapshot,
    pub credential_original: FacadeSourceState,
    pub credential_candidate: Vec<u8>,
    pub source_overrides: BTreeMap<String, Vec<u8>>,
    pub legacy_cleanups: Vec<PlannedLegacyCleanup>,
}

#[derive(Debug, Clone, Default)]
struct FacadeRootSecretAuthority {
    providers: BTreeSet<String>,
    provider_instances: bool,
    mcp: bool,
    mcp_servers: BTreeSet<String>,
    connect_platforms: BTreeSet<String>,
    access_control: bool,
}

fn provider_document_authority(bytes: &[u8]) -> ConfigStoreResult<(BTreeSet<String>, bool)> {
    let mut root: Value = serde_json::from_slice(bytes)?;
    let (data, _) = section_data_mut(&mut root)?;
    let object = data.as_object().ok_or_else(|| {
        ConfigStoreError::Validation("provider section must be an object".to_string())
    })?;
    let providers = ["openai", "anthropic", "gemini", "bodhi"]
        .into_iter()
        .filter(|provider| object.contains_key(*provider))
        .map(str::to_string)
        .collect();
    Ok((providers, object.contains_key("provider_instances")))
}

fn mcp_document_server_ids(bytes: &[u8]) -> ConfigStoreResult<BTreeSet<String>> {
    let mut root: Value = serde_json::from_slice(bytes)?;
    let (data, _) = section_data_mut(&mut root)?;
    if let Some(servers) = data.get("servers").and_then(Value::as_array) {
        return servers
            .iter()
            .map(|server| {
                server
                    .get("id")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .ok_or_else(|| {
                        ConfigStoreError::Validation("MCP server id is missing".to_string())
                    })
            })
            .collect();
    }
    let servers = data
        .as_object()
        .ok_or_else(|| ConfigStoreError::Validation("MCP section must be an object".to_string()))?;
    Ok(servers.keys().cloned().collect())
}

fn discard_shadowed_root_secrets(
    extracted: &mut Vec<ExtractedSecret>,
    authority: &FacadeRootSecretAuthority,
) {
    extracted.retain(|secret| match secret.kind {
        ExtractedSecretKind::ProviderBuiltin => !secret
            .provider_owner
            .as_ref()
            .is_some_and(|provider| authority.providers.contains(provider)),
        ExtractedSecretKind::ProviderInstance => !authority.provider_instances,
        ExtractedSecretKind::Mcp => !authority.mcp,
        ExtractedSecretKind::Connect => !secret
            .connect_owner
            .as_ref()
            .is_some_and(|platform| authority.connect_platforms.contains(platform)),
        ExtractedSecretKind::AccessControl => !authority.access_control,
        _ => true,
    });
}

fn discard_shadowed_sidecar_backup_secrets(
    extracted: &mut Vec<ExtractedSecret>,
    authority: &FacadeRootSecretAuthority,
) {
    extracted.retain(|secret| match secret.kind {
        ExtractedSecretKind::ProviderBuiltin => !secret
            .provider_owner
            .as_ref()
            .is_some_and(|provider| authority.providers.contains(provider)),
        ExtractedSecretKind::ProviderInstance => !authority.provider_instances,
        ExtractedSecretKind::Mcp => !secret
            .mcp_owner
            .as_ref()
            .is_some_and(|server| authority.mcp_servers.contains(server)),
        ExtractedSecretKind::Connect => !secret
            .connect_owner
            .as_ref()
            .is_some_and(|platform| authority.connect_platforms.contains(platform)),
        ExtractedSecretKind::AccessControl => !authority.access_control,
        _ => true,
    });
}

fn scrub_mcp_credential_metadata(value: &mut Value) -> bool {
    let Some(object) = value.as_object_mut() else {
        return false;
    };
    let mut changed = object.remove("env_credential_refs").is_some()
        | object.remove("header_credential_refs").is_some();
    if let Some(headers) = object.get_mut("headers").and_then(Value::as_array_mut) {
        for header in headers.iter_mut().filter_map(Value::as_object_mut) {
            changed |= header.remove("credential_ref").is_some();
            changed |= header.remove("configured").is_some();
        }
    }
    if let Some(transport) = object.get_mut("transport") {
        changed |= scrub_mcp_credential_metadata(transport);
    }
    changed
}

fn scrub_shadowed_provider_document_metadata(
    bytes: &mut Vec<u8>,
    authority: &FacadeRootSecretAuthority,
) -> ConfigStoreResult<bool> {
    let mut root: Value = serde_json::from_slice(bytes)?;
    let (data, _) = section_data_mut(&mut root)?;
    let object = data.as_object_mut().ok_or_else(|| {
        ConfigStoreError::Validation("provider section must be an object".to_string())
    })?;
    let mut changed = false;
    for provider in &authority.providers {
        if let Some(config) = object.get_mut(provider).and_then(Value::as_object_mut) {
            changed |= config.remove("credential_ref").is_some();
            changed |= config.remove("configured").is_some();
        }
    }
    if authority.provider_instances {
        if let Some(instances) = object
            .get_mut("provider_instances")
            .and_then(Value::as_object_mut)
        {
            for instance in instances.values_mut().filter_map(Value::as_object_mut) {
                changed |= instance.remove("credential_ref").is_some();
                changed |= instance.remove("configured").is_some();
            }
        }
    }
    if changed {
        *bytes = serde_json::to_vec_pretty(&root)?;
    }
    Ok(changed)
}

fn scrub_shadowed_mcp_document_metadata(
    bytes: &mut Vec<u8>,
    authority: &FacadeRootSecretAuthority,
) -> ConfigStoreResult<bool> {
    let mut root: Value = serde_json::from_slice(bytes)?;
    let (data, _) = section_data_mut(&mut root)?;
    let mut changed = false;
    if let Some(servers) = data.get_mut("servers").and_then(Value::as_array_mut) {
        for server in servers {
            if server
                .get("id")
                .and_then(Value::as_str)
                .is_some_and(|id| authority.mcp_servers.contains(id))
            {
                changed |= scrub_mcp_credential_metadata(server);
            }
        }
    } else if let Some(servers) = data.as_object_mut() {
        for (id, server) in servers {
            if authority.mcp_servers.contains(id) {
                changed |= scrub_mcp_credential_metadata(server);
            }
        }
    }
    if changed {
        *bytes = serde_json::to_vec_pretty(&root)?;
    }
    Ok(changed)
}

fn scrub_shadowed_root_credential_metadata(
    bytes: &mut Vec<u8>,
    authority: &FacadeRootSecretAuthority,
) -> ConfigStoreResult<bool> {
    let mut root: Value = serde_json::from_slice(bytes)?;
    let object = root.as_object_mut().ok_or_else(|| {
        ConfigStoreError::Validation("root configuration must be an object".to_string())
    })?;
    let mut changed = false;
    if let Some(providers) = object.get_mut("providers").and_then(Value::as_object_mut) {
        for provider in &authority.providers {
            if let Some(config) = providers.get_mut(provider).and_then(Value::as_object_mut) {
                changed |= config.remove("credential_ref").is_some();
                changed |= config.remove("configured").is_some();
            }
        }
    }
    if authority.provider_instances {
        if let Some(instances) = object
            .get_mut("provider_instances")
            .and_then(Value::as_object_mut)
        {
            for instance in instances.values_mut().filter_map(Value::as_object_mut) {
                changed |= instance.remove("credential_ref").is_some();
                changed |= instance.remove("configured").is_some();
            }
        }
    }
    if authority.mcp {
        let mcp = if object.contains_key("mcpServers") {
            object.get_mut("mcpServers")
        } else {
            object.get_mut("mcp")
        };
        if let Some(mcp) = mcp {
            if let Some(servers) = mcp.get_mut("servers").and_then(Value::as_array_mut) {
                for server in servers {
                    changed |= scrub_mcp_credential_metadata(server);
                }
            } else if let Some(servers) = mcp.as_object_mut() {
                for server in servers.values_mut() {
                    changed |= scrub_mcp_credential_metadata(server);
                }
            }
        }
    }
    if changed {
        *bytes = serde_json::to_vec_pretty(&root)?;
    }
    Ok(changed)
}

pub(crate) fn plan_facade_compound_credentials(
    data_dir: &Path,
    source_snapshot: FacadeSourceSnapshot,
    broker_ready: bool,
) -> ConfigStoreResult<FacadeCredentialPlan> {
    let mut primary_extracted = Vec::new();
    let mut backup_extractions = Vec::new();
    let mut source_overrides = BTreeMap::new();
    let mut legacy_cleanups = Vec::new();

    let mut primary_authority = FacadeRootSecretAuthority::default();
    if let Some(original) = source_snapshot
        .bytes(PROVIDERS_FILE)?
        .map(ToOwned::to_owned)
    {
        let (providers, owns_instances) = provider_document_authority(&original)?;
        primary_authority.providers = providers;
        primary_authority.provider_instances = owns_instances;
        if let Some(section) = plan_provider_document(original, &mut primary_extracted, 1)? {
            source_overrides.insert(PROVIDERS_FILE.to_string(), section.bytes);
        }
    }
    if let Some(original) = source_snapshot.bytes(MCP_FILE)?.map(ToOwned::to_owned) {
        primary_authority.mcp = true;
        primary_authority.mcp_servers = mcp_document_server_ids(&original)?;
        if let Some(section) = plan_mcp_document(original, &mut primary_extracted, 1)? {
            source_overrides.insert(MCP_FILE.to_string(), section.bytes);
        }
    }
    if let Some(original) = source_snapshot.bytes(CONNECT_FILE)?.map(ToOwned::to_owned) {
        primary_authority.connect_platforms = connect_platform_ids(&original)?;
        if let Some(section) = plan_connect_document(original, &mut primary_extracted, 1)? {
            source_overrides.insert(CONNECT_FILE.to_string(), section.bytes);
        }
    }
    if let Some(original) = source_snapshot
        .bytes(ACCESS_CONTROL_FILE)?
        .map(ToOwned::to_owned)
    {
        primary_authority.access_control = true;
        if let Some(section) = plan_access_control_document(original, &mut primary_extracted, 1)? {
            source_overrides.insert(ACCESS_CONTROL_FILE.to_string(), section.bytes);
        }
    }

    if let Some(original) = source_snapshot.bytes(CONFIG_FILE)?.map(ToOwned::to_owned) {
        let mut root_extracted = Vec::new();
        let planned = plan_facade_root_document(original.clone(), &mut root_extracted, 1)?;
        let mut candidate = planned
            .as_ref()
            .map(|section| section.bytes.clone())
            .unwrap_or_else(|| original.clone());
        let metadata_changed =
            scrub_shadowed_root_credential_metadata(&mut candidate, &primary_authority)?;
        discard_shadowed_root_secrets(&mut root_extracted, &primary_authority);
        if planned.is_some() || metadata_changed {
            primary_extracted.extend(root_extracted);
            source_overrides.insert(CONFIG_FILE.to_string(), candidate.clone());
            legacy_cleanups.push(PlannedLegacyCleanup {
                name: CONFIG_FILE.to_string(),
                original: source_snapshot.state(CONFIG_FILE)?.clone(),
                candidate: FacadeSourceState::Regular(candidate),
                sensitive: true,
            });
        }
    }

    if broker_ready {
        let mut broker_extracted = Vec::new();
        if let Some(original) = source_snapshot.bytes(BROKER_FILE)?.map(ToOwned::to_owned) {
            if let Some(section) =
                plan_broker_document(data_dir, original, &mut broker_extracted, 1)?
            {
                primary_extracted.extend(broker_extracted);
                source_overrides.insert(BROKER_FILE.to_string(), section.bytes.clone());
                legacy_cleanups.push(PlannedLegacyCleanup {
                    name: BROKER_FILE.to_string(),
                    original: source_snapshot.state(BROKER_FILE)?.clone(),
                    candidate: FacadeSourceState::Regular(section.bytes),
                    sensitive: true,
                });
            }
        }
    }

    for suffix in ["bak", "bak.1", "bak.2"] {
        let mut generation_extracted = Vec::new();
        let providers_name = format!("{PROVIDERS_FILE}.{suffix}");
        let mut generation_authority = primary_authority.clone();
        if let Some(original) = source_snapshot
            .bytes(&providers_name)?
            .map(ToOwned::to_owned)
        {
            if serde_json::from_slice::<Value>(&original).is_ok() {
                let (providers, owns_instances) = provider_document_authority(&original)?;
                generation_authority.providers.extend(providers);
                generation_authority.provider_instances |= owns_instances;
                let mut provider_extracted = Vec::new();
                let planned = plan_provider_document(original.clone(), &mut provider_extracted, 1)?;
                let mut candidate = planned
                    .as_ref()
                    .map(|section| section.bytes.clone())
                    .unwrap_or_else(|| original.clone());
                let metadata_changed =
                    scrub_shadowed_provider_document_metadata(&mut candidate, &primary_authority)?;
                discard_shadowed_sidecar_backup_secrets(
                    &mut provider_extracted,
                    &primary_authority,
                );
                if planned.is_some() || metadata_changed {
                    generation_extracted.extend(provider_extracted);
                    legacy_cleanups.push(PlannedLegacyCleanup {
                        name: providers_name.clone(),
                        original: source_snapshot.state(&providers_name)?.clone(),
                        candidate: FacadeSourceState::Regular(candidate),
                        sensitive: true,
                    });
                }
            }
        }

        let mcp_name = format!("{MCP_FILE}.{suffix}");
        if let Some(original) = source_snapshot.bytes(&mcp_name)?.map(ToOwned::to_owned) {
            if serde_json::from_slice::<Value>(&original).is_ok() {
                generation_authority.mcp = true;
                generation_authority
                    .mcp_servers
                    .extend(mcp_document_server_ids(&original)?);
                let mut mcp_extracted = Vec::new();
                let planned = plan_mcp_document(original.clone(), &mut mcp_extracted, 1)?;
                let mut candidate = planned
                    .as_ref()
                    .map(|section| section.bytes.clone())
                    .unwrap_or_else(|| original.clone());
                let metadata_changed =
                    scrub_shadowed_mcp_document_metadata(&mut candidate, &primary_authority)?;
                discard_shadowed_sidecar_backup_secrets(&mut mcp_extracted, &primary_authority);
                if planned.is_some() || metadata_changed {
                    generation_extracted.extend(mcp_extracted);
                    legacy_cleanups.push(PlannedLegacyCleanup {
                        name: mcp_name.clone(),
                        original: source_snapshot.state(&mcp_name)?.clone(),
                        candidate: FacadeSourceState::Regular(candidate),
                        sensitive: true,
                    });
                }
            }
        }

        let connect_name = format!("{CONNECT_FILE}.{suffix}");
        if let Some(original) = source_snapshot.bytes(&connect_name)?.map(ToOwned::to_owned) {
            if serde_json::from_slice::<Value>(&original).is_ok() {
                let value: Value = serde_json::from_slice(&original)?;
                let has_platforms = value
                    .get("platforms")
                    .or_else(|| value.get("data").and_then(|data| data.get("platforms")))
                    .is_some();
                if has_platforms {
                    let mut connect_extracted = Vec::new();
                    let planned =
                        plan_connect_document(original.clone(), &mut connect_extracted, 1)?;
                    discard_shadowed_sidecar_backup_secrets(
                        &mut connect_extracted,
                        &primary_authority,
                    );
                    if let Some(section) = planned {
                        generation_extracted.extend(connect_extracted);
                        legacy_cleanups.push(PlannedLegacyCleanup {
                            name: connect_name,
                            original: source_snapshot
                                .state(&format!("{CONNECT_FILE}.{suffix}"))?
                                .clone(),
                            candidate: FacadeSourceState::Regular(section.bytes),
                            sensitive: true,
                        });
                    }
                }
            }
        }

        let access_name = format!("{ACCESS_CONTROL_FILE}.{suffix}");
        if let Some(original) = source_snapshot.bytes(&access_name)?.map(ToOwned::to_owned) {
            if serde_json::from_slice::<Value>(&original).is_ok() {
                let mut access_extracted = Vec::new();
                let planned =
                    plan_access_control_document(original.clone(), &mut access_extracted, 1)?;
                discard_shadowed_sidecar_backup_secrets(&mut access_extracted, &primary_authority);
                if let Some(section) = planned {
                    generation_extracted.extend(access_extracted);
                    legacy_cleanups.push(PlannedLegacyCleanup {
                        name: access_name.clone(),
                        original: source_snapshot.state(&access_name)?.clone(),
                        candidate: FacadeSourceState::Regular(section.bytes),
                        sensitive: true,
                    });
                }
            }
        }

        let config_name = format!("{CONFIG_FILE}.{suffix}");
        if let Some(original) = source_snapshot.bytes(&config_name)?.map(ToOwned::to_owned) {
            if serde_json::from_slice::<Value>(&original).is_ok() {
                let mut root_extracted = Vec::new();
                let planned = plan_facade_root_document(original.clone(), &mut root_extracted, 1)?;
                let mut candidate = planned
                    .as_ref()
                    .map(|section| section.bytes.clone())
                    .unwrap_or_else(|| original.clone());
                let metadata_changed =
                    scrub_shadowed_root_credential_metadata(&mut candidate, &generation_authority)?;
                discard_shadowed_root_secrets(&mut root_extracted, &generation_authority);
                if planned.is_some() || metadata_changed {
                    generation_extracted.extend(root_extracted);
                    legacy_cleanups.push(PlannedLegacyCleanup {
                        name: config_name.clone(),
                        original: source_snapshot.state(&config_name)?.clone(),
                        candidate: FacadeSourceState::Regular(candidate),
                        sensitive: true,
                    });
                }
            }
        }

        if broker_ready {
            let broker_name = format!("{BROKER_FILE}.{suffix}");
            if let Some(original) = source_snapshot.bytes(&broker_name)?.map(ToOwned::to_owned) {
                if serde_json::from_slice::<Value>(&original).is_ok() {
                    let mut broker_extracted = Vec::new();
                    if let Some(section) =
                        plan_broker_document(data_dir, original, &mut broker_extracted, 1)?
                    {
                        generation_extracted.extend(broker_extracted);
                        legacy_cleanups.push(PlannedLegacyCleanup {
                            name: broker_name.clone(),
                            original: source_snapshot.state(&broker_name)?.clone(),
                            candidate: FacadeSourceState::Regular(section.bytes),
                            sensitive: true,
                        });
                    }
                }
            }
        }
        backup_extractions.push(generation_extracted);
    }

    let credential_primary = source_snapshot.bytes(CREDENTIALS_FILE)?;
    let credential_backup = source_snapshot.bytes(&format!("{CREDENTIALS_FILE}.bak"))?;
    let sources =
        CredentialStore::migration_sources_from_snapshot(credential_primary, credential_backup)?;
    let resolved = resolve_compound_extracted_secrets_from_sources(
        &sources,
        primary_extracted,
        backup_extractions,
    )?;
    let prepared = CredentialStore::prepare_migration_from_snapshot(
        credential_primary,
        credential_backup,
        resolved,
    )?;

    Ok(FacadeCredentialPlan {
        credential_original: source_snapshot.state(CREDENTIALS_FILE)?.clone(),
        credential_candidate: prepared.bytes,
        source_snapshot,
        source_overrides,
        legacy_cleanups,
    })
}

#[derive(Debug)]
enum LegacySecret {
    Plaintext(String),
    Ciphertext(String),
}

#[derive(Debug)]
struct ExtractedSecret {
    credential_ref: CredentialRef,
    value: LegacySecret,
    migration_generation: u64,
    kind: ExtractedSecretKind,
    env_owner: Option<String>,
    provider_owner: Option<String>,
    mcp_owner: Option<String>,
    connect_owner: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExtractedSecretKind {
    Other,
    ProviderBuiltin,
    ProviderInstance,
    Mcp,
    ProxyAuth,
    EnvVar,
    NotificationNtfy,
    NotificationBark,
    ExternalBroker,
    Connect,
    AccessControl,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MigrationFault {
    None,
    AfterStaging,
    AfterJournal,
    AfterManifest,
    AfterCredentials,
    AfterAuthoritativeSection,
    AfterProviders,
    AfterMcp,
    AfterBroker,
    AfterConfig,
    AfterSectionLayout,
    AfterRebaseCredentialCommit,
    AfterRebaseStageWrite,
    AfterRebaseManifest,
    BeforeExactCommitCredentialRace,
    BeforeExactStagingUnrelatedCredentialRace,
    BeforeExactCommitUnrelatedCredentialRace,
    AfterExactCommitCredentialRace,
    AfterExactCommitUnrelatedCredentialRace,
    AfterExactCommitCredentialClearRace,
    AfterExactCredentialRebaseStage,
    AfterExactCredentialRebaseManifest,
    AfterExactProxyConfigRebaseManifestExternalWrite,
    AfterExactClusterConsumerConflict,
    AfterExactClusterSectionRebase,
}

fn authoritative_section_file(scope: ExactTransactionScope) -> &'static str {
    match scope {
        ExactTransactionScope::Providers => PROVIDERS_FILE,
        ExactTransactionScope::Mcp => MCP_FILE,
        ExactTransactionScope::ProxyAuth => CORE_FILE,
        ExactTransactionScope::EnvVars => ENV_FILE,
        ExactTransactionScope::Notifications => NOTIFICATIONS_FILE,
        ExactTransactionScope::Connect => CONNECT_FILE,
        ExactTransactionScope::AccessControl => ACCESS_CONTROL_FILE,
        ExactTransactionScope::ClusterFabric => CLUSTER_FABRIC_FILE,
    }
}

fn exact_authority_file(manifest: &MigrationManifest) -> Option<&str> {
    let scope = manifest.exact_scope?;
    let section = authoritative_section_file(scope);
    manifest
        .files
        .iter()
        .any(|file| file.name == section)
        .then_some(section)
        .or_else(|| {
            manifest
                .files
                .iter()
                .any(|file| file.name == CONFIG_FILE)
                .then_some(CONFIG_FILE)
        })
}

#[cfg(feature = "test-utils")]
type EnvTransactionTestHook = Box<dyn FnOnce(&Path) + Send + 'static>;

#[cfg(feature = "test-utils")]
static ENV_TRANSACTION_TEST_HOOK: std::sync::Mutex<Option<EnvTransactionTestHook>> =
    std::sync::Mutex::new(None);

/// One-shot crash boundary for an adopted cluster-fabric exact transaction.
///
/// Test-only: production builds have neither this type nor the corresponding
/// timing branches.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterExactCommitTestFault {
    AfterManifest,
    AfterManifestRecoveryFailure,
    AfterCredentialInstall,
    BeforeFinish,
    AbortMarkerRecoveryFailure,
    AbortJournalMarkerAndCleanupRecoveryFailure,
    AbortBothMarkersRecoveryFailure,
    AbortCleanupRecoveryFailure,
}

#[cfg(any(test, feature = "test-utils"))]
#[derive(Debug, Clone, Copy)]
struct ClusterExactCommitTestFaultState {
    fault: ClusterExactCommitTestFault,
    cleanup_failure_observed: bool,
}

#[cfg(any(test, feature = "test-utils"))]
static CLUSTER_EXACT_COMMIT_TEST_FAULTS: LazyLock<
    Mutex<BTreeMap<PathBuf, ClusterExactCommitTestFaultState>>,
> = LazyLock::new(|| Mutex::new(BTreeMap::new()));

/// Install a one-shot hook immediately after an env exact manifest commits.
/// Test-only: production builds have no hook or timing branch.
#[cfg(feature = "test-utils")]
pub fn set_env_transaction_test_hook(hook: impl FnOnce(&Path) + Send + 'static) {
    *ENV_TRANSACTION_TEST_HOOK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(hook));
}

/// Inject one recoverable failure into the next adopted cluster exact
/// transaction for `data_dir`.
#[cfg(any(test, feature = "test-utils"))]
pub fn set_cluster_exact_commit_test_fault(
    data_dir: impl Into<PathBuf>,
    fault: ClusterExactCommitTestFault,
) {
    CLUSTER_EXACT_COMMIT_TEST_FAULTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(
            data_dir.into(),
            ClusterExactCommitTestFaultState {
                fault,
                cleanup_failure_observed: false,
            },
        );
}

#[cfg(any(test, feature = "test-utils"))]
pub fn clear_cluster_exact_commit_test_fault(data_dir: impl AsRef<Path>) {
    CLUSTER_EXACT_COMMIT_TEST_FAULTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(data_dir.as_ref());
}

#[cfg(any(test, feature = "test-utils"))]
fn take_cluster_exact_commit_test_fault(
    data_dir: &Path,
    fault: ClusterExactCommitTestFault,
) -> bool {
    let mut faults = CLUSTER_EXACT_COMMIT_TEST_FAULTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if faults
        .get(data_dir)
        .is_some_and(|state| state.fault == fault)
    {
        faults.remove(data_dir);
        true
    } else {
        false
    }
}

#[cfg(any(test, feature = "test-utils"))]
fn take_cluster_exact_after_manifest_test_fault(data_dir: &Path) -> bool {
    let mut faults = CLUSTER_EXACT_COMMIT_TEST_FAULTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    match faults.get(data_dir).map(|state| state.fault) {
        Some(ClusterExactCommitTestFault::AfterManifest) => {
            faults.remove(data_dir);
            true
        }
        // Retain this variant until the recovery attempt consumes its second
        // deterministic failure below.
        Some(ClusterExactCommitTestFault::AfterManifestRecoveryFailure) => true,
        _ => false,
    }
}

#[cfg(any(test, feature = "test-utils"))]
fn cluster_abort_cleanup_test_fault(data_dir: &Path) -> bool {
    let mut faults = CLUSTER_EXACT_COMMIT_TEST_FAULTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let Some(state) = faults.get_mut(data_dir) else {
        return false;
    };
    match state.fault {
        ClusterExactCommitTestFault::AbortCleanupRecoveryFailure => true,
        ClusterExactCommitTestFault::AbortJournalMarkerAndCleanupRecoveryFailure
            if !state.cleanup_failure_observed =>
        {
            state.cleanup_failure_observed = true;
            true
        }
        _ => false,
    }
}

#[cfg(any(test, feature = "test-utils"))]
fn cluster_abort_marker_test_fault(data_dir: &Path) -> bool {
    CLUSTER_EXACT_COMMIT_TEST_FAULTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(data_dir)
        .is_some_and(|state| {
            matches!(
                state.fault,
                ClusterExactCommitTestFault::AbortMarkerRecoveryFailure
                    | ClusterExactCommitTestFault::AbortBothMarkersRecoveryFailure
            )
        })
}

#[cfg(any(test, feature = "test-utils"))]
fn cluster_abort_journal_marker_test_fault(data_dir: &Path) -> bool {
    CLUSTER_EXACT_COMMIT_TEST_FAULTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(data_dir)
        .is_some_and(|state| {
            matches!(
                state.fault,
                ClusterExactCommitTestFault::AbortJournalMarkerAndCleanupRecoveryFailure
                    | ClusterExactCommitTestFault::AbortBothMarkersRecoveryFailure
            )
        })
}

/// Extract provider/MCP/root secrets into the isolated credential store.
/// Calling this on every load is intentional: it is idempotent and also
/// catches legacy ciphertext written later by an older binary.
pub fn migrate_provider_mcp_credentials(
    data_dir: impl AsRef<Path>,
) -> ConfigStoreResult<CredentialMigrationOutcome> {
    #[cfg(test)]
    return migrate_with_fault(data_dir.as_ref(), MigrationFault::None);
    #[cfg(not(test))]
    migrate_inner(data_dir.as_ref(), None)
}

pub(crate) fn with_migration_lock<T>(
    data_dir: &Path,
    operation: impl FnOnce() -> ConfigStoreResult<T>,
) -> ConfigStoreResult<T> {
    with_provider_mcp_migration_lock(data_dir, operation)
}

pub(crate) fn migrate_provider_mcp_credentials_for_facade_locked(
    data_dir: &Path,
) -> ConfigStoreResult<CredentialMigrationOutcome> {
    migrate_inner_with_root_builtins_locked(data_dir, None, true)
}

pub(crate) fn migrate_external_broker_credentials_locked(
    data_dir: &Path,
) -> ConfigStoreResult<CredentialMigrationOutcome> {
    migrate_broker_locked(data_dir, None)
}

pub(crate) fn migrate_cluster_credentials_locked(
    data_dir: &Path,
) -> ConfigStoreResult<CredentialMigrationOutcome> {
    migrate_cluster_locked(data_dir, None)
}

/// Run the provider/MCP/root planner and every ownership/decryption check
/// without creating the migration lock or staging any bytes. This is the
/// facade's zero-write gate before independently-scoped migrations begin.
pub(crate) fn preflight_provider_mcp_credential_migration(
    data_dir: &Path,
) -> ConfigStoreResult<()> {
    let mut extracted = Vec::new();
    let mut authority = FacadeRootSecretAuthority::default();
    let providers = match read_optional_target(&data_dir.join(PROVIDERS_FILE))? {
        Some(original) => {
            let (providers, owns_instances) = provider_document_authority(&original)?;
            authority.providers = providers;
            authority.provider_instances = owns_instances;
            plan_provider_document(original, &mut extracted, 1)?
        }
        None => None,
    };
    let mcp = match read_optional_target(&data_dir.join(MCP_FILE))? {
        Some(original) => {
            authority.mcp = true;
            authority.mcp_servers = mcp_document_server_ids(&original)?;
            plan_mcp_document(original, &mut extracted, 1)?
        }
        None => None,
    };
    let mut root_extracted = Vec::new();
    let root = match read_optional_target(&data_dir.join(CONFIG_FILE))? {
        Some(original) => plan_facade_root_document(original, &mut root_extracted, 1)?,
        None => None,
    };
    discard_shadowed_root_secrets(&mut root_extracted, &authority);
    extracted.extend(root_extracted);
    let credential_snapshot = credential_snapshot_bytes(data_dir)?;
    let sources = CredentialStore::migration_sources_from_snapshot(
        credential_snapshot.primary.as_deref(),
        credential_snapshot.backup.as_deref(),
    )?;
    let prospective_documents = [&providers, &mcp, &root]
        .into_iter()
        .filter_map(Option::as_ref)
        .map(|section| (section.bytes.as_slice(), section.name == CONFIG_FILE))
        .collect::<Vec<_>>();
    ensure_legacy_env_extractions_are_safe(data_dir, &extracted, &prospective_documents)?;
    ensure_legacy_notification_extractions_are_safe(data_dir, &extracted, &prospective_documents)?;
    ensure_legacy_proxy_extractions_are_safe_from_sources(
        data_dir,
        &sources,
        &extracted,
        &prospective_documents,
    )?;
    ensure_backup_legacy_proxy_extractions_are_safe_from_sources(data_dir, &sources)?;
    let resolved = resolve_extracted_secrets_from_sources(&sources, extracted)?;
    CredentialStore::prepare_migration_from_snapshot(
        credential_snapshot.primary.as_deref(),
        credential_snapshot.backup.as_deref(),
        resolved,
    )?;
    Ok(())
}

/// Extract the external broker bearer token without coupling a malformed
/// `broker.json` to provider/MCP/root configuration loading. This uses the
/// same migration lock and manifest as every other credential transaction, so
/// an already committed partial transaction still blocks all readers until it
/// is recovered.
pub fn migrate_external_broker_credentials(
    data_dir: impl AsRef<Path>,
) -> ConfigStoreResult<CredentialMigrationOutcome> {
    #[cfg(test)]
    return migrate_broker_with_fault(data_dir.as_ref(), MigrationFault::None);
    #[cfg(not(test))]
    migrate_broker_inner(data_dir.as_ref(), None)
}

/// Extract cluster-fabric SSH credentials from the root configuration without
/// coupling malformed optional cluster data to provider/MCP migration. The
/// transaction uses the shared lock and recovery manifest, but carries an
/// explicit scope so a committed config rebase reruns the cluster planner.
pub fn migrate_cluster_credentials(
    data_dir: impl AsRef<Path>,
) -> ConfigStoreResult<CredentialMigrationOutcome> {
    migrate_cluster_inner(data_dir.as_ref(), None)
}

/// Validate the complete cluster credential migration without creating the
/// shared lock, transaction directories, credentials document, or backups.
/// The facade calls this before any independent credential domain is allowed
/// to commit so a late cluster failure cannot leave a partial split behind.
pub(crate) fn preflight_cluster_credential_migration(data_dir: &Path) -> ConfigStoreResult<()> {
    let mut extracted = Vec::new();
    let section = plan_cluster_section(data_dir, &mut extracted, 1, true)?;
    let prospective = section
        .as_ref()
        .map(|section| vec![(section.bytes.as_slice(), true)])
        .unwrap_or_default();
    ensure_cluster_extractions_are_safe(data_dir, &extracted, &prospective)?;
    let credential_snapshot = credential_snapshot_bytes(data_dir)?;
    let sources = CredentialStore::migration_sources_from_snapshot(
        credential_snapshot.primary.as_deref(),
        credential_snapshot.backup.as_deref(),
    )?;
    let primary_refs = extracted
        .iter()
        .map(|secret| secret.credential_ref.clone())
        .collect::<BTreeSet<_>>();
    let mut all = extracted;
    for secret in collect_cluster_backup_extractions(data_dir)? {
        if !sources.contains_key(&secret.credential_ref)
            && !primary_refs.contains(&secret.credential_ref)
        {
            all.push(secret);
        }
    }
    let resolved = resolve_extracted_secrets_from_sources(&sources, all)?;
    CredentialStore::prepare_migration_from_snapshot(
        credential_snapshot.primary.as_deref(),
        credential_snapshot.backup.as_deref(),
        resolved,
    )?;
    Ok(())
}

struct CredentialSnapshotBytes {
    primary: Option<Vec<u8>>,
    backup: Option<Vec<u8>>,
}

fn credential_snapshot_bytes(data_dir: &Path) -> ConfigStoreResult<CredentialSnapshotBytes> {
    Ok(CredentialSnapshotBytes {
        primary: read_optional_target(&data_dir.join(CREDENTIALS_FILE))?,
        backup: read_optional_target(&data_dir.join(format!("{CREDENTIALS_FILE}.bak")))?,
    })
}

/// Install every typed config section through the existing credential
/// migration transaction while the facade coordinator holds the shared lock.
pub(crate) fn install_section_split_migration_locked(
    data_dir: &Path,
    plan: crate::section_facade::SectionSplitPlan,
) -> ConfigStoreResult<SectionMigrationOutcome> {
    install_section_split_migration_locked_inner(
        data_dir,
        plan,
        #[cfg(test)]
        MigrationFault::None,
    )
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) enum SectionSplitTestFault {
    Staging,
    Journal,
    Manifest,
    LayoutMarker,
}

#[cfg(test)]
type FacadeCompoundTestHook = Box<dyn FnOnce(&Path) + Send + 'static>;

#[cfg(test)]
fn facade_compound_test_hooks(
) -> &'static std::sync::Mutex<BTreeMap<PathBuf, FacadeCompoundTestHook>> {
    static HOOKS: std::sync::OnceLock<std::sync::Mutex<BTreeMap<PathBuf, FacadeCompoundTestHook>>> =
        std::sync::OnceLock::new();
    HOOKS.get_or_init(|| std::sync::Mutex::new(BTreeMap::new()))
}

#[cfg(test)]
pub(crate) fn set_facade_compound_after_manifest_test_hook(
    data_dir: &Path,
    hook: impl FnOnce(&Path) + Send + 'static,
) {
    facade_compound_test_hooks()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(data_dir.to_path_buf(), Box::new(hook));
}

#[cfg(test)]
fn run_facade_compound_after_manifest_test_hook(data_dir: &Path) {
    if let Some(hook) = facade_compound_test_hooks()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(data_dir)
    {
        hook(data_dir);
    }
}

#[cfg(test)]
pub(crate) fn install_section_split_migration_with_fault(
    data_dir: &Path,
    plan: crate::section_facade::SectionSplitPlan,
    fault: SectionSplitTestFault,
) -> ConfigStoreResult<SectionMigrationOutcome> {
    let fault = match fault {
        SectionSplitTestFault::Staging => MigrationFault::AfterStaging,
        SectionSplitTestFault::Journal => MigrationFault::AfterJournal,
        SectionSplitTestFault::Manifest => MigrationFault::AfterManifest,
        SectionSplitTestFault::LayoutMarker => MigrationFault::AfterSectionLayout,
    };
    install_section_split_migration_inner(data_dir, plan, fault)
}

#[cfg(test)]
fn install_section_split_migration_inner(
    data_dir: &Path,
    plan: crate::section_facade::SectionSplitPlan,
    #[cfg(test)] fault: MigrationFault,
) -> ConfigStoreResult<SectionMigrationOutcome> {
    with_migration_lock(data_dir, || {
        install_section_split_migration_locked_inner(
            data_dir,
            plan,
            #[cfg(test)]
            fault,
        )
    })
}

fn install_section_split_migration_locked_inner(
    data_dir: &Path,
    plan: crate::section_facade::SectionSplitPlan,
    #[cfg(test)] fault: MigrationFault,
) -> ConfigStoreResult<SectionMigrationOutcome> {
    cleanup_orphan_transaction_dirs(data_dir)?;
    let resumed = recover_committed(
        data_dir,
        #[cfg(test)]
        Some(fault),
    )?
    .is_some();
    if crate::section_layout_is_active(data_dir)? {
        return Ok(SectionMigrationOutcome {
            activated: false,
            resumed,
        });
    }
    discard_uncommitted(data_dir)?;
    validate_section_split_plan(data_dir, &plan)?;

    let (credential_original, credential_candidate) = match plan.credential_plan.as_ref() {
        Some(compound) => (
            match &compound.credential_original {
                FacadeSourceState::Missing => None,
                FacadeSourceState::Regular(bytes) => Some(bytes.clone()),
            },
            compound.credential_candidate.clone(),
        ),
        None => {
            let original = read_optional_target(&data_dir.join(CREDENTIALS_FILE))?;
            let store = CredentialStore::open(data_dir);
            let prepared = store.prepare_migration(Vec::new())?;
            (original, prepared.bytes)
        }
    };
    let transaction_id = Uuid::new_v4().to_string();
    let stage_dir_name = format!("{STAGE_PREFIX}{transaction_id}");
    let stage_dir = data_dir.join(&stage_dir_name);
    let backup_dir = data_dir.join(format!("{BACKUP_PREFIX}{transaction_id}"));
    create_private_dir(&stage_dir)?;
    create_private_dir(&backup_dir)?;
    sync_dir(data_dir)?;

    let mut staged = Vec::with_capacity(plan.files.len() + 2);
    stage_file(
        &stage_dir,
        &backup_dir,
        CREDENTIALS_FILE,
        &credential_candidate,
        credential_original.as_deref(),
        true,
        None,
        InstallMode::Migration,
        None,
        &mut staged,
    )?;
    for file in &plan.files {
        stage_file(
            &stage_dir,
            &backup_dir,
            file.name,
            &file.candidate,
            file.original_present.then_some(file.original.as_slice()),
            false,
            Some(file.revision),
            InstallMode::Migration,
            None,
            &mut staged,
        )?;
    }
    if let Some(compound) = plan.credential_plan.as_ref() {
        for cleanup in &compound.legacy_cleanups {
            let original = match &cleanup.original {
                FacadeSourceState::Missing => None,
                FacadeSourceState::Regular(bytes) => Some(bytes.as_slice()),
            };
            let FacadeSourceState::Regular(candidate) = &cleanup.candidate else {
                return Err(ConfigStoreError::Validation(
                    "compound cleanup deletion is not implemented".to_string(),
                ));
            };
            stage_file(
                &stage_dir,
                &backup_dir,
                &cleanup.name,
                candidate,
                original,
                cleanup.sensitive,
                None,
                InstallMode::LegacyCleanup,
                None,
                &mut staged,
            )?;
        }
        let staged_names = staged
            .iter()
            .map(|file| file.name.clone())
            .collect::<BTreeSet<_>>();
        for (name, state) in &compound.source_snapshot.files {
            if staged_names.contains(name)
                || is_compound_internal_derivative_source(name)
                || matches!(
                    name.as_str(),
                    MANIFEST_FILE
                        | JOURNAL_FILE
                        | SECTION_LAYOUT_COMPLETION_FILE
                        | crate::SECTION_LAYOUT_FILE
                )
            {
                continue;
            }
            let (candidate, original) = match state {
                FacadeSourceState::Missing => (&[][..], None),
                FacadeSourceState::Regular(bytes) => (bytes.as_slice(), Some(bytes.as_slice())),
            };
            stage_file(
                &stage_dir,
                &backup_dir,
                name,
                candidate,
                original,
                true,
                None,
                InstallMode::SourceGuard,
                None,
                &mut staged,
            )?;
        }
    }
    let marker_original = read_optional_target(&data_dir.join(crate::SECTION_LAYOUT_FILE))?;
    stage_file(
        &stage_dir,
        &backup_dir,
        crate::SECTION_LAYOUT_FILE,
        &plan.marker,
        marker_original.as_deref(),
        false,
        Some(crate::SECTION_LAYOUT_VERSION as u64),
        InstallMode::Migration,
        None,
        &mut staged,
    )?;
    restrict_directory_files_to_owner(&stage_dir)?;
    restrict_directory_files_to_owner(&backup_dir)?;
    sync_dir(&stage_dir)?;
    sync_dir(&backup_dir)?;
    #[cfg(test)]
    if fault == MigrationFault::AfterStaging {
        return Err(injected_fault());
    }

    let manifest = MigrationManifest {
        version: MIGRATION_VERSION,
        transaction_id,
        stage_dir: stage_dir_name,
        state: MigrationState::Pending,
        exact_scope: None,
        migration_scope: Some(if plan.credential_plan.is_some() {
            MigrationScope::FacadeCompound
        } else {
            MigrationScope::SectionSplit
        }),
        source_attestation: plan
            .credential_plan
            .as_ref()
            .map(|compound| compound.source_snapshot.attestation())
            .unwrap_or_default(),
        files: staged,
    };
    write_manifest(data_dir.join(JOURNAL_FILE), &manifest)?;
    #[cfg(test)]
    if fault == MigrationFault::AfterJournal {
        return Err(injected_fault());
    }
    write_manifest(data_dir.join(MANIFEST_FILE), &manifest)?;
    #[cfg(test)]
    if manifest.migration_scope == Some(MigrationScope::FacadeCompound) {
        run_facade_compound_after_manifest_test_hook(data_dir);
    }
    #[cfg(test)]
    if fault == MigrationFault::AfterManifest {
        return Err(injected_fault());
    }
    let mut manifest = manifest;
    install_pending(
        data_dir,
        &mut manifest,
        None,
        #[cfg(test)]
        Some(fault),
    )?;
    finish_transaction(data_dir, manifest)?;
    Ok(SectionMigrationOutcome {
        activated: true,
        resumed,
    })
}

fn validate_section_split_plan(
    data_dir: &Path,
    plan: &crate::section_facade::SectionSplitPlan,
) -> ConfigStoreResult<()> {
    if let Some(compound) = plan.credential_plan.as_ref() {
        if FacadeSourceSnapshot::capture(data_dir)? != compound.source_snapshot {
            return Err(ConfigStoreError::Validation(
                "facade source changed during compound planning".to_string(),
            ));
        }
    }
    let expected = crate::SECTION_DESCRIPTORS
        .iter()
        .filter(|descriptor| descriptor.id != crate::SectionId::Credentials)
        .map(|descriptor| descriptor.file_name)
        .collect::<BTreeSet<_>>();
    let actual = plan
        .files
        .iter()
        .map(|file| file.name)
        .collect::<BTreeSet<_>>();
    if actual != expected || plan.files.len() != expected.len() {
        return Err(ConfigStoreError::Validation(
            "section split plan is incomplete".to_string(),
        ));
    }
    for file in &plan.files {
        let current = read_optional_target(&data_dir.join(file.name))?;
        let expected = file.original_present.then_some(file.original.as_slice());
        if current.as_deref() != expected {
            return Err(ConfigStoreError::Validation(
                "legacy configuration changed during section planning".to_string(),
            ));
        }
        let envelope: Value = serde_json::from_slice(&file.candidate)?;
        if envelope.get("schema_version").and_then(Value::as_u64) != Some(1)
            || envelope.get("revision").and_then(Value::as_u64) != Some(file.revision)
            || envelope.get("data").is_none()
        {
            return Err(ConfigStoreError::Validation(
                "section split candidate is invalid".to_string(),
            ));
        }
    }
    let mut expected_sources = crate::SECTION_DESCRIPTORS
        .iter()
        .map(|descriptor| descriptor.file_name.to_string())
        .collect::<BTreeSet<_>>();
    expected_sources.extend([
        CONFIG_FILE.to_string(),
        BROKER_FILE.to_string(),
        "connect.json".to_string(),
        crate::SECTION_LAYOUT_FILE.to_string(),
    ]);
    let actual_sources = plan
        .source_hashes
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<BTreeSet<_>>();
    if actual_sources != expected_sources || plan.source_hashes.len() != expected_sources.len() {
        return Err(ConfigStoreError::Validation(
            "section split source attestation is incomplete".to_string(),
        ));
    }
    let current_sources = crate::section_facade::migration_source_hashes(data_dir)?
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    for (name, expected_hash) in &plan.source_hashes {
        if expected_hash.len() != 64 || current_sources.get(name) != Some(expected_hash) {
            return Err(ConfigStoreError::Validation(
                "legacy configuration changed during section planning".to_string(),
            ));
        }
    }
    Ok(())
}

/// Fail-closed guard for every production reader of credential transaction
/// members. A malformed manifest is treated like a pending one: callers must
/// retain their existing runtime rather than guessing which files committed.
pub fn ensure_provider_mcp_migration_ready(data_dir: impl AsRef<Path>) -> ConfigStoreResult<()> {
    let path = data_dir.as_ref().join(MANIFEST_FILE);
    let Some(bytes) = read_optional_migration_file(&path)? else {
        return Ok(());
    };
    let manifest: MigrationManifest = serde_json::from_slice(&bytes).map_err(|_| {
        ConfigStoreError::Validation(
            "provider/MCP/broker credential migration is pending".to_string(),
        )
    })?;
    validate_manifest(&manifest).map_err(|_| {
        ConfigStoreError::Validation(
            "provider/MCP/broker credential migration is pending".to_string(),
        )
    })?;
    if manifest.state != MigrationState::Complete {
        let journal_aborting = manifest.state == MigrationState::Pending
            && matching_cluster_abort_journal(data_dir.as_ref(), &manifest)?;
        if manifest.migration_scope == Some(MigrationScope::ClusterFabric)
            && manifest.state == MigrationState::Pending
            && !journal_aborting
            && provider_mcp_documents_are_migration_free(data_dir.as_ref())?
            && pending_cluster_refs_are_exclusive(data_dir.as_ref())?
        {
            return Ok(());
        }
        return Err(ConfigStoreError::Validation(
            "provider/MCP/broker credential migration is pending".to_string(),
        ));
    }
    Ok(())
}

/// Recover an already-committed config transaction without planning or
/// starting any new migration. A journal without a manifest is deliberately
/// left untouched so the original planner remains the only owner allowed to
/// discard an uncommitted transaction.
// Kept separate from planning so facade startup can resume a committed
// transaction before validating current source files.
pub(crate) fn recover_pending_config_transaction(
    data_dir: impl AsRef<Path>,
) -> ConfigStoreResult<bool> {
    let data_dir = data_dir.as_ref();
    let data_dir = if data_dir.as_os_str().is_empty() {
        Path::new(".")
    } else {
        data_dir
    };
    if read_optional_migration_file(&data_dir.join(MANIFEST_FILE))?.is_none() {
        return Ok(false);
    }
    with_provider_mcp_migration_lock(data_dir, || {
        recover_committed(
            data_dir,
            #[cfg(test)]
            None,
        )
        .map(|outcome| outcome.is_some())
    })
}

/// Serialize public credential access with provider/MCP/broker transactions.
/// Keeping `ensure -> load/decrypt` (and mutation CAS) under this lock closes
/// windows where a reader could observe a transaction member with stale
/// metadata or a writer could commit after the durable transaction point.
pub(crate) fn with_provider_mcp_migration_lock<T>(
    data_dir: &Path,
    operation: impl FnOnce() -> ConfigStoreResult<T>,
) -> ConfigStoreResult<T> {
    let data_dir = if data_dir.as_os_str().is_empty() {
        Path::new(".")
    } else {
        data_dir
    };
    std::fs::create_dir_all(data_dir)?;
    // Advisory file locks do not reliably serialize two descriptors owned by
    // the same process on every supported OS. Pair them with a path-keyed
    // process mutex so thread-local stores cannot rotate/read the same
    // credential files concurrently; the file lock still coordinates other
    // processes.
    let key = data_dir
        .canonicalize()
        .unwrap_or_else(|_| data_dir.to_path_buf());
    let process_lock = {
        let mut locks = PROCESS_MIGRATION_LOCKS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
            lock
        } else {
            let lock = Arc::new(Mutex::new(()));
            locks.insert(key, Arc::downgrade(&lock));
            lock
        }
    };
    let _process_guard = process_lock
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let lock = open_migration_lock(&data_dir.join(LOCK_FILE))?;
    lock.lock_exclusive()?;
    let _lock = MigrationLock(lock);
    operation()
}

/// Persist a legacy provider-key update as one manifest-gated transaction.
/// The caller must pass a detached candidate config and publish it to live
/// memory only after this function succeeds.
pub fn persist_provider_credential_transaction(
    data_dir: impl AsRef<Path>,
    config: &mut crate::Config,
    intents: &BTreeSet<String>,
) -> ConfigStoreResult<()> {
    persist_provider_instance_credential_transaction(data_dir, config, intents, &BTreeSet::new())
}

/// Persist proxy authentication and its stable root metadata as one
/// recoverable credential/config transaction.
#[cfg(test)]
fn persist_proxy_auth_credential_transaction(
    data_dir: impl AsRef<Path>,
    config: &mut crate::Config,
) -> ConfigStoreResult<u64> {
    let data_dir = data_dir.as_ref();
    let expected_revision = CredentialStore::open(data_dir).revision()?;
    persist_proxy_auth_credential_transaction_at_revision(data_dir, config, expected_revision)
}

/// Persist proxy authentication with an explicit credential-store revision
/// precondition. The returned revision is the durable credential revision
/// committed by the exact transaction.
pub fn persist_proxy_auth_credential_transaction_at_revision(
    data_dir: impl AsRef<Path>,
    config: &mut crate::Config,
    expected_revision: u64,
) -> ConfigStoreResult<u64> {
    #[cfg(test)]
    return persist_provider_credential_transaction_with_instances_inner(
        data_dir.as_ref(),
        config,
        &BTreeSet::from(["__proxy_auth".to_string()]),
        &BTreeSet::new(),
        Some(expected_revision),
        &BTreeSet::new(),
        None,
        false,
        &BTreeSet::new(),
        false,
        None,
        None,
    );
    #[cfg(not(test))]
    persist_provider_credential_transaction_with_instances_inner(
        data_dir.as_ref(),
        config,
        &BTreeSet::from(["__proxy_auth".to_string()]),
        &BTreeSet::new(),
        Some(expected_revision),
        &BTreeSet::new(),
        None,
        false,
        &BTreeSet::new(),
        false,
        None,
    )
}

/// Persist secret env values and their root metadata as one recoverable exact
/// transaction guarded by the credential document revision.
pub fn persist_env_var_credential_transaction_at_revision(
    data_dir: impl AsRef<Path>,
    config: &mut crate::Config,
    env_intents: &BTreeSet<String>,
    expected_revision: u64,
) -> ConfigStoreResult<u64> {
    #[cfg(test)]
    return persist_provider_credential_transaction_with_instances_inner(
        data_dir.as_ref(),
        config,
        &BTreeSet::new(),
        &BTreeSet::new(),
        None,
        env_intents,
        Some(expected_revision),
        false,
        &BTreeSet::new(),
        false,
        None,
        None,
    );
    #[cfg(not(test))]
    persist_provider_credential_transaction_with_instances_inner(
        data_dir.as_ref(),
        config,
        &BTreeSet::new(),
        &BTreeSet::new(),
        None,
        env_intents,
        Some(expected_revision),
        false,
        &BTreeSet::new(),
        false,
        None,
    )
}

/// Persist the complete notification metadata domain and explicitly touched
/// ntfy/Bark credentials in one recoverable exact transaction.
pub fn persist_notification_credential_transaction_at_revision(
    data_dir: impl AsRef<Path>,
    config: &mut crate::Config,
    secret_intents: &BTreeSet<String>,
    expected_revision: u64,
) -> ConfigStoreResult<u64> {
    persist_notification_credential_transaction_at_revision_with_reset(
        data_dir,
        config,
        secret_intents,
        false,
        expected_revision,
    )
}

/// Notification transaction variant used by the root compatibility API's
/// explicit `notifications: null` domain reset.
pub fn persist_notification_credential_transaction_at_revision_with_reset(
    data_dir: impl AsRef<Path>,
    config: &mut crate::Config,
    secret_intents: &BTreeSet<String>,
    reset_domain: bool,
    expected_revision: u64,
) -> ConfigStoreResult<u64> {
    #[cfg(test)]
    return persist_provider_credential_transaction_with_instances_inner(
        data_dir.as_ref(),
        config,
        &BTreeSet::new(),
        &BTreeSet::new(),
        None,
        &BTreeSet::new(),
        None,
        true,
        secret_intents,
        reset_domain,
        Some(expected_revision),
        None,
    );
    #[cfg(not(test))]
    persist_provider_credential_transaction_with_instances_inner(
        data_dir.as_ref(),
        config,
        &BTreeSet::new(),
        &BTreeSet::new(),
        None,
        &BTreeSet::new(),
        None,
        true,
        secret_intents,
        reset_domain,
        Some(expected_revision),
    )
}

/// Persist the complete connect platform collection and all explicitly
/// touched token/app-secret values in one recoverable exact transaction.
/// Connect credentials are only writable after the modular layout is active;
/// this prevents a compatibility writer from reviving legacy `connect.json`
/// ciphertext while the transaction is in flight.
pub fn persist_connect_credential_transaction_at_revision(
    data_dir: impl AsRef<Path>,
    config: &mut crate::Config,
    secret_intents: &crate::patch::ConnectSecretIntents,
    expected_revision: u64,
) -> ConfigStoreResult<u64> {
    if !crate::section_layout_is_active(data_dir.as_ref())? {
        return Err(ConfigStoreError::Validation(
            "connect credential updates require the modular configuration layout".to_string(),
        ));
    }
    persist_exact_credential_transaction_inner(
        data_dir.as_ref(),
        config,
        &BTreeSet::new(),
        &BTreeSet::new(),
        None,
        &BTreeSet::new(),
        None,
        false,
        &BTreeSet::new(),
        false,
        None,
        None,
        Some((secret_intents, expected_revision)),
        None,
        None,
        None,
        None,
        None,
        #[cfg(test)]
        None,
    )
}

/// Persist access-control metadata and password/device verifier records as one
/// recoverable exact transaction.
pub fn persist_access_control_credential_transaction_at_revision(
    data_dir: impl AsRef<Path>,
    config: &mut crate::Config,
    password_intent: bool,
    device_intents: &BTreeSet<String>,
    expected_revision: u64,
) -> ConfigStoreResult<u64> {
    if !crate::section_layout_is_active(data_dir.as_ref())? {
        return Err(ConfigStoreError::Validation(
            "access-control credential updates require the modular configuration layout"
                .to_string(),
        ));
    }
    persist_exact_credential_transaction_inner(
        data_dir.as_ref(),
        config,
        &BTreeSet::new(),
        &BTreeSet::new(),
        None,
        &BTreeSet::new(),
        None,
        false,
        &BTreeSet::new(),
        false,
        None,
        None,
        None,
        Some((password_intent, device_intents, expected_revision)),
        None,
        None,
        None,
        None,
        #[cfg(test)]
        None,
    )
}

/// Result of a cluster-fabric compound commit whose process-local facade
/// adoption was attempted before the cross-process transaction lock was
/// released.
pub struct ClusterFabricTransactionCommit {
    pub revision: u64,
    pub adoption: Option<ConfigStoreResult<crate::ConfigSectionEvent>>,
    pub credential_adoption: Option<ConfigStoreResult<()>>,
    /// A durable manifest crossed the commit point, but completing/recovering
    /// its members failed. Callers must publish the captured secret-free
    /// candidate and classify this as committed, never as a pre-commit error.
    pub committed_recovery: ConfigStoreResult<()>,
    /// Exact runtime and public credential metadata captured from one
    /// immutable credential document before the transaction lock admitted
    /// another writer. Present for changed and semantic no-op transactions.
    pub runtime: ConfigStoreResult<ClusterFabricRuntimeSnapshot>,
}

pub struct ClusterFabricRuntimeSnapshot {
    pub cluster_fabric: crate::ClusterFabricConfig,
    pub credential_statuses: Vec<crate::CredentialStatus>,
    pub credential_health: crate::CredentialStoreHealth,
}

struct ExactClusterRuntimeSnapshot {
    runtime: ClusterFabricRuntimeSnapshot,
    credential_lkg: crate::credential_store::CredentialDocumentLkg,
}

type ClusterPostCommit<'a> = &'a mut dyn FnMut(
    u64,
    bool,
    &crate::ClusterFabricConfig,
    ConfigStoreResult<ExactClusterRuntimeSnapshot>,
    ConfigStoreResult<()>,
);

fn materialize_cluster_runtime_under_migration_lock(
    config: &crate::Config,
    store: &CredentialStore,
) -> ConfigStoreResult<ExactClusterRuntimeSnapshot> {
    let (credential_lkg, credential_statuses, credential_health) =
        store.snapshot_with_health_unchecked()?;
    let mut runtime = config.clone();
    runtime.hydrate_cluster_credentials_from_snapshot(&credential_lkg)?;
    Ok(ExactClusterRuntimeSnapshot {
        runtime: ClusterFabricRuntimeSnapshot {
            cluster_fabric: runtime.cluster_fabric.clone(),
            credential_statuses,
            credential_health,
        },
        credential_lkg,
    })
}

fn cluster_candidate_from_section_bytes(
    bytes: &[u8],
) -> ConfigStoreResult<(u64, crate::ClusterFabricConfig)> {
    let (_, revision, data) = parse_exact_section_document(CLUSTER_FABRIC_FILE, bytes)?;
    let section: crate::ClusterFabricSection = serde_json::from_value(data)?;
    Ok((revision, section.0))
}

fn load_durable_cluster_candidate_under_migration_lock(
    data_dir: &Path,
) -> ConfigStoreResult<(u64, crate::ClusterFabricConfig)> {
    let bytes = read_file_reject_symlink(&data_dir.join(CLUSTER_FABRIC_FILE))?;
    cluster_candidate_from_section_bytes(&bytes)
}

fn load_staged_cluster_candidate_under_migration_lock(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<(u64, crate::ClusterFabricConfig)> {
    let authority = manifest
        .files
        .iter()
        .find(|file| file.name == CLUSTER_FABRIC_FILE)
        .ok_or_else(|| {
            ConfigStoreError::Validation(
                "cluster transaction authority is missing from its manifest".to_string(),
            )
        })?;
    let bytes = read_managed_file(data_dir, &manifest.stage_dir, &authority.staged_name)?;
    if sha256(&bytes) != authority.sha256 {
        return Err(ConfigStoreError::Validation(
            "staged cluster transaction authority failed integrity validation".to_string(),
        ));
    }
    cluster_candidate_from_section_bytes(&bytes)
}

enum LiveClusterTransaction {
    NotCommitted,
    Aborting,
    Committed(MigrationManifest),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClusterInstallOutcome {
    CommitIntent,
    AbortAttempt,
    Aborting,
}

struct ClusterInstallTracker(Cell<ClusterInstallOutcome>);

impl ClusterInstallTracker {
    fn new() -> Self {
        Self(Cell::new(ClusterInstallOutcome::CommitIntent))
    }

    fn mark_abort_attempt(&self) {
        self.0.set(ClusterInstallOutcome::AbortAttempt);
    }

    fn mark_aborting(&self) {
        self.0.set(ClusterInstallOutcome::Aborting);
    }

    fn outcome(&self) -> ClusterInstallOutcome {
        self.0.get()
    }
}

/// Classify the still-live manifest for this exact cluster transaction.
///
/// `Aborting` is a durable non-commit outcome written before rollback starts.
/// It can therefore never enter committed-candidate adoption, even if rollback
/// or cleanup repeatedly fails. A Complete manifest is different: only an
/// authority that still equals its candidate proves the transaction committed;
/// Complete plus a missing/nonmatching authority is an intentional abort tail.
fn load_live_cluster_transaction_manifest(
    data_dir: &Path,
    expected: &MigrationManifest,
) -> ConfigStoreResult<LiveClusterTransaction> {
    let Some(bytes) = read_optional_migration_file(&data_dir.join(MANIFEST_FILE))? else {
        return Ok(LiveClusterTransaction::NotCommitted);
    };
    let manifest: MigrationManifest = serde_json::from_slice(&bytes)?;
    validate_manifest(&manifest)?;
    if manifest.transaction_id != expected.transaction_id
        || manifest.stage_dir != expected.stage_dir
        || manifest.exact_scope != Some(ExactTransactionScope::ClusterFabric)
    {
        return Err(ConfigStoreError::Validation(
            "cluster transaction manifest no longer matches its commit".to_string(),
        ));
    }
    if manifest.state == MigrationState::Pending
        && matching_cluster_abort_journal(data_dir, &manifest)?
    {
        return Ok(LiveClusterTransaction::Aborting);
    }
    if manifest.state == MigrationState::Aborting {
        return Ok(LiveClusterTransaction::Aborting);
    }
    if manifest.state == MigrationState::Complete {
        let authority = manifest
            .files
            .iter()
            .find(|file| file.name == CLUSTER_FABRIC_FILE)
            .ok_or_else(|| {
                ConfigStoreError::Validation(
                    "cluster transaction authority is missing from its manifest".to_string(),
                )
            })?;
        let current = read_target_or_empty(&data_dir.join(CLUSTER_FABRIC_FILE))?;
        if sha256(&current) != authority.sha256 {
            return Ok(LiveClusterTransaction::NotCommitted);
        }
    }
    Ok(LiveClusterTransaction::Committed(manifest))
}

#[allow(clippy::too_many_arguments)]
fn report_cluster_committed_recovery_failure(
    data_dir: &Path,
    config: &mut crate::Config,
    local_manifest: &MigrationManifest,
    live_manifest: Option<&MigrationManifest>,
    installed_candidate: Option<&(u64, crate::ClusterFabricConfig)>,
    planned_revision: u64,
    initial_error: &ConfigStoreError,
    recovery_error: &ConfigStoreError,
    cluster_post_commit: &mut Option<ClusterPostCommit<'_>>,
) -> u64 {
    let actual = installed_candidate
        .cloned()
        .or_else(|| {
            live_manifest.and_then(|manifest| {
                load_staged_cluster_candidate_under_migration_lock(data_dir, manifest).ok()
            })
        })
        .or_else(|| {
            live_manifest
                .filter(|manifest| manifest.state == MigrationState::Complete)
                .and_then(|_| load_durable_cluster_candidate_under_migration_lock(data_dir).ok())
        })
        .or_else(|| {
            load_staged_cluster_candidate_under_migration_lock(data_dir, local_manifest).ok()
        })
        .unwrap_or_else(|| (planned_revision, config.cluster_fabric.clone()));
    let (revision, candidate) = actual;
    config.cluster_fabric = candidate;
    let message = format!(
        "cluster transaction committed at revision {revision}, completion failed \
         ({initial_error}), and recovery failed ({recovery_error})"
    );
    if let Some(callback) = cluster_post_commit.as_mut() {
        callback(
            revision,
            true,
            &config.cluster_fabric,
            Err(ConfigStoreError::Validation(message.clone())),
            Err(ConfigStoreError::Validation(message)),
        );
    }
    revision
}

/// One coherent durable cluster generation captured under the shared
/// migration lock. The runtime fabric may contain hydrated SSH credentials,
/// so this type deliberately does not implement `Debug` or `Serialize`.
pub struct ExactClusterFabricReadSnapshot {
    pub cluster_fabric: crate::ClusterFabricConfig,
    pub section: crate::SectionEnvelope<Value>,
    pub credential_statuses: Vec<crate::CredentialStatus>,
    pub credential_health: crate::CredentialStoreHealth,
}

/// Recover readiness and read the durable cluster section plus one immutable
/// credential generation while one migration lock excludes compound writers.
///
/// When `expected_revision` is present, the durable section itself is the CAS
/// authority. A stale process facade therefore cannot authorize preflight or
/// another external lifecycle action.
pub fn read_exact_cluster_fabric_snapshot(
    data_dir: impl AsRef<Path>,
    expected_revision: Option<u64>,
) -> ConfigStoreResult<ExactClusterFabricReadSnapshot> {
    let data_dir = data_dir.as_ref();
    with_migration_lock(data_dir, || {
        recover_committed(
            data_dir,
            #[cfg(test)]
            None,
        )?;
        ensure_provider_mcp_migration_ready(data_dir)?;

        let primary_path = data_dir.join(CLUSTER_FABRIC_FILE);
        let store = crate::AtomicJsonStore::<crate::ClusterFabricSection>::new(
            &primary_path,
            crate::section_facade::SECTION_SCHEMA_VERSION,
        )
        .with_raw_validator(crate::section_facade::validate_ordinary_section_raw);
        let stored = store
            .load_validated(crate::section_facade::validate_cluster_fabric)?
            .ok_or_else(|| {
                ConfigStoreError::Validation(
                    "cluster section revision envelope is missing".to_string(),
                )
            })?;
        let revision = stored.revision;
        if let Some(expected) = expected_revision {
            if stored.recovered_from_backup {
                return Err(ConfigStoreError::Validation(
                    "revision-bound cluster actions require a healthy primary section".to_string(),
                ));
            }
            if expected != revision {
                return Err(ConfigStoreError::Conflict {
                    expected,
                    actual: revision,
                });
            }
        }
        let source_kind = if stored.recovered_from_backup {
            crate::SectionSourceKind::Backup
        } else {
            crate::SectionSourceKind::File
        };
        let status = if stored.recovered_from_backup {
            crate::SectionStatus::Degraded
        } else {
            crate::SectionStatus::Healthy
        };
        let last_error = stored
            .recovered_from_backup
            .then(|| "primary document invalid; using last-known-good backup".to_string());
        let data = serde_json::to_value(&stored.data)?;

        let credential_store = CredentialStore::open(data_dir);
        let (credential_lkg, mut credential_statuses, credential_health) =
            match credential_store.snapshot_with_health_unchecked() {
                Ok((lkg, statuses, health)) => (Some(lkg), statuses, health),
                Err(error) if expected_revision.is_some() => return Err(error),
                Err(_) => (
                    None,
                    Vec::new(),
                    crate::CredentialStoreHealth {
                        revision: 0,
                        status: crate::SectionStatus::Degraded,
                        source: crate::SectionSourceKind::Default,
                        last_error: Some("credential status is unavailable".to_string()),
                    },
                ),
            };
        if expected_revision.is_none() {
            if let Some(credential_lkg) = credential_lkg.as_ref() {
                let active_refs = stored
                    .data
                    .0
                    .credential_refs
                    .values()
                    .flat_map(|metadata| metadata.references().cloned())
                    .collect::<BTreeSet<_>>();
                credential_statuses =
                    credential_lkg.statuses_with_cluster_crypto_validation(&active_refs);
            }
        }
        let credential_degraded = credential_health.status == crate::SectionStatus::Degraded;
        if expected_revision.is_some() && credential_degraded {
            return Err(ConfigStoreError::Validation(
                "revision-bound cluster actions require a healthy primary credential section"
                    .to_string(),
            ));
        }
        let mut config = crate::Config::default();
        config.cluster_fabric = stored.data.0;
        // GET needs only secret-free metadata and statuses, so it never
        // materializes plaintext. Revision-bound actions hydrate only after
        // both primary authorities have passed the coherence checks above.
        if expected_revision.is_some() && !stored.recovered_from_backup && !credential_degraded {
            config.hydrate_cluster_credentials_from_snapshot(
                credential_lkg
                    .as_ref()
                    .expect("revision-bound credential snapshot checked above"),
            )?;
        }

        Ok(ExactClusterFabricReadSnapshot {
            cluster_fabric: config.cluster_fabric.clone(),
            section: crate::SectionEnvelope {
                data,
                revision,
                loaded_at: chrono::Utc::now(),
                source_path: stored.source_path,
                source_kind,
                status,
                last_error,
            },
            credential_statuses,
            credential_health,
        })
    })
}

/// Persist node metadata and every explicitly touched SSH credential through
/// the same recoverable credential/config exact transaction used by the other
/// root credential domains.
pub fn persist_cluster_fabric_credential_transaction_at_revision(
    data_dir: impl AsRef<Path>,
    config: &mut crate::Config,
    node_intents: &BTreeMap<String, crate::ClusterNodeCredentialIntents>,
    expected_revision: u64,
) -> ConfigStoreResult<u64> {
    persist_cluster_fabric_credential_transaction_inner(
        data_dir.as_ref(),
        config,
        node_intents,
        expected_revision,
        None,
        None,
    )
}

/// Persist a cluster-fabric compound transaction and adopt its exact immutable
/// candidate into the supplied process facade while the migration lock still
/// excludes later cross-process commits. An adoption failure is returned as a
/// post-commit outcome rather than being confused with a failed durable CAS.
pub fn persist_cluster_fabric_credential_transaction_with_adoption<F>(
    data_dir: impl AsRef<Path>,
    config: &mut crate::Config,
    node_intents: &BTreeMap<String, crate::ClusterNodeCredentialIntents>,
    expected_revision: u64,
    facade: &crate::ConfigFacade,
    mut before_adoption: F,
) -> ConfigStoreResult<ClusterFabricTransactionCommit>
where
    F: FnMut(u64, &crate::ClusterFabricConfig),
{
    let mut adoption = None;
    let mut exact_runtime = None;
    let mut committed_recovery = None;
    let mut transaction_changed = false;
    let mut callback = |revision: u64,
                        changed: bool,
                        candidate: &crate::ClusterFabricConfig,
                        runtime: ConfigStoreResult<ExactClusterRuntimeSnapshot>,
                        recovery: ConfigStoreResult<()>| {
        exact_runtime = Some(runtime);
        committed_recovery = Some(recovery);
        transaction_changed = changed;
        if changed {
            before_adoption(revision, candidate);
            adoption = Some(facade.adopt_committed_cluster_fabric(
                expected_revision,
                revision,
                candidate.clone(),
            ));
        }
    };
    let revision = persist_cluster_fabric_credential_transaction_inner(
        data_dir.as_ref(),
        config,
        node_intents,
        expected_revision,
        Some(&mut callback),
        None,
    )?;
    let (runtime, credential_adoption) = match exact_runtime.unwrap_or_else(|| {
        Err(ConfigStoreError::Validation(
            "cluster transaction omitted exact runtime materialization".to_string(),
        ))
    }) {
        Ok(exact) => {
            let runtime = exact.runtime;
            let credential_adoption = transaction_changed.then(|| {
                facade.adopt_captured_cluster_credentials(
                    exact.credential_lkg,
                    runtime.credential_statuses.clone(),
                    runtime.credential_health.clone(),
                )
            });
            (Ok(runtime), credential_adoption)
        }
        Err(error) => (Err(error), None),
    };
    Ok(ClusterFabricTransactionCommit {
        revision,
        adoption,
        credential_adoption,
        committed_recovery: committed_recovery.unwrap_or_else(|| {
            Err(ConfigStoreError::Validation(
                "cluster transaction omitted its committed recovery outcome".to_string(),
            ))
        }),
        runtime,
    })
}

fn persist_cluster_fabric_credential_transaction_inner(
    data_dir: &Path,
    config: &mut crate::Config,
    node_intents: &BTreeMap<String, crate::ClusterNodeCredentialIntents>,
    expected_revision: u64,
    cluster_post_commit: Option<ClusterPostCommit<'_>>,
    #[cfg_attr(not(test), allow(unused_variables))] fault: Option<MigrationFault>,
) -> ConfigStoreResult<u64> {
    if !crate::section_layout_is_active(data_dir)? {
        return Err(ConfigStoreError::Validation(
            "cluster credential updates require the modular configuration layout".to_string(),
        ));
    }
    persist_exact_credential_transaction_inner(
        data_dir,
        config,
        &BTreeSet::new(),
        &BTreeSet::new(),
        None,
        &BTreeSet::new(),
        None,
        false,
        &BTreeSet::new(),
        false,
        None,
        Some((node_intents, expected_revision)),
        None,
        None,
        None,
        None,
        None,
        cluster_post_commit,
        #[cfg(test)]
        fault,
    )
}

/// Persist built-in and provider-instance API-key updates as one recoverable
/// credential/config transaction. Instance deletes are represented by an
/// intent whose id is absent from `config`; the prior durable credential ref
/// is recovered from `config.json` so custom legacy refs are cleared too.
pub fn persist_provider_instance_credential_transaction(
    data_dir: impl AsRef<Path>,
    config: &mut crate::Config,
    provider_intents: &BTreeSet<String>,
    provider_instance_intents: &BTreeSet<String>,
) -> ConfigStoreResult<()> {
    if provider_intents.contains("__proxy_auth") {
        return Err(ConfigStoreError::Validation(
            "proxy auth requires the dedicated revisioned credential transaction".to_string(),
        ));
    }
    #[cfg(test)]
    return persist_provider_credential_transaction_with_instances_inner(
        data_dir.as_ref(),
        config,
        provider_intents,
        provider_instance_intents,
        None,
        &BTreeSet::new(),
        None,
        false,
        &BTreeSet::new(),
        false,
        None,
        None,
    )
    .map(|_| ());
    #[cfg(not(test))]
    persist_provider_credential_transaction_with_instances_inner(
        data_dir.as_ref(),
        config,
        provider_intents,
        provider_instance_intents,
        None,
        &BTreeSet::new(),
        None,
        false,
        &BTreeSet::new(),
        false,
        None,
    )
    .map(|_| ())
}

/// Persist a complete provider metadata/API-key mutation through the exact
/// provider + credential transaction at the typed provider section revision.
/// Metadata-only updates participate in the same CAS transaction even when
/// the credential document itself has no changed entries.
pub fn persist_provider_credential_transaction_at_revision(
    data_dir: impl AsRef<Path>,
    config: &mut crate::Config,
    provider_intents: &BTreeSet<String>,
    provider_instance_intents: &BTreeSet<String>,
    expected_revision: u64,
) -> ConfigStoreResult<u64> {
    persist_provider_credential_transaction_with_instances_at_revision_inner(
        data_dir.as_ref(),
        config,
        provider_intents,
        provider_instance_intents,
        None,
        &BTreeSet::new(),
        None,
        false,
        &BTreeSet::new(),
        false,
        None,
        Some(expected_revision),
        #[cfg(test)]
        None,
    )
}

/// Reset the provider domain and its owned credentials under the provider
/// section revision supplied by a typed client. Compatibility writes may
/// rebase, but an explicit reset must fail when its section snapshot is stale.
pub fn persist_provider_reset_credential_transaction_at_revision(
    data_dir: impl AsRef<Path>,
    config: &mut crate::Config,
    provider_intents: &BTreeSet<String>,
    provider_instance_intents: &BTreeSet<String>,
    expected_revision: u64,
) -> ConfigStoreResult<u64> {
    persist_provider_credential_transaction_with_instances_at_revision_inner(
        data_dir.as_ref(),
        config,
        provider_intents,
        provider_instance_intents,
        None,
        &BTreeSet::new(),
        None,
        false,
        &BTreeSet::new(),
        false,
        None,
        Some(expected_revision),
        #[cfg(test)]
        None,
    )
}

/// Reset MCP metadata and every MCP-owned credential reference in one exact
/// transaction guarded by the MCP section revision.
pub fn persist_mcp_reset_credential_transaction_at_revision(
    data_dir: impl AsRef<Path>,
    config: &mut crate::Config,
    expected_revision: u64,
) -> ConfigStoreResult<u64> {
    let intents = BTreeSet::new();
    persist_exact_credential_transaction_inner(
        data_dir.as_ref(),
        config,
        &BTreeSet::new(),
        &BTreeSet::new(),
        None,
        &BTreeSet::new(),
        None,
        false,
        &BTreeSet::new(),
        false,
        None,
        None,
        None,
        None,
        None,
        Some((&intents, expected_revision)),
        None,
        None,
        #[cfg(test)]
        None,
    )
}

/// Persist MCP metadata and every explicitly touched environment/header
/// credential in one recoverable exact transaction guarded by the MCP section
/// revision.
pub fn persist_mcp_credential_transaction_at_revision(
    data_dir: impl AsRef<Path>,
    config: &mut crate::Config,
    intents: &BTreeSet<CredentialRef>,
    expected_revision: u64,
) -> ConfigStoreResult<u64> {
    persist_mcp_credential_transaction_inner(
        data_dir.as_ref(),
        config,
        intents,
        expected_revision,
        None,
    )
}

fn persist_mcp_credential_transaction_inner(
    data_dir: &Path,
    config: &mut crate::Config,
    intents: &BTreeSet<CredentialRef>,
    expected_revision: u64,
    #[cfg_attr(not(test), allow(unused_variables))] fault: Option<MigrationFault>,
) -> ConfigStoreResult<u64> {
    if !crate::section_layout_is_active(data_dir)? {
        return Err(ConfigStoreError::Validation(
            "MCP credential updates require the modular configuration layout".to_string(),
        ));
    }
    persist_exact_credential_transaction_inner(
        data_dir,
        config,
        &BTreeSet::new(),
        &BTreeSet::new(),
        None,
        &BTreeSet::new(),
        None,
        false,
        &BTreeSet::new(),
        false,
        None,
        None,
        None,
        None,
        None,
        Some((intents, expected_revision)),
        None,
        None,
        #[cfg(test)]
        fault,
    )
}

/// Reset one credential-backed section at its typed section revision while
/// rebasing safely over unrelated credential-document changes. The section is
/// the client CAS authority; its owned references are cleared from the latest
/// credential snapshot inside the same cross-process transaction lock.
pub fn persist_credential_backed_section_reset_at_revision(
    data_dir: impl AsRef<Path>,
    config: &mut crate::Config,
    section: crate::SectionId,
    expected_revision: u64,
) -> ConfigStoreResult<u64> {
    let scope = match section {
        crate::SectionId::Core => ExactTransactionScope::ProxyAuth,
        crate::SectionId::Notifications => ExactTransactionScope::Notifications,
        crate::SectionId::Connect => ExactTransactionScope::Connect,
        crate::SectionId::Env => ExactTransactionScope::EnvVars,
        crate::SectionId::ClusterFabric => ExactTransactionScope::ClusterFabric,
        crate::SectionId::AccessControl => ExactTransactionScope::AccessControl,
        _ => {
            return Err(ConfigStoreError::Validation(
                "section is not a credential-backed reset domain".to_string(),
            ))
        }
    };
    persist_exact_credential_transaction_inner(
        data_dir.as_ref(),
        config,
        &BTreeSet::new(),
        &BTreeSet::new(),
        None,
        &BTreeSet::new(),
        None,
        false,
        &BTreeSet::new(),
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        Some((scope, expected_revision)),
        None,
        #[cfg(test)]
        None,
    )
}

/// Reset cluster-fabric and adopt its exact committed snapshot into one
/// process facade before the cross-process transaction lock is released.
pub fn persist_cluster_fabric_reset_at_revision_with_adoption<F>(
    data_dir: impl AsRef<Path>,
    config: &mut crate::Config,
    expected_revision: u64,
    facade: &crate::ConfigFacade,
    mut before_adoption: F,
) -> ConfigStoreResult<ClusterFabricTransactionCommit>
where
    F: FnMut(u64, &crate::ClusterFabricConfig),
{
    if !crate::section_layout_is_active(data_dir.as_ref())? {
        return Err(ConfigStoreError::Validation(
            "cluster reset requires the modular configuration layout".to_string(),
        ));
    }
    let mut adoption = None;
    let mut exact_runtime = None;
    let mut committed_recovery = None;
    let mut transaction_changed = false;
    let mut callback = |revision: u64,
                        changed: bool,
                        candidate: &crate::ClusterFabricConfig,
                        runtime: ConfigStoreResult<ExactClusterRuntimeSnapshot>,
                        recovery: ConfigStoreResult<()>| {
        exact_runtime = Some(runtime);
        committed_recovery = Some(recovery);
        transaction_changed = changed;
        if changed {
            before_adoption(revision, candidate);
            adoption = Some(facade.adopt_committed_cluster_fabric(
                expected_revision,
                revision,
                candidate.clone(),
            ));
        }
    };
    let revision = persist_exact_credential_transaction_inner(
        data_dir.as_ref(),
        config,
        &BTreeSet::new(),
        &BTreeSet::new(),
        None,
        &BTreeSet::new(),
        None,
        false,
        &BTreeSet::new(),
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        Some((ExactTransactionScope::ClusterFabric, expected_revision)),
        Some(&mut callback),
        #[cfg(test)]
        None,
    )?;
    let (runtime, credential_adoption) = match exact_runtime.unwrap_or_else(|| {
        Err(ConfigStoreError::Validation(
            "cluster reset omitted exact runtime materialization".to_string(),
        ))
    }) {
        Ok(exact) => {
            let runtime = exact.runtime;
            let credential_adoption = transaction_changed.then(|| {
                facade.adopt_captured_cluster_credentials(
                    exact.credential_lkg,
                    runtime.credential_statuses.clone(),
                    runtime.credential_health.clone(),
                )
            });
            (Ok(runtime), credential_adoption)
        }
        Err(error) => (Err(error), None),
    };
    Ok(ClusterFabricTransactionCommit {
        revision,
        adoption,
        credential_adoption,
        committed_recovery: committed_recovery.unwrap_or_else(|| {
            Err(ConfigStoreError::Validation(
                "cluster reset omitted its committed recovery outcome".to_string(),
            ))
        }),
        runtime,
    })
}

#[cfg(test)]
fn persist_provider_credential_transaction_inner(
    data_dir: &Path,
    config: &mut crate::Config,
    provider_intents: &BTreeSet<String>,
    fault: Option<MigrationFault>,
) -> ConfigStoreResult<()> {
    persist_provider_credential_transaction_with_instances_inner(
        data_dir,
        config,
        provider_intents,
        &BTreeSet::new(),
        provider_intents
            .contains("__proxy_auth")
            .then(|| CredentialStore::open(data_dir).revision_unchecked())
            .transpose()?,
        &BTreeSet::new(),
        None,
        false,
        &BTreeSet::new(),
        false,
        None,
        fault,
    )
    .map(|_| ())
}

#[allow(clippy::too_many_arguments)]
fn persist_provider_credential_transaction_with_instances_inner(
    data_dir: &Path,
    config: &mut crate::Config,
    provider_intents: &BTreeSet<String>,
    provider_instance_intents: &BTreeSet<String>,
    proxy_expected_revision: Option<u64>,
    env_intents: &BTreeSet<String>,
    env_expected_revision: Option<u64>,
    notification_transaction: bool,
    notification_intents: &BTreeSet<String>,
    notification_reset: bool,
    notification_expected_revision: Option<u64>,
    #[cfg(test)] fault: Option<MigrationFault>,
) -> ConfigStoreResult<u64> {
    persist_provider_credential_transaction_with_instances_at_revision_inner(
        data_dir,
        config,
        provider_intents,
        provider_instance_intents,
        proxy_expected_revision,
        env_intents,
        env_expected_revision,
        notification_transaction,
        notification_intents,
        notification_reset,
        notification_expected_revision,
        None,
        #[cfg(test)]
        fault,
    )
}

#[allow(clippy::too_many_arguments)]
fn persist_provider_credential_transaction_with_instances_at_revision_inner(
    data_dir: &Path,
    config: &mut crate::Config,
    provider_intents: &BTreeSet<String>,
    provider_instance_intents: &BTreeSet<String>,
    proxy_expected_revision: Option<u64>,
    env_intents: &BTreeSet<String>,
    env_expected_revision: Option<u64>,
    notification_transaction: bool,
    notification_intents: &BTreeSet<String>,
    notification_reset: bool,
    notification_expected_revision: Option<u64>,
    provider_expected_revision: Option<u64>,
    #[cfg(test)] fault: Option<MigrationFault>,
) -> ConfigStoreResult<u64> {
    persist_exact_credential_transaction_inner(
        data_dir,
        config,
        provider_intents,
        provider_instance_intents,
        proxy_expected_revision,
        env_intents,
        env_expected_revision,
        notification_transaction,
        notification_intents,
        notification_reset,
        notification_expected_revision,
        None,
        None,
        None,
        provider_expected_revision,
        None,
        None,
        None,
        #[cfg(test)]
        fault,
    )
}

#[allow(clippy::too_many_arguments)]
fn persist_exact_credential_transaction_inner(
    data_dir: &Path,
    config: &mut crate::Config,
    provider_intents: &BTreeSet<String>,
    provider_instance_intents: &BTreeSet<String>,
    proxy_expected_revision: Option<u64>,
    env_intents: &BTreeSet<String>,
    env_expected_revision: Option<u64>,
    notification_transaction: bool,
    notification_intents: &BTreeSet<String>,
    notification_reset: bool,
    notification_expected_revision: Option<u64>,
    cluster_transaction: Option<(&BTreeMap<String, crate::ClusterNodeCredentialIntents>, u64)>,
    connect_transaction: Option<(&crate::patch::ConnectSecretIntents, u64)>,
    access_transaction: Option<(bool, &BTreeSet<String>, u64)>,
    provider_expected_revision: Option<u64>,
    mcp_transaction: Option<(&BTreeSet<CredentialRef>, u64)>,
    reset_scope: Option<(ExactTransactionScope, u64)>,
    mut cluster_post_commit: Option<ClusterPostCommit<'_>>,
    #[cfg(test)] fault: Option<MigrationFault>,
) -> ConfigStoreResult<u64> {
    let reset_is = |scope| reset_scope.is_some_and(|(reset, _)| reset == scope);
    let proxy_only = (provider_intents.len() == 1
        && provider_intents.contains("__proxy_auth")
        && provider_instance_intents.is_empty())
        || reset_is(ExactTransactionScope::ProxyAuth);
    let env_only = provider_intents.is_empty()
        && provider_instance_intents.is_empty()
        && (!env_intents.is_empty() || reset_is(ExactTransactionScope::EnvVars));
    let notification_only = provider_intents.is_empty()
        && provider_instance_intents.is_empty()
        && env_intents.is_empty()
        && (notification_transaction || reset_is(ExactTransactionScope::Notifications))
        && cluster_transaction.is_none()
        && connect_transaction.is_none()
        && access_transaction.is_none();
    let cluster_only = provider_intents.is_empty()
        && provider_instance_intents.is_empty()
        && env_intents.is_empty()
        && !notification_transaction
        && (cluster_transaction.is_some() || reset_is(ExactTransactionScope::ClusterFabric))
        && connect_transaction.is_none()
        && access_transaction.is_none();
    let connect_only = provider_intents.is_empty()
        && provider_instance_intents.is_empty()
        && env_intents.is_empty()
        && !notification_transaction
        && cluster_transaction.is_none()
        && (connect_transaction.is_some() || reset_is(ExactTransactionScope::Connect))
        && access_transaction.is_none();
    let access_only = provider_intents.is_empty()
        && provider_instance_intents.is_empty()
        && env_intents.is_empty()
        && !notification_transaction
        && cluster_transaction.is_none()
        && connect_transaction.is_none()
        && (access_transaction.is_some() || reset_is(ExactTransactionScope::AccessControl));
    let mcp_only = mcp_transaction.is_some()
        && provider_intents.is_empty()
        && provider_instance_intents.is_empty()
        && env_intents.is_empty()
        && !notification_transaction
        && cluster_transaction.is_none()
        && connect_transaction.is_none()
        && access_transaction.is_none();
    if !env_intents.is_empty() && !env_only {
        return Err(ConfigStoreError::Validation(
            "env credentials must be updated in their own transaction".to_string(),
        ));
    }
    if !notification_intents.is_empty() && !notification_only {
        return Err(ConfigStoreError::Validation(
            "notification credentials must be updated in their own transaction".to_string(),
        ));
    }
    if cluster_transaction.is_some() && !cluster_only {
        return Err(ConfigStoreError::Validation(
            "cluster credentials must be updated in their own transaction".to_string(),
        ));
    }
    if connect_transaction.is_some() && !connect_only {
        return Err(ConfigStoreError::Validation(
            "connect credentials must be updated in their own transaction".to_string(),
        ));
    }
    if access_transaction.is_some() && !access_only {
        return Err(ConfigStoreError::Validation(
            "access-control credentials must be updated in their own transaction".to_string(),
        ));
    }
    std::fs::create_dir_all(data_dir)?;
    let lock = open_migration_lock(&data_dir.join(LOCK_FILE))?;
    lock.lock_exclusive()?;
    let _lock = MigrationLock(lock);

    cleanup_orphan_transaction_dirs(data_dir)?;
    recover_committed(
        data_dir,
        #[cfg(test)]
        None,
    )?;
    discard_uncommitted(data_dir)?;

    let exact_scope = if let Some((scope, _)) = reset_scope {
        Some(scope)
    } else if proxy_only {
        Some(ExactTransactionScope::ProxyAuth)
    } else if env_only {
        Some(ExactTransactionScope::EnvVars)
    } else if notification_only {
        Some(ExactTransactionScope::Notifications)
    } else if connect_only {
        Some(ExactTransactionScope::Connect)
    } else if access_only {
        Some(ExactTransactionScope::AccessControl)
    } else if cluster_only {
        Some(ExactTransactionScope::ClusterFabric)
    } else if mcp_only {
        Some(ExactTransactionScope::Mcp)
    } else if crate::section_layout_is_active(data_dir)? {
        Some(ExactTransactionScope::Providers)
    } else {
        None
    };
    let active_section = if let Some(scope) = exact_scope {
        crate::section_layout_is_active(data_dir)?
            .then(|| load_active_exact_section(data_dir, scope))
            .transpose()?
    } else {
        None
    };
    if let (Some(expected), Some(section), Some(ExactTransactionScope::Providers)) = (
        provider_expected_revision,
        active_section.as_ref(),
        exact_scope,
    ) {
        if section.expected_revision != expected {
            return Err(ConfigStoreError::Conflict {
                expected,
                actual: section.expected_revision,
            });
        }
    }
    if let (Some(expected), Some(section), Some(ExactTransactionScope::Mcp)) = (
        mcp_transaction.map(|(_, revision)| revision),
        active_section.as_ref(),
        exact_scope,
    ) {
        if section.expected_revision != expected {
            return Err(ConfigStoreError::Conflict {
                expected,
                actual: section.expected_revision,
            });
        }
    }
    if let (Some((_, expected)), Some(section), Some(ExactTransactionScope::ClusterFabric)) =
        (cluster_transaction, active_section.as_ref(), exact_scope)
    {
        if section.expected_revision != expected {
            return Err(ConfigStoreError::Conflict {
                expected,
                actual: section.expected_revision,
            });
        }
    }
    if let (Some((_, expected)), Some(section)) = (reset_scope, active_section.as_ref()) {
        if section.expected_revision != expected {
            return Err(ConfigStoreError::Conflict {
                expected,
                actual: section.expected_revision,
            });
        }
    }

    let credentials_original = read_target_or_empty(&data_dir.join(CREDENTIALS_FILE))?;
    let providers_original = read_target_or_empty(&data_dir.join(PROVIDERS_FILE))?;
    let config_original = read_target_or_empty(&data_dir.join(CONFIG_FILE))?;
    let domain_original = active_section
        .as_ref()
        .map(|section| section.legacy_root.as_slice())
        .unwrap_or(config_original.as_slice());
    let persisted_instance_refs = provider_instance_refs_from_document(
        if exact_scope == Some(ExactTransactionScope::Providers) {
            domain_original
        } else {
            &config_original
        },
    )?;
    let persisted_provider_refs =
        provider_refs_from_document(if exact_scope == Some(ExactTransactionScope::Providers) {
            domain_original
        } else {
            &config_original
        })?;
    let persisted_mcp_refs = if mcp_only {
        mcp_refs_from_document(domain_original)?
    } else {
        BTreeSet::new()
    };
    let persisted_env_refs = env_refs_from_document(domain_original)?;
    let persisted_notification_refs = notification_refs_from_document(domain_original)?;
    let persisted_connect_refs = if connect_only {
        connect_refs_from_document(domain_original)?
    } else {
        BTreeMap::new()
    };
    let persisted_access_refs = if access_only {
        access_refs_from_document(domain_original)?
    } else {
        crate::credential_store::PersistedAccessCredentialRefs::default()
    };
    let persisted_cluster_refs = if cluster_only {
        cluster_node_refs_from_document(domain_original)?
    } else {
        BTreeMap::new()
    };
    let reset_refs = if let Some((scope, _)) = reset_scope {
        let mut references = BTreeSet::new();
        match scope {
            ExactTransactionScope::ProxyAuth => {
                let root: Value = serde_json::from_slice(domain_original)?;
                let reference = root
                    .get("proxy_auth_credential_ref")
                    .and_then(Value::as_str)
                    .map(|value| CredentialRef::parse(value.to_string()))
                    .transpose()?
                    .unwrap_or(credential_ref("proxy", "default", "auth")?);
                references.insert(reference);
            }
            ExactTransactionScope::EnvVars => {
                references.extend(persisted_env_refs.values().cloned());
            }
            ExactTransactionScope::Notifications => {
                references.extend(persisted_notification_refs.values().cloned());
            }
            ExactTransactionScope::Connect => {
                for (token, app_secret) in persisted_connect_refs.values() {
                    references.extend(token.iter().chain(app_secret.iter()).cloned());
                }
            }
            ExactTransactionScope::AccessControl => {
                references.extend(persisted_access_refs.password.iter().cloned());
                references.extend(persisted_access_refs.devices.values().cloned());
            }
            ExactTransactionScope::ClusterFabric => {
                for metadata in persisted_cluster_refs.values() {
                    references.extend(metadata.references().cloned());
                }
            }
            ExactTransactionScope::Providers | ExactTransactionScope::Mcp => {
                unreachable!("provider and MCP resets use their runtime-staged exact transactions")
            }
        }
        references
    } else {
        BTreeSet::new()
    };
    if env_only {
        for name in env_intents {
            if let Some(reference) = persisted_env_refs.get(name) {
                ensure_env_ref_exclusive(data_dir, reference.as_str(), name)?;
            }
        }
    }
    if notification_only {
        for channel in ["ntfy", "bark"] {
            let configured = if channel == "ntfy" {
                config.notifications.ntfy.configured
            } else {
                config.notifications.bark.configured
            };
            if !persisted_notification_refs.contains_key(channel)
                && !notification_intents.contains(channel)
                && !configured
            {
                continue;
            }
            let reference =
                persisted_notification_refs
                    .get(channel)
                    .cloned()
                    .unwrap_or(credential_ref(
                        "notification",
                        channel,
                        if channel == "ntfy" {
                            "token"
                        } else {
                            "device_key"
                        },
                    )?);
            ensure_notification_ref_exclusive(data_dir, reference.as_str(), channel)?;
        }
    }
    if let Some((node_intents, _)) = cluster_transaction {
        let mut refs = BTreeSet::new();
        for node_id in node_intents.keys() {
            refs.insert(crate::cluster_password_credential_ref(node_id)?);
            refs.insert(crate::cluster_private_key_credential_ref(node_id)?);
            refs.insert(crate::cluster_passphrase_credential_ref(node_id)?);
            if let Some(metadata) = persisted_cluster_refs.get(node_id) {
                refs.extend(metadata.references().cloned());
            }
        }
        ensure_cluster_refs_are_safe(data_dir, &refs, &[])?;
    }
    if connect_only {
        for (id, (token_ref, app_secret_ref)) in &persisted_connect_refs {
            if let Some(reference) = token_ref {
                ensure_connect_ref_exclusive(data_dir, reference.as_str(), id, "token")?;
            }
            if let Some(reference) = app_secret_ref {
                ensure_connect_ref_exclusive(data_dir, reference.as_str(), id, "app_secret")?;
            }
        }
    }
    if let Some((password_intent, device_intents, _)) = access_transaction {
        if let Some(reference) = persisted_access_refs.password.as_ref() {
            ensure_access_ref_exclusive(data_dir, reference.as_str(), None)?;
        } else if password_intent {
            let reference = crate::config_crypto::access_password_credential_ref()?;
            ensure_access_ref_exclusive(data_dir, reference.as_str(), None)?;
        }
        for device_id in device_intents {
            let reference = persisted_access_refs
                .devices
                .get(device_id)
                .cloned()
                .map(Ok)
                .unwrap_or_else(|| crate::config_crypto::access_device_credential_ref(device_id))?;
            ensure_access_ref_exclusive(data_dir, reference.as_str(), Some(device_id))?;
        }
    }
    let store = CredentialStore::open(data_dir);
    let prepared = if reset_scope.is_some() {
        Some(store.prepare_clear_owned_references(config, &reset_refs)?)
    } else if env_only {
        store.prepare_env_var_intents(config, env_intents, &persisted_env_refs)?
    } else if notification_only {
        store.prepare_notification_intents(
            config,
            notification_intents,
            &persisted_notification_refs,
            notification_reset,
        )?
    } else if let Some((node_intents, _)) = cluster_transaction {
        store.prepare_cluster_node_intents(config, node_intents, &persisted_cluster_refs)?
    } else if let Some((connect_intents, _)) = connect_transaction {
        store.prepare_connect_intents(config, connect_intents, &persisted_connect_refs)?
    } else if let Some((password_intent, device_intents, _)) = access_transaction {
        store.prepare_access_control_intents(
            config,
            password_intent,
            device_intents,
            &persisted_access_refs,
        )?
    } else if let Some((intents, _)) = mcp_transaction {
        if intents.is_empty() {
            Some(store.prepare_clear_owned_references(config, &persisted_mcp_refs)?)
        } else {
            store.prepare_mcp_intents(config, intents, &persisted_mcp_refs)?
        }
    } else {
        store.prepare_provider_api_key_intents(
            config,
            provider_intents,
            &persisted_provider_refs,
            provider_instance_intents,
            &persisted_instance_refs,
        )?
    };
    let mut prepared = match prepared {
        Some(prepared) => prepared,
        None if provider_expected_revision.is_some() || cluster_transaction.is_some() => {
            store.prepare_provider_metadata_update()?
        }
        None => return store.revision_unchecked(),
    };
    if proxy_only && reset_scope.is_none() {
        let expected = proxy_expected_revision.ok_or_else(|| {
            ConfigStoreError::Validation(
                "proxy auth credential revision precondition is required".to_string(),
            )
        })?;
        if prepared.expected_revision != expected {
            return Err(ConfigStoreError::Conflict {
                expected,
                actual: prepared.expected_revision,
            });
        }
    }
    if env_only && reset_scope.is_none() {
        let expected = env_expected_revision.ok_or_else(|| {
            ConfigStoreError::Validation(
                "env credential revision precondition is required".to_string(),
            )
        })?;
        if prepared.expected_revision != expected {
            return Err(ConfigStoreError::Conflict {
                expected,
                actual: prepared.expected_revision,
            });
        }
    }
    if notification_only && reset_scope.is_none() {
        let expected = notification_expected_revision.ok_or_else(|| {
            ConfigStoreError::Validation(
                "notification credential revision precondition is required".to_string(),
            )
        })?;
        if prepared.expected_revision != expected {
            return Err(ConfigStoreError::Conflict {
                expected,
                actual: prepared.expected_revision,
            });
        }
    }
    if let Some((_, expected)) = connect_transaction {
        if prepared.expected_revision != expected {
            return Err(ConfigStoreError::Conflict {
                expected,
                actual: prepared.expected_revision,
            });
        }
    }
    if let Some((_, _, expected)) = access_transaction {
        if prepared.expected_revision != expected {
            return Err(ConfigStoreError::Conflict {
                expected,
                actual: prepared.expected_revision,
            });
        }
    }
    if proxy_only && active_section.is_some() && prepared.required_refs.is_empty() {
        config.proxy_auth_credential_ref = None;
    }
    let (config_bytes, provider_bytes) = if proxy_only {
        (
            prepare_proxy_auth_config_document(domain_original, config)?,
            providers_original.clone(),
        )
    } else if env_only {
        (
            prepare_env_var_config_document(domain_original, config)?,
            providers_original.clone(),
        )
    } else if notification_only {
        (
            prepare_notification_config_document(
                domain_original,
                config,
                notification_reset || reset_is(ExactTransactionScope::Notifications),
            )?,
            providers_original.clone(),
        )
    } else if cluster_only {
        (
            prepare_cluster_fabric_config_document(domain_original, config)?,
            providers_original.clone(),
        )
    } else if connect_only {
        (
            prepare_connect_config_document(domain_original, config)?,
            providers_original.clone(),
        )
    } else if access_only {
        (
            prepare_access_control_config_document(domain_original, config)?,
            providers_original.clone(),
        )
    } else if mcp_only {
        (
            serde_json::to_vec_pretty(&config.mcp)?,
            providers_original.clone(),
        )
    } else {
        config
            .prepare_provider_transaction_documents(&providers_original)
            .map_err(|error| ConfigStoreError::Validation(error.to_string()))?
    };
    let (
        authority_name,
        authority_original,
        authority_bytes,
        authority_expected_revision,
        active_domain_changed,
    ) = if let (Some(section), Some(ExactTransactionScope::Providers)) =
        (active_section.as_ref(), exact_scope)
    {
        let (_, _, original_data) = parse_exact_section_document(section.name, &section.original)?;
        let candidate_data = crate::section_facade::provider_section_value(config)?;
        let candidate_revision = section.expected_revision.checked_add(1).ok_or_else(|| {
            ConfigStoreError::Validation("provider section revision counter exhausted".to_string())
        })?;
        let mut candidate_envelope: Value = serde_json::from_slice(&section.original)?;
        let candidate_object = candidate_envelope.as_object_mut().ok_or_else(|| {
            ConfigStoreError::Validation("provider section envelope is invalid".to_string())
        })?;
        candidate_object.insert("revision".to_string(), Value::from(candidate_revision));
        candidate_object.insert("data".to_string(), candidate_data.clone());
        let provider_section_bytes = serde_json::to_vec_pretty(&candidate_envelope)?;
        crate::section_facade::validate_section_envelope(
            section.name,
            &provider_section_bytes,
            candidate_revision,
        )?;
        (
            section.name,
            section.original.as_slice(),
            provider_section_bytes,
            Some(section.expected_revision),
            original_data != candidate_data,
        )
    } else if let (Some(section), Some(scope)) = (active_section.as_ref(), exact_scope) {
        let credential_changed =
            (mcp_only || cluster_only) && prepared.revision != prepared.expected_revision;
        let (bytes, changed) = prepare_active_exact_section_document(
            section,
            scope,
            &config_bytes,
            credential_changed,
        )?;
        (
            section.name,
            section.original.as_slice(),
            bytes,
            Some(section.expected_revision),
            changed,
        )
    } else {
        (
            CONFIG_FILE,
            config_original.as_slice(),
            config_bytes.clone(),
            None,
            false,
        )
    };
    let env_domain_changed = env_only && env_var_domain_changed(domain_original, &config_bytes)?;
    let notification_domain_changed =
        notification_only && notification_domain_changed(domain_original, &config_bytes)?;
    let cluster_domain_changed =
        cluster_only && cluster_fabric_domain_changed(domain_original, &config_bytes)?;
    let connect_domain_changed =
        connect_only && connect_domain_changed(domain_original, &config_bytes)?;
    let access_domain_changed =
        access_only && access_control_domain_changed(domain_original, &config_bytes)?;
    let exact_domain_changed = active_domain_changed
        || env_domain_changed
        || notification_domain_changed
        || cluster_domain_changed
        || connect_domain_changed
        || access_domain_changed;
    if exact_domain_changed && !cluster_only {
        prepared.advance_revision_for_domain_change()?;
    }
    let committed_authority_revision = if cluster_only && active_section.is_some() {
        crate::section_facade::validate_section_envelope(authority_name, &authority_bytes, 0)?
    } else {
        prepared.revision
    };
    #[cfg(test)]
    if fault == Some(MigrationFault::BeforeExactStagingUnrelatedCredentialRace) {
        store.replace_unchecked(
            credential_ref("provider", "anthropic", "api_key")?,
            "concurrent-pre-staging-winner",
            crate::CredentialSource::User,
            prepared.expected_revision,
        )?;
    }
    let current_credential_revision = store.revision_unchecked()?;
    if current_credential_revision != prepared.expected_revision && !cluster_only {
        return Err(ConfigStoreError::Conflict {
            expected: prepared.expected_revision,
            actual: current_credential_revision,
        });
    }
    // Cluster-fabric's section revision is the public CAS authority. If an
    // unrelated credential writer advances this internal member after
    // preparation, stage the immutable base/candidate pair and let the exact
    // installer perform its touched-ref three-way merge.
    // A true env-domain no-op keeps the CAS revision and avoids publishing an
    // event or rewriting either durable member. Any credential or config
    // semantic change advances the one shared revision exactly once.
    if (env_only
        || notification_only
        || cluster_only
        || connect_only
        || access_only
        || active_section.is_some())
        && !exact_domain_changed
        && prepared.revision == prepared.expected_revision
    {
        if cluster_only && active_section.is_some() {
            if let Some(callback) = cluster_post_commit.as_mut() {
                let runtime = materialize_cluster_runtime_under_migration_lock(config, &store);
                callback(
                    committed_authority_revision,
                    false,
                    &config.cluster_fabric,
                    runtime,
                    Ok(()),
                );
            }
        }
        return Ok(committed_authority_revision);
    }

    let transaction_id = Uuid::new_v4().to_string();
    let stage_dir_name = format!("{STAGE_PREFIX}{transaction_id}");
    let stage_dir = data_dir.join(&stage_dir_name);
    let backup_dir = data_dir.join(format!("{BACKUP_PREFIX}{transaction_id}"));
    create_private_dir(&stage_dir)?;
    create_private_dir(&backup_dir)?;
    sync_dir(data_dir)?;
    let mut staged = Vec::new();
    stage_file(
        &stage_dir,
        &backup_dir,
        CREDENTIALS_FILE,
        &prepared.bytes,
        Some(&credentials_original),
        true,
        None,
        InstallMode::Exact,
        Some(prepared.expected_revision),
        &mut staged,
    )?;
    let credential_file = staged
        .last_mut()
        .expect("credential transaction stages credentials first");
    credential_file.touched_credential_refs = prepared
        .touched_refs
        .iter()
        .map(|reference| reference.as_str().to_string())
        .collect();
    credential_file.required_credential_refs = prepared
        .required_refs
        .iter()
        .map(|reference| reference.as_str().to_string())
        .collect();
    credential_file.transaction_base_sha256 = credential_file.original_sha256.clone();
    if exact_scope.is_none() {
        stage_file(
            &stage_dir,
            &backup_dir,
            PROVIDERS_FILE,
            &provider_bytes,
            Some(&providers_original),
            false,
            None,
            InstallMode::Exact,
            None,
            &mut staged,
        )?;
    }
    stage_file(
        &stage_dir,
        &backup_dir,
        authority_name,
        &authority_bytes,
        Some(authority_original),
        false,
        None,
        InstallMode::Exact,
        authority_expected_revision,
        &mut staged,
    )?;
    if env_only {
        staged
            .last_mut()
            .expect("env transaction stages its authority last")
            .touched_env_names = env_intents.iter().cloned().collect();
    }
    restrict_directory_files_to_owner(&stage_dir)?;
    restrict_directory_files_to_owner(&backup_dir)?;
    sync_dir(&stage_dir)?;
    sync_dir(&backup_dir)?;
    #[cfg(test)]
    if fault == Some(MigrationFault::AfterStaging) {
        return Err(injected_fault());
    }

    let manifest = MigrationManifest {
        version: MIGRATION_VERSION,
        transaction_id,
        stage_dir: stage_dir_name,
        state: MigrationState::Pending,
        exact_scope,
        migration_scope: None,
        source_attestation: BTreeMap::new(),
        files: staged,
    };
    write_manifest(data_dir.join(JOURNAL_FILE), &manifest)?;

    #[cfg(test)]
    if matches!(
        fault,
        Some(
            MigrationFault::BeforeExactCommitCredentialRace
                | MigrationFault::BeforeExactCommitUnrelatedCredentialRace
        )
    ) {
        let reference = if fault == Some(MigrationFault::BeforeExactCommitCredentialRace) {
            prepared.touched_refs.first().cloned().ok_or_else(|| {
                ConfigStoreError::Validation("credential intent is empty".to_string())
            })?
        } else {
            credential_ref("provider", "anthropic", "api_key")?
        };
        store.replace_unchecked(
            reference,
            "concurrent-winner",
            crate::CredentialSource::User,
            prepared.expected_revision,
        )?;
    }

    // Recheck every CAS immediately before the durable commit point. A loser
    // leaves only an uncommitted journal, which the next run safely discards.
    for file in &manifest.files {
        let current = read_target_or_empty(&data_dir.join(&file.name))?;
        let current_sha256 = sha256(&current);
        if file.original_sha256.as_deref() != Some(current_sha256.as_str()) {
            if file.name == CREDENTIALS_FILE {
                if exact_scope == Some(ExactTransactionScope::ClusterFabric) {
                    // The cluster section is the external CAS authority. Commit
                    // the manifest against its still-current revision and let
                    // `install_exact_credential_member` three-way rebase this
                    // internal member over unrelated credential winners.
                    continue;
                }
                return Err(ConfigStoreError::Conflict {
                    expected: file.expected_revision.unwrap_or(0),
                    actual: store.revision_unchecked()?,
                });
            }
            if let Some(expected) = file.expected_revision {
                let actual =
                    crate::section_facade::validate_section_envelope(&file.name, &current, 0)?;
                if actual != expected {
                    return Err(ConfigStoreError::Conflict { expected, actual });
                }
                return Err(ConfigStoreError::Validation(format!(
                    "{} reused revision {expected} with different content",
                    file.name
                )));
            }
            return Err(ConfigStoreError::Validation(format!(
                "{} changed during provider credential transaction",
                file.name
            )));
        }
    }
    write_manifest(data_dir.join(MANIFEST_FILE), &manifest)?;
    #[cfg(any(test, feature = "test-utils"))]
    let cluster_fault_after_manifest = cluster_only
        && active_section.is_some()
        && take_cluster_exact_after_manifest_test_fault(data_dir);
    #[cfg(not(any(test, feature = "test-utils")))]
    let cluster_fault_after_manifest = false;
    if cluster_fault_after_manifest && cluster_post_commit.is_none() {
        return Err(ConfigStoreError::Validation(
            "injected cluster exact transaction fault after manifest".to_string(),
        ));
    }
    #[cfg(feature = "test-utils")]
    if env_only {
        if let Some(hook) = ENV_TRANSACTION_TEST_HOOK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            hook(data_dir);
        }
    }
    #[cfg(test)]
    if matches!(
        fault,
        Some(
            MigrationFault::AfterExactCommitCredentialRace
                | MigrationFault::AfterExactCommitUnrelatedCredentialRace
                | MigrationFault::AfterExactCommitCredentialClearRace
                | MigrationFault::AfterExactCredentialRebaseStage
                | MigrationFault::AfterExactCredentialRebaseManifest
        )
    ) {
        let clear_same_ref = fault == Some(MigrationFault::AfterExactCommitCredentialClearRace);
        let same_ref =
            clear_same_ref || fault == Some(MigrationFault::AfterExactCommitCredentialRace);
        let reference = if same_ref {
            prepared.touched_refs.first().cloned().ok_or_else(|| {
                ConfigStoreError::Validation("credential intent is empty".to_string())
            })?
        } else {
            credential_ref("provider", "anthropic", "api_key")?
        };
        if clear_same_ref {
            store.clear_unchecked(&reference, prepared.expected_revision)?;
        } else {
            store.replace_unchecked(
                reference,
                if same_ref {
                    "concurrent-post-commit-winner"
                } else {
                    "concurrent-unrelated-winner"
                },
                crate::CredentialSource::User,
                prepared.expected_revision,
            )?;
        }
    }
    #[cfg(test)]
    if fault == Some(MigrationFault::AfterExactClusterConsumerConflict) {
        let reference = prepared.touched_refs.first().ok_or_else(|| {
            ConfigStoreError::Validation("cluster credential intent is empty".to_string())
        })?;
        AtomicFileStore::new(data_dir.join(PROVIDERS_FILE)).write_bytes_without_backup(
            &serde_json::to_vec_pretty(&serde_json::json!({
                "test_external_consumer": {
                    "credential_ref": reference.as_str()
                }
            }))?,
        )?;
    }
    #[cfg(test)]
    if fault == Some(MigrationFault::AfterExactClusterSectionRebase) {
        let target = data_dir.join(CLUSTER_FABRIC_FILE);
        let current = read_file_reject_symlink(&target)?;
        let current_hash = sha256(&current);
        let (mut envelope, revision, mut data) =
            parse_exact_section_document(CLUSTER_FABRIC_FILE, &current)?;
        let external = crate::Node {
            id: "external-rebase-node".to_string(),
            label: "external-rebase-node".to_string(),
            placement: crate::NodePlacement::Local,
            trust_level: crate::TrustLevel::Trusted,
            deploy: crate::DeployProfile::default(),
            state: None,
            enabled: true,
        };
        data.as_object_mut()
            .ok_or_else(|| {
                ConfigStoreError::Validation("cluster section data is invalid".to_string())
            })?
            .entry("nodes".to_string())
            .or_insert_with(|| Value::Array(Vec::new()))
            .as_array_mut()
            .ok_or_else(|| {
                ConfigStoreError::Validation("cluster node collection is invalid".to_string())
            })?
            .push(serde_json::to_value(external)?);
        let next_revision = revision.checked_add(1).ok_or_else(|| {
            ConfigStoreError::Validation("cluster section revision counter exhausted".to_string())
        })?;
        let object = envelope.as_object_mut().ok_or_else(|| {
            ConfigStoreError::Validation("cluster section envelope is invalid".to_string())
        })?;
        object.insert("revision".to_string(), Value::from(next_revision));
        object.insert("data".to_string(), data);
        let bytes = serde_json::to_vec_pretty(&envelope)?;
        crate::section_facade::validate_section_envelope(
            CLUSTER_FABRIC_FILE,
            &bytes,
            next_revision,
        )?;
        if !AtomicFileStore::new(&target).write_bytes_if_hash(&current_hash, &bytes)? {
            return Err(ConfigStoreError::Validation(
                "cluster section changed during test race injection".to_string(),
            ));
        }
    }
    #[cfg(test)]
    if fault == Some(MigrationFault::AfterManifest) {
        return Err(injected_fault());
    }
    let mut manifest = manifest;
    let adopted_cluster_transaction =
        cluster_only && active_section.is_some() && cluster_post_commit.is_some();
    let cluster_install_tracker = adopted_cluster_transaction.then(ClusterInstallTracker::new);
    let mut installed_cluster_candidate = None;
    let completion = if cluster_fault_after_manifest {
        Err(ConfigStoreError::Validation(
            "injected cluster exact transaction fault after manifest".to_string(),
        ))
    } else {
        match install_pending(
            data_dir,
            &mut manifest,
            cluster_install_tracker.as_ref(),
            #[cfg(test)]
            fault,
        ) {
            Err(error) => Err(error),
            Ok(()) => {
                let capture = adopted_cluster_transaction
                    .then(|| load_durable_cluster_candidate_under_migration_lock(data_dir))
                    .transpose();
                if let Err(error) = capture {
                    Err(error)
                } else {
                    installed_cluster_candidate = capture.expect("checked above");
                    #[cfg(any(test, feature = "test-utils"))]
                    let before_finish_fault = cluster_only
                        && active_section.is_some()
                        && take_cluster_exact_commit_test_fault(
                            data_dir,
                            ClusterExactCommitTestFault::BeforeFinish,
                        );
                    #[cfg(not(any(test, feature = "test-utils")))]
                    let before_finish_fault = false;
                    if before_finish_fault {
                        Err(ConfigStoreError::Validation(
                            "injected cluster exact transaction fault before finish".to_string(),
                        ))
                    } else {
                        finish_transaction(data_dir, manifest.clone())
                    }
                }
            }
        }
    };
    if let Err(initial_error) = completion {
        if cluster_install_tracker
            .as_ref()
            .is_some_and(|tracker| tracker.outcome() != ClusterInstallOutcome::CommitIntent)
        {
            // Once an intentional abort has started, never infer commit or
            // publish the staged candidate in this process. `AbortAttempt`
            // covers the indeterminate case where neither redundant durable
            // marker could be confirmed; `Aborting` covers either marker
            // succeeding before rollback or cleanup failed.
            return Err(initial_error);
        }
        if !adopted_cluster_transaction {
            return Err(initial_error);
        }
        let live_before_recovery = match load_live_cluster_transaction_manifest(data_dir, &manifest)
        {
            Ok(LiveClusterTransaction::NotCommitted | LiveClusterTransaction::Aborting) => {
                return Err(initial_error);
            }
            Ok(LiveClusterTransaction::Committed(live)) => live,
            Err(inspection_error) => {
                let revision = report_cluster_committed_recovery_failure(
                    data_dir,
                    config,
                    &manifest,
                    None,
                    installed_cluster_candidate.as_ref(),
                    committed_authority_revision,
                    &initial_error,
                    &inspection_error,
                    &mut cluster_post_commit,
                );
                return Ok(revision);
            }
        };
        let recovery = {
            #[cfg(any(test, feature = "test-utils"))]
            if take_cluster_exact_commit_test_fault(
                data_dir,
                ClusterExactCommitTestFault::AfterManifestRecoveryFailure,
            ) {
                Err(ConfigStoreError::Validation(
                    "injected cluster exact transaction recovery failure".to_string(),
                ))
            } else {
                recover_committed(
                    data_dir,
                    #[cfg(test)]
                    None,
                )
            }
            #[cfg(not(any(test, feature = "test-utils")))]
            recover_committed(
                data_dir,
                #[cfg(test)]
                None,
            )
        };
        match recovery {
            Ok(_) => match load_durable_cluster_candidate_under_migration_lock(data_dir) {
                Ok(actual) => installed_cluster_candidate = Some(actual),
                Err(load_error) => {
                    let revision = report_cluster_committed_recovery_failure(
                        data_dir,
                        config,
                        &manifest,
                        Some(&live_before_recovery),
                        installed_cluster_candidate.as_ref(),
                        committed_authority_revision,
                        &initial_error,
                        &load_error,
                        &mut cluster_post_commit,
                    );
                    return Ok(revision);
                }
            },
            Err(recovery_error) => {
                let live_after_recovery =
                    match load_live_cluster_transaction_manifest(data_dir, &manifest) {
                        Ok(
                            LiveClusterTransaction::NotCommitted | LiveClusterTransaction::Aborting,
                        ) => return Err(recovery_error),
                        Ok(LiveClusterTransaction::Committed(live)) => Some(live),
                        Err(inspection_error) => {
                            let combined = ConfigStoreError::Validation(format!(
                                "{recovery_error}; cluster transaction state inspection failed \
                                 ({inspection_error})"
                            ));
                            let revision = report_cluster_committed_recovery_failure(
                                data_dir,
                                config,
                                &manifest,
                                None,
                                installed_cluster_candidate.as_ref(),
                                committed_authority_revision,
                                &initial_error,
                                &combined,
                                &mut cluster_post_commit,
                            );
                            return Ok(revision);
                        }
                    };
                let revision = report_cluster_committed_recovery_failure(
                    data_dir,
                    config,
                    &manifest,
                    live_after_recovery.as_ref(),
                    installed_cluster_candidate.as_ref(),
                    committed_authority_revision,
                    &initial_error,
                    &recovery_error,
                    &mut cluster_post_commit,
                );
                return Ok(revision);
            }
        }
    }
    if cluster_only && active_section.is_some() {
        let (actual_revision, actual_candidate) = installed_cluster_candidate
            .or_else(|| load_durable_cluster_candidate_under_migration_lock(data_dir).ok())
            .ok_or_else(|| {
                ConfigStoreError::Validation(
                    "committed cluster transaction authority is unavailable".to_string(),
                )
            })?;
        config.cluster_fabric = actual_candidate;
        if let Some(callback) = cluster_post_commit.as_mut() {
            let runtime = materialize_cluster_runtime_under_migration_lock(config, &store);
            callback(
                actual_revision,
                true,
                &config.cluster_fabric,
                runtime,
                Ok(()),
            );
        }
        Ok(actual_revision)
    } else {
        // Credential-member rebases can advance beyond the initially staged
        // revision. Preserve the existing exact-transaction return contract
        // for every non-cluster domain.
        store.revision_unchecked()
    }
}

#[cfg(test)]
fn migrate_with_fault(
    data_dir: &Path,
    fault: MigrationFault,
) -> ConfigStoreResult<CredentialMigrationOutcome> {
    migrate_inner(data_dir, Some(fault))
}

#[cfg(test)]
fn migrate_broker_with_fault(
    data_dir: &Path,
    fault: MigrationFault,
) -> ConfigStoreResult<CredentialMigrationOutcome> {
    migrate_broker_inner(data_dir, Some(fault))
}

fn migrate_broker_inner(
    data_dir: &Path,
    #[cfg_attr(not(test), allow(unused_variables))] fault: Option<MigrationFault>,
) -> ConfigStoreResult<CredentialMigrationOutcome> {
    with_migration_lock(data_dir, || migrate_broker_locked(data_dir, fault))
}

fn migrate_broker_locked(
    data_dir: &Path,
    #[cfg_attr(not(test), allow(unused_variables))] fault: Option<MigrationFault>,
) -> ConfigStoreResult<CredentialMigrationOutcome> {
    cleanup_orphan_transaction_dirs(data_dir)?;
    if let Some(outcome) = recover_committed(
        data_dir,
        #[cfg(test)]
        fault,
    )? {
        return Ok(outcome);
    }
    discard_uncommitted(data_dir)?;

    let mut extracted = Vec::new();
    let broker = plan_broker_section(data_dir, &mut extracted, 1)?;
    if broker.is_none() {
        let migrated_credentials = scrub_broker_credentials_from_backups(data_dir)?;
        return Ok(CredentialMigrationOutcome {
            migrated_credentials,
            resumed: false,
        });
    }

    let credential_original = read_optional_target(&data_dir.join(CREDENTIALS_FILE))?;
    let store = CredentialStore::open(data_dir);
    let resolved = resolve_extracted_secrets(&store, extracted)?;
    let prepared_credentials = store.prepare_migration(resolved)?;
    let broker = broker.expect("checked above");

    let transaction_id = Uuid::new_v4().to_string();
    let stage_dir_name = format!("{STAGE_PREFIX}{transaction_id}");
    let stage_dir = data_dir.join(&stage_dir_name);
    let backup_dir = data_dir.join(format!("{BACKUP_PREFIX}{transaction_id}"));
    create_private_dir(&stage_dir)?;
    create_private_dir(&backup_dir)?;
    sync_dir(data_dir)?;

    let mut staged = Vec::new();
    stage_file(
        &stage_dir,
        &backup_dir,
        CREDENTIALS_FILE,
        &prepared_credentials.bytes,
        credential_original.as_deref(),
        true,
        None,
        InstallMode::Migration,
        None,
        &mut staged,
    )?;
    stage_file(
        &stage_dir,
        &backup_dir,
        broker.name,
        &broker.bytes,
        Some(&broker.original),
        false,
        Some(broker.migration_generation),
        InstallMode::Migration,
        None,
        &mut staged,
    )?;
    restrict_directory_files_to_owner(&stage_dir)?;
    restrict_directory_files_to_owner(&backup_dir)?;
    sync_dir(&stage_dir)?;
    sync_dir(&backup_dir)?;
    #[cfg(test)]
    if fault == Some(MigrationFault::AfterStaging) {
        return Err(injected_fault());
    }

    let manifest = MigrationManifest {
        version: MIGRATION_VERSION,
        transaction_id,
        stage_dir: stage_dir_name,
        state: MigrationState::Pending,
        exact_scope: None,
        migration_scope: None,
        source_attestation: BTreeMap::new(),
        files: staged,
    };
    write_manifest(data_dir.join(JOURNAL_FILE), &manifest)?;
    #[cfg(test)]
    if fault == Some(MigrationFault::AfterJournal) {
        return Err(injected_fault());
    }
    write_manifest(data_dir.join(MANIFEST_FILE), &manifest)?;
    #[cfg(test)]
    if fault == Some(MigrationFault::AfterManifest) {
        return Err(injected_fault());
    }

    let mut manifest = manifest;
    install_pending(
        data_dir,
        &mut manifest,
        None,
        #[cfg(test)]
        fault,
    )?;
    finish_transaction(data_dir, manifest)?;
    Ok(CredentialMigrationOutcome {
        migrated_credentials: prepared_credentials.added,
        resumed: false,
    })
}

#[cfg(test)]
fn migrate_cluster_with_fault(
    data_dir: &Path,
    fault: MigrationFault,
) -> ConfigStoreResult<CredentialMigrationOutcome> {
    migrate_cluster_inner(data_dir, Some(fault))
}

fn migrate_cluster_inner(
    data_dir: &Path,
    #[cfg_attr(not(test), allow(unused_variables))] fault: Option<MigrationFault>,
) -> ConfigStoreResult<CredentialMigrationOutcome> {
    with_migration_lock(data_dir, || migrate_cluster_locked(data_dir, fault))
}

fn migrate_cluster_locked(
    data_dir: &Path,
    #[cfg_attr(not(test), allow(unused_variables))] fault: Option<MigrationFault>,
) -> ConfigStoreResult<CredentialMigrationOutcome> {
    cleanup_orphan_transaction_dirs(data_dir)?;
    let resumed = recover_committed(
        data_dir,
        #[cfg(test)]
        None,
    )?
    .is_some();
    discard_uncommitted(data_dir)?;

    let mut extracted = Vec::new();
    let section = plan_cluster_section(data_dir, &mut extracted, 1, true)?;
    let credential_original = read_optional_target(&data_dir.join(CREDENTIALS_FILE))?;
    let store = CredentialStore::open(data_dir);
    if let Some(section) = section {
        ensure_cluster_extractions_are_safe(
            data_dir,
            &extracted,
            &[(section.bytes.as_slice(), true)],
        )?;
        let resolved = resolve_extracted_secrets(&store, extracted)?;
        let prepared_credentials = store.prepare_migration(resolved)?;
        let transaction_id = Uuid::new_v4().to_string();
        let stage_dir_name = format!("{STAGE_PREFIX}{transaction_id}");
        let stage_dir = data_dir.join(&stage_dir_name);
        let backup_dir = data_dir.join(format!("{BACKUP_PREFIX}{transaction_id}"));
        create_private_dir(&stage_dir)?;
        create_private_dir(&backup_dir)?;
        sync_dir(data_dir)?;

        let mut staged = Vec::new();
        stage_file(
            &stage_dir,
            &backup_dir,
            CREDENTIALS_FILE,
            &prepared_credentials.bytes,
            credential_original.as_deref(),
            true,
            None,
            InstallMode::Migration,
            None,
            &mut staged,
        )?;
        stage_file(
            &stage_dir,
            &backup_dir,
            CONFIG_FILE,
            &section.bytes,
            Some(&section.original),
            false,
            Some(section.migration_generation),
            InstallMode::Migration,
            None,
            &mut staged,
        )?;
        restrict_directory_files_to_owner(&stage_dir)?;
        restrict_directory_files_to_owner(&backup_dir)?;
        sync_dir(&stage_dir)?;
        sync_dir(&backup_dir)?;

        let manifest = MigrationManifest {
            version: MIGRATION_VERSION,
            transaction_id,
            stage_dir: stage_dir_name,
            state: MigrationState::Pending,
            exact_scope: None,
            migration_scope: Some(MigrationScope::ClusterFabric),
            source_attestation: BTreeMap::new(),
            files: staged,
        };
        write_manifest(data_dir.join(JOURNAL_FILE), &manifest)?;
        write_manifest(data_dir.join(MANIFEST_FILE), &manifest)?;
        #[cfg(test)]
        if fault == Some(MigrationFault::AfterManifest) {
            return Err(injected_fault());
        }
        let mut manifest = manifest;
        install_pending(
            data_dir,
            &mut manifest,
            None,
            #[cfg(test)]
            fault,
        )?;
        let added = prepared_credentials.added;
        finish_transaction(data_dir, manifest)?;
        return Ok(CredentialMigrationOutcome {
            migrated_credentials: added,
            resumed,
        });
    }

    let migrated_credentials = scrub_cluster_credentials_from_backups(data_dir)?;
    Ok(CredentialMigrationOutcome {
        migrated_credentials,
        resumed,
    })
}

fn pending_cluster_migration(data_dir: &Path) -> ConfigStoreResult<bool> {
    let Some(bytes) = read_optional_migration_file(&data_dir.join(MANIFEST_FILE))? else {
        return Ok(false);
    };
    let manifest: MigrationManifest = serde_json::from_slice(&bytes)?;
    validate_manifest(&manifest)?;
    let pending = manifest.state == MigrationState::Pending
        && manifest.migration_scope == Some(MigrationScope::ClusterFabric);
    if pending && matching_cluster_abort_journal(data_dir, &manifest)? {
        return Ok(false);
    }
    Ok(pending)
}

fn pending_cluster_refs_are_exclusive(data_dir: &Path) -> ConfigStoreResult<bool> {
    let Some(bytes) = read_optional_migration_file(&data_dir.join(MANIFEST_FILE))? else {
        return Ok(false);
    };
    let manifest: MigrationManifest = serde_json::from_slice(&bytes)?;
    validate_manifest(&manifest)?;
    if manifest.state != MigrationState::Pending
        || manifest.migration_scope != Some(MigrationScope::ClusterFabric)
    {
        return Ok(false);
    }
    let config_file = manifest
        .files
        .iter()
        .find(|file| file.name == CONFIG_FILE)
        .ok_or_else(|| {
            ConfigStoreError::Validation(
                "committed cluster credential transaction is incomplete".to_string(),
            )
        })?;
    let staged = read_managed_file(data_dir, &manifest.stage_dir, &config_file.staged_name)?;
    if sha256(&staged) != config_file.sha256 {
        return Err(ConfigStoreError::Validation(
            "staged cluster configuration failed integrity validation".to_string(),
        ));
    }
    let mut refs = cluster_refs_from_document(&staged)?;
    refs.extend(cluster_refs_from_document(&read_target_or_empty(
        &data_dir.join(CONFIG_FILE),
    )?)?);
    for suffix in ["bak", "bak.1", "bak.2"] {
        let bytes =
            match read_file_reject_symlink(&data_dir.join(format!("{CONFIG_FILE}.{suffix}"))) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
        if serde_json::from_slice::<Value>(&bytes).is_err() {
            continue;
        }
        refs.extend(cluster_refs_from_document(&bytes)?);
        let mut extracted = Vec::new();
        if let Ok(Some(planned)) = plan_cluster_document(bytes, &mut extracted, 1) {
            refs.extend(cluster_refs_from_document(&planned.bytes)?);
            refs.extend(extracted.into_iter().map(|secret| secret.credential_ref));
        }
    }
    ensure_cluster_refs_are_safe(data_dir, &refs, &[(staged.as_slice(), true)])?;
    Ok(true)
}

fn provider_mcp_documents_are_migration_free(data_dir: &Path) -> ConfigStoreResult<bool> {
    let mut extracted = Vec::new();
    let providers = plan_provider_section(data_dir, &mut extracted, 1)?;
    let mcp = plan_mcp_section(data_dir, &mut extracted, 1)?;
    let root = plan_provider_instance_section(data_dir, &mut extracted, 1, true, false)?;
    if providers.is_some() || mcp.is_some() || root.is_some() || !extracted.is_empty() {
        return Ok(false);
    }
    for suffix in ["bak", "bak.1", "bak.2"] {
        let bytes =
            match read_file_reject_symlink(&data_dir.join(format!("{CONFIG_FILE}.{suffix}"))) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
        if serde_json::from_slice::<Value>(&bytes).is_err() {
            continue;
        }
        let mut backup_extracted = Vec::new();
        if plan_provider_instance_document(bytes, &mut backup_extracted, 1, false)?.is_some()
            || !backup_extracted.is_empty()
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn migrate_inner(
    data_dir: &Path,
    #[cfg_attr(not(test), allow(unused_variables))] fault: Option<MigrationFault>,
) -> ConfigStoreResult<CredentialMigrationOutcome> {
    migrate_inner_with_root_builtins(data_dir, fault, false)
}

fn migrate_inner_with_root_builtins(
    data_dir: &Path,
    #[cfg_attr(not(test), allow(unused_variables))] fault: Option<MigrationFault>,
    include_root_builtins: bool,
) -> ConfigStoreResult<CredentialMigrationOutcome> {
    with_migration_lock(data_dir, || {
        migrate_inner_with_root_builtins_locked(data_dir, fault, include_root_builtins)
    })
}

fn migrate_inner_with_root_builtins_locked(
    data_dir: &Path,
    #[cfg_attr(not(test), allow(unused_variables))] fault: Option<MigrationFault>,
    include_root_builtins: bool,
) -> ConfigStoreResult<CredentialMigrationOutcome> {
    cleanup_orphan_transaction_dirs(data_dir)?;
    match recover_committed(
        data_dir,
        #[cfg(test)]
        fault,
    ) {
        Ok(Some(outcome)) => return Ok(outcome),
        Ok(None) => {}
        Err(_error)
            if pending_cluster_migration(data_dir)?
                && provider_mcp_documents_are_migration_free(data_dir)?
                && pending_cluster_refs_are_exclusive(data_dir)? =>
        {
            // A committed cluster transaction may be waiting on cluster-only
            // backup repair. Provider/MCP readers can safely retain their
            // already-isolated documents: the atomic credential envelope
            // changes only canonical cluster refs, and no second provider
            // migration manifest is started while this one is pending.
            return Ok(CredentialMigrationOutcome {
                migrated_credentials: 0,
                resumed: false,
            });
        }
        Err(error) => return Err(error),
    }
    discard_uncommitted(data_dir)?;

    let mut extracted = Vec::new();
    let providers = plan_provider_section(data_dir, &mut extracted, 1)?;
    let mcp = plan_mcp_section(data_dir, &mut extracted, 1)?;
    let provider_instances =
        plan_provider_instance_section(data_dir, &mut extracted, 1, true, include_root_builtins)?;
    let provider_facade_scope = include_root_builtins && provider_instances.is_some();
    let credential_original = read_optional_target(&data_dir.join(CREDENTIALS_FILE))?;
    let credential_store = CredentialStore::open(data_dir);
    let prospective_documents = [&providers, &mcp, &provider_instances]
        .into_iter()
        .filter_map(Option::as_ref)
        .map(|section| (section.bytes.as_slice(), section.name == CONFIG_FILE))
        .collect::<Vec<_>>();
    ensure_legacy_env_extractions_are_safe(data_dir, &extracted, &prospective_documents)?;
    ensure_legacy_notification_extractions_are_safe(data_dir, &extracted, &prospective_documents)?;
    ensure_legacy_proxy_extractions_are_safe(
        data_dir,
        &credential_store,
        &extracted,
        &prospective_documents,
    )?;
    ensure_backup_legacy_proxy_extractions_are_safe(data_dir, &credential_store)?;
    if providers.is_none() && mcp.is_none() && provider_instances.is_none() {
        scrub_provider_instance_credentials_from_backups(
            data_dir,
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
        )?;
        return Ok(CredentialMigrationOutcome {
            migrated_credentials: 0,
            resumed: false,
        });
    }

    let resolved = resolve_extracted_secrets(&credential_store, extracted)?;
    let prepared_credentials = credential_store.prepare_migration(resolved)?;

    let transaction_id = Uuid::new_v4().to_string();
    let stage_dir_name = format!("{STAGE_PREFIX}{transaction_id}");
    let stage_dir = data_dir.join(&stage_dir_name);
    let backup_dir = data_dir.join(format!("{BACKUP_PREFIX}{transaction_id}"));
    create_private_dir(&stage_dir)?;
    create_private_dir(&backup_dir)?;
    sync_dir(data_dir)?;

    let mut staged = Vec::new();
    stage_file(
        &stage_dir,
        &backup_dir,
        CREDENTIALS_FILE,
        &prepared_credentials.bytes,
        credential_original.as_deref(),
        true,
        None,
        InstallMode::Migration,
        None,
        &mut staged,
    )?;
    for section in providers.into_iter().chain(mcp).chain(provider_instances) {
        stage_file(
            &stage_dir,
            &backup_dir,
            section.name,
            &section.bytes,
            Some(&section.original),
            false,
            Some(section.migration_generation),
            InstallMode::Migration,
            None,
            &mut staged,
        )?;
    }
    restrict_directory_files_to_owner(&stage_dir)?;
    restrict_directory_files_to_owner(&backup_dir)?;
    sync_dir(&stage_dir)?;
    sync_dir(&backup_dir)?;
    #[cfg(test)]
    if fault == Some(MigrationFault::AfterStaging) {
        return Err(injected_fault());
    }

    let manifest = MigrationManifest {
        version: MIGRATION_VERSION,
        transaction_id,
        stage_dir: stage_dir_name,
        state: MigrationState::Pending,
        exact_scope: None,
        migration_scope: if provider_facade_scope {
            Some(MigrationScope::ProviderFacade)
        } else {
            None
        },
        source_attestation: BTreeMap::new(),
        files: staged,
    };
    write_manifest(data_dir.join(JOURNAL_FILE), &manifest)?;
    #[cfg(test)]
    if fault == Some(MigrationFault::AfterJournal) {
        return Err(injected_fault());
    }

    // Commit point. Every candidate and pre-migration backup is durable before
    // this manifest becomes visible.
    write_manifest(data_dir.join(MANIFEST_FILE), &manifest)?;
    #[cfg(test)]
    if fault == Some(MigrationFault::AfterManifest) {
        return Err(injected_fault());
    }

    let mut manifest = manifest;
    install_pending(
        data_dir,
        &mut manifest,
        None,
        #[cfg(test)]
        fault,
    )?;
    finish_transaction(data_dir, manifest)?;
    Ok(CredentialMigrationOutcome {
        migrated_credentials: prepared_credentials.added,
        resumed: false,
    })
}

fn resolve_extracted_secrets(
    store: &CredentialStore,
    extracted: Vec<ExtractedSecret>,
) -> ConfigStoreResult<Vec<(CredentialRef, String, u64)>> {
    let mut resolved = Vec::new();
    for secret in extracted {
        // A user replacement is authoritative over any stale legacy copy. This
        // also makes a fresh plan after an uncommitted crash idempotent.
        let status = store.status_unchecked(&secret.credential_ref)?;
        if status.configured && status.source != crate::CredentialSource::Migrated {
            if secret.kind == ExtractedSecretKind::EnvVar {
                return Err(ConfigStoreError::Validation(
                    "legacy env credential target is already user-managed".to_string(),
                ));
            }
            continue;
        }
        let value = match secret.value {
            LegacySecret::Plaintext(value) => value,
            LegacySecret::Ciphertext(value) => {
                crate::encryption::decrypt(&value).map_err(|_| {
                    ConfigStoreError::Validation(
                        "legacy credential could not be decrypted for migration".to_string(),
                    )
                })?
            }
        };
        resolved.push((secret.credential_ref, value, secret.migration_generation));
    }
    Ok(resolved)
}

fn resolve_extracted_secrets_from_sources(
    sources: &BTreeMap<CredentialRef, crate::CredentialSource>,
    extracted: Vec<ExtractedSecret>,
) -> ConfigStoreResult<Vec<(CredentialRef, String, u64)>> {
    let mut resolved = Vec::new();
    for secret in extracted {
        if sources
            .get(&secret.credential_ref)
            .is_some_and(|source| *source != crate::CredentialSource::Migrated)
        {
            if secret.kind == ExtractedSecretKind::EnvVar {
                return Err(ConfigStoreError::Validation(
                    "legacy env credential target is already user-managed".to_string(),
                ));
            }
            continue;
        }
        let value = match secret.value {
            LegacySecret::Plaintext(value) => value,
            LegacySecret::Ciphertext(value) => {
                crate::encryption::decrypt(&value).map_err(|_| {
                    ConfigStoreError::Validation(
                        "legacy credential could not be decrypted for migration".to_string(),
                    )
                })?
            }
        };
        resolved.push((secret.credential_ref, value, secret.migration_generation));
    }
    Ok(resolved)
}

fn resolve_compound_extracted_secrets_from_sources(
    sources: &BTreeMap<CredentialRef, crate::CredentialSource>,
    primary: Vec<ExtractedSecret>,
    backup_generations: Vec<Vec<ExtractedSecret>>,
) -> ConfigStoreResult<Vec<(CredentialRef, String, u64)>> {
    let mut selected = BTreeMap::<CredentialRef, (String, u64)>::new();

    // A durable credential entry is already authoritative. Otherwise every
    // live primary participates at the same precedence level: contradictory
    // copies fail closed instead of silently choosing a file ordering.
    for secret in primary {
        if let Some(source) = sources.get(&secret.credential_ref) {
            if *source != crate::CredentialSource::Migrated {
                if secret.kind == ExtractedSecretKind::EnvVar {
                    return Err(ConfigStoreError::Validation(
                        "legacy env credential target is already user-managed".to_string(),
                    ));
                }
                continue;
            }
        }
        insert_precedence_secret(&mut selected, secret)?;
    }

    // Rotating backups are historical, not competing authorities. The newest
    // generation that owns a reference supplies its value; older generations
    // are still scrubbed by their planned cleanup but cannot overwrite it.
    let mut claimed = sources.keys().cloned().collect::<BTreeSet<_>>();
    claimed.extend(selected.keys().cloned());
    for generation in backup_generations {
        let mut generation_selected = BTreeMap::<CredentialRef, (String, u64)>::new();
        for secret in generation {
            if let Some(source) = sources.get(&secret.credential_ref) {
                if secret.kind == ExtractedSecretKind::EnvVar
                    && *source != crate::CredentialSource::Migrated
                {
                    return Err(ConfigStoreError::Validation(
                        "legacy env credential target is already user-managed".to_string(),
                    ));
                }
                continue;
            }
            if claimed.contains(&secret.credential_ref) {
                continue;
            }
            insert_precedence_secret(&mut generation_selected, secret)?;
        }
        claimed.extend(generation_selected.keys().cloned());
        selected.extend(generation_selected);
    }

    Ok(selected
        .into_iter()
        .map(|(reference, (secret, generation))| (reference, secret, generation))
        .collect())
}

fn insert_precedence_secret(
    selected: &mut BTreeMap<CredentialRef, (String, u64)>,
    secret: ExtractedSecret,
) -> ConfigStoreResult<()> {
    let value = match secret.value {
        LegacySecret::Plaintext(value) => value,
        LegacySecret::Ciphertext(value) => crate::encryption::decrypt(&value).map_err(|_| {
            ConfigStoreError::Validation(
                "legacy credential could not be decrypted for migration".to_string(),
            )
        })?,
    };
    match selected.get_mut(&secret.credential_ref) {
        Some((existing, generation)) if *existing == value => {
            *generation = (*generation).max(secret.migration_generation);
        }
        Some(_) => {
            return Err(ConfigStoreError::Validation(
                "conflicting legacy credentials share a credential reference".to_string(),
            ));
        }
        None => {
            selected.insert(secret.credential_ref, (value, secret.migration_generation));
        }
    }
    Ok(())
}

fn ensure_legacy_proxy_extractions_are_safe(
    data_dir: &Path,
    store: &CredentialStore,
    extracted: &[ExtractedSecret],
    prospective_documents: &[(&[u8], bool)],
) -> ConfigStoreResult<()> {
    for secret in extracted
        .iter()
        .filter(|secret| secret.kind == ExtractedSecretKind::ProxyAuth)
    {
        ensure_no_durable_non_proxy_consumers(data_dir, secret.credential_ref.as_str())?;
        for (bytes, config_root) in prospective_documents {
            ensure_no_non_proxy_consumers_in_document(
                bytes,
                secret.credential_ref.as_str(),
                *config_root,
                "prospective configuration",
            )?;
        }
        let status = store.status_unchecked(&secret.credential_ref)?;
        if status.configured && status.source != crate::CredentialSource::Migrated {
            return Err(ConfigStoreError::Validation(
                "legacy proxy auth target credential is already user-managed".to_string(),
            ));
        }
    }
    Ok(())
}

fn ensure_legacy_proxy_extractions_are_safe_from_sources(
    data_dir: &Path,
    sources: &BTreeMap<CredentialRef, crate::CredentialSource>,
    extracted: &[ExtractedSecret],
    prospective_documents: &[(&[u8], bool)],
) -> ConfigStoreResult<()> {
    for secret in extracted
        .iter()
        .filter(|secret| secret.kind == ExtractedSecretKind::ProxyAuth)
    {
        ensure_no_durable_non_proxy_consumers(data_dir, secret.credential_ref.as_str())?;
        for (bytes, config_root) in prospective_documents {
            ensure_no_non_proxy_consumers_in_document(
                bytes,
                secret.credential_ref.as_str(),
                *config_root,
                "prospective configuration",
            )?;
        }
        if sources
            .get(&secret.credential_ref)
            .is_some_and(|source| *source != crate::CredentialSource::Migrated)
        {
            return Err(ConfigStoreError::Validation(
                "legacy proxy auth target credential is already user-managed".to_string(),
            ));
        }
    }
    Ok(())
}

fn ensure_legacy_env_extractions_are_safe(
    data_dir: &Path,
    extracted: &[ExtractedSecret],
    prospective_documents: &[(&[u8], bool)],
) -> ConfigStoreResult<()> {
    for secret in extracted
        .iter()
        .filter(|secret| secret.kind == ExtractedSecretKind::EnvVar)
    {
        let owner = secret.env_owner.as_deref().ok_or_else(|| {
            ConfigStoreError::Validation("legacy env credential owner is missing".to_string())
        })?;
        ensure_env_ref_exclusive(data_dir, secret.credential_ref.as_str(), owner)?;
        for (bytes, _) in prospective_documents {
            let value: Value = serde_json::from_slice(bytes)?;
            if contains_other_env_credential_consumer(&value, secret.credential_ref.as_str(), owner)
            {
                return Err(ConfigStoreError::Validation(
                    "env credential reference is shared by another consumer".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn ensure_legacy_notification_extractions_are_safe(
    data_dir: &Path,
    extracted: &[ExtractedSecret],
    prospective_documents: &[(&[u8], bool)],
) -> ConfigStoreResult<()> {
    for secret in extracted.iter().filter(|secret| {
        matches!(
            secret.kind,
            ExtractedSecretKind::NotificationNtfy | ExtractedSecretKind::NotificationBark
        )
    }) {
        let channel = match secret.kind {
            ExtractedSecretKind::NotificationNtfy => "ntfy",
            ExtractedSecretKind::NotificationBark => "bark",
            _ => unreachable!("filtered above"),
        };
        for name in [PROVIDERS_FILE, MCP_FILE, CONFIG_FILE] {
            let bytes = read_target_or_empty(&data_dir.join(name))?;
            if notification_document_has_other_consumer(
                &bytes,
                secret.credential_ref.as_str(),
                channel,
                name == CONFIG_FILE,
            )? {
                return Err(ConfigStoreError::Validation(
                    "notification credential reference is shared by another consumer".to_string(),
                ));
            }
        }
        for (bytes, config_root) in prospective_documents {
            if notification_document_has_other_consumer(
                bytes,
                secret.credential_ref.as_str(),
                channel,
                *config_root,
            )? {
                return Err(ConfigStoreError::Validation(
                    "notification credential reference is shared by another consumer".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn ensure_broker_ref_exclusive(
    data_dir: &Path,
    reference: &CredentialRef,
) -> ConfigStoreResult<()> {
    for name in [PROVIDERS_FILE, MCP_FILE, CONFIG_FILE] {
        for suffix in ["", ".bak", ".bak.1", ".bak.2"] {
            let path = data_dir.join(format!("{name}{suffix}"));
            let bytes = read_target_or_empty(&path)?;
            if bytes.is_empty() {
                continue;
            }
            let value: Value = match serde_json::from_slice(&bytes) {
                Ok(value) => value,
                Err(_) if !suffix.is_empty() => continue,
                Err(error) => return Err(error.into()),
            };
            if contains_credential_reference(&value, reference.as_str()) {
                return Err(ConfigStoreError::Validation(
                    "broker credential reference is shared by another consumer".to_string(),
                ));
            }
        }
    }
    let broker_bytes = read_target_or_empty(&data_dir.join(BROKER_FILE))?;
    if !broker_bytes.is_empty() {
        let mut broker: Value = serde_json::from_slice(&broker_bytes)?;
        if let Some(object) = broker.as_object_mut() {
            object.remove("credential_ref");
        }
        if contains_credential_reference(&broker, reference.as_str()) {
            return Err(ConfigStoreError::Validation(
                "broker credential reference is shared inside broker configuration".to_string(),
            ));
        }
    }
    Ok(())
}

fn ensure_broker_backup_ownership(
    data_dir: &Path,
    preferred_ref: &CredentialRef,
) -> ConfigStoreResult<()> {
    for suffix in ["bak", "bak.1", "bak.2"] {
        let bytes =
            match read_file_reject_symlink(&data_dir.join(format!("{BROKER_FILE}.{suffix}"))) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
        broker_backup_candidate(&bytes, Some(preferred_ref))?;
        let mut value: Value = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if let Some(object) = value.as_object_mut() {
            object.remove("credential_ref");
        }
        if contains_credential_reference(&value, preferred_ref.as_str()) {
            return Err(ConfigStoreError::Validation(
                "broker credential reference is shared inside broker backup".to_string(),
            ));
        }
    }
    Ok(())
}

fn contains_credential_reference(value: &Value, expected: &str) -> bool {
    match value {
        Value::Object(object) => object.iter().any(|(key, child)| {
            ((key == "credential_ref" || key.ends_with("_credential_ref"))
                && child.as_str() == Some(expected))
                || (key.ends_with("_credential_refs") && contains_string_value(child, expected))
                || contains_credential_reference(child, expected)
        }),
        Value::Array(values) => values
            .iter()
            .any(|child| contains_credential_reference(child, expected)),
        _ => false,
    }
}

fn notification_document_has_other_consumer(
    bytes: &[u8],
    reference: &str,
    channel: &str,
    config_root: bool,
) -> ConfigStoreResult<bool> {
    if bytes.is_empty() {
        return Ok(false);
    }
    let mut value: Value = serde_json::from_slice(bytes)?;
    if config_root {
        let owner = if value.get("notifications").is_some() {
            Some(&mut value)
        } else {
            value.get_mut("data")
        };
        if let Some(config) = owner
            .and_then(|owner| owner.get_mut("notifications"))
            .and_then(Value::as_object_mut)
            .and_then(|notifications| notifications.get_mut(channel))
            .and_then(Value::as_object_mut)
        {
            if config.get("credential_ref").and_then(Value::as_str) == Some(reference) {
                config.remove("credential_ref");
            }
        }
    }
    Ok(contains_notification_credential_reference(
        &value, reference,
    ))
}

fn contains_notification_credential_reference(value: &Value, expected: &str) -> bool {
    match value {
        Value::Array(values) => values
            .iter()
            .any(|value| contains_notification_credential_reference(value, expected)),
        Value::Object(object) => object.iter().any(|(key, value)| {
            let credential_field = key == "credential_ref"
                || key.ends_with("_credential_ref")
                || key.ends_with("_credential_refs");
            (credential_field && contains_string_value(value, expected))
                || contains_notification_credential_reference(value, expected)
        }),
        _ => false,
    }
}

fn ensure_connect_ref_exclusive(
    data_dir: &Path,
    reference: &str,
    platform_id: &str,
    secret_field: &str,
) -> ConfigStoreResult<()> {
    for name in [
        PROVIDERS_FILE,
        MCP_FILE,
        BROKER_FILE,
        CONFIG_FILE,
        CONNECT_FILE,
    ] {
        for suffix in ["", ".bak", ".bak.1", ".bak.2"] {
            let path = data_dir.join(format!("{name}{suffix}"));
            let bytes = read_target_or_empty(&path)?;
            if bytes.is_empty() {
                continue;
            }
            let mut value: Value = match serde_json::from_slice(&bytes) {
                Ok(value) => value,
                Err(_) if !suffix.is_empty() => continue,
                Err(error) if name == CONFIG_FILE || name == CONNECT_FILE => {
                    return Err(error.into());
                }
                Err(_) => continue,
            };
            strip_connect_owned_reference(&mut value, reference, platform_id, secret_field);
            if contains_credential_reference(&value, reference) {
                return Err(ConfigStoreError::Validation(
                    "connect credential reference is shared by another consumer".to_string(),
                ));
            }
        }
    }
    for descriptor in crate::SECTION_DESCRIPTORS.iter().filter(|descriptor| {
        descriptor.id != crate::SectionId::Credentials
            && !matches!(
                descriptor.file_name,
                PROVIDERS_FILE | MCP_FILE | CONNECT_FILE
            )
    }) {
        let bytes = read_target_or_empty(&data_dir.join(descriptor.file_name))?;
        if bytes.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_slice(&bytes)?;
        if contains_credential_reference(&value, reference) {
            return Err(ConfigStoreError::Validation(
                "connect credential reference is shared by another consumer".to_string(),
            ));
        }
    }
    Ok(())
}

fn ensure_access_ref_exclusive(
    data_dir: &Path,
    reference: &str,
    owner: Option<&str>,
) -> ConfigStoreResult<()> {
    for name in [
        PROVIDERS_FILE,
        MCP_FILE,
        BROKER_FILE,
        CONFIG_FILE,
        ACCESS_CONTROL_FILE,
    ] {
        for suffix in ["", ".bak", ".bak.1", ".bak.2"] {
            let bytes = read_target_or_empty(&data_dir.join(format!("{name}{suffix}")))?;
            if bytes.is_empty() {
                continue;
            }
            let mut value: Value = match serde_json::from_slice(&bytes) {
                Ok(value) => value,
                Err(_) if !suffix.is_empty() => continue,
                Err(error) if name == CONFIG_FILE || name == ACCESS_CONTROL_FILE => {
                    return Err(error.into());
                }
                Err(_) => continue,
            };
            strip_access_owned_reference(&mut value, reference, owner);
            if contains_credential_reference(&value, reference) {
                return Err(ConfigStoreError::Validation(
                    "access-control credential reference is shared by another consumer".to_string(),
                ));
            }
        }
    }
    for descriptor in crate::SECTION_DESCRIPTORS.iter().filter(|descriptor| {
        descriptor.id != crate::SectionId::Credentials
            && !matches!(
                descriptor.file_name,
                PROVIDERS_FILE | MCP_FILE | ACCESS_CONTROL_FILE
            )
    }) {
        let bytes = read_target_or_empty(&data_dir.join(descriptor.file_name))?;
        if bytes.is_empty() {
            continue;
        }
        let mut value: Value = serde_json::from_slice(&bytes)?;
        strip_access_owned_reference(&mut value, reference, owner);
        if contains_credential_reference(&value, reference) {
            return Err(ConfigStoreError::Validation(
                "access-control credential reference is shared by another consumer".to_string(),
            ));
        }
    }
    Ok(())
}

fn strip_access_owned_reference(value: &mut Value, reference: &str, owner: Option<&str>) {
    match value {
        Value::Object(object) => {
            if owner.is_none()
                && object.contains_key("password_enabled")
                && object
                    .get("password_credential_ref")
                    .and_then(Value::as_str)
                    == Some(reference)
            {
                object.remove("password_credential_ref");
            }
            if owner.is_some()
                && object.get("device_id").and_then(Value::as_str) == owner
                && object.get("token_credential_ref").and_then(Value::as_str) == Some(reference)
            {
                object.remove("token_credential_ref");
            }
            for child in object.values_mut() {
                strip_access_owned_reference(child, reference, owner);
            }
        }
        Value::Array(values) => {
            for child in values {
                strip_access_owned_reference(child, reference, owner);
            }
        }
        _ => {}
    }
}

fn strip_connect_owned_reference(
    value: &mut Value,
    reference: &str,
    platform_id: &str,
    secret_field: &str,
) {
    match value {
        Value::Object(object) => {
            if let Some(platforms) = object.get_mut("platforms").and_then(Value::as_array_mut) {
                let reference_key = format!("{secret_field}_credential_ref");
                for platform in platforms.iter_mut().filter_map(Value::as_object_mut) {
                    if platform.get("id").and_then(Value::as_str) == Some(platform_id)
                        && platform.get(&reference_key).and_then(Value::as_str) == Some(reference)
                    {
                        platform.remove(&reference_key);
                    }
                }
            }
            for child in object.values_mut() {
                strip_connect_owned_reference(child, reference, platform_id, secret_field);
            }
        }
        Value::Array(values) => {
            for child in values {
                strip_connect_owned_reference(child, reference, platform_id, secret_field);
            }
        }
        _ => {}
    }
}

fn ensure_env_ref_exclusive(
    data_dir: &Path,
    reference: &str,
    owner: &str,
) -> ConfigStoreResult<()> {
    for name in [
        CONFIG_FILE,
        PROVIDERS_FILE,
        MCP_FILE,
        "config.json.bak",
        "config.json.bak.1",
        "config.json.bak.2",
    ] {
        let bytes = read_target_or_empty(&data_dir.join(name))?;
        if bytes.is_empty() {
            continue;
        }
        let value: Value = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(_) if name.starts_with("config.json.bak") => continue,
            Err(error) => return Err(error.into()),
        };
        if contains_other_env_credential_consumer(&value, reference, owner) {
            return Err(ConfigStoreError::Validation(
                "env credential reference is shared by another consumer".to_string(),
            ));
        }
    }
    for descriptor in crate::SECTION_DESCRIPTORS.iter().filter(|descriptor| {
        descriptor.id != crate::SectionId::Credentials
            && !matches!(descriptor.file_name, PROVIDERS_FILE | MCP_FILE)
    }) {
        let bytes = read_target_or_empty(&data_dir.join(descriptor.file_name))?;
        if bytes.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_slice(&bytes)?;
        if contains_other_env_credential_consumer(&value, reference, owner) {
            return Err(ConfigStoreError::Validation(
                "env credential reference is shared by another consumer".to_string(),
            ));
        }
    }
    Ok(())
}

fn contains_other_env_credential_consumer(value: &Value, reference: &str, owner: &str) -> bool {
    match value {
        Value::Object(object) => {
            let owned_env_entry = object.get("name").and_then(Value::as_str) == Some(owner)
                && object.contains_key("secret");
            object.iter().any(|(key, child)| {
                if owned_env_entry && key == "credential_ref" {
                    return false;
                }
                ((key == "credential_ref" || key.ends_with("_credential_ref"))
                    && child.as_str() == Some(reference))
                    || (key.ends_with("_credential_refs")
                        && contains_string_value(child, reference))
                    || contains_other_env_credential_consumer(child, reference, owner)
            })
        }
        Value::Array(values) => values
            .iter()
            .any(|child| contains_other_env_credential_consumer(child, reference, owner)),
        _ => false,
    }
}

fn ensure_backup_legacy_proxy_extractions_are_safe(
    data_dir: &Path,
    store: &CredentialStore,
) -> ConfigStoreResult<()> {
    for suffix in ["bak", "bak.1", "bak.2"] {
        let bytes =
            match read_file_reject_symlink(&data_dir.join(format!("{CONFIG_FILE}.{suffix}"))) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
        let mut root: Value = match serde_json::from_slice(&bytes) {
            Ok(root) => root,
            Err(_) => continue,
        };
        let Some(object) = root.as_object_mut() else {
            continue;
        };
        let mut extracted = Vec::new();
        if !scrub_authoritative_or_tombstoned_proxy_auth(object, store, &BTreeSet::new())? {
            migrate_proxy_auth(object, &mut extracted, 1)?;
        }
        ensure_legacy_proxy_extractions_are_safe(
            data_dir,
            store,
            &extracted,
            &[(bytes.as_slice(), true)],
        )?;
    }
    Ok(())
}

fn ensure_backup_legacy_proxy_extractions_are_safe_from_sources(
    data_dir: &Path,
    sources: &BTreeMap<CredentialRef, crate::CredentialSource>,
) -> ConfigStoreResult<()> {
    for suffix in ["bak", "bak.1", "bak.2"] {
        let bytes =
            match read_file_reject_symlink(&data_dir.join(format!("{CONFIG_FILE}.{suffix}"))) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
        let mut root: Value = match serde_json::from_slice(&bytes) {
            Ok(root) => root,
            Err(_) => continue,
        };
        let Some(object) = root.as_object_mut() else {
            continue;
        };
        let reference = object
            .get("proxy_auth_credential_ref")
            .and_then(Value::as_str)
            .map(|value| CredentialRef::parse(value.to_string()))
            .transpose()?
            .unwrap_or(credential_ref("proxy", "default", "auth")?);
        if sources
            .get(&reference)
            .is_some_and(|source| *source != crate::CredentialSource::Migrated)
        {
            continue;
        }
        let mut extracted = Vec::new();
        migrate_proxy_auth(object, &mut extracted, 1)?;
        ensure_legacy_proxy_extractions_are_safe_from_sources(
            data_dir,
            sources,
            &extracted,
            &[(bytes.as_slice(), true)],
        )?;
    }
    Ok(())
}

fn scrub_authoritative_or_tombstoned_proxy_auth(
    object: &mut Map<String, Value>,
    store: &CredentialStore,
    proxy_clear_tombstones: &BTreeSet<String>,
) -> ConfigStoreResult<bool> {
    let had_legacy = [
        "proxy_auth",
        "proxy_auth_encrypted",
        "http_proxy_auth_encrypted",
        "https_proxy_auth_encrypted",
    ]
    .iter()
    .any(|key| object.contains_key(*key));
    if !had_legacy {
        return Ok(false);
    }
    let tombstone_reference = proxy_clear_tombstones
        .iter()
        .next()
        .map(|reference| CredentialRef::parse(reference.clone()))
        .transpose()?;
    let reference = match tombstone_reference.as_ref() {
        Some(reference) => reference.clone(),
        None => object
            .get("proxy_auth_credential_ref")
            .map(|value| {
                value
                    .as_str()
                    .ok_or_else(|| {
                        ConfigStoreError::Validation(
                            "proxy auth credential reference must be a string".to_string(),
                        )
                    })
                    .and_then(|value| CredentialRef::parse(value.to_string()))
            })
            .transpose()?
            .unwrap_or(credential_ref("proxy", "default", "auth")?),
    };
    let authoritative = if tombstone_reference.is_some() {
        true
    } else {
        let status = store.status_unchecked(&reference)?;
        status.configured && status.source != crate::CredentialSource::Migrated
    };
    if !authoritative {
        return Ok(false);
    }
    // The store value or exact clear is authoritative. Scrub the stale backup
    // by metadata only: do not decrypt, normalize, or compare its secret.
    for key in [
        "proxy_auth",
        "proxy_auth_encrypted",
        "http_proxy_auth_encrypted",
        "https_proxy_auth_encrypted",
    ] {
        object.remove(key);
    }
    object.insert(
        "proxy_auth_credential_ref".to_string(),
        Value::String(reference.as_str().to_string()),
    );
    Ok(true)
}

fn take_consistent_legacy_secret(
    object: &mut Map<String, Value>,
    plaintext_key: &str,
    ciphertext_key: &str,
    label: &str,
) -> ConfigStoreResult<Option<LegacySecret>> {
    let plaintext = take_nonempty_string(object, plaintext_key)?;
    let ciphertext = take_nonempty_string(object, ciphertext_key)?;
    consistent_legacy_secret(plaintext, ciphertext, label)
}

fn consistent_legacy_secret(
    plaintext: Option<String>,
    ciphertext: Option<String>,
    label: &str,
) -> ConfigStoreResult<Option<LegacySecret>> {
    match (plaintext, ciphertext) {
        (Some(plaintext), Some(ciphertext)) => {
            let decrypted = crate::encryption::decrypt(&ciphertext).map_err(|_| {
                ConfigStoreError::Validation(format!("{label} ciphertext is invalid"))
            })?;
            if decrypted != plaintext {
                return Err(ConfigStoreError::Validation(format!(
                    "{label} plaintext and ciphertext disagree"
                )));
            }
            Ok(Some(LegacySecret::Plaintext(plaintext)))
        }
        (Some(plaintext), None) => Ok(Some(LegacySecret::Plaintext(plaintext))),
        (None, Some(ciphertext)) => Ok(Some(LegacySecret::Ciphertext(ciphertext))),
        (None, None) => Ok(None),
    }
}

fn migrate_builtin_provider_credentials(
    object: &mut Map<String, Value>,
    extracted: &mut Vec<ExtractedSecret>,
    migration_generation: u64,
) -> ConfigStoreResult<bool> {
    let mut changed = false;
    for provider in ["openai", "anthropic", "gemini", "bodhi"] {
        let Some(config) = object.get_mut(provider).and_then(Value::as_object_mut) else {
            continue;
        };
        let from_environment = config
            .get("api_key_from_env")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let secret = take_consistent_legacy_secret(
            config,
            "api_key",
            "api_key_encrypted",
            &format!("provider '{provider}' API key"),
        )?;
        if matches!(secret.as_ref(), Some(LegacySecret::Plaintext(_))) && from_environment {
            return Err(ConfigStoreError::Validation(
                "environment-sourced provider plaintext must not be persisted".to_string(),
            ));
        }
        if secret.is_none() {
            continue;
        }
        let reference = existing_or_generated_ref(
            config.get("credential_ref"),
            "provider",
            provider,
            "api_key",
        )?;
        config.insert(
            "credential_ref".to_string(),
            Value::String(reference.as_str().to_string()),
        );
        extracted.push(ExtractedSecret {
            credential_ref: reference,
            value: secret.expect("one provider credential exists"),
            migration_generation,
            kind: ExtractedSecretKind::ProviderBuiltin,
            env_owner: None,
            provider_owner: Some(provider.to_string()),
            mcp_owner: None,
            connect_owner: None,
        });
        changed = true;
    }
    Ok(changed)
}

fn migrate_provider_instance_credentials(
    object: &mut Map<String, Value>,
    extracted: &mut Vec<ExtractedSecret>,
    migration_generation: u64,
) -> ConfigStoreResult<bool> {
    let Some(instances) = object.get_mut("provider_instances") else {
        return Ok(false);
    };
    let instances = instances.as_object_mut().ok_or_else(|| {
        ConfigStoreError::Validation("provider_instances must be an object".to_string())
    })?;
    let mut changed = false;
    for (instance_id, value) in instances {
        let instance = value.as_object_mut().ok_or_else(|| {
            ConfigStoreError::Validation("provider instance must be an object".to_string())
        })?;
        let secret = take_consistent_legacy_secret(
            instance,
            "api_key",
            "api_key_encrypted",
            &format!("provider instance '{instance_id}' API key"),
        )?;
        if secret.is_none() {
            continue;
        }
        let reference = existing_or_generated_ref(
            instance.get("credential_ref"),
            "provider_instance",
            instance_id,
            "api_key",
        )?;
        instance.insert(
            "credential_ref".to_string(),
            Value::String(reference.as_str().to_string()),
        );
        extracted.push(ExtractedSecret {
            credential_ref: reference,
            value: secret.expect("one provider-instance credential exists"),
            migration_generation,
            kind: ExtractedSecretKind::ProviderInstance,
            env_owner: None,
            provider_owner: None,
            mcp_owner: None,
            connect_owner: None,
        });
        changed = true;
    }
    Ok(changed)
}

fn plan_provider_section(
    data_dir: &Path,
    extracted: &mut Vec<ExtractedSecret>,
    minimum_generation: u64,
) -> ConfigStoreResult<Option<PlannedSection>> {
    let path = data_dir.join(PROVIDERS_FILE);
    let original = match read_file_reject_symlink(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    plan_provider_document(original, extracted, minimum_generation)
}

fn plan_provider_document(
    original: Vec<u8>,
    extracted: &mut Vec<ExtractedSecret>,
    minimum_generation: u64,
) -> ConfigStoreResult<Option<PlannedSection>> {
    let mut root: Value = serde_json::from_slice(&original)?;
    let (data, revision) = section_data_mut(&mut root)?;
    let migration_generation = next_revision(revision.unwrap_or(0))?.max(minimum_generation);
    let object = data.as_object_mut().ok_or_else(|| {
        ConfigStoreError::Validation("provider section must be an object".to_string())
    })?;
    let mut changed =
        migrate_builtin_provider_credentials(object, extracted, migration_generation)?;
    changed |= migrate_provider_instance_credentials(object, extracted, migration_generation)?;
    if !changed {
        return Ok(None);
    }
    validate_provider_data(data)?;
    advance_or_wrap(&mut root, revision.is_some(), migration_generation);
    Ok(Some(PlannedSection {
        name: PROVIDERS_FILE,
        bytes: serde_json::to_vec_pretty(&root)?,
        original,
        migration_generation,
    }))
}

fn provider_instance_refs_from_document(
    bytes: &[u8],
) -> ConfigStoreResult<BTreeMap<String, CredentialRef>> {
    if bytes.is_empty() {
        return Ok(BTreeMap::new());
    }
    let root: Value = serde_json::from_slice(bytes)?;
    let Some(instances) = root.get("provider_instances") else {
        return Ok(BTreeMap::new());
    };
    let instances = instances.as_object().ok_or_else(|| {
        ConfigStoreError::Validation("provider_instances must be an object".to_string())
    })?;
    instances
        .iter()
        .filter_map(|(instance_id, value)| {
            value
                .get("credential_ref")
                .map(|value| (instance_id, value))
        })
        .map(|(instance_id, value)| {
            let value = value.as_str().ok_or_else(|| {
                ConfigStoreError::Validation(
                    "provider instance credential reference must be a string".to_string(),
                )
            })?;
            Ok((
                instance_id.clone(),
                CredentialRef::parse(value.to_string())?,
            ))
        })
        .collect()
}

fn provider_refs_from_document(bytes: &[u8]) -> ConfigStoreResult<BTreeMap<String, CredentialRef>> {
    if bytes.is_empty() {
        return Ok(BTreeMap::new());
    }
    let root: Value = serde_json::from_slice(bytes)?;
    ["openai", "anthropic", "gemini", "bodhi"]
        .into_iter()
        .filter_map(|provider| {
            root.get(provider)
                .and_then(|value| value.get("credential_ref"))
                .map(|value| (provider, value))
        })
        .map(|(provider, value)| {
            let value = value.as_str().ok_or_else(|| {
                ConfigStoreError::Validation(
                    "provider credential reference must be a string".to_string(),
                )
            })?;
            Ok((
                provider.to_string(),
                CredentialRef::parse(value.to_string())?,
            ))
        })
        .collect()
}

fn mcp_refs_from_document(bytes: &[u8]) -> ConfigStoreResult<BTreeSet<CredentialRef>> {
    if bytes.is_empty() {
        return Ok(BTreeSet::new());
    }
    fn collect(value: &Value, output: &mut BTreeSet<CredentialRef>) -> ConfigStoreResult<()> {
        match value {
            Value::Object(object) => {
                for (key, value) in object {
                    if key == "credential_ref" {
                        if let Some(reference) = value.as_str() {
                            output.insert(CredentialRef::parse(reference.to_string())?);
                        }
                    } else if key.ends_with("credential_refs") {
                        if let Some(references) = value.as_object() {
                            for reference in references.values().filter_map(Value::as_str) {
                                output.insert(CredentialRef::parse(reference.to_string())?);
                            }
                        }
                    } else {
                        collect(value, output)?;
                    }
                }
            }
            Value::Array(values) => {
                for value in values {
                    collect(value, output)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    let root: Value = serde_json::from_slice(bytes)?;
    let mut references = BTreeSet::new();
    collect(&root, &mut references)?;
    Ok(references)
}

fn env_refs_from_document(bytes: &[u8]) -> ConfigStoreResult<BTreeMap<String, CredentialRef>> {
    if bytes.is_empty() {
        return Ok(BTreeMap::new());
    }
    let root: Value = serde_json::from_slice(bytes)?;
    let Some(entries) = root.get("env_vars").and_then(Value::as_array) else {
        return Ok(BTreeMap::new());
    };
    let mut refs = BTreeMap::new();
    for entry in entries {
        let Some(name) = entry.get("name").and_then(Value::as_str) else {
            continue;
        };
        if let Some(raw) = entry.get("credential_ref") {
            let raw = raw.as_str().ok_or_else(|| {
                ConfigStoreError::Validation(
                    "env credential reference must be a string".to_string(),
                )
            })?;
            refs.insert(name.to_string(), CredentialRef::parse(raw.to_string())?);
        }
    }
    Ok(refs)
}

fn notification_refs_from_document(
    bytes: &[u8],
) -> ConfigStoreResult<BTreeMap<String, CredentialRef>> {
    if bytes.is_empty() {
        return Ok(BTreeMap::new());
    }
    let root: Value = serde_json::from_slice(bytes)?;
    let mut refs = BTreeMap::new();
    for channel in ["ntfy", "bark"] {
        let Some(raw) = root
            .get("notifications")
            .and_then(|value| value.get(channel))
            .and_then(|value| value.get("credential_ref"))
        else {
            continue;
        };
        let raw = raw.as_str().ok_or_else(|| {
            ConfigStoreError::Validation(
                "notification credential reference must be a string".to_string(),
            )
        })?;
        refs.insert(channel.to_string(), CredentialRef::parse(raw.to_string())?);
    }
    Ok(refs)
}

fn plan_cluster_section(
    data_dir: &Path,
    extracted: &mut Vec<ExtractedSecret>,
    minimum_generation: u64,
    tolerate_corrupt_root: bool,
) -> ConfigStoreResult<Option<PlannedSection>> {
    let path = data_dir.join(CONFIG_FILE);
    let original = match read_file_reject_symlink(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) if tolerate_corrupt_root && error.kind() == std::io::ErrorKind::IsADirectory => {
            return Ok(None);
        }
        Err(error) => return Err(error.into()),
    };
    let mut planned_extracted = Vec::new();
    match plan_cluster_document(original, &mut planned_extracted, minimum_generation) {
        Ok(planned) => {
            extracted.extend(planned_extracted);
            Ok(planned)
        }
        Err(ConfigStoreError::Validation(message))
            if message.starts_with("cluster ")
                || message.starts_with("legacy cluster credential") =>
        {
            Err(ConfigStoreError::Validation(message))
        }
        Err(_) if tolerate_corrupt_root => Ok(None),
        Err(error) => Err(error),
    }
}

fn plan_cluster_document(
    original: Vec<u8>,
    extracted: &mut Vec<ExtractedSecret>,
    migration_generation: u64,
) -> ConfigStoreResult<Option<PlannedSection>> {
    let mut root: Value = serde_json::from_slice(&original)?;
    let root_object = root.as_object_mut().ok_or_else(|| {
        ConfigStoreError::Validation("root configuration must be an object".to_string())
    })?;
    let Some(cluster) = root_object
        .get_mut("cluster_fabric")
        .and_then(Value::as_object_mut)
    else {
        return Ok(None);
    };
    let credential_refs_value = cluster.remove("credential_refs");
    let had_credential_refs = credential_refs_value.is_some();
    let mut credential_refs = match credential_refs_value {
        None | Some(Value::Null) => Map::new(),
        Some(Value::Object(value)) => value,
        Some(_) => {
            return Err(ConfigStoreError::Validation(
                "cluster credential metadata must be an object".to_string(),
            ));
        }
    };
    let Some(nodes) = cluster.get_mut("nodes").and_then(Value::as_array_mut) else {
        if !credential_refs.is_empty() {
            return Err(ConfigStoreError::Validation(
                "cluster credential metadata references an unknown node".to_string(),
            ));
        }
        if had_credential_refs {
            cluster.insert(
                "credential_refs".to_string(),
                Value::Object(credential_refs),
            );
        }
        return Ok(None);
    };

    let mut changed = false;
    let mut node_ids = BTreeSet::new();
    for node in nodes {
        let node = node.as_object_mut().ok_or_else(|| {
            ConfigStoreError::Validation("cluster node must be an object".to_string())
        })?;
        let node_id = node
            .get("id")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| ConfigStoreError::Validation("cluster node id is empty".to_string()))?
            .to_string();
        if !node_ids.insert(node_id.clone()) {
            return Err(ConfigStoreError::Validation(
                "cluster node ids must be unique".to_string(),
            ));
        }
        let metadata_value = credential_refs.remove(&node_id);
        let had_metadata = metadata_value.is_some();
        let mut metadata = match metadata_value {
            None | Some(Value::Null) => Map::new(),
            Some(Value::Object(value)) => value,
            Some(_) => {
                return Err(ConfigStoreError::Validation(
                    "cluster node credential metadata must be an object".to_string(),
                ));
            }
        };
        let placement = node
            .get_mut("placement")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| {
                ConfigStoreError::Validation("cluster node placement is invalid".to_string())
            })?;
        match placement.get("type").and_then(Value::as_str) {
            Some("local") => ensure_cluster_metadata_empty(&metadata, "local cluster node")?,
            Some("ssh") => {
                let auth = placement
                    .get_mut("auth")
                    .and_then(Value::as_object_mut)
                    .ok_or_else(|| {
                        ConfigStoreError::Validation(
                            "cluster SSH authentication is invalid".to_string(),
                        )
                    })?;
                match auth.get("method").and_then(Value::as_str) {
                    Some("system_ssh_config") => {
                        ensure_cluster_metadata_empty(&metadata, "system SSH node")?
                    }
                    Some("password") => {
                        ensure_cluster_metadata_fields_absent(
                            &metadata,
                            &[
                                "private_key_credential_ref",
                                "private_key_configured",
                                "passphrase_credential_ref",
                                "passphrase_configured",
                            ],
                            "password auth",
                        )?;
                        changed |= migrate_cluster_auth_field(
                            auth,
                            &mut metadata,
                            &node_id,
                            "password",
                            "password_encrypted",
                            "password_credential_ref",
                            "password_configured",
                            crate::cluster_password_credential_ref(&node_id)?,
                            extracted,
                            migration_generation,
                        )?;
                    }
                    Some("private_key") => {
                        ensure_cluster_metadata_fields_absent(
                            &metadata,
                            &["password_credential_ref", "password_configured"],
                            "private-key auth",
                        )?;
                        changed |= migrate_cluster_auth_field(
                            auth,
                            &mut metadata,
                            &node_id,
                            "private_key",
                            "private_key_encrypted",
                            "private_key_credential_ref",
                            "private_key_configured",
                            crate::cluster_private_key_credential_ref(&node_id)?,
                            extracted,
                            migration_generation,
                        )?;
                        changed |= migrate_cluster_auth_field(
                            auth,
                            &mut metadata,
                            &node_id,
                            "passphrase",
                            "passphrase_encrypted",
                            "passphrase_credential_ref",
                            "passphrase_configured",
                            crate::cluster_passphrase_credential_ref(&node_id)?,
                            extracted,
                            migration_generation,
                        )?;
                    }
                    _ => {
                        return Err(ConfigStoreError::Validation(
                            "cluster SSH authentication method is invalid".to_string(),
                        ));
                    }
                }
            }
            _ => {
                return Err(ConfigStoreError::Validation(
                    "cluster node placement is invalid".to_string(),
                ));
            }
        }
        if had_metadata || !metadata.is_empty() {
            credential_refs.insert(node_id, Value::Object(metadata));
        }
    }
    if !credential_refs
        .keys()
        .all(|node_id| node_ids.contains(node_id))
    {
        return Err(ConfigStoreError::Validation(
            "cluster credential metadata references an unknown node".to_string(),
        ));
    }
    if had_credential_refs || !credential_refs.is_empty() {
        cluster.insert(
            "credential_refs".to_string(),
            Value::Object(credential_refs),
        );
    }
    if !changed {
        return Ok(None);
    }
    serde_json::from_value::<crate::ClusterFabricConfig>(
        root.get("cluster_fabric").cloned().unwrap_or_default(),
    )
    .map_err(|_| ConfigStoreError::Validation("cluster configuration is invalid".to_string()))?;
    Ok(Some(PlannedSection {
        name: CONFIG_FILE,
        bytes: serde_json::to_vec_pretty(&root)?,
        original,
        migration_generation,
    }))
}

fn ensure_cluster_metadata_empty(
    metadata: &Map<String, Value>,
    owner: &str,
) -> ConfigStoreResult<()> {
    ensure_cluster_metadata_fields_absent(
        metadata,
        &[
            "password_credential_ref",
            "password_configured",
            "private_key_credential_ref",
            "private_key_configured",
            "passphrase_credential_ref",
            "passphrase_configured",
        ],
        owner,
    )
}

fn ensure_cluster_metadata_fields_absent(
    metadata: &Map<String, Value>,
    fields: &[&str],
    owner: &str,
) -> ConfigStoreResult<()> {
    if fields.iter().any(|field| metadata.contains_key(*field)) {
        return Err(ConfigStoreError::Validation(format!(
            "cluster credential metadata does not match {owner}"
        )));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn migrate_cluster_auth_field(
    auth: &mut Map<String, Value>,
    metadata: &mut Map<String, Value>,
    _node_id: &str,
    plaintext_key: &str,
    ciphertext_key: &str,
    reference_key: &str,
    configured_key: &str,
    canonical: CredentialRef,
    extracted: &mut Vec<ExtractedSecret>,
    migration_generation: u64,
) -> ConfigStoreResult<bool> {
    let had_plaintext = auth.contains_key(plaintext_key);
    let had_ciphertext = auth.contains_key(ciphertext_key);
    let plaintext = take_nonempty_string(auth, plaintext_key)?;
    let ciphertext = take_nonempty_string(auth, ciphertext_key)?;
    let secret = match (plaintext, ciphertext) {
        (Some(plaintext), Some(ciphertext)) => {
            let decrypted = crate::encryption::decrypt(&ciphertext).map_err(|_| {
                ConfigStoreError::Validation(
                    "legacy cluster credential could not be decrypted".to_string(),
                )
            })?;
            if plaintext != decrypted {
                return Err(ConfigStoreError::Validation(
                    "legacy cluster credential fields contain conflicting values".to_string(),
                ));
            }
            Some(LegacySecret::Plaintext(plaintext))
        }
        (Some(plaintext), None) => Some(LegacySecret::Plaintext(plaintext)),
        (None, Some(ciphertext)) => Some(LegacySecret::Ciphertext(ciphertext)),
        (None, None) => None,
    };
    let previous_ref = metadata.get(reference_key).cloned();
    let explicit_ref = previous_ref
        .as_ref()
        .map(|value| {
            value.as_str().ok_or_else(|| {
                ConfigStoreError::Validation(
                    "cluster credential reference must be a string".to_string(),
                )
            })
        })
        .transpose()?
        .map(|value| CredentialRef::parse(value.to_string()))
        .transpose()?;
    if explicit_ref
        .as_ref()
        .is_some_and(|value| value != &canonical)
    {
        return Err(ConfigStoreError::Validation(
            "cluster credential reference is not canonical".to_string(),
        ));
    }
    let previous_configured = metadata.get(configured_key).cloned();
    let configured = previous_configured
        .as_ref()
        .map(|value| {
            value.as_bool().ok_or_else(|| {
                ConfigStoreError::Validation(
                    "cluster credential configured metadata must be boolean".to_string(),
                )
            })
        })
        .transpose()?
        .unwrap_or(false)
        || explicit_ref.is_some()
        || secret.is_some();
    if configured {
        metadata.insert(
            reference_key.to_string(),
            Value::String(canonical.as_str().to_string()),
        );
        metadata.insert(configured_key.to_string(), Value::Bool(true));
    } else {
        metadata.remove(reference_key);
        metadata.remove(configured_key);
    }
    if let Some(secret) = secret {
        extracted.push(ExtractedSecret {
            credential_ref: canonical.clone(),
            value: secret,
            migration_generation,
            kind: ExtractedSecretKind::Other,
            env_owner: None,
            provider_owner: None,
            mcp_owner: None,
            connect_owner: None,
        });
    }
    Ok(had_plaintext
        || had_ciphertext
        || previous_ref.as_ref() != metadata.get(reference_key)
        || previous_configured.as_ref() != metadata.get(configured_key))
}

fn cluster_refs_from_document(bytes: &[u8]) -> ConfigStoreResult<BTreeSet<CredentialRef>> {
    if bytes.is_empty() {
        return Ok(BTreeSet::new());
    }
    let root: Value = serde_json::from_slice(bytes)?;
    let Some(entries) = root
        .get("cluster_fabric")
        .and_then(|value| value.get("credential_refs"))
        .and_then(Value::as_object)
    else {
        return Ok(BTreeSet::new());
    };
    let mut refs = BTreeSet::new();
    for metadata in entries.values() {
        let metadata = metadata.as_object().ok_or_else(|| {
            ConfigStoreError::Validation(
                "cluster node credential metadata must be an object".to_string(),
            )
        })?;
        for key in [
            "password_credential_ref",
            "private_key_credential_ref",
            "passphrase_credential_ref",
        ] {
            let Some(value) = metadata.get(key) else {
                continue;
            };
            let value = value.as_str().ok_or_else(|| {
                ConfigStoreError::Validation(
                    "cluster credential reference must be a string".to_string(),
                )
            })?;
            refs.insert(CredentialRef::parse(value.to_string())?);
        }
    }
    Ok(refs)
}

fn strip_cluster_credential_metadata(value: &mut Value) {
    if let Some(cluster) = value
        .get_mut("cluster_fabric")
        .and_then(Value::as_object_mut)
    {
        cluster.remove("credential_refs");
    }
}

fn ensure_cluster_refs_are_safe(
    data_dir: &Path,
    refs: &BTreeSet<CredentialRef>,
    prospective_documents: &[(&[u8], bool)],
) -> ConfigStoreResult<()> {
    for reference in refs {
        for name in [PROVIDERS_FILE, MCP_FILE, BROKER_FILE, CONFIG_FILE] {
            for suffix in ["", ".bak", ".bak.1", ".bak.2"] {
                let bytes = read_target_or_empty(&data_dir.join(format!("{name}{suffix}")))?;
                if bytes.is_empty() {
                    continue;
                }
                let mut value: Value = match serde_json::from_slice(&bytes) {
                    Ok(value) => value,
                    Err(_) if !suffix.is_empty() => continue,
                    Err(error) if name == CONFIG_FILE => return Err(error.into()),
                    Err(_) => continue,
                };
                if name == CONFIG_FILE {
                    strip_cluster_credential_metadata(&mut value);
                }
                if contains_credential_reference(&value, reference.as_str()) {
                    return Err(ConfigStoreError::Validation(
                        "cluster credential reference is shared by another consumer".to_string(),
                    ));
                }
            }
        }
        for (bytes, config_root) in prospective_documents {
            let mut value: Value = serde_json::from_slice(bytes)?;
            if *config_root {
                strip_cluster_credential_metadata(&mut value);
            }
            if contains_credential_reference(&value, reference.as_str()) {
                return Err(ConfigStoreError::Validation(
                    "cluster credential reference is shared by another consumer".to_string(),
                ));
            }
        }
        for descriptor in crate::SECTION_DESCRIPTORS.iter().filter(|descriptor| {
            descriptor.id != crate::SectionId::Credentials
                && !matches!(descriptor.file_name, PROVIDERS_FILE | MCP_FILE)
        }) {
            let bytes = read_target_or_empty(&data_dir.join(descriptor.file_name))?;
            if bytes.is_empty() {
                continue;
            }
            let mut value: Value = serde_json::from_slice(&bytes)?;
            if descriptor.file_name == CLUSTER_FABRIC_FILE {
                if let Some(cluster) = value.get_mut("data").and_then(Value::as_object_mut) {
                    cluster.remove("credential_refs");
                }
            }
            if contains_credential_reference(&value, reference.as_str()) {
                return Err(ConfigStoreError::Validation(
                    "cluster credential reference is shared by another consumer".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn ensure_cluster_extractions_are_safe(
    data_dir: &Path,
    extracted: &[ExtractedSecret],
    prospective_documents: &[(&[u8], bool)],
) -> ConfigStoreResult<()> {
    let mut refs = extracted
        .iter()
        .map(|secret| secret.credential_ref.clone())
        .collect::<BTreeSet<_>>();
    for (bytes, _) in prospective_documents {
        refs.extend(cluster_refs_from_document(bytes)?);
    }
    preflight_cluster_backups(data_dir, &mut refs)?;
    ensure_cluster_refs_are_safe(data_dir, &refs, prospective_documents)
}

fn preflight_cluster_backups(
    data_dir: &Path,
    refs: &mut BTreeSet<CredentialRef>,
) -> ConfigStoreResult<()> {
    for suffix in ["bak", "bak.1", "bak.2"] {
        let bytes =
            match read_file_reject_symlink(&data_dir.join(format!("{CONFIG_FILE}.{suffix}"))) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
        if serde_json::from_slice::<Value>(&bytes).is_err() {
            continue;
        }
        let mut extracted = Vec::new();
        let planned = plan_cluster_document(bytes.clone(), &mut extracted, 1)?;
        refs.extend(extracted.into_iter().map(|secret| secret.credential_ref));
        refs.extend(cluster_refs_from_document(
            planned
                .as_ref()
                .map(|section| section.bytes.as_slice())
                .unwrap_or(bytes.as_slice()),
        )?);
    }
    Ok(())
}

fn collect_cluster_backup_extractions(data_dir: &Path) -> ConfigStoreResult<Vec<ExtractedSecret>> {
    let mut all = Vec::new();
    for suffix in ["bak", "bak.1", "bak.2"] {
        let bytes =
            match read_file_reject_symlink(&data_dir.join(format!("{CONFIG_FILE}.{suffix}"))) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
        if serde_json::from_slice::<Value>(&bytes).is_err() {
            continue;
        }
        let mut extracted = Vec::new();
        plan_cluster_document(bytes, &mut extracted, 1)?;
        all.extend(extracted);
    }
    Ok(all)
}

fn scrub_cluster_credentials_from_backups(data_dir: &Path) -> ConfigStoreResult<usize> {
    let store = CredentialStore::open(data_dir);
    let mut plans = Vec::<(PathBuf, Vec<u8>, Vec<ExtractedSecret>)>::new();
    let mut refs = cluster_refs_from_document(&read_target_or_empty(&data_dir.join(CONFIG_FILE))?)?;
    for suffix in ["bak", "bak.1", "bak.2"] {
        let path = data_dir.join(format!("{CONFIG_FILE}.{suffix}"));
        let bytes = match read_file_reject_symlink(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        if serde_json::from_slice::<Value>(&bytes).is_err() {
            continue;
        }
        let mut extracted = Vec::new();
        let planned = plan_cluster_document(bytes.clone(), &mut extracted, 1)?;
        let candidate = planned
            .as_ref()
            .map(|section| section.bytes.clone())
            .unwrap_or_else(|| bytes.clone());
        refs.extend(cluster_refs_from_document(&candidate)?);
        if planned.is_some() {
            plans.push((path, candidate, extracted));
        }
    }
    ensure_cluster_refs_are_safe(data_dir, &refs, &[])?;

    let mut backup_secrets = Vec::new();
    for (_, _, extracted) in &mut plans {
        for secret in std::mem::take(extracted) {
            if !store.status_unchecked(&secret.credential_ref)?.configured {
                backup_secrets.push(secret);
            }
        }
    }
    let migrated_credentials = if backup_secrets.is_empty() {
        0
    } else {
        let resolved = resolve_extracted_secrets(&store, backup_secrets)?;
        let prepared = store.prepare_migration(resolved)?;
        let added = prepared.added;
        store.commit_migration(&prepared.bytes)?;
        added
    };
    for (path, candidate, _) in plans {
        AtomicFileStore::new(path).write_bytes_without_backup(&candidate)?;
    }
    Ok(migrated_credentials)
}

fn plan_provider_instance_section(
    data_dir: &Path,
    extracted: &mut Vec<ExtractedSecret>,
    minimum_generation: u64,
    tolerate_corrupt_root: bool,
    include_root_builtins: bool,
) -> ConfigStoreResult<Option<PlannedSection>> {
    let path = data_dir.join(CONFIG_FILE);
    let original = match read_file_reject_symlink(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        // A non-file at the optional root-config location cannot contain a
        // provider-instance migration source. Leave it for Config's normal
        // root write/recovery handling while still propagating real read and
        // permission failures.
        Err(error) if tolerate_corrupt_root && error.kind() == std::io::ErrorKind::IsADirectory => {
            return Ok(None);
        }
        Err(error) => return Err(error.into()),
    };
    let mut root_extracted = Vec::new();
    let planned = plan_provider_instance_document(
        original,
        &mut root_extracted,
        minimum_generation,
        include_root_builtins,
    );
    match planned {
        Ok(planned) => {
            extracted.extend(root_extracted);
            Ok(planned)
        }
        Err(ConfigStoreError::Validation(message))
            if message.starts_with("legacy proxy auth")
                || message.starts_with("legacy env credential")
                || message.starts_with("legacy notification credential") =>
        {
            Err(ConfigStoreError::Validation(message))
        }
        // Root-config corruption recovery runs after migration readiness. Do
        // not block provider/MCP/store hydration because this optional member
        // has invalid JSON or validation errors. Crucially, extraction is
        // isolated above so an error cannot publish a partial credential set.
        Err(_) if tolerate_corrupt_root => Ok(None),
        Err(error) => Err(error),
    }
}

fn plan_provider_instance_document(
    original: Vec<u8>,
    extracted: &mut Vec<ExtractedSecret>,
    minimum_generation: u64,
    include_root_builtins: bool,
) -> ConfigStoreResult<Option<PlannedSection>> {
    let mut root: Value = serde_json::from_slice(&original)?;
    let object = root.as_object_mut().ok_or_else(|| {
        ConfigStoreError::Validation("root configuration must be an object".to_string())
    })?;
    let migration_generation = minimum_generation;
    let mut changed = migrate_proxy_auth(object, extracted, migration_generation)?;
    changed |= migrate_env_vars(object, extracted, migration_generation)?;
    changed |= migrate_notification_credentials(object, extracted, migration_generation)?;
    if include_root_builtins {
        if let Some(providers) = object.get_mut("providers") {
            let providers = providers.as_object_mut().ok_or_else(|| {
                ConfigStoreError::Validation("providers must be an object".to_string())
            })?;
            changed |=
                migrate_builtin_provider_credentials(providers, extracted, migration_generation)?;
        }
    }
    changed |= migrate_provider_instance_credentials(object, extracted, migration_generation)?;
    if !changed {
        return Ok(None);
    }
    Ok(Some(PlannedSection {
        name: CONFIG_FILE,
        bytes: serde_json::to_vec_pretty(&root)?,
        original,
        migration_generation,
    }))
}

fn plan_facade_root_document(
    original: Vec<u8>,
    extracted: &mut Vec<ExtractedSecret>,
    migration_generation: u64,
) -> ConfigStoreResult<Option<PlannedSection>> {
    let provider =
        plan_provider_instance_document(original.clone(), extracted, migration_generation, true)?;
    let provider_changed = provider.is_some();
    let provider_bytes = provider
        .as_ref()
        .map(|section| section.bytes.clone())
        .unwrap_or_else(|| original.clone());
    let mut root_after_provider: Value = serde_json::from_slice(&provider_bytes)?;
    let root_object = root_after_provider.as_object_mut().ok_or_else(|| {
        ConfigStoreError::Validation("root configuration must be an object".to_string())
    })?;
    let root_mcp = if let Some(mcp) = if root_object.contains_key("mcpServers") {
        root_object.get_mut("mcpServers")
    } else {
        root_object.get_mut("mcp")
    } {
        let changed = migrate_mcp_document_data(mcp, extracted, migration_generation)?;
        if changed {
            validate_mcp_data(mcp)?;
        }
        changed
    } else {
        false
    };
    let root_mcp_bytes = if root_mcp {
        serde_json::to_vec_pretty(&root_after_provider)?
    } else {
        provider_bytes
    };
    let cluster = plan_cluster_document(root_mcp_bytes.clone(), extracted, migration_generation)?;
    let cluster_changed = cluster.is_some();
    let cluster_bytes = cluster
        .as_ref()
        .map(|section| section.bytes.clone())
        .unwrap_or(root_mcp_bytes);
    let mut root_after_cluster: Value = serde_json::from_slice(&cluster_bytes)?;
    let connect_changed = root_after_cluster
        .as_object_mut()
        .and_then(|root| root.get_mut("connect"))
        .map(|connect| migrate_connect_credentials(connect, extracted, migration_generation))
        .transpose()?
        .unwrap_or(false);
    let access_changed = root_after_cluster
        .as_object_mut()
        .and_then(|root| root.get_mut("access_control"))
        .map(|access| migrate_access_control_credentials(access, extracted, migration_generation))
        .transpose()?
        .unwrap_or(false);
    if !provider_changed && !root_mcp && !cluster_changed && !connect_changed && !access_changed {
        return Ok(None);
    }
    Ok(Some(PlannedSection {
        name: CONFIG_FILE,
        bytes: if connect_changed || access_changed {
            serde_json::to_vec_pretty(&root_after_cluster)?
        } else {
            cluster_bytes
        },
        original,
        migration_generation,
    }))
}

fn plan_access_control_document(
    original: Vec<u8>,
    extracted: &mut Vec<ExtractedSecret>,
    minimum_generation: u64,
) -> ConfigStoreResult<Option<PlannedSection>> {
    let mut root: Value = serde_json::from_slice(&original)?;
    let (data, revision) = section_data_mut(&mut root)?;
    let migration_generation = next_revision(revision.unwrap_or(0))?.max(minimum_generation);
    if !migrate_access_control_credentials(data, extracted, migration_generation)? {
        return Ok(None);
    }
    let access: Option<crate::AccessControlConfig> = serde_json::from_value(data.clone())?;
    if let Some(access) = access.as_ref() {
        if access.password_enabled && !access.password_configured {
            return Err(ConfigStoreError::Validation(
                "enabled access-control password has no verifier".to_string(),
            ));
        }
        if access.devices.iter().any(|device| !device.token_configured) {
            return Err(ConfigStoreError::Validation(
                "access-control device has no token verifier".to_string(),
            ));
        }
    }
    advance_or_wrap(&mut root, revision.is_some(), migration_generation);
    Ok(Some(PlannedSection {
        name: ACCESS_CONTROL_FILE,
        bytes: serde_json::to_vec_pretty(&root)?,
        original,
        migration_generation,
    }))
}

fn migrate_access_control_credentials(
    value: &mut Value,
    extracted: &mut Vec<ExtractedSecret>,
    migration_generation: u64,
) -> ConfigStoreResult<bool> {
    if value.is_null() {
        return Ok(false);
    }
    let access = value.as_object_mut().ok_or_else(|| {
        ConfigStoreError::Validation("access_control must be an object or null".to_string())
    })?;
    let mut changed = false;
    let password_hash = take_nonempty_string(access, "password_hash")?;
    let password_salt = take_nonempty_string(access, "password_salt")?;
    match (password_hash, password_salt) {
        (Some(hash), Some(salt)) => {
            let reference = existing_or_generated_ref(
                access.get("password_credential_ref"),
                "access",
                "root",
                "password_verifier",
            )?;
            let value = crate::config_crypto::encode_access_verifier(&hash, &salt)?;
            access.insert(
                "password_credential_ref".to_string(),
                Value::String(reference.as_str().to_string()),
            );
            access.insert("password_configured".to_string(), Value::Bool(true));
            extracted.push(ExtractedSecret {
                credential_ref: reference,
                value: LegacySecret::Plaintext(value),
                migration_generation,
                kind: ExtractedSecretKind::AccessControl,
                env_owner: None,
                provider_owner: None,
                mcp_owner: None,
                connect_owner: None,
            });
            changed = true;
        }
        (None, None) => {}
        _ => {
            return Err(ConfigStoreError::Validation(
                "legacy access-control password verifier is incomplete".to_string(),
            ));
        }
    }
    if let Some(devices) = access.get_mut("devices").and_then(Value::as_array_mut) {
        for device in devices {
            let device = device.as_object_mut().ok_or_else(|| {
                ConfigStoreError::Validation("access-control device must be an object".to_string())
            })?;
            let device_id = device
                .get("device_id")
                .and_then(Value::as_str)
                .filter(|id| !id.trim().is_empty())
                .ok_or_else(|| {
                    ConfigStoreError::Validation("access-control device id is missing".to_string())
                })?
                .to_string();
            let token_hash = take_nonempty_string(device, "token_hash")?;
            let token_salt = take_nonempty_string(device, "token_salt")?;
            match (token_hash, token_salt) {
                (Some(hash), Some(salt)) => {
                    let reference = existing_or_generated_ref(
                        device.get("token_credential_ref"),
                        "access",
                        &device_id,
                        "device_token_verifier",
                    )?;
                    let value = crate::config_crypto::encode_access_verifier(&hash, &salt)?;
                    device.insert(
                        "token_credential_ref".to_string(),
                        Value::String(reference.as_str().to_string()),
                    );
                    device.insert("token_configured".to_string(), Value::Bool(true));
                    extracted.push(ExtractedSecret {
                        credential_ref: reference,
                        value: LegacySecret::Plaintext(value),
                        migration_generation,
                        kind: ExtractedSecretKind::AccessControl,
                        env_owner: None,
                        provider_owner: None,
                        mcp_owner: None,
                        connect_owner: None,
                    });
                    changed = true;
                }
                (None, None) => {}
                _ => {
                    return Err(ConfigStoreError::Validation(format!(
                        "legacy access-control device '{device_id}' verifier is incomplete"
                    )));
                }
            }
        }
    }
    Ok(changed)
}

fn migrate_env_vars(
    object: &mut Map<String, Value>,
    extracted: &mut Vec<ExtractedSecret>,
    migration_generation: u64,
) -> ConfigStoreResult<bool> {
    let Some(entries) = object.get_mut("env_vars").and_then(Value::as_array_mut) else {
        return Ok(false);
    };
    let mut changed = false;
    for entry in entries {
        let entry = entry.as_object_mut().ok_or_else(|| {
            ConfigStoreError::Validation("env var entry must be an object".to_string())
        })?;
        if !entry
            .get("secret")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            changed |= entry.remove("value_encrypted").is_some();
            changed |= entry.remove("credential_ref").is_some();
            changed |= entry.remove("configured").is_some();
            continue;
        }
        let name = entry
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| ConfigStoreError::Validation("env var name is missing".to_string()))?
            .to_string();
        let had_plaintext_field = entry.contains_key("value");
        let had_ciphertext_field = entry.contains_key("value_encrypted");
        let previous_ref = entry.get("credential_ref").cloned();
        let previous_configured = entry.get("configured").cloned();
        let plaintext = take_nonempty_string(entry, "value")?;
        let ciphertext = take_nonempty_string(entry, "value_encrypted")?;
        let value = match (plaintext, ciphertext) {
            (Some(plaintext), Some(ciphertext)) => {
                let decrypted = crate::encryption::decrypt(&ciphertext).map_err(|_| {
                    ConfigStoreError::Validation(
                        "legacy env credential could not be decrypted".to_string(),
                    )
                })?;
                if plaintext != decrypted {
                    return Err(ConfigStoreError::Validation(
                        "legacy env credential fields contain conflicting values".to_string(),
                    ));
                }
                Some(LegacySecret::Plaintext(plaintext))
            }
            (Some(plaintext), None) => Some(LegacySecret::Plaintext(plaintext)),
            (None, Some(ciphertext)) => Some(LegacySecret::Ciphertext(ciphertext)),
            (None, None) => None,
        };
        let reference =
            existing_or_generated_ref(entry.get("credential_ref"), "env", &name, "value")?;
        entry.insert(
            "credential_ref".to_string(),
            Value::String(reference.as_str().to_string()),
        );
        let configured = value.is_some()
            || entry
                .get("configured")
                .and_then(Value::as_bool)
                .unwrap_or(false);
        entry.insert("configured".to_string(), Value::Bool(configured));
        if let Some(value) = value {
            extracted.push(ExtractedSecret {
                credential_ref: reference.clone(),
                value,
                migration_generation,
                kind: ExtractedSecretKind::EnvVar,
                env_owner: Some(name.clone()),
                provider_owner: None,
                mcp_owner: None,
                connect_owner: None,
            });
        }
        changed |= had_plaintext_field
            || had_ciphertext_field
            || previous_ref.as_ref() != Some(&Value::String(reference.as_str().to_string()))
            || previous_configured.as_ref() != Some(&Value::Bool(configured));
    }
    Ok(changed)
}

fn migrate_notification_credentials(
    object: &mut Map<String, Value>,
    extracted: &mut Vec<ExtractedSecret>,
    migration_generation: u64,
) -> ConfigStoreResult<bool> {
    let Some(notifications) = object
        .get_mut("notifications")
        .and_then(Value::as_object_mut)
    else {
        return Ok(false);
    };
    let mut changed = false;
    for (channel, plaintext_key, ciphertext_key, field) in [
        ("ntfy", "token", "token_encrypted", "token"),
        ("bark", "device_key", "device_key_encrypted", "device_key"),
    ] {
        let Some(config) = notifications
            .get_mut(channel)
            .and_then(Value::as_object_mut)
        else {
            continue;
        };
        let had_plaintext = config.contains_key(plaintext_key);
        let had_ciphertext = config.contains_key(ciphertext_key);
        let previous_ref = config.get("credential_ref").cloned();
        let previous_configured = config.get("configured").cloned();
        let plaintext = take_nonempty_string(config, plaintext_key)?;
        let ciphertext = take_nonempty_string(config, ciphertext_key)?;
        let secret = match (plaintext, ciphertext) {
            (Some(plaintext), Some(ciphertext)) => {
                let decrypted = crate::encryption::decrypt(&ciphertext).map_err(|_| {
                    ConfigStoreError::Validation(
                        "legacy notification credential could not be decrypted".to_string(),
                    )
                })?;
                if plaintext != decrypted {
                    return Err(ConfigStoreError::Validation(
                        "legacy notification credential fields contain conflicting values"
                            .to_string(),
                    ));
                }
                Some(LegacySecret::Plaintext(plaintext))
            }
            (Some(plaintext), None) => Some(LegacySecret::Plaintext(plaintext)),
            (None, Some(ciphertext)) => Some(LegacySecret::Ciphertext(ciphertext)),
            (None, None) => None,
        };
        let configured = secret.is_some()
            || config
                .get("configured")
                .and_then(Value::as_bool)
                .unwrap_or(false);
        if secret.is_none() && previous_ref.is_none() && !configured {
            changed |= had_plaintext || had_ciphertext;
            continue;
        }
        let reference = existing_or_generated_ref(
            config.get("credential_ref"),
            "notification",
            channel,
            field,
        )?;
        config.insert(
            "credential_ref".to_string(),
            Value::String(reference.as_str().to_string()),
        );
        config.insert("configured".to_string(), Value::Bool(configured));
        if let Some(secret) = secret {
            extracted.push(ExtractedSecret {
                credential_ref: reference.clone(),
                value: secret,
                migration_generation,
                kind: if channel == "ntfy" {
                    ExtractedSecretKind::NotificationNtfy
                } else {
                    ExtractedSecretKind::NotificationBark
                },
                env_owner: None,
                provider_owner: None,
                mcp_owner: None,
                connect_owner: None,
            });
        }
        changed |= had_plaintext
            || had_ciphertext
            || previous_ref.as_ref() != Some(&Value::String(reference.as_str().to_string()))
            || previous_configured.as_ref() != Some(&Value::Bool(configured));
    }
    Ok(changed)
}

fn connect_platform_ids(bytes: &[u8]) -> ConfigStoreResult<BTreeSet<String>> {
    let mut root: Value = serde_json::from_slice(bytes)?;
    let (data, _) = section_data_mut(&mut root)?;
    let platforms = data
        .get("platforms")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ConfigStoreError::Validation(
                "connect configuration must contain a platforms array".to_string(),
            )
        })?;
    platforms
        .iter()
        .filter_map(|platform| platform.get("id").and_then(Value::as_str))
        .map(|id| {
            if id.trim().is_empty() {
                Err(ConfigStoreError::Validation(
                    "connect platform id must not be empty".to_string(),
                ))
            } else {
                Ok(id.to_string())
            }
        })
        .collect()
}

fn migrate_connect_credentials(
    connect: &mut Value,
    extracted: &mut Vec<ExtractedSecret>,
    migration_generation: u64,
) -> ConfigStoreResult<bool> {
    let platforms = connect
        .get_mut("platforms")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| {
            ConfigStoreError::Validation(
                "connect configuration must contain a platforms array".to_string(),
            )
        })?;
    let mut changed = false;
    let mut ids = BTreeSet::new();
    for platform in platforms {
        let platform = platform.as_object_mut().ok_or_else(|| {
            ConfigStoreError::Validation("connect platform must be an object".to_string())
        })?;
        let id = match platform.get("id") {
            Some(Value::String(id)) if !id.trim().is_empty() => id.clone(),
            Some(Value::Null) | None => {
                let id = Uuid::new_v4().to_string();
                platform.insert("id".to_string(), Value::String(id.clone()));
                changed = true;
                id
            }
            _ => {
                return Err(ConfigStoreError::Validation(
                    "connect platform id is invalid".to_string(),
                ));
            }
        };
        if !ids.insert(id.clone()) {
            return Err(ConfigStoreError::Validation(
                "connect platform ids must be unique".to_string(),
            ));
        }
        for (plaintext_key, ciphertext_key, reference_key, configured_key, field) in [
            (
                "token",
                "token_encrypted",
                "token_credential_ref",
                "token_configured",
                "token",
            ),
            (
                "app_secret",
                "app_secret_encrypted",
                "app_secret_credential_ref",
                "app_secret_configured",
                "app_secret",
            ),
        ] {
            let had_plaintext = platform.contains_key(plaintext_key);
            let had_ciphertext = platform.contains_key(ciphertext_key);
            let previous_ref = platform.get(reference_key).cloned();
            let previous_configured = platform.get(configured_key).cloned();
            let secret = take_consistent_legacy_secret(
                platform,
                plaintext_key,
                ciphertext_key,
                &format!("connect platform '{id}' {field}"),
            )?;
            let configured = secret.is_some()
                || platform
                    .get(configured_key)
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
            if secret.is_none() && previous_ref.is_none() && !configured {
                changed |= had_plaintext || had_ciphertext;
                continue;
            }
            let reference =
                existing_or_generated_ref(platform.get(reference_key), "connect", &id, field)?;
            let canonical = credential_ref("connect", &id, field)?;
            if reference != canonical {
                return Err(ConfigStoreError::Validation(
                    "connect credential reference is not canonical".to_string(),
                ));
            }
            platform.insert(
                reference_key.to_string(),
                Value::String(reference.as_str().to_string()),
            );
            platform.insert(configured_key.to_string(), Value::Bool(configured));
            if let Some(value) = secret {
                extracted.push(ExtractedSecret {
                    credential_ref: reference.clone(),
                    value,
                    migration_generation,
                    kind: ExtractedSecretKind::Connect,
                    env_owner: None,
                    provider_owner: None,
                    mcp_owner: None,
                    connect_owner: Some(id.clone()),
                });
            }
            changed |= had_plaintext
                || had_ciphertext
                || previous_ref.as_ref() != Some(&Value::String(reference.as_str().to_string()))
                || previous_configured.as_ref() != Some(&Value::Bool(configured));
        }
    }
    Ok(changed)
}

fn plan_connect_document(
    original: Vec<u8>,
    extracted: &mut Vec<ExtractedSecret>,
    migration_generation: u64,
) -> ConfigStoreResult<Option<PlannedSection>> {
    let mut root: Value = serde_json::from_slice(&original)?;
    let revisioned = root.as_object().is_some_and(|object| {
        object.contains_key("schema_version")
            || object.contains_key("revision")
            || object.contains_key("data")
    });
    let (connect, revision) = section_data_mut(&mut root)?;
    let generation = revision
        .and_then(|revision| revision.checked_add(1))
        .unwrap_or(migration_generation)
        .max(migration_generation);
    if !migrate_connect_credentials(connect, extracted, generation)? {
        return Ok(None);
    }
    let connect: crate::ConnectConfig = serde_json::from_value(connect.clone())?;
    crate::section_facade::validate_connect_isolated(&connect)
        .map_err(ConfigStoreError::Validation)?;
    advance_or_wrap(&mut root, revisioned, generation);
    Ok(Some(PlannedSection {
        name: CONNECT_FILE,
        bytes: serde_json::to_vec_pretty(&root)?,
        original,
        migration_generation: generation,
    }))
}

fn migrate_proxy_auth(
    object: &mut Map<String, Value>,
    extracted: &mut Vec<ExtractedSecret>,
    migration_generation: u64,
) -> ConfigStoreResult<bool> {
    let had_legacy = [
        "proxy_auth",
        "proxy_auth_encrypted",
        "http_proxy_auth_encrypted",
        "https_proxy_auth_encrypted",
    ]
    .iter()
    .any(|key| object.contains_key(*key));
    let plaintext = match object.remove("proxy_auth") {
        None | Some(Value::Null) => None,
        Some(value) => Some(normalize_proxy_auth_secret(&serde_json::to_string(
            &value,
        )?)?),
    };
    let mut secrets = plaintext.into_iter().collect::<BTreeSet<_>>();
    for key in [
        "proxy_auth_encrypted",
        "https_proxy_auth_encrypted",
        "http_proxy_auth_encrypted",
    ] {
        if let Some(ciphertext) = take_nonempty_string(object, key)? {
            let decrypted = crate::encryption::decrypt(&ciphertext).map_err(|_| {
                ConfigStoreError::Validation(
                    "legacy proxy auth credential could not be decrypted".to_string(),
                )
            })?;
            secrets.insert(normalize_proxy_auth_secret(&decrypted)?);
        }
    }
    if secrets.len() > 1 {
        return Err(ConfigStoreError::Validation(
            "legacy proxy auth fields contain conflicting credentials".to_string(),
        ));
    }
    let existing = object
        .get("proxy_auth_credential_ref")
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| {
                    ConfigStoreError::Validation(
                        "proxy auth credential reference must be a string".to_string(),
                    )
                })
                .and_then(|value| CredentialRef::parse(value.to_string()))
        })
        .transpose()?;
    let Some(secret) = secrets.into_iter().next() else {
        return Ok(had_legacy);
    };
    let reference = existing.unwrap_or(credential_ref("proxy", "default", "auth")?);
    object.insert(
        "proxy_auth_credential_ref".to_string(),
        Value::String(reference.as_str().to_string()),
    );
    extracted.push(ExtractedSecret {
        credential_ref: reference,
        value: LegacySecret::Plaintext(secret),
        migration_generation,
        kind: ExtractedSecretKind::ProxyAuth,
        env_owner: None,
        provider_owner: None,
        mcp_owner: None,
        connect_owner: None,
    });
    Ok(true)
}

fn normalize_proxy_auth_secret(value: &str) -> ConfigStoreResult<String> {
    let auth: crate::ProxyAuth = serde_json::from_str(value).map_err(|_| {
        ConfigStoreError::Validation("legacy proxy auth credential is invalid".to_string())
    })?;
    serde_json::to_string(&auth).map_err(ConfigStoreError::Json)
}

fn plan_mcp_section(
    data_dir: &Path,
    extracted: &mut Vec<ExtractedSecret>,
    minimum_generation: u64,
) -> ConfigStoreResult<Option<PlannedSection>> {
    let path = data_dir.join(MCP_FILE);
    let original = match read_file_reject_symlink(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    plan_mcp_document(original, extracted, minimum_generation)
}

fn plan_mcp_document(
    original: Vec<u8>,
    extracted: &mut Vec<ExtractedSecret>,
    minimum_generation: u64,
) -> ConfigStoreResult<Option<PlannedSection>> {
    let mut root: Value = serde_json::from_slice(&original)?;
    let (data, revision) = section_data_mut(&mut root)?;
    let migration_generation = next_revision(revision.unwrap_or(0))?.max(minimum_generation);
    let changed = migrate_mcp_document_data(data, extracted, migration_generation)?;
    if !changed {
        return Ok(None);
    }
    validate_mcp_data(data)?;
    advance_or_wrap(&mut root, revision.is_some(), migration_generation);
    Ok(Some(PlannedSection {
        name: MCP_FILE,
        bytes: serde_json::to_vec_pretty(&root)?,
        original,
        migration_generation,
    }))
}

fn migrate_mcp_document_data(
    data: &mut Value,
    extracted: &mut Vec<ExtractedSecret>,
    migration_generation: u64,
) -> ConfigStoreResult<bool> {
    let mut changed = false;
    if let Some(servers) = data.get_mut("servers").and_then(Value::as_array_mut) {
        for server in servers {
            let object = server.as_object_mut().ok_or_else(|| {
                ConfigStoreError::Validation("MCP server must be an object".to_string())
            })?;
            let id = object
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    ConfigStoreError::Validation("MCP server id is missing".to_string())
                })?
                .to_string();
            if let Some(transport) = object.get_mut("transport").and_then(Value::as_object_mut) {
                changed |= migrate_mcp_transport(&id, transport, extracted, migration_generation)?;
            }
        }
    } else {
        let servers = data.as_object_mut().ok_or_else(|| {
            ConfigStoreError::Validation("MCP section must be an object".to_string())
        })?;
        for (id, server) in servers {
            let object = server.as_object_mut().ok_or_else(|| {
                ConfigStoreError::Validation("MCP server must be an object".to_string())
            })?;
            changed |= migrate_mcp_transport(id, object, extracted, migration_generation)?;
            if let Some(transport) = object.get_mut("transport").and_then(Value::as_object_mut) {
                changed |= migrate_mcp_transport(id, transport, extracted, migration_generation)?;
            }
        }
    }
    Ok(changed)
}

fn plan_broker_section(
    data_dir: &Path,
    extracted: &mut Vec<ExtractedSecret>,
    minimum_generation: u64,
) -> ConfigStoreResult<Option<PlannedSection>> {
    let path = data_dir.join(BROKER_FILE);
    let original = match read_file_reject_symlink(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    plan_broker_document(data_dir, original, extracted, minimum_generation)
}

fn plan_broker_document(
    data_dir: &Path,
    original: Vec<u8>,
    extracted: &mut Vec<ExtractedSecret>,
    minimum_generation: u64,
) -> ConfigStoreResult<Option<PlannedSection>> {
    let mut root: Value = serde_json::from_slice(&original)?;
    let object = root.as_object_mut().ok_or_else(|| {
        ConfigStoreError::Validation("broker configuration must be an object".to_string())
    })?;
    let had_plaintext = object.contains_key("token");
    let had_ciphertext = object.contains_key("token_encrypted");
    if !had_plaintext && !had_ciphertext {
        let config: crate::BrokerClientConfig = serde_json::from_value(root).map_err(|_| {
            ConfigStoreError::Validation("broker configuration is invalid".to_string())
        })?;
        if let Some(reference) = config.credential_ref.as_ref() {
            ensure_broker_ref_exclusive(data_dir, reference)?;
            ensure_broker_backup_ownership(data_dir, reference)?;
        }
        return Ok(None);
    }

    let plaintext = take_nonempty_string(object, "token")?;
    let ciphertext = take_nonempty_string(object, "token_encrypted")?;
    let secret = match (plaintext, ciphertext) {
        (Some(plaintext), Some(ciphertext)) => {
            let decrypted = crate::encryption::decrypt(&ciphertext).map_err(|_| {
                ConfigStoreError::Validation(
                    "legacy broker credential could not be decrypted".to_string(),
                )
            })?;
            if plaintext != decrypted {
                return Err(ConfigStoreError::Validation(
                    "legacy broker credential fields contain conflicting values".to_string(),
                ));
            }
            Some(LegacySecret::Plaintext(plaintext))
        }
        (Some(plaintext), None) => Some(LegacySecret::Plaintext(plaintext)),
        (None, Some(ciphertext)) => Some(LegacySecret::Ciphertext(ciphertext)),
        (None, None) => None,
    };

    let previous_ref = object.get("credential_ref").cloned();
    let previous_configured = object.get("configured").cloned();
    let configured = secret.is_some()
        || object
            .get("configured")
            .and_then(Value::as_bool)
            .unwrap_or(false);
    if secret.is_some() || previous_ref.is_some() || configured {
        let reference = existing_or_generated_ref(
            object.get("credential_ref"),
            "broker",
            "external",
            "bearer_token",
        )?;
        ensure_broker_ref_exclusive(data_dir, &reference)?;
        ensure_broker_backup_ownership(data_dir, &reference)?;
        object.insert(
            "credential_ref".to_string(),
            Value::String(reference.as_str().to_string()),
        );
        object.insert("configured".to_string(), Value::Bool(configured));
        if let Some(secret) = secret {
            extracted.push(ExtractedSecret {
                credential_ref: reference,
                value: secret,
                migration_generation: minimum_generation,
                kind: ExtractedSecretKind::ExternalBroker,
                env_owner: None,
                provider_owner: None,
                mcp_owner: None,
                connect_owner: None,
            });
        }
    } else {
        object.remove("credential_ref");
        object.remove("configured");
    }

    serde_json::from_value::<crate::BrokerClientConfig>(root.clone())
        .map_err(|_| ConfigStoreError::Validation("broker configuration is invalid".to_string()))?;
    let changed = had_plaintext
        || had_ciphertext
        || previous_ref != root.get("credential_ref").cloned()
        || previous_configured != root.get("configured").cloned();
    if !changed {
        return Ok(None);
    }
    Ok(Some(PlannedSection {
        name: BROKER_FILE,
        bytes: serde_json::to_vec_pretty(&root)?,
        original,
        migration_generation: minimum_generation,
    }))
}

fn migrate_mcp_transport(
    server_id: &str,
    object: &mut Map<String, Value>,
    extracted: &mut Vec<ExtractedSecret>,
    migration_generation: u64,
) -> ConfigStoreResult<bool> {
    let mut changed = migrate_named_secret_map(
        server_id,
        object,
        "env",
        "env_encrypted",
        "env_credential_refs",
        "env",
        extracted,
        migration_generation,
    )?;

    let array_headers = object.get("headers").is_some_and(Value::is_array);
    if array_headers {
        let headers = object
            .get_mut("headers")
            .and_then(Value::as_array_mut)
            .expect("array checked");
        for header in headers {
            let header = header.as_object_mut().ok_or_else(|| {
                ConfigStoreError::Validation("MCP header must be an object".to_string())
            })?;
            let name = header
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    ConfigStoreError::Validation("MCP header name is missing".to_string())
                })?
                .to_string();
            let value = consistent_legacy_secret(
                take_nonempty_string(header, "value")?,
                take_nonempty_string(header, "value_encrypted")?,
                &format!("MCP server '{server_id}' header '{name}'"),
            )?;
            if value.is_none() {
                continue;
            }
            let reference = existing_or_generated_ref(
                header.get("credential_ref"),
                "mcp",
                server_id,
                &format!("header_{name}"),
            )?;
            header.insert(
                "credential_ref".to_string(),
                Value::String(reference.as_str().to_string()),
            );
            extracted.push(ExtractedSecret {
                credential_ref: reference,
                value: value.expect("one header credential exists"),
                migration_generation,
                kind: ExtractedSecretKind::Mcp,
                env_owner: None,
                provider_owner: None,
                mcp_owner: Some(server_id.to_string()),
                connect_owner: None,
            });
            changed = true;
        }
    } else {
        changed |= migrate_named_secret_map(
            server_id,
            object,
            "headers",
            "headers_encrypted",
            "header_credential_refs",
            "header",
            extracted,
            migration_generation,
        )?;
    }
    Ok(changed)
}

#[allow(clippy::too_many_arguments)]
fn migrate_named_secret_map(
    server_id: &str,
    object: &mut Map<String, Value>,
    plaintext_key: &str,
    ciphertext_key: &str,
    refs_key: &str,
    field_prefix: &str,
    extracted: &mut Vec<ExtractedSecret>,
    migration_generation: u64,
) -> ConfigStoreResult<bool> {
    let plaintext = take_string_map(object, plaintext_key)?;
    let ciphertext = take_string_map(object, ciphertext_key)?;
    if plaintext.is_empty() && ciphertext.is_empty() {
        return Ok(false);
    }
    let mut refs = object
        .get(refs_key)
        .map(parse_string_map)
        .transpose()?
        .unwrap_or_default();
    let names = plaintext
        .keys()
        .chain(ciphertext.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    for name in names {
        let reference = if let Some(existing) = refs.get(&name) {
            CredentialRef::parse(existing.clone())?
        } else {
            credential_ref("mcp", server_id, &format!("{field_prefix}_{name}"))?
        };
        refs.insert(name.clone(), reference.as_str().to_string());
        let value = consistent_legacy_secret(
            plaintext
                .get(&name)
                .filter(|value| !value.trim().is_empty())
                .cloned(),
            ciphertext
                .get(&name)
                .filter(|value| !value.trim().is_empty())
                .cloned(),
            &format!("MCP server '{server_id}' {field_prefix} '{name}'"),
        )?;
        if let Some(value) = value {
            extracted.push(ExtractedSecret {
                credential_ref: reference,
                value,
                migration_generation,
                kind: ExtractedSecretKind::Mcp,
                env_owner: None,
                provider_owner: None,
                mcp_owner: Some(server_id.to_string()),
                connect_owner: None,
            });
        }
    }
    object.insert(
        refs_key.to_string(),
        serde_json::to_value(refs).expect("string map serializes"),
    );
    Ok(true)
}

fn section_data_mut(root: &mut Value) -> ConfigStoreResult<(&mut Value, Option<u64>)> {
    let revisioned = root.as_object().is_some_and(|object| {
        object.contains_key("schema_version")
            || object.contains_key("revision")
            || object.contains_key("data")
    });
    if !revisioned {
        return Ok((root, None));
    }
    let object = root.as_object_mut().ok_or_else(|| {
        ConfigStoreError::Validation("section envelope must be an object".to_string())
    })?;
    let schema = object.get("schema_version").and_then(Value::as_u64);
    let revision = object.get("revision").and_then(Value::as_u64);
    if schema != Some(1) || revision.is_none() || !object.contains_key("data") {
        return Err(ConfigStoreError::Validation(
            "section envelope is incomplete or unsupported".to_string(),
        ));
    }
    Ok((object.get_mut("data").expect("checked"), revision))
}

fn advance_or_wrap(root: &mut Value, revisioned: bool, migration_generation: u64) {
    if revisioned {
        let object = root.as_object_mut().expect("validated envelope");
        object.insert(
            "revision".to_string(),
            Value::Number(migration_generation.into()),
        );
    } else {
        let data = std::mem::take(root);
        *root = serde_json::json!({
            "schema_version": 1,
            "revision": migration_generation,
            "data": data,
        });
    }
}

fn validate_provider_data(value: &Value) -> ConfigStoreResult<()> {
    serde_json::from_value::<ProviderConfigs>(value.clone())?;
    Ok(())
}

fn validate_mcp_data(value: &Value) -> ConfigStoreResult<()> {
    serde_json::from_value::<bamboo_domain::McpConfig>(value.clone())?;
    Ok(())
}

fn existing_or_generated_ref(
    existing: Option<&Value>,
    domain: &str,
    owner: &str,
    field: &str,
) -> ConfigStoreResult<CredentialRef> {
    match existing {
        Some(Value::String(value)) => CredentialRef::parse(value.clone()),
        Some(_) => Err(ConfigStoreError::Validation(
            "credential reference must be a string".to_string(),
        )),
        None => credential_ref(domain, owner, field),
    }
}

fn take_nonempty_string(
    object: &mut Map<String, Value>,
    key: &str,
) -> ConfigStoreResult<Option<String>> {
    let Some(value) = object.remove(key) else {
        return Ok(None);
    };
    match value {
        Value::Null => Ok(None),
        Value::String(value) if value.trim().is_empty() => Ok(None),
        Value::String(value) => Ok(Some(value)),
        _ => Err(ConfigStoreError::Validation(
            "legacy credential must be a string".to_string(),
        )),
    }
}

fn take_string_map(
    object: &mut Map<String, Value>,
    key: &str,
) -> ConfigStoreResult<BTreeMap<String, String>> {
    object
        .remove(key)
        .map(|value| parse_string_map(&value))
        .transpose()
        .map(Option::unwrap_or_default)
}

fn parse_string_map(value: &Value) -> ConfigStoreResult<BTreeMap<String, String>> {
    let object = value.as_object().ok_or_else(|| {
        ConfigStoreError::Validation("legacy credential map must be an object".to_string())
    })?;
    object
        .iter()
        .map(|(name, value)| {
            value
                .as_str()
                .map(|value| (name.clone(), value.to_string()))
                .ok_or_else(|| {
                    ConfigStoreError::Validation(
                        "legacy credential map values must be strings".to_string(),
                    )
                })
        })
        .collect()
}

// The arguments mirror the durable manifest record at every call site; a
// builder would obscure which original/candidate pair is covered by the hash.
#[allow(clippy::too_many_arguments)]
fn stage_file(
    stage_dir: &Path,
    backup_dir: &Path,
    name: &str,
    candidate: &[u8],
    original: Option<&[u8]>,
    sensitive: bool,
    migration_generation: Option<u64>,
    install_mode: InstallMode,
    expected_revision: Option<u64>,
    staged: &mut Vec<StagedFile>,
) -> ConfigStoreResult<()> {
    AtomicFileStore::new(stage_dir.join(name))
        .sensitive(sensitive)
        .write_bytes_without_backup(candidate)?;
    if let Some(original) = original {
        let plaintext = std::str::from_utf8(original).map_err(|_| {
            ConfigStoreError::Validation("migration backup source is not valid UTF-8".to_string())
        })?;
        let encrypted = crate::encryption::encrypt(plaintext).map_err(|_| {
            ConfigStoreError::Validation("migration backup encryption failed".to_string())
        })?;
        let protected_backup = serde_json::to_vec_pretty(&serde_json::json!({
            "version": 1,
            "ciphertext": encrypted,
        }))?;
        AtomicFileStore::new(backup_dir.join(name))
            .sensitive(true)
            .write_bytes_without_backup(&protected_backup)?;
    }
    staged.push(StagedFile {
        name: name.to_string(),
        staged_name: name.to_string(),
        sha256: sha256(candidate),
        original_sha256: original.map(sha256),
        original_present: Some(original.is_some()),
        migration_generation,
        sensitive,
        install_mode,
        expected_revision,
        transaction_base_sha256: None,
        touched_credential_refs: Vec::new(),
        required_credential_refs: Vec::new(),
        touched_env_names: Vec::new(),
    });
    Ok(())
}

#[derive(Debug)]
struct ActiveExactSection {
    name: &'static str,
    original: Vec<u8>,
    legacy_root: Vec<u8>,
    expected_revision: u64,
}

fn section_data_as_legacy_root(
    scope: ExactTransactionScope,
    data: Value,
) -> ConfigStoreResult<Value> {
    let root = match scope {
        ExactTransactionScope::Providers
        | ExactTransactionScope::Mcp
        | ExactTransactionScope::ProxyAuth
        | ExactTransactionScope::Notifications => data,
        ExactTransactionScope::EnvVars => serde_json::json!({ "env_vars": data }),
        ExactTransactionScope::Connect => serde_json::json!({ "connect": data }),
        ExactTransactionScope::AccessControl => serde_json::json!({ "access_control": data }),
        ExactTransactionScope::ClusterFabric => serde_json::json!({ "cluster_fabric": data }),
    };
    root.is_object().then_some(root).ok_or_else(|| {
        ConfigStoreError::Validation("authoritative section data has an invalid shape".to_string())
    })
}

fn exact_authority_as_legacy_bytes(
    scope: ExactTransactionScope,
    name: &str,
    bytes: &[u8],
) -> ConfigStoreResult<Vec<u8>> {
    if name == CONFIG_FILE {
        return Ok(bytes.to_vec());
    }
    let (_, _, data) = parse_exact_section_document(name, bytes)?;
    Ok(serde_json::to_vec_pretty(&section_data_as_legacy_root(
        scope, data,
    )?)?)
}

fn load_active_exact_section(
    data_dir: &Path,
    scope: ExactTransactionScope,
) -> ConfigStoreResult<ActiveExactSection> {
    let name = authoritative_section_file(scope);
    let original = read_target_or_empty(&data_dir.join(name))?;
    let expected_revision = crate::section_facade::validate_section_envelope(name, &original, 0)?;
    let envelope: Value = serde_json::from_slice(&original)?;
    let data = envelope.get("data").cloned().ok_or_else(|| {
        ConfigStoreError::Validation("authoritative section envelope has no data".to_string())
    })?;
    let legacy_root = section_data_as_legacy_root(scope, data)?;
    Ok(ActiveExactSection {
        name,
        original,
        legacy_root: serde_json::to_vec_pretty(&legacy_root)?,
        expected_revision,
    })
}

fn prepare_active_exact_section_document(
    section: &ActiveExactSection,
    scope: ExactTransactionScope,
    updated_legacy_root: &[u8],
    force_revision: bool,
) -> ConfigStoreResult<(Vec<u8>, bool)> {
    let updated_root = parse_config_root_object(
        updated_legacy_root,
        "exact transaction produced an invalid authoritative section",
    )?;
    let mut updated_data = match scope {
        ExactTransactionScope::Providers
        | ExactTransactionScope::Mcp
        | ExactTransactionScope::ProxyAuth
        | ExactTransactionScope::Notifications => Value::Object(updated_root),
        ExactTransactionScope::EnvVars => updated_root
            .get("env_vars")
            .cloned()
            .unwrap_or_else(|| Value::Array(Vec::new())),
        ExactTransactionScope::Connect => updated_root
            .get("connect")
            .cloned()
            .unwrap_or_else(|| Value::Object(Map::new())),
        ExactTransactionScope::AccessControl => updated_root
            .get("access_control")
            .cloned()
            .unwrap_or(Value::Null),
        ExactTransactionScope::ClusterFabric => updated_root
            .get("cluster_fabric")
            .cloned()
            .unwrap_or_else(|| Value::Object(Map::new())),
    };
    match scope {
        ExactTransactionScope::EnvVars => {
            if let Some(entries) = updated_data.as_array_mut() {
                for entry in entries {
                    let Some(entry) = entry.as_object_mut() else {
                        continue;
                    };
                    if entry.get("secret").and_then(Value::as_bool) == Some(true)
                        && entry.get("configured").and_then(Value::as_bool) == Some(false)
                    {
                        entry.remove("credential_ref");
                    }
                }
            }
        }
        ExactTransactionScope::Notifications => {
            if let Some(notifications) = updated_data
                .get_mut("notifications")
                .and_then(Value::as_object_mut)
            {
                for channel in ["ntfy", "bark"] {
                    if let Some(channel) = notifications
                        .get_mut(channel)
                        .and_then(Value::as_object_mut)
                    {
                        if channel.get("configured").and_then(Value::as_bool) == Some(false) {
                            channel.remove("credential_ref");
                        }
                    }
                }
            }
        }
        ExactTransactionScope::Connect => {
            if let Some(platforms) = updated_data
                .get_mut("platforms")
                .and_then(Value::as_array_mut)
            {
                for platform in platforms.iter_mut().filter_map(Value::as_object_mut) {
                    if platform.get("token_configured").and_then(Value::as_bool) == Some(false) {
                        platform.remove("token_credential_ref");
                    }
                    if platform
                        .get("app_secret_configured")
                        .and_then(Value::as_bool)
                        == Some(false)
                    {
                        platform.remove("app_secret_credential_ref");
                    }
                }
            }
        }
        ExactTransactionScope::Providers
        | ExactTransactionScope::Mcp
        | ExactTransactionScope::ProxyAuth
        | ExactTransactionScope::ClusterFabric
        | ExactTransactionScope::AccessControl => {}
    }
    let mut envelope: Value = serde_json::from_slice(&section.original)?;
    let object = envelope.as_object_mut().ok_or_else(|| {
        ConfigStoreError::Validation("authoritative section envelope is invalid".to_string())
    })?;
    let changed = force_revision || object.get("data") != Some(&updated_data);
    let revision = if changed {
        section.expected_revision.checked_add(1).ok_or_else(|| {
            ConfigStoreError::Validation("configuration revision counter exhausted".to_string())
        })?
    } else {
        section.expected_revision
    };
    object.insert("revision".to_string(), Value::from(revision));
    object.insert("data".to_string(), updated_data);
    let candidate = serde_json::to_vec_pretty(&envelope)?;
    let validated =
        crate::section_facade::validate_section_envelope(section.name, &candidate, revision)?;
    if validated != revision {
        return Err(ConfigStoreError::Validation(
            "authoritative section revision validation failed".to_string(),
        ));
    }
    Ok((candidate, changed))
}

fn prepare_proxy_auth_config_document(
    current: &[u8],
    candidate: &crate::Config,
) -> ConfigStoreResult<Vec<u8>> {
    let mut root = parse_config_root_object(
        current,
        "config.json is invalid during proxy credential transaction",
    )?;
    for key in PROXY_AUTH_DOMAIN_KEYS {
        root.remove(key);
    }
    if let Some(reference) = candidate.proxy_auth_credential_ref.as_ref() {
        root.insert(
            "proxy_auth_credential_ref".to_string(),
            Value::String(reference.as_str().to_string()),
        );
    }
    let document = Value::Object(root);
    serde_json::from_value::<crate::Config>(document.clone()).map_err(|_| {
        ConfigStoreError::Validation(
            "config.json is invalid during proxy credential transaction".to_string(),
        )
    })?;
    Ok(serde_json::to_vec_pretty(&document)?)
}

fn prepare_env_var_config_document(
    current: &[u8],
    candidate: &crate::Config,
) -> ConfigStoreResult<Vec<u8>> {
    let mut root = parse_config_root_object(
        current,
        "config.json is invalid during env credential transaction",
    )?;
    let current_entries = root
        .get("env_vars")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let object = entry.as_object()?.clone();
            let name = object.get("name")?.as_str()?.to_string();
            Some((name, object))
        })
        .collect::<BTreeMap<_, _>>();
    let mut entries = Vec::with_capacity(candidate.env_vars.len());
    for mut entry in candidate.env_vars.clone() {
        if entry.secret {
            if entry.configured && entry.credential_ref.is_none() {
                return Err(ConfigStoreError::Validation(
                    "secret env credential metadata is incomplete".to_string(),
                ));
            }
            entry.value.clear();
            entry.value_encrypted = None;
        } else {
            entry.credential_ref = None;
            entry.configured = !entry.value.is_empty();
        }
        let mut durable = current_entries
            .get(&entry.name)
            .cloned()
            .unwrap_or_default();
        for key in [
            "name",
            "value",
            "secret",
            "value_encrypted",
            "credential_ref",
            "configured",
            "description",
        ] {
            durable.remove(key);
        }
        let typed = serde_json::to_value(entry)?;
        durable.extend(
            typed
                .as_object()
                .expect("env var serializes as object")
                .clone(),
        );
        entries.push(Value::Object(durable));
    }
    if entries.is_empty() {
        root.remove("env_vars");
    } else {
        root.insert("env_vars".to_string(), Value::Array(entries));
    }
    let document = Value::Object(root);
    serde_json::from_value::<crate::Config>(document.clone()).map_err(|_| {
        ConfigStoreError::Validation(
            "config.json is invalid during env credential transaction".to_string(),
        )
    })?;
    Ok(serde_json::to_vec_pretty(&document)?)
}

fn prepare_notification_config_document(
    original: &[u8],
    candidate: &crate::Config,
    reset_domain: bool,
) -> ConfigStoreResult<Vec<u8>> {
    let mut root = parse_config_root_object(
        original,
        "config.json is invalid during notification credential transaction",
    )?;
    let candidate_notifications = serde_json::to_value(&candidate.notifications)?;
    let candidate_notifications = candidate_notifications.as_object().ok_or_else(|| {
        ConfigStoreError::Validation(
            "notification config transaction document is invalid".to_string(),
        )
    })?;
    if reset_domain {
        root.insert(
            "notifications".to_string(),
            Value::Object(candidate_notifications.clone()),
        );
        let value = Value::Object(root);
        serde_json::from_value::<crate::Config>(value.clone()).map_err(|_| {
            ConfigStoreError::Validation(
                "config.json is invalid during notification credential transaction".to_string(),
            )
        })?;
        return Ok(serde_json::to_vec_pretty(&value)?);
    }
    let notifications = root
        .entry("notifications".to_string())
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .ok_or_else(|| {
            ConfigStoreError::Validation(
                "notifications must be an object during credential transaction".to_string(),
            )
        })?;
    for channel in ["desktop", "ntfy", "bark"] {
        let candidate_channel = candidate_notifications
            .get(channel)
            .and_then(Value::as_object)
            .ok_or_else(|| {
                ConfigStoreError::Validation(
                    "notification channel config transaction document is invalid".to_string(),
                )
            })?;
        let channel_object = notifications
            .entry(channel.to_string())
            .or_insert_with(|| Value::Object(Map::new()))
            .as_object_mut()
            .ok_or_else(|| {
                ConfigStoreError::Validation(format!(
                    "notification channel '{channel}' must be an object"
                ))
            })?;
        for (key, value) in candidate_channel {
            channel_object.insert(key.clone(), value.clone());
        }
        if channel == "ntfy" {
            channel_object.remove("token");
            channel_object.remove("token_encrypted");
        } else if channel == "bark" {
            channel_object.remove("device_key");
            channel_object.remove("device_key_encrypted");
        }
    }
    let value = Value::Object(root);
    serde_json::from_value::<crate::Config>(value.clone()).map_err(|_| {
        ConfigStoreError::Validation(
            "config.json is invalid during notification credential transaction".to_string(),
        )
    })?;
    Ok(serde_json::to_vec_pretty(&value)?)
}

fn env_var_domain_changed(current: &[u8], candidate: &[u8]) -> ConfigStoreResult<bool> {
    fn domain(bytes: &[u8]) -> ConfigStoreResult<Vec<Value>> {
        let root = parse_config_root_object(
            bytes,
            "config.json is invalid during env credential transaction",
        )?;
        Ok(root
            .get("env_vars")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default())
    }

    Ok(domain(current)? != domain(candidate)?)
}

fn notification_domain_changed(current: &[u8], candidate: &[u8]) -> ConfigStoreResult<bool> {
    let current = parse_config_root_object(
        current,
        "config.json is invalid during notification credential transaction",
    )?;
    let candidate = parse_config_root_object(
        candidate,
        "config.json is invalid during notification credential transaction",
    )?;
    Ok(current.get("notifications") != candidate.get("notifications"))
}

fn connect_refs_from_document(
    bytes: &[u8],
) -> ConfigStoreResult<crate::credential_store::PersistedConnectCredentialRefs> {
    let root = parse_config_root_object(
        bytes,
        "config.json is invalid during connect credential transaction",
    )?;
    let Some(platforms) = root
        .get("connect")
        .and_then(|connect| connect.get("platforms"))
        .and_then(Value::as_array)
    else {
        return Ok(BTreeMap::new());
    };
    let mut refs = BTreeMap::new();
    for platform in platforms {
        let platform = platform.as_object().ok_or_else(|| {
            ConfigStoreError::Validation("connect platform metadata must be an object".to_string())
        })?;
        let id = platform.get("id").and_then(Value::as_str).ok_or_else(|| {
            ConfigStoreError::Validation(
                "connect credential metadata requires a stable platform id".to_string(),
            )
        })?;
        if id.trim().is_empty() || refs.contains_key(id) {
            return Err(ConfigStoreError::Validation(
                "connect platform ids must be nonempty and unique".to_string(),
            ));
        }
        let parse = |key: &str| -> ConfigStoreResult<Option<CredentialRef>> {
            platform
                .get(key)
                .map(|value| {
                    value
                        .as_str()
                        .ok_or_else(|| {
                            ConfigStoreError::Validation(
                                "connect credential reference must be a string".to_string(),
                            )
                        })
                        .and_then(|value| CredentialRef::parse(value.to_string()))
                })
                .transpose()
        };
        refs.insert(
            id.to_string(),
            (
                parse("token_credential_ref")?,
                parse("app_secret_credential_ref")?,
            ),
        );
    }
    Ok(refs)
}

fn prepare_connect_config_document(
    current: &[u8],
    candidate: &crate::Config,
) -> ConfigStoreResult<Vec<u8>> {
    let mut root = parse_config_root_object(
        current,
        "config.json is invalid during connect credential transaction",
    )?;
    let mut candidate = candidate.clone();
    candidate.sanitize_connect_credentials_for_disk();
    crate::section_facade::validate_connect_isolated(&candidate.connect)
        .map_err(ConfigStoreError::Validation)?;
    let candidate_connect = serde_json::to_value(&candidate.connect)?;
    let candidate_connect = candidate_connect.as_object().ok_or_else(|| {
        ConfigStoreError::Validation("connect config transaction document is invalid".to_string())
    })?;
    let mut connect = root
        .remove("connect")
        .unwrap_or_else(|| Value::Object(Map::new()))
        .as_object()
        .cloned()
        .ok_or_else(|| {
            ConfigStoreError::Validation(
                "connect must be an object during credential transaction".to_string(),
            )
        })?;
    let current_platforms = connect
        .get("platforms")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|platform| {
            let platform = platform.as_object()?.clone();
            let id = platform.get("id")?.as_str()?.to_string();
            Some((id, platform))
        })
        .collect::<BTreeMap<_, _>>();
    let mut platforms = Vec::new();
    for platform in candidate_connect
        .get("platforms")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let typed = platform.as_object().ok_or_else(|| {
            ConfigStoreError::Validation(
                "connect platform config transaction document is invalid".to_string(),
            )
        })?;
        let id = typed.get("id").and_then(Value::as_str).ok_or_else(|| {
            ConfigStoreError::Validation(
                "connect credential metadata requires a stable platform id".to_string(),
            )
        })?;
        let mut durable = current_platforms.get(id).cloned().unwrap_or_default();
        for key in [
            "id",
            "type",
            "token",
            "token_encrypted",
            "token_credential_ref",
            "token_configured",
            "app_id",
            "app_secret",
            "app_secret_encrypted",
            "app_secret_credential_ref",
            "app_secret_configured",
            "domain",
            "allow_from",
            "admin_from",
        ] {
            durable.remove(key);
        }
        durable.extend(typed.clone());
        platforms.push(Value::Object(durable));
    }
    connect.insert("platforms".to_string(), Value::Array(platforms));
    root.insert("connect".to_string(), Value::Object(connect));
    let document = Value::Object(root);
    serde_json::from_value::<crate::Config>(document.clone()).map_err(|_| {
        ConfigStoreError::Validation(
            "config.json is invalid during connect credential transaction".to_string(),
        )
    })?;
    Ok(serde_json::to_vec_pretty(&document)?)
}

fn connect_domain_changed(current: &[u8], candidate: &[u8]) -> ConfigStoreResult<bool> {
    let current = parse_config_root_object(
        current,
        "config.json is invalid during connect credential transaction",
    )?;
    let candidate = parse_config_root_object(
        candidate,
        "config.json is invalid during connect credential transaction",
    )?;
    Ok(current.get("connect") != candidate.get("connect"))
}

fn access_refs_from_document(
    bytes: &[u8],
) -> ConfigStoreResult<crate::credential_store::PersistedAccessCredentialRefs> {
    let root = parse_config_root_object(
        bytes,
        "config.json is invalid during access-control credential transaction",
    )?;
    let Some(access) = root.get("access_control") else {
        return Ok(Default::default());
    };
    if access.is_null() {
        return Ok(Default::default());
    }
    let access = access.as_object().ok_or_else(|| {
        ConfigStoreError::Validation("access_control must be an object or null".to_string())
    })?;
    let parse_ref = |value: Option<&Value>| -> ConfigStoreResult<Option<CredentialRef>> {
        value
            .map(|value| {
                value
                    .as_str()
                    .ok_or_else(|| {
                        ConfigStoreError::Validation(
                            "access-control credential reference must be a string".to_string(),
                        )
                    })
                    .and_then(|value| CredentialRef::parse(value.to_string()))
            })
            .transpose()
    };
    let password = parse_ref(access.get("password_credential_ref"))?;
    let mut devices = BTreeMap::new();
    for device in access
        .get("devices")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let device = device.as_object().ok_or_else(|| {
            ConfigStoreError::Validation("access-control device must be an object".to_string())
        })?;
        let id = device
            .get("device_id")
            .and_then(Value::as_str)
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| {
                ConfigStoreError::Validation("access-control device id is missing".to_string())
            })?;
        if let Some(reference) = parse_ref(device.get("token_credential_ref"))? {
            if devices.insert(id.to_string(), reference).is_some() {
                return Err(ConfigStoreError::Validation(
                    "access-control device ids must be unique".to_string(),
                ));
            }
        }
    }
    Ok(crate::credential_store::PersistedAccessCredentialRefs { password, devices })
}

fn prepare_access_control_config_document(
    current: &[u8],
    candidate: &crate::Config,
) -> ConfigStoreResult<Vec<u8>> {
    let mut root = parse_config_root_object(
        current,
        "config.json is invalid during access-control credential transaction",
    )?;
    let mut candidate = candidate.clone();
    candidate.sanitize_access_control_for_disk();
    let access = crate::AccessControlSection(candidate.access_control.clone());
    let envelope = serde_json::json!({
        "schema_version": 1,
        "revision": 0,
        "data": access,
    });
    crate::section_facade::validate_section_envelope(
        ACCESS_CONTROL_FILE,
        &serde_json::to_vec(&envelope)?,
        0,
    )?;
    match serde_json::to_value(&candidate.access_control)? {
        Value::Null => {
            root.remove("access_control");
        }
        value => {
            root.insert("access_control".to_string(), value);
        }
    }
    Ok(serde_json::to_vec_pretty(&Value::Object(root))?)
}

fn access_control_domain_changed(current: &[u8], candidate: &[u8]) -> ConfigStoreResult<bool> {
    let current = parse_config_root_object(
        current,
        "config.json is invalid during access-control credential transaction",
    )?;
    let candidate = parse_config_root_object(
        candidate,
        "config.json is invalid during access-control credential transaction",
    )?;
    Ok(current.get("access_control") != candidate.get("access_control"))
}

fn cluster_node_refs_from_document(
    bytes: &[u8],
) -> ConfigStoreResult<BTreeMap<String, crate::ClusterNodeCredentialRefs>> {
    let root = parse_config_root_object(
        bytes,
        "config.json is invalid during cluster credential transaction",
    )?;
    let Some(value) = root
        .get("cluster_fabric")
        .and_then(Value::as_object)
        .and_then(|cluster| cluster.get("credential_refs"))
    else {
        return Ok(BTreeMap::new());
    };
    serde_json::from_value(value.clone()).map_err(|_| {
        ConfigStoreError::Validation(
            "cluster credential metadata is invalid during credential transaction".to_string(),
        )
    })
}

fn prepare_cluster_fabric_config_document(
    current: &[u8],
    candidate: &crate::Config,
) -> ConfigStoreResult<Vec<u8>> {
    let mut root = parse_config_root_object(
        current,
        "config.json is invalid during cluster credential transaction",
    )?;
    let mut candidate = candidate.clone();
    candidate.sanitize_cluster_fabric_for_disk();
    let candidate_cluster = serde_json::to_value(&candidate.cluster_fabric)?;
    let candidate_cluster = candidate_cluster.as_object().ok_or_else(|| {
        ConfigStoreError::Validation("cluster config transaction document is invalid".to_string())
    })?;
    let mut cluster = root
        .remove("cluster_fabric")
        .unwrap_or_else(|| Value::Object(Map::new()))
        .as_object()
        .cloned()
        .ok_or_else(|| {
            ConfigStoreError::Validation(
                "cluster_fabric must be an object during credential transaction".to_string(),
            )
        })?;

    // Replace the typed domain while retaining unknown/future cluster fields.
    for key in [
        "clusters",
        "nodes",
        "credential_refs",
        "health_interval_secs",
    ] {
        let current_value = cluster.get(key).cloned();
        cluster.remove(key);
        if let Some(value) = candidate_cluster.get(key) {
            let value = if key == "nodes" {
                merge_cluster_named_array(
                    current_value.as_ref(),
                    Some(value),
                    "id",
                    "cluster node",
                )?
            } else if key == "clusters" {
                merge_cluster_named_array(current_value.as_ref(), Some(value), "name", "cluster")?
            } else {
                Some(value.clone())
            };
            if let Some(value) = value {
                cluster.insert(key.to_string(), value);
            }
        }
    }
    if cluster.is_empty() {
        root.remove("cluster_fabric");
    } else {
        root.insert("cluster_fabric".to_string(), Value::Object(cluster));
    }
    let document = Value::Object(root);
    serde_json::from_value::<crate::Config>(document.clone()).map_err(|_| {
        ConfigStoreError::Validation(
            "config.json is invalid during cluster credential transaction".to_string(),
        )
    })?;
    Ok(serde_json::to_vec_pretty(&document)?)
}

fn merge_cluster_named_array(
    current: Option<&Value>,
    candidate: Option<&Value>,
    identity_key: &str,
    label: &str,
) -> ConfigStoreResult<Option<Value>> {
    let Some(candidate) = candidate else {
        return Ok(None);
    };
    let candidate = candidate
        .as_array()
        .ok_or_else(|| ConfigStoreError::Validation(format!("{label} collection is invalid")))?;
    let mut prior = BTreeMap::<String, Map<String, Value>>::new();
    if let Some(current) = current {
        let current = current.as_array().ok_or_else(|| {
            ConfigStoreError::Validation(format!("{label} collection is invalid"))
        })?;
        for item in current {
            let object = item
                .as_object()
                .ok_or_else(|| ConfigStoreError::Validation(format!("{label} entry is invalid")))?;
            let identity = object
                .get(identity_key)
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    ConfigStoreError::Validation(format!("{label} identity is invalid"))
                })?;
            prior.insert(identity.to_string(), object.clone());
        }
    }
    let mut merged = Vec::with_capacity(candidate.len());
    for item in candidate {
        let object = item
            .as_object()
            .ok_or_else(|| ConfigStoreError::Validation(format!("{label} entry is invalid")))?;
        let identity = object
            .get(identity_key)
            .and_then(Value::as_str)
            .ok_or_else(|| ConfigStoreError::Validation(format!("{label} identity is invalid")))?;
        let mut merged_object = prior.remove(identity).unwrap_or_default();
        merged_object.extend(object.clone());
        merged.push(Value::Object(merged_object));
    }
    Ok(Some(Value::Array(merged)))
}

fn cluster_fabric_domain_changed(current: &[u8], candidate: &[u8]) -> ConfigStoreResult<bool> {
    let current = parse_config_root_object(
        current,
        "config.json is invalid during cluster credential transaction",
    )?;
    let candidate = parse_config_root_object(
        candidate,
        "config.json is invalid during cluster credential transaction",
    )?;
    Ok(current.get("cluster_fabric") != candidate.get("cluster_fabric"))
}

fn parse_config_root_object(
    bytes: &[u8],
    invalid_message: &str,
) -> ConfigStoreResult<Map<String, Value>> {
    if bytes.is_empty() {
        return Ok(Map::new());
    }
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|_| ConfigStoreError::Validation(invalid_message.to_string()))?;
    value
        .as_object()
        .cloned()
        .ok_or_else(|| ConfigStoreError::Validation(invalid_message.to_string()))
}

fn proxy_auth_domain(root: &Map<String, Value>) -> Map<String, Value> {
    PROXY_AUTH_DOMAIN_KEYS
        .iter()
        .filter_map(|key| {
            root.get(*key)
                .cloned()
                .map(|value| ((*key).to_string(), value))
        })
        .collect()
}

fn replace_proxy_auth_domain(target: &mut Map<String, Value>, source: &Map<String, Value>) {
    for key in PROXY_AUTH_DOMAIN_KEYS {
        target.remove(key);
        if let Some(value) = source.get(key) {
            target.insert(key.to_string(), value.clone());
        }
    }
}

fn write_manifest(path: PathBuf, manifest: &MigrationManifest) -> ConfigStoreResult<()> {
    AtomicFileStore::new(path).write_bytes_without_backup(&serde_json::to_vec_pretty(manifest)?)
}

fn validate_matching_cluster_journal(
    manifest: &MigrationManifest,
    journal: &MigrationManifest,
) -> ConfigStoreResult<()> {
    if manifest.exact_scope != Some(ExactTransactionScope::ClusterFabric)
        || journal.exact_scope != Some(ExactTransactionScope::ClusterFabric)
        || journal.transaction_id != manifest.transaction_id
        || journal.stage_dir != manifest.stage_dir
        || journal.migration_scope != manifest.migration_scope
    {
        return Err(ConfigStoreError::Validation(
            "cluster abort journal does not match its committed manifest".to_string(),
        ));
    }
    Ok(())
}

/// Return whether the immutable transaction journal carries durable non-commit
/// intent for this exact cluster transaction.
///
/// The journal's original file members are the rollback authority and may
/// differ from a rebased main manifest. Matching therefore compares identity
/// and scope only, never candidate member metadata.
fn matching_cluster_abort_journal(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<bool> {
    let Some(bytes) = read_optional_migration_file(&data_dir.join(JOURNAL_FILE))? else {
        return Ok(false);
    };
    let journal: MigrationManifest = serde_json::from_slice(&bytes)?;
    validate_manifest(&journal)?;
    if journal.state != MigrationState::Aborting {
        return Ok(false);
    }
    validate_matching_cluster_journal(manifest, &journal)?;
    Ok(true)
}

/// Persist durable abort intent into the immutable journal before the mutable
/// main manifest is transitioned. Only `state` changes; original staged-member
/// metadata remains available for exact rollback after rebases or restart.
fn persist_cluster_abort_journal(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    let path = data_dir.join(JOURNAL_FILE);
    let bytes = read_optional_migration_file(&path)?.ok_or_else(|| {
        ConfigStoreError::Validation("cluster abort journal is missing".to_string())
    })?;
    let mut journal: MigrationManifest = serde_json::from_slice(&bytes)?;
    validate_manifest(&journal)?;
    validate_matching_cluster_journal(manifest, &journal)?;
    if journal.state == MigrationState::Complete {
        return Err(ConfigStoreError::Validation(
            "a complete cluster journal cannot enter abort rollback".to_string(),
        ));
    }
    if journal.state != MigrationState::Aborting {
        #[cfg(any(test, feature = "test-utils"))]
        if cluster_abort_journal_marker_test_fault(data_dir) {
            return Err(ConfigStoreError::Validation(
                "injected persistent cluster abort journal marker failure".to_string(),
            ));
        }
        journal.state = MigrationState::Aborting;
        write_manifest(path, &journal)?;
    }
    Ok(())
}

fn recover_committed(
    data_dir: &Path,
    #[cfg(test)] fault: Option<MigrationFault>,
) -> ConfigStoreResult<Option<CredentialMigrationOutcome>> {
    let path = data_dir.join(MANIFEST_FILE);
    let Some(bytes) = read_optional_migration_file(&path)? else {
        return Ok(None);
    };
    let mut manifest: MigrationManifest = serde_json::from_slice(&bytes)?;
    validate_manifest(&manifest)?;
    if manifest.state == MigrationState::Complete {
        if is_section_layout_scope(manifest.migration_scope) {
            verify_section_split_completion(data_dir, &manifest)?;
            persist_section_layout_completion_ledger(data_dir, &manifest)?;
        }
        cleanup_transaction_dirs(data_dir, &manifest)?;
        remove_file_if_exists(&data_dir.join(JOURNAL_FILE))?;
        return Ok(None);
    }
    if manifest.state == MigrationState::Pending
        && matching_cluster_abort_journal(data_dir, &manifest)?
    {
        // The journal was durably marked before a failed main-manifest abort
        // transition. Resume rollback before any install validation; the
        // original conflict may no longer exist after restart.
        abort_cluster_exact_transaction(data_dir, &manifest, None)?;
        return Ok(None);
    }
    if manifest.state == MigrationState::Aborting {
        if manifest.exact_scope != Some(ExactTransactionScope::ClusterFabric) {
            return Err(ConfigStoreError::Validation(
                "only cluster exact transactions may resume abort rollback".to_string(),
            ));
        }
        abort_cluster_exact_transaction(data_dir, &manifest, None)?;
        return Ok(None);
    }
    install_pending(
        data_dir,
        &mut manifest,
        None,
        #[cfg(test)]
        fault,
    )?;
    finish_transaction(data_dir, manifest)?;
    Ok(Some(CredentialMigrationOutcome {
        migrated_credentials: 0,
        resumed: true,
    }))
}

fn initial_exact_transaction_member(
    data_dir: &Path,
    manifest: &MigrationManifest,
    name: &str,
) -> ConfigStoreResult<(StagedFile, Vec<u8>)> {
    let journal_bytes =
        read_optional_migration_file(&data_dir.join(JOURNAL_FILE))?.ok_or_else(|| {
            ConfigStoreError::Validation("credential transaction journal is missing".to_string())
        })?;
    let journal: MigrationManifest = serde_json::from_slice(&journal_bytes)?;
    validate_manifest(&journal)?;
    if journal.transaction_id != manifest.transaction_id
        || journal.stage_dir != manifest.stage_dir
        || journal.exact_scope != manifest.exact_scope
    {
        return Err(ConfigStoreError::Validation(
            "credential transaction journal does not match the committed manifest".to_string(),
        ));
    }
    let file = journal
        .files
        .into_iter()
        .find(|file| file.name == name)
        .ok_or_else(|| {
            ConfigStoreError::Validation("credential transaction journal is incomplete".to_string())
        })?;
    let staged = read_managed_file(data_dir, &journal.stage_dir, &file.staged_name)?;
    if sha256(&staged) != file.sha256 {
        return Err(ConfigStoreError::Validation(
            "initial transaction document failed integrity validation".to_string(),
        ));
    }
    Ok((file, staged))
}

fn rollback_active_section_member(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    let scope = manifest.exact_scope.ok_or_else(|| {
        ConfigStoreError::Validation("exact transaction scope is missing".to_string())
    })?;
    let name = authoritative_section_file(scope);
    if !manifest.files.iter().any(|file| file.name == name) {
        return Ok(());
    }
    let (initial_file, initial_staged) =
        initial_exact_transaction_member(data_dir, manifest, name)?;
    let original = read_encrypted_migration_backup(data_dir, &manifest.transaction_id, name)?;
    let (_, _, original_data) = parse_exact_section_document(name, &original)?;
    let (_, _, staged_data) = parse_exact_section_document(name, &initial_staged)?;
    let target = data_dir.join(name);
    for _ in 0..16 {
        let current = read_target_or_empty(&target)?;
        let current_hash = sha256(&current);
        let (mut envelope, current_revision, current_data) =
            parse_exact_section_document(name, &current)?;
        let rolled_back = match merge_active_section_data(
            scope,
            &current_data,
            &staged_data,
            &original_data,
            &initial_file.touched_env_names,
        ) {
            Ok(rolled_back) => rolled_back,
            Err(_) => {
                // A later same-domain writer won. Never overwrite it while
                // compensating the credential half of an aborted transaction.
                return Ok(());
            }
        };
        if rolled_back == current_data {
            return Ok(());
        }
        let revision = current_revision.checked_add(1).ok_or_else(|| {
            ConfigStoreError::Validation("configuration revision counter exhausted".to_string())
        })?;
        let object = envelope.as_object_mut().ok_or_else(|| {
            ConfigStoreError::Validation("authoritative section envelope is invalid".to_string())
        })?;
        object.insert("revision".to_string(), Value::from(revision));
        object.insert("data".to_string(), rolled_back);
        let bytes = serde_json::to_vec_pretty(&envelope)?;
        crate::section_facade::validate_section_envelope(name, &bytes, revision)?;
        if AtomicFileStore::new(&target).write_bytes_if_hash_with_backup(&current_hash, &bytes)? {
            return Ok(());
        }
    }
    Err(ConfigStoreError::Validation(
        "authoritative section rollback could not obtain a stable revision".to_string(),
    ))
}

fn rollback_exact_authority_member(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    match exact_authority_file(manifest) {
        Some(CONFIG_FILE) => match manifest.exact_scope {
            Some(ExactTransactionScope::Providers) | Some(ExactTransactionScope::Mcp) => {
                Err(ConfigStoreError::Validation(
                    "runtime section exact transaction has no legacy root authority".to_string(),
                ))
            }
            Some(ExactTransactionScope::ProxyAuth) => {
                rollback_proxy_config_member(data_dir, manifest)
            }
            Some(ExactTransactionScope::EnvVars) => rollback_env_config_member(data_dir, manifest),
            Some(ExactTransactionScope::Notifications) => {
                rollback_notification_config_member(data_dir, manifest)
            }
            Some(ExactTransactionScope::Connect) => Err(ConfigStoreError::Validation(
                "connect exact transaction has no active section authority".to_string(),
            )),
            Some(ExactTransactionScope::AccessControl) => Err(ConfigStoreError::Validation(
                "access-control exact transaction has no active section authority".to_string(),
            )),
            Some(ExactTransactionScope::ClusterFabric) => {
                rollback_cluster_config_member(data_dir, manifest)
            }
            None => Ok(()),
        },
        Some(_) => rollback_active_section_member(data_dir, manifest),
        None => Ok(()),
    }
}

fn rollback_proxy_config_member(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    let (initial_file, initial_staged) =
        initial_exact_transaction_member(data_dir, manifest, CONFIG_FILE)?;
    let original =
        read_encrypted_migration_backup(data_dir, &manifest.transaction_id, CONFIG_FILE)?;
    if initial_file.original_sha256.as_deref() != Some(sha256(&original).as_str()) {
        return Err(ConfigStoreError::Validation(
            "config transaction backup failed integrity validation".to_string(),
        ));
    }
    let original_root = parse_config_root_object(
        &original,
        "config transaction backup is not a valid document",
    )?;
    let staged_root = parse_config_root_object(
        &initial_staged,
        "initial proxy config transaction document is invalid",
    )?;
    let staged_domain = proxy_auth_domain(&staged_root);
    let target = data_dir.join(CONFIG_FILE);

    for _ in 0..16 {
        let current = read_target_or_empty(&target)?;
        let current_hash = sha256(&current);
        if current_hash == sha256(&original) {
            return Ok(());
        }
        let mut current_root = parse_config_root_object(
            &current,
            "config.json changed to an invalid document while aborting proxy transaction",
        )?;
        if proxy_auth_domain(&current_root) != staged_domain {
            // The transaction never installed its auth-domain change, or a
            // later same-domain writer won. Preserve that current value.
            return Ok(());
        }
        replace_proxy_auth_domain(&mut current_root, &original_root);
        let rolled_back = Value::Object(current_root);
        serde_json::from_value::<crate::Config>(rolled_back.clone()).map_err(|_| {
            ConfigStoreError::Validation(
                "config.json is invalid after proxy transaction rollback".to_string(),
            )
        })?;
        let rolled_back = serde_json::to_vec_pretty(&rolled_back)?;
        if sha256(&rolled_back) == current_hash {
            return Ok(());
        }
        if AtomicFileStore::new(&target)
            .write_bytes_if_hash_with_backup(&current_hash, &rolled_back)?
        {
            return Ok(());
        }
    }
    Err(ConfigStoreError::Validation(
        "proxy config transaction rollback could not obtain a stable document".to_string(),
    ))
}

fn rollback_exact_credential_member(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    let credential_file = manifest
        .files
        .iter()
        .find(|file| file.name == CREDENTIALS_FILE)
        .ok_or_else(|| {
            ConfigStoreError::Validation(
                "committed credential transaction is incomplete".to_string(),
            )
        })?;
    let (initial_file, initial_staged) =
        initial_exact_transaction_member(data_dir, manifest, CREDENTIALS_FILE)?;
    let original =
        read_encrypted_migration_backup(data_dir, &manifest.transaction_id, CREDENTIALS_FILE)?;
    if initial_file.original_sha256.as_deref() != Some(sha256(&original).as_str()) {
        return Err(ConfigStoreError::Validation(
            "credential transaction backup failed integrity validation".to_string(),
        ));
    }
    let target = data_dir.join(CREDENTIALS_FILE);
    let authority = exact_authority_file(manifest);
    let authority_value = authority
        .filter(|name| *name != CONFIG_FILE)
        .map(|name| read_target_or_empty(&data_dir.join(name)))
        .transpose()?
        .filter(|bytes| !bytes.is_empty())
        .map(|bytes| serde_json::from_slice::<Value>(&bytes))
        .transpose()?;
    let rollback_refs = credential_file
        .touched_credential_refs
        .iter()
        .filter(|reference| {
            !authority_value
                .as_ref()
                .is_some_and(|value| contains_credential_reference(value, reference))
        })
        .cloned()
        .collect::<Vec<_>>();
    for _ in 0..16 {
        let current = read_target_or_empty(&target)?;
        let current_hash = sha256(&current);
        if current_hash == sha256(&original) {
            return Ok(());
        }
        let (rolled_back, _revision, changed) =
            CredentialStore::rollback_exact_transaction_documents(
                &original,
                &initial_staged,
                &current,
                &rollback_refs,
            )?;
        if !changed {
            return Ok(());
        }
        if AtomicFileStore::new(&target)
            .sensitive(true)
            .write_bytes_if_hash_with_backup(&current_hash, &rolled_back)?
        {
            return Ok(());
        }
    }
    Err(ConfigStoreError::Validation(
        "credential transaction rollback could not obtain a stable revision".to_string(),
    ))
}

fn rollback_proxy_credential_member(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    rollback_exact_credential_member(data_dir, manifest)
}

fn abort_proxy_exact_transaction(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    if manifest.exact_scope != Some(ExactTransactionScope::ProxyAuth) {
        return Ok(());
    }
    rollback_exact_authority_member(data_dir, manifest)?;
    rollback_proxy_credential_member(data_dir, manifest)?;
    let mut complete = manifest.clone();
    complete.state = MigrationState::Complete;
    write_manifest(data_dir.join(MANIFEST_FILE), &complete)?;
    remove_file_if_exists(&data_dir.join(JOURNAL_FILE))?;
    cleanup_transaction_dirs(data_dir, &complete)?;
    remove_file_if_exists(&data_dir.join(MANIFEST_FILE))?;
    sync_dir(data_dir)
}

fn rollback_env_config_member(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    let (_initial_file, initial_staged) =
        initial_exact_transaction_member(data_dir, manifest, CONFIG_FILE)?;
    let staged = parse_config_root_object(
        &initial_staged,
        "staged env config transaction document is invalid",
    )?;
    let original =
        read_encrypted_migration_backup(data_dir, &manifest.transaction_id, CONFIG_FILE)?;
    let original = parse_config_root_object(
        &original,
        "config transaction backup is not a valid document",
    )?;
    let target = data_dir.join(CONFIG_FILE);
    for _ in 0..16 {
        let current = read_target_or_empty(&target)?;
        let current_hash = sha256(&current);
        let mut object = parse_config_root_object(
            &current,
            "config.json changed to an invalid document during env rollback",
        )?;
        if object.get("env_vars") != staged.get("env_vars") {
            return Ok(());
        }
        match original.get("env_vars").cloned() {
            Some(entries) => {
                object.insert("env_vars".to_string(), entries);
            }
            None => {
                object.remove("env_vars");
            }
        }
        let rolled_back = serde_json::to_vec_pretty(&Value::Object(object))?;
        if AtomicFileStore::new(&target)
            .write_bytes_if_hash_with_backup(&current_hash, &rolled_back)?
        {
            return Ok(());
        }
    }
    Err(ConfigStoreError::Validation(
        "env config transaction rollback could not obtain a stable document".to_string(),
    ))
}

fn abort_env_exact_transaction(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    if manifest.exact_scope != Some(ExactTransactionScope::EnvVars) {
        return Ok(());
    }
    rollback_exact_authority_member(data_dir, manifest)?;
    rollback_proxy_credential_member(data_dir, manifest)?;
    let mut complete = manifest.clone();
    complete.state = MigrationState::Complete;
    write_manifest(data_dir.join(MANIFEST_FILE), &complete)?;
    remove_file_if_exists(&data_dir.join(JOURNAL_FILE))?;
    cleanup_transaction_dirs(data_dir, &complete)?;
    remove_file_if_exists(&data_dir.join(MANIFEST_FILE))?;
    sync_dir(data_dir)
}

fn rollback_notification_config_member(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    let (_initial_file, initial_staged) =
        initial_exact_transaction_member(data_dir, manifest, CONFIG_FILE)?;
    let staged = parse_config_root_object(
        &initial_staged,
        "staged notification config transaction document is invalid",
    )?;
    let original =
        read_encrypted_migration_backup(data_dir, &manifest.transaction_id, CONFIG_FILE)?;
    let original = parse_config_root_object(
        &original,
        "config transaction backup is not a valid document",
    )?;
    let target = data_dir.join(CONFIG_FILE);
    for _ in 0..16 {
        let current = read_target_or_empty(&target)?;
        let current_hash = sha256(&current);
        let mut object = parse_config_root_object(
            &current,
            "config.json changed to an invalid document during notification rollback",
        )?;
        if object.get("notifications") != staged.get("notifications") {
            return Ok(());
        }
        match original.get("notifications").cloned() {
            Some(value) => {
                object.insert("notifications".to_string(), value);
            }
            None => {
                object.remove("notifications");
            }
        }
        let rolled_back = serde_json::to_vec_pretty(&Value::Object(object))?;
        if AtomicFileStore::new(&target)
            .write_bytes_if_hash_with_backup(&current_hash, &rolled_back)?
        {
            return Ok(());
        }
    }
    Err(ConfigStoreError::Validation(
        "notification config rollback could not obtain a stable document".to_string(),
    ))
}

fn abort_notification_exact_transaction(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    if manifest.exact_scope != Some(ExactTransactionScope::Notifications) {
        return Ok(());
    }
    rollback_exact_authority_member(data_dir, manifest)?;
    rollback_proxy_credential_member(data_dir, manifest)?;
    let mut complete = manifest.clone();
    complete.state = MigrationState::Complete;
    write_manifest(data_dir.join(MANIFEST_FILE), &complete)?;
    remove_file_if_exists(&data_dir.join(JOURNAL_FILE))?;
    cleanup_transaction_dirs(data_dir, &complete)?;
    remove_file_if_exists(&data_dir.join(MANIFEST_FILE))?;
    sync_dir(data_dir)
}

fn abort_connect_exact_transaction(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    if manifest.exact_scope != Some(ExactTransactionScope::Connect) {
        return Ok(());
    }
    rollback_exact_authority_member(data_dir, manifest)?;
    rollback_exact_credential_member(data_dir, manifest)?;
    let mut complete = manifest.clone();
    complete.state = MigrationState::Complete;
    write_manifest(data_dir.join(MANIFEST_FILE), &complete)?;
    remove_file_if_exists(&data_dir.join(JOURNAL_FILE))?;
    cleanup_transaction_dirs(data_dir, &complete)?;
    remove_file_if_exists(&data_dir.join(MANIFEST_FILE))?;
    sync_dir(data_dir)
}

fn abort_access_control_exact_transaction(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    if manifest.exact_scope != Some(ExactTransactionScope::AccessControl) {
        return Ok(());
    }
    rollback_exact_authority_member(data_dir, manifest)?;
    rollback_exact_credential_member(data_dir, manifest)?;
    let mut complete = manifest.clone();
    complete.state = MigrationState::Complete;
    write_manifest(data_dir.join(MANIFEST_FILE), &complete)?;
    remove_file_if_exists(&data_dir.join(JOURNAL_FILE))?;
    cleanup_transaction_dirs(data_dir, &complete)?;
    remove_file_if_exists(&data_dir.join(MANIFEST_FILE))?;
    sync_dir(data_dir)
}

fn rollback_cluster_config_member(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    let (_initial_file, initial_staged) =
        initial_exact_transaction_member(data_dir, manifest, CONFIG_FILE)?;
    let staged = parse_config_root_object(
        &initial_staged,
        "staged cluster config transaction document is invalid",
    )?;
    let original =
        read_encrypted_migration_backup(data_dir, &manifest.transaction_id, CONFIG_FILE)?;
    let original = parse_config_root_object(
        &original,
        "config transaction backup is not a valid document",
    )?;
    let target = data_dir.join(CONFIG_FILE);
    for _ in 0..16 {
        let current = read_target_or_empty(&target)?;
        let current_hash = sha256(&current);
        let mut object = parse_config_root_object(
            &current,
            "config.json changed to an invalid document during cluster rollback",
        )?;
        if object.get("cluster_fabric") != staged.get("cluster_fabric") {
            return Ok(());
        }
        match original.get("cluster_fabric").cloned() {
            Some(value) => {
                object.insert("cluster_fabric".to_string(), value);
            }
            None => {
                object.remove("cluster_fabric");
            }
        }
        let rolled_back = Value::Object(object);
        serde_json::from_value::<crate::Config>(rolled_back.clone()).map_err(|_| {
            ConfigStoreError::Validation(
                "config.json is invalid after cluster transaction rollback".to_string(),
            )
        })?;
        let rolled_back = serde_json::to_vec_pretty(&rolled_back)?;
        if AtomicFileStore::new(&target)
            .write_bytes_if_hash_with_backup(&current_hash, &rolled_back)?
        {
            return Ok(());
        }
    }
    Err(ConfigStoreError::Validation(
        "cluster config transaction rollback could not obtain a stable document".to_string(),
    ))
}

fn abort_cluster_exact_transaction(
    data_dir: &Path,
    manifest: &MigrationManifest,
    install_tracker: Option<&ClusterInstallTracker>,
) -> ConfigStoreResult<()> {
    if manifest.exact_scope != Some(ExactTransactionScope::ClusterFabric) {
        return Ok(());
    }
    if manifest.state == MigrationState::Complete {
        return Err(ConfigStoreError::Validation(
            "a complete cluster transaction cannot enter abort rollback".to_string(),
        ));
    }
    let mut aborting = manifest.clone();
    aborting.state = MigrationState::Aborting;

    if manifest.state == MigrationState::Aborting {
        // A restart may arrive here after the main manifest was the successful
        // fallback marker while the immutable journal remained Pending. The
        // durable main state is already sufficient non-commit evidence; leave
        // the journal's original rollback members untouched.
        if let Some(tracker) = install_tracker {
            tracker.mark_aborting();
        }
    } else {
        if let Some(tracker) = install_tracker {
            // This process has chosen abort, even though neither durable marker
            // is confirmed yet. A double write failure must not fall back into
            // committed-candidate adoption in the caller.
            tracker.mark_abort_attempt();
        }

        // Prefer the journal marker because it preserves a fallback if the
        // mutable main marker cannot be written. If the journal transition
        // fails, the main manifest is the redundant durable abort marker.
        let journal_durable = persist_cluster_abort_journal(data_dir, manifest).is_ok();
        if journal_durable {
            if let Some(tracker) = install_tracker {
                tracker.mark_aborting();
            }
        }

        #[cfg(any(test, feature = "test-utils"))]
        let main_marker = if cluster_abort_marker_test_fault(data_dir) {
            Err(ConfigStoreError::Validation(
                "injected persistent cluster abort marker failure".to_string(),
            ))
        } else {
            write_manifest(data_dir.join(MANIFEST_FILE), &aborting)
        };
        #[cfg(not(any(test, feature = "test-utils")))]
        let main_marker = write_manifest(data_dir.join(MANIFEST_FILE), &aborting);

        match main_marker {
            Ok(()) => {
                if let Some(tracker) = install_tracker {
                    tracker.mark_aborting();
                }
            }
            Err(error) if journal_durable => return Err(error),
            Err(_) => {
                return Err(ConfigStoreError::Validation(
                    "cluster abort intent could not be persisted to either durable marker"
                        .to_string(),
                ));
            }
        }
    }
    #[cfg(any(test, feature = "test-utils"))]
    if cluster_abort_cleanup_test_fault(data_dir) {
        return Err(ConfigStoreError::Validation(
            "injected persistent cluster abort cleanup failure".to_string(),
        ));
    }
    rollback_exact_authority_member(data_dir, &aborting)?;
    rollback_exact_credential_member(data_dir, &aborting)?;
    let mut complete = aborting;
    complete.state = MigrationState::Complete;
    write_manifest(data_dir.join(MANIFEST_FILE), &complete)?;
    remove_file_if_exists(&data_dir.join(JOURNAL_FILE))?;
    cleanup_transaction_dirs(data_dir, &complete)?;
    remove_file_if_exists(&data_dir.join(MANIFEST_FILE))?;
    sync_dir(data_dir)
}

fn abort_exact_transaction(
    data_dir: &Path,
    manifest: &MigrationManifest,
    install_tracker: Option<&ClusterInstallTracker>,
) -> ConfigStoreResult<()> {
    match manifest.exact_scope {
        Some(ExactTransactionScope::Providers) => {
            abort_active_exact_transaction(data_dir, manifest, ExactTransactionScope::Providers)
        }
        Some(ExactTransactionScope::Mcp) => {
            abort_active_exact_transaction(data_dir, manifest, ExactTransactionScope::Mcp)
        }
        Some(ExactTransactionScope::ProxyAuth) => abort_proxy_exact_transaction(data_dir, manifest),
        Some(ExactTransactionScope::EnvVars) => abort_env_exact_transaction(data_dir, manifest),
        Some(ExactTransactionScope::Notifications) => {
            abort_notification_exact_transaction(data_dir, manifest)
        }
        Some(ExactTransactionScope::Connect) => abort_connect_exact_transaction(data_dir, manifest),
        Some(ExactTransactionScope::AccessControl) => {
            abort_access_control_exact_transaction(data_dir, manifest)
        }
        Some(ExactTransactionScope::ClusterFabric) => {
            abort_cluster_exact_transaction(data_dir, manifest, install_tracker)
        }
        None => Ok(()),
    }
}

fn abort_active_exact_transaction(
    data_dir: &Path,
    manifest: &MigrationManifest,
    scope: ExactTransactionScope,
) -> ConfigStoreResult<()> {
    if manifest.exact_scope != Some(scope) {
        return Ok(());
    }
    rollback_exact_authority_member(data_dir, manifest)?;
    rollback_exact_credential_member(data_dir, manifest)?;
    let mut complete = manifest.clone();
    complete.state = MigrationState::Complete;
    write_manifest(data_dir.join(MANIFEST_FILE), &complete)?;
    remove_file_if_exists(&data_dir.join(JOURNAL_FILE))?;
    cleanup_transaction_dirs(data_dir, &complete)?;
    remove_file_if_exists(&data_dir.join(MANIFEST_FILE))?;
    sync_dir(data_dir)
}

fn ensure_proxy_consumers_or_abort(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    if let Err(error) = ensure_proxy_ref_has_no_durable_non_proxy_consumers(data_dir, manifest) {
        abort_proxy_exact_transaction(data_dir, manifest)?;
        return Err(error);
    }
    Ok(())
}

fn ensure_notification_ref_exclusive(
    data_dir: &Path,
    reference: &str,
    channel: &str,
) -> ConfigStoreResult<()> {
    for name in [PROVIDERS_FILE, MCP_FILE, CONFIG_FILE] {
        let bytes = read_target_or_empty(&data_dir.join(name))?;
        if notification_document_has_other_consumer(
            &bytes,
            reference,
            channel,
            name == CONFIG_FILE,
        )? {
            return Err(ConfigStoreError::Validation(
                "notification credential reference is shared by another consumer".to_string(),
            ));
        }
    }
    for descriptor in crate::SECTION_DESCRIPTORS.iter().filter(|descriptor| {
        descriptor.id != crate::SectionId::Credentials
            && !matches!(descriptor.file_name, PROVIDERS_FILE | MCP_FILE)
    }) {
        let bytes = read_target_or_empty(&data_dir.join(descriptor.file_name))?;
        if bytes.is_empty() {
            continue;
        }
        if notification_document_has_other_consumer(
            &bytes,
            reference,
            channel,
            descriptor.file_name == NOTIFICATIONS_FILE,
        )? {
            return Err(ConfigStoreError::Validation(
                "notification credential reference is shared by another consumer".to_string(),
            ));
        }
    }
    Ok(())
}

fn ensure_notification_consumers_or_abort(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    if manifest.exact_scope != Some(ExactTransactionScope::Notifications) {
        return Ok(());
    }
    let credential_file = manifest
        .files
        .iter()
        .find(|file| file.name == CREDENTIALS_FILE)
        .ok_or_else(|| {
            ConfigStoreError::Validation(
                "committed credential transaction is incomplete".to_string(),
            )
        })?;
    let authority = exact_authority_file(manifest).ok_or_else(|| {
        ConfigStoreError::Validation("notification transaction authority is missing".to_string())
    })?;
    let (_, staged_authority) = initial_exact_transaction_member(data_dir, manifest, authority)?;
    let staged_config = exact_authority_as_legacy_bytes(
        ExactTransactionScope::Notifications,
        authority,
        &staged_authority,
    )?;
    let mut refs = notification_refs_from_document(&staged_config)?;
    let current_authority = read_target_or_empty(&data_dir.join(authority))?;
    let current_config = exact_authority_as_legacy_bytes(
        ExactTransactionScope::Notifications,
        authority,
        &current_authority,
    )?;
    for (channel, reference) in notification_refs_from_document(&current_config)? {
        refs.entry(channel).or_insert(reference);
    }
    for reference in &credential_file.touched_credential_refs {
        let channel = refs
            .iter()
            .find_map(|(channel, candidate)| {
                (candidate.as_str() == reference).then_some(channel.as_str())
            })
            .or_else(|| {
                (reference == "notification.ntfy.token")
                    .then_some("ntfy")
                    .or_else(|| (reference == "notification.bark.device_key").then_some("bark"))
            })
            .ok_or_else(|| {
                ConfigStoreError::Validation(
                    "committed notification transaction reference is invalid".to_string(),
                )
            })?;
        if let Err(error) = ensure_notification_ref_exclusive(data_dir, reference, channel) {
            abort_notification_exact_transaction(data_dir, manifest)?;
            return Err(error);
        }
    }
    Ok(())
}

fn ensure_cluster_consumers_or_abort(
    data_dir: &Path,
    manifest: &MigrationManifest,
    install_tracker: Option<&ClusterInstallTracker>,
) -> ConfigStoreResult<()> {
    if manifest.exact_scope != Some(ExactTransactionScope::ClusterFabric) {
        return Ok(());
    }
    let credential_file = manifest
        .files
        .iter()
        .find(|file| file.name == CREDENTIALS_FILE)
        .ok_or_else(|| {
            ConfigStoreError::Validation(
                "committed credential transaction is incomplete".to_string(),
            )
        })?;
    let refs = credential_file
        .touched_credential_refs
        .iter()
        .chain(credential_file.required_credential_refs.iter())
        .map(|reference| CredentialRef::parse(reference.clone()))
        .collect::<ConfigStoreResult<BTreeSet<_>>>()?;
    if let Err(error) = ensure_cluster_refs_are_safe(data_dir, &refs, &[]) {
        abort_cluster_exact_transaction(data_dir, manifest, install_tracker)?;
        return Err(error);
    }
    Ok(())
}

fn ensure_connect_consumers_or_abort(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    if manifest.exact_scope != Some(ExactTransactionScope::Connect) {
        return Ok(());
    }
    let credential_file = manifest
        .files
        .iter()
        .find(|file| file.name == CREDENTIALS_FILE)
        .ok_or_else(|| {
            ConfigStoreError::Validation(
                "committed connect credential transaction is incomplete".to_string(),
            )
        })?;
    let authority = exact_authority_file(manifest).ok_or_else(|| {
        ConfigStoreError::Validation("connect transaction authority is missing".to_string())
    })?;
    let (_, staged_authority) = initial_exact_transaction_member(data_dir, manifest, authority)?;
    let original_authority =
        read_encrypted_migration_backup(data_dir, &manifest.transaction_id, authority)?;
    let staged = exact_authority_as_legacy_bytes(
        ExactTransactionScope::Connect,
        authority,
        &staged_authority,
    )?;
    let original = exact_authority_as_legacy_bytes(
        ExactTransactionScope::Connect,
        authority,
        &original_authority,
    )?;
    let mut owners = connect_refs_from_document(&original)?;
    for (id, (token_ref, app_secret_ref)) in connect_refs_from_document(&staged)? {
        let owner = owners.entry(id).or_default();
        if token_ref.is_some() {
            owner.0 = token_ref;
        }
        if app_secret_ref.is_some() {
            owner.1 = app_secret_ref;
        }
    }
    for reference in &credential_file.touched_credential_refs {
        let owner = owners.iter().find_map(|(id, (token_ref, app_secret_ref))| {
            token_ref
                .as_ref()
                .is_some_and(|candidate| candidate.as_str() == reference)
                .then_some((id.as_str(), "token"))
                .or_else(|| {
                    app_secret_ref
                        .as_ref()
                        .is_some_and(|candidate| candidate.as_str() == reference)
                        .then_some((id.as_str(), "app_secret"))
                })
        });
        let Some((platform_id, field)) = owner else {
            abort_connect_exact_transaction(data_dir, manifest)?;
            return Err(ConfigStoreError::Validation(
                "committed connect credential reference is invalid".to_string(),
            ));
        };
        if let Err(error) = ensure_connect_ref_exclusive(data_dir, reference, platform_id, field) {
            abort_connect_exact_transaction(data_dir, manifest)?;
            return Err(error);
        }
    }
    Ok(())
}

fn ensure_access_consumers_or_abort(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    if manifest.exact_scope != Some(ExactTransactionScope::AccessControl) {
        return Ok(());
    }
    let credential_file = manifest
        .files
        .iter()
        .find(|file| file.name == CREDENTIALS_FILE)
        .ok_or_else(|| {
            ConfigStoreError::Validation(
                "committed access-control credential transaction is incomplete".to_string(),
            )
        })?;
    let authority = exact_authority_file(manifest).ok_or_else(|| {
        ConfigStoreError::Validation("access-control transaction authority is missing".to_string())
    })?;
    let (_, staged_authority) = initial_exact_transaction_member(data_dir, manifest, authority)?;
    let original_authority =
        read_encrypted_migration_backup(data_dir, &manifest.transaction_id, authority)?;
    let staged = exact_authority_as_legacy_bytes(
        ExactTransactionScope::AccessControl,
        authority,
        &staged_authority,
    )?;
    let original = exact_authority_as_legacy_bytes(
        ExactTransactionScope::AccessControl,
        authority,
        &original_authority,
    )?;
    let staged_refs = access_refs_from_document(&staged)?;
    let original_refs = access_refs_from_document(&original)?;
    for raw in &credential_file.touched_credential_refs {
        let owner = if staged_refs
            .password
            .as_ref()
            .is_some_and(|r| r.as_str() == raw)
            || original_refs
                .password
                .as_ref()
                .is_some_and(|r| r.as_str() == raw)
        {
            None
        } else {
            staged_refs
                .devices
                .iter()
                .chain(original_refs.devices.iter())
                .find_map(|(id, reference)| (reference.as_str() == raw).then_some(id.as_str()))
        };
        if owner.is_none()
            && staged_refs
                .password
                .as_ref()
                .is_none_or(|r| r.as_str() != raw)
            && original_refs
                .password
                .as_ref()
                .is_none_or(|r| r.as_str() != raw)
        {
            abort_access_control_exact_transaction(data_dir, manifest)?;
            return Err(ConfigStoreError::Validation(
                "committed access-control credential reference is invalid".to_string(),
            ));
        }
        if let Err(error) = ensure_access_ref_exclusive(data_dir, raw, owner) {
            abort_access_control_exact_transaction(data_dir, manifest)?;
            return Err(error);
        }
    }
    Ok(())
}

fn section_split_completion_members(
    manifest: &MigrationManifest,
) -> ConfigStoreResult<BTreeMap<String, crate::section_facade::SectionLayoutCompletionMember>> {
    let mut members = BTreeMap::new();
    for file in manifest.files.iter().filter(|file| {
        file.name != crate::SECTION_LAYOUT_FILE
            && !matches!(
                file.install_mode,
                InstallMode::LegacyCleanup | InstallMode::SourceGuard
            )
    }) {
        let revision = file.migration_generation.ok_or_else(|| {
            ConfigStoreError::Validation(
                "section layout completion member has no revision".to_string(),
            )
        })?;
        members.insert(
            file.name.clone(),
            crate::section_facade::SectionLayoutCompletionMember {
                revision,
                sha256: file.sha256.clone(),
            },
        );
    }
    if members.len() != crate::SECTION_DESCRIPTORS.len()
        || !crate::SECTION_DESCRIPTORS
            .iter()
            .all(|descriptor| members.contains_key(descriptor.file_name))
    {
        return Err(ConfigStoreError::Validation(
            "section layout completion transcript is incomplete".to_string(),
        ));
    }
    Ok(members)
}

fn section_split_manifest_matches_completion(
    manifest: &MigrationManifest,
    marker_sha256: &str,
    completion: &crate::section_facade::SectionLayoutCompletion,
) -> ConfigStoreResult<bool> {
    if !is_section_layout_scope(manifest.migration_scope) {
        return Ok(false);
    }
    let Some(marker) = manifest
        .files
        .iter()
        .find(|file| file.name == crate::SECTION_LAYOUT_FILE)
    else {
        return Ok(false);
    };
    Ok(manifest.transaction_id == completion.transaction_id
        && marker.sha256 == marker_sha256
        && section_split_completion_members(manifest)? == completion.members)
}

fn persist_section_layout_completion_ledger(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    if manifest.state != MigrationState::Complete
        || !is_section_layout_scope(manifest.migration_scope)
    {
        return Err(ConfigStoreError::Validation(
            "section layout completion ledger requires a complete split transaction".to_string(),
        ));
    }
    let marker = read_target_or_empty(&data_dir.join(crate::SECTION_LAYOUT_FILE))?;
    let marker_sha256 = sha256(&marker);
    let completion = crate::section_facade::validate_completed_section_layout_marker(&marker)?;
    if !section_split_manifest_matches_completion(manifest, &marker_sha256, &completion)? {
        return Err(ConfigStoreError::Validation(
            "section layout completion ledger does not match its transaction".to_string(),
        ));
    }
    let ledger = SectionLayoutCompletionLedger {
        version: SECTION_LAYOUT_COMPLETION_LEDGER_VERSION,
        state: MigrationState::Complete,
        facade_compound: manifest.migration_scope == Some(MigrationScope::FacadeCompound),
        marker_sha256,
        completion,
        legacy_cleanup: manifest
            .files
            .iter()
            .filter(|file| {
                file.install_mode == InstallMode::LegacyCleanup
                    && is_durable_legacy_authority(&file.name)
            })
            .map(|file| (file.name.clone(), file.sha256.clone()))
            .collect(),
        legacy_source_guards: manifest
            .files
            .iter()
            .filter(|file| {
                file.install_mode == InstallMode::SourceGuard
                    && is_durable_legacy_authority(&file.name)
            })
            .map(|file| {
                let base = match file.original_present {
                    Some(false) => FacadeSourceBase::Missing,
                    Some(true) => FacadeSourceBase::Regular {
                        sha256: file
                            .original_sha256
                            .clone()
                            .expect("validated guard has a content hash"),
                    },
                    None => unreachable!("compound guards are presence-aware"),
                };
                (file.name.clone(), base)
            })
            .collect(),
    };
    let bytes = serde_json::to_vec_pretty(&ledger)?;
    if read_optional_migration_file(&data_dir.join(SECTION_LAYOUT_COMPLETION_FILE))?
        .is_some_and(|current| current == bytes)
    {
        return Ok(());
    }
    AtomicFileStore::new(data_dir.join(SECTION_LAYOUT_COMPLETION_FILE))
        .write_bytes_without_backup(&bytes)
}

pub(crate) fn section_layout_completion_evidence_matches(
    data_dir: &Path,
    marker: &[u8],
    completion: &crate::section_facade::SectionLayoutCompletion,
) -> ConfigStoreResult<bool> {
    let marker_sha256 = sha256(marker);
    let manifest_scope = if let Some(manifest_bytes) =
        read_optional_migration_file(&data_dir.join(MANIFEST_FILE))?
    {
        let manifest: MigrationManifest = match serde_json::from_slice(&manifest_bytes) {
            Ok(manifest) => manifest,
            Err(_) => return Ok(false),
        };
        if validate_manifest(&manifest).is_err() || manifest.state != MigrationState::Complete {
            return Ok(false);
        }
        if is_section_layout_scope(manifest.migration_scope)
            && !section_split_manifest_matches_completion(&manifest, &marker_sha256, completion)?
        {
            return Ok(false);
        }
        manifest.migration_scope
    } else {
        None
    };

    let Some(bytes) = read_optional_migration_file(&data_dir.join(SECTION_LAYOUT_COMPLETION_FILE))?
    else {
        return Ok(false);
    };
    let ledger: SectionLayoutCompletionLedger = match serde_json::from_slice(&bytes) {
        Ok(ledger) => ledger,
        Err(_) => return Ok(false),
    };
    let durable_evidence_names = ledger
        .legacy_cleanup
        .keys()
        .chain(ledger.legacy_source_guards.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let expected_durable_names = durable_legacy_authority_names();
    let durable_evidence_shape_valid = if ledger.facade_compound {
        durable_evidence_names == expected_durable_names
            && ledger
                .legacy_cleanup
                .keys()
                .all(|name| !ledger.legacy_source_guards.contains_key(name))
            && manifest_scope.is_none_or(|scope| scope == MigrationScope::FacadeCompound)
    } else {
        durable_evidence_names.is_empty()
            && manifest_scope.is_none_or(|scope| scope == MigrationScope::SectionSplit)
    };
    Ok(ledger.version == SECTION_LAYOUT_COMPLETION_LEDGER_VERSION
        && ledger.state == MigrationState::Complete
        && ledger.marker_sha256 == marker_sha256
        && &ledger.completion == completion
        && durable_evidence_shape_valid
        && ledger.legacy_cleanup.iter().all(|(name, expected)| {
            is_sha256_hex(expected)
                && read_target_or_empty(&data_dir.join(name))
                    .map(|bytes| sha256(&bytes) == *expected)
                    .unwrap_or(false)
        })
        && ledger.legacy_source_guards.iter().all(|(name, expected)| {
            let current = read_optional_target(&data_dir.join(name));
            match (expected, current) {
                (FacadeSourceBase::Missing, Ok(None)) => true,
                (
                    FacadeSourceBase::Regular {
                        sha256: expected_hash,
                    },
                    Ok(Some(bytes)),
                ) => is_sha256_hex(expected_hash) && sha256(&bytes) == *expected_hash,
                _ => false,
            }
        }))
}

fn capture_durable_section_split_credentials(
    data_dir: &Path,
    manifest: &mut MigrationManifest,
    file_index: usize,
) -> ConfigStoreResult<bool> {
    let current = read_target_or_empty(&data_dir.join(CREDENTIALS_FILE))?;
    let revision = CredentialStore::validate_section_document_revision(&current)?;
    let current_hash = sha256(&current);
    let file = manifest.files[file_index].clone();
    if let Some(attested_revision) = file.migration_generation {
        if revision < attested_revision {
            return Err(ConfigStoreError::Validation(
                "credential section revision moved backwards during migration".to_string(),
            ));
        }
        if revision == attested_revision && current_hash != file.sha256 {
            return Err(ConfigStoreError::Validation(
                "credential section changed without advancing its revision".to_string(),
            ));
        }
        if revision == attested_revision {
            return Ok(false);
        }
    }

    let staged_name = format!("{CREDENTIALS_FILE}.rebase.{}", Uuid::new_v4());
    let stage_dir = validated_stage_dir(data_dir, &manifest.stage_dir)?;
    AtomicFileStore::new(stage_dir.join(&staged_name))
        .sensitive(true)
        .write_bytes_without_backup(&current)?;
    sync_dir(&stage_dir)?;
    let file = &mut manifest.files[file_index];
    file.staged_name = staged_name;
    file.sha256 = current_hash;
    file.migration_generation = Some(revision);
    write_manifest(data_dir.join(MANIFEST_FILE), manifest)?;
    Ok(true)
}

fn section_split_targets_match_manifest(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<bool> {
    for file in manifest
        .files
        .iter()
        .filter(|file| file.name != crate::SECTION_LAYOUT_FILE)
    {
        if file.install_mode == InstallMode::SourceGuard {
            if !source_guard_matches(&data_dir.join(&file.name), file)? {
                return Ok(false);
            }
            continue;
        }
        let current = read_target_or_empty(&data_dir.join(&file.name))?;
        if file.install_mode == InstallMode::LegacyCleanup {
            if sha256(&current) != file.sha256 {
                return Ok(false);
            }
            continue;
        }
        let Some(expected_revision) = file.migration_generation else {
            return Ok(false);
        };
        let revision = if file.name == CREDENTIALS_FILE {
            CredentialStore::validate_section_document_revision(&current)?
        } else {
            crate::section_facade::validate_section_envelope(&file.name, &current, 0)?
        };
        if revision != expected_revision || sha256(&current) != file.sha256 {
            return Ok(false);
        }
    }
    Ok(true)
}

fn finalize_section_split_completion(
    data_dir: &Path,
    manifest: &mut MigrationManifest,
) -> ConfigStoreResult<()> {
    if !is_section_layout_scope(manifest.migration_scope)
        || manifest.state != MigrationState::Pending
    {
        return Err(ConfigStoreError::Validation(
            "section layout completion requires a pending split transaction".to_string(),
        ));
    }

    for _ in 0..8 {
        let member_names = manifest
            .files
            .iter()
            .filter(|file| file.name != crate::SECTION_LAYOUT_FILE)
            .map(|file| file.name.clone())
            .collect::<Vec<_>>();
        for name in member_names {
            let file_index = manifest
                .files
                .iter()
                .position(|file| file.name == name)
                .ok_or_else(|| {
                    ConfigStoreError::Validation(
                        "section layout completion transcript is incomplete".to_string(),
                    )
                })?;
            if name == CREDENTIALS_FILE {
                if let Err(error) =
                    capture_durable_section_split_credentials(data_dir, manifest, file_index)
                {
                    abort_section_split_transaction(data_dir, manifest)?;
                    return Err(error);
                }
                continue;
            }

            if manifest.files[file_index].install_mode == InstallMode::SourceGuard {
                if !source_guard_matches(&data_dir.join(&name), &manifest.files[file_index])? {
                    let error = ConfigStoreError::Validation(
                        "facade source changed during committed compound migration".to_string(),
                    );
                    abort_section_split_transaction(data_dir, manifest)?;
                    return Err(error);
                }
                continue;
            }

            if manifest.files[file_index].install_mode == InstallMode::LegacyCleanup {
                let current = read_target_or_empty(&data_dir.join(&name))?;
                if sha256(&current) != manifest.files[file_index].sha256 {
                    let error = ConfigStoreError::Validation(
                        "legacy source changed during committed facade migration".to_string(),
                    );
                    abort_section_split_transaction(data_dir, manifest)?;
                    return Err(error);
                }
                continue;
            }

            let current = read_target_or_empty(&data_dir.join(&name))?;
            if sha256(&current) != manifest.files[file_index].sha256 {
                rebase_changed_section(
                    data_dir,
                    manifest,
                    file_index,
                    #[cfg(test)]
                    None,
                )?;
            }
        }

        let stable = match section_split_targets_match_manifest(data_dir, manifest) {
            Ok(stable) => stable,
            Err(error) => {
                abort_section_split_transaction(data_dir, manifest)?;
                return Err(error);
            }
        };
        if !stable {
            continue;
        }

        let members = section_split_completion_members(manifest)?;
        let completed = crate::section_facade::build_completed_section_layout_marker(
            &manifest.transaction_id,
            members,
        )?;
        let marker_index = manifest
            .files
            .iter()
            .position(|file| file.name == crate::SECTION_LAYOUT_FILE)
            .ok_or_else(|| {
                ConfigStoreError::Validation(
                    "section layout completion marker is missing".to_string(),
                )
            })?;
        let completed_hash = sha256(&completed);
        if manifest.files[marker_index].sha256 != completed_hash {
            let staged_name = format!("{}.rebase.{}", crate::SECTION_LAYOUT_FILE, Uuid::new_v4());
            let stage_dir = validated_stage_dir(data_dir, &manifest.stage_dir)?;
            AtomicFileStore::new(stage_dir.join(&staged_name))
                .write_bytes_without_backup(&completed)?;
            sync_dir(&stage_dir)?;
            let marker = &mut manifest.files[marker_index];
            marker.staged_name = staged_name;
            marker.sha256 = completed_hash;
            write_manifest(data_dir.join(MANIFEST_FILE), manifest)?;
        }
        return Ok(());
    }

    let error = ConfigStoreError::Validation(
        "configuration sections changed repeatedly during layout completion".to_string(),
    );
    abort_section_split_transaction(data_dir, manifest)?;
    Err(error)
}

fn verify_section_split_completion(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    if !is_section_layout_scope(manifest.migration_scope) {
        return Ok(());
    }
    let marker = manifest
        .files
        .iter()
        .find(|file| file.name == crate::SECTION_LAYOUT_FILE)
        .ok_or_else(|| {
            ConfigStoreError::Validation("section layout completion marker is missing".to_string())
        })?;
    let marker_bytes = read_target_or_empty(&data_dir.join(crate::SECTION_LAYOUT_FILE))?;
    if sha256(&marker_bytes) != marker.sha256 {
        return Err(ConfigStoreError::Validation(
            "section layout completion marker failed integrity validation".to_string(),
        ));
    }
    let completion =
        crate::section_facade::validate_completed_section_layout_marker(&marker_bytes)?;
    if completion.transaction_id != manifest.transaction_id
        || completion.members != section_split_completion_members(manifest)?
        || !crate::section_facade::current_members_satisfy_completion(data_dir, &completion)?
    {
        return Err(ConfigStoreError::Validation(
            "section layout completion attestation does not match its transaction".to_string(),
        ));
    }
    Ok(())
}

fn install_pending(
    data_dir: &Path,
    manifest: &mut MigrationManifest,
    install_tracker: Option<&ClusterInstallTracker>,
    #[cfg(test)] fault: Option<MigrationFault>,
) -> ConfigStoreResult<()> {
    validate_manifest(manifest)?;
    if manifest.state != MigrationState::Pending {
        return Err(ConfigStoreError::Validation(
            "only a pending transaction may install staged members".to_string(),
        ));
    }
    if manifest.migration_scope == Some(MigrationScope::ClusterFabric) {
        pending_cluster_refs_are_exclusive(data_dir)?;
    }
    ensure_proxy_consumers_or_abort(data_dir, manifest)?;
    ensure_notification_consumers_or_abort(data_dir, manifest)?;
    ensure_connect_consumers_or_abort(data_dir, manifest)?;
    ensure_access_consumers_or_abort(data_dir, manifest)?;
    ensure_cluster_consumers_or_abort(data_dir, manifest, install_tracker)?;
    let _stage_dir = validated_stage_dir(data_dir, &manifest.stage_dir)?;
    let names = manifest
        .files
        .iter()
        .map(|file| file.name.clone())
        .collect::<Vec<_>>();
    for name in names {
        let Some(file_index) = manifest.files.iter().position(|file| file.name == name) else {
            continue;
        };
        if is_section_layout_scope(manifest.migration_scope) && name == crate::SECTION_LAYOUT_FILE {
            finalize_section_split_completion(data_dir, manifest)?;
        }
        let file = manifest.files[file_index].clone();
        let target = data_dir.join(&file.name);
        if file.install_mode == InstallMode::SourceGuard {
            if !source_guard_matches(&target, &file)? {
                let error = ConfigStoreError::Validation(
                    "facade source changed during committed compound migration".to_string(),
                );
                abort_section_split_transaction(data_dir, manifest)?;
                return Err(error);
            }
            continue;
        }
        if file.install_mode == InstallMode::Exact && file.name == CREDENTIALS_FILE {
            install_exact_credential_member(
                data_dir,
                manifest,
                file_index,
                #[cfg(test)]
                fault,
            )?;
            #[cfg(test)]
            if fault == Some(MigrationFault::AfterCredentials) {
                return Err(injected_fault());
            }
            #[cfg(any(test, feature = "test-utils"))]
            if manifest.exact_scope == Some(ExactTransactionScope::ClusterFabric)
                && take_cluster_exact_commit_test_fault(
                    data_dir,
                    ClusterExactCommitTestFault::AfterCredentialInstall,
                )
            {
                return Err(ConfigStoreError::Validation(
                    "injected cluster exact transaction fault after credential install".to_string(),
                ));
            }
            continue;
        }
        let staged = read_managed_file(data_dir, &manifest.stage_dir, &file.staged_name)?;
        if sha256(&staged) != file.sha256 {
            return Err(ConfigStoreError::Validation(
                "staged migration document failed integrity validation".to_string(),
            ));
        }
        if file.install_mode == InstallMode::Exact {
            ensure_exact_transaction_credentials(data_dir, manifest)?;
            let current = read_target_or_empty(&target)?;
            if sha256(&current) != file.sha256 {
                let expected = file.original_sha256.as_deref().ok_or_else(|| {
                    ConfigStoreError::Validation(
                        "transaction member base hash is missing".to_string(),
                    )
                })?;
                if !AtomicFileStore::new(&target)
                    .sensitive(file.sensitive)
                    .write_bytes_if_hash_with_backup(expected, &staged)?
                {
                    if exact_authority_file(manifest) == Some(file.name.as_str())
                        && install_rebased_exact_config_member(
                            data_dir,
                            manifest,
                            file_index,
                            install_tracker,
                            #[cfg(test)]
                            fault,
                        )?
                    {
                        continue;
                    }
                    return Err(ConfigStoreError::Validation(format!(
                        "{} changed during committed transaction",
                        file.name
                    )));
                }
            }
        } else if file.name == CREDENTIALS_FILE {
            let store = CredentialStore::open(data_dir);
            store.commit_migration(&staged)?;
            if is_section_layout_scope(manifest.migration_scope) {
                store.ensure_initialized_for_section_layout()?;
            }
        } else {
            let current_hash = match read_file_reject_symlink(&target) {
                Ok(bytes) => Some(sha256(&bytes)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(error.into()),
            };
            if current_hash.as_deref() != Some(file.sha256.as_str())
                && !install_migration_member_if_base_matches(&target, &file, &staged)?
            {
                if !rebase_changed_section(
                    data_dir,
                    manifest,
                    file_index,
                    #[cfg(test)]
                    fault,
                )? {
                    continue;
                }
                let rebased = manifest.files[file_index].clone();
                let rebased_bytes =
                    read_managed_file(data_dir, &manifest.stage_dir, &rebased.staged_name)?;
                if !install_migration_member_if_base_matches(&target, &rebased, &rebased_bytes)? {
                    return Err(ConfigStoreError::Validation(
                        "configuration section changed repeatedly during migration".to_string(),
                    ));
                }
            }
        }
        #[cfg(test)]
        if matches!(
            (fault, file.name.as_str()),
            (Some(MigrationFault::AfterCredentials), CREDENTIALS_FILE)
                | (Some(MigrationFault::AfterProviders), PROVIDERS_FILE)
                | (Some(MigrationFault::AfterMcp), MCP_FILE)
                | (Some(MigrationFault::AfterBroker), BROKER_FILE)
                | (Some(MigrationFault::AfterConfig), CONFIG_FILE)
        ) {
            return Err(injected_fault());
        }
        #[cfg(test)]
        if fault == Some(MigrationFault::AfterAuthoritativeSection)
            && exact_authority_file(manifest) == Some(file.name.as_str())
            && file.name != CONFIG_FILE
        {
            return Err(injected_fault());
        }
        #[cfg(test)]
        if fault == Some(MigrationFault::AfterSectionLayout)
            && is_section_layout_scope(manifest.migration_scope)
            && file.name == crate::SECTION_LAYOUT_FILE
        {
            return Err(injected_fault());
        }
    }
    Ok(())
}

fn source_guard_matches(target: &Path, file: &StagedFile) -> ConfigStoreResult<bool> {
    let current = read_optional_target(target)?;
    Ok(match (file.original_present, current.as_deref()) {
        (Some(false), None) => true,
        (Some(true), Some(bytes)) => {
            file.original_sha256.as_deref() == Some(sha256(bytes).as_str())
        }
        _ => false,
    })
}

fn install_migration_member_if_base_matches(
    target: &Path,
    file: &StagedFile,
    staged: &[u8],
) -> ConfigStoreResult<bool> {
    let store = AtomicFileStore::new(target).sensitive(file.sensitive);
    match file.original_present {
        Some(false) => store.write_bytes_if_state(None, staged),
        Some(true) => {
            let expected = file.original_sha256.as_deref().ok_or_else(|| {
                ConfigStoreError::Validation("migration section base hash is missing".to_string())
            })?;
            store.write_bytes_if_state(Some(expected), staged)
        }
        None => {
            let expected = file.original_sha256.as_deref().ok_or_else(|| {
                ConfigStoreError::Validation("migration section base hash is missing".to_string())
            })?;
            store.write_bytes_if_hash(expected, staged)
        }
    }
}

fn parse_exact_section_document(
    name: &str,
    bytes: &[u8],
) -> ConfigStoreResult<(Value, u64, Value)> {
    let revision = crate::section_facade::validate_section_envelope(name, bytes, 0)?;
    let envelope: Value = serde_json::from_slice(bytes)?;
    let data = envelope.get("data").cloned().ok_or_else(|| {
        ConfigStoreError::Validation("authoritative section envelope has no data".to_string())
    })?;
    Ok((envelope, revision, data))
}

fn merge_json_slot_three_way(
    current: Option<&Value>,
    original: Option<&Value>,
    staged: Option<&Value>,
) -> Result<Option<Value>, ()> {
    if current == staged {
        return Ok(current.cloned());
    }
    if current == original {
        return Ok(staged.cloned());
    }
    if staged == original {
        return Ok(current.cloned());
    }
    let (Some(Value::Object(current)), Some(Value::Object(staged))) = (current, staged) else {
        return Err(());
    };
    let empty = Map::new();
    let original = match original {
        Some(Value::Object(original)) => original,
        None => &empty,
        Some(_) => return Err(()),
    };
    let keys = current
        .keys()
        .chain(original.keys())
        .chain(staged.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut merged = Map::new();
    for key in keys {
        if let Some(value) =
            merge_json_slot_three_way(current.get(&key), original.get(&key), staged.get(&key))?
        {
            merged.insert(key, value);
        }
    }
    Ok(Some(Value::Object(merged)))
}

fn named_value_map(value: &Value, identity: &str) -> Result<BTreeMap<String, Value>, ()> {
    let Value::Array(entries) = value else {
        return Err(());
    };
    let mut mapped = BTreeMap::new();
    for entry in entries {
        let Some(name) = entry.get(identity).and_then(Value::as_str) else {
            return Err(());
        };
        if mapped.insert(name.to_string(), entry.clone()).is_some() {
            return Err(());
        }
    }
    Ok(mapped)
}

fn merge_named_array_slot_three_way(
    current: Option<&Value>,
    original: Option<&Value>,
    staged: Option<&Value>,
    identity: &str,
) -> Result<Option<Value>, ()> {
    if current == staged {
        return Ok(current.cloned());
    }
    if current == original {
        return Ok(staged.cloned());
    }
    if staged == original {
        return Ok(current.cloned());
    }
    let (Some(current), Some(original), Some(staged)) = (current, original, staged) else {
        return Err(());
    };
    let current_map = named_value_map(current, identity)?;
    let original_map = named_value_map(original, identity)?;
    let staged_map = named_value_map(staged, identity)?;
    let names = current_map
        .keys()
        .chain(original_map.keys())
        .chain(staged_map.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut merged_map = BTreeMap::new();
    for name in names {
        if let Some(value) = merge_json_slot_three_way(
            current_map.get(&name),
            original_map.get(&name),
            staged_map.get(&name),
        )? {
            merged_map.insert(name, value);
        }
    }
    let mut ordered = Vec::new();
    let mut emitted = BTreeSet::new();
    for source in [current, staged] {
        for entry in source.as_array().ok_or(())? {
            let name = entry.get(identity).and_then(Value::as_str).ok_or(())?;
            if emitted.insert(name.to_string()) {
                if let Some(value) = merged_map.get(name) {
                    ordered.push(value.clone());
                }
            }
        }
    }
    Ok(Some(Value::Array(ordered)))
}

fn merge_active_env_data(
    current: &Value,
    original: &Value,
    staged: &Value,
    touched_names: &[String],
) -> Result<Value, ()> {
    let current_map = named_value_map(current, "name")?;
    let original_map = named_value_map(original, "name")?;
    let staged_map = named_value_map(staged, "name")?;
    let touched = touched_names.iter().cloned().collect::<BTreeSet<_>>();
    if touched.is_empty() {
        return Err(());
    }
    let mut merged = current_map.clone();
    for name in &touched {
        match merge_json_slot_three_way(
            current_map.get(name),
            original_map.get(name),
            staged_map.get(name),
        )? {
            Some(value) => {
                merged.insert(name.clone(), value);
            }
            None => {
                merged.remove(name);
            }
        }
    }
    let mut ordered = Vec::new();
    let mut emitted = BTreeSet::new();
    for source in [current, staged] {
        for entry in source.as_array().ok_or(())? {
            let name = entry.get("name").and_then(Value::as_str).ok_or(())?;
            if emitted.insert(name.to_string()) {
                if let Some(value) = merged.get(name) {
                    ordered.push(value.clone());
                }
            }
        }
    }
    Ok(Value::Array(ordered))
}

fn merge_active_cluster_data(
    current: &Value,
    original: &Value,
    staged: &Value,
) -> Result<Value, ()> {
    let (Value::Object(current), Value::Object(original), Value::Object(staged)) =
        (current, original, staged)
    else {
        return Err(());
    };
    let keys = current
        .keys()
        .chain(original.keys())
        .chain(staged.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut merged = Map::new();
    for key in keys {
        let value = match key.as_str() {
            "nodes" => merge_named_array_slot_three_way(
                current.get(&key),
                original.get(&key),
                staged.get(&key),
                "id",
            )?,
            "clusters" => merge_named_array_slot_three_way(
                current.get(&key),
                original.get(&key),
                staged.get(&key),
                "name",
            )?,
            _ => {
                merge_json_slot_three_way(current.get(&key), original.get(&key), staged.get(&key))?
            }
        };
        if let Some(value) = value {
            merged.insert(key, value);
        }
    }
    Ok(Value::Object(merged))
}

fn merge_active_section_data(
    scope: ExactTransactionScope,
    current: &Value,
    original: &Value,
    staged: &Value,
    touched_env_names: &[String],
) -> ConfigStoreResult<Value> {
    let merged = match scope {
        ExactTransactionScope::EnvVars => {
            merge_active_env_data(current, original, staged, touched_env_names)
        }
        ExactTransactionScope::ClusterFabric => {
            merge_active_cluster_data(current, original, staged)
        }
        ExactTransactionScope::Providers
        | ExactTransactionScope::Mcp
        | ExactTransactionScope::ProxyAuth
        | ExactTransactionScope::Notifications
        | ExactTransactionScope::Connect
        | ExactTransactionScope::AccessControl => {
            merge_json_slot_three_way(Some(current), Some(original), Some(staged))
                .and_then(|value| value.ok_or(()))
        }
    };
    merged.map_err(|_| {
        ConfigStoreError::Validation(
            "authoritative section changed in the same domain during committed transaction"
                .to_string(),
        )
    })
}

fn install_rebased_active_section_member(
    data_dir: &Path,
    manifest: &mut MigrationManifest,
    file_index: usize,
    install_tracker: Option<&ClusterInstallTracker>,
) -> ConfigStoreResult<bool> {
    let scope = manifest.exact_scope.ok_or_else(|| {
        ConfigStoreError::Validation("exact transaction scope is missing".to_string())
    })?;
    let name = authoritative_section_file(scope);
    let file = manifest.files[file_index].clone();
    if file.name != name {
        return Ok(false);
    }
    let (initial_file, initial_staged) =
        initial_exact_transaction_member(data_dir, manifest, name)?;
    let original = read_encrypted_migration_backup(data_dir, &manifest.transaction_id, name)?;
    let (_, original_revision, original_data) = parse_exact_section_document(name, &original)?;
    if initial_file.expected_revision != Some(original_revision)
        || initial_file.original_sha256.as_deref() != Some(sha256(&original).as_str())
    {
        return Err(ConfigStoreError::Validation(
            "authoritative section transaction base is invalid".to_string(),
        ));
    }
    let (_, _, staged_data) = parse_exact_section_document(name, &initial_staged)?;
    let target = data_dir.join(name);
    let current = read_target_or_empty(&target)?;
    let current_hash = sha256(&current);
    let (mut current_envelope, current_revision, current_data) =
        parse_exact_section_document(name, &current)?;
    if current_revision < original_revision {
        abort_exact_transaction(data_dir, manifest, install_tracker)?;
        return Err(ConfigStoreError::Validation(
            "authoritative section revision moved backwards".to_string(),
        ));
    }
    if current_revision == original_revision && current_hash != sha256(&original) {
        abort_exact_transaction(data_dir, manifest, install_tracker)?;
        return Err(ConfigStoreError::Validation(
            "authoritative section reused a revision with different content".to_string(),
        ));
    }
    let merged = match merge_active_section_data(
        scope,
        &current_data,
        &original_data,
        &staged_data,
        &initial_file.touched_env_names,
    ) {
        Ok(merged) => merged,
        Err(error) => {
            abort_exact_transaction(data_dir, manifest, install_tracker)?;
            return Err(error);
        }
    };
    let (rebased, rebased_revision) = if merged == current_data {
        (current.clone(), current_revision)
    } else {
        let revision = current_revision.checked_add(1).ok_or_else(|| {
            ConfigStoreError::Validation("configuration revision counter exhausted".to_string())
        })?;
        let envelope = current_envelope.as_object_mut().ok_or_else(|| {
            ConfigStoreError::Validation("authoritative section envelope is invalid".to_string())
        })?;
        envelope.insert("revision".to_string(), Value::from(revision));
        envelope.insert("data".to_string(), merged);
        let bytes = serde_json::to_vec_pretty(&current_envelope)?;
        crate::section_facade::validate_section_envelope(name, &bytes, revision)?;
        (bytes, revision)
    };
    let stage_dir = validated_stage_dir(data_dir, &manifest.stage_dir)?;
    let staged_name = format!("{name}.rebase.{}", Uuid::new_v4());
    AtomicFileStore::new(stage_dir.join(&staged_name)).write_bytes_without_backup(&rebased)?;
    sync_dir(&stage_dir)?;
    let file = &mut manifest.files[file_index];
    file.staged_name = staged_name;
    file.sha256 = sha256(&rebased);
    file.original_sha256 = Some(current_hash.clone());
    file.original_present = Some(true);
    file.expected_revision = Some(current_revision);
    write_manifest(data_dir.join(MANIFEST_FILE), manifest)?;
    if sha256(&rebased) != current_hash
        && !AtomicFileStore::new(&target)
            .write_bytes_if_hash_with_backup(&current_hash, &rebased)?
    {
        return Err(ConfigStoreError::Validation(
            "authoritative section changed repeatedly during committed transaction".to_string(),
        ));
    }
    debug_assert!(rebased_revision >= current_revision);
    Ok(true)
}

fn install_rebased_exact_config_member(
    data_dir: &Path,
    manifest: &mut MigrationManifest,
    file_index: usize,
    install_tracker: Option<&ClusterInstallTracker>,
    #[cfg(test)] fault: Option<MigrationFault>,
) -> ConfigStoreResult<bool> {
    if manifest.files[file_index].name != CONFIG_FILE {
        return install_rebased_active_section_member(
            data_dir,
            manifest,
            file_index,
            install_tracker,
        );
    }
    if manifest.exact_scope == Some(ExactTransactionScope::EnvVars) {
        return install_rebased_env_config_member(data_dir, manifest, file_index);
    }
    if manifest.exact_scope == Some(ExactTransactionScope::Notifications) {
        return install_rebased_notification_config_member(data_dir, manifest, file_index);
    }
    if manifest.exact_scope == Some(ExactTransactionScope::Connect) {
        return Err(ConfigStoreError::Validation(
            "connect exact transaction has no legacy root authority".to_string(),
        ));
    }
    if manifest.exact_scope == Some(ExactTransactionScope::ClusterFabric) {
        return install_rebased_cluster_config_member(
            data_dir,
            manifest,
            file_index,
            install_tracker,
        );
    }
    install_rebased_proxy_config_member(
        data_dir,
        manifest,
        file_index,
        #[cfg(test)]
        fault,
    )
}

fn install_rebased_cluster_config_member(
    data_dir: &Path,
    manifest: &mut MigrationManifest,
    file_index: usize,
    install_tracker: Option<&ClusterInstallTracker>,
) -> ConfigStoreResult<bool> {
    let stage_dir = validated_stage_dir(data_dir, &manifest.stage_dir)?;
    let file = manifest.files[file_index].clone();
    let staged_bytes = read_managed_file(data_dir, &manifest.stage_dir, &file.staged_name)?;
    let staged = parse_config_root_object(
        &staged_bytes,
        "staged cluster config transaction document is invalid",
    )?;
    let target = data_dir.join(CONFIG_FILE);
    let current = read_target_or_empty(&target)?;
    let current_hash = sha256(&current);
    let mut current_object = parse_config_root_object(
        &current,
        "config.json changed to an invalid document during committed cluster transaction",
    )?;
    let original =
        read_encrypted_migration_backup(data_dir, &manifest.transaction_id, CONFIG_FILE)?;
    let original_object = parse_config_root_object(
        &original,
        "config transaction backup is not a valid document",
    )?;
    if current_object.get("cluster_fabric") != original_object.get("cluster_fabric")
        && current_object.get("cluster_fabric") != staged.get("cluster_fabric")
    {
        abort_cluster_exact_transaction(data_dir, manifest, install_tracker)?;
        return Err(ConfigStoreError::Validation(
            "cluster metadata changed during committed transaction".to_string(),
        ));
    }
    match staged.get("cluster_fabric").cloned() {
        Some(value) => {
            current_object.insert("cluster_fabric".to_string(), value);
        }
        None => {
            current_object.remove("cluster_fabric");
        }
    }
    let rebased = Value::Object(current_object);
    serde_json::from_value::<crate::Config>(rebased.clone()).map_err(|_| {
        ConfigStoreError::Validation(
            "config.json changed to an invalid document during committed cluster transaction"
                .to_string(),
        )
    })?;
    let rebased = serde_json::to_vec_pretty(&rebased)?;
    let staged_name = format!("{CONFIG_FILE}.rebase.{}", Uuid::new_v4());
    AtomicFileStore::new(stage_dir.join(&staged_name)).write_bytes_without_backup(&rebased)?;
    sync_dir(&stage_dir)?;
    let file = &mut manifest.files[file_index];
    file.staged_name = staged_name;
    file.sha256 = sha256(&rebased);
    file.original_sha256 = Some(current_hash.clone());
    file.original_present = Some(true);
    write_manifest(data_dir.join(MANIFEST_FILE), manifest)?;
    if !AtomicFileStore::new(&target).write_bytes_if_hash_with_backup(&current_hash, &rebased)? {
        return Err(ConfigStoreError::Validation(
            "config.json changed repeatedly during committed cluster transaction".to_string(),
        ));
    }
    Ok(true)
}

fn install_rebased_notification_config_member(
    data_dir: &Path,
    manifest: &mut MigrationManifest,
    file_index: usize,
) -> ConfigStoreResult<bool> {
    let stage_dir = validated_stage_dir(data_dir, &manifest.stage_dir)?;
    let file = manifest.files[file_index].clone();
    let staged_bytes = read_managed_file(data_dir, &manifest.stage_dir, &file.staged_name)?;
    let staged = parse_config_root_object(
        &staged_bytes,
        "staged notification config transaction document is invalid",
    )?;
    let target = data_dir.join(CONFIG_FILE);
    let current = read_target_or_empty(&target)?;
    let current_hash = sha256(&current);
    let mut current_object = parse_config_root_object(
        &current,
        "config.json changed to an invalid document during committed notification transaction",
    )?;
    let original =
        read_encrypted_migration_backup(data_dir, &manifest.transaction_id, CONFIG_FILE)?;
    let original_object = parse_config_root_object(
        &original,
        "config transaction backup is not a valid document",
    )?;
    if current_object.get("notifications") != original_object.get("notifications")
        && current_object.get("notifications") != staged.get("notifications")
    {
        abort_notification_exact_transaction(data_dir, manifest)?;
        return Err(ConfigStoreError::Validation(
            "notification metadata changed during committed transaction".to_string(),
        ));
    }
    match staged.get("notifications").cloned() {
        Some(value) => {
            current_object.insert("notifications".to_string(), value);
        }
        None => {
            current_object.remove("notifications");
        }
    }
    let rebased = Value::Object(current_object);
    serde_json::from_value::<crate::Config>(rebased.clone()).map_err(|_| {
        ConfigStoreError::Validation(
            "config.json changed to an invalid document during committed notification transaction"
                .to_string(),
        )
    })?;
    let rebased = serde_json::to_vec_pretty(&rebased)?;
    let staged_name = format!("{CONFIG_FILE}.rebase.{}", Uuid::new_v4());
    AtomicFileStore::new(stage_dir.join(&staged_name)).write_bytes_without_backup(&rebased)?;
    sync_dir(&stage_dir)?;
    let file = &mut manifest.files[file_index];
    file.staged_name = staged_name;
    file.sha256 = sha256(&rebased);
    file.original_sha256 = Some(current_hash.clone());
    file.original_present = Some(true);
    write_manifest(data_dir.join(MANIFEST_FILE), manifest)?;
    if !AtomicFileStore::new(&target).write_bytes_if_hash_with_backup(&current_hash, &rebased)? {
        return Err(ConfigStoreError::Validation(
            "config.json changed repeatedly during committed notification transaction".to_string(),
        ));
    }
    Ok(true)
}

fn install_rebased_env_config_member(
    data_dir: &Path,
    manifest: &mut MigrationManifest,
    file_index: usize,
) -> ConfigStoreResult<bool> {
    let stage_dir = validated_stage_dir(data_dir, &manifest.stage_dir)?;
    let file = manifest.files[file_index].clone();
    let staged_bytes = read_managed_file(data_dir, &manifest.stage_dir, &file.staged_name)?;
    let staged = parse_config_root_object(
        &staged_bytes,
        "staged env config transaction document is invalid",
    )?;
    let target = data_dir.join(CONFIG_FILE);
    let current = read_target_or_empty(&target)?;
    let current_hash = sha256(&current);
    let mut current_object = parse_config_root_object(
        &current,
        "config.json changed to an invalid document during committed env transaction",
    )?;
    let original =
        read_encrypted_migration_backup(data_dir, &manifest.transaction_id, CONFIG_FILE)?;
    let original_object = parse_config_root_object(
        &original,
        "config transaction backup is not a valid document",
    )?;
    let current_entries = env_entry_map(&current_object)?;
    let original_entries = env_entry_map(&original_object)?;
    let staged_entries = env_entry_map(&staged)?;
    let touched_names = file
        .touched_env_names
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if touched_names.is_empty() {
        return Err(ConfigStoreError::Validation(
            "committed env transaction is missing touched names".to_string(),
        ));
    }
    let mut merged_entries = current_entries.clone();
    let required_refs = manifest
        .files
        .iter()
        .find(|file| file.name == CREDENTIALS_FILE)
        .map(|file| {
            file.required_credential_refs
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    for name in &touched_names {
        let current_entry = current_entries.get(name);
        let original_entry = original_entries.get(name);
        let staged_entry = staged_entries.get(name);
        if current_entry == staged_entry {
            continue;
        }
        match staged_entry {
            Some(staged_entry) => {
                match merge_env_entry_three_way(current_entry, original_entry, staged_entry)? {
                    Some(merged) => {
                        merged_entries.insert(name.clone(), merged);
                    }
                    None => {
                        let current_ref = current_entry
                            .and_then(|entry| entry.get("credential_ref"))
                            .and_then(Value::as_str);
                        if current_ref.is_some_and(|reference| required_refs.contains(reference)) {
                            // The external same-name winner still consumes the
                            // credential required by the transaction. Keep both;
                            // rolling the credential back could manufacture a
                            // dangling configured reference.
                            continue;
                        }
                        abort_env_exact_transaction(data_dir, manifest)?;
                        return Err(ConfigStoreError::Validation(
                            "env metadata changed during committed transaction".to_string(),
                        ));
                    }
                }
            }
            None => {
                if env_known_fields(current_entry) == env_known_fields(original_entry) {
                    merged_entries.remove(name);
                } else if current_entry.is_some() {
                    let current_ref = current_entry
                        .and_then(|entry| entry.get("credential_ref"))
                        .and_then(Value::as_str);
                    if !current_ref.is_some_and(|reference| required_refs.contains(reference)) {
                        abort_env_exact_transaction(data_dir, manifest)?;
                        return Err(ConfigStoreError::Validation(
                            "env metadata changed during committed transaction".to_string(),
                        ));
                    }
                }
            }
        }
    }
    let mut ordered = Vec::new();
    let mut emitted = BTreeSet::new();
    if let Some(current_array) = current_object.get("env_vars").and_then(Value::as_array) {
        for entry in current_array {
            let Some(name) = entry.get("name").and_then(Value::as_str) else {
                continue;
            };
            if let Some(merged) = merged_entries.get(name) {
                ordered.push(merged.clone());
                emitted.insert(name.to_string());
            }
        }
    }
    if let Some(staged_array) = staged.get("env_vars").and_then(Value::as_array) {
        for entry in staged_array {
            let Some(name) = entry.get("name").and_then(Value::as_str) else {
                continue;
            };
            if !emitted.contains(name) {
                if let Some(merged) = merged_entries.get(name) {
                    ordered.push(merged.clone());
                    emitted.insert(name.to_string());
                }
            }
        }
    }
    if ordered.is_empty() {
        current_object.remove("env_vars");
    } else {
        current_object.insert("env_vars".to_string(), Value::Array(ordered));
    }
    let rebased = Value::Object(current_object);
    serde_json::from_value::<crate::Config>(rebased.clone()).map_err(|_| {
        ConfigStoreError::Validation(
            "config.json changed to an invalid document during committed env transaction"
                .to_string(),
        )
    })?;
    let rebased = serde_json::to_vec_pretty(&rebased)?;
    let staged_name = format!("{CONFIG_FILE}.rebase.{}", Uuid::new_v4());
    AtomicFileStore::new(stage_dir.join(&staged_name)).write_bytes_without_backup(&rebased)?;
    sync_dir(&stage_dir)?;
    let file = &mut manifest.files[file_index];
    file.staged_name = staged_name;
    file.sha256 = sha256(&rebased);
    file.original_sha256 = Some(current_hash.clone());
    file.original_present = Some(true);
    write_manifest(data_dir.join(MANIFEST_FILE), manifest)?;
    if !AtomicFileStore::new(&target).write_bytes_if_hash_with_backup(&current_hash, &rebased)? {
        return Err(ConfigStoreError::Validation(
            "config.json changed repeatedly during committed env transaction".to_string(),
        ));
    }
    Ok(true)
}

const ENV_ENTRY_KNOWN_KEYS: [&str; 7] = [
    "name",
    "value",
    "secret",
    "value_encrypted",
    "credential_ref",
    "configured",
    "description",
];

fn env_entry_map(object: &Map<String, Value>) -> ConfigStoreResult<BTreeMap<String, Value>> {
    let mut entries = BTreeMap::new();
    let Some(array) = object.get("env_vars").and_then(Value::as_array) else {
        return Ok(entries);
    };
    for entry in array {
        let name = entry
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| ConfigStoreError::Validation("env var name is missing".to_string()))?;
        if entries.insert(name.to_string(), entry.clone()).is_some() {
            return Err(ConfigStoreError::Validation(
                "environment variable names must be unique".to_string(),
            ));
        }
    }
    Ok(entries)
}

fn env_known_fields(entry: Option<&Value>) -> Option<Map<String, Value>> {
    let object = entry?.as_object()?;
    Some(
        ENV_ENTRY_KNOWN_KEYS
            .iter()
            .filter_map(|key| {
                object
                    .get(*key)
                    .cloned()
                    .map(|value| ((*key).to_string(), value))
            })
            .collect(),
    )
}

/// Returns `None` for a true same-field three-way conflict. Unknown fields are
/// always taken from the current document so forward-compatible metadata is
/// not normalized away by an env credential update.
fn merge_env_entry_three_way(
    current: Option<&Value>,
    original: Option<&Value>,
    staged: &Value,
) -> ConfigStoreResult<Option<Value>> {
    let staged = staged.as_object().ok_or_else(|| {
        ConfigStoreError::Validation("staged env entry is not an object".to_string())
    })?;
    let current_object = current.and_then(Value::as_object);
    let original_object = original.and_then(Value::as_object);
    let mut merged = current_object.cloned().unwrap_or_default();
    for key in ENV_ENTRY_KNOWN_KEYS {
        let current_value = current_object.and_then(|object| object.get(key));
        let original_value = original_object.and_then(|object| object.get(key));
        let staged_value = staged.get(key);
        let chosen = if current_value == staged_value || current_value == original_value {
            staged_value
        } else if staged_value == original_value {
            current_value
        } else {
            return Ok(None);
        };
        match chosen {
            Some(value) => {
                merged.insert(key.to_string(), value.clone());
            }
            None => {
                merged.remove(key);
            }
        }
    }
    Ok(Some(Value::Object(merged)))
}

fn install_rebased_proxy_config_member(
    data_dir: &Path,
    manifest: &mut MigrationManifest,
    file_index: usize,
    #[cfg(test)] fault: Option<MigrationFault>,
) -> ConfigStoreResult<bool> {
    if manifest.exact_scope != Some(ExactTransactionScope::ProxyAuth) {
        return Ok(false);
    }
    let credential_file = manifest
        .files
        .iter()
        .find(|file| file.name == CREDENTIALS_FILE)
        .ok_or_else(|| {
            ConfigStoreError::Validation(
                "committed credential transaction is incomplete".to_string(),
            )
        })?;
    if credential_file.touched_credential_refs.len() != 1 {
        return Ok(false);
    }
    let touched_ref = credential_file.touched_credential_refs[0].clone();
    let stage_dir = validated_stage_dir(data_dir, &manifest.stage_dir)?;
    let file = manifest.files[file_index].clone();
    let staged: Value = serde_json::from_slice(&read_managed_file(
        data_dir,
        &manifest.stage_dir,
        &file.staged_name,
    )?)?;
    let staged_ref = staged
        .get("proxy_auth_credential_ref")
        .and_then(Value::as_str);
    if staged_ref != Some(touched_ref.as_str()) {
        return Ok(false);
    }

    ensure_proxy_consumers_or_abort(data_dir, manifest)?;
    let target = data_dir.join(CONFIG_FILE);
    let current = read_target_or_empty(&target)?;
    let current_hash = sha256(&current);
    let mut object = parse_config_root_object(
        &current,
        "config.json changed to an invalid document during committed proxy transaction",
    )?;
    let original =
        read_encrypted_migration_backup(data_dir, &manifest.transaction_id, CONFIG_FILE)?;
    let original_object = parse_config_root_object(
        &original,
        "config transaction backup is not a valid document",
    )?;
    let staged_object = parse_config_root_object(
        &serde_json::to_vec(&staged)?,
        "staged proxy config transaction document is invalid",
    )?;
    let current_domain = proxy_auth_domain(&object);
    let original_domain = proxy_auth_domain(&original_object);
    let staged_domain = proxy_auth_domain(&staged_object);
    if current_domain != original_domain && current_domain != staged_domain {
        abort_proxy_exact_transaction(data_dir, manifest)?;
        return Err(ConfigStoreError::Validation(
            "proxy authentication metadata changed during committed transaction".to_string(),
        ));
    }
    replace_proxy_auth_domain(&mut object, &staged_object);
    let rebased = Value::Object(object);
    serde_json::from_value::<crate::Config>(rebased.clone()).map_err(|_| {
        ConfigStoreError::Validation(
            "config.json changed to an invalid document during committed proxy transaction"
                .to_string(),
        )
    })?;
    let rebased = serde_json::to_vec_pretty(&rebased)?;
    let staged_name = format!("{CONFIG_FILE}.rebase.{}", Uuid::new_v4());
    AtomicFileStore::new(stage_dir.join(&staged_name)).write_bytes_without_backup(&rebased)?;
    sync_dir(&stage_dir)?;
    let file = &mut manifest.files[file_index];
    file.staged_name = staged_name;
    file.sha256 = sha256(&rebased);
    file.original_sha256 = Some(current_hash.clone());
    file.original_present = Some(true);
    write_manifest(data_dir.join(MANIFEST_FILE), manifest)?;
    #[cfg(test)]
    if fault == Some(MigrationFault::AfterExactProxyConfigRebaseManifestExternalWrite) {
        inject_external_proxy_root_write(&target)?;
        return Err(injected_fault());
    }
    if !AtomicFileStore::new(&target).write_bytes_if_hash_with_backup(&current_hash, &rebased)? {
        return Err(ConfigStoreError::Validation(
            "config.json changed repeatedly during committed proxy transaction".to_string(),
        ));
    }
    Ok(true)
}

fn ensure_proxy_ref_has_no_durable_non_proxy_consumers(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    if manifest.exact_scope != Some(ExactTransactionScope::ProxyAuth) {
        return Ok(());
    }
    let credential_file = manifest
        .files
        .iter()
        .find(|file| file.name == CREDENTIALS_FILE)
        .ok_or_else(|| {
            ConfigStoreError::Validation(
                "committed credential transaction is incomplete".to_string(),
            )
        })?;
    let [touched_ref] = credential_file.touched_credential_refs.as_slice() else {
        return Err(ConfigStoreError::Validation(
            "committed proxy credential transaction is invalid".to_string(),
        ));
    };

    ensure_no_durable_non_proxy_consumers(data_dir, touched_ref)
}

fn ensure_no_durable_non_proxy_consumers(
    data_dir: &Path,
    touched_ref: &str,
) -> ConfigStoreResult<()> {
    for name in [CONFIG_FILE, PROVIDERS_FILE, MCP_FILE] {
        let bytes = read_target_or_empty(&data_dir.join(name))?;
        if bytes.is_empty() {
            continue;
        }
        ensure_no_non_proxy_consumers_in_document(&bytes, touched_ref, name == CONFIG_FILE, name)?;
    }
    for descriptor in crate::SECTION_DESCRIPTORS.iter().filter(|descriptor| {
        descriptor.id != crate::SectionId::Credentials
            && !matches!(descriptor.file_name, PROVIDERS_FILE | MCP_FILE)
    }) {
        let bytes = read_target_or_empty(&data_dir.join(descriptor.file_name))?;
        if bytes.is_empty() {
            continue;
        }
        let mut document: Value = serde_json::from_slice(&bytes)?;
        if descriptor.file_name == CORE_FILE {
            if let Some(core) = document.get_mut("data").and_then(Value::as_object_mut) {
                core.remove("proxy_auth_credential_ref");
            }
        }
        if contains_non_proxy_credential_reference(&document, touched_ref, false) {
            return Err(ConfigStoreError::Validation(
                "proxy auth credential reference has a durable non-proxy consumer".to_string(),
            ));
        }
    }
    Ok(())
}

fn ensure_no_non_proxy_consumers_in_document(
    bytes: &[u8],
    touched_ref: &str,
    config_root: bool,
    document_name: &str,
) -> ConfigStoreResult<()> {
    let document: Value = serde_json::from_slice(bytes).map_err(|_| {
        ConfigStoreError::Validation(format!(
            "{document_name} changed to an invalid document during proxy credential migration"
        ))
    })?;
    if contains_non_proxy_credential_reference(&document, touched_ref, config_root) {
        return Err(ConfigStoreError::Validation(
            "proxy auth credential reference has a durable non-proxy consumer".to_string(),
        ));
    }
    Ok(())
}

fn contains_non_proxy_credential_reference(
    value: &Value,
    touched_ref: &str,
    config_root: bool,
) -> bool {
    match value {
        Value::Object(object) => object.iter().any(|(key, child)| {
            if config_root && key == "proxy_auth_credential_ref" {
                return false;
            }
            ((key == "credential_ref" || key.ends_with("_credential_ref"))
                && child.as_str() == Some(touched_ref))
                || (key.ends_with("_credential_refs") && contains_string_value(child, touched_ref))
                || contains_non_proxy_credential_reference(child, touched_ref, false)
        }),
        Value::Array(values) => values
            .iter()
            .any(|child| contains_non_proxy_credential_reference(child, touched_ref, false)),
        _ => false,
    }
}

fn contains_string_value(value: &Value, expected: &str) -> bool {
    match value {
        Value::String(value) => value == expected,
        Value::Array(values) => values
            .iter()
            .any(|value| contains_string_value(value, expected)),
        Value::Object(object) => object
            .values()
            .any(|value| contains_string_value(value, expected)),
        _ => false,
    }
}

#[cfg(test)]
fn inject_external_proxy_root_write(target: &Path) -> ConfigStoreResult<()> {
    let mut current: Value = serde_json::from_slice(&read_file_reject_symlink(target)?)?;
    let object = current.as_object_mut().ok_or_else(|| {
        ConfigStoreError::Validation(
            "config.json changed to an invalid document during committed proxy transaction"
                .to_string(),
        )
    })?;
    object.insert(
        "external_rebase_generation".to_string(),
        Value::Number(2_u64.into()),
    );
    AtomicFileStore::new(target).write_bytes_without_backup(&serde_json::to_vec_pretty(&current)?)
}

fn install_exact_credential_member(
    data_dir: &Path,
    manifest: &mut MigrationManifest,
    file_index: usize,
    #[cfg(test)] fault: Option<MigrationFault>,
) -> ConfigStoreResult<()> {
    let stage_dir = validated_stage_dir(data_dir, &manifest.stage_dir)?;
    let original =
        read_encrypted_migration_backup(data_dir, &manifest.transaction_id, CREDENTIALS_FILE)?;
    let target = data_dir.join(CREDENTIALS_FILE);

    for _ in 0..16 {
        let file = manifest.files[file_index].clone();
        if file
            .transaction_base_sha256
            .as_deref()
            .is_some_and(|expected| sha256(&original) != expected)
        {
            return Err(ConfigStoreError::Validation(
                "credential migration backup failed integrity validation".to_string(),
            ));
        }
        let staged = read_managed_file(data_dir, &manifest.stage_dir, &file.staged_name)?;
        if sha256(&staged) != file.sha256 {
            return Err(ConfigStoreError::Validation(
                "staged migration document failed integrity validation".to_string(),
            ));
        }
        let current = read_target_or_empty(&target)?;
        let current_hash = sha256(&current);
        if current_hash == file.sha256 {
            CredentialStore::ensure_required_refs_in_bytes(
                &current,
                &file.required_credential_refs,
            )?;
            return Ok(());
        }
        let expected_hash = file.original_sha256.as_deref().ok_or_else(|| {
            ConfigStoreError::Validation("transaction member base hash is missing".to_string())
        })?;
        if current_hash == expected_hash {
            CredentialStore::ensure_required_refs_in_bytes(
                &staged,
                &file.required_credential_refs,
            )?;
            if AtomicFileStore::new(&target)
                .sensitive(true)
                .write_bytes_if_hash_with_backup(expected_hash, &staged)?
            {
                CredentialStore::ensure_required_refs_in_bytes(
                    &staged,
                    &file.required_credential_refs,
                )?;
                return Ok(());
            }
            continue;
        }
        if file.touched_credential_refs.is_empty()
            && manifest.exact_scope != Some(ExactTransactionScope::Notifications)
            && manifest.exact_scope != Some(ExactTransactionScope::Connect)
            && manifest.exact_scope != Some(ExactTransactionScope::ClusterFabric)
        {
            return Err(ConfigStoreError::Validation(
                "committed credential transaction cannot be safely rebased".to_string(),
            ));
        }
        let preserve_notification_domain_revision =
            if manifest.exact_scope == Some(ExactTransactionScope::Notifications) {
                let authority = exact_authority_file(manifest).ok_or_else(|| {
                    ConfigStoreError::Validation(
                        "notification transaction authority is missing".to_string(),
                    )
                })?;
                let (_, staged_authority) =
                    initial_exact_transaction_member(data_dir, manifest, authority)?;
                let original_authority =
                    read_encrypted_migration_backup(data_dir, &manifest.transaction_id, authority)?;
                if authority == CONFIG_FILE {
                    notification_domain_changed(&original_authority, &staged_authority)?
                } else {
                    let (_, _, original_data) =
                        parse_exact_section_document(authority, &original_authority)?;
                    let (_, _, staged_data) =
                        parse_exact_section_document(authority, &staged_authority)?;
                    original_data != staged_data
                }
            } else {
                false
            };
        let (rebased, current_revision, remaining_required_refs) =
            CredentialStore::merge_exact_transaction_documents(
                &original,
                &staged,
                &current,
                &file.touched_credential_refs,
                &file.required_credential_refs,
                preserve_notification_domain_revision,
            )?;
        let staged_name = format!("{CREDENTIALS_FILE}.rebase.{}", Uuid::new_v4());
        AtomicFileStore::new(stage_dir.join(&staged_name))
            .sensitive(true)
            .write_bytes_without_backup(&rebased)?;
        sync_dir(&stage_dir)?;
        #[cfg(test)]
        if fault == Some(MigrationFault::AfterExactCredentialRebaseStage) {
            return Err(injected_fault());
        }
        let file = &mut manifest.files[file_index];
        file.staged_name = staged_name;
        file.sha256 = sha256(&rebased);
        file.original_sha256 = Some(current_hash);
        file.original_present = Some(true);
        file.expected_revision = Some(current_revision);
        file.required_credential_refs = remaining_required_refs;
        write_manifest(data_dir.join(MANIFEST_FILE), manifest)?;
        #[cfg(test)]
        if fault == Some(MigrationFault::AfterExactCredentialRebaseManifest) {
            return Err(injected_fault());
        }
    }
    Err(ConfigStoreError::Validation(
        "credential transaction could not obtain a stable revision".to_string(),
    ))
}

fn ensure_exact_transaction_credentials(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    let file = manifest
        .files
        .iter()
        .find(|file| file.name == CREDENTIALS_FILE)
        .ok_or_else(|| {
            ConfigStoreError::Validation(
                "committed credential transaction is incomplete".to_string(),
            )
        })?;
    let current = read_target_or_empty(&data_dir.join(CREDENTIALS_FILE))?;
    CredentialStore::ensure_required_refs_in_bytes(&current, &file.required_credential_refs)
}

fn rollback_removed_provider_facade_credentials(
    data_dir: &Path,
    manifest: &MigrationManifest,
    config_file: &StagedFile,
) -> ConfigStoreResult<()> {
    let credential_file = manifest
        .files
        .iter()
        .find(|file| file.name == CREDENTIALS_FILE)
        .ok_or_else(|| {
            ConfigStoreError::Validation(
                "provider facade transaction has no credential member".to_string(),
            )
        })?;
    let original = match credential_file.original_sha256.as_deref() {
        Some(expected) => {
            let bytes = read_encrypted_migration_backup(
                data_dir,
                &manifest.transaction_id,
                CREDENTIALS_FILE,
            )?;
            if sha256(&bytes) != expected {
                return Err(ConfigStoreError::Validation(
                    "credential transaction backup failed integrity validation".to_string(),
                ));
            }
            bytes
        }
        None => Vec::new(),
    };
    let staged_credentials =
        read_managed_file(data_dir, &manifest.stage_dir, &credential_file.staged_name)?;
    let staged_config = read_managed_file(data_dir, &manifest.stage_dir, &config_file.staged_name)?;
    let current_config = read_target_or_empty(&data_dir.join(CONFIG_FILE))?;
    let staged_config_value: Value = serde_json::from_slice(&staged_config)?;
    let current_config_value: Value = serde_json::from_slice(&current_config)?;
    let mut rollback_refs = Vec::new();
    for reference in
        CredentialStore::changed_refs_between_documents(&original, &staged_credentials)?
    {
        if contains_credential_reference(&staged_config_value, &reference)
            && !contains_credential_reference(&current_config_value, &reference)
            && !provider_facade_ref_has_other_consumer(data_dir, &reference)?
        {
            rollback_refs.push(reference);
        }
    }
    if rollback_refs.is_empty() {
        return Ok(());
    }
    let target = data_dir.join(CREDENTIALS_FILE);
    for _ in 0..16 {
        let current = read_target_or_empty(&target)?;
        if current.is_empty() {
            return Ok(());
        }
        let current_hash = sha256(&current);
        let (rolled_back, _, changed) = CredentialStore::rollback_exact_transaction_documents(
            &original,
            &staged_credentials,
            &current,
            &rollback_refs,
        )?;
        if !changed {
            return Ok(());
        }
        if AtomicFileStore::new(&target)
            .sensitive(true)
            .write_bytes_if_hash_with_backup(&current_hash, &rolled_back)?
        {
            return Ok(());
        }
    }
    Err(ConfigStoreError::Validation(
        "provider facade credential rollback could not obtain a stable revision".to_string(),
    ))
}

fn provider_facade_ref_has_other_consumer(
    data_dir: &Path,
    reference: &str,
) -> ConfigStoreResult<bool> {
    let mut names = crate::section_facade::SECTION_DESCRIPTORS
        .iter()
        .filter(|descriptor| descriptor.file_name != CREDENTIALS_FILE)
        .map(|descriptor| descriptor.file_name)
        .collect::<BTreeSet<_>>();
    names.insert(BROKER_FILE);
    for name in names {
        let bytes = match read_file_reject_symlink(&data_dir.join(name)) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        let value: Value = serde_json::from_slice(&bytes)?;
        if contains_credential_reference(&value, reference) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn read_encrypted_migration_backup(
    data_dir: &Path,
    transaction_id: &str,
    file_name: &str,
) -> ConfigStoreResult<Vec<u8>> {
    validated_backup_dir(data_dir, transaction_id)?;
    let backup_dir = format!("{BACKUP_PREFIX}{transaction_id}");
    let protected: Value =
        serde_json::from_slice(&read_managed_file(data_dir, &backup_dir, file_name)?)?;
    if protected.get("version").and_then(Value::as_u64) != Some(1) {
        return Err(ConfigStoreError::Validation(
            "credential migration backup is invalid".to_string(),
        ));
    }
    let ciphertext = protected
        .get("ciphertext")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            ConfigStoreError::Validation("credential migration backup is invalid".to_string())
        })?;
    crate::encryption::decrypt(ciphertext)
        .map(String::into_bytes)
        .map_err(|_| {
            ConfigStoreError::Validation("credential migration backup is unavailable".to_string())
        })
}

fn rebase_changed_section(
    data_dir: &Path,
    manifest: &mut MigrationManifest,
    file_index: usize,
    #[cfg(test)] fault: Option<MigrationFault>,
) -> ConfigStoreResult<bool> {
    if is_section_layout_scope(manifest.migration_scope) {
        let file = manifest.files[file_index].clone();
        if file.name == crate::SECTION_LAYOUT_FILE || file.name == CREDENTIALS_FILE {
            let error = ConfigStoreError::Validation(
                "layout transaction metadata changed during committed migration".to_string(),
            );
            abort_section_split_transaction(data_dir, manifest)?;
            return Err(error);
        }
        if file.install_mode == InstallMode::LegacyCleanup {
            let error = ConfigStoreError::Validation(
                "legacy source changed during committed facade migration".to_string(),
            );
            abort_section_split_transaction(data_dir, manifest)?;
            return Err(error);
        }
        let current = read_target_or_empty(&data_dir.join(&file.name))?;
        let minimum_revision = file.migration_generation.unwrap_or(0);
        let revision = match crate::section_facade::validate_section_envelope(
            &file.name,
            &current,
            minimum_revision,
        ) {
            Ok(revision) => revision,
            Err(error) => {
                abort_section_split_transaction(data_dir, manifest)?;
                return Err(error);
            }
        };
        if revision == minimum_revision {
            let error = ConfigStoreError::Validation(
                "configuration section changed without advancing its revision".to_string(),
            );
            abort_section_split_transaction(data_dir, manifest)?;
            return Err(error);
        }

        // A valid external section edit is authoritative. Keep a staged copy
        // so manifest integrity remains replayable, but mark base == candidate
        // and skip installation; a later abort can distinguish this adopted
        // edit from members installed by the transaction.
        let staged_name = format!("{}.rebase.{}", file.name, Uuid::new_v4());
        let stage_dir = validated_stage_dir(data_dir, &manifest.stage_dir)?;
        AtomicFileStore::new(stage_dir.join(&staged_name))
            .sensitive(file.sensitive)
            .write_bytes_without_backup(&current)?;
        sync_dir(&stage_dir)?;
        let current_hash = sha256(&current);
        let file = &mut manifest.files[file_index];
        file.staged_name = staged_name;
        file.sha256 = current_hash.clone();
        file.original_sha256 = Some(current_hash);
        file.original_present = Some(true);
        file.migration_generation = Some(revision);
        write_manifest(data_dir.join(MANIFEST_FILE), manifest)?;
        return Ok(false);
    }
    let name = manifest.files[file_index].name.clone();
    let mut extracted = Vec::new();
    let minimum_generation =
        next_revision(manifest.files[file_index].migration_generation.unwrap_or(0))?;
    let section = match name.as_str() {
        PROVIDERS_FILE => plan_provider_section(data_dir, &mut extracted, minimum_generation)?,
        MCP_FILE => plan_mcp_section(data_dir, &mut extracted, minimum_generation)?,
        BROKER_FILE => plan_broker_section(data_dir, &mut extracted, minimum_generation)?,
        CONFIG_FILE if manifest.migration_scope == Some(MigrationScope::ClusterFabric) => {
            plan_cluster_section(data_dir, &mut extracted, minimum_generation, false)?
        }
        CONFIG_FILE => plan_provider_instance_section(
            data_dir,
            &mut extracted,
            minimum_generation,
            false,
            manifest.migration_scope == Some(MigrationScope::ProviderFacade),
        )?,
        _ => {
            return Err(ConfigStoreError::Validation(
                "migration section target is invalid".to_string(),
            ))
        }
    };
    let Some(section) = section else {
        // A concurrent writer already produced a secret-free document. Treat
        // that newer revision as authoritative and remove the stale candidate.
        if manifest.migration_scope == Some(MigrationScope::ClusterFabric) {
            // Credentials install before config. Scrub recoverable cluster
            // backups while the scoped manifest is still authoritative, then
            // clear the scope so the remaining credentials-only completed
            // manifest is valid and cannot poison every later reader.
            scrub_cluster_credentials_from_backups(data_dir)?;
            manifest.migration_scope = None;
        } else if manifest.migration_scope == Some(MigrationScope::ProviderFacade) {
            // The facade-only scope exists solely to make a CONFIG_FILE rebase
            // include root built-in providers. Once a newer secret-free root
            // removes that member, the remaining generic provider/MCP files
            // form a valid recoverable transaction without the scope.
            let config_file = manifest.files[file_index].clone();
            rollback_removed_provider_facade_credentials(data_dir, manifest, &config_file)?;
            manifest.migration_scope = None;
        }
        manifest.files.remove(file_index);
        write_manifest(data_dir.join(MANIFEST_FILE), manifest)?;
        return Ok(false);
    };
    if manifest.migration_scope == Some(MigrationScope::ClusterFabric) {
        ensure_cluster_extractions_are_safe(
            data_dir,
            &extracted,
            &[(section.bytes.as_slice(), true)],
        )?;
    }
    let store = CredentialStore::open(data_dir);
    let resolved = resolve_extracted_secrets(&store, extracted)?;
    let prepared = store.prepare_migration(resolved)?;
    store.commit_migration(&prepared.bytes)?;
    #[cfg(test)]
    if fault == Some(MigrationFault::AfterRebaseCredentialCommit) {
        return Err(injected_fault());
    }

    let staged_name = format!("{}.rebase.{}", section.name, Uuid::new_v4());
    let stage_path = validated_stage_dir(data_dir, &manifest.stage_dir)?.join(&staged_name);
    AtomicFileStore::new(stage_path).write_bytes_without_backup(&section.bytes)?;
    #[cfg(test)]
    if fault == Some(MigrationFault::AfterRebaseStageWrite) {
        return Err(injected_fault());
    }
    manifest.files[file_index].staged_name = staged_name;
    manifest.files[file_index].sha256 = sha256(&section.bytes);
    manifest.files[file_index].original_sha256 = Some(sha256(&section.original));
    manifest.files[file_index].original_present = Some(true);
    manifest.files[file_index].migration_generation = Some(section.migration_generation);
    write_manifest(data_dir.join(MANIFEST_FILE), manifest)?;
    #[cfg(test)]
    if fault == Some(MigrationFault::AfterRebaseManifest) {
        return Err(injected_fault());
    }
    Ok(true)
}

fn abort_section_split_transaction(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    if !is_section_layout_scope(manifest.migration_scope) {
        return Ok(());
    }
    let journal_bytes =
        read_optional_migration_file(&data_dir.join(JOURNAL_FILE))?.ok_or_else(|| {
            ConfigStoreError::Validation("section migration journal is missing".to_string())
        })?;
    let initial: MigrationManifest = serde_json::from_slice(&journal_bytes)?;
    validate_manifest(&initial)?;
    if initial.transaction_id != manifest.transaction_id || initial.stage_dir != manifest.stage_dir
    {
        return Err(ConfigStoreError::Validation(
            "section migration journal does not match committed manifest".to_string(),
        ));
    }
    for file in manifest.files.iter().filter(|file| {
        file.name != CREDENTIALS_FILE && file.install_mode != InstallMode::SourceGuard
    }) {
        // Adopted concurrent edits deliberately use base == candidate and must
        // survive a later failure in another member.
        if file.original_sha256.as_deref() == Some(file.sha256.as_str()) {
            continue;
        }
        let initial_file = initial
            .files
            .iter()
            .find(|candidate| candidate.name == file.name)
            .ok_or_else(|| {
                ConfigStoreError::Validation("section migration journal is incomplete".to_string())
            })?;
        let target = data_dir.join(&file.name);
        let current = read_target_or_empty(&target)?;
        let installed_hash = if file.name == crate::SECTION_LAYOUT_FILE {
            &file.sha256
        } else {
            &initial_file.sha256
        };
        if sha256(&current) != *installed_hash {
            // Not installed, or changed by an external writer. Preserve it.
            continue;
        }
        if initial_file.original_present == Some(false) {
            if !AtomicFileStore::new(&target).remove_if_hash(installed_hash)? {
                return Err(ConfigStoreError::Validation(
                    "section changed repeatedly during migration rollback".to_string(),
                ));
            }
            continue;
        }
        let original =
            read_encrypted_migration_backup(data_dir, &manifest.transaction_id, &file.name)?;
        if initial_file.original_sha256.as_deref() != Some(sha256(&original).as_str()) {
            return Err(ConfigStoreError::Validation(
                "section migration backup failed integrity validation".to_string(),
            ));
        }
        if initial_file.original_present.is_none() && original.is_empty() {
            if !AtomicFileStore::new(&target).remove_if_hash(installed_hash)? {
                return Err(ConfigStoreError::Validation(
                    "section changed repeatedly during migration rollback".to_string(),
                ));
            }
            continue;
        }
        if !AtomicFileStore::new(&target).write_bytes_if_hash(installed_hash, &original)? {
            return Err(ConfigStoreError::Validation(
                "section changed repeatedly during migration rollback".to_string(),
            ));
        }
    }

    rollback_section_split_credentials(data_dir, &initial)?;

    let mut complete = manifest.clone();
    complete.state = MigrationState::Complete;
    write_manifest(data_dir.join(MANIFEST_FILE), &complete)?;
    remove_file_if_exists(&data_dir.join(JOURNAL_FILE))?;
    cleanup_transaction_dirs(data_dir, &complete)?;
    remove_file_if_exists(&data_dir.join(MANIFEST_FILE))?;
    sync_dir(data_dir)
}

fn rollback_section_split_credentials(
    data_dir: &Path,
    initial: &MigrationManifest,
) -> ConfigStoreResult<()> {
    let credential_file = initial
        .files
        .iter()
        .find(|file| file.name == CREDENTIALS_FILE)
        .ok_or_else(|| {
            ConfigStoreError::Validation("section migration journal is incomplete".to_string())
        })?;
    let original = match credential_file.original_present {
        Some(false) => Vec::new(),
        Some(true) | None if credential_file.original_sha256.is_some() => {
            read_encrypted_migration_backup(data_dir, &initial.transaction_id, CREDENTIALS_FILE)?
        }
        Some(true) => {
            return Err(ConfigStoreError::Validation(
                "credential migration backup is missing".to_string(),
            ));
        }
        None => Vec::new(),
    };
    let staged = read_managed_file(data_dir, &initial.stage_dir, &credential_file.staged_name)?;
    let changed_refs = CredentialStore::changed_refs_between_documents(&original, &staged)?;
    let target = data_dir.join(CREDENTIALS_FILE);
    for _ in 0..16 {
        let current = read_target_or_empty(&target)?;
        if current.is_empty() {
            return Ok(());
        }
        let current_hash = sha256(&current);
        let (rolled_back, _, changed) = CredentialStore::rollback_exact_transaction_documents(
            &original,
            &staged,
            &current,
            &changed_refs,
        )?;
        if changed
            && !AtomicFileStore::new(&target)
                .sensitive(true)
                .write_bytes_if_hash(&current_hash, &rolled_back)?
        {
            continue;
        }
        let durable = if changed { rolled_back } else { current };
        let empty_document = serde_json::from_slice::<Value>(&durable)
            .ok()
            .and_then(|value| value.get("data").cloned())
            .and_then(|value| value.get("entries").cloned())
            .and_then(|value| value.as_object().cloned())
            .is_some_and(|entries| entries.is_empty());
        if credential_file.original_present == Some(false) && empty_document {
            let durable_hash = sha256(&durable);
            if !AtomicFileStore::new(&target)
                .sensitive(true)
                .remove_if_hash(&durable_hash)?
            {
                continue;
            }
        }
        return Ok(());
    }
    Err(ConfigStoreError::Validation(
        "credential section changed repeatedly during migration rollback".to_string(),
    ))
}

fn finish_transaction(data_dir: &Path, mut manifest: MigrationManifest) -> ConfigStoreResult<()> {
    let section_split = is_section_layout_scope(manifest.migration_scope);
    if section_split {
        verify_section_split_completion(data_dir, &manifest)?;
    }
    let contains_broker = manifest.files.iter().any(|file| file.name == BROKER_FILE);
    let contains_non_broker_config = manifest
        .files
        .iter()
        .any(|file| matches!(file.name.as_str(), PROVIDERS_FILE | MCP_FILE | CONFIG_FILE))
        || manifest.exact_scope.is_some();
    let compound = manifest.migration_scope == Some(MigrationScope::FacadeCompound);
    if contains_non_broker_config && !compound {
        let proxy_clear_tombstones = proxy_clear_tombstones_from_manifest(&manifest);
        let env_clear_tombstones = env_clear_tombstones_from_manifest(&manifest);
        let notification_clear_tombstones = notification_clear_tombstones_from_manifest(&manifest);
        scrub_provider_instance_credentials_from_backups(
            data_dir,
            &proxy_clear_tombstones,
            &env_clear_tombstones,
            &notification_clear_tombstones,
        )?;
    }
    if contains_broker && !compound {
        scrub_broker_credentials_from_backups(data_dir)?;
    }
    if !compound
        && (manifest.migration_scope == Some(MigrationScope::ClusterFabric)
            || manifest.exact_scope == Some(ExactTransactionScope::ClusterFabric))
    {
        scrub_cluster_credentials_from_backups(data_dir)?;
    }
    manifest.state = MigrationState::Complete;
    write_manifest(data_dir.join(MANIFEST_FILE), &manifest)?;
    if section_split {
        persist_section_layout_completion_ledger(data_dir, &manifest)?;
    }
    remove_file_if_exists(&data_dir.join(JOURNAL_FILE))?;
    cleanup_transaction_dirs(data_dir, &manifest)
}

fn scrub_broker_credentials_from_backups(data_dir: &Path) -> ConfigStoreResult<usize> {
    let mut reference = match read_file_reject_symlink(&data_dir.join(BROKER_FILE)) {
        Ok(bytes) => serde_json::from_slice::<crate::BrokerClientConfig>(&bytes)
            .ok()
            .and_then(|primary| primary.credential_ref),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    let store = CredentialStore::open(data_dir);
    let mut backup_secret = None;
    for suffix in ["bak", "bak.1", "bak.2"] {
        let path = data_dir.join(format!("{BROKER_FILE}.{suffix}"));
        let bytes = match read_file_reject_symlink(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        let (candidate_ref, candidate_secret) =
            broker_backup_candidate(&bytes, reference.as_ref())?;
        if reference.is_none() {
            reference = candidate_ref;
        }
        let Some(current_ref) = reference.as_ref() else {
            continue;
        };
        let mut root: Value = match serde_json::from_slice(&bytes) {
            Ok(root) => root,
            Err(_) => continue,
        };
        if let Some(object) = root.as_object_mut() {
            object.remove("credential_ref");
        }
        if contains_credential_reference(&root, current_ref.as_str()) {
            return Err(ConfigStoreError::Validation(
                "broker credential reference is shared inside broker backup".to_string(),
            ));
        }
        if backup_secret.is_none()
            && !store.status_unchecked(current_ref)?.configured
            && candidate_secret.is_some()
        {
            backup_secret = candidate_secret;
        }
    }
    let Some(reference) = reference else {
        return Ok(0);
    };
    ensure_broker_ref_exclusive(data_dir, &reference)?;
    let migrated_credentials = if let Some(secret) = backup_secret {
        let resolved = resolve_extracted_secrets(
            &store,
            vec![ExtractedSecret {
                credential_ref: reference.clone(),
                value: secret,
                migration_generation: 1,
                kind: ExtractedSecretKind::ExternalBroker,
                env_owner: None,
                provider_owner: None,
                mcp_owner: None,
                connect_owner: None,
            }],
        )?;
        let prepared = store.prepare_migration(resolved)?;
        let added = prepared.added;
        store.commit_migration(&prepared.bytes)?;
        added
    } else {
        0
    };
    let configured = store.status_unchecked(&reference)?.configured;
    for suffix in ["bak", "bak.1", "bak.2"] {
        let path = data_dir.join(format!("{BROKER_FILE}.{suffix}"));
        let bytes = match read_file_reject_symlink(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        let mut root: Value = match serde_json::from_slice(&bytes) {
            Ok(root) => root,
            Err(_) => continue,
        };
        let Some(object) = root.as_object_mut() else {
            continue;
        };
        let mut changed = object.remove("token").is_some();
        changed |= object.remove("token_encrypted").is_some();
        changed |= object.get("credential_ref").and_then(Value::as_str) != Some(reference.as_str());
        changed |= object.get("configured").and_then(Value::as_bool) != Some(configured);
        if !changed {
            continue;
        }
        object.insert(
            "credential_ref".to_string(),
            Value::String(reference.as_str().to_string()),
        );
        object.insert("configured".to_string(), Value::Bool(configured));
        AtomicFileStore::new(path)
            .write_bytes_without_backup(&serde_json::to_vec_pretty(&root)?)?;
    }
    Ok(migrated_credentials)
}

fn broker_backup_candidate(
    bytes: &[u8],
    preferred_ref: Option<&CredentialRef>,
) -> ConfigStoreResult<(Option<CredentialRef>, Option<LegacySecret>)> {
    let mut root: Value = match serde_json::from_slice(bytes) {
        Ok(root) => root,
        Err(_) => return Ok((None, None)),
    };
    let Some(object) = root.as_object_mut() else {
        return Ok((None, None));
    };
    let plaintext = take_nonempty_string(object, "token")?;
    let ciphertext = take_nonempty_string(object, "token_encrypted")?;
    let secret = match (plaintext, ciphertext) {
        (Some(plaintext), Some(ciphertext)) => {
            let decrypted = crate::encryption::decrypt(&ciphertext).map_err(|_| {
                ConfigStoreError::Validation(
                    "legacy broker backup credential could not be decrypted".to_string(),
                )
            })?;
            if plaintext != decrypted {
                return Err(ConfigStoreError::Validation(
                    "legacy broker backup credential fields contain conflicting values".to_string(),
                ));
            }
            Some(LegacySecret::Plaintext(plaintext))
        }
        (Some(plaintext), None) => Some(LegacySecret::Plaintext(plaintext)),
        (None, Some(ciphertext)) => Some(LegacySecret::Ciphertext(ciphertext)),
        (None, None) => None,
    };
    let configured = object
        .get("configured")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let explicit_ref = object
        .get("credential_ref")
        .map(|value| {
            value.as_str().ok_or_else(|| {
                ConfigStoreError::Validation(
                    "broker backup credential reference must be a string".to_string(),
                )
            })
        })
        .transpose()?
        .map(|value| CredentialRef::parse(value.to_string()))
        .transpose()?;
    if let (Some(preferred), Some(explicit)) = (preferred_ref, explicit_ref.as_ref()) {
        if preferred != explicit {
            return Err(ConfigStoreError::Validation(
                "broker backup credential reference conflicts with primary configuration"
                    .to_string(),
            ));
        }
    }
    let reference = match preferred_ref {
        Some(reference) => Some(reference.clone()),
        None if explicit_ref.is_some() => explicit_ref,
        None if secret.is_some() || configured => {
            Some(credential_ref("broker", "external", "bearer_token")?)
        }
        None => None,
    };
    Ok((reference, secret))
}

fn proxy_clear_tombstones_from_manifest(manifest: &MigrationManifest) -> BTreeSet<String> {
    if manifest.exact_scope != Some(ExactTransactionScope::ProxyAuth) {
        return BTreeSet::new();
    }
    manifest
        .files
        .iter()
        .find(|file| file.name == CREDENTIALS_FILE)
        .filter(|file| file.required_credential_refs.is_empty())
        .into_iter()
        .flat_map(|file| file.touched_credential_refs.iter().cloned())
        .collect()
}

fn env_clear_tombstones_from_manifest(manifest: &MigrationManifest) -> BTreeSet<String> {
    if manifest.exact_scope != Some(ExactTransactionScope::EnvVars) {
        return BTreeSet::new();
    }
    let Some(file) = manifest
        .files
        .iter()
        .find(|file| file.name == CREDENTIALS_FILE)
    else {
        return BTreeSet::new();
    };
    file.touched_credential_refs
        .iter()
        .filter(|reference| !file.required_credential_refs.contains(reference))
        .cloned()
        .collect()
}

fn notification_clear_tombstones_from_manifest(manifest: &MigrationManifest) -> BTreeSet<String> {
    if manifest.exact_scope != Some(ExactTransactionScope::Notifications) {
        return BTreeSet::new();
    }
    let Some(file) = manifest
        .files
        .iter()
        .find(|file| file.name == CREDENTIALS_FILE)
    else {
        return BTreeSet::new();
    };
    file.touched_credential_refs
        .iter()
        .filter(|reference| !file.required_credential_refs.contains(reference))
        .cloned()
        .collect()
}

fn scrub_provider_instance_credentials_from_backups(
    data_dir: &Path,
    proxy_clear_tombstones: &BTreeSet<String>,
    env_clear_tombstones: &BTreeSet<String>,
    notification_clear_tombstones: &BTreeSet<String>,
) -> ConfigStoreResult<()> {
    let store = CredentialStore::open(data_dir);
    for suffix in ["bak", "bak.1", "bak.2"] {
        let path = data_dir.join(format!("{CONFIG_FILE}.{suffix}"));
        let bytes = match read_file_reject_symlink(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        let mut root: Value = match serde_json::from_slice(&bytes) {
            Ok(root) => root,
            Err(_) => continue,
        };
        if !scrub_provider_instance_credentials(
            data_dir,
            &store,
            &mut root,
            proxy_clear_tombstones,
            env_clear_tombstones,
            notification_clear_tombstones,
        )? {
            continue;
        }
        AtomicFileStore::new(path)
            .write_bytes_without_backup(&serde_json::to_vec_pretty(&root)?)?;
    }
    Ok(())
}

fn scrub_provider_instance_credentials(
    data_dir: &Path,
    store: &CredentialStore,
    root: &mut Value,
    proxy_clear_tombstones: &BTreeSet<String>,
    env_clear_tombstones: &BTreeSet<String>,
    notification_clear_tombstones: &BTreeSet<String>,
) -> ConfigStoreResult<bool> {
    let Some(object) = root.as_object_mut() else {
        return Ok(false);
    };
    let mut extracted = Vec::new();
    let mut changed =
        if scrub_authoritative_or_tombstoned_proxy_auth(object, store, proxy_clear_tombstones)? {
            true
        } else {
            migrate_proxy_auth(object, &mut extracted, 1)?
        };
    changed |= migrate_env_vars(object, &mut extracted, 1)?;
    changed |= migrate_notification_credentials(object, &mut extracted, 1)?;
    if let Some(entries) = object.get_mut("env_vars").and_then(Value::as_array_mut) {
        for entry in entries {
            let Some(entry) = entry.as_object_mut() else {
                continue;
            };
            if entry
                .get("credential_ref")
                .and_then(Value::as_str)
                .is_some_and(|reference| env_clear_tombstones.contains(reference))
            {
                entry.insert("configured".to_string(), Value::Bool(false));
            }
        }
    }
    for channel in ["ntfy", "bark"] {
        let Some(config) = object
            .get_mut("notifications")
            .and_then(Value::as_object_mut)
            .and_then(|notifications| notifications.get_mut(channel))
            .and_then(Value::as_object_mut)
        else {
            continue;
        };
        if config
            .get("credential_ref")
            .and_then(Value::as_str)
            .is_some_and(|reference| notification_clear_tombstones.contains(reference))
        {
            config.insert("configured".to_string(), Value::Bool(false));
        }
    }
    let mut pending = Vec::with_capacity(extracted.len());
    for secret in extracted.drain(..) {
        if secret.kind == ExtractedSecretKind::EnvVar {
            if env_clear_tombstones.contains(secret.credential_ref.as_str()) {
                continue;
            }
            let status = store.status_unchecked(&secret.credential_ref)?;
            if status.configured && status.source != crate::CredentialSource::Migrated {
                // The live store is authoritative; the backup was already
                // rewritten to metadata above, so never replay its stale copy.
                continue;
            }
        }
        if matches!(
            secret.kind,
            ExtractedSecretKind::NotificationNtfy | ExtractedSecretKind::NotificationBark
        ) {
            if notification_clear_tombstones.contains(secret.credential_ref.as_str()) {
                continue;
            }
            let status = store.status_unchecked(&secret.credential_ref)?;
            if status.configured && status.source != crate::CredentialSource::Migrated {
                continue;
            }
        }
        pending.push(secret);
    }
    extracted = pending;
    if let Some(instances) = object
        .get_mut("provider_instances")
        .and_then(Value::as_object_mut)
    {
        for (instance_id, value) in instances {
            let Some(instance) = value.as_object_mut() else {
                continue;
            };
            let had_secret =
                instance.contains_key("api_key") || instance.contains_key("api_key_encrypted");
            if !had_secret {
                continue;
            }
            let value = take_consistent_legacy_secret(
                instance,
                "api_key",
                "api_key_encrypted",
                &format!("provider instance '{instance_id}' API key"),
            )?;
            let reference = if let Some(existing) = instance.get("credential_ref") {
                let existing = existing.as_str().ok_or_else(|| {
                    ConfigStoreError::Validation(
                        "provider instance credential reference must be a string".to_string(),
                    )
                })?;
                CredentialRef::parse(existing.to_string())?
            } else {
                credential_ref("provider_instance", instance_id, "api_key")?
            };
            let status = store.status_unchecked(&reference)?;
            if !status.configured {
                if let Some(value) = value {
                    extracted.push(ExtractedSecret {
                        credential_ref: reference.clone(),
                        value,
                        migration_generation: 1,
                        kind: ExtractedSecretKind::Other,
                        env_owner: None,
                        provider_owner: None,
                        mcp_owner: None,
                        connect_owner: None,
                    });
                } else {
                    instance.remove("api_key");
                    instance.remove("api_key_encrypted");
                    changed = true;
                    continue;
                }
            }
            instance.remove("api_key");
            instance.remove("api_key_encrypted");
            instance.insert(
                "credential_ref".to_string(),
                Value::String(reference.as_str().to_string()),
            );
            changed = true;
        }
    }
    let prospective = serde_json::to_vec(root)?;
    ensure_legacy_proxy_extractions_are_safe(
        data_dir,
        store,
        &extracted,
        &[(prospective.as_slice(), true)],
    )?;
    ensure_legacy_notification_extractions_are_safe(
        data_dir,
        &extracted,
        &[(prospective.as_slice(), true)],
    )?;
    if !extracted.is_empty() {
        let resolved = resolve_extracted_secrets(store, extracted)?;
        let prepared = store.prepare_migration(resolved)?;
        store.commit_migration(&prepared.bytes)?;
    }
    Ok(changed)
}

fn cleanup_transaction_dirs(
    data_dir: &Path,
    manifest: &MigrationManifest,
) -> ConfigStoreResult<()> {
    let stage_dir = validated_stage_dir(data_dir, &manifest.stage_dir)?;
    remove_managed_directory_if_exists(&stage_dir)?;
    let backup_dir = data_dir.join(format!("{BACKUP_PREFIX}{}", manifest.transaction_id));
    remove_managed_directory_if_exists(&backup_dir)?;
    sync_dir(data_dir)?;
    Ok(())
}

pub(crate) fn discard_uncommitted(data_dir: &Path) -> ConfigStoreResult<()> {
    let journal_path = data_dir.join(JOURNAL_FILE);
    let Some(bytes) = read_optional_migration_file(&journal_path)? else {
        return Ok(());
    };
    let journal: MigrationManifest = serde_json::from_slice(&bytes)?;
    validate_manifest(&journal)?;
    let stage_dir = validated_stage_dir(data_dir, &journal.stage_dir)?;
    remove_managed_directory_if_exists(&stage_dir)?;
    let backup_dir = data_dir.join(format!("{BACKUP_PREFIX}{}", journal.transaction_id));
    remove_managed_directory_if_exists(&backup_dir)?;
    remove_file_if_exists(&journal_path)?;
    sync_dir(data_dir)
}

fn cleanup_orphan_transaction_dirs(data_dir: &Path) -> ConfigStoreResult<()> {
    let mut referenced = BTreeSet::new();
    for file in [MANIFEST_FILE, JOURNAL_FILE] {
        let Some(bytes) = read_optional_migration_file(&data_dir.join(file))? else {
            continue;
        };
        let manifest: MigrationManifest = serde_json::from_slice(&bytes).map_err(|_| {
            ConfigStoreError::Validation("credential migration metadata is unavailable".to_string())
        })?;
        validate_manifest(&manifest).map_err(|_| {
            ConfigStoreError::Validation("credential migration metadata is unavailable".to_string())
        })?;
        referenced.insert(manifest.stage_dir.clone());
        referenced.insert(format!("{BACKUP_PREFIX}{}", manifest.transaction_id));
    }

    let mut removed = false;
    for entry in std::fs::read_dir(data_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if referenced.contains(&name) || !is_strict_transaction_dir_name(&name) {
            continue;
        }
        std::fs::remove_dir_all(entry.path())?;
        removed = true;
    }
    if removed {
        sync_dir(data_dir)?;
    }
    Ok(())
}

fn is_strict_transaction_dir_name(name: &str) -> bool {
    [STAGE_PREFIX, BACKUP_PREFIX].iter().any(|prefix| {
        name.strip_prefix(prefix)
            .and_then(|suffix| Uuid::parse_str(suffix).ok().map(|uuid| (suffix, uuid)))
            .is_some_and(|(suffix, uuid)| uuid.to_string() == suffix)
    })
}

fn read_optional_migration_file(path: &Path) -> ConfigStoreResult<Option<Vec<u8>>> {
    match read_file_reject_symlink(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(ConfigStoreError::Validation(
            "credential migration metadata is unavailable".to_string(),
        )),
    }
}

fn read_target_or_empty(path: &Path) -> ConfigStoreResult<Vec<u8>> {
    Ok(read_optional_target(path)?.unwrap_or_default())
}

pub(crate) fn read_section_attestation_target(path: &Path) -> ConfigStoreResult<Option<Vec<u8>>> {
    read_optional_target(path)
}

fn read_optional_target(path: &Path) -> ConfigStoreResult<Option<Vec<u8>>> {
    match read_file_reject_symlink(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn read_file_reject_symlink(path: &Path) -> std::io::Result<Vec<u8>> {
    let mut file = open_existing_regular_file(path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn read_managed_file(
    data_dir: &Path,
    directory_name: &str,
    file_name: &str,
) -> std::io::Result<Vec<u8>> {
    validate_relative_component(OsStr::new(directory_name))?;
    validate_relative_component(OsStr::new(file_name))?;
    #[cfg(any(unix, windows))]
    let mut file = {
        let data_dir = open_directory_no_follow(data_dir)?;
        let directory = open_relative_no_follow(
            &data_dir,
            OsStr::new(directory_name),
            RelativeKind::Directory,
        )?;
        open_relative_no_follow(&directory, OsStr::new(file_name), RelativeKind::RegularFile)?
    };
    #[cfg(not(any(unix, windows)))]
    let mut file = open_existing_regular_file(&data_dir.join(directory_name).join(file_name))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn open_managed_directory(data_dir: &Path, directory_name: &str) -> std::io::Result<File> {
    validate_relative_component(OsStr::new(directory_name))?;
    #[cfg(any(unix, windows))]
    {
        let data_dir = open_directory_no_follow(data_dir)?;
        open_relative_no_follow(
            &data_dir,
            OsStr::new(directory_name),
            RelativeKind::Directory,
        )
    }
    #[cfg(not(any(unix, windows)))]
    open_existing_directory(&data_dir.join(directory_name))
}

/// Open one existing migration input relative to a pinned parent directory
/// handle, then validate the opened handle rather than re-querying the path.
/// This closes both the final-component race and a swap of the immediate parent.
fn open_existing_regular_file(path: &Path) -> std::io::Result<File> {
    #[cfg(any(unix, windows))]
    {
        let parent = path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let name = path.file_name().ok_or_else(invalid_migration_file)?;
        validate_relative_component(name)?;
        let parent = open_directory_no_follow(parent)?;
        open_relative_no_follow(&parent, name, RelativeKind::RegularFile)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let mut options = OpenOptions::new();
        options.read(true);
        let file = options.open(path)?;
        validate_open_regular_file(&file)?;
        Ok(file)
    }
}

/// Open (or race-safely create) the shared migration lock without following a
/// link inserted between existence checking and open. The create/open happens
/// relative to a pinned data-directory handle, so a swapped data-directory
/// path cannot redirect the lock operation after that handle is acquired.
fn open_migration_lock(path: &Path) -> std::io::Result<File> {
    #[cfg(any(unix, windows))]
    {
        let parent = path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let name = path.file_name().ok_or_else(invalid_migration_file)?;
        validate_relative_component(name)?;
        let parent = open_directory_no_follow(parent)?;
        open_relative_no_follow(&parent, name, RelativeKind::LockFile)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let mut options = OpenOptions::new();
        options.create(true).truncate(false).read(true).write(true);
        let file = options.open(path)?;
        validate_open_regular_file(&file)?;
        Ok(file)
    }
}

#[cfg(not(any(unix, windows)))]
fn open_existing_directory(path: &Path) -> std::io::Result<File> {
    let file = File::open(path)?;
    validate_open_directory(&file)?;
    Ok(file)
}

#[derive(Clone, Copy)]
enum RelativeKind {
    Directory,
    RegularFile,
    LockFile,
}

fn validate_relative_component(name: &OsStr) -> std::io::Result<()> {
    let mut components = Path::new(name).components();
    if matches!(components.next(), Some(std::path::Component::Normal(_)))
        && components.next().is_none()
    {
        return Ok(());
    }
    Err(invalid_migration_file())
}

#[cfg(unix)]
fn open_directory_no_follow(path: &Path) -> std::io::Result<File> {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::io::FromRawFd;

    let path = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|_| invalid_migration_file())?;
    let fd = unsafe {
        libc::open(
            path.as_ptr(),
            libc::O_RDONLY
                | libc::O_DIRECTORY
                | libc::O_NOFOLLOW
                | libc::O_CLOEXEC
                | libc::O_NONBLOCK,
        )
    };
    if fd < 0 {
        return Err(normalize_no_follow_error(std::io::Error::last_os_error()));
    }
    let file = unsafe { File::from_raw_fd(fd) };
    validate_open_directory(&file)?;
    Ok(file)
}

#[cfg(unix)]
fn open_relative_no_follow(
    directory: &File,
    name: &OsStr,
    kind: RelativeKind,
) -> std::io::Result<File> {
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;

    validate_relative_component(name)?;
    let name = std::ffi::CString::new(name.as_bytes()).map_err(|_| invalid_migration_file())?;
    let (access, expected_type) = match kind {
        RelativeKind::Directory => (libc::O_RDONLY | libc::O_DIRECTORY, RelativeKind::Directory),
        RelativeKind::RegularFile => (libc::O_RDONLY, RelativeKind::RegularFile),
        RelativeKind::LockFile => (libc::O_RDWR | libc::O_CREAT, RelativeKind::RegularFile),
    };
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            access | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
            0o600,
        )
    };
    if fd < 0 {
        return Err(normalize_no_follow_error(std::io::Error::last_os_error()));
    }
    let file = unsafe { File::from_raw_fd(fd) };
    match expected_type {
        RelativeKind::Directory => validate_open_directory(&file)?,
        RelativeKind::RegularFile | RelativeKind::LockFile => validate_open_regular_file(&file)?,
    }
    Ok(file)
}

#[cfg(windows)]
fn open_directory_no_follow(path: &Path) -> std::io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
    };

    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT);
    let file = options.open(path)?;
    validate_open_directory(&file)?;
    Ok(file)
}

#[cfg(windows)]
fn open_relative_no_follow(
    directory: &File,
    name: &OsStr,
    kind: RelativeKind,
) -> std::io::Result<File> {
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::io::{AsRawHandle, FromRawHandle};
    use windows_sys::Wdk::Foundation::OBJECT_ATTRIBUTES;
    use windows_sys::Wdk::Storage::FileSystem::{
        NtCreateFile, FILE_DIRECTORY_FILE, FILE_NON_DIRECTORY_FILE, FILE_OPEN, FILE_OPEN_IF,
        FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT,
    };
    use windows_sys::Win32::Foundation::{
        RtlNtStatusToDosError, HANDLE, OBJ_CASE_INSENSITIVE, UNICODE_STRING,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_NORMAL, FILE_GENERIC_READ, FILE_GENERIC_WRITE, FILE_READ_ATTRIBUTES,
        FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, SYNCHRONIZE,
    };
    use windows_sys::Win32::System::IO::IO_STATUS_BLOCK;

    validate_relative_component(name)?;
    let mut name = name.encode_wide().collect::<Vec<_>>();
    if name.iter().any(|character| *character == 0)
        || name.len() > (u16::MAX as usize / std::mem::size_of::<u16>())
    {
        return Err(invalid_migration_file());
    }
    let byte_len = (name.len() * std::mem::size_of::<u16>()) as u16;
    let unicode_name = UNICODE_STRING {
        Length: byte_len,
        MaximumLength: byte_len,
        Buffer: name.as_mut_ptr(),
    };
    let attributes = OBJECT_ATTRIBUTES {
        Length: std::mem::size_of::<OBJECT_ATTRIBUTES>() as u32,
        RootDirectory: directory.as_raw_handle() as HANDLE,
        ObjectName: &unicode_name,
        Attributes: OBJ_CASE_INSENSITIVE,
        SecurityDescriptor: std::ptr::null(),
        SecurityQualityOfService: std::ptr::null(),
    };
    let mut handle: HANDLE = std::ptr::null_mut();
    let mut io_status = IO_STATUS_BLOCK::default();
    let (access, disposition, expected_type) = match kind {
        RelativeKind::Directory => (
            FILE_READ_ATTRIBUTES | SYNCHRONIZE,
            FILE_OPEN,
            RelativeKind::Directory,
        ),
        RelativeKind::RegularFile => (FILE_GENERIC_READ, FILE_OPEN, RelativeKind::RegularFile),
        RelativeKind::LockFile => (
            FILE_GENERIC_READ | FILE_GENERIC_WRITE,
            FILE_OPEN_IF,
            RelativeKind::RegularFile,
        ),
    };
    let type_option = match expected_type {
        RelativeKind::Directory => FILE_DIRECTORY_FILE,
        RelativeKind::RegularFile | RelativeKind::LockFile => FILE_NON_DIRECTORY_FILE,
    };
    let status = unsafe {
        NtCreateFile(
            &mut handle,
            access,
            &attributes,
            &mut io_status,
            std::ptr::null(),
            FILE_ATTRIBUTE_NORMAL,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            disposition,
            type_option | FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
            std::ptr::null(),
            0,
        )
    };
    if status < 0 {
        let error = unsafe { RtlNtStatusToDosError(status) };
        return Err(std::io::Error::from_raw_os_error(error as i32));
    }
    let file = unsafe { File::from_raw_handle(handle) };
    match expected_type {
        RelativeKind::Directory => validate_open_directory(&file)?,
        RelativeKind::RegularFile | RelativeKind::LockFile => validate_open_regular_file(&file)?,
    }
    Ok(file)
}

#[cfg(unix)]
fn normalize_no_follow_error(error: std::io::Error) -> std::io::Error {
    if matches!(error.raw_os_error(), Some(libc::ELOOP | libc::ENOTDIR)) {
        return invalid_migration_file();
    }
    error
}

fn validate_open_regular_file(file: &File) -> std::io::Result<()> {
    let metadata = file.metadata()?;
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(invalid_migration_file());
        }
    }
    if !metadata.is_file() {
        return Err(invalid_migration_file());
    }
    Ok(())
}

fn validate_open_directory(file: &File) -> std::io::Result<()> {
    let metadata = file.metadata()?;
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(invalid_migration_file());
        }
    }
    if !metadata.is_dir() {
        return Err(invalid_migration_file());
    }
    Ok(())
}

fn invalid_migration_file() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        "credential migration path must be a regular file opened without following links",
    )
}

fn facade_source_base_matches_file(base: &FacadeSourceBase, file: &StagedFile) -> bool {
    match (base, file.original_present, file.original_sha256.as_deref()) {
        (FacadeSourceBase::Missing, Some(false), None) => true,
        (FacadeSourceBase::Regular { sha256 }, Some(true), Some(original_sha256)) => {
            sha256 == original_sha256
        }
        _ => false,
    }
}

fn facade_source_member_was_rebased(file: &StagedFile) -> bool {
    file.install_mode == InstallMode::Migration
        && file.staged_name != file.name
        && file
            .staged_name
            .strip_prefix(&format!("{}.rebase.", file.name))
            .is_some_and(|suffix| Uuid::parse_str(suffix).is_ok())
        && crate::SECTION_DESCRIPTORS
            .iter()
            .any(|descriptor| descriptor.file_name == file.name)
}

fn facade_source_attestation_is_valid(manifest: &MigrationManifest) -> bool {
    if manifest.migration_scope != Some(MigrationScope::FacadeCompound) {
        return manifest.source_attestation.is_empty();
    }

    let expected = crate::section_facade::preflight_source_names();
    if manifest.source_attestation.len() != expected.len()
        || expected
            .iter()
            .any(|name| !manifest.source_attestation.contains_key(name))
    {
        return false;
    }

    for (name, base) in &manifest.source_attestation {
        if matches!(base, FacadeSourceBase::Regular { sha256 } if !is_sha256_hex(sha256)) {
            return false;
        }
        if matches!(
            name.as_str(),
            MANIFEST_FILE | JOURNAL_FILE | SECTION_LAYOUT_COMPLETION_FILE
        ) || is_compound_internal_derivative_source(name)
        {
            if manifest.files.iter().any(|file| file.name == *name) {
                return false;
            }
            continue;
        }

        let Some(file) = manifest.files.iter().find(|file| file.name == *name) else {
            return false;
        };
        if !facade_source_base_matches_file(base, file) && !facade_source_member_was_rebased(file) {
            return false;
        }
        if file.install_mode == InstallMode::SourceGuard {
            let guard_candidate_matches = match base {
                FacadeSourceBase::Missing => file.sha256 == sha256(&[]),
                FacadeSourceBase::Regular { sha256 } => file.sha256 == *sha256,
            };
            if !guard_candidate_matches {
                return false;
            }
        }
    }
    true
}

fn validate_manifest(manifest: &MigrationManifest) -> ConfigStoreResult<()> {
    let expected_stage = format!("{STAGE_PREFIX}{}", manifest.transaction_id);
    let unique = manifest
        .files
        .iter()
        .map(|file| file.name.as_str())
        .collect::<BTreeSet<_>>();
    let credential_count = manifest
        .files
        .iter()
        .filter(|file| file.name == CREDENTIALS_FILE)
        .count();
    let exact_transaction = manifest
        .files
        .iter()
        .any(|file| file.install_mode == InstallMode::Exact);
    let migration_scope_valid = match (exact_transaction, manifest.migration_scope) {
        (true, None) | (false, None) => true,
        (false, Some(MigrationScope::ClusterFabric)) => {
            manifest.files.len() == 2
                && unique.contains(CREDENTIALS_FILE)
                && unique.contains(CONFIG_FILE)
        }
        (false, Some(MigrationScope::ProviderFacade)) => {
            unique.contains(CREDENTIALS_FILE)
                && unique.contains(CONFIG_FILE)
                && unique.iter().all(|name| {
                    matches!(
                        *name,
                        CREDENTIALS_FILE | CONFIG_FILE | PROVIDERS_FILE | MCP_FILE
                    )
                })
        }
        (false, Some(MigrationScope::SectionSplit)) => {
            manifest.files.len() == crate::SECTION_DESCRIPTORS.len() + 1
                && unique.contains(CREDENTIALS_FILE)
                && unique.contains(crate::SECTION_LAYOUT_FILE)
                && manifest
                    .files
                    .last()
                    .is_some_and(|file| file.name == crate::SECTION_LAYOUT_FILE)
                && crate::SECTION_DESCRIPTORS
                    .iter()
                    .all(|descriptor| unique.contains(descriptor.file_name))
                && manifest
                    .files
                    .iter()
                    .all(|file| file.install_mode != InstallMode::LegacyCleanup)
        }
        (false, Some(MigrationScope::FacadeCompound)) => {
            unique.contains(CREDENTIALS_FILE)
                && unique.contains(crate::SECTION_LAYOUT_FILE)
                && manifest
                    .files
                    .last()
                    .is_some_and(|file| file.name == crate::SECTION_LAYOUT_FILE)
                && crate::SECTION_DESCRIPTORS
                    .iter()
                    .all(|descriptor| unique.contains(descriptor.file_name))
                && manifest.files.iter().all(|file| match file.install_mode {
                    InstallMode::LegacyCleanup => is_legacy_cleanup_file(&file.name),
                    InstallMode::SourceGuard => valid_source_guard_name(&file.name),
                    InstallMode::Migration | InstallMode::Exact => true,
                })
        }
        (true, Some(_)) => false,
    };
    let exact_scope_valid = match (exact_transaction, manifest.exact_scope) {
        (false, None) => true,
        (true, Some(ExactTransactionScope::Providers)) => {
            manifest.files.len() == 2
                && unique.contains(CREDENTIALS_FILE)
                && unique.contains(PROVIDERS_FILE)
        }
        (true, Some(ExactTransactionScope::Mcp)) => {
            manifest.files.len() == 2
                && unique.contains(CREDENTIALS_FILE)
                && unique.contains(MCP_FILE)
        }
        (true, Some(ExactTransactionScope::ProxyAuth)) => {
            manifest.files.len() == 2
                && (unique.contains(CONFIG_FILE) ^ unique.contains(CORE_FILE))
                && manifest
                    .files
                    .iter()
                    .find(|file| file.name == CREDENTIALS_FILE)
                    .is_some_and(|file| file.touched_credential_refs.len() == 1)
        }
        (true, Some(ExactTransactionScope::EnvVars)) => {
            manifest.files.len() == 2 && (unique.contains(CONFIG_FILE) ^ unique.contains(ENV_FILE))
        }
        (true, Some(ExactTransactionScope::Notifications)) => {
            manifest.files.len() == 2
                && (unique.contains(CONFIG_FILE) ^ unique.contains(NOTIFICATIONS_FILE))
        }
        (true, Some(ExactTransactionScope::Connect)) => {
            manifest.files.len() == 2
                && unique.contains(CREDENTIALS_FILE)
                && unique.contains(CONNECT_FILE)
        }
        (true, Some(ExactTransactionScope::AccessControl)) => {
            manifest.files.len() == 2
                && unique.contains(CREDENTIALS_FILE)
                && unique.contains(ACCESS_CONTROL_FILE)
        }
        (true, Some(ExactTransactionScope::ClusterFabric)) => {
            manifest.files.len() == 2
                && (unique.contains(CONFIG_FILE) ^ unique.contains(CLUSTER_FABRIC_FILE))
                && unique.contains(CREDENTIALS_FILE)
        }
        (true, None) => manifest.files.len() == 3 && unique.contains(PROVIDERS_FILE),
        (false, Some(_)) => false,
    };
    let exact_authority = exact_authority_file(manifest);
    let source_attestation_valid = facade_source_attestation_is_valid(manifest);
    let exact_shape_valid = !exact_transaction
        || ((manifest.files.len() == 2 || manifest.files.len() == 3)
            && manifest
                .files
                .iter()
                .all(|file| file.install_mode == InstallMode::Exact)
            && (manifest.exact_scope.is_some() && exact_authority.is_some()
                || manifest.exact_scope.is_none() && unique.contains(CONFIG_FILE))
            && exact_scope_valid
            && manifest.files.iter().all(|file| {
                file.migration_generation.is_none()
                    && ((file.name == CREDENTIALS_FILE && file.expected_revision.is_some())
                        || (file.name != CREDENTIALS_FILE
                            && if exact_authority == Some(file.name.as_str())
                                && file.name != CONFIG_FILE
                            {
                                file.expected_revision.is_some()
                            } else {
                                file.expected_revision.is_none()
                            }))
            }));
    if manifest.version != MIGRATION_VERSION
        || Uuid::parse_str(&manifest.transaction_id).is_err()
        || manifest.stage_dir != expected_stage
        || manifest.files.is_empty()
        || unique.len() != manifest.files.len()
        || credential_count != 1
        || !migration_scope_valid
        || !exact_scope_valid
        || !exact_shape_valid
        || !source_attestation_valid
        || manifest.files.iter().any(|file| {
            !(is_allowed_manifest_file(&file.name)
                || (manifest.migration_scope == Some(MigrationScope::FacadeCompound)
                    && file.install_mode == InstallMode::SourceGuard
                    && valid_source_guard_name(&file.name)))
                || !is_sha256_hex(&file.sha256)
                || !valid_staged_name(file)
                || file
                    .original_sha256
                    .as_ref()
                    .is_some_and(|hash| !is_sha256_hex(hash))
                || (file.name != CREDENTIALS_FILE
                    && match file.original_present {
                        Some(false) => file.original_sha256.is_some(),
                        Some(true) | None => file
                            .original_sha256
                            .as_ref()
                            .is_none_or(|hash| !is_sha256_hex(hash)),
                    })
                || (file.install_mode == InstallMode::Exact
                    && file
                        .original_sha256
                        .as_ref()
                        .is_none_or(|hash| !is_sha256_hex(hash)))
                || (file.install_mode == InstallMode::LegacyCleanup
                    && (file.migration_generation.is_some()
                        || file.expected_revision.is_some()
                        || file.name == CREDENTIALS_FILE
                        || file.name == crate::SECTION_LAYOUT_FILE))
                || (file.install_mode == InstallMode::SourceGuard
                    && (file.migration_generation.is_some()
                        || file.expected_revision.is_some()
                        || file.name == CREDENTIALS_FILE
                        || file.name == crate::SECTION_LAYOUT_FILE))
                || (file.name == CREDENTIALS_FILE && !file.sensitive)
                || (file.name != CREDENTIALS_FILE
                    && file.sensitive
                    && !matches!(
                        file.install_mode,
                        InstallMode::LegacyCleanup | InstallMode::SourceGuard
                    ))
                || (matches!(
                    file.install_mode,
                    InstallMode::LegacyCleanup | InstallMode::SourceGuard
                ) && !file.sensitive)
                || (file.expected_revision.is_some()
                    && file.name != CREDENTIALS_FILE
                    && !(exact_transaction
                        && exact_authority == Some(file.name.as_str())
                        && file.name != CONFIG_FILE))
                || file
                    .transaction_base_sha256
                    .as_ref()
                    .is_some_and(|hash| !is_sha256_hex(hash))
                || !valid_credential_ref_metadata(file)
        })
    {
        return Err(ConfigStoreError::Validation(
            "credential migration manifest is invalid".to_string(),
        ));
    }
    Ok(())
}

fn is_allowed_manifest_file(name: &str) -> bool {
    matches!(
        name,
        PROVIDERS_FILE
            | MCP_FILE
            | BROKER_FILE
            | CREDENTIALS_FILE
            | CONFIG_FILE
            | CORE_FILE
            | TOOLS_SKILLS_FILE
            | MEMORY_FILE
            | SUBAGENTS_FILE
            | NOTIFICATIONS_FILE
            | CONNECT_FILE
            | CLUSTER_FABRIC_FILE
            | ENV_FILE
            | ACCESS_CONTROL_FILE
            | HOOKS_FILE
            | MODEL_POLICY_FILE
            | MODEL_LIMITS_FILE
    ) || name == crate::SECTION_LAYOUT_FILE
        || is_legacy_cleanup_file(name)
}

fn valid_source_guard_name(name: &str) -> bool {
    crate::section_facade::preflight_source_names().contains(name)
        && !matches!(
            name,
            MANIFEST_FILE
                | JOURNAL_FILE
                | SECTION_LAYOUT_COMPLETION_FILE
                | crate::SECTION_LAYOUT_FILE
        )
}

fn is_compound_internal_derivative_source(name: &str) -> bool {
    ["bak", "bak.1", "bak.2"]
        .into_iter()
        .any(|suffix| name == format!("{CREDENTIALS_FILE}.{suffix}"))
}

fn is_legacy_cleanup_file(name: &str) -> bool {
    matches!(name, CONFIG_FILE | BROKER_FILE)
        || [
            CONFIG_FILE,
            PROVIDERS_FILE,
            MCP_FILE,
            BROKER_FILE,
            CONNECT_FILE,
            ACCESS_CONTROL_FILE,
        ]
        .into_iter()
        .any(|base| {
            ["bak", "bak.1", "bak.2"]
                .into_iter()
                .any(|suffix| name == format!("{base}.{suffix}"))
        })
}

/// Legacy root configuration is no longer an active section authority once
/// the facade marker is published. Keep its scrubbed primary/backups bound to
/// the completion evidence. Provider/MCP/connect/credential backups remain
/// active LKG generations and broker.json remains an independently managed
/// runtime source, so normal post-activation writes must not invalidate the
/// section layout.
fn is_durable_legacy_authority(name: &str) -> bool {
    durable_legacy_authority_names().contains(name)
}

fn durable_legacy_authority_names() -> BTreeSet<String> {
    std::iter::once(CONFIG_FILE.to_string())
        .chain(
            ["bak", "bak.1", "bak.2"]
                .into_iter()
                .map(|suffix| format!("{CONFIG_FILE}.{suffix}")),
        )
        .collect()
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_staged_name(file: &StagedFile) -> bool {
    if file.staged_name == file.name {
        return true;
    }
    let Some(suffix) = file
        .staged_name
        .strip_prefix(&format!("{}.rebase.", file.name))
    else {
        return false;
    };
    Uuid::parse_str(suffix).is_ok()
}

fn valid_credential_ref_metadata(file: &StagedFile) -> bool {
    if file.name != CREDENTIALS_FILE || file.install_mode != InstallMode::Exact {
        let env_names = file.touched_env_names.iter().collect::<BTreeSet<_>>();
        return file.transaction_base_sha256.is_none()
            && file.touched_credential_refs.is_empty()
            && file.required_credential_refs.is_empty()
            && (file.touched_env_names.is_empty()
                || ((file.name == CONFIG_FILE || file.name == ENV_FILE)
                    && file.install_mode == InstallMode::Exact
                    && env_names.len() == file.touched_env_names.len()
                    && file.touched_env_names.iter().all(|name| !name.is_empty())));
    }
    let touched = file.touched_credential_refs.iter().collect::<BTreeSet<_>>();
    let required = file
        .required_credential_refs
        .iter()
        .collect::<BTreeSet<_>>();
    (file.touched_credential_refs.is_empty() || file.transaction_base_sha256.is_some())
        && touched.len() == file.touched_credential_refs.len()
        && required.len() == file.required_credential_refs.len()
        && required.is_subset(&touched)
        && touched
            .iter()
            .all(|value| CredentialRef::parse((*value).clone()).is_ok())
        && file.touched_env_names.is_empty()
}

fn validated_stage_dir(data_dir: &Path, name: &str) -> ConfigStoreResult<PathBuf> {
    let path = data_dir.join(name);
    if !name.starts_with(STAGE_PREFIX) || path.parent() != Some(data_dir) {
        return Err(ConfigStoreError::Validation(
            "credential migration stage path is invalid".to_string(),
        ));
    }
    match open_managed_directory(data_dir, name) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => {
            return Err(ConfigStoreError::Validation(
                "credential migration stage path is invalid".to_string(),
            ));
        }
    }
    Ok(path)
}

fn validated_backup_dir(data_dir: &Path, transaction_id: &str) -> ConfigStoreResult<PathBuf> {
    let parsed = Uuid::parse_str(transaction_id).map_err(|_| {
        ConfigStoreError::Validation("credential migration backup path is invalid".to_string())
    })?;
    if parsed.to_string() != transaction_id {
        return Err(ConfigStoreError::Validation(
            "credential migration backup path is invalid".to_string(),
        ));
    }
    let name = format!("{BACKUP_PREFIX}{transaction_id}");
    let path = data_dir.join(&name);
    if path.parent() != Some(data_dir) {
        return Err(ConfigStoreError::Validation(
            "credential migration backup path is invalid".to_string(),
        ));
    }
    match open_managed_directory(data_dir, &name) {
        Ok(_) => Ok(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Err(
            ConfigStoreError::Validation("credential migration backup is unavailable".to_string()),
        ),
        Err(_) => Err(ConfigStoreError::Validation(
            "credential migration backup path is invalid".to_string(),
        )),
    }
}

fn remove_managed_directory_if_exists(path: &Path) -> ConfigStoreResult<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() => {
            std::fs::remove_dir_all(path)?;
            Ok(())
        }
        Ok(_) => Err(ConfigStoreError::Validation(
            "credential migration directory is invalid".to_string(),
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn next_revision(revision: u64) -> ConfigStoreResult<u64> {
    revision.checked_add(1).ok_or_else(|| {
        ConfigStoreError::Validation("configuration revision counter exhausted".to_string())
    })
}

fn remove_file_if_exists(path: &Path) -> ConfigStoreResult<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn create_private_dir(path: &Path) -> ConfigStoreResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new().mode(0o700).create(path)?;
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    #[cfg(not(unix))]
    std::fs::create_dir(path)?;
    Ok(())
}

#[cfg(unix)]
fn restrict_file_to_owner(path: &Path) -> ConfigStoreResult<()> {
    use std::os::unix::fs::PermissionsExt;
    if path.exists() {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn restrict_directory_files_to_owner(path: &Path) -> ConfigStoreResult<()> {
    #[cfg(unix)]
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            restrict_file_to_owner(&entry.path())?;
        }
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn sync_dir(path: &Path) -> ConfigStoreResult<()> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

#[cfg(test)]
fn injected_fault() -> ConfigStoreError {
    ConfigStoreError::Io(std::io::Error::other("injected migration crash"))
}

struct MigrationLock(File);

impl Drop for MigrationLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn opened_regular_file_does_not_follow_a_swapped_final_component() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.json");
        let displaced = dir.path().join("source.original.json");
        let external = dir.path().join("external.json");
        std::fs::write(&path, b"original").unwrap();
        std::fs::write(&external, b"external").unwrap();

        let mut opened = open_existing_regular_file(&path).unwrap();
        std::fs::rename(&path, &displaced).unwrap();
        symlink(&external, &path).unwrap();

        let mut bytes = Vec::new();
        opened.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"original");
        assert!(matches!(
            read_file_reject_symlink(&path),
            Err(error) if error.kind() == std::io::ErrorKind::InvalidInput
        ));
        assert_eq!(std::fs::read(&external).unwrap(), b"external");
    }

    #[cfg(unix)]
    #[test]
    fn managed_read_stays_beneath_pinned_data_and_stage_handles() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let data_dir = root.path().join("data");
        let displaced_data_dir = root.path().join("data-original");
        let external_data_dir = root.path().join("external-data");
        let stage_name = format!("{STAGE_PREFIX}{}", Uuid::new_v4());
        let stage_dir = data_dir.join(&stage_name);
        let external_stage_dir = external_data_dir.join(&stage_name);
        std::fs::create_dir_all(&stage_dir).unwrap();
        std::fs::create_dir_all(&external_stage_dir).unwrap();
        std::fs::write(stage_dir.join(CONFIG_FILE), b"original").unwrap();
        std::fs::write(external_stage_dir.join(CONFIG_FILE), b"external").unwrap();

        let data_handle = open_directory_no_follow(&data_dir).unwrap();
        std::fs::rename(&data_dir, &displaced_data_dir).unwrap();
        symlink(&external_data_dir, &data_dir).unwrap();
        let stage_handle = open_relative_no_follow(
            &data_handle,
            OsStr::new(&stage_name),
            RelativeKind::Directory,
        )
        .unwrap();

        let displaced_stage = displaced_data_dir.join(format!("{stage_name}-original"));
        std::fs::rename(displaced_data_dir.join(&stage_name), &displaced_stage).unwrap();
        symlink(&external_stage_dir, displaced_data_dir.join(&stage_name)).unwrap();
        let mut file = open_relative_no_follow(
            &stage_handle,
            OsStr::new(CONFIG_FILE),
            RelativeKind::RegularFile,
        )
        .unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();

        assert_eq!(bytes, b"original");
        assert!(matches!(
            read_managed_file(&data_dir, &stage_name, CONFIG_FILE),
            Err(error) if error.kind() == std::io::ErrorKind::InvalidInput
        ));
        assert!(matches!(
            read_file_reject_symlink(&data_dir.join(CONFIG_FILE)),
            Err(error) if error.kind() == std::io::ErrorKind::InvalidInput
        ));
        assert!(matches!(
            open_migration_lock(&data_dir.join(LOCK_FILE)),
            Err(error) if error.kind() == std::io::ErrorKind::InvalidInput
        ));
        assert!(!external_data_dir.join(LOCK_FILE).exists());
        assert_eq!(
            std::fs::read(external_stage_dir.join(CONFIG_FILE)).unwrap(),
            b"external"
        );
    }

    #[cfg(unix)]
    #[test]
    fn migration_lock_open_rejects_a_symlink_without_touching_its_target() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let external = dir.path().join("external-lock");
        let lock_path = dir.path().join(LOCK_FILE);
        std::fs::write(&external, b"sentinel").unwrap();
        symlink(&external, &lock_path).unwrap();

        assert!(matches!(
            open_migration_lock(&lock_path),
            Err(error) if error.kind() == std::io::ErrorKind::InvalidInput
        ));
        assert_eq!(std::fs::read(&external).unwrap(), b"sentinel");
    }

    #[cfg(unix)]
    #[test]
    fn migration_metadata_reads_reject_symlinks_without_following_targets() {
        use std::os::unix::fs::symlink;

        for metadata_name in [MANIFEST_FILE, JOURNAL_FILE] {
            let dir = tempfile::tempdir().unwrap();
            let external = dir.path().join("external-metadata.json");
            let external_bytes = br#"{"external":"must-survive"}"#;
            std::fs::write(&external, external_bytes).unwrap();
            symlink(&external, dir.path().join(metadata_name)).unwrap();

            let error = if metadata_name == MANIFEST_FILE {
                ensure_provider_mcp_migration_ready(dir.path()).unwrap_err()
            } else {
                discard_uncommitted(dir.path()).unwrap_err()
            };
            assert!(matches!(error, ConfigStoreError::Validation(_)));
            assert_eq!(std::fs::read(&external).unwrap(), external_bytes);
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_no_follow_helpers_accept_only_regular_files() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.json");
        std::fs::write(&source, b"regular").unwrap();
        assert_eq!(read_file_reject_symlink(&source).unwrap(), b"regular");

        let directory = dir.path().join("not-a-file");
        std::fs::create_dir(&directory).unwrap();
        assert!(matches!(
            open_existing_regular_file(&directory),
            Err(error) if error.kind() == std::io::ErrorKind::InvalidInput
        ));

        let lock = open_migration_lock(&dir.path().join(LOCK_FILE)).unwrap();
        assert!(lock.metadata().unwrap().is_file());

        let managed = dir.path().join("managed");
        std::fs::create_dir(&managed).unwrap();
        std::fs::write(managed.join(CONFIG_FILE), b"managed").unwrap();
        assert_eq!(
            read_managed_file(dir.path(), "managed", CONFIG_FILE).unwrap(),
            b"managed"
        );
    }

    fn assert_no_migration_transaction_artifacts(data_dir: &Path) {
        assert!(!data_dir.join(MANIFEST_FILE).exists());
        assert!(!data_dir.join(JOURNAL_FILE).exists());
        for entry in std::fs::read_dir(data_dir).unwrap() {
            let name = entry.unwrap().file_name();
            let name = name.to_string_lossy();
            assert!(!name.starts_with(STAGE_PREFIX));
            assert!(!name.starts_with(BACKUP_PREFIX));
        }
    }

    #[test]
    fn legacy_proxy_auth_migrates_idempotently_and_hydrates_runtime() {
        let _key = crate::encryption::set_test_encryption_key([0x91; 32]);
        let dir = tempfile::tempdir().unwrap();
        let auth = serde_json::json!({"username": "alice", "password": "proxy-secret"});
        let ciphertext = crate::encryption::encrypt(&auth.to_string()).unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "http_proxy": "http://proxy.example:8080",
                "proxy_auth_encrypted": ciphertext,
                "unknown_proxy_peer": {"kept": true}
            }))
            .unwrap(),
        )
        .unwrap();

        migrate_provider_mcp_credentials(dir.path()).unwrap();
        let first_root = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        let first_credentials = std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap();
        let root: Value = serde_json::from_slice(&first_root).unwrap();
        assert_eq!(root["proxy_auth_credential_ref"], "proxy.default.auth");
        assert!(root.get("proxy_auth").is_none());
        assert!(root.get("proxy_auth_encrypted").is_none());
        assert!(root.get("http_proxy_auth_encrypted").is_none());
        assert!(root.get("https_proxy_auth_encrypted").is_none());
        assert_eq!(root["unknown_proxy_peer"]["kept"], true);
        assert!(!String::from_utf8_lossy(&first_credentials).contains("proxy-secret"));

        let loaded = crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        let loaded_auth = loaded.proxy_auth.as_ref().expect("proxy auth hydrated");
        assert_eq!(loaded_auth.username, "alice");
        assert_eq!(loaded_auth.password, "proxy-secret");

        migrate_provider_mcp_credentials(dir.path()).unwrap();
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            first_root
        );
        assert_eq!(
            std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap(),
            first_credentials
        );
    }

    #[test]
    fn conflicting_legacy_proxy_auth_rolls_back_before_any_commit() {
        let _key = crate::encryption::set_test_encryption_key([0x92; 32]);
        let dir = tempfile::tempdir().unwrap();
        let first = crate::encryption::encrypt(
            &serde_json::json!({"username": "alice", "password": "one"}).to_string(),
        )
        .unwrap();
        let second = crate::encryption::encrypt(
            &serde_json::json!({"username": "alice", "password": "two"}).to_string(),
        )
        .unwrap();
        let original = serde_json::to_vec_pretty(&serde_json::json!({
            "proxy_auth_encrypted": first,
            "https_proxy_auth_encrypted": second
        }))
        .unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), &original).unwrap();

        let error = migrate_provider_mcp_credentials(dir.path()).unwrap_err();
        assert!(error.to_string().contains("conflicting"));
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            original
        );
        assert!(!dir.path().join(CREDENTIALS_FILE).exists());
        assert!(!dir.path().join(MANIFEST_FILE).exists());
    }

    #[test]
    fn legacy_proxy_migration_rejects_a_ref_shared_with_a_provider_instance_before_staging() {
        let _key = crate::encryption::set_test_encryption_key([0x9c; 32]);
        let dir = tempfile::tempdir().unwrap();
        let shared = CredentialRef::parse("provider.shared").unwrap();
        let store = CredentialStore::open(dir.path());
        store
            .replace(
                shared.clone(),
                "provider-secret",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let ciphertext = crate::encryption::encrypt(
            &serde_json::json!({"username": "legacy-user", "password": "legacy-password"})
                .to_string(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "proxy_auth_encrypted": ciphertext,
                "proxy_auth_credential_ref": shared.as_str(),
                "provider_instances": {
                    "shared-consumer": {
                        "provider_type": "openai",
                        "model": "gpt-test",
                        "credential_ref": shared.as_str()
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let root_before = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        let credentials_before = std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap();

        let error = migrate_provider_mcp_credentials(dir.path()).unwrap_err();
        assert!(error.to_string().contains("non-proxy consumer"));
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            root_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap(),
            credentials_before
        );
        assert_no_migration_transaction_artifacts(dir.path());
    }

    #[test]
    fn legacy_proxy_migration_rejects_generated_canonical_ref_used_by_provider_sidecar() {
        let _key = crate::encryption::set_test_encryption_key([0x9d; 32]);
        let dir = tempfile::tempdir().unwrap();
        let canonical = credential_ref("proxy", "default", "auth").unwrap();
        let store = CredentialStore::open(dir.path());
        store
            .replace(
                canonical.clone(),
                "provider-secret",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let ciphertext = crate::encryption::encrypt(
            &serde_json::json!({"username": "legacy-user", "password": "legacy-password"})
                .to_string(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "proxy_auth_encrypted": ciphertext
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join(PROVIDERS_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "openai": {
                    "model": "gpt-test",
                    "credential_ref": canonical.as_str()
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let root_before = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        let providers_before = std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap();
        let credentials_before = std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap();

        let error = migrate_provider_mcp_credentials(dir.path()).unwrap_err();
        assert!(error.to_string().contains("non-proxy consumer"));
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            root_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap(),
            providers_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap(),
            credentials_before
        );
        assert_no_migration_transaction_artifacts(dir.path());
    }

    #[test]
    fn legacy_proxy_migration_rejects_an_invalid_user_managed_canonical_target() {
        let _key = crate::encryption::set_test_encryption_key([0x9e; 32]);
        let dir = tempfile::tempdir().unwrap();
        let canonical = credential_ref("proxy", "default", "auth").unwrap();
        let store = CredentialStore::open(dir.path());
        store
            .replace(
                canonical,
                "not-a-proxy-auth-document",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let ciphertext = crate::encryption::encrypt(
            &serde_json::json!({"username": "legacy-user", "password": "legacy-password"})
                .to_string(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "proxy_auth_encrypted": ciphertext
            }))
            .unwrap(),
        )
        .unwrap();
        let root_before = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        let credentials_before = std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap();

        let error = migrate_provider_mcp_credentials(dir.path()).unwrap_err();
        assert!(error.to_string().contains("already user-managed"));
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            root_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap(),
            credentials_before
        );
        assert_no_migration_transaction_artifacts(dir.path());
    }

    #[test]
    fn legacy_proxy_migration_rejects_a_prospective_provider_instance_ref_collision() {
        let _key = crate::encryption::set_test_encryption_key([0xa0; 32]);
        let dir = tempfile::tempdir().unwrap();
        let shared = credential_ref("provider_instance", "shared", "api_key").unwrap();
        let auth = crate::ProxyAuth {
            username: "same-user".to_string(),
            password: "same-password".to_string(),
        };
        let same_secret = serde_json::to_string(&auth).unwrap();
        let ciphertext = crate::encryption::encrypt(&same_secret).unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "proxy_auth_encrypted": ciphertext,
                "proxy_auth_credential_ref": shared.as_str(),
                "provider_instances": {
                    "shared": {
                        "provider_type": "openai",
                        "model": "gpt-test",
                        "api_key": same_secret
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let root_before = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        let credentials_before = read_target_or_empty(&dir.path().join(CREDENTIALS_FILE)).unwrap();

        let error = migrate_provider_mcp_credentials(dir.path()).unwrap_err();
        assert!(error.to_string().contains("non-proxy consumer"));
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            root_before
        );
        assert_eq!(
            read_target_or_empty(&dir.path().join(CREDENTIALS_FILE)).unwrap(),
            credentials_before
        );
        assert_no_migration_transaction_artifacts(dir.path());
    }

    #[test]
    fn legacy_proxy_migration_rejects_a_prospective_provider_sidecar_ref_collision() {
        let _key = crate::encryption::set_test_encryption_key([0xa1; 32]);
        let dir = tempfile::tempdir().unwrap();
        let shared = credential_ref("provider", "openai", "api_key").unwrap();
        let auth = crate::ProxyAuth {
            username: "same-user".to_string(),
            password: "same-password".to_string(),
        };
        let same_secret = serde_json::to_string(&auth).unwrap();
        let ciphertext = crate::encryption::encrypt(&same_secret).unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "proxy_auth_encrypted": ciphertext,
                "proxy_auth_credential_ref": shared.as_str()
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join(PROVIDERS_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "openai": {
                    "model": "gpt-test",
                    "api_key": same_secret
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let root_before = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        let providers_before = std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap();
        let credentials_before = read_target_or_empty(&dir.path().join(CREDENTIALS_FILE)).unwrap();

        let error = migrate_provider_mcp_credentials(dir.path()).unwrap_err();
        assert!(error.to_string().contains("non-proxy consumer"));
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            root_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap(),
            providers_before
        );
        assert_eq!(
            read_target_or_empty(&dir.path().join(CREDENTIALS_FILE)).unwrap(),
            credentials_before
        );
        assert_no_migration_transaction_artifacts(dir.path());
    }

    #[test]
    fn proxy_auth_exact_transaction_preserves_shared_refs_on_clear() {
        let _key = crate::encryption::set_test_encryption_key([0x93; 32]);
        let dir = tempfile::tempdir().unwrap();
        let shared = credential_ref("provider", "openai", "api_key").unwrap();
        let store = CredentialStore::open(dir.path());
        store
            .replace(
                shared.clone(),
                "shared-secret",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let mut config = crate::Config::default();
        config.proxy_auth_credential_ref = Some(shared.clone());
        config.providers.openai = Some(crate::OpenAIConfig {
            credential_ref: Some(shared.clone()),
            ..crate::OpenAIConfig::default()
        });
        config.save_to_dir(dir.path().to_path_buf()).unwrap();
        config.proxy_auth = None;

        persist_proxy_auth_credential_transaction(dir.path(), &mut config).unwrap();
        assert_eq!(
            store.resolve(&shared).unwrap().unwrap().expose(),
            "shared-secret"
        );
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(root["proxy_auth_credential_ref"], "proxy.default.auth");
        assert!(root.get("proxy_auth_encrypted").is_none());
        assert!(store
            .resolve(&credential_ref("proxy", "default", "auth").unwrap())
            .unwrap()
            .is_none());
    }

    #[test]
    fn proxy_clear_tombstone_scrubs_a_late_backup_without_reimporting_secret() {
        let _key = crate::encryption::set_test_encryption_key([0xa7; 32]);
        let dir = tempfile::tempdir().unwrap();
        let cleared_ref = CredentialRef::parse("proxy.custom.auth").unwrap();
        let store = CredentialStore::open(dir.path());
        store
            .replace(
                cleared_ref.clone(),
                &serde_json::to_string(&crate::ProxyAuth {
                    username: "current-user".to_string(),
                    password: "current-password".to_string(),
                })
                .unwrap(),
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "proxy_auth_credential_ref": cleared_ref.as_str()
            }))
            .unwrap(),
        )
        .unwrap();
        let mut candidate = crate::Config::default();
        candidate.proxy_auth_credential_ref = Some(cleared_ref.clone());
        candidate.proxy_auth = None;
        let intents = BTreeSet::from(["__proxy_auth".to_string()]);

        assert!(persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterConfig),
        )
        .is_err());
        assert!(store.resolve_unchecked(&cleared_ref).unwrap().is_none());

        let late_ciphertext = crate::encryption::encrypt(
            &serde_json::to_string(&crate::ProxyAuth {
                username: "late-backup-user".to_string(),
                password: "late-backup-password".to_string(),
            })
            .unwrap(),
        )
        .unwrap();
        let late_backup = dir.path().join("config.json.bak.2");
        std::fs::write(
            &late_backup,
            serde_json::to_vec_pretty(&serde_json::json!({
                "proxy_auth_encrypted": late_ciphertext
            }))
            .unwrap(),
        )
        .unwrap();

        let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert!(outcome.resumed);
        assert!(store.resolve_unchecked(&cleared_ref).unwrap().is_none());
        let backup: Value = serde_json::from_slice(&std::fs::read(&late_backup).unwrap()).unwrap();
        assert!(backup.get("proxy_auth_encrypted").is_none());
        assert_eq!(backup["proxy_auth_credential_ref"], cleared_ref.as_str());
        let loaded = crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        assert!(loaded.proxy_auth.is_none());
        assert!(store.resolve(&cleared_ref).unwrap().is_none());
        assert!(store
            .resolve(&credential_ref("proxy", "default", "auth").unwrap())
            .unwrap()
            .is_none());
    }

    #[test]
    fn proxy_auth_replace_detaches_from_shared_provider_ref_without_overwrite() {
        let _key = crate::encryption::set_test_encryption_key([0x96; 32]);
        let dir = tempfile::tempdir().unwrap();
        let shared = credential_ref("provider", "openai", "api_key").unwrap();
        let store = CredentialStore::open(dir.path());
        store
            .replace(
                shared.clone(),
                "provider-secret",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let mut config = crate::Config::default();
        config.proxy_auth_credential_ref = Some(shared.clone());
        config.providers.openai = Some(crate::OpenAIConfig {
            credential_ref: Some(shared.clone()),
            ..crate::OpenAIConfig::default()
        });
        config.save_to_dir(dir.path().to_path_buf()).unwrap();
        config.proxy_auth = Some(crate::ProxyAuth {
            username: "proxy-user".to_string(),
            password: "proxy-password".to_string(),
        });

        persist_proxy_auth_credential_transaction(dir.path(), &mut config).unwrap();
        assert_eq!(
            store.resolve(&shared).unwrap().unwrap().expose(),
            "provider-secret"
        );
        let canonical = credential_ref("proxy", "default", "auth").unwrap();
        let proxy: crate::ProxyAuth =
            serde_json::from_str(store.resolve(&canonical).unwrap().unwrap().expose()).unwrap();
        assert_eq!(proxy.username, "proxy-user");
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(root["proxy_auth_credential_ref"], canonical.as_str());
    }

    #[test]
    fn occupied_canonical_proxy_ref_fails_closed_without_any_write() {
        let _key = crate::encryption::set_test_encryption_key([0x97; 32]);
        let dir = tempfile::tempdir().unwrap();
        let shared = credential_ref("provider", "openai", "api_key").unwrap();
        let canonical = credential_ref("proxy", "default", "auth").unwrap();
        let store = CredentialStore::open(dir.path());
        store
            .replace(
                shared.clone(),
                "openai-secret",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        store
            .replace(
                canonical.clone(),
                "anthropic-secret",
                crate::CredentialSource::User,
                store.revision().unwrap(),
            )
            .unwrap();
        let mut config = crate::Config::default();
        config.proxy_auth_credential_ref = Some(shared.clone());
        config.providers.openai = Some(crate::OpenAIConfig {
            credential_ref: Some(shared),
            ..crate::OpenAIConfig::default()
        });
        config.providers.anthropic = Some(crate::AnthropicConfig {
            credential_ref: Some(canonical),
            ..crate::AnthropicConfig::default()
        });
        config.save_to_dir(dir.path().to_path_buf()).unwrap();
        config.proxy_auth = Some(crate::ProxyAuth {
            username: "proxy-user".to_string(),
            password: "proxy-password".to_string(),
        });
        let root_before = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        let credentials_before = std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap();

        let error = persist_proxy_auth_credential_transaction(dir.path(), &mut config).unwrap_err();
        assert!(error
            .to_string()
            .contains("canonical reference is occupied"));
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            root_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap(),
            credentials_before
        );
    }

    #[test]
    fn custom_proxy_only_ref_replace_and_clear_resume_without_touching_other_credentials() {
        let _key = crate::encryption::set_test_encryption_key([0x9b; 32]);
        for replacement in [
            Some(crate::ProxyAuth {
                username: "replacement-user".to_string(),
                password: "replacement-password".to_string(),
            }),
            None,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let custom = CredentialRef::parse("proxy.historical.custom").unwrap();
            let unrelated = credential_ref("provider", "anthropic", "api_key").unwrap();
            let store = CredentialStore::open(dir.path());
            store
                .replace(
                    custom.clone(),
                    &serde_json::to_string(&crate::ProxyAuth {
                        username: "original-user".to_string(),
                        password: "original-password".to_string(),
                    })
                    .unwrap(),
                    crate::CredentialSource::User,
                    0,
                )
                .unwrap();
            store
                .replace(
                    unrelated.clone(),
                    "unrelated-provider-secret",
                    crate::CredentialSource::User,
                    store.revision().unwrap(),
                )
                .unwrap();
            let mut initial = crate::Config::default();
            initial.proxy_auth_credential_ref = Some(custom.clone());
            initial.save_to_dir(dir.path().to_path_buf()).unwrap();
            let mut candidate =
                crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
            candidate.proxy_auth = replacement.clone();
            let intents = BTreeSet::from(["__proxy_auth".to_string()]);

            assert!(persist_provider_credential_transaction_inner(
                dir.path(),
                &mut candidate,
                &intents,
                Some(MigrationFault::AfterManifest),
            )
            .is_err());
            assert!(ensure_provider_mcp_migration_ready(dir.path()).is_err());

            let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
            assert!(outcome.resumed);
            ensure_provider_mcp_migration_ready(dir.path()).unwrap();
            let root: Value =
                serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap())
                    .unwrap();
            assert_eq!(root["proxy_auth_credential_ref"], custom.as_str());
            match replacement {
                Some(expected) => {
                    let resolved: crate::ProxyAuth =
                        serde_json::from_str(store.resolve(&custom).unwrap().unwrap().expose())
                            .unwrap();
                    assert_eq!(resolved.username, expected.username);
                    assert_eq!(resolved.password, expected.password);
                }
                None => assert!(store.resolve(&custom).unwrap().is_none()),
            }
            assert_eq!(
                store.resolve(&unrelated).unwrap().unwrap().expose(),
                "unrelated-provider-secret"
            );
        }
    }

    #[test]
    fn committed_proxy_auth_transaction_recovers_without_partial_publication() {
        let _key = crate::encryption::set_test_encryption_key([0x94; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.proxy_auth = Some(crate::ProxyAuth {
            username: "recover-user".to_string(),
            password: "recover-secret".to_string(),
        });
        let intents = BTreeSet::from(["__proxy_auth".to_string()]);

        assert!(persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterManifest),
        )
        .is_err());
        let before_recovery: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert!(before_recovery.get("proxy_auth_credential_ref").is_none());

        let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert!(outcome.resumed);
        let loaded = crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        let auth = loaded
            .proxy_auth
            .as_ref()
            .expect("committed proxy auth recovered");
        assert_eq!(auth.username, "recover-user");
        assert_eq!(auth.password, "recover-secret");
        let root = std::fs::read_to_string(dir.path().join(CONFIG_FILE)).unwrap();
        let credentials = std::fs::read_to_string(dir.path().join(CREDENTIALS_FILE)).unwrap();
        for secret in ["recover-user", "recover-secret"] {
            assert!(!root.contains(secret));
            assert!(!credentials.contains(secret));
        }
    }

    #[test]
    fn committed_proxy_transaction_rebases_valid_external_root_edit() {
        let _key = crate::encryption::set_test_encryption_key([0x98; 32]);
        let dir = tempfile::tempdir().unwrap();
        let mut initial = crate::Config::default();
        initial.http_proxy = "http://before.example:8080".to_string();
        initial.save_to_dir(dir.path().to_path_buf()).unwrap();
        let mut candidate = initial.clone();
        candidate.proxy_auth = Some(crate::ProxyAuth {
            username: "race-user".to_string(),
            password: "race-password".to_string(),
        });
        let intents = BTreeSet::from(["__proxy_auth".to_string()]);

        assert!(persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterManifest),
        )
        .is_err());
        let mut external: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        external["http_proxy"] = Value::String("http://external.example:9090".to_string());
        external["external_edit"] = serde_json::json!({"preserved": true});
        AtomicFileStore::new(dir.path().join(CONFIG_FILE))
            .write_bytes_without_backup(&serde_json::to_vec_pretty(&external).unwrap())
            .unwrap();

        let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert!(outcome.resumed);
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(root["http_proxy"], "http://external.example:9090");
        assert_eq!(root["external_edit"]["preserved"], true);
        assert_eq!(root["proxy_auth_credential_ref"], "proxy.default.auth");
        assert!(root.get("proxy_auth_encrypted").is_none());
        let loaded = crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        let auth = loaded.proxy_auth.as_ref().expect("proxy auth hydrated");
        assert_eq!(auth.username, "race-user");
        assert_eq!(auth.password, "race-password");
    }

    #[test]
    fn proxy_transaction_narrow_patch_preserves_root_edit_present_before_staging() {
        let _key = crate::encryption::set_test_encryption_key([0xa2; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.proxy_auth = Some(crate::ProxyAuth {
            username: "narrow-user".to_string(),
            password: "narrow-password".to_string(),
        });
        let mut external: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        external["http_proxy"] = Value::String("http://external-before-stage:9090".to_string());
        external["external_before_stage"] = serde_json::json!({"preserved": true});
        AtomicFileStore::new(dir.path().join(CONFIG_FILE))
            .write_bytes_without_backup(&serde_json::to_vec_pretty(&external).unwrap())
            .unwrap();

        persist_proxy_auth_credential_transaction(dir.path(), &mut candidate).unwrap();

        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(root["http_proxy"], "http://external-before-stage:9090");
        assert_eq!(root["external_before_stage"]["preserved"], true);
        assert_eq!(root["proxy_auth_credential_ref"], "proxy.default.auth");
    }

    #[test]
    fn committed_proxy_transaction_rebases_again_after_manifested_rebase_loses_cas() {
        let _key = crate::encryption::set_test_encryption_key([0x99; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.proxy_auth = Some(crate::ProxyAuth {
            username: "repeat-user".to_string(),
            password: "repeat-password".to_string(),
        });
        let intents = BTreeSet::from(["__proxy_auth".to_string()]);

        assert!(persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterManifest),
        )
        .is_err());
        let mut external: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        external["external_rebase_generation"] = Value::Number(1_u64.into());
        AtomicFileStore::new(dir.path().join(CONFIG_FILE))
            .write_bytes_without_backup(&serde_json::to_vec_pretty(&external).unwrap())
            .unwrap();

        assert!(migrate_with_fault(
            dir.path(),
            MigrationFault::AfterExactProxyConfigRebaseManifestExternalWrite,
        )
        .is_err());
        let after_lost_cas: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(after_lost_cas["external_rebase_generation"], 2);
        assert!(after_lost_cas.get("proxy_auth_credential_ref").is_none());
        assert!(ensure_provider_mcp_migration_ready(dir.path()).is_err());

        let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert!(outcome.resumed);
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(root["external_rebase_generation"], 2);
        assert_eq!(root["proxy_auth_credential_ref"], "proxy.default.auth");
        let loaded = crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        let auth = loaded.proxy_auth.as_ref().expect("proxy auth hydrated");
        assert_eq!(auth.username, "repeat-user");
        assert_eq!(auth.password, "repeat-password");
    }

    #[test]
    fn committed_proxy_transaction_rejects_new_provider_instance_consumer_before_credential_write()
    {
        let _key = crate::encryption::set_test_encryption_key([0x9a; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.proxy_auth = Some(crate::ProxyAuth {
            username: "must-not-install".to_string(),
            password: "must-not-install-secret".to_string(),
        });
        let intents = BTreeSet::from(["__proxy_auth".to_string()]);

        assert!(persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterManifest),
        )
        .is_err());
        let mut external: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        external["provider_instances"] = serde_json::json!({
            "racer": {
                "provider_type": "openai",
                "model": "gpt-test",
                "credential_ref": "proxy.default.auth"
            }
        });
        AtomicFileStore::new(dir.path().join(CONFIG_FILE))
            .write_bytes_without_backup(&serde_json::to_vec_pretty(&external).unwrap())
            .unwrap();
        let root_before = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        let credentials_before = read_target_or_empty(&dir.path().join(CREDENTIALS_FILE)).unwrap();

        let error = migrate_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("non-proxy consumer"));
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            root_before
        );
        assert_eq!(
            read_target_or_empty(&dir.path().join(CREDENTIALS_FILE)).unwrap(),
            credentials_before
        );
        assert!(ensure_provider_mcp_migration_ready(dir.path()).is_ok());
        assert_no_migration_transaction_artifacts(dir.path());
    }

    #[test]
    fn committed_proxy_transaction_rolls_back_installed_credential_for_new_consumer() {
        let _key = crate::encryption::set_test_encryption_key([0xa3; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.proxy_auth = Some(crate::ProxyAuth {
            username: "partial-user".to_string(),
            password: "partial-password".to_string(),
        });
        let intents = BTreeSet::from(["__proxy_auth".to_string()]);

        assert!(persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterCredentials),
        )
        .is_err());
        let reference = credential_ref("proxy", "default", "auth").unwrap();
        assert!(CredentialStore::open(dir.path())
            .resolve_unchecked(&reference)
            .unwrap()
            .is_some());
        let mut external: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        external["provider_instances"] = serde_json::json!({
            "racer": {
                "provider_type": "openai",
                "model": "gpt-test",
                "credential_ref": "proxy.default.auth"
            }
        });
        AtomicFileStore::new(dir.path().join(CONFIG_FILE))
            .write_bytes_without_backup(&serde_json::to_vec_pretty(&external).unwrap())
            .unwrap();

        let error = migrate_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("non-proxy consumer"));
        assert!(CredentialStore::open(dir.path())
            .resolve_unchecked(&reference)
            .unwrap()
            .is_none());
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(
            root["provider_instances"]["racer"]["credential_ref"],
            "proxy.default.auth"
        );
        assert!(ensure_provider_mcp_migration_ready(dir.path()).is_ok());
        assert_no_migration_transaction_artifacts(dir.path());
    }

    #[test]
    fn proxy_abort_rolls_back_config_installed_before_new_consumer() {
        let _key = crate::encryption::set_test_encryption_key([0xa6; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.proxy_auth = Some(crate::ProxyAuth {
            username: "fully-installed-user".to_string(),
            password: "fully-installed-password".to_string(),
        });
        let intents = BTreeSet::from(["__proxy_auth".to_string()]);
        assert!(persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterConfig),
        )
        .is_err());
        let mut external: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(external["proxy_auth_credential_ref"], "proxy.default.auth");
        external["provider_instances"] = serde_json::json!({
            "racer": {
                "provider_type": "openai",
                "model": "gpt-test",
                "credential_ref": "proxy.default.auth"
            }
        });
        AtomicFileStore::new(dir.path().join(CONFIG_FILE))
            .write_bytes_without_backup(&serde_json::to_vec_pretty(&external).unwrap())
            .unwrap();

        let error = migrate_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("non-proxy consumer"));
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert!(root.get("proxy_auth_credential_ref").is_none());
        assert_eq!(
            root["provider_instances"]["racer"]["credential_ref"],
            "proxy.default.auth"
        );
        let reference = credential_ref("proxy", "default", "auth").unwrap();
        assert!(CredentialStore::open(dir.path())
            .resolve_unchecked(&reference)
            .unwrap()
            .is_none());
        assert!(ensure_provider_mcp_migration_ready(dir.path()).is_ok());
        assert_no_migration_transaction_artifacts(dir.path());
    }

    #[test]
    fn proxy_abort_preserves_a_later_same_ref_credential_winner() {
        let _key = crate::encryption::set_test_encryption_key([0xa5; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.proxy_auth = Some(crate::ProxyAuth {
            username: "transaction-user".to_string(),
            password: "transaction-password".to_string(),
        });
        let intents = BTreeSet::from(["__proxy_auth".to_string()]);
        assert!(persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterCredentials),
        )
        .is_err());

        let reference = credential_ref("proxy", "default", "auth").unwrap();
        let winner = serde_json::to_string(&crate::ProxyAuth {
            username: "winner-user".to_string(),
            password: "winner-password".to_string(),
        })
        .unwrap();
        let store = CredentialStore::open(dir.path());
        let revision = store.revision_unchecked().unwrap();
        store
            .replace_unchecked(
                reference.clone(),
                &winner,
                crate::CredentialSource::User,
                revision,
            )
            .unwrap();
        let mut external: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        external["provider_instances"] = serde_json::json!({
            "racer": {
                "provider_type": "openai",
                "model": "gpt-test",
                "credential_ref": "proxy.default.auth"
            }
        });
        AtomicFileStore::new(dir.path().join(CONFIG_FILE))
            .write_bytes_without_backup(&serde_json::to_vec_pretty(&external).unwrap())
            .unwrap();

        let error = migrate_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("non-proxy consumer"));
        assert_eq!(
            store
                .resolve_unchecked(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            winner
        );
        assert!(ensure_provider_mcp_migration_ready(dir.path()).is_ok());
        assert_no_migration_transaction_artifacts(dir.path());
    }

    #[test]
    fn committed_proxy_transaction_aborts_on_same_domain_root_change() {
        let _key = crate::encryption::set_test_encryption_key([0xa4; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.proxy_auth = Some(crate::ProxyAuth {
            username: "losing-user".to_string(),
            password: "losing-password".to_string(),
        });
        let intents = BTreeSet::from(["__proxy_auth".to_string()]);

        assert!(persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterManifest),
        )
        .is_err());
        let mut external: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        external["proxy_auth_credential_ref"] = Value::String("proxy.external.auth".to_string());
        external["same_domain_winner"] = Value::Bool(true);
        AtomicFileStore::new(dir.path().join(CONFIG_FILE))
            .write_bytes_without_backup(&serde_json::to_vec_pretty(&external).unwrap())
            .unwrap();

        let error = migrate_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(error
            .to_string()
            .contains("proxy authentication metadata changed"));
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(root["proxy_auth_credential_ref"], "proxy.external.auth");
        assert_eq!(root["same_domain_winner"], true);
        let losing_ref = credential_ref("proxy", "default", "auth").unwrap();
        assert!(CredentialStore::open(dir.path())
            .resolve_unchecked(&losing_ref)
            .unwrap()
            .is_none());
        assert!(ensure_provider_mcp_migration_ready(dir.path()).is_ok());
        assert_no_migration_transaction_artifacts(dir.path());
    }

    #[test]
    fn backup_only_proxy_auth_is_stored_before_backup_is_scrubbed() {
        let _key = crate::encryption::set_test_encryption_key([0x95; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), b"{}").unwrap();
        let ciphertext = crate::encryption::encrypt(
            &serde_json::json!({"username": "backup-user", "password": "backup-secret"})
                .to_string(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("config.json.bak"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "proxy_auth_encrypted": ciphertext
            }))
            .unwrap(),
        )
        .unwrap();

        migrate_provider_mcp_credentials(dir.path()).unwrap();
        let reference = credential_ref("proxy", "default", "auth").unwrap();
        let stored = CredentialStore::open(dir.path())
            .resolve(&reference)
            .unwrap()
            .unwrap();
        let auth: crate::ProxyAuth = serde_json::from_str(stored.expose()).unwrap();
        assert_eq!(auth.password, "backup-secret");
        let backup = std::fs::read_to_string(dir.path().join("config.json.bak")).unwrap();
        assert!(!backup.contains("proxy_auth_encrypted"));
        assert!(!backup.contains("backup-secret"));
        assert!(backup.contains("proxy_auth_credential_ref"));
    }

    #[test]
    fn user_managed_proxy_auth_scrubs_stale_backup_and_keeps_provider_sidecar_ready() {
        let _key = crate::encryption::set_test_encryption_key([0x9f; 32]);
        let dir = tempfile::tempdir().unwrap();
        let canonical = credential_ref("proxy", "default", "auth").unwrap();
        let authoritative = serde_json::to_string(&crate::ProxyAuth {
            username: "authoritative-user".to_string(),
            password: "authoritative-password".to_string(),
        })
        .unwrap();
        let store = CredentialStore::open(dir.path());
        store
            .replace(
                canonical.clone(),
                &authoritative,
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "proxy_auth_credential_ref": canonical.as_str(),
                "providers": {
                    "openai": {"model": "root-stale-model"}
                }
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join(PROVIDERS_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "openai": {
                    "api_key": "provider-sidecar-secret",
                    "model": "sidecar-authoritative-model"
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let backup_path = dir.path().join("config.json.bak");
        std::fs::write(
            &backup_path,
            serde_json::to_vec_pretty(&serde_json::json!({
                "proxy_auth_encrypted": "invalid-stale-ciphertext-must-not-be-decrypted"
            }))
            .unwrap(),
        )
        .unwrap();

        let loaded = crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        let auth = loaded
            .proxy_auth
            .as_ref()
            .expect("user proxy auth hydrated");
        assert_eq!(auth.username, "authoritative-user");
        assert_eq!(auth.password, "authoritative-password");
        let openai = loaded.providers.openai.as_ref().expect("sidecar loaded");
        assert_eq!(openai.model.as_deref(), Some("sidecar-authoritative-model"));
        assert_eq!(openai.api_key, "provider-sidecar-secret");
        assert_eq!(
            store.resolve(&canonical).unwrap().unwrap().expose(),
            authoritative
        );
        assert_eq!(
            store.status(&canonical).unwrap().source,
            crate::CredentialSource::User
        );
        let backup: Value = serde_json::from_slice(&std::fs::read(&backup_path).unwrap()).unwrap();
        assert!(backup.get("proxy_auth_encrypted").is_none());
        assert_eq!(backup["proxy_auth_credential_ref"], canonical.as_str());
        ensure_provider_mcp_migration_ready(dir.path()).unwrap();
    }

    #[test]
    fn backup_only_proxy_auth_shared_with_provider_instance_fails_before_credential_commit() {
        let _key = crate::encryption::set_test_encryption_key([0xa2; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), b"{}").unwrap();
        let shared = CredentialRef::parse("backup.shared.proxy").unwrap();
        let auth = crate::ProxyAuth {
            username: "same-user".to_string(),
            password: "same-password".to_string(),
        };
        let same_secret = serde_json::to_string(&auth).unwrap();
        let ciphertext = crate::encryption::encrypt(&same_secret).unwrap();
        let backup_path = dir.path().join("config.json.bak");
        std::fs::write(
            &backup_path,
            serde_json::to_vec_pretty(&serde_json::json!({
                "proxy_auth_encrypted": ciphertext,
                "proxy_auth_credential_ref": shared.as_str(),
                "provider_instances": {
                    "shared": {
                        "provider_type": "openai",
                        "model": "gpt-test",
                        "credential_ref": shared.as_str(),
                        "api_key": same_secret
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let backup_before = std::fs::read(&backup_path).unwrap();
        let credentials_before = read_target_or_empty(&dir.path().join(CREDENTIALS_FILE)).unwrap();

        let error = migrate_provider_mcp_credentials(dir.path()).unwrap_err();
        assert!(error.to_string().contains("non-proxy consumer"));
        assert_eq!(std::fs::read(&backup_path).unwrap(), backup_before);
        assert_eq!(
            read_target_or_empty(&dir.path().join(CREDENTIALS_FILE)).unwrap(),
            credentials_before
        );
        assert_no_migration_transaction_artifacts(dir.path());
    }

    fn install_fixture(dir: &Path) {
        let provider = include_bytes!("../tests/fixtures/config_migration/providers-legacy.json");
        let mut mcp: Value = serde_json::from_slice(include_bytes!(
            "../tests/fixtures/config_migration/mcp-legacy.json"
        ))
        .unwrap();
        // Use real ciphertext for one legacy entry while keeping the fixture
        // stable and human-auditable.
        mcp["data"]["stdio unsafe/name"]["env_encrypted"]["PRIVATE TOKEN"] =
            Value::String(crate::encryption::encrypt("stdio-cipher-secret").unwrap());
        std::fs::write(dir.join(PROVIDERS_FILE), provider).unwrap();
        std::fs::write(dir.join(MCP_FILE), serde_json::to_vec_pretty(&mcp).unwrap()).unwrap();
    }

    fn assert_migrated(dir: &Path) {
        let providers = std::fs::read_to_string(dir.join(PROVIDERS_FILE)).unwrap();
        let mcp = std::fs::read_to_string(dir.join(MCP_FILE)).unwrap();
        let credentials = std::fs::read_to_string(dir.join(CREDENTIALS_FILE)).unwrap();
        for secret in [
            "sk-provider-plain",
            "Bearer mcp-header-secret",
            "stdio-plain-secret",
            "stdio-cipher-secret",
        ] {
            assert!(!providers.contains(secret));
            assert!(!mcp.contains(secret));
            assert!(!credentials.contains(secret));
        }
        assert!(!providers.contains("api_key_encrypted"));
        assert!(!mcp.contains("env_encrypted"));
        assert!(!mcp.contains("headers_encrypted"));
        assert!(providers.contains("credential_ref"));
        assert!(mcp.contains("env_credential_refs"));
        assert!(mcp.contains("header_credential_refs"));
        assert!(providers.contains("provider-unknown-kept"));
        assert!(mcp.contains("mcp-unknown-kept"));

        let store = CredentialStore::open(dir);
        assert_eq!(
            store
                .resolve(&credential_ref("provider", "openai", "api_key").unwrap())
                .unwrap()
                .unwrap()
                .expose(),
            "sk-provider-plain"
        );
        assert_eq!(
            store
                .resolve(&credential_ref("mcp", "stdio unsafe/name", "env_PRIVATE TOKEN").unwrap())
                .unwrap()
                .unwrap()
                .expose(),
            "stdio-cipher-secret"
        );
        assert_no_legacy_plaintext(dir);
        assert!(!std::fs::read_dir(dir)
            .unwrap()
            .filter_map(Result::ok)
            .any(|entry| {
                entry.file_type().is_ok_and(|kind| kind.is_dir())
                    && is_strict_transaction_dir_name(&entry.file_name().to_string_lossy())
            }));
    }

    fn assert_no_legacy_plaintext(dir: &Path) {
        fn visit(path: &Path, needles: &[&str]) {
            for entry in std::fs::read_dir(path).unwrap().filter_map(Result::ok) {
                let path = entry.path();
                if path.is_dir() {
                    visit(&path, needles);
                } else if let Ok(content) = std::fs::read_to_string(&path) {
                    for needle in needles {
                        assert!(
                            !content.contains(needle),
                            "legacy plaintext remained in {}",
                            path.display()
                        );
                    }
                }
            }
        }
        visit(
            dir,
            &[
                "sk-provider-plain",
                "Bearer mcp-header-secret",
                "stdio-plain-secret",
                "stdio-cipher-secret",
            ],
        );
    }

    #[test]
    fn real_legacy_fixtures_migrate_without_secret_or_unknown_field_loss() {
        let _key = crate::encryption::set_test_encryption_key([0x61; 32]);
        let dir = tempfile::tempdir().unwrap();
        install_fixture(dir.path());
        let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert!(outcome.migrated_credentials >= 3);
        assert_migrated(dir.path());
        assert!(!std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .any(|entry| entry
                .file_name()
                .to_string_lossy()
                .starts_with(BACKUP_PREFIX)));
    }

    #[cfg(unix)]
    #[test]
    fn pending_transaction_backups_are_encrypted_owner_only_and_cleaned_on_retry() {
        use std::os::unix::fs::PermissionsExt;

        let _key = crate::encryption::set_test_encryption_key([0x69; 32]);
        let dir = tempfile::tempdir().unwrap();
        install_fixture(dir.path());
        std::fs::set_permissions(
            dir.path().join(PROVIDERS_FILE),
            std::fs::Permissions::from_mode(0o640),
        )
        .unwrap();
        std::fs::set_permissions(
            dir.path().join(MCP_FILE),
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        let source_modes = [PROVIDERS_FILE, MCP_FILE].map(|source| {
            std::fs::metadata(dir.path().join(source))
                .unwrap()
                .permissions()
                .mode()
                & 0o777
        });
        assert!(migrate_with_fault(dir.path(), MigrationFault::AfterManifest).is_err());

        let backup = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .find(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(BACKUP_PREFIX)
            })
            .expect("pending transaction has a backup directory")
            .path();
        assert_eq!(
            std::fs::metadata(&backup).unwrap().permissions().mode() & 0o777,
            0o700
        );
        for entry in std::fs::read_dir(&backup).unwrap().filter_map(Result::ok) {
            assert_eq!(
                entry.metadata().unwrap().permissions().mode() & 0o777,
                0o600
            );
            let content = std::fs::read_to_string(entry.path()).unwrap();
            if !content.is_empty() {
                assert!(content.contains("ciphertext"));
            }
            for plaintext in [
                "sk-provider-plain",
                "Bearer mcp-header-secret",
                "stdio-plain-secret",
            ] {
                assert!(!content.contains(plaintext));
            }
        }
        for (source, expected_mode) in [PROVIDERS_FILE, MCP_FILE].into_iter().zip(source_modes) {
            assert_eq!(
                std::fs::metadata(dir.path().join(source))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                expected_mode
            );
        }

        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert_migrated(dir.path());
    }

    #[test]
    fn crash_before_manifest_is_discarded_and_retried_idempotently() {
        let _key = crate::encryption::set_test_encryption_key([0x62; 32]);
        let dir = tempfile::tempdir().unwrap();
        install_fixture(dir.path());
        let providers_before = std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap();
        let mcp_before = std::fs::read(dir.path().join(MCP_FILE)).unwrap();
        assert!(migrate_with_fault(dir.path(), MigrationFault::AfterJournal).is_err());
        assert_eq!(
            std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap(),
            providers_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(MCP_FILE)).unwrap(),
            mcp_before
        );
        assert!(!dir.path().join(CREDENTIALS_FILE).exists());
        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert_migrated(dir.path());
    }

    #[test]
    fn crash_after_staging_cleans_only_strict_unreferenced_orphans_on_every_retry() {
        let _key = crate::encryption::set_test_encryption_key([0x6f; 32]);
        let dir = tempfile::tempdir().unwrap();
        install_fixture(dir.path());
        let non_uuid_stage = dir.path().join(format!("{STAGE_PREFIX}not-a-uuid"));
        let non_uuid_backup = dir.path().join(format!("{BACKUP_PREFIX}not-a-uuid"));
        std::fs::create_dir(&non_uuid_stage).unwrap();
        std::fs::create_dir(&non_uuid_backup).unwrap();

        #[cfg(unix)]
        let (external, external_link) = {
            use std::os::unix::fs::symlink;
            let external = tempfile::tempdir().unwrap();
            std::fs::write(external.path().join("must-survive"), b"sentinel").unwrap();
            let external_link = dir.path().join(format!("{STAGE_PREFIX}{}", Uuid::new_v4()));
            symlink(external.path(), &external_link).unwrap();
            (external, external_link)
        };

        let strict_dir_count = || {
            std::fs::read_dir(dir.path())
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
                .filter(|entry| {
                    is_strict_transaction_dir_name(&entry.file_name().to_string_lossy())
                })
                .count()
        };
        assert!(migrate_with_fault(dir.path(), MigrationFault::AfterStaging).is_err());
        assert_eq!(strict_dir_count(), 2);
        assert!(migrate_with_fault(dir.path(), MigrationFault::AfterStaging).is_err());
        assert_eq!(strict_dir_count(), 2, "orphan pairs must not accumulate");

        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert_eq!(strict_dir_count(), 0);
        assert!(non_uuid_stage.exists());
        assert!(non_uuid_backup.exists());
        #[cfg(unix)]
        {
            assert!(std::fs::symlink_metadata(&external_link).is_ok());
            assert_eq!(
                std::fs::read(external.path().join("must-survive")).unwrap(),
                b"sentinel"
            );
        }
        assert_migrated(dir.path());
    }

    #[test]
    fn non_not_found_manifest_and_journal_read_errors_fail_closed_and_redacted() {
        let _key = crate::encryption::set_test_encryption_key([0x70; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("config.json"),
            br#"{"providers":{"openai":{"model":"root-lkg"}}}"#,
        )
        .unwrap();
        install_fixture(dir.path());
        std::fs::create_dir(dir.path().join(MANIFEST_FILE)).unwrap();

        for error in [
            ensure_provider_mcp_migration_ready(dir.path()).unwrap_err(),
            recover_committed(dir.path(), Some(MigrationFault::None)).unwrap_err(),
        ] {
            let rendered = error.to_string();
            assert!(rendered.contains("migration"));
            assert!(!rendered.contains(dir.path().to_string_lossy().as_ref()));
            assert!(!rendered.contains("sk-provider-plain"));
        }
        let loaded = crate::Config::from_data_dir_without_env(Some(dir.path().to_path_buf()));
        assert_eq!(
            loaded.providers.openai.as_ref().unwrap().model.as_deref(),
            Some("root-lkg"),
            "provider target must not be consumed when manifest cannot be read"
        );

        let journal_dir = tempfile::tempdir().unwrap();
        install_fixture(journal_dir.path());
        std::fs::create_dir(journal_dir.path().join(JOURNAL_FILE)).unwrap();
        for error in [
            discard_uncommitted(journal_dir.path()).unwrap_err(),
            migrate_with_fault(journal_dir.path(), MigrationFault::None).unwrap_err(),
        ] {
            let rendered = error.to_string();
            assert!(rendered.contains("migration"));
            assert!(!rendered.contains(journal_dir.path().to_string_lossy().as_ref()));
            assert!(!rendered.contains("sk-provider-plain"));
        }
    }

    #[test]
    fn non_not_found_provider_and_mcp_read_errors_fail_closed() {
        let _key = crate::encryption::set_test_encryption_key([0x74; 32]);
        for target in [PROVIDERS_FILE, MCP_FILE] {
            let dir = tempfile::tempdir().unwrap();
            std::fs::create_dir(dir.path().join(target)).unwrap();

            let error = migrate_with_fault(dir.path(), MigrationFault::None).unwrap_err();
            assert!(matches!(error, ConfigStoreError::Io(_)));
            assert!(!dir.path().join(MANIFEST_FILE).exists());
            assert!(!dir.path().join(CREDENTIALS_FILE).exists());
        }
    }

    #[test]
    fn migration_rejects_exhausted_section_and_credential_revisions() {
        let _key = crate::encryption::set_test_encryption_key([0x75; 32]);

        let section_dir = tempfile::tempdir().unwrap();
        std::fs::write(
            section_dir.path().join(PROVIDERS_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema_version": 1,
                "revision": u64::MAX,
                "data": {"openai": {"api_key": "sk-overflow"}}
            }))
            .unwrap(),
        )
        .unwrap();
        let error = migrate_with_fault(section_dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("revision counter exhausted"));
        assert!(!section_dir.path().join(MANIFEST_FILE).exists());
        assert!(
            std::fs::read_to_string(section_dir.path().join(PROVIDERS_FILE))
                .unwrap()
                .contains("sk-overflow")
        );

        let credential_dir = tempfile::tempdir().unwrap();
        std::fs::write(
            credential_dir.path().join(PROVIDERS_FILE),
            br#"{"openai":{"api_key":"sk-credential-overflow"}}"#,
        )
        .unwrap();
        std::fs::write(
            credential_dir.path().join(CREDENTIALS_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema_version": 1,
                "revision": u64::MAX,
                "data": {"entries": {}}
            }))
            .unwrap(),
        )
        .unwrap();
        let error = migrate_with_fault(credential_dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("revision counter exhausted"));
        assert!(!credential_dir.path().join(MANIFEST_FILE).exists());
    }

    #[test]
    fn exhausted_credential_revision_allows_secret_free_cleanup_when_user_value_wins() {
        let _key = crate::encryption::set_test_encryption_key([0x78; 32]);
        let dir = tempfile::tempdir().unwrap();
        let store = CredentialStore::open(dir.path());
        let reference = credential_ref("provider", "openai", "api_key").unwrap();
        store
            .replace(
                reference.clone(),
                "user-authoritative",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let mut credentials: Value =
            serde_json::from_slice(&std::fs::read(store.path()).unwrap()).unwrap();
        credentials["revision"] = Value::from(u64::MAX);
        std::fs::write(
            store.path(),
            serde_json::to_vec_pretty(&credentials).unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join(PROVIDERS_FILE),
            br#"{"openai":{"api_key":"stale-legacy","model":"kept"}}"#,
        )
        .unwrap();

        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();

        assert_eq!(store.revision().unwrap(), u64::MAX);
        assert_eq!(
            store.resolve(&reference).unwrap().unwrap().expose(),
            "user-authoritative"
        );
        let providers = std::fs::read_to_string(dir.path().join(PROVIDERS_FILE)).unwrap();
        assert!(!providers.contains("stale-legacy"));
        assert!(providers.contains("credential_ref"));
    }

    #[cfg(unix)]
    #[test]
    fn committed_manifest_rejects_a_symlinked_stage_directory() {
        use std::os::unix::fs::symlink;

        let _key = crate::encryption::set_test_encryption_key([0x76; 32]);
        let dir = tempfile::tempdir().unwrap();
        install_fixture(dir.path());
        assert!(migrate_with_fault(dir.path(), MigrationFault::AfterManifest).is_err());
        let manifest: MigrationManifest =
            serde_json::from_slice(&std::fs::read(dir.path().join(MANIFEST_FILE)).unwrap())
                .unwrap();
        let stage = dir.path().join(&manifest.stage_dir);
        std::fs::remove_dir_all(&stage).unwrap();
        let external = tempfile::tempdir().unwrap();
        std::fs::write(external.path().join("must-survive"), b"sentinel").unwrap();
        symlink(external.path(), &stage).unwrap();

        let error = migrate_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("stage path is invalid"));
        assert_eq!(
            std::fs::read(external.path().join("must-survive")).unwrap(),
            b"sentinel"
        );
    }

    #[cfg(unix)]
    #[test]
    fn exact_transaction_rejects_a_symlinked_backup_directory() {
        use std::os::unix::fs::symlink;

        let _key = crate::encryption::set_test_encryption_key([0x7d; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let (mut candidate, intents) = provider_transaction_candidate("sk-backup-symlink");
        assert!(persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterManifest),
        )
        .is_err());
        let manifest: MigrationManifest =
            serde_json::from_slice(&std::fs::read(dir.path().join(MANIFEST_FILE)).unwrap())
                .unwrap();
        let backup = dir
            .path()
            .join(format!("{BACKUP_PREFIX}{}", manifest.transaction_id));
        std::fs::remove_dir_all(&backup).unwrap();
        let external = tempfile::tempdir().unwrap();
        std::fs::write(external.path().join("must-survive"), b"sentinel").unwrap();
        symlink(external.path(), &backup).unwrap();

        let error = migrate_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("backup path is invalid"));
        assert_eq!(
            std::fs::read(external.path().join("must-survive")).unwrap(),
            b"sentinel"
        );
    }

    #[test]
    fn crashes_at_every_post_manifest_durable_boundary_resume_before_reads() {
        for fault in [
            MigrationFault::AfterManifest,
            MigrationFault::AfterCredentials,
            MigrationFault::AfterProviders,
            MigrationFault::AfterMcp,
        ] {
            let _key = crate::encryption::set_test_encryption_key([0x63; 32]);
            let dir = tempfile::tempdir().unwrap();
            install_fixture(dir.path());
            assert!(migrate_with_fault(dir.path(), fault).is_err());
            let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
            assert!(outcome.resumed);
            assert_migrated(dir.path());
        }
    }

    #[test]
    fn retry_never_overwrites_a_user_replaced_credential() {
        let _key = crate::encryption::set_test_encryption_key([0x64; 32]);
        let dir = tempfile::tempdir().unwrap();
        install_fixture(dir.path());
        assert!(migrate_with_fault(dir.path(), MigrationFault::AfterManifest).is_err());
        let store = CredentialStore::open(dir.path());
        let reference = credential_ref("provider", "openai", "api_key").unwrap();
        store
            .replace_unchecked(
                reference.clone(),
                "user-newer",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert_eq!(
            store.resolve(&reference).unwrap().unwrap().expose(),
            "user-newer"
        );
        assert_migrated_except_provider_value(dir.path());
    }

    #[test]
    fn unversioned_provider_and_mcp_rewrites_advance_migrated_secret_generation() {
        let _key = crate::encryption::set_test_encryption_key([0x6a; 32]);
        let dir = tempfile::tempdir().unwrap();
        install_fixture(dir.path());
        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();

        std::fs::write(
            dir.path().join(PROVIDERS_FILE),
            br#"{"openai":{"api_key":"sk-provider-from-old-binary","model":"old-binary"}}"#,
        )
        .unwrap();
        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        let provider_ref = credential_ref("provider", "openai", "api_key").unwrap();
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&provider_ref)
                .unwrap()
                .unwrap()
                .expose(),
            "sk-provider-from-old-binary"
        );

        let mut old_mcp: Value = serde_json::from_slice(include_bytes!(
            "../tests/fixtures/config_migration/mcp-legacy.json"
        ))
        .unwrap();
        old_mcp["data"]["stdio unsafe/name"]["env"]["PUBLIC TOKEN"] =
            Value::String("mcp-from-old-binary".to_string());
        old_mcp["data"]["stdio unsafe/name"]["env_encrypted"] = serde_json::json!({});
        std::fs::write(
            dir.path().join(MCP_FILE),
            serde_json::to_vec_pretty(&old_mcp).unwrap(),
        )
        .unwrap();
        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        let mcp_ref = credential_ref("mcp", "stdio unsafe/name", "env_PUBLIC TOKEN").unwrap();
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&mcp_ref)
                .unwrap()
                .unwrap()
                .expose(),
            "mcp-from-old-binary"
        );
    }

    #[test]
    fn section_write_racing_a_committed_manifest_is_rebased_without_lost_update() {
        let _key = crate::encryption::set_test_encryption_key([0x66; 32]);
        let dir = tempfile::tempdir().unwrap();
        install_fixture(dir.path());
        assert!(migrate_with_fault(dir.path(), MigrationFault::AfterManifest).is_err());

        std::fs::write(
            dir.path().join(PROVIDERS_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema_version": 1,
                "revision": 99,
                "data": {
                    "openai": {
                        "api_key": "sk-racing-newer",
                        "model": "newer-model",
                        "new_unknown": "must-survive-rebase"
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert!(outcome.resumed);
        let providers: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap())
                .unwrap();
        assert_eq!(providers["revision"], 100);
        assert_eq!(providers["data"]["openai"]["model"], "newer-model");
        assert_eq!(
            providers["data"]["openai"]["new_unknown"],
            "must-survive-rebase"
        );
        assert!(providers["data"]["openai"].get("api_key").is_none());
        let store = CredentialStore::open(dir.path());
        assert_eq!(
            store
                .resolve(&credential_ref("provider", "openai", "api_key").unwrap())
                .unwrap()
                .unwrap()
                .expose(),
            "sk-racing-newer"
        );
    }

    #[test]
    fn committed_provider_instance_migration_rejects_a_corrupt_root_rebase() {
        let _key = crate::encryption::set_test_encryption_key([0x86; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "provider_instances": {
                    "work": {"provider_type": "openai", "api_key": "sk-staged"}
                }
            }))
            .unwrap(),
        )
        .unwrap();
        assert!(migrate_with_fault(dir.path(), MigrationFault::AfterManifest).is_err());
        std::fs::write(dir.path().join(CONFIG_FILE), b"{concurrent-broken").unwrap();

        assert!(migrate_with_fault(dir.path(), MigrationFault::None).is_err());
        assert!(ensure_provider_mcp_migration_ready(dir.path()).is_err());
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            b"{concurrent-broken"
        );
    }

    #[test]
    fn invalid_root_validation_is_ignored_without_partial_secret_extraction() {
        let _key = crate::encryption::set_test_encryption_key([0x87; 32]);
        let dir = tempfile::tempdir().unwrap();
        let store = CredentialStore::open(dir.path());
        let provider_ref = credential_ref("provider", "openai", "api_key").unwrap();
        store
            .replace(
                provider_ref.clone(),
                "sk-sidecar-survives-invalid-root",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        std::fs::write(
            dir.path().join(PROVIDERS_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "openai": {
                    "credential_ref": provider_ref.as_str(),
                    "model": "gpt-sidecar"
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let invalid_root = serde_json::to_vec_pretty(&serde_json::json!({
            "provider_instances": {
                "first": {
                    "provider_type": "openai",
                    "api_key": "must-not-be-partially-extracted"
                },
                "invalid": {
                    "provider_type": "anthropic",
                    "api_key": {"not": "a string"}
                }
            }
        }))
        .unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), &invalid_root).unwrap();

        let loaded = crate::Config::from_data_dir_without_env(Some(dir.path().to_path_buf()));
        assert_eq!(
            loaded.providers.openai.as_ref().unwrap().api_key,
            "sk-sidecar-survives-invalid-root"
        );
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            invalid_root
        );
        assert!(store
            .resolve(&credential_ref("provider_instance", "first", "api_key").unwrap())
            .unwrap()
            .is_none());
    }

    #[test]
    fn rebase_crashes_keep_manifest_stage_pair_valid_and_never_replay_older_secret() {
        for fault in [
            MigrationFault::AfterRebaseCredentialCommit,
            MigrationFault::AfterRebaseStageWrite,
            MigrationFault::AfterRebaseManifest,
        ] {
            let _key = crate::encryption::set_test_encryption_key([0x67; 32]);
            let dir = tempfile::tempdir().unwrap();
            install_fixture(dir.path());
            assert!(migrate_with_fault(dir.path(), MigrationFault::AfterManifest).is_err());
            std::fs::write(
                dir.path().join(PROVIDERS_FILE),
                serde_json::to_vec_pretty(&serde_json::json!({
                    "schema_version": 1,
                    "revision": 99,
                    "data": {"openai": {
                        "api_key": "sk-rebase-newer",
                        "model": "rebase-model"
                    }}
                }))
                .unwrap(),
            )
            .unwrap();
            assert!(migrate_with_fault(dir.path(), fault).is_err());
            migrate_with_fault(dir.path(), MigrationFault::None).unwrap();

            let store = CredentialStore::open(dir.path());
            assert_eq!(
                store
                    .resolve(&credential_ref("provider", "openai", "api_key").unwrap())
                    .unwrap()
                    .unwrap()
                    .expose(),
                "sk-rebase-newer",
                "old staged credential replayed after {fault:?}"
            );
            let providers: Value =
                serde_json::from_slice(&std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap())
                    .unwrap();
            assert_eq!(providers["revision"], 100);
            assert_eq!(providers["data"]["openai"]["model"], "rebase-model");
        }
    }

    #[test]
    fn manifest_rejects_stage_path_traversal_and_duplicate_targets() {
        let transaction_id = Uuid::new_v4().to_string();
        let file = StagedFile {
            name: CREDENTIALS_FILE.to_string(),
            staged_name: CREDENTIALS_FILE.to_string(),
            sha256: "0".repeat(64),
            original_sha256: None,
            original_present: None,
            migration_generation: None,
            sensitive: true,
            install_mode: InstallMode::Migration,
            expected_revision: None,
            transaction_base_sha256: None,
            touched_credential_refs: Vec::new(),
            required_credential_refs: Vec::new(),
            touched_env_names: Vec::new(),
        };
        let traversal = MigrationManifest {
            version: MIGRATION_VERSION,
            transaction_id: transaction_id.clone(),
            stage_dir: format!("{STAGE_PREFIX}{transaction_id}/../../outside"),
            state: MigrationState::Pending,
            exact_scope: None,
            migration_scope: None,
            source_attestation: BTreeMap::new(),
            files: vec![file.clone()],
        };
        assert!(validate_manifest(&traversal).is_err());

        let invalid_hash = MigrationManifest {
            version: MIGRATION_VERSION,
            transaction_id: transaction_id.clone(),
            stage_dir: format!("{STAGE_PREFIX}{transaction_id}"),
            state: MigrationState::Pending,
            exact_scope: None,
            migration_scope: None,
            source_attestation: BTreeMap::new(),
            files: vec![StagedFile {
                sha256: "G".repeat(64),
                ..file.clone()
            }],
        };
        assert!(validate_manifest(&invalid_hash).is_err());

        let duplicate = MigrationManifest {
            version: MIGRATION_VERSION,
            transaction_id: transaction_id.clone(),
            stage_dir: format!("{STAGE_PREFIX}{transaction_id}"),
            state: MigrationState::Pending,
            exact_scope: None,
            migration_scope: None,
            source_attestation: BTreeMap::new(),
            files: vec![file.clone(), file],
        };
        assert!(validate_manifest(&duplicate).is_err());
    }

    fn provider_transaction_candidate(secret: &str) -> (crate::Config, BTreeSet<String>) {
        let mut config = crate::Config::default();
        config.provider = "openai".to_string();
        config.providers.openai = Some(crate::OpenAIConfig {
            api_key: secret.to_string(),
            model: Some("transaction-model".to_string()),
            ..Default::default()
        });
        (config, BTreeSet::from(["openai".to_string()]))
    }

    fn default_section_split_plan(data_dir: &Path) -> crate::section_facade::SectionSplitPlan {
        let projection = crate::SectionProjection::from_config(
            &crate::Config::default(),
            crate::ModelLimitsSection::default(),
        )
        .unwrap();
        let source_hashes = crate::section_facade::migration_source_hashes(data_dir).unwrap();
        projection
            .into_migration_plan(data_dir, source_hashes)
            .unwrap()
    }

    #[test]
    fn recovery_only_resumes_pending_section_split_and_returns_true() {
        let _key = crate::encryption::set_test_encryption_key([0x45; 32]);
        let dir = tempfile::tempdir().unwrap();
        let plan = default_section_split_plan(dir.path());
        assert!(install_section_split_migration_with_fault(
            dir.path(),
            plan,
            SectionSplitTestFault::Manifest,
        )
        .is_err());

        assert!(recover_pending_config_transaction(dir.path()).unwrap());
        assert!(crate::section_layout_is_active(dir.path()).unwrap());
        assert!(dir.path().join(SECTION_LAYOUT_COMPLETION_FILE).exists());
        assert!(!dir.path().join(JOURNAL_FILE).exists());
        let manifest: MigrationManifest =
            serde_json::from_slice(&std::fs::read(dir.path().join(MANIFEST_FILE)).unwrap())
                .unwrap();
        assert_eq!(manifest.state, MigrationState::Complete);
        assert!(!recover_pending_config_transaction(dir.path()).unwrap());
    }

    #[test]
    fn complete_section_split_recovery_recreates_missing_completion_ledger() {
        let _key = crate::encryption::set_test_encryption_key([0x4a; 32]);
        let dir = tempfile::tempdir().unwrap();
        let plan = default_section_split_plan(dir.path());
        assert!(install_section_split_migration_with_fault(
            dir.path(),
            plan,
            SectionSplitTestFault::LayoutMarker,
        )
        .is_err());

        let manifest_path = dir.path().join(MANIFEST_FILE);
        let mut manifest: MigrationManifest =
            serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
        manifest.state = MigrationState::Complete;
        write_manifest(manifest_path, &manifest).unwrap();
        assert!(!dir.path().join(SECTION_LAYOUT_COMPLETION_FILE).exists());
        assert!(!crate::section_layout_is_active(dir.path()).unwrap());

        assert!(!recover_pending_config_transaction(dir.path()).unwrap());
        assert!(dir.path().join(SECTION_LAYOUT_COMPLETION_FILE).exists());
        assert!(crate::section_layout_is_active(dir.path()).unwrap());
        assert!(!dir.path().join(JOURNAL_FILE).exists());
        assert!(!dir.path().join(&manifest.stage_dir).exists());
    }

    #[test]
    fn section_split_manifest_recovery_rejects_a_nonfinal_layout_marker() {
        let _key = crate::encryption::set_test_encryption_key([0x4b; 32]);
        let dir = tempfile::tempdir().unwrap();
        let plan = default_section_split_plan(dir.path());
        assert!(install_section_split_migration_with_fault(
            dir.path(),
            plan,
            SectionSplitTestFault::Manifest,
        )
        .is_err());

        let manifest_path = dir.path().join(MANIFEST_FILE);
        let mut manifest: MigrationManifest =
            serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
        let marker_index = manifest
            .files
            .iter()
            .position(|file| file.name == crate::SECTION_LAYOUT_FILE)
            .unwrap();
        let marker = manifest.files.remove(marker_index);
        manifest.files.insert(0, marker);
        write_manifest(manifest_path, &manifest).unwrap();
        let before = crate::SECTION_DESCRIPTORS
            .iter()
            .map(|descriptor| {
                (
                    descriptor.file_name,
                    read_target_or_empty(&dir.path().join(descriptor.file_name)).unwrap(),
                )
            })
            .collect::<BTreeMap<_, _>>();

        assert!(recover_pending_config_transaction(dir.path()).is_err());
        for (name, bytes) in before {
            assert_eq!(read_target_or_empty(&dir.path().join(name)).unwrap(), bytes);
        }
        assert!(!dir.path().join(crate::SECTION_LAYOUT_FILE).exists());
        assert!(!dir.path().join(SECTION_LAYOUT_COMPLETION_FILE).exists());
    }

    #[test]
    fn complete_section_split_recovery_keeps_evidence_when_marker_is_unverifiable() {
        for missing in [false, true] {
            let _key = crate::encryption::set_test_encryption_key(if missing {
                [0x47; 32]
            } else {
                [0x48; 32]
            });
            let dir = tempfile::tempdir().unwrap();
            let plan = default_section_split_plan(dir.path());
            assert!(install_section_split_migration_with_fault(
                dir.path(),
                plan,
                SectionSplitTestFault::LayoutMarker,
            )
            .is_err());

            let manifest_path = dir.path().join(MANIFEST_FILE);
            let mut manifest: MigrationManifest =
                serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
            manifest.state = MigrationState::Complete;
            write_manifest(manifest_path.clone(), &manifest).unwrap();
            let stage_dir = dir.path().join(&manifest.stage_dir);
            assert!(stage_dir.exists());
            assert!(dir.path().join(JOURNAL_FILE).exists());
            if missing {
                std::fs::remove_file(dir.path().join(crate::SECTION_LAYOUT_FILE)).unwrap();
            } else {
                std::fs::write(
                    dir.path().join(crate::SECTION_LAYOUT_FILE),
                    br#"{"forged":true}"#,
                )
                .unwrap();
            }

            assert!(recover_pending_config_transaction(dir.path()).is_err());
            assert!(manifest_path.exists());
            assert!(stage_dir.exists());
            assert!(dir.path().join(JOURNAL_FILE).exists());
            assert!(!dir.path().join(SECTION_LAYOUT_COMPLETION_FILE).exists());
        }
    }

    #[test]
    fn later_exact_transaction_can_advance_credentials_without_refreshing_layout_marker() {
        let _key = crate::encryption::set_test_encryption_key([0x49; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::migrate_config_facade_layout(dir.path()).unwrap();
        let marker_path = dir.path().join(crate::SECTION_LAYOUT_FILE);
        let marker_before = std::fs::read(&marker_path).unwrap();
        let ledger_path = dir.path().join(SECTION_LAYOUT_COMPLETION_FILE);
        let ledger_before = std::fs::read(&ledger_path).unwrap();
        let ledger_modified_before = std::fs::metadata(&ledger_path).unwrap().modified().unwrap();
        let completion =
            crate::section_facade::validate_completed_section_layout_marker(&marker_before)
                .unwrap();
        let split_manifest: MigrationManifest =
            serde_json::from_slice(&std::fs::read(dir.path().join(MANIFEST_FILE)).unwrap())
                .unwrap();
        assert_eq!(
            split_manifest.files.last().map(|file| file.name.as_str()),
            Some(crate::SECTION_LAYOUT_FILE)
        );

        let mut config =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        config.proxy_auth = Some(crate::ProxyAuth {
            username: "layout-user".to_string(),
            password: "layout-password".to_string(),
        });
        persist_proxy_auth_credential_transaction(dir.path(), &mut config).unwrap();

        assert_eq!(std::fs::read(&marker_path).unwrap(), marker_before);
        assert_eq!(std::fs::read(&ledger_path).unwrap(), ledger_before);
        assert_eq!(
            std::fs::metadata(&ledger_path).unwrap().modified().unwrap(),
            ledger_modified_before
        );
        let current_credentials = std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap();
        assert!(
            CredentialStore::validate_section_document_revision(&current_credentials).unwrap()
                > completion.members[CREDENTIALS_FILE].revision
        );
        let exact_manifest: MigrationManifest =
            serde_json::from_slice(&std::fs::read(dir.path().join(MANIFEST_FILE)).unwrap())
                .unwrap();
        assert_eq!(exact_manifest.state, MigrationState::Complete);
        assert_eq!(exact_manifest.migration_scope, None);
        assert_ne!(exact_manifest.transaction_id, completion.transaction_id);
        assert!(crate::section_layout_is_active(dir.path()).unwrap());
    }

    fn active_facade_config_and_credential_revision(data_dir: &Path) -> (crate::Config, u64) {
        let facade = crate::ConfigFacade::open(data_dir).unwrap();
        let revision = facade.registry().credentials.snapshot().revision;
        (facade.effective_config(), revision)
    }

    fn active_facade_config_and_cluster_revision(data_dir: &Path) -> (crate::Config, u64) {
        let facade = crate::ConfigFacade::open(data_dir).unwrap();
        let revision = facade.registry().cluster_fabric.snapshot().revision;
        (facade.effective_config(), revision)
    }

    fn cluster_test_node(id: &str, auth: crate::SshAuth) -> crate::Node {
        crate::Node {
            id: id.to_string(),
            label: id.to_string(),
            placement: crate::NodePlacement::Ssh(crate::SshTarget {
                host: "127.0.0.1".to_string(),
                port: 22,
                username: "deploy".to_string(),
                auth,
                host_key_fingerprint: None,
            }),
            trust_level: crate::TrustLevel::Trusted,
            deploy: crate::DeployProfile::default(),
            state: None,
            enabled: true,
        }
    }

    fn password_intents(
        action: crate::ClusterCredentialAction,
    ) -> crate::ClusterNodeCredentialIntents {
        crate::ClusterNodeCredentialIntents {
            password: action,
            private_key: crate::ClusterCredentialAction::Clear,
            passphrase: crate::ClusterCredentialAction::Clear,
        }
    }

    fn assert_active_exact_manifest(data_dir: &Path, expected: ExactTransactionScope) {
        let manifest: MigrationManifest =
            serde_json::from_slice(&std::fs::read(data_dir.join(MANIFEST_FILE)).unwrap()).unwrap();
        assert_eq!(manifest.state, MigrationState::Complete);
        assert_eq!(manifest.exact_scope, Some(expected));
        assert_eq!(manifest.files.len(), 2);
        assert_eq!(manifest.files[0].name, CREDENTIALS_FILE);
        assert_eq!(manifest.files[1].name, authoritative_section_file(expected));
        assert!(!manifest.files.iter().any(|file| file.name == CONFIG_FILE));
        assert!(manifest.files[1].expected_revision.is_some());
    }

    #[test]
    fn active_mcp_exact_transaction_recovers_metadata_and_credentials_together() {
        let _key = crate::encryption::set_test_encryption_key([0x56; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::migrate_config_facade_layout(dir.path()).unwrap();
        let root_before = read_target_or_empty(&dir.path().join(CONFIG_FILE)).unwrap();
        let facade = crate::ConfigFacade::open(dir.path()).unwrap();
        let expected_revision = facade.registry().mcp.snapshot().revision;
        let mut candidate = facade.effective_config();
        let reference = credential_ref("mcp", "recoverable", "env_TOKEN").unwrap();
        candidate
            .mcp
            .servers
            .push(bamboo_domain::mcp_config::McpServerConfig {
                id: "recoverable".to_string(),
                name: Some("Recoverable MCP".to_string()),
                enabled: false,
                transport: bamboo_domain::mcp_config::TransportConfig::Stdio(
                    bamboo_domain::mcp_config::StdioConfig {
                        command: "unused".to_string(),
                        args: vec!["--safe".to_string()],
                        cwd: Some("/tmp/mcp-recovery".to_string()),
                        env: std::collections::HashMap::from([(
                            "TOKEN".to_string(),
                            "recoverable-mcp-secret".to_string(),
                        )]),
                        env_encrypted: Default::default(),
                        env_credential_refs: std::collections::HashMap::from([(
                            "TOKEN".to_string(),
                            reference.as_str().to_string(),
                        )]),
                        startup_timeout_ms: 100,
                    },
                ),
                request_timeout_ms: 100,
                healthcheck_interval_ms: 100,
                reconnect: Default::default(),
                allowed_tools: vec!["read".to_string()],
                denied_tools: vec!["delete".to_string()],
            });
        let intents = BTreeSet::from([reference.clone()]);

        assert!(persist_mcp_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            expected_revision,
            Some(MigrationFault::AfterCredentials),
        )
        .is_err());
        assert!(ensure_provider_mcp_migration_ready(dir.path()).is_err());

        let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert!(outcome.resumed);
        ensure_provider_mcp_migration_ready(dir.path()).unwrap();
        assert_active_exact_manifest(dir.path(), ExactTransactionScope::Mcp);
        let fresh = crate::ConfigFacade::open(dir.path()).unwrap();
        assert_eq!(fresh.registry().mcp.snapshot().revision, 1);
        assert_eq!(fresh.registry().credentials.snapshot().revision, 1);
        let mut hydrated = fresh.effective_config();
        hydrated
            .hydrate_mcp_credentials_from_store(dir.path())
            .unwrap();
        let bamboo_domain::mcp_config::TransportConfig::Stdio(stdio) =
            &hydrated.mcp.servers[0].transport
        else {
            panic!("expected stdio MCP transport");
        };
        assert_eq!(stdio.args, vec!["--safe"]);
        assert_eq!(stdio.cwd.as_deref(), Some("/tmp/mcp-recovery"));
        assert_eq!(stdio.env["TOKEN"], "recoverable-mcp-secret");
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "recoverable-mcp-secret"
        );
        assert_eq!(
            read_target_or_empty(&dir.path().join(CONFIG_FILE)).unwrap(),
            root_before
        );
    }

    #[test]
    fn active_proxy_exact_transaction_is_visible_to_fresh_facades_after_set_and_clear() {
        let _key = crate::encryption::set_test_encryption_key([0x51; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::migrate_config_facade_layout(dir.path()).unwrap();
        let root_before = read_target_or_empty(&dir.path().join(CONFIG_FILE)).unwrap();

        let (mut set, revision) = active_facade_config_and_credential_revision(dir.path());
        set.proxy_auth = Some(crate::ProxyAuth {
            username: "active-user".to_string(),
            password: "active-password".to_string(),
        });
        persist_proxy_auth_credential_transaction_at_revision(dir.path(), &mut set, revision)
            .unwrap();
        assert_active_exact_manifest(dir.path(), ExactTransactionScope::ProxyAuth);
        let reference = crate::credential_ref("proxy", "default", "auth").unwrap();
        let (mut clear, revision) = active_facade_config_and_credential_revision(dir.path());
        assert_eq!(clear.proxy_auth_credential_ref.as_ref(), Some(&reference));
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            serde_json::to_string(&crate::ProxyAuth {
                username: "active-user".to_string(),
                password: "active-password".to_string(),
            })
            .unwrap()
        );

        clear.proxy_auth = None;
        persist_proxy_auth_credential_transaction_at_revision(dir.path(), &mut clear, revision)
            .unwrap();
        let (fresh, _) = active_facade_config_and_credential_revision(dir.path());
        assert!(fresh.proxy_auth_credential_ref.is_none());
        assert!(CredentialStore::open(dir.path())
            .resolve(&reference)
            .unwrap()
            .is_none());
        assert_eq!(
            read_target_or_empty(&dir.path().join(CONFIG_FILE)).unwrap(),
            root_before
        );
        assert!(crate::section_layout_is_active(dir.path()).unwrap());
    }

    #[test]
    fn active_env_exact_transaction_is_visible_to_fresh_facades_after_set_and_clear() {
        let _key = crate::encryption::set_test_encryption_key([0x52; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::migrate_config_facade_layout(dir.path()).unwrap();
        let root_before = read_target_or_empty(&dir.path().join(CONFIG_FILE)).unwrap();
        let intents = BTreeSet::from(["ACTIVE_TOKEN".to_string()]);

        let (mut set, revision) = active_facade_config_and_credential_revision(dir.path());
        set.env_vars.push(crate::EnvVarEntry {
            name: "ACTIVE_TOKEN".to_string(),
            value: "active-env-secret".to_string(),
            secret: true,
            value_encrypted: None,
            credential_ref: None,
            configured: true,
            description: Some("active metadata".to_string()),
        });
        persist_env_var_credential_transaction_at_revision(
            dir.path(),
            &mut set,
            &intents,
            revision,
        )
        .unwrap();
        assert_active_exact_manifest(dir.path(), ExactTransactionScope::EnvVars);
        let reference = crate::credential_ref("env", "ACTIVE_TOKEN", "value").unwrap();
        let (mut clear, revision) = active_facade_config_and_credential_revision(dir.path());
        let entry = clear
            .env_vars
            .iter_mut()
            .find(|entry| entry.name == "ACTIVE_TOKEN")
            .unwrap();
        assert!(entry.configured);
        assert_eq!(entry.credential_ref.as_ref(), Some(&reference));
        entry.value.clear();
        entry.configured = false;
        persist_env_var_credential_transaction_at_revision(
            dir.path(),
            &mut clear,
            &intents,
            revision,
        )
        .unwrap();
        let (fresh, _) = active_facade_config_and_credential_revision(dir.path());
        let entry = fresh
            .env_vars
            .iter()
            .find(|entry| entry.name == "ACTIVE_TOKEN")
            .unwrap();
        assert!(!entry.configured);
        assert!(entry.credential_ref.is_none());
        assert!(CredentialStore::open(dir.path())
            .resolve(&reference)
            .unwrap()
            .is_none());
        assert_eq!(
            read_target_or_empty(&dir.path().join(CONFIG_FILE)).unwrap(),
            root_before
        );
    }

    #[test]
    fn active_notification_exact_transaction_is_visible_to_fresh_facades_after_set_and_clear() {
        let _key = crate::encryption::set_test_encryption_key([0x53; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::migrate_config_facade_layout(dir.path()).unwrap();
        let root_before = read_target_or_empty(&dir.path().join(CONFIG_FILE)).unwrap();
        let intents = BTreeSet::from(["ntfy".to_string()]);

        let (mut set, revision) = active_facade_config_and_credential_revision(dir.path());
        set.notifications.ntfy.enabled = true;
        set.notifications.ntfy.topic = "active-topic".to_string();
        set.notifications.ntfy.token = Some("active-ntfy-secret".to_string());
        set.notifications.ntfy.configured = true;
        persist_notification_credential_transaction_at_revision(
            dir.path(),
            &mut set,
            &intents,
            revision,
        )
        .unwrap();
        assert_active_exact_manifest(dir.path(), ExactTransactionScope::Notifications);
        let reference = crate::credential_ref("notification", "ntfy", "token").unwrap();
        let (mut clear, revision) = active_facade_config_and_credential_revision(dir.path());
        assert_eq!(
            clear.notifications.ntfy.credential_ref.as_ref(),
            Some(&reference)
        );
        clear.notifications.ntfy.token = None;
        clear.notifications.ntfy.configured = false;
        persist_notification_credential_transaction_at_revision(
            dir.path(),
            &mut clear,
            &intents,
            revision,
        )
        .unwrap();
        let (fresh, _) = active_facade_config_and_credential_revision(dir.path());
        assert!(!fresh.notifications.ntfy.configured);
        assert!(fresh.notifications.ntfy.credential_ref.is_none());
        assert!(CredentialStore::open(dir.path())
            .resolve(&reference)
            .unwrap()
            .is_none());
        assert_eq!(
            read_target_or_empty(&dir.path().join(CONFIG_FILE)).unwrap(),
            root_before
        );
    }

    #[test]
    fn active_connect_exact_transaction_is_visible_to_fresh_facades_after_set_metadata_and_clear() {
        let _key = crate::encryption::set_test_encryption_key([0x55; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::migrate_config_facade_layout(dir.path()).unwrap();
        let root_before = read_target_or_empty(&dir.path().join(CONFIG_FILE)).unwrap();

        let (mut set, revision) = active_facade_config_and_credential_revision(dir.path());
        set.connect.platforms.push(
            serde_json::from_value(serde_json::json!({
                "id": "telegram-main",
                "type": "telegram",
                "token": "active-connect-secret",
                "allow_from": ["user-1"]
            }))
            .unwrap(),
        );
        let mut intents = crate::patch::ConnectSecretIntents::default();
        intents.token.insert(0);
        persist_connect_credential_transaction_at_revision(
            dir.path(),
            &mut set,
            &intents,
            revision,
        )
        .unwrap();
        assert_active_exact_manifest(dir.path(), ExactTransactionScope::Connect);
        let reference = crate::credential_ref("connect", "telegram-main", "token").unwrap();
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "active-connect-secret"
        );

        let (mut metadata, revision) = active_facade_config_and_credential_revision(dir.path());
        let platform = &mut metadata.connect.platforms[0];
        assert!(platform.token_configured);
        assert_eq!(platform.token_credential_ref.as_ref(), Some(&reference));
        platform.allow_from.push("user-2".to_string());
        persist_connect_credential_transaction_at_revision(
            dir.path(),
            &mut metadata,
            &crate::patch::ConnectSecretIntents::default(),
            revision,
        )
        .unwrap();
        let (mut clear, revision) = active_facade_config_and_credential_revision(dir.path());
        assert_eq!(
            clear.connect.platforms[0].allow_from,
            vec!["user-1".to_string(), "user-2".to_string()]
        );
        clear.connect.platforms[0].token = None;
        let mut clear_intents = crate::patch::ConnectSecretIntents::default();
        clear_intents.token.insert(0);
        persist_connect_credential_transaction_at_revision(
            dir.path(),
            &mut clear,
            &clear_intents,
            revision,
        )
        .unwrap();
        let (fresh, _) = active_facade_config_and_credential_revision(dir.path());
        assert!(!fresh.connect.platforms[0].token_configured);
        assert!(fresh.connect.platforms[0].token_credential_ref.is_none());
        assert!(CredentialStore::open(dir.path())
            .resolve(&reference)
            .unwrap()
            .is_none());
        assert_eq!(
            read_target_or_empty(&dir.path().join(CONFIG_FILE)).unwrap(),
            root_before
        );
    }

    #[test]
    fn active_access_exact_transaction_is_visible_to_fresh_facades() {
        let _key = crate::encryption::set_test_encryption_key([0x55; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::migrate_config_facade_layout(dir.path()).unwrap();
        let root_before = read_target_or_empty(&dir.path().join(CONFIG_FILE)).unwrap();

        let (mut candidate, revision) = active_facade_config_and_credential_revision(dir.path());
        candidate.access_control = Some(crate::AccessControlConfig {
            password_enabled: true,
            password_hash: Some("a".repeat(64)),
            password_salt: Some("01".repeat(16)),
            password_credential_ref: None,
            password_configured: false,
            updated_at: None,
            devices: vec![crate::DeviceCredential {
                device_id: "bamboo_access01".to_string(),
                label: "Phone".to_string(),
                token_hash: "b".repeat(64),
                token_salt: "02".repeat(16),
                token_credential_ref: None,
                token_configured: false,
                created_at: "2026-07-21T00:00:00Z".to_string(),
                last_used_at: None,
                revoked: false,
            }],
        });
        persist_access_control_credential_transaction_at_revision(
            dir.path(),
            &mut candidate,
            true,
            &BTreeSet::from(["bamboo_access01".to_string()]),
            revision,
        )
        .unwrap();
        assert_active_exact_manifest(dir.path(), ExactTransactionScope::AccessControl);
        let facade = crate::ConfigFacade::open(dir.path()).unwrap();
        let mut fresh = facade.effective_config();
        let access = fresh.access_control.as_ref().unwrap();
        assert!(access.password_configured);
        assert!(access.password_hash.is_none());
        assert!(access.devices[0].token_configured);
        assert!(access.devices[0].token_hash.is_empty());
        fresh
            .hydrate_access_control_credentials_from_store(dir.path())
            .unwrap();
        assert_eq!(
            fresh
                .access_control
                .as_ref()
                .unwrap()
                .password_hash
                .as_deref(),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );
        assert_eq!(
            fresh.access_control.as_ref().unwrap().devices[0].token_hash,
            "b".repeat(64)
        );
        assert_eq!(
            read_target_or_empty(&dir.path().join(CONFIG_FILE)).unwrap(),
            root_before
        );
    }

    #[test]
    fn active_cluster_exact_transaction_is_visible_to_fresh_facades_after_set_and_clear() {
        let _key = crate::encryption::set_test_encryption_key([0x54; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::migrate_config_facade_layout(dir.path()).unwrap();
        let root_before = read_target_or_empty(&dir.path().join(CONFIG_FILE)).unwrap();

        let (mut set, revision) = active_facade_config_and_cluster_revision(dir.path());
        set.cluster_fabric = serde_json::from_value(serde_json::json!({
            "nodes": [{
                "id": "active-node",
                "label": "active-node",
                "placement": {
                    "type": "ssh",
                    "host": "127.0.0.1",
                    "username": "deploy",
                    "auth": {"method": "password"}
                }
            }]
        }))
        .unwrap();
        let intents = BTreeMap::from([(
            "active-node".to_string(),
            crate::ClusterNodeCredentialIntents {
                password: crate::ClusterCredentialAction::Replace("active-ssh-secret".to_string()),
                private_key: crate::ClusterCredentialAction::Clear,
                passphrase: crate::ClusterCredentialAction::Clear,
            },
        )]);
        persist_cluster_fabric_credential_transaction_at_revision(
            dir.path(),
            &mut set,
            &intents,
            revision,
        )
        .unwrap();
        assert_active_exact_manifest(dir.path(), ExactTransactionScope::ClusterFabric);
        let reference = crate::cluster_password_credential_ref("active-node").unwrap();
        let (mut clear, revision) = active_facade_config_and_cluster_revision(dir.path());
        assert!(clear.cluster_fabric.credential_refs["active-node"].password_configured);
        clear.cluster_fabric.nodes.clear();
        let intents = BTreeMap::from([(
            "active-node".to_string(),
            crate::ClusterNodeCredentialIntents::clear_all(),
        )]);
        persist_cluster_fabric_credential_transaction_at_revision(
            dir.path(),
            &mut clear,
            &intents,
            revision,
        )
        .unwrap();
        let (fresh, _) = active_facade_config_and_cluster_revision(dir.path());
        assert!(fresh.cluster_fabric.nodes.is_empty());
        assert!(!fresh
            .cluster_fabric
            .credential_refs
            .contains_key("active-node"));
        assert!(CredentialStore::open(dir.path())
            .resolve(&reference)
            .unwrap()
            .is_none());
        assert_eq!(
            read_target_or_empty(&dir.path().join(CONFIG_FILE)).unwrap(),
            root_before
        );
    }

    #[test]
    fn cluster_section_revision_is_authoritative_and_rebases_unrelated_credentials() {
        let _key = crate::encryption::set_test_encryption_key([0xb1; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::migrate_config_facade_layout(dir.path()).unwrap();
        let unrelated = credential_ref("provider", "anthropic", "api_key").unwrap();
        let store = CredentialStore::open(dir.path());
        store
            .replace(
                unrelated.clone(),
                "unrelated-secret",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        assert_eq!(store.revision().unwrap(), 1);

        let (mut candidate, section_revision) =
            active_facade_config_and_cluster_revision(dir.path());
        assert_eq!(section_revision, 0);
        candidate.cluster_fabric.nodes.push(crate::Node {
            id: "local-1".to_string(),
            label: "local-1".to_string(),
            placement: crate::NodePlacement::Local,
            trust_level: crate::TrustLevel::Trusted,
            deploy: crate::DeployProfile::default(),
            state: None,
            enabled: true,
        });
        let intents = BTreeMap::from([(
            "local-1".to_string(),
            crate::ClusterNodeCredentialIntents::clear_all(),
        )]);
        let committed = persist_cluster_fabric_credential_transaction_at_revision(
            dir.path(),
            &mut candidate,
            &intents,
            section_revision,
        )
        .unwrap();
        assert_eq!(committed, 1, "the returned revision is the cluster section");
        assert_eq!(
            store.revision().unwrap(),
            1,
            "metadata-only cluster edits do not manufacture credential revisions"
        );
        assert_eq!(
            store.resolve(&unrelated).unwrap().unwrap().expose(),
            "unrelated-secret"
        );

        let cluster_before = std::fs::read(dir.path().join(CLUSTER_FABRIC_FILE)).unwrap();
        let credentials_before = std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap();
        candidate.cluster_fabric.nodes[0].label = "stale-loser".to_string();
        let error = persist_cluster_fabric_credential_transaction_at_revision(
            dir.path(),
            &mut candidate,
            &intents,
            section_revision,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ConfigStoreError::Conflict {
                expected: 0,
                actual: 1
            }
        ));
        assert_eq!(
            std::fs::read(dir.path().join(CLUSTER_FABRIC_FILE)).unwrap(),
            cluster_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap(),
            credentials_before
        );
    }

    #[test]
    fn cluster_transaction_merges_unrelated_post_commit_credential_winner() {
        let _key = crate::encryption::set_test_encryption_key([0xb2; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::migrate_config_facade_layout(dir.path()).unwrap();
        let (mut candidate, revision) = active_facade_config_and_cluster_revision(dir.path());
        candidate.cluster_fabric.nodes.push(crate::Node {
            id: "race-local".to_string(),
            label: "race-local".to_string(),
            placement: crate::NodePlacement::Local,
            trust_level: crate::TrustLevel::Trusted,
            deploy: crate::DeployProfile::default(),
            state: None,
            enabled: true,
        });
        let intents = BTreeMap::from([(
            "race-local".to_string(),
            crate::ClusterNodeCredentialIntents::clear_all(),
        )]);

        let committed = persist_cluster_fabric_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            revision,
            None,
            Some(MigrationFault::AfterExactCommitUnrelatedCredentialRace),
        )
        .unwrap();
        assert_eq!(committed, 1);
        let unrelated = credential_ref("provider", "anthropic", "api_key").unwrap();
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&unrelated)
                .unwrap()
                .unwrap()
                .expose(),
            "concurrent-unrelated-winner"
        );
        let (fresh, revision) = active_facade_config_and_cluster_revision(dir.path());
        assert_eq!(revision, 1);
        assert!(fresh.cluster_fabric.node("race-local").is_some());
    }

    #[test]
    fn adopted_cluster_transaction_does_not_publish_an_intentional_abort() {
        let _key = crate::encryption::set_test_encryption_key([0xba; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::migrate_config_facade_layout(dir.path()).unwrap();
        let (mut candidate, revision) = active_facade_config_and_cluster_revision(dir.path());
        candidate.cluster_fabric.nodes.push(cluster_test_node(
            "aborted-node",
            crate::SshAuth::Password {
                password: String::new(),
                password_encrypted: None,
            },
        ));
        let intents = BTreeMap::from([(
            "aborted-node".to_string(),
            password_intents(crate::ClusterCredentialAction::Replace(
                "must-roll-back".to_string(),
            )),
        )]);
        let cluster_before = std::fs::read(dir.path().join(CLUSTER_FABRIC_FILE)).unwrap();
        let credentials_before = std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap();
        let mut callbacks = 0;
        let mut callback = |_: u64,
                            _: bool,
                            _: &crate::ClusterFabricConfig,
                            _: ConfigStoreResult<ExactClusterRuntimeSnapshot>,
                            _: ConfigStoreResult<()>| {
            callbacks += 1;
        };

        let error = persist_cluster_fabric_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            revision,
            Some(&mut callback),
            Some(MigrationFault::AfterExactClusterConsumerConflict),
        )
        .unwrap_err();

        assert!(error.to_string().contains("shared by another consumer"));
        assert_eq!(callbacks, 0, "a rolled-back intent must not be adopted");
        assert_eq!(
            std::fs::read(dir.path().join(CLUSTER_FABRIC_FILE)).unwrap(),
            cluster_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap(),
            credentials_before
        );
        assert!(!dir.path().join(MANIFEST_FILE).exists());
    }

    #[test]
    fn aborting_cluster_manifest_never_adopts_when_cleanup_recovery_keeps_failing() {
        let _key = crate::encryption::set_test_encryption_key([0xbd; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::migrate_config_facade_layout(dir.path()).unwrap();
        let (mut candidate, revision) = active_facade_config_and_cluster_revision(dir.path());
        candidate.cluster_fabric.nodes.push(cluster_test_node(
            "phantom-abort-node",
            crate::SshAuth::Password {
                password: String::new(),
                password_encrypted: None,
            },
        ));
        let abort_secret = Uuid::new_v4().to_string();
        let intents = BTreeMap::from([(
            "phantom-abort-node".to_string(),
            password_intents(crate::ClusterCredentialAction::Replace(abort_secret)),
        )]);
        let cluster_before = std::fs::read(dir.path().join(CLUSTER_FABRIC_FILE)).unwrap();
        let credentials_before = std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap();
        let mut callbacks = 0;
        let mut callback = |_: u64,
                            _: bool,
                            _: &crate::ClusterFabricConfig,
                            _: ConfigStoreResult<ExactClusterRuntimeSnapshot>,
                            _: ConfigStoreResult<()>| {
            callbacks += 1;
        };
        set_cluster_exact_commit_test_fault(
            dir.path().to_path_buf(),
            ClusterExactCommitTestFault::AbortCleanupRecoveryFailure,
        );

        let error = persist_cluster_fabric_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            revision,
            Some(&mut callback),
            Some(MigrationFault::AfterExactClusterConsumerConflict),
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("persistent cluster abort cleanup failure"));
        assert_eq!(callbacks, 0, "an aborting candidate must never be adopted");
        assert_eq!(
            std::fs::read(dir.path().join(CLUSTER_FABRIC_FILE)).unwrap(),
            cluster_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap(),
            credentials_before
        );

        let manifest_path = dir.path().join(MANIFEST_FILE);
        let manifest: MigrationManifest =
            serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
        assert_eq!(manifest.state, MigrationState::Aborting);
        assert!(matches!(
            load_live_cluster_transaction_manifest(dir.path(), &manifest).unwrap(),
            LiveClusterTransaction::Aborting
        ));
        assert!(crate::ensure_provider_mcp_migration_ready(dir.path()).is_err());
        assert!(
            recover_committed(
                dir.path(),
                #[cfg(test)]
                None,
            )
            .is_err(),
            "recovery must resume abort and retain its fail-closed marker"
        );
        let persisted: MigrationManifest =
            serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
        assert_eq!(persisted.state, MigrationState::Aborting);
        assert_eq!(callbacks, 0);

        clear_cluster_exact_commit_test_fault(dir.path());
        assert!(recover_committed(
            dir.path(),
            #[cfg(test)]
            None,
        )
        .unwrap()
        .is_none());
        assert!(!manifest_path.exists());
        assert_eq!(
            std::fs::read(dir.path().join(CLUSTER_FABRIC_FILE)).unwrap(),
            cluster_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap(),
            credentials_before
        );
        assert_eq!(callbacks, 0);
    }

    #[test]
    fn abort_marker_failure_is_still_a_typed_noncommit_outcome() {
        let _key = crate::encryption::set_test_encryption_key([0xbe; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::migrate_config_facade_layout(dir.path()).unwrap();
        let providers_path = dir.path().join(PROVIDERS_FILE);
        let providers_before = std::fs::read(&providers_path).unwrap();
        let (mut candidate, revision) = active_facade_config_and_cluster_revision(dir.path());
        candidate.cluster_fabric.nodes.push(cluster_test_node(
            "marker-failure-node",
            crate::SshAuth::Password {
                password: String::new(),
                password_encrypted: None,
            },
        ));
        let abort_secret = Uuid::new_v4().to_string();
        let intents = BTreeMap::from([(
            "marker-failure-node".to_string(),
            password_intents(crate::ClusterCredentialAction::Replace(
                abort_secret.clone(),
            )),
        )]);
        let cluster_before = std::fs::read(dir.path().join(CLUSTER_FABRIC_FILE)).unwrap();
        let credentials_before = std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap();
        let mut callbacks = 0;
        let mut callback = |_: u64,
                            _: bool,
                            _: &crate::ClusterFabricConfig,
                            _: ConfigStoreResult<ExactClusterRuntimeSnapshot>,
                            _: ConfigStoreResult<()>| {
            callbacks += 1;
        };
        set_cluster_exact_commit_test_fault(
            dir.path().to_path_buf(),
            ClusterExactCommitTestFault::AbortMarkerRecoveryFailure,
        );

        let error = persist_cluster_fabric_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            revision,
            Some(&mut callback),
            Some(MigrationFault::AfterExactClusterConsumerConflict),
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("persistent cluster abort marker failure"));
        assert_eq!(
            callbacks, 0,
            "an in-memory abort outcome must bypass committed reporting"
        );
        assert_eq!(
            std::fs::read(dir.path().join(CLUSTER_FABRIC_FILE)).unwrap(),
            cluster_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap(),
            credentials_before
        );

        let manifest_path = dir.path().join(MANIFEST_FILE);
        let manifest: MigrationManifest =
            serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
        assert_eq!(
            manifest.state,
            MigrationState::Pending,
            "the injected marker failure must exercise the typed fallback"
        );
        let journal_path = dir.path().join(JOURNAL_FILE);
        let journal: MigrationManifest =
            serde_json::from_slice(&std::fs::read(&journal_path).unwrap()).unwrap();
        assert_eq!(
            journal.state,
            MigrationState::Aborting,
            "the immutable journal must retain durable non-commit intent"
        );
        let stage_dir = dir.path().join(&journal.stage_dir);
        let backup_dir = dir
            .path()
            .join(format!("{BACKUP_PREFIX}{}", journal.transaction_id));
        assert!(stage_dir.exists());
        assert!(backup_dir.exists());
        assert_ne!(
            std::fs::read(&providers_path).unwrap(),
            providers_before,
            "the conflict must still be present before restart"
        );

        // Simulate a restart after the external conflict has disappeared. The
        // Pending main manifest alone would now be installable, so recovery
        // must honor the durable Aborting journal instead.
        std::fs::write(&providers_path, &providers_before).unwrap();
        assert!(matches!(
            load_live_cluster_transaction_manifest(dir.path(), &manifest).unwrap(),
            LiveClusterTransaction::Aborting
        ));
        assert!(crate::ensure_provider_mcp_migration_ready(dir.path()).is_err());

        clear_cluster_exact_commit_test_fault(dir.path());
        assert!(
            !recover_pending_config_transaction(dir.path()).unwrap(),
            "restart recovery must roll back rather than report a committed candidate"
        );
        assert!(
            !recover_pending_config_transaction(dir.path()).unwrap(),
            "completed abort recovery must be idempotent"
        );
        assert!(!manifest_path.exists());
        assert!(!journal_path.exists());
        assert!(!stage_dir.exists());
        assert!(!backup_dir.exists());
        assert_eq!(
            std::fs::read(dir.path().join(CLUSTER_FABRIC_FILE)).unwrap(),
            cluster_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap(),
            credentials_before
        );
        let password_ref = crate::cluster_password_credential_ref("marker-failure-node").unwrap();
        assert!(CredentialStore::open(dir.path())
            .resolve(&password_ref)
            .unwrap()
            .is_none());
        let (fresh, _) = active_facade_config_and_cluster_revision(dir.path());
        assert!(fresh.cluster_fabric.node("marker-failure-node").is_none());
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                assert!(
                    !String::from_utf8_lossy(&std::fs::read(entry.path()).unwrap())
                        .contains(&abort_secret),
                    "aborted cluster plaintext remained in {}",
                    entry.path().display()
                );
            }
        }
        assert_eq!(callbacks, 0);
    }

    #[test]
    fn abort_journal_marker_failure_falls_back_to_main_abort_across_restart() {
        let _key = crate::encryption::set_test_encryption_key([0xc5; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::migrate_config_facade_layout(dir.path()).unwrap();
        let providers_path = dir.path().join(PROVIDERS_FILE);
        let providers_before = std::fs::read(&providers_path).unwrap();
        let (mut candidate, revision) = active_facade_config_and_cluster_revision(dir.path());
        candidate.cluster_fabric.nodes.push(cluster_test_node(
            "journal-marker-failure-node",
            crate::SshAuth::Password {
                password: String::new(),
                password_encrypted: None,
            },
        ));
        let abort_secret = Uuid::new_v4().to_string();
        let intents = BTreeMap::from([(
            "journal-marker-failure-node".to_string(),
            password_intents(crate::ClusterCredentialAction::Replace(
                abort_secret.clone(),
            )),
        )]);
        let cluster_before = std::fs::read(dir.path().join(CLUSTER_FABRIC_FILE)).unwrap();
        let credentials_before = std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap();
        let mut callbacks = 0;
        let mut callback = |_: u64,
                            _: bool,
                            _: &crate::ClusterFabricConfig,
                            _: ConfigStoreResult<ExactClusterRuntimeSnapshot>,
                            _: ConfigStoreResult<()>| {
            callbacks += 1;
        };
        set_cluster_exact_commit_test_fault(
            dir.path().to_path_buf(),
            ClusterExactCommitTestFault::AbortJournalMarkerAndCleanupRecoveryFailure,
        );

        let error = persist_cluster_fabric_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            revision,
            Some(&mut callback),
            Some(MigrationFault::AfterExactClusterConsumerConflict),
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("persistent cluster abort cleanup failure"));
        assert_eq!(callbacks, 0);
        assert_eq!(
            std::fs::read(dir.path().join(CLUSTER_FABRIC_FILE)).unwrap(),
            cluster_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap(),
            credentials_before
        );

        let manifest_path = dir.path().join(MANIFEST_FILE);
        let manifest: MigrationManifest =
            serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
        assert_eq!(
            manifest.state,
            MigrationState::Aborting,
            "the main manifest must be the redundant durable abort marker"
        );
        let journal_path = dir.path().join(JOURNAL_FILE);
        let journal: MigrationManifest =
            serde_json::from_slice(&std::fs::read(&journal_path).unwrap()).unwrap();
        assert_eq!(
            journal.state,
            MigrationState::Pending,
            "journal marker failure must preserve the original rollback journal"
        );
        let stage_dir = dir.path().join(&journal.stage_dir);
        let backup_dir = dir
            .path()
            .join(format!("{BACKUP_PREFIX}{}", journal.transaction_id));
        assert!(stage_dir.exists());
        assert!(backup_dir.exists());
        assert_ne!(std::fs::read(&providers_path).unwrap(), providers_before);

        // The original conflict disappears before restart. Keep the injected
        // journal failure active: recovery can succeed only if the durable main
        // Aborting marker skips any attempt to mutate the Pending journal.
        std::fs::write(&providers_path, &providers_before).unwrap();
        assert!(matches!(
            load_live_cluster_transaction_manifest(dir.path(), &manifest).unwrap(),
            LiveClusterTransaction::Aborting
        ));
        assert!(crate::ensure_provider_mcp_migration_ready(dir.path()).is_err());
        assert!(
            !recover_pending_config_transaction(dir.path()).unwrap(),
            "restart recovery must roll back from the main abort marker"
        );
        clear_cluster_exact_commit_test_fault(dir.path());
        assert!(!recover_pending_config_transaction(dir.path()).unwrap());

        assert!(!manifest_path.exists());
        assert!(!journal_path.exists());
        assert!(!stage_dir.exists());
        assert!(!backup_dir.exists());
        assert_eq!(
            std::fs::read(dir.path().join(CLUSTER_FABRIC_FILE)).unwrap(),
            cluster_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap(),
            credentials_before
        );
        let password_ref =
            crate::cluster_password_credential_ref("journal-marker-failure-node").unwrap();
        assert!(CredentialStore::open(dir.path())
            .resolve(&password_ref)
            .unwrap()
            .is_none());
        let (fresh, _) = active_facade_config_and_cluster_revision(dir.path());
        assert!(fresh
            .cluster_fabric
            .node("journal-marker-failure-node")
            .is_none());
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                assert!(
                    !String::from_utf8_lossy(&std::fs::read(entry.path()).unwrap())
                        .contains(&abort_secret),
                    "aborted cluster plaintext remained in {}",
                    entry.path().display()
                );
            }
        }
        assert_eq!(callbacks, 0);
    }

    #[test]
    fn abort_double_marker_failure_never_adopts_in_current_process() {
        let _key = crate::encryption::set_test_encryption_key([0xc6; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::migrate_config_facade_layout(dir.path()).unwrap();
        let (mut candidate, revision) = active_facade_config_and_cluster_revision(dir.path());
        candidate.cluster_fabric.nodes.push(cluster_test_node(
            "double-marker-failure-node",
            crate::SshAuth::Password {
                password: String::new(),
                password_encrypted: None,
            },
        ));
        let intents = BTreeMap::from([(
            "double-marker-failure-node".to_string(),
            password_intents(crate::ClusterCredentialAction::Replace(
                "double-marker-secret".to_string(),
            )),
        )]);
        let cluster_before = std::fs::read(dir.path().join(CLUSTER_FABRIC_FILE)).unwrap();
        let credentials_before = std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap();
        let mut callbacks = 0;
        let mut callback = |_: u64,
                            _: bool,
                            _: &crate::ClusterFabricConfig,
                            _: ConfigStoreResult<ExactClusterRuntimeSnapshot>,
                            _: ConfigStoreResult<()>| {
            callbacks += 1;
        };
        set_cluster_exact_commit_test_fault(
            dir.path().to_path_buf(),
            ClusterExactCommitTestFault::AbortBothMarkersRecoveryFailure,
        );

        let error = persist_cluster_fabric_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            revision,
            Some(&mut callback),
            Some(MigrationFault::AfterExactClusterConsumerConflict),
        )
        .unwrap_err();
        assert!(error.to_string().contains("either durable marker"));
        assert_eq!(
            callbacks, 0,
            "an undurable abort attempt must bypass committed reporting"
        );
        assert_eq!(
            std::fs::read(dir.path().join(CLUSTER_FABRIC_FILE)).unwrap(),
            cluster_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap(),
            credentials_before
        );

        let manifest: MigrationManifest =
            serde_json::from_slice(&std::fs::read(dir.path().join(MANIFEST_FILE)).unwrap())
                .unwrap();
        let journal: MigrationManifest =
            serde_json::from_slice(&std::fs::read(dir.path().join(JOURNAL_FILE)).unwrap()).unwrap();
        assert_eq!(manifest.state, MigrationState::Pending);
        assert_eq!(journal.state, MigrationState::Pending);
        assert!(matches!(
            load_live_cluster_transaction_manifest(dir.path(), &manifest).unwrap(),
            LiveClusterTransaction::Committed(_)
        ));
        clear_cluster_exact_commit_test_fault(dir.path());
    }

    #[test]
    fn complete_cluster_abort_manifest_is_not_a_live_commit() {
        let _key = crate::encryption::set_test_encryption_key([0xbc; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::migrate_config_facade_layout(dir.path()).unwrap();
        let (mut candidate, revision) = active_facade_config_and_cluster_revision(dir.path());
        candidate.cluster_fabric.nodes.push(crate::Node {
            id: "never-installed".to_string(),
            label: "never-installed".to_string(),
            placement: crate::NodePlacement::Local,
            trust_level: crate::TrustLevel::Trusted,
            deploy: crate::DeployProfile::default(),
            state: None,
            enabled: true,
        });
        let intents = BTreeMap::from([(
            "never-installed".to_string(),
            crate::ClusterNodeCredentialIntents::clear_all(),
        )]);
        assert!(persist_cluster_fabric_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            revision,
            None,
            Some(MigrationFault::AfterManifest),
        )
        .is_err());
        let manifest_path = dir.path().join(MANIFEST_FILE);
        let mut manifest: MigrationManifest =
            serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
        assert_eq!(manifest.state, MigrationState::Pending);
        manifest.state = MigrationState::Complete;
        write_manifest(manifest_path, &manifest).unwrap();

        assert!(
            matches!(
                load_live_cluster_transaction_manifest(dir.path(), &manifest).unwrap(),
                LiveClusterTransaction::NotCommitted
            ),
            "Complete plus rolled-back/nonmatching authority is an abort tail"
        );
    }

    #[test]
    fn adopted_cluster_transaction_reports_the_actual_rebased_candidate() {
        let _key = crate::encryption::set_test_encryption_key([0xbb; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::migrate_config_facade_layout(dir.path()).unwrap();
        let (mut baseline, revision) = active_facade_config_and_cluster_revision(dir.path());
        baseline.cluster_fabric.nodes.push(crate::Node {
            id: "baseline-node".to_string(),
            label: "baseline-node".to_string(),
            placement: crate::NodePlacement::Local,
            trust_level: crate::TrustLevel::Trusted,
            deploy: crate::DeployProfile::default(),
            state: None,
            enabled: true,
        });
        persist_cluster_fabric_credential_transaction_at_revision(
            dir.path(),
            &mut baseline,
            &BTreeMap::from([(
                "baseline-node".to_string(),
                crate::ClusterNodeCredentialIntents::clear_all(),
            )]),
            revision,
        )
        .unwrap();
        let (mut candidate, revision) = active_facade_config_and_cluster_revision(dir.path());
        candidate.cluster_fabric.nodes.push(crate::Node {
            id: "planned-node".to_string(),
            label: "planned-node".to_string(),
            placement: crate::NodePlacement::Local,
            trust_level: crate::TrustLevel::Trusted,
            deploy: crate::DeployProfile::default(),
            state: None,
            enabled: true,
        });
        let intents = BTreeMap::from([(
            "planned-node".to_string(),
            crate::ClusterNodeCredentialIntents::clear_all(),
        )]);
        let mut observed = None;
        let mut callback = |committed_revision: u64,
                            changed: bool,
                            committed: &crate::ClusterFabricConfig,
                            runtime: ConfigStoreResult<ExactClusterRuntimeSnapshot>,
                            recovery: ConfigStoreResult<()>| {
            observed = Some((
                committed_revision,
                changed,
                committed.clone(),
                runtime.is_ok(),
                recovery.is_ok(),
            ));
        };

        let committed = persist_cluster_fabric_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            revision,
            Some(&mut callback),
            Some(MigrationFault::AfterExactClusterSectionRebase),
        )
        .unwrap();

        assert_eq!(committed, 3);
        let (callback_revision, changed, callback_candidate, runtime_ok, recovery_ok) =
            observed.expect("post-commit callback");
        assert_eq!(callback_revision, committed);
        assert!(changed);
        assert!(runtime_ok);
        assert!(recovery_ok);
        for node_id in ["external-rebase-node", "planned-node"] {
            assert!(callback_candidate.node(node_id).is_some());
            assert!(candidate.cluster_fabric.node(node_id).is_some());
        }
        let (fresh, fresh_revision) = active_facade_config_and_cluster_revision(dir.path());
        assert_eq!(fresh_revision, committed);
        assert_eq!(fresh.cluster_fabric, callback_candidate);
    }

    #[test]
    fn cluster_transaction_rebases_unrelated_pre_manifest_credential_winner() {
        let _key = crate::encryption::set_test_encryption_key([0xb6; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::migrate_config_facade_layout(dir.path()).unwrap();
        let (mut candidate, revision) = active_facade_config_and_cluster_revision(dir.path());
        candidate.cluster_fabric.nodes.push(crate::Node {
            id: "pre-manifest-local".to_string(),
            label: "pre-manifest-local".to_string(),
            placement: crate::NodePlacement::Local,
            trust_level: crate::TrustLevel::Trusted,
            deploy: crate::DeployProfile::default(),
            state: None,
            enabled: true,
        });
        let intents = BTreeMap::from([(
            "pre-manifest-local".to_string(),
            crate::ClusterNodeCredentialIntents::clear_all(),
        )]);

        let committed = persist_cluster_fabric_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            revision,
            None,
            Some(MigrationFault::BeforeExactCommitUnrelatedCredentialRace),
        )
        .unwrap();
        assert_eq!(committed, 1);
        let unrelated = credential_ref("provider", "anthropic", "api_key").unwrap();
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&unrelated)
                .unwrap()
                .unwrap()
                .expose(),
            "concurrent-winner"
        );
        let (fresh, revision) = active_facade_config_and_cluster_revision(dir.path());
        assert_eq!(revision, 1);
        assert!(fresh.cluster_fabric.node("pre-manifest-local").is_some());
    }

    #[test]
    fn cluster_transaction_rebases_unrelated_pre_staging_credential_winner() {
        let _key = crate::encryption::set_test_encryption_key([0xb7; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::migrate_config_facade_layout(dir.path()).unwrap();
        let (mut candidate, revision) = active_facade_config_and_cluster_revision(dir.path());
        candidate.cluster_fabric.nodes.push(crate::Node {
            id: "pre-staging-local".to_string(),
            label: "pre-staging-local".to_string(),
            placement: crate::NodePlacement::Local,
            trust_level: crate::TrustLevel::Trusted,
            deploy: crate::DeployProfile::default(),
            state: None,
            enabled: true,
        });
        let intents = BTreeMap::from([(
            "pre-staging-local".to_string(),
            crate::ClusterNodeCredentialIntents::clear_all(),
        )]);

        let committed = persist_cluster_fabric_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            revision,
            None,
            Some(MigrationFault::BeforeExactStagingUnrelatedCredentialRace),
        )
        .unwrap();
        assert_eq!(committed, 1);
        let unrelated = credential_ref("provider", "anthropic", "api_key").unwrap();
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&unrelated)
                .unwrap()
                .unwrap()
                .expose(),
            "concurrent-pre-staging-winner"
        );
        let (fresh, revision) = active_facade_config_and_cluster_revision(dir.path());
        assert_eq!(revision, 1);
        assert!(fresh.cluster_fabric.node("pre-staging-local").is_some());
    }

    #[test]
    fn cluster_password_key_and_passphrase_actions_are_explicit_and_secret_free() {
        let _key = crate::encryption::set_test_encryption_key([0xb3; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::migrate_config_facade_layout(dir.path()).unwrap();
        let (mut create, revision) = active_facade_config_and_cluster_revision(dir.path());
        create.cluster_fabric.nodes.push(cluster_test_node(
            "ssh-1",
            crate::SshAuth::Password {
                password: String::new(),
                password_encrypted: None,
            },
        ));
        let mut intents = BTreeMap::from([(
            "ssh-1".to_string(),
            password_intents(crate::ClusterCredentialAction::Replace(
                "password-secret".to_string(),
            )),
        )]);
        assert_eq!(
            persist_cluster_fabric_credential_transaction_at_revision(
                dir.path(),
                &mut create,
                &intents,
                revision,
            )
            .unwrap(),
            1
        );
        let store = CredentialStore::open(dir.path());
        let password_ref = crate::cluster_password_credential_ref("ssh-1").unwrap();
        let key_ref = crate::cluster_private_key_credential_ref("ssh-1").unwrap();
        let passphrase_ref = crate::cluster_passphrase_credential_ref("ssh-1").unwrap();
        assert_eq!(
            store.resolve(&password_ref).unwrap().unwrap().expose(),
            "password-secret"
        );

        let (mut keep, revision) = active_facade_config_and_cluster_revision(dir.path());
        keep.cluster_fabric.node_mut("ssh-1").unwrap().label = "kept".to_string();
        intents.insert(
            "ssh-1".to_string(),
            password_intents(crate::ClusterCredentialAction::Keep),
        );
        assert_eq!(
            persist_cluster_fabric_credential_transaction_at_revision(
                dir.path(),
                &mut keep,
                &intents,
                revision,
            )
            .unwrap(),
            2
        );
        assert_eq!(
            store.resolve(&password_ref).unwrap().unwrap().expose(),
            "password-secret"
        );

        let (mut switch, revision) = active_facade_config_and_cluster_revision(dir.path());
        switch.cluster_fabric.node_mut("ssh-1").unwrap().placement =
            crate::NodePlacement::Ssh(crate::SshTarget {
                host: "127.0.0.1".to_string(),
                port: 22,
                username: "deploy".to_string(),
                auth: crate::SshAuth::PrivateKey {
                    private_key: String::new(),
                    private_key_encrypted: None,
                    private_key_path: None,
                    passphrase: String::new(),
                    passphrase_encrypted: None,
                },
                host_key_fingerprint: None,
            });
        intents.insert(
            "ssh-1".to_string(),
            crate::ClusterNodeCredentialIntents {
                password: crate::ClusterCredentialAction::Clear,
                private_key: crate::ClusterCredentialAction::Replace(
                    "private-key-secret".to_string(),
                ),
                passphrase: crate::ClusterCredentialAction::Replace(
                    "passphrase-secret".to_string(),
                ),
            },
        );
        assert_eq!(
            persist_cluster_fabric_credential_transaction_at_revision(
                dir.path(),
                &mut switch,
                &intents,
                revision,
            )
            .unwrap(),
            3
        );
        assert!(store.resolve(&password_ref).unwrap().is_none());
        assert_eq!(
            store.resolve(&key_ref).unwrap().unwrap().expose(),
            "private-key-secret"
        );
        assert_eq!(
            store.resolve(&passphrase_ref).unwrap().unwrap().expose(),
            "passphrase-secret"
        );

        let (mut clear_passphrase, revision) =
            active_facade_config_and_cluster_revision(dir.path());
        clear_passphrase
            .cluster_fabric
            .node_mut("ssh-1")
            .unwrap()
            .label = "clear-passphrase".to_string();
        intents.insert(
            "ssh-1".to_string(),
            crate::ClusterNodeCredentialIntents {
                password: crate::ClusterCredentialAction::Clear,
                private_key: crate::ClusterCredentialAction::Keep,
                passphrase: crate::ClusterCredentialAction::Clear,
            },
        );
        assert_eq!(
            persist_cluster_fabric_credential_transaction_at_revision(
                dir.path(),
                &mut clear_passphrase,
                &intents,
                revision,
            )
            .unwrap(),
            4
        );
        assert_eq!(
            store.resolve(&key_ref).unwrap().unwrap().expose(),
            "private-key-secret"
        );
        assert!(store.resolve(&passphrase_ref).unwrap().is_none());

        let ordinary = std::fs::read_to_string(dir.path().join(CLUSTER_FABRIC_FILE)).unwrap();
        for forbidden in [
            "password-secret",
            "private-key-secret",
            "passphrase-secret",
            "****",
            "password_encrypted",
            "private_key_encrypted",
            "passphrase_encrypted",
        ] {
            assert!(
                !ordinary.contains(forbidden),
                "ordinary cluster section leaked {forbidden}"
            );
        }
        let section: Value = serde_json::from_str(&ordinary).unwrap();
        let auth = &section["data"]["nodes"][0]["placement"]["auth"];
        assert!(auth.get("password").is_none());
        assert!(auth.get("private_key").is_none());
        assert!(auth.get("passphrase").is_none());
    }

    #[test]
    fn exact_cluster_get_status_marks_corrupt_and_wrong_key_ciphertext_as_error_metadata() {
        let key = crate::encryption::set_test_encryption_key([0xbe; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::migrate_config_facade_layout(dir.path()).unwrap();
        let (mut candidate, revision) = active_facade_config_and_cluster_revision(dir.path());
        candidate.cluster_fabric.nodes.push(cluster_test_node(
            "status-node",
            crate::SshAuth::Password {
                password: String::new(),
                password_encrypted: None,
            },
        ));
        let password_ref = crate::cluster_password_credential_ref("status-node").unwrap();
        let status_secret = Uuid::new_v4().to_string();
        assert_eq!(
            persist_cluster_fabric_credential_transaction_at_revision(
                dir.path(),
                &mut candidate,
                &BTreeMap::from([(
                    "status-node".to_string(),
                    password_intents(crate::ClusterCredentialAction::Replace(
                        status_secret.clone(),
                    )),
                )]),
                revision,
            )
            .unwrap(),
            1
        );

        let healthy = read_exact_cluster_fabric_snapshot(dir.path(), None).unwrap();
        assert!(
            healthy
                .credential_statuses
                .iter()
                .find(|status| status.credential_ref == password_ref)
                .unwrap()
                .configured
        );
        let crate::NodePlacement::Ssh(target) = &healthy
            .cluster_fabric
            .node("status-node")
            .unwrap()
            .placement
        else {
            panic!("expected SSH placement")
        };
        let crate::SshAuth::Password { password, .. } = &target.auth else {
            panic!("expected password authentication")
        };
        assert!(
            password.is_empty(),
            "GET status must not hydrate plaintext into Config"
        );

        let credentials_path = dir.path().join(CREDENTIALS_FILE);
        let credentials = std::fs::read(&credentials_path).unwrap();
        let mut corrupt: Value = serde_json::from_slice(&credentials).unwrap();
        corrupt["data"]["entries"][password_ref.as_str()]["ciphertext"] =
            Value::String("nonempty-but-not-ciphertext".to_string());
        std::fs::write(
            &credentials_path,
            serde_json::to_vec_pretty(&corrupt).unwrap(),
        )
        .unwrap();
        let corrupt = read_exact_cluster_fabric_snapshot(dir.path(), None).unwrap();
        assert!(
            !corrupt
                .credential_statuses
                .iter()
                .find(|status| status.credential_ref == password_ref)
                .unwrap()
                .configured
        );

        std::fs::write(&credentials_path, credentials).unwrap();
        drop(key);
        let _wrong_key = crate::encryption::set_test_encryption_key([0xbf; 32]);
        let wrong_key = read_exact_cluster_fabric_snapshot(dir.path(), None).unwrap();
        assert!(
            !wrong_key
                .credential_statuses
                .iter()
                .find(|status| status.credential_ref == password_ref)
                .unwrap()
                .configured
        );
        let encoded = serde_json::to_string(&wrong_key.cluster_fabric).unwrap();
        assert!(!encoded.contains(&status_secret));
    }

    #[test]
    fn secret_free_cluster_snapshot_supports_explicit_corrupt_credential_repair() {
        let _key = crate::encryption::set_test_encryption_key([0xc0; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::migrate_config_facade_layout(dir.path()).unwrap();
        let (mut candidate, revision) = active_facade_config_and_cluster_revision(dir.path());
        candidate.cluster_fabric.nodes.push(cluster_test_node(
            "repair-node",
            crate::SshAuth::Password {
                password: String::new(),
                password_encrypted: None,
            },
        ));
        let password_ref = crate::cluster_password_credential_ref("repair-node").unwrap();
        let initial_secret = Uuid::new_v4().to_string();
        assert_eq!(
            persist_cluster_fabric_credential_transaction_at_revision(
                dir.path(),
                &mut candidate,
                &BTreeMap::from([(
                    "repair-node".to_string(),
                    password_intents(crate::ClusterCredentialAction::Replace(initial_secret,)),
                )]),
                revision,
            )
            .unwrap(),
            1
        );

        let credentials_path = dir.path().join(CREDENTIALS_FILE);
        let corrupt_active_password = || {
            let mut document: Value =
                serde_json::from_slice(&std::fs::read(&credentials_path).unwrap()).unwrap();
            document["data"]["entries"][password_ref.as_str()]["ciphertext"] =
                Value::String("corrupt-active-ciphertext".to_string());
            std::fs::write(
                &credentials_path,
                serde_json::to_vec_pretty(&document).unwrap(),
            )
            .unwrap();
        };
        corrupt_active_password();
        let exact = read_exact_cluster_fabric_snapshot(dir.path(), None).unwrap();
        assert!(
            !exact
                .credential_statuses
                .iter()
                .find(|status| status.credential_ref == password_ref)
                .unwrap()
                .configured
        );
        let mut replacement = crate::Config::default();
        replacement.cluster_fabric = exact.cluster_fabric;
        let repaired_secret = Uuid::new_v4().to_string();
        assert_eq!(
            persist_cluster_fabric_credential_transaction_at_revision(
                dir.path(),
                &mut replacement,
                &BTreeMap::from([(
                    "repair-node".to_string(),
                    password_intents(crate::ClusterCredentialAction::Replace(
                        repaired_secret.clone(),
                    )),
                )]),
                1,
            )
            .unwrap(),
            2
        );
        let replaced = read_exact_cluster_fabric_snapshot(dir.path(), Some(2)).unwrap();
        let crate::NodePlacement::Ssh(target) = &replaced
            .cluster_fabric
            .node("repair-node")
            .unwrap()
            .placement
        else {
            panic!("expected SSH placement")
        };
        let crate::SshAuth::Password { password, .. } = &target.auth else {
            panic!("expected password authentication")
        };
        assert_eq!(password, &repaired_secret);

        corrupt_active_password();
        let exact = read_exact_cluster_fabric_snapshot(dir.path(), None).unwrap();
        let mut clearing = crate::Config::default();
        clearing.cluster_fabric = exact.cluster_fabric;
        clearing
            .cluster_fabric
            .node_mut("repair-node")
            .unwrap()
            .placement = crate::NodePlacement::Ssh(crate::SshTarget {
            host: "repair.example.test".to_string(),
            port: 22,
            username: "operator".to_string(),
            auth: crate::SshAuth::SystemSshConfig,
            host_key_fingerprint: None,
        });
        assert_eq!(
            persist_cluster_fabric_credential_transaction_at_revision(
                dir.path(),
                &mut clearing,
                &BTreeMap::from([(
                    "repair-node".to_string(),
                    crate::ClusterNodeCredentialIntents::clear_all(),
                )]),
                2,
            )
            .unwrap(),
            3
        );
        let cleared = read_exact_cluster_fabric_snapshot(dir.path(), Some(3)).unwrap();
        assert!(!cleared
            .cluster_fabric
            .credential_refs
            .contains_key("repair-node"));
        assert!(CredentialStore::open(dir.path())
            .resolve(&password_ref)
            .unwrap()
            .is_none());
    }

    #[test]
    fn secret_only_cluster_replace_advances_section_revision_and_rejects_stale_writer() {
        let _key = crate::encryption::set_test_encryption_key([0xb8; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::migrate_config_facade_layout(dir.path()).unwrap();

        let (mut create, revision) = active_facade_config_and_cluster_revision(dir.path());
        create.cluster_fabric.nodes.push(cluster_test_node(
            "secret-cas",
            crate::SshAuth::Password {
                password: String::new(),
                password_encrypted: None,
            },
        ));
        let initial_intents = BTreeMap::from([(
            "secret-cas".to_string(),
            password_intents(crate::ClusterCredentialAction::Replace(
                "initial-secret".to_string(),
            )),
        )]);
        assert_eq!(
            persist_cluster_fabric_credential_transaction_at_revision(
                dir.path(),
                &mut create,
                &initial_intents,
                revision,
            )
            .unwrap(),
            1
        );

        let (first_base, shared_revision) = active_facade_config_and_cluster_revision(dir.path());
        assert_eq!(shared_revision, 1);
        let mut first = first_base.clone();
        let mut stale = first_base;
        let winner_intents = BTreeMap::from([(
            "secret-cas".to_string(),
            password_intents(crate::ClusterCredentialAction::Replace(
                "winner-secret".to_string(),
            )),
        )]);
        assert_eq!(
            persist_cluster_fabric_credential_transaction_at_revision(
                dir.path(),
                &mut first,
                &winner_intents,
                shared_revision,
            )
            .unwrap(),
            2,
            "a secret-only write must advance the public cluster revision"
        );

        let stale_intents = BTreeMap::from([(
            "secret-cas".to_string(),
            password_intents(crate::ClusterCredentialAction::Replace(
                "stale-loser-secret".to_string(),
            )),
        )]);
        let error = persist_cluster_fabric_credential_transaction_at_revision(
            dir.path(),
            &mut stale,
            &stale_intents,
            shared_revision,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ConfigStoreError::Conflict {
                expected: 1,
                actual: 2
            }
        ));
        let reference = crate::cluster_password_credential_ref("secret-cas").unwrap();
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "winner-secret"
        );
    }

    #[test]
    fn node_and_new_cluster_membership_commit_together_or_not_at_all() {
        let _key = crate::encryption::set_test_encryption_key([0xb4; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::migrate_config_facade_layout(dir.path()).unwrap();
        let (mut candidate, revision) = active_facade_config_and_cluster_revision(dir.path());
        candidate.cluster_fabric.nodes.push(cluster_test_node(
            "atomic-node",
            crate::SshAuth::Password {
                password: String::new(),
                password_encrypted: None,
            },
        ));
        candidate.cluster_fabric.clusters.push(crate::Cluster {
            name: "new-cluster".to_string(),
            description: None,
            node_ids: vec!["atomic-node".to_string()],
        });
        let intents = BTreeMap::from([(
            "atomic-node".to_string(),
            password_intents(crate::ClusterCredentialAction::Replace(
                "atomic-secret".to_string(),
            )),
        )]);
        assert_eq!(
            persist_cluster_fabric_credential_transaction_at_revision(
                dir.path(),
                &mut candidate,
                &intents,
                revision,
            )
            .unwrap(),
            1
        );
        let (fresh, revision) = active_facade_config_and_cluster_revision(dir.path());
        assert_eq!(revision, 1);
        assert!(fresh.cluster_fabric.node("atomic-node").is_some());
        assert_eq!(
            fresh
                .cluster_fabric
                .cluster("new-cluster")
                .unwrap()
                .node_ids,
            ["atomic-node"]
        );

        let cluster_before = std::fs::read(dir.path().join(CLUSTER_FABRIC_FILE)).unwrap();
        let credentials_before = std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap();
        let (mut failed, revision) = active_facade_config_and_cluster_revision(dir.path());
        failed.cluster_fabric.nodes.push(cluster_test_node(
            "failed-node",
            crate::SshAuth::Password {
                password: String::new(),
                password_encrypted: None,
            },
        ));
        failed.cluster_fabric.clusters.push(crate::Cluster {
            name: "failed-cluster".to_string(),
            description: None,
            node_ids: vec!["failed-node".to_string()],
        });
        let failed_intents = BTreeMap::from([(
            "failed-node".to_string(),
            password_intents(crate::ClusterCredentialAction::Replace(
                "failed-secret".to_string(),
            )),
        )]);
        assert!(persist_cluster_fabric_credential_transaction_inner(
            dir.path(),
            &mut failed,
            &failed_intents,
            revision,
            None,
            Some(MigrationFault::AfterStaging),
        )
        .is_err());
        assert_eq!(
            std::fs::read(dir.path().join(CLUSTER_FABRIC_FILE)).unwrap(),
            cluster_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap(),
            credentials_before
        );
        let (fresh, fresh_revision) = active_facade_config_and_cluster_revision(dir.path());
        assert_eq!(fresh_revision, revision);
        assert!(fresh.cluster_fabric.node("failed-node").is_none());
        assert!(fresh.cluster_fabric.cluster("failed-cluster").is_none());
    }

    #[test]
    fn stale_node_delete_cannot_overwrite_concurrent_membership_edit() {
        let _key = crate::encryption::set_test_encryption_key([0xb5; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::migrate_config_facade_layout(dir.path()).unwrap();
        let (mut initial, revision) = active_facade_config_and_cluster_revision(dir.path());
        initial.cluster_fabric.nodes.push(crate::Node {
            id: "member".to_string(),
            label: "member".to_string(),
            placement: crate::NodePlacement::Local,
            trust_level: crate::TrustLevel::Trusted,
            deploy: crate::DeployProfile::default(),
            state: None,
            enabled: true,
        });
        initial.cluster_fabric.clusters.push(crate::Cluster {
            name: "first".to_string(),
            description: None,
            node_ids: vec!["member".to_string()],
        });
        let node_intents = BTreeMap::from([(
            "member".to_string(),
            crate::ClusterNodeCredentialIntents::clear_all(),
        )]);
        persist_cluster_fabric_credential_transaction_at_revision(
            dir.path(),
            &mut initial,
            &node_intents,
            revision,
        )
        .unwrap();

        let (base, revision) = active_facade_config_and_cluster_revision(dir.path());
        let mut membership_winner = base.clone();
        membership_winner
            .cluster_fabric
            .clusters
            .push(crate::Cluster {
                name: "second".to_string(),
                description: None,
                node_ids: vec!["member".to_string()],
            });
        persist_cluster_fabric_credential_transaction_at_revision(
            dir.path(),
            &mut membership_winner,
            &BTreeMap::new(),
            revision,
        )
        .unwrap();

        let mut stale_delete = base;
        stale_delete.cluster_fabric.nodes.clear();
        stale_delete.cluster_fabric.clusters.clear();
        let error = persist_cluster_fabric_credential_transaction_at_revision(
            dir.path(),
            &mut stale_delete,
            &node_intents,
            revision,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ConfigStoreError::Conflict { actual: 2, .. }
        ));
        let (fresh, _) = active_facade_config_and_cluster_revision(dir.path());
        assert!(fresh.cluster_fabric.node("member").is_some());
        assert_eq!(
            fresh.cluster_fabric.cluster("second").unwrap().node_ids,
            ["member"]
        );
    }

    #[test]
    fn active_cluster_exact_transaction_recovers_every_durable_boundary_atomically() {
        for (index, fault) in [
            MigrationFault::AfterManifest,
            MigrationFault::AfterCredentials,
            MigrationFault::AfterAuthoritativeSection,
        ]
        .into_iter()
        .enumerate()
        {
            let _key = crate::encryption::set_test_encryption_key([0xb7 + index as u8; 32]);
            let dir = tempfile::tempdir().unwrap();
            crate::migrate_config_facade_layout(dir.path()).unwrap();
            let (mut candidate, revision) = active_facade_config_and_cluster_revision(dir.path());
            let node_id = format!("recover-node-{index}");
            let cluster_name = format!("recover-cluster-{index}");
            let secret = format!("recover-cluster-secret-{index}");
            candidate.cluster_fabric.nodes.push(cluster_test_node(
                &node_id,
                crate::SshAuth::Password {
                    password: String::new(),
                    password_encrypted: None,
                },
            ));
            candidate.cluster_fabric.clusters.push(crate::Cluster {
                name: cluster_name.clone(),
                description: None,
                node_ids: vec![node_id.clone()],
            });
            let intents = BTreeMap::from([(
                node_id.clone(),
                password_intents(crate::ClusterCredentialAction::Replace(secret.clone())),
            )]);

            let error = persist_cluster_fabric_credential_transaction_inner(
                dir.path(),
                &mut candidate,
                &intents,
                revision,
                None,
                Some(fault),
            )
            .unwrap_err();
            assert!(matches!(error, ConfigStoreError::Io(_)));
            assert!(
                crate::ConfigFacade::open(dir.path()).is_err(),
                "a partial durable boundary must not open as a fresh facade"
            );
            assert!(recover_pending_config_transaction(dir.path()).unwrap());
            let (fresh, committed_revision) = active_facade_config_and_cluster_revision(dir.path());
            assert_eq!(committed_revision, revision + 1);
            assert!(fresh.cluster_fabric.node(&node_id).is_some());
            assert_eq!(
                fresh
                    .cluster_fabric
                    .cluster(&cluster_name)
                    .unwrap()
                    .node_ids
                    .as_slice(),
                std::slice::from_ref(&node_id)
            );
            let reference = crate::cluster_password_credential_ref(&node_id).unwrap();
            assert_eq!(
                CredentialStore::open(dir.path())
                    .resolve(&reference)
                    .unwrap()
                    .unwrap()
                    .expose(),
                secret
            );
            assert_active_exact_manifest(dir.path(), ExactTransactionScope::ClusterFabric);
        }
    }

    #[test]
    fn active_env_exact_transaction_recovers_each_durable_boundary_before_fresh_open() {
        for (index, fault) in [
            MigrationFault::AfterManifest,
            MigrationFault::AfterCredentials,
            MigrationFault::AfterAuthoritativeSection,
        ]
        .into_iter()
        .enumerate()
        {
            let _key = crate::encryption::set_test_encryption_key([0x60 + index as u8; 32]);
            let dir = tempfile::tempdir().unwrap();
            crate::migrate_config_facade_layout(dir.path()).unwrap();
            let (mut candidate, revision) =
                active_facade_config_and_credential_revision(dir.path());
            candidate.env_vars.push(crate::EnvVarEntry {
                name: "RECOVER_TOKEN".to_string(),
                value: format!("recover-secret-{index}"),
                secret: true,
                value_encrypted: None,
                credential_ref: None,
                configured: true,
                description: None,
            });
            let intents = BTreeSet::from(["RECOVER_TOKEN".to_string()]);
            let error = persist_provider_credential_transaction_with_instances_inner(
                dir.path(),
                &mut candidate,
                &BTreeSet::new(),
                &BTreeSet::new(),
                None,
                &intents,
                Some(revision),
                false,
                &BTreeSet::new(),
                false,
                None,
                Some(fault),
            )
            .unwrap_err();
            assert!(matches!(error, ConfigStoreError::Io(_)));
            assert!(crate::ConfigFacade::open(dir.path()).is_err());
            assert!(recover_pending_config_transaction(dir.path()).unwrap());
            let (fresh, _) = active_facade_config_and_credential_revision(dir.path());
            let entry = fresh
                .env_vars
                .iter()
                .find(|entry| entry.name == "RECOVER_TOKEN")
                .unwrap();
            assert!(entry.configured);
            assert_active_exact_manifest(dir.path(), ExactTransactionScope::EnvVars);
        }
    }

    fn advance_env_section(data_dir: &Path, update: impl FnOnce(&mut Vec<Value>)) -> u64 {
        let path = data_dir.join(ENV_FILE);
        let mut envelope: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let revision = envelope["revision"].as_u64().unwrap() + 1;
        envelope["revision"] = Value::from(revision);
        update(envelope["data"].as_array_mut().unwrap());
        let bytes = serde_json::to_vec_pretty(&envelope).unwrap();
        crate::section_facade::validate_section_envelope(ENV_FILE, &bytes, revision).unwrap();
        std::fs::write(path, bytes).unwrap();
        revision
    }

    fn crash_active_env_after_credentials(data_dir: &Path, description: &str) -> CredentialRef {
        let (mut candidate, revision) = active_facade_config_and_credential_revision(data_dir);
        candidate.env_vars.push(crate::EnvVarEntry {
            name: "RACE_TOKEN".to_string(),
            value: "race-secret".to_string(),
            secret: true,
            value_encrypted: None,
            credential_ref: None,
            configured: true,
            description: Some(description.to_string()),
        });
        let error = persist_provider_credential_transaction_with_instances_inner(
            data_dir,
            &mut candidate,
            &BTreeSet::new(),
            &BTreeSet::new(),
            None,
            &BTreeSet::from(["RACE_TOKEN".to_string()]),
            Some(revision),
            false,
            &BTreeSet::new(),
            false,
            None,
            Some(MigrationFault::AfterCredentials),
        )
        .unwrap_err();
        assert!(matches!(error, ConfigStoreError::Io(_)));
        crate::credential_ref("env", "RACE_TOKEN", "value").unwrap()
    }

    #[test]
    fn active_env_recovery_rebases_forward_and_preserves_future_metadata() {
        let _key = crate::encryption::set_test_encryption_key([0x64; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::migrate_config_facade_layout(dir.path()).unwrap();
        let reference = crash_active_env_after_credentials(dir.path(), "staged");
        let (_, staged) = {
            let manifest: MigrationManifest =
                serde_json::from_slice(&std::fs::read(dir.path().join(MANIFEST_FILE)).unwrap())
                    .unwrap();
            initial_exact_transaction_member(dir.path(), &manifest, ENV_FILE).unwrap()
        };
        let staged: Value = serde_json::from_slice(&staged).unwrap();
        let mut staged_entry = staged["data"][0].clone();
        staged_entry["future_metadata"] = serde_json::json!({"preserved": true});
        advance_env_section(dir.path(), |entries| {
            entries.push(staged_entry);
            entries.push(serde_json::json!({
                "name": "UNRELATED",
                "value": "external",
                "secret": false,
                "configured": true
            }));
        });

        assert!(recover_pending_config_transaction(dir.path()).unwrap());
        let (fresh, _) = active_facade_config_and_credential_revision(dir.path());
        let raced = fresh
            .env_vars
            .iter()
            .find(|entry| entry.name == "RACE_TOKEN")
            .unwrap();
        assert!(raced.configured);
        assert!(fresh.env_vars.iter().any(|entry| entry.name == "UNRELATED"));
        let raw: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(ENV_FILE)).unwrap()).unwrap();
        assert_eq!(raw["data"][0]["future_metadata"]["preserved"], true);
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "race-secret"
        );
    }

    #[test]
    fn active_env_same_field_conflict_aborts_forward_without_dangling_reference() {
        let _key = crate::encryption::set_test_encryption_key([0x65; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::migrate_config_facade_layout(dir.path()).unwrap();
        let reference = crash_active_env_after_credentials(dir.path(), "staged");
        advance_env_section(dir.path(), |entries| {
            entries.push(serde_json::json!({
                "name": "RACE_TOKEN",
                "value": "",
                "secret": true,
                "credential_ref": reference.as_str(),
                "configured": true,
                "description": "external-winner"
            }));
        });

        let error = recover_pending_config_transaction(dir.path()).unwrap_err();
        assert!(error.to_string().contains("same domain"));
        assert!(!dir.path().join(MANIFEST_FILE).exists());
        let (fresh, _) = active_facade_config_and_credential_revision(dir.path());
        let raced = fresh
            .env_vars
            .iter()
            .find(|entry| entry.name == "RACE_TOKEN")
            .unwrap();
        assert_eq!(raced.description.as_deref(), Some("external-winner"));
        assert_eq!(raced.credential_ref.as_ref(), Some(&reference));
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "race-secret"
        );
    }

    #[test]
    fn recovery_only_resumes_pending_exact_transaction_and_returns_true() {
        let _key = crate::encryption::set_test_encryption_key([0x46; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let (mut candidate, intents) = provider_transaction_candidate("sk-recovery-only");
        assert!(persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterManifest),
        )
        .is_err());

        assert!(recover_pending_config_transaction(dir.path()).unwrap());
        let reference = credential_ref("provider", "openai", "api_key").unwrap();
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "sk-recovery-only"
        );
        assert!(!dir.path().join(JOURNAL_FILE).exists());
        let manifest: MigrationManifest =
            serde_json::from_slice(&std::fs::read(dir.path().join(MANIFEST_FILE)).unwrap())
                .unwrap();
        assert_eq!(manifest.state, MigrationState::Complete);
    }

    #[test]
    fn recovery_only_without_manifest_returns_false_without_writes() {
        let dir = tempfile::tempdir().unwrap();
        let sentinel_path = dir.path().join("sentinel");
        let sentinel = b"directory-must-remain-byte-identical".to_vec();
        std::fs::write(&sentinel_path, &sentinel).unwrap();

        assert!(!recover_pending_config_transaction(dir.path()).unwrap());

        assert_eq!(std::fs::read(&sentinel_path).unwrap(), sentinel);
        assert!(!dir.path().join(LOCK_FILE).exists());
        assert_no_migration_transaction_artifacts(dir.path());

        let missing = dir.path().join("does-not-exist");
        assert!(!recover_pending_config_transaction(&missing).unwrap());
        assert!(!missing.exists());
    }

    #[test]
    fn recovery_only_preserves_journal_only_uncommitted_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let journal = b"journal-only-must-survive".to_vec();
        std::fs::write(dir.path().join(JOURNAL_FILE), &journal).unwrap();
        let orphan = dir.path().join(format!("{STAGE_PREFIX}{}", Uuid::new_v4()));
        std::fs::create_dir(&orphan).unwrap();
        std::fs::write(orphan.join("sentinel"), b"must-survive").unwrap();

        assert!(!recover_pending_config_transaction(dir.path()).unwrap());

        assert_eq!(
            std::fs::read(dir.path().join(JOURNAL_FILE)).unwrap(),
            journal
        );
        assert_eq!(
            std::fs::read(orphan.join("sentinel")).unwrap(),
            b"must-survive"
        );
        assert!(!dir.path().join(MANIFEST_FILE).exists());
        assert!(!dir.path().join(LOCK_FILE).exists());
    }

    #[test]
    fn recovery_only_malformed_manifest_fails_closed_without_new_migration() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(LOCK_FILE), b"lock-sentinel").unwrap();
        let manifest = b"{malformed-manifest".to_vec();
        let journal = b"journal-must-survive".to_vec();
        std::fs::write(dir.path().join(MANIFEST_FILE), &manifest).unwrap();
        std::fs::write(dir.path().join(JOURNAL_FILE), &journal).unwrap();

        assert!(recover_pending_config_transaction(dir.path()).is_err());

        assert_eq!(
            std::fs::read(dir.path().join(MANIFEST_FILE)).unwrap(),
            manifest
        );
        assert_eq!(
            std::fs::read(dir.path().join(JOURNAL_FILE)).unwrap(),
            journal
        );
        assert!(!std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .any(|entry| {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                name.starts_with(STAGE_PREFIX) || name.starts_with(BACKUP_PREFIX)
            }));
    }

    fn provider_instance_transaction_candidate(
        instance_id: &str,
        secret: &str,
    ) -> (crate::Config, BTreeSet<String>) {
        let mut config = crate::Config::default();
        let instance: crate::ProviderInstanceConfig = serde_json::from_value(serde_json::json!({
            "provider_type": "openai",
            "label": "Work",
            "api_key": secret,
            "model": "gpt-test",
            "unknown_instance_field": "must-survive"
        }))
        .unwrap();
        config
            .provider_instances
            .insert(instance_id.to_string(), instance);
        (config, BTreeSet::from([instance_id.to_string()]))
    }

    #[test]
    fn legacy_provider_instances_migrate_recoverably_and_scrub_backups() {
        let _key = crate::encryption::set_test_encryption_key([0x81; 32]);
        let dir = tempfile::tempdir().unwrap();
        let ciphertext = crate::encryption::encrypt("sk-cipher-instance").unwrap();
        let root = serde_json::json!({
            "provider_instances": {
                "work unsafe/id": {
                    "provider_type": "openai",
                    "api_key": "sk-plain-instance",
                    "api_key_encrypted": crate::encryption::encrypt("sk-plain-instance").unwrap(),
                    "unknown_instance_field": "must-survive"
                },
                "personal": {
                    "provider_type": "anthropic",
                    "api_key_encrypted": ciphertext
                }
            },
            "unknown_root_field": "must-survive"
        });
        let bytes = serde_json::to_vec_pretty(&root).unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), &bytes).unwrap();
        std::fs::write(dir.path().join("config.json.bak"), &bytes).unwrap();

        assert!(migrate_with_fault(dir.path(), MigrationFault::AfterConfig).is_err());
        let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert!(outcome.resumed);

        for path in [
            dir.path().join(CONFIG_FILE),
            dir.path().join("config.json.bak"),
        ] {
            let value: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
            assert_eq!(value["unknown_root_field"], "must-survive");
            assert_eq!(
                value["provider_instances"]["work unsafe/id"]["unknown_instance_field"],
                "must-survive"
            );
            for id in ["work unsafe/id", "personal"] {
                let instance = &value["provider_instances"][id];
                assert!(instance.get("api_key").is_none());
                assert!(instance.get("api_key_encrypted").is_none());
                assert!(instance.get("credential_ref").is_some());
            }
        }
        let loaded = crate::Config::from_data_dir_without_env(Some(dir.path().to_path_buf()));
        assert_eq!(
            loaded.provider_instances["work unsafe/id"].api_key,
            "sk-plain-instance"
        );
        assert_eq!(
            loaded.provider_instances["personal"].api_key,
            "sk-cipher-instance"
        );
        let credentials = std::fs::read_to_string(dir.path().join(CREDENTIALS_FILE)).unwrap();
        assert!(!credentials.contains("sk-plain-instance"));
        assert!(!credentials.contains("sk-cipher-instance"));
    }

    #[test]
    fn provider_facade_recovery_accepts_newer_secret_free_root_without_poisoning_manifest() {
        let _key = crate::encryption::set_test_encryption_key([0x82; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "providers": {"openai": {
                    "api_key_encrypted": crate::encryption::encrypt("legacy-secret").unwrap()
                }}
            }))
            .unwrap(),
        )
        .unwrap();

        assert!(migrate_inner_with_root_builtins(
            dir.path(),
            Some(MigrationFault::AfterManifest),
            true,
        )
        .is_err());
        let newer = serde_json::to_vec_pretty(&serde_json::json!({
            "providers": {"openai": {"model": "newer-model"}},
            "concurrent_unknown": true
        }))
        .unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), &newer).unwrap();

        let outcome = migrate_inner_with_root_builtins(dir.path(), None, true).unwrap();
        assert!(outcome.resumed);
        assert_eq!(std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(), newer);
        ensure_provider_mcp_migration_ready(dir.path()).unwrap();
        let manifest: MigrationManifest =
            serde_json::from_slice(&std::fs::read(dir.path().join(MANIFEST_FILE)).unwrap())
                .unwrap();
        assert_eq!(manifest.state, MigrationState::Complete);
        assert_eq!(manifest.migration_scope, None);
        assert!(!manifest.files.iter().any(|file| file.name == CONFIG_FILE));
        let reference = credential_ref("provider", "openai", "api_key").unwrap();
        assert!(CredentialStore::open(dir.path())
            .resolve(&reference)
            .unwrap()
            .is_none());
    }

    #[test]
    fn provider_facade_recovery_preserves_a_shared_ref_still_used_by_provider_section() {
        let _key = crate::encryption::set_test_encryption_key([0x83; 32]);
        let dir = tempfile::tempdir().unwrap();
        let ciphertext = crate::encryption::encrypt("shared-secret").unwrap();
        std::fs::write(
            dir.path().join(PROVIDERS_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "openai": {
                    "api_key_encrypted": ciphertext,
                    "model": "provider-model"
                }
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "providers": {"openai": {
                    "api_key_encrypted": crate::encryption::encrypt("shared-secret").unwrap(),
                    "model": "root-model"
                }}
            }))
            .unwrap(),
        )
        .unwrap();

        assert!(migrate_inner_with_root_builtins(
            dir.path(),
            Some(MigrationFault::AfterManifest),
            true,
        )
        .is_err());
        let newer = serde_json::to_vec_pretty(&serde_json::json!({
            "providers": {"openai": {"model": "newer-model"}},
            "concurrent_unknown": true
        }))
        .unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), &newer).unwrap();

        let outcome = migrate_inner_with_root_builtins(dir.path(), None, true).unwrap();
        assert!(outcome.resumed);
        assert_eq!(std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(), newer);
        let providers: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap())
                .unwrap();
        let reference = credential_ref("provider", "openai", "api_key").unwrap();
        assert_eq!(
            providers["data"]["openai"]["credential_ref"].as_str(),
            Some(reference.as_str())
        );
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "shared-secret"
        );
        ensure_provider_mcp_migration_ready(dir.path()).unwrap();
        let manifest: MigrationManifest =
            serde_json::from_slice(&std::fs::read(dir.path().join(MANIFEST_FILE)).unwrap())
                .unwrap();
        assert_eq!(manifest.state, MigrationState::Complete);
        assert_eq!(manifest.migration_scope, None);
        assert!(!manifest.files.iter().any(|file| file.name == CONFIG_FILE));
    }

    #[test]
    fn migration_rejects_shared_ref_with_different_legacy_secrets_without_writes() {
        let _key = crate::encryption::set_test_encryption_key([0x89; 32]);
        let dir = tempfile::tempdir().unwrap();
        let shared_ref = "provider_instance.shared.api_key";
        let original = serde_json::to_vec_pretty(&serde_json::json!({
            "provider_instances": {
                "first": {
                    "provider_type": "openai",
                    "api_key": "sk-first",
                    "credential_ref": shared_ref
                },
                "second": {
                    "provider_type": "anthropic",
                    "api_key": "sk-second",
                    "credential_ref": shared_ref
                }
            }
        }))
        .unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), &original).unwrap();

        assert!(migrate_with_fault(dir.path(), MigrationFault::None).is_err());
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            original
        );
        assert!(!dir.path().join(CREDENTIALS_FILE).exists());
        assert!(!dir.path().join(MANIFEST_FILE).exists());
    }

    #[test]
    fn migration_deduplicates_shared_ref_with_the_same_legacy_secret() {
        let _key = crate::encryption::set_test_encryption_key([0x8a; 32]);
        let dir = tempfile::tempdir().unwrap();
        let shared_ref = CredentialRef::parse("provider_instance.shared.api_key").unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "provider_instances": {
                    "first": {
                        "provider_type": "openai",
                        "api_key": "sk-same",
                        "credential_ref": shared_ref.as_str()
                    },
                    "second": {
                        "provider_type": "anthropic",
                        "api_key": "sk-same",
                        "credential_ref": shared_ref.as_str()
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert_eq!(outcome.migrated_credentials, 1);
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&shared_ref)
                .unwrap()
                .unwrap()
                .expose(),
            "sk-same"
        );
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        for id in ["first", "second"] {
            assert!(root["provider_instances"][id].get("api_key").is_none());
            assert_eq!(
                root["provider_instances"][id]["credential_ref"],
                shared_ref.as_str()
            );
        }
    }

    #[test]
    fn backup_only_provider_instance_is_stored_before_backup_is_scrubbed() {
        let _key = crate::encryption::set_test_encryption_key([0x85; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let store = CredentialStore::open(dir.path());
        let existing_ref = credential_ref("provider_instance", "shared", "api_key").unwrap();
        store
            .replace(
                existing_ref.clone(),
                "sk-current-winner",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let provider_ref = credential_ref("provider", "openai", "api_key").unwrap();
        store
            .replace(
                provider_ref.clone(),
                "sk-built-in-survives-corrupt-root",
                crate::CredentialSource::User,
                store.revision().unwrap(),
            )
            .unwrap();
        let mcp_ref = credential_ref("mcp", "backup-stdio", "env_TOKEN").unwrap();
        store
            .replace(
                mcp_ref.clone(),
                "mcp-survives-corrupt-root",
                crate::CredentialSource::User,
                store.revision().unwrap(),
            )
            .unwrap();
        std::fs::write(
            dir.path().join(PROVIDERS_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "openai": {
                    "credential_ref": provider_ref.as_str(),
                    "model": "gpt-sidecar"
                }
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("config.json.bak"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "provider_instances": {
                    "personal": {
                        "provider_type": "anthropic",
                        "api_key": "sk-backup-only",
                        "model": "backup-model"
                    },
                    "shared": {
                        "provider_type": "openai",
                        "api_key": "sk-stale-backup"
                    }
                },
                "mcpServers": {
                    "backup-stdio": {
                        "command": "unused-disabled-command",
                        "enabled": false,
                        "env_credential_refs": {"TOKEN": mcp_ref.as_str()},
                        "request_timeout_ms": 100,
                        "healthcheck_interval_ms": 100
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();

        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        let reference = credential_ref("provider_instance", "personal", "api_key").unwrap();
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "sk-backup-only"
        );
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&existing_ref)
                .unwrap()
                .unwrap()
                .expose(),
            "sk-current-winner"
        );
        let backup: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("config.json.bak")).unwrap())
                .unwrap();
        assert!(backup["provider_instances"]["personal"]
            .get("api_key")
            .is_none());
        assert_eq!(
            backup["provider_instances"]["personal"]["credential_ref"],
            reference.as_str()
        );
        assert!(backup["provider_instances"]["shared"]
            .get("api_key")
            .is_none());
        assert_eq!(
            backup["provider_instances"]["shared"]["credential_ref"],
            existing_ref.as_str()
        );

        std::fs::write(dir.path().join(CONFIG_FILE), b"{broken").unwrap();
        let recovered = crate::Config::from_data_dir_without_env(Some(dir.path().to_path_buf()));
        assert_eq!(
            recovered.provider_instances["personal"].api_key,
            "sk-backup-only"
        );
        assert_eq!(
            recovered.provider_instances["personal"].model.as_deref(),
            Some("backup-model")
        );
        assert_eq!(
            recovered.providers.openai.as_ref().unwrap().api_key,
            "sk-built-in-survives-corrupt-root"
        );
        let stdio = match &recovered.mcp.servers[0].transport {
            bamboo_domain::mcp_config::TransportConfig::Stdio(stdio) => stdio,
            other => panic!("expected stdio transport, got {other:?}"),
        };
        assert_eq!(
            stdio.env.get("TOKEN").map(String::as_str),
            Some("mcp-survives-corrupt-root")
        );
    }

    #[test]
    fn provider_instance_transaction_keeps_only_refs_through_generic_saves() {
        let _key = crate::encryption::set_test_encryption_key([0x82; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let (mut candidate, instance_intents) =
            provider_instance_transaction_candidate("work", "sk-instance-transaction");
        persist_provider_credential_transaction_with_instances_inner(
            dir.path(),
            &mut candidate,
            &BTreeSet::new(),
            &instance_intents,
            None,
            &BTreeSet::new(),
            None,
            false,
            &BTreeSet::new(),
            false,
            None,
            None,
        )
        .unwrap();

        let reference = credential_ref("provider_instance", "work", "api_key").unwrap();
        assert_eq!(
            candidate.provider_instances["work"].credential_ref.as_ref(),
            Some(&reference)
        );
        candidate.provider_instances.get_mut("work").unwrap().label = Some("Renamed".to_string());
        candidate.save_to_dir(dir.path().to_path_buf()).unwrap();

        for suffix in ["", ".bak", ".bak.1", ".bak.2"] {
            let path = dir.path().join(format!("config.json{suffix}"));
            let Ok(bytes) = std::fs::read(path) else {
                continue;
            };
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            let Some(instance) = value
                .get("provider_instances")
                .and_then(|instances| instances.get("work"))
            else {
                continue;
            };
            assert!(instance.get("api_key").is_none());
            assert!(instance.get("api_key_encrypted").is_none());
            assert_eq!(instance["credential_ref"], reference.as_str());
        }
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "sk-instance-transaction"
        );
    }

    #[test]
    fn provider_instance_delete_clears_custom_ref_and_survives_concurrent_clear() {
        let _key = crate::encryption::set_test_encryption_key([0x83; 32]);
        let dir = tempfile::tempdir().unwrap();
        let custom_ref = CredentialRef::parse("provider_instance.custom.secret").unwrap();
        CredentialStore::open(dir.path())
            .replace(
                custom_ref.clone(),
                "sk-delete-me",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let mut current = crate::Config::default();
        let mut instance: crate::ProviderInstanceConfig = serde_json::from_value(
            serde_json::json!({"provider_type": "openai", "model": "gpt-test"}),
        )
        .unwrap();
        instance.credential_ref = Some(custom_ref.clone());
        current
            .provider_instances
            .insert("work".to_string(), instance);
        current.save_to_dir(dir.path().to_path_buf()).unwrap();

        let mut candidate = current.clone();
        candidate.provider_instances.remove("work");
        persist_provider_credential_transaction_with_instances_inner(
            dir.path(),
            &mut candidate,
            &BTreeSet::new(),
            &BTreeSet::from(["work".to_string()]),
            None,
            &BTreeSet::new(),
            None,
            false,
            &BTreeSet::new(),
            false,
            None,
            Some(MigrationFault::AfterExactCommitCredentialClearRace),
        )
        .unwrap();

        assert!(CredentialStore::open(dir.path())
            .resolve(&custom_ref)
            .unwrap()
            .is_none());
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert!(root["provider_instances"].get("work").is_none());
    }

    #[test]
    fn provider_instance_delete_preserves_a_ref_used_by_other_instances_and_mcp() {
        let _key = crate::encryption::set_test_encryption_key([0x88; 32]);
        let dir = tempfile::tempdir().unwrap();
        let shared_ref = CredentialRef::parse("shared.provider.and.mcp.secret").unwrap();
        CredentialStore::open(dir.path())
            .replace(
                shared_ref.clone(),
                "sk-shared-survivor",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let mut current = crate::Config::default();
        for id in ["delete-me", "keep-me"] {
            let mut instance: crate::ProviderInstanceConfig = serde_json::from_value(
                serde_json::json!({"provider_type": "openai", "model": "gpt-test"}),
            )
            .unwrap();
            instance.credential_ref = Some(shared_ref.clone());
            current.provider_instances.insert(id.to_string(), instance);
        }
        current
            .mcp
            .servers
            .push(bamboo_domain::mcp_config::McpServerConfig {
                id: "shared-stdio".to_string(),
                name: None,
                enabled: false,
                transport: bamboo_domain::mcp_config::TransportConfig::Stdio(
                    bamboo_domain::mcp_config::StdioConfig {
                        command: "unused".to_string(),
                        args: Vec::new(),
                        cwd: None,
                        env: Default::default(),
                        env_encrypted: Default::default(),
                        env_credential_refs: std::collections::HashMap::from([(
                            "TOKEN".to_string(),
                            shared_ref.as_str().to_string(),
                        )]),
                        startup_timeout_ms: 100,
                    },
                ),
                request_timeout_ms: 100,
                healthcheck_interval_ms: 100,
                reconnect: Default::default(),
                allowed_tools: Vec::new(),
                denied_tools: Vec::new(),
            });
        current.save_to_dir(dir.path().to_path_buf()).unwrap();

        let mut candidate = current.clone();
        candidate.provider_instances.remove("delete-me");
        persist_provider_instance_credential_transaction(
            dir.path(),
            &mut candidate,
            &BTreeSet::new(),
            &BTreeSet::from(["delete-me".to_string()]),
        )
        .unwrap();

        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&shared_ref)
                .unwrap()
                .unwrap()
                .expose(),
            "sk-shared-survivor"
        );
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert!(root["provider_instances"].get("delete-me").is_none());
        assert_eq!(
            root["provider_instances"]["keep-me"]["credential_ref"],
            shared_ref.as_str()
        );
        assert_eq!(
            root["mcpServers"]["shared-stdio"]["env_credential_refs"]["TOKEN"],
            shared_ref.as_str()
        );
    }

    #[test]
    fn dangling_provider_instance_ref_fails_closed_without_rewriting_metadata() {
        let _key = crate::encryption::set_test_encryption_key([0x84; 32]);
        let dir = tempfile::tempdir().unwrap();
        let reference = credential_ref("provider_instance", "work", "api_key").unwrap();
        let mut config = crate::Config::default();
        let mut instance: crate::ProviderInstanceConfig = serde_json::from_value(
            serde_json::json!({"provider_type": "openai", "model": "gpt-test"}),
        )
        .unwrap();
        instance.credential_ref = Some(reference.clone());
        config
            .provider_instances
            .insert("work".to_string(), instance);
        config.save_to_dir(dir.path().to_path_buf()).unwrap();
        let before = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();

        let loaded = crate::Config::from_data_dir_without_env(Some(dir.path().to_path_buf()));
        assert!(loaded.provider_instances["work"].api_key.is_empty());
        assert_eq!(
            loaded.provider_instances["work"].credential_ref.as_ref(),
            Some(&reference)
        );
        assert_eq!(std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(), before);
        assert!(!dir.path().join(CREDENTIALS_FILE).exists());
    }

    #[test]
    fn provider_transaction_precommit_failure_leaves_all_originals_unchanged() {
        let _key = crate::encryption::set_test_encryption_key([0x71; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let original_config = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        let original_providers = std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap();
        let original_credentials = std::fs::read(dir.path().join(CREDENTIALS_FILE)).ok();
        let (mut candidate, intents) = provider_transaction_candidate("sk-precommit");

        assert!(persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterStaging),
        )
        .is_err());
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            original_config
        );
        assert_eq!(
            std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap(),
            original_providers
        );
        assert_eq!(
            std::fs::read(dir.path().join(CREDENTIALS_FILE)).ok(),
            original_credentials
        );
    }

    #[cfg(unix)]
    #[test]
    fn provider_transaction_rejects_symlink_target_without_following_or_writing() {
        use std::os::unix::fs::symlink;

        let _key = crate::encryption::set_test_encryption_key([0x70; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let config_before = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        let credentials_before = std::fs::read(dir.path().join(CREDENTIALS_FILE)).ok();

        let external = tempfile::tempdir().unwrap();
        let external_target = external.path().join("providers-external.json");
        let external_before = br#"{"must":"survive"}"#.to_vec();
        std::fs::write(&external_target, &external_before).unwrap();
        let providers_path = dir.path().join(PROVIDERS_FILE);
        std::fs::remove_file(&providers_path).unwrap();
        symlink(&external_target, &providers_path).unwrap();

        let (mut candidate, intents) = provider_transaction_candidate("sk-symlink-target");
        let error = persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterStaging),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            ConfigStoreError::Io(ref source)
                if source.kind() == std::io::ErrorKind::InvalidInput
        ));
        assert!(std::fs::symlink_metadata(&providers_path)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(std::fs::read(&external_target).unwrap(), external_before);
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            config_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(CREDENTIALS_FILE)).ok(),
            credentials_before
        );
        assert_no_migration_transaction_artifacts(dir.path());
    }

    #[cfg(unix)]
    #[test]
    fn legacy_migration_rejects_symlink_backup_source_without_following_or_writing() {
        use std::os::unix::fs::symlink;

        let _key = crate::encryption::set_test_encryption_key([0x6e; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let config_before = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        let providers_before = std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap();
        let credentials_before = std::fs::read(dir.path().join(CREDENTIALS_FILE)).ok();

        let external = tempfile::tempdir().unwrap();
        let external_source = external.path().join("config-backup-external.json");
        let external_before = br#"{"provider_instances":{}}"#.to_vec();
        std::fs::write(&external_source, &external_before).unwrap();
        let backup_path = dir.path().join(format!("{CONFIG_FILE}.bak"));
        symlink(&external_source, &backup_path).unwrap();

        let error = migrate_provider_mcp_credentials(dir.path()).unwrap_err();

        assert!(matches!(
            error,
            ConfigStoreError::Io(ref source)
                if source.kind() == std::io::ErrorKind::InvalidInput
        ));
        assert!(std::fs::symlink_metadata(&backup_path)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(std::fs::read(&external_source).unwrap(), external_before);
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            config_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap(),
            providers_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(CREDENTIALS_FILE)).ok(),
            credentials_before
        );
        assert_no_migration_transaction_artifacts(dir.path());
    }

    #[test]
    fn provider_transaction_resumes_after_credential_or_metadata_boundary() {
        for fault in [
            MigrationFault::AfterCredentials,
            MigrationFault::AfterProviders,
        ] {
            let _key = crate::encryption::set_test_encryption_key([0x72; 32]);
            let dir = tempfile::tempdir().unwrap();
            crate::Config::default()
                .save_to_dir(dir.path().to_path_buf())
                .unwrap();
            let (mut candidate, intents) = provider_transaction_candidate("sk-resume");
            assert!(persist_provider_credential_transaction_inner(
                dir.path(),
                &mut candidate,
                &intents,
                Some(fault),
            )
            .is_err());
            assert!(ensure_provider_mcp_migration_ready(dir.path()).is_err());

            let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
            assert!(outcome.resumed);
            ensure_provider_mcp_migration_ready(dir.path()).unwrap();
            let loaded = crate::Config::from_data_dir(Some(dir.path().to_path_buf()));
            assert_eq!(loaded.provider, "openai");
            assert_eq!(
                loaded.providers.openai.as_ref().unwrap().model.as_deref(),
                Some("transaction-model")
            );
            assert_eq!(
                CredentialStore::open(dir.path())
                    .resolve(&credential_ref("provider", "openai", "api_key").unwrap())
                    .unwrap()
                    .unwrap()
                    .expose(),
                "sk-resume"
            );
        }
    }

    #[test]
    fn provider_transaction_credential_cas_loser_never_commits_metadata() {
        let _key = crate::encryption::set_test_encryption_key([0x73; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let original_config = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        let original_providers = std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap();
        let (mut candidate, intents) = provider_transaction_candidate("sk-loser");
        let error = persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::BeforeExactCommitCredentialRace),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ConfigStoreError::Conflict {
                expected: 0,
                actual: 1
            }
        ));
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            original_config
        );
        assert_eq!(
            std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap(),
            original_providers
        );
        let reference = credential_ref("provider", "openai", "api_key").unwrap();
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "concurrent-winner"
        );
        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
    }

    #[test]
    fn provider_transaction_keeps_a_post_commit_credential_winner_and_finishes_metadata() {
        let _key = crate::encryption::set_test_encryption_key([0x79; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let (mut candidate, intents) = provider_transaction_candidate("sk-transaction-loser");

        persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterExactCommitCredentialRace),
        )
        .unwrap();

        ensure_provider_mcp_migration_ready(dir.path()).unwrap();
        let reference = credential_ref("provider", "openai", "api_key").unwrap();
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "concurrent-post-commit-winner"
        );
        let loaded = crate::Config::from_data_dir(Some(dir.path().to_path_buf()));
        let provider = loaded.providers.openai.as_ref().unwrap();
        assert_eq!(provider.model.as_deref(), Some("transaction-model"));
        assert_eq!(provider.credential_ref.as_ref(), Some(&reference));
    }

    #[test]
    fn provider_transaction_finishes_when_a_post_commit_clear_wins_the_same_ref() {
        let _key = crate::encryption::set_test_encryption_key([0x7d; 32]);
        let dir = tempfile::tempdir().unwrap();
        let reference = credential_ref("provider", "openai", "api_key").unwrap();
        let store = CredentialStore::open(dir.path());
        store
            .replace(
                reference.clone(),
                "previous-openai-secret",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let (mut candidate, intents) = provider_transaction_candidate("transaction-loses-to-clear");

        persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterExactCommitCredentialClearRace),
        )
        .unwrap();

        ensure_provider_mcp_migration_ready(dir.path()).unwrap();
        assert!(store.resolve(&reference).unwrap().is_none());
        let providers: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap())
                .unwrap();
        let provider_data = providers.get("data").unwrap_or(&providers);
        assert_eq!(
            provider_data["openai"]["model"],
            Value::String("transaction-model".to_string())
        );
        assert_eq!(
            provider_data["openai"]["credential_ref"],
            Value::String(reference.as_str().to_string())
        );
        let loaded = crate::Config::from_data_dir(Some(dir.path().to_path_buf()));
        assert!(
            loaded.providers.openai.is_none(),
            "missing ref must fail closed"
        );
    }

    #[test]
    fn provider_transaction_merges_an_unrelated_post_commit_credential_winner() {
        let _key = crate::encryption::set_test_encryption_key([0x7a; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let (mut candidate, intents) = provider_transaction_candidate("sk-transaction-openai");

        persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterExactCommitUnrelatedCredentialRace),
        )
        .unwrap();

        ensure_provider_mcp_migration_ready(dir.path()).unwrap();
        let store = CredentialStore::open(dir.path());
        let openai = credential_ref("provider", "openai", "api_key").unwrap();
        let anthropic = credential_ref("provider", "anthropic", "api_key").unwrap();
        assert_eq!(
            store.resolve(&openai).unwrap().unwrap().expose(),
            "sk-transaction-openai"
        );
        assert_eq!(
            store.resolve(&anthropic).unwrap().unwrap().expose(),
            "concurrent-unrelated-winner"
        );
        let loaded = crate::Config::from_data_dir(Some(dir.path().to_path_buf()));
        assert_eq!(
            loaded
                .providers
                .openai
                .as_ref()
                .and_then(|provider| provider.credential_ref.as_ref()),
            Some(&openai)
        );
    }

    #[test]
    fn provider_transaction_rebase_resumes_across_each_durable_boundary() {
        for fault in [
            MigrationFault::AfterExactCredentialRebaseStage,
            MigrationFault::AfterExactCredentialRebaseManifest,
        ] {
            let _key = crate::encryption::set_test_encryption_key([0x7b; 32]);
            let dir = tempfile::tempdir().unwrap();
            crate::Config::default()
                .save_to_dir(dir.path().to_path_buf())
                .unwrap();
            let (mut candidate, intents) = provider_transaction_candidate("sk-rebase-resume");

            assert!(persist_provider_credential_transaction_inner(
                dir.path(),
                &mut candidate,
                &intents,
                Some(fault),
            )
            .is_err());
            assert!(ensure_provider_mcp_migration_ready(dir.path()).is_err());

            let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
            assert!(outcome.resumed);
            ensure_provider_mcp_migration_ready(dir.path()).unwrap();
            let store = CredentialStore::open(dir.path());
            let openai = credential_ref("provider", "openai", "api_key").unwrap();
            let anthropic = credential_ref("provider", "anthropic", "api_key").unwrap();
            assert_eq!(
                store.resolve(&openai).unwrap().unwrap().expose(),
                "sk-rebase-resume"
            );
            assert_eq!(
                store.resolve(&anthropic).unwrap().unwrap().expose(),
                "concurrent-unrelated-winner"
            );
        }
    }

    #[test]
    fn provider_transaction_rejects_a_malformed_higher_revision_credential_winner() {
        let _key = crate::encryption::set_test_encryption_key([0x7c; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let providers_before = std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap();
        let config_before = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        let (mut candidate, intents) = provider_transaction_candidate("sk-must-not-publish");
        assert!(persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::AfterManifest),
        )
        .is_err());

        std::fs::write(
            dir.path().join(CREDENTIALS_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema_version": 1,
                "revision": 1,
                "data": {
                    "entries": {
                        "provider.openai.api_key": {
                            "ciphertext": "",
                            "source": "user",
                            "updated_at": "2026-07-19T00:00:00Z",
                            "key_version": 1
                        }
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();

        assert!(migrate_with_fault(dir.path(), MigrationFault::None).is_err());
        assert!(ensure_provider_mcp_migration_ready(dir.path()).is_err());
        assert_eq!(
            std::fs::read(dir.path().join(PROVIDERS_FILE)).unwrap(),
            providers_before
        );
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            config_before
        );
    }

    #[test]
    fn provider_transaction_rejects_exhausted_provider_revision_before_commit() {
        let _key = crate::encryption::set_test_encryption_key([0x77; 32]);
        let dir = tempfile::tempdir().unwrap();
        crate::Config::default()
            .save_to_dir(dir.path().to_path_buf())
            .unwrap();
        let provider_path = dir.path().join(PROVIDERS_FILE);
        let provider_data: Value =
            serde_json::from_slice(&std::fs::read(&provider_path).unwrap()).unwrap();
        std::fs::write(
            &provider_path,
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema_version": 1,
                "revision": u64::MAX,
                "data": provider_data
            }))
            .unwrap(),
        )
        .unwrap();
        let providers_before = std::fs::read(&provider_path).unwrap();
        let config_before = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        let (mut candidate, intents) = provider_transaction_candidate("sk-overflow");

        let error = persist_provider_credential_transaction_inner(
            dir.path(),
            &mut candidate,
            &intents,
            Some(MigrationFault::None),
        )
        .unwrap_err();
        assert!(error.to_string().contains("revision counter exhausted"));
        assert_eq!(std::fs::read(&provider_path).unwrap(), providers_before);
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            config_before
        );
        assert!(!dir.path().join(MANIFEST_FILE).exists());
    }

    fn assert_migrated_except_provider_value(dir: &Path) {
        let providers = std::fs::read_to_string(dir.join(PROVIDERS_FILE)).unwrap();
        let mcp = std::fs::read_to_string(dir.join(MCP_FILE)).unwrap();
        assert!(!providers.contains("sk-provider-plain"));
        assert!(!mcp.contains("mcp-header-secret"));
    }

    #[test]
    fn migration_errors_are_redacted() {
        let _key = crate::encryption::set_test_encryption_key([0x65; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(PROVIDERS_FILE),
            br#"{"openai":{"api_key_encrypted":"definitely-secret-bad-cipher"}}"#,
        )
        .unwrap();
        let error = migrate_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        let rendered = error.to_string();
        assert!(!rendered.contains("definitely-secret-bad-cipher"));
        assert!(!rendered.contains(dir.path().to_string_lossy().as_ref()));
    }

    #[test]
    fn legacy_secret_env_migrates_to_ref_and_scrubs_backups_idempotently() {
        let _key = crate::encryption::set_test_encryption_key([0x91; 32]);
        let dir = tempfile::tempdir().unwrap();
        let ciphertext = crate::encryption::encrypt("legacy-env-secret").unwrap();
        let legacy = serde_json::json!({
            "future_root": {"kept": true},
            "env_vars": [{
                "name": "PRIVATE_TOKEN",
                "secret": true,
                "value_encrypted": ciphertext,
                "future_entry": "kept"
            }]
        });
        for name in [CONFIG_FILE, "config.json.bak"] {
            std::fs::write(
                dir.path().join(name),
                serde_json::to_vec_pretty(&legacy).unwrap(),
            )
            .unwrap();
        }

        let first = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert_eq!(first.migrated_credentials, 1);
        let reference = credential_ref("env", "PRIVATE_TOKEN", "value").unwrap();
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "legacy-env-secret"
        );
        for name in [CONFIG_FILE, "config.json.bak"] {
            let document: Value =
                serde_json::from_slice(&std::fs::read(dir.path().join(name)).unwrap()).unwrap();
            let entry = &document["env_vars"][0];
            assert_eq!(entry["credential_ref"], reference.as_str());
            assert_eq!(entry["configured"], true);
            assert!(entry.get("value").is_none());
            assert!(entry.get("value_encrypted").is_none());
            assert_eq!(entry["future_entry"], "kept");
            assert_eq!(document["future_root"]["kept"], true);
        }
        let revision = CredentialStore::open(dir.path()).revision().unwrap();
        let second = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert_eq!(second.migrated_credentials, 0);
        assert_eq!(
            CredentialStore::open(dir.path()).revision().unwrap(),
            revision
        );
    }

    #[test]
    fn legacy_env_plaintext_and_ciphertext_must_agree_before_any_write() {
        let _key = crate::encryption::set_test_encryption_key([0x94; 32]);
        let dir = tempfile::tempdir().unwrap();
        let conflicting = serde_json::json!({
            "env_vars": [{
                "name": "TOKEN", "secret": true, "value": "plain-winner",
                "value_encrypted": crate::encryption::encrypt("different").unwrap()
            }]
        });
        let bytes = serde_json::to_vec_pretty(&conflicting).unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), &bytes).unwrap();
        let error = migrate_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("conflicting values"));
        assert_eq!(std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(), bytes);
        assert!(!dir.path().join(CREDENTIALS_FILE).exists());
        assert!(!dir.path().join(MANIFEST_FILE).exists());

        let matching = serde_json::json!({
            "env_vars": [{
                "name": "TOKEN", "secret": true, "value": "same",
                "value_encrypted": crate::encryption::encrypt("same").unwrap()
            }]
        });
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&matching).unwrap(),
        )
        .unwrap();
        let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert_eq!(outcome.migrated_credentials, 1);
        let revision = CredentialStore::open(dir.path()).revision().unwrap();
        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert_eq!(
            CredentialStore::open(dir.path()).revision().unwrap(),
            revision
        );
    }

    #[test]
    fn non_secret_env_legacy_ciphertext_is_scrubbed_from_root_and_backup() {
        let _key = crate::encryption::set_test_encryption_key([0x95; 32]);
        let dir = tempfile::tempdir().unwrap();
        let legacy = serde_json::json!({
            "env_vars": [{
                "name": "PUBLIC", "secret": false, "value": "visible",
                "value_encrypted": "must-not-remain"
            }]
        });
        for name in [CONFIG_FILE, "config.json.bak"] {
            std::fs::write(
                dir.path().join(name),
                serde_json::to_vec_pretty(&legacy).unwrap(),
            )
            .unwrap();
        }
        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        for name in [CONFIG_FILE, "config.json.bak"] {
            let bytes = std::fs::read_to_string(dir.path().join(name)).unwrap();
            assert!(!bytes.contains("value_encrypted"));
            assert!(!bytes.contains("must-not-remain"));
            assert!(bytes.contains("visible"));
        }
    }

    #[test]
    fn env_exact_transaction_enforces_cas_and_recovers_once_after_manifest() {
        let _key = crate::encryption::set_test_encryption_key([0x92; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), b"{}").unwrap();
        let mut candidate = crate::Config::default();
        candidate.env_vars.push(crate::EnvVarEntry {
            name: "TOKEN".to_string(),
            value: "winner".to_string(),
            secret: true,
            value_encrypted: None,
            credential_ref: None,
            configured: true,
            description: None,
        });
        let intents = BTreeSet::from(["TOKEN".to_string()]);
        let error = persist_provider_credential_transaction_with_instances_inner(
            dir.path(),
            &mut candidate,
            &BTreeSet::new(),
            &BTreeSet::new(),
            None,
            &intents,
            Some(0),
            false,
            &BTreeSet::new(),
            false,
            None,
            Some(MigrationFault::AfterManifest),
        )
        .unwrap_err();
        assert!(matches!(error, ConfigStoreError::Io(_)));
        assert!(dir.path().join(MANIFEST_FILE).exists());

        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        ensure_provider_mcp_migration_ready(dir.path()).unwrap();
        let reference = credential_ref("env", "TOKEN", "value").unwrap();
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "winner"
        );
        let revision = CredentialStore::open(dir.path()).revision().unwrap();
        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert_eq!(
            CredentialStore::open(dir.path()).revision().unwrap(),
            revision
        );

        let stale = persist_env_var_credential_transaction_at_revision(
            dir.path(),
            &mut candidate,
            &intents,
            0,
        )
        .unwrap_err();
        assert!(matches!(stale, ConfigStoreError::Conflict { actual, .. } if actual == revision));
    }

    #[test]
    fn env_domain_revision_advances_for_public_metadata_order_and_delete_but_not_noop() {
        let _key = crate::encryption::set_test_encryption_key([0x9a; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), b"{}").unwrap();
        let mut candidate = crate::Config::default();
        candidate.env_vars.push(crate::EnvVarEntry {
            name: "FIRST".to_string(),
            value: "one".to_string(),
            secret: false,
            value_encrypted: None,
            credential_ref: None,
            configured: true,
            description: None,
        });
        let first_intent = BTreeSet::from(["FIRST".to_string()]);
        assert_eq!(
            persist_env_var_credential_transaction_at_revision(
                dir.path(),
                &mut candidate,
                &first_intent,
                0,
            )
            .unwrap(),
            1
        );

        candidate.env_vars[0].description = Some("metadata".to_string());
        assert_eq!(
            persist_env_var_credential_transaction_at_revision(
                dir.path(),
                &mut candidate,
                &first_intent,
                1,
            )
            .unwrap(),
            2
        );
        let stale = persist_env_var_credential_transaction_at_revision(
            dir.path(),
            &mut candidate,
            &first_intent,
            1,
        )
        .unwrap_err();
        assert!(matches!(
            stale,
            ConfigStoreError::Conflict { actual: 2, .. }
        ));
        let bytes_before_noop = std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap();
        assert_eq!(
            persist_env_var_credential_transaction_at_revision(
                dir.path(),
                &mut candidate,
                &first_intent,
                2,
            )
            .unwrap(),
            2
        );
        assert_eq!(
            std::fs::read(dir.path().join(CREDENTIALS_FILE)).unwrap(),
            bytes_before_noop
        );

        candidate.env_vars.push(crate::EnvVarEntry {
            name: "SECOND".to_string(),
            value: "two".to_string(),
            secret: false,
            value_encrypted: None,
            credential_ref: None,
            configured: true,
            description: None,
        });
        let both = BTreeSet::from(["FIRST".to_string(), "SECOND".to_string()]);
        assert_eq!(
            persist_env_var_credential_transaction_at_revision(
                dir.path(),
                &mut candidate,
                &both,
                2,
            )
            .unwrap(),
            3
        );
        candidate.env_vars.swap(0, 1);
        assert_eq!(
            persist_env_var_credential_transaction_at_revision(
                dir.path(),
                &mut candidate,
                &both,
                3,
            )
            .unwrap(),
            4
        );
        candidate.env_vars.retain(|entry| entry.name != "FIRST");
        assert_eq!(
            persist_env_var_credential_transaction_at_revision(
                dir.path(),
                &mut candidate,
                &first_intent,
                4,
            )
            .unwrap(),
            5
        );
        assert_eq!(CredentialStore::open(dir.path()).revision().unwrap(), 5);
    }

    fn crash_env_transaction_after_credentials(
        dir: &Path,
        name: &str,
        value: &str,
    ) -> CredentialRef {
        std::fs::write(dir.join(CONFIG_FILE), b"{}").unwrap();
        let mut candidate = crate::Config::default();
        candidate.env_vars.push(crate::EnvVarEntry {
            name: name.to_string(),
            value: value.to_string(),
            secret: true,
            value_encrypted: None,
            credential_ref: None,
            configured: true,
            description: None,
        });
        let error = persist_provider_credential_transaction_with_instances_inner(
            dir,
            &mut candidate,
            &BTreeSet::new(),
            &BTreeSet::new(),
            None,
            &BTreeSet::from([name.to_string()]),
            Some(0),
            false,
            &BTreeSet::new(),
            false,
            None,
            Some(MigrationFault::AfterCredentials),
        )
        .unwrap_err();
        assert!(matches!(error, ConfigStoreError::Io(_)));
        credential_ref("env", name, "value").unwrap()
    }

    #[test]
    fn committed_env_recovery_merges_unrelated_name_and_same_name_future_metadata() {
        let _key = crate::encryption::set_test_encryption_key([0x97; 32]);
        for same_name_future_metadata in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let reference = crash_env_transaction_after_credentials(dir.path(), "TOKEN", "winner");
            let external = if same_name_future_metadata {
                serde_json::json!({
                    "external_root": true,
                    "env_vars": [{
                        "name": "TOKEN", "secret": true,
                        "credential_ref": reference.as_str(), "configured": true,
                        "future_metadata": {"kept": true}
                    }]
                })
            } else {
                serde_json::json!({
                    "external_root": true,
                    "env_vars": [{"name": "OTHER", "value": "external", "secret": false}]
                })
            };
            std::fs::write(
                dir.path().join(CONFIG_FILE),
                serde_json::to_vec_pretty(&external).unwrap(),
            )
            .unwrap();
            migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
            let root: Value =
                serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap())
                    .unwrap();
            assert_eq!(root["external_root"], true);
            let entries = root["env_vars"].as_array().unwrap();
            assert!(entries.iter().any(|entry| entry["name"] == "TOKEN"));
            if same_name_future_metadata {
                assert_eq!(entries[0]["future_metadata"]["kept"], true);
            } else {
                assert!(entries.iter().any(|entry| entry["name"] == "OTHER"));
            }
            assert_eq!(
                CredentialStore::open(dir.path())
                    .resolve(&reference)
                    .unwrap()
                    .unwrap()
                    .expose(),
                "winner"
            );
            let revision = CredentialStore::open(dir.path()).revision().unwrap();
            migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
            assert_eq!(
                CredentialStore::open(dir.path()).revision().unwrap(),
                revision
            );
        }
    }

    #[test]
    fn same_name_custom_ref_winner_compensates_touched_credential_without_dangling() {
        let _key = crate::encryption::set_test_encryption_key([0x98; 32]);
        let dir = tempfile::tempdir().unwrap();
        let touched = crash_env_transaction_after_credentials(dir.path(), "TOKEN", "staged");
        let custom = credential_ref("env_external", "TOKEN", "value").unwrap();
        CredentialStore::open(dir.path())
            .replace_unchecked(
                custom.clone(),
                "external-winner",
                crate::CredentialSource::User,
                1,
            )
            .unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "env_vars": [{
                    "name": "TOKEN", "secret": true,
                    "credential_ref": custom.as_str(), "configured": true,
                    "description": "external metadata"
                }]
            }))
            .unwrap(),
        )
        .unwrap();
        let error = migrate_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("env metadata changed"));
        assert!(!dir.path().join(MANIFEST_FILE).exists());
        assert!(CredentialStore::open(dir.path())
            .resolve(&touched)
            .unwrap()
            .is_none());
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&custom)
                .unwrap()
                .unwrap()
                .expose(),
            "external-winner"
        );
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(root["env_vars"][0]["credential_ref"], custom.as_str());
        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert_eq!(root["env_vars"][0]["configured"], true);
    }

    #[test]
    fn env_clear_or_delete_recovery_restores_credential_for_same_name_external_winner() {
        let _key = crate::encryption::set_test_encryption_key([0x99; 32]);
        for delete in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let reference = credential_ref("env", "TOKEN", "value").unwrap();
            CredentialStore::open(dir.path())
                .replace(
                    reference.clone(),
                    "original-secret",
                    crate::CredentialSource::User,
                    0,
                )
                .unwrap();
            std::fs::write(
                dir.path().join(CONFIG_FILE),
                serde_json::to_vec_pretty(&serde_json::json!({
                    "env_vars": [{
                        "name": "TOKEN", "secret": true,
                        "credential_ref": reference.as_str(), "configured": true,
                        "description": "original"
                    }]
                }))
                .unwrap(),
            )
            .unwrap();
            let mut candidate =
                crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
            if delete {
                candidate.env_vars.clear();
            } else {
                candidate.env_vars[0].value.clear();
                candidate.env_vars[0].configured = false;
                candidate.env_vars[0].description = Some("transaction-clear".to_string());
            }
            let error = persist_provider_credential_transaction_with_instances_inner(
                dir.path(),
                &mut candidate,
                &BTreeSet::new(),
                &BTreeSet::new(),
                None,
                &BTreeSet::from(["TOKEN".to_string()]),
                Some(1),
                false,
                &BTreeSet::new(),
                false,
                None,
                Some(MigrationFault::AfterCredentials),
            )
            .unwrap_err();
            assert!(matches!(error, ConfigStoreError::Io(_)));
            assert!(!std::fs::read_to_string(dir.path().join(CREDENTIALS_FILE))
                .unwrap()
                .contains(reference.as_str()));

            std::fs::write(
                dir.path().join(CONFIG_FILE),
                serde_json::to_vec_pretty(&serde_json::json!({
                    "env_vars": [{
                        "name": "TOKEN", "secret": true,
                        "credential_ref": reference.as_str(), "configured": true,
                        "description": if delete { "external-readd" } else { "external-clear-winner" }
                    }]
                }))
                .unwrap(),
            )
            .unwrap();

            let recovery = migrate_with_fault(dir.path(), MigrationFault::None).unwrap_err();
            assert!(recovery.to_string().contains("env metadata changed"));
            assert!(!dir.path().join(MANIFEST_FILE).exists());
            assert_eq!(
                CredentialStore::open(dir.path())
                    .resolve(&reference)
                    .unwrap()
                    .unwrap()
                    .expose(),
                "original-secret"
            );
            let root: Value =
                serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap())
                    .unwrap();
            assert_eq!(root["env_vars"][0]["configured"], true);
            assert_eq!(
                root["env_vars"][0]["description"],
                if delete {
                    "external-readd"
                } else {
                    "external-clear-winner"
                }
            );
            migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
            assert!(!dir.path().join(MANIFEST_FILE).exists());
            assert!(CredentialStore::open(dir.path())
                .resolve(&reference)
                .unwrap()
                .is_some());
        }
    }

    #[test]
    fn env_clear_rejects_a_reference_shared_by_another_consumer() {
        let _key = crate::encryption::set_test_encryption_key([0x93; 32]);
        let dir = tempfile::tempdir().unwrap();
        let shared = credential_ref("shared", "owner", "secret").unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "env_vars": [{
                    "name": "TOKEN", "secret": true,
                    "credential_ref": shared.as_str(), "configured": true
                }],
                "provider_instances": {"other": {
                    "provider_type": "openai", "model": "gpt-test",
                    "credential_ref": shared.as_str()
                }}
            }))
            .unwrap(),
        )
        .unwrap();
        CredentialStore::open(dir.path())
            .replace(
                shared.clone(),
                "shared-value",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.env_vars.clear();
        let revision = CredentialStore::open(dir.path()).revision().unwrap();
        let error = persist_env_var_credential_transaction_at_revision(
            dir.path(),
            &mut candidate,
            &BTreeSet::from(["TOKEN".to_string()]),
            revision,
        )
        .unwrap_err();
        assert!(error.to_string().contains("shared by another consumer"));
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&shared)
                .unwrap()
                .unwrap()
                .expose(),
            "shared-value"
        );
    }

    #[test]
    fn ownerless_canonical_env_credential_cannot_be_cleared_by_non_secret_create() {
        let _key = crate::encryption::set_test_encryption_key([0x96; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), b"{}").unwrap();
        let canonical = credential_ref("env", "TOKEN", "value").unwrap();
        CredentialStore::open(dir.path())
            .replace(
                canonical.clone(),
                "generic-owner",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let mut candidate = crate::Config::default();
        candidate.env_vars.push(crate::EnvVarEntry {
            name: "TOKEN".to_string(),
            value: "public".to_string(),
            secret: false,
            value_encrypted: None,
            credential_ref: None,
            configured: true,
            description: None,
        });
        let error = persist_env_var_credential_transaction_at_revision(
            dir.path(),
            &mut candidate,
            &BTreeSet::from(["TOKEN".to_string()]),
            1,
        )
        .unwrap_err();
        assert!(error.to_string().contains("already in use"));
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&canonical)
                .unwrap()
                .unwrap()
                .expose(),
            "generic-owner"
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap())
                .unwrap(),
            serde_json::json!({})
        );
    }

    #[test]
    fn legacy_notification_secrets_migrate_to_refs_and_scrub_root_backups() {
        let _key = crate::encryption::set_test_encryption_key([0xa1; 32]);
        let dir = tempfile::tempdir().unwrap();
        let bark_cipher = crate::encryption::encrypt("bark-legacy-secret").unwrap();
        let root = serde_json::json!({
            "notifications": {
                "desktop": {"enabled": true},
                "ntfy": {
                    "enabled": true,
                    "topic": "alerts",
                    "token": "ntfy-legacy-secret",
                    "unknown_channel_field": {"kept": true}
                },
                "bark": {
                    "enabled": true,
                    "device_key_encrypted": bark_cipher
                }
            },
            "unknown_root": {"kept": true}
        });
        let bytes = serde_json::to_vec_pretty(&root).unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), &bytes).unwrap();
        std::fs::write(dir.path().join("config.json.bak"), &bytes).unwrap();

        migrate_provider_mcp_credentials(dir.path()).unwrap();
        let migrated = std::fs::read_to_string(dir.path().join(CONFIG_FILE)).unwrap();
        assert!(!migrated.contains("ntfy-legacy-secret"));
        assert!(!migrated.contains("bark-legacy-secret"));
        assert!(!migrated.contains("token_encrypted"));
        assert!(!migrated.contains("device_key_encrypted"));
        let root: Value = serde_json::from_str(&migrated).unwrap();
        assert_eq!(
            root["notifications"]["ntfy"]["credential_ref"],
            "notification.ntfy.token"
        );
        assert_eq!(
            root["notifications"]["bark"]["credential_ref"],
            "notification.bark.device_key"
        );
        assert_eq!(
            root["notifications"]["ntfy"]["unknown_channel_field"]["kept"],
            true
        );
        assert_eq!(root["unknown_root"]["kept"], true);
        let backup = std::fs::read_to_string(dir.path().join("config.json.bak")).unwrap();
        assert!(!backup.contains("ntfy-legacy-secret"));
        assert!(!backup.contains("token_encrypted"));

        let loaded = crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        assert_eq!(
            loaded.notifications.ntfy.token.as_deref(),
            Some("ntfy-legacy-secret")
        );
        assert_eq!(
            loaded.notifications.bark.device_key.as_deref(),
            Some("bark-legacy-secret")
        );
        migrate_provider_mcp_credentials(dir.path()).unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.path().join(CONFIG_FILE)).unwrap(),
            migrated
        );
    }

    #[test]
    fn backup_only_notification_secret_is_migrated_and_scrubbed() {
        let _key = crate::encryption::set_test_encryption_key([0xa4; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), br#"{"unknown_root":true}"#).unwrap();
        std::fs::write(
            dir.path().join("config.json.bak"),
            br#"{"notifications":{"ntfy":{"token":"backup-only-secret"}}}"#,
        )
        .unwrap();

        migrate_provider_mcp_credentials(dir.path()).unwrap();

        let backup = std::fs::read_to_string(dir.path().join("config.json.bak")).unwrap();
        assert!(!backup.contains("backup-only-secret"));
        assert!(!backup.contains("\"token\""));
        let backup: Value = serde_json::from_str(&backup).unwrap();
        assert_eq!(
            backup["notifications"]["ntfy"]["credential_ref"],
            "notification.ntfy.token"
        );
        assert_eq!(backup["notifications"]["ntfy"]["configured"], true);
        let reference = credential_ref("notification", "ntfy", "token").unwrap();
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "backup-only-secret"
        );
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(root["unknown_root"], true);
        assert!(root.get("notifications").is_none());
    }

    #[test]
    fn notification_exact_transaction_supports_cas_metadata_and_clear() {
        let _key = crate::encryption::set_test_encryption_key([0xa2; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            br#"{
                "unknown_root":{"kept":true},
                "notifications":{
                    "future_channel":{"kept":true},
                    "ntfy":{"future_metadata":{"kept":true}}
                }
            }"#,
        )
        .unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.notifications.ntfy.enabled = true;
        candidate.notifications.ntfy.topic = "alerts".to_string();
        candidate.notifications.ntfy.token = Some("new-ntfy-secret".to_string());
        candidate.notifications.ntfy.configured = true;
        let revision = persist_notification_credential_transaction_at_revision(
            dir.path(),
            &mut candidate,
            &BTreeSet::from(["ntfy".to_string()]),
            0,
        )
        .unwrap();
        assert_eq!(revision, 1);

        let mut metadata =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        metadata.notifications.ntfy.topic = "renamed".to_string();
        let revision = persist_notification_credential_transaction_at_revision(
            dir.path(),
            &mut metadata,
            &BTreeSet::new(),
            1,
        )
        .unwrap();
        assert_eq!(revision, 2);
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&credential_ref("notification", "ntfy", "token").unwrap())
                .unwrap()
                .unwrap()
                .expose(),
            "new-ntfy-secret"
        );

        let mut clear =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        clear.notifications.ntfy.token = None;
        clear.notifications.ntfy.configured = false;
        let revision = persist_notification_credential_transaction_at_revision(
            dir.path(),
            &mut clear,
            &BTreeSet::from(["ntfy".to_string()]),
            2,
        )
        .unwrap();
        assert_eq!(revision, 3);
        assert!(CredentialStore::open(dir.path())
            .resolve(&credential_ref("notification", "ntfy", "token").unwrap())
            .unwrap()
            .is_none());
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(root["unknown_root"]["kept"], true);
        assert_eq!(root["notifications"]["future_channel"]["kept"], true);
        assert_eq!(
            root["notifications"]["ntfy"]["future_metadata"]["kept"],
            true
        );
        assert_eq!(root["notifications"]["ntfy"]["configured"], false);
        assert!(root["notifications"]["ntfy"].get("token").is_none());
        assert!(root["notifications"]["ntfy"]
            .get("token_encrypted")
            .is_none());

        let mut stale = clear;
        stale.notifications.ntfy.token = Some("stale-secret".to_string());
        stale.notifications.ntfy.configured = true;
        let error = persist_notification_credential_transaction_at_revision(
            dir.path(),
            &mut stale,
            &BTreeSet::from(["ntfy".to_string()]),
            2,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ConfigStoreError::Conflict { actual: 3, .. }
        ));
    }

    #[test]
    fn metadata_only_notification_update_does_not_claim_unbound_canonical_ref() {
        let _key = crate::encryption::set_test_encryption_key([0xa7; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), b"{}").unwrap();
        let canonical = credential_ref("notification", "ntfy", "token").unwrap();
        CredentialStore::open(dir.path())
            .replace(
                canonical.clone(),
                "unbound-foreign-secret",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.notifications.ntfy.topic = "metadata-only".to_string();

        let revision = persist_notification_credential_transaction_at_revision(
            dir.path(),
            &mut candidate,
            &BTreeSet::new(),
            1,
        )
        .unwrap();

        assert_eq!(revision, 2);
        assert!(candidate.notifications.ntfy.credential_ref.is_none());
        assert!(!candidate.notifications.ntfy.configured);
        let loaded = crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        assert_eq!(loaded.notifications.ntfy.topic, "metadata-only");
        assert!(loaded.notifications.ntfy.credential_ref.is_none());
        assert!(loaded.notifications.ntfy.token.is_none());
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&canonical)
                .unwrap()
                .unwrap()
                .expose(),
            "unbound-foreign-secret"
        );
    }

    #[test]
    fn fresh_metadata_only_notification_update_keeps_unconfigured_refs_absent() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), b"{}").unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.notifications.ntfy.topic = "metadata-only".to_string();

        let revision = persist_notification_credential_transaction_at_revision(
            dir.path(),
            &mut candidate,
            &BTreeSet::new(),
            0,
        )
        .unwrap();

        assert_eq!(revision, 1);
        assert!(candidate.notifications.ntfy.credential_ref.is_none());
        assert!(candidate.notifications.bark.credential_ref.is_none());
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert!(root["notifications"]["ntfy"]
            .get("credential_ref")
            .is_none());
        assert!(root["notifications"]["bark"]
            .get("credential_ref")
            .is_none());
    }

    #[test]
    fn explicit_notification_domain_reset_drops_unknown_fields_and_refs() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            br#"{
                "notifications": {
                    "future_channel": {"kept_on_patch": true},
                    "ntfy": {"future_metadata": true}
                }
            }"#,
        )
        .unwrap();
        let mut candidate = crate::Config::default();
        let revision = persist_notification_credential_transaction_at_revision_with_reset(
            dir.path(),
            &mut candidate,
            &BTreeSet::from(["ntfy".to_string(), "bark".to_string()]),
            true,
            0,
        )
        .unwrap();

        assert_eq!(revision, 1);
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert!(root["notifications"].get("future_channel").is_none());
        assert!(root["notifications"]["ntfy"]
            .get("future_metadata")
            .is_none());
        assert!(root["notifications"]["ntfy"]
            .get("credential_ref")
            .is_none());
        assert!(root["notifications"]["bark"]
            .get("credential_ref")
            .is_none());
        assert_eq!(
            candidate.notifications,
            crate::NotificationsConfig::default()
        );
    }

    #[test]
    fn metadata_only_notification_update_rejects_an_existing_shared_ref() {
        let _key = crate::encryption::set_test_encryption_key([0xa8; 32]);
        let dir = tempfile::tempdir().unwrap();
        let shared = CredentialRef::parse("shared.notification.ref").unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "proxy_auth_credential_ref": shared,
                "notifications": {
                    "ntfy": {"credential_ref": shared, "configured": true}
                }
            }))
            .unwrap(),
        )
        .unwrap();
        CredentialStore::open(dir.path())
            .replace(
                shared.clone(),
                "shared-secret",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let mut candidate: crate::Config =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();

        let error = persist_notification_credential_transaction_at_revision(
            dir.path(),
            &mut candidate,
            &BTreeSet::new(),
            1,
        )
        .unwrap_err();

        assert!(error.to_string().contains("shared by another consumer"));
    }

    #[test]
    fn notification_consumer_scan_ignores_plain_strings_but_finds_credential_fields() {
        let reference = "notification.ntfy.token";
        let harmless = serde_json::to_vec(&serde_json::json!({
            "description": reference,
            "nested": {"model": reference},
            "notifications": {"ntfy": {"credential_ref": reference}}
        }))
        .unwrap();
        assert!(
            !notification_document_has_other_consumer(&harmless, reference, "ntfy", true).unwrap()
        );

        let real_consumer = serde_json::to_vec(&serde_json::json!({
            "description": reference,
            "proxy_auth_credential_ref": reference,
            "notifications": {"ntfy": {"credential_ref": reference}}
        }))
        .unwrap();
        assert!(
            notification_document_has_other_consumer(&real_consumer, reference, "ntfy", true)
                .unwrap()
        );
    }

    #[test]
    fn metadata_only_notification_rebase_preserves_unrelated_write_and_bumps_revision() {
        let _key = crate::encryption::set_test_encryption_key([0xa9; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), b"{}").unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.notifications.ntfy.topic = "metadata-after-race".to_string();

        let revision = persist_provider_credential_transaction_with_instances_inner(
            dir.path(),
            &mut candidate,
            &BTreeSet::new(),
            &BTreeSet::new(),
            None,
            &BTreeSet::new(),
            None,
            true,
            &BTreeSet::new(),
            false,
            Some(0),
            Some(MigrationFault::AfterExactCommitUnrelatedCredentialRace),
        )
        .unwrap();

        assert_eq!(revision, 2);
        assert_eq!(CredentialStore::open(dir.path()).revision().unwrap(), 2);
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&credential_ref("provider", "anthropic", "api_key").unwrap())
                .unwrap()
                .unwrap()
                .expose(),
            "concurrent-unrelated-winner"
        );
        let loaded = crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        assert_eq!(loaded.notifications.ntfy.topic, "metadata-after-race");
        assert!(loaded.notifications.ntfy.credential_ref.is_none());
    }

    #[test]
    fn committed_notification_transaction_rebases_unrelated_root_edit() {
        let _key = crate::encryption::set_test_encryption_key([0xa3; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), br#"{"server":{"port":9562}}"#).unwrap();
        let mut candidate =
            crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        candidate.notifications.bark.enabled = true;
        candidate.notifications.bark.device_key = Some("bark-transaction-secret".to_string());
        candidate.notifications.bark.configured = true;
        let error = persist_provider_credential_transaction_with_instances_inner(
            dir.path(),
            &mut candidate,
            &BTreeSet::new(),
            &BTreeSet::new(),
            None,
            &BTreeSet::new(),
            None,
            true,
            &BTreeSet::from(["bark".to_string()]),
            false,
            Some(0),
            Some(MigrationFault::AfterManifest),
        )
        .unwrap_err();
        assert!(matches!(error, ConfigStoreError::Io(_)));
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            br#"{"server":{"port":9999},"external":{"kept":true}}"#,
        )
        .unwrap();

        migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(root["server"]["port"], 9999);
        assert_eq!(root["external"]["kept"], true);
        assert_eq!(
            root["notifications"]["bark"]["credential_ref"],
            "notification.bark.device_key"
        );
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&credential_ref("notification", "bark", "device_key").unwrap())
                .unwrap()
                .unwrap()
                .expose(),
            "bark-transaction-secret"
        );
    }

    #[test]
    fn broker_plaintext_migrates_to_ref_only_and_hydrates_idempotently() {
        let _key = crate::encryption::set_test_encryption_key([0xb1; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(BROKER_FILE),
            br#"{
                "endpoint": "wss://broker.example/ws",
                "token": "broker-secret",
                "future_metadata": {"kept": true}
            }"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join(format!("{BROKER_FILE}.bak")),
            br#"{
                "endpoint": "wss://old-broker.example/ws",
                "token": "stale-backup-secret",
                "backup_metadata": true
            }"#,
        )
        .unwrap();

        let outcome = migrate_broker_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert_eq!(outcome.migrated_credentials, 1);
        let durable = std::fs::read_to_string(dir.path().join(BROKER_FILE)).unwrap();
        assert!(!durable.contains("broker-secret"));
        assert!(!durable.contains("\"token\""));
        assert!(!durable.contains("token_encrypted"));
        let root: Value = serde_json::from_str(&durable).unwrap();
        assert_eq!(root["credential_ref"], "broker.external.bearer_token");
        assert_eq!(root["configured"], true);
        assert_eq!(root["future_metadata"]["kept"], true);
        let backup =
            std::fs::read_to_string(dir.path().join(format!("{BROKER_FILE}.bak"))).unwrap();
        assert!(!backup.contains("stale-backup-secret"));
        assert!(!backup.contains("\"token\""));
        let backup: Value = serde_json::from_str(&backup).unwrap();
        assert_eq!(backup["credential_ref"], "broker.external.bearer_token");
        assert_eq!(backup["backup_metadata"], true);

        let reference = credential_ref("broker", "external", "bearer_token").unwrap();
        let store = CredentialStore::open(dir.path());
        assert_eq!(
            store.resolve(&reference).unwrap().unwrap().expose(),
            "broker-secret"
        );
        let status = store.status(&reference).unwrap();
        assert!(status.configured);
        assert_eq!(status.source, crate::CredentialSource::Migrated);
        let mut runtime: crate::BrokerClientConfig = serde_json::from_str(&durable).unwrap();
        runtime.hydrate_credential_from_store(dir.path()).unwrap();
        assert_eq!(runtime.token, "broker-secret");
        assert!(runtime.configured);
        assert!(!serde_json::to_string(&runtime)
            .unwrap()
            .contains("broker-secret"));
        assert!(!format!("{runtime:?}").contains("broker-secret"));

        let second = migrate_broker_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert_eq!(second.migrated_credentials, 0);
        assert_eq!(
            std::fs::read_to_string(dir.path().join(BROKER_FILE)).unwrap(),
            durable
        );
    }

    #[test]
    fn broker_conflicting_legacy_fields_fail_without_partial_commit() {
        let _key = crate::encryption::set_test_encryption_key([0xb2; 32]);
        let dir = tempfile::tempdir().unwrap();
        let ciphertext = crate::encryption::encrypt("cipher-winner").unwrap();
        let original = serde_json::to_vec_pretty(&serde_json::json!({
            "endpoint": "wss://broker.example/ws",
            "token": "plain-winner",
            "token_encrypted": ciphertext,
        }))
        .unwrap();
        std::fs::write(dir.path().join(BROKER_FILE), &original).unwrap();

        let error = migrate_broker_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("conflicting values"));
        assert_eq!(
            std::fs::read(dir.path().join(BROKER_FILE)).unwrap(),
            original
        );
        assert!(!dir.path().join(CREDENTIALS_FILE).exists());
        assert!(!dir.path().join(MANIFEST_FILE).exists());
    }

    #[test]
    fn broker_ciphertext_only_migrates_without_persisting_ciphertext() {
        let _key = crate::encryption::set_test_encryption_key([0xb5; 32]);
        let dir = tempfile::tempdir().unwrap();
        let ciphertext = crate::encryption::encrypt("encrypted-broker-secret").unwrap();
        std::fs::write(
            dir.path().join(BROKER_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "endpoint": "wss://broker.example/ws",
                "token_encrypted": ciphertext,
            }))
            .unwrap(),
        )
        .unwrap();

        migrate_broker_with_fault(dir.path(), MigrationFault::None).unwrap();
        let durable = std::fs::read_to_string(dir.path().join(BROKER_FILE)).unwrap();
        assert!(!durable.contains("token_encrypted"));
        assert!(!durable.contains(&ciphertext));
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&credential_ref("broker", "external", "bearer_token").unwrap())
                .unwrap()
                .unwrap()
                .expose(),
            "encrypted-broker-secret"
        );
    }

    #[test]
    fn backup_only_broker_secret_is_committed_before_backup_is_scrubbed() {
        let _key = crate::encryption::set_test_encryption_key([0xb6; 32]);
        let dir = tempfile::tempdir().unwrap();
        let backup_path = dir.path().join(format!("{BROKER_FILE}.bak"));
        std::fs::write(
            &backup_path,
            br#"{
                "endpoint": "wss://recover.example/ws",
                "token": "backup-only-broker-secret",
                "recovery_metadata": true
            }"#,
        )
        .unwrap();

        migrate_broker_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert!(!dir.path().join(BROKER_FILE).exists());
        let backup = std::fs::read_to_string(&backup_path).unwrap();
        assert!(!backup.contains("backup-only-broker-secret"));
        assert!(!backup.contains("\"token\""));
        let backup: Value = serde_json::from_str(&backup).unwrap();
        assert_eq!(backup["credential_ref"], "broker.external.bearer_token");
        assert_eq!(backup["configured"], true);
        assert_eq!(backup["recovery_metadata"], true);
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&credential_ref("broker", "external", "bearer_token").unwrap())
                .unwrap()
                .unwrap()
                .expose(),
            "backup-only-broker-secret"
        );
    }

    #[test]
    fn committed_broker_migration_rebases_external_metadata_and_recovers() {
        let _key = crate::encryption::set_test_encryption_key([0xb3; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(BROKER_FILE),
            br#"{"endpoint":"wss://old.example/ws","token":"broker-secret"}"#,
        )
        .unwrap();
        let error = migrate_broker_with_fault(dir.path(), MigrationFault::AfterBroker).unwrap_err();
        assert!(matches!(error, ConfigStoreError::Io(_)));
        std::fs::write(
            dir.path().join(BROKER_FILE),
            br#"{
                "endpoint":"wss://new.example/ws",
                "token":"broker-secret",
                "external_generation": 2
            }"#,
        )
        .unwrap();

        let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
        assert!(outcome.resumed);
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(BROKER_FILE)).unwrap()).unwrap();
        assert_eq!(root["endpoint"], "wss://new.example/ws");
        assert_eq!(root["external_generation"], 2);
        assert_eq!(root["credential_ref"], "broker.external.bearer_token");
        assert!(root.get("token").is_none());
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&credential_ref("broker", "external", "bearer_token").unwrap())
                .unwrap()
                .unwrap()
                .expose(),
            "broker-secret"
        );
        ensure_provider_mcp_migration_ready(dir.path()).unwrap();
    }

    #[test]
    fn broker_migration_rejects_cross_domain_shared_reference() {
        let _key = crate::encryption::set_test_encryption_key([0xb4; 32]);
        let dir = tempfile::tempdir().unwrap();
        let shared = "shared.cross_domain.secret";
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "env_vars": [{
                    "name": "TOKEN",
                    "secret": true,
                    "credential_ref": shared,
                    "configured": true
                }]
            }))
            .unwrap(),
        )
        .unwrap();
        let original = serde_json::to_vec_pretty(&serde_json::json!({
            "endpoint": "wss://broker.example/ws",
            "token": "broker-secret",
            "credential_ref": shared,
        }))
        .unwrap();
        std::fs::write(dir.path().join(BROKER_FILE), &original).unwrap();

        let error = migrate_broker_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("shared by another consumer"));
        assert_eq!(
            std::fs::read(dir.path().join(BROKER_FILE)).unwrap(),
            original
        );
        assert!(!dir.path().join(CREDENTIALS_FILE).exists());

        std::fs::write(
            dir.path().join(BROKER_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "endpoint": "wss://broker.example/ws",
                "credential_ref": shared,
                "configured": true,
            }))
            .unwrap(),
        )
        .unwrap();
        let metadata_only =
            migrate_broker_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(metadata_only
            .to_string()
            .contains("shared by another consumer"));
    }

    #[test]
    fn malformed_broker_does_not_block_provider_migration_or_loading() {
        let _key = crate::encryption::set_test_encryption_key([0xb7; 32]);
        for invalid_broker in [
            b"{ definitely-not-valid-json".as_slice(),
            br#"{"endpoint":42,"credential_ref":false}"#.as_slice(),
        ] {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(dir.path().join(BROKER_FILE), invalid_broker).unwrap();
            std::fs::write(
                dir.path().join(PROVIDERS_FILE),
                br#"{
                    "openai": {
                        "api_key": "provider-isolated-secret",
                        "model": "provider-isolated-model"
                    }
                }"#,
            )
            .unwrap();

            let outcome = migrate_with_fault(dir.path(), MigrationFault::None).unwrap();
            assert_eq!(outcome.migrated_credentials, 1);
            assert_eq!(
                std::fs::read(dir.path().join(BROKER_FILE)).unwrap(),
                invalid_broker
            );
            let loaded =
                crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
            let openai = loaded.providers.openai.as_ref().expect("provider loaded");
            assert_eq!(openai.api_key, "provider-isolated-secret");
            assert_eq!(openai.model.as_deref(), Some("provider-isolated-model"));
        }
    }

    #[test]
    fn broker_primary_and_backup_refs_must_match_before_commit() {
        let _key = crate::encryption::set_test_encryption_key([0xb8; 32]);
        let dir = tempfile::tempdir().unwrap();
        let primary = serde_json::to_vec_pretty(&serde_json::json!({
            "endpoint": "wss://broker.example/ws",
            "token": "primary-secret",
            "credential_ref": "broker.external.primary_token",
        }))
        .unwrap();
        let backup = serde_json::to_vec_pretty(&serde_json::json!({
            "endpoint": "wss://old-broker.example/ws",
            "token": "backup-secret",
            "credential_ref": "broker.external.backup_token",
        }))
        .unwrap();
        let backup_path = dir.path().join(format!("{BROKER_FILE}.bak"));
        std::fs::write(dir.path().join(BROKER_FILE), &primary).unwrap();
        std::fs::write(&backup_path, &backup).unwrap();

        let error = migrate_broker_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("conflicts with primary"));
        assert_eq!(
            std::fs::read(dir.path().join(BROKER_FILE)).unwrap(),
            primary
        );
        assert_eq!(std::fs::read(&backup_path).unwrap(), backup);
        assert!(!dir.path().join(CREDENTIALS_FILE).exists());
        assert!(!dir.path().join(MANIFEST_FILE).exists());
    }

    #[test]
    fn backup_only_broker_ref_rejects_config_backup_consumer_before_commit() {
        let _key = crate::encryption::set_test_encryption_key([0xb9; 32]);
        let dir = tempfile::tempdir().unwrap();
        let shared = "shared.backup.broker_token";
        let broker_backup = serde_json::to_vec_pretty(&serde_json::json!({
            "endpoint": "wss://recover.example/ws",
            "token": "backup-only-secret",
            "credential_ref": shared,
        }))
        .unwrap();
        let config_backup = serde_json::to_vec_pretty(&serde_json::json!({
            "provider_instances": {
                "work": {"credential_ref": shared, "configured": true}
            },
            "env_vars": [{
                "name": "SHARED_TOKEN",
                "secret": true,
                "credential_ref": shared,
                "configured": true
            }]
        }))
        .unwrap();
        let broker_backup_path = dir.path().join(format!("{BROKER_FILE}.bak"));
        let config_backup_path = dir.path().join(format!("{CONFIG_FILE}.bak"));
        std::fs::write(&broker_backup_path, &broker_backup).unwrap();
        std::fs::write(&config_backup_path, &config_backup).unwrap();

        let error = migrate_broker_with_fault(dir.path(), MigrationFault::None).unwrap_err();
        assert!(error.to_string().contains("shared by another consumer"));
        assert_eq!(std::fs::read(&broker_backup_path).unwrap(), broker_backup);
        assert_eq!(std::fs::read(&config_backup_path).unwrap(), config_backup);
        assert!(!dir.path().join(CREDENTIALS_FILE).exists());
        assert!(!dir.path().join(MANIFEST_FILE).exists());
    }

    #[test]
    fn cluster_password_key_and_passphrase_migrate_and_hydrate() {
        let _key = crate::encryption::set_test_encryption_key([0xc1; 32]);
        let dir = tempfile::tempdir().unwrap();
        let key_cipher = crate::encryption::encrypt("private-key-secret").unwrap();
        let original = serde_json::to_vec_pretty(&serde_json::json!({
            "cluster_fabric": {
                "future_fabric_field": {"kept": true},
                "nodes": [
                    {
                        "id": "password-node",
                        "label": "password-node",
                        "placement": {
                            "type": "ssh",
                            "host": "10.0.0.1",
                            "username": "deploy",
                            "auth": {"method": "password", "password": "password-secret"}
                        },
                        "future_node_field": "kept"
                    },
                    {
                        "id": "key-node",
                        "label": "key-node",
                        "placement": {
                            "type": "ssh",
                            "host": "10.0.0.2",
                            "username": "deploy",
                            "auth": {
                                "method": "private_key",
                                "private_key_encrypted": key_cipher,
                                "passphrase": "passphrase-secret"
                            }
                        }
                    }
                ]
            },
            "future_root_field": {"kept": true}
        }))
        .unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), &original).unwrap();

        let outcome = migrate_cluster_credentials(dir.path()).unwrap();
        assert_eq!(outcome.migrated_credentials, 3);
        let migrated = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        let migrated_text = String::from_utf8(migrated.clone()).unwrap();
        for forbidden in [
            "password-secret",
            "private-key-secret",
            "passphrase-secret",
            "password_encrypted",
            "private_key_encrypted",
            "passphrase_encrypted",
        ] {
            assert!(!migrated_text.contains(forbidden), "leaked {forbidden}");
        }
        let root: Value = serde_json::from_slice(&migrated).unwrap();
        assert_eq!(root["future_root_field"]["kept"], true);
        assert_eq!(root["cluster_fabric"]["future_fabric_field"]["kept"], true);
        assert_eq!(
            root["cluster_fabric"]["nodes"][0]["future_node_field"],
            "kept"
        );
        assert_eq!(
            root["cluster_fabric"]["credential_refs"]["password-node"]["password_credential_ref"],
            "cluster.password-node.password"
        );
        assert_eq!(
            root["cluster_fabric"]["credential_refs"]["key-node"]["private_key_credential_ref"],
            "cluster.key-node.private_key"
        );
        assert_eq!(
            root["cluster_fabric"]["credential_refs"]["key-node"]["passphrase_credential_ref"],
            "cluster.key-node.passphrase"
        );

        let loaded = crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        let crate::NodePlacement::Ssh(password_target) = &loaded.cluster_fabric.nodes[0].placement
        else {
            panic!("expected SSH node")
        };
        let crate::SshAuth::Password { password, .. } = &password_target.auth else {
            panic!("expected password auth")
        };
        assert_eq!(password, "password-secret");
        let crate::NodePlacement::Ssh(key_target) = &loaded.cluster_fabric.nodes[1].placement
        else {
            panic!("expected SSH node")
        };
        let crate::SshAuth::PrivateKey {
            private_key,
            passphrase,
            ..
        } = &key_target.auth
        else {
            panic!("expected private-key auth")
        };
        assert_eq!(private_key, "private-key-secret");
        assert_eq!(passphrase, "passphrase-secret");

        loaded.save_to_dir(dir.path().to_path_buf()).unwrap();
        for suffix in ["", ".bak", ".bak.1", ".bak.2"] {
            let path = dir.path().join(format!("{CONFIG_FILE}{suffix}"));
            let Ok(durable) = std::fs::read_to_string(path) else {
                continue;
            };
            for forbidden in [
                "password-secret",
                "private-key-secret",
                "passphrase-secret",
                "password_encrypted",
                "private_key_encrypted",
                "passphrase_encrypted",
            ] {
                assert!(
                    !durable.contains(forbidden),
                    "saved {suffix} leaked {forbidden}"
                );
            }
        }
        let reloaded = crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        let crate::NodePlacement::Ssh(reloaded_target) =
            &reloaded.cluster_fabric.nodes[0].placement
        else {
            panic!("expected SSH node")
        };
        let crate::SshAuth::Password {
            password: reloaded_password,
            ..
        } = &reloaded_target.auth
        else {
            panic!("expected password auth")
        };
        assert_eq!(reloaded_password, "password-secret");

        let saved = std::fs::read(dir.path().join(CONFIG_FILE)).unwrap();
        migrate_cluster_credentials(dir.path()).unwrap();
        assert_eq!(std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(), saved);
    }

    #[test]
    fn backup_only_cluster_secret_commits_before_backup_scrub() {
        let _key = crate::encryption::set_test_encryption_key([0xc2; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), br#"{"unknown_root":true}"#).unwrap();
        let backup_path = dir.path().join(format!("{CONFIG_FILE}.bak"));
        std::fs::write(
            &backup_path,
            br#"{
                "cluster_fabric": {
                    "unknown_cluster": true,
                    "nodes": [{
                        "id": "backup-node",
                        "label": "backup-node",
                        "placement": {
                            "type": "ssh",
                            "host": "10.0.0.3",
                            "username": "deploy",
                            "auth": {"method": "password", "password": "backup-only-secret"}
                        }
                    }]
                }
            }"#,
        )
        .unwrap();

        let outcome = migrate_cluster_credentials(dir.path()).unwrap();
        assert_eq!(outcome.migrated_credentials, 1);
        let backup = std::fs::read_to_string(&backup_path).unwrap();
        assert!(!backup.contains("backup-only-secret"));
        assert!(backup.contains("cluster.backup-node.password"));
        assert!(backup.contains("unknown_cluster"));
        let reference = crate::cluster_password_credential_ref("backup-node").unwrap();
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "backup-only-secret"
        );
        let root: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(root["unknown_root"], true);
        assert!(root.get("cluster_fabric").is_none());
    }

    #[test]
    fn conflicting_legacy_cluster_fields_fail_without_durable_changes() {
        let _key = crate::encryption::set_test_encryption_key([0xc3; 32]);
        let dir = tempfile::tempdir().unwrap();
        let ciphertext = crate::encryption::encrypt("ciphertext-winner").unwrap();
        let original = serde_json::to_vec_pretty(&serde_json::json!({
            "cluster_fabric": {"nodes": [{
                "id": "node",
                "label": "node",
                "placement": {
                    "type": "ssh",
                    "host": "10.0.0.4",
                    "username": "deploy",
                    "auth": {
                        "method": "password",
                        "password": "plaintext-loser",
                        "password_encrypted": ciphertext
                    }
                }
            }]}
        }))
        .unwrap();
        std::fs::write(dir.path().join(CONFIG_FILE), &original).unwrap();

        let error = migrate_cluster_credentials(dir.path()).unwrap_err();
        assert!(error.to_string().contains("conflicting values"));
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            original
        );
        assert!(!dir.path().join(CREDENTIALS_FILE).exists());
        assert!(!dir.path().join(MANIFEST_FILE).exists());
        assert!(!dir.path().join(JOURNAL_FILE).exists());
    }

    #[test]
    fn noncanonical_cluster_backup_ref_rejects_primary_before_commit() {
        let _key = crate::encryption::set_test_encryption_key([0xc4; 32]);
        let dir = tempfile::tempdir().unwrap();
        let primary = br#"{
            "cluster_fabric": {"nodes": [{
                "id": "node", "label": "node",
                "placement": {"type":"ssh","host":"h","username":"u",
                    "auth":{"method":"password","password":"primary-secret"}}
            }]}
        }"#;
        let backup = br#"{
            "cluster_fabric": {
                "nodes": [{
                    "id": "node", "label": "node",
                    "placement": {"type":"ssh","host":"h","username":"u",
                        "auth":{"method":"password"}}
                }],
                "credential_refs": {"node": {
                    "password_credential_ref": "cluster.other.password",
                    "password_configured": true
                }}
            }
        }"#;
        let backup_path = dir.path().join(format!("{CONFIG_FILE}.bak"));
        std::fs::write(dir.path().join(CONFIG_FILE), primary).unwrap();
        std::fs::write(&backup_path, backup).unwrap();

        let error = migrate_cluster_credentials(dir.path()).unwrap_err();
        assert!(error.to_string().contains("not canonical"));
        assert_eq!(
            std::fs::read(dir.path().join(CONFIG_FILE)).unwrap(),
            primary
        );
        assert_eq!(std::fs::read(&backup_path).unwrap(), backup);
        assert!(!dir.path().join(CREDENTIALS_FILE).exists());
    }

    #[test]
    fn committed_cluster_migration_accepts_a_newer_secret_free_config() {
        let _key = crate::encryption::set_test_encryption_key([0xc5; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            br#"{
                "cluster_fabric": {"nodes": [{
                    "id": "node", "label": "node",
                    "placement": {"type":"ssh","host":"old","username":"deploy",
                        "auth":{"method":"password","password":"committed-secret"}}
                }]}
            }"#,
        )
        .unwrap();
        assert!(migrate_cluster_with_fault(dir.path(), MigrationFault::AfterManifest).is_err());

        std::fs::write(
            dir.path().join(CONFIG_FILE),
            br#"{
                "cluster_fabric": {
                    "nodes": [{
                        "id": "node", "label": "node",
                        "placement": {"type":"ssh","host":"newer","username":"deploy",
                            "auth":{"method":"password"}}
                    }],
                    "credential_refs": {"node": {
                        "password_credential_ref": "cluster.node.password",
                        "password_configured": true
                    }}
                },
                "concurrent_unknown": true
            }"#,
        )
        .unwrap();

        let outcome = migrate_cluster_credentials(dir.path()).unwrap();
        assert!(outcome.resumed);
        ensure_provider_mcp_migration_ready(dir.path()).unwrap();
        let current: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(CONFIG_FILE)).unwrap()).unwrap();
        assert_eq!(current["concurrent_unknown"], true);
        assert_eq!(
            current["cluster_fabric"]["nodes"][0]["placement"]["host"],
            "newer"
        );
        let loaded = crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        let crate::NodePlacement::Ssh(target) = &loaded.cluster_fabric.nodes[0].placement else {
            panic!("expected SSH node")
        };
        let crate::SshAuth::Password { password, .. } = &target.auth else {
            panic!("expected password auth")
        };
        assert_eq!(password, "committed-secret");
    }

    #[test]
    fn cluster_recovery_failure_does_not_disable_stable_provider_hydration() {
        let _key = crate::encryption::set_test_encryption_key([0xc6; 32]);
        let dir = tempfile::tempdir().unwrap();
        let provider_ref = credential_ref("provider", "openai", "api_key").unwrap();
        CredentialStore::open(dir.path())
            .replace(
                provider_ref.clone(),
                "stable-provider-secret",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        std::fs::write(
            dir.path().join(PROVIDERS_FILE),
            serde_json::to_vec_pretty(&serde_json::json!({
                "openai": {
                    "credential_ref": provider_ref,
                    "model": "stable-model"
                }
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            br#"{
                "cluster_fabric": {"nodes": [{
                    "id": "node", "label": "node",
                    "placement": {"type":"ssh","host":"h","username":"u",
                        "auth":{"method":"password","password":"cluster-secret"}}
                }]}
            }"#,
        )
        .unwrap();
        assert!(migrate_cluster_with_fault(dir.path(), MigrationFault::AfterManifest).is_err());
        std::fs::write(
            dir.path().join(format!("{CONFIG_FILE}.bak")),
            br#"{
                "cluster_fabric": {
                    "nodes": [{
                        "id": "node", "label": "node",
                        "placement": {"type":"ssh","host":"h","username":"u",
                            "auth":{"method":"password"}}
                    }],
                    "credential_refs": {"node": {
                        "password_credential_ref": "cluster.wrong.password",
                        "password_configured": true
                    }}
                }
            }"#,
        )
        .unwrap();

        migrate_provider_mcp_credentials(dir.path()).unwrap();
        ensure_provider_mcp_migration_ready(dir.path()).unwrap();
        assert!(pending_cluster_migration(dir.path()).unwrap());
        let loaded = crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        let openai = loaded.providers.openai.as_ref().expect("provider loaded");
        assert_eq!(openai.api_key, "stable-provider-secret");
        assert_eq!(openai.model.as_deref(), Some("stable-model"));
        assert!(loaded.cluster_fabric.nodes.iter().all(|node| {
            let crate::NodePlacement::Ssh(target) = &node.placement else {
                return true;
            };
            match &target.auth {
                crate::SshAuth::Password { password, .. } => password.is_empty(),
                _ => true,
            }
        }));
    }

    #[test]
    fn cluster_pending_bypass_rejects_unsafe_provider_domain_backups() {
        let _key = crate::encryption::set_test_encryption_key([0xc7; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            br#"{
                "cluster_fabric": {"nodes": [{
                    "id": "node", "label": "node",
                    "placement": {"type":"ssh","host":"h","username":"u",
                        "auth":{"method":"password","password":"cluster-secret"}}
                }]}
            }"#,
        )
        .unwrap();
        assert!(migrate_cluster_with_fault(dir.path(), MigrationFault::AfterManifest).is_err());
        let conflicting = crate::encryption::encrypt("different-secret").unwrap();
        std::fs::write(
            dir.path().join(format!("{CONFIG_FILE}.bak")),
            serde_json::to_vec_pretty(&serde_json::json!({
                "env_vars": [{
                    "name": "UNSAFE_BACKUP",
                    "secret": true,
                    "value": "plaintext-secret",
                    "value_encrypted": conflicting
                }]
            }))
            .unwrap(),
        )
        .unwrap();

        let error = migrate_provider_mcp_credentials(dir.path()).unwrap_err();
        assert!(error.to_string().contains("conflicting values"));
        assert!(ensure_provider_mcp_migration_ready(dir.path()).is_err());
        assert!(pending_cluster_migration(dir.path()).unwrap());
    }

    #[test]
    fn cluster_pending_bypass_rejects_cross_domain_metadata_ref() {
        let _key = crate::encryption::set_test_encryption_key([0xc8; 32]);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            br#"{
                "cluster_fabric": {"nodes": [{
                    "id": "node", "label": "node",
                    "placement": {"type":"ssh","host":"h","username":"u",
                        "auth":{"method":"password","password":"ssh-password"}}
                }]}
            }"#,
        )
        .unwrap();
        assert!(migrate_cluster_with_fault(dir.path(), MigrationFault::AfterManifest).is_err());
        std::fs::write(
            dir.path().join(PROVIDERS_FILE),
            br#"{
                "openai": {
                    "credential_ref": "cluster.node.password",
                    "model": "must-not-hydrate"
                }
            }"#,
        )
        .unwrap();

        let error = migrate_provider_mcp_credentials(dir.path()).unwrap_err();
        assert!(error.to_string().contains("shared by another consumer"));
        assert!(ensure_provider_mcp_migration_ready(dir.path()).is_err());
        assert!(
            !CredentialStore::open(dir.path())
                .status_unchecked(&crate::cluster_password_credential_ref("node").unwrap())
                .unwrap()
                .configured
        );
        let loaded = crate::Config::from_data_dir_without_publish(Some(dir.path().to_path_buf()));
        assert!(loaded
            .providers
            .openai
            .as_ref()
            .is_none_or(|provider| provider.api_key.is_empty()));
    }

    #[test]
    fn cluster_pending_bypass_rejects_provider_ref_owned_only_by_backup_node() {
        let _key = crate::encryption::set_test_encryption_key([0xc9; 32]);
        let dir = tempfile::tempdir().unwrap();
        let backup_ref = crate::cluster_password_credential_ref("backup-node").unwrap();
        CredentialStore::open(dir.path())
            .replace(
                backup_ref.clone(),
                "backup-node-ssh-secret",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        std::fs::write(
            dir.path().join(CONFIG_FILE),
            br#"{
                "cluster_fabric": {"nodes": [{
                    "id": "current-node", "label": "current-node",
                    "placement": {"type":"ssh","host":"h","username":"u",
                        "auth":{"method":"password","password":"current-secret"}}
                }]}
            }"#,
        )
        .unwrap();
        assert!(migrate_cluster_with_fault(dir.path(), MigrationFault::AfterManifest).is_err());
        std::fs::write(
            dir.path().join(format!("{CONFIG_FILE}.bak")),
            br#"{
                "cluster_fabric": {
                    "nodes": [{
                        "id": "backup-node", "label": "backup-node",
                        "placement": {"type":"ssh","host":"old","username":"u",
                            "auth":{"method":"password"}}
                    }],
                    "credential_refs": {"backup-node": {
                        "password_credential_ref": "cluster.backup-node.password",
                        "password_configured": true
                    }}
                }
            }"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join(PROVIDERS_FILE),
            br#"{
                "openai": {
                    "credential_ref": "cluster.backup-node.password",
                    "model": "must-not-hydrate"
                }
            }"#,
        )
        .unwrap();

        let error = migrate_provider_mcp_credentials(dir.path()).unwrap_err();
        assert!(error.to_string().contains("shared by another consumer"));
        assert!(ensure_provider_mcp_migration_ready(dir.path()).is_err());
        assert!(
            CredentialStore::open(dir.path())
                .status_unchecked(&backup_ref)
                .unwrap()
                .configured
        );
    }
}
