//! Authoritative saved-data sharing. Only the three owned section snapshots
//! enter a plan. No effective loader, runtime factory, provider probe, or
//! executable configuration participates. One runtime-only provider-selector
//! presence check closes the automatic-authentication boundary; its value is
//! never exported, retained, or logged.

use std::path::Path;

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::config_share::{
    self, ShareBundle, ShareDecisions, ShareError, ShareModelRef, SharePreview, ShareScenario,
    ShareSelection,
};
use crate::credential_migration::share_transaction::{
    self, ShareOperationStatus, ShareRevisions, ShareTransactionCandidate, ShareTransactionSnapshot,
};
use crate::credential_store::share::ShareCredentialSnapshot;
use crate::{
    Config, ConfigStoreError, DefaultsConfig, EnvSection, EnvVarEntry, ProviderInstanceConfig,
    ProvidersSection,
};

#[derive(Debug, Clone, Copy, thiserror::Error)]
pub enum ShareApiError {
    #[error("Configuration sharing data is invalid")]
    Invalid,
    #[error("Configuration changed; create a new preview")]
    Conflict,
    #[error("Configuration sharing is unavailable until saved credentials are initialized")]
    Unavailable,
    #[error(
        "This import could initialize Copilot; configure an instance-only non-Copilot target first"
    )]
    RuntimeBoundary,
    #[error(transparent)]
    Share(#[from] ShareError),
}

#[derive(Serialize)]
pub struct ConfigSharePreview {
    pub preview_token: String,
    pub expected_revisions: ShareRevisions,
    pub preview: SharePreview,
    pub warnings: Vec<ShareWarning>,
    pub apply_blocked: bool,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ShareWarning {
    AutomaticProviderAuthenticationBlocked,
    CredentialKeyUninitialized,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Stored<T> {
    schema_version: u32,
    revision: u64,
    data: T,
}

fn map_store(error: ConfigStoreError) -> ShareApiError {
    match error {
        ConfigStoreError::Conflict { .. } => ShareApiError::Conflict,
        ConfigStoreError::Validation(message) if message == "share preview changed" => {
            ShareApiError::Conflict
        }
        ConfigStoreError::Validation(message) if message == "share runtime boundary" => {
            ShareApiError::RuntimeBoundary
        }
        ConfigStoreError::Validation(message) if message == "share candidate invalid" => {
            ShareApiError::Invalid
        }
        ConfigStoreError::Validation(message)
            if [
                share_transaction::SHARE_TRANSACTION_INVALID,
                share_transaction::SHARE_OPERATION_ID_REUSED,
                share_transaction::SHARE_OPERATION_CAPACITY_REACHED,
            ]
            .contains(&message.as_str()) =>
        {
            ShareApiError::Invalid
        }
        _ => ShareApiError::Unavailable,
    }
}

fn decode_section<T: DeserializeOwned + Default>(
    bytes: Option<&[u8]>,
) -> Result<Stored<T>, ShareApiError> {
    let value = match bytes {
        Some(bytes) => {
            serde_json::from_slice::<Stored<T>>(bytes).map_err(|_| ShareApiError::Unavailable)?
        }
        None => Stored {
            schema_version: 1,
            revision: 0,
            data: T::default(),
        },
    };
    if value.schema_version != 1 {
        return Err(ShareApiError::Unavailable);
    }
    Ok(value)
}

fn target(snapshot: &ShareTransactionSnapshot) -> Result<Config, ShareApiError> {
    let providers: Stored<ProvidersSection> = decode_section(snapshot.providers.as_deref())?;
    let env: Stored<EnvSection> = decode_section(snapshot.env.as_deref())?;
    let mut target = Config::default(); // pure in-memory constructor (#38)
    if let Some(provider) = providers.data.provider {
        target.provider = provider;
    }
    target.provider_instances = providers.data.provider_instances;
    target.default_provider_instance = providers.data.default_provider_instance;
    target.defaults = providers.data.defaults;
    target.features = providers.data.features;
    *target.providers_mut() = providers.data.providers;
    target.env_vars = env.data.0;
    Ok(target)
}

/// The token binds exact bytes and presence, including same-revision edits.
/// It is a preview binding, not permission to bypass apply-time validation.
fn preview_token(share_string: &str, snapshot: &ShareTransactionSnapshot) -> String {
    let mut hash = Sha256::new();
    hash.update(b"bamboo-share-preview-v1\0");
    hash.update((share_string.len() as u64).to_be_bytes());
    hash.update(share_string.as_bytes());
    for bytes in [&snapshot.providers, &snapshot.env, &snapshot.credentials] {
        hash.update([u8::from(bytes.is_some())]);
        if let Some(bytes) = bytes {
            hash.update((bytes.len() as u64).to_be_bytes());
            hash.update(bytes);
        }
    }
    hex::encode(hash.finalize())
}

pub fn export_config_share(
    data_dir: &Path,
    selection: &ShareSelection,
    include_secrets: bool,
    passphrase: Option<&str>,
) -> Result<String, ShareApiError> {
    let wants_secrets =
        !selection.provider_api_keys.is_empty() || selection.env.values().any(|include| *include);
    if wants_secrets && !include_secrets {
        return Err(ShareApiError::Invalid);
    }
    let snapshot = share_transaction::read_share_snapshot(data_dir).map_err(map_store)?;
    let mut current = target(&snapshot)?;
    let credentials =
        ShareCredentialSnapshot::from_bytes(snapshot.credentials.as_deref()).map_err(map_store)?;
    // Only explicitly selected values are decrypted, and only with the
    // already initialized runtime key. OS-env overrides are never consulted.
    let mut instances: std::collections::HashMap<_, _> =
        crate::synthesize_legacy_instances(&current)
            .into_iter()
            .collect();
    instances.extend(current.provider_instances.clone());
    for id in &selection.provider_api_keys {
        let instance = instances.get_mut(id).ok_or(ShareApiError::Invalid)?;
        let reference = instance
            .credential_ref
            .as_ref()
            .ok_or(ShareApiError::Unavailable)?;
        instance.api_key = credentials.resolve_selected(reference).map_err(map_store)?;
    }
    current.provider_instances = instances;
    for (name, include_value) in &selection.env {
        if !include_value {
            continue;
        }
        let entry = current
            .env_vars
            .iter_mut()
            .find(|entry| &entry.name == name)
            .ok_or(ShareApiError::Invalid)?;
        if entry.secret {
            let reference = entry
                .credential_ref
                .as_ref()
                .ok_or(ShareApiError::Unavailable)?;
            entry.value = credentials.resolve_selected(reference).map_err(map_store)?;
        }
    }
    let bundle = config_share::select_bundle(&current, selection)?;
    Ok(config_share::encode_bundle(&bundle, passphrase)?)
}

pub fn preview_config_share(
    data_dir: &Path,
    share_string: &str,
    passphrase: Option<&str>,
) -> Result<ConfigSharePreview, ShareApiError> {
    let bundle = config_share::decode_bundle(share_string, passphrase)?;
    let snapshot = share_transaction::read_share_snapshot(data_dir).map_err(map_store)?;
    let current = target(&snapshot)?;
    let mut warnings = Vec::new();
    if reject_network_initialization(&current).is_err()
        || bundle
            .providers
            .iter()
            .any(|provider| provider.provider_type.as_str() == "copilot")
    {
        warnings.push(ShareWarning::AutomaticProviderAuthenticationBlocked);
    }
    let credentials =
        ShareCredentialSnapshot::from_bytes(snapshot.credentials.as_deref()).map_err(map_store)?;
    if credentials.require_initialized_if_configured().is_err()
        || (requires_local_key(&bundle, &current)
            && crate::encryption::require_initialized_key().is_err())
    {
        warnings.push(ShareWarning::CredentialKeyUninitialized);
    }
    let apply_blocked = !warnings.is_empty();
    Ok(ConfigSharePreview {
        preview_token: preview_token(share_string, &snapshot),
        expected_revisions: snapshot.revisions,
        preview: config_share::preview_bundle(&bundle, &current)?,
        warnings,
        apply_blocked,
    })
}

/// Watcher registry construction attempts silent Copilot authentication. Keep
/// that runtime boundary closed in this data-only import slice, including
/// unchanged Copilot instances and the legacy registry's implicit Copilot.
fn reject_network_initialization(config: &Config) -> Result<(), ShareApiError> {
    // Runtime-only presence check, never a saved value, export value or log.
    // The existing scoped test override keeps synthetic tests off real env.
    let provider_override = crate::runtime_env_var("BAMBOO_PROVIDER").is_ok();
    if provider_override
        || config.provider_instances.is_empty()
        || config
            .provider_instances
            .values()
            .any(|instance| instance.enabled && instance.provider_type == "copilot")
        || (config.effective_default_provider() == "copilot"
            && !config.provider_instances.contains_key("copilot"))
    {
        return Err(ShareApiError::RuntimeBoundary);
    }
    Ok(())
}

fn operation_fingerprint(
    share_string: &str,
    choices: &ShareDecisions,
    revisions: &ShareRevisions,
    token: &str,
) -> Result<String, ShareApiError> {
    let mut hash = Sha256::new();
    hash.update(b"bamboo-share-apply-v1\0");
    // Deliberately excludes passphrase and decrypted bundle.
    for part in [
        share_string.as_bytes().to_vec(),
        serde_json::to_vec(choices).map_err(|_| ShareApiError::Invalid)?,
        serde_json::to_vec(revisions).map_err(|_| ShareApiError::Invalid)?,
        token.as_bytes().to_vec(),
    ] {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part);
    }
    Ok(hex::encode(hash.finalize()))
}

pub fn apply_config_share(
    data_dir: &Path,
    share_string: &str,
    passphrase: Option<&str>,
    choices: &ShareDecisions,
    expected: ShareRevisions,
    token: &str,
    operation_id: Uuid,
) -> Result<ShareOperationStatus, ShareApiError> {
    if operation_id.is_nil()
        || token.len() != 64
        || !token.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(ShareApiError::Invalid);
    }
    let bundle = config_share::decode_bundle(share_string, passphrase)?;
    let fingerprint = operation_fingerprint(share_string, choices, &expected, token)?;
    let result = share_transaction::commit_share_transaction(
        data_dir,
        operation_id,
        &fingerprint,
        expected,
        |snapshot| {
            if preview_token(share_string, snapshot) != token {
                return Err(ConfigStoreError::Validation("share preview changed".into()));
            }
            prepare_import(&bundle, choices, snapshot).map_err(|error| {
                ConfigStoreError::Validation(
                    match error {
                        ShareApiError::RuntimeBoundary => "share runtime boundary",
                        _ => "share candidate invalid",
                    }
                    .into(),
                )
            })
        },
    );
    match result {
        Ok(receipt) => Ok(ShareOperationStatus::Committed {
            operation_id: receipt.operation_id,
            revisions: receipt.revisions,
        }),
        Err(ConfigStoreError::CommitIndeterminate(_)) => {
            Ok(ShareOperationStatus::Pending { operation_id })
        }
        Err(error) => Err(map_store(error)),
    }
}

fn new_reference(domain: &str) -> Result<crate::CredentialRef, ShareApiError> {
    crate::credential_ref(domain, &Uuid::new_v4().to_string(), "share").map_err(map_store)
}

fn prepare_import(
    bundle: &ShareBundle,
    choices: &ShareDecisions,
    snapshot: &ShareTransactionSnapshot,
) -> Result<ShareTransactionCandidate, ShareApiError> {
    let mut current = target(snapshot)?;
    let plan = config_share::plan_import(bundle, &current, choices)?;
    let mut providers: Stored<ProvidersSection> = decode_section(snapshot.providers.as_deref())?;
    let mut env: Stored<EnvSection> = decode_section(snapshot.env.as_deref())?;
    let mut credentials =
        ShareCredentialSnapshot::from_bytes(snapshot.credentials.as_deref()).map_err(map_store)?;
    credentials
        .require_initialized_if_configured()
        .map_err(map_store)?;
    if requires_local_key(&plan, &current) {
        crate::encryption::require_initialized_key().map_err(|_| ShareApiError::Unavailable)?;
    }
    let existing: std::collections::HashMap<_, _> = crate::synthesize_legacy_instances(&current)
        .into_iter()
        .collect();
    for provider in &plan.providers {
        let old = current
            .provider_instances
            .get(&provider.id)
            .or_else(|| existing.get(&provider.id));
        if old.is_some_and(|old| old.provider_type != provider.provider_type.as_str()) {
            return Err(ShareApiError::Invalid);
        }
        let mut instance = old.cloned().unwrap_or_else(|| ProviderInstanceConfig {
            provider_type: provider.provider_type.as_str().into(),
            label: None,
            api_key: String::new(),
            api_key_encrypted: None,
            credential_ref: None,
            base_url: None,
            model: None,
            fast_model: None,
            vision_model: None,
            reasoning_effort: None,
            responses_only_models: Vec::new(),
            request_overrides: None,
            enabled: true,
            extra: Default::default(),
        });
        // Only present values are edits. In particular, key and base URL
        // omission preserve the selected destination's existing authority.
        if provider.label.is_some() {
            instance.label.clone_from(&provider.label);
        }
        if provider.base_url.is_some() {
            instance.base_url.clone_from(&provider.base_url);
        }
        if provider.model.is_some() {
            instance.model.clone_from(&provider.model);
        }
        if provider.fast_model.is_some() {
            instance.fast_model.clone_from(&provider.fast_model);
        }
        if provider.vision_model.is_some() {
            instance.vision_model.clone_from(&provider.vision_model);
        }
        if provider.reasoning_effort.is_some() {
            instance.reasoning_effort = provider.reasoning_effort;
        }
        if !provider.responses_only_models.is_empty() {
            instance
                .responses_only_models
                .clone_from(&provider.responses_only_models);
        }
        if let Some(models) = &provider.runtime_models {
            instance
                .extra
                .insert("runtime_models".into(), serde_json::json!(models));
        }
        if let Some(value) = &provider.api_key {
            let reference = new_reference("provider")?;
            credentials
                .insert_share_secret(reference.clone(), value)
                .map_err(map_store)?;
            instance.credential_ref = Some(reference);
            instance.api_key_encrypted = None;
            instance.api_key.clear();
        }
        current
            .provider_instances
            .insert(provider.id.clone(), instance);
    }
    merge_scenarios(&mut current, &plan)?;
    for entry in plan.env {
        let identity = config_share::canonical_env_name(&entry.name);
        if let Some(existing) = current
            .env_vars
            .iter_mut()
            .find(|item| config_share::canonical_env_name(&item.name) == identity)
        {
            let Some(value) = entry.value else {
                continue;
            };
            // A share may not silently declassify an already secret value.
            let secret = existing.secret || entry.secret;
            if secret {
                let reference = new_reference("env")?;
                credentials
                    .insert_share_secret(reference.clone(), &value)
                    .map_err(map_store)?;
                existing.value.clear();
                existing.credential_ref = Some(reference);
                existing.configured = true;
            } else {
                existing.value = value;
                existing.configured = !existing.value.is_empty();
            }
            existing.secret = secret;
            existing.value_encrypted = None;
        } else {
            // Names-only imports create no value, no credential and no Clear.
            let mut destination = EnvVarEntry {
                name: entry.name,
                value: String::new(),
                secret: entry.secret,
                value_encrypted: None,
                credential_ref: None,
                configured: false,
                description: None,
            };
            if let Some(value) = entry.value {
                if destination.secret {
                    let reference = new_reference("env")?;
                    credentials
                        .insert_share_secret(reference.clone(), &value)
                        .map_err(map_store)?;
                    destination.credential_ref = Some(reference);
                    destination.configured = true;
                } else {
                    destination.value = value;
                    destination.configured = !destination.value.is_empty();
                }
            }
            current.env_vars.push(destination);
        }
    }
    reject_network_initialization(&current)?;
    validate_retained_credentials(&current, &credentials)?;
    crate::validate_runtime_model_admission(&current).map_err(|_| ShareApiError::Invalid)?;
    providers.data.provider_instances = current.provider_instances.clone();
    providers.data.defaults = current.defaults.clone();
    env.data.0 = std::mem::take(&mut current.env_vars);
    Ok(ShareTransactionCandidate {
        providers: encode_changed_section(providers, snapshot.providers.as_deref())?,
        env: encode_changed_section(env, snapshot.env.as_deref())?,
        credentials: credentials.encode().map_err(map_store)?,
    })
}

fn encode_changed_section<T: Serialize + DeserializeOwned + Default>(
    mut section: Stored<T>,
    original: Option<&[u8]>,
) -> Result<Vec<u8>, ShareApiError> {
    let candidate = serde_json::to_value(&section.data).map_err(|_| ShareApiError::Invalid)?;
    let old: Stored<T> = decode_section(original)?;
    let old = serde_json::to_value(old.data).map_err(|_| ShareApiError::Invalid)?;
    if old == candidate {
        if let Some(original) = original {
            return Ok(original.to_vec());
        }
    } else {
        section.revision = section
            .revision
            .checked_add(1)
            .ok_or(ShareApiError::Invalid)?;
    }
    serde_json::to_vec_pretty(&section).map_err(|_| ShareApiError::Invalid)
}

fn requires_local_key(bundle: &ShareBundle, target: &Config) -> bool {
    bundle
        .providers
        .iter()
        .any(|provider| provider.api_key.is_some())
        || bundle.env.iter().any(|entry| {
            entry.value.is_some()
                && (entry.secret
                    || target.env_vars.iter().any(|saved| {
                        saved.secret
                            && config_share::canonical_env_name(&saved.name)
                                == config_share::canonical_env_name(&entry.name)
                    }))
        })
}

fn validate_retained_credentials(
    config: &Config,
    credentials: &ShareCredentialSnapshot,
) -> Result<(), ShareApiError> {
    let mut references = std::collections::BTreeSet::new();
    for instance in config.provider_instances.values() {
        if instance.api_key_encrypted.is_some() || !instance.api_key.is_empty() {
            return Err(ShareApiError::Unavailable);
        }
        references.extend(instance.credential_ref.iter().cloned());
    }
    macro_rules! legacy_refs {
        ($field:ident) => {
            if let Some(provider) = config.providers().$field.as_ref() {
                if provider.api_key_encrypted.is_some() || !provider.api_key.is_empty() {
                    return Err(ShareApiError::Unavailable);
                }
                references.extend(provider.credential_ref.iter().cloned());
            }
        };
    }
    legacy_refs!(openai);
    legacy_refs!(anthropic);
    legacy_refs!(gemini);
    legacy_refs!(bodhi);
    for entry in &config.env_vars {
        if entry.value_encrypted.is_some() || (entry.secret && !entry.value.is_empty()) {
            return Err(ShareApiError::Unavailable);
        }
        if entry.secret && entry.configured && entry.credential_ref.is_none() {
            return Err(ShareApiError::Unavailable);
        }
        references.extend(entry.credential_ref.iter().cloned());
    }
    for reference in references {
        let value = credentials
            .resolve_selected(&reference)
            .map_err(map_store)?;
        if value.trim().is_empty() || crate::patch::is_masked_api_key(&value) {
            return Err(ShareApiError::Unavailable);
        }
    }
    Ok(())
}

fn model_ref(
    value: &ShareModelRef,
    old: Option<&bamboo_domain::ProviderModelRef>,
) -> bamboo_domain::ProviderModelRef {
    bamboo_domain::ProviderModelRef {
        provider: value.provider.clone(),
        model: value.model.clone(),
        reasoning_effort: value
            .reasoning_effort
            .or_else(|| old.and_then(|old| old.reasoning_effort)),
    }
}

fn merge_scenarios(current: &mut Config, plan: &ShareBundle) -> Result<(), ShareApiError> {
    if plan.scenarios.is_empty() && plan.subagent_models.is_empty() {
        return Ok(());
    }
    if current.defaults.is_none() {
        let chat = plan
            .scenarios
            .get(&ShareScenario::Chat)
            .ok_or(ShareApiError::Invalid)?;
        current.defaults = Some(DefaultsConfig {
            chat: model_ref(chat, None),
            fast: None,
            task_summary: None,
            vision: None,
            memory_background: None,
            planning: None,
            search: None,
            code_review: None,
            sub_agent: None,
            subagent_models: Default::default(),
        });
    }
    let defaults = current.defaults.as_mut().ok_or(ShareApiError::Invalid)?;
    for (scenario, reference) in &plan.scenarios {
        let slot = match scenario {
            ShareScenario::Chat => {
                defaults.chat = model_ref(reference, Some(&defaults.chat));
                continue;
            }
            ShareScenario::Fast => &mut defaults.fast,
            ShareScenario::TaskSummary => &mut defaults.task_summary,
            ShareScenario::Vision => &mut defaults.vision,
            ShareScenario::MemoryBackground => &mut defaults.memory_background,
            ShareScenario::Planning => &mut defaults.planning,
            ShareScenario::Search => &mut defaults.search,
            ShareScenario::CodeReview => &mut defaults.code_review,
            ShareScenario::SubAgent => &mut defaults.sub_agent,
        };
        *slot = Some(model_ref(reference, slot.as_ref()));
    }
    for (name, reference) in &plan.subagent_models {
        defaults.subagent_models.insert(
            name.clone(),
            model_ref(reference, defaults.subagent_models.get(name)),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests;
