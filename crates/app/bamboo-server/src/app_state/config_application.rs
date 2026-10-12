//! Process-local configuration application receipts.
//!
//! A facade revision describes an observed effective/LKG input. Only a consumer's
//! successful publication records an applied receipt. Future-use readers report
//! readiness, and consumers without a publication receipt remain unknown.

use std::collections::{BTreeMap, BTreeSet};

use bamboo_config::{Config, ConfigFacade, SectionId, SectionStatus};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::AppState;

pub(super) type RevisionVector = BTreeMap<SectionId, u64>;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum ConfigConsumer {
    ProviderRuntime,
    McpRuntime,
    ConnectManager,
    AgentLoopDefaults,
    LiveToolFilters,
    AuxiliaryModels,
    NotificationSinks,
    Listener,
    MemoryBackgroundCadence,
    BrokerAndCluster,
}

const CONSUMERS: &[ConfigConsumer] = &[
    ConfigConsumer::ProviderRuntime,
    ConfigConsumer::McpRuntime,
    ConfigConsumer::ConnectManager,
    ConfigConsumer::AgentLoopDefaults,
    ConfigConsumer::LiveToolFilters,
    ConfigConsumer::AuxiliaryModels,
    ConfigConsumer::NotificationSinks,
    ConfigConsumer::Listener,
    ConfigConsumer::MemoryBackgroundCadence,
    ConfigConsumer::BrokerAndCluster,
];

#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ApplyMode {
    Immediate,
    NextRun,
    NextRound,
    NextEvent,
    Restart,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ApplicationStatus {
    InSync,
    PendingApply,
    Failed,
    PendingRestart,
    Unknown,
}

/// Static codes only. Never put a runtime error, configuration value or secret
/// identifier into an application diagnostic.
#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ConfigApplyError {
    ProviderInitializationFailed,
    RuntimeInputUnavailable,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct InputDigest([u8; 32]);

#[derive(Clone, Copy)]
enum CredentialBinding {
    NotRequired,
    Observed(u64),
    Unknown,
}

/// Captured from the exact configuration used to stage a runtime. It contains
/// only private digests and revision metadata, never another retained Config.
/// Neither this type nor its digests are serializable or debug-printable.
#[derive(Clone)]
pub(super) struct ApplicationInputs {
    revisions: RevisionVector,
    provider_complete: bool,
    digests: BTreeMap<ConfigConsumer, InputDigest>,
    credential_digests: BTreeMap<ConfigConsumer, InputDigest>,
    credentials: BTreeMap<ConfigConsumer, CredentialBinding>,
}

impl ApplicationInputs {
    /// Bootstrap only: no watcher has published a different live generation yet.
    pub(super) fn boot(config: &Config, facade: Option<&ConfigFacade>) -> Self {
        let mut inputs = Self::with_revisions(config, observed_revisions(facade));
        inputs.bind_observed_credentials(config, facade);
        inputs
    }

    fn with_revisions(config: &Config, revisions: RevisionVector) -> Self {
        Self {
            revisions,
            provider_complete: true,
            digests: CONSUMERS
                .iter()
                .filter_map(|consumer| input_digest(*consumer, config).map(|d| (*consumer, d)))
                .collect(),
            credential_digests: CONSUMERS
                .iter()
                .copied()
                .map(|consumer| (consumer, credential_digest(consumer, config)))
                .collect(),
            credentials: CONSUMERS
                .iter()
                .map(|consumer| {
                    (
                        *consumer,
                        if requires_credential_document(*consumer, config) {
                            CredentialBinding::Unknown
                        } else {
                            CredentialBinding::NotRequired
                        },
                    )
                })
                .collect(),
        }
    }

    pub(super) fn verify_provider_registry(
        &mut self,
        config: &Config,
        registry: &bamboo_llm::ProviderRegistry,
    ) {
        self.provider_complete = boot_provider_is_complete(config, registry);
    }

    /// A successful durable commit can advance its own input revision. Other
    /// revisions remain those captured before staging, even if another writer
    /// has since changed a different authority.
    pub(super) fn set_revision(&mut self, section: SectionId, revision: u64) {
        self.revisions.insert(section, revision);
    }

    /// Validate the actual staging inputs against one already observed LKG.
    /// This binds a source before construction; it never promotes an old
    /// receipt merely because a GET observes a newer document.
    pub(super) fn bind_observed_credentials(
        &mut self,
        config: &Config,
        facade: Option<&ConfigFacade>,
    ) {
        let Some(facade) = facade else {
            return;
        };
        let mut observed = facade.effective_config();
        observed.apply_runtime_env_overrides();
        let revision = facade.registry().credentials.snapshot().revision;
        let mut failed = BTreeSet::new();
        for section in [SectionId::Core, SectionId::Providers] {
            if facade
                .hydrate_runtime_section_from_observed_credentials(section, &mut observed)
                .ok()
                != Some(revision)
            {
                failed.insert(section);
            }
        }
        for consumer in [ConfigConsumer::ProviderRuntime] {
            if requires_credential_document(consumer, config)
                && !consumer.dependencies().iter().any(|id| failed.contains(id))
                && self.credential_digests.get(&consumer)
                    == Some(&credential_digest(consumer, &observed))
            {
                self.credentials
                    .insert(consumer, CredentialBinding::Observed(revision));
            }
        }
    }

    /// An owned-section transaction does not materialize sibling Core proxy
    /// inputs. Prove the complete consumer against the adopted credential LKG
    /// before attributing its staged runtime to the new document generation.
    pub(super) fn bind_committed_credentials(
        &mut self,
        installed: &Config,
        revision: u64,
        facade: Option<&ConfigFacade>,
    ) {
        let mut proof = self.clone();
        for consumer in CONSUMERS.iter().copied() {
            if requires_credential_document(consumer, installed) {
                proof
                    .credentials
                    .insert(consumer, CredentialBinding::Unknown);
            }
        }
        proof.bind_observed_credentials(installed, facade);
        for consumer in CONSUMERS.iter().copied() {
            if requires_credential_document(consumer, installed) {
                let binding = if self.credential_digests.get(&consumer)
                    == Some(&credential_digest(consumer, installed))
                    && matches!(proof.credentials.get(&consumer), Some(CredentialBinding::Observed(observed)) if *observed == revision)
                {
                    CredentialBinding::Observed(revision)
                } else {
                    CredentialBinding::Unknown
                };
                self.credentials.insert(consumer, binding);
            }
        }
    }
}

struct AppliedReceipt {
    revisions: RevisionVector,
    digest: InputDigest,
    applied_at: DateTime<Utc>,
    credentials: CredentialBinding,
}

#[derive(Default)]
struct ConsumerReceipt {
    applied: Option<AppliedReceipt>,
    last_error: Option<ConfigApplyError>,
    failed_section: Option<SectionId>,
}

#[derive(Default)]
pub(crate) struct ConfigApplicationState {
    receipts: BTreeMap<ConfigConsumer, ConsumerReceipt>,
    published_revisions: RevisionVector,
}

impl ConfigApplicationState {
    pub(super) fn new(boot: &ApplicationInputs) -> Self {
        Self {
            published_revisions: boot.revisions.clone(),
            ..Self::default()
        }
    }

    /// Runtime staging inherits the input versions actually installed in the
    /// live Config. The facade can already contain a newer, unapplied batch.
    pub(super) fn capture_live(&self, config: &Config) -> ApplicationInputs {
        ApplicationInputs::with_revisions(config, self.published_revisions.clone())
    }

    pub(super) fn capture_staged(
        &self,
        config: &Config,
        facade: Option<&ConfigFacade>,
    ) -> ApplicationInputs {
        let mut inputs = self.capture_live(config);
        inputs.bind_observed_credentials(config, facade);
        inputs
    }

    pub(super) fn record_facade_publication(&mut self, facade: Option<&ConfigFacade>) {
        self.published_revisions = observed_revisions(facade);
    }

    pub(super) fn record_events(&mut self, events: &[bamboo_config::ConfigSectionEvent]) {
        for event in events {
            match event {
                bamboo_config::ConfigSectionEvent::Changed {
                    section, revision, ..
                }
                | bamboo_config::ConfigSectionEvent::Recovered {
                    section, revision, ..
                } => {
                    if let Some(section) = SectionId::from_name(section) {
                        self.record_published(section, *revision);
                    }
                }
                _ => {}
            }
        }
    }

    pub(super) fn record_published(&mut self, section: SectionId, revision: u64) {
        self.published_revisions.insert(section, revision);
    }

    /// Call only after this consumer has actually published its staged runtime,
    /// while the existing config_io_lock still owns the publication generation.
    pub(super) fn record_applied(&mut self, consumer: ConfigConsumer, inputs: &ApplicationInputs) {
        if !matches!(consumer, ConfigConsumer::ProviderRuntime) {
            return;
        }
        if (consumer == ConfigConsumer::ProviderRuntime && !inputs.provider_complete)
            || consumer
                .dependencies()
                .iter()
                .any(|id| !inputs.revisions.contains_key(id))
        {
            // A legacy/unknown source is not a versioned publication receipt.
            self.receipts.remove(&consumer);
            return;
        }
        let Some(digest) = inputs.digests.get(&consumer).copied() else {
            return;
        };
        let credentials = inputs
            .credentials
            .get(&consumer)
            .copied()
            .unwrap_or(CredentialBinding::Unknown);
        let mut revisions = consumer_revisions(consumer, &inputs.revisions);
        if let CredentialBinding::Observed(revision) = credentials {
            revisions.insert(SectionId::Credentials, revision);
        }
        self.receipts.insert(
            consumer,
            ConsumerReceipt {
                applied: Some(AppliedReceipt {
                    revisions,
                    digest,
                    applied_at: Utc::now(),
                    credentials,
                }),
                last_error: None,
                failed_section: None,
            },
        );
    }

    pub(super) fn clear_matching_failures(&mut self, config: &Config, adopted: SectionId) {
        for (consumer, receipt) in &mut self.receipts {
            if receipt.failed_section == Some(adopted)
                && receipt
                    .applied
                    .as_ref()
                    .is_some_and(|applied| input_digest(*consumer, config) == Some(applied.digest))
            {
                receipt.last_error = None;
            }
        }
    }

    pub(super) fn record_failure(&mut self, consumer: ConfigConsumer, error: ConfigApplyError) {
        let section = if consumer == ConfigConsumer::ProviderRuntime {
            SectionId::Providers
        } else {
            SectionId::Mcp
        };
        self.record_section_failure(consumer, error, section);
    }

    pub(super) fn record_section_failure(
        &mut self,
        consumer: ConfigConsumer,
        error: ConfigApplyError,
        section: SectionId,
    ) {
        let receipt = self.receipts.entry(consumer).or_default();
        receipt.last_error = Some(error);
        receipt.failed_section = Some(section);
    }

    pub(super) fn diagnostics(
        &self,
        facade: Option<&ConfigFacade>,
        desired: &Config,
        live: &Config,
        unavailable: &BTreeSet<SectionId>,
    ) -> ConfigApplicationDiagnostics {
        let sections: BTreeMap<_, _> = facade
            .into_iter()
            .flat_map(|facade| facade.registry().health().unwrap_or_default())
            .map(|health| {
                (
                    health.section,
                    ObservedSection {
                        revision: health.revision,
                        status: health.status,
                    },
                )
            })
            .collect();
        self.report(sections, desired, live, unavailable, facade.is_some())
    }

    fn report(
        &self,
        sections: BTreeMap<SectionId, ObservedSection>,
        desired: &Config,
        _live: &Config,
        unavailable: &BTreeSet<SectionId>,
        authoritative: bool,
    ) -> ConfigApplicationDiagnostics {
        let revisions = sections
            .iter()
            .map(|(id, section)| (*id, section.revision))
            .collect();
        let mut desired_inputs = ApplicationInputs::with_revisions(desired, revisions);
        if let Some(revision) = sections.get(&SectionId::Credentials).map(|s| s.revision) {
            for consumer in CONSUMERS {
                if requires_credential_document(*consumer, desired) {
                    desired_inputs
                        .credentials
                        .insert(*consumer, CredentialBinding::Observed(revision));
                }
            }
        }
        let desired = desired_inputs;
        let consumers = CONSUMERS
            .iter()
            .map(|consumer| {
                let mode = consumer.mode();
                let receipt = self.receipts.get(consumer);
                let applied = receipt.and_then(|receipt| receipt.applied.as_ref());
                let inputs_unavailable = consumer
                    .dependencies()
                    .iter()
                    .any(|id| unavailable.contains(id));
                let wanted = (authoritative && !inputs_unavailable)
                    .then(|| desired.digests.get(consumer))
                    .flatten();
                let inputs_changed = wanted
                    .zip(applied)
                    .map(|(wanted, applied)| wanted != &applied.digest);
                let last_error = receipt.and_then(|receipt| receipt.last_error).or_else(|| {
                    inputs_unavailable.then_some(ConfigApplyError::RuntimeInputUnavailable)
                });
                let state = if !authoritative || consumer.untracked() {
                    ApplicationStatus::Unknown
                } else if last_error.is_some() && inputs_changed != Some(false) {
                    ApplicationStatus::Failed
                } else if applied.is_some_and(|applied| {
                    matches!(applied.credentials, CredentialBinding::Unknown)
                }) {
                    ApplicationStatus::Unknown
                } else {
                    match inputs_changed {
                        Some(false) => ApplicationStatus::InSync,
                        Some(true) if mode == ApplyMode::Restart => {
                            ApplicationStatus::PendingRestart
                        }
                        Some(true) => ApplicationStatus::PendingApply,
                        None => ApplicationStatus::Unknown,
                    }
                };
                ConsumerApplication {
                    id: *consumer,
                    fields: consumer.fields(),
                    mode,
                    state,
                    desired_dependencies: {
                        let mut revisions = consumer_revisions(*consumer, &desired.revisions);
                        if let Some(CredentialBinding::Observed(revision)) =
                            desired.credentials.get(consumer)
                        {
                            revisions.insert(SectionId::Credentials, *revision);
                        }
                        revisions
                    },
                    applied_dependencies: applied.map(|applied| applied.revisions.clone()),
                    published_dependencies: None,
                    inputs_changed,
                    last_applied_at: applied.map(|applied| applied.applied_at),
                    last_apply_error: last_error.map(|code| ApplyError { code }),
                    credential_source: applied.map(|applied| applied.credentials).map(|binding| {
                        match binding {
                            CredentialBinding::NotRequired => CredentialApplication {
                                status: "not_required",
                                revision: None,
                            },
                            CredentialBinding::Observed(revision) => CredentialApplication {
                                status: "observed_inputs",
                                revision: Some(revision),
                            },
                            CredentialBinding::Unknown => CredentialApplication {
                                status: "unknown",
                                revision: None,
                            },
                        }
                    }),
                }
            })
            .collect();
        ConfigApplicationDiagnostics {
            source: "process_observed_facade",
            observed_at: Utc::now(),
            sections,
            consumers,
        }
    }
}

#[derive(Serialize)]
pub(crate) struct ConfigApplicationDiagnostics {
    source: &'static str,
    observed_at: DateTime<Utc>,
    sections: BTreeMap<SectionId, ObservedSection>,
    consumers: Vec<ConsumerApplication>,
}

#[derive(Serialize)]
struct ObservedSection {
    revision: u64,
    status: SectionStatus,
}

#[derive(Serialize)]
struct ApplyError {
    code: ConfigApplyError,
}

#[derive(Serialize)]
struct CredentialApplication {
    status: &'static str,
    revision: Option<u64>,
}

#[derive(Serialize)]
struct ConsumerApplication {
    id: ConfigConsumer,
    fields: &'static [&'static str],
    mode: ApplyMode,
    state: ApplicationStatus,
    desired_dependencies: RevisionVector,
    applied_dependencies: Option<RevisionVector>,
    published_dependencies: Option<RevisionVector>,
    inputs_changed: Option<bool>,
    last_applied_at: Option<DateTime<Utc>>,
    last_apply_error: Option<ApplyError>,
    credential_source: Option<CredentialApplication>,
}

fn observed_revisions(facade: Option<&ConfigFacade>) -> RevisionVector {
    facade
        .into_iter()
        .flat_map(|facade| facade.registry().health().unwrap_or_default())
        .map(|health| (health.section, health.revision))
        .collect()
}

impl AppState {
    /// The caller holds config_io_lock, matching runtime publication ordering.
    pub(crate) async fn config_application_diagnostics_locked(
        &self,
    ) -> ConfigApplicationDiagnostics {
        let live = self.config.read().await;
        let mut desired = self
            .config_facade
            .as_ref()
            .map(|f| f.effective_config())
            .unwrap_or_else(|| live.clone());
        let mut unavailable = BTreeSet::new();
        desired.apply_runtime_env_overrides();
        // Reuse the existing hydration boundary without publishing environment
        // variables, mutating health or executing any runtime effect on a GET.
        if let Some(facade) = self.config_facade.as_ref() {
            let revision = facade.registry().credentials.snapshot().revision;
            for id in [SectionId::Core, SectionId::Providers] {
                if facade
                    .hydrate_runtime_section_from_observed_credentials(id, &mut desired)
                    .ok()
                    != Some(revision)
                {
                    unavailable.insert(id);
                }
            }
        }
        self.config_application
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .diagnostics(self.config_facade.as_deref(), &desired, &live, &unavailable)
    }
}

pub(super) fn boot_provider_is_complete(
    config: &Config,
    registry: &bamboo_llm::ProviderRegistry,
) -> bool {
    if registry.get_default().is_none() {
        return false;
    }
    if config.has_provider_instances() {
        return config
            .provider_instances
            .iter()
            .filter(|(_, instance)| instance.enabled)
            .all(|(name, _)| registry.get(name).is_some());
    }
    let providers = config.providers();
    [
        ("openai", providers.openai.is_some()),
        ("anthropic", providers.anthropic.is_some()),
        ("gemini", providers.gemini.is_some()),
        ("copilot", providers.copilot.is_some()),
        ("bodhi", providers.bodhi.is_some()),
    ]
    .iter()
    .all(|(name, configured)| !configured || registry.get(name).is_some())
}

fn consumer_revisions(consumer: ConfigConsumer, revisions: &RevisionVector) -> RevisionVector {
    consumer
        .dependencies()
        .iter()
        .filter_map(|id| revisions.get(id).map(|revision| (*id, *revision)))
        .collect()
}

impl ConfigConsumer {
    fn mode(self) -> ApplyMode {
        match self {
            Self::ProviderRuntime | Self::McpRuntime => ApplyMode::Immediate,
            Self::AgentLoopDefaults => ApplyMode::NextRun,
            Self::LiveToolFilters | Self::AuxiliaryModels => ApplyMode::NextRound,
            Self::NotificationSinks => ApplyMode::NextEvent,
            _ => ApplyMode::Restart,
        }
    }

    fn untracked(self) -> bool {
        self != Self::ProviderRuntime
    }

    fn dependencies(self) -> &'static [SectionId] {
        match self {
            Self::ProviderRuntime => &[
                SectionId::Providers,
                SectionId::Core,
                SectionId::ModelPolicy,
            ],
            _ => &[],
        }
    }

    fn fields(self) -> &'static [&'static str] {
        match self {
            Self::ProviderRuntime => &[
                "providers.clients",
                "core.proxy",
                "model-policy.keyword_masking",
            ],
            _ => &[],
        }
    }
}

fn proxy_input(config: &Config) -> Value {
    let proxy = if config.http_proxy.trim().is_empty() {
        config.https_proxy.trim()
    } else {
        config.http_proxy.trim()
    };
    json!({ "url": proxy, "auth": (!proxy.is_empty()).then_some(config.proxy_auth.as_ref()).flatten() })
}

fn provider_input(config: &Config) -> Value {
    // Plaintext keys intentionally do not implement Serialize. Hash them here
    // explicitly so rotation under an unchanged reference is still detected.
    let providers = config.providers();
    let keys: BTreeMap<_, _> = config
        .provider_instances
        .iter()
        .map(|(id, instance)| (id, &instance.api_key))
        .collect();
    let mut input = json!({
        "provider": config.provider,
        "providers": providers,
        "instances": config.provider_instances,
        "default_instance": config.default_provider_instance,
        "keys": {
            "openai": providers.openai.as_ref().map(|p| &p.api_key),
            "anthropic": providers.anthropic.as_ref().map(|p| &p.api_key),
            "gemini": providers.gemini.as_ref().map(|p| &p.api_key),
            "bodhi": providers.bodhi.as_ref().map(|p| &p.api_key),
            "instances": keys,
        },
    });
    for name in ["openai", "anthropic", "gemini", "bodhi"] {
        remove_storage_fields(
            &mut input["providers"][name],
            &["credential_ref", "api_key_encrypted"],
        );
    }
    if let Some(instances) = input["instances"].as_object_mut() {
        for instance in instances.values_mut() {
            remove_storage_fields(instance, &["credential_ref", "api_key_encrypted"]);
        }
    }
    input
}

fn credential_digest(consumer: ConfigConsumer, config: &Config) -> InputDigest {
    let providers = config.providers();
    let projection = match consumer {
        ConfigConsumer::ProviderRuntime => json!({
            "keys": [providers.openai.as_ref().map(|p| &p.api_key), providers.anthropic.as_ref().map(|p| &p.api_key), providers.gemini.as_ref().map(|p| &p.api_key), providers.bodhi.as_ref().map(|p| &p.api_key)],
            "instances": config.provider_instances.iter().map(|(id, p)| (id, &p.api_key)).collect::<BTreeMap<_, _>>(),
            "proxy_auth": config.proxy_auth,
        }),
        _ => Value::Null,
    };
    let mut hasher = Sha256::new();
    hash_value(&mut hasher, &projection);
    InputDigest(hasher.finalize().into())
}

fn requires_credential_document(consumer: ConfigConsumer, config: &Config) -> bool {
    let proxy = config.proxy_auth_credential_ref.is_some()
        && (!config.http_proxy.trim().is_empty() || !config.https_proxy.trim().is_empty());
    let providers = config.providers();
    match consumer {
        ConfigConsumer::ProviderRuntime => {
            proxy
                || providers
                    .openai
                    .as_ref()
                    .is_some_and(|p| p.credential_ref.is_some() && !p.api_key_from_env)
                || providers
                    .anthropic
                    .as_ref()
                    .is_some_and(|p| p.credential_ref.is_some() && !p.api_key_from_env)
                || providers
                    .gemini
                    .as_ref()
                    .is_some_and(|p| p.credential_ref.is_some() && !p.api_key_from_env)
                || providers
                    .bodhi
                    .as_ref()
                    .is_some_and(|p| p.credential_ref.is_some())
                || config.provider_instances.values().any(|p| {
                    p.credential_ref.is_some()
                        && !bamboo_config::provider_instance_environment_override_active(p)
                })
        }
        _ => false,
    }
}

fn input_digest(consumer: ConfigConsumer, config: &Config) -> Option<InputDigest> {
    let projection = match consumer {
        ConfigConsumer::ProviderRuntime => {
            json!({"clients": provider_input(config), "proxy": proxy_input(config), "masking": config.keyword_masking})
        }
        _ => return None,
    };
    let mut hasher = Sha256::new();
    hash_value(&mut hasher, &projection);
    Some(InputDigest(hasher.finalize().into()))
}

fn remove_storage_fields(value: &mut Value, fields: &[&str]) {
    if let Some(object) = value.as_object_mut() {
        for field in fields {
            object.remove(*field);
        }
    }
}

// Object order is not an input change (provider instances include HashMaps).
// Type and length framing prevent ambiguous concatenations. The resulting
// digest stays in memory and is never returned or written to disk.
fn hash_value(hasher: &mut Sha256, value: &Value) {
    match value {
        Value::Object(values) => {
            hasher.update(b"object");
            hasher.update((values.len() as u64).to_be_bytes());
            let sorted: BTreeMap<_, _> = values.iter().collect();
            for (key, value) in sorted {
                hash_value(hasher, &Value::String(key.to_string()));
                hash_value(hasher, value);
            }
        }
        Value::Array(values) => {
            hasher.update(b"array");
            hasher.update((values.len() as u64).to_be_bytes());
            for value in values {
                hash_value(hasher, value);
            }
        }
        scalar => {
            let bytes = scalar.to_string();
            hasher.update((bytes.len() as u64).to_be_bytes());
            hasher.update(bytes.as_bytes());
        }
    }
}

#[cfg(test)]
#[path = "config_application_tests.rs"]
mod tests;
