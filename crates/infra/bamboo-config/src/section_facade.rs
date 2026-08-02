//! Typed, independently revisioned configuration sections.
//!
//! [`Config`] remains the compatibility/effective-value view. This module is
//! the persistence-facing foundation: it gives every ADR-owned section one
//! stable file, typed DTO, [`AtomicJsonStore`] and immutable [`LiveSection`].
//! Server/API/CLI authority routing is deliberately layered on top of this
//! crate rather than embedded here.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::config_crypto::{access_device_credential_ref, access_password_credential_ref};
use crate::credential_store::{CredentialDocumentLkg, CredentialMutation};
use crate::{
    AccessControlConfig, AnthropicModelMapping, AtomicJsonStore, BrokerClientConfig,
    ClusterFabricConfig, Config, ConfigSectionEvent, ConfigStoreResult, ConfigValues,
    ConnectConfig, CredentialRef, CredentialSource, CredentialStatus, CredentialStore,
    CredentialStoreHealth, DefaultWorkAreaConfig, DefaultsConfig, EnvVarEntry, FeatureFlags,
    GeminiModelMapping, HooksConfig, KeywordMaskingConfig, LifecycleHooksConfig, LiveSection,
    MemoryConfig, NotificationsConfig, PluginTrustConfig, ProviderConfigs, ProviderInstanceConfig,
    RunBudgetConfig, SectionEnvelope, SectionSourceKind, SectionStatus, ServerConfig, SkillsConfig,
    StreamTimeoutConfig, SubagentsConfig, ToolsConfig,
};

pub(crate) const SECTION_SCHEMA_VERSION: u32 = 1;
pub const SECTION_LAYOUT_VERSION: u32 = 1;
pub const SECTION_LAYOUT_FILE: &str = "config-sections.json";
const SECTION_LAYOUT_COMPLETION_VERSION: u32 = 1;
pub const COPILOT_GITHUB_ACCESS_CREDENTIAL_REF: &str = "copilot.oauth.github_access_token";
pub const COPILOT_CHAT_CONFIG_CREDENTIAL_REF: &str = "copilot.oauth.chat_config";

/// Stable logical identity for every section in the accepted #597 ADR.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SectionId {
    Core,
    Providers,
    Mcp,
    ToolsSkills,
    Memory,
    Subagents,
    Notifications,
    Connect,
    ClusterFabric,
    Env,
    AccessControl,
    Hooks,
    ModelPolicy,
    ModelLimits,
    Credentials,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SectionDescriptor {
    pub id: SectionId,
    pub name: &'static str,
    pub file_name: &'static str,
    pub sensitive: bool,
}

pub const SECTION_DESCRIPTORS: [SectionDescriptor; 15] = [
    SectionDescriptor {
        id: SectionId::Core,
        name: "core",
        file_name: "core.json",
        sensitive: false,
    },
    SectionDescriptor {
        id: SectionId::Providers,
        name: "providers",
        file_name: "providers.json",
        sensitive: false,
    },
    SectionDescriptor {
        id: SectionId::Mcp,
        name: "mcp",
        file_name: "mcp.json",
        sensitive: false,
    },
    SectionDescriptor {
        id: SectionId::ToolsSkills,
        name: "tools-skills",
        file_name: "tools-skills.json",
        sensitive: false,
    },
    SectionDescriptor {
        id: SectionId::Memory,
        name: "memory",
        file_name: "memory.json",
        sensitive: false,
    },
    SectionDescriptor {
        id: SectionId::Subagents,
        name: "subagents",
        file_name: "subagents.json",
        sensitive: false,
    },
    SectionDescriptor {
        id: SectionId::Notifications,
        name: "notifications",
        file_name: "notifications.json",
        sensitive: false,
    },
    SectionDescriptor {
        id: SectionId::Connect,
        name: "connect",
        file_name: "connect.json",
        sensitive: false,
    },
    SectionDescriptor {
        id: SectionId::ClusterFabric,
        name: "cluster-fabric",
        file_name: "cluster-fabric.json",
        sensitive: false,
    },
    SectionDescriptor {
        id: SectionId::Env,
        name: "env",
        file_name: "env.json",
        sensitive: false,
    },
    SectionDescriptor {
        id: SectionId::AccessControl,
        name: "access-control",
        file_name: "access-control.json",
        sensitive: false,
    },
    SectionDescriptor {
        id: SectionId::Hooks,
        name: "hooks",
        file_name: "hooks.json",
        sensitive: false,
    },
    SectionDescriptor {
        id: SectionId::ModelPolicy,
        name: "model-policy",
        file_name: "model-policy.json",
        sensitive: false,
    },
    SectionDescriptor {
        id: SectionId::ModelLimits,
        name: "model-limits",
        file_name: "model_limits.json",
        sensitive: false,
    },
    SectionDescriptor {
        id: SectionId::Credentials,
        name: "credentials",
        file_name: "credentials.json",
        sensitive: true,
    },
];

impl SectionId {
    pub fn descriptor(self) -> &'static SectionDescriptor {
        SECTION_DESCRIPTORS
            .iter()
            .find(|descriptor| descriptor.id == self)
            .expect("every SectionId has one descriptor")
    }

    pub fn from_file_name(file_name: &str) -> Option<Self> {
        SECTION_DESCRIPTORS
            .iter()
            .find(|descriptor| descriptor.file_name == file_name)
            .map(|descriptor| descriptor.id)
    }

    pub fn from_name(name: &str) -> Option<Self> {
        SECTION_DESCRIPTORS
            .iter()
            .find(|descriptor| descriptor.name == name)
            .map(|descriptor| descriptor.id)
    }
}

/// Explicit ownership table for every field in `ConfigValues`.
///
/// `proxy_auth` and `proxy_auth_encrypted` remain compatibility read fields,
/// but their owner is still core; section serialization always drops them in
/// favour of `proxy_auth_credential_ref`.
pub const CONFIG_VALUE_FIELD_OWNERS: [(&str, SectionId); 30] = [
    ("http_proxy", SectionId::Core),
    ("https_proxy", SectionId::Core),
    ("proxy_auth", SectionId::Core),
    ("proxy_auth_encrypted", SectionId::Core),
    ("proxy_auth_credential_ref", SectionId::Core),
    ("headless_auth", SectionId::Core),
    ("provider", SectionId::Providers),
    ("defaults", SectionId::Providers),
    ("provider_instances", SectionId::Providers),
    ("default_provider_instance", SectionId::Providers),
    ("server", SectionId::Core),
    ("keyword_masking", SectionId::ModelPolicy),
    ("anthropic_model_mapping", SectionId::ModelPolicy),
    ("gemini_model_mapping", SectionId::ModelPolicy),
    ("hooks", SectionId::Hooks),
    ("lifecycle_hooks", SectionId::Hooks),
    ("tools", SectionId::ToolsSkills),
    ("skills", SectionId::ToolsSkills),
    ("env_vars", SectionId::Env),
    ("default_work_area", SectionId::Core),
    ("access_control", SectionId::AccessControl),
    ("features", SectionId::Providers),
    ("run_budget", SectionId::Core),
    ("stream_timeout", SectionId::Core),
    ("cluster_fabric", SectionId::ClusterFabric),
    ("mcp", SectionId::Mcp),
    ("notifications", SectionId::Notifications),
    ("connect", SectionId::Connect),
    ("plugin_trust", SectionId::ToolsSkills),
    ("extra", SectionId::Core),
];

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CoreSection {
    #[serde(default)]
    pub http_proxy: String,
    #[serde(default)]
    pub https_proxy: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_auth_credential_ref: Option<crate::CredentialRef>,
    #[serde(default)]
    pub headless_auth: bool,
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_work_area: Option<DefaultWorkAreaConfig>,
    #[serde(default)]
    pub run_budget: RunBudgetConfig,
    #[serde(default)]
    pub stream_timeout: StreamTimeoutConfig,
    /// Only genuinely unclassified root keys live here.
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Provider settings retain the existing flattened `providers.json` wire
/// shape. Routing/default/features fields are additive flattened keys, so the
/// existing `AtomicJsonStore<ProviderConfigs>` and exact credential transaction
/// can continue parsing them through `ProviderConfigs::extra`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProvidersSection {
    /// Legacy single-provider routing key. Instance-native configurations with
    /// an explicit default omit it from durable storage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub defaults: Option<DefaultsConfig>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub provider_instances: HashMap<String, ProviderInstanceConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_provider_instance: Option<String>,
    #[serde(default)]
    pub features: FeatureFlags,
    #[serde(flatten)]
    pub providers: ProviderConfigs,
}

fn default_provider_name() -> String {
    "openai".to_string()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct McpSection(pub bamboo_domain::mcp_config::McpConfig);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ToolsSkillsSection {
    #[serde(default)]
    pub tools: ToolsConfig,
    #[serde(default)]
    pub skills: SkillsConfig,
    #[serde(default)]
    pub plugin_trust: PluginTrustConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MemorySection(pub Option<MemoryConfig>);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SubagentsSection {
    #[serde(flatten)]
    pub subagents: SubagentsConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_broker: Option<BrokerClientConfig>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NotificationsSection {
    #[serde(default)]
    pub notifications: NotificationsConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ConnectSection(pub ConnectConfig);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ClusterFabricSection(pub ClusterFabricConfig);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EnvSection(pub Vec<EnvVarEntry>);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AccessControlSection(pub Option<AccessControlConfig>);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HooksSection {
    #[serde(flatten)]
    pub request_hooks: HooksConfig,
    #[serde(default, skip_serializing_if = "LifecycleHooksConfig::is_empty")]
    pub lifecycle_hooks: LifecycleHooksConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelPolicySection {
    #[serde(default)]
    pub keyword_masking: KeywordMaskingConfig,
    #[serde(default)]
    pub anthropic_model_mapping: AnthropicModelMapping,
    #[serde(default)]
    pub gemini_model_mapping: GeminiModelMapping,
}

/// The compression crate owns the row schema. Keeping the rows as JSON here
/// preserves forward-compatible custom fields while still typing the section
/// boundary and requiring an array document.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ModelLimitsSection(pub Vec<Value>);

/// Complete typed projection used by the migration planner and tests.
#[derive(Debug, Clone)]
pub struct SectionProjection {
    pub core: CoreSection,
    pub providers: ProvidersSection,
    pub mcp: McpSection,
    pub tools_skills: ToolsSkillsSection,
    pub memory: MemorySection,
    pub subagents: SubagentsSection,
    pub notifications: NotificationsSection,
    pub connect: ConnectSection,
    pub cluster_fabric: ClusterFabricSection,
    pub env: EnvSection,
    pub access_control: AccessControlSection,
    pub hooks: HooksSection,
    pub model_policy: ModelPolicySection,
    pub model_limits: ModelLimitsSection,
}

#[derive(Debug, Clone)]
pub(crate) struct PlannedSectionFile {
    pub name: &'static str,
    pub candidate: Vec<u8>,
    pub original: Vec<u8>,
    pub original_present: bool,
    pub revision: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct SectionSplitPlan {
    pub files: Vec<PlannedSectionFile>,
    /// Source documents which are not themselves section targets. The shared
    /// migration lock rechecks these hashes before installing the manifest so
    /// a compatibility writer cannot be silently overwritten by a stale plan.
    pub source_hashes: Vec<(String, String)>,
    pub marker: Vec<u8>,
    pub credential_plan: Option<crate::credential_migration::FacadeCredentialPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SectionDocument<T> {
    schema_version: u32,
    revision: u64,
    data: T,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SectionLayoutMarker {
    layout_version: u32,
    sections: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    completion: Option<SectionLayoutCompletion>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SectionLayoutCompletion {
    pub(crate) version: u32,
    pub(crate) transaction_id: String,
    pub(crate) members: BTreeMap<String, SectionLayoutCompletionMember>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SectionLayoutCompletionMember {
    pub(crate) revision: u64,
    pub(crate) sha256: String,
}

impl SectionProjection {
    pub fn from_config(
        config: &Config,
        model_limits: ModelLimitsSection,
    ) -> ConfigStoreResult<Self> {
        let mut disk = config.clone();
        disk.clear_legacy_provider_aliases_for_instance_mode();
        disk.ensure_provider_instance_credentials_isolated()
            .map_err(|error| crate::ConfigStoreError::Validation(error.to_string()))?;
        disk.sanitize_mcp_credential_refs_for_disk();
        disk.sanitize_env_vars_for_disk();
        disk.sanitize_notifications_for_disk();
        disk.sanitize_cluster_fabric_for_disk();
        disk.sanitize_connect_credentials_for_disk();
        disk.sanitize_access_control_for_disk();

        let mut providers = disk.providers().clone();
        for reserved in [
            "provider",
            "defaults",
            "provider_instances",
            "default_provider_instance",
            "features",
        ] {
            providers.extra.remove(reserved);
        }
        macro_rules! clear_provider_ciphertext {
            ($field:ident) => {
                if let Some(provider) = providers.$field.as_mut() {
                    provider.api_key.clear();
                    provider.api_key_encrypted = None;
                }
            };
        }
        clear_provider_ciphertext!(openai);
        clear_provider_ciphertext!(anthropic);
        clear_provider_ciphertext!(gemini);
        if let Some(provider) = providers.bodhi.as_mut() {
            provider.api_key.clear();
            provider.api_key_encrypted = None;
        }

        // `SubagentsConfig::broker` is runtime-only. The migration planner
        // adopts a user-managed `broker.json` explicitly below; projecting the
        // live value here would accidentally persist the embedded broker's
        // ephemeral loopback endpoint on an unrelated compatibility write.
        let external_broker = None;

        // Exhaustive destructuring is intentional: adding a ConfigValues field
        // without assigning it to a section is a compile error.
        let ConfigValues {
            http_proxy,
            https_proxy,
            proxy_auth: _,
            proxy_auth_encrypted: _,
            proxy_auth_credential_ref,
            headless_auth,
            provider,
            defaults,
            provider_instances,
            default_provider_instance,
            server,
            keyword_masking,
            anthropic_model_mapping,
            gemini_model_mapping,
            hooks,
            lifecycle_hooks,
            tools,
            skills,
            env_vars,
            default_work_area,
            access_control,
            features,
            run_budget,
            stream_timeout,
            cluster_fabric,
            mcp,
            notifications,
            connect,
            plugin_trust,
            extra,
        } = disk.section_values();

        let provider = default_provider_instance.is_none().then_some(provider);

        Ok(Self {
            core: CoreSection {
                http_proxy,
                https_proxy,
                proxy_auth_credential_ref,
                headless_auth,
                server,
                default_work_area,
                run_budget,
                stream_timeout,
                extra,
            },
            providers: ProvidersSection {
                provider,
                defaults,
                provider_instances,
                default_provider_instance,
                features,
                providers,
            },
            mcp: McpSection(mcp),
            tools_skills: ToolsSkillsSection {
                tools,
                skills,
                plugin_trust,
            },
            memory: MemorySection(disk.memory().clone()),
            subagents: SubagentsSection {
                subagents: disk.subagents().clone(),
                external_broker,
            },
            notifications: NotificationsSection { notifications },
            connect: ConnectSection(connect),
            cluster_fabric: ClusterFabricSection(cluster_fabric),
            env: EnvSection(env_vars),
            access_control: AccessControlSection(access_control),
            hooks: HooksSection {
                request_hooks: hooks,
                lifecycle_hooks,
            },
            model_policy: ModelPolicySection {
                keyword_masking,
                anthropic_model_mapping,
                gemini_model_mapping,
            },
            model_limits,
        })
    }

    pub fn into_config(self) -> Config {
        let Self {
            core,
            providers,
            mcp,
            tools_skills,
            memory,
            subagents,
            notifications,
            connect,
            cluster_fabric,
            env,
            access_control,
            hooks,
            model_policy,
            model_limits: _,
        } = self;
        let CoreSection {
            http_proxy,
            https_proxy,
            proxy_auth_credential_ref,
            headless_auth,
            server,
            default_work_area,
            run_budget,
            stream_timeout,
            extra,
        } = core;
        let ProvidersSection {
            provider,
            defaults,
            provider_instances,
            default_provider_instance,
            features,
            providers,
        } = providers;
        let ToolsSkillsSection {
            tools,
            skills,
            plugin_trust,
        } = tools_skills;
        let NotificationsSection { notifications } = notifications;
        let ModelPolicySection {
            keyword_masking,
            anthropic_model_mapping,
            gemini_model_mapping,
        } = model_policy;
        let SubagentsSection {
            subagents: mut subagent_values,
            external_broker,
        } = subagents;
        subagent_values.broker = external_broker;

        Config::from_section_parts(
            ConfigValues {
                http_proxy,
                https_proxy,
                proxy_auth: None,
                proxy_auth_encrypted: None,
                proxy_auth_credential_ref,
                headless_auth,
                provider: provider.unwrap_or_else(default_provider_name),
                defaults,
                provider_instances,
                default_provider_instance,
                server,
                keyword_masking,
                anthropic_model_mapping,
                gemini_model_mapping,
                hooks: hooks.request_hooks,
                lifecycle_hooks: hooks.lifecycle_hooks,
                tools,
                skills,
                env_vars: env.0,
                default_work_area,
                access_control: access_control.0,
                features,
                run_budget,
                stream_timeout,
                cluster_fabric: cluster_fabric.0,
                mcp: mcp.0,
                notifications,
                connect: connect.0,
                plugin_trust,
                extra,
            },
            memory.0,
            subagent_values,
            providers,
        )
    }

    #[cfg(test)]
    pub(crate) fn into_migration_plan(
        self,
        data_dir: &Path,
        source_hashes: Vec<(String, String)>,
    ) -> ConfigStoreResult<SectionSplitPlan> {
        self.into_migration_plan_with_raw(data_dir, source_hashes, BTreeMap::new(), None)
    }

    fn into_migration_plan_with_raw(
        mut self,
        data_dir: &Path,
        source_hashes: Vec<(String, String)>,
        mut legacy_raw: BTreeMap<SectionId, Value>,
        overrides: Option<&StrictSourceOverrides>,
    ) -> ConfigStoreResult<SectionSplitPlan> {
        // model_limits may still be present as a legacy flattened root key.
        // Its typed target owns it after the layout commit.
        self.core.extra.remove("model_limits");
        let mut files = Vec::with_capacity(14);
        macro_rules! add {
            ($id:ident, $value:expr, $validate:expr) => {{
                let descriptor = SectionId::$id.descriptor();
                files.push(plan_section_file(
                    data_dir,
                    descriptor.file_name,
                    &$value,
                    legacy_raw.remove(&SectionId::$id),
                    $validate,
                    overrides,
                )?);
            }};
        }
        add!(Core, self.core, validate_core);
        add!(Providers, self.providers, validate_providers);
        add!(Mcp, self.mcp, validate_mcp);
        add!(ToolsSkills, self.tools_skills, validate_tools_skills);
        add!(Memory, self.memory, validate_memory);
        add!(Subagents, self.subagents, validate_subagents);
        add!(Notifications, self.notifications, validate_notifications);
        add!(Connect, self.connect, validate_connect);
        add!(ClusterFabric, self.cluster_fabric, validate_cluster_fabric);
        add!(Env, self.env, validate_env);
        add!(AccessControl, self.access_control, validate_access_control);
        add!(Hooks, self.hooks, validate_hooks);
        add!(ModelPolicy, self.model_policy, validate_model_policy);
        add!(ModelLimits, self.model_limits, validate_model_limits);

        let marker = SectionLayoutMarker {
            layout_version: SECTION_LAYOUT_VERSION,
            sections: SECTION_DESCRIPTORS
                .iter()
                .map(|descriptor| {
                    (
                        descriptor.name.to_string(),
                        descriptor.file_name.to_string(),
                    )
                })
                .collect(),
            completion: None,
        };
        Ok(SectionSplitPlan {
            files,
            source_hashes,
            marker: serde_json::to_vec_pretty(&marker)?,
            credential_plan: None,
        })
    }
}

pub(crate) fn apply_runtime_section(id: SectionId, source: &Config, target: &mut Config) {
    match id {
        SectionId::Core => {
            target.http_proxy = source.http_proxy.clone();
            target.https_proxy = source.https_proxy.clone();
            target.proxy_auth = source.proxy_auth.clone();
            target.proxy_auth_encrypted = None;
            target.proxy_auth_credential_ref = source.proxy_auth_credential_ref.clone();
            target.headless_auth = source.headless_auth;
            target.server = source.server.clone();
            target.default_work_area = source.default_work_area.clone();
            target.run_budget = source.run_budget;
            target.stream_timeout = source.stream_timeout;
            target.extra = source.extra.clone();
        }
        SectionId::Providers => {
            target.provider = source.provider.clone();
            target.defaults = source.defaults.clone();
            target.provider_instances = source.provider_instances.clone();
            target.default_provider_instance = source.default_provider_instance.clone();
            target.features = source.features.clone();
            *target.providers_mut() = source.providers().clone();
        }
        SectionId::Mcp => target.mcp = source.mcp.clone(),
        SectionId::ToolsSkills => {
            target.tools = source.tools.clone();
            target.skills = source.skills.clone();
            target.plugin_trust = source.plugin_trust.clone();
        }
        SectionId::Memory => *target.memory_mut() = source.memory().clone(),
        SectionId::Subagents => *target.subagents_mut() = source.subagents().clone(),
        SectionId::Notifications => target.notifications = source.notifications.clone(),
        SectionId::Connect => target.connect = source.connect.clone(),
        SectionId::ClusterFabric => target.cluster_fabric = source.cluster_fabric.clone(),
        SectionId::Env => target.env_vars = source.env_vars.clone(),
        SectionId::AccessControl => target.access_control = source.access_control.clone(),
        SectionId::Hooks => target.hooks = source.hooks.clone(),
        SectionId::ModelPolicy => {
            target.keyword_masking = source.keyword_masking.clone();
            target.anthropic_model_mapping = source.anthropic_model_mapping.clone();
            target.gemini_model_mapping = source.gemini_model_mapping.clone();
        }
        SectionId::ModelLimits | SectionId::Credentials => {}
    }
}

fn plan_section_file<T>(
    data_dir: &Path,
    name: &'static str,
    value: &T,
    legacy_raw: Option<Value>,
    validate: fn(&T) -> Result<(), String>,
    overrides: Option<&StrictSourceOverrides>,
) -> ConfigStoreResult<PlannedSectionFile>
where
    T: Serialize + serde::de::DeserializeOwned,
{
    let path = data_dir.join(name);
    let original_state = read_existing_bytes(&path)?;
    let original_present = original_state.is_some();
    let original = original_state.unwrap_or_default();
    let planning_bytes = overrides
        .and_then(|overrides| overrides.get(name))
        .cloned()
        .unwrap_or_else(|| original.clone());
    let (existing_revision, existing_data) = compatible_section_data(&planning_bytes)?;
    let existing_envelope = serde_json::from_slice::<Value>(&planning_bytes)
        .ok()
        .and_then(|value| value.as_object().cloned())
        .is_some_and(|object| {
            object.contains_key("schema_version")
                && object.contains_key("revision")
                && object.contains_key("data")
        });
    let has_legacy_raw = legacy_raw.is_some();
    let typed_baseline = serde_json::to_value(value)?;
    let instance_native_providers = name == "providers.json"
        && typed_baseline
            .get("default_provider_instance")
            .is_some_and(|value| !value.is_null());
    let mut merged_data = match legacy_raw {
        Some(mut raw) => {
            if instance_native_providers {
                remove_instance_native_legacy_provider_fields(&mut raw);
            }
            deep_merge_typed_over_raw(&mut raw, typed_baseline);
            raw
        }
        None => typed_baseline,
    };
    if let Some(existing_data) = existing_data.as_ref() {
        let mut existing_data = existing_data.clone();
        if instance_native_providers {
            remove_instance_native_legacy_provider_fields(&mut existing_data);
        }
        deep_merge_json(&mut merged_data, existing_data);
    }
    if instance_native_providers {
        remove_instance_native_legacy_provider_fields(&mut merged_data);
    }
    // A materialized default is still the revision-zero baseline. Preserve an
    // already-versioned sidecar when migration leaves its data byte-equivalent;
    // importing legacy data or normalizing a sparse envelope is a real revision
    // advance.
    let revision = if !original_present && !has_legacy_raw {
        0
    } else if existing_envelope && existing_data.as_ref() == Some(&merged_data) {
        existing_revision
    } else {
        existing_revision.checked_add(1).ok_or_else(|| {
            crate::ConfigStoreError::Validation(
                "configuration revision counter exhausted".to_string(),
            )
        })?
    };
    validate_ordinary_section_json(&merged_data)?;
    let typed: T = serde_json::from_value(merged_data.clone())?;
    validate(&typed).map_err(crate::ConfigStoreError::Validation)?;
    let candidate = serde_json::to_vec_pretty(&SectionDocument {
        schema_version: SECTION_SCHEMA_VERSION,
        revision,
        data: merged_data,
    })?;
    Ok(PlannedSectionFile {
        name,
        candidate,
        original,
        original_present,
        revision,
    })
}

fn compatible_section_data(bytes: &[u8]) -> ConfigStoreResult<(u64, Option<Value>)> {
    if bytes.is_empty() {
        return Ok((0, None));
    }
    let value: Value = serde_json::from_slice(bytes)?;
    let Some(object) = value.as_object() else {
        return Ok((0, Some(value)));
    };
    let is_complete_envelope = object.contains_key("schema_version")
        && object.contains_key("revision")
        && object.contains_key("data");
    if !is_complete_envelope {
        return Ok((0, Some(value)));
    }
    let schema = object
        .get("schema_version")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            crate::ConfigStoreError::Validation("section revision envelope is invalid".to_string())
        })?;
    if schema > u64::from(SECTION_SCHEMA_VERSION) {
        return Err(crate::ConfigStoreError::Validation(
            "section document schema is newer than this runtime".to_string(),
        ));
    }
    let revision = object
        .get("revision")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            crate::ConfigStoreError::Validation("section revision envelope is invalid".to_string())
        })?;
    let data = object.get("data").cloned().ok_or_else(|| {
        crate::ConfigStoreError::Validation("section revision envelope is invalid".to_string())
    })?;
    Ok((revision, Some(data)))
}

fn deep_merge_json(base: &mut Value, overlay: Value) {
    match (base, overlay) {
        (Value::Object(base), Value::Object(overlay)) => {
            for (key, value) in overlay {
                match base.get_mut(&key) {
                    Some(base_value) => deep_merge_json(base_value, value),
                    None => {
                        base.insert(key, value);
                    }
                }
            }
        }
        (base, overlay) => *base = overlay,
    }
}

/// Remove only legacy routing and known built-in aliases from an
/// instance-native providers document. Unknown keys remain untouched so a
/// newer runtime's provider metadata survives migration by an older binary.
fn remove_instance_native_legacy_provider_fields(value: &mut Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    for key in [
        "provider",
        "openai",
        "anthropic",
        "gemini",
        "copilot",
        "bodhi",
    ] {
        object.remove(key);
    }
}

/// Merge a typed, precedence-resolved baseline over legacy raw JSON while
/// retaining forward fields which the current DTO does not yet understand.
/// Array elements are paired by index so nested future fields in legacy
/// connect/platform and policy entries are not erased by typed serialization.
fn deep_merge_typed_over_raw(base: &mut Value, overlay: Value) {
    match (base, overlay) {
        (Value::Object(base), Value::Object(overlay)) => {
            for (key, value) in overlay {
                match base.get_mut(&key) {
                    Some(base_value) => deep_merge_typed_over_raw(base_value, value),
                    None => {
                        base.insert(key, value);
                    }
                }
            }
        }
        (Value::Array(base), Value::Array(overlay)) => {
            let overlay_len = overlay.len();
            for (index, value) in overlay.into_iter().enumerate() {
                match base.get_mut(index) {
                    Some(base_value) => deep_merge_typed_over_raw(base_value, value),
                    None => base.push(value),
                }
            }
            base.truncate(overlay_len);
        }
        (base, overlay) => *base = overlay,
    }
}

fn validate_ordinary_section_json(value: &Value) -> ConfigStoreResult<()> {
    fn walk(value: &Value, classify_object_keys: bool) -> Result<(), String> {
        match value {
            Value::Object(object) => {
                for (key, value) in object {
                    let normalized_key = key
                        .chars()
                        .filter(|ch| ch.is_ascii_alphanumeric())
                        .flat_map(char::to_lowercase)
                        .collect::<String>();
                    let forbidden_key = classify_object_keys
                        && (key.to_ascii_lowercase().ends_with("_encrypted")
                            || normalized_key.ends_with("encrypted")
                            || normalized_key.ends_with("token")
                            || normalized_key.ends_with("password")
                            || normalized_key.ends_with("secret")
                            || matches!(
                                normalized_key.as_str(),
                                "apikey" | "proxyauth" | "privatekey" | "passphrase" | "devicekey"
                            ));
                    let boolean_secret_metadata = normalized_key == "secret" && value.is_boolean();
                    if forbidden_key && !boolean_secret_metadata && !value_is_empty(value) {
                        return Err(format!(
                            "ordinary section contains forbidden credential material in {key}"
                        ));
                    }
                    if classify_object_keys
                        && (normalized_key == "credentialref"
                            || normalized_key.ends_with("credentialref"))
                    {
                        if let Some(reference) = value.as_str() {
                            CredentialRef::parse(reference.to_string()).map_err(|_| {
                                "credential reference has an invalid format".to_string()
                            })?;
                        } else if !value.is_null() {
                            return Err("credential reference must be a string".to_string());
                        }
                    }
                    if classify_object_keys && normalized_key == "providerinstances" {
                        let instances = value.as_object().ok_or_else(|| {
                            "provider instance collection must be an object".to_string()
                        })?;
                        if instances.values().any(|instance| !instance.is_object()) {
                            return Err("provider instance must be an object".to_string());
                        }
                    }
                    if classify_object_keys && normalized_key.ends_with("credentialrefs") {
                        if !matches!(
                            normalized_key.as_str(),
                            "credentialrefs" | "envcredentialrefs" | "headercredentialrefs"
                        ) {
                            return Err("unknown credential reference collection is not allowed"
                                .to_string());
                        }
                        let references = value.as_object().ok_or_else(|| {
                            "credential reference collection must be an object".to_string()
                        })?;
                        for reference in references.values() {
                            if let Some(reference) = reference.as_str() {
                                CredentialRef::parse(reference.to_string()).map_err(|_| {
                                    "credential reference has an invalid format".to_string()
                                })?;
                            } else if !reference.is_object() {
                                return Err(
                                    "credential reference entry must be a string or object"
                                        .to_string(),
                                );
                            }
                        }
                    }
                    let child_keys_are_identifiers = classify_object_keys
                        && (matches!(
                            normalized_key.as_str(),
                            "headers" | "providerinstances" | "credentialrefs"
                        ) || normalized_key.ends_with("credentialrefs"));
                    if child_keys_are_identifiers {
                        if normalized_key == "headers" {
                            if let Some(headers) = value.as_object() {
                                for (name, expression) in headers {
                                    if is_credential_header(name)
                                        && !value_is_empty(expression)
                                        && !is_nonliteral_runtime_template(expression)
                                    {
                                        return Err(format!(
                                            "credential header {name} must not contain a literal"
                                        ));
                                    }
                                }
                            }
                        }
                        match value {
                            Value::Object(entries) => {
                                for entry in entries.values() {
                                    walk(entry, true)?;
                                }
                            }
                            Value::Array(entries) => {
                                for entry in entries {
                                    walk(entry, true)?;
                                }
                            }
                            _ => walk(value, true)?,
                        }
                    } else {
                        walk(value, true)?;
                    }
                }
                if classify_object_keys {
                    validate_configured_reference_coherence(object)
                } else {
                    Ok(())
                }
            }
            Value::Array(values) => values.iter().try_for_each(|value| walk(value, true)),
            _ => Ok(()),
        }
    }

    walk(value, true).map_err(crate::ConfigStoreError::Validation)?;
    validate_provider_override_credentials(value).map_err(crate::ConfigStoreError::Validation)
}

pub(crate) fn validate_ordinary_section_raw(value: &Value) -> Result<(), String> {
    validate_ordinary_section_json(value).map_err(|error| error.to_string())
}

fn value_is_empty(value: &Value) -> bool {
    value.is_null()
        || value.as_str().is_some_and(|value| value.trim().is_empty())
        || value.as_object().is_some_and(serde_json::Map::is_empty)
        || value.as_array().is_some_and(Vec::is_empty)
}

fn validate_configured_reference_coherence(
    object: &serde_json::Map<String, Value>,
) -> Result<(), String> {
    for (key, configured) in object {
        let Some(prefix) = key.strip_suffix("configured") else {
            continue;
        };
        let reference_key = format!("{prefix}credential_ref");
        if !object.contains_key(&reference_key) && prefix.is_empty() {
            continue;
        }
        let configured = configured
            .as_bool()
            .ok_or_else(|| "credential configured metadata must be boolean".to_string())?;
        let referenced = object
            .get(&reference_key)
            .is_some_and(|value| value.as_str().is_some_and(|value| !value.is_empty()));
        if configured != referenced {
            return Err("credential configured metadata does not match its reference".to_string());
        }
    }
    Ok(())
}

fn validate_json_serializable<T: Serialize>(value: &T) -> Result<(), String> {
    serde_json::to_value(value)
        .map(|_| ())
        .map_err(|_| "section cannot be serialized".to_string())
}

fn validate_core(value: &CoreSection) -> Result<(), String> {
    value.stream_timeout.validate()?;
    validate_secret_free_http_url("HTTP proxy", &value.http_proxy, true)?;
    validate_secret_free_http_url("HTTPS proxy", &value.https_proxy, true)?;
    validate_json_serializable(value)
}

fn validate_providers(value: &ProvidersSection) -> Result<(), String> {
    if value
        .provider
        .as_ref()
        .is_some_and(|provider| provider.trim().is_empty())
    {
        return Err("default provider must not be empty".to_string());
    }
    for (id, instance) in &value.provider_instances {
        if id.trim().is_empty() || instance.provider_type.trim().is_empty() {
            return Err("provider instance id and type must not be empty".to_string());
        }
    }
    if value
        .default_provider_instance
        .as_ref()
        .is_some_and(|id| !value.provider_instances.contains_key(id))
    {
        return Err("default provider instance does not exist".to_string());
    }
    macro_rules! validate_builtin_credential {
        ($field:ident) => {
            if let Some(provider) = &value.providers.$field {
                validate_provider_credential(
                    &provider.api_key,
                    provider.api_key_encrypted.as_deref(),
                    provider.credential_ref.as_ref(),
                    provider.api_key_from_env,
                )?;
            }
        };
    }
    validate_builtin_credential!(openai);
    validate_builtin_credential!(anthropic);
    validate_builtin_credential!(gemini);
    if let Some(provider) = &value.providers.bodhi {
        validate_provider_credential(
            &provider.api_key,
            provider.api_key_encrypted.as_deref(),
            provider.credential_ref.as_ref(),
            false,
        )?;
    }
    for instance in value.provider_instances.values() {
        validate_provider_credential(
            &instance.api_key,
            instance.api_key_encrypted.as_deref(),
            instance.credential_ref.as_ref(),
            false,
        )?;
    }
    let serialized = serde_json::to_value(value)
        .map_err(|_| "provider section cannot be serialized".to_string())?;
    validate_provider_urls_and_overrides(&serialized)
}

fn validate_provider_credential(
    plaintext: &str,
    ciphertext: Option<&str>,
    credential_ref: Option<&CredentialRef>,
    from_environment: bool,
) -> Result<(), String> {
    if ciphertext.is_some()
        || (!plaintext.trim().is_empty() && !from_environment && credential_ref.is_none())
    {
        return Err("provider secret is outside the credential store".to_string());
    }
    Ok(())
}

fn validate_mcp(value: &McpSection) -> Result<(), String> {
    use bamboo_domain::mcp_config::TransportConfig;

    let mut ids = BTreeSet::new();
    for server in &value.0.servers {
        if server.id.trim().is_empty() || !ids.insert(server.id.as_str()) {
            return Err("MCP server ids must be non-empty and unique".to_string());
        }
        if server.request_timeout_ms == 0 {
            return Err("MCP request timeout must be greater than zero".to_string());
        }
        if server.healthcheck_interval_ms == 0 {
            return Err("MCP health-check interval must be greater than zero".to_string());
        }
        match &server.transport {
            TransportConfig::Stdio(transport) => {
                if transport.command.trim().is_empty() {
                    return Err("MCP stdio command must not be empty".to_string());
                }
                if transport.startup_timeout_ms == 0 {
                    return Err("MCP startup timeout must be greater than zero".to_string());
                }
                if !transport.env.is_empty() || !transport.env_encrypted.is_empty() {
                    return Err(
                        "MCP environment credentials must use credential references".to_string()
                    );
                }
                for reference in transport.env_credential_refs.values() {
                    CredentialRef::parse(reference.clone())
                        .map_err(|_| "MCP credential reference is invalid".to_string())?;
                }
            }
            TransportConfig::Sse(transport) => {
                validate_secret_free_http_url("MCP SSE", &transport.url, false)?;
                if transport.connect_timeout_ms == 0 {
                    return Err("MCP connection timeout must be greater than zero".to_string());
                }
                validate_mcp_headers(&transport.headers)?;
            }
            TransportConfig::StreamableHttp(transport) => {
                validate_secret_free_http_url("MCP streamable HTTP", &transport.url, false)?;
                if transport.connect_timeout_ms == 0 {
                    return Err("MCP connection timeout must be greater than zero".to_string());
                }
                validate_mcp_headers(&transport.headers)?;
            }
        }
    }
    validate_json_serializable(value)
}

fn validate_secret_free_http_url(label: &str, raw: &str, allow_empty: bool) -> Result<(), String> {
    if raw.is_empty() && allow_empty {
        return Ok(());
    }
    if raw.trim().is_empty() || raw.trim() != raw {
        return Err(format!("{label} URL is empty or non-canonical"));
    }
    let parsed = url::Url::parse(raw).map_err(|_| format!("{label} URL is invalid"))?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
        return Err(format!("{label} URL must be an absolute HTTP(S) URL"));
    }
    if !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(format!(
            "{label} URL must not contain userinfo, query parameters, or a fragment"
        ));
    }
    Ok(())
}

fn validate_provider_urls_and_overrides(value: &Value) -> Result<(), String> {
    match value {
        Value::Object(object) => {
            if let Some(base_url) = object.get("base_url") {
                let base_url = base_url
                    .as_str()
                    .ok_or_else(|| "provider base URL must be a string".to_string())?;
                validate_secret_free_http_url("provider base", base_url, true)?;
            }
            for (key, child) in object {
                if key == "request_overrides" {
                    validate_request_overrides_credentials(child)?;
                } else {
                    validate_provider_urls_and_overrides(child)?;
                }
            }
        }
        Value::Array(values) => values
            .iter()
            .try_for_each(validate_provider_urls_and_overrides)?,
        _ => {}
    }
    Ok(())
}

fn validate_provider_override_credentials(value: &Value) -> Result<(), String> {
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                let normalized = key
                    .chars()
                    .filter(|ch| ch.is_ascii_alphanumeric())
                    .flat_map(char::to_lowercase)
                    .collect::<String>();
                if key == "request_overrides" {
                    validate_request_overrides_credentials(child)?;
                } else if normalized == "requestoverrides" {
                    return Err("provider request_overrides key is noncanonical".to_string());
                } else {
                    validate_provider_override_credentials(child)?;
                }
            }
        }
        Value::Array(values) => values
            .iter()
            .try_for_each(validate_provider_override_credentials)?,
        _ => {}
    }
    Ok(())
}

fn validate_request_overrides_credentials(value: &Value) -> Result<(), String> {
    let object = value
        .as_object()
        .ok_or_else(|| "provider request_overrides must be an object".to_string())?;
    if object
        .keys()
        .any(|key| !matches!(key.as_str(), "common" | "endpoints" | "rules"))
    {
        return Err("provider request_overrides contains an unknown field".to_string());
    }
    if let Some(common) = object.get("common") {
        validate_request_scope_override(common)?;
    }
    if let Some(endpoints) = object.get("endpoints") {
        let endpoints = endpoints
            .as_object()
            .ok_or_else(|| "provider override endpoints must be an object".to_string())?;
        for scope in endpoints.values() {
            validate_request_scope_override(scope)?;
        }
    }
    if let Some(rules) = object.get("rules") {
        let rules = rules
            .as_array()
            .ok_or_else(|| "provider override rules must be an array".to_string())?;
        for rule in rules {
            let rule = rule
                .as_object()
                .ok_or_else(|| "provider override rule must be an object".to_string())?;
            if rule
                .keys()
                .any(|key| !matches!(key.as_str(), "model_pattern" | "endpoint" | "scope"))
            {
                return Err("provider override rule contains an unknown field".to_string());
            }
            if let Some(scope) = rule.get("scope") {
                validate_request_scope_override(scope)?;
            }
        }
    }
    Ok(())
}

fn validate_request_scope_override(value: &Value) -> Result<(), String> {
    let object = value
        .as_object()
        .ok_or_else(|| "provider request override scope must be an object".to_string())?;
    if object
        .keys()
        .any(|key| !matches!(key.as_str(), "headers" | "body_patch"))
    {
        return Err("provider request override scope contains an unknown field".to_string());
    }
    if let Some(headers) = object.get("headers") {
        let headers = headers
            .as_object()
            .ok_or_else(|| "provider request override headers must be an object".to_string())?;
        for (name, expression) in headers {
            validate_template_expression_shape(expression)?;
            if is_credential_header(name) && !is_nonliteral_runtime_template(expression) {
                return Err(format!(
                    "provider request override {name} must not contain a literal credential"
                ));
            }
        }
    }
    if let Some(body_patches) = object.get("body_patch") {
        let body_patches = body_patches
            .as_array()
            .ok_or_else(|| "provider request body_patch must be an array".to_string())?;
        for patch in body_patches {
            validate_provider_body_patch(patch)?;
        }
    }
    Ok(())
}

fn validate_template_expression_shape(value: &Value) -> Result<(), String> {
    if value.is_string() {
        return Ok(());
    }
    let object = value
        .as_object()
        .ok_or_else(|| "provider request template must be a string or object".to_string())?;
    let type_name = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| "provider request template type is missing".to_string())?;
    let valid = match type_name {
        "literal" => {
            object
                .keys()
                .all(|key| matches!(key.as_str(), "type" | "value"))
                && object.get("value").is_some_and(Value::is_string)
        }
        "env_ref" => {
            object
                .keys()
                .all(|key| matches!(key.as_str(), "type" | "name" | "fallback"))
                && object.get("name").is_some_and(Value::is_string)
                && object
                    .get("fallback")
                    .is_none_or(|fallback| fallback.is_null() || fallback.is_string())
        }
        "generated" => {
            object
                .keys()
                .all(|key| matches!(key.as_str(), "type" | "generator"))
                && object.get("generator").is_some_and(Value::is_string)
        }
        "format" => {
            object
                .keys()
                .all(|key| matches!(key.as_str(), "type" | "template"))
                && object.get("template").is_some_and(Value::is_string)
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err("provider request template contains an unknown or invalid field".to_string())
    }
}

fn validate_provider_body_patch(value: &Value) -> Result<(), String> {
    let patch = value
        .as_object()
        .ok_or_else(|| "provider request body patch must be an object".to_string())?;
    if patch
        .keys()
        .any(|key| !matches!(key.as_str(), "path" | "op" | "value"))
    {
        return Err("provider request body patch contains an unknown field".to_string());
    }
    let path = patch
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| "provider request body patch path is missing".to_string())?;
    if let Some(op) = patch.get("op") {
        if !op.as_str().is_some_and(|op| matches!(op, "set" | "remove")) {
            return Err("provider request body patch operation is invalid".to_string());
        }
    }
    if !body_patch_targets_credential(path) {
        return Ok(());
    }
    let Some(value) = patch.get("value") else {
        return Ok(());
    };
    if !is_nonliteral_runtime_template(value) {
        return Err(format!(
            "provider request body patch {path} must not contain literal credential material"
        ));
    }
    Ok(())
}

fn body_patch_targets_credential(path: &str) -> bool {
    fn credential_token(segment: &str) -> bool {
        matches!(
            segment,
            "apikey"
                | "token"
                | "password"
                | "secret"
                | "appsecret"
                | "clientsecret"
                | "devicekey"
                | "privatekey"
                | "passphrase"
                | "authorization"
                | "cookie"
                | "accesskey"
                | "authkey"
                | "credential"
                | "credentials"
        ) || segment.ends_with("token")
            || segment.ends_with("password")
            || segment.ends_with("secret")
            || segment.ends_with("accesskey")
            || segment.ends_with("authkey")
            || segment.ends_with("credential")
            || segment.ends_with("credentials")
    }

    let decoded = path.replace("~1", "/").replace("~0", "~");
    let segments = decoded
        .split(['/', '.'])
        .map(|segment| {
            segment
                .chars()
                .filter(|ch| ch.is_ascii_alphanumeric())
                .flat_map(char::to_lowercase)
                .collect::<String>()
        })
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    segments.iter().any(|segment| credential_token(segment))
        || segments
            .windows(2)
            .any(|pair| credential_token(&format!("{}{}", pair[0], pair[1])))
}

fn is_nonliteral_runtime_template(value: &Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    match object.get("type").and_then(Value::as_str) {
        Some("env_ref") => {
            object
                .keys()
                .all(|key| matches!(key.as_str(), "type" | "name" | "fallback"))
                && object
                    .get("name")
                    .and_then(Value::as_str)
                    .is_some_and(valid_env_reference_name)
                && object.get("fallback").is_none_or(value_is_empty)
        }
        Some("generated") => {
            object
                .keys()
                .all(|key| matches!(key.as_str(), "type" | "generator"))
                && object
                    .get("generator")
                    .and_then(Value::as_str)
                    .is_some_and(|generator| matches!(generator, "uuid" | "unix_ms"))
        }
        Some("format") => {
            object
                .keys()
                .all(|key| matches!(key.as_str(), "type" | "template"))
                && object
                    .get("template")
                    .and_then(Value::as_str)
                    .is_some_and(runtime_format_has_no_secret_literal)
        }
        _ => false,
    }
}

fn valid_env_reference_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|ch| ch == '_' || ch.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn runtime_format_has_no_secret_literal(template: &str) -> bool {
    let mut rest = template;
    let mut literal = String::new();
    let mut found_runtime_value = false;
    while let Some(start) = rest.find('{') {
        literal.push_str(&rest[..start]);
        let Some(end) = rest[start + 1..].find('}') else {
            return false;
        };
        let expression = &rest[start + 1..start + 1 + end];
        let valid = expression
            .strip_prefix("env:")
            .is_some_and(valid_env_reference_name)
            || matches!(expression, "uuid" | "unix_ms");
        if !valid {
            return false;
        }
        found_runtime_value = true;
        rest = &rest[start + end + 2..];
    }
    literal.push_str(rest);
    if !literal.is_ascii()
        || literal.chars().any(|ch| {
            !ch.is_ascii_alphanumeric() && !matches!(ch, ' ' | '\t' | '-' | '_' | ':' | '/' | '=')
        })
    {
        return false;
    }
    let words = literal
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>();
    let safe_literal = if words.is_empty() {
        literal.trim().is_empty()
    } else {
        words.into_iter().all(|word| {
            matches!(
                word.to_ascii_lowercase().as_str(),
                "bearer" | "basic" | "token" | "apikey" | "key"
            )
        })
    };
    found_runtime_value && safe_literal
}

fn is_credential_header(name: &str) -> bool {
    let compact = name
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect::<String>();
    matches!(compact.as_str(), "cookie" | "setcookie" | "authentication")
        || compact.ends_with("authorization")
        || compact.ends_with("authkey")
        || compact.ends_with("auth")
        || compact.contains("apikey")
        || compact.ends_with("token")
        || compact.ends_with("sessiontoken")
        || compact == "secret"
        || compact.ends_with("secret")
        || compact == "password"
        || compact.ends_with("password")
        || compact.ends_with("accesskey")
        || compact.ends_with("credential")
        || compact.ends_with("credentials")
}

fn validate_mcp_headers(headers: &[bamboo_domain::mcp_config::HeaderConfig]) -> Result<(), String> {
    let mut names = BTreeSet::new();
    for header in headers {
        if header.name.trim().is_empty() || !names.insert(header.name.to_ascii_lowercase()) {
            return Err("MCP header names must be non-empty and unique".to_string());
        }
        if !header.value.trim().is_empty() || header.value_encrypted.is_some() {
            return Err("MCP header credentials must use credential references".to_string());
        }
        if let Some(reference) = &header.credential_ref {
            CredentialRef::parse(reference.clone())
                .map_err(|_| "MCP credential reference is invalid".to_string())?;
        }
    }
    Ok(())
}

fn validate_tools_skills(value: &ToolsSkillsSection) -> Result<(), String> {
    validate_json_serializable(value)
}

fn validate_memory(value: &MemorySection) -> Result<(), String> {
    validate_json_serializable(value)
}

fn validate_subagents(value: &SubagentsSection) -> Result<(), String> {
    if let Some(broker) = &value.external_broker {
        validate_external_broker_endpoint(&broker.endpoint)?;
        if !broker.token.trim().is_empty() || broker.token_encrypted.is_some() {
            return Err("external broker credential must use a credential reference".to_string());
        }
        if broker.configured != broker.credential_ref.is_some() {
            return Err("external broker configured metadata is inconsistent".to_string());
        }
    }
    validate_codex_subagents_config(&value.subagents)?;
    validate_json_serializable(value)
}

/// Cross-field validation for the Codex auth/provider matrix. Exported so the
/// root `/bamboo/config/validate` endpoint can return the same rules through its
/// structured field-error contract.
pub fn validate_codex_subagents_config(value: &SubagentsConfig) -> Result<(), String> {
    let codex_fields_present = value.codex_mode.is_some()
        || value.codex_auth_mode.is_some()
        || value.codex_base_url.is_some()
        || value.codex_wire_api.is_some()
        || value.codex_provider_key_ref.is_some()
        || value.codex_forward_env.is_some()
        || value.codex_sandbox.is_some()
        || value.codex_approval_policy.is_some()
        || value.codex_network_access.is_some()
        || value.codex_allow_danger_bypass.is_some();
    if value.executor.as_deref() != Some("codex") && !codex_fields_present {
        return Ok(());
    }
    let mode = value.codex_auth_mode.unwrap_or_default();
    let transport = value.codex_mode.unwrap_or_default();

    match (transport, value.codex_approval_policy) {
        (crate::CodexMode::Exec, Some(crate::CodexApprovalPolicy::OnRequest)) => {
            return Err(
                "codex_approval_policy on-request requires codex_mode = app_server; exec mode has no approval relay"
                    .to_string(),
            )
        }
        (crate::CodexMode::AppServer, None | Some(crate::CodexApprovalPolicy::OnRequest)) => {}
        (crate::CodexMode::AppServer, Some(_)) => {
            return Err(
                "codex_mode app_server requires codex_approval_policy = on-request"
                    .to_string(),
            )
        }
        (crate::CodexMode::Exec, _) => {}
    }

    let forwarded = value.codex_forward_env.as_deref().unwrap_or_default();
    let mut names = BTreeSet::new();
    for name in forwarded {
        let valid = name
            .chars()
            .next()
            .is_some_and(|first| first == '_' || first.is_ascii_alphabetic())
            && name
                .chars()
                .all(|character| character == '_' || character.is_ascii_alphanumeric());
        if !valid || !names.insert(name.as_str()) {
            return Err("codex_forward_env names must be valid and unique".to_string());
        }
        if name.starts_with("CODEX_") || name == "BAMBOO_CODEX_PROVIDER_KEY" {
            return Err("codex_forward_env may not override reserved Codex variables".to_string());
        }
    }
    let forwards_openai = names.contains("OPENAI_API_KEY");
    if mode == crate::CodexAuthMode::ApiKey && !forwards_openai {
        return Err("api_key auth requires OPENAI_API_KEY in codex_forward_env".to_string());
    }
    if mode != crate::CodexAuthMode::ApiKey && forwards_openai {
        return Err("OPENAI_API_KEY may only be forwarded in api_key auth mode".to_string());
    }

    if value.codex_network_access == Some(true)
        && value.codex_sandbox == Some(crate::CodexSandbox::ReadOnly)
    {
        return Err(
            "codex_network_access requires the workspace-write sandbox (or an unset sandbox)"
                .to_string(),
        );
    }
    if value.codex_sandbox == Some(crate::CodexSandbox::DangerFullAccess)
        && value.codex_allow_danger_bypass != Some(true)
    {
        return Err(
            "danger-full-access sandbox requires codex_allow_danger_bypass = true; parent bypass is still required at run time"
                .to_string(),
        );
    }

    match mode {
        crate::CodexAuthMode::Custom => {
            let raw = value
                .codex_base_url
                .as_deref()
                .map(str::trim)
                .filter(|raw| !raw.is_empty())
                .ok_or_else(|| "custom auth requires codex_base_url".to_string())?;
            let parsed = url::Url::parse(raw)
                .map_err(|_| "codex_base_url must be an absolute HTTP(S) URL".to_string())?;
            if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
                return Err("codex_base_url must be an absolute HTTP(S) URL".to_string());
            }
            if !parsed.username().is_empty()
                || parsed.password().is_some()
                || parsed.query().is_some()
                || parsed.fragment().is_some()
            {
                return Err(
                    "codex_base_url must not contain credentials, query parameters, or a fragment"
                        .to_string(),
                );
            }
            if value.codex_provider_key_ref.is_none() {
                return Err("custom auth requires codex_provider_key_ref".to_string());
            }
        }
        crate::CodexAuthMode::Inherit
        | crate::CodexAuthMode::ApiKey
        | crate::CodexAuthMode::Bamboo => {
            if value
                .codex_base_url
                .as_deref()
                .is_some_and(|url| !url.trim().is_empty())
            {
                return Err("codex_base_url is only valid in custom auth mode".to_string());
            }
            if value.codex_provider_key_ref.is_some() {
                return Err("codex_provider_key_ref is only valid in custom auth mode".to_string());
            }
        }
    }
    Ok(())
}

fn validate_external_broker_endpoint(raw: &str) -> Result<(), String> {
    if raw.trim().is_empty() || raw.trim() != raw {
        return Err("external broker endpoint is empty or non-canonical".to_string());
    }
    let endpoint =
        url::Url::parse(raw).map_err(|_| "external broker endpoint is invalid".to_string())?;
    if !matches!(endpoint.scheme(), "ws" | "wss") || endpoint.host_str().is_none() {
        return Err("external broker endpoint must be an absolute WebSocket URL".to_string());
    }
    if !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(
            "external broker endpoint must not contain credentials, query parameters, or a fragment"
                .to_string(),
        );
    }
    Ok(())
}

fn validate_notifications(value: &NotificationsSection) -> Result<(), String> {
    let ntfy = &value.notifications.ntfy;
    validate_secret_free_http_url("ntfy base", &ntfy.base_url, false)?;
    if ntfy.token.is_some()
        || ntfy.token_encrypted.is_some()
        || ntfy.configured != ntfy.credential_ref.is_some()
    {
        return Err("ntfy credential metadata is not isolated".to_string());
    }
    let bark = &value.notifications.bark;
    validate_secret_free_http_url("Bark base", &bark.base_url, false)?;
    if bark.device_key.is_some()
        || bark.device_key_encrypted.is_some()
        || bark.configured != bark.credential_ref.is_some()
    {
        return Err("Bark credential metadata is not isolated".to_string());
    }
    validate_json_serializable(value)
}

fn validate_connect(value: &ConnectSection) -> Result<(), String> {
    validate_connect_isolated(&value.0)?;
    validate_json_serializable(value)
}

pub(crate) fn validate_connect_isolated(value: &ConnectConfig) -> Result<(), String> {
    for platform in &value.platforms {
        let has_secret = platform.token.is_some()
            || platform.token_encrypted.is_some()
            || platform.app_secret.is_some()
            || platform.app_secret_encrypted.is_some();
        if has_secret {
            return Err("connect credentials must use credential references".to_string());
        }
        if platform.platform_type.trim().is_empty() {
            return Err("connect platform type must not be empty".to_string());
        }
        if let Some(domain) = platform
            .domain
            .as_deref()
            .filter(|domain| domain.contains("://"))
        {
            validate_secret_free_http_url("connect platform domain", domain, false)?;
        }
        if platform.token_configured != platform.token_credential_ref.is_some()
            || platform.app_secret_configured != platform.app_secret_credential_ref.is_some()
        {
            return Err("connect credential metadata is inconsistent".to_string());
        }
        if platform.token_credential_ref.is_some() || platform.app_secret_credential_ref.is_some() {
            let id = platform.id.as_deref().ok_or_else(|| {
                "connect credential references require a stable platform id".to_string()
            })?;
            let expected_token = crate::credential_ref("connect", id, "token")
                .map_err(|_| "connect platform id is invalid".to_string())?;
            let expected_app_secret = crate::credential_ref("connect", id, "app_secret")
                .map_err(|_| "connect platform id is invalid".to_string())?;
            if platform
                .token_credential_ref
                .as_ref()
                .is_some_and(|reference| reference != &expected_token)
                || platform
                    .app_secret_credential_ref
                    .as_ref()
                    .is_some_and(|reference| reference != &expected_app_secret)
            {
                return Err("connect credential reference is not canonical".to_string());
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_cluster_fabric(value: &ClusterFabricSection) -> Result<(), String> {
    use crate::cluster_fabric::{NodePlacement, SshAuth};

    let mut node_ids = BTreeSet::new();
    for node in &value.0.nodes {
        if node.id.trim().is_empty() || !node_ids.insert(node.id.as_str()) {
            return Err("cluster node ids must be non-empty and unique".to_string());
        }
        if let NodePlacement::Ssh(target) = &node.placement {
            match &target.auth {
                SshAuth::SystemSshConfig => {}
                SshAuth::Password {
                    password,
                    password_encrypted,
                } if password.trim().is_empty() && password_encrypted.is_none() => {}
                SshAuth::PrivateKey {
                    private_key,
                    private_key_encrypted,
                    passphrase,
                    passphrase_encrypted,
                    ..
                } if private_key.trim().is_empty()
                    && private_key_encrypted.is_none()
                    && passphrase.trim().is_empty()
                    && passphrase_encrypted.is_none() => {}
                _ => {
                    return Err("cluster SSH credentials must use credential references".to_string())
                }
            }
        }
    }
    for (node_id, refs) in &value.0.credential_refs {
        if !node_ids.contains(node_id.as_str()) {
            return Err("cluster credential metadata references an unknown node".to_string());
        }
        for (configured, reference) in [
            (
                refs.password_configured,
                refs.password_credential_ref.as_ref(),
            ),
            (
                refs.private_key_configured,
                refs.private_key_credential_ref.as_ref(),
            ),
            (
                refs.passphrase_configured,
                refs.passphrase_credential_ref.as_ref(),
            ),
        ] {
            if configured != reference.is_some() {
                return Err("cluster credential metadata is inconsistent".to_string());
            }
        }
    }
    let mut cluster_names = BTreeSet::new();
    for cluster in &value.0.clusters {
        if cluster.name.trim().is_empty() || !cluster_names.insert(cluster.name.as_str()) {
            return Err("cluster names must be non-empty and unique".to_string());
        }
        let mut members = BTreeSet::new();
        for node_id in &cluster.node_ids {
            if !node_ids.contains(node_id.as_str()) {
                return Err("cluster membership references an unknown node".to_string());
            }
            if !members.insert(node_id.as_str()) {
                return Err("cluster membership must not contain duplicate nodes".to_string());
            }
        }
    }
    validate_json_serializable(value)
}

fn validate_env(value: &EnvSection) -> Result<(), String> {
    let mut names = BTreeSet::new();
    for entry in &value.0 {
        let mut chars = entry.name.chars();
        let valid_name = chars
            .next()
            .is_some_and(|ch| ch == '_' || ch.is_ascii_alphabetic())
            && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric());
        if !valid_name || !names.insert(entry.name.as_str()) {
            return Err("environment variable names must be valid and unique".to_string());
        }
        if entry.value_encrypted.is_some() {
            return Err("environment credentials must not contain ciphertext".to_string());
        }
        if entry.secret {
            if !entry.value.is_empty() || entry.configured != entry.credential_ref.is_some() {
                return Err("secret environment variable metadata is not isolated".to_string());
            }
        } else if entry.credential_ref.is_some() || entry.configured != !entry.value.is_empty() {
            return Err("non-secret environment variable metadata is inconsistent".to_string());
        }
    }
    validate_json_serializable(value)
}

fn validate_access_control(value: &AccessControlSection) -> Result<(), String> {
    if let Some(access) = &value.0 {
        if access.password_hash.is_some()
            || access.password_salt.is_some()
            || access.password_configured != access.password_credential_ref.is_some()
        {
            return Err("access-control password verifier metadata is not isolated".to_string());
        }
        if access
            .password_credential_ref
            .as_ref()
            .is_some_and(|reference| match access_password_credential_ref() {
                Ok(expected) => reference != &expected,
                Err(_) => true,
            })
        {
            return Err("access-control password credential reference is invalid".to_string());
        }
        if access.devices.iter().any(|device| {
            device.device_id.trim().is_empty()
                || !device.token_hash.is_empty()
                || !device.token_salt.is_empty()
                || !device.token_configured
                || device
                    .token_credential_ref
                    .as_ref()
                    .is_none_or(
                        |reference| match access_device_credential_ref(&device.device_id) {
                            Ok(expected) => reference != &expected,
                            Err(_) => true,
                        },
                    )
        }) {
            return Err("access-control device verifier metadata is not isolated".to_string());
        }
    }
    validate_json_serializable(value)
}

fn validate_hooks(value: &HooksSection) -> Result<(), String> {
    if !matches!(
        value.request_hooks.image_fallback.mode.as_str(),
        "placeholder" | "error" | "ocr" | "vision"
    ) {
        return Err("image fallback mode is invalid".to_string());
    }
    validate_json_serializable(value)
}

fn validate_model_policy(value: &ModelPolicySection) -> Result<(), String> {
    value.keyword_masking.validate().map_err(|errors| {
        errors
            .into_iter()
            .map(|(index, error)| format!("keyword masking entry {index}: {error}"))
            .collect::<Vec<_>>()
            .join("; ")
    })?;
    validate_json_serializable(value)
}

fn validate_model_limits(value: &ModelLimitsSection) -> Result<(), String> {
    if value.0.iter().any(|entry| !entry.is_object()) {
        return Err("model limit entries must be objects".to_string());
    }
    validate_json_serializable(value)
}

pub(crate) fn validate_section_envelope(
    name: &str,
    bytes: &[u8],
    minimum_revision: u64,
) -> ConfigStoreResult<u64> {
    let envelope: SectionDocument<Value> = serde_json::from_slice(bytes)?;
    if envelope.schema_version != SECTION_SCHEMA_VERSION {
        return Err(crate::ConfigStoreError::Validation(
            "section document schema is unsupported".to_string(),
        ));
    }
    if envelope.revision < minimum_revision {
        return Err(crate::ConfigStoreError::Validation(
            "section document revision moved backwards during migration".to_string(),
        ));
    }
    validate_section_data(name, &envelope.data)?;
    Ok(envelope.revision)
}

fn validate_section_data(name: &str, data: &Value) -> ConfigStoreResult<()> {
    validate_ordinary_section_json(data)?;

    macro_rules! validate_typed {
        ($ty:ty, $validate:expr) => {{
            let value: $ty = serde_json::from_value(data.clone())?;
            $validate(&value).map_err(crate::ConfigStoreError::Validation)?;
        }};
    }
    match name {
        "core.json" => validate_typed!(CoreSection, validate_core),
        "providers.json" => validate_typed!(ProvidersSection, validate_providers),
        "mcp.json" => validate_typed!(McpSection, validate_mcp),
        "tools-skills.json" => validate_typed!(ToolsSkillsSection, validate_tools_skills),
        "memory.json" => validate_typed!(MemorySection, validate_memory),
        "subagents.json" => validate_typed!(SubagentsSection, validate_subagents),
        "notifications.json" => {
            validate_typed!(NotificationsSection, validate_notifications)
        }
        "connect.json" => validate_typed!(ConnectSection, validate_connect),
        "cluster-fabric.json" => {
            validate_typed!(ClusterFabricSection, validate_cluster_fabric)
        }
        "env.json" => validate_typed!(EnvSection, validate_env),
        "access-control.json" => {
            validate_typed!(AccessControlSection, validate_access_control)
        }
        "hooks.json" => validate_typed!(HooksSection, validate_hooks),
        "model-policy.json" => validate_typed!(ModelPolicySection, validate_model_policy),
        "model_limits.json" => validate_typed!(ModelLimitsSection, validate_model_limits),
        _ => {
            return Err(crate::ConfigStoreError::Validation(
                "migration section target is invalid".to_string(),
            ))
        }
    }
    Ok(())
}

fn read_optional_bytes(path: &Path) -> ConfigStoreResult<Vec<u8>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(error.into()),
    }
}

fn file_hash(path: &Path) -> ConfigStoreResult<String> {
    let mut digest = Sha256::new();
    match read_existing_bytes(path)? {
        Some(bytes) => {
            digest.update([1]);
            digest.update(bytes);
        }
        None => digest.update([0]),
    }
    Ok(hex::encode(digest.finalize()))
}

pub(crate) fn migration_source_hashes(data_dir: &Path) -> ConfigStoreResult<Vec<(String, String)>> {
    migration_source_names()
        .into_iter()
        .map(|name| Ok((name.clone(), file_hash(&data_dir.join(name))?)))
        .collect()
}

fn migration_source_names() -> BTreeSet<String> {
    let mut names = BTreeSet::from([
        "config.json".to_string(),
        "broker.json".to_string(),
        "connect.json".to_string(),
    ]);
    names.extend(
        SECTION_DESCRIPTORS
            .iter()
            .map(|descriptor| descriptor.file_name.to_string()),
    );
    names.insert(SECTION_LAYOUT_FILE.to_string());
    names
}

pub(crate) fn preflight_source_hashes(data_dir: &Path) -> ConfigStoreResult<Vec<(String, String)>> {
    preflight_source_names()
        .into_iter()
        .map(|name| Ok((name.clone(), file_hash(&data_dir.join(name))?)))
        .collect()
}

pub(crate) fn preflight_source_names() -> BTreeSet<String> {
    let mut names = migration_source_names();
    for base in [
        "config.json",
        "providers.json",
        "mcp.json",
        "broker.json",
        "connect.json",
        "access-control.json",
        "credentials.json",
    ] {
        for suffix in [".bak", ".bak.1", ".bak.2"] {
            names.insert(format!("{base}{suffix}"));
        }
    }
    for name in [
        "config-credential-migration.json",
        "config-credential-migration.journal.json",
        crate::credential_migration::SECTION_LAYOUT_COMPLETION_FILE,
    ] {
        names.insert(name.to_string());
    }
    names
}

struct StrictPlanningInput {
    config: Config,
    root_raw: Value,
    model_limits: ModelLimitsSection,
    broker: Option<(BrokerClientConfig, Value)>,
    authoritative_sidecars: BTreeSet<SectionId>,
}

type StrictSourceOverrides = BTreeMap<String, Vec<u8>>;

fn read_existing_bytes(path: &Path) -> ConfigStoreResult<Option<Vec<u8>>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn read_planning_source(
    data_dir: &Path,
    name: &str,
    overrides: Option<&StrictSourceOverrides>,
) -> ConfigStoreResult<Option<Vec<u8>>> {
    if let Some(bytes) = overrides.and_then(|overrides| overrides.get(name)) {
        return Ok(Some(bytes.clone()));
    }
    read_existing_bytes(&data_dir.join(name))
}

fn load_strict_root(
    data_dir: &Path,
    overrides: Option<&StrictSourceOverrides>,
) -> ConfigStoreResult<(Config, Value)> {
    let raw = match read_planning_source(data_dir, "config.json", overrides)? {
        Some(bytes) => {
            let value: Value = serde_json::from_slice(&bytes)?;
            if !value.is_object() {
                return Err(crate::ConfigStoreError::Validation(
                    "legacy configuration must be an object".to_string(),
                ));
            }
            value
        }
        None => Value::Object(Map::new()),
    };
    let mut effective = raw.clone();
    let effective_root = effective
        .as_object_mut()
        .expect("strict root shape was checked above");
    if read_planning_source(data_dir, "memory.json", overrides)?.is_some() {
        effective_root.remove("memory");
    }
    if read_planning_source(data_dir, "subagents.json", overrides)?.is_some() {
        effective_root.remove("subagents");
    }
    if let Some(provider_sidecar) =
        load_strict_sidecar_value_from(data_dir, "providers.json", overrides)?
    {
        let provider_sidecar = provider_sidecar.as_object().ok_or_else(|| {
            crate::ConfigStoreError::Validation("providers.json must contain an object".to_string())
        })?;
        for field in [
            "provider",
            "defaults",
            "provider_instances",
            "default_provider_instance",
            "features",
        ] {
            if provider_sidecar.contains_key(field) {
                effective_root.remove(field);
            }
        }
        if let Some(root_providers) = effective_root
            .get_mut("providers")
            .and_then(Value::as_object_mut)
        {
            for provider in ["openai", "anthropic", "gemini", "bodhi"] {
                if provider_sidecar.contains_key(provider) {
                    root_providers.remove(provider);
                }
            }
        }
    }
    if read_planning_source(data_dir, "mcp.json", overrides)?.is_some() {
        effective_root.remove("mcp");
        effective_root.remove("mcpServers");
    }
    if read_planning_source(data_dir, "connect.json", overrides)?.is_some() {
        effective_root.remove("connect");
    }
    if let Some(access) = effective_root.get_mut("access_control") {
        // Strict root decoding must use the same secret-free repair projection
        // as the later compound planner. Otherwise one malformed verifier
        // disables every typed section before AccessControl can be isolated.
        scrub_preflight_access_control_secrets(access)?;
    }
    let config = serde_json::from_value::<Config>(effective).map_err(|error| {
        crate::ConfigStoreError::Validation(format!(
            "legacy configuration failed strict decoding: {error}"
        ))
    })?;
    Ok((config, raw))
}

fn load_strict_sidecar_value(data_dir: &Path, name: &str) -> ConfigStoreResult<Option<Value>> {
    load_strict_sidecar_value_from(data_dir, name, None)
}

fn load_strict_sidecar_value_from(
    data_dir: &Path,
    name: &str,
    overrides: Option<&StrictSourceOverrides>,
) -> ConfigStoreResult<Option<Value>> {
    let Some(bytes) = read_planning_source(data_dir, name, overrides)? else {
        return Ok(None);
    };
    if bytes.is_empty() {
        return Err(crate::ConfigStoreError::Validation(format!(
            "{name} is empty"
        )));
    }
    compatible_section_data(&bytes).and_then(|(_, data)| {
        data.map(Some)
            .ok_or_else(|| crate::ConfigStoreError::Validation(format!("{name} is empty")))
    })
}

fn decode_strict_sidecar<T>(
    data_dir: &Path,
    name: &str,
    overrides: Option<&StrictSourceOverrides>,
) -> ConfigStoreResult<Option<(T, Value)>>
where
    T: serde::de::DeserializeOwned,
{
    load_strict_sidecar_value_from(data_dir, name, overrides)?
        .map(|raw| {
            let typed = serde_json::from_value(raw.clone()).map_err(|error| {
                crate::ConfigStoreError::Validation(format!(
                    "{name} failed strict decoding: {error}"
                ))
            })?;
            Ok((typed, raw))
        })
        .transpose()
}

fn scrub_preflight_provider_secrets(raw: &mut Value) {
    let Some(object) = raw.as_object_mut() else {
        return;
    };
    for provider in ["openai", "anthropic", "gemini", "bodhi"] {
        if let Some(config) = object.get_mut(provider).and_then(Value::as_object_mut) {
            if !config
                .get("api_key_from_env")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                config.remove("api_key");
            }
            config.remove("api_key_encrypted");
        }
    }
    if let Some(instances) = object
        .get_mut("provider_instances")
        .and_then(Value::as_object_mut)
    {
        for instance in instances.values_mut().filter_map(Value::as_object_mut) {
            instance.remove("api_key");
            instance.remove("api_key_encrypted");
        }
    }
}

fn validate_preflight_provider_secret_consistency(raw: &Value) -> ConfigStoreResult<()> {
    fn check(config: &serde_json::Map<String, Value>, label: &str) -> ConfigStoreResult<()> {
        let plaintext = config
            .get("api_key")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty());
        let ciphertext = config
            .get("api_key_encrypted")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty());
        if let (Some(plaintext), Some(ciphertext)) = (plaintext, ciphertext) {
            let decrypted = crate::encryption::decrypt(ciphertext).map_err(|_| {
                crate::ConfigStoreError::Validation(format!("{label} ciphertext is invalid"))
            })?;
            if decrypted != plaintext {
                return Err(crate::ConfigStoreError::Validation(format!(
                    "{label} plaintext and ciphertext disagree"
                )));
            }
        }
        Ok(())
    }

    let Some(object) = raw.as_object() else {
        return Ok(());
    };
    for provider in ["openai", "anthropic", "gemini", "bodhi"] {
        if let Some(config) = object.get(provider).and_then(Value::as_object) {
            check(config, &format!("provider '{provider}' API key"))?;
        }
    }
    if let Some(instances) = object.get("provider_instances").and_then(Value::as_object) {
        for (id, instance) in instances {
            if let Some(instance) = instance.as_object() {
                check(instance, &format!("provider instance '{id}' API key"))?;
            }
        }
    }
    Ok(())
}

fn scrub_preflight_mcp_secrets(raw: &mut Value) {
    fn scrub_transport(value: &mut Value) {
        let Some(transport) = value.as_object_mut() else {
            return;
        };
        for key in ["env", "env_encrypted", "headers", "headers_encrypted"] {
            transport.remove(key);
        }
        if let Some(nested) = transport.get_mut("transport") {
            scrub_transport(nested);
        }
    }

    if let Some(servers) = raw.get_mut("servers").and_then(Value::as_array_mut) {
        for server in servers {
            scrub_transport(server);
        }
    } else if let Some(servers) = raw.as_object_mut() {
        for server in servers.values_mut() {
            scrub_transport(server);
        }
    }
}

fn scrub_preflight_notification_secrets(raw: &mut Value) {
    let notifications = if raw.get("notifications").is_some() {
        raw.get_mut("notifications").and_then(Value::as_object_mut)
    } else {
        raw.as_object_mut()
    };
    let Some(notifications) = notifications else {
        return;
    };
    for (channel, secret_keys) in [
        ("ntfy", ["token", "token_encrypted"]),
        ("bark", ["device_key", "device_key_encrypted"]),
    ] {
        let Some(channel) = notifications
            .get_mut(channel)
            .and_then(Value::as_object_mut)
        else {
            continue;
        };
        if secret_keys.iter().any(|key| channel.contains_key(*key)) {
            for key in secret_keys {
                channel.remove(key);
            }
            channel.remove("credential_ref");
            channel.insert("configured".to_string(), Value::Bool(false));
        }
    }
}

fn scrub_preflight_config_secrets(config: &Config) -> ConfigStoreResult<Config> {
    let mut sanitized = config.clone();
    sanitized.proxy_auth = None;
    sanitized.proxy_auth_encrypted = None;
    for instance in sanitized.provider_instances.values_mut() {
        instance.api_key.clear();
        instance.api_key_encrypted = None;
    }
    macro_rules! clear_provider {
        ($field:ident) => {
            if let Some(provider) = sanitized.providers_mut().$field.as_mut() {
                provider.api_key.clear();
                provider.api_key_encrypted = None;
            }
        };
    }
    clear_provider!(openai);
    clear_provider!(anthropic);
    clear_provider!(gemini);
    clear_provider!(bodhi);
    sanitized.sanitize_mcp_credential_refs_for_disk();
    sanitized.sanitize_env_vars_for_disk();
    sanitized.sanitize_notifications_for_disk();
    sanitized.sanitize_cluster_fabric_for_disk();
    let mut raw = serde_json::to_value(&sanitized)?;
    let object = raw.as_object_mut().ok_or_else(|| {
        crate::ConfigStoreError::Validation("configuration must serialize as an object".to_string())
    })?;
    for key in [
        "proxy_auth",
        "proxy_auth_encrypted",
        "http_proxy_auth_encrypted",
        "https_proxy_auth_encrypted",
    ] {
        object.remove(key);
    }
    if let Some(providers) = object.get_mut("providers") {
        scrub_preflight_provider_secrets(providers);
    }
    if let Some(instances) = object
        .get_mut("provider_instances")
        .and_then(Value::as_object_mut)
    {
        for instance in instances.values_mut().filter_map(Value::as_object_mut) {
            instance.remove("api_key");
            instance.remove("api_key_encrypted");
        }
    }
    if let Some(mcp) = object.get_mut("mcp") {
        scrub_preflight_mcp_secrets(mcp);
    } else if let Some(mcp) = object.get_mut("mcpServers") {
        scrub_preflight_mcp_secrets(mcp);
    }
    if let Some(notifications) = object.get_mut("notifications") {
        scrub_preflight_notification_secrets(notifications);
    }
    if let Some(env) = object.get_mut("env_vars") {
        scrub_preflight_env_secrets(env);
    }
    if let Some(cluster) = object.get_mut("cluster_fabric") {
        scrub_preflight_cluster_secrets(cluster);
    }
    serde_json::from_value(raw).map_err(crate::ConfigStoreError::Json)
}

fn scrub_preflight_env_secrets(raw: &mut Value) {
    let Some(entries) = raw.as_array_mut() else {
        return;
    };
    for entry in entries.iter_mut().filter_map(Value::as_object_mut) {
        if entry.get("secret").and_then(Value::as_bool) == Some(true) {
            entry.insert("value".to_string(), Value::String(String::new()));
            entry.remove("value_encrypted");
            entry.remove("credential_ref");
            entry.insert("configured".to_string(), Value::Bool(false));
        }
    }
}

fn scrub_preflight_cluster_secrets(raw: &mut Value) {
    let Some(nodes) = raw.get_mut("nodes").and_then(Value::as_array_mut) else {
        return;
    };
    for auth in nodes
        .iter_mut()
        .filter_map(|node| node.get_mut("placement")?.get_mut("auth")?.as_object_mut())
    {
        for key in [
            "password",
            "password_encrypted",
            "private_key",
            "private_key_encrypted",
            "passphrase",
            "passphrase_encrypted",
        ] {
            auth.remove(key);
        }
    }
}

fn scrub_preflight_connect_secrets(raw: &mut Value) {
    let Some(platforms) = raw.get_mut("platforms").and_then(Value::as_array_mut) else {
        return;
    };
    for platform in platforms.iter_mut().filter_map(Value::as_object_mut) {
        for key in [
            "token",
            "token_encrypted",
            "app_secret",
            "app_secret_encrypted",
        ] {
            platform.remove(key);
        }
    }
}

/// Mirror the credential planner's AccessControl extraction in the zero-write
/// facade preflight by invoking the same pure sanitizer against a clone. The
/// returned extraction plan is intentionally discarded: durable writes remain
/// owned by the migration transaction after every preflight succeeds.
fn scrub_preflight_access_control_secrets(raw: &mut Value) -> ConfigStoreResult<()> {
    crate::credential_migration::sanitize_access_control_for_preflight(raw)
}

fn preflight_legacy_root_sections(
    data_dir: &Path,
    input: &StrictPlanningInput,
) -> ConfigStoreResult<()> {
    let sanitized_config = scrub_preflight_config_secrets(&input.config)?;
    let projection = SectionProjection::from_config(&sanitized_config, input.model_limits.clone())?;
    let mut sections =
        project_legacy_raw_sections(&input.root_raw, None, &input.authoritative_sidecars)?;
    if let Some(providers) = sections.get(&SectionId::Providers) {
        validate_preflight_provider_secret_consistency(providers)?;
    }
    if let Some(providers) = load_strict_sidecar_value(data_dir, "providers.json")? {
        validate_preflight_provider_secret_consistency(&providers)?;
    }
    let instance_native_providers = projection.providers.default_provider_instance.is_some();
    for descriptor in SECTION_DESCRIPTORS
        .iter()
        .filter(|descriptor| descriptor.id != SectionId::Credentials)
    {
        let id = descriptor.id;
        let typed_baseline = match id {
            SectionId::Core => serde_json::to_value(&projection.core)?,
            SectionId::Providers => serde_json::to_value(&projection.providers)?,
            SectionId::Mcp => serde_json::to_value(&projection.mcp)?,
            SectionId::ToolsSkills => serde_json::to_value(&projection.tools_skills)?,
            SectionId::Memory => serde_json::to_value(&projection.memory)?,
            SectionId::Subagents => serde_json::to_value(&projection.subagents)?,
            SectionId::Notifications => serde_json::to_value(&projection.notifications)?,
            SectionId::Connect => serde_json::to_value(&projection.connect)?,
            SectionId::ClusterFabric => serde_json::to_value(&projection.cluster_fabric)?,
            SectionId::Env => serde_json::to_value(&projection.env)?,
            SectionId::AccessControl => serde_json::to_value(&projection.access_control)?,
            SectionId::Hooks => serde_json::to_value(&projection.hooks)?,
            SectionId::ModelPolicy => serde_json::to_value(&projection.model_policy)?,
            SectionId::ModelLimits => serde_json::to_value(&projection.model_limits)?,
            SectionId::Credentials => continue,
        };
        let mut data = match sections.remove(&id) {
            Some(mut legacy) => {
                if id == SectionId::Providers && instance_native_providers {
                    remove_instance_native_legacy_provider_fields(&mut legacy);
                }
                deep_merge_typed_over_raw(&mut legacy, typed_baseline);
                legacy
            }
            None => typed_baseline,
        };
        if let Some(mut existing) = load_strict_sidecar_value(data_dir, id.descriptor().file_name)?
        {
            if id == SectionId::Providers && instance_native_providers {
                remove_instance_native_legacy_provider_fields(&mut existing);
            }
            deep_merge_json(&mut data, existing);
        }
        if id == SectionId::Providers && instance_native_providers {
            remove_instance_native_legacy_provider_fields(&mut data);
        }
        match id {
            SectionId::Core => {
                if let Some(object) = data.as_object_mut() {
                    for key in [
                        "proxy_auth",
                        "proxy_auth_encrypted",
                        "http_proxy_auth_encrypted",
                        "https_proxy_auth_encrypted",
                    ] {
                        object.remove(key);
                    }
                }
            }
            SectionId::Providers => scrub_preflight_provider_secrets(&mut data),
            SectionId::Mcp => scrub_preflight_mcp_secrets(&mut data),
            SectionId::Notifications => scrub_preflight_notification_secrets(&mut data),
            SectionId::Env => scrub_preflight_env_secrets(&mut data),
            SectionId::ClusterFabric => scrub_preflight_cluster_secrets(&mut data),
            SectionId::Connect => scrub_preflight_connect_secrets(&mut data),
            SectionId::AccessControl => scrub_preflight_access_control_secrets(&mut data)?,
            SectionId::Credentials => unreachable!("credentials were skipped above"),
            _ => {}
        }
        validate_section_data(id.descriptor().file_name, &data)?;
    }
    Ok(())
}

fn load_strict_broker(data_dir: &Path) -> ConfigStoreResult<Option<(BrokerClientConfig, Value)>> {
    load_strict_broker_from(data_dir, None)
}

fn load_strict_broker_from(
    data_dir: &Path,
    overrides: Option<&StrictSourceOverrides>,
) -> ConfigStoreResult<Option<(BrokerClientConfig, Value)>> {
    let Some(bytes) = read_planning_source(data_dir, "broker.json", overrides)? else {
        return Ok(None);
    };
    let Ok(document) = serde_json::from_slice(&bytes) else {
        return Ok(None);
    };
    let Ok(raw) = unwrapped_document_data(document) else {
        return Ok(None);
    };
    let Ok(typed) = serde_json::from_value::<BrokerClientConfig>(raw.clone()) else {
        return Ok(None);
    };
    if validate_external_broker_endpoint(&typed.endpoint).is_err()
        || !typed.token.trim().is_empty()
        || typed.token_encrypted.is_some()
        || !broker_raw_is_safe_after_known_secret_removal(&raw, &typed)
    {
        return Ok(None);
    }
    Ok(Some((typed, raw)))
}

fn broker_raw_is_safe_after_known_secret_removal(raw: &Value, typed: &BrokerClientConfig) -> bool {
    let mut raw = raw.clone();
    let Some(object) = raw.as_object_mut() else {
        return false;
    };
    let had_legacy_secret = object.contains_key("token") || object.contains_key("token_encrypted");
    object.remove("token");
    object.remove("token_encrypted");
    if had_legacy_secret {
        object.remove("credential_ref");
        object.insert("configured".to_string(), Value::Bool(false));
    }
    let candidate = Value::Object(Map::from_iter([("external_broker".to_string(), raw)]));
    if validate_ordinary_section_json(&candidate).is_err() {
        return false;
    }
    let mut typed = typed.clone();
    typed.token.clear();
    typed.token_encrypted = None;
    if had_legacy_secret {
        typed.credential_ref = None;
        typed.configured = false;
    }
    validate_subagents(&SubagentsSection {
        subagents: SubagentsConfig::default(),
        external_broker: Some(typed),
    })
    .is_ok()
}

/// Pure read-only loader for migration planning. It intentionally bypasses
/// Config's recovery/salvage/hydration loader and applies legacy sidecar
/// precedence only after every source document has decoded successfully.
fn load_strict_planning_input(data_dir: &Path) -> ConfigStoreResult<StrictPlanningInput> {
    load_strict_planning_input_with_overrides(data_dir, None)
}

fn load_strict_planning_input_with_overrides(
    data_dir: &Path,
    overrides: Option<&StrictSourceOverrides>,
) -> ConfigStoreResult<StrictPlanningInput> {
    let (mut config, root_raw) = load_strict_root(data_dir, overrides)?;
    let mut authoritative_sidecars = BTreeSet::new();

    if let Some((memory, _)) =
        decode_strict_sidecar::<MemorySection>(data_dir, "memory.json", overrides)?
    {
        *config.memory_mut() = memory.0;
    }
    if let Some((subagents, _)) =
        decode_strict_sidecar::<SubagentsSection>(data_dir, "subagents.json", overrides)?
    {
        let SubagentsSection {
            mut subagents,
            external_broker,
        } = subagents;
        if external_broker.is_some() {
            subagents.broker = external_broker;
        }
        *config.subagents_mut() = subagents;
    }
    if let Some((providers, raw)) =
        decode_strict_sidecar::<ProvidersSection>(data_dir, "providers.json", overrides)?
    {
        let raw = raw.as_object().ok_or_else(|| {
            crate::ConfigStoreError::Validation("providers.json must contain an object".to_string())
        })?;
        let owns_provider = raw.contains_key("provider");
        let owns_defaults = raw.contains_key("defaults");
        let owns_instances = raw.contains_key("provider_instances");
        let owns_default_instance = raw.contains_key("default_provider_instance");
        let owns_features = raw.contains_key("features");
        let ProvidersSection {
            provider,
            defaults,
            provider_instances,
            default_provider_instance,
            features,
            providers,
        } = providers;
        if owns_provider {
            config.provider = provider.unwrap_or_else(default_provider_name);
        }
        if owns_defaults {
            config.defaults = defaults;
        }
        if owns_instances {
            config.provider_instances = provider_instances;
        }
        if owns_default_instance {
            config.default_provider_instance = default_provider_instance;
        }
        if owns_features {
            config.features = features;
        }
        *config.providers_mut() = providers;
    }
    if let Some((mcp, _)) = decode_strict_sidecar::<McpSection>(data_dir, "mcp.json", overrides)? {
        authoritative_sidecars.insert(SectionId::Mcp);
        config.mcp = mcp.0;
    }
    if let Some((connect, _)) =
        decode_strict_sidecar::<ConnectSection>(data_dir, "connect.json", overrides)?
    {
        config.connect = connect.0;
    }

    let model_limits = match decode_strict_sidecar::<ModelLimitsSection>(
        data_dir,
        "model_limits.json",
        overrides,
    )? {
        Some((limits, _)) => limits,
        None => root_raw
            .get("model_limits")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| {
                crate::ConfigStoreError::Validation(format!(
                    "inline model_limits failed strict decoding: {error}"
                ))
            })?
            .unwrap_or_default(),
    };

    Ok(StrictPlanningInput {
        config,
        root_raw,
        model_limits,
        broker: load_strict_broker_from(data_dir, overrides)?,
        authoritative_sidecars,
    })
}

fn project_legacy_raw_sections(
    root_raw: &Value,
    broker_raw: Option<Value>,
    authoritative_sidecars: &BTreeSet<SectionId>,
) -> ConfigStoreResult<BTreeMap<SectionId, Value>> {
    let root = root_raw.as_object().ok_or_else(|| {
        crate::ConfigStoreError::Validation("legacy configuration must be an object".to_string())
    })?;
    let mut sections = BTreeMap::<SectionId, Value>::new();
    let mut core = Map::new();
    let mut providers = Map::new();
    let mut tools_skills = Map::new();
    let mut subagents = Map::new();
    let mut notifications = Map::new();
    let mut hooks = Map::new();
    let mut model_policy = Map::new();

    for (key, value) in root {
        match key.as_str() {
            "http_proxy"
            | "https_proxy"
            | "proxy_auth"
            | "proxy_auth_encrypted"
            | "proxy_auth_credential_ref"
            | "headless_auth"
            | "server"
            | "default_work_area"
            | "run_budget"
            | "stream_timeout" => {
                core.insert(key.clone(), value.clone());
            }
            "provider"
            | "defaults"
            | "provider_instances"
            | "default_provider_instance"
            | "features" => {
                providers.insert(key.clone(), value.clone());
            }
            "providers" => {
                let object = value.as_object().ok_or_else(|| {
                    crate::ConfigStoreError::Validation(
                        "legacy providers configuration must be an object".to_string(),
                    )
                })?;
                providers.extend(object.clone());
            }
            "mcpServers" | "mcp" => {
                sections.insert(SectionId::Mcp, value.clone());
            }
            "tools" | "skills" | "plugin_trust" => {
                tools_skills.insert(key.clone(), value.clone());
            }
            "memory" => {
                sections.insert(SectionId::Memory, value.clone());
            }
            "subagents" => {
                let object = value.as_object().ok_or_else(|| {
                    crate::ConfigStoreError::Validation(
                        "legacy subagents configuration must be an object".to_string(),
                    )
                })?;
                subagents.extend(object.clone());
            }
            "notifications" => {
                notifications.insert("notifications".to_string(), value.clone());
            }
            "connect" => {
                sections.insert(SectionId::Connect, value.clone());
            }
            "cluster_fabric" => {
                sections.insert(SectionId::ClusterFabric, value.clone());
            }
            "env_vars" => {
                sections.insert(SectionId::Env, value.clone());
            }
            "access_control" => {
                sections.insert(SectionId::AccessControl, value.clone());
            }
            "hooks" => {
                let object = value.as_object().ok_or_else(|| {
                    crate::ConfigStoreError::Validation(
                        "legacy hooks configuration must be an object".to_string(),
                    )
                })?;
                hooks.extend(object.clone());
            }
            "lifecycle_hooks" => {
                hooks.insert(key.clone(), value.clone());
            }
            "keyword_masking" | "anthropic_model_mapping" | "gemini_model_mapping" => {
                model_policy.insert(key.clone(), value.clone());
            }
            "model_limits" => {
                sections.insert(SectionId::ModelLimits, value.clone());
            }
            _ => {
                core.insert(key.clone(), value.clone());
            }
        }
    }

    let external_broker_raw = broker_raw.clone();
    if let Some(raw) = broker_raw {
        subagents.insert("external_broker".to_string(), raw);
    }
    for (id, object) in [
        (SectionId::Core, core),
        (SectionId::Providers, providers),
        (SectionId::ToolsSkills, tools_skills),
        (SectionId::Subagents, subagents),
        (SectionId::Notifications, notifications),
        (SectionId::Hooks, hooks),
        (SectionId::ModelPolicy, model_policy),
    ] {
        if !object.is_empty() {
            sections.insert(id, Value::Object(object));
        }
    }
    for id in authoritative_sidecars {
        sections.remove(id);
    }
    if authoritative_sidecars.contains(&SectionId::Subagents) {
        if let Some(raw) = external_broker_raw {
            sections.insert(
                SectionId::Subagents,
                Value::Object(Map::from_iter([("external_broker".to_string(), raw)])),
            );
        }
    }
    Ok(sections)
}

fn unwrapped_document_data(value: Value) -> ConfigStoreResult<Value> {
    let Some(object) = value.as_object() else {
        return Ok(value);
    };
    let has_marker = object.contains_key("schema_version")
        || object.contains_key("revision")
        || object.contains_key("data");
    if !has_marker {
        return Ok(value);
    }
    if object.get("schema_version").and_then(Value::as_u64)
        != Some(u64::from(SECTION_SCHEMA_VERSION))
        || object.get("revision").and_then(Value::as_u64).is_none()
    {
        return Err(crate::ConfigStoreError::Validation(
            "configuration revision envelope is invalid".to_string(),
        ));
    }
    object.get("data").cloned().ok_or_else(|| {
        crate::ConfigStoreError::Validation(
            "configuration revision envelope is invalid".to_string(),
        )
    })
}

fn preflight_broker_document(data_dir: &Path) -> ConfigStoreResult<bool> {
    let bytes = read_optional_bytes(&data_dir.join("broker.json"))?;
    if bytes.is_empty() {
        return Ok(false);
    }
    let Ok(document) = serde_json::from_slice(&bytes) else {
        return Ok(false);
    };
    let Ok(raw) = unwrapped_document_data(document) else {
        return Ok(false);
    };
    let Ok(broker) = serde_json::from_value::<BrokerClientConfig>(raw.clone()) else {
        return Ok(false);
    };
    Ok(validate_external_broker_endpoint(&broker.endpoint).is_ok()
        && broker_raw_is_safe_after_known_secret_removal(&raw, &broker))
}

#[cfg(test)]
type FacadePreflightTestHook = Box<dyn FnOnce() + Send + 'static>;

#[cfg(test)]
fn facade_preflight_test_hook() -> &'static Mutex<HashMap<PathBuf, FacadePreflightTestHook>> {
    static HOOK: std::sync::OnceLock<Mutex<HashMap<PathBuf, FacadePreflightTestHook>>> =
        std::sync::OnceLock::new();
    HOOK.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
fn set_facade_preflight_test_hook(data_dir: &Path, hook: impl FnOnce() + Send + 'static) {
    facade_preflight_test_hook()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(data_dir.to_path_buf(), Box::new(hook));
}

#[cfg(test)]
fn run_facade_preflight_test_hook(data_dir: &Path) {
    if let Some(hook) = facade_preflight_test_hook()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(data_dir)
    {
        hook();
    }
}

#[cfg(test)]
type FacadeEpochTestHook = Box<dyn FnOnce() + Send + 'static>;

#[cfg(test)]
fn facade_epoch_test_hook() -> &'static Mutex<HashMap<PathBuf, FacadeEpochTestHook>> {
    static HOOK: std::sync::OnceLock<Mutex<HashMap<PathBuf, FacadeEpochTestHook>>> =
        std::sync::OnceLock::new();
    HOOK.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
fn set_facade_epoch_test_hook(data_dir: &Path, hook: impl FnOnce() + Send + 'static) {
    facade_epoch_test_hook()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(data_dir.to_path_buf(), Box::new(hook));
}

#[cfg(test)]
fn run_facade_epoch_test_hook(data_dir: &Path) {
    if let Some(hook) = facade_epoch_test_hook()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(data_dir)
    {
        hook();
    }
}

/// Idempotently split the compatibility documents through the existing
/// credential migration lock/journal/manifest. Security-sensitive preflights
/// run after recovery-only handling and before any new migration may write.
pub fn migrate_config_facade_layout(
    data_dir: impl AsRef<Path>,
) -> ConfigStoreResult<crate::SectionMigrationOutcome> {
    let data_dir = data_dir.as_ref();
    let mut recovered = false;
    for _ in 0..4 {
        recovered |= crate::recover_pending_config_transaction(data_dir)?;
        if section_layout_is_active(data_dir)?
            && crate::credential_migration::access_control_requires_opaque_repair(data_dir)?
        {
            let access = crate::credential_migration::with_migration_lock(data_dir, || {
                crate::credential_migration::migrate_access_control_credentials_for_facade_locked(
                    data_dir,
                )
            })?;
            recovered |= access.resumed;
        }
        let source_before = preflight_source_hashes(data_dir)?;
        let preflight = (|| {
            let broker_ready = preflight_broker_document(data_dir)?;
            // Decode every planner input now so malformed roots or sidecars
            // fail byte-for-byte unchanged when no committed recovery exists.
            let input = load_strict_planning_input(data_dir)?;
            preflight_legacy_root_sections(data_dir, &input)?;
            crate::credential_migration::preflight_provider_mcp_credential_migration(data_dir)?;
            crate::credential_migration::preflight_cluster_credential_migration(data_dir)?;
            Ok::<bool, crate::ConfigStoreError>(broker_ready)
        })();
        #[cfg(test)]
        run_facade_preflight_test_hook(data_dir);
        // A transaction may publish its manifest while either a successful or
        // failing preflight is reading source files. In both cases the result
        // belongs to a stale epoch and must not authorize a new migration.
        let source_after = preflight_source_hashes(data_dir)?;
        if source_after != source_before {
            recovered |= crate::recover_pending_config_transaction(data_dir)?;
            continue;
        }
        let broker_ready = preflight?;
        #[cfg(test)]
        run_facade_epoch_test_hook(data_dir);
        let attempt = crate::credential_migration::with_migration_lock(data_dir, || {
            // An AfterJournal crash has no committed intent. Discard it before
            // comparing the preflight epoch so this same public call can
            // observe the change, retry, and build a fresh compound plan.
            crate::credential_migration::discard_uncommitted(data_dir)?;
            // Bind the complete zero-write preflight to the same exclusive
            // epoch as every credential domain and the final section split.
            // A managed writer cannot enter between these commits; a writer
            // that won the race before the lock invalidates this attempt.
            if preflight_source_hashes(data_dir)? != source_after {
                return Ok(None);
            }
            if section_layout_is_active(data_dir)? {
                let provider = crate::credential_migration::
                    migrate_provider_mcp_credentials_for_facade_locked(data_dir)?;
                let cluster =
                    crate::credential_migration::migrate_cluster_credentials_locked(data_dir)?;
                let access = crate::credential_migration::
                    migrate_access_control_credentials_for_facade_locked(data_dir)?;
                let mut broker_resumed = false;
                if broker_ready {
                    match crate::credential_migration::migrate_external_broker_credentials_locked(
                        data_dir,
                    ) {
                        Ok(outcome) => {
                            broker_resumed = outcome.resumed;
                            sync_active_external_broker(data_dir)?;
                        }
                        Err(
                            crate::ConfigStoreError::Json(_)
                            | crate::ConfigStoreError::Validation(_),
                        ) => {}
                        Err(error) => return Err(error),
                    }
                }
                return Ok(Some(crate::SectionMigrationOutcome {
                    activated: false,
                    resumed: recovered
                        || provider.resumed
                        || cluster.resumed
                        || access.resumed
                        || broker_resumed,
                }));
            }

            let source_snapshot =
                crate::credential_migration::FacadeSourceSnapshot::capture(data_dir)?;
            let credential_plan = crate::credential_migration::plan_facade_compound_credentials(
                data_dir,
                source_snapshot.clone(),
                broker_ready,
            )?;
            let plan = plan_config_facade_compound_layout(data_dir, broker_ready, credential_plan)?;
            if crate::credential_migration::FacadeSourceSnapshot::capture(data_dir)?
                != source_snapshot
            {
                return Ok(None);
            }
            let mut outcome = crate::credential_migration::install_section_split_migration_locked(
                data_dir, plan,
            )?;
            outcome.resumed |= recovered;
            Ok(Some(outcome))
        })?;
        if let Some(outcome) = attempt {
            return Ok(outcome);
        }
    }
    Err(crate::ConfigStoreError::Validation(
        "configuration changed repeatedly during facade preflight".to_string(),
    ))
}

fn sync_active_external_broker(data_dir: &Path) -> ConfigStoreResult<()> {
    let Some((_, broker_raw)) = load_strict_broker(data_dir)? else {
        return Ok(());
    };
    let path = data_dir.join("subagents.json");
    let store = AtomicJsonStore::<Value>::new(&path, SECTION_SCHEMA_VERSION)
        .with_raw_validator(validate_ordinary_section_raw);
    for _ in 0..4 {
        let bytes = read_existing_bytes(&path)?.ok_or_else(layout_not_committed)?;
        let (revision, current) = compatible_section_data(&bytes)?;
        let current = current.ok_or_else(layout_not_committed)?;
        let mut candidate = current.clone();
        let object = candidate.as_object_mut().ok_or_else(|| {
            crate::ConfigStoreError::Validation(
                "subagents section must contain an object".to_string(),
            )
        })?;
        if object.get("external_broker") == Some(&broker_raw) {
            return Ok(());
        }
        object.insert("external_broker".to_string(), broker_raw.clone());
        validate_section_data("subagents.json", &candidate)?;
        match store.commit_from_live_snapshot(revision, &current, candidate, |value| {
            validate_section_data("subagents.json", value).map_err(|error| error.to_string())
        }) {
            Ok(_) => return Ok(()),
            Err(crate::ConfigStoreError::Conflict { .. }) => continue,
            Err(error) => return Err(error),
        }
    }
    Err(crate::ConfigStoreError::Validation(
        "subagents section changed repeatedly while adopting the repaired broker".to_string(),
    ))
}

#[cfg(test)]
fn plan_config_facade_layout(data_dir: &Path) -> ConfigStoreResult<SectionSplitPlan> {
    plan_config_facade_layout_with_broker(data_dir, true)
}

#[cfg(test)]
type FacadePlanningTestHook = Box<dyn FnOnce() + Send + 'static>;

#[cfg(test)]
fn facade_planning_test_hook() -> &'static Mutex<HashMap<PathBuf, FacadePlanningTestHook>> {
    static HOOK: std::sync::OnceLock<Mutex<HashMap<PathBuf, FacadePlanningTestHook>>> =
        std::sync::OnceLock::new();
    HOOK.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
fn set_facade_planning_test_hook(data_dir: &Path, hook: impl FnOnce() + Send + 'static) {
    facade_planning_test_hook()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(data_dir.to_path_buf(), Box::new(hook));
}

#[cfg(test)]
fn run_facade_planning_test_hook(data_dir: &Path) {
    if let Some(hook) = facade_planning_test_hook()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(data_dir)
    {
        hook();
    }
}

#[cfg(test)]
fn plan_config_facade_layout_with_broker(
    data_dir: &Path,
    include_broker: bool,
) -> ConfigStoreResult<SectionSplitPlan> {
    for _ in 0..4 {
        let before = migration_source_hashes(data_dir)?;
        let input = load_strict_planning_input(data_dir)?;
        #[cfg(test)]
        run_facade_planning_test_hook(data_dir);
        let after = migration_source_hashes(data_dir)?;
        if before != after {
            continue;
        }
        let StrictPlanningInput {
            config,
            root_raw,
            model_limits,
            broker,
            authoritative_sidecars,
        } = input;
        let mut projection = SectionProjection::from_config(&config, model_limits)?;
        let mut broker_raw = None;
        if include_broker {
            if let Some((typed, raw)) = broker {
                projection.subagents.external_broker = Some(typed);
                broker_raw = Some(raw);
            }
        }
        let legacy_raw =
            project_legacy_raw_sections(&root_raw, broker_raw, &authoritative_sidecars)?;
        let plan =
            projection.into_migration_plan_with_raw(data_dir, after.clone(), legacy_raw, None)?;
        if migration_source_hashes(data_dir)? != after {
            continue;
        }
        return Ok(plan);
    }
    Err(crate::ConfigStoreError::Validation(
        "legacy configuration changed repeatedly during section planning".to_string(),
    ))
}

fn plan_config_facade_compound_layout(
    data_dir: &Path,
    include_broker: bool,
    credential_plan: crate::credential_migration::FacadeCredentialPlan,
) -> ConfigStoreResult<SectionSplitPlan> {
    let source_hashes = migration_source_hashes(data_dir)?;
    let overrides = &credential_plan.source_overrides;
    let StrictPlanningInput {
        config,
        root_raw,
        model_limits,
        broker,
        authoritative_sidecars,
    } = load_strict_planning_input_with_overrides(data_dir, Some(overrides))?;
    let mut projection = SectionProjection::from_config(&config, model_limits)?;
    let mut broker_raw = None;
    if include_broker {
        if let Some((typed, raw)) = broker {
            projection.subagents.external_broker = Some(typed);
            broker_raw = Some(raw);
        }
    }
    let legacy_raw = project_legacy_raw_sections(&root_raw, broker_raw, &authoritative_sidecars)?;
    let mut plan = projection.into_migration_plan_with_raw(
        data_dir,
        source_hashes,
        legacy_raw,
        Some(overrides),
    )?;
    plan.credential_plan = Some(credential_plan);
    Ok(plan)
}

fn expected_section_layout() -> BTreeMap<String, String> {
    SECTION_DESCRIPTORS
        .iter()
        .map(|descriptor| {
            (
                descriptor.name.to_string(),
                descriptor.file_name.to_string(),
            )
        })
        .collect()
}

fn validate_section_layout_completion_shape(marker: &SectionLayoutMarker) -> ConfigStoreResult<()> {
    let completion = marker.completion.as_ref().ok_or_else(|| {
        crate::ConfigStoreError::Validation(
            "section layout marker has no completion attestation".to_string(),
        )
    })?;
    let expected_members = SECTION_DESCRIPTORS
        .iter()
        .map(|descriptor| descriptor.file_name.to_string())
        .collect::<BTreeSet<_>>();
    let actual_members = completion.members.keys().cloned().collect::<BTreeSet<_>>();
    if marker.layout_version != SECTION_LAYOUT_VERSION
        || marker.sections != expected_section_layout()
        || completion.version != SECTION_LAYOUT_COMPLETION_VERSION
        || Uuid::parse_str(&completion.transaction_id).is_err()
        || actual_members != expected_members
        || completion.members.len() != SECTION_DESCRIPTORS.len()
        || completion.members.values().any(|member| {
            member.sha256.len() != 64
                || !member
                    .sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        })
    {
        return Err(crate::ConfigStoreError::Validation(
            "section layout completion attestation is invalid".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn build_completed_section_layout_marker(
    transaction_id: &str,
    members: BTreeMap<String, SectionLayoutCompletionMember>,
) -> ConfigStoreResult<Vec<u8>> {
    let marker = SectionLayoutMarker {
        layout_version: SECTION_LAYOUT_VERSION,
        sections: expected_section_layout(),
        completion: Some(SectionLayoutCompletion {
            version: SECTION_LAYOUT_COMPLETION_VERSION,
            transaction_id: transaction_id.to_string(),
            members,
        }),
    };
    validate_section_layout_completion_shape(&marker)?;
    Ok(serde_json::to_vec_pretty(&marker)?)
}

pub(crate) fn validate_completed_section_layout_marker(
    bytes: &[u8],
) -> ConfigStoreResult<SectionLayoutCompletion> {
    let marker: SectionLayoutMarker = serde_json::from_slice(bytes)?;
    validate_section_layout_completion_shape(&marker)?;
    marker.completion.ok_or_else(|| {
        crate::ConfigStoreError::Validation(
            "section layout marker has no completion attestation".to_string(),
        )
    })
}

pub(crate) fn current_members_satisfy_completion(
    data_dir: &Path,
    completion: &SectionLayoutCompletion,
) -> ConfigStoreResult<bool> {
    for descriptor in SECTION_DESCRIPTORS {
        let Some(bytes) = crate::credential_migration::read_section_attestation_target(
            &data_dir.join(descriptor.file_name),
        )?
        else {
            return Ok(false);
        };
        let attested = completion
            .members
            .get(descriptor.file_name)
            .ok_or_else(|| {
                crate::ConfigStoreError::Validation(
                    "section layout completion attestation is incomplete".to_string(),
                )
            })?;
        let current_revision = if descriptor.id == SectionId::Credentials {
            match CredentialStore::validate_section_document_revision(&bytes) {
                Ok(revision) => revision,
                Err(_) => return Ok(false),
            }
        } else {
            match validate_section_envelope(descriptor.file_name, &bytes, 0) {
                Ok(revision) => revision,
                Err(_) if descriptor.id == SectionId::AccessControl => {
                    // Access is the sole section whose damaged bytes can be
                    // projected into an encrypted, fail-closed repair record.
                    // Keep validating every structurally recoverable revision
                    // below, but let opaque Access damage through recovery so
                    // it cannot revoke the already-attested unrelated layout.
                    let Ok(mut envelope) = serde_json::from_slice::<Value>(&bytes) else {
                        continue;
                    };
                    let Some(object) = envelope.as_object_mut() else {
                        continue;
                    };
                    if object.get("schema_version").and_then(Value::as_u64)
                        != Some(u64::from(SECTION_SCHEMA_VERSION))
                    {
                        continue;
                    }
                    let Some(revision) = object.get("revision").and_then(Value::as_u64) else {
                        continue;
                    };
                    let Some(data) = object.get_mut("data") else {
                        continue;
                    };
                    if crate::credential_migration::sanitize_access_control_for_preflight(data)
                        .is_err()
                        || validate_section_data(descriptor.file_name, data).is_err()
                    {
                        continue;
                    }
                    revision
                }
                Err(_) => return Ok(false),
            }
        };
        if current_revision < attested.revision {
            return Ok(false);
        }
        if current_revision == attested.revision
            && hex::encode(Sha256::digest(&bytes)) != attested.sha256
        {
            return Ok(false);
        }
    }
    Ok(true)
}

pub fn section_layout_is_active(data_dir: impl AsRef<Path>) -> ConfigStoreResult<bool> {
    let data_dir = data_dir.as_ref();
    let Some(bytes) = crate::credential_migration::read_section_attestation_target(
        &data_dir.join(SECTION_LAYOUT_FILE),
    )?
    else {
        return Ok(false);
    };
    let completion = match validate_completed_section_layout_marker(&bytes) {
        Ok(completion) => completion,
        Err(_) => return Ok(false),
    };
    crate::credential_migration::section_layout_completion_evidence_matches(
        data_dir,
        &bytes,
        &completion,
    )
}

#[derive(Debug, Clone, Serialize)]
pub struct SectionHealth {
    pub section: SectionId,
    pub revision: u64,
    pub loaded_at: DateTime<Utc>,
    pub source_path: PathBuf,
    pub source_kind: SectionSourceKind,
    pub status: SectionStatus,
    pub last_error: Option<String>,
}

fn live_health<T>(id: SectionId, section: &LiveSection<T>) -> SectionHealth
where
    T: Clone + Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    let snapshot = section.snapshot();
    SectionHealth {
        section: id,
        revision: snapshot.revision,
        loaded_at: snapshot.loaded_at,
        source_path: snapshot.source_path.clone(),
        source_kind: snapshot.source_kind,
        status: snapshot.status,
        last_error: snapshot.last_error.clone(),
    }
}

#[derive(Debug, Clone)]
pub struct CredentialSectionSnapshot {
    pub data: Arc<Vec<CredentialStatus>>,
    pub revision: u64,
    pub loaded_at: DateTime<Utc>,
    pub source_path: PathBuf,
    pub source_kind: SectionSourceKind,
    pub status: SectionStatus,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CredentialRepairAuthority {
    None,
    DegradedBackup,
    MissingAfterGood,
    InvalidAfterGood,
}

impl CredentialRepairAuthority {
    fn can_repair(self) -> bool {
        self != Self::None
    }
}

pub struct CredentialSection {
    store: CredentialStore,
    operation_lock: Mutex<()>,
    document_lkg: RwLock<Option<Arc<CredentialDocumentLkg>>>,
    trusted_lkg: RwLock<bool>,
    repair_authority: RwLock<CredentialRepairAuthority>,
    snapshot: RwLock<Arc<CredentialSectionSnapshot>>,
}

impl CredentialSection {
    fn open(data_dir: &Path, migration_lock_held: bool) -> Self {
        let store = CredentialStore::open(data_dir);
        let (snapshot, document_lkg) = match load_credential_snapshot(&store, migration_lock_held) {
            Ok((snapshot, document_lkg)) => (snapshot, Some(Arc::new(document_lkg))),
            Err(error) => (
                Arc::new(CredentialSectionSnapshot {
                    data: Arc::new(Vec::new()),
                    revision: 0,
                    loaded_at: Utc::now(),
                    source_path: store.path().to_path_buf(),
                    source_kind: SectionSourceKind::Default,
                    status: SectionStatus::Invalid,
                    last_error: Some(error_category(&error)),
                }),
                None,
            ),
        };
        let repair_authority = if snapshot.status == SectionStatus::Degraded {
            CredentialRepairAuthority::DegradedBackup
        } else {
            CredentialRepairAuthority::None
        };
        let trusted_lkg = matches!(
            snapshot.status,
            SectionStatus::Healthy | SectionStatus::Degraded
        );
        Self {
            store,
            operation_lock: Mutex::new(()),
            document_lkg: RwLock::new(document_lkg),
            trusted_lkg: RwLock::new(trusted_lkg),
            repair_authority: RwLock::new(repair_authority),
            snapshot: RwLock::new(snapshot),
        }
    }

    pub fn snapshot(&self) -> Arc<CredentialSectionSnapshot> {
        self.snapshot
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn statuses(&self) -> Vec<CredentialStatus> {
        self.snapshot().data.as_ref().clone()
    }

    pub fn health(&self) -> SectionHealth {
        let snapshot = self.snapshot();
        SectionHealth {
            section: SectionId::Credentials,
            revision: snapshot.revision,
            loaded_at: snapshot.loaded_at,
            source_path: snapshot.source_path.clone(),
            source_kind: snapshot.source_kind,
            status: snapshot.status,
            last_error: snapshot.last_error.clone(),
        }
    }

    /// Silently adopt a credential snapshot captured by a compound
    /// transaction while it held the migration lock. This method is called
    /// only after that outer lock has been released, preserving the ordinary
    /// credential mutation lock order (`operation_lock` then migration lock).
    ///
    /// A newer process-local snapshot always wins. Equal revisions must carry
    /// identical public status data; otherwise the durable source reused a
    /// revision and adoption fails closed.
    fn adopt_captured(
        &self,
        document_lkg: CredentialDocumentLkg,
        statuses: Vec<CredentialStatus>,
        health: CredentialStoreHealth,
    ) -> ConfigStoreResult<()> {
        self.adopt_captured_with_event(document_lkg, statuses, health)
            .map(|_| ())
    }

    fn adopt_captured_with_event(
        &self,
        document_lkg: CredentialDocumentLkg,
        statuses: Vec<CredentialStatus>,
        health: CredentialStoreHealth,
    ) -> ConfigStoreResult<Option<ConfigSectionEvent>> {
        let _operation = self
            .operation_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let current = self.snapshot();
        if current.revision > health.revision {
            return Ok(None);
        }
        if current.revision == health.revision && current.data.as_ref() != &statuses {
            return Err(crate::ConfigStoreError::Validation(
                "credential document reused the live revision with different data".to_string(),
            ));
        }
        if current.revision == health.revision
            && current.status == health.status
            && current.source_kind == health.source
            && current.last_error == health.last_error
            && (health.status != SectionStatus::Healthy || health.source != SectionSourceKind::File)
        {
            // A cluster-only metadata commit still captures credential health
            // under the transaction lock. Missing/default or degraded/backup
            // snapshots need no adoption when the facade already owns that
            // exact state.
            return Ok(None);
        }
        // Healthy/file snapshots continue through publication even when their
        // public status projection is unchanged, so the facade's opaque LKG is
        // the exact credential document captured by this compound commit.
        if health.status != SectionStatus::Healthy
            || health.source != SectionSourceKind::File
            || document_lkg.revision() != health.revision
        {
            return Err(crate::ConfigStoreError::Validation(
                "captured credential transaction snapshot is not healthy".to_string(),
            ));
        }

        let observable_change = current.revision != health.revision
            || current.status != health.status
            || current.source_kind != health.source
            || current.last_error != health.last_error
            || current.data.as_ref() != &statuses;
        let recovered =
            current.status != SectionStatus::Healthy && health.status == SectionStatus::Healthy;
        *self
            .document_lkg
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::new(document_lkg));
        *self
            .repair_authority
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = CredentialRepairAuthority::None;
        *self
            .trusted_lkg
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        *self
            .snapshot
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Arc::new(CredentialSectionSnapshot {
                data: Arc::new(statuses),
                revision: health.revision,
                loaded_at: Utc::now(),
                source_path: self.store.path().to_path_buf(),
                source_kind: health.source,
                status: health.status,
                last_error: health.last_error,
            });
        Ok(observable_change.then(|| {
            if recovered {
                ConfigSectionEvent::Recovered {
                    section: SectionId::Credentials.descriptor().name.to_string(),
                    revision: health.revision,
                }
            } else {
                ConfigSectionEvent::Changed {
                    section: SectionId::Credentials.descriptor().name.to_string(),
                    revision: health.revision,
                }
            }
        }))
    }

    pub fn reload(&self) -> ConfigStoreResult<Arc<CredentialSectionSnapshot>> {
        let _operation = self
            .operation_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.reload_locked()
    }

    fn reload_locked(&self) -> ConfigStoreResult<Arc<CredentialSectionSnapshot>> {
        let data_dir = self
            .store
            .path()
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let mut readiness_failed = false;
        let loaded =
            crate::credential_migration::with_provider_mcp_migration_lock(data_dir, || {
                if let Err(error) = crate::ensure_provider_mcp_migration_ready(data_dir) {
                    readiness_failed = true;
                    return Err(error);
                }
                load_credential_snapshot(&self.store, true)
            });
        if readiness_failed {
            return match loaded {
                Err(error) => Err(error),
                Ok((snapshot, _)) => Ok(snapshot),
            };
        }
        match loaded {
            Ok((mut snapshot, candidate_lkg)) => {
                let current = self.snapshot();
                let has_trusted_lkg = self.has_trusted_credential_lkg();
                let mut retain_lkg = false;
                let repair_authority = if snapshot.status == SectionStatus::Degraded
                    && has_trusted_lkg
                    && current.revision >= snapshot.revision
                {
                    retain_lkg = true;
                    snapshot = retain_credential_lkg(
                        &current,
                        &snapshot,
                        SectionStatus::Degraded,
                        snapshot.last_error.clone(),
                    );
                    CredentialRepairAuthority::DegradedBackup
                } else if snapshot.status == SectionStatus::Missing && has_trusted_lkg {
                    retain_lkg = true;
                    snapshot = retain_credential_lkg(
                        &current,
                        &snapshot,
                        SectionStatus::Missing,
                        Some(
                            "credential document is missing; retaining last-known-good snapshot"
                                .to_string(),
                        ),
                    );
                    CredentialRepairAuthority::MissingAfterGood
                } else if snapshot.revision < current.revision {
                    retain_lkg = true;
                    snapshot = retain_credential_lkg(
                        &current,
                        &snapshot,
                        SectionStatus::Invalid,
                        Some(
                            "credential document revision moved backwards; retaining last-known-good snapshot"
                                .to_string(),
                            ),
                    );
                    CredentialRepairAuthority::None
                } else if snapshot.revision == current.revision
                    && snapshot.data.as_ref() != current.data.as_ref()
                {
                    retain_lkg = true;
                    snapshot = retain_credential_lkg(
                        &current,
                        &snapshot,
                        SectionStatus::Invalid,
                        Some(
                            "credential document reused the live revision with different data; retaining last-known-good snapshot"
                                .to_string(),
                            ),
                    );
                    CredentialRepairAuthority::None
                } else if snapshot.status == SectionStatus::Degraded {
                    CredentialRepairAuthority::DegradedBackup
                } else {
                    CredentialRepairAuthority::None
                };
                *self
                    .snapshot
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = snapshot.clone();
                if !retain_lkg {
                    *self
                        .document_lkg
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) =
                        Some(Arc::new(candidate_lkg));
                }
                if matches!(
                    snapshot.status,
                    SectionStatus::Healthy | SectionStatus::Degraded
                ) {
                    *self
                        .trusted_lkg
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
                }
                *self
                    .repair_authority
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = repair_authority;
                Ok(snapshot)
            }
            Err(error) => {
                let current = self.snapshot();
                let has_trusted_lkg = self.has_trusted_credential_lkg();
                let repair_authority =
                    if has_trusted_lkg && crate::config_store::repairable_live_lkg_error(&error) {
                        CredentialRepairAuthority::InvalidAfterGood
                    } else {
                        CredentialRepairAuthority::None
                    };
                let invalid = Arc::new(CredentialSectionSnapshot {
                    data: current.data.clone(),
                    revision: current.revision,
                    loaded_at: Utc::now(),
                    source_path: current.source_path.clone(),
                    source_kind: current.source_kind,
                    status: SectionStatus::Invalid,
                    last_error: Some(error_category(&error)),
                });
                *self
                    .snapshot
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = invalid;
                *self
                    .repair_authority
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = repair_authority;
                Err(error)
            }
        }
    }

    pub fn replace(
        &self,
        credential_ref: CredentialRef,
        secret: &str,
        source: CredentialSource,
        expected_revision: u64,
    ) -> ConfigStoreResult<(u64, CredentialStatus)> {
        self.replace_inner(credential_ref, secret, source, expected_revision, || {})
    }

    fn replace_inner<F>(
        &self,
        credential_ref: CredentialRef,
        secret: &str,
        source: CredentialSource,
        expected_revision: u64,
        before_publish: F,
    ) -> ConfigStoreResult<(u64, CredentialStatus)>
    where
        F: FnOnce(),
    {
        let _operation = self
            .operation_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let current = self.snapshot();
        ensure_live_credential_revision(&current, expected_revision)?;
        let live_lkg = self.live_credential_document(expected_revision)?;
        let repair_from_lkg = self.credential_repair_authority().can_repair();
        self.store.with_transaction_lock(|| {
            let mutation = if repair_from_lkg {
                self.store.replace_from_live_lkg_unchecked(
                    &live_lkg,
                    credential_ref,
                    secret,
                    source,
                    expected_revision,
                )?
            } else {
                self.store.replace_from_live_snapshot_unchecked(
                    &live_lkg,
                    credential_ref,
                    secret,
                    source,
                    expected_revision,
                )?
            };
            before_publish();
            let result = (mutation.revision, mutation.status.clone());
            self.publish_credential_mutation(mutation);
            Ok(result)
        })
    }

    pub fn clear(
        &self,
        credential_ref: &CredentialRef,
        expected_revision: u64,
    ) -> ConfigStoreResult<(u64, CredentialStatus)> {
        self.clear_inner(credential_ref, expected_revision, || {})
    }

    fn clear_inner<F>(
        &self,
        credential_ref: &CredentialRef,
        expected_revision: u64,
        before_publish: F,
    ) -> ConfigStoreResult<(u64, CredentialStatus)>
    where
        F: FnOnce(),
    {
        let _operation = self
            .operation_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let current = self.snapshot();
        ensure_live_credential_revision(&current, expected_revision)?;
        let live_lkg = self.live_credential_document(expected_revision)?;
        let repair_from_lkg = self.credential_repair_authority().can_repair();
        self.store.with_transaction_lock(|| {
            let mutation = if repair_from_lkg {
                self.store.clear_from_live_lkg_unchecked(
                    &live_lkg,
                    credential_ref,
                    expected_revision,
                )?
            } else {
                self.store.clear_from_live_snapshot_unchecked(
                    &live_lkg,
                    credential_ref,
                    expected_revision,
                )?
            };
            before_publish();
            let result = (mutation.revision, mutation.status.clone());
            self.publish_credential_mutation(mutation);
            Ok(result)
        })
    }

    fn publish_credential_mutation(&self, mutation: CredentialMutation) {
        let CredentialMutation {
            revision,
            status: _,
            statuses,
            lkg,
        } = mutation;
        let snapshot = Arc::new(CredentialSectionSnapshot {
            data: Arc::new(statuses),
            revision,
            loaded_at: Utc::now(),
            source_path: self.store.path().to_path_buf(),
            source_kind: SectionSourceKind::File,
            status: SectionStatus::Healthy,
            last_error: None,
        });
        *self
            .document_lkg
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::new(lkg));
        *self
            .repair_authority
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = CredentialRepairAuthority::None;
        *self
            .trusted_lkg
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        *self
            .snapshot
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = snapshot;
    }

    fn live_credential_document(
        &self,
        expected_revision: u64,
    ) -> ConfigStoreResult<Arc<CredentialDocumentLkg>> {
        let lkg = self
            .document_lkg
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or_else(|| {
                crate::ConfigStoreError::Validation(
                    "credential last-known-good document is unavailable".to_string(),
                )
            })?;
        if lkg.revision() != expected_revision {
            return Err(crate::ConfigStoreError::Conflict {
                expected: expected_revision,
                actual: lkg.revision(),
            });
        }
        Ok(lkg)
    }

    fn credential_repair_authority(&self) -> CredentialRepairAuthority {
        *self
            .repair_authority
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn has_trusted_credential_lkg(&self) -> bool {
        *self
            .trusted_lkg
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn retain_credential_lkg(
    current: &CredentialSectionSnapshot,
    observed: &CredentialSectionSnapshot,
    status: SectionStatus,
    last_error: Option<String>,
) -> Arc<CredentialSectionSnapshot> {
    Arc::new(CredentialSectionSnapshot {
        data: current.data.clone(),
        revision: current.revision,
        loaded_at: observed.loaded_at,
        source_path: observed.source_path.clone(),
        source_kind: observed.source_kind,
        status,
        last_error,
    })
}

fn ensure_live_credential_revision(
    current: &CredentialSectionSnapshot,
    expected_revision: u64,
) -> ConfigStoreResult<()> {
    if current.revision == expected_revision {
        Ok(())
    } else {
        Err(crate::ConfigStoreError::Conflict {
            expected: expected_revision,
            actual: current.revision,
        })
    }
}

fn load_credential_snapshot(
    store: &CredentialStore,
    migration_lock_held: bool,
) -> ConfigStoreResult<(Arc<CredentialSectionSnapshot>, CredentialDocumentLkg)> {
    let (document_lkg, statuses, health) = if migration_lock_held {
        store.snapshot_with_health_unchecked()?
    } else {
        store.snapshot_with_health()?
    };
    Ok((
        Arc::new(CredentialSectionSnapshot {
            data: Arc::new(statuses),
            revision: health.revision,
            loaded_at: Utc::now(),
            source_path: store.path().to_path_buf(),
            source_kind: health.source,
            status: health.status,
            last_error: health.last_error,
        }),
        document_lkg,
    ))
}

fn error_category(error: &crate::ConfigStoreError) -> String {
    match error {
        crate::ConfigStoreError::Io(_) => "credential store I/O failed",
        crate::ConfigStoreError::Json(_) => "credential document is invalid",
        crate::ConfigStoreError::Conflict { .. } => "credential revision conflict",
        crate::ConfigStoreError::Validation(_) => "credential document failed validation",
        crate::ConfigStoreError::CommitIndeterminate(_) => {
            "credential transaction outcome is indeterminate"
        }
        crate::ConfigStoreError::Watch(_) => "credential store watch failed",
    }
    .to_string()
}

/// Process-local registry of typed immutable section snapshots.
pub struct SectionRegistry {
    pub core: Arc<LiveSection<CoreSection>>,
    pub providers: Arc<LiveSection<ProvidersSection>>,
    pub mcp: Arc<LiveSection<McpSection>>,
    pub tools_skills: Arc<LiveSection<ToolsSkillsSection>>,
    pub memory: Arc<LiveSection<MemorySection>>,
    pub subagents: Arc<LiveSection<SubagentsSection>>,
    pub notifications: Arc<LiveSection<NotificationsSection>>,
    pub connect: Arc<LiveSection<ConnectSection>>,
    pub cluster_fabric: Arc<LiveSection<ClusterFabricSection>>,
    pub env: Arc<LiveSection<EnvSection>>,
    pub access_control: Arc<LiveSection<AccessControlSection>>,
    pub hooks: Arc<LiveSection<HooksSection>>,
    pub model_policy: Arc<LiveSection<ModelPolicySection>>,
    pub model_limits: Arc<LiveSection<ModelLimitsSection>>,
    pub credentials: Arc<CredentialSection>,
}

impl SectionRegistry {
    #[cfg(test)]
    pub(crate) fn open(data_dir: impl AsRef<Path>) -> ConfigStoreResult<Self> {
        Self::open_configured(data_dir.as_ref(), false)
    }

    fn open_configured(data_dir: &Path, migration_lock_held: bool) -> ConfigStoreResult<Self> {
        macro_rules! open {
            ($id:ident, $ty:ty, $validate:expr) => {{
                let descriptor = SectionId::$id.descriptor();
                Arc::new(LiveSection::open_with_raw_validator(
                    descriptor.name,
                    AtomicJsonStore::<$ty>::new(
                        data_dir.join(descriptor.file_name),
                        SECTION_SCHEMA_VERSION,
                    ),
                    <$ty>::default(),
                    $validate,
                    validate_ordinary_section_raw,
                )?)
            }};
        }
        Ok(Self {
            core: open!(Core, CoreSection, validate_core),
            providers: {
                let descriptor = SectionId::Providers.descriptor();
                Arc::new(LiveSection::open_with_raw_validator_allowing_unversioned(
                    descriptor.name,
                    AtomicJsonStore::<ProvidersSection>::new(
                        data_dir.join(descriptor.file_name),
                        SECTION_SCHEMA_VERSION,
                    ),
                    ProvidersSection::default(),
                    validate_providers,
                    validate_ordinary_section_raw,
                )?)
            },
            mcp: open!(Mcp, McpSection, validate_mcp),
            tools_skills: open!(ToolsSkills, ToolsSkillsSection, validate_tools_skills),
            memory: open!(Memory, MemorySection, validate_memory),
            subagents: open!(Subagents, SubagentsSection, validate_subagents),
            notifications: open!(Notifications, NotificationsSection, validate_notifications),
            connect: open!(Connect, ConnectSection, validate_connect),
            cluster_fabric: open!(ClusterFabric, ClusterFabricSection, validate_cluster_fabric),
            env: open!(Env, EnvSection, validate_env),
            access_control: open!(AccessControl, AccessControlSection, validate_access_control),
            hooks: open!(Hooks, HooksSection, validate_hooks),
            model_policy: open!(ModelPolicy, ModelPolicySection, validate_model_policy),
            model_limits: open!(ModelLimits, ModelLimitsSection, validate_model_limits),
            credentials: Arc::new(CredentialSection::open(data_dir, migration_lock_held)),
        })
    }

    pub fn health(&self) -> ConfigStoreResult<Vec<SectionHealth>> {
        let mut health = vec![
            live_health(SectionId::Core, &self.core),
            live_health(SectionId::Providers, &self.providers),
            live_health(SectionId::Mcp, &self.mcp),
            live_health(SectionId::ToolsSkills, &self.tools_skills),
            live_health(SectionId::Memory, &self.memory),
            live_health(SectionId::Subagents, &self.subagents),
            live_health(SectionId::Notifications, &self.notifications),
            live_health(SectionId::Connect, &self.connect),
            live_health(SectionId::ClusterFabric, &self.cluster_fabric),
            live_health(SectionId::Env, &self.env),
            live_health(SectionId::AccessControl, &self.access_control),
            live_health(SectionId::Hooks, &self.hooks),
            live_health(SectionId::ModelPolicy, &self.model_policy),
            live_health(SectionId::ModelLimits, &self.model_limits),
        ];
        health.push(self.credentials.health());
        Ok(health)
    }

    /// Return the public, secret-free envelope for one typed authority.
    pub fn envelope_value(&self, id: SectionId) -> ConfigStoreResult<SectionEnvelope<Value>> {
        macro_rules! envelope {
            ($field:ident) => {{
                let snapshot = self.$field.snapshot();
                SectionEnvelope {
                    data: serde_json::to_value(snapshot.data.as_ref())?,
                    revision: snapshot.revision,
                    loaded_at: snapshot.loaded_at,
                    source_path: snapshot.source_path.clone(),
                    source_kind: snapshot.source_kind,
                    status: snapshot.status,
                    last_error: snapshot.last_error.clone(),
                }
            }};
        }
        Ok(match id {
            SectionId::Core => envelope!(core),
            SectionId::Providers => envelope!(providers),
            SectionId::Mcp => envelope!(mcp),
            SectionId::ToolsSkills => envelope!(tools_skills),
            SectionId::Memory => envelope!(memory),
            SectionId::Subagents => envelope!(subagents),
            SectionId::Notifications => envelope!(notifications),
            SectionId::Connect => envelope!(connect),
            SectionId::ClusterFabric => envelope!(cluster_fabric),
            SectionId::Env => envelope!(env),
            SectionId::AccessControl => envelope!(access_control),
            SectionId::Hooks => envelope!(hooks),
            SectionId::ModelPolicy => envelope!(model_policy),
            SectionId::ModelLimits => envelope!(model_limits),
            SectionId::Credentials => {
                let snapshot = self.credentials.snapshot();
                SectionEnvelope {
                    data: serde_json::to_value(snapshot.data.as_ref())?,
                    revision: snapshot.revision,
                    loaded_at: snapshot.loaded_at,
                    source_path: snapshot.source_path.clone(),
                    source_kind: snapshot.source_kind,
                    status: snapshot.status,
                    last_error: snapshot.last_error.clone(),
                }
            }
        })
    }

    /// Commit one ordinary section from its public JSON representation.
    /// Provider and MCP writes are intentionally owned by server adapters that
    /// stage their replacement runtimes; credential values use their dedicated
    /// replace/clear API.
    pub fn commit_value(
        &self,
        id: SectionId,
        expected_revision: u64,
        candidate: Value,
    ) -> ConfigStoreResult<ConfigSectionEvent> {
        self.commit_value_with_envelope(id, expected_revision, candidate)
            .map(|(event, _)| event)
    }

    /// Commit one ordinary section and return the immutable public envelope
    /// captured by that exact commit before a later reload can advance the
    /// process snapshot.
    pub fn commit_value_with_envelope(
        &self,
        id: SectionId,
        expected_revision: u64,
        candidate: Value,
    ) -> ConfigStoreResult<(ConfigSectionEvent, SectionEnvelope<Value>)> {
        validate_ordinary_section_json(&candidate)?;
        macro_rules! commit {
            ($field:ident, $ty:ty) => {{
                let typed: $ty = serde_json::from_value(candidate)?;
                let (event, envelope) =
                    self.$field.commit_with_envelope(expected_revision, typed)?;
                Ok((
                    event,
                    SectionEnvelope {
                        data: serde_json::to_value(envelope.data)?,
                        revision: envelope.revision,
                        loaded_at: envelope.loaded_at,
                        source_path: envelope.source_path,
                        source_kind: envelope.source_kind,
                        status: envelope.status,
                        last_error: envelope.last_error,
                    },
                ))
            }};
        }
        match id {
            SectionId::Core => commit!(core, CoreSection),
            SectionId::ToolsSkills => commit!(tools_skills, ToolsSkillsSection),
            SectionId::Memory => commit!(memory, MemorySection),
            SectionId::Subagents => commit!(subagents, SubagentsSection),
            SectionId::Notifications => commit!(notifications, NotificationsSection),
            SectionId::Connect => commit!(connect, ConnectSection),
            SectionId::ClusterFabric => commit!(cluster_fabric, ClusterFabricSection),
            SectionId::Env => commit!(env, EnvSection),
            SectionId::AccessControl => commit!(access_control, AccessControlSection),
            SectionId::Hooks => commit!(hooks, HooksSection),
            SectionId::ModelPolicy => commit!(model_policy, ModelPolicySection),
            SectionId::ModelLimits => commit!(model_limits, ModelLimitsSection),
            SectionId::Providers | SectionId::Mcp => Err(crate::ConfigStoreError::Validation(
                "provider and MCP sections require runtime-staged endpoints".to_string(),
            )),
            SectionId::Credentials => Err(crate::ConfigStoreError::Validation(
                "credentials require the status/replace/clear API".to_string(),
            )),
        }
    }

    /// Reload one watched section into its immutable LKG snapshot. Runtime
    /// side effects remain the server owner's responsibility; this method only
    /// publishes typed data/health and returns the corresponding public event.
    pub fn reload(&self, id: SectionId) -> ConfigSectionEvent {
        macro_rules! reload {
            ($field:ident) => {
                self.$field.reload()
            };
        }
        match id {
            SectionId::Core => reload!(core),
            SectionId::Providers => reload!(providers),
            SectionId::Mcp => reload!(mcp),
            SectionId::ToolsSkills => reload!(tools_skills),
            SectionId::Memory => reload!(memory),
            SectionId::Subagents => reload!(subagents),
            SectionId::Notifications => reload!(notifications),
            SectionId::Connect => reload!(connect),
            SectionId::ClusterFabric => reload!(cluster_fabric),
            SectionId::Env => reload!(env),
            SectionId::AccessControl => reload!(access_control),
            SectionId::Hooks => reload!(hooks),
            SectionId::ModelPolicy => reload!(model_policy),
            SectionId::ModelLimits => reload!(model_limits),
            SectionId::Credentials => {
                let before = self.credentials.health();
                let loaded = self.credentials.reload();
                let after = self.credentials.health();
                if loaded.is_err() || after.status != SectionStatus::Healthy {
                    ConfigSectionEvent::Invalid {
                        section: id.descriptor().name.to_string(),
                        revision: after.revision,
                    }
                } else if before.status != SectionStatus::Healthy {
                    ConfigSectionEvent::Recovered {
                        section: id.descriptor().name.to_string(),
                        revision: after.revision,
                    }
                } else {
                    ConfigSectionEvent::Changed {
                        section: id.descriptor().name.to_string(),
                        revision: after.revision,
                    }
                }
            }
        }
    }

    /// Reload a watcher notification and suppress exact self-write echoes.
    /// Revision, health and data all participate in the comparison, so an
    /// invalid/recovered transition is never hidden merely because the LKG
    /// payload stayed unchanged.
    pub fn reload_if_changed(&self, id: SectionId) -> Option<ConfigSectionEvent> {
        let before = self.observation(id);
        let event = self.reload(id);
        (self.observation(id) != before).then_some(event)
    }

    fn observation(
        &self,
        id: SectionId,
    ) -> (u64, SectionStatus, SectionSourceKind, Option<String>, Value) {
        macro_rules! observe {
            ($field:ident) => {{
                let snapshot = self.$field.snapshot();
                (
                    snapshot.revision,
                    snapshot.status,
                    snapshot.source_kind,
                    snapshot.last_error.clone(),
                    serde_json::to_value(snapshot.data.as_ref())
                        .expect("live section data remains serializable"),
                )
            }};
        }
        match id {
            SectionId::Core => observe!(core),
            SectionId::Providers => observe!(providers),
            SectionId::Mcp => observe!(mcp),
            SectionId::ToolsSkills => observe!(tools_skills),
            SectionId::Memory => observe!(memory),
            SectionId::Subagents => observe!(subagents),
            SectionId::Notifications => observe!(notifications),
            SectionId::Connect => observe!(connect),
            SectionId::ClusterFabric => observe!(cluster_fabric),
            SectionId::Env => observe!(env),
            SectionId::AccessControl => observe!(access_control),
            SectionId::Hooks => observe!(hooks),
            SectionId::ModelPolicy => observe!(model_policy),
            SectionId::ModelLimits => observe!(model_limits),
            SectionId::Credentials => {
                let snapshot = self.credentials.snapshot();
                (
                    snapshot.revision,
                    snapshot.status,
                    snapshot.source_kind,
                    snapshot.last_error.clone(),
                    serde_json::to_value(snapshot.data.as_ref())
                        .expect("credential statuses remain serializable"),
                )
            }
        }
    }

    /// Attach a runtime-hook failure to a successfully parsed durable section
    /// without replacing its data or pretending the old runtime was updated.
    pub fn mark_runtime_degraded(
        &self,
        id: SectionId,
        message: impl Into<String>,
    ) -> Option<ConfigSectionEvent> {
        let message = message.into();
        match id {
            SectionId::Core => Some(self.core.mark_runtime_degraded(message)),
            SectionId::Providers => Some(self.providers.mark_runtime_degraded(message)),
            SectionId::Mcp => Some(self.mcp.mark_runtime_degraded(message)),
            SectionId::ToolsSkills => Some(self.tools_skills.mark_runtime_degraded(message)),
            SectionId::Memory => Some(self.memory.mark_runtime_degraded(message)),
            SectionId::Subagents => Some(self.subagents.mark_runtime_degraded(message)),
            SectionId::Notifications => Some(self.notifications.mark_runtime_degraded(message)),
            SectionId::Connect => Some(self.connect.mark_runtime_degraded(message)),
            SectionId::ClusterFabric => Some(self.cluster_fabric.mark_runtime_degraded(message)),
            SectionId::Env => Some(self.env.mark_runtime_degraded(message)),
            SectionId::AccessControl => Some(self.access_control.mark_runtime_degraded(message)),
            SectionId::Hooks => Some(self.hooks.mark_runtime_degraded(message)),
            SectionId::ModelPolicy => Some(self.model_policy.mark_runtime_degraded(message)),
            SectionId::ModelLimits => Some(self.model_limits.mark_runtime_degraded(message)),
            SectionId::Credentials => None,
        }
    }

    pub fn projection(&self) -> SectionProjection {
        SectionProjection {
            core: self.core.snapshot().data.as_ref().clone(),
            providers: self.providers.snapshot().data.as_ref().clone(),
            mcp: self.mcp.snapshot().data.as_ref().clone(),
            tools_skills: self.tools_skills.snapshot().data.as_ref().clone(),
            memory: self.memory.snapshot().data.as_ref().clone(),
            subagents: self.subagents.snapshot().data.as_ref().clone(),
            notifications: self.notifications.snapshot().data.as_ref().clone(),
            connect: self.connect.snapshot().data.as_ref().clone(),
            cluster_fabric: self.cluster_fabric.snapshot().data.as_ref().clone(),
            env: self.env.snapshot().data.as_ref().clone(),
            access_control: self.access_control.snapshot().data.as_ref().clone(),
            hooks: self.hooks.snapshot().data.as_ref().clone(),
            model_policy: self.model_policy.snapshot().data.as_ref().clone(),
            model_limits: self.model_limits.snapshot().data.as_ref().clone(),
        }
    }
}

/// Reconstruct the effective configuration from the coherent modular
/// authorities while the caller owns the shared migration lock.
///
/// Credential ownership checks must use these durable snapshots rather than a
/// process-local runtime that may lag a commit made by another process.
pub(crate) fn load_durable_effective_config_under_migration_lock(
    data_dir: &Path,
) -> ConfigStoreResult<Config> {
    if !section_layout_is_active(data_dir)? {
        return Err(crate::ConfigStoreError::Validation(
            "durable credential ownership requires the modular configuration layout".to_string(),
        ));
    }
    let registry = SectionRegistry::open_configured(data_dir, true)?;
    let health = registry.health()?;
    for id in [
        SectionId::Core,
        SectionId::Providers,
        SectionId::Mcp,
        SectionId::Notifications,
        SectionId::Connect,
        SectionId::ClusterFabric,
        SectionId::Env,
        SectionId::AccessControl,
        SectionId::Subagents,
    ] {
        let health = health
            .iter()
            .find(|health| health.section == id)
            .expect("every durable credential owner has section health");
        if health.status != SectionStatus::Healthy || health.source_kind != SectionSourceKind::File
        {
            return Err(crate::ConfigStoreError::Validation(format!(
                "durable {} credential ownership is unavailable",
                id.descriptor().name
            )));
        }
    }
    Ok(registry.projection().into_config())
}

/// Reconstruct a runtime source for one exact owned section while the caller
/// owns the shared migration lock.
///
/// Unlike the full ownership preflight above, this intentionally does not
/// require the legacy-root completion attestation to remain active after the
/// exact section commit. A compatibility process may recreate `config.json`
/// after the commit point; that must not prevent adopting the still-healthy
/// typed authority, nor authorize installing the recreated root wholesale.
pub(crate) fn load_durable_effective_section_under_migration_lock(
    data_dir: &Path,
    id: SectionId,
) -> ConfigStoreResult<(Config, SectionEnvelope<Value>)> {
    let (config, envelope) =
        load_durable_effective_section_snapshot_under_migration_lock(data_dir, id)?;
    if envelope.status != SectionStatus::Healthy || envelope.source_kind != SectionSourceKind::File
    {
        return Err(crate::ConfigStoreError::Validation(format!(
            "durable {} credential ownership is unavailable",
            id.descriptor().name
        )));
    }
    Ok((config, envelope))
}

/// Reconstruct one durable section and its exact envelope while the caller
/// owns the shared migration lock, including degraded last-known-good reads.
///
/// Mutation preflights must use
/// [`load_durable_effective_section_under_migration_lock`] so a backup can
/// never authorize a write. Secret-free GET projections use this variant to
/// pair the same immutable section generation with one credential snapshot.
pub(crate) fn load_durable_effective_section_snapshot_under_migration_lock(
    data_dir: &Path,
    id: SectionId,
) -> ConfigStoreResult<(Config, SectionEnvelope<Value>)> {
    let registry = SectionRegistry::open_configured(data_dir, true)?;
    let envelope = registry.envelope_value(id)?;
    Ok((registry.projection().into_config(), envelope))
}

/// Effective compatibility facade assembled from immutable section snapshots.
pub struct ConfigFacade {
    registry: Arc<SectionRegistry>,
}

impl ConfigFacade {
    pub fn open(data_dir: impl AsRef<Path>) -> ConfigStoreResult<Self> {
        Self::open_stable(data_dir.as_ref(), |_| {})
    }

    fn open_stable<F>(data_dir: &Path, after_registry_open: F) -> ConfigStoreResult<Self>
    where
        F: FnOnce(&SectionRegistry),
    {
        let registry =
            crate::credential_migration::with_provider_mcp_migration_lock(data_dir, || {
                crate::ensure_provider_mcp_migration_ready(data_dir)?;
                if !section_layout_is_active(data_dir)? {
                    return Err(layout_not_committed());
                }
                let before = capture_layout_member_states(data_dir)?;
                let registry = SectionRegistry::open_configured(data_dir, true)?;
                after_registry_open(&registry);
                let after = capture_layout_member_states(data_dir)?;
                if before != after {
                    return Err(crate::ConfigStoreError::Validation(
                        "configuration changed while opening the modular facade".to_string(),
                    ));
                }
                crate::ensure_provider_mcp_migration_ready(data_dir)?;
                if !section_layout_is_active(data_dir)? {
                    return Err(layout_not_committed());
                }
                Ok(registry)
            })?;
        Ok(Self {
            registry: Arc::new(registry),
        })
    }

    pub fn open_or_migrate(data_dir: impl AsRef<Path>) -> ConfigStoreResult<Self> {
        migrate_config_facade_layout(data_dir.as_ref())?;
        migrate_copilot_oauth_cache(data_dir.as_ref())?;
        Self::open(data_dir)
    }

    pub fn registry(&self) -> &Arc<SectionRegistry> {
        &self.registry
    }

    pub fn effective_config(&self) -> Config {
        self.registry.projection().into_config()
    }

    /// Publish the exact cluster-fabric snapshot installed by the credential
    /// compound transaction. This is a process-local adoption only: the
    /// transaction has already made both durable members authoritative and
    /// must still hold its cross-process migration lock while calling here.
    pub(crate) fn adopt_committed_cluster_fabric(
        &self,
        expected_revision: u64,
        committed_revision: u64,
        candidate: ClusterFabricConfig,
    ) -> ConfigStoreResult<ConfigSectionEvent> {
        let mut sanitized = Config::default();
        sanitized.cluster_fabric = candidate;
        sanitized.sanitize_cluster_fabric_for_disk();
        self.registry
            .cluster_fabric
            .adopt_committed_from_durable_base(
                expected_revision,
                committed_revision,
                ClusterFabricSection(sanitized.cluster_fabric.clone()),
            )
    }

    /// Adopt one exact compound-transaction section while the shared
    /// migration lock still excludes a later cross-process writer.
    pub(crate) fn adopt_committed_section(
        &self,
        id: SectionId,
        expected_revision: u64,
        committed_revision: u64,
        candidate: &Config,
    ) -> ConfigStoreResult<ConfigSectionEvent> {
        let projection =
            SectionProjection::from_config(candidate, self.registry.projection().model_limits)?;
        macro_rules! adopt {
            ($field:ident, $candidate:expr) => {
                self.registry.$field.adopt_committed_from_durable_base(
                    expected_revision,
                    committed_revision,
                    $candidate,
                )
            };
        }
        match id {
            SectionId::Core => adopt!(core, projection.core),
            SectionId::Providers => adopt!(providers, projection.providers),
            SectionId::Mcp => adopt!(mcp, projection.mcp),
            SectionId::ToolsSkills => adopt!(tools_skills, projection.tools_skills),
            SectionId::Memory => adopt!(memory, projection.memory),
            SectionId::Subagents => adopt!(subagents, projection.subagents),
            SectionId::Notifications => adopt!(notifications, projection.notifications),
            SectionId::Connect => adopt!(connect, projection.connect),
            SectionId::ClusterFabric => adopt!(cluster_fabric, projection.cluster_fabric),
            SectionId::Env => adopt!(env, projection.env),
            SectionId::AccessControl => adopt!(access_control, projection.access_control),
            SectionId::Hooks => adopt!(hooks, projection.hooks),
            SectionId::ModelPolicy => adopt!(model_policy, projection.model_policy),
            SectionId::ModelLimits => adopt!(model_limits, projection.model_limits),
            SectionId::Credentials => Err(crate::ConfigStoreError::Validation(
                "credential snapshots require opaque credential adoption".to_string(),
            )),
        }
    }

    /// A semantic no-op can still name a durable owned generation newer than
    /// this process. Reload it while the caller holds the migration lock so
    /// the returned event and snapshot cannot be assembled from different
    /// cross-process generations.
    pub(crate) fn catch_up_committed_section(&self, id: SectionId) -> Option<ConfigSectionEvent> {
        self.registry.reload_if_changed(id)
    }

    pub(crate) fn adopt_captured_cluster_credentials(
        &self,
        document_lkg: CredentialDocumentLkg,
        statuses: Vec<CredentialStatus>,
        health: CredentialStoreHealth,
    ) -> ConfigStoreResult<()> {
        self.registry
            .credentials
            .adopt_captured(document_lkg, statuses, health)
    }

    pub(crate) fn adopt_captured_credentials_with_event(
        &self,
        document_lkg: CredentialDocumentLkg,
        statuses: Vec<CredentialStatus>,
        health: CredentialStoreHealth,
    ) -> ConfigStoreResult<Option<ConfigSectionEvent>> {
        self.registry
            .credentials
            .adopt_captured_with_event(document_lkg, statuses, health)
    }
}

/// Commit secret-free Core metadata against the exact durable Core generation
/// even when `process_facade` has not observed that generation yet.
///
/// The shared migration lock remains held from durable Core/credential
/// validation through the content-aware Core file CAS. A successful stale
/// process jumps directly to the committed revision; a failed attempt leaves
/// its facade untouched so the ordinary watcher can still adopt the durable
/// base later.
pub fn commit_core_metadata_from_durable_base(
    data_dir: impl AsRef<Path>,
    process_facade: &ConfigFacade,
    expected_revision: u64,
    candidate: Value,
) -> ConfigStoreResult<(ConfigSectionEvent, SectionEnvelope<Value>)> {
    validate_ordinary_section_json(&candidate)?;
    let candidate: CoreSection = serde_json::from_value(candidate)?;
    let data_dir = data_dir.as_ref();

    crate::credential_migration::with_provider_mcp_migration_lock(data_dir, || {
        crate::ensure_provider_mcp_migration_ready(data_dir)?;
        if !section_layout_is_active(data_dir)? {
            return Err(layout_not_committed());
        }

        let (mut durable_config, durable_envelope) =
            load_durable_effective_section_under_migration_lock(data_dir, SectionId::Core)?;
        if durable_envelope.revision != expected_revision {
            return Err(crate::ConfigStoreError::Conflict {
                expected: expected_revision,
                actual: durable_envelope.revision,
            });
        }
        let durable: CoreSection = serde_json::from_value(durable_envelope.data)?;
        if durable.proxy_auth_credential_ref != candidate.proxy_auth_credential_ref {
            return Err(crate::ConfigStoreError::Validation(
                "Core credential bindings are server-managed; use the proxy-auth API".to_string(),
            ));
        }

        let store = CredentialStore::open(data_dir);
        let (credential_lkg, _, credential_health) = store.snapshot_with_health_unchecked()?;
        if credential_health.status == SectionStatus::Degraded {
            return Err(crate::ConfigStoreError::Validation(
                "Core metadata mutation base is unavailable".to_string(),
            ));
        }
        durable_config.proxy_auth_credential_ref = candidate.proxy_auth_credential_ref.clone();
        crate::credential_migration::validate_active_section_credential_values(
            &durable_config,
            SectionId::Core,
            &credential_lkg,
        )?;

        let (event, envelope) = process_facade
            .registry
            .core
            .commit_from_durable_base_with_envelope(expected_revision, &durable, candidate)?;
        Ok((
            event,
            SectionEnvelope {
                data: serde_json::to_value(envelope.data)?,
                revision: envelope.revision,
                loaded_at: envelope.loaded_at,
                source_path: envelope.source_path,
                source_kind: envelope.source_kind,
                status: envelope.status,
                last_error: envelope.last_error,
            },
        ))
    })
}

/// Adopt the two historical plaintext Copilot OAuth caches into the encrypted
/// credential authority. The credential is committed first and the legacy
/// source is removed only if its bytes are still unchanged, making retries
/// safe after a crash at either boundary.
pub fn migrate_copilot_oauth_cache(data_dir: &Path) -> ConfigStoreResult<()> {
    for (name, reference, kind) in [
        (
            ".token",
            COPILOT_GITHUB_ACCESS_CREDENTIAL_REF,
            CopilotCacheKind::AccessToken,
        ),
        (
            ".copilot_token.json",
            COPILOT_CHAT_CONFIG_CREDENTIAL_REF,
            CopilotCacheKind::ChatConfig,
        ),
        (
            "copilot_token.json",
            COPILOT_CHAT_CONFIG_CREDENTIAL_REF,
            CopilotCacheKind::ChatConfig,
        ),
    ] {
        migrate_one_copilot_cache(data_dir, name, reference, kind)?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum CopilotCacheKind {
    AccessToken,
    ChatConfig,
}

fn migrate_one_copilot_cache(
    data_dir: &Path,
    name: &str,
    reference: &str,
    kind: CopilotCacheKind,
) -> ConfigStoreResult<()> {
    let path = data_dir.join(name);
    let metadata = match std::fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(crate::ConfigStoreError::Validation(
            "Copilot OAuth cache is not a regular file".to_string(),
        ));
    }
    let original = std::fs::read(&path)?;
    let text = std::str::from_utf8(&original).map_err(|_| {
        crate::ConfigStoreError::Validation("Copilot OAuth cache is invalid".to_string())
    })?;
    let secret = match kind {
        CopilotCacheKind::AccessToken => {
            let token = text.trim();
            if token.is_empty() {
                return Err(crate::ConfigStoreError::Validation(
                    "Copilot OAuth cache is empty".to_string(),
                ));
            }
            token.to_string()
        }
        CopilotCacheKind::ChatConfig => {
            let value: Value = serde_json::from_str(text)?;
            let object = value.as_object().ok_or_else(|| {
                crate::ConfigStoreError::Validation("Copilot OAuth cache is invalid".to_string())
            })?;
            if !object
                .get("token")
                .and_then(Value::as_str)
                .is_some_and(|token| !token.trim().is_empty())
                || !object.get("expires_at").is_some_and(Value::is_u64)
            {
                return Err(crate::ConfigStoreError::Validation(
                    "Copilot OAuth cache is invalid".to_string(),
                ));
            }
            serde_json::to_string(&value)?
        }
    };
    let reference = CredentialRef::parse(reference.to_string()).map_err(|_| {
        crate::ConfigStoreError::Validation("Copilot credential reference is invalid".to_string())
    })?;
    let store = CredentialStore::open(data_dir);
    let mut installed = false;
    for _ in 0..8 {
        if let Some(existing) = store.resolve(&reference)? {
            if existing.expose() != secret {
                return Err(crate::ConfigStoreError::Validation(
                    "Copilot OAuth cache conflicts with the credential store".to_string(),
                ));
            }
            installed = true;
            break;
        }
        let revision = store.revision()?;
        match store.replace(
            reference.clone(),
            &secret,
            CredentialSource::Migrated,
            revision,
        ) {
            Ok(_) => {
                installed = true;
                break;
            }
            Err(crate::ConfigStoreError::Conflict { .. }) => continue,
            Err(error) => return Err(error),
        }
    }
    if !installed {
        return Err(crate::ConfigStoreError::Validation(
            "Copilot credential store remained busy".to_string(),
        ));
    }
    if std::fs::read(&path)? == original {
        std::fs::remove_file(path)?;
    }
    Ok(())
}

/// Opaque runtime projection for the one section changed by a compatibility
/// write. It can install that owned section into an existing process config but
/// cannot expose or replace unrelated sections.
pub struct FacadeSectionRuntimeSnapshot {
    section: SectionId,
    runtime: Config,
}

impl FacadeSectionRuntimeSnapshot {
    pub fn install_into(self, target: &mut Config) {
        apply_runtime_section(self.section, &self.runtime, target);
    }
}

/// Exact process-adoption result captured for a compatibility write while the
/// shared migration lock still excluded a later writer.
pub struct FacadeConfigCommit {
    pub section_adoption: Option<ConfigStoreResult<ConfigSectionEvent>>,
    pub runtime: ConfigStoreResult<Option<FacadeSectionRuntimeSnapshot>>,
}

fn materialize_facade_commit_runtime_under_lock(
    data_dir: &Path,
    section: SectionId,
    mut runtime: Config,
) -> ConfigStoreResult<FacadeSectionRuntimeSnapshot> {
    let credentials = matches!(
        section,
        SectionId::Core
            | SectionId::Providers
            | SectionId::Mcp
            | SectionId::Notifications
            | SectionId::Connect
            | SectionId::ClusterFabric
            | SectionId::Env
            | SectionId::AccessControl
    )
    .then(|| CredentialStore::open(data_dir).snapshot_with_health_unchecked())
    .transpose()?
    .map(|(snapshot, _, _)| snapshot);
    match section {
        SectionId::Core => runtime.hydrate_proxy_auth_from_snapshot(
            credentials.as_ref().expect("credential-backed section"),
        )?,
        SectionId::Providers => runtime.hydrate_provider_credentials_from_snapshot(
            credentials.as_ref().expect("credential-backed section"),
        )?,
        SectionId::Mcp => runtime.hydrate_mcp_credentials_from_snapshot(
            credentials.as_ref().expect("credential-backed section"),
        )?,
        SectionId::Notifications => runtime.hydrate_notification_credentials_from_snapshot(
            credentials.as_ref().expect("credential-backed section"),
        )?,
        SectionId::Connect => runtime.hydrate_connect_credentials_from_snapshot(
            credentials.as_ref().expect("credential-backed section"),
        )?,
        SectionId::ClusterFabric => runtime.hydrate_cluster_credentials_from_snapshot(
            credentials.as_ref().expect("credential-backed section"),
        )?,
        SectionId::Env => runtime.hydrate_env_var_credentials_from_snapshot(
            credentials.as_ref().expect("credential-backed section"),
        )?,
        SectionId::AccessControl => runtime.hydrate_access_control_credentials_from_snapshot(
            credentials.as_ref().expect("credential-backed section"),
        )?,
        SectionId::ToolsSkills
        | SectionId::Memory
        | SectionId::Subagents
        | SectionId::Hooks
        | SectionId::ModelPolicy
        | SectionId::ModelLimits => {}
        SectionId::Credentials => {
            return Err(crate::ConfigStoreError::Validation(
                "compatibility config cannot own the credential section".to_string(),
            ))
        }
    }
    runtime.apply_runtime_env_overrides();
    Ok(FacadeSectionRuntimeSnapshot { section, runtime })
}

fn exact_section_credential_bindings(
    projection: &SectionProjection,
    section: SectionId,
) -> Vec<(String, Option<String>)> {
    let reference = |reference: &Option<crate::CredentialRef>| {
        reference
            .as_ref()
            .map(|reference| reference.as_str().to_string())
    };
    let mut bindings = match section {
        SectionId::Core => vec![(
            "proxy.default.auth".to_string(),
            reference(&projection.core.proxy_auth_credential_ref),
        )],
        SectionId::Env => projection
            .env
            .0
            .iter()
            .map(|entry| {
                (
                    format!("env.{}", entry.name),
                    reference(&entry.credential_ref),
                )
            })
            .collect(),
        SectionId::Notifications => vec![
            (
                "notifications.ntfy".to_string(),
                reference(&projection.notifications.notifications.ntfy.credential_ref),
            ),
            (
                "notifications.bark".to_string(),
                reference(&projection.notifications.notifications.bark.credential_ref),
            ),
        ],
        SectionId::Connect => projection
            .connect
            .0
            .platforms
            .iter()
            .enumerate()
            .flat_map(|(index, platform)| {
                let owner = platform
                    .id
                    .clone()
                    .unwrap_or_else(|| format!("index-{index}:{}", platform.platform_type));
                [
                    (
                        format!("connect.{owner}.token"),
                        reference(&platform.token_credential_ref),
                    ),
                    (
                        format!("connect.{owner}.app_secret"),
                        reference(&platform.app_secret_credential_ref),
                    ),
                ]
            })
            .collect(),
        SectionId::AccessControl => projection
            .access_control
            .0
            .as_ref()
            .map(|access| {
                std::iter::once((
                    "access.root.password".to_string(),
                    reference(&access.password_credential_ref),
                ))
                .chain(access.devices.iter().map(|device| {
                    (
                        format!("access.device.{}", device.device_id),
                        reference(&device.token_credential_ref),
                    )
                }))
                .collect()
            })
            .unwrap_or_default(),
        _ => Vec::new(),
    };
    bindings.sort();
    bindings
}

fn persist_facade_effective_config_inner(
    data_dir: impl AsRef<Path>,
    config: &Config,
    process_facade: Option<&ConfigFacade>,
) -> ConfigStoreResult<(Vec<ConfigSectionEvent>, Option<FacadeConfigCommit>)> {
    let data_dir = data_dir.as_ref();
    let facade = ConfigFacade::open(data_dir)?;
    let durable = facade.registry.projection();
    // A process-owned facade is the immutable source generation from which
    // AppState built this compatibility candidate. Comparing against that
    // source, rather than the freshly opened durable facade, distinguishes the
    // caller's one intended section change from unrelated external winners.
    let source = process_facade
        .map(|facade| facade.registry.projection())
        .unwrap_or_else(|| durable.clone());
    let mut candidate = SectionProjection::from_config(config, source.model_limits.clone())?;
    // The external broker is owned by broker.json / the migration planner and
    // is not part of the compatibility Config wire shape. Preserve its current
    // source projection across unrelated root writes.
    candidate.subagents.external_broker = source.subagents.external_broker.clone();
    // Compatibility/root writers never own cluster CAS. It is excluded from
    // the caller's intended changes; the dedicated cluster transaction remains
    // its sole write authority.
    candidate.cluster_fabric = source.cluster_fabric.clone();
    validate_projection(&candidate)?;

    crate::credential_migration::with_provider_mcp_migration_lock(data_dir, || {
        if !section_layout_is_active(data_dir)? {
            return Err(layout_not_committed());
        }
        let mut changed = Vec::<SectionId>::new();
        macro_rules! record_change {
            ($id:ident, $field:ident) => {
                if serde_json::to_value(&source.$field)? != serde_json::to_value(&candidate.$field)?
                {
                    changed.push(SectionId::$id);
                }
            };
        }
        record_change!(Core, core);
        record_change!(Providers, providers);
        record_change!(Mcp, mcp);
        record_change!(ToolsSkills, tools_skills);
        record_change!(Memory, memory);
        record_change!(Subagents, subagents);
        record_change!(Notifications, notifications);
        record_change!(Connect, connect);
        record_change!(Env, env);
        record_change!(AccessControl, access_control);
        record_change!(Hooks, hooks);
        record_change!(ModelPolicy, model_policy);
        if changed.len() > 1 {
            return Err(crate::ConfigStoreError::Validation(format!(
                "a compatibility write changed multiple sections ({}); split the request",
                changed
                    .iter()
                    .map(|section| section.descriptor().name)
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
        let Some(section) = changed.into_iter().next() else {
            let process_commit = process_facade.map(|_| FacadeConfigCommit {
                section_adoption: None,
                runtime: Ok(None),
            });
            return Ok((Vec::new(), process_commit));
        };
        let exact_credential_section = matches!(
            section,
            SectionId::Core
                | SectionId::Env
                | SectionId::Notifications
                | SectionId::Connect
                | SectionId::AccessControl
        );
        if process_facade.is_none() && exact_credential_section {
            return Err(crate::ConfigStoreError::Validation(format!(
                "{} must be changed through its dedicated revisioned API",
                section.descriptor().name
            )));
        }
        if exact_credential_section
            && exact_section_credential_bindings(&source, section)
                != exact_section_credential_bindings(&candidate, section)
        {
            return Err(crate::ConfigStoreError::Validation(format!(
                "{} credential bindings are server-managed; use its dedicated revisioned API",
                section.descriptor().name
            )));
        }

        let source_registry = process_facade
            .map(|facade| facade.registry.as_ref())
            .unwrap_or(facade.registry.as_ref());
        let (expected_revision, source_status, source_kind, _, source_value) =
            source_registry.observation(section);
        let (actual_revision, durable_status, durable_kind, _, durable_value) =
            facade.registry.observation(section);
        if expected_revision != actual_revision {
            return Err(crate::ConfigStoreError::Conflict {
                expected: expected_revision,
                actual: actual_revision,
            });
        }
        if source_value != durable_value {
            return Err(crate::ConfigStoreError::Validation(format!(
                "{} changed without advancing revision {expected_revision}",
                section.descriptor().name
            )));
        }
        if exact_credential_section
            && (source_status != SectionStatus::Healthy
                || source_kind != SectionSourceKind::File
                || durable_status != SectionStatus::Healthy
                || durable_kind != SectionSourceKind::File)
        {
            return Err(crate::ConfigStoreError::Validation(format!(
                "{} compatibility mutation base is unavailable",
                section.descriptor().name
            )));
        }
        if exact_credential_section {
            let store = CredentialStore::open(data_dir);
            let (credential_lkg, _, credential_health) = store.snapshot_with_health_unchecked()?;
            if credential_health.status == SectionStatus::Degraded {
                return Err(crate::ConfigStoreError::Validation(format!(
                    "{} compatibility mutation base is unavailable",
                    section.descriptor().name
                )));
            }
            crate::credential_migration::validate_active_section_credential_values(
                &candidate.clone().into_config(),
                section,
                &credential_lkg,
            )?;
        }

        let event = match section {
            SectionId::Core => facade
                .registry
                .core
                .commit(expected_revision, candidate.core)?,
            SectionId::Providers => facade
                .registry
                .providers
                .commit(expected_revision, candidate.providers)?,
            SectionId::Mcp => facade
                .registry
                .mcp
                .commit(expected_revision, candidate.mcp)?,
            SectionId::ToolsSkills => facade
                .registry
                .tools_skills
                .commit(expected_revision, candidate.tools_skills)?,
            SectionId::Memory => facade
                .registry
                .memory
                .commit(expected_revision, candidate.memory)?,
            SectionId::Subagents => facade
                .registry
                .subagents
                .commit(expected_revision, candidate.subagents)?,
            SectionId::Notifications => facade
                .registry
                .notifications
                .commit(expected_revision, candidate.notifications)?,
            SectionId::Connect => facade
                .registry
                .connect
                .commit(expected_revision, candidate.connect)?,
            SectionId::ClusterFabric => {
                return Err(crate::ConfigStoreError::Validation(
                    "cluster-fabric requires the dedicated revisioned API".to_string(),
                ))
            }
            SectionId::Env => facade
                .registry
                .env
                .commit(expected_revision, candidate.env)?,
            SectionId::AccessControl => facade
                .registry
                .access_control
                .commit(expected_revision, candidate.access_control)?,
            SectionId::Hooks => facade
                .registry
                .hooks
                .commit(expected_revision, candidate.hooks)?,
            SectionId::ModelPolicy => facade
                .registry
                .model_policy
                .commit(expected_revision, candidate.model_policy)?,
            SectionId::ModelLimits => {
                return Err(crate::ConfigStoreError::Validation(
                    "model limits require their dedicated persistence path".to_string(),
                ))
            }
            SectionId::Credentials => {
                return Err(crate::ConfigStoreError::Validation(
                    "compatibility config cannot own the credential section".to_string(),
                ))
            }
        };
        let committed_revision = match &event {
            ConfigSectionEvent::Changed { revision, .. }
            | ConfigSectionEvent::Recovered { revision, .. }
            | ConfigSectionEvent::Invalid { revision, .. } => *revision,
        };
        let events = vec![event];
        let committed = Some((section, expected_revision, committed_revision));

        let process_commit = process_facade.map(|process_facade| {
            let Some((section, expected_revision, committed_revision)) = committed else {
                return FacadeConfigCommit {
                    section_adoption: None,
                    runtime: Ok(None),
                };
            };
            let exact = facade.effective_config();
            let runtime =
                materialize_facade_commit_runtime_under_lock(data_dir, section, exact).map(Some);
            // A failed exact runtime must leave the process facade behind the
            // durable revision. Its watcher can then observe and retry that
            // generation instead of treating an uninstalled commit as current.
            let section_adoption = runtime.as_ref().ok().map(|_| {
                process_facade.adopt_committed_section(
                    section,
                    expected_revision,
                    committed_revision,
                    &facade.effective_config(),
                )
            });
            FacadeConfigCommit {
                section_adoption,
                runtime,
            }
        });
        Ok((events, process_commit))
    })
}

/// Persist a compatibility [`Config`] through the active modular authorities
/// and return the durable section events without adopting a process facade.
pub fn persist_facade_effective_config(
    data_dir: impl AsRef<Path>,
    config: &Config,
) -> ConfigStoreResult<Vec<ConfigSectionEvent>> {
    persist_facade_effective_config_inner(data_dir, config, None).map(|(events, _)| events)
}

/// Persist a compatibility [`Config`] through the active modular authorities
/// and capture the exact changed-section runtime for `process_facade`.
///
/// This is the bridge used by legacy root PATCH/CLI callers during the
/// compatibility window: `config.json` is never recreated after activation,
/// unchanged sections retain their own revisions, and the caller can install
/// only the section it actually committed.
pub fn persist_facade_effective_config_with_adoption(
    data_dir: impl AsRef<Path>,
    config: &Config,
    process_facade: &ConfigFacade,
) -> ConfigStoreResult<FacadeConfigCommit> {
    let (_, commit) =
        persist_facade_effective_config_inner(data_dir, config, Some(process_facade))?;
    commit.ok_or_else(|| {
        crate::ConfigStoreError::Validation(
            "compatibility commit omitted process adoption result".to_string(),
        )
    })
}

/// Compare two effective configs using the same section projection as the
/// modular persistence facade. Runtime-only hydrated secret fields are
/// sanitized by the projection and therefore do not create false changes.
pub fn changed_facade_sections(
    current: &Config,
    candidate: &Config,
) -> ConfigStoreResult<Vec<SectionId>> {
    let current = SectionProjection::from_config(current, ModelLimitsSection::default())?;
    let candidate = SectionProjection::from_config(candidate, ModelLimitsSection::default())?;
    let mut changed = Vec::new();
    macro_rules! compare {
        ($id:ident, $field:ident) => {
            if serde_json::to_value(&current.$field)? != serde_json::to_value(&candidate.$field)? {
                changed.push(SectionId::$id);
            }
        };
    }
    compare!(Core, core);
    compare!(Providers, providers);
    compare!(Mcp, mcp);
    compare!(ToolsSkills, tools_skills);
    compare!(Memory, memory);
    compare!(Subagents, subagents);
    compare!(Notifications, notifications);
    compare!(Connect, connect);
    compare!(ClusterFabric, cluster_fabric);
    compare!(Env, env);
    compare!(AccessControl, access_control);
    compare!(Hooks, hooks);
    compare!(ModelPolicy, model_policy);
    Ok(changed)
}

pub(crate) fn provider_section_value(config: &Config) -> ConfigStoreResult<Value> {
    Ok(serde_json::to_value(
        SectionProjection::from_config(config, ModelLimitsSection::default())?.providers,
    )?)
}

fn validate_projection(projection: &SectionProjection) -> ConfigStoreResult<()> {
    for result in [
        validate_core(&projection.core),
        validate_providers(&projection.providers),
        validate_mcp(&projection.mcp),
        validate_tools_skills(&projection.tools_skills),
        validate_memory(&projection.memory),
        validate_subagents(&projection.subagents),
        validate_notifications(&projection.notifications),
        validate_connect(&projection.connect),
        validate_cluster_fabric(&projection.cluster_fabric),
        validate_env(&projection.env),
        validate_access_control(&projection.access_control),
        validate_hooks(&projection.hooks),
        validate_model_policy(&projection.model_policy),
        validate_model_limits(&projection.model_limits),
    ] {
        result.map_err(crate::ConfigStoreError::Validation)?;
    }
    Ok(())
}

fn capture_layout_member_states(
    data_dir: &Path,
) -> ConfigStoreResult<BTreeMap<String, Option<Vec<u8>>>> {
    SECTION_DESCRIPTORS
        .iter()
        .map(|descriptor| {
            let bytes = crate::credential_migration::read_section_attestation_target(
                &data_dir.join(descriptor.file_name),
            )?;
            Ok((descriptor.file_name.to_string(), bytes))
        })
        .collect()
}

fn layout_not_committed() -> crate::ConfigStoreError {
    crate::ConfigStoreError::Validation("modular configuration layout is not committed".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;
    use serde_json::json;
    use std::collections::BTreeSet;
    use tempfile::TempDir;

    fn write_json(path: &Path, value: &Value) {
        std::fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
    }

    #[test]
    fn codex_auth_mode_matrix_validation_fails_closed() {
        let dormant_invalid = SubagentsConfig {
            codex_auth_mode: Some(crate::CodexAuthMode::Custom),
            codex_base_url: Some("not a URL".to_string()),
            ..Default::default()
        };
        assert!(validate_codex_subagents_config(&dormant_invalid)
            .unwrap_err()
            .contains("absolute HTTP(S) URL"));

        let mut config = SubagentsConfig {
            executor: Some("codex".to_string()),
            ..Default::default()
        };

        // Unset means Bamboo-as-provider; it needs no persisted URL or secret.
        assert!(validate_codex_subagents_config(&config).is_ok());

        config.codex_auth_mode = Some(crate::CodexAuthMode::ApiKey);
        assert!(validate_codex_subagents_config(&config)
            .unwrap_err()
            .contains("requires OPENAI_API_KEY"));
        config.codex_forward_env = Some(vec!["OPENAI_API_KEY".to_string()]);
        assert!(validate_codex_subagents_config(&config).is_ok());

        config.codex_auth_mode = Some(crate::CodexAuthMode::Inherit);
        assert!(validate_codex_subagents_config(&config)
            .unwrap_err()
            .contains("only be forwarded"));
        config.codex_forward_env = Some(vec!["CODEX_HOME".to_string()]);
        assert!(validate_codex_subagents_config(&config)
            .unwrap_err()
            .contains("reserved Codex variables"));

        config.codex_auth_mode = Some(crate::CodexAuthMode::Custom);
        config.codex_forward_env = None;
        assert!(validate_codex_subagents_config(&config)
            .unwrap_err()
            .contains("requires codex_base_url"));
        config.codex_base_url = Some("https://provider.example/v1".to_string());
        assert!(validate_codex_subagents_config(&config)
            .unwrap_err()
            .contains("requires codex_provider_key_ref"));
        config.codex_provider_key_ref = Some(
            crate::CredentialRef::parse("provider.codex.api_key")
                .expect("valid credential reference"),
        );
        assert!(validate_codex_subagents_config(&config).is_ok());

        config.codex_base_url = Some("https://user:secret@provider.example/v1?leak=1".to_string());
        assert!(validate_codex_subagents_config(&config)
            .unwrap_err()
            .contains("must not contain credentials"));

        config.codex_base_url = Some("https://provider.example/v1".to_string());
        config.codex_sandbox = Some(crate::CodexSandbox::ReadOnly);
        config.codex_network_access = Some(true);
        assert!(validate_codex_subagents_config(&config)
            .unwrap_err()
            .contains("workspace-write"));

        config.codex_network_access = None;
        config.codex_sandbox = Some(crate::CodexSandbox::DangerFullAccess);
        assert!(validate_codex_subagents_config(&config)
            .unwrap_err()
            .contains("codex_allow_danger_bypass"));
        config.codex_allow_danger_bypass = Some(true);
        assert!(validate_codex_subagents_config(&config).is_ok());
    }

    #[test]
    fn codex_transport_mode_requires_matching_approval_semantics() {
        let mut config = SubagentsConfig {
            executor: Some("codex".to_string()),
            codex_mode: Some(crate::CodexMode::AppServer),
            ..Default::default()
        };
        assert!(validate_codex_subagents_config(&config).is_ok());

        config.codex_approval_policy = Some(crate::CodexApprovalPolicy::Never);
        assert!(validate_codex_subagents_config(&config)
            .unwrap_err()
            .contains("requires codex_approval_policy = on-request"));

        config.codex_mode = Some(crate::CodexMode::Exec);
        config.codex_approval_policy = Some(crate::CodexApprovalPolicy::OnRequest);
        assert!(validate_codex_subagents_config(&config)
            .unwrap_err()
            .contains("exec mode has no approval relay"));
    }

    fn directory_snapshot(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
        fn visit(root: &Path, current: &Path, snapshot: &mut BTreeMap<PathBuf, Option<Vec<u8>>>) {
            let mut entries = std::fs::read_dir(current)
                .unwrap()
                .map(Result::unwrap)
                .collect::<Vec<_>>();
            entries.sort_by_key(std::fs::DirEntry::file_name);
            for entry in entries {
                let path = entry.path();
                let relative = path.strip_prefix(root).unwrap().to_path_buf();
                if entry.file_type().unwrap().is_dir() {
                    snapshot.insert(relative, None);
                    visit(root, &path, snapshot);
                } else {
                    snapshot.insert(relative, Some(std::fs::read(path).unwrap()));
                }
            }
        }

        let mut snapshot = BTreeMap::new();
        visit(root, root, &mut snapshot);
        snapshot
    }

    fn assert_no_plaintext_in_persistent_files(root: &Path, forbidden: &[&str]) {
        fn visit(root: &Path, current: &Path, forbidden: &[&str]) {
            for entry in std::fs::read_dir(current).unwrap().map(Result::unwrap) {
                let path = entry.path();
                if entry.file_type().unwrap().is_dir() {
                    visit(root, &path, forbidden);
                    continue;
                }
                if path.extension().and_then(|extension| extension.to_str()) == Some("lock") {
                    continue;
                }
                let bytes = std::fs::read(&path).unwrap();
                for sentinel in forbidden {
                    assert!(
                        !bytes
                            .windows(sentinel.len())
                            .any(|window| window == sentinel.as_bytes()),
                        "{} leaked plaintext sentinel {sentinel}",
                        path.strip_prefix(root).unwrap().display()
                    );
                }
            }
        }

        visit(root, root, forbidden);
    }

    fn section_document(revision: u64, data: Value) -> Value {
        json!({
            "schema_version": SECTION_SCHEMA_VERSION,
            "revision": revision,
            "data": data,
        })
    }

    fn assert_no_layout_transaction_artifacts(dir: &Path) {
        for name in [
            "credentials.json",
            "config-credential-migration.json",
            "config-credential-migration.journal.json",
            crate::credential_migration::SECTION_LAYOUT_COMPLETION_FILE,
            SECTION_LAYOUT_FILE,
        ] {
            assert!(!dir.join(name).exists(), "unexpected artifact: {name}");
        }
        for descriptor in SECTION_DESCRIPTORS
            .iter()
            .filter(|descriptor| descriptor.id != SectionId::Credentials)
        {
            assert!(
                !dir.join(descriptor.file_name).exists(),
                "unexpected section: {}",
                descriptor.file_name
            );
        }
    }

    #[test]
    fn descriptors_cover_the_accepted_adr_mapping_once() {
        let ids = SECTION_DESCRIPTORS
            .iter()
            .map(|descriptor| descriptor.id)
            .collect::<BTreeSet<_>>();
        let files = SECTION_DESCRIPTORS
            .iter()
            .map(|descriptor| descriptor.file_name)
            .collect::<BTreeSet<_>>();
        assert_eq!(ids.len(), 15);
        assert_eq!(files.len(), 15);
        assert_eq!(SectionId::Core.descriptor().file_name, "core.json");
        assert_eq!(
            SectionId::ClusterFabric.descriptor().file_name,
            "cluster-fabric.json"
        );
        assert_eq!(
            SectionId::ModelLimits.descriptor().file_name,
            "model_limits.json"
        );
        assert!(SectionId::Credentials.descriptor().sensitive);
        assert!(SECTION_DESCRIPTORS
            .iter()
            .filter(|descriptor| descriptor.sensitive)
            .all(|descriptor| descriptor.id == SectionId::Credentials));
    }

    #[test]
    fn config_value_ownership_is_unique_and_explicit() {
        let names = CONFIG_VALUE_FIELD_OWNERS
            .iter()
            .map(|(name, _)| *name)
            .collect::<BTreeSet<_>>();
        assert_eq!(names.len(), CONFIG_VALUE_FIELD_OWNERS.len());
        for required in [
            "connect",
            "plugin_trust",
            "default_work_area",
            "run_budget",
            "stream_timeout",
            "lifecycle_hooks",
        ] {
            assert!(names.contains(required), "missing owner for {required}");
        }
    }

    #[test]
    fn projection_round_trips_every_safe_compatibility_field_and_unknown_key() {
        let mut config: Config = serde_json::from_value(json!({
            "http_proxy": "http://proxy.test",
            "headless_auth": true,
            "server": {"port": 9876},
            "provider": "gemini",
            "features": {"dynamic_model_routing": true},
            "tools": {"disabled": ["bash"]},
            "skills": {"disabled": ["private"]},
            "env_vars": [{"name": "VISIBLE", "value": "ok"}],
            "default_work_area": {"path": "/workspace"},
            "access_control": {"password_enabled": true, "password_hash": "hash"},
            "run_budget": {"max_tool_calls": 4},
            "stream_timeout": {
                "transport_idle_timeout_secs": 30,
                "first_semantic_timeout_secs": 45,
                "semantic_idle_timeout_secs": 60
            },
            "notifications": {"desktop": {"enabled": true}},
            "connect": {"platforms": []},
            "plugin_trust": {"enforcement": "off"},
            "lifecycle_hooks": {
                "enabled": true,
                "PreToolUse": [{
                    "matcher": "bash",
                    "hooks": [{"type": "command", "command": "guard.sh"}]
                }]
            },
            "memory": {"background_model": "memory-model"},
            "subagents": {"max_concurrent": 3},
            "providers": {"openai": {"model": "gpt-test"}},
            "future_root_key": {"nested": true}
        }))
        .unwrap();
        config.subagents_mut().broker = Some(BrokerClientConfig {
            endpoint: "ws://broker.test".to_string(),
            credential_ref: Some(crate::CredentialRef::parse("broker.external.token").unwrap()),
            configured: true,
            ..BrokerClientConfig::default()
        });

        let projection = SectionProjection::from_config(
            &config,
            ModelLimitsSection(vec![json!({"model_pattern": "gpt-*", "max_tokens": 42})]),
        )
        .unwrap();
        assert_eq!(projection.core.extra["future_root_key"]["nested"], true);
        let round_trip = projection.into_config();
        let value = round_trip.to_compatibility_value().unwrap();
        assert_eq!(value["http_proxy"], "http://proxy.test");
        assert_eq!(value["server"]["port"], 9876);
        assert_eq!(value["provider"], "gemini");
        assert_eq!(value["tools"]["disabled"][0], "bash");
        assert_eq!(value["default_work_area"]["path"], "/workspace");
        assert_eq!(value["run_budget"]["max_tool_calls"], 4);
        assert_eq!(value["plugin_trust"]["enforcement"], "off");
        assert_eq!(value["lifecycle_hooks"]["enabled"], true);
        assert_eq!(
            value["lifecycle_hooks"]["PreToolUse"][0]["hooks"][0]["command"],
            "guard.sh"
        );
        assert!(round_trip.connect.platforms.is_empty());
        assert_eq!(value["future_root_key"]["nested"], true);
        assert_eq!(
            round_trip
                .memory()
                .as_ref()
                .unwrap()
                .background_model
                .as_deref(),
            Some("memory-model")
        );
        assert_eq!(round_trip.subagents().max_concurrent, Some(3));
        assert!(
            round_trip.subagents().broker.is_none(),
            "runtime broker endpoints must not enter a durable projection"
        );
    }

    #[test]
    fn registry_opens_all_sections_with_independent_missing_health() {
        let dir = TempDir::new().unwrap();
        let registry = SectionRegistry::open(dir.path()).unwrap();
        let health = registry.health().unwrap();
        assert_eq!(health.len(), 15);
        assert_eq!(
            health
                .iter()
                .map(|entry| entry.section)
                .collect::<BTreeSet<_>>()
                .len(),
            15
        );
        assert!(health
            .iter()
            .all(|entry| entry.status == SectionStatus::Missing && entry.revision == 0));
    }

    #[test]
    fn registry_rejects_unknown_raw_credentials_before_typed_materialization() {
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("core.json"),
            &section_document(1, json!({"token": "must-not-be-dropped"})),
        );

        let rejected = SectionRegistry::open(dir.path()).unwrap();
        let rejected_snapshot = rejected.core.snapshot();
        assert_eq!(rejected_snapshot.revision, 0);
        assert_eq!(rejected_snapshot.status, SectionStatus::Invalid);
        assert!(!dir.path().join("core.json.corrupt").exists());

        write_json(
            &dir.path().join("core.json"),
            &section_document(1, json!({"http_proxy": "http://proxy.test"})),
        );
        let registry = SectionRegistry::open(dir.path()).unwrap();
        write_json(
            &dir.path().join("core.json"),
            &section_document(
                2,
                json!({
                    "http_proxy": "http://changed.test",
                    "token": "must-not-be-dropped"
                }),
            ),
        );

        assert!(matches!(
            registry.core.reload(),
            crate::ConfigSectionEvent::Invalid { revision: 1, .. }
        ));
        let retained = registry.core.snapshot();
        assert_eq!(retained.revision, 1);
        assert_eq!(retained.data.http_proxy, "http://proxy.test");
        assert_eq!(retained.status, SectionStatus::Invalid);
    }

    #[test]
    fn facade_open_rejects_a_section_swap_after_registry_materialization() {
        let _key = crate::encryption::set_test_encryption_key([95; 32]);
        let dir = TempDir::new().unwrap();
        migrate_config_facade_layout(dir.path()).unwrap();
        let path = dir.path().join("core.json");
        let original = std::fs::read(&path).unwrap();
        let mut swapped: Value = serde_json::from_slice(&original).unwrap();
        swapped["data"]["http_proxy"] = json!("http://same-revision-swap.test");
        let swapped = serde_json::to_vec_pretty(&swapped).unwrap();

        let error = ConfigFacade::open_stable(dir.path(), |_| {
            std::fs::write(&path, &swapped).unwrap();
        })
        .err()
        .unwrap();
        assert!(matches!(error, crate::ConfigStoreError::Validation(_)));

        std::fs::write(&path, &original).unwrap();
        assert!(ConfigFacade::open(dir.path()).is_ok());
    }

    #[test]
    fn facade_startup_migrates_copilot_oauth_caches_before_runtime_reads() {
        let _key = crate::encryption::set_test_encryption_key([0x98; 32]);
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join(".token"), "github-oauth-secret\n").unwrap();
        write_json(
            &dir.path().join(".copilot_token.json"),
            &json!({
                "token": "copilot-chat-secret",
                "expires_at": 4_000_000_000u64,
                "refresh_in": 120
            }),
        );

        ConfigFacade::open_or_migrate(dir.path()).unwrap();

        assert!(!dir.path().join(".token").exists());
        assert!(!dir.path().join(".copilot_token.json").exists());
        let store = CredentialStore::open(dir.path());
        let github =
            CredentialRef::parse(COPILOT_GITHUB_ACCESS_CREDENTIAL_REF.to_string()).unwrap();
        let chat = CredentialRef::parse(COPILOT_CHAT_CONFIG_CREDENTIAL_REF.to_string()).unwrap();
        assert_eq!(
            store.resolve(&github).unwrap().unwrap().expose(),
            "github-oauth-secret"
        );
        let chat = store.resolve(&chat).unwrap().unwrap();
        assert!(chat.expose().contains("copilot-chat-secret"));
        let durable = std::fs::read_to_string(dir.path().join("credentials.json")).unwrap();
        assert!(!durable.contains("github-oauth-secret"));
        assert!(!durable.contains("copilot-chat-secret"));
    }

    #[test]
    fn providers_section_remains_readable_by_existing_exact_transaction_wire_type() {
        let section = ProvidersSection {
            provider: Some("openai".to_string()),
            features: FeatureFlags {
                dynamic_model_routing: true,
                ..FeatureFlags::default()
            },
            ..ProvidersSection::default()
        };
        let bytes = serde_json::to_vec(&section).unwrap();
        let legacy: ProviderConfigs = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(legacy.extra["provider"], "openai");
        assert_eq!(legacy.extra["features"]["dynamic_model_routing"], true);
        let reparsed: ProvidersSection = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(reparsed.provider.as_deref(), Some("openai"));
        assert!(reparsed.features.dynamic_model_routing);
    }

    #[test]
    fn instance_native_projection_omits_legacy_provider_routing() {
        let mut config = Config::default();
        config.provider = "anthropic".to_string();
        config.providers_mut().openai = Some(crate::OpenAIConfig::default());
        config.providers_mut().anthropic = Some(crate::AnthropicConfig::default());
        config.providers_mut().gemini = Some(crate::GeminiConfig::default());
        config.providers_mut().copilot = Some(crate::CopilotConfig::default());
        config.providers_mut().bodhi = Some(serde_json::from_value(json!({})).unwrap());
        config
            .providers_mut()
            .extra
            .insert("future_provider".to_string(), json!({"kept": true}));
        config.provider_instances.insert(
            "work".to_string(),
            serde_json::from_value(json!({
                "provider_type": "openai",
                "enabled": true
            }))
            .unwrap(),
        );
        config.default_provider_instance = Some("work".to_string());

        let projection =
            SectionProjection::from_config(&config, ModelLimitsSection::default()).unwrap();
        let durable = serde_json::to_value(&projection.providers).unwrap();

        assert!(durable.get("provider").is_none());
        for key in ["openai", "anthropic", "gemini", "copilot", "bodhi"] {
            assert!(durable.get(key).is_none(), "persisted legacy alias {key}");
        }
        assert_eq!(durable["future_provider"]["kept"], true);
        assert_eq!(durable["default_provider_instance"], "work");
        assert!(durable["provider_instances"]["work"].is_object());
    }

    #[test]
    fn legacy_split_is_manifest_gated_idempotent_and_preserves_sidecar_precedence() {
        let _key = crate::encryption::set_test_encryption_key([61; 32]);
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({
                "http_proxy": "http://legacy-proxy",
                "memory": {"background_model": "inline-memory"},
                "subagents": {"max_concurrent": 2},
                "providers": {"openai": {"model": "inline-provider"}},
                "mcpServers": {"inline": {"command": "inline-command"}},
                "future_root_key": {"kept": true}
            }),
        );
        write_json(
            &dir.path().join("memory.json"),
            &json!({"background_model": "sidecar-memory"}),
        );
        write_json(
            &dir.path().join("providers.json"),
            &json!({"openai": {"model": "sidecar-provider"}}),
        );
        write_json(
            &dir.path().join("model_limits.json"),
            &json!([{"model_pattern": "future-*", "custom": {"kept": true}}]),
        );
        write_json(
            &dir.path().join("mcp.json"),
            &json!({"sidecar": {"command": "sidecar-command"}}),
        );

        let first = migrate_config_facade_layout(dir.path()).unwrap();
        assert!(first.activated);
        assert!(section_layout_is_active(dir.path()).unwrap());
        let facade = ConfigFacade::open(dir.path()).unwrap();
        let effective = facade.effective_config();
        assert_eq!(
            effective
                .memory()
                .as_ref()
                .unwrap()
                .background_model
                .as_deref(),
            Some("sidecar-memory")
        );
        assert_eq!(
            effective
                .providers()
                .openai
                .as_ref()
                .unwrap()
                .model
                .as_deref(),
            Some("sidecar-provider")
        );
        assert_eq!(effective.extra["future_root_key"]["kept"], true);
        assert_eq!(effective.mcp.servers.len(), 1);
        assert_eq!(effective.mcp.servers[0].id, "sidecar");
        assert_eq!(
            facade.registry().model_limits.snapshot().data.0[0]["custom"]["kept"],
            true
        );
        assert_eq!(facade.registry().health().unwrap().len(), 15);

        // Provider/MCP section data remains consumable by the exact
        // credential transaction's existing wire types.
        let providers = AtomicJsonStore::<ProviderConfigs>::new(
            dir.path().join("providers.json"),
            SECTION_SCHEMA_VERSION,
        )
        .load()
        .unwrap()
        .unwrap();
        assert_eq!(
            providers.data.openai.unwrap().model.as_deref(),
            Some("sidecar-provider")
        );
        AtomicJsonStore::<bamboo_domain::mcp_config::McpConfig>::new(
            dir.path().join("mcp.json"),
            SECTION_SCHEMA_VERSION,
        )
        .load()
        .unwrap()
        .unwrap();

        let before = SECTION_DESCRIPTORS
            .iter()
            .map(|descriptor| {
                (
                    descriptor.file_name,
                    std::fs::read(dir.path().join(descriptor.file_name)).unwrap_or_default(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let second = migrate_config_facade_layout(dir.path()).unwrap();
        assert!(!second.activated);
        for (name, bytes) in before {
            assert_eq!(
                std::fs::read(dir.path().join(name)).unwrap_or_default(),
                bytes
            );
        }
    }

    #[test]
    fn incomplete_enabled_access_control_keeps_facade_available_and_reports_repair_state() {
        let _key = crate::encryption::set_test_encryption_key([0xa1; 32]);
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({
                "access_control": {
                    "password_enabled": true
                },
                "tools": {"disabled": ["bash"]},
                "mcp": {}
            }),
        );

        let facade = ConfigFacade::open_or_migrate(dir.path()).unwrap();
        let access = facade.registry().access_control.snapshot();
        let access = access.data.0.as_ref().unwrap();
        assert!(access.password_enabled);
        assert!(!access.password_configured);
        assert!(access.password_credential_ref.is_none());
        assert!(facade.registry().tools_skills.snapshot().status == SectionStatus::Healthy);
        assert!(facade.registry().mcp.snapshot().status == SectionStatus::Healthy);
        assert!(facade.registry().credentials.statuses().is_empty());

        let exact = crate::read_exact_credential_section_snapshot(
            dir.path(),
            SectionId::AccessControl,
            None,
        )
        .unwrap();
        assert_eq!(exact.section.status, SectionStatus::Degraded);
        assert_eq!(
            exact.section.last_error.as_deref(),
            Some("access-control credential repair is required")
        );

        let before = directory_snapshot(dir.path());
        let outcome = migrate_config_facade_layout(dir.path()).unwrap();
        assert!(!outcome.activated);
        assert_eq!(directory_snapshot(dir.path()), before);
    }

    #[test]
    fn corrupted_access_password_metadata_isolated_to_degraded_access_section() {
        let _key = crate::encryption::set_test_encryption_key([0xa4; 32]);
        for (label, access, forbidden) in [
            (
                "hash-only",
                json!({
                    "password_enabled": true,
                    "password_hash": "orphan-hash"
                }),
                "orphan-hash",
            ),
            (
                "salt-only",
                json!({
                    "password_enabled": true,
                    "password_salt": "orphan-salt"
                }),
                "orphan-salt",
            ),
            (
                "configured-without-ref",
                json!({
                    "password_enabled": true,
                    "password_configured": true
                }),
                "configured-without-ref",
            ),
            (
                "noncanonical-ref",
                json!({
                    "password_enabled": true,
                    "password_configured": true,
                    "password_credential_ref": "access.other.password_verifier"
                }),
                "access.other.password_verifier",
            ),
        ] {
            let dir = TempDir::new().unwrap();
            write_json(
                &dir.path().join("config.json"),
                &json!({
                    "access_control": access,
                    "tools": {"disabled": ["bash"]},
                    "mcp": {}
                }),
            );

            let facade = ConfigFacade::open_or_migrate(dir.path())
                .unwrap_or_else(|error| panic!("{label} must not disable the facade: {error}"));
            assert_eq!(
                facade.registry().tools_skills.snapshot().status,
                SectionStatus::Healthy,
                "{label}"
            );
            assert_eq!(
                facade.registry().mcp.snapshot().status,
                SectionStatus::Healthy,
                "{label}"
            );
            let exact = crate::read_exact_credential_section_snapshot(
                dir.path(),
                SectionId::AccessControl,
                None,
            )
            .unwrap();
            assert_eq!(exact.section.status, SectionStatus::Degraded, "{label}");
            assert_eq!(
                exact.section.last_error.as_deref(),
                Some("access-control credential repair is required"),
                "{label}"
            );
            let access: AccessControlSection = serde_json::from_value(exact.section.data).unwrap();
            let access = access.0.unwrap();
            assert!(access.password_enabled, "{label}");
            assert!(!access.password_configured, "{label}");
            assert!(access.password_credential_ref.is_none(), "{label}");
            assert!(access.password_hash.is_none(), "{label}");
            assert!(access.password_salt.is_none(), "{label}");
            if label != "configured-without-ref" {
                for name in ["config.json", "access-control.json", "credentials.json"] {
                    assert!(
                        !std::fs::read_to_string(dir.path().join(name))
                            .unwrap()
                            .contains(forbidden),
                        "{label} leaked repair material in {name}"
                    );
                }
            }
        }
    }

    #[test]
    fn malformed_access_fragments_are_encrypted_in_one_stable_recovery_slot() {
        let _key = crate::encryption::set_test_encryption_key([0xa5; 32]);
        let dir = TempDir::new().unwrap();
        let original_access = json!({
            "password_enabled": true,
            "password_hash": "root-hash-fragment",
            "password_salt": "root-salt-fragment",
            "devices": [{
                "device_id": "bamboo_0123456789ab",
                "label": "Damaged phone",
                "token_hash": "device-hash-fragment",
                "token_salt": "device-salt-fragment",
                "created_at": "2026-07-29T00:00:00Z"
            }],
            "future_private_fragment": "payload-private-fragment"
        });
        write_json(
            &dir.path().join("config.json"),
            &json!({
                "access_control": original_access,
                "tools": {"disabled": ["bash"]},
                "mcp": {}
            }),
        );

        let facade = ConfigFacade::open_or_migrate(dir.path()).unwrap();
        assert_eq!(
            facade.registry().tools_skills.snapshot().status,
            SectionStatus::Healthy
        );
        assert_eq!(
            facade.registry().mcp.snapshot().status,
            SectionStatus::Healthy
        );
        let access = facade.registry().access_control.snapshot();
        let access = access.data.0.as_ref().unwrap();
        assert!(access.password_enabled);
        assert!(access.repair_required);
        assert!(!access.password_configured);
        assert!(access.devices.is_empty());

        let recovery_ref = CredentialRef::parse("access_repair.root.payload".to_string()).unwrap();
        let store = CredentialStore::open(dir.path());
        let recovery = store.resolve(&recovery_ref).unwrap().unwrap();
        let payload: Value = serde_json::from_str(recovery.expose()).unwrap();
        assert_eq!(payload["schema_version"], 1);
        assert_eq!(payload["source_slot"], "root");
        assert_eq!(payload["access_control"], original_access);
        assert!(store.statuses().unwrap().is_empty());
        assert!(store.status_with_health(&recovery_ref).is_err());

        assert_no_plaintext_in_persistent_files(
            dir.path(),
            &[
                "root-hash-fragment",
                "root-salt-fragment",
                "device-hash-fragment",
                "device-salt-fragment",
                "payload-private-fragment",
            ],
        );

        let before = directory_snapshot(dir.path());
        let second = migrate_config_facade_layout(dir.path()).unwrap();
        assert!(!second.activated);
        assert_eq!(directory_snapshot(dir.path()), before);
        assert!(ConfigFacade::open(dir.path())
            .unwrap()
            .registry()
            .access_control
            .snapshot()
            .data
            .0
            .as_ref()
            .is_some_and(|access| access.repair_required));
    }

    #[test]
    fn active_layout_quarantines_access_damage_without_accepting_flag_clear() {
        let _key = crate::encryption::set_test_encryption_key([0xa6; 32]);
        let dir = TempDir::new().unwrap();
        let facade = ConfigFacade::open_or_migrate(dir.path()).unwrap();
        let revision = facade.registry().access_control.snapshot().revision;
        let damaged = json!({
            "schema_version": SECTION_SCHEMA_VERSION,
            "revision": revision + 1,
            "data": {
                "password_enabled": "yes",
                "password_hash": "active-layout-secret"
            }
        });
        write_json(&dir.path().join("access-control.json"), &damaged);

        migrate_config_facade_layout(dir.path()).unwrap();
        let exact = crate::read_exact_credential_section_snapshot(
            dir.path(),
            SectionId::AccessControl,
            None,
        )
        .unwrap();
        assert_eq!(exact.section.status, SectionStatus::Degraded);
        let exact_revision = exact.section.revision;
        let mut candidate = Config::default();
        exact.install_into(&mut candidate);
        let access = candidate.access_control.as_mut().unwrap();
        assert!(access.password_enabled);
        assert!(access.repair_required);
        access.repair_required = false;

        let before_access = std::fs::read(dir.path().join("access-control.json")).unwrap();
        let before_credentials = std::fs::read(dir.path().join("credentials.json")).unwrap();
        let error = crate::persist_access_control_credential_transaction_at_revision(
            dir.path(),
            &mut candidate,
            false,
            &BTreeSet::new(),
            exact_revision,
        )
        .unwrap_err();
        assert!(error.to_string().contains("server-owned"));
        assert_eq!(
            std::fs::read(dir.path().join("access-control.json")).unwrap(),
            before_access
        );
        assert_eq!(
            std::fs::read(dir.path().join("credentials.json")).unwrap(),
            before_credentials
        );

        let recovery_ref =
            CredentialRef::parse("access_repair.section.payload".to_string()).unwrap();
        let recovery = CredentialStore::open(dir.path())
            .resolve(&recovery_ref)
            .unwrap()
            .unwrap();
        let payload: Value = serde_json::from_str(recovery.expose()).unwrap();
        assert_eq!(payload["access_control"], damaged["data"]);
        assert_no_plaintext_in_persistent_files(dir.path(), &["active-layout-secret"]);
    }

    #[test]
    fn active_layout_repairs_syntax_invalid_access_without_disabling_other_sections() {
        let _key = crate::encryption::set_test_encryption_key([0xa8; 32]);
        let dir = TempDir::new().unwrap();
        let initial = ConfigFacade::open_or_migrate(dir.path()).unwrap();
        let initial_access_revision = initial.registry().access_control.snapshot().revision;
        drop(initial);

        let damaged = br#"{ "password_hash": "opaque-access-secret""#;
        std::fs::write(dir.path().join("access-control.json"), damaged).unwrap();

        let facade = ConfigFacade::open_or_migrate(dir.path())
            .expect("AccessControl damage must remain section-local");
        assert_eq!(
            facade.registry().tools_skills.snapshot().status,
            SectionStatus::Healthy
        );
        assert_eq!(
            facade.registry().mcp.snapshot().status,
            SectionStatus::Healthy
        );
        let access = facade.registry().access_control.snapshot();
        let access = access.data.0.as_ref().unwrap();
        assert!(access.password_enabled);
        assert!(access.repair_required);
        assert!(!access.password_configured);

        let tools = facade.registry().tools_skills.snapshot();
        let mut tools_candidate = serde_json::to_value(tools.data.as_ref()).unwrap();
        tools_candidate["tools"]["disabled"] = json!(["bash"]);
        let event = facade
            .registry()
            .commit_value(SectionId::ToolsSkills, tools.revision, tools_candidate)
            .unwrap();
        assert_eq!(
            event,
            ConfigSectionEvent::Changed {
                section: "tools-skills".to_string(),
                revision: tools.revision + 1,
            }
        );

        let exact = crate::read_exact_credential_section_snapshot(
            dir.path(),
            SectionId::AccessControl,
            None,
        )
        .unwrap();
        assert_eq!(exact.section.status, SectionStatus::Degraded);
        assert_eq!(
            exact.section.last_error.as_deref(),
            Some("access-control credential repair is required")
        );
        assert!(validate_section_envelope(
            "access-control.json",
            &std::fs::read(dir.path().join("access-control.json")).unwrap(),
            initial_access_revision + 1,
        )
        .is_ok());

        let recovery_ref =
            CredentialRef::parse("access_repair.section.payload".to_string()).unwrap();
        let recovery = CredentialStore::open(dir.path())
            .resolve(&recovery_ref)
            .unwrap()
            .unwrap();
        let payload: Value = serde_json::from_str(recovery.expose()).unwrap();
        assert_eq!(payload["encoding"], "base64");
        let encoded = payload["access_control_bytes"].as_str().unwrap();
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .unwrap(),
            damaged
        );
        assert_no_plaintext_in_persistent_files(dir.path(), &["opaque-access-secret"]);
    }

    #[test]
    fn access_recovery_slot_collision_aborts_without_mutating_any_source() {
        let _key = crate::encryption::set_test_encryption_key([0xa7; 32]);
        let dir = TempDir::new().unwrap();
        let store = CredentialStore::open(dir.path());
        let ordinary = CredentialRef::parse("custom.collision.payload").unwrap();
        store
            .replace(
                ordinary,
                "preexisting-user-value",
                crate::CredentialSource::User,
                0,
            )
            .unwrap();
        let credentials_path = dir.path().join("credentials.json");
        let credentials = std::fs::read_to_string(&credentials_path)
            .unwrap()
            .replace("custom.collision.payload", "access_repair.root.payload");
        std::fs::write(&credentials_path, credentials).unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({
                "access_control": {
                    "password_enabled": "yes",
                    "password_hash": "collision-private-fragment"
                },
                "tools": {"disabled": ["bash"]},
                "mcp": {}
            }),
        );
        let before = directory_snapshot(dir.path());

        let error = migrate_config_facade_layout(dir.path()).unwrap_err();

        assert!(error
            .to_string()
            .contains("access recovery target credential is already user-managed"));
        assert_eq!(directory_snapshot(dir.path()), before);
        assert!(std::fs::read_to_string(dir.path().join("config.json"))
            .unwrap()
            .contains("collision-private-fragment"));
    }

    #[test]
    fn complete_legacy_access_verifier_migrates_atomically_and_idempotently() {
        let _key = crate::encryption::set_test_encryption_key([0xa2; 32]);
        let dir = TempDir::new().unwrap();
        let hash = "a".repeat(64);
        let salt = "01".repeat(16);
        write_json(
            &dir.path().join("config.json"),
            &json!({
                "access_control": {
                    "password_enabled": true,
                    "password_hash": hash,
                    "password_salt": salt
                }
            }),
        );

        let facade = ConfigFacade::open_or_migrate(dir.path()).unwrap();
        let access = facade.registry().access_control.snapshot();
        let access = access.data.0.as_ref().unwrap();
        assert!(access.password_enabled);
        assert!(access.password_configured);
        assert_eq!(
            access
                .password_credential_ref
                .as_ref()
                .map(CredentialRef::as_str),
            Some("access.root.password_verifier")
        );
        assert!(access.password_hash.is_none());
        assert!(access.password_salt.is_none());

        let mut runtime = facade.effective_config();
        runtime
            .hydrate_access_control_credentials_from_store(dir.path())
            .unwrap();
        let hydrated = runtime.access_control.as_ref().unwrap();
        assert_eq!(hydrated.password_hash.as_deref(), Some(hash.as_str()));
        assert_eq!(hydrated.password_salt.as_deref(), Some(salt.as_str()));
        for name in ["config.json", "access-control.json", "credentials.json"] {
            let persisted = std::fs::read_to_string(dir.path().join(name)).unwrap();
            assert!(!persisted.contains(&hash), "{name} leaked the legacy hash");
            assert!(!persisted.contains(&salt), "{name} leaked the legacy salt");
        }

        let before = directory_snapshot(dir.path());
        let outcome = migrate_config_facade_layout(dir.path()).unwrap();
        assert!(!outcome.activated);
        assert_eq!(directory_snapshot(dir.path()), before);
    }

    #[test]
    fn legacy_access_verifier_recovers_after_compound_manifest_interruption() {
        use crate::credential_migration::{
            install_section_split_migration_with_fault, plan_facade_compound_credentials,
            FacadeSourceSnapshot, SectionSplitTestFault,
        };

        let _key = crate::encryption::set_test_encryption_key([0xa3; 32]);
        let dir = TempDir::new().unwrap();
        let hash = "b".repeat(64);
        let salt = "02".repeat(16);
        write_json(
            &dir.path().join("config.json"),
            &json!({
                "access_control": {
                    "password_enabled": true,
                    "password_hash": hash,
                    "password_salt": salt
                }
            }),
        );

        let source_snapshot = FacadeSourceSnapshot::capture(dir.path()).unwrap();
        let credentials =
            plan_facade_compound_credentials(dir.path(), source_snapshot, false).unwrap();
        let plan = plan_config_facade_compound_layout(dir.path(), false, credentials).unwrap();
        assert!(install_section_split_migration_with_fault(
            dir.path(),
            plan,
            SectionSplitTestFault::Manifest,
        )
        .is_err());
        assert!(!section_layout_is_active(dir.path()).unwrap());

        let recovered = migrate_config_facade_layout(dir.path()).unwrap();
        assert!(recovered.resumed);
        let facade = ConfigFacade::open(dir.path()).unwrap();
        let mut runtime = facade.effective_config();
        runtime
            .hydrate_access_control_credentials_from_store(dir.path())
            .unwrap();
        let access = runtime.access_control.as_ref().unwrap();
        assert_eq!(access.password_hash.as_deref(), Some(hash.as_str()));
        assert_eq!(access.password_salt.as_deref(), Some(salt.as_str()));
        assert_eq!(
            facade.registry().credentials.statuses().len(),
            1,
            "recovery must not duplicate the migrated verifier"
        );

        let before = directory_snapshot(dir.path());
        let second = migrate_config_facade_layout(dir.path()).unwrap();
        assert!(!second.activated);
        assert_eq!(directory_snapshot(dir.path()), before);
    }

    #[test]
    fn active_layout_survives_normal_section_and_credential_backup_rotation() {
        let _key = crate::encryption::set_test_encryption_key([114; 32]);
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({"unknown_root": "preserved"}),
        );
        let facade = ConfigFacade::open_or_migrate(dir.path()).unwrap();

        let providers = facade.registry().providers.snapshot();
        let mut provider_candidate = providers.data.as_ref().clone();
        provider_candidate.provider = Some("anthropic".to_string());
        facade
            .registry()
            .providers
            .commit(providers.revision, provider_candidate)
            .unwrap();
        assert!(dir.path().join("providers.json.bak").exists());
        assert!(section_layout_is_active(dir.path()).unwrap());
        ConfigFacade::open(dir.path()).unwrap();

        let credentials = facade.registry().credentials.snapshot();
        facade
            .registry()
            .credentials
            .replace(
                CredentialRef::parse("provider.test.api_key").unwrap(),
                "post-activation-secret",
                CredentialSource::User,
                credentials.revision,
            )
            .unwrap();
        assert!(dir.path().join("credentials.json.bak").exists());
        assert!(section_layout_is_active(dir.path()).unwrap());
        ConfigFacade::open(dir.path()).unwrap();
    }

    #[test]
    fn layout_activation_requires_completion_evidence_but_allows_degraded_members() {
        let _key = crate::encryption::set_test_encryption_key([89; 32]);
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({"http_proxy": "http://attested"}),
        );
        migrate_config_facade_layout(dir.path()).unwrap();

        let marker_path = dir.path().join(SECTION_LAYOUT_FILE);
        let marker_bytes = std::fs::read(&marker_path).unwrap();
        let ledger_path = dir
            .path()
            .join(crate::credential_migration::SECTION_LAYOUT_COMPLETION_FILE);
        let ledger_bytes = std::fs::read(&ledger_path).unwrap();
        let completion = validate_completed_section_layout_marker(&marker_bytes).unwrap();
        assert_eq!(completion.members.len(), SECTION_DESCRIPTORS.len());
        assert!(!completion.members.contains_key(SECTION_LAYOUT_FILE));
        assert!(Uuid::parse_str(&completion.transaction_id).is_ok());
        for descriptor in SECTION_DESCRIPTORS {
            let bytes = std::fs::read(dir.path().join(descriptor.file_name)).unwrap();
            let member = &completion.members[descriptor.file_name];
            assert_eq!(member.sha256, hex::encode(Sha256::digest(&bytes)));
            let revision = if descriptor.id == SectionId::Credentials {
                CredentialStore::validate_section_document_revision(&bytes).unwrap()
            } else {
                validate_section_envelope(descriptor.file_name, &bytes, 0).unwrap()
            };
            assert_eq!(member.revision, revision);
        }

        let marker_only = TempDir::new().unwrap();
        std::fs::write(marker_only.path().join(SECTION_LAYOUT_FILE), &marker_bytes).unwrap();
        assert!(!section_layout_is_active(marker_only.path()).unwrap());

        let core_path = dir.path().join("core.json");
        let original_core = std::fs::read(&core_path).unwrap();
        std::fs::remove_file(&core_path).unwrap();
        assert!(section_layout_is_active(dir.path()).unwrap());
        std::fs::write(&core_path, &original_core).unwrap();

        let mut forged_uuid: Value = serde_json::from_slice(&marker_bytes).unwrap();
        forged_uuid["completion"]["transaction_id"] = Value::String("not-a-uuid".to_string());
        write_json(&marker_path, &forged_uuid);
        assert!(!section_layout_is_active(dir.path()).unwrap());
        std::fs::write(&marker_path, &marker_bytes).unwrap();

        let mut forged_valid_uuid: Value = serde_json::from_slice(&marker_bytes).unwrap();
        forged_valid_uuid["completion"]["transaction_id"] =
            Value::String(Uuid::new_v4().to_string());
        write_json(&marker_path, &forged_valid_uuid);
        assert!(!section_layout_is_active(dir.path()).unwrap());
        std::fs::write(&marker_path, &marker_bytes).unwrap();

        std::fs::remove_file(&ledger_path).unwrap();
        assert!(!section_layout_is_active(dir.path()).unwrap());
        std::fs::write(&ledger_path, ledger_bytes).unwrap();

        let attested_revision = completion.members["core.json"].revision;
        let mut same_revision: Value = serde_json::from_slice(&original_core).unwrap();
        same_revision["data"]["future_same_revision"] = json!({"tampered": true});
        write_json(&core_path, &same_revision);
        assert!(section_layout_is_active(dir.path()).unwrap());

        same_revision["revision"] = json!(attested_revision - 1);
        write_json(&core_path, &same_revision);
        assert!(section_layout_is_active(dir.path()).unwrap());

        let mut higher: Value = serde_json::from_slice(&original_core).unwrap();
        higher["revision"] = json!(attested_revision + 1);
        higher["data"]["future_forward"] = json!({"kept": true});
        write_json(&core_path, &higher);
        assert!(section_layout_is_active(dir.path()).unwrap());

        higher["revision"] = json!(attested_revision + 2);
        higher["data"]["token"] = json!("must-not-enter-an-ordinary-section");
        write_json(&core_path, &higher);
        assert!(section_layout_is_active(dir.path()).unwrap());

        std::fs::write(&core_path, &original_core).unwrap();
        let skeleton = SectionLayoutMarker {
            layout_version: SECTION_LAYOUT_VERSION,
            sections: expected_section_layout(),
            completion: None,
        };
        std::fs::write(&marker_path, serde_json::to_vec_pretty(&skeleton).unwrap()).unwrap();
        assert!(!section_layout_is_active(dir.path()).unwrap());
    }

    #[test]
    fn strict_root_failures_leave_the_directory_byte_for_byte_unchanged() {
        for invalid in [
            b"{ malformed".as_slice(),
            b"[]".as_slice(),
            br#"{"server":"not-an-object"}"#.as_slice(),
        ] {
            let dir = TempDir::new().unwrap();
            std::fs::write(dir.path().join("config.json"), invalid).unwrap();
            std::fs::write(dir.path().join("unrelated.bin"), b"preserve-me").unwrap();
            let before = directory_snapshot(dir.path());

            assert!(migrate_config_facade_layout(dir.path()).is_err());
            assert_eq!(directory_snapshot(dir.path()), before);
        }
    }

    #[test]
    fn preflight_retries_a_stale_error_across_manifest_aba_and_source_change() {
        let dir = TempDir::new().unwrap();
        let memory_path = dir.path().join("memory.json");
        std::fs::write(&memory_path, b"{ invalid before concurrent commit").unwrap();
        let hook_memory = memory_path.clone();
        let hook_manifest = dir.path().join("config-credential-migration.json");
        set_facade_preflight_test_hook(dir.path(), move || {
            std::fs::write(&hook_manifest, b"transient committed manifest").unwrap();
            write_json(
                &hook_memory,
                &section_document(1, json!({"background_model": "repaired-model"})),
            );
            std::fs::remove_file(hook_manifest).unwrap();
        });

        let outcome = migrate_config_facade_layout(dir.path()).unwrap();
        assert!(outcome.activated);
        let facade = ConfigFacade::open(dir.path()).unwrap();
        assert_eq!(
            facade
                .registry()
                .memory
                .snapshot()
                .data
                .0
                .as_ref()
                .and_then(|memory| memory.background_model.as_deref()),
            Some("repaired-model")
        );
    }

    #[test]
    fn preflight_distinguishes_absent_from_empty_source_without_partial_migration() {
        let _key = crate::encryption::set_test_encryption_key([109; 32]);
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({"providers": {"openai": {
                "api_key_encrypted": crate::encryption::encrypt("provider-secret").unwrap()
            }}}),
        );
        let memory_path = dir.path().join("memory.json");
        let hook_memory = memory_path.clone();
        set_facade_preflight_test_hook(dir.path(), move || {
            std::fs::write(hook_memory, b"").unwrap();
        });

        let error = migrate_config_facade_layout(dir.path()).unwrap_err();
        assert!(error.to_string().contains("empty"));
        assert_eq!(std::fs::read(memory_path).unwrap(), b"");
        let config = std::fs::read_to_string(dir.path().join("config.json")).unwrap();
        assert!(config.contains("api_key_encrypted"));
        assert!(!dir.path().join("credentials.json").exists());
        assert!(!dir.path().join("credentials.json.lock").exists());
    }

    #[test]
    fn facade_epoch_rechecks_all_domains_before_the_first_commit() {
        let _key = crate::encryption::set_test_encryption_key([111; 32]);
        let dir = TempDir::new().unwrap();
        let provider_ciphertext = crate::encryption::encrypt("provider-secret").unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({"providers": {"openai": {
                "api_key_encrypted": provider_ciphertext
            }}}),
        );
        let late_bytes = serde_json::to_vec_pretty(&json!({
            "providers": {"openai": {
                "api_key_encrypted": crate::encryption::encrypt("provider-secret").unwrap()
            }},
            "cluster_fabric": {"nodes": [{
                "id": "late-node",
                "label": "late-node",
                "placement": {
                    "type": "ssh",
                    "host": "10.0.0.9",
                    "username": "deploy",
                    "auth": {
                        "method": "password",
                        "password_encrypted": "not-a-valid-ciphertext"
                    }
                }
            }]}
        }))
        .unwrap();
        let config_path = dir.path().join("config.json");
        let hook_path = config_path.clone();
        let hook_bytes = late_bytes.clone();
        set_facade_epoch_test_hook(dir.path(), move || {
            std::fs::write(hook_path, hook_bytes).unwrap();
        });

        assert!(migrate_config_facade_layout(dir.path()).is_err());
        assert_eq!(std::fs::read(config_path).unwrap(), late_bytes);
        for forbidden in [
            "providers.json",
            "credentials.json",
            "config-credential-migration.json",
            SECTION_LAYOUT_FILE,
        ] {
            assert!(!dir.path().join(forbidden).exists(), "created {forbidden}");
        }
    }

    #[test]
    fn compound_manifest_rolls_back_earlier_domains_when_editor_wins_after_commit() {
        let _key = crate::encryption::set_test_encryption_key([112; 32]);
        let dir = TempDir::new().unwrap();
        let root = serde_json::to_vec_pretty(&json!({
            "providers": {"openai": {
                "api_key_encrypted": crate::encryption::encrypt("provider-secret").unwrap()
            }},
            "unknown_root": "must-survive"
        }))
        .unwrap();
        std::fs::write(dir.path().join("config.json"), &root).unwrap();
        crate::credential_migration::set_facade_compound_after_manifest_test_hook(
            dir.path(),
            |data_dir| {
                std::fs::write(
                    data_dir.join("cluster-fabric.json"),
                    b"{ editor-won-with-invalid-cluster",
                )
                .unwrap();
            },
        );

        assert!(migrate_config_facade_layout(dir.path()).is_err());
        assert_eq!(std::fs::read(dir.path().join("config.json")).unwrap(), root);
        assert_eq!(
            std::fs::read(dir.path().join("cluster-fabric.json")).unwrap(),
            b"{ editor-won-with-invalid-cluster"
        );
        for rolled_back in [
            "core.json",
            "providers.json",
            "mcp.json",
            "credentials.json",
            "config-sections.json",
            "config-credential-migration.json",
            "config-credential-migration.journal.json",
        ] {
            assert!(
                !dir.path().join(rolled_back).exists(),
                "transaction member survived rollback: {rolled_back}"
            );
        }
    }

    #[test]
    fn compound_source_guards_preserve_changed_and_newly_created_editor_files() {
        for (initial, editor) in [
            (
                Some(br#"{"safe":"initial"}"#.as_slice()),
                br#"{"safe":"editor-won"}"#.as_slice(),
            ),
            (None, b"".as_slice()),
        ] {
            let _key = crate::encryption::set_test_encryption_key([118; 32]);
            let dir = TempDir::new().unwrap();
            write_json(
                &dir.path().join("config.json"),
                &json!({"unknown_root": true}),
            );
            let guarded_path = dir.path().join("connect.json.bak.1");
            if let Some(initial) = initial {
                std::fs::write(&guarded_path, initial).unwrap();
            }
            let hook_path = guarded_path.clone();
            let editor = editor.to_vec();
            let expected_editor = editor.clone();
            crate::credential_migration::set_facade_compound_after_manifest_test_hook(
                dir.path(),
                move |_| std::fs::write(hook_path, editor).unwrap(),
            );

            assert!(migrate_config_facade_layout(dir.path()).is_err());
            assert_eq!(std::fs::read(&guarded_path).unwrap(), expected_editor);
            assert_no_layout_transaction_artifacts(dir.path());
        }
    }

    #[test]
    fn after_journal_crash_is_discarded_and_replanned_in_one_public_retry() {
        let _key = crate::encryption::set_test_encryption_key([119; 32]);
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({"unknown_root": true}),
        );
        let snapshot =
            crate::credential_migration::FacadeSourceSnapshot::capture(dir.path()).unwrap();
        let credentials = crate::credential_migration::plan_facade_compound_credentials(
            dir.path(),
            snapshot,
            true,
        )
        .unwrap();
        let plan = plan_config_facade_compound_layout(dir.path(), true, credentials).unwrap();
        assert!(
            crate::credential_migration::install_section_split_migration_with_fault(
                dir.path(),
                plan,
                crate::credential_migration::SectionSplitTestFault::Journal,
            )
            .is_err()
        );
        assert!(dir
            .path()
            .join("config-credential-migration.journal.json")
            .exists());
        assert!(!dir.path().join("config-credential-migration.json").exists());

        let outcome = migrate_config_facade_layout(dir.path()).unwrap();
        assert!(outcome.activated);
        assert!(section_layout_is_active(dir.path()).unwrap());
        assert!(!dir
            .path()
            .join("config-credential-migration.journal.json")
            .exists());
        assert!(std::fs::read_dir(dir.path()).unwrap().all(|entry| {
            let name = entry.unwrap().file_name().to_string_lossy().to_string();
            !name.starts_with(".config-migration-stage-")
                && !name.starts_with(".config-migration-backup-")
        }));
    }

    #[test]
    fn root_only_mcp_secrets_migrate_before_the_root_is_scrubbed() {
        let _key = crate::encryption::set_test_encryption_key([120; 32]);
        let dir = TempDir::new().unwrap();
        let mut fixture: Value = serde_json::from_slice(include_bytes!(
            "../tests/fixtures/config_migration/mcp-legacy.json"
        ))
        .unwrap();
        fixture["data"]["stdio unsafe/name"]["env_encrypted"]["PRIVATE TOKEN"] =
            Value::String(crate::encryption::encrypt("root-private-secret").unwrap());
        let root = json!({"mcpServers": fixture["data"].clone(), "unknown_root": true});
        write_json(&dir.path().join("config.json"), &root);
        write_json(&dir.path().join("config.json.bak"), &root);

        migrate_config_facade_layout(dir.path()).unwrap();
        assert!(section_layout_is_active(dir.path()).unwrap());
        let mcp: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("mcp.json")).unwrap()).unwrap();
        let data = &mcp["data"];
        let refs = [
            (
                data["stdio unsafe/name"]["env_credential_refs"]["PUBLIC TOKEN"]
                    .as_str()
                    .unwrap(),
                "stdio-plain-secret",
            ),
            (
                data["stdio unsafe/name"]["env_credential_refs"]["PRIVATE TOKEN"]
                    .as_str()
                    .unwrap(),
                "root-private-secret",
            ),
            (
                data["http.server"]["header_credential_refs"]["Authorization"]
                    .as_str()
                    .unwrap(),
                "Bearer mcp-header-secret",
            ),
        ];
        let store = CredentialStore::open(dir.path());
        for (reference, expected) in refs {
            assert_eq!(
                store
                    .resolve(&CredentialRef::parse(reference).unwrap())
                    .unwrap()
                    .unwrap()
                    .expose(),
                expected
            );
        }
        for name in ["config.json", "config.json.bak", "mcp.json"] {
            let text = std::fs::read_to_string(dir.path().join(name)).unwrap();
            for forbidden in [
                "stdio-plain-secret",
                "root-private-secret",
                "Bearer mcp-header-secret",
                "env_encrypted",
                "headers_encrypted",
            ] {
                assert!(!text.contains(forbidden), "{name} retained {forbidden}");
            }
        }
    }

    #[test]
    fn sidecar_secret_authority_is_field_scoped_while_mcp_shadows_the_root_domain() {
        let _key = crate::encryption::set_test_encryption_key([121; 32]);
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({
                "providers": {
                    "openai": {"api_key": "root-stale-provider"},
                    "anthropic": {"api_key": "unshadowed-root-provider"}
                },
                "mcpServers": {
                    "shared": {
                        "command": "unused",
                        "env": {"TOKEN": "root-stale-mcp"},
                        "request_timeout_ms": 100,
                        "healthcheck_interval_ms": 100
                    },
                    "root-only": {
                        "command": "unused",
                        "env": {"ONLY": "shadowed-root-only-mcp"},
                        "request_timeout_ms": 100,
                        "healthcheck_interval_ms": 100
                    }
                }
            }),
        );
        write_json(
            &dir.path().join("providers.json"),
            &json!({"openai": {"api_key": "sidecar-provider"}}),
        );
        write_json(
            &dir.path().join("mcp.json"),
            &json!({"shared": {
                "command": "unused",
                "env": {"TOKEN": "sidecar-mcp"},
                "request_timeout_ms": 100,
                "healthcheck_interval_ms": 100
            }}),
        );

        migrate_config_facade_layout(dir.path()).unwrap();
        let store = CredentialStore::open(dir.path());
        assert_eq!(
            store
                .resolve(&CredentialRef::parse("provider.openai.api_key").unwrap())
                .unwrap()
                .unwrap()
                .expose(),
            "sidecar-provider"
        );
        assert_eq!(
            store
                .resolve(&CredentialRef::parse("provider.anthropic.api_key").unwrap())
                .unwrap()
                .unwrap()
                .expose(),
            "unshadowed-root-provider"
        );
        let mcp: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("mcp.json")).unwrap()).unwrap();
        let shared_ref = mcp["data"]["shared"]["env_credential_refs"]["TOKEN"]
            .as_str()
            .unwrap();
        assert_eq!(
            store
                .resolve(&CredentialRef::parse(shared_ref).unwrap())
                .unwrap()
                .unwrap()
                .expose(),
            "sidecar-mcp"
        );
        assert!(mcp["data"].get("root-only").is_none());
        assert!(store
            .resolve(&CredentialRef::parse("mcp.root-only.env_ONLY").unwrap())
            .unwrap()
            .is_none());
        let root = std::fs::read_to_string(dir.path().join("config.json")).unwrap();
        for forbidden in [
            "root-stale-provider",
            "unshadowed-root-provider",
            "root-stale-mcp",
            "shadowed-root-only-mcp",
        ] {
            assert!(!root.contains(forbidden));
        }
        assert!(!root.contains("env_credential_refs"));
    }

    #[test]
    fn primary_sidecar_keys_tombstone_stale_backups_without_hiding_backup_only_keys() {
        let _key = crate::encryption::set_test_encryption_key([122; 32]);
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({"unknown_root": true}),
        );
        write_json(
            &dir.path().join("providers.json"),
            &json!({"openai": {"model": "primary-without-secret"}}),
        );
        write_json(
            &dir.path().join("providers.json.bak"),
            &json!({
                "openai": {"api_key": "stale-openai-secret"},
                "anthropic": {"api_key": "backup-only-anthropic"}
            }),
        );
        write_json(
            &dir.path().join("mcp.json"),
            &json!({
                "shared": {
                    "command": "unused",
                    "request_timeout_ms": 100,
                    "healthcheck_interval_ms": 100
                },
                "live": {
                    "command": "unused",
                    "env": {"TOKEN": "primary-live-mcp"},
                    "request_timeout_ms": 100,
                    "healthcheck_interval_ms": 100
                }
            }),
        );
        write_json(
            &dir.path().join("mcp.json.bak"),
            &json!({
                "shared": {
                    "command": "unused",
                    "env": {"TOKEN": "stale-shared-mcp"},
                    "request_timeout_ms": 100,
                    "healthcheck_interval_ms": 100
                },
                "live": {
                    "command": "unused",
                    "env": {"TOKEN": "stale-live-mcp"},
                    "request_timeout_ms": 100,
                    "healthcheck_interval_ms": 100
                },
                "backup-only": {
                    "command": "unused",
                    "env": {"TOKEN": "backup-only-mcp"},
                    "request_timeout_ms": 100,
                    "healthcheck_interval_ms": 100
                }
            }),
        );

        migrate_config_facade_layout(dir.path()).unwrap();
        let store = CredentialStore::open(dir.path());
        for missing in ["provider.openai.api_key", "mcp.shared.env_TOKEN"] {
            assert!(store
                .resolve(&CredentialRef::parse(missing).unwrap())
                .unwrap()
                .is_none());
        }
        for (reference, expected) in [
            ("provider.anthropic.api_key", "backup-only-anthropic"),
            ("mcp.live.env_TOKEN", "primary-live-mcp"),
            ("mcp.backup-only.env_TOKEN", "backup-only-mcp"),
        ] {
            assert_eq!(
                store
                    .resolve(&CredentialRef::parse(reference).unwrap())
                    .unwrap()
                    .unwrap()
                    .expose(),
                expected
            );
        }

        let provider_backup: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("providers.json.bak")).unwrap())
                .unwrap();
        assert!(provider_backup["data"]["openai"]
            .get("credential_ref")
            .is_none());
        assert_eq!(
            provider_backup["data"]["anthropic"]["credential_ref"],
            "provider.anthropic.api_key"
        );

        let mcp_backup: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("mcp.json.bak")).unwrap())
                .unwrap();
        for tombstoned in ["shared", "live"] {
            assert!(mcp_backup["data"][tombstoned]
                .get("env_credential_refs")
                .is_none());
        }
        assert_eq!(
            mcp_backup["data"]["backup-only"]["env_credential_refs"]["TOKEN"],
            "mcp.backup-only.env_TOKEN"
        );
        for name in ["providers.json.bak", "mcp.json.bak"] {
            let text = std::fs::read_to_string(dir.path().join(name)).unwrap();
            for forbidden in [
                "stale-openai-secret",
                "backup-only-anthropic",
                "stale-shared-mcp",
                "stale-live-mcp",
                "backup-only-mcp",
            ] {
                assert!(!text.contains(forbidden), "{name} retained {forbidden}");
            }
        }
    }

    #[test]
    fn compound_migration_scrubs_all_valid_legacy_backup_generations() {
        let _key = crate::encryption::set_test_encryption_key([113; 32]);
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({"unknown_root": true}),
        );
        let root_backup = serde_json::to_vec_pretty(&json!({
            "providers": {"anthropic": {
                "api_key_encrypted": crate::encryption::encrypt("root-backup-secret").unwrap()
            }},
            "cluster_fabric": {"nodes": [{
                "id": "backup-node",
                "label": "backup-node",
                "placement": {
                    "type": "ssh",
                    "host": "10.0.0.8",
                    "username": "deploy",
                    "auth": {"method": "password", "password": "cluster-backup-secret"}
                }
            }]}
        }))
        .unwrap();
        std::fs::write(dir.path().join("config.json.bak.2"), root_backup).unwrap();
        std::fs::write(
            dir.path().join("providers.json.bak"),
            include_bytes!("../tests/fixtures/config_migration/providers-legacy.json"),
        )
        .unwrap();
        let mut mcp_backup: Value = serde_json::from_slice(include_bytes!(
            "../tests/fixtures/config_migration/mcp-legacy.json"
        ))
        .unwrap();
        mcp_backup["data"]["stdio unsafe/name"]["env_encrypted"]["PRIVATE TOKEN"] =
            Value::String(crate::encryption::encrypt("mcp-backup-cipher").unwrap());
        std::fs::write(
            dir.path().join("mcp.json.bak.1"),
            serde_json::to_vec_pretty(&mcp_backup).unwrap(),
        )
        .unwrap();

        migrate_config_facade_layout(dir.path()).unwrap();
        assert!(section_layout_is_active(dir.path()).unwrap());
        for name in ["config.json.bak.2", "providers.json.bak", "mcp.json.bak.1"] {
            let text = std::fs::read_to_string(dir.path().join(name)).unwrap();
            for forbidden in [
                "root-backup-secret",
                "cluster-backup-secret",
                "sk-provider-plain",
                "stdio-plain-secret",
                "mcp-backup-cipher",
                "mcp-header-secret",
                "api_key_encrypted",
                "password_encrypted",
                "env_encrypted",
            ] {
                assert!(!text.contains(forbidden), "{name} retained {forbidden}");
            }
        }
        let store = CredentialStore::open(dir.path());
        for (reference, expected) in [
            ("provider.anthropic.api_key", "root-backup-secret"),
            ("cluster.backup-node.password", "cluster-backup-secret"),
            ("provider.openai.api_key", "sk-provider-plain"),
        ] {
            let reference = CredentialRef::parse(reference).unwrap();
            assert_eq!(
                store.resolve(&reference).unwrap().unwrap().expose(),
                expected
            );
        }
        let mcp_backup: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("mcp.json.bak.1")).unwrap())
                .unwrap();
        for (reference, expected) in [
            (
                mcp_backup["data"]["stdio unsafe/name"]["env_credential_refs"]["PUBLIC TOKEN"]
                    .as_str()
                    .unwrap(),
                "stdio-plain-secret",
            ),
            (
                mcp_backup["data"]["stdio unsafe/name"]["env_credential_refs"]["PRIVATE TOKEN"]
                    .as_str()
                    .unwrap(),
                "mcp-backup-cipher",
            ),
            (
                mcp_backup["data"]["http.server"]["header_credential_refs"]["Authorization"]
                    .as_str()
                    .unwrap(),
                "Bearer mcp-header-secret",
            ),
        ] {
            assert_eq!(
                store
                    .resolve(&CredentialRef::parse(reference).unwrap())
                    .unwrap()
                    .unwrap()
                    .expose(),
                expected
            );
        }
    }

    #[test]
    fn compound_migration_prefers_store_then_primaries_then_newest_backup() {
        let _key = crate::encryption::set_test_encryption_key([115; 32]);
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({"unknown_root": true}),
        );
        write_json(
            &dir.path().join("providers.json"),
            &json!({"openai": {"api_key": "provider-primary"}}),
        );
        write_json(
            &dir.path().join("providers.json.bak"),
            &json!({
                "openai": {"api_key": "provider-stale"},
                "anthropic": {"api_key": "anthropic-newest-backup"}
            }),
        );
        write_json(
            &dir.path().join("providers.json.bak.1"),
            &json!({"anthropic": {"api_key": "anthropic-older-backup"}}),
        );
        write_json(
            &dir.path().join("broker.json"),
            &json!({"endpoint": "ws://127.0.0.1:9600", "token": "broker-primary"}),
        );
        write_json(
            &dir.path().join("broker.json.bak"),
            &json!({"endpoint": "ws://127.0.0.1:9600", "token": "broker-stale"}),
        );

        let store = CredentialStore::open(dir.path());
        store
            .replace(
                CredentialRef::parse("provider.gemini.api_key").unwrap(),
                "store-authority",
                CredentialSource::User,
                0,
            )
            .unwrap();
        write_json(
            &dir.path().join("config.json.bak"),
            &json!({"providers": {"gemini": {"api_key": "stale-user-copy"}}}),
        );

        migrate_config_facade_layout(dir.path()).unwrap();
        assert!(section_layout_is_active(dir.path()).unwrap());
        for (reference, expected) in [
            ("provider.gemini.api_key", "store-authority"),
            ("provider.openai.api_key", "provider-primary"),
            ("provider.anthropic.api_key", "anthropic-newest-backup"),
            ("broker.external.bearer_token", "broker-primary"),
        ] {
            let reference = CredentialRef::parse(reference).unwrap();
            assert_eq!(
                store.resolve(&reference).unwrap().unwrap().expose(),
                expected
            );
        }
        for name in [
            "providers.json",
            "providers.json.bak",
            "providers.json.bak.1",
            "config.json.bak",
            "broker.json",
            "broker.json.bak",
        ] {
            let text = std::fs::read_to_string(dir.path().join(name)).unwrap();
            for forbidden in [
                "provider-primary",
                "provider-stale",
                "anthropic-newest-backup",
                "anthropic-older-backup",
                "stale-user-copy",
                "broker-primary",
                "broker-stale",
            ] {
                assert!(!text.contains(forbidden), "{name} retained {forbidden}");
            }
        }
    }

    #[test]
    fn compound_migration_with_existing_credentials_can_rotate_lkg_backups() {
        let _key = crate::encryption::set_test_encryption_key([116; 32]);
        let dir = TempDir::new().unwrap();
        let store = CredentialStore::open(dir.path());
        store
            .replace(
                CredentialRef::parse("user.first.token").unwrap(),
                "first-user-secret",
                CredentialSource::User,
                0,
            )
            .unwrap();
        store
            .replace(
                CredentialRef::parse("user.second.token").unwrap(),
                "second-user-secret",
                CredentialSource::User,
                1,
            )
            .unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({"providers": {"openai": {"api_key": "legacy-provider-secret"}}}),
        );

        migrate_config_facade_layout(dir.path()).unwrap();
        assert!(section_layout_is_active(dir.path()).unwrap());
        for (reference, expected) in [
            ("user.first.token", "first-user-secret"),
            ("user.second.token", "second-user-secret"),
            ("provider.openai.api_key", "legacy-provider-secret"),
        ] {
            let reference = CredentialRef::parse(reference).unwrap();
            assert_eq!(
                store.resolve(&reference).unwrap().unwrap().expose(),
                expected
            );
        }
    }

    #[test]
    fn compound_migration_accepts_new_primary_over_an_older_migrated_value() {
        let _key = crate::encryption::set_test_encryption_key([117; 32]);
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({"unknown_root": true}),
        );
        write_json(
            &dir.path().join("providers.json"),
            &json!({"openai": {"api_key": "old-migrated-value"}}),
        );
        crate::migrate_provider_mcp_credentials(dir.path()).unwrap();
        let store = CredentialStore::open(dir.path());
        let before_revision = store.revision().unwrap();
        assert_eq!(
            store
                .resolve(&CredentialRef::parse("provider.openai.api_key").unwrap())
                .unwrap()
                .unwrap()
                .expose(),
            "old-migrated-value"
        );

        write_json(
            &dir.path().join("providers.json"),
            &json!({"openai": {"api_key": "new-primary-value"}}),
        );
        migrate_config_facade_layout(dir.path()).unwrap();

        assert!(section_layout_is_active(dir.path()).unwrap());
        assert!(store.revision().unwrap() > before_revision);
        assert_eq!(
            store
                .resolve(&CredentialRef::parse("provider.openai.api_key").unwrap())
                .unwrap()
                .unwrap()
                .expose(),
            "new-primary-value"
        );
    }

    #[test]
    fn empty_existing_credential_document_is_not_treated_as_missing() {
        let _key = crate::encryption::set_test_encryption_key([110; 32]);
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({"providers": {"openai": {
                "api_key_encrypted": crate::encryption::encrypt("provider-secret").unwrap()
            }}}),
        );
        std::fs::write(dir.path().join("credentials.json"), b"").unwrap();
        let before = directory_snapshot(dir.path());

        assert!(migrate_config_facade_layout(dir.path()).is_err());
        assert_eq!(directory_snapshot(dir.path()), before);
        assert!(!dir.path().join("credentials.json.lock").exists());
    }

    #[test]
    fn strict_raw_planner_rejects_env_sourced_plaintext_from_ordinary_candidate() {
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({
                "providers": {
                    "openai": {
                        "api_key": "runtime-only-plaintext",
                        "api_key_from_env": true,
                        "model": "gpt-safe"
                    }
                }
            }),
        );
        let before = directory_snapshot(dir.path());

        assert!(plan_config_facade_layout(dir.path()).is_err());
        assert_eq!(directory_snapshot(dir.path()), before);
    }

    #[test]
    fn strict_planner_uses_the_broker_from_the_same_stable_input_snapshot() {
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("broker.json"),
            &json!({
                "endpoint": "ws://late-broker.test:9600",
                "configured": false,
                "future_broker": {"preserved": true}
            }),
        );

        let plan = plan_config_facade_layout(dir.path()).unwrap();
        let subagents = plan
            .files
            .iter()
            .find(|file| file.name == "subagents.json")
            .unwrap();
        let document: Value = serde_json::from_slice(&subagents.candidate).unwrap();
        assert_eq!(
            document["data"]["external_broker"]["endpoint"],
            "ws://late-broker.test:9600"
        );
        assert_eq!(
            document["data"]["external_broker"]["future_broker"]["preserved"],
            true
        );
    }

    #[test]
    fn planner_retries_sidecar_epoch_without_resurrecting_concurrent_deletion() {
        let dir = TempDir::new().unwrap();
        let providers_path = dir.path().join("providers.json");
        write_json(
            &providers_path,
            &section_document(
                1,
                json!({
                    "provider": "openai",
                    "openai": {
                        "model": "before-model",
                        "future_deleted": {"must_not_return": true}
                    }
                }),
            ),
        );
        let replacement_path = providers_path.clone();
        set_facade_planning_test_hook(dir.path(), move || {
            write_json(
                &replacement_path,
                &section_document(
                    2,
                    json!({
                        "provider": "openai",
                        "openai": {"model": "after-model"}
                    }),
                ),
            );
        });

        let plan = plan_config_facade_layout(dir.path()).unwrap();
        let providers = plan
            .files
            .iter()
            .find(|file| file.name == "providers.json")
            .unwrap();
        let candidate: Value = serde_json::from_slice(&providers.candidate).unwrap();
        assert_eq!(candidate["data"]["openai"]["model"], "after-model");
        assert!(candidate["data"]["openai"].get("future_deleted").is_none());
        assert_eq!(providers.original, std::fs::read(providers_path).unwrap());
    }

    #[test]
    fn invalid_nonsecurity_sidecar_is_zero_write_before_credential_migration() {
        let _key = crate::encryption::set_test_encryption_key([64; 32]);
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({
                "providers": {
                    "openai": {
                        "api_key_encrypted": crate::encryption::encrypt("provider-secret").unwrap()
                    }
                }
            }),
        );
        write_json(
            &dir.path().join("memory.json"),
            &json!({"auto_dream_interval_secs": "invalid-type"}),
        );
        let before = directory_snapshot(dir.path());

        assert!(migrate_config_facade_layout(dir.path()).is_err());
        assert_eq!(directory_snapshot(dir.path()), before);
    }

    #[test]
    fn raw_legacy_projection_preserves_nested_future_fields_without_rewriting_root() {
        let _key = crate::encryption::set_test_encryption_key([62; 32]);
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({
                "server": {
                    "port": 9650,
                    "future_server": {"nested": "kept"}
                },
                "memory": {
                    "background_model": "memory-model",
                    "future_memory": {"nested": "kept"}
                },
                "subagents": {
                    "max_concurrent": 3,
                    "future_subagents": {"nested": "kept"}
                },
                "providers": {
                    "openai": {
                        "model": "gpt-safe",
                        "future_provider": {"nested": "kept"}
                    }
                },
                "notifications": {
                    "desktop": {"enabled": false},
                    "future_channel": {"nested": "kept"}
                },
                "connect": {
                    "platforms": [{
                        "id": "future-platform",
                        "type": "future-safe-adapter",
                        "allow_from": ["user-a"],
                        "future_platform": {"nested": "kept"}
                    }],
                    "future_connect": {"nested": "kept"}
                },
                "model_limits": [{
                    "model_pattern": "future-*",
                    "future_limit": {"nested": "kept"}
                }]
            }),
        );
        write_json(
            &dir.path().join("broker.json"),
            &json!({
                "endpoint": "ws://broker.test:9600",
                "configured": false,
                "future_broker": {"nested": "kept"}
            }),
        );
        let root_before = std::fs::read(dir.path().join("config.json")).unwrap();

        migrate_config_facade_layout(dir.path()).unwrap();

        assert_eq!(
            std::fs::read(dir.path().join("config.json")).unwrap(),
            root_before,
            "section split must not strip the inline connect source"
        );
        let read_data = |name: &str| -> Value {
            let document: Value =
                serde_json::from_slice(&std::fs::read(dir.path().join(name)).unwrap()).unwrap();
            document["data"].clone()
        };
        assert_eq!(
            read_data("core.json")["server"]["future_server"]["nested"],
            "kept"
        );
        assert_eq!(read_data("memory.json")["future_memory"]["nested"], "kept");
        assert_eq!(
            read_data("subagents.json")["future_subagents"]["nested"],
            "kept"
        );
        assert_eq!(
            read_data("subagents.json")["external_broker"]["future_broker"]["nested"],
            "kept"
        );
        assert_eq!(
            read_data("providers.json")["openai"]["future_provider"]["nested"],
            "kept"
        );
        assert_eq!(
            read_data("notifications.json")["notifications"]["future_channel"]["nested"],
            "kept"
        );
        assert_eq!(
            read_data("connect.json")["future_connect"]["nested"],
            "kept"
        );
        assert_eq!(
            read_data("connect.json")["platforms"][0]["future_platform"]["nested"],
            "kept"
        );
        assert_eq!(
            read_data("model_limits.json")[0]["future_limit"]["nested"],
            "kept"
        );
    }

    #[test]
    fn legacy_provider_sidecar_does_not_default_over_root_routing_metadata() {
        let _key = crate::encryption::set_test_encryption_key([63; 32]);
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({
                "provider": "anthropic",
                "defaults": {
                    "chat": {"provider": "anthropic", "model": "claude-safe"},
                    "future_defaults": {"nested": "kept"}
                },
                "provider_instances": {
                    "work": {
                        "provider_type": "openai",
                        "model": "gpt-safe",
                        "future_instance": {"nested": "kept"}
                    }
                },
                "providers": {
                    "openai": {
                        "model": "inline-model",
                        "future_provider": {"nested": "kept"}
                    }
                }
            }),
        );
        write_json(
            &dir.path().join("providers.json"),
            &json!({"openai": {"model": "sidecar-model"}}),
        );

        migrate_config_facade_layout(dir.path()).unwrap();

        let providers: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("providers.json")).unwrap())
                .unwrap();
        let data = &providers["data"];
        assert_eq!(data["provider"], "anthropic");
        assert_eq!(data["defaults"]["chat"]["provider"], "anthropic");
        assert_eq!(data["defaults"]["future_defaults"]["nested"], "kept");
        assert_eq!(
            data["provider_instances"]["work"]["future_instance"]["nested"],
            "kept"
        );
        assert_eq!(data["openai"]["model"], "sidecar-model");
        assert_eq!(data["openai"]["future_provider"]["nested"], "kept");
    }

    #[test]
    fn instance_native_dual_config_migration_drops_legacy_aliases_and_restarts() {
        let _key = crate::encryption::set_test_encryption_key([0x64; 32]);
        let dir = TempDir::new().unwrap();
        let instance_secret = "instance-native-migration-secret";
        write_json(
            &dir.path().join("config.json"),
            &json!({
                "provider": "anthropic",
                "defaults": {
                    "chat": {"provider": "work", "model": "gpt-instance"}
                },
                "features": {
                    "provider_model_ref": true,
                    "dynamic_model_routing": false
                },
                "provider_instances": {
                    "work": {
                        "provider_type": "openai",
                        "api_key": instance_secret,
                        "model": "gpt-instance",
                        "explicit_prompt_cache": false,
                        "enabled": true,
                        "future_instance": {"nested": "kept"}
                    }
                },
                "default_provider_instance": "work",
                "providers": {
                    "openai": {"model": "inline-stale"},
                    "anthropic": {"model": "inline-stale"},
                    "future_provider": {"from_root": true}
                }
            }),
        );
        write_json(
            &dir.path().join("providers.json"),
            &json!({
                "provider": "gemini",
                "openai": {"model": "sidecar-stale"},
                "gemini": {"model": "sidecar-stale"},
                "future_provider": {"from_sidecar": true}
            }),
        );

        migrate_config_facade_layout(dir.path()).unwrap();

        let providers: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("providers.json")).unwrap())
                .unwrap();
        let data = &providers["data"];
        for key in [
            "provider",
            "openai",
            "anthropic",
            "gemini",
            "copilot",
            "bodhi",
        ] {
            assert!(data.get(key).is_none(), "migrated legacy field {key}");
        }
        assert_eq!(data["default_provider_instance"], "work");
        assert_eq!(data["defaults"]["chat"]["provider"], "work");
        assert_eq!(data["features"]["provider_model_ref"], true);
        assert_eq!(
            data["provider_instances"]["work"]["future_instance"]["nested"],
            "kept"
        );
        assert_eq!(
            data["provider_instances"]["work"][crate::OPENAI_EXPLICIT_PROMPT_CACHE_CONFIG_KEY],
            false
        );
        assert_eq!(data["future_provider"]["from_root"], true);
        assert_eq!(data["future_provider"]["from_sidecar"], true);
        assert!(!std::fs::read_to_string(dir.path().join("providers.json"))
            .unwrap()
            .contains(instance_secret));

        let restarted = Config::from_data_dir_without_env(Some(dir.path().to_path_buf()));
        assert_eq!(restarted.default_provider_instance.as_deref(), Some("work"));
        assert_eq!(
            restarted.provider_instances["work"].api_key,
            instance_secret
        );
        assert_eq!(
            restarted.provider_instances["work"].extra["future_instance"]["nested"],
            "kept"
        );
        assert_eq!(
            restarted.provider_instances["work"].extra
                [crate::OPENAI_EXPLICIT_PROMPT_CACHE_CONFIG_KEY],
            false
        );
        assert!(restarted.providers().openai.is_none());
        assert!(restarted.providers().anthropic.is_none());
        assert!(restarted.providers().gemini.is_none());
        assert_eq!(
            restarted.providers().extra["future_provider"]["from_sidecar"],
            true
        );
    }

    #[test]
    fn split_crashes_recover_without_partial_authority() {
        use crate::credential_migration::{
            install_section_split_migration_with_fault, SectionSplitTestFault,
        };

        for (index, fault) in [
            SectionSplitTestFault::Staging,
            SectionSplitTestFault::Manifest,
            SectionSplitTestFault::LayoutMarker,
        ]
        .into_iter()
        .enumerate()
        {
            let _key = crate::encryption::set_test_encryption_key([70 + index as u8; 32]);
            let dir = TempDir::new().unwrap();
            write_json(
                &dir.path().join("config.json"),
                &json!({"http_proxy": format!("http://fault-{index}"), "unknown": index}),
            );
            crate::migrate_provider_mcp_credentials(dir.path()).unwrap();
            crate::migrate_cluster_credentials(dir.path()).unwrap();
            let plan = plan_config_facade_layout(dir.path()).unwrap();
            assert!(install_section_split_migration_with_fault(dir.path(), plan, fault).is_err());
            assert!(!section_layout_is_active(dir.path()).unwrap());
            assert!(ConfigFacade::open(dir.path()).is_err());
            let marker_after_fault = matches!(fault, SectionSplitTestFault::LayoutMarker)
                .then(|| std::fs::read(dir.path().join(SECTION_LAYOUT_FILE)).unwrap());
            if let Some(marker) = marker_after_fault.as_ref() {
                validate_completed_section_layout_marker(marker).unwrap();
            }

            let recovered = migrate_config_facade_layout(dir.path()).unwrap();
            if matches!(
                fault,
                SectionSplitTestFault::Manifest | SectionSplitTestFault::LayoutMarker
            ) {
                assert!(recovered.resumed);
            }
            let facade = ConfigFacade::open(dir.path()).unwrap();
            assert_eq!(
                facade.effective_config().http_proxy,
                format!("http://fault-{index}")
            );
            assert_eq!(facade.registry().health().unwrap().len(), 15);
            if let Some(marker) = marker_after_fault {
                assert_eq!(
                    std::fs::read(dir.path().join(SECTION_LAYOUT_FILE)).unwrap(),
                    marker
                );
            }
        }
    }

    #[test]
    fn committed_pending_split_recovers_before_invalid_connect_or_broker_preflight() {
        use crate::credential_migration::{
            install_section_split_migration_with_fault, SectionSplitTestFault,
        };

        for invalid_source in ["connect", "broker"] {
            let _key = crate::encryption::set_test_encryption_key(if invalid_source == "connect" {
                [71; 32]
            } else {
                [72; 32]
            });
            let dir = TempDir::new().unwrap();
            write_json(
                &dir.path().join("config.json"),
                &json!({"http_proxy": "http://before-recovery"}),
            );
            crate::migrate_provider_mcp_credentials(dir.path()).unwrap();
            crate::migrate_cluster_credentials(dir.path()).unwrap();
            let plan = plan_config_facade_layout(dir.path()).unwrap();
            assert!(install_section_split_migration_with_fault(
                dir.path(),
                plan,
                SectionSplitTestFault::Manifest,
            )
            .is_err());
            assert!(!section_layout_is_active(dir.path()).unwrap());

            if invalid_source == "connect" {
                write_json(
                    &dir.path().join("config.json"),
                    &json!({"connect": {"platforms": "invalid"}}),
                );
            } else {
                std::fs::write(dir.path().join("broker.json"), b"{ invalid").unwrap();
            }

            if invalid_source == "connect" {
                assert!(migrate_config_facade_layout(dir.path()).is_err());
            } else {
                migrate_config_facade_layout(dir.path()).unwrap();
            }
            assert!(
                section_layout_is_active(dir.path()).unwrap(),
                "committed split was not recovered before {invalid_source} preflight"
            );
        }
    }

    #[test]
    fn connect_secret_preflight_is_zero_write_even_with_other_legacy_secrets() {
        let _key = crate::encryption::set_test_encryption_key([81; 32]);
        let dir = TempDir::new().unwrap();
        let config = json!({
            "providers": {
                "openai": {"api_key_encrypted": crate::encryption::encrypt("provider-secret").unwrap()}
            },
            "connect": {
                "platforms": [{"type": "telegram", "token_encrypted": "legacy-connect-cipher"}]
            }
        });
        write_json(&dir.path().join("config.json"), &config);
        let before = std::fs::read(dir.path().join("config.json")).unwrap();

        assert!(migrate_config_facade_layout(dir.path()).is_err());
        assert_eq!(
            std::fs::read(dir.path().join("config.json")).unwrap(),
            before
        );
        assert_no_layout_transaction_artifacts(dir.path());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn connect_backup_secret_is_committed_and_scrubbed_by_compound_migration() {
        let _key = crate::encryption::set_test_encryption_key([114; 32]);
        let dir = TempDir::new().unwrap();
        let root = serde_json::to_vec_pretty(&json!({
            "providers": {"openai": {
                "api_key_encrypted": crate::encryption::encrypt("provider-secret").unwrap()
            }}
        }))
        .unwrap();
        std::fs::write(dir.path().join("config.json"), &root).unwrap();
        let connect_backup = serde_json::to_vec_pretty(&json!({
            "platforms": [{"type": "telegram", "token": "backup-token"}]
        }))
        .unwrap();
        std::fs::write(dir.path().join("connect.json.bak"), &connect_backup).unwrap();

        migrate_config_facade_layout(dir.path()).unwrap();
        let backup: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("connect.json.bak")).unwrap())
                .unwrap();
        let platform = &backup["data"]["platforms"][0];
        assert!(platform.get("token").is_none());
        assert!(platform.get("token_encrypted").is_none());
        let reference = CredentialRef::parse(
            platform["token_credential_ref"]
                .as_str()
                .unwrap()
                .to_string(),
        )
        .unwrap();
        assert_eq!(
            CredentialStore::open(dir.path())
                .resolve(&reference)
                .unwrap()
                .unwrap()
                .expose(),
            "backup-token"
        );
        assert!(dir.path().join("credentials.json").exists());
        assert!(dir.path().join(SECTION_LAYOUT_FILE).exists());
    }

    #[test]
    fn invalid_core_sidecar_is_zero_write_before_provider_credential_migration() {
        for (index, core) in [
            b"{ malformed core".to_vec(),
            serde_json::to_vec_pretty(&section_document(
                7,
                json!({"future": {"refresh_token": "must-not-migrate"}}),
            ))
            .unwrap(),
        ]
        .into_iter()
        .enumerate()
        {
            let _key = crate::encryption::set_test_encryption_key([100 + index as u8; 32]);
            let dir = TempDir::new().unwrap();
            write_json(
                &dir.path().join("config.json"),
                &json!({"providers": {"openai": {
                    "api_key_encrypted": crate::encryption::encrypt("provider-secret").unwrap()
                }}}),
            );
            std::fs::write(dir.path().join("core.json"), core).unwrap();
            let before = directory_snapshot(dir.path());

            assert!(migrate_config_facade_layout(dir.path()).is_err());
            assert_eq!(directory_snapshot(dir.path()), before);
        }
    }

    #[test]
    fn invalid_root_semantics_are_zero_write_before_builtin_provider_migration() {
        let _key = crate::encryption::set_test_encryption_key([102; 32]);
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({"providers": {"openai": {
                "api_key_encrypted": crate::encryption::encrypt("provider-secret").unwrap(),
                "base_url": "https://api.test/v1?token=literal"
            }}}),
        );
        let before = directory_snapshot(dir.path());

        assert!(migrate_config_facade_layout(dir.path()).is_err());
        assert_eq!(directory_snapshot(dir.path()), before);
    }

    #[test]
    fn conflicting_provider_plaintext_and_ciphertext_fail_closed_before_any_write() {
        let _key = crate::encryption::set_test_encryption_key([104; 32]);
        let ciphertext = crate::encryption::encrypt("ciphertext-winner").unwrap();
        for (index, config, sidecar) in [
            (
                0,
                json!({"providers": {"openai": {
                    "api_key": "plaintext-winner",
                    "api_key_encrypted": ciphertext.clone()
                }}}),
                None,
            ),
            (
                1,
                json!({"provider_instances": {"work": {
                    "provider_type": "openai",
                    "api_key": "plaintext-winner",
                    "api_key_encrypted": ciphertext.clone()
                }}}),
                None,
            ),
            (
                2,
                json!({}),
                Some(json!({"openai": {
                    "api_key": "plaintext-winner",
                    "api_key_encrypted": ciphertext.clone()
                }})),
            ),
        ] {
            let dir = TempDir::new().unwrap();
            write_json(&dir.path().join("config.json"), &config);
            if let Some(sidecar) = sidecar {
                write_json(&dir.path().join("providers.json"), &sidecar);
            }
            let before = directory_snapshot(dir.path());

            assert!(
                migrate_config_facade_layout(dir.path()).is_err(),
                "conflicting provider shape {index} unexpectedly migrated"
            );
            assert_eq!(directory_snapshot(dir.path()), before);
        }
    }

    #[test]
    fn cluster_conflict_is_zero_write_before_provider_migration() {
        let _key = crate::encryption::set_test_encryption_key([105; 32]);
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({
                "providers": {"openai": {
                    "api_key_encrypted": crate::encryption::encrypt("provider-secret").unwrap()
                }},
                "cluster_fabric": {"nodes": [{
                    "id": "node",
                    "label": "node",
                    "placement": {
                        "type": "ssh",
                        "host": "cluster.test",
                        "username": "deploy",
                        "auth": {
                            "method": "password",
                            "password": "plaintext-value",
                            "password_encrypted": crate::encryption::encrypt("different-value").unwrap()
                        }
                    }
                }]}
            }),
        );
        let before = directory_snapshot(dir.path());

        let error = migrate_config_facade_layout(dir.path()).unwrap_err();
        assert!(error.to_string().contains("conflicting values"));
        assert_eq!(directory_snapshot(dir.path()), before);
    }

    #[test]
    fn cluster_bad_ciphertext_in_primary_or_backup_is_zero_write() {
        let _key = crate::encryption::set_test_encryption_key([108; 32]);
        for bad_in_backup in [false, true] {
            let dir = TempDir::new().unwrap();
            write_json(
                &dir.path().join("config.json"),
                &json!({"providers": {"openai": {
                    "api_key_encrypted": crate::encryption::encrypt("provider-secret").unwrap()
                }}}),
            );
            let cluster = json!({"cluster_fabric": {"nodes": [{
                "id": "node",
                "label": "node",
                "placement": {
                    "type": "ssh",
                    "host": "cluster.test",
                    "username": "deploy",
                    "auth": {
                        "method": "password",
                        "password_encrypted": "not-a-valid-ciphertext"
                    }
                }
            }]}});
            if bad_in_backup {
                write_json(&dir.path().join("config.json.bak"), &cluster);
            } else {
                let mut root: Value =
                    serde_json::from_slice(&std::fs::read(dir.path().join("config.json")).unwrap())
                        .unwrap();
                root.as_object_mut()
                    .unwrap()
                    .extend(cluster.as_object().unwrap().clone());
                write_json(&dir.path().join("config.json"), &root);
            }
            let before = directory_snapshot(dir.path());

            assert!(migrate_config_facade_layout(dir.path()).is_err());
            assert_eq!(directory_snapshot(dir.path()), before);
        }
    }

    #[test]
    fn mcp_dual_secret_channels_fail_closed_before_any_write() {
        let _key = crate::encryption::set_test_encryption_key([106; 32]);
        let cases = [
            json!({
                "command": "node",
                "env": {"API_TOKEN": "plaintext"},
                "env_encrypted": {"API_TOKEN": crate::encryption::encrypt("different").unwrap()}
            }),
            json!({
                "transport": {"type": "sse", "url": "https://mcp.test/sse"},
                "headers": {"Authorization": "plaintext"},
                "headers_encrypted": {"Authorization": crate::encryption::encrypt("different").unwrap()}
            }),
            json!({
                "transport": {"type": "sse", "url": "https://mcp.test/sse"},
                "headers": [{
                    "name": "Authorization",
                    "value": "plaintext",
                    "value_encrypted": crate::encryption::encrypt("different").unwrap()
                }]
            }),
        ];
        for (index, server) in cases.into_iter().enumerate() {
            let dir = TempDir::new().unwrap();
            write_json(
                &dir.path().join("config.json"),
                &json!({"providers": {"openai": {
                    "api_key_encrypted": crate::encryption::encrypt("provider-secret").unwrap()
                }}}),
            );
            write_json(
                &dir.path().join("mcp.json"),
                &json!({"schema_version": 1, "revision": index + 1, "data": {
                    "conflict": server
                }}),
            );
            let before = directory_snapshot(dir.path());

            let error = migrate_config_facade_layout(dir.path()).unwrap_err();
            assert!(error
                .to_string()
                .contains("plaintext and ciphertext disagree"));
            assert_eq!(directory_snapshot(dir.path()), before);
        }
    }

    #[test]
    fn valid_sidecar_precedence_repairs_invalid_root_before_dry_plan() {
        let _key = crate::encryption::set_test_encryption_key([103; 32]);
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({
                "memory": "invalid-root-shape",
                "provider_instances": {"work": {
                    "provider_type": "openai",
                    "api_key_encrypted": crate::encryption::encrypt("provider-secret").unwrap()
                }}
            }),
        );
        write_json(
            &dir.path().join("memory.json"),
            &json!({"background_model": "sidecar-model"}),
        );

        migrate_config_facade_layout(dir.path()).unwrap();
        let facade = ConfigFacade::open(dir.path()).unwrap();
        assert_eq!(
            facade
                .registry()
                .memory
                .snapshot()
                .data
                .0
                .as_ref()
                .and_then(|memory| memory.background_model.as_deref()),
            Some("sidecar-model")
        );
        assert_eq!(facade.registry().credentials.snapshot().data.len(), 1);
    }

    #[test]
    fn malformed_optional_broker_does_not_block_main_layout_or_provider_migration() {
        let _key = crate::encryption::set_test_encryption_key([82; 32]);
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({
                "provider_instances": {
                    "work": {
                        "provider_type": "openai",
                        "api_key_encrypted": crate::encryption::encrypt("provider-secret").unwrap()
                    }
                }
            }),
        );
        std::fs::write(dir.path().join("broker.json"), b"{ malformed").unwrap();
        let broker_before = std::fs::read(dir.path().join("broker.json")).unwrap();

        let outcome = migrate_config_facade_layout(dir.path()).unwrap();
        assert!(outcome.activated);
        assert!(section_layout_is_active(dir.path()).unwrap());
        assert_eq!(
            std::fs::read(dir.path().join("broker.json")).unwrap(),
            broker_before
        );
        let config = std::fs::read_to_string(dir.path().join("config.json")).unwrap();
        assert!(!config.contains("api_key_encrypted"));
        let facade = ConfigFacade::open(dir.path()).unwrap();
        assert!(facade
            .registry()
            .subagents
            .snapshot()
            .data
            .external_broker
            .is_none());
        assert_eq!(facade.registry().credentials.snapshot().data.len(), 1);

        write_json(
            &dir.path().join("broker.json"),
            &json!({
                "endpoint": "wss://repaired-broker.test/ws",
                "token_encrypted": crate::encryption::encrypt("broker-secret").unwrap(),
                "future_broker": {"preserved": true}
            }),
        );
        let repaired = migrate_config_facade_layout(dir.path()).unwrap();
        assert!(!repaired.activated);
        let facade = ConfigFacade::open(dir.path()).unwrap();
        let broker = facade
            .registry()
            .subagents
            .snapshot()
            .data
            .external_broker
            .clone()
            .expect("repaired broker must be adopted into an active layout");
        assert_eq!(broker.endpoint, "wss://repaired-broker.test/ws");
        assert!(broker.configured);
        let subagents: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("subagents.json")).unwrap())
                .unwrap();
        assert_eq!(
            subagents["data"]["external_broker"]["future_broker"]["preserved"],
            true
        );
        assert_eq!(facade.registry().credentials.snapshot().data.len(), 2);
    }

    #[test]
    fn unknown_broker_secret_is_isolated_from_the_main_layout() {
        let _key = crate::encryption::set_test_encryption_key([105; 32]);
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({"provider_instances": {"work": {
                "provider_type": "openai",
                "api_key_encrypted": crate::encryption::encrypt("provider-secret").unwrap()
            }}}),
        );
        write_json(
            &dir.path().join("broker.json"),
            &json!({
                "endpoint": "wss://broker.test/ws",
                "future_token": "must-remain-isolated"
            }),
        );
        let broker_before = std::fs::read(dir.path().join("broker.json")).unwrap();

        migrate_config_facade_layout(dir.path()).unwrap();
        assert_eq!(
            std::fs::read(dir.path().join("broker.json")).unwrap(),
            broker_before
        );
        let facade = ConfigFacade::open(dir.path()).unwrap();
        assert!(facade
            .registry()
            .subagents
            .snapshot()
            .data
            .external_broker
            .is_none());
        assert_eq!(facade.registry().credentials.snapshot().data.len(), 1);
    }

    #[test]
    fn existing_sections_win_and_nested_unknown_fields_survive_split() {
        let _key = crate::encryption::set_test_encryption_key([83; 32]);
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({
                "http_proxy": "http://legacy",
                "server": {"port": 9000},
                "notifications": {"desktop": {"enabled": false}}
            }),
        );
        write_json(
            &dir.path().join("core.json"),
            &section_document(
                7,
                json!({
                    "http_proxy": "http://section",
                    "server": {"port": 9777, "future_server": {"nested": "kept"}},
                    "future_core": {"nested": {"kept": true}}
                }),
            ),
        );
        write_json(
            &dir.path().join("notifications.json"),
            &section_document(
                3,
                json!({
                    "notifications": {
                        "desktop": {"enabled": true},
                        "future_channel": {"nested": "kept"}
                    },
                    "connect": {"platforms": []}
                }),
            ),
        );
        write_json(
            &dir.path().join("connect.json"),
            &json!({
                "platforms": [{
                    "id": "safe-platform",
                    "type": "future-safe-adapter",
                    "allow_from": ["user-a"],
                    "future_platform": {"nested": "kept"}
                }],
                "future_connect": {"nested": true}
            }),
        );

        migrate_config_facade_layout(dir.path()).unwrap();
        let facade = ConfigFacade::open(dir.path()).unwrap();
        assert_eq!(facade.effective_config().http_proxy, "http://section");
        assert_eq!(facade.effective_config().server.port, 9777);
        let core: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("core.json")).unwrap()).unwrap();
        assert_eq!(core["revision"], 8);
        assert_eq!(core["data"]["server"]["future_server"]["nested"], "kept");
        assert_eq!(core["data"]["future_core"]["nested"]["kept"], true);
        let notifications: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("notifications.json")).unwrap())
                .unwrap();
        assert_eq!(notifications["revision"], 4);
        assert_eq!(
            notifications["data"]["notifications"]["future_channel"]["nested"],
            "kept"
        );
        let connect: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("connect.json")).unwrap())
                .unwrap();
        assert_eq!(connect["schema_version"], SECTION_SCHEMA_VERSION);
        assert_eq!(
            connect["data"]["platforms"][0]["future_platform"]["nested"],
            "kept"
        );
        assert_eq!(connect["data"]["future_connect"]["nested"], true);
    }

    #[test]
    fn partial_envelope_markers_remain_ordinary_unknown_data() {
        for value in [
            json!({"schema_version": 7, "future": "schema-only"}),
            json!({"revision": 7, "future": "revision-only"}),
            json!({"data": {"nested": true}, "future": "data-only"}),
            json!({"schema_version": 1, "revision": 7, "future": "no-data"}),
            json!({"schema_version": 1, "data": {"nested": true}, "future": "no-revision"}),
            json!({"revision": 7, "data": {"nested": true}, "future": "no-schema"}),
        ] {
            let (revision, data) =
                compatible_section_data(&serde_json::to_vec(&value).unwrap()).unwrap();
            assert_eq!(revision, 0);
            assert_eq!(data, Some(value));
        }

        let envelope = section_document(7, json!({"future": "unwrapped"}));
        let (revision, data) =
            compatible_section_data(&serde_json::to_vec(&envelope).unwrap()).unwrap();
        assert_eq!(revision, 7);
        assert_eq!(data, Some(json!({"future": "unwrapped"})));
    }

    #[test]
    fn section_validators_reject_semantic_and_secret_candidates() {
        assert!(validate_env(&EnvSection(vec![EnvVarEntry {
            name: "VISIBLE".to_string(),
            value: "safe".to_string(),
            secret: false,
            value_encrypted: None,
            credential_ref: None,
            configured: true,
            description: None,
        }]))
        .is_ok());
        let isolated_secret_env = serde_json::to_vec(&section_document(
            1,
            json!([{
                "name": "PRIVATE_TOKEN",
                "value": "",
                "secret": true,
                "credential_ref": "env.PRIVATE_TOKEN.value",
                "configured": true
            }]),
        ))
        .unwrap();
        assert!(validate_section_envelope("env.json", &isolated_secret_env, 1).is_ok());
        for (name, data) in [
            (
                "env.json",
                json!([{"name": "9INVALID", "value": "safe", "secret": false}]),
            ),
            (
                "model-policy.json",
                json!({"keyword_masking": {"entries": [{"pattern": "[", "match_type": "regex"}]}}),
            ),
            (
                "subagents.json",
                json!({"external_broker": {"endpoint": "ws://broker", "credential_ref": "bad/ref", "configured": true}}),
            ),
            (
                "subagents.json",
                json!({"external_broker": {
                    "endpoint": "wss://user:pass@broker.test/ws?token=literal#secret",
                    "credential_ref": "broker.external.token",
                    "configured": true
                }}),
            ),
            ("core.json", json!({"token": "must-not-persist"})),
            (
                "core.json",
                json!({"future": {"ClientSecret": "must-not-persist"}}),
            ),
            (
                "core.json",
                json!({"future": {"secret": "must-not-persist"}}),
            ),
            (
                "core.json",
                json!({"future": {"apiKeyEncrypted": "must-not-persist"}}),
            ),
            (
                "core.json",
                json!({"future": {"valueEncrypted": "must-not-persist"}}),
            ),
            (
                "core.json",
                json!({"future": {"refresh_token": "must-not-persist"}}),
            ),
            (
                "core.json",
                json!({"future": {"csrfToken": "must-not-persist"}}),
            ),
            (
                "core.json",
                json!({"future_credential_refs": {"api_key": "plaintext"}}),
            ),
            (
                "core.json",
                json!({"provider_instances": {"api_key": "plaintext"}}),
            ),
            (
                "core.json",
                json!({"provider_instances": {"work": {"future_password": "plaintext"}}}),
            ),
            (
                "core.json",
                json!({"credential_refs": {"work": {"future_password": "plaintext"}}}),
            ),
            (
                "providers.json",
                json!({"provider": "openai", "future": {
                    "headers": {"Authorization": "literal-secret"}
                }}),
            ),
            (
                "providers.json",
                json!({"provider": "openai", "openai": {
                    "Request_Overrides": {"common": {"body_patch": [{
                        "path": "/token", "value": "literal-secret"
                    }]}}
                }}),
            ),
            (
                "providers.json",
                json!({
                    "provider": "openai",
                    "provider_instances": {
                        "headers": {
                            "provider_type": "openai",
                            "api_key": "must-not-persist",
                            "credential_ref": "provider.headers.api_key"
                        }
                    }
                }),
            ),
        ] {
            let bytes = serde_json::to_vec(&section_document(1, data)).unwrap();
            assert!(
                validate_section_envelope(name, &bytes, 1).is_err(),
                "{name} candidate unexpectedly passed"
            );
        }

        let verifier_metadata = serde_json::to_vec(&section_document(
            1,
            json!({
                "password_hash": "argon2-verifier",
                "password_salt": "public-salt",
                "device_token_hash": "device-verifier",
                "device_token_salt": "public-device-salt"
            }),
        ))
        .unwrap();
        assert!(validate_section_envelope("access-control.json", &verifier_metadata, 1).is_err());

        let isolated_verifier_metadata = serde_json::to_vec(&section_document(
            1,
            json!({
                "password_enabled": true,
                "password_credential_ref": "access.root.password_verifier",
                "password_configured": true,
                "devices": [{
                    "device_id": "bamboo_device01",
                    "label": "Phone",
                    "token_credential_ref": "access.bamboo_device01.device_token_verifier",
                    "token_configured": true,
                    "created_at": "2026-07-21T00:00:00Z"
                }]
            }),
        ))
        .unwrap();
        assert!(
            validate_section_envelope("access-control.json", &isolated_verifier_metadata, 1)
                .is_ok()
        );

        let canonical_broker = serde_json::to_vec(&section_document(
            1,
            json!({"external_broker": {
                "endpoint": "wss://broker.test/ws",
                "credential_ref": "broker.external.token",
                "configured": true
            }}),
        ))
        .unwrap();
        assert!(validate_section_envelope("subagents.json", &canonical_broker, 1).is_ok());

        let safe_mcp_refs = serde_json::to_vec(&section_document(
            1,
            json!({
                "safe-stdio": {
                    "command": "node",
                    "disabled": true,
                    "env_credential_refs": {"TOKEN": "mcp.safe-stdio.env_TOKEN"}
                },
                "safe-http": {
                    "url": "https://mcp.test/rpc",
                    "transport_kind": "streamable_http",
                    "disabled": true,
                    "header_credential_refs": {"X-API-Key": "mcp.safe-http.header_X_API_Key"}
                }
            }),
        ))
        .unwrap();
        assert!(validate_section_envelope("mcp.json", &safe_mcp_refs, 1).is_ok());

        let safe_provider_identifier = serde_json::to_vec(&section_document(
            1,
            json!({
                "provider": "openai",
                "provider_instances": {
                    "token": {"provider_type": "openai", "model": "gpt-test"}
                }
            }),
        ))
        .unwrap();
        assert!(validate_section_envelope("providers.json", &safe_provider_identifier, 1).is_ok());

        let safe_identifier_maps = serde_json::to_vec(&section_document(
            1,
            json!({
                "provider_instances": {
                    "password": {"provider_type": "openai", "model": "gpt-test"}
                },
                "credential_refs": {"token": "provider.password.api_key"},
                "headers": {"X-Token-Budget": "1000"}
            }),
        ))
        .unwrap();
        assert!(validate_section_envelope("core.json", &safe_identifier_maps, 1).is_ok());

        for (pattern, match_type) in [(".*", "regex"), ("***", "exact")] {
            let bytes = serde_json::to_vec(&section_document(
                1,
                json!({
                    "keyword_masking": {
                        "entries": [{"pattern": pattern, "match_type": match_type}]
                    }
                }),
            ))
            .unwrap();
            assert!(
                validate_section_envelope("model-policy.json", &bytes, 1).is_ok(),
                "legitimate masking pattern {pattern:?} was mistaken for a secret mask"
            );
        }
    }

    #[test]
    fn section_validators_reject_noncanonical_secret_channels() {
        for url in [
            "http://user:password@example.test/path",
            "https://example.test/path?token=secret",
            "https://example.test/path#secret",
        ] {
            assert!(validate_secret_free_http_url("test", url, false).is_err());
        }
        assert!(validate_secret_free_http_url("test", "https://example.test/path", false).is_ok());

        let invalid_sections = [
            (
                "core.json",
                json!({"http_proxy": "http://user:password@proxy.test:8080"}),
            ),
            (
                "core.json",
                json!({"https_proxy": "https://proxy.test:8443?token=secret"}),
            ),
            (
                "providers.json",
                json!({"provider": "openai", "openai": {"base_url": "https://api.test/v1?api_key=secret"}}),
            ),
            (
                "providers.json",
                json!({
                    "provider": "openai",
                    "openai": {
                        "request_overrides": {
                            "common": {"headers": {"Authorization": "Bearer literal-secret"}}
                        }
                    }
                }),
            ),
            (
                "providers.json",
                json!({
                    "provider": "openai",
                    "openai": {
                        "request_overrides": {
                            "common": {"headers": {"Authorization": {"type": "env_ref", "name": "AUTH", "fallback": "hardcoded-secret"}}}
                        }
                    }
                }),
            ),
            (
                "providers.json",
                json!({
                    "provider": "openai",
                    "openai": {
                        "request_overrides": {
                            "common": {"headers": {"Authorization": {"type": "format", "template": "Bearer hardcoded-secret"}}}
                        }
                    }
                }),
            ),
            (
                "providers.json",
                json!({
                    "provider": "openai",
                    "openai": {
                        "request_overrides": {
                            "common": {"headers": {"X-Auth-Token": "literal-secret"}}
                        }
                    }
                }),
            ),
            (
                "providers.json",
                json!({
                    "provider": "openai",
                    "openai": {
                        "request_overrides": {
                            "common": {"body_patch": [
                                {"path": "/access_token", "value": "literal-secret"},
                                {"path": "/client_secret", "value": "literal-secret"},
                                {"path": "/authToken", "op": "remove", "value": "ignored-but-persisted-secret"}
                            ]}
                        }
                    }
                }),
            ),
            (
                "providers.json",
                json!({
                    "provider": "openai",
                    "openai": {
                        "request_overrides": {
                            "common": {"body_patch": [{"path": "/api_key", "value": "literal-secret"}]}
                        }
                    }
                }),
            ),
            (
                "providers.json",
                json!({
                    "provider": "openai",
                    "openai": {
                        "request_overrides": {
                            "endpoints": {"responses": {"body_patch": [{"path": "auth.token", "value": {"raw": "literal-secret"}}]}}
                        }
                    }
                }),
            ),
            (
                "providers.json",
                json!({
                    "provider": "openai",
                    "openai": {
                        "request_overrides": {
                            "rules": [{
                                "model_pattern": "gpt-*",
                                "scope": {"body_patch": [{"path": "/password", "value": {"type": "literal", "value": "secret"}}]}
                            }]
                        }
                    }
                }),
            ),
            (
                "providers.json",
                json!({
                    "provider": "openai",
                    "openai": {
                        "request_overrides": {
                            "endpoints": {"responses": {"headers": {"Cookie": "session=literal"}}}
                        }
                    }
                }),
            ),
            (
                "providers.json",
                json!({
                    "provider": "openai",
                    "openai": {
                        "request_overrides": {
                            "rules": [{
                                "model_pattern": "gpt-*",
                                "scope": {"headers": {"X-API-Key": {"type": "literal", "value": "secret"}}}
                            }]
                        }
                    }
                }),
            ),
            (
                "providers.json",
                json!({
                    "provider": "openai",
                    "openai": {"request_overrides": {"common": {"headers": {
                        "Authorization": {"type": "format", "template": "机密{env:AUTH_TOKEN}"}
                    }}}}
                }),
            ),
            (
                "providers.json",
                json!({
                    "provider": "openai",
                    "openai": {"request_overrides": {"common": {"headers": {
                        "Authorization": {"type": "format", "template": "****{env:AUTH_TOKEN}"}
                    }}}}
                }),
            ),
            (
                "providers.json",
                json!({
                    "provider": "openai",
                    "openai": {"request_overrides": {"common": {"headers": {
                        "Authorization": {"type": "env_ref", "name": "AUTH_TOKEN", "literal": "must-not-persist"}
                    }}}}
                }),
            ),
            (
                "providers.json",
                json!({
                    "provider": "openai",
                    "openai": {"request_overrides": {"common": {"headers": {
                        "Authorization": {"type": "generated", "generator": "uuid", "fallback": "must-not-persist"}
                    }}}}
                }),
            ),
            (
                "providers.json",
                json!({
                    "provider": "openai",
                    "openai": {"request_overrides": {"common": {"headers": {
                        "Authorization": {"type": "format", "template": "Bearer {env:AUTH_TOKEN}", "value": "must-not-persist"}
                    }}}}
                }),
            ),
            (
                "providers.json",
                json!({
                    "provider": "openai",
                    "openai": {"request_overrides": {
                        "future": {"auth": "must-not-persist"}
                    }}
                }),
            ),
            (
                "providers.json",
                json!({
                    "provider": "openai",
                    "openai": {"request_overrides": {"common": {"body_patch": [{
                        "path": "/model",
                        "value": "gpt-test",
                        "fallback": "must-not-persist"
                    }]}}}
                }),
            ),
            (
                "mcp.json",
                json!({
                    "version": 1,
                    "servers": [{
                        "id": "empty-url",
                        "transport": {"type": "sse", "url": ""}
                    }]
                }),
            ),
            (
                "mcp.json",
                json!({
                    "version": 1,
                    "servers": [{
                        "id": "query-secret",
                        "transport": {"type": "streamable_http", "url": "https://mcp.test/rpc?token=secret"}
                    }]
                }),
            ),
            (
                "mcp.json",
                json!({
                    "version": 1,
                    "servers": [{
                        "id": "zero-request",
                        "request_timeout_ms": 0,
                        "transport": {"type": "stdio", "command": "node"}
                    }]
                }),
            ),
            (
                "mcp.json",
                json!({
                    "version": 1,
                    "servers": [{
                        "id": "zero-health",
                        "healthcheck_interval_ms": 0,
                        "transport": {"type": "stdio", "command": "node"}
                    }]
                }),
            ),
            (
                "mcp.json",
                json!({
                    "version": 1,
                    "servers": [{
                        "id": "zero-startup",
                        "transport": {"type": "stdio", "command": "node", "startup_timeout_ms": 0}
                    }]
                }),
            ),
            (
                "mcp.json",
                json!({
                    "version": 1,
                    "servers": [{
                        "id": "zero-connect",
                        "transport": {"type": "sse", "url": "https://mcp.test/sse", "connect_timeout_ms": 0}
                    }]
                }),
            ),
            (
                "notifications.json",
                json!({
                    "notifications": {
                        "ntfy": {
                            "base_url": "https://user:literal-secret@ntfy.test",
                            "topic": "alerts"
                        }
                    }
                }),
            ),
            (
                "notifications.json",
                json!({
                    "notifications": {
                        "bark": {
                            "base_url": "https://bark.test?device_key=literal-secret"
                        }
                    }
                }),
            ),
            (
                "notifications.json",
                json!({
                    "notifications": {
                        "ntfy": {
                            "base_url": "https://ntfy.test",
                            "token": ""
                        }
                    }
                }),
            ),
            (
                "connect.json",
                json!({
                    "platforms": [{
                        "type": "feishu",
                        "domain": "https://user:literal-secret@feishu.test",
                        "allow_from": ["owner"]
                    }]
                }),
            ),
            (
                "connect.json",
                json!({
                    "platforms": [{
                        "type": "telegram",
                        "token_encrypted": "",
                        "allow_from": ["owner"]
                    }]
                }),
            ),
        ];
        for (name, data) in invalid_sections {
            let bytes = serde_json::to_vec(&section_document(1, data)).unwrap();
            assert!(
                validate_section_envelope(name, &bytes, 1).is_err(),
                "{name} noncanonical candidate unexpectedly passed"
            );
        }
        for header in [
            "X-Refresh-Token",
            "X-CSRF-Token",
            "X-Token",
            "X-Auth-Key",
            "X-Auth",
            "Authentication",
            "X-Authorization",
            "X-Access-Key",
            "X-Credential",
        ] {
            let bytes = serde_json::to_vec(&section_document(
                1,
                json!({
                    "provider": "openai",
                    "openai": {
                        "request_overrides": {
                            "common": {"headers": {(header): "literal-secret"}}
                        }
                    }
                }),
            ))
            .unwrap();
            assert!(
                validate_section_envelope("providers.json", &bytes, 1).is_err(),
                "credential header {header} unexpectedly accepted a literal"
            );
        }
        for path in [
            "/access_key",
            "/auth/key",
            "auth.key",
            "/access/key",
            "/auth~1key",
            "/credential",
            "/credentials",
        ] {
            let bytes = serde_json::to_vec(&section_document(
                1,
                json!({
                    "provider": "openai",
                    "openai": {"request_overrides": {"common": {"body_patch": [{
                        "path": path, "value": "literal-secret"
                    }]}}}
                }),
            ))
            .unwrap();
            assert!(
                validate_section_envelope("providers.json", &bytes, 1).is_err(),
                "credential body path {path} unexpectedly accepted a literal"
            );
        }

        let safe_provider = serde_json::to_vec(&section_document(
            1,
            json!({
                "provider": "openai",
                "openai": {
                    "base_url": "https://api.test/v1",
                    "request_overrides": {
                        "common": {
                            "headers": {
                                "Authorization": {"type": "env_ref", "name": "CUSTOM_AUTH"}
                            }
                        }
                    }
                }
            }),
        ))
        .unwrap();
        assert!(validate_section_envelope("providers.json", &safe_provider, 1).is_ok());

        let safe_provider_env = serde_json::to_vec(&section_document(
            1,
            json!({
                "provider": "openai",
                "openai": {
                    "request_overrides": {
                        "common": {
                            "headers": {
                                "X-Auth-Token": {"type": "env_ref", "name": "AUTH_TOKEN"},
                                "Authorization": {"type": "format", "template": "Bearer {env:AUTH_TOKEN}"},
                                "X-Session-Token": {"type": "generated", "generator": "uuid"},
                                "X-Token-Budget": "1000"
                            },
                            "body_patch": [{"path": "/api_key", "value": {"type": "env_ref", "name": "API_KEY"}}]
                        }
                    }
                }
            }),
        ))
        .unwrap();
        assert!(validate_section_envelope("providers.json", &safe_provider_env, 1).is_ok());
    }

    #[test]
    fn provider_validator_enforces_credential_store_coherence_for_all_provider_shapes() {
        let mut builtin: ProvidersSection = serde_json::from_value(json!({
            "provider": "openai",
            "openai": {"api_key": "plaintext"}
        }))
        .unwrap();
        assert!(validate_providers(&builtin).is_err());
        builtin.providers.openai.as_mut().unwrap().api_key_from_env = true;
        assert!(validate_providers(&builtin).is_ok());
        builtin.providers.openai.as_mut().unwrap().api_key_from_env = false;
        builtin.providers.openai.as_mut().unwrap().credential_ref =
            Some(CredentialRef::parse("provider.openai.api_key").unwrap());
        assert!(validate_providers(&builtin).is_ok());
        builtin.providers.openai.as_mut().unwrap().api_key_encrypted =
            Some("legacy-ciphertext".to_string());
        assert!(validate_providers(&builtin).is_err());

        let instance_without_ref: ProvidersSection = serde_json::from_value(json!({
            "provider": "openai",
            "provider_instances": {
                "work": {"provider_type": "openai", "api_key": "plaintext"}
            }
        }))
        .unwrap();
        assert!(validate_providers(&instance_without_ref).is_err());
        let instance_with_ref: ProvidersSection = serde_json::from_value(json!({
            "provider": "openai",
            "provider_instances": {
                "work": {
                    "provider_type": "openai",
                    "api_key": "plaintext",
                    "credential_ref": "provider.work.api_key"
                }
            }
        }))
        .unwrap();
        assert!(validate_providers(&instance_with_ref).is_ok());
        let instance_with_ciphertext: ProvidersSection = serde_json::from_value(json!({
            "provider": "openai",
            "provider_instances": {
                "work": {
                    "provider_type": "openai",
                    "api_key_encrypted": "legacy-ciphertext",
                    "credential_ref": "provider.work.api_key"
                }
            }
        }))
        .unwrap();
        assert!(validate_providers(&instance_with_ciphertext).is_err());

        let bodhi: ProvidersSection = serde_json::from_value(json!({
            "provider": "bodhi",
            "bodhi": {"api_key": "plaintext"}
        }))
        .unwrap();
        assert!(validate_providers(&bodhi).is_err());
    }

    #[test]
    fn valid_nested_cluster_credential_refs_migrate_and_load() {
        let _key = crate::encryption::set_test_encryption_key([84; 32]);
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({
                "cluster_fabric": {
                    "nodes": [{
                        "id": "node-a",
                        "label": "Node A",
                        "placement": {
                            "type": "ssh",
                            "host": "node-a.test",
                            "username": "bamboo",
                            "auth": {"method": "password", "password": "cluster-secret"}
                        }
                    }]
                }
            }),
        );

        migrate_config_facade_layout(dir.path()).unwrap();
        let facade = ConfigFacade::open(dir.path()).unwrap();
        let cluster = &facade.registry().cluster_fabric.snapshot().data.0;
        let refs = cluster.credential_refs.get("node-a").unwrap();
        assert_eq!(
            refs.password_credential_ref
                .as_ref()
                .map(CredentialRef::as_str),
            Some("cluster.node-a.password")
        );
        assert!(refs.password_configured);
        assert_eq!(facade.registry().credentials.statuses().len(), 1);
    }

    #[test]
    fn valid_post_manifest_section_edit_is_adopted() {
        use crate::credential_migration::{
            install_section_split_migration_with_fault, SectionSplitTestFault,
        };

        let _key = crate::encryption::set_test_encryption_key([85; 32]);
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({"http_proxy": "http://legacy"}),
        );
        crate::migrate_provider_mcp_credentials(dir.path()).unwrap();
        crate::migrate_cluster_credentials(dir.path()).unwrap();
        let plan = plan_config_facade_layout(dir.path()).unwrap();
        assert!(install_section_split_migration_with_fault(
            dir.path(),
            plan,
            SectionSplitTestFault::Manifest,
        )
        .is_err());
        write_json(
            &dir.path().join("core.json"),
            &section_document(
                5,
                serde_json::to_value(CoreSection {
                    http_proxy: "http://external".to_string(),
                    ..CoreSection::default()
                })
                .unwrap(),
            ),
        );

        migrate_config_facade_layout(dir.path()).unwrap();
        assert_eq!(
            ConfigFacade::open(dir.path())
                .unwrap()
                .effective_config()
                .http_proxy,
            "http://external"
        );
        let manifest: Value = serde_json::from_slice(
            &std::fs::read(dir.path().join("config-credential-migration.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(manifest["state"], "complete");
        let marker = validate_completed_section_layout_marker(
            &std::fs::read(dir.path().join(SECTION_LAYOUT_FILE)).unwrap(),
        )
        .unwrap();
        let core_bytes = std::fs::read(dir.path().join("core.json")).unwrap();
        assert_eq!(marker.members["core.json"].revision, 5);
        assert_eq!(
            marker.members["core.json"].sha256,
            hex::encode(Sha256::digest(core_bytes))
        );
    }

    #[test]
    fn same_revision_post_manifest_section_edit_is_rejected() {
        use crate::credential_migration::{
            install_section_split_migration_with_fault, SectionSplitTestFault,
        };

        let _key = crate::encryption::set_test_encryption_key([90; 32]);
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({"http_proxy": "http://legacy"}),
        );
        crate::migrate_provider_mcp_credentials(dir.path()).unwrap();
        crate::migrate_cluster_credentials(dir.path()).unwrap();
        let plan = plan_config_facade_layout(dir.path()).unwrap();
        let core = plan
            .files
            .iter()
            .find(|file| file.name == "core.json")
            .unwrap();
        let mut same_revision: Value = serde_json::from_slice(&core.candidate).unwrap();
        same_revision["data"]["http_proxy"] = json!("http://same-revision-writer");
        let same_revision = serde_json::to_vec_pretty(&same_revision).unwrap();
        assert!(install_section_split_migration_with_fault(
            dir.path(),
            plan,
            SectionSplitTestFault::Manifest,
        )
        .is_err());
        std::fs::write(dir.path().join("core.json"), &same_revision).unwrap();

        let error = migrate_config_facade_layout(dir.path()).unwrap_err();
        assert!(error.to_string().contains("without advancing its revision"));
        assert_eq!(
            std::fs::read(dir.path().join("core.json")).unwrap(),
            same_revision
        );
        assert!(!section_layout_is_active(dir.path()).unwrap());
    }

    #[test]
    fn secret_post_manifest_edit_aborts_without_wedging_or_overwriting_it() {
        use crate::credential_migration::{
            install_section_split_migration_with_fault, SectionSplitTestFault,
        };

        let _key = crate::encryption::set_test_encryption_key([86; 32]);
        let dir = TempDir::new().unwrap();
        write_json(
            &dir.path().join("config.json"),
            &json!({"http_proxy": "http://legacy"}),
        );
        crate::migrate_provider_mcp_credentials(dir.path()).unwrap();
        crate::migrate_cluster_credentials(dir.path()).unwrap();
        let plan = plan_config_facade_layout(dir.path()).unwrap();
        assert!(install_section_split_migration_with_fault(
            dir.path(),
            plan,
            SectionSplitTestFault::Manifest,
        )
        .is_err());
        let invalid = serde_json::to_vec_pretty(&section_document(
            5,
            json!({"http_proxy": "http://external", "token": "external-secret"}),
        ))
        .unwrap();
        std::fs::write(dir.path().join("core.json"), &invalid).unwrap();

        assert!(migrate_config_facade_layout(dir.path()).is_err());
        assert_eq!(
            std::fs::read(dir.path().join("core.json")).unwrap(),
            invalid
        );
        assert!(!dir.path().join(SECTION_LAYOUT_FILE).exists());
        assert!(!dir.path().join("config-credential-migration.json").exists());
        assert!(!dir
            .path()
            .join("config-credential-migration.journal.json")
            .exists());
        assert!(!dir.path().join("credentials.json").exists());

        std::fs::remove_file(dir.path().join("core.json")).unwrap();
        migrate_config_facade_layout(dir.path()).unwrap();
        assert!(section_layout_is_active(dir.path()).unwrap());
    }

    #[test]
    fn credential_reload_never_drops_lkg_for_missing_invalid_or_reused_revision() {
        let _key = crate::encryption::set_test_encryption_key([87; 32]);
        let dir = TempDir::new().unwrap();
        let registry = SectionRegistry::open(dir.path()).unwrap();
        let reference = CredentialRef::parse("provider.openai.api_key").unwrap();
        let initial = registry.credentials.snapshot();
        assert_eq!(initial.revision, 0);
        registry
            .credentials
            .replace(reference, "secret", CredentialSource::User, 0)
            .unwrap();
        let committed = registry.credentials.snapshot();
        assert_eq!(committed.revision, 1);
        assert_eq!(committed.data.len(), 1);
        let credential_path = dir.path().join("credentials.json");
        let committed_bytes = std::fs::read(&credential_path).unwrap();

        std::fs::remove_file(&credential_path).unwrap();
        let missing = registry.credentials.reload().unwrap();
        assert_eq!(missing.revision, committed.revision);
        assert_eq!(missing.data.as_ref(), committed.data.as_ref());
        assert_eq!(missing.status, SectionStatus::Missing);
        assert!(missing.last_error.is_some());

        std::fs::write(&credential_path, &committed_bytes).unwrap();
        let restored = registry.credentials.reload().unwrap();
        assert_eq!(restored.status, SectionStatus::Healthy);
        assert_eq!(restored.revision, committed.revision);
        assert_eq!(restored.data.as_ref(), committed.data.as_ref());

        std::fs::write(&credential_path, b"{ invalid").unwrap();
        assert!(registry.credentials.reload().is_err());
        let invalid = registry.credentials.snapshot();
        assert_eq!(invalid.revision, 1);
        assert_eq!(invalid.data.len(), 1);
        assert_eq!(invalid.status, SectionStatus::Invalid);
        assert!(invalid.loaded_at >= committed.loaded_at);

        std::fs::write(&credential_path, &committed_bytes).unwrap();
        assert_eq!(
            registry.credentials.reload().unwrap().status,
            SectionStatus::Healthy
        );
        let mut reused_revision: Value = serde_json::from_slice(&committed_bytes).unwrap();
        reused_revision["data"]["entries"] = json!({});
        std::fs::write(
            &credential_path,
            serde_json::to_vec_pretty(&reused_revision).unwrap(),
        )
        .unwrap();
        let reused = registry.credentials.reload().unwrap();
        assert_eq!(reused.status, SectionStatus::Invalid);
        assert_eq!(reused.revision, committed.revision);
        assert_eq!(reused.data.as_ref(), committed.data.as_ref());
        assert!(reused.last_error.is_some());
    }

    #[test]
    fn credential_degraded_backup_reload_retains_newer_live_snapshot() {
        let _key = crate::encryption::set_test_encryption_key([88; 32]);
        let dir = TempDir::new().unwrap();
        let section = CredentialSection::open(dir.path(), false);
        let first = CredentialRef::parse("provider.openai.api_key").unwrap();
        let second = CredentialRef::parse("provider.anthropic.api_key").unwrap();
        section
            .replace(first, "first-secret", CredentialSource::User, 0)
            .unwrap();
        section
            .replace(second, "second-secret", CredentialSource::User, 1)
            .unwrap();
        let committed = section.snapshot();
        assert_eq!(committed.revision, 2);
        assert_eq!(committed.data.len(), 2);

        std::fs::write(section.store.path(), b"{ corrupt primary").unwrap();
        let degraded = section.reload().unwrap();
        assert_eq!(degraded.status, SectionStatus::Degraded);
        assert_eq!(degraded.source_kind, SectionSourceKind::Backup);
        assert_eq!(degraded.revision, committed.revision);
        assert_eq!(degraded.data.as_ref(), committed.data.as_ref());
        assert!(degraded.last_error.is_some());
    }

    #[test]
    fn credential_degraded_live_revision_repairs_replace_and_clear_at_revision_three() {
        let _key = crate::encryption::set_test_encryption_key([90; 32]);
        let first = CredentialRef::parse("provider.openai.api_key").unwrap();
        let second = CredentialRef::parse("provider.anthropic.api_key").unwrap();

        let replace_dir = TempDir::new().unwrap();
        let replace_section = CredentialSection::open(replace_dir.path(), false);
        replace_section
            .replace(first.clone(), "first-secret", CredentialSource::User, 0)
            .unwrap();
        replace_section
            .replace(second.clone(), "second-secret", CredentialSource::User, 1)
            .unwrap();
        std::fs::write(replace_section.store.path(), b"{ corrupt primary").unwrap();
        let degraded = replace_section.reload().unwrap();
        assert_eq!(degraded.status, SectionStatus::Degraded);
        assert_eq!(degraded.revision, 2);
        let (revision, status) = replace_section
            .replace(first.clone(), "repaired-secret", CredentialSource::User, 2)
            .unwrap();
        assert_eq!(revision, 3);
        assert!(status.configured);
        let replaced = replace_section.snapshot();
        assert_eq!(replaced.status, SectionStatus::Healthy);
        assert_eq!(replaced.revision, 3);
        assert_eq!(replaced.data.len(), 2);
        let replaced_document: Value =
            serde_json::from_slice(&std::fs::read(replace_section.store.path()).unwrap()).unwrap();
        assert_eq!(replaced_document["revision"], 3);
        assert_eq!(
            replace_section
                .store
                .resolve(&first)
                .unwrap()
                .unwrap()
                .expose(),
            "repaired-secret"
        );
        assert_eq!(
            replace_section
                .store
                .resolve(&second)
                .unwrap()
                .unwrap()
                .expose(),
            "second-secret"
        );

        let clear_dir = TempDir::new().unwrap();
        let clear_section = CredentialSection::open(clear_dir.path(), false);
        clear_section
            .replace(first.clone(), "first-secret", CredentialSource::User, 0)
            .unwrap();
        clear_section
            .replace(second.clone(), "second-secret", CredentialSource::User, 1)
            .unwrap();
        std::fs::write(clear_section.store.path(), b"{ corrupt primary").unwrap();
        let degraded = clear_section.reload().unwrap();
        assert_eq!(degraded.status, SectionStatus::Degraded);
        assert_eq!(degraded.revision, 2);
        let (revision, status) = clear_section.clear(&first, 2).unwrap();
        assert_eq!(revision, 3);
        assert!(!status.configured);
        let cleared = clear_section.snapshot();
        assert_eq!(cleared.status, SectionStatus::Healthy);
        assert_eq!(cleared.revision, 3);
        assert_eq!(cleared.data.len(), 1);
        assert_eq!(cleared.data[0].credential_ref, second);
        let cleared_document: Value =
            serde_json::from_slice(&std::fs::read(clear_section.store.path()).unwrap()).unwrap();
        assert_eq!(cleared_document["revision"], 3);
        assert!(clear_section.store.resolve(&first).unwrap().is_none());
        assert_eq!(
            clear_section
                .store
                .resolve(&second)
                .unwrap()
                .unwrap()
                .expose(),
            "second-secret"
        );
    }

    #[test]
    fn credential_missing_and_invalid_lkg_repairs_preserve_unrelated_entries() {
        let _key = crate::encryption::set_test_encryption_key([93; 32]);
        let first = CredentialRef::parse("provider.openai.api_key").unwrap();
        let second = CredentialRef::parse("provider.anthropic.api_key").unwrap();

        let missing_dir = TempDir::new().unwrap();
        let missing = CredentialSection::open(missing_dir.path(), false);
        missing
            .replace(first.clone(), "first-secret", CredentialSource::User, 0)
            .unwrap();
        std::fs::remove_file(missing.store.path()).unwrap();
        assert_eq!(missing.reload().unwrap().status, SectionStatus::Missing);
        let (revision, _) = missing
            .replace(second.clone(), "second-secret", CredentialSource::User, 1)
            .unwrap();
        assert_eq!(revision, 2);
        assert_eq!(missing.snapshot().data.len(), 2);
        assert_eq!(
            missing.store.resolve(&first).unwrap().unwrap().expose(),
            "first-secret"
        );
        assert_eq!(
            missing.store.resolve(&second).unwrap().unwrap().expose(),
            "second-secret"
        );

        let invalid_dir = TempDir::new().unwrap();
        let invalid = CredentialSection::open(invalid_dir.path(), false);
        invalid
            .replace(first.clone(), "first-secret", CredentialSource::User, 0)
            .unwrap();
        write_json(
            invalid.store.path(),
            &json!({
                "schema_version": 1,
                "revision": 100,
                "data": {"entries": "invalid"}
            }),
        );
        assert!(invalid.reload().is_err());
        assert_eq!(invalid.snapshot().status, SectionStatus::Invalid);
        let (revision, _) = invalid
            .replace(second.clone(), "second-secret", CredentialSource::User, 1)
            .unwrap();
        assert_eq!(revision, 101);
        let published = invalid.snapshot();
        let (durable_revision, durable_statuses) = invalid.store.statuses_with_revision().unwrap();
        assert_eq!(published.revision, durable_revision);
        assert_eq!(published.data.as_ref(), &durable_statuses);
        assert_eq!(published.data.len(), 2);
        assert_eq!(
            invalid.store.resolve(&first).unwrap().unwrap().expose(),
            "first-secret"
        );
    }

    #[test]
    fn credential_readiness_failure_preserves_snapshot_and_lkg_authority() {
        let _key = crate::encryption::set_test_encryption_key([106; 32]);
        let dir = TempDir::new().unwrap();
        let section = CredentialSection::open(dir.path(), false);
        let reference = CredentialRef::parse("provider.openai.api_key").unwrap();
        section
            .replace(reference, "first-secret", CredentialSource::User, 0)
            .unwrap();
        let healthy = section.snapshot();
        assert_eq!(
            section.credential_repair_authority(),
            CredentialRepairAuthority::None
        );

        let manifest = dir.path().join("config-credential-migration.json");
        std::fs::write(&manifest, b"{ malformed readiness manifest").unwrap();
        assert!(section.reload().is_err());
        let retained = section.snapshot();
        assert_eq!(retained.status, healthy.status);
        assert_eq!(retained.revision, healthy.revision);
        assert_eq!(retained.data.as_ref(), healthy.data.as_ref());
        assert_eq!(
            section.credential_repair_authority(),
            CredentialRepairAuthority::None
        );

        std::fs::remove_file(manifest).unwrap();
        std::fs::remove_file(section.store.path()).unwrap();
        let missing = section.reload().unwrap();
        assert_eq!(missing.status, SectionStatus::Missing);
        assert_eq!(missing.revision, healthy.revision);
        assert_eq!(missing.data.as_ref(), healthy.data.as_ref());
        assert_eq!(
            section.credential_repair_authority(),
            CredentialRepairAuthority::MissingAfterGood
        );
    }

    #[test]
    fn credential_nonrepairable_error_then_missing_retains_trusted_lkg() {
        let _key = crate::encryption::set_test_encryption_key([107; 32]);
        let dir = TempDir::new().unwrap();
        let section = CredentialSection::open(dir.path(), false);
        let reference = CredentialRef::parse("provider.openai.api_key").unwrap();
        section
            .replace(reference.clone(), "first-secret", CredentialSource::User, 0)
            .unwrap();
        let healthy = section.snapshot();
        let path = section.store.path().to_path_buf();

        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        assert!(section.reload().is_err());
        assert_eq!(section.snapshot().status, SectionStatus::Invalid);
        assert_eq!(
            section.credential_repair_authority(),
            CredentialRepairAuthority::None
        );

        std::fs::remove_dir(&path).unwrap();
        let missing = section.reload().unwrap();
        assert_eq!(missing.status, SectionStatus::Missing);
        assert_eq!(missing.revision, healthy.revision);
        assert_eq!(missing.data.as_ref(), healthy.data.as_ref());
        assert_eq!(
            section.credential_repair_authority(),
            CredentialRepairAuthority::MissingAfterGood
        );

        let second = CredentialRef::parse("provider.anthropic.api_key").unwrap();
        section
            .replace(
                second,
                "second-secret",
                CredentialSource::User,
                healthy.revision,
            )
            .unwrap();
        assert_eq!(section.snapshot().data.len(), 2);
        assert_eq!(
            section.store.resolve(&reference).unwrap().unwrap().expose(),
            "first-secret"
        );
    }

    #[test]
    fn credential_same_revision_external_edit_conflicts_without_publication() {
        let _key = crate::encryption::set_test_encryption_key([94; 32]);
        let dir = TempDir::new().unwrap();
        let section = CredentialSection::open(dir.path(), false);
        let first = CredentialRef::parse("provider.openai.api_key").unwrap();
        let second = CredentialRef::parse("provider.anthropic.api_key").unwrap();
        let third = CredentialRef::parse("provider.gemini.api_key").unwrap();
        section
            .replace(first.clone(), "first-secret", CredentialSource::User, 0)
            .unwrap();
        section
            .store
            .replace(second, "external-secret", CredentialSource::User, 1)
            .unwrap();
        let path = section.store.path();
        let mut external: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        external["revision"] = json!(1);
        let external = serde_json::to_vec_pretty(&external).unwrap();
        std::fs::write(path, &external).unwrap();

        let retained = section.reload().unwrap();
        assert_eq!(retained.status, SectionStatus::Invalid);
        assert_eq!(retained.revision, 1);
        assert_eq!(retained.data.len(), 1);
        assert!(matches!(
            section.replace(third, "must-not-persist", CredentialSource::User, 1),
            Err(crate::ConfigStoreError::Conflict {
                expected: 1,
                actual: 1
            })
        ));
        assert_eq!(std::fs::read(path).unwrap(), external);
        let after = section.snapshot();
        assert_eq!(after.status, SectionStatus::Invalid);
        assert_eq!(after.revision, 1);
        assert_eq!(after.data.as_ref(), retained.data.as_ref());
    }

    #[test]
    fn credential_durable_success_does_not_reload_before_publication() {
        let _key = crate::encryption::set_test_encryption_key([91; 32]);
        let dir = TempDir::new().unwrap();
        let section = CredentialSection::open(dir.path(), false);
        let reference = CredentialRef::parse("provider.openai.api_key").unwrap();
        let credential_path = section.store.path().to_path_buf();

        let result = section.replace_inner(reference, "secret", CredentialSource::User, 0, || {
            std::fs::write(&credential_path, b"{ fault after durable commit").unwrap()
        });

        let (revision, status) = result.expect("durable success must remain an API success");
        assert_eq!(revision, 1);
        assert!(status.configured);
        let published = section.snapshot();
        assert_eq!(published.status, SectionStatus::Healthy);
        assert_eq!(published.revision, 1);
        assert_eq!(published.data.as_slice(), &[status]);
    }

    #[test]
    fn concurrent_credential_mutations_have_one_winner_and_one_conflict() {
        use std::sync::Barrier;

        let _key = crate::encryption::set_test_encryption_key([92; 32]);
        let dir = TempDir::new().unwrap();
        let section = Arc::new(CredentialSection::open(dir.path(), false));
        let reference = CredentialRef::parse("provider.openai.api_key").unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let mut threads = Vec::new();
        for secret in ["first", "second"] {
            let section = section.clone();
            let reference = reference.clone();
            let barrier = barrier.clone();
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                section.replace(reference, secret, CredentialSource::User, 0)
            }));
        }
        barrier.wait();
        let results = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| {
                    matches!(
                        result,
                        Err(crate::ConfigStoreError::Conflict {
                            expected: 0,
                            actual: 1
                        })
                    )
                })
                .count(),
            1
        );
        let published = section.snapshot();
        assert_eq!(published.revision, 1);
        assert_eq!(published.data.len(), 1);
        let (durable_revision, durable_statuses) = section.store.statuses_with_revision().unwrap();
        assert_eq!(durable_revision, 1);
        assert_eq!(durable_statuses, published.data.as_ref().clone());
    }

    #[test]
    fn credential_mutation_and_publication_are_serialized_with_reload_and_clear() {
        use std::sync::mpsc;
        use std::time::Duration;

        let _key = crate::encryption::set_test_encryption_key([89; 32]);
        let dir = TempDir::new().unwrap();
        let section = Arc::new(CredentialSection::open(dir.path(), false));
        let reference = CredentialRef::parse("provider.openai.api_key").unwrap();
        let (durable_tx, durable_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::sync_channel(0);

        let replacing = {
            let section = section.clone();
            let reference = reference.clone();
            std::thread::spawn(move || {
                section.replace_inner(reference, "secret", CredentialSource::User, 0, || {
                    durable_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                })
            })
        };
        durable_rx.recv().unwrap();
        assert_eq!(
            section.snapshot().revision,
            0,
            "durable mutation must not publish before its publication point"
        );

        let (reload_done_tx, reload_done_rx) = mpsc::channel();
        let reloading = {
            let section = section.clone();
            std::thread::spawn(move || {
                let result = section.reload();
                reload_done_tx.send(()).unwrap();
                result
            })
        };
        let (clear_done_tx, clear_done_rx) = mpsc::channel();
        let clearing = {
            let section = section.clone();
            let reference = reference.clone();
            std::thread::spawn(move || {
                let result = section.clear(&reference, 1);
                clear_done_tx.send(()).unwrap();
                result
            })
        };
        assert!(matches!(
            reload_done_rx.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        assert!(matches!(
            clear_done_rx.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));

        release_tx.send(()).unwrap();
        assert_eq!(replacing.join().unwrap().unwrap().0, 1);
        let reloaded = reloading.join().unwrap().unwrap();
        assert!(reloaded.revision >= 1);
        assert_eq!(clearing.join().unwrap().unwrap().0, 2);
        let final_snapshot = section.snapshot();
        assert_eq!(final_snapshot.revision, 2);
        assert!(final_snapshot.data.is_empty());
        assert_eq!(final_snapshot.status, SectionStatus::Healthy);
    }

    #[test]
    fn credential_publication_holds_transaction_lock_against_independent_writers() {
        use std::sync::mpsc;
        use std::time::Duration;

        let _key = crate::encryption::set_test_encryption_key([88; 32]);
        let dir = TempDir::new().unwrap();
        let data_dir = dir.path().to_path_buf();
        let section = Arc::new(CredentialSection::open(&data_dir, false));
        let first = CredentialRef::parse("provider.openai.api_key").unwrap();
        let second = CredentialRef::parse("provider.anthropic.api_key").unwrap();
        let (durable_tx, durable_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::sync_channel(0);

        let replacing = {
            let section = section.clone();
            std::thread::spawn(move || {
                section.replace_inner(first, "first-secret", CredentialSource::User, 0, || {
                    durable_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                })
            })
        };
        durable_rx.recv().unwrap();

        let (writer_started_tx, writer_started_rx) = mpsc::sync_channel(0);
        let (writer_done_tx, writer_done_rx) = mpsc::channel();
        let independent_writer = std::thread::spawn(move || {
            writer_started_tx.send(()).unwrap();
            let result = CredentialStore::open(data_dir).replace(
                second,
                "second-secret",
                CredentialSource::User,
                1,
            );
            writer_done_tx.send(()).unwrap();
            result
        });
        writer_started_rx.recv().unwrap();
        assert!(matches!(
            writer_done_rx.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));

        release_tx.send(()).unwrap();
        assert_eq!(replacing.join().unwrap().unwrap().0, 1);
        assert_eq!(independent_writer.join().unwrap().unwrap().0, 2);
        let reloaded = section.reload().unwrap();
        assert_eq!(reloaded.revision, 2);
        assert_eq!(reloaded.status, SectionStatus::Healthy);
        assert_eq!(reloaded.data.len(), 2);
    }

    #[test]
    fn compatibility_write_rejects_multiple_sections_before_any_commit() {
        let _key = crate::encryption::set_test_encryption_key([89; 32]);
        let dir = TempDir::new().unwrap();
        let facade = ConfigFacade::open_or_migrate(dir.path()).unwrap();
        let core_path = dir.path().join("core.json");
        let tools_path = dir.path().join("tools-skills.json");
        let core_before = std::fs::read(&core_path).unwrap();
        let tools_before = std::fs::read(&tools_path).unwrap();
        let core_revision = facade.registry().core.snapshot().revision;
        let tools_revision = facade.registry().tools_skills.snapshot().revision;

        let mut candidate = facade.effective_config();
        candidate.http_proxy = "http://proxy.invalid:8080".to_string();
        candidate.tools.disabled.push("bash".to_string());

        let error = persist_facade_effective_config(dir.path(), &candidate).unwrap_err();
        assert!(error.to_string().contains("changed multiple sections"));
        assert_eq!(std::fs::read(core_path).unwrap(), core_before);
        assert_eq!(std::fs::read(tools_path).unwrap(), tools_before);

        let reopened = ConfigFacade::open(dir.path()).unwrap();
        assert_eq!(reopened.registry().core.snapshot().revision, core_revision);
        assert_eq!(
            reopened.registry().tools_skills.snapshot().revision,
            tools_revision
        );
    }

    #[test]
    fn stale_compatibility_config_never_overwrites_newer_exact_cluster_authority() {
        let _key = crate::encryption::set_test_encryption_key([90; 32]);
        let dir = TempDir::new().unwrap();
        let initial_facade = ConfigFacade::open_or_migrate(dir.path()).unwrap();
        let mut initial = initial_facade.effective_config();
        initial.cluster_fabric.nodes.push(crate::Node {
            id: "owned-node".to_string(),
            label: "generation-one".to_string(),
            placement: crate::NodePlacement::Local,
            trust_level: crate::TrustLevel::Trusted,
            deploy: crate::DeployProfile::default(),
            state: None,
            enabled: true,
        });
        let intents = std::collections::BTreeMap::from([(
            "owned-node".to_string(),
            crate::ClusterNodeCredentialIntents::clear_all(),
        )]);
        assert_eq!(
            crate::persist_cluster_fabric_credential_transaction_at_revision(
                dir.path(),
                &mut initial,
                &intents,
                0,
            )
            .unwrap(),
            1
        );

        let stale_process = ConfigFacade::open(dir.path()).unwrap();
        let stale_caller = stale_process.effective_config();
        let mut external = stale_caller.clone();
        external
            .cluster_fabric
            .node_mut("owned-node")
            .unwrap()
            .label = "external-generation-two".to_string();
        assert_eq!(
            crate::persist_cluster_fabric_credential_transaction_at_revision(
                dir.path(),
                &mut external,
                &std::collections::BTreeMap::new(),
                1,
            )
            .unwrap(),
            2
        );
        let cluster_path = dir.path().join("cluster-fabric.json");
        let cluster_r2 = std::fs::read(&cluster_path).unwrap();

        assert!(
            persist_facade_effective_config(dir.path(), &stale_caller)
                .unwrap()
                .is_empty(),
            "a stale no-op must not manufacture a cluster revision"
        );
        assert_eq!(std::fs::read(&cluster_path).unwrap(), cluster_r2);

        let mut unrelated = stale_caller;
        unrelated.server.port = 22_222;
        let commit =
            persist_facade_effective_config_with_adoption(dir.path(), &unrelated, &stale_process)
                .unwrap();
        assert!(matches!(
            commit.section_adoption.unwrap().unwrap(),
            ConfigSectionEvent::Changed { section, .. } if section == "core"
        ));
        assert_eq!(std::fs::read(&cluster_path).unwrap(), cluster_r2);

        let reopened = ConfigFacade::open(dir.path()).unwrap();
        assert_eq!(reopened.registry().cluster_fabric.snapshot().revision, 2);
        assert_eq!(
            reopened
                .effective_config()
                .cluster_fabric
                .node("owned-node")
                .unwrap()
                .label,
            "external-generation-two"
        );
        assert_eq!(reopened.effective_config().server.port, 22_222);
    }

    #[test]
    fn compatibility_core_write_conflicts_with_newer_exact_proxy_generation() {
        let _key = crate::encryption::set_test_encryption_key([0x5c; 32]);
        let dir = TempDir::new().unwrap();
        let stale_process = ConfigFacade::open_or_migrate(dir.path()).unwrap();
        let mut stale_candidate = stale_process.effective_config();
        stale_candidate.server.port = 22_224;

        let external_facade = ConfigFacade::open(dir.path()).unwrap();
        let mut external_core = external_facade
            .registry()
            .envelope_value(SectionId::Core)
            .unwrap()
            .data;
        external_core["http_proxy"] = json!("http://external-proxy.test:8080");
        external_facade
            .registry()
            .commit_value(SectionId::Core, 0, external_core)
            .unwrap();
        let mut external = ConfigFacade::open(dir.path()).unwrap().effective_config();
        external.proxy_auth = Some(crate::ProxyAuth {
            username: "external-user".to_string(),
            password: "external-password".to_string(),
        });
        assert_eq!(
            crate::persist_proxy_auth_credential_transaction_at_revision(
                dir.path(),
                &mut external,
                1,
            )
            .unwrap(),
            2
        );
        let core_before = std::fs::read(dir.path().join("core.json")).unwrap();
        let credentials_before = std::fs::read(dir.path().join("credentials.json")).unwrap();

        let error = match persist_facade_effective_config_with_adoption(
            dir.path(),
            &stale_candidate,
            &stale_process,
        ) {
            Ok(_) => panic!("stale Core compatibility writer must conflict"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            crate::ConfigStoreError::Conflict {
                expected: 0,
                actual: 2
            }
        ));
        assert_eq!(
            std::fs::read(dir.path().join("core.json")).unwrap(),
            core_before
        );
        assert_eq!(
            std::fs::read(dir.path().join("credentials.json")).unwrap(),
            credentials_before
        );
        let exact =
            crate::read_exact_credential_section_snapshot(dir.path(), SectionId::Core, Some(2))
                .unwrap();
        let mut committed = Config::default();
        exact.install_into(&mut committed);
        assert_eq!(committed.http_proxy, "http://external-proxy.test:8080");
        assert_eq!(
            committed.proxy_auth.as_ref().unwrap().username,
            "external-user"
        );
        assert_ne!(committed.server.port, 22_224);
    }

    #[test]
    fn processless_compatibility_write_rejects_exact_credential_sections() {
        let dir = TempDir::new().unwrap();
        let facade = ConfigFacade::open_or_migrate(dir.path()).unwrap();
        let mut candidate = facade.effective_config();
        candidate.server.port = 22_225;

        let error = persist_facade_effective_config(dir.path(), &candidate).unwrap_err();
        assert!(error
            .to_string()
            .contains("core must be changed through its dedicated revisioned API"));
        assert_ne!(
            ConfigFacade::open(dir.path())
                .unwrap()
                .effective_config()
                .server
                .port,
            22_225
        );
    }

    #[test]
    fn invalid_compatibility_credential_base_is_zero_write_and_process_facade_stays_reloadable() {
        let _key = crate::encryption::set_test_encryption_key([91; 32]);
        let dir = TempDir::new().unwrap();
        let process_facade = ConfigFacade::open_or_migrate(dir.path()).unwrap();
        let reference = CredentialRef::parse("proxy.default.auth").unwrap();
        let mut proxy = process_facade.effective_config();
        proxy.proxy_auth = Some(crate::ProxyAuth {
            username: "initial-user".to_string(),
            password: "initial-secret".to_string(),
        });
        crate::persist_proxy_auth_credential_transaction_at_revision_with_adoption(
            dir.path(),
            &mut proxy,
            0,
            &process_facade,
        )
        .unwrap();
        let store = CredentialStore::open(dir.path());
        store
            .replace(
                reference,
                "not-json",
                CredentialSource::User,
                store.revision().unwrap(),
            )
            .unwrap();

        let mut candidate = process_facade.effective_config();
        candidate.server.port = 22_223;
        let core_before = process_facade.registry().core.snapshot().revision;
        let core_bytes_before = std::fs::read(dir.path().join("core.json")).unwrap();

        let error =
            persist_facade_effective_config_with_adoption(dir.path(), &candidate, &process_facade)
                .err()
                .expect("invalid active credential must fail before the Core commit");
        assert!(error.to_string().contains("explicitly replace or clear"));
        assert_eq!(
            process_facade.registry().core.snapshot().revision,
            core_before
        );
        assert_eq!(
            std::fs::read(dir.path().join("core.json")).unwrap(),
            core_bytes_before
        );

        let durable = ConfigFacade::open(dir.path()).unwrap();
        assert_eq!(durable.registry().core.snapshot().revision, core_before);
        assert_ne!(durable.effective_config().server.port, 22_223);
    }
}
