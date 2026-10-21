//! Portable, explicitly selected configuration data. This module performs no
//! filesystem/environment access, provider discovery, network I/O or execution.
//! Decoding and conflict planning are pure: callers must separately authorize
//! and transactionally apply a plan. Omitted values never express deletion.

mod codec;
mod planning;
mod selection;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use bamboo_domain::ReasoningEffort;
use serde::{Deserialize, Serialize};

pub use codec::{decode_bundle, encode_bundle, MAX_SHARE_STRING_BYTES, SHARE_PREFIX};
pub use planning::{plan_import, preview_bundle};
pub use selection::select_bundle;

pub const MAX_SHARE_BUNDLE_BYTES: usize = 512 * 1024;
const MAX_PROVIDERS: usize = 64;
const MAX_ENV_VARS: usize = 256;
const MAX_SUBAGENT_MODELS: usize = 64;
const MAX_MODELS: usize = 256;
const MAX_VALUE_BYTES: usize = 32 * 1024;

/// Diagnostics deliberately carry neither user input nor serde/crypto errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ShareError {
    #[error("Configuration share exceeds a supported size or depth limit")]
    Limit,
    #[error("Unsupported configuration share version or encryption format")]
    Unsupported,
    #[error("Invalid configuration share data")]
    Invalid,
    #[error("This configuration share requires a passphrase of 12 to 1024 bytes")]
    PassphraseRequired,
    #[error("Unable to authenticate configuration share; check its passphrase and integrity")]
    Authentication,
    #[error("Configuration share selection includes an unavailable item")]
    Selection,
    #[error("Configuration share contains an unsafe environment variable or URL")]
    Unsafe,
    #[error("Configuration share contains an unresolved provider reference")]
    Reference,
    #[error("Configuration share conflicts require an explicit valid decision")]
    Conflict,
}

pub type ShareResult<T> = Result<T, ShareError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShareScenario {
    Chat,
    Fast,
    TaskSummary,
    Vision,
    MemoryBackground,
    Planning,
    Search,
    CodeReview,
    SubAgent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ShareProviderType {
    Openai,
    Anthropic,
    Gemini,
    Copilot,
    Bodhi,
}

impl ShareProviderType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Openai => "openai",
            Self::Anthropic => "anthropic",
            Self::Gemini => "gemini",
            Self::Copilot => "copilot",
            Self::Bodhi => "bodhi",
        }
    }
}

/// Deliberately independent of ProviderInstanceConfig: its flattened `extra`,
/// request overrides, host credential references and ciphertext are excluded.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShareProvider {
    pub id: String,
    pub provider_type: ShareProviderType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fast_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vision_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub responses_only_models: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_models: Option<Vec<String>>,
}

impl fmt::Debug for ShareProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShareProvider").finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShareModelRef {
    pub provider: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
}

/// `None` means name/metadata only, never Clear. Every included value (even a
/// non-secret entry or empty string) requires an encrypted share envelope.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShareEnvVar {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(default)]
    pub secret: bool,
}

impl fmt::Debug for ShareEnvVar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShareEnvVar").finish_non_exhaustive()
    }
}

#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShareBundle {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub providers: Vec<ShareProvider>,
    #[serde(
        default,
        deserialize_with = "unique_map",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub scenarios: BTreeMap<ShareScenario, ShareModelRef>,
    #[serde(
        default,
        deserialize_with = "unique_map",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub subagent_models: BTreeMap<String, ShareModelRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<ShareEnvVar>,
}

impl fmt::Debug for ShareBundle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShareBundle")
            .field("providers", &self.providers.len())
            .field("scenarios", &self.scenarios.len())
            .field("subagent_models", &self.subagent_models.len())
            .field("env", &self.env.len())
            .finish()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShareSelection {
    #[serde(default)]
    pub providers: BTreeSet<String>,
    #[serde(default)]
    pub provider_api_keys: BTreeSet<String>,
    #[serde(default)]
    pub provider_base_urls: BTreeSet<String>,
    #[serde(default)]
    pub scenarios: BTreeSet<ShareScenario>,
    #[serde(default)]
    pub subagent_models: BTreeSet<String>,
    /// Key = selected variable name. Value = include its value explicitly.
    #[serde(default, deserialize_with = "unique_map")]
    pub env: BTreeMap<String, bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ShareConflictChoice {
    Skip,
    Replace,
    Rename { target: String },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShareDecisions {
    #[serde(default, deserialize_with = "unique_map")]
    pub providers: BTreeMap<String, ShareConflictChoice>,
    #[serde(default, deserialize_with = "unique_map")]
    pub scenarios: BTreeMap<ShareScenario, ShareConflictChoice>,
    #[serde(default, deserialize_with = "unique_map")]
    pub subagent_models: BTreeMap<String, ShareConflictChoice>,
    #[serde(default, deserialize_with = "unique_map")]
    pub env: BTreeMap<String, ShareConflictChoice>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShareProviderPreview {
    pub id: String,
    pub provider_type: ShareProviderType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub has_api_key: bool,
    pub has_base_url: bool,
    pub conflict: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShareScenarioPreview {
    pub scenario: ShareScenario,
    pub provider: String,
    pub model: String,
    pub conflict: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShareSubagentPreview {
    pub name: String,
    pub provider: String,
    pub model: String,
    pub conflict: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShareEnvPreview {
    pub name: String,
    pub has_value: bool,
    pub secret: bool,
    pub conflict: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SharePreview {
    pub providers: Vec<ShareProviderPreview>,
    pub scenarios: Vec<ShareScenarioPreview>,
    pub subagent_models: Vec<ShareSubagentPreview>,
    pub env: Vec<ShareEnvPreview>,
    pub contains_secrets: bool,
}

impl ShareBundle {
    pub fn contains_secrets(&self) -> bool {
        self.providers.iter().any(|p| p.api_key.is_some())
            || self.env.iter().any(|entry| entry.value.is_some())
    }

    pub fn validate(&self) -> ShareResult<()> {
        if self.providers.len() > MAX_PROVIDERS
            || self.env.len() > MAX_ENV_VARS
            || self.subagent_models.len() > MAX_SUBAGENT_MODELS
        {
            return Err(ShareError::Limit);
        }
        let mut ids = BTreeSet::new();
        for provider in &self.providers {
            validate_id(&provider.id)?;
            if !ids.insert(&provider.id) {
                return Err(ShareError::Invalid);
            }
            if let Some(label) = &provider.label {
                validate_text(label, 256)?;
            }
            if let Some(url) = &provider.base_url {
                validate_base_url(url)?;
            }
            if let Some(key) = &provider.api_key {
                validate_text(key, MAX_VALUE_BYTES)?;
                if crate::patch::is_masked_api_key(key) {
                    return Err(ShareError::Invalid);
                }
            }
            for model in [
                &provider.model,
                &provider.fast_model,
                &provider.vision_model,
            ]
            .into_iter()
            .flatten()
            {
                validate_text(model, 512)?;
            }
            validate_models(&provider.responses_only_models)?;
            if let Some(models) = &provider.runtime_models {
                validate_models(models)?;
            }
        }
        for (name, reference) in &self.subagent_models {
            validate_id(name)?;
            validate_reference(reference, &ids)?;
        }
        for reference in self.scenarios.values() {
            validate_reference(reference, &ids)?;
        }
        let mut names = BTreeSet::new();
        for entry in &self.env {
            validate_env_name(&entry.name)?;
            if !names.insert(canonical_env_name(&entry.name)) {
                return Err(ShareError::Invalid);
            }
            if entry
                .value
                .as_ref()
                .is_some_and(|v| v.len() > MAX_VALUE_BYTES || v.contains('\0'))
            {
                return Err(ShareError::Limit);
            }
        }
        if serde_json::to_vec(self)
            .map_err(|_| ShareError::Invalid)?
            .len()
            > MAX_SHARE_BUNDLE_BYTES
        {
            return Err(ShareError::Limit);
        }
        Ok(())
    }
}

fn validate_reference(reference: &ShareModelRef, ids: &BTreeSet<&String>) -> ShareResult<()> {
    validate_id(&reference.provider)?;
    validate_text(&reference.model, 512)?;
    if !ids.contains(&reference.provider) {
        return Err(ShareError::Reference);
    }
    Ok(())
}

fn validate_models(models: &[String]) -> ShareResult<()> {
    if models.len() > MAX_MODELS {
        return Err(ShareError::Limit);
    }
    let mut seen = BTreeSet::new();
    for model in models {
        validate_text(model, 512)?;
        if !seen.insert(model) {
            return Err(ShareError::Invalid);
        }
    }
    Ok(())
}

fn validate_text(value: &str, max: usize) -> ShareResult<()> {
    if value.len() > max {
        return Err(ShareError::Limit);
    }
    if value.trim().is_empty() || value.chars().any(char::is_control) {
        return Err(ShareError::Invalid);
    }
    Ok(())
}

fn validate_id(value: &str) -> ShareResult<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-.".contains(&b))
        || value == "."
        || value == ".."
    {
        return Err(ShareError::Invalid);
    }
    Ok(())
}

fn validate_base_url(value: &str) -> ShareResult<()> {
    validate_text(value, 4096)?;
    let url = url::Url::parse(value).map_err(|_| ShareError::Unsafe)?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ShareError::Unsafe);
    }
    Ok(())
}

/// Portable identity for an already validated ASCII environment name. Every
/// platform uses the same case-folding rule; keep the original name for display
/// and use the target's existing spelling when replacing a matching entry.
pub fn canonical_env_name(name: &str) -> String {
    name.to_ascii_uppercase()
}

/// Import changes only app-managed env entries. Reject runtime/loader/config
/// selectors rather than letting a portable bundle change execution authority.
pub fn validate_env_name(name: &str) -> ShareResult<()> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .enumerate()
            .all(|(i, b)| b.is_ascii_alphabetic() || b == b'_' || (i > 0 && b.is_ascii_digit()))
    {
        return Err(ShareError::Invalid);
    }
    let upper = canonical_env_name(name);
    let forbidden = [
        "PATH",
        "HOME",
        "USER",
        "USERNAME",
        "SHELL",
        "COMSPEC",
        "PATHEXT",
        "SYSTEMROOT",
        "WINDIR",
        "TMP",
        "TEMP",
        "TMPDIR",
        "IFS",
        "ENV",
        "BASH_ENV",
        "ZDOTDIR",
        "RUSTC_WRAPPER",
        "RUSTC_WORKSPACE_WRAPPER",
        "RUSTFLAGS",
        "GIT_CONFIG",
        "GIT_CONFIG_COUNT",
        "GIT_SSH",
        "GIT_SSH_COMMAND",
        "SSH_ASKPASS",
        "SUDO_ASKPASS",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
        "REQUESTS_CA_BUNDLE",
        "CURL_CA_BUNDLE",
        "JAVA_TOOL_OPTIONS",
        "_JAVA_OPTIONS",
        "JDK_JAVA_OPTIONS",
        "GODEBUG",
        "GOFLAGS",
        "GOTOOLCHAIN",
        "R_PROFILE",
        "R_ENVIRON",
        "PSMODULEPATH",
        "PROMPT_COMMAND",
        "BASHOPTS",
        "SHELLOPTS",
        "PAGER",
        "EDITOR",
        "VISUAL",
        "GIT_EDITOR",
        "GIT_PAGER",
        "LESSOPEN",
        "CLASSPATH",
        "PHPRC",
        "GIT_EXEC_PATH",
        "GIT_TEMPLATE_DIR",
        "BROWSER",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
    ];
    let prefixes = [
        "LD_",
        "DYLD_",
        "NODE_",
        "NPM_",
        "PYTHON",
        "PERL",
        "RUBY",
        "BAMBOO_",
        "BODHI_",
        "COPILOT_",
        "OPENAI_",
        "ANTHROPIC_",
        "GEMINI_",
        "GOOGLE_",
        "GIT_",
        "CARGO_",
        "RUSTUP_",
        "DOTNET_",
        "BASH_FUNC_",
        "JAVA_",
        "JDK_",
        "PHP_",
        "ELECTRON_",
    ];
    if forbidden.contains(&upper.as_str()) || prefixes.iter().any(|p| upper.starts_with(p)) {
        return Err(ShareError::Unsafe);
    }
    Ok(())
}

fn unique_map<'de, D, K, V>(deserializer: D) -> Result<BTreeMap<K, V>, D::Error>
where
    D: serde::Deserializer<'de>,
    K: Deserialize<'de> + Ord,
    V: Deserialize<'de>,
{
    struct MapVisitor<K, V>(std::marker::PhantomData<(K, V)>);
    impl<'de, K, V> serde::de::Visitor<'de> for MapVisitor<K, V>
    where
        K: Deserialize<'de> + Ord,
        V: Deserialize<'de>,
    {
        type Value = BTreeMap<K, V>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a map with unique keys")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut map: A,
        ) -> Result<Self::Value, A::Error> {
            let mut result = BTreeMap::new();
            while let Some((key, value)) = map.next_entry()? {
                if result.insert(key, value).is_some() {
                    return Err(serde::de::Error::custom("duplicate map key"));
                }
            }
            Ok(result)
        }
    }
    deserializer.deserialize_map(MapVisitor(std::marker::PhantomData))
}
