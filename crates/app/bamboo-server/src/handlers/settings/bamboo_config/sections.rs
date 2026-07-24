use actix_web::{web, HttpResponse};
use bamboo_config::{
    patch::is_masked_api_key, ConfigStoreError, DefaultsConfig, FeatureFlags, HooksSection,
    MemorySection, ModelLimitsSection, ModelPolicySection, ProviderConfigs, ProviderInstanceConfig,
    RequestOverridesConfig, SectionEnvelope, SectionId, SubagentsSection, ToolsSkillsSection,
};
use bamboo_mcp::McpConfig;
use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::{
    app_state::{AppState, ConfigSectionMutationError},
    error::AppError,
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PutProviderSectionRequest {
    pub expected_revision: u64,
    #[serde(deserialize_with = "deserialize_provider_candidate")]
    pub data: ProviderConfigs,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PutMcpSectionRequest {
    pub expected_revision: u64,
    #[serde(deserialize_with = "deserialize_mcp_candidate")]
    pub data: McpConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PutTypedSectionRequest {
    pub expected_revision: u64,
    pub data: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResetTypedSectionRequest {
    pub expected_revision: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PutProviderSettingsRequest {
    pub expected_revision: u64,
    #[serde(deserialize_with = "deserialize_provider_settings_candidate")]
    pub data: ProviderSettingsData,
    #[serde(default)]
    pub credential_changes: ProviderCredentialChanges,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderSettingsData {
    pub provider: String,
    #[serde(default)]
    pub providers: ProviderConfigs,
    #[serde(default)]
    pub defaults: Option<DefaultsConfig>,
    #[serde(default)]
    pub features: FeatureFlags,
    #[serde(default)]
    pub provider_instances: HashMap<String, ProviderInstanceSettingsData>,
    #[serde(default, alias = "default_provider_instance")]
    pub default_provider_instance_id: Option<String>,
    // Read-only response fields are accepted and ignored so the canonical
    // response data can be round-tripped without client-side shape surgery.
    #[serde(default)]
    pub available_providers: Vec<String>,
    #[serde(default)]
    pub credential_status: Value,
}

/// Secret-free, editable provider-instance contract. Provider-specific fields
/// that the runtime historically stored in `ProviderInstanceConfig::extra`
/// are explicit here so clients can round-trip them without gaining access to
/// unclassified server-owned metadata.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderInstanceSettingsData {
    pub provider_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fast_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vision_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<bamboo_domain::ReasoningEffort>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub responses_only_models: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_overrides: Option<RequestOverridesConfig>,
    #[serde(default = "provider_instance_enabled_default")]
    pub enabled: bool,
    /// Bodhi-only upstream routing target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_provider: Option<String>,
    /// Anthropic-compatible upstream opt-in introduced by #520.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking_replay_always: Option<bool>,
}

fn provider_instance_enabled_default() -> bool {
    true
}

impl ProviderInstanceSettingsData {
    fn from_config(instance: &ProviderInstanceConfig) -> Self {
        Self {
            provider_type: instance.provider_type.clone(),
            label: instance.label.clone(),
            base_url: instance.base_url.clone(),
            model: instance.model.clone(),
            fast_model: instance.fast_model.clone(),
            vision_model: instance.vision_model.clone(),
            reasoning_effort: instance.reasoning_effort,
            responses_only_models: instance.responses_only_models.clone(),
            request_overrides: instance.request_overrides.clone(),
            enabled: instance.enabled,
            target_provider: instance
                .extra
                .get("target_provider")
                .and_then(Value::as_str)
                .map(str::to_string),
            thinking_replay_always: instance
                .extra
                .get("thinking_replay_always")
                .and_then(Value::as_bool),
        }
    }

    fn into_config(self) -> ProviderInstanceConfig {
        let mut extra = BTreeMap::new();
        if let Some(target_provider) = self.target_provider {
            extra.insert("target_provider".to_string(), json!(target_provider));
        }
        if let Some(thinking_replay_always) = self.thinking_replay_always {
            extra.insert(
                "thinking_replay_always".to_string(),
                json!(thinking_replay_always),
            );
        }
        ProviderInstanceConfig {
            provider_type: self.provider_type,
            label: self.label,
            api_key: String::new(),
            api_key_encrypted: None,
            credential_ref: None,
            base_url: self.base_url,
            model: self.model,
            fast_model: self.fast_model,
            vision_model: self.vision_model,
            reasoning_effort: self.reasoning_effort,
            responses_only_models: self.responses_only_models,
            request_overrides: self.request_overrides,
            enabled: self.enabled,
            extra,
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderCredentialChanges {
    #[serde(default)]
    pub providers: BTreeMap<String, ProviderCredentialChange>,
    #[serde(default)]
    pub provider_instances: BTreeMap<String, ProviderCredentialChange>,
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum ProviderCredentialChange {
    Replace { value: String },
    Clear,
}

#[derive(Serialize)]
struct ProviderCredentialStatusView {
    credential_ref: Option<String>,
    configured: bool,
    source: Option<String>,
    updated_at: Option<String>,
}

/// Read-only, secret-free provider section projection. Credential values,
/// ciphertext, UI masks, request override headers, and forward-compatible
/// unknown fields are intentionally excluded; callers use the credential
/// status API for configured-secret metadata.
pub async fn get_provider_section(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let _io = app_state.config_io_lock.lock().await;
    let config = app_state.config.read().await.clone();
    let data = json!({
        "active_provider": config.provider,
        "providers": provider_diagnostics(&config),
        "defaults": config.defaults,
        "features": config.features,
    });
    let health = app_state
        .config_live_health
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    Ok(HttpResponse::Ok().json(section_envelope(data, health)))
}

/// Canonical provider-settings projection for editable UI state. Unlike the
/// compact diagnostics endpoint above, this contains every non-secret field
/// owned by providers.json plus explicit credential status metadata.
pub async fn get_provider_settings_section(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let _io = app_state.config_io_lock.lock().await;
    let config = app_state.config.read().await.clone();
    let providers = sanitized_provider_metadata(config.providers())?;
    let provider_instances = sanitized_provider_instance_metadata(&config.provider_instances)?;
    let mut builtin_status = Map::new();

    macro_rules! builtin_status {
        ($name:literal, $field:ident, $from_env:expr) => {
            if let Some(provider) = config.providers().$field.as_ref() {
                builtin_status.insert(
                    $name.to_string(),
                    serde_json::to_value(provider_credential_status(
                        &app_state,
                        provider.credential_ref.as_ref(),
                        $from_env,
                        !provider.api_key.trim().is_empty() || provider.api_key_encrypted.is_some(),
                    )?)?,
                );
            }
        };
    }
    builtin_status!(
        "openai",
        openai,
        config
            .providers()
            .openai
            .as_ref()
            .is_some_and(|p| p.api_key_from_env)
    );
    builtin_status!(
        "anthropic",
        anthropic,
        config
            .providers()
            .anthropic
            .as_ref()
            .is_some_and(|p| p.api_key_from_env)
    );
    builtin_status!(
        "gemini",
        gemini,
        config
            .providers()
            .gemini
            .as_ref()
            .is_some_and(|p| p.api_key_from_env)
    );
    builtin_status!("bodhi", bodhi, false);

    let mut instance_status = Map::new();
    for (id, instance) in &config.provider_instances {
        instance_status.insert(
            id.clone(),
            serde_json::to_value(provider_credential_status(
                &app_state,
                instance.credential_ref.as_ref(),
                false,
                !instance.api_key.trim().is_empty() || instance.api_key_encrypted.is_some(),
            )?)?,
        );
    }

    let data = json!({
        "provider": config.provider,
        "providers": providers,
        "defaults": config.defaults,
        "features": config.features,
        "provider_instances": provider_instances,
        "default_provider_instance_id": config.default_provider_instance,
        "available_providers": bamboo_llm::AVAILABLE_PROVIDERS,
        "credential_status": {
            "providers": builtin_status,
            "provider_instances": instance_status,
        },
    });
    let health = app_state
        .config_live_health
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    Ok(HttpResponse::Ok().json(section_envelope(data, health)))
}

pub async fn put_provider_settings_section(
    app_state: web::Data<AppState>,
    payload: web::Json<PutProviderSettingsRequest>,
) -> Result<HttpResponse, AppError> {
    let payload = payload.into_inner();
    validate_provider_shape(&payload.data.providers).map_err(AppError::BadRequest)?;
    validate_provider_instance_shape(&payload.data.provider_instances)
        .map_err(AppError::BadRequest)?;
    let expected_revision = payload.expected_revision;
    let data = payload.data;
    let credential_changes = payload.credential_changes;
    app_state
        .put_provider_settings(expected_revision, move |current, candidate| {
            let mut provider_intents = credential_changes
                .providers
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>();
            let mut provider_instance_intents = credential_changes
                .provider_instances
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>();

            let ProviderSettingsData {
                provider,
                providers,
                defaults,
                features,
                provider_instances,
                default_provider_instance_id,
                available_providers: _,
                credential_status: _,
            } = data;

            for name in ["openai", "anthropic", "gemini", "bodhi"] {
                if provider_exists(current.providers(), name) && !provider_exists(&providers, name)
                {
                    provider_intents.insert(name.to_string());
                }
            }
            provider_instance_intents.extend(
                current
                    .provider_instances
                    .keys()
                    .filter(|id| !provider_instances.contains_key(*id))
                    .cloned(),
            );

            candidate.provider = provider;
            *candidate.providers_mut() = providers;
            candidate.defaults = defaults;
            candidate.features = features;
            candidate.provider_instances = provider_instances
                .into_iter()
                .map(|(id, instance)| (id, instance.into_config()))
                .collect();
            candidate.default_provider_instance = default_provider_instance_id;
            retain_provider_settings_server_owned_fields(current, candidate);
            apply_provider_credential_changes(candidate, &credential_changes.providers)?;
            apply_provider_instance_credential_changes(
                candidate,
                &credential_changes.provider_instances,
            )?;
            bamboo_llm::validate_provider_config(candidate).map_err(|error| {
                ConfigSectionMutationError::Invalid(format!("invalid provider settings: {error}"))
            })?;
            Ok((provider_intents, provider_instance_intents))
        })
        .await
        .map_err(map_mutation_error)?;
    get_provider_settings_section(app_state).await
}

/// Read-only MCP section projection. Transport diagnostics remain visible,
/// while environment/header values and legacy ciphertext never enter the DTO.
pub async fn get_mcp_section(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let _io = app_state.config_io_lock.lock().await;
    let config = app_state.config.read().await.clone();
    let servers = config
        .mcp
        .servers
        .iter()
        .map(mcp_server_diagnostics)
        .collect::<Vec<_>>();
    let data = json!({
        "version": config.mcp.version,
        "servers": servers,
    });
    let health = app_state
        .mcp_config_live_health
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    Ok(HttpResponse::Ok().json(section_envelope(data, health)))
}

pub async fn put_provider_section(
    app_state: web::Data<AppState>,
    payload: web::Json<PutProviderSectionRequest>,
) -> Result<HttpResponse, AppError> {
    let payload = payload.into_inner();
    app_state
        .put_provider_section(payload.expected_revision, payload.data)
        .await
        .map_err(map_mutation_error)?;
    get_provider_section(app_state).await
}

pub async fn put_mcp_section(
    app_state: web::Data<AppState>,
    payload: web::Json<PutMcpSectionRequest>,
) -> Result<HttpResponse, AppError> {
    let payload = payload.into_inner();
    app_state
        .put_mcp_section(payload.expected_revision, payload.data)
        .await
        .map_err(map_mutation_error)?;
    get_mcp_section(app_state).await
}

/// Read any non-secret typed section through the process-owned facade. The
/// provider/MCP names retain their stricter diagnostic projections above.
pub async fn get_typed_section(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let id = section_id(&path.into_inner())?;
    match id {
        bamboo_config::SectionId::Providers => get_provider_section(app_state).await,
        bamboo_config::SectionId::Mcp => get_mcp_section(app_state).await,
        _ => {
            let _io = app_state.config_io_lock.lock().await;
            let facade = app_state.config_facade.as_ref().ok_or_else(|| {
                AppError::BadRequest(
                    "typed sections require the modular configuration facade".to_string(),
                )
            })?;
            let envelope = facade
                .registry()
                .envelope_value(id)
                .map_err(|error| map_mutation_error(ConfigSectionMutationError::Store(error)))?;
            Ok(HttpResponse::Ok().json(envelope))
        }
    }
}

/// Replace one typed ordinary section with revision/CAS semantics. Provider,
/// MCP and credential mutations stay on their runtime-staged or secret-only
/// endpoints.
pub async fn put_typed_section(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<PutTypedSectionRequest>,
) -> Result<HttpResponse, AppError> {
    let id = section_id(&path.into_inner())?;
    if matches!(
        id,
        bamboo_config::SectionId::Providers
            | bamboo_config::SectionId::Mcp
            | bamboo_config::SectionId::Credentials
    ) {
        return Err(AppError::BadRequest(
            "this section requires its dedicated endpoint".to_string(),
        ));
    }
    let payload = payload.into_inner();
    app_state
        .put_ordinary_section(id, payload.expected_revision, payload.data)
        .await
        .map_err(map_mutation_error)?;

    let _io = app_state.config_io_lock.lock().await;
    let facade = app_state.config_facade.as_ref().ok_or_else(|| {
        AppError::BadRequest("typed sections require the modular configuration facade".to_string())
    })?;
    let envelope = facade
        .registry()
        .envelope_value(id)
        .map_err(|error| map_mutation_error(ConfigSectionMutationError::Store(error)))?;
    Ok(HttpResponse::Ok().json(envelope))
}

/// Reset exactly one section to its backend-owned default using the typed
/// section revision as the CAS authority. Credential-backed resets rebase on
/// unrelated credential changes inside their recoverable exact transaction.
pub async fn reset_typed_section(
    app_state: web::Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<ResetTypedSectionRequest>,
) -> Result<HttpResponse, AppError> {
    let name = path.into_inner();
    let id = section_id(&name)?;
    let expected_revision = payload.expected_revision;

    match id {
        SectionId::Core
        | SectionId::Notifications
        | SectionId::Connect
        | SectionId::Env
        | SectionId::ClusterFabric
        | SectionId::AccessControl => {
            app_state
                .reset_credential_backed_section(id, expected_revision)
                .await
                .map_err(map_mutation_error)?;
        }
        SectionId::Providers => {
            app_state
                .reset_provider_section(expected_revision)
                .await
                .map_err(map_mutation_error)?;
        }
        SectionId::Mcp => {
            app_state
                .reset_mcp_section(expected_revision)
                .await
                .map_err(map_mutation_error)?;
        }
        SectionId::Credentials => {
            return super::credentials::reset_credentials(
                app_state,
                web::Json(super::credentials::ResetCredentialsRequest { expected_revision }),
            )
            .await;
        }
        ordinary => {
            let candidate = default_section_value(ordinary)?;
            app_state
                .put_ordinary_section(ordinary, expected_revision, candidate)
                .await
                .map_err(map_mutation_error)?;
        }
    }

    get_typed_section(app_state, web::Path::from(name)).await
}

fn default_section_value(id: SectionId) -> Result<Value, AppError> {
    let value = match id {
        SectionId::ToolsSkills => serde_json::to_value(ToolsSkillsSection::default()),
        SectionId::Memory => serde_json::to_value(MemorySection::default()),
        SectionId::Subagents => serde_json::to_value(SubagentsSection::default()),
        SectionId::Hooks => serde_json::to_value(HooksSection::default()),
        SectionId::ModelPolicy => serde_json::to_value(ModelPolicySection::default()),
        SectionId::ModelLimits => serde_json::to_value(ModelLimitsSection::default()),
        _ => {
            return Err(AppError::BadRequest(
                "this section requires its dedicated reset path".to_string(),
            ))
        }
    };
    value.map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))
}

fn section_id(name: &str) -> Result<bamboo_config::SectionId, AppError> {
    bamboo_config::SectionId::from_name(name)
        .ok_or_else(|| AppError::BadRequest("unknown configuration section".to_string()))
}

fn map_mutation_error(error: ConfigSectionMutationError) -> AppError {
    match error {
        ConfigSectionMutationError::Store(ConfigStoreError::Conflict { expected, actual }) => {
            AppError::ConfigConflict { expected, actual }
        }
        ConfigSectionMutationError::Store(ConfigStoreError::Validation(message))
        | ConfigSectionMutationError::Invalid(message)
        | ConfigSectionMutationError::Runtime(message) => AppError::BadRequest(message),
        ConfigSectionMutationError::Store(ConfigStoreError::Io(error)) => {
            AppError::StorageError(error)
        }
        ConfigSectionMutationError::Store(ConfigStoreError::Json(_)) => {
            AppError::BadRequest("section document is invalid".to_string())
        }
        ConfigSectionMutationError::Store(ConfigStoreError::Watch(error)) => {
            AppError::InternalError(anyhow::anyhow!("section store watch failed: {error}"))
        }
    }
}

fn sanitized_provider_metadata(providers: &ProviderConfigs) -> Result<Value, AppError> {
    let mut providers = providers.clone();
    providers.extra.clear();
    macro_rules! sanitize {
        ($field:ident) => {
            if let Some(provider) = providers.$field.as_mut() {
                provider.api_key.clear();
                provider.api_key_encrypted = None;
                provider.credential_ref = None;
                provider.base_url = safe_url_diagnostic(provider.base_url.as_deref());
                provider.extra.clear();
            }
        };
    }
    sanitize!(openai);
    sanitize!(anthropic);
    sanitize!(gemini);
    if let Some(provider) = providers.copilot.as_mut() {
        provider.extra.clear();
    }
    if let Some(provider) = providers.bodhi.as_mut() {
        provider.api_key.clear();
        provider.api_key_encrypted = None;
        provider.credential_ref = None;
        provider.base_url = safe_url_diagnostic(provider.base_url.as_deref());
        provider.extra.clear();
    }
    let mut value = serde_json::to_value(providers)?;
    scrub_unsafe_request_override_literals(&mut value);
    Ok(value)
}

fn sanitized_provider_instance_metadata(
    instances: &HashMap<String, ProviderInstanceConfig>,
) -> Result<Value, AppError> {
    let mut data = HashMap::new();
    for (id, instance) in instances {
        let mut instance = ProviderInstanceSettingsData::from_config(instance);
        instance.base_url = safe_url_diagnostic(instance.base_url.as_deref());
        data.insert(id.clone(), instance);
    }
    let mut value = serde_json::to_value(data)?;
    scrub_unsafe_request_override_literals(&mut value);
    Ok(value)
}

fn scrub_unsafe_request_override_literals(value: &mut Value) {
    match value {
        Value::Object(object) => {
            if let Some(headers) = object.get_mut("headers").and_then(Value::as_object_mut) {
                headers.retain(|name, expression| {
                    !is_sensitive_header_name(name) || is_external_template_reference(expression)
                });
            }
            if let Some(patches) = object.get_mut("body_patch").and_then(Value::as_array_mut) {
                patches.retain(|patch| {
                    let Some(patch) = patch.as_object() else {
                        return false;
                    };
                    !patch
                        .get("path")
                        .and_then(Value::as_str)
                        .is_some_and(is_sensitive_override_path)
                        || patch
                            .get("value")
                            .is_none_or(is_external_template_reference)
                });
            }
            for value in object.values_mut() {
                scrub_unsafe_request_override_literals(value);
            }
        }
        Value::Array(values) => {
            for value in values {
                scrub_unsafe_request_override_literals(value);
            }
        }
        _ => {}
    }
}

fn provider_credential_status(
    app_state: &AppState,
    credential_ref: Option<&bamboo_config::CredentialRef>,
    from_environment: bool,
    runtime_configured: bool,
) -> Result<ProviderCredentialStatusView, AppError> {
    if let Some(reference) = credential_ref {
        let status = app_state
            .credential_store
            .status(reference)
            .map_err(|error| map_mutation_error(ConfigSectionMutationError::Store(error)))?;
        let source = status.configured.then_some(match status.source {
            bamboo_config::CredentialSource::User => "user",
            bamboo_config::CredentialSource::Migrated => "migrated",
            bamboo_config::CredentialSource::Environment => "environment",
            bamboo_config::CredentialSource::ExternalStore => "external_store",
        });
        return Ok(ProviderCredentialStatusView {
            credential_ref: Some(reference.as_str().to_string()),
            configured: status.configured,
            source: source.map(str::to_string),
            updated_at: status.updated_at.map(|value| value.to_rfc3339()),
        });
    }
    Ok(ProviderCredentialStatusView {
        credential_ref: None,
        configured: from_environment || runtime_configured,
        source: if from_environment {
            Some("environment".to_string())
        } else if runtime_configured {
            Some("migrated".to_string())
        } else {
            None
        },
        updated_at: None,
    })
}

fn provider_exists(providers: &ProviderConfigs, name: &str) -> bool {
    match name {
        "openai" => providers.openai.is_some(),
        "anthropic" => providers.anthropic.is_some(),
        "gemini" => providers.gemini.is_some(),
        "copilot" => providers.copilot.is_some(),
        "bodhi" => providers.bodhi.is_some(),
        _ => false,
    }
}

fn retain_provider_settings_server_owned_fields(
    current: &bamboo_config::Config,
    candidate: &mut bamboo_config::Config,
) {
    // Unknown forward-compatible metadata is intentionally not exposed by the
    // network DTO because its contents cannot be classified as non-secret.
    // Preserve it from the process-owned snapshot just like credential refs.
    candidate.providers_mut().extra = current.providers().extra.clone();
    macro_rules! retain_env_provider {
        ($field:ident) => {
            if let (Some(current), Some(candidate)) = (
                current.providers().$field.as_ref(),
                candidate.providers_mut().$field.as_mut(),
            ) {
                candidate.api_key = current.api_key.clone();
                candidate.api_key_encrypted = current.api_key_encrypted.clone();
                candidate.credential_ref = current.credential_ref.clone();
                candidate.api_key_from_env = current.api_key_from_env;
                candidate.extra = current.extra.clone();
            }
        };
    }
    retain_env_provider!(openai);
    retain_env_provider!(anthropic);
    retain_env_provider!(gemini);
    if let (Some(current), Some(candidate)) = (
        current.providers().bodhi.as_ref(),
        candidate.providers_mut().bodhi.as_mut(),
    ) {
        candidate.api_key = current.api_key.clone();
        candidate.api_key_encrypted = current.api_key_encrypted.clone();
        candidate.credential_ref = current.credential_ref.clone();
        candidate.extra = current.extra.clone();
    }
    if let (Some(current), Some(candidate)) = (
        current.providers().copilot.as_ref(),
        candidate.providers_mut().copilot.as_mut(),
    ) {
        candidate.extra = current.extra.clone();
    }
    for (id, instance) in &mut candidate.provider_instances {
        if let Some(current) = current.provider_instances.get(id) {
            instance.api_key = current.api_key.clone();
            instance.api_key_encrypted = current.api_key_encrypted.clone();
            instance.credential_ref = current.credential_ref.clone();
            let editable_extra = std::mem::take(&mut instance.extra);
            instance.extra = current
                .extra
                .iter()
                .filter(|(key, _)| {
                    !matches!(key.as_str(), "target_provider" | "thinking_replay_always")
                })
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            instance.extra.extend(editable_extra);
        }
    }
}

fn apply_provider_credential_changes(
    candidate: &mut bamboo_config::Config,
    changes: &BTreeMap<String, ProviderCredentialChange>,
) -> Result<(), ConfigSectionMutationError> {
    macro_rules! apply_env_provider {
        ($name:literal, $field:ident) => {
            if let Some(change) = changes.get($name) {
                match candidate.providers_mut().$field.as_mut() {
                    Some(provider) => match change {
                        ProviderCredentialChange::Replace { value } if !value.trim().is_empty() => {
                            provider.api_key = value.clone();
                            provider.api_key_encrypted = None;
                            provider.api_key_from_env = false;
                        }
                        ProviderCredentialChange::Replace { .. } => {
                            return Err(ConfigSectionMutationError::Invalid(format!(
                                "replacement credential for '{}' must not be empty",
                                $name
                            )));
                        }
                        ProviderCredentialChange::Clear => {
                            provider.api_key.clear();
                            provider.api_key_encrypted = None;
                            provider.api_key_from_env = false;
                        }
                    },
                    None if matches!(change, ProviderCredentialChange::Clear) => {}
                    None => {
                        return Err(ConfigSectionMutationError::Invalid(format!(
                            "credential replacement targets missing provider '{}'",
                            $name
                        )));
                    }
                }
            }
        };
    }
    apply_env_provider!("openai", openai);
    apply_env_provider!("anthropic", anthropic);
    apply_env_provider!("gemini", gemini);
    if let Some(change) = changes.get("bodhi") {
        match candidate.providers_mut().bodhi.as_mut() {
            Some(provider) => match change {
                ProviderCredentialChange::Replace { value } if !value.trim().is_empty() => {
                    provider.api_key = value.clone();
                    provider.api_key_encrypted = None;
                }
                ProviderCredentialChange::Replace { .. } => {
                    return Err(ConfigSectionMutationError::Invalid(
                        "replacement credential for 'bodhi' must not be empty".to_string(),
                    ));
                }
                ProviderCredentialChange::Clear => {
                    provider.api_key.clear();
                    provider.api_key_encrypted = None;
                }
            },
            None if matches!(change, ProviderCredentialChange::Clear) => {}
            None => {
                return Err(ConfigSectionMutationError::Invalid(
                    "credential replacement targets missing provider 'bodhi'".to_string(),
                ));
            }
        }
    }
    if let Some(name) = changes
        .keys()
        .find(|name| !matches!(name.as_str(), "openai" | "anthropic" | "gemini" | "bodhi"))
    {
        return Err(ConfigSectionMutationError::Invalid(format!(
            "unknown provider credential target '{name}'"
        )));
    }
    Ok(())
}

fn apply_provider_instance_credential_changes(
    candidate: &mut bamboo_config::Config,
    changes: &BTreeMap<String, ProviderCredentialChange>,
) -> Result<(), ConfigSectionMutationError> {
    for (id, change) in changes {
        match candidate.provider_instances.get_mut(id) {
            Some(instance) => match change {
                ProviderCredentialChange::Replace { value } if !value.trim().is_empty() => {
                    instance.api_key = value.clone();
                    instance.api_key_encrypted = None;
                }
                ProviderCredentialChange::Replace { .. } => {
                    return Err(ConfigSectionMutationError::Invalid(format!(
                        "replacement credential for provider instance '{id}' must not be empty"
                    )));
                }
                ProviderCredentialChange::Clear => {
                    instance.api_key.clear();
                    instance.api_key_encrypted = None;
                }
            },
            None if matches!(change, ProviderCredentialChange::Clear) => {}
            None => {
                return Err(ConfigSectionMutationError::Invalid(format!(
                    "credential replacement targets missing provider instance '{id}'"
                )));
            }
        }
    }
    Ok(())
}

fn validate_provider_instance_shape(
    instances: &HashMap<String, ProviderInstanceSettingsData>,
) -> Result<(), String> {
    for (id, instance) in instances {
        if id.trim().is_empty() {
            return Err("provider instance id must not be empty".to_string());
        }
        if !bamboo_llm::AVAILABLE_PROVIDERS.contains(&instance.provider_type.as_str()) {
            return Err(format!(
                "unknown provider type '{}' for provider instance '{id}'",
                instance.provider_type
            ));
        }
        if let Some(url) = instance.base_url.as_deref() {
            validate_public_url(url)?;
        }
        if let Some(target) = instance.target_provider.as_deref() {
            if instance.provider_type != "bodhi" {
                return Err(format!(
                    "target_provider is only accepted for bodhi provider instance '{id}'"
                ));
            }
            if !matches!(target, "openai" | "anthropic" | "gemini") {
                return Err(format!(
                    "unknown bodhi target provider '{target}' for provider instance '{id}'"
                ));
            }
        }
        if instance.thinking_replay_always.is_some() && instance.provider_type != "anthropic" {
            return Err(format!(
                "thinking_replay_always is only accepted for anthropic provider instance '{id}'"
            ));
        }
    }
    Ok(())
}

fn deserialize_provider_settings_candidate<'de, D>(
    deserializer: D,
) -> Result<ProviderSettingsData, D::Error>
where
    D: Deserializer<'de>,
{
    let mut value = Value::deserialize(deserializer)?;
    if let Some(object) = value.as_object_mut() {
        object.remove("available_providers");
        object.remove("credential_status");
    }
    reject_provider_settings_secret_fields(&value).map_err(D::Error::custom)?;
    serde_json::from_value(value).map_err(D::Error::custom)
}

fn reject_provider_settings_secret_fields(value: &Value) -> Result<(), String> {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                let normalized = key.to_ascii_lowercase();
                if matches!(
                    normalized.as_str(),
                    "api_key" | "api_key_encrypted" | "credential_ref"
                ) {
                    return Err(format!(
                        "secret-bearing field '{key}' is not accepted; use credential_changes"
                    ));
                }
                if normalized == "headers" {
                    reject_sensitive_header_literals(value)?;
                }
                if normalized == "body_patch" {
                    reject_sensitive_body_patch_literals(value)?;
                }
                reject_provider_settings_secret_fields(value)?;
            }
            Ok(())
        }
        Value::Array(values) => {
            for value in values {
                reject_provider_settings_secret_fields(value)?;
            }
            Ok(())
        }
        Value::String(value) if is_masked_api_key(value) => {
            Err("masked secret placeholders are not accepted".to_string())
        }
        _ => Ok(()),
    }
}

fn reject_sensitive_header_literals(value: &Value) -> Result<(), String> {
    let Some(headers) = value.as_object() else {
        return Ok(());
    };
    for (name, expression) in headers {
        if is_sensitive_header_name(name) && !is_external_template_reference(expression) {
            return Err(format!(
                "sensitive request override header '{name}' must use an env_ref or generated value"
            ));
        }
    }
    Ok(())
}

fn reject_sensitive_body_patch_literals(value: &Value) -> Result<(), String> {
    let Some(patches) = value.as_array() else {
        return Ok(());
    };
    for patch in patches.iter().filter_map(Value::as_object) {
        let sensitive_path = patch
            .get("path")
            .and_then(Value::as_str)
            .is_some_and(is_sensitive_override_path);
        if sensitive_path
            && patch
                .get("value")
                .is_some_and(|value| !is_external_template_reference(value))
        {
            return Err(
                "sensitive request override body values must use an env_ref or generated value"
                    .to_string(),
            );
        }
    }
    Ok(())
}

fn is_sensitive_header_name(name: &str) -> bool {
    let normalized = name.to_ascii_lowercase().replace('_', "-");
    [
        "authorization",
        "api-key",
        "apikey",
        "token",
        "secret",
        "password",
        "cookie",
    ]
    .iter()
    .any(|needle| normalized.contains(needle))
}

fn is_sensitive_override_path(path: &str) -> bool {
    let normalized = path.to_ascii_lowercase();
    [
        "api_key",
        "api-key",
        "apikey",
        "token",
        "secret",
        "password",
        "authorization",
        "cookie",
    ]
    .iter()
    .any(|needle| normalized.contains(needle))
}

fn is_external_template_reference(value: &Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    matches!(
        object.get("type").and_then(Value::as_str),
        Some("env_ref") | Some("generated")
    ) && object.get("fallback").is_none_or(Value::is_null)
}

fn deserialize_provider_candidate<'de, D>(deserializer: D) -> Result<ProviderConfigs, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    reject_secret_fields(&value, SecretPolicy::Provider).map_err(D::Error::custom)?;
    let candidate: ProviderConfigs = serde_json::from_value(value).map_err(D::Error::custom)?;
    validate_provider_shape(&candidate).map_err(D::Error::custom)?;
    Ok(candidate)
}

fn deserialize_mcp_candidate<'de, D>(deserializer: D) -> Result<McpConfig, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    reject_secret_fields(&value, SecretPolicy::Mcp).map_err(D::Error::custom)?;
    let candidate: McpConfig = serde_json::from_value(value).map_err(D::Error::custom)?;
    validate_mcp_public_shape(&candidate).map_err(D::Error::custom)?;
    Ok(candidate)
}

#[derive(Clone, Copy)]
enum SecretPolicy {
    Provider,
    Mcp,
}

fn reject_secret_fields(value: &Value, policy: SecretPolicy) -> Result<(), String> {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                let normalized = key.to_ascii_lowercase();
                let forbidden = match policy {
                    SecretPolicy::Provider => matches!(
                        normalized.as_str(),
                        "api_key" | "api_key_encrypted" | "request_overrides"
                    ),
                    SecretPolicy::Mcp => {
                        matches!(normalized.as_str(), "env_encrypted" | "value_encrypted")
                            || (matches!(normalized.as_str(), "env" | "headers")
                                && !value.as_object().is_some_and(Map::is_empty)
                                && !value.as_array().is_some_and(Vec::is_empty))
                    }
                };
                if forbidden {
                    return Err(format!(
                        "secret-bearing field '{key}' is not accepted; use the credential API"
                    ));
                }
                reject_secret_fields(value, policy)?;
            }
            Ok(())
        }
        Value::Array(values) => {
            for value in values {
                reject_secret_fields(value, policy)?;
            }
            Ok(())
        }
        Value::String(value) if is_masked_api_key(value) => {
            Err("masked secret placeholders are not accepted".to_string())
        }
        _ => Ok(()),
    }
}

fn validate_provider_shape(candidate: &ProviderConfigs) -> Result<(), String> {
    if !candidate.extra.is_empty() {
        return Err("unknown provider fields are not accepted by the typed endpoint".to_string());
    }
    macro_rules! validate {
        ($field:ident) => {
            if let Some(provider) = &candidate.$field {
                if !provider.extra.is_empty() {
                    return Err(
                        "unknown provider fields are not accepted by the typed endpoint"
                            .to_string(),
                    );
                }
                if let Some(url) = provider.base_url.as_deref() {
                    validate_public_url(url)?;
                }
            }
        };
    }
    validate!(openai);
    validate!(anthropic);
    validate!(gemini);
    if let Some(provider) = &candidate.copilot {
        if !provider.extra.is_empty() {
            return Err(
                "unknown provider fields are not accepted by the typed endpoint".to_string(),
            );
        }
    }
    if let Some(provider) = &candidate.bodhi {
        if !provider.extra.is_empty() {
            return Err(
                "unknown provider fields are not accepted by the typed endpoint".to_string(),
            );
        }
        if let Some(url) = provider.base_url.as_deref() {
            validate_public_url(url)?;
        }
    }
    Ok(())
}

fn validate_mcp_public_shape(candidate: &McpConfig) -> Result<(), String> {
    for server in &candidate.servers {
        match &server.transport {
            bamboo_mcp::TransportConfig::Stdio(_) => {}
            bamboo_mcp::TransportConfig::Sse(config) => validate_public_url(&config.url)?,
            bamboo_mcp::TransportConfig::StreamableHttp(config) => {
                validate_public_url(&config.url)?
            }
        }
    }
    Ok(())
}

fn validate_public_url(raw: &str) -> Result<(), String> {
    let url = url::Url::parse(raw).map_err(|_| "section URL is invalid".to_string())?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(
            "credentials, query strings, and fragments are not accepted in section URLs"
                .to_string(),
        );
    }
    Ok(())
}

fn section_envelope(
    data: Value,
    health: crate::app_state::ConfigLiveHealth,
) -> SectionEnvelope<Value> {
    SectionEnvelope {
        data,
        revision: health.revision,
        loaded_at: health.loaded_at,
        source_path: health.source_path,
        source_kind: health.source_kind,
        status: health.status,
        last_error: health.last_error,
    }
}

fn provider_diagnostics(config: &bamboo_llm::Config) -> Value {
    let providers = config.providers();
    let mut result = Map::new();

    if let Some(provider) = &providers.openai {
        result.insert(
            "openai".to_string(),
            json!({
                "api_key_configured": provider_key_configured(&provider.api_key, &provider.api_key_encrypted, provider.credential_ref.is_some()),
                "base_url": safe_url_diagnostic(provider.base_url.as_deref()),
                "model": provider.model,
                "fast_model": provider.fast_model,
                "vision_model": provider.vision_model,
                "reasoning_effort": provider.reasoning_effort,
                "responses_only_models": provider.responses_only_models,
            }),
        );
    }
    if let Some(provider) = &providers.anthropic {
        result.insert(
            "anthropic".to_string(),
            json!({
                "api_key_configured": provider_key_configured(&provider.api_key, &provider.api_key_encrypted, provider.credential_ref.is_some()),
                "base_url": safe_url_diagnostic(provider.base_url.as_deref()),
                "model": provider.model,
                "fast_model": provider.fast_model,
                "vision_model": provider.vision_model,
                "max_tokens": provider.max_tokens,
                "reasoning_effort": provider.reasoning_effort,
                "thinking_replay_always": provider.thinking_replay_always,
            }),
        );
    }
    if let Some(provider) = &providers.gemini {
        result.insert(
            "gemini".to_string(),
            json!({
                "api_key_configured": provider_key_configured(&provider.api_key, &provider.api_key_encrypted, provider.credential_ref.is_some()),
                "base_url": safe_url_diagnostic(provider.base_url.as_deref()),
                "model": provider.model,
                "fast_model": provider.fast_model,
                "vision_model": provider.vision_model,
                "reasoning_effort": provider.reasoning_effort,
            }),
        );
    }
    if let Some(provider) = &providers.copilot {
        result.insert(
            "copilot".to_string(),
            json!({
                "enabled": provider.enabled,
                "headless_auth": provider.headless_auth,
                "model": provider.model,
                "fast_model": provider.fast_model,
                "vision_model": provider.vision_model,
                "reasoning_effort": provider.reasoning_effort,
                "responses_only_models": provider.responses_only_models,
            }),
        );
    }
    if let Some(provider) = &providers.bodhi {
        result.insert(
            "bodhi".to_string(),
            json!({
                "api_key_configured": provider_key_configured(&provider.api_key, &provider.api_key_encrypted, provider.credential_ref.is_some()),
                "base_url": safe_url_diagnostic(provider.base_url.as_deref()),
                "target_provider": provider.target_provider,
                "reasoning_effort": provider.reasoning_effort,
            }),
        );
    }

    Value::Object(result)
}

fn provider_key_configured(plaintext: &str, ciphertext: &Option<String>, referenced: bool) -> bool {
    !plaintext.trim().is_empty() || ciphertext.is_some() || referenced
}

fn safe_url_diagnostic(raw: Option<&str>) -> Option<String> {
    let raw = raw?;
    let mut url = url::Url::parse(raw).ok()?;
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.set_fragment(None);
    Some(url.to_string())
}

fn mcp_server_diagnostics(server: &bamboo_mcp::McpServerConfig) -> Value {
    let transport = match &server.transport {
        bamboo_mcp::TransportConfig::Stdio(stdio) => {
            let mut env_keys = stdio.env.keys().cloned().collect::<Vec<_>>();
            env_keys.extend(stdio.env_encrypted.keys().cloned());
            env_keys.extend(stdio.env_credential_refs.keys().cloned());
            env_keys.sort();
            env_keys.dedup();
            json!({
                "type": "stdio",
                "command": stdio.command,
                "arg_count": stdio.args.len(),
                "cwd_configured": stdio.cwd.is_some(),
                "env_keys": env_keys,
                "startup_timeout_ms": stdio.startup_timeout_ms,
            })
        }
        bamboo_mcp::TransportConfig::Sse(sse) => {
            http_transport_diagnostics("sse", &sse.url, &sse.headers, sse.connect_timeout_ms)
        }
        bamboo_mcp::TransportConfig::StreamableHttp(http) => http_transport_diagnostics(
            "streamable_http",
            &http.url,
            &http.headers,
            http.connect_timeout_ms,
        ),
    };

    json!({
        "id": server.id,
        "name": server.name,
        "enabled": server.enabled,
        "transport": transport,
        "request_timeout_ms": server.request_timeout_ms,
        "healthcheck_interval_ms": server.healthcheck_interval_ms,
        "reconnect": server.reconnect,
        "allowed_tools": server.allowed_tools,
        "denied_tools": server.denied_tools,
    })
}

fn http_transport_diagnostics(
    kind: &str,
    url: &str,
    headers: &[bamboo_mcp::HeaderConfig],
    connect_timeout_ms: u64,
) -> Value {
    let mut header_names = headers
        .iter()
        .filter_map(|header| {
            let name = header.name.trim();
            (!name.is_empty()).then(|| name.to_string())
        })
        .collect::<Vec<_>>();
    header_names.sort();
    header_names.dedup();
    json!({
        "type": kind,
        "url": safe_url_diagnostic(Some(url)),
        "header_names": header_names,
        "connect_timeout_ms": connect_timeout_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{test, App};
    use bamboo_config::{OpenAIConfig, ProviderConfigs, SectionSourceKind, SectionStatus};
    use bamboo_mcp::{
        HeaderConfig, McpConfig, McpServerConfig, ReconnectConfig, StdioConfig,
        StreamableHttpConfig, TransportConfig,
    };
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use std::time::Duration;

    fn server(id: &str, transport: TransportConfig) -> McpServerConfig {
        McpServerConfig {
            id: id.to_string(),
            name: Some(format!("{id} diagnostics")),
            enabled: false,
            transport,
            request_timeout_ms: 2_000,
            healthcheck_interval_ms: 3_000,
            reconnect: ReconnectConfig::default(),
            allowed_tools: vec!["read".to_string()],
            denied_tools: vec!["delete".to_string()],
        }
    }

    fn editable_provider_settings() -> Value {
        json!({
            "provider": "openai",
            "providers": {
                "openai": {
                    "base_url": "https://openai.example.test/v1",
                    "model": "gpt-initial"
                },
                "anthropic": {
                    "base_url": "https://anthropic.example.test/v1",
                    "model": "claude-initial"
                }
            },
            "defaults": null,
            "features": {
                "provider_model_ref": false,
                "dynamic_model_routing": false
            },
            "provider_instances": {},
            "default_provider_instance_id": null
        })
    }

    #[actix_web::test]
    async fn typed_sections_expose_health_and_diagnostics_without_secret_material() {
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        {
            let mut config = state.config.write().await;
            *config.providers_mut() = ProviderConfigs {
                openai: Some(OpenAIConfig {
                    api_key: "provider-plaintext-secret".to_string(),
                    api_key_encrypted: Some("provider-ciphertext-secret".to_string()),
                    credential_ref: None,
                    base_url: Some(
                        "https://provider-url-secret@provider.example/v1?token=query-secret"
                            .to_string(),
                    ),
                    model: Some("diagnostic-model".to_string()),
                    request_overrides: Some(
                        serde_json::from_value(json!({
                            "common": {"headers": {"Authorization": "override-header-secret"}}
                        }))
                        .unwrap(),
                    ),
                    extra: BTreeMap::from([(
                        "future_secret".to_string(),
                        json!("unknown-provider-secret"),
                    )]),
                    ..OpenAIConfig::default()
                }),
                ..ProviderConfigs::default()
            };
            config.mcp = McpConfig {
                version: 1,
                servers: vec![
                    server(
                        "stdio",
                        TransportConfig::Stdio(StdioConfig {
                            command: "diagnostic-command".to_string(),
                            args: vec!["mcp-argument-secret".to_string()],
                            cwd: Some("/safe/workspace".to_string()),
                            env: HashMap::from([(
                                "TOKEN".to_string(),
                                "mcp-env-plaintext-secret".to_string(),
                            )]),
                            env_encrypted: HashMap::from([(
                                "LEGACY_TOKEN".to_string(),
                                "mcp-env-ciphertext-secret".to_string(),
                            )]),
                            env_credential_refs: HashMap::new(),
                            startup_timeout_ms: 4_000,
                        }),
                    ),
                    server(
                        "http",
                        TransportConfig::StreamableHttp(StreamableHttpConfig {
                            url: "https://mcp-url-secret@mcp.example/rpc?token=mcp-query-secret"
                                .to_string(),
                            headers: vec![HeaderConfig {
                                name: "Authorization".to_string(),
                                value: "mcp-header-plaintext-secret".to_string(),
                                value_encrypted: Some("mcp-header-ciphertext-secret".to_string()),
                                credential_ref: None,
                            }],
                            connect_timeout_ms: 5_000,
                        }),
                    ),
                ],
            };
        }
        {
            let mut health = state
                .config_live_health
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            health.revision = 7;
            health.source_kind = SectionSourceKind::File;
            health.status = SectionStatus::Healthy;
            health.last_error = None;
        }
        {
            let mut health = state
                .mcp_config_live_health
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            health.revision = 11;
            health.source_kind = SectionSourceKind::Backup;
            health.status = SectionStatus::Degraded;
            health.last_error = Some("redacted runtime failure".to_string());
        }

        let app = test::init_service(
            App::new()
                .app_data(state)
                .route("/providers", web::get().to(get_provider_section))
                .route(
                    "/provider-settings",
                    web::get().to(get_provider_settings_section),
                )
                .route("/mcp", web::get().to(get_mcp_section)),
        )
        .await;

        let provider_response = test::call_service(
            &app,
            test::TestRequest::get().uri("/providers").to_request(),
        )
        .await;
        assert!(provider_response.status().is_success());
        let provider_body =
            String::from_utf8(test::read_body(provider_response).await.to_vec()).unwrap();
        for forbidden in [
            "provider-plaintext-secret",
            "provider-ciphertext-secret",
            "override-header-secret",
            "unknown-provider-secret",
            "provider-url-secret",
            "query-secret",
            "****...****",
            "request_overrides",
            "api_key_encrypted",
        ] {
            assert!(!provider_body.contains(forbidden), "leaked {forbidden}");
        }
        let provider: Value = serde_json::from_str(&provider_body).unwrap();
        assert_eq!(provider["revision"], 7);
        assert_eq!(provider["status"], "healthy");
        assert_eq!(provider["source_kind"], "file");
        assert_eq!(
            provider["source_path"],
            dir.path().join("providers.json").to_string_lossy().as_ref()
        );
        assert_eq!(
            provider["data"]["providers"]["openai"]["api_key_configured"],
            true
        );
        assert_eq!(
            provider["data"]["providers"]["openai"]["model"],
            "diagnostic-model"
        );
        assert_eq!(
            provider["data"]["providers"]["openai"]["base_url"],
            "https://provider.example/v1"
        );

        let provider_settings_response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/provider-settings")
                .to_request(),
        )
        .await;
        assert!(provider_settings_response.status().is_success());
        let provider_settings_body =
            String::from_utf8(test::read_body(provider_settings_response).await.to_vec()).unwrap();
        for forbidden in [
            "provider-plaintext-secret",
            "provider-ciphertext-secret",
            "override-header-secret",
            "unknown-provider-secret",
            "provider-url-secret",
            "query-secret",
            "****...****",
            "api_key_encrypted",
        ] {
            assert!(
                !provider_settings_body.contains(forbidden),
                "provider settings leaked {forbidden}"
            );
        }
        let provider_settings: Value = serde_json::from_str(&provider_settings_body).unwrap();
        assert_eq!(
            provider_settings["data"]["providers"]["openai"]["model"],
            "diagnostic-model"
        );
        assert_eq!(
            provider_settings["data"]["providers"]["openai"]["base_url"],
            "https://provider.example/v1"
        );

        let mcp_response =
            test::call_service(&app, test::TestRequest::get().uri("/mcp").to_request()).await;
        assert!(mcp_response.status().is_success());
        let mcp_body = String::from_utf8(test::read_body(mcp_response).await.to_vec()).unwrap();
        for forbidden in [
            "mcp-env-plaintext-secret",
            "mcp-env-ciphertext-secret",
            "mcp-header-plaintext-secret",
            "mcp-header-ciphertext-secret",
            "mcp-argument-secret",
            "mcp-url-secret",
            "mcp-query-secret",
            "****...****",
            "value_encrypted",
            "env_encrypted",
        ] {
            assert!(!mcp_body.contains(forbidden), "leaked {forbidden}");
        }
        let mcp: Value = serde_json::from_str(&mcp_body).unwrap();
        assert_eq!(mcp["revision"], 11);
        assert_eq!(mcp["status"], "degraded");
        assert_eq!(mcp["source_kind"], "backup");
        assert_eq!(
            mcp["source_path"],
            dir.path().join("mcp.json").to_string_lossy().as_ref()
        );
        assert_eq!(mcp["last_error"], "redacted runtime failure");
        assert_eq!(
            mcp["data"]["servers"][0]["transport"]["env_keys"],
            json!(["LEGACY_TOKEN", "TOKEN"])
        );
        assert_eq!(
            mcp["data"]["servers"][1]["transport"]["header_names"],
            json!(["Authorization"])
        );
        assert_eq!(
            mcp["data"]["servers"][1]["transport"]["url"],
            "https://mcp.example/rpc"
        );
        assert_eq!(mcp["data"]["servers"][0]["transport"]["arg_count"], 1);
        assert_eq!(
            mcp["data"]["servers"][0]["transport"]["cwd_configured"],
            true
        );
    }

    #[actix_web::test]
    async fn provider_section_waits_for_atomic_config_and_health_publication() {
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let io = state.config_io_lock.lock().await;
        let mut response = Box::pin(get_provider_section(state.clone()));

        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut response)
                .await
                .is_err()
        );

        state.config.write().await.provider = "coherent-provider".to_string();
        {
            let mut health = state
                .config_live_health
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            health.revision = 99;
            health.status = SectionStatus::Healthy;
        }
        drop(io);

        let response = tokio::time::timeout(Duration::from_secs(2), response)
            .await
            .expect("handler resumes after publication")
            .unwrap();
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["revision"], 99);
        assert_eq!(body["data"]["active_provider"], "coherent-provider");
    }

    #[actix_web::test]
    async fn provider_put_uses_observed_legacy_cas_preserves_secret_and_redacts_response() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x51; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let secret = "provider-put-secret-597";
        let reference = bamboo_config::credential_ref("provider", "openai", "api_key").unwrap();
        state
            .credential_store
            .replace(
                reference.clone(),
                secret,
                bamboo_config::CredentialSource::User,
                0,
            )
            .unwrap();
        {
            let mut config = state.config.write().await;
            config.provider = "openai".to_string();
            *config.providers_mut() = ProviderConfigs {
                openai: Some(OpenAIConfig {
                    api_key: secret.to_string(),
                    credential_ref: Some(reference),
                    model: Some("old-model".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            };
        }
        let raw = serde_json::to_vec_pretty(state.config.read().await.providers()).unwrap();
        std::fs::write(dir.path().join("providers.json"), raw).unwrap();
        assert!(matches!(
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .providers
                .reload(),
            bamboo_config::ConfigSectionEvent::Changed { revision: 1, .. }
        ));
        let mut feed = state.account_sink.subscribe();

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .route("/providers", web::put().to(put_provider_section)),
        )
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/providers")
                .set_json(json!({
                    "expected_revision": 1,
                    "data": {"openai": {"model": "new-model"}}
                }))
                .to_request(),
        )
        .await;
        let status = response.status();
        let body = String::from_utf8(test::read_body(response).await.to_vec()).unwrap();
        assert!(status.is_success(), "unexpected response {status}: {body}");
        assert!(!body.contains(secret));
        assert!(!body.contains("api_key_encrypted"));
        let body: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(body["revision"], 2);
        assert_eq!(body["data"]["providers"]["openai"]["model"], "new-model");
        assert_eq!(
            state
                .config
                .read()
                .await
                .providers()
                .openai
                .as_ref()
                .unwrap()
                .api_key,
            secret
        );

        let disk = std::fs::read_to_string(dir.path().join("providers.json")).unwrap();
        assert!(!disk.contains(secret));
        let disk: Value = serde_json::from_str(&disk).unwrap();
        assert_eq!(disk["revision"], 2);
        assert!(disk["data"]["openai"]["credential_ref"].is_string());
        assert!(disk["data"]["openai"].get("api_key_encrypted").is_none());
        let first = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let event = feed.recv().await.unwrap();
                if matches!(
                    &event.event,
                    bamboo_agent_core::AgentEvent::ConfigChanged { section, revision }
                        | bamboo_agent_core::AgentEvent::ConfigRecovered { section, revision }
                        if section == "providers" && *revision == 2
                ) {
                    break;
                }
            }
        })
        .await;
        assert!(first.is_ok());
        let duplicate_provider = tokio::time::timeout(Duration::from_millis(500), async {
            loop {
                let event = feed.recv().await.unwrap();
                if matches!(
                    &event.event,
                    bamboo_agent_core::AgentEvent::ConfigChanged { section, .. }
                        | bamboo_agent_core::AgentEvent::ConfigRecovered { section, .. }
                        | bamboo_agent_core::AgentEvent::ConfigInvalid { section, .. }
                        if section == "providers"
                ) {
                    break event.event.clone();
                }
            }
        })
        .await;
        assert!(
            duplicate_provider.is_err(),
            "the provider watcher echo must not publish a duplicate event: {duplicate_provider:?}"
        );

        let stale = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/providers")
                .set_json(json!({
                    "expected_revision": 0,
                    "data": {"openai": {"model": "stale-model"}}
                }))
                .to_request(),
        )
        .await;
        assert_eq!(stale.status(), actix_web::http::StatusCode::CONFLICT);
        assert_eq!(
            state
                .config
                .read()
                .await
                .providers()
                .openai
                .as_ref()
                .unwrap()
                .model
                .as_deref(),
            Some("new-model")
        );

        let mut external = state.config.read().await.providers().clone();
        external.openai.as_mut().unwrap().model = Some("external-model".to_string());
        std::fs::write(
            dir.path().join("providers.json"),
            serde_json::to_vec_pretty(&external).unwrap(),
        )
        .unwrap();
        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                let health = state
                    .config_live_health
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone();
                if health.status == SectionStatus::Healthy && health.revision == 3 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("external raw provider edit is normalized");
        let normalized: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("providers.json")).unwrap())
                .unwrap();
        assert_eq!(normalized["revision"], 3);
        let stale_after_external = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/providers")
                .set_json(json!({
                    "expected_revision": 2,
                    "data": {"openai": {"model": "lost-update"}}
                }))
                .to_request(),
        )
        .await;
        assert_eq!(
            stale_after_external.status(),
            actix_web::http::StatusCode::CONFLICT
        );
    }

    #[actix_web::test]
    async fn provider_settings_round_trip_is_full_cas_and_credentials_are_explicit_and_redacted() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x70; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let app = test::init_service(
            App::new().app_data(state.clone()).service(
                web::resource("/provider-settings")
                    .route(web::get().to(get_provider_settings_section))
                    .route(web::put().to(put_provider_settings_section)),
            ),
        )
        .await;
        let openai_secret = "provider-settings-openai-secret";
        let anthropic_secret = "provider-settings-anthropic-secret";
        let instance_secret = "provider-settings-instance-secret";
        let deleted_instance_secret = "provider-settings-deleted-instance-secret";

        let created = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/provider-settings")
                .set_json(json!({
                    "expected_revision": 0,
                    "data": editable_provider_settings(),
                    "credential_changes": {
                        "providers": {
                            "openai": {"action": "replace", "value": openai_secret},
                            "anthropic": {"action": "replace", "value": anthropic_secret}
                        }
                    }
                }))
                .to_request(),
        )
        .await;
        let status = created.status();
        let created_body = String::from_utf8(test::read_body(created).await.to_vec()).unwrap();
        assert!(
            status.is_success(),
            "unexpected provider settings response {status}: {created_body}"
        );
        for secret in [
            openai_secret,
            anthropic_secret,
            instance_secret,
            deleted_instance_secret,
        ] {
            assert!(!created_body.contains(secret), "response leaked {secret}");
        }
        for forbidden in ["\"api_key\":", "\"api_key_encrypted\":"] {
            assert!(
                !created_body.contains(forbidden),
                "response leaked secret-bearing field {forbidden}"
            );
        }
        let created: Value = serde_json::from_str(&created_body).unwrap();
        assert_eq!(created["revision"], 1);
        assert_eq!(
            created["data"]["credential_status"]["providers"]["openai"]["configured"],
            true
        );
        assert_eq!(
            created["data"]["credential_status"]["providers"]["anthropic"]["source"],
            "user"
        );
        let hidden_forward_value = "server-owned-forward-metadata";
        state
            .config
            .write()
            .await
            .providers_mut()
            .openai
            .as_mut()
            .unwrap()
            .extra
            .insert(
                "future_provider_metadata".to_string(),
                json!(hidden_forward_value),
            );

        let mut edited = created["data"].clone();
        edited["features"]["provider_model_ref"] = json!(true);
        edited["defaults"] = json!({
            "chat": {"provider": "openai", "model": "gpt-edited"},
            "fast": {"provider": "openai", "model": "gpt-fast"}
        });
        edited["providers"]["openai"]["model"] = json!("gpt-edited");
        edited["providers"]["openai"]["request_overrides"] = json!({
            "common": {
                "headers": {
                    "Authorization": {"type": "env_ref", "name": "PROVIDER_AUTH"}
                }
            }
        });
        edited["provider_instances"]["work"] = json!({
            "provider_type": "openai",
            "label": "Work",
            "model": "gpt-work",
            "enabled": true
        });
        edited["provider_instances"]["personal"] = json!({
            "provider_type": "openai",
            "label": "Personal",
            "model": "gpt-personal",
            "enabled": false
        });
        edited["default_provider_instance_id"] = json!("work");

        let updated = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/provider-settings")
                .set_json(json!({
                    "expected_revision": 1,
                    "data": edited,
                    "credential_changes": {
                        "provider_instances": {
                            "work": {"action": "replace", "value": instance_secret},
                            "personal": {
                                "action": "replace",
                                "value": deleted_instance_secret
                            }
                        }
                    }
                }))
                .to_request(),
        )
        .await;
        let status = updated.status();
        let updated_body = String::from_utf8(test::read_body(updated).await.to_vec()).unwrap();
        assert!(
            status.is_success(),
            "unexpected provider settings response {status}: {updated_body}"
        );
        for secret in [
            openai_secret,
            anthropic_secret,
            instance_secret,
            deleted_instance_secret,
        ] {
            assert!(!updated_body.contains(secret), "response leaked {secret}");
        }
        assert!(!updated_body.contains(hidden_forward_value));
        let updated: Value = serde_json::from_str(&updated_body).unwrap();
        assert_eq!(updated["revision"], 2);
        assert_eq!(
            updated["data"]["providers"]["openai"]["model"],
            "gpt-edited"
        );
        assert_eq!(updated["data"]["defaults"]["fast"]["model"], "gpt-fast");
        assert_eq!(updated["data"]["features"]["provider_model_ref"], true);
        assert_eq!(updated["data"]["default_provider_instance_id"], "work");
        assert_eq!(
            updated["data"]["credential_status"]["provider_instances"]["work"]["configured"],
            true
        );

        let stale = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/provider-settings")
                .set_json(json!({
                    "expected_revision": 1,
                    "data": editable_provider_settings()
                }))
                .to_request(),
        )
        .await;
        assert_eq!(stale.status(), actix_web::http::StatusCode::CONFLICT);
        assert_eq!(
            state
                .config
                .read()
                .await
                .defaults
                .as_ref()
                .unwrap()
                .fast
                .as_ref()
                .unwrap()
                .model,
            "gpt-fast"
        );

        let mut cleared = updated["data"].clone();
        cleared["provider_instances"] = json!({});
        cleared["default_provider_instance_id"] = Value::Null;
        let cleared = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/provider-settings")
                .set_json(json!({
                    "expected_revision": 2,
                    "data": cleared,
                    "credential_changes": {
                        "provider_instances": {
                            "work": {"action": "clear"}
                        }
                    }
                }))
                .to_request(),
        )
        .await;
        let status = cleared.status();
        let cleared_body = String::from_utf8(test::read_body(cleared).await.to_vec()).unwrap();
        assert!(
            status.is_success(),
            "unexpected provider settings response {status}: {cleared_body}"
        );
        let cleared: Value = serde_json::from_str(&cleared_body).unwrap();
        assert_eq!(cleared["revision"], 3);
        assert!(cleared["data"]["provider_instances"]["work"].is_null());
        assert!(cleared["data"]["provider_instances"]["personal"].is_null());

        let mut metadata_only = cleared["data"].clone();
        metadata_only["providers"]["openai"]["model"] = json!("gpt-metadata-only");
        metadata_only["features"]["dynamic_model_routing"] = json!(true);
        let metadata_only = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/provider-settings")
                .set_json(json!({
                    "expected_revision": 3,
                    "data": metadata_only
                }))
                .to_request(),
        )
        .await;
        let status = metadata_only.status();
        let metadata_only_body =
            String::from_utf8(test::read_body(metadata_only).await.to_vec()).unwrap();
        assert!(
            status.is_success(),
            "unexpected provider settings response {status}: {metadata_only_body}"
        );
        let metadata_only: Value = serde_json::from_str(&metadata_only_body).unwrap();
        assert_eq!(metadata_only["revision"], 4);
        assert_eq!(
            metadata_only["data"]["providers"]["openai"]["model"],
            "gpt-metadata-only"
        );
        assert_eq!(
            metadata_only["data"]["features"]["dynamic_model_routing"],
            true
        );
        assert_eq!(
            state
                .config
                .read()
                .await
                .providers()
                .openai
                .as_ref()
                .unwrap()
                .extra["future_provider_metadata"],
            hidden_forward_value
        );

        let mut unknown = metadata_only["data"].clone();
        unknown["providers"]["openai"]["future_client_field"] = json!("must-reject");
        let unknown = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/provider-settings")
                .set_json(json!({
                    "expected_revision": 4,
                    "data": unknown
                }))
                .to_request(),
        )
        .await;
        assert_eq!(unknown.status(), actix_web::http::StatusCode::BAD_REQUEST);

        let store = bamboo_config::CredentialStore::open(dir.path());
        let openai_ref = bamboo_config::credential_ref("provider", "openai", "api_key").unwrap();
        let work_ref =
            bamboo_config::credential_ref("provider_instance", "work", "api_key").unwrap();
        let personal_ref =
            bamboo_config::credential_ref("provider_instance", "personal", "api_key").unwrap();
        assert!(store.status(&openai_ref).unwrap().configured);
        assert!(!store.status(&work_ref).unwrap().configured);
        assert!(!store.status(&personal_ref).unwrap().configured);
        assert_eq!(store.revision().unwrap(), 4);
        for path in [
            dir.path().join("providers.json"),
            dir.path().join("credentials.json"),
        ] {
            let disk = std::fs::read_to_string(&path).unwrap();
            for secret in [
                openai_secret,
                anthropic_secret,
                instance_secret,
                deleted_instance_secret,
            ] {
                assert!(!disk.contains(secret), "disk leaked {secret}");
            }
            if path.ends_with("providers.json") {
                assert!(disk.contains(hidden_forward_value));
            }
        }
    }

    #[actix_web::test]
    async fn provider_instance_runtime_fields_are_explicit_editable_and_preserve_unknown_metadata()
    {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x71; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let app = test::init_service(
            App::new().app_data(state.clone()).service(
                web::resource("/provider-settings")
                    .route(web::get().to(get_provider_settings_section))
                    .route(web::put().to(put_provider_settings_section)),
            ),
        )
        .await;
        let data = json!({
            "provider": "openai",
            "providers": {
                "openai": {
                    "model": "gpt-test"
                }
            },
            "defaults": null,
            "features": {},
            "provider_instances": {
                "bodhi-proxy": {
                    "provider_type": "bodhi",
                    "label": "Bodhi proxy",
                    "target_provider": "gemini",
                    "enabled": false
                },
                "glm-compat": {
                    "provider_type": "anthropic",
                    "label": "GLM compatible",
                    "thinking_replay_always": true,
                    "enabled": false
                }
            },
            "default_provider_instance_id": null
        });

        let created = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/provider-settings")
                .set_json(json!({
                    "expected_revision": 0,
                    "data": data,
                    "credential_changes": {
                        "providers": {
                            "openai": {
                                "action": "replace",
                                "value": "provider-instance-contract-test-secret"
                            }
                        }
                    }
                }))
                .to_request(),
        )
        .await;
        let status = created.status();
        let created_body = String::from_utf8(test::read_body(created).await.to_vec()).unwrap();
        assert!(
            status.is_success(),
            "unexpected provider instance settings response {status}: {created_body}"
        );
        assert!(!created_body.contains("provider-instance-contract-test-secret"));
        let created: Value = serde_json::from_str(&created_body).unwrap();
        assert_eq!(
            created["data"]["provider_instances"]["bodhi-proxy"]["target_provider"],
            "gemini"
        );
        assert_eq!(
            created["data"]["provider_instances"]["glm-compat"]["thinking_replay_always"],
            true
        );

        let hidden_value = "server-owned-provider-instance-metadata";
        state
            .config
            .write()
            .await
            .provider_instances
            .get_mut("bodhi-proxy")
            .unwrap()
            .extra
            .insert("future_server_field".to_string(), json!(hidden_value));

        let mut updated = created["data"].clone();
        updated["provider_instances"]["bodhi-proxy"]["target_provider"] = json!("anthropic");
        updated["provider_instances"]["glm-compat"]["thinking_replay_always"] = json!(false);
        let updated = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/provider-settings")
                .set_json(json!({"expected_revision": 1, "data": updated}))
                .to_request(),
        )
        .await;
        assert!(updated.status().is_success());
        let updated_body = String::from_utf8(test::read_body(updated).await.to_vec()).unwrap();
        assert!(!updated_body.contains(hidden_value));
        let updated: Value = serde_json::from_str(&updated_body).unwrap();
        assert_eq!(updated["revision"], 2);
        assert_eq!(
            updated["data"]["provider_instances"]["bodhi-proxy"]["target_provider"],
            "anthropic"
        );
        assert_eq!(
            updated["data"]["provider_instances"]["glm-compat"]["thinking_replay_always"],
            false
        );
        {
            let config = state.config.read().await;
            assert_eq!(
                config.provider_instances["bodhi-proxy"].extra["future_server_field"],
                hidden_value
            );
            assert_eq!(
                config.provider_instances["bodhi-proxy"].extra["target_provider"],
                "anthropic"
            );
            assert_eq!(
                config.provider_instances["glm-compat"].extra["thinking_replay_always"],
                false
            );
        }

        let mut removed = updated["data"].clone();
        removed["provider_instances"]["bodhi-proxy"]
            .as_object_mut()
            .unwrap()
            .remove("target_provider");
        removed["provider_instances"]["glm-compat"]
            .as_object_mut()
            .unwrap()
            .remove("thinking_replay_always");
        let removed = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/provider-settings")
                .set_json(json!({"expected_revision": 2, "data": removed}))
                .to_request(),
        )
        .await;
        assert!(removed.status().is_success());
        let removed: Value = test::read_body_json(removed).await;
        assert_eq!(removed["revision"], 3);
        assert!(removed["data"]["provider_instances"]["bodhi-proxy"]["target_provider"].is_null());
        assert!(
            removed["data"]["provider_instances"]["glm-compat"]["thinking_replay_always"].is_null()
        );
        {
            let config = state.config.read().await;
            assert_eq!(
                config.provider_instances["bodhi-proxy"].extra["future_server_field"],
                hidden_value
            );
            assert!(!config.provider_instances["bodhi-proxy"]
                .extra
                .contains_key("target_provider"));
            assert!(!config.provider_instances["glm-compat"]
                .extra
                .contains_key("thinking_replay_always"));
        }

        for (id, field, value) in [
            ("bodhi-proxy", "future_client_field", json!("must reject")),
            ("bodhi-proxy", "thinking_replay_always", json!(true)),
            ("bodhi-proxy", "target_provider", json!("copilot")),
            ("glm-compat", "target_provider", json!("openai")),
        ] {
            let mut invalid = removed["data"].clone();
            invalid["provider_instances"][id][field] = value;
            let response = test::call_service(
                &app,
                test::TestRequest::put()
                    .uri("/provider-settings")
                    .set_json(json!({"expected_revision": 3, "data": invalid}))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
        }
    }

    #[actix_web::test]
    async fn provider_put_rejects_invalid_credential_refs_without_mutating_lkg() {
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let providers_before = std::fs::read(dir.path().join("providers.json")).unwrap();
        state.config.write().await.providers_mut().openai = Some(OpenAIConfig {
            model: Some("lkg-model".to_string()),
            ..Default::default()
        });
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .route("/providers", web::put().to(put_provider_section)),
        )
        .await;

        for invalid_ref in ["../credentials".to_string(), "x".repeat(161)] {
            let response = test::call_service(
                &app,
                test::TestRequest::put()
                    .uri("/providers")
                    .set_json(json!({
                        "expected_revision": 0,
                        "data": {"openai": {
                            "model": "must-not-publish",
                            "credential_ref": invalid_ref
                        }}
                    }))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
        }
        assert_eq!(
            std::fs::read(dir.path().join("providers.json")).unwrap(),
            providers_before
        );
        assert_eq!(
            state
                .config
                .read()
                .await
                .providers()
                .openai
                .as_ref()
                .unwrap()
                .model
                .as_deref(),
            Some("lkg-model")
        );
    }

    #[actix_web::test]
    async fn mcp_put_preserves_secret_stages_runtime_and_retains_lkg_on_failure() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x52; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let health = state
                    .mcp_config_live_health
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone();
                if health.status == SectionStatus::Healthy && health.revision == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("initial MCP runtime reconciliation completes");
        let secret = "mcp-put-secret-597";
        let env_reference = bamboo_config::credential_ref("mcp", "preserved", "env_TOKEN").unwrap();
        let header_reference =
            bamboo_config::credential_ref("mcp", "preserved-http", "header_Authorization").unwrap();
        state
            .credential_store
            .replace(
                env_reference.clone(),
                secret,
                bamboo_config::CredentialSource::User,
                0,
            )
            .unwrap();
        state
            .credential_store
            .replace(
                header_reference.clone(),
                secret,
                bamboo_config::CredentialSource::User,
                1,
            )
            .unwrap();
        let current = McpConfig {
            version: 1,
            servers: vec![
                server(
                    "preserved",
                    TransportConfig::Stdio(StdioConfig {
                        command: "unused-disabled-command".to_string(),
                        args: Vec::new(),
                        cwd: None,
                        env: HashMap::from([("TOKEN".to_string(), secret.to_string())]),
                        env_encrypted: HashMap::new(),
                        env_credential_refs: HashMap::from([(
                            "TOKEN".to_string(),
                            env_reference.as_str().to_string(),
                        )]),
                        startup_timeout_ms: 500,
                    }),
                ),
                server(
                    "preserved-http",
                    TransportConfig::StreamableHttp(StreamableHttpConfig {
                        url: "https://mcp.example/rpc".to_string(),
                        headers: vec![HeaderConfig {
                            name: "Authorization".to_string(),
                            value: secret.to_string(),
                            value_encrypted: None,
                            credential_ref: Some(header_reference.as_str().to_string()),
                        }],
                        connect_timeout_ms: 500,
                    }),
                ),
            ],
        };
        state.config.write().await.mcp = current;
        let mut feed = state.account_sink.subscribe();

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .route("/mcp", web::put().to(put_mcp_section)),
        )
        .await;
        let candidate = McpConfig {
            version: 1,
            servers: vec![
                server(
                    "preserved",
                    TransportConfig::Stdio(StdioConfig {
                        command: "updated-disabled-command".to_string(),
                        args: vec!["--safe".to_string()],
                        cwd: None,
                        env: HashMap::new(),
                        env_encrypted: HashMap::new(),
                        env_credential_refs: std::collections::HashMap::new(),
                        startup_timeout_ms: 500,
                    }),
                ),
                server(
                    "preserved-http",
                    TransportConfig::StreamableHttp(StreamableHttpConfig {
                        url: "https://mcp.example/rpc".to_string(),
                        headers: Vec::new(),
                        connect_timeout_ms: 500,
                    }),
                ),
            ],
        };
        let response = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/mcp")
                .set_json(json!({"expected_revision": 0, "data": candidate.clone()}))
                .to_request(),
        )
        .await;
        let status = response.status();
        let body = String::from_utf8(test::read_body(response).await.to_vec()).unwrap();
        assert!(status.is_success(), "unexpected response {status}: {body}");
        assert!(!body.contains(secret));
        let config = state.config.read().await;
        let TransportConfig::Stdio(stdio) = &config.mcp.servers[0].transport else {
            panic!("expected stdio transport");
        };
        assert_eq!(stdio.env["TOKEN"], secret);
        let TransportConfig::StreamableHttp(http) = &config.mcp.servers[1].transport else {
            panic!("expected streamable HTTP transport");
        };
        assert_eq!(http.headers[0].value, secret);
        drop(config);
        let disk = std::fs::read_to_string(dir.path().join("mcp.json")).unwrap();
        assert!(!disk.contains(secret));
        assert!(!disk.contains("env_encrypted"));
        assert!(!disk.contains("headers_encrypted"));
        assert!(disk.contains("env_credential_refs"));
        assert!(disk.contains("header_credential_refs"));
        let first = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let event = feed.recv().await.unwrap();
                if matches!(
                    &event.event,
                    bamboo_agent_core::AgentEvent::ConfigChanged { section, revision }
                        | bamboo_agent_core::AgentEvent::ConfigRecovered { section, revision }
                        if section == "mcp" && *revision == 1
                ) {
                    break;
                }
            }
        })
        .await;
        assert!(first.is_ok());
        let duplicate_mcp = tokio::time::timeout(Duration::from_millis(500), async {
            loop {
                let event = feed.recv().await.unwrap();
                if matches!(
                    &event.event,
                    bamboo_agent_core::AgentEvent::ConfigChanged { section, .. }
                        | bamboo_agent_core::AgentEvent::ConfigRecovered { section, .. }
                        | bamboo_agent_core::AgentEvent::ConfigInvalid { section, .. }
                        if section == "mcp"
                ) {
                    break event.event.clone();
                }
            }
        })
        .await;
        assert!(
            duplicate_mcp.is_err(),
            "the MCP watcher echo must not publish a duplicate event: {duplicate_mcp:?}"
        );
        assert_eq!(
            state
                .mcp_config_live_health
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .revision,
            1
        );

        let mut failing = candidate;
        failing.servers[0].enabled = true;
        if let TransportConfig::Stdio(stdio) = &mut failing.servers[0].transport {
            stdio.command = "definitely-not-a-real-mcp-command-597".to_string();
        }
        let failure = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/mcp")
                .set_json(json!({"expected_revision": 1, "data": failing}))
                .to_request(),
        )
        .await;
        assert_eq!(failure.status(), actix_web::http::StatusCode::BAD_REQUEST);
        let persisted: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("mcp.json")).unwrap()).unwrap();
        assert_eq!(persisted["revision"], 1);
        assert!(
            !state.config.read().await.mcp.servers[0].enabled,
            "runtime failure must retain the last-known-good config"
        );
        let health = state
            .mcp_config_live_health
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        assert_eq!(health.revision, 1);
        assert_eq!(health.status, SectionStatus::Degraded);
    }

    #[::core::prelude::v1::test]
    fn typed_writes_reject_secret_fields_masks_and_credential_urls() {
        for payload in [
            json!({"expected_revision": 0, "data": {"openai": {"api_key": "secret"}}}),
            json!({"expected_revision": 0, "data": {"openai": {"model": "****...****"}}}),
            json!({"expected_revision": 0, "data": {"openai": {"base_url": "https://user:pass@example.test/v1"}}}),
        ] {
            assert!(serde_json::from_value::<PutProviderSectionRequest>(payload).is_err());
        }
        for payload in [
            json!({"expected_revision": 0, "data": {"server": {"command": "cmd", "env": {"TOKEN": "secret"}}}}),
            json!({"expected_revision": 0, "data": {"server": {"url": "https://example.test/mcp?token=secret"}}}),
        ] {
            assert!(serde_json::from_value::<PutMcpSectionRequest>(payload).is_err());
        }

        assert!(serde_json::from_value::<PutProviderSectionRequest>(json!({
            "expected_revision": 0,
            "data": {"openai": {"model": "foo****bar"}}
        }))
        .is_ok());
        assert!(serde_json::from_value::<PutMcpSectionRequest>(json!({
            "expected_revision": 0,
            "data": {"server": {"command": "cmd****name"}}
        }))
        .is_ok());
    }

    #[::core::prelude::v1::test]
    fn provider_settings_accept_external_override_refs_and_reject_secret_literals() {
        let mut literal = editable_provider_settings();
        literal["providers"]["openai"]["request_overrides"] = json!({
            "common": {"headers": {"Authorization": "Bearer secret"}}
        });
        assert!(serde_json::from_value::<PutProviderSettingsRequest>(json!({
            "expected_revision": 0,
            "data": literal
        }))
        .is_err());

        let mut external = editable_provider_settings();
        external["providers"]["openai"]["request_overrides"] = json!({
            "common": {
                "headers": {
                    "Authorization": {"type": "env_ref", "name": "PROVIDER_AUTH"}
                },
                "body_patch": [{
                    "path": "auth.token",
                    "value": {"type": "generated", "generator": "uuid"}
                }, {
                    "path": "legacy.api_key",
                    "op": "remove"
                }]
            }
        });
        assert!(serde_json::from_value::<PutProviderSettingsRequest>(json!({
            "expected_revision": 0,
            "data": external
        }))
        .is_ok());
    }

    #[actix_web::test]
    async fn ordinary_typed_section_put_is_single_section_cas_and_keeps_refs_server_owned() {
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let app = test::init_service(
            App::new().app_data(state.clone()).service(
                web::resource("/sections/{section}")
                    .route(web::get().to(get_typed_section))
                    .route(web::put().to(put_typed_section)),
            ),
        )
        .await;

        let initial: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get().uri("/sections/core").to_request(),
        )
        .await;
        let revision = initial["revision"].as_u64().unwrap();
        let mut data = initial["data"].clone();
        data["headless_auth"] = json!(true);
        let updated = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/sections/core")
                .set_json(json!({"expected_revision": revision, "data": data}))
                .to_request(),
        )
        .await;
        assert!(updated.status().is_success());
        let updated: Value = test::read_body_json(updated).await;
        assert_eq!(updated["revision"], revision + 1);
        assert_eq!(updated["data"]["headless_auth"], true);
        assert!(state.config.read().await.headless_auth);

        let stale = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/sections/core")
                .set_json(json!({
                    "expected_revision": revision,
                    "data": updated["data"].clone()
                }))
                .to_request(),
        )
        .await;
        assert_eq!(stale.status(), actix_web::http::StatusCode::CONFLICT);

        let mut forged = updated["data"].clone();
        forged["proxy_auth_credential_ref"] = json!("proxy.default.auth");
        let rejected = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/sections/core")
                .set_json(json!({
                    "expected_revision": revision + 1,
                    "data": forged
                }))
                .to_request(),
        )
        .await;
        assert_eq!(rejected.status(), actix_web::http::StatusCode::BAD_REQUEST);
        assert!(state
            .config
            .read()
            .await
            .proxy_auth_credential_ref
            .is_none());
    }

    #[actix_web::test]
    async fn ordinary_section_reset_uses_backend_default_and_section_cas() {
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        let app = test::init_service(
            App::new()
                .app_data(state)
                .route("/sections/{section}", web::put().to(put_typed_section))
                .route(
                    "/sections/{section}/reset",
                    web::post().to(reset_typed_section),
                ),
        )
        .await;

        let updated = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/sections/model-limits")
                .set_json(json!({
                    "expected_revision": 0,
                    "data": [{"model": "custom", "max_tokens": 42}]
                }))
                .to_request(),
        )
        .await;
        assert!(updated.status().is_success());

        let reset = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/sections/model-limits/reset")
                .set_json(json!({"expected_revision": 1}))
                .to_request(),
        )
        .await;
        assert!(reset.status().is_success());
        let reset: Value = test::read_body_json(reset).await;
        assert_eq!(reset["revision"], 2);
        assert_eq!(reset["data"], json!([]));

        let stale = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/sections/model-limits/reset")
                .set_json(json!({"expected_revision": 1}))
                .to_request(),
        )
        .await;
        assert_eq!(stale.status(), actix_web::http::StatusCode::CONFLICT);

        let repeated = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/sections/model-limits/reset")
                .set_json(json!({"expected_revision": 2}))
                .to_request(),
        )
        .await;
        assert!(repeated.status().is_success());
        let repeated: Value = test::read_body_json(repeated).await;
        assert_eq!(repeated["revision"], 3);
        assert_eq!(repeated["data"], json!([]));
    }

    #[actix_web::test]
    async fn notification_reset_atomically_clears_owned_secret_and_rejects_stale_cas() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x61; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        state
            .update_notification_credentials(
                0,
                BTreeSet::from(["ntfy".to_string()]),
                false,
                |config| {
                    config.notifications.ntfy.enabled = true;
                    config.notifications.ntfy.topic = "alerts".to_string();
                    config.notifications.ntfy.token = Some("never-return-reset-secret".to_string());
                    Ok(())
                },
            )
            .await
            .unwrap();
        let reference = bamboo_config::credential_ref("notification", "ntfy", "token").unwrap();
        assert!(
            state
                .credential_store
                .status(&reference)
                .unwrap()
                .configured
        );
        let unrelated = bamboo_config::CredentialRef::parse("custom.unrelated.token").unwrap();
        state
            .credential_store
            .replace(
                unrelated.clone(),
                "preserved-unrelated-secret",
                bamboo_config::CredentialSource::User,
                1,
            )
            .unwrap();

        let app = test::init_service(App::new().app_data(state.clone()).route(
            "/sections/{section}/reset",
            web::post().to(reset_typed_section),
        ))
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/sections/notifications/reset")
                .set_json(json!({"expected_revision": 1}))
                .to_request(),
        )
        .await;
        let status = response.status();
        let body = String::from_utf8(test::read_body(response).await.to_vec()).unwrap();
        assert!(status.is_success(), "unexpected response {status}: {body}");
        assert!(!body.contains("never-return-reset-secret"));
        assert!(!body.contains("preserved-unrelated-secret"));
        assert!(body.contains("\"revision\":2"));
        assert!(
            !state
                .credential_store
                .status(&reference)
                .unwrap()
                .configured
        );
        assert!(
            state
                .credential_store
                .status(&unrelated)
                .unwrap()
                .configured
        );
        assert_eq!(state.config.read().await.notifications, Default::default());

        let stale = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/sections/notifications/reset")
                .set_json(json!({"expected_revision": 1}))
                .to_request(),
        )
        .await;
        assert_eq!(stale.status(), actix_web::http::StatusCode::CONFLICT);
    }

    #[actix_web::test]
    async fn core_reset_clears_proxy_credential_with_core_metadata() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x64; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        state.config.write().await.http_proxy = "http://proxy.example:8080".to_string();
        state
            .update_proxy_auth_credential(
                Some(bamboo_config::ProxyAuth {
                    username: "proxy-user".to_string(),
                    password: "proxy-reset-secret".to_string(),
                }),
                0,
                Default::default(),
            )
            .await
            .unwrap();
        let reference = bamboo_config::credential_ref("proxy", "default", "auth").unwrap();
        assert!(
            state
                .credential_store
                .status(&reference)
                .unwrap()
                .configured
        );

        let app = test::init_service(App::new().app_data(state.clone()).route(
            "/sections/{section}/reset",
            web::post().to(reset_typed_section),
        ))
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/sections/core/reset")
                .set_json(json!({"expected_revision": 1}))
                .to_request(),
        )
        .await;
        let status = response.status();
        let body = String::from_utf8(test::read_body(response).await.to_vec()).unwrap();
        assert!(status.is_success(), "unexpected response {status}: {body}");
        assert!(!body.contains("proxy-reset-secret"));
        assert!(state.config.read().await.http_proxy.is_empty());
        assert!(state.config.read().await.proxy_auth.is_none());
        assert!(
            !state
                .credential_store
                .status(&reference)
                .unwrap()
                .configured
        );
    }

    #[actix_web::test]
    async fn access_control_reset_restores_none_and_clears_verifier() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x65; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        state
            .update_access_control_credentials(0, true, BTreeSet::new(), |config| {
                config.access_control = Some(bamboo_config::AccessControlConfig {
                    password_enabled: true,
                    password_hash: Some("a".repeat(64)),
                    password_salt: Some("b".repeat(32)),
                    password_configured: true,
                    ..Default::default()
                });
                Ok(())
            })
            .await
            .unwrap();
        let reference = bamboo_config::config_crypto::access_password_credential_ref().unwrap();
        assert!(
            state
                .credential_store
                .status(&reference)
                .unwrap()
                .configured
        );

        let app = test::init_service(App::new().app_data(state.clone()).route(
            "/sections/{section}/reset",
            web::post().to(reset_typed_section),
        ))
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/sections/access-control/reset")
                .set_json(json!({"expected_revision": 1}))
                .to_request(),
        )
        .await;
        let status = response.status();
        let body = String::from_utf8(test::read_body(response).await.to_vec()).unwrap();
        assert!(status.is_success(), "unexpected response {status}: {body}");
        assert!(state.config.read().await.access_control.is_none());
        assert!(
            !state
                .credential_store
                .status(&reference)
                .unwrap()
                .configured
        );
    }

    #[actix_web::test]
    async fn provider_reset_clears_owned_credential_and_uses_provider_revision() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x62; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        state
            .update_config_with_provider_credentials(
                |config| {
                    config.provider = "openai".to_string();
                    config.providers_mut().openai = Some(OpenAIConfig {
                        api_key: "provider-reset-secret".to_string(),
                        model: Some("custom-model".to_string()),
                        ..Default::default()
                    });
                    Ok(())
                },
                BTreeSet::from(["openai".to_string()]),
                BTreeSet::new(),
                Default::default(),
            )
            .await
            .unwrap();
        let reference = bamboo_config::credential_ref("provider", "openai", "api_key").unwrap();
        assert!(
            state
                .credential_store
                .status(&reference)
                .unwrap()
                .configured
        );
        let revision = state
            .config_facade
            .as_ref()
            .unwrap()
            .registry()
            .providers
            .snapshot()
            .revision;

        let app = test::init_service(App::new().app_data(state.clone()).route(
            "/sections/{section}/reset",
            web::post().to(reset_typed_section),
        ))
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/sections/providers/reset")
                .set_json(json!({"expected_revision": revision}))
                .to_request(),
        )
        .await;
        let status = response.status();
        let body = String::from_utf8(test::read_body(response).await.to_vec()).unwrap();
        assert!(status.is_success(), "unexpected response {status}: {body}");
        assert!(!body.contains("provider-reset-secret"));
        assert!(
            !state
                .credential_store
                .status(&reference)
                .unwrap()
                .configured
        );
        assert!(state.config.read().await.providers().openai.is_none());

        let stale = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/sections/providers/reset")
                .set_json(json!({"expected_revision": revision}))
                .to_request(),
        )
        .await;
        assert_eq!(stale.status(), actix_web::http::StatusCode::CONFLICT);
    }

    #[actix_web::test]
    async fn mcp_reset_clears_owned_credentials_at_runtime_commit_boundary() {
        let _key = bamboo_config::encryption::set_test_encryption_key([0x66; 32]);
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let health = state
                    .mcp_config_live_health
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone();
                if health.status == SectionStatus::Healthy && health.revision == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let reference = bamboo_config::credential_ref("mcp", "reset-server", "env_TOKEN").unwrap();
        state
            .credential_store
            .replace(
                reference.clone(),
                "mcp-reset-secret",
                bamboo_config::CredentialSource::User,
                0,
            )
            .unwrap();
        state
            .put_mcp_section(
                0,
                McpConfig {
                    version: 1,
                    servers: vec![server(
                        "reset-server",
                        TransportConfig::Stdio(StdioConfig {
                            command: "disabled-command".to_string(),
                            args: Vec::new(),
                            cwd: None,
                            env: HashMap::from([(
                                "TOKEN".to_string(),
                                "mcp-reset-secret".to_string(),
                            )]),
                            env_encrypted: HashMap::new(),
                            env_credential_refs: HashMap::from([(
                                "TOKEN".to_string(),
                                reference.as_str().to_string(),
                            )]),
                            startup_timeout_ms: 500,
                        }),
                    )],
                },
            )
            .await
            .unwrap();

        let app = test::init_service(App::new().app_data(state.clone()).route(
            "/sections/{section}/reset",
            web::post().to(reset_typed_section),
        ))
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/sections/mcp/reset")
                .set_json(json!({"expected_revision": 1}))
                .to_request(),
        )
        .await;
        let status = response.status();
        let body = String::from_utf8(test::read_body(response).await.to_vec()).unwrap();
        assert!(status.is_success(), "unexpected response {status}: {body}");
        assert!(!body.contains("mcp-reset-secret"));
        assert!(state.config.read().await.mcp.servers.is_empty());
        assert!(
            !state
                .credential_store
                .status(&reference)
                .unwrap()
                .configured
        );

        let stale = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/sections/mcp/reset")
                .set_json(json!({"expected_revision": 1}))
                .to_request(),
        )
        .await;
        assert_eq!(stale.status(), actix_web::http::StatusCode::CONFLICT);
    }
}
