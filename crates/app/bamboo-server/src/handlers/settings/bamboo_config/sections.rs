use actix_web::{web, HttpResponse};
use bamboo_config::{patch::is_masked_api_key, ConfigStoreError, ProviderConfigs, SectionEnvelope};
use bamboo_mcp::McpConfig;
use serde::{de::Error as _, Deserialize, Deserializer};
use serde_json::{json, Map, Value};

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
                if !provider.extra.is_empty() || provider.request_overrides.is_some() {
                    return Err(
                        "unknown provider fields and request overrides are not accepted by the typed endpoint"
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
        if !provider.extra.is_empty() || provider.request_overrides.is_some() {
            return Err(
                "unknown provider fields and request overrides are not accepted by the typed endpoint"
                    .to_string(),
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
    use std::collections::{BTreeMap, HashMap};
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
    async fn provider_put_upgrades_legacy_cas_preserves_secret_and_redacts_response() {
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
                    "expected_revision": 0,
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
        assert_eq!(body["revision"], 1);
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
        assert_eq!(disk["revision"], 1);
        assert!(disk["data"]["openai"]["credential_ref"].is_string());
        assert!(disk["data"]["openai"].get("api_key_encrypted").is_none());
        let first = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let event = feed.recv().await.unwrap();
                if matches!(
                    &event.event,
                    bamboo_agent_core::AgentEvent::ConfigChanged { section, revision }
                        | bamboo_agent_core::AgentEvent::ConfigRecovered { section, revision }
                        if section == "providers" && *revision == 1
                ) {
                    break;
                }
            }
        })
        .await;
        assert!(first.is_ok());
        assert!(
            tokio::time::timeout(Duration::from_millis(500), feed.recv())
                .await
                .is_err(),
            "the watcher echo must not publish a duplicate event"
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
                if health.status == SectionStatus::Healthy && health.revision == 2 {
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
        assert_eq!(normalized["revision"], 2);
        let stale_after_external = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/providers")
                .set_json(json!({
                    "expected_revision": 1,
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
    async fn provider_put_rejects_invalid_credential_refs_without_mutating_lkg() {
        let dir = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(dir.path().to_path_buf()).await.unwrap());
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
        assert!(!dir.path().join("providers.json").exists());
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
        assert!(
            tokio::time::timeout(Duration::from_millis(500), feed.recv())
                .await
                .is_err(),
            "the MCP watcher echo must not publish a duplicate event"
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
}
