use actix_web::{web, HttpResponse};
use bamboo_config::SectionEnvelope;
use serde_json::{json, Map, Value};

use crate::{app_state::AppState, error::AppError};

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
                "api_key_configured": provider_key_configured(&provider.api_key, &provider.api_key_encrypted),
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
                "api_key_configured": provider_key_configured(&provider.api_key, &provider.api_key_encrypted),
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
                "api_key_configured": provider_key_configured(&provider.api_key, &provider.api_key_encrypted),
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
                "api_key_configured": provider_key_configured(&provider.api_key, &provider.api_key_encrypted),
                "base_url": safe_url_diagnostic(provider.base_url.as_deref()),
                "target_provider": provider.target_provider,
                "reasoning_effort": provider.reasoning_effort,
            }),
        );
    }

    Value::Object(result)
}

fn provider_key_configured(plaintext: &str, ciphertext: &Option<String>) -> bool {
    !plaintext.trim().is_empty() || ciphertext.is_some()
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
}
