//! Encryption, decryption, and hydration methods for [`Config`].
//!
//! These methods handle the in-memory hydration of encrypted credentials
//! (API keys, proxy auth, MCP secrets, env vars) and their re-encryption
//! before persisting to disk.

use anyhow::{Context, Result};

use super::{Config, ProxyAuth};

fn hydrate_header_credentials(
    store: &crate::CredentialStore,
    headers: &mut [bamboo_domain::mcp_config::HeaderConfig],
) -> crate::ConfigStoreResult<()> {
    for header in headers {
        if !header.value.is_empty() {
            continue;
        }
        let Some(raw_reference) = header.credential_ref.as_ref() else {
            continue;
        };
        let reference = crate::CredentialRef::parse(raw_reference.clone())?;
        header.value = store
            .resolve(&reference)?
            .ok_or_else(|| {
                crate::ConfigStoreError::Validation(
                    "referenced MCP credential is unavailable".to_string(),
                )
            })?
            .expose()
            .to_string();
    }
    Ok(())
}

impl Config {
    // ── Proxy auth ─────────────────────────────────────────────────────

    /// Populate `proxy_auth` (plaintext) from `proxy_auth_encrypted` if present.
    ///
    /// Many parts of the code rely on `proxy_auth` being hydrated in-memory so
    /// we can re-encrypt deterministically on save without ever persisting
    /// plaintext credentials.
    pub fn hydrate_proxy_auth_from_encrypted(&mut self) {
        if self.proxy_auth.is_some() {
            return;
        }

        // Backward compatibility:
        // Older Bodhi/Tauri builds persisted proxy auth as per-scheme encrypted fields:
        // `http_proxy_auth_encrypted` / `https_proxy_auth_encrypted`.
        //
        // Those live under `extra` (flatten) in the unified config. Seed the new
        // `proxy_auth_encrypted` field so the rest of the code can stay uniform.
        if self
            .proxy_auth_encrypted
            .as_deref()
            .map(|s| s.trim().is_empty())
            .unwrap_or(true)
        {
            let legacy = self
                .extra
                .get("https_proxy_auth_encrypted")
                .and_then(|v| v.as_str())
                .or_else(|| {
                    self.extra
                        .get("http_proxy_auth_encrypted")
                        .and_then(|v| v.as_str())
                })
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string());

            if let Some(legacy) = legacy {
                self.proxy_auth_encrypted = Some(legacy);
            }
        }

        let Some(encrypted) = self.proxy_auth_encrypted.as_deref() else {
            return;
        };

        match crate::encryption::decrypt(encrypted) {
            Ok(decrypted) => match serde_json::from_str::<ProxyAuth>(&decrypted) {
                Ok(auth) => {
                    self.proxy_auth = Some(auth);
                    // Once hydrated successfully, drop legacy keys so a future save writes only
                    // the canonical `proxy_auth_encrypted` field.
                    self.extra.remove("http_proxy_auth_encrypted");
                    self.extra.remove("https_proxy_auth_encrypted");
                }
                Err(e) => tracing::warn!("Failed to parse decrypted proxy auth JSON: {}", e),
            },
            Err(e) => tracing::warn!("Failed to decrypt proxy auth: {}", e),
        }
    }

    /// Refresh `proxy_auth_encrypted` from the current in-memory `proxy_auth`.
    ///
    /// This is used both when persisting the config to disk and when generating
    /// API responses that should never include plaintext proxy credentials.
    pub fn refresh_proxy_auth_encrypted(&mut self) -> Result<()> {
        // Keep on-disk representation fully derived from the in-memory plaintext:
        // - Some(auth)  => always (re-)encrypt and store `proxy_auth_encrypted`
        // - None        => remove `proxy_auth_encrypted`
        let Some(auth) = self.proxy_auth.as_ref() else {
            self.proxy_auth_encrypted = None;
            return Ok(());
        };

        let auth_str = serde_json::to_string(auth).context("Failed to serialize proxy auth")?;
        let encrypted =
            crate::encryption::encrypt(&auth_str).context("Failed to encrypt proxy auth")?;
        self.proxy_auth_encrypted = Some(encrypted);
        Ok(())
    }

    // ── Provider API keys ──────────────────────────────────────────────

    pub fn hydrate_provider_api_keys_from_encrypted(&mut self) {
        if let Some(openai) = self.providers.openai.as_mut() {
            if openai.api_key.trim().is_empty() {
                if let Some(encrypted) = openai.api_key_encrypted.as_deref() {
                    match crate::encryption::decrypt(encrypted) {
                        Ok(value) => openai.api_key = value,
                        Err(e) => tracing::warn!("Failed to decrypt OpenAI api_key: {}", e),
                    }
                }
            }
        }

        if let Some(anthropic) = self.providers.anthropic.as_mut() {
            if anthropic.api_key.trim().is_empty() {
                if let Some(encrypted) = anthropic.api_key_encrypted.as_deref() {
                    match crate::encryption::decrypt(encrypted) {
                        Ok(value) => anthropic.api_key = value,
                        Err(e) => tracing::warn!("Failed to decrypt Anthropic api_key: {}", e),
                    }
                }
            }
        }

        if let Some(gemini) = self.providers.gemini.as_mut() {
            if gemini.api_key.trim().is_empty() {
                if let Some(encrypted) = gemini.api_key_encrypted.as_deref() {
                    match crate::encryption::decrypt(encrypted) {
                        Ok(value) => gemini.api_key = value,
                        Err(e) => tracing::warn!("Failed to decrypt Gemini api_key: {}", e),
                    }
                }
            }
        }

        if let Some(bodhi) = self.providers.bodhi.as_mut() {
            if bodhi.api_key.trim().is_empty() {
                if let Some(encrypted) = bodhi.api_key_encrypted.as_deref() {
                    match crate::encryption::decrypt(encrypted) {
                        Ok(value) => bodhi.api_key = value,
                        Err(e) => tracing::warn!("Failed to decrypt Bodhi api_key: {}", e),
                    }
                }
            }
        }
    }

    /// Resolve built-in provider and provider-instance credential references
    /// after legacy ciphertext hydration. Existing in-memory values (notably
    /// environment overrides) retain precedence.
    pub fn hydrate_provider_credentials_from_store(
        &mut self,
        data_dir: &std::path::Path,
    ) -> crate::ConfigStoreResult<()> {
        let store = crate::CredentialStore::open(data_dir);
        macro_rules! hydrate {
            ($provider:expr) => {
                if let Some(provider) = $provider {
                    if provider.api_key.trim().is_empty() {
                        if let Some(reference) = provider.credential_ref.as_ref() {
                            provider.api_key = store
                                .resolve(reference)?
                                .ok_or_else(|| {
                                    crate::ConfigStoreError::Validation(
                                        "referenced provider credential is unavailable".to_string(),
                                    )
                                })?
                                .expose()
                                .to_string();
                        }
                    }
                }
            };
        }
        hydrate!(self.providers.openai.as_mut());
        hydrate!(self.providers.anthropic.as_mut());
        hydrate!(self.providers.gemini.as_mut());
        hydrate!(self.providers.bodhi.as_mut());
        for instance in self.provider_instances.values_mut() {
            if instance.api_key.trim().is_empty() {
                if let Some(reference) = instance.credential_ref.as_ref() {
                    instance.api_key = store
                        .resolve(reference)?
                        .ok_or_else(|| {
                            crate::ConfigStoreError::Validation(
                                "referenced provider credential is unavailable".to_string(),
                            )
                        })?
                        .expose()
                        .to_string();
                }
            }
        }
        Ok(())
    }

    pub fn refresh_provider_api_keys_encrypted(&mut self) -> Result<()> {
        // Env-injected keys (`api_key_from_env`) are runtime-only: leave
        // `api_key_encrypted` untouched so they're never baked into config.json
        // on save (which would otherwise persist the secret even after the env
        // var is removed). (#253)
        if let Some(openai) = self.providers.openai.as_mut() {
            if openai.credential_ref.is_some() {
                openai.api_key_encrypted = None;
            } else if !openai.api_key_from_env {
                let api_key = openai.api_key.trim();
                // Only (re)encrypt when we actually hold a plaintext key. When the
                // plaintext is empty because the stored ciphertext failed to
                // decrypt at hydration (config.json moved across machines, a
                // machine-id change, the ephemeral fallback key), DON'T null the
                // ciphertext — that would permanently drop a working key the user
                // never touched on the next unrelated save. #268.
                if !api_key.is_empty() {
                    openai.api_key_encrypted = Some(
                        crate::encryption::encrypt(api_key)
                            .context("Failed to encrypt OpenAI api_key")?,
                    );
                }
            }
        }

        if let Some(anthropic) = self.providers.anthropic.as_mut() {
            if anthropic.credential_ref.is_some() {
                anthropic.api_key_encrypted = None;
            } else if !anthropic.api_key_from_env {
                let api_key = anthropic.api_key.trim();
                // Empty plaintext → preserve existing ciphertext (see OpenAI above). #268.
                if !api_key.is_empty() {
                    anthropic.api_key_encrypted = Some(
                        crate::encryption::encrypt(api_key)
                            .context("Failed to encrypt Anthropic api_key")?,
                    );
                }
            }
        }

        if let Some(gemini) = self.providers.gemini.as_mut() {
            if gemini.credential_ref.is_some() {
                gemini.api_key_encrypted = None;
            } else if !gemini.api_key_from_env {
                let api_key = gemini.api_key.trim();
                // Empty plaintext → preserve existing ciphertext (see OpenAI above). #268.
                if !api_key.is_empty() {
                    gemini.api_key_encrypted = Some(
                        crate::encryption::encrypt(api_key)
                            .context("Failed to encrypt Gemini api_key")?,
                    );
                }
            }
        }

        if let Some(bodhi) = self.providers.bodhi.as_mut() {
            if bodhi.credential_ref.is_some() {
                bodhi.api_key_encrypted = None;
                return Ok(());
            }
            let api_key = bodhi.api_key.trim();
            // Empty plaintext → preserve existing ciphertext (see OpenAI above). #268.
            if !api_key.is_empty() {
                bodhi.api_key_encrypted = Some(
                    crate::encryption::encrypt(api_key)
                        .context("Failed to encrypt Bodhi api_key")?,
                );
            }
        }

        Ok(())
    }

    // ── Provider instance API keys ─────────────────────────────────────

    /// Hydrate plaintext `api_key` fields on provider instances from their
    /// encrypted counterparts.
    pub fn hydrate_provider_instance_api_keys_from_encrypted(&mut self) {
        for (id, instance) in self.provider_instances.iter_mut() {
            if instance.api_key.trim().is_empty() {
                if let Some(encrypted) = instance.api_key_encrypted.as_deref() {
                    match crate::encryption::decrypt(encrypted) {
                        Ok(value) => instance.api_key = value,
                        Err(e) => {
                            tracing::warn!(instance_id = id, "Failed to decrypt api_key: {}", e)
                        }
                    }
                }
            }
        }
    }

    /// Re-encrypt all provider instance API keys and write back to
    /// `api_key_encrypted`. Used before persisting to disk.
    pub fn refresh_provider_instance_api_keys_encrypted(&mut self) -> Result<()> {
        for (id, instance) in self.provider_instances.iter_mut() {
            let api_key = instance.api_key.trim();
            // Empty plaintext → preserve existing ciphertext (see
            // refresh_provider_api_keys_encrypted). #268.
            if !api_key.is_empty() {
                instance.api_key_encrypted = Some(crate::encryption::encrypt(api_key).context(
                    format!("Failed to encrypt api_key for provider instance '{}'", id),
                )?);
            }
        }
        Ok(())
    }

    // ── MCP secrets ────────────────────────────────────────────────────

    pub fn hydrate_mcp_secrets_from_encrypted(&mut self) {
        for server in self.mcp.servers.iter_mut() {
            match &mut server.transport {
                bamboo_domain::mcp_config::TransportConfig::Stdio(stdio) => {
                    if stdio.env_encrypted.is_empty() {
                        continue;
                    }

                    // Avoid borrow-checker gymnastics by iterating a cloned map.
                    for (key, encrypted) in stdio.env_encrypted.clone() {
                        let should_hydrate = stdio
                            .env
                            .get(&key)
                            .map(|v| v.trim().is_empty())
                            .unwrap_or(true);
                        if !should_hydrate {
                            continue;
                        }

                        match crate::encryption::decrypt(&encrypted) {
                            Ok(value) => {
                                stdio.env.insert(key, value);
                            }
                            Err(e) => tracing::warn!("Failed to decrypt MCP stdio env var: {}", e),
                        }
                    }
                }
                bamboo_domain::mcp_config::TransportConfig::Sse(sse) => {
                    for header in sse.headers.iter_mut() {
                        if !header.value.trim().is_empty() {
                            continue;
                        }
                        let Some(encrypted) = header.value_encrypted.as_deref() else {
                            continue;
                        };
                        match crate::encryption::decrypt(encrypted) {
                            Ok(value) => header.value = value,
                            Err(e) => {
                                tracing::warn!("Failed to decrypt MCP SSE header value: {}", e)
                            }
                        }
                    }
                }
                bamboo_domain::mcp_config::TransportConfig::StreamableHttp(sh) => {
                    for header in sh.headers.iter_mut() {
                        if !header.value.trim().is_empty() {
                            continue;
                        }
                        let Some(encrypted) = header.value_encrypted.as_deref() else {
                            continue;
                        };
                        match crate::encryption::decrypt(encrypted) {
                            Ok(value) => header.value = value,
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to decrypt MCP StreamableHTTP header value: {}",
                                    e
                                )
                            }
                        }
                    }
                }
            }
        }
    }

    /// Resolve MCP env/header references without exposing credential values to
    /// serialization or debug output.
    pub fn hydrate_mcp_credentials_from_store(
        &mut self,
        data_dir: &std::path::Path,
    ) -> crate::ConfigStoreResult<()> {
        let store = crate::CredentialStore::open(data_dir);
        for server in &mut self.mcp.servers {
            match &mut server.transport {
                bamboo_domain::mcp_config::TransportConfig::Stdio(stdio) => {
                    for (name, raw_reference) in &stdio.env_credential_refs {
                        if stdio.env.get(name).is_some_and(|value| !value.is_empty()) {
                            continue;
                        }
                        let reference = crate::CredentialRef::parse(raw_reference.clone())?;
                        let secret = store.resolve(&reference)?.ok_or_else(|| {
                            crate::ConfigStoreError::Validation(
                                "referenced MCP credential is unavailable".to_string(),
                            )
                        })?;
                        stdio.env.insert(name.clone(), secret.expose().to_string());
                    }
                }
                bamboo_domain::mcp_config::TransportConfig::Sse(config) => {
                    hydrate_header_credentials(&store, &mut config.headers)?;
                }
                bamboo_domain::mcp_config::TransportConfig::StreamableHttp(config) => {
                    hydrate_header_credentials(&store, &mut config.headers)?;
                }
            }
        }
        Ok(())
    }

    pub fn refresh_mcp_secrets_encrypted(&mut self) -> Result<()> {
        for server in self.mcp.servers.iter_mut() {
            match &mut server.transport {
                bamboo_domain::mcp_config::TransportConfig::Stdio(stdio) => {
                    stdio.env_encrypted.clear();
                    for (key, value) in &stdio.env {
                        let encrypted = crate::encryption::encrypt(value).with_context(|| {
                            format!("Failed to encrypt MCP stdio env var '{key}'")
                        })?;
                        stdio.env_encrypted.insert(key.clone(), encrypted);
                    }
                }
                bamboo_domain::mcp_config::TransportConfig::Sse(sse) => {
                    for header in sse.headers.iter_mut() {
                        let configured = !header.value.trim().is_empty();
                        header.value_encrypted = if !configured {
                            None
                        } else {
                            Some(crate::encryption::encrypt(&header.value).with_context(|| {
                                format!("Failed to encrypt MCP SSE header '{}'", header.name)
                            })?)
                        };
                    }
                }
                bamboo_domain::mcp_config::TransportConfig::StreamableHttp(sh) => {
                    for header in sh.headers.iter_mut() {
                        let configured = !header.value.trim().is_empty();
                        header.value_encrypted = if !configured {
                            None
                        } else {
                            Some(crate::encryption::encrypt(&header.value).with_context(|| {
                                format!(
                                    "Failed to encrypt MCP StreamableHTTP header '{}'",
                                    header.name
                                )
                            })?)
                        };
                    }
                }
            }
        }

        Ok(())
    }

    /// Project credential-ref-backed MCP runtime values to the root disk DTO.
    /// Public serialization remains compatibility-oriented, but config.json
    /// must never duplicate either hydrated plaintext or legacy ciphertext
    /// once the isolated credential store is authoritative.
    pub fn sanitize_mcp_credential_refs_for_disk(&mut self) {
        for server in &mut self.mcp.servers {
            match &mut server.transport {
                bamboo_domain::mcp_config::TransportConfig::Stdio(stdio) => {
                    for name in stdio
                        .env_credential_refs
                        .keys()
                        .cloned()
                        .collect::<Vec<_>>()
                    {
                        stdio.env.remove(&name);
                        stdio.env_encrypted.remove(&name);
                    }
                }
                bamboo_domain::mcp_config::TransportConfig::Sse(config) => {
                    for header in &mut config.headers {
                        if header.credential_ref.is_some() {
                            header.value.clear();
                            header.value_encrypted = None;
                        }
                    }
                }
                bamboo_domain::mcp_config::TransportConfig::StreamableHttp(config) => {
                    for header in &mut config.headers {
                        if header.credential_ref.is_some() {
                            header.value.clear();
                            header.value_encrypted = None;
                        }
                    }
                }
            }
        }
    }

    // ── Env vars encryption ────────────────────────────────────────────

    /// Decrypt secret env vars into in-memory plaintext after loading config.
    pub fn hydrate_env_vars_from_encrypted(&mut self) {
        for entry in &mut self.env_vars {
            if !entry.secret {
                continue;
            }
            if !entry.value.trim().is_empty() {
                // Already has plaintext (e.g. in-memory update).
                continue;
            }
            let Some(encrypted) = &entry.value_encrypted else {
                continue;
            };
            match crate::encryption::decrypt(encrypted) {
                Ok(value) => entry.value = value,
                Err(e) => tracing::warn!("Failed to decrypt env var '{}': {}", entry.name, e),
            }
        }
    }

    /// Re-encrypt secret env vars before persisting to disk.
    pub fn refresh_env_vars_encrypted(&mut self) -> Result<()> {
        for entry in &mut self.env_vars {
            if entry.secret && !entry.value.trim().is_empty() {
                entry.value_encrypted = Some(
                    crate::encryption::encrypt(&entry.value)
                        .with_context(|| format!("Failed to encrypt env var '{}'", entry.name))?,
                );
            } else if !entry.secret {
                entry.value_encrypted = None;
            }
        }
        Ok(())
    }

    /// Clear plaintext values for secrets before serialization to disk.
    pub fn sanitize_env_vars_for_disk(&mut self) {
        for entry in &mut self.env_vars {
            if entry.secret {
                entry.value = String::new();
            }
        }
    }

    // ── Broker client token encryption ─────────────────────────────────

    /// Decrypt the broker token into in-memory plaintext after loading config.
    pub fn hydrate_broker_token_from_encrypted(&mut self) {
        let Some(broker) = self.subagents.broker.as_mut() else {
            return;
        };
        if !broker.token.trim().is_empty() {
            return; // already has plaintext
        }
        if let Some(encrypted) = &broker.token_encrypted {
            match crate::encryption::decrypt(encrypted) {
                Ok(value) => broker.token = value,
                Err(e) => tracing::warn!("Failed to decrypt broker token: {}", e),
            }
        }
    }

    /// Re-encrypt the broker token before persisting to disk.
    pub fn refresh_broker_token_encrypted(&mut self) -> Result<()> {
        let Some(broker) = self.subagents.broker.as_mut() else {
            return Ok(());
        };
        if broker.token.trim().is_empty() {
            // Keep any existing ciphertext (a redacted round-trip never re-sends it).
            return Ok(());
        }
        broker.token_encrypted = Some(
            crate::encryption::encrypt(&broker.token).context("Failed to encrypt broker token")?,
        );
        Ok(())
    }

    /// Clear the plaintext broker token before serialization to disk.
    pub fn sanitize_broker_token_for_disk(&mut self) {
        if let Some(broker) = self.subagents.broker.as_mut() {
            broker.token = String::new();
        }
    }

    // ── Notification channel secrets (ntfy token, Bark device key) ─────

    /// Decrypt notification-channel secrets into in-memory plaintext after
    /// loading config. Mirrors [`Config::hydrate_provider_api_keys_from_encrypted`]:
    /// the plaintext fields are `#[serde(skip_serializing)]` (never on disk), so
    /// this is the only way they get populated after a fresh load.
    pub fn hydrate_notifications_from_encrypted(&mut self) {
        let ntfy = &mut self.notifications.ntfy;
        if ntfy
            .token
            .as_deref()
            .map(str::trim)
            .unwrap_or("")
            .is_empty()
        {
            if let Some(encrypted) = ntfy.token_encrypted.as_deref() {
                match crate::encryption::decrypt(encrypted) {
                    Ok(value) => ntfy.token = Some(value),
                    Err(e) => tracing::warn!("Failed to decrypt ntfy token: {}", e),
                }
            }
        }

        let bark = &mut self.notifications.bark;
        if bark
            .device_key
            .as_deref()
            .map(str::trim)
            .unwrap_or("")
            .is_empty()
        {
            if let Some(encrypted) = bark.device_key_encrypted.as_deref() {
                match crate::encryption::decrypt(encrypted) {
                    Ok(value) => bark.device_key = Some(value),
                    Err(e) => tracing::warn!("Failed to decrypt Bark device key: {}", e),
                }
            }
        }
    }

    /// Re-encrypt notification-channel secrets from current in-memory plaintext
    /// before persisting to disk. Mirrors
    /// [`Config::refresh_provider_api_keys_encrypted`]: an empty/absent
    /// plaintext leaves any existing ciphertext intact (a redacted round-trip
    /// where the client never re-sent the secret keeps it).
    pub fn refresh_notifications_encrypted(&mut self) -> Result<()> {
        let ntfy = &mut self.notifications.ntfy;
        let token = ntfy.token.as_deref().unwrap_or("").trim();
        if !token.is_empty() {
            ntfy.token_encrypted =
                Some(crate::encryption::encrypt(token).context("Failed to encrypt ntfy token")?);
        }

        let bark = &mut self.notifications.bark;
        let device_key = bark.device_key.as_deref().unwrap_or("").trim();
        if !device_key.is_empty() {
            bark.device_key_encrypted = Some(
                crate::encryption::encrypt(device_key)
                    .context("Failed to encrypt Bark device key")?,
            );
        }

        Ok(())
    }

    // ── bamboo-connect platform tokens (Telegram bot token, etc.) ───────

    /// Decrypt every configured platform's token (and Feishu `app_secret`)
    /// into in-memory plaintext after loading config. Mirrors
    /// [`Config::hydrate_notifications_from_encrypted`]: both fields are
    /// `#[serde(skip_serializing)]` (never on disk), so this is the only way
    /// they get populated after a fresh load.
    pub fn hydrate_connect_platform_tokens_from_encrypted(&mut self) {
        for platform in &mut self.connect.platforms {
            let has_plaintext = platform
                .token
                .as_deref()
                .map(str::trim)
                .map(|value| !value.is_empty())
                .unwrap_or(false);
            if !has_plaintext {
                if let Some(encrypted) = platform.token_encrypted.as_deref() {
                    match crate::encryption::decrypt(encrypted) {
                        Ok(value) => platform.token = Some(value),
                        Err(e) => tracing::warn!(
                            "Failed to decrypt connect platform '{}' token: {}",
                            platform.platform_type,
                            e
                        ),
                    }
                }
            }

            let has_app_secret_plaintext = platform
                .app_secret
                .as_deref()
                .map(str::trim)
                .map(|value| !value.is_empty())
                .unwrap_or(false);
            if !has_app_secret_plaintext {
                if let Some(encrypted) = platform.app_secret_encrypted.as_deref() {
                    match crate::encryption::decrypt(encrypted) {
                        Ok(value) => platform.app_secret = Some(value),
                        Err(e) => tracing::warn!(
                            "Failed to decrypt connect platform '{}' app_secret: {}",
                            platform.platform_type,
                            e
                        ),
                    }
                }
            }
        }
    }

    /// Re-encrypt every configured platform's token (and Feishu `app_secret`)
    /// from current in-memory plaintext before persisting to disk. Mirrors
    /// [`Config::refresh_notifications_encrypted`]: an empty/absent plaintext
    /// leaves any existing ciphertext intact (a redacted round-trip where the
    /// client never re-sent the secret keeps it).
    pub fn refresh_connect_platform_tokens_encrypted(&mut self) -> Result<()> {
        for platform in &mut self.connect.platforms {
            let token = platform.token.as_deref().unwrap_or("").trim();
            if !token.is_empty() {
                platform.token_encrypted =
                    Some(crate::encryption::encrypt(token).with_context(|| {
                        format!(
                            "Failed to encrypt connect platform '{}' token",
                            platform.platform_type
                        )
                    })?);
            }

            let app_secret = platform.app_secret.as_deref().unwrap_or("").trim();
            if !app_secret.is_empty() {
                platform.app_secret_encrypted =
                    Some(crate::encryption::encrypt(app_secret).with_context(|| {
                        format!(
                            "Failed to encrypt connect platform '{}' app_secret",
                            platform.platform_type
                        )
                    })?);
            }
        }
        Ok(())
    }

    /// Restore env-sourced provider `api_key`s that a serde round-trip dropped.
    ///
    /// `api_key` is `#[serde(skip_serializing)]`, so serializing `previous` and
    /// deserializing it back — as the settings-PATCH merge in
    /// `config_manager::build_merged_config` does — loses every provider's
    /// plaintext key. `hydrate_provider_api_keys_from_encrypted` then restores
    /// only keys that have a persisted ciphertext, which an env-injected key
    /// never has (that's the #253 design). Without this, a PATCH to ANY provider
    /// setting silently blanks the live env-sourced key of every OTHER provider
    /// until the process restarts.
    ///
    /// Copies the key back from `previous` for each provider still flagged
    /// env-sourced there whose key wasn't explicitly re-set by the patch (i.e. is
    /// empty after the round-trip), so an explicit `api_key` in the patch still
    /// wins. #373.
    pub fn preserve_env_sourced_provider_keys(&mut self, previous: &Config) {
        macro_rules! restore_env_key {
            ($field:ident) => {
                if let (Some(current), Some(prev)) = (
                    self.providers.$field.as_mut(),
                    previous.providers.$field.as_ref(),
                ) {
                    if prev.api_key_from_env && current.api_key.trim().is_empty() {
                        current.api_key = prev.api_key.clone();
                        current.api_key_from_env = true;
                    }
                }
            };
        }
        restore_env_key!(openai);
        restore_env_key!(anthropic);
        restore_env_key!(gemini);
    }

    /// Re-encrypt every secret domain's `*_encrypted` field from current
    /// in-memory plaintext, without the disk-only sanitization steps.
    ///
    /// `Config::save_to_dir` runs these refreshes on a save-time clone, so the
    /// live in-memory config never sees the resulting ciphertext: a provider
    /// instance created over HTTP keeps `api_key_encrypted: None` in memory for
    /// the rest of the session. Any code that then serializes the live config
    /// and deserializes it back — the settings-PATCH merge in
    /// `config_manager::build_merged_config` — drops the
    /// `#[serde(skip_serializing)]` plaintext and is left with neither field,
    /// permanently losing the key on the next persist (#516). Call this after
    /// mutating the live config so ciphertext stays in sync with plaintext.
    pub fn refresh_encrypted_secrets(&mut self) -> Result<()> {
        self.refresh_proxy_auth_encrypted()?;
        self.refresh_provider_api_keys_encrypted()?;
        self.refresh_provider_instance_api_keys_encrypted()?;
        self.refresh_env_vars_encrypted()?;
        self.refresh_cluster_fabric_encrypted()?;
        self.refresh_notifications_encrypted()?;
        self.refresh_connect_platform_tokens_encrypted()?;
        Ok(())
    }
}
