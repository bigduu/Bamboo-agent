//! Config patch domain logic.
//!
//! Pure business rules for interpreting, sanitizing, and merging
//! partial config patches. Used by the server's config management endpoints.

use serde_json::{Map, Value};

use crate::Config;

/// Detect whether a string value looks like a masked/placeholder API key.
///
/// Only a value consisting entirely of `*`/`.` characters counts (the redaction
/// placeholder `****...****`, or truncated/retyped variants of it). Substring
/// matching is deliberately avoided: the UI prefills the placeholder into the
/// editable field, so a paste that doesn't fully clear it yields values like
/// `****...****sk-new…` — treating those as "keep existing key" silently
/// discards the user's new token (#430).
pub fn is_masked_api_key(value: &str) -> bool {
    let v = value.trim();
    // Empty string is treated as an explicit "clear" signal (we control all clients).
    !v.is_empty() && v.chars().all(|c| c == '*' || c == '.')
}

/// Decide whether `obj[field]` expresses a secret update **intent** — a
/// genuine new value or an explicit clear — as opposed to "leave alone"
/// (field absent) or "keep existing" (a masked placeholder string).
///
/// Three ways a client can write a secret field, and what each means:
/// - absent → not an intent (existing #521/#516 behavior, unchanged).
/// - a masked placeholder string (`is_masked_api_key`) → not an intent, the
///   caller resolves it back to the live plaintext (`preserve_masked_*`).
/// - anything else — a real new value, an explicit `""`, OR an explicit
///   JSON `null` (#505's RFC-7386-style delete) → **is** an intent. `""`
///   and `null` are equivalent clear signals here: [`deep_merge_json`]
///   removes a `null` field from the merge target the same way it would
///   settle on an empty string for a plain scalar, and treating both as
///   "the caller explicitly asked to clear this" keeps the intent set in
///   sync with what the merge is about to do — a `null` clear must be
///   registered as an intent or `preserve_unpatched_provider_secrets` (which
///   only skips fields the intents mark as touched) would resurrect the
///   value the caller just deleted.
fn is_secret_field_intent(obj: &Map<String, Value>, field: &str) -> bool {
    match obj.get(field) {
        None => false,
        Some(Value::Null) => true,
        Some(value) => match value.as_str() {
            Some(s) => !is_masked_api_key(s),
            // Not a string and not null (shouldn't happen from a
            // well-behaved client) — no coherent intent to extract.
            None => false,
        },
    }
}

/// Extract API-key update intents from a config patch.
///
/// Masked placeholders are ignored — they signal "keep existing key". An
/// explicit `null` is treated the same as an explicit `""` clear (#505) —
/// see [`is_secret_field_intent`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProviderApiKeyIntents {
    pub providers: std::collections::BTreeSet<String>,
    pub provider_instances: std::collections::BTreeSet<String>,
}

pub fn provider_api_key_intents(patch_obj: &Map<String, Value>) -> ProviderApiKeyIntents {
    let mut intents = ProviderApiKeyIntents::default();

    if let Some(root) = patch_obj.get("providers").and_then(|v| v.as_object()) {
        for (provider_name, provider_patch) in root.iter() {
            let Some(obj) = provider_patch.as_object() else {
                continue;
            };
            if is_secret_field_intent(obj, "api_key") {
                intents.providers.insert(provider_name.clone());
            }
        }
    }

    if let Some(root) = patch_obj
        .get("provider_instances")
        .and_then(|v| v.as_object())
    {
        for (instance_id, instance_patch) in root.iter() {
            if instance_patch.is_null() {
                intents.provider_instances.insert(instance_id.clone());
                continue;
            }
            let Some(obj) = instance_patch.as_object() else {
                continue;
            };
            if is_secret_field_intent(obj, "api_key") {
                intents.provider_instances.insert(instance_id.clone());
            }
        }
    }

    intents
}

/// Reload strategy to apply after a config patch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReloadMode {
    None,
    /// Attempt reload, but do not fail the request if reload fails.
    BestEffort,
    /// Reload must succeed; otherwise the request fails.
    Strict,
}

/// Side-effects determined from a config patch.
#[derive(Debug, Clone, Copy)]
pub struct PatchEffects {
    pub reload_provider: ReloadMode,
    pub reconcile_mcp: bool,
}

/// Which config domains are touched by a patch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DomainChanges {
    pub provider: bool,
    pub proxy: bool,
    pub setup: bool,
    pub mcp: bool,
    pub keyword_masking: bool,
    pub hooks: bool,
    pub model_mapping: bool,
}

/// Classify which config domains are affected by a patch.
pub fn domains_for_root_patch(patch_obj: &Map<String, Value>) -> DomainChanges {
    let mut changes = DomainChanges::default();

    for key in patch_obj.keys() {
        match key.as_str() {
            // Provider domain
            "provider"
            | "providers"
            | "provider_instances"
            | "default_provider_instance"
            | "model"
            | "defaults"
            | "features" => changes.provider = true,

            // Proxy domain
            "http_proxy"
            | "https_proxy"
            | "proxy_auth"
            | "proxy_auth_encrypted"
            | "http_proxy_auth_encrypted"
            | "https_proxy_auth_encrypted" => changes.proxy = true,

            // Setup domain (stored under Config.extra via serde flatten)
            "setup" => changes.setup = true,

            // MCP domain
            "mcp" | "mcpServers" => changes.mcp = true,

            // Other known config domains
            "keyword_masking" => changes.keyword_masking = true,
            "hooks" => changes.hooks = true,
            "anthropic_model_mapping" | "gemini_model_mapping" => changes.model_mapping = true,

            _ => {}
        }
    }

    changes
}

/// Determine what side-effects a config patch should trigger.
pub fn effects_for_root_patch(patch_obj: &Map<String, Value>) -> PatchEffects {
    let domains = domains_for_root_patch(patch_obj);

    let touches_provider = domains.provider || domains.hooks || domains.keyword_masking;
    let touches_proxy = domains.proxy;
    let touches_mcp = domains.mcp;

    PatchEffects {
        reload_provider: if touches_provider || touches_proxy {
            ReloadMode::BestEffort
        } else {
            ReloadMode::None
        },
        // SSE-based MCP servers are HTTP clients and must respect proxy settings.
        // Reconcile so proxy changes take effect without a restart.
        reconcile_mcp: touches_mcp || touches_proxy,
    }
}

/// Remove forbidden fields from a config patch before application.
///
/// Strips encrypted auth material, data_dir, and MCP secret fields
/// that should never be set directly by clients.
pub fn sanitize_root_patch(patch_obj: &mut Map<String, Value>) {
    // Never allow clients to modify proxy auth fields or data_dir via this endpoint.
    patch_obj.remove("proxy_auth");
    patch_obj.remove("proxy_auth_encrypted");
    patch_obj.remove("proxy_auth_credential_ref");
    // Legacy/compat proxy auth keys (written by older Bodhi/Tauri builds).
    patch_obj.remove("http_proxy_auth_encrypted");
    patch_obj.remove("https_proxy_auth_encrypted");
    patch_obj.remove("data_dir");
    // Env values and storage metadata are managed by the revisioned env API.
    // Dropping the whole domain prevents mask/ref/configured spoofing through
    // the permissive root PATCH surface.
    patch_obj.remove("env_vars");

    // Cluster credential refs/configured state are server-owned metadata. Keep
    // ordinary node edits compatible, but refuse client-selected references or
    // legacy ciphertext injection through the permissive root PATCH surface.
    if let Some(cluster_fabric) = patch_obj
        .get_mut("cluster_fabric")
        .and_then(|value| value.as_object_mut())
    {
        cluster_fabric.remove("credential_refs");
        if let Some(nodes) = cluster_fabric
            .get_mut("nodes")
            .and_then(|value| value.as_array_mut())
        {
            for node in nodes {
                let Some(auth) = node
                    .get_mut("placement")
                    .and_then(|value| value.as_object_mut())
                    .and_then(|placement| placement.get_mut("auth"))
                    .and_then(|value| value.as_object_mut())
                else {
                    continue;
                };
                auth.remove("password_encrypted");
                auth.remove("private_key_encrypted");
                auth.remove("passphrase_encrypted");
            }
        }
    }

    // Never allow clients to set encrypted key material directly.
    if let Some(providers) = patch_obj
        .get_mut("providers")
        .and_then(|v| v.as_object_mut())
    {
        for (_provider_name, provider_cfg) in providers.iter_mut() {
            let Some(obj) = provider_cfg.as_object_mut() else {
                continue;
            };
            obj.remove("api_key_encrypted");
        }
    }

    if let Some(provider_instances) = patch_obj
        .get_mut("provider_instances")
        .and_then(|v| v.as_object_mut())
    {
        for (_instance_id, instance_cfg) in provider_instances.iter_mut() {
            let Some(obj) = instance_cfg.as_object_mut() else {
                continue;
            };
            obj.remove("api_key_encrypted");
            obj.remove("credential_ref");
        }
    }

    // Never allow clients to set encrypted notification-channel secrets directly.
    if let Some(notifications) = patch_obj
        .get_mut("notifications")
        .and_then(|v| v.as_object_mut())
    {
        if let Some(ntfy) = notifications
            .get_mut("ntfy")
            .and_then(|v| v.as_object_mut())
        {
            ntfy.remove("token_encrypted");
        }
        if let Some(bark) = notifications
            .get_mut("bark")
            .and_then(|v| v.as_object_mut())
        {
            bark.remove("device_key_encrypted");
        }
    }

    // Never allow clients to set encrypted bamboo-connect platform secrets
    // (token, Feishu app_secret) directly.
    if let Some(platforms) = patch_obj
        .get_mut("connect")
        .and_then(|c| c.get_mut("platforms"))
        .and_then(|v| v.as_array_mut())
    {
        for platform in platforms.iter_mut() {
            if let Some(obj) = platform.as_object_mut() {
                obj.remove("token_encrypted");
                obj.remove("app_secret_encrypted");
            }
        }
    }

    // Never allow clients to set encrypted secret material directly.
    //
    // Canonical MCP format:
    //   "mcpServers": { "<id>": { env_encrypted, headers[*].value_encrypted, ... } }
    if let Some(mcp_servers) = patch_obj
        .get_mut("mcpServers")
        .and_then(|v| v.as_object_mut())
    {
        for (_id, server) in mcp_servers.iter_mut() {
            let Some(server_obj) = server.as_object_mut() else {
                continue;
            };
            server_obj.remove("env_encrypted");
            if let Some(headers) = server_obj.get_mut("headers").and_then(|v| v.as_array_mut()) {
                for header in headers.iter_mut() {
                    let Some(header_obj) = header.as_object_mut() else {
                        continue;
                    };
                    header_obj.remove("value_encrypted");
                }
            }
        }
    }

    // Legacy MCP shape:
    //   "mcp": { "servers": [ { transport: { env_encrypted / headers[*].value_encrypted } } ] }
    if let Some(servers) = patch_obj
        .get_mut("mcp")
        .and_then(|m| m.get_mut("servers"))
        .and_then(|v| v.as_array_mut())
    {
        for server in servers.iter_mut() {
            let Some(server_obj) = server.as_object_mut() else {
                continue;
            };
            let Some(transport) = server_obj
                .get_mut("transport")
                .and_then(|v| v.as_object_mut())
            else {
                continue;
            };

            match transport.get("type").and_then(|v| v.as_str()) {
                Some("stdio") => {
                    transport.remove("env_encrypted");
                }
                Some("sse") => {
                    if let Some(headers) =
                        transport.get_mut("headers").and_then(|v| v.as_array_mut())
                    {
                        for header in headers.iter_mut() {
                            let Some(header_obj) = header.as_object_mut() else {
                                continue;
                            };
                            header_obj.remove("value_encrypted");
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

/// Replace masked API key placeholders in a patch with the current config's plain keys.
///
/// The UI sends masked values (e.g. `****...****`) to indicate "do not change this key".
/// This function resolves those back to the existing plain-text key from the live config.
pub fn preserve_masked_provider_api_keys(patch_obj: &mut Map<String, Value>, current: &Config) {
    if let Some(patch_providers) = patch_obj
        .get_mut("providers")
        .and_then(|v| v.as_object_mut())
    {
        for (provider_name, provider_patch) in patch_providers.iter_mut() {
            let Some(patch_cfg_obj) = provider_patch.as_object_mut() else {
                continue;
            };

            let Some(api_key) = patch_cfg_obj.get("api_key").and_then(|v| v.as_str()) else {
                continue;
            };
            if !is_masked_api_key(api_key) {
                continue;
            }

            let existing_plain = match provider_name.as_str() {
                "openai" => current.providers.openai.as_ref().map(|c| c.api_key.clone()),
                "anthropic" => current
                    .providers
                    .anthropic
                    .as_ref()
                    .map(|c| c.api_key.clone()),
                "gemini" => current.providers.gemini.as_ref().map(|c| c.api_key.clone()),
                "bodhi" => current.providers.bodhi.as_ref().map(|c| c.api_key.clone()),
                _ => None,
            };

            if let Some(existing_plain) = existing_plain {
                if !existing_plain.trim().is_empty() {
                    patch_cfg_obj.insert("api_key".to_string(), Value::String(existing_plain));
                } else {
                    patch_cfg_obj.remove("api_key");
                }
            } else {
                patch_cfg_obj.remove("api_key");
            }
        }
    }

    if let Some(patch_instances) = patch_obj
        .get_mut("provider_instances")
        .and_then(|v| v.as_object_mut())
    {
        for (instance_id, instance_patch) in patch_instances.iter_mut() {
            let Some(patch_cfg_obj) = instance_patch.as_object_mut() else {
                continue;
            };

            let Some(api_key) = patch_cfg_obj.get("api_key").and_then(|v| v.as_str()) else {
                continue;
            };
            if !is_masked_api_key(api_key) {
                continue;
            }

            let existing_plain = current
                .provider_instances
                .get(instance_id)
                .map(|instance| instance.api_key.clone());

            if let Some(existing_plain) = existing_plain {
                if !existing_plain.trim().is_empty() {
                    patch_cfg_obj.insert("api_key".to_string(), Value::String(existing_plain));
                } else {
                    patch_cfg_obj.remove("api_key");
                }
            } else {
                patch_cfg_obj.remove("api_key");
            }
        }
    }
}

/// Carry provider secrets that the merge round-trip dropped forward from the
/// live config.
///
/// `config_manager::build_merged_config` serializes the live config before
/// merging a patch, which drops every `#[serde(skip_serializing)]` plaintext
/// `api_key`. Hydration afterwards only restores ciphertext-backed keys — but
/// the live config can legitimately hold a plaintext-only secret whose
/// `api_key_encrypted` is still `None` (ciphertext is only ever computed on
/// `save_to_dir`'s save-time clone; a provider instance freshly created via
/// the instance CRUD endpoints stays plaintext-only in memory). The merged
/// config then ends up with NEITHER field and the key is silently lost on the
/// next persist: config.json loses `api_key_encrypted` while config.json.bak
/// keeps it (#516).
///
/// Restores `api_key`/`api_key_encrypted` from `current` for every legacy
/// provider and provider instance that the patch did not explicitly set or
/// clear (per `intents`), whenever the merge left neither field behind.
/// Generalizes the env-sourced rescue of
/// [`Config::preserve_env_sourced_provider_keys`] (#373).
pub fn preserve_unpatched_provider_secrets(
    merged: &mut Config,
    current: &Config,
    intents: &ProviderApiKeyIntents,
) {
    macro_rules! carry_forward {
        ($field:ident) => {
            if !intents.providers.contains(stringify!($field)) {
                if let (Some(new_cfg), Some(prev)) = (
                    merged.providers.$field.as_mut(),
                    current.providers.$field.as_ref(),
                ) {
                    if new_cfg.api_key.trim().is_empty()
                        && new_cfg.api_key_encrypted.is_none()
                        && (!prev.api_key.trim().is_empty() || prev.api_key_encrypted.is_some())
                    {
                        new_cfg.api_key = prev.api_key.clone();
                        new_cfg.api_key_encrypted = prev.api_key_encrypted.clone();
                    }
                }
            }
        };
    }
    carry_forward!(openai);
    carry_forward!(anthropic);
    carry_forward!(gemini);
    carry_forward!(bodhi);

    for (id, instance) in merged.provider_instances.iter_mut() {
        if intents.provider_instances.contains(id) {
            continue;
        }
        if !instance.api_key.trim().is_empty() || instance.api_key_encrypted.is_some() {
            continue;
        }
        if let Some(prev) = current.provider_instances.get(id) {
            if !prev.api_key.trim().is_empty() || prev.api_key_encrypted.is_some() {
                instance.api_key = prev.api_key.clone();
                instance.api_key_encrypted = prev.api_key_encrypted.clone();
            }
        }
    }
}

/// Make an explicit `api_key: ""` clear actually clear.
///
/// The merge round-trip carries the live config's `api_key_encrypted` into the
/// merged value; hydration would then refill the plaintext from it and the
/// subsequent sync/save would re-encrypt — silently undoing the clear. For
/// every provider/instance the patch explicitly touched whose merged plaintext
/// is empty (a clear, not a set), drop the round-tripped ciphertext BEFORE
/// hydration so nothing refills the key (#516).
pub fn clear_provider_ciphertext_for_explicit_clears(
    merged: &mut Config,
    intents: &ProviderApiKeyIntents,
) {
    macro_rules! clear_ciphertext {
        ($field:ident) => {
            if intents.providers.contains(stringify!($field)) {
                if let Some(cfg) = merged.providers.$field.as_mut() {
                    if cfg.api_key.trim().is_empty() {
                        cfg.api_key_encrypted = None;
                    }
                }
            }
        };
    }
    clear_ciphertext!(openai);
    clear_ciphertext!(anthropic);
    clear_ciphertext!(gemini);
    clear_ciphertext!(bodhi);

    for id in intents.provider_instances.iter() {
        if let Some(instance) = merged.provider_instances.get_mut(id) {
            if instance.api_key.trim().is_empty() {
                instance.api_key_encrypted = None;
            }
        }
    }
}

/// Extract notification-channel secret update intents (ntfy `token`, Bark
/// `device_key`) from a config patch.
///
/// Mirrors [`provider_api_key_intents`]: masked placeholders are ignored
/// (they signal "keep existing secret"); an explicit empty string OR an
/// explicit JSON `null` (#505) is a genuine intent (a clear), same as a
/// genuine new value (a set) — see [`is_secret_field_intent`]. Must be
/// read from the patch AFTER [`preserve_masked_notification_secrets`] has
/// resolved masked placeholders — same calling convention as the provider
/// intents (#521).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NotificationSecretIntents {
    pub ntfy_token: bool,
    pub bark_device_key: bool,
}

pub fn notification_secret_intents(patch_obj: &Map<String, Value>) -> NotificationSecretIntents {
    let mut intents = NotificationSecretIntents::default();

    let Some(notifications) = patch_obj.get("notifications").and_then(|v| v.as_object()) else {
        return intents;
    };

    if let Some(ntfy) = notifications.get("ntfy").and_then(|v| v.as_object()) {
        intents.ntfy_token = is_secret_field_intent(ntfy, "token");
    }

    if let Some(bark) = notifications.get("bark").and_then(|v| v.as_object()) {
        intents.bark_device_key = is_secret_field_intent(bark, "device_key");
    }

    intents
}

/// Make an explicit `token: ""` / `device_key: ""` clear actually clear when
/// processing legacy in-memory ciphertext. New writes never serialize those
/// ciphertext fields, but an old loaded config can still carry them until its
/// migration completes.
pub fn clear_notification_ciphertext_for_explicit_clears(
    merged: &mut Config,
    intents: &NotificationSecretIntents,
) {
    if intents.ntfy_token
        && merged
            .notifications
            .ntfy
            .token
            .as_deref()
            .unwrap_or("")
            .trim()
            .is_empty()
    {
        merged.notifications.ntfy.token_encrypted = None;
    }

    if intents.bark_device_key
        && merged
            .notifications
            .bark
            .device_key
            .as_deref()
            .unwrap_or("")
            .trim()
            .is_empty()
    {
        merged.notifications.bark.device_key_encrypted = None;
    }
}

/// Restore store-hydrated notification plaintext after a compatibility JSON
/// round-trip for fields the patch did not explicitly replace or clear.
/// Credential references and configured metadata are serialized, but secret
/// plaintext is deliberately not.
pub fn preserve_unpatched_notification_secrets(
    merged: &mut Config,
    current: &Config,
    intents: &NotificationSecretIntents,
) {
    if !intents.ntfy_token {
        merged.notifications.ntfy.token = current.notifications.ntfy.token.clone();
    }
    if !intents.bark_device_key {
        merged.notifications.bark.device_key = current.notifications.bark.device_key.clone();
    }
}

/// Extract bamboo-connect platform secret update intents (`token`,
/// `app_secret`) from a config patch, keyed by the platform's position in the
/// patch's `connect.platforms` array.
///
/// `connect.platforms` is a full-array replace (the settings UI always
/// round-trips the whole list back in the same order it was fetched in — see
/// [`preserve_masked_connect_secrets`]'s module docs), so
/// `config_manager::build_merged_config`'s merged `connect.platforms[i]`
/// always originates verbatim from the patch's `connect.platforms[i]` —
/// position `i` in the patch and position `i` in the merged config always
/// refer to the same logical entry, regardless of how that entry's position
/// relates to `current.connect.platforms` (id/type resolution, #490/#492/#496,
/// happens earlier and only rewrites the VALUE at a given patch index, never
/// its position). Masked placeholders are ignored — mirrors
/// [`provider_api_key_intents`]; an explicit `null` is treated the same as
/// an explicit `""` clear (#505) — see [`is_secret_field_intent`]. Must be
/// read from the patch AFTER [`preserve_masked_connect_secrets`] has
/// resolved masked placeholders (#521).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConnectSecretIntents {
    pub token: std::collections::BTreeSet<usize>,
    pub app_secret: std::collections::BTreeSet<usize>,
}

pub fn connect_secret_intents(patch_obj: &Map<String, Value>) -> ConnectSecretIntents {
    let mut intents = ConnectSecretIntents::default();

    let Some(platforms) = patch_obj
        .get("connect")
        .and_then(|c| c.get("platforms"))
        .and_then(|v| v.as_array())
    else {
        return intents;
    };

    for (index, platform) in platforms.iter().enumerate() {
        let Some(obj) = platform.as_object() else {
            continue;
        };

        if is_secret_field_intent(obj, "token") {
            intents.token.insert(index);
        }

        if is_secret_field_intent(obj, "app_secret") {
            intents.app_secret.insert(index);
        }
    }

    intents
}

/// Make an explicit `token: ""` / `app_secret: ""` clear actually clear.
///
/// Generalizes [`clear_provider_ciphertext_for_explicit_clears`] (#521) to
/// bamboo-connect platform secrets — same rationale as
/// [`clear_notification_ciphertext_for_explicit_clears`]:
/// `token_encrypted`/`app_secret_encrypted` are not `skip_serializing`, so an
/// explicit clear's round-tripped ciphertext must be dropped BEFORE
/// hydration or it silently refills the plaintext hydration cleared.
pub fn clear_connect_ciphertext_for_explicit_clears(
    merged: &mut Config,
    intents: &ConnectSecretIntents,
) {
    for &index in intents.token.iter() {
        if let Some(platform) = merged.connect.platforms.get_mut(index) {
            if platform.token.as_deref().unwrap_or("").trim().is_empty() {
                platform.token_encrypted = None;
            }
        }
    }

    for &index in intents.app_secret.iter() {
        if let Some(platform) = merged.connect.platforms.get_mut(index) {
            if platform
                .app_secret
                .as_deref()
                .unwrap_or("")
                .trim()
                .is_empty()
            {
                platform.app_secret_encrypted = None;
            }
        }
    }
}

/// Replace masked notification-channel secret placeholders (ntfy `token`, Bark
/// `device_key`) in a patch with the current config's plaintext values.
///
/// Mirrors [`preserve_masked_provider_api_keys`]: the UI sends the masked
/// placeholder to mean "do not change this secret"; this resolves that back to
/// the live plaintext so the merge doesn't wipe it. A masked value with no
/// existing plaintext (nothing configured yet) is dropped from the patch
/// entirely, same as an unset key.
pub fn preserve_masked_notification_secrets(patch_obj: &mut Map<String, Value>, current: &Config) {
    let Some(notifications) = patch_obj
        .get_mut("notifications")
        .and_then(|v| v.as_object_mut())
    else {
        return;
    };

    if let Some(ntfy) = notifications
        .get_mut("ntfy")
        .and_then(|v| v.as_object_mut())
    {
        preserve_masked_secret_field(ntfy, "token", current.notifications.ntfy.token.as_deref());
    }

    if let Some(bark) = notifications
        .get_mut("bark")
        .and_then(|v| v.as_object_mut())
    {
        preserve_masked_secret_field(
            bark,
            "device_key",
            current.notifications.bark.device_key.as_deref(),
        );
    }
}

/// Replace masked bamboo-connect platform `token` placeholders in a patch with
/// the current config's plaintext values.
///
/// Mirrors [`preserve_masked_notification_secrets`]. `connect.platforms` is a
/// list (not a single object like ntfy/bark), so each patch entry is resolved
/// against a `current.connect.platforms` entry via three strategies, tried in
/// order:
///
/// 1. **Id match (#496)** — if the patch entry carries an `id` and some entry
///    in `current` has the same [`crate::ConnectPlatformConfig::id`] AND a
///    consistent `platform_type`, that entry is authoritative. A stable
///    server-assigned id unambiguously identifies the same logical platform
///    regardless of position — this is the only strategy that correctly
///    disambiguates two entries that share the same `platform_type` and have
///    been reordered relative to each other (the scenario #490/#492's
///    type+positional fallback cannot fully resolve; see the regression test
///    `preserve_masked_connect_secrets_resolves_duplicate_type_by_id_even_when_reordered`
///    below). The id match is guarded by the same type-consistency check as
///    strategy 2: a patch entry whose `type` DISAGREES with the id-matched
///    entry's `platform_type` (e.g. a stale/reused id after a client-side
///    bug, or a flow that repurposes an entry's identity for a different
///    platform) must not inherit the differently-typed entry's secret — a
///    Telegram bot token pasted into a Feishu adapter is never right. Such
///    a mismatch falls through to strategies 2/3 (which will also refuse a
///    cross-type resolution, dropping the mask — same as "nothing configured
///    yet"). A patch entry with an `id` but no `type` at all can't be
///    checked and resolves on id alone, mirroring strategy 2's handling of
///    a missing `type`. Legacy entries without an id (or a patch from a
///    client that doesn't echo it back) simply fall through to strategy 2.
/// 2. **Positional + type guard** — patch index `i` is resolved against
///    `current.connect.platforms[i]`, guarded by `type` equality at that
///    index. This mirrors how the settings UI round-trips the list (it
///    always sends the full array back in the same order it was fetched in
///    — the same convention `env_vars`' full-array replace relies on).
/// 3. **Type-based fallback (#490)** — when the positional entry's type
///    disagrees with the patch entry's `type` (or the patch index is beyond
///    `current.connect.platforms`), fall back to
///    `current.connect.platforms.iter().find(|p| p.platform_type == patch_type)`.
///    This is safe because `multi_bot_guard` (#462) means only the FIRST
///    entry of a given type is ever started, so resolving to any same-typed
///    entry is strictly better than silently wiping the secret. Only when no
///    entry of that type exists anywhere in `current` does the mask get
///    dropped, same as "nothing configured yet".
pub fn preserve_masked_connect_secrets(patch_obj: &mut Map<String, Value>, current: &Config) {
    let Some(platforms) = patch_obj
        .get_mut("connect")
        .and_then(|c| c.get_mut("platforms"))
        .and_then(|v| v.as_array_mut())
    else {
        return;
    };

    for (index, platform) in platforms.iter_mut().enumerate() {
        let Some(obj) = platform.as_object_mut() else {
            continue;
        };
        let patch_type = obj.get("type").and_then(|v| v.as_str());
        let patch_id = obj.get("id").and_then(|v| v.as_str());

        // Strategy 1 (#496): an id match is position-independent, but it is
        // still guarded by the SAME type-consistency check strategies 2/3
        // enforce — an id pointing at a differently-typed entry (stale or
        // repurposed id) must not leak that entry's secret into an adapter
        // of another type. A patch entry with no "type" can't be checked and
        // resolves on id alone (mirrors strategy 2's missing-"type" case).
        let by_id = patch_id.and_then(|patch_id| {
            current.connect.platforms.iter().find(|p| {
                p.id.as_deref() == Some(patch_id)
                    && match patch_type {
                        Some(patch_type) => patch_type == p.platform_type,
                        None => true,
                    }
            })
        });

        // Strategies 2/3 (#490/#492): positional + type guard, falling back
        // to a type-only lookup — unchanged, and only consulted when there
        // was no id, the id didn't match anything in `current` (e.g. a
        // stale id echoed after the entry was removed), or the id-matched
        // entry failed the type-consistency guard above.
        let existing = current.connect.platforms.get(index);
        // A patch entry that names a "type" disagreeing with `current`'s
        // entry at the same index (or an index beyond `current`'s array)
        // means the array was reordered/shrunk since the client fetched it
        // — the position no longer identifies the same logical platform, so
        // don't resolve the mask against it positionally. A patch entry with
        // no "type" at all (shouldn't happen with a well-behaved client,
        // which always echoes the whole object back) can't be checked and
        // falls back to the pre-existing positional-only behavior (no
        // type-based fallback either, since there's no type to search by).
        let guarded = by_id.or_else(|| {
            existing
                .filter(|p| match patch_type {
                    Some(patch_type) => patch_type == p.platform_type,
                    None => true,
                })
                .or_else(|| {
                    patch_type.and_then(|patch_type| {
                        current
                            .connect
                            .platforms
                            .iter()
                            .find(|p| p.platform_type == patch_type)
                    })
                })
        });
        preserve_masked_secret_field(obj, "token", guarded.and_then(|p| p.token.as_deref()));
        preserve_masked_secret_field(
            obj,
            "app_secret",
            guarded.and_then(|p| p.app_secret.as_deref()),
        );
    }
}

/// Resolve a single masked secret field in place: replace a masked placeholder
/// with `existing_plain`, or drop the field if nothing is configured yet.
/// A non-masked value (a genuine new secret, or an explicit empty-string
/// clear) is left untouched.
fn preserve_masked_secret_field(
    obj: &mut Map<String, Value>,
    field: &str,
    existing_plain: Option<&str>,
) {
    let Some(value) = obj.get(field).and_then(|v| v.as_str()) else {
        return;
    };
    if !is_masked_api_key(value) {
        return;
    }

    match existing_plain {
        Some(plain) if !plain.trim().is_empty() => {
            obj.insert(field.to_string(), Value::String(plain.to_string()));
        }
        _ => {
            obj.remove(field);
        }
    }
}

/// Deep merge `src` into `dst`, recursively combining objects and replacing leaf
/// values — [RFC 7386 JSON Merge Patch](https://www.rfc-editor.org/rfc/rfc7386)
/// semantics, generalized to arbitrary depth (#505).
///
/// ## Semantics (opt-in per value)
///
/// | Patch value at key `k`                        | Effect on `dst`                                    |
/// |------------------------------------------------|-----------------------------------------------------|
/// | key `k` absent from the patch object            | `dst[k]` unchanged (back-compat, unaffected by this)|
/// | `k: null`                                       | `dst[k]` **removed** — see below for what that means|
/// | `k: <object>`, `dst[k]` also an object          | recursively merged (this table applied one level down)|
/// | `k: <object>`, `dst[k]` absent/non-object       | `dst[k]` set verbatim to the patch object            |
/// | `k: <scalar \| array>`                          | `dst[k]` replaced verbatim (arrays are leaf values — RFC 7386 never merges into an array; a `null` *inside* an array is a literal element, not a delete marker)|
///
/// Removing `dst[k]` (the `null` row) has an effect that depends on what `k`
/// deserializes into on the Rust side, because "removed" means the merged
/// JSON object no longer carries that key at all — the subsequent
/// `serde_json::from_value::<Config>` falls back to that field's own
/// `#[serde(default)]`:
/// - `Option<T>` field → default is `None` → **the value is cleared/unset**.
///   This is the fix issue #505 asks for (e.g. `subagents.claude_code_binary:
///   null` un-sets a previously-written override).
/// - `Vec<T>` / `HashMap<K, V>` field reached directly (i.e. `null` sits at
///   the position of the *whole* collection, not one of its entries) →
///   default is empty → **the whole collection resets to empty/default**.
///   Per the design note in #505: a `null` *inside* an array never reaches
///   this path (arrays are leaf-replaced, previous row), so "does a null
///   inside an array delete that element" does not apply — only "does a
///   null in place of the whole array delete the array" does, and the
///   answer is yes.
/// - Plain non-`Option` scalar/struct field with a `Default` impl → resets
///   to that type's default (e.g. `notifications: null` resets the entire
///   notifications subtree to defaults, `http_proxy: null` resets it to
///   `""`).
/// - A key one level inside a map keyed by dynamic strings (e.g.
///   `provider_instances`, `mcpServers`) → since JSON can't distinguish a
///   struct's named fields from a `HashMap`'s dynamic keys, the same rule
///   removes just that one entry — **this is how a client deletes a single
///   provider instance or MCP server** (`provider_instances: { "<id>": null
///   }`).
///
/// A patch that wants to clear ONE field of a nested config (e.g. just
/// `providers.openai.api_key`) should send `null` at that leaf, not at an
/// enclosing object — `providers: { openai: null }` wipes the *entire*
/// openai provider config (model, base_url, api_key, everything), not just
/// the key. Both are valid, opt-in RFC 7386 semantics; callers choose their
/// blast radius by choosing which level they null out.
///
/// ## Secret-field composition (must NOT be bypassed)
///
/// This function is intentionally unaware of which fields are secrets. The
/// server layer (`config_manager::build_merged_config`) MUST extract secret
/// clear/set *intents* from the RAW patch object (via
/// [`provider_api_key_intents`], [`notification_secret_intents`],
/// [`connect_secret_intents`]) **before** calling this function — those
/// intent extractors now treat `null` on `api_key` / `token` / `device_key`
/// / `app_secret` identically to an explicit `""`: both are a "clear"
/// intent, distinct from an absent field ("leave alone") and from a masked
/// placeholder ("keep existing"). This preserves the precedence chain
/// established by #516/#517/#521/#522:
/// 1. masked placeholder → resolved back to the current plaintext (a `null`
///    is never masked — `is_masked_api_key` only matches strings — so this
///    step is a no-op for a `null` clear, which is correct: nothing should
///    resurrect a value the caller explicitly asked to delete).
/// 2. explicit clear intent (`""` OR `null`) → the merge (this function)
///    removes/empties the field, then `clear_*_ciphertext_for_explicit_clears`
///    drops the round-tripped ciphertext so hydration can't refill it.
/// 3. no intent at all (key absent from the patch) → `preserve_unpatched_*`
///    carries the live secret forward across the serde round-trip.
///
/// Without step 2 recognizing `null` as a clear intent, a `null`-delete of a
/// secret field would silently get UNDONE by step 3 (which only skips
/// providers/instances the intents mark as explicitly touched) — the merge
/// would drop the key, but `preserve_unpatched_provider_secrets` would then
/// think the field was untouched and copy the old ciphertext straight back.
pub fn deep_merge_json(dst: &mut Value, src: Value) {
    match (dst, src) {
        (Value::Object(dst_map), Value::Object(src_map)) => {
            for (key, value) in src_map {
                if value.is_null() {
                    // RFC 7386: `null` deletes the member from the target
                    // object. Absent from `dst` afterwards → the eventual
                    // `serde_json::from_value` falls back to that field's
                    // own `#[serde(default)]` (None for Option<T>, empty for
                    // Vec/HashMap, the type default otherwise). A key that
                    // was never in `dst` to begin with is simply a no-op,
                    // matching RFC 7386 (deleting a non-existent member is
                    // not an error).
                    dst_map.remove(&key);
                    continue;
                }
                match dst_map.get_mut(&key) {
                    Some(existing) if existing.is_object() && value.is_object() => {
                        deep_merge_json(existing, value);
                    }
                    _ => {
                        dst_map.insert(key, value);
                    }
                }
            }
        }
        (dst_slot, src_value) => {
            *dst_slot = src_value;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn domains_for_root_patch_detects_proxy_and_provider() {
        let patch = json!({
            "provider": "openai",
            "http_proxy": "http://proxy:8080",
            "setup": { "completed": false },
            "mcpServers": {}
        });

        let domains = domains_for_root_patch(patch.as_object().unwrap());
        assert!(domains.provider);
        assert!(domains.proxy);
        assert!(domains.setup);
        assert!(domains.mcp);
    }

    #[test]
    fn domains_for_root_patch_detects_provider_instances() {
        let patch = json!({
            "provider_instances": {
                "openai-work": { "provider_type": "openai" }
            },
            "default_provider_instance": "openai-work",
            "defaults": {
                "chat": { "provider": "openai-work", "model": "gpt-4o" }
            },
            "features": {
                "provider_model_ref": true
            }
        });

        let domains = domains_for_root_patch(patch.as_object().unwrap());
        assert!(domains.provider);
    }

    #[test]
    fn provider_api_key_intents_ignores_masked_placeholders() {
        let patch = json!({
            "providers": {
                "openai": { "api_key": "****...****" },
                "gemini": { "api_key": "sk-real" }
            },
            "provider_instances": {
                "work-openai": { "api_key": "****...****" },
                "personal-openai": { "api_key": "sk-live" }
            }
        });
        let intents = provider_api_key_intents(patch.as_object().unwrap());
        assert!(intents.providers.contains("gemini"));
        assert!(!intents.providers.contains("openai"));
        assert!(intents.provider_instances.contains("personal-openai"));
        assert!(!intents.provider_instances.contains("work-openai"));
    }

    /// Build a provider instance via serde (the struct has no `Default`), so
    /// the tests stay robust to new fields.
    fn instance(api_key: &str, encrypted: Option<&str>) -> crate::ProviderInstanceConfig {
        serde_json::from_value(json!({
            "provider_type": "openai",
            "api_key": api_key,
            "api_key_encrypted": encrypted,
        }))
        .expect("valid instance")
    }

    #[test]
    fn preserve_unpatched_provider_secrets_restores_roundtrip_dropped_keys() {
        // #516: the live config can hold a plaintext-only secret (ciphertext is
        // computed only on save_to_dir's save-time clone). The merge round-trip
        // drops the `skip_serializing` plaintext, leaving neither field.
        let mut current = Config::default();
        current
            .provider_instances
            .insert("uuid-1".to_string(), instance("sk-instance-live", None));
        current.providers.openai = Some(crate::OpenAIConfig {
            api_key: "sk-legacy-live".to_string(),
            ..Default::default()
        });

        // Simulate the post-round-trip merge result: both fields lost.
        let mut merged = Config::default();
        merged
            .provider_instances
            .insert("uuid-1".to_string(), instance("", None));
        merged.providers.openai = Some(crate::OpenAIConfig::default());

        preserve_unpatched_provider_secrets(
            &mut merged,
            &current,
            &ProviderApiKeyIntents::default(),
        );

        assert_eq!(
            merged.provider_instances["uuid-1"].api_key,
            "sk-instance-live"
        );
        assert_eq!(
            merged.providers.openai.as_ref().unwrap().api_key,
            "sk-legacy-live"
        );
    }

    #[test]
    fn preserve_unpatched_provider_secrets_carries_ciphertext_only_keys() {
        // A key whose plaintext failed to hydrate (#268) must still carry its
        // ciphertext forward instead of being dropped.
        let mut current = Config::default();
        current
            .provider_instances
            .insert("uuid-1".to_string(), instance("", Some("preexisting-ct")));

        let mut merged = Config::default();
        merged
            .provider_instances
            .insert("uuid-1".to_string(), instance("", None));

        preserve_unpatched_provider_secrets(
            &mut merged,
            &current,
            &ProviderApiKeyIntents::default(),
        );

        assert_eq!(
            merged.provider_instances["uuid-1"]
                .api_key_encrypted
                .as_deref(),
            Some("preexisting-ct")
        );
    }

    #[test]
    fn preserve_unpatched_provider_secrets_respects_explicit_intents() {
        // A provider/instance the patch explicitly set or cleared must not have
        // the old key resurrected.
        let mut current = Config::default();
        current
            .provider_instances
            .insert("uuid-1".to_string(), instance("sk-old", None));

        let mut merged = Config::default();
        merged
            .provider_instances
            .insert("uuid-1".to_string(), instance("", None));

        let mut intents = ProviderApiKeyIntents::default();
        intents.provider_instances.insert("uuid-1".to_string());

        preserve_unpatched_provider_secrets(&mut merged, &current, &intents);

        let cleared = &merged.provider_instances["uuid-1"];
        assert!(cleared.api_key.is_empty(), "explicit clear must win");
        assert!(cleared.api_key_encrypted.is_none());
    }

    #[test]
    fn clear_provider_ciphertext_drops_roundtripped_ciphertext_on_clear_intents() {
        // Merged state right after deserialization of a clear patch: empty
        // plaintext from the patch, ciphertext carried over by the round-trip
        // of the live config. Hydration must find nothing to refill.
        let mut merged = Config::default();
        merged
            .provider_instances
            .insert("uuid-1".to_string(), instance("", Some("roundtripped-ct")));
        merged.providers.openai = Some(crate::OpenAIConfig {
            api_key_encrypted: Some("legacy-ct".to_string()),
            ..Default::default()
        });

        let mut intents = ProviderApiKeyIntents::default();
        intents.provider_instances.insert("uuid-1".to_string());
        intents.providers.insert("openai".to_string());

        clear_provider_ciphertext_for_explicit_clears(&mut merged, &intents);

        assert!(merged.provider_instances["uuid-1"]
            .api_key_encrypted
            .is_none());
        assert!(merged
            .providers
            .openai
            .as_ref()
            .unwrap()
            .api_key_encrypted
            .is_none());
    }

    #[test]
    fn clear_provider_ciphertext_leaves_set_intents_and_unpatched_alone() {
        // A SET intent (non-empty merged plaintext) keeps its ciphertext (the
        // later sync overwrites it), and an instance without any intent is
        // untouched.
        let mut merged = Config::default();
        merged
            .provider_instances
            .insert("uuid-1".to_string(), instance("sk-new", Some("stale-ct")));
        merged
            .provider_instances
            .insert("uuid-2".to_string(), instance("", Some("kept-ct")));

        let mut intents = ProviderApiKeyIntents::default();
        intents.provider_instances.insert("uuid-1".to_string());

        clear_provider_ciphertext_for_explicit_clears(&mut merged, &intents);

        assert!(merged.provider_instances["uuid-1"]
            .api_key_encrypted
            .is_some());
        assert_eq!(
            merged.provider_instances["uuid-2"]
                .api_key_encrypted
                .as_deref(),
            Some("kept-ct")
        );
    }

    // ── #521: notification-secret clear intent ─────────────────────────

    #[test]
    fn notification_secret_intents_ignores_masked_placeholders() {
        let patch = json!({
            "notifications": {
                "ntfy": { "token": "****...****" },
                "bark": { "device_key": "tk-real-new-value" }
            }
        });
        let intents = notification_secret_intents(patch.as_object().unwrap());
        assert!(!intents.ntfy_token, "masked placeholder is not an intent");
        assert!(intents.bark_device_key, "a real value is an intent");
    }

    #[test]
    fn notification_secret_intents_detects_explicit_clear() {
        let patch = json!({
            "notifications": {
                "ntfy": { "token": "" },
                "bark": { "device_key": "" }
            }
        });
        let intents = notification_secret_intents(patch.as_object().unwrap());
        assert!(intents.ntfy_token, "empty string is a clear intent");
        assert!(intents.bark_device_key, "empty string is a clear intent");
    }

    #[test]
    fn notification_secret_intents_empty_when_untouched() {
        let patch = json!({ "notifications": { "ntfy": { "enabled": true } } });
        let intents = notification_secret_intents(patch.as_object().unwrap());
        assert!(!intents.ntfy_token);
        assert!(!intents.bark_device_key);
    }

    #[test]
    fn clear_notification_ciphertext_drops_roundtripped_ciphertext_on_clear_intents() {
        // Merged state right after deserialization of a clear patch: empty
        // plaintext from the patch, ciphertext carried over by the round-trip
        // of the live config. Hydration must find nothing to refill (#521).
        let mut merged = Config::default();
        merged.notifications.ntfy.token = None;
        merged.notifications.ntfy.token_encrypted = Some("roundtripped-ntfy-ct".to_string());
        merged.notifications.bark.device_key = None;
        merged.notifications.bark.device_key_encrypted = Some("roundtripped-bark-ct".to_string());

        let intents = NotificationSecretIntents {
            ntfy_token: true,
            bark_device_key: true,
        };
        clear_notification_ciphertext_for_explicit_clears(&mut merged, &intents);

        assert!(merged.notifications.ntfy.token_encrypted.is_none());
        assert!(merged.notifications.bark.device_key_encrypted.is_none());
    }

    #[test]
    fn clear_notification_ciphertext_leaves_set_intents_and_unpatched_alone() {
        let mut merged = Config::default();
        // A SET intent (non-empty merged plaintext) keeps its ciphertext — the
        // later refresh overwrites it with the new value's encryption.
        merged.notifications.ntfy.token = Some("brand-new-token".to_string());
        merged.notifications.ntfy.token_encrypted = Some("stale-ct".to_string());
        // No intent at all: untouched regardless of plaintext/ciphertext state.
        merged.notifications.bark.device_key = None;
        merged.notifications.bark.device_key_encrypted = Some("kept-ct".to_string());

        let intents = NotificationSecretIntents {
            ntfy_token: true,
            bark_device_key: false,
        };
        clear_notification_ciphertext_for_explicit_clears(&mut merged, &intents);

        assert_eq!(
            merged.notifications.ntfy.token_encrypted.as_deref(),
            Some("stale-ct")
        );
        assert_eq!(
            merged.notifications.bark.device_key_encrypted.as_deref(),
            Some("kept-ct")
        );
    }

    // ── #521: connect-secret clear intent ───────────────────────────────

    #[test]
    fn connect_secret_intents_ignores_masked_and_detects_clear_by_position() {
        let patch = json!({
            "connect": {
                "platforms": [
                    { "type": "telegram", "token": "****...****" },
                    { "type": "telegram", "token": "" },
                    { "type": "feishu", "app_secret": "real-new-secret" }
                ]
            }
        });
        let intents = connect_secret_intents(patch.as_object().unwrap());
        assert!(
            !intents.token.contains(&0),
            "masked placeholder is not an intent"
        );
        assert!(intents.token.contains(&1), "empty string is a clear intent");
        assert!(intents.app_secret.contains(&2), "a real value is an intent");
    }

    #[test]
    fn connect_secret_intents_empty_when_no_platforms_patched() {
        let patch = json!({ "http_proxy": "http://example.invalid:8080" });
        let intents = connect_secret_intents(patch.as_object().unwrap());
        assert!(intents.token.is_empty());
        assert!(intents.app_secret.is_empty());
    }

    #[test]
    fn clear_connect_ciphertext_drops_roundtripped_ciphertext_on_clear_intents() {
        // Merged state right after deserialization of a clear patch: empty
        // plaintext, ciphertext carried over by the round-trip of the live
        // config at the SAME position the patch's full-array-replace put it
        // (#521).
        let mut merged = Config::default();
        merged.connect.platforms = vec![
            connect_platform("telegram", ""), // will be overwritten below
            feishu_platform(""),
        ];
        merged.connect.platforms[0].token = None;
        merged.connect.platforms[0].token_encrypted = Some("roundtripped-token-ct".to_string());
        merged.connect.platforms[1].app_secret = None;
        merged.connect.platforms[1].app_secret_encrypted =
            Some("roundtripped-secret-ct".to_string());

        let mut intents = ConnectSecretIntents::default();
        intents.token.insert(0);
        intents.app_secret.insert(1);

        clear_connect_ciphertext_for_explicit_clears(&mut merged, &intents);

        assert!(merged.connect.platforms[0].token_encrypted.is_none());
        assert!(merged.connect.platforms[1].app_secret_encrypted.is_none());
    }

    #[test]
    fn clear_connect_ciphertext_leaves_set_intents_and_unpatched_alone() {
        let mut merged = Config::default();
        merged.connect.platforms = vec![connect_platform("telegram", "brand-new-token")];
        merged.connect.platforms[0].token_encrypted = Some("stale-ct".to_string());
        // A second platform with no clear intent (untouched by the patch)
        // must keep its ciphertext regardless of its plaintext state.
        merged
            .connect
            .platforms
            .push(connect_platform("feishu", ""));
        merged.connect.platforms[1].token = None;
        merged.connect.platforms[1].token_encrypted = Some("kept-ct".to_string());

        let mut intents = ConnectSecretIntents::default();
        intents.token.insert(0);

        clear_connect_ciphertext_for_explicit_clears(&mut merged, &intents);

        assert_eq!(
            merged.connect.platforms[0].token_encrypted.as_deref(),
            Some("stale-ct")
        );
        assert_eq!(
            merged.connect.platforms[1].token_encrypted.as_deref(),
            Some("kept-ct")
        );
    }

    #[test]
    fn is_masked_api_key_requires_placeholder_only_values() {
        // The redaction placeholder and all-asterisk/dot variants are masked.
        assert!(is_masked_api_key("****...****"));
        assert!(is_masked_api_key("********"));
        assert!(is_masked_api_key("  ****...****  "));

        // Empty is a "clear" signal, not a mask.
        assert!(!is_masked_api_key(""));
        assert!(!is_masked_api_key("   "));

        // A placeholder with a real key pasted after it must NOT be treated as
        // masked — that silently discards the user's new token (#430).
        assert!(!is_masked_api_key("****...****sk-newkey123"));
        assert!(!is_masked_api_key("sk-newkey123****...****"));

        // Real keys containing dots or asterisks among other characters are keys.
        assert!(!is_masked_api_key("id.secret...suffix"));
        assert!(!is_masked_api_key("sk-live-abc"));
    }

    #[test]
    fn sanitize_root_patch_strips_notification_encrypted_fields() {
        let mut patch = json!({
            "notifications": {
                "ntfy": { "token": "new-token", "token_encrypted": "client-supplied-cipher" },
                "bark": { "device_key": "new-key", "device_key_encrypted": "client-supplied-cipher" }
            }
        });
        let obj = patch.as_object_mut().unwrap();
        sanitize_root_patch(obj);

        assert!(!obj["notifications"]["ntfy"]
            .as_object()
            .unwrap()
            .contains_key("token_encrypted"));
        assert!(!obj["notifications"]["bark"]
            .as_object()
            .unwrap()
            .contains_key("device_key_encrypted"));
        // Plaintext fields the client legitimately sent are untouched.
        assert_eq!(obj["notifications"]["ntfy"]["token"], "new-token");
        assert_eq!(obj["notifications"]["bark"]["device_key"], "new-key");
    }

    #[test]
    fn sanitize_root_patch_strips_provider_instance_storage_metadata() {
        let mut patch = json!({
            "provider_instances": {
                "work": {
                    "provider_type": "openai",
                    "api_key": "sk-user-value",
                    "api_key_encrypted": "client-ciphertext",
                    "credential_ref": "attacker.chosen.ref"
                }
            }
        });
        let obj = patch.as_object_mut().unwrap();
        sanitize_root_patch(obj);

        let instance = obj["provider_instances"]["work"].as_object().unwrap();
        assert_eq!(instance["api_key"], "sk-user-value");
        assert!(!instance.contains_key("api_key_encrypted"));
        assert!(!instance.contains_key("credential_ref"));
    }

    #[test]
    fn sanitize_root_patch_strips_proxy_credential_metadata() {
        let mut patch = json!({
            "http_proxy": "http://proxy.example:8080",
            "proxy_auth": {"username": "attacker", "password": "secret"},
            "proxy_auth_encrypted": "client-ciphertext",
            "proxy_auth_credential_ref": "attacker.chosen.ref"
        });
        let obj = patch.as_object_mut().unwrap();
        sanitize_root_patch(obj);
        assert_eq!(obj["http_proxy"], "http://proxy.example:8080");
        assert!(!obj.contains_key("proxy_auth"));
        assert!(!obj.contains_key("proxy_auth_encrypted"));
        assert!(!obj.contains_key("proxy_auth_credential_ref"));
    }

    #[test]
    fn sanitize_root_patch_strips_cluster_storage_metadata_and_ciphertext() {
        let mut patch = json!({
            "cluster_fabric": {
                "credential_refs": {
                    "node-a": {
                        "password_credential_ref": "attacker.chosen.ref",
                        "password_configured": true
                    }
                },
                "nodes": [{
                    "id": "node-a",
                    "placement": {
                        "type": "ssh",
                        "auth": {
                            "method": "private_key",
                            "private_key": "new-user-value",
                            "private_key_encrypted": "client-ciphertext",
                            "passphrase": "new-passphrase",
                            "passphrase_encrypted": "client-ciphertext"
                        }
                    }
                }]
            }
        });
        let obj = patch.as_object_mut().unwrap();
        sanitize_root_patch(obj);

        let cluster = obj["cluster_fabric"].as_object().unwrap();
        assert!(!cluster.contains_key("credential_refs"));
        let auth = cluster["nodes"][0]["placement"]["auth"]
            .as_object()
            .unwrap();
        assert_eq!(auth["private_key"], "new-user-value");
        assert_eq!(auth["passphrase"], "new-passphrase");
        assert!(!auth.contains_key("private_key_encrypted"));
        assert!(!auth.contains_key("passphrase_encrypted"));
    }

    #[test]
    fn preserve_masked_notification_secrets_keeps_existing_plaintext() {
        let mut current = Config::default();
        current.notifications.ntfy.token = Some("existing-ntfy-token".to_string());
        current.notifications.bark.device_key = Some("existing-bark-key".to_string());

        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"notifications":{"ntfy":{"token":"****...****"},"bark":{"device_key":"****...****"}}}"#,
        )
        .unwrap();

        preserve_masked_notification_secrets(&mut patch, &current);

        assert_eq!(
            patch["notifications"]["ntfy"]["token"],
            "existing-ntfy-token"
        );
        assert_eq!(
            patch["notifications"]["bark"]["device_key"],
            "existing-bark-key"
        );
    }

    #[test]
    fn preserve_masked_notification_secrets_drops_mask_when_nothing_configured() {
        let current = Config::default();
        let mut patch: Map<String, Value> =
            serde_json::from_str(r#"{"notifications":{"ntfy":{"token":"****...****"}}}"#).unwrap();

        preserve_masked_notification_secrets(&mut patch, &current);

        assert!(!patch["notifications"]["ntfy"]
            .as_object()
            .unwrap()
            .contains_key("token"));
    }

    #[test]
    fn preserve_masked_notification_secrets_leaves_real_values_untouched() {
        let current = Config::default();
        let mut patch: Map<String, Value> =
            serde_json::from_str(r#"{"notifications":{"ntfy":{"token":"tk-real-new-value"}}}"#)
                .unwrap();

        preserve_masked_notification_secrets(&mut patch, &current);

        assert_eq!(patch["notifications"]["ntfy"]["token"], "tk-real-new-value");
    }

    fn connect_platform(platform_type: &str, token: &str) -> crate::ConnectPlatformConfig {
        crate::ConnectPlatformConfig {
            id: None,
            platform_type: platform_type.to_string(),
            token: Some(token.to_string()),
            token_encrypted: None,
            token_credential_ref: None,
            token_configured: false,
            app_id: None,
            app_secret: None,
            app_secret_encrypted: None,
            app_secret_credential_ref: None,
            app_secret_configured: false,
            domain: None,
            allow_from: Vec::new(),
            admin_from: Vec::new(),
        }
    }

    fn connect_platform_with_id(
        id: &str,
        platform_type: &str,
        token: &str,
    ) -> crate::ConnectPlatformConfig {
        crate::ConnectPlatformConfig {
            id: Some(id.to_string()),
            ..connect_platform(platform_type, token)
        }
    }

    #[test]
    fn sanitize_root_patch_strips_connect_platform_encrypted_field() {
        let mut patch = json!({
            "connect": {
                "platforms": [
                    { "type": "telegram", "token": "new-token", "token_encrypted": "client-supplied-cipher" }
                ]
            }
        });
        let obj = patch.as_object_mut().unwrap();
        sanitize_root_patch(obj);

        let platform = &obj["connect"]["platforms"][0];
        assert!(!platform
            .as_object()
            .unwrap()
            .contains_key("token_encrypted"));
        assert_eq!(platform["token"], "new-token");
    }

    #[test]
    fn sanitize_root_patch_strips_connect_platform_app_secret_encrypted_field() {
        let mut patch = json!({
            "connect": {
                "platforms": [
                    {
                        "type": "feishu",
                        "app_id": "cli_x",
                        "app_secret": "new-secret",
                        "app_secret_encrypted": "client-supplied-cipher",
                        "domain": "feishu"
                    }
                ]
            }
        });
        let obj = patch.as_object_mut().unwrap();
        sanitize_root_patch(obj);

        let platform = &obj["connect"]["platforms"][0];
        assert!(!platform
            .as_object()
            .unwrap()
            .contains_key("app_secret_encrypted"));
        assert_eq!(platform["app_secret"], "new-secret");
        assert_eq!(platform["app_id"], "cli_x");
    }

    #[test]
    fn preserve_masked_connect_secrets_keeps_existing_plaintext_by_position() {
        let mut current = Config::default();
        current.connect.platforms = vec![connect_platform("telegram", "existing-bot-token")];

        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[{"type":"telegram","token":"****...****"}]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert_eq!(
            patch["connect"]["platforms"][0]["token"],
            "existing-bot-token"
        );
    }

    #[test]
    fn preserve_masked_connect_secrets_drops_mask_when_nothing_configured() {
        let current = Config::default();
        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[{"type":"telegram","token":"****...****"}]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert!(!patch["connect"]["platforms"][0]
            .as_object()
            .unwrap()
            .contains_key("token"));
    }

    #[test]
    fn preserve_masked_connect_secrets_leaves_real_values_untouched() {
        let current = Config::default();
        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[{"type":"telegram","token":"tg-real-new-value"}]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert_eq!(
            patch["connect"]["platforms"][0]["token"],
            "tg-real-new-value"
        );
    }

    /// Issue #454 follow-up: if the array was reordered/edited since the
    /// client fetched it — detectable here because the patch entry's "type"
    /// disagrees with `current`'s entry at the same index — a masked token
    /// must NOT be resolved against the wrong (now co-located) platform's
    /// plaintext; it must drop, exactly like "nothing configured yet".
    #[test]
    fn preserve_masked_connect_secrets_drops_mask_when_type_at_index_disagrees() {
        let mut current = Config::default();
        current.connect.platforms = vec![connect_platform("telegram", "telegram-secret-token")];

        // The patch's entry at index 0 claims to be a DIFFERENT platform type
        // than what's actually at `current.connect.platforms[0]` — e.g. a
        // reorder/insert raced the fetch that pre-filled this mask.
        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[{"type":"feishu","token":"****...****"}]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert!(
            !patch["connect"]["platforms"][0]
                .as_object()
                .unwrap()
                .contains_key("token"),
            "masked token must not be resolved against a different platform's secret"
        );
    }

    /// #490: when the type at an index mismatches, the guard now falls back
    /// to a type-based lookup across all of `current.connect.platforms`
    /// rather than dropping outright. Here index 1's patch entry claims
    /// "telegram" (matching index 0's type, not index 1's) — the mismatch at
    /// index 1 is detected, but since a "telegram" entry DOES exist
    /// elsewhere in `current` (at index 0), the mask resolves to it. This is
    /// safe because `multi_bot_guard` (#462) only ever starts the first
    /// entry of a given type, so resolving to any same-typed entry is
    /// strictly better than wiping the secret.
    #[test]
    fn preserve_masked_connect_secrets_type_mismatch_falls_back_to_type_lookup() {
        let mut current = Config::default();
        current.connect.platforms = vec![
            connect_platform("telegram", "bot-a-token"),
            connect_platform("feishu", "feishu-token"),
        ];

        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[
                {"type":"telegram","token":"tg-real-value"},
                {"type":"telegram","token":"****...****"}
            ]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert_eq!(patch["connect"]["platforms"][0]["token"], "tg-real-value");
        assert_eq!(
            patch["connect"]["platforms"][1]["token"], "bot-a-token",
            "index 1's mismatched type must fall back to the same-typed entry found elsewhere in current"
        );
    }

    /// A patch entry with a matching "type" at its index is unaffected by
    /// the new guard — this is the common case and must keep working exactly
    /// as before.
    #[test]
    fn preserve_masked_connect_secrets_keeps_plaintext_when_type_at_index_matches() {
        let mut current = Config::default();
        current.connect.platforms = vec![
            connect_platform("telegram", "bot-a-token"),
            connect_platform("telegram", "bot-b-token"),
        ];

        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[
                {"type":"telegram","token":"****...****"},
                {"type":"telegram","token":"****...****"}
            ]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert_eq!(patch["connect"]["platforms"][0]["token"], "bot-a-token");
        assert_eq!(patch["connect"]["platforms"][1]["token"], "bot-b-token");
    }

    /// A patch entry missing "type" entirely (shouldn't happen with a
    /// well-behaved client) falls back to the pre-existing positional-only
    /// behavior rather than being treated as an automatic mismatch.
    #[test]
    fn preserve_masked_connect_secrets_falls_back_to_positional_when_type_is_absent() {
        let mut current = Config::default();
        current.connect.platforms = vec![connect_platform("telegram", "existing-bot-token")];

        let mut patch: Map<String, Value> =
            serde_json::from_str(r#"{"connect":{"platforms":[{"token":"****...****"}]}}"#).unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert_eq!(
            patch["connect"]["platforms"][0]["token"],
            "existing-bot-token"
        );
    }

    /// #496 — the exact scenario the issue is about: two entries share the
    /// same `platform_type` ("telegram") AND have been reordered, so the
    /// type+positional guard alone can't tell they were swapped (the type at
    /// each index still matches "telegram" either way). Without an id, this
    /// class of reorder can attach the wrong sibling's secret. With a
    /// server-assigned id present on both, resolution is unambiguous
    /// regardless of position.
    #[test]
    fn preserve_masked_connect_secrets_resolves_duplicate_type_by_id_even_when_reordered() {
        let mut current = Config::default();
        current.connect.platforms = vec![
            connect_platform_with_id("id-a", "telegram", "bot-a-token"),
            connect_platform_with_id("id-b", "telegram", "bot-b-token"),
        ];

        // The client echoes the array back SWAPPED: id-b now at index 0,
        // id-a now at index 1. Positional+type resolution alone would
        // (wrongly) resolve index 0 against current[0] (id-a's token) since
        // both are "telegram" — the id lets us catch the swap.
        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[
                {"id":"id-b","type":"telegram","token":"****...****"},
                {"id":"id-a","type":"telegram","token":"****...****"}
            ]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert_eq!(
            patch["connect"]["platforms"][0]["token"], "bot-b-token",
            "index 0 (id-b) must resolve to id-b's own token, not the positionally-co-located id-a"
        );
        assert_eq!(
            patch["connect"]["platforms"][1]["token"], "bot-a-token",
            "index 1 (id-a) must resolve to id-a's own token"
        );
    }

    /// A patch entry whose `id` doesn't match anything in `current` (e.g. a
    /// brand-new entry a client invented an id for, or a stale id echoed
    /// after the entry was removed) falls through to the existing
    /// positional/type-based resolution rather than treating the mismatch
    /// as fatal.
    #[test]
    fn preserve_masked_connect_secrets_falls_back_when_id_not_found_in_current() {
        let mut current = Config::default();
        current.connect.platforms = vec![connect_platform("telegram", "existing-bot-token")];

        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[{"id":"unknown-id","type":"telegram","token":"****...****"}]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert_eq!(
            patch["connect"]["platforms"][0]["token"], "existing-bot-token",
            "an id absent from `current` falls back to the positional+type guard"
        );
    }

    /// PR #510 review: the id branch is guarded by the same type-consistency
    /// check as the positional branch. A patch entry whose `id` matches a
    /// `current` entry of a DIFFERENT `platform_type` (stale/reused id, or a
    /// flow repurposing an entry's identity for another platform) must NOT
    /// inherit that entry's secret — with no same-typed entry anywhere in
    /// `current`, the mask drops, same as "nothing configured yet".
    #[test]
    fn preserve_masked_connect_secrets_rejects_id_match_when_type_differs() {
        let mut current = Config::default();
        current.connect.platforms = vec![connect_platform_with_id(
            "id-a",
            "telegram",
            "telegram-secret-token",
        )];

        // The patch reuses stored entry id-a's id but claims to be a Feishu
        // adapter — resolving the mask against the Telegram entry would paste
        // a Telegram bot token into a Feishu config.
        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[{"id":"id-a","type":"feishu","token":"****...****"}]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert!(
            !patch["connect"]["platforms"][0]
                .as_object()
                .unwrap()
                .contains_key("token"),
            "an id match with a disagreeing type must not resolve the mask from that entry"
        );
    }

    /// The type-mismatched-id fall-through still reaches strategies 2/3: if a
    /// same-typed entry DOES exist elsewhere in `current`, the type-based
    /// fallback (#490) resolves the mask against it — the bad id only
    /// disqualifies the id branch, it doesn't poison the whole resolution.
    #[test]
    fn preserve_masked_connect_secrets_type_mismatched_id_still_falls_through_to_type_lookup() {
        let mut current = Config::default();
        current.connect.platforms = vec![
            connect_platform_with_id("id-a", "telegram", "telegram-secret-token"),
            connect_platform_with_id("id-b", "feishu", "feishu-secret-token"),
        ];

        // id-a belongs to the telegram entry, but the patch entry says
        // "feishu" (at index 0, where current has telegram): the id branch
        // and the positional branch both refuse, and the type-based fallback
        // resolves against the feishu entry at index 1.
        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[{"id":"id-a","type":"feishu","token":"****...****"}]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert_eq!(
            patch["connect"]["platforms"][0]["token"], "feishu-secret-token",
            "the fall-through must resolve via the type lookup, never via the mismatched id"
        );
    }

    /// A patch entry with an `id` but no `type` at all can't be
    /// type-checked and resolves on id alone — mirrors the positional
    /// branch's handling of a missing `type` (documented in the id-branch
    /// guard). Placed at an out-of-range index to prove it's the id doing
    /// the work, not position.
    #[test]
    fn preserve_masked_connect_secrets_id_without_type_resolves_on_id_alone() {
        let mut current = Config::default();
        current.connect.platforms = vec![
            connect_platform_with_id("id-a", "telegram", "bot-a-token"),
            connect_platform_with_id("id-b", "telegram", "bot-b-token"),
        ];

        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[{"id":"id-b","token":"****...****"}]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert_eq!(
            patch["connect"]["platforms"][0]["token"], "bot-b-token",
            "with no type to check, the id match resolves to its own entry, not the positional one"
        );
    }

    /// Back-compat: a patch entry with no "id" field at all (legacy client,
    /// or an entry created before ids existed) is unaffected by the new
    /// id-matching branch and goes straight to the existing
    /// positional/type-based resolution — exercised here with `current`
    /// entries that DO have ids (post-migration) to prove the id branch is
    /// skipped cleanly rather than erroring on the missing field.
    #[test]
    fn preserve_masked_connect_secrets_ignores_id_branch_when_patch_omits_id() {
        let mut current = Config::default();
        current.connect.platforms = vec![connect_platform_with_id(
            "id-a",
            "telegram",
            "existing-bot-token",
        )];

        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[{"type":"telegram","token":"****...****"}]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert_eq!(
            patch["connect"]["platforms"][0]["token"],
            "existing-bot-token"
        );
    }

    fn feishu_platform(app_secret: &str) -> crate::ConnectPlatformConfig {
        crate::ConnectPlatformConfig {
            id: None,
            platform_type: "feishu".to_string(),
            token: None,
            token_encrypted: None,
            token_credential_ref: None,
            token_configured: false,
            app_id: Some("cli_x".to_string()),
            app_secret: Some(app_secret.to_string()),
            app_secret_encrypted: None,
            app_secret_credential_ref: None,
            app_secret_configured: false,
            domain: Some("lark".to_string()),
            allow_from: Vec::new(),
            admin_from: Vec::new(),
        }
    }

    #[test]
    fn preserve_masked_connect_secrets_keeps_existing_app_secret_by_position() {
        let mut current = Config::default();
        current.connect.platforms = vec![feishu_platform("existing-app-secret")];

        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[{"type":"feishu","app_id":"cli_x","app_secret":"****...****","domain":"lark"}]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert_eq!(
            patch["connect"]["platforms"][0]["app_secret"],
            "existing-app-secret"
        );
        // app_id/domain are untouched by the secret-preserve pass.
        assert_eq!(patch["connect"]["platforms"][0]["app_id"], "cli_x");
        assert_eq!(patch["connect"]["platforms"][0]["domain"], "lark");
    }

    #[test]
    fn preserve_masked_connect_secrets_drops_app_secret_mask_when_nothing_configured() {
        let current = Config::default();
        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[{"type":"feishu","app_secret":"****...****"}]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert!(!patch["connect"]["platforms"][0]
            .as_object()
            .unwrap()
            .contains_key("app_secret"));
    }

    #[test]
    fn preserve_masked_connect_secrets_leaves_real_app_secret_untouched() {
        let current = Config::default();
        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[{"type":"feishu","app_secret":"feishu-real-new-value"}]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert_eq!(
            patch["connect"]["platforms"][0]["app_secret"],
            "feishu-real-new-value"
        );
    }

    /// The #454 type-at-index guard applies to app_secret exactly as it does
    /// to token: a reordered array must not resolve a masked app_secret
    /// against whatever platform now sits at that index.
    #[test]
    fn preserve_masked_connect_secrets_drops_app_secret_mask_when_type_at_index_disagrees() {
        let mut current = Config::default();
        current.connect.platforms = vec![feishu_platform("feishu-secret")];

        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[{"type":"telegram","app_secret":"****...****"}]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert!(
            !patch["connect"]["platforms"][0]
                .as_object()
                .unwrap()
                .contains_key("app_secret"),
            "masked app_secret must not be resolved against a different platform's secret"
        );
    }

    /// #490 regression: the exact scenario from the issue. Stored platforms
    /// are `[telegram, feishu]`; the client disables telegram and echoes
    /// back only the (still-masked) feishu entry, which now shifts to index
    /// 0. The positional guard sees telegram≠feishu at index 0 and used to
    /// drop the mask outright, silently wiping the feishu app_secret on
    /// save. It must now fall back to a type-based lookup and resolve to the
    /// stored feishu plaintext.
    #[test]
    fn preserve_masked_connect_secrets_resolves_by_type_when_preceding_entry_removed() {
        let mut current = Config::default();
        current.connect.platforms = vec![
            connect_platform("telegram", "telegram-token"),
            feishu_platform("existing-app-secret"),
        ];

        // telegram was removed client-side; only the feishu entry (still
        // masked) is echoed back, now at index 0.
        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[
                {"type":"feishu","app_id":"cli_x","app_secret":"****...****","domain":"lark"}
            ]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert_eq!(
            patch["connect"]["platforms"][0]["app_secret"], "existing-app-secret",
            "masked app_secret must resolve via type fallback after a preceding entry was removed"
        );
    }

    /// #490: an index beyond `current.connect.platforms` (the patch array
    /// grew, e.g. a new platform was added client-side before saving) must
    /// also fall back to the type-based lookup rather than dropping the mask
    /// when a same-typed entry exists elsewhere in `current`.
    #[test]
    fn preserve_masked_connect_secrets_resolves_by_type_when_index_out_of_range() {
        let mut current = Config::default();
        current.connect.platforms = vec![
            connect_platform("telegram", "telegram-token"),
            feishu_platform("existing-app-secret"),
        ];

        // Patch has 3 entries; index 2 is beyond `current`'s 2-entry array
        // (a new telegram entry was inserted at index 1 client-side), but
        // its masked feishu app_secret should still resolve by type.
        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[
                {"type":"telegram","token":"tg-real-value"},
                {"type":"telegram","token":"new-bot-token"},
                {"type":"feishu","app_id":"cli_x","app_secret":"****...****","domain":"lark"}
            ]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert_eq!(
            patch["connect"]["platforms"][2]["app_secret"], "existing-app-secret",
            "masked app_secret at an out-of-range index must resolve via type fallback"
        );
    }

    /// #490: the type-based fallback only searches by type — if the patched
    /// type doesn't exist anywhere in `current`, the mask still drops, same
    /// as before. Unchanged behavior, exercised here with a multi-entry
    /// `current` (not just the empty-config case already covered above).
    #[test]
    fn preserve_masked_connect_secrets_drops_mask_when_type_absent_from_current_entirely() {
        let mut current = Config::default();
        current.connect.platforms = vec![connect_platform("telegram", "telegram-token")];

        // No feishu entry exists anywhere in `current` — the type-based
        // fallback has nothing to resolve against.
        let mut patch: Map<String, Value> = serde_json::from_str(
            r#"{"connect":{"platforms":[
                {"type":"feishu","app_id":"cli_x","app_secret":"****...****","domain":"lark"}
            ]}}"#,
        )
        .unwrap();

        preserve_masked_connect_secrets(&mut patch, &current);

        assert!(
            !patch["connect"]["platforms"][0]
                .as_object()
                .unwrap()
                .contains_key("app_secret"),
            "mask must still drop when no entry of that type exists anywhere in current"
        );
    }

    // ── #505: RFC 7386-style null-delete ────────────────────────────────
    //
    // Full round-trip helper: serialize `current`, deep-merge `patch` into
    // it, and deserialize the result back into a `Config` — exactly what
    // `config_manager::build_merged_config` does around `deep_merge_json`,
    // minus the secret-specific composition (covered separately below).
    fn merge_and_deserialize(current: &Config, patch: Value) -> Config {
        let mut merged = current.to_compatibility_value().unwrap();
        deep_merge_json(&mut merged, patch);
        serde_json::from_value(merged).expect("merged config should deserialize")
    }

    #[test]
    fn null_deletes_option_scalar_leaf() {
        // The exact case #505 was filed for: an Option<String> field that
        // was written once can never be un-set through plain overwrite
        // semantics. A null leaf deletes just that field.
        let mut current = Config::default();
        current.subagents.claude_code_binary = Some("/usr/local/bin/claude".to_string());
        current.subagents.claude_code_model = Some("claude-sonnet".to_string());

        let merged = merge_and_deserialize(
            &current,
            json!({ "subagents": { "claude_code_binary": null } }),
        );

        assert_eq!(merged.subagents.claude_code_binary, None);
        // Surgical: a sibling Option field in the same object that the
        // patch didn't touch survives untouched.
        assert_eq!(
            merged.subagents.claude_code_model,
            Some("claude-sonnet".to_string())
        );
    }

    #[test]
    fn absent_key_leaves_value_unchanged_back_compat() {
        // Back-compat is a hard requirement: omitting a key from the patch
        // must never be interpreted as a delete, regardless of the new null
        // semantics living right next to it.
        let mut current = Config::default();
        current.subagents.claude_code_binary = Some("/usr/local/bin/claude".to_string());
        current.subagents.max_concurrent = Some(4);

        // The patch touches a sibling field only; claude_code_binary is
        // simply not mentioned.
        let merged =
            merge_and_deserialize(&current, json!({ "subagents": { "max_concurrent": 16 } }));

        assert_eq!(
            merged.subagents.claude_code_binary,
            Some("/usr/local/bin/claude".to_string()),
            "an omitted key must leave the existing value untouched"
        );
        assert_eq!(merged.subagents.max_concurrent, Some(16));
    }

    #[test]
    fn null_on_whole_object_subtree_resets_it_to_defaults() {
        // Before #505, `null` on a non-Option struct field (like
        // `notifications`) crashed the ENTIRE patch with a deserialize
        // error ("invalid type: null, expected struct..."). Deleting the
        // key now falls back to that field's own `#[serde(default)]`,
        // resetting the whole subtree rather than erroring.
        let mut current = Config::default();
        current.notifications.ntfy.token = Some("existing-token".to_string());
        current.notifications.ntfy.enabled = true;

        let merged = merge_and_deserialize(&current, json!({ "notifications": null }));

        assert_eq!(merged.notifications, crate::NotificationsConfig::default());
    }

    #[test]
    fn null_deletes_one_hashmap_entry_keeps_siblings() {
        // The other half of the issue's motivating gap: a client could never
        // delete a single map entry (provider instance, MCP server, ...) —
        // sending null for one entry used to crash deserialization of the
        // WHOLE map. Now it deletes just that entry.
        //
        // Note: `api_key` is `#[serde(skip_serializing)]` (plaintext never
        // round-trips through `serde_json::to_value` — that's the unrelated
        // #516 quirk `preserve_unpatched_provider_secrets` exists to paper
        // over), so this test asserts survival via `label`, a plain
        // serialized field, to isolate the hashmap-entry-delete mechanic
        // being tested here from that separate secret-round-trip concern
        // (covered by its own tests below).
        fn labeled_instance(label: &str) -> crate::ProviderInstanceConfig {
            serde_json::from_value(json!({
                "provider_type": "openai",
                "label": label,
            }))
            .expect("valid instance")
        }

        let mut current = Config::default();
        current
            .provider_instances
            .insert("uuid-1".to_string(), labeled_instance("Work"));
        current
            .provider_instances
            .insert("uuid-2".to_string(), labeled_instance("Personal"));

        let merged = merge_and_deserialize(
            &current,
            json!({ "provider_instances": { "uuid-1": null } }),
        );

        assert!(!merged.provider_instances.contains_key("uuid-1"));
        assert_eq!(
            merged
                .provider_instances
                .get("uuid-2")
                .and_then(|i| i.label.as_deref()),
            Some("Personal"),
            "sibling map entries the patch didn't touch must survive"
        );
    }

    #[test]
    fn null_on_whole_array_field_resets_it_to_empty() {
        // Design decision (#505): arrays are leaf-replaced, never merged
        // element-by-element. A `null` standing in for the WHOLE array
        // deletes it (falls back to Vec's default, i.e. empty) — but this
        // is a distinct case from "null as one element inside a
        // surviving array" (covered by the next test), which is NOT a
        // delete marker.
        let mut current = Config::default();
        current.connect.platforms = vec![connect_platform("telegram", "tok")];

        let merged = merge_and_deserialize(&current, json!({ "connect": { "platforms": null } }));

        assert!(merged.connect.platforms.is_empty());
    }

    #[test]
    fn null_inside_a_surviving_array_is_a_literal_element_not_a_delete_marker() {
        // RFC 7386 never recurses into arrays — they're leaf values, always
        // replaced wholesale. So a patch that sends `[..., null, ...]` does
        // NOT delete an element out of the existing array; the whole array
        // is replaced verbatim, null and all. Demonstrated here against a
        // `Vec<String>` field: since `String` (not `Option<String>`) can't
        // hold a null, the round-trip surfaces a normal type error instead
        // of silently dropping an element — proving the null was carried
        // through literally, not specially interpreted.
        let mut current = Config::default();
        current.subagents.worker_args = Some(vec!["subagent-worker".to_string()]);

        let mut merged = serde_json::to_value(&current).unwrap();
        deep_merge_json(
            &mut merged,
            json!({ "subagents": { "worker_args": ["a", null, "b"] } }),
        );

        // The array in the merged JSON is the patch's array verbatim,
        // literal null included — not silently filtered.
        assert_eq!(
            merged["subagents"]["worker_args"],
            json!(["a", null, "b"]),
            "arrays are leaf-replaced verbatim; a null element is not deleted"
        );
        let result: Result<Config, _> = serde_json::from_value(merged);
        assert!(
            result.is_err(),
            "a literal null inside a Vec<String> is a type error, not an element delete"
        );
    }

    #[test]
    fn sentinel_string_values_still_work_after_null_delete_support() {
        // Lotus #80: `subagents.executor = "bamboo_runtime"` is a plain
        // string sentinel value (not related to null-delete at all) that
        // must keep working exactly as a normal overwrite.
        let current = Config::default();

        let merged = merge_and_deserialize(
            &current,
            json!({ "subagents": { "executor": "bamboo_runtime" } }),
        );

        assert_eq!(
            merged.subagents.executor,
            Some("bamboo_runtime".to_string())
        );
    }

    #[test]
    fn subagents_max_concurrent_null_clears_it_like_todays_lotus_ui() {
        // Existing-null-usage survey finding: Lotus's SystemSettingsConfigTab
        // already sends `{"subagents":{"max_concurrent": null}}` today (the
        // AntD InputNumber reports `null` when the field is cleared — see
        // `SystemSettingsConfigTab.tsx`), relying on serde_json's built-in
        // `Option<T>` + `null` -> `None` handling as an ACCIDENT of the old
        // "just overwrite the leaf with whatever JSON value arrived"
        // catch-all. The new delete-the-key implementation must reproduce
        // that exact end result (None) so this already-shipped Lotus flow
        // keeps working unchanged.
        let mut current = Config::default();
        current.subagents.max_concurrent = Some(4);

        let merged =
            merge_and_deserialize(&current, json!({ "subagents": { "max_concurrent": null } }));

        assert_eq!(merged.subagents.max_concurrent, None);
    }

    // ── #505: secret-field composition (intents recognize null as clear) ──

    #[test]
    fn provider_api_key_intents_treats_null_as_clear_intent() {
        let patch = json!({
            "providers": { "openai": { "api_key": null } },
            "provider_instances": { "uuid-1": { "api_key": null } }
        });
        let intents = provider_api_key_intents(patch.as_object().unwrap());
        assert!(
            intents.providers.contains("openai"),
            "a null api_key must register as an explicit clear intent, \
             same as an empty string — otherwise preserve_unpatched_provider_secrets \
             would resurrect the deleted key"
        );
        assert!(intents.provider_instances.contains("uuid-1"));
    }

    #[test]
    fn provider_api_key_intents_treats_whole_instance_null_as_delete_intent() {
        let patch = json!({ "provider_instances": { "uuid-1": null } });
        let intents = provider_api_key_intents(patch.as_object().unwrap());
        assert!(intents.provider_instances.contains("uuid-1"));
    }

    #[test]
    fn provider_api_key_intents_null_and_empty_string_are_equivalent_intents() {
        let null_patch = json!({ "providers": { "openai": { "api_key": null } } });
        let empty_patch = json!({ "providers": { "openai": { "api_key": "" } } });
        assert_eq!(
            provider_api_key_intents(null_patch.as_object().unwrap()),
            provider_api_key_intents(empty_patch.as_object().unwrap()),
            "null and \"\" must be recognized as the same clear intent"
        );
    }

    #[test]
    fn null_api_key_does_not_get_resurrected_by_preserve_unpatched_secrets() {
        // End-to-end composition proof for the precedence rule documented on
        // `deep_merge_json`: null-delete (step 2) must not be undone by the
        // unpatched-secret carry-forward (step 3). This mirrors
        // `preserve_unpatched_provider_secrets_respects_explicit_intents`
        // above, but with a `null` clear instead of `""`.
        let mut current = Config::default();
        current
            .provider_instances
            .insert("uuid-1".to_string(), instance("sk-old", None));

        let patch = json!({ "provider_instances": { "uuid-1": { "api_key": null } } });
        let intents = provider_api_key_intents(patch.as_object().unwrap());
        assert!(intents.provider_instances.contains("uuid-1"));

        let mut merged = merge_and_deserialize(&current, patch);
        preserve_unpatched_provider_secrets(&mut merged, &current, &intents);

        assert_eq!(
            merged.provider_instances["uuid-1"].api_key, "",
            "a null-delete of api_key must stick, not get resurrected from `current`"
        );
    }

    #[test]
    fn notification_secret_intents_treats_null_as_clear_intent() {
        let patch = json!({
            "notifications": { "ntfy": { "token": null }, "bark": { "device_key": null } }
        });
        let intents = notification_secret_intents(patch.as_object().unwrap());
        assert!(intents.ntfy_token);
        assert!(intents.bark_device_key);
    }

    #[test]
    fn connect_secret_intents_treats_null_as_clear_intent() {
        let patch = json!({
            "connect": { "platforms": [ { "type": "telegram", "token": null } ] }
        });
        let intents = connect_secret_intents(patch.as_object().unwrap());
        assert!(intents.token.contains(&0));
    }

    #[test]
    fn null_ntfy_token_clears_roundtripped_ciphertext_via_clear_intents() {
        // Full composition: neither plaintext nor legacy ciphertext survives
        // the JSON round-trip; the clear-intent pass remains idempotent for a
        // config loaded from older in-memory state.
        let mut current = Config::default();
        current.notifications.ntfy.token = Some("existing-token".to_string());
        current.notifications.ntfy.token_encrypted = Some("existing-ct".to_string());

        let patch = json!({ "notifications": { "ntfy": { "token": null } } });
        let intents = notification_secret_intents(patch.as_object().unwrap());
        assert!(intents.ntfy_token);

        let mut merged = merge_and_deserialize(&current, patch);
        assert!(
            merged.notifications.ntfy.token_encrypted.is_none(),
            "legacy notification ciphertext is no longer serialized through the merge"
        );

        clear_notification_ciphertext_for_explicit_clears(&mut merged, &intents);

        assert_eq!(merged.notifications.ntfy.token, None);
        assert!(
            merged.notifications.ntfy.token_encrypted.is_none(),
            "the clear-intent pass must drop the stale ciphertext so hydration can't refill it"
        );
    }

    #[test]
    fn whole_providers_null_wipes_everything_without_resurrecting_secrets() {
        // Coarse-grained blast radius: `{"providers": null}` deletes the
        // ENTIRE providers subtree (not just one provider's key). Confirms
        // this doesn't accidentally resurrect secrets: intents are empty
        // (the raw patch's "providers" is a scalar null, not an object, so
        // `provider_api_key_intents` finds no per-provider object to walk),
        // but `preserve_unpatched_provider_secrets`'s carry-forward only
        // fires when the MERGED side still has a `Some(..)` provider config
        // to carry into — which a whole-subtree wipe doesn't leave behind.
        let mut current = Config::default();
        current.providers.openai = Some(crate::OpenAIConfig {
            api_key: "sk-legacy-live".to_string(),
            ..Default::default()
        });

        let patch = json!({ "providers": null });
        let intents = provider_api_key_intents(patch.as_object().unwrap());
        assert!(intents.providers.is_empty());

        let mut merged = merge_and_deserialize(&current, patch);
        assert!(
            merged.providers.openai.is_none(),
            "the whole providers subtree must reset to default"
        );

        preserve_unpatched_provider_secrets(&mut merged, &current, &intents);

        assert!(
            merged.providers.openai.is_none(),
            "carry-forward must not resurrect a provider the patch wiped out entirely"
        );
    }
}
