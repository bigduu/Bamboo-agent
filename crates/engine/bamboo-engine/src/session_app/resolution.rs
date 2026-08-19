//! Single source of truth for resolving the per-session execution "dance":
//! provider-name derivation + provider_type + global auxiliary models +
//! gold config, mapped into the snapshot structs the agent loop consumes.
//!
//! These resolution rules are load-bearing invariants — the resolved provider
//! name (session model ref, falling back to the effective default provider) and the global
//! (never session-bound) auxiliary model fallback. They were previously
//! hand-rolled at every spawn/resume site; centralizing them here keeps the
//! rules consistent by construction. The builder is pure orchestration over the
//! existing atomic helpers, so behavior is identical to the hand-rolled copies.

use std::sync::Arc;

use bamboo_agent_core::Session;
use bamboo_llm::{Config, ProviderRegistry};

use crate::config::GoldConfig;
use crate::model_areas::resolve_global_area_models;
use crate::model_config_helper::{
    resolve_gold_config, resolve_image_fallback, resolve_provider_type, GOLD_CONFIG_METADATA_KEY,
};
use crate::session_app::provider_model::session_effective_model_ref;
use crate::session_app::types::ResumeConfigSnapshot;

/// Resolve the [`ResumeConfigSnapshot`] for a session from a cloned `Config`
/// snapshot + provider registry.
///
/// The auxiliary (fast / background / summarization) models are global —
/// config-derived, keyed to the session's resolved provider only for fallback,
/// never read from the session itself. `gold_config_override`, when `Some`,
/// wins over the session-metadata-derived gold config.
///
/// Pure (no lock): callers clone their `Config` first, which works for both the
/// async tokio lock and the cached `std::RwLock` snapshot.
pub fn resolve_resume_config_snapshot(
    config: &Config,
    registry: &Arc<ProviderRegistry>,
    session: &Session,
    gold_config_override: Option<GoldConfig>,
) -> ResumeConfigSnapshot {
    let resolved_provider_name = session_effective_model_ref(session)
        .map(|model_ref| model_ref.provider)
        .unwrap_or_else(|| config.effective_default_provider().to_string());
    let resolved_provider_type = resolve_provider_type(config, &resolved_provider_name, registry);
    // Auxiliary models are global (config-derived), never session-bound.
    let areas = resolve_global_area_models(config, &resolved_provider_name, registry);

    ResumeConfigSnapshot {
        provider_name: resolved_provider_name,
        provider_type: resolved_provider_type,
        fast_model: areas.fast.as_ref().map(|model| model.model_name.clone()),
        fast_model_ref: areas.fast_ref.clone(),
        background_model: areas
            .background
            .as_ref()
            .map(|model| model.model_name.clone()),
        background_model_ref: areas.background_ref.clone(),
        background_model_provider: areas.background.map(|model| model.provider),
        summarization_model: areas
            .summarization
            .as_ref()
            .map(|model| model.model_name.clone()),
        summarization_model_ref: areas.summarization_ref.clone(),
        summarization_model_provider: areas.summarization.map(|model| model.provider),
        disabled_tools: config.disabled_tool_names(),
        disabled_skill_ids: config.disabled_skill_ids(),
        image_fallback: resolve_image_fallback(config).ok().flatten(),
        gold_config: gold_config_override.or_else(|| {
            resolve_gold_config(
                config,
                session
                    .metadata
                    .get(GOLD_CONFIG_METADATA_KEY)
                    .map(String::as_str),
            )
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn session_without_model_ref_resumes_on_effective_default_instance() {
        let mut config = Config::default();
        config.provider = "openai".to_string();
        config.provider_instances.insert(
            "work".to_string(),
            serde_json::from_value(serde_json::json!({
                "provider_type": "anthropic",
                "model": "claude-instance",
                "enabled": true
            }))
            .unwrap(),
        );
        config.default_provider_instance = Some("work".to_string());
        let registry = Arc::new(ProviderRegistry::new(HashMap::new(), "work".to_string()));
        let session = Session::new("resume-instance-default", "claude-instance");

        let resolved = resolve_resume_config_snapshot(&config, &registry, &session, None);

        assert_eq!(resolved.provider_name, "work");
        assert_eq!(resolved.provider_type.as_deref(), Some("anthropic"));
    }
}
