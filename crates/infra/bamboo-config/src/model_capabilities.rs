//! Per-model Vision overrides, scoped to one provider instance.
use crate::ProviderInstanceConfig;
use std::collections::BTreeMap;

/// Missing/null overrides inherit provider metadata or legacy behavior.
pub fn model_vision_overrides(
    instance: &ProviderInstanceConfig,
) -> Result<BTreeMap<String, bool>, String> {
    let Some(value) = instance
        .extra
        .get("model_capabilities")
        .filter(|v| !v.is_null())
    else {
        return Ok(BTreeMap::new());
    };
    let entries = value
        .as_object()
        .ok_or("model_capabilities must be an object keyed by model ID")?;
    let mut overrides = BTreeMap::new();
    for (model, capabilities) in entries {
        if model.trim().is_empty() || model.trim() != model {
            return Err("model_capabilities keys must be non-empty, unpadded model IDs".into());
        }
        if capabilities.is_null() {
            continue;
        }
        let capabilities = capabilities
            .as_object()
            .ok_or("model_capabilities entries must be objects")?;
        if let Some(vision) = capabilities.get("supports_vision").filter(|v| !v.is_null()) {
            overrides.insert(
                model.clone(),
                vision
                    .as_bool()
                    .ok_or("supports_vision must be a boolean or null (inherit)")?,
            );
        }
    }
    Ok(overrides)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn vision_overrides_are_per_model_and_missing_is_not_false() {
        let mut instance: ProviderInstanceConfig =
            serde_json::from_value(json!({"provider_type":"openai"})).unwrap();
        assert!(model_vision_overrides(&instance).unwrap().is_empty());
        instance.extra.insert("model_capabilities".into(), json!({"image":{"supports_vision":true},"text":{"supports_vision":false},"old":{"supports_vision":null}}));
        assert_eq!(
            model_vision_overrides(&instance).unwrap(),
            BTreeMap::from([("image".into(), true), ("text".into(), false)])
        );
    }
    #[test]
    fn invalid_vision_config_is_rejected_without_coercion() {
        for value in [
            json!([]),
            json!({" ":{}}),
            json!({" image":{}}),
            json!({"model":true}),
            json!({"model":{"supports_vision":"false"}}),
        ] {
            let instance = serde_json::from_value(
                json!({"provider_type":"openai","model_capabilities":value}),
            )
            .unwrap();
            assert!(model_vision_overrides(&instance).is_err());
        }
    }
    #[test]
    fn vision_capabilities_survive_cold_load_without_cross_instance_leakage() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = crate::Config::default();
        for (id, image) in [("work", true), ("personal", false)] {
            config.provider_instances.insert(id.into(), serde_json::from_value(json!({
                "provider_type":"openai", "model":"same-name", "runtime_models":["same-name","text"],
                "model_capabilities":{"same-name":{"supports_vision":image},"text":{"supports_vision":false}}
            })).unwrap());
        }
        config.default_provider_instance = Some("work".into());
        config.save_to_dir(temp.path().to_owned()).unwrap();
        let loaded = crate::Config::from_data_dir_without_publish(Some(temp.path().to_owned()));
        assert_eq!(
            model_vision_overrides(&loaded.provider_instances["work"]).unwrap()["same-name"],
            true
        );
        assert_eq!(
            model_vision_overrides(&loaded.provider_instances["personal"]).unwrap()["same-name"],
            false
        );
        assert_eq!(
            model_vision_overrides(&loaded.provider_instances["work"]).unwrap()["text"],
            false
        );
    }
}
