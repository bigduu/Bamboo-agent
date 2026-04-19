use bamboo_infrastructure::model_mapping::AnthropicModelMapping;

pub(super) struct ModelResolution {
    pub(super) mapped_model: String,
    pub(super) response_model: String,
}

pub(super) fn resolve_model(mapping: &AnthropicModelMapping, model: &str) -> ModelResolution {
    tracing::info!(
        "Resolving model '{}', available mappings: {:?}",
        model,
        mapping.mappings
    );

    // Match by keyword in model name (case-insensitive)
    let model_lower = model.to_lowercase();

    let model_type = if model_lower.contains("opus") {
        "opus"
    } else if model_lower.contains("sonnet") {
        "sonnet"
    } else if model_lower.contains("haiku") {
        "haiku"
    } else {
        tracing::warn!(
            "No Anthropic model mapping found for '{}', falling back to default model",
            model
        );
        return ModelResolution {
            mapped_model: String::new(),
            response_model: model.to_string(),
        };
    };

    if let Some(mapped) = mapping
        .mappings
        .get(model_type)
        .filter(|value| !value.trim().is_empty())
    {
        tracing::info!(
            "Model '{}' (type: {}) mapped to '{}'",
            model,
            model_type,
            mapped
        );
        return ModelResolution {
            mapped_model: mapped.to_string(),
            response_model: model.to_string(),
        };
    }

    tracing::warn!(
        "No mapping configured for model type '{}', falling back to default model",
        model_type
    );

    ModelResolution {
        mapped_model: String::new(),
        response_model: model.to_string(),
    }
}
