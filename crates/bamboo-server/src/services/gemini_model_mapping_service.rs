use bamboo_infrastructure::model_mapping::GeminiModelMapping;

/// Resolve a Gemini model name to the actual backend model.
///
/// This no longer performs any file I/O. The mapping should be sourced from the
/// unified `Config` (`config.gemini_model_mapping`) and passed in.
pub fn resolve_model(mapping: &GeminiModelMapping, gemini_model: &str) -> ModelResolution {
    tracing::info!(
        "Resolving Gemini model '{}', available mappings: {:?}",
        gemini_model,
        mapping.mappings
    );

    // Match by keyword in model name (case-insensitive)
    let model_lower = gemini_model.to_lowercase();

    // Extract model type from Gemini model names like:
    // - gemini-pro, gemini-ultra, gemini-1.5-pro, gemini-1.5-flash, etc.
    let model_type = if model_lower.contains("ultra") {
        "ultra"
    } else if model_lower.contains("1.5") && model_lower.contains("flash") {
        "flash-1.5"
    } else if model_lower.contains("1.5") && model_lower.contains("pro") {
        "pro-1.5"
    } else if model_lower.contains("flash") {
        "flash"
    } else if model_lower.contains("pro") {
        "pro"
    } else {
        tracing::warn!(
            "No Gemini model mapping found for '{}', falling back to default model",
            gemini_model
        );
        return ModelResolution {
            mapped_model: String::new(),
            response_model: gemini_model.to_string(),
        };
    };

    if let Some(mapped) = mapping
        .mappings
        .get(model_type)
        .filter(|value| !value.trim().is_empty())
    {
        tracing::info!(
            "Model '{}' (type: {}) mapped to '{}'",
            gemini_model,
            model_type,
            mapped
        );
        return ModelResolution {
            mapped_model: mapped.to_string(),
            response_model: gemini_model.to_string(),
        };
    }

    tracing::warn!(
        "No mapping configured for model type '{}', falling back to default model",
        model_type
    );

    ModelResolution {
        mapped_model: String::new(),
        response_model: gemini_model.to_string(),
    }
}

#[derive(Clone)]
pub struct ModelResolution {
    pub mapped_model: String,
    pub response_model: String,
}
