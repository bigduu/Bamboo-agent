use std::collections::HashSet;

use actix_web::{web, HttpResponse, Result};

use crate::server::app_state::AppState;

use super::storage::{
    ensure_unique_preset_id, load_store, normalize_content, normalize_name,
    normalize_optional_description, read_default_prompt_content, save_store, slugify_name,
    store_file_path, to_custom_item, validate_preset_id,
};
use super::types::{
    CreatePromptPresetRequest, PatchPromptPresetRequest, PromptPresetItem,
    PromptPresetListResponse, PromptPresetResponse, StoredPromptPreset, DEFAULT_PRESET_DESCRIPTION,
    DEFAULT_PRESET_ID, DEFAULT_PRESET_NAME,
};

fn default_preset_item() -> PromptPresetItem {
    PromptPresetItem {
        id: DEFAULT_PRESET_ID.to_string(),
        name: DEFAULT_PRESET_NAME.to_string(),
        description: Some(DEFAULT_PRESET_DESCRIPTION.to_string()),
        content: read_default_prompt_content(),
        is_default: true,
    }
}

fn bad_request(message: &str) -> HttpResponse {
    HttpResponse::BadRequest().json(serde_json::json!({
        "error": message
    }))
}

fn not_found(preset_id: &str) -> HttpResponse {
    HttpResponse::NotFound().json(serde_json::json!({
        "error": "Prompt preset not found",
        "preset_id": preset_id
    }))
}

fn normalize_requested_id(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|candidate| !candidate.is_empty())
        .map(str::to_ascii_lowercase)
}

/// `GET /api/v1/prompt-presets`
pub async fn list_prompt_presets(state: web::Data<AppState>) -> Result<HttpResponse> {
    let store_path = store_file_path(&state.app_data_dir);
    let store = load_store(&store_path).await.map_err(|error| {
        actix_web::error::ErrorInternalServerError(format!(
            "Failed to load prompt preset store: {error}"
        ))
    })?;

    let mut prompts = Vec::with_capacity(store.prompts.len() + 1);
    prompts.push(default_preset_item());
    prompts.extend(store.prompts.iter().map(to_custom_item));

    Ok(HttpResponse::Ok().json(PromptPresetListResponse { prompts }))
}

/// `POST /api/v1/prompt-presets`
pub async fn create_prompt_preset(
    state: web::Data<AppState>,
    req: web::Json<CreatePromptPresetRequest>,
) -> Result<HttpResponse> {
    let Some(name) = normalize_name(&req.name) else {
        return Ok(bad_request("Prompt name cannot be empty"));
    };
    let Some(content) = normalize_content(&req.content) else {
        return Ok(bad_request("Prompt content cannot be empty"));
    };
    let description = normalize_optional_description(req.description.as_deref());

    let store_path = store_file_path(&state.app_data_dir);
    let mut store = load_store(&store_path).await.map_err(|error| {
        actix_web::error::ErrorInternalServerError(format!(
            "Failed to load prompt preset store: {error}"
        ))
    })?;

    let custom_ids: HashSet<String> = store
        .prompts
        .iter()
        .map(|preset| preset.id.clone())
        .collect();
    let mut all_ids = custom_ids.clone();
    all_ids.insert(DEFAULT_PRESET_ID.to_string());

    let preset_id = match normalize_requested_id(req.id.as_deref()) {
        Some(candidate) => {
            if candidate == DEFAULT_PRESET_ID {
                return Ok(bad_request(
                    "Preset id 'general_assistant' is reserved for the default prompt",
                ));
            }
            if !validate_preset_id(&candidate) {
                return Ok(bad_request(
                    "Invalid preset id. Use lowercase letters, numbers, or underscores only",
                ));
            }
            if custom_ids.contains(&candidate) {
                return Ok(bad_request("Prompt preset id already exists"));
            }
            candidate
        }
        None => {
            let base_id = slugify_name(&name);
            ensure_unique_preset_id(&base_id, &all_ids)
        }
    };

    let created = StoredPromptPreset {
        id: preset_id,
        name,
        description,
        content,
    };
    store.prompts.push(created.clone());

    save_store(&store_path, &store).await.map_err(|error| {
        actix_web::error::ErrorInternalServerError(format!(
            "Failed to persist prompt preset store: {error}"
        ))
    })?;

    Ok(HttpResponse::Created().json(PromptPresetResponse {
        prompt: to_custom_item(&created),
    }))
}

/// `PATCH /api/v1/prompt-presets/{preset_id}`
pub async fn patch_prompt_preset(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: web::Json<PatchPromptPresetRequest>,
) -> Result<HttpResponse> {
    let preset_id = path.into_inner();
    if preset_id == DEFAULT_PRESET_ID {
        return Ok(bad_request("Default prompt preset is read-only"));
    }

    let store_path = store_file_path(&state.app_data_dir);
    let mut store = load_store(&store_path).await.map_err(|error| {
        actix_web::error::ErrorInternalServerError(format!(
            "Failed to load prompt preset store: {error}"
        ))
    })?;

    let Some(preset) = store
        .prompts
        .iter_mut()
        .find(|preset| preset.id == preset_id)
    else {
        return Ok(not_found(&preset_id));
    };

    if let Some(name) = req.name.as_deref() {
        let Some(normalized) = normalize_name(name) else {
            return Ok(bad_request("Prompt name cannot be empty"));
        };
        preset.name = normalized;
    }

    if let Some(content) = req.content.as_deref() {
        let Some(normalized) = normalize_content(content) else {
            return Ok(bad_request("Prompt content cannot be empty"));
        };
        preset.content = normalized;
    }

    if let Some(description) = req.description.as_deref() {
        preset.description = normalize_optional_description(Some(description));
    }

    let updated = preset.clone();

    save_store(&store_path, &store).await.map_err(|error| {
        actix_web::error::ErrorInternalServerError(format!(
            "Failed to persist prompt preset store: {error}"
        ))
    })?;

    Ok(HttpResponse::Ok().json(PromptPresetResponse {
        prompt: to_custom_item(&updated),
    }))
}

/// `DELETE /api/v1/prompt-presets/{preset_id}`
pub async fn delete_prompt_preset(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let preset_id = path.into_inner();
    if preset_id == DEFAULT_PRESET_ID {
        return Ok(bad_request("Default prompt preset is read-only"));
    }

    let store_path = store_file_path(&state.app_data_dir);
    let mut store = load_store(&store_path).await.map_err(|error| {
        actix_web::error::ErrorInternalServerError(format!(
            "Failed to load prompt preset store: {error}"
        ))
    })?;

    let initial_len = store.prompts.len();
    store.prompts.retain(|preset| preset.id != preset_id);
    if store.prompts.len() == initial_len {
        return Ok(not_found(&preset_id));
    }

    save_store(&store_path, &store).await.map_err(|error| {
        actix_web::error::ErrorInternalServerError(format!(
            "Failed to persist prompt preset store: {error}"
        ))
    })?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "preset_id": preset_id
    })))
}

#[cfg(test)]
mod tests {
    use super::normalize_requested_id;

    #[test]
    fn normalize_requested_id_trims_and_lowercases() {
        assert_eq!(
            normalize_requested_id(Some("  My_PROMPT  ")).as_deref(),
            Some("my_prompt")
        );
    }

    #[test]
    fn normalize_requested_id_drops_empty_values() {
        assert_eq!(normalize_requested_id(Some("   ")), None);
        assert_eq!(normalize_requested_id(None), None);
    }
}
