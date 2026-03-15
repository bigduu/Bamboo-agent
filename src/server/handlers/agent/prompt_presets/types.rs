use serde::{Deserialize, Serialize};

pub(super) const DEFAULT_PRESET_ID: &str = "general_assistant";
pub(super) const DEFAULT_PRESET_NAME: &str = "Bodhi";
pub(super) const DEFAULT_PRESET_DESCRIPTION: &str = "System prompt configured in Bamboo backend.";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct StoredPromptPreset {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub content: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct PromptPresetStore {
    #[serde(default)]
    pub prompts: Vec<StoredPromptPreset>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PromptPresetItem {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub content: String,
    #[serde(default)]
    pub is_default: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PromptPresetListResponse {
    pub prompts: Vec<PromptPresetItem>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PromptPresetResponse {
    pub prompt: PromptPresetItem,
}

#[derive(Debug, Deserialize)]
pub struct CreatePromptPresetRequest {
    #[serde(default)]
    pub id: Option<String>,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub content: String,
}

#[derive(Debug, Deserialize)]
pub struct PatchPromptPresetRequest {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub content: Option<String>,
}
