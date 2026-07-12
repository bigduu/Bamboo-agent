use serde::{Deserialize, Serialize};

pub(super) const DEFAULT_PRESET_ID: &str = "general_assistant";
pub(super) const DEFAULT_PRESET_NAME: &str = "Bodhi";
pub(super) const DEFAULT_PRESET_DESCRIPTION: &str = "System prompt configured in Bamboo backend.";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct StoredPromptPreset {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub content: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PromptPresetStore {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stored_prompt_preset_full() {
        let json = r#"{"id":"test-id","name":"Test","description":"A test preset","content":"Test content"}"#;
        let preset: StoredPromptPreset = serde_json::from_str(json).unwrap();

        assert_eq!(preset.id, "test-id");
        assert_eq!(preset.name, "Test");
        assert_eq!(preset.description, Some("A test preset".to_string()));
        assert_eq!(preset.content, "Test content");
    }

    #[test]
    fn test_stored_prompt_preset_no_description() {
        let json = r#"{"id":"test-id","name":"Test","content":"Content"}"#;
        let preset: StoredPromptPreset = serde_json::from_str(json).unwrap();

        assert_eq!(preset.id, "test-id");
        assert!(preset.description.is_none());
    }

    #[test]
    fn test_stored_prompt_preset_serialization() {
        let preset = StoredPromptPreset {
            id: "id-123".to_string(),
            name: "My Preset".to_string(),
            description: Some("Description".to_string()),
            content: "Content here".to_string(),
        };

        let json = serde_json::to_string(&preset).unwrap();
        assert!(json.contains("\"id\":\"id-123\""));
        assert!(json.contains("\"My Preset\""));
    }

    #[test]
    fn test_stored_prompt_preset_clone() {
        let preset = StoredPromptPreset {
            id: "id".to_string(),
            name: "Name".to_string(),
            description: None,
            content: "Content".to_string(),
        };

        let cloned = preset.clone();
        assert_eq!(preset.id, cloned.id);
        assert_eq!(preset.name, cloned.name);
    }

    #[test]
    fn test_stored_prompt_preset_eq() {
        let preset1 = StoredPromptPreset {
            id: "id".to_string(),
            name: "Name".to_string(),
            description: Some("Desc".to_string()),
            content: "Content".to_string(),
        };

        let preset2 = StoredPromptPreset {
            id: "id".to_string(),
            name: "Name".to_string(),
            description: Some("Desc".to_string()),
            content: "Content".to_string(),
        };

        assert_eq!(preset1, preset2);
    }

    #[test]
    fn test_prompt_preset_store_default() {
        let store = PromptPresetStore::default();
        assert!(store.prompts.is_empty());
    }

    #[test]
    fn test_prompt_preset_store_with_prompts() {
        let json = r#"{"prompts":[{"id":"1","name":"Test","content":"Content"}]}"#;
        let store: PromptPresetStore = serde_json::from_str(json).unwrap();

        assert_eq!(store.prompts.len(), 1);
        assert_eq!(store.prompts[0].id, "1");
    }

    #[test]
    fn test_prompt_preset_item_full() {
        let json = r#"{"id":"id-1","name":"Test","description":"Desc","content":"Content","is_default":true}"#;
        let item: PromptPresetItem = serde_json::from_str(json).unwrap();

        assert_eq!(item.id, "id-1");
        assert_eq!(item.name, "Test");
        assert_eq!(item.description, Some("Desc".to_string()));
        assert_eq!(item.content, "Content");
        assert!(item.is_default);
    }

    #[test]
    fn test_prompt_preset_item_minimal() {
        let json = r#"{"id":"id-1","name":"Test","content":"Content"}"#;
        let item: PromptPresetItem = serde_json::from_str(json).unwrap();

        assert_eq!(item.id, "id-1");
        assert!(item.description.is_none());
        assert!(!item.is_default);
    }

    #[test]
    fn test_prompt_preset_item_serialization_skip_none() {
        let item = PromptPresetItem {
            id: "id".to_string(),
            name: "Name".to_string(),
            description: None,
            content: "Content".to_string(),
            is_default: false,
        };

        let json = serde_json::to_string(&item).unwrap();
        assert!(!json.contains("\"description\""));
    }

    #[test]
    fn test_prompt_preset_item_serialization_with_description() {
        let item = PromptPresetItem {
            id: "id".to_string(),
            name: "Name".to_string(),
            description: Some("Desc".to_string()),
            content: "Content".to_string(),
            is_default: true,
        };

        let json = serde_json::to_string(&item).unwrap();
        assert!(json.contains("\"description\""));
        assert!(json.contains("\"is_default\":true"));
    }

    #[test]
    fn test_prompt_preset_list_response() {
        let response = PromptPresetListResponse { prompts: vec![] };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"prompts\":[]"));
    }

    #[test]
    fn test_prompt_preset_list_response_with_items() {
        let item = PromptPresetItem {
            id: "id-1".to_string(),
            name: "Test".to_string(),
            description: None,
            content: "Content".to_string(),
            is_default: false,
        };

        let response = PromptPresetListResponse {
            prompts: vec![item],
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"id-1\""));
        assert!(json.contains("\"Test\""));
    }

    #[test]
    fn test_prompt_preset_response() {
        let item = PromptPresetItem {
            id: "preset-id".to_string(),
            name: "My Preset".to_string(),
            description: Some("A preset".to_string()),
            content: "System prompt".to_string(),
            is_default: false,
        };

        let response = PromptPresetResponse { prompt: item };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"prompt\""));
        assert!(json.contains("\"preset-id\""));
    }

    #[test]
    fn test_create_prompt_preset_request_with_id() {
        let json = r#"{"id":"custom-id","name":"Test","description":"Desc","content":"Content"}"#;
        let req: CreatePromptPresetRequest = serde_json::from_str(json).unwrap();

        assert_eq!(req.id, Some("custom-id".to_string()));
        assert_eq!(req.name, "Test");
        assert_eq!(req.description, Some("Desc".to_string()));
        assert_eq!(req.content, "Content");
    }

    #[test]
    fn test_create_prompt_preset_request_without_id() {
        let json = r#"{"name":"Test","content":"Content"}"#;
        let req: CreatePromptPresetRequest = serde_json::from_str(json).unwrap();

        assert!(req.id.is_none());
        assert!(req.description.is_none());
    }

    #[test]
    fn test_patch_prompt_preset_request_partial() {
        let json = r#"{"name":"New Name"}"#;
        let req: PatchPromptPresetRequest = serde_json::from_str(json).unwrap();

        assert_eq!(req.name, Some("New Name".to_string()));
        assert!(req.description.is_none());
        assert!(req.content.is_none());
    }

    #[test]
    fn test_patch_prompt_preset_request_multiple() {
        let json = r#"{"name":"Name","description":"New Desc"}"#;
        let req: PatchPromptPresetRequest = serde_json::from_str(json).unwrap();

        assert_eq!(req.name, Some("Name".to_string()));
        assert_eq!(req.description, Some("New Desc".to_string()));
        assert!(req.content.is_none());
    }

    #[test]
    fn test_patch_prompt_preset_request_empty() {
        let json = r#"{}"#;
        let req: PatchPromptPresetRequest = serde_json::from_str(json).unwrap();

        assert!(req.name.is_none());
        assert!(req.description.is_none());
        assert!(req.content.is_none());
    }

    #[test]
    fn test_create_prompt_preset_request_debug() {
        let req = CreatePromptPresetRequest {
            id: Some("id".to_string()),
            name: "Test".to_string(),
            description: None,
            content: "Content".to_string(),
        };

        let debug_str = format!("{:?}", req);
        assert!(debug_str.contains("CreatePromptPresetRequest"));
    }

    #[test]
    fn test_patch_prompt_preset_request_debug() {
        let req = PatchPromptPresetRequest {
            name: Some("Test".to_string()),
            description: None,
            content: None,
        };

        let debug_str = format!("{:?}", req);
        assert!(debug_str.contains("PatchPromptPresetRequest"));
    }

    #[test]
    fn test_stored_prompt_preset_debug() {
        let preset = StoredPromptPreset {
            id: "id".to_string(),
            name: "Name".to_string(),
            description: None,
            content: "Content".to_string(),
        };

        let debug_str = format!("{:?}", preset);
        assert!(debug_str.contains("StoredPromptPreset"));
    }

    #[test]
    fn test_prompt_preset_item_debug() {
        let item = PromptPresetItem {
            id: "id".to_string(),
            name: "Name".to_string(),
            description: None,
            content: "Content".to_string(),
            is_default: false,
        };

        let debug_str = format!("{:?}", item);
        assert!(debug_str.contains("PromptPresetItem"));
    }
}
