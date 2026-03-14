use crate::agent::skill::SkillDefinition;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize)]
pub(super) struct SkillListResponse {
    pub(super) skills: Vec<SkillDefinition>,
    pub(super) total: usize,
}

#[derive(Deserialize)]
pub struct ListSkillsQuery {
    pub(super) search: Option<String>,
    pub(super) refresh: Option<bool>,
}

#[derive(Serialize)]
pub(super) struct AvailableToolsResponse {
    pub(super) tools: Vec<String>,
}

#[derive(Serialize)]
pub(super) struct FilteredToolsResponse {
    pub(super) tools: Vec<OpenAiTool>,
}

#[derive(Serialize)]
pub(super) struct OpenAiTool {
    #[serde(rename = "type")]
    pub(super) tool_type: String,
    pub(super) function: OpenAiFunction,
}

#[derive(Serialize)]
pub(super) struct OpenAiFunction {
    pub(super) name: String,
    pub(super) description: String,
    pub(super) parameters: Value,
}

#[derive(Serialize)]
pub(super) struct AvailableWorkflowsResponse {
    pub(super) workflows: Vec<String>,
}

#[derive(Deserialize)]
pub struct FilteredToolsQuery {
    pub(super) session_id: Option<String>,
    pub(super) chat_id: Option<String>,
}
