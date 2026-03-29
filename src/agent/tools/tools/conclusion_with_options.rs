use crate::agent::core::tools::{Tool, ToolError, ToolResult};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;

const DEFAULT_OPTIONS: [&str; 2] = ["OK", "Need changes"];
const MAX_OPTIONS: usize = 6;
const MAX_LIST_ITEMS: usize = 8;

fn default_options() -> Vec<String> {
    DEFAULT_OPTIONS.iter().map(|s| (*s).to_string()).collect()
}

fn default_allow_custom() -> bool {
    true
}

fn normalize_text(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn normalize_optional_text(value: Option<String>) -> Option<String> {
    value.and_then(|raw| normalize_text(&raw))
}

fn normalize_text_list(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .filter_map(|value| normalize_text(&value))
        .take(MAX_LIST_ITEMS)
        .collect()
}

#[derive(Debug, Deserialize)]
struct ConclusionWithOptionsMermaidArgs {
    #[serde(default)]
    title: Option<String>,
    graph: String,
}

#[derive(Debug, Deserialize)]
struct ConclusionWithOptionsConclusionArgs {
    #[serde(default)]
    title: Option<String>,
    summary: String,
    #[serde(default)]
    key_points: Vec<String>,
    #[serde(default)]
    next_steps: Vec<String>,
    #[serde(default)]
    confidence: Option<String>,
    mermaid: ConclusionWithOptionsMermaidArgs,
}

#[derive(Debug, Deserialize)]
struct ConclusionWithOptionsArgs {
    question: String,
    #[serde(default)]
    options: Vec<String>,
    #[serde(default = "default_allow_custom")]
    allow_custom: bool,
    conclusion: ConclusionWithOptionsConclusionArgs,
}

/// Tool for asking user a question with multiple choice options
pub struct ConclusionWithOptionsTool;

impl ConclusionWithOptionsTool {
    /// Create a new ConclusionWithOptionsTool instance.
    ///
    /// This tool prompts the user with a question and provides multiple choice options.
    /// It supports custom answers when `allow_custom` is true.
    pub fn new() -> Self {
        Self
    }
}

impl Default for ConclusionWithOptionsTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for ConclusionWithOptionsTool {
    fn name(&self) -> &str {
        "conclusion_with_options"
    }

    fn description(&self) -> &str {
        "Ask the user a question with options and wait for the user to select or enter a custom answer. The `conclusion` object is required and must include both a summary and a Mermaid graph."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "question": {
                    "type": "string",
                    "description": "The question to display to the user"
                },
                "conclusion": {
                    "type": "object",
                    "description": "Structured wrap-up context shown before the confirmation question.",
                    "properties": {
                        "title": {
                            "type": "string",
                            "description": "Optional title for the conclusion block."
                        },
                        "summary": {
                            "type": "string",
                            "description": "Main summary text shown to the user."
                        },
                        "key_points": {
                            "type": "array",
                            "description": "Optional short bullet points supporting the summary.",
                            "items": { "type": "string" }
                        },
                        "next_steps": {
                            "type": "array",
                            "description": "Optional follow-up actions.",
                            "items": { "type": "string" }
                        },
                        "confidence": {
                            "type": "string",
                            "description": "Optional confidence label, for example high/medium/low."
                        },
                        "mermaid": {
                            "type": "object",
                            "description": "Mermaid chart payload rendered in the UI.",
                            "properties": {
                                "title": {
                                    "type": "string",
                                    "description": "Optional Mermaid section title."
                                },
                                "graph": {
                                    "type": "string",
                                    "description": "Mermaid graph definition text."
                                }
                            },
                            "required": ["graph"],
                            "additionalProperties": false
                        }
                    },
                    "required": ["summary", "mermaid"],
                    "additionalProperties": false
                },
                "options": {
                    "type": "array",
                    "description": "Candidate answer options (optional). If omitted or invalid, defaults to [\"OK\", \"Need changes\"].",
                    "items": {
                        "type": "string"
                    }
                },
                "allow_custom": {
                    "type": "boolean",
                    "description": "Whether to allow user to enter a custom answer (instead of selecting from options), default true",
                    "default": true
                }
            },
            "required": ["question", "conclusion"],
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        let parsed: ConclusionWithOptionsArgs = serde_json::from_value(args).map_err(|error| {
            ToolError::InvalidArguments(format!("Invalid conclusion_with_options args: {error}"))
        })?;
        let question = normalize_text(&parsed.question).ok_or_else(|| {
            ToolError::InvalidArguments("question must be a non-empty string".to_string())
        })?;
        let summary = normalize_text(&parsed.conclusion.summary).ok_or_else(|| {
            ToolError::InvalidArguments("conclusion.summary must be a non-empty string".to_string())
        })?;
        let mermaid_graph = normalize_text(&parsed.conclusion.mermaid.graph).ok_or_else(|| {
            ToolError::InvalidArguments(
                "conclusion.mermaid.graph must be a non-empty string".to_string(),
            )
        })?;

        let mut options = normalize_text_list(parsed.options);

        if options.len() < 2 {
            options = default_options();
        } else if options.len() > MAX_OPTIONS {
            options.truncate(MAX_OPTIONS);
        }

        let allow_custom = parsed.allow_custom;

        // Build the result payload that will be handled by the agent loop
        let result_payload = json!({
            "status": "awaiting_user_input",
            "type": "conclusion_with_options",
            "question": question,
            "options": options,
            "allow_custom": allow_custom,
            "conclusion": {
                "title": normalize_optional_text(parsed.conclusion.title).unwrap_or_else(|| "Conclusion".to_string()),
                "summary": summary,
                "key_points": normalize_text_list(parsed.conclusion.key_points),
                "next_steps": normalize_text_list(parsed.conclusion.next_steps),
                "confidence": normalize_optional_text(parsed.conclusion.confidence),
                "mermaid": {
                    "title": normalize_optional_text(parsed.conclusion.mermaid.title),
                    "graph": mermaid_graph
                }
            }
        });

        Ok(ToolResult {
            success: true,
            result: result_payload.to_string(),
            display_preference: Some("conclusion_with_options".to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_conclusion() -> serde_json::Value {
        json!({
            "summary": "Core changes are done and ready for confirmation.",
            "mermaid": {
                "graph": "graph TD\nA[Done]-->B[Confirm]"
            }
        })
    }

    #[test]
    fn test_conclusion_with_options_tool_name() {
        let tool = ConclusionWithOptionsTool::new();
        assert_eq!(tool.name(), "conclusion_with_options");
    }

    #[tokio::test]
    async fn test_execute_valid_input() {
        let tool = ConclusionWithOptionsTool::new();

        let result = tool
            .execute(json!({
                "question": "Please select deployment environment",
                "options": ["Development", "Testing", "Production"],
                "conclusion": minimal_conclusion()
            }))
            .await
            .expect("tool should execute successfully");

        assert!(result.success);
        assert_eq!(result.display_preference, Some("conclusion_with_options".to_string()));

        let parsed: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(parsed["status"], "awaiting_user_input");
        assert_eq!(parsed["question"], "Please select deployment environment");
        assert!(parsed["allow_custom"].as_bool().unwrap());
        assert_eq!(
            parsed["conclusion"]["summary"],
            "Core changes are done and ready for confirmation."
        );
        assert_eq!(
            parsed["conclusion"]["mermaid"]["graph"],
            "graph TD\nA[Done]-->B[Confirm]"
        );
    }

    #[tokio::test]
    async fn test_execute_accepts_two_options() {
        let tool = ConclusionWithOptionsTool::new();

        let result = tool
            .execute(json!({
                "question": "Please confirm?",
                "options": ["Yes", "No"],
                "conclusion": minimal_conclusion()
            }))
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_with_too_few_options_uses_defaults() {
        let tool = ConclusionWithOptionsTool::new();

        let result = tool
            .execute(json!({
                "question": "Please select?",
                "options": ["Only one option"],
                "conclusion": minimal_conclusion()
            }))
            .await
            .expect("tool should execute with fallback defaults");

        let parsed: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(parsed["options"], json!(["OK", "Need changes"]));
    }

    #[tokio::test]
    async fn test_execute_without_options_uses_defaults() {
        let tool = ConclusionWithOptionsTool::new();

        let result = tool
            .execute(json!({
                "question": "Any other requests before I finish?",
                "conclusion": minimal_conclusion()
            }))
            .await
            .expect("tool should execute without options");

        let parsed: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(parsed["options"], json!(["OK", "Need changes"]));
    }

    #[tokio::test]
    async fn test_execute_truncates_options_to_six_items() {
        let tool = ConclusionWithOptionsTool::new();

        let result = tool
            .execute(json!({
                "question": "Please pick one",
                "options": ["1", "2", "3", "4", "5", "6", "7"],
                "conclusion": minimal_conclusion()
            }))
            .await
            .expect("tool should execute and truncate options");

        let parsed: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(parsed["options"], json!(["1", "2", "3", "4", "5", "6"]));
    }

    #[tokio::test]
    async fn test_execute_with_allow_custom_false() {
        let tool = ConclusionWithOptionsTool::new();

        let result = tool
            .execute(json!({
                "question": "Please confirm",
                "options": ["Yes", "No", "Cancel"],
                "allow_custom": false,
                "conclusion": minimal_conclusion()
            }))
            .await
            .expect("tool should execute");

        let parsed: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        assert!(!parsed["allow_custom"].as_bool().unwrap());
    }

    #[tokio::test]
    async fn test_execute_rejects_missing_conclusion() {
        let tool = ConclusionWithOptionsTool::new();

        let result = tool
            .execute(json!({
                "question": "Please confirm"
            }))
            .await;

        assert!(result.is_err());
        let error = result.expect_err("expected invalid args");
        if let ToolError::InvalidArguments(message) = error {
            assert!(message.contains("conclusion"));
        } else {
            panic!("expected invalid arguments");
        }
    }

    #[tokio::test]
    async fn test_execute_rejects_empty_mermaid_graph() {
        let tool = ConclusionWithOptionsTool::new();

        let result = tool
            .execute(json!({
                "question": "Please confirm",
                "conclusion": {
                    "summary": "Summary",
                    "mermaid": { "graph": "   " }
                }
            }))
            .await;

        assert!(result.is_err());
        let error = result.expect_err("expected invalid args");
        if let ToolError::InvalidArguments(message) = error {
            assert!(message.contains("conclusion.mermaid.graph"));
        } else {
            panic!("expected invalid arguments");
        }
    }
}
