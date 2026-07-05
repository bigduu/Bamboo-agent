use async_trait::async_trait;
use bamboo_agent_core::{Tool, ToolCtx, ToolError, ToolOutcome, ToolResult};
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::Path;

use super::file_change;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum CellType {
    Code,
    Markdown,
}

impl CellType {
    fn as_str(&self) -> &'static str {
        match self {
            CellType::Code => "code",
            CellType::Markdown => "markdown",
        }
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
enum EditMode {
    #[default]
    Replace,
    Insert,
    Delete,
}

#[derive(Debug, Deserialize)]
struct NotebookEditArgs {
    notebook_path: String,
    #[serde(default)]
    cell_id: Option<String>,
    new_source: String,
    #[serde(default)]
    cell_type: Option<CellType>,
    #[serde(default)]
    edit_mode: Option<EditMode>,
}

pub struct NotebookEditTool;

impl NotebookEditTool {
    pub fn new() -> Self {
        Self
    }

    fn source_to_lines(source: &str) -> Vec<Value> {
        if source.is_empty() {
            return vec![Value::String(String::new())];
        }

        source
            .lines()
            .map(|line| Value::String(format!("{}\n", line)))
            .collect()
    }

    fn find_cell_index(cells: &[Value], cell_id: Option<&str>) -> Option<usize> {
        let cell_id = cell_id?;

        cells.iter().position(|cell| {
            cell.get("id")
                .and_then(|value| value.as_str())
                .map(|value| value == cell_id)
                .unwrap_or(false)
        })
    }
}

impl Default for NotebookEditTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for NotebookEditTool {
    fn name(&self) -> &str {
        "NotebookEdit"
    }

    fn description(&self) -> &str {
        "Replace, insert, or delete a Jupyter notebook cell"
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "notebook_path": {
                    "type": "string",
                    "description": "Absolute path to the notebook file"
                },
                "cell_id": {
                    "type": "string",
                    "description": "Cell ID to edit"
                },
                "new_source": {
                    "type": "string",
                    "description": "New source content for the cell"
                },
                "cell_type": {
                    "type": "string",
                    "enum": ["code", "markdown"],
                    "description": "Cell type when inserting"
                },
                "edit_mode": {
                    "type": "string",
                    "enum": ["replace", "insert", "delete"],
                    "description": "Edit mode"
                }
            },
            "required": ["notebook_path", "new_source"],
            "additionalProperties": false
        })
    }

    async fn invoke(
        &self,
        args: serde_json::Value,
        _ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let parsed: NotebookEditArgs = serde_json::from_value(args).map_err(|e| {
            ToolError::InvalidArguments(format!("Invalid NotebookEdit args: {}", e))
        })?;

        let path = Path::new(parsed.notebook_path.trim());
        if !path.is_absolute() {
            return Err(ToolError::InvalidArguments(
                "notebook_path must be absolute".to_string(),
            ));
        }

        let content = tokio::fs::read_to_string(path)
            .await
            .map_err(|e| ToolError::Execution(format!("Failed to read notebook: {}", e)))?;
        let checkpoint = file_change::create_checkpoint(path, Some(content.as_bytes())).await?;

        let mut notebook: Value = serde_json::from_str(&content)
            .map_err(|e| ToolError::Execution(format!("Invalid notebook JSON: {}", e)))?;

        let cells = notebook
            .get_mut("cells")
            .and_then(|value| value.as_array_mut())
            .ok_or_else(|| ToolError::Execution("Notebook missing 'cells' array".to_string()))?;

        let edit_mode = parsed.edit_mode.unwrap_or_default();
        let cell_id = parsed
            .cell_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let target_index = Self::find_cell_index(cells, cell_id);

        match edit_mode {
            EditMode::Replace => {
                if cell_id.is_none() {
                    return Err(ToolError::InvalidArguments(
                        "cell_id is required when edit_mode=replace".to_string(),
                    ));
                }
                let idx = target_index.ok_or_else(|| {
                    ToolError::Execution("Target cell not found for replace".to_string())
                })?;
                if let Some(cell) = cells.get_mut(idx) {
                    cell["source"] = Value::Array(Self::source_to_lines(&parsed.new_source));
                }
            }
            EditMode::Insert => {
                let cell_type = parsed.cell_type.ok_or_else(|| {
                    ToolError::InvalidArguments(
                        "cell_type is required when edit_mode=insert".to_string(),
                    )
                })?;
                let new_cell = json!({
                    "id": uuid::Uuid::new_v4().to_string(),
                    "cell_type": cell_type.as_str(),
                    "metadata": {},
                    "source": Self::source_to_lines(&parsed.new_source),
                    "outputs": [],
                    "execution_count": serde_json::Value::Null,
                });

                if let Some(cell_id) = cell_id {
                    let idx = target_index.ok_or_else(|| {
                        ToolError::Execution(format!(
                            "Target cell '{}' not found for insert",
                            cell_id
                        ))
                    })?;
                    cells.insert(idx + 1, new_cell);
                } else {
                    cells.push(new_cell);
                }
            }
            EditMode::Delete => {
                if cell_id.is_none() {
                    return Err(ToolError::InvalidArguments(
                        "cell_id is required when edit_mode=delete".to_string(),
                    ));
                }
                let idx = target_index.ok_or_else(|| {
                    ToolError::Execution("Target cell not found for delete".to_string())
                })?;
                cells.remove(idx);
            }
        }

        let updated = serde_json::to_string_pretty(&notebook)
            .map_err(|e| ToolError::Execution(format!("Failed to serialize notebook: {}", e)))?;

        file_change::atomic_write_text(path, &updated).await?;

        let payload = file_change::build_file_change_payload(
            "NotebookEdit",
            path,
            format!("Notebook updated: {}", parsed.notebook_path),
            checkpoint,
            &content,
            &updated,
        );

        Ok(ToolOutcome::Completed(ToolResult {
            success: true,
            result: payload,
            display_preference: Some("Default".to_string()),
            images: Vec::new(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_notebook() -> serde_json::Value {
        json!({
            "cells": [
                {
                    "id": "cell-a",
                    "cell_type": "code",
                    "metadata": {},
                    "source": ["print('a')\n"],
                    "outputs": [],
                    "execution_count": 1
                },
                {
                    "id": "cell-b",
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": ["# title\n"]
                }
            ],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 5
        })
    }

    async fn write_notebook(path: &Path) {
        tokio::fs::write(
            path,
            serde_json::to_string_pretty(&sample_notebook()).unwrap(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn replace_requires_cell_id() {
        let file = tempfile::NamedTempFile::new().unwrap();
        write_notebook(file.path()).await;

        let tool = NotebookEditTool::new();
        let result = tool
            .invoke(
                json!({
                    "notebook_path": file.path(),
                    "edit_mode": "replace",
                    "new_source": "updated"
                }),
                ToolCtx::none("t"),
            )
            .await;

        assert!(matches!(result, Err(ToolError::InvalidArguments(msg)) if msg.contains("cell_id")));
    }

    #[tokio::test]
    async fn delete_requires_cell_id() {
        let file = tempfile::NamedTempFile::new().unwrap();
        write_notebook(file.path()).await;

        let tool = NotebookEditTool::new();
        let result = tool
            .invoke(
                json!({
                    "notebook_path": file.path(),
                    "edit_mode": "delete",
                    "new_source": ""
                }),
                ToolCtx::none("t"),
            )
            .await;

        assert!(matches!(result, Err(ToolError::InvalidArguments(msg)) if msg.contains("cell_id")));
    }

    #[tokio::test]
    async fn insert_without_cell_id_appends_cell() {
        let file = tempfile::NamedTempFile::new().unwrap();
        write_notebook(file.path()).await;

        let tool = NotebookEditTool::new();
        let out = tool
            .invoke(
                json!({
                    "notebook_path": file.path(),
                    "edit_mode": "insert",
                    "cell_type": "markdown",
                    "new_source": "appended cell"
                }),
                ToolCtx::none("t"),
            )
            .await
            .unwrap();
        let ToolOutcome::Completed(result) = out else {
            panic!("expected Completed")
        };
        assert!(result.success);

        let updated: Value =
            serde_json::from_str(&tokio::fs::read_to_string(file.path()).await.unwrap()).unwrap();
        let cells = updated["cells"].as_array().unwrap();
        assert_eq!(cells.len(), 3);
        let last = cells.last().unwrap();
        assert_eq!(last["cell_type"], "markdown");
        let source = last["source"].as_array().unwrap();
        assert_eq!(source[0], "appended cell\n");
    }

    #[tokio::test]
    async fn insert_with_unknown_cell_id_returns_error() {
        let file = tempfile::NamedTempFile::new().unwrap();
        write_notebook(file.path()).await;

        let tool = NotebookEditTool::new();
        let result = tool
            .invoke(
                json!({
                    "notebook_path": file.path(),
                    "edit_mode": "insert",
                    "cell_id": "does-not-exist",
                    "cell_type": "code",
                    "new_source": "print('x')"
                }),
                ToolCtx::none("t"),
            )
            .await;

        assert!(matches!(result, Err(ToolError::Execution(msg)) if msg.contains("not found")));
    }
}
