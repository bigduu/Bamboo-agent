//! Root-agent browser tool. The target chat session comes only from ToolCtx.

use std::sync::Arc;

use async_trait::async_trait;
use bamboo_agent_core::tools::{
    Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult, ToolResultImage,
};
use serde_json::{json, Value};

use crate::browser::{BrowserError, BrowserManager};

pub struct BrowserTool {
    browser: Arc<BrowserManager>,
}

impl BrowserTool {
    pub fn new(browser: Arc<BrowserManager>) -> Self {
        Self { browser }
    }
}

fn text_arg<'a>(args: &'a Value, name: &str) -> Result<&'a str, ToolError> {
    args.get(name)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ToolError::InvalidArguments(format!("browser requires nonempty {name}")))
}

fn browser_error(error: BrowserError) -> ToolError {
    ToolError::Execution(error.to_string())
}

#[async_trait]
impl Tool for BrowserTool {
    fn name(&self) -> &str {
        "browser"
    }

    fn description(&self) -> &str {
        "Operate the browser page shared with this chat's right workbench. Read its DOM snapshot or screenshot, navigate, click, fill, press, or scroll. The page belongs to the current chat session; no session ID argument is accepted. Take a snapshot and pass its page_epoch before acting on a previously seen page."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type":"object",
            "properties": {
                "action":{"type":"string","enum":["navigate","snapshot","click","fill","press","scroll","screenshot"]},
                "url":{"type":"string","description":"HTTP(S) URL for navigate"},
                "selector":{"type":"string","description":"CSS selector for click, fill, or optional press"},
                "text":{"type":"string","description":"Text for fill"},
                "key":{"type":"string","description":"Keyboard key for press, e.g. Enter"},
                "x":{"type":"number","description":"CSS viewport x for scroll"},
                "y":{"type":"number","description":"CSS viewport y for scroll"},
                "delta_x":{"type":"number"},
                "delta_y":{"type":"number"},
                "expected_epoch":{"type":"integer","description":"Required for click/fill/press/scroll: page_epoch from a prior snapshot; rejects actions if the page navigated"},
                "include_html":{"type":"boolean","description":"Include bounded raw HTML in snapshot output"}
            },
            "required":["action"],
            "additionalProperties":false
        })
    }

    fn classify(&self, args: &Value) -> ToolClass {
        if matches!(
            args.get("action").and_then(Value::as_str),
            Some("snapshot" | "screenshot")
        ) {
            ToolClass::READONLY_PARALLEL
        } else {
            ToolClass::MUTATING_SERIAL
        }
    }

    async fn invoke(&self, args: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        if args.get("session_id").is_some() {
            return Err(ToolError::InvalidArguments(
                "browser session is bound to the current chat".into(),
            ));
        }
        let session_id = ctx
            .session_id()
            .ok_or_else(|| ToolError::Execution("browser requires a chat session".into()))?;
        let action = text_arg(&args, "action")?;
        if matches!(action, "click" | "fill" | "press" | "scroll")
            && args.get("expected_epoch").and_then(Value::as_u64).is_none()
        {
            return Err(ToolError::InvalidArguments(
                "browser interaction requires expected_epoch from a snapshot".into(),
            ));
        }
        let state = self.browser.open(session_id).await.map_err(browser_error)?;
        let epoch = args
            .get("expected_epoch")
            .and_then(Value::as_u64)
            .or_else(|| state.get("page_epoch").and_then(Value::as_u64))
            .ok_or_else(|| ToolError::Execution("browser page epoch missing".into()))?;
        let result = match action {
            "navigate" => {
                let url = text_arg(&args, "url")?;
                let parsed = url::Url::parse(url).map_err(|error| ToolError::InvalidArguments(error.to_string()))?;
                if !matches!(parsed.scheme(), "http" | "https") || !parsed.username().is_empty() || parsed.password().is_some() {
                    return Err(ToolError::InvalidArguments("browser requires an http(s) URL without credentials".into()));
                }
                self.browser.command(session_id, "navigate", json!({"url":parsed.as_str(),"expected_epoch":epoch})).await.map_err(browser_error)?
            }
            "click" => self.browser.command(session_id, "click_selector", json!({"selector":text_arg(&args,"selector")?,"expected_epoch":epoch})).await.map_err(browser_error)?,
            "fill" => self.browser.command(session_id, "fill_selector", json!({"selector":text_arg(&args,"selector")?,"text":args.get("text").and_then(Value::as_str).ok_or_else(|| ToolError::InvalidArguments("browser requires text for fill".into()))?,"expected_epoch":epoch})).await.map_err(browser_error)?,
            "press" => self.browser.command(session_id, "press_selector", json!({"selector":args.get("selector"),"key":text_arg(&args,"key")?,"expected_epoch":epoch})).await.map_err(browser_error)?,
            "scroll" => self.browser.command(session_id, "input", json!({"kind":"scroll","x":args.get("x").and_then(Value::as_f64).unwrap_or(500.0),"y":args.get("y").and_then(Value::as_f64).unwrap_or(360.0),"delta_x":args.get("delta_x").and_then(Value::as_f64).unwrap_or(0.0),"delta_y":args.get("delta_y").and_then(Value::as_f64).unwrap_or(500.0),"expected_epoch":epoch})).await.map_err(browser_error)?,
            "snapshot" => {
                let dom = self.browser.command(session_id, "dom", json!({})).await.map_err(browser_error)?;
                let mut text = format!("page_epoch: {}\nurl: {}\ntitle: {}\n\n{}", dom["page_epoch"], dom["url"].as_str().unwrap_or(""), dom["title"].as_str().unwrap_or(""), dom["snapshot"].as_str().unwrap_or(""));
                if args.get("include_html").and_then(Value::as_bool) == Some(true) {
                    text.push_str("\n\nHTML:\n");
                    text.push_str(dom["html"].as_str().unwrap_or(""));
                }
                return Ok(ToolOutcome::Completed(ToolResult::text(true, text)));
            }
            "screenshot" => {
                let image = self.browser.command(session_id, "screenshot", json!({})).await.map_err(browser_error)?;
                let data = image.get("data").and_then(Value::as_str).ok_or_else(|| ToolError::Execution("browser screenshot missing data".into()))?;
                return Ok(ToolOutcome::Completed(ToolResult {
                    success: true,
                    result: format!("Screenshot of {} (page_epoch {})", image["url"].as_str().unwrap_or(""), image["page_epoch"]),
                    display_preference: None,
                    images: vec![ToolResultImage { mime_type: "image/jpeg".into(), data: data.into() }],
                }));
            }
            _ => return Err(ToolError::InvalidArguments("unknown browser action".into())),
        };
        Ok(ToolOutcome::Completed(ToolResult::text(
            true,
            result.to_string(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn browser_tool_never_accepts_a_model_selected_session_id() {
        let tool = BrowserTool::new(Arc::new(BrowserManager::default()));
        let mut ctx = ToolCtx::none("browser-test");
        ctx.session_id = Some(Arc::from("current-chat"));
        let error = tool
            .invoke(json!({"action":"snapshot","session_id":"other-chat"}), ctx)
            .await
            .unwrap_err();
        assert!(matches!(error, ToolError::InvalidArguments(_)));
    }
}
