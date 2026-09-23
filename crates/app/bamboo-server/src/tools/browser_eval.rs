//! Root-only page-realm JavaScript on the browser page shared with the workbench.

use std::sync::Arc;

use async_trait::async_trait;
use bamboo_agent_core::tools::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult};
use serde_json::{json, Value};

use crate::browser::BrowserManager;

const MAX_CODE_BYTES: usize = 8 * 1024;
const MAX_URL_BYTES: usize = 8 * 1024;
const MAX_SAFE_EPOCH: u64 = (1_u64 << 53) - 1;

pub struct BrowserEvalTool {
    browser: Arc<BrowserManager>,
}

impl BrowserEvalTool {
    pub fn new(browser: Arc<BrowserManager>) -> Self {
        Self { browser }
    }
}

fn validated_args(args: &Value) -> Result<(&str, u64, &str), ToolError> {
    let code = args
        .get("code")
        .and_then(Value::as_str)
        .filter(|code| !code.trim().is_empty() && code.len() <= MAX_CODE_BYTES)
        .ok_or_else(|| {
            ToolError::InvalidArguments(
                "browser_eval code must be nonempty and at most 8 KiB".into(),
            )
        })?;
    let epoch = args
        .get("expected_epoch")
        .and_then(Value::as_u64)
        .filter(|epoch| *epoch <= MAX_SAFE_EPOCH)
        .ok_or_else(|| {
            ToolError::InvalidArguments("browser_eval requires a safe expected_epoch".into())
        })?;
    let expected_url = args
        .get("expected_url")
        .and_then(Value::as_str)
        .filter(|url| !url.is_empty() && url.len() <= MAX_URL_BYTES)
        .ok_or_else(|| {
            ToolError::InvalidArguments("browser_eval requires a bounded expected_url".into())
        })?;
    if expected_url != "about:blank" {
        let parsed = url::Url::parse(expected_url).map_err(|error| {
            ToolError::InvalidArguments(format!("invalid browser_eval URL: {error}"))
        })?;
        if !matches!(parsed.scheme(), "http" | "https")
            || !parsed.username().is_empty()
            || parsed.password().is_some()
        {
            return Err(ToolError::InvalidArguments(
                "browser_eval requires an http(s) URL without credentials or about:blank".into(),
            ));
        }
    }
    Ok((code, epoch, expected_url))
}

#[async_trait]
impl Tool for BrowserEvalTool {
    fn name(&self) -> &str {
        "browser_eval"
    }

    fn description(&self) -> &str {
        "Execute a bounded JavaScript expression or IIFE in the active browser page shared with this chat. This page-realm capability can read or change the DOM and is separate from ordinary browser controls. First use browser snapshot to get page_epoch and exact URL, then pass them as expected_epoch and expected_url. A navigation or tab switch invalidates the request. The code cannot access Bamboo's Node host, local files, Playwright page object, or CDP. Return only JSON-safe values."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type":"object",
            "properties":{
                "code":{"type":"string","description":"JavaScript expression or IIFE evaluated in the active page; at most 8 KiB UTF-8 bytes","maxLength":8192},
                "expected_epoch":{"type":"integer","description":"page_epoch from the page snapshot; required to reject stale scripts"},
                "expected_url":{"type":"string","description":"Exact URL from the page snapshot, including origin; required to reject navigation","maxLength":8192}
            },
            "required":["code","expected_epoch","expected_url"],
            "additionalProperties":false
        })
    }

    fn classify(&self, _args: &Value) -> ToolClass {
        ToolClass::MUTATING_SERIAL
    }

    async fn invoke(&self, args: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        if args.get("session_id").is_some() {
            return Err(ToolError::InvalidArguments(
                "browser_eval session is bound to the current chat".into(),
            ));
        }
        let session_id = ctx
            .session_id()
            .ok_or_else(|| ToolError::Execution("browser_eval requires a chat session".into()))?;
        let (code, expected_epoch, expected_url) = validated_args(&args)?;
        let result = self
            .browser
            .eval(
                session_id,
                json!({"code":code,"expected_epoch":expected_epoch,"expected_url":expected_url}),
            )
            .await
            .map_err(|error| ToolError::Execution(error.to_string()))?;
        Ok(ToolOutcome::Completed(ToolResult::text(
            true,
            result.to_string(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eval_is_a_separate_mutating_tool_with_bounded_arguments() {
        let tool = BrowserEvalTool::new(Arc::new(BrowserManager::default()));
        assert_eq!(tool.name(), "browser_eval");
        assert_eq!(tool.classify(&json!({})), ToolClass::MUTATING_SERIAL);
        let schema = tool.parameters_schema();
        assert_eq!(schema["properties"]["code"]["maxLength"], 8192);
        assert_eq!(
            schema["required"],
            json!(["code", "expected_epoch", "expected_url"])
        );
        for bad in [
            json!({"code":" ","expected_epoch":17,"expected_url":"about:blank"}),
            json!({"code":"x".repeat(8193),"expected_epoch":17,"expected_url":"about:blank"}),
            json!({"code":"1","expected_url":"about:blank"}),
            json!({"code":"1","expected_epoch":17,"expected_url":"file:///secret"}),
            json!({"code":"1","expected_epoch":17,"expected_url":"https://user:password@example.com/"}),
        ] {
            assert!(validated_args(&bad).is_err(), "{bad}");
        }
        assert!(validated_args(&json!({
            "code":"document.title", "expected_epoch":17, "expected_url":"about:blank"
        }))
        .is_ok());
    }

    #[tokio::test]
    async fn eval_uses_only_the_tool_context_session() {
        let tool = BrowserEvalTool::new(Arc::new(BrowserManager::default()));
        let mut ctx = ToolCtx::none("browser-eval-test");
        ctx.session_id = Some(Arc::from("current-chat"));
        let rejected = tool
            .invoke(
                json!({"code":"1","expected_epoch":17,"expected_url":"about:blank","session_id":"other-chat"}),
                ctx.clone(),
            )
            .await
            .unwrap_err();
        assert!(matches!(rejected, ToolError::InvalidArguments(_)));
        let missing = tool
            .invoke(
                json!({"code":"1","expected_epoch":17,"expected_url":"about:blank"}),
                ctx,
            )
            .await
            .unwrap_err();
        assert!(matches!(missing, ToolError::Execution(_)));
    }
}
