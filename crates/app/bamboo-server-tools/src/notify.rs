//! Agent-invocable `notify` tool: proactively alert the human owner outside
//! the chat transcript.
//!
//! This crate stays free of `AppState` (see the crate doc), so the actual
//! delivery (classify/dedup through `NotificationService`, broadcast to
//! connected clients, fan-out to desktop/push sinks) lives in `bamboo-server`
//! behind the [`NotificationDispatcher`] port — this module only validates
//! arguments and calls it. Crib: `SessionInspectorTool` for the framework-side
//! tool shape, `ChildSessionAdapter` (bamboo-server) for the port-adapter
//! precedent.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

use bamboo_agent_core::tools::{Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult};

/// Maximum `title` length, in characters.
const MAX_TITLE_CHARS: usize = 100;
/// Maximum `message` length, in characters.
const MAX_MESSAGE_CHARS: usize = 500;

/// Port the `notify` tool dispatches through. Implemented server-side by an
/// adapter with real access to `NotificationService`, the session broadcast
/// channels, and configured delivery sinks (desktop/ntfy/bark).
///
/// `dispatch` is deliberately synchronous and fire-and-forget — it mirrors
/// `bamboo_server::notify_sinks::NotificationSink::deliver`: the real
/// classify/dedup/broadcast/sink work happens off this call (a spawned
/// task), so a slow or unreachable delivery channel can never stall the
/// agent loop. Consequently the tool can only ever confirm a request was
/// ATTEMPTED via currently-enabled channels, never that it was delivered.
pub trait NotificationDispatcher: Send + Sync {
    /// `priority` is always one of `"low"` | `"normal"` | `"high"` (validated
    /// by [`NotifyTool::invoke`] before this is called). `url` is an optional
    /// click-through link for push channels that support one.
    fn dispatch(
        &self,
        session_id: &str,
        title: &str,
        body: &str,
        priority: &str,
        url: Option<&str>,
    );
}

#[derive(Debug, Deserialize)]
struct NotifyArgs {
    title: String,
    message: String,
    #[serde(default)]
    priority: Option<String>,
    #[serde(default)]
    url: Option<String>,
}

/// Agent-invocable tool for proactively alerting the human owner outside this
/// conversation (desktop popup and/or configured push channels).
pub struct NotifyTool {
    dispatcher: Arc<dyn NotificationDispatcher>,
}

impl NotifyTool {
    pub fn new(dispatcher: Arc<dyn NotificationDispatcher>) -> Self {
        Self { dispatcher }
    }
}

#[async_trait]
impl Tool for NotifyTool {
    fn name(&self) -> &str {
        "notify"
    }

    fn description(&self) -> &str {
        "Proactively alert the human owner OUTSIDE this conversation (OS desktop popup and/or any push channels \
         they've configured), for things worth interrupting them for even if they are not watching this session \
         right now: a scheduled reminder firing, a long-running task finishing, or something that needs their \
         attention. Do NOT use this for a normal conversational reply — that already reaches the user through the \
         chat transcript itself. Use it sparingly, only when the message has standalone value outside the chat."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "title": {
                    "type": "string",
                    "description": "Short notification title (at most 100 characters)."
                },
                "message": {
                    "type": "string",
                    "description": "Notification body (at most 500 characters)."
                },
                "priority": {
                    "type": "string",
                    "enum": ["low", "normal", "high"],
                    "description": "Urgency of the notification. Defaults to \"normal\"."
                },
                "url": {
                    "type": "string",
                    "description": "Optional click-through URL/deep-link surfaced by push channels that support one."
                }
            },
            "required": ["title", "message"]
        })
    }

    fn classify(&self, _args: &serde_json::Value) -> ToolClass {
        // Mutates nothing in the session or workspace: it only fires an
        // outbound side effect (OS popup / push) and never produces a result
        // the model could branch on, so it's safe in the read-only parallel
        // batch alongside other tool calls in the same round.
        ToolClass::READONLY_PARALLEL
    }

    async fn invoke(
        &self,
        args: serde_json::Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let session_id = ctx
            .session_id()
            .ok_or_else(|| {
                ToolError::Execution("notify requires a session_id in tool context".to_string())
            })?
            .to_string();

        let parsed: NotifyArgs = serde_json::from_value(args)
            .map_err(|e| ToolError::InvalidArguments(format!("Invalid notify args: {e}")))?;

        let title = parsed.title.trim();
        if title.is_empty() {
            return Err(ToolError::InvalidArguments(
                "notify: `title` must not be empty".to_string(),
            ));
        }
        if title.chars().count() > MAX_TITLE_CHARS {
            return Err(ToolError::InvalidArguments(format!(
                "notify: `title` must be at most {MAX_TITLE_CHARS} characters (got {})",
                title.chars().count()
            )));
        }

        let message = parsed.message.trim();
        if message.is_empty() {
            return Err(ToolError::InvalidArguments(
                "notify: `message` must not be empty".to_string(),
            ));
        }
        if message.chars().count() > MAX_MESSAGE_CHARS {
            return Err(ToolError::InvalidArguments(format!(
                "notify: `message` must be at most {MAX_MESSAGE_CHARS} characters (got {})",
                message.chars().count()
            )));
        }

        let priority = match parsed.priority.as_deref() {
            None => "normal",
            Some(p) if p.eq_ignore_ascii_case("low") => "low",
            Some(p) if p.eq_ignore_ascii_case("normal") => "normal",
            Some(p) if p.eq_ignore_ascii_case("high") => "high",
            Some(other) => {
                return Err(ToolError::InvalidArguments(format!(
                    "notify: `priority` must be one of low|normal|high (got \"{other}\")"
                )))
            }
        };

        let url = parsed
            .url
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());

        self.dispatcher
            .dispatch(&session_id, title, message, priority, url);

        Ok(ToolOutcome::Completed(ToolResult {
            success: true,
            result: format!(
                "Notification \"{title}\" (priority: {priority}) sent to every notification channel currently \
                 enabled in settings (in-app plus any configured desktop/push channels). This confirms the request \
                 was dispatched, not that it was delivered or read."
            ),
            display_preference: Some("Collapsible".to_string()),
            images: Vec::new(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordedCall {
        session_id: String,
        title: String,
        body: String,
        priority: String,
        url: Option<String>,
    }

    #[derive(Default)]
    struct MockDispatcher {
        calls: Mutex<Vec<RecordedCall>>,
    }

    impl NotificationDispatcher for MockDispatcher {
        fn dispatch(
            &self,
            session_id: &str,
            title: &str,
            body: &str,
            priority: &str,
            url: Option<&str>,
        ) {
            self.calls.lock().unwrap().push(RecordedCall {
                session_id: session_id.to_string(),
                title: title.to_string(),
                body: body.to_string(),
                priority: priority.to_string(),
                url: url.map(str::to_string),
            });
        }
    }

    fn ctx() -> ToolCtx {
        let mut ctx = ToolCtx::none("call-1");
        ctx.session_id = Some(std::sync::Arc::from("sess-1"));
        ctx
    }

    #[test]
    fn tool_name_is_notify() {
        let dispatcher = Arc::new(MockDispatcher::default());
        let tool = NotifyTool::new(dispatcher);
        assert_eq!(tool.name(), "notify");
    }

    #[test]
    fn schema_requires_title_and_message() {
        let dispatcher = Arc::new(MockDispatcher::default());
        let tool = NotifyTool::new(dispatcher);
        let schema = tool.parameters_schema();
        let required: Vec<&str> = schema["required"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert!(required.contains(&"title"));
        assert!(required.contains(&"message"));
        assert!(!required.contains(&"priority"));
        assert!(!required.contains(&"url"));
    }

    #[test]
    fn classify_is_readonly_parallel() {
        let dispatcher = Arc::new(MockDispatcher::default());
        let tool = NotifyTool::new(dispatcher);
        assert_eq!(tool.classify(&json!({})), ToolClass::READONLY_PARALLEL);
    }

    #[tokio::test]
    async fn invoke_dispatches_with_defaults_and_confirms() {
        let dispatcher = Arc::new(MockDispatcher::default());
        let tool = NotifyTool::new(dispatcher.clone());

        let out = tool
            .invoke(json!({"title": "Reminder", "message": "Stand up"}), ctx())
            .await
            .expect("invoke should succeed");
        let ToolOutcome::Completed(result) = out else {
            panic!("expected Completed")
        };
        assert!(result.success);
        assert!(result.result.contains("Reminder"));
        assert!(result.result.contains("normal"));

        let calls = dispatcher.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].session_id, "sess-1");
        assert_eq!(calls[0].title, "Reminder");
        assert_eq!(calls[0].body, "Stand up");
        assert_eq!(calls[0].priority, "normal");
        assert_eq!(calls[0].url, None);
    }

    #[tokio::test]
    async fn invoke_forwards_explicit_priority_and_url() {
        let dispatcher = Arc::new(MockDispatcher::default());
        let tool = NotifyTool::new(dispatcher.clone());

        tool.invoke(
            json!({
                "title": "Build finished",
                "message": "The nightly build finished with 0 failures.",
                "priority": "HIGH",
                "url": "bamboo://session/sess-1"
            }),
            ctx(),
        )
        .await
        .expect("invoke should succeed");

        let calls = dispatcher.calls.lock().unwrap();
        assert_eq!(calls[0].priority, "high");
        assert_eq!(calls[0].url.as_deref(), Some("bamboo://session/sess-1"));
    }

    #[tokio::test]
    async fn invoke_rejects_missing_title() {
        let dispatcher = Arc::new(MockDispatcher::default());
        let tool = NotifyTool::new(dispatcher);
        let result = tool.invoke(json!({"message": "hi"}), ctx()).await;
        assert!(matches!(result, Err(ToolError::InvalidArguments(_))));
    }

    #[tokio::test]
    async fn invoke_rejects_missing_message() {
        let dispatcher = Arc::new(MockDispatcher::default());
        let tool = NotifyTool::new(dispatcher);
        let result = tool.invoke(json!({"title": "hi"}), ctx()).await;
        assert!(matches!(result, Err(ToolError::InvalidArguments(_))));
    }

    #[tokio::test]
    async fn invoke_rejects_oversize_title() {
        let dispatcher = Arc::new(MockDispatcher::default());
        let tool = NotifyTool::new(dispatcher);
        let long_title = "x".repeat(MAX_TITLE_CHARS + 1);
        let result = tool
            .invoke(json!({"title": long_title, "message": "hi"}), ctx())
            .await;
        assert!(matches!(result, Err(ToolError::InvalidArguments(_))));
    }

    #[tokio::test]
    async fn invoke_rejects_oversize_message() {
        let dispatcher = Arc::new(MockDispatcher::default());
        let tool = NotifyTool::new(dispatcher);
        let long_message = "x".repeat(MAX_MESSAGE_CHARS + 1);
        let result = tool
            .invoke(json!({"title": "hi", "message": long_message}), ctx())
            .await;
        assert!(matches!(result, Err(ToolError::InvalidArguments(_))));
    }

    #[tokio::test]
    async fn invoke_rejects_empty_title_after_trim() {
        let dispatcher = Arc::new(MockDispatcher::default());
        let tool = NotifyTool::new(dispatcher);
        let result = tool
            .invoke(json!({"title": "   ", "message": "hi"}), ctx())
            .await;
        assert!(matches!(result, Err(ToolError::InvalidArguments(_))));
    }

    #[tokio::test]
    async fn invoke_rejects_invalid_priority() {
        let dispatcher = Arc::new(MockDispatcher::default());
        let tool = NotifyTool::new(dispatcher);
        let result = tool
            .invoke(
                json!({"title": "hi", "message": "hi", "priority": "urgent"}),
                ctx(),
            )
            .await;
        assert!(matches!(result, Err(ToolError::InvalidArguments(_))));
    }

    #[tokio::test]
    async fn invoke_rejects_missing_session_id() {
        let dispatcher = Arc::new(MockDispatcher::default());
        let tool = NotifyTool::new(dispatcher);
        let result = tool
            .invoke(
                json!({"title": "hi", "message": "hi"}),
                ToolCtx::none("call-1"),
            )
            .await;
        assert!(matches!(result, Err(ToolError::Execution(_))));
    }

    #[tokio::test]
    async fn invoke_treats_blank_url_as_absent() {
        let dispatcher = Arc::new(MockDispatcher::default());
        let tool = NotifyTool::new(dispatcher.clone());
        tool.invoke(json!({"title": "hi", "message": "hi", "url": "   "}), ctx())
            .await
            .expect("invoke should succeed");
        let calls = dispatcher.calls.lock().unwrap();
        assert_eq!(calls[0].url, None);
    }
}
