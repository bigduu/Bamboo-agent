use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::RwLock;

use crate::agent::core::budget::{
    apply_compression_plan, build_compression_plan_with_summary,
    estimate_context_compression_exposure, prepare_hybrid_context, summary_source_messages,
    HeuristicTokenCounter, LlmSummarizer, Summarizer,
};
use crate::agent::core::storage::Storage;
use crate::agent::core::tools::{Tool, ToolError, ToolExecutionContext, ToolResult};
use crate::agent::core::{AgentEvent, Session, TokenBudgetUsage};
use crate::agent::llm::LLMProvider;
use crate::core::Config;

const SESSION_CONTEXT_COMPRESSION_ENABLED_KEY: &str = "context_compression_tool_enabled";
const SESSION_CONTEXT_COMPRESSION_TRIGGER_PCT_KEY: &str = "context_compression_tool_trigger_pct";
const SESSION_CONTEXT_COMPRESSION_USAGE_PCT_KEY: &str = "context_compression_tool_usage_pct";
const SUMMARY_MIN_CHARS: usize = 240;
const SUMMARY_MIN_SECTIONS: usize = 3;
const SUMMARY_QUALITY_PASS_SCORE: usize = 5;

#[derive(Debug, Clone)]
struct SummaryQualityAssessment {
    score: usize,
    max_score: usize,
    section_count: usize,
    char_count: usize,
    missing_checks: Vec<&'static str>,
}

impl SummaryQualityAssessment {
    fn is_acceptable(&self) -> bool {
        self.score >= SUMMARY_QUALITY_PASS_SCORE
    }
}

#[derive(Debug, Default, Deserialize)]
struct CompressContextArgs {
    #[serde(default)]
    reason: Option<String>,
}

pub struct CompressContextTool {
    provider: Arc<dyn LLMProvider>,
    storage: Arc<dyn Storage>,
    sessions: Arc<RwLock<HashMap<String, Session>>>,
    config: Arc<RwLock<Config>>,
}

impl CompressContextTool {
    pub fn new(
        provider: Arc<dyn LLMProvider>,
        storage: Arc<dyn Storage>,
        sessions: Arc<RwLock<HashMap<String, Session>>>,
        config: Arc<RwLock<Config>>,
    ) -> Self {
        Self {
            provider,
            storage,
            sessions,
            config,
        }
    }

    async fn load_live_session(&self, session_id: &str) -> Result<Session, ToolError> {
        if let Some(session) = {
            let sessions = self.sessions.read().await;
            sessions.get(session_id).cloned()
        } {
            return Ok(session);
        }

        match self.storage.load_session(session_id).await {
            Ok(Some(session)) => Ok(session),
            Ok(None) => Err(ToolError::Execution(format!(
                "session not found: {session_id}"
            ))),
            Err(error) => Err(ToolError::Execution(format!(
                "failed to load session '{session_id}': {error}"
            ))),
        }
    }

    async fn persist_live_session(&self, session: &Session) -> Result<(), ToolError> {
        self.storage.save_session(session).await.map_err(|error| {
            ToolError::Execution(format!(
                "failed to save session '{}' after compress_context: {error}",
                session.id
            ))
        })?;

        let mut sessions = self.sessions.write().await;
        sessions.insert(session.id.clone(), session.clone());
        Ok(())
    }

    async fn summary_model_name(&self, session: &Session) -> String {
        let config = self.config.read().await;
        config
            .get_fast_model()
            .or_else(|| {
                if session.model.trim().is_empty() {
                    None
                } else {
                    Some(session.model.clone())
                }
            })
            .or_else(|| config.get_model())
            .unwrap_or_else(|| "gpt-4o-mini".to_string())
    }

    async fn summarize_messages(
        &self,
        session: &Session,
        model_name: &str,
        messages: &[crate::agent::core::Message],
        existing_summary_override: Option<String>,
    ) -> Result<String, ToolError> {
        let existing_summary = existing_summary_override.or_else(|| {
            session
                .conversation_summary
                .as_ref()
                .map(|summary| summary.content.clone())
        });
        let task_list_prompt = session
            .task_list
            .as_ref()
            .map(|_| session.format_task_list_for_prompt())
            .filter(|value| !value.trim().is_empty());
        let summarizer = LlmSummarizer::new(
            Arc::clone(&self.provider),
            model_name.to_string(),
            existing_summary,
            task_list_prompt,
        );
        summarizer
            .summarize(messages)
            .await
            .map_err(|error| {
                ToolError::Execution(format!("compress_context summary call failed: {error}"))
            })
    }

    fn section_count(summary: &str) -> usize {
        summary
            .lines()
            .filter(|line| {
                let trimmed = line.trim();
                trimmed.starts_with('#')
                    || trimmed.starts_with("1.")
                    || trimmed.starts_with("2.")
                    || trimmed.starts_with("3.")
                    || trimmed.starts_with("4.")
                    || trimmed.starts_with("5.")
                    || trimmed.starts_with("6.")
                    || trimmed.starts_with("7.")
            })
            .count()
    }

    fn text_contains_any(text: &str, patterns: &[&str]) -> bool {
        patterns.iter().any(|pattern| text.contains(pattern))
    }

    fn assess_summary_quality(summary: &str) -> SummaryQualityAssessment {
        let lower = summary.to_lowercase();
        let char_count = summary.chars().count();
        let section_count = Self::section_count(summary);
        let mut score = 0usize;
        let mut missing_checks = Vec::new();

        if char_count >= SUMMARY_MIN_CHARS {
            score += 1;
        } else {
            missing_checks.push("minimum_length");
        }

        if section_count >= SUMMARY_MIN_SECTIONS {
            score += 1;
        } else {
            missing_checks.push("structured_sections");
        }

        if Self::text_contains_any(
            &lower,
            &[
                "active",
                "in progress",
                "current objective",
                "当前",
                "进行中",
                "目标",
            ],
        ) {
            score += 1;
        } else {
            missing_checks.push("active_work_state");
        }

        if Self::text_contains_any(
            &lower,
            &[
                "completed",
                "done",
                "finished",
                "已完成",
                "完成",
                "done tasks",
            ],
        ) {
            score += 1;
        } else {
            missing_checks.push("completed_work_state");
        }

        if Self::text_contains_any(
            &lower,
            &[
                "next step",
                "next action",
                "open issue",
                "下一步",
                "后续",
                "待办",
                "下一阶段",
            ],
        ) {
            score += 1;
        } else {
            missing_checks.push("next_step");
        }

        if Self::text_contains_any(
            &lower,
            &[
                "constraint",
                "decision",
                "file",
                "tool",
                "限制",
                "约束",
                "文件",
                "工具",
                "决定",
            ],
        ) {
            score += 1;
        } else {
            missing_checks.push("context_density");
        }

        SummaryQualityAssessment {
            score,
            max_score: 6,
            section_count,
            char_count,
            missing_checks,
        }
    }

    async fn summarize_messages_with_quality_gate(
        &self,
        session: &Session,
        model_name: &str,
        messages: &[crate::agent::core::Message],
    ) -> Result<(String, SummaryQualityAssessment, bool), ToolError> {
        let first_summary = self
            .summarize_messages(session, model_name, messages, None)
            .await?;
        let first_assessment = Self::assess_summary_quality(&first_summary);
        if first_assessment.is_acceptable() {
            return Ok((first_summary, first_assessment, false));
        }

        let missing = if first_assessment.missing_checks.is_empty() {
            "none".to_string()
        } else {
            first_assessment.missing_checks.join(", ")
        };
        let refinement_seed = format!(
            "The previous draft summary needs quality repair before compression.\nMissing checks: {missing}\n\nPlease rewrite it with clear sections, active/completed state, and a concrete next step.\n\nDraft:\n{first_summary}"
        );

        let repaired_summary = self
            .summarize_messages(
                session,
                model_name,
                messages,
                Some(refinement_seed),
            )
            .await?;
        let repaired_assessment = Self::assess_summary_quality(&repaired_summary);

        if repaired_assessment.score >= first_assessment.score {
            Ok((repaired_summary, repaired_assessment, true))
        } else {
            Ok((first_summary, first_assessment, true))
        }
    }

    fn build_token_budget_usage_snapshot(
        session: &Session,
        budget: &crate::agent::core::budget::TokenBudget,
    ) -> Option<TokenBudgetUsage> {
        let counter = HeuristicTokenCounter::default();
        let prepared = prepare_hybrid_context(session, budget, &counter).ok()?;
        Some(TokenBudgetUsage {
            system_tokens: prepared.token_usage.system_tokens,
            summary_tokens: prepared.token_usage.summary_tokens,
            window_tokens: prepared.token_usage.window_tokens,
            total_tokens: prepared.token_usage.total_tokens,
            max_context_tokens: budget.max_context_tokens,
            budget_limit: prepared.token_usage.budget_limit,
            truncation_occurred: prepared.truncation_occurred,
            segments_removed: prepared.segments_removed,
        })
    }
}

#[async_trait]
impl Tool for CompressContextTool {
    fn name(&self) -> &str {
        "compress_context"
    }

    fn description(&self) -> &str {
        "Summarize and archive older conversation context when the active token budget has crossed the configured compression threshold. Use only after the threshold is reached and when enough progress has accumulated to compress safely."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "reason": {
                    "type": "string",
                    "description": "Optional short explanation of why now is a good time to compress context."
                }
            },
            "additionalProperties": false
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult, ToolError> {
        self.execute_with_context(args, ToolExecutionContext::none("tool_call"))
            .await
    }

    async fn execute_with_context(
        &self,
        args: serde_json::Value,
        ctx: ToolExecutionContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        let Some(session_id) = ctx.session_id else {
            return Err(ToolError::Execution(
                "compress_context requires a session_id in tool context".to_string(),
            ));
        };

        let parsed: CompressContextArgs = serde_json::from_value(args).map_err(|error| {
            ToolError::InvalidArguments(format!("Invalid compress_context args: {error}"))
        })?;

        let mut session = self.load_live_session(session_id).await?;
        let model_name = if session.model.trim().is_empty() {
            self.summary_model_name(&session).await
        } else {
            session.model.clone()
        };

        let exposure = estimate_context_compression_exposure(
            &session,
            &model_name,
            session.token_budget.as_ref(),
        );

        if !exposure.should_expose_tool {
            return Err(ToolError::Execution(format!(
                "compress_context is only available after context pressure reaches the threshold (current={}%, trigger={}%)",
                exposure.active_usage_percent_rounded, exposure.budget.compression_trigger_percent
            )));
        }

        let messages = summary_source_messages(&session);
        if messages.len() < 3 {
            return Err(ToolError::Execution(
                "Not enough active conversation history to compress yet".to_string(),
            ));
        }

        let summary_model = self.summary_model_name(&session).await;
        let (summary, summary_quality, summary_retried) = self
            .summarize_messages_with_quality_gate(&session, &summary_model, &messages)
            .await?;

        let Some(plan) = build_compression_plan_with_summary(
            &session,
            &model_name,
            session.token_budget.as_ref(),
            summary,
        ) else {
            return Err(ToolError::Execution(
                "Context compression plan could not be built safely for the current session state"
                    .to_string(),
            ));
        };

        let compressed_count = apply_compression_plan(&mut session, plan.clone());
        if compressed_count == 0 {
            return Err(ToolError::Execution(
                "compress_context did not archive any messages".to_string(),
            ));
        }

        session.metadata.insert(
            SESSION_CONTEXT_COMPRESSION_ENABLED_KEY.to_string(),
            "false".to_string(),
        );
        session.metadata.insert(
            SESSION_CONTEXT_COMPRESSION_USAGE_PCT_KEY.to_string(),
            format!("{:.1}", plan.active_usage_after_percent),
        );
        session.metadata.insert(
            SESSION_CONTEXT_COMPRESSION_TRIGGER_PCT_KEY.to_string(),
            plan.trigger_percent.to_string(),
        );

        let usage_snapshot = Self::build_token_budget_usage_snapshot(&session, &exposure.budget);
        if let Some(ref usage) = usage_snapshot {
            session.token_usage = Some(usage.clone());
        }

        self.persist_live_session(&session).await?;

        if let (Some(event_tx), Some(usage)) = (ctx.cloned_sender(), usage_snapshot.clone()) {
            let _ = event_tx.send(AgentEvent::TokenBudgetUpdated { usage }).await;
        }

        let reason = parsed
            .reason
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("threshold crossed and a stable checkpoint was available");

        Ok(ToolResult {
            success: true,
            result: json!({
                "session_id": session.id,
                "compressed_messages": compressed_count,
                "active_usage_before_percent": plan.active_usage_before_percent,
                "active_usage_after_percent": plan.active_usage_after_percent,
                "trigger_percent": plan.trigger_percent,
                "target_percent": plan.target_percent,
                "summary_model": summary_model,
                "reason": reason,
                "summary_chars": plan.summary_content.chars().count(),
                "summary_quality": {
                    "score": summary_quality.score,
                    "max_score": summary_quality.max_score,
                    "section_count": summary_quality.section_count,
                    "char_count": summary_quality.char_count,
                    "missing_checks": summary_quality.missing_checks,
                    "retried": summary_retried,
                    "acceptable": summary_quality.is_acceptable(),
                },
            })
            .to_string(),
            display_preference: Some("json".to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use async_trait::async_trait;
    use futures::stream;
    use std::collections::HashMap;
    use std::io;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::mpsc;

    use crate::agent::core::budget::TokenBudget;
    use crate::agent::core::storage::AttachmentReader;
    use crate::agent::core::{AgentEvent, Message};
    use crate::agent::llm::{LLMChunk, LLMError, LLMStream};

    #[derive(Default)]
    struct MemoryStorage {
        sessions: Mutex<HashMap<String, Session>>,
    }

    #[async_trait]
    impl Storage for MemoryStorage {
        async fn save_session(&self, session: &Session) -> io::Result<()> {
            self.sessions
                .lock()
                .expect("sessions lock")
                .insert(session.id.clone(), session.clone());
            Ok(())
        }

        async fn load_session(&self, session_id: &str) -> io::Result<Option<Session>> {
            Ok(self
                .sessions
                .lock()
                .expect("sessions lock")
                .get(session_id)
                .cloned())
        }

        async fn append_event(
            &self,
            _session_id: &str,
            _event: &crate::agent::core::AgentEvent,
        ) -> io::Result<()> {
            Ok(())
        }

        async fn load_events(
            &self,
            _session_id: &str,
        ) -> io::Result<Vec<crate::agent::core::AgentEvent>> {
            Ok(Vec::new())
        }

        async fn delete_session(&self, session_id: &str) -> io::Result<bool> {
            Ok(self
                .sessions
                .lock()
                .expect("sessions lock")
                .remove(session_id)
                .is_some())
        }
    }

    #[async_trait]
    impl AttachmentReader for MemoryStorage {
        async fn read_attachment(
            &self,
            _session_id: &str,
            _attachment_id: &str,
        ) -> io::Result<Option<(Vec<u8>, String)>> {
            Ok(None)
        }
    }

    struct StaticSummaryProvider {
        summary_text: String,
        requested_models: Mutex<Vec<String>>,
        requested_messages: Mutex<Vec<Vec<Message>>>,
    }

    #[async_trait]
    impl LLMProvider for StaticSummaryProvider {
        async fn chat_stream(
            &self,
            messages: &[Message],
            _tools: &[crate::agent::core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            model: &str,
        ) -> Result<LLMStream, LLMError> {
            self.requested_models
                .lock()
                .expect("models lock")
                .push(model.to_string());
            self.requested_messages
                .lock()
                .expect("messages lock")
                .push(messages.to_vec());
            let chunks = vec![
                Ok::<LLMChunk, LLMError>(LLMChunk::Token(self.summary_text.clone())),
                Ok::<LLMChunk, LLMError>(LLMChunk::Done),
            ];
            Ok(Box::pin(stream::iter(chunks)))
        }
    }

    fn make_pressure_session(session_id: &str) -> Session {
        let mut session = Session::new(session_id, "gpt-4o-mini");
        session.title = "Compression target".to_string();
        session.token_budget = Some(TokenBudget {
            max_context_tokens: 1000,
            max_output_tokens: 100,
            strategy: crate::agent::core::budget::BudgetStrategy::Hybrid {
                window_size: 20,
                enable_summarization: true,
            },
            safety_margin: 0,
            compression_trigger_percent: 20,
            compression_target_percent: 20,
        });
        session.add_message(Message::system(
            "System: keep working on the repository state.",
        ));
        for i in 0..8 {
            session.add_message(Message::user(format!(
                "User asks for progress update {i}: {}",
                "alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu ".repeat(6)
            )));
            session.add_message(Message::assistant(
                format!(
                    "Assistant reply {i}: {}",
                    "implemented analysis, reviewed files, tracked risks and next steps ".repeat(6)
                ),
                None,
            ));
        }
        session.metadata.insert(
            "responses.previous_response_id".to_string(),
            "resp_previous_123".to_string(),
        );
        session
    }

    fn tool_with_session(
        session: Session,
    ) -> (
        CompressContextTool,
        Arc<MemoryStorage>,
        Arc<RwLock<HashMap<String, Session>>>,
    ) {
        let storage = Arc::new(MemoryStorage::default());
        let sessions = Arc::new(RwLock::new(HashMap::new()));
        futures::executor::block_on(async {
            storage.save_session(&session).await.expect("save session");
            sessions
                .write()
                .await
                .insert(session.id.clone(), session.clone());
        });

        let provider = Arc::new(StaticSummaryProvider {
            summary_text: "Condensed summary: preserve goals, risks, changed files, and next step."
                .to_string(),
            requested_models: Mutex::new(Vec::new()),
            requested_messages: Mutex::new(Vec::new()),
        });
        let config = Arc::new(RwLock::new(Config::default()));
        let tool = CompressContextTool::new(provider, storage.clone(), sessions.clone(), config);
        (tool, storage, sessions)
    }

    #[tokio::test]
    async fn compress_context_requires_session_id_in_context() {
        let session = make_pressure_session("compress-test-no-ctx");
        let (tool, _storage, _sessions) = tool_with_session(session);

        let err = tool
            .execute_with_context(json!({}), ToolExecutionContext::none("tool_call"))
            .await
            .expect_err("missing session_id should fail");

        match err {
            ToolError::Execution(msg) => {
                assert!(msg.contains("requires a session_id"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn compress_context_rejects_when_threshold_not_reached() {
        let mut session = Session::new("compress-threshold-low", "gpt-4o-mini");
        session.token_budget = Some(TokenBudget {
            max_context_tokens: 100_000,
            max_output_tokens: 1_000,
            strategy: crate::agent::core::budget::BudgetStrategy::Hybrid {
                window_size: 20,
                enable_summarization: true,
            },
            safety_margin: 0,
            compression_trigger_percent: 80,
            compression_target_percent: 50,
        });
        session.add_message(Message::system("system"));
        session.add_message(Message::user("short request"));
        session.add_message(Message::assistant("short reply", None));
        session.add_message(Message::user("another short request"));

        let (tool, _storage, _sessions) = tool_with_session(session);
        let err = tool
            .execute_with_context(
                json!({ "reason": "too early" }),
                ToolExecutionContext {
                    session_id: Some("compress-threshold-low"),
                    tool_call_id: "tool_call",
                    event_tx: None,
                },
            )
            .await
            .expect_err("tool should refuse before threshold");

        match err {
            ToolError::Execution(msg) => {
                assert!(msg.contains("only available after context pressure reaches the threshold"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn compress_context_compresses_session_and_persists_state() {
        let session = make_pressure_session("compress-success");
        let (tool, storage, sessions) = tool_with_session(session.clone());

        let result = tool
            .execute_with_context(
                json!({ "reason": "stable milestone reached" }),
                ToolExecutionContext {
                    session_id: Some("compress-success"),
                    tool_call_id: "tool_call",
                    event_tx: None,
                },
            )
            .await
            .expect("compress_context should succeed");

        assert!(result.success);
        assert_eq!(result.display_preference.as_deref(), Some("json"));
        let payload: serde_json::Value =
            serde_json::from_str(&result.result).expect("tool result json");
        assert_eq!(payload["session_id"], "compress-success");
        assert_eq!(payload["reason"], "stable milestone reached");
        assert!(payload["compressed_messages"].as_u64().unwrap_or(0) > 0);
        assert!(!payload["summary_model"].as_str().unwrap_or("").is_empty());
        assert!(payload["summary_chars"].as_u64().unwrap_or(0) > 0);
        assert!(payload["summary_quality"]["score"].as_u64().unwrap_or(0) > 0);
        assert!(payload["summary_quality"]["max_score"].as_u64().unwrap_or(0) >= 6);

        let persisted = storage
            .load_session("compress-success")
            .await
            .expect("load persisted")
            .expect("persisted session present");
        assert!(persisted.conversation_summary.is_some());
        assert!(!persisted.compression_events.is_empty());
        assert!(persisted.messages.iter().any(|m| m.compressed));
        assert!(persisted
            .messages
            .iter()
            .filter(|m| m.compressed)
            .all(|m| m.compressed_by_event_id.is_some()));
        assert!(!persisted
            .metadata
            .contains_key("responses.previous_response_id"));
        assert_eq!(
            persisted
                .metadata
                .get(SESSION_CONTEXT_COMPRESSION_ENABLED_KEY)
                .map(String::as_str),
            Some("false")
        );
        assert!(persisted
            .metadata
            .contains_key(SESSION_CONTEXT_COMPRESSION_TRIGGER_PCT_KEY));
        assert!(persisted
            .metadata
            .contains_key(SESSION_CONTEXT_COMPRESSION_USAGE_PCT_KEY));

        let cached = sessions
            .read()
            .await
            .get("compress-success")
            .cloned()
            .expect("cached session present");
        assert_eq!(
            cached
                .conversation_summary
                .as_ref()
                .map(|s| s.content.as_str()),
            persisted
                .conversation_summary
                .as_ref()
                .map(|s| s.content.as_str())
        );
    }

    #[test]
    fn summary_quality_assessment_flags_sparse_summary_and_accepts_structured_summary() {
        let sparse = "Quick note only.";
        let sparse_assessment = CompressContextTool::assess_summary_quality(sparse);
        assert!(!sparse_assessment.is_acceptable());
        assert!(sparse_assessment.missing_checks.contains(&"structured_sections"));
        assert!(sparse_assessment.missing_checks.contains(&"next_step"));

        let structured = r#"
## Current active objective
Stabilize context compression and event refresh behavior.

## Active tasks
- Implement quality gate and retry logic for compress summaries.
- Emit token budget refresh event immediately after compression.

## Completed tasks
- Added host fallback guidance and critical usage re-exposure.

## Important context and constraints
- Preserve tool findings and file paths.
- Keep summary concise but actionable.

## Open issues and next step
- Validate with integration tests and watch frontend refresh behavior.
"#;
        let structured_assessment = CompressContextTool::assess_summary_quality(structured);
        assert!(structured_assessment.is_acceptable());
    }

    #[tokio::test]
    async fn compress_context_emits_token_budget_updated_event_after_success() {
        let session = make_pressure_session("compress-event-stream");
        let (tool, _storage, _sessions) = tool_with_session(session);
        let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(8);

        let _result = tool
            .execute_with_context(
                json!({ "reason": "sync UI after compression" }),
                ToolExecutionContext {
                    session_id: Some("compress-event-stream"),
                    tool_call_id: "tool_call",
                    event_tx: Some(&event_tx),
                },
            )
            .await
            .expect("compress_context should succeed");

        let event = tokio::time::timeout(Duration::from_millis(300), event_rx.recv())
            .await
            .expect("expected token budget event in stream")
            .expect("token budget event should exist");
        match event {
            AgentEvent::TokenBudgetUpdated { usage } => {
                assert!(usage.max_context_tokens > 0);
                assert!(usage.budget_limit > 0);
                assert!(usage.total_tokens <= usage.budget_limit);
            }
            other => panic!("expected TokenBudgetUpdated event, got {other:?}"),
        }
    }
}
