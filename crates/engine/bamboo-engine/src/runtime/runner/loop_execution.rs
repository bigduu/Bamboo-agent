use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::runtime::config::AgentLoopConfig;
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentEvent, Session};
use bamboo_domain::{AgentHookPoint, HookPayload};
use bamboo_llm::LLMProvider;

mod gold;
mod pipeline;
mod startup;

use pipeline::run_pipeline;
use startup::{initialize_loop_state, LoopRunState};

/// Runs the agent loop with a custom configuration.
///
/// This is the primary entry point for executing an agent conversation loop.
/// It manages LLM streaming, tool execution, task list tracking, metrics collection,
/// and event emission throughout the conversation lifecycle.
///
/// # Arguments
///
/// * `session` - The conversation session to operate on
/// * `initial_message` - The user's initial message to process
/// * `event_tx` - Channel sender for agent events
/// * `llm` - The LLM provider to use for generation
/// * `tools` - The tool executor for handling tool calls
/// * `cancel_token` - Token for cancelling the operation
/// * `config` - Configuration controlling loop behavior
///
/// # Returns
///
/// Returns `Ok(())` on successful completion, or an error if the loop fails.
pub(crate) async fn run_agent_loop_with_config(
    session: &mut Session,
    initial_message: String,
    event_tx: mpsc::Sender<AgentEvent>,
    llm: Arc<dyn LLMProvider>,
    tools: Arc<dyn ToolExecutor>,
    cancel_token: CancellationToken,
    config: AgentLoopConfig,
) -> super::Result<()> {
    let session_span = tracing::info_span!("agent_loop", session_id = %session.id);
    async {
        let mut state: LoopRunState = initialize_loop_state(
            session,
            initial_message.as_str(),
            &config,
            tools.as_ref(),
            &event_tx,
        )
        .await?;

        if config
            .hook_runner
            .has_hooks_for(AgentHookPoint::AfterSessionSetup)
        {
            let payload = HookPayload::SessionSetup {
                initial_message: initial_message.clone(),
            };
            let outcome = config
                .hook_runner
                .run_hooks(
                    AgentHookPoint::AfterSessionSetup,
                    &payload,
                    session,
                    &mut state.runtime_state,
                    Some(&event_tx),
                )
                .await;
            let hook_result = crate::runtime::hooks::apply_hook_outcome(
                AgentHookPoint::AfterSessionSetup,
                outcome,
                session,
                &mut state.runtime_state,
            );
            super::state_bridge::write_runtime_state(session, &state.runtime_state);
            if let Err(error) = hook_result {
                if error.is_hook_suspended() {
                    super::session_finalize::finalize_session(
                        state.task_context.take(),
                        session,
                        &event_tx,
                        &state.session_id,
                        &config,
                        state.metrics_collector.as_ref(),
                        false,
                        &mut state.runtime_state,
                    )
                    .await;
                    return Ok(());
                }
                if let Some(skill_manager) = config.skill_manager.as_ref() {
                    let workspace = session.workspace_path_meta().map(std::path::PathBuf::from);
                    if let Err(release_error) = skill_manager
                        .release_activation_for_workspace(&state.session_id, workspace.as_deref())
                        .await
                    {
                        tracing::warn!(
                            "[{}] Failed to release hook-aborted workflow activation snapshot: {}",
                            state.session_id,
                            release_error
                        );
                    }
                }
                return Err(error);
            }
        }

        let pipeline_result = run_pipeline(
            session,
            &event_tx,
            llm,
            tools,
            &cancel_token,
            &config,
            &mut state,
        )
        .await;

        let sent_complete = match pipeline_result {
            Ok(sent_complete) => sent_complete,
            Err(error) if error.is_hook_suspended() => {
                crate::runtime::hooks::merge_session_hook_checkpoints(
                    session,
                    &mut state.runtime_state,
                );
                super::session_finalize::finalize_session(
                    state.task_context.take(),
                    session,
                    &event_tx,
                    &state.session_id,
                    &config,
                    state.metrics_collector.as_ref(),
                    false,
                    &mut state.runtime_state,
                )
                .await;
                return Ok(());
            }
            Err(error) => {
                if !config.hook_runner.is_empty() {
                    crate::runtime::hooks::merge_session_hook_checkpoints(
                        session,
                        &mut state.runtime_state,
                    );
                    super::state_bridge::write_runtime_state(session, &state.runtime_state);
                }
                // Errors and cancellation are terminal for this activation but must
                // not flow through normal finalization: that would emit a false
                // Complete event and stamp the runtime state Completed. Release only
                // the immutable workflow snapshot, then preserve the original error.
                if let Some(skill_manager) = config.skill_manager.as_ref() {
                    let workspace = session.workspace_path_meta().map(std::path::PathBuf::from);
                    if let Err(release_error) = skill_manager
                        .release_activation_for_workspace(&state.session_id, workspace.as_deref())
                        .await
                    {
                        tracing::warn!(
                            "[{}] Failed to release errored workflow activation snapshot: {}",
                            state.session_id,
                            release_error
                        );
                    }
                }
                return Err(error);
            }
        };

        super::session_finalize::finalize_session(
            state.task_context,
            session,
            &event_tx,
            &state.session_id,
            &config,
            state.metrics_collector.as_ref(),
            sent_complete,
            &mut state.runtime_state,
        )
        .await;

        Ok(())
    }
    .instrument(session_span)
    .await
}

#[cfg(test)]
mod hook_tests {
    use super::*;
    use async_trait::async_trait;
    use bamboo_agent_core::tools::{ToolCall, ToolError, ToolResult, ToolSchema};
    use bamboo_agent_core::{AgentHook, Message};
    use bamboo_domain::{HookResult, Role};
    use bamboo_llm::provider::LLMStream;

    struct PanicProvider;

    #[async_trait]
    impl LLMProvider for PanicProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> bamboo_llm::provider::Result<LLMStream> {
            panic!("LLM must not run after a BeforeRound abort")
        }
    }

    struct EmptyTools;

    #[async_trait]
    impl ToolExecutor for EmptyTools {
        async fn execute(&self, _call: &ToolCall) -> Result<ToolResult, ToolError> {
            panic!("tools must not run in lifecycle-hook tests")
        }

        fn list_tools(&self) -> Vec<ToolSchema> {
            Vec::new()
        }
    }

    struct AbortRoundHook;

    #[async_trait]
    impl AgentHook for AbortRoundHook {
        fn point(&self) -> AgentHookPoint {
            AgentHookPoint::BeforeRound
        }

        async fn run(
            &self,
            _point: AgentHookPoint,
            payload: &HookPayload,
            _session: &Session,
        ) -> HookResult {
            assert_eq!(payload, &HookPayload::Round { round: 1 });
            HookResult::Abort {
                reason: "round rejected".to_string(),
            }
        }
    }

    struct InjectSetupHook;

    #[async_trait]
    impl AgentHook for InjectSetupHook {
        fn point(&self) -> AgentHookPoint {
            AgentHookPoint::AfterSessionSetup
        }

        async fn run(
            &self,
            _point: AgentHookPoint,
            payload: &HookPayload,
            _session: &Session,
        ) -> HookResult {
            assert!(matches!(
                payload,
                HookPayload::SessionSetup { initial_message } if initial_message == "hello hooks"
            ));
            HookResult::InjectContext {
                text: "injected setup context".to_string(),
            }
        }
    }

    fn config_with_hooks(hooks: Vec<Arc<dyn AgentHook>>) -> AgentLoopConfig {
        let mut runner = crate::runtime::hooks::HookRunner::new();
        for hook in hooks {
            runner.register(hook);
        }
        AgentLoopConfig {
            model_name: Some("model".to_string()),
            hook_runner: Arc::new(runner),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn before_round_hook_aborts_before_llm_call() {
        let mut session = Session::new("abort-round", "model");
        let (event_tx, _event_rx) = mpsc::channel(32);
        let error = run_agent_loop_with_config(
            &mut session,
            "hello hooks".to_string(),
            event_tx,
            Arc::new(PanicProvider),
            Arc::new(EmptyTools),
            CancellationToken::new(),
            config_with_hooks(vec![Arc::new(AbortRoundHook)]),
        )
        .await
        .expect_err("BeforeRound abort must terminate the run");
        assert!(
            matches!(error, bamboo_agent_core::AgentError::Tool(message) if message.contains("round rejected"))
        );
    }

    #[tokio::test]
    async fn after_session_setup_hook_injects_context_before_round() {
        let mut session = Session::new("inject-setup", "model");
        let (event_tx, _event_rx) = mpsc::channel(32);
        let error = run_agent_loop_with_config(
            &mut session,
            "hello hooks".to_string(),
            event_tx,
            Arc::new(PanicProvider),
            Arc::new(EmptyTools),
            CancellationToken::new(),
            config_with_hooks(vec![Arc::new(InjectSetupHook), Arc::new(AbortRoundHook)]),
        )
        .await
        .expect_err("the test's BeforeRound hook stops after setup");
        assert!(matches!(error, bamboo_agent_core::AgentError::Tool(_)));
        let injected = session.messages.iter().find(|message| {
            message.role == Role::System && message.content.contains("injected setup context")
        });
        assert!(
            injected.is_some(),
            "hook context must be stored in the session"
        );
        assert!(injected.is_some_and(|message| message.never_compress));
    }
}
