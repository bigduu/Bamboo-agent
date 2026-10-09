use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::runtime::config::AgentLoopConfig;
use bamboo_agent_core::tools::ToolExecutor;
use bamboo_agent_core::{AgentEvent, Session};
use bamboo_domain::{AgentHookPoint, HookPayload, SessionStartSource};
use bamboo_llm::LLMProvider;

mod final_answer;
mod gold;
mod pipeline;
mod startup;

use pipeline::run_pipeline;
pub(in crate::runtime::runner) use pipeline::{
    legacy_browser_loaded_result_content, legacy_browser_needs_discovery,
};
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
        let session_start_source = session
            .metadata
            .remove(crate::session_app::chat::SESSION_START_SOURCE_METADATA_KEY)
            .as_deref()
            .map(|source| match source {
                "resume" => SessionStartSource::Resume,
                _ => SessionStartSource::Startup,
            })
            .unwrap_or_else(|| {
                let has_prior_run = session.last_run_status().is_some()
                    || session.messages.iter().any(|message| {
                        matches!(
                            message.role,
                            bamboo_agent_core::Role::Assistant | bamboo_agent_core::Role::Tool
                        )
                    });
                if has_prior_run {
                    SessionStartSource::Resume
                } else {
                    SessionStartSource::Startup
                }
            });
        let submitted_message = initial_message;
        let system_resume = config.skip_initial_user_message
            && session.messages.last().is_some_and(|message| {
                message.content == submitted_message
                    && bamboo_domain::session::is_system_resume_message(message)
            });
        let initial_message = if system_resume {
            // Runtime child/guardian/retry notifications are continuations, not
            // new user submissions. Never carry a stale one-shot receipt into
            // the next genuine external prompt.
            session.metadata.remove("runtime.plugin_prompt_prechecked");
            submitted_message.clone()
        } else {
            config
                .hook_runner
                .apply_portable_user_prompt(session, &submitted_message)
                .await?
        };
        if config.skip_initial_user_message && initial_message != submitted_message {
            if let Some(message) = session.messages.iter_mut().rev().find(|message| {
                message.role == bamboo_agent_core::Role::User
                    && message.content == submitted_message
            }) {
                message.content = initial_message.clone();
            }
        }
        if let Some(inputs) = config.initial_untrusted_inputs.as_ref() {
            // Observation failure denies the optional Skill caller while
            // retaining the ordinary SDK append and execution semantics.
            if let Err(error) = inputs
                .seal_sdk_input(session, config.persistence.as_ref())
                .await
            {
                tracing::warn!(session_id = %session.id, %error, "SDK input remains unsealed");
            }
        }
        super::state_bridge::ensure_initial_root_tool_authority(session, config.storage.as_ref())
            .await?;
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
                initial_message: submitted_message.clone(),
                source: session_start_source,
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
                HookPayload::SessionSetup {
                    initial_message,
                    source: SessionStartSource::Startup,
                } if initial_message == "hello hooks"
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

    async fn portable_prompt_config(command: &str) -> (tempfile::TempDir, AgentLoopConfig) {
        use bamboo_plugin::{
            InstalledPlugin, InstalledPlugins, PluginInstallStatus, PluginManifest, PluginSource,
            RegisteredCapabilities,
        };
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("plugins");
        let bundle = root.join("prompt-policy");
        std::fs::create_dir_all(&bundle).unwrap();
        let manifest: PluginManifest = serde_json::from_value(serde_json::json!({
            "id":"prompt-policy", "name":"Prompt policy", "version":"0.1.0",
            "provides":{"hooks":[{"config":"hooks.json","scripts":["policy.sh"]}]}
        }))
        .unwrap();
        std::fs::write(
            bundle.join("plugin.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        std::fs::write(bundle.join("policy.sh"), "# reviewed fixture").unwrap();
        std::fs::write(
            bundle.join("hooks.json"),
            serde_json::to_vec(&serde_json::json!({"hooks":{
                "UserPromptSubmit":[{"hooks":[{"type":"command","command":command,"timeout":1}]}]
            }}))
            .unwrap(),
        )
        .unwrap();
        let mut hooks = bamboo_plugin::hooks::registrations(&manifest, &bundle).unwrap();
        let digest = hooks[0].digest.clone();
        hooks[0].confirm_review(&digest).unwrap();
        InstalledPlugins {
            plugins: vec![InstalledPlugin {
                id: manifest.id,
                version: manifest.version,
                source: PluginSource::LocalDir {
                    path: bundle.clone(),
                },
                plugin_dir: bundle,
                installed_at: chrono::Utc::now(),
                status: PluginInstallStatus::Installed,
                registered: RegisteredCapabilities {
                    hooks,
                    ..Default::default()
                },
            }],
        }
        .save(&root.join("installed.json"))
        .await
        .unwrap();
        let mut runner = crate::runtime::hooks::HookRunner::new().with_lifecycle_config(
            &bamboo_config::LifecycleHooksConfig::default(),
            Some(temp.path().to_owned()),
        );
        runner.register(Arc::new(AbortRoundHook));
        (
            temp,
            AgentLoopConfig {
                model_name: Some("model".into()),
                hook_runner: Arc::new(runner),
                ..Default::default()
            },
        )
    }

    #[tokio::test]
    async fn portable_prompt_blocks_shared_loop_before_provider_or_session_setup() {
        let (_temp, config) = portable_prompt_config("printf 'prompt blocked' >&2; exit 2").await;
        let mut session = Session::new("portable-block", "model");
        let (tx, _rx) = mpsc::channel(32);
        let error = run_agent_loop_with_config(
            &mut session,
            "raw prompt".into(),
            tx,
            Arc::new(PanicProvider),
            Arc::new(EmptyTools),
            CancellationToken::new(),
            config,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("prompt blocked"));
        assert!(session.messages.is_empty());
    }

    #[tokio::test]
    async fn portable_prompt_context_reaches_shared_loop_and_preappended_sdk_input() {
        for preappended in [false, true] {
            let (_temp, mut config) =
                portable_prompt_config("printf 'portable prompt context'").await;
            config.skip_initial_user_message = preappended;
            let mut session = Session::new("portable-context", "model");
            if preappended {
                session.add_message(Message::user("raw prompt"));
            }
            let (tx, _rx) = mpsc::channel(32);
            let error = run_agent_loop_with_config(
                &mut session,
                "raw prompt".into(),
                tx,
                Arc::new(PanicProvider),
                Arc::new(EmptyTools),
                CancellationToken::new(),
                config,
            )
            .await
            .unwrap_err();
            assert!(error.to_string().contains("round rejected"));
            let prompts = session
                .messages
                .iter()
                .filter(|message| message.role == Role::User)
                .collect::<Vec<_>>();
            assert_eq!(prompts.len(), 1);
            assert!(prompts[0]
                .content
                .contains("untrusted; source prompt-policy@0.1.0"));
            assert!(prompts[0].content.contains("portable prompt context"));
            assert!(!session
                .messages
                .iter()
                .any(|message| message.role == Role::System
                    && message.content.contains("portable prompt context")));
        }
    }

    #[tokio::test]
    async fn portable_prompt_does_not_reprocess_structured_runtime_resumes() {
        for metadata in [
            serde_json::json!({"hidden_from_ui":true,"runtime_kind":"child_completion_resume"}),
            serde_json::json!({"runtime_kind":"retry_resume"}),
            serde_json::json!({"hidden_from_ui":true}),
        ] {
            let (_temp, mut config) =
                portable_prompt_config("printf 'prompt blocked' >&2; exit 2").await;
            config.skip_initial_user_message = true;
            let runner = config.hook_runner.clone();
            let mut session = Session::new("portable-internal-resume", "model");
            let mut message = Message::user("runtime continuation");
            message.metadata = Some(metadata);
            session.add_message(message);
            crate::runtime::hooks::HookRunner::mark_user_prompt_prechecked(
                &mut session,
                "later prompt",
            );
            let (tx, _rx) = mpsc::channel(32);
            let error = run_agent_loop_with_config(
                &mut session,
                "runtime continuation".into(),
                tx,
                Arc::new(PanicProvider),
                Arc::new(EmptyTools),
                CancellationToken::new(),
                config,
            )
            .await
            .unwrap_err();
            assert!(error.to_string().contains("round rejected"), "{error}");
            assert!(!session
                .metadata
                .contains_key("runtime.plugin_prompt_prechecked"));
            assert!(runner
                .apply_portable_user_prompt(&mut session, "later prompt")
                .await
                .is_err());
        }

        // The same text, preappended as a genuine user follow-up, still passes
        // through UserPromptSubmit. Resume alone is not a policy exemption.
        let (_temp, mut config) =
            portable_prompt_config("printf 'prompt blocked' >&2; exit 2").await;
        config.skip_initial_user_message = true;
        let mut session = Session::new("portable-user-followup", "model");
        session.add_message(Message::user("runtime continuation"));
        let (tx, _rx) = mpsc::channel(32);
        let error = run_agent_loop_with_config(
            &mut session,
            "runtime continuation".into(),
            tx,
            Arc::new(PanicProvider),
            Arc::new(EmptyTools),
            CancellationToken::new(),
            config,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("prompt blocked"));
    }

    #[tokio::test]
    async fn portable_prompt_server_receipt_is_exact_and_consumed_once() {
        let (_temp, config) = portable_prompt_config("printf 'must run once' >&2; exit 2").await;
        let runner = config.hook_runner.clone();
        let mut session = Session::new("portable-prechecked", "model");
        crate::runtime::hooks::HookRunner::mark_user_prompt_prechecked(
            &mut session,
            "accepted prompt",
        );
        let (tx, _rx) = mpsc::channel(32);
        let error = run_agent_loop_with_config(
            &mut session,
            "accepted prompt".into(),
            tx,
            Arc::new(PanicProvider),
            Arc::new(EmptyTools),
            CancellationToken::new(),
            config,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("round rejected"));
        assert!(!session
            .metadata
            .contains_key("runtime.plugin_prompt_prechecked"));
        assert!(runner
            .apply_portable_user_prompt(&mut session, "accepted prompt")
            .await
            .unwrap_err()
            .to_string()
            .contains("must run once"));
        crate::runtime::hooks::HookRunner::mark_user_prompt_prechecked(
            &mut session,
            "different prompt",
        );
        assert!(runner
            .apply_portable_user_prompt(&mut session, "accepted prompt")
            .await
            .is_err());
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
        assert!(session.messages.iter().all(|message| {
            message.role != Role::System || !message.content.contains("injected setup context")
        }));
        assert_eq!(
            session
                .agent_runtime_state
                .as_ref()
                .map(|state| state.hook_contexts.as_slice()),
            Some(["injected setup context".to_string()].as_slice()),
            "session hook context must ride runtime state, not the cached system prompt"
        );
    }
}
