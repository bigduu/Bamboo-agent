use std::path::PathBuf;

use bamboo_agent_core::Session;
use bamboo_config::LifecycleHooksConfig;
use bamboo_domain::{AgentHookPoint, AgentRuntimeState, HookPayload, HookResult};
use bamboo_engine::HookRunner;

const USER_PROMPT_CONTEXT_START: &str = "<user_prompt_submit_context>";
const USER_PROMPT_CONTEXT_END: &str = "</user_prompt_submit_context>";

/// Run config-backed `UserPromptSubmit` hooks before the caller persists the
/// user message. A blocked result is fail-closed and carries the hook reason;
/// injected context is returned as a clearly-delimited extension of the user
/// message without changing the raw prompt delivered to the hook.
pub(crate) async fn apply_user_prompt_submit_hooks(
    config: &LifecycleHooksConfig,
    fallback_cwd: Option<PathBuf>,
    session: &mut Session,
    raw_prompt: &str,
) -> Result<String, String> {
    let runner = HookRunner::new().with_lifecycle_config(config, fallback_cwd);
    let mut runtime_state = session
        .agent_runtime_state
        .clone()
        .unwrap_or_else(|| AgentRuntimeState::new(&session.id));
    // A submitted user prompt starts a new run. Preserve sticky control fields
    // on the cloned state, but reset per-run hook observations before recording
    // this prompt's checkpoints.
    runtime_state.checkpoints.clear();
    runtime_state.hook_contexts.clear();
    runtime_state.stop_hook_forced_continuations = 0;
    if !runner.has_hooks_for(AgentHookPoint::BeforeSessionSetup) {
        session.agent_runtime_state = Some(runtime_state);
        return Ok(raw_prompt.to_string());
    }
    let outcome = runner
        .run_hooks(
            AgentHookPoint::BeforeSessionSetup,
            &HookPayload::Prompt {
                prompt: raw_prompt.to_string(),
            },
            session,
            &mut runtime_state,
            None,
        )
        .await;
    session.agent_runtime_state = Some(runtime_state);

    let blocked_reason = match outcome.decision {
        HookResult::Deny { reason } | HookResult::Abort { reason } => Some(reason),
        HookResult::Suspend { reason } => Some(format!("hook suspended prompt submission: {reason}")),
        HookResult::Ask => Some(
            "UserPromptSubmit hook requested parent-agent review, but a user prompt has no owning parent agent"
                .to_string(),
        ),
        HookResult::Continue
        | HookResult::Mutated
        | HookResult::Allow
        | HookResult::InjectContext { .. } => None,
        HookResult::WithContext { .. } => unreachable!("hook runner unwraps context results"),
    };
    if let Some(reason) = blocked_reason {
        return Err(reason);
    }

    let contexts = outcome
        .injected_contexts
        .into_iter()
        .map(|context| context.trim().to_string())
        .filter(|context| !context.is_empty())
        .collect::<Vec<_>>();
    if contexts.is_empty() {
        return Ok(raw_prompt.to_string());
    }
    Ok(format!(
        "{raw_prompt}\n\n{USER_PROMPT_CONTEXT_START}\n{}\n{USER_PROMPT_CONTEXT_END}",
        contexts.join("\n\n---\n\n")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_config::{
        LifecycleHookGroup, LifecycleHookHandler, DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS,
    };

    fn config(command: &str) -> LifecycleHooksConfig {
        LifecycleHooksConfig {
            enabled: true,
            user_prompt_submit: vec![LifecycleHookGroup {
                enabled: true,
                matcher: None,
                hooks: vec![LifecycleHookHandler::command(
                    command,
                    DEFAULT_LIFECYCLE_HOOK_TIMEOUT_MS,
                )],
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn prompt_block_is_fail_closed_and_never_appends_a_message() {
        let mut session = Session::new("blocked-prompt", "model");
        let result = apply_user_prompt_submit_hooks(
            &config("payload=$(cat); case \"$payload\" in *'\"prompt\":\"raw prompt\"'*) printf 'policy says no' >&2; exit 2 ;; *) exit 1 ;; esac"),
            None,
            &mut session,
            "raw prompt",
        )
        .await;

        assert_eq!(result, Err("policy says no".to_string()));
        assert!(session.messages.is_empty());
        assert_eq!(
            session
                .agent_runtime_state
                .as_ref()
                .map(|state| state.checkpoints.len()),
            Some(1)
        );
    }

    #[tokio::test]
    async fn prompt_context_is_delimited_and_raw_prompt_is_unchanged_on_stdin() {
        let mut session = Session::new("context-prompt", "model");
        let prompt = apply_user_prompt_submit_hooks(
            &config(
                "payload=$(cat); case \"$payload\" in *'\"prompt\":\"raw prompt\"'*) printf '%s' '{\"additional_context\":\"workspace policy\"}' ;; *) exit 2 ;; esac",
            ),
            None,
            &mut session,
            "raw prompt",
        )
        .await
        .expect("context hook should pass");

        assert_eq!(
            prompt,
            "raw prompt\n\n<user_prompt_submit_context>\nworkspace policy\n</user_prompt_submit_context>"
        );
        assert!(session.messages.is_empty());
    }

    #[tokio::test]
    async fn prompt_ask_has_no_manual_fallback_and_fails_closed_without_parent() {
        let mut session = Session::new("ask-prompt", "model");
        let result = apply_user_prompt_submit_hooks(
            &config("printf '%s' '{\"decision\":\"ask\"}'"),
            None,
            &mut session,
            "raw prompt",
        )
        .await;

        assert!(result.is_err_and(|reason| reason.contains("no owning parent agent")));
        assert!(session.messages.is_empty());
    }
}
