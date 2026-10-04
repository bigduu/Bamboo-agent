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

    let mut contexts = outcome
        .injected_contexts
        .into_iter()
        .map(|context| context.trim().to_string())
        .filter(|context| !context.is_empty())
        .collect::<Vec<_>>();
    contexts.extend(
        outcome
            .plugin_contexts
            .into_iter()
            .map(|context| context.rendered_text()),
    );
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
    async fn portable_prompt_context_reaches_user_prompt_without_system_injection() {
        use bamboo_plugin::{
            InstalledPlugin, InstalledPlugins, PluginInstallStatus, PluginManifest, PluginSource,
            RegisteredCapabilities,
        };
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("plugins");
        let bundle = root.join("fixture");
        std::fs::create_dir_all(&bundle).unwrap();
        let manifest: PluginManifest = serde_json::from_value(serde_json::json!({
            "id":"fixture", "name":"Fixture", "version":"0.1.0",
            "provides":{"hooks":[{"config":"hooks.json","scripts":["script.sh"]}]}
        }))
        .unwrap();
        std::fs::write(
            bundle.join("plugin.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        std::fs::write(bundle.join("script.sh"), "# reviewed fixture").unwrap();
        std::fs::write(bundle.join("hooks.json"), serde_json::to_vec(&serde_json::json!({"hooks":{"UserPromptSubmit":[{"hooks":[{"type":"command","command":"printf 'portable context'","timeout":1}]}]}})).unwrap()).unwrap();
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
        let mut session = Session::new("portable-prompt", "model");
        let prompt = apply_user_prompt_submit_hooks(
            &LifecycleHooksConfig::default(),
            Some(temp.path().to_owned()),
            &mut session,
            "raw prompt",
        )
        .await
        .unwrap();
        assert!(prompt.starts_with("raw prompt\n\n<user_prompt_submit_context>"));
        assert!(prompt.contains("untrusted; source fixture@0.1.0:hooks.json:UserPromptSubmit"));
        assert!(prompt.contains("portable context"));
        assert!(session.messages.is_empty());
        assert!(!session
            .metadata
            .contains_key("runtime.plugin_hook_contexts"));
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
