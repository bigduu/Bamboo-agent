use std::path::PathBuf;

use crate::app_state::update_admission::UntrackedUpdateAdmission;
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
    update_admission: Option<&UntrackedUpdateAdmission>,
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
        HookRunner::mark_user_prompt_prechecked(session, raw_prompt);
        return Ok(raw_prompt.to_string());
    }
    // These effects can start before a Root owns the prompt or even when the
    // prompt is rejected and no Root ever starts.
    if let Some(admission) = update_admission {
        admission
            .record_dispatch()
            .await
            .map_err(|error| error.to_string())?;
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
        HookRunner::mark_user_prompt_prechecked(session, raw_prompt);
        return Ok(raw_prompt.to_string());
    }
    let effective_prompt = format!(
        "{raw_prompt}\n\n{USER_PROMPT_CONTEXT_START}\n{}\n{USER_PROMPT_CONTEXT_END}",
        contexts.join("\n\n---\n\n")
    );
    HookRunner::mark_user_prompt_prechecked(session, &effective_prompt);
    Ok(effective_prompt)
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

    async fn install_portable_prompt_fixture(profile: &std::path::Path, command: &str) {
        use bamboo_plugin::{
            InstalledPlugin, InstalledPlugins, PluginInstallStatus, PluginManifest, PluginSource,
            RegisteredCapabilities,
        };
        let root = profile.join("plugins");
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
        std::fs::write(
            bundle.join("hooks.json"),
            serde_json::to_vec(&serde_json::json!({
                "hooks": {"UserPromptSubmit": [{"hooks": [
                    {"type": "command", "command": command, "timeout": 1}
                ]}]}
            }))
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
    }

    #[tokio::test]
    async fn portable_prompt_context_reaches_user_prompt_without_system_injection() {
        let temp = tempfile::tempdir().unwrap();
        install_portable_prompt_fixture(temp.path(), "printf 'portable context'").await;
        let mut session = Session::new("portable-prompt", "model");
        let prompt = apply_user_prompt_submit_hooks(
            &LifecycleHooksConfig::default(),
            Some(temp.path().to_owned()),
            &mut session,
            "raw prompt",
            None,
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
    async fn managed_update_host_refuses_a_portable_hook_that_rejected_a_prompt() {
        use crate::app_state::{planned_update::HostUpdateReadiness, AppState};
        use std::time::Duration;

        let profile = tempfile::tempdir().unwrap();
        install_portable_prompt_fixture(
            profile.path(),
            "printf 'managed portable fixture rejected the prompt' >&2; exit 2",
        )
        .await;
        let state = AppState::new_for_managed_update(profile.path().to_owned())
            .await
            .unwrap();
        state.wait_for_boot_reconcile_services().await;
        let mut session = Session::new("blocked-portable-prompt", "model");
        let error = apply_user_prompt_submit_hooks(
            &LifecycleHooksConfig::default(),
            Some(profile.path().to_owned()),
            &mut session,
            "raw prompt",
            state.managed_lifecycle_hook_admission(),
        )
        .await
        .unwrap_err();
        assert!(error.contains("managed portable fixture rejected the prompt"));
        assert!(session.messages.is_empty());

        let mut preparation = state.begin_planned_update().await.unwrap();
        let error = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match preparation.observe().await {
                    Err(error) => break error,
                    Ok(HostUpdateReadiness::Busy { .. }) => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Ok(HostUpdateReadiness::Ready) => panic!("untracked hook cannot prove closure"),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
        assert!(error.to_string().contains("lifecycle hook"));
        drop(preparation);
        assert!(state.begin_planned_update().await.is_ok());
        state.shutdown().await;
    }

    #[tokio::test]
    async fn prompt_block_is_fail_closed_and_never_appends_a_message() {
        let mut session = Session::new("blocked-prompt", "model");
        let result = apply_user_prompt_submit_hooks(
            &config("payload=$(cat); case \"$payload\" in *'\"prompt\":\"raw prompt\"'*) printf 'policy says no' >&2; exit 2 ;; *) exit 1 ;; esac"),
            None,
            &mut session,
            "raw prompt",
            None,
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
            None,
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
            None,
        )
        .await;

        assert!(result.is_err_and(|reason| reason.contains("no owning parent agent")));
        assert!(session.messages.is_empty());
    }
}
