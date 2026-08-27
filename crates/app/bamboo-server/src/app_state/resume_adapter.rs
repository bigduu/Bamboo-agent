//! Resume execution port adapter.
//!
//! Bridges the application-layer `ResumeExecutionPort` trait to server
//! infrastructure (storage, runner lifecycle, agent spawning).
//!
//! `AppStateResumeRef` is a newtype wrapper around `Data<AppState>` to satisfy
//! Rust's orphan rules (can't impl a foreign trait on a foreign type).

use async_trait::async_trait;
use bamboo_agent_core::AgentEvent;
use bamboo_engine::execution::{reserve_session_execution, SessionExecutionReserveOutcome};
use bamboo_engine::model_areas::resolve_global_area_models;
use bamboo_engine::model_config_helper::{
    resolve_gold_config, resolve_provider_routing_key, resolve_provider_type,
    GOLD_CONFIG_METADATA_KEY,
};
use bamboo_engine::session_app::approval_replay::{
    apply_permission_replay_result, find_permission_replay_target, refresh_approval_replay_posture,
    repark_permission_replay, restore_permission_replay_authorization, ApprovalReplayDecision,
    PermissionReplayTarget,
};
use bamboo_engine::session_app::execute::consume_pending_clarification_resume;
use bamboo_engine::session_app::provider_model::{persist_model_ref, session_effective_model_ref};
use bamboo_engine::session_app::respond::{
    PERMISSION_REEXECUTE_GENERATION_METADATA_KEY, PERMISSION_REEXECUTE_METADATA_KEY,
};
use bamboo_engine::session_app::resume::{ResumeExecutionPort, ResumeSpawnRequest};
use tokio::sync::broadcast;

use super::session_events::get_or_create_event_sender;
use super::AppState;
use crate::handlers::agent::execute::runtime::SpawnAgentExecution;
use crate::handlers::agent::execute::{spawn_agent_execution, spawn_event_forwarder};

/// Newtype wrapper that implements `ResumeExecutionPort`.
///
/// Needed because Rust's orphan rules prevent implementing
/// `bamboo_engine::session_app::resume::ResumeExecutionPort` directly on
/// `actix_web::web::Data<AppState>`.
pub struct AppStateResumeRef(pub actix_web::web::Data<AppState>);

#[async_trait]
impl ResumeExecutionPort for AppStateResumeRef {
    async fn load_session(&self, session_id: &str) -> Option<bamboo_agent_core::Session> {
        AppState::load_session(&self.0, session_id).await
    }

    async fn save_and_cache_session(&self, session: &mut bamboo_agent_core::Session) {
        AppState::save_and_cache_session(&self.0, session).await;
    }

    async fn reserve_session_execution(
        &self,
        session_id: &str,
        event_sender: &broadcast::Sender<AgentEvent>,
    ) -> SessionExecutionReserveOutcome {
        reserve_session_execution(
            &self.0.agent,
            &self.0.agent_runners,
            &self.0.session_event_senders,
            session_id,
            event_sender,
        )
        .await
    }

    async fn get_or_create_event_sender(&self, session_id: &str) -> broadcast::Sender<AgentEvent> {
        get_or_create_event_sender(&self.0.session_event_senders, session_id).await
    }

    fn dispatch_resume_execution(
        &self,
        request: ResumeSpawnRequest,
    ) -> Result<(), ResumeSpawnRequest> {
        let owner = AppStateResumeRef(self.0.clone());
        tokio::spawn(async move {
            ResumeExecutionPort::spawn_resume_execution(&owner, request).await;
        });
        Ok(())
    }

    async fn spawn_resume_execution(&self, request: ResumeSpawnRequest) {
        let ResumeSpawnRequest {
            session_id,
            mut session,
            mut execution_reservation,
            event_sender,
            config,
        } = request;
        if let Err(error) = execution_reservation.ensure_registered().await {
            tracing::warn!(
                %session_id,
                run_id = %execution_reservation.run_id(),
                %error,
                "cannot resume server session without exact router ownership"
            );
            return;
        }

        let config_snapshot = self.0.config.read().await.clone();
        let model = session.model.clone();
        let session_model_ref = session_effective_model_ref(&session);
        let requested_provider = session_model_ref
            .as_ref()
            .map(|model_ref| model_ref.provider.as_str())
            .unwrap_or(config.provider_name.as_str());
        let resolved_provider_name = match resolve_provider_routing_key(
            &config_snapshot,
            requested_provider,
            &self.0.provider_registry,
        ) {
            Ok(provider) => provider,
            Err(error) => {
                tracing::error!(
                    %session_id,
                    provider = requested_provider,
                    %error,
                    "resume provider target is unavailable; refusing to fall back"
                );
                execution_reservation.abandon().await;
                return;
            }
        };
        if let Some(mut model_ref) = session_model_ref {
            model_ref.provider = resolved_provider_name.clone();
            persist_model_ref(&mut session, &model_ref);
        }
        let provider_override = match self.0.provider_registry.get(&resolved_provider_name) {
            Some(provider) => provider,
            None => {
                tracing::error!(
                    %session_id,
                    provider = %resolved_provider_name,
                    "resume provider disappeared after resolution; refusing to fall back"
                );
                execution_reservation.abandon().await;
                return;
            }
        };
        let resolved_provider_type = resolve_provider_type(
            &config_snapshot,
            &resolved_provider_name,
            &self.0.provider_registry,
        );
        // Auxiliary models are global (config-derived), never session-bound.
        let areas = resolve_global_area_models(
            &config_snapshot,
            &resolved_provider_name,
            &self.0.provider_registry,
        );
        let resolved_fast_model = config
            .fast_model
            .clone()
            .or_else(|| areas.fast.as_ref().map(|m| m.model_name.clone()));
        let resolved_fast_provider = areas.fast.map(|m| m.provider);
        let resolved_background_model = config
            .background_model
            .clone()
            .or_else(|| areas.background.as_ref().map(|m| m.model_name.clone()));
        let resolved_bg_provider = config
            .background_model_provider
            .clone()
            .or_else(|| areas.background.map(|m| m.provider));
        let resolved_summarization_model = config
            .summarization_model
            .clone()
            .or_else(|| areas.summarization.as_ref().map(|m| m.model_name.clone()));
        let resolved_summarization_provider = config
            .summarization_model_provider
            .clone()
            .or_else(|| areas.summarization.map(|m| m.provider));
        let is_child_session = session.kind == bamboo_agent_core::SessionKind::Child;
        let reasoning_effort = session.reasoning_effort;
        let reasoning_effort_source = session
            .metadata
            .get("reasoning_effort_source")
            .cloned()
            .unwrap_or_default();

        let image_fallback = config.image_fallback.clone();
        let gold_config = resolve_gold_config(
            &config_snapshot,
            session
                .metadata
                .get(GOLD_CONFIG_METADATA_KEY)
                .map(String::as_str),
        )
        .or(config.gold_config.clone());

        let (mpsc_tx, mpsc_rx) = tokio::sync::mpsc::channel::<bamboo_agent_core::AgentEvent>(100);

        let state = self.0.clone();
        spawn_event_forwarder(
            state.clone(),
            session_id.clone(),
            execution_reservation.run_id().to_string(),
            mpsc_rx,
            event_sender,
            gold_config.clone(),
        );

        let model_roster = bamboo_engine::ModelRoster {
            model: Some(model),
            provider_name: Some(resolved_provider_name.clone()),
            provider_type: resolved_provider_type,
            fast: bamboo_engine::RoleModel::from_parts(resolved_fast_model, resolved_fast_provider),
            background: bamboo_engine::RoleModel::from_parts(
                resolved_background_model,
                resolved_bg_provider,
            ),
            summarization: bamboo_engine::RoleModel::from_parts(
                resolved_summarization_model,
                resolved_summarization_provider,
            ),
        };

        // If the user just approved a permission prompt, the gated tool call was
        // intercepted before it ran — its result is only a placeholder. The grant
        // has already been recorded (by the respond handler) on the shared
        // permission checker, so re-execute the tool now for real, write the
        // output back, then start the loop. This happens off the /respond response
        // path (in this spawned task) and streams via the same mpsc → forwarder,
        // so the re-run shows up live and the model sees genuine output instead of
        // inferring it. The common (non-permission) resume path is unchanged.
        let reexecute_tool_call_id = session
            .metadata
            .get(PERMISSION_REEXECUTE_METADATA_KEY)
            .cloned();
        let reexecute_request_generation = session
            .metadata
            .get(PERMISSION_REEXECUTE_GENERATION_METADATA_KEY)
            .cloned();
        let reexecute_tool_call_id = match reexecute_tool_call_id {
            None => {
                if reexecute_request_generation.is_some() {
                    tracing::error!(
                        %session_id,
                        "orphaned permission replay generation marker; refusing to resume"
                    );
                    return;
                }
                // Keep the durable startup marker armed across every async
                // preparation step. Clear it only in the in-memory snapshot at
                // the synchronous handoff to the spawned runner; the runner's
                // first checkpoint durably acknowledges takeover.
                consume_pending_clarification_resume(&mut session);
                spawn_agent_execution(SpawnAgentExecution {
                    state: state.clone(),
                    session_id,
                    session,
                    execution_reservation,
                    is_child_session,
                    provider_name: resolved_provider_name,
                    provider_override: Some(provider_override.clone()),
                    model_roster,
                    reasoning_effort,
                    reasoning_effort_source,
                    disabled_tools: config.disabled_tools,
                    disabled_skill_ids: config.disabled_skill_ids,
                    mpsc_tx,
                    image_fallback,
                    gold_config,
                    app_data_dir: Some(state.app_data_dir.clone()),
                    // Resume has no per-request override channel; the
                    // config-level default (issue #221) still applies.
                    run_budget: None,
                });
                return;
            }
            Some(id) => id,
        };

        tokio::spawn(async move {
            let mut session = session;

            if let Some(replay_target) = find_pending_tool_call(
                &session,
                &reexecute_tool_call_id,
                reexecute_request_generation.as_deref(),
            ) {
                if reexecute_request_generation.is_none()
                    && replay_target.request_generation().is_some()
                {
                    tracing::error!(
                        %session_id,
                        tool_call_id = %reexecute_tool_call_id,
                        "typed permission replay is missing its generation marker; refusing to resume"
                    );
                    return;
                }
                let tool_call = replay_target.tool_call().clone();
                let tool_name = tool_call.function.name.clone();
                let configured_mode = state
                    .permission_checker
                    .permission_config()
                    .map(|config| config.mode())
                    .unwrap_or_default();
                let decision = match refresh_approval_replay_posture(
                    state.storage.as_ref(),
                    &mut session,
                    configured_mode,
                    &tool_name,
                )
                .await
                {
                    Ok(decision) => decision,
                    Err(error) => {
                        // The durable marker remains intact so a later retry can
                        // re-check policy; no ToolStart or executor entry occurs.
                        tracing::error!(
                            %session_id,
                            tool_call_id = %reexecute_tool_call_id,
                            %error,
                            "approval replay posture refresh failed closed"
                        );
                        return;
                    }
                };
                session.metadata.remove(PERMISSION_REEXECUTE_METADATA_KEY);
                session
                    .metadata
                    .remove(PERMISSION_REEXECUTE_GENERATION_METADATA_KEY);

                let (content, success) = match decision {
                    ApprovalReplayDecision::BlockedByPlan(_) => (
                        format!(
                            "Plan mode blocked approved mutating tool '{tool_name}'; the stale approval was not executed"
                        ),
                        false,
                    ),
                    ApprovalReplayDecision::Execute(flags) => {
                        let Some(permission_config) =
                            state.permission_checker.permission_config()
                        else {
                            tracing::error!(
                                %session_id,
                                tool_call_id = %reexecute_tool_call_id,
                                "typed approval replay has no permission configuration; refusing to resume"
                            );
                            return;
                        };
                        if let Err(error) = restore_permission_replay_authorization(
                            permission_config.as_ref(),
                            &session,
                            &replay_target,
                        ) {
                            tracing::error!(
                                %session_id,
                                tool_call_id = %reexecute_tool_call_id,
                                %error,
                                "typed approval replay authorization recovery failed closed"
                            );
                            return;
                        }
                        let executor = state.tools_for(crate::tools::ToolSurface::Root);
                        let is_mutating = bamboo_tools::orchestrator::classify_tool(&tool_name)
                            == bamboo_tools::orchestrator::ToolMutability::Mutating;

                        // Only an admitted replay emits lifecycle start.
                        let mut emitter = bamboo_tools::ToolEmitter::new(
                            &tool_call.id,
                            &tool_name,
                            is_mutating,
                        );
                        emitter.set_auto_approved(true);
                        let _ = mpsc_tx
                            .send(emitter.begin().clone().into_agent_event())
                            .await;
                        let exec_result = bamboo_tools::permission::with_permission_replay_generation(
                            session.id.as_str(),
                            reexecute_tool_call_id.as_str(),
                            reexecute_request_generation.as_deref(),
                            executor.execute_with_context(
                                    &tool_call,
                                bamboo_agent_core::tools::ToolExecutionContext {
                                    session_id: Some(session.id.as_str()),
                                    root_session_id: Some(
                                        if session.root_session_id.trim().is_empty() {
                                            session.id.as_str()
                                        } else {
                                            session.root_session_id.as_str()
                                        },
                                    ),
                                    tool_call_id: reexecute_tool_call_id.as_str(),
                                    event_tx: Some(&mpsc_tx),
                                    available_tool_schemas: None,
                                    bypass_permissions: flags.bypass_permissions,
                                    auto_approve_permissions: flags.auto_approve_permissions,
                                    plan_read_only: flags.plan_read_only,
                                    can_async_resume: false,
                                    bash_completion_sink: None,
                                    pre_parsed_args: None,
                                },
                            ),
                        )
                        .await;

                        match exec_result {
                            Ok(tool_result) => {
                                match repark_permission_replay(
                                    &mut session,
                                    &replay_target,
                                    &tool_result,
                                ) {
                                    Ok(Some(reparked)) => {
                                        let _ = mpsc_tx
                                            .send(
                                                emitter
                                                    .finish(Some(
                                                        "Awaiting additional permission approval"
                                                            .to_string(),
                                                    ))
                                                    .clone()
                                                    .into_agent_event(),
                                            )
                                            .await;
                                        let _ = mpsc_tx
                                            .send(bamboo_agent_core::AgentEvent::ToolComplete {
                                                tool_call_id: tool_call.id.clone(),
                                                result: tool_result,
                                            })
                                            .await;
                                        let _ = mpsc_tx
                                            .send(bamboo_agent_core::AgentEvent::NeedClarification {
                                                question: reparked.question,
                                                options: (!reparked.options.is_empty())
                                                    .then_some(reparked.options),
                                                tool_call_id: Some(tool_call.id.clone()),
                                                tool_name: Some(tool_name.clone()),
                                                allow_custom: reparked.allow_custom,
                                                source: Some(
                                                    bamboo_agent_core::PendingQuestionSource::PauseTool,
                                                ),
                                            })
                                            .await;
                                        state.save_and_cache_session(&mut session).await;
                                        return;
                                    }
                                    Ok(None) => {}
                                    Err(error) => {
                                        tracing::error!(
                                            %session_id,
                                            tool_call_id = %reexecute_tool_call_id,
                                            %error,
                                            "additional permission replay could not be re-parked; refusing to resume"
                                        );
                                        return;
                                    }
                                }
                                let _ = mpsc_tx
                                    .send(
                                        emitter
                                            .finish(Some(
                                                "Re-executed after approval".to_string(),
                                            ))
                                            .clone()
                                            .into_agent_event(),
                                    )
                                    .await;
                                let _ = mpsc_tx
                                    .send(bamboo_agent_core::AgentEvent::ToolComplete {
                                        tool_call_id: tool_call.id.clone(),
                                        result: tool_result.clone(),
                                    })
                                    .await;
                                (tool_result.result, tool_result.success)
                            }
                            Err(error) => {
                                let message =
                                    format!("Tool re-execution after approval failed: {error}");
                                let _ = mpsc_tx
                                    .send(
                                        emitter.error(message.clone()).clone().into_agent_event(),
                                    )
                                    .await;
                                (message, false)
                            }
                        }
                    }
                };

                tracing::info!(
                    "[{}] Resolved approved tool replay '{}' ({}) -> success={}",
                    session_id,
                    tool_name,
                    reexecute_tool_call_id,
                    success
                );
                if !apply_tool_result(&mut session, &replay_target, content, success) {
                    tracing::error!(
                        %session_id,
                        tool_call_id = %reexecute_tool_call_id,
                        "approved tool replay result target changed unexpectedly; refusing to resume"
                    );
                    return;
                }
                state.save_and_cache_session(&mut session).await;
            } else {
                tracing::error!(
                    %session_id,
                    tool_call_id = %reexecute_tool_call_id,
                    request_generation = ?reexecute_request_generation,
                    "permission replay target missing or generation-mismatched; markers retained and resume refused"
                );
                return;
            }

            consume_pending_clarification_resume(&mut session);
            spawn_agent_execution(SpawnAgentExecution {
                state: state.clone(),
                session_id,
                session,
                execution_reservation,
                is_child_session,
                provider_name: resolved_provider_name,
                provider_override: Some(provider_override),
                model_roster,
                reasoning_effort,
                reasoning_effort_source,
                disabled_tools: config.disabled_tools,
                disabled_skill_ids: config.disabled_skill_ids,
                mpsc_tx,
                image_fallback,
                gold_config,
                app_data_dir: Some(state.app_data_dir.clone()),
                // Resume has no per-request override channel; the
                // config-level default (issue #221) still applies.
                run_budget: None,
            });
        });
    }
}

/// Find the concrete approved invocation, newest-first and generation-bound.
fn find_pending_tool_call(
    session: &bamboo_agent_core::Session,
    tool_call_id: &str,
    request_generation: Option<&str>,
) -> Option<PermissionReplayTarget> {
    find_permission_replay_target(session, tool_call_id, request_generation)
}

/// Overwrite only the exact generation-bound tool-result message.
fn apply_tool_result(
    session: &mut bamboo_agent_core::Session,
    target: &PermissionReplayTarget,
    content: String,
    success: bool,
) -> bool {
    apply_permission_replay_result(session, target, content, success)
}

#[cfg(test)]
mod replay_target_tests {
    use bamboo_agent_core::tools::{FunctionCall, ToolCall};
    use bamboo_agent_core::{Message, Session};

    use super::{apply_tool_result, find_pending_tool_call};

    fn append_permission_round(
        session: &mut Session,
        call_id: &str,
        generation: &str,
        arguments: &str,
        result_id: &str,
    ) {
        session.add_message(Message::assistant(
            "",
            Some(vec![ToolCall {
                id: call_id.to_string(),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: "Write".to_string(),
                    arguments: arguments.to_string(),
                },
            }]),
        ));
        let mut result = Message::tool_result(
            call_id,
            serde_json::json!({
                "status": "awaiting_permission_approval",
                "permission_request": { "request_generation": generation }
            })
            .to_string(),
        );
        result.id = result_id.to_string();
        session.add_message(result);
    }

    #[test]
    fn app_state_replay_targets_current_generation_when_provider_reuses_id() {
        let mut session = Session::new("session", "model");
        append_permission_round(
            &mut session,
            "reused",
            "generation-old",
            r#"{"content":"old"}"#,
            "result-old",
        );
        append_permission_round(
            &mut session,
            "reused",
            "generation-current",
            r#"{"content":"current"}"#,
            "result-current",
        );

        let target = find_pending_tool_call(&session, "reused", Some("generation-current"))
            .expect("current generation target");
        assert_eq!(
            target.tool_call().function.arguments,
            r#"{"content":"current"}"#
        );
        assert!(apply_tool_result(
            &mut session,
            &target,
            "executed current".to_string(),
            true,
        ));
        assert!(session.messages[1].content.contains("generation-old"));
        assert_eq!(session.messages[3].content, "executed current");
    }
}
