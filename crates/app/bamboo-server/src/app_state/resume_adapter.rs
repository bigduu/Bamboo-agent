//! Resume execution port adapter.
//!
//! Bridges the application-layer `ResumeExecutionPort` trait to server
//! infrastructure (storage, runner lifecycle, agent spawning).
//!
//! `AppStateResumeRef` is a newtype wrapper around `Data<AppState>` to satisfy
//! Rust's orphan rules (can't impl a foreign trait on a foreign type).

use async_trait::async_trait;
use bamboo_agent_core::AgentEvent;
use bamboo_engine::model_areas::resolve_global_area_models;
use bamboo_engine::model_config_helper::{
    resolve_gold_config, resolve_provider_type, GOLD_CONFIG_METADATA_KEY,
};
use bamboo_engine::session_app::provider_model::session_effective_model_ref;
use bamboo_engine::session_app::respond::PERMISSION_REEXECUTE_METADATA_KEY;
use bamboo_engine::session_app::resume::{ResumeExecutionPort, ResumeSpawnRequest};
use tokio::sync::broadcast;

use super::runner_lifecycle::{try_reserve_runner, RunnerReservation};
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

    async fn try_reserve_runner(
        &self,
        session_id: &str,
        event_sender: &broadcast::Sender<AgentEvent>,
    ) -> Option<RunnerReservation> {
        try_reserve_runner(
            &self.0.agent_runners,
            &self.0.session_event_senders,
            session_id,
            event_sender,
        )
        .await
    }

    async fn get_existing_runner_run_id(&self, session_id: &str) -> Option<String> {
        let runners = self.0.agent_runners.read().await;
        runners.get(session_id).map(|r| r.run_id.clone())
    }

    async fn get_or_create_event_sender(&self, session_id: &str) -> broadcast::Sender<AgentEvent> {
        get_or_create_event_sender(&self.0.session_event_senders, session_id).await
    }

    async fn release_reservation(&self, session_id: &str) {
        // Issue #546 row 7: this adapter's `spawn_resume_execution` always
        // spawns something (there is no early-bail path today), so this is
        // never exercised in production — implemented for trait completeness
        // and so a future bail path here is automatically safe.
        self.0.agent_runners.write().await.remove(session_id);
    }

    async fn spawn_resume_execution(&self, request: ResumeSpawnRequest) -> bool {
        let ResumeSpawnRequest {
            session_id,
            session,
            cancel_token,
            run_id: _,
            event_sender,
            config,
        } = request;

        let model = session.model.clone();
        let resolved_provider_name = session_effective_model_ref(&session)
            .map(|model_ref| model_ref.provider)
            .unwrap_or(config.provider_name);
        let config_snapshot = self.0.config.read().await.clone();
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
        let reexecute_tool_call_id = match reexecute_tool_call_id {
            None => {
                spawn_agent_execution(SpawnAgentExecution {
                    state: state.clone(),
                    session_id,
                    session,
                    is_child_session,
                    provider_name: resolved_provider_name,
                    provider_override: None,
                    model_roster,
                    reasoning_effort,
                    reasoning_effort_source,
                    disabled_tools: config.disabled_tools,
                    disabled_skill_ids: config.disabled_skill_ids,
                    cancel_token,
                    mpsc_tx,
                    image_fallback,
                    gold_config,
                    app_data_dir: Some(state.app_data_dir.clone()),
                    // Resume has no per-request override channel; the
                    // config-level default (issue #221) still applies.
                    run_budget: None,
                });
                return true;
            }
            Some(id) => id,
        };

        tokio::spawn(async move {
            let mut session = session;
            session.metadata.remove(PERMISSION_REEXECUTE_METADATA_KEY);

            if let Some(tool_call) = find_pending_tool_call(&session, &reexecute_tool_call_id) {
                let executor = state.tools_for(crate::tools::ToolSurface::Root);
                let tool_name = tool_call.function.name.clone();
                let is_mutating = bamboo_tools::orchestrator::classify_tool(&tool_name)
                    == bamboo_tools::orchestrator::ToolMutability::Mutating;

                // Frame the re-run with the same lifecycle events the normal loop
                // emits (via ToolEmitter) so the frontend updates the tool card
                // (running → finished) and ToolComplete carries the REAL output —
                // raw execute_with_context only streams tool tokens, not lifecycle.
                let mut emitter =
                    bamboo_tools::ToolEmitter::new(&tool_call.id, &tool_name, is_mutating);
                emitter.set_auto_approved(true);
                let _ = mpsc_tx
                    .send(emitter.begin().clone().into_agent_event())
                    .await;

                let exec_result = {
                    let ctx = bamboo_agent_core::tools::ToolExecutionContext {
                        session_id: Some(session.id.as_str()),
                        tool_call_id: reexecute_tool_call_id.as_str(),
                        event_tx: Some(&mpsc_tx),
                        available_tool_schemas: None,
                        bypass_permissions: false,
                        can_async_resume: false,
                        bash_completion_sink: None,
                        pre_parsed_args: None,
                    };
                    executor.execute_with_context(&tool_call, ctx).await
                };

                let (content, success) = match exec_result {
                    Ok(tool_result) => {
                        let _ = mpsc_tx
                            .send(
                                emitter
                                    .finish(Some("Re-executed after approval".to_string()))
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
                        let message = format!("Tool re-execution after approval failed: {error}");
                        let _ = mpsc_tx
                            .send(emitter.error(message.clone()).clone().into_agent_event())
                            .await;
                        (message, false)
                    }
                };

                tracing::info!(
                    "[{}] Re-executed approved tool '{}' ({}) -> success={}",
                    session_id,
                    tool_name,
                    reexecute_tool_call_id,
                    success
                );
                apply_tool_result(&mut session, &reexecute_tool_call_id, content, success);
                state.save_and_cache_session(&mut session).await;
            } else {
                tracing::warn!(
                    "[{}] Permission re-exec marker set but tool call '{}' not found in history",
                    session_id,
                    reexecute_tool_call_id
                );
            }

            spawn_agent_execution(SpawnAgentExecution {
                state: state.clone(),
                session_id,
                session,
                is_child_session,
                provider_name: resolved_provider_name,
                provider_override: None,
                model_roster,
                reasoning_effort,
                reasoning_effort_source,
                disabled_tools: config.disabled_tools,
                disabled_skill_ids: config.disabled_skill_ids,
                cancel_token,
                mpsc_tx,
                image_fallback,
                gold_config,
                app_data_dir: Some(state.app_data_dir.clone()),
                // Resume has no per-request override channel; the
                // config-level default (issue #221) still applies.
                run_budget: None,
            });
        });
        true
    }
}

/// Find the original tool call (with its arguments) by id in the session history.
fn find_pending_tool_call(
    session: &bamboo_agent_core::Session,
    tool_call_id: &str,
) -> Option<bamboo_agent_core::tools::ToolCall> {
    session.messages.iter().find_map(|message| {
        message
            .tool_calls
            .as_ref()
            .and_then(|calls| calls.iter().find(|call| call.id == tool_call_id).cloned())
    })
}

/// Overwrite the tool-result message for `tool_call_id` with the real tool output.
fn apply_tool_result(
    session: &mut bamboo_agent_core::Session,
    tool_call_id: &str,
    content: String,
    success: bool,
) {
    for message in &mut session.messages {
        if message.tool_call_id.as_deref() == Some(tool_call_id) {
            message.content = content;
            message.tool_success = Some(success);
            return;
        }
    }
}
