use actix_web::{web, HttpResponse, Responder};

use super::{ChatRequest, ChatResponse};
use crate::app_state::AppState;
use bamboo_engine::config::GoldConfig;
use bamboo_engine::model_config_helper::{
    parse_session_gold_config, resolve_gold_config, GOLD_CONFIG_METADATA_KEY,
};
use bamboo_engine::session_app::chat::{parse_goal_command, GoalCommand};
use bamboo_engine::session_app::metadata::SessionMetadataService;

mod images;
mod request;

// Sync runtime workspace so tools can resolve the working directory.
fn sync_runtime_workspace(session_id: &str, workspace_path: Option<&str>) {
    let preferred = workspace_path
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(std::path::PathBuf::from)
        .and_then(|path| std::fs::canonicalize(&path).ok().or(Some(path)))
        .filter(|path| path.is_dir());
    let _ = bamboo_tools::tools::workspace_state::ensure_session_workspace(session_id, preferred);
}

#[cfg(test)]
mod tests;

/// Create a new chat message or update an existing session.
///
/// This endpoint accepts a user message and creates or updates a chat session.
/// After calling this endpoint, use the returned `stream_url` to execute
/// the agent and receive events.
pub async fn handler(state: web::Data<AppState>, req: web::Json<ChatRequest>) -> impl Responder {
    let session_id = request::resolve_session_id(req.session_id.as_deref());
    tracing::debug!(
        "[{}] Chat requested: message_len={}, is_goal_command={}, image_count={}",
        session_id,
        req.message.len(),
        parse_goal_command(&req.message).is_some(),
        req.images.as_ref().map(|i| i.len()).unwrap_or(0),
    );
    // Only resolve the server-side default (a config read + the shared
    // resolution cascade) when the request omitted `model` — the common case
    // (an explicit model) pays none of that cost. #480: fall back to the SAME
    // resolved default the connect bridge and `GET /execute/defaults` use, so
    // there is one implementation of "what model does this server default to".
    let default_model = if request::optional_non_empty(req.model.as_deref()).is_some() {
        None
    } else {
        let config_snapshot = state.config.read().await.clone();
        bamboo_engine::resolved_defaults::resolve_default_run_config(
            &config_snapshot,
            &state.provider_registry,
        )
        .model_roster
        .model
    };
    let model = match request::resolve_model(req.model.as_deref(), default_model.as_deref()) {
        Ok(model) => model,
        Err(response) => return response,
    };

    let global_default_prompt =
        bamboo_engine::prompt_defaults::read_global_default_system_prompt_template();
    let builtin_fallback_prompt = crate::app_state::DEFAULT_BASE_PROMPT;

    let workspace_path = request::optional_non_empty(req.workspace_path.as_deref());
    let data_dir = Some(state.app_data_dir.clone());

    // Sync runtime workspace early so the directory exists before prompt building.
    sync_runtime_workspace(
        &session_id,
        workspace_path.map(str::trim).filter(|s| !s.is_empty()),
    );

    let input = bamboo_engine::session_app::types::ChatTurnInput {
        session_id: session_id.clone(),
        model: model.clone(),
        model_ref: req.model_ref.clone(),
        provider: req.provider.clone(),
        message: req.message.clone(),
        system_prompt: request::optional_non_empty(req.system_prompt.as_deref()).map(String::from),
        enhance_prompt: request::optional_non_empty(req.enhance_prompt.as_deref())
            .map(String::from),
        workspace_path: workspace_path.map(String::from),
        selected_skill_ids: req.selected_skill_ids.clone(),
        copilot_conclusion_with_options_enhancement_enabled: req
            .copilot_conclusion_with_options_enhancement_enabled,
        data_dir,
    };

    let mut session = match bamboo_engine::session_app::chat::prepare_chat_turn(
        state.as_ref(),
        input,
        global_default_prompt.as_str(),
        builtin_fallback_prompt,
    )
    .await
    {
        Ok(session) => session,
        Err(error) => {
            tracing::error!("Chat turn preparation failed: {error}");
            return HttpResponse::InternalServerError().json(serde_json::json!({
                "error": crate::error::error_value(format!("Failed to prepare chat: {error}"))
            }));
        }
    };

    // ---- Goal command interception ----
    if let Some(goal_cmd) = parse_goal_command(&req.message) {
        tracing::debug!(
            "[{}] Chat intercepted as /goal command: {:?}",
            session_id,
            goal_cmd
        );
        return handle_goal_command(state.as_ref(), &session_id, &goal_cmd).await;
    }

    // Image handling stays in the handler layer (depends on AppState attachment reader).
    if let Err(response) =
        images::append_user_message(&state, &mut session, &req.message, req.images.as_deref()).await
    {
        return response;
    }

    // Re-save to persist image attachments (if any).
    state.save_and_cache_session(&mut session).await;

    // Publish the user message onto the account change feed so other clients
    // see it without reloading history. The feed seq becomes the message's
    // delta coordinate for `GET /history?since`.
    if let Some(msg) = session.messages.last() {
        state.account_sink.record(
            Some(&session_id),
            &bamboo_agent_core::AgentEvent::MessageAppended {
                session_id: session_id.clone(),
                message_id: msg.id.clone(),
                role: msg.role.clone(),
                content: msg.content.clone(),
                created_at: msg.created_at,
            },
        );
    }

    tracing::debug!(
        "[{}] Chat turn persisted: messages={}, last_role={:?} -> client should now POST /execute",
        session_id,
        session.messages.len(),
        session.messages.last().map(|m| format!("{:?}", m.role)),
    );

    HttpResponse::Created().json(ChatResponse {
        session_id: session_id.clone(),
        stream_url: format!("/api/v1/events/{}", session_id),
        status: "streaming".to_string(),
        goal_command: None,
    })
}

/// Additional response payload for `/goal` control commands.
#[derive(Debug, serde::Serialize)]
pub struct GoalCommandResponse {
    /// The action taken: "status", "off", "clear", "on", "set_prompt", "on_no_prompt".
    pub action: String,
    /// Whether the frontend should proceed with execute after this response.
    pub should_execute: bool,
    /// The updated (or current) gold config for this session.
    pub gold_config: Option<GoldConfig>,
}

/// Handle a parsed `/goal` command by updating session metadata and optionally
/// injecting a hidden resume message to trigger the Gold mini-loop.
async fn handle_goal_command(
    state: &AppState,
    session_id: &str,
    cmd: &GoalCommand,
) -> HttpResponse {
    let config_snapshot = state.config.read().await.clone();

    // Load current session to read the existing gold_config override.
    let session = match state.load_session_merged(session_id).await {
        Some(s) => s,
        None => {
            return HttpResponse::NotFound().json(serde_json::json!({
                "error": crate::error::error_value("Session not found"),
                "session_id": session_id
            }));
        }
    };

    // Resolve the current effective gold config (session override → global default).
    let current_json = session.metadata.get(GOLD_CONFIG_METADATA_KEY).cloned();
    let current_effective = resolve_gold_config(&config_snapshot, current_json.as_deref());

    let (new_config, should_resume) = match cmd {
        GoalCommand::Status => {
            let response_config = current_effective.clone();
            return HttpResponse::Ok().json(ChatResponse {
                session_id: session_id.to_string(),
                stream_url: format!("/api/v1/events/{}", session_id),
                status: "accepted".to_string(),
                goal_command: Some(GoalCommandResponse {
                    action: "status".to_string(),
                    should_execute: false,
                    gold_config: response_config,
                }),
            });
        }
        GoalCommand::Off => {
            let mut cfg = current_effective.unwrap_or_default();
            cfg.enabled = false;
            cfg.auto_answer_enabled = false;
            cfg.auto_continue_enabled = false;
            (cfg, false)
        }
        GoalCommand::Clear => {
            let mut cfg = current_effective.unwrap_or_default();
            cfg.enabled = false;
            cfg.auto_answer_enabled = false;
            cfg.auto_continue_enabled = false;
            cfg.goal = None;
            cfg.evaluation_prompt = None;
            (cfg, false)
        }
        GoalCommand::On => {
            let mut cfg = current_effective.unwrap_or_default();
            let has_prompt = cfg.effective_goal().is_some();
            if !has_prompt {
                return HttpResponse::Ok().json(ChatResponse {
                    session_id: session_id.to_string(),
                    stream_url: format!("/api/v1/events/{}", session_id),
                    status: "accepted".to_string(),
                    goal_command: Some(GoalCommandResponse {
                        action: "on_no_prompt".to_string(),
                        should_execute: false,
                        gold_config: Some(cfg),
                    }),
                });
            }
            cfg.enabled = true;
            cfg.auto_answer_enabled = true;
            cfg.auto_continue_enabled = true;
            (cfg, false)
        }
        GoalCommand::SetPrompt(prompt) => {
            let mut cfg = current_effective.unwrap_or_default();
            cfg.enabled = true;
            cfg.auto_answer_enabled = true;
            cfg.auto_continue_enabled = true;
            cfg.goal = Some(prompt.clone());
            (cfg, true)
        }
    };

    // Serialize the new config and persist via authoritative metadata writer.
    let new_json = serde_json::to_string(&new_config).ok();
    match SessionMetadataService::set_gold_config_json(state, session_id, new_json.clone(), None)
        .await
    {
        Ok(_) => {}
        Err(e) => {
            tracing::error!(session_id = %session_id, "Failed to persist gold_config: {e}");
            return HttpResponse::InternalServerError().json(serde_json::json!({
                "error": crate::error::error_value(format!("Failed to update goal config: {e}"))
            }));
        }
    }

    // Reset stale goal runtime state on ANY goal-config change — off / clear / on
    // / set-prompt — not just set-prompt. Otherwise a finished goal's status and
    // its double-check eval history linger in `goal.state` and keep being
    // surfaced to the frontend (and over the history API) after the goal is
    // turned off or cleared. The suspension reset + clarification turn below are
    // specific to `/goal <prompt>` and stay gated on `should_resume`.
    if let Some(mut session) = state.load_session_merged(session_id).await {
        // Drop ALL `gold.*` runtime snapshot keys by prefix — last_evaluation /
        // last_decision / last_confidence / last_reasoning / last_checkpoint /
        // last_iteration / evaluation_count (written by
        // `apply_gold_evaluation_result`) plus the legacy auto_continue_count — so
        // this list can't drift out of sync with what the evaluator writes. The
        // config lives under `gold_config` (no dot), so it is left untouched.
        session.metadata.retain(|key, _| !key.starts_with("gold."));
        // Durable goal state (status, continuation budget, declared status, and
        // the double-check eval history). Keyed by
        // `goal_state::GOAL_STATE_METADATA_KEY`.
        session.metadata.remove("goal.state");

        if should_resume {
            // Reset runtime suspension so execute can proceed.
            if let Some(runtime_state) = session.agent_runtime_state.as_mut() {
                runtime_state.status = bamboo_domain::AgentStatusState::Idle;
                runtime_state.suspension = None;
                runtime_state.waiting_for_children = None;
            }
            session.metadata.remove("runtime.suspend_reason");

            // Read the goal text back so we can echo it into the discussion turn.
            let goal_text = parse_session_gold_config(new_json.as_deref())
                .as_ref()
                .and_then(|cfg| cfg.effective_goal().map(str::to_string))
                .unwrap_or_default();

            // Inject a runtime instruction that asks the agent to discuss the
            // goal with the user before executing. The instruction itself is
            // hidden from the UI; the agent's visible reply IS the discussion.
            let instruction = format!(
                "The user has set a session goal:\n\n{goal_text}\n\nBefore taking any action, briefly confirm your understanding of this goal, surface any ambiguities or assumptions, and outline how you plan to achieve it. If anything is genuinely unclear or you need a decision from the user, ask them now. Otherwise, state your plan and begin working toward the goal."
            );
            let mut resume_msg = bamboo_domain::Message::user(instruction);
            resume_msg.metadata = Some(serde_json::json!({
                "hidden_from_ui": true,
                "runtime_kind": "gold_goal_resume"
            }));
            session.add_message(resume_msg);
        }

        state.save_and_cache_session(&mut session).await;
    }

    // Parse back the persisted config for the response.
    let response_config = parse_session_gold_config(new_json.as_deref());

    HttpResponse::Ok().json(ChatResponse {
        session_id: session_id.to_string(),
        stream_url: format!("/api/v1/events/{}", session_id),
        status: "accepted".to_string(),
        goal_command: Some(GoalCommandResponse {
            action: match cmd {
                GoalCommand::Off => "off".to_string(),
                GoalCommand::Clear => "clear".to_string(),
                GoalCommand::On => "on".to_string(),
                GoalCommand::SetPrompt(_) => "set_prompt".to_string(),
                GoalCommand::Status => unreachable!(),
            },
            should_execute: should_resume,
            gold_config: response_config,
        }),
    })
}

// Note: image attachments are stored on disk in SessionStoreV2, and message parts
// use `bamboo-attachment://<session_id>/<attachment_id>` references.
