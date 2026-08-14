//! Typed permission-decision endpoint.

use std::sync::Arc;

use actix_web::{web, HttpResponse};
use bamboo_config::ConfigStoreError;
use bamboo_tools::permission::{
    DurablePermissionRule, PermissionDecision, PermissionDecisionKind, PermissionMatcher,
    PermissionRuleEffect, PermissionRuleScope, PermissionRuleSource,
};

use crate::{app_state::AppState, error::AppError};

use super::super::types::RespondRequest;

#[derive(Debug, serde::Serialize)]
struct PermissionDecisionResponse {
    success: bool,
    replayed: bool,
    receipt: bamboo_tools::permission::PermissionDecisionReceipt,
    #[serde(skip_serializing_if = "Option::is_none")]
    resume: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    auto_resume_status: Option<String>,
}

fn selected_matcher(
    request: &bamboo_tools::permission::PermissionRequest,
    decision: &PermissionDecision,
) -> Result<PermissionMatcher, AppError> {
    match decision.matcher_id.as_deref() {
        Some(id) => request
            .suggested_matchers
            .iter()
            .find(|matcher| matcher.id == id)
            .cloned()
            .ok_or_else(|| AppError::BadRequest(format!("unknown matcher id '{id}'"))),
        None => request
            .suggested_matchers
            .iter()
            .find(|matcher| matcher.id == "exact_resource")
            .cloned()
            .ok_or_else(|| AppError::BadRequest("request has no exact matcher".to_string())),
    }
}

fn map_store_error(error: ConfigStoreError) -> AppError {
    match error {
        ConfigStoreError::Conflict { expected, actual } => {
            AppError::ConfigConflict { expected, actual }
        }
        other => AppError::InternalError(anyhow::anyhow!(
            "failed to persist permission decision: {other}"
        )),
    }
}

fn durable_rule_for_decision(
    request: &bamboo_tools::permission::PermissionRequest,
    decision: &PermissionDecision,
) -> Result<Option<DurablePermissionRule>, AppError> {
    let scope = match decision.decision {
        PermissionDecisionKind::AllowWorkspace => PermissionRuleScope::Workspace,
        PermissionDecisionKind::AllowGlobal => PermissionRuleScope::Global,
        _ => return Ok(None),
    };
    if scope == PermissionRuleScope::Global && !decision.confirm_global {
        return Err(AppError::BadRequest(
            "global allow requires explicit second confirmation".to_string(),
        ));
    }
    let matcher = selected_matcher(request, decision)?;
    matcher
        .validate(request.permission_type)
        .map_err(AppError::BadRequest)?;
    let workspace_path = if scope == PermissionRuleScope::Workspace {
        Some(request.workspace_path.clone().ok_or_else(|| {
            AppError::BadRequest(
                "workspace decision requires a stable workspace identity".to_string(),
            )
        })?)
    } else {
        None
    };
    let rule = DurablePermissionRule {
        id: format!(
            "remembered:{}:{}:{}:{}",
            match scope {
                PermissionRuleScope::Workspace => "workspace",
                PermissionRuleScope::Global => "global",
            },
            request.session_id,
            request.request_id,
            request.request_generation
        ),
        permission_type: request.permission_type,
        effect: PermissionRuleEffect::Allow,
        scope,
        workspace_path,
        matcher,
        source: PermissionRuleSource::User,
        expires_at: None,
    };
    rule.validate().map_err(AppError::BadRequest)?;
    Ok(Some(rule))
}

/// Recover the semantic receipt encoded by a deterministic remembered rule.
/// This closes the crash boundary where the policy commit reached disk but the
/// process-local receipt did not. A mismatching reserved rule fails closed.
fn recovered_durable_decision(
    request: &bamboo_tools::permission::PermissionRequest,
    rules: &[DurablePermissionRule],
) -> Result<Option<PermissionDecision>, AppError> {
    let workspace_id = format!(
        "remembered:workspace:{}:{}:{}",
        request.session_id, request.request_id, request.request_generation
    );
    let global_id = format!(
        "remembered:global:{}:{}:{}",
        request.session_id, request.request_id, request.request_generation
    );
    let mut matching = rules
        .iter()
        .filter(|rule| rule.id == workspace_id || rule.id == global_id);
    let Some(rule) = matching.next() else {
        return Ok(None);
    };
    if matching.next().is_some() {
        return Err(AppError::BadRequest(
            "permission request has conflicting durable receipt rules".to_string(),
        ));
    }
    let decision_kind = match rule.scope {
        PermissionRuleScope::Workspace if rule.id == workspace_id => {
            PermissionDecisionKind::AllowWorkspace
        }
        PermissionRuleScope::Global if rule.id == global_id => PermissionDecisionKind::AllowGlobal,
        _ => {
            return Err(AppError::BadRequest(
                "permission request has a malformed durable receipt rule".to_string(),
            ));
        }
    };
    let recovered = PermissionDecision {
        request_id: request.request_id.clone(),
        request_generation: request.request_generation.clone(),
        decision: decision_kind,
        matcher_id: Some(rule.matcher.id.clone()),
        expected_policy_revision: Some(request.policy_revision),
        confirm_global: decision_kind == PermissionDecisionKind::AllowGlobal,
    };
    let expected_rule =
        durable_rule_for_decision(request, &recovered)?.expect("recovered decision is durable");
    if expected_rule != *rule {
        return Err(AppError::BadRequest(
            "permission request has a conflicting durable receipt rule".to_string(),
        ));
    }
    Ok(Some(recovered))
}

fn same_durable_decision_semantics(
    recovered: &PermissionDecision,
    submitted: &PermissionDecision,
) -> bool {
    recovered.request_id == submitted.request_id
        && recovered.request_generation == submitted.request_generation
        && recovered.decision == submitted.decision
        && canonical_matcher_identity(recovered) == canonical_matcher_identity(submitted)
        && recovered.confirm_global == submitted.confirm_global
    // `expected_policy_revision` is the pre-commit CAS token. It is not part
    // of the durable effect and cannot always be reconstructed after a stale
    // contract was safely re-evaluated before the original commit.
}

fn canonical_matcher_identity(decision: &PermissionDecision) -> Option<&str> {
    decision.matcher_id.as_deref().or_else(|| {
        matches!(
            decision.decision,
            PermissionDecisionKind::AllowSession
                | PermissionDecisionKind::DenySession
                | PermissionDecisionKind::AllowWorkspace
                | PermissionDecisionKind::AllowGlobal
        )
        .then_some("exact_resource")
    })
}

fn canonicalize_remembered_matcher(
    request: &bamboo_tools::permission::PermissionRequest,
    decision: &mut PermissionDecision,
) -> Result<(), AppError> {
    if decision.matcher_id.is_none()
        && matches!(
            decision.decision,
            PermissionDecisionKind::AllowSession
                | PermissionDecisionKind::DenySession
                | PermissionDecisionKind::AllowWorkspace
                | PermissionDecisionKind::AllowGlobal
        )
    {
        decision.matcher_id = Some(selected_matcher(request, decision)?.id);
    }
    Ok(())
}

fn resolved_decision_conflict(
    current: &PermissionDecision,
    submitted: &PermissionDecision,
) -> HttpResponse {
    HttpResponse::Conflict().json(serde_json::json!({
        "error": crate::error::error_value(
            "Permission request was already resolved with a different decision"
        ),
        "conflict": "permission_decision_already_resolved",
        "request_id": submitted.request_id,
        "request_generation": submitted.request_generation,
        "current_decision": current,
        "submitted_decision": submitted,
    }))
}

pub async fn submit_permission_decision(
    state: web::Data<AppState>,
    session_id: web::Path<String>,
    payload: web::Json<PermissionDecision>,
) -> Result<HttpResponse, AppError> {
    let session_id = session_id.into_inner();
    let mut decision = payload.into_inner();
    if decision.request_generation.trim().is_empty() {
        return Err(AppError::BadRequest(
            "permission decision generation must not be blank".to_string(),
        ));
    }
    let expected_tool_call_id = decision.request_id.clone();
    let Some(config) = state.permission_checker.permission_config() else {
        return Err(AppError::InternalError(anyhow::anyhow!(
            "permission checker does not expose typed decisions"
        )));
    };

    // Hold the same per-session response transaction guard used by every
    // response source across policy mutation and pending-question consume.
    // This prevents a remembered rule from being committed for a question
    // that another responder replaced between preflight and the final CAS.
    let response_guard =
        bamboo_engine::session_app::respond::acquire_pending_response_guard(&session_id).await;
    let mut permission_guard = Some(Arc::clone(&state.permission_io_lock).lock_owned().await);
    let existing = config.decision_receipt(
        &session_id,
        &decision.request_id,
        &decision.request_generation,
    );
    let replayed = if let Some(receipt) = existing.as_ref() {
        if !same_durable_decision_semantics(&receipt.decision, &decision) {
            return Ok(resolved_decision_conflict(&receipt.decision, &decision));
        }
        // A provider may legally reuse a tool-call id in a later round. Even
        // an exact replay receipt for the old generation must not consume that
        // newer parked operation.
        let session = state
            .load_session(&session_id)
            .await
            .ok_or_else(|| AppError::NotFound("session".to_string()))?;
        if let Some(pending) = session.pending_question.as_ref() {
            if pending.tool_call_id != decision.request_id {
                return Ok(HttpResponse::Conflict().json(serde_json::json!({
                    "error": crate::error::error_value("Pending question changed"),
                    "expected_tool_call_id": decision.request_id,
                    "actual_tool_call_id": pending.tool_call_id,
                })));
            }
            let in_memory = config.pending_request(&session_id, &decision.request_id);
            let interaction =
                super::pending::resolve_pending_interaction(&session, pending, in_memory);
            let current_generation = interaction
                .permission_request
                .as_ref()
                .map(|request| request.request_generation.as_str());
            if current_generation != Some(decision.request_generation.as_str()) {
                return Ok(HttpResponse::Conflict().json(serde_json::json!({
                    "error": crate::error::error_value("Pending permission generation changed"),
                    "expected_request_generation": decision.request_generation,
                    "actual_request_generation": current_generation,
                })));
            }
        }
        true
    } else {
        // Validate the durable pending identity before any grant or policy
        // mutation. The later response CAS repeats this check, closing normal
        // races without ever trusting display text.
        let session = state
            .load_session(&session_id)
            .await
            .ok_or_else(|| AppError::NotFound("session".to_string()))?;
        let Some(pending) = session.pending_question.as_ref() else {
            if let Some(receipt) = super::pending::persisted_permission_decision_receipt(
                &session,
                &decision.request_id,
            ) {
                if !same_durable_decision_semantics(&receipt.decision, &decision) {
                    return Ok(resolved_decision_conflict(&receipt.decision, &decision));
                }
                config
                    .record_decision_receipt(receipt.clone())
                    .map_err(AppError::BadRequest)?;
                return Ok(HttpResponse::Ok().json(PermissionDecisionResponse {
                    success: true,
                    replayed: true,
                    receipt,
                    resume: None,
                    auto_resume_status: None,
                }));
            }
            // After a successful durable decision, a daemon restart loses the
            // process-local receipt. A crash before the response CAS also
            // lacks the atomic transcript receipt, so recover only from the
            // exact session-qualified deterministic remembered rule.
            let request =
                super::pending::persisted_permission_request(&session, &decision.request_id)
                    .ok_or_else(|| AppError::NotFound("pending permission request".to_string()))?;
            canonicalize_remembered_matcher(&request, &mut decision)?;
            let snapshot = state.permission_section.snapshot();
            let recovered = recovered_durable_decision(&request, &snapshot.data.durable_rules)?
                .ok_or_else(|| AppError::NotFound("permission decision receipt".to_string()))?;
            if !same_durable_decision_semantics(&recovered, &decision) {
                return Ok(resolved_decision_conflict(&recovered, &decision));
            }
            config.publish_persistent_policy(snapshot.revision, snapshot.data.as_ref());
            config
                .record_decision(&session_id, decision.clone())
                .map_err(AppError::BadRequest)?;
            let receipt = config
                .decision_receipt(
                    &session_id,
                    &decision.request_id,
                    &decision.request_generation,
                )
                .expect("recovered durable receipt was recorded");
            return Ok(HttpResponse::Ok().json(PermissionDecisionResponse {
                success: true,
                replayed: true,
                receipt,
                resume: None,
                auto_resume_status: None,
            }));
        };
        if pending.tool_call_id != decision.request_id {
            return Ok(HttpResponse::Conflict().json(serde_json::json!({
                "error": crate::error::error_value("Pending question changed"),
                "expected_tool_call_id": decision.request_id,
                "actual_tool_call_id": pending.tool_call_id,
            })));
        }
        let in_memory = config.pending_request(&session_id, &decision.request_id);
        let interaction = super::pending::resolve_pending_interaction(&session, pending, in_memory);
        let request = interaction
            .permission_request
            .filter(|request| request.request_id == decision.request_id)
            .ok_or_else(|| AppError::NotFound("pending permission request".to_string()))?;
        if request.request_generation != decision.request_generation {
            return Ok(HttpResponse::Conflict().json(serde_json::json!({
                "error": crate::error::error_value("Pending permission generation changed"),
                "expected_request_generation": decision.request_generation,
                "actual_request_generation": request.request_generation,
            })));
        }
        canonicalize_remembered_matcher(&request, &mut decision)?;
        config.register_pending_request(request.clone());

        let policy_snapshot = state.permission_section.snapshot();
        if let Some(recovered) =
            recovered_durable_decision(&request, &policy_snapshot.data.durable_rules)?
        {
            if !request.allowed_decisions.contains(&recovered.decision) {
                return Err(AppError::BadRequest(
                    "durable receipt records a decision that was not allowed".to_string(),
                ));
            }
            if !same_durable_decision_semantics(&recovered, &decision) {
                return Ok(resolved_decision_conflict(&recovered, &decision));
            }
            config
                .publish_persistent_policy(policy_snapshot.revision, policy_snapshot.data.as_ref());
            config
                .record_decision(&session_id, decision.clone())
                .map_err(AppError::BadRequest)?;
            true
        } else {
            let actual_revision = policy_snapshot.revision;
            if request.policy_revision != actual_revision {
                if let Some(refreshed) = super::pending::refresh_request_for_current_policy(
                    &config, &session, pending, &request,
                ) {
                    config.register_pending_request(refreshed);
                }
                return Err(AppError::ConfigConflict {
                    expected: request.policy_revision,
                    actual: actual_revision,
                });
            }
            if !request.allowed_decisions.contains(&decision.decision) {
                return Err(AppError::Forbidden(format!(
                    "decision {:?} is not allowed for this request",
                    decision.decision
                )));
            }
            if let Some(expected) = decision.expected_policy_revision {
                if expected != actual_revision {
                    return Err(AppError::ConfigConflict {
                        expected,
                        actual: actual_revision,
                    });
                }
            }

            match decision.decision {
                PermissionDecisionKind::AllowOnce => config
                    .grant_once_for_generation(
                        &session_id,
                        &request.request_id,
                        &request.request_generation,
                        request.permission_type,
                        request.resource.clone(),
                    )
                    .map_err(AppError::BadRequest)?,
                PermissionDecisionKind::AllowSession => {
                    let matcher = selected_matcher(&request, &decision)?;
                    config
                        .grant_typed_scoped_session_permission(
                            &session_id,
                            request.permission_type,
                            matcher,
                        )
                        .map_err(AppError::BadRequest)?;
                }
                PermissionDecisionKind::DenySession => {
                    let matcher = selected_matcher(&request, &decision)?;
                    config
                        .deny_typed_scoped_session_permission(
                            &session_id,
                            request.permission_type,
                            matcher,
                        )
                        .map_err(AppError::BadRequest)?;
                }
                PermissionDecisionKind::AllowWorkspace | PermissionDecisionKind::AllowGlobal => {
                    let expected_revision = decision.expected_policy_revision.ok_or_else(|| {
                        AppError::BadRequest(
                            "durable decision requires expected_policy_revision".to_string(),
                        )
                    })?;
                    let durable_rule = durable_rule_for_decision(&request, &decision)?
                        .expect("workspace/global decision builds a durable rule");
                    let mut candidate = policy_snapshot.data.as_ref().clone();
                    candidate
                        .durable_rules
                        .retain(|rule| rule.id != durable_rule.id);
                    candidate.durable_rules.push(durable_rule);
                    let section = Arc::clone(&state.permission_section);
                    let live_config = Arc::clone(&config);
                    let mutation_guard = permission_guard
                        .take()
                        .expect("permission mutation lock is held");
                    // Once the durable mutation starts, a disconnected client
                    // cannot cancel between commit and live publication. The
                    // spawned task owns the serialization guard through both
                    // steps and hands it back only after the live snapshot is
                    // current.
                    let mutation = tokio::spawn(async move {
                        let writer = Arc::clone(&section);
                        tokio::task::spawn_blocking(move || {
                            writer.commit(expected_revision, candidate)
                        })
                        .await
                        .map_err(|error| {
                            AppError::InternalError(anyhow::anyhow!(
                                "permission commit task failed: {error}"
                            ))
                        })?
                        .map_err(map_store_error)?;
                        let snapshot = section.snapshot();
                        live_config
                            .publish_persistent_policy(snapshot.revision, snapshot.data.as_ref());
                        Ok::<_, AppError>((snapshot, mutation_guard))
                    });
                    let (_snapshot, returned_guard) = mutation.await.map_err(|error| {
                        AppError::InternalError(anyhow::anyhow!(
                            "permission mutation task failed: {error}"
                        ))
                    })??;
                    permission_guard = Some(returned_guard);
                }
                PermissionDecisionKind::DenyOnce => {}
            }
            config
                .record_decision(&session_id, decision.clone())
                .map_err(AppError::BadRequest)?;
            false
        }
    };
    let receipt = config
        .decision_receipt(
            &session_id,
            &decision.request_id,
            &decision.request_generation,
        )
        .expect("decision receipt was recorded or observed");
    drop(permission_guard.take());

    // A replay after the original response completed is a successful no-op. If
    // the first attempt committed policy but failed before saving the pending
    // answer, retry the ordinary response path to finish resume safely.
    if replayed
        && state
            .load_session(&session_id)
            .await
            .is_some_and(|session| session.pending_question.is_none())
    {
        return Ok(HttpResponse::Ok().json(PermissionDecisionResponse {
            success: true,
            replayed: true,
            receipt,
            resume: None,
            auto_resume_status: None,
        }));
    }

    let legacy_response = match decision.decision {
        PermissionDecisionKind::AllowOnce
        | PermissionDecisionKind::AllowSession
        | PermissionDecisionKind::AllowWorkspace
        | PermissionDecisionKind::AllowGlobal => "Approve",
        PermissionDecisionKind::DenyOnce | PermissionDecisionKind::DenySession => "Deny",
    };
    let response = super::submit::submit_typed_permission_response(
        state,
        web::Path::from(session_id),
        web::Json(RespondRequest {
            response: legacy_response.to_string(),
            // Permission request identity is the originating tool-call id.
            // Keep that exact CAS on replay so receipt A can never answer a
            // newer pending permission B.
            expected_tool_call_id: Some(expected_tool_call_id),
            model: None,
            provider: None,
            model_ref: None,
            reasoning_effort: None,
        }),
        receipt.clone(),
        &response_guard,
    )
    .await
    .map_err(|error| AppError::InternalError(anyhow::anyhow!(error.to_string())))?;

    if !response.status().is_success() {
        return Ok(response);
    }
    let response_body = actix_web::body::to_bytes(response.into_body())
        .await
        .map_err(|error| {
            AppError::InternalError(anyhow::anyhow!(
                "failed to read successful response submission body: {error}"
            ))
        })?;
    let resume: serde_json::Value = serde_json::from_slice(&response_body).map_err(|error| {
        AppError::InternalError(anyhow::anyhow!(
            "successful response submission returned invalid JSON: {error}"
        ))
    })?;
    let auto_resume_status = resume
        .get("auto_resume_status")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            AppError::InternalError(anyhow::anyhow!(
                "successful response submission omitted auto_resume_status"
            ))
        })?;
    Ok(HttpResponse::Ok().json(PermissionDecisionResponse {
        success: true,
        replayed,
        receipt,
        resume: Some(resume),
        auto_resume_status: Some(auto_resume_status),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::{Message, PendingQuestionSource, Session};
    use bamboo_engine::execution::{AgentRunner, AgentStatus};
    use bamboo_tools::permission::{
        PermissionMatcherKind, PermissionMode, PermissionReasonCode, PermissionRequest,
        PermissionType, RiskLevel,
    };

    fn test_permission_request(
        session_id: &str,
        request_id: &str,
        policy_revision: u64,
        allowed_decisions: Vec<PermissionDecisionKind>,
    ) -> PermissionRequest {
        PermissionRequest {
            request_id: request_id.to_string(),
            request_generation: format!("generation-{request_id}"),
            session_id: session_id.to_string(),
            workspace_path: Some("/workspace".to_string()),
            tool_name: "Bash".to_string(),
            permission_type: PermissionType::ExecuteCommand,
            resource: "cargo test".to_string(),
            operation_summary: "Run cargo test".to_string(),
            risk_level: RiskLevel::High,
            reason_code: PermissionReasonCode::RiskThreshold,
            effective_mode: PermissionMode::Default,
            bypass_requested: false,
            auto_approve_requested: false,
            policy_revision,
            matched_rule: None,
            allowed_decisions,
            suggested_matchers: vec![PermissionMatcher {
                id: "exact_resource".to_string(),
                kind: PermissionMatcherKind::ExactResource,
                value: "cargo test".to_string(),
            }],
        }
    }

    fn parked_permission_session(request: &PermissionRequest) -> Session {
        let mut session = Session::new(&request.session_id, "test-model");
        session.messages.push(Message::tool_result(
            &request.request_id,
            serde_json::json!({
                "status": "awaiting_permission_approval",
                "permission_type": "execute_command",
                "resource": request.resource,
                "permission_request": request,
            })
            .to_string(),
        ));
        session.set_pending_question_with_source(
            request.request_id.clone(),
            request.tool_name.clone(),
            "Allow cargo test?".to_string(),
            vec!["Approve".to_string(), "Deny".to_string()],
            false,
            PendingQuestionSource::PauseTool,
        );
        session
    }

    fn unrelated_rule() -> DurablePermissionRule {
        DurablePermissionRule {
            id: "unrelated-rule".to_string(),
            permission_type: PermissionType::ExecuteCommand,
            effect: PermissionRuleEffect::Allow,
            scope: PermissionRuleScope::Global,
            workspace_path: None,
            matcher: PermissionMatcher {
                id: "unrelated-exact".to_string(),
                kind: PermissionMatcherKind::ExactResource,
                value: "npm test".to_string(),
            },
            source: PermissionRuleSource::User,
            expires_at: None,
        }
    }

    fn matching_deny_rule() -> DurablePermissionRule {
        DurablePermissionRule {
            id: "deny-cargo-test".to_string(),
            permission_type: PermissionType::ExecuteCommand,
            effect: PermissionRuleEffect::Deny,
            scope: PermissionRuleScope::Global,
            workspace_path: None,
            matcher: PermissionMatcher {
                id: "deny-cargo-test-exact".to_string(),
                kind: PermissionMatcherKind::ExactResource,
                value: "cargo test".to_string(),
            },
            source: PermissionRuleSource::User,
            expires_at: None,
        }
    }

    #[actix_web::test]
    async fn replayed_decision_returns_immediately_while_successor_is_running() {
        let dir = tempfile::tempdir().expect("temp dir");
        let state = web::Data::new(
            AppState::new(dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let session_id = "permission-replay-running-successor";
        state
            .storage
            .save_session(&Session::new(session_id, "test-model"))
            .await
            .expect("save completed session");

        let decision = PermissionDecision {
            request_id: "request-1".to_string(),
            request_generation: "generation-request-1".to_string(),
            decision: PermissionDecisionKind::AllowOnce,
            matcher_id: None,
            expected_policy_revision: None,
            confirm_global: false,
        };
        state
            .permission_checker
            .permission_config()
            .expect("typed permission config")
            .record_decision(session_id, decision.clone())
            .expect("record original decision");

        let mut successor = AgentRunner::new();
        successor.status = AgentStatus::Running;
        state
            .agent_runners
            .write()
            .await
            .insert(session_id.to_string(), successor);

        let response = tokio::time::timeout(
            std::time::Duration::from_millis(250),
            submit_permission_decision(
                state.clone(),
                web::Path::from(session_id.to_string()),
                web::Json(decision.clone()),
            ),
        )
        .await
        .expect("idempotent replay must not wait for the running successor")
        .expect("response");

        assert_eq!(response.status(), actix_web::http::StatusCode::OK);
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .expect("response body");
        let body: serde_json::Value = serde_json::from_slice(&body).expect("response JSON");
        assert_eq!(body["success"], true);
        assert_eq!(body["replayed"], true);
        assert!(body.get("resume").is_none());
        assert!(body.get("auto_resume_status").is_none());

        let baseline_runners = state.agent_runners.read().await.len();
        let baseline_senders = state.session_event_senders.read().await.len();
        let mut opposite = decision;
        opposite.decision = PermissionDecisionKind::DenyOnce;
        let response = submit_permission_decision(
            state.clone(),
            web::Path::from(session_id.to_string()),
            web::Json(opposite),
        )
        .await
        .expect("conflicting duplicate is a typed conflict response");
        assert_eq!(response.status(), actix_web::http::StatusCode::CONFLICT);
        assert_eq!(state.agent_runners.read().await.len(), baseline_runners);
        assert_eq!(
            state.session_event_senders.read().await.len(),
            baseline_senders
        );
    }

    #[actix_web::test]
    async fn replayed_old_decision_cannot_consume_a_newer_permission_question() {
        let dir = tempfile::tempdir().expect("temp dir");
        let state = web::Data::new(
            AppState::new(dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let session_id = "permission-replay-newer-question";
        let old_decision = PermissionDecision {
            request_id: "request-a".to_string(),
            request_generation: "generation-request-a".to_string(),
            decision: PermissionDecisionKind::AllowOnce,
            matcher_id: None,
            expected_policy_revision: None,
            confirm_global: false,
        };
        state
            .permission_checker
            .permission_config()
            .expect("typed permission config")
            .record_decision(session_id, old_decision.clone())
            .expect("record old receipt");

        let mut session = Session::new(session_id, "test-model");
        session.set_pending_question_with_source(
            "request-b".to_string(),
            "Bash".to_string(),
            "Allow the newer command?".to_string(),
            vec!["Approve".to_string(), "Deny".to_string()],
            false,
            PendingQuestionSource::PauseTool,
        );
        state.save_and_cache_session(&mut session).await;
        let baseline_runners = state.agent_runners.read().await.len();
        let baseline_senders = state.session_event_senders.read().await.len();

        let response = submit_permission_decision(
            state.clone(),
            web::Path::from(session_id.to_string()),
            web::Json(old_decision),
        )
        .await
        .expect("replay response");
        assert_eq!(response.status(), actix_web::http::StatusCode::CONFLICT);
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .expect("conflict body");
        let body: serde_json::Value = serde_json::from_slice(&body).expect("conflict JSON");
        assert_eq!(body["error"]["message"], "Pending question changed");
        assert_eq!(body["expected_tool_call_id"], "request-a");
        assert_eq!(body["actual_tool_call_id"], "request-b");

        let durable = state
            .storage
            .load_session(session_id)
            .await
            .expect("durable load")
            .expect("durable session");
        let pending = durable.pending_question.expect("newer question remains");
        assert_eq!(pending.tool_call_id, "request-b");
        assert_eq!(pending.question, "Allow the newer command?");
        assert_eq!(state.agent_runners.read().await.len(), baseline_runners);
        assert_eq!(
            state.session_event_senders.read().await.len(),
            baseline_senders
        );
    }

    #[actix_web::test]
    async fn replayed_old_generation_cannot_consume_reused_request_id() {
        let dir = tempfile::tempdir().expect("temp dir");
        let state = web::Data::new(
            AppState::new(dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let session_id = "permission-reused-request-id";
        let request_id = "reused-request";
        let old_decision = PermissionDecision {
            request_id: request_id.to_string(),
            request_generation: "generation-old".to_string(),
            decision: PermissionDecisionKind::AllowOnce,
            matcher_id: None,
            expected_policy_revision: None,
            confirm_global: false,
        };
        let config = state
            .permission_checker
            .permission_config()
            .expect("typed permission config");
        config
            .record_decision(session_id, old_decision.clone())
            .expect("record old receipt");

        let mut current_request = test_permission_request(
            session_id,
            request_id,
            0,
            PermissionRequest::forced_decisions(),
        );
        current_request.request_generation = "generation-new".to_string();
        current_request.resource = "cargo test --workspace".to_string();
        current_request.operation_summary = "Run the current workspace tests".to_string();
        let mut session = parked_permission_session(&current_request);
        state.save_and_cache_session(&mut session).await;
        config.register_pending_request(current_request.clone());

        let response = submit_permission_decision(
            state.clone(),
            web::Path::from(session_id.to_string()),
            web::Json(old_decision),
        )
        .await
        .expect("stale replay response");
        assert_eq!(response.status(), actix_web::http::StatusCode::CONFLICT);
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .expect("conflict body");
        let body: serde_json::Value = serde_json::from_slice(&body).expect("conflict JSON");
        assert_eq!(
            body["error"]["message"],
            "Pending permission generation changed"
        );
        assert_eq!(body["expected_request_generation"], "generation-old");
        assert_eq!(body["actual_request_generation"], "generation-new");

        let durable = state
            .storage
            .load_session(session_id)
            .await
            .expect("durable load")
            .expect("durable session");
        assert_eq!(
            durable
                .pending_question
                .expect("current question remains")
                .tool_call_id,
            request_id
        );
        assert_eq!(
            config
                .pending_request(session_id, request_id)
                .map(|request| request.request_generation),
            Some("generation-new".to_string())
        );
        assert!(config
            .decision_receipt(session_id, request_id, "generation-new")
            .is_none());
        assert!(config.temporary_grants().is_empty());
    }

    #[actix_web::test]
    async fn first_decision_returns_submit_response_auto_resume_status() {
        let dir = tempfile::tempdir().expect("temp dir");
        let state = web::Data::new(
            AppState::new(dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let session_id = "permission-decision-resume-status";
        let request_id = "permission-request-1";
        let request = test_permission_request(
            session_id,
            request_id,
            0,
            PermissionRequest::forced_decisions(),
        );
        let mut session = parked_permission_session(&request);
        state.save_and_cache_session(&mut session).await;

        state
            .permission_checker
            .permission_config()
            .expect("typed permission config")
            .register_pending_request(request);

        let response = submit_permission_decision(
            state,
            web::Path::from(session_id.to_string()),
            web::Json(PermissionDecision {
                request_id: request_id.to_string(),
                request_generation: format!("generation-{request_id}"),
                decision: PermissionDecisionKind::DenyOnce,
                matcher_id: None,
                expected_policy_revision: None,
                confirm_global: false,
            }),
        )
        .await
        .expect("typed decision response");
        assert_eq!(response.status(), actix_web::http::StatusCode::OK);
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .expect("response body");
        let body: serde_json::Value = serde_json::from_slice(&body).expect("response JSON");

        assert_eq!(body["success"], true);
        assert_eq!(body["replayed"], false);
        assert_eq!(body["auto_resume_status"], "started");
        assert_eq!(body["resume"]["auto_resume_status"], "started");
        assert!(body["resume"].get("accepted").is_none());
    }

    #[actix_web::test]
    async fn direct_typed_decision_recovers_durable_request_after_map_loss() {
        let dir = tempfile::tempdir().expect("temp dir");
        let state = web::Data::new(
            AppState::new(dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let session_id = "permission-direct-after-map-loss";
        let request_id = "permission-request-durable";
        let request = PermissionRequest {
            request_id: request_id.to_string(),
            request_generation: format!("generation-{request_id}"),
            session_id: session_id.to_string(),
            workspace_path: Some("/workspace".to_string()),
            tool_name: "Bash".to_string(),
            permission_type: PermissionType::ExecuteCommand,
            resource: "cargo test".to_string(),
            operation_summary: "Run cargo test".to_string(),
            risk_level: RiskLevel::High,
            reason_code: PermissionReasonCode::RiskThreshold,
            effective_mode: PermissionMode::Default,
            bypass_requested: false,
            auto_approve_requested: false,
            policy_revision: 0,
            matched_rule: None,
            allowed_decisions: vec![
                PermissionDecisionKind::AllowOnce,
                PermissionDecisionKind::DenyOnce,
            ],
            suggested_matchers: vec![PermissionMatcher {
                id: "exact_resource".to_string(),
                kind: PermissionMatcherKind::ExactResource,
                value: "cargo test".to_string(),
            }],
        };
        let mut session = Session::new(session_id, "test-model");
        session.messages.push(Message::tool_result(
            request_id,
            serde_json::json!({
                "status": "awaiting_permission_approval",
                "permission_request": request,
            })
            .to_string(),
        ));
        session.set_pending_question_with_source(
            request_id.to_string(),
            "Bash".to_string(),
            "Allow cargo test?".to_string(),
            vec!["Approve".to_string(), "Deny".to_string()],
            false,
            PendingQuestionSource::PauseTool,
        );
        state.save_and_cache_session(&mut session).await;
        let config = state
            .permission_checker
            .permission_config()
            .expect("typed permission config");
        assert!(config.pending_request(session_id, request_id).is_none());

        let decision = PermissionDecision {
            request_id: request_id.to_string(),
            request_generation: format!("generation-{request_id}"),
            decision: PermissionDecisionKind::DenyOnce,
            matcher_id: None,
            expected_policy_revision: None,
            confirm_global: false,
        };
        let response = submit_permission_decision(
            state.clone(),
            web::Path::from(session_id.to_string()),
            web::Json(decision.clone()),
        )
        .await
        .expect("typed decision response");
        assert_eq!(response.status(), actix_web::http::StatusCode::OK);
        assert!(config
            .decision_receipt(session_id, request_id, &format!("generation-{request_id}"))
            .is_some());
        drop(config);
        drop(state);

        let restarted = web::Data::new(
            AppState::new(dir.path().to_path_buf())
                .await
                .expect("restarted app state"),
        );
        let response = submit_permission_decision(
            restarted,
            web::Path::from(session_id.to_string()),
            web::Json(decision),
        )
        .await
        .expect("non-durable decision receipt survives restart");
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .expect("response body");
        let body: serde_json::Value = serde_json::from_slice(&body).expect("response JSON");
        assert_eq!(body["replayed"], true);
        assert!(body.get("resume").is_none());
    }

    #[actix_web::test]
    async fn stale_policy_contract_refreshes_then_retry_uses_current_revision() {
        let dir = tempfile::tempdir().expect("temp dir");
        let state = web::Data::new(
            AppState::new(dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let session_id = "permission-stale-policy-refresh";
        let request_id = "permission-stale-request";
        let request = test_permission_request(
            session_id,
            request_id,
            0,
            PermissionRequest::ordinary_decisions(true),
        );
        let mut session = parked_permission_session(&request);
        state.save_and_cache_session(&mut session).await;
        let config = state
            .permission_checker
            .permission_config()
            .expect("typed permission config");
        config.register_pending_request(request);

        let mut candidate = state.permission_section.snapshot().data.as_ref().clone();
        candidate.durable_rules.push(unrelated_rule());
        state
            .permission_section
            .commit(0, candidate)
            .expect("advance policy revision");
        let current = state.permission_section.snapshot();
        config.publish_persistent_policy(current.revision, current.data.as_ref());
        assert_eq!(current.revision, 1);

        let stale_decision = PermissionDecision {
            request_id: request_id.to_string(),
            request_generation: format!("generation-{request_id}"),
            decision: PermissionDecisionKind::AllowWorkspace,
            // Omitting the matcher is a valid request for the conservative
            // exact_resource default; persistence canonicalizes it before the
            // receipt is written.
            matcher_id: None,
            expected_policy_revision: Some(0),
            confirm_global: false,
        };
        let error = submit_permission_decision(
            state.clone(),
            web::Path::from(session_id.to_string()),
            web::Json(stale_decision.clone()),
        )
        .await
        .expect_err("stale contract must conflict");
        assert!(matches!(
            error,
            AppError::ConfigConflict {
                expected: 0,
                actual: 1
            }
        ));
        let refreshed = config
            .pending_request(session_id, request_id)
            .expect("pending request was safely re-evaluated");
        assert_eq!(refreshed.policy_revision, 1);
        assert!(refreshed
            .allowed_decisions
            .contains(&PermissionDecisionKind::AllowWorkspace));

        let mut retry = stale_decision;
        retry.expected_policy_revision = Some(1);
        let response = submit_permission_decision(
            state.clone(),
            web::Path::from(session_id.to_string()),
            web::Json(retry),
        )
        .await
        .expect("retry at refreshed revision");
        assert_eq!(response.status(), actix_web::http::StatusCode::OK);
        assert_eq!(state.permission_section.snapshot().revision, 2);
        let durable = state
            .storage
            .load_session(session_id)
            .await
            .expect("durable load")
            .expect("durable session");
        assert!(durable.pending_question.is_none());
    }

    #[actix_web::test]
    async fn stale_contract_narrows_to_explicit_deny_choices_when_policy_now_denies() {
        let dir = tempfile::tempdir().expect("temp dir");
        let state = web::Data::new(
            AppState::new(dir.path().to_path_buf())
                .await
                .expect("app state"),
        );
        let session_id = "permission-stale-deny";
        let request_id = "permission-stale-deny-request";
        let request = test_permission_request(
            session_id,
            request_id,
            0,
            PermissionRequest::ordinary_decisions(true),
        );
        let mut session = parked_permission_session(&request);
        state.save_and_cache_session(&mut session).await;

        let mut candidate = state.permission_section.snapshot().data.as_ref().clone();
        candidate.durable_rules.push(matching_deny_rule());
        state
            .permission_section
            .commit(0, candidate)
            .expect("commit newer deny policy");
        let snapshot = state.permission_section.snapshot();
        state
            .permission_checker
            .permission_config()
            .expect("permission config")
            .publish_persistent_policy(snapshot.revision, snapshot.data.as_ref());

        let response = super::super::pending::get_pending_question(
            state.clone(),
            web::Path::from(session_id.to_string()),
        )
        .await
        .expect("refresh pending question");
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .expect("response body");
        let body: serde_json::Value = serde_json::from_slice(&body).expect("response JSON");
        assert_eq!(body["permission_request"]["policy_revision"], 1);
        assert_eq!(
            body["permission_request"]["allowed_decisions"],
            serde_json::json!(["deny_once", "deny_session"])
        );
        assert!(state
            .load_session(session_id)
            .await
            .expect("session remains parked")
            .pending_question
            .is_some());

        let error = submit_permission_decision(
            state.clone(),
            web::Path::from(session_id.to_string()),
            web::Json(PermissionDecision {
                request_id: request_id.to_string(),
                request_generation: format!("generation-{request_id}"),
                decision: PermissionDecisionKind::AllowOnce,
                matcher_id: None,
                expected_policy_revision: Some(1),
                confirm_global: false,
            }),
        )
        .await
        .expect_err("new deny policy cannot be overridden by the stale prompt");
        assert!(matches!(error, AppError::Forbidden(_)));

        let response = submit_permission_decision(
            state.clone(),
            web::Path::from(session_id.to_string()),
            web::Json(PermissionDecision {
                request_id: request_id.to_string(),
                request_generation: format!("generation-{request_id}"),
                decision: PermissionDecisionKind::DenyOnce,
                matcher_id: None,
                expected_policy_revision: Some(1),
                confirm_global: false,
            }),
        )
        .await
        .expect("explicit denial resolves the parked request");
        assert_eq!(response.status(), actix_web::http::StatusCode::OK);
        assert!(state
            .load_session(session_id)
            .await
            .expect("session remains")
            .pending_question
            .is_none());
    }

    #[actix_web::test]
    async fn durable_rule_recovers_crash_boundaries_and_rejects_changed_decision() {
        let dir = tempfile::tempdir().expect("temp dir");
        let session_id = "permission-durable-crash-recovery";
        let request_id = "permission-durable-crash-request";
        let request = test_permission_request(
            session_id,
            request_id,
            0,
            PermissionRequest::ordinary_decisions(true),
        );
        let decision = PermissionDecision {
            request_id: request_id.to_string(),
            request_generation: format!("generation-{request_id}"),
            decision: PermissionDecisionKind::AllowWorkspace,
            // A legal omitted matcher must canonicalize to exact_resource
            // before the durable receipt is reconstructed after restart.
            matcher_id: None,
            // The request was issued at revision 0, then safely refreshed at
            // revision 1 before this durable decision committed.
            expected_policy_revision: Some(1),
            confirm_global: false,
        };

        // Simulate a crash after the durable rule commit but before the
        // process-local receipt or pending response was recorded.
        {
            let state = web::Data::new(
                AppState::new(dir.path().to_path_buf())
                    .await
                    .expect("initial app state"),
            );
            let mut session = parked_permission_session(&request);
            state.save_and_cache_session(&mut session).await;
            let mut candidate = state.permission_section.snapshot().data.as_ref().clone();
            candidate.durable_rules.push(unrelated_rule());
            state
                .permission_section
                .commit(0, candidate)
                .expect("advance policy before remembered decision");
            let mut candidate = state.permission_section.snapshot().data.as_ref().clone();
            candidate.durable_rules.push(
                durable_rule_for_decision(&request, &decision)
                    .expect("valid remembered rule")
                    .expect("durable decision"),
            );
            state
                .permission_section
                .commit(1, candidate)
                .expect("commit remembered rule");
        }

        let state = web::Data::new(
            AppState::new(dir.path().to_path_buf())
                .await
                .expect("restarted app state"),
        );
        assert!(state
            .permission_checker
            .permission_config()
            .expect("permission config")
            .decision_receipt(session_id, request_id, &format!("generation-{request_id}"),)
            .is_none());

        let conflict = PermissionDecision {
            request_id: request_id.to_string(),
            request_generation: format!("generation-{request_id}"),
            decision: PermissionDecisionKind::DenyOnce,
            matcher_id: None,
            expected_policy_revision: None,
            confirm_global: false,
        };
        let conflict_response = submit_permission_decision(
            state.clone(),
            web::Path::from(session_id.to_string()),
            web::Json(conflict),
        )
        .await
        .expect("committed durable decision conflict is recoverable");
        assert_eq!(
            conflict_response.status(),
            actix_web::http::StatusCode::CONFLICT
        );
        let conflict_body = actix_web::body::to_bytes(conflict_response.into_body())
            .await
            .expect("conflict body");
        let conflict_body: serde_json::Value =
            serde_json::from_slice(&conflict_body).expect("conflict JSON");
        assert_eq!(
            conflict_body["conflict"],
            "permission_decision_already_resolved"
        );
        assert!(state
            .load_session(session_id)
            .await
            .expect("session remains")
            .pending_question
            .is_some());

        let response = submit_permission_decision(
            state.clone(),
            web::Path::from(session_id.to_string()),
            web::Json(decision.clone()),
        )
        .await
        .expect("exact durable replay resumes pending request");
        assert_eq!(response.status(), actix_web::http::StatusCode::OK);
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .expect("response body");
        let body: serde_json::Value = serde_json::from_slice(&body).expect("response JSON");
        assert_eq!(body["replayed"], true);
        assert_eq!(state.permission_section.snapshot().revision, 2);

        let mut semantic_replay = decision.clone();
        semantic_replay.expected_policy_revision = Some(999);
        let response = submit_permission_decision(
            state.clone(),
            web::Path::from(session_id.to_string()),
            web::Json(semantic_replay),
        )
        .await
        .expect("post-commit CAS token is not part of receipt semantics");
        assert_eq!(response.status(), actix_web::http::StatusCode::OK);

        // The completed response now owns an immutable session-qualified
        // receipt. Removing the user-managed remembered rule must not erase
        // replay idempotency after another restart.
        let mut candidate = state.permission_section.snapshot().data.as_ref().clone();
        candidate
            .durable_rules
            .retain(|rule| !rule.id.starts_with("remembered:workspace:"));
        state
            .permission_section
            .commit(2, candidate)
            .expect("remove remembered user rule after completion");
        drop(state);

        // The consumed tool result retains the typed request and exact receipt
        // in non-visible metadata, so a second restart can acknowledge replay
        // without the editable rule or another successor.
        let restarted = web::Data::new(
            AppState::new(dir.path().to_path_buf())
                .await
                .expect("second restarted app state"),
        );
        let mut persisted_semantic_replay = decision;
        persisted_semantic_replay.expected_policy_revision = Some(1_000);
        let response = submit_permission_decision(
            restarted,
            web::Path::from(session_id.to_string()),
            web::Json(persisted_semantic_replay),
        )
        .await
        .expect("completed durable replay is recoverable");
        assert_eq!(response.status(), actix_web::http::StatusCode::OK);
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .expect("response body");
        let body: serde_json::Value = serde_json::from_slice(&body).expect("response JSON");
        assert_eq!(body["replayed"], true);
        assert!(body.get("resume").is_none());
    }
}
