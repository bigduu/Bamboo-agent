//! Typed permission-decision endpoint.

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

pub async fn submit_permission_decision(
    state: web::Data<AppState>,
    session_id: web::Path<String>,
    payload: web::Json<PermissionDecision>,
) -> Result<HttpResponse, AppError> {
    let session_id = session_id.into_inner();
    let decision = payload.into_inner();
    let Some(config) = state.permission_checker.permission_config() else {
        return Err(AppError::InternalError(anyhow::anyhow!(
            "permission checker does not expose typed decisions"
        )));
    };

    let _guard = state.permission_io_lock.lock().await;
    let existing = config.decision_receipt(&session_id, &decision.request_id);
    let replayed = if let Some(receipt) = existing.as_ref() {
        if receipt.decision != decision {
            return Err(AppError::BadRequest(
                "request already resolved with a different decision".to_string(),
            ));
        }
        true
    } else {
        let request = config
            .pending_request(&session_id, &decision.request_id)
            .ok_or_else(|| AppError::NotFound("pending permission request".to_string()))?;
        if !request.allowed_decisions.contains(&decision.decision) {
            return Err(AppError::Forbidden(format!(
                "decision {:?} is not allowed for this request",
                decision.decision
            )));
        }
        if let Some(expected) = decision.expected_policy_revision {
            let actual = state.permission_section.snapshot().revision;
            if expected != actual {
                return Err(AppError::ConfigConflict { expected, actual });
            }
        }

        match decision.decision {
            PermissionDecisionKind::AllowOnce => config.grant_once(
                &session_id,
                &request.request_id,
                request.permission_type,
                request.resource.clone(),
            ),
            PermissionDecisionKind::AllowSession => {
                let matcher = selected_matcher(&request, &decision)?;
                config.grant_scoped_session_permission(
                    &session_id,
                    request.permission_type,
                    matcher.value,
                );
            }
            PermissionDecisionKind::DenySession => {
                let matcher = selected_matcher(&request, &decision)?;
                config.deny_scoped_session_permission(
                    &session_id,
                    request.permission_type,
                    matcher.value,
                );
            }
            PermissionDecisionKind::AllowWorkspace | PermissionDecisionKind::AllowGlobal => {
                let expected_revision = decision.expected_policy_revision.ok_or_else(|| {
                    AppError::BadRequest(
                        "durable decision requires expected_policy_revision".to_string(),
                    )
                })?;
                if decision.decision == PermissionDecisionKind::AllowGlobal
                    && !decision.confirm_global
                {
                    return Err(AppError::BadRequest(
                        "global allow requires explicit second confirmation".to_string(),
                    ));
                }
                let matcher = selected_matcher(&request, &decision)?;
                matcher
                    .validate(request.permission_type)
                    .map_err(AppError::BadRequest)?;
                let scope = if decision.decision == PermissionDecisionKind::AllowWorkspace {
                    PermissionRuleScope::Workspace
                } else {
                    PermissionRuleScope::Global
                };
                let workspace_path = if scope == PermissionRuleScope::Workspace {
                    Some(request.workspace_path.clone().ok_or_else(|| {
                        AppError::BadRequest(
                            "workspace decision requires a stable workspace identity".to_string(),
                        )
                    })?)
                } else {
                    None
                };
                let durable_rule = DurablePermissionRule {
                    id: format!(
                        "remembered:{}:{}",
                        match scope {
                            PermissionRuleScope::Workspace => "workspace",
                            PermissionRuleScope::Global => "global",
                        },
                        request.request_id
                    ),
                    permission_type: request.permission_type,
                    effect: PermissionRuleEffect::Allow,
                    scope,
                    workspace_path,
                    matcher,
                    source: PermissionRuleSource::User,
                    expires_at: None,
                };
                durable_rule.validate().map_err(AppError::BadRequest)?;
                let mut candidate = state.permission_section.snapshot().data.as_ref().clone();
                candidate
                    .durable_rules
                    .retain(|rule| rule.id != durable_rule.id);
                candidate.durable_rules.push(durable_rule);
                let section = state.permission_section.clone();
                tokio::task::spawn_blocking(move || section.commit(expected_revision, candidate))
                    .await
                    .map_err(|error| {
                        AppError::InternalError(anyhow::anyhow!(
                            "permission commit task failed: {error}"
                        ))
                    })?
                    .map_err(map_store_error)?;
                let snapshot = state.permission_section.snapshot();
                config.publish_persistent_policy(snapshot.revision, snapshot.data.as_ref());
            }
            PermissionDecisionKind::DenyOnce => {}
        }
        config
            .record_decision(&session_id, decision.clone())
            .map_err(AppError::BadRequest)?;
        false
    };
    let receipt = config
        .decision_receipt(&session_id, &decision.request_id)
        .expect("decision receipt was recorded or observed");
    drop(_guard);

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
        }));
    }

    let legacy_response = match decision.decision {
        PermissionDecisionKind::AllowOnce
        | PermissionDecisionKind::AllowSession
        | PermissionDecisionKind::AllowWorkspace
        | PermissionDecisionKind::AllowGlobal => "Approve",
        PermissionDecisionKind::DenyOnce | PermissionDecisionKind::DenySession => "Deny",
    };
    let response = super::submit::submit_response(
        state,
        web::Path::from(session_id),
        web::Json(RespondRequest {
            response: legacy_response.to_string(),
            model: None,
            provider: None,
            model_ref: None,
            reasoning_effort: None,
        }),
    )
    .await
    .map_err(|error| AppError::InternalError(anyhow::anyhow!(error.to_string())))?;

    if !response.status().is_success() {
        return Ok(response);
    }
    Ok(HttpResponse::Ok().json(PermissionDecisionResponse {
        success: true,
        replayed,
        receipt,
        resume: Some(serde_json::json!({ "accepted": true })),
    }))
}
