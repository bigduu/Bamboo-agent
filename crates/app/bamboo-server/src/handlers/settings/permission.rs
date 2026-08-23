//! Permission rule settings backed by the independent permission section.

use std::path::PathBuf;
use std::sync::Arc;

use actix_web::{web, HttpResponse};
use bamboo_config::{ConfigStoreError, SectionSnapshot, SectionSourceKind, SectionStatus};
use bamboo_tools::permission::{
    DurablePermissionRule, ParsedRule, PermissionDecisionKind, PermissionEvaluation,
    PermissionOutcome, PermissionType, SerializablePermissionConfig, TemporaryPermissionGrant,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{app_state::AppState, error::AppError};

#[derive(Debug, Serialize)]
pub struct AskRulesResponse {
    pub rules: Vec<String>,
    pub revision: u64,
    pub loaded_at: DateTime<Utc>,
    pub source_path: PathBuf,
    pub source_kind: SectionSourceKind,
    pub status: SectionStatus,
    pub last_error: Option<String>,
}

impl AskRulesResponse {
    fn from_snapshot(snapshot: &SectionSnapshot<SerializablePermissionConfig>) -> Self {
        Self {
            rules: snapshot.data.ask_rules.clone(),
            revision: snapshot.revision,
            loaded_at: snapshot.loaded_at,
            source_path: snapshot.source_path.clone(),
            source_kind: snapshot.source_kind,
            status: snapshot.status,
            last_error: snapshot.last_error.clone(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct UpdateAskRulesRequest {
    /// Clients should send the revision returned by GET. Optional only for
    /// compatibility with pre-revision clients; those use the current process
    /// snapshot and still perform a store-level CAS.
    #[serde(default)]
    pub expected_revision: Option<u64>,
    #[serde(default)]
    pub rules: Vec<String>,
}

pub async fn get_permission_ask_rules(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let snapshot = app_state.permission_section.snapshot();
    Ok(HttpResponse::Ok().json(AskRulesResponse::from_snapshot(&snapshot)))
}

/// Replaces always-ask rules with durable-before-live, cancellation-safe ordering.
pub async fn update_permission_ask_rules(
    app_state: web::Data<AppState>,
    payload: web::Json<UpdateAskRulesRequest>,
) -> Result<HttpResponse, AppError> {
    let req = payload.into_inner();
    let mut rules: Vec<String> = req
        .rules
        .into_iter()
        .map(|rule| rule.trim().to_string())
        .filter(|rule| !rule.is_empty())
        .collect();
    let mut seen = std::collections::HashSet::new();
    rules.retain(|rule| seen.insert(rule.clone()));
    for (index, rule) in rules.iter().enumerate() {
        ParsedRule::try_parse(rule).map_err(|error| {
            AppError::BadRequest(format!("invalid ask rule at index {index}: {error}"))
        })?;
    }

    let Some(config) = app_state.permission_checker.permission_config() else {
        return Err(AppError::InternalError(anyhow::anyhow!(
            "permission checker does not support configurable rules"
        )));
    };

    let section = Arc::clone(&app_state.permission_section);
    let io_lock = Arc::clone(&app_state.permission_io_lock);
    let expected_revision = req
        .expected_revision
        .unwrap_or_else(|| app_state.permission_section.snapshot().revision);
    // This task is deliberately detached from request cancellation. Once a
    // mutation starts, it completes the durable commit and live publication as
    // one serialized operation even if the client disconnects.
    let mutation = tokio::spawn(async move {
        let _guard = io_lock.lock().await;
        let mut candidate = section.snapshot().data.as_ref().clone();
        candidate.ask_rules = rules;
        let writer = Arc::clone(&section);
        tokio::task::spawn_blocking(move || writer.commit(expected_revision, candidate))
            .await
            .map_err(|error| {
                AppError::InternalError(anyhow::anyhow!("permission commit task failed: {error}"))
            })?
            .map_err(map_store_error)?;

        let snapshot = section.snapshot();
        config.publish_persistent_policy(snapshot.revision, snapshot.data.as_ref());
        Ok::<_, AppError>(snapshot)
    });

    let snapshot = mutation.await.map_err(|error| {
        AppError::InternalError(anyhow::anyhow!("permission mutation task failed: {error}"))
    })??;
    Ok(HttpResponse::Ok().json(AskRulesResponse::from_snapshot(&snapshot)))
}

fn map_store_error(error: ConfigStoreError) -> AppError {
    match error {
        ConfigStoreError::Conflict { expected, actual } => {
            AppError::ConfigConflict { expected, actual }
        }
        other => AppError::InternalError(anyhow::anyhow!(
            "failed to persist permission rules: {other}"
        )),
    }
}

fn require_current_revision(expected: u64, actual: u64) -> Result<(), AppError> {
    if expected == actual {
        Ok(())
    } else {
        Err(AppError::ConfigConflict { expected, actual })
    }
}

#[derive(Debug, Serialize)]
pub struct PermissionPolicyResponse {
    pub revision: u64,
    pub loaded_at: DateTime<Utc>,
    pub source_path: PathBuf,
    pub source_kind: SectionSourceKind,
    pub status: SectionStatus,
    pub last_error: Option<String>,
    pub policy: SerializablePermissionConfig,
    pub temporary_grants: Vec<TemporaryPermissionGrant>,
}

impl PermissionPolicyResponse {
    fn from_snapshot(
        snapshot: &SectionSnapshot<SerializablePermissionConfig>,
        temporary_grants: Vec<TemporaryPermissionGrant>,
    ) -> Self {
        Self {
            revision: snapshot.revision,
            loaded_at: snapshot.loaded_at,
            source_path: snapshot.source_path.clone(),
            source_kind: snapshot.source_kind,
            status: snapshot.status,
            last_error: snapshot.last_error.clone(),
            policy: snapshot.data.as_ref().clone(),
            temporary_grants,
        }
    }
}

fn permission_policy_response(
    app_state: &web::Data<AppState>,
    snapshot: &SectionSnapshot<SerializablePermissionConfig>,
) -> Result<PermissionPolicyResponse, AppError> {
    let Some(config) = app_state.permission_checker.permission_config() else {
        return Err(AppError::InternalError(anyhow::anyhow!(
            "permission checker does not expose runtime grants"
        )));
    };
    Ok(PermissionPolicyResponse::from_snapshot(
        snapshot,
        config.temporary_grants(),
    ))
}

pub async fn get_permission_policy(
    app_state: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    let snapshot = app_state.permission_section.snapshot();
    Ok(HttpResponse::Ok().json(permission_policy_response(&app_state, &snapshot)?))
}

#[derive(Debug, Deserialize)]
pub struct PutPermissionRuleRequest {
    pub expected_revision: u64,
    pub rule: DurablePermissionRule,
}

#[derive(Debug, Deserialize)]
pub struct DeletePermissionRuleRequest {
    pub expected_revision: u64,
}

async fn commit_permission_candidate(
    app_state: &web::Data<AppState>,
    expected_revision: u64,
    candidate: SerializablePermissionConfig,
) -> Result<Arc<SectionSnapshot<SerializablePermissionConfig>>, AppError> {
    let Some(config) = app_state.permission_checker.permission_config() else {
        return Err(AppError::InternalError(anyhow::anyhow!(
            "permission checker does not support configurable rules"
        )));
    };
    let section = Arc::clone(&app_state.permission_section);
    let io_lock = Arc::clone(&app_state.permission_io_lock);
    tokio::spawn(async move {
        let _guard = io_lock.lock().await;
        let writer = Arc::clone(&section);
        tokio::task::spawn_blocking(move || writer.commit(expected_revision, candidate))
            .await
            .map_err(|error| {
                AppError::InternalError(anyhow::anyhow!("permission commit task failed: {error}"))
            })?
            .map_err(map_store_error)?;
        let snapshot = section.snapshot();
        config.publish_persistent_policy(snapshot.revision, snapshot.data.as_ref());
        Ok::<_, AppError>(snapshot)
    })
    .await
    .map_err(|error| {
        AppError::InternalError(anyhow::anyhow!("permission mutation task failed: {error}"))
    })?
}

pub async fn create_permission_rule(
    app_state: web::Data<AppState>,
    payload: web::Json<PutPermissionRuleRequest>,
) -> Result<HttpResponse, AppError> {
    let request = payload.into_inner();
    request.rule.validate().map_err(AppError::BadRequest)?;
    let snapshot = app_state.permission_section.snapshot();
    require_current_revision(request.expected_revision, snapshot.revision)?;
    if snapshot
        .data
        .durable_rules
        .iter()
        .any(|rule| rule.id == request.rule.id)
    {
        return Err(AppError::BadRequest(format!(
            "permission rule '{}' already exists",
            request.rule.id
        )));
    }
    let mut candidate = snapshot.data.as_ref().clone();
    candidate.durable_rules.push(request.rule);
    let snapshot =
        commit_permission_candidate(&app_state, request.expected_revision, candidate).await?;
    Ok(HttpResponse::Created().json(permission_policy_response(&app_state, &snapshot)?))
}

pub async fn update_permission_rule(
    app_state: web::Data<AppState>,
    rule_id: web::Path<String>,
    payload: web::Json<PutPermissionRuleRequest>,
) -> Result<HttpResponse, AppError> {
    let rule_id = rule_id.into_inner();
    let request = payload.into_inner();
    if request.rule.id != rule_id {
        return Err(AppError::BadRequest(
            "rule id in path and body must match".to_string(),
        ));
    }
    request.rule.validate().map_err(AppError::BadRequest)?;
    let snapshot = app_state.permission_section.snapshot();
    require_current_revision(request.expected_revision, snapshot.revision)?;
    let mut candidate = snapshot.data.as_ref().clone();
    let Some(existing) = candidate
        .durable_rules
        .iter_mut()
        .find(|rule| rule.id == rule_id)
    else {
        return Err(AppError::NotFound(format!("permission rule '{rule_id}'")));
    };
    *existing = request.rule;
    let snapshot =
        commit_permission_candidate(&app_state, request.expected_revision, candidate).await?;
    Ok(HttpResponse::Ok().json(permission_policy_response(&app_state, &snapshot)?))
}

pub async fn delete_permission_rule(
    app_state: web::Data<AppState>,
    rule_id: web::Path<String>,
    query: web::Query<DeletePermissionRuleRequest>,
) -> Result<HttpResponse, AppError> {
    let rule_id = rule_id.into_inner();
    let snapshot = app_state.permission_section.snapshot();
    require_current_revision(query.expected_revision, snapshot.revision)?;
    let mut candidate = snapshot.data.as_ref().clone();
    let before = candidate.durable_rules.len();
    candidate.durable_rules.retain(|rule| rule.id != rule_id);
    if candidate.durable_rules.len() == before {
        return Err(AppError::NotFound(format!("permission rule '{rule_id}'")));
    }
    let snapshot =
        commit_permission_candidate(&app_state, query.expected_revision, candidate).await?;
    Ok(HttpResponse::Ok().json(permission_policy_response(&app_state, &snapshot)?))
}

#[derive(Debug, Deserialize)]
pub struct DiagnosePermissionRequest {
    #[serde(default)]
    pub request_id: String,
    #[serde(default)]
    pub session_id: String,
    #[serde(default)]
    pub workspace_path: Option<String>,
    pub tool_name: String,
    #[serde(default)]
    pub tool_args: serde_json::Value,
    pub permission_type: PermissionType,
    pub resource: String,
    #[serde(default)]
    pub operation_summary: String,
    #[serde(default)]
    pub bypass_requested: bool,
    #[serde(default)]
    pub auto_approve_requested: bool,
    #[serde(default)]
    pub platform_hard_deny: Option<String>,
}

pub async fn diagnose_permission(
    app_state: web::Data<AppState>,
    payload: web::Json<DiagnosePermissionRequest>,
) -> Result<HttpResponse, AppError> {
    let request = payload.into_inner();
    let Some(config) = app_state.permission_checker.permission_config() else {
        return Err(AppError::InternalError(anyhow::anyhow!(
            "permission checker does not expose typed policy"
        )));
    };
    let outcome: PermissionOutcome = config.evaluate(PermissionEvaluation {
        request_id: request.request_id,
        session_id: request.session_id,
        workspace_path: request.workspace_path,
        tool_name: request.tool_name,
        tool_args: request.tool_args,
        permission_type: request.permission_type,
        resource: request.resource,
        operation_summary: request.operation_summary,
        risk_level: request.permission_type.risk_level(),
        bypass_requested: request.bypass_requested,
        auto_approve_requested: request.auto_approve_requested,
        platform_hard_deny: request.platform_hard_deny,
        consume_once: false,
        supported_decisions: PermissionDecisionKind::all_supported(),
    });
    Ok(HttpResponse::Ok().json(outcome))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn global_rule(
        effect: bamboo_tools::permission::PermissionRuleEffect,
    ) -> DurablePermissionRule {
        DurablePermissionRule {
            id: "rule-1".to_string(),
            permission_type: PermissionType::ExecuteCommand,
            effect,
            scope: bamboo_tools::permission::PermissionRuleScope::Global,
            workspace_path: None,
            matcher: bamboo_tools::permission::PermissionMatcher {
                id: "git-status".to_string(),
                kind: bamboo_tools::permission::PermissionMatcherKind::CommandPrefix,
                value: "git status".to_string(),
            },
            source: bamboo_tools::permission::PermissionRuleSource::User,
            expires_at: None,
        }
    }

    #[tokio::test]
    async fn conflict_leaves_live_policy_and_disk_unchanged_then_valid_cas_publishes() {
        let temp = tempfile::tempdir().unwrap();
        let state = web::Data::new(
            AppState::new(temp.path().to_path_buf())
                .await
                .expect("app state should initialize"),
        );

        let conflict = update_permission_ask_rules(
            state.clone(),
            web::Json(UpdateAskRulesRequest {
                expected_revision: Some(99),
                rules: vec!["Bash(git push *)".to_string()],
            }),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            conflict,
            AppError::ConfigConflict {
                expected: 99,
                actual: 0
            }
        ));
        assert!(state
            .permission_checker
            .permission_config()
            .unwrap()
            .ask_rule_patterns()
            .is_empty());
        assert!(!temp.path().join("permissions.json").exists());

        let response = update_permission_ask_rules(
            state.clone(),
            web::Json(UpdateAskRulesRequest {
                expected_revision: Some(0),
                rules: vec![" Bash(git push *) ".to_string()],
            }),
        )
        .await
        .unwrap();

        assert_eq!(response.status(), actix_web::http::StatusCode::OK);
        assert_eq!(
            state
                .permission_checker
                .permission_config()
                .unwrap()
                .ask_rule_patterns(),
            vec!["Bash(git push *)"]
        );
        let reopened = bamboo_tools::permission::PermissionSection::open(temp.path()).unwrap();
        assert_eq!(reopened.snapshot().revision, 1);
        assert_eq!(reopened.snapshot().data.ask_rules, vec!["Bash(git push *)"]);
    }

    #[tokio::test]
    async fn invalid_ask_rule_is_rejected_before_disk_or_live_mutation() {
        let temp = tempfile::tempdir().unwrap();
        let state = web::Data::new(
            AppState::new(temp.path().to_path_buf())
                .await
                .expect("app state should initialize"),
        );

        let error = update_permission_ask_rules(
            state.clone(),
            web::Json(UpdateAskRulesRequest {
                expected_revision: Some(0),
                rules: vec!["Bash(git push *".to_string()],
            }),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            AppError::BadRequest(message)
                if message.contains("invalid ask rule at index 0")
                    && message.contains("unbalanced parentheses")
        ));
        assert_eq!(state.permission_section.snapshot().revision, 0);
        assert!(state
            .permission_checker
            .permission_config()
            .unwrap()
            .ask_rule_patterns()
            .is_empty());
        assert!(!temp.path().join("permissions.json").exists());
    }

    #[tokio::test]
    async fn policy_response_projects_active_runtime_grants() {
        let temp = tempfile::tempdir().unwrap();
        let state = web::Data::new(
            AppState::new(temp.path().to_path_buf())
                .await
                .expect("app state should initialize"),
        );
        let config = state.permission_checker.permission_config().unwrap();
        config.grant_scoped_session_permission(
            "session-a",
            PermissionType::ExecuteCommand,
            "cargo test",
        );
        config.deny_scoped_session_permission(
            "session-a",
            PermissionType::WriteFile,
            "/private/**",
        );
        config.grant_once(
            "session-a",
            "request-1",
            PermissionType::GitWrite,
            "git push".to_string(),
        );

        let response = get_permission_policy(state).await.unwrap();
        let body = actix_web::body::to_bytes(response.into_body())
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let grants = body["temporary_grants"].as_array().unwrap();

        assert_eq!(grants.len(), 3);
        assert!(grants.iter().any(|grant| {
            grant["scope"] == "session"
                && grant["effect"] == "allow"
                && grant["session_id"] == "session-a"
                && grant["matcher"] == "cargo test"
                && grant["expires_at"].is_string()
        }));
        assert!(grants.iter().any(|grant| {
            grant["scope"] == "session"
                && grant["effect"] == "deny"
                && grant["matcher"] == "/private/**"
        }));
        assert!(grants.iter().any(|grant| {
            grant["scope"] == "one_shot"
                && grant["request_id"] == "request-1"
                && grant["matcher"] == "git push"
                && grant.get("expires_at").is_none()
        }));
    }

    #[tokio::test]
    async fn durable_rule_crud_is_cas_guarded_and_published_after_commit() {
        let temp = tempfile::tempdir().unwrap();
        let state = web::Data::new(
            AppState::new(temp.path().to_path_buf())
                .await
                .expect("app state should initialize"),
        );

        let created = create_permission_rule(
            state.clone(),
            web::Json(PutPermissionRuleRequest {
                expected_revision: 0,
                rule: global_rule(bamboo_tools::permission::PermissionRuleEffect::Allow),
            }),
        )
        .await
        .unwrap();
        assert_eq!(created.status(), actix_web::http::StatusCode::CREATED);
        assert_eq!(
            state
                .permission_checker
                .permission_config()
                .unwrap()
                .durable_rules()[0]
                .effect,
            bamboo_tools::permission::PermissionRuleEffect::Allow
        );

        update_permission_rule(
            state.clone(),
            web::Path::from("rule-1".to_string()),
            web::Json(PutPermissionRuleRequest {
                expected_revision: 1,
                rule: global_rule(bamboo_tools::permission::PermissionRuleEffect::Deny),
            }),
        )
        .await
        .unwrap();

        let stale = delete_permission_rule(
            state.clone(),
            web::Path::from("rule-1".to_string()),
            web::Query(DeletePermissionRuleRequest {
                expected_revision: 1,
            }),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            stale,
            AppError::ConfigConflict {
                expected: 1,
                actual: 2
            }
        ));
        assert_eq!(
            state
                .permission_checker
                .permission_config()
                .unwrap()
                .durable_rules()[0]
                .effect,
            bamboo_tools::permission::PermissionRuleEffect::Deny
        );

        delete_permission_rule(
            state.clone(),
            web::Path::from("rule-1".to_string()),
            web::Query(DeletePermissionRuleRequest {
                expected_revision: 2,
            }),
        )
        .await
        .unwrap();
        assert!(state
            .permission_checker
            .permission_config()
            .unwrap()
            .durable_rules()
            .is_empty());
        let reopened = bamboo_tools::permission::PermissionSection::open(temp.path()).unwrap();
        assert_eq!(reopened.snapshot().revision, 3);
        assert!(reopened.snapshot().data.durable_rules.is_empty());
    }

    #[tokio::test]
    async fn stale_rule_mutations_report_revision_conflict_before_semantic_state() {
        let temp = tempfile::tempdir().unwrap();
        let state = web::Data::new(
            AppState::new(temp.path().to_path_buf())
                .await
                .expect("app state should initialize"),
        );

        create_permission_rule(
            state.clone(),
            web::Json(PutPermissionRuleRequest {
                expected_revision: 0,
                rule: global_rule(bamboo_tools::permission::PermissionRuleEffect::Allow),
            }),
        )
        .await
        .unwrap();

        let stale_duplicate = create_permission_rule(
            state.clone(),
            web::Json(PutPermissionRuleRequest {
                expected_revision: 0,
                rule: global_rule(bamboo_tools::permission::PermissionRuleEffect::Allow),
            }),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            stale_duplicate,
            AppError::ConfigConflict {
                expected: 0,
                actual: 1
            }
        ));

        delete_permission_rule(
            state.clone(),
            web::Path::from("rule-1".to_string()),
            web::Query(DeletePermissionRuleRequest {
                expected_revision: 1,
            }),
        )
        .await
        .unwrap();

        let stale_missing = update_permission_rule(
            state,
            web::Path::from("rule-1".to_string()),
            web::Json(PutPermissionRuleRequest {
                expected_revision: 1,
                rule: global_rule(bamboo_tools::permission::PermissionRuleEffect::Deny),
            }),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            stale_missing,
            AppError::ConfigConflict {
                expected: 1,
                actual: 2
            }
        ));
    }
}
