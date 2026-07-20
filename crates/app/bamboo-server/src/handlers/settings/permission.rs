//! Permission rule settings backed by the independent permission section.

use std::path::PathBuf;
use std::sync::Arc;

use actix_web::{web, HttpResponse};
use bamboo_config::{ConfigStoreError, SectionSnapshot, SectionSourceKind, SectionStatus};
use bamboo_tools::permission::SerializablePermissionConfig;
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
        config.set_ask_rules(snapshot.data.ask_rules.clone());
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
