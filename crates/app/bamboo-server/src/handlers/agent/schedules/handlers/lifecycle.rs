use actix_web::{web, HttpResponse, Result};

use crate::app_state::AppState;
use crate::schedule_app::ScheduleRunJob;

use super::super::types::{CreateScheduleRequest, PatchScheduleRequest, ScheduleView};
use super::super::validation::{
    resolve_create_schedule_definition, resolve_patch_schedule_definition,
    validate_auto_execute_run_config, validate_schedule_name,
};
use super::response::{internal_server_error, schedule_not_found};

/// `POST /api/v1/schedules`
pub async fn create_schedule(
    state: web::Data<AppState>,
    req: web::Json<CreateScheduleRequest>,
) -> Result<HttpResponse> {
    let name = match validate_schedule_name(&req.name) {
        Ok(name) => name,
        Err(response) => return Ok(response),
    };
    let resolved = match resolve_create_schedule_definition(&req) {
        Ok(value) => value,
        Err(response) => return Ok(response),
    };
    let run_config = match validate_auto_execute_run_config(&state, &req.run_config).await {
        Ok(run_config) => run_config,
        Err(response) => return Ok(response),
    };

    let created = state
        .schedule_store
        .create_schedule_with_definition(name, req.enabled, run_config, resolved.definition)
        .await
        .map_err(|error| internal_server_error("create schedule", error))?;

    Ok(HttpResponse::Ok().json(ScheduleView::from(created)))
}

/// `PATCH /api/v1/schedules/{schedule_id}`
pub async fn patch_schedule(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: web::Json<PatchScheduleRequest>,
) -> Result<HttpResponse> {
    let id = path.into_inner();
    let name = normalize_optional_name(req.name.as_deref());

    let run_config = match req.run_config.as_ref() {
        Some(run_config) => {
            let Some(existing) = state.schedule_store.get_schedule(&id).await else {
                return Ok(schedule_not_found(&id));
            };
            let mut run_config = run_config.clone();
            // `project_id` predates many PATCH clients. Its serde default is
            // `None`, so an omitted nested field is indistinguishable from
            // explicit null in the legacy DTO. Preserve membership until the
            // API grows an explicit tri-state clear operation.
            if run_config.project_id.is_none() {
                run_config.project_id = existing.run_config.project_id;
            }
            match validate_auto_execute_run_config(&state, &run_config).await {
                Ok(run_config) => Some(run_config),
                Err(response) => return Ok(response),
            }
        }
        None => None,
    };

    let resolved = match resolve_patch_schedule_definition(&req) {
        Ok(value) => value,
        Err(response) => return Ok(response),
    };

    let updated = state
        .schedule_store
        .patch_schedule_with_definition(&id, name, req.enabled, run_config, resolved.definition)
        .await
        .map_err(|error| internal_server_error("patch schedule", error))?;

    match updated {
        Some(schedule) => Ok(HttpResponse::Ok().json(ScheduleView::from(schedule))),
        None => Ok(schedule_not_found(&id)),
    }
}

/// `DELETE /api/v1/schedules/{schedule_id}`
pub async fn delete_schedule(
    state: web::Data<AppState>,
    path: web::Path<String>,
) -> Result<HttpResponse> {
    let id = path.into_inner();
    let deleted = state
        .schedule_store
        .delete_schedule(&id)
        .await
        .map_err(|error| internal_server_error("delete schedule", error))?;

    if deleted {
        Ok(HttpResponse::Ok().json(serde_json::json!({ "success": true })))
    } else {
        Ok(schedule_not_found(&id))
    }
}

/// `POST /api/v1/schedules/{schedule_id}/run`
pub async fn run_now(state: web::Data<AppState>, path: web::Path<String>) -> Result<HttpResponse> {
    let id = path.into_inner();
    let Some(schedule) = state.schedule_store.get_schedule(&id).await else {
        return Ok(schedule_not_found(&id));
    };
    if let Err(response) = validate_auto_execute_run_config(&state, &schedule.run_config).await {
        return Ok(response);
    }
    let Some(claimed) = state
        .schedule_store
        .create_run_now_if_config(&id, &schedule.run_config)
        .await
        .map_err(|error| internal_server_error("create run job", error))?
    else {
        return Ok(schedule_not_found(&id));
    };

    let enqueued_at = claimed.claimed_at;
    state
        .schedule_manager
        .enqueue_run_now(ScheduleRunJob {
            run_id: claimed.run_id.clone(),
            schedule_id: claimed.schedule_id.clone(),
            schedule_name: claimed.schedule_name.clone(),
            run_config: claimed.run_config.clone(),
            scheduled_for: claimed.scheduled_for,
            claimed_at: claimed.claimed_at,
            was_catch_up: claimed.was_catch_up,
        })
        .await
        .map_err(|error| internal_server_error("enqueue run", error))?;

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "success": true,
        "schedule_id": claimed.schedule_id,
        "run_id": claimed.run_id,
        "enqueued_at": enqueued_at
    })))
}

pub(in crate::handlers::agent::schedules) fn normalize_optional_name(
    value: Option<&str>,
) -> Option<String> {
    value
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(ToString::to_string)
}

#[cfg(test)]
mod tests {
    use super::normalize_optional_name;

    #[test]
    fn normalize_optional_name_trims_non_empty_values() {
        let normalized = normalize_optional_name(Some("  hourly sweep  "));
        assert_eq!(normalized.as_deref(), Some("hourly sweep"));
    }

    #[cfg(test)]
    mod http_tests {
        use actix_web::{http::StatusCode, test, web, App};
        use serde_json::Value;
        use tempfile::tempdir;

        use crate::routes::configure_routes;
        use crate::AppState;

        /// `DELETE /api/v1/schedules/{id}` for an unknown schedule must use
        /// the canonical nested error envelope (`{"error": {"message",
        /// "type"}}`), not the old flat `{"error": "<string>"}` shape.
        /// #251/#507.
        #[actix_web::test]
        async fn delete_schedule_not_found_uses_canonical_error_envelope() {
            let temp_dir = tempdir().expect("tempdir");
            bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
            let state = web::Data::new(
                AppState::new(temp_dir.path().to_path_buf())
                    .await
                    .expect("app state"),
            );
            let app = test::init_service(
                App::new()
                    .app_data(state.clone())
                    .configure(configure_routes),
            )
            .await;

            let resp = test::call_service(
                &app,
                test::TestRequest::delete()
                    .uri("/api/v1/schedules/does-not-exist")
                    .to_request(),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::NOT_FOUND);

            let body: Value = test::read_body_json(resp).await;
            assert_eq!(body["error"]["type"], "api_error");
            assert_eq!(body["error"]["message"], "Schedule not found");
            assert_eq!(body["schedule_id"], "does-not-exist");
        }

        #[actix_web::test]
        async fn run_now_rejects_archived_or_missing_project_before_run_creation() {
            let temp_dir = tempdir().expect("tempdir");
            bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
            let state = web::Data::new(
                AppState::new(temp_dir.path().to_path_buf())
                    .await
                    .expect("app state"),
            );
            let project = state.project_store.create("Scheduled", None).unwrap();
            let archived_schedule = state
                .schedule_store
                .create_schedule(
                    "archived".to_string(),
                    crate::schedule_app::ScheduleTrigger::Interval {
                        every_seconds: 3600,
                        anchor_at: None,
                    },
                    true,
                    crate::schedule_app::ScheduleRunConfig {
                        project_id: Some(project.id.clone()),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            state
                .project_store
                .archive(&project.id, project.revision)
                .unwrap();
            let missing_schedule = state
                .schedule_store
                .create_schedule(
                    "missing".to_string(),
                    crate::schedule_app::ScheduleTrigger::Interval {
                        every_seconds: 3600,
                        anchor_at: None,
                    },
                    true,
                    crate::schedule_app::ScheduleRunConfig {
                        project_id: Some("project-missing".parse().unwrap()),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let app = test::init_service(
                App::new()
                    .app_data(state.clone())
                    .configure(configure_routes),
            )
            .await;

            for (schedule_id, expected) in [
                (archived_schedule.id, StatusCode::CONFLICT),
                (missing_schedule.id, StatusCode::BAD_REQUEST),
            ] {
                let response = test::call_service(
                    &app,
                    test::TestRequest::post()
                        .uri(&format!("/api/v1/schedules/{schedule_id}/run"))
                        .to_request(),
                )
                .await;
                assert_eq!(response.status(), expected);
                assert!(
                    state
                        .schedule_store
                        .list_run_records_for_schedule(&schedule_id)
                        .await
                        .is_empty(),
                    "run_now validation must happen before a queued run record is created"
                );
            }
        }

        #[actix_web::test]
        async fn legacy_run_config_patch_preserves_existing_project_membership() {
            let temp_dir = tempdir().expect("tempdir");
            bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
            let state = web::Data::new(
                AppState::new(temp_dir.path().to_path_buf())
                    .await
                    .expect("app state"),
            );
            let project = state.project_store.create("Scheduled", None).unwrap();
            let schedule = state
                .schedule_store
                .create_schedule(
                    "legacy patch".to_string(),
                    crate::schedule_app::ScheduleTrigger::Interval {
                        every_seconds: 3600,
                        anchor_at: None,
                    },
                    true,
                    crate::schedule_app::ScheduleRunConfig {
                        project_id: Some(project.id.clone()),
                        task_message: Some("old task".to_string()),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let app = test::init_service(
                App::new()
                    .app_data(state.clone())
                    .configure(configure_routes),
            )
            .await;

            let response = test::call_service(
                &app,
                test::TestRequest::patch()
                    .uri(&format!("/api/v1/schedules/{}", schedule.id))
                    .set_json(serde_json::json!({
                        "run_config": {
                            "task_message": "updated by an old client",
                            "auto_execute": false
                        }
                    }))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            let persisted = state
                .schedule_store
                .get_schedule(&schedule.id)
                .await
                .expect("schedule");
            assert_eq!(persisted.run_config.project_id, Some(project.id));
            assert_eq!(
                persisted.run_config.task_message.as_deref(),
                Some("updated by an old client")
            );
        }

        #[actix_web::test]
        async fn create_patch_and_run_now_reject_cross_project_workspace_before_side_effects() {
            let temp_dir = tempdir().expect("tempdir");
            bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
            let workspace = tempdir().expect("workspace");
            let state = web::Data::new(
                AppState::new(temp_dir.path().to_path_buf())
                    .await
                    .expect("app state"),
            );
            let session_project = state
                .project_store
                .create("Schedule Project", None)
                .expect("Schedule Project");
            let _workspace_owner = state
                .project_store
                .create_with_bindings(
                    "Workspace Owner",
                    None,
                    vec![bamboo_domain::WorkspaceBinding {
                        path: workspace.path().to_string_lossy().into_owned(),
                        label: None,
                        git_common_dir: None,
                    }],
                )
                .expect("Workspace Owner");
            let app = test::init_service(
                App::new()
                    .app_data(state.clone())
                    .configure(configure_routes),
            )
            .await;
            let conflicting_run_config = serde_json::json!({
                "project_id": session_project.id,
                "workspace_path": workspace.path(),
                "auto_execute": false
            });

            let create = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri("/api/v1/schedules")
                    .set_json(serde_json::json!({
                        "name": "Must not persist",
                        "trigger": {"type": "interval", "every_seconds": 3600},
                        "run_config": conflicting_run_config
                    }))
                    .to_request(),
            )
            .await;
            assert_eq!(create.status(), StatusCode::CONFLICT);
            assert!(state.schedule_store.list_schedules().await.is_empty());

            let schedule = state
                .schedule_store
                .create_schedule(
                    "Existing".to_string(),
                    crate::schedule_app::ScheduleTrigger::Interval {
                        every_seconds: 3600,
                        anchor_at: None,
                    },
                    true,
                    crate::schedule_app::ScheduleRunConfig {
                        project_id: Some(session_project.id.clone()),
                        ..Default::default()
                    },
                )
                .await
                .expect("schedule");
            let patch = test::call_service(
                &app,
                test::TestRequest::patch()
                    .uri(&format!("/api/v1/schedules/{}", schedule.id))
                    .set_json(serde_json::json!({
                        "run_config": conflicting_run_config
                    }))
                    .to_request(),
            )
            .await;
            assert_eq!(patch.status(), StatusCode::CONFLICT);
            let unchanged = state
                .schedule_store
                .get_schedule(&schedule.id)
                .await
                .expect("schedule remains");
            assert!(unchanged.run_config.workspace_path.is_none());

            state
                .schedule_store
                .patch_schedule(
                    &schedule.id,
                    None,
                    None,
                    None,
                    Some(crate::schedule_app::ScheduleRunConfig {
                        project_id: Some(session_project.id),
                        workspace_path: Some(workspace.path().to_string_lossy().into_owned()),
                        ..Default::default()
                    }),
                )
                .await
                .expect("inject legacy conflicting config");
            let run_now = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri(&format!("/api/v1/schedules/{}/run", schedule.id))
                    .to_request(),
            )
            .await;
            assert_eq!(run_now.status(), StatusCode::CONFLICT);
            assert!(
                state
                    .schedule_store
                    .list_run_records_for_schedule(&schedule.id)
                    .await
                    .is_empty(),
                "run_now must fail before creating a run record"
            );
        }
    }

    #[test]
    fn normalize_optional_name_drops_blank_values() {
        let normalized = normalize_optional_name(Some("   "));
        assert_eq!(normalized, None);
    }
}
