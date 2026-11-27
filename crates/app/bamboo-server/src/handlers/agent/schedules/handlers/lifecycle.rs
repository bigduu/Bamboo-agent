use actix_web::{http::StatusCode, web, HttpResponse, Result};

use crate::app_state::AppState;
use crate::schedule_app::store::SchedulePatchRejection;
use crate::schedule_app::{ScheduleRunJob, ScheduleRunStatus};

use super::super::types::{
    CreateScheduleRequest, PatchScheduleRequest, ScheduleRunRecordView, ScheduleView,
};
use super::super::validation::{
    resolve_create_schedule_definition, resolve_patch_schedule_definition,
    validate_auto_execute_run_config, validate_schedule_name, validate_workflow_target,
};
use super::response::{internal_server_error, schedule_not_found};

/// `POST /api/v1/schedules`
pub async fn create_schedule(
    state: web::Data<AppState>,
    req: web::Json<CreateScheduleRequest>,
) -> Result<HttpResponse> {
    let name = match validate_schedule_name(&req.name) {
        Ok(name) => name,
        Err(response) => return Ok(*response),
    };
    let resolved = match resolve_create_schedule_definition(&req) {
        Ok(value) => value,
        Err(response) => return Ok(*response),
    };
    if let Err(response) = validate_workflow_target(&req.trigger, &req.run_config) {
        return Ok(*response);
    }
    let run_config = match validate_auto_execute_run_config(&state, &req.run_config).await {
        Ok(run_config) => run_config,
        Err(response) => return Ok(*response),
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
    let Some(existing) = state.schedule_store.get_schedule(&id).await else {
        return Ok(schedule_not_found(&id));
    };
    if let Err(response) = validate_workflow_target(
        req.trigger.as_ref().unwrap_or(&existing.trigger),
        req.run_config.as_ref().unwrap_or(&existing.run_config),
    ) {
        return Ok(*response);
    }

    let run_config = match req.run_config.as_ref() {
        Some(run_config) => {
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
                Err(response) => return Ok(*response),
            }
        }
        None => None,
    };

    let resolved = match resolve_patch_schedule_definition(&req) {
        Ok(value) => value,
        Err(response) => return Ok(*response),
    };

    match state
        .schedule_store
        .patch_schedule_with_definition(&id, name, req.enabled, run_config, resolved.definition)
        .await
    {
        Ok(Some(schedule)) => Ok(HttpResponse::Ok().json(ScheduleView::from(schedule))),
        Ok(None) => Ok(schedule_not_found(&id)),
        Err(error) => match error
            .get_ref()
            .and_then(|cause| cause.downcast_ref::<SchedulePatchRejection>())
        {
            Some(SchedulePatchRejection::NoFutureOccurrence) => {
                Ok(crate::error::json_error_with_code(
                    StatusCode::CONFLICT,
                    "Schedule has no future occurrence".into(),
                    Some("schedule_no_future_occurrence"),
                ))
            }
            Some(SchedulePatchRejection::InvalidWindow) => Ok(crate::error::json_error_with_code(
                StatusCode::BAD_REQUEST,
                "start_at must be earlier than end_at".into(),
                Some("invalid_schedule_window"),
            )),
            None => Err(internal_server_error("patch schedule", error)),
        },
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
    if let Err(response) = validate_workflow_target(&schedule.trigger, &schedule.run_config) {
        return Ok(*response);
    }
    if let Err(response) = validate_auto_execute_run_config(&state, &schedule.run_config).await {
        return Ok(*response);
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

/// `POST /api/v1/schedules/{schedule_id}/runs/{run_id}/cancel`
pub async fn cancel_schedule_run(
    state: web::Data<AppState>,
    path: web::Path<(String, String)>,
) -> HttpResponse {
    let (schedule_id, run_id) = path.into_inner();
    let store = &state.schedule_store;
    let lookup = || store.get_public_schedule_run_record(&schedule_id, &run_id);
    let not_found = || {
        crate::error::json_error_with_code(
            StatusCode::NOT_FOUND,
            "Schedule run not found".into(),
            Some("schedule_run_not_found"),
        )
    };
    let Some(record) = lookup().await else {
        return not_found();
    };
    if record.status.is_terminal() {
        return HttpResponse::Ok().json(ScheduleRunRecordView::from(record));
    }

    let requested = match record.session_id.as_deref() {
        Some(session_id)
            if record.status == ScheduleRunStatus::Running && record.workflow_run_id.is_none() =>
        {
            state
                .schedule_manager
                .request_ordinary_run_cancel(&schedule_id, &run_id, session_id)
                .await
        }
        _ => false,
    };
    if requested {
        return HttpResponse::Accepted().json(serde_json::json!({
            "schedule_id": schedule_id,
            "run_id": run_id,
            "status": "cancel_requested"
        }));
    }

    // The owner may have finished or the schedule may have been deleted
    // since the initial lookup. Report the current record without writing it.
    match lookup().await {
        None => not_found(),
        Some(record) if record.status.is_terminal() => {
            HttpResponse::Ok().json(ScheduleRunRecordView::from(record))
        }
        Some(_) => crate::error::json_error_with_code(
            StatusCode::CONFLICT,
            "Schedule run has no cancellable live execution".into(),
            Some("schedule_run_not_cancellable"),
        ),
    }
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
        use actix_web::{
            dev::{Service, ServiceResponse},
            http::StatusCode,
            test, web, App,
        };
        use serde_json::Value;
        use tempfile::tempdir;

        use crate::handlers::agent::schedules::ScheduleView;
        use crate::routes::configure_routes;
        use crate::schedule_app::store::ScheduleDefinitionChanges;
        use crate::schedule_app::{
            default_trigger_engine, ScheduleRunConfig, ScheduleRunStatus, ScheduleStore,
            ScheduleTrigger,
        };
        use crate::AppState;

        async fn assert_name_only_patch_is_persisted(
            app: &impl Service<
                actix_http::Request,
                Response = ServiceResponse,
                Error = actix_web::Error,
            >,
            store: &ScheduleStore,
            schedule_id: &str,
            name: &str,
        ) {
            let previous = store.get_schedule(schedule_id).await.unwrap();
            assert_ne!(previous.name, name);
            let records = store.list_run_records_for_schedule(schedule_id).await;
            let mut expected = serde_json::to_value(ScheduleView::from(previous)).unwrap();
            let uri = format!("/api/v1/schedules/{schedule_id}");
            let response = test::call_service(
                app,
                test::TestRequest::patch()
                    .uri(&uri)
                    .set_json(serde_json::json!({ "name": format!("  {name}\t") }))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            let renamed: Value = test::read_body_json(response).await;
            assert!(renamed["updated_at"].is_string());
            expected["name"] = serde_json::json!(name);
            expected["updated_at"] = renamed["updated_at"].clone();
            assert_eq!(renamed, expected);
            let persisted = tokio::fs::read(store.index_path()).await.unwrap();

            let repeated = test::call_service(
                app,
                test::TestRequest::patch()
                    .uri(&uri)
                    .set_json(serde_json::json!({ "name": name }))
                    .to_request(),
            )
            .await;
            assert_eq!(repeated.status(), StatusCode::OK);
            let repeated: Value = test::read_body_json(repeated).await;
            assert_eq!(repeated, renamed);

            let listed: Value = test::call_and_read_body_json(
                app,
                test::TestRequest::get()
                    .uri("/api/v1/schedules")
                    .to_request(),
            )
            .await;
            assert_eq!(listed["schedules"][0], renamed);
            assert_eq!(
                store.list_run_records_for_schedule(schedule_id).await,
                records
            );
            let home = store.index_path().parent().unwrap().to_path_buf();
            let reopened = ScheduleStore::new(home).await.unwrap();
            let entry = reopened.get_schedule(schedule_id).await.unwrap();
            assert_eq!(
                serde_json::to_value(ScheduleView::from(entry)).unwrap(),
                renamed
            );
            assert_eq!(
                reopened.list_run_records_for_schedule(schedule_id).await,
                records
            );
            assert_eq!(
                tokio::fs::read(store.index_path()).await.unwrap(),
                persisted
            );
        }

        #[actix_web::test]
        async fn patch_schedule_disables_and_resumes_an_upcoming_schedule() {
            let temp_dir = tempdir().unwrap();
            bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
            let state = web::Data::new(AppState::new(temp_dir.path().to_path_buf()).await.unwrap());
            let schedule = state
                .schedule_store
                .create_schedule(
                    "Upcoming schedule".into(),
                    ScheduleTrigger::Interval {
                        every_seconds: 3600,
                        anchor_at: Some(chrono::Utc::now() + chrono::Duration::hours(2)),
                    },
                    true,
                    ScheduleRunConfig::default(),
                )
                .await
                .unwrap();
            let app =
                test::init_service(App::new().app_data(state).configure(configure_routes)).await;
            let uri = format!("/api/v1/schedules/{}", schedule.id);

            for enabled in [false, true, true] {
                let response = test::call_service(
                    &app,
                    test::TestRequest::patch()
                        .uri(&uri)
                        .set_json(serde_json::json!({ "enabled": enabled }))
                        .to_request(),
                )
                .await;
                assert_eq!(response.status(), StatusCode::OK);
                let body: Value = test::read_body_json(response).await;
                assert_eq!(body["id"], schedule.id);
                assert_eq!(body["enabled"], enabled);
                assert_eq!(
                    body["state"]["next_fire_at"],
                    serde_json::json!(schedule.state.next_fire_at)
                );
                let reopened = ScheduleStore::new(temp_dir.path().to_path_buf())
                    .await
                    .unwrap();
                let persisted = reopened.get_schedule(&schedule.id).await.unwrap();
                assert_eq!(persisted.enabled, enabled);
                assert_eq!(persisted.state.next_fire_at, schedule.state.next_fire_at);
            }
        }

        #[actix_web::test]
        async fn patch_schedule_disables_but_cannot_resume_an_elapsed_cadence() {
            let temp_dir = tempdir().unwrap();
            let home = temp_dir.path().to_path_buf();
            let now = chrono::Utc::now();
            let start_at = now - chrono::Duration::hours(4);
            let end_at = now - chrono::Duration::hours(1);
            let schedule_id = {
                let store = ScheduleStore::new(home.clone()).await.unwrap();
                let schedule = store
                    .create_schedule(
                        "Elapsed cadence".into(),
                        ScheduleTrigger::Interval {
                            every_seconds: 3600,
                            anchor_at: Some(start_at),
                        },
                        true,
                        ScheduleRunConfig::default(),
                    )
                    .await
                    .unwrap();
                // An exhausted cadence is inert when the Host's first tick runs.
                let path = store.index_path();
                let mut index: Value =
                    serde_json::from_slice(&tokio::fs::read(path).await.unwrap()).unwrap();
                let entry = index["schedules"].get_mut(schedule.id.as_str()).unwrap();
                entry["enabled"] = serde_json::json!(false);
                entry["created_at"] = serde_json::json!(start_at);
                entry["updated_at"] = serde_json::json!(end_at);
                entry["start_at"] = serde_json::json!(start_at);
                entry["end_at"] = serde_json::json!(end_at);
                entry["state"]["next_fire_at"] = Value::Null;
                entry["state"]["last_scheduled_at"] = serde_json::json!(end_at);
                tokio::fs::write(path, serde_json::to_vec_pretty(&index).unwrap())
                    .await
                    .unwrap();
                schedule.id
            };

            let state = web::Data::new(AppState::new(home.clone()).await.unwrap());
            let store = &state.schedule_store;
            let before = store.get_schedule(&schedule_id).await.unwrap();
            assert!(!before.enabled);
            assert_eq!(before.state.next_fire_at, None);
            assert_eq!(before.state.last_scheduled_at, Some(end_at));
            let app = test::init_service(
                App::new()
                    .app_data(state.clone())
                    .configure(configure_routes),
            )
            .await;
            let uri = format!("/api/v1/schedules/{schedule_id}");
            let disable = test::call_service(
                &app,
                test::TestRequest::patch()
                    .uri(&uri)
                    .set_json(serde_json::json!({ "enabled": false }))
                    .to_request(),
            )
            .await;
            assert_eq!(disable.status(), StatusCode::OK);
            let disabled: Value = test::read_body_json(disable).await;
            let expected = serde_json::to_value(ScheduleView::from(before)).unwrap();
            assert_eq!(disabled, expected);
            let persisted = tokio::fs::read(store.index_path()).await.unwrap();

            let enable = test::call_service(
                &app,
                test::TestRequest::patch()
                    .uri(&uri)
                    .set_json(serde_json::json!({ "enabled": true }))
                    .to_request(),
            )
            .await;
            assert_eq!(enable.status(), StatusCode::CONFLICT);
            let error: Value = test::read_body_json(enable).await;
            assert_eq!(error["error"]["code"], "schedule_no_future_occurrence");

            let listed: Value = test::call_and_read_body_json(
                &app,
                test::TestRequest::get()
                    .uri("/api/v1/schedules")
                    .to_request(),
            )
            .await;
            assert_eq!(listed["schedules"][0], disabled);
            let reopened = ScheduleStore::new(home).await.unwrap();
            let entry = reopened.get_schedule(&schedule_id).await.unwrap();
            assert_eq!(
                serde_json::to_value(ScheduleView::from(entry)).unwrap(),
                disabled
            );
            assert_eq!(
                tokio::fs::read(store.index_path()).await.unwrap(),
                persisted
            );
            assert_name_only_patch_is_persisted(&app, store, &schedule_id, "Archived cadence")
                .await;
        }

        #[actix_web::test]
        async fn patch_schedule_cannot_reenable_a_consumed_once_schedule() {
            let temp_dir = tempdir().unwrap();
            bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
            let state = web::Data::new(AppState::new(temp_dir.path().to_path_buf()).await.unwrap());
            let store = &state.schedule_store;
            let at = chrono::Utc::now() + chrono::Duration::hours(1);
            let schedule = store
                .create_schedule(
                    "Once schedule".into(),
                    ScheduleTrigger::Once { at },
                    true,
                    ScheduleRunConfig {
                        task_message: Some("Review the saved report".into()),
                        model: Some("saved-model".into()),
                        enhance_prompt: Some("Retain original evidence".into()),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let claimed = store
                .claim_due_runs_with_engine(at, default_trigger_engine().as_ref())
                .await
                .unwrap();
            assert_eq!(claimed.len(), 1);
            assert_eq!(claimed[0].schedule_id, schedule.id);
            assert!(store
                .get_schedule(&schedule.id)
                .await
                .unwrap()
                .state
                .next_fire_at
                .is_none());
            let app = test::init_service(
                App::new()
                    .app_data(state.clone())
                    .configure(configure_routes),
            )
            .await;
            let uri = format!("/api/v1/schedules/{}", schedule.id);
            let disabled = test::call_service(
                &app,
                test::TestRequest::patch()
                    .uri(&uri)
                    .set_json(serde_json::json!({ "enabled": false }))
                    .to_request(),
            )
            .await;
            assert_eq!(disabled.status(), StatusCode::OK);
            let disabled_view: Value = test::read_body_json(disabled).await;
            assert_eq!(disabled_view["enabled"], false);
            assert!(disabled_view["state"]["next_fire_at"].is_null());
            let persisted = tokio::fs::read(store.index_path()).await.unwrap();

            let enable = test::call_service(
                &app,
                test::TestRequest::patch()
                    .uri(&uri)
                    .set_json(serde_json::json!({ "enabled": true }))
                    .to_request(),
            )
            .await;
            assert_eq!(enable.status(), StatusCode::CONFLICT);
            let error: Value = test::read_body_json(enable).await;
            assert_eq!(error["error"]["code"], "schedule_no_future_occurrence");
            let listed: Value = test::call_and_read_body_json(
                &app,
                test::TestRequest::get()
                    .uri("/api/v1/schedules")
                    .to_request(),
            )
            .await;
            assert_eq!(listed["schedules"][0], disabled_view);
            let reopened = ScheduleStore::new(temp_dir.path().to_path_buf())
                .await
                .unwrap();
            let entry = reopened.get_schedule(&schedule.id).await.unwrap();
            assert_eq!(
                serde_json::to_value(ScheduleView::from(entry)).unwrap(),
                disabled_view
            );
            assert_eq!(
                tokio::fs::read(store.index_path()).await.unwrap(),
                persisted
            );
            assert_name_only_patch_is_persisted(&app, store, &schedule.id, "Archived one-shot")
                .await;
        }

        #[actix_web::test]
        async fn patch_schedule_rejects_an_invalid_merged_window_without_changes() {
            let temp_dir = tempdir().unwrap();
            bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
            let state = web::Data::new(AppState::new(temp_dir.path().to_path_buf()).await.unwrap());
            let store = &state.schedule_store;
            let start_at = chrono::Utc::now() + chrono::Duration::hours(4);
            let schedule = store
                .create_schedule_with_definition(
                    "Original schedule".into(),
                    true,
                    ScheduleRunConfig::default(),
                    ScheduleDefinitionChanges {
                        trigger: Some(ScheduleTrigger::Interval {
                            every_seconds: 3600,
                            anchor_at: None,
                        }),
                        start_at: Some(start_at),
                        end_at: Some(start_at + chrono::Duration::hours(2)),
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
            let persisted = tokio::fs::read(store.index_path()).await.unwrap();

            let rejected = test::call_service(
                &app,
                test::TestRequest::patch()
                    .uri(&format!("/api/v1/schedules/{}", schedule.id))
                    .set_json(serde_json::json!({
                        "end_at": start_at,
                        "enabled": false,
                        "name": "Rejected schedule"
                    }))
                    .to_request(),
            )
            .await;
            assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
            let error: Value = test::read_body_json(rejected).await;
            assert_eq!(error["error"]["code"], "invalid_schedule_window");
            assert_eq!(
                error["error"]["message"],
                "start_at must be earlier than end_at"
            );
            let listed: Value = test::call_and_read_body_json(
                &app,
                test::TestRequest::get()
                    .uri("/api/v1/schedules")
                    .to_request(),
            )
            .await;
            let reopened = ScheduleStore::new(temp_dir.path().to_path_buf())
                .await
                .unwrap();
            let entry = reopened.get_schedule(&schedule.id).await.unwrap();
            let expected = serde_json::to_value(ScheduleView::from(schedule)).unwrap();
            assert_eq!(listed["schedules"][0], expected);
            assert_eq!(
                serde_json::to_value(ScheduleView::from(entry)).unwrap(),
                expected
            );
            assert_eq!(
                tokio::fs::read(store.index_path()).await.unwrap(),
                persisted
            );
        }

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
        async fn cancel_schedule_run_reports_recorded_outcomes_and_rejects_unowned_runs() {
            let temp_dir = tempdir().expect("tempdir");
            bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
            let state = web::Data::new(
                AppState::new(temp_dir.path().to_path_buf())
                    .await
                    .expect("app state"),
            );
            let store = &state.schedule_store;
            let schedule = store
                .create_schedule(
                    "Recurring task".into(),
                    ScheduleTrigger::Interval {
                        every_seconds: 3600,
                        anchor_at: None,
                    },
                    true,
                    ScheduleRunConfig {
                        auto_execute: true,
                        task_message: Some("fixture task".into()),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let workflow = store
                .create_schedule(
                    "Workflow".into(),
                    ScheduleTrigger::Once {
                        at: chrono::Utc::now() + chrono::Duration::hours(1),
                    },
                    true,
                    ScheduleRunConfig {
                        auto_execute: true,
                        workflow_target: Some(bamboo_domain::ScheduledWorkflowTarget {
                            workflow_id: "fixture-workflow".into(),
                            revision: 1,
                            args: serde_json::json!({}),
                        }),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let queued = store.create_run_now(&schedule.id).await.unwrap().unwrap();
            let unbound = store.create_run_now(&schedule.id).await.unwrap().unwrap();
            store
                .mark_run_started(&schedule.id, &unbound.run_id)
                .await
                .unwrap();
            let stale = store.create_run_now(&schedule.id).await.unwrap().unwrap();
            store
                .mark_run_started(&schedule.id, &stale.run_id)
                .await
                .unwrap();
            store
                .bind_run_session(&schedule.id, &stale.run_id, "prior-host-session")
                .await
                .unwrap();
            let workflow_run = store.create_run_now(&workflow.id).await.unwrap().unwrap();
            assert!(store
                .start_workflow_occurrence(&workflow.id, &workflow_run.run_id)
                .await
                .unwrap());
            store
                .bind_run_session(&workflow.id, &workflow_run.run_id, "workflow-session")
                .await
                .unwrap();
            store
                .bind_run_workflow(&workflow.id, &workflow_run.run_id, "workflow-execution")
                .await
                .unwrap();

            let app = test::init_service(
                App::new()
                    .app_data(state.clone())
                    .configure(configure_routes),
            )
            .await;
            let cancel = |schedule_id: &str, run_id: &str| {
                test::TestRequest::post()
                    .uri(&format!(
                        "/api/v1/schedules/{schedule_id}/runs/{run_id}/cancel"
                    ))
                    .to_request()
            };
            for (schedule_id, run_id, expected, code) in [
                (
                    "missing",
                    queued.run_id.as_str(),
                    StatusCode::NOT_FOUND,
                    "schedule_run_not_found",
                ),
                (
                    workflow.id.as_str(),
                    queued.run_id.as_str(),
                    StatusCode::NOT_FOUND,
                    "schedule_run_not_found",
                ),
                (
                    schedule.id.as_str(),
                    "missing",
                    StatusCode::NOT_FOUND,
                    "schedule_run_not_found",
                ),
                (
                    schedule.id.as_str(),
                    queued.run_id.as_str(),
                    StatusCode::CONFLICT,
                    "schedule_run_not_cancellable",
                ),
                (
                    schedule.id.as_str(),
                    unbound.run_id.as_str(),
                    StatusCode::CONFLICT,
                    "schedule_run_not_cancellable",
                ),
                (
                    schedule.id.as_str(),
                    stale.run_id.as_str(),
                    StatusCode::CONFLICT,
                    "schedule_run_not_cancellable",
                ),
                (
                    workflow.id.as_str(),
                    workflow_run.run_id.as_str(),
                    StatusCode::CONFLICT,
                    "schedule_run_not_cancellable",
                ),
            ] {
                let record_before = store.get_run_record(run_id).await;
                let response = test::call_service(&app, cancel(schedule_id, run_id)).await;
                assert_eq!(response.status(), expected, "{schedule_id}/{run_id}");
                let body: Value = test::read_body_json(response).await;
                assert_eq!(body["error"]["type"], "api_error");
                assert_eq!(body["error"]["code"], code);
                assert_eq!(store.get_run_record(run_id).await, record_before);
            }

            for (status, wire_status) in [
                (ScheduleRunStatus::Success, "success"),
                (ScheduleRunStatus::Cancelled, "cancelled"),
            ] {
                let run = store.create_run_now(&schedule.id).await.unwrap().unwrap();
                store
                    .mark_run_started(&schedule.id, &run.run_id)
                    .await
                    .unwrap();
                store
                    .mark_run_terminal(
                        &schedule.id,
                        &run.run_id,
                        status,
                        Some("fixture outcome".into()),
                    )
                    .await
                    .unwrap();
                let recorded = store.get_run_record(&run.run_id).await.unwrap();
                for _ in 0..2 {
                    let response =
                        test::call_service(&app, cancel(&schedule.id, &run.run_id)).await;
                    assert_eq!(response.status(), StatusCode::OK);
                    let body: Value = test::read_body_json(response).await;
                    assert_eq!(body["schedule_id"], schedule.id);
                    assert_eq!(body["run_id"], run.run_id);
                    assert_eq!(body["status"], wire_status);
                    assert_eq!(body["outcome_reason"], "fixture outcome");
                    assert_eq!(
                        body["completed_at"],
                        serde_json::to_value(recorded.completed_at).unwrap()
                    );
                }
                assert_eq!(store.get_run_record(&run.run_id).await, Some(recorded));
            }
            let retained = store.get_schedule(&schedule.id).await.unwrap();
            assert!(retained.enabled);
            assert_eq!(retained.state.next_fire_at, schedule.state.next_fire_at);
        }

        #[actix_web::test]
        async fn create_schedule_validates_but_does_not_pin_project_default_workspace() {
            let temp_dir = tempdir().expect("tempdir");
            bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
            let project_path = tempdir().expect("Project path");
            let foreign_default = tempdir().expect("foreign global default");
            let state = web::Data::new(
                AppState::new(temp_dir.path().to_path_buf())
                    .await
                    .expect("app state"),
            );
            let project = state
                .project_store
                .create_with_project_path(
                    "Scheduled",
                    None,
                    project_path.path().to_string_lossy(),
                    Vec::new(),
                )
                .expect("configured Project");
            state.config.write().await.default_work_area =
                Some(bamboo_config::DefaultWorkAreaConfig {
                    path: Some(foreign_default.path().to_string_lossy().into_owned()),
                });
            let app = test::init_service(
                App::new()
                    .app_data(state.clone())
                    .configure(configure_routes),
            )
            .await;

            let response = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri("/api/v1/schedules")
                    .set_json(serde_json::json!({
                        "name": "Project default",
                        "trigger": {"type": "interval", "every_seconds": 3600},
                        "run_config": {
                            "project_id": project.id,
                            "auto_execute": false
                        }
                    }))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            let schedules = state.schedule_store.list_schedules().await;
            assert_eq!(schedules.len(), 1);
            assert_eq!(
                schedules[0].run_config.project_id.as_ref(),
                Some(&project.id)
            );
            assert!(
                schedules[0].run_config.workspace_path.is_none(),
                "omission must remain a live Project-default selection"
            );
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
            let project_path = tempdir().expect("Project path");
            let project = state
                .project_store
                .create_with_project_path(
                    "Scheduled",
                    None,
                    project_path.path().to_string_lossy(),
                    Vec::new(),
                )
                .unwrap();
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
            let project_path = tempdir().expect("Project path");
            let state = web::Data::new(
                AppState::new(temp_dir.path().to_path_buf())
                    .await
                    .expect("app state"),
            );
            let project = state
                .project_store
                .create_with_project_path(
                    "Scheduled",
                    None,
                    project_path.path().to_string_lossy(),
                    Vec::new(),
                )
                .unwrap();
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
            assert!(persisted.run_config.workspace_path.is_none());
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
