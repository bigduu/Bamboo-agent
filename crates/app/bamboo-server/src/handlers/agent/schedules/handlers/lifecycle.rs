use actix_web::{http::StatusCode, web, HttpResponse, Result};
use chrono::{DateTime, Utc};

use crate::app_state::AppState;
use crate::schedule_app::store::{ScheduleClockKind, SchedulePatchRejection};
use crate::schedule_app::{ScheduleRunJob, ScheduleRunStatus};

use super::super::types::{
    CreateScheduleRequest, PatchScheduleRequest, RenameScheduleRequest, ScheduleRunRecordView,
    ScheduleView, SetScheduleClockTimeRequest, SetScheduleDailyTimeRequest,
    SetScheduleIntervalRequest, SetScheduleOnceAtRequest, SetSchedulePromptRequest,
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
            Some(SchedulePatchRejection::NoFutureOccurrence) => Ok(schedule_no_future_occurrence()),
            Some(SchedulePatchRejection::InvalidWindow) => Ok(crate::error::json_error_with_code(
                StatusCode::BAD_REQUEST,
                "start_at must be earlier than end_at".into(),
                Some("invalid_schedule_window"),
            )),
            _ => Err(internal_server_error("patch schedule", error)),
        },
    }
}

fn schedule_no_future_occurrence() -> HttpResponse {
    crate::error::json_error_with_code(
        StatusCode::CONFLICT,
        "Schedule has no future occurrence".into(),
        Some("schedule_no_future_occurrence"),
    )
}

/// `PATCH /api/v1/schedules/{schedule_id}/name`
pub async fn rename_schedule(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: Result<web::Json<RenameScheduleRequest>>,
) -> Result<HttpResponse> {
    let name = match req {
        Ok(req) => {
            let RenameScheduleRequest::Title { name } = req.into_inner();
            normalize_optional_name(Some(&name))
        }
        Err(error) if error.as_response_error().status_code() == StatusCode::BAD_REQUEST => None,
        Err(error) => return Err(error),
    };
    let Some(name) = name else {
        return Ok(crate::error::json_error_with_code(
            StatusCode::BAD_REQUEST,
            "name is required".into(),
            Some("invalid_schedule_name"),
        ));
    };

    let id = path.into_inner();
    let updated = state
        .schedule_store
        .patch_schedule(&id, Some(name), None, None, None)
        .await
        .map_err(|error| internal_server_error("rename schedule", error))?;
    match updated {
        Some(schedule) => Ok(HttpResponse::Ok().json(ScheduleView::from(schedule))),
        None => Ok(crate::error::json_error_with_code(
            StatusCode::NOT_FOUND,
            "Schedule not found".into(),
            Some("schedule_not_found"),
        )),
    }
}

fn invalid_schedule_interval() -> HttpResponse {
    crate::error::json_error_with_code(
        StatusCode::BAD_REQUEST,
        "Interval must be a positive, representable number of seconds".into(),
        Some("invalid_schedule_interval"),
    )
}

/// `PATCH /api/v1/schedules/{schedule_id}/interval`
pub async fn set_schedule_interval(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: Result<web::Json<SetScheduleIntervalRequest>>,
) -> Result<HttpResponse> {
    let SetScheduleIntervalRequest::Period { every_seconds } = match req {
        Ok(req) => req.into_inner(),
        Err(error) if error.as_response_error().status_code() == StatusCode::BAD_REQUEST => {
            return Ok(invalid_schedule_interval());
        }
        Err(error) => return Err(error),
    };
    match state
        .schedule_store
        .patch_interval_period(&path, every_seconds)
        .await
    {
        Ok(Some(schedule)) => Ok(HttpResponse::Ok().json(ScheduleView::from(schedule))),
        Ok(None) => Ok(crate::error::json_error_with_code(
            StatusCode::NOT_FOUND,
            "Schedule not found".into(),
            Some("schedule_not_found"),
        )),
        Err(error) => match error
            .get_ref()
            .and_then(|cause| cause.downcast_ref::<SchedulePatchRejection>())
        {
            Some(SchedulePatchRejection::InvalidInterval) => Ok(invalid_schedule_interval()),
            Some(SchedulePatchRejection::TriggerChanged) => Ok(crate::error::json_error_with_code(
                StatusCode::CONFLICT,
                "Schedule no longer uses an Interval trigger".into(),
                Some("schedule_trigger_changed"),
            )),
            Some(
                SchedulePatchRejection::NoFutureOccurrence | SchedulePatchRejection::InvalidWindow,
            ) => Ok(schedule_no_future_occurrence()),
            _ => Err(internal_server_error("change schedule interval", error)),
        },
    }
}

fn invalid_schedule_once_at() -> HttpResponse {
    crate::error::json_error_with_code(
        StatusCode::BAD_REQUEST,
        "Enter an RFC3339 timestamp with an offset".into(),
        Some("invalid_schedule_once_at"),
    )
}

/// `PATCH /api/v1/schedules/{schedule_id}/once-at`
pub async fn set_schedule_once_at(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: Result<web::Json<SetScheduleOnceAtRequest>>,
) -> Result<HttpResponse> {
    let SetScheduleOnceAtRequest::Instant { at } = match req {
        Ok(req) => req.into_inner(),
        Err(error) if error.as_response_error().status_code() == StatusCode::BAD_REQUEST => {
            return Ok(invalid_schedule_once_at());
        }
        Err(error) => return Err(error),
    };
    let Ok(at) = DateTime::parse_from_rfc3339(&at) else {
        return Ok(invalid_schedule_once_at());
    };
    match state
        .schedule_store
        .patch_once_at(&path, at.with_timezone(&Utc))
        .await
    {
        Ok(Some(schedule)) => Ok(HttpResponse::Ok().json(ScheduleView::from(schedule))),
        Ok(None) => Ok(crate::error::json_error_with_code(
            StatusCode::NOT_FOUND,
            "Schedule not found".into(),
            Some("schedule_not_found"),
        )),
        Err(error) => match error
            .get_ref()
            .and_then(|cause| cause.downcast_ref::<SchedulePatchRejection>())
        {
            Some(SchedulePatchRejection::TriggerChanged) => Ok(crate::error::json_error_with_code(
                StatusCode::CONFLICT,
                "Schedule no longer uses an editable Once trigger".into(),
                Some("schedule_trigger_changed"),
            )),
            Some(SchedulePatchRejection::OnceNotPending) => Ok(crate::error::json_error_with_code(
                StatusCode::CONFLICT,
                "One-time occurrence is no longer pending".into(),
                Some("schedule_once_not_pending"),
            )),
            Some(SchedulePatchRejection::NoFutureOccurrence) => Ok(schedule_no_future_occurrence()),
            _ => Err(internal_server_error(
                "change schedule once timestamp",
                error,
            )),
        },
    }
}

/// `PATCH /api/v1/schedules/{schedule_id}/daily-time`
pub async fn set_schedule_daily_time(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: Result<web::Json<SetScheduleDailyTimeRequest>>,
) -> Result<HttpResponse> {
    set_schedule_clock_time(state, path, req, ScheduleClockKind::Daily).await
}

/// `PATCH /api/v1/schedules/{schedule_id}/weekly-time`
pub async fn set_schedule_weekly_time(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: Result<web::Json<SetScheduleClockTimeRequest>>,
) -> Result<HttpResponse> {
    set_schedule_clock_time(state, path, req, ScheduleClockKind::Weekly).await
}

/// `PATCH /api/v1/schedules/{schedule_id}/monthly-time`
pub async fn set_schedule_monthly_time(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: Result<web::Json<SetScheduleClockTimeRequest>>,
) -> Result<HttpResponse> {
    set_schedule_clock_time(state, path, req, ScheduleClockKind::Monthly).await
}

async fn set_schedule_clock_time(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: Result<web::Json<SetScheduleClockTimeRequest>>,
    kind: ScheduleClockKind,
) -> Result<HttpResponse> {
    let (invalid_message, invalid_code, changed_message, operation) = match kind {
        ScheduleClockKind::Daily => (
            "Enter a valid daily time (HH:MM[:SS])",
            "invalid_schedule_daily_time",
            "Schedule no longer uses an editable Daily trigger",
            "change schedule daily time",
        ),
        ScheduleClockKind::Weekly => (
            "Enter a valid weekly time (HH:MM[:SS])",
            "invalid_schedule_weekly_time",
            "Schedule no longer uses an editable Weekly trigger",
            "change schedule weekly time",
        ),
        ScheduleClockKind::Monthly => (
            "Enter a valid monthly time (HH:MM[:SS])",
            "invalid_schedule_monthly_time",
            "Schedule no longer uses an editable Monthly trigger",
            "change schedule monthly time",
        ),
    };
    let invalid_time = || {
        crate::error::json_error_with_code(
            StatusCode::BAD_REQUEST,
            invalid_message.into(),
            Some(invalid_code),
        )
    };
    let SetScheduleClockTimeRequest::Clock {
        hour,
        minute,
        second,
    } = match req {
        Ok(req) => req.into_inner(),
        Err(error) if error.as_response_error().status_code() == StatusCode::BAD_REQUEST => {
            return Ok(invalid_time());
        }
        Err(error) => return Err(error),
    };
    match state
        .schedule_store
        .patch_calendar_time(&path, kind, hour, minute, second)
        .await
    {
        Ok(Some(schedule)) => Ok(HttpResponse::Ok().json(ScheduleView::from(schedule))),
        Ok(None) => Ok(crate::error::json_error_with_code(
            StatusCode::NOT_FOUND,
            "Schedule not found".into(),
            Some("schedule_not_found"),
        )),
        Err(error) => match error
            .get_ref()
            .and_then(|cause| cause.downcast_ref::<SchedulePatchRejection>())
        {
            Some(SchedulePatchRejection::InvalidClockTime) => Ok(invalid_time()),
            Some(SchedulePatchRejection::TriggerChanged) => Ok(crate::error::json_error_with_code(
                StatusCode::CONFLICT,
                changed_message.into(),
                Some("schedule_trigger_changed"),
            )),
            Some(SchedulePatchRejection::NoFutureOccurrence) => Ok(schedule_no_future_occurrence()),
            _ => Err(internal_server_error(operation, error)),
        },
    }
}

fn invalid_schedule_prompt() -> HttpResponse {
    crate::error::json_error_with_code(
        StatusCode::BAD_REQUEST,
        "task_message is required".into(),
        Some("invalid_schedule_prompt"),
    )
}

/// `PATCH /api/v1/schedules/{schedule_id}/prompt`
pub async fn set_schedule_prompt(
    state: web::Data<AppState>,
    path: web::Path<String>,
    req: Result<web::Json<SetSchedulePromptRequest>>,
) -> Result<HttpResponse> {
    let SetSchedulePromptRequest::Prompt { task_message } = match req {
        Ok(req) => req.into_inner(),
        Err(error) if error.as_response_error().status_code() == StatusCode::BAD_REQUEST => {
            return Ok(invalid_schedule_prompt());
        }
        Err(error) => return Err(error),
    };
    match state
        .schedule_store
        .patch_task_prompt(&path, task_message)
        .await
    {
        Ok(Some(schedule)) => Ok(HttpResponse::Ok().json(ScheduleView::from(schedule))),
        Ok(None) => Ok(crate::error::json_error_with_code(
            StatusCode::NOT_FOUND,
            "Schedule not found".into(),
            Some("schedule_not_found"),
        )),
        Err(error) => match error
            .get_ref()
            .and_then(|cause| cause.downcast_ref::<SchedulePatchRejection>())
        {
            Some(SchedulePatchRejection::InvalidPrompt) => Ok(invalid_schedule_prompt()),
            Some(SchedulePatchRejection::PromptChanged) => Ok(crate::error::json_error_with_code(
                StatusCode::CONFLICT,
                "Schedule no longer has an editable task prompt".into(),
                Some("schedule_prompt_changed"),
            )),
            _ => Err(internal_server_error("change schedule prompt", error)),
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
            default_trigger_engine, MisFirePolicy, OverlapPolicy, ScheduleRunConfig,
            ScheduleRunStatus, ScheduleStore, ScheduleTrigger,
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
        async fn rename_schedule_route_changes_only_the_exact_saved_title() {
            let temp_dir = tempdir().unwrap();
            bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
            let state = web::Data::new(AppState::new(temp_dir.path().to_path_buf()).await.unwrap());
            let store = &state.schedule_store;
            let at = chrono::Utc::now() + chrono::Duration::hours(2);
            let trigger = ScheduleTrigger::Once { at };
            let config = ScheduleRunConfig {
                task_message: Some("Prepare the saved schedule report".into()),
                ..Default::default()
            };
            let schedule = store
                .create_schedule_with_definition(
                    "Saved title".into(),
                    false,
                    config.clone(),
                    ScheduleDefinitionChanges {
                        trigger: Some(trigger.clone()),
                        start_at: Some(at - chrono::Duration::minutes(5)),
                        end_at: Some(at + chrono::Duration::minutes(5)),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let sibling = store
                .create_schedule(schedule.name.clone(), trigger, false, config)
                .await
                .unwrap();
            let app = test::init_service(
                App::new()
                    .app_data(state.clone())
                    .configure(configure_routes),
            )
            .await;
            let uri = format!("/api/v1/schedules/{}/name", schedule.id);
            let saved = tokio::fs::read(store.index_path()).await.unwrap();

            for payload in [
                serde_json::json!({}),
                serde_json::json!({ "name": null }),
                serde_json::json!({ "name": 1 }),
                serde_json::json!({ "name": " \t " }),
                serde_json::json!({ "name": "Mixed update", "enabled": true }),
                serde_json::json!(["Positional title"]),
            ] {
                let response = test::call_service(
                    &app,
                    test::TestRequest::patch()
                        .uri(&uri)
                        .set_json(payload)
                        .to_request(),
                )
                .await;
                assert_eq!(response.status(), StatusCode::BAD_REQUEST);
                let body: Value = test::read_body_json(response).await;
                assert_eq!(body["error"]["type"], "api_error");
                assert_eq!(body["error"]["code"], "invalid_schedule_name");
            }

            let missing = test::call_service(
                &app,
                test::TestRequest::patch()
                    .uri("/api/v1/schedules/unknown-schedule/name")
                    .set_json(serde_json::json!({ "name": "Valid title" }))
                    .to_request(),
            )
            .await;
            assert_eq!(missing.status(), StatusCode::NOT_FOUND);
            let body: Value = test::read_body_json(missing).await;
            assert_eq!(body["error"]["type"], "api_error");
            assert_eq!(body["error"]["code"], "schedule_not_found");
            assert_eq!(body["error"]["message"], "Schedule not found");
            assert_eq!(
                store.get_schedule(&schedule.id).await,
                Some(schedule.clone())
            );
            assert_eq!(tokio::fs::read(store.index_path()).await.unwrap(), saved);

            let response = test::call_service(
                &app,
                test::TestRequest::patch()
                    .uri(&uri)
                    .set_json(serde_json::json!({ "name": "  新しい名前  \t" }))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            let renamed: Value = test::read_body_json(response).await;
            assert!(renamed["updated_at"].is_string());
            let mut expected = serde_json::to_value(ScheduleView::from(schedule)).unwrap();
            expected["name"] = serde_json::json!("新しい名前");
            expected["updated_at"] = renamed["updated_at"].clone();
            assert_eq!(renamed, expected);

            let home = store.index_path().parent().unwrap().to_path_buf();
            let reopened = ScheduleStore::new(home).await.unwrap();
            let renamed_entry = reopened
                .get_schedule(renamed["id"].as_str().unwrap())
                .await
                .unwrap();
            assert_eq!(
                serde_json::to_value(ScheduleView::from(renamed_entry)).unwrap(),
                renamed
            );
            assert_eq!(reopened.get_schedule(&sibling.id).await, Some(sibling));
        }

        #[actix_web::test]
        async fn set_schedule_interval_preserves_saved_fields_and_queued_run() {
            let temp_dir = tempdir().unwrap();
            bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
            let state = web::Data::new(AppState::new(temp_dir.path().to_path_buf()).await.unwrap());
            let store = &state.schedule_store;
            let anchor = chrono::Utc::now() - chrono::Duration::hours(1);
            let occurrence = anchor + chrono::Duration::hours(2);
            let schedule = store
                .create_schedule_with_definition(
                    "Recurring report".into(),
                    true,
                    ScheduleRunConfig {
                        task_message: Some("Keep the saved prompt".into()),
                        ..Default::default()
                    },
                    ScheduleDefinitionChanges {
                        trigger: Some(ScheduleTrigger::legacy_interval(7200, Some(anchor))),
                        timezone: Some("Asia/Shanghai".into()),
                        start_at: Some(anchor),
                        end_at: Some(anchor + chrono::Duration::hours(8)),
                        misfire_policy: Some(MisFirePolicy::CatchUpWindow {
                            max_catch_up_runs: 3,
                            max_lateness_seconds: 60,
                        }),
                        overlap_policy: Some(OverlapPolicy::Allow),
                    },
                )
                .await
                .unwrap();
            assert_eq!(schedule.state.next_fire_at, Some(occurrence));
            // Claim through the real store at the saved instant without dispatching a job.
            let claimed = store.claim_due_runs(occurrence).await.unwrap();
            assert_eq!(claimed.len(), 1);
            assert_eq!(claimed[0].scheduled_for, occurrence);
            let before = store
                .patch_schedule(&schedule.id, None, Some(false), None, None)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(before.state.queued_run_count, 1);
            assert_eq!(before.state.last_scheduled_at, Some(occurrence));
            let records = store.list_run_records_for_schedule(&schedule.id).await;
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].status, ScheduleRunStatus::Queued);
            assert_eq!(records[0].run_id, claimed[0].run_id);

            let app = test::init_service(
                App::new()
                    .app_data(state.clone())
                    .configure(configure_routes),
            )
            .await;
            let response = test::call_service(
                &app,
                test::TestRequest::patch()
                    .uri(&format!("/api/v1/schedules/{}/interval", schedule.id))
                    .set_json(serde_json::json!({ "every_seconds": 1800 }))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            let edited: Value = test::read_body_json(response).await;
            let mut expected = serde_json::to_value(ScheduleView::from(before)).unwrap();
            expected["trigger"]["every_seconds"] = serde_json::json!(1800);
            expected["state"]["next_fire_at"] =
                serde_json::json!(occurrence + chrono::Duration::seconds(1800));
            assert!(edited["updated_at"].is_string());
            expected["updated_at"] = edited["updated_at"].clone();
            assert_eq!(edited, expected);

            let reopened = ScheduleStore::new(store.index_path().parent().unwrap().to_path_buf())
                .await
                .unwrap();
            assert_eq!(
                store.list_run_records_for_schedule(&schedule.id).await,
                records
            );
            assert_eq!(
                reopened.list_run_records_for_schedule(&schedule.id).await,
                records
            );
            let entry = reopened.get_schedule(&schedule.id).await.unwrap();
            assert_eq!(
                serde_json::to_value(ScheduleView::from(entry)).unwrap(),
                edited
            );
        }

        #[actix_web::test]
        async fn set_schedule_interval_keeps_exhausted_and_incompatible_rows_intact() {
            let temp_dir = tempdir().unwrap();
            bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
            let state = web::Data::new(AppState::new(temp_dir.path().to_path_buf()).await.unwrap());
            let store = &state.schedule_store;
            let occurrence = chrono::Utc::now() + chrono::Duration::hours(2);
            let schedule = store
                .create_schedule_with_definition(
                    "Finite interval".into(),
                    true,
                    ScheduleRunConfig::default(),
                    ScheduleDefinitionChanges {
                        trigger: Some(ScheduleTrigger::legacy_interval(900, Some(occurrence))),
                        end_at: Some(occurrence + chrono::Duration::seconds(30)),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let claimed = store.claim_due_runs(occurrence).await.unwrap();
            assert_eq!(claimed.len(), 1);
            let exhausted = store.get_schedule(&schedule.id).await.unwrap();
            assert!(!exhausted.enabled);
            assert_eq!(exhausted.state.next_fire_at, None);
            let once = store
                .create_schedule(
                    "One shot".into(),
                    ScheduleTrigger::Once { at: occurrence },
                    false,
                    ScheduleRunConfig::default(),
                )
                .await
                .unwrap();
            let app = test::init_service(
                App::new()
                    .app_data(state.clone())
                    .configure(configure_routes),
            )
            .await;
            let saved = tokio::fs::read(store.index_path()).await.unwrap();
            let cases = [
                (
                    &schedule.id,
                    serde_json::json!({}),
                    StatusCode::BAD_REQUEST,
                    "invalid_schedule_interval",
                ),
                (
                    &schedule.id,
                    serde_json::json!({"every_seconds": 0}),
                    StatusCode::BAD_REQUEST,
                    "invalid_schedule_interval",
                ),
                (
                    &schedule.id,
                    serde_json::json!({"every_seconds": u64::MAX}),
                    StatusCode::BAD_REQUEST,
                    "invalid_schedule_interval",
                ),
                (
                    &schedule.id,
                    serde_json::json!({"every_seconds": 30, "enabled": true}),
                    StatusCode::BAD_REQUEST,
                    "invalid_schedule_interval",
                ),
                (
                    &schedule.id,
                    serde_json::json!({"every_seconds": 1800}),
                    StatusCode::CONFLICT,
                    "schedule_no_future_occurrence",
                ),
                (
                    &once.id,
                    serde_json::json!({"every_seconds": 30}),
                    StatusCode::CONFLICT,
                    "schedule_trigger_changed",
                ),
            ];
            for (id, payload, status, code) in cases {
                let response = test::call_service(
                    &app,
                    test::TestRequest::patch()
                        .uri(&format!("/api/v1/schedules/{id}/interval"))
                        .set_json(payload)
                        .to_request(),
                )
                .await;
                assert_eq!(response.status(), status);
                let body: Value = test::read_body_json(response).await;
                assert_eq!(body["error"]["type"], "api_error");
                assert_eq!(body["error"]["code"], code);
            }
            let missing = test::call_service(
                &app,
                test::TestRequest::patch()
                    .uri("/api/v1/schedules/missing/interval")
                    .set_json(serde_json::json!({"every_seconds": 30}))
                    .to_request(),
            )
            .await;
            assert_eq!(missing.status(), StatusCode::NOT_FOUND);
            let body: Value = test::read_body_json(missing).await;
            assert_eq!(body["error"]["code"], "schedule_not_found");

            let repeated = test::call_service(
                &app,
                test::TestRequest::patch()
                    .uri(&format!("/api/v1/schedules/{}/interval", schedule.id))
                    .set_json(serde_json::json!({"every_seconds": 900}))
                    .to_request(),
            )
            .await;
            assert_eq!(repeated.status(), StatusCode::OK);
            let repeated: Value = test::read_body_json(repeated).await;
            assert_eq!(
                repeated,
                serde_json::to_value(ScheduleView::from(exhausted.clone())).unwrap()
            );
            assert_eq!(store.get_schedule(&schedule.id).await, Some(exhausted));
            assert_eq!(store.get_schedule(&once.id).await, Some(once));
            assert_eq!(tokio::fs::read(store.index_path()).await.unwrap(), saved);
        }

        #[actix_web::test]
        async fn set_schedule_daily_time_preserves_saved_timezone_and_admitted_run() {
            use chrono::{Duration, Utc};

            let temp_dir = tempdir().unwrap();
            bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
            let state = web::Data::new(AppState::new(temp_dir.path().to_path_buf()).await.unwrap());
            let store = &state.schedule_store;
            let day = (Utc::now() + Duration::days(3)).date_naive();
            let occurrence = day.and_hms_opt(2, 0, 0).unwrap().and_utc();
            let config = ScheduleRunConfig {
                task_message: Some("Review the saved report".into()),
                system_prompt: Some("Keep the saved format".into()),
                auto_execute: true,
                ..Default::default()
            };
            let entry = store
                .create_schedule_with_definition(
                    "Daily review".into(),
                    true,
                    config.clone(),
                    ScheduleDefinitionChanges {
                        trigger: Some(ScheduleTrigger::Daily {
                            hour: 10,
                            minute: 0,
                            second: 0,
                        }),
                        timezone: Some("Asia/Shanghai".into()),
                        start_at: Some(occurrence),
                        end_at: Some(occurrence + Duration::days(2)),
                        misfire_policy: Some(MisFirePolicy::RunOnce),
                        overlap_policy: Some(OverlapPolicy::Allow),
                    },
                )
                .await
                .unwrap();
            assert_eq!(entry.state.next_fire_at, Some(occurrence));
            let claims = store.claim_due_runs(occurrence).await.unwrap();
            assert_eq!(claims.len(), 1);
            assert_eq!(claims[0].scheduled_for, occurrence);
            let before = store
                .patch_schedule(&entry.id, None, Some(false), None, None)
                .await
                .unwrap()
                .unwrap();
            assert!(!before.enabled);
            assert_eq!(before.state.last_scheduled_at, Some(occurrence));
            assert_eq!(before.state.queued_run_count, 1);
            let records = store.list_run_records_for_schedule(&entry.id).await;
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].run_id, claims[0].run_id);
            assert_eq!(records[0].status, ScheduleRunStatus::Queued);

            let app = test::init_service(
                App::new()
                    .app_data(state.clone())
                    .configure(configure_routes),
            )
            .await;
            let uri = format!("/api/v1/schedules/{}/daily-time", entry.id);
            let response = test::call_service(
                &app,
                test::TestRequest::patch()
                    .uri(&uri)
                    .set_json(serde_json::json!({"hour":9,"minute":45,"second":7}))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            let view: Value = test::read_body_json(response).await;
            let saved = store.get_schedule(&entry.id).await.unwrap();
            let mut expected = before;
            expected.trigger = ScheduleTrigger::Daily {
                hour: 9,
                minute: 45,
                second: 7,
            };
            // The chosen clock has already passed on the last claim's local date.
            expected.state.next_fire_at =
                Some(occurrence + Duration::days(1) - Duration::minutes(15) + Duration::seconds(7));
            assert_ne!(saved.updated_at, expected.updated_at);
            expected.updated_at = saved.updated_at;
            assert_eq!(saved, expected);
            assert_eq!(
                view,
                serde_json::to_value(ScheduleView::from(expected.clone())).unwrap()
            );
            assert_eq!(claims[0].run_config, config);
            assert_eq!(
                store.list_run_records_for_schedule(&entry.id).await,
                records
            );
            let reopened = ScheduleStore::new(store.index_path().parent().unwrap().to_path_buf())
                .await
                .unwrap();
            assert_eq!(reopened.get_schedule(&entry.id).await, Some(expected));
            assert_eq!(
                reopened.list_run_records_for_schedule(&entry.id).await,
                records
            );
        }

        #[actix_web::test]
        async fn set_schedule_daily_time_keeps_legacy_seconds_and_inclusive_window_boundaries() {
            use chrono::{Duration, Utc};

            let temp_dir = tempdir().unwrap();
            bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
            let seed = ScheduleStore::new(temp_dir.path().to_path_buf())
                .await
                .unwrap();
            let day = (Utc::now() + Duration::days(3)).date_naive();
            let clock = day.and_hms_opt(2, 15, 19).unwrap().and_utc();
            let windows = [
                (clock, clock),
                (clock + Duration::minutes(30), clock + Duration::days(1)),
            ];
            let mut ids = Vec::new();
            for (start, end) in windows {
                let entry = seed
                    .create_schedule_with_definition(
                        "Daily report".into(),
                        false,
                        ScheduleRunConfig::default(),
                        ScheduleDefinitionChanges {
                            trigger: Some(ScheduleTrigger::Daily {
                                hour: 9,
                                minute: 0,
                                second: 0,
                            }),
                            timezone: Some("Asia/Shanghai".into()),
                            start_at: Some(start),
                            end_at: Some(end),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
                ids.push(entry.id);
            }
            let mut persisted: Value =
                serde_json::from_slice(&tokio::fs::read(seed.index_path()).await.unwrap()).unwrap();
            for id in &ids {
                assert_eq!(
                    persisted["schedules"][id.as_str()]["trigger"]
                        .as_object_mut()
                        .unwrap()
                        .remove("second"),
                    Some(serde_json::json!(0))
                );
            }
            tokio::fs::write(seed.index_path(), serde_json::to_vec(&persisted).unwrap())
                .await
                .unwrap();
            drop(seed);

            let state = web::Data::new(AppState::new(temp_dir.path().to_path_buf()).await.unwrap());
            let store = &state.schedule_store;
            let app = test::init_service(
                App::new()
                    .app_data(state.clone())
                    .configure(configure_routes),
            )
            .await;
            for (id, (start, expected_fire)) in ids.into_iter().zip(windows) {
                let saved = store.get_schedule(&id).await.unwrap();
                assert_eq!(
                    saved.trigger,
                    ScheduleTrigger::Daily {
                        hour: 9,
                        minute: 0,
                        second: 0,
                    }
                );
                assert_eq!(saved.state.next_fire_at, Some(start));
                let uri = format!("/api/v1/schedules/{id}/daily-time");
                let before = tokio::fs::read(store.index_path()).await.unwrap();
                let noop = test::call_service(
                    &app,
                    test::TestRequest::patch()
                        .uri(&uri)
                        .set_json(serde_json::json!({"hour":9,"minute":0,"second":0}))
                        .to_request(),
                )
                .await;
                assert_eq!(noop.status(), StatusCode::OK);
                assert_eq!(
                    test::read_body_json::<Value, _>(noop).await,
                    serde_json::to_value(ScheduleView::from(saved.clone())).unwrap()
                );
                assert_eq!(tokio::fs::read(store.index_path()).await.unwrap(), before);

                let edited = test::call_service(
                    &app,
                    test::TestRequest::patch()
                        .uri(&uri)
                        .set_json(serde_json::json!({"hour":10,"minute":15,"second":19}))
                        .to_request(),
                )
                .await;
                assert_eq!(edited.status(), StatusCode::OK);
                let view: Value = test::read_body_json(edited).await;
                let mut expected = serde_json::to_value(ScheduleView::from(saved)).unwrap();
                expected["trigger"] =
                    serde_json::json!({"type":"daily","hour":10,"minute":15,"second":19});
                expected["state"]["next_fire_at"] = serde_json::json!(expected_fire);
                expected["updated_at"] = view["updated_at"].clone();
                assert_eq!(view, expected);
            }
        }

        #[actix_web::test]
        async fn set_schedule_daily_time_refuses_invalid_missing_and_unusable_schedules() {
            use chrono::{Duration, Utc};

            let temp_dir = tempdir().unwrap();
            bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
            let seed = ScheduleStore::new(temp_dir.path().to_path_buf())
                .await
                .unwrap();
            let day = (Utc::now() + Duration::days(3)).date_naive();
            let occurrence = day.and_hms_opt(10, 0, 0).unwrap().and_utc();
            let daily = ScheduleTrigger::Daily {
                hour: 10,
                minute: 0,
                second: 0,
            };
            let ended = seed
                .create_schedule_with_definition(
                    "Finite daily review".into(),
                    true,
                    ScheduleRunConfig::default(),
                    ScheduleDefinitionChanges {
                        trigger: Some(daily.clone()),
                        start_at: Some(occurrence),
                        end_at: Some(occurrence + Duration::seconds(30)),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let once = seed
                .create_schedule(
                    "One-shot task".into(),
                    ScheduleTrigger::Once { at: occurrence },
                    false,
                    ScheduleRunConfig::default(),
                )
                .await
                .unwrap();
            // The public creation route rejects Workflow + Daily. Retain an
            // incompatible persisted row to check that this edit rejects it too.
            let workflow = seed
                .create_schedule(
                    "Legacy Workflow".into(),
                    daily,
                    false,
                    ScheduleRunConfig {
                        workflow_target: Some(bamboo_domain::ScheduledWorkflowTarget {
                            workflow_id: "fixture-workflow".into(),
                            revision: 1,
                            args: serde_json::json!({}),
                        }),
                        auto_execute: true,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            // Use the serialized inactive monitor shape without starting a scan.
            let monitor = seed
                .create_schedule(
                    "Inactive monitor".into(),
                    ScheduleTrigger::legacy_interval(60, None),
                    false,
                    ScheduleRunConfig::default(),
                )
                .await
                .unwrap();
            let claims = seed.claim_due_runs(occurrence).await.unwrap();
            assert_eq!(claims.len(), 1);
            assert_eq!(claims[0].schedule_id, ended.id);
            let ended = seed.get_schedule(&ended.id).await.unwrap();
            assert!(!ended.enabled);
            assert_eq!(ended.state.next_fire_at, None);
            let mut persisted: Value =
                serde_json::from_slice(&tokio::fs::read(seed.index_path()).await.unwrap()).unwrap();
            let stamp = occurrence;
            let file = serde_json::json!({"device":1,"inode":2});
            persisted["schedules"][monitor.id.as_str()]["log_monitor"] = serde_json::json!({
                "owner": {
                    "supervisor": {"session_id":"fixture", "incarnation_id": uuid::Uuid::nil()},
                    "created_at": stamp
                },
                "spec": {
                    "project_id": "fixture-project", "relative_path": "fixture.log",
                    "every_seconds": 60, "workspace": temp_dir.path(), "workspace_identity": file
                },
                "active": false, "generation": uuid::Uuid::nil(), "session_created_at": stamp,
                "cursor": {
                    "file": file, "offset": 0, "skip_partial_line": false,
                    "checkpoint": vec![0; 32]
                },
                "pending_alert": null
            });
            tokio::fs::write(seed.index_path(), serde_json::to_vec(&persisted).unwrap())
                .await
                .unwrap();
            drop(seed);

            let state = web::Data::new(AppState::new(temp_dir.path().to_path_buf()).await.unwrap());
            let store = &state.schedule_store;
            let app = test::init_service(
                App::new()
                    .app_data(state.clone())
                    .configure(configure_routes),
            )
            .await;
            let saved = tokio::fs::read(store.index_path()).await.unwrap();
            let ended_uri = format!("/api/v1/schedules/{}/daily-time", ended.id);
            for payload in [
                r#"{"hour":9,"minute":0"#,
                "[9,0,0]",
                r#"{"hour":9,"minute":0}"#,
                r#"{"hour":-1,"minute":0,"second":0}"#,
                r#"{"hour":24,"minute":0,"second":0}"#,
                r#"{"hour":9,"minute":60,"second":0}"#,
                r#"{"hour":9,"minute":0,"second":60}"#,
                r#"{"hour":9,"minute":0,"second":0,"enabled":true}"#,
            ] {
                let response = test::call_service(
                    &app,
                    test::TestRequest::patch()
                        .uri(&ended_uri)
                        .insert_header(("Content-Type", "application/json"))
                        .set_payload(payload)
                        .to_request(),
                )
                .await;
                assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{payload}");
                let body: Value = test::read_body_json(response).await;
                assert_eq!(body["error"]["type"], "api_error");
                assert_eq!(body["error"]["code"], "invalid_schedule_daily_time");
            }
            for (id, status, code) in [
                (
                    ended.id.as_str(),
                    StatusCode::CONFLICT,
                    "schedule_no_future_occurrence",
                ),
                (
                    once.id.as_str(),
                    StatusCode::CONFLICT,
                    "schedule_trigger_changed",
                ),
                (
                    workflow.id.as_str(),
                    StatusCode::CONFLICT,
                    "schedule_trigger_changed",
                ),
                (
                    monitor.id.as_str(),
                    StatusCode::NOT_FOUND,
                    "schedule_not_found",
                ),
                ("missing", StatusCode::NOT_FOUND, "schedule_not_found"),
            ] {
                let response = test::call_service(
                    &app,
                    test::TestRequest::patch()
                        .uri(&format!("/api/v1/schedules/{id}/daily-time"))
                        .set_json(serde_json::json!({"hour":9,"minute":0,"second":0}))
                        .to_request(),
                )
                .await;
                assert_eq!(response.status(), status, "{id}");
                let body: Value = test::read_body_json(response).await;
                assert_eq!(body["error"]["type"], "api_error");
                assert_eq!(body["error"]["code"], code);
            }
            let noop = test::call_service(
                &app,
                test::TestRequest::patch()
                    .uri(&ended_uri)
                    .set_json(serde_json::json!({"hour":10,"minute":0,"second":0}))
                    .to_request(),
            )
            .await;
            assert_eq!(noop.status(), StatusCode::OK);
            assert_eq!(
                test::read_body_json::<Value, _>(noop).await,
                serde_json::to_value(ScheduleView::from(ended.clone())).unwrap()
            );
            assert_eq!(tokio::fs::read(store.index_path()).await.unwrap(), saved);
            assert_eq!(store.get_schedule(&ended.id).await, Some(ended));
            assert_eq!(store.get_schedule(&once.id).await, Some(once));
            assert_eq!(store.get_schedule(&workflow.id).await, Some(workflow));
        }

        #[actix_web::test]
        async fn set_schedule_weekly_time_preserves_saved_weekdays_and_admitted_run() {
            use bamboo_domain::ScheduleWeekday::{Fri, Mon, Wed};
            use chrono::{Datelike, Duration, Utc};

            let temp_dir = tempdir().unwrap();
            bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
            let state = web::Data::new(AppState::new(temp_dir.path().to_path_buf()).await.unwrap());
            let store = &state.schedule_store;
            let day = (Utc::now() + Duration::days(3)).date_naive();
            let until_monday = i64::from((7 - day.weekday().num_days_from_monday()) % 7);
            let monday = (day + Duration::days(until_monday))
                .and_hms_opt(2, 0, 0)
                .unwrap()
                .and_utc();
            // 09:45:07 in Shanghai on Wednesday is the inclusive closing instant.
            let wednesday =
                monday + Duration::days(2) - Duration::minutes(15) + Duration::seconds(7);
            let weekdays = vec![Wed, Mon, Wed, Fri];
            let config = ScheduleRunConfig {
                task_message: Some("Review the saved weekly report".into()),
                system_prompt: Some("Keep the saved format".into()),
                auto_execute: true,
                ..Default::default()
            };
            let entry = store
                .create_schedule_with_definition(
                    "Weekly review".into(),
                    true,
                    config.clone(),
                    ScheduleDefinitionChanges {
                        trigger: Some(ScheduleTrigger::Weekly {
                            weekdays: weekdays.clone(),
                            hour: 10,
                            minute: 0,
                            second: 0,
                        }),
                        timezone: Some("Asia/Shanghai".into()),
                        start_at: Some(monday),
                        end_at: Some(wednesday),
                        misfire_policy: Some(MisFirePolicy::RunOnce),
                        overlap_policy: Some(OverlapPolicy::Allow),
                    },
                )
                .await
                .unwrap();
            assert_eq!(entry.state.next_fire_at, Some(monday));
            let claims = store.claim_due_runs(monday).await.unwrap();
            assert_eq!(claims.len(), 1);
            assert_eq!(claims[0].scheduled_for, monday);
            assert_eq!(claims[0].run_config, config);
            let before = store.get_schedule(&entry.id).await.unwrap();
            assert!(!before.enabled);
            assert_eq!(before.state.last_scheduled_at, Some(monday));
            assert_eq!(before.state.next_fire_at, None);
            assert_eq!(before.state.queued_run_count, 1);
            let records = store.list_run_records_for_schedule(&entry.id).await;
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].run_id, claims[0].run_id);
            assert_eq!(records[0].status, ScheduleRunStatus::Queued);

            let app = test::init_service(
                App::new()
                    .app_data(state.clone())
                    .configure(configure_routes),
            )
            .await;
            let response = test::call_service(
                &app,
                test::TestRequest::patch()
                    .uri(&format!("/api/v1/schedules/{}/weekly-time", entry.id))
                    .set_json(serde_json::json!({"hour":9,"minute":45,"second":7}))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            let view: Value = test::read_body_json(response).await;
            let saved = store.get_schedule(&entry.id).await.unwrap();
            let mut expected = before;
            expected.trigger = ScheduleTrigger::Weekly {
                weekdays,
                hour: 9,
                minute: 45,
                second: 7,
            };
            expected.state.next_fire_at = Some(wednesday);
            assert_ne!(saved.updated_at, expected.updated_at);
            expected.updated_at = saved.updated_at;
            assert_eq!(saved, expected);
            assert_eq!(
                view,
                serde_json::to_value(ScheduleView::from(expected.clone())).unwrap()
            );

            let reopened = ScheduleStore::new(store.index_path().parent().unwrap().to_path_buf())
                .await
                .unwrap();
            assert_eq!(reopened.get_schedule(&entry.id).await, Some(expected));
            assert_eq!(
                reopened.list_run_records_for_schedule(&entry.id).await,
                records
            );
        }

        #[actix_web::test]
        async fn set_schedule_weekly_time_refuses_invalid_edits_and_keeps_expired_same_clock() {
            use bamboo_domain::ScheduleWeekday::Wed;
            use chrono::Duration;

            let temp_dir = tempdir().unwrap();
            bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
            let seed = ScheduleStore::new(temp_dir.path().to_path_buf())
                .await
                .unwrap();
            let weekly = ScheduleTrigger::Weekly {
                weekdays: vec![Wed],
                hour: 10,
                minute: 0,
                second: 0,
            };
            let ended = seed
                .create_schedule("Ended".into(), weekly.clone(), false, Default::default())
                .await
                .unwrap();
            let empty = seed
                .create_schedule("Empty".into(), weekly.clone(), false, Default::default())
                .await
                .unwrap();
            let stale = seed
                .create_schedule("Stale".into(), weekly.clone(), false, Default::default())
                .await
                .unwrap();
            let stale = seed
                .patch_schedule(
                    &stale.id,
                    None,
                    None,
                    Some(ScheduleTrigger::Daily {
                        hour: 10,
                        minute: 0,
                        second: 0,
                    }),
                    None,
                )
                .await
                .unwrap()
                .unwrap();
            let workflow = seed
                .create_schedule(
                    "Incompatible Workflow".into(),
                    weekly,
                    false,
                    ScheduleRunConfig {
                        workflow_target: Some(bamboo_domain::ScheduledWorkflowTarget {
                            workflow_id: "fixture-workflow".into(),
                            revision: 1,
                            args: serde_json::json!({}),
                        }),
                        auto_execute: true,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let mut persisted: Value =
                serde_json::from_slice(&tokio::fs::read(seed.index_path()).await.unwrap()).unwrap();
            persisted["schedules"][empty.id.as_str()]["trigger"]["weekdays"] =
                serde_json::json!([]);
            let end = ended.state.next_fire_at.unwrap() - Duration::weeks(3);
            let ended_record = &mut persisted["schedules"][ended.id.as_str()];
            ended_record["start_at"] = serde_json::json!(end - Duration::weeks(1));
            ended_record["end_at"] = serde_json::json!(end);
            ended_record["state"]["next_fire_at"] = serde_json::json!(end);
            ended_record["trigger"]
                .as_object_mut()
                .unwrap()
                .remove("second");
            tokio::fs::write(seed.index_path(), serde_json::to_vec(&persisted).unwrap())
                .await
                .unwrap();
            drop(seed);

            let state = web::Data::new(AppState::new(temp_dir.path().to_path_buf()).await.unwrap());
            let store = &state.schedule_store;
            let app = test::init_service(
                App::new()
                    .app_data(state.clone())
                    .configure(configure_routes),
            )
            .await;
            let saved = tokio::fs::read(store.index_path()).await.unwrap();
            let ended = store.get_schedule(&ended.id).await.unwrap();
            let uri = |id| format!("/api/v1/schedules/{id}/weekly-time");
            for payload in [
                r#"{"hour":10,"minute":0"#,
                "[10,0,0]",
                r#"{"hour":10,"minute":0}"#,
                r#"{"hour":24,"minute":0,"second":0}"#,
                r#"{"hour":10,"minute":60,"second":0}"#,
                r#"{"hour":10,"minute":0,"second":60}"#,
                r#"{"hour":10,"minute":0,"second":0,"weekdays":["mon"]}"#,
            ] {
                let response = test::call_service(
                    &app,
                    test::TestRequest::patch()
                        .uri(&uri(ended.id.as_str()))
                        .insert_header(("Content-Type", "application/json"))
                        .set_payload(payload)
                        .to_request(),
                )
                .await;
                assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{payload}");
                let body: Value = test::read_body_json(response).await;
                assert_eq!(body["error"]["type"], "api_error");
                assert_eq!(body["error"]["code"], "invalid_schedule_weekly_time");
            }
            for (id, hour, status, code) in [
                (
                    ended.id.as_str(),
                    9,
                    StatusCode::CONFLICT,
                    "schedule_no_future_occurrence",
                ),
                (
                    empty.id.as_str(),
                    10,
                    StatusCode::CONFLICT,
                    "schedule_trigger_changed",
                ),
                (
                    stale.id.as_str(),
                    10,
                    StatusCode::CONFLICT,
                    "schedule_trigger_changed",
                ),
                (
                    workflow.id.as_str(),
                    10,
                    StatusCode::CONFLICT,
                    "schedule_trigger_changed",
                ),
                ("missing", 10, StatusCode::NOT_FOUND, "schedule_not_found"),
            ] {
                let response = test::call_service(
                    &app,
                    test::TestRequest::patch()
                        .uri(&uri(id))
                        .set_json(serde_json::json!({"hour":hour,"minute":0,"second":0}))
                        .to_request(),
                )
                .await;
                assert_eq!(response.status(), status, "{id}");
                let body: Value = test::read_body_json(response).await;
                assert_eq!(body["error"]["type"], "api_error");
                assert_eq!(body["error"]["code"], code);
            }
            let noop = test::call_service(
                &app,
                test::TestRequest::patch()
                    .uri(&uri(ended.id.as_str()))
                    .set_json(serde_json::json!({"hour":10,"minute":0,"second":0}))
                    .to_request(),
            )
            .await;
            assert_eq!(noop.status(), StatusCode::OK);
            assert_eq!(
                test::read_body_json::<Value, _>(noop).await,
                serde_json::to_value(ScheduleView::from(ended.clone())).unwrap()
            );
            assert_eq!(tokio::fs::read(store.index_path()).await.unwrap(), saved);
            assert_eq!(store.get_schedule(&ended.id).await, Some(ended));
            assert_eq!(store.get_schedule(&stale.id).await, Some(stale));
            assert_eq!(store.get_schedule(&workflow.id).await, Some(workflow));
        }

        #[actix_web::test]
        async fn set_schedule_monthly_time_skips_short_month_and_preserves_admitted_run() {
            use chrono::{Datelike, TimeZone, Utc};

            let temp_dir = tempdir().unwrap();
            bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
            let state = web::Data::new(AppState::new(temp_dir.path().to_path_buf()).await.unwrap());
            let store = &state.schedule_store;
            let year = Utc::now().year() + 1;
            let january = Utc.with_ymd_and_hms(year, 1, 31, 2, 0, 0).single().unwrap();
            // 09:45:07 in Shanghai on March 30 is the inclusive closing instant.
            let march = Utc
                .with_ymd_and_hms(year, 3, 30, 1, 45, 7)
                .single()
                .unwrap();
            let days = vec![31, 30, 31];
            let config = ScheduleRunConfig {
                task_message: Some("Review the saved month-end report".into()),
                system_prompt: Some("Keep the saved format".into()),
                auto_execute: true,
                ..Default::default()
            };
            let entry = store
                .create_schedule_with_definition(
                    "Month-end review".into(),
                    true,
                    config.clone(),
                    ScheduleDefinitionChanges {
                        trigger: Some(ScheduleTrigger::Monthly {
                            days: days.clone(),
                            hour: 10,
                            minute: 0,
                            second: 0,
                        }),
                        timezone: Some("Asia/Shanghai".into()),
                        start_at: Some(january),
                        end_at: Some(march),
                        misfire_policy: Some(MisFirePolicy::RunOnce),
                        overlap_policy: Some(OverlapPolicy::Allow),
                    },
                )
                .await
                .unwrap();
            assert_eq!(entry.state.next_fire_at, Some(january));
            let claims = store.claim_due_runs(january).await.unwrap();
            assert_eq!(claims.len(), 1);
            assert_eq!(claims[0].scheduled_for, january);
            assert_eq!(claims[0].run_config, config);
            let before = store.get_schedule(&entry.id).await.unwrap();
            assert!(!before.enabled);
            assert_eq!(before.state.last_scheduled_at, Some(january));
            assert_eq!(before.state.next_fire_at, None);
            assert_eq!(before.state.queued_run_count, 1);
            let records = store.list_run_records_for_schedule(&entry.id).await;
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].run_id, claims[0].run_id);
            assert_eq!(records[0].status, ScheduleRunStatus::Queued);

            let app = test::init_service(
                App::new()
                    .app_data(state.clone())
                    .configure(configure_routes),
            )
            .await;
            let response = test::call_service(
                &app,
                test::TestRequest::patch()
                    .uri(&format!("/api/v1/schedules/{}/monthly-time", entry.id))
                    .set_json(serde_json::json!({"hour":9,"minute":45,"second":7}))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            let view: Value = test::read_body_json(response).await;
            let saved = store.get_schedule(&entry.id).await.unwrap();
            let mut expected = before;
            expected.trigger = ScheduleTrigger::Monthly {
                days,
                hour: 9,
                minute: 45,
                second: 7,
            };
            expected.state.next_fire_at = Some(march);
            assert_ne!(saved.updated_at, expected.updated_at);
            expected.updated_at = saved.updated_at;
            assert_eq!(saved, expected);
            assert_eq!(
                view,
                serde_json::to_value(ScheduleView::from(expected.clone())).unwrap()
            );
            assert_eq!(
                store.list_run_records_for_schedule(&entry.id).await,
                records
            );

            let reopened = ScheduleStore::new(store.index_path().parent().unwrap().to_path_buf())
                .await
                .unwrap();
            assert_eq!(reopened.get_schedule(&entry.id).await, Some(expected));
            assert_eq!(
                reopened.list_run_records_for_schedule(&entry.id).await,
                records
            );
        }

        #[actix_web::test]
        async fn set_schedule_monthly_time_refuses_invalid_edits_and_keeps_expired_same_clock() {
            use chrono::{Datelike, Duration, TimeZone, Utc};

            let temp_dir = tempdir().unwrap();
            bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
            let seed = ScheduleStore::new(temp_dir.path().to_path_buf())
                .await
                .unwrap();
            let year = Utc::now().year();
            let expired = Utc
                .with_ymd_and_hms(year - 1, 1, 31, 10, 0, 0)
                .single()
                .unwrap();
            let opening = Utc
                .with_ymd_and_hms(year + 1, 1, 31, 10, 0, 0)
                .single()
                .unwrap();
            let monthly = ScheduleTrigger::Monthly {
                days: vec![31, 30, 31],
                hour: 10,
                minute: 0,
                second: 0,
            };
            let ended = seed
                .create_schedule("Ended".into(), monthly.clone(), false, Default::default())
                .await
                .unwrap();
            let bounded = seed
                .create_schedule_with_definition(
                    "Existing bounded clock".into(),
                    false,
                    Default::default(),
                    ScheduleDefinitionChanges {
                        trigger: Some(monthly.clone()),
                        start_at: Some(opening),
                        end_at: Some(opening + Duration::seconds(30)),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            assert_eq!(bounded.state.next_fire_at, Some(opening));
            let wrong_kind = seed
                .create_schedule(
                    "Already changed".into(),
                    ScheduleTrigger::Daily {
                        hour: 10,
                        minute: 0,
                        second: 0,
                    },
                    false,
                    Default::default(),
                )
                .await
                .unwrap();
            let workflow = seed
                .create_schedule(
                    "Incompatible legacy Workflow".into(),
                    monthly.clone(),
                    false,
                    ScheduleRunConfig {
                        workflow_target: Some(bamboo_domain::ScheduledWorkflowTarget {
                            workflow_id: "fixture-workflow".into(),
                            revision: 1,
                            args: serde_json::json!({}),
                        }),
                        auto_execute: true,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let mut invalid_days = Vec::new();
            for (label, days) in [
                ("Empty", vec![]),
                ("Zero", vec![31, 0]),
                ("Too large", vec![30, 32]),
            ] {
                let entry = seed
                    .create_schedule(label.into(), monthly.clone(), false, Default::default())
                    .await
                    .unwrap();
                invalid_days.push((entry.id, days));
            }
            let mut persisted: Value =
                serde_json::from_slice(&tokio::fs::read(seed.index_path()).await.unwrap()).unwrap();
            for (id, days) in &invalid_days {
                persisted["schedules"][id.as_str()]["trigger"]["days"] = serde_json::json!(days);
            }
            let expired_record = &mut persisted["schedules"][ended.id.as_str()];
            expired_record["end_at"] = serde_json::json!(expired);
            expired_record["state"]["next_fire_at"] = serde_json::json!(expired);
            expired_record["trigger"]
                .as_object_mut()
                .unwrap()
                .remove("second");
            tokio::fs::write(seed.index_path(), serde_json::to_vec(&persisted).unwrap())
                .await
                .unwrap();
            drop(seed);

            let state = web::Data::new(AppState::new(temp_dir.path().to_path_buf()).await.unwrap());
            let store = &state.schedule_store;
            let app = test::init_service(
                App::new()
                    .app_data(state.clone())
                    .configure(configure_routes),
            )
            .await;
            let saved = tokio::fs::read(store.index_path()).await.unwrap();
            let ended = store.get_schedule(&ended.id).await.unwrap();
            let uri = |id| format!("/api/v1/schedules/{id}/monthly-time");
            for payload in [
                r#"{"hour":10,"minute":0"#,
                "[10,0,0]",
                r#"{"hour":10,"minute":0}"#,
                r#"{"hour":24,"minute":0,"second":0}"#,
                r#"{"hour":10,"minute":0,"second":0,"days":[1]}"#,
            ] {
                let response = test::call_service(
                    &app,
                    test::TestRequest::patch()
                        .uri(&uri(ended.id.as_str()))
                        .insert_header(("Content-Type", "application/json"))
                        .set_payload(payload)
                        .to_request(),
                )
                .await;
                assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{payload}");
                let body: Value = test::read_body_json(response).await;
                assert_eq!(body["error"]["type"], "api_error");
                assert_eq!(body["error"]["code"], "invalid_schedule_monthly_time");
            }
            let refusals = [
                (
                    ended.id.as_str(),
                    9,
                    StatusCode::CONFLICT,
                    "schedule_no_future_occurrence",
                ),
                (
                    bounded.id.as_str(),
                    9,
                    StatusCode::CONFLICT,
                    "schedule_no_future_occurrence",
                ),
                (
                    wrong_kind.id.as_str(),
                    10,
                    StatusCode::CONFLICT,
                    "schedule_trigger_changed",
                ),
                (
                    workflow.id.as_str(),
                    10,
                    StatusCode::CONFLICT,
                    "schedule_trigger_changed",
                ),
                ("missing", 10, StatusCode::NOT_FOUND, "schedule_not_found"),
            ];
            for (id, hour, status, code) in
                refusals
                    .into_iter()
                    .chain(invalid_days.iter().map(|(id, _)| {
                        (
                            id.as_str(),
                            10,
                            StatusCode::CONFLICT,
                            "schedule_trigger_changed",
                        )
                    }))
            {
                let response = test::call_service(
                    &app,
                    test::TestRequest::patch()
                        .uri(&uri(id))
                        .set_json(serde_json::json!({"hour":hour,"minute":0,"second":0}))
                        .to_request(),
                )
                .await;
                assert_eq!(response.status(), status, "{id}");
                let body: Value = test::read_body_json(response).await;
                assert_eq!(body["error"]["type"], "api_error");
                assert_eq!(body["error"]["code"], code);
            }
            let noop = test::call_service(
                &app,
                test::TestRequest::patch()
                    .uri(&uri(ended.id.as_str()))
                    .set_json(serde_json::json!({"hour":10,"minute":0,"second":0}))
                    .to_request(),
            )
            .await;
            assert_eq!(noop.status(), StatusCode::OK);
            assert_eq!(
                test::read_body_json::<Value, _>(noop).await,
                serde_json::to_value(ScheduleView::from(ended.clone())).unwrap()
            );
            assert_eq!(tokio::fs::read(store.index_path()).await.unwrap(), saved);
            assert_eq!(store.get_schedule(&ended.id).await, Some(ended));
            assert_eq!(store.get_schedule(&bounded.id).await, Some(bounded));
        }

        async fn patch_once_at_request(
            app: &impl Service<
                actix_http::Request,
                Response = ServiceResponse,
                Error = actix_web::Error,
            >,
            schedule_id: &str,
            payload: &str,
        ) -> (StatusCode, Value) {
            let response = test::call_service(
                app,
                test::TestRequest::patch()
                    .uri(&format!("/api/v1/schedules/{schedule_id}/once-at"))
                    .insert_header(("Content-Type", "application/json"))
                    .set_payload(payload.to_owned())
                    .to_request(),
            )
            .await;
            (response.status(), test::read_body_json(response).await)
        }

        #[actix_web::test]
        async fn set_schedule_once_at_preserves_disabled_legacy_pending_and_manual_run() {
            use chrono::{Duration, FixedOffset, SecondsFormat, Timelike, Utc};

            let temp_dir = tempdir().unwrap();
            let home = temp_dir.path().to_path_buf();
            bamboo_config::paths::init_bamboo_dir(home.clone());
            let seed = ScheduleStore::new(home.clone()).await.unwrap();
            let start = (Utc::now() + Duration::days(2))
                .with_nanosecond(123_456_789)
                .unwrap();
            let end = start + Duration::hours(2);
            let config = ScheduleRunConfig {
                task_message: Some("Review the saved report".into()),
                system_prompt: Some("Keep the saved format".into()),
                auto_execute: true,
                ..Default::default()
            };
            let entry = seed
                .create_schedule_with_definition(
                    "One-time review".into(),
                    false,
                    config.clone(),
                    ScheduleDefinitionChanges {
                        trigger: Some(ScheduleTrigger::Once { at: start }),
                        timezone: Some("America/New_York".into()),
                        start_at: Some(start),
                        end_at: Some(end),
                        misfire_policy: Some(MisFirePolicy::RunOnce),
                        overlap_policy: Some(OverlapPolicy::Allow),
                    },
                )
                .await
                .unwrap();
            let manual = seed.create_run_now(&entry.id).await.unwrap().unwrap();
            assert_eq!(manual.run_config, config);
            let records = seed.list_run_records_for_schedule(&entry.id).await;
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].run_id, manual.run_id);
            assert_eq!(records[0].status, ScheduleRunStatus::Queued);

            // An earlier nominal trigger may have been clamped to the future window start.
            let legacy_at = start - Duration::days(3);
            let mut index: Value =
                serde_json::from_slice(&tokio::fs::read(seed.index_path()).await.unwrap()).unwrap();
            let row = index["schedules"][entry.id.as_str()]
                .as_object_mut()
                .unwrap();
            row.remove("state");
            row.insert("next_run_at".into(), serde_json::json!(start));
            row.insert(
                "trigger".into(),
                serde_json::json!(ScheduleTrigger::Once { at: legacy_at }),
            );
            let overdue_id = "overdue-legacy-once";
            let mut overdue = row.clone();
            overdue.insert("id".into(), serde_json::json!(overdue_id));
            overdue.insert("next_run_at".into(), serde_json::json!(legacy_at));
            index["schedules"][overdue_id] = Value::Object(overdue);
            tokio::fs::write(seed.index_path(), serde_json::to_vec(&index).unwrap())
                .await
                .unwrap();
            drop(seed);

            let state = web::Data::new(AppState::new(home.clone()).await.unwrap());
            let store = &state.schedule_store;
            let before = store.get_schedule(&entry.id).await.unwrap();
            assert!(!before.enabled);
            assert_eq!(before.trigger, ScheduleTrigger::Once { at: legacy_at });
            assert_eq!(before.state.next_fire_at, Some(start));
            let overdue = store.get_schedule(overdue_id).await.unwrap();
            assert_eq!(overdue.state.next_fire_at, Some(legacy_at));
            let app = test::init_service(
                App::new()
                    .app_data(state.clone())
                    .configure(configure_routes),
            )
            .await;

            let previous_bytes = tokio::fs::read(store.index_path()).await.unwrap();
            let (status, body) =
                patch_once_at_request(&app, overdue_id, &serde_json::json!({"at":end}).to_string())
                    .await;
            assert_eq!(status, StatusCode::CONFLICT);
            assert_eq!(body["error"]["code"], "schedule_once_not_pending");
            assert_eq!(store.get_schedule(overdue_id).await, Some(overdue));
            for outside in [
                start - Duration::nanoseconds(1),
                end + Duration::nanoseconds(1),
            ] {
                let (status, body) = patch_once_at_request(
                    &app,
                    &entry.id,
                    &serde_json::json!({"at":outside}).to_string(),
                )
                .await;
                assert_eq!(status, StatusCode::CONFLICT);
                assert_eq!(body["error"]["code"], "schedule_no_future_occurrence");
            }
            assert_eq!(
                tokio::fs::read(store.index_path()).await.unwrap(),
                previous_bytes
            );
            assert_eq!(store.get_schedule(&entry.id).await, Some(before.clone()));

            let offset = FixedOffset::east_opt(8 * 3600).unwrap();
            let payload = serde_json::json!({
                "at": end.with_timezone(&offset).to_rfc3339_opts(SecondsFormat::Nanos, false)
            });
            let (status, view) = patch_once_at_request(&app, &entry.id, &payload.to_string()).await;
            assert_eq!(status, StatusCode::OK, "{view}");
            let saved = store.get_schedule(&entry.id).await.unwrap();
            let mut expected = before;
            expected.trigger = ScheduleTrigger::Once { at: end };
            expected.state.next_fire_at = Some(end);
            assert_ne!(saved.updated_at, expected.updated_at);
            expected.updated_at = saved.updated_at;
            assert_eq!(saved, expected);
            assert_eq!(
                view,
                serde_json::to_value(ScheduleView::from(expected.clone())).unwrap()
            );

            let saved_bytes = tokio::fs::read(store.index_path()).await.unwrap();
            let (status, repeated) =
                patch_once_at_request(&app, &entry.id, &serde_json::json!({"at":end}).to_string())
                    .await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(repeated, view);
            assert_eq!(
                tokio::fs::read(store.index_path()).await.unwrap(),
                saved_bytes
            );
            assert_eq!(
                store.list_run_records_for_schedule(&entry.id).await,
                records
            );
            let reopened = ScheduleStore::new(home).await.unwrap();
            assert_eq!(reopened.get_schedule(&entry.id).await, Some(expected));
            assert_eq!(
                reopened.list_run_records_for_schedule(&entry.id).await,
                records
            );
        }

        #[actix_web::test]
        async fn set_schedule_once_at_refuses_consumed_automatic_occurrence_after_reopen() {
            use chrono::{Duration, Utc};

            let temp_dir = tempdir().unwrap();
            let home = temp_dir.path().to_path_buf();
            bamboo_config::paths::init_bamboo_dir(home.clone());
            let seed = ScheduleStore::new(home.clone()).await.unwrap();
            let original_at = Utc::now() + Duration::hours(2);
            let entry = seed
                .create_schedule_with_definition(
                    "Automatic one-time review".into(),
                    true,
                    ScheduleRunConfig {
                        task_message: Some("Review the report".into()),
                        ..Default::default()
                    },
                    ScheduleDefinitionChanges {
                        trigger: Some(ScheduleTrigger::Once { at: original_at }),
                        misfire_policy: Some(MisFirePolicy::RunOnce),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let claims = seed.claim_due_runs(original_at).await.unwrap();
            assert_eq!(claims.len(), 1);
            assert_eq!(claims[0].scheduled_for, original_at);
            let run_id = &claims[0].run_id;
            drop(seed);

            for run_status in [ScheduleRunStatus::Queued, ScheduleRunStatus::Running] {
                let state = web::Data::new(AppState::new(home.clone()).await.unwrap());
                let store = &state.schedule_store;
                let before = store.get_schedule(&entry.id).await.unwrap();
                assert!(Utc::now() < original_at);
                assert!(!before.enabled);
                assert_eq!(before.state.next_fire_at, None);
                assert_eq!(before.state.last_scheduled_at, Some(original_at));
                let records = store.list_run_records_for_schedule(&entry.id).await;
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].run_id, *run_id);
                assert_eq!(records[0].status, run_status);
                let bytes = tokio::fs::read(store.index_path()).await.unwrap();

                let app = test::init_service(
                    App::new()
                        .app_data(state.clone())
                        .configure(configure_routes),
                )
                .await;
                for at in [original_at, original_at + Duration::hours(1)] {
                    let (status, body) = patch_once_at_request(
                        &app,
                        &entry.id,
                        &serde_json::json!({"at":at}).to_string(),
                    )
                    .await;
                    assert_eq!(status, StatusCode::CONFLICT);
                    assert_eq!(body["error"]["code"], "schedule_once_not_pending");
                }
                assert_eq!(tokio::fs::read(store.index_path()).await.unwrap(), bytes);
                assert_eq!(store.get_schedule(&entry.id).await, Some(before));
                assert_eq!(
                    store.list_run_records_for_schedule(&entry.id).await,
                    records
                );

                if run_status == ScheduleRunStatus::Queued {
                    store.mark_run_started(&entry.id, run_id).await.unwrap();
                }
            }
        }

        #[actix_web::test]
        async fn set_schedule_once_at_rejects_malformed_requests_and_other_targets() {
            use chrono::{Duration, Utc};

            let temp_dir = tempdir().unwrap();
            let home = temp_dir.path().to_path_buf();
            bamboo_config::paths::init_bamboo_dir(home.clone());
            let state = web::Data::new(AppState::new(home).await.unwrap());
            let store = &state.schedule_store;
            let at = Utc::now() + Duration::hours(2);
            let interval = store
                .create_schedule_with_definition(
                    "Existing recurrence".into(),
                    false,
                    ScheduleRunConfig::default(),
                    ScheduleDefinitionChanges {
                        trigger: Some(ScheduleTrigger::legacy_interval(60, None)),
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
            let bytes = tokio::fs::read(store.index_path()).await.unwrap();
            for payload in [
                "[]",
                "{}",
                r#"{"at":42}"#,
                r#"{"at":"2099-06-01T12:00:00"}"#,
                r#"{"at":"2099-06-01T12:00:00+0800"}"#,
                r#"{"at":"2099-06-01T12:00:00Z","enabled":true}"#,
            ] {
                let (status, body) = patch_once_at_request(&app, &interval.id, payload).await;
                assert_eq!(status, StatusCode::BAD_REQUEST, "{payload} {body}");
                assert_eq!(body["error"]["code"], "invalid_schedule_once_at");
            }
            let payload = serde_json::json!({"at":at}).to_string();
            for (id, expected_status, code) in [
                (
                    interval.id.as_str(),
                    StatusCode::CONFLICT,
                    "schedule_trigger_changed",
                ),
                (
                    "missing-schedule",
                    StatusCode::NOT_FOUND,
                    "schedule_not_found",
                ),
            ] {
                let (status, body) = patch_once_at_request(&app, id, &payload).await;
                assert_eq!(status, expected_status, "{body}");
                assert_eq!(body["error"]["code"], code);
            }
            assert_eq!(tokio::fs::read(store.index_path()).await.unwrap(), bytes);
            assert_eq!(store.get_schedule(&interval.id).await, Some(interval));
        }

        #[actix_web::test]
        async fn set_schedule_prompt_preserves_disabled_and_exhausted_tasks_and_prior_claims() {
            const OLD_PROMPT: &str = "Previous report instructions";
            const NEW_PROMPT: &str = " \tRéviser les notes 🧪\r\n写结论\n  ";

            let temp_dir = tempdir().unwrap();
            bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
            let state = web::Data::new(AppState::new(temp_dir.path().to_path_buf()).await.unwrap());
            let store = &state.schedule_store;
            let config = ScheduleRunConfig {
                task_message: Some(OLD_PROMPT.into()),
                system_prompt: Some("Keep the saved report format".into()),
                enhance_prompt: Some("Keep the saved refinement".into()),
                model: Some("fixture-model".into()),
                auto_execute: true,
                ..Default::default()
            };
            let occurrence = chrono::Utc::now() + chrono::Duration::hours(2);
            let once = store
                .create_schedule(
                    "Consumed one-shot".into(),
                    ScheduleTrigger::Once { at: occurrence },
                    true,
                    config.clone(),
                )
                .await
                .unwrap();
            let ended = store
                .create_schedule_with_definition(
                    "Ended interval".into(),
                    true,
                    config.clone(),
                    ScheduleDefinitionChanges {
                        trigger: Some(ScheduleTrigger::legacy_interval(900, Some(occurrence))),
                        timezone: Some("Asia/Shanghai".into()),
                        end_at: Some(occurrence + chrono::Duration::seconds(30)),
                        overlap_policy: Some(OverlapPolicy::Allow),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let disabled = store
                .create_schedule(
                    "Disabled recurring task".into(),
                    ScheduleTrigger::legacy_interval(900, Some(occurrence)),
                    false,
                    config.clone(),
                )
                .await
                .unwrap();
            assert!(disabled.state.next_fire_at.is_some());
            // Claim the due occurrences directly in the disposable store.
            let claims = store.claim_due_runs(occurrence).await.unwrap();
            assert_eq!(claims.len(), 2);
            assert!(claims.iter().all(|claim| claim.run_config == config));

            let app = test::init_service(
                App::new()
                    .app_data(state.clone())
                    .configure(configure_routes),
            )
            .await;
            for (id, previous_next) in [
                (&once.id, None),
                (&ended.id, None),
                (&disabled.id, disabled.state.next_fire_at),
            ] {
                let mut expected = store.get_schedule(id).await.unwrap();
                assert!(!expected.enabled);
                assert_eq!(expected.state.next_fire_at, previous_next);
                assert_eq!(expected.run_config, config);
                let records = store.list_run_records_for_schedule(id).await;
                if let Some(claim) = claims.iter().find(|claim| claim.schedule_id == *id) {
                    assert_eq!(records.len(), 1);
                    assert_eq!(records[0].run_id, claim.run_id);
                    assert_eq!(records[0].status, ScheduleRunStatus::Queued);
                } else {
                    assert!(records.is_empty());
                }

                let uri = format!("/api/v1/schedules/{id}/prompt");
                let request = || {
                    test::TestRequest::patch()
                        .uri(&uri)
                        .set_json(serde_json::json!({ "task_message": NEW_PROMPT }))
                        .to_request()
                };
                let response = test::call_service(&app, request()).await;
                assert_eq!(response.status(), StatusCode::OK);
                let view: Value = test::read_body_json(response).await;
                assert_eq!(view["id"].as_str(), Some(id.as_str()));
                assert_eq!(view["run_config"]["task_message"], NEW_PROMPT);

                let saved = store.get_schedule(id).await.unwrap();
                expected.run_config.task_message = Some(NEW_PROMPT.into());
                assert_ne!(saved.updated_at, expected.updated_at);
                expected.updated_at = saved.updated_at;
                assert_eq!(saved, expected);
                assert_eq!(
                    view,
                    serde_json::to_value(ScheduleView::from(saved.clone())).unwrap()
                );
                assert_eq!(store.list_run_records_for_schedule(id).await, records);

                let persisted = tokio::fs::read(store.index_path()).await.unwrap();
                let repeated = test::call_service(&app, request()).await;
                assert_eq!(repeated.status(), StatusCode::OK);
                assert_eq!(test::read_body_json::<Value, _>(repeated).await, view);
                assert_eq!(
                    tokio::fs::read(store.index_path()).await.unwrap(),
                    persisted
                );
                let reopened =
                    ScheduleStore::new(store.index_path().parent().unwrap().to_path_buf())
                        .await
                        .unwrap();
                assert_eq!(reopened.get_schedule(id).await, Some(saved));
                assert_eq!(reopened.list_run_records_for_schedule(id).await, records);
            }

            let previous = claims
                .iter()
                .find(|claim| claim.schedule_id == once.id)
                .unwrap();
            let later = store.create_run_now(&once.id).await.unwrap().unwrap();
            assert_ne!(previous.run_id, later.run_id);
            assert_eq!(previous.run_config, config);
            let mut expected = config;
            expected.task_message = Some(NEW_PROMPT.into());
            assert_eq!(later.run_config, expected);
        }

        #[actix_web::test]
        async fn set_schedule_prompt_rejects_bad_payloads_and_uneditable_rows_without_writes() {
            let temp_dir = tempdir().unwrap();
            bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
            let state = web::Data::new(AppState::new(temp_dir.path().to_path_buf()).await.unwrap());
            let store = &state.schedule_store;
            let trigger = ScheduleTrigger::Once {
                at: chrono::Utc::now() + chrono::Duration::hours(2),
            };
            let task = store
                .create_schedule(
                    "Task".into(),
                    trigger.clone(),
                    false,
                    ScheduleRunConfig {
                        task_message: Some("Saved prompt".into()),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let workflow = store
                .create_schedule(
                    "Workflow".into(),
                    trigger.clone(),
                    false,
                    ScheduleRunConfig {
                        workflow_target: Some(bamboo_domain::ScheduledWorkflowTarget {
                            workflow_id: "fixture-workflow".into(),
                            revision: 1,
                            args: serde_json::json!({}),
                        }),
                        auto_execute: true,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let empty = store
                .create_schedule(
                    "Empty".into(),
                    trigger,
                    false,
                    ScheduleRunConfig {
                        task_message: Some(" \t\n\u{2003}".into()),
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
            let saved = tokio::fs::read(store.index_path()).await.unwrap();
            let uri = format!("/api/v1/schedules/{}/prompt", task.id);
            for payload in [
                r#"{"task_message":"unfinished"#,
                "{}",
                r#"["prompt"]"#,
                r#"{"task_message":null}"#,
                r#"{"task_message":7}"#,
                r#"{"task_message":" \n\t\u2003"}"#,
                r#"{"task_message":"Edited prompt","enabled":true}"#,
            ] {
                let response = test::call_service(
                    &app,
                    test::TestRequest::patch()
                        .uri(&uri)
                        .insert_header(("Content-Type", "application/json"))
                        .set_payload(payload)
                        .to_request(),
                )
                .await;
                assert_eq!(
                    response.status(),
                    StatusCode::BAD_REQUEST,
                    "payload {payload}"
                );
                let body: Value = test::read_body_json(response).await;
                assert_eq!(body["error"]["type"], "api_error");
                assert_eq!(body["error"]["code"], "invalid_schedule_prompt");
            }
            for (id, status, code) in [
                (
                    workflow.id.as_str(),
                    StatusCode::CONFLICT,
                    "schedule_prompt_changed",
                ),
                (
                    empty.id.as_str(),
                    StatusCode::CONFLICT,
                    "schedule_prompt_changed",
                ),
                ("missing", StatusCode::NOT_FOUND, "schedule_not_found"),
            ] {
                let response = test::call_service(
                    &app,
                    test::TestRequest::patch()
                        .uri(&format!("/api/v1/schedules/{id}/prompt"))
                        .set_json(serde_json::json!({ "task_message": "Edited prompt" }))
                        .to_request(),
                )
                .await;
                assert_eq!(response.status(), status);
                let body: Value = test::read_body_json(response).await;
                assert_eq!(body["error"]["type"], "api_error");
                assert_eq!(body["error"]["code"], code);
            }
            assert_eq!(tokio::fs::read(store.index_path()).await.unwrap(), saved);
            assert_eq!(store.get_schedule(&task.id).await, Some(task));
            assert_eq!(store.get_schedule(&workflow.id).await, Some(workflow));
            assert_eq!(store.get_schedule(&empty.id).await, Some(empty));
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
