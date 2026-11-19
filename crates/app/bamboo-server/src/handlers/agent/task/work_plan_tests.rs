use actix_web::{http::StatusCode, test as http_test, web, App};
use bamboo_domain::{Session, TaskItem, TaskItemStatus, TaskList};
use bamboo_engine::{
    ticket_runtime::TICKET_DISPATCH_KEY, ticket_worker_plan::TICKET_LOCAL_PLAN_KEY,
};
use serde_json::Value;

fn plan(session_id: &str, item_id: &str) -> TaskList {
    TaskList {
        session_id: session_id.into(),
        title: format!("Plan for {session_id}"),
        items: vec![TaskItem {
            id: item_id.into(),
            description: format!("Purpose for {item_id}"),
            status: TaskItemStatus::InProgress,
            ..Default::default()
        }],
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    }
}

async fn check_child_task_queries(local_work_plan: bool, has_child_plan: bool) {
    let home = tempfile::tempdir().unwrap();
    bamboo_config::paths::init_bamboo_dir(home.path().to_path_buf());
    let state = web::Data::new(crate::AppState::new(home.path().into()).await.unwrap());
    let mut root = Session::new("task-query-root", "fixture-model");
    root.task_list = Some(plan(&root.id, "supervisor-only"));
    root.set_task_list_version_meta("91");
    state.storage.save_session(&root).await.unwrap();
    let mut child = Session::new_child_of("task-query-child", &root, "fixture-model", "Worker");
    if local_work_plan {
        // This read-only fixture is the projection installed by the existing
        // trusted Work Task adapter; it grants no execution capability.
        let key = if has_child_plan {
            TICKET_LOCAL_PLAN_KEY
        } else {
            TICKET_DISPATCH_KEY
        };
        child.metadata.insert(key.into(), "work-assignment".into());
    }
    child.task_list = has_child_plan.then(|| plan(&child.id, "worker-only"));
    child.set_task_list_version_meta("7");
    state.storage.save_session(&child).await.unwrap();
    assert!(
        !state.sessions.contains_key(&child.id),
        "exercise the persisted read path"
    );
    let app = http_test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(crate::routes::configure_routes),
    )
    .await;

    for path in [
        format!("/api/v1/task/{}", child.id),
        format!("/api/v1/sessions/{}/task", child.id),
    ] {
        let response =
            http_test::call_service(&app, http_test::TestRequest::get().uri(&path).to_request())
                .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = http_test::read_body_json(response).await;
        let expected = if local_work_plan { &child } else { &root };
        assert_eq!(body["session_id"], expected.id, "{path}: {body}");
        assert_eq!(body["version"], if local_work_plan { 7 } else { 91 });
        if local_work_plan && !has_child_plan {
            assert!(body["title"].is_null());
            assert_eq!(body["items"], serde_json::json!([]));
            assert_eq!(body["progress"]["total"], 0);
        } else {
            assert_eq!(
                body["items"][0]["id"],
                if local_work_plan {
                    "worker-only"
                } else {
                    "supervisor-only"
                }
            );
            assert_eq!(body["progress"]["total"], 1);
        }

        let exists_path = format!("{path}/exists");
        let response = http_test::call_service(
            &app,
            http_test::TestRequest::get().uri(&exists_path).to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = http_test::read_body_json(response).await;
        assert_eq!(body["session_id"], expected.id);
        assert_eq!(body["has_task_list"], !local_work_plan || has_child_plan);
    }
    state.shutdown().await;
}

#[test]
fn native_work_task_queries_return_the_child_plan_and_revision() {
    let future = Box::pin(check_child_task_queries(true, true));
    actix_web::rt::System::new().block_on(future);
}

#[test]
fn native_work_without_local_tasks_does_not_inherit_supervisor_purpose() {
    let future = Box::pin(check_child_task_queries(true, false));
    actix_web::rt::System::new().block_on(future);
}

#[test]
fn ordinary_child_task_queries_keep_the_existing_shared_root_plan() {
    let future = Box::pin(check_child_task_queries(false, true));
    actix_web::rt::System::new().block_on(future);
}
