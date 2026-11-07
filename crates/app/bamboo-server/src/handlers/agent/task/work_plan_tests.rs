use actix_web::{http::StatusCode, test, web, App};
use bamboo_domain::{Session, TaskItem, TaskItemStatus, TaskList};
use bamboo_engine::ticket_worker_plan::TICKET_LOCAL_PLAN_KEY;
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
        child
            .metadata
            .insert(TICKET_LOCAL_PLAN_KEY.into(), "work-assignment".into());
    }
    child.task_list = has_child_plan.then(|| plan(&child.id, "worker-only"));
    child.set_task_list_version_meta("7");
    state.storage.save_session(&child).await.unwrap();
    assert!(
        !state.sessions.contains_key(&child.id),
        "exercise the persisted read path"
    );
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(crate::routes::configure_routes),
    )
    .await;

    for prefix in ["/api/v1/task", "/api/v1/sessions"] {
        let path = if prefix.ends_with("sessions") {
            format!("{prefix}/{}/task", child.id)
        } else {
            format!("{prefix}/{}", child.id)
        };
        let response =
            test::call_service(&app, test::TestRequest::get().uri(&path).to_request()).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = test::read_body_json(response).await;
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

        let exists_path = if prefix.ends_with("sessions") {
            format!("{path}/exists")
        } else {
            format!("{prefix}/{}/exists", child.id)
        };
        let response = test::call_service(
            &app,
            test::TestRequest::get().uri(&exists_path).to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = test::read_body_json(response).await;
        assert_eq!(body["session_id"], expected.id);
        assert_eq!(body["has_task_list"], !local_work_plan || has_child_plan);
    }
    state.shutdown().await;
}

#[actix_web::test]
async fn native_work_task_queries_return_the_child_plan_and_revision() {
    check_child_task_queries(true, true).await;
}

#[actix_web::test]
async fn native_work_without_local_tasks_does_not_inherit_supervisor_purpose() {
    check_child_task_queries(true, false).await;
}

#[actix_web::test]
async fn ordinary_child_task_queries_keep_the_existing_shared_root_plan() {
    check_child_task_queries(false, true).await;
}
