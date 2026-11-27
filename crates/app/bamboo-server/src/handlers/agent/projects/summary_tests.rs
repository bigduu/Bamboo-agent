use actix_web::{http::StatusCode, test, web, App};
use bamboo_domain::Session;
use chrono::{Duration, Utc};
use serde_json::{json, Value};

use crate::app_state::AppState;

#[actix_web::test]
async fn projects_list_reports_indexed_session_counts_and_latest_activity() {
    let data = tempfile::tempdir().unwrap();
    let state = web::Data::new(
        Box::pin(AppState::new(data.path().to_path_buf()))
            .await
            .unwrap(),
    );
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(crate::routes::agent_routes),
    )
    .await;
    let list_projects = || {
        test::TestRequest::get()
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .uri("/api/v1/projects")
            .to_request()
    };
    let response = test::call_service(&app, list_projects()).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = test::read_body_json(response).await;
    assert_eq!(body, json!({"projects": []}));

    let empty = state.project_store.create("Empty Project", None).unwrap();
    let single = state.project_store.create("Single Session", None).unwrap();
    let multiple = state
        .project_store
        .create("Multiple Sessions", None)
        .unwrap();
    let archived = state
        .project_store
        .create("Archived Project", None)
        .unwrap();
    let archived = state
        .project_store
        .archive(&archived.id, archived.revision)
        .unwrap();

    let now = Utc::now();
    let single_activity = now - Duration::minutes(40);
    let archived_activity = now - Duration::minutes(30);
    let multiple_latest = now - Duration::minutes(20);
    for (id, project, activity) in [
        ("single-root", Some(&single), single_activity),
        ("multiple-newer", Some(&multiple), multiple_latest),
        (
            "multiple-older",
            Some(&multiple),
            now - Duration::minutes(50),
        ),
        ("archived-root", Some(&archived), archived_activity),
        ("unassigned-root", None, now - Duration::minutes(10)),
    ] {
        let mut session = Session::new(id, "test-model");
        session.created_at = activity - Duration::minutes(1);
        session.updated_at = activity;
        let project_id = project.map(|project| project.id.to_string());
        if let Some(project_id) = &project_id {
            session.set_project_id_meta(project_id.clone());
        }
        state.storage.save_session(&session).await.unwrap();
        let indexed = state.session_store.get_index_entry(id).await.unwrap();
        assert_eq!(indexed.project_id, project_id);
        assert_eq!(indexed.last_activity_at, activity);
    }

    let response = test::call_service(&app, list_projects()).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = test::read_body_json(response).await;
    let projects = body["projects"].as_array().unwrap();
    assert_eq!(projects.len(), 4);
    for (project, count, activity) in [
        (empty, 0, None),
        (single, 1, Some(single_activity)),
        (multiple, 2, Some(multiple_latest)),
        (archived, 1, Some(archived_activity)),
    ] {
        let summary = projects
            .iter()
            .find(|summary| summary["id"] == project.id.as_str())
            .unwrap();
        let mut expected = json!(project.into_public_view());
        expected["session_count"] = json!(count);
        expected["last_activity_at"] = json!(activity);
        assert_eq!(summary, &expected);
    }
}
