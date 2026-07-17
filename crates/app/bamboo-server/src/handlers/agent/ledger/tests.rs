//! Unit tests for the ledger HTTP core logic, run against a temp-dir
//! `LedgerStore` (no `AppState` needed — the actix entry points only add
//! store construction on top of the `*_core` functions tested here).

use actix_web::body::to_bytes;
use actix_web::http::StatusCode;
use actix_web::HttpResponse;
use chrono::{Duration, Utc};

use bamboo_domain::ledger::{LedgerScope, RecordStatus};
use bamboo_memory::ledger_store::LedgerStore;

use super::handlers::{
    agenda_core, delete_record_core, list_records_core, patch_record_core, upsert_record_core,
};
use super::types::{
    AgendaQuery, ListRecordsQuery, LocateRecordQuery, PatchRecordRequest, UpsertRecordRequest,
};

async fn response_json(response: HttpResponse) -> (StatusCode, serde_json::Value) {
    let status = response.status();
    let bytes = to_bytes(response.into_body()).await.expect("read body");
    let value = serde_json::from_slice(&bytes).expect("body is JSON");
    (status, value)
}

fn temp_store() -> (tempfile::TempDir, LedgerStore) {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = LedgerStore::new(dir.path());
    (dir, store)
}

fn upsert(title: &str) -> UpsertRecordRequest {
    UpsertRecordRequest {
        title: Some(title.to_string()),
        ..UpsertRecordRequest::default()
    }
}

async fn create_record(store: &LedgerStore, req: UpsertRecordRequest) -> serde_json::Value {
    let response = upsert_record_core(store, req).await.expect("upsert");
    let (status, body) = response_json(response).await;
    assert_eq!(status, StatusCode::OK, "create failed: {body}");
    body
}

#[actix_web::test]
async fn post_creates_record_with_user_actor_and_defaults() {
    let (_dir, store) = temp_store();
    let body = create_record(
        &store,
        UpsertRecordRequest {
            due_at: Some("2026-08-01T09:00:00Z".to_string()),
            tags: Some(vec!["Errands".to_string()]),
            ..upsert("Renew passport")
        },
    )
    .await;

    assert_eq!(body["result"], "create");
    let record = &body["record"];
    assert!(record["id"].as_str().unwrap().starts_with("rec_"));
    assert_eq!(record["title"], "Renew passport");
    assert_eq!(record["kind"], "todo");
    assert_eq!(record["status"], "open");
    assert_eq!(record["scope"], "global");
    assert_eq!(record["time"]["due_at"], "2026-08-01T09:00:00Z");
    assert_eq!(record["tags"][0], "errands");

    // HTTP-created records are user-authored (the tool layer writes as agent).
    // `source` is elided from JSON when it equals the default, so check the
    // typed record instead.
    let id = record["id"].as_str().unwrap();
    let doc = store
        .get_record(LedgerScope::Global, None, id)
        .await
        .expect("get")
        .expect("record exists");
    assert_eq!(
        doc.record.source.created_by,
        bamboo_domain::ledger::RecordActor::User
    );
}

#[actix_web::test]
async fn post_without_title_is_bad_request() {
    let (_dir, store) = temp_store();
    let response = upsert_record_core(&store, UpsertRecordRequest::default())
        .await
        .expect("upsert");
    let (status, body) = response_json(response).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"]["message"].as_str().unwrap().contains("title"));
}

#[actix_web::test]
async fn post_with_bad_datetime_is_bad_request() {
    let (_dir, store) = temp_store();
    let response = upsert_record_core(
        &store,
        UpsertRecordRequest {
            due_at: Some("next tuesday".to_string()),
            ..upsert("Bad date")
        },
    )
    .await
    .expect("upsert");
    let (status, body) = response_json(response).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"]["message"]
        .as_str()
        .unwrap()
        .contains("due_at"));
}

#[actix_web::test]
async fn post_project_scope_without_key_is_bad_request() {
    let (_dir, store) = temp_store();
    let response = upsert_record_core(
        &store,
        UpsertRecordRequest {
            scope: Some("project".to_string()),
            ..upsert("Project item")
        },
    )
    .await
    .expect("upsert");
    let (status, body) = response_json(response).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"]["message"]
        .as_str()
        .unwrap()
        .contains("project_key"));
}

#[actix_web::test]
async fn post_with_existing_id_updates_instead_of_creating() {
    let (_dir, store) = temp_store();
    let created = create_record(&store, upsert("Draft report")).await;
    let id = created["record"]["id"].as_str().unwrap().to_string();

    let body = create_record(
        &store,
        UpsertRecordRequest {
            id: Some(id.clone()),
            priority: Some("high".to_string()),
            ..UpsertRecordRequest::default()
        },
    )
    .await;
    assert_eq!(body["result"], "update");
    assert_eq!(body["record"]["id"], id.as_str());
    assert_eq!(body["record"]["title"], "Draft report");
    assert_eq!(body["record"]["priority"], "high");
}

#[actix_web::test]
async fn list_filters_by_status_and_reports_matched() {
    let (_dir, store) = temp_store();
    let first = create_record(&store, upsert("First")).await;
    create_record(&store, upsert("Second")).await;
    let first_id = first["record"]["id"].as_str().unwrap();
    store
        .transition_record(
            LedgerScope::Global,
            None,
            first_id,
            RecordStatus::Done,
            None,
        )
        .await
        .expect("transition")
        .expect("record exists");

    // Default listing hides terminal records.
    let response = list_records_core(&store, ListRecordsQuery::default())
        .await
        .expect("list");
    let (status, body) = response_json(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["matched"], 1);
    assert_eq!(body["records"][0]["title"], "Second");

    // Explicit status filter surfaces the done record.
    let response = list_records_core(
        &store,
        ListRecordsQuery {
            status: Some("done".to_string()),
            ..ListRecordsQuery::default()
        },
    )
    .await
    .expect("list");
    let (_, body) = response_json(response).await;
    assert_eq!(body["matched"], 1);
    assert_eq!(body["records"][0]["title"], "First");
}

#[actix_web::test]
async fn list_project_scope_without_key_degrades_to_global() {
    let (_dir, store) = temp_store();
    create_record(&store, upsert("Global item")).await;

    let response = list_records_core(
        &store,
        ListRecordsQuery {
            scope: Some("project".to_string()),
            ..ListRecordsQuery::default()
        },
    )
    .await
    .expect("list");
    let (status, body) = response_json(response).await;
    assert_eq!(status, StatusCode::OK, "forgiving scope must not error");
    assert_eq!(body["matched"], 1);
    assert_eq!(body["records"][0]["title"], "Global item");
}

#[actix_web::test]
async fn list_spans_global_and_project_scopes() {
    let (_dir, store) = temp_store();
    create_record(&store, upsert("Personal")).await;
    create_record(
        &store,
        UpsertRecordRequest {
            scope: Some("project".to_string()),
            project_key: Some("proj-a".to_string()),
            ..upsert("Work item")
        },
    )
    .await;

    let response = list_records_core(
        &store,
        ListRecordsQuery {
            project_key: Some("proj-a".to_string()),
            ..ListRecordsQuery::default()
        },
    )
    .await
    .expect("list");
    let (_, body) = response_json(response).await;
    assert_eq!(body["matched"], 2);

    let response = list_records_core(
        &store,
        ListRecordsQuery {
            scope: Some("global".to_string()),
            project_key: Some("proj-a".to_string()),
            ..ListRecordsQuery::default()
        },
    )
    .await
    .expect("list");
    let (_, body) = response_json(response).await;
    assert_eq!(body["matched"], 1);
    assert_eq!(body["records"][0]["title"], "Personal");
}

#[actix_web::test]
async fn list_rejects_unknown_scope() {
    let (_dir, store) = temp_store();
    let response = list_records_core(
        &store,
        ListRecordsQuery {
            scope: Some("universe".to_string()),
            ..ListRecordsQuery::default()
        },
    )
    .await
    .expect("list");
    let (status, _) = response_json(response).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[actix_web::test]
async fn patch_updates_fields_and_transitions_status_with_history() {
    let (_dir, store) = temp_store();
    let created = create_record(&store, upsert("Ship release")).await;
    let id = created["record"]["id"].as_str().unwrap().to_string();

    let response = patch_record_core(
        &store,
        &id,
        LocateRecordQuery::default(),
        PatchRecordRequest {
            title: Some("Ship release v2".to_string()),
            status: Some("done".to_string()),
            reason: Some("shipped".to_string()),
            ..PatchRecordRequest::default()
        },
    )
    .await
    .expect("patch");
    let (status, body) = response_json(response).await;
    assert_eq!(status, StatusCode::OK);
    let record = &body["record"];
    assert_eq!(record["title"], "Ship release v2");
    assert_eq!(record["status"], "done");
    // The status change went through transition_record: history is recorded.
    assert_eq!(record["transitions"][0]["to_status"], "done");
    assert_eq!(record["transitions"][0]["reason"], "shipped");
}

#[actix_web::test]
async fn patch_unknown_record_is_not_found() {
    let (_dir, store) = temp_store();
    let response = patch_record_core(
        &store,
        "rec_missing",
        LocateRecordQuery::default(),
        PatchRecordRequest::default(),
    )
    .await
    .expect("patch");
    let (status, body) = response_json(response).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["record_id"], "rec_missing");
}

#[actix_web::test]
async fn patch_locates_project_record_via_query_key() {
    let (_dir, store) = temp_store();
    let created = create_record(
        &store,
        UpsertRecordRequest {
            scope: Some("project".to_string()),
            project_key: Some("proj-a".to_string()),
            ..upsert("Project task")
        },
    )
    .await;
    let id = created["record"]["id"].as_str().unwrap().to_string();

    // Without the project_key the record is invisible…
    let response = patch_record_core(
        &store,
        &id,
        LocateRecordQuery::default(),
        PatchRecordRequest::default(),
    )
    .await
    .expect("patch");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // …with it, the project scope is searched.
    let response = patch_record_core(
        &store,
        &id,
        LocateRecordQuery {
            project_key: Some("proj-a".to_string()),
        },
        PatchRecordRequest {
            priority: Some("critical".to_string()),
            ..PatchRecordRequest::default()
        },
    )
    .await
    .expect("patch");
    let (status, body) = response_json(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["record"]["priority"], "critical");
}

#[actix_web::test]
async fn delete_cancels_instead_of_removing() {
    let (_dir, store) = temp_store();
    let created = create_record(&store, upsert("Obsolete task")).await;
    let id = created["record"]["id"].as_str().unwrap().to_string();

    let response = delete_record_core(&store, &id, LocateRecordQuery::default())
        .await
        .expect("delete");
    let (status, body) = response_json(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["success"], true);
    assert_eq!(body["record"]["status"], "cancelled");
    assert_eq!(
        body["record"]["transitions"][0]["reason"],
        "deleted via API"
    );

    // The document survives (never hard-deleted).
    let doc = store
        .get_record(LedgerScope::Global, None, &id)
        .await
        .expect("get")
        .expect("record still on disk");
    assert_eq!(doc.record.status, RecordStatus::Cancelled);

    // Deleting again is an idempotent success.
    let response = delete_record_core(&store, &id, LocateRecordQuery::default())
        .await
        .expect("delete twice");
    let (status, body) = response_json(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["success"], true);
}

#[actix_web::test]
async fn delete_unknown_record_is_not_found() {
    let (_dir, store) = temp_store();
    let response = delete_record_core(&store, "rec_nope", LocateRecordQuery::default())
        .await
        .expect("delete");
    let (status, _) = response_json(response).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[actix_web::test]
async fn agenda_buckets_overdue_and_upcoming() {
    let (_dir, store) = temp_store();
    let now = Utc::now();
    create_record(
        &store,
        UpsertRecordRequest {
            due_at: Some((now - Duration::days(1)).to_rfc3339()),
            ..upsert("Late item")
        },
    )
    .await;
    create_record(
        &store,
        UpsertRecordRequest {
            due_at: Some((now + Duration::days(3)).to_rfc3339()),
            ..upsert("Later this week")
        },
    )
    .await;
    create_record(&store, upsert("Undated idea")).await;

    let response = agenda_core(&store, AgendaQuery::default())
        .await
        .expect("agenda");
    let (status, body) = response_json(response).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["overdue"][0]["title"], "Late item");
    assert_eq!(body["upcoming"][0]["title"], "Later this week");
    assert_eq!(body["undated"][0]["title"], "Undated idea");
}

#[actix_web::test]
async fn agenda_clamps_horizon() {
    let (_dir, store) = temp_store();
    let now = Utc::now();
    create_record(
        &store,
        UpsertRecordRequest {
            due_at: Some((now + Duration::days(3)).to_rfc3339()),
            ..upsert("Three days out")
        },
    )
    .await;

    // horizon_days=0 clamps to 1: the 3-days-out item falls outside it.
    let response = agenda_core(
        &store,
        AgendaQuery {
            horizon_days: Some(0),
            ..AgendaQuery::default()
        },
    )
    .await
    .expect("agenda");
    let (_, body) = response_json(response).await;
    assert_eq!(body["upcoming"].as_array().map(Vec::len).unwrap_or(0), 0);

    // horizon_days=999 clamps to 31 and still works.
    let response = agenda_core(
        &store,
        AgendaQuery {
            horizon_days: Some(999),
            ..AgendaQuery::default()
        },
    )
    .await
    .expect("agenda");
    let (_, body) = response_json(response).await;
    assert_eq!(body["upcoming"][0]["title"], "Three days out");
}
