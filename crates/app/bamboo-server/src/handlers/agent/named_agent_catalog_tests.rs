use super::*;
use actix_web::{test, App};
use bamboo_config::AccessControlConfig;
use bamboo_domain::{ProjectId, Session};
use bamboo_engine::{external_agents::actor_adapter::CodexRunTokenAuthority, SessionSnapshot};
use std::{fs, sync::Arc};

fn local(uri: &str) -> test::TestRequest {
    test::TestRequest::get()
        .uri(uri)
        .peer_addr("127.0.0.1:1234".parse().unwrap())
        .insert_header((header::HOST, "localhost"))
}

fn profile(home: &std::path::Path, name: &str, body: &str) {
    fs::create_dir_all(home.join("agents")).unwrap();
    fs::write(home.join("agents/private-filename.md"), format!("---\nschema_version: 1\nname: {name}\ndescription: Public description\nmodel_hint: provider:private-hint\ntools:\n  allow: [Read]\n---\n{body}")).unwrap();
}

#[actix_web::test]
async fn actual_authenticated_route_uses_durable_project_without_workspace_or_cache_authority() {
    let _key = bamboo_config::encryption::set_test_encryption_key([0x38; 32]);
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().canonicalize().unwrap();
    let state = web::Data::new(AppState::new(home.clone()).await.unwrap());
    let id = ProjectId::parse("durable-project").unwrap();
    let manifest = state
        .project_store
        .create_with_id(id.clone(), "No workspace", None)
        .unwrap();
    assert!(manifest.project_path.is_none());
    assert!(manifest.workspace_bindings.is_empty());
    profile(&home, "reviewer", "GLOBAL_PROTECTED_BODY");
    profile(
        &state.project_store.paths().project_home(&id),
        "reviewer",
        "PROJECT_PROTECTED_BODY",
    );
    let mut session = Session::new("catalog-session", "model");
    session.set_project_id_meta(id.as_str());
    state.storage.save_session(&session).await.unwrap();
    let mut cached = session.clone();
    cached.set_project_id_meta("foreign-project");
    state
        .sessions
        .insert(session.id.clone(), Arc::new(SessionSnapshot::new(cached)));
    let main = home.join("sessions/catalog-session/session.json");
    let before = fs::read(&main).unwrap();
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(crate::agent_routes),
    )
    .await;
    let uri = "/api/v1/sessions/catalog-session/named-agent-profiles";
    let response = test::call_service(&app, local(uri).to_request()).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CACHE_CONTROL).unwrap(),
        "private, no-store"
    );
    let response_bytes = test::read_body(response).await;
    assert!(response_bytes.len() <= NamedAgentLimits::default().max_publication_bytes);
    let body: serde_json::Value = serde_json::from_slice(&response_bytes).unwrap();
    assert_eq!(body["status"], "available");
    let selected = body["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["status"] == "selectable" && entry["identity"]["name"] == "reviewer")
        .unwrap();
    assert_eq!(selected["identity"]["source"], "project");
    assert_eq!(selected["identity"]["project_id"], id.as_str());
    assert_eq!(fs::read(main).unwrap(), before);
    let serialized = body.to_string();
    for private in [
        "PROTECTED_BODY",
        "private-filename",
        "private-hint",
        "\"tools\"",
        "foreign-project",
        "Builtin role package v1:",
        home.to_str().unwrap(),
    ] {
        assert!(!serialized.contains(private));
    }
    let forged = test::call_service(
        &app,
        local(&format!("{uri}?project_id=foreign-project")).to_request(),
    )
    .await;
    assert_eq!(forged.status(), StatusCode::BAD_REQUEST);
    let remote = test::call_service(
        &app,
        test::TestRequest::get()
            .uri(uri)
            .peer_addr("203.0.113.9:1234".parse().unwrap())
            .insert_header((header::HOST, "localhost"))
            .to_request(),
    )
    .await;
    assert_eq!(remote.status(), StatusCode::UNAUTHORIZED); // Remote Open is not authenticated host authority.
    let (device, token) = crate::handlers::settings::issue_device_token("catalog-device");
    let device_id = device.device_id.clone();
    state.config.write().await.access_control = Some(AccessControlConfig {
        devices: vec![device],
        ..Default::default()
    });
    let authenticated = test::call_service(
        &app,
        test::TestRequest::get()
            .uri(uri)
            .peer_addr("203.0.113.9:1234".parse().unwrap())
            .insert_header((header::HOST, "bamboo.example.com"))
            .insert_header((header::AUTHORIZATION, format!("Bearer {token}")))
            .insert_header(("x-device-id", device_id))
            .to_request(),
    )
    .await;
    assert_eq!(authenticated.status(), StatusCode::OK);
    let codex = state.codex_run_tokens.issue(&session.id).unwrap();
    let direct = test::init_service(App::new().app_data(state.clone()).route(
        "/api/v1/sessions/{session_id}/named-agent-profiles",
        web::get().to(handler),
    ))
    .await;
    for prefix in ["Bearer ", "bearer ", "Bearer   "] {
        let request = || {
            local(uri)
                .insert_header((header::AUTHORIZATION, format!("{prefix}{}", codex.token)))
                .to_request()
        };
        assert_eq!(
            test::call_service(&direct, request()).await.status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            test::call_service(&app, request()).await.status(),
            StatusCode::UNAUTHORIZED
        );
    }
}

#[actix_web::test]
async fn durable_missing_invalid_archived_foreign_and_unrecoverable_project_fail_closed() {
    let _key = bamboo_config::encryption::set_test_encryption_key([0x38; 32]);
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().canonicalize().unwrap();
    let state = web::Data::new(AppState::new(home.clone()).await.unwrap());
    profile(&home, "global", "PRIVATE_GLOBAL_BODY");
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(crate::agent_routes),
    )
    .await;
    state.sessions.insert(
        "absent".into(),
        Arc::new(SessionSnapshot::new(Session::new("absent", "model"))),
    );
    let missing = test::call_service(
        &app,
        local("/api/v1/sessions/absent/named-agent-profiles").to_request(),
    )
    .await;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    for (session_id, project_id) in [
        ("invalid-project", "../private-path"),
        ("missing-project", "missing"),
    ] {
        let mut session = Session::new(session_id, "model");
        session.set_project_id_meta(project_id);
        state.storage.save_session(&session).await.unwrap();
        let rejected = test::call_service(
            &app,
            local(&format!(
                "/api/v1/sessions/{session_id}/named-agent-profiles"
            ))
            .to_request(),
        )
        .await;
        assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            test::read_body_json::<serde_json::Value, _>(rejected).await,
            serde_json::json!({"error": crate::error::error_value("project_unavailable")})
        );
    }
    let wrong = Session::new("wrong-session", "model");
    state.storage.save_session(&wrong).await.unwrap();
    let path = home.join("sessions/wrong-session/session.json");
    let mut raw: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    raw["id"] = serde_json::json!("foreign-session");
    fs::write(path, serde_json::to_vec(&raw).unwrap()).unwrap();
    let rejected = test::call_service(
        &app,
        local("/api/v1/sessions/wrong-session/named-agent-profiles").to_request(),
    )
    .await;
    assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        test::read_body_json::<serde_json::Value, _>(rejected).await,
        serde_json::json!({"error": crate::error::error_value("session_unavailable")})
    );
    assert!(!home.join("projects/missing").exists());
    let projects = ["archived", "foreign", "corrupt"].map(|kind| {
        let id = ProjectId::parse(kind).unwrap();
        let manifest = state
            .project_store
            .create_with_id(id.clone(), kind, None)
            .unwrap();
        (kind, id, manifest)
    });
    for (kind, id, manifest) in projects {
        let mut session = Session::new(format!("{kind}-session"), "model");
        session.set_project_id_meta(id.as_str());
        state.storage.save_session(&session).await.unwrap();
        let path = state.project_store.paths().manifest_path(&id);
        match kind {
            "archived" => {
                state.project_store.archive(&id, manifest.revision).unwrap();
            }
            "foreign" => {
                let mut foreign = manifest;
                foreign.id = ProjectId::parse("other-project").unwrap();
                fs::write(&path, serde_json::to_vec(&foreign).unwrap()).unwrap();
            }
            _ => {
                fs::write(&path, "PRIVATE_CORRUPT_MANIFEST").unwrap();
            }
        }
        let backup = path.with_file_name("project.json.bak");
        if kind != "archived" && backup.exists() {
            fs::remove_file(backup).unwrap();
        }
        let rejected = test::call_service(
            &app,
            local(&format!(
                "/api/v1/sessions/{kind}-session/named-agent-profiles"
            ))
            .to_request(),
        )
        .await;
        assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            test::read_body_json::<serde_json::Value, _>(rejected).await,
            serde_json::json!({"error": crate::error::error_value("project_unavailable")})
        );
    }
}

#[actix_web::test]
async fn existing_project_backup_recovery_is_usable_and_catalog_creates_no_agents_directory() {
    let _key = bamboo_config::encryption::set_test_encryption_key([0x38; 32]);
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().canonicalize().unwrap();
    let state = web::Data::new(AppState::new(home.clone()).await.unwrap());
    let id = ProjectId::parse("recovered").unwrap();
    let manifest = state
        .project_store
        .create_with_id(id.clone(), "Recoverable", None)
        .unwrap();
    state
        .project_store
        .update(&id, manifest.revision, |candidate| {
            candidate.description = Some("Updated".into());
            Ok(())
        })
        .unwrap();
    let path = state.project_store.paths().manifest_path(&id);
    assert!(path.with_file_name("project.json.bak").exists());
    fs::write(&path, "PRIVATE_CORRUPT_PRIMARY").unwrap();
    let mut session = Session::new("recovered-session", "model");
    session.set_project_id_meta(id.as_str());
    state.storage.save_session(&session).await.unwrap();
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(crate::agent_routes),
    )
    .await;
    let response = test::call_service(
        &app,
        local("/api/v1/sessions/recovered-session/named-agent-profiles").to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = test::read_body_json(response).await;
    assert_eq!(body["status"], "available");
    let entries = body["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 3);
    let names: std::collections::BTreeSet<_> = entries
        .iter()
        .map(|entry| {
            assert_eq!(entry["status"], "selectable");
            assert_eq!(entry["identity"]["source"], "builtin");
            assert!(entry["identity"]["project_id"].is_null());
            assert_eq!(entry["identity"]["revision"].as_str().unwrap().len(), 64);
            entry["identity"]["name"].as_str().unwrap()
        })
        .collect();
    assert_eq!(
        names,
        std::collections::BTreeSet::from(["explorer", "implementer", "reviewer"])
    );
    assert!(!body.to_string().contains("Builtin role package v1:"));
    assert_eq!(state.project_store.get(&id).unwrap().id, id); // Existing get recovered the authority; the catalog added no recovery protocol.
    assert!(!home.join("agents").exists());
    assert!(!state
        .project_store
        .paths()
        .project_home(&id)
        .join("agents")
        .exists());
}

#[actix_web::test]
async fn app_state_homes_remain_distinct_and_anonymous_project_source_disables_http_selection() {
    let _key = bamboo_config::encryption::set_test_encryption_key([0x38; 32]);
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    for (temp, name) in [(&first, "first"), (&second, "second")] {
        let home = temp.path().canonicalize().unwrap();
        let state = web::Data::new(AppState::new(home.clone()).await.unwrap());
        profile(&home, name, "PRIVATE_BODY");
        let session = Session::new("same-session", "model");
        state.storage.save_session(&session).await.unwrap();
        let catalog = named_agent_catalog::discover(&state, &session.id, Default::default())
            .await
            .unwrap();
        let identity = catalog
            .metadata()
            .entries
            .iter()
            .find_map(|row| {
                row.identity
                    .as_ref()
                    .filter(|identity| identity.name == name)
            })
            .unwrap();
        assert_eq!(
            identity.source,
            bamboo_skills::named_agents::NamedAgentProfileSource::Global
        );
        assert_eq!(identity.project_id, None);
        let id = ProjectId::parse("assigned").unwrap();
        state
            .project_store
            .create_with_id(id.clone(), "Assigned", None)
            .unwrap();
        let project_home = state.project_store.paths().project_home(&id);
        profile(&project_home, "hidden", "api_key: DO_NOT_ECHO");
        let mut assigned = session;
        assigned.set_project_id_meta(id.as_str());
        assigned.metadata_version = assigned.metadata_version.checked_add(1).unwrap();
        state.storage.save_session(&assigned).await.unwrap();
        let app =
            test::init_service(App::new().app_data(state).configure(crate::agent_routes)).await;
        let response = test::call_service(
            &app,
            local("/api/v1/sessions/same-session/named-agent-profiles").to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(body["status"], "unavailable");
        assert!(body["entries"]
            .as_array()
            .unwrap()
            .iter()
            .all(|entry| entry["status"] != "selectable"));
        for private in [
            "hidden",
            "DO_NOT_ECHO",
            "api_key",
            "PRIVATE_BODY",
            "private-filename",
        ] {
            assert!(!body.to_string().contains(private));
        }
    }
}
