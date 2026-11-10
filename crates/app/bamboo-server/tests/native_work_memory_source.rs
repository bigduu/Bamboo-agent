//! Native Jiandu memory through the public Host entry points.

use actix_web::{
    body::MessageBody,
    dev::{Service, ServiceResponse},
    http::{header, StatusCode},
    test, web, App,
};
use async_trait::async_trait;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_domain::{
    Message, ProjectId, ProjectManifest, ProjectSidebar, ProjectSidebarSection, Session,
    WorkspaceBinding, DEFAULT_SUPERVISOR_SESSION_ID,
};
use bamboo_llm::{LLMError, LLMProvider, LLMStream};
use bamboo_memory::memory_store::{
    DurableMemoryType, MemoryRetrievalInput, MemoryScope, ProjectMemoryRef,
};
use bamboo_memory::memory_store::{MemoryStore, ProjectMemoryRead};
use bamboo_server::AppState;
use bamboo_tickets::{
    canonical_bytes, Contract, Error, Operation, Principal, ProjectContextSnapshot, TicketKind,
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

#[derive(Default)]
struct NoModelProvider {
    calls: AtomicUsize,
}

#[async_trait]
impl LLMProvider for NoModelProvider {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        _model: &str,
    ) -> std::result::Result<LLMStream, LLMError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        panic!("native memory tool and source reads require no model call")
    }
}

const FIRST: &str = "section:2d567c47-a081-4a8d-9c9e-246bce470c9f";
const SECOND: &str = "section:28f595c0-21dc-4a86-8b83-5f8b258386fa";

async fn section_memory_fixture() -> (tempfile::TempDir, web::Data<AppState>, Arc<NoModelProvider>)
{
    let temp = tempfile::tempdir().unwrap();
    let provider = Arc::new(NoModelProvider::default());
    let mut config: bamboo_llm::Config = serde_json::from_value(json!({
        "provider":"openai",
        "providers":{"openai":{"api_key":"unused-local-fixture","model":"fixture-model"}}
    }))
    .unwrap();
    config
        .memory_mut()
        .get_or_insert_with(Default::default)
        .auto_dream_enabled = false;
    let state = web::Data::new(
        AppState::new_with_provider_and_memory_store(
            temp.path().join("host"),
            config,
            provider.clone(),
            MemoryStore::new(temp.path().join("jiandu")),
        )
        .await
        .unwrap(),
    );
    (temp, state, provider)
}

fn memory_request(session_id: &str, args: Value) -> actix_http::Request {
    let parameters: Vec<_> = args
        .as_object()
        .unwrap()
        .iter()
        .map(|(name, value)| json!({"name":name,"value":value.to_string()}))
        .collect();
    test::TestRequest::post()
        .uri("/v1/tools/execute")
        .set_json(json!({
            "tool_name": "memory",
            "session_id": session_id,
            "parameters": parameters,
        }))
        .to_request()
}

async fn execute_memory<B: MessageBody>(
    app: &impl Service<actix_http::Request, Response = ServiceResponse<B>, Error = actix_web::Error>,
    request: actix_http::Request,
) -> Result<Value, String> {
    let response = test::call_service(app, request).await;
    let status = response.status();
    let body = test::read_body(response).await;
    if !status.is_success() {
        return Err(String::from_utf8_lossy(&body).into_owned());
    }
    let response: Value = serde_json::from_slice(&body).unwrap();
    let payload: Value = serde_json::from_str(response["result"].as_str().unwrap()).unwrap();
    let result = payload["result"].as_str().unwrap();
    if payload["success"] == true {
        Ok(serde_json::from_str::<Value>(result).unwrap())
    } else {
        Err(result.to_owned())
    }
}

fn write_memory(scope: &str, content: &str) -> Value {
    json!({"action":"write","scope":scope,"type":"reference","title":content,"content":content})
}

async fn patch_at_current_revision<B: MessageBody>(
    app: &impl Service<actix_http::Request, Response = ServiceResponse<B>, Error = actix_web::Error>,
    uri: &str,
    body: Value,
) -> Value {
    let current = test::call_service(app, test::TestRequest::get().uri(uri).to_request()).await;
    assert_eq!(current.status(), StatusCode::OK);
    let etag = current.headers().get(header::ETAG).unwrap().clone();
    let response = test::call_service(
        app,
        test::TestRequest::patch()
            .uri(uri)
            .insert_header((header::IF_MATCH, etag))
            .set_json(body)
            .to_request(),
    )
    .await;
    let status = response.status();
    let body = test::read_body(response).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    serde_json::from_slice(&body).unwrap()
}

#[actix_web::test]
async fn native_section_memory_follows_server_assignment_and_keeps_other_scopes_available() {
    let (_temp, state, provider) = section_memory_fixture().await;
    let projects = &state.project_store;
    let project = projects.create("Section memory fixture", None).unwrap();
    let mut root = Session::new("section-root", "fixture-model");
    let mut sibling = Session::new("other-section-root", "fixture-model");
    let mut unsectioned = Session::new("unsectioned-root", "fixture-model");
    for session in [&mut root, &mut sibling, &mut unsectioned] {
        session.set_project_id_meta(project.id.to_string());
        state.storage.save_session(session).await.unwrap();
    }
    let mut child = Session::new_child_of("section-child", &root, "fixture-model", "child");
    child.set_project_id_meta(project.id.to_string());
    state.storage.save_session(&child).await.unwrap();
    let global = Session::new("projectless-root", "fixture-model");
    state.storage.save_session(&global).await.unwrap();

    let project = projects
        .update(&project.id, project.revision, |project| {
            project.sidebar = Some(ProjectSidebar {
                sections: vec![
                    ProjectSidebarSection {
                        id: FIRST.into(),
                        name: "First".into(),
                    },
                    ProjectSidebarSection {
                        id: SECOND.into(),
                        name: "Second".into(),
                    },
                ],
                session_sections: BTreeMap::from([
                    (root.id.clone(), Some(FIRST.into())),
                    (sibling.id.clone(), Some(SECOND.into())),
                    (unsectioned.id.clone(), None),
                ]),
                hidden_legacy: false,
            });
            Ok(())
        })
        .unwrap();

    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(bamboo_server::configure_routes),
    )
    .await;
    for view in ["summary", "timeline"] {
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/api/v1/metrics/memory/{view}?scope=section"))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    let execute =
        |session_id: &str, args: Value| execute_memory(&app, memory_request(session_id, args));
    let section = execute(&root.id, write_memory("section", "firstsectionneedle"))
        .await
        .unwrap();
    assert_eq!(section["memory"]["section_id"], FIRST);
    let section_id = &section["memory"]["id"];
    let lookup = json!({"action":"get","id":section_id});
    assert_eq!(
        execute(&child.id, lookup.clone()).await.unwrap()["memory"]["body"],
        "firstsectionneedle",
    );
    assert!(execute(&sibling.id, lookup.clone())
        .await
        .unwrap_err()
        .contains("memory not found"));
    assert!(execute(&unsectioned.id, lookup.clone())
        .await
        .unwrap_err()
        .contains("memory not found"));
    assert!(execute(
        &root.id,
        json!({"action":"inspect","scope":"section","section_id":SECOND})
    )
    .await
    .unwrap_err()
    .contains("section_id comes from Bamboo"));

    let shared = execute(
        &unsectioned.id,
        write_memory("project", "projectwideneedle"),
    )
    .await
    .unwrap();
    assert!(shared["memory"].get("section_id").is_none());
    assert!(execute(
        &unsectioned.id,
        write_memory("section", "unassignedsectionneedle")
    )
    .await
    .unwrap_err()
    .contains("Keep Section-only context in Session notes"));
    assert_eq!(
        execute(&root.id, json!({"action":"query","scope":"project"}))
            .await
            .unwrap()["data"]["returned_count"],
        1
    );
    assert!(execute(
        &root.id,
        json!({"action":"purge","scope":"section","id":shared["memory"]["id"]})
    )
    .await
    .unwrap_err()
    .contains("memory not found"));
    assert_eq!(
        execute(
            &root.id,
            json!({"action":"get","id":shared["memory"]["id"]})
        )
        .await
        .unwrap()["memory"]["frontmatter"]["status"],
        "active"
    );
    execute(
        &unsectioned.id,
        json!({"action":"session_append","content":"session continuity"}),
    )
    .await
    .unwrap();
    assert_eq!(
        execute(&unsectioned.id, json!({"action":"session_read"}))
            .await
            .unwrap()["content"],
        "session continuity"
    );
    let global_fact = execute(&global.id, write_memory("global", "globalneedle"))
        .await
        .unwrap();
    let global_hits = execute(
        &root.id,
        json!({
            "action":"query","scope":"global","query":"globalneedle"
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        global_hits["data"]["items"][0]["id"],
        global_fact["memory"]["id"]
    );

    let project = projects
        .update(&project.id, project.revision, |project| {
            project.sidebar.as_mut().unwrap().sections[0].name = "Renamed".into();
            Ok(())
        })
        .unwrap();
    assert_eq!(
        execute(&root.id, lookup.clone()).await.unwrap()["memory"]["body"],
        "firstsectionneedle"
    );
    let second = execute(&sibling.id, write_memory("section", "secondsectionneedle"))
        .await
        .unwrap();
    projects
        .update(&project.id, project.revision, |project| {
            project.section = Some("Legacy grouping".into());
            let sidebar = project.sidebar.as_mut().unwrap();
            sidebar
                .session_sections
                .insert(root.id.clone(), Some(SECOND.into()));
            sidebar
                .session_sections
                .insert(unsectioned.id.clone(), Some("legacy".into()));
            Ok(())
        })
        .unwrap();
    for session in [&root, &child] {
        assert!(execute(&session.id, lookup.clone())
            .await
            .unwrap_err()
            .contains("memory not found"));
        let hits = execute(
            &session.id,
            json!({
                "action":"query","scope":"section","query":"secondsectionneedle"
            }),
        )
        .await
        .unwrap();
        assert_eq!(hits["data"]["items"][0]["id"], second["memory"]["id"]);
    }
    assert!(execute(
        &unsectioned.id,
        json!({"action":"inspect","scope":"section"})
    )
    .await
    .unwrap_err()
    .contains("current server-assigned Section"));
    let retained = state
        .memory_store
        .for_section(
            &project.id,
            &bamboo_memory::memory_store::SectionId::parse(FIRST).unwrap(),
        )
        .get_memory(section_id.as_str().unwrap(), Some(project.id.as_str()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.body, "firstsectionneedle");
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
}

#[actix_web::test]
async fn recreated_supervisor_requires_explicit_section_reassignment() {
    let (temp, state, provider) = section_memory_fixture().await;
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(bamboo_server::configure_routes),
    )
    .await;
    let create_project = |name: &str| {
        let workspace = temp.path().join(name);
        std::fs::create_dir(&workspace).unwrap();
        let request = test::TestRequest::post()
            .uri("/api/v1/projects")
            .set_json(json!({"name":name,"project_path":workspace}))
            .to_request();
        let app = &app;
        async move {
            let response = test::call_service(app, request).await;
            assert_eq!(response.status(), StatusCode::CREATED);
            test::read_body_json::<ProjectManifest, _>(response).await
        }
    };
    let project = create_project("Lifetime fixture").await;
    let import_project = create_project("Browser import fixture").await;
    let project_uri = format!("/api/v1/projects/{}", project.id);
    let supervisor = DEFAULT_SUPERVISOR_SESSION_ID;
    let session_uri = format!("/api/v1/sessions/{supervisor}");
    let open_supervisor = || async {
        let receipt: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/supervisor/default")
                .peer_addr("127.0.0.1:12345".parse().unwrap())
                .to_request(),
        )
        .await;
        assert_eq!(receipt["session_id"], supervisor);
        assert_eq!(receipt["created"], true);
        receipt["incarnation_id"].clone()
    };
    let bind_project =
        |id: &ProjectId| patch_at_current_revision(&app, &session_uri, json!({"project_id":id}));
    let mut incarnation = open_supervisor().await;
    let sidebar = |section_id: Option<&str>, name: &str| ProjectSidebar {
        sections: vec![ProjectSidebarSection {
            id: FIRST.into(),
            name: name.into(),
        }],
        session_sections: BTreeMap::from([(supervisor.into(), section_id.map(str::to_owned))]),
        hidden_legacy: false,
    };
    bind_project(&import_project.id).await;
    let old_browser_snapshot = sidebar(Some(FIRST), "Browser Section");
    bind_project(&project.id).await;
    // A Project saved before the Host had birth-bound assignments.
    state
        .project_store
        .update(&project.id, project.revision, |project| {
            project.sidebar = Some(sidebar(Some(FIRST), "First"));
            Ok(())
        })
        .unwrap();
    let save_sidebar = |snapshot: ProjectSidebar| {
        patch_at_current_revision(&app, &project_uri, json!({"sidebar":snapshot}))
    };
    let execute = |args: Value| execute_memory(&app, memory_request(supervisor, args));
    let section = execute(write_memory("section", "Section lifetime fixture"))
        .await
        .unwrap();
    let shared = execute(write_memory("project", "Project lifetime fixture"))
        .await
        .unwrap();
    let get_section = json!({"action":"get","id":section["memory"]["id"]});
    let get_project = json!({"action":"get","id":shared["memory"]["id"]});
    let inspect_section = json!({"action":"inspect","scope":"section"});

    // The first deletion leaves an unbound legacy row. The second leaves a
    // saved binding for an old birth token.
    for name in ["Renamed", "Renamed again"] {
        let response = test::call_service(
            &app,
            test::TestRequest::delete().uri(&session_uri).to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let replacement = open_supervisor().await;
        bind_project(&project.id).await;
        assert_ne!(replacement, incarnation);
        incarnation = replacement;
        assert!(execute(get_section.clone())
            .await
            .unwrap_err()
            .contains("memory not found"));
        save_sidebar(sidebar(Some(FIRST), name)).await;
        assert!(execute(inspect_section.clone())
            .await
            .unwrap_err()
            .contains("Keep Section-only context in Session notes"));
        assert_eq!(
            execute(get_project.clone()).await.unwrap()["memory"]["body"],
            "Project lifetime fixture",
        );

        save_sidebar(sidebar(None, name)).await;
        let reassigned = save_sidebar(sidebar(Some(FIRST), name)).await;
        assert!(reassigned.get("sidebar_section_bindings").is_none());
        assert_eq!(
            execute(get_section.clone()).await.unwrap()["memory"]["body"],
            "Section lifetime fixture",
        );
    }
    let listed: Value = test::call_and_read_body_json(
        &app,
        test::TestRequest::get()
            .uri("/api/v1/projects")
            .to_request(),
    )
    .await;
    let entry = listed["projects"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["id"] == project.id.as_str())
        .unwrap();
    assert!(entry.get("sidebar_section_bindings").is_none());
    bind_project(&import_project.id).await;
    assert!(state
        .project_store
        .get(&import_project.id)
        .unwrap()
        .sidebar
        .is_none());
    let imported = patch_at_current_revision(
        &app,
        &format!("/api/v1/projects/{}", import_project.id),
        json!({"import_sidebar":old_browser_snapshot}),
    )
    .await;
    assert_eq!(imported["sidebar"]["session_sections"][supervisor], FIRST);
    assert!(execute(inspect_section)
        .await
        .unwrap_err()
        .contains("Keep Section-only context in Session notes"));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
}

#[actix_web::test]
async fn adopted_native_source_needs_current_project_and_never_mutates_tickets() {
    let root = tempfile::tempdir().unwrap();
    let data_root = root.path().join("host");
    let native_root = root.path().join("native-jiandu");
    let memory = MemoryStore::new(&native_root);
    let provider = Arc::new(NoModelProvider::default());
    let mut config: bamboo_llm::Config = serde_json::from_value(serde_json::json!({
        "provider":"openai", "features":{"ticket_mutation":true},
        "providers":{"openai":{"api_key":"unused-local-fixture","model":"fixture-model"}}
    }))
    .unwrap();
    config
        .memory_mut()
        .get_or_insert_with(Default::default)
        .auto_dream_enabled = false;
    let state = AppState::new_with_provider_and_memory_store(
        data_root.clone(),
        config,
        provider.clone(),
        memory.clone(),
    )
    .await
    .unwrap();
    let app = &state.tickets;
    let (service, owner) = app
        .authority(Principal::User {
            user_id: "source-owner".into(),
        })
        .await
        .unwrap();
    let projects = &state.project_store;
    let project = projects.create("Adopted Jiandu source", None).unwrap();
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).unwrap();
    let project = projects
        .bind_workspace(
            &project.id,
            project.revision,
            WorkspaceBinding {
                path: workspace
                    .canonicalize()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
                label: None,
                git_common_dir: None,
            },
        )
        .unwrap();
    let project_memory = memory.for_project(&project.id);
    let doc = project_memory
        .write_memory(
            MemoryScope::Project,
            Some(project.id.as_str()),
            DurableMemoryType::Reference,
            "Adopted Work source",
            "Canonical Project source bytes.",
            &[],
            None,
            "fixture",
            false,
            None,
        )
        .await
        .unwrap();
    let pin = ProjectMemoryRef::from_document(&doc).unwrap();
    let contract = Contract {
        title: "Source-bound Work".into(),
        objective: "Consume only the adopted version".into(),
        constraints: vec![],
        acceptance: vec!["Explicit owner acceptance".into()],
        user_acceptance_required: true,
        allowed_tools: BTreeSet::from(["Task".into()]),
    };
    let create = service
        .prepare_command(
            &owner,
            "source-create",
            vec![
                Operation::Create {
                    temp_id: "work".into(),
                    kind: TicketKind::Work,
                    parent: None,
                    contract: contract.clone(),
                    depends_on: BTreeSet::new(),
                },
                Operation::AssociateProject {
                    ticket_id: "work".into(),
                    project_id: project.id.clone(),
                },
            ],
        )
        .unwrap();
    let work = app
        .update(owner.principal().clone(), &create)
        .await
        .unwrap()
        .ids["work"]
        .clone();
    let mut context = ProjectContextSnapshot {
        project_id: project.id.clone(),
        project_revision: project.revision,
        resource_revision: project.resource_revision,
        workspace_binding: project.workspace_bindings[0].clone(),
        contract_revision: service.published().unwrap().1.tickets[&work].contract_revision,
        contract,
        sources: BTreeMap::new(),
    };
    let no_source = service
        .prepare_command(
            &owner,
            "source-context-empty",
            vec![Operation::SetProjectContext {
                work_id: work.clone(),
                context: context.clone(),
            }],
        )
        .unwrap();
    app.update(owner.principal().clone(), &no_source)
        .await
        .unwrap();
    assert!(matches!(
        state.read_work_memory_source(&work, pin.memory_id()).await,
        Err(Error::ResourceBlocked(_))
    ));
    context
        .sources
        .insert(pin.source_key(), pin.content_version().into());
    let adopt = service
        .prepare_command(
            &owner,
            "source-context-adopt",
            vec![Operation::SetProjectContext {
                work_id: work.clone(),
                context,
            }],
        )
        .unwrap();
    app.update(owner.principal().clone(), &adopt).await.unwrap();
    let before = canonical_bytes(&service.published().unwrap()).unwrap();
    match state
        .read_work_memory_source(&work, pin.memory_id())
        .await
        .unwrap()
    {
        ProjectMemoryRead::VersionMatched {
            content,
            confidence,
            ..
        } => {
            assert_eq!(content, doc.body);
            assert_eq!(confidence, None);
        }
        other => panic!("unexpected {other:?}"),
    }
    assert!(matches!(
        state
            .read_work_memory_source("unknown-work", pin.memory_id())
            .await,
        Err(Error::ScopeDenied(_))
    ));
    assert_eq!(
        canonical_bytes(&service.published().unwrap()).unwrap(),
        before
    );
    project_memory
        .merge_memory_with_retrieval(
            pin.memory_id(),
            Some(project.id.as_str()),
            "Canonical source changed.",
            &[],
            &MemoryRetrievalInput::default(),
            None,
            "fixture",
            &[],
        )
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        state
            .read_work_memory_source(&work, pin.memory_id())
            .await
            .unwrap(),
        ProjectMemoryRead::VersionMismatch { .. }
    ));
    projects
        .bump_resource_revision(&project.id, project.revision)
        .unwrap();
    assert!(matches!(
        state.read_work_memory_source(&work, pin.memory_id()).await,
        Err(Error::RevisionConflict)
    ));
    assert_eq!(
        canonical_bytes(&service.published().unwrap()).unwrap(),
        before
    );
    assert!(native_root.exists());
    assert!(!data_root.join("jiandu").exists());
    let mut supervisor = state
        .storage
        .load_root_authority(bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    supervisor.set_root_orchestration_only(false).unwrap();
    state.storage.save_session(&supervisor).await.unwrap();
    // Thinking mode does not revoke the canonical Supervisor role. The
    // already-stale Project resource fence still rejects the adopted source.
    assert!(matches!(
        state.read_work_memory_source(&work, pin.memory_id()).await,
        Err(Error::RevisionConflict)
    ));
    state.storage.delete_session(&supervisor.id).await.unwrap();
    state
        .storage
        .get_or_create_default_supervisor("replacement")
        .await
        .unwrap();
    assert!(
        matches!(
            state.read_work_memory_source(&work, pin.memory_id()).await,
            Err(Error::ScopeDenied(_))
        ),
        "a different canonical incarnation cannot borrow the old Work scope"
    );
    assert_eq!(
        canonical_bytes(&service.published().unwrap()).unwrap(),
        before
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
}
