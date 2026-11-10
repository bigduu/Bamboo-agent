//! Native Jiandu memory through the public Host entry points.

use actix_web::{test, web, App};
use async_trait::async_trait;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_domain::{Message, ProjectSidebar, ProjectSidebarSection, Session, WorkspaceBinding};
use bamboo_llm::{LLMError, LLMProvider, LLMStream};
use bamboo_memory::memory_store::{
    DurableMemoryType, MemoryRetrievalInput, MemoryScope, ProjectMemoryRef,
};
use bamboo_memory::memory_store::{MemoryStore, ProjectMemoryRead};
use bamboo_server::handlers::agent::metrics;
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

#[actix_web::test]
async fn native_section_memory_follows_server_assignment_and_keeps_other_scopes_available() {
    const FIRST: &str = "section:2d567c47-a081-4a8d-9c9e-246bce470c9f";
    const SECOND: &str = "section:28f595c0-21dc-4a86-8b83-5f8b258386fa";

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
        App::new().app_data(state.clone()).service(
            web::scope("/api/v1")
                .configure(bamboo_server::handlers::tools::config)
                .route(
                    "/agent/metrics/memory/summary",
                    web::get().to(metrics::memory_summary),
                )
                .route(
                    "/agent/metrics/memory/timeline",
                    web::get().to(metrics::memory_timeline),
                ),
        ),
    )
    .await;
    for view in ["summary", "timeline"] {
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!(
                    "/api/v1/agent/metrics/memory/{view}?scope=section"
                ))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }
    let execute = |session_id: &str, args: Value| {
        let parameters: Vec<_> = args
            .as_object()
            .unwrap()
            .iter()
            .map(|(name, value)| json!({"name":name,"value":value.to_string()}))
            .collect();
        let request = test::TestRequest::post()
            .uri("/api/v1/tools/execute")
            .set_json(json!({
                "tool_name": "memory",
                "session_id": session_id,
                "parameters": parameters,
            }))
            .to_request();
        let app = &app;
        async move {
            let response = test::call_service(app, request).await;
            let status = response.status();
            let body = test::read_body(response).await;
            if !status.is_success() {
                return Err(String::from_utf8_lossy(&body).into_owned());
            }
            let response: Value = serde_json::from_slice(&body).unwrap();
            let payload: Value =
                serde_json::from_str(response["result"].as_str().unwrap()).unwrap();
            let result = payload["result"].as_str().unwrap();
            if payload["success"] == true {
                Ok(serde_json::from_str::<Value>(result).unwrap())
            } else {
                Err(result.to_owned())
            }
        }
    };
    let write = |scope: &str, content: &str| json!({"action":"write","scope":scope,"type":"reference","title":content,"content":content});
    let section = execute(&root.id, write("section", "firstsectionneedle"))
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

    let shared = execute(&unsectioned.id, write("project", "projectwideneedle"))
        .await
        .unwrap();
    assert!(shared["memory"].get("section_id").is_none());
    assert!(
        execute(&unsectioned.id, write("section", "unassignedsectionneedle"))
            .await
            .unwrap_err()
            .contains("Keep Section-only context in Session notes")
    );
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
    let global_fact = execute(&global.id, write("global", "globalneedle"))
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
    let second = execute(&sibling.id, write("section", "secondsectionneedle"))
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
