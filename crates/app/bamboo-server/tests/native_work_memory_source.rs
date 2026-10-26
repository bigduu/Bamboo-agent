//! Actual public Host read of an explicitly adopted native Project source.
//! No provider calls, automatic adoption or execution-status changes.
use async_trait::async_trait;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_domain::{Message, WorkspaceBinding};
use bamboo_llm::{LLMError, LLMProvider, LLMStream};
use bamboo_memory::memory_store::{
    DurableMemoryType, MemoryRetrievalInput, MemoryScope, ProjectMemoryRef,
};
use bamboo_memory::memory_store::{MemoryStore, ProjectMemoryRead};
use bamboo_server::AppState;
use bamboo_tickets::{
    canonical_bytes, Contract, Error, Operation, Principal, ProjectContextSnapshot, TicketKind,
};
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
        panic!("adopted-source Host reads must not call a model")
    }
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
    assert!(matches!(
        state.read_work_memory_source(&work, pin.memory_id()).await,
        Err(Error::ScopeDenied(_))
    ));
    assert_eq!(
        canonical_bytes(&service.published().unwrap()).unwrap(),
        before
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
}
