//! Saved role selection through real Work dispatch and the native LocalTask request.
use super::*;
use async_trait::async_trait;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_domain::{Message, ProviderModelRef, ReasoningEffort};
use bamboo_llm::{LLMError, LLMProvider, LLMRequestOptions, LLMStream, ProviderRegistry};
use bamboo_server::AppState;
use bamboo_tickets::{Command, ExecutionWorkspace, TicketService};
use std::collections::HashMap;
use tokio::sync::oneshot;

const ACCOUNT: &str = "work-role-account";
const MODEL: &str = "configured-work-role-model";
const PROFILE_ACCOUNT: &str = "named-profile-account";
const PROFILE_MODEL: &str = "named-profile-model";

struct Request {
    account: &'static str,
    model: String,
    effort: Option<ReasoningEffort>,
    reply: oneshot::Sender<()>,
}

struct CaptureProvider {
    account: &'static str,
    requests: mpsc::UnboundedSender<Request>,
}

#[async_trait]
impl LLMProvider for CaptureProvider {
    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        panic!("native LocalTask must retain request options")
    }

    async fn chat_stream_with_options(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream, LLMError> {
        let (reply, receiver) = oneshot::channel();
        self.requests
            .send(Request {
                account: self.account,
                model: model.into(),
                effort: options.and_then(|options| options.reasoning_effort),
                reply,
            })
            .map_err(|_| LLMError::Api("capture closed".into()))?;
        receiver
            .await
            .map_err(|_| LLMError::Api("gate closed".into()))?;
        Ok(Box::pin(futures::stream::iter([
            Ok(LLMChunk::Token(RESULT.into())),
            Ok(LLMChunk::Done),
        ])))
    }
}

struct Fixture {
    state: AppState,
    service: Arc<TicketService>,
    start: Command,
    requests: mpsc::UnboundedReceiver<Request>,
    _home: tempfile::TempDir,
}

fn owner() -> Principal {
    Principal::User {
        user_id: "host-owner".into(),
    }
}

impl Fixture {
    async fn new(role: Option<ProviderModelRef>, fast_effort: Option<ReasoningEffort>) -> Self {
        let home = tempfile::tempdir().unwrap();
        let root = home.path().canonicalize().unwrap();
        let workspace: ExecutionWorkspace = execution_workspace(&root);
        let (send, requests) = mpsc::unbounded_channel();
        let capture = |account| -> Arc<dyn LLMProvider> {
            Arc::new(CaptureProvider {
                account,
                requests: send.clone(),
            })
        };
        let provider = capture("openai");
        let state = host(&root, provider.clone()).await;
        {
            let mut config = state.config.write().await;
            for (account, model) in [
                ("openai", native_work::MODEL),
                (ACCOUNT, MODEL),
                (PROFILE_ACCOUNT, PROFILE_MODEL),
            ] {
                config.provider_instances.insert(
                    account.into(),
                    serde_json::from_value(json!({
                        "provider_type":"openai", "enabled":true, "model":model,
                        "runtime_models":[model]
                    }))
                    .unwrap(),
                );
            }
            config.defaults = Some(serde_json::from_value(json!({
                "chat":{"provider":"openai", "model":native_work::MODEL, "reasoning_effort":"high"},
                "fast":{"provider":ACCOUNT, "model":MODEL, "reasoning_effort":fast_effort},
                "subagent_models":role.map(|role| HashMap::from([("worker", role)])).unwrap_or_default()
            })).unwrap());
        }
        let registry = ProviderRegistry::new(
            HashMap::from([
                ("openai".into(), provider.clone()),
                (ACCOUNT.into(), capture(ACCOUNT)),
                (PROFILE_ACCOUNT.into(), capture(PROFILE_ACCOUNT)),
            ]),
            "openai".into(),
        );
        registry.set_runtime_models(HashMap::from([
            ("openai".into(), vec![native_work::MODEL.into()]),
            (ACCOUNT.into(), vec![MODEL.into()]),
            (PROFILE_ACCOUNT.into(), vec![PROFILE_MODEL.into()]),
        ]));
        state.provider_registry.replace_with(registry);
        let (service, authority) = state.tickets.authority(owner()).await.unwrap();
        let binding = service.published().unwrap().1.binding;
        let mut parent = state
            .storage
            .load_root_authority(&binding.supervisor_session_id)
            .await
            .unwrap()
            .unwrap();
        parent.reasoning_effort = Some(ReasoningEffort::High);
        state.storage.save_session(&parent).await.unwrap();
        let project = state.project_store.create("Work role route", None).unwrap();
        let project = state
            .project_store
            .bind_workspace(
                &project.id,
                project.revision,
                WorkspaceBinding {
                    path: workspace.repo.clone(),
                    label: None,
                    git_common_dir: None,
                },
            )
            .unwrap();
        let contract = Contract {
            title: "Work role route".into(),
            objective: "Return one bounded result".into(),
            constraints: vec![],
            acceptance: vec!["Exact owner review".into()],
            user_acceptance_required: true,
            allowed_tools: BTreeSet::from(["Task".into()]),
        };
        let create = service
            .prepare_command(
                &authority,
                "role-create",
                vec![
                    Operation::Create {
                        temp_id: "w".into(),
                        kind: TicketKind::Work,
                        parent: None,
                        contract: contract.clone(),
                        depends_on: BTreeSet::new(),
                    },
                    Operation::AssociateProject {
                        ticket_id: "w".into(),
                        project_id: project.id.clone(),
                    },
                ],
            )
            .unwrap();
        let work = state.tickets.update(owner(), &create).await.unwrap().ids["w"].clone();
        let ready = service
            .prepare_command(
                &authority,
                "role-ready",
                vec![
                    Operation::SetProjectContext {
                        work_id: work.clone(),
                        context: ProjectContextSnapshot {
                            project_id: project.id,
                            project_revision: project.revision,
                            resource_revision: project.resource_revision,
                            workspace_binding: project.workspace_bindings[0].clone(),
                            contract_revision: 1,
                            contract,
                            sources: BTreeMap::new(),
                        },
                    },
                    Operation::Ready {
                        work_id: work.clone(),
                    },
                ],
            )
            .unwrap();
        state.tickets.update(owner(), &ready).await.unwrap();
        let start = service
            .prepare_command(
                &authority,
                "role-start",
                vec![Operation::Start {
                    work_id: work,
                    temp_id: "a".into(),
                    workspace: Some(workspace),
                }],
            )
            .unwrap();
        Self {
            state,
            service,
            start,
            requests,
            _home: home,
        }
    }

    async fn run(&mut self, expected_effort: Option<ReasoningEffort>) {
        self.run_selected(ACCOUNT, MODEL, expected_effort).await;
    }

    async fn run_selected(
        &mut self,
        account: &str,
        model: &str,
        expected_effort: Option<ReasoningEffort>,
    ) {
        let dispatched = self
            .state
            .tickets
            .dispatch(owner(), &self.start)
            .await
            .unwrap();
        assert_eq!(dispatched["errors"], json!([]), "{dispatched}");
        let request = tokio::time::timeout(WAIT, self.requests.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            request.model, model,
            "native Work must resolve its configured role"
        );
        assert_eq!(
            request.account, account,
            "the actual routed provider account must match the persisted selection"
        );
        assert_eq!(
            request.effort, expected_effort,
            "configured effort must reach the actual provider request"
        );
        let snapshot = self.service.published().unwrap().1;
        let assignment = &snapshot.assignments[dispatched["receipt"]["ids"]["a"].as_str().unwrap()];
        let child_id = assignment.runtime.as_ref().unwrap().session_id.clone();
        let child = self
            .state
            .storage
            .load_session(&child_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(child.model_ref.as_ref().unwrap().provider, account);
        assert_eq!(child.model_ref.as_ref().unwrap().model, model);
        assert_eq!(
            child.model_ref.as_ref().unwrap().reasoning_effort,
            expected_effort
        );
        assert_eq!(child.reasoning_effort, expected_effort);
        // A replay must not re-resolve the role or create a second run after a config change.
        self.state
            .config
            .write()
            .await
            .defaults
            .as_mut()
            .unwrap()
            .subagent_models
            .insert(
                "worker".into(),
                ProviderModelRef::new("missing-account", "invalid-after-birth"),
            );
        let replay = self
            .state
            .tickets
            .dispatch(owner(), &self.start)
            .await
            .unwrap();
        assert_eq!(replay["receipt"], dispatched["receipt"]);
        assert_eq!(replay["errors"], json!([]));
        let reloaded = self
            .state
            .storage
            .load_session(&child_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.model_ref, child.model_ref);
        assert_eq!(reloaded.reasoning_effort, child.reasoning_effort);
        assert_eq!(self.service.published().unwrap().1.assignments.len(), 1);
        assert!(self.requests.try_recv().is_err());
        request.reply.send(()).unwrap();
        tokio::time::timeout(WAIT, async {
            loop {
                let current = self.service.published().unwrap().1;
                if current.assignments[&assignment.id].process_stopped {
                    assert_eq!(current.submissions.len(), 1);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("native result settles before fixture shutdown");
    }
}

#[actix_web::test]
async fn native_work_retains_worker_model_and_effort_through_request_and_replay() {
    for effort in [
        Some(ReasoningEffort::Max),
        Some(ReasoningEffort::Low),
        Some(ReasoningEffort::Disabled),
        None,
    ] {
        let mut role = ProviderModelRef::new(ACCOUNT, MODEL);
        role.reasoning_effort = effort;
        Fixture::new(Some(role), Some(ReasoningEffort::High))
            .await
            .run(effort)
            .await;
    }
}

#[actix_web::test]
async fn native_work_profile_override_does_not_borrow_discarded_role_effort() {
    for hint in [
        format!("{PROFILE_ACCOUNT}:{PROFILE_MODEL}"),
        format!("{ACCOUNT}:{MODEL}"),
    ] {
        let mut fixture = Fixture::new(
            Some(
                ProviderModelRef::new(ACCOUNT, MODEL)
                    .with_reasoning_effort(ReasoningEffort::Disabled),
            ),
            Some(ReasoningEffort::Max),
        )
        .await;
        let agents = fixture.state.session_store.bamboo_home_dir().join("agents");
        std::fs::create_dir_all(&agents).unwrap();
        std::fs::write(agents.join("worker.md"), format!(
            "---\nschema_version: 1\nname: worker\ndescription: Native Work profile\nmodel_hint: {hint}\n---\nReturn the assigned bounded result.\n"
        )).unwrap();
        let (account, model) = hint.split_once(':').unwrap();
        fixture.run_selected(account, model, None).await;
    }
}

#[actix_web::test]
async fn native_work_uses_existing_fast_fallback_without_inheriting_parent_effort() {
    for effort in [Some(ReasoningEffort::Max), None] {
        Fixture::new(None, effort).await.run(effort).await;
    }
}

#[actix_web::test]
async fn native_work_rejects_unadmitted_role_before_child_creation_or_provider_call() {
    for provider in ["missing-account", "openai"] {
        let mut fixture = Fixture::new(
            Some(ProviderModelRef::new(provider, MODEL)),
            Some(ReasoningEffort::Max),
        )
        .await;
        let before = fixture.state.session_store.list_index_entries().await;
        let response = fixture
            .state
            .tickets
            .dispatch(owner(), &fixture.start)
            .await
            .unwrap();
        assert_ne!(
            response["errors"],
            json!([]),
            "invalid role must not inherit the parent's route: {response}"
        );
        assert!(fixture.requests.try_recv().is_err());
        assert_eq!(
            fixture.state.session_store.list_index_entries().await.len(),
            before.len()
        );
        assert!(fixture
            .service
            .published()
            .unwrap()
            .1
            .assignments
            .values()
            .all(|a| a.runtime.is_none()));
    }
}
