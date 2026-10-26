//! Actual Native Host installs the budget on provider entry; only the model is local.
use actix_web::{http::StatusCode, test, web, App};
use async_trait::async_trait;
use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolSchema};
use bamboo_domain::{Message, WorkspaceBinding};
use bamboo_llm::{provider::ProviderModelInfo, LLMChunk, LLMError, LLMProvider, LLMStream};
use bamboo_tickets::{
    Authority, Contract, Operation, Principal, ProjectContextSnapshot, StepStatus, TicketKind,
    WorkState, WorkTokenBudget, WorkTokenLimits,
};
use futures::StreamExt;
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::mpsc;

#[macro_use]
#[path = "common/native_work.rs"]
mod native_work;
use native_work::{execution_workspace, host, LocalProvider, MODEL, WAIT};
const INPUT: u32 = 65_536;
const OUTPUT: u32 = 1_024;
const CAP: u64 = INPUT as u64 + OUTPUT as u64;

struct Metered(Arc<LocalProvider>);
#[async_trait]
impl LLMProvider for Metered {
    async fn list_model_info(&self) -> Result<Vec<ProviderModelInfo>, LLMError> {
        Ok(vec![ProviderModelInfo {
            id: MODEL.into(),
            max_context_tokens: Some(INPUT),
            max_output_tokens: Some(OUTPUT),
        }])
    }
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        cap: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        assert_eq!(cap, Some(OUTPUT));
        let stream = self.0.chat_stream(messages, tools, cap, model).await?;
        Ok(Box::pin(stream.chain(futures::stream::iter([Ok(
            LLMChunk::ProviderUsage {
                input_tokens: Some(20),
                output_tokens: Some(10),
                total_tokens: Some(30),
                reasoning_tokens: Some(5),
                cache_creation_input_tokens: None,
                cache_read_input_tokens: Some(10),
                cache_write_input_tokens: None,
            },
        )]))))
    }
}
fn ledger(service: &bamboo_tickets::TicketService, _authority: &Authority) -> WorkTokenBudget {
    service
        .work_token_budget(&Authority::from_verified_host(
            service.published().unwrap().1.binding,
            Principal::Runtime,
        ))
        .unwrap()
        .unwrap()
}

#[actix_web::test]
async fn native_host_reserves_before_calls_and_blocks_exhausted_continuation() {
    for exhausted in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let physical = root.path().canonicalize().unwrap();
        let workspace = execution_workspace(&physical);
        let (send, mut requests) = mpsc::unbounded_channel();
        let local = Arc::new(LocalProvider {
            requests: send,
            calls: AtomicUsize::new(0),
            max_requests: 2,
        });
        let state = web::Data::new(host(&physical, Arc::new(Metered(local.clone()))).await);
        state.config.write().await.extra.insert("model_limits".into(), json!([{
            "model_pattern": MODEL, "max_context_tokens": INPUT, "max_output_tokens": OUTPUT, "safety_margin": 1000
        }]));
        let (service, owner) = state
            .tickets
            .authority(Principal::User {
                user_id: "budget-owner".into(),
            })
            .await
            .unwrap();
        let manifest = state
            .project_store
            .create("Native token budget", None)
            .unwrap();
        let manifest = state
            .project_store
            .bind_workspace(
                &manifest.id,
                manifest.revision,
                WorkspaceBinding {
                    path: workspace.repo.clone(),
                    label: None,
                    git_common_dir: None,
                },
            )
            .unwrap();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(bamboo_server::configure_routes),
        )
        .await;
        let contract = Contract {
            title: "Budgeted native Work".into(),
            objective: "One attributable bounded Task".into(),
            constraints: vec!["Respect owner token limits".into()],
            acceptance: vec!["Exact result for owner review".into()],
            user_acceptance_required: true,
            allowed_tools: BTreeSet::from(["Task".into()]),
        };
        let create = service
            .prepare_command(
                &owner,
                "budget-create",
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
                        project_id: manifest.id.clone(),
                    },
                ],
            )
            .unwrap();
        let created = owner_post!(app, "/api/v1/tickets/update", &create, StatusCode::OK);
        let work = created["ids"]["work"].as_str().unwrap().to_owned();
        let ready = service
            .prepare_command(
                &owner,
                "budget-ready",
                vec![
                    Operation::SetProjectContext {
                        work_id: work.clone(),
                        context: ProjectContextSnapshot {
                            project_id: manifest.id.clone(),
                            project_revision: manifest.revision,
                            resource_revision: manifest.resource_revision,
                            workspace_binding: manifest.workspace_bindings[0].clone(),
                            contract_revision: service.published().unwrap().1.tickets[&work]
                                .contract_revision,
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
        owner_post!(app, "/api/v1/tickets/update", &ready, StatusCode::OK);
        let limit = if exhausted { CAP } else { CAP + 1000 };
        service
            .configure_work_token_budget(
                &owner,
                0,
                WorkTokenLimits {
                    work_tokens: BTreeMap::from([(work.clone(), limit)]),
                    utc_day_tokens: limit,
                    total_tokens: limit,
                },
            )
            .unwrap();
        let start = service
            .prepare_command(
                &owner,
                "budget-start",
                vec![Operation::Start {
                    work_id: work.clone(),
                    temp_id: "assignment".into(),
                    workspace: Some(workspace),
                }],
            )
            .unwrap();
        let dispatched = owner_post!(app, "/api/v1/tickets/dispatch", &start, StatusCode::OK);
        assert_eq!(dispatched["errors"], json!([]));
        let assignment = dispatched["receipt"]["ids"]["assignment"]
            .as_str()
            .unwrap()
            .to_owned();
        let first = tokio::time::timeout(WAIT, requests.recv())
            .await
            .unwrap()
            .unwrap();
        let budget = ledger(&service, &owner);
        assert_eq!(budget.reservations.len(), 1);
        assert_eq!(budget.total_charged, CAP);
        let reserved = budget.reservations.values().next().unwrap();
        assert_eq!(reserved.request.input_cap, INPUT);
        assert_eq!(reserved.request.output_cap, OUTPUT);
        assert_eq!(
            reserved.request.attempt.as_ref().unwrap().assignment_id,
            assignment
        );
        assert!(
            service.published().unwrap().1.assignments[&assignment]
                .runtime
                .is_some(),
            "actual Native admission"
        );
        first.reply.send(LLMChunk::ToolCalls(vec![ToolCall { id: "budget-task".into(), tool_type: "function".into(), function: FunctionCall { name: "Task".into(), arguments: json!({"tasks":[{"id":"step","content":"Finish the attributable plan","status":"completed"}]}).to_string() } }])).unwrap();
        if !exhausted {
            let second = tokio::time::timeout(WAIT, requests.recv())
                .await
                .unwrap()
                .unwrap();
            let budget = ledger(&service, &owner);
            assert_eq!(
                budget.total_charged,
                CAP + 30,
                "first actual usage plus second full cap"
            );
            assert_eq!(budget.reservations.len(), 2);
            assert!(second
                .messages
                .iter()
                .any(|m| m.tool_call_id.as_deref() == Some("budget-task")
                    && m.tool_success == Some(true)));
            second
                .reply
                .send(LLMChunk::Token("Actual bounded native result".into()))
                .unwrap();
        }
        tokio::time::timeout(WAIT, async {
            loop {
                let snapshot = service.published().unwrap().1;
                let failed_terminal = if exhausted {
                    let child = &snapshot.assignments[&assignment]
                        .runtime
                        .as_ref()
                        .unwrap()
                        .session_id;
                    state
                        .storage
                        .load_runtime_control_plane(child)
                        .await
                        .unwrap()
                        .unwrap()
                        .last_run_status()
                        .as_deref()
                        == Some("error")
                } else {
                    false
                };
                if snapshot.assignments[&assignment].process_stopped
                    && (failed_terminal
                        || (!exhausted && snapshot.tickets[&work].state == WorkState::Submitted))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let snapshot = service.published().unwrap().1;
        let budget = ledger(&service, &owner);
        assert_eq!(
            snapshot.assignments[&assignment].plan.steps[0].status,
            Some(StepStatus::Completed),
            "actual Task committed before the continuation"
        );
        if exhausted {
            assert_eq!(local.calls.load(Ordering::SeqCst), 1);
            assert_eq!(budget.reservations.len(), 1);
            assert_eq!(budget.total_charged, 30);
            assert!(snapshot.tickets[&work].current_submission.is_none());
            let child = state
                .storage
                .load_runtime_control_plane(
                    &snapshot.assignments[&assignment]
                        .runtime
                        .as_ref()
                        .unwrap()
                        .session_id,
                )
                .await
                .unwrap()
                .unwrap();
            assert!(
                child
                    .last_run_error()
                    .is_some_and(|error| error.contains("Work token budget")
                        && error.contains("admission blocked: Work")),
                "actual terminal cause: {:?}",
                child.last_run_error()
            );
        } else {
            assert_eq!(local.calls.load(Ordering::SeqCst), 2);
            assert_eq!(budget.total_charged, 60);
            assert_eq!(snapshot.tickets[&work].state, WorkState::Submitted);
        }
        assert!(budget
            .reservations
            .values()
            .all(|r| r.request.work_id == work && r.request.project_id == manifest.id));
        state.shutdown().await;
    }
    eprintln!("native_work_token_budget: actual Host reservation/usage/exhaustion PASS, local provider=3, external=0");
}
