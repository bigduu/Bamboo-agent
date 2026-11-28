//! Publishes the sealed result of a Project's designated Ticket Creator.
use super::project_manager::{coordinator, operation_key};
use super::*;
use bamboo_domain::{ProjectId, Session, SessionCallerReference};
use serde::Deserialize;

struct DraftRequest {
    title: String,
    objective: String,
    acceptance: Vec<String>,
}

#[derive(Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum CreatorReport {
    Draft {
        title: String,
        objective: String,
        acceptance: Vec<String>,
        facts: Vec<String>,
        decisions: Vec<String>,
        open_questions: Vec<String>,
    },
    NeedsInput {
        question: String,
    },
}

#[async_trait::async_trait]
impl bamboo_engine::session_app::child_session::ticket_creator::TicketDraftPublisher
    for TicketApplication
{
    async fn publish_draft(
        &self,
        parent: &Session,
        child: &Session,
        context: &bamboo_domain::ChildContextBinding,
        report: Value,
    ) -> std::result::Result<Value, String> {
        let unavailable =
            |reason: String| Ok(json!({"view":"ticket_draft","available":false,"reason":reason}));
        let report: CreatorReport = match serde_json::from_value(report) {
            Ok(report) => report,
            Err(_) => return unavailable("Ticket Creator returned an invalid draft".into()),
        };
        let CreatorReport::Draft {
            title,
            objective,
            acceptance,
            facts,
            decisions,
            open_questions,
        } = report
        else {
            let CreatorReport::NeedsInput { question } = report else {
                unreachable!()
            };
            if question.trim().is_empty() || question.len() > 1024 {
                return unavailable("Ticket Creator returned an invalid question".into());
            }
            return Ok(json!({"view":"ticket_draft","available":false,"question":question}));
        };
        let caller = SessionCallerReference::from(parent);
        let (project, current) = match self.resolve_project_caller(&caller).await {
            Ok(binding) => binding,
            Err(error) => return unavailable(error.to_string()),
        };
        if child.project_id_meta().as_deref() != Some(project.id.as_str())
            || context.payload.parent_session_id != parent.id
            || current
                .agent_runtime_state
                .as_ref()
                .is_some_and(|state| state.read_only || state.plan_mode.is_some())
        {
            return unavailable("Ticket Creator Project or permission changed".into());
        }
        let operation_id = match uuid::Uuid::parse_str(&child.id) {
            Ok(id) => id,
            Err(_) => return unavailable("Ticket Creator has an invalid Session ID".into()),
        };
        let key = operation_key("ticket-creator-v1", &caller, &project.id, operation_id)
            .map_err(|error| error.to_string())?;
        let brief = DraftBrief {
            source_session_id: parent.id.clone(),
            facts,
            decisions,
            open_questions: open_questions.clone(),
            required_files: BTreeSet::new(),
            sources: context
                .payload
                .sources
                .iter()
                .map(|source| DraftSource {
                    message_id: source.message_id.clone(),
                    content_sha256: source.content_sha256.clone(),
                })
                .collect(),
        };
        for attempt in 0..3 {
            let request = DraftRequest {
                title: title.clone(),
                objective: objective.clone(),
                acceptance: acceptance.clone(),
            };
            match self
                .write_creator_draft(&caller, &project.id, key.clone(), request, brief.clone())
                .await
            {
                Ok(result) => {
                    return Ok(json!({
                        "view":"ticket_draft", "available":true, "work_id":result["work_id"],
                        "title":title, "state":"draft", "project_id":result["project_id"],
                        "execution_section_id":null, "open_questions":open_questions,
                        "placement":"Project Work board. Execution Section is recorded at dispatch.",
                    }))
                }
                Err(Error::RevisionConflict) if attempt < 2 => continue,
                Err(Error::RevisionConflict | Error::Io(_)) => {
                    return Err("Ticket draft publication must be reconciled".into());
                }
                Err(error) => return unavailable(error.to_string()),
            }
        }
        unreachable!()
    }
}

impl TicketApplication {
    async fn write_creator_draft(
        &self,
        caller: &SessionCallerReference,
        project_id: &ProjectId,
        key: String,
        request: DraftRequest,
        brief: DraftBrief,
    ) -> Result<Value> {
        let (service, authority) = self.authority(coordinator()).await?;
        let snapshot = service.published()?.1;
        let draft = "draft";
        let operations = vec![
            Operation::Create {
                temp_id: draft.into(),
                kind: TicketKind::Work,
                parent: None,
                contract: Contract {
                    title: request.title,
                    objective: request.objective,
                    constraints: vec![format!("Reported by Project Session {}", caller.session_id)],
                    acceptance: request.acceptance,
                    user_acceptance_required: true,
                    allowed_tools: BTreeSet::from(["Task".into()]),
                },
                depends_on: BTreeSet::new(),
            },
            Operation::AssociateProject {
                ticket_id: draft.into(),
                project_id: project_id.clone(),
            },
            Operation::SetDraftBrief {
                work_id: draft.into(),
                brief,
            },
        ];
        let mut command = Command {
            operation_id: key.clone(),
            binding: snapshot.binding.clone(),
            expected_seq: snapshot.seq,
            expected_epoch: snapshot.authority_epoch,
            operations,
            source: None,
        };
        if let Some(receipt) = snapshot.receipts.get(&key) {
            let original: Command = serde_json::from_str(&receipt.canonical_request)?;
            command.expected_seq = original.expected_seq;
            command.expected_epoch = original.expected_epoch;
        }
        let host = self.managed_work_host.get().ok_or_else(|| {
            Error::AuthorityUnavailable("Project Work Host is unavailable".into())
        })?;
        // A Session move or permission change uses this same writer lock.
        // Keep the verified caller in scope until the draft is committed.
        let _guard = host.persistence.acquire_lock(&caller.session_id).await;
        let (project, current) = self.resolve_project_caller(caller).await?;
        if &project.id != project_id
            || current
                .agent_runtime_state
                .as_ref()
                .is_some_and(|state| state.read_only || state.plan_mode.is_some())
        {
            return Err(Error::ScopeDenied("Session Project changed".into()));
        }
        let receipt = self.update(authority.principal().clone(), &command).await?;
        Ok(json!({"project_id": project_id, "work_id": receipt.ids.get(draft), "receipt": receipt}))
    }
}
