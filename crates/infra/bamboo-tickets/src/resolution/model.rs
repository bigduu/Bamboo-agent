use crate::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Frozen authenticated ingress. This record is data; it cannot construct the
/// Rust-only ingress capability consumed by TicketService.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HumanIngressRecord {
    pub user_id: String,
    pub source_ingress_seq: u64,
    pub text: String,
    pub thread_id: Option<String>,
    pub in_reply_to: Option<String>,
    pub correlation_id: Option<String>,
}

/// Construct only after the Host validates a canonical authenticated User
/// envelope/receipt. Neither model output nor client JSON deserializes this.
#[derive(Clone, Debug)]
pub struct VerifiedUserIngress {
    pub(super) message_id: String,
    pub(super) record: HumanIngressRecord,
}
impl VerifiedUserIngress {
    pub fn from_verified_host(message_id: String, record: HumanIngressRecord) -> Result<Self> {
        if message_id.is_empty()
            || message_id.len() > 128
            || record.user_id.is_empty()
            || record.source_ingress_seq == 0
            || record.text.is_empty()
            || record.text.len() > 32768
            || [
                &record.thread_id,
                &record.in_reply_to,
                &record.correlation_id,
            ]
            .into_iter()
            .flatten()
            .any(|s| s.is_empty() || s.len() > 128)
        {
            return Err(Error::InvalidTransition(
                "invalid canonical Human ingress bounds".into(),
            ));
        }
        Ok(Self { message_id, record })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolutionBasis {
    pub commit: String,
    pub seq: u64,
    pub graph_revision: u64,
    pub authority_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "ref", rename_all = "snake_case", deny_unknown_fields)]
pub enum TicketReference {
    Existing {
        id: String,
        record_revision: u64,
        contract_revision: u64,
        generation: u64,
    },
    Temporary {
        id: String,
    },
}
impl TicketReference {
    pub fn from_ticket(ticket: &Ticket) -> Self {
        Self::Existing {
            id: ticket.id.clone(),
            record_revision: ticket.record_revision,
            contract_revision: ticket.contract_revision,
            generation: ticket.generation,
        }
    }
    pub(super) fn id(&self) -> &str {
        match self {
            Self::Existing { id, .. } | Self::Temporary { id } => id,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestReference {
    pub request_id: String,
    pub work_id: String,
    pub assignment_id: Option<String>,
    pub generation: u64,
    pub contract_revision: u64,
    pub prompt_revision: u64,
}
impl RequestReference {
    pub fn from_request(request: &PendingRequest) -> Self {
        Self {
            request_id: request.id.clone(),
            work_id: request.work_id.clone(),
            assignment_id: request.assignment_id.clone(),
            generation: request.generation,
            contract_revision: request.contract_revision,
            prompt_revision: request.prompt_revision,
        }
    }
}

/// Deliberate semantic allowlist: no Runtime, Worker, effect ledger, authority,
/// import or arbitrary patch operation can be proposed by a model.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum SemanticOperation {
    Create {
        temp_id: String,
        kind: TicketKind,
        parent: Option<TicketReference>,
        contract: Contract,
        depends_on: Vec<TicketReference>,
    },
    Ready {
        target: TicketReference,
    },
    Steer {
        target: TicketReference,
        contract: Contract,
    },
    SetDependencies {
        target: TicketReference,
        depends_on: Vec<TicketReference>,
    },
    Start {
        target: TicketReference,
        temp_id: String,
        workspace: Option<ExecutionWorkspace>,
    },
    Pause {
        target: TicketReference,
        reason: String,
    },
    Cancel {
        target: TicketReference,
    },
    Retry {
        target: TicketReference,
    },
    Answer {
        target: RequestReference,
        answer: String,
    },
    DecideApproval {
        target: RequestReference,
        fingerprint: String,
        approve: bool,
    },
    Accept {
        target: TicketReference,
        submission_id: String,
        evidence: Vec<String>,
    },
    Reject {
        target: TicketReference,
        submission_id: String,
        reason: String,
    },
    Archive {
        target: TicketReference,
        archived: bool,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticGroup {
    pub group_id: String,
    pub item_ids: Vec<String>,
    /// Exact current Human text, never a candidate/tool/document quote.
    pub source_quote: String,
    pub operations: Vec<SemanticOperation>,
    pub clarification: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageProposal {
    pub groups: Vec<SemanticGroup>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResolutionCandidate {
    pub target: TicketReference,
    pub kind: TicketKind,
    pub state: WorkState,
    pub contract: Contract,
    pub requests: Vec<PendingRequest>,
    pub current_submission: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResolutionInput {
    pub message_id: String,
    pub ingress_seq: u64,
    pub human: HumanIngressRecord,
    pub basis: ResolutionBasis,
    pub candidates: Vec<ResolutionCandidate>,
    pub truncated: bool,
    pub omitted_count: usize,
    pub coverage: String,
}

pub(super) fn operation_references(op: &SemanticOperation) -> Vec<&TicketReference> {
    use SemanticOperation::*;
    match op {
        Create {
            parent, depends_on, ..
        } => parent.iter().chain(depends_on).collect(),
        SetDependencies { target, depends_on } => {
            std::iter::once(target).chain(depends_on).collect()
        }
        Ready { target }
        | Steer { target, .. }
        | Start { target, .. }
        | Pause { target, .. }
        | Cancel { target }
        | Retry { target }
        | Accept { target, .. }
        | Reject { target, .. }
        | Archive { target, .. } => vec![target],
        Answer { .. } | DecideApproval { .. } => vec![],
    }
}

pub(super) fn operation(op: &SemanticOperation) -> Operation {
    use SemanticOperation::*;
    let ids =
        |refs: &Vec<TicketReference>| refs.iter().map(|r| r.id().into()).collect::<BTreeSet<_>>();
    match op {
        Create {
            temp_id,
            kind,
            parent,
            contract,
            depends_on,
        } => Operation::Create {
            temp_id: temp_id.clone(),
            kind: *kind,
            parent: parent.as_ref().map(|r| r.id().into()),
            contract: contract.clone(),
            depends_on: ids(depends_on),
        },
        Ready { target } => Operation::Ready {
            work_id: target.id().into(),
        },
        Steer { target, contract } => Operation::UpdateContract {
            work_id: target.id().into(),
            contract: contract.clone(),
        },
        SetDependencies { target, depends_on } => Operation::SetDependencies {
            work_id: target.id().into(),
            depends_on: ids(depends_on),
        },
        Start {
            target,
            temp_id,
            workspace,
        } => Operation::Start {
            work_id: target.id().into(),
            temp_id: temp_id.clone(),
            workspace: workspace.clone(),
        },
        Pause { target, reason } => Operation::Pause {
            work_id: target.id().into(),
            reason: reason.clone(),
        },
        Cancel { target } => Operation::Cancel {
            work_id: target.id().into(),
        },
        Retry { target } => Operation::Reopen {
            work_id: target.id().into(),
        },
        Answer { target, answer } => Operation::Answer {
            request_id: target.request_id.clone(),
            prompt_revision: target.prompt_revision,
            answer: answer.clone(),
        },
        DecideApproval {
            target,
            fingerprint,
            approve,
        } => Operation::DecideApproval {
            request_id: target.request_id.clone(),
            prompt_revision: target.prompt_revision,
            fingerprint: fingerprint.clone(),
            approve: *approve,
        },
        Accept {
            target,
            submission_id,
            evidence,
        } => Operation::Accept {
            work_id: target.id().into(),
            submission_id: submission_id.clone(),
            evidence: evidence.clone(),
        },
        Reject {
            target,
            submission_id,
            reason,
        } => Operation::Reject {
            work_id: target.id().into(),
            submission_id: submission_id.clone(),
            reason: reason.clone(),
        },
        Archive { target, archived } => Operation::Archive {
            ticket_id: target.id().into(),
            archived: *archived,
        },
    }
}
