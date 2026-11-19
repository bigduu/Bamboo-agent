//! A bounded review projection of one exact submission and its recorded run.
//! Task progress and recorded source hashes are evidence for the reviewer.
use std::{
    collections::HashSet,
    path::{Component, Path, PathBuf},
};

use bamboo_domain::{Message, MessagePhase, Role, Session};
use bamboo_tickets::{
    content_hash, Error, ExecutionWorkspace, FileReply, ReadEnvelope, Result, Submission,
    SubmissionFacts,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const MAX_PLAN_STEPS: usize = 16;
const MAX_ARTIFACTS: usize = 4;
const MAX_READ_RECORDS: usize = 8;
const MAX_RUN_MESSAGES: usize = 512;
const MAX_READ_BYTES: usize = 16_384;
const MAX_PATH_BYTES: usize = 1024;

pub struct Request<'a> {
    pub work_id: &'a str,
    pub submission_id: &'a str,
    pub fixed_commit: Option<&'a str>,
    pub source_path: Option<&'a str>,
    pub budget_bytes: usize,
}

#[derive(Serialize)]
struct RecordedRead<'a> {
    message_id: &'a str,
    sha256: String,
    page: Option<bamboo_tickets::ReadPageInfo>,
}

#[derive(Deserialize)]
struct ReadArguments {
    file_path: String,
}

fn relative_path(path: &str, workspace: Option<&ExecutionWorkspace>) -> Option<PathBuf> {
    if path.len() > MAX_PATH_BYTES {
        return None;
    }
    let mut path = Path::new(path);
    if path.is_absolute() {
        path = path.strip_prefix(&workspace?.worktree).ok()?;
    }
    let mut relative = PathBuf::new();
    for part in path.components() {
        match part {
            Component::Normal(name) => relative.push(name),
            Component::CurDir => {}
            _ => return None,
        }
    }
    (!relative.as_os_str().is_empty()).then_some(relative)
}

/// Resolve only the message named by the ledger's native result checkpoint.
/// Retained sessions may have been compacted or may have newer messages.
fn terminal_message(session: &Session, submission: &Submission) -> Option<usize> {
    let receipt = &submission.runtime;
    let kind = if session.kind == bamboo_domain::SessionKind::Root {
        "Root"
    } else {
        "Child"
    };
    let prefix = format!(
        "canonical {kind} {}; run {}; message ",
        receipt.session_id, receipt.run_id
    );
    let (message_id, sha256) = submission
        .evidence
        .iter()
        .filter_map(|evidence| evidence.strip_prefix(&prefix))
        .find_map(|evidence| evidence.split_once("; sha256 "))?;
    if !submission
        .artifacts
        .iter()
        .any(|artifact| artifact.sha256 == sha256)
    {
        return None;
    }
    session.messages.iter().position(|message| {
        message.id == message_id
            && message.role == Role::Assistant
            && message.phase != Some(MessagePhase::Commentary)
            && message.tool_calls.as_ref().is_none_or(Vec::is_empty)
            && content_hash(message.content.as_bytes()) == sha256
    })
}

fn recorded_reads<'a>(
    messages: &'a [Message],
    workspace: Option<&ExecutionWorkspace>,
    path: &Path,
) -> (Vec<RecordedRead<'a>>, bool) {
    let first = messages.len().saturating_sub(MAX_RUN_MESSAGES);
    let mut pending_reads = HashSet::new();
    let mut records = Vec::new();
    for message in &messages[first..] {
        if message.role == Role::Assistant {
            pending_reads.clear();
            for call in message.tool_calls.iter().flatten() {
                if call.function.name != "Read" || call.function.arguments.len() > MAX_READ_BYTES {
                    continue;
                }
                let matches = serde_json::from_str::<ReadArguments>(&call.function.arguments)
                    .ok()
                    .and_then(|args| relative_path(&args.file_path, workspace))
                    .is_some_and(|candidate| candidate == path);
                if matches {
                    pending_reads.insert(call.id.as_str());
                }
            }
        }
        if message.role != Role::Tool
            || !message
                .tool_call_id
                .as_deref()
                .is_some_and(|id| pending_reads.remove(id))
            || message.tool_success != Some(true)
            || message.content.len() > MAX_READ_BYTES
        {
            continue;
        }
        let Ok(reply) = serde_json::from_str::<FileReply>(&message.content) else {
            continue;
        };
        let Some(content) = reply.content else {
            continue;
        };
        let valid = match &reply.page {
            Some(page) => {
                page.end_offset_bytes.checked_sub(page.offset_bytes) == Some(content.len() as u64)
                    && page.end_offset_bytes <= page.total_bytes
            }
            None => content_hash(content.as_bytes()) == reply.sha256,
        };
        if !valid
            || reply.sha256.len() != 64
            || !reply.sha256.bytes().all(|b| b.is_ascii_hexdigit())
        {
            continue;
        }
        if records.len() == MAX_READ_RECORDS {
            return (records, true);
        }
        records.push(RecordedRead {
            message_id: &message.id,
            sha256: reply.sha256,
            page: reply.page,
        });
    }
    (records, first > 0)
}

/// The adapter supplies an already verified Session birth and runtime receipt.
/// Missing retained execution evidence still permits inspection of ledger facts.
pub fn project(
    facts: &ReadEnvelope<SubmissionFacts>,
    session: Option<&Session>,
    source_path: Option<&str>,
    budget_bytes: usize,
) -> Result<Value> {
    if !(128..=65_536).contains(&budget_bytes) {
        return Err(Error::InvalidTransition("inspect budget 128..65536".into()));
    }
    let facts_data = &facts.data;
    let assignment = &facts_data.assignment;
    let submission = &facts_data.submission;
    let workspace = assignment.workspace.as_ref();
    let path = source_path
        .map(|path| {
            relative_path(path, workspace)
                .ok_or_else(|| Error::InvalidTransition("source_path must name a Work file".into()))
        })
        .transpose()?;
    let terminal = session.and_then(|session| {
        let position = terminal_message(session, submission)?;
        Some((&session.messages[position], &session.messages[..position]))
    });
    let source_reads = path.as_deref().map(|path| match terminal {
        Some((_, history)) => {
            let (records, truncated) = recorded_reads(history, workspace, path);
            json!({
                "path": path,
                "status": if records.is_empty() { "no_retained_read" } else { "recorded" },
                "records": records,
                "truncated": truncated,
            })
        }
        None => json!({"path": path, "status": "execution_evidence_unavailable"}),
    });
    let plan = &assignment.plan;
    let steps: Vec<_> = plan
        .steps
        .iter()
        .take(MAX_PLAN_STEPS)
        .map(|step| {
            let title: String = step.title.chars().take(160).collect();
            json!({
                "id": step.id, "parent": step.parent, "title": title,
                "title_truncated": title.len() < step.title.len(),
                "status": step.status, "completed": step.completed,
            })
        })
        .collect();
    let artifacts = &submission.artifacts[..submission.artifacts.len().min(MAX_ARTIFACTS)];
    let value = json!({
        "snapshot": facts.snapshot,
        "work_id": submission.work_id,
        "work_state": facts_data.work_state,
        "submission_id": submission.id,
        "current": facts_data.current,
        "accepted": facts_data.accepted,
        "stale": submission.stale,
        "assignment_id": assignment.id,
        "generation": submission.generation,
        "contract_revision": submission.contract_revision,
        "runtime": submission.runtime,
        "process_stopped": assignment.process_stopped,
        "committed_plan": {
            "revision": plan.plan_revision,
            "omitted_steps": plan.steps.len() - steps.len(),
            "steps": steps,
        },
        "artifacts": artifacts,
        "omitted_artifacts": submission.artifacts.len() - artifacts.len(),
        "terminal_message": terminal.map(|(message, _)| json!({
            "message_id": message.id,
            "sha256": content_hash(message.content.as_bytes()),
        })),
        "source_reads": source_reads,
    });
    if serde_json::to_vec(&value)?.len() > budget_bytes {
        return Err(Error::ContextBudgetExceeded);
    }
    Ok(value)
}
