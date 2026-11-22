//! Resolve continuations from the authorized Work ledger and its exact session.

use std::{
    collections::HashMap,
    path::Path,
    time::{Duration, Instant},
};

use bamboo_domain::{ProjectId, Session, SessionKind};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    ticket_runtime, Assignment, Error, Health, Principal, Result, Snapshot, Ticket,
    TicketApplication, TicketKind, WorkState,
};

const OFFER_TTL: Duration = Duration::from_secs(10 * 60);
const MAX_OFFERS: usize = 64;

#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkSelector {
    pub project_id: ProjectId,
    pub work_id: String,
    pub generation: u64,
    pub record_revision: u64,
    pub authority_epoch: u64,
}

#[derive(Serialize)]
pub(crate) struct PreparedOffer {
    pub offer_id: Uuid,
    pub cwd: String,
}

#[derive(Clone)]
struct Offer {
    selector: WorkSelector,
    scope_id: String,
    cwd: String,
    expires_at: Instant,
}

#[derive(Default)]
pub(super) struct Offers(parking_lot::Mutex<HashMap<Uuid, Offer>>);

fn unavailable() -> Error {
    Error::ResourceBlocked(
        "Codex handoff expired or was already claimed; prepare a new handoff".into(),
    )
}

impl Offers {
    fn create(
        &self,
        selector: WorkSelector,
        scope_id: String,
        cwd: String,
    ) -> Result<PreparedOffer> {
        let now = Instant::now();
        let mut offers = self.0.lock();
        offers.retain(|_, offer| offer.expires_at > now && offer.selector != selector);
        if offers.len() >= MAX_OFFERS {
            return Err(Error::ResourceBlocked(
                "too many pending Codex handoffs".into(),
            ));
        }
        let offer_id = Uuid::new_v4();
        offers.insert(
            offer_id,
            Offer {
                selector,
                scope_id,
                cwd: cwd.clone(),
                expires_at: now + OFFER_TTL,
            },
        );
        Ok(PreparedOffer { offer_id, cwd })
    }

    fn pending(&self, id: Uuid) -> Result<Offer> {
        let mut offers = self.0.lock();
        offers.retain(|_, offer| offer.expires_at > Instant::now());
        offers.get(&id).cloned().ok_or_else(unavailable)
    }

    fn consume(&self, id: Uuid) -> Result<()> {
        self.0
            .lock()
            .remove(&id)
            .filter(|offer| offer.expires_at > Instant::now())
            .map(|_| ())
            .ok_or_else(unavailable)
    }
}

fn select_work<'a>(
    snapshot: &'a Snapshot,
    selector: &WorkSelector,
) -> Result<(&'a Ticket, Option<&'a Assignment>)> {
    if selector.work_id.is_empty() || selector.work_id.len() > 256 {
        return Err(Error::InvalidTransition("invalid Work ID".into()));
    }
    let work = snapshot
        .tickets
        .get(&selector.work_id)
        .filter(|work| {
            work.kind == TicketKind::Work
                && work.scope_id == snapshot.binding.scope_id
                && snapshot.project.links.get(&work.id) == Some(&selector.project_id)
        })
        .ok_or_else(|| Error::ScopeDenied("Work does not belong to the selected Project".into()))?;
    if work.generation != selector.generation
        || work.record_revision != selector.record_revision
        || snapshot.authority_epoch != selector.authority_epoch
    {
        return Err(Error::RevisionConflict);
    }
    if work.archived || matches!(work.state, WorkState::Accepted | WorkState::Cancelled) {
        return Err(Error::InvalidTransition("Work is closed".into()));
    }
    let attempt = snapshot
        .assignments
        .values()
        .filter(|attempt| {
            attempt.work_id == work.id
                && attempt.contract_revision == work.contract_revision
                && attempt.generation <= work.generation
        })
        .max_by_key(|attempt| (attempt.generation, attempt.updated_seq));
    Ok((work, attempt))
}

impl TicketApplication {
    async fn handoff_cwd(
        &self,
        snapshot: &Snapshot,
        work: &Ticket,
        attempt: Option<&Assignment>,
        project: &ProjectId,
    ) -> Result<String> {
        let key = attempt
            .and_then(|item| snapshot.project.assignment_contexts.get(&item.id))
            .or_else(|| snapshot.project.work_contexts.get(&work.id));
        let context = key
            .and_then(|key| snapshot.project.contexts.get(key))
            .filter(|context| {
                &context.project_id == project
                    && context.contract_revision == work.contract_revision
            })
            .ok_or_else(|| {
                Error::ResourceBlocked("Work needs a current Project workspace binding".into())
            })?;
        self.validate_project_context(project, Some(context), false)?;
        let binding = Path::new(&context.workspace_binding.path);
        let workspace = attempt.and_then(|attempt| attempt.workspace.as_ref());
        let selected = workspace
            .map(|workspace| Path::new(&workspace.worktree))
            .unwrap_or(binding);
        let unavailable =
            || Error::ResourceBlocked("the Work workspace is unavailable on this host".into());
        if !selected.is_dir() || !binding.is_dir() {
            return Err(unavailable());
        }
        let checkout = selected.canonicalize().map_err(|_| unavailable())?;
        snapshot.verify_handoff_checkout_released(attempt, &checkout)?;
        if let Some(workspace) = workspace {
            bamboo_engine::ticket_worker_plan::files::verify_continuation_workspace(
                workspace.clone(),
            )
            .await?;
        }
        if selected != binding {
            let binding_git =
                bamboo_projects::resolve_git_common_dir(binding).map_err(|_| unavailable())?;
            let selected_git =
                bamboo_projects::resolve_git_common_dir(selected).map_err(|_| unavailable())?;
            if binding_git.is_none() || binding_git != selected_git {
                return Err(Error::ScopeDenied(
                    "Work checkout no longer matches the selected Project".into(),
                ));
            }
        }
        bamboo_projects::canonicalize_workspace_path(selected).map_err(|_| unavailable())
    }

    async fn continuation_session(
        &self,
        snapshot: &Snapshot,
        attempt: &Assignment,
    ) -> Result<Option<Session>> {
        let Some(receipt) = attempt.runtime.as_ref() else {
            return Ok(None);
        };
        let Some(intent) = snapshot
            .intents
            .get(&attempt.dispatch_key)
            .filter(|intent| {
                intent.assignment_id == attempt.id
                    && intent.dispatch_key == receipt.dispatch_key
                    && intent.spec_hash == receipt.spec_hash
                    && intent.receipt.as_ref() == Some(receipt)
                    && intent.immutable_spec.generation == attempt.generation
                    && intent.immutable_spec.contract_revision == attempt.contract_revision
            })
        else {
            return Ok(None);
        };
        let Some(session) = self
            .storage
            .load_execution_admission_snapshot(&receipt.session_id)
            .await?
        else {
            return Ok(None);
        };
        let Some(dispatch) = ticket_runtime::read_dispatch(&session)? else {
            return Ok(None);
        };
        if session.id != receipt.session_id
            || dispatch.binding != snapshot.binding
            || dispatch.assignment_id != attempt.id
            || dispatch.dispatch_key != receipt.dispatch_key
            || dispatch.spec_hash != receipt.spec_hash
        {
            return Ok(None);
        }
        let exact = if let Some(target) = &intent.immutable_spec.managed_root {
            intent.managed_admission.as_ref().is_some_and(|admission| {
                admission.target_session_id == session.id
                    && admission.target_created_at == session.created_at
                    && admission.run_id == receipt.run_id
                    && bamboo_orchestration::managed_work::matches_root(
                        target,
                        &intent.immutable_spec,
                        &session,
                    )
            })
        } else {
            session.kind == SessionKind::Child
                && session.id == ticket_runtime::ticket_child_id(&receipt.dispatch_key)
                && session.parent_session_id.as_deref()
                    == Some(snapshot.binding.supervisor_session_id.as_str())
                && session.root_session_id == snapshot.binding.supervisor_session_id
                && dispatch.receipt.as_ref() == Some(receipt)
                && dispatch.receipt_created_at == Some(session.created_at)
        };
        Ok(exact.then_some(session))
    }

    pub(crate) async fn prepare_codex_handoff(
        &self,
        principal: Principal,
        selector: WorkSelector,
    ) -> Result<PreparedOffer> {
        let (service, _) = self.authority(principal).await?;
        if service.health() != Health::Writable {
            return Err(Error::AuthorityUnavailable(
                "current Work scope is unavailable".into(),
            ));
        }
        let (_, snapshot) = service.published()?;
        let (work, attempt) = select_work(&snapshot, &selector)?;
        let cwd = self
            .handoff_cwd(&snapshot, work, attempt, &selector.project_id)
            .await?;
        self.codex_handoffs
            .create(selector, snapshot.binding.scope_id, cwd)
    }

    pub(crate) async fn claim_codex_handoff(
        &self,
        principal: Principal,
        id: Uuid,
    ) -> Result<String> {
        let offer = self.codex_handoffs.pending(id)?;
        let (service, _) = self.authority(principal).await?;
        if service.health() != Health::Writable {
            return Err(Error::AuthorityUnavailable(
                "current Work scope is unavailable".into(),
            ));
        }
        let (commit, snapshot) = service.published()?;
        if snapshot.binding.scope_id != offer.scope_id {
            return Err(Error::RevisionConflict);
        }
        let (work, attempt) = select_work(&snapshot, &offer.selector)?;
        if self
            .handoff_cwd(&snapshot, work, attempt, &offer.selector.project_id)
            .await?
            != offer.cwd
        {
            return Err(Error::RevisionConflict);
        }
        let session = if let Some(attempt) = attempt {
            match self.continuation_session(&snapshot, attempt).await {
                Ok(session) => session,
                Err(error) => {
                    tracing::warn!(assignment_id = %attempt.id, %error, "Work handoff session evidence unavailable");
                    None
                }
            }
        } else {
            None
        };
        let submission = work
            .current_submission
            .as_ref()
            .and_then(|id| snapshot.submissions.get(id))
            .filter(|submission| {
                submission.work_id == work.id
                    && submission.generation == work.generation
                    && submission.contract_revision == work.contract_revision
                    && !submission.stale
            });
        let prompt = bamboo_orchestration::handoff::continuation::render(
            &offer.selector.project_id,
            work,
            attempt,
            submission,
            session.as_ref(),
        );
        // Do not consume an offer after a concurrent ledger change or checkout change.
        if self
            .handoff_cwd(&snapshot, work, attempt, &offer.selector.project_id)
            .await?
            != offer.cwd
            || service.published()?.0 != commit
        {
            return Err(Error::RevisionConflict);
        }
        self.codex_handoffs.consume(id)?;
        Ok(prompt)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::process::Command;

    use bamboo_tickets::{AssignmentState, Effect, EffectState, ExecutionWorkspace, ScopeBinding};
    use serde_json::json;

    use super::*;

    fn git(directory: &Path, arguments: &[&str]) -> String {
        let output = Command::new("git")
            .args([
                "-c",
                "user.name=Handoff fixture",
                "-c",
                "user.email=fixture@example.invalid",
            ])
            .arg("-C")
            .arg(directory)
            .args(arguments)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {arguments:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    #[tokio::test]
    async fn codex_handoff_requires_released_checkout_and_recorded_git_branch() {
        let directory = tempfile::tempdir().unwrap();
        let repo = directory.path().join("repo");
        let checkout = directory.path().join("worktree");
        std::fs::create_dir(&repo).unwrap();
        git(&repo, &["init", "-b", "main"]);
        git(
            &repo,
            &[
                "commit",
                "--allow-empty",
                "--no-gpg-sign",
                "-m",
                "fixture base",
            ],
        );
        let branch = "bamboo/work/handoff-fixture";
        git(
            &repo,
            &["worktree", "add", "-b", branch, checkout.to_str().unwrap()],
        );
        let canonical_checkout = checkout.canonicalize().unwrap();
        let worktree = checkout.to_str().unwrap().to_owned();
        let workspace = ExecutionWorkspace {
            repo: repo.to_str().unwrap().to_owned(),
            worktree: worktree.clone(),
            base_commit: git(&repo, &["rev-parse", "HEAD"]),
            branch: branch.to_owned(),
            claims: BTreeSet::from([format!("worktree:{}", canonical_checkout.display())]),
            write_roots: vec![worktree],
        };
        let mut assignment: Assignment = serde_json::from_value(json!({
            "id":"original","work_id":"work-fixture","generation":1,"contract_revision":1,
            "authority_epoch":1,"record_revision":1,"state":"running","dispatch_key":"fixture",
            "plan":{"plan_revision":1,"steps":[]},"dependency_inputs":[],"allowed_tools":[],
            "effects":{},"workspace":workspace,"updated_seq":1
        }))
        .unwrap();
        let mut snapshot = Snapshot::empty(ScopeBinding {
            scope_id: "fixture".to_owned(),
            supervisor_session_id: "supervisor".to_owned(),
            binding_revision: 1,
        });
        let verify = |snapshot: &Snapshot, attempt: &Assignment| {
            snapshot.verify_handoff_checkout_released(Some(attempt), &canonical_checkout)
        };
        assert!(matches!(
            verify(&snapshot, &assignment),
            Err(Error::ResourceBlocked(_))
        ));
        assignment.state = AssignmentState::Cancelling;
        assert!(matches!(
            verify(&snapshot, &assignment),
            Err(Error::ResourceBlocked(_))
        ));
        assignment.process_stopped = true;
        assignment.effects.insert(
            "effect".to_owned(),
            Effect {
                action_fingerprint: "fixture".to_owned(),
                state: EffectState::OutcomeUnknown,
                provider_receipt: None,
                artifact: None,
                file_intent: None,
            },
        );
        assert!(matches!(
            verify(&snapshot, &assignment),
            Err(Error::ResourceBlocked(_))
        ));
        assignment.effects.clear();
        let mut competing = assignment.clone();
        competing.id = "other-assignment".to_owned();
        competing.state = AssignmentState::Running;
        competing.process_stopped = false;
        snapshot.assignments.insert(competing.id.clone(), competing);
        assert!(matches!(
            verify(&snapshot, &assignment),
            Err(Error::ResourceBlocked(_))
        ));
        snapshot.assignments.clear();
        verify(&snapshot, &assignment).unwrap();

        use bamboo_engine::ticket_worker_plan::files::{
            verify_continuation_workspace, verify_workspace,
        };
        verify_continuation_workspace(workspace.clone())
            .await
            .unwrap();
        git(
            &checkout,
            &[
                "commit",
                "--allow-empty",
                "--no-gpg-sign",
                "-m",
                "committed Work progress",
            ],
        );
        verify_continuation_workspace(workspace.clone())
            .await
            .unwrap();
        assert!(verify_workspace(workspace.clone()).await.is_err());
        git(&checkout, &["switch", "-c", "unrelated-branch"]);
        assert!(matches!(
            verify_continuation_workspace(workspace).await,
            Err(Error::ScopeDenied(_))
        ));
    }
}
