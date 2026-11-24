//! Process-local authority for the bounded ordinary Root update path.

use super::{ParkedUpdateRoot, SessionActivationRouter, SessionUpdateAdmission, UpdateHold};
use bamboo_domain::{RootActorRuntimeWrite, Session, SessionKind};
use bamboo_tools::local_task_resources::{LocalTaskResources, LocalTaskUpdateHold};
use std::{collections::HashMap, io, sync::Mutex};
use tokio::sync::watch;

const MAX_RETAINED_ROOT_SCOPES: usize = 1024;

pub(crate) type OrdinaryRootJoin = watch::Receiver<Option<Result<(), String>>>;

#[derive(Default)]
pub(super) struct OrdinaryRootRegistry {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    scopes: HashMap<String, RootScope>,
    failure: Option<String>,
    certificate_held: bool,
}

struct RootScope {
    writer: RootActorRuntimeWrite,
    resources: LocalTaskResources,
    joined: OrdinaryRootJoin,
}

/// Holds the exact managed ordinary Root resource domains across publication
/// and finalization. Dropping a pre-commit hold leaves executions untouched.
pub struct OrdinaryRootUpdateResources {
    router: SessionActivationRouter,
    update_id: uuid::Uuid,
    parked: Vec<ParkedUpdateRoot>,
    held: Vec<(LocalTaskUpdateHold, OrdinaryRootJoin)>,
}

impl OrdinaryRootRegistry {
    fn state(&self) -> io::Result<std::sync::MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| io::Error::other("ordinary Root resource registry poisoned"))
    }

    pub(super) fn unconfirmed(&self, reason: &str) -> io::Result<()> {
        self.state()?
            .failure
            .get_or_insert_with(|| reason.to_owned());
        Ok(())
    }

    pub(super) fn owns(
        &self,
        resources: &LocalTaskResources,
        writer: &RootActorRuntimeWrite,
    ) -> bool {
        self.state().is_ok_and(|state| {
            state.scopes.get(&writer.fence.run_id).is_some_and(|scope| {
                scope.writer.fence == writer.fence
                    && scope.writer.created_at == writer.created_at
                    && scope.resources.identity() == resources.identity()
            })
        })
    }

    pub(super) fn enroll(
        &self,
        session: &Session,
        writer: RootActorRuntimeWrite,
        joined: OrdinaryRootJoin,
    ) -> io::Result<LocalTaskResources> {
        let mut state = self.state()?;
        state.reap_finished();
        let run_id = writer.fence.run_id.clone();
        if session.kind != SessionKind::Root
            || !session.authority_identity.is_ordinary()
            || writer.fence.actor_id.as_str() != session.id
            || writer.created_at != session.created_at
            || state.scopes.contains_key(&run_id)
            || state.scopes.len() >= MAX_RETAINED_ROOT_SCOPES
        {
            let error = "ordinary Root resource owner cannot be registered";
            state.failure.get_or_insert_with(|| error.into());
            return Err(io::Error::other(error));
        }
        let resources =
            LocalTaskResources::new_for_ordinary_root(&session.id, &run_id, session.created_at);
        state.scopes.insert(
            run_id,
            RootScope {
                writer,
                resources: resources.clone(),
                joined,
            },
        );
        Ok(resources)
    }

    pub(super) fn try_hold(
        &self,
        admission: &SessionUpdateAdmission,
        parked: Vec<ParkedUpdateRoot>,
    ) -> io::Result<Option<OrdinaryRootUpdateResources>> {
        let mut state = self.state()?;
        if state.certificate_held {
            return Ok(None);
        }
        state.reap_finished();
        state.check()?;
        for root in &parked {
            let scope = state.scopes.get(&root.fence.run_id).ok_or_else(|| {
                io::Error::other("parked ordinary Root has no physical resource owner")
            })?;
            if scope.writer.fence != root.fence || scope.writer.created_at != root.created_at {
                return Err(io::Error::other(
                    "parked ordinary Root resource owner changed",
                ));
            }
        }
        let mut held = Vec::with_capacity(state.scopes.len());
        for scope in state.scopes.values() {
            let active = parked.iter().any(|root| root.fence == scope.writer.fence);
            if !active && !joined(&scope.joined)? {
                return Ok(None);
            }
            let Some(hold) = scope.resources.try_hold_for_update()? else {
                return Ok(None);
            };
            held.push((hold, scope.joined.clone()));
        }
        state.certificate_held = true;
        Ok(Some(OrdinaryRootUpdateResources {
            router: admission.router.clone(),
            update_id: admission.id,
            parked,
            held,
        }))
    }
}

impl State {
    fn check(&self) -> io::Result<()> {
        if let Some(error) = &self.failure {
            return Err(io::Error::other(error.clone()));
        }
        Ok(())
    }

    fn reap_finished(&mut self) {
        if self.certificate_held {
            return;
        }
        self.scopes.retain(|_, scope| match joined(&scope.joined) {
            Ok(false) => true,
            Ok(true) => match scope.resources.try_hold_for_update() {
                Ok(None) => true,
                Ok(Some(_hold)) => false,
                Err(error) => {
                    self.failure.get_or_insert_with(|| error.to_string());
                    false
                }
            },
            Err(error) => {
                self.failure.get_or_insert_with(|| error.to_string());
                false
            }
        });
    }
}

fn joined(receipt: &OrdinaryRootJoin) -> io::Result<bool> {
    let closed = receipt.has_changed().is_err();
    let result = receipt.borrow().clone();
    match result {
        Some(Ok(())) => Ok(true),
        Some(Err(error)) => Err(io::Error::other(error)),
        None if closed => Err(io::Error::other("exact ordinary Root outer join was lost")),
        None => Ok(false),
    }
}

impl OrdinaryRootUpdateResources {
    pub(super) fn validate_for(
        &self,
        admission: &SessionUpdateAdmission,
        parked: &[ParkedUpdateRoot],
    ) -> io::Result<()> {
        if self.update_id != admission.id
            || !std::sync::Arc::ptr_eq(
                &self.router.runtime_instance,
                &admission.router.runtime_instance,
            )
            || self.parked != parked
        {
            return Err(super::update_busy());
        }
        self.check_holds()
    }

    fn check_holds(&self) -> io::Result<()> {
        self.router
            .ordinary_root_resources
            .as_ref()
            .ok_or_else(|| io::Error::other("ordinary Root resources are unavailable"))?
            .state()?
            .check()?;
        for (hold, _receipt) in &self.held {
            hold.ensure_current()?;
        }
        Ok(())
    }

    /// After commit, await the exact outer tasks and their Actor closes. The
    /// host keeps its separate checks for resource domains outside these Roots.
    pub async fn finish_committed(&self) -> io::Result<()> {
        if *self.router.update_hold.borrow() != UpdateHold::Committed(self.update_id) {
            return Err(io::Error::other("ordinary Root update is not committed"));
        }
        for (_, receipt) in &self.held {
            let mut receipt = receipt.clone();
            loop {
                if joined(&receipt)? {
                    break;
                }
                receipt.changed().await.map_err(io::Error::other)?;
            }
        }
        self.check_holds()
    }
}

impl Drop for OrdinaryRootUpdateResources {
    fn drop(&mut self) {
        if let Some(registry) = &self.router.ordinary_root_resources {
            if let Ok(mut state) = registry.state() {
                self.held.clear();
                state.certificate_held = false;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::ActorActivationFence;
    use std::time::Duration;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn ordinary_root_update_retains_background_work_after_the_logical_owner_leaves() {
        let router = SessionActivationRouter::new_for_managed_update();
        let session = Session::new("root-resource-retention-fixture", "fixture-model");
        let run_id = "retired-root";
        let registration = router.register_session_run(&session, run_id).await.unwrap();
        let writer = RootActorRuntimeWrite {
            fence: ActorActivationFence {
                schema_version: bamboo_domain::session::actor::ACTOR_DIRECTORY_SCHEMA_VERSION,
                actor_id: session.id.as_str().into(),
                activation_id: "resource-retention-activation".into(),
                attempt: 1,
                run_id: run_id.into(),
                lease_owner: "fixture-host".into(),
                lease_epoch: 1,
            },
            created_at: session.created_at,
            conversation_generation: session.conversation_generation_observation(),
        };
        let (joined_tx, joined_rx) = watch::channel(None);
        let scope = router
            .enroll_ordinary_root_resources(&session, writer, &registration.ownership(), joined_rx)
            .await
            .unwrap();
        let (complete, wait) = oneshot::channel();
        let finished = scope.spawn_owned(async { wait.await.unwrap() }).unwrap();
        // Even a joined outer task cannot attest this separately owned tail.
        registration.finish(0).await.unwrap();
        joined_tx.send_replace(Some(Ok(())));
        drop(scope);

        let update = router.hold_admissions_for_update().await.unwrap();
        assert!(update.outstanding_sessions().await.is_empty());
        assert!(update
            .try_hold_ordinary_root_resources()
            .await
            .unwrap()
            .is_none());
        assert!(update
            .commit_parked_roots(|_| unreachable!())
            .await
            .is_err());

        complete.send(()).unwrap();
        finished.await.unwrap();
        let proof = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(resources) = update.try_hold_ordinary_root_resources().await.unwrap() {
                    break resources;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let refused = update
            .commit_parked_roots_with_resources(&proof, |_| Err(io::Error::other("disk failure")))
            .await;
        assert!(refused.is_err());
        assert!(proof.finish_committed().await.is_err());
        drop(proof);
        drop(update);
        assert!(router.preparing_update().is_none());
    }
}
