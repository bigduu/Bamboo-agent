//! In-process Host scope publication, never another Session persistence layer.
//! No installation happens at startup or as a consequence of Project/link data.

use bamboo_domain::{
    HostSessionScopeBinding, HostSessionScopeIssuer, HostSessionScopeToken, SessionCallerReference,
};
use std::{collections::BTreeMap, io, sync::Arc};
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

const MAX_HOST_SCOPE_CALLERS: usize = 256;

fn denied(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
}
fn conflict() -> io::Error {
    denied("host_session_scope_revision_conflict")
}

#[derive(Debug, Default)]
struct ScopePublication {
    revision: u64,
    token: Option<HostSessionScopeToken>,
}

/// This instance is owned by one canonical SessionStore. Clones share the same
/// issuer and fences. A newly constructed store has no installed grants.
#[derive(Debug, Clone, Default)]
pub(crate) struct HostSessionScopeRegistry {
    issuer: HostSessionScopeIssuer,
    slots: Arc<RwLock<BTreeMap<String, Arc<RwLock<ScopePublication>>>>>,
}

/// The final authority set retains this guard through owned physical work.
/// Revocation/replacement waits for that work; preflight lookup is insufficient.
pub(crate) struct HostSessionScopeUse {
    publication: OwnedRwLockReadGuard<ScopePublication>,
}

impl HostSessionScopeUse {
    pub(crate) fn token(&self) -> &HostSessionScopeToken {
        self.publication
            .token
            .as_ref()
            .expect("retained installed scope")
    }
}

pub(crate) struct HostSessionScopePublication {
    caller: String,
    issuer: HostSessionScopeIssuer,
    publication: OwnedRwLockWriteGuard<ScopePublication>,
}

impl HostSessionScopePublication {
    /// Final caller/target validation and this publication share one guard set.
    /// A failed validation drops this value without changing revision or scope.
    pub(crate) fn install(
        mut self,
        binding: HostSessionScopeBinding,
    ) -> io::Result<HostSessionScopeToken> {
        if binding.caller.identity.session_id != self.caller
            || self.publication.revision.checked_add(1) != Some(binding.revision)
        {
            return Err(conflict());
        }
        let token = self
            .issuer
            .issue(binding)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        self.publication.revision = token.binding().revision;
        self.publication.token = Some(token.clone());
        Ok(token)
    }
}

impl HostSessionScopeRegistry {
    async fn slot(&self, id: &str) -> Option<Arc<RwLock<ScopePublication>>> {
        self.slots.read().await.get(id).cloned()
    }

    /// Publication is always acquired before lifecycle/Task/Session locks.
    /// The Host writer validates canonical observations while retaining this
    /// guard, then commits the installation. Drop before commit changes nothing.
    pub(crate) async fn begin_install(
        &self,
        caller: &str,
        expected_revision: u64,
    ) -> io::Result<HostSessionScopePublication> {
        let slot = {
            let mut slots = self.slots.write().await;
            if !slots.contains_key(caller) && slots.len() >= MAX_HOST_SCOPE_CALLERS {
                return Err(denied("host_session_scope_capacity_exceeded"));
            }
            Arc::clone(slots.entry(caller.to_owned()).or_default())
        };
        let publication = slot.write_owned().await;
        if publication.revision != expected_revision {
            return Err(conflict());
        }
        Ok(HostSessionScopePublication {
            caller: caller.into(),
            issuer: self.issuer.clone(),
            publication,
        })
    }

    pub(crate) async fn revoke(&self, caller: &str, expected_revision: u64) -> io::Result<u64> {
        let slot = self.slot(caller).await.ok_or_else(conflict)?;
        let mut publication = slot.write().await;
        if publication.revision != expected_revision {
            return Err(conflict());
        }
        publication.revision = publication.revision.checked_add(1).ok_or_else(conflict)?;
        publication.token = None;
        Ok(publication.revision)
    }

    /// Lookup supplies a candidate token, never final read/write authority.
    pub(crate) async fn lookup(
        &self,
        caller: &SessionCallerReference,
    ) -> Option<HostSessionScopeToken> {
        let slot = self.slot(&caller.session_id).await?;
        let publication = slot.read().await;
        publication
            .token
            .as_ref()
            .filter(|token| token.binding().caller.identity.matches_caller(caller))
            .cloned()
    }

    pub(crate) async fn retain(
        &self,
        token: &HostSessionScopeToken,
        caller: &SessionCallerReference,
    ) -> io::Result<HostSessionScopeUse> {
        if !self.issuer.recognizes(token) || !token.binding().caller.identity.matches_caller(caller)
        {
            return Err(denied("host_session_scope_unavailable"));
        }
        let slot = self
            .slot(&caller.session_id)
            .await
            .ok_or_else(|| denied("host_session_scope_unavailable"))?;
        let publication = slot.read_owned().await;
        if !publication
            .token
            .as_ref()
            .is_some_and(|current| current.same_installation(token))
        {
            return Err(denied("host_session_scope_revoked_or_replaced"));
        }
        Ok(HostSessionScopeUse { publication })
    }
}

#[cfg(test)]
#[path = "host_session_scope_tests.rs"]
mod tests;
