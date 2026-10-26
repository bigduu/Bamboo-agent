//! Host-installed Native Work attribution. The canonical Ticket ledger is the
//! only wallet; canonical Session ancestry and dispatch receipts supply scope.
use std::sync::Arc;

use bamboo_domain::{Session, SessionKind, Storage};
use bamboo_tickets::{Authority, Error, Principal, Result, TicketService};

use crate::ticket_runtime::{self, TicketDispatchBinding};

mod provider;

#[derive(Clone)]
struct SessionStamp {
    id: String,
    created_at: chrono::DateTime<chrono::Utc>,
    parent: Option<String>,
    root: String,
    kind: SessionKind,
}

impl SessionStamp {
    fn from_session(session: &Session) -> Self {
        Self {
            id: session.id.clone(),
            created_at: session.created_at,
            parent: session.parent_session_id.clone(),
            root: session.root_session_id.clone(),
            kind: session.kind,
        }
    }
    fn matches(&self, session: &Session) -> bool {
        self.id == session.id
            && self.created_at == session.created_at
            && self.parent == session.parent_session_id
            && self.root == session.root_session_id
            && self.kind == session.kind
    }
}

/// Constructible only inside the Native Host. This capability is not decoded
/// from HTTP, prompts, tools, session display metadata or Worker payloads.
pub struct NativeWorkTokenBudget {
    service: Arc<TicketService>,
    storage: Arc<dyn Storage>,
    chain: Vec<SessionStamp>,
    dispatch: Option<TicketDispatchBinding>,
    binding: bamboo_tickets::ScopeBinding,
}

impl NativeWorkTokenBudget {
    pub(crate) async fn capture(
        service: Option<Arc<TicketService>>,
        storage: Arc<dyn Storage>,
        session: &Session,
    ) -> Result<Option<Arc<Self>>> {
        let Some(service) = service else {
            if ticket_runtime::read_dispatch(session)?.is_some() {
                return Err(Error::AuthorityUnavailable(
                    "Ticket token authority unavailable".into(),
                ));
            }
            return Ok(None);
        };
        let mut chain = Vec::<SessionStamp>::new();
        let mut id = session.id.clone();
        let mut dispatch = None;
        for _ in 0..32 {
            if chain.iter().any(|stamp| stamp.id == id) {
                return Err(Error::ScopeDenied("cyclic Native ancestry".into()));
            }
            let canonical = storage
                .load_runtime_control_plane(&id)
                .await?
                .ok_or_else(|| Error::ScopeDenied("Native ancestor missing".into()))?;
            if chain.is_empty() && !SessionStamp::from_session(session).matches(&canonical) {
                return Err(Error::ScopeDenied(
                    "Native execution incarnation changed".into(),
                ));
            }
            if dispatch.is_none() {
                if let Some(actual) = ticket_runtime::read_dispatch(&canonical)? {
                    if canonical.id != ticket_runtime::ticket_child_id(&actual.dispatch_key) {
                        return Err(Error::ScopeDenied(
                            "Native budget binding is outside its canonical Ticket child".into(),
                        ));
                    }
                    dispatch = Some(actual);
                }
            }
            chain.push(SessionStamp::from_session(&canonical));
            if canonical.kind == SessionKind::Root {
                let published = service.published()?.1;
                if published.binding.supervisor_session_id != canonical.id {
                    return if dispatch.is_some() {
                        Err(Error::ScopeDenied(
                            "Ticket budget scope differs from Native ancestry".into(),
                        ))
                    } else {
                        Ok(None)
                    };
                }
                // A mounted historical TicketService must not block an
                // ordinary, unbudgeted Child after Root leaves orchestration
                // mode. Keep the capability, so configuring limits during the
                // run still revalidates fresh Root/Work authority per call.
                let binding = if published.execution_budget.is_some() || dispatch.is_some() {
                    ticket_runtime::verified_scope_binding(storage.as_ref(), &canonical.id)
                        .await?
                        .0
                } else {
                    published.binding.clone()
                };
                if binding != published.binding
                    || dispatch.as_ref().is_some_and(|d| d.binding != binding)
                {
                    return Err(Error::ScopeDenied("Ticket budget binding changed".into()));
                }
                return Ok(Some(Arc::new(Self {
                    service,
                    storage,
                    chain,
                    dispatch,
                    binding,
                })));
            }
            if canonical.kind != SessionKind::Child {
                return Err(Error::ScopeDenied(
                    "Native ancestry must use canonical Child sessions".into(),
                ));
            }
            id = canonical
                .parent_session_id
                .ok_or_else(|| Error::ScopeDenied("Native parent missing".into()))?;
        }
        Err(Error::ResourceBlocked(
            "Native budget ancestry exceeds bound".into(),
        ))
    }

    async fn authority(&self) -> Result<Authority> {
        let (binding, _) = ticket_runtime::verified_scope_binding(
            self.storage.as_ref(),
            &self.binding.supervisor_session_id,
        )
        .await?;
        if binding != self.binding {
            return Err(Error::ScopeDenied(
                "Native budget Supervisor changed".into(),
            ));
        }
        for stamp in &self.chain {
            let canonical = self
                .storage
                .load_runtime_control_plane(&stamp.id)
                .await?
                .ok_or_else(|| Error::ScopeDenied("Native budget ancestor missing".into()))?;
            if !stamp.matches(&canonical) {
                return Err(Error::ScopeDenied("Native budget ancestry changed".into()));
            }
            if let Some(actual) = ticket_runtime::read_dispatch(&canonical)? {
                if self.dispatch.as_ref() != Some(&actual) {
                    return Err(Error::ScopeDenied("Native budget dispatch changed".into()));
                }
            }
        }
        let dispatch = self.dispatch.as_ref().ok_or_else(|| {
            Error::ResourceBlocked("configured scope requires an attributable Work dispatch".into())
        })?;
        ticket_runtime::require_dispatch_permission(&self.service, dispatch)?;
        let snapshot = self.service.published()?.1;
        let spec = &snapshot
            .intents
            .get(&dispatch.dispatch_key)
            .ok_or_else(|| Error::ScopeDenied("Native budget dispatch missing".into()))?
            .immutable_spec;
        if !matches!(ticket_runtime::query_dispatch(self.storage.as_ref(), &self.service, &dispatch.dispatch_key, spec).await?,
            ticket_runtime::DispatchObservation::Admitted { receipt } if dispatch.receipt.as_ref() == Some(&receipt))
        {
            return Err(Error::ScopeDenied(
                "Native budget requires the original admitted attempt".into(),
            ));
        }
        Ok(Authority::from_verified_host(binding, Principal::Runtime))
    }
}
