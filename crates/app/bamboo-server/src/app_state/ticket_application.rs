//! One opt-in scope over the existing canonical Supervisor and Child Runtime.
//! Authority always comes from Host storage; client JSON never selects a role.
use std::{
    path::Path,
    sync::{Arc, OnceLock},
};

use bamboo_config::Config;
use bamboo_domain::{SessionAuthorityIdentity, Storage, DEFAULT_SUPERVISOR_SESSION_ID};
use bamboo_engine::ticket_runtime;
use bamboo_engine::ticket_worker_plan::tickets::*;
use serde_json::{json, Value};
use tokio::sync::RwLock;

use crate::tools::{ticket_dispatch_adapter::TicketDispatchPolicy, ChildSessionAdapter};

pub struct TicketApplication {
    storage: Arc<dyn Storage>,
    config: Arc<RwLock<Config>>,
    service: Option<Arc<TicketService>>,
    unavailable: Option<String>,
    adapter: OnceLock<Arc<ChildSessionAdapter>>,
    workspace: String,
}

#[cfg(test)]
mod tests;

impl TicketApplication {
    pub async fn open(root: &Path, storage: Arc<dyn Storage>, config: Arc<RwLock<Config>>) -> Self {
        let opened = Self::open_scope(root, storage.as_ref(), &config).await;
        let (service, unavailable) = match opened {
            Ok(service) => (Some(Arc::new(service)), None),
            Err(error) => (None, Some(error.to_string())),
        };
        Self {
            storage,
            config,
            service,
            unavailable,
            adapter: OnceLock::new(),
            workspace: root
                .join("workspaces")
                .join(DEFAULT_SUPERVISOR_SESSION_ID)
                .to_string_lossy()
                .into_owned(),
        }
    }

    async fn open_scope(
        root: &Path,
        storage: &dyn Storage,
        config: &RwLock<Config>,
    ) -> Result<TicketService> {
        let config = config.read().await;
        let existing = storage
            .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
            .await?;
        let (supervisor, freshly_created) = match existing {
            Some(supervisor) => (supervisor, false),
            None if config.features.ticket_mutation => {
                let model =
                    bamboo_engine::model_config_helper::get_default_model_from_config(&config)
                        .map_err(|e| Error::AuthorityUnavailable(e.to_string()))?;
                // Only this atomic Storage port can confer Supervisor identity.
                let receipt = storage.get_or_create_default_supervisor(&model).await?;
                if !receipt.created {
                    return Err(Error::AuthorityUnavailable(
                        "Supervisor was concurrently created; explicit scope recovery required"
                            .into(),
                    ));
                }
                let mut supervisor = storage
                    .load_root_authority(&receipt.session_id)
                    .await?
                    .ok_or_else(|| Error::AuthorityUnavailable("new Supervisor missing".into()))?;
                supervisor
                    .set_root_orchestration_only(true)
                    .map_err(|e| Error::InvalidTransition(e.to_string()))?;
                storage.save_session(&supervisor).await?;
                (supervisor, true)
            }
            None => {
                return Err(Error::AuthorityUnavailable(
                    "Ticket mutation feature is disabled".into(),
                ))
            }
        };
        let SessionAuthorityIdentity::Supervisor { incarnation_id } = supervisor.authority_identity
        else {
            return Err(Error::ScopeDenied(
                "canonical Supervisor identity required".into(),
            ));
        };
        let scope_root = root.join("tickets").join(incarnation_id.to_string());
        // Existing Supervisors are never silently attached, even with the flag
        // enabled. A new empty bootstrap has no legacy history to take over.
        if !scope_root.join("HEAD").exists() && !freshly_created {
            return Err(Error::AuthorityUnavailable(
                "existing Supervisor needs explicit attach/import; no automatic takeover".into(),
            ));
        }
        drop(config);
        let (binding, _) = ticket_runtime::verified_scope_binding(storage, &supervisor.id).await?;
        TicketService::open(scope_root, binding)
    }

    pub fn service(&self) -> Result<Arc<TicketService>> {
        self.service.clone().ok_or_else(|| {
            Error::AuthorityUnavailable(
                self.unavailable
                    .clone()
                    .unwrap_or_else(|| "Ticket scope unavailable".into()),
            )
        })
    }

    pub fn bind_adapter(&self, adapter: Arc<ChildSessionAdapter>) {
        let _ = self.adapter.set(adapter);
    }

    pub async fn authority(&self, principal: Principal) -> Result<(Arc<TicketService>, Authority)> {
        let service = self.service()?;
        let (binding, _) = ticket_runtime::verified_scope_binding(
            self.storage.as_ref(),
            DEFAULT_SUPERVISOR_SESSION_ID,
        )
        .await?;
        if service.published()?.1.binding != binding {
            return Err(Error::ScopeDenied(
                "Supervisor scope binding changed".into(),
            ));
        }
        Ok((service, Authority::from_verified_host(binding, principal)))
    }

    pub async fn status(&self) -> Value {
        let flags = self.config.read().await.features.clone();
        match self
            .authority(Principal::User {
                user_id: "host-owner".into(),
            })
            .await
        {
            Ok((service, authority)) => match service.work_overview(&authority) {
                Ok(overview) => {
                    json!({"available": true, "binding": service.published().map(|(_,s)| s.binding).ok(),
                    "health": match service.health() { Health::Writable => "writable", _ => "read_only" },
                    "mutation_enabled": flags.ticket_mutation, "dispatch_enabled": flags.ticket_dispatch,
                    "overview": overview})
                }
                Err(error) => json!({"available":false,"reason":error.to_string()}),
            },
            Err(error) => json!({"available": false, "reason": error.to_string(),
                "mutation_enabled": flags.ticket_mutation, "dispatch_enabled": flags.ticket_dispatch}),
        }
    }

    pub async fn update(
        &self,
        principal: Principal,
        command: &Command,
    ) -> Result<OperationReceipt> {
        if !self.config.read().await.features.ticket_mutation {
            return Err(Error::AuthorityUnavailable(
                "Ticket mutation feature is disabled".into(),
            ));
        }
        let (service, authority) = self.authority(principal).await?;
        service.work_update(&authority, command)
    }

    /// Commit before enqueue, return admission observations without waiting for
    /// the Worker. Errors after commit are reported alongside its receipt.
    pub async fn dispatch(&self, principal: Principal, command: &Command) -> Result<Value> {
        let flags = self.config.read().await.features.clone();
        if !flags.ticket_mutation {
            return Err(Error::AuthorityUnavailable(
                "Ticket mutation feature is disabled".into(),
            ));
        }
        let (service, authority) = self.authority(principal).await?;
        let receipt = service.work_dispatch(&authority, command)?;
        let snapshot = service.published()?.1;
        let mut runtime = Vec::new();
        let mut errors = Vec::new();
        for assignment in snapshot
            .assignments
            .values()
            .filter(|a| receipt.ids.values().any(|id| id == &a.id))
        {
            let intent = &snapshot.intents[&assignment.dispatch_key];
            let observation = self
                .ensure(&assignment.dispatch_key, &intent.immutable_spec)
                .await;
            match observation {
                Ok(observation) => runtime.push(json!({"dispatch_key":assignment.dispatch_key, "observation":observation})),
                Err(error) => errors.push(json!({"dispatch_key":assignment.dispatch_key,"status_code":error.status_code(),"reason":error.to_string()})),
            }
        }
        Ok(
            json!({"status":"accepted_for_dispatch", "receipt":receipt,"runtime":runtime,"errors":errors}),
        )
    }

    async fn ensure(
        &self,
        key: &str,
        spec: &DispatchSpec,
    ) -> Result<ticket_runtime::DispatchObservation> {
        let service = self.service()?;
        let enabled = self.config.read().await.features.ticket_dispatch;
        if enabled {
            std::fs::create_dir_all(&self.workspace)?;
        }
        let adapter = self.adapter.get().ok_or_else(|| {
            Error::AuthorityUnavailable("existing Child adapter not ready".into())
        })?;
        adapter
            .ensure_ticket_dispatch(
                service,
                key,
                spec,
                &TicketDispatchPolicy {
                    enabled,
                    worker_role: "worker".into(),
                    workspace: self.workspace.clone(),
                },
            )
            .await
    }

    pub async fn query_dispatch(&self, key: &str) -> Result<ticket_runtime::DispatchObservation> {
        let (service, _) = self.authority(Principal::Runtime).await?;
        let spec = service
            .published()?
            .1
            .intents
            .get(key)
            .ok_or_else(|| Error::InvalidTransition("dispatch key absent".into()))?
            .immutable_spec
            .clone();
        ticket_runtime::query_dispatch(self.storage.as_ref(), &service, key, &spec).await
    }
}
