//! Owner-triggered empty Work scope initialization over existing Host authority.
use super::*;
use serde::{Deserialize, Serialize};
use std::{
    io,
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ScopeInitialization {
    pub supervisor_session_id: String,
    pub incarnation_id: uuid::Uuid,
    pub binding_revision: u64,
}

#[derive(Debug)]
pub struct ScopeInitializationError {
    pub status: u16,
    pub code: &'static str,
    pub error: String,
}
impl ScopeInitializationError {
    fn new(status: u16, code: &'static str, error: impl ToString) -> Self {
        Self {
            status,
            code,
            error: error.to_string(),
        }
    }
}

/// Observation only. Unknown files, partial stores and symlinks are never empty.
fn scope_is_empty(root: &Path) -> io::Result<bool> {
    if let Some(parent) = root.parent() {
        match std::fs::symlink_metadata(parent) {
            Ok(meta) if meta.file_type().is_symlink() || !meta.is_dir() => {
                return Err(io::Error::other("Ticket parent is not a plain directory"))
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(true),
            Err(error) => return Err(error),
        }
    }
    match std::fs::symlink_metadata(root) {
        Ok(meta) if meta.file_type().is_symlink() || !meta.is_dir() => {
            Err(io::Error::other("Ticket scope is not a plain directory"))
        }
        Ok(_) => Ok(std::fs::read_dir(root)?.next().transpose()?.is_none()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(error),
    }
}

impl TicketApplication {
    pub(super) fn scope_root(&self, incarnation: uuid::Uuid) -> PathBuf {
        self.root.join("tickets").join(incarnation.to_string())
    }

    pub async fn status(&self) -> Value {
        let flags = self.config.read().await.features.clone();
        let mut status = json!({
            "available":false,"readable":false,"can_initialize":false,"initialization":null,
            "mutation_enabled":flags.ticket_mutation,"dispatch_enabled":flags.ticket_dispatch,
            "capabilities":{"ticket_scope_v1":true,"multi_pending_v1":true,"precise_request_response_v1":true,"message_references_v1":true,"semantic_messages_v1":true,"scope_initialize_v1":true}
        });
        let unavailable = |mut status: Value, kind: &str, code: &str, reason: String| {
            status["availability"] = json!({"kind":kind,"reason_code":code});
            status["reason"] = json!(reason);
            status
        };
        let supervisor = match self
            .storage
            .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
            .await
        {
            Ok(Some(session)) => session,
            Ok(None) if !flags.ticket_mutation => {
                return unavailable(
                    status,
                    "disabled",
                    "ticket_mutation_disabled",
                    "Ticket mutation feature is disabled".into(),
                )
            }
            Ok(None) => {
                return unavailable(
                    status,
                    "forbidden",
                    "supervisor_identity_mismatch",
                    "Canonical Supervisor is missing".into(),
                )
            }
            Err(error) => return unavailable(status, "error", "scope_io_error", error.to_string()),
        };
        let Some(incarnation_id) = supervisor.supervisor_incarnation_id() else {
            return unavailable(
                status,
                "forbidden",
                "supervisor_identity_mismatch",
                "Canonical Supervisor identity required".into(),
            );
        };
        if !supervisor.root_orchestration_only_enabled()
            || supervisor.root_tool_authority_revision == 0
        {
            return unavailable(
                status,
                "forbidden",
                "supervisor_mode_required",
                "Supervisor orchestration authority required".into(),
            );
        }
        let binding = ScopeBinding {
            scope_id: format!("supervisor/{incarnation_id}"),
            supervisor_session_id: supervisor.id.clone(),
            binding_revision: supervisor.root_tool_authority_revision,
        };
        if let Some(service) = self.service.get() {
            let published = match service.published() {
                Ok((_, snapshot)) => snapshot,
                Err(error) if service.binding_mismatch() => {
                    return unavailable(
                        status,
                        "forbidden",
                        "scope_binding_mismatch",
                        error.to_string(),
                    )
                }
                Err(error) => {
                    return unavailable(
                        status,
                        "error",
                        "scope_requires_recovery",
                        error.to_string(),
                    )
                }
            };
            if published.binding != binding {
                return unavailable(
                    status,
                    "forbidden",
                    "scope_binding_mismatch",
                    "Supervisor scope binding changed".into(),
                );
            }
            let authority = Authority::from_verified_host(
                binding,
                Principal::User {
                    user_id: "host-owner".into(),
                },
            );
            return match service.work_overview(&authority) {
                Ok(overview) => {
                    status["available"] = json!(true);
                    status["readable"] = json!(true);
                    status["availability"] = json!({"kind":"ready","reason_code":"scope_ready"});
                    status["binding"] = json!(published.binding);
                    status["health"] = json!(if service.health() == Health::Writable {
                        "writable"
                    } else {
                        "read_only"
                    });
                    status["overview"] = json!(overview);
                    status
                }
                Err(error) => unavailable(
                    status,
                    "error",
                    "scope_requires_recovery",
                    error.to_string(),
                ),
            };
        }
        if !flags.ticket_mutation {
            return unavailable(
                status,
                "disabled",
                "ticket_mutation_disabled",
                "Ticket mutation feature is disabled".into(),
            );
        }
        let root = self.scope_root(incarnation_id);
        match tokio::task::spawn_blocking(move || scope_is_empty(&root)).await {
            Ok(Ok(true)) => {
                status["can_initialize"] = json!(true);
                status["initialization"] = json!(ScopeInitialization {
                    supervisor_session_id: supervisor.id,
                    incarnation_id,
                    binding_revision: binding.binding_revision
                });
                unavailable(
                    status,
                    "uninitialized",
                    "scope_uninitialized",
                    "Work scope has not been initialized".into(),
                )
            }
            Ok(Ok(false)) => unavailable(
                status,
                "error",
                "scope_requires_recovery",
                self.unavailable
                    .clone()
                    .unwrap_or_else(|| "Existing scope requires explicit recovery".into()),
            ),
            Ok(Err(error)) => unavailable(status, "error", "scope_io_error", error.to_string()),
            Err(error) => unavailable(status, "error", "scope_io_error", error.to_string()),
        }
    }

    pub async fn initialize_scope(
        &self,
        principal: Principal,
        expected: ScopeInitialization,
    ) -> std::result::Result<bool, ScopeInitializationError> {
        if !matches!(principal, Principal::User { ref user_id } if user_id == "host-owner") {
            return Err(ScopeInitializationError::new(
                403,
                "supervisor_identity_mismatch",
                "verified owner access required",
            ));
        }
        let _serial = self.initializing.lock().await;
        let supervisor = self
            .storage
            .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
            .await
            .map_err(|error| ScopeInitializationError::new(503, "scope_io_error", error))?
            .ok_or_else(|| {
                ScopeInitializationError::new(
                    403,
                    "supervisor_identity_mismatch",
                    "Canonical Supervisor missing",
                )
            })?;
        let incarnation_id = supervisor.supervisor_incarnation_id().ok_or_else(|| {
            ScopeInitializationError::new(
                403,
                "supervisor_identity_mismatch",
                "Canonical Supervisor required",
            )
        })?;
        if expected.supervisor_session_id != supervisor.id
            || expected.incarnation_id != incarnation_id
            || expected.binding_revision != supervisor.root_tool_authority_revision
        {
            return Err(ScopeInitializationError::new(
                409,
                "scope_binding_mismatch",
                "Supervisor scope binding changed",
            ));
        }
        if !supervisor.root_orchestration_only_enabled() || expected.binding_revision == 0 {
            return Err(ScopeInitializationError::new(
                403,
                "supervisor_mode_required",
                "Supervisor orchestration authority required",
            ));
        }
        let binding = ScopeBinding {
            scope_id: format!("supervisor/{incarnation_id}"),
            supervisor_session_id: supervisor.id.clone(),
            binding_revision: expected.binding_revision,
        };
        if let Some(service) = self.service.get() {
            return if service
                .published()
                .is_ok_and(|(_, snapshot)| snapshot.binding == binding)
            {
                Ok(false)
            } else {
                Err(ScopeInitializationError::new(
                    409,
                    "scope_binding_mismatch",
                    "Existing scope binding differs",
                ))
            };
        }
        // Keep the existing mutation gate unchanged throughout the physical job.
        let config = self.config.clone().read_owned().await;
        if !config.features.ticket_mutation {
            return Err(ScopeInitializationError::new(
                503,
                "ticket_mutation_disabled",
                "Ticket mutation feature is disabled",
            ));
        }
        let root = self.scope_root(incarnation_id);
        let service_slot = self.service.clone();
        let created = Arc::new(AtomicBool::new(false));
        let created_job = created.clone();
        let operation = Box::new(move || {
            let _config = config;
            if let Some(service) = service_slot.get() {
                return if service
                    .published()
                    .is_ok_and(|(_, snapshot)| snapshot.binding == binding)
                {
                    Ok(())
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "scope binding changed",
                    ))
                };
            }
            if !scope_is_empty(&root)? {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "Existing scope requires explicit recovery",
                ));
            }
            let service =
                TicketService::initialize_empty(&root, binding).map_err(|error| match error {
                    Error::Io(error) => error,
                    error => io::Error::other(error),
                })?;
            if service.health() != Health::Writable {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "New scope requires recovery",
                ));
            }
            service_slot.set(Arc::new(service)).map_err(|_| {
                io::Error::new(io::ErrorKind::WouldBlock, "concurrent initialization")
            })?;
            created_job.store(true, Ordering::Release);
            Ok(())
        });
        let reference = bamboo_domain::SupervisorReference {
            session_id: supervisor.id,
            incarnation_id,
        };
        self.storage
            .with_supervisor_orchestration_authority(
                &reference,
                expected.binding_revision,
                operation,
            )
            .await
            .map_err(|error| {
                let (status, code) = match error.kind() {
                    io::ErrorKind::AlreadyExists => (409, "scope_requires_recovery"),
                    io::ErrorKind::WouldBlock => (409, "initialization_conflict"),
                    io::ErrorKind::PermissionDenied => (409, "scope_binding_mismatch"),
                    _ => (503, "scope_io_error"),
                };
                ScopeInitializationError::new(status, code, error)
            })?;
        Ok(created.load(Ordering::Acquire))
    }
}
