//! Caller selection and Host integration for the one existing Root-create flow.

use super::*;
use bamboo_domain::{host_session_create_identity, HostSessionScopeToken, SessionCallerReference};
use bamboo_engine::SessionPeerDeliveryReceipt;
use bamboo_server_tools::session_control::root_creation::SessionRootCreatePort;

/// A create-purpose Inbox adapter, distinct from target SendPeer or management.
/// The implementation must revalidate the retained token, original caller and
/// reserved Root under the canonical final Inbox guards and preserve receipts
/// on activation failure. Possession of a creation receipt is never permission.
#[async_trait]
pub(in crate::app_state) trait HostRootFirstInputPort: Send + Sync {
    async fn deliver(
        &self,
        token: &HostSessionScopeToken,
        caller: &SessionCallerReference,
        operation_id: uuid::Uuid,
        reserved_root: &Session,
        message: &str,
    ) -> Result<SessionPeerDeliveryReceipt, SessionMessengerError>;
}

struct CanonicalHostRootFirstInput(Arc<SessionMessenger>);

#[async_trait]
impl HostRootFirstInputPort for CanonicalHostRootFirstInput {
    async fn deliver(
        &self,
        token: &HostSessionScopeToken,
        caller: &SessionCallerReference,
        operation_id: uuid::Uuid,
        reserved_root: &Session,
        message: &str,
    ) -> Result<SessionPeerDeliveryReceipt, SessionMessengerError> {
        self.0
            .send_host_root_first_input(token, caller, operation_id, reserved_root, message)
            .await
    }
}

#[derive(Clone)]
pub(super) struct CallerRootCreator {
    creator: SupervisedRootCreator,
    first_input: Option<Arc<dyn HostRootFirstInputPort>>,
}

#[derive(Clone)]
pub(super) enum CreationAuthority {
    Supervisor {
        reference: SupervisorReference,
        original_caller: Option<SessionCallerReference>,
    },
    Host {
        caller: SessionCallerReference,
        token: HostSessionScopeToken,
        first_input: Option<Arc<dyn HostRootFirstInputPort>>,
    },
}

pub(super) struct CreationPublication {
    pub state_revision: u64,
    pub link_revision: Option<u64>,
    pub linked: Option<bool>,
}

pub(super) struct CreationAuthorizationError {
    pub code: &'static str,
    pub message: String,
}

impl CreationAuthorizationError {
    fn io(error: std::io::Error) -> Self {
        Self {
            code: io_code(&error),
            message: error.to_string(),
        }
    }
    fn blocked(code: &'static str, message: impl ToString) -> Self {
        Self {
            code,
            message: message.to_string(),
        }
    }
}

impl SupervisedRootCreator {
    /// All framework assemblies share the same canonical create-purpose Inbox.
    /// Missing backend support fails closed at its final admission boundary.
    pub(in crate::app_state) fn caller_port(&self) -> Arc<dyn SessionRootCreatePort> {
        self.with_host_first_input(Arc::new(CanonicalHostRootFirstInput(
            self.messenger.clone(),
        )))
    }

    #[cfg(test)]
    pub(in crate::app_state) fn unconfigured_caller_port(&self) -> Arc<dyn SessionRootCreatePort> {
        Arc::new(CallerRootCreator {
            creator: self.clone(),
            first_input: None,
        })
    }

    pub(in crate::app_state) fn with_host_first_input(
        &self,
        first_input: Arc<dyn HostRootFirstInputPort>,
    ) -> Arc<dyn SessionRootCreatePort> {
        Arc::new(CallerRootCreator {
            creator: self.clone(),
            first_input: Some(first_input),
        })
    }

    async fn check_original_caller(
        &self,
        caller: &SessionCallerReference,
    ) -> Result<Session, ToolError> {
        let current = self
            .store
            .load_execution_admission_snapshot(&caller.session_id)
            .await
            .map_err(|error| {
                ToolError::Execution(format!("Root caller admission unavailable: {error}"))
            })?
            .ok_or_else(|| ToolError::Execution("Root caller is unavailable".into()))?;
        current
            .check_execution_compatibility()
            .map_err(|error| ToolError::Execution(error.to_string()))?;
        if !caller.matches_session(&current) {
            return Err(ToolError::Execution(
                "Root caller lifetime, generation or authority changed".into(),
            ));
        }
        Ok(current)
    }
}

impl CallerRootCreator {
    async fn authority(
        &self,
        caller: &SessionCallerReference,
    ) -> Result<CreationAuthority, ToolError> {
        let current = self.creator.check_original_caller(caller).await?;
        // Lookup errors, revocation, stale identity and any installed finite
        // grant are terminal here. None means this caller was never installed.
        match self
            .creator
            .store
            .find_host_session_scope(caller)
            .await
            .map_err(|error| {
                ToolError::Execution(format!("Host Root scope unavailable: {error}"))
            })? {
            Some(token) => Ok(CreationAuthority::Host {
                caller: caller.clone(),
                token,
                first_input: self.first_input.clone(),
            }),
            None => {
                let incarnation_id = current.supervisor_incarnation_id().ok_or_else(|| {
                    ToolError::Execution("session_root_scope_unavailable: this caller has no explicit Host Root creation scope".into())
                })?;
                Ok(CreationAuthority::Supervisor {
                    reference: SupervisorReference {
                        session_id: current.id,
                        incarnation_id,
                    },
                    original_caller: Some(caller.clone()),
                })
            }
        }
    }
}

#[async_trait]
impl SessionRootCreatePort for CallerRootCreator {
    async fn scope(&self, caller: &SessionCallerReference) -> Result<ToolResult, ToolError> {
        self.authority(caller).await?.scope(&self.creator).await
    }

    async fn create(
        &self,
        caller: &SessionCallerReference,
        request: SupervisedRootCreateRequest,
    ) -> Result<ToolResult, ToolError> {
        request.validate()?;
        let authority = self.authority(caller).await?;
        if !authority.first_input_available() {
            return Ok(blocked(request.operation_id, None, "first_input", "host_root_first_input_unavailable",
                "This Host has no canonical create-purpose first-input port; no Root creation was started"));
        }
        let service = self.creator.clone();
        tokio::spawn(async move { service.create_authorized_inner(&authority, request).await })
            .await
            .map_err(|error| {
                ToolError::Execution(format!("caller Root creation task failed: {error}"))
            })?
    }
}

impl CreationAuthority {
    pub(super) fn legacy_supervisor(reference: SupervisorReference) -> Self {
        Self::Supervisor {
            reference,
            original_caller: None,
        }
    }

    pub(super) fn is_host(&self) -> bool {
        matches!(self, Self::Host { .. })
    }

    pub(super) fn first_input_available(&self) -> bool {
        !matches!(
            self,
            Self::Host {
                first_input: None,
                ..
            }
        )
    }

    /// Freeze only immutable lifetime plus the entire request. A replacement
    /// grant or a new conversation never changes an existing operation's key.
    pub(super) fn operation_identity(
        &self,
        request: &SupervisedRootCreateRequest,
    ) -> Result<(String, String, String), ToolError> {
        let (digest, reserved_id, fingerprint) = match self {
            Self::Supervisor { reference, .. } => {
                let digest = key_digest(&format!(
                    "supervised-root:v1\0{}\0{}\0{}",
                    reference.session_id, reference.incarnation_id, request.operation_id
                ));
                let reserved_id = uuid::Uuid::parse_str(&digest[..32])
                    .expect("SHA-256 prefix is a simple UUID")
                    .to_string();
                let fingerprint = payload_fingerprint(&(reference, request));
                (digest, reserved_id, fingerprint)
            }
            Self::Host { caller, .. } => {
                let (digest, reserved_id) =
                    host_session_create_identity(caller, request.operation_id)
                        .map_err(|error| ToolError::InvalidArguments(error.into()))?;
                let fingerprint = match &caller.authority_identity {
                    // Preserve the already retained legacy operation across
                    // never-installed -> explicit Host scope selection changes.
                    bamboo_domain::SessionAuthorityIdentity::Supervisor { incarnation_id } => {
                        payload_fingerprint(&(
                            SupervisorReference {
                                session_id: caller.session_id.clone(),
                                incarnation_id: *incarnation_id,
                            },
                            request,
                        ))
                    }
                    bamboo_domain::SessionAuthorityIdentity::Ordinary => {
                        payload_fingerprint(&(&caller.session_id, caller.created_at, request))
                    }
                };
                (digest, reserved_id, fingerprint)
            }
        };
        Ok((
            digest,
            reserved_id,
            fingerprint.map_err(|error| ToolError::InvalidArguments(error.to_string()))?,
        ))
    }

    async fn current_revision(&self, creator: &SupervisedRootCreator) -> Result<u64, ToolError> {
        match self {
            Self::Supervisor {
                reference,
                original_caller,
            } => {
                if let Some(caller) = original_caller {
                    creator.check_original_caller(caller).await?;
                }
                creator
                    .store
                    .inspect_supervisor_scope(reference)
                    .await
                    .map(|scope| scope.state_revision)
                    .map_err(|error| ToolError::Execution(format!("{}: {error}", io_code(&error))))
            }
            Self::Host { caller, token, .. } => {
                creator.check_original_caller(caller).await?;
                let current = creator
                    .store
                    .find_host_session_scope(caller)
                    .await
                    .map_err(|error| {
                        ToolError::Execution(format!("Host Root scope unavailable: {error}"))
                    })?
                    .ok_or_else(|| {
                        ToolError::Execution(
                            "Host Root scope is unavailable; no fallback is permitted".into(),
                        )
                    })?;
                if !current.same_installation(token) {
                    return Err(ToolError::Execution("Host Root scope was replaced; start a fresh admission with the unchanged request".into()));
                }
                // The integrated finite read validates publication, caller,
                // Project and complete lineage while holding canonical guards.
                creator
                    .store
                    .list_host_scoped_targets(token, caller)
                    .await
                    .map_err(|error| {
                        ToolError::Execution(format!("Host Root scope validation failed: {error}"))
                    })?;
                Ok(token.binding().revision)
            }
        }
    }

    pub(super) async fn scope(
        &self,
        creator: &SupervisedRootCreator,
    ) -> Result<ToolResult, ToolError> {
        match self {
            Self::Supervisor {
                reference,
                original_caller,
            } => {
                self.current_revision(creator).await?;
                let result = SupervisedRootCreatePort::scope(creator, reference).await?;
                if let Some(caller) = original_caller {
                    creator.check_original_caller(caller).await?;
                    // Last await for this read: an installation or tombstone
                    // cannot be reinterpreted as legacy Supervisor scope.
                    if creator
                        .store
                        .find_host_session_scope(caller)
                        .await
                        .map_err(|error| {
                            ToolError::Execution(format!("Host Root scope unavailable: {error}"))
                        })?
                        .is_some()
                    {
                        return Err(ToolError::Execution("Host Root scope was installed before the legacy observation completed; read current scope again".into()));
                    }
                }
                Ok(result)
            }
            Self::Host { token, .. } => {
                let revision = self.current_revision(creator).await?;
                let creates: Vec<_> = token.binding().creates.values().collect();
                let available = self.first_input_available()
                    && !creates.is_empty()
                    && creates
                        .iter()
                        .any(|permit| permit.created_target_rights.is_empty());
                Ok(ToolResult::text(available, json!({
                    "action":"scope", "authority":"host_finite", "state_revision":revision,
                    "create_route_exposed":available, "creation_permits":creates,
                    "code":if !self.first_input_available() { "host_root_first_input_unavailable" }
                        else if creates.is_empty() { "host_root_create_not_granted" }
                        else if !available { "host_created_target_rights_unsupported" }
                        else { "host_root_create_configured" },
                    "note":"Only these exact operation UUID/Project/workspace permits authorize creation. The observation, creation receipt and created Root imply no target rights, management link or task completion."
                }).to_string()))
            }
        }
    }

    pub(super) async fn validate_request(
        &self,
        creator: &SupervisedRootCreator,
        request: &SupervisedRootCreateRequest,
    ) -> Result<u64, CreationAuthorizationError> {
        match self {
            Self::Supervisor {
                reference,
                original_caller,
            } => {
                if let Some(caller) = original_caller {
                    creator
                        .check_original_caller(caller)
                        .await
                        .map_err(|error| {
                            CreationAuthorizationError::blocked("scope_denied", error)
                        })?;
                }
                let scope = creator
                    .store
                    .inspect_supervisor_scope(reference)
                    .await
                    .map_err(CreationAuthorizationError::io)?;
                if !scope.allowed_projects.contains(&request.project_id) {
                    return Err(CreationAuthorizationError::blocked(
                        "scope_denied",
                        "Project is outside the current explicit Supervisor scope",
                    ));
                }
                Ok(scope.state_revision)
            }
            Self::Host { token, .. } => {
                let revision = self
                    .current_revision(creator)
                    .await
                    .map_err(|error| CreationAuthorizationError::blocked("scope_denied", error))?;
                if !self.first_input_available() {
                    return Err(CreationAuthorizationError::blocked(
                        "host_root_first_input_unavailable",
                        "This Host has no canonical create-purpose first-input port",
                    ));
                }
                let permit = token
                    .binding()
                    .creates
                    .get(&request.operation_id)
                    .filter(|scope| scope.project_id == request.project_id)
                    .ok_or_else(|| {
                        CreationAuthorizationError::blocked(
                            "scope_denied",
                            "This exact creation operation and Project are not granted",
                        )
                    })?;
                if !permit.created_target_rights.is_empty() {
                    return Err(CreationAuthorizationError::blocked(
                        "host_created_target_rights_unsupported",
                        "Created-target rights require a separate explicit Host installation",
                    ));
                }
                Ok(revision)
            }
        }
    }

    pub(super) fn validate_workspace(
        &self,
        request: &SupervisedRootCreateRequest,
        canonical_workspace: &std::path::Path,
    ) -> Result<(), CreationAuthorizationError> {
        if let Self::Host { token, .. } = self {
            // Keep the original request bytes for idempotency. A legacy retry
            // may use an alias of the same already-bound physical workspace.
            let workspace = bamboo_config::paths::path_to_display_string(canonical_workspace);
            token
                .binding()
                .create(request.operation_id, &request.project_id, &workspace)
                .map_err(|error| CreationAuthorizationError::blocked("scope_denied", error))?;
        }
        Ok(())
    }

    pub(super) async fn create_root(
        &self,
        creator: &SupervisedRootCreator,
        request: &SupervisedRootCreateRequest,
        candidate: &Session,
        replayed: bool,
    ) -> std::io::Result<CreationPublication> {
        match self {
            Self::Supervisor {
                reference,
                original_caller,
            } => {
                let relation = match original_caller {
                    Some(caller) => {
                        creator
                            .store
                            .create_and_attach_supervised_root_observed(
                                caller,
                                reference,
                                request.expected_state_revision,
                                candidate,
                            )
                            .await
                    }
                    None => {
                        creator
                            .store
                            .create_and_attach_supervised_root(
                                reference,
                                request.expected_state_revision,
                                candidate,
                            )
                            .await
                    }
                }?;
                Ok(CreationPublication {
                    state_revision: relation.state_revision,
                    link_revision: relation.link_revision,
                    linked: Some(true),
                })
            }
            Self::Host { caller, token, .. } => {
                creator
                    .store
                    .create_host_scoped_root(
                        token,
                        caller,
                        request.operation_id,
                        (!replayed).then_some(request.expected_state_revision),
                        candidate,
                    )
                    .await?;
                Ok(CreationPublication {
                    state_revision: token.binding().revision,
                    link_revision: None,
                    linked: None,
                })
            }
        }
    }

    pub(super) async fn first_input(
        &self,
        creator: &SupervisedRootCreator,
        request: &SupervisedRootCreateRequest,
        candidate: &Session,
    ) -> Result<SessionPeerDeliveryReceipt, SessionMessengerError> {
        match self {
            Self::Supervisor {
                reference,
                original_caller,
            } => match original_caller {
                Some(caller) => {
                    creator
                        .messenger
                        .supervisor_root_first_input_observed(
                            caller,
                            reference,
                            request.operation_id,
                            candidate,
                            &request.message,
                        )
                        .await
                }
                None => creator
                    .messenger
                    .supervisor_followup(
                        reference,
                        &candidate.id,
                        &request.operation_id.to_string(),
                        &request.message,
                    )
                    .await
                    .map(Into::into),
            },
            Self::Host {
                caller,
                token,
                first_input: Some(port),
            } => {
                port.deliver(
                    token,
                    caller,
                    request.operation_id,
                    candidate,
                    &request.message,
                )
                .await
            }
            Self::Host {
                first_input: None, ..
            } => Err(SessionMessengerError::Storage(
                "host_root_first_input_unavailable".into(),
            )),
        }
    }
}
