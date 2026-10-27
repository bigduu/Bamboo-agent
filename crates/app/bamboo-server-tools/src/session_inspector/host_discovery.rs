//! Role-neutral discovery through the Host's retained canonical read ports.

use std::sync::Arc;

use async_trait::async_trait;
use bamboo_agent_core::tools::{ToolCtx, ToolError};
use bamboo_domain::{
    HostSessionScopeToken, SessionCallerReference, Storage, MAX_HOST_SESSION_TARGETS,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::discovery::{
    arguments, authority_required, authority_unavailable, caller_fingerprint, progress_observation,
    stale, AuthorizedSessionInspectRequest, AuthorizedSessionListRequest, AuthorizedSessionPage,
    AuthorizedSessionTarget, SessionDiscoveryPort, SessionProgressObservation,
    SupervisorSessionDiscovery, MAX_CURSOR_BYTES, MAX_DIRECTORY_PAGE_SIZE,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HostDirectoryCursor {
    version: u8,
    grant_id: Uuid,
    revision: u64,
    caller_sha256: String,
    query: Option<String>,
    limit: usize,
    after: String,
}

pub struct HostSessionDiscovery {
    storage: Arc<dyn Storage>,
}

impl HostSessionDiscovery {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self { storage }
    }

    async fn list_with_token(
        &self,
        caller: &SessionCallerReference,
        token: &HostSessionScopeToken,
        request: AuthorizedSessionListRequest,
    ) -> Result<AuthorizedSessionPage, ToolError> {
        request.validate()?;
        let binding = token.binding();
        let caller_sha256 = caller_fingerprint(caller)?;
        let query = request
            .query
            .as_deref()
            .map(str::trim)
            .filter(|q| !q.is_empty())
            .map(str::to_lowercase);
        let limit = request.limit.unwrap_or(MAX_DIRECTORY_PAGE_SIZE);
        let after = request
            .cursor
            .as_ref()
            .map(|value| {
                let cursor: HostDirectoryCursor = serde_json::from_str(value)
                    .map_err(|_| arguments("malformed Host Session cursor"))?;
                if cursor.version != 1
                    || cursor.grant_id != binding.grant_id
                    || cursor.revision != binding.revision
                    || cursor.caller_sha256 != caller_sha256
                    || cursor.query != query
                    || cursor.limit != limit
                {
                    return Err(stale(
                        "Host grant, original caller generation or query changed; start a new page",
                    ));
                }
                bamboo_domain::validate_supervisor_target_id(&cursor.after).map_err(arguments)?;
                Ok(cursor.after)
            })
            .transpose()?;
        // This port retains publication/lifecycle/Task/Session/tree authority
        // across all physical metadata observations. No index/preflight read.
        let targets = self
            .storage
            .list_host_scoped_targets(token, caller)
            .await
            .map_err(authority_unavailable)?;
        if targets.len() > MAX_HOST_SESSION_TARGETS {
            return Err(authority_unavailable("Host target capacity exceeded"));
        }
        let mut authorized = Vec::with_capacity(targets.len());
        for target in targets {
            let observation = target.observation;
            let id = observation.identity.session_id;
            let project = observation
                .project_id
                .ok_or_else(|| authority_unavailable("Host target Project is unavailable"))?;
            if query.as_ref().is_some_and(|query| {
                !id.to_lowercase().contains(query)
                    && !project.as_str().to_lowercase().contains(query)
            }) {
                continue;
            }
            authorized.push(AuthorizedSessionTarget {
                session_id: id,
                project_id: Some(project),
                created_at: observation.identity.created_at,
                metadata_version: observation.metadata_version,
                link_revision: 0,
                authorization_revision: Some(binding.revision),
                current_user_target: None,
            });
        }
        authorized.sort_by(|a, b| a.session_id.cmp(&b.session_id));
        if after
            .as_ref()
            .is_some_and(|id| !authorized.iter().any(|target| &target.session_id == id))
        {
            return Err(stale("cursor target is no longer in the authorized result"));
        }
        let mut sessions: Vec<_> = authorized
            .into_iter()
            .filter(|target| {
                after
                    .as_ref()
                    .is_none_or(|after| target.session_id > *after)
            })
            .take(limit + 1)
            .collect();
        let has_more = sessions.len() > limit;
        sessions.truncate(limit);
        let next_cursor = if has_more {
            let value = serde_json::to_string(&HostDirectoryCursor {
                version: 1,
                grant_id: binding.grant_id,
                revision: binding.revision,
                caller_sha256,
                query,
                limit,
                after: sessions
                    .last()
                    .expect("nonempty bounded page")
                    .session_id
                    .clone(),
            })
            .map_err(authority_unavailable)?;
            if value.len() > MAX_CURSOR_BYTES {
                return Err(authority_unavailable("Host cursor exceeds its bound"));
            }
            Some(value)
        } else {
            None
        };
        Ok(AuthorizedSessionPage {
            caller_session_id: caller.session_id.clone(),
            scope_revision: binding.revision,
            sessions,
            next_cursor,
        })
    }

    async fn inspect_with_token(
        &self,
        caller: &SessionCallerReference,
        token: &HostSessionScopeToken,
        request: AuthorizedSessionInspectRequest,
    ) -> Result<SessionProgressObservation, ToolError> {
        request.validate()?;
        // InspectBoundedProgress is checked at this final canonical reader;
        // ListMetadata is neither a transcript nor an inspection grant.
        let session = self
            .storage
            .load_host_scoped_target(token, caller, &request.session_id)
            .await
            .map_err(authority_unavailable)?;
        Ok(progress_observation(
            &session,
            token.binding().revision,
            request.max_chars.unwrap_or(2_000),
        ))
    }
}

#[async_trait]
impl SessionDiscoveryPort for HostSessionDiscovery {
    async fn list(
        &self,
        ctx: &ToolCtx,
        request: AuthorizedSessionListRequest,
    ) -> Result<AuthorizedSessionPage, ToolError> {
        let caller = original_caller(ctx)?;
        let token = self
            .storage
            .find_host_session_scope(caller)
            .await
            .map_err(authority_unavailable)?
            .ok_or_else(authority_required)?;
        self.list_with_token(caller, &token, request).await
    }
    async fn inspect(
        &self,
        ctx: &ToolCtx,
        request: AuthorizedSessionInspectRequest,
    ) -> Result<SessionProgressObservation, ToolError> {
        let caller = original_caller(ctx)?;
        let token = self
            .storage
            .find_host_session_scope(caller)
            .await
            .map_err(authority_unavailable)?
            .ok_or_else(authority_required)?;
        self.inspect_with_token(caller, &token, request).await
    }
}

/// An installed Host token selects one route irrevocably for this call. Only
/// never-installed absence permits the real original Supervisor compatibility
/// reader. Denied/stale/revoked/unsupported scope never becomes another route.
pub struct SessionDiscoveryRouter {
    storage: Arc<dyn Storage>,
    host: HostSessionDiscovery,
    supervisor: SupervisorSessionDiscovery,
}

impl SessionDiscoveryRouter {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self {
            host: HostSessionDiscovery::new(storage.clone()),
            supervisor: SupervisorSessionDiscovery::new(storage.clone()),
            storage,
        }
    }
}

#[async_trait]
impl SessionDiscoveryPort for SessionDiscoveryRouter {
    async fn list(
        &self,
        ctx: &ToolCtx,
        request: AuthorizedSessionListRequest,
    ) -> Result<AuthorizedSessionPage, ToolError> {
        request.validate()?;
        let caller = original_caller(ctx)?;
        if ctx.executing_supervisor_for(&caller.session_id).is_some() {
            validate_original_caller(self.storage.as_ref(), ctx).await?;
            return self.supervisor.list(ctx, request).await;
        }
        match self
            .storage
            .find_host_session_scope(caller)
            .await
            .map_err(authority_unavailable)?
        {
            Some(token) => self.host.list_with_token(caller, &token, request).await,
            None => {
                require_legacy_supervisor(ctx)?;
                let page = self.supervisor.list(ctx, request).await?;
                // Publication may have narrowed/revoked the legacy route while
                // its canonical reader was waiting. Do not return its old view.
                require_absent_host_for_legacy_history(self.storage.as_ref(), ctx).await?;
                Ok(page)
            }
        }
    }
    async fn inspect(
        &self,
        ctx: &ToolCtx,
        request: AuthorizedSessionInspectRequest,
    ) -> Result<SessionProgressObservation, ToolError> {
        request.validate()?;
        let caller = original_caller(ctx)?;
        if ctx.executing_supervisor_for(&caller.session_id).is_some() {
            validate_original_caller(self.storage.as_ref(), ctx).await?;
            return self.supervisor.inspect(ctx, request).await;
        }
        match self
            .storage
            .find_host_session_scope(caller)
            .await
            .map_err(authority_unavailable)?
        {
            Some(token) => self.host.inspect_with_token(caller, &token, request).await,
            None => {
                require_legacy_supervisor(ctx)?;
                let observation = self.supervisor.inspect(ctx, request).await?;
                require_absent_host_for_legacy_history(self.storage.as_ref(), ctx).await?;
                Ok(observation)
            }
        }
    }
}

fn original_caller(ctx: &ToolCtx) -> Result<&SessionCallerReference, ToolError> {
    ctx.session_id()
        .and_then(|id| ctx.executing_caller_for(id))
        .ok_or_else(authority_required)
}

fn require_legacy_supervisor(ctx: &ToolCtx) -> Result<(), ToolError> {
    ctx.session_id()
        .and_then(|id| ctx.executing_supervisor_for(id))
        .map(|_| ())
        .ok_or_else(|| ToolError::Execution("session_discovery_scope_required: no Host read scope is installed for this caller; Project membership or known IDs grant no cross-Session read".into()))
}

pub(super) async fn validate_original_caller(
    storage: &dyn Storage,
    ctx: &ToolCtx,
) -> Result<(), ToolError> {
    let caller = original_caller(ctx)?;
    let current = storage
        .load_execution_admission_snapshot(&caller.session_id)
        .await
        .map_err(authority_unavailable)?
        .ok_or_else(authority_required)?;
    current
        .check_execution_compatibility()
        .map_err(authority_unavailable)?;
    if !caller.matches_session(&current) {
        return Err(authority_required());
    }
    Ok(())
}

pub(super) async fn require_absent_host_for_legacy_history(
    storage: &dyn Storage,
    ctx: &ToolCtx,
) -> Result<(), ToolError> {
    require_legacy_supervisor(ctx)?;
    validate_original_caller(storage, ctx).await?;
    let caller = original_caller(ctx)?;
    let reference = ctx
        .executing_supervisor_for(&caller.session_id)
        .ok_or_else(authority_required)?
        .supervisor_reference();
    // The old private helper name is retained for its existing call sites;
    // current Supervisor authority no longer borrows a finite Host scope.
    storage
        .inspect_supervisor_current_user_scope(&reference)
        .await
        .map_err(authority_unavailable)?;
    validate_original_caller(storage, ctx).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::{ConversationResetReceipt, ConversationResetState, Session};

    #[test]
    fn cursor_digest_binds_the_complete_ledger_without_embedding_it_or_workspace() {
        // Pure data only: no generation is published and no token is issued.
        let mut source = Session::new("large-caller", "model");
        source.workspace = Some(format!("/private/{}", "workspace".repeat(600)));
        source.conversation_reset = Some(ConversationResetState {
            schema_version: 1,
            conversation_revision: 8,
            receipts: (1..=8)
                .map(|revision| ConversationResetReceipt {
                    operation_id: format!("{}:{}", revision - 1, Uuid::new_v4()),
                    birth_token: source.root_mode_birth_token(),
                    expected_snapshot_sha256: "a".repeat(64),
                    previous_revision: revision - 1,
                    conversation_revision: revision,
                    retained_system_messages: 1,
                    reset_at: source.created_at,
                })
                .collect(),
        });
        source.validate_conversation_state().unwrap();
        let original = SessionCallerReference::from(&source);
        assert!(serde_json::to_vec(&original).unwrap().len() > MAX_CURSOR_BYTES);
        let digest = caller_fingerprint(&original).unwrap();
        let cursor = serde_json::to_string(&HostDirectoryCursor {
            version: 1,
            grant_id: Uuid::new_v4(),
            revision: 1,
            caller_sha256: digest.clone(),
            query: Some("project".into()),
            limit: 1,
            after: "target".into(),
        })
        .unwrap();
        assert!(cursor.len() < MAX_CURSOR_BYTES);
        for private in ["receipts", "workspace", "birth_token", "conversation_reset"] {
            assert!(!cursor.contains(private));
        }
        let mut changed = original;
        changed.conversation_reset.as_mut().unwrap().receipts[0].expected_snapshot_sha256 =
            "b".repeat(64);
        assert_ne!(
            digest,
            caller_fingerprint(&changed).unwrap(),
            "same revision with a different receipt is a different original observation"
        );
    }
}
