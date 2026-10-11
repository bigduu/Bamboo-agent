//! Per-activation, brokerless approval transport. The existing scoped reviewer
//! owns durable direct-parent requests and decisions; a boolean alone is never
//! enough to authorize the parked native operation.
use super::super::{
    actor_adapter::{ChildApprovalReview, ChildApprovalReviewer, ChildApprovalScope},
    approval_registry::{ApprovalState, DurableApproval, SharedApprovalRegistry},
};
use bamboo_domain::{
    ChildActorRuntimeWrite, ParentRequest, ParentRequestKind, ParentRequestOption,
    ParentResolution, Session, SessionMessageEnvelope, SessionMessageId, Storage,
};
use bamboo_tools::{
    permission::{
        PermissionConfig, PermissionDecisionKind, PermissionReasonCode, PermissionRequest,
    },
    ApprovalAsk, ApprovalProxy, LocalTaskResources,
};
use serde_json::{json, Value};
use std::{
    collections::HashSet,
    sync::{atomic::AtomicU64, Arc, Mutex},
};
use tokio_util::sync::CancellationToken;

pub(crate) struct LocalApprovalContext {
    pub store: Arc<bamboo_storage::SessionStoreV2>,
    pub owner: ChildActorRuntimeWrite,
    pub router: Arc<crate::SessionActivationRouter>,
    pub cancel: CancellationToken,
    pub policy: Arc<PermissionConfig>,
    pub reviewer: Arc<dyn ChildApprovalReviewer>,
    pub registry: Option<SharedApprovalRegistry>,
}

pub(crate) struct LocalApprovalProxy {
    context: Arc<LocalApprovalContext>,
    generations: Mutex<HashSet<String>>,
}

impl LocalApprovalProxy {
    pub fn new(context: LocalApprovalContext) -> Self {
        Self {
            context: Arc::new(context),
            generations: Mutex::new(HashSet::new()),
        }
    }
}

impl LocalApprovalContext {
    async fn current(&self, revision: u64) -> bool {
        !self.cancel.is_cancelled()
            && self.policy.policy_revision() == revision
            && self
                .router
                .owns_run(&self.owner.fence.actor_id, &self.owner.fence.run_id)
                .await
            && self
                .store
                .publish_child_actor_runtime_event(&self.owner, Box::new(|validate| validate()))
                .await
                .is_ok()
    }

    fn valid_request(&self, ask: &ApprovalAsk) -> Option<PermissionRequest> {
        let request = ask.permission_request.as_ref()?;
        if request.session_id != self.owner.fence.actor_id
            || request.request_id.is_empty()
            || request.request_id.len() > 256
            || request.tool_name != ask.tool_name
            || request.resource != ask.resource
            || request.permission_type.description() != ask.permission
            || request.policy_revision != self.policy.policy_revision()
            || !matches!(
                request.reason_code,
                PermissionReasonCode::HardDangerous | PermissionReasonCode::ConfiguredAlwaysAsk
            )
            || !request
                .allowed_decisions
                .contains(&PermissionDecisionKind::AllowOnce)
            || request.allowed_decisions.iter().any(|decision| {
                !matches!(
                    decision,
                    PermissionDecisionKind::AllowOnce | PermissionDecisionKind::DenyOnce
                )
            })
            || uuid::Uuid::parse_str(&request.request_generation)
                .ok()?
                .to_string()
                != request.request_generation
            || serde_json::to_vec(request).ok()?.len() > 8192
        {
            return None;
        }
        Some(request.clone())
    }

    async fn durable_approval(
        &self,
        request: &PermissionRequest,
        scope: &ChildApprovalScope,
    ) -> bool {
        let Ok(Some(parent)) = self.store.load_session(&self.owner.parent_session_id).await else {
            return false;
        };
        let Ok(Some(child)) = self.store.load_session(&self.owner.fence.actor_id).await else {
            return false;
        };
        if parent.created_at != self.owner.parent_created_at
            || child.created_at != self.owner.created_at
        {
            return false;
        }
        durable_approval(&parent, &child, request, scope)
    }
}

fn canonical_envelope(message: &bamboo_domain::Message) -> Option<SessionMessageEnvelope> {
    let envelope: SessionMessageEnvelope =
        serde_json::from_value(message.metadata.as_ref()?.get("session_message")?.clone()).ok()?;
    if !bamboo_domain::is_matching_session_message(message, &envelope)
        || serde_json::to_value(message).ok()?
            != serde_json::to_value(envelope.to_provider_message().ok()?).ok()?
    {
        return None;
    }
    Some(envelope)
}

fn durable_approval(
    parent: &Session,
    child: &Session,
    permission: &PermissionRequest,
    scope: &ChildApprovalScope,
) -> bool {
    let request_id = SessionMessageId::stable(
        "direct-parent-forced-approval-v1",
        &json!({
            "child": child.id, "birth": child.created_at,
            "parent": parent.id, "parent_birth": parent.created_at,
            "generation": permission.request_generation,
        }),
    );
    let Some(typed) = ParentRequest::inspect_direct_parent(parent, child, &request_id) else {
        return false;
    };
    let ParentRequestKind::ForcedPermission {
        operation_digest,
        policy_revision,
        ..
    } = &typed.kind;
    let Ok(permission_value) = serde_json::to_value(permission) else {
        return false;
    };
    let stamp = scope.stamp();
    if *operation_digest
        != SessionMessageId::stable("permission-operation", &permission_value).as_str()
        || *policy_revision != permission.policy_revision
        || typed.generation != permission.request_generation
        || typed.deadline != scope.deadline()
        || chrono::Utc::now() >= typed.deadline
        || stamp.get("host_scope").and_then(Value::as_str)
            != Some(typed.activation.host_scope.as_str())
        || stamp.get("attempt").and_then(Value::as_u64) != Some(u64::from(typed.activation.attempt))
        || stamp.get("run").and_then(Value::as_str) != Some(typed.activation.run.as_str())
        || stamp.get("epoch").and_then(Value::as_u64) != Some(typed.activation.epoch)
        || stamp.get("reply").and_then(Value::as_str) != Some(typed.activation.reply.as_str())
    {
        return false;
    }
    let terminal_id =
        SessionMessageId::stable("direct-parent-forced-terminal-v1", &json!(request_id));
    let mut request_envelope = None;
    let mut approved = false;
    let mut terminal_seen = false;
    for message in &parent.messages {
        let marker = message
            .metadata
            .as_ref()
            .and_then(|meta| meta.get("session_message"));
        let same_request = marker.is_some_and(|marker| {
            marker
                .pointer("/body/data/request_generation")
                .and_then(Value::as_str)
                == Some(permission.request_generation.as_str())
                || marker.pointer("/body/data/request").and_then(Value::as_str)
                    == Some(request_id.as_str())
        });
        if same_request && message.id != request_id.as_str() && message.id != terminal_id.as_str() {
            return false;
        }
        if message.id == request_id.as_str() {
            if request_envelope.is_some() || terminal_seen {
                return false;
            }
            request_envelope = canonical_envelope(message);
            if request_envelope.is_none() {
                return false;
            }
        } else if message.id == terminal_id.as_str() {
            if terminal_seen {
                return false;
            }
            let Some(request) = request_envelope.as_ref() else {
                return false;
            };
            let Some(terminal) = canonical_envelope(message) else {
                return false;
            };
            let Some(resolution) =
                ParentResolution::from_forced_permission_terminal(request, &terminal)
            else {
                return false;
            };
            terminal_seen = true;
            approved = resolution.decision == ParentRequestOption::ApproveOnce;
        }
    }
    terminal_seen && approved
}

/// Cancel the scoped review and fail-close its optional audit record on every
/// early exit, including a dropped tool future. No native operation is detached.
struct ReviewGuard {
    context: Arc<LocalApprovalContext>,
    cancel: CancellationToken,
    attempt: u32,
    request_id: String,
    registered: bool,
    finished: bool,
}
impl ReviewGuard {
    fn register(&mut self, request: &PermissionRequest) -> bool {
        let Some(registry) = &self.context.registry else {
            return true;
        };
        let now = chrono::Utc::now();
        let record = DurableApproval {
            parent_session_id: self.context.owner.parent_session_id.clone(),
            child_session_id: self.context.owner.fence.actor_id.clone(),
            child_attempt: self.attempt,
            request_id: self.request_id.clone(),
            tool_name: request.tool_name.clone(),
            permission: request.permission_type.description().into(),
            // Commands, credential-bearing URLs and keyed browser resources
            // remain transient. The canonical parent audit applies its own
            // established redaction rules and exact operation digest.
            resource: "[redacted]".into(),
            created_at: now.to_rfc3339(),
            updated_at: now.to_rfc3339(),
            version: now.timestamp_micros().max(0) as u64,
            state: ApprovalState::Pending,
            approved: None,
            reason: None,
        };
        let Ok(mut registry) = registry.lock() else {
            return false;
        };
        self.registered = registry.register(record).is_ok();
        self.registered
    }
    fn finish(&mut self, approved: bool) -> bool {
        if let Some(registry) = &self.context.registry {
            let Ok(mut registry) = registry.lock() else {
                return false;
            };
            if !matches!(
                registry.record_decision(
                    &self.context.owner.parent_session_id,
                    &self.context.owner.fence.actor_id,
                    self.attempt,
                    &self.request_id,
                    approved
                ),
                Ok(Some(_))
            ) || !matches!(
                registry.finish(
                    &self.context.owner.parent_session_id,
                    &self.context.owner.fence.actor_id,
                    self.attempt,
                    &self.request_id,
                    true,
                    None
                ),
                Ok(Some(_))
            ) {
                return false;
            }
        }
        self.finished = true;
        true
    }
}
impl Drop for ReviewGuard {
    fn drop(&mut self) {
        self.cancel.cancel();
        if self.registered && !self.finished {
            if let Some(registry) = &self.context.registry {
                if let Ok(mut registry) = registry.lock() {
                    let _ = registry.finish(
                        &self.context.owner.parent_session_id,
                        &self.context.owner.fence.actor_id,
                        self.attempt,
                        &self.request_id,
                        false,
                        Some("local_approval_scope_ended"),
                    );
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl ApprovalProxy for LocalApprovalProxy {
    async fn request_approval(&self, ask: ApprovalAsk) -> bool {
        let context = &self.context;
        let Some(request) = context.valid_request(&ask) else {
            return false;
        };
        let Some(resources) = LocalTaskResources::current() else {
            return false;
        };
        if resources.identity().session_id != context.owner.fence.actor_id
            || resources.identity().run_id != context.owner.fence.run_id
            || resources.identity().child_birth != context.owner.created_at
            || !context.current(request.policy_revision).await
        {
            return false;
        }
        {
            let Ok(mut generations) = self.generations.lock() else {
                return false;
            };
            if generations.len() >= 64 || !generations.insert(request.request_generation.clone()) {
                return false;
            }
        }
        let Ok(attempt) = u32::try_from(context.owner.fence.attempt) else {
            return false;
        };
        let cancel = context.cancel.child_token();
        let scope = ChildApprovalScope::new(
            &context.owner.parent_session_id,
            &context.owner.fence.actor_id,
            (
                attempt,
                &context.owner.fence.run_id,
                context.owner.fence.lease_epoch,
                &request.request_generation,
                chrono::Utc::now() + chrono::Duration::seconds(240),
            ),
            context.router.clone(),
            cancel.clone(),
            Arc::new(AtomicU64::new(context.owner.fence.lease_epoch)),
        );
        let mut guard = ReviewGuard {
            context: context.clone(),
            cancel: cancel.clone(),
            attempt,
            request_id: request.request_generation.clone(),
            registered: false,
            finished: false,
        };
        if !guard.register(&request) {
            return false;
        }
        let body = json!({
            "tool_name": ask.tool_name, "permission": ask.permission, "resource": ask.resource,
            "permission_request": request,
            "approval_identity": {
                "logical_session": {
                    "session_id": context.owner.fence.actor_id,
                    "parent_session_id": context.owner.parent_session_id,
                    "root_session_id": context.owner.root_session_id,
                    "creation": { "created_at": context.owner.created_at, "spawn_depth": context.owner.spawn_depth }
                },
                "project_id": context.owner.project_id,
            }
        });
        let review_context = context.clone();
        let review_scope = scope.clone();
        let remaining = (scope.deadline() - chrono::Utc::now())
            .to_std()
            .unwrap_or_default();
        let Ok(reply) = resources.spawn_owned(async move {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => ChildApprovalReview::Reply(false),
                _ = tokio::time::sleep(remaining) => ChildApprovalReview::Reply(false),
                result = review_context.reviewer.review_scoped(&review_context.owner.parent_session_id,
                    &review_context.owner.fence.actor_id, &body, &review_scope) => result,
            }
        }) else { return false };
        let Ok(result) = reply.await else {
            return false;
        };
        let approved = result == ChildApprovalReview::Reply(true)
            && scope
                .is_current(
                    &context.owner.parent_session_id,
                    &context.owner.fence.actor_id,
                )
                .await
            && context.current(request.policy_revision).await
            && context.durable_approval(&request, &scope).await;
        guard.finish(approved) && approved && context.current(request.policy_revision).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::{
        ActorActivationClaim, ActorDirectoryPort, ParentRequestActivation, ParentRequestActor,
        ParentRequestDelegation, SessionMessageBody, SessionMessageContent, SessionMessageKind,
        SessionMessageSource, SessionProviderMessage, SessionRuntimeInstruction,
        PARENT_REQUEST_VERSION,
    };
    use bamboo_tools::permission::{PermissionMode, PermissionType, RiskLevel};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn record_decision(
        parent: &mut Session,
        child: &Session,
        permission: &PermissionRequest,
        scope: &ChildApprovalScope,
        approved: bool,
    ) {
        let id = SessionMessageId::stable(
            "direct-parent-forced-approval-v1",
            &json!({
                "child": child.id, "birth": child.created_at, "parent": parent.id,
                "parent_birth": parent.created_at, "generation": permission.request_generation,
            }),
        );
        let digest = SessionMessageId::stable(
            "permission-operation",
            &serde_json::to_value(permission).unwrap(),
        )
        .to_string();
        let stamp = scope.stamp();
        let request = ParentRequest {
            version: PARENT_REQUEST_VERSION,
            id: id.clone(),
            generation: permission.request_generation.clone(),
            child: ParentRequestActor {
                session_id: child.id.clone(),
                created_at: child.created_at,
            },
            parent: ParentRequestActor {
                session_id: parent.id.clone(),
                created_at: parent.created_at,
            },
            root_session_id: child.root_session_id.clone(),
            project_id: child.project_id_meta(),
            activation: ParentRequestActivation {
                host_scope: stamp["host_scope"].as_str().unwrap().into(),
                attempt: stamp["attempt"].as_u64().unwrap() as u32,
                run: stamp["run"].as_str().unwrap().into(),
                epoch: stamp["epoch"].as_u64().unwrap(),
                reply: stamp["reply"].as_str().unwrap().into(),
            },
            deadline: scope.deadline(),
            kind: ParentRequestKind::ForcedPermission {
                operation_digest: digest.clone(),
                policy_revision: permission.policy_revision,
                maximum_delegation: ParentRequestDelegation::ExactOperationOnce,
                options: vec![ParentRequestOption::Deny, ParentRequestOption::ApproveOnce],
            },
        };
        let content = SessionMessageContent::text("fixture direct-parent permission request");
        let mut envelope = SessionMessageEnvelope {
            id,
            source: SessionMessageSource::Runtime {
                subsystem: "direct_parent_permission_review".into(),
            },
            target_session_id: parent.id.clone(),
            kind: SessionMessageKind::RuntimeInstruction,
            body: SessionMessageBody::RuntimeInstruction(SessionRuntimeInstruction {
                instruction: "direct_parent_forced_permission_request_v1".into(),
                content: Some(content.clone()),
                provider_message: Some(SessionProviderMessage {
                    content,
                    metadata: Default::default(),
                    never_compress: true,
                }),
                data: Some(json!({"parent_request":request, "live":stamp,
                    "child_session_id": child.id, "child_created_at":child.created_at,
                    "parent_created_at":parent.created_at, "root_session_id":child.root_session_id,
                    "project_id":child.project_id_meta(), "request_generation":permission.request_generation,
                    "operation_digest":digest, "policy_revision":permission.policy_revision,
                })),
            }),
            created_at: scope.deadline() - chrono::Duration::seconds(240),
            thread_id: None,
            in_reply_to: None,
            attempt: None,
            correlation_id: None,
        };
        assert!(ParentRequest::from_forced_permission_envelope(&envelope).is_some());
        parent
            .messages
            .push(envelope.to_provider_message().unwrap());
        let resolution =
            ParentResolution::for_forced_permission(&request, chrono::Utc::now(), approved);
        envelope.id = resolution.id.clone();
        envelope.in_reply_to = Some(request.id.clone());
        envelope.created_at = resolution.resolved_at;
        let SessionMessageBody::RuntimeInstruction(body) = &mut envelope.body else {
            unreachable!()
        };
        body.instruction = "direct_parent_forced_permission_terminal_v1".into();
        body.data = Some(
            json!({"request":request.id, "live":stamp, "approved":approved, "parent_resolution":resolution}),
        );
        parent
            .messages
            .push(envelope.to_provider_message().unwrap());
    }

    struct Reviewer {
        store: Arc<bamboo_storage::SessionStoreV2>,
        durable: bool,
        calls: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl ChildApprovalReviewer for Reviewer {
        async fn review(&self, _: &str, _: &str, _: &Value) -> bool {
            panic!("unscoped review cannot authorize a local operation")
        }
        async fn review_scoped(
            &self,
            parent: &str,
            child: &str,
            body: &Value,
            scope: &ChildApprovalScope,
        ) -> ChildApprovalReview {
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert!(scope.is_current(parent, child).await);
            if self.durable {
                let mut parent = self.store.load_session(parent).await.unwrap().unwrap();
                let child = self.store.load_session(child).await.unwrap().unwrap();
                let permission: PermissionRequest =
                    serde_json::from_value(body["permission_request"].clone()).unwrap();
                record_decision(&mut parent, &child, &permission, scope, true);
                self.store.save_session(&parent).await.unwrap();
            }
            ChildApprovalReview::Reply(true)
        }
    }

    struct Fixture {
        proxy: LocalApprovalProxy,
        reviewer: Arc<Reviewer>,
        resources: LocalTaskResources,
        ask: ApprovalAsk,
        _registration: crate::SessionRunRegistration,
        _home: tempfile::TempDir,
    }
    impl Fixture {
        async fn new(durable: bool) -> Self {
            let home = tempfile::tempdir().unwrap();
            let store = Arc::new(
                bamboo_storage::SessionStoreV2::new(home.path().into())
                    .await
                    .unwrap(),
            );
            let parent = Session::new("local-approval-parent", "test-model");
            store.save_session(&parent).await.unwrap();
            let child =
                Session::new_child_of("local-approval-child", &parent, "test-model", "Child");
            store.save_session(&child).await.unwrap();
            let now = chrono::Utc::now();
            let activation = store
                .claim_activation(&ActorActivationClaim {
                    actor_id: child.id.clone(),
                    run_id: "local-approval-run".into(),
                    lease_owner: "local-host".into(),
                    lease_expires_at: now + chrono::Duration::minutes(5),
                    inbox_generation: 0,
                    placement_ref: None,
                    now,
                })
                .await
                .unwrap();
            store
                .start_activation(&activation.fence(), now)
                .await
                .unwrap();
            let owner = ChildActorRuntimeWrite::from_session(activation.fence(), &child).unwrap();
            let router = crate::SessionActivationRouter::new();
            let registration = router
                .register_run(&child.id, "local-approval-run")
                .await
                .unwrap();
            let policy = Arc::new(PermissionConfig::new());
            let reviewer = Arc::new(Reviewer {
                store: store.clone(),
                durable,
                calls: AtomicUsize::new(0),
            });
            let registry = Arc::new(Mutex::new(
                super::super::super::approval_registry::ApprovalRegistry::open(
                    home.path().join("approvals.json"),
                )
                .unwrap(),
            ));
            let proxy = LocalApprovalProxy::new(LocalApprovalContext {
                store,
                owner,
                router,
                cancel: CancellationToken::new(),
                policy: policy.clone(),
                reviewer: reviewer.clone(),
                registry: Some(registry),
            });
            let request = PermissionRequest {
                request_id: "forced-write".into(),
                request_generation: PermissionRequest::fresh_generation(),
                session_id: child.id.clone(),
                workspace_path: None,
                tool_name: "Write".into(),
                permission_type: PermissionType::WriteFile,
                resource: "/fixture/secret-action".into(),
                operation_summary: "Create one fixture file".into(),
                risk_level: RiskLevel::Medium,
                reason_code: PermissionReasonCode::ConfiguredAlwaysAsk,
                effective_mode: PermissionMode::Default,
                bypass_requested: false,
                auto_approve_requested: false,
                policy_revision: policy.policy_revision(),
                matched_rule: None,
                allowed_decisions: PermissionRequest::forced_decisions(),
                suggested_matchers: vec![],
            };
            let ask = ApprovalAsk {
                tool_name: request.tool_name.clone(),
                permission: request.permission_type.description().into(),
                resource: request.resource.clone(),
                permission_request: Some(request),
            };
            Self {
                proxy,
                reviewer,
                resources: LocalTaskResources::new(
                    &child.id,
                    "local-approval-run",
                    child.created_at,
                ),
                ask,
                _registration: registration,
                _home: home,
            }
        }
        async fn approve(&self) -> bool {
            self.resources
                .scope(self.proxy.request_approval(self.ask.clone()))
                .await
        }
    }

    #[tokio::test]
    async fn local_approval_requires_durable_parent_decision_and_consumes_generation_once() {
        let f = Fixture::new(true).await;
        assert!(f.approve().await);
        assert!(!f.approve().await);
        assert_eq!(f.reviewer.calls.load(Ordering::SeqCst), 1);
        let parent = f
            .proxy
            .context
            .store
            .load_session(&f.proxy.context.owner.parent_session_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(parent.messages.len(), 2);
        assert!(f
            .proxy
            .context
            .registry
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .unresolved_snapshot()
            .approvals
            .is_empty());
        let audit = std::fs::read_to_string(f._home.path().join("approvals.json")).unwrap();
        assert!(!audit.contains("secret-action"));
        assert!(audit.contains("delivered"));
        f.resources.close_and_drain().await.unwrap();
    }

    #[tokio::test]
    async fn local_approval_rejects_boolean_only_reviewer_and_untyped_or_broad_requests() {
        let f = Fixture::new(false).await;
        assert!(!f.approve().await);
        assert_eq!(f.reviewer.calls.load(Ordering::SeqCst), 1);
        for reason in [
            PermissionReasonCode::PlatformHardDeny,
            PermissionReasonCode::ExplicitDeny,
            PermissionReasonCode::RiskThreshold,
        ] {
            let mut ask = f.ask.clone();
            ask.permission_request.as_mut().unwrap().reason_code = reason;
            assert!(!f.resources.scope(f.proxy.request_approval(ask)).await);
        }
        let mut broad = f.ask.clone();
        broad
            .permission_request
            .as_mut()
            .unwrap()
            .allowed_decisions
            .push(PermissionDecisionKind::AllowGlobal);
        assert!(!f.resources.scope(f.proxy.request_approval(broad)).await);
        let mut untyped = f.ask.clone();
        untyped.permission_request = None;
        assert!(!f.resources.scope(f.proxy.request_approval(untyped)).await);
        assert_eq!(f.reviewer.calls.load(Ordering::SeqCst), 1);
        f.resources.close_and_drain().await.unwrap();
    }

    #[tokio::test]
    async fn local_approval_rejects_stale_owner_cancel_and_resource_scope_before_review() {
        let f = Fixture::new(true).await;
        assert!(!f.proxy.request_approval(f.ask.clone()).await);
        let wrong = LocalTaskResources::new(
            &f.proxy.context.owner.fence.actor_id,
            "another-run",
            f.proxy.context.owner.created_at,
        );
        assert!(!wrong.scope(f.proxy.request_approval(f.ask.clone())).await);
        wrong.close_and_drain().await.unwrap();
        f.proxy.context.cancel.cancel();
        assert!(!f.approve().await);
        assert_eq!(f.reviewer.calls.load(Ordering::SeqCst), 0);
        f.resources.close_and_drain().await.unwrap();
    }

    #[tokio::test]
    async fn local_approval_rechecks_the_canonical_activation_fence() {
        let f = Fixture::new(true).await;
        f.proxy
            .context
            .store
            .finish_activation(
                &f.proxy.context.owner.fence,
                chrono::Utc::now(),
                bamboo_domain::ActorActivationFinish::Cancelled,
            )
            .await
            .unwrap();
        // The in-memory router still owns this run; canonical revocation wins.
        assert!(
            f.proxy
                .context
                .router
                .owns_run(
                    &f.proxy.context.owner.fence.actor_id,
                    &f.proxy.context.owner.fence.run_id,
                )
                .await
        );
        assert!(!f.approve().await);
        assert_eq!(f.reviewer.calls.load(Ordering::SeqCst), 0);
        f.resources.close_and_drain().await.unwrap();
    }
}
