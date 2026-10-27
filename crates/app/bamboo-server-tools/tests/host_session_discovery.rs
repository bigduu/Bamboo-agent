//! Explicit Host grants in temporary stores only. No real accounts or resets.

use std::{collections::BTreeMap, io, sync::Arc};

use async_trait::async_trait;
use bamboo_agent_core::tools::{ExecutingSupervisorObservation, ToolClass, ToolOutcome};
use bamboo_agent_core::{Message, Session, Storage, Tool, ToolCtx};
use bamboo_domain::{
    ActorAncestorObservation, HostScopeSessionObservation, HostSessionRight as Right,
    HostSessionScopeBinding, HostSessionScopeToken, HostSessionTargetScope, SessionCallerReference,
    SupervisorLinkObservation, SupervisorManagementMutation as Mutation,
    SupervisorManagementRequest, SupervisorReference, HOST_SESSION_SCOPE_VERSION,
};
use bamboo_server_tools::session_inspector::{
    discovery::{AuthorizedSessionListRequest, SessionDiscoveryPort},
    host_discovery::HostSessionDiscovery,
};
use bamboo_server_tools::SessionInspectorTool;
use bamboo_storage::SessionStoreV2;
use serde_json::{json, Value};

struct Fixture {
    home: tempfile::TempDir,
    store: Arc<SessionStoreV2>,
    caller: Session,
    targets: Vec<Session>,
}

impl Fixture {
    async fn new() -> Self {
        let home = tempfile::tempdir().unwrap();
        let store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
        let mut caller = Session::new("ordinary-reader", "model");
        caller.set_project_id_meta("caller-project");
        caller.add_message(Message::system("private own system"));
        caller.add_message(Message::user("own safe needle"));
        store.save_session(&caller).await.unwrap();
        let mut targets = vec![];
        for id in ["aa-one", "bb-two", "cc-three", "zz-ungranted"] {
            let mut target = Session::new(id, "private model");
            target.set_project_id_meta("target-project");
            target.title = "private target title".repeat(30);
            target.add_message(Message::system("private target system"));
            target.add_message(Message::user("private target user"));
            let mut visible = Message::assistant("结果🎋".repeat(100), None);
            visible.reasoning = Some("private target reasoning".into());
            target.add_message(visible);
            let mut hidden = Message::assistant("private hidden assistant", None);
            hidden.metadata = Some(json!({"hidden_from_ui":true}));
            target.add_message(hidden);
            store.save_session(&target).await.unwrap();
            targets.push(target);
        }
        Self {
            home,
            store,
            caller,
            targets,
        }
    }

    fn ctx(caller: &Session) -> ToolCtx {
        let mut ctx = ToolCtx::none("host-discovery-fixture");
        ctx.session_id = Some(Arc::from(caller.id.as_str()));
        ctx.executing_caller = Some(Arc::new(SessionCallerReference::from(caller)));
        ctx.executing_supervisor =
            ExecutingSupervisorObservation::capture_from_executing_session(caller);
        ctx
    }

    fn tool(&self) -> SessionInspectorTool {
        SessionInspectorTool::host_routed(self.store.clone(), self.store.clone())
    }

    async fn install(
        &self,
        caller: &Session,
        ancestors: Vec<ActorAncestorObservation>,
        targets: &[(usize, Vec<Right>)],
        expected: u64,
    ) -> HostSessionScopeToken {
        let binding = HostSessionScopeBinding {
            schema_version: HOST_SESSION_SCOPE_VERSION,
            grant_id: uuid::Uuid::new_v4(),
            revision: expected + 1,
            caller: HostScopeSessionObservation::capture(caller, ancestors).unwrap(),
            targets: targets
                .iter()
                .map(|(index, rights)| {
                    let target = &self.targets[*index];
                    (
                        target.id.clone(),
                        HostSessionTargetScope {
                            observation: HostScopeSessionObservation::capture(target, vec![])
                                .unwrap(),
                            rights: rights.iter().copied().collect(),
                        },
                    )
                })
                .collect(),
            creates: BTreeMap::new(),
        };
        self.store
            .install_host_session_scope(expected, binding)
            .await
            .unwrap()
    }

    async fn supervisor(&self) -> Session {
        let receipt = self
            .store
            .get_or_create_default_supervisor("model")
            .await
            .unwrap();
        let reference: SupervisorReference = (&receipt).into();
        for (revision, mutation) in [
            Mutation::ConfigureProjectScope {
                allowed_projects: ["target-project".parse().unwrap()].into(),
            },
            Mutation::Attach {
                target_session_id: self.targets[0].id.clone(),
            },
        ]
        .into_iter()
        .enumerate()
        {
            self.store
                .mutate_supervisor_management(&SupervisorManagementRequest {
                    supervisor: reference.clone(),
                    expected_state_revision: revision as u64,
                    mutation,
                })
                .await
                .unwrap();
        }
        self.store
            .load_session(&receipt.session_id)
            .await
            .unwrap()
            .unwrap()
    }
}

fn result(outcome: ToolOutcome) -> Value {
    let ToolOutcome::Completed(result) = outcome else {
        panic!("read-only discovery cannot suspend")
    };
    assert!(result.success);
    serde_json::from_str(&result.result).unwrap()
}

#[tokio::test]
async fn installed_host_scope_serves_root_child_and_supervisor_without_role_impersonation() {
    let fixture = Fixture::new().await;
    let mut child =
        Session::new_child_of("short-child", &fixture.caller, "model", "temporary work");
    child.set_project_id_meta(fixture.caller.project_id_meta().unwrap());
    fixture.store.save_session(&child).await.unwrap();
    let supervisor = fixture.supervisor().await;
    for (caller, ancestors) in [
        (&fixture.caller, vec![]),
        (
            &child,
            vec![ActorAncestorObservation {
                actor_id: fixture.caller.id.clone(),
                session_created_at: fixture.caller.created_at,
                metadata_version: fixture.caller.metadata_version,
            }],
        ),
        (&supervisor, vec![]),
    ] {
        fixture
            .install(
                caller,
                ancestors,
                &[(1, vec![Right::ListMetadata, Right::InspectBoundedProgress])],
                0,
            )
            .await;
        let page = result(
            fixture
                .tool()
                .invoke(json!({"action":"list"}), Fixture::ctx(caller))
                .await
                .unwrap(),
        );
        let is_supervisor = caller.supervisor_incarnation_id().is_some();
        assert_eq!(
            page["sessions"].as_array().unwrap().len(),
            if is_supervisor { 6 } else { 1 }
        );
        assert_eq!(
            page["sessions"][0]["session_id"],
            if is_supervisor {
                &fixture.targets[0].id
            } else {
                &fixture.targets[1].id
            }
        );
        assert_eq!(page["sessions"][0]["link_revision"], 0);
        assert_eq!(page["sessions"][0]["authorization_revision"], 1);
        for private in ["private target", "private model", "workspace"] {
            assert!(!page.to_string().contains(private));
        }
        assert_eq!(page.to_string().contains("zz-ungranted"), is_supervisor);
        let view = result(
            fixture
                .tool()
                .invoke(
                    json!({"action":"inspect", "session_id":fixture.targets[1].id, "max_chars":4}),
                    Fixture::ctx(caller),
                )
                .await
                .unwrap(),
        );
        assert_eq!(
            view["latest_visible_assistant_excerpt"]["content"],
            "结果🎋结"
        );
        assert_eq!(view["latest_visible_assistant_excerpt"]["truncated"], true);
        assert_eq!(view["completion_verified"], false);
        assert_eq!(view["status_is_live"], false);
        assert_eq!(view["input_specific"], false);
        assert!(!view.to_string().contains("reasoning"));
        assert!(!view.to_string().contains("hidden assistant"));
    }
}

#[tokio::test]
async fn host_pagination_binds_query_limit_grant_and_full_original_generation() {
    let fixture = Fixture::new().await;
    fixture
        .install(
            &fixture.caller,
            vec![],
            &[
                (0, vec![Right::ListMetadata]),
                (1, vec![Right::ListMetadata]),
                (2, vec![Right::ListMetadata]),
            ],
            0,
        )
        .await;
    let first = result(
        fixture
            .tool()
            .invoke(
                json!({"action":"list", "limit":1, "query":" TARGET-PROJECT "}),
                Fixture::ctx(&fixture.caller),
            )
            .await
            .unwrap(),
    );
    let cursor = first["next_cursor"].as_str().unwrap();
    assert!(cursor.len() <= 4096);
    let second = result(
        fixture
            .tool()
            .invoke(
                json!({"action":"list", "limit":1, "query":"target-project", "cursor":cursor}),
                Fixture::ctx(&fixture.caller),
            )
            .await
            .unwrap(),
    );
    assert_eq!(first["sessions"][0]["session_id"], "aa-one");
    assert_eq!(second["sessions"][0]["session_id"], "bb-two");
    for request in [
        json!({"action":"list","limit":2,"query":"target-project","cursor":cursor}),
        json!({"action":"list","limit":1,"query":"other","cursor":cursor}),
        json!({"action":"list","limit":21}),
        json!({"action":"list","cursor":"malformed"}),
        json!({"action":"list","kind":"root"}),
    ] {
        assert!(fixture
            .tool()
            .invoke(request, Fixture::ctx(&fixture.caller))
            .await
            .is_err());
    }
    let direct = HostSessionDiscovery::new(fixture.store.clone());
    let mut stale_ctx = Fixture::ctx(&fixture.caller);
    let mut stale_caller = SessionCallerReference::from(&fixture.caller);
    // A changed original operation observation only, never a real reset write.
    stale_caller.conversation_reset = Some(Default::default());
    stale_ctx.executing_caller = Some(Arc::new(stale_caller));
    let request = AuthorizedSessionListRequest {
        query: Some("target-project".into()),
        limit: Some(1),
        cursor: Some(cursor.into()),
    };
    assert!(direct
        .list(&stale_ctx, request)
        .await
        .unwrap_err()
        .to_string()
        .contains("cursor_stale"));
    assert!(
        direct
            .list(&stale_ctx, AuthorizedSessionListRequest::default())
            .await
            .is_err(),
        "final read must compare full caller without relying on a cursor digest"
    );
    fixture
        .install(
            &fixture.caller,
            vec![],
            &[
                (0, vec![Right::ListMetadata]),
                (1, vec![Right::ListMetadata]),
            ],
            1,
        )
        .await;
    assert!(fixture
        .tool()
        .invoke(
            json!({"action":"list","limit":1,"query":"target-project","cursor":cursor}),
            Fixture::ctx(&fixture.caller)
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("cursor_stale"));
}

#[tokio::test]
async fn metadata_and_bounded_progress_rights_remain_distinct_read_only_and_non_mutating() {
    let fixture = Fixture::new().await;
    fixture
        .install(
            &fixture.caller,
            vec![],
            &[
                (0, vec![Right::ListMetadata]),
                (1, vec![Right::InspectBoundedProgress]),
            ],
            0,
        )
        .await;
    let before = serde_json::to_value(
        fixture
            .store
            .load_session(&fixture.targets[1].id)
            .await
            .unwrap(),
    )
    .unwrap();
    let mut ctx = Fixture::ctx(&fixture.caller);
    ctx.plan_read_only = true;
    assert_eq!(
        fixture.tool().classify(&json!({"action":"inspect"})),
        ToolClass::READONLY_PARALLEL
    );
    let page = result(
        fixture
            .tool()
            .invoke(json!({"action":"list"}), ctx)
            .await
            .unwrap(),
    );
    assert_eq!(page["sessions"].as_array().unwrap().len(), 1);
    assert_eq!(page["sessions"][0]["session_id"], fixture.targets[0].id);
    assert!(fixture
        .tool()
        .invoke(
            json!({"action":"inspect","session_id":fixture.targets[0].id}),
            Fixture::ctx(&fixture.caller)
        )
        .await
        .is_err());
    let view = result(
        fixture
            .tool()
            .invoke(
                json!({"action":"inspect","session_id":fixture.targets[1].id,"max_chars":4}),
                Fixture::ctx(&fixture.caller),
            )
            .await
            .unwrap(),
    );
    assert_eq!(view["title"].as_str().unwrap().chars().count(), 256);
    assert_eq!(
        view["latest_visible_assistant_excerpt"]["content"],
        "结果🎋结"
    );
    assert_eq!(
        before,
        serde_json::to_value(
            fixture
                .store
                .load_session(&fixture.targets[1].id)
                .await
                .unwrap()
        )
        .unwrap()
    );
    for action in [
        "read_messages",
        "get_meta",
        "read_compressed_cache",
        "export_context",
    ] {
        assert!(fixture
            .tool()
            .invoke(
                json!({"action":action,"session_id":fixture.targets[1].id}),
                Fixture::ctx(&fixture.caller)
            )
            .await
            .is_err());
    }
}

#[tokio::test]
async fn no_scope_or_known_id_never_exposes_global_or_peer_history_but_self_reads_remain() {
    let fixture = Fixture::new().await;
    assert!(fixture
        .tool()
        .invoke(json!({"action":"list"}), Fixture::ctx(&fixture.caller))
        .await
        .unwrap_err()
        .to_string()
        .contains("scope_required"));
    for request in [
        json!({"action":"get_meta","session_id":fixture.targets[0].id}),
        json!({"action":"read_messages","session_id":fixture.targets[0].id}),
        json!({"action":"read_compressed_cache","session_id":fixture.targets[0].id}),
        json!({"action":"search","query":"private"}),
        json!({"action":"export_context","session_id":fixture.targets[0].id}),
        json!({"action":"list","grant":{"rights":["list_metadata"]}}),
    ] {
        assert!(fixture
            .tool()
            .invoke(request, Fixture::ctx(&fixture.caller))
            .await
            .is_err());
    }
    let own = result(
        fixture
            .tool()
            .invoke(
                json!({"action":"search_current","query":"needle"}),
                Fixture::ctx(&fixture.caller),
            )
            .await
            .unwrap(),
    );
    assert!(own.to_string().contains("own safe needle"));
    assert!(!own.to_string().contains("private own system"));
    let current = SessionInspectorTool::current(fixture.store.clone(), fixture.store.clone());
    let mut old_generation = Fixture::ctx(&fixture.caller);
    let mut stale = SessionCallerReference::from(&fixture.caller);
    stale.conversation_reset = Some(Default::default());
    old_generation.executing_caller = Some(Arc::new(stale));
    assert!(current
        .invoke(
            json!({"action":"search_current","query":"needle"}),
            old_generation
        )
        .await
        .is_err());
    let mut missing = Fixture::ctx(&fixture.caller);
    missing.executing_caller = None;
    assert!(fixture
        .tool()
        .invoke(
            json!({"action":"get_meta","session_id":fixture.caller.id}),
            missing
        )
        .await
        .is_err());
    assert!(fixture
        .store
        .find_host_session_scope(&SessionCallerReference::from(&fixture.caller))
        .await
        .unwrap()
        .is_none());
    let normalized =
        bamboo_llm::providers::common::tool_schema::sanitize_openai_function_parameters_schema(
            &fixture.tool().parameters_schema(),
        );
    assert!(normalized["properties"]["cursor"].is_object());
    assert!(!normalized["properties"]["action"]["enum"]
        .as_array()
        .unwrap()
        .contains(&json!("search")));
}

#[tokio::test]
async fn canonical_supervisor_default_history_ignores_legacy_host_acl_and_revocation() {
    let fixture = Fixture::new().await;
    let supervisor = fixture.supervisor().await;
    let legacy = result(
        fixture
            .tool()
            .invoke(json!({"action":"list"}), Fixture::ctx(&supervisor))
            .await
            .unwrap(),
    );
    assert_eq!(legacy["sessions"][0]["session_id"], fixture.targets[0].id);
    for action in ["get_meta", "read_messages"] {
        assert!(fixture
            .tool()
            .invoke(
                json!({"action":action,"session_id":fixture.targets[0].id}),
                Fixture::ctx(&supervisor)
            )
            .await
            .is_ok());
    }
    fixture
        .install(&supervisor, vec![], &[(1, vec![Right::ListMetadata])], 0)
        .await;
    let page = result(
        fixture
            .tool()
            .invoke(json!({"action":"list"}), Fixture::ctx(&supervisor))
            .await
            .unwrap(),
    );
    assert_eq!(page["sessions"][0]["session_id"], fixture.targets[0].id);
    assert_eq!(page["sessions"].as_array().unwrap().len(), 5);
    for request in [
        json!({"action":"get_meta","session_id":fixture.targets[0].id}),
        json!({"action":"read_messages","session_id":fixture.targets[0].id}),
        json!({"action":"inspect","session_id":fixture.targets[0].id}),
        json!({"action":"inspect","session_id":fixture.targets[1].id}),
    ] {
        assert!(fixture
            .tool()
            .invoke(request, Fixture::ctx(&supervisor))
            .await
            .is_ok());
    }
    fixture
        .store
        .revoke_host_session_scope(&supervisor.id, 1)
        .await
        .unwrap();
    for request in [
        json!({"action":"list"}),
        json!({"action":"get_meta","session_id":fixture.targets[0].id}),
    ] {
        assert!(fixture
            .tool()
            .invoke(request, Fixture::ctx(&supervisor))
            .await
            .is_ok());
    }
    let mut current = fixture
        .store
        .load_session(&supervisor.id)
        .await
        .unwrap()
        .unwrap();
    for ultra in [true, false, true] {
        current.set_root_orchestration_only(ultra).unwrap();
        fixture.store.save_session(&current).await.unwrap();
        let page = result(
            fixture
                .tool()
                .invoke(json!({"action":"list"}), Fixture::ctx(&current))
                .await
                .unwrap(),
        );
        assert_eq!(page["sessions"].as_array().unwrap().len(), 5);
    }
    assert!(
        fixture
            .tool()
            .invoke(json!({"action":"list"}), Fixture::ctx(&supervisor))
            .await
            .is_err(),
        "a fresh role observation does not authorize an old executing generation"
    );
}

struct UnavailableHost {
    store: Arc<SessionStoreV2>,
    installed_but_denied: bool,
}

struct PauseLegacyRead {
    store: Arc<SessionStoreV2>,
    inspect: bool,
    paused: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

#[async_trait]
impl Storage for PauseLegacyRead {
    async fn save_session(&self, _: &Session) -> io::Result<()> {
        panic!("discovery must not write")
    }
    async fn delete_session(&self, _: &str) -> io::Result<bool> {
        panic!("discovery must not delete")
    }
    async fn load_session(&self, _: &str) -> io::Result<Option<Session>> {
        panic!("no plain target/history fallback")
    }
    async fn load_root_authority(&self, id: &str) -> io::Result<Option<Session>> {
        self.store.load_root_authority(id).await
    }
    async fn load_execution_admission_snapshot(&self, id: &str) -> io::Result<Option<Session>> {
        self.store.load_execution_admission_snapshot(id).await
    }
    async fn inspect_supervisor_current_user_scope(
        &self,
        supervisor: &SupervisorReference,
    ) -> io::Result<bamboo_domain::SupervisorCurrentUserScope> {
        self.store
            .inspect_supervisor_current_user_scope(supervisor)
            .await
    }
    async fn list_supervisor_current_user_targets(
        &self,
        supervisor: &SupervisorReference,
        query: Option<&str>,
        after: Option<&str>,
        limit: usize,
    ) -> io::Result<bamboo_domain::SupervisorCurrentUserPage> {
        let page = self
            .store
            .list_supervisor_current_user_targets(supervisor, query, after, limit)
            .await?;
        if !self.inspect {
            self.paused.notify_one();
            self.resume.notified().await;
        }
        Ok(page)
    }
    async fn load_supervisor_current_user_observed_session(
        &self,
        supervisor: &SupervisorReference,
        target: &str,
    ) -> io::Result<(Session, bamboo_domain::SupervisorCurrentUserTarget)> {
        let observed = self
            .store
            .load_supervisor_current_user_observed_session(supervisor, target)
            .await?;
        if self.inspect {
            self.paused.notify_one();
            self.resume.notified().await;
        }
        Ok(observed)
    }
    async fn find_host_session_scope(
        &self,
        caller: &SessionCallerReference,
    ) -> io::Result<Option<HostSessionScopeToken>> {
        self.store.find_host_session_scope(caller).await
    }
    async fn inspect_supervisor_link(
        &self,
        supervisor: &SupervisorReference,
        target: &str,
    ) -> io::Result<SupervisorLinkObservation> {
        let observation = self
            .store
            .inspect_supervisor_link(supervisor, target)
            .await?;
        if !self.inspect {
            self.paused.notify_one();
            self.resume.notified().await;
        }
        Ok(observation)
    }
    async fn load_supervisor_managed_session(
        &self,
        supervisor: &SupervisorReference,
        target: &str,
    ) -> io::Result<Option<Session>> {
        let session = self
            .store
            .load_supervisor_managed_session(supervisor, target)
            .await?;
        if self.inspect {
            self.paused.notify_one();
            self.resume.notified().await;
        }
        Ok(session)
    }
}

#[tokio::test]
async fn current_user_reads_keep_original_caller_fences_without_legacy_host_acl() {
    for inspect in [false, true] {
        for change in ["install", "revoke", "caller"] {
            let fixture = Fixture::new().await;
            let supervisor = fixture.supervisor().await;
            let storage = Arc::new(PauseLegacyRead {
                store: fixture.store.clone(),
                inspect,
                paused: tokio::sync::Notify::new(),
                resume: tokio::sync::Notify::new(),
            });
            let tool = SessionInspectorTool::host_routed(fixture.store.clone(), storage.clone());
            let request = if inspect {
                json!({"action":"inspect","session_id":fixture.targets[0].id})
            } else {
                json!({"action":"list"})
            };
            let ctx = Fixture::ctx(&supervisor);
            let read = tokio::spawn(async move { tool.invoke(request, ctx).await });
            tokio::time::timeout(std::time::Duration::from_secs(5), storage.paused.notified())
                .await
                .expect("current-user reader reaches its synthetic pause");
            // Old finite Host ACL publication cannot restrict the canonical
            // Supervisor role. A changed original execution still must fail.
            if change == "caller" {
                let mut changed = supervisor.clone();
                changed.workspace = Some(
                    fixture
                        .home
                        .path()
                        .join("new-workspace")
                        .to_string_lossy()
                        .into_owned(),
                );
                fixture.store.save_session(&changed).await.unwrap();
            } else {
                fixture
                    .install(&supervisor, vec![], &[(1, vec![Right::ListMetadata])], 0)
                    .await;
            }
            if change == "revoke" {
                fixture
                    .store
                    .revoke_host_session_scope(&supervisor.id, 1)
                    .await
                    .unwrap();
            }
            storage.resume.notify_one();
            let observed = tokio::time::timeout(std::time::Duration::from_secs(5), read)
                .await
                .expect("current-user reader completes after release")
                .unwrap();
            if change == "caller" {
                assert!(
                    observed.is_err(),
                    "changed original caller must stay denied"
                );
            } else {
                let observed = result(observed.unwrap());
                if inspect {
                    assert_eq!(observed["session_id"], fixture.targets[0].id);
                } else {
                    assert_eq!(observed["sessions"].as_array().unwrap().len(), 5);
                }
            }
        }
    }
}

#[async_trait]
impl Storage for UnavailableHost {
    async fn save_session(&self, _: &Session) -> io::Result<()> {
        panic!("discovery must not write")
    }
    async fn delete_session(&self, _: &str) -> io::Result<bool> {
        panic!("discovery must not delete")
    }
    async fn load_session(&self, _: &str) -> io::Result<Option<Session>> {
        panic!("no plain target/history fallback")
    }
    async fn load_root_authority(&self, _: &str) -> io::Result<Option<Session>> {
        panic!("no Supervisor fallback after Host error")
    }
    async fn load_execution_admission_snapshot(&self, id: &str) -> io::Result<Option<Session>> {
        self.store.load_execution_admission_snapshot(id).await
    }
    async fn find_host_session_scope(
        &self,
        caller: &SessionCallerReference,
    ) -> io::Result<Option<HostSessionScopeToken>> {
        if self.installed_but_denied {
            self.store.find_host_session_scope(caller).await
        } else {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "synthetic unsupported Host",
            ))
        }
    }
    async fn list_host_scoped_targets(
        &self,
        _: &HostSessionScopeToken,
        _: &SessionCallerReference,
    ) -> io::Result<Vec<HostSessionTargetScope>> {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "synthetic final Host deny",
        ))
    }
    async fn load_host_scoped_target(
        &self,
        _: &HostSessionScopeToken,
        _: &SessionCallerReference,
        _: &str,
    ) -> io::Result<Session> {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "synthetic final Host deny",
        ))
    }
}

#[tokio::test]
async fn unsupported_denied_and_fresh_store_issuer_never_become_a_legacy_fallback() {
    let fixture = Fixture::new().await;
    let supervisor = fixture.supervisor().await;
    fixture
        .install(
            &supervisor,
            vec![],
            &[(1, vec![Right::ListMetadata, Right::InspectBoundedProgress])],
            0,
        )
        .await;
    for installed_but_denied in [false, true] {
        let storage = Arc::new(UnavailableHost {
            store: fixture.store.clone(),
            installed_but_denied,
        });
        let tool = SessionInspectorTool::host_routed(fixture.store.clone(), storage);
        for request in [
            json!({"action":"list"}),
            json!({"action":"inspect","session_id":fixture.targets[1].id}),
            json!({"action":"get_meta","session_id":fixture.targets[0].id}),
        ] {
            assert!(tool
                .invoke(request, Fixture::ctx(&supervisor))
                .await
                .is_err());
        }
    }
    fixture
        .install(
            &fixture.caller,
            vec![],
            &[(0, vec![Right::ListMetadata])],
            0,
        )
        .await;
    let reopened = Arc::new(
        SessionStoreV2::new(fixture.home.path().into())
            .await
            .unwrap(),
    );
    assert!(reopened
        .find_host_session_scope(&SessionCallerReference::from(&fixture.caller))
        .await
        .unwrap()
        .is_none());
    let tool = SessionInspectorTool::host_routed(reopened.clone(), reopened);
    assert!(
        tool.invoke(json!({"action":"list"}), Fixture::ctx(&fixture.caller))
            .await
            .is_err(),
        "new store issuer requires an explicit Host reinstallation"
    );
}
