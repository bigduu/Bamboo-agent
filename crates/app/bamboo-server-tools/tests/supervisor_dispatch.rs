//! Real Core -> Root overlay -> owned load_skill -> concrete nested Tool path.

use async_trait::async_trait;
use bamboo_agent_core::tools::{
    ExecutingSupervisorObservation, FunctionCall, Tool, ToolCall, ToolClass, ToolCtx, ToolError,
    ToolExecutionContext, ToolExecutionSessionFlags, ToolExecutor, ToolOutcome, ToolResult,
};
use bamboo_domain::{Session, Storage, DEFAULT_SUPERVISOR_SESSION_ID};
use bamboo_engine::session_app::supervisor::SupervisorSessionService;
use bamboo_engine::{SessionCache, SessionRepository};
use bamboo_server_tools::{LoadSkillTool, OverlayToolExecutor};
use bamboo_skills::{SkillManager, SkillStoreConfig};
use bamboo_storage::{LockedSessionStore, SessionStoreV2};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, RwLock, Semaphore};

#[derive(Debug)]
struct Seen {
    observation: Option<ExecutingSupervisorObservation>,
    allowed: bool,
    bypass: bool,
    auto: bool,
    plan: bool,
    args: serde_json::Value,
}

struct ReadProbe {
    service: SupervisorSessionService,
    seen: Arc<Mutex<Vec<Seen>>>,
}

#[async_trait]
impl Tool for ReadProbe {
    fn name(&self) -> &str {
        "Read"
    }
    fn description(&self) -> &str {
        "Observe nested context"
    }
    fn parameters_schema(&self) -> serde_json::Value {
        serde_json::json!({"type":"object"})
    }
    fn classify(&self, _: &serde_json::Value) -> ToolClass {
        ToolClass::READONLY_PARALLEL
    }
    async fn invoke(
        &self,
        args: serde_json::Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let allowed = match ctx.executing_supervisor {
            Some(observation) => self
                .service
                .inspect_scope(&observation.supervisor_reference())
                .await
                .is_ok(),
            None => false,
        };
        self.seen.lock().unwrap().push(Seen {
            observation: ctx.executing_supervisor,
            allowed,
            bypass: ctx.bypass_permissions,
            auto: ctx.auto_approve_permissions,
            plan: ctx.plan_read_only,
            args,
        });
        Ok(ToolOutcome::Completed(ToolResult {
            success: true,
            result: "provider observation recorded".into(),
            display_preference: None,
            images: vec![],
        }))
    }
}

struct PausedLoadSkill {
    inner: LoadSkillTool,
    entered: mpsc::UnboundedSender<Option<ExecutingSupervisorObservation>>,
    resume: Arc<Semaphore>,
}

#[async_trait]
impl Tool for PausedLoadSkill {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn description(&self) -> &str {
        self.inner.description()
    }
    fn parameters_schema(&self) -> serde_json::Value {
        self.inner.parameters_schema()
    }
    fn classify(&self, args: &serde_json::Value) -> ToolClass {
        self.inner.classify(args)
    }
    async fn invoke(
        &self,
        args: serde_json::Value,
        ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let retained = ctx.clone();
        self.entered.send(retained.executing_supervisor).unwrap();
        self.resume.acquire().await.unwrap().forget();
        // The actual load_skill reads repository configuration only after the
        // test replaces the Supervisor. The original owned context survives.
        self.inner.invoke(args, retained).await
    }
}

fn load_call(id: &str) -> ToolCall {
    ToolCall {
        id: id.into(),
        tool_type: "function".into(),
        function: FunctionCall {
            name: "load_skill".into(),
            arguments: serde_json::json!({"skill_id":"dispatch-probe"}).to_string(),
        },
    }
}

fn flags() -> ToolExecutionSessionFlags {
    ToolExecutionSessionFlags {
        bypass_permissions: true,
        auto_approve_permissions: true,
        plan_read_only: false,
    }
}

fn configure(session: &mut Session, workspace: &std::path::Path) {
    session.set_workspace_path_meta(workspace.to_string_lossy());
    session.metadata.insert(
        bamboo_skills::runtime_metadata::SKILL_RUNTIME_SELECTED_SKILL_IDS_KEY.into(),
        r#"["dispatch-probe"]"#.into(),
    );
}

async fn core_dispatch(session: &mut Session, tools: &dyn ToolExecutor, id: &str) {
    let (tx, _rx) = mpsc::channel(128);
    bamboo_agent_core::tools::result_handler::execute_sub_actions(
        &[load_call(id)],
        &tx,
        session,
        tools,
        flags(),
        None,
    )
    .await;
}

#[tokio::test]
async fn supervisor_overlay_and_nested_load_skill_keep_original_identity_after_configuration_reload(
) {
    let home = tempfile::tempdir().unwrap();
    let old_workspace = home.path().join("old-workspace");
    let new_workspace = home.path().join("new-workspace");
    for workspace in [&old_workspace, &new_workspace] {
        std::fs::create_dir_all(workspace).unwrap();
        std::fs::write(workspace.join("input.txt"), "workspace input").unwrap();
    }
    let skill_dir = home.path().join("skills/dispatch-probe");
    std::fs::create_dir_all(&skill_dir).unwrap();
    std::fs::write(
        skill_dir.join("SKILL.md"),
        r#"---
name: dispatch-probe
description: Observe executing Supervisor transport
metadata:
  dynamic_context:
    - id: observe
      tool: Read
      input:
        path: input.txt
      timeout_ms: 5000
---
Use the declared context.
"#,
    )
    .unwrap();
    let manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
        skills_dir: home.path().join("skills"),
        ..Default::default()
    }));
    manager.initialize().await.unwrap();
    let store: Arc<dyn Storage> =
        Arc::new(SessionStoreV2::new(home.path().join("data")).await.unwrap());
    let service = SupervisorSessionService::new(store.clone());
    service.get_or_create_default("initial").await.unwrap();
    let mut original_session = store
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    configure(&mut original_session, &old_workspace);
    store.save_session(&original_session).await.unwrap();
    let original =
        ExecutingSupervisorObservation::capture_from_executing_session(&original_session);
    let repo = SessionRepository::new(
        SessionCache::default(),
        store.clone(),
        Arc::new(LockedSessionStore::new(store.clone())),
    );
    let seen = Arc::new(Mutex::new(Vec::new()));
    let permission_config = Arc::new(bamboo_tools::permission::PermissionConfig::new());
    let base: Arc<dyn ToolExecutor> = Arc::new(
        bamboo_tools::BuiltinToolExecutorBuilder::new()
            .with_tool(ReadProbe {
                service: service.clone(),
                seen: seen.clone(),
            })
            .unwrap()
            .with_permission_checker(Arc::new(
                bamboo_tools::permission::ConfigPermissionChecker::new(permission_config.clone()),
            ))
            .build(),
    );
    let inner = LoadSkillTool::new(
        manager,
        Arc::new(RwLock::new(bamboo_llm::Config::default())),
        repo.clone(),
    )
    .with_permission_checked_context_registry(base.clone(), Some(permission_config));
    let (entered, mut entered_rx) = mpsc::unbounded_channel();
    let resume = Arc::new(Semaphore::new(0));
    let paused = Arc::new(PausedLoadSkill {
        inner,
        entered,
        resume: resume.clone(),
    });
    let overlay = OverlayToolExecutor::new(base, paused.clone());
    let execute = core_dispatch(&mut original_session, &overlay, "old-load");
    let replace = async {
        assert_eq!(entered_rx.recv().await.unwrap(), original);
        assert!(store
            .delete_session(DEFAULT_SUPERVISOR_SESSION_ID)
            .await
            .unwrap());
        service.get_or_create_default("replacement").await.unwrap();
        let mut fresh = store
            .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
            .await
            .unwrap()
            .unwrap();
        configure(&mut fresh, &new_workspace);
        repo.save(&mut fresh).await.unwrap();
        let replacement = ExecutingSupervisorObservation::capture_from_executing_session(&fresh);
        assert_ne!(replacement, original);
        resume.add_permits(1);
        fresh
    };
    let (_, mut fresh) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(execute, replace)
    })
    .await
    .expect("actual overlay and nested reload finish");
    {
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1, "actual nested Tool::invoke must be reached");
        assert_eq!(seen[0].observation, original);
        assert!(
            !seen[0].allowed,
            "canonical service rejects the deleted lifetime"
        );
        assert!(
            seen[0].args.to_string().contains("new-workspace"),
            "configuration was actually reloaded from the replacement Session"
        );
        assert!(!seen[0].bypass);
        assert!(seen[0].auto && !seen[0].plan);
    }
    resume.add_permits(1);
    core_dispatch(&mut fresh, &overlay, "fresh-load").await;
    {
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(
            seen[1].observation,
            ExecutingSupervisorObservation::capture_from_executing_session(&fresh)
        );
        assert!(
            seen[1].allowed,
            "fresh real dispatch passes canonical revalidation"
        );
    }

    let mut ordinary = Session::new("ordinary-nested", "model");
    configure(&mut ordinary, &new_workspace);
    repo.save(&mut ordinary).await.unwrap();
    let (tx, _rx) = mpsc::channel(128);
    // A generic same-ID context and a forged mismatched caller must remain
    // empty even though load_skill can read the current Supervisor configuration.
    for (id, observation) in [(fresh.id.as_str(), None), (ordinary.id.as_str(), original)] {
        let call = load_call("synthetic-load");
        let mut context = ToolExecutionContext::for_dispatch(
            id,
            id,
            &call.id,
            &tx,
            &[],
            flags(),
            false,
            None,
            None,
        );
        context.executing_supervisor = observation;
        resume.add_permits(1);
        let result = overlay.execute_with_context(&call, context).await.unwrap();
        assert!(
            result.success,
            "synthetic dispatch still loads its allowed workflow"
        );
    }
    {
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 4);
        for item in &seen[2..] {
            assert_eq!(item.observation, None);
            assert!(!item.allowed && !item.bypass && item.auto && !item.plan);
        }
    }

    // Existing public dispatch classifies load_skill itself as mutating. Test
    // its already-owned nested Plan context at Tool::invoke, as direct callers
    // do, without changing that independent outer authorization policy.
    let call = load_call("owned-plan-load");
    let current = ExecutingSupervisorObservation::capture_from_executing_session(&fresh);
    let mut context = ToolExecutionContext::for_dispatch(
        &fresh.id,
        &fresh.root_session_id,
        &call.id,
        &tx,
        &[],
        flags(),
        false,
        None,
        None,
    )
    .with_executing_supervisor(current)
    .to_tool_ctx();
    context.plan_read_only = true;
    resume.add_permits(1);
    paused
        .invoke(serde_json::json!({"skill_id":"dispatch-probe"}), context)
        .await
        .unwrap();
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 5);
    assert_eq!(seen[4].observation, current);
    assert!(seen[4].allowed && seen[4].auto && seen[4].plan && !seen[4].bypass);
}
