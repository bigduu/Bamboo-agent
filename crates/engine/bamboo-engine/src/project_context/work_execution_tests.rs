use super::*;
use crate::ticket_worker_plan::WorkerLocalPlan;
use bamboo_tickets::{Contract, ProjectContextSnapshot};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex,
};

struct Plan {
    context: ProjectContextSnapshot,
    workspace: PathBuf,
    live: AtomicBool,
}

#[async_trait]
impl WorkerLocalPlan for Plan {
    fn run_id(&self) -> &str {
        "work-run"
    }
    fn bind_session(&self, _: &mut Session) -> bamboo_tickets::Result<()> {
        Ok(())
    }
    fn workspace_project_context(
        &self,
        _: &Session,
        workspace: Option<&Path>,
    ) -> bamboo_tickets::Result<Option<ProjectContextSnapshot>> {
        if !self.live.load(Ordering::SeqCst) || workspace != Some(self.workspace.as_path()) {
            return Err(bamboo_tickets::Error::ScopeDenied(
                "workspace grant revoked or changed".into(),
            ));
        }
        Ok(Some(self.context.clone()))
    }
    async fn apply_task(
        &self,
        _: &mut Session,
        _: &str,
        _: &serde_json::Value,
    ) -> bamboo_tickets::Result<bamboo_domain::TaskList> {
        panic!("workspace resolution executes no Task")
    }
}

struct Source {
    facts: Mutex<Option<ProjectWorkspaceFacts>>,
    fail: AtomicBool,
    revoke: AtomicBool,
    plan: Arc<Plan>,
}

#[async_trait]
impl ProjectContextSource for Source {
    async fn find_project(
        &self,
        _: &ProjectId,
    ) -> Result<Option<ProjectDescriptor>, ProjectContextError> {
        panic!("a Work workspace grant must not load adopted Project resources")
    }
    async fn find_workspace_owner(
        &self,
        _: &Path,
    ) -> Result<Option<ProjectId>, ProjectContextError> {
        Ok(self
            .facts
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|facts| facts.owner.clone()))
    }
    async fn workspace_facts(
        &self,
        _: &ProjectId,
        _: &Path,
    ) -> Result<Option<ProjectWorkspaceFacts>, ProjectContextError> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(ProjectContextError::Source("registry unavailable".into()));
        }
        let facts = self.facts.lock().unwrap().clone();
        if self.revoke.load(Ordering::SeqCst) {
            self.plan.live.store(false, Ordering::SeqCst);
            tokio::task::yield_now().await;
        }
        Ok(facts)
    }
}

fn fixture(workspace: PathBuf) -> (Arc<Plan>, Arc<Source>, Session) {
    let project_id = ProjectId::parse("adopted-work-project").unwrap();
    let binding = WorkspaceBinding {
        path: workspace.to_string_lossy().into_owned(),
        label: None,
        git_common_dir: None,
    };
    let plan = Arc::new(Plan {
        context: ProjectContextSnapshot {
            project_id: project_id.clone(),
            project_revision: 1,
            resource_revision: 7,
            workspace_binding: binding.clone(),
            contract_revision: 1,
            contract: Contract {
                title: "Work".into(),
                objective: "Read its own workspace".into(),
                constraints: Vec::new(),
                acceptance: vec!["Exact result".into()],
                user_acceptance_required: true,
                allowed_tools: std::collections::BTreeSet::from(["Task".into()]),
            },
            sources: std::collections::BTreeMap::new(),
        },
        workspace: workspace.clone(),
        live: AtomicBool::new(true),
    });
    let source = Arc::new(Source {
        facts: Mutex::new(Some(ProjectWorkspaceFacts {
            project_id: project_id.clone(),
            status: bamboo_domain::ProjectStatus::Active,
            resource_revision: 7,
            project_path: Some(binding.path),
            workspace_bindings: Vec::new(),
            owner: Some(project_id),
        })),
        fail: AtomicBool::new(false),
        revoke: AtomicBool::new(false),
        plan: plan.clone(),
    });
    let mut session = Session::new("workspace-projection", "test");
    session.set_workspace_path_meta(workspace.to_string_lossy());
    (plan, source, session)
}

#[tokio::test]
async fn typed_work_workspace_keeps_session_unassigned_and_grants_no_project_resources() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().canonicalize().unwrap();
    let (plan, source, mut session) = fixture(path.clone());
    session.metadata.insert(
        crate::ticket_worker_plan::TICKET_LOCAL_PLAN_KEY.into(),
        "forged".into(),
    );
    session.metadata.insert(
        "ticket.work_contract_ref.v1".into(),
        serde_json::to_string(&plan.context).unwrap(),
    );
    let ordinary = ProjectContextResolver::new(source);
    assert!(matches!(
        ordinary.resolve(&session, Some(&path)).await,
        Err(ProjectContextError::UnassignedWorkspaceConflict { .. })
    ));
    let scoped = ordinary.for_work_execution(plan);
    assert!(scoped
        .refresh_session_prompt(&mut session)
        .await
        .unwrap()
        .is_none());
    assert!(session.project_id_meta().is_none());
    assert!(scoped
        .resolve_memory_read_scope(&session, Some(&path))
        .await
        .unwrap()
        .is_none());
    assert!(!session.metadata.contains_key(PROJECT_CONTEXT_RENDERED_KEY));
    assert!(!session
        .metadata
        .contains_key(PROJECT_RESOURCES_RENDERED_KEY));
    // Installing one Run's capability must not change the ordinary resolver.
    assert!(ordinary.resolve(&session, Some(&path)).await.is_err());
}

#[tokio::test]
async fn work_workspace_refuses_changed_missing_or_unreadable_registry_facts() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().canonicalize().unwrap();
    for change in 0..8 {
        let (plan, source, session) = fixture(path.clone());
        let resolver = ProjectContextResolver::new(source.clone()).for_work_execution(plan);
        assert!(resolver
            .resolve(&session, Some(&path))
            .await
            .unwrap()
            .is_none());
        match change {
            0 => {
                source.facts.lock().unwrap().as_mut().unwrap().owner =
                    Some(ProjectId::parse("foreign-project").unwrap())
            }
            1 => {
                source.facts.lock().unwrap().as_mut().unwrap().status =
                    bamboo_domain::ProjectStatus::Archived
            }
            2 => {
                source
                    .facts
                    .lock()
                    .unwrap()
                    .as_mut()
                    .unwrap()
                    .resource_revision += 1
            }
            3 => source.facts.lock().unwrap().as_mut().unwrap().project_path = None,
            4 => source
                .facts
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .workspace_bindings
                .push(WorkspaceBinding {
                    path: path.to_string_lossy().into_owned(),
                    label: Some("changed-binding".into()),
                    git_common_dir: None,
                }),
            5 => {
                source.facts.lock().unwrap().as_mut().unwrap().project_id =
                    ProjectId::parse("wrong-project-facts").unwrap()
            }
            6 => *source.facts.lock().unwrap() = None,
            7 => source.fail.store(true, Ordering::SeqCst),
            _ => unreachable!(),
        }
        assert!(
            resolver.resolve(&session, Some(&path)).await.is_err(),
            "registry change {change} must not fall back to Unassigned"
        );
    }
}

#[tokio::test]
async fn revoked_work_capability_is_rechecked_after_async_registry_read() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().canonicalize().unwrap();
    let (plan, source, session) = fixture(path.clone());
    let resolver = ProjectContextResolver::new(source.clone()).for_work_execution(plan);
    source.revoke.store(true, Ordering::SeqCst);
    assert!(resolver.resolve(&session, Some(&path)).await.is_err());
    *source.facts.lock().unwrap() = None;
    assert!(resolver.resolve(&session, Some(&path)).await.is_err());
}

#[tokio::test]
async fn work_workspace_refuses_descendants_and_other_execution_directories() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().canonicalize().unwrap();
    let descendant = path.join("descendant");
    std::fs::create_dir(&descendant).unwrap();
    let other = tempfile::tempdir().unwrap();
    let (plan, source, session) = fixture(path);
    let resolver = ProjectContextResolver::new(source).for_work_execution(plan);
    assert!(resolver.resolve(&session, Some(&descendant)).await.is_err());
    assert!(resolver
        .resolve(&session, Some(&other.path().canonicalize().unwrap()))
        .await
        .is_err());
}
