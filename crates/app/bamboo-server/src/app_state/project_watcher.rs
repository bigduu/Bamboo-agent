//! Coalesced Project shared-resource invalidation watcher.

use std::collections::BTreeSet;
use std::path::{Component, Path};
use std::sync::Arc;
use std::time::Duration;

use bamboo_agent_core::AgentEvent;
use bamboo_domain::ProjectId;
use bamboo_engine::events::AccountEventSink;
use bamboo_projects::{ProjectStore, ProjectStoreError};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;

pub struct ProjectResourceWatcher {
    _watcher: RecommendedWatcher,
    task: tokio::task::JoinHandle<()>,
}

impl ProjectResourceWatcher {
    pub fn start(
        store: Arc<ProjectStore>,
        account_sink: Arc<AccountEventSink>,
        debounce: Duration,
    ) -> Result<Self, String> {
        let projects_dir = store.paths().projects_dir();
        std::fs::create_dir_all(&projects_dir).map_err(|error| error.to_string())?;
        let (tx, mut rx) = mpsc::unbounded_channel::<notify::Result<notify::Event>>();
        let mut watcher = notify::recommended_watcher(move |event| {
            let _ = tx.send(event);
        })
        .map_err(|error| error.to_string())?;
        watcher
            .watch(&projects_dir, RecursiveMode::Recursive)
            .map_err(|error| error.to_string())?;

        let task = tokio::spawn(async move {
            while let Some(first) = rx.recv().await {
                let mut projects = BTreeSet::new();
                collect_projects(&projects_dir, first, &mut projects);
                let delay = tokio::time::sleep(debounce);
                tokio::pin!(delay);
                loop {
                    tokio::select! {
                        _ = &mut delay => break,
                        next = rx.recv() => match next {
                            Some(event) => collect_projects(&projects_dir, event, &mut projects),
                            None => return,
                        }
                    }
                }
                for project_id in projects {
                    bump_with_retry(&store, &account_sink, &project_id);
                }
            }
        });
        Ok(Self {
            _watcher: watcher,
            task,
        })
    }
}

impl Drop for ProjectResourceWatcher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn collect_projects(
    projects_dir: &Path,
    event: notify::Result<notify::Event>,
    projects: &mut BTreeSet<ProjectId>,
) {
    let Ok(event) = event else {
        tracing::warn!("Project resource watcher received an OS watch error");
        return;
    };
    for path in event.paths {
        if let Some(project_id) = project_for_resource_path(projects_dir, &path) {
            projects.insert(project_id);
        }
    }
}

fn project_for_resource_path(projects_dir: &Path, path: &Path) -> Option<ProjectId> {
    let relative = path.strip_prefix(projects_dir).ok()?;
    let mut components = relative.components();
    let Component::Normal(project_component) = components.next()? else {
        return None;
    };
    let project_id = project_component.to_str()?.parse::<ProjectId>().ok()?;
    let Component::Normal(resource_component) = components.next()? else {
        return None;
    };
    let resource = resource_component.to_str()?;

    // Watch only shared resource namespaces. Manifest/index/state/locks and
    // atomic temporary files are excluded, preventing our own CAS writes from
    // feeding back into another bump.
    let watched = resource == "settings.json"
        || resource == "skills"
        || resource.starts_with("skills-")
        || matches!(resource, "commands" | "memory" | "artifacts");
    if !watched
        || path.components().any(|component| {
            let value = component.as_os_str().to_string_lossy();
            value == "state"
                || value.ends_with(".lock")
                || value.ends_with(".tmp")
                || value.starts_with(".tmp-")
        })
    {
        return None;
    }
    Some(project_id)
}

fn bump_with_retry(store: &ProjectStore, account_sink: &AccountEventSink, project_id: &ProjectId) {
    for _ in 0..8 {
        let current = match store.get(project_id) {
            Ok(project) => project,
            Err(ProjectStoreError::NotFound(_)) => return,
            Err(error) => {
                tracing::warn!(%project_id, %error, "Project resource watcher read failed");
                return;
            }
        };
        match store.bump_resource_revision(project_id, current.revision) {
            Ok(updated) => {
                account_sink.record(
                    None,
                    &AgentEvent::ProjectUpdated {
                        project_id: updated.id.to_string(),
                        revision: updated.revision,
                    },
                );
                return;
            }
            Err(ProjectStoreError::Conflict { .. }) => continue,
            Err(error) => {
                tracing::warn!(%project_id, %error, "Project resource revision bump failed");
                return;
            }
        }
    }
    tracing::warn!(%project_id, "Project resource revision bump exhausted CAS retries");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifier_accepts_resources_and_rejects_self_writes() {
        let root = std::path::PathBuf::from("/data/projects");
        assert_eq!(
            project_for_resource_path(&root, Path::new("/data/projects/project-1/commands/a.md"))
                .map(|id| id.to_string()),
            Some("project-1".to_string())
        );
        assert!(project_for_resource_path(
            &root,
            Path::new("/data/projects/project-1/project.json")
        )
        .is_none());
        assert!(project_for_resource_path(
            &root,
            Path::new("/data/projects/project-1/state/migration.json")
        )
        .is_none());
        assert!(
            project_for_resource_path(&root, Path::new("/data/projects/.registry.lock")).is_none()
        );
    }

    #[tokio::test]
    async fn external_change_bumps_once_without_self_write_loop() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(ProjectStore::open(dir.path()).unwrap());
        let project = store.create("Project", None).unwrap();
        let sink = AccountEventSink::new(dir.path().join("events")).unwrap();
        let _watcher =
            ProjectResourceWatcher::start(store.clone(), sink, Duration::from_millis(40)).unwrap();
        let commands = store.paths().project_home(&project.id).join("commands");
        std::fs::create_dir_all(&commands).unwrap();
        std::fs::write(commands.join("review.md"), "one").unwrap();

        let mut bumped = None;
        for _ in 0..40 {
            let current = store.get(&project.id).unwrap();
            if current.resource_revision > project.resource_revision {
                bumped = Some(current);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let bumped = bumped.expect("external resource change should bump revision");
        tokio::time::sleep(Duration::from_millis(180)).await;
        let stable = store.get(&project.id).unwrap();
        assert_eq!(stable.resource_revision, bumped.resource_revision);
        assert_eq!(stable.revision, bumped.revision);
    }

    #[tokio::test]
    async fn separated_concurrent_change_batches_are_not_lost() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(ProjectStore::open(dir.path()).unwrap());
        let project = store.create("Project", None).unwrap();
        let sink = AccountEventSink::new(dir.path().join("events")).unwrap();
        let _watcher =
            ProjectResourceWatcher::start(store.clone(), sink, Duration::from_millis(30)).unwrap();
        let commands = store.paths().project_home(&project.id).join("commands");
        std::fs::create_dir_all(&commands).unwrap();
        std::fs::write(commands.join("one.md"), "one").unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        std::fs::write(commands.join("two.md"), "two").unwrap();

        for _ in 0..50 {
            let current = store.get(&project.id).unwrap();
            if current.resource_revision >= project.resource_revision + 2 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("two external change batches should produce two revision bumps");
    }
}
