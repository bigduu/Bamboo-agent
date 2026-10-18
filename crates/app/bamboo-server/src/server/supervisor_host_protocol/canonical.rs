//! Native adapter over the existing canonical management service. Inspection
//! never bootstraps/promotes a Supervisor and apply retains the storage CAS.
use super::{state::Submission, wire::Outcome};
use crate::app_state::AppState;
use actix_web::web;
use async_trait::async_trait;
use bamboo_domain::{
    ProjectId, ProjectStatus, SupervisorManagementMutation, SupervisorManagementRequest,
    DEFAULT_SUPERVISOR_SESSION_ID, MAX_SUPERVISOR_PROJECTS,
};
use bamboo_engine::session_app::supervisor::SupervisorSessionService;
use bamboo_projects::ProjectStoreError;
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ScopeContext {
    pub(super) data_directory: String,
    pub(super) previous_projects: BTreeSet<ProjectId>,
    pub(super) selected_project_names: BTreeMap<ProjectId, String>,
}
#[derive(Clone, Debug)]
pub(super) struct Refused {
    pub(super) code: &'static str,
    pub(super) status_code: u16,
}
impl Refused {
    pub(super) fn new(code: &'static str, status_code: u16) -> Self {
        Self { code, status_code }
    }
    pub(super) fn outcome(self) -> Outcome {
        Outcome::Refused {
            code: self.code,
            status_code: self.status_code,
        }
    }
}
fn inspection_error(error: io::Error) -> Refused {
    match error.kind() {
        io::ErrorKind::WouldBlock => Refused::new("scope_revision_conflict", 409),
        io::ErrorKind::PermissionDenied => Refused::new("supervisor_identity_mismatch", 403),
        io::ErrorKind::NotFound => Refused::new("supervisor_missing", 404),
        io::ErrorKind::InvalidInput => Refused::new("project_scope_invalid", 400),
        io::ErrorKind::InvalidData => Refused::new("scope_authority_invalid", 503),
        _ => Refused::new("scope_unavailable", 503),
    }
}
#[async_trait]
pub(super) trait ScopeStore: Send + Sync {
    async fn prepare(&self, intent: &SupervisorManagementRequest) -> Result<ScopeContext, Refused>;
    async fn apply(&self, submission: &Submission<SupervisorManagementRequest>) -> Outcome;
}
pub(super) struct CanonicalScope(pub(super) web::Data<AppState>);
#[async_trait]
impl ScopeStore for CanonicalScope {
    async fn prepare(&self, intent: &SupervisorManagementRequest) -> Result<ScopeContext, Refused> {
        if intent.supervisor.session_id != DEFAULT_SUPERVISOR_SESSION_ID
            || intent.supervisor.incarnation_id.is_nil()
        {
            return Err(Refused::new("supervisor_identity_mismatch", 403));
        }
        let SupervisorManagementMutation::ConfigureProjectScope { allowed_projects } =
            &intent.mutation
        else {
            return Err(Refused::new("project_scope_invalid", 400));
        };
        if allowed_projects.len() > MAX_SUPERVISOR_PROJECTS {
            return Err(Refused::new("project_scope_invalid", 400));
        }
        let canonical = self
            .0
            .storage
            .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
            .await
            .map_err(inspection_error)?
            .ok_or_else(|| Refused::new("supervisor_missing", 404))?;
        if canonical.supervisor_incarnation_id() != Some(intent.supervisor.incarnation_id) {
            return Err(Refused::new("supervisor_identity_mismatch", 403));
        }
        let scope = SupervisorSessionService::new(self.0.storage.clone())
            .inspect_scope(&intent.supervisor)
            .await
            .map_err(inspection_error)?;
        if scope.state_revision != intent.expected_state_revision {
            return Err(Refused::new("scope_revision_conflict", 409));
        }
        let projects = self.0.project_store.clone();
        let selected = allowed_projects.clone();
        let names = tokio::task::spawn_blocking(move || {
            let mut names = BTreeMap::new();
            for id in selected {
                match projects.get(&id) {
                    Ok(project) if project.status == ProjectStatus::Active => {
                        names.insert(id, project.name);
                    }
                    Ok(_) => return Err(Refused::new("project_archived", 409)),
                    Err(ProjectStoreError::NotFound(_)) => {
                        return Err(Refused::new("project_unavailable", 404))
                    }
                    Err(_) => return Err(Refused::new("project_store_unavailable", 503)),
                }
            }
            Ok(names)
        })
        .await
        .map_err(|_| Refused::new("project_store_unavailable", 503))??;
        let path = self
            .0
            .app_data_dir
            .canonicalize()
            .map_err(|_| Refused::new("native_confirmation_unavailable", 503))?;
        let data_directory = path
            .to_str()
            .ok_or_else(|| Refused::new("native_confirmation_unavailable", 503))?
            .to_owned();
        Ok(ScopeContext {
            data_directory,
            previous_projects: scope.allowed_projects,
            selected_project_names: names,
        })
    }
    async fn apply(&self, submission: &Submission<SupervisorManagementRequest>) -> Outcome {
        let intent = submission.intent();
        let SupervisorManagementMutation::ConfigureProjectScope { allowed_projects } =
            &intent.mutation
        else {
            return Outcome::Refused {
                code: "project_scope_invalid",
                status_code: 400,
            };
        };
        match SupervisorSessionService::new(self.0.storage.clone())
            .configure_project_scope(
                &intent.supervisor,
                intent.expected_state_revision,
                allowed_projects.clone(),
            )
            .await
        {
            Ok(receipt) => Outcome::Committed { receipt },
            // Storage checks revision under its physical guards before publishing.
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Outcome::Refused {
                code: "scope_revision_conflict",
                status_code: 409,
            },
            // Once issued, a generic I/O failure cannot prove publication absent.
            Err(_) => Outcome::Unconfirmed {
                code: "scope_outcome_unconfirmed",
                status_code: 503,
            },
        }
    }
}
