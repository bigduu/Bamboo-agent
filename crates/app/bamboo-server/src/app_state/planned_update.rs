use std::io;
use std::sync::Arc;

use bamboo_engine::auto_dream::{MemoryMaintenanceAdmission, MemoryMaintenanceUpdateHold};
use bamboo_engine::execution::ExternalChildRunner;
use bamboo_engine::external_agents::local_task::NativeChildUpdateHold;
use bamboo_engine::session_activation::{
    OrdinaryRootUpdateResources, ParkedUpdateRoot, SessionUpdateAdmission,
};
use tokio::sync::{MutexGuard, OwnedMutexGuard};
use tokio::task::JoinHandle;

use super::ticket_application::TicketUpdatePreparation;
use super::update_admission::{DispatchHold, UntrackedUpdateAdmission};
use super::AppState;
use crate::browser::{BrowserManager, BrowserUpdateHold};

pub(super) struct ManagedUpdateRuntime {
    pub(super) boot_refusal: Option<&'static str>,
    pub(super) memory: MemoryMaintenanceAdmission,
    pub(super) lifecycle_hooks: UntrackedUpdateAdmission,
}

pub(crate) enum HostUpdateReadiness {
    Busy {
        pending_sessions: Vec<String>,
        message: String,
    },
    Ready,
}

struct ReadyHosts {
    root: OrdinaryRootUpdateResources,
    child: NativeChildUpdateHold,
    browser: BrowserUpdateHold,
    memory: MemoryMaintenanceUpdateHold,
    schedule: DispatchHold,
    workflow: DispatchHold,
    side_chat: DispatchHold,
    lifecycle_hooks: DispatchHold,
    ticket: TicketUpdatePreparation,
    _plugin: MutexGuard<'static, ()>,
    _config: OwnedMutexGuard<()>,
}

enum HostUpdatePhase {
    Preparing(Option<ReadyHosts>),
    Committing {
        finish: JoinHandle<io::Result<()>>,
        _ticket: TicketUpdatePreparation,
        _plugin: MutexGuard<'static, ()>,
        _config: OwnedMutexGuard<()>,
    },
    Finished {
        result: Result<(), String>,
        _ticket: TicketUpdatePreparation,
        _plugin: MutexGuard<'static, ()>,
        _config: OwnedMutexGuard<()>,
    },
}

pub(crate) struct HostUpdatePreparation {
    child: Arc<dyn ExternalChildRunner>,
    browser: Arc<BrowserManager>,
    memory: MemoryMaintenanceAdmission,
    config: Arc<tokio::sync::RwLock<bamboo_config::Config>>,
    config_io: Arc<tokio::sync::Mutex<()>>,
    schedule: UntrackedUpdateAdmission,
    workflow: UntrackedUpdateAdmission,
    side_chat: UntrackedUpdateAdmission,
    lifecycle_hooks: UntrackedUpdateAdmission,
    services: Arc<crate::service_manager::ServiceManager>,
    mcp: Arc<bamboo_mcp::manager::McpServerManager>,
    tickets: Arc<super::ticket_application::TicketApplication>,
    fabric: bamboo_server_tools::DeployedRegistry,
    phase: HostUpdatePhase,
    // Release every reversible physical hold before waking parked sessions.
    admission: SessionUpdateAdmission,
}

impl AppState {
    pub(crate) fn managed_lifecycle_hook_admission(&self) -> Option<&UntrackedUpdateAdmission> {
        self.managed_update_runtime
            .as_ref()
            .map(|runtime| &runtime.lifecycle_hooks)
    }

    pub(crate) fn supports_managed_planned_update(&self) -> bool {
        self.managed_update_runtime.is_some()
    }

    pub(crate) async fn begin_planned_update(&self) -> io::Result<HostUpdatePreparation> {
        let runtime = self
            .managed_update_runtime
            .as_ref()
            .ok_or_else(|| unsupported("This Host was not started for managed updates"))?;
        if let Some(reason) = runtime.boot_refusal {
            return Err(unsupported(reason));
        }
        let boot_reconcile = self.boot_reconcile_services_handle.lock().await;
        if boot_reconcile
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
        {
            return Err(busy("Waiting for plugin service startup reconciliation"));
        }
        drop(boot_reconcile);
        let admission = self
            .session_activation_router
            .hold_admissions_for_update()
            .await
            .ok_or_else(|| busy("Another planned update owns session admission"))?;
        Ok(HostUpdatePreparation {
            admission,
            child: self.external_child_runner.clone(),
            browser: self.browser.clone(),
            memory: runtime.memory.clone(),
            config: self.config.clone(),
            config_io: self.config_io_lock.clone(),
            schedule: self.schedule_manager.update_admission().clone(),
            workflow: self.workflow_runs.update_admission().clone(),
            side_chat: self.side_chats.update_admission.clone(),
            lifecycle_hooks: runtime.lifecycle_hooks.clone(),
            services: self.service_manager.clone(),
            mcp: self.mcp_manager.clone(),
            tickets: self.tickets.clone(),
            fabric: self.fabric_deployer.registry(),
            phase: HostUpdatePhase::Preparing(None),
        })
    }
}

impl HostUpdatePreparation {
    pub(crate) async fn observe(&mut self) -> io::Result<HostUpdateReadiness> {
        let HostUpdatePhase::Preparing(ready) = &self.phase else {
            return Err(io::Error::other(
                "Planned update intent has already committed",
            ));
        };
        if ready.is_some() {
            return Ok(HostUpdateReadiness::Ready);
        }

        let owners = match self.try_hold_owners().await {
            Ok(owners) => owners,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                return Ok(HostUpdateReadiness::Busy {
                    pending_sessions: self.admission.outstanding_sessions().await,
                    message: error.to_string(),
                });
            }
            Err(error) => return Err(error),
        };
        self.phase = HostUpdatePhase::Preparing(Some(owners));
        Ok(HostUpdateReadiness::Ready)
    }

    async fn try_hold_owners(&self) -> io::Result<ReadyHosts> {
        // Install/reconcile already takes plugin before configuration. Retain
        // both through ordinary HTTP drain, including old requests in flight.
        let plugin = bamboo_plugin::registry::PLUGIN_OPERATION_LOCK
            .try_lock()
            .map_err(|_| busy("Waiting for an active plugin operation"))?;
        let config = self
            .config_io
            .clone()
            .try_lock_owned()
            .map_err(|_| busy("Waiting for a configuration operation"))?;
        if let Some(reason) = unsupported_host_routes(&*self.config.read().await) {
            return Err(unsupported(reason));
        }
        self.require_supported_host_owners().await?;

        let schedule = self.schedule.try_hold(
            "Waiting for schedule admission to finish",
            "This Host has dispatched scheduled work without an update checkpoint",
        )?;
        let workflow = self.workflow.try_hold(
            "Waiting for Workflow admission to finish",
            "This Host has dispatched a Workflow without an update checkpoint",
        )?;
        let side_chat = self.side_chat.try_hold(
            "Waiting for Side Chat admission to finish",
            "This Host has dispatched a Side Chat without an update checkpoint",
        )?;
        let lifecycle_hooks = self.lifecycle_hooks.try_hold(
            "Waiting for lifecycle hook admission",
            "This Host has dispatched a lifecycle hook without an update checkpoint",
        )?;
        let ticket = self.tickets.try_hold_for_planned_update()?;
        let memory = self
            .memory
            .try_hold_for_update()?
            .ok_or_else(|| busy("Waiting for memory maintenance"))?;
        let browser = self
            .browser
            .try_hold_for_planned_update()?
            .ok_or_else(|| busy("Waiting for owned browser activity to finish"))?;
        let child = self
            .child
            .try_hold_native_children_for_update()?
            .ok_or_else(|| busy("Waiting for native Child executions to finish"))?;
        let root = self
            .admission
            .try_hold_ordinary_root_resources()
            .await?
            .ok_or_else(|| busy("Waiting for sessions to reach an update checkpoint"))?;
        Ok(ReadyHosts {
            root,
            child,
            browser,
            memory,
            schedule,
            workflow,
            side_chat,
            lifecycle_hooks,
            ticket,
            _plugin: plugin,
            _config: config,
        })
    }

    async fn require_supported_host_owners(&self) -> io::Result<()> {
        if !self.fabric.lock().await.is_empty() {
            return Err(unsupported(
                "Active cluster workers prevent a planned update",
            ));
        }
        if !self.mcp.list_servers().is_empty() || !self.services.list_status().await.is_empty() {
            return Err(unsupported(
                "Active MCP or plugin services prevent a planned update",
            ));
        }
        Ok(())
    }

    pub(crate) async fn commit(
        &mut self,
        publish: impl FnOnce(&[ParkedUpdateRoot]) -> io::Result<()> + Send,
    ) -> io::Result<()> {
        let HostUpdatePhase::Preparing(ready) = &mut self.phase else {
            return Err(io::Error::other(
                "Planned update intent has already committed",
            ));
        };
        let proof = ready
            .as_ref()
            .ok_or_else(|| busy("Planned update preparation is incomplete"))?;
        let committed = self
            .admission
            .commit_parked_roots_with_resources(&proof.root, publish)
            .await;
        let ready = ready.take().expect("observed ready Host owners");
        committed?;
        let ReadyHosts {
            root,
            child,
            browser,
            memory,
            schedule,
            workflow,
            side_chat,
            lifecycle_hooks,
            mut ticket,
            _plugin,
            _config,
        } = ready;
        browser.commit();
        memory.close();
        schedule.close();
        workflow.close();
        side_chat.close();
        lifecycle_hooks.close();
        ticket.commit();
        let external_child = self.child.clone();
        let finish = tokio::spawn(async move {
            root.finish_committed().await?;
            // The logical admission stays closed. Release the preparation
            // permits so the same Child owner can enter its terminal drain.
            drop(child);
            external_child.shutdown().await.map_err(io::Error::other)
        });
        self.phase = HostUpdatePhase::Committing {
            finish,
            _ticket: ticket,
            _plugin,
            _config,
        };
        Ok(())
    }

    pub(crate) async fn observe_committed(&mut self) -> io::Result<HostUpdateReadiness> {
        let phase = &mut self.phase;
        match phase {
            HostUpdatePhase::Committing { finish, .. } if !finish.is_finished() => {
                return Ok(HostUpdateReadiness::Busy {
                    pending_sessions: self.admission.outstanding_sessions().await,
                    message: "Waiting for the old Host to finish owned executions".into(),
                });
            }
            HostUpdatePhase::Committing { finish, .. } => {
                let result = finish
                    .await
                    .map_err(io::Error::other)
                    .and_then(|result| result)
                    .map_err(|error| error.to_string());
                let HostUpdatePhase::Committing {
                    _ticket,
                    _plugin,
                    _config,
                    ..
                } = std::mem::replace(phase, HostUpdatePhase::Preparing(None))
                else {
                    unreachable!("only the owning protocol can change update phase");
                };
                *phase = HostUpdatePhase::Finished {
                    result,
                    _ticket,
                    _plugin,
                    _config,
                };
            }
            HostUpdatePhase::Preparing(_) => {
                return Err(io::Error::other("Planned update intent is not committed"));
            }
            HostUpdatePhase::Finished { .. } => {}
        }
        let HostUpdatePhase::Finished { result, .. } = phase else {
            unreachable!("committed progress was observed above");
        };
        result
            .as_ref()
            .map(|_| HostUpdateReadiness::Ready)
            .map_err(|error| io::Error::other(error.clone()))
    }
}

pub(super) fn unsupported_host_routes(config: &bamboo_config::Config) -> Option<&'static str> {
    if bamboo_engine::external_agents::runtime::requires_legacy_child_runtime(config)
        || config
            .subagents()
            .broker
            .as_ref()
            .is_some_and(|broker| !broker.endpoint.trim().is_empty())
        || config.extra.get("externalAgents").is_some_and(|value| {
            value
                .as_object()
                .is_none_or(|profiles| !profiles.is_empty())
        })
    {
        return Some("Remote or legacy Child execution prevents a planned update");
    }
    if !config.connect.platforms.is_empty() || !config.mcp.servers.is_empty() {
        return Some("Configured Connect or MCP workers prevent a planned update");
    }
    if config.lifecycle_hooks.enabled {
        return Some("Configured lifecycle hooks prevent a planned update");
    }
    None
}

fn busy(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, message)
}

fn unsupported(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::Unsupported, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    async fn start_managed_host(profile: &tempfile::TempDir) -> AppState {
        let state = AppState::new_for_managed_update(profile.path().to_owned())
            .await
            .unwrap();
        state.wait_for_boot_reconcile_services().await;
        assert!(state.supports_managed_planned_update());
        let config = state.config.read().await;
        assert!(config.features.ticket_mutation && config.features.ticket_dispatch);
        drop(config);
        state
    }

    async fn wait_until_ready(preparation: &mut HostUpdatePreparation, committed: bool) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let readiness = if committed {
                    preparation.observe_committed().await
                } else {
                    preparation.observe().await
                };
                if matches!(readiness.unwrap(), HostUpdateReadiness::Ready) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("quiet native desktop reaches update readiness");
    }

    #[tokio::test]
    async fn managed_update_host_holds_actual_native_owner_and_reopens_on_abort() {
        let profile = tempfile::tempdir().unwrap();
        let state = start_managed_host(&profile).await;
        let mut session = bamboo_domain::Session::new("update-test-no-hooks", "model");
        let prompt = crate::lifecycle_hooks::apply_user_prompt_submit_hooks(
            &Default::default(),
            Some(profile.path().to_owned()),
            &mut session,
            "a plain prompt",
            state.managed_lifecycle_hook_admission(),
        )
        .await
        .unwrap();
        assert_eq!(prompt, "a plain prompt");
        let mut preparation = state.begin_planned_update().await.unwrap();
        wait_until_ready(&mut preparation, false).await;

        assert!(state
            .external_child_runner
            .try_hold_native_children_for_update()
            .unwrap()
            .is_none());
        assert!(matches!(
            state.begin_planned_update().await,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock
        ));
        drop(preparation);

        assert!(state
            .external_child_runner
            .try_hold_native_children_for_update()
            .unwrap()
            .is_some());
        let mut preparation = state.begin_planned_update().await.unwrap();
        wait_until_ready(&mut preparation, false).await;
        preparation
            .commit(|roots| {
                assert!(roots.is_empty());
                Ok(())
            })
            .await
            .unwrap();
        wait_until_ready(&mut preparation, true).await;
        drop(preparation);
        assert!(state.begin_planned_update().await.is_err());
        assert!(state
            .side_chats
            .update_admission
            .record_dispatch()
            .await
            .is_err());
        assert!(state
            .managed_lifecycle_hook_admission()
            .unwrap()
            .record_dispatch()
            .await
            .is_err());
        state.shutdown().await;
    }

    #[tokio::test]
    async fn managed_update_host_refuses_a_dispatched_side_chat_and_releases_child_admission() {
        let profile = tempfile::tempdir().unwrap();
        let state = start_managed_host(&profile).await;
        state
            .side_chats
            .update_admission
            .record_dispatch()
            .await
            .unwrap();
        let mut preparation = state.begin_planned_update().await.unwrap();
        let error = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match preparation.observe().await {
                    Err(error) => break error,
                    Ok(HostUpdateReadiness::Busy { .. }) => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Ok(HostUpdateReadiness::Ready) => panic!("detached work cannot prove closure"),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert!(error.to_string().contains("dispatched a Side Chat"));
        drop(preparation);
        assert!(state
            .external_child_runner
            .try_hold_native_children_for_update()
            .unwrap()
            .is_some());
        state
            .side_chats
            .update_admission
            .record_dispatch()
            .await
            .unwrap();
        state.shutdown().await;
    }
}
