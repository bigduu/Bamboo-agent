//! Internal, capability-aware WorkerHost placement foundation.
//!
//! The scoped broker supplies operator policy capabilities and physical
//! connection identity before this adapter publishes durable capacity.
//! ActorDirectory admission remains a separate step.

use bamboo_broker::BrokerClient;
use bamboo_domain::{
    ActorPlacementClass, HostPlacementRequest, HostRegistryError, RegisteredWorkerHost,
    WorkerHostRegistration, WorkerSlotLease,
};
use bamboo_storage::v2::FileHostRegistry;
use chrono::{Duration, Utc};

#[derive(Debug, thiserror::Error)]
pub enum PlacementSchedulerError {
    #[error("scoped broker observation failed: {0}")]
    Broker(#[from] bamboo_broker::BrokerError),
    #[error(transparent)]
    Registry(#[from] HostRegistryError),
    #[error("configured WorkerHost is unavailable or its authenticated identity changed")]
    Unavailable,
}

#[derive(Debug, Clone)]
pub struct PlacementScheduler {
    registry: FileHostRegistry,
}

impl PlacementScheduler {
    pub fn new(registry: FileHostRegistry) -> Self {
        Self { registry }
    }

    pub fn registry(&self) -> &FileHostRegistry {
        &self.registry
    }

    /// Observe through a scoped broker peer and CAS the connection generation.
    /// Project/trust/workspace/tool/network policy and slot capacity must be
    /// present in the broker's operator PeerPolicy. Legacy or self-described
    /// peers have no scheduling grant and fail closed.
    pub async fn observe_scoped_host(
        &self,
        broker: &mut BrokerClient,
        expected_host_ref: &str,
        mailbox: &str,
        role: &str,
        ttl: Duration,
    ) -> Result<RegisteredWorkerHost, PlacementSchedulerError> {
        if ttl <= Duration::zero() || ttl > Duration::minutes(2) {
            return Err(HostRegistryError::Invalid.into());
        }
        for _ in 0..3 {
            let previous = self.registry.inspect_host(expected_host_ref).await?;
            let observed = broker
                .observe_host_capacity(mailbox, role)
                .await?
                .ok_or(PlacementSchedulerError::Unavailable)?;
            if observed.host_ref != expected_host_ref
                || observed.mailbox != mailbox
                || observed.role.as_deref() != Some(role)
            {
                return Err(PlacementSchedulerError::Unavailable);
            }
            let capabilities = observed
                .host_capabilities
                .ok_or(PlacementSchedulerError::Unavailable)?;
            let max_slots = observed
                .max_slots
                .ok_or(PlacementSchedulerError::Unavailable)?;
            // This worker opt-in is only a wire-version check. The trusted
            // capability grant above comes from PeerPolicy, and each remote
            // Run must still validate its own EnvironmentLease.
            if capabilities.placement_class != ActorPlacementClass::Local
                && !observed.environment_lease_v1
            {
                return Err(PlacementSchedulerError::Unavailable);
            }
            let now = Utc::now();
            let expires_at = (now + ttl).min(observed.credential_expires_at);
            if expires_at <= now {
                return Err(PlacementSchedulerError::Unavailable);
            }
            let registration = WorkerHostRegistration {
                host_ref: observed.host_ref,
                mailbox: observed.mailbox,
                role: observed.role,
                connection_generation: observed.connection_generation,
                credential_expires_at: observed.credential_expires_at,
                observed_at: now,
                lease_expires_at: expires_at,
                expected_connection_generation: previous.map(|host| host.connection_generation),
                max_slots,
                capabilities,
            };
            match self.registry.observe_host(registration).await {
                Err(HostRegistryError::StaleGeneration) => continue,
                result => return result.map_err(Into::into),
            }
        }
        Err(HostRegistryError::StaleGeneration.into())
    }

    /// Reserve eligible capacity atomically. The returned lease ID is suitable
    /// for ActorActivation.placement_ref, but does not itself authorize a Run.
    pub async fn reserve(
        &self,
        request: HostPlacementRequest,
    ) -> Result<WorkerSlotLease, PlacementSchedulerError> {
        self.registry
            .reserve_slot(request)
            .await
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, path::Path, process::Command, sync::Arc};

    use bamboo_broker::{
        client_config_trusting_cert, BrokerCore, BrokerLimits, BrokerServer, PeerPolicy,
    };
    use bamboo_domain::{HostPlacementIntent, HostPlacementRequest, ProjectId};
    use bamboo_subagent::AgentRef;
    use serde_json::json;
    use tokio::net::TcpListener;

    use super::*;

    const PARENT_TOKEN: &str = "parent-fixture-credential-000000001";
    const WORKER_TOKEN: &str = "worker-fixture-credential-000000001";

    fn certificate(dir: &Path) -> (std::path::PathBuf, std::path::PathBuf) {
        let (cert, key) = (dir.join("cert.pem"), dir.join("key.pem"));
        assert!(Command::new("openssl")
            .args(["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1"])
            .args(["-subj", "/CN=127.0.0.1"])
            .args(["-addext", "subjectAltName=IP:127.0.0.1"])
            .args(["-addext", "basicConstraints=critical,CA:FALSE"])
            .arg("-keyout")
            .arg(&key)
            .arg("-out")
            .arg(&cert)
            .output()
            .unwrap()
            .status
            .success());
        (cert, key)
    }

    async fn scoped_broker(
        dir: &Path,
        attested: bool,
    ) -> (
        String,
        std::path::PathBuf,
        tokio::task::JoinHandle<bamboo_broker::BrokerResult<()>>,
    ) {
        let (cert, key) = certificate(dir);
        let expiry = Utc::now() + Duration::minutes(5);
        let mut worker = json!({
            "credential":WORKER_TOKEN,"host":"trusted-worker","mailbox":"worker",
            "role":"worker","expires_at":expiry,
            "destinations":[],"cancel":[],"presence":[]
        });
        if attested {
            worker["max_slots"] = json!(1);
            worker["host_capabilities"] = json!({
                "placement_class":"remote","project_ids":["project-a"],
                "allow_unscoped_project":false,"trust_zone":"trusted",
                "workspace_labels":["clean-git"],
                "executors":["bamboo-runtime"],"tools":["Glob"],
                "network_zones":["internal"],"network_isolation":true
            });
        }
        let policy = json!({"peers":[
            {"credential":PARENT_TOKEN,"host":"trusted-parent","mailbox":"parent",
             "role":"host","expires_at":expiry,
             "destinations":[{"mailbox":"worker","kinds":["run"]}],
             "cancel":[],"presence":["worker"]},
            worker
        ]});
        let core = Arc::new(BrokerCore::new_scoped(dir.join("broker")));
        let server = Arc::new(
            BrokerServer::with_peer_policy(
                core,
                PeerPolicy::from_json(&serde_json::to_vec(&policy).unwrap()).unwrap(),
                BrokerLimits::default(),
            )
            .with_tls(&cert, &key)
            .unwrap(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("wss://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { server.serve(listener).await });
        (url, cert, task)
    }

    fn agent(mailbox: &str, role: &str) -> AgentRef {
        AgentRef {
            session_id: mailbox.into(),
            role: Some(role.into()),
        }
    }

    fn placement(now: chrono::DateTime<Utc>) -> HostPlacementRequest {
        HostPlacementRequest {
            intent: HostPlacementIntent::PinnedHost {
                host_ref: "trusted-worker".into(),
            },
            actor_id: "child-a".into(),
            run_id: "run-a".into(),
            project_id: Some(ProjectId::parse("project-a").unwrap()),
            trust_zone: "trusted".into(),
            workspace_label: Some("clean-git".into()),
            executor: "bamboo-runtime".into(),
            required_tools: BTreeSet::from(["Glob".into()]),
            network_zone: Some("internal".into()),
            require_network_isolation: true,
            preferred_host_ref: None,
            now,
            lease_expires_at: now + Duration::seconds(20),
        }
    }

    async fn wait_for_worker(parent: &mut BrokerClient) {
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                if parent
                    .observe_host("worker", "worker")
                    .await
                    .unwrap()
                    .is_some()
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn scoped_broker_policy_registers_current_generation_and_fences_reconnect() {
        let dir = tempfile::tempdir().unwrap();
        let (url, cert, server) = scoped_broker(dir.path(), true).await;
        let tls = || Some(client_config_trusting_cert(&cert).unwrap());
        let mut worker =
            BrokerClient::connect_with_tls(&url, agent("worker", "worker"), WORKER_TOKEN, tls())
                .await
                .unwrap();
        worker.subscribe_environment_lease_v1().await.unwrap();
        let mut parent =
            BrokerClient::connect_with_tls(&url, agent("parent", "host"), PARENT_TOKEN, tls())
                .await
                .unwrap();
        let registry = FileHostRegistry::new(dir.path().join("home"))
            .await
            .unwrap();
        let scheduler = PlacementScheduler::new(registry.clone());
        wait_for_worker(&mut parent).await;
        let first = scheduler
            .observe_scoped_host(
                &mut parent,
                "trusted-worker",
                "worker",
                "worker",
                Duration::seconds(30),
            )
            .await
            .unwrap();
        assert_eq!(first.host_ref, "trusted-worker");
        assert_eq!(first.max_slots, 1);
        assert_eq!(first.capabilities.trust_zone, "trusted");
        let old = scheduler.reserve(placement(Utc::now())).await.unwrap();
        assert_eq!(old.connection_generation, first.connection_generation);

        let mut replacement =
            BrokerClient::connect_with_tls(&url, agent("worker", "worker"), WORKER_TOKEN, tls())
                .await
                .unwrap();
        replacement.subscribe_environment_lease_v1().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                if parent
                    .observe_host("worker", "worker")
                    .await
                    .unwrap()
                    .is_some_and(|host| host.connection_generation != first.connection_generation)
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let next = scheduler
            .observe_scoped_host(
                &mut parent,
                "trusted-worker",
                "worker",
                "worker",
                Duration::seconds(30),
            )
            .await
            .unwrap();
        assert_ne!(first.connection_generation, next.connection_generation);
        assert!(matches!(
            registry.validate_slot(&old, Utc::now()).await,
            Err(HostRegistryError::StaleLease)
        ));
        server.abort();
    }

    #[tokio::test]
    async fn legacy_scoped_peer_has_no_capacity_grant() {
        let dir = tempfile::tempdir().unwrap();
        let (url, cert, server) = scoped_broker(dir.path(), false).await;
        let tls = || Some(client_config_trusting_cert(&cert).unwrap());
        let mut worker =
            BrokerClient::connect_with_tls(&url, agent("worker", "worker"), WORKER_TOKEN, tls())
                .await
                .unwrap();
        worker.subscribe_environment_lease_v1().await.unwrap();
        let mut parent =
            BrokerClient::connect_with_tls(&url, agent("parent", "host"), PARENT_TOKEN, tls())
                .await
                .unwrap();
        let registry = FileHostRegistry::new(dir.path().join("home"))
            .await
            .unwrap();
        let scheduler = PlacementScheduler::new(registry.clone());
        wait_for_worker(&mut parent).await;
        assert!(matches!(
            scheduler
                .observe_scoped_host(
                    &mut parent,
                    "trusted-worker",
                    "worker",
                    "worker",
                    Duration::seconds(30),
                )
                .await,
            Err(PlacementSchedulerError::Unavailable)
        ));
        assert!(registry
            .inspect_host("trusted-worker")
            .await
            .unwrap()
            .is_none());
        server.abort();
    }
}
