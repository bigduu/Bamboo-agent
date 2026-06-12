//! Demo actor worker — the fixed three-stage worker shape:
//!
//! ```text
//! read ProvisionSpec from stdin → executor factory → bind WS / self-register / serve → cleanup
//! ```
//!
//! This binary only carries the dependency-free [`EchoExecutor`]; the real
//! `bamboo subagent-worker` subcommand maps `ExecutorSpec::BambooRuntime` to the
//! actual agent loop via the same factory seam.

use std::sync::Arc;

use chrono::{Duration, Utc};

use bamboo_subagent::discovery::Fabric;
use bamboo_subagent::executor::{ChildExecutor, EchoExecutor};
use bamboo_subagent::proto::AgentRecord;
use bamboo_subagent::provision::{ExecutorSpec, ProvisionSpec};
use bamboo_subagent::transport::WsServer;

/// Stage 2: map the parent-declared engine to an implementation.
/// Adding an engine = one new arm here; stages 1 and 3 never change.
fn build_executor(spec: &ProvisionSpec) -> Result<Arc<dyn ChildExecutor>, String> {
    match &spec.executor {
        ExecutorSpec::Echo => Ok(Arc::new(EchoExecutor)),
        other => Err(format!(
            "demo worker only supports the echo executor, got {other:?}; \
             use `bamboo subagent-worker` for real engines"
        )),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // Stage 1: provision (one JSON document on stdin; pipe closes after).
    let spec = ProvisionSpec::read_from_stdin()
        .await
        .expect("read ProvisionSpec from stdin");

    // Stage 2: executor factory.
    let executor = match build_executor(&spec) {
        Ok(e) => e,
        Err(msg) => {
            eprintln!("provision error: {msg}");
            std::process::exit(2);
        }
    };

    // Stage 3: bind, self-register, serve, cleanup.
    let server = WsServer::bind_loopback()
        .await
        .expect("bind loopback ws server");
    let endpoint = server.ws_endpoint();

    let fab = Fabric::at(&spec.fabric_dir);
    let now = Utc::now();
    let record = AgentRecord {
        agent_id: spec.identity.child_id.clone(),
        role: spec.identity.role.clone(),
        labels: Vec::new(),
        endpoint,
        pid: std::process::id(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        started_at: now,
        lease_expires_at: now + Duration::seconds(60),
    };
    fab.publish(&record).await.expect("publish discovery record");

    // Serve a single connection, then clean up. Self-terminate if the parent
    // never connects (orphan defense).
    let _ = server
        .serve_one_with_accept_timeout(executor, std::time::Duration::from_secs(120))
        .await;
    let _ = fab.withdraw(&spec.identity.child_id).await;
}
