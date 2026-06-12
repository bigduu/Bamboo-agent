//! Demo actor worker: boots a WS server, self-registers into the Tier-1 fabric, serves one run
//! with the dependency-free [`EchoExecutor`], then withdraws and exits.
//!
//! Usage: `subagent-demo-worker <child_id> <fabric_dir> [role]`
//!
//! This is the stand-in for `bamboo subagent-worker`: it exercises the full
//! spawn → self-register → discover → WS run → stream → terminal path with no LLM/runtime.

use std::path::PathBuf;
use std::sync::Arc;

use chrono::{Duration, Utc};

use bamboo_subagent::discovery::Fabric;
use bamboo_subagent::executor::EchoExecutor;
use bamboo_subagent::proto::AgentRecord;
use bamboo_subagent::transport::WsServer;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut args = std::env::args().skip(1);
    let child_id = args.next().expect("usage: <child_id> <fabric_dir> [role]");
    let fabric_dir = PathBuf::from(args.next().expect("missing fabric_dir"));
    let role = args.next().unwrap_or_else(|| "worker".to_string());

    let server = WsServer::bind_loopback()
        .await
        .expect("bind loopback ws server");
    let endpoint = server.ws_endpoint();

    let fab = Fabric::at(&fabric_dir);
    let now = Utc::now();
    let record = AgentRecord {
        agent_id: child_id.clone(),
        role,
        labels: Vec::new(),
        endpoint,
        pid: std::process::id(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        started_at: now,
        lease_expires_at: now + Duration::seconds(60),
    };
    fab.publish(&record).await.expect("publish discovery record");

    // Serve a single connection, then clean up.
    let _ = server.serve_one(Arc::new(EchoExecutor)).await;
    let _ = fab.withdraw(&child_id).await;
}
