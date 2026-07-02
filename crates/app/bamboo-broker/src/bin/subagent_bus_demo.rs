//! Test-only worker binary: reads a `ProvisionSpec` from stdin, dials the
//! mailbox bus it names (`spec.bus`), and serves an `EchoExecutor` over it.
//!
//! Mirrors exactly what the real `bamboo subagent-worker` does on the bus path
//! (dial + `serve_executor`), so the `bus_e2e` integration test can drive a REAL
//! subprocess worker over a real broker — validating `spawn_worker_on_bus` +
//! the worker's bus-dial + `BrokerChildLink` end to end.

use std::sync::Arc;

use bamboo_subagent::{AgentRef, EchoExecutor, ProvisionSpec};

#[tokio::main]
async fn main() {
    let spec = ProvisionSpec::read_from_stdin()
        .await
        .expect("read ProvisionSpec from stdin");
    let bus = spec.bus.as_ref().expect("spec.bus must be set for this worker");
    let me = AgentRef {
        session_id: spec.identity.child_id.clone(),
        role: Some(spec.identity.role.clone()),
    };
    if let Err(e) =
        bamboo_broker::serve_executor(&bus.endpoint, me, &bus.token, Arc::new(EchoExecutor)).await
    {
        eprintln!("bus demo worker serve ended: {e}");
    }
}
