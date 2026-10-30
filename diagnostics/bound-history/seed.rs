//! Diagnostic-only synthetic data, written through Bamboo's real storage port.
use bamboo_domain::{AgentRuntimeState, Message, Session, Storage};
use bamboo_storage::SessionStoreV2;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::env::var_os("DIAGNOSTIC_DATA_DIR").ok_or("missing diagnostic data dir")?;
    let store = SessionStoreV2::new(data.into()).await?;
    store
        .get_or_create_default_supervisor("synthetic-diagnostic")
        .await?;
    for (id, label) in [
        ("10000000-0000-4000-8000-000000000001", "success"),
        ("10000000-0000-4000-8000-000000000002", "unknown"),
        ("10000000-0000-4000-8000-000000000003", "conflict"),
    ] {
        let mut session = Session::new(id, "synthetic-diagnostic");
        session.title = format!("Synthetic bound history {label}");
        session.agent_runtime_state = Some(AgentRuntimeState::new(id));
        session.add_message(Message::user(format!("[bound-clear-fixture:{label}]")));
        store.save_session(&session).await?;
        println!("seeded {id} ({label})");
    }
    Ok(())
}
