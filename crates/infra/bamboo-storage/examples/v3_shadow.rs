//! Import a captured Session tree into an explicitly named shadow database.
use bamboo_domain::Session;
use bamboo_storage::v3::SessionStoreV3;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let source = args
        .next()
        .ok_or("usage: v3_shadow <session-tree.json> <shadow.sqlite>")?;
    let target = args
        .next()
        .ok_or("usage: v3_shadow <session-tree.json> <shadow.sqlite>")?;
    if args.next().is_some() {
        return Err("unexpected arguments".into());
    }
    let sessions: Vec<Session> = serde_json::from_slice(&std::fs::read(source)?)?;
    let store = SessionStoreV3::open(target)?;
    store.import_tree_shadow(&sessions)?;
    for session in &sessions {
        if !store.verify_shadow(session)? {
            return Err("shadow verification mismatch".into());
        }
    }
    println!(
        "Verified {} session snapshots. Runtime authority is unchanged.",
        sessions.len()
    );
    Ok(())
}
