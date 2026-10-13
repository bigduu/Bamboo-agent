//! Opt-in, fixture-scoped error capture before the public facade redacts it.
//! Only compiled with test-utils; never changes the result returned to a model.
use bamboo_agent_core::tools::{ToolCtx, ToolError, ToolOutcome};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

type Key = (String, String);
type Slot = Arc<Mutex<Option<String>>>;
fn registry() -> &'static Mutex<HashMap<Key, Slot>> {
    static REGISTRY: OnceLock<Mutex<HashMap<Key, Slot>>> = OnceLock::new();
    REGISTRY.get_or_init(Mutex::default)
}

pub struct ErrorProbe {
    key: Key,
    slot: Slot,
}

/// Register only a known synthetic fixture's exact session and call identity.
/// A bounded registry and RAII removal prevent process-wide error accumulation.
pub fn install(session_id: &str, call_id: &str) -> ErrorProbe {
    let key = (session_id.to_owned(), call_id.to_owned());
    let slot = Arc::new(Mutex::new(None));
    let mut active = registry().lock().unwrap();
    assert!(
        active.len() < 8,
        "too many simultaneous private test probes"
    );
    assert!(!active.contains_key(&key), "duplicate private test probe");
    active.insert(key.clone(), slot.clone());
    ErrorProbe { key, slot }
}

impl ErrorProbe {
    pub fn take(&self) -> Option<String> {
        self.slot.lock().unwrap().take()
    }
}
impl Drop for ErrorProbe {
    fn drop(&mut self) {
        let mut active = registry().lock().unwrap();
        if active
            .get(&self.key)
            .is_some_and(|slot| Arc::ptr_eq(slot, &self.slot))
        {
            active.remove(&self.key);
        }
    }
}

pub(super) fn capture(ctx: &ToolCtx) -> Option<Slot> {
    let key = (ctx.session_id()?.to_owned(), ctx.tool_call_id.to_string());
    registry().lock().unwrap().get(&key).cloned()
}

pub(super) fn record(slot: &Option<Slot>, result: &Result<ToolOutcome, ToolError>) {
    if let (Some(slot), Err(error)) = (slot, result) {
        let mut captured = slot.lock().unwrap();
        if captured.is_none() {
            *captured = Some(error.to_string().chars().take(8192).collect());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_probe_is_exact_opt_in_one_shot_and_removed_on_drop() {
        let probe = install("diagnostic-root", "known-call");
        let mut ctx = ToolCtx::none("known-call");
        ctx.session_id = Some(Arc::from("different-root"));
        assert!(capture(&ctx).is_none());
        ctx.session_id = Some(Arc::from("diagnostic-root"));
        let slot = capture(&ctx);
        let failure = Err(ToolError::Execution("synthetic private failure".into()));
        record(&slot, &failure);
        record(&slot, &Err(ToolError::Execution("later failure".into())));
        assert_eq!(
            probe.take().as_deref(),
            Some("Execution failed: synthetic private failure")
        );
        assert!(probe.take().is_none());
        drop(probe);
        assert!(capture(&ctx).is_none());
    }
}
