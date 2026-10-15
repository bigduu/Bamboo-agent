//! Host-only observation and optional denial of detached tool outcomes.
//! No Session/model field can construct or weaken the scoped authority.
use std::future::Future;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ToolSuspensionObservation {
    pub running_seen: bool,
    pub forbidden_running_seen: bool,
}
#[derive(Default)]
struct SharedObservation {
    running_seen: AtomicBool,
    forbidden_running_seen: AtomicBool,
}
/// Opaque, clonable host token; rebinds the same sticky report in an owned task.
#[derive(Clone)]
pub struct SuspensionRestriction {
    shared: Arc<SharedObservation>,
    deny: bool,
}
tokio::task_local! { static TOOL_SUSPENSION_SCOPE: SuspensionRestriction; }
impl SuspensionRestriction {
    pub fn observation(&self) -> ToolSuspensionObservation {
        let forbidden = self.shared.forbidden_running_seen.load(Ordering::Acquire);
        ToolSuspensionObservation {
            running_seen: forbidden || self.shared.running_seen.load(Ordering::Acquire),
            forbidden_running_seen: forbidden,
        }
    }
    /// A nested scope inherits the current shared report; denial is monotonic OR.
    pub async fn scope<F: Future>(&self, future: F) -> F::Output {
        let mut token = capture_suspension_restriction().unwrap_or_else(|| self.clone());
        token.deny |= self.deny;
        TOOL_SUSPENSION_SCOPE.scope(token, future).await
    }
}
pub fn capture_suspension_restriction() -> Option<SuspensionRestriction> {
    TOOL_SUSPENSION_SCOPE.try_with(Clone::clone).ok()
}
pub fn current_tool_suspension_scope() -> Option<SuspensionRestriction> {
    capture_suspension_restriction()
}
/// Observing before Start changes no tool result. Nested false cannot weaken true.
/// Keep a captured token outside a separately imposed timeout if that timeout
/// could discard this future's returned report.
pub async fn observe_tool_suspension<F: Future>(
    deny: bool,
    future: F,
) -> (F::Output, ToolSuspensionObservation) {
    let mut token = capture_suspension_restriction().unwrap_or_else(|| SuspensionRestriction {
        shared: Arc::new(SharedObservation::default()),
        deny: false,
    });
    token.deny |= deny;
    let output = token.scope(future).await;
    (output, token.observation())
}
pub async fn with_suspension_disabled<F: Future>(future: F) -> F::Output {
    observe_tool_suspension(true, future).await.0
}
pub async fn with_suspension_disabled_observed<F: Future>(future: F) -> (F::Output, bool) {
    let (output, seen) = observe_tool_suspension(true, future).await;
    (output, seen.forbidden_running_seen)
}
pub fn suspension_allowed() -> bool {
    capture_suspension_restriction().is_none_or(|token| !token.deny)
}
pub fn suspension_running_violated() -> bool {
    capture_suspension_restriction().is_some_and(|token| token.observation().forbidden_running_seen)
}
/// Record every scoped Running; return whether it is forbidden in this scope.
pub(crate) fn observe_running_outcome() -> bool {
    let Some(token) = capture_suspension_restriction() else {
        return false;
    };
    token.shared.running_seen.store(true, Ordering::Release);
    if token.deny {
        token
            .shared
            .forbidden_running_seen
            .store(true, Ordering::Release);
    }
    token.deny
}
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn host_only_suspension_scope_is_nested_task_local_and_not_session_metadata() {
        assert!(suspension_allowed());
        with_suspension_disabled(async {
            let mut forged = crate::Session::new("suspension-probe", "synthetic-only");
            forged
                .metadata
                .insert("suspension_allowed".into(), "true".into());
            assert!(!suspension_allowed());
            with_suspension_disabled(async {
                assert!(!suspension_allowed());
            })
            .await;
            assert!(!suspension_allowed());
            // A new task needs its own host scope. Side-effect owners must
            // reject before they spawn; no model-controlled flag restores it.
            assert!(tokio::spawn(async { suspension_allowed() }).await.unwrap());
            assert!(!suspension_allowed());
        })
        .await;
        assert!(suspension_allowed());
    }

    #[tokio::test]
    async fn dropping_a_scoped_future_restores_the_callers_default() {
        let mut pending = Box::pin(with_suspension_disabled(std::future::pending::<()>()));
        tokio::select! {
            biased;
            _ = &mut pending => panic!("pending future completed"),
            _ = std::future::ready(()) => {}
        }
        drop(pending);
        assert!(suspension_allowed());
    }
}

#[cfg(test)]
mod running_observation_tests;
