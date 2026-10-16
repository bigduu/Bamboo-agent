use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
struct NeverDrive {
    polled: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}
impl Future for NeverDrive {
    type Output = ToolResult;
    fn poll(self: Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<ToolResult> {
        self.polled.fetch_add(1, Ordering::SeqCst);
        panic!("refusal must not drive completion")
    }
}
impl Drop for NeverDrive {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
fn running(
    driven: bool,
    panic_kill: bool,
    kills: Arc<AtomicUsize>,
    polled: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
) -> ToolOutcome {
    ToolOutcome::Running(RunningHandle {
        tool_call_id: "restricted-running".into(),
        ack: ToolResult::text(true, "original successful ack"),
        completion: if driven {
            RunningCompletion::Driven(Box::pin(NeverDrive { polled, dropped }))
        } else {
            RunningCompletion::Detached
        },
        wait_kind: AsyncWaitKind::AsyncTools,
        kill: Box::new(move || {
            kills.fetch_add(1, Ordering::SeqCst);
            assert!(!panic_kill, "injected cleanup panic");
        }),
    })
}
#[tokio::test]
async fn restricted_collapse_kills_once_and_drops_driven_even_when_cleanup_panics() {
    for driven in [false, true] {
        for panic_kill in [false, true] {
            let kills = Arc::new(AtomicUsize::new(0));
            let polled = Arc::new(AtomicUsize::new(0));
            let dropped = Arc::new(AtomicUsize::new(0));
            let outcome = running(
                driven,
                panic_kill,
                kills.clone(),
                polled.clone(),
                dropped.clone(),
            );
            let result =
                crate::tools::with_suspension_disabled(async { outcome.into_tool_result() }).await;
            assert!(!result.success);
            assert!(!result.result.contains("original successful ack"));
            assert!(result.result.contains("stop unconfirmed"));
            assert_eq!(kills.load(Ordering::SeqCst), 1);
            assert_eq!(polled.load(Ordering::SeqCst), 0);
            assert_eq!(dropped.load(Ordering::SeqCst), usize::from(driven));
        }
    }
}
#[test]
fn ordinary_collapse_keeps_ack_without_requesting_kill() {
    let kills = Arc::new(AtomicUsize::new(0));
    let outcome = running(
        false,
        false,
        kills.clone(),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
    );
    let result = outcome.into_tool_result();
    assert!(result.success);
    assert_eq!(result.result, "original successful ack");
    assert_eq!(kills.load(Ordering::SeqCst), 0);
}
