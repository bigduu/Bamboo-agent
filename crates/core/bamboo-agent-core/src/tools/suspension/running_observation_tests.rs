use super::*;
#[tokio::test]
async fn prestart_observation_preserves_allowed_running_and_nested_denial_is_monotonic() {
    let ((outer, inner), report) = observe_tool_suspension(false, async {
        assert!(suspension_allowed());
        assert!(!observe_running_outcome());
        let outer = capture_suspension_restriction().unwrap();
        let (_, inner) = observe_tool_suspension(true, async {
            let (_, nested) = observe_tool_suspension(false, async {
                assert!(!suspension_allowed());
                assert!(observe_running_outcome());
            })
            .await;
            assert!(nested.forbidden_running_seen);
        })
        .await;
        (outer.observation(), inner)
    })
    .await;
    for seen in [outer, inner, report] {
        assert!(seen.running_seen);
        assert!(seen.forbidden_running_seen);
    }
    assert!(suspension_allowed());
    let (_, clean) = observe_tool_suspension(false, async {}).await;
    assert_eq!(clean, ToolSuspensionObservation::default());
}
#[tokio::test]
async fn captured_owned_task_rebind_preserves_observation_and_forbidden_report() {
    let (_, report) = observe_tool_suspension(true, async {
        let token = capture_suspension_restriction().unwrap();
        tokio::spawn(async move {
            token
                .scope(async {
                    assert!(!suspension_allowed());
                    assert!(observe_running_outcome());
                })
                .await
        })
        .await
        .unwrap();
    })
    .await;
    assert!(report.running_seen && report.forbidden_running_seen);
}
#[tokio::test]
async fn retained_token_preserves_report_when_an_inner_observed_future_is_dropped() {
    let (_,report)=observe_tool_suspension(true,async {
        let token=capture_suspension_restriction().unwrap();
        let mut pending=Box::pin(observe_tool_suspension(false,async {observe_running_outcome();std::future::pending::<()>().await;}));
        tokio::select! {biased; _=&mut pending=>panic!("pending completed"),_=std::future::ready(())=>{}}
        drop(pending);assert!(token.observation().forbidden_running_seen);
    }).await;
    assert!(report.forbidden_running_seen);
}
#[tokio::test]
async fn allowed_public_collapse_reports_running_without_changing_ack_or_requesting_kill() {
    use crate::tools::{AsyncWaitKind, RunningCompletion, RunningHandle, ToolOutcome, ToolResult};
    use std::sync::atomic::AtomicUsize;
    let kills = Arc::new(AtomicUsize::new(0));
    let count = kills.clone();
    let (result, report) = observe_tool_suspension(false, async {
        ToolOutcome::Running(RunningHandle {
            tool_call_id: "prestart".into(),
            ack: ToolResult::text(true, "original ack"),
            completion: RunningCompletion::Detached,
            wait_kind: AsyncWaitKind::AsyncTools,
            kill: Box::new(move || {
                count.fetch_add(1, Ordering::SeqCst);
            }),
        })
        .into_tool_result()
    })
    .await;
    assert!(result.success);
    assert_eq!(result.result, "original ack");
    assert_eq!(kills.load(Ordering::SeqCst), 0);
    assert!(report.running_seen);
    assert!(!report.forbidden_running_seen);
}
