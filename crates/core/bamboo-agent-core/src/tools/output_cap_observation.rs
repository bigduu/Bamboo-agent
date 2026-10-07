//! Transient observations of the cap used by one in-process tool dispatch.

use std::future::Future;

use super::ToolCtx;

struct DispatchCap {
    session_id: String,
    tool_call_id: String,
    cap: Option<u32>,
}

tokio::task_local! {
    static DISPATCH_CAP: DispatchCap;
}

/// Observe one real executor future's current output cap, matched by both IDs.
///
/// Hosts can construct this scope; it conveys no source or caller permission.
/// The scope is installed on each poll, restored afterwards, and not inherited
/// by an unwrapped spawned task. No `Send` bound is added to inline futures.
pub async fn scope_tool_output_cap<F: Future>(
    session_id: &str,
    tool_call_id: &str,
    cap: Option<u32>,
    future: F,
) -> F::Output {
    DISPATCH_CAP
        .scope(
            DispatchCap {
                session_id: session_id.to_owned(),
                tool_call_id: tool_call_id.to_owned(),
                cap,
            },
            future,
        )
        .await
}

/// Copy the observation only for the currently executing Session and call.
/// Unknown/absent observations stay `None`; a known no-hard-cap stays `Some(0)`.
/// This scalar is not authorization. Consumers must still validate their current
/// caller, input, configuration and source independently. Nothing is persisted.
pub fn observed_tool_output_cap(ctx: &ToolCtx) -> Option<u32> {
    DISPATCH_CAP
        .try_with(|value| {
            (!value.session_id.is_empty()
                && !value.tool_call_id.is_empty()
                && ctx.session_id.as_deref() == Some(value.session_id.as_str())
                && ctx.tool_call_id.as_ref() == value.tool_call_id.as_str())
            .then_some(value.cap)
            .flatten()
        })
        .ok()
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{rc::Rc, sync::Arc, time::Duration};
    use tokio::sync::{Barrier, Notify};

    fn context(session: Option<&str>, call: &str) -> ToolCtx {
        let mut ctx = ToolCtx::none(call.to_owned());
        ctx.session_id = session.map(Into::into);
        ctx
    }

    #[tokio::test]
    async fn scalar_is_exact_and_wrong_or_missing_identity_stays_unknown() {
        let actual = context(Some("session"), "call");
        assert_eq!(observed_tool_output_cap(&actual), None);
        for cap in [None, Some(0), Some(41), Some(u32::MAX)] {
            scope_tool_output_cap("session", "call", cap, async {
                assert_eq!(observed_tool_output_cap(&actual), cap);
                for wrong in [
                    context(None, "call"),
                    context(Some("other"), "call"),
                    context(Some("session"), "other"),
                    context(Some("session"), ""),
                ] {
                    assert_eq!(observed_tool_output_cap(&wrong), None);
                }
                let copied = observed_tool_output_cap(&actual);
                tokio::task::yield_now().await;
                assert_eq!(copied, cap);
                assert_eq!(observed_tool_output_cap(&actual), cap);
            })
            .await;
            assert_eq!(observed_tool_output_cap(&actual), None);
        }
    }

    #[tokio::test]
    async fn nested_matching_unknown_and_new_call_restore_original_scope() {
        let outer = context(Some("session"), "outer");
        let inner = context(Some("session"), "inner");
        scope_tool_output_cap("session", "outer", Some(7), async {
            assert_eq!(observed_tool_output_cap(&outer), Some(7));
            scope_tool_output_cap("session", "inner", Some(13), async {
                assert_eq!(observed_tool_output_cap(&inner), Some(13));
                assert_eq!(observed_tool_output_cap(&outer), None);
                tokio::task::yield_now().await;
            })
            .await;
            assert_eq!(observed_tool_output_cap(&inner), None);
            assert_eq!(observed_tool_output_cap(&outer), Some(7));
            scope_tool_output_cap("session", "outer", None, async {
                assert_eq!(observed_tool_output_cap(&outer), None);
            })
            .await;
            assert_eq!(observed_tool_output_cap(&outer), Some(7));
        })
        .await;
        assert_eq!(observed_tool_output_cap(&outer), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn moved_send_scopes_with_identical_ids_do_not_cross_pending_polls() {
        let barrier = Arc::new(Barrier::new(2));
        let mut tasks = Vec::new();
        for cap in [Some(0), Some(97)] {
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(scope_tool_output_cap(
                "shared-session",
                "shared-call",
                cap,
                async move {
                    let ctx = context(Some("shared-session"), "shared-call");
                    assert_eq!(observed_tool_output_cap(&ctx), cap);
                    barrier.wait().await;
                    for _ in 0..20 {
                        tokio::task::yield_now().await;
                        assert_eq!(observed_tool_output_cap(&ctx), cap);
                    }
                    cap
                },
            )));
        }
        assert_eq!(tasks.remove(0).await.unwrap(), Some(0));
        assert_eq!(tasks.remove(0).await.unwrap(), Some(97));
        assert_eq!(
            observed_tool_output_cap(&context(Some("shared-session"), "shared-call")),
            None
        );
    }

    #[tokio::test]
    async fn inline_scope_accepts_non_send_future_and_restores_on_timeout() {
        let local = Rc::new(23);
        let ctx = context(Some("session"), "call");
        let result = scope_tool_output_cap("session", "call", Some(23), async {
            let local = local.clone();
            tokio::task::yield_now().await;
            assert_eq!(observed_tool_output_cap(&ctx), Some(*local));
            *local
        })
        .await;
        assert_eq!(result, 23);
        assert_eq!(observed_tool_output_cap(&ctx), None);
        let timeout = tokio::time::timeout(
            Duration::from_millis(10),
            scope_tool_output_cap("session", "call", Some(23), async {
                assert_eq!(observed_tool_output_cap(&ctx), Some(23));
                std::future::pending::<()>().await;
            }),
        )
        .await;
        assert!(timeout.is_err());
        assert_eq!(observed_tool_output_cap(&ctx), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_and_panicking_scopes_leave_no_retained_observation() {
        let entered = Arc::new(Notify::new());
        let signal = entered.clone();
        let task = tokio::spawn(scope_tool_output_cap("s", "c", Some(19), async move {
            assert_eq!(observed_tool_output_cap(&context(Some("s"), "c")), Some(19));
            signal.notify_one();
            std::future::pending::<()>().await;
        }));
        entered.notified().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let panicking = tokio::spawn(scope_tool_output_cap("s", "c", Some(11), async {
            assert_eq!(observed_tool_output_cap(&context(Some("s"), "c")), Some(11));
            panic!("intentional scoped fixture unwind");
        }));
        assert!(panicking.await.unwrap_err().is_panic());
        assert_eq!(observed_tool_output_cap(&context(Some("s"), "c")), None);
        scope_tool_output_cap("s", "c", Some(3), async {
            assert_eq!(observed_tool_output_cap(&context(Some("s"), "c")), Some(3));
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn child_blocking_and_detached_tasks_do_not_inherit_parent_scope() {
        let release = Arc::new(Notify::new());
        let signal = release.clone();
        let (detached,) = scope_tool_output_cap("s", "c", Some(41), async {
            let child = tokio::spawn(async { observed_tool_output_cap(&context(Some("s"), "c")) });
            let blocking =
                tokio::task::spawn_blocking(|| observed_tool_output_cap(&context(Some("s"), "c")));
            assert_eq!(child.await.unwrap(), None);
            assert_eq!(blocking.await.unwrap(), None);
            assert_eq!(observed_tool_output_cap(&context(Some("s"), "c")), Some(41));
            (tokio::spawn(async move {
                signal.notified().await;
                observed_tool_output_cap(&context(Some("s"), "c"))
            }),)
        })
        .await;
        release.notify_one();
        assert_eq!(detached.await.unwrap(), None);
        assert_eq!(observed_tool_output_cap(&context(Some("s"), "c")), None);
    }

    #[tokio::test]
    async fn writer_scope_and_cap_scope_have_independent_keys_and_restore() {
        use crate::tools::context::{root_actor_tool_writer, with_root_actor_tool_writer};
        let ctx = context(Some("s"), "c");
        assert!(root_actor_tool_writer().is_none());
        scope_tool_output_cap("s", "c", Some(29), async {
            assert!(root_actor_tool_writer().is_none());
            with_root_actor_tool_writer(None, async {
                assert!(root_actor_tool_writer().is_some_and(|x| x.is_none()));
                assert_eq!(observed_tool_output_cap(&ctx), Some(29));
                tokio::task::yield_now().await;
            })
            .await;
            assert!(root_actor_tool_writer().is_none());
            assert_eq!(observed_tool_output_cap(&ctx), Some(29));
        })
        .await;
        with_root_actor_tool_writer(None, async {
            assert_eq!(observed_tool_output_cap(&ctx), None);
        })
        .await;
        assert!(root_actor_tool_writer().is_none());
    }

    #[tokio::test]
    async fn pending_inline_scopes_restore_between_each_poll_and_drop() {
        let ctx = context(Some("s"), "c");
        let entered = Arc::new(Notify::new());
        let released = Arc::new(Notify::new());
        let first = scope_tool_output_cap("s", "c", Some(31), async {
            assert_eq!(observed_tool_output_cap(&ctx), Some(31));
            entered.notify_one();
            released.notified().await;
            assert_eq!(observed_tool_output_cap(&ctx), Some(31));
        });
        let second = scope_tool_output_cap("s", "c", Some(47), async {
            entered.notified().await;
            assert_eq!(observed_tool_output_cap(&ctx), Some(47));
            released.notify_one();
            tokio::task::yield_now().await;
            assert_eq!(observed_tool_output_cap(&ctx), Some(47));
        });
        tokio::join!(first, second);
        assert_eq!(observed_tool_output_cap(&ctx), None);
        scope_tool_output_cap("s", "c", Some(59), async {
            let nested = tokio::time::timeout(
                Duration::from_millis(1),
                scope_tool_output_cap("s", "c", Some(61), async {
                    assert_eq!(observed_tool_output_cap(&ctx), Some(61));
                    std::future::pending::<()>().await;
                }),
            )
            .await;
            assert!(nested.is_err());
            assert_eq!(observed_tool_output_cap(&ctx), Some(59));
            use futures::FutureExt;
            let panicked =
                std::panic::AssertUnwindSafe(scope_tool_output_cap("s", "c", Some(63), async {
                    tokio::task::yield_now().await;
                    assert_eq!(observed_tool_output_cap(&ctx), Some(63));
                    panic!("intentional inline unwind");
                }))
                .catch_unwind()
                .await;
            assert!(panicked.is_err());
            assert_eq!(observed_tool_output_cap(&ctx), Some(59));
            let mut abandoned = Box::pin(scope_tool_output_cap("s", "c", Some(67), async {
                assert_eq!(observed_tool_output_cap(&ctx), Some(67));
                std::future::pending::<()>().await;
            }));
            tokio::select! {
                biased;
                _ = &mut abandoned => panic!("pending scope completed"),
                _ = std::future::ready(()) => {}
            }
            assert_eq!(observed_tool_output_cap(&ctx), Some(59));
            drop(abandoned);
            assert_eq!(observed_tool_output_cap(&ctx), Some(59));
        })
        .await;
        assert_eq!(observed_tool_output_cap(&ctx), None);
    }

    #[tokio::test]
    async fn empty_binding_cannot_make_a_missing_identity_match() {
        for (session, call) in [("", "c"), ("s", ""), ("", "")] {
            scope_tool_output_cap(session, call, Some(71), async {
                assert_eq!(
                    observed_tool_output_cap(&context(Some(session), call)),
                    None
                );
            })
            .await;
        }
    }
}
