use std::cell::Cell;
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::rc::Rc;
use std::time::Duration;

thread_local! {
    static TIMEOUT_DROP: Cell<bool> = const { Cell::new(false) };
}

pub(super) fn is_timeout_drop() -> bool {
    TIMEOUT_DROP.with(Cell::get)
}

// This guard exists only during synchronous destruction, never across a poll or
// await. Keep it thread-bound and restore an outer scope even during unwinding.
struct TimeoutDropGuard {
    previous: bool,
    _thread_bound: PhantomData<Rc<()>>,
}

impl TimeoutDropGuard {
    fn new() -> Self {
        Self {
            previous: TIMEOUT_DROP.with(|reason| reason.replace(true)),
            _thread_bound: PhantomData,
        }
    }
}

impl Drop for TimeoutDropGuard {
    fn drop(&mut self) {
        TIMEOUT_DROP.with(|reason| reason.set(self.previous));
    }
}

/// A Tokio deadline that preserves the cause when unfinished observations end.
/// Ordinary cancellation still reports cancellation. Only a deadline that has
/// actually elapsed marks observations destroyed with its owned future as timed
/// out; completed siblings and unrelated tasks keep their own outcomes.
pub fn with_timeout<F: IntoFuture>(
    duration: Duration,
    future: F,
) -> impl Future<Output = Result<F::Output, tokio::time::error::Elapsed>> {
    // Construct the deadline now, exactly as Tokio does, rather than on first
    // poll. Own the pinned future: dropping a Pin<&mut _> would not destroy it.
    let mut timed = Box::pin(tokio::time::timeout(duration, future));
    async move {
        let result = timed.as_mut().await;
        if result.is_err() {
            let _guard = TimeoutDropGuard::new();
            drop(timed);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::{test_support::TraceTest, Observation, Outcome};
    use opentelemetry::trace::Status;

    #[tokio::test]
    async fn actual_and_nested_timeout_drop_spans_with_timeout_and_restore_cancellation() {
        for nested in [false, true] {
            let trace = TraceTest::default();
            trace
                .scope(async {
                    let result = with_timeout(Duration::ZERO, async {
                        let dispatch = async {
                            let _span = Observation::tool("Read", "pending", "s", "r");
                            std::future::pending::<()>().await;
                        };
                        if nested {
                            with_timeout(Duration::from_secs(60), dispatch)
                                .await
                                .unwrap();
                        } else {
                            dispatch.await;
                        }
                    })
                    .await;
                    assert!(result.is_err());
                    // The timed future must be destroyed before Err is returned.
                    assert_eq!(trace.spans().len(), 1);
                    drop(Observation::tool("Read", "later-cancel", "s", "r"));
                })
                .await;
            let spans = trace.spans();
            assert_eq!(spans.len(), 2);
            assert_eq!(spans[0].status, Status::error("timeout"));
            assert!(spans[0].attributes.iter().any(|kv| {
                kv.key.as_str() == "bamboo.outcome" && kv.value.to_string() == "error"
            }));
            assert_eq!(spans[1].status, Status::error("cancelled"));
        }
    }

    #[tokio::test]
    async fn panic_takes_precedence_and_unwinding_restores_the_drop_reason() {
        let trace = TraceTest::default();
        trace
            .scope(async {
                let result = std::panic::catch_unwind(|| {
                    let _guard = TimeoutDropGuard::new();
                    let _span = Observation::tool("Read", "panic", "s", "r");
                    panic!("private panic payload");
                });
                assert!(result.is_err());
                assert!(!is_timeout_drop());
                drop(Observation::tool("Read", "cancelled", "s", "r"));
            })
            .await;
        let spans = trace.spans();
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0].status, Status::error("panic"));
        assert_eq!(spans[1].status, Status::error("cancelled"));
        assert!(!format!("{spans:?}").contains("private panic payload"));
    }

    #[tokio::test]
    async fn early_drop_stays_cancelled_and_ready_values_keep_tokio_semantics() {
        fn assert_send<T: Send>(value: T) -> T {
            value
        }
        assert_eq!(
            assert_send(with_timeout(Duration::ZERO, async { 7 }))
                .await
                .unwrap(),
            7
        );
        let trace = TraceTest::default();
        trace
            .scope(async {
                let mut pending = Box::pin(with_timeout(Duration::from_secs(60), async {
                    let _span = Observation::tool("Read", "cancelled", "s", "r");
                    std::future::pending::<()>().await;
                }));
                std::future::poll_fn(|cx| {
                    assert!(pending.as_mut().poll(cx).is_pending());
                    std::task::Poll::Ready(())
                })
                .await;
                drop(pending);
                assert_eq!(
                    with_timeout(Duration::ZERO, async { 42 }).await.unwrap(),
                    42
                );
                Observation::tool("Read", "success", "s", "r").finish(Outcome::Success);
            })
            .await;
        let spans = trace.spans();
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0].status, Status::error("cancelled"));
        assert_eq!(spans[1].status, Status::Unset);
    }
}
