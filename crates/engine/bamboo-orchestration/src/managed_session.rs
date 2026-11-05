//! Bounded waits on an existing, exact managed Session run.
use std::{future::Future, time::Duration};

const MAX_FINALIZATION_WAIT: Duration = Duration::from_secs(60);

/// The Host supplies a signal bound to one already observed run. Timing out
/// leaves its outcome unknown and never cancels the run.
pub async fn wait_for_finalization(signal: impl Future<Output = ()>) -> bool {
    tokio::time::timeout(MAX_FINALIZATION_WAIT, signal)
        .await
        .is_ok()
}

/// Await the first verified outcome across two exact runs. A candidate with
/// unavailable evidence does not win or cancel its sibling. Dropping the
/// wait drops only these observation futures.
pub async fn wait_for_outcome<T, E>(
    initial: impl Future<Output = Result<(Option<T>, Option<T>), E>>,
    first: impl Future<Output = Result<Option<T>, E>>,
    second: impl Future<Output = Result<Option<T>, E>>,
) -> Result<Option<T>, E> {
    tokio::time::timeout(MAX_FINALIZATION_WAIT, async {
        let (left, right) = initial.await?;
        if let Some(outcome) = left.or(right) {
            return Ok(Some(outcome));
        }
        tokio::pin!(first, second);
        tokio::select! {
            result = &mut first => match result? {
                Some(outcome) => Ok(Some(outcome)),
                None => second.await,
            },
            result = &mut second => match result? {
                Some(outcome) => Ok(Some(outcome)),
                None => first.await,
            },
        }
    })
    .await
    .unwrap_or(Ok(None))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn finalization_wakes_early_and_absent_signal_times_out() {
        let start = tokio::time::Instant::now();
        assert!(wait_for_finalization(tokio::time::sleep(Duration::from_secs(2))).await);
        assert_eq!(start.elapsed(), Duration::from_secs(2));

        let start = tokio::time::Instant::now();
        assert!(!wait_for_finalization(std::future::pending()).await);
        assert_eq!(start.elapsed(), MAX_FINALIZATION_WAIT);
    }

    #[tokio::test(start_paused = true)]
    async fn unavailable_candidate_does_not_end_the_other_wait() {
        let start = tokio::time::Instant::now();
        let outcome = wait_for_outcome(
            async { Ok((None, None)) },
            async { Ok::<_, ()>(None) },
            async {
                tokio::time::sleep(Duration::from_secs(2)).await;
                Ok(Some("second run"))
            },
        )
        .await;
        assert_eq!(outcome, Ok(Some("second run")));
        assert_eq!(start.elapsed(), Duration::from_secs(2));
        let no_evidence = wait_for_outcome(
            async { Ok((None, None)) },
            std::future::pending::<Result<Option<()>, ()>>(),
            async { Ok(None) },
        )
        .await;
        assert_eq!(no_evidence, Ok(None));
    }
}
