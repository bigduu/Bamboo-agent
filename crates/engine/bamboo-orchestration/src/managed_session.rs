//! Bounded waits on an existing, exact managed Session run.
use bamboo_domain::{ObserverRootOutcomeRead, ObserverSupervisedRootOutcome};
use std::{future::Future, time::Duration};

const MAX_FINALIZATION_WAIT: Duration = Duration::from_secs(60);
const FIRST_ACK_RECHECK: Duration = Duration::from_millis(250);
const MAX_ACK_RECHECK: Duration = Duration::from_secs(4);

/// The Host supplies a signal bound to one already observed run. Timing out
/// leaves its outcome unknown and never cancels the run.
pub async fn wait_for_finalization(signal: impl Future<Output = ()>) -> bool {
    tokio::time::timeout(MAX_FINALIZATION_WAIT, signal)
        .await
        .is_ok()
}

/// One candidate for the bounded two-run wait. While the run has no recorded
/// outcome, wait for the Host signal. A committed outcome with a missing ACK
/// can become verifiable independently, so recheck only that pending proof.
pub async fn read_when_verified<E, F, Read>(
    mut read: F,
    mut signal: Option<impl Future<Output = ()>>,
) -> Result<Option<ObserverSupervisedRootOutcome>, E>
where
    F: FnMut() -> Read,
    Read: Future<Output = Result<ObserverRootOutcomeRead, E>>,
{
    let mut ack_delay = FIRST_ACK_RECHECK;
    loop {
        match read().await? {
            ObserverRootOutcomeRead::Next(outcome) => return Ok(Some(outcome)),
            ObserverRootOutcomeRead::PendingSource => {
                tokio::time::sleep(ack_delay).await;
                ack_delay = (ack_delay * 2).min(MAX_ACK_RECHECK);
            }
            ObserverRootOutcomeRead::CaughtUp | ObserverRootOutcomeRead::LegacySnapshot => {
                let Some(signal) = signal.take() else {
                    return Ok(None);
                };
                signal.await;
            }
        }
    }
}

/// Await the first verified outcome across two exact runs. A candidate with
/// unavailable evidence does not win or cancel its sibling. Dropping the
/// wait drops only these observation futures.
pub async fn wait_for_outcome<T, E>(
    first: impl Future<Output = Result<Option<T>, E>>,
    second: impl Future<Output = Result<Option<T>, E>>,
) -> Result<Option<T>, E> {
    tokio::time::timeout(MAX_FINALIZATION_WAIT, async {
        tokio::pin!(first, second);
        tokio::select! {
            result = &mut first => prefer_verified_outcome(result, second).await,
            result = &mut second => prefer_verified_outcome(result, first).await,
        }
    })
    .await
    .unwrap_or(Ok(None))
}

async fn prefer_verified_outcome<T, E>(
    result: Result<Option<T>, E>,
    sibling: impl Future<Output = Result<Option<T>, E>>,
) -> Result<Option<T>, E> {
    match result {
        Ok(Some(outcome)) => Ok(Some(outcome)),
        Ok(None) => sibling.await,
        Err(error) => match sibling.await {
            Ok(Some(outcome)) => Ok(Some(outcome)),
            _ => Err(error),
        },
    }
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
        let outcome = wait_for_outcome(async { Ok::<_, ()>(None) }, async {
            tokio::time::sleep(Duration::from_secs(2)).await;
            Ok(Some("second run"))
        })
        .await;
        assert_eq!(outcome, Ok(Some("second run")));
        assert_eq!(start.elapsed(), Duration::from_secs(2));
        let no_evidence =
            wait_for_outcome(std::future::pending::<Result<Option<()>, ()>>(), async {
                Ok(None)
            })
            .await;
        assert_eq!(no_evidence, Ok(None));
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_candidate_cannot_hide_a_verified_sibling() {
        let observed = wait_for_outcome(async { Err("stale target") }, async {
            tokio::time::sleep(Duration::from_secs(2)).await;
            Ok(Some("verified outcome"))
        })
        .await;
        assert_eq!(observed, Ok(Some("verified outcome")));
        let denied = wait_for_outcome::<(), _>(async { Err("caller invalid") }, async {
            Err("caller invalid")
        })
        .await;
        assert_eq!(denied, Err("caller invalid"));
    }

    #[tokio::test(start_paused = true)]
    async fn a_pending_ack_is_rechecked_without_another_run_signal() {
        let expected = ObserverSupervisedRootOutcome {
            target_session_id: "root".into(),
            target_created_at: "2026-01-01T00:00:00Z".parse().unwrap(),
            sequence: 1,
            run_id: "exact-run".into(),
            status: bamboo_domain::SupervisedRootOutcomeResult::Completed,
            inputs: vec![],
        };
        let start = tokio::time::Instant::now();
        let mut reads = 0;
        let selected = read_when_verified(
            || {
                reads += 1;
                let state = if reads == 1 {
                    ObserverRootOutcomeRead::PendingSource
                } else {
                    ObserverRootOutcomeRead::Next(expected.clone())
                };
                std::future::ready(Ok::<_, ()>(state))
            },
            None::<std::future::Pending<()>>,
        )
        .await;
        assert_eq!(selected, Ok(Some(expected)));
        assert_eq!(reads, 2);
        assert_eq!(start.elapsed(), FIRST_ACK_RECHECK);
    }
}
