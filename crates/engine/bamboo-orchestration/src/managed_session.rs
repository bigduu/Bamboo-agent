//! Bounded waits on an existing, exact managed Session run.
use async_trait::async_trait;
use bamboo_domain::{ManagedRootEvent, ManagedRootEventRead, ManagedRootNotification, Session};
use std::{future::Future, time::Duration};

const MAX_FINALIZATION_WAIT: Duration = Duration::from_secs(60);
const FIRST_ACK_RECHECK: Duration = Duration::from_millis(250);
const MAX_ACK_RECHECK: Duration = Duration::from_secs(4);
const RECENT_NOTIFICATION_SOURCES: usize = 8;

/// Selectors only. The native Inbox writer proves each original source again.
pub fn notification_candidates(
    session: &Session,
) -> Result<Vec<ManagedRootNotification>, &'static str> {
    let Some(metadata) = session.runtime_metadata.as_ref() else {
        return Ok(Vec::new());
    };
    let Some(outcomes) = metadata.supervised_root_outcomes.as_ref() else {
        return Ok(Vec::new());
    };
    outcomes.validate()?;
    let mut candidates = Vec::new();
    for fact in &outcomes.facts {
        let has_material = fact.result.is_some()
            || metadata
                .managed_task_blocker
                .as_ref()
                .is_some_and(|blocker| blocker.run_id == fact.run_id);
        if !has_material {
            continue;
        }
        for input in &fact.inputs {
            let Some(origin) = input
                .supervisor_origin
                .as_ref()
                .filter(|origin| origin.material_notifications_requested())
            else {
                continue;
            };
            let (Some(caller), Some(target)) =
                (origin.original_caller(), origin.current_user_target())
            else {
                continue;
            };
            candidates.push(ManagedRootNotification {
                caller: caller.clone(),
                target: target.clone(),
                run_id: fact.run_id.clone(),
                after_sequence: fact.sequence - 1,
                input_receipt_id: input.receipt_id.clone(),
            });
        }
    }
    Ok(candidates)
}

#[async_trait]
pub trait ManagedRootNotificationIo: Send + Sync {
    /// Derived source hints, ordered newest first. Delivery proves authority.
    async fn sources(&self) -> Vec<String>;
    async fn candidates(&self, source: &str) -> Result<Vec<ManagedRootNotification>, String>;
    async fn deliver(&self, selected: &ManagedRootNotification) -> Result<(), String>;
}

/// Pacing is process-local. Original input facts and Inbox receipts own truth.
#[derive(Default)]
pub struct ManagedRootNotifications {
    next: tokio::sync::Mutex<usize>,
}

impl ManagedRootNotifications {
    /// Prioritize recent changes and revisit one older source each pass.
    pub async fn tick(&self, io: &impl ManagedRootNotificationIo) -> Result<(), String> {
        let Ok(mut next) = self.next.try_lock() else {
            return Ok(());
        };
        let sources = io.sources().await;
        if sources.is_empty() {
            return Ok(());
        }
        let position = *next;
        *next = next.wrapping_add(1);
        let mut selected: Vec<_> = sources
            .iter()
            .take(RECENT_NOTIFICATION_SOURCES)
            .map(|source| (source, position))
            .collect();
        let older = &sources[position % sources.len()];
        if !selected.iter().any(|(source, _)| *source == older) {
            selected.push((older, position / sources.len()));
        }
        let mut first_error = None;
        for (source, cursor) in selected {
            let result = async {
                let candidates = io.candidates(source).await?;
                if !candidates.is_empty() {
                    io.deliver(&candidates[cursor % candidates.len()]).await?;
                }
                Ok::<_, String>(())
            }
            .await;
            if let Err(error) = result {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

/// The Host supplies a signal bound to one already observed run. Timing out
/// leaves its outcome unknown and never cancels the run.
pub async fn wait_for_finalization(signal: impl Future<Output = ()>) -> bool {
    tokio::time::timeout(MAX_FINALIZATION_WAIT, signal)
        .await
        .is_ok()
}

/// A process-local prompt to reread durable evidence.
pub enum ManagedRunHint {
    TaskChanged,
    Finalized,
}

#[async_trait]
pub trait ManagedRunSignals: Send {
    async fn next_hint(&mut self) -> ManagedRunHint;
}

/// One candidate for the bounded two-run wait. While the run has no recorded
/// event, wait for a Host hint. A committed event with a missing ACK
/// can become verifiable independently, so recheck only that pending proof.
pub async fn read_when_verified<E, F, Read>(
    mut read: F,
    mut signals: Option<impl ManagedRunSignals>,
) -> Result<Option<ManagedRootEvent>, E>
where
    F: FnMut() -> Read + Send,
    Read: Future<Output = Result<ManagedRootEventRead, E>> + Send,
{
    let mut ack_delay = FIRST_ACK_RECHECK;
    let mut signal_closed = false;
    loop {
        match read().await? {
            ManagedRootEventRead::Next(outcome) => return Ok(Some(outcome)),
            ManagedRootEventRead::PendingSource => {
                tokio::time::sleep(ack_delay).await;
                ack_delay = (ack_delay * 2).min(MAX_ACK_RECHECK);
            }
            ManagedRootEventRead::CaughtUp => {
                if signal_closed {
                    return Ok(None);
                }
                let Some(signals) = signals.as_mut() else {
                    return Ok(None);
                };
                signal_closed = matches!(signals.next_hint().await, ManagedRunHint::Finalized);
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

    struct SourceReads(std::sync::Mutex<Vec<String>>);

    #[async_trait]
    impl ManagedRootNotificationIo for SourceReads {
        async fn sources(&self) -> Vec<String> {
            (0..12).map(|index| format!("source-{index}")).collect()
        }

        async fn candidates(&self, source: &str) -> Result<Vec<ManagedRootNotification>, String> {
            self.0.lock().unwrap().push(source.into());
            if source == "source-0" {
                return Err("unavailable source".into());
            }
            Ok(Vec::new())
        }

        async fn deliver(&self, _: &ManagedRootNotification) -> Result<(), String> {
            unreachable!("empty candidate sets cannot deliver")
        }
    }

    #[tokio::test]
    async fn recent_notification_errors_keep_older_sources_in_the_reconciliation_rotation() {
        let reads = SourceReads(std::sync::Mutex::new(Vec::new()));
        let notifications = ManagedRootNotifications::default();
        for _ in 0..24 {
            assert_eq!(
                notifications.tick(&reads).await,
                Err("unavailable source".into())
            );
        }
        let reads = reads.0.lock().unwrap();
        for index in 0..12 {
            let minimum = if index < RECENT_NOTIFICATION_SOURCES {
                24
            } else {
                2
            };
            assert!(
                reads
                    .iter()
                    .filter(|source| **source == format!("source-{index}"))
                    .count()
                    >= minimum
            );
        }
    }

    struct Finalization;

    #[async_trait]
    impl ManagedRunSignals for Finalization {
        async fn next_hint(&mut self) -> ManagedRunHint {
            ManagedRunHint::Finalized
        }
    }

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
        let expected = ManagedRootEvent::Outcome(bamboo_domain::ObserverSupervisedRootOutcome {
            target_session_id: "root".into(),
            target_created_at: "2026-01-01T00:00:00Z".parse().unwrap(),
            sequence: 1,
            run_id: "exact-run".into(),
            status: bamboo_domain::SupervisedRootOutcomeResult::Completed,
            inputs: vec![],
        });
        let start = tokio::time::Instant::now();
        let mut reads = 0;
        let selected = read_when_verified(
            || {
                reads += 1;
                let state = if reads == 1 {
                    ManagedRootEventRead::PendingSource
                } else {
                    ManagedRootEventRead::Next(expected.clone())
                };
                std::future::ready(Ok::<_, ()>(state))
            },
            None::<Finalization>,
        )
        .await;
        assert_eq!(selected, Ok(Some(expected)));
        assert_eq!(reads, 2);
        assert_eq!(start.elapsed(), FIRST_ACK_RECHECK);
    }
}
