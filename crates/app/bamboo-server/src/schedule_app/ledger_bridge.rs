//! Ledger → schedule bridge.
//!
//! Keeps real [`ScheduleStore`] entries in step with a ledger record's
//! `remind_at` / `recurrence` times, implementing the
//! [`LedgerScheduleBridge`] port the `ledger` tool talks to. Managed
//! schedules are tagged by name (`ledger:<record_id>:…`) and their ids live on
//! the record's `schedule_ids`; the invariant is that a terminal record owns
//! no schedules.
//!
//! The `ledger` tool sits in the base tool layer, which is assembled before
//! the schedule store exists, so the wiring goes through
//! [`LateBoundLedgerBridge`]: a handle installed at tool-build time and bound
//! to the real bridge once the scheduler is up.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::RwLock;

use bamboo_domain::ledger::LedgerRecord;
use bamboo_domain::schedule::{ScheduleRunConfig, ScheduleTrigger};
use bamboo_server_tools::LedgerScheduleBridge;

use super::store::ScheduleStore;

pub struct ScheduleLedgerBridge {
    store: Arc<ScheduleStore>,
}

impl ScheduleLedgerBridge {
    pub fn new(store: Arc<ScheduleStore>) -> Self {
        Self { store }
    }

    fn reminder_run_config(record: &LedgerRecord) -> ScheduleRunConfig {
        ScheduleRunConfig {
            task_message: Some(format!(
                "Reminder fired for ledger record `{id}`: {title}.\n\
                 Load it with the `ledger` tool (action=get, id={id}) and check its \
                 current status. If it is already done or cancelled, stop. Otherwise \
                 do whatever preparation is genuinely useful, then alert the user \
                 with a concise notify message that names the record and when it is due.",
                id = record.id,
                title = record.title,
            )),
            auto_execute: true,
            ..ScheduleRunConfig::default()
        }
    }
}

#[async_trait]
impl LedgerScheduleBridge for ScheduleLedgerBridge {
    async fn sync_record_schedules(&self, record: &LedgerRecord) -> Result<Vec<String>, String> {
        // Reconcile by replacement: drop the old managed set, create the new
        // one. Reminder counts are tiny and this stays deterministic across
        // partial edits (changed times, removed reminders, added recurrence).
        self.release_schedules(&record.schedule_ids).await?;

        let now = Utc::now();
        let mut created = Vec::new();
        let mut triggers: Vec<(String, ScheduleTrigger)> = Vec::new();
        for (index, remind_at) in record.time.remind_at.iter().enumerate() {
            // A past reminder can never fire; creating it would be rejected.
            if *remind_at <= now {
                continue;
            }
            triggers.push((
                format!("ledger:{}:remind:{}", record.id, index),
                ScheduleTrigger::Once { at: *remind_at },
            ));
        }
        if let Some(recurrence) = &record.time.recurrence {
            triggers.push((
                format!("ledger:{}:recurrence", record.id),
                recurrence.clone(),
            ));
        }

        for (name, trigger) in triggers {
            let entry = self
                .store
                .create_schedule(name, trigger, true, Self::reminder_run_config(record))
                .await
                .map_err(|error| format!("failed to create reminder schedule: {error}"))?;
            created.push(entry.id);
        }
        Ok(created)
    }

    async fn release_schedules(&self, schedule_ids: &[String]) -> Result<(), String> {
        for schedule_id in schedule_ids {
            // Already-gone schedules (fired Once entries prune, manual deletes)
            // are fine — the goal is absence.
            self.store
                .delete_schedule(schedule_id)
                .await
                .map_err(|error| format!("failed to delete schedule {schedule_id}: {error}"))?;
        }
        Ok(())
    }
}

/// A bridge handle that can be installed before the scheduler exists and bound
/// afterwards. Until bound, mutations that need scheduling degrade to a
/// warning on the tool result (the record itself still persists).
#[derive(Default)]
pub struct LateBoundLedgerBridge {
    inner: RwLock<Option<Arc<dyn LedgerScheduleBridge>>>,
}

impl LateBoundLedgerBridge {
    pub async fn bind(&self, bridge: Arc<dyn LedgerScheduleBridge>) {
        *self.inner.write().await = Some(bridge);
    }
}

#[async_trait]
impl LedgerScheduleBridge for LateBoundLedgerBridge {
    async fn sync_record_schedules(&self, record: &LedgerRecord) -> Result<Vec<String>, String> {
        match self.inner.read().await.as_ref() {
            Some(bridge) => bridge.sync_record_schedules(record).await,
            None => Err("the scheduler is not initialized yet".to_string()),
        }
    }

    async fn release_schedules(&self, schedule_ids: &[String]) -> Result<(), String> {
        match self.inner.read().await.as_ref() {
            Some(bridge) => bridge.release_schedules(schedule_ids).await,
            None => Err("the scheduler is not initialized yet".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::ledger::{RecordKind, RecordStatus};
    use chrono::Duration;

    async fn test_store() -> (Arc<ScheduleStore>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(ScheduleStore::new(dir.path().to_path_buf()).await.unwrap());
        (store, dir)
    }

    #[tokio::test]
    async fn sync_creates_once_schedules_for_future_reminders_and_skips_past() {
        let (store, _dir) = test_store().await;
        let bridge = ScheduleLedgerBridge::new(store.clone());

        let mut record = LedgerRecord::new("rec_1", RecordKind::Reminder, "Take medication");
        record.time.remind_at = vec![
            Utc::now() - Duration::hours(1), // past: skipped
            Utc::now() + Duration::hours(3),
        ];
        let ids = bridge.sync_record_schedules(&record).await.unwrap();
        assert_eq!(ids.len(), 1);

        let entry = store.get_schedule(&ids[0]).await.unwrap();
        assert!(entry.name.starts_with("ledger:rec_1:remind:"));
        assert!(matches!(entry.trigger, ScheduleTrigger::Once { .. }));
        assert!(entry.run_config.auto_execute);
        assert!(entry
            .run_config
            .task_message
            .as_deref()
            .unwrap()
            .contains("rec_1"));
    }

    #[tokio::test]
    async fn sync_replaces_previous_managed_schedules() {
        let (store, _dir) = test_store().await;
        let bridge = ScheduleLedgerBridge::new(store.clone());

        let mut record = LedgerRecord::new("rec_1", RecordKind::Reminder, "Standup");
        record.time.remind_at = vec![Utc::now() + Duration::hours(1)];
        let first = bridge.sync_record_schedules(&record).await.unwrap();
        record.schedule_ids = first.clone();

        // The reminder moves; the old schedule must not survive.
        record.time.remind_at = vec![Utc::now() + Duration::hours(2)];
        let second = bridge.sync_record_schedules(&record).await.unwrap();
        assert_eq!(second.len(), 1);
        assert!(store.get_schedule(&first[0]).await.is_none());
        assert!(store.get_schedule(&second[0]).await.is_some());
    }

    #[tokio::test]
    async fn recurrence_maps_to_a_recurring_schedule_and_release_deletes() {
        let (store, _dir) = test_store().await;
        let bridge = ScheduleLedgerBridge::new(store.clone());

        let mut record = LedgerRecord::new("rec_habit", RecordKind::Habit, "Daily review");
        record.time.recurrence = Some(ScheduleTrigger::Daily {
            hour: 9,
            minute: 0,
            second: 0,
        });
        let ids = bridge.sync_record_schedules(&record).await.unwrap();
        assert_eq!(ids.len(), 1);
        assert!(matches!(
            store.get_schedule(&ids[0]).await.unwrap().trigger,
            ScheduleTrigger::Daily { .. }
        ));

        record.transition_to(RecordStatus::Done, None);
        bridge.release_schedules(&ids).await.unwrap();
        assert!(store.get_schedule(&ids[0]).await.is_none());
        // Releasing again is a no-op, not an error.
        bridge.release_schedules(&ids).await.unwrap();
    }

    #[tokio::test]
    async fn late_bound_bridge_errors_until_bound_then_delegates() {
        let (store, _dir) = test_store().await;
        let handle = LateBoundLedgerBridge::default();

        let mut record = LedgerRecord::new("rec_1", RecordKind::Reminder, "Ping");
        record.time.remind_at = vec![Utc::now() + Duration::hours(1)];
        assert!(handle.sync_record_schedules(&record).await.is_err());

        handle
            .bind(Arc::new(ScheduleLedgerBridge::new(store)))
            .await;
        assert_eq!(
            handle.sync_record_schedules(&record).await.unwrap().len(),
            1
        );
    }
}
