//! Background ledger gardener: maintenance for prospective-memory records.
//!
//! Three ordered passes, mirroring the memory gardener's cost model (each pass
//! is config-gated, capped, and free when its deterministic prefilter finds
//! nothing):
//!
//! 1. **Expiry** (deterministic, zero LLM): past events and fully-fired
//!    reminders transition to `Expired` and drop out of the agenda. Todos
//!    never auto-expire — an overdue todo is still open work.
//! 2. **Schedule reconciliation** (deterministic, zero LLM): repairs
//!    record↔schedule drift through the [`LedgerScheduleBridge`] — terminal
//!    records release leftover schedules; open records with reminder times but
//!    no managed schedules (a crash between writes) get them re-synced.
//! 3. **Distillation** (background model): completed records become durable
//!    memories — the ledger feeding the long-term memory system once
//!    prospective records resolve into retrospective knowledge.

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde::Deserialize;

use bamboo_domain::ledger::{LedgerRecord, LedgerScope, RecordStatus};
use bamboo_memory::ledger_store::{LedgerScheduleBridge, LedgerStore, RecordFilter};
use bamboo_memory::memory_store::{DurableMemoryType, MemoryStore};

use crate::auto_dream::AutoDreamContext;
use crate::gardener::{collect_model_json, resolve_background_model};

const LEDGER_GARDENER_TRACING_TARGET: &str = "bamboo.ledger_gardener";
/// Grace period before a past event/reminder is auto-expired, so "the meeting
/// just ended" records survive long enough for a follow-up conversation.
const EXPIRY_GRACE_HOURS: i64 = 48;
/// Max completed records fed to one distillation call.
const DISTILL_MAX_RECORDS_PER_RUN: usize = 8;
/// Tag marking a record as already distilled (or considered and skipped).
const DISTILLED_TAG: &str = "distilled";

const DISTILL_SYSTEM_INSTRUCTION: &str = "You are Bamboo's background ledger gardener. From the user's completed ledger records, extract only durable, long-term facts worth remembering (habits, recurring obligations, stable preferences, notable life events). Return only the specified JSON array. No prose, no markdown fences.";

pub struct LedgerGardenerContext {
    pub dream: AutoDreamContext,
    pub schedule_bridge: Option<Arc<dyn LedgerScheduleBridge>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LedgerGardenerRunResult {
    pub scanned: usize,
    pub expired: usize,
    pub schedules_released: usize,
    pub schedules_synced: usize,
    pub distilled_records: usize,
    pub memories_written: usize,
    pub failed: usize,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct DistilledMemoryCandidate {
    pub title: String,
    pub content: String,
    #[serde(default)]
    pub r#type: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
}

/// Whether the record should transition to `Expired` at `now`. Pure so the
/// policy is unit-testable: only records whose *entire* time purpose is in the
/// past expire — events past their end, and reminder-only records whose every
/// reminder has fired. Anything with a due date, a recurrence, or no time at
/// all stays open.
pub fn should_expire(record: &LedgerRecord, now: DateTime<Utc>) -> bool {
    if record.status.is_terminal() {
        return false;
    }
    if record.time.due_at.is_some() || record.time.recurrence.is_some() {
        return false;
    }
    let cutoff = now - chrono::Duration::hours(EXPIRY_GRACE_HOURS);
    if let Some(end) = record.time.ends_at.or(record.time.starts_at) {
        return end < cutoff;
    }
    if !record.time.remind_at.is_empty() {
        return record.time.remind_at.iter().all(|at| *at < cutoff);
    }
    false
}

/// Lenient parse of the distillation model output: the first `[`…`]` slice is
/// parsed as the candidate array; anything unparseable yields an empty list
/// (a malformed model reply must never fail the run).
pub fn parse_distilled_candidates(raw: &str) -> Vec<DistilledMemoryCandidate> {
    let Some(start) = raw.find('[') else {
        return Vec::new();
    };
    let Some(end) = raw.rfind(']') else {
        return Vec::new();
    };
    if end < start {
        return Vec::new();
    }
    serde_json::from_str::<Vec<DistilledMemoryCandidate>>(&raw[start..=end])
        .unwrap_or_default()
        .into_iter()
        .filter(|candidate| {
            !candidate.title.trim().is_empty() && !candidate.content.trim().is_empty()
        })
        .collect()
}

fn build_distillation_prompt(docs: &[(String, String)]) -> String {
    let mut prompt = String::from(
        "The user completed the following ledger records (todos/events/reminders).\n\
         Extract durable long-term memories ONLY where a record reveals a lasting fact:\n\
         a habit or routine, a recurring obligation, a stable preference, or a notable\n\
         life event worth recalling months from now. Routine one-off chores yield nothing.\n\n\
         Records:\n",
    );
    for (title, detail) in docs {
        prompt.push_str(&format!("- {title}\n  {detail}\n"));
    }
    prompt.push_str(
        "\nReturn a JSON array (possibly empty), each item:\n\
         {\"title\": \"specific descriptive title\", \"content\": \"one atomic fact\", \
         \"type\": \"user|reference\", \"tags\": [\"...\"]}\n",
    );
    prompt
}

fn all_scopes(project_keys: &[String]) -> Vec<(LedgerScope, Option<String>)> {
    let mut scopes: Vec<(LedgerScope, Option<String>)> = vec![(LedgerScope::Global, None)];
    scopes.extend(
        project_keys
            .iter()
            .map(|key| (LedgerScope::Project, Some(key.clone()))),
    );
    scopes
}

pub async fn run_ledger_gardener_once(
    ctx: &LedgerGardenerContext,
) -> Result<Option<LedgerGardenerRunResult>, String> {
    let ledger = LedgerStore::new(ctx.dream.session_store.bamboo_home_dir());
    let memory = ctx.dream.memory.clone();
    run_ledger_gardener_once_with_stores(ctx, &ledger, &memory).await
}

async fn run_ledger_gardener_once_with_stores(
    ctx: &LedgerGardenerContext,
    ledger: &LedgerStore,
    memory: &MemoryStore,
) -> Result<Option<LedgerGardenerRunResult>, String> {
    let config_snapshot = ctx.dream.config.read().await.clone();
    let memory_cfg = config_snapshot.memory().clone().unwrap_or_default();
    if !memory_cfg.ledger_gardener_enabled {
        return Ok(None);
    }

    let now = Utc::now();
    let mut result = LedgerGardenerRunResult::default();
    let project_keys = ledger
        .list_project_keys()
        .await
        .map_err(|error| format!("ledger gardener scope scan failed: {error}"))?;
    let scopes = all_scopes(&project_keys);

    // Pass 1+2 share one record sweep per scope.
    for (scope, project_key) in &scopes {
        let docs = ledger
            .list_records(
                *scope,
                project_key.as_deref(),
                &RecordFilter {
                    include_terminal: true,
                    ..RecordFilter::default()
                },
            )
            .await
            .map_err(|error| format!("ledger gardener list failed: {error}"))?;
        result.scanned += docs.len();

        for doc in docs {
            let record = &doc.record;

            // Pass 1: expiry.
            if should_expire(record, now) {
                match ledger
                    .transition_record(
                        *scope,
                        project_key.as_deref(),
                        &record.id,
                        RecordStatus::Expired,
                        Some("auto-expired by the ledger gardener"),
                    )
                    .await
                {
                    Ok(Some(expired_doc)) => {
                        result.expired += 1;
                        // Release the expired record's schedules right away
                        // instead of waiting a full interval for the terminal
                        // branch below. Rewrite from the freshly transitioned
                        // document so the recorded transition history survives.
                        if let (Some(bridge), false) =
                            (&ctx.schedule_bridge, record.schedule_ids.is_empty())
                        {
                            if bridge.release_schedules(&record.schedule_ids).await.is_ok() {
                                let mut cleared = expired_doc.record.clone();
                                cleared.schedule_ids.clear();
                                let _ = ledger
                                    .write_record(cleared, Some(expired_doc.body.clone()))
                                    .await;
                                result.schedules_released += record.schedule_ids.len();
                            }
                        }
                        continue;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        result.failed += 1;
                        tracing::warn!(
                            target: LEDGER_GARDENER_TRACING_TARGET,
                            record_id = %record.id,
                            "[ledger-gardener] expiry transition failed: {error}"
                        );
                        continue;
                    }
                }
            }

            // Pass 2: schedule reconciliation.
            let Some(bridge) = &ctx.schedule_bridge else {
                continue;
            };
            if record.status.is_terminal() && !record.schedule_ids.is_empty() {
                match bridge.release_schedules(&record.schedule_ids).await {
                    Ok(()) => {
                        let mut cleared = record.clone();
                        cleared.schedule_ids.clear();
                        if ledger
                            .write_record(cleared, Some(doc.body.clone()))
                            .await
                            .is_ok()
                        {
                            result.schedules_released += record.schedule_ids.len();
                        }
                    }
                    Err(error) => {
                        result.failed += 1;
                        tracing::warn!(
                            target: LEDGER_GARDENER_TRACING_TARGET,
                            record_id = %record.id,
                            "[ledger-gardener] schedule release failed: {error}"
                        );
                    }
                }
            } else if !record.status.is_terminal()
                && record.schedule_ids.is_empty()
                && (record.time.recurrence.is_some()
                    || record.time.remind_at.iter().any(|at| *at > now))
            {
                match bridge.sync_record_schedules(record).await {
                    Ok(ids) if !ids.is_empty() => {
                        let mut updated = record.clone();
                        updated.schedule_ids = ids.clone();
                        if ledger
                            .write_record(updated, Some(doc.body.clone()))
                            .await
                            .is_ok()
                        {
                            result.schedules_synced += ids.len();
                        }
                    }
                    Ok(_) => {}
                    Err(error) => {
                        result.failed += 1;
                        tracing::warn!(
                            target: LEDGER_GARDENER_TRACING_TARGET,
                            record_id = %record.id,
                            "[ledger-gardener] schedule sync failed: {error}"
                        );
                    }
                }
            }
        }
    }

    // Pass 3: distillation (the only LLM arm; free when nothing completed).
    if memory_cfg.ledger_distillation_enabled {
        if let Err(error) = run_distillation_pass(ctx, ledger, memory, &scopes, &mut result).await {
            result.failed += 1;
            tracing::warn!(
                target: LEDGER_GARDENER_TRACING_TARGET,
                "[ledger-gardener] distillation failed: {error}"
            );
        }
    }

    tracing::info!(
        target: LEDGER_GARDENER_TRACING_TARGET,
        event = "run_complete",
        scanned = result.scanned,
        expired = result.expired,
        schedules_released = result.schedules_released,
        schedules_synced = result.schedules_synced,
        distilled_records = result.distilled_records,
        memories_written = result.memories_written,
        failed = result.failed,
        "[ledger-gardener] run complete"
    );
    Ok(Some(result))
}

async fn run_distillation_pass(
    ctx: &LedgerGardenerContext,
    ledger: &LedgerStore,
    memory: &MemoryStore,
    scopes: &[(LedgerScope, Option<String>)],
    result: &mut LedgerGardenerRunResult,
) -> Result<(), String> {
    // Deterministic prefilter: Done records not yet distilled.
    let mut pending = Vec::new();
    for (scope, project_key) in scopes {
        let docs = ledger
            .list_records(
                *scope,
                project_key.as_deref(),
                &RecordFilter {
                    statuses: Some([RecordStatus::Done].into_iter().collect()),
                    include_terminal: true,
                    ..RecordFilter::default()
                },
            )
            .await
            .map_err(|error| format!("distillation list failed: {error}"))?;
        pending.extend(
            docs.into_iter()
                .filter(|doc| !doc.record.tags.iter().any(|tag| tag == DISTILLED_TAG)),
        );
        if pending.len() >= DISTILL_MAX_RECORDS_PER_RUN {
            break;
        }
    }
    pending.truncate(DISTILL_MAX_RECORDS_PER_RUN);
    if pending.is_empty() {
        return Ok(());
    }

    let config_snapshot = ctx.dream.config.read().await.clone();
    let Some((provider, model)) = resolve_background_model(&ctx.dream, &config_snapshot) else {
        // No background model → leave records unmarked so a later configured
        // model can still distill them.
        return Ok(());
    };

    let lines: Vec<(String, String)> = pending
        .iter()
        .map(|doc| {
            let record = &doc.record;
            let detail = format!(
                "kind={}, completed={}, notes: {}",
                record.kind.as_str(),
                record.updated_at.format("%Y-%m-%d"),
                doc.body
                    .chars()
                    .take(200)
                    .collect::<String>()
                    .replace('\n', " "),
            );
            (record.title.clone(), detail)
        })
        .collect();
    let raw = collect_model_json(
        provider,
        &model,
        DISTILL_SYSTEM_INSTRUCTION,
        build_distillation_prompt(&lines),
    )
    .await?;
    let candidates = parse_distilled_candidates(&raw);

    for candidate in &candidates {
        let r#type = match candidate.r#type.as_deref() {
            Some("reference") => DurableMemoryType::Reference,
            _ => DurableMemoryType::User,
        };
        match memory
            .write_memory(
                bamboo_memory::memory_store::MemoryScope::Global,
                None,
                r#type,
                candidate.title.trim(),
                candidate.content.trim(),
                &candidate.tags,
                None,
                "ledger-gardener",
                true, // merge-if-similar: repeated habits reinforce one memory
                None,
            )
            .await
        {
            Ok(_) => result.memories_written += 1,
            Err(error) => {
                result.failed += 1;
                tracing::warn!(
                    target: LEDGER_GARDENER_TRACING_TARGET,
                    "[ledger-gardener] distilled memory write failed: {error}"
                );
            }
        }
    }

    // Mark every considered record — including ones yielding no memory — so
    // the same records are never re-fed to the model.
    for doc in pending {
        let mut record = doc.record.clone();
        record.tags.push(DISTILLED_TAG.to_string());
        if ledger
            .write_record(record, Some(doc.body.clone()))
            .await
            .is_ok()
        {
            result.distilled_records += 1;
        }
    }
    Ok(())
}

/// Spawn the recurring ledger gardener loop: one run at startup, then on the
/// configured interval. Disabled config makes each run return immediately.
pub fn spawn_ledger_gardener_task(ctx: LedgerGardenerContext) {
    tokio::spawn(async move {
        let interval_secs = {
            let guard = ctx.dream.config.read().await;
            guard
                .memory()
                .as_ref()
                .map(|memory| memory.ledger_gardener_interval_secs)
                .filter(|secs| *secs > 0)
                .unwrap_or_else(|| {
                    bamboo_config::MemoryConfig::default().ledger_gardener_interval_secs
                })
        };
        // Short poll so a config flip or startup work is picked up promptly;
        // the interval gate below decides whether a poll becomes a run.
        let poll_secs = 300.min(interval_secs.max(1));
        let mut ticker = tokio::time::interval(Duration::from_secs(poll_secs));
        let mut last_run: Option<Instant> = None;

        loop {
            ticker.tick().await;
            let due = last_run
                .map(|at| at.elapsed().as_secs() >= interval_secs)
                .unwrap_or(true);
            if !due {
                continue;
            }
            if let Err(error) = run_ledger_gardener_once(&ctx).await {
                tracing::warn!(
                    target: LEDGER_GARDENER_TRACING_TARGET,
                    event = "run_failed",
                    "[ledger-gardener] run failed: {error}"
                );
            }
            last_run = Some(Instant::now());
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use bamboo_agent_core::Message;
    use bamboo_domain::ledger::RecordKind;
    use bamboo_domain::schedule::ScheduleTrigger;
    use bamboo_llm::{Config, LLMChunk, LLMError, LLMProvider, LLMStream, ProviderRegistry};
    use bamboo_storage::SessionStoreV2;
    use chrono::Duration as ChronoDuration;
    use futures::stream;
    use tokio::sync::{Mutex as AsyncMutex, RwLock};

    fn config_with_memory(memory: bamboo_config::MemoryConfig) -> Config {
        let mut config = Config::default();
        *config.memory_mut() = Some(memory);
        config
    }

    fn record(kind: RecordKind, status: RecordStatus) -> LedgerRecord {
        let mut record = LedgerRecord::new("rec_test", kind, "Test record");
        record.status = status;
        record
    }

    #[test]
    fn expiry_policy_expires_only_fully_past_events_and_reminders() {
        let now = Utc::now();
        let past = now - ChronoDuration::hours(EXPIRY_GRACE_HOURS + 1);
        let recent_past = now - ChronoDuration::hours(1);

        // Event fully past the grace window → expires.
        let mut event = record(RecordKind::Event, RecordStatus::Open);
        event.time.starts_at = Some(past);
        assert!(should_expire(&event, now));

        // Event within the grace window → survives.
        let mut recent_event = record(RecordKind::Event, RecordStatus::Open);
        recent_event.time.starts_at = Some(recent_past);
        assert!(!should_expire(&recent_event, now));

        // Reminder with every remind_at past → expires.
        let mut reminder = record(RecordKind::Reminder, RecordStatus::Open);
        reminder.time.remind_at = vec![past, past - ChronoDuration::hours(2)];
        assert!(should_expire(&reminder, now));

        // One future reminder keeps it alive.
        let mut live_reminder = record(RecordKind::Reminder, RecordStatus::Open);
        live_reminder.time.remind_at = vec![past, now + ChronoDuration::hours(1)];
        assert!(!should_expire(&live_reminder, now));

        // A due date means it's open work — never auto-expired.
        let mut todo = record(RecordKind::Todo, RecordStatus::Open);
        todo.time.due_at = Some(past);
        assert!(!should_expire(&todo, now));

        // Recurrence keeps a record alive even with past reminders.
        let mut habit = record(RecordKind::Habit, RecordStatus::Open);
        habit.time.remind_at = vec![past];
        habit.time.recurrence = Some(ScheduleTrigger::Daily {
            hour: 9,
            minute: 0,
            second: 0,
        });
        assert!(!should_expire(&habit, now));

        // Terminal and undated records are untouched.
        assert!(!should_expire(
            &record(RecordKind::Todo, RecordStatus::Done),
            now
        ));
        assert!(!should_expire(
            &record(RecordKind::Todo, RecordStatus::Open),
            now
        ));
    }

    #[test]
    fn parse_distilled_candidates_is_lenient() {
        let wrapped = "Sure! Here you go:\n```json\n[{\"title\": \"Takes medication daily\", \"content\": \"User takes medication every morning at 9.\", \"type\": \"user\", \"tags\": [\"health\"]}]\n```";
        let parsed = parse_distilled_candidates(wrapped);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].title, "Takes medication daily");

        assert!(parse_distilled_candidates("no json here").is_empty());
        assert!(parse_distilled_candidates("[]").is_empty());
        assert!(parse_distilled_candidates("[{\"title\": \" \", \"content\": \"x\"}]").is_empty());
        assert!(parse_distilled_candidates("[{broken").is_empty());
    }

    #[test]
    fn distillation_prompt_lists_records_and_requests_json() {
        let prompt = build_distillation_prompt(&[(
            "Renew passport".to_string(),
            "kind=todo, completed=2026-07-13".to_string(),
        )]);
        assert!(prompt.contains("Renew passport"));
        assert!(prompt.contains("JSON array"));
    }

    #[derive(Clone)]
    struct CannedProvider {
        responses: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl LLMProvider for CannedProvider {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _tools: &[bamboo_agent_core::tools::ToolSchema],
            _max_output_tokens: Option<u32>,
            _model: &str,
        ) -> Result<LLMStream, LLMError> {
            let mut responses = self.responses.lock().expect("lock poisoned");
            let text = if responses.is_empty() {
                "[]".to_string()
            } else {
                responses.remove(0)
            };
            Ok(Box::pin(stream::iter(vec![
                Ok(LLMChunk::Token(text)),
                Ok(LLMChunk::Done),
            ])))
        }
    }

    #[derive(Default)]
    struct RecordingBridge {
        synced: AsyncMutex<Vec<String>>,
        released: AsyncMutex<Vec<String>>,
    }

    #[async_trait]
    impl LedgerScheduleBridge for RecordingBridge {
        async fn sync_record_schedules(
            &self,
            record: &LedgerRecord,
        ) -> Result<Vec<String>, String> {
            self.synced.lock().await.push(record.id.clone());
            Ok(vec![format!("sched_for_{}", record.id)])
        }

        async fn release_schedules(&self, schedule_ids: &[String]) -> Result<(), String> {
            self.released
                .lock()
                .await
                .extend(schedule_ids.iter().cloned());
            Ok(())
        }
    }

    #[tokio::test]
    async fn full_run_expires_reconciles_and_distills() {
        let temp = tempfile::tempdir().expect("tempdir");
        let session_store = Arc::new(
            SessionStoreV2::new(temp.path().to_path_buf())
                .await
                .unwrap(),
        );
        let ledger = LedgerStore::new(session_store.bamboo_home_dir());
        let jiandu_root = temp.path().join("jiandu");
        let memory = MemoryStore::new(&jiandu_root);
        let now = Utc::now();

        // 1) A long-past event holding a schedule: must expire + release.
        let mut past_event = LedgerRecord::new("rec_event", RecordKind::Event, "Old meetup");
        past_event.time.starts_at = Some(now - ChronoDuration::hours(EXPIRY_GRACE_HOURS + 10));
        past_event.schedule_ids = vec!["sched_old".to_string()];
        ledger.write_record(past_event, None).await.unwrap();

        // 2) An open reminder with a future time but no managed schedule
        //    (crash drift): must get schedules re-synced.
        let mut drifted = LedgerRecord::new("rec_drift", RecordKind::Reminder, "Call the bank");
        drifted.time.remind_at = vec![now + ChronoDuration::hours(5)];
        ledger.write_record(drifted, None).await.unwrap();

        // 3) A completed record: must be distilled into a durable memory and
        //    tagged so it is never re-fed to the model.
        let mut done = LedgerRecord::new("rec_done", RecordKind::Habit, "Morning run streak");
        done.status = RecordStatus::Done;
        ledger.write_record(done, None).await.unwrap();

        let provider: Arc<dyn LLMProvider> = Arc::new(CannedProvider {
            responses: Arc::new(Mutex::new(vec![
                "[{\"title\": \"Runs every morning\", \"content\": \"User keeps a morning run habit.\", \"type\": \"user\", \"tags\": [\"health\"]}]".to_string(),
            ])),
        });
        let config = Arc::new(RwLock::new(config_with_memory(
            bamboo_config::MemoryConfig {
                background_model: Some("fast-model".to_string()),
                ..bamboo_config::MemoryConfig::default()
            },
        )));
        let bridge = Arc::new(RecordingBridge::default());
        let ctx = LedgerGardenerContext {
            dream: AutoDreamContext {
                session_store: session_store.clone(),
                storage: session_store.clone(),
                memory: memory.clone(),
                provider,
                config,
                provider_registry: Arc::new(ProviderRegistry::new(
                    HashMap::new(),
                    "test".to_string(),
                )),
            },
            schedule_bridge: Some(bridge.clone()),
        };

        let result = run_ledger_gardener_once_with_stores(&ctx, &ledger, &memory)
            .await
            .unwrap()
            .expect("gardener enabled by default");

        assert_eq!(result.expired, 1);
        assert_eq!(result.schedules_released, 1);
        assert_eq!(result.schedules_synced, 1);
        assert_eq!(result.memories_written, 1);
        assert_eq!(result.distilled_records, 1);
        assert_eq!(result.failed, 0);

        let expired = ledger
            .get_record(LedgerScope::Global, None, "rec_event")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(expired.record.status, RecordStatus::Expired);
        assert!(expired.record.schedule_ids.is_empty());
        assert!(
            !expired.record.transitions.is_empty(),
            "history must survive"
        );
        assert_eq!(*bridge.released.lock().await, vec!["sched_old".to_string()]);

        let drifted = ledger
            .get_record(LedgerScope::Global, None, "rec_drift")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(drifted.record.schedule_ids, vec!["sched_for_rec_drift"]);

        let done = ledger
            .get_record(LedgerScope::Global, None, "rec_done")
            .await
            .unwrap()
            .unwrap();
        assert!(done.record.tags.iter().any(|tag| tag == DISTILLED_TAG));

        // A second run finds nothing new to do.
        let second = run_ledger_gardener_once_with_stores(&ctx, &ledger, &memory)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.expired, 0);
        assert_eq!(second.distilled_records, 0);
        assert_eq!(second.memories_written, 0);
        assert!(
            !jiandu_root.join("ledger").exists(),
            "Jiandu memory root must never host Bamboo Ledger data"
        );
    }
}
