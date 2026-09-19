//! Prompt-cache prefix drift diagnostics.
//!
//! The cacheable prefix sent to the LLM (`tools` + the stable system
//! instructions) must stay byte-identical across rounds for a prompt-cache hit.
//! This module observes the stable system prefix every round and, when it
//! changes versus the previous round of the same session, records *which*
//! section changed — flagging shrinks (length decreases) specially, since a
//! shrink means content was dropped out of the cached prefix (the worst case for
//! hit rate).
//!
//! This is a pure side-channel: it never affects what is sent to the LLM and all
//! its IO is best-effort (failures are logged at debug and swallowed).
//!
//! ## Behaviour
//! - Cheap per round: hashes each section and compares against the previous
//!   round's hashes stored in session metadata. No disk IO when the prefix is
//!   stable.
//! - On change: emits a `warn!` summary and writes a drift report plus a session
//!   snapshot with provider-native payloads redacted under
//!   `<app_data_dir>/prompt-cache-drift/<session_id>/`.
//! - Bounded: at most [`MAX_DUMPS_PER_SESSION`] report/snapshot pairs are written
//!   per session; beyond that, detection and logging continue but disk writes
//!   stop.

use std::path::Path;

use sha2::{Digest, Sha256};

use bamboo_agent_core::Session;

use crate::runtime::runner::session_setup::prompt_setup::StablePrefixSection;

/// Session-metadata key holding the previous round's per-section state (JSON).
const STATE_KEY: &str = "prefix_cache_section_state";
/// Session-metadata key holding a monotonic observation counter (used for
/// report filenames).
const OBS_ROUND_KEY: &str = "prefix_cache_obs_round";
/// Session-metadata key counting how many drift reports were written so far.
const DUMP_COUNT_KEY: &str = "prefix_cache_drift_dumps";
/// Cap on report/snapshot pairs written per session, to bound disk usage when
/// the prefix drifts every round.
const MAX_DUMPS_PER_SESSION: usize = 20;

const DRIFT_SUBDIR: &str = "prompt-cache-drift";
const LAST_SECTIONS_FILE: &str = "_last_sections.json";

fn sha256_hex(content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    hex::encode(hasher.finalize())
}

/// Per-section state persisted between rounds for cheap change detection.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct SectionState {
    name: String,
    len: usize,
    sha: String,
}

fn capture_state(sections: &[StablePrefixSection]) -> Vec<SectionState> {
    sections
        .iter()
        .map(|section| SectionState {
            name: section.name.to_string(),
            len: section.content.len(),
            sha: sha256_hex(&section.content),
        })
        .collect()
}

fn metadata_usize(session: &Session, key: &str) -> usize {
    session
        .metadata
        .get(key)
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(0)
}

/// Observe the stable prefix for this round and record drift if it changed.
///
/// Never returns an error: diagnostics must not interfere with the agent loop.
pub(super) fn record_prefix_drift(
    session: &mut Session,
    app_data_dir: Option<&Path>,
    sections: &[StablePrefixSection],
) {
    let obs_round = metadata_usize(session, OBS_ROUND_KEY) + 1;
    session
        .metadata
        .insert(OBS_ROUND_KEY.to_string(), obs_round.to_string());

    let current = capture_state(sections);
    let previous: Option<Vec<SectionState>> = session
        .metadata
        .get(STATE_KEY)
        .and_then(|raw| serde_json::from_str(raw).ok());

    // Always advance the recorded state so the next round compares against this
    // round.
    if let Ok(raw) = serde_json::to_string(&current) {
        session.metadata.insert(STATE_KEY.to_string(), raw);
    }

    let session_id = session.id.clone();
    let drift_dir = app_data_dir.map(|dir| dir.join(DRIFT_SUBDIR).join(&session_id));

    let Some(previous) = previous else {
        // First observed round: establish a content baseline for later diffs.
        if let Some(dir) = drift_dir.as_ref() {
            write_last_sections(dir, sections);
        }
        tracing::debug!(
            "[{}] prefix-cache baseline established at obs_round={} ({} sections, {} bytes)",
            session_id,
            obs_round,
            sections.len(),
            sections.iter().map(|s| s.content.len()).sum::<usize>(),
        );
        return;
    };

    let changes = diff_sections(&previous, &current);
    if changes.is_empty() {
        tracing::debug!(
            "[{}] prefix-cache stable at obs_round={}",
            session_id,
            obs_round
        );
        return;
    }

    let shrunk: Vec<&SectionChange> = changes.iter().filter(|c| c.delta < 0).collect();
    tracing::warn!(
        "[{}] prefix-cache DRIFT at obs_round={}: changed={:?} shrunk={:?} (shrinks drop cached content and break the prefix cache)",
        session_id,
        obs_round,
        changes
            .iter()
            .map(|c| format!("{}({:+})", c.name, c.delta))
            .collect::<Vec<_>>(),
        shrunk.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
    );

    let Some(dir) = drift_dir else {
        return;
    };

    let dumps = metadata_usize(session, DUMP_COUNT_KEY);
    if dumps >= MAX_DUMPS_PER_SESSION {
        tracing::debug!(
            "[{}] prefix-cache drift dump cap ({}) reached; logging only",
            session_id,
            MAX_DUMPS_PER_SESSION
        );
        return;
    }

    let old_sections = read_last_sections(&dir);
    write_drift_report(&dir, obs_round, &changes, sections, old_sections.as_deref());
    write_session_snapshot(&dir, obs_round, session);
    write_last_sections(&dir, sections);
    session
        .metadata
        .insert(DUMP_COUNT_KEY.to_string(), (dumps + 1).to_string());
}

struct SectionChange {
    name: String,
    old_len: usize,
    new_len: usize,
    /// `new_len - old_len`; negative means the section shrank.
    delta: i64,
    old_sha: String,
    new_sha: String,
}

fn diff_sections(previous: &[SectionState], current: &[SectionState]) -> Vec<SectionChange> {
    let mut changes = Vec::new();
    for cur in current {
        let prev = previous.iter().find(|p| p.name == cur.name);
        let (old_len, old_sha) = prev
            .map(|p| (p.len, p.sha.clone()))
            .unwrap_or((0, String::new()));
        if old_sha != cur.sha {
            changes.push(SectionChange {
                name: cur.name.clone(),
                old_len,
                new_len: cur.len,
                delta: cur.len as i64 - old_len as i64,
                old_sha,
                new_sha: cur.sha.clone(),
            });
        }
    }
    // Sections that disappeared entirely (present before, absent now).
    for prev in previous {
        if !current.iter().any(|c| c.name == prev.name) {
            changes.push(SectionChange {
                name: prev.name.clone(),
                old_len: prev.len,
                new_len: 0,
                delta: -(prev.len as i64),
                old_sha: prev.sha.clone(),
                new_sha: String::new(),
            });
        }
    }
    changes
}

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredSection {
    name: String,
    content: String,
}

fn write_last_sections(dir: &Path, sections: &[StablePrefixSection]) {
    let stored: Vec<StoredSection> = sections
        .iter()
        .map(|s| StoredSection {
            name: s.name.to_string(),
            content: s.content.clone(),
        })
        .collect();
    write_json(&dir.join(LAST_SECTIONS_FILE), &stored);
}

fn read_last_sections(dir: &Path) -> Option<Vec<StoredSection>> {
    let raw = std::fs::read_to_string(dir.join(LAST_SECTIONS_FILE)).ok()?;
    serde_json::from_str(&raw).ok()
}

fn write_drift_report(
    dir: &Path,
    obs_round: usize,
    changes: &[SectionChange],
    current_sections: &[StablePrefixSection],
    old_sections: Option<&[StoredSection]>,
) {
    let total_old: usize = changes.iter().map(|c| c.old_len).sum();
    let total_new: usize = changes.iter().map(|c| c.new_len).sum();
    let section_reports: Vec<serde_json::Value> = changes
        .iter()
        .map(|c| {
            let new_content = current_sections
                .iter()
                .find(|s| s.name == c.name)
                .map(|s| s.content.clone());
            let old_content = old_sections
                .and_then(|olds| olds.iter().find(|s| s.name == c.name))
                .map(|s| s.content.clone());
            serde_json::json!({
                "name": c.name,
                "old_len": c.old_len,
                "new_len": c.new_len,
                "delta": c.delta,
                "shrunk": c.delta < 0,
                "old_sha": c.old_sha,
                "new_sha": c.new_sha,
                "old_content": old_content,
                "new_content": new_content,
            })
        })
        .collect();

    let report = serde_json::json!({
        "obs_round": obs_round,
        "recorded_at": chrono::Utc::now().to_rfc3339(),
        "changed_section_count": changes.len(),
        "changed_total_old_len": total_old,
        "changed_total_new_len": total_new,
        "changed_total_delta": total_new as i64 - total_old as i64,
        "any_shrunk": changes.iter().any(|c| c.delta < 0),
        "old_content_available": old_sections.is_some(),
        "sections": section_reports,
    });

    write_json(&dir.join(format!("round-{obs_round}.json")), &report);
}

fn write_session_snapshot(dir: &Path, obs_round: usize, session: &Session) {
    let mut redacted = session.clone();
    redacted.provider_transcript = Default::default();
    write_json(&dir.join(format!("session-{obs_round}.json")), &redacted);
}

fn write_json<T: serde::Serialize>(path: &Path, value: &T) {
    if let Some(parent) = path.parent() {
        if let Err(err) = std::fs::create_dir_all(parent) {
            tracing::debug!(
                "prefix-cache drift: mkdir {} failed: {}",
                parent.display(),
                err
            );
            return;
        }
    }
    match serde_json::to_vec_pretty(value) {
        Ok(bytes) => {
            if let Err(err) = std::fs::write(path, bytes) {
                tracing::debug!(
                    "prefix-cache drift: write {} failed: {}",
                    path.display(),
                    err
                );
            }
        }
        Err(err) => {
            tracing::debug!(
                "prefix-cache drift: serialize {} failed: {}",
                path.display(),
                err
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn section(name: &'static str, content: &str) -> StablePrefixSection {
        StablePrefixSection {
            name,
            content: content.to_string(),
        }
    }

    #[test]
    fn stable_prefix_writes_no_files_and_logs_stable() {
        let dir = std::env::temp_dir().join("bamboo-prefix-drift-stable-test");
        let _ = std::fs::remove_dir_all(&dir);
        let mut session = Session::new("drift-stable", "model");
        let sections = vec![section("base", "hello"), section("tool_guide", "guide")];

        // Round 1 establishes a baseline (writes _last only).
        record_prefix_drift(&mut session, Some(&dir), &sections);
        // Round 2 identical → no drift report.
        record_prefix_drift(&mut session, Some(&dir), &sections);

        let session_dir = dir.join(DRIFT_SUBDIR).join("drift-stable");
        assert!(!session_dir.join("round-2.json").exists());
        assert_eq!(metadata_usize(&session, DUMP_COUNT_KEY), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn shrink_is_detected_and_recorded_with_old_and_new_content() {
        let dir = std::env::temp_dir().join("bamboo-prefix-drift-shrink-test");
        let _ = std::fs::remove_dir_all(&dir);
        let mut session = Session::new("drift-shrink", "model");
        let assistant = bamboo_domain::Message::assistant("normalized discovery", None);
        let anchor = assistant.id.clone();
        session.add_message(assistant);
        let item = bamboo_domain::ProviderTranscriptItem::try_from_payload(
            bamboo_domain::ProviderFamily::OpenAi,
            bamboo_domain::ProviderProtocol::OpenAiResponsesV1,
            bamboo_domain::ProviderTranscriptOrigin::Provider,
            bamboo_domain::ProviderTranscriptAuthor::Model,
            serde_json::json!({
                "type":"tool_search_call",
                "id":"tsc_drift",
                "execution":"client",
                "call_id":"call_drift",
                "status":"completed",
                "arguments":{"query":"NATIVE_DIAGNOSTIC_SENTINEL"}
            }),
        )
        .unwrap();
        session
            .append_provider_transcript_group(&anchor, None, vec![item])
            .unwrap();

        record_prefix_drift(
            &mut session,
            Some(&dir),
            &[
                section("base", "BASE"),
                section("tool_guide", "long guide text"),
            ],
        );
        // tool_guide shrinks.
        record_prefix_drift(
            &mut session,
            Some(&dir),
            &[section("base", "BASE"), section("tool_guide", "short")],
        );

        let session_dir = dir.join(DRIFT_SUBDIR).join("drift-shrink");
        let report_raw = std::fs::read_to_string(session_dir.join("round-2.json"))
            .expect("drift report should exist");
        let report: serde_json::Value = serde_json::from_str(&report_raw).unwrap();
        assert_eq!(report["any_shrunk"], serde_json::json!(true));
        assert_eq!(report["old_content_available"], serde_json::json!(true));
        let sections = report["sections"].as_array().unwrap();
        let guide = sections
            .iter()
            .find(|s| s["name"] == serde_json::json!("tool_guide"))
            .unwrap();
        assert_eq!(guide["shrunk"], serde_json::json!(true));
        assert_eq!(guide["old_content"], serde_json::json!("long guide text"));
        assert_eq!(guide["new_content"], serde_json::json!("short"));
        let session_snapshot = std::fs::read_to_string(session_dir.join("session-2.json"))
            .expect("redacted session snapshot should exist");
        assert!(!session_snapshot.contains("NATIVE_DIAGNOSTIC_SENTINEL"));
        let session_snapshot: serde_json::Value = serde_json::from_str(&session_snapshot).unwrap();
        assert!(session_snapshot["provider_transcript"]["groups"]
            .as_array()
            .is_none_or(Vec::is_empty));
        assert_eq!(metadata_usize(&session, DUMP_COUNT_KEY), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
