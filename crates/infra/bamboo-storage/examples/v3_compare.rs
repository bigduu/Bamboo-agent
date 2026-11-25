//! Compare durable append and control-only saves on a synthetic conversation.
use std::{error::Error, path::Path, time::Instant};

use bamboo_domain::{Message, Session, Storage};
use bamboo_storage::{v2::SessionStoreV2, v3::SessionStoreV3};
use rusqlite::Connection;
use serde_json::json;

fn arguments() -> Result<(usize, usize), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let history = args.next().map_or(Ok(4096), |value| value.parse())?;
    let samples = args.next().map_or(Ok(30), |value| value.parse())?;
    if samples == 0 || args.next().is_some() {
        return Err("usage: v3_compare [initial-messages] [samples > 0]".into());
    }
    Ok((history, samples))
}

fn p95(mut samples: Vec<u64>) -> u64 {
    samples.sort_unstable();
    samples[(samples.len() * 95).div_ceil(100) - 1]
}

fn checkpoint(connection: &Connection) -> Result<(), Box<dyn Error>> {
    let busy: i64 =
        connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))?;
    if busy != 0 {
        return Err("unexpected busy checkpoint on disposable shadow database".into());
    }
    Ok(())
}

fn bytes(path: &Path) -> std::io::Result<u64> {
    Ok(std::fs::metadata(path)?.len())
}

fn main() -> Result<(), Box<dyn Error>> {
    let (history_size, samples) = arguments()?;
    let scratch = tempfile::tempdir()?;
    let v2_home = scratch.path().join("v2");
    let db = scratch.path().join("v3.sqlite");
    let wal = scratch.path().join("v3.sqlite-wal");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let v2 = rt.block_on(SessionStoreV2::new(v2_home.clone()))?;
    let v3 = SessionStoreV3::open(&db)?;
    let monitor = Connection::open(&db)?;
    let mut session = Session::new("perf-fixture", "fixture-model");
    let body = "Synthetic tool output for a long conversation. 中文 log and code.\n".repeat(16);

    for index in 0..history_size {
        session.add_message(Message::user(format!("{index} {body}")));
    }
    rt.block_on(v2.save_session(&session))?;
    let mut revision = v3.put(&session, None)?;
    let main = v2_home.join("sessions/perf-fixture/session.json");
    let mut v2_append = Vec::with_capacity(samples);
    let mut v3_append = Vec::with_capacity(samples);
    let mut main_bytes = Vec::with_capacity(samples);
    let mut wal_bytes = Vec::with_capacity(samples);
    for index in 0..samples {
        checkpoint(&monitor)?;
        session.add_message(Message::user(format!("appended {index}: {body}")));
        let started = Instant::now();
        rt.block_on(v2.save_session(&session))?;
        v2_append.push(started.elapsed().as_micros() as u64);
        main_bytes.push(bytes(&main)?);
        let started = Instant::now();
        revision = v3.put(&session, Some(revision))?;
        v3_append.push(started.elapsed().as_micros() as u64);
        wal_bytes.push(bytes(&wal)?);
    }

    let mut runtime = v3.load_runtime(&session.id)?.ok_or("missing V3 runtime")?;
    let mut v2_runtime = Vec::with_capacity(samples);
    let mut v3_runtime = Vec::with_capacity(samples);
    for _ in 0..samples {
        let tick = chrono::Utc::now();
        session.updated_at = tick;
        runtime.session.updated_at = tick;
        let started = Instant::now();
        rt.block_on(v2.save_runtime_state(&session))?;
        v2_runtime.push(started.elapsed().as_micros() as u64);
        let started = Instant::now();
        revision = v3.put_runtime(&runtime.session, revision.runtime)?;
        v3_runtime.push(started.elapsed().as_micros() as u64);
    }

    let history_page = v3.message_page(
        &session.id,
        history_size as u64,
        samples.min(1000),
        revision.history,
    )?;
    assert_eq!(history_page[0].id, session.messages[history_size].id);
    // The search worker can still hold the V2 SQLite connection after a save.
    rt.block_on(v2.flush_search_index());
    drop(v2);
    drop(v3);
    drop(monitor);
    let v2 = rt.block_on(SessionStoreV2::new(v2_home))?;
    let v3 = SessionStoreV3::open(&db)?;
    let saved_v2 = rt
        .block_on(v2.load_session(&session.id))?
        .ok_or("missing reopened V2 session")?;
    let saved_v3 = v3.load(&session.id)?.ok_or("missing reopened V3 session")?;
    let messages = serde_json::to_value(&session.messages)?;
    assert_eq!(serde_json::to_value(&saved_v2.messages)?, messages);
    assert_eq!(serde_json::to_value(&saved_v3.session.messages)?, messages);
    assert_eq!(saved_v3.revision, revision);

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "initial_messages": history_size,
            "samples_per_operation": samples,
            "v2_append_p95_us": p95(v2_append),
            "v3_append_p95_us": p95(v3_append),
            "v2_runtime_p95_us": p95(v2_runtime),
            "v3_runtime_p95_us": p95(v3_runtime),
            "v2_main_p95_bytes": p95(main_bytes),
            "v3_wal_p95_bytes": p95(wal_bytes),
            "reopen_messages_verified": session.messages.len(),
        }))?
    );
    Ok(())
}
