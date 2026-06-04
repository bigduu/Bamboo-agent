//! Durable JSONL journal for the account change feed.
//!
//! Each [`ChangeEvent`] is appended as one JSON line to a rotating set of files
//! under `<bamboo_dir>/events/`, named `events-<20-digit-start-seq>.jsonl`. The
//! zero-padded start-seq in the filename makes the lexical file order match the
//! numeric seq order, so seeking and finding the oldest/newest file are cheap.
//!
//! ## Ownership model
//!
//! Writes are owned by a single writer task (see [`super::account_sink`]); this
//! type's `&mut self` append methods are never shared. Reads ([`read_since`],
//! [`oldest_seq`]) are stateless free functions that open files read-only, so a
//! `/stream` replay never contends with the writer.
//!
//! ## Rotation & recovery
//!
//! A new file is opened lazily on the first append, named by that event's seq.
//! When the current file passes [`ROTATE_THRESHOLD_BYTES`] it is closed and the
//! next append opens a fresh file — so files never need to be reopened for
//! append, which sidesteps torn-line-on-append entirely. On boot,
//! [`EventJournal::open`] recovers the max seq by reading the last *complete*
//! line of the newest file (a partial trailing line from a crash mid-write is
//! tolerated).

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use super::change_feed::ChangeEvent;

/// Roll to a new journal file once the current one passes this size.
pub const ROTATE_THRESHOLD_BYTES: u64 = 8 * 1024 * 1024;

const FILE_PREFIX: &str = "events-";
const FILE_SUFFIX: &str = ".jsonl";
const SEQ_PAD_WIDTH: usize = 20;

fn file_name_for(start_seq: u64) -> String {
    format!("{FILE_PREFIX}{start_seq:0SEQ_PAD_WIDTH$}{FILE_SUFFIX}")
}

/// Parse the start-seq encoded in a journal filename, if it matches the scheme.
fn start_seq_from_name(name: &str) -> Option<u64> {
    name.strip_prefix(FILE_PREFIX)?
        .strip_suffix(FILE_SUFFIX)?
        .parse::<u64>()
        .ok()
}

/// List journal files in ascending seq order (by encoded start-seq).
fn list_journal_files(dir: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut files: Vec<(u64, PathBuf)> = Vec::new();
    let read_dir = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(files),
        Err(e) => return Err(e),
    };
    for entry in read_dir {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(start) = start_seq_from_name(&name) {
            files.push((start, entry.path()));
        }
    }
    files.sort_by_key(|(start, _)| *start);
    Ok(files)
}

/// The greatest `seq` present across all journal files, or 0 if empty.
///
/// Reads only the last file and scans it for the last complete line, tolerating
/// a torn trailing line from a crash mid-write.
fn recover_max_seq(dir: &Path) -> io::Result<u64> {
    let files = list_journal_files(dir)?;
    let Some((_, path)) = files.last() else {
        return Ok(0);
    };
    let contents = std::fs::read_to_string(path)?;
    let mut max_seq = 0u64;
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        // A torn final line will fail to parse; skip it (only complete lines
        // were ever fully flushed).
        if let Ok(ce) = serde_json::from_str::<ChangeEvent>(line) {
            max_seq = max_seq.max(ce.seq);
        }
    }
    Ok(max_seq)
}

/// Append-only writer for the change-feed journal.
pub struct EventJournal {
    dir: PathBuf,
    current: Option<BufWriter<File>>,
    bytes_written: u64,
    rotate_threshold: u64,
}

impl EventJournal {
    /// Open (or create) the journal directory and recover the max seq seen.
    ///
    /// Returns the writer plus the recovered max seq (0 for a fresh journal),
    /// which the caller uses to seed the sequence counter.
    pub fn open(dir: PathBuf) -> io::Result<(Self, u64)> {
        Self::open_with_threshold(dir, ROTATE_THRESHOLD_BYTES)
    }

    /// Like [`Self::open`] but with a custom rotation threshold (bytes). A small
    /// threshold forces one file per event, which retention/resume tests use to
    /// build a multi-file journal deterministically.
    pub fn open_with_threshold(dir: PathBuf, rotate_threshold: u64) -> io::Result<(Self, u64)> {
        std::fs::create_dir_all(&dir)?;
        let max_seq = recover_max_seq(&dir)?;
        Ok((
            Self {
                dir,
                current: None,
                bytes_written: 0,
                rotate_threshold,
            },
            max_seq,
        ))
    }

    /// Append one change event as a JSON line, rotating first if needed.
    ///
    /// The line is flushed immediately: change events are low-volume, and
    /// flushing keeps concurrent `/stream` replays able to read the latest
    /// line without waiting on a timer.
    pub fn append(&mut self, ce: &ChangeEvent) -> io::Result<()> {
        if self.current.is_none() {
            let path = self.dir.join(file_name_for(ce.seq));
            let file = OpenOptions::new().create(true).append(true).open(&path)?;
            self.current = Some(BufWriter::new(file));
            self.bytes_written = 0;
        }

        let mut line = serde_json::to_string(ce).map_err(io::Error::other)?;
        line.push('\n');

        let writer = self
            .current
            .as_mut()
            .expect("current writer set above when None");
        writer.write_all(line.as_bytes())?;
        writer.flush()?;
        self.bytes_written += line.len() as u64;

        if self.bytes_written >= self.rotate_threshold {
            // Drop the current writer; the next append opens a fresh file named
            // by that event's seq.
            self.current = None;
        }
        Ok(())
    }
}

/// Read all change events with `seq > since`, in ascending seq order.
///
/// Scans the journal files whose range may contain matching events. Used by the
/// `/stream` resume path and by delta `/history?since`.
pub fn read_since(dir: &Path, since: u64) -> io::Result<Vec<ChangeEvent>> {
    let files = list_journal_files(dir)?;
    let mut out: Vec<ChangeEvent> = Vec::new();
    for (idx, (_start, path)) in files.iter().enumerate() {
        // Skip a file only when the *next* file's start-seq proves this whole
        // file precedes `since` (i.e. every event here is <= since).
        if let Some((next_start, _)) = files.get(idx + 1) {
            if next_start.saturating_sub(1) <= since {
                continue;
            }
        }
        let contents = std::fs::read_to_string(path)?;
        for line in contents.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if let Ok(ce) = serde_json::from_str::<ChangeEvent>(line) {
                if ce.seq > since {
                    out.push(ce);
                }
            }
        }
    }
    Ok(out)
}

/// The smallest `seq` still retained in the journal, or `None` if empty.
///
/// Derived from the lexically-smallest filename's encoded start-seq. Used to
/// decide whether a resuming client's cursor predates the retained window
/// (triggering a `feed_reset`).
pub fn oldest_seq(dir: &Path) -> io::Result<Option<u64>> {
    let files = list_journal_files(dir)?;
    Ok(files.first().map(|(start, _)| *start))
}

/// Delete the oldest journal files, keeping at most `max_files` newest.
///
/// Called at boot to bound disk usage. Change events are low-volume, so a
/// file-count cap (each file ~[`ROTATE_THRESHOLD_BYTES`]) is a predictable
/// proxy for a size budget. A client whose cursor falls below the new oldest
/// retained seq is told to full-resync via a `feed_reset` on `/stream`.
///
/// Returns the number of files deleted.
pub fn prune(dir: &Path, max_files: usize) -> io::Result<usize> {
    let files = list_journal_files(dir)?;
    if files.len() <= max_files {
        return Ok(0);
    }
    let to_delete = files.len() - max_files;
    let mut deleted = 0;
    for (_, path) in files.iter().take(to_delete) {
        match std::fs::remove_file(path) {
            Ok(()) => deleted += 1,
            Err(e) => tracing::warn!("failed to prune journal file {}: {e}", path.display()),
        }
    }
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::AgentEvent;
    use chrono::Utc;

    fn ev(seq: u64) -> ChangeEvent {
        ChangeEvent {
            seq,
            ts: Utc::now(),
            session_id: Some(format!("s{seq}")),
            event: AgentEvent::SessionDeleted {
                session_id: format!("s{seq}"),
            },
        }
    }

    #[test]
    fn round_trips_and_reads_since() {
        let dir = tempfile::tempdir().unwrap();
        let (mut j, max) = EventJournal::open(dir.path().to_path_buf()).unwrap();
        assert_eq!(max, 0);
        for seq in 1..=5 {
            j.append(&ev(seq)).unwrap();
        }
        let got = read_since(dir.path(), 0).unwrap();
        assert_eq!(
            got.iter().map(|e| e.seq).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );

        let tail = read_since(dir.path(), 3).unwrap();
        assert_eq!(tail.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![4, 5]);

        let none = read_since(dir.path(), 5).unwrap();
        assert!(none.is_empty());
    }

    #[test]
    fn recovers_max_seq_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut j, _) = EventJournal::open(dir.path().to_path_buf()).unwrap();
            for seq in 1..=3 {
                j.append(&ev(seq)).unwrap();
            }
        }
        let (_j, max) = EventJournal::open(dir.path().to_path_buf()).unwrap();
        assert_eq!(max, 3);
    }

    #[test]
    fn rotates_by_size_and_reads_across_files() {
        let dir = tempfile::tempdir().unwrap();
        // Tiny threshold so each append rotates.
        let (mut j, _) = EventJournal::open_with_threshold(dir.path().to_path_buf(), 1).unwrap();
        for seq in 1..=4 {
            j.append(&ev(seq)).unwrap();
        }
        let files = list_journal_files(dir.path()).unwrap();
        assert!(files.len() >= 2, "expected rotation into multiple files");
        let got = read_since(dir.path(), 0).unwrap();
        assert_eq!(
            got.iter().map(|e| e.seq).collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        assert_eq!(oldest_seq(dir.path()).unwrap(), Some(1));
    }

    #[test]
    fn prune_keeps_newest_files_and_advances_oldest() {
        let dir = tempfile::tempdir().unwrap();
        // One file per event.
        let (mut j, _) = EventJournal::open_with_threshold(dir.path().to_path_buf(), 1).unwrap();
        for seq in 1..=6 {
            j.append(&ev(seq)).unwrap();
        }
        assert_eq!(list_journal_files(dir.path()).unwrap().len(), 6);

        let deleted = prune(dir.path(), 2).unwrap();
        assert_eq!(deleted, 4);
        // Only the two newest files (seq 5, 6) remain.
        assert_eq!(oldest_seq(dir.path()).unwrap(), Some(5));
        let remaining = read_since(dir.path(), 0).unwrap();
        assert_eq!(
            remaining.iter().map(|e| e.seq).collect::<Vec<_>>(),
            vec![5, 6]
        );

        // Pruning below the file count is a no-op.
        assert_eq!(prune(dir.path(), 10).unwrap(), 0);
    }

    #[test]
    fn tolerates_torn_final_line_on_recovery() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut j, _) = EventJournal::open(dir.path().to_path_buf()).unwrap();
            for seq in 1..=3 {
                j.append(&ev(seq)).unwrap();
            }
        }
        // Append a partial (torn) line directly to the newest file.
        let files = list_journal_files(dir.path()).unwrap();
        let (_, path) = files.last().unwrap();
        let mut f = OpenOptions::new().append(true).open(path).unwrap();
        f.write_all(b"{\"seq\":4,\"ts\":\"broke").unwrap();
        drop(f);

        let (_j, max) = EventJournal::open(dir.path().to_path_buf()).unwrap();
        assert_eq!(max, 3, "torn line must be ignored");
    }
}
