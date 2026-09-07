//! Opt-in SQLite session snapshots and shadow imports.
//!
//! This store is not a runtime authority selector. Opening it never changes V2
//! files or activates a session. Call synchronous methods on a blocking worker
//! when embedding this offline/shadow API in an async application.
use std::collections::HashSet;
use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use bamboo_domain::{Message, Session, SessionInboxAdmissionState};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};

const APPLICATION_ID: i64 = 0x424d4233;
const VERSION: i64 = 3;

#[derive(Debug, thiserror::Error)]
pub enum V3Error {
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("session revision or creation identity changed")]
    Conflict,
    #[error("session snapshot not found")]
    NotFound,
    #[error("invalid session snapshot: {0}")]
    Invalid(String),
    #[error("unsupported session database")]
    UnsupportedDatabase,
    #[error("session database lock was poisoned")]
    Poisoned,
}
type Result<T> = std::result::Result<T, V3Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct V3Revision {
    pub history: i64,
    pub runtime: i64,
}

#[derive(Debug)]
pub struct V3Snapshot {
    pub session: Session,
    pub revision: V3Revision,
}

#[derive(Serialize, Deserialize)]
struct HistoryProof {
    provider_transcript: bamboo_domain::ProviderTranscriptState,
    admission: Option<SessionInboxAdmissionState>,
}

pub struct SessionStoreV3 {
    connection: Mutex<Connection>,
}

impl SessionStoreV3 {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        let application: i64 =
            connection.pragma_query_value(None, "application_id", |row| row.get(0))?;
        let version: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
        let tables: i64 = connection.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            [],
            |row| row.get(0),
        )?;
        if !(application == APPLICATION_ID && version == VERSION)
            && !(application == 0 && version == 0 && tables == 0)
        {
            return Err(V3Error::UnsupportedDatabase);
        }
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS sessions (
            session_id TEXT PRIMARY KEY,
            root_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            history_rev INTEGER NOT NULL CHECK(history_rev > 0),
            runtime_rev INTEGER NOT NULL CHECK(runtime_rev > 0),
            runtime_json BLOB NOT NULL,
            proof_json BLOB NOT NULL
        );
        CREATE TABLE IF NOT EXISTS messages (
            session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
            ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
            payload BLOB NOT NULL,
            PRIMARY KEY(session_id, ordinal)
        );",
        )?;
        tx.pragma_update(None, "application_id", APPLICATION_ID)?;
        tx.pragma_update(None, "user_version", VERSION)?;
        tx.commit()?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>> {
        self.connection.lock().map_err(|_| V3Error::Poisoned)
    }

    /// Full snapshot CAS. None creates; Some requires both current revisions.
    /// Only changed message rows are written, even when an earlier row is edited.
    pub fn put(&self, session: &Session, expected: Option<V3Revision>) -> Result<V3Revision> {
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let revision = put_in_transaction(&tx, session, expected)?;
        tx.commit()?;
        Ok(revision)
    }

    /// Replace control state without loading or writing messages or history proof.
    /// Pass the message-free snapshot returned by `load_runtime`. A runtime CAS
    /// and immutable root/creation identity protect against stale updates.
    pub fn put_runtime(&self, session: &Session, expected_runtime: i64) -> Result<V3Revision> {
        if session.id.trim().is_empty() {
            return Err(V3Error::Invalid("empty session id".into()));
        }
        if !session.messages.is_empty()
            || !session.provider_transcript.is_empty()
            || session
                .runtime_metadata
                .as_ref()
                .is_some_and(|metadata| metadata.session_inbox_admission.is_some())
        {
            return Err(V3Error::Invalid(
                "runtime updates require a message-free runtime snapshot".into(),
            ));
        }
        let bytes = runtime_bytes(session)?;
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let stored = runtime_header(&tx, &session.id)?.ok_or(V3Error::NotFound)?;
        check_identity(session, &stored)?;
        if stored.revision.runtime != expected_runtime {
            return Err(V3Error::Conflict);
        }
        let mut revision = stored.revision;
        if stored.runtime != bytes {
            revision.runtime = next_revision(revision.runtime)?;
            tx.execute(
                "UPDATE sessions SET runtime_json=?1, runtime_rev=?2 WHERE session_id=?3",
                params![bytes, revision.runtime, session.id],
            )?;
        }
        tx.commit()?;
        Ok(revision)
    }

    /// Read a coherent control/history snapshot across independent connections.
    pub fn load(&self, id: &str) -> Result<Option<V3Snapshot>> {
        let mut connection = self.connection()?;
        let tx = connection.transaction()?;
        let Some(stored) = header(&tx, id)? else {
            return Ok(None);
        };
        let mut session: Session = serde_json::from_slice(&stored.runtime)?;
        if session.id != id {
            return Err(V3Error::Invalid("runtime identity mismatch".into()));
        }
        let proof: HistoryProof = serde_json::from_slice(&stored.proof)?;
        session.provider_transcript = proof.provider_transcript;
        if let Some(admission) = proof.admission {
            session
                .runtime_metadata
                .get_or_insert_with(Default::default)
                .session_inbox_admission = Some(admission);
        }
        let rows = message_rows(&tx, id)?;
        session.messages = rows
            .into_iter()
            .map(|row| serde_json::from_slice(&row))
            .collect::<std::result::Result<_, _>>()?;
        validate_session(&session)?;
        check_identity(&session, &stored)?;
        tx.commit()?;
        Ok(Some(V3Snapshot {
            session,
            revision: stored.revision,
        }))
    }

    /// Small control snapshot; no history or provider transcript is returned.
    pub fn load_runtime(&self, id: &str) -> Result<Option<V3Snapshot>> {
        let connection = self.connection()?;
        let Some(stored) = runtime_header(&connection, id)? else {
            return Ok(None);
        };
        let session: Session = serde_json::from_slice(&stored.runtime)?;
        if session.id != id {
            return Err(V3Error::Invalid("runtime identity mismatch".into()));
        }
        check_identity(&session, &stored)?;
        Ok(Some(V3Snapshot {
            session,
            revision: stored.revision,
        }))
    }

    /// Pagination fails if history changed since the caller obtained its revision.
    pub fn message_page(
        &self,
        id: &str,
        offset: u64,
        limit: usize,
        history_revision: i64,
    ) -> Result<Vec<Message>> {
        if limit == 0 || limit > 1000 || offset > i64::MAX as u64 {
            return Err(V3Error::Invalid("invalid page bounds".into()));
        }
        let mut connection = self.connection()?;
        let tx = connection.transaction()?;
        let revision: Option<i64> = tx
            .query_row(
                "SELECT history_rev FROM sessions WHERE session_id=?1",
                [id],
                |row| row.get(0),
            )
            .optional()?;
        if revision.ok_or(V3Error::NotFound)? != history_revision {
            return Err(V3Error::Conflict);
        }
        let result = {
            let mut query = tx.prepare("SELECT payload FROM messages WHERE session_id=?1 AND ordinal>=?2 ORDER BY ordinal LIMIT ?3")?;
            let rows = query.query_map(params![id, offset as i64, limit as i64], |row| {
                row.get::<_, Vec<u8>>(0)
            })?;
            rows.map(|row| Ok(serde_json::from_slice(&row?)?))
                .collect::<Result<Vec<_>>>()?
        };
        tx.commit()?;
        Ok(result)
    }

    /// Import one complete tree atomically into an empty shadow namespace.
    /// A duplicate, missing parent, cycle, or existing id aborts the entire tree.
    pub fn import_tree_shadow(&self, sessions: &[Session]) -> Result<Vec<V3Revision>> {
        validate_tree(sessions)?;
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut revisions = Vec::new();
        for session in sessions {
            revisions.push(put_in_transaction(&tx, session, None)?);
        }
        tx.commit()?;
        Ok(revisions)
    }

    /// Exact semantic comparison for a captured source snapshot. This grants no
    /// write-authority switch; active V2 writers may advance after capture.
    pub fn verify_shadow(&self, source: &Session) -> Result<bool> {
        let Some(snapshot) = self.load(&source.id)? else {
            return Ok(false);
        };
        Ok(
            bamboo_domain::canonical_json_bytes(&serde_json::to_value(source)?)
                == bamboo_domain::canonical_json_bytes(&serde_json::to_value(snapshot.session)?),
        )
    }
}

struct Header {
    root: String,
    created_at: String,
    revision: V3Revision,
    runtime: Vec<u8>,
    proof: Vec<u8>,
}
fn header(connection: &Connection, id: &str) -> Result<Option<Header>> {
    Ok(connection.query_row("SELECT root_id,created_at,history_rev,runtime_rev,runtime_json,proof_json FROM sessions WHERE session_id=?1", [id], |row| {
        Ok(Header { root: row.get(0)?, created_at: row.get(1)?, revision: V3Revision { history: row.get(2)?, runtime: row.get(3)? }, runtime: row.get(4)?, proof: row.get(5)? })
    }).optional()?)
}
fn runtime_header(connection: &Connection, id: &str) -> Result<Option<Header>> {
    Ok(connection.query_row("SELECT root_id,created_at,history_rev,runtime_rev,runtime_json FROM sessions WHERE session_id=?1", [id], |row| {
        Ok(Header { root: row.get(0)?, created_at: row.get(1)?, revision: V3Revision { history: row.get(2)?, runtime: row.get(3)? }, runtime: row.get(4)?, proof: Vec::new() })
    }).optional()?)
}
fn root_id(session: &Session) -> &str {
    if session.root_session_id.is_empty() {
        &session.id
    } else {
        &session.root_session_id
    }
}
fn check_identity(session: &Session, stored: &Header) -> Result<()> {
    let current: Session = serde_json::from_slice(&stored.runtime)?;
    if session.id != current.id
        || root_id(session) != stored.root
        || session.created_at.to_rfc3339() != stored.created_at
        || session.authority_identity != current.authority_identity
        || session.kind != current.kind
        || session.parent_session_id != current.parent_session_id
        || session.spawn_depth != current.spawn_depth
        || session.metadata_version < current.metadata_version
        || (session.project_id_meta() != current.project_id_meta()
            && current.metadata_version.checked_add(1) != Some(session.metadata_version))
    {
        return Err(V3Error::Conflict);
    }
    Ok(())
}
fn canonical<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    Ok(bamboo_domain::canonical_json_bytes(&serde_json::to_value(
        value,
    )?))
}
fn runtime_bytes(session: &Session) -> Result<Vec<u8>> {
    canonical(&crate::v2::runtime_sidecar_snapshot(session))
}
fn message_rows(connection: &Connection, id: &str) -> Result<Vec<Vec<u8>>> {
    let mut query =
        connection.prepare("SELECT payload FROM messages WHERE session_id=?1 ORDER BY ordinal")?;
    let rows = query.query_map([id], |row| row.get(0))?;
    Ok(rows.collect::<std::result::Result<_, _>>()?)
}
fn validate_session(session: &Session) -> Result<()> {
    if session.id.trim().is_empty() {
        return Err(V3Error::Invalid("empty session id".into()));
    }
    if session.kind == bamboo_domain::SessionKind::Root
        && (session.parent_session_id.is_some()
            || session.spawn_depth != 0
            || root_id(session) != session.id)
    {
        return Err(V3Error::Invalid("invalid root identity".into()));
    }
    let mut ids = HashSet::new();
    for message in &session.messages {
        if message.id.is_empty() || !ids.insert(&message.id) {
            return Err(V3Error::Invalid("empty or duplicate message id".into()));
        }
    }
    Ok(())
}
fn next_revision(value: i64) -> Result<i64> {
    value
        .checked_add(1)
        .filter(|next| *next > 0)
        .ok_or(V3Error::Conflict)
}
fn put_in_transaction(
    tx: &Transaction<'_>,
    session: &Session,
    expected: Option<V3Revision>,
) -> Result<V3Revision> {
    validate_session(session)?;
    let existing = header(tx, &session.id)?;
    if existing.as_ref().map(|header| header.revision) != expected {
        return Err(V3Error::Conflict);
    }
    if let Some(stored) = &existing {
        check_identity(session, stored)?;
    }
    let runtime = runtime_bytes(session)?;
    let proof = canonical(&HistoryProof {
        provider_transcript: session.provider_transcript.clone(),
        admission: session
            .runtime_metadata
            .as_ref()
            .and_then(|metadata| metadata.session_inbox_admission.clone()),
    })?;
    let messages = session
        .messages
        .iter()
        .map(canonical)
        .collect::<Result<Vec<_>>>()?;
    let previous = message_rows(tx, &session.id)?;
    let history_changed =
        previous != messages || existing.as_ref().is_none_or(|stored| stored.proof != proof);
    let runtime_changed = existing
        .as_ref()
        .is_none_or(|stored| stored.runtime != runtime);
    let revision = match &existing {
        Some(stored) => V3Revision {
            history: if history_changed {
                next_revision(stored.revision.history)?
            } else {
                stored.revision.history
            },
            runtime: if runtime_changed {
                next_revision(stored.revision.runtime)?
            } else {
                stored.revision.runtime
            },
        },
        None => V3Revision {
            history: 1,
            runtime: 1,
        },
    };
    if history_changed || runtime_changed {
        tx.execute("INSERT INTO sessions(session_id,root_id,created_at,history_rev,runtime_rev,runtime_json,proof_json) VALUES(?1,?2,?3,?4,?5,?6,?7)
            ON CONFLICT(session_id) DO UPDATE SET history_rev=excluded.history_rev,runtime_rev=excluded.runtime_rev,runtime_json=excluded.runtime_json,proof_json=excluded.proof_json",
            params![session.id, root_id(session), session.created_at.to_rfc3339(), revision.history, revision.runtime, runtime, proof])?;
    }
    for (ordinal, payload) in messages.iter().enumerate() {
        if previous.get(ordinal) != Some(payload) {
            tx.execute("INSERT INTO messages(session_id,ordinal,payload) VALUES(?1,?2,?3) ON CONFLICT(session_id,ordinal) DO UPDATE SET payload=excluded.payload",
                params![session.id, ordinal as i64, payload])?;
        }
    }
    if previous.len() > messages.len() {
        tx.execute(
            "DELETE FROM messages WHERE session_id=?1 AND ordinal>=?2",
            params![session.id, messages.len() as i64],
        )?;
    }
    Ok(revision)
}
fn validate_tree(sessions: &[Session]) -> Result<()> {
    let Some(first) = sessions.first() else {
        return Err(V3Error::Invalid("empty tree".into()));
    };
    let root = root_id(first);
    let mut ids = HashSet::new();
    for session in sessions {
        validate_session(session)?;
        if root_id(session) != root || !ids.insert(session.id.as_str()) {
            return Err(V3Error::Invalid("mixed roots or duplicate sessions".into()));
        }
    }
    if !ids.contains(root) {
        return Err(V3Error::Invalid("missing root".into()));
    }
    for session in sessions {
        let mut seen = HashSet::new();
        let mut current = session;
        loop {
            if !seen.insert(current.id.as_str()) {
                return Err(V3Error::Invalid("parent cycle".into()));
            }
            if current.id == root {
                if current.parent_session_id.is_some()
                    || current.kind != bamboo_domain::SessionKind::Root
                {
                    return Err(V3Error::Invalid("invalid root".into()));
                }
                break;
            }
            let parent = current
                .parent_session_id
                .as_deref()
                .ok_or_else(|| V3Error::Invalid("missing parent".into()))?;
            current = sessions
                .iter()
                .find(|candidate| candidate.id == parent)
                .ok_or_else(|| V3Error::Invalid("parent outside tree".into()))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
