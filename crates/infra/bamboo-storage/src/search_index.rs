use std::path::{Path, PathBuf};

use chrono::{DateTime, Duration, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use tokio::task;

use bamboo_domain::{Role, Session, SessionKind};

mod delta;
#[cfg(test)]
mod incremental_tests;
mod schema;

const INDEX_RECENT_DAYS: i64 = 7;
const PURGE_OLDER_THAN_DAYS: i64 = 10;
const VACUUM_MIN_DB_BYTES: u64 = 256 * 1024 * 1024;
const VACUUM_MIN_PURGED_ROWS: usize = 500;

/// How long a contended writer waits for the lock before giving up with
/// `SQLITE_BUSY`. SQLite's default is `0` (fail immediately); a non-zero
/// `busy_timeout` makes writers block-and-retry, which matters here because
/// `upsert_session_db` holds the write lock across a `BEGIN IMMEDIATE`
/// transaction while a concurrent indexer/pruner may also be writing. #357.
const SQLITE_BUSY_TIMEOUT_MS: u64 = 5000;

fn to_io_error(message: impl Into<String>) -> std::io::Error {
    std::io::Error::other(message.into())
}

/// Apply the search index's busy timeout and NORMAL synchronization policy.
///
/// Both settings are per connection, so all readers and writers must open via
/// this helper. WAL remains a persistent database setting applied by init_db.
fn open_db(db_path: &Path) -> std::io::Result<Connection> {
    let conn =
        Connection::open(db_path).map_err(|e| to_io_error(format!("sqlite open failed: {e}")))?;
    conn.busy_timeout(std::time::Duration::from_millis(SQLITE_BUSY_TIMEOUT_MS))
        .map_err(|e| to_io_error(format!("sqlite busy_timeout failed: {e}")))?;
    conn.pragma_update(None, "synchronous", "NORMAL")
        .map_err(|e| to_io_error(format!("sqlite pragma synchronous failed: {e}")))?;
    Ok(conn)
}

#[derive(Debug, Clone)]
pub struct SessionSearchIndex {
    db_path: PathBuf,
}

struct SearchSourceRevision {
    path: PathBuf,
    expected: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionSearchMatch {
    pub match_type: String,
    pub session_id: String,
    pub session_title: String,
    pub session_kind: String,
    pub root_session_id: String,
    pub parent_session_id: Option<String>,
    pub pinned: bool,
    pub updated_at: DateTime<Utc>,
    pub rank: f64,
    pub message_id: Option<String>,
    pub message_index: Option<usize>,
    pub role: Option<String>,
    pub content_preview: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CompressedMessageCacheRow {
    pub message_id: String,
    pub message_index: usize,
    pub role: String,
    pub created_at: DateTime<Utc>,
    pub content: String,
    pub content_len: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionCompressedCacheSnapshot {
    pub session_id: String,
    pub summary: Option<String>,
    pub total_compressed_messages: usize,
    pub offset: usize,
    pub limit: usize,
    pub messages: Vec<CompressedMessageCacheRow>,
}

impl SessionSearchIndex {
    pub fn new(db_path: impl AsRef<Path>) -> Self {
        Self {
            db_path: db_path.as_ref().to_path_buf(),
        }
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub async fn init(&self) -> std::io::Result<()> {
        let db_path = self.db_path.clone();
        task::spawn_blocking(move || init_db(&db_path))
            .await
            .map_err(|error| to_io_error(format!("session search init join error: {error}")))?
    }

    pub async fn upsert_session(&self, session: &Session) -> std::io::Result<()> {
        let db_path = self.db_path.clone();
        let session = session.clone();
        task::spawn_blocking(move || upsert_session_db(&db_path, &session, None).map(|_| ()))
            .await
            .map_err(|error| to_io_error(format!("session search upsert join error: {error}")))?
    }

    pub(crate) async fn upsert_session_if_current(
        &self,
        session: &Session,
        revision_path: &Path,
        expected_revision: &str,
    ) -> std::io::Result<()> {
        let db_path = self.db_path.clone();
        let session = session.clone();
        let revision = SearchSourceRevision {
            path: revision_path.to_path_buf(),
            expected: expected_revision.to_string(),
        };
        task::spawn_blocking(move || {
            upsert_session_db(&db_path, &session, Some(&revision)).map(|_| ())
        })
        .await
        .map_err(|error| {
            to_io_error(format!("guarded session search upsert join error: {error}"))
        })?
    }

    pub async fn delete_session(&self, session_id: &str) -> std::io::Result<()> {
        let db_path = self.db_path.clone();
        let session_id = session_id.to_string();
        task::spawn_blocking(move || delete_session_db(&db_path, &session_id, None))
            .await
            .map_err(|error| to_io_error(format!("session search delete join error: {error}")))?
    }

    pub(crate) async fn delete_session_if_source_missing(
        &self,
        session_id: &str,
        revision_path: &Path,
    ) -> std::io::Result<()> {
        let db_path = self.db_path.clone();
        let session_id = session_id.to_string();
        let revision_path = revision_path.to_path_buf();
        task::spawn_blocking(move || delete_session_db(&db_path, &session_id, Some(&revision_path)))
            .await
            .map_err(|error| {
                to_io_error(format!("guarded session search delete join error: {error}"))
            })?
    }

    pub async fn prune_stale_sessions(&self) -> std::io::Result<usize> {
        let db_path = self.db_path.clone();
        task::spawn_blocking(move || prune_stale_sessions_db(&db_path))
            .await
            .map_err(|error| to_io_error(format!("session search prune join error: {error}")))?
    }

    pub async fn maybe_vacuum_if_needed(&self, purged_rows: usize) -> std::io::Result<bool> {
        let db_path = self.db_path.clone();
        task::spawn_blocking(move || maybe_vacuum_db(&db_path, purged_rows))
            .await
            .map_err(|error| to_io_error(format!("session search vacuum join error: {error}")))?
    }

    pub async fn search(
        &self,
        query: &str,
        limit: usize,
    ) -> std::io::Result<Vec<SessionSearchMatch>> {
        let db_path = self.db_path.clone();
        let query = query.to_string();
        let limit = limit.min(200);
        task::spawn_blocking(move || search_db(&db_path, &query, limit))
            .await
            .map_err(|error| to_io_error(format!("session search query join error: {error}")))?
    }

    pub async fn read_compressed_cache(
        &self,
        session_id: &str,
        offset: usize,
        limit: usize,
        truncate_chars: usize,
    ) -> std::io::Result<SessionCompressedCacheSnapshot> {
        let db_path = self.db_path.clone();
        let session_id = session_id.to_string();
        let offset = offset.min(1_000_000);
        let limit = limit.min(500);
        let truncate_chars = truncate_chars.min(20_000);
        task::spawn_blocking(move || {
            read_compressed_cache_db(&db_path, &session_id, offset, limit, truncate_chars)
        })
        .await
        .map_err(|error| {
            to_io_error(format!("session compressed cache read join error: {error}"))
        })?
    }
}

pub fn should_index_session(updated_at: DateTime<Utc>) -> bool {
    updated_at >= Utc::now() - Duration::days(INDEX_RECENT_DAYS)
}

pub fn should_purge_session(updated_at: DateTime<Utc>) -> bool {
    updated_at < Utc::now() - Duration::days(PURGE_OLDER_THAN_DAYS)
}

fn init_db(db_path: &Path) -> std::io::Result<()> {
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut conn = open_db(db_path)?;
    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(|e| to_io_error(format!("sqlite pragma journal_mode failed: {e}")))?;
    schema::initialize(&mut conn)
}

fn upsert_session_db(
    db_path: &Path,
    session: &Session,
    source_revision: Option<&SearchSourceRevision>,
) -> std::io::Result<delta::Changes> {
    let conn = open_db(db_path)?;
    conn.execute_batch("BEGIN IMMEDIATE TRANSACTION;")
        .map_err(|e| to_io_error(format!("sqlite begin transaction failed: {e}")))?;

    let result = (|| {
        if let Some(source_revision) = source_revision {
            match std::fs::read_to_string(&source_revision.path) {
                Ok(current) if current.trim() == source_revision.expected => {}
                Ok(_) => {
                    conn.execute_batch("COMMIT;").map_err(|e| {
                        to_io_error(format!("sqlite commit superseded no-op failed: {e}"))
                    })?;
                    return Ok(delta::Changes::default());
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    conn.execute_batch("COMMIT;").map_err(|e| {
                        to_io_error(format!("sqlite commit deleted-source no-op failed: {e}"))
                    })?;
                    return Ok(delta::Changes::default());
                }
                Err(error) => return Err(error),
            }
        }

        let indexed_updated_at = conn
            .query_row(
                "SELECT updated_at FROM sessions_search WHERE session_id = ?1",
                params![session.id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|e| to_io_error(format!("sqlite read indexed revision failed: {e}")))?
            .and_then(|value| {
                DateTime::parse_from_rfc3339(&value)
                    .ok()
                    .map(|value| value.with_timezone(&Utc))
            });
        if indexed_updated_at.is_some_and(|updated_at| updated_at > session.updated_at) {
            conn.execute_batch("COMMIT;")
                .map_err(|e| to_io_error(format!("sqlite commit stale no-op failed: {e}")))?;
            return Ok(delta::Changes::default());
        }

        if !should_index_session(session.updated_at) {
            delete_session_rows(&conn, &session.id)?;
            conn.execute_batch("COMMIT;")
                .map_err(|e| to_io_error(format!("sqlite commit expiry delete failed: {e}")))?;
            return Ok(delta::Changes::default());
        }

        let changes = delta::sync_session(&conn, session)
            .map_err(|e| to_io_error(format!("sqlite synchronize session search failed: {e}")))?;

        conn.execute_batch("COMMIT;")
            .map_err(|e| to_io_error(format!("sqlite commit failed: {e}")))?;
        Ok(changes)
    })();

    if result.is_err() {
        let _ = conn.execute_batch("ROLLBACK;");
    }
    result
}

fn delete_session_db(
    db_path: &Path,
    session_id: &str,
    required_missing_revision: Option<&Path>,
) -> std::io::Result<()> {
    let conn = open_db(db_path)?;
    conn.execute_batch("BEGIN IMMEDIATE TRANSACTION;")
        .map_err(|e| to_io_error(format!("sqlite begin delete transaction failed: {e}")))?;
    let result = (|| {
        if let Some(revision_path) = required_missing_revision {
            match std::fs::metadata(revision_path) {
                Ok(_) => {
                    conn.execute_batch("COMMIT;").map_err(|e| {
                        to_io_error(format!("sqlite commit recreated-source no-op failed: {e}"))
                    })?;
                    return Ok(());
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        delete_session_rows(&conn, session_id)?;
        conn.execute_batch("COMMIT;")
            .map_err(|e| to_io_error(format!("sqlite commit delete failed: {e}")))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = conn.execute_batch("ROLLBACK;");
    }
    result
}

fn delete_session_rows(conn: &Connection, session_id: &str) -> std::io::Result<()> {
    delta::delete_session(conn, session_id)
        .map_err(|e| to_io_error(format!("sqlite delete session search rows failed: {e}")))
}

fn prune_stale_sessions_db(db_path: &Path) -> std::io::Result<usize> {
    let conn = open_db(db_path)?;
    let cutoff = (Utc::now() - Duration::days(PURGE_OLDER_THAN_DAYS)).to_rfc3339();
    let mut stmt = conn
        .prepare("SELECT session_id FROM sessions_search WHERE updated_at < ?1")
        .map_err(|e| to_io_error(format!("sqlite prepare prune query failed: {e}")))?;
    let ids = stmt
        .query_map(params![cutoff], |row| row.get::<_, String>(0))
        .map_err(|e| to_io_error(format!("sqlite run prune query failed: {e}")))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| to_io_error(format!("sqlite read prune rows failed: {e}")))?;
    let count = ids.len();
    for id in ids {
        delete_session_db(db_path, &id, None)?;
    }
    Ok(count)
}

fn maybe_vacuum_db(db_path: &Path, purged_rows: usize) -> std::io::Result<bool> {
    if purged_rows < VACUUM_MIN_PURGED_ROWS {
        return Ok(false);
    }
    let size_bytes = std::fs::metadata(db_path)
        .map(|meta| meta.len())
        .unwrap_or(0);
    if size_bytes < VACUUM_MIN_DB_BYTES {
        return Ok(false);
    }

    let conn = open_db(db_path)?;
    conn.execute_batch("VACUUM;")
        .map_err(|e| to_io_error(format!("sqlite vacuum failed: {e}")))?;
    Ok(true)
}

// Keep the bm25 score columns, but order by the qualified hidden rank so
// SQLite can consume FTS5 results in rank order without a temporary sort.
const SESSION_SEARCH_SQL: &str = r#"
SELECT
    session_id,
    title,
    summary,
    bm25(sessions_search_fts) AS rank,
    snippet(sessions_search_fts, 1, '[', ']', '...', 24) AS snippet
FROM sessions_search_fts
WHERE sessions_search_fts MATCH ?1
ORDER BY sessions_search_fts.rank
LIMIT ?2
"#;

const MESSAGE_SEARCH_SQL: &str = r#"
SELECT
    s.session_id,
    s.title,
    s.kind,
    s.root_session_id,
    s.parent_session_id,
    s.pinned,
    s.updated_at,
    bm25(session_messages_search_fts) AS rank,
    m.message_id,
    m.message_index,
    m.role,
    snippet(session_messages_search_fts, 4, '[', ']', '...', 24) AS snippet
FROM session_messages_search_fts
JOIN sessions_search s ON s.session_id = session_messages_search_fts.session_id
JOIN session_messages_search m
  ON m.session_id = session_messages_search_fts.session_id
 AND m.message_id = session_messages_search_fts.message_id
WHERE session_messages_search_fts MATCH ?1
ORDER BY session_messages_search_fts.rank
LIMIT ?2
"#;

fn search_db(
    db_path: &Path,
    query: &str,
    limit: usize,
) -> std::io::Result<Vec<SessionSearchMatch>> {
    let conn = open_db(db_path)?;
    let fts_query = build_fts_query(query);
    let mut matches = Vec::new();

    let mut session_stmt = conn
        .prepare(SESSION_SEARCH_SQL)
        .map_err(|e| to_io_error(format!("sqlite prepare session search failed: {e}")))?;
    let session_rows = session_stmt
        .query_map(params![fts_query, limit as i64], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, f64>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        })
        .map_err(|e| to_io_error(format!("sqlite run session search failed: {e}")))?;
    for row in session_rows {
        let (session_id, title, rank, snippet) =
            row.map_err(|e| to_io_error(format!("sqlite read session match failed: {e}")))?;
        if let Some(meta) = conn
            .query_row(
                r#"
                SELECT kind, root_session_id, parent_session_id, pinned, updated_at
                FROM sessions_search
                WHERE session_id = ?1
                "#,
                params![session_id],
                |row| {
                    let updated_at_raw: String = row.get(4)?;
                    let updated_at = chrono::DateTime::parse_from_rfc3339(&updated_at_raw)
                        .map(|dt| dt.with_timezone(&Utc))
                        .map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                4,
                                rusqlite::types::Type::Text,
                                Box::new(error),
                            )
                        })?;
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, i64>(3)? != 0,
                        updated_at,
                    ))
                },
            )
            .optional()
            .map_err(|e| to_io_error(format!("sqlite lookup session metadata failed: {e}")))?
        {
            matches.push(SessionSearchMatch {
                match_type: "session".to_string(),
                session_id,
                session_title: title,
                session_kind: meta.0,
                root_session_id: meta.1,
                parent_session_id: meta.2,
                pinned: meta.3,
                updated_at: meta.4,
                rank,
                message_id: None,
                message_index: None,
                role: None,
                content_preview: snippet,
            });
        }
    }

    if matches.len() < limit {
        let remaining = limit - matches.len();
        let mut message_stmt = conn
            .prepare(MESSAGE_SEARCH_SQL)
            .map_err(|e| to_io_error(format!("sqlite prepare message search failed: {e}")))?;
        let message_rows = message_stmt
            .query_map(params![build_fts_query(query), remaining as i64], |row| {
                let updated_at_raw: String = row.get(6)?;
                let updated_at = chrono::DateTime::parse_from_rfc3339(&updated_at_raw)
                    .map(|dt| dt.with_timezone(&Utc))
                    .map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            6,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?;
                Ok(SessionSearchMatch {
                    match_type: "message".to_string(),
                    session_id: row.get(0)?,
                    session_title: row.get(1)?,
                    session_kind: row.get(2)?,
                    root_session_id: row.get(3)?,
                    parent_session_id: row.get(4)?,
                    pinned: row.get::<_, i64>(5)? != 0,
                    updated_at,
                    rank: row.get::<_, f64>(7)?,
                    message_id: row.get(8)?,
                    message_index: row.get::<_, i64>(9).ok().map(|value| value as usize),
                    role: row.get(10)?,
                    content_preview: row.get::<_, Option<String>>(11)?,
                })
            })
            .map_err(|e| to_io_error(format!("sqlite run message search failed: {e}")))?;
        for row in message_rows {
            matches.push(
                row.map_err(|e| to_io_error(format!("sqlite read message match failed: {e}")))?,
            );
        }
    }

    Ok(matches)
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    let mut iter = value.chars();
    let mut out = String::new();
    for _ in 0..max_chars {
        let Some(ch) = iter.next() else {
            return value.to_string();
        };
        out.push(ch);
    }
    if iter.next().is_some() {
        out.push_str("...");
    }
    out
}

fn read_compressed_cache_db(
    db_path: &Path,
    session_id: &str,
    offset: usize,
    limit: usize,
    truncate_chars_limit: usize,
) -> std::io::Result<SessionCompressedCacheSnapshot> {
    let conn = open_db(db_path)?;

    let summary = conn
        .query_row(
            "SELECT summary FROM sessions_search WHERE session_id = ?1",
            params![session_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()
        .map_err(|e| to_io_error(format!("sqlite load summary failed: {e}")))?
        .flatten();

    let total_compressed_messages: usize = conn
        .query_row(
            "SELECT COUNT(*) FROM session_messages_search WHERE session_id = ?1 AND compressed = 1",
            params![session_id],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(|e| to_io_error(format!("sqlite count compressed rows failed: {e}")))?
        .unwrap_or(0)
        .max(0) as usize;

    if total_compressed_messages == 0 || limit == 0 {
        return Ok(SessionCompressedCacheSnapshot {
            session_id: session_id.to_string(),
            summary,
            total_compressed_messages,
            offset,
            limit,
            messages: Vec::new(),
        });
    }

    let mut stmt = conn
        .prepare(
            r#"
            SELECT message_id, message_index, role, content, created_at
            FROM session_messages_search
            WHERE session_id = ?1 AND compressed = 1
            ORDER BY message_index ASC
            LIMIT ?2 OFFSET ?3
            "#,
        )
        .map_err(|e| to_io_error(format!("sqlite prepare compressed rows failed: {e}")))?;

    let rows = stmt
        .query_map(params![session_id, limit as i64, offset as i64], |row| {
            let created_at_raw: String = row.get(4)?;
            let created_at = chrono::DateTime::parse_from_rfc3339(&created_at_raw)
                .map(|dt| dt.with_timezone(&Utc))
                .map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        4,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?;
            let content: String = row.get(3)?;
            let content_len = content.chars().count();
            Ok(CompressedMessageCacheRow {
                message_id: row.get(0)?,
                message_index: row.get::<_, i64>(1)?.max(0) as usize,
                role: row.get(2)?,
                created_at,
                content: truncate_chars(&content, truncate_chars_limit),
                content_len,
            })
        })
        .map_err(|e| to_io_error(format!("sqlite run compressed rows query failed: {e}")))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| to_io_error(format!("sqlite read compressed rows failed: {e}")))?;

    Ok(SessionCompressedCacheSnapshot {
        session_id: session_id.to_string(),
        summary,
        total_compressed_messages,
        offset,
        limit,
        messages: rows,
    })
}

fn build_fts_query(query: &str) -> String {
    let parts = query
        .split_whitespace()
        .filter_map(|part| {
            let cleaned = part
                .trim()
                .trim_matches(|ch: char| {
                    !ch.is_alphanumeric() && ch != '_' && ch != '-' && ch != '/'
                })
                .replace('"', "");
            if cleaned.is_empty() {
                None
            } else {
                Some(format!("{}*", cleaned))
            }
        })
        .collect::<Vec<_>>();

    if parts.is_empty() {
        query.trim().to_string()
    } else {
        parts.join(" ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::{ConversationSummary, Message};
    use tempfile::TempDir;

    fn sample_session() -> Session {
        let mut session = Session::new("session-1", "gpt-4o-mini");
        session.title = "Context Compression Investigation".to_string();
        session.add_message(Message::system("system"));
        session.add_message(Message::user("Investigate SQLite FTS search integration"));
        session.add_message(Message::assistant(
            "Plan: index session history into SQLite and enable search recall.",
            None,
        ));
        session
    }

    #[test]
    fn open_db_sets_busy_timeout_and_normal_synchronization() {
        // #357: every connection must carry a non-zero busy_timeout so a contended
        // writer blocks-and-retries instead of failing immediately with SQLITE_BUSY.
        let temp = TempDir::new().expect("tempdir");
        let conn = open_db(&temp.path().join("search.db")).expect("open");
        let timeout: i64 = conn
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
            .expect("read busy_timeout");
        assert_eq!(timeout, SQLITE_BUSY_TIMEOUT_MS as i64);
        let synchronous: i64 = conn
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .expect("read synchronous");
        assert_eq!(synchronous, 1, "fresh connections use NORMAL");
        conn.pragma_update(None, "synchronous", "FULL").unwrap();
        drop(conn);
        let reopened = open_db(&temp.path().join("search.db")).unwrap();
        let synchronous: i64 = reopened
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .unwrap();
        assert_eq!(synchronous, 1, "reopened connections reapply NORMAL");
    }

    #[tokio::test]
    async fn populated_search_index_retains_data_and_connection_policy_across_init() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("search.db");
        let index = SessionSearchIndex::new(&path);
        index.init().await.unwrap();
        index.upsert_session(&sample_session()).await.unwrap();
        // Existing files use the same schema. Simulate the old opener on a
        // populated database before reopening through the per-connection policy.
        let legacy_connection = Connection::open(&path).unwrap();
        legacy_connection
            .pragma_update(None, "synchronous", "FULL")
            .unwrap();
        drop(legacy_connection);
        let title_before =
            serde_json::to_value(index.search("Compression", 10).await.unwrap()).unwrap();
        let message_before =
            serde_json::to_value(index.search("SQLite", 10).await.unwrap()).unwrap();
        for _ in 0..2 {
            index
                .init()
                .await
                .expect("existing index initialization remains idempotent");
            let connection = open_db(&path).unwrap();
            let timeout: i64 = connection
                .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
                .unwrap();
            let synchronous: i64 = connection
                .query_row("PRAGMA synchronous", [], |row| row.get(0))
                .unwrap();
            let journal: String = connection
                .query_row("PRAGMA journal_mode", [], |row| row.get(0))
                .unwrap();
            assert_eq!(timeout, 5000);
            assert_eq!(synchronous, 1);
            assert_eq!(journal, "wal");
            drop(connection);
            assert_eq!(
                serde_json::to_value(index.search("Compression", 10).await.unwrap()).unwrap(),
                title_before
            );
            assert_eq!(
                serde_json::to_value(index.search("SQLite", 10).await.unwrap()).unwrap(),
                message_before
            );
        }
    }

    #[tokio::test]
    async fn search_index_can_find_session_and_message_content() {
        let temp = TempDir::new().expect("tempdir");
        let index = SessionSearchIndex::new(temp.path().join("search.db"));
        index.init().await.expect("init");

        let session = sample_session();
        index.upsert_session(&session).await.expect("upsert");

        let title_matches = index.search("Compression", 10).await.expect("search title");
        assert!(!title_matches.is_empty());
        assert!(title_matches.iter().any(|m| m.session_id == session.id));

        let message_matches = index.search("SQLite", 10).await.expect("search message");
        assert!(!message_matches.is_empty());
        assert!(message_matches
            .iter()
            .any(|m| m.match_type == "message" || m.match_type == "session"));
    }

    async fn populate_rank_fixture(index: &SessionSearchIndex) -> Vec<Session> {
        let mut sessions = Vec::new();
        for (id, title, contents) in [
            ("rank-a", "nebula nebula nebula", vec!["nebula context"]),
            (
                "rank-b",
                "nebula",
                vec!["nebula sparse context", "messageonly token"],
            ),
            ("rank-c", "nebula", vec!["messageonly token"]),
            (
                "rank-d",
                "background",
                vec!["nebula nebula nebula", "messageonly token"],
            ),
        ] {
            let mut session = Session::new(id, "fixture-model");
            session.title = title.to_string();
            if id == "rank-c" {
                session.kind = SessionKind::Child;
                session.root_session_id = "rank-a".to_string();
                session.parent_session_id = Some("rank-a".to_string());
                session.pinned = true;
            }
            for (message_index, content) in contents.into_iter().enumerate() {
                let mut message = if message_index % 2 == 0 {
                    Message::user(content)
                } else {
                    Message::assistant(content, None)
                };
                message.id = format!("{id}-message-{message_index}");
                session.add_message(message);
            }
            index.upsert_session(&session).await.unwrap();
            sessions.push(session);
        }
        sessions
    }

    #[tokio::test]
    async fn production_search_queries_use_native_rank_without_temporary_order_sort() {
        let temp = TempDir::new().unwrap();
        let index = SessionSearchIndex::new(temp.path().join("search.db"));
        index.init().await.unwrap();
        populate_rank_fixture(&index).await;
        let connection = open_db(index.db_path()).unwrap();
        for sql in [SESSION_SEARCH_SQL, MESSAGE_SEARCH_SQL] {
            let mut statement = connection
                .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                .unwrap();
            let plan = statement
                .query_map(params![build_fts_query("nebula"), 10], |row| {
                    row.get::<_, String>(3)
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert!(
                !plan
                    .iter()
                    .any(|step| step.contains("TEMP B-TREE") && step.contains("ORDER BY")),
                "production FTS ordering must not require a temporary ORDER BY tree: {plan:?}"
            );
        }
    }

    #[tokio::test]
    async fn native_rank_preserves_bm25_scores_snippets_metadata_and_session_first_results() {
        use std::collections::HashMap;

        let temp = TempDir::new().unwrap();
        let index = SessionSearchIndex::new(temp.path().join("search.db"));
        index.init().await.unwrap();
        let sessions = populate_rank_fixture(&index).await;
        let connection = open_db(index.db_path()).unwrap();
        for query in ["nebula", "messageonly"] {
            // Use the previous explicit bm25 ordering as the score/snippet
            // oracle, keyed by identity so equal ranks imply no tie order.
            let mut expected = HashMap::new();
            for (sql, table, score_column, snippet_column, message_column) in [
                (SESSION_SEARCH_SQL, "sessions_search_fts", 3, 4, None),
                (
                    MESSAGE_SEARCH_SQL,
                    "session_messages_search_fts",
                    7,
                    11,
                    Some(8),
                ),
            ] {
                let baseline = sql.replace(
                    &format!("ORDER BY {table}.rank"),
                    &format!("ORDER BY bm25({table})"),
                );
                let mut statement = connection.prepare(&baseline).unwrap();
                let rows = statement
                    .query_map(params![build_fts_query(query), 200], |row| {
                        Ok((
                            (
                                row.get::<_, String>(0)?,
                                message_column
                                    .map(|column| row.get::<_, String>(column))
                                    .transpose()?,
                            ),
                            (
                                row.get::<_, f64>(score_column)?,
                                row.get::<_, Option<String>>(snippet_column)?,
                            ),
                        ))
                    })
                    .unwrap();
                for row in rows {
                    let (key, value) = row.unwrap();
                    assert!(expected.insert(key, value).is_none());
                }
            }
            let matches = index.search(query, 200).await.unwrap();
            assert_eq!(matches.len(), expected.len());
            assert_eq!(
                matches
                    .iter()
                    .map(|hit| (hit.session_id.clone(), hit.message_id.clone()))
                    .collect::<std::collections::HashSet<_>>(),
                expected
                    .keys()
                    .cloned()
                    .collect::<std::collections::HashSet<_>>(),
                "all matching identities survive even when bm25 scores tie"
            );
            for hit in &matches {
                let (rank, snippet) = expected
                    .get(&(hit.session_id.clone(), hit.message_id.clone()))
                    .unwrap();
                assert_eq!(hit.rank, *rank);
                assert_eq!(hit.content_preview, *snippet);
                let session = sessions
                    .iter()
                    .find(|session| session.id == hit.session_id)
                    .unwrap();
                assert_eq!(hit.session_title, session.title);
                assert_eq!(
                    hit.session_kind,
                    if session.kind == SessionKind::Child {
                        "child"
                    } else {
                        "root"
                    }
                );
                assert_eq!(hit.root_session_id, session.root_session_id);
                assert_eq!(hit.parent_session_id, session.parent_session_id);
                assert_eq!(hit.pinned, session.pinned);
                assert_eq!(hit.updated_at, session.updated_at);
                if let Some(message_id) = &hit.message_id {
                    let (message_index, message) = session
                        .messages
                        .iter()
                        .enumerate()
                        .find(|(_, message)| &message.id == message_id)
                        .unwrap();
                    assert_eq!(hit.match_type, "message");
                    assert_eq!(hit.message_index, Some(message_index));
                    assert_eq!(
                        hit.role.as_deref(),
                        Some(if message.role == Role::User {
                            "user"
                        } else {
                            "assistant"
                        })
                    );
                } else {
                    assert_eq!(hit.match_type, "session");
                    assert!(hit.message_index.is_none());
                    assert!(hit.role.is_none());
                }
            }
            for match_type in ["session", "message"] {
                let ranks = matches
                    .iter()
                    .filter(|hit| hit.match_type == match_type)
                    .map(|hit| hit.rank)
                    .collect::<Vec<_>>();
                assert!(ranks.windows(2).all(|pair| pair[0] <= pair[1]));
            }
            if query == "nebula" {
                assert!(matches[..3].iter().all(|hit| hit.match_type == "session"));
                assert!(matches[3..].iter().all(|hit| hit.match_type == "message"));
                let tied = matches
                    .iter()
                    .filter(|hit| {
                        hit.match_type == "session"
                            && matches!(hit.session_id.as_str(), "rank-b" | "rank-c")
                    })
                    .collect::<Vec<_>>();
                assert_eq!(tied.len(), 2);
                assert_eq!(tied[0].rank, tied[1].rank);
                for limit in [1, 2, 3, 4] {
                    let limited = index.search(query, limit).await.unwrap();
                    assert_eq!(limited.len(), limit);
                    assert!(limited[..limit.min(3)]
                        .iter()
                        .all(|hit| hit.match_type == "session"));
                    if limit > 3 {
                        assert_eq!(limited[3].match_type, "message");
                    }
                }
            } else {
                assert_eq!(matches.len(), 3);
                assert!(matches.iter().all(|hit| hit.match_type == "message"));
                assert!(matches.iter().all(|hit| hit.rank == matches[0].rank));
            }
            assert!(index.search(query, 0).await.unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn native_rank_preserves_result_limit_cap_for_sessions_and_messages() {
        let temp = TempDir::new().unwrap();
        let index = SessionSearchIndex::new(temp.path().join("search.db"));
        index.init().await.unwrap();
        for id in 0..205 {
            let mut session = Session::new(format!("capacity-{id}"), "fixture-model");
            session.title = "titlecapacity".to_string();
            session.add_message(Message::user("messagecapacity"));
            index.upsert_session(&session).await.unwrap();
        }
        for (query, match_type) in [("titlecapacity", "session"), ("messagecapacity", "message")] {
            let rows = index.search(query, usize::MAX).await.unwrap();
            assert_eq!(rows.len(), 200);
            assert!(rows.iter().all(|hit| hit.match_type == match_type));
            let identities = rows
                .iter()
                .map(|hit| (&hit.session_id, &hit.message_id))
                .collect::<std::collections::HashSet<_>>();
            assert_eq!(identities.len(), 200);
            // All scores tie; the contract limits the count, not which tied
            // identities SQLite happens to place on either side of the cutoff.
            assert!(rows.iter().all(|hit| hit.rank == rows[0].rank));
            assert!(index.search(query, 0).await.unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn search_index_delete_session_removes_results() {
        let temp = TempDir::new().expect("tempdir");
        let index = SessionSearchIndex::new(temp.path().join("search.db"));
        index.init().await.expect("init");

        let session = sample_session();
        index.upsert_session(&session).await.expect("upsert");
        assert!(!index
            .search("Compression", 10)
            .await
            .expect("pre-search")
            .is_empty());

        index.delete_session(&session.id).await.expect("delete");
        assert!(index
            .search("Compression", 10)
            .await
            .expect("post-search")
            .is_empty());
    }

    #[tokio::test]
    async fn search_index_ignores_superseded_upserts() {
        let temp = TempDir::new().expect("tempdir");
        let index = SessionSearchIndex::new(temp.path().join("search.db"));
        index.init().await.expect("init");

        let mut newest = sample_session();
        newest.title = "Newest searchable revision".to_string();
        newest.updated_at = Utc::now();
        index.upsert_session(&newest).await.expect("newest upsert");

        let mut stale = newest.clone();
        stale.title = "Superseded searchable revision".to_string();
        stale.updated_at = newest.updated_at - Duration::days(8);
        index.upsert_session(&stale).await.expect("stale no-op");

        let newest_matches = index.search("Newest", 10).await.expect("search newest");
        assert!(newest_matches
            .iter()
            .any(|entry| entry.session_id == newest.id));
        let stale_matches = index.search("Superseded", 10).await.expect("search stale");
        assert!(stale_matches
            .iter()
            .all(|entry| entry.session_id != newest.id));
    }

    #[test]
    fn recent_window_policy_works() {
        assert!(should_index_session(Utc::now() - Duration::days(3)));
        assert!(!should_index_session(Utc::now() - Duration::days(8)));
        assert!(should_purge_session(Utc::now() - Duration::days(11)));
        assert!(!should_purge_session(Utc::now() - Duration::days(5)));
    }

    #[tokio::test]
    async fn read_compressed_cache_returns_summary_and_compressed_rows() {
        let temp = TempDir::new().expect("tempdir");
        let index = SessionSearchIndex::new(temp.path().join("search.db"));
        index.init().await.expect("init");

        let mut session = sample_session();
        session.conversation_summary = Some(ConversationSummary::new(
            "compressed summary for recall",
            2,
            30,
        ));
        session.add_message(Message::user("older user detail"));
        session.add_message(Message::assistant("older assistant detail", None));
        if let Some(message) = session.messages.get_mut(1) {
            message.compressed = true;
        }
        if let Some(message) = session.messages.get_mut(2) {
            message.compressed = true;
        }

        index.upsert_session(&session).await.expect("upsert");

        let snapshot = index
            .read_compressed_cache(&session.id, 0, 10, 200)
            .await
            .expect("read compressed cache");
        assert_eq!(snapshot.session_id, session.id);
        assert_eq!(
            snapshot.summary.as_deref(),
            Some("compressed summary for recall")
        );
        assert_eq!(snapshot.total_compressed_messages, 2);
        assert_eq!(snapshot.messages.len(), 2);
        assert!(snapshot.messages[0].content_len > 0);
    }
}
