use std::path::{Path, PathBuf};

use chrono::{DateTime, Duration, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use tokio::task;

use bamboo_domain::{Role, Session, SessionKind};

const INDEX_RECENT_DAYS: i64 = 7;
const PURGE_OLDER_THAN_DAYS: i64 = 10;
const VACUUM_MIN_DB_BYTES: u64 = 256 * 1024 * 1024;
const VACUUM_MIN_PURGED_ROWS: usize = 500;

fn to_io_error(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, message.into())
}

#[derive(Debug, Clone)]
pub struct SessionSearchIndex {
    db_path: PathBuf,
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
        task::spawn_blocking(move || upsert_session_db(&db_path, &session))
            .await
            .map_err(|error| to_io_error(format!("session search upsert join error: {error}")))?
    }

    pub async fn delete_session(&self, session_id: &str) -> std::io::Result<()> {
        let db_path = self.db_path.clone();
        let session_id = session_id.to_string();
        task::spawn_blocking(move || delete_session_db(&db_path, &session_id))
            .await
            .map_err(|error| to_io_error(format!("session search delete join error: {error}")))?
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
    let conn =
        Connection::open(db_path).map_err(|e| to_io_error(format!("sqlite open failed: {e}")))?;
    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(|e| to_io_error(format!("sqlite pragma journal_mode failed: {e}")))?;
    conn.pragma_update(None, "synchronous", "NORMAL")
        .map_err(|e| to_io_error(format!("sqlite pragma synchronous failed: {e}")))?;
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS session_search_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sessions_search (
            session_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            kind TEXT NOT NULL,
            root_session_id TEXT NOT NULL,
            parent_session_id TEXT,
            pinned INTEGER NOT NULL,
            updated_at TEXT NOT NULL,
            summary TEXT
        );

        CREATE TABLE IF NOT EXISTS session_messages_search (
            session_id TEXT NOT NULL,
            message_id TEXT NOT NULL,
            message_index INTEGER NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            compressed INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (session_id, message_id)
        );

        CREATE INDEX IF NOT EXISTS idx_session_messages_search_session_id
        ON session_messages_search(session_id, message_index);

        CREATE VIRTUAL TABLE IF NOT EXISTS sessions_search_fts USING fts5(
            session_id UNINDEXED,
            title,
            summary
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS session_messages_search_fts USING fts5(
            session_id UNINDEXED,
            message_id UNINDEXED,
            message_index UNINDEXED,
            role UNINDEXED,
            content
        );
        "#,
    )
    .map_err(|e| to_io_error(format!("sqlite schema init failed: {e}")))?;
    conn.execute(
        r#"INSERT INTO session_search_meta (key, value) VALUES ('schema_version', '3')
           ON CONFLICT(key) DO UPDATE SET value = excluded.value"#,
        [],
    )
    .map_err(|e| to_io_error(format!("sqlite meta init failed: {e}")))?;
    Ok(())
}

fn upsert_session_db(db_path: &Path, session: &Session) -> std::io::Result<()> {
    if !should_index_session(session.updated_at) {
        return delete_session_db(db_path, &session.id);
    }

    let conn =
        Connection::open(db_path).map_err(|e| to_io_error(format!("sqlite open failed: {e}")))?;
    conn.execute_batch("BEGIN IMMEDIATE TRANSACTION;")
        .map_err(|e| to_io_error(format!("sqlite begin transaction failed: {e}")))?;

    let result = (|| {
        conn.execute(
            r#"
            INSERT INTO sessions_search (
                session_id, title, kind, root_session_id, parent_session_id, pinned, updated_at, summary
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(session_id) DO UPDATE SET
                title = excluded.title,
                kind = excluded.kind,
                root_session_id = excluded.root_session_id,
                parent_session_id = excluded.parent_session_id,
                pinned = excluded.pinned,
                updated_at = excluded.updated_at,
                summary = excluded.summary
            "#,
            params![
                session.id,
                session.title,
                match session.kind {
                    SessionKind::Root => "root",
                    SessionKind::Child => "child",
                },
                session.root_session_id,
                session.parent_session_id,
                if session.pinned { 1 } else { 0 },
                session.updated_at.to_rfc3339(),
                session.conversation_summary.as_ref().map(|summary| summary.content.as_str()),
            ],
        )
        .map_err(|e| to_io_error(format!("sqlite upsert session failed: {e}")))?;

        conn.execute(
            "DELETE FROM session_messages_search WHERE session_id = ?1",
            params![session.id],
        )
        .map_err(|e| to_io_error(format!("sqlite delete old message rows failed: {e}")))?;
        conn.execute(
            "DELETE FROM sessions_search_fts WHERE session_id = ?1",
            params![session.id],
        )
        .map_err(|e| to_io_error(format!("sqlite delete old session fts rows failed: {e}")))?;
        conn.execute(
            "DELETE FROM session_messages_search_fts WHERE session_id = ?1",
            params![session.id],
        )
        .map_err(|e| to_io_error(format!("sqlite delete old message fts rows failed: {e}")))?;

        conn.execute(
            "INSERT INTO sessions_search_fts (session_id, title, summary) VALUES (?1, ?2, ?3)",
            params![
                session.id,
                session.title,
                session
                    .conversation_summary
                    .as_ref()
                    .map(|summary| summary.content.as_str())
                    .unwrap_or_default(),
            ],
        )
        .map_err(|e| to_io_error(format!("sqlite insert session fts row failed: {e}")))?;

        for (index, message) in session.messages.iter().enumerate() {
            let role = match message.role {
                Role::System => "system",
                Role::User => "user",
                Role::Assistant => "assistant",
                Role::Tool => "tool",
            };
            conn.execute(
                r#"
                INSERT INTO session_messages_search (
                    session_id, message_id, message_index, role, content, compressed, created_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                "#,
                params![
                    session.id,
                    message.id,
                    index as i64,
                    role,
                    message.content,
                    if message.compressed { 1 } else { 0 },
                    message.created_at.to_rfc3339(),
                ],
            )
            .map_err(|e| to_io_error(format!("sqlite insert message row failed: {e}")))?;
            conn.execute(
                r#"
                INSERT INTO session_messages_search_fts (
                    session_id, message_id, message_index, role, content
                ) VALUES (?1, ?2, ?3, ?4, ?5)
                "#,
                params![session.id, message.id, index as i64, role, message.content],
            )
            .map_err(|e| to_io_error(format!("sqlite insert message fts row failed: {e}")))?;
        }

        conn.execute_batch("COMMIT;")
            .map_err(|e| to_io_error(format!("sqlite commit failed: {e}")))?;
        Ok(())
    })();

    if result.is_err() {
        let _ = conn.execute_batch("ROLLBACK;");
    }
    result
}

fn delete_session_db(db_path: &Path, session_id: &str) -> std::io::Result<()> {
    let conn =
        Connection::open(db_path).map_err(|e| to_io_error(format!("sqlite open failed: {e}")))?;
    conn.execute(
        "DELETE FROM sessions_search WHERE session_id = ?1",
        params![session_id],
    )
    .map_err(|e| to_io_error(format!("sqlite delete session row failed: {e}")))?;
    conn.execute(
        "DELETE FROM session_messages_search WHERE session_id = ?1",
        params![session_id],
    )
    .map_err(|e| to_io_error(format!("sqlite delete message rows failed: {e}")))?;
    conn.execute(
        "DELETE FROM sessions_search_fts WHERE session_id = ?1",
        params![session_id],
    )
    .map_err(|e| to_io_error(format!("sqlite delete session fts row failed: {e}")))?;
    conn.execute(
        "DELETE FROM session_messages_search_fts WHERE session_id = ?1",
        params![session_id],
    )
    .map_err(|e| to_io_error(format!("sqlite delete message fts rows failed: {e}")))?;
    Ok(())
}

fn prune_stale_sessions_db(db_path: &Path) -> std::io::Result<usize> {
    let conn =
        Connection::open(db_path).map_err(|e| to_io_error(format!("sqlite open failed: {e}")))?;
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
        delete_session_db(db_path, &id)?;
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

    let conn =
        Connection::open(db_path).map_err(|e| to_io_error(format!("sqlite open failed: {e}")))?;
    conn.execute_batch("VACUUM;")
        .map_err(|e| to_io_error(format!("sqlite vacuum failed: {e}")))?;
    Ok(true)
}

fn search_db(
    db_path: &Path,
    query: &str,
    limit: usize,
) -> std::io::Result<Vec<SessionSearchMatch>> {
    let conn =
        Connection::open(db_path).map_err(|e| to_io_error(format!("sqlite open failed: {e}")))?;
    let fts_query = build_fts_query(query);
    let mut matches = Vec::new();

    let mut session_stmt = conn
        .prepare(
            r#"
            SELECT
                session_id,
                title,
                summary,
                bm25(sessions_search_fts) AS rank,
                snippet(sessions_search_fts, 1, '[', ']', '...', 24) AS snippet
            FROM sessions_search_fts
            WHERE sessions_search_fts MATCH ?1
            ORDER BY rank
            LIMIT ?2
            "#,
        )
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
            .prepare(
                r#"
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
                ORDER BY rank
                LIMIT ?2
                "#,
            )
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
    let conn =
        Connection::open(db_path).map_err(|e| to_io_error(format!("sqlite open failed: {e}")))?;

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
    use bamboo_domain::{Message, ConversationSummary};
    use tempfile::TempDir;

    fn sample_session() -> Session {
        let mut session = Session::new("session-1", "gpt-4o-mini");
        session.title = "Context Compression Investigation".to_string();
        session.add_message(Message::system("system"));
        session.add_message(Message::user(
            "Investigate SQLite FTS search integration",
        ));
        session.add_message(Message::assistant(
            "Plan: index session history into SQLite and enable search recall.",
            None,
        ));
        session
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
        session.add_message(Message::assistant(
            "older assistant detail",
            None,
        ));
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
