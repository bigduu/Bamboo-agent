use std::collections::{HashMap, HashSet};

use super::{params, Connection, OptionalExtension, Role, Session, SessionKind};

// Counts come from sqlite3_changes for the top-level statements, excluding
// FTS shadow-table internals. They keep differential-write tests independent of
// FTS5's internal storage implementation; callers need no public metrics API.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Changes {
    pub sessions: usize,
    pub messages: usize,
    pub session_fts: usize,
    pub message_fts: usize,
}

pub(super) const DELETE_SESSION_FTS: &str = "DELETE FROM sessions_search_fts WHERE rowid IN
    (SELECT search_rowid FROM sessions_search WHERE session_id = ?1)";
pub(super) const DELETE_MESSAGE_FTS: &str = "DELETE FROM session_messages_search_fts WHERE rowid IN
    (SELECT search_rowid FROM session_messages_search WHERE session_id = ?1)";
const DELETE_MESSAGE_FTS_ROW: &str = "DELETE FROM session_messages_search_fts WHERE rowid = ?1";

#[derive(Debug, PartialEq, Eq)]
struct MessageRow {
    rowid: i64,
    index: i64,
    role: String,
    content: String,
    compressed: bool,
    created_at: String,
}

fn sync_session_row(conn: &Connection, session: &Session) -> rusqlite::Result<Changes> {
    let summary = session
        .conversation_summary
        .as_ref()
        .map(|summary| summary.content.as_str());
    let sessions = conn.execute(
        "INSERT INTO sessions_search
            (session_id, title, kind, root_session_id, parent_session_id, pinned, updated_at, summary)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT(session_id) DO UPDATE SET
            title=excluded.title, kind=excluded.kind, root_session_id=excluded.root_session_id,
            parent_session_id=excluded.parent_session_id, pinned=excluded.pinned,
            updated_at=excluded.updated_at, summary=excluded.summary
         WHERE title IS NOT excluded.title OR kind IS NOT excluded.kind
            OR root_session_id IS NOT excluded.root_session_id
            OR parent_session_id IS NOT excluded.parent_session_id
            OR pinned IS NOT excluded.pinned OR updated_at IS NOT excluded.updated_at
            OR summary IS NOT excluded.summary",
        params![
            session.id,
            session.title,
            match session.kind {
                SessionKind::Root => "root",
                SessionKind::Child => "child",
            },
            session.root_session_id,
            session.parent_session_id,
            session.pinned,
            session.updated_at.to_rfc3339(),
            summary,
        ],
    )?;
    let mut changes = Changes {
        sessions,
        ..Default::default()
    };
    let rowid: i64 = conn.query_row(
        "SELECT search_rowid FROM sessions_search WHERE session_id = ?1",
        [&session.id],
        |row| row.get(0),
    )?;
    let fts_matches = conn
        .query_row(
            "SELECT session_id, title, summary FROM sessions_search_fts WHERE rowid = ?1",
            [rowid],
            |row| {
                Ok(
                    row.get::<_, Option<String>>(0)?.as_deref() == Some(&session.id)
                        && row.get::<_, Option<String>>(1)?.as_deref() == Some(&session.title)
                        && row.get::<_, Option<String>>(2)?.as_deref()
                            == Some(summary.unwrap_or_default()),
                )
            },
        )
        .optional()?
        .unwrap_or(false);
    if !fts_matches {
        changes.session_fts = conn.execute(
            "INSERT OR REPLACE INTO sessions_search_fts (rowid, session_id, title, summary)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                rowid,
                session.id,
                session.title,
                summary.unwrap_or_default()
            ],
        )?;
    }
    Ok(changes)
}

pub(super) fn sync_session(conn: &Connection, session: &Session) -> rusqlite::Result<Changes> {
    let mut ids = HashSet::with_capacity(session.messages.len());
    if session
        .messages
        .iter()
        .any(|message| !ids.insert(&message.id))
    {
        return Err(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY),
            Some("duplicate message ID in search snapshot".to_string()),
        ));
    }
    let mut changes = sync_session_row(conn, session)?;
    let mut stored = conn
        .prepare(
            "SELECT message_id, search_rowid, message_index, role, content, compressed, created_at
            FROM session_messages_search WHERE session_id = ?1",
        )?
        .query_map([&session.id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                MessageRow {
                    rowid: row.get(1)?,
                    index: row.get(2)?,
                    role: row.get(3)?,
                    content: row.get(4)?,
                    compressed: row.get(5)?,
                    created_at: row.get(6)?,
                },
            ))
        })?
        .collect::<rusqlite::Result<HashMap<_, _>>>()?;

    let mut insert = conn.prepare(
        "INSERT INTO session_messages_search
        (session_id, message_id, message_index, role, content, compressed, created_at)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )?;
    let mut update = conn.prepare(
        "UPDATE session_messages_search
        SET message_index=?2, role=?3, content=?4, compressed=?5, created_at=?6
        WHERE search_rowid=?1",
    )?;
    let mut fts_read = conn.prepare(
        "SELECT session_id, message_id, message_index, role, content
        FROM session_messages_search_fts WHERE rowid=?1",
    )?;
    let mut fts_write = conn.prepare(
        "INSERT OR REPLACE INTO session_messages_search_fts
        (rowid, session_id, message_id, message_index, role, content)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )?;
    for (index, message) in session.messages.iter().enumerate() {
        let old = stored.remove(&message.id);
        let mut next = MessageRow {
            rowid: old.as_ref().map_or(0, |row| row.rowid),
            index: index as i64,
            role: match message.role {
                Role::System => "system",
                Role::User => "user",
                Role::Assistant => "assistant",
                Role::Tool => "tool",
            }
            .to_string(),
            content: message.content.clone(),
            compressed: message.compressed,
            created_at: message.created_at.to_rfc3339(),
        };
        if let Some(old) = old {
            if old != next {
                changes.messages += update.execute(params![
                    next.rowid,
                    next.index,
                    next.role,
                    next.content,
                    next.compressed,
                    next.created_at
                ])?;
            }
        } else {
            changes.messages += insert.execute(params![
                session.id,
                message.id,
                next.index,
                next.role,
                next.content,
                next.compressed,
                next.created_at
            ])?;
            next.rowid = conn.last_insert_rowid();
        }
        // Compare the point-addressed FTS projection too. Unchanged normal rows
        // must still repair missing/stale FTS during upsert or startup rebuild.
        let fts_matches = fts_read
            .query_row([next.rowid], |row| {
                Ok(
                    row.get::<_, Option<String>>(0)?.as_deref() == Some(&session.id)
                        && row.get::<_, Option<String>>(1)?.as_deref() == Some(&message.id)
                        && row.get::<_, Option<i64>>(2)? == Some(next.index)
                        && row.get::<_, Option<String>>(3)?.as_deref() == Some(&next.role)
                        && row.get::<_, Option<String>>(4)?.as_deref() == Some(&next.content),
                )
            })
            .optional()?
            .unwrap_or(false);
        if !fts_matches {
            changes.message_fts += fts_write.execute(params![
                next.rowid,
                session.id,
                message.id,
                next.index,
                next.role,
                next.content
            ])?;
        }
    }
    let mut fts_delete = conn.prepare(DELETE_MESSAGE_FTS_ROW)?;
    let mut delete = conn.prepare("DELETE FROM session_messages_search WHERE search_rowid=?1")?;
    for removed in stored.into_values() {
        changes.message_fts += fts_delete.execute([removed.rowid])?;
        changes.messages += delete.execute([removed.rowid])?;
    }
    Ok(changes)
}

pub(super) fn delete_session(conn: &Connection, session_id: &str) -> rusqlite::Result<()> {
    // Resolve FTS rowids through indexed ordinary identity columns before
    // deleting those ordinary rows. UNINDEXED FTS session_id is never scanned.
    conn.execute(DELETE_SESSION_FTS, [session_id])?;
    conn.execute(DELETE_MESSAGE_FTS, [session_id])?;
    conn.execute(
        "DELETE FROM session_messages_search WHERE session_id=?1",
        [session_id],
    )?;
    conn.execute(
        "DELETE FROM sessions_search WHERE session_id=?1",
        [session_id],
    )?;
    Ok(())
}
