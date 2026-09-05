use super::{to_io_error, Connection, OptionalExtension};

const VERSION: &str = "4";
const SESSION_COLUMNS: &str =
    "session_id, title, kind, root_session_id, parent_session_id, pinned, updated_at, summary";
const MESSAGE_COLUMNS: &str =
    "session_id, message_id, message_index, role, content, compressed, created_at";
const SESSION_FIELDS: &str = "session_id TEXT NOT NULL UNIQUE, title TEXT NOT NULL,
    kind TEXT NOT NULL, root_session_id TEXT NOT NULL, parent_session_id TEXT,
    pinned INTEGER NOT NULL, updated_at TEXT NOT NULL, summary TEXT";
const MESSAGE_FIELDS: &str = "session_id TEXT NOT NULL, message_id TEXT NOT NULL,
    message_index INTEGER NOT NULL, role TEXT NOT NULL, content TEXT NOT NULL,
    compressed INTEGER NOT NULL, created_at TEXT NOT NULL, UNIQUE(session_id, message_id)";
const FTS_SCHEMA: &str = "
    CREATE VIRTUAL TABLE IF NOT EXISTS sessions_search_fts USING fts5(
        session_id UNINDEXED, title, summary
    );
    CREATE VIRTUAL TABLE IF NOT EXISTS session_messages_search_fts USING fts5(
        session_id UNINDEXED, message_id UNINDEXED, message_index UNINDEXED,
        role UNINDEXED, content
    );";

fn sql_error(error: rusqlite::Error) -> std::io::Error {
    to_io_error(format!("sqlite search schema failed: {error}"))
}

fn create_table(conn: &Connection, name: &str, fields: &str) -> std::io::Result<()> {
    // Names/fields are static application definitions, never database input.
    conn.execute_batch(&format!(
        "CREATE TABLE {name} (search_rowid INTEGER PRIMARY KEY, {fields});"
    ))
    .map_err(sql_error)
}

fn validate_fts(conn: &Connection) -> std::io::Result<()> {
    for (table, expected, definition) in [
        (
            "sessions_search_fts",
            "session_id,title,summary",
            "session_idunindexed,title,summary",
        ),
        (
            "session_messages_search_fts",
            "session_id,message_id,message_index,role,content",
            "session_idunindexed,message_idunindexed,message_indexunindexed,roleunindexed,content",
        ),
    ] {
        let sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_schema WHERE name=?1",
                [table],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        let columns = conn
            .prepare(&format!("PRAGMA table_info({table})"))
            .map_err(sql_error)?
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        let compact_sql: String = sql
            .to_ascii_lowercase()
            .chars()
            .filter(|ch| !ch.is_whitespace())
            .collect();
        if columns.join(",") != expected
            || !compact_sql.contains(&format!("usingfts5({definition})"))
        {
            return Err(to_io_error(format!(
                "unsupported search FTS shape: {table}"
            )));
        }
    }
    Ok(())
}

fn validate_columns(
    conn: &Connection,
    table: &str,
    columns: &str,
    stable_ids: bool,
) -> std::io::Result<()> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .map_err(sql_error)?;
    let actual = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(5)?,
                row.get::<_, bool>(3)?,
            ))
        })
        .map_err(sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sql_error)?;
    let expected = columns.split(", ").collect::<Vec<_>>();
    let offset = usize::from(stable_ids);
    if actual.len() != expected.len() + offset
        || !actual[offset..]
            .iter()
            .map(|column| column.0.as_str())
            .eq(expected)
        || (stable_ids
            && (actual[0].0 != "search_rowid"
                || !actual[0].1.eq_ignore_ascii_case("INTEGER")
                || actual[0].2 != 1))
    {
        return Err(to_io_error(format!(
            "unsupported search index table shape: {table}"
        )));
    }
    for (name, kind, primary_key, not_null) in &actual[offset..] {
        let expected_type = match name.as_str() {
            "pinned" | "message_index" | "compressed" => "INTEGER",
            _ => "TEXT",
        };
        let expected_pk = if stable_ids {
            0
        } else {
            match name.as_str() {
                "session_id" => 1,
                "message_id" => 2,
                _ => 0,
            }
        };
        let expected_not_null = !matches!(name.as_str(), "parent_session_id" | "summary")
            && (stable_ids || table != "sessions_search" || name != "session_id");
        if !kind.eq_ignore_ascii_case(expected_type)
            || *primary_key != expected_pk
            || *not_null != expected_not_null
        {
            return Err(to_io_error(format!(
                "unsupported search column: {table}.{name}"
            )));
        }
    }
    if stable_ids {
        let unique_keys = conn
            .prepare(&format!("PRAGMA index_list({table})"))
            .map_err(sql_error)?
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(1)?,
                    row.get::<_, bool>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, bool>(4)?,
                ))
            })
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        let expected_key: &[&str] = if table == "sessions_search" {
            &["session_id"]
        } else {
            &["session_id", "message_id"]
        };
        let mut found_key = false;
        for (name, unique, origin, partial) in unique_keys {
            // INTEGER PRIMARY KEY DESC is not a rowid alias and creates a PK
            // index. Reject it (and composite PKs) rather than trust VACUUM.
            if origin == "pk" {
                return Err(to_io_error(format!(
                    "search_rowid must alias rowid: {table}"
                )));
            }
            if unique && !partial {
                let names = conn
                    .prepare("SELECT name FROM pragma_index_info(?1) ORDER BY seqno")
                    .map_err(sql_error)?
                    .query_map([name], |row| row.get::<_, Option<String>>(0))
                    .map_err(sql_error)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(sql_error)?;
                found_key |= names
                    .iter()
                    .map(Option::as_deref)
                    .eq(expected_key.iter().copied().map(Some));
            }
        }
        if !found_key {
            return Err(to_io_error(format!(
                "missing search identity constraint: {table}"
            )));
        }
    }
    Ok(())
}

fn migrate_v3(conn: &Connection) -> std::io::Result<()> {
    validate_columns(conn, "sessions_search", SESSION_COLUMNS, false)?;
    validate_columns(conn, "session_messages_search", MESSAGE_COLUMNS, false)?;

    // DROP TABLE removes its indexes/triggers. Preserve their exact SQL,
    // including user objects; SQLite-owned autoindexes come from constraints.
    let objects = conn
        .prepare(
            "SELECT type, name, sql FROM sqlite_schema WHERE tbl_name IN
            ('sessions_search', 'session_messages_search')
            AND type IN ('index', 'trigger') AND sql IS NOT NULL ORDER BY type, name",
        )
        .map_err(sql_error)?
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sql_error)?;

    // Remove attached triggers together before either table is replaced: a
    // trigger on one table can reference the other during schema validation.
    for (kind, name, _) in &objects {
        if kind == "trigger" {
            conn.execute_batch(&format!("DROP TRIGGER \"{}\"", name.replace('"', "\"\"")))
                .map_err(sql_error)?;
        }
    }
    for (table, fields, columns) in [
        ("sessions_search", SESSION_FIELDS, SESSION_COLUMNS),
        ("session_messages_search", MESSAGE_FIELDS, MESSAGE_COLUMNS),
    ] {
        let replacement = format!("{table}_v4");
        create_table(conn, &replacement, fields)?;
        conn.execute_batch(&format!(
            "INSERT INTO {replacement} (search_rowid, {columns})
                SELECT rowid, {columns} FROM {table};
             DROP TABLE {table};
             ALTER TABLE {replacement} RENAME TO {table};"
        ))
        .map_err(sql_error)?;
    }
    for (_, _, sql) in objects {
        conn.execute_batch(&sql).map_err(sql_error)?;
    }

    // The legacy FTS rowids were independent. Rebuild only this derived cache
    // once so every FTS row uses the corresponding explicit ordinary-table ID.
    conn.execute_batch(FTS_SCHEMA).map_err(sql_error)?;
    validate_fts(conn)?;
    conn.execute_batch(
        "DELETE FROM sessions_search_fts;
         INSERT INTO sessions_search_fts (rowid, session_id, title, summary)
            SELECT search_rowid, session_id, title, COALESCE(summary, '') FROM sessions_search;
         DELETE FROM session_messages_search_fts;
         INSERT INTO session_messages_search_fts
            (rowid, session_id, message_id, message_index, role, content)
            SELECT search_rowid, session_id, message_id, message_index, role, content
            FROM session_messages_search;",
    )
    .map_err(sql_error)
}

pub(super) fn initialize(conn: &mut Connection) -> std::io::Result<()> {
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(sql_error)?;
    let has_meta: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = 'session_search_meta')",
            [],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let version: Option<String> = if has_meta {
        tx.query_row(
            "SELECT value FROM session_search_meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?
    } else {
        None
    };
    match version.as_deref() {
        Some("3") => migrate_v3(&tx)?,
        Some(VERSION) => {
            validate_columns(&tx, "sessions_search", SESSION_COLUMNS, true)?;
            validate_columns(&tx, "session_messages_search", MESSAGE_COLUMNS, true)?;
        }
        None => {
            // Never overwrite an unversioned/partial owned schema as a new DB.
            let existing: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name IN
                    ('sessions_search', 'session_messages_search',
                     'sessions_search_fts', 'session_messages_search_fts'))",
                    [],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            if existing {
                return Err(to_io_error("unversioned existing search index schema"));
            }
            tx.execute_batch(
                "CREATE TABLE IF NOT EXISTS session_search_meta
                (key TEXT PRIMARY KEY, value TEXT NOT NULL);",
            )
            .map_err(sql_error)?;
            create_table(&tx, "sessions_search", SESSION_FIELDS)?;
            create_table(&tx, "session_messages_search", MESSAGE_FIELDS)?;
        }
        Some(unknown) => {
            return Err(to_io_error(format!(
                "unsupported search index schema version: {unknown}"
            )));
        }
    }
    tx.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_session_messages_search_session_id
        ON session_messages_search(session_id, message_index);",
    )
    .map_err(sql_error)?;
    tx.execute_batch(FTS_SCHEMA).map_err(sql_error)?;
    validate_fts(&tx)?;
    tx.execute(
        "INSERT INTO session_search_meta (key, value) VALUES ('schema_version', ?1)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value WHERE value != excluded.value",
        [VERSION],
    )
    .map_err(sql_error)?;
    tx.commit().map_err(sql_error)
}
