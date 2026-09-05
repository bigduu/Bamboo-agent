use std::collections::BTreeMap;

use bamboo_domain::{ConversationSummary, Message};
use rusqlite::types::Value;
use tempfile::TempDir;

use super::*;

fn fixture(id: &str) -> Session {
    let mut session = Session::new(id, "model");
    session.title = "search beacon".to_string();
    for index in 0..3 {
        let mut message = Message::user(format!("quartz message {index}"));
        message.id = format!("m{index}");
        session.messages.push(message);
    }
    session
}

fn rows(conn: &Connection, sql: &str) -> Vec<Vec<Value>> {
    let mut statement = conn.prepare(sql).unwrap();
    let columns = statement.column_count();
    statement
        .query_map([], |row| (0..columns).map(|index| row.get(index)).collect())
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn identity_rows(conn: &Connection) -> BTreeMap<(String, String), i64> {
    conn.prepare("SELECT session_id, message_id, search_rowid FROM session_messages_search")
        .unwrap()
        .query_map([], |row| Ok(((row.get(0)?, row.get(1)?), row.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

// Deliberately independent of schema.rs: this is the actual populated schema
// shipped before this migration, including independent ordinary/FTS rowids.
fn legacy_db(path: &Path) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch(
        "CREATE TABLE session_search_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
         INSERT INTO session_search_meta VALUES ('schema_version', '3');
         CREATE TABLE sessions_search (
            session_id TEXT PRIMARY KEY, title TEXT NOT NULL, kind TEXT NOT NULL,
            root_session_id TEXT NOT NULL, parent_session_id TEXT, pinned INTEGER NOT NULL,
            updated_at TEXT NOT NULL, summary TEXT);
         CREATE TABLE session_messages_search (
            session_id TEXT NOT NULL, message_id TEXT NOT NULL, message_index INTEGER NOT NULL,
            role TEXT NOT NULL, content TEXT NOT NULL, compressed INTEGER NOT NULL,
            created_at TEXT NOT NULL, PRIMARY KEY(session_id, message_id));
         CREATE INDEX idx_session_messages_search_session_id
            ON session_messages_search(session_id, message_index);
         CREATE VIRTUAL TABLE sessions_search_fts USING fts5(session_id UNINDEXED, title, summary);
         CREATE VIRTUAL TABLE session_messages_search_fts USING fts5(
            session_id UNINDEXED, message_id UNINDEXED, message_index UNINDEXED, role UNINDEXED, content);
         CREATE TABLE user_audit (kind TEXT NOT NULL);
         CREATE TABLE unrelated (value TEXT NOT NULL);
         INSERT INTO unrelated VALUES ('preserve this table');",
    ).unwrap();
    let now = Utc::now().to_rfc3339();
    conn.execute(
        "INSERT INTO sessions_search VALUES
        ('legacy', 'legacy beacon', 'child', 'root', 'parent', 1, ?1, 'archived summary')",
        [&now],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO session_messages_search (rowid, session_id, message_id,
        message_index, role, content, compressed, created_at)
        VALUES (7, 'legacy', 'a', 0, 'user', 'quartz legacy message', 1, ?1),
               (29, 'legacy', 'b', 1, 'assistant', 'unrelated answer', 0, ?1)",
        [&now],
    )
    .unwrap();
    conn.execute_batch("INSERT INTO sessions_search_fts (rowid, session_id, title, summary)
        VALUES (900, 'legacy', 'legacy beacon', 'archived summary');
        INSERT INTO session_messages_search_fts (rowid, session_id, message_id, message_index, role, content)
        VALUES (901, 'legacy', 'a', 0, 'user', 'quartz legacy message'),
               (905, 'legacy', 'b', 1, 'assistant', 'unrelated answer');
        CREATE INDEX user_title_index ON sessions_search(title COLLATE NOCASE);
        CREATE UNIQUE INDEX user_message_expression ON session_messages_search(length(content), message_id);
        CREATE TRIGGER user_session_update AFTER UPDATE ON sessions_search BEGIN
            INSERT INTO user_audit VALUES ('session'); END;
        CREATE TRIGGER user_message_update AFTER UPDATE ON session_messages_search BEGIN
            INSERT INTO user_audit SELECT title FROM sessions_search WHERE session_id=NEW.session_id; END;")
        .unwrap();
    conn
}

fn assert_identity_alignment(conn: &Connection) {
    for (normal, fts) in [
        ("sessions_search", "sessions_search_fts"),
        ("session_messages_search", "session_messages_search_fts"),
    ] {
        let mut columns = "session_id".to_string();
        if normal == "session_messages_search" {
            columns.push_str(", message_id");
        }
        assert_eq!(
            rows(
                conn,
                &format!("SELECT search_rowid, {columns} FROM {normal} ORDER BY search_rowid")
            ),
            rows(
                conn,
                &format!("SELECT rowid, {columns} FROM {fts} ORDER BY rowid")
            )
        );
    }
}

#[test]
fn independent_v3_migration_preserves_search_cache_and_user_objects() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("search.db");
    let conn = legacy_db(&path);
    let object_sql = rows(
        &conn,
        "SELECT name, sql FROM sqlite_schema
        WHERE name LIKE 'user_%' OR name='idx_session_messages_search_session_id' ORDER BY name",
    );
    let before_search = serde_json::to_value(search_db(&path, "quartz", 10).unwrap()).unwrap();
    let before_session = serde_json::to_value(search_db(&path, "beacon", 10).unwrap()).unwrap();
    let before_cache =
        serde_json::to_value(read_compressed_cache_db(&path, "legacy", 0, 20, 100).unwrap())
            .unwrap();
    drop(conn);

    for _ in 0..3 {
        init_db(&path).unwrap();
        let conn = open_db(&path).unwrap();
        assert_eq!(
            conn.query_row(
                "SELECT value FROM session_search_meta WHERE key='schema_version'",
                [],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
            "4"
        );
        assert_identity_alignment(&conn);
        assert_eq!(
            identity_rows(&conn).values().copied().collect::<Vec<_>>(),
            vec![7, 29]
        );
        assert_eq!(rows(&conn, "SELECT name, sql FROM sqlite_schema
            WHERE name LIKE 'user_%' OR name='idx_session_messages_search_session_id' ORDER BY name"), object_sql);
        assert_eq!(
            rows(&conn, "SELECT * FROM unrelated"),
            vec![vec![Value::Text("preserve this table".into())]]
        );
        assert_eq!(
            serde_json::to_value(search_db(&path, "quartz", 10).unwrap()).unwrap(),
            before_search
        );
        assert_eq!(
            serde_json::to_value(search_db(&path, "beacon", 10).unwrap()).unwrap(),
            before_session
        );
        assert_eq!(
            serde_json::to_value(read_compressed_cache_db(&path, "legacy", 0, 20, 100).unwrap())
                .unwrap(),
            before_cache
        );
    }
    let conn = open_db(&path).unwrap();
    conn.execute(
        "UPDATE session_messages_search SET compressed=0 WHERE message_id='a'",
        [],
    )
    .unwrap();
    conn.execute("UPDATE sessions_search SET pinned=0", [])
        .unwrap();
    assert_eq!(
        rows(&conn, "SELECT * FROM user_audit ORDER BY rowid"),
        vec![
            vec![Value::Text("legacy beacon".into())],
            vec![Value::Text("session".into())]
        ]
    );
}

#[test]
fn migration_failure_rolls_back_tables_fts_objects_and_version() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("search.db");
    let conn = legacy_db(&path);
    conn.execute_batch(
        "CREATE TRIGGER reject_version BEFORE UPDATE ON session_search_meta BEGIN
        SELECT RAISE(ABORT, 'injected publication failure'); END;",
    )
    .unwrap();
    let schema_before = rows(&conn, "SELECT name, sql FROM sqlite_schema ORDER BY name");
    let fts_before = rows(
        &conn,
        "SELECT rowid, * FROM session_messages_search_fts ORDER BY rowid",
    );
    let normal_before = rows(
        &conn,
        "SELECT rowid, * FROM session_messages_search ORDER BY rowid",
    );
    assert!(init_db(&path)
        .unwrap_err()
        .to_string()
        .contains("injected publication failure"));
    assert_eq!(
        rows(&conn, "SELECT name, sql FROM sqlite_schema ORDER BY name"),
        schema_before
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT rowid, * FROM session_messages_search_fts ORDER BY rowid"
        ),
        fts_before
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT rowid, * FROM session_messages_search ORDER BY rowid"
        ),
        normal_before
    );
    assert!(!search_db(&path, "quartz", 10).unwrap().is_empty());
    conn.execute_batch("DROP TRIGGER reject_version").unwrap();
    init_db(&path).unwrap();
    assert_identity_alignment(&conn);
}

#[test]
fn unsupported_version_and_shape_fail_without_republishing_schema() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("search.db");
    let conn = legacy_db(&path);
    conn.execute("UPDATE session_search_meta SET value='99'", [])
        .unwrap();
    let before = rows(&conn, "SELECT name, sql FROM sqlite_schema ORDER BY name");
    assert!(init_db(&path).unwrap_err().to_string().contains("99"));
    assert_eq!(
        rows(&conn, "SELECT name, sql FROM sqlite_schema ORDER BY name"),
        before
    );
    assert_eq!(
        rows(&conn, "SELECT value FROM session_search_meta"),
        vec![vec![Value::Text("99".into())]]
    );
    conn.execute("UPDATE session_search_meta SET value='4'", [])
        .unwrap();
    assert!(init_db(&path).unwrap_err().to_string().contains("shape"));
    assert_eq!(
        rows(&conn, "SELECT name, sql FROM sqlite_schema ORDER BY name"),
        before
    );
}

fn assert_shape_rejected_without_mutation(path: &Path, conn: &Connection, reason: &str) {
    let snapshot = || {
        [
            "SELECT name, sql FROM sqlite_schema ORDER BY name",
            "SELECT * FROM session_search_meta ORDER BY key",
            "SELECT rowid, * FROM sessions_search ORDER BY rowid",
            "SELECT rowid, * FROM session_messages_search ORDER BY rowid",
            "SELECT rowid, * FROM sessions_search_fts ORDER BY rowid",
            "SELECT rowid, * FROM session_messages_search_fts ORDER BY rowid",
        ]
        .map(|sql| rows(conn, sql))
    };
    let before = snapshot();
    let error = init_db(path).unwrap_err().to_string();
    assert!(error.contains(reason), "{error}");
    assert_eq!(
        snapshot(),
        before,
        "rejection must preserve schema, version and payloads"
    );
}

#[test]
fn v4_rejects_changed_fts_indexing_tokenizer_and_content_mode() {
    for definition in [
        "session_id, title, summary",
        "session_id UNINDEXED, title, summary, tokenize='porter'",
        "session_id UNINDEXED, title, summary, content=''",
    ] {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("search.db");
        init_db(&path).unwrap();
        upsert_session_db(&path, &fixture("fts-shape"), None).unwrap();
        let conn = open_db(&path).unwrap();
        // Keep the same visible columns and populated ordinary rows. A
        // column-name-only check would incorrectly accept all three variants.
        conn.execute_batch(&format!("DROP TABLE sessions_search_fts;
            CREATE VIRTUAL TABLE sessions_search_fts USING fts5({definition});
            INSERT INTO sessions_search_fts(rowid, session_id, title, summary)
                SELECT search_rowid, session_id, title, COALESCE(summary, '') FROM sessions_search;"))
            .unwrap();
        assert_shape_rejected_without_mutation(&path, &conn, "unsupported search FTS shape");
    }
}

#[test]
fn v4_rejects_non_alias_primary_key_and_missing_or_invalid_identity_uniqueness() {
    for (table, from, to, extra_sql, reason) in [
        (
            "sessions_search",
            "INTEGER PRIMARY KEY",
            "INTEGER PRIMARY KEY DESC",
            "",
            "must alias rowid",
        ),
        (
            "sessions_search",
            "TEXT NOT NULL UNIQUE",
            "TEXT NOT NULL",
            "",
            "missing search identity constraint",
        ),
        (
            "sessions_search",
            "TEXT NOT NULL UNIQUE",
            "TEXT NOT NULL",
            "CREATE UNIQUE INDEX partial_identity ON sessions_search(session_id) WHERE pinned=1;",
            "missing search identity constraint",
        ),
        (
            "session_messages_search",
            "UNIQUE(session_id, message_id)",
            "UNIQUE(message_id)",
            "",
            "missing search identity constraint",
        ),
    ] {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("search.db");
        init_db(&path).unwrap();
        upsert_session_db(&path, &fixture("identity-shape"), None).unwrap();
        let conn = open_db(&path).unwrap();
        let original: String = conn
            .query_row(
                "SELECT sql FROM sqlite_schema WHERE name=?1",
                [table],
                |row| row.get(0),
            )
            .unwrap();
        let changed = original.replace(from, to);
        assert_ne!(
            original, changed,
            "fixture must alter the actual v4 definition"
        );
        conn.execute_batch(&format!(
            "CREATE TEMP TABLE saved_rows AS SELECT * FROM {table};
            DROP TABLE {table}; {changed}; INSERT INTO {table} SELECT * FROM saved_rows;
            DROP TABLE saved_rows; {extra_sql}"
        ))
        .unwrap();
        assert_shape_rejected_without_mutation(&path, &conn, reason);
    }
}

fn install_audit(conn: &Connection) {
    conn.execute_batch("CREATE TABLE write_audit (table_name TEXT, operation TEXT)")
        .unwrap();
    for table in ["sessions_search", "session_messages_search"] {
        for operation in ["INSERT", "UPDATE", "DELETE"] {
            conn.execute_batch(&format!(
                "CREATE TRIGGER audit_{table}_{operation} AFTER {operation} ON {table}
                BEGIN INSERT INTO write_audit VALUES ('{table}', '{operation}'); END;"
            ))
            .unwrap();
        }
    }
}

fn apply_checked(path: &Path, session: &Session, expected: delta::Changes) {
    let conn = open_db(path).unwrap();
    conn.execute("DELETE FROM write_audit", []).unwrap();
    let before_ids = identity_rows(&conn);
    assert_eq!(upsert_session_db(path, session, None).unwrap(), expected);
    for (identity, rowid) in identity_rows(&conn) {
        if let Some(previous) = before_ids.get(&identity) {
            assert_eq!(*previous, rowid);
        }
    }
    for (table, count) in [
        ("sessions_search", expected.sessions),
        ("session_messages_search", expected.messages),
    ] {
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM write_audit WHERE table_name=?1",
                [table],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            count as i64
        );
    }
    assert_identity_alignment(&conn);
}

#[test]
fn differential_writes_cover_identical_metadata_append_edit_order_role_and_delete() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("search.db");
    init_db(&path).unwrap();
    let conn = open_db(&path).unwrap();
    install_audit(&conn);
    let mut session = fixture("delta");
    apply_checked(
        &path,
        &session,
        delta::Changes {
            sessions: 1,
            messages: 3,
            session_fts: 1,
            message_fts: 3,
        },
    );
    apply_checked(&path, &session, delta::Changes::default());
    session.title = "updated beacon".into();
    apply_checked(
        &path,
        &session,
        delta::Changes {
            sessions: 1,
            session_fts: 1,
            ..Default::default()
        },
    );
    session.conversation_summary = Some(ConversationSummary::new("summary recollection", 1, 2));
    apply_checked(
        &path,
        &session,
        delta::Changes {
            sessions: 1,
            session_fts: 1,
            ..Default::default()
        },
    );
    session.kind = SessionKind::Child;
    session.root_session_id = "root".into();
    session.parent_session_id = Some("parent".into());
    session.pinned = true;
    session.updated_at += Duration::seconds(1);
    apply_checked(
        &path,
        &session,
        delta::Changes {
            sessions: 1,
            ..Default::default()
        },
    );
    let metadata = search_db(&path, "beacon", 10).unwrap();
    assert!(metadata[0].pinned);
    assert_eq!(metadata[0].parent_session_id.as_deref(), Some("parent"));
    session.messages[0].compressed = true;
    apply_checked(
        &path,
        &session,
        delta::Changes {
            messages: 1,
            ..Default::default()
        },
    );
    session.messages[0].created_at += Duration::seconds(1);
    apply_checked(
        &path,
        &session,
        delta::Changes {
            messages: 1,
            ..Default::default()
        },
    );
    let cache = read_compressed_cache_db(&path, &session.id, 0, 10, 100).unwrap();
    assert_eq!(cache.messages[0].created_at, session.messages[0].created_at);
    let mut appended = Message::assistant("appended zircon", None);
    appended.id = "m3".into();
    session.messages.push(appended);
    apply_checked(
        &path,
        &session,
        delta::Changes {
            messages: 1,
            message_fts: 1,
            ..Default::default()
        },
    );
    session.messages[1].content = "amended sapphire".into();
    apply_checked(
        &path,
        &session,
        delta::Changes {
            messages: 1,
            message_fts: 1,
            ..Default::default()
        },
    );
    session.messages[2].role = Role::Tool;
    apply_checked(
        &path,
        &session,
        delta::Changes {
            messages: 1,
            message_fts: 1,
            ..Default::default()
        },
    );
    session.messages.swap(0, 2);
    apply_checked(
        &path,
        &session,
        delta::Changes {
            messages: 2,
            message_fts: 2,
            ..Default::default()
        },
    );
    let hits = search_db(&path, "quartz", 10).unwrap();
    assert!(hits
        .iter()
        .any(|hit| hit.message_id.as_deref() == Some("m2")
            && hit.message_index == Some(0)
            && hit.role.as_deref() == Some("tool")));
    session.messages.remove(1);
    apply_checked(
        &path,
        &session,
        delta::Changes {
            messages: 3,
            message_fts: 3,
            ..Default::default()
        },
    );
    assert!(search_db(&path, "sapphire", 10).unwrap().is_empty());
    session.messages.clear();
    apply_checked(
        &path,
        &session,
        delta::Changes {
            messages: 3,
            message_fts: 3,
            ..Default::default()
        },
    );
    assert!(search_db(&path, "quartz", 10).unwrap().is_empty());
    apply_checked(&path, &session, delta::Changes::default());
}

#[test]
fn unchanged_projection_repairs_missing_stale_and_null_fts_payloads() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("search.db");
    init_db(&path).unwrap();
    let session = fixture("repair");
    upsert_session_db(&path, &session, None).unwrap();
    let conn = open_db(&path).unwrap();
    install_audit(&conn);
    conn.execute_batch("UPDATE sessions_search_fts SET title='corrupt', summary=NULL;
        DELETE FROM session_messages_search_fts WHERE rowid=1;
        UPDATE session_messages_search_fts SET content='stale payload', role=NULL, message_index=99 WHERE rowid=2;").unwrap();
    apply_checked(
        &path,
        &session,
        delta::Changes {
            session_fts: 1,
            message_fts: 2,
            ..Default::default()
        },
    );
    assert_eq!(search_db(&path, "quartz", 10).unwrap().len(), 3);
    assert!(search_db(&path, "corrupt", 10).unwrap().is_empty());
    assert!(search_db(&path, "stale", 10).unwrap().is_empty());
    conn.execute_batch("DELETE FROM sessions_search_fts; DELETE FROM session_messages_search_fts;")
        .unwrap();
    apply_checked(
        &path,
        &session,
        delta::Changes {
            session_fts: 1,
            message_fts: 3,
            ..Default::default()
        },
    );
    apply_checked(&path, &session, delta::Changes::default());
}

#[test]
fn duplicate_ids_and_fts_payload_failure_leave_entire_snapshot_unchanged() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("search.db");
    init_db(&path).unwrap();
    let session = fixture("rollback");
    upsert_session_db(&path, &session, None).unwrap();
    let conn = open_db(&path).unwrap();
    let normal = rows(
        &conn,
        "SELECT * FROM session_messages_search ORDER BY search_rowid",
    );
    let titles = rows(&conn, "SELECT * FROM sessions_search ORDER BY search_rowid");
    let mut duplicate = session.clone();
    duplicate.title = "must rollback".into();
    duplicate.messages.push(duplicate.messages[0].clone());
    assert!(upsert_session_db(&path, &duplicate, None).is_err());
    assert_eq!(
        rows(
            &conn,
            "SELECT * FROM session_messages_search ORDER BY search_rowid"
        ),
        normal
    );
    assert_eq!(
        rows(&conn, "SELECT * FROM sessions_search ORDER BY search_rowid"),
        titles
    );

    // Substitute a rejecting content table only in this test. The production
    // FTS INSERT executes after ordinary/session-FTS changes and must fail the
    // same encompassing transaction; no FTS shadow-table details are involved.
    conn.execute_batch("ALTER TABLE session_messages_search_fts RENAME TO saved_fts;
        CREATE TABLE session_messages_search_fts (session_id, message_id, message_index, role,
            content CHECK(content NOT LIKE '%rejectpayload%'));
        INSERT INTO session_messages_search_fts (rowid, session_id, message_id, message_index, role, content)
            SELECT rowid, session_id, message_id, message_index, role, content FROM saved_fts;").unwrap();
    let fts_before = rows(
        &conn,
        "SELECT rowid, * FROM session_messages_search_fts ORDER BY rowid",
    );
    let mut changed = session.clone();
    changed.title = "must rollback".into();
    changed.messages[0].content = "rejectpayload".into();
    assert!(upsert_session_db(&path, &changed, None).is_err());
    assert_eq!(
        rows(
            &conn,
            "SELECT * FROM session_messages_search ORDER BY search_rowid"
        ),
        normal
    );
    assert_eq!(
        rows(&conn, "SELECT * FROM sessions_search ORDER BY search_rowid"),
        titles
    );
    assert_eq!(
        rows(
            &conn,
            "SELECT rowid, * FROM session_messages_search_fts ORDER BY rowid"
        ),
        fts_before
    );
    assert!(search_db(&path, "beacon", 1).unwrap()[0].session_title == session.title);
    conn.execute_batch(
        "DROP TABLE session_messages_search_fts;
        ALTER TABLE saved_fts RENAME TO session_messages_search_fts;",
    )
    .unwrap();
    assert_eq!(search_db(&path, "quartz", 10).unwrap().len(), 3);
}

#[test]
fn explicit_ids_survive_vacuum_holes_restart_and_later_delta_writes() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("search.db");
    init_db(&path).unwrap();
    for id in ["hole", "kept", "unrelated"] {
        upsert_session_db(&path, &fixture(id), None).unwrap();
    }
    delete_session_db(&path, "hole", None).unwrap();
    let conn = open_db(&path).unwrap();
    let before = identity_rows(&conn);
    let session_ids = rows(
        &conn,
        "SELECT search_rowid, session_id FROM sessions_search ORDER BY search_rowid",
    );
    conn.execute_batch("VACUUM").unwrap();
    drop(conn);
    init_db(&path).unwrap();
    let conn = open_db(&path).unwrap();
    assert_eq!(identity_rows(&conn), before);
    assert_eq!(
        rows(
            &conn,
            "SELECT search_rowid, session_id FROM sessions_search ORDER BY search_rowid"
        ),
        session_ids
    );
    assert_identity_alignment(&conn);
    let unrelated = rows(
        &conn,
        "SELECT * FROM session_messages_search WHERE session_id='unrelated' ORDER BY search_rowid",
    );
    let mut updated = fixture("kept");
    updated.messages[0].content = "edited postvacuum".into();
    updated.messages.remove(1);
    let mut new = Message::user("added postvacuum");
    new.id = "new".into();
    updated.messages.push(new);
    upsert_session_db(&path, &updated, None).unwrap();
    for (identity, rowid) in identity_rows(&conn) {
        if let Some(previous) = before.get(&identity) {
            assert_eq!(*previous, rowid);
        }
    }
    assert_identity_alignment(&conn);
    assert_eq!(rows(&conn, "SELECT * FROM session_messages_search WHERE session_id='unrelated' ORDER BY search_rowid"), unrelated);
    assert_eq!(search_db(&path, "postvacuum", 10).unwrap().len(), 2);
}

#[test]
fn deletion_and_pruning_use_keyed_fts_access_among_unrelated_sessions() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("search.db");
    init_db(&path).unwrap();
    for index in 0..128 {
        upsert_session_db(&path, &fixture(&format!("s{index}")), None).unwrap();
    }
    let conn = open_db(&path).unwrap();
    for sql in [delta::DELETE_SESSION_FTS, delta::DELETE_MESSAGE_FTS] {
        let plan = conn
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .unwrap()
            .query_map(["s0"], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        println!("Production keyed FTS deletion plan: {plan:?}");
        assert!(
            plan.iter()
                .any(|detail| detail.contains("VIRTUAL TABLE INDEX") && detail.contains('=')),
            "{plan:?}"
        );
        assert!(
            plan.iter().any(|detail| detail.contains("SEARCH")
                && detail.contains("INDEX")
                && detail.contains("session_id=?")),
            "{plan:?}"
        );
    }
    let unrelated = rows(&conn, "SELECT * FROM session_messages_search WHERE session_id NOT IN ('s0', 's1') ORDER BY search_rowid");
    delete_session_db(&path, "s0", None).unwrap();
    conn.execute(
        "UPDATE sessions_search SET updated_at=?1 WHERE session_id='s1'",
        [(Utc::now() - Duration::days(11)).to_rfc3339()],
    )
    .unwrap();
    assert_eq!(prune_stale_sessions_db(&path).unwrap(), 1);
    assert_identity_alignment(&conn);
    assert_eq!(
        rows(
            &conn,
            "SELECT * FROM session_messages_search ORDER BY search_rowid"
        ),
        unrelated
    );
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM sessions_search", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        126
    );
}
