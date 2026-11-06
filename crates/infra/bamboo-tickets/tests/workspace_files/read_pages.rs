use super::*;

#[test]
fn legacy_file_reply_bytes_and_complete_read_request_remain_compatible() {
    let legacy =
        serde_json::json!({"content":"old source","sha256":"a".repeat(64),"artifact":null});
    let reply: FileReply = serde_json::from_value(legacy.clone()).unwrap();
    assert!(reply.page.is_none());
    assert_eq!(
        canonical_bytes(&reply).unwrap(),
        canonical_bytes(&legacy).unwrap()
    );
    let operation = serde_json::json!({"tool":"Read","file_path":"/source"});
    let read: FileOperation = serde_json::from_value(operation.clone()).unwrap();
    assert_eq!(
        canonical_bytes(&read).unwrap(),
        canonical_bytes(&operation).unwrap()
    );
}

pub(super) fn paged(
    path: &Path,
    offset: u64,
    max_bytes: usize,
    expected: Option<String>,
) -> FileOperation {
    FileOperation::ReadPage {
        file_path: path.to_string_lossy().into_owned(),
        offset_bytes: offset,
        max_bytes,
        expected_sha256: expected,
    }
}

fn reconstruct(content: &str, max_bytes: usize) {
    let f = fixture_with_workspace(".bamboo/worktrees/worker", &[]);
    let path = Path::new(&f.workspace.worktree).join("source.tsx");
    fs::write(&path, content).unwrap();
    let before = canonical_bytes(&f.service.published().unwrap().1).unwrap();
    let mut offset = 0;
    let mut expected = None;
    let mut assembled = String::new();
    let mut pages = 0;
    loop {
        let reply = f
            .service
            .workspace_file(
                &f.worker,
                "source-page",
                &paged(&path, offset, max_bytes, expected),
            )
            .unwrap();
        assert!(
            canonical_bytes(&serde_json::json!({"result": &reply}))
                .unwrap()
                .len()
                <= 16384
        );
        assert_eq!(
            reply.sha256,
            content_hash(content.as_bytes()),
            "sha256 identifies the entire file"
        );
        assert!(reply.artifact.is_none());
        let page = reply.page.unwrap();
        let text = reply.content.unwrap();
        assert_eq!(page.offset_bytes, offset);
        assert_eq!(page.total_bytes, content.len() as u64);
        assert_eq!(
            page.start_line,
            content[..offset as usize]
                .bytes()
                .filter(|b| *b == b'\n')
                .count() as u64
                + 1
        );
        if !text.is_empty() {
            assert_eq!(
                page.end_line,
                page.start_line + text.lines().count() as u64 - 1
            );
        }
        assert!(text.len() <= max_bytes);
        assert_eq!(page.end_offset_bytes - offset, text.len() as u64);
        assert_eq!(
            text,
            content[offset as usize..page.end_offset_bytes as usize]
        );
        assembled.push_str(&text);
        pages += 1;
        assert!(pages < 256, "bounded representative fixture makes progress");
        match page.next_offset_bytes {
            Some(next) => {
                assert!(!page.eof);
                assert!(next > offset && next < content.len() as u64);
                assert_eq!(next, page.end_offset_bytes);
                offset = next;
                expected = Some(reply.sha256);
            }
            None => {
                assert!(page.eof);
                assert_eq!(page.end_offset_bytes, content.len() as u64);
                break;
            }
        }
    }
    assert_eq!(assembled, content);
    assert_eq!(
        canonical_bytes(&f.service.published().unwrap().1).unwrap(),
        before
    );
}

#[test]
fn paged_sources_preserve_every_byte_lines_hash_and_encoded_budget() {
    reconstruct(&"\tconst sectionId = \"节日🌿\";\r\n".repeat(6000), 16384);
    reconstruct(&"\\\"\u{0001}".repeat(12000), 16384); // Long line with six-byte JSON escapes.
    reconstruct(&"🌿".repeat(200), 7); // Never split a multibyte character.
    reconstruct("", 1);
    reconstruct("a\nb\nc\n", 1);
}

#[test]
fn changed_source_or_missing_continuation_hash_cannot_mix_file_versions() {
    let f = fixture();
    let path = Path::new(&f.workspace.worktree).join("source.rs");
    fs::write(&path, "old source\n".repeat(2000)).unwrap();
    let first = f
        .service
        .workspace_file(&f.worker, "first", &paged(&path, 0, 12000, None))
        .unwrap();
    let offset = first.page.unwrap().next_offset_bytes.unwrap();
    assert!(matches!(
        f.service
            .workspace_file(&f.worker, "missing", &paged(&path, offset, 12000, None)),
        Err(Error::InvalidTransition(_))
    ));
    let snapshot = canonical_bytes(&f.service.published().unwrap().1).unwrap();
    fs::write(&path, "new source\n".repeat(2000)).unwrap();
    assert!(matches!(
        f.service.workspace_file(
            &f.worker,
            "changed",
            &paged(&path, offset, 12000, Some(first.sha256))
        ),
        Err(Error::RevisionConflict)
    ));
    assert_eq!(
        canonical_bytes(&f.service.published().unwrap().1).unwrap(),
        snapshot
    );
}

#[test]
fn invalid_page_parameters_binary_source_eof_and_file_cap_are_explicit() {
    let f = fixture();
    let path = Path::new(&f.workspace.worktree).join("source.txt");
    fs::write(&path, "🌿\n").unwrap();
    let hash = content_hash("🌿\n".as_bytes());
    for (offset, size, expected) in [
        (0, 0, None),
        (0, 16385, None),
        (0, 1, None),
        (1, 12000, Some(hash.clone())),
        (6, 12000, Some(hash.clone())),
        (READ_SOURCE_BYTES_LIMIT as u64 + 1, 1, Some(hash.clone())),
        (0, 12000, Some("z".repeat(64))),
    ] {
        assert!(matches!(
            f.service
                .workspace_file(&f.worker, "invalid", &paged(&path, offset, size, expected)),
            Err(Error::InvalidTransition(_))
        ));
    }
    let eof = f
        .service
        .workspace_file(&f.worker, "eof", &paged(&path, 5, 12000, Some(hash)))
        .unwrap();
    assert_eq!(eof.content.as_deref(), Some(""));
    assert!(eof.page.unwrap().eof);
    fs::write(&path, [b'a', 0xff]).unwrap();
    assert!(matches!(
        f.service
            .workspace_file(&f.worker, "binary", &paged(&path, 0, 1, None)),
        Err(Error::InvalidTransition(_))
    ));
    fs::write(&path, vec![b'a'; READ_SOURCE_BYTES_LIMIT + 1]).unwrap();
    assert!(matches!(
        f.service
            .workspace_file(&f.worker, "cap", &paged(&path, 0, 12000, None)),
        Err(Error::ContextBudgetExceeded)
    ));
}

#[test]
fn paged_read_keeps_link_root_run_and_cancellation_fences() {
    use std::os::unix::fs::symlink;
    let f = fixture_with_workspace(".bamboo/worktrees/worker", &[]);
    let root = Path::new(&f.workspace.worktree);
    let outside = f._temp.path().join("outside");
    fs::write(&outside, "private sibling").unwrap();
    symlink(&outside, root.join("link")).unwrap();
    symlink(f._temp.path(), root.join("directory")).unwrap();
    fs::hard_link(&outside, root.join("hardlink")).unwrap();
    for path in [
        outside.clone(),
        root.join("../outside"),
        root.join("link"),
        root.join("directory/outside"),
        root.join("hardlink"),
    ] {
        assert!(f
            .service
            .workspace_file(&f.worker, "denied", &paged(&path, 0, 12000, None))
            .is_err());
    }
    let source = root.join("source.rs");
    fs::write(&source, "scoped source").unwrap();
    let snapshot = f.service.published().unwrap().1;
    let wrong_run = Authority::from_verified_host(
        snapshot.binding.clone(),
        Principal::Worker {
            assignment_id: f.assignment.clone(),
            generation: 1,
            run_id: "foreign".into(),
            session_id: "child".into(),
        },
    );
    assert!(matches!(
        f.service
            .workspace_file(&wrong_run, "foreign", &paged(&source, 0, 12000, None)),
        Err(Error::ScopeDenied(_))
    ));
    let host = Authority::from_verified_host(
        snapshot.binding.clone(),
        Principal::Supervisor {
            session_id: "root".into(),
        },
    );
    execute(
        &f.service,
        &host,
        "cancel",
        vec![Operation::Cancel {
            work_id: snapshot.assignments[&f.assignment].work_id.clone(),
        }],
    )
    .unwrap();
    let stopped = canonical_bytes(&f.service.published().unwrap().1).unwrap();
    assert!(matches!(
        f.service
            .workspace_file(&f.worker, "late", &paged(&source, 0, 12000, None)),
        Err(Error::ScopeDenied(_))
    ));
    assert_eq!(
        canonical_bytes(&f.service.published().unwrap().1).unwrap(),
        stopped
    );
    assert_eq!(fs::read_to_string(outside).unwrap(), "private sibling");
}
