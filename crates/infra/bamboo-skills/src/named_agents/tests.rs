use super::*;
use NamedAgentDiagnosticCode as Code;

fn document(name: &str, body: &str) -> String {
    format!("---\nschema_version: 1\nname: {name}\ndescription: Reviews Rust changes\nmodel_hint: provider:model-1\ntools:\n  allow: [Read, mcp__files__read]\n  deny: [Write]\n---\n{body}\n")
}

fn parsed_code(text: &str) -> Code {
    parser::parse(text.as_bytes(), NamedAgentLimits::default()).unwrap_err()
}

#[test]
fn v1_parser_keeps_exact_names_and_bounded_host_content() {
    let source = document("rust-reviewer", "HOST_ONLY_PROMPT");
    let definition = parser::parse(source.as_bytes(), NamedAgentLimits::default()).unwrap();
    assert_eq!(definition.name(), "rust-reviewer");
    assert_eq!(definition.description(), "Reviews Rust changes");
    assert_eq!(definition.model_hint(), Some("provider:model-1"));
    assert_eq!(definition.tool_allow(), ["Read", "mcp__files__read"]);
    assert_eq!(definition.tool_deny(), ["Write"]);
    assert_eq!(definition.system_prompt(), "HOST_ONLY_PROMPT");
    assert_eq!(definition.revision().len(), 64);
    assert!(!format!("{definition:?}").contains("HOST_ONLY_PROMPT"));
    let crlf = source.replace('\n', "\r\n");
    assert!(parser::parse(crlf.as_bytes(), NamedAgentLimits::default()).is_ok());
    let changed = document("rust-reviewer", "ANOTHER_BODY");
    assert_ne!(
        definition.revision(),
        parser::parse(changed.as_bytes(), NamedAgentLimits::default())
            .unwrap()
            .revision()
    );
}

#[test]
fn omitted_routing_and_tool_declarations_have_no_implicit_authority() {
    let source = "---\nschema_version: 1\nname: reviewer\ndescription: Reviews patches\n---\nReview the patch.\n";
    let definition = parser::parse(source.as_bytes(), NamedAgentLimits::default()).unwrap();
    assert_eq!(definition.model_hint(), None);
    assert!(definition.tool_allow().is_empty());
    assert!(definition.tool_deny().is_empty());
}

#[test]
fn rejects_unknown_versions_duplicate_keys_unknown_fields_and_yaml_expansion() {
    let valid = document("reviewer", "Review the patch.");
    assert_eq!(
        parsed_code(&valid.replace("schema_version: 1", "schema_version: 2")),
        Code::UnsupportedSchemaVersion
    );
    for replacement in [
        "schema_version: nope",
        "schema_version: '1'",
        "schema_version: 1\nname: duplicate",
        "schema_version: 1\nunknown: value",
        "schema_version: 1\nunknown: &anchor [*anchor]",
        "schema_version: !!int 1",
    ] {
        assert_eq!(
            parsed_code(&valid.replace("schema_version: 1", replacement)),
            Code::MalformedFrontmatter
        );
    }
    assert_eq!(parsed_code("no header"), Code::MissingFrontmatter);
    assert_eq!(
        parsed_code(&valid.replace("Review the patch.", " ")),
        Code::EmptyPrompt
    );
    let bytes = [0xff, 0xfe];
    assert_eq!(
        parser::parse(&bytes, NamedAgentLimits::default()).unwrap_err(),
        Code::InvalidUtf8
    );
    for (old, replacement) in [
        ("name: reviewer", "name: 123"),
        ("description: Reviews Rust changes", "description: true"),
        ("model_hint: provider:model-1", "model_hint: 123"),
        ("allow: [Read, mcp__files__read]", "allow: [true]"),
    ] {
        assert_eq!(
            parsed_code(&valid.replace(old, replacement)),
            Code::MalformedFrontmatter
        );
    }
}

#[test]
fn yaml_indicator_scan_handles_plain_apostrophes_comments_and_scalar_quotes() {
    let valid = document("reviewer", "Review the patch.");
    for description in [
        "Review owner's patch",
        "Review owner 'special' patch",
        "\"Review &literal markers\"",
        "'Review owner''s patch'",
    ] {
        let source = valid.replace("Reviews Rust changes", description);
        assert!(parser::parse(source.as_bytes(), NamedAgentLimits::default()).is_ok());
    }
    let comment = valid.replace("schema_version: 1", "# ' ignored\nschema_version: 1");
    assert!(parser::parse(comment.as_bytes(), NamedAgentLimits::default()).is_ok());
    for source in [
        valid
            .replace("Reviews Rust changes", "Review owner's patch")
            .replace(
                "model_hint: provider:model-1",
                "model_hint: &hint provider:model-1",
            ),
        valid
            .replace("Reviews Rust changes", "Review owner 'special' patch")
            .replace(
                "model_hint: provider:model-1",
                "model_hint: &hint provider:model-1",
            ),
        comment.replace("name: reviewer", "name: &n reviewer"),
        valid
            .replace(
                "Reviews Rust changes",
                "Review owner's patch\n  'plain continuation",
            )
            .replace(
                "model_hint: provider:model-1",
                "model_hint: &hint provider:model-1",
            ),
        valid
            .replace("Reviews Rust changes", "Review, 'plain apostrophe")
            .replace(
                "model_hint: provider:model-1",
                "model_hint: &hint provider:model-1",
            ),
        valid
            .replace("Reviews Rust changes", "Review [ 'plain apostrophe")
            .replace(
                "model_hint: provider:model-1",
                "model_hint: &hint provider:model-1",
            ),
        valid
            .replace("Reviews Rust changes", "Review { 'plain apostrophe")
            .replace(
                "model_hint: provider:model-1",
                "model_hint: &hint provider:model-1",
            ),
        valid
            .replace("Reviews Rust changes", "Review repo:'s patch")
            .replace(
                "model_hint: provider:model-1",
                "model_hint: &hint provider:model-1",
            ),
    ] {
        assert_eq!(parsed_code(&source), Code::MalformedFrontmatter);
    }
}

#[test]
fn flow_plain_colon_apostrophe_cannot_hide_anchors_and_quoted_keys_still_work() {
    let malicious = "---\n{schema_version: 1, name: reviewer, description: repo:'s patch,\nmodel_hint: &hint provider:model-1}\n---\nReview the patch.\n";
    assert_eq!(parsed_code(malicious), Code::MalformedFrontmatter);
    let valid = r#"---
{"schema_version":1,"name":"reviewer","description":"Review &literal markers"}
---
Review the patch.
"#;
    assert!(parser::parse(valid.as_bytes(), NamedAgentLimits::default()).is_ok());
}

#[test]
fn credential_detection_rescans_all_yaml_decoded_scalar_and_list_values() {
    let valid = document("reviewer", "Review the patch.");
    for (old, replacement) in [
        (
            "description: Reviews Rust changes",
            r#"description: "api_key\u003dLITERAL""#,
        ),
        (
            "description: Reviews Rust changes",
            r#"description: "github_pat\u005f012345678901234567890123456789""#,
        ),
        (
            "model_hint: provider:model-1",
            r#"model_hint: "sk\u002d012345678901234567890123456789""#,
        ),
        (
            "allow: [Read, mcp__files__read]",
            r#"allow: ["sk\u002d012345678901234567890123456789"]"#,
        ),
        (
            "deny: [Write]",
            r#"deny: ["sk\u002d012345678901234567890123456789"]"#,
        ),
        (
            "name: reviewer",
            r#"name: "sk\u002d012345678901234567890123456789""#,
        ),
    ] {
        assert_eq!(
            parsed_code(&valid.replace(old, replacement)),
            Code::CredentialMaterial
        );
    }
}

#[test]
fn rejects_unsafe_names_descriptions_routing_and_tool_declarations() {
    let valid = document("reviewer", "Review the patch.");
    for name in ["../escape", "UPPERCASE", "with space", "-leading"] {
        assert_eq!(
            parsed_code(&valid.replace("name: reviewer", &format!("name: '{name}'"))),
            Code::InvalidName
        );
    }
    for description in [
        "/private/path",
        "C:\\private\\path",
        "hello\u{202e}world",
        "hello\nworld",
    ] {
        let replacement = format!(
            "description: {}",
            serde_json::to_string(description).unwrap()
        );
        assert_eq!(
            parsed_code(&valid.replace("description: Reviews Rust changes", &replacement)),
            Code::InvalidDescription
        );
    }
    assert_eq!(
        parsed_code(&valid.replace("provider:model-1", "https://provider.example")),
        Code::InvalidModelHint
    );
    for replacement in ["[Read, Read]", "[Write]", "['../tool']"] {
        assert_eq!(
            parsed_code(&valid.replace("[Read, mcp__files__read]", replacement)),
            Code::InvalidToolDeclarations
        );
    }
    assert_eq!(
        parsed_code(&valid.replace("deny: [Write]", "other: [Write]")),
        Code::MalformedFrontmatter
    );
}

#[test]
fn rejects_literal_credentials_and_recognizable_secret_families_everywhere() {
    let valid = document("reviewer", "Review the patch.");
    for secret in [
        "api_key: literal-value",
        "API-KEY = literal-value",
        "{\"client_secret\": \"literal-value\"}",
        "authorization: Bearer literal-value",
        "-----BEGIN OPENSSH PRIVATE KEY-----\nnot-a-real-key\n-----END OPENSSH PRIVATE KEY-----",
        "-----BEGIN PGP PRIVATE KEY BLOCK-----",
        "sk-012345678901234567890123456789",
        "ghp_012345678901234567890123456789012345",
        "github_pat_012345678901234567890123456789",
        "AKIA0123456789012345",
        "xoxb-01234567890123456789",
    ] {
        assert_eq!(
            parsed_code(&valid.replace("Review the patch.", secret)),
            Code::CredentialMaterial
        );
    }
    assert_eq!(
        parsed_code(&valid.replace(
            "schema_version: 1",
            "schema_version: 1\npassword: literal-value"
        )),
        Code::CredentialMaterial
    );
    assert_eq!(
        parsed_code(&valid.replace("Reviews Rust changes", "api_key=literal-value")),
        Code::CredentialMaterial
    );
    // This boundary is intentionally documented: arbitrary unlabeled prose is
    // not guaranteed to be recognized as a credential.
    assert!(parser::parse(
        document("reviewer", "An arbitrary unlabeled phrase.").as_bytes(),
        NamedAgentLimits::default()
    )
    .is_ok());
}

#[test]
fn parser_enforces_prompt_frontmatter_and_field_budgets() {
    let limits = NamedAgentLimits {
        max_prompt_bytes: 3,
        ..Default::default()
    };
    assert_eq!(
        parser::parse(document("reviewer", "1234").as_bytes(), limits).unwrap_err(),
        Code::PromptTooLarge
    );
    let valid = document("reviewer", "Review the patch.");
    assert_eq!(
        parsed_code(&valid.replace("Reviews Rust changes", &"a".repeat(513))),
        Code::InvalidDescription
    );
    assert_eq!(
        parsed_code(&valid.replace("name: reviewer", &format!("name: {}", "a".repeat(65)))),
        Code::InvalidName
    );
    assert_eq!(
        parsed_code(&valid.replace("provider:model-1", &"a".repeat(129))),
        Code::InvalidModelHint
    );
    assert_eq!(
        parsed_code(&valid.replace(
            "[Read, mcp__files__read]",
            &format!("[{}]", (0..33).map(|n| format!("tool{n}")).collect::<Vec<_>>().join(","))
        )),
        Code::InvalidToolDeclarations
    );
    assert_eq!(
        parsed_code(&valid.replace(
            "description: Reviews Rust changes",
            &format!("description: {}", "a".repeat(16_384))
        )),
        Code::MalformedFrontmatter
    );
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod filesystem {
    use std::ffi::{CString, OsStr};
    use std::fs::{self, File};
    use std::io::Write;
    use std::os::unix::fs::symlink;
    use std::path::{Path, PathBuf};

    use tempfile::TempDir;

    use super::*;

    struct Fixture {
        _temp: TempDir,
        root: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            // macOS's default /var temp prefix is a symlink. Configure its real
            // physical path so normal fixtures don't silently weaken the policy.
            let root = temp.path().canonicalize().unwrap();
            fs::create_dir(root.join("agents")).unwrap();
            Self { _temp: temp, root }
        }

        fn write(&self, filename: &str, source: &str) {
            fs::write(self.root.join("agents").join(filename), source).unwrap();
        }

        fn load(&self) -> NamedAgentCatalog {
            NamedAgentCatalog::discover(&self.root, NamedAgentLimits::default())
        }
    }

    fn assert_rejected(catalog: &NamedAgentCatalog, code: Code) {
        assert_eq!(catalog.metadata().diagnostic_code, Some(code));
        assert!(catalog.metadata().entries.is_empty());
        assert!(catalog.definitions.is_empty());
    }

    fn assert_invalid(catalog: &NamedAgentCatalog, code: Code) {
        assert_eq!(catalog.metadata().diagnostic_code, None);
        assert_eq!(
            catalog.metadata().entries,
            [NamedAgentMetadata::invalid(code)]
        );
        assert!(catalog.definitions.is_empty());
    }

    #[test]
    fn global_catalog_metadata_redacts_paths_prompt_routing_tools_and_invalid_sources() {
        let fixture = Fixture::new();
        fixture.write(
            "different-from-name.md",
            &document("reviewer", "HOST_ONLY_BODY"),
        );
        fixture.write(
            "secret-filename-password.md",
            &document("secret-name", "password: DO_NOT_ECHO"),
        );
        fixture.write(
            "encoded.md",
            &document("encoded-secret-name", "Review the patch.").replace(
                "description: Reviews Rust changes",
                r#"description: "api_key\u003dDECODED_LITERAL""#,
            ),
        );
        let catalog = fixture.load();
        assert_eq!(
            catalog.get("reviewer").unwrap().system_prompt(),
            "HOST_ONLY_BODY"
        );
        assert!(catalog.get("Reviewer").is_none());
        assert!(catalog.get("different-from-name").is_none());
        assert!(catalog.get("secret-name").is_none());
        let metadata = serde_json::to_string(catalog.metadata()).unwrap();
        let debug = format!("{catalog:?}");
        for forbidden in [
            "HOST_ONLY_BODY",
            "provider:model-1",
            "mcp__files__read",
            "DO_NOT_ECHO",
            "secret-name",
            "encoded-secret-name",
            "DECODED_LITERAL",
            "secret-filename-password",
            "different-from-name",
            fixture.root.to_str().unwrap(),
        ] {
            assert!(
                !metadata.contains(forbidden),
                "unexpected metadata exposure"
            );
            assert!(!debug.contains(forbidden), "unexpected debug exposure");
        }
        assert!(metadata.contains("reviewer"));
        assert!(metadata.contains("credential_material"));
    }

    #[test]
    fn all_duplicate_names_conflict_without_selecting_a_winner() {
        let fixture = Fixture::new();
        fixture.write("a.md", &document("reviewer", "FIRST"));
        fixture.write("b.md", &document("reviewer", "SECOND"));
        fixture.write("c.md", &document("other", "THIRD"));
        let catalog = fixture.load();
        assert!(catalog.get("reviewer").is_none());
        assert!(catalog.get("other").is_some());
        let conflicts = catalog
            .metadata()
            .entries
            .iter()
            .filter(|entry| entry.status == NamedAgentStatus::Conflict)
            .collect::<Vec<_>>();
        assert_eq!(conflicts.len(), 2);
        assert!(conflicts
            .iter()
            .all(|entry| entry.name.as_deref() == Some("reviewer")
                && entry.diagnostic_code == Some(Code::DuplicateName)));
    }

    #[test]
    fn missing_agents_is_empty_and_bad_roots_are_typed_without_paths() {
        let fixture = Fixture::new();
        fs::remove_dir(fixture.root.join("agents")).unwrap();
        assert_eq!(
            fixture.load().metadata(),
            &NamedAgentCatalogMetadata::default()
        );
        assert_rejected(
            &NamedAgentCatalog::discover(Path::new("relative"), NamedAgentLimits::default()),
            Code::PathEscape,
        );
        assert_rejected(
            &NamedAgentCatalog::discover(&fixture.root.join(".."), NamedAgentLimits::default()),
            Code::PathEscape,
        );
        assert_rejected(
            &NamedAgentCatalog::discover(
                &fixture.root.join("missing"),
                NamedAgentLimits::default(),
            ),
            Code::RootUnavailable,
        );
        assert_rejected(
            &NamedAgentCatalog::discover(
                &fixture.root,
                NamedAgentLimits {
                    max_candidates: 129,
                    ..Default::default()
                },
            ),
            Code::InvalidLimits,
        );
    }

    #[test]
    fn rejects_symlink_ancestors_agents_roots_and_final_files() {
        let fixture = Fixture::new();
        let external = Fixture::new();
        external.write("outside.md", &document("outside", "EXTERNAL_BODY"));
        symlink(&external.root, fixture.root.join("linked-root")).unwrap();
        assert_rejected(
            &NamedAgentCatalog::discover(
                &fixture.root.join("linked-root"),
                NamedAgentLimits::default(),
            ),
            Code::UnsafePath,
        );
        assert_rejected(
            &NamedAgentCatalog::discover(
                &fixture.root.join("linked-root/agents"),
                NamedAgentLimits::default(),
            ),
            Code::UnsafePath,
        );
        fs::remove_dir(fixture.root.join("agents")).unwrap();
        symlink(external.root.join("agents"), fixture.root.join("agents")).unwrap();
        assert_rejected(&fixture.load(), Code::UnsafePath);
        fs::remove_file(fixture.root.join("agents")).unwrap();
        fs::create_dir(fixture.root.join("agents")).unwrap();
        symlink(
            external.root.join("agents/outside.md"),
            fixture.root.join("agents/link.md"),
        )
        .unwrap();
        assert_invalid(&fixture.load(), Code::UnsafePath);
        assert!(!serde_json::to_string(fixture.load().metadata())
            .unwrap()
            .contains("EXTERNAL_BODY"));
    }

    #[test]
    fn retained_capability_reads_original_directory_after_ancestor_replacement() {
        let fixture = Fixture::new();
        let external = Fixture::new();
        fixture.write("same.md", &document("inside", "ORIGINAL_BODY"));
        external.write("same.md", &document("outside", "EXTERNAL_BODY"));
        let directory = reader::AgentDirectory::open(&fixture.root)
            .unwrap()
            .unwrap();
        let displaced = fixture.root.with_extension("displaced");
        fs::rename(&fixture.root, &displaced).unwrap();
        symlink(&external.root, &fixture.root).unwrap();
        let candidates = directory
            .candidates(NamedAgentLimits::default(), &mut ScanBudget::default())
            .unwrap();
        assert_eq!(candidates, [std::ffi::OsString::from("same.md")]);
        let bytes = directory
            .read(OsStr::new("same.md"), 65_536, 1_048_576)
            .unwrap();
        assert!(String::from_utf8(bytes).unwrap().contains("ORIGINAL_BODY"));
        assert_eq!(
            directory
                .read(OsStr::new("../same.md"), 65_536, 1_048_576)
                .unwrap_err(),
            (Code::PathEscape, 0)
        );
        fs::remove_file(&fixture.root).unwrap();
        fs::rename(displaced, &fixture.root).unwrap();
    }

    #[test]
    fn replacing_final_file_with_symlink_after_enumeration_is_rejected() {
        let fixture = Fixture::new();
        let external = Fixture::new();
        fixture.write("same.md", &document("inside", "ORIGINAL_BODY"));
        external.write("outside.md", &document("outside", "EXTERNAL_BODY"));
        let directory = reader::AgentDirectory::open(&fixture.root)
            .unwrap()
            .unwrap();
        let candidates = directory
            .candidates(NamedAgentLimits::default(), &mut ScanBudget::default())
            .unwrap();
        fs::remove_file(fixture.root.join("agents/same.md")).unwrap();
        symlink(
            external.root.join("agents/outside.md"),
            fixture.root.join("agents/same.md"),
        )
        .unwrap();
        assert_eq!(
            directory
                .read(&candidates[0], 65_536, 1_048_576)
                .unwrap_err(),
            (Code::UnsafePath, 0)
        );
    }

    #[test]
    fn rejects_directories_and_fifo_candidates_without_blocking() {
        let fixture = Fixture::new();
        fs::create_dir(fixture.root.join("agents/directory.md")).unwrap();
        assert_invalid(&fixture.load(), Code::NotRegularFile);
        fs::remove_dir(fixture.root.join("agents/directory.md")).unwrap();
        let fifo = CString::new(fixture.root.join("agents/pipe.md").to_str().unwrap()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
        assert_invalid(&fixture.load(), Code::NotRegularFile);
    }

    #[test]
    fn bounds_candidates_total_scanned_entries_file_bytes_and_prompt_bytes() {
        let fixture = Fixture::new();
        fixture.write("a.md", &document("a", "BODY"));
        fixture.write("b.md", &document("b", "BODY"));
        let limits = NamedAgentLimits {
            max_candidates: 1,
            ..Default::default()
        };
        assert_rejected(
            &NamedAgentCatalog::discover(&fixture.root, limits),
            Code::CandidateLimitExceeded,
        );
        fs::remove_file(fixture.root.join("agents/a.md")).unwrap();
        fs::remove_file(fixture.root.join("agents/b.md")).unwrap();
        fixture.write("a.txt", "ignored");
        fixture.write("b.txt", "ignored");
        let limits = NamedAgentLimits {
            max_candidates: 1,
            max_scan_entries: 1,
            ..Default::default()
        };
        assert_rejected(
            &NamedAgentCatalog::discover(&fixture.root, limits),
            Code::ScanLimitExceeded,
        );
        fs::remove_file(fixture.root.join("agents/a.txt")).unwrap();
        fs::remove_file(fixture.root.join("agents/b.txt")).unwrap();
        fixture.write("a.md", &document("a", "BODY"));
        let limits = NamedAgentLimits {
            max_file_bytes: 10,
            ..Default::default()
        };
        assert_invalid(
            &NamedAgentCatalog::discover(&fixture.root, limits),
            Code::FileTooLarge,
        );
        let limits = NamedAgentLimits {
            max_prompt_bytes: 3,
            ..Default::default()
        };
        assert_invalid(
            &NamedAgentCatalog::discover(&fixture.root, limits),
            Code::PromptTooLarge,
        );
    }

    #[test]
    fn aggregate_budget_rejects_entire_publication_and_counts_invalid_sources_and_metadata() {
        let fixture = Fixture::new();
        let source = document("a", "BODY");
        fixture.write("a.md", &source);
        fixture.write("b.md", &document("b", "password: LITERAL"));
        let limits = NamedAgentLimits {
            max_publication_bytes: source.len() + 1,
            ..Default::default()
        };
        assert_rejected(
            &NamedAgentCatalog::discover(&fixture.root, limits),
            Code::AggregateLimitExceeded,
        );
        fs::remove_file(fixture.root.join("agents/b.md")).unwrap();
        let limits = NamedAgentLimits {
            max_publication_bytes: source.len(),
            ..Default::default()
        };
        assert_rejected(
            &NamedAgentCatalog::discover(&fixture.root, limits),
            Code::AggregateLimitExceeded,
        );
        fixture.write("a.md", "bad");
        let limits = NamedAgentLimits {
            max_publication_bytes: 3,
            ..Default::default()
        };
        assert_rejected(
            &NamedAgentCatalog::discover(&fixture.root, limits),
            Code::AggregateLimitExceeded,
        );
    }

    #[test]
    fn bounded_actual_read_detects_growth_after_a_stale_stat_and_respects_remaining_budget() {
        let fixture = Fixture::new();
        fixture.write("grow.md", "123");
        let path = fixture.root.join("agents/grow.md");
        let file = File::open(&path).unwrap();
        assert_eq!(file.metadata().unwrap().len(), 3);
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"456789")
            .unwrap();
        let bytes = reader::read_content(file, 5).unwrap();
        assert_eq!(bytes, b"123456"); // budget + exactly one overflow probe
        let file = File::open(&path).unwrap();
        let bytes = reader::read_content(file, 2).unwrap();
        assert_eq!(bytes, b"123"); // a tighter aggregate remainder wins
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
#[test]
fn unsupported_platform_fails_closed_without_claiming_malformed_definitions() {
    let catalog =
        NamedAgentCatalog::discover(Path::new("unread-path"), NamedAgentLimits::default());
    assert_eq!(
        catalog.metadata().diagnostic_code,
        Some(Code::UnsupportedPlatform)
    );
    assert!(catalog.metadata().entries.is_empty());
    assert!(catalog.definitions.is_empty());
}
