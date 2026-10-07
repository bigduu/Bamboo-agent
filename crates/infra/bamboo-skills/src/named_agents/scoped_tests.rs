use super::*;

#[test]
fn invalid_limits_never_publish_a_scoped_catalog() {
    let limits = NamedAgentLimits {
        max_candidates: 129,
        ..Default::default()
    };
    assert_eq!(
        ScopedNamedAgentCatalog::discover(Path::new("unused"), None, limits).unwrap_err(),
        NamedAgentDiagnosticCode::InvalidLimits
    );
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
#[test]
fn unsupported_platform_has_no_selectable_metadata() {
    let catalog =
        ScopedNamedAgentCatalog::discover(Path::new("private-home"), None, Default::default())
            .unwrap();
    assert_eq!(
        catalog.metadata().status,
        ScopedNamedAgentCatalogStatus::Unavailable
    );
    assert_eq!(
        catalog.metadata().diagnostic_code,
        Some(NamedAgentDiagnosticCode::UnsupportedPlatform)
    );
    assert!(catalog.metadata().entries.is_empty());
    assert!(!format!("{catalog:?}").contains("private-home"));
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod filesystem {
    use super::*;
    use std::fs;
    use std::os::unix::fs::symlink;
    use std::path::PathBuf;

    struct Fixture {
        _temp: tempfile::TempDir,
        global: PathBuf,
        project: PathBuf,
        id: ProjectId,
    }

    fn document(name: &str, body: &str) -> String {
        format!("---\nschema_version: 1\nname: {name}\ndescription: Safe public description\nmodel_hint: provider:private-route\ntools:\n  allow: [Read, mcp__files__read]\n  deny: [Write]\n---\n{body}")
    }

    impl Fixture {
        fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().canonicalize().unwrap();
            let global = root.join("global");
            let project = root.join("project");
            fs::create_dir_all(global.join("agents")).unwrap();
            fs::create_dir_all(project.join("agents")).unwrap();
            Self {
                _temp: temp,
                global,
                project,
                id: ProjectId::parse("exact-project").unwrap(),
            }
        }

        fn write(&self, project: bool, file: &str, name: &str, body: &str) {
            fs::write(
                if project { &self.project } else { &self.global }
                    .join("agents")
                    .join(file),
                document(name, body),
            )
            .unwrap();
        }

        fn load(
            &self,
            limits: NamedAgentLimits,
        ) -> Result<ScopedNamedAgentCatalog, NamedAgentDiagnosticCode> {
            ScopedNamedAgentCatalog::discover(&self.global, Some((&self.id, &self.project)), limits)
        }
    }

    fn selected(catalog: &ScopedNamedAgentCatalog, name: &str) -> NamedAgentProfileIdentity {
        catalog
            .metadata()
            .entries
            .iter()
            .find(|entry| {
                entry.status == NamedAgentProfileStatus::Selectable
                    && entry.identity.as_ref().is_some_and(|id| id.name == name)
            })
            .unwrap()
            .identity
            .clone()
            .unwrap()
    }

    fn unavailable(catalog: &ScopedNamedAgentCatalog, code: NamedAgentDiagnosticCode) {
        assert_eq!(
            catalog.metadata().status,
            ScopedNamedAgentCatalogStatus::Unavailable
        );
        assert_eq!(catalog.metadata().diagnostic_code, Some(code));
        assert!(catalog.definitions.is_empty());
        assert!(catalog
            .metadata()
            .entries
            .iter()
            .all(|entry| entry.status != NamedAgentProfileStatus::Selectable));
    }

    #[test]
    fn exact_project_override_and_retained_revision_are_coherent_and_private() {
        let fixture = Fixture::new();
        fixture.write(
            false,
            "global-private-file.md",
            "reviewer",
            "GLOBAL_PRIVATE_BODY",
        );
        fixture.write(
            true,
            "project-private-file.md",
            "reviewer",
            "PROJECT_PRIVATE_BODY",
        );
        let catalog = fixture.load(Default::default()).unwrap();
        let identity = selected(&catalog, "reviewer");
        assert_eq!(identity.source, NamedAgentProfileSource::Project);
        assert_eq!(identity.project_id.as_ref(), Some(&fixture.id));
        assert!(catalog
            .metadata()
            .entries
            .iter()
            .any(|entry| entry.status == NamedAgentProfileStatus::Shadowed));
        assert_eq!(
            catalog.get(&identity).unwrap().system_prompt(),
            "PROJECT_PRIVATE_BODY"
        );
        for wrong in [
            NamedAgentProfileIdentity {
                source: NamedAgentProfileSource::Global,
                ..identity.clone()
            },
            NamedAgentProfileIdentity {
                project_id: None,
                ..identity.clone()
            },
            NamedAgentProfileIdentity {
                project_id: Some(ProjectId::parse("foreign").unwrap()),
                ..identity.clone()
            },
            NamedAgentProfileIdentity {
                revision: "0".repeat(64),
                ..identity.clone()
            },
        ] {
            assert!(catalog.get(&wrong).is_none());
        }
        fixture.write(true, "replacement.md", "reviewer", "REPLACEMENT_BODY");
        fs::rename(
            fixture.project.join("agents/replacement.md"),
            fixture.project.join("agents/project-private-file.md"),
        )
        .unwrap();
        assert_eq!(
            catalog.get(&identity).unwrap().system_prompt(),
            "PROJECT_PRIVATE_BODY"
        );
        let later = fixture.load(Default::default()).unwrap();
        assert!(later.get(&identity).is_none());
        assert_eq!(
            later
                .get(&selected(&later, "reviewer"))
                .unwrap()
                .system_prompt(),
            "REPLACEMENT_BODY"
        );
        let public = format!(
            "{} {catalog:?}",
            serde_json::to_string(catalog.metadata()).unwrap()
        );
        for private in [
            "PRIVATE_BODY",
            "private-file",
            "private-route",
            "mcp__files__read",
            "\"tools\"",
            fixture.global.to_str().unwrap(),
        ] {
            assert!(!public.contains(private));
        }
    }

    #[test]
    fn project_known_conflict_blocks_only_that_global_name() {
        let fixture = Fixture::new();
        fixture.write(false, "g.md", "reviewer", "Global");
        fixture.write(false, "other.md", "other", "Unrelated");
        fixture.write(true, "a.md", "reviewer", "First");
        fixture.write(true, "b.md", "reviewer", "Second");
        let catalog = fixture.load(Default::default()).unwrap();
        assert_eq!(
            catalog.metadata().status,
            ScopedNamedAgentCatalogStatus::Available
        );
        assert_eq!(
            catalog
                .metadata()
                .entries
                .iter()
                .filter(|entry| entry.status == NamedAgentProfileStatus::Conflict)
                .count(),
            2
        );
        assert!(!catalog.definitions.contains_key("reviewer"));
        assert_eq!(
            catalog
                .get(&selected(&catalog, "other"))
                .unwrap()
                .system_prompt(),
            "Unrelated"
        );
    }

    #[test]
    fn anonymous_project_rejection_closes_public_and_private_selection() {
        let fixture = Fixture::new();
        fixture.write(false, "g.md", "reviewer", "Global");
        fs::write(
            fixture.project.join("agents/api_key-secret-filename.md"),
            document("hidden", "api_key: DO_NOT_ECHO"),
        )
        .unwrap();
        let catalog = fixture.load(Default::default()).unwrap();
        unavailable(&catalog, NamedAgentDiagnosticCode::CredentialMaterial);
        let public = format!(
            "{} {catalog:?}",
            serde_json::to_string(catalog.metadata()).unwrap()
        );
        for private in ["hidden", "api_key", "DO_NOT_ECHO", "secret-filename"] {
            assert!(!public.contains(private));
        }
        fs::remove_file(fixture.project.join("agents/api_key-secret-filename.md")).unwrap();
        fs::remove_dir(fixture.project.join("agents")).unwrap();
        let empty_project = fixture.load(Default::default()).unwrap();
        assert_eq!(
            selected(&empty_project, "reviewer").source,
            NamedAgentProfileSource::Global
        );
        let global =
            ScopedNamedAgentCatalog::discover(&fixture.global, None, Default::default()).unwrap();
        assert_eq!(selected(&global, "reviewer").project_id, None);
    }

    #[test]
    fn two_layers_share_candidate_entry_and_invalid_actual_read_budgets() {
        let fixture = Fixture::new();
        fixture.write(false, "g.md", "global", "Global");
        fixture.write(true, "p.md", "project", "Project");
        unavailable(
            &fixture
                .load(NamedAgentLimits {
                    max_candidates: 1,
                    ..Default::default()
                })
                .unwrap(),
            NamedAgentDiagnosticCode::CandidateLimitExceeded,
        );
        fs::remove_file(fixture.global.join("agents/g.md")).unwrap();
        fs::remove_file(fixture.project.join("agents/p.md")).unwrap();
        fs::write(fixture.global.join("agents/ignored.txt"), "ignored").unwrap();
        fs::write(fixture.project.join("agents/ignored.txt"), "ignored").unwrap();
        unavailable(
            &fixture
                .load(NamedAgentLimits {
                    max_candidates: 1,
                    max_scan_entries: 1,
                    ..Default::default()
                })
                .unwrap(),
            NamedAgentDiagnosticCode::ScanLimitExceeded,
        );
        fs::write(fixture.global.join("agents/invalid.md"), "x".repeat(600)).unwrap();
        fs::write(fixture.project.join("agents/invalid.md"), "y".repeat(600)).unwrap();
        unavailable(
            &fixture
                .load(NamedAgentLimits {
                    max_publication_bytes: 1000,
                    ..Default::default()
                })
                .unwrap(),
            NamedAgentDiagnosticCode::AggregateLimitExceeded,
        );
    }

    #[test]
    fn combined_retained_and_serialized_publication_is_bounded() {
        let fixture = Fixture::new();
        fixture.write(false, "g.md", "global", "Global");
        fixture.write(true, "p.md", "project", "Project");
        let read_bytes = fs::read(fixture.global.join("agents/g.md")).unwrap().len()
            + fs::read(fixture.project.join("agents/p.md")).unwrap().len();
        let catalog = fixture.load(Default::default()).unwrap();
        // The actual public projection fits alone. Each layer also publishes
        // independently; only retaining both definitions with it exceeds the cap.
        let limits = NamedAgentLimits {
            max_publication_bytes: serde_json::to_vec(catalog.metadata()).unwrap().len(),
            ..Default::default()
        };
        assert!(read_bytes <= limits.max_publication_bytes);
        for home in [&fixture.global, &fixture.project] {
            let layer = NamedAgentCatalog::discover(home, limits);
            assert!(layer.metadata().diagnostic_code.is_none());
            assert_eq!(layer.metadata().entries[0].status, NamedAgentStatus::Valid);
            assert!(layer
                .get(layer.metadata().entries[0].name.as_ref().unwrap())
                .is_some());
        }
        assert_eq!(
            fixture.load(limits).unwrap_err(),
            NamedAgentDiagnosticCode::AggregateLimitExceeded
        );
        assert!(
            serde_json::to_vec(catalog.metadata()).unwrap().len()
                < NamedAgentLimits::default().max_publication_bytes
        );
    }

    #[test]
    fn project_final_file_directory_and_ancestor_symlinks_fail_closed() {
        let fixture = Fixture::new();
        fixture.write(false, "g.md", "global", "Global");
        fs::write(
            fixture.project.join("agents/oversize.md"),
            vec![b'x'; 65_537],
        )
        .unwrap();
        unavailable(
            &fixture.load(Default::default()).unwrap(),
            NamedAgentDiagnosticCode::FileTooLarge,
        );
        fs::remove_file(fixture.project.join("agents/oversize.md")).unwrap();
        let foreign = fixture.global.join("agents/g.md");
        symlink(&foreign, fixture.project.join("agents/p.md")).unwrap();
        unavailable(
            &fixture.load(Default::default()).unwrap(),
            NamedAgentDiagnosticCode::UnsafePath,
        );
        fs::remove_file(fixture.project.join("agents/p.md")).unwrap();
        fs::remove_dir(fixture.project.join("agents")).unwrap();
        symlink(
            fixture.global.join("agents"),
            fixture.project.join("agents"),
        )
        .unwrap();
        unavailable(
            &fixture.load(Default::default()).unwrap(),
            NamedAgentDiagnosticCode::UnsafePath,
        );
        let alias = fixture.project.with_file_name("alias");
        symlink(&fixture.project, &alias).unwrap();
        unavailable(
            &ScopedNamedAgentCatalog::discover(
                &fixture.global,
                Some((&fixture.id, &alias)),
                Default::default(),
            )
            .unwrap(),
            NamedAgentDiagnosticCode::UnsafePath,
        );
    }
}
