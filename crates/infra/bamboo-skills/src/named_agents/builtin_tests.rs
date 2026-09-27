use super::*;

#[test]
fn builtin_package_has_stable_private_revision_bounded_roles_and_native_declarations() {
    let limits = NamedAgentLimits::default();
    let catalog = catalog(limits, &mut ScanBudget::default());
    assert!(catalog.metadata().diagnostic_code.is_none());
    assert_eq!(catalog.metadata().entries.len(), 3);
    for (name, bytes) in ["explorer", "implementer", "reviewer"]
        .into_iter()
        .zip(DOCUMENTS)
    {
        let definition = catalog.get(name).unwrap();
        assert_eq!(
            definition.revision(),
            parser::parse(bytes, limits).unwrap().revision()
        );
        assert_eq!(definition.revision().len(), 64);
        assert_eq!(definition.model_hint(), None);
        assert!(definition.system_prompt().len() <= MAX_BUILTIN_PROMPT_BYTES);
        for required in ["Responsibility:", "Scope:", "Evidence:", "stop"] {
            assert!(
                definition.system_prompt().contains(required)
                    || (required == "stop"
                        && definition
                            .system_prompt()
                            .to_ascii_lowercase()
                            .contains(required)),
                "{name}: {required}"
            );
        }
        if name == "implementer" {
            assert_eq!(
                definition.tool_allow(),
                ["Bash", "Edit", "Glob", "Read", "Write"]
            );
            assert!(definition.tool_deny().is_empty());
        } else {
            assert_eq!(definition.tool_allow(), ["Read", "Glob"]);
            assert_eq!(definition.tool_deny(), ["Bash", "Edit", "Write"]);
        }
        assert!(!format!("{definition:?}").contains("Builtin role package"));
    }
    assert!(catalog.get("plan").is_none());
    assert!(catalog.get("planner").is_none());
}

#[cfg(unix)]
mod scoped {
    use super::*;
    use bamboo_domain::ProjectId;
    use std::fs;
    use NamedAgentDiagnosticCode as Code;
    use NamedAgentProfileSource as Source;
    use NamedAgentProfileStatus as Status;

    struct Fixture(tempfile::TempDir, ProjectId);
    impl Fixture {
        fn new() -> Self {
            let fixture = Self(
                tempfile::tempdir().unwrap(),
                ProjectId::parse("role-project").unwrap(),
            );
            for project in [false, true] {
                fs::create_dir_all(fixture.home(project).join("agents")).unwrap();
            }
            fixture
        }
        fn home(&self, project: bool) -> std::path::PathBuf {
            self.0
                .path()
                .canonicalize()
                .unwrap()
                .join(if project { "project" } else { "global" })
        }
        fn load(&self, limits: NamedAgentLimits) -> ScopedNamedAgentCatalog {
            ScopedNamedAgentCatalog::discover_with_builtins(
                &self.home(false),
                Some((&self.1, &self.home(true))),
                limits,
            )
            .unwrap()
        }
        fn write(&self, project: bool, file: &str, name: &str) {
            fs::write(self.home(project).join("agents").join(file), format!(
                "---\nschema_version: 1\nname: {name}\ndescription: Safe override\n---\nPRIVATE_OVERRIDE"
            )).unwrap();
        }
    }
    fn selected(catalog: &ScopedNamedAgentCatalog, name: &str) -> NamedAgentProfileIdentity {
        catalog
            .metadata()
            .entries
            .iter()
            .find(|row| {
                row.status == Status::Selectable
                    && row.identity.as_ref().is_some_and(|id| id.name == name)
            })
            .unwrap()
            .identity
            .clone()
            .unwrap()
    }
    fn unavailable(catalog: &ScopedNamedAgentCatalog) {
        assert_eq!(
            catalog.metadata().status,
            ScopedNamedAgentCatalogStatus::Unavailable
        );
        for row in &catalog.metadata().entries {
            assert_ne!(row.status, Status::Selectable);
            if let Some(identity) = &row.identity {
                assert!(catalog.get(identity).is_none());
            }
        }
    }
    #[test]
    fn builtin_catalog_safe_metadata_and_project_global_builtin_exact_precedence() {
        let f = Fixture::new();
        let initial = f.load(Default::default());
        let builtin = selected(&initial, "explorer");
        assert_eq!(builtin.source, Source::Builtin);
        assert_eq!(builtin.project_id, None);
        let public = format!(
            "{} {initial:?}",
            serde_json::to_string(initial.metadata()).unwrap()
        );
        let global_home = f.home(false);
        for private in [
            "Builtin role package",
            "system_prompt",
            "tool_allow",
            "Bash",
            global_home.to_str().unwrap(),
        ] {
            assert!(!public.contains(private));
        }
        f.write(false, "explorer.md", "explorer");
        let global = f.load(Default::default());
        assert_eq!(selected(&global, "explorer").source, Source::Global);
        assert!(global.get(&builtin).is_none());
        f.write(true, "explorer.md", "explorer");
        let project = f.load(Default::default());
        let id = selected(&project, "explorer");
        assert_eq!(id.source, Source::Project);
        assert_eq!(id.project_id.as_ref(), Some(&f.1));
        assert_eq!(
            project.get(&id).unwrap().system_prompt(),
            "PRIVATE_OVERRIDE"
        );
        assert_eq!(
            project
                .metadata()
                .entries
                .iter()
                .filter(|e| e.status == Status::Shadowed)
                .count(),
            2
        );
        assert!(project.get(&builtin).is_none());
        assert!(
            initial.get(&builtin).is_some(),
            "retained observation is immutable, not current permission"
        );
        assert!(project
            .get(&NamedAgentProfileIdentity {
                revision: "0".repeat(64),
                ..id
            })
            .is_none());
        assert_eq!(selected(&project, "reviewer").source, Source::Builtin);
    }
    #[test]
    fn builtin_never_rescues_a_known_duplicate_higher_layer_name() {
        for project in [false, true] {
            let f = Fixture::new();
            let prior = selected(&f.load(Default::default()), "reviewer");
            f.write(project, "a.md", "reviewer");
            f.write(project, "b.md", "reviewer");
            let catalog = f.load(Default::default());
            assert!(catalog.get(&prior).is_none());
            assert_eq!(
                catalog
                    .metadata()
                    .entries
                    .iter()
                    .filter(|e| e.status == Status::Conflict)
                    .count(),
                2
            );
            assert!(!catalog
                .metadata()
                .entries
                .iter()
                .any(|e| e.status == Status::Selectable
                    && e.identity.as_ref().is_some_and(|id| id.name == "reviewer")));
            assert_eq!(selected(&catalog, "explorer").source, Source::Builtin);
        }
    }
    #[test]
    fn builtin_never_rescues_an_anonymous_invalid_or_unavailable_source() {
        for project in [false, true] {
            let f = Fixture::new();
            let identity = selected(&f.load(Default::default()), "reviewer");
            fs::write(f.home(project).join("agents/private.md"), "not a profile").unwrap();
            let catalog = f.load(Default::default());
            unavailable(&catalog);
            assert!(catalog.get(&identity).is_none());
        }
        unavailable(
            &ScopedNamedAgentCatalog::discover_with_builtins(
                Path::new("relative"),
                None,
                Default::default(),
            )
            .unwrap(),
        );
    }
    #[test]
    fn builtin_profiles_share_caps_and_honor_tighter_prompt_limits() {
        let f = Fixture::new();
        let default = NamedAgentLimits::default();
        for (limits, code) in [
            (
                NamedAgentLimits {
                    max_candidates: 2,
                    ..default
                },
                Code::CandidateLimitExceeded,
            ),
            (
                NamedAgentLimits {
                    max_prompt_bytes: 4,
                    ..default
                },
                Code::PromptTooLarge,
            ),
            (
                NamedAgentLimits {
                    max_publication_bytes: 1024,
                    ..default
                },
                Code::AggregateLimitExceeded,
            ),
        ] {
            let catalog = f.load(limits);
            unavailable(&catalog);
            assert_eq!(catalog.metadata().diagnostic_code, Some(code));
        }
        f.write(false, "extra.md", "extra");
        let catalog = f.load(NamedAgentLimits {
            max_candidates: 3,
            ..default
        });
        unavailable(&catalog);
        assert_eq!(
            catalog.metadata().diagnostic_code,
            Some(Code::CandidateLimitExceeded)
        );
        assert_eq!(
            ScopedNamedAgentCatalog::discover(&f.home(false), None, default)
                .unwrap()
                .metadata()
                .entries
                .len(),
            1,
            "legacy discovery does not silently opt into defaults"
        );
    }
}
