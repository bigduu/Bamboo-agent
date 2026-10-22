#[path = "../manual/publication.rs"]
mod publication;
use bamboo_skills::store::builtin::BUILTIN_SKILL_FILES;
#[test]
fn deterministic_export_preserves_compiled_resources_and_refuses_overwrite() {
    let first = publication::build(BUILTIN_SKILL_FILES).unwrap();
    assert_eq!(first, publication::build(BUILTIN_SKILL_FILES).unwrap());
    for (path, bytes) in BUILTIN_SKILL_FILES {
        if let Some(path) = path.strip_prefix("bamboo-manual/") {
            let output = if path == "manifest.json" {
                path.into()
            } else {
                format!("resources/{path}")
            };
            assert_eq!(&first.1[&output], bytes);
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let destination = temp.path().join("export");
    let root = publication::write_new(&destination, BUILTIN_SKILL_FILES).unwrap();
    assert!(root.join("index.html").is_file());
    assert!(publication::write_new(&destination, BUILTIN_SKILL_FILES).is_err());
    assert_eq!(
        std::fs::read(root.join("index.json")).unwrap(),
        first.1["index.json"]
    );
}
#[test]
fn invalid_bundle_fails_before_io() {
    let temp = tempfile::tempdir().unwrap();
    let destination = temp.path().join("export");
    let mut files = BUILTIN_SKILL_FILES.to_vec();
    let index = files
        .iter()
        .position(|(path, _)| *path == "bamboo-manual/references/hooks.md")
        .unwrap();
    files[index].1 = b"altered";
    assert!(publication::write_new(&destination, &files).is_err());
    assert!(!destination.exists());
    let mut duplicate = BUILTIN_SKILL_FILES.to_vec();
    duplicate.push(BUILTIN_SKILL_FILES[index]);
    assert!(publication::build(&duplicate).is_err());
    let missing: Vec<_> = BUILTIN_SKILL_FILES
        .iter()
        .copied()
        .filter(|(p, _)| *p != "bamboo-manual/manifest.json")
        .collect();
    assert!(publication::build(&missing).is_err());
}

#[test]
fn rejects_unsafe_or_inconsistent_manifests_before_writing() {
    let manifest_position = BUILTIN_SKILL_FILES
        .iter()
        .position(|(path, _)| *path == "bamboo-manual/manifest.json")
        .unwrap();
    let original: serde_json::Value =
        serde_json::from_slice(BUILTIN_SKILL_FILES[manifest_position].1).unwrap();
    for case in ["version", "path", "hash", "content", "missing"] {
        let mut manifest = original.clone();
        match case {
            "version" => manifest["package_version"] = "../outside".into(),
            "path" => manifest["resources"][0]["path"] = "../outside".into(),
            "hash" => manifest["resources"][0]["sha256"] = "wrong".into(),
            "content" => manifest["content_sha256"] = "wrong".into(),
            _ => {
                manifest["resources"].as_array_mut().unwrap().pop();
            }
        }
        let bytes = serde_json::to_vec(&manifest).unwrap();
        let mut files = BUILTIN_SKILL_FILES.to_vec();
        files[manifest_position].1 = &bytes;
        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("export");
        assert!(
            publication::write_new(&destination, &files).is_err(),
            "{case}"
        );
        assert!(!destination.exists(), "{case}");
    }
}
