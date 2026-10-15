use std::collections::BTreeMap;
use std::path::{Component, PathBuf};

use bamboo_domain::{ProjectId, PROJECT_MANIFEST_SCHEMA_VERSION};
use bamboo_projects::ProjectStore;

fn main() {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let data = PathBuf::from(&args[0]);
    let fixture = PathBuf::from(&args[1]);
    let operation = args[2].to_str().unwrap();
    let id: ProjectId = "interrupted-migration".parse().unwrap();
    let store = ProjectStore::open(&data).unwrap();
    store.create_with_id(id.clone(), "fixture", None).unwrap();

    // Install a durable-prefix fixture after opening the reader, so `get`
    // itself (rather than the initial index rebuild) exercises normalization.
    let files: BTreeMap<String, String> =
        serde_json::from_slice(&std::fs::read(fixture).unwrap()).unwrap();
    let home = store.paths().project_home(&id);
    for (relative, bytes) in files {
        let relative = PathBuf::from(relative);
        assert!(relative
            .components()
            .all(|c| matches!(c, Component::Normal(_))));
        let path = home.join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, bytes).unwrap();
        std::fs::File::open(path).unwrap().sync_all().unwrap();
    }

    let mut mutated = false;
    let result = match operation {
        "get" => store.get(&id),
        "update" => store.get(&id).and_then(|current| {
            store.update(&id, current.revision, |project| {
                mutated = true;
                project.name = "metadata edited by archived reader".into();
                Ok(())
            })
        }),
        _ => panic!("unknown operation"),
    };
    let output = match result {
        Ok(manifest) => serde_json::json!({
            "reader_schema": PROJECT_MANIFEST_SCHEMA_VERSION,
            "ok": true, "mutated": mutated, "manifest": manifest
        }),
        Err(error) => serde_json::json!({
            "reader_schema": PROJECT_MANIFEST_SCHEMA_VERSION,
            "ok": false, "mutated": mutated, "error": error.to_string()
        }),
    };
    println!("{}", serde_json::to_string(&output).unwrap());
}
