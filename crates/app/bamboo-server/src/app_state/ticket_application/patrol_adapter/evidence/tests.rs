use super::*;
use bamboo_domain::ProjectId;
use bamboo_memory::memory_store::{
    DurableMemoryDocument, DurableMemoryType, MemoryRetrievalInput, MemoryScope, MemoryStore,
    MAX_MAX_CHARS,
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

pub(super) fn files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(path: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
        if !path.exists() {
            return;
        }
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                walk(&entry.path(), out)
            } else if entry.file_type().unwrap().is_file() {
                out.insert(entry.path(), std::fs::read(entry.path()).unwrap());
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, &mut out);
    out
}

pub(super) async fn write(
    memory: &MemoryStore,
    project: &ProjectId,
    title: &str,
    body: &str,
) -> DurableMemoryDocument {
    memory
        .for_project(project)
        .write_memory(
            MemoryScope::Project,
            Some(project.as_str()),
            DurableMemoryType::Reference,
            title,
            body,
            &[],
            None,
            "fixture",
            false,
            None,
        )
        .await
        .unwrap()
}

async fn read(
    memory: &MemoryStore,
    project: &ProjectId,
    pin: &ProjectMemoryRef,
) -> WorkPatrolSourceOutcome {
    let baseline = files(&memory.data_dir());
    let value = resolve(
        pin,
        memory
            .read_project_source_read_only(project, pin.memory_id(), pin.content_version())
            .await,
    );
    assert_eq!(
        files(&memory.data_dir()),
        baseline,
        "native evidence read must not write indexes/access logs/content"
    );
    value
}

#[tokio::test]
async fn exact_native_versions_inactive_missing_and_project_isolation_are_explicit() {
    let root = tempfile::tempdir().unwrap();
    let memory = MemoryStore::new(root.path());
    let a = ProjectId::new();
    let b = ProjectId::new();
    let doc = write(&memory, &a, "Evidence", "Untrusted source bytes.").await;
    write(
        &memory,
        &b,
        "Foreign",
        "Foreign bytes must never be queried.",
    )
    .await;
    let pin = ProjectMemoryRef::from_document(&doc).unwrap();
    assert_eq!(
        native_pin(&a, &pin.source_key(), pin.content_version()).unwrap(),
        Some(pin.clone())
    );
    assert!(matches!(
        native_pin(&b, &pin.source_key(), pin.content_version()),
        Err(Error::ScopeDenied(_))
    ));
    assert!(matches!(
        native_pin(&a, &pin.source_key(), "malformed"),
        Err(Error::ScopeDenied(_))
    ));
    assert!(native_pin(&a, "external:unsupported", "opaque-version")
        .unwrap()
        .is_none());
    assert!(
        matches!(read(&memory,&a,&pin).await,WorkPatrolSourceOutcome::VersionMatched{content,..} if content==doc.body)
    );
    let foreign:ProjectMemoryRef=serde_json::from_value(serde_json::json!({"project_id":b,"memory_id":pin.memory_id(),"content_version":pin.content_version()})).unwrap();
    assert!(matches!(
        read(&memory, &b, &foreign).await,
        WorkPatrolSourceOutcome::Missing
    ));
    memory
        .for_project(&a)
        .merge_memory_with_retrieval(
            pin.memory_id(),
            Some(a.as_str()),
            "Changed canonical source.",
            &[],
            &MemoryRetrievalInput::default(),
            None,
            "fixture",
            &[],
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(read(&memory,&a,&pin).await,WorkPatrolSourceOutcome::VersionMismatch{observed_version} if observed_version!=pin.content_version())
    );
    memory
        .for_project(&a)
        .archive_memory(
            pin.memory_id(),
            Some(a.as_str()),
            DurableMemoryStatus::Archived,
            None,
        )
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        read(&memory, &a, &pin).await,
        WorkPatrolSourceOutcome::Inactive {
            status: DurableMemoryStatus::Archived
        }
    ));
    // Mismatched/foreign responses and native I/O errors never expose a body.
    assert!(matches!(
        resolve(
            &pin,
            Ok(ProjectMemoryRead::VersionMatched {
                reference: foreign,
                title: "foreign".into(),
                content: "foreign".into(),
                confidence: None
            })
        ),
        WorkPatrolSourceOutcome::Unavailable
    ));
    assert!(matches!(
        resolve(
            &pin,
            Err(std::io::Error::other("private filesystem detail"))
        ),
        WorkPatrolSourceOutcome::Unavailable
    ));
}

#[tokio::test]
async fn native_and_serialized_source_limits_do_not_clip_or_claim_complete() {
    let root = tempfile::tempdir().unwrap();
    let memory = MemoryStore::new(root.path());
    let project = ProjectId::new();
    let doc = write(&memory, &project, "Serialized limit", &"界".repeat(3000)).await;
    let pin = ProjectMemoryRef::from_document(&doc).unwrap();
    assert!(matches!(
        read(&memory, &project, &pin).await,
        WorkPatrolSourceOutcome::Oversized {
            kind: WorkPatrolEvidenceLimit::SourceBytes,
            ..
        }
    ));
    let doc = write(
        &memory,
        &project,
        "Native limit",
        &"x".repeat(MAX_MAX_CHARS + 1),
    )
    .await;
    let pin = ProjectMemoryRef::from_document(&doc).unwrap();
    assert!(matches!(
        read(&memory, &project, &pin).await,
        WorkPatrolSourceOutcome::Oversized {
            kind: WorkPatrolEvidenceLimit::NativeCharacters,
            ..
        }
    ));
}
