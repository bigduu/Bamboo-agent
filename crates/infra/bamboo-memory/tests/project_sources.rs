//! Actual native Jiandu files and facade reads; no copied hash or memory index.
use bamboo_memory::memory_store::{MemoryStore, ProjectMemoryRead, ProjectMemoryRef};
use jiandu_memory::memory_store::{
    DurableMemoryStatus, DurableMemoryType, MemoryRetrievalInput, MemoryScope,
};

#[tokio::test]
async fn native_project_source_versions_cross_the_facade_without_body_fallback() {
    let root = tempfile::tempdir().unwrap();
    let project: bamboo_domain::ProjectId = "source-fixture-project".parse().unwrap();
    let native_project = project
        .as_str()
        .parse::<jiandu_memory::ProjectId>()
        .unwrap();
    let native =
        jiandu_memory::memory_store::MemoryStore::new(root.path()).for_project(&native_project);
    let doc = native
        .write_memory(
            MemoryScope::Project,
            Some(project.as_str()),
            DurableMemoryType::Reference,
            "Work source boundary",
            "Canonical adopted source bytes.",
            &[],
            None,
            "fixture",
            false,
            None,
        )
        .await
        .unwrap();
    let pin = ProjectMemoryRef::from_document(&doc).unwrap();
    let facade = MemoryStore::new(root.path());
    let recalled = facade
        .query_project_sources_read_only(&project, "Work source boundary")
        .await
        .unwrap();
    assert_eq!(recalled.items[0].id, pin.memory_id());
    match facade
        .read_project_source_read_only(&project, pin.memory_id(), pin.content_version())
        .await
        .unwrap()
    {
        ProjectMemoryRead::VersionMatched {
            reference,
            content,
            confidence,
            ..
        } => {
            assert_eq!(reference, pin);
            assert_eq!(content, doc.body);
            assert_eq!(confidence, None);
        }
        other => panic!("unexpected native source resolution: {other:?}"),
    }
    let other: bamboo_domain::ProjectId = "foreign-source-project".parse().unwrap();
    assert!(matches!(
        facade
            .read_project_source_read_only(&other, pin.memory_id(), pin.content_version())
            .await
            .unwrap(),
        ProjectMemoryRead::Missing
    ));
    native
        .merge_memory_with_retrieval(
            pin.memory_id(),
            Some(project.as_str()),
            "Current source bytes changed.",
            &[],
            &MemoryRetrievalInput::default(),
            None,
            "fixture",
            &[],
        )
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        facade
            .read_project_source_read_only(&project, pin.memory_id(), pin.content_version())
            .await
            .unwrap(),
        ProjectMemoryRead::VersionMismatch { .. }
    ));
    native
        .archive_memory(
            pin.memory_id(),
            Some(project.as_str()),
            DurableMemoryStatus::Archived,
            Some("fixture retirement"),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        facade
            .read_project_source_read_only(&project, pin.memory_id(), pin.content_version())
            .await
            .unwrap(),
        ProjectMemoryRead::Inactive { .. }
    ));
}

#[tokio::test]
async fn cold_project_source_discovery_never_creates_the_native_root() {
    let root = tempfile::tempdir().unwrap();
    let missing = root.path().join("absent-jiandu");
    let facade = MemoryStore::new(&missing);
    let project: bamboo_domain::ProjectId = "cold-source-project".parse().unwrap();
    assert!(facade
        .query_project_sources_read_only(&project, "source")
        .await
        .unwrap()
        .items
        .is_empty());
    assert!(!missing.exists());
    assert!(facade
        .query_project_sources_read_only(&project, " ")
        .await
        .is_err());
    assert!(!missing.exists());
}
