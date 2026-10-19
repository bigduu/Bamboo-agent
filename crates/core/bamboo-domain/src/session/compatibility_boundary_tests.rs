//! Existing-API boundaries frozen before the behavior-version implementation.
use super::{Message, Session};
use serde_json::{json, Value};

#[test]
fn session_version_new_root_and_child_are_explicitly_stamped() {
    let root = Session::new("version-root", "model");
    let child = Session::new_child_of("version-child", &root, "model", "worker");
    for session in [root, child] {
        assert_eq!(serde_json::to_value(session).unwrap()["session_version"], json!(1));
    }
}

#[test]
fn session_version_unknown_number_remains_readable_and_roundtrips() {
    let mut original = Session::new("future-history", "model");
    original.add_message(Message::user("Keep this history readable."));
    let mut raw = serde_json::to_value(original).unwrap();
    raw["session_version"] = json!(99);
    let loaded: Session = serde_json::from_value(raw.clone()).unwrap();
    let roundtrip = serde_json::to_value(loaded).unwrap();
    assert_eq!(roundtrip["messages"], raw["messages"]);
    assert_eq!(roundtrip["session_version"], json!(99));
}

#[test]
fn session_version_missing_is_not_automatically_filled_on_legacy_read() {
    let mut raw = serde_json::to_value(Session::new("legacy-history", "model")).unwrap();
    raw.as_object_mut().unwrap().remove("session_version");
    let loaded: Session = serde_json::from_value(raw.clone()).unwrap();
    let roundtrip = serde_json::to_value(loaded).unwrap();
    assert_eq!(roundtrip["messages"], raw["messages"]);
    assert!(roundtrip.get("session_version").is_none());
}

#[test]
fn session_version_legacy_spawn_alias_is_not_silently_reinterpreted() {
    assert_eq!(crate::canonical_tool_name("spawn_session"), "SubAgent");
    assert_eq!(crate::canonical_tool_name("spawnSession"), "SubAgent");
    let raw: Value = serde_json::to_value(Session::new("legacy-root", "model")).unwrap();
    assert_eq!(raw["kind"], "root");
    assert!(raw.get("supervisor_management").is_none());
}
