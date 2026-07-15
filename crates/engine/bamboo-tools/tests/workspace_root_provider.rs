//! Issue #217 end-to-end test for `bamboo_agent_core::workspace_state`'s
//! `set_workspace_root_provider` wiring — the mechanism the server/SDK
//! composition root uses to point tool cwd resolution at
//! `data_dir/workspaces` instead of the process cwd, and to enable explicit
//! -path confinement.
//!
//! This lives in its own integration-test binary (a separate process from
//! `cargo test -p bamboo-tools --lib`) deliberately: `set_workspace_root_
//! provider` is a process-global `OnceLock` (first-registration-wins), so
//! registering it here would otherwise poison every unit test in the lib
//! binary that assumes the pre-#217 unconfined default (e.g. `bash`/`glob`/
//! `grep`/`workspace` tests that set a workspace to an arbitrary tempdir
//! outside any root and expect it stored verbatim). The pure pin/relocate/
//! default-dir ALGORITHMS are unit-tested exhaustively, with no global state,
//! in `bamboo-agent-core::workspace_state`'s own test module — this file only
//! proves the WIRING (does `workspace_or_process_cwd`/the `Workspace` tool
//! actually consult the registered provider).

use std::path::PathBuf;

use bamboo_agent_core::workspace_state::{self, WorkspaceRootConfig};
use bamboo_agent_core::{Tool, ToolCtx, ToolOutcome};
use bamboo_tools::tools::WorkspaceTool;
use serde_json::json;

fn ctx_for(session: &str) -> ToolCtx {
    ToolCtx {
        session_id: Some(std::sync::Arc::from(session)),
        tool_call_id: std::sync::Arc::from("call_1"),
        event_tx: None,
        available_tool_schemas: std::sync::Arc::from(Vec::new()),
        bypass_permissions: false,
        can_async_resume: false,
        async_completion_sink: None,
        bash_completion_sink: None,
    }
}

/// Both acceptance-criterion angles (default-under-root, explicit-path
/// confinement) are exercised together under the ONE provider policy this
/// process registers — a second test in this file registering a DIFFERENT
/// policy would be racy for the same first-wins-`OnceLock` reason described
/// above.
#[tokio::test]
async fn workspace_provider_wiring_confines_and_defaults_end_to_end() {
    let root_dir = tempfile::tempdir().unwrap();
    let root = root_dir.path().join("workspaces");
    workspace_state::set_workspace_root_provider(Box::new({
        let root = root.clone();
        move || WorkspaceRootConfig {
            root: root.clone(),
            confine: true,
        }
    }));

    // Criterion 1: no explicit workspace path -> default lands under
    // `root/{session}` (created), never the process cwd.
    let session_a = format!("session_{}", uuid::Uuid::new_v4());
    let default_dir = workspace_state::workspace_or_process_cwd(Some(&session_a));
    let canon_root = root.canonicalize().unwrap();
    assert!(
        default_dir.starts_with(&canon_root),
        "default workspace must land under the registered root, got {default_dir:?}"
    );
    assert!(default_dir.is_dir());
    assert_ne!(default_dir, std::env::current_dir().unwrap());

    // Criterion 2: an explicit path outside root, set via the Workspace
    // tool, is relocated under root (confinement is enabled) rather than
    // honored as-is — and the tool reports the ACTUAL stored path.
    let outside = tempfile::tempdir().unwrap();
    let real_project = outside.path().join("my-real-project");
    tokio::fs::create_dir_all(&real_project).await.unwrap();
    let session_b = format!("session_{}", uuid::Uuid::new_v4());

    let tool = WorkspaceTool::new();
    let out = tool
        .invoke(
            json!({"path": real_project.to_string_lossy()}),
            ctx_for(&session_b),
        )
        .await
        .unwrap();
    let ToolOutcome::Completed(result) = out else {
        panic!("expected Completed")
    };
    assert!(result.success);
    let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
    let reported = PathBuf::from(payload["workspace"].as_str().unwrap());
    assert!(
        reported.starts_with(&canon_root),
        "explicit outside-root path must be relocated under the confined root, got {reported:?}"
    );
    assert!(!reported.starts_with(outside.path().canonicalize().unwrap()));
    assert!(
        payload.get("relocated_from").is_some(),
        "response should flag that relocation happened: {payload}"
    );

    // The session's tracked workspace matches what the tool reported —
    // subsequent Bash/Glob/Grep calls in this session see the same dir.
    let tracked = workspace_state::get_workspace(&session_b).unwrap();
    assert_eq!(tracked, reported);
}
