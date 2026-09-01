use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use actix_web::web;
use bamboo_agent_core::tools::{FunctionCall, ToolCall};
use bamboo_agent_core::ToolExecutionContext;
use bamboo_config::PluginTrustConfig;
use bamboo_plugin::{
    platform_bin_path, InstallDisposition, ObservationPermissionId, Platform, PluginInstaller,
    OBSERVE_METADATA_PERMISSION, OBSERVE_PATHS_PERMISSION,
};
use bamboo_plugin_protocol::{
    FileChangedV1, ProjectedToolEventV1, ToolEventContextV1, ToolEventPublisher, ToolEventV1,
    TOOL_EVENT_PATH_REDACTION_PERMISSION_NOT_GRANTED, TOOL_EVENT_PATH_REDACTION_SENSITIVE,
};
use bamboo_server::app_state::{AppState, MemoryStore};
use bamboo_server::plugin_installer::ServerPluginInstaller;
use bamboo_server::plugin_source::{
    install_server_plugin_from_source_with_event_sink_grants, PluginSourceInput,
};
use bamboo_server::tool_event_policy::EventSinkGrantRequest;
use bamboo_server::tool_event_router::{ToolEventSinkState, ToolEventSinkStatusSnapshot};
use bamboo_server::tools::ToolSurface;
use serde_json::json;

const PLUGIN_ID: &str = "tool-event-recorder";
const SERVICE_ID: &str = "tool-event-recorder-service";
const SINK_ID: &str = "tool-event-recorder-events";
const WAIT: Duration = Duration::from_secs(15);

async fn test_app_state(data_dir: PathBuf) -> AppState {
    AppState::new_with_memory_store(data_dir.clone(), MemoryStore::new(data_dir.join("jiandu")))
        .await
        .expect("test AppState should initialize")
}

struct RecorderFiles {
    output: PathBuf,
    startups: PathBuf,
    crash_marker: PathBuf,
}

impl RecorderFiles {
    fn under(root: &Path) -> Self {
        Self {
            output: root.join("observed.ndjson"),
            startups: root.join("starts.log"),
            crash_marker: root.join("crashed-once.marker"),
        }
    }
}

async fn write_recorder_config(
    data_dir: &Path,
    files: &RecorderFiles,
    startup_delay_ms: u64,
    crash_once: bool,
) {
    let path = data_dir
        .join("plugin_service_config")
        .join(PLUGIN_ID)
        .join("config.json");
    tokio::fs::create_dir_all(path.parent().unwrap())
        .await
        .unwrap();
    let mut value = json!({
        "output_path": files.output,
        "startup_log_path": files.startups,
        "startup_delay_ms": startup_delay_ms
    });
    if crash_once {
        value["crash_once_marker_path"] = json!(files.crash_marker);
    }
    tokio::fs::write(path, serde_json::to_vec_pretty(&value).unwrap())
        .await
        .unwrap();
}

async fn stage_bundle(source: &Path, version: &str, requested_permissions: &[&str]) -> PathBuf {
    tokio::fs::create_dir_all(source).await.unwrap();
    let example_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut manifest: serde_json::Value = serde_json::from_slice(
        &tokio::fs::read(example_root.join("plugin.json"))
            .await
            .unwrap(),
    )
    .unwrap();
    manifest["version"] = json!(version);
    manifest["provides"]["event_sinks"][0]["requested_permissions"] = json!(requested_permissions);
    tokio::fs::write(
        source.join("plugin.json"),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .await
    .unwrap();

    let platform = Platform::current().expect("supported Bamboo test platform");
    let binary_path = platform_bin_path(source, PLUGIN_ID, platform);
    tokio::fs::create_dir_all(binary_path.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::copy(
        Path::new(env!("CARGO_BIN_EXE_tool-event-recorder")),
        &binary_path,
    )
    .await
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = tokio::fs::metadata(&binary_path)
            .await
            .unwrap()
            .permissions();
        permissions.set_mode(0o755);
        tokio::fs::set_permissions(&binary_path, permissions)
            .await
            .unwrap();
    }
    source.to_path_buf()
}

async fn install_bundle(
    installer: &ServerPluginInstaller,
    source: PathBuf,
    plugins_root: &Path,
    disposition: InstallDisposition,
    grants: Option<&[EventSinkGrantRequest]>,
) {
    install_server_plugin_from_source_with_event_sink_grants(
        installer,
        PluginSourceInput::LocalDir(source),
        plugins_root,
        &PluginTrustConfig::default(),
        disposition,
        Some(PLUGIN_ID),
        grants,
    )
    .await
    .unwrap();
}

async fn execute_write(state: &AppState, call_id: &str, path: &Path, content: &str) -> Duration {
    let call = ToolCall {
        id: call_id.to_string(),
        tool_type: "function".to_string(),
        function: FunctionCall {
            name: "Write".to_string(),
            arguments: json!({"file_path": path, "content": content}).to_string(),
        },
    };
    let started = Instant::now();
    let result = state
        .tools_for(ToolSurface::Base)
        .execute_with_context(
            &call,
            ToolExecutionContext {
                session_id: Some("recorder-session"),
                root_session_id: Some("recorder-root-session"),
                tool_call_id: &call.id,
                event_tx: None,
                available_tool_schemas: None,
                bypass_permissions: false,
                auto_approve_permissions: false,
                plan_read_only: false,
                can_async_resume: false,
                bash_completion_sink: None,
                pre_parsed_args: None,
            },
        )
        .await
        .unwrap();
    let elapsed = started.elapsed();
    assert!(result.success, "Write failed: {}", result.result);
    assert_eq!(tokio::fs::read_to_string(path).await.unwrap(), content);
    elapsed
}

async fn read_events(path: &Path) -> Vec<ProjectedToolEventV1> {
    let Ok(raw) = tokio::fs::read_to_string(path).await else {
        return Vec::new();
    };
    raw.lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

async fn wait_for_event(path: &Path, call_id: &str) -> ProjectedToolEventV1 {
    let deadline = Instant::now() + WAIT;
    loop {
        let matching: Vec<_> = read_events(path)
            .await
            .into_iter()
            .filter(|event| event.context.tool_call_id == call_id)
            .collect();
        if let Some(event) = matching.first() {
            assert_eq!(matching.len(), 1, "delivery must be at-most-once");
            return event.clone();
        }
        assert!(Instant::now() < deadline, "timed out waiting for {call_id}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_live_sink(
    state: &AppState,
    generation_after: Option<u64>,
) -> ToolEventSinkStatusSnapshot {
    let deadline = Instant::now() + WAIT;
    loop {
        state.tool_event_router.reconcile_once().await;
        let status = state
            .tool_event_router
            .status_for_ids(&[SINK_ID.to_string()])
            .await
            .into_iter()
            .next()
            .unwrap();
        let new_enough = match (status.generation, generation_after) {
            (Some(generation), Some(previous)) => generation > previous,
            (Some(_), None) => true,
            _ => false,
        };
        if status.state == ToolEventSinkState::Live && new_enough {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "sink did not become live: {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_restart(state: &AppState, generation: u64) -> ToolEventSinkStatusSnapshot {
    let deadline = Instant::now() + WAIT;
    loop {
        let status = wait_for_live_sink(state, Some(generation)).await;
        if state
            .service_manager
            .status(SERVICE_ID)
            .await
            .is_some_and(|service| service.restart_count >= 1)
        {
            return status;
        }
        assert!(Instant::now() < deadline, "service did not report restart");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_input_drain(state: &AppState) {
    let deadline = Instant::now() + WAIT;
    loop {
        let drained = state
            .service_manager
            .status(SERVICE_ID)
            .await
            .and_then(|service| service.input)
            .is_some_and(|input| input.accepted_lines == input.written_lines);
        if drained {
            tokio::time::sleep(Duration::from_millis(50)).await;
            return;
        }
        assert!(Instant::now() < deadline, "service input did not drain");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_pressure_output(path: &Path) {
    let deadline = Instant::now() + WAIT;
    loop {
        if read_events(path)
            .await
            .iter()
            .any(|event| event.context.tool_call_id.starts_with("pressure-"))
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "slow recorder never resumed reading pressure events"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn mutation_root() -> tempfile::TempDir {
    let target = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("example package must live under the workspace examples directory")
        .join("target");
    std::fs::create_dir_all(&target).unwrap();
    tempfile::Builder::new()
        .prefix("tool-event-recorder-e2e-")
        .tempdir_in(target)
        .unwrap()
}

fn pressure_event(sequence: usize, root: &Path) -> ToolEventV1 {
    ToolEventV1::file_changed(
        ToolEventContextV1::bounded(
            "pressure-session",
            "pressure-root",
            "Write",
            format!("pressure-{sequence}"),
        )
        .unwrap(),
        FileChangedV1::bounded(
            root.join(format!("pressure-{sequence}.txt"))
                .to_string_lossy()
                .into_owned(),
        )
        .unwrap(),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_recorder_lifecycle_is_bounded_and_generation_safe() {
    let fixture = tempfile::tempdir().unwrap();
    let data_dir = fixture.path().join("data");
    let files = RecorderFiles::under(fixture.path());
    write_recorder_config(&data_dir, &files, 0, true).await;

    let state = web::Data::new(test_app_state(data_dir.clone()).await);
    state.wait_for_boot_reconcile_services().await;
    let installer = ServerPluginInstaller::new(state.clone());
    let plugins_root = data_dir.join("plugins");
    let source_v1 = stage_bundle(
        &fixture.path().join("source-v1"),
        "0.1.0",
        &[OBSERVE_METADATA_PERMISSION],
    )
    .await;
    install_bundle(
        &installer,
        source_v1,
        &plugins_root,
        InstallDisposition::FailIfInstalled,
        None,
    )
    .await;

    let initial = wait_for_live_sink(state.get_ref(), None).await;
    assert_eq!(
        initial
            .requested_permissions
            .iter()
            .map(ObservationPermissionId::as_str)
            .collect::<Vec<_>>(),
        vec![OBSERVE_METADATA_PERMISSION]
    );
    assert_eq!(
        initial
            .granted_permissions
            .iter()
            .map(ObservationPermissionId::as_str)
            .collect::<Vec<_>>(),
        vec![OBSERVE_METADATA_PERMISSION]
    );
    let mutation_dir = mutation_root();
    let metadata_path = mutation_dir.path().join("metadata.txt");
    execute_write(
        state.get_ref(),
        "metadata-write",
        &metadata_path,
        "metadata",
    )
    .await;
    let metadata = wait_for_event(&files.output, "metadata-write").await;
    assert!(metadata.context.tool_name.is_none());
    assert!(metadata.data.path.is_none());
    assert!(metadata.data.diff.is_none());
    assert!(metadata.data.content.is_none());
    assert_eq!(
        metadata.data.path_redaction_reason.as_deref(),
        Some(TOOL_EVENT_PATH_REDACTION_PERMISSION_NOT_GRANTED)
    );
    assert_eq!(
        metadata.observation_policy_generation,
        initial.policy_generation
    );

    let restarted = wait_for_restart(state.get_ref(), initial.generation.unwrap()).await;
    let after_restart_path = mutation_dir.path().join("after-restart.txt");
    execute_write(
        state.get_ref(),
        "after-restart-write",
        &after_restart_path,
        "restart",
    )
    .await;
    let after_restart = wait_for_event(&files.output, "after-restart-write").await;
    assert_eq!(
        after_restart.observation_policy_generation, initial.policy_generation,
        "a process restart must not silently change observation authority"
    );
    assert!(
        tokio::fs::read_to_string(&files.startups)
            .await
            .unwrap()
            .lines()
            .count()
            >= 2
    );

    write_recorder_config(&data_dir, &files, 2_500, true).await;
    let source_v2 = stage_bundle(
        &fixture.path().join("source-v2"),
        "0.2.0",
        &[OBSERVE_METADATA_PERMISSION, OBSERVE_PATHS_PERMISSION],
    )
    .await;
    let grants = [EventSinkGrantRequest {
        sink_id: SINK_ID.to_string(),
        granted_permissions: vec![
            ObservationPermissionId::new(OBSERVE_METADATA_PERMISSION),
            ObservationPermissionId::new(OBSERVE_PATHS_PERMISSION),
        ],
    }];
    install_bundle(
        &installer,
        source_v2,
        &plugins_root,
        InstallDisposition::Upgrade,
        Some(&grants),
    )
    .await;
    let upgraded = wait_for_live_sink(state.get_ref(), Some(restarted.generation.unwrap())).await;
    assert!(upgraded.policy_generation.unwrap() > restarted.policy_generation.unwrap());
    assert_eq!(
        upgraded
            .granted_permissions
            .iter()
            .map(ObservationPermissionId::as_str)
            .collect::<Vec<_>>(),
        vec![OBSERVE_METADATA_PERMISSION, OBSERVE_PATHS_PERMISSION]
    );

    let pressure_deadline = Instant::now() + WAIT;
    let mut sequence = 0;
    loop {
        for _ in 0..256 {
            state
                .tool_event_publisher
                .try_publish(pressure_event(sequence, mutation_dir.path()))
                .unwrap();
            sequence += 1;
        }
        let status = state
            .tool_event_router
            .status_for_ids(&[SINK_ID.to_string()])
            .await
            .into_iter()
            .next()
            .unwrap();
        if status.queue_full >= 1 {
            break;
        }
        assert!(
            Instant::now() < pressure_deadline,
            "queue pressure did not reach a bounded drop"
        );
        tokio::task::yield_now().await;
    }
    let pressure_path = mutation_dir.path().join("write-during-pressure.txt");
    let pressure_latency = execute_write(
        state.get_ref(),
        "write-during-pressure",
        &pressure_path,
        "tool is independent",
    )
    .await;
    assert!(
        pressure_latency < Duration::from_millis(1_500),
        "tool execution waited on a 2.5s-slow sink: {pressure_latency:?}"
    );

    wait_for_pressure_output(&files.output).await;
    wait_for_input_drain(state.get_ref()).await;
    let authorized_path = mutation_dir.path().join("authorized.txt");
    execute_write(
        state.get_ref(),
        "authorized-path-write",
        &authorized_path,
        "authorized",
    )
    .await;
    let authorized = wait_for_event(&files.output, "authorized-path-write").await;
    let normalized_authorized_path = authorized_path.to_string_lossy().replace('\\', "/");
    assert_eq!(
        authorized.data.path.as_deref(),
        Some(normalized_authorized_path.as_str())
    );
    assert!(authorized.data.path_redaction_reason.is_none());
    assert_eq!(
        authorized.observation_policy_generation,
        upgraded.policy_generation
    );

    let sensitive_path = mutation_dir.path().join(".env");
    execute_write(
        state.get_ref(),
        "sensitive-path-write",
        &sensitive_path,
        "SENTINEL_SECRET=must-not-leak",
    )
    .await;
    let sensitive = wait_for_event(&files.output, "sensitive-path-write").await;
    assert!(sensitive.data.path.is_none());
    assert_eq!(
        sensitive.data.path_redaction_reason.as_deref(),
        Some(TOOL_EVENT_PATH_REDACTION_SENSITIVE)
    );
    assert!(sensitive.data.diff.is_none());
    assert!(sensitive.data.content.is_none());
    let sensitive_wire = serde_json::to_string(&sensitive).unwrap();
    assert!(!sensitive_wire.contains("SENTINEL_SECRET"));
    assert!(!sensitive_wire.contains(".env"));

    installer.uninstall(PLUGIN_ID).await.unwrap();
    assert!(!state.service_manager.is_running(SERVICE_ID));
    assert!(!state.tool_event_router.is_enabled());
    assert!(!plugins_root.join(PLUGIN_ID).exists());
    let before = read_events(&files.output).await.len();
    let after_uninstall_path = mutation_dir.path().join("after-uninstall.txt");
    execute_write(
        state.get_ref(),
        "after-uninstall-write",
        &after_uninstall_path,
        "no delivery",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(read_events(&files.output).await.len(), before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boot_reconcile_restarts_the_installed_native_recorder() {
    let fixture = tempfile::tempdir().unwrap();
    let data_dir = fixture.path().join("data");
    let files = RecorderFiles::under(fixture.path());
    write_recorder_config(&data_dir, &files, 0, false).await;
    let mutation_dir = mutation_root();

    let first = web::Data::new(test_app_state(data_dir.clone()).await);
    first.wait_for_boot_reconcile_services().await;
    let first_installer = ServerPluginInstaller::new(first.clone());
    let plugins_root = data_dir.join("plugins");
    let source = stage_bundle(
        &fixture.path().join("boot-source"),
        "0.1.0",
        &[OBSERVE_METADATA_PERMISSION],
    )
    .await;
    install_bundle(
        &first_installer,
        source,
        &plugins_root,
        InstallDisposition::FailIfInstalled,
        None,
    )
    .await;
    wait_for_live_sink(first.get_ref(), None).await;
    let first_path = mutation_dir.path().join("before-boot.txt");
    execute_write(first.get_ref(), "before-boot-write", &first_path, "first").await;
    wait_for_event(&files.output, "before-boot-write").await;
    first
        .tool_event_router
        .unregister_sinks(&[SINK_ID.to_string()])
        .await;
    first
        .service_manager
        .stop_service(SERVICE_ID)
        .await
        .unwrap();
    assert!(!first.service_manager.is_running(SERVICE_ID));
    assert!(!first.tool_event_router.is_enabled());
    drop(first_installer);
    drop(first);

    let second = web::Data::new(test_app_state(data_dir.clone()).await);
    second.wait_for_boot_reconcile_services().await;
    let reconciled = wait_for_live_sink(second.get_ref(), None).await;
    assert_eq!(reconciled.state, ToolEventSinkState::Live);
    let second_path = mutation_dir.path().join("after-boot.txt");
    execute_write(second.get_ref(), "after-boot-write", &second_path, "second").await;
    wait_for_event(&files.output, "after-boot-write").await;
    assert!(
        tokio::fs::read_to_string(&files.startups)
            .await
            .unwrap()
            .lines()
            .count()
            >= 2
    );

    ServerPluginInstaller::new(second.clone())
        .uninstall(PLUGIN_ID)
        .await
        .unwrap();
}
