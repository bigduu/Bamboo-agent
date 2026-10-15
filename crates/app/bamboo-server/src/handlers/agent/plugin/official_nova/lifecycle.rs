//! Explicit Nova App start and separately observed readiness.
//! No status read starts an App, probes desktop permissions, or connects MCP.

use super::*;
use chrono::{DateTime, Utc};

const MAX_OBSERVATION_AGE: chrono::Duration = chrono::Duration::seconds(60);
const START_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Serialize)]
pub(super) struct Observation {
    schema_version: u8,
    observed_at: DateTime<Utc>,
    app_runtime: &'static str,
    permissions: &'static str,
    mcp: McpObservation,
}

#[derive(Serialize)]
struct McpObservation {
    server_id: Option<String>,
    state: String,
    tool_count: usize,
    configuration_applied: &'static str,
}

pub(super) fn observe(state: &AppState, desktop_id: Option<&str>) -> Observation {
    let runtime = desktop_id.and_then(|id| state.mcp_manager.snapshot().server_info(id));
    Observation {
        schema_version: 1,
        observed_at: Utc::now(),
        // A filesystem report or MCP handshake is not an App PID/TCC probe.
        app_runtime: "not_verified",
        permissions: "not_verified",
        mcp: McpObservation {
            server_id: desktop_id.map(str::to_owned),
            state: runtime
                .as_ref()
                .map_or_else(|| "not_observed".into(), |info| info.status.to_string()),
            tool_count: runtime.as_ref().map_or(0, |info| info.tool_count),
            // The manager may still be adopting a desired configuration.
            // Do not equate a saved row or an older connection with adoption.
            configuration_applied: "not_verified",
        },
    }
}

fn launch_path(report: &AppReport) -> Option<&Path> {
    // The path comes only from the reviewed, hash-verified producer CLI, never
    // from a request/manifest. Retain its finite Nova bundle shape as well.
    report.path.as_deref().filter(|path| {
        path.is_absolute()
            && path.file_name().is_some_and(|name| name == "Nova.app")
            && path
                .parent()
                .and_then(Path::file_name)
                .is_some_and(|name| name == "Applications")
            && !path
                .components()
                .any(|part| matches!(part, std::path::Component::ParentDir))
    })
}

pub(super) fn start_availability(
    host: &Host,
    connector: &Connector,
    capability: &Capability,
    app: Option<&AppReport>,
) -> Availability {
    let reason = host
        .reason
        .or_else(|| (!host.supported).then_some("unsupported_host"))
        .or(connector.reason)
        .or_else(|| (connector.state != "verified").then_some("unverified_install"))
        .or(capability.reason)
        .or_else(|| (!capability.supported).then_some("app_status_unavailable"))
        .or_else(|| {
            let Some(app) = app else {
                return Some("app_status_unavailable");
            };
            if !app.success || app.state != AppStateKind::Installed {
                return Some("app_not_installed");
            }
            if app.connector_compatible != Some(true) || app.version != connector.version {
                return Some("app_version_mismatch");
            }
            launch_path(app).is_none().then_some("invalid_app_path")
        });
    Availability {
        available: reason.is_none(),
        reason,
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppStartRequest {
    observed_at: DateTime<Utc>,
    app_version: String,
}

fn checked_path<'a>(
    report: &'a Setup,
    request: &AppStartRequest,
    now: DateTime<Utc>,
) -> Result<&'a Path, &'static str> {
    let age = now.signed_duration_since(request.observed_at);
    if age < chrono::Duration::zero() || age > MAX_OBSERVATION_AGE {
        return Err("observation_expired");
    }
    if !report.start.available {
        return Err(report.start.reason.unwrap_or("start_unavailable"));
    }
    let app = report.app.as_ref().ok_or("app_status_unavailable")?;
    if request.app_version.is_empty()
        || request.app_version.len() > 128
        || app.version.as_deref() != Some(request.app_version.as_str())
    {
        return Err("app_changed");
    }
    launch_path(app).ok_or("invalid_app_path")
}

pub(super) async fn start_locked_with<F, Fut>(
    state: &web::Data<AppState>,
    host: Host,
    catalog: &Catalog,
    request: AppStartRequest,
    launch: F,
) -> Result<Setup, &'static str>
where
    F: FnOnce(PathBuf) -> Fut,
    Fut: Future<Output = Result<(), &'static str>>,
{
    let fresh = inspect_locked(state, host.clone(), catalog)
        .await
        .map_err(|_| "invalid_registry")?;
    let path = checked_path(&fresh.report, &request, Utc::now())?.to_owned();
    // Keep the verified snapshot alive through the explicit operation.
    let _command = fresh.command.ok_or("unverified_install")?;
    launch(path).await?;
    // Starting does not imply running, permission grants, or connectivity.
    inspect_locked(state, host, catalog)
        .await
        .map(|fresh| fresh.report)
        .map_err(|_| "invalid_registry")
}

async fn launch_native(path: PathBuf) -> Result<(), &'static str> {
    #[cfg(target_os = "macos")]
    {
        let mut command = tokio::process::Command::new("/usr/bin/open");
        command
            .args([
                OsString::from("-g"),
                OsString::from("-a"),
                path.into_os_string(),
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let status = tokio::time::timeout(START_TIMEOUT, command.status())
            .await
            .map_err(|_| "start_timeout")?
            .map_err(|_| "start_failed")?;
        if status.success() {
            Ok(())
        } else {
            Err("start_failed")
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (path, START_TIMEOUT);
        Err("unsupported_host")
    }
}

/// Explicit native start only. The existing operation owner retains cleanup
/// after a started request is cancelled, while queued cancellation does no work.
pub async fn start_nova_app(
    state: web::Data<AppState>,
    body: web::Json<AppStartRequest>,
) -> HttpResponse {
    let operation_state = state.clone();
    let request = body.into_inner();
    match run_owned_operation(state, async move {
        start_locked_with(
            &operation_state,
            Host::detect(),
            &accepted_catalog(),
            request,
            launch_native,
        )
        .await
    })
    .await
    {
        Ok(Ok(report)) => HttpResponse::Accepted().json(serde_json::json!({
            "action": "start_requested", "setup": report,
        })),
        result => {
            let reason = match result {
                Ok(Err(reason)) => reason,
                _ => "start_failed",
            };
            HttpResponse::Conflict().json(serde_json::json!({
                "error": {"type": "official_nova_unavailable", "reason": reason},
            }))
        }
    }
}
