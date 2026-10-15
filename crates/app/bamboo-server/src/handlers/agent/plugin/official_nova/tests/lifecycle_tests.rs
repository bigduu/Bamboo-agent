use super::*;
use chrono::Duration as Age;

fn installed(version: &str, compatible: bool, path: &str) -> String {
    serde_json::json!({
        "success":true,"state":"installed","message":"Fixture App installed",
        "next_action":"Open explicitly; grant permissions manually",
        "path":path,"version":version,"signing":"ad_hoc","development":true,
        "connector_compatible":compatible,
    })
    .to_string()
}

fn start_request(observed_at: chrono::DateTime<Utc>, version: &str) -> lifecycle::AppStartRequest {
    serde_json::from_value(serde_json::json!({
        "observed_at":observed_at,"app_version":version,
    }))
    .unwrap()
}

#[test]
fn start_payload_cannot_supply_install_or_execution_authority() {
    for field in [
        "path",
        "url",
        "command",
        "args",
        "install",
        "grant_permissions",
    ] {
        let mut payload = serde_json::json!({"observed_at":Utc::now(),"app_version":"0.3.1"});
        payload[field] = serde_json::json!("untrusted");
        assert!(serde_json::from_value::<lifecycle::AppStartRequest>(payload).is_err());
    }
    assert!(serde_json::from_value::<lifecycle::AppStartRequest>(
        serde_json::json!({"app_version":"0.3.1"})
    )
    .is_err());
}

#[cfg(unix)]
#[actix_web::test]
async fn passive_layers_do_not_promote_installed_app_to_runtime_or_permission_readiness() {
    let body = format!(
        "test \"$*\" = 'app status'\nprintf '%s\\n' '{}'",
        installed("0.3.1", true, "/Applications/Nova.app")
    );
    let (_dir, state, catalog, _entry) = fixture(&body).await;
    let before = serde_json::to_value(&state.config.read().await.mcp).unwrap();
    let report = inspect_locked(&state, mac(), &catalog)
        .await
        .unwrap()
        .report;
    assert!(report.start.available);
    assert!(!report.install.available);
    let json = serde_json::to_value(&report).unwrap();
    assert_eq!(json["desktop_readiness"], "not_tested");
    assert_eq!(json["observation"]["app_runtime"], "not_verified");
    assert_eq!(json["observation"]["permissions"], "not_verified");
    assert_eq!(
        json["observation"]["mcp"]["configuration_applied"],
        "not_verified"
    );
    assert_eq!(json["observation"]["mcp"]["server_id"], "nova");
    assert_eq!(json["observation"]["mcp"]["state"], "not_observed");
    assert_eq!(
        serde_json::to_value(&state.config.read().await.mcp).unwrap(),
        before
    );
}

#[cfg(unix)]
#[actix_web::test]
async fn expired_future_or_changed_version_never_reaches_launcher() {
    let report = installed("0.3.1", true, "/Applications/Nova.app");
    let (_dir, state, catalog, _entry) = fixture(&format!("printf '%s\\n' '{report}'")).await;
    for (time, version, reason) in [
        (
            Utc::now() - Age::seconds(61),
            "0.3.1",
            "observation_expired",
        ),
        (Utc::now() + Age::minutes(5), "0.3.1", "observation_expired"),
        (Utc::now(), "0.2.1", "app_changed"),
        (Utc::now(), "", "app_changed"),
    ] {
        let result = lifecycle::start_locked_with(
            &state,
            mac(),
            &catalog,
            start_request(time, version),
            |_| async {
                panic!("rejected observation must not start an App");
            },
        )
        .await;
        assert_eq!(result.err(), Some(reason));
    }
}

#[cfg(unix)]
#[actix_web::test]
async fn missing_incompatible_or_non_nova_path_never_reaches_launcher() {
    for (report, reason) in [
        (missing().to_owned(), "app_not_installed"),
        (
            installed("0.2.1", false, "/Applications/Nova.app"),
            "app_version_mismatch",
        ),
        (
            installed("0.3.2", true, "/Applications/Nova.app"),
            "app_version_mismatch",
        ),
        (
            installed("0.3.1", true, "/Applications/Other.app"),
            "invalid_app_path",
        ),
        (
            installed("0.3.1", true, "Applications/Nova.app"),
            "invalid_app_path",
        ),
        (
            installed("0.3.1", true, "/tmp/Applications/../Applications/Nova.app"),
            "invalid_app_path",
        ),
    ] {
        let (_dir, state, catalog, _entry) = fixture(&format!("printf '%s\\n' '{report}'")).await;
        let result = lifecycle::start_locked_with(
            &state,
            mac(),
            &catalog,
            start_request(Utc::now(), "0.3.1"),
            |_| async {
                panic!("unavailable App must not start");
            },
        )
        .await;
        assert_eq!(result.err(), Some(reason));
    }
}

#[cfg(unix)]
#[actix_web::test]
async fn failed_start_can_retry_without_config_changes_or_false_ready() {
    use std::cell::Cell;
    let report = installed("0.3.1", true, "/Applications/Nova.app");
    let (_dir, state, catalog, _entry) = fixture(&format!("printf '%s\\n' '{report}'")).await;
    let before = serde_json::to_value(&state.config.read().await.mcp).unwrap();
    let attempts = Cell::new(0);
    let count = &attempts;
    let failed = lifecycle::start_locked_with(
        &state,
        mac(),
        &catalog,
        start_request(Utc::now(), "0.3.1"),
        |path| async move {
            assert_eq!(path, Path::new("/Applications/Nova.app"));
            count.set(count.get() + 1);
            Err("start_failed")
        },
    )
    .await;
    assert_eq!(failed.err(), Some("start_failed"));
    let retry = lifecycle::start_locked_with(
        &state,
        mac(),
        &catalog,
        start_request(Utc::now(), "0.3.1"),
        |path| async move {
            assert_eq!(path, Path::new("/Applications/Nova.app"));
            count.set(count.get() + 1);
            Ok(())
        },
    )
    .await
    .unwrap();
    assert_eq!(attempts.get(), 2);
    assert_eq!(retry.desktop_readiness, "not_tested");
    let json = serde_json::to_value(retry).unwrap();
    assert_eq!(json["observation"]["app_runtime"], "not_verified");
    assert_eq!(json["observation"]["permissions"], "not_verified");
    assert_eq!(
        serde_json::to_value(&state.config.read().await.mcp).unwrap(),
        before
    );
}

#[cfg(unix)]
#[actix_web::test]
async fn post_start_observation_replaces_stale_version_and_mcp_binding() {
    let dir = tempfile::tempdir().unwrap();
    let report_file = dir.path().join("owned-app-report.json");
    tokio::fs::write(
        &report_file,
        installed("0.3.1", true, "/Applications/Nova.app"),
    )
    .await
    .unwrap();
    let (_fixture, state, catalog, _entry) =
        fixture(&format!("cat {}", quoted(&report_file))).await;
    let launch_state = state.clone();
    let result = lifecycle::start_locked_with(
        &state,
        mac(),
        &catalog,
        start_request(Utc::now(), "0.3.1"),
        |_| async {
            tokio::fs::write(
                &report_file,
                installed("0.3.2", true, "/Applications/Nova.app"),
            )
            .await
            .unwrap();
            launch_state.config.write().await.mcp.servers.clear();
            Ok(())
        },
    )
    .await
    .unwrap();
    assert!(!result.start.available);
    assert_eq!(result.start.reason, Some("app_version_mismatch"));
    assert_eq!(
        result.app.as_ref().unwrap().version.as_deref(),
        Some("0.3.2")
    );
    assert!(result.desktop_server_id.is_none());
    let json = serde_json::to_value(result).unwrap();
    assert!(json["observation"]["mcp"]["server_id"].is_null());
    assert_eq!(json["desktop_readiness"], "not_tested");
}

#[cfg(unix)]
#[actix_web::test]
async fn cancelled_started_request_retains_ownership_until_start_and_fresh_status_finish() {
    let report = installed("0.3.1", true, "/Applications/Nova.app");
    let (_dir, state, catalog, _entry) = fixture(&format!("printf '%s\\n' '{report}'")).await;
    let installer = ServerPluginInstaller::new(state.clone());
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, wait) = tokio::sync::oneshot::channel();
    let (finished, done) = tokio::sync::oneshot::channel();
    let operation_state = state.clone();
    let request = actix_web::rt::spawn(run_owned_operation(state, async move {
        let result = lifecycle::start_locked_with(
            &operation_state,
            mac(),
            &catalog,
            start_request(Utc::now(), "0.3.1"),
            |_| async move {
                entered.send(()).unwrap();
                wait.await.unwrap();
                Ok(())
            },
        )
        .await;
        finished.send(result.is_ok()).unwrap();
    }));
    tokio::time::timeout(Duration::from_secs(5), started)
        .await
        .unwrap()
        .unwrap();
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    let mut following = Box::pin(installer.begin_operation());
    assert!(futures::poll!(following.as_mut()).is_pending());
    release.send(()).unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(5), done)
        .await
        .unwrap()
        .unwrap());
    let _guard = tokio::time::timeout(Duration::from_secs(5), following)
        .await
        .unwrap();
}
