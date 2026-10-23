//! Actual lifecycle/EOF with a synthetic v2 child. No Chromium or storage image.
use super::*;

const FIXTURE: &str = "BAMBOO_LEASE_FREE_RETIREMENT_FIXTURE";

fn isolated(test: &str, run: impl FnOnce(PathBuf)) {
    if let Some(root) = std::env::var_os(FIXTURE) {
        run(root.into());
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let script = root.path().join("synthetic-clean-host.cjs");
    std::fs::write(&script, HOST).unwrap();
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &format!("browser::lease_free_retirement_tests::{test}"), "--nocapture"])
        .env(FIXTURE, root.path())
        .env("BAMBOO_BROWSER_HOST_SCRIPT", &script)
        .status()
        .unwrap();
    assert!(result.success(), "lease-free lifecycle fixture failed");
}

fn run_async(future: impl std::future::Future<Output = ()>) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future);
}

fn manager(root: &std::path::Path) -> BrowserManager {
    BrowserManager::with_profile_root_for_platform(root.join("profiles"), "linux")
}

fn launch_count(root: &std::path::Path) -> usize {
    std::fs::read_to_string(root.join("launches")).unwrap().lines().count()
}

#[test]
fn normal_lease_free_close_keeps_the_manager_usable() {
    isolated("normal_lease_free_close_keeps_the_manager_usable", |root| run_async(async {
        let browser = manager(&root);
        browser.open_with_mode("clean-a", Some(BrowserProfileMode::Clean)).await.unwrap();
        let core = browser.shared.test_core().await.unwrap();
        assert!(core.profile_lease.lock().unwrap().is_none());
        browser.close("clean-a").await.unwrap();
        assert!(core.close_confirmed.load(Ordering::Acquire));
        assert!(!core.kill_issued.load(Ordering::Acquire));
        browser.lifecycle.ensure_writer_admission().unwrap();
        browser.open_with_mode("clean-b", Some(BrowserProfileMode::Clean)).await.unwrap();
        assert_eq!(launch_count(&root), 2);
        browser.close("clean-b").await.unwrap();
        assert!(!root.join("profiles").join(profile::shared_key()).exists());
        browser.shutdown().await;
    }));
}

#[test]
fn normal_lease_free_reset_keeps_admission_without_account_markers() {
    isolated("normal_lease_free_reset_keeps_admission_without_account_markers", |root| run_async(async {
        let browser = manager(&root);
        browser.open_with_mode("clean-a", Some(BrowserProfileMode::Clean)).await.unwrap();
        drop(browser.reset_profiles().await.unwrap());
        browser.lifecycle.ensure_writer_admission().unwrap();
        browser.open_with_mode("clean-b", Some(BrowserProfileMode::Clean)).await.unwrap();
        assert_eq!(launch_count(&root), 2);
        browser.close("clean-b").await.unwrap();
        assert!(!root.join("profiles").join(profile::shared_key()).exists());
        browser.shutdown().await;
    }));
}

#[test]
fn normal_lease_free_shutdown_is_not_an_unknown_writer() {
    isolated("normal_lease_free_shutdown_is_not_an_unknown_writer", |root| run_async(async {
        let browser = manager(&root);
        browser.open_with_mode("clean-a", Some(BrowserProfileMode::Clean)).await.unwrap();
        let core = browser.shared.test_core().await.unwrap();
        browser.shutdown().await;
        assert!(core.close_confirmed.load(Ordering::Acquire));
        assert!(!browser.lifecycle.unconfirmed_writers.load(Ordering::Acquire));
        assert!(!core.kill_issued.load(Ordering::Acquire));
        assert!(!root.join("profiles").join(profile::shared_key()).exists());
    }));
}

#[test]
fn dropping_a_retiring_lease_free_core_waits_for_normal_eof() {
    isolated("dropping_a_retiring_lease_free_core_waits_for_normal_eof", |root| run_async(async {
        let lifecycle = BrowserLifecycle::default();
        let scratch = create_host_temporary_directory(None).unwrap();
        let scratch_path = scratch.path().to_owned();
        let mut command = Command::new(std::env::var_os("BAMBOO_BROWSER_NODE").unwrap_or_else(|| "node".into()));
        restrict_host_environment(&mut command);
        command.arg(root.join("synthetic-clean-host.cjs")).stdin(Stdio::piped())
            .stdout(Stdio::piped()).stderr(Stdio::inherit()).kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        let core = BrowserSession::from_child_owned(command.spawn().unwrap(), Some(scratch), &lifecycle, None, None).unwrap();
        let cleanup = core.cleanup.clone();
        core.begin_retirement("synthetic lease-free drop").unwrap();
        drop(core);
        tokio::time::timeout(BROWSER_CLOSE_DEADLINE + BROWSER_REAP_DEADLINE, cleanup.wait())
            .await.expect("normal unleased EOF must complete its reaper");
        assert!(!cleanup.is_quarantined());
        lifecycle.ensure_writer_admission().unwrap();
        assert!(!scratch_path.exists());
    }));
}

#[test]
fn nonzero_lease_free_exit_still_retains_scratch_and_blocks_new_writers() {
    isolated("nonzero_lease_free_exit_still_retains_scratch_and_blocks_new_writers", |root| run_async(async {
        std::fs::write(root.join("nonzero"), b"synthetic child exit7").unwrap();
        let browser = manager(&root);
        browser.open_with_mode("clean-a", Some(BrowserProfileMode::Clean)).await.unwrap();
        let core = browser.shared.test_core().await.unwrap();
        let scratch = core.temp_dir.lock().unwrap().as_ref().unwrap().path().to_owned();
        assert!(browser.close("clean-a").await.is_err());
        assert!(core.cleanup.is_quarantined());
        assert!(scratch.exists());
        assert!(browser.lifecycle.ensure_writer_admission().is_err());
        assert!(browser.open_with_mode("clean-b", Some(BrowserProfileMode::Clean)).await.is_err());
        assert_eq!(launch_count(&root), 1);
        assert!(!root.join("profiles").join(profile::shared_key()).exists());
        browser.shutdown().await;
    }));
}

const HOST: &str = r#"#!/usr/bin/env node
// BAMBOO_BROWSER_STORAGE_INSPECTION_V1
// BAMBOO_BROWSER_LAUNCH_ADMISSION_V1
// BAMBOO_BROWSER_EPHEMERAL_CLEAN_V1
const fs = require('node:fs');
const path = require('node:path');
const {randomBytes} = require('node:crypto');
const home = __dirname;
const generation = randomBytes(12).toString('hex');
const instance = randomBytes(12).toString('hex');
fs.appendFileSync(path.join(home, 'launches'), process.pid + '\n');
const input = require('node:readline').createInterface({input:process.stdin});
input.on('line', line => {
  const request = JSON.parse(line);
  const state = {profile_mode:'clean',storage_shared:false,page_epoch:1,tabs:[],active_tab_id:null};
  const result = request.action === 'runtime_state' ? {protocol_version:2,host_generation:generation,
    browser_instance_id:instance,scope_count:0,profile_modes:['clean'],
    storage_boundary:{provider:'ephemeral_clean_v1',persistent:false}} :
    request.action === 'scope_close' ? {closed:true} : state;
  process.stdout.write(JSON.stringify({id:request.id,ok:true,result,version:2,host_generation:generation,
    ...(request.scope_id ? {scope_id:request.scope_id,scope_generation:request.scope_generation} : {})})+'\n');
});
input.on('close', () => {process.exitCode = fs.existsSync(path.join(home, 'nonzero')) ? 7 : 0;});
"#;
