//! Manual native acceptance using a newly created, bounded HFS+ volume.

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use actix_web::{http::Method, test, web, App};
use bamboo_agent_core::tools::ToolSchema;
use bamboo_domain::{Message, Session};
use bamboo_llm::{Config, LLMChunk, LLMError, LLMProvider, LLMStream, ProviderRegistry};
use chrono::{TimeDelta, Utc};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::{AppState, Operation, StatusCode};

const IMAGE_BYTES: u64 = 48 * 1024 * 1024;
const ID: &str = "archive-enospc-root";
const OLD_INPUT: &str = "Retain this exact historical message.";
const NEW_INPUT: &str = "Continue this session after freeing space.";
const REPLY: &str = "Continuation completed in the original session.";
const MODEL: &str = "archive-enospc-model";
const PROVIDER: &str = "archive-enospc-probe";
const PREVIEW: &str = "/api/v1/session-storage/archive-preview";
const ARCHIVE: &str = "/api/v1/sessions/archive-enospc-root/storage-archive";
const RESTORE: &str = "/api/v1/sessions/archive-enospc-root/storage-archive/restore";
const HISTORY: &str = "/api/v1/sessions/archive-enospc-root/history";
const EXECUTE: &str = "/api/v1/sessions/archive-enospc-root/execute";

struct ScratchVolume {
    scratch: Option<tempfile::TempDir>,
    mount: PathBuf,
}

impl ScratchVolume {
    fn create() -> Self {
        let scratch = tempfile::Builder::new()
            .prefix("bamboo-archive-enospc-")
            .tempdir_in("/private/tmp")
            .unwrap();
        let mount = scratch.path().join("mount");
        fs::create_dir(&mount).unwrap();
        let image = scratch.path().join("archive-test.dmg");
        let volume = Self {
            scratch: Some(scratch),
            mount,
        };
        hdiutil(
            &[
                "create",
                "-size",
                "48m",
                "-layout",
                "NONE",
                "-fs",
                "HFS+",
                "-nospotlight",
                "-volname",
                "BambooArchiveENOSPC",
            ],
            &image,
        )
        .unwrap();
        let output = Command::new("/usr/bin/hdiutil")
            .args(["attach", "-nobrowse", "-noautoopen", "-mountpoint"])
            .arg(&volume.mount)
            .arg(&image)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(volume.is_mounted().unwrap());
        assert_eq!(fs2::total_space(&volume.mount).unwrap(), IMAGE_BYTES);
        eprintln!(
            "native test volume {} capacity_bytes={IMAGE_BYTES}",
            volume.mount.display()
        );
        volume
    }

    fn is_mounted(&self) -> io::Result<bool> {
        let scratch = self.scratch.as_ref().unwrap();
        Ok(fs::metadata(&self.mount)?.dev() != fs::metadata(scratch.path())?.dev())
    }

    fn detach(&self) -> io::Result<()> {
        if self.is_mounted()? {
            hdiutil(&["detach"], &self.mount)?;
        }
        Ok(())
    }
}

impl Drop for ScratchVolume {
    fn drop(&mut self) {
        if let Err(error) = self.detach() {
            // Preserve the backing image if the OS still owns the mount.
            let path = self.scratch.take().unwrap().keep();
            eprintln!(
                "could not detach scratch image in {}: {error}",
                path.display()
            );
        }
    }
}

fn hdiutil(args: &[&str], path: &Path) -> io::Result<()> {
    let result = Command::new("/usr/bin/hdiutil")
        .args(args)
        .arg(path)
        .output()?;
    if result.status.success() {
        Ok(())
    } else {
        Err(io::Error::other(
            String::from_utf8_lossy(&result.stderr).into_owned(),
        ))
    }
}

fn assert_scratch_mount(mount: &Path) {
    let mount = mount.canonicalize().unwrap();
    let scratch = mount.parent().unwrap();
    assert_eq!(scratch.parent(), Some(Path::new("/private/tmp")));
    assert!(scratch
        .file_name()
        .unwrap()
        .to_string_lossy()
        .starts_with("bamboo-archive-enospc-"));
    assert_eq!(mount.file_name().unwrap(), "mount");
    assert!(scratch.join("archive-test.dmg").is_file());
    assert_ne!(
        fs::metadata(&mount).unwrap().dev(),
        fs::metadata(scratch).unwrap().dev(),
    );
    assert_eq!(fs2::total_space(&mount).unwrap(), IMAGE_BYTES);
}

fn fill_volume(mount: &Path) -> PathBuf {
    assert_scratch_mount(mount);
    let filler = mount.join("fixture-filler");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&filler)
        .unwrap();
    let zeros = [0; 64 * 1024];
    // Smaller requests consume any allocation blocks that could not
    // satisfy the larger sequential writes.
    for size in [zeros.len(), 4096, 512] {
        loop {
            match file.write(&zeros[..size]) {
                Ok(written) => {
                    assert!(written > 0 && file.metadata().unwrap().len() <= IMAGE_BYTES);
                }
                Err(error) => {
                    assert_eq!(error.raw_os_error(), Some(libc::ENOSPC), "{error}");
                    break;
                }
            }
        }
    }
    file.sync_all().unwrap();
    assert_eq!(fs2::available_space(mount).unwrap(), 0);
    eprintln!(
        "volume full filler_bytes={}",
        file.metadata().unwrap().len()
    );
    filler
}

fn request(method: Method, uri: &str) -> test::TestRequest {
    test::TestRequest::default()
        .method(method)
        .uri(uri)
        .peer_addr("127.0.0.1:1234".parse().unwrap())
        .insert_header(("Host", "localhost"))
}

async fn require_enospc(state: &AppState, operation: Operation) {
    let error = state
        .session_store
        .maintain_session_archive(ID, Utc::now() - TimeDelta::days(30), operation)
        .await
        .unwrap_err();
    assert_eq!(
        error.raw_os_error(),
        Some(libc::ENOSPC),
        "{operation:?}: {error}"
    );
    eprintln!(
        "{operation:?} native failure errno={} {error}",
        libc::ENOSPC
    );
}

fn checksum(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn verify_files(directory: &Path, main: &[u8], runtime: &[u8], attachment: &[u8]) {
    assert!(
        fs::read(directory.join("session.json")).unwrap() == main,
        "Main changed"
    );
    assert!(
        fs::read(directory.join("runtime.json")).unwrap() == runtime,
        "runtime changed"
    );
    assert_eq!(
        fs::read(directory.join("attachments/fixture.bin")).unwrap(),
        attachment,
    );
    assert!(!fs::read_dir(directory).unwrap().any(|entry| entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .contains(".durable.tmp.")));
    eprintln!(
        "exact bytes Main={} sha256={} runtime={} sha256={} attachment={} sha256={}",
        main.len(),
        checksum(main),
        runtime.len(),
        checksum(runtime),
        attachment.len(),
        checksum(attachment),
    );
}

struct ContinuationProvider;

#[async_trait::async_trait]
impl LLMProvider for ContinuationProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        assert_eq!(model, MODEL);
        assert!(messages.iter().any(|message| message.content == OLD_INPUT));
        assert!(messages.iter().any(|message| message.content == NEW_INPUT));
        Ok(Box::pin(futures::stream::iter([
            Ok(LLMChunk::Token(REPLY.into())),
            Ok(LLMChunk::Done),
        ])))
    }
}

#[actix_web::test]
#[ignore = "requires a native macOS scratch disk image"]
async fn archive_and_restore_retry_after_real_disk_full_in_the_same_session() {
    const CHILD_VOLUME: &str = "BAMBOO_ARCHIVE_ENOSPC_CHILD_VOLUME";
    if let Some(mount) = std::env::var_os(CHILD_VOLUME) {
        let mount = PathBuf::from(mount);
        assert_scratch_mount(&mount);
        exercise_archive_recovery(&mount).await;
        return;
    }

    let volume = ScratchVolume::create();
    // AppState registers process-lifetime handles for the skill store. Keep
    // the image owner in the parent so all handles close before ejecting it.
    let (_, module) = module_path!().split_once("::").unwrap();
    let test_name =
        format!("{module}::archive_and_restore_retry_after_real_disk_full_in_the_same_session");
    let output = tokio::time::timeout(
        Duration::from_secs(120),
        tokio::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", &test_name, "--ignored", "--nocapture"])
            .env(CHILD_VOLUME, &volume.mount)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("native archive test subprocess timed out")
    .unwrap();
    print!("{}", String::from_utf8_lossy(&output.stdout));
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    volume.detach().unwrap();
    assert!(!volume.is_mounted().unwrap());
    assert!(
        output.status.success(),
        "native archive test subprocess exited with {}",
        output.status
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
}

async fn exercise_archive_recovery(mount: &Path) {
    let home = mount.join("bamboo");
    fs::create_dir(&home).unwrap();
    let state = web::Data::new(
        AppState::new_for_legacy_queued_ingress_test(home.clone())
            .await
            .unwrap(),
    );
    let mut original = Session::new(ID, MODEL);
    original.title = "Original full-disk test session".into();
    original.title_generated = true;
    original.add_message(Message::user(OLD_INPUT));
    let mut answer = Message::assistant("Original response.", None);
    // Exercise a large Main without consuming the mock model's text budget.
    let mut padding = "fixture storage padding\n".repeat(65_000);
    for index in 0..2048u32 {
        use std::fmt::Write as _;
        writeln!(&mut padding, "{}", checksum(&index.to_le_bytes())).unwrap();
    }
    answer.metadata = Some(json!({"archive_enospc_fixture": padding}));
    original.add_message(answer);
    original.created_at = Utc::now() - TimeDelta::days(31);
    original.updated_at = original.created_at;
    state.storage.save_session(&original).await.unwrap();

    let directory = home.join("sessions").join(ID);
    let main_path = directory.join("session.json");
    OpenOptions::new()
        .write(true)
        .open(&main_path)
        .unwrap()
        .set_modified(original.updated_at.into())
        .unwrap();
    let plain = fs::read(&main_path).unwrap();
    let runtime = fs::read(directory.join("runtime.json")).unwrap();
    let attachment = b"synthetic attachment bytes";
    fs::write(directory.join("attachments/fixture.bin"), attachment).unwrap();
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(crate::routes::configure_routes),
    )
    .await;

    let filler = fill_volume(mount);
    let preview: Value =
        test::call_and_read_body_json(&app, request(Method::GET, PREVIEW).to_request()).await;
    assert_eq!(preview["sessions"][0]["status"], "eligible", "{preview}");
    require_enospc(&state, Operation::Archive).await;
    let failure = test::call_service(&app, request(Method::POST, ARCHIVE).to_request()).await;
    assert_eq!(failure.status(), StatusCode::SERVICE_UNAVAILABLE);
    let error: Value = test::read_body_json(failure).await;
    assert_eq!(error["error"]["code"], "session_archive_unavailable");
    verify_files(&directory, &plain, &runtime, attachment);
    fs::remove_file(filler).unwrap();
    eprintln!(
        "space freed after archive failure bytes={}",
        fs2::available_space(&home).unwrap()
    );

    let archived: Value =
        test::call_and_read_body_json(&app, request(Method::POST, ARCHIVE).to_request()).await;
    assert_eq!(archived["status"], "archived", "{archived}");
    let cold = fs::read(&main_path).unwrap();
    assert!(cold.len() < plain.len());
    verify_files(&directory, &cold, &runtime, attachment);

    let filler = fill_volume(mount);
    require_enospc(&state, Operation::Restore).await;
    let failure = test::call_service(&app, request(Method::POST, RESTORE).to_request()).await;
    assert_eq!(failure.status(), StatusCode::SERVICE_UNAVAILABLE);
    let error: Value = test::read_body_json(failure).await;
    assert_eq!(error["error"]["code"], "session_archive_unavailable");
    let history: Value =
        test::call_and_read_body_json(&app, request(Method::GET, HISTORY).to_request()).await;
    assert_eq!(history["messages"][0]["id"], original.messages[0].id);
    assert_eq!(history["messages"][0]["content"], OLD_INPUT);
    verify_files(&directory, &cold, &runtime, attachment);
    fs::remove_file(filler).unwrap();
    eprintln!(
        "space freed after restore failure bytes={}",
        fs2::available_space(&home).unwrap()
    );

    let restored: Value =
        test::call_and_read_body_json(&app, request(Method::POST, RESTORE).to_request()).await;
    assert_eq!(restored["status"], "restored", "{restored}");
    verify_files(&directory, &plain, &runtime, attachment);
    state.shutdown().await;
    drop(app);
    drop(state);

    let provider: Arc<dyn LLMProvider> = Arc::new(ContinuationProvider);
    let mut config = Config::from_data_dir(Some(home.clone()));
    config.provider = PROVIDER.into();
    config.features.provider_model_ref = true;
    config.defaults =
        Some(serde_json::from_value(json!({"chat":{"provider":PROVIDER,"model":MODEL}})).unwrap());
    config
        .memory_mut()
        .get_or_insert_with(Default::default)
        .auto_dream_enabled = false;
    let state = web::Data::new(
        AppState::new_with_provider_for_legacy_ingress_test(home.clone(), config, provider.clone())
            .await
            .unwrap(),
    );
    state.provider_registry.replace_with(ProviderRegistry::new(
        HashMap::from([(PROVIDER.into(), provider)]),
        PROVIDER.into(),
    ));
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(crate::routes::configure_routes),
    )
    .await;

    for (uri, body) in [
        (
            "/api/v1/chat",
            json!({"session_id":ID,"provider":PROVIDER,"model":MODEL,"message":NEW_INPUT}),
        ),
        (EXECUTE, json!({"provider":PROVIDER,"model":MODEL})),
    ] {
        let response =
            test::call_service(&app, request(Method::POST, uri).set_json(body).to_request()).await;
        let status = response.status();
        let body = test::read_body(response).await;
        assert!(
            status.is_success(),
            "{uri} {status}: {}",
            String::from_utf8_lossy(&body)
        );
    }
    let continued = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let current = state.storage.load_session(ID).await.unwrap().unwrap();
            let finished = state
                .agent_runners
                .read()
                .await
                .get(ID)
                .is_some_and(|runner| runner.completed_at.is_some());
            if finished {
                assert!(
                    current
                        .messages
                        .iter()
                        .any(|message| message.content == REPLY),
                    "execution ended without its local reply: {:?}",
                    current.last_run_error()
                );
                return current;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("local provider response was not committed");
    assert_eq!(continued.id, ID);
    assert_eq!(
        continued.root_mode_birth_token(),
        original.root_mode_birth_token()
    );
    for message in &original.messages {
        assert!(continued
            .messages
            .iter()
            .any(|retained| retained.id == message.id
                && retained.content == message.content
                && retained.metadata == message.metadata));
    }
    assert!(continued
        .messages
        .iter()
        .any(|message| message.content == NEW_INPUT));
    assert_eq!(
        fs::read(directory.join("attachments/fixture.bin")).unwrap(),
        attachment
    );
    eprintln!(
        "continued with a committed local model reply session_id={} birth={}",
        continued.id,
        continued.root_mode_birth_token()
    );
    state.session_store.flush_search_index().await;
    state.shutdown().await;
    drop(app);
    drop(state);
}
