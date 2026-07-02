use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use anyhow::{anyhow, Result};
use rand::Rng;
use sha2::{Digest, Sha256};
use std::process::Command;
#[cfg(not(any(test, feature = "test-utils")))]
use std::sync::OnceLock;

/// Hide console window on Windows. No-op on non-Windows.
fn hide_window_for_std_command(command: &mut Command) {
    #[cfg(target_os = "windows")]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000); // CREATE_NO_WINDOW
    }
    let _ = command;
}

/// Trace a command invocation. No-op in this crate.
fn trace_windows_command<S: AsRef<str>, I: IntoIterator<Item = S>>(
    _scope: &str,
    _program: &str,
    _args: I,
) {
    // No-op
}

const KEY_ENV_VAR: &str = "BAMBOO_CONFIG_ENCRYPTION_KEY";
const KEY_DERIVATION_CONTEXT: &[u8] = b"bamboo-config-encryption-v1";
const KEY_FILE_NAME: &str = ".bamboo_encryption_key";

#[cfg(any(test, feature = "test-utils"))]
use std::cell::RefCell;

// Cache the computed key to avoid repeated syscalls and file IO at runtime.
//
// Important: tests often mutate env vars; we intentionally do not cache in tests.
#[cfg(not(any(test, feature = "test-utils")))]
static KEY_CACHE: OnceLock<Vec<u8>> = OnceLock::new();

// Test-only override to avoid mutating process-wide environment variables (which
// is not thread-safe under concurrent test execution). Thread-local to prevent
// cross-test interference under `cargo test` parallelism.
#[cfg(any(test, feature = "test-utils"))]
thread_local! {
    static TEST_KEY_OVERRIDE: RefCell<Option<Vec<u8>>> = const { RefCell::new(None) };
}

/// Get the encryption key.
/// Priority: environment variable, machine-derived key, then random fallback.
pub fn get_encryption_key() -> Vec<u8> {
    #[cfg(not(any(test, feature = "test-utils")))]
    if let Some(key) = KEY_CACHE.get() {
        return key.clone();
    }

    let key = get_encryption_key_uncached();

    #[cfg(not(any(test, feature = "test-utils")))]
    {
        let _ = KEY_CACHE.set(key.clone());
    }

    key
}

fn get_encryption_key_uncached() -> Vec<u8> {
    #[cfg(any(test, feature = "test-utils"))]
    if let Some(key) = TEST_KEY_OVERRIDE.with(|cell| cell.borrow().clone()) {
        return key;
    }

    if let Some(key) = read_env_key() {
        return key;
    }

    if let Some(key) = read_key_file() {
        return key;
    }

    if let Some(machine_id) = machine_identifier() {
        let key = derive_key(machine_id.as_bytes());
        // Best-effort: persist the derived key so future runs don't depend on host ID lookups.
        let _ = write_key_file(&key);
        return key;
    }

    // Last-resort fallback: generate a key and persist it so encryption remains stable
    // even if host identifiers are unavailable (e.g. sandboxed environments).
    let key = rand::thread_rng().gen::<[u8; 32]>().to_vec();
    let _ = write_key_file(&key);
    key
}

#[cfg(any(test, feature = "test-utils"))]
pub struct TestKeyGuard {
    previous: Option<Vec<u8>>,
}

#[cfg(any(test, feature = "test-utils"))]
impl Drop for TestKeyGuard {
    fn drop(&mut self) {
        TEST_KEY_OVERRIDE.with(|cell| {
            *cell.borrow_mut() = self.previous.clone();
        });
    }
}

#[cfg(any(test, feature = "test-utils"))]
pub fn set_test_encryption_key(key: [u8; 32]) -> TestKeyGuard {
    let previous = TEST_KEY_OVERRIDE.with(|cell| cell.replace(Some(key.to_vec())));
    TestKeyGuard { previous }
}

fn read_env_key() -> Option<Vec<u8>> {
    let key_hex = std::env::var(KEY_ENV_VAR).ok()?;
    let key = hex::decode(key_hex).ok()?;
    (key.len() == 32).then_some(key)
}

fn derive_key(material: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(KEY_DERIVATION_CONTEXT);
    hasher.update(material);
    hasher.finalize().to_vec()
}

fn key_file_path() -> std::path::PathBuf {
    crate::paths::bamboo_dir().join(KEY_FILE_NAME)
}

fn read_key_file() -> Option<Vec<u8>> {
    let path = key_file_path();
    let raw = std::fs::read_to_string(path).ok()?;
    let trimmed = raw.trim();
    let decoded = hex::decode(trimmed).ok()?;
    (decoded.len() == 32).then_some(decoded)
}

fn write_key_file(key: &[u8]) -> Result<()> {
    if key.len() != 32 {
        return Err(anyhow!("Invalid encryption key length: {}", key.len()));
    }

    let path = key_file_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| anyhow!("Failed to create key dir {}: {e}", parent.display()))?;
    }

    // Store as hex for easy inspection/backup. This file is the MASTER key for
    // decrypting all API keys / proxy auth / MCP secrets, so it must NEVER be
    // world/group readable. On Unix, create it 0600 ATOMICALLY (mode applies at
    // creation) rather than fs::write + chmod, which would leave a brief umask-perm
    // (0644) window an attacker could read in. #31.
    let contents = hex::encode(key);
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&path)
            .map_err(|e| anyhow!("Failed to write key file {}: {e}", path.display()))?;
        file.write_all(contents.as_bytes())
            .map_err(|e| anyhow!("Failed to write key file {}: {e}", path.display()))?;
    }
    #[cfg(not(unix))]
    {
        std::fs::write(&path, contents)
            .map_err(|e| anyhow!("Failed to write key file {}: {e}", path.display()))?;
    }
    Ok(())
}

fn machine_identifier() -> Option<String> {
    read_machine_id().or_else(derived_fallback_identifier)
}

fn read_machine_id() -> Option<String> {
    for path in ["/etc/machine-id", "/var/lib/dbus/machine-id"] {
        if let Some(machine_id) = read_trimmed_file(path) {
            return Some(machine_id);
        }
    }

    #[cfg(target_os = "windows")]
    {
        if let Some(machine_id) = read_windows_machine_guid() {
            return Some(machine_id);
        }
    }

    #[cfg(target_os = "macos")]
    {
        if let Some(machine_id) = read_macos_platform_uuid() {
            return Some(machine_id);
        }
    }

    None
}

fn command_with_hidden_window(program: &str) -> Command {
    let mut command = Command::new(program);
    hide_window_for_std_command(&mut command);
    command
}

#[cfg(target_os = "windows")]
fn read_windows_machine_guid() -> Option<String> {
    // Prefer a stable host identifier on Windows so secrets can be decrypted across
    // restarts/builds. Falling back to derived identifiers (like current_exe()) can
    // break decryption when paths change (e.g. Tauri dev builds).
    //
    // Registry: HKLM\SOFTWARE\Microsoft\Cryptography\MachineGuid
    trace_windows_command(
        "core.encryption.read_windows_machine_guid",
        "reg",
        [
            "query",
            r"HKLM\SOFTWARE\Microsoft\Cryptography",
            "/v",
            "MachineGuid",
        ],
    );
    let output = command_with_hidden_window("reg")
        .args([
            "query",
            r"HKLM\SOFTWARE\Microsoft\Cryptography",
            "/v",
            "MachineGuid",
        ])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let stdout = String::from_utf8(output.stdout).ok()?;
    for line in stdout.lines() {
        // Example:
        // MachineGuid    REG_SZ    01234567-89ab-cdef-0123-456789abcdef
        if !line.to_ascii_lowercase().contains("machineguid") {
            continue;
        }
        let parts: Vec<&str> = line.split_whitespace().collect();
        let guid = parts.last()?.trim();
        if guid.is_empty() {
            continue;
        }
        return Some(guid.to_string());
    }

    None
}

fn derived_fallback_identifier() -> Option<String> {
    let mut parts = vec![
        format!("os={}", std::env::consts::OS),
        format!("arch={}", std::env::consts::ARCH),
    ];

    if let Some(hostname) = system_hostname() {
        parts.push(format!("host={hostname}"));
    }
    if let Some(username) = read_first_env_var(&["USER", "USERNAME"]) {
        parts.push(format!("user={username}"));
    }
    if let Some(home) = read_first_env_path(&["HOME", "USERPROFILE"]) {
        parts.push(format!("home={home}"));
    }
    if let Ok(exe_path) = std::env::current_exe() {
        parts.push(format!("exe={}", exe_path.display()));
    }

    (parts.len() > 2).then(|| parts.join("|"))
}

/// The backend machine's hostname, detected once and cached for the process.
/// Falls back to `"localhost"` when detection fails. Used to label which machine
/// a session runs on (the "local" placement host shown in the UI).
pub fn local_hostname() -> &'static str {
    use std::sync::OnceLock;
    static HOST: OnceLock<String> = OnceLock::new();
    HOST.get_or_init(|| system_hostname().unwrap_or_else(|| "localhost".to_string()))
}

fn system_hostname() -> Option<String> {
    if let Some(hostname) = read_first_env_var(&["HOSTNAME", "COMPUTERNAME"]) {
        return Some(hostname);
    }

    if let Some(hostname) = read_trimmed_file("/etc/hostname") {
        return Some(hostname);
    }

    #[cfg(target_os = "macos")]
    {
        if let Some(hostname) = run_command_first_line("scutil", &["--get", "ComputerName"]) {
            return Some(hostname);
        }
        if let Some(hostname) = run_command_first_line("scutil", &["--get", "LocalHostName"]) {
            return Some(hostname);
        }
    }

    run_command_first_line("hostname", &[])
}

fn read_first_env_var(keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        let value = std::env::var(key).ok()?;
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

fn read_first_env_path(keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        let value = std::env::var_os(key)?;
        let value = value.to_string_lossy();
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

fn read_trimmed_file(path: &str) -> Option<String> {
    let value = std::fs::read_to_string(path).ok()?;
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn run_command_first_line(program: &str, args: &[&str]) -> Option<String> {
    trace_windows_command(
        "core.encryption.run_command_first_line",
        program,
        args.iter().copied(),
    );
    let output = command_with_hidden_window(program)
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    let stdout = String::from_utf8(output.stdout).ok()?;
    let line = stdout.lines().next()?.trim();
    (!line.is_empty()).then(|| line.to_string())
}

#[cfg(target_os = "macos")]
fn read_macos_platform_uuid() -> Option<String> {
    let output = command_with_hidden_window("ioreg")
        .args(["-rd1", "-c", "IOPlatformExpertDevice"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    let stdout = String::from_utf8(output.stdout).ok()?;
    extract_quoted_property(&stdout, "IOPlatformUUID")
}

#[cfg(target_os = "macos")]
fn extract_quoted_property(content: &str, key: &str) -> Option<String> {
    content.lines().find_map(|line| {
        if !line.contains(key) {
            return None;
        }

        let mut quoted = line.split('"').skip(1).step_by(2);
        let found_key = quoted.next()?;
        let value = quoted.next()?;
        (found_key == key).then(|| value.trim().to_string())
    })
}

/// Encrypt data.
/// Returns: nonce(12 bytes) + ciphertext.
pub fn encrypt(plaintext: &str) -> Result<String> {
    let key = get_encryption_key();
    let cipher =
        Aes256Gcm::new_from_slice(&key).map_err(|e| anyhow!("Failed to create cipher: {e}"))?;

    let nonce_bytes: [u8; 12] = rand::thread_rng().gen();
    let nonce = Nonce::from(nonce_bytes);

    let ciphertext = cipher
        .encrypt(&nonce, plaintext.as_bytes())
        .map_err(|e| anyhow!("Encryption failed: {e}"))?;

    // Format: hex(nonce) + ":" + hex(ciphertext)
    let result = format!("{}:{}", hex::encode(nonce_bytes), hex::encode(ciphertext));
    Ok(result)
}

/// Decrypt data.
pub fn decrypt(encrypted: &str) -> Result<String> {
    let parts: Vec<&str> = encrypted.split(':').collect();
    if parts.len() != 2 {
        return Err(anyhow!("Invalid encrypted format"));
    }

    let nonce_bytes = hex::decode(parts[0]).map_err(|e| anyhow!("Invalid nonce: {e}"))?;
    let ciphertext = hex::decode(parts[1]).map_err(|e| anyhow!("Invalid ciphertext: {e}"))?;

    if nonce_bytes.len() != 12 {
        return Err(anyhow!(
            "Invalid nonce length: expected 12, got {}",
            nonce_bytes.len()
        ));
    }

    let key = get_encryption_key();
    let cipher =
        Aes256Gcm::new_from_slice(&key).map_err(|e| anyhow!("Failed to create cipher: {e}"))?;

    let nonce_array: [u8; 12] = nonce_bytes.as_slice().try_into().map_err(|_| {
        anyhow!(
            "Invalid nonce length: expected 12, got {}",
            nonce_bytes.len()
        )
    })?;
    let nonce = Nonce::from(nonce_array);
    let plaintext = cipher
        .decrypt(&nonce, ciphertext.as_ref())
        .map_err(|e| anyhow!("Decryption failed: {e}"))?;

    String::from_utf8(plaintext).map_err(|e| anyhow!("Invalid UTF-8: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::sync::Mutex;
    use tempfile::TempDir;

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(key);
            std::env::set_var(key, value);
            Self { key, previous }
        }

        fn unset(key: &'static str) -> Self {
            let previous = std::env::var_os(key);
            std::env::remove_var(key);
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }

    // Delegate to the single crate-wide test lock (see `config::tests::env_lock`)
    // so encryption tests serialize with all other env-mutating tests.
    fn env_lock() -> &'static Mutex<()> {
        crate::test_support::env_cache_lock()
    }

    struct EnvPathGuard {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl EnvPathGuard {
        fn set(key: &'static str, value: &std::path::Path) -> Self {
            let previous = std::env::var_os(key);
            std::env::set_var(key, value);
            Self { key, previous }
        }
    }

    impl Drop for EnvPathGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }

    #[test]
    fn test_encrypt_decrypt() {
        let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let _key = EnvVarGuard::unset(KEY_ENV_VAR);
        let plaintext = "my_secret_password";
        let encrypted = encrypt(plaintext).unwrap();
        let decrypted = decrypt(&encrypted).unwrap();
        assert_eq!(plaintext, decrypted);
    }

    #[test]
    fn test_get_encryption_key_prefers_valid_env_key() {
        let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let expected = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
        let _key = EnvVarGuard::set(KEY_ENV_VAR, expected);

        assert_eq!(get_encryption_key(), hex::decode(expected).unwrap());
    }

    #[test]
    fn test_get_encryption_key_is_stable_without_env_var() {
        let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let _key = EnvVarGuard::unset(KEY_ENV_VAR);

        let first = get_encryption_key();
        let second = get_encryption_key();

        assert_eq!(first.len(), 32);
        assert_eq!(second.len(), 32);
        assert_eq!(first, second);
    }

    #[test]
    fn test_get_encryption_key_ignores_invalid_env_key() {
        let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let _key = EnvVarGuard::set(KEY_ENV_VAR, "abcd");

        let first = get_encryption_key();
        let second = get_encryption_key();

        assert_eq!(first.len(), 32);
        assert_eq!(second.len(), 32);
        assert_eq!(first, second);
    }

    #[test]
    fn test_get_encryption_key_persists_key_file_under_bamboo_data_dir() {
        let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let _env_key = EnvVarGuard::unset(KEY_ENV_VAR);

        let dir = TempDir::new().expect("tempdir");
        let _data_dir = EnvPathGuard::set("BAMBOO_DATA_DIR", dir.path());

        // First call should create the key file (either derived or random).
        let first = get_encryption_key();
        assert_eq!(first.len(), 32);

        let key_path = crate::paths::bamboo_dir().join(KEY_FILE_NAME);
        assert!(key_path.exists(), "expected key file to exist");

        // Subsequent calls must be stable and load the same key.
        let second = get_encryption_key();
        assert_eq!(first, second);
    }

    #[cfg(unix)]
    #[test]
    fn key_file_is_written_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let _env_key = EnvVarGuard::unset(KEY_ENV_VAR);
        let dir = TempDir::new().expect("tempdir");
        let _data_dir = EnvPathGuard::set("BAMBOO_DATA_DIR", dir.path());

        // Creating the key writes the master key file.
        let _ = get_encryption_key();
        let key_path = crate::paths::bamboo_dir().join(KEY_FILE_NAME);
        assert!(key_path.exists(), "expected key file to exist");

        // It must be owner-only (0600) — never world/group readable (#31).
        let mode = std::fs::metadata(&key_path)
            .expect("metadata")
            .permissions()
            .mode();
        assert_eq!(
            mode & 0o777,
            0o600,
            "key file must be 0600, got {:o}",
            mode & 0o777
        );
    }
}
