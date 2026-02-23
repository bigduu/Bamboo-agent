use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use anyhow::{anyhow, Result};
use rand::Rng;
use sha2::{Digest, Sha256};
use std::process::Command;

const KEY_ENV_VAR: &str = "BAMBOO_CONFIG_ENCRYPTION_KEY";
const KEY_DERIVATION_CONTEXT: &[u8] = b"bamboo-config-encryption-v1";

/// Get the encryption key.
/// Priority: environment variable, machine-derived key, then random fallback.
pub fn get_encryption_key() -> Vec<u8> {
    if let Some(key) = read_env_key() {
        return key;
    }

    if let Some(machine_id) = machine_identifier() {
        return derive_key(machine_id.as_bytes());
    }

    // Last-resort fallback keeps behavior safe if host identifiers are unavailable.
    rand::thread_rng().gen::<[u8; 32]>().to_vec()
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

fn machine_identifier() -> Option<String> {
    read_machine_id().or_else(derived_fallback_identifier)
}

fn read_machine_id() -> Option<String> {
    for path in ["/etc/machine-id", "/var/lib/dbus/machine-id"] {
        if let Some(machine_id) = read_trimmed_file(path) {
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
    let output = Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }

    let stdout = String::from_utf8(output.stdout).ok()?;
    let line = stdout.lines().next()?.trim();
    (!line.is_empty()).then(|| line.to_string())
}

#[cfg(target_os = "macos")]
fn read_macos_platform_uuid() -> Option<String> {
    let output = Command::new("ioreg")
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
        return Err(anyhow!("Invalid nonce length: expected 12, got {}", nonce_bytes.len()));
    }

    let key = get_encryption_key();
    let cipher =
        Aes256Gcm::new_from_slice(&key).map_err(|e| anyhow!("Failed to create cipher: {e}"))?;

    let nonce_array: [u8; 12] = nonce_bytes
        .try_into()
        .expect("nonce length checked above");
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
    use std::sync::{Mutex, OnceLock};

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

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
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
}
