//! Comprehensive tests for encryption module
//!
//! Tests cover:
//! - Encryption/decryption round trips
//! - Key derivation
//! - Error handling
//! - Edge cases (empty strings, unicode, large data)

use bamboo_agent::core::encryption;
use std::ffi::OsString;
use std::sync::{Mutex, OnceLock};
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

fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[test]
fn test_encrypt_decrypt_simple_string() {
    let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());
    let _key = EnvVarGuard::unset("BAMBOO_CONFIG_ENCRYPTION_KEY");

    let plaintext = "Hello, World!";
    let encrypted = encryption::encrypt(plaintext).unwrap();
    let decrypted = encryption::decrypt(&encrypted).unwrap();

    assert_eq!(plaintext, decrypted);
}

#[test]
fn test_encrypt_decrypt_empty_string() {
    let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());
    let _key = EnvVarGuard::unset("BAMBOO_CONFIG_ENCRYPTION_KEY");

    let plaintext = "";
    let encrypted = encryption::encrypt(plaintext).unwrap();
    let decrypted = encryption::decrypt(&encrypted).unwrap();

    assert_eq!(plaintext, decrypted);
}

#[test]
fn test_encrypt_decrypt_unicode() {
    let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());
    let _key = EnvVarGuard::unset("BAMBOO_CONFIG_ENCRYPTION_KEY");

    let plaintext = "你好世界 🌍 مرحبا العالم";
    let encrypted = encryption::encrypt(plaintext).unwrap();
    let decrypted = encryption::decrypt(&encrypted).unwrap();

    assert_eq!(plaintext, decrypted);
}

#[test]
fn test_encrypt_decrypt_special_characters() {
    let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());
    let _key = EnvVarGuard::unset("BAMBOO_CONFIG_ENCRYPTION_KEY");

    let plaintext = "Special chars: \n\t\r\\\"'@#$%^&*()";
    let encrypted = encryption::encrypt(plaintext).unwrap();
    let decrypted = encryption::decrypt(&encrypted).unwrap();

    assert_eq!(plaintext, decrypted);
}

#[test]
fn test_encrypt_decrypt_large_data() {
    let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());
    let _key = EnvVarGuard::unset("BAMBOO_CONFIG_ENCRYPTION_KEY");

    // 1MB of data
    let plaintext = "x".repeat(1024 * 1024);
    let encrypted = encryption::encrypt(&plaintext).unwrap();
    let decrypted = encryption::decrypt(&encrypted).unwrap();

    assert_eq!(plaintext, decrypted);
}

#[test]
fn test_encrypt_decrypt_multiline() {
    let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());
    let _key = EnvVarGuard::unset("BAMBOO_CONFIG_ENCRYPTION_KEY");

    let plaintext = r#"Line 1
Line 2
Line 3

Line 5 with blank line above"#;
    let encrypted = encryption::encrypt(plaintext).unwrap();
    let decrypted = encryption::decrypt(&encrypted).unwrap();

    assert_eq!(plaintext, decrypted);
}

#[test]
fn test_decrypt_invalid_format_no_colon() {
    let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());
    let _key = EnvVarGuard::unset("BAMBOO_CONFIG_ENCRYPTION_KEY");

    let result = encryption::decrypt("invalidhexstring");
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid encrypted format"));
}

#[test]
fn test_decrypt_invalid_format_wrong_parts() {
    let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());
    let _key = EnvVarGuard::unset("BAMBOO_CONFIG_ENCRYPTION_KEY");

    let result = encryption::decrypt("part1:part2:part3");
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid encrypted format"));
}

#[test]
fn test_decrypt_invalid_nonce_hex() {
    let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());
    let _key = EnvVarGuard::unset("BAMBOO_CONFIG_ENCRYPTION_KEY");

    let result = encryption::decrypt("invalid_hex:validhex1234567890");
    assert!(result.is_err());
}

#[test]
fn test_decrypt_invalid_ciphertext_hex() {
    let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());
    let _key = EnvVarGuard::unset("BAMBOO_CONFIG_ENCRYPTION_KEY");

    let result = encryption::decrypt("0123456789abcdef0123456789abcdef:invalid_hex");
    assert!(result.is_err());
}

#[test]
fn test_decrypt_wrong_nonce_length() {
    let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());
    let _key = EnvVarGuard::unset("BAMBOO_CONFIG_ENCRYPTION_KEY");

    // Nonce should be 12 bytes (24 hex chars), this is only 10 bytes
    let result = encryption::decrypt("0123456789abcdef0123:0123456789abcdef");
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid nonce length"));
}

#[test]
fn test_encrypt_produces_different_ciphertext() {
    let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());
    let _key = EnvVarGuard::unset("BAMBOO_CONFIG_ENCRYPTION_KEY");

    let plaintext = "Same plaintext";
    let encrypted1 = encryption::encrypt(plaintext).unwrap();
    let encrypted2 = encryption::encrypt(plaintext).unwrap();

    // Different nonces should produce different ciphertext
    assert_ne!(encrypted1, encrypted2);

    // But both should decrypt to the same plaintext
    assert_eq!(plaintext, encryption::decrypt(&encrypted1).unwrap());
    assert_eq!(plaintext, encryption::decrypt(&encrypted2).unwrap());
}

#[test]
fn test_encryption_format_structure() {
    let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());
    let _key = EnvVarGuard::unset("BAMBOO_CONFIG_ENCRYPTION_KEY");

    let plaintext = "Test";
    let encrypted = encryption::encrypt(plaintext).unwrap();

    // Should have format: nonce_hex:ciphertext_hex
    let parts: Vec<&str> = encrypted.split(':').collect();
    assert_eq!(parts.len(), 2);

    // Nonce should be 12 bytes = 24 hex characters
    assert_eq!(parts[0].len(), 24);

    // Ciphertext length varies, but should be non-empty
    assert!(!parts[1].is_empty());
}

#[test]
fn test_get_encryption_key_with_invalid_env_var_too_short() {
    let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());

    let _key = EnvVarGuard::set("BAMBOO_CONFIG_ENCRYPTION_KEY", "abcd");

    // Should fall back to alternative methods
    let key = encryption::get_encryption_key();
    assert_eq!(key.len(), 32);
}

#[test]
fn test_get_encryption_key_with_invalid_env_var_not_hex() {
    let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());

    let _key = EnvVarGuard::set(
        "BAMBOO_CONFIG_ENCRYPTION_KEY",
        "not_valid_hex_12345678901234567890123456789012",
    );

    // Should fall back to alternative methods
    let key = encryption::get_encryption_key();
    assert_eq!(key.len(), 32);
}

#[test]
fn test_get_encryption_key_stability() {
    let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());
    let _key = EnvVarGuard::unset("BAMBOO_CONFIG_ENCRYPTION_KEY");

    let key1 = encryption::get_encryption_key();
    let key2 = encryption::get_encryption_key();

    // Should return the same key on successive calls
    assert_eq!(key1, key2);
}

#[test]
fn test_encryption_key_caching() {
    let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());
    let _key = EnvVarGuard::unset("BAMBOO_CONFIG_ENCRYPTION_KEY");

    let key1 = encryption::get_encryption_key();

    // In the runtime build, the key is cached after first resolution; later
    // env changes should not alter it during the same process.
    let _override = EnvVarGuard::set(
        "BAMBOO_CONFIG_ENCRYPTION_KEY",
        "1111111111111111111111111111111111111111111111111111111111111111",
    );
    let key2 = encryption::get_encryption_key();

    assert_eq!(key1, key2);
}

#[test]
fn test_encrypt_decrypt_json_data() {
    let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());
    let _key = EnvVarGuard::unset("BAMBOO_CONFIG_ENCRYPTION_KEY");

    let json = r#"{"name":"test","value":123,"nested":{"key":"value"}}"#;
    let encrypted = encryption::encrypt(json).unwrap();
    let decrypted = encryption::decrypt(&encrypted).unwrap();

    assert_eq!(json, decrypted);
}

#[test]
fn test_encrypt_decrypt_base64_data() {
    let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());
    let _key = EnvVarGuard::unset("BAMBOO_CONFIG_ENCRYPTION_KEY");

    let base64 = "SGVsbG8gV29ybGQhIFRoaXMgaXMgYSBiYXNlNjQgZW5jb2RlZCBzdHJpbmcu";
    let encrypted = encryption::encrypt(base64).unwrap();
    let decrypted = encryption::decrypt(&encrypted).unwrap();

    assert_eq!(base64, decrypted);
}

#[test]
fn test_key_persistence_in_temp_dir() {
    let _lock = env_lock().lock().unwrap_or_else(|e| e.into_inner());
    let _env_key = EnvVarGuard::unset("BAMBOO_CONFIG_ENCRYPTION_KEY");

    let dir = TempDir::new().expect("tempdir");
    let _data_dir = EnvVarGuard::set("BAMBOO_DATA_DIR", dir.path().to_str().unwrap());

    // First call creates the key
    let key1 = encryption::get_encryption_key();
    assert_eq!(key1.len(), 32);

    // Second call should load from file
    let key2 = encryption::get_encryption_key();
    assert_eq!(key1, key2);
}
