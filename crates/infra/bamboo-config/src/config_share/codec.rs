use aes_gcm::{
    aead::{Aead, KeyInit, Payload},
    Aes256Gcm, Nonce,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use rand::RngExt;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use zeroize::Zeroizing;

use super::{ShareBundle, ShareError, ShareResult, MAX_SHARE_BUNDLE_BYTES};

pub const SHARE_PREFIX: &str = "BAMBOO-SHARE:1:";
pub const MAX_SHARE_STRING_BYTES: usize = 1024 * 1024;
const MAX_ENVELOPE_BYTES: usize = 768 * 1024;
const ALGORITHM: &str = "scrypt-n17-r8-p1-aes256gcm";
const AAD: &[u8] = b"BAMBOO-SHARE:1:scrypt-n17-r8-p1-aes256gcm";
#[cfg(test)]
thread_local! { static KDF_CALLS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) }; }
#[cfg(test)]
pub(super) fn test_kdf_calls() -> u32 {
    KDF_CALLS.with(std::cell::Cell::get)
}

// No tunable KDF parameters and no compression. Reject all unsupported modes
// before deriving a key. The outer JSON is also bounded before allocation.
#[derive(Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum Envelope {
    Plain {
        bundle: ShareBundle,
    },
    Encrypted {
        algorithm: String,
        salt: String,
        nonce: String,
        ciphertext: String,
    },
}

/// Including any credential or env value requires authenticated encryption.
/// The caller owns the explicit sensitive-data confirmation and must run KDF
/// work in a bounded blocking worker, rather than on an async request executor.
pub fn encode_bundle(bundle: &ShareBundle, passphrase: Option<&str>) -> ShareResult<String> {
    bundle.validate()?;
    let envelope = match passphrase {
        None if bundle.contains_secrets() => return Err(ShareError::PassphraseRequired),
        None => Envelope::Plain {
            bundle: bundle.clone(),
        },
        Some(passphrase) => {
            validate_passphrase(passphrase)?;
            let mut salt = [0u8; 16];
            let mut nonce = [0u8; 12];
            rand::rng().fill(&mut salt);
            rand::rng().fill(&mut nonce);
            let key = derive_key(passphrase, &salt)?;
            let cipher =
                Aes256Gcm::new_from_slice(key.as_ref()).map_err(|_| ShareError::Invalid)?;
            let plaintext =
                Zeroizing::new(serde_json::to_vec(bundle).map_err(|_| ShareError::Invalid)?);
            let ciphertext = cipher
                .encrypt(
                    &Nonce::from(nonce),
                    Payload {
                        msg: &plaintext,
                        aad: AAD,
                    },
                )
                .map_err(|_| ShareError::Authentication)?;
            Envelope::Encrypted {
                algorithm: ALGORITHM.into(),
                salt: URL_SAFE_NO_PAD.encode(salt),
                nonce: URL_SAFE_NO_PAD.encode(nonce),
                ciphertext: URL_SAFE_NO_PAD.encode(ciphertext),
            }
        }
    };
    let bytes = serde_json::to_vec(&envelope).map_err(|_| ShareError::Invalid)?;
    if bytes.len() > MAX_ENVELOPE_BYTES {
        return Err(ShareError::Limit);
    }
    let encoded = format!("{SHARE_PREFIX}{}", URL_SAFE_NO_PAD.encode(bytes));
    if encoded.len() > MAX_SHARE_STRING_BYTES {
        return Err(ShareError::Limit);
    }
    Ok(encoded)
}

/// Decode and authenticate only. This does not hydrate host credentials or
/// apply any change; cancelling a preview requires no rollback.
pub fn decode_bundle(encoded: &str, passphrase: Option<&str>) -> ShareResult<ShareBundle> {
    if encoded.len() > MAX_SHARE_STRING_BYTES {
        return Err(ShareError::Limit);
    }
    let encoded = encoded.trim();
    let body = encoded
        .strip_prefix(SHARE_PREFIX)
        .ok_or(ShareError::Unsupported)?;
    let bytes = URL_SAFE_NO_PAD
        .decode(body)
        .map_err(|_| ShareError::Invalid)?;
    let envelope: Envelope = bounded_json(&bytes, MAX_ENVELOPE_BYTES)?;
    let bundle = match envelope {
        Envelope::Plain { bundle } => {
            if bundle.contains_secrets() {
                return Err(ShareError::Invalid);
            }
            bundle
        }
        Envelope::Encrypted {
            algorithm,
            salt,
            nonce,
            ciphertext,
        } => {
            if algorithm != ALGORITHM {
                return Err(ShareError::Unsupported);
            }
            let salt: [u8; 16] = decode_fixed(&salt)?;
            let nonce: [u8; 12] = decode_fixed(&nonce)?;
            let ciphertext = URL_SAFE_NO_PAD
                .decode(ciphertext)
                .map_err(|_| ShareError::Invalid)?;
            if ciphertext.len() < 16 || ciphertext.len() > MAX_SHARE_BUNDLE_BYTES + 16 {
                return Err(ShareError::Limit);
            }
            let passphrase = passphrase.ok_or(ShareError::PassphraseRequired)?;
            validate_passphrase(passphrase)?;
            let key = derive_key(passphrase, &salt)?;
            let cipher =
                Aes256Gcm::new_from_slice(key.as_ref()).map_err(|_| ShareError::Invalid)?;
            let plaintext = Zeroizing::new(
                cipher
                    .decrypt(
                        &Nonce::from(nonce),
                        Payload {
                            msg: &ciphertext,
                            aad: AAD,
                        },
                    )
                    .map_err(|_| ShareError::Authentication)?,
            );
            bounded_json(&plaintext, MAX_SHARE_BUNDLE_BYTES)?
        }
    };
    bundle.validate()?;
    Ok(bundle)
}

fn decode_fixed<const N: usize>(encoded: &str) -> ShareResult<[u8; N]> {
    if encoded.len() > 32 {
        return Err(ShareError::Limit);
    }
    URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| ShareError::Invalid)?
        .try_into()
        .map_err(|_| ShareError::Invalid)
}

fn derive_key(passphrase: &str, salt: &[u8; 16]) -> ShareResult<Zeroizing<[u8; 32]>> {
    #[cfg(test)]
    KDF_CALLS.with(|calls| calls.set(calls.get() + 1));
    let params = scrypt::Params::new(17, 8, 1).map_err(|_| ShareError::Invalid)?;
    let mut key = Zeroizing::new([0u8; 32]);
    scrypt::scrypt(passphrase.as_bytes(), salt, &params, key.as_mut())
        .map_err(|_| ShareError::Invalid)?;
    Ok(key)
}

fn validate_passphrase(value: &str) -> ShareResult<()> {
    if !(12..=1024).contains(&value.len()) || value.trim().is_empty() {
        return Err(ShareError::PassphraseRequired);
    }
    Ok(())
}

fn bounded_json<T: DeserializeOwned>(bytes: &[u8], max: usize) -> ShareResult<T> {
    if bytes.len() > max {
        return Err(ShareError::Limit);
    }
    // Count only structural bytes outside JSON strings. serde still owns all
    // syntax/UTF-8 validation. This bounds depth before recursive deserialization.
    let (mut depth, mut quoted, mut escaped) = (0usize, false, false);
    for &byte in bytes {
        if quoted {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
        } else {
            match byte {
                b'"' => quoted = true,
                b'{' | b'[' => {
                    depth += 1;
                    if depth > 16 {
                        return Err(ShareError::Limit);
                    }
                }
                b'}' | b']' => depth = depth.checked_sub(1).ok_or(ShareError::Invalid)?,
                _ => {}
            }
        }
    }
    serde_json::from_slice(bytes).map_err(|_| ShareError::Invalid)
}
