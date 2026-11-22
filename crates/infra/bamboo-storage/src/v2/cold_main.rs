//! Lossless encoding of the canonical Main file. The original compact authority
//! section stays readable at the front of both encodings, on the same descriptor.

use std::io::{self, Read};
use std::path::Path;

use sha2::{Digest, Sha256};

use super::compact_main;

pub(super) const MAX_ARCHIVE_MAIN_BYTES: usize = 64 * 1024 * 1024;
const MAGIC: &[u8] = b"\0BAMBOO-MAIN-ZSTD\x01";
const LENGTH_BYTES: usize = size_of::<u64>();
const DIGEST_BYTES: usize = 32;
const ZSTD_LEVEL: i32 = 3;

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn authority_end(bytes: &[u8]) -> Option<usize> {
    let end = compact_main::section_length(bytes.get(..compact_main::HEADER_BYTES)?).ok()?;
    (end <= compact_main::SECTION_CAP).then_some(end)
}

fn archive_start(bytes: &[u8]) -> Option<usize> {
    let end = authority_end(bytes)?;
    // A NUL cannot start a valid JSON suffix. Reserve it for the archive marker,
    // including unknown versions and incomplete markers.
    (bytes.get(end) == Some(&0)).then_some(end)
}

pub(super) fn is_archived(bytes: &[u8]) -> bool {
    archive_start(bytes).is_some()
}

/// Inspect the bounded framing alone. A classification never grants read or
/// write authority; full readers validate the checksum and canonical JSON.
pub(super) async fn file_is_archived(file: &mut tokio::fs::File) -> io::Result<bool> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    let mut header = [0; compact_main::HEADER_BYTES];
    match file.read_exact(&mut header).await {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(false),
        Err(error) => return Err(error),
    }
    let Some(end) = authority_end(&header) else {
        return Ok(false);
    };
    file.seek(io::SeekFrom::Start(end as u64)).await?;
    let mut marker = [0];
    Ok(file.read(&mut marker).await? == 1 && marker[0] == 0)
}

pub(super) fn encode(plain: &[u8]) -> io::Result<Vec<u8>> {
    let end = authority_end(plain)
        .filter(|end| *end < plain.len())
        .ok_or_else(|| invalid("Session Main has no compact authority section"))?;
    if plain.len() > MAX_ARCHIVE_MAIN_BYTES || is_archived(plain) {
        return Err(invalid("Session Main cannot be archived"));
    }
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&plain[..end]);
    encoded.extend_from_slice(MAGIC);
    encoded.extend_from_slice(&(plain.len() as u64).to_le_bytes());
    encoded.extend_from_slice(&Sha256::digest(plain));
    zstd::stream::copy_encode(&plain[end..], &mut encoded, ZSTD_LEVEL)?;
    Ok(encoded)
}

pub(super) fn decode(bytes: Vec<u8>) -> io::Result<Vec<u8>> {
    decode_bounded(bytes, MAX_ARCHIVE_MAIN_BYTES)
}

pub(super) fn decode_bounded(bytes: Vec<u8>, limit: usize) -> io::Result<Vec<u8>> {
    let Some(end) = archive_start(&bytes) else {
        return Ok(bytes);
    };
    let archive = bytes[end..]
        .strip_prefix(MAGIC)
        .ok_or_else(|| invalid("Unsupported Session Main archive encoding"))?;
    let (length, archive) = archive
        .split_at_checked(LENGTH_BYTES)
        .ok_or_else(|| invalid("Truncated Session Main archive length"))?;
    let (digest, body) = archive
        .split_at_checked(DIGEST_BYTES)
        .ok_or_else(|| invalid("Truncated Session Main archive checksum"))?;
    let length = usize::try_from(u64::from_le_bytes(length.try_into().unwrap()))
        .ok()
        .filter(|length| (end + 1..=limit.min(MAX_ARCHIVE_MAIN_BYTES)).contains(length))
        .ok_or_else(|| invalid("Invalid Session Main archive length"))?;
    let mut decoder = zstd::stream::read::Decoder::with_buffer(body)?;
    decoder.window_log_max(26)?;
    let mut plain = Vec::with_capacity(length);
    plain.extend_from_slice(&bytes[..end]);
    decoder
        .take((length - end + 1) as u64)
        .read_to_end(&mut plain)?;
    if plain.len() != length || &Sha256::digest(&plain)[..] != digest {
        return Err(invalid("Session Main archive checksum mismatch"));
    }
    Ok(plain)
}

pub(super) async fn read(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    decode_async(tokio::fs::read(path).await?).await
}

pub(super) async fn decode_async(bytes: Vec<u8>) -> io::Result<Vec<u8>> {
    if !is_archived(&bytes) {
        return Ok(bytes);
    }
    tokio::task::spawn_blocking(move || decode(bytes)).await?
}

/// Callers of this helper have already checked that the path is a regular
/// state file. Only the canonical Main filename admits the archive encoding.
pub(super) async fn read_state_file(path: &Path) -> io::Result<Vec<u8>> {
    if path.file_name().is_some_and(|name| name == "session.json") {
        read(path).await
    } else {
        tokio::fs::read(path).await
    }
}

pub(super) async fn read_text(path: impl AsRef<Path>) -> io::Result<String> {
    String::from_utf8(read(path).await?)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}
