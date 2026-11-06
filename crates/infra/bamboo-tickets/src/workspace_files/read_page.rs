//! Bounded source pages, identified by the hash of one complete physical read.
//! Continuations re-read and verify that hash; no second snapshot store or grant.
use super::*;

pub const READ_SOURCE_BYTES_LIMIT: usize = 1024 * 1024;
// Reserve more than the maximum metadata/envelope size for the bounded numbers
// and fixed hash. The final encoded reply is still checked independently.
const CONTENT_JSON_BYTES: usize = FILE_REPLY_BYTES_LIMIT - 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadPageInfo {
    pub offset_bytes: u64,
    /// Exclusive byte offset; also the next cursor when EOF is false.
    pub end_offset_bytes: u64,
    pub total_bytes: u64,
    pub start_line: u64,
    /// Inclusive last observed line (equal to start_line for an empty EOF page).
    pub end_line: u64,
    pub next_offset_bytes: Option<u64>,
    pub eof: bool,
}

pub(super) fn validate(offset: u64, max_bytes: usize, expected: Option<&str>) -> Result<()> {
    if max_bytes == 0 || max_bytes > FILE_BYTES_LIMIT || offset > READ_SOURCE_BYTES_LIMIT as u64 {
        return Err(Error::InvalidTransition(
            "Read requires max_bytes 1..16384 and offset_bytes within a 1 MiB source".into(),
        ));
    }
    if (offset > 0 && expected.is_none())
        || expected.is_some_and(|hash| {
            hash.len() != 64
                || !hash
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
    {
        return Err(Error::InvalidTransition(
            "Read continuation requires the previous page's full-file sha256 as expected_sha256"
                .into(),
        ));
    }
    Ok(())
}

fn encoded_char_bytes(c: char) -> usize {
    match c {
        '"' | '\\' | '\u{0008}' | '\u{000c}' | '\n' | '\r' | '\t' => 2,
        '\u{0000}'..='\u{001f}' => 6,
        _ => c.len_utf8(),
    }
}

pub(super) fn reply(
    bytes: Vec<u8>,
    offset: u64,
    max_bytes: usize,
    expected: Option<&str>,
) -> Result<FileReply> {
    validate(offset, max_bytes, expected)?;
    if bytes.len() > READ_SOURCE_BYTES_LIMIT {
        return Err(Error::ContextBudgetExceeded);
    }
    let sha256 = content_hash(&bytes);
    if expected.is_some_and(|expected| expected != sha256) {
        return Err(Error::RevisionConflict);
    }
    let source = String::from_utf8(bytes)
        .map_err(|_| Error::InvalidTransition("file is not UTF-8".into()))?;
    let start = offset as usize;
    if start > source.len() || !source.is_char_boundary(start) {
        return Err(Error::InvalidTransition(
            "offset_bytes must be a UTF-8 boundary at or before EOF".into(),
        ));
    }
    let mut end = start;
    let mut encoded = 0;
    for (i, c) in source[start..].char_indices() {
        let next = i + c.len_utf8();
        let cost = encoded_char_bytes(c);
        if next > max_bytes || encoded + cost > CONTENT_JSON_BYTES {
            break;
        }
        encoded += cost;
        end = start + next;
    }
    if end == start && start < source.len() {
        return Err(Error::InvalidTransition(
            "max_bytes is too small for the next UTF-8 character".into(),
        ));
    }
    let content = &source[start..end];
    let start_line = source[..start].bytes().filter(|b| *b == b'\n').count() as u64 + 1;
    let end_line = start_line + content.bytes().filter(|b| *b == b'\n').count() as u64
        - u64::from(content.ends_with('\n'));
    let eof = end == source.len();
    let reply = FileReply {
        content: Some(content.to_owned()),
        sha256,
        artifact: None,
        page: Some(ReadPageInfo {
            offset_bytes: offset,
            end_offset_bytes: end as u64,
            total_bytes: source.len() as u64,
            start_line,
            end_line,
            next_offset_bytes: (!eof).then_some(end as u64),
            eof,
        }),
    };
    if canonical_bytes(&serde_json::json!({"result": &reply}))?.len() > FILE_REPLY_BYTES_LIMIT {
        return Err(Error::ContextBudgetExceeded);
    }
    Ok(reply)
}
