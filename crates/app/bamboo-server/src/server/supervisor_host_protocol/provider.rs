//! Bamboo owns this same-executable helper and both of its pipes. No HTTP or
//! sidecar frame is parsed as a helper reply, and no consent is persisted.
use async_trait::async_trait;
use std::{
    io::{Read, Write},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{
    confirmation::{
        self, ConfirmationRequest, Decision, HelperDecision, HelperResponse, HELPER_ARG,
        MAX_HELPER_BYTES,
    },
    native_macos,
    wire::AuthorityProfile,
};

#[async_trait]
pub(super) trait ConfirmationProvider: Send + Sync {
    fn profile(&self) -> AuthorityProfile;
    async fn confirm(&self, request: &ConfirmationRequest) -> Decision;
}

pub(super) struct NativeConfirmation;
#[async_trait]
impl ConfirmationProvider for NativeConfirmation {
    fn profile(&self) -> AuthorityProfile {
        AuthorityProfile::new(native_macos::readiness().err().map(|reason| reason.code()))
    }
    async fn confirm(&self, request: &ConfirmationRequest) -> Decision {
        if let Err(code) = native_macos::readiness() {
            return Decision::Refused(code.code());
        }
        if request.preview().is_err() {
            return Decision::Refused("native_confirmation_unavailable");
        }
        match tokio::time::timeout(Duration::from_secs(120), owned_helper(request)).await {
            Ok(Ok(bytes)) => confirmation::owned_helper_response(request, &bytes),
            Ok(Err(_)) => Decision::Refused("native_confirmation_unavailable"),
            Err(_) => Decision::Cancelled("native_confirmation_timed_out"),
        }
    }
}

async fn owned_helper(request: &ConfirmationRequest) -> std::io::Result<Vec<u8>> {
    use std::process::Stdio;
    let payload = serde_json::to_vec(request)?;
    if payload.len() > MAX_HELPER_BYTES {
        return Err(std::io::Error::other("native preview too large"));
    }
    let executable = std::env::current_exe()?.canonicalize()?;
    let mut child = tokio::process::Command::new(executable)
        .arg(HELPER_ARG)
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    let mut input = child
        .stdin
        .take()
        .ok_or_else(|| std::io::Error::other("native input unavailable"))?;
    input.write_all(&payload).await?;
    input.shutdown().await?;
    drop(input);
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| std::io::Error::other("native output unavailable"))?;
    let mut bytes = Vec::new();
    stdout
        .take(MAX_HELPER_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() > MAX_HELPER_BYTES {
        return Err(std::io::Error::other("native output too large"));
    }
    if !child.wait().await?.success() {
        return Err(std::io::Error::other("native helper unavailable"));
    }
    Ok(bytes)
}

/// Intercepted before Tokio/config/logging on the executable's initial thread.
/// This UI-only process cannot load AppState or issue a mutation. Invoking it
/// manually produces no reusable credential; only the parent's owned pipe is read.
pub fn native_scope_confirmation_helper_entry() -> Option<i32> {
    if !is_helper_args(std::env::args_os().skip(1)) {
        return None;
    }
    let code = match helper_stdio() {
        Ok(()) => 0,
        Err(_) => 2,
    };
    Some(code)
}

fn is_helper_args(args: impl IntoIterator<Item = std::ffi::OsString>) -> bool {
    let mut args = args.into_iter();
    args.next().is_some_and(|arg| arg == HELPER_ARG) && args.next().is_none()
}

fn helper_stdio() -> std::io::Result<()> {
    let mut bytes = Vec::new();
    std::io::stdin()
        .lock()
        .take(MAX_HELPER_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_HELPER_BYTES {
        return Err(std::io::Error::other("native input too large"));
    }
    let request: ConfirmationRequest = serde_json::from_slice(&bytes)?;
    let decision = match request.preview() {
        Ok(preview) => native_macos::show(&preview),
        Err(_) => HelperDecision::Unavailable {
            code: confirmation::Unavailable::UiUnavailable,
        },
    };
    let response = serde_json::to_vec(&HelperResponse { request, decision })?;
    if response.len() > MAX_HELPER_BYTES {
        return Err(std::io::Error::other("native output too large"));
    }
    let mut out = std::io::stdout().lock();
    out.write_all(&response)?;
    out.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn helper_requires_its_exact_ui_only_argv_and_no_application_arguments() {
        let args = |items: &[&str]| {
            items
                .iter()
                .map(std::ffi::OsString::from)
                .collect::<Vec<_>>()
        };
        assert!(is_helper_args(args(&[HELPER_ARG])));
        for items in [
            vec![],
            vec!["serve"],
            vec![HELPER_ARG, "--data-dir", "/user"],
            vec!["--confirmed", HELPER_ARG],
        ] {
            assert!(!is_helper_args(args(&items)));
        }
    }
}
