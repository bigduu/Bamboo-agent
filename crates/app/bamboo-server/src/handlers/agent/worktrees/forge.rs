//! Authoritative forge merge state (**slice B**: optional, strictly degrading).
//!
//! Slice A (`merge_state`) answers from local refs and therefore cannot observe a
//! squash merge. A forge API can, because the pull request itself records the
//! merge. This module adds that as an *optional* enrichment that may only ever
//! upgrade an unproven `in_progress` to `merged`, and that reports a precise
//! reason whenever it cannot answer.
//!
//! Design constraints, all deliberate:
//!
//! - **No credential is a normal state.** `GH_TOKEN`, then `GITHUB_TOKEN`, then
//!   `gh auth token` (only if `gh` resolves and exits 0 quickly). Absent all
//!   three, the answer is `no_credentials` and the endpoint still succeeds.
//! - **Never send a GitHub token anywhere but GitHub.** The remote host is parsed
//!   first; a GitLab/Gitea/Bitbucket/self-hosted or local remote is
//!   `unsupported_forge` / `no_remote` and no request is attempted.
//! - **A token never appears in output.** No token value is logged, returned, or
//!   embedded in an error string; only the reason codes below are surfaced.
//! - **Every failure is a reason code, not an error.** Rate limits, 5xx, DNS,
//!   timeouts and malformed JSON all degrade to the local verdict.
//!
//! The decision table lives in [`resolve`] as a pure function so the whole
//! degradation matrix is exhaustively testable without network access.
use super::merge_state::{MergeState, MergeVerdict};
use serde::Deserialize;
use std::path::Path;
use std::time::Duration;

/// Why the forge could not, or did not need to, answer.
///
/// These are stable machine codes, not UI copy: the client renders the local
/// state and may show the code as diagnostic detail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForgeReason {
    /// A merged pull request proves the work landed.
    MergedPullRequest,
    /// The pull request exists and is still open.
    OpenPullRequest,
    /// The forge answered; it knows of no pull request for this branch.
    NoPullRequest,
    /// No `origin` remote, or a remote that does not identify a repository.
    NoRemote,
    /// A remote host this build cannot query (GitLab, Gitea, self-hosted, ...).
    UnsupportedForge,
    /// No token from the environment and no usable `gh`.
    NoCredentials,
    /// The call failed: network, DNS, timeout, 5xx, malformed body.
    RequestFailed,
    /// GitHub's rate limit is exhausted for this token.
    RateLimited,
    /// The forge asked for an answer but the entry needs no forge check.
    NotRequired,
}

impl ForgeReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MergedPullRequest => "merged_pull_request",
            Self::OpenPullRequest => "open_pull_request",
            Self::NoPullRequest => "no_pull_request",
            Self::NoRemote => "no_remote",
            Self::UnsupportedForge => "unsupported_forge",
            Self::NoCredentials => "no_credentials",
            Self::RequestFailed => "request_failed",
            Self::RateLimited => "rate_limited",
            Self::NotRequired => "not_required",
        }
    }
}

/// What the forge said about one branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrState {
    Merged,
    Open,
    None,
    Unavailable(ForgeReason),
}

/// A parsed `origin` remote that identifies a repository on some host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Remote {
    pub host: String,
    pub owner: String,
    pub repo: String,
}

impl Remote {
    /// The only forge this provider speaks. Case-insensitive because hosts are.
    pub fn is_github(&self) -> bool {
        self.host.eq_ignore_ascii_case("github.com")
    }
}

/// Parse the shapes Git accepts for a remote and that people actually configure:
/// `git@host:owner/repo.git`, `ssh://git@host/owner/repo.git`,
/// `https://host/owner/repo(.git)`, with optional credentials and port.
pub fn parse_remote(url: &str) -> Result<Remote, ForgeReason> {
    let url = url.trim();
    if url.is_empty() {
        return Err(ForgeReason::NoRemote);
    }
    // A local path or `file://` remote identifies no forge at all.
    if url.starts_with('/') || url.starts_with('.') || url.starts_with("file://") {
        return Err(ForgeReason::NoRemote);
    }
    let (authority, path) = if let Some(rest) = url.strip_prefix("git@") {
        // scp-like syntax: no scheme, `:` separates host from path.
        match rest.split_once(':') {
            Some((host, path)) => (host.to_owned(), path.to_owned()),
            None => return Err(ForgeReason::NoRemote),
        }
    } else if let Some((scheme, rest)) = url.split_once("://") {
        if !matches!(scheme, "ssh" | "https" | "http" | "git") {
            return Err(ForgeReason::NoRemote);
        }
        // Drop any `user@` and any `:port`, then split host from path.
        let (authority, path) = rest.split_once('/').ok_or(ForgeReason::NoRemote)?;
        (authority.to_owned(), path.to_owned())
    } else {
        // A bare `host/owner/repo` is not a form Git produces for a remote.
        return Err(ForgeReason::NoRemote);
    };
    let host = authority
        .rsplit_once('@')
        .map(|(_, host)| host)
        .unwrap_or(&authority)
        .split(':')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    let path = path.trim_end_matches('/');
    let path = path.strip_suffix(".git").unwrap_or(path);
    let mut segments = path.split('/').filter(|segment| !segment.is_empty());
    let owner = segments.next().ok_or(ForgeReason::NoRemote)?;
    let repo = segments.next().ok_or(ForgeReason::NoRemote)?;
    if host.is_empty() || segments.next().is_some() {
        // Extra path segments mean this is not a plain repository URL.
        return Err(ForgeReason::NoRemote);
    }
    Ok(Remote {
        host,
        owner: owner.to_owned(),
        repo: repo.to_owned(),
    })
}

/// Whether the forge may be consulted at all, before any network concern.
///
/// Kept separate from [`parse_remote`] so "we cannot reach this *kind* of forge"
/// is distinguishable from "the URL was unparseable", which read very
/// differently in a tooltip.
pub fn support(remote: &Result<Remote, ForgeReason>) -> Result<&Remote, ForgeReason> {
    match remote {
        Ok(remote) if remote.is_github() => Ok(remote),
        Ok(_) => Err(ForgeReason::UnsupportedForge),
        Err(reason) => Err(*reason),
    }
}

/// Resolve a token, preferring explicit environment configuration over the CLI so
/// a machine with a stale `gh` login cannot override an operator's choice.
///
/// The value is returned only to the caller that makes the request; it is never
/// logged, never placed in a response body and never included in an error string.
pub fn credential_token() -> Option<String> {
    for name in ["GH_TOKEN", "GITHUB_TOKEN"] {
        if let Ok(value) = std::env::var(name) {
            let value = value.trim().to_owned();
            if !value.is_empty() {
                return Some(value);
            }
        }
    }
    gh_cli_token()
}

/// Resolve `origin` and confirm this build may query it, in one place so callers
/// cannot accidentally query an unsupported forge with a GitHub token.
pub fn github_remote(repo: &Path) -> Result<Remote, ForgeReason> {
    support(&parse_remote(&remote_url(repo)?)).cloned()
}

/// `gh auth token`, bounded. `gh` is optional: a missing, hung, unauthenticated or
/// failing CLI is simply "no credential", never an error the caller must handle.
fn gh_cli_token() -> Option<String> {
    use std::process::{Command, Stdio};
    use std::time::Instant;

    let mut child = Command::new("gh")
        .args(["auth", "token"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if !status.success() {
                    return None;
                }
                let output = child.wait_with_output().ok()?;
                let token = String::from_utf8(output.stdout).ok()?;
                let token = token.trim().to_owned();
                return (!token.is_empty()).then_some(token);
            }
            Ok(None) if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(10)),
            Err(_) => return None,
        }
    }
}

/// The decision table. Pure, total, and deliberately monotone: `merged` may only
/// ever be asserted from proof (local ancestry/patch evidence or a merged PR), and
/// a local `unknown` is never turned into a confident answer by a missing forge.
///
/// Returns the effective state plus the two provenance codes the client needs to
/// avoid over-claiming.
pub fn resolve(local: &MergeVerdict, pr: PrState) -> (MergeState, &'static str, ForgeReason) {
    match pr {
        // Proof: the change request says it landed.
        PrState::Merged => (
            MergeState::Merged,
            local.reason,
            ForgeReason::MergedPullRequest,
        ),
        PrState::Open => (
            // An open PR is not proof by itself, but it does confirm the local
            // "outstanding work" reading, so keep it rather than downgrading.
            if local.state == MergeState::InProgress {
                MergeState::InProgress
            } else {
                local.state
            },
            local.reason,
            ForgeReason::OpenPullRequest,
        ),
        PrState::None => (local.state, local.reason, ForgeReason::NoPullRequest),
        PrState::Unavailable(reason) => (local.state, local.reason, reason),
    }
}

/// Should the forge be asked about this entry at all?
///
/// Only locally-unproven `in_progress` entries benefit: `primary` and `merged` are
/// already settled, and `unknown` means the local repository itself could not be
/// read, so a forge answer would be about a different question.
pub fn needs_forge(state: MergeState) -> bool {
    state == MergeState::InProgress
}

const API_TIMEOUT: Duration = Duration::from_secs(6);

#[derive(Deserialize)]
struct PullRequest {
    #[serde(default)]
    merged_at: Option<String>,
    #[serde(default)]
    state: Option<String>,
}

/// One bounded GitHub query for one branch.
///
/// Unauthenticated requests are allowed by the API but rate-limit aggressively,
/// which is why `no_credentials` short-circuits before this is called.
pub async fn query_github(
    client: &reqwest::Client,
    remote: &Remote,
    branch: &str,
    token: &str,
) -> PrState {
    let url = format!(
        "https://api.github.com/repos/{}/{}/pulls",
        remote.owner, remote.repo
    );
    let query = [
        ("head", format!("{}:{branch}", remote.owner)),
        ("state", "all".to_owned()),
        ("per_page", "1".to_owned()),
    ];
    let response = match client
        .get(&url)
        .query(&query)
        .timeout(API_TIMEOUT)
        .header("Accept", "application/vnd.github+json")
        .header("X-GitHub-Api-Version", "2022-11-28")
        .header("User-Agent", "bamboo-worktree-merge-state")
        .bearer_auth(token)
        .send()
        .await
    {
        Ok(response) => response,
        Err(_) => return PrState::Unavailable(ForgeReason::RequestFailed),
    };
    let status = response.status();
    if status == reqwest::StatusCode::FORBIDDEN || status == reqwest::StatusCode::TOO_MANY_REQUESTS
    {
        // 403 with an exhausted quota is GitHub's rate-limit signal.
        let limited = response
            .headers()
            .get("x-ratelimit-remaining")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.trim() == "0");
        return PrState::Unavailable(
            if limited || status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                ForgeReason::RateLimited
            } else {
                ForgeReason::RequestFailed
            },
        );
    }
    if !status.is_success() {
        return PrState::Unavailable(ForgeReason::RequestFailed);
    }
    let pulls: Vec<PullRequest> = match response.json().await {
        Ok(pulls) => pulls,
        Err(_) => return PrState::Unavailable(ForgeReason::RequestFailed),
    };
    match pulls.first() {
        None => PrState::None,
        Some(pull) if pull.merged_at.is_some() => PrState::Merged,
        Some(pull) if pull.state.as_deref() == Some("open") => PrState::Open,
        // A closed-and-unmerged PR is not proof of anything: the work may have
        // landed under another PR, so keep the local verdict and report "none".
        Some(_) => PrState::None,
    }
}

/// Read `origin`'s URL with the same bounded, hook-free settings the rest of this
/// module uses. A repository without `origin` is `no_remote`, not a failure.
pub fn remote_url(repo: &Path) -> Result<String, ForgeReason> {
    let output = super::git::git_command(repo)
        .args(["remote", "get-url", "origin"])
        .output()
        .map_err(|_| ForgeReason::NoRemote)?;
    if !output.status.success() {
        return Err(ForgeReason::NoRemote);
    }
    String::from_utf8(output.stdout)
        .map(|url| url.trim().to_owned())
        .map_err(|_| ForgeReason::NoRemote)
}
