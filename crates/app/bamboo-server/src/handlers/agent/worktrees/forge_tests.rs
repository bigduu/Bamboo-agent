//! Degradation-matrix tests for the optional forge provider.
//!
//! These are pure: no network, no token, no `gh`. The point of the matrix is that
//! every unavailable-forge situation has one specific, benign outcome, and that
//! only proof ever produces `merged`.
use super::forge::*;
use super::merge_state::{MergeState, MergeVerdict};

fn local(state: MergeState, reason: &'static str) -> MergeVerdict {
    MergeVerdict { state, reason }
}

fn out(reason: &'static str) -> PrState {
    PrState::Unavailable(match reason {
        "no_remote" => ForgeReason::NoRemote,
        "unsupported_forge" => ForgeReason::UnsupportedForge,
        "no_credentials" => ForgeReason::NoCredentials,
        "rate_limited" => ForgeReason::RateLimited,
        _ => ForgeReason::RequestFailed,
    })
}

#[test]
fn parses_the_remote_shapes_people_actually_configure() {
    for (url, expected) in [
        (
            "git@github.com:bigduu/lotus-next.git",
            ("github.com", "bigduu", "lotus-next"),
        ),
        (
            "git@github.com:bigduu/lotus-next",
            ("github.com", "bigduu", "lotus-next"),
        ),
        (
            "https://github.com/bigduu/lotus-next.git",
            ("github.com", "bigduu", "lotus-next"),
        ),
        (
            "https://github.com/bigduu/lotus-next",
            ("github.com", "bigduu", "lotus-next"),
        ),
        (
            "https://github.com/bigduu/lotus-next/",
            ("github.com", "bigduu", "lotus-next"),
        ),
        (
            "ssh://git@github.com/bigduu/lotus-next.git",
            ("github.com", "bigduu", "lotus-next"),
        ),
        (
            "ssh://git@github.com:22/bigduu/lotus-next.git",
            ("github.com", "bigduu", "lotus-next"),
        ),
        (
            "https://user:secret@github.com/bigduu/lotus-next.git",
            ("github.com", "bigduu", "lotus-next"),
        ),
        (
            "https://GitHub.com/bigduu/lotus-next.git",
            ("github.com", "bigduu", "lotus-next"),
        ),
    ] {
        let remote = parse_remote(url).unwrap_or_else(|e| panic!("{url}: {e:?}"));
        assert_eq!(
            (
                remote.host.as_str(),
                remote.owner.as_str(),
                remote.repo.as_str()
            ),
            expected,
            "{url}"
        );
    }
}

#[test]
fn a_remote_that_identifies_no_repository_is_no_remote() {
    for url in [
        "",
        "/Users/someone/repo",
        "../repo",
        "file:///Users/someone/repo",
        "https://github.com/only-owner",
        "https://github.com/bigduu/lotus-next/extra/path",
        "git@github.com:only-owner",
        "ftp://github.com/bigduu/repo",
    ] {
        assert_eq!(
            parse_remote(url).unwrap_err(),
            ForgeReason::NoRemote,
            "{url:?}"
        );
    }
}

#[test]
fn only_github_is_supported_and_every_other_host_fails_closed() {
    let github = parse_remote("https://github.com/bigduu/lotus-next.git");
    assert!(support(&github).is_ok());

    for url in [
        "https://gitlab.com/bigduu/lotus-next.git",
        "git@gitlab.com:bigduu/lotus-next.git",
        "https://gitea.example.com/bigduu/lotus-next.git",
        "https://bitbucket.org/bigduu/lotus-next.git",
        "https://github.example.com/bigduu/lotus-next.git",
        "https://git.internal.corp/team/repo.git",
    ] {
        let remote = parse_remote(url).unwrap();
        assert_eq!(
            support(&Ok(remote)).unwrap_err(),
            ForgeReason::UnsupportedForge,
            "{url} must never be queried with a GitHub token"
        );
    }

    assert_eq!(
        support(&Err(ForgeReason::NoRemote)).unwrap_err(),
        ForgeReason::NoRemote
    );
}

#[test]
fn a_merged_pull_request_is_the_only_thing_that_proves_a_squash_landing() {
    // The whole reason slice B exists: local refs said "outstanding work".
    let verdict = local(MergeState::InProgress, "unique_commits");
    let (state, reason, forge) = resolve(&verdict, PrState::Merged);
    assert_eq!(state, MergeState::Merged);
    assert_eq!(reason, "unique_commits", "local provenance is preserved");
    assert_eq!(forge, ForgeReason::MergedPullRequest);
    assert_eq!(forge.as_str(), "merged_pull_request");
}

#[test]
fn an_open_pull_request_confirms_outstanding_work_without_escalating() {
    let verdict = local(MergeState::InProgress, "unique_commits");
    let (state, reason, forge) = resolve(&verdict, PrState::Open);
    assert_eq!(state, MergeState::InProgress);
    assert_eq!(reason, "unique_commits");
    assert_eq!(forge, ForgeReason::OpenPullRequest);
}

#[test]
fn an_unavailable_forge_never_changes_the_local_verdict() {
    // The full degradation matrix. Each row is a distinct real-world situation.
    let cases = [
        ("no_remote", "local-only repository or no origin"),
        (
            "unsupported_forge",
            "GitLab / Gitea / Bitbucket / self-hosted",
        ),
        (
            "no_credentials",
            "no GH_TOKEN, no GITHUB_TOKEN, no usable gh",
        ),
        (
            "request_failed",
            "offline, DNS failure, 5xx, timeout, malformed body",
        ),
        ("rate_limited", "quota exhausted"),
    ];
    for local_state in [
        MergeState::InProgress,
        MergeState::Merged,
        MergeState::Primary,
        MergeState::Unknown,
    ] {
        for (code, description) in cases {
            let verdict = local(local_state, "unique_commits");
            let (state, reason, forge) = resolve(&verdict, out(code));
            assert_eq!(
                state, local_state,
                "{description} must not override the local state"
            );
            assert_eq!(reason, "unique_commits", "{description}");
            assert_eq!(forge.as_str(), code, "{description}");
        }
    }
}

#[test]
fn an_unproven_local_unknown_is_not_upgraded_by_a_missing_forge() {
    let verdict = local(MergeState::Unknown, "no_base_ref");
    for pr in [
        out("no_credentials"),
        out("unsupported_forge"),
        PrState::None,
    ] {
        let (state, reason, _) = resolve(&verdict, pr);
        assert_eq!(state, MergeState::Unknown);
        assert_eq!(reason, "no_base_ref");
    }
}

#[test]
fn no_pull_request_keeps_the_conservative_local_answer() {
    let verdict = local(MergeState::InProgress, "unique_commits");
    let (state, reason, forge) = resolve(&verdict, PrState::None);
    // A branch merged by another mechanism (direct push, another PR) must not be
    // silently downgraded or upgraded: say "no PR" and let the UI stay muted.
    assert_eq!(state, MergeState::InProgress);
    assert_eq!(reason, "unique_commits");
    assert_eq!(forge, ForgeReason::NoPullRequest);
}

#[test]
fn only_locally_unproven_work_needs_a_forge_round_trip() {
    assert!(needs_forge(MergeState::InProgress));
    assert!(!needs_forge(MergeState::Merged));
    assert!(!needs_forge(MergeState::Primary));
    assert!(!needs_forge(MergeState::Unknown));
}

#[test]
fn a_non_github_remote_is_reported_before_any_credential_question() {
    // Ordering matters for the operator: "this is a GitLab remote" is actionable,
    // while "no token" would send them looking for a token that cannot help.
    // `github_remote` is the single gate the handler uses, so assert its outcome
    // for each unsupported host rather than trusting caller discipline.
    let dir = std::env::temp_dir().join(format!("bodhi-forge-gitlab-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let git = |args: &[&str]| {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(&dir)
            .args(args)
            .output()
            .unwrap();
        assert!(output.status.success(), "{:?}", args);
    };
    git(&["init", "-b", "main"]);
    git(&["remote", "add", "origin", "git@gitlab.com:team/repo.git"]);
    assert_eq!(
        github_remote(&dir).unwrap_err(),
        ForgeReason::UnsupportedForge
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn every_reason_code_is_stable_and_machine_readable() {
    for (reason, code) in [
        (ForgeReason::MergedPullRequest, "merged_pull_request"),
        (ForgeReason::OpenPullRequest, "open_pull_request"),
        (ForgeReason::NoPullRequest, "no_pull_request"),
        (ForgeReason::NoRemote, "no_remote"),
        (ForgeReason::UnsupportedForge, "unsupported_forge"),
        (ForgeReason::NoCredentials, "no_credentials"),
        (ForgeReason::RequestFailed, "request_failed"),
        (ForgeReason::RateLimited, "rate_limited"),
        (ForgeReason::NotRequired, "not_required"),
    ] {
        assert_eq!(reason.as_str(), code);
    }
}

#[test]
fn a_remote_url_without_origin_is_no_remote_not_a_failure() {
    let dir = std::env::temp_dir().join(format!("bodhi-forge-no-origin-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(&dir)
        .args(["init", "-b", "main"])
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(remote_url(&dir).unwrap_err(), ForgeReason::NoRemote);
    let _ = std::fs::remove_dir_all(&dir);
}
