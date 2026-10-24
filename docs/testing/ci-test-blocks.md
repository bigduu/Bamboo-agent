# CI tests by crate

Full snapshot and release-promotion CI uses two native GitHub Actions matrices.
Each matrix lists every Cargo `default-members` crate once. Policy tests compare
both lists with the manifests, so adding a default crate requires updating both.

| Job | Command |
| --- | --- |
| `Test (<crate>)` | `cargo test --locked --no-fail-fast -p <crate>` |
| `E2E (<crate>)` | `cargo test --locked --no-fail-fast -p <crate> --all-features --lib --tests` |
| `Test SSH` | `bash scripts/run-russh-live.sh` |

The default command includes unit tests, integration tests and doctests. The E2E
command retains the existing all-feature library and integration coverage,
including the server's `env_cache_isolation` target. Cargo compiles and runs the
selected crate in its job using the existing Rust caches. There is no shared
build prerequisite, inventory planner or binary transport. Both matrices use
`fail-fast: false` and allow GitHub's available runners to execute them in parallel.

`Test` requires all default crate jobs and the real Docker/SSH fixture to succeed.
`E2E Tests` requires all all-feature crate jobs to succeed. Failed, cancelled or
skipped work cannot pass either summary. Existing dev compile admission and
release platform, lint, security and source-identity checks remain in place.

## Rerun a failed crate

Use the failed job's **Re-run jobs** button, or:

```sh
gh run rerun RUN_ID -R bigduu/Bamboo-agent --job JOB_ID
# Retry every failed job while retaining successful crate results:
gh run rerun RUN_ID -R bigduu/Bamboo-agent --failed
```

Reproduce locally with the matching `cargo test -p <crate>` command from the table.
Successful jobs from an earlier attempt remain valid for the same run and source;
publication verifies the actual job names, commands, results and attempts.
