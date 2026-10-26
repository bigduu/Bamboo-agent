# Feature Unit and E2E execution

Every feature PR to `dev` uses one physical Linux Cargo executor and one owned
`target/feature-all-features` directory. `scripts/run-feature-tests.cjs` reads the
actual Cargo metadata and selects existing `default-members` plus the recorder;
`bamboo-analytics` and unrelated example packages remain outside acceptance.

The executor invokes `cargo test --locked --all-features --no-fail-fast` once,
with an explicit `-p` for each selected package and Cargo's default target
selection. That runs library/binary Unit tests, integration E2E targets and
doctests without combining incompatible `--doc`/target flags or compiling the
same server library test binary in separate crate/filter workers. Unit and E2E
are logical target/reporting groups, not separate Cargo build jobs.

Only this feature job sets dev/test debug info to `0`; it keeps debug assertions,
the existing optimization settings and `CARGO_INCREMENTAL=0`. Formal release
profiles are unchanged. All selected-package artifacts coexist in the owned
target directory. The job preserves the exact toolchain/profile, available disk
before/after execution and final target size. GitHub documents 14 GB SSD storage
for [standard Ubuntu runners](https://docs.github.com/en/actions/how-tos/write-workflows/choose-where-workflows-run/choose-the-runner-for-a-job);
a full cold hosted run must establish that the
complete suite fits. A small executor fixture or cache hit does not prove this.
Do not recover capacity by deleting outputs and compiling the same targets again.

Cargo's JSON compiler-artifact record identifies the exact `russh_live`
integration executable. The live Docker/SSH fixture uses that compiled binary
with `--exact --ignored`; it never invokes Cargo again. Source checkout,
`CARGO_BIN_EXE` paths, generated resources and working directory remain together
in the same job. Runtime test evidence stays in the owned Cargo log artifact.

The canonical executor already includes ordinary Linux orchestration, scoped
context and recorder lifecycle cases. Separate workflows retain native
Mac/Windows behavior and the Jiandu registry identity policy, rather than
repeating those Linux test suites.

Mac/Windows TLS and recorder jobs still compile common server/dependency code in
separate jobs. The macOS TLS guard deliberately uses the dev profile to check
compact-unwind limits; other native cases retain their existing test profiles
and feature sets. This slice removes duplicate ordinary Linux producers, but
does not establish zero duplicate compilation on every OS. Native consolidation
needs one owner/target directory per OS and compatible configuration, retaining
the dev-profile unwind guard as its own configuration. It is a separate bounded
dependency before claiming the complete no-duplication contract is active.

Required `Test` rejects failed, missing, cancelled or skipped feature execution,
Quick Checks and native fixture results. `E2E Tests` reports that same executor's
result without executing tests again. A failure is repaired in the feature
branch before dev admission. Rerun its failed job, preserving the original
source and attempt evidence; a cache hit is not independent runtime proof.

The release-source publication cutover is separate from #2311. Existing frozen
release matrices/main admission and historical recovery remain unchanged until
that change is accepted. Their crate jobs retain independent failed-job reruns:

```sh
gh run rerun RUN_ID -R bigduu/Bamboo-agent --job JOB_ID
gh run rerun RUN_ID -R bigduu/Bamboo-agent --failed
```

See [CI and publication flow](../releases/ci-publication-flow.md) for activation
boundaries. No new production dispatch is implied by feature admission.
