# Feature Unit and E2E execution

Every feature PR to dev uses one physical Linux executor and one owned
target/feature-tests directory. The controller selects existing Cargo
default-members plus the recorder; analytics and unrelated example packages
stay outside acceptance.

Each package runs independently with its default features. Cargo's default
target selection covers library/binary Unit cases, integration E2E cases and
doctests once. Do not combine the selected packages into one Cargo command:
workspace feature unification can enable test utilities in production-default
dependencies. Unit/E2E are logical target groups, not separate build jobs.

The default infrastructure context must compile and execute the telemetry
feature-unavailable regression. Tickets retain the direct production publication
branches. Standalone storage retains its own default dependency configuration.
Different configurations may require different compiled artifacts; their
compatible units share the owned target without cleanup/recompilation cycles.

Only these additive feature owners run extra cases:

| Package | Explicit features | Additional cases |
| --- | --- | --- |
| Infrastructure | otel-otlp, test-utils | 5 OTLP, 1 shutdown, 1 public test-API |
| Tickets | test-utils | 5 file publication/reconciliation fault cases |
| Server | test-utils | 1 credential Unit case, 1 env-cache integration case |
| Agent | ticket-runtime-fixtures | 3 publication, 1 file reconciliation cases |

Each owner first compiles its selected targets and compares their actual test
catalog with the independently tested default catalog. Added/removed names must
match the explicit map. Cargo then runs only the new names with exact filters;
all compiler units must report fresh reuse. Common ordinary cases do not run
again under another flag. The controller preserves package working directories,
Cargo-generated resources and executable environment while running these cases.

A pinned inventory covers feature declarations, required-feature targets and
full feature-sensitive source/module fingerprints, including macro additions. A new feature-gated case
outside the mapped targets also fails admission until its ownership is reviewed.
Update that inventory only after reviewing the default/additive case map.
Ordinary cases in existing ungated targets join default execution automatically.

The original serial storage selectors retain --test-threads=1, including their
matching session-merge regression. The Cargo invocation excludes those qualified
selectors, then the same standalone default storage executable runs them.
Its catalog and exact success log must match all five required families and
four Root birth regressions. The Root Birth workflow retains strict Clippy
analysis without another runtime producer.

Cargo's JSON compiler-artifact record identifies the default broker's exact
russh_live executable. The ignored Docker/SSH fixture uses it with exact/ignored
selection immediately in that context, without another Cargo compilation.

Only this feature job sets dev/test debug info to 0; debug assertions, existing
optimization settings and CARGO_INCREMENTAL=0 remain. Formal release profiles
are unchanged. All context artifacts coexist. Evidence includes per-context
commands, features/profile/fresh receipts, toolchain, before/after disk and final
target size. Standard [Ubuntu runners](https://docs.github.com/en/actions/how-tos/write-workflows/choose-where-workflows-run/choose-the-runner-for-a-job)
advertise 14 GB SSD storage. A full cold hosted run must establish that the
suite fits; a tiny executor fixture or cache hit does not prove capacity.

Ordinary Linux orchestration, scoped-context and recorder cases already belong
to this executor. All agents use the same ownership path. Policy rejects new
ordinary Linux PR Cargo-test producers, including tests hidden behind supported
repository wrappers. Native OS, containment boundaries, explicit manual
diagnostics and pinned legacy routes retain named scoped exceptions.

Mac/Windows TLS and recorder jobs still compile common code in separate jobs.
The macOS TLS guard deliberately uses a dev profile for compact-unwind checks.
Native consolidation needs one owner/target per OS and compatible configuration;
this slice does not establish ecosystem-wide zero duplicate compilation.

Required Test rejects failed, missing, cancelled or skipped selected results.
E2E Tests reports the same executor result without another runtime pass. Repair
failures in the feature branch before dev admission. Keep original attempt and
source evidence when rerunning a failed job.

Release-source publication cutover is separate from #2311. Frozen legacy release
matrices, main admission and historical recovery remain active until it lands.
See [CI and publication flow](../releases/ci-publication-flow.md) for those
activation boundaries. No new production dispatch is implied.
